from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sys
import time
from collections import OrderedDict, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

import pytest
import yaml

from ai_trading_system.yaml_loader import safe_load_yaml_path

RUNTIME_PROFILE_SCHEMA_VERSION = "test_runtime_profile.v1"
DURATION_PROFILE_SCHEMA_VERSION = "arch_004g2_full_duration_profile.v1"
RUNTIME_PROFILE_OUTPUT_ENV = "AITS_PYTEST_RUNTIME_PROFILE_OUTPUT"
RUNTIME_PROFILE_FORMAL_SELECTION_ENV = "AITS_PYTEST_RUNTIME_PROFILE_FORMAL_SELECTION"
DURATION_PROFILE_OPTION = "--aits-duration-profile"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
PHASE_ORDER = {"setup": 0, "call": 1, "teardown": 2}


@dataclass(frozen=True)
class DurationProfile:
    configured_path: str
    manifest_sha256: str | None
    schema_version: str | None
    profile_id: str | None
    owner: str | None
    version: int | None
    partial_seed: bool
    source_artifact_path: str | None
    source_artifact_sha256: str | None
    source_workers: int | None
    source_dist: str | None
    observed_seconds: Mapping[str, float]
    valid: bool
    fallback_reason: str | None


@dataclass(frozen=True)
class SchedulerDecision:
    policy: str
    applied: bool
    fallback: bool
    plugin_fallback_by_count: bool
    fallback_reason: str | None


@dataclass(frozen=True)
class DurationOrderVerification:
    verified: bool
    matched_tracked_file_count: int
    matched_tracked_node_count: int
    expected_ordered_sha256: str


def _normalize_file_path(value: str) -> str:
    return value.replace("\\", "/").split("::", 1)[0]


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _invalid_duration_profile(path: Path, reason: str) -> DurationProfile:
    return DurationProfile(
        configured_path=str(path),
        manifest_sha256=None,
        schema_version=None,
        profile_id=None,
        owner=None,
        version=None,
        partial_seed=False,
        source_artifact_path=None,
        source_artifact_sha256=None,
        source_workers=None,
        source_dist=None,
        observed_seconds={},
        valid=False,
        fallback_reason=reason,
    )


def load_duration_profile(path: Path) -> DurationProfile:
    try:
        raw_bytes = path.read_bytes()
        payload = safe_load_yaml_path(path)
    except (OSError, ValueError, TypeError, yaml.YAMLError) as exc:
        return _invalid_duration_profile(path, f"duration profile could not be read: {exc}")

    if not isinstance(payload, dict):
        return _invalid_duration_profile(path, "duration profile root must be a mapping")

    schema_version = payload.get("schema_version")
    profile_id = payload.get("profile_id")
    status = payload.get("status")
    owner = payload.get("owner")
    version = payload.get("version")
    source = payload.get("source")
    partial_seed = payload.get("partial_seed")
    review = payload.get("review")
    file_rows = payload.get("files")

    if schema_version != DURATION_PROFILE_SCHEMA_VERSION:
        return _invalid_duration_profile(path, "duration profile schema_version is unsupported")
    if not isinstance(profile_id, str) or not profile_id.strip():
        return _invalid_duration_profile(path, "duration profile profile_id is required")
    if status != "PARTIAL_SEED":
        return _invalid_duration_profile(path, "duration profile status must be PARTIAL_SEED")
    if not isinstance(owner, str) or not owner.strip():
        return _invalid_duration_profile(path, "duration profile owner is required")
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        return _invalid_duration_profile(
            path,
            "duration profile version must be a positive integer",
        )
    if not isinstance(source, dict):
        return _invalid_duration_profile(path, "duration profile source is required")
    if not isinstance(partial_seed, dict) or partial_seed.get("enabled") is not True:
        return _invalid_duration_profile(path, "duration profile must declare partial_seed.enabled")
    if not isinstance(review, dict):
        return _invalid_duration_profile(path, "duration profile review contract is required")
    conditions = review.get("conditions")
    if not isinstance(conditions, list) or not conditions:
        return _invalid_duration_profile(path, "duration profile review conditions are required")
    if review.get("stable_improvement_claimed") is not False:
        return _invalid_duration_profile(
            path,
            "partial duration profile must set stable_improvement_claimed=false",
        )
    if source.get("tier") != "full" or source.get("dist") != "loadfile":
        return _invalid_duration_profile(path, "duration profile source must be full/loadfile")
    if source.get("workers") != 16:
        return _invalid_duration_profile(path, "duration profile source workers must equal 16")

    source_artifact_path = source.get("artifact_path")
    source_artifact_sha256 = source.get("artifact_sha256")
    if not isinstance(source_artifact_path, str) or not source_artifact_path.strip():
        return _invalid_duration_profile(path, "duration profile source artifact_path is required")
    if not isinstance(source_artifact_sha256, str) or SHA256_RE.fullmatch(
        source_artifact_sha256
    ) is None:
        return _invalid_duration_profile(path, "duration profile source artifact_sha256 is invalid")
    if not isinstance(file_rows, list) or not file_rows:
        return _invalid_duration_profile(path, "duration profile files must be non-empty")

    observed_seconds: dict[str, float] = {}
    for row in file_rows:
        if not isinstance(row, dict):
            return _invalid_duration_profile(path, "duration profile file row must be a mapping")
        raw_file = row.get("path")
        raw_seconds = row.get("observed_seconds")
        if not isinstance(raw_file, str) or not raw_file.strip():
            return _invalid_duration_profile(path, "duration profile file path is required")
        normalized_file = _normalize_file_path(raw_file.strip())
        path_parts = PurePosixPath(normalized_file).parts
        if (
            not normalized_file.startswith("tests/")
            or Path(normalized_file).is_absolute()
            or "." in path_parts
            or ".." in path_parts
        ):
            return _invalid_duration_profile(
                path,
                f"duration profile path is out of scope: {raw_file}",
            )
        if normalized_file in observed_seconds:
            return _invalid_duration_profile(
                path,
                f"duration profile contains duplicate file: {normalized_file}",
            )
        if isinstance(raw_seconds, bool) or not isinstance(raw_seconds, (int, float)):
            return _invalid_duration_profile(
                path,
                f"duration profile observed_seconds is invalid: {normalized_file}",
            )
        seconds = float(raw_seconds)
        if not math.isfinite(seconds) or seconds <= 0:
            return _invalid_duration_profile(
                path,
                f"duration profile observed_seconds must be positive: {normalized_file}",
            )
        observed_seconds[normalized_file] = seconds

    aggregated_file_count = partial_seed.get("aggregated_file_count")
    source_duration_row_count = partial_seed.get("source_duration_row_count")
    if aggregated_file_count != len(observed_seconds):
        return _invalid_duration_profile(path, "partial seed aggregated_file_count is stale")
    if (
        isinstance(source_duration_row_count, bool)
        or not isinstance(source_duration_row_count, int)
        or source_duration_row_count < len(observed_seconds)
    ):
        return _invalid_duration_profile(path, "partial seed source_duration_row_count is invalid")

    return DurationProfile(
        configured_path=str(path),
        manifest_sha256=_sha256_bytes(raw_bytes),
        schema_version=str(schema_version),
        profile_id=profile_id,
        owner=owner,
        version=version,
        partial_seed=True,
        source_artifact_path=source_artifact_path,
        source_artifact_sha256=source_artifact_sha256,
        source_workers=int(source["workers"]),
        source_dist=str(source["dist"]),
        observed_seconds=observed_seconds,
        valid=True,
        fallback_reason=None,
    )


def resolve_scheduler_decision(
    duration_profile: DurationProfile,
    *,
    expected_worker_count: int,
    xdist_dist: str,
    loadscope_reorder: bool,
) -> SchedulerDecision:
    normalized_dist = xdist_dist.strip().lower() or "no"
    if normalized_dist != "loadfile":
        return SchedulerDecision(
            policy="non_loadfile_collection_order_preserved",
            applied=False,
            fallback=False,
            plugin_fallback_by_count=False,
            fallback_reason=(
                "duration-aware scheduling requires pytest-xdist --dist loadfile; "
                f"observed={normalized_dist}"
            ),
        )
    if expected_worker_count < 2:
        return SchedulerDecision(
            policy="serial_collection_order_preserved",
            applied=False,
            fallback=False,
            plugin_fallback_by_count=False,
            fallback_reason="duration-aware scheduling requires parallel pytest-xdist workers",
        )
    if loadscope_reorder:
        return SchedulerDecision(
            policy="stock_loadfile_test_count_order",
            applied=False,
            fallback=True,
            plugin_fallback_by_count=False,
            fallback_reason=(
                "duration-aware scheduling requires --no-loadscope-reorder; "
                "pytest-xdist owns the stock test-count fallback"
            ),
        )
    if not duration_profile.valid:
        return SchedulerDecision(
            policy="stock_loadfile_test_count_order",
            applied=False,
            fallback=True,
            plugin_fallback_by_count=True,
            fallback_reason=duration_profile.fallback_reason,
        )
    if expected_worker_count != duration_profile.source_workers:
        return SchedulerDecision(
            policy="stock_loadfile_test_count_order",
            applied=False,
            fallback=True,
            plugin_fallback_by_count=True,
            fallback_reason=(
                "duration profile worker count mismatch: "
                f"profile={duration_profile.source_workers} observed={expected_worker_count}"
            ),
        )
    return SchedulerDecision(
        policy="tracked_partial_seed_duration_descending_stable",
        applied=True,
        fallback=False,
        plugin_fallback_by_count=False,
        fallback_reason=None,
    )


def _stable_file_order_indices(
    nodeids: Sequence[str],
    observed_seconds: Mapping[str, float],
    *,
    fallback_by_count: bool,
) -> list[int]:
    grouped: OrderedDict[str, list[int]] = OrderedDict()
    for index, nodeid in enumerate(nodeids):
        grouped.setdefault(_normalize_file_path(nodeid), []).append(index)

    if fallback_by_count:
        ordered_groups = sorted(grouped.items(), key=lambda row: -len(row[1]))
    else:
        ordered_groups = sorted(
            grouped.items(),
            key=lambda row: -float(observed_seconds.get(row[0], 0.0)),
        )
    return [index for _, indices in ordered_groups for index in indices]


def stable_reorder_nodeids(
    nodeids: Sequence[str],
    observed_seconds: Mapping[str, float],
    *,
    fallback_by_count: bool = False,
) -> list[str]:
    order = _stable_file_order_indices(
        nodeids,
        observed_seconds,
        fallback_by_count=fallback_by_count,
    )
    return [nodeids[index] for index in order]


def verify_duration_order(
    nodeids: Sequence[str],
    observed_seconds: Mapping[str, float],
) -> DurationOrderVerification:
    """Verify the final collection is a stable file-level duration ordering.

    File groups with the same observed duration retain their first-seen order.  All
    untracked files have the same implicit zero weight, so they remain in their
    first-seen order after every tracked positive-duration group.  Reapplying the
    stable ordering must therefore be a fixed point of the final collection.
    """

    normalized_nodeids = [str(nodeid) for nodeid in nodeids]
    expected_nodeids = stable_reorder_nodeids(normalized_nodeids, observed_seconds)
    matched_files = {
        _normalize_file_path(nodeid)
        for nodeid in normalized_nodeids
        if _normalize_file_path(nodeid) in observed_seconds
    }
    matched_node_count = sum(
        1
        for nodeid in normalized_nodeids
        if _normalize_file_path(nodeid) in observed_seconds
    )
    expected_identity = collection_identity(expected_nodeids)
    return DurationOrderVerification(
        verified=(
            bool(matched_files)
            and normalized_nodeids == expected_nodeids
        ),
        matched_tracked_file_count=len(matched_files),
        matched_tracked_node_count=matched_node_count,
        expected_ordered_sha256=str(expected_identity["ordered_sha256"]),
    )


def collection_identity(nodeids: Sequence[str]) -> dict[str, object]:
    normalized = [str(nodeid) for nodeid in nodeids]
    counts: dict[str, int] = defaultdict(int)
    for nodeid in normalized:
        counts[nodeid] += 1
    duplicates = sorted(nodeid for nodeid, count in counts.items() if count > 1)
    ordered_payload = "\n".join(normalized).encode("utf-8")
    set_payload = "\n".join(sorted(normalized)).encode("utf-8")
    return {
        "count": len(normalized),
        "ordered_sha256": _sha256_bytes(ordered_payload),
        "set_sha256": _sha256_bytes(set_payload),
        "duplicate_nodeids": duplicates,
    }


def _as_finite_float(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _node_outcome(phases: Sequence[Mapping[str, object]]) -> str:
    outcomes = {str(row.get("outcome") or "unknown") for row in phases}
    if "failed" in outcomes:
        return "failed"
    if "skipped" in outcomes:
        return "skipped"
    if outcomes == {"passed"}:
        return "passed"
    return "unknown"


def _iso_utc(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=UTC).isoformat().replace("+00:00", "Z")


def _worker_cli_option(
    argv: Sequence[str],
    option: str,
    *,
    default: str,
) -> str:
    value = default
    for index, raw_arg in enumerate(argv):
        arg = str(raw_arg)
        if arg == option and index + 1 < len(argv):
            value = str(argv[index + 1])
        elif arg.startswith(f"{option}="):
            value = arg.split("=", 1)[1]
    return value


def _worker_loadscope_reorder(argv: Sequence[str], *, default: bool) -> bool:
    value = default
    for raw_arg in argv:
        arg = str(raw_arg)
        if arg == "--loadscope-reorder":
            value = True
        elif arg == "--no-loadscope-reorder":
            value = False
    return value


def build_runtime_profile(
    *,
    collections: Mapping[str, Sequence[str]],
    phase_reports: Sequence[Mapping[str, object]],
    duration_profile: DurationProfile,
    expected_worker_count: int,
    xdist_dist: str,
    loadscope_reorder: bool,
    formal_full_selection_eligible: bool,
    pytest_exitstatus: int,
    started_at: float,
    ended_at: float,
) -> dict[str, object]:
    warnings: list[str] = []
    scheduler_decision = resolve_scheduler_decision(
        duration_profile,
        expected_worker_count=expected_worker_count,
        xdist_dist=xdist_dist,
        loadscope_reorder=loadscope_reorder,
    )
    collection_rows = {
        worker_id: [str(nodeid) for nodeid in nodeids]
        for worker_id, nodeids in sorted(collections.items())
    }
    canonical_nodeids = next(iter(collection_rows.values()), [])
    canonical_identity = collection_identity(canonical_nodeids)
    duration_order_verification = verify_duration_order(
        canonical_nodeids,
        duration_profile.observed_seconds,
    )
    collection_complete = bool(collection_rows)

    if len(collection_rows) != expected_worker_count:
        collection_complete = False
        warnings.append(
            "collection worker count mismatch: "
            f"expected={expected_worker_count} observed={len(collection_rows)}"
        )
    for worker_id, nodeids in collection_rows.items():
        identity = collection_identity(nodeids)
        if nodeids != canonical_nodeids:
            collection_complete = False
            warnings.append(f"collection order mismatch for worker={worker_id}")
        if identity["duplicate_nodeids"]:
            collection_complete = False
            warnings.append(f"duplicate collected nodeids for worker={worker_id}")

    canonical_set = set(canonical_nodeids)
    reports_by_node: dict[str, list[dict[str, object]]] = defaultdict(list)
    invalid_report_count = 0
    for raw_report in phase_reports:
        nodeid = raw_report.get("nodeid")
        phase = raw_report.get("phase")
        worker_id = raw_report.get("worker_id")
        start = _as_finite_float(raw_report.get("start"))
        stop = _as_finite_float(raw_report.get("stop"))
        duration = _as_finite_float(raw_report.get("duration"))
        outcome = raw_report.get("outcome")
        if (
            not isinstance(nodeid, str)
            or not nodeid
            or phase not in PHASE_ORDER
            or not isinstance(worker_id, str)
            or not worker_id
            or start is None
            or stop is None
            or duration is None
            or stop < start
            or duration < 0
            or outcome not in {"passed", "failed", "skipped"}
        ):
            invalid_report_count += 1
            continue
        reports_by_node[nodeid].append(
            {
                "phase": phase,
                "start_utc": _iso_utc(start),
                "stop_utc": _iso_utc(stop),
                "start_epoch_seconds": start,
                "stop_epoch_seconds": stop,
                "duration_seconds": duration,
                "outcome": outcome,
                "worker_id": worker_id,
            }
        )

    reported_set = set(reports_by_node)
    missing_nodeids = sorted(canonical_set - reported_set)
    extra_nodeids = sorted(reported_set - canonical_set)
    duplicate_phase_nodeids: list[str] = []
    missing_required_phase_nodeids: list[str] = []
    inconsistent_worker_nodeids: list[str] = []
    node_rows: list[dict[str, object]] = []

    for nodeid in canonical_nodeids:
        phases = sorted(
            reports_by_node.get(nodeid, []),
            key=lambda row: PHASE_ORDER[str(row["phase"])],
        )
        phase_names = [str(row["phase"]) for row in phases]
        if len(phase_names) != len(set(phase_names)):
            duplicate_phase_nodeids.append(nodeid)
        phase_by_name = {str(row["phase"]): row for row in phases}
        setup = phase_by_name.get("setup")
        required_phases_present = "setup" in phase_by_name and "teardown" in phase_by_name
        if isinstance(setup, dict) and setup.get("outcome") == "passed":
            required_phases_present = required_phases_present and "call" in phase_by_name
        if not required_phases_present:
            missing_required_phase_nodeids.append(nodeid)
        worker_ids = sorted({str(row["worker_id"]) for row in phases})
        if len(worker_ids) != 1:
            inconsistent_worker_nodeids.append(nodeid)
        starts = [float(row["start_epoch_seconds"]) for row in phases]
        stops = [float(row["stop_epoch_seconds"]) for row in phases]
        total_duration = sum(float(row["duration_seconds"]) for row in phases)
        node_rows.append(
            {
                "nodeid": nodeid,
                "file": _normalize_file_path(nodeid),
                "worker_id": worker_ids[0] if len(worker_ids) == 1 else None,
                "outcome": _node_outcome(phases),
                "start_utc": _iso_utc(min(starts)) if starts else None,
                "stop_utc": _iso_utc(max(stops)) if stops else None,
                "start_epoch_seconds": min(starts) if starts else None,
                "stop_epoch_seconds": max(stops) if stops else None,
                "duration_seconds": round(total_duration, 9),
                "phases": phases,
            }
        )

    if invalid_report_count:
        warnings.append(f"invalid phase report count={invalid_report_count}")
    if missing_nodeids:
        warnings.append(f"missing runtime telemetry node count={len(missing_nodeids)}")
    if extra_nodeids:
        warnings.append(f"unexpected runtime telemetry node count={len(extra_nodeids)}")
    if duplicate_phase_nodeids:
        warnings.append(f"duplicate phase telemetry node count={len(duplicate_phase_nodeids)}")
    if missing_required_phase_nodeids:
        warnings.append(
            "missing required phase telemetry node count="
            f"{len(missing_required_phase_nodeids)}"
        )
    if inconsistent_worker_nodeids:
        warnings.append(
            f"inconsistent worker telemetry node count={len(inconsistent_worker_nodeids)}"
        )

    valid_nodes = [
        row
        for row in node_rows
        if row["start_epoch_seconds"] is not None and row["stop_epoch_seconds"] is not None
    ]
    global_first_start = min(
        (float(row["start_epoch_seconds"]) for row in valid_nodes),
        default=None,
    )
    global_last_stop = max(
        (float(row["stop_epoch_seconds"]) for row in valid_nodes),
        default=None,
    )

    files: OrderedDict[str, list[dict[str, object]]] = OrderedDict()
    workers: OrderedDict[str, list[dict[str, object]]] = OrderedDict()
    for row in valid_nodes:
        files.setdefault(str(row["file"]), []).append(row)
        worker_id = row.get("worker_id")
        if isinstance(worker_id, str):
            workers.setdefault(worker_id, []).append(row)

    file_rows: list[dict[str, object]] = []
    for file_path, rows in files.items():
        starts = [float(row["start_epoch_seconds"]) for row in rows]
        stops = [float(row["stop_epoch_seconds"]) for row in rows]
        worker_ids = sorted(
            {str(row["worker_id"]) for row in rows if isinstance(row.get("worker_id"), str)}
        )
        file_rows.append(
            {
                "path": file_path,
                "node_count": len(rows),
                "worker_ids": worker_ids,
                "duration_seconds": round(
                    sum(float(row["duration_seconds"]) for row in rows),
                    9,
                ),
                "start_utc": _iso_utc(min(starts)),
                "stop_utc": _iso_utc(max(stops)),
                "elapsed_envelope_seconds": round(max(stops) - min(starts), 9),
            }
        )

    worker_rows: list[dict[str, object]] = []
    for worker_id, rows in workers.items():
        starts = [float(row["start_epoch_seconds"]) for row in rows]
        stops = [float(row["stop_epoch_seconds"]) for row in rows]
        busy_seconds = sum(float(row["duration_seconds"]) for row in rows)
        span_seconds = max(stops) - min(starts)
        tail_idle = (
            max(0.0, global_last_stop - max(stops)) if global_last_stop is not None else 0.0
        )
        worker_rows.append(
            {
                "worker_id": worker_id,
                "node_count": len(rows),
                "first_start_utc": _iso_utc(min(starts)),
                "last_stop_utc": _iso_utc(max(stops)),
                "busy_seconds": round(busy_seconds, 9),
                "span_seconds": round(span_seconds, 9),
                "internal_idle_seconds": round(max(0.0, span_seconds - busy_seconds), 9),
                "tail_idle_seconds": round(tail_idle, 9),
            }
        )

    collection_worker_ids = set(collection_rows)
    runtime_worker_ids = set(workers)
    inactive_worker_ids = sorted(collection_worker_ids - runtime_worker_ids)
    unexpected_runtime_worker_ids = sorted(runtime_worker_ids - collection_worker_ids)
    if inactive_worker_ids:
        warnings.append(f"inactive runtime worker count={len(inactive_worker_ids)}")
    if unexpected_runtime_worker_ids:
        warnings.append(
            "unexpected runtime worker count=" f"{len(unexpected_runtime_worker_ids)}"
        )

    telemetry_complete = (
        collection_complete
        and invalid_report_count == 0
        and not missing_nodeids
        and not extra_nodeids
        and not duplicate_phase_nodeids
        and not missing_required_phase_nodeids
        and not inconsistent_worker_nodeids
        and not inactive_worker_ids
        and not unexpected_runtime_worker_ids
    )
    scheduler_applied = scheduler_decision.applied
    if not scheduler_applied:
        warnings.append(
            "duration-aware scheduling was not eligible: "
            f"{scheduler_decision.fallback_reason}"
        )
    elif not duration_order_verification.verified:
        warnings.append(
            "duration-aware scheduler final collection order verification failed: "
            "matched_tracked_file_count="
            f"{duration_order_verification.matched_tracked_file_count} "
            "matched_tracked_node_count="
            f"{duration_order_verification.matched_tracked_node_count}"
        )
    if not formal_full_selection_eligible:
        warnings.append(
            "runtime profile invocation is not the formal unfiltered full-tier selection"
        )
    if pytest_exitstatus != 0:
        warnings.append(f"pytest exitstatus is not passing: {pytest_exitstatus}")
    performance_evidence_status = (
        "PASS"
        if (
            telemetry_complete
            and scheduler_applied
            and duration_order_verification.verified
            and formal_full_selection_eligible
            and pytest_exitstatus == 0
        )
        else "FAIL"
    )
    outcome_counts: dict[str, int] = defaultdict(int)
    for row in node_rows:
        outcome_counts[str(row["outcome"])] += 1

    return {
        "schema_version": RUNTIME_PROFILE_SCHEMA_VERSION,
        "report_type": "test_runtime_profile",
        "profile_status": "PASS" if telemetry_complete else "FAIL",
        "telemetry_status": "PASS" if telemetry_complete else "FAIL",
        "performance_evidence_status": performance_evidence_status,
        "stable_full_improvement_claimed": False,
        "pytest_exitstatus": pytest_exitstatus,
        "pytest_outcome_authoritative": True,
        "pytest_outcome_overridden": False,
        "started_at_utc": _iso_utc(started_at),
        "ended_at_utc": _iso_utc(ended_at),
        "elapsed_seconds": round(max(0.0, ended_at - started_at), 9),
        "scheduler": {
            "policy": scheduler_decision.policy,
            "applied": scheduler_applied,
            "fallback": scheduler_decision.fallback,
            "fallback_reason": scheduler_decision.fallback_reason,
            "configured_manifest_path": duration_profile.configured_path,
            "manifest_sha256": duration_profile.manifest_sha256,
            "manifest_schema_version": duration_profile.schema_version,
            "profile_id": duration_profile.profile_id,
            "owner": duration_profile.owner,
            "version": duration_profile.version,
            "partial_seed": duration_profile.partial_seed,
            "tracked_file_count": len(duration_profile.observed_seconds),
            "source_artifact_path": duration_profile.source_artifact_path,
            "source_artifact_sha256": duration_profile.source_artifact_sha256,
            "file_internal_node_order_preserved": True,
            "duration_order_verified": duration_order_verification.verified,
            "matched_tracked_file_count": (
                duration_order_verification.matched_tracked_file_count
            ),
            "matched_tracked_node_count": (
                duration_order_verification.matched_tracked_node_count
            ),
            "expected_ordered_sha256": (
                duration_order_verification.expected_ordered_sha256
            ),
            "equal_duration_tie_policy": "stable_first_seen_file_order",
            "untracked_file_weight_seconds": 0.0,
            "expected_worker_count": expected_worker_count,
            "xdist_dist": xdist_dist,
            "loadscope_reorder_disabled": not loadscope_reorder,
            "formal_full_selection_eligible": formal_full_selection_eligible,
        },
        "collection": {
            **canonical_identity,
            "complete": collection_complete,
            "expected_worker_count": expected_worker_count,
            "observed_worker_count": len(collection_rows),
            "worker_identities": {
                worker_id: collection_identity(nodeids)
                for worker_id, nodeids in collection_rows.items()
            },
            "nodeids": canonical_nodeids,
        },
        "telemetry": {
            "complete": telemetry_complete,
            "phase_report_count": len(phase_reports),
            "invalid_phase_report_count": invalid_report_count,
            "reported_node_count": len(reported_set),
            "missing_nodeids": missing_nodeids,
            "extra_nodeids": extra_nodeids,
            "duplicate_phase_nodeids": duplicate_phase_nodeids,
            "missing_required_phase_nodeids": missing_required_phase_nodeids,
            "inconsistent_worker_nodeids": inconsistent_worker_nodeids,
            "inactive_worker_ids": inactive_worker_ids,
            "unexpected_runtime_worker_ids": unexpected_runtime_worker_ids,
        },
        "outcome_counts": dict(sorted(outcome_counts.items())),
        "node_count": len(node_rows),
        "file_count": len(file_rows),
        "worker_count": len(worker_rows),
        "observed_test_window_seconds": (
            round(global_last_stop - global_first_start, 9)
            if global_first_start is not None and global_last_stop is not None
            else 0.0
        ),
        "tail_idle_total_seconds": round(
            sum(float(row["tail_idle_seconds"]) for row in worker_rows),
            9,
        ),
        "tail_idle_max_seconds": round(
            max((float(row["tail_idle_seconds"]) for row in worker_rows), default=0.0),
            9,
        ),
        "nodes": node_rows,
        "files": file_rows,
        "workers": worker_rows,
        "warnings": warnings,
        "strategy_logic_changed": False,
        "production_effect": "none",
        "cached_data_mutated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
    }


class RuntimeProfilePlugin:
    def __init__(self, config: pytest.Config, duration_profile: DurationProfile) -> None:
        self.config = config
        self.duration_profile = duration_profile
        self.is_worker = hasattr(config, "workerinput")
        worker_input = getattr(config, "workerinput", {})
        worker_argv = (
            [str(value) for value in worker_input.get("mainargv", [])]
            if isinstance(worker_input, dict)
            else []
        )
        worker_count = (
            worker_input.get("workercount")
            if self.is_worker and isinstance(worker_input, dict)
            else config.getoption("numprocesses", default=0)
        )
        self.expected_worker_count = worker_count if isinstance(worker_count, int) else 1
        if self.expected_worker_count < 1:
            self.expected_worker_count = 1
        self.xdist_dist = (
            _worker_cli_option(worker_argv, "--dist", default="no")
            if self.is_worker
            else str(config.getoption("dist", default="no") or "no")
        ).strip().lower()
        self.loadscope_reorder = (
            _worker_loadscope_reorder(worker_argv, default=True)
            if self.is_worker
            else bool(config.getoption("loadscopereorder", default=True))
        )
        self.formal_full_selection_eligible = (
            os.environ.get(RUNTIME_PROFILE_FORMAL_SELECTION_ENV, "").strip() == "1"
        )
        self.scheduler_decision = resolve_scheduler_decision(
            duration_profile,
            expected_worker_count=self.expected_worker_count,
            xdist_dist=self.xdist_dist,
            loadscope_reorder=self.loadscope_reorder,
        )
        self.started_at = time.time()
        self.collections: dict[str, list[str]] = {}
        self.phase_reports: list[dict[str, object]] = []

    @pytest.hookimpl(tryfirst=True)
    def pytest_collection_modifyitems(
        self,
        session: pytest.Session,
        config: pytest.Config,
        items: list[pytest.Item],
    ) -> None:
        del session, config
        if not (
            self.scheduler_decision.applied
            or self.scheduler_decision.plugin_fallback_by_count
        ):
            return
        nodeids = [item.nodeid for item in items]
        order = _stable_file_order_indices(
            nodeids,
            self.duration_profile.observed_seconds,
            fallback_by_count=self.scheduler_decision.plugin_fallback_by_count,
        )
        original = list(items)
        items[:] = [original[index] for index in order]

    @pytest.hookimpl(optionalhook=True)
    def pytest_xdist_node_collection_finished(
        self,
        node: object,
        ids: Sequence[str],
    ) -> None:
        if self.is_worker:
            return
        gateway = getattr(node, "gateway", None)
        worker_id = str(getattr(gateway, "id", "unknown"))
        self.collections[worker_id] = [str(nodeid) for nodeid in ids]

    def pytest_collection_finish(self, session: pytest.Session) -> None:
        if self.is_worker or self.expected_worker_count > 1:
            return
        self.collections["master"] = [item.nodeid for item in session.items]

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        if self.is_worker:
            return
        worker_id = getattr(report, "worker_id", None)
        if not isinstance(worker_id, str) or not worker_id:
            node = getattr(report, "node", None)
            gateway = getattr(node, "gateway", None)
            worker_id = str(getattr(gateway, "id", "master"))
        start = float(getattr(report, "start", time.time() - report.duration))
        stop = float(getattr(report, "stop", start + report.duration))
        self.phase_reports.append(
            {
                "nodeid": report.nodeid,
                "phase": report.when,
                "start": start,
                "stop": stop,
                "duration": float(report.duration),
                "outcome": report.outcome,
                "worker_id": worker_id,
            }
        )

    @pytest.hookimpl(trylast=True)
    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int) -> None:
        del session
        if self.is_worker:
            return
        output_value = os.environ.get(RUNTIME_PROFILE_OUTPUT_ENV, "").strip()
        if not output_value:
            return
        payload = build_runtime_profile(
            collections=self.collections,
            phase_reports=self.phase_reports,
            duration_profile=self.duration_profile,
            expected_worker_count=self.expected_worker_count,
            xdist_dist=self.xdist_dist,
            loadscope_reorder=self.loadscope_reorder,
            formal_full_selection_eligible=self.formal_full_selection_eligible,
            pytest_exitstatus=int(exitstatus),
            started_at=self.started_at,
            ended_at=time.time(),
        )
        output_path = Path(output_value)
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            temporary_path = output_path.with_name(f".{output_path.name}.tmp")
            temporary_path.write_text(
                json.dumps(payload, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            temporary_path.replace(output_path)
        except (OSError, TypeError, ValueError, OverflowError) as exc:
            print(
                "ARCH-004G2 runtime profile sidecar write failed; "
                f"pytest exit status remains authoritative: {exc}",
                file=sys.stderr,
                flush=True,
            )


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("aits-runtime-profile")
    group.addoption(
        DURATION_PROFILE_OPTION,
        action="store",
        default="",
        dest="aits_duration_profile",
        help="Tracked ARCH-004G2 duration profile used for stable loadfile ordering.",
    )


def pytest_configure(config: pytest.Config) -> None:
    configured = str(config.getoption("aits_duration_profile") or "").strip()
    if configured:
        candidate = Path(configured)
        profile_path = candidate if candidate.is_absolute() else Path(config.rootpath) / candidate
    else:
        profile_path = Path(config.rootpath) / "<not-configured>"
    duration_profile = load_duration_profile(profile_path)
    plugin = RuntimeProfilePlugin(config, duration_profile)
    config.pluginmanager.register(plugin, "aits-runtime-profile-state")
    if not duration_profile.valid and not plugin.is_worker:
        config.issue_config_time_warning(
            pytest.PytestWarning(
                "ARCH-004G2 duration profile invalid; using explicit stock loadfile fallback: "
                f"{duration_profile.fallback_reason}"
            ),
            stacklevel=2,
        )
    elif not plugin.scheduler_decision.applied and not plugin.is_worker:
        config.issue_config_time_warning(
            pytest.PytestWarning(
                "ARCH-004G2 duration-aware scheduler is ineligible for this execution: "
                f"{plugin.scheduler_decision.fallback_reason}"
            ),
            stacklevel=2,
        )
