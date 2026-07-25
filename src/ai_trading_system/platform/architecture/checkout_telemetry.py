from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from ai_trading_system.platform.architecture.checkout_guard import (
    CHECKOUT_INTENT_SCHEMA_VERSION,
    DEFAULT_CHECKOUT_GUARD_POLICY_PATH,
    CheckoutGuardError,
    CheckoutGuardPolicy,
    CheckoutOperationClass,
    load_checkout_guard_policy,
)
from ai_trading_system.platform.architecture.checkout_reconciliation import (
    CHECKOUT_HANDOFF_SCHEMA_VERSION,
    CHECKOUT_RECONCILIATION_REPORT_SCHEMA_VERSION,
    validate_checkout_handoff,
    validate_checkout_reconciliation_report,
)
from ai_trading_system.platform.architecture.parallel_control_kernel import (
    LeaseEvent,
    parse_lease_event,
    replay_lease_events,
)
from ai_trading_system.platform.architecture.supervised_automation import (
    RUN_REPORT_SCHEMA_VERSION,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

CHECKOUT_TELEMETRY_SNAPSHOT_SCHEMA_VERSION = "checkout_guard_telemetry_snapshot.v1"
CHECKOUT_TELEMETRY_ROLLUP_SCHEMA_VERSION = "checkout_guard_telemetry_rollup.v1"
CHECKOUT_FALSE_BLOCK_REVIEW_SCHEMA_VERSION = "checkout_guard_false_block_review.v1"

_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FALSE_BLOCK_CLASSIFICATIONS = frozenset(
    {"EXPECTED_BLOCK", "CONFIRMED_FALSE_BLOCK"}
)
_SOURCE_KINDS = frozenset(
    {
        "checkout_intent",
        "lease_event",
        "supervised_run",
        "checkout_handoff",
        "checkout_reconciliation",
        "false_block_review",
    }
)


@dataclass(frozen=True)
class CheckoutTelemetryPolicy:
    status: str
    owner: str
    approval_ref: str
    snapshot_schema: str
    rollup_schema: str
    false_block_review_schema: str
    output_root: str
    minimum_observation_batches: int
    accepted_batch_kinds: tuple[str, ...]
    guard_policy: CheckoutGuardPolicy

    @property
    def policy_version(self) -> str:
        return self.guard_policy.policy_version


def load_checkout_telemetry_policy(
    path: Path = DEFAULT_CHECKOUT_GUARD_POLICY_PATH,
) -> CheckoutTelemetryPolicy:
    payload = _mapping(safe_load_yaml_path(path), "policy")
    guard_policy = load_checkout_guard_policy(path)
    telemetry = _mapping(payload.get("telemetry"), "telemetry")
    if telemetry.get("status") != "OWNER_APPROVED_READ_ONLY":
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_POLICY_STATUS",
            str(telemetry.get("status")),
        )
    if telemetry.get("snapshot_schema") != CHECKOUT_TELEMETRY_SNAPSHOT_SCHEMA_VERSION:
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_POLICY_SNAPSHOT_SCHEMA",
            str(telemetry.get("snapshot_schema")),
        )
    if telemetry.get("rollup_schema") != CHECKOUT_TELEMETRY_ROLLUP_SCHEMA_VERSION:
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_POLICY_ROLLUP_SCHEMA",
            str(telemetry.get("rollup_schema")),
        )
    if (
        telemetry.get("false_block_review_schema")
        != CHECKOUT_FALSE_BLOCK_REVIEW_SCHEMA_VERSION
    ):
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_POLICY_FALSE_BLOCK_SCHEMA",
            str(telemetry.get("false_block_review_schema")),
        )
    minimum_batches = _positive_int(
        telemetry.get("minimum_observation_batches"),
        "telemetry.minimum_observation_batches",
    )
    if minimum_batches != 2:
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_POLICY_OBSERVATION_FLOOR",
            str(minimum_batches),
        )
    accepted_batch_kinds = _strings(
        telemetry.get("accepted_batch_kinds"),
        "telemetry.accepted_batch_kinds",
    )
    expected_batch_kinds = (
        "supervised_automation",
        "s4c_integration",
        "manual_control_plane",
    )
    if accepted_batch_kinds != expected_batch_kinds:
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_POLICY_BATCH_KINDS",
            ",".join(accepted_batch_kinds),
        )
    safety = _mapping(payload.get("safety"), "safety")
    if safety.get("s5_cutover_authorized") is not False:
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_POLICY_S5_PERMISSION",
            str(safety.get("s5_cutover_authorized")),
        )
    return CheckoutTelemetryPolicy(
        status=str(telemetry["status"]),
        owner=_required_text(telemetry.get("owner"), "telemetry.owner"),
        approval_ref=_required_text(
            telemetry.get("approval_ref"),
            "telemetry.approval_ref",
        ),
        snapshot_schema=str(telemetry["snapshot_schema"]),
        rollup_schema=str(telemetry["rollup_schema"]),
        false_block_review_schema=str(telemetry["false_block_review_schema"]),
        output_root=_portable_path(telemetry.get("output_root"), "telemetry.output_root"),
        minimum_observation_batches=minimum_batches,
        accepted_batch_kinds=accepted_batch_kinds,
        guard_policy=guard_policy,
    )


def build_checkout_telemetry_snapshot(
    *,
    project_root: Path,
    batch_id: str,
    batch_kind: str,
    runtime_root: Path | None = None,
    supervised_run_paths: Sequence[Path] = (),
    handoff_paths: Sequence[Path] = (),
    reconciliation_paths: Sequence[Path] = (),
    false_block_review_path: Path | None = None,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_CHECKOUT_GUARD_POLICY_PATH,
) -> dict[str, object]:
    root = project_root.resolve()
    policy = load_checkout_telemetry_policy(policy_path)
    checked_batch_id = _identifier(batch_id, "batch_id")
    if batch_kind not in policy.accepted_batch_kinds:
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_BATCH_KIND", batch_kind)
    instant = _aware_utc(generated_at or datetime.now(tz=UTC))
    lease_root = (
        runtime_root.resolve()
        if runtime_root is not None
        else (root / policy.guard_policy.runtime_root).resolve()
    )
    if not lease_root.is_relative_to(root):
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_RUNTIME_ROOT_OUTSIDE",
            str(lease_root),
        )
    source_paths: list[tuple[str, Path]] = []
    source_paths.extend(
        ("checkout_intent", path)
        for path in sorted((lease_root / "intents").glob("*.json"))
    )
    source_paths.extend(
        ("lease_event", path)
        for path in sorted((lease_root / "leases" / "events").glob("*/*.json"))
    )
    source_paths.extend(("supervised_run", path) for path in supervised_run_paths)
    source_paths.extend(("checkout_handoff", path) for path in handoff_paths)
    source_paths.extend(
        ("checkout_reconciliation", path) for path in reconciliation_paths
    )
    if false_block_review_path is not None:
        source_paths.append(("false_block_review", false_block_review_path))
    sources = _source_records(
        root,
        source_paths,
        policy=policy,
        batch_id=checked_batch_id,
    )
    return _snapshot_from_sources(
        project_root=root,
        batch_id=checked_batch_id,
        batch_kind=batch_kind,
        generated_at=instant,
        sources=sources,
        policy=policy,
    )


def validate_checkout_telemetry_snapshot(
    payload: Mapping[str, object],
    *,
    project_root: Path,
    policy_path: Path = DEFAULT_CHECKOUT_GUARD_POLICY_PATH,
) -> None:
    root = project_root.resolve()
    policy = load_checkout_telemetry_policy(policy_path)
    _require_exact_keys(
        payload,
        {
            "schema_version",
            "snapshot_id",
            "status",
            "batch_id",
            "batch_kind",
            "policy_version",
            "approval_ref",
            "generated_at",
            "sources",
            "identity",
            "lease_replay",
            "metrics",
            "wait_observations",
            "lease_hold_observations",
            "block_observations",
            "source_artifact_summary",
            "task_governance_status_mutated",
            "automatic_task_mutation",
            "s5_cutover_authorized",
            "task_source_cutover",
            "production_effect",
            "broker_action",
            "telemetry_checksum",
        },
        "CHECKOUT_TELEMETRY_SNAPSHOT_FIELDS",
    )
    if payload.get("schema_version") != policy.snapshot_schema:
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_SNAPSHOT_SCHEMA",
            str(payload.get("schema_version")),
        )
    batch_id = _identifier(payload.get("batch_id"), "batch_id")
    batch_kind = _required_text(payload.get("batch_kind"), "batch_kind")
    if batch_kind not in policy.accepted_batch_kinds:
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_BATCH_KIND", batch_kind)
    generated_at = _timestamp(payload.get("generated_at"), "generated_at")
    raw_sources = payload.get("sources")
    if not isinstance(raw_sources, list):
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_SOURCES",
            "sources must be a list",
        )
    sources = tuple(
        _validate_source_record(row, field=f"sources[{index}]")
        for index, row in enumerate(raw_sources)
    )
    if tuple(sorted(sources, key=_source_sort_key)) != sources:
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_SOURCE_ORDER",
            "sources must be sorted",
        )
    expected = _snapshot_from_sources(
        project_root=root,
        batch_id=batch_id,
        batch_kind=batch_kind,
        generated_at=generated_at,
        sources=sources,
        policy=policy,
    )
    if dict(payload) != expected:
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_SNAPSHOT_DRIFT",
            batch_id,
        )


def write_checkout_telemetry_snapshot(
    path: Path,
    payload: Mapping[str, object],
    *,
    project_root: Path,
    policy_path: Path = DEFAULT_CHECKOUT_GUARD_POLICY_PATH,
) -> None:
    validate_checkout_telemetry_snapshot(
        payload,
        project_root=project_root,
        policy_path=policy_path,
    )
    policy = load_checkout_telemetry_policy(policy_path)
    _assert_governed_output_path(path, project_root.resolve(), policy)
    _write_immutable_json(path, payload, "CHECKOUT_TELEMETRY_SNAPSHOT_IMMUTABILITY")


def build_checkout_telemetry_rollup(
    *,
    project_root: Path,
    snapshot_paths: Sequence[Path],
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_CHECKOUT_GUARD_POLICY_PATH,
) -> dict[str, object]:
    root = project_root.resolve()
    policy = load_checkout_telemetry_policy(policy_path)
    instant = _aware_utc(generated_at or datetime.now(tz=UTC))
    records: list[dict[str, object]] = []
    snapshots: list[dict[str, object]] = []
    for path in snapshot_paths:
        record, payload = _load_rollup_snapshot(root, path, policy_path=policy_path)
        records.append(record)
        snapshots.append(payload)
    if not records:
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_ROLLUP_EMPTY",
            "at least one snapshot is required",
        )
    ordered = sorted(
        zip(records, snapshots, strict=True),
        key=lambda item: str(item[1]["batch_id"]),
    )
    records = [item[0] for item in ordered]
    snapshots = [item[1] for item in ordered]
    batch_ids = [str(snapshot["batch_id"]) for snapshot in snapshots]
    if len(batch_ids) != len(set(batch_ids)):
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_ROLLUP_BATCH_DUPLICATE",
            ",".join(batch_ids),
        )
    metrics = {
        "observed_batch_count": len(batch_ids),
        "conflict_count": sum(
            _metric_int(snapshot, "conflict_count") for snapshot in snapshots
        ),
        "unattributed_path_count": sum(
            _metric_int(snapshot, "unattributed_path_count")
            for snapshot in snapshots
        ),
        "reconciliation_unattributed_path_count": sum(
            _metric_int(snapshot, "reconciliation_unattributed_path_count")
            for snapshot in snapshots
        ),
        "confirmed_false_block_count": sum(
            _metric_int(snapshot, "confirmed_false_block_count")
            for snapshot in snapshots
        ),
        "unreviewed_block_count": sum(
            _metric_int(snapshot, "unreviewed_block_count")
            for snapshot in snapshots
        ),
    }
    enough_batches = len(batch_ids) >= policy.minimum_observation_batches
    body: dict[str, object] = {
        "schema_version": policy.rollup_schema,
        "status": "PASS",
        "policy_version": policy.policy_version,
        "approval_ref": policy.approval_ref,
        "generated_at": instant.isoformat(),
        "snapshot_sources": records,
        "batch_ids": batch_ids,
        "minimum_observation_batches": policy.minimum_observation_batches,
        "metrics": metrics,
        "s5_evaluation_evidence_ready": enough_batches,
        "s5_owner_decision_required": True,
        "s5_cutover_authorized": False,
        "task_source_cutover": False,
        "task_governance_status_mutated": False,
        "automatic_task_mutation": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    body["rollup_id"] = f"checkout-telemetry-rollup-{_canonical_sha256(body)[:20]}"
    body["rollup_checksum"] = _payload_checksum(body, "rollup_checksum")
    return body


def validate_checkout_telemetry_rollup(
    payload: Mapping[str, object],
    *,
    project_root: Path,
    policy_path: Path = DEFAULT_CHECKOUT_GUARD_POLICY_PATH,
) -> None:
    _require_exact_keys(
        payload,
        {
            "schema_version",
            "rollup_id",
            "status",
            "policy_version",
            "approval_ref",
            "generated_at",
            "snapshot_sources",
            "batch_ids",
            "minimum_observation_batches",
            "metrics",
            "s5_evaluation_evidence_ready",
            "s5_owner_decision_required",
            "s5_cutover_authorized",
            "task_source_cutover",
            "task_governance_status_mutated",
            "automatic_task_mutation",
            "production_effect",
            "broker_action",
            "rollup_checksum",
        },
        "CHECKOUT_TELEMETRY_ROLLUP_FIELDS",
    )
    root = project_root.resolve()
    policy = load_checkout_telemetry_policy(policy_path)
    if payload.get("schema_version") != policy.rollup_schema:
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_ROLLUP_SCHEMA",
            str(payload.get("schema_version")),
        )
    sources = payload.get("snapshot_sources")
    if not isinstance(sources, list):
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_ROLLUP_SOURCES",
            "snapshot_sources must be a list",
        )
    paths = [
        _contained_source_path(
            root,
            _required_text(
                _mapping(source, f"snapshot_sources[{index}]").get("path"),
                f"snapshot_sources[{index}].path",
            ),
            known_unrelated_paths=(),
        )
        for index, source in enumerate(sources)
    ]
    expected = build_checkout_telemetry_rollup(
        project_root=root,
        snapshot_paths=paths,
        generated_at=_timestamp(payload.get("generated_at"), "generated_at"),
        policy_path=policy_path,
    )
    if dict(payload) != expected:
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_ROLLUP_DRIFT",
            str(payload.get("rollup_id")),
        )


def write_checkout_telemetry_rollup(
    path: Path,
    payload: Mapping[str, object],
    *,
    project_root: Path,
    policy_path: Path = DEFAULT_CHECKOUT_GUARD_POLICY_PATH,
) -> None:
    validate_checkout_telemetry_rollup(
        payload,
        project_root=project_root,
        policy_path=policy_path,
    )
    policy = load_checkout_telemetry_policy(policy_path)
    _assert_governed_output_path(path, project_root.resolve(), policy)
    _write_immutable_json(path, payload, "CHECKOUT_TELEMETRY_ROLLUP_IMMUTABILITY")


def validate_false_block_review(
    payload: Mapping[str, object],
    *,
    expected_batch_id: str | None = None,
) -> None:
    _require_exact_keys(
        payload,
        {
            "schema_version",
            "review_id",
            "status",
            "batch_id",
            "reviewer",
            "reviewed_at",
            "records",
            "automatic_task_mutation",
            "s5_cutover_authorized",
            "production_effect",
            "broker_action",
            "review_checksum",
        },
        "CHECKOUT_FALSE_BLOCK_REVIEW_FIELDS",
    )
    if payload.get("schema_version") != CHECKOUT_FALSE_BLOCK_REVIEW_SCHEMA_VERSION:
        raise CheckoutGuardError(
            "CHECKOUT_FALSE_BLOCK_REVIEW_SCHEMA",
            str(payload.get("schema_version")),
        )
    if payload.get("status") != "PASS":
        raise CheckoutGuardError(
            "CHECKOUT_FALSE_BLOCK_REVIEW_STATUS",
            str(payload.get("status")),
        )
    batch_id = _identifier(payload.get("batch_id"), "false_block_review.batch_id")
    if expected_batch_id is not None and batch_id != expected_batch_id:
        raise CheckoutGuardError(
            "CHECKOUT_FALSE_BLOCK_REVIEW_BATCH",
            f"{batch_id}!={expected_batch_id}",
        )
    _identifier(payload.get("review_id"), "false_block_review.review_id")
    _required_text(payload.get("reviewer"), "false_block_review.reviewer")
    _timestamp(payload.get("reviewed_at"), "false_block_review.reviewed_at")
    records = payload.get("records")
    if not isinstance(records, list):
        raise CheckoutGuardError(
            "CHECKOUT_FALSE_BLOCK_REVIEW_RECORDS",
            "records must be a list",
        )
    observation_ids: list[str] = []
    for index, value in enumerate(records):
        row = _mapping(value, f"false_block_review.records[{index}]")
        _require_exact_keys(
            row,
            {"observation_id", "classification", "rationale", "source_ref"},
            "CHECKOUT_FALSE_BLOCK_REVIEW_RECORD_FIELDS",
        )
        observation_ids.append(
            _required_text(
                row.get("observation_id"),
                f"false_block_review.records[{index}].observation_id",
            )
        )
        classification = row.get("classification")
        if classification not in _FALSE_BLOCK_CLASSIFICATIONS:
            raise CheckoutGuardError(
                "CHECKOUT_FALSE_BLOCK_REVIEW_CLASSIFICATION",
                str(classification),
            )
        _required_text(
            row.get("rationale"),
            f"false_block_review.records[{index}].rationale",
        )
        _required_text(
            row.get("source_ref"),
            f"false_block_review.records[{index}].source_ref",
        )
    if len(observation_ids) != len(set(observation_ids)):
        raise CheckoutGuardError(
            "CHECKOUT_FALSE_BLOCK_REVIEW_DUPLICATE",
            ",".join(observation_ids),
        )
    if observation_ids != sorted(observation_ids):
        raise CheckoutGuardError(
            "CHECKOUT_FALSE_BLOCK_REVIEW_ORDER",
            "records must be sorted by observation_id",
        )
    if any(
        payload.get(field) is not False
        for field in ("automatic_task_mutation", "s5_cutover_authorized")
    ):
        raise CheckoutGuardError(
            "CHECKOUT_FALSE_BLOCK_REVIEW_UNSAFE_PERMISSION",
            str(payload.get("review_id")),
        )
    if payload.get("production_effect") != "none" or payload.get("broker_action") != "none":
        raise CheckoutGuardError(
            "CHECKOUT_FALSE_BLOCK_REVIEW_UNSAFE_EFFECT",
            str(payload.get("review_id")),
        )
    if payload.get("review_checksum") != _payload_checksum(payload, "review_checksum"):
        raise CheckoutGuardError(
            "CHECKOUT_FALSE_BLOCK_REVIEW_CHECKSUM",
            str(payload.get("review_id")),
        )


def _snapshot_from_sources(
    *,
    project_root: Path,
    batch_id: str,
    batch_kind: str,
    generated_at: datetime,
    sources: Sequence[Mapping[str, object]],
    policy: CheckoutTelemetryPolicy,
) -> dict[str, object]:
    loaded = _load_sources(
        project_root,
        sources,
        policy=policy,
        batch_id=batch_id,
    )
    events = tuple(
        value for kind, _, value in loaded if kind == "lease_event"
    )
    intents = tuple(
        value for kind, _, value in loaded if kind == "checkout_intent"
    )
    reviews = tuple(
        value for kind, _, value in loaded if kind == "false_block_review"
    )
    if len(reviews) > 1:
        raise CheckoutGuardError(
            "CHECKOUT_FALSE_BLOCK_REVIEW_MULTIPLE",
            batch_id,
        )
    replay = replay_lease_events(events)
    if replay.status != "PASS":
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_LEASE_REPLAY",
            replay.issues[0].code,
        )
    block_observations = _block_observations(intents, events)
    review_by_observation = (
        {}
        if not reviews
        else {
            str(row["observation_id"]): row
            for row in _list(reviews[0].get("records"), "false_block_review.records")
        }
    )
    unknown_reviews = sorted(set(review_by_observation) - {
        str(row["observation_id"]) for row in block_observations
    })
    if unknown_reviews:
        raise CheckoutGuardError(
            "CHECKOUT_FALSE_BLOCK_REVIEW_UNKNOWN_OBSERVATION",
            ",".join(unknown_reviews),
        )
    reviewed_blocks: list[dict[str, object]] = []
    for row in block_observations:
        review = review_by_observation.get(str(row["observation_id"]))
        reviewed_blocks.append(
            {
                **row,
                "false_block_classification": (
                    None if review is None else review["classification"]
                ),
                "review_rationale": None if review is None else review["rationale"],
                "review_source_ref": None if review is None else review["source_ref"],
            }
        )
    wait_observations = _wait_observations(events)
    hold_observations = _lease_hold_observations(events, generated_at)
    operation_counts = Counter(str(intent["operation_class"]) for intent in intents)
    identity = {
        "workspace_ids": sorted(
            {
                str(_mapping(intent["workspace_identity"], "workspace_identity")["workspace_id"])
                for intent in intents
            }
        ),
        "task_ids": sorted({str(intent["task_id"]) for intent in intents}),
        "thread_ids": sorted({str(intent["thread_id"]) for intent in intents}),
        "actors": sorted({str(intent["actor"]) for intent in intents}),
        "operation_classes": sorted(operation_counts),
    }
    reconciliation_rows = tuple(
        value for kind, _, value in loaded if kind == "checkout_reconciliation"
    )
    source_artifact_summary = {
        "supervised_run_count": sum(
            kind == "supervised_run" for kind, _, _ in loaded
        ),
        "checkout_handoff_count": sum(
            kind == "checkout_handoff" for kind, _, _ in loaded
        ),
        "checkout_reconciliation_count": len(reconciliation_rows),
        "reconciliation_blocking_path_count": sum(
            len(_list(row.get("blocking_paths"), "blocking_paths"))
            for row in reconciliation_rows
        ),
        "reconciliation_unattributed_path_count": sum(
            len(_list(row.get("unattributed_dirty_paths"), "unattributed_dirty_paths"))
            for row in reconciliation_rows
        ),
    }
    conflict_reason_counts = Counter(
        str(row["reason_code"])
        for row in reviewed_blocks
        if str(row["observation_kind"]) == "lease_conflict"
    )
    metrics = {
        "intent_count": len(intents),
        "lease_count": len(replay.lease_heads),
        "lease_event_count": len(events),
        "active_lease_count": len(replay.active_leases),
        "operation_class_counts": {
            operation.value: operation_counts.get(operation.value, 0)
            for operation in CheckoutOperationClass
        },
        "wait_duration_seconds": _duration_summary(
            _nonnegative_number(row["wait_seconds"], "wait_seconds")
            for row in wait_observations
        ),
        "lease_held_duration_seconds": _duration_summary(
            _nonnegative_number(row["held_seconds"], "held_seconds")
            for row in hold_observations
        ),
        "conflict_count": sum(
            row["observation_kind"] == "lease_conflict" for row in reviewed_blocks
        ),
        "conflict_reason_counts": dict(sorted(conflict_reason_counts.items())),
        "expiry_count": sum(event.to_state == "EXPIRED" for event in events),
        "heartbeat_count": sum(
            "LEASE_HEARTBEAT" in event.reason_codes for event in events
        ),
        "reassignment_count": sum(event.to_state == "REASSIGNED" for event in events),
        "lease_replay_status": replay.status,
        "lease_replay_issue_count": len(replay.issues),
        "unattributed_intent_count": len(
            {
                row["intent_id"]
                for row in reviewed_blocks
                if row["observation_kind"] == "unattributed_dirty"
            }
        ),
        "unattributed_path_count": sum(
            row["observation_kind"] == "unattributed_dirty"
            for row in reviewed_blocks
        ),
        "reconciliation_unattributed_path_count": source_artifact_summary[
            "reconciliation_unattributed_path_count"
        ],
        "false_block_review_count": len(review_by_observation),
        "confirmed_false_block_count": sum(
            row["false_block_classification"] == "CONFIRMED_FALSE_BLOCK"
            for row in reviewed_blocks
        ),
        "expected_block_count": sum(
            row["false_block_classification"] == "EXPECTED_BLOCK"
            for row in reviewed_blocks
        ),
        "unreviewed_block_count": sum(
            row["false_block_classification"] is None for row in reviewed_blocks
        ),
    }
    body: dict[str, object] = {
        "schema_version": policy.snapshot_schema,
        "status": "PASS",
        "batch_id": batch_id,
        "batch_kind": batch_kind,
        "policy_version": policy.policy_version,
        "approval_ref": policy.approval_ref,
        "generated_at": generated_at.isoformat(),
        "sources": [dict(source) for source in sources],
        "identity": identity,
        "lease_replay": {
            "status": replay.status,
            "event_count": replay.event_count,
            "lease_count": len(replay.lease_heads),
            "active_lease_count": len(replay.active_leases),
            "issue_count": len(replay.issues),
            "head_event_ids": [
                {"lease_id": lease_id, "event_id": event_id}
                for lease_id, event_id in replay.head_event_ids
            ],
        },
        "metrics": metrics,
        "wait_observations": wait_observations,
        "lease_hold_observations": hold_observations,
        "block_observations": reviewed_blocks,
        "source_artifact_summary": source_artifact_summary,
        "task_governance_status_mutated": False,
        "automatic_task_mutation": False,
        "s5_cutover_authorized": False,
        "task_source_cutover": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    body["snapshot_id"] = f"checkout-telemetry-{_canonical_sha256(body)[:20]}"
    body["telemetry_checksum"] = _payload_checksum(body, "telemetry_checksum")
    return body


def _source_records(
    project_root: Path,
    source_paths: Sequence[tuple[str, Path]],
    *,
    policy: CheckoutTelemetryPolicy,
    batch_id: str,
) -> tuple[dict[str, object], ...]:
    records: list[dict[str, object]] = []
    seen: set[str] = set()
    for kind, path in source_paths:
        if kind not in _SOURCE_KINDS:
            raise CheckoutGuardError("CHECKOUT_TELEMETRY_SOURCE_KIND", kind)
        absolute = path.resolve()
        if not absolute.is_relative_to(project_root):
            raise CheckoutGuardError(
                "CHECKOUT_TELEMETRY_SOURCE_OUTSIDE",
                str(absolute),
            )
        relative = absolute.relative_to(project_root).as_posix()
        if relative in seen:
            raise CheckoutGuardError(
                "CHECKOUT_TELEMETRY_SOURCE_DUPLICATE",
                relative,
            )
        seen.add(relative)
        payload = _load_json_source(
            project_root,
            relative,
            known_unrelated_paths=tuple(
                row.path for row in policy.guard_policy.known_unrelated_exclusions
            ),
        )
        source_id, schema_version = _validate_source_payload(
            kind,
            payload,
            relative,
            batch_id=batch_id,
        )
        records.append(
            {
                "kind": kind,
                "path": relative,
                "sha256": _sha256_path(absolute),
                "schema_version": schema_version,
                "source_id": source_id,
            }
        )
    return tuple(sorted(records, key=_source_sort_key))


def _load_sources(
    project_root: Path,
    sources: Sequence[Mapping[str, object]],
    *,
    policy: CheckoutTelemetryPolicy,
    batch_id: str,
) -> tuple[tuple[str, str, Any], ...]:
    loaded: list[tuple[str, str, Any]] = []
    seen: set[str] = set()
    known_unrelated = tuple(
        row.path for row in policy.guard_policy.known_unrelated_exclusions
    )
    for index, source in enumerate(sources):
        record = _validate_source_record(source, field=f"sources[{index}]")
        kind = str(record["kind"])
        path = str(record["path"])
        if path in seen:
            raise CheckoutGuardError("CHECKOUT_TELEMETRY_SOURCE_DUPLICATE", path)
        seen.add(path)
        absolute = _contained_source_path(
            project_root,
            path,
            known_unrelated_paths=known_unrelated,
        )
        actual_sha = _sha256_path(absolute)
        if actual_sha != record["sha256"]:
            raise CheckoutGuardError(
                "CHECKOUT_TELEMETRY_SOURCE_HASH",
                f"{path}:{record['sha256']}!={actual_sha}",
            )
        payload = _load_json_source(
            project_root,
            path,
            known_unrelated_paths=known_unrelated,
        )
        source_id, schema_version = _validate_source_payload(
            kind,
            payload,
            path,
            batch_id=batch_id,
        )
        if source_id != record["source_id"] or schema_version != record["schema_version"]:
            raise CheckoutGuardError("CHECKOUT_TELEMETRY_SOURCE_IDENTITY", path)
        value: Any = (
            parse_lease_event(payload) if kind == "lease_event" else payload
        )
        loaded.append((kind, path, value))
    return tuple(loaded)


def _validate_source_payload(
    kind: str,
    payload: Mapping[str, object],
    path: str,
    *,
    batch_id: str,
) -> tuple[str, str]:
    if kind == "checkout_intent":
        _validate_intent(payload, path)
        return str(payload["intent_id"]), CHECKOUT_INTENT_SCHEMA_VERSION
    if kind == "lease_event":
        event = parse_lease_event(payload)
        if PurePosixPath(path).stem != event.event_id:
            raise CheckoutGuardError("CHECKOUT_TELEMETRY_EVENT_PATH", path)
        if PurePosixPath(path).parent.name != event.lease.lease_id:
            raise CheckoutGuardError("CHECKOUT_TELEMETRY_EVENT_LEASE_PATH", path)
        return event.event_id, str(payload["schema_version"])
    if kind == "supervised_run":
        _validate_supervised_run_source(payload)
        return str(payload["report_id"]), RUN_REPORT_SCHEMA_VERSION
    if kind == "checkout_handoff":
        validate_checkout_handoff(payload)
        return str(payload["handoff_checksum"]), CHECKOUT_HANDOFF_SCHEMA_VERSION
    if kind == "checkout_reconciliation":
        validate_checkout_reconciliation_report(payload)
        return (
            str(payload["report_checksum"]),
            CHECKOUT_RECONCILIATION_REPORT_SCHEMA_VERSION,
        )
    if kind == "false_block_review":
        validate_false_block_review(payload, expected_batch_id=batch_id)
        return str(payload["review_id"]), CHECKOUT_FALSE_BLOCK_REVIEW_SCHEMA_VERSION
    raise CheckoutGuardError("CHECKOUT_TELEMETRY_SOURCE_KIND", kind)


def _validate_intent(payload: Mapping[str, object], path: str) -> None:
    _require_exact_keys(
        payload,
        {
            "schema_version",
            "intent_id",
            "task_id",
            "thread_id",
            "actor",
            "operation_class",
            "base_commit",
            "owned_paths",
            "shared_paths",
            "workspace_identity",
            "observed_dirty_paths",
            "known_unrelated_exclusions",
            "task_source_cutover",
            "production_effect",
            "broker_action",
            "created_at",
        },
        "CHECKOUT_TELEMETRY_INTENT_FIELDS",
    )
    if payload.get("schema_version") != CHECKOUT_INTENT_SCHEMA_VERSION:
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_INTENT_SCHEMA",
            path,
        )
    intent_id = _identifier(payload.get("intent_id"), "intent.intent_id")
    if PurePosixPath(path).stem != intent_id:
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_INTENT_PATH", path)
    _required_text(payload.get("task_id"), "intent.task_id")
    _required_text(payload.get("thread_id"), "intent.thread_id")
    _required_text(payload.get("actor"), "intent.actor")
    try:
        CheckoutOperationClass(str(payload.get("operation_class")))
    except ValueError as exc:
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_INTENT_OPERATION",
            str(payload.get("operation_class")),
        ) from exc
    _required_text(payload.get("base_commit"), "intent.base_commit")
    _strings(payload.get("owned_paths"), "intent.owned_paths")
    _strings(payload.get("shared_paths"), "intent.shared_paths")
    _strings(payload.get("observed_dirty_paths"), "intent.observed_dirty_paths")
    identity = _mapping(payload.get("workspace_identity"), "intent.workspace_identity")
    _required_text(identity.get("workspace_id"), "intent.workspace_identity.workspace_id")
    _timestamp(payload.get("created_at"), "intent.created_at")
    if payload.get("task_source_cutover") is not False:
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_INTENT_CUTOVER", intent_id)
    if payload.get("production_effect") != "none" or payload.get("broker_action") != "none":
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_INTENT_EFFECT", intent_id)


def _validate_supervised_run_source(payload: Mapping[str, object]) -> None:
    if payload.get("schema_version") != RUN_REPORT_SCHEMA_VERSION:
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_SUPERVISED_SCHEMA",
            str(payload.get("schema_version")),
        )
    if payload.get("status") != "PASS":
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_SUPERVISED_STATUS",
            str(payload.get("status")),
        )
    body = dict(payload)
    report_id = body.pop("report_id", None)
    if report_id != f"supervised-run-{_canonical_sha256(body)[:20]}":
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_SUPERVISED_ID",
            str(report_id),
        )
    required_false = (
        "human_coordinator_approved",
        "merge_allowed",
        "canonical_source_cutover",
        "task_governance_status_mutated",
        "automatic_commit_performed",
        "automatic_merge_performed",
        "automatic_push_performed",
        "automatic_pr_performed",
    )
    if any(payload.get(field) is not False for field in required_false):
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_SUPERVISED_PERMISSION",
            str(report_id),
        )
    if payload.get("production_effect") != "none" or payload.get("broker_action") != "none":
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_SUPERVISED_EFFECT",
            str(report_id),
        )


def _block_observations(
    intents: Sequence[Mapping[str, object]],
    events: Sequence[LeaseEvent],
) -> list[dict[str, object]]:
    observations: list[dict[str, object]] = []
    for intent in intents:
        declared = tuple(
            str(value)
            for value in (
                *_list(intent.get("owned_paths"), "intent.owned_paths"),
                *_list(intent.get("shared_paths"), "intent.shared_paths"),
            )
        )
        operation = CheckoutOperationClass(str(intent["operation_class"]))
        dirty = tuple(
            str(value)
            for value in _list(
                intent.get("observed_dirty_paths"),
                "intent.observed_dirty_paths",
            )
        )
        unattributed = (
            dirty
            if operation
            not in {
                CheckoutOperationClass.DOMAIN_MUTATION,
                CheckoutOperationClass.SHARED_MUTATION,
            }
            else tuple(
                path
                for path in dirty
                if not any(_paths_overlap(path, claimed) for claimed in declared)
            )
        )
        for path in unattributed:
            observation_id = (
                f"intent:{intent['intent_id']}:unattributed:"
                f"{hashlib.sha256(path.encode('utf-8')).hexdigest()[:16]}"
            )
            observations.append(
                {
                    "observation_id": observation_id,
                    "observation_kind": "unattributed_dirty",
                    "intent_id": intent["intent_id"],
                    "lease_id": None,
                    "reason_code": f"CHECKOUT_DIRTY_UNATTRIBUTED:{path}",
                    "path": path,
                    "occurred_at": intent["created_at"],
                }
            )
    for event in events:
        for reason_code in event.reason_codes:
            release_prefix = "CHECKOUT_RELEASE_DIRTY_UNATTRIBUTED:"
            if event.to_state == "RELEASED" and reason_code.startswith(
                release_prefix
            ):
                path = reason_code.removeprefix(release_prefix)
                observations.append(
                    {
                        "observation_id": (
                            f"lease:{event.lease.lease_id}:{event.event_id}:"
                            f"{hashlib.sha256(reason_code.encode('utf-8')).hexdigest()[:16]}"
                        ),
                        "observation_kind": "unattributed_dirty",
                        "intent_id": event.lease.change_id.removeprefix("checkout:"),
                        "lease_id": event.lease.lease_id,
                        "reason_code": reason_code,
                        "path": path,
                        "occurred_at": event.occurred_at,
                    }
                )
                continue
            if event.to_state != "BLOCKED":
                continue
            observations.append(
                {
                    "observation_id": (
                        f"lease:{event.lease.lease_id}:{event.event_id}:"
                        f"{hashlib.sha256(reason_code.encode('utf-8')).hexdigest()[:16]}"
                    ),
                    "observation_kind": "lease_conflict",
                    "intent_id": event.lease.change_id.removeprefix("checkout:"),
                    "lease_id": event.lease.lease_id,
                    "reason_code": reason_code,
                    "path": None,
                    "occurred_at": event.occurred_at,
                }
            )
    return sorted(observations, key=lambda row: str(row["observation_id"]))


def _wait_observations(events: Sequence[LeaseEvent]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    by_lease = _events_by_lease(events)
    for lease_id, records in sorted(by_lease.items()):
        requested = next(
            (event for event in records if event.to_state == "REQUESTED"),
            None,
        )
        resolved = next(
            (
                event
                for event in records
                if event.to_state in {"ACTIVE", "BLOCKED"}
            ),
            None,
        )
        if requested is None or resolved is None:
            continue
        rows.append(
            {
                "lease_id": lease_id,
                "task_id": resolved.lease.task_id,
                "change_id": resolved.lease.change_id,
                "resolution": resolved.to_state,
                "reason_codes": list(resolved.reason_codes),
                "requested_at": requested.occurred_at,
                "resolved_at": resolved.occurred_at,
                "wait_seconds": _seconds_between(
                    requested.occurred_at,
                    resolved.occurred_at,
                ),
            }
        )
    return rows


def _lease_hold_observations(
    events: Sequence[LeaseEvent],
    cutoff: datetime,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    by_lease = _events_by_lease(events)
    for lease_id, records in sorted(by_lease.items()):
        acquired = next(
            (event for event in records if event.to_state == "ACTIVE"),
            None,
        )
        if acquired is None:
            continue
        terminal = next(
            (
                event
                for event in reversed(records)
                if event.to_state in {"RELEASED", "EXPIRED"}
            ),
            None,
        )
        end = cutoff if terminal is None else _timestamp(terminal.occurred_at, "occurred_at")
        start = _timestamp(acquired.occurred_at, "occurred_at")
        if end < start:
            raise CheckoutGuardError(
                "CHECKOUT_TELEMETRY_CUTOFF_BEFORE_ACQUIRE",
                lease_id,
            )
        rows.append(
            {
                "lease_id": lease_id,
                "task_id": acquired.lease.task_id,
                "change_id": acquired.lease.change_id,
                "state_at_cutoff": (
                    records[-1].to_state if terminal is None else terminal.to_state
                ),
                "complete": terminal is not None,
                "acquired_at": acquired.occurred_at,
                "ended_at": end.isoformat(),
                "held_seconds": round((end - start).total_seconds(), 6),
            }
        )
    return rows


def _events_by_lease(
    events: Sequence[LeaseEvent],
) -> dict[str, list[LeaseEvent]]:
    by_lease: dict[str, list[LeaseEvent]] = {}
    for event in events:
        by_lease.setdefault(event.lease.lease_id, []).append(event)
    for records in by_lease.values():
        records.sort(key=lambda event: _timestamp(event.occurred_at, "occurred_at"))
    return by_lease


def _duration_summary(values: Iterable[float]) -> dict[str, object]:
    rows = [round(float(value), 6) for value in values]
    return {
        "count": len(rows),
        "minimum": None if not rows else min(rows),
        "maximum": None if not rows else max(rows),
        "total": round(sum(rows), 6),
    }


def _nonnegative_number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_NUMBER", field)
    return float(value)


def _load_rollup_snapshot(
    project_root: Path,
    path: Path,
    *,
    policy_path: Path,
) -> tuple[dict[str, object], dict[str, object]]:
    absolute = path.resolve()
    if not absolute.is_relative_to(project_root):
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_ROLLUP_SOURCE_OUTSIDE",
            str(absolute),
        )
    relative = absolute.relative_to(project_root).as_posix()
    payload = _load_json_source(
        project_root,
        relative,
        known_unrelated_paths=(),
    )
    validate_checkout_telemetry_snapshot(
        payload,
        project_root=project_root,
        policy_path=policy_path,
    )
    return (
        {
            "path": relative,
            "sha256": _sha256_path(absolute),
            "snapshot_id": payload["snapshot_id"],
            "batch_id": payload["batch_id"],
        },
        dict(payload),
    )


def _metric_int(snapshot: Mapping[str, object], key: str) -> int:
    metrics = _mapping(snapshot.get("metrics"), "snapshot.metrics")
    value = metrics.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_METRIC", key)
    return value


def _validate_source_record(
    value: object,
    *,
    field: str,
) -> dict[str, object]:
    row = _mapping(value, field)
    _require_exact_keys(
        row,
        {"kind", "path", "sha256", "schema_version", "source_id"},
        "CHECKOUT_TELEMETRY_SOURCE_FIELDS",
    )
    kind = _required_text(row.get("kind"), f"{field}.kind")
    if kind not in _SOURCE_KINDS:
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_SOURCE_KIND", kind)
    path = _portable_path(row.get("path"), f"{field}.path")
    sha256 = _sha256(row.get("sha256"), f"{field}.sha256")
    schema_version = _required_text(
        row.get("schema_version"),
        f"{field}.schema_version",
    )
    source_id = _required_text(row.get("source_id"), f"{field}.source_id")
    return {
        "kind": kind,
        "path": path,
        "sha256": sha256,
        "schema_version": schema_version,
        "source_id": source_id,
    }


def _source_sort_key(source: Mapping[str, object]) -> tuple[str, str]:
    return str(source["kind"]), str(source["path"]).casefold()


def _contained_source_path(
    project_root: Path,
    relative: str,
    *,
    known_unrelated_paths: Sequence[str],
) -> Path:
    portable = _portable_path(relative, "source.path")
    if portable.casefold() in {path.casefold() for path in known_unrelated_paths}:
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_KNOWN_UNRELATED_READ",
            portable,
        )
    path = project_root / Path(*PurePosixPath(portable).parts)
    _assert_no_reparse_components(project_root, portable)
    resolved = path.resolve()
    if not resolved.is_relative_to(project_root) or not resolved.is_file():
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_SOURCE_MISSING",
            portable,
        )
    return resolved


def _load_json_source(
    project_root: Path,
    relative: str,
    *,
    known_unrelated_paths: Sequence[str],
) -> dict[str, Any]:
    path = _contained_source_path(
        project_root,
        relative,
        known_unrelated_paths=known_unrelated_paths,
    )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_SOURCE_INVALID",
            f"{relative}:{exc}",
        ) from exc
    return _mapping(payload, relative)


def _assert_no_reparse_components(project_root: Path, relative: str) -> None:
    current = project_root
    for part in PurePosixPath(relative).parts:
        current = current / part
        try:
            metadata = os.lstat(current)
        except FileNotFoundError:
            continue
        attributes = getattr(metadata, "st_file_attributes", 0)
        reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
        if stat.S_ISLNK(metadata.st_mode) or bool(
            reparse_flag and attributes & reparse_flag
        ):
            raise CheckoutGuardError(
                "CHECKOUT_TELEMETRY_SOURCE_REPARSE",
                relative,
            )


def _assert_governed_output_path(
    path: Path,
    project_root: Path,
    policy: CheckoutTelemetryPolicy,
) -> None:
    output_root = (project_root / policy.output_root).resolve()
    resolved = path.resolve()
    if not resolved.is_relative_to(output_root):
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_OUTPUT_OUTSIDE",
            str(resolved),
        )
    relative = resolved.relative_to(project_root).as_posix()
    _assert_no_reparse_components(project_root, relative)


def _write_immutable_json(
    path: Path,
    payload: Mapping[str, object],
    error_code: str,
) -> None:
    raw = (
        json.dumps(
            dict(payload),
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8")
        + b"\n"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != raw:
            raise CheckoutGuardError(error_code, str(path))
        return
    try:
        with path.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        if path.read_bytes() != raw:
            raise CheckoutGuardError(error_code, str(path)) from None


def _paths_overlap(first: str, second: str) -> bool:
    left = tuple(part.casefold() for part in PurePosixPath(first).parts)
    right = tuple(part.casefold() for part in PurePosixPath(second).parts)
    common = min(len(left), len(right))
    return left[:common] == right[:common]


def _seconds_between(start: str, end: str) -> float:
    first = _timestamp(start, "start")
    second = _timestamp(end, "end")
    if second < first:
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_TIME_ORDER",
            f"{start}>{end}",
        )
    return round((second - first).total_seconds(), 6)


def _timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_TIMESTAMP", field)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_TIMESTAMP", field) from exc
    if parsed.tzinfo is None:
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_TIMESTAMP", field)
    return parsed.astimezone(UTC)


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_TIMESTAMP",
            "timezone required",
        )
    return value.astimezone(UTC)


def _payload_checksum(payload: Mapping[str, object], field: str) -> str:
    return _canonical_sha256({key: value for key, value in payload.items() if key != field})


def _canonical_sha256(value: object) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_SHA256", field)
    return value


def _portable_path(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_PATH", field)
    candidate = PurePosixPath(value.replace("\\", "/"))
    if (
        candidate.is_absolute()
        or ".." in candidate.parts
        or "." in candidate.parts
        or candidate.as_posix() != value.replace("\\", "/")
    ):
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_PATH", f"{field}:{value}")
    return candidate.as_posix()


def _identifier(value: object, field: str) -> str:
    if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_IDENTIFIER", field)
    return value


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_TEXT", field)
    return value


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_POSITIVE_INT", field)
    return value


def _strings(value: object, field: str) -> tuple[str, ...]:
    rows = _list(value, field)
    if any(not isinstance(row, str) or not row for row in rows):
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_STRINGS", field)
    result = tuple(str(row) for row in rows)
    if len(result) != len(set(result)):
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_STRINGS_DUPLICATE", field)
    return result


def _list(value: object, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_LIST", field)
    return value


def _mapping(value: object, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise CheckoutGuardError("CHECKOUT_TELEMETRY_MAPPING", field)
    return dict(value)


def _require_exact_keys(
    value: Mapping[str, object],
    expected: set[str],
    code: str,
) -> None:
    if set(value) != expected:
        raise CheckoutGuardError(
            code,
            f"expected={sorted(expected)} actual={sorted(value)}",
        )
