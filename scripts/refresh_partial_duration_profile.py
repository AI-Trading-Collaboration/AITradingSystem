from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts import run_validation_tier as validation_tier  # noqa: E402

PROFILE_SCHEMA_VERSION = "test_runtime_profile.v1"
DURATION_PROFILE_SCHEMA_VERSION = "arch_004g2_full_duration_profile.v1"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def _mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be a mapping")
    return value


def _load_json(path: Path, *, label: str) -> tuple[dict[str, object], bytes]:
    try:
        raw_bytes = path.read_bytes()
        payload = json.loads(
            raw_bytes,
            object_pairs_hook=validation_tier._reject_duplicate_json_keys,
            parse_constant=validation_tier._reject_non_finite_json_constant,
        )
    except (OSError, ValueError) as exc:
        raise ValueError(f"{label} could not be read: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{label} root must be a mapping")
    return payload, raw_bytes


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _require_equal(actual: object, expected: object, *, label: str) -> None:
    if type(actual) is not type(expected) or actual != expected:
        raise ValueError(f"{label} must equal {expected!r}; observed {actual!r}")


def _require_projection_equal(actual: object, expected: object, *, label: str) -> None:
    """JSON equality that cannot substitute booleans for measured counts."""
    if isinstance(expected, dict):
        if not isinstance(actual, dict) or actual.keys() != expected.keys():
            raise ValueError(f"{label} keys differ from derived evidence")
        for key, value in expected.items():
            _require_projection_equal(actual[key], value, label=f"{label}.{key}")
    elif isinstance(expected, list):
        if not isinstance(actual, list) or len(actual) != len(expected):
            raise ValueError(f"{label} list differs from derived evidence")
        for index, value in enumerate(expected):
            _require_projection_equal(actual[index], value, label=f"{label}[{index}]")
    else:
        _require_equal(actual, expected, label=label)


def _utc_timestamp(value: object, *, label: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be a UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label} must be a UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{label} must include the UTC timezone")
    return parsed


def _recorded_locator(value: object, *, original_root: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("recorded artifact locator must be a non-empty string")
    normalized = value.replace("\\", "/")
    if any(part in {".", ".."} for part in normalized.split("/")):
        raise ValueError("recorded artifact locator contains dot segments")
    # Drive/root-relative paths must not become relative children of original_root.
    if value.startswith(("/", "\\")) or ":" in value:
        return validation_tier.normalize_absolute_runtime_locator(value)
    return validation_tier.normalize_absolute_runtime_locator(f"{original_root}/{value}")


def _git_manifest_bytes(commit: str, relative_path: str) -> bytes:
    """Read only fixed manifest blobs at the recorded exact source commit."""
    if GIT_SHA_RE.fullmatch(commit) is None:
        raise ValueError("source commit must be an exact lowercase Git SHA")
    if relative_path not in {
        validation_tier.FULL_DURATION_PROFILE_MANIFEST,
        validation_tier.FULL_TEST_MANIFEST,
    }:
        raise ValueError("historical manifest path is not an allowed source contract")
    object_type = subprocess.run(
        ["git", "-C", str(PROJECT_ROOT), "cat-file", "-t", commit],
        capture_output=True,
        check=False,
    )
    if object_type.returncode != 0 or object_type.stdout.strip() != b"commit":
        raise ValueError("source Git identity must resolve to a commit object")
    result = subprocess.run(
        ["git", "-C", str(PROJECT_ROOT), "show", f"{commit}:{relative_path}"],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0 or not result.stdout:
        raise ValueError(f"original source manifest is unavailable: {commit}:{relative_path}")
    return result.stdout


def _validate_source_capture(
    summary: Mapping[str, object],
    *,
    profile_bytes: bytes,
    source_profile_path: Path,
    source_summary_path: Path,
) -> tuple[dict[str, object], dict[str, object]]:
    for path, filename in (
        (source_profile_path, "test_runtime_profile.json"),
        (source_summary_path, "test_runtime_summary.json"),
    ):
        _repo_relative(path)
        if path.name != filename:
            raise ValueError(f"source artifact must use canonical filename {filename}")
    if source_profile_path.resolve().parent != source_summary_path.resolve().parent:
        raise ValueError("source summary and profile must be canonical siblings")
    for field, expected in (
        ("schema_version", 1),
        ("report_type", "test_runtime_summary"),
        ("tier", "full"),
        ("requested_tier", "full"),
        ("resolved_tier", "full"),
        ("suite_family", "full_pytest"),
        ("print_only", False),
        ("benchmark_mode", False),
        ("extra_pytest_args", []),
        ("pytest_output_captured", True),
        ("cached_data_mutated", False),
        ("production_state_mutated", False),
        ("broker_action_allowed", False),
    ):
        _require_equal(summary.get(field), expected, label=f"runtime summary {field}")
    commit = summary.get("git_commit")
    if not isinstance(commit, str) or GIT_SHA_RE.fullmatch(commit) is None:
        raise ValueError("runtime summary git_commit must be an exact lowercase SHA")
    environment = _mapping(summary.get("environment_summary"), label="environment_summary")
    original_root = validation_tier.normalize_absolute_runtime_locator(
        environment.get("working_directory")
    )
    _require_equal(
        original_root,
        validation_tier.normalize_absolute_runtime_locator(str(PROJECT_ROOT.resolve())),
        label="source original working directory",
    )
    for field, path in (
        ("runtime_profile_path", source_profile_path),
        ("summary_path", source_summary_path),
        ("artifact_dir", source_summary_path.parent),
    ):
        _require_equal(
            _recorded_locator(summary.get(field), original_root=original_root),
            validation_tier.normalize_absolute_runtime_locator(str(path.resolve())),
            label=f"runtime summary {field}",
        )
    provenance = _mapping(summary.get("validation_provenance"), label="validation_provenance")
    errors = validation_tier.validate_full_provenance(provenance)
    if errors:
        raise ValueError("source Full provenance is invalid: " + "; ".join(errors))
    publication = _mapping(summary.get("publication_transaction"), label="publication_transaction")
    for field, expected in (
        ("schema_version", "integration_publication_fence.v1"),
        ("status", "PASS"),
        ("phase", "FULL_DISPATCHED"),
        ("candidate_sha", commit),
        ("task_id", provenance["task_id"]),
        ("production_effect", "none"),
        ("broker_action", "none"),
    ):
        _require_equal(publication.get(field), expected, label=f"source publication {field}")
    for field, pattern in (
        ("expected_main_sha", GIT_SHA_RE),
        ("transaction_sha256", SHA256_RE),
    ):
        value = publication.get(field)
        if not isinstance(value, str) or pattern.fullmatch(value) is None:
            raise ValueError(f"source publication {field} is invalid")
    transaction_id = publication.get("transaction_id")
    if (
        not isinstance(transaction_id, str)
        or validation_tier.PROVENANCE_IDENTIFIER_RE.fullmatch(transaction_id) is None
    ):
        raise ValueError("source publication transaction_id is invalid")
    _require_equal(
        publication.get("transaction_path"),
        "outputs/architecture/arch_005_integration_publication_fence/transactions/"
        f"{transaction_id}/transaction.json",
        label="source publication transaction_path",
    )
    readiness = _mapping(publication.get("pre_dispatch_readiness"), label="pre_dispatch_readiness")
    for field, expected in (
        ("schema_version", "full_validation_readiness.v1"),
        ("status", "PASS"),
        ("candidate_sha", commit),
        ("full_dispatch_ready", True),
        ("blockers", []),
        ("production_effect", "none"),
        ("broker_action", "none"),
        ("research_dispatch_allowed", False),
        ("dq_validation_executed", False),
    ):
        _require_equal(readiness.get(field), expected, label=f"source readiness {field}")
    for field in ("target_root", "inspection_code_root"):
        _require_equal(
            validation_tier.normalize_absolute_runtime_locator(readiness.get(field)),
            original_root,
            label=f"source readiness {field}",
        )
    command = summary.get("command")
    if not isinstance(command, list) or not command or not isinstance(command[0], str):
        raise ValueError("source Full command must be a recorded argument list")
    _require_projection_equal(
        command,
        validation_tier.build_command(
            "full",
            python_executable=command[0],
            repo_root=PROJECT_ROOT,
            workers="16",
            dist="loadfile",
        ),
        label="source Full command",
    )
    _require_projection_equal(
        summary.get("safety_boundary"),
        {
            "production_effect": "none",
            "strategy_logic_changed": False,
            "cached_data_mutated": False,
            "production_state_mutated": False,
            "broker_action_allowed": False,
            "broker_action_taken": False,
        },
        label="source Full safety_boundary",
    )
    duration_bytes = _git_manifest_bytes(commit, validation_tier.FULL_DURATION_PROFILE_MANIFEST)
    test_bytes = _git_manifest_bytes(commit, validation_tier.FULL_TEST_MANIFEST)
    duration_locator = _recorded_locator(
        validation_tier.FULL_DURATION_PROFILE_MANIFEST, original_root=original_root
    )
    artifacts = summary.get("input_artifacts")
    if not isinstance(artifacts, list):
        raise ValueError("source summary input_artifacts must be a list")
    matches = [
        _mapping(row, label="input artifact")
        for row in artifacts
        if _recorded_locator(
            _mapping(row, label="input artifact").get("path"), original_root=original_root
        )
        == duration_locator
    ]
    if len(matches) != 1:
        raise ValueError("source summary must inventory original duration manifest exactly once")
    for field, expected in (
        ("exists", True),
        ("sha256", _sha256(duration_bytes)),
        ("size_bytes", len(duration_bytes)),
    ):
        _require_equal(matches[0].get(field), expected, label=f"original manifest {field}")
    checksums = _mapping(summary.get("input_checksums"), label="input_checksums")
    checksum_matches = [
        value
        for key, value in checksums.items()
        if _recorded_locator(key, original_root=original_root) == duration_locator
    ]
    _require_equal(checksum_matches, [_sha256(duration_bytes)], label="original manifest checksum")
    captured = validation_tier.CapturedDurationManifest(duration_bytes, duration_locator)
    profile = validation_tier.validate_captured_runtime_profile(
        profile_bytes,
        duration_manifest=captured,
        full_test_manifest_bytes=test_bytes,
        expected_validation_provenance=provenance,
        pytest_exitstatus=0,
        expected_worker_count=16,
        expected_dist="loadfile",
        formal_selection_eligible=True,
    )
    if profile.get("profile_status") != "PASS":
        raise ValueError(f"source runtime profile failed strict replay: {profile.get('warnings')}")
    summary_start = _utc_timestamp(summary.get("started_at_utc"), label="summary start")
    summary_end = _utc_timestamp(summary.get("ended_at_utc"), label="summary end")
    profile_start = _utc_timestamp(profile.get("started_at_utc"), label="profile start")
    profile_end = _utc_timestamp(profile.get("ended_at_utc"), label="profile end")
    if not summary_start <= profile_start < profile_end <= summary_end:
        raise ValueError("source summary UTC window must contain its runtime profile window")
    summary_elapsed = summary.get("elapsed_seconds")
    if (
        isinstance(summary_elapsed, bool)
        or not isinstance(summary_elapsed, (int, float))
        or not math.isfinite(summary_elapsed)
        or summary_elapsed <= 0
    ):
        raise ValueError("source summary elapsed_seconds must be positive and finite")
    _require_projection_equal(
        profile.get("validation_provenance"), dict(provenance), label="source provenance binding"
    )
    derived = validation_tier._summarize_runtime_profile(profile, final_path=source_profile_path)
    _require_projection_equal(
        summary.get("runtime_profile_summary"),
        derived["runtime_profile_summary"],
        label="runtime_profile_summary",
    )
    return profile, {
        "contract": "captured_full_runtime_strict_replay.v1",
        "source_git_commit": commit,
        "duration_manifest_path": validation_tier.FULL_DURATION_PROFILE_MANIFEST,
        "duration_manifest_sha256": _sha256(duration_bytes),
        "duration_manifest_size_bytes": len(duration_bytes),
        "full_test_manifest_path": validation_tier.FULL_TEST_MANIFEST,
        "full_test_manifest_sha256": _sha256(test_bytes),
        "full_test_manifest_size_bytes": len(test_bytes),
        "original_working_directory": original_root,
        "technical_validation_state": "PASS",
    }


def _repo_relative(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise ValueError(f"source artifact must remain inside project root: {resolved}") from exc


def _validate_test_path(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("runtime profile file path is required")
    normalized = value.strip().replace("\\", "/").split("::", 1)[0]
    parts = PurePosixPath(normalized).parts
    if (
        not normalized.startswith("tests/")
        or PurePosixPath(normalized).is_absolute()
        or "." in parts
        or ".." in parts
    ):
        raise ValueError(f"runtime profile file path is out of scope: {value}")
    return normalized


def _validate_source_profile(
    payload: Mapping[str, object],
    *,
    expected_nodes: int,
    expected_files: int,
) -> list[dict[str, object]]:
    for field, expected in (
        ("schema_version", PROFILE_SCHEMA_VERSION),
        ("profile_status", "PASS"),
        ("telemetry_status", "PASS"),
        ("performance_evidence_status", "PASS"),
        ("validation_provenance_binding_status", "PASS"),
        ("pytest_exitstatus", 0),
        ("worker_count", 16),
        ("node_count", expected_nodes),
        ("file_count", expected_files),
        ("production_effect", "none"),
        ("strategy_logic_changed", False),
        ("broker_action_taken", False),
    ):
        _require_equal(payload.get(field), expected, label=f"runtime profile {field}")

    scheduler = _mapping(payload.get("scheduler"), label="runtime profile scheduler")
    for field, expected in (
        ("applied", True),
        ("fallback", False),
        ("expected_worker_count", 16),
        ("xdist_dist", "loadfile"),
        ("formal_full_selection_eligible", True),
    ):
        _require_equal(scheduler.get(field), expected, label=f"runtime profile scheduler.{field}")

    collection = _mapping(payload.get("collection"), label="runtime profile collection")
    for field, expected in (
        ("complete", True),
        ("count", expected_nodes),
        ("observed_worker_count", 16),
    ):
        _require_equal(collection.get(field), expected, label=f"runtime profile collection.{field}")
    _require_equal(
        collection.get("duplicate_nodeids"),
        [],
        label="runtime profile collection.duplicate_nodeids",
    )

    raw_rows = payload.get("files")
    if not isinstance(raw_rows, list) or len(raw_rows) != expected_files:
        raise ValueError(
            "runtime profile files must contain exactly "
            f"{expected_files} rows; observed "
            f"{len(raw_rows) if isinstance(raw_rows, list) else None}"
        )
    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    node_total = 0
    for source_index, raw_row in enumerate(raw_rows):
        row = _mapping(raw_row, label="runtime profile file row")
        path = _validate_test_path(row.get("path"))
        if path in seen:
            raise ValueError(f"runtime profile contains duplicate file path: {path}")
        seen.add(path)
        node_count = row.get("node_count")
        duration = row.get("duration_seconds")
        if isinstance(node_count, bool) or not isinstance(node_count, int) or node_count < 1:
            raise ValueError(f"runtime profile node_count is invalid for {path}")
        if (
            isinstance(duration, bool)
            or not isinstance(duration, (int, float))
            or not math.isfinite(float(duration))
            or float(duration) <= 0.0
        ):
            raise ValueError(f"runtime profile duration_seconds is invalid for {path}")
        node_total += node_count
        rows.append(
            {
                "path": path,
                "node_count": node_count,
                "observed_seconds": float(duration),
                "_source_index": source_index,
            }
        )
    _require_equal(node_total, expected_nodes, label="runtime profile file node_count total")
    collection_nodeids = collection.get("nodeids")
    if not isinstance(collection_nodeids, list):
        raise ValueError("runtime profile collection nodeids are required")
    first_seen_files = list(
        dict.fromkeys(_validate_test_path(nodeid) for nodeid in collection_nodeids)
    )
    _require_equal(
        [row["path"] for row in rows],
        first_seen_files,
        label="runtime profile file first-seen order",
    )
    rows.sort(key=lambda row: (-float(row["observed_seconds"]), int(row["_source_index"])))
    for row in rows:
        row.pop("_source_index")
    return rows


def _validate_summary_binding(
    payload: Mapping[str, object],
    *,
    source_profile_path: Path,
    source_sha256: str,
    source_size_bytes: int,
    expected_nodes: int,
    expected_files: int,
) -> str:
    for field, expected in (
        ("status", "PASS"),
        ("exit_code", 0),
        ("runtime_profile_status", "PASS"),
        ("validation_provenance_status", "PASS"),
        ("dist", "loadfile"),
        ("formal_full_selection_eligible", True),
        ("production_effect", "none"),
        ("strategy_logic_changed", False),
        ("broker_action_taken", False),
    ):
        _require_equal(payload.get(field), expected, label=f"runtime summary {field}")
    if not (
        type(payload.get("workers")) is int
        and payload["workers"] == 16
        or type(payload.get("workers")) is str
        and payload["workers"] == "16"
    ):
        raise ValueError("runtime summary workers must equal 16")

    original_root = validation_tier.normalize_absolute_runtime_locator(str(PROJECT_ROOT.resolve()))
    expected_profile = validation_tier.normalize_absolute_runtime_locator(
        str(source_profile_path.resolve())
    )
    _require_equal(
        _recorded_locator(payload.get("runtime_profile_path"), original_root=original_root),
        expected_profile,
        label="runtime summary runtime_profile_path",
    )

    summary = _mapping(payload.get("runtime_profile_summary"), label="runtime_profile_summary")
    for field, expected in (
        ("collection_count", expected_nodes),
        ("node_count", expected_nodes),
        ("file_count", expected_files),
        ("worker_count", 16),
        ("performance_evidence_status", "PASS"),
        ("telemetry_status", "PASS"),
        ("validation_provenance_binding_status", "PASS"),
        ("scheduler_applied", True),
        ("scheduler_fallback", False),
        ("formal_full_selection_eligible", True),
    ):
        _require_equal(summary.get(field), expected, label=f"runtime_profile_summary.{field}")

    output_artifacts = payload.get("output_artifacts")
    if not isinstance(output_artifacts, list):
        raise ValueError("runtime summary output_artifacts must be a list")
    matches = []
    for raw_artifact in output_artifacts:
        artifact = _mapping(raw_artifact, label="runtime summary output artifact")
        artifact_path = artifact.get("path")
        if _recorded_locator(artifact_path, original_root=original_root) == expected_profile:
            matches.append(artifact)
    if len(matches) != 1:
        raise ValueError("runtime summary must inventory the exact runtime profile once")
    artifact = matches[0]
    for field, expected in (
        ("exists", True),
        ("sha256", source_sha256),
        ("size_bytes", source_size_bytes),
    ):
        _require_equal(artifact.get(field), expected, label=f"runtime profile inventory {field}")

    git_commit = payload.get("git_commit")
    if not isinstance(git_commit, str) or GIT_SHA_RE.fullmatch(git_commit) is None:
        raise ValueError("runtime summary git_commit must be a 40-character lowercase SHA")
    return git_commit


def build_partial_duration_manifest(
    *,
    source_profile_path: Path,
    source_summary_path: Path,
    profile_id: str,
    version: int,
    expected_nodes: int,
    expected_files: int,
) -> dict[str, object]:
    if not isinstance(profile_id, str) or not profile_id.strip():
        raise ValueError("profile_id is required")
    if type(version) is not int or version < 1:
        raise ValueError("version must be a positive integer")
    if any(type(value) is not int or value < 1 for value in (expected_nodes, expected_files)):
        raise ValueError("expected node and file counts must be positive")

    _, profile_bytes = _load_json(source_profile_path, label="runtime profile")
    summary, summary_bytes = _load_json(source_summary_path, label="runtime summary")
    profile, capture_proof = _validate_source_capture(
        summary,
        profile_bytes=profile_bytes,
        source_profile_path=source_profile_path,
        source_summary_path=source_summary_path,
    )
    source_sha256 = _sha256(profile_bytes)
    rows = _validate_source_profile(
        profile,
        expected_nodes=expected_nodes,
        expected_files=expected_files,
    )
    git_commit = _validate_summary_binding(
        summary,
        source_profile_path=source_profile_path,
        source_sha256=source_sha256,
        source_size_bytes=len(profile_bytes),
        expected_nodes=expected_nodes,
        expected_files=expected_files,
    )
    elapsed_seconds = profile.get("elapsed_seconds")
    if (
        isinstance(elapsed_seconds, bool)
        or not isinstance(elapsed_seconds, (int, float))
        or not math.isfinite(float(elapsed_seconds))
        or float(elapsed_seconds) <= 0.0
    ):
        raise ValueError("runtime profile elapsed_seconds must be positive and finite")

    return {
        "schema_version": DURATION_PROFILE_SCHEMA_VERSION,
        "profile_id": profile_id,
        "status": "PARTIAL_SEED",
        "owner": "validation_operations",
        "version": version,
        "source": {
            "artifact_path": _repo_relative(source_profile_path),
            "artifact_sha256": source_sha256,
            "artifact_size_bytes": len(profile_bytes),
            "summary_artifact_path": _repo_relative(source_summary_path),
            "summary_artifact_sha256": _sha256(summary_bytes),
            "summary_artifact_size_bytes": len(summary_bytes),
            "strict_replay": capture_proof,
            "tier": "full",
            "workers": 16,
            "dist": "loadfile",
            "elapsed_seconds": float(elapsed_seconds),
            "git_commit": git_commit,
            "profile_status": "PASS",
            "telemetry_status": "PASS",
            "performance_evidence_status": "PASS",
            "validation_provenance_status": "PASS",
            "pytest_exitstatus": 0,
        },
        "partial_seed": {
            "enabled": True,
            "source_duration_row_count": expected_files,
            "aggregated_file_count": expected_files,
            "coverage_limitation": (
                f"The source Full exactly observed {expected_nodes:,} nodes in "
                f"{expected_files:,} files at its source commit. Subsequent changes may alter "
                "collection identity, so preserve every observed file duration as an advisory "
                "seed. Files absent from this seed retain stable first-seen order after seeded "
                "files. Existing seeded paths retain historical weights even when their node "
                "collection changes; those weights remain advisory until a later natural Full "
                "refreshes them."
            ),
        },
        "review": {
            "use_scope": "advisory_test_file_scheduling_only",
            "stable_improvement_claimed": False,
            "conditions": [
                "Use only with the full tier and pytest-xdist loadfile distribution.",
                "Preserve every nodeid and the order of nodes within each test file.",
                "Keep files absent from this seed in stable collection order after profiled files.",
                "Treat missing, duplicate, worker-mismatched, or invalid evidence as an "
                "explicit stock loadfile fallback.",
                "Do not promote fallback, incomplete telemetry, or this seed as stable "
                "improvement evidence.",
            ],
        },
        "files": rows,
    }


def _serialized_yaml(payload: Mapping[str, object]) -> bytes:
    rendered = yaml.safe_dump(
        dict(payload),
        allow_unicode=True,
        sort_keys=False,
        width=100,
    )
    return rendered.encode("utf-8")


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Refresh the advisory Full duration seed from one validated runtime profile."
    )
    parser.add_argument("--source-profile", type=Path, required=True)
    parser.add_argument("--source-summary", type=Path, required=True)
    parser.add_argument("--profile-id", required=True)
    parser.add_argument("--version", type=int, required=True)
    parser.add_argument("--expected-nodes", type=int, required=True)
    parser.add_argument("--expected-files", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    manifest = build_partial_duration_manifest(
        source_profile_path=args.source_profile,
        source_summary_path=args.source_summary,
        profile_id=args.profile_id,
        version=args.version,
        expected_nodes=args.expected_nodes,
        expected_files=args.expected_files,
    )
    serialized = _serialized_yaml(manifest)
    if args.write:
        if _repo_relative(args.output) != validation_tier.FULL_DURATION_PROFILE_MANIFEST:
            raise ValueError("write target must be the governed Full duration manifest")
        canonical_output = PROJECT_ROOT.resolve() / validation_tier.FULL_DURATION_PROFILE_MANIFEST
        if _repo_relative(canonical_output) != validation_tier.FULL_DURATION_PROFILE_MANIFEST:
            raise ValueError("canonical Full duration manifest must not redirect outside its path")
        _atomic_write(canonical_output, serialized)
    print(
        json.dumps(
            {
                "status": "PASS",
                "mode": "WRITE" if args.write else "DRY_RUN",
                "output": _repo_relative(args.output),
                "output_sha256": _sha256(serialized),
                "profile_id": args.profile_id,
                "version": args.version,
                "file_count": len(manifest["files"]),
                "node_count": sum(int(row["node_count"]) for row in manifest["files"]),
                "production_effect": "none",
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
