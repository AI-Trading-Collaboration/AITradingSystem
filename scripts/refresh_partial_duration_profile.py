from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import tempfile
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
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
        payload = json.loads(raw_bytes)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} could not be read: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{label} root must be a mapping")
    return payload, raw_bytes


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _require_equal(actual: object, expected: object, *, label: str) -> None:
    if actual != expected:
        raise ValueError(f"{label} must equal {expected!r}; observed {actual!r}")


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
            or float(duration) < 0.0
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
    if str(payload.get("workers")) != "16":
        raise ValueError("runtime summary workers must equal 16")

    configured_profile = payload.get("runtime_profile_path")
    if not isinstance(configured_profile, str):
        raise ValueError("runtime summary runtime_profile_path is required")
    _require_equal(
        Path(configured_profile).resolve(),
        source_profile_path.resolve(),
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
        if (
            isinstance(artifact_path, str)
            and Path(artifact_path).resolve() == source_profile_path.resolve()
        ):
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
    if not profile_id.strip():
        raise ValueError("profile_id is required")
    if isinstance(version, bool) or version < 1:
        raise ValueError("version must be a positive integer")
    if expected_nodes < 1 or expected_files < 1:
        raise ValueError("expected node and file counts must be positive")

    profile, profile_bytes = _load_json(source_profile_path, label="runtime profile")
    summary, _ = _load_json(source_summary_path, label="runtime summary")
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
                "seed; new or changed files retain stable collection order until the next natural "
                "Full records the current collection."
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
        _repo_relative(args.output)
        _atomic_write(args.output, serialized)
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
