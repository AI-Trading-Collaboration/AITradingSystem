from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from ai_trading_system.yaml_loader import safe_load_yaml_text

BOOTSTRAP_HANDOFF_SCHEMA_VERSION = "arch_005_bootstrap_handoff.v1"
BOOTSTRAP_HANDOFF_PRODUCER_VERSION = "arch_005_bootstrap_handoff_producer.v1"
REQUIRED_VALIDATION_TIERS = (
    "focused",
    "architecture_fitness",
    "contract_validation",
    "full_validation",
)

_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class BootstrapHandoffError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


def build_bootstrap_handoff(
    *,
    project_root: Path,
    head_commit: str,
    base_commit: str,
    branch: str,
    validation_artifacts: Mapping[str, str | Path],
    known_unrelated_worktree_files: Sequence[str],
    generated_at: datetime | None = None,
    frozen_tracked_files: Mapping[str, bytes] | None = None,
) -> dict[str, Any]:
    root = project_root.resolve()
    matrix_path = "inputs/architecture/arch_004g2_callback_migration_matrix.yaml"
    matrix_bytes = _tracked_file_bytes(root, matrix_path, frozen_tracked_files)
    matrix = _mapping(
        safe_load_yaml_text(matrix_bytes.decode("utf-8")),
        "migration matrix",
    )
    matrix_summary = _mapping(matrix.get("summary"), "migration matrix summary")
    tiers = _validation_artifact_records(root, validation_artifacts)
    architecture_state = {
        "module_manifest": _file_state(
            root,
            "inputs/architecture/arch_004e_module_manifest.yaml",
            frozen_tracked_files,
        ),
        "test_manifest": _file_state(
            root,
            "inputs/architecture/arch_004e_test_manifest.yaml",
            frozen_tracked_files,
        ),
        "compatibility_baseline": _file_state(
            root,
            "inputs/architecture/arch_004_compatibility_baseline.yaml",
            frozen_tracked_files,
        ),
        "deprecation_inventory": _file_state(
            root,
            "inputs/architecture/arch_004g_deprecation_inventory.yaml",
            frozen_tracked_files,
        ),
    }
    attribution_path = "inputs/architecture/arch_004_worktree_attribution.yaml"
    generated = generated_at or datetime.now(UTC)
    if generated.tzinfo is None:
        raise BootstrapHandoffError(
            "GENERATED_AT_TIMEZONE",
            "generated_at must be timezone-aware",
        )
    payload: dict[str, Any] = {
        "schema_version": BOOTSTRAP_HANDOFF_SCHEMA_VERSION,
        "source_task_id": "ARCH-004",
        "completed_phase": "ARCH-004G2.4",
        "head_commit": _commit(head_commit, "head_commit"),
        "base_commit": _commit(base_commit, "base_commit"),
        "branch": _required_text(branch, "branch"),
        "push_status": "SYNCED_WITH_UPSTREAM",
        "tracked_file_hash_basis": (
            "source_commit_git_blob_sha256"
            if frozen_tracked_files is not None
            else "source_snapshot_bytes_sha256"
        ),
        "migration_matrix": {
            "path": matrix_path,
            "sha256": hashlib.sha256(matrix_bytes).hexdigest(),
            "status": "PASS",
            "baseline_callback_count": matrix_summary.get("baseline_callback_count"),
            "migrated_callback_count": matrix_summary.get("migrated_callback_count"),
            "pending_callback_count": matrix_summary.get("pending_callback_count"),
            "unresolved_callback_count": matrix_summary.get("unresolved_callback_count"),
            "duplicate_registration_count": matrix_summary.get("duplicate_callback_count"),
            "phase_exit_criteria_passed": matrix_summary.get("phase_exit_ready"),
        },
        "validation_artifacts": tiers,
        "architecture_state": architecture_state,
        "shared_path_activity": {
            "status": "PASS",
            "active_shared_path_owner_count": 0,
            "active_shared_path_lease_count": 0,
            "active_shared_path_integration_count": 0,
            "lease_registry_present": False,
            "evidence": (
                "ARCH-005-PB1 is non-cutover and fixes lease_acquisition_allowed=false; "
                "no canonical task/lease/integration registry exists before S0/S2."
            ),
        },
        "worktree_attribution": {
            "source_worktree_clean": True,
            "attribution_path": attribution_path,
            "attribution_sha256": hashlib.sha256(
                _tracked_file_bytes(root, attribution_path, frozen_tracked_files)
            ).hexdigest(),
            "known_unrelated_worktree_files": sorted(
                set(
                    _portable_path(item, "known_unrelated_worktree_files")
                    for item in known_unrelated_worktree_files
                )
            ),
            "known_unrelated_scope": "adjacent_main_worktree_only",
            "handoff_artifact_attribution": "ARCH-004 phase exit coordinator",
        },
        "next_slice_unblocked": False,
        "production_effect": "none",
        "broker_action": "none",
        "generated_at": generated.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "producer_version": BOOTSTRAP_HANDOFF_PRODUCER_VERSION,
    }
    payload["handoff_checksum"] = bootstrap_handoff_checksum(payload)
    validate_bootstrap_handoff(
        payload,
        project_root=root,
        frozen_tracked_files=frozen_tracked_files,
    )
    return payload


def validate_bootstrap_handoff(
    payload: Mapping[str, Any],
    *,
    project_root: Path,
    expected_head_commit: str | None = None,
    expected_branch: str | None = None,
    frozen_tracked_files: Mapping[str, bytes] | None = None,
    frozen_validation_artifacts: Mapping[str, bytes] | None = None,
) -> None:
    expected_top = {
        "schema_version",
        "source_task_id",
        "completed_phase",
        "head_commit",
        "base_commit",
        "branch",
        "push_status",
        "tracked_file_hash_basis",
        "migration_matrix",
        "validation_artifacts",
        "architecture_state",
        "shared_path_activity",
        "worktree_attribution",
        "next_slice_unblocked",
        "production_effect",
        "broker_action",
        "generated_at",
        "producer_version",
        "handoff_checksum",
    }
    _require_exact_keys(payload, expected_top, "HANDOFF_FIELDS")
    if payload["schema_version"] != BOOTSTRAP_HANDOFF_SCHEMA_VERSION:
        raise BootstrapHandoffError("HANDOFF_SCHEMA", "unsupported schema_version")
    if payload["source_task_id"] != "ARCH-004":
        raise BootstrapHandoffError("HANDOFF_SOURCE_TASK", "source_task_id must be ARCH-004")
    if payload["completed_phase"] != "ARCH-004G2.4":
        raise BootstrapHandoffError(
            "HANDOFF_COMPLETED_PHASE",
            "completed_phase must be ARCH-004G2.4",
        )
    head = _commit(payload["head_commit"], "head_commit")
    _commit(payload["base_commit"], "base_commit")
    branch = _required_text(payload["branch"], "branch")
    if expected_head_commit is not None and head != expected_head_commit:
        raise BootstrapHandoffError("HANDOFF_HEAD_DRIFT", f"expected {expected_head_commit}")
    if expected_branch is not None and branch != expected_branch:
        raise BootstrapHandoffError("HANDOFF_BRANCH_DRIFT", f"expected {expected_branch}")
    if payload["push_status"] != "SYNCED_WITH_UPSTREAM":
        raise BootstrapHandoffError("HANDOFF_NOT_PUSHED", "push_status must be synced")
    hash_basis = payload["tracked_file_hash_basis"]
    if hash_basis not in {
        "source_commit_git_blob_sha256",
        "source_snapshot_bytes_sha256",
    }:
        raise BootstrapHandoffError("HANDOFF_HASH_BASIS", "unsupported hash basis")
    if hash_basis == "source_commit_git_blob_sha256" and frozen_tracked_files is None:
        raise BootstrapHandoffError(
            "HANDOFF_FROZEN_SOURCE_REQUIRED",
            "git-blob hash basis requires source-commit bytes",
        )
    if payload["next_slice_unblocked"] is not False:
        raise BootstrapHandoffError(
            "HANDOFF_NEXT_SLICE_UNSAFE",
            "next_slice_unblocked must be false",
        )
    if payload["production_effect"] != "none" or payload["broker_action"] != "none":
        raise BootstrapHandoffError(
            "HANDOFF_UNSAFE_EFFECT",
            "production_effect and broker_action must be none",
        )
    if payload["producer_version"] != BOOTSTRAP_HANDOFF_PRODUCER_VERSION:
        raise BootstrapHandoffError("HANDOFF_PRODUCER", "unsupported producer_version")
    _parse_generated_at(payload["generated_at"])
    root = project_root.resolve()
    _validate_matrix(payload["migration_matrix"], root, frozen_tracked_files)
    _validate_tiers(
        payload["validation_artifacts"],
        root,
        frozen_validation_artifacts,
    )
    _validate_architecture_state(payload["architecture_state"], root, frozen_tracked_files)
    _validate_shared_path_activity(payload["shared_path_activity"])
    _validate_worktree_attribution(payload["worktree_attribution"], root, frozen_tracked_files)
    checksum = _sha256(payload["handoff_checksum"], "handoff_checksum")
    if checksum != bootstrap_handoff_checksum(payload):
        raise BootstrapHandoffError("HANDOFF_CHECKSUM_DRIFT", "handoff checksum mismatch")


def bootstrap_handoff_checksum(payload: Mapping[str, Any]) -> str:
    body = {key: value for key, value in payload.items() if key != "handoff_checksum"}
    encoded = json.dumps(
        body,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validation_artifact_records(
    root: Path,
    artifacts: Mapping[str, str | Path],
) -> dict[str, dict[str, str]]:
    if set(artifacts) != set(REQUIRED_VALIDATION_TIERS):
        raise BootstrapHandoffError(
            "HANDOFF_VALIDATION_TIERS",
            f"required={list(REQUIRED_VALIDATION_TIERS)} actual={sorted(artifacts)}",
        )
    result: dict[str, dict[str, str]] = {}
    for tier in REQUIRED_VALIDATION_TIERS:
        path = Path(artifacts[tier])
        if path.is_absolute():
            try:
                path = path.resolve().relative_to(root)
            except ValueError as exc:
                raise BootstrapHandoffError(
                    "HANDOFF_ARTIFACT_OUTSIDE_ROOT",
                    str(path),
                ) from exc
        portable = _portable_path(path.as_posix(), f"validation_artifacts.{tier}")
        absolute = (root / Path(portable)).resolve()
        summary = _json_mapping(absolute, f"validation artifact {tier}")
        status = str(summary.get("status") or "")
        if status != "PASS":
            raise BootstrapHandoffError(
                "HANDOFF_VALIDATION_NOT_PASS",
                f"{tier}: {status or 'missing'}",
            )
        result[tier] = {
            "tier": tier,
            "status": "PASS",
            "artifact_path": portable,
            "artifact_sha256": _file_sha256(absolute),
        }
    return result


def _validate_matrix(
    value: object,
    root: Path,
    frozen_tracked_files: Mapping[str, bytes] | None,
) -> None:
    matrix = _mapping(value, "migration_matrix")
    expected = {
        "path",
        "sha256",
        "status",
        "baseline_callback_count",
        "migrated_callback_count",
        "pending_callback_count",
        "unresolved_callback_count",
        "duplicate_registration_count",
        "phase_exit_criteria_passed",
    }
    _require_exact_keys(matrix, expected, "HANDOFF_MATRIX_FIELDS")
    matrix_bytes = _verify_tracked_file_reference(
        matrix,
        root,
        path_key="path",
        sha_key="sha256",
        frozen_tracked_files=frozen_tracked_files,
    )
    if matrix["status"] != "PASS":
        raise BootstrapHandoffError("HANDOFF_MATRIX_STATUS", "migration matrix must PASS")
    counts = (
        matrix["baseline_callback_count"],
        matrix["migrated_callback_count"],
        matrix["pending_callback_count"],
        matrix["unresolved_callback_count"],
        matrix["duplicate_registration_count"],
    )
    if counts != (967, 967, 0, 0, 0) or matrix["phase_exit_criteria_passed"] is not True:
        raise BootstrapHandoffError(
            "HANDOFF_MATRIX_INCOMPLETE",
            f"counts={counts} phase_exit={matrix['phase_exit_criteria_passed']}",
        )
    frozen = _mapping(
        safe_load_yaml_text(matrix_bytes.decode("utf-8")),
        "migration matrix",
    )
    summary = _mapping(frozen.get("summary"), "migration matrix summary")
    source_fields = {
        "baseline_callback_count": "baseline_callback_count",
        "migrated_callback_count": "migrated_callback_count",
        "pending_callback_count": "pending_callback_count",
        "unresolved_callback_count": "unresolved_callback_count",
        "duplicate_registration_count": "duplicate_callback_count",
        "phase_exit_criteria_passed": "phase_exit_ready",
    }
    for handoff_field, source_field in source_fields.items():
        if summary.get(source_field) != matrix[handoff_field]:
            raise BootstrapHandoffError("HANDOFF_MATRIX_DRIFT", handoff_field)


def _validate_tiers(
    value: object,
    root: Path,
    frozen_validation_artifacts: Mapping[str, bytes] | None,
) -> None:
    tiers = _mapping(value, "validation_artifacts")
    if set(tiers) != set(REQUIRED_VALIDATION_TIERS):
        raise BootstrapHandoffError("HANDOFF_VALIDATION_TIERS", str(sorted(tiers)))
    expected = {"tier", "status", "artifact_path", "artifact_sha256"}
    expected_paths: set[str] = set()
    records: dict[str, Mapping[str, Any]] = {}
    for tier in REQUIRED_VALIDATION_TIERS:
        record = _mapping(tiers[tier], tier)
        _require_exact_keys(record, expected, "HANDOFF_VALIDATION_FIELDS")
        if record["tier"] != tier or record["status"] != "PASS":
            raise BootstrapHandoffError("HANDOFF_VALIDATION_NOT_PASS", tier)
        path = _portable_path(record["artifact_path"], f"validation_artifacts.{tier}")
        if path in expected_paths:
            raise BootstrapHandoffError("HANDOFF_VALIDATION_PATH_DUPLICATE", path)
        expected_paths.add(path)
        records[tier] = record
    if (
        frozen_validation_artifacts is not None
        and set(frozen_validation_artifacts) != expected_paths
    ):
        raise BootstrapHandoffError(
            "HANDOFF_FROZEN_VALIDATION_SET",
            f"expected={sorted(expected_paths)} actual={sorted(frozen_validation_artifacts)}",
        )
    expected_summary_tiers = {
        "focused": "fast-unit",
        "architecture_fitness": "architecture-fitness",
        "contract_validation": "contract-validation",
        "full_validation": "full",
    }
    for tier in REQUIRED_VALIDATION_TIERS:
        record = records[tier]
        if frozen_validation_artifacts is None:
            artifact = _verify_file_reference(
                record,
                root,
                path_key="artifact_path",
                sha_key="artifact_sha256",
            )
            summary = _json_mapping(artifact, f"validation artifact {tier}")
        else:
            path = _portable_path(record["artifact_path"], f"validation_artifacts.{tier}")
            content = frozen_validation_artifacts[path]
            if not isinstance(content, bytes):
                raise BootstrapHandoffError("HANDOFF_FROZEN_VALIDATION_BYTES", path)
            expected_sha = _sha256(record["artifact_sha256"], "artifact_sha256")
            actual_sha = hashlib.sha256(content).hexdigest()
            if actual_sha != expected_sha:
                raise BootstrapHandoffError(
                    "HANDOFF_FILE_HASH_DRIFT",
                    f"{path}: expected={expected_sha} actual={actual_sha}",
                )
            summary = _json_bytes_mapping(content, f"validation artifact {tier}")
        if summary.get("status") != "PASS" or summary.get("exit_code") != 0:
            raise BootstrapHandoffError("HANDOFF_VALIDATION_ARTIFACT_INVALID", tier)
        if summary.get("tier") != expected_summary_tiers[tier]:
            raise BootstrapHandoffError("HANDOFF_VALIDATION_ARTIFACT_TIER", tier)


def _validate_architecture_state(
    value: object,
    root: Path,
    frozen_tracked_files: Mapping[str, bytes] | None,
) -> None:
    state = _mapping(value, "architecture_state")
    expected_names = {
        "module_manifest",
        "test_manifest",
        "compatibility_baseline",
        "deprecation_inventory",
    }
    if set(state) != expected_names:
        raise BootstrapHandoffError("HANDOFF_ARCHITECTURE_STATE_FIELDS", str(sorted(state)))
    for name in sorted(expected_names):
        record = _mapping(state[name], name)
        _require_exact_keys(record, {"path", "sha256", "fresh"}, "HANDOFF_FILE_STATE")
        if record["fresh"] is not True:
            raise BootstrapHandoffError("HANDOFF_FILE_NOT_FRESH", name)
        content = _verify_tracked_file_reference(
            record,
            root,
            path_key="path",
            sha_key="sha256",
            frozen_tracked_files=frozen_tracked_files,
        )
        parsed = _mapping(safe_load_yaml_text(content.decode("utf-8")), name)
        if name == "module_manifest" and (
            parsed.get("status") != "PASS" or parsed.get("orphan_count") != 0
        ):
            raise BootstrapHandoffError("HANDOFF_MODULE_MANIFEST_INVALID", name)
        if name == "test_manifest" and (
            parsed.get("status") != "PASS" or parsed.get("orphan_count") != 0
        ):
            raise BootstrapHandoffError("HANDOFF_TEST_MANIFEST_INVALID", name)
        if name == "deprecation_inventory" and not str(parsed.get("inventory_id") or ""):
            raise BootstrapHandoffError("HANDOFF_DEPRECATION_INVALID", name)
        if name == "compatibility_baseline":
            phase = _mapping(parsed.get("phase_g2_4_phase_exit"), "phase_g2_4_phase_exit")
            if phase.get("status") != "PASS":
                raise BootstrapHandoffError("HANDOFF_PHASE_EXIT_NOT_PASS", name)


def _validate_shared_path_activity(value: object) -> None:
    activity = _mapping(value, "shared_path_activity")
    expected = {
        "status",
        "active_shared_path_owner_count",
        "active_shared_path_lease_count",
        "active_shared_path_integration_count",
        "lease_registry_present",
        "evidence",
    }
    _require_exact_keys(activity, expected, "HANDOFF_SHARED_ACTIVITY_FIELDS")
    counts = (
        activity["active_shared_path_owner_count"],
        activity["active_shared_path_lease_count"],
        activity["active_shared_path_integration_count"],
    )
    if activity["status"] != "PASS" or counts != (0, 0, 0):
        raise BootstrapHandoffError("HANDOFF_SHARED_ACTIVITY_ACTIVE", str(counts))
    if activity["lease_registry_present"] is not False:
        raise BootstrapHandoffError("HANDOFF_LEASE_REGISTRY_UNEXPECTED", "must be false")
    _required_text(activity["evidence"], "shared_path_activity.evidence")


def _validate_worktree_attribution(
    value: object,
    root: Path,
    frozen_tracked_files: Mapping[str, bytes] | None,
) -> None:
    attribution = _mapping(value, "worktree_attribution")
    expected = {
        "source_worktree_clean",
        "attribution_path",
        "attribution_sha256",
        "known_unrelated_worktree_files",
        "known_unrelated_scope",
        "handoff_artifact_attribution",
    }
    _require_exact_keys(attribution, expected, "HANDOFF_ATTRIBUTION_FIELDS")
    if attribution["source_worktree_clean"] is not True:
        raise BootstrapHandoffError("HANDOFF_SOURCE_WORKTREE_DIRTY", "must be true")
    _verify_tracked_file_reference(
        attribution,
        root,
        path_key="attribution_path",
        sha_key="attribution_sha256",
        frozen_tracked_files=frozen_tracked_files,
    )
    unrelated = attribution["known_unrelated_worktree_files"]
    if not isinstance(unrelated, list):
        raise BootstrapHandoffError("HANDOFF_UNRELATED_FILES", "must be a list")
    normalized = [_portable_path(item, "known_unrelated_worktree_files") for item in unrelated]
    if normalized != sorted(set(normalized)):
        raise BootstrapHandoffError(
            "HANDOFF_UNRELATED_FILES",
            "paths must be unique and sorted",
        )
    if attribution["known_unrelated_scope"] != "adjacent_main_worktree_only":
        raise BootstrapHandoffError("HANDOFF_UNRELATED_SCOPE", "unexpected scope")
    if attribution["handoff_artifact_attribution"] != "ARCH-004 phase exit coordinator":
        raise BootstrapHandoffError("HANDOFF_ARTIFACT_ATTRIBUTION", "unexpected owner")


def _file_state(
    root: Path,
    relative: str,
    frozen_tracked_files: Mapping[str, bytes] | None,
) -> dict[str, object]:
    content = _tracked_file_bytes(root, relative, frozen_tracked_files)
    return {
        "path": relative,
        "sha256": hashlib.sha256(content).hexdigest(),
        "fresh": True,
    }


def _tracked_file_bytes(
    root: Path,
    relative: str,
    frozen_tracked_files: Mapping[str, bytes] | None,
) -> bytes:
    if frozen_tracked_files is None:
        return (root / relative).read_bytes()
    try:
        content = frozen_tracked_files[relative]
    except KeyError as exc:
        raise BootstrapHandoffError("HANDOFF_FROZEN_SOURCE_MISSING", relative) from exc
    if not isinstance(content, bytes):
        raise BootstrapHandoffError("HANDOFF_FROZEN_SOURCE_BYTES", relative)
    return content


def _verify_file_reference(
    record: Mapping[str, Any],
    root: Path,
    *,
    path_key: str,
    sha_key: str,
) -> Path:
    portable = _portable_path(record[path_key], path_key)
    expected = _sha256(record[sha_key], sha_key)
    path = (root / Path(portable)).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise BootstrapHandoffError("HANDOFF_PATH_OUTSIDE_ROOT", portable) from exc
    if not path.is_file():
        raise BootstrapHandoffError("HANDOFF_FILE_MISSING", portable)
    actual = _file_sha256(path)
    if actual != expected:
        raise BootstrapHandoffError(
            "HANDOFF_FILE_HASH_DRIFT",
            f"{portable}: expected={expected} actual={actual}",
        )
    return path


def _verify_tracked_file_reference(
    record: Mapping[str, Any],
    root: Path,
    *,
    path_key: str,
    sha_key: str,
    frozen_tracked_files: Mapping[str, bytes] | None,
) -> bytes:
    portable = _portable_path(record[path_key], path_key)
    expected = _sha256(record[sha_key], sha_key)
    if frozen_tracked_files is None:
        path = _verify_file_reference(
            record,
            root,
            path_key=path_key,
            sha_key=sha_key,
        )
        return path.read_bytes()
    try:
        content = frozen_tracked_files[portable]
    except KeyError as exc:
        raise BootstrapHandoffError("HANDOFF_FROZEN_SOURCE_MISSING", portable) from exc
    if not isinstance(content, bytes):
        raise BootstrapHandoffError("HANDOFF_FROZEN_SOURCE_BYTES", portable)
    actual = hashlib.sha256(content).hexdigest()
    if actual != expected:
        raise BootstrapHandoffError(
            "HANDOFF_FILE_HASH_DRIFT",
            f"{portable}: expected={expected} actual={actual}",
        )
    return content


def _json_mapping(path: Path, label: str) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BootstrapHandoffError("HANDOFF_ARTIFACT_READ", f"{label}: {exc}") from exc
    return _mapping(value, label)


def _json_bytes_mapping(content: bytes, label: str) -> Mapping[str, Any]:
    try:
        value = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BootstrapHandoffError("HANDOFF_ARTIFACT_READ", f"{label}: {exc}") from exc
    return _mapping(value, label)


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise BootstrapHandoffError("HANDOFF_MAPPING_REQUIRED", label)
    return value


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise BootstrapHandoffError("HANDOFF_TEXT_REQUIRED", field)
    return value


def _commit(value: object, field: str) -> str:
    text = _required_text(value, field)
    if _COMMIT_RE.fullmatch(text) is None:
        raise BootstrapHandoffError("HANDOFF_COMMIT_INVALID", field)
    return text


def _sha256(value: object, field: str) -> str:
    text = _required_text(value, field)
    if _SHA256_RE.fullmatch(text) is None:
        raise BootstrapHandoffError("HANDOFF_SHA256_INVALID", field)
    return text


def _portable_path(value: object, field: str) -> str:
    text = _required_text(value, field).replace("\\", "/")
    path = PurePosixPath(text)
    if path.is_absolute() or ".." in path.parts or text != path.as_posix():
        raise BootstrapHandoffError("HANDOFF_PATH_INVALID", f"{field}: {text}")
    return text


def _parse_generated_at(value: object) -> datetime:
    text = _required_text(value, "generated_at")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BootstrapHandoffError("HANDOFF_GENERATED_AT_INVALID", text) from exc
    if parsed.tzinfo is None:
        raise BootstrapHandoffError("HANDOFF_GENERATED_AT_INVALID", text)
    return parsed


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _require_exact_keys(
    payload: Mapping[str, Any],
    expected: set[str],
    code: str,
) -> None:
    actual = set(payload)
    if actual != expected:
        raise BootstrapHandoffError(
            code,
            f"missing={sorted(expected - actual)} unknown={sorted(actual - expected)}",
        )


__all__ = [
    "BOOTSTRAP_HANDOFF_PRODUCER_VERSION",
    "BOOTSTRAP_HANDOFF_SCHEMA_VERSION",
    "REQUIRED_VALIDATION_TIERS",
    "BootstrapHandoffError",
    "bootstrap_handoff_checksum",
    "build_bootstrap_handoff",
    "validate_bootstrap_handoff",
]
