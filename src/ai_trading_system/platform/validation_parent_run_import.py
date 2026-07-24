from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from pathlib import Path, PurePosixPath, PureWindowsPath

from ai_trading_system.platform.artifacts import (
    StrictJsonContractError,
    canonical_json_bytes,
    load_strict_json_text,
    write_json_atomic,
)

IMPORT_SCHEMA_VERSION = "validation_parent_run_import.v1"
IMPORT_MANIFEST_FILENAME = "validation_parent_run_import.json"
PARENT_RUN_IMPORT_ENV = "AITS_VALIDATION_PARENT_RUN_IMPORT"

SUMMARY_FILENAME = "test_runtime_summary.json"
RUNTIME_PROFILE_FILENAME = "test_runtime_profile.json"
VALIDATION_RUNTIME_RELATIVE_ROOT = PurePosixPath("outputs/validation_runtime")

_GIT_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,255}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IMPORT_MANIFEST_KEYS = frozenset(
    {
        "schema_version",
        "status",
        "production_effect",
        "run_id",
        "source_git_commit",
        "source_summary_path",
        "source_runtime_profile_path",
        "source_profile_inventory_path",
        "imported_summary_path",
        "imported_runtime_profile_path",
        "summary_sha256",
        "summary_size_bytes",
        "runtime_profile_sha256",
        "runtime_profile_size_bytes",
    }
)


class ValidationParentRunImportError(ValueError):
    """Raised when a portable parent-run import cannot be proven safely."""


def load_strict_json_bytes(raw_bytes: bytes, *, label: str) -> dict[str, object]:
    """Strictly decode UTF-8 JSON bytes and require a mapping root."""

    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValidationParentRunImportError(
            f"PARENT_RUN_IMPORT_JSON_INVALID: {label} is not valid UTF-8"
        ) from exc
    try:
        payload = load_strict_json_text(text, label=label)
    except StrictJsonContractError as exc:
        raise ValidationParentRunImportError(f"PARENT_RUN_IMPORT_JSON_INVALID: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValidationParentRunImportError(
            f"PARENT_RUN_IMPORT_JSON_ROOT_INVALID: {label} root must be a mapping"
        )
    return payload


def load_strict_json_file(path: Path, *, label: str) -> tuple[dict[str, object], bytes]:
    """Read one file once, then apply the portable-import strict JSON contract."""

    try:
        raw_bytes = path.read_bytes()
    except OSError as exc:
        raise ValidationParentRunImportError(
            f"PARENT_RUN_IMPORT_FILE_READ_FAILED: {label} could not be read"
        ) from exc
    return load_strict_json_bytes(raw_bytes, label=label), raw_bytes


def build_parent_run_import(
    parent_summary_path: Path,
    *,
    repo_root: Path,
) -> dict[str, object]:
    """Build and atomically publish a fixed-sibling portable import proof."""

    resolved_repo_root, resolved_summary, run_id = _validated_parent_summary_location(
        parent_summary_path,
        repo_root=repo_root,
    )
    resolved_profile = _validated_fixed_sibling(
        resolved_summary.parent / RUNTIME_PROFILE_FILENAME,
        expected_filename=RUNTIME_PROFILE_FILENAME,
        parent_directory=resolved_summary.parent,
        repo_root=resolved_repo_root,
        label="parent runtime profile",
    )
    summary, summary_bytes = load_strict_json_file(
        resolved_summary,
        label="parent runtime summary",
    )
    _, profile_bytes = load_strict_json_file(
        resolved_profile,
        label="parent runtime profile",
    )

    build_errors = _validate_source_summary_for_import(
        summary,
        run_id=run_id,
        profile_bytes=profile_bytes,
    )
    if build_errors:
        raise ValidationParentRunImportError("; ".join(build_errors))

    source_summary_path = str(summary["summary_path"])
    source_runtime_profile_path = str(summary["runtime_profile_path"])
    inventory_record, inventory_errors = _source_profile_inventory_record(
        summary,
        run_id=run_id,
        source_runtime_profile_path=source_runtime_profile_path,
        profile_bytes=profile_bytes,
    )
    if inventory_errors or inventory_record is None:
        raise ValidationParentRunImportError("; ".join(inventory_errors))
    source_profile_inventory_path = str(inventory_record["path"])

    imported_summary_path = _expected_imported_locator(run_id, SUMMARY_FILENAME)
    imported_profile_path = _expected_imported_locator(run_id, RUNTIME_PROFILE_FILENAME)
    manifest_payload: dict[str, object] = {
        "schema_version": IMPORT_SCHEMA_VERSION,
        "status": "PASS",
        "production_effect": "none",
        "run_id": run_id,
        "source_git_commit": summary["git_commit"],
        "source_summary_path": source_summary_path,
        "source_runtime_profile_path": source_runtime_profile_path,
        "source_profile_inventory_path": source_profile_inventory_path,
        "imported_summary_path": imported_summary_path,
        "imported_runtime_profile_path": imported_profile_path,
        "summary_sha256": _sha256(summary_bytes),
        "summary_size_bytes": len(summary_bytes),
        "runtime_profile_sha256": _sha256(profile_bytes),
        "runtime_profile_size_bytes": len(profile_bytes),
    }

    manifest_path = resolved_summary.parent / IMPORT_MANIFEST_FILENAME
    if _is_link_like(manifest_path):
        raise ValidationParentRunImportError(
            "PARENT_RUN_IMPORT_MANIFEST_SYMLINK: fixed sibling must not be a symlink"
        )
    write_json_atomic(manifest_path, manifest_payload, sort_keys=True)

    validated, validation_errors = validate_parent_run_import(
        manifest_path,
        parent_summary_path=resolved_summary,
        parent_profile_path=resolved_profile,
        repo_root=resolved_repo_root,
        summary_bytes=summary_bytes,
        summary=summary,
        profile_bytes=profile_bytes,
    )
    if validation_errors or validated is None:
        raise ValidationParentRunImportError("; ".join(validation_errors))
    return validated


def validate_parent_run_import(
    manifest_path: Path,
    *,
    parent_summary_path: Path,
    parent_profile_path: Path,
    repo_root: Path,
    summary_bytes: bytes,
    summary: Mapping[str, object],
    profile_bytes: bytes,
) -> tuple[dict[str, object] | None, list[str]]:
    """Validate a portable import proof without touching its source worktree."""

    errors: list[str] = []
    try:
        resolved_repo_root, resolved_summary, run_id = _validated_parent_summary_location(
            parent_summary_path,
            repo_root=repo_root,
        )
        resolved_profile = _validated_fixed_sibling(
            parent_profile_path,
            expected_filename=RUNTIME_PROFILE_FILENAME,
            parent_directory=resolved_summary.parent,
            repo_root=resolved_repo_root,
            label="parent runtime profile",
        )
        resolved_manifest = _validated_fixed_sibling(
            manifest_path,
            expected_filename=IMPORT_MANIFEST_FILENAME,
            parent_directory=resolved_summary.parent,
            repo_root=resolved_repo_root,
            label="parent run import manifest",
        )
    except ValidationParentRunImportError as exc:
        return None, [str(exc)]

    if _is_link_like(resolved_manifest) or _is_link_like(manifest_path):
        return None, ["PARENT_RUN_IMPORT_MANIFEST_SYMLINK: fixed sibling must not be a symlink"]

    try:
        manifest, manifest_bytes = load_strict_json_file(
            resolved_manifest,
            label="parent run import manifest",
        )
    except ValidationParentRunImportError as exc:
        return None, [str(exc)]
    if manifest_bytes != canonical_json_bytes(manifest, sort_keys=True):
        errors.append(
            "PARENT_RUN_IMPORT_MANIFEST_NONCANONICAL: manifest bytes must use canonical JSON"
        )

    try:
        captured_summary_bytes = resolved_summary.read_bytes()
    except OSError:
        errors.append("PARENT_RUN_IMPORT_SUMMARY_READ_FAILED: copied summary could not be re-read")
        captured_summary_bytes = b""
    try:
        captured_profile_bytes = resolved_profile.read_bytes()
    except OSError:
        errors.append("PARENT_RUN_IMPORT_PROFILE_READ_FAILED: copied profile could not be re-read")
        captured_profile_bytes = b""

    if captured_summary_bytes != summary_bytes:
        errors.append(
            "PARENT_RUN_IMPORT_SUMMARY_BYTES_DRIFT: caller bytes differ from fixed sibling"
        )
    if captured_profile_bytes != profile_bytes:
        errors.append(
            "PARENT_RUN_IMPORT_PROFILE_BYTES_DRIFT: caller bytes differ from fixed sibling"
        )

    parsed_summary: dict[str, object] | None = None
    parsed_profile: dict[str, object] | None = None
    try:
        parsed_summary = load_strict_json_bytes(
            captured_summary_bytes,
            label="parent runtime summary",
        )
    except ValidationParentRunImportError as exc:
        errors.append(str(exc))
    try:
        parsed_profile = load_strict_json_bytes(
            captured_profile_bytes,
            label="parent runtime profile",
        )
    except ValidationParentRunImportError as exc:
        errors.append(str(exc))
    if parsed_summary is not None and parsed_summary != dict(summary):
        errors.append(
            "PARENT_RUN_IMPORT_SUMMARY_MAPPING_DRIFT: caller mapping differs from fixed sibling"
        )
    if parsed_profile is None:
        errors.append("PARENT_RUN_IMPORT_PROFILE_INVALID: runtime profile must be a strict mapping")

    manifest_keys = set(manifest)
    if manifest_keys != _IMPORT_MANIFEST_KEYS:
        errors.append(
            "PARENT_RUN_IMPORT_KEYS_MISMATCH: "
            f"missing={sorted(_IMPORT_MANIFEST_KEYS - manifest_keys)} "
            f"extra={sorted(manifest_keys - _IMPORT_MANIFEST_KEYS)}"
        )
    if manifest.get("schema_version") != IMPORT_SCHEMA_VERSION:
        errors.append("PARENT_RUN_IMPORT_SCHEMA_INVALID: schema_version is invalid")
    if manifest.get("status") != "PASS":
        errors.append("PARENT_RUN_IMPORT_STATUS_INVALID: status must be PASS")
    if manifest.get("production_effect") != "none":
        errors.append("PARENT_RUN_IMPORT_PRODUCTION_EFFECT_INVALID: production_effect must be none")
    if manifest.get("run_id") != run_id:
        errors.append("PARENT_RUN_IMPORT_RUN_ID_MISMATCH: run_id does not match directory")

    imported_summary_path = _expected_imported_locator(run_id, SUMMARY_FILENAME)
    imported_profile_path = _expected_imported_locator(run_id, RUNTIME_PROFILE_FILENAME)
    if manifest.get("imported_summary_path") != imported_summary_path:
        errors.append(
            "PARENT_RUN_IMPORT_SUMMARY_LOCATOR_INVALID: imported summary is not fixed sibling"
        )
    if manifest.get("imported_runtime_profile_path") != imported_profile_path:
        errors.append(
            "PARENT_RUN_IMPORT_PROFILE_LOCATOR_INVALID: imported profile is not fixed sibling"
        )

    effective_summary = parsed_summary if parsed_summary is not None else dict(summary)
    source_errors = _validate_source_summary_for_import(
        effective_summary,
        run_id=run_id,
        profile_bytes=captured_profile_bytes,
    )
    errors.extend(source_errors)

    source_git_commit = effective_summary.get("git_commit")
    if manifest.get("source_git_commit") != source_git_commit:
        errors.append("PARENT_RUN_IMPORT_COMMIT_MISMATCH: source_git_commit differs from summary")

    source_summary_path = effective_summary.get("summary_path")
    source_runtime_profile_path = effective_summary.get("runtime_profile_path")
    inventory_record: Mapping[str, object] | None = None
    if isinstance(source_runtime_profile_path, str):
        inventory_record, inventory_errors = _source_profile_inventory_record(
            effective_summary,
            run_id=run_id,
            source_runtime_profile_path=source_runtime_profile_path,
            profile_bytes=captured_profile_bytes,
        )
        errors.extend(inventory_errors)
    else:
        inventory_errors = []
    source_profile_inventory_path = (
        inventory_record.get("path") if inventory_record is not None else None
    )
    for field_name, expected in (
        ("source_summary_path", source_summary_path),
        ("source_runtime_profile_path", source_runtime_profile_path),
        ("source_profile_inventory_path", source_profile_inventory_path),
    ):
        if manifest.get(field_name) != expected:
            errors.append(
                f"PARENT_RUN_IMPORT_SOURCE_LOCATOR_MISMATCH: {field_name} differs from summary"
            )

    summary_sha256 = _sha256(captured_summary_bytes)
    profile_sha256 = _sha256(captured_profile_bytes)
    for field_name, value, expected in (
        ("summary_sha256", manifest.get("summary_sha256"), summary_sha256),
        ("runtime_profile_sha256", manifest.get("runtime_profile_sha256"), profile_sha256),
    ):
        if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
            errors.append(f"PARENT_RUN_IMPORT_HASH_INVALID: {field_name} is invalid")
        elif value != expected:
            errors.append(f"PARENT_RUN_IMPORT_HASH_MISMATCH: {field_name} is stale")
    for field_name, value, expected in (
        ("summary_size_bytes", manifest.get("summary_size_bytes"), len(captured_summary_bytes)),
        (
            "runtime_profile_size_bytes",
            manifest.get("runtime_profile_size_bytes"),
            len(captured_profile_bytes),
        ),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            errors.append(f"PARENT_RUN_IMPORT_SIZE_INVALID: {field_name} is invalid")
        elif value != expected:
            errors.append(f"PARENT_RUN_IMPORT_SIZE_MISMATCH: {field_name} is stale")

    if errors:
        return None, errors

    manifest_relative_path = resolved_manifest.relative_to(resolved_repo_root).as_posix()
    result = dict(manifest)
    result.update(
        {
            "manifest_sha256": _sha256(manifest_bytes),
            "manifest_size_bytes": len(manifest_bytes),
            "manifest_relative_path": manifest_relative_path,
        }
    )
    return result, []


def _validated_parent_summary_location(
    parent_summary_path: Path,
    *,
    repo_root: Path,
) -> tuple[Path, Path, str]:
    resolved_repo_root = _resolve_existing_directory(repo_root, label="repository root")
    raw_parts = parent_summary_path.parts
    if "\x00" in str(parent_summary_path) or "." in raw_parts or ".." in raw_parts:
        raise ValidationParentRunImportError(
            "PARENT_RUN_IMPORT_SUMMARY_PATH_INVALID: traversal or null is not allowed"
        )
    candidate = (
        parent_summary_path
        if parent_summary_path.is_absolute()
        else resolved_repo_root / parent_summary_path
    )
    try:
        resolved_summary = candidate.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ValidationParentRunImportError(
            "PARENT_RUN_IMPORT_SUMMARY_PATH_INVALID: parent summary does not exist"
        ) from exc
    if not _is_relative_to(candidate, resolved_repo_root):
        raise ValidationParentRunImportError(
            "PARENT_RUN_IMPORT_SUMMARY_PATH_INVALID: lexical path is outside repository root"
        )
    if _path_chain_has_link(candidate, root=resolved_repo_root):
        raise ValidationParentRunImportError(
            "PARENT_RUN_IMPORT_SUMMARY_SYMLINK: parent summary and run directory must be real"
        )
    allowed_root = resolved_repo_root / VALIDATION_RUNTIME_RELATIVE_ROOT
    if _path_chain_has_link(allowed_root, root=resolved_repo_root):
        raise ValidationParentRunImportError(
            "PARENT_RUN_IMPORT_ROOT_INVALID: outputs/validation_runtime must not be a link"
        )
    try:
        resolved_allowed_root = allowed_root.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ValidationParentRunImportError(
            "PARENT_RUN_IMPORT_ROOT_INVALID: outputs/validation_runtime does not exist"
        ) from exc
    if (
        not _is_relative_to(resolved_allowed_root, resolved_repo_root)
        or resolved_allowed_root != allowed_root
    ):
        raise ValidationParentRunImportError(
            "PARENT_RUN_IMPORT_ROOT_INVALID: outputs/validation_runtime escapes repository root"
        )
    if (
        resolved_summary.name != SUMMARY_FILENAME
        or resolved_summary.parent.parent != resolved_allowed_root
        or not _is_relative_to(resolved_summary, resolved_allowed_root)
    ):
        raise ValidationParentRunImportError(
            "PARENT_RUN_IMPORT_SUMMARY_PATH_INVALID: parent summary must be "
            "a direct run fixed sibling"
        )
    run_id = resolved_summary.parent.name
    if _RUN_ID_RE.fullmatch(run_id) is None:
        raise ValidationParentRunImportError(
            "PARENT_RUN_IMPORT_RUN_ID_INVALID: run directory name is not stable"
        )
    return resolved_repo_root, resolved_summary, run_id


def _validated_fixed_sibling(
    path: Path,
    *,
    expected_filename: str,
    parent_directory: Path,
    repo_root: Path,
    label: str,
) -> Path:
    raw_parts = path.parts
    if "\x00" in str(path) or "." in raw_parts or ".." in raw_parts:
        raise ValidationParentRunImportError(
            f"PARENT_RUN_IMPORT_FIXED_SIBLING_INVALID: {label} contains traversal or null"
        )
    candidate = path if path.is_absolute() else repo_root / path
    if _is_link_like(candidate) or _is_link_like(candidate.parent):
        raise ValidationParentRunImportError(
            f"PARENT_RUN_IMPORT_FIXED_SIBLING_INVALID: {label} must not be a link"
        )
    try:
        resolved = candidate.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ValidationParentRunImportError(
            f"PARENT_RUN_IMPORT_FIXED_SIBLING_INVALID: {label} does not exist"
        ) from exc
    allowed_root = (repo_root / VALIDATION_RUNTIME_RELATIVE_ROOT).resolve(strict=True)
    if (
        resolved.name != expected_filename
        or resolved.parent != parent_directory
        or not _is_relative_to(resolved, allowed_root)
        or candidate.parent.is_symlink()
    ):
        raise ValidationParentRunImportError(
            f"PARENT_RUN_IMPORT_FIXED_SIBLING_INVALID: {label} is not the required sibling"
        )
    return resolved


def _resolve_existing_directory(path: Path, *, label: str) -> Path:
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ValidationParentRunImportError(
            f"PARENT_RUN_IMPORT_ROOT_INVALID: {label} does not exist"
        ) from exc
    if not resolved.is_dir():
        raise ValidationParentRunImportError(
            f"PARENT_RUN_IMPORT_ROOT_INVALID: {label} must be a directory"
        )
    return resolved


def _validate_source_summary_for_import(
    summary: Mapping[str, object],
    *,
    run_id: str,
    profile_bytes: bytes,
) -> list[str]:
    errors: list[str] = []
    for field_name, expected in (
        ("schema_version", 1),
        ("report_type", "test_runtime_summary"),
        ("resolved_tier", "full"),
        ("production_effect", "none"),
        ("print_only", False),
        ("benchmark_mode", False),
    ):
        if summary.get(field_name) != expected:
            errors.append(
                f"PARENT_RUN_IMPORT_SUMMARY_CONTRACT_INVALID: {field_name} must equal {expected!r}"
            )
    source_git_commit = summary.get("git_commit")
    if (
        not isinstance(source_git_commit, str)
        or _GIT_COMMIT_RE.fullmatch(source_git_commit) is None
    ):
        errors.append(
            "PARENT_RUN_IMPORT_COMMIT_INVALID: summary git_commit must be a lowercase 40-hex SHA"
        )
    source_status = summary.get("status")
    exit_code = summary.get("exit_code")
    if source_status not in ("PASS", "FAIL"):
        errors.append("PARENT_RUN_IMPORT_SUMMARY_CONTRACT_INVALID: status must be PASS or FAIL")
    if isinstance(exit_code, bool) or not isinstance(exit_code, int):
        errors.append("PARENT_RUN_IMPORT_SUMMARY_CONTRACT_INVALID: exit_code must be an integer")
    elif source_status in ("PASS", "FAIL") and ((source_status == "PASS") != (exit_code == 0)):
        errors.append(
            "PARENT_RUN_IMPORT_SUMMARY_CONTRACT_INVALID: status and exit_code are inconsistent"
        )

    source_summary_path = summary.get("summary_path")
    source_profile_path = summary.get("runtime_profile_path")
    source_prefixes: list[str] = []
    for value, filename, label in (
        (source_summary_path, SUMMARY_FILENAME, "source_summary_path"),
        (source_profile_path, RUNTIME_PROFILE_FILENAME, "source_runtime_profile_path"),
    ):
        prefix, locator_error = _source_locator_prefix(
            value,
            run_id=run_id,
            filename=filename,
            label=label,
        )
        if locator_error is not None:
            errors.append(locator_error)
        elif prefix is not None:
            source_prefixes.append(prefix)
    if len(source_prefixes) == 2 and source_prefixes[0] != source_prefixes[1]:
        errors.append(
            "PARENT_RUN_IMPORT_SOURCE_PREFIX_MISMATCH: source summary and profile prefixes differ"
        )

    if isinstance(source_profile_path, str):
        _, inventory_errors = _source_profile_inventory_record(
            summary,
            run_id=run_id,
            source_runtime_profile_path=source_profile_path,
            profile_bytes=profile_bytes,
        )
        errors.extend(inventory_errors)
    else:
        errors.append("PARENT_RUN_IMPORT_INVENTORY_INVALID: runtime_profile_path is not a string")
    return errors


def _source_profile_inventory_record(
    summary: Mapping[str, object],
    *,
    run_id: str,
    source_runtime_profile_path: str,
    profile_bytes: bytes,
) -> tuple[Mapping[str, object] | None, list[str]]:
    output_artifacts = summary.get("output_artifacts")
    if not isinstance(output_artifacts, list):
        return None, ["PARENT_RUN_IMPORT_INVENTORY_INVALID: output_artifacts must be a list"]
    candidates: list[Mapping[str, object]] = []
    for record in output_artifacts:
        if not isinstance(record, Mapping):
            continue
        path_value = record.get("path")
        if _source_locator_has_suffix(
            path_value,
            run_id=run_id,
            filename=RUNTIME_PROFILE_FILENAME,
        ):
            candidates.append(record)
    if len(candidates) != 1:
        return None, [
            "PARENT_RUN_IMPORT_INVENTORY_COUNT_INVALID: exactly one runtime "
            "profile inventory record is required"
        ]
    record = candidates[0]
    inventory_path = record.get("path")
    errors: list[str] = []
    if inventory_path != source_runtime_profile_path:
        errors.append(
            "PARENT_RUN_IMPORT_INVENTORY_LOCATOR_MISMATCH: profile inventory "
            "path differs from runtime_profile_path"
        )
    _, locator_error = _source_locator_prefix(
        inventory_path,
        run_id=run_id,
        filename=RUNTIME_PROFILE_FILENAME,
        label="source_profile_inventory_path",
    )
    if locator_error is not None:
        errors.append(locator_error)
    expected_sha256 = _sha256(profile_bytes)
    expected_size = len(profile_bytes)
    if record.get("artifact_type") != "json":
        errors.append(
            "PARENT_RUN_IMPORT_INVENTORY_INVALID: runtime profile artifact_type must be json"
        )
    if record.get("exists") is not True:
        errors.append("PARENT_RUN_IMPORT_INVENTORY_INVALID: runtime profile inventory must exist")
    if record.get("sha256") != expected_sha256:
        errors.append(
            "PARENT_RUN_IMPORT_INVENTORY_HASH_MISMATCH: runtime profile inventory hash is stale"
        )
    size_bytes = record.get("size_bytes")
    if (
        isinstance(size_bytes, bool)
        or not isinstance(size_bytes, int)
        or size_bytes != expected_size
    ):
        errors.append(
            "PARENT_RUN_IMPORT_INVENTORY_SIZE_MISMATCH: runtime profile inventory size is stale"
        )
    return (record if not errors else None), errors


def _source_locator_prefix(
    value: object,
    *,
    run_id: str,
    filename: str,
    label: str,
) -> tuple[str | None, str | None]:
    if not isinstance(value, str) or not value or value != value.strip() or "\x00" in value:
        return None, (
            f"PARENT_RUN_IMPORT_SOURCE_LOCATOR_INVALID: {label} must be a non-empty absolute path"
        )
    windows_path = PureWindowsPath(value)
    posix_path = PurePosixPath(value)
    if not windows_path.is_absolute() and not posix_path.is_absolute():
        return None, (
            f"PARENT_RUN_IMPORT_SOURCE_LOCATOR_INVALID: {label} must be "
            "absolute, not drive-relative"
        )
    normalized = value.replace("\\", "/")
    parts = PurePosixPath(normalized).parts
    if "." in parts or ".." in parts:
        return None, (f"PARENT_RUN_IMPORT_SOURCE_LOCATOR_INVALID: {label} contains traversal")
    suffix = f"/outputs/validation_runtime/{run_id}/{filename}"
    if not normalized.endswith(suffix):
        return None, (
            f"PARENT_RUN_IMPORT_SOURCE_LOCATOR_INVALID: {label} has wrong run id or suffix"
        )
    prefix = normalized[: -len(suffix)]
    if not prefix:
        return None, (f"PARENT_RUN_IMPORT_SOURCE_LOCATOR_INVALID: {label} has no repository prefix")
    return prefix, None


def _source_locator_has_suffix(
    value: object,
    *,
    run_id: str,
    filename: str,
) -> bool:
    if not isinstance(value, str) or "\x00" in value:
        return False
    normalized = value.replace("\\", "/")
    return normalized.endswith(f"/outputs/validation_runtime/{run_id}/{filename}")


def _expected_imported_locator(run_id: str, filename: str) -> str:
    return (VALIDATION_RUNTIME_RELATIVE_ROOT / run_id / filename).as_posix()


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _is_link_like(path: Path) -> bool:
    try:
        if path.is_symlink():
            return True
        is_junction = getattr(path, "is_junction", None)
        return bool(is_junction()) if is_junction is not None else False
    except OSError:
        return True


def _path_chain_has_link(path: Path, *, root: Path) -> bool:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return True
    current = root
    for part in relative.parts:
        current /= part
        if _is_link_like(current):
            return True
    return False


def _sha256(raw_bytes: bytes) -> str:
    return hashlib.sha256(raw_bytes).hexdigest()


__all__ = [
    "IMPORT_MANIFEST_FILENAME",
    "IMPORT_SCHEMA_VERSION",
    "PARENT_RUN_IMPORT_ENV",
    "ValidationParentRunImportError",
    "build_parent_run_import",
    "load_strict_json_bytes",
    "load_strict_json_file",
    "validate_parent_run_import",
]
