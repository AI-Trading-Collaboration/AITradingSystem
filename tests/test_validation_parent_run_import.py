from __future__ import annotations

import copy
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

from ai_trading_system.platform.artifacts import canonical_json_bytes
from ai_trading_system.platform.validation_parent_run_import import (
    IMPORT_MANIFEST_FILENAME,
    IMPORT_SCHEMA_VERSION,
    ValidationParentRunImportError,
    build_parent_run_import,
    load_strict_json_bytes,
    validate_parent_run_import,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "full_20260723T235642Z"
SOURCE_COMMIT = "0" * 40


@dataclass(frozen=True)
class ParentFixture:
    repo_root: Path
    run_dir: Path
    summary_path: Path
    profile_path: Path
    source_summary_path: str
    source_profile_path: str

    @property
    def manifest_path(self) -> Path:
        return self.run_dir / IMPORT_MANIFEST_FILENAME


def _parent_fixture(tmp_path: Path) -> ParentFixture:
    repo_root = tmp_path / "repo"
    run_dir = repo_root / "outputs" / "validation_runtime" / RUN_ID
    run_dir.mkdir(parents=True)
    summary_path = run_dir / "test_runtime_summary.json"
    profile_path = run_dir / "test_runtime_profile.json"
    profile_bytes = canonical_json_bytes(
        {
            "schema_version": "test_runtime_profile.v1",
            "profile_status": "FAIL",
            "production_effect": "none",
        }
    )
    profile_path.write_bytes(profile_bytes)

    source_prefix = r"Z:\retired_validation_clone"
    source_summary_path = (
        source_prefix + rf"\outputs\validation_runtime\{RUN_ID}\test_runtime_summary.json"
    )
    source_profile_path = (
        source_prefix + rf"\outputs\validation_runtime\{RUN_ID}\test_runtime_profile.json"
    )
    summary = {
        "schema_version": 1,
        "report_type": "test_runtime_summary",
        "resolved_tier": "full",
        "status": "FAIL",
        "exit_code": 1,
        "git_commit": SOURCE_COMMIT,
        "production_effect": "none",
        "print_only": False,
        "benchmark_mode": False,
        "summary_path": source_summary_path,
        "runtime_profile_path": source_profile_path,
        "output_artifacts": [
            {
                "artifact_type": "json",
                "exists": True,
                "path": source_profile_path,
                "sha256": _sha256(profile_bytes),
                "size_bytes": len(profile_bytes),
            }
        ],
    }
    summary_path.write_bytes(canonical_json_bytes(summary))
    return ParentFixture(
        repo_root=repo_root,
        run_dir=run_dir,
        summary_path=summary_path,
        profile_path=profile_path,
        source_summary_path=source_summary_path,
        source_profile_path=source_profile_path,
    )


def _summary(fixture: ParentFixture) -> tuple[dict[str, object], bytes]:
    raw_bytes = fixture.summary_path.read_bytes()
    return load_strict_json_bytes(raw_bytes, label="test summary"), raw_bytes


def _validate(
    fixture: ParentFixture,
    *,
    summary_bytes: bytes | None = None,
    summary: dict[str, object] | None = None,
    profile_bytes: bytes | None = None,
    manifest_path: Path | None = None,
    profile_path: Path | None = None,
) -> tuple[dict[str, object] | None, list[str]]:
    captured_summary, captured_summary_bytes = _summary(fixture)
    return validate_parent_run_import(
        manifest_path or fixture.manifest_path,
        parent_summary_path=fixture.summary_path,
        parent_profile_path=profile_path or fixture.profile_path,
        repo_root=fixture.repo_root,
        summary_bytes=(captured_summary_bytes if summary_bytes is None else summary_bytes),
        summary=captured_summary if summary is None else summary,
        profile_bytes=(
            fixture.profile_path.read_bytes() if profile_bytes is None else profile_bytes
        ),
    )


def test_builder_publishes_fixed_sibling_and_binds_same_bytes(tmp_path: Path) -> None:
    fixture = _parent_fixture(tmp_path)

    result = build_parent_run_import(
        fixture.summary_path.relative_to(fixture.repo_root),
        repo_root=fixture.repo_root,
    )

    manifest_bytes = fixture.manifest_path.read_bytes()
    manifest = load_strict_json_bytes(manifest_bytes, label="manifest")
    assert manifest["schema_version"] == IMPORT_SCHEMA_VERSION
    assert manifest["status"] == "PASS"
    assert manifest["production_effect"] == "none"
    assert manifest["run_id"] == RUN_ID
    assert manifest["source_git_commit"] == SOURCE_COMMIT
    assert manifest["source_summary_path"] == fixture.source_summary_path
    assert manifest["source_runtime_profile_path"] == fixture.source_profile_path
    assert manifest["source_profile_inventory_path"] == fixture.source_profile_path
    assert (
        manifest["imported_summary_path"]
        == f"outputs/validation_runtime/{RUN_ID}/test_runtime_summary.json"
    )
    assert (
        manifest["imported_runtime_profile_path"]
        == f"outputs/validation_runtime/{RUN_ID}/test_runtime_profile.json"
    )
    assert manifest["summary_sha256"] == _sha256(fixture.summary_path.read_bytes())
    assert manifest["summary_size_bytes"] == fixture.summary_path.stat().st_size
    assert manifest["runtime_profile_sha256"] == _sha256(fixture.profile_path.read_bytes())
    assert manifest["runtime_profile_size_bytes"] == fixture.profile_path.stat().st_size
    assert result["manifest_sha256"] == _sha256(manifest_bytes)
    assert result["manifest_size_bytes"] == len(manifest_bytes)
    assert (
        result["manifest_relative_path"]
        == f"outputs/validation_runtime/{RUN_ID}/{IMPORT_MANIFEST_FILENAME}"
    )


def test_builder_cli_uses_fixed_output_and_reports_validated_binding(
    tmp_path: Path,
) -> None:
    fixture = _parent_fixture(tmp_path)

    completed = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "build_validation_parent_run_import.py"),
            "--repo-root",
            str(fixture.repo_root),
            "--parent-run",
            str(fixture.summary_path.relative_to(fixture.repo_root)),
        ],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    output = json.loads(completed.stdout)
    assert output["schema_version"] == IMPORT_SCHEMA_VERSION
    assert output["manifest_relative_path"].endswith(IMPORT_MANIFEST_FILENAME)
    assert fixture.manifest_path.is_file()


@pytest.mark.parametrize(
    ("raw_bytes", "error_fragment"),
    [
        (
            b'{"schema_version": 1, "schema_version": 1}',
            "duplicate JSON key",
        ),
        (
            b'{"duration": NaN}',
            "non-standard JSON constant",
        ),
        (
            b'["not", "a", "mapping"]',
            "root must be a mapping",
        ),
        (
            b'{"invalid": "\xff"}',
            "not valid UTF-8",
        ),
    ],
)
def test_builder_rejects_ambiguous_summary_json(
    tmp_path: Path,
    raw_bytes: bytes,
    error_fragment: str,
) -> None:
    fixture = _parent_fixture(tmp_path)
    fixture.summary_path.write_bytes(raw_bytes)

    with pytest.raises(ValidationParentRunImportError, match=error_fragment):
        build_parent_run_import(fixture.summary_path, repo_root=fixture.repo_root)


@pytest.mark.parametrize(
    "raw_bytes",
    [
        b'{"schema_version": "v1", "schema_version": "v1"}',
        b'{"duration": Infinity}',
        b'["not", "a", "mapping"]',
        b'{"invalid": "\xff"}',
    ],
)
def test_builder_rejects_ambiguous_profile_json(
    tmp_path: Path,
    raw_bytes: bytes,
) -> None:
    fixture = _parent_fixture(tmp_path)
    fixture.profile_path.write_bytes(raw_bytes)

    with pytest.raises(ValidationParentRunImportError, match="PARENT_RUN_IMPORT_JSON"):
        build_parent_run_import(fixture.summary_path, repo_root=fixture.repo_root)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        (
            "summary_path",
            r"Z:\retired_validation_clone\outputs\validation_runtime"
            r"\full_wrong\test_runtime_summary.json",
        ),
        (
            "summary_path",
            rf"Z:retired_validation_clone\outputs\validation_runtime\{RUN_ID}"
            r"\test_runtime_summary.json",
        ),
        (
            "summary_path",
            rf"Z:\retired_validation_clone\..\outputs\validation_runtime\{RUN_ID}"
            r"\test_runtime_summary.json",
        ),
        (
            "summary_path",
            rf"Z:\retired_validation_clone\x00\outputs\validation_runtime\{RUN_ID}"
            r"\test_runtime_summary.json",
        ),
    ],
)
def test_builder_rejects_source_locator_wrong_run_relative_traversal_or_null(
    tmp_path: Path,
    field_name: str,
    value: str,
) -> None:
    fixture = _parent_fixture(tmp_path)
    summary, _ = _summary(fixture)
    summary[field_name] = value.replace(r"\x00", "\x00")
    fixture.summary_path.write_bytes(canonical_json_bytes(summary))

    with pytest.raises(
        ValidationParentRunImportError,
        match="PARENT_RUN_IMPORT_SOURCE_LOCATOR_INVALID",
    ):
        build_parent_run_import(fixture.summary_path, repo_root=fixture.repo_root)


def test_builder_rejects_source_repository_prefix_mismatch(tmp_path: Path) -> None:
    fixture = _parent_fixture(tmp_path)
    summary, _ = _summary(fixture)
    other_profile = (
        rf"Y:\different_clone\outputs\validation_runtime\{RUN_ID}" r"\test_runtime_profile.json"
    )
    summary["runtime_profile_path"] = other_profile
    output_artifacts = copy.deepcopy(summary["output_artifacts"])
    assert isinstance(output_artifacts, list)
    output_artifacts[0]["path"] = other_profile
    summary["output_artifacts"] = output_artifacts
    fixture.summary_path.write_bytes(canonical_json_bytes(summary))

    with pytest.raises(
        ValidationParentRunImportError,
        match="PARENT_RUN_IMPORT_SOURCE_PREFIX_MISMATCH",
    ):
        build_parent_run_import(fixture.summary_path, repo_root=fixture.repo_root)


@pytest.mark.parametrize("inventory_variant", ["missing", "duplicate", "stale"])
def test_builder_rejects_missing_duplicate_or_stale_profile_inventory(
    tmp_path: Path,
    inventory_variant: str,
) -> None:
    fixture = _parent_fixture(tmp_path)
    summary, _ = _summary(fixture)
    records = copy.deepcopy(summary["output_artifacts"])
    assert isinstance(records, list)
    if inventory_variant == "missing":
        records = []
    elif inventory_variant == "duplicate":
        records.append(copy.deepcopy(records[0]))
    else:
        records[0]["sha256"] = "f" * 64
    summary["output_artifacts"] = records
    fixture.summary_path.write_bytes(canonical_json_bytes(summary))

    with pytest.raises(ValidationParentRunImportError, match="INVENTORY"):
        build_parent_run_import(fixture.summary_path, repo_root=fixture.repo_root)


def test_validator_rejects_summary_and_profile_byte_drift(tmp_path: Path) -> None:
    fixture = _parent_fixture(tmp_path)
    build_parent_run_import(fixture.summary_path, repo_root=fixture.repo_root)
    summary, original_summary_bytes = _summary(fixture)
    original_profile_bytes = fixture.profile_path.read_bytes()

    fixture.summary_path.write_bytes(original_summary_bytes + b" ")
    _, summary_errors = _validate(
        fixture,
        summary=summary,
        summary_bytes=original_summary_bytes,
        profile_bytes=original_profile_bytes,
    )
    assert any("SUMMARY_BYTES_DRIFT" in error for error in summary_errors)
    assert any("HASH_MISMATCH" in error for error in summary_errors)

    fixture.summary_path.write_bytes(original_summary_bytes)
    fixture.profile_path.write_bytes(original_profile_bytes + b" ")
    _, profile_errors = _validate(
        fixture,
        summary=summary,
        summary_bytes=original_summary_bytes,
        profile_bytes=original_profile_bytes,
    )
    assert any("PROFILE_BYTES_DRIFT" in error for error in profile_errors)
    assert any("HASH_MISMATCH" in error for error in profile_errors)


@pytest.mark.parametrize(
    ("mutation", "error_fragment"),
    [
        ({"run_id": "full_wrong"}, "RUN_ID_MISMATCH"),
        (
            {"imported_summary_path": "../test_runtime_summary.json"},
            "SUMMARY_LOCATOR_INVALID",
        ),
        ({"source_git_commit": "f" * 40}, "COMMIT_MISMATCH"),
        ({"summary_sha256": "f" * 64}, "HASH_MISMATCH"),
        ({"status": "FAIL"}, "STATUS_INVALID"),
        ({"production_effect": "weights_written"}, "PRODUCTION_EFFECT_INVALID"),
        ({"unexpected": True}, "KEYS_MISMATCH"),
    ],
)
def test_validator_rejects_manifest_semantic_tamper(
    tmp_path: Path,
    mutation: dict[str, object],
    error_fragment: str,
) -> None:
    fixture = _parent_fixture(tmp_path)
    build_parent_run_import(fixture.summary_path, repo_root=fixture.repo_root)
    manifest = load_strict_json_bytes(
        fixture.manifest_path.read_bytes(),
        label="manifest",
    )
    manifest.update(mutation)
    fixture.manifest_path.write_bytes(canonical_json_bytes(manifest))

    validated, errors = _validate(fixture)

    assert validated is None
    assert any(error_fragment in error for error in errors)


@pytest.mark.parametrize(
    "manifest_bytes",
    [
        b'{"schema_version":"validation_parent_run_import.v1",'
        b'"schema_version":"validation_parent_run_import.v1"}',
        b'{"schema_version":"validation_parent_run_import.v1","size":NaN}',
    ],
)
def test_validator_rejects_duplicate_or_nonfinite_manifest(
    tmp_path: Path,
    manifest_bytes: bytes,
) -> None:
    fixture = _parent_fixture(tmp_path)
    build_parent_run_import(fixture.summary_path, repo_root=fixture.repo_root)
    fixture.manifest_path.write_bytes(manifest_bytes)

    validated, errors = _validate(fixture)

    assert validated is None
    assert any("PARENT_RUN_IMPORT_JSON_INVALID" in error for error in errors)


def test_validator_rejects_noncanonical_manifest_byte_tamper(tmp_path: Path) -> None:
    fixture = _parent_fixture(tmp_path)
    build_parent_run_import(fixture.summary_path, repo_root=fixture.repo_root)
    fixture.manifest_path.write_bytes(fixture.manifest_path.read_bytes() + b" ")

    validated, errors = _validate(fixture)

    assert validated is None
    assert any("MANIFEST_NONCANONICAL" in error for error in errors)


def test_builder_rejects_parent_outside_repository_runtime_root(tmp_path: Path) -> None:
    fixture = _parent_fixture(tmp_path)
    outside_root = tmp_path / "outside"
    outside_run = outside_root / "outputs" / "validation_runtime" / RUN_ID
    outside_run.mkdir(parents=True)
    outside_summary = outside_run / fixture.summary_path.name
    outside_profile = outside_run / fixture.profile_path.name
    outside_summary.write_bytes(fixture.summary_path.read_bytes())
    outside_profile.write_bytes(fixture.profile_path.read_bytes())

    with pytest.raises(
        ValidationParentRunImportError,
        match="outside repository root",
    ):
        build_parent_run_import(outside_summary, repo_root=fixture.repo_root)


def test_validator_rejects_profile_that_is_not_fixed_sibling(tmp_path: Path) -> None:
    fixture = _parent_fixture(tmp_path)
    build_parent_run_import(fixture.summary_path, repo_root=fixture.repo_root)
    wrong_profile = fixture.repo_root / "outputs" / "validation_runtime" / "wrong.json"
    wrong_profile.write_bytes(fixture.profile_path.read_bytes())

    validated, errors = _validate(fixture, profile_path=wrong_profile)

    assert validated is None
    assert any("FIXED_SIBLING_INVALID" in error for error in errors)


def test_validator_rejects_manifest_symlink(tmp_path: Path) -> None:
    fixture = _parent_fixture(tmp_path)
    build_parent_run_import(fixture.summary_path, repo_root=fixture.repo_root)
    target = tmp_path / "outside_manifest.json"
    target.write_bytes(fixture.manifest_path.read_bytes())
    fixture.manifest_path.unlink()
    try:
        fixture.manifest_path.symlink_to(target)
    except OSError as exc:
        pytest.skip(f"symlink creation is unavailable: {exc}")

    validated, errors = _validate(fixture)

    assert validated is None
    assert any("MANIFEST_SYMLINK" in error or "FIXED_SIBLING_INVALID" in error for error in errors)


def test_builder_rejects_validation_runtime_link_escape(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    outputs = repo_root / "outputs"
    outputs.mkdir(parents=True)
    outside_runtime = tmp_path / "outside_runtime"
    fixture = _parent_fixture(outside_runtime)
    validation_runtime = outputs / "validation_runtime"
    try:
        validation_runtime.symlink_to(
            fixture.repo_root / "outputs" / "validation_runtime",
            target_is_directory=True,
        )
    except OSError as exc:
        pytest.skip(f"directory symlink creation is unavailable: {exc}")
    linked_summary = validation_runtime / RUN_ID / "test_runtime_summary.json"

    with pytest.raises(ValidationParentRunImportError, match="SYMLINK|ROOT_INVALID"):
        build_parent_run_import(linked_summary, repo_root=repo_root)


def _sha256(raw_bytes: bytes) -> str:
    import hashlib

    return hashlib.sha256(raw_bytes).hexdigest()
