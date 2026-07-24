from __future__ import annotations

from copy import deepcopy

import pytest

from ai_trading_system.platform.validation_trigger_provenance import (
    PARENT_RUN_IMPORT_KEYS,
    PARENT_RUN_KEYS,
    PROVENANCE_KEYS,
    validate_full_provenance,
)


def _direct_parent_run() -> dict[str, object]:
    return {
        "run_id": "full_parent_failure",
        "summary_path": (
            "outputs/validation_runtime/full_parent_failure/" "test_runtime_summary.json"
        ),
        "summary_sha256": "1" * 64,
        "runtime_profile_sha256": "2" * 64,
        "report_type": "test_runtime_summary",
        "resolved_tier": "full",
        "status": "FAIL",
        "failure_basis": "PYTEST_FAIL",
        "production_effect": "none",
    }


def _portable_import_parent_run() -> dict[str, object]:
    return {
        **_direct_parent_run(),
        "locator_mode": "portable_import_v1",
        "import_manifest_path": (
            "outputs/validation_runtime/full_parent_failure/" "validation_parent_run_import.json"
        ),
        "import_manifest_sha256": "3" * 64,
    }


def _failure_fix_provenance(parent_run: object) -> dict[str, object]:
    return {
        "schema_version": "validation_trigger_provenance.v1",
        "status": "PASS",
        "required_for_tier": True,
        "trigger_reason": "failure_fix_rerun",
        "task_id": "ENG-VAL-010",
        "boundary_id": "portable-parent-import",
        "parent_run": parent_run,
        "envelope_source": "cli",
        "field_sources": {
            "trigger_reason": "cli",
            "task_id": "cli",
            "boundary_id": "cli",
            "parent_run": "cli",
        },
        "cli_over_environment_precedence": "whole_envelope",
        "validation_errors": [],
    }


def test_legacy_direct_parent_run_exact_shape_remains_valid() -> None:
    parent_run = _direct_parent_run()
    payload = _failure_fix_provenance(parent_run)

    assert set(parent_run) == PARENT_RUN_KEYS
    assert set(payload) == PROVENANCE_KEYS
    assert validate_full_provenance(payload) == []


def test_portable_import_parent_run_exact_shape_is_valid() -> None:
    parent_run = _portable_import_parent_run()
    payload = _failure_fix_provenance(parent_run)

    assert set(parent_run) == PARENT_RUN_IMPORT_KEYS
    assert set(payload) == PROVENANCE_KEYS
    assert validate_full_provenance(payload) == []


@pytest.mark.parametrize(
    "parent_run",
    [
        {
            **_direct_parent_run(),
            "locator_mode": "portable_import_v1",
        },
        {
            **_direct_parent_run(),
            "import_manifest_path": (
                "outputs/validation_runtime/full_parent_failure/"
                "validation_parent_run_import.json"
            ),
            "import_manifest_sha256": "3" * 64,
        },
        {
            **_portable_import_parent_run(),
            "unexpected": "not-allowed",
        },
    ],
)
def test_portable_import_parent_run_rejects_partial_or_extra_keys(
    parent_run: dict[str, object],
) -> None:
    errors = validate_full_provenance(_failure_fix_provenance(parent_run))

    assert any("parent_run keys mismatch" in error for error in errors)


@pytest.mark.parametrize("locator_mode", [None, "", "portable_import_v2"])
def test_portable_import_parent_run_requires_exact_locator_mode(
    locator_mode: object,
) -> None:
    parent_run = _portable_import_parent_run()
    parent_run["locator_mode"] = locator_mode

    errors = validate_full_provenance(_failure_fix_provenance(parent_run))

    assert "parent_run locator_mode is invalid" in errors


@pytest.mark.parametrize(
    "import_manifest_path",
    [
        ("outputs/validation_runtime/other_run/" "validation_parent_run_import.json"),
        ("outputs\\validation_runtime\\full_parent_failure\\" "validation_parent_run_import.json"),
        ("outputs/validation_runtime/full_parent_failure/../" "validation_parent_run_import.json"),
        ("/outputs/validation_runtime/full_parent_failure/" "validation_parent_run_import.json"),
        (
            "outputs/validation_runtime/full_parent_failure/nested/"
            "validation_parent_run_import.json"
        ),
        ("outputs/validation_runtime/full_parent_failure/" "wrong_manifest.json"),
    ],
)
def test_portable_import_parent_run_rejects_noncanonical_manifest_path(
    import_manifest_path: str,
) -> None:
    parent_run = _portable_import_parent_run()
    parent_run["import_manifest_path"] = import_manifest_path

    errors = validate_full_provenance(_failure_fix_provenance(parent_run))

    assert "parent_run import_manifest_path is invalid" in errors


@pytest.mark.parametrize(
    "import_manifest_sha256",
    [None, "", "3" * 63, "G" * 64, "A" * 64],
)
def test_portable_import_parent_run_requires_lowercase_sha256(
    import_manifest_sha256: object,
) -> None:
    parent_run = _portable_import_parent_run()
    parent_run["import_manifest_sha256"] = import_manifest_sha256

    errors = validate_full_provenance(_failure_fix_provenance(parent_run))

    assert "parent_run import_manifest_sha256 is invalid" in errors


def test_portable_import_parent_run_does_not_change_top_level_schema() -> None:
    payload = _failure_fix_provenance(_portable_import_parent_run())
    payload_with_extra = deepcopy(payload)
    payload_with_extra["import_manifest_path"] = (
        "outputs/validation_runtime/full_parent_failure/" "validation_parent_run_import.json"
    )

    errors = validate_full_provenance(payload_with_extra)

    assert any(error.startswith("validation provenance keys mismatch") for error in errors)
