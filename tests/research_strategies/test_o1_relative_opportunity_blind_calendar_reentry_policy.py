from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest
import yaml

from scripts.trading2467_validate_o1_blind_calendar_reentry_policy import (
    IMMUTABLE_SECTIONS,
    POLICY_PATH,
    STABLE_TASK_ID,
    canonical_policy_bytes,
    validate_blind_calendar_reentry_policy,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
POLICY_FILE = PROJECT_ROOT / POLICY_PATH


def _load_policy() -> dict[str, Any]:
    payload = yaml.safe_load(POLICY_FILE.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _write_policy(tmp_path: Path, payload: dict[str, Any]) -> Path:
    path = tmp_path / "policy.yaml"
    path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return path


def _validate_tmp(tmp_path: Path, payload: dict[str, Any]):
    return validate_blind_calendar_reentry_policy(
        project_root=PROJECT_ROOT,
        policy_path=_write_policy(tmp_path, payload),
    )


def _set_nested(payload: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    target = payload
    for key in path[:-1]:
        child = target[key]
        assert isinstance(child, dict)
        target = child
    target[path[-1]] = value


def _reverse_mappings(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _reverse_mappings(value[key])
            for key in reversed(tuple(value))
        }
    if isinstance(value, list):
        return [_reverse_mappings(member) for member in value]
    return value


def test_canonical_inactive_policy_passes_independent_validator() -> None:
    result = validate_blind_calendar_reentry_policy(project_root=PROJECT_ROOT)
    assert result.status == "PASS"
    assert result.error_count == 0
    assert result.errors == ()
    assert result.task_id == STABLE_TASK_ID
    assert result.to_dict()["production_effect"] == "none"
    assert result.to_dict()["broker_action"] == "none"


def test_canonical_policy_has_no_unselected_or_execution_authority() -> None:
    policy = _load_policy()
    serialized = canonical_policy_bytes(policy)
    authorization = policy["authorization"]
    look_budget = policy["look_budget_contract"]
    safety = policy["safety"]

    assert b"NOT_SELECTED" not in serialized
    assert policy["activation_allowed"] is False
    assert policy["automatic_execution_allowed"] is False
    assert look_budget["maximum_reentry_coverage_looks"] == 1
    assert look_budget["rolling_window"] is False
    assert look_budget["automatic_retry_allowed"] is False
    assert look_budget["resume_allowed"] is False
    assert look_budget["overwrite_allowed"] is False
    assert look_budget["second_candidate_allowed"] is False
    assert authorization["data_acquisition_allowed"] is False
    assert authorization["dq_execution_allowed"] is False
    assert authorization["coverage_read_allowed"] is False
    assert authorization["model_training_allowed"] is False
    assert authorization["canonical_run_allowed"] is False
    assert authorization["falsification_execution_allowed"] is False
    assert authorization["decision_value_audit_allowed"] is False
    assert authorization["candidate_or_backtest_allowed"] is False
    assert safety["retained_runtime_evidence_bytes_read_by_this_task"] is False
    assert safety["cached_market_or_macro_data_required"] is False


def test_deterministic_serialization_is_order_independent_and_byte_stable() -> None:
    policy = _load_policy()
    reordered = _reverse_mappings(policy)
    once = canonical_policy_bytes(policy)
    twice = canonical_policy_bytes(policy)
    reordered_bytes = canonical_policy_bytes(reordered)

    assert once == twice == reordered_bytes


@pytest.mark.parametrize(
    ("path", "value", "expected_error"),
    [
        (
            (
                "authority",
                "source_bindings",
                "o1_v1_policy",
                "git_blob_sha1_at_exact_source_commit",
            ),
            "0" * 40,
            "SOURCE_GIT_BLOB_MISMATCH:o1_v1_policy",
        ),
        (
            (
                "authority",
                "source_bindings",
                "o1_v1_policy",
                "content_sha256_at_exact_source_commit",
            ),
            "0" * 64,
            "SOURCE_CONTENT_SHA256_MISMATCH:o1_v1_policy",
        ),
        (
            ("v1_immutable_contract", "immutable_section_sha256", "coverage_contract"),
            "f" * 64,
            "V1_IMMUTABLE_SECTION_HASH_MISMATCH:coverage_contract",
        ),
    ],
)
def test_validator_rejects_v1_git_content_or_section_identity_tamper(
    tmp_path: Path,
    path: tuple[str, ...],
    value: Any,
    expected_error: str,
) -> None:
    policy = copy.deepcopy(_load_policy())
    _set_nested(policy, path, value)
    result = _validate_tmp(tmp_path, policy)

    assert result.status == "FAIL"
    assert expected_error in result.errors


def test_canonical_policy_binds_all_nine_v1_sections() -> None:
    policy = _load_policy()
    section_hashes = policy["v1_immutable_contract"]["immutable_section_sha256"]

    assert set(section_hashes) == set(IMMUTABLE_SECTIONS)
    assert all(
        isinstance(section_hashes[name], str) and len(section_hashes[name]) == 64
        for name in IMMUTABLE_SECTIONS
    )


@pytest.mark.parametrize(
    ("path", "value", "expected_error"),
    [
        (
            ("route_contract", "threshold_or_model_redesign_selected"),
            True,
            "ROUTE_CONTRACT_MISMATCH",
        ),
        (
            ("calendar_trigger", "not_before_date"),
            "2027-02-02",
            "CALENDAR_TRIGGER_MISMATCH",
        ),
        (
            ("data_vintage_contract", "source_publication_cutoff_inclusive"),
            "2027-02-01T00:00:00-05:00",
            "DATA_VINTAGE_FIELD_MISMATCH:source_publication_cutoff_inclusive",
        ),
        (
            ("data_vintage_contract", "requested_end"),
            "2027-01-28",
            "DATA_VINTAGE_FIELD_MISMATCH:requested_end",
        ),
        (
            ("data_vintage_contract", "evaluated_end"),
            "2027-01-21",
            "DATA_VINTAGE_FIELD_MISMATCH:evaluated_end",
        ),
    ],
)
def test_validator_rejects_route_date_or_vintage_tamper(
    tmp_path: Path,
    path: tuple[str, ...],
    value: Any,
    expected_error: str,
) -> None:
    policy = copy.deepcopy(_load_policy())
    _set_nested(policy, path, value)
    result = _validate_tmp(tmp_path, policy)

    assert result.status == "FAIL"
    assert expected_error in result.errors


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("look_budget_contract", "maximum_reentry_coverage_looks"), 2),
        (("look_budget_contract", "rolling_window"), True),
        (("look_budget_contract", "automatic_rollover"), True),
        (("look_budget_contract", "automatic_retry_allowed"), True),
        (("look_budget_contract", "resume_allowed"), True),
        (("look_budget_contract", "overwrite_allowed"), True),
        (("look_budget_contract", "second_candidate_allowed"), True),
        (("look_budget_contract", "result_driven_rerun_allowed"), True),
    ],
)
def test_validator_rejects_rolling_retry_resume_overwrite_or_second_look(
    tmp_path: Path,
    path: tuple[str, ...],
    value: Any,
) -> None:
    policy = copy.deepcopy(_load_policy())
    _set_nested(policy, path, value)
    result = _validate_tmp(tmp_path, policy)

    assert result.status == "FAIL"
    assert "LOOK_BUDGET_CONTRACT_MISMATCH" in result.errors


@pytest.mark.parametrize(
    ("path", "value", "expected_error"),
    [
        (
            (
                "data_vintage_contract",
                "pit_requirements",
                "unknown_historical_timestamp_allowed",
            ),
            True,
            "PIT_REQUIREMENTS_MISMATCH",
        ),
        (
            (
                "data_vintage_contract",
                "pit_requirements",
                "current_view_substitution_allowed",
            ),
            True,
            "PIT_REQUIREMENTS_MISMATCH",
        ),
        (
            (
                "data_vintage_contract",
                "canonical_manifest",
                "exact_vintage_identity",
            ),
            "O1_V2_VINTAGE_SHA256_UNAUTHORIZED",
            "CANONICAL_MANIFEST_FIELD_MISMATCH:exact_vintage_identity",
        ),
        (
            (
                "data_vintage_contract",
                "canonical_manifest",
                "deterministic_utf8_serialization",
            ),
            False,
            "CANONICAL_MANIFEST_FIELD_MISMATCH:deterministic_utf8_serialization",
        ),
    ],
)
def test_validator_rejects_pit_or_exact_vintage_contract_tamper(
    tmp_path: Path,
    path: tuple[str, ...],
    value: Any,
    expected_error: str,
) -> None:
    policy = copy.deepcopy(_load_policy())
    _set_nested(policy, path, value)
    result = _validate_tmp(tmp_path, policy)

    assert result.status == "FAIL"
    assert expected_error in result.errors


@pytest.mark.parametrize(
    ("path", "value", "expected_error"),
    [
        (("activation_allowed",), True, "POLICY_FLAG_ENABLED:activation_allowed"),
        (
            ("automatic_execution_allowed",),
            True,
            "POLICY_FLAG_ENABLED:automatic_execution_allowed",
        ),
        (
            ("authorization", "coverage_read_allowed"),
            True,
            "AUTHORIZATION_BOUNDARY_MISMATCH",
        ),
        (
            ("authorization", "model_training_allowed"),
            True,
            "AUTHORIZATION_BOUNDARY_MISMATCH",
        ),
        (
            ("authorization", "decision_value_audit_allowed"),
            True,
            "AUTHORIZATION_BOUNDARY_MISMATCH",
        ),
        (
            ("authorization", "candidate_or_backtest_allowed"),
            True,
            "AUTHORIZATION_BOUNDARY_MISMATCH",
        ),
    ],
)
def test_validator_rejects_activation_empirical_or_downstream_authority(
    tmp_path: Path,
    path: tuple[str, ...],
    value: Any,
    expected_error: str,
) -> None:
    policy = copy.deepcopy(_load_policy())
    _set_nested(policy, path, value)
    result = _validate_tmp(tmp_path, policy)

    assert result.status == "FAIL"
    assert expected_error in result.errors


def test_validator_rejects_not_selected_and_stop_matrix_tamper(
    tmp_path: Path,
) -> None:
    policy = copy.deepcopy(_load_policy())
    policy["route_contract"]["route_id"] = "NOT_SELECTED"
    policy["outcome_contract"]["PASS"]["model_training_allowed"] = True
    policy["outcome_contract"]["INSUFFICIENT"]["retry_allowed"] = True
    policy["outcome_contract"]["INVALID"]["conditions"].remove(
        "INDEPENDENT_VALIDATOR_ERROR"
    )
    result = _validate_tmp(tmp_path, policy)

    assert result.status == "FAIL"
    assert "NOT_SELECTED_PRESENT:$.route_contract.route_id" in result.errors
    assert "STOP_MATRIX_PASS_MODEL_TRAINING_ALLOWED" in result.errors
    assert "STOP_MATRIX_INSUFFICIENT_MISMATCH" in result.errors
    assert "STOP_MATRIX_INVALID_MISMATCH" in result.errors


def test_validator_rejects_owner_or_future_attempt_authority_tamper(
    tmp_path: Path,
) -> None:
    policy = copy.deepcopy(_load_policy())
    policy["authority"]["owner_decisions"]["inactive_policy_freeze"]["accepted"] = False
    policy["future_attempt_contract"][
        "generic_acquisition_or_dq_authorized_by_this_policy"
    ] = True
    policy["future_attempt_contract"]["separate_real_run_owner_token_status"] = (
        "GRANTED"
    )
    result = _validate_tmp(tmp_path, policy)

    assert result.status == "FAIL"
    assert "OWNER_DECISION_NOT_ACCEPTED:inactive_policy_freeze" in result.errors
    assert "FUTURE_ATTEMPT_CONTRACT_MISMATCH" in result.errors
