from __future__ import annotations

import copy
from pathlib import Path

import yaml

from scripts.trading2465_validate_o1_reentry_preregistration import (
    PROPOSAL_PATH,
    TASK_ID,
    validate_reentry_preregistration,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROPOSAL_FILE = PROJECT_ROOT / PROPOSAL_PATH
REQUIREMENT_FILE = (
    PROJECT_ROOT
    / "docs"
    / "requirements"
    / "TRADING-2465_Post_O1_Route_Decision_And_Blind_Reentry_Preregistration.md"
)
TASK_REGISTER_FILE = PROJECT_ROOT / "docs" / "task_register.md"


def _load_proposal() -> dict[str, object]:
    payload = yaml.safe_load(PROPOSAL_FILE.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _write_proposal(tmp_path: Path, payload: dict[str, object]) -> Path:
    path = tmp_path / "proposal.yaml"
    path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return path


def _validate_tmp(tmp_path: Path, payload: dict[str, object]):
    return validate_reentry_preregistration(
        project_root=PROJECT_ROOT,
        proposal_path=_write_proposal(tmp_path, payload),
    )


def test_canonical_inactive_proposal_passes_independent_validator() -> None:
    result = validate_reentry_preregistration(project_root=PROJECT_ROOT)
    assert result.status == "PASS"
    assert result.error_count == 0
    assert result.errors == ()
    assert result.task_id == TASK_ID


def test_route_and_numeric_policy_slots_remain_unselected() -> None:
    proposal = _load_proposal()
    route = proposal["route_decision"]
    route_a = proposal["route_a_plus_d_policy_slots"]
    route_b = proposal["route_b_policy_slots"]
    route_c = proposal["route_c_policy_slots"]
    assert isinstance(route, dict)
    assert isinstance(route_a, dict)
    assert isinstance(route_b, dict)
    assert isinstance(route_c, dict)

    assert proposal["status"] == "OWNER_REVIEW_REQUIRED_NOT_ACTIVE"
    assert proposal["activation_allowed"] is False
    assert proposal["owner_route_decision"] == "NOT_SELECTED"
    assert route["selected_route"] == "NOT_SELECTED"
    assert route["owner_decision_token"] == "NOT_SELECTED"
    assert route_a["calendar_trigger"] == {
        "trigger_type": "NOT_SELECTED",
        "not_before_date": "NOT_SELECTED",
        "rationale": "NOT_SELECTED",
        "count_based_or_dynamic_trigger_allowed": False,
    }
    assert route_a["look_budget"]["maximum_reentry_coverage_looks"] == ("NOT_SELECTED")
    assert route_a["future_attempt"]["selected_attempt_id"] == "NOT_SELECTED"
    assert route_b["owner_decision_token"] == "NOT_SELECTED"
    assert route_c["owner_decision_token"] == "NOT_SELECTED"


def test_prior_consumed_attempt_and_frozen_o1_contract_are_bound() -> None:
    proposal = _load_proposal()
    prior = proposal["prior_attempt"]
    frozen = proposal["frozen_o1_contract"]
    assert isinstance(prior, dict)
    assert isinstance(frozen, dict)

    assert prior["attempt_id"] == "O1_M1_RIDGE_CROSS_ASSET_STATE_V1"
    assert prior["single_run_consumed"] is True
    assert prior["result_driven_retry_allowed"] is False
    assert prior["coverage_report"]["sha256"] == (
        "bbed79b499b57274dd49bede0c37219894233964732fcde5656626933781ada7"
    )
    assert prior["coverage_gate"]["sha256"] == (
        "a97ee44832a41aeb90a6f9a18b0358eb81cefec4d491438deb6fd27b624f31b8"
    )
    assert prior["retained_evidence_bytes_read_by_this_task"] is False
    assert prior["retained_evidence_mutation_allowed"] is False
    assert prior["retained_evidence_cleanup_allowed"] is False
    assert frozen["exact_reuse_required_for_route_a_plus_d"] is True
    assert set(frozen["immutable_section_sha256"]) == {
        "target_contract",
        "split_contract",
        "coverage_contract",
        "regime_contract",
        "event_contract",
        "model_feature_contract",
        "metric_contract",
        "falsification_contract",
        "classification_contract",
    }


def test_validator_rejects_prior_attempt_sha_tamper(tmp_path: Path) -> None:
    proposal = copy.deepcopy(_load_proposal())
    proposal["prior_attempt"]["coverage_gate"]["sha256"] = "0" * 64
    result = _validate_tmp(tmp_path, proposal)
    assert result.status == "FAIL"
    assert "PRIOR_COVERAGE_GATE_SHA256_MISMATCH" in result.errors


def test_validator_rejects_unauthorized_route_and_calendar_selection(
    tmp_path: Path,
) -> None:
    proposal = copy.deepcopy(_load_proposal())
    proposal["owner_route_decision"] = "A_PLUS_D"
    proposal["route_decision"]["selected_route"] = (
        "A_PLUS_D_O1_BLIND_CALENDAR_REENTRY_WITH_GENERIC_EVIDENCE_INFRASTRUCTURE"
    )
    proposal["route_a_plus_d_policy_slots"]["calendar_trigger"]["not_before_date"] = "2027-02-01"
    result = _validate_tmp(tmp_path, proposal)
    assert result.status == "FAIL"
    assert "OWNER_ROUTE_ALREADY_SELECTED" in result.errors
    assert "ROUTE_SELECTED_WITHOUT_OWNER" in result.errors
    assert "CALENDAR_SLOT_ALREADY_SELECTED:not_before_date" in result.errors


def test_validator_rejects_frozen_contract_tamper(tmp_path: Path) -> None:
    proposal = copy.deepcopy(_load_proposal())
    proposal["frozen_o1_contract"]["immutable_section_sha256"]["coverage_contract"] = "f" * 64
    result = _validate_tmp(tmp_path, proposal)
    assert result.status == "FAIL"
    assert "FROZEN_SECTION_HASH_MISMATCH:coverage_contract" in result.errors


def test_validator_rejects_empirical_or_downstream_authorization(
    tmp_path: Path,
) -> None:
    proposal = copy.deepcopy(_load_proposal())
    proposal["authorization"]["coverage_run_allowed"] = True
    proposal["authorization"]["model_training_allowed"] = True
    proposal["safety"]["decision_value_audit_started"] = True
    result = _validate_tmp(tmp_path, proposal)
    assert result.status == "FAIL"
    assert "UNAUTHORIZED_EMPIRICAL_FLAG:coverage_run_allowed" in result.errors
    assert "UNAUTHORIZED_EMPIRICAL_FLAG:model_training_allowed" in result.errors
    assert "DOWNSTREAM_FLAG_ENABLED:decision_value_audit_started" in result.errors


def test_task_and_requirement_expose_owner_decision_handoff() -> None:
    task_register = TASK_REGISTER_FILE.read_text(encoding="utf-8")
    requirement = REQUIREMENT_FILE.read_text(encoding="utf-8")
    task_line = next(line for line in task_register.splitlines() if line.startswith(f"|{TASK_ID}|"))
    assert "|P0|BASELINE_DONE|" in task_line
    assert "TRADING-2467" in task_line
    assert "2027-02-01" in requirement
    assert "状态：`BASELINE_DONE`" in requirement
    assert "CANNOT_VERIFY_EXACT_BACKEND_ROUTE=true" in requirement
    assert (
        "owner_decision:TRADING-2465:2026-07-30:"
        "select_o1_blind_calendar_reentry_with_generic_evidence_"
        "infrastructure_v1"
    ) in requirement
    assert "TRADING-2467_O1_BLIND_CALENDAR_REENTRY_POLICY_SLOT_FREEZE" in requirement
    assert "INVALID_POST_RESULT_REDESIGN_CONTAMINATION" in requirement
