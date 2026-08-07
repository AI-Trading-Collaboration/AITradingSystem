from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.daily_capability_gate import (
    EXPECTED_AGGREGATE_FIELDS,
    EXPECTED_SESSIONS,
)
from ai_trading_system.qqq_options_research.daily_capability_gate_retry import (
    DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_POLICY_PATH,
    DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_PROPOSAL_PATH,
    EXPECTED_RETRY_ALLOWED_ACTIONS,
    EXPECTED_RETRY_PROHIBITED_ACTIONS,
    PREDECESSOR_BLOCKED_BUILD_ID,
    PREDECESSOR_BLOCKED_SCREENSHOT_SHA256,
    PREDECESSOR_POLICY_CANONICAL_SHA256,
    PREDECESSOR_PROPOSAL_CONTENT_SHA256,
    PREDECESSOR_PROPOSAL_FILE_SHA256,
    PREDECESSOR_SCRIPT_LF_SHA256,
    PROPOSED_RETRY_OWNER_DECISION,
    DailyCapabilityRetryRunScope,
    QCQQQOptionsDailyCapabilityGateRetryPolicy,
    QCQQQOptionsDailyCapabilityGateRetryProposal,
    build_qc_qqq_options_daily_capability_gate_retry_proposal,
    load_qc_qqq_options_daily_capability_gate_retry_policy,
    load_qc_qqq_options_daily_capability_gate_retry_proposal,
)

REPOSITORY_CODE_SHA = "1" * 40
TRACKED_IMPLEMENTATION_COMMIT = "c880bb9e55dbcf5c641756e80fdd2f9d00eaa0e2"
TRACKED_PROPOSAL_FILE_SHA256 = "d5ecad8167e2abef7e5a8d6427604da5b6f59d4be50607228097191eba74239e"
TRACKED_PROPOSAL_CONTENT_SHA256 = "77570e7ff88e1c567c29d10dcfc534cef07628cab58ceb894da79c6075f013b9"
TRACKED_AUTHORITY_SET_SHA256 = "52f8246d8192f4fbf40c3aa415aee56bdbb5eb937f4778daa30fda42f06ad3a2"


def _proposal() -> QCQQQOptionsDailyCapabilityGateRetryProposal:
    return build_qc_qqq_options_daily_capability_gate_retry_proposal(
        record_id="qc_qqq_options_daily_capability_gate_retry_proposal_20260808_v1",
        created_at_utc=datetime(2026, 8, 8, tzinfo=UTC),
        repository_code_sha=REPOSITORY_CODE_SHA,
    )


def test_policy_loads_and_replays_2498_predecessor() -> None:
    loaded = load_qc_qqq_options_daily_capability_gate_retry_policy()
    assert loaded.predecessor_proposal_path.name == (
        "qc_qqq_options_daily_capability_gate_proposal_20260808.json"
    )
    assert loaded.policy.predecessor_proposal_file_sha256 == (PREDECESSOR_PROPOSAL_FILE_SHA256)
    assert loaded.predecessor.content_sha256 == PREDECESSOR_PROPOSAL_CONTENT_SHA256
    assert loaded.policy.predecessor_policy_canonical_sha256 == (
        PREDECESSOR_POLICY_CANONICAL_SHA256
    )
    assert loaded.predecessor.owner_token_present is False
    assert loaded.predecessor.gate_status == "UNKNOWN_EVIDENCE_INCOMPLETE"


def test_policy_freezes_verified_account_retry_without_project_mutation() -> None:
    policy = load_qc_qqq_options_daily_capability_gate_retry_policy().policy
    scope = policy.run_scope
    assert scope.target_project_id == 34808569
    assert scope.required_project_code_lf_sha256 == PREDECESSOR_SCRIPT_LF_SHA256
    assert scope.requested_start == date(2021, 2, 22)
    assert scope.requested_end == date(2021, 2, 26)
    assert scope.expected_sessions == EXPECTED_SESSIONS
    assert scope.maximum_project_mutations == 0
    assert scope.maximum_cloud_backtests == 1
    assert scope.maximum_orders == 0
    assert scope.maximum_fills == 0
    assert scope.account_verification_precheck_required is True
    assert policy.required_aggregate_fields == EXPECTED_AGGREGATE_FIELDS
    assert policy.allowed_actions_after_exact_owner_token == EXPECTED_RETRY_ALLOWED_ACTIONS
    assert policy.prohibited_actions == EXPECTED_RETRY_PROHIBITED_ACTIONS


def test_policy_binds_blocked_attempt_and_pending_independent_review() -> None:
    policy = load_qc_qqq_options_daily_capability_gate_retry_policy().policy
    assert policy.predecessor_script_lf_sha256 == PREDECESSOR_SCRIPT_LF_SHA256
    assert policy.predecessor_blocked_build_id == PREDECESSOR_BLOCKED_BUILD_ID
    assert policy.predecessor_blocked_screenshot_sha256 == (PREDECESSOR_BLOCKED_SCREENSHOT_SHA256)
    assert policy.predecessor_authorization_status == "INVALIDATED_SINGLE_USE"
    assert policy.predecessor_backtest_id_present is False
    assert policy.predecessor_independent_review_status == "PENDING_INDEPENDENT_REVIEW"
    assert policy.account_verification_claim_status == ("OWNER_CLAIMED_REQUIRES_UI_CONFIRMATION")


def test_retry_proposal_is_deterministic_and_unauthorized() -> None:
    first = _proposal()
    second = _proposal()
    assert first == second
    assert first.canonical_bytes == second.canonical_bytes
    assert first.proposed_owner_decision == PROPOSED_RETRY_OWNER_DECISION
    assert first.authorization_status == "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
    assert first.owner_token_present is False
    assert first.gate_status == "UNKNOWN_EVIDENCE_INCOMPLETE"
    assert first.safety.external_platform_action_authorized is False
    assert first.safety.project_mutation_authorized is False
    assert first.safety.cloud_backtest_authorized is False


def test_tracked_retry_proposal_replays_exact_implementation_authority() -> None:
    loaded = load_qc_qqq_options_daily_capability_gate_retry_proposal()
    proposal = loaded.proposal

    assert loaded.proposal_path == (
        PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_PROPOSAL_PATH
    )
    assert loaded.proposal_file_sha256 == TRACKED_PROPOSAL_FILE_SHA256
    assert proposal.repository_code_sha == TRACKED_IMPLEMENTATION_COMMIT
    assert proposal.content_sha256 == TRACKED_PROPOSAL_CONTENT_SHA256
    assert proposal.authority_set_sha256 == TRACKED_AUTHORITY_SET_SHA256
    assert proposal.authorization_status == "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
    assert proposal.owner_token_present is False


def test_retry_proposal_canonical_round_trip_and_format_tamper() -> None:
    proposal = _proposal()
    assert (
        QCQQQOptionsDailyCapabilityGateRetryProposal.from_json_bytes(proposal.canonical_bytes)
        == proposal
    )
    compact = json.dumps(proposal.model_dump(mode="json"), sort_keys=True).encode("utf-8")
    with pytest.raises(ValueError, match="not canonical"):
        QCQQQOptionsDailyCapabilityGateRetryProposal.from_json_bytes(compact)


@pytest.mark.parametrize(
    ("field", "replacement", "message"),
    [
        ("predecessor_proposal_file_sha256", "a" * 64, "proposal_file_sha256 drifted"),
        ("predecessor_script_lf_sha256", "b" * 64, "script_lf_sha256 drifted"),
        (
            "predecessor_blocked_screenshot_sha256",
            "c" * 64,
            "blocked_screenshot_sha256 drifted",
        ),
    ],
)
def test_predecessor_identity_tamper_fails_closed(
    field: str, replacement: str, message: str
) -> None:
    payload = load_qc_qqq_options_daily_capability_gate_retry_policy().policy.model_dump(
        mode="python"
    )
    payload[field] = replacement
    with pytest.raises(ValidationError, match=message):
        QCQQQOptionsDailyCapabilityGateRetryPolicy.model_validate(payload)


@pytest.mark.parametrize(
    ("field", "replacement", "message"),
    [
        ("required_aggregate_fields", ("contract_count",), "aggregate field"),
        ("allowed_actions_after_exact_owner_token", ("QUANTCONNECT_LOGIN",), "allowed action"),
        ("prohibited_actions", ("API",), "prohibited action"),
        ("proposed_owner_decision", "owner_decision:forged", "Owner decision"),
    ],
)
def test_retry_policy_inventory_or_authority_drift_fails_closed(
    field: str, replacement: object, message: str
) -> None:
    payload = load_qc_qqq_options_daily_capability_gate_retry_policy().policy.model_dump(
        mode="python"
    )
    payload[field] = replacement
    with pytest.raises(ValidationError, match=message):
        QCQQQOptionsDailyCapabilityGateRetryPolicy.model_validate(payload)


def test_range_session_and_code_identity_drift_fail_closed() -> None:
    payload = load_qc_qqq_options_daily_capability_gate_retry_policy().policy.run_scope.model_dump(
        mode="python"
    )
    payload["requested_start"] = date(2021, 2, 23)
    with pytest.raises(ValidationError, match="primary-window start"):
        DailyCapabilityRetryRunScope.model_validate(payload)

    payload = load_qc_qqq_options_daily_capability_gate_retry_policy().policy.run_scope.model_dump(
        mode="python"
    )
    payload["expected_sessions"] = tuple(reversed(EXPECTED_SESSIONS))
    with pytest.raises(ValidationError, match="session inventory"):
        DailyCapabilityRetryRunScope.model_validate(payload)

    payload = load_qc_qqq_options_daily_capability_gate_retry_policy().policy.run_scope.model_dump(
        mode="python"
    )
    payload["required_project_code_lf_sha256"] = "d" * 64
    with pytest.raises(ValidationError, match="code identity"):
        DailyCapabilityRetryRunScope.model_validate(payload)


def test_retry_scope_cannot_promote_project_mutation_orders_or_fills() -> None:
    scope = load_qc_qqq_options_daily_capability_gate_retry_policy().policy.run_scope
    for field in ("maximum_project_mutations", "maximum_orders", "maximum_fills"):
        payload = scope.model_dump(mode="python")
        payload[field] = 1
        with pytest.raises(ValidationError):
            DailyCapabilityRetryRunScope.model_validate(payload)


def test_policy_path_is_repository_relative_and_exists() -> None:
    assert not DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_POLICY_PATH.is_absolute()
    assert (PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_POLICY_PATH).is_file()
