from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.daily_capability_gate import (
    DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_POLICY_PATH,
    EXPECTED_AGGREGATE_FIELDS,
    EXPECTED_ALLOWED_ACTIONS,
    EXPECTED_PROHIBITED_ACTIONS,
    EXPECTED_SESSIONS,
    PROPOSED_OWNER_DECISION,
    DailyCapabilityGateDecision,
    DailyCapabilitySessionObservation,
    QCQQQOptionsDailyCapabilityGatePolicy,
    QCQQQOptionsDailyCapabilityGateProposal,
    QCQQQOptionsDailyCapabilityGateRecord,
    QCQQQOptionsDailyCapabilityRunObservation,
    build_qc_qqq_options_daily_capability_gate_proposal,
    build_qc_qqq_options_daily_capability_gate_record,
    load_qc_qqq_options_daily_capability_gate_policy,
)

REPOSITORY_CODE_SHA = "1" * 40
PROPOSAL_FILE_SHA = "2" * 64
AUTHORIZATION_TOKEN_SHA = "3" * 64
RESULT_ARTIFACT_SHA = "4" * 64


def _proposal() -> QCQQQOptionsDailyCapabilityGateProposal:
    return build_qc_qqq_options_daily_capability_gate_proposal(
        record_id="qc_qqq_options_daily_capability_gate_proposal_20260808_v1",
        created_at_utc=datetime(2026, 8, 8, tzinfo=UTC),
        repository_code_sha=REPOSITORY_CODE_SHA,
    )


def _session(
    session: date,
    *,
    option_chain_present: bool = True,
    contract_count: int = 10,
    two_sided_quote_count: int = 8,
    positive_open_interest_count: int = 7,
    finite_greeks_count: int = 6,
    finite_implied_volatility_count: int = 6,
) -> DailyCapabilitySessionObservation:
    return DailyCapabilitySessionObservation(
        session=session,
        option_chain_present=option_chain_present,
        contract_count=contract_count,
        two_sided_quote_count=two_sided_quote_count,
        positive_open_interest_count=positive_open_interest_count,
        finite_greeks_count=finite_greeks_count,
        finite_implied_volatility_count=finite_implied_volatility_count,
        raw_rows_logged=False,
        orders_submitted=0,
    )


def _observation(
    *,
    proposal: QCQQQOptionsDailyCapabilityGateProposal | None = None,
    session_observations: tuple[DailyCapabilitySessionObservation, ...] | None = None,
    project_id: int = 34808569,
    requested_start: date = date(2021, 2, 22),
    requested_end: date = date(2021, 2, 26),
    evaluated_start: date = date(2021, 2, 22),
    evaluated_end: date = date(2021, 2, 26),
    free_organization_reviewed: bool = True,
    daily_entitlement_observed: bool = True,
    scope_violation_detected: bool = False,
    exceptions: tuple[str, ...] = (),
) -> QCQQQOptionsDailyCapabilityRunObservation:
    bound_proposal = proposal or _proposal()
    observations = session_observations or tuple(_session(item) for item in EXPECTED_SESSIONS)
    return QCQQQOptionsDailyCapabilityRunObservation.seal(
        schema_version="qc_qqq_options_daily_capability_run_observation.v1",
        record_id="qc_qqq_options_daily_capability_observation_v1",
        observed_at_utc=datetime(2026, 8, 8, 1, tzinfo=UTC),
        proposal_file_sha256=PROPOSAL_FILE_SHA,
        proposal_content_sha256=bound_proposal.content_sha256,
        authorization_token_sha256=AUTHORIZATION_TOKEN_SHA,
        project_id=project_id,
        backtest_id="dailycapabilitybacktest",
        algorithm_id="dailycapabilityalgorithm",
        engine_version="LEAN Engine 2.5",
        build_id="manual-build-id",
        requested_start=requested_start,
        requested_end=requested_end,
        evaluated_start=evaluated_start,
        evaluated_end=evaluated_end,
        free_organization_reviewed=free_organization_reviewed,
        daily_entitlement_observed=daily_entitlement_observed,
        session_observations=observations,
        processed_data_points=5000,
        elapsed_milliseconds=1200,
        orders_submitted=0,
        fills=0,
        portfolio_invested=False,
        raw_rows_logged_or_exported=False,
        scope_violation_detected=scope_violation_detected,
        result_artifact_byte_count=100,
        result_artifact_sha256=RESULT_ARTIFACT_SHA,
        reviewed_by="project_owner",
        exceptions=exceptions,
    )


def _gate(
    observation: QCQQQOptionsDailyCapabilityRunObservation,
    *,
    proposal: QCQQQOptionsDailyCapabilityGateProposal | None = None,
) -> QCQQQOptionsDailyCapabilityGateRecord:
    bound_proposal = proposal or _proposal()
    return build_qc_qqq_options_daily_capability_gate_record(
        record_id="qc_qqq_options_daily_capability_gate_v1",
        created_at_utc=datetime(2026, 8, 8, 2, tzinfo=UTC),
        proposal_file_sha256=PROPOSAL_FILE_SHA,
        proposal=bound_proposal,
        observation=observation,
    )


def test_policy_loads_and_replays_exact_predecessor() -> None:
    loaded = load_qc_qqq_options_daily_capability_gate_policy()
    assert (
        loaded.policy_path
        == (PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_POLICY_PATH).resolve()
    )
    assert loaded.policy.predecessor_aggregate_recommendation == (
        "NO_GO_KEEP_BLOCKED_PRIMARY_WINDOW_AND_SHARED_GATES"
    )
    assert loaded.predecessor.owner_review_completed is False
    assert loaded.predecessor.owner_attestation_present is False
    assert loaded.predecessor.primary_window_status == "NOT_TESTED_ACCOUNT_SPECIFIC"


def test_policy_freezes_daily_zero_order_scope() -> None:
    policy = load_qc_qqq_options_daily_capability_gate_policy().policy
    assert policy.run_scope.requested_start == date(2021, 2, 22)
    assert policy.run_scope.requested_end == date(2021, 2, 26)
    assert policy.run_scope.expected_sessions == EXPECTED_SESSIONS
    assert policy.run_scope.equity_resolution == "DAILY"
    assert policy.run_scope.option_resolution == "DAILY"
    assert policy.run_scope.maximum_orders == 0
    assert policy.run_scope.maximum_fills == 0
    assert policy.required_aggregate_fields == EXPECTED_AGGREGATE_FIELDS
    assert policy.allowed_actions_after_exact_owner_token == EXPECTED_ALLOWED_ACTIONS
    assert policy.prohibited_actions == EXPECTED_PROHIBITED_ACTIONS


def test_proposal_is_deterministic_and_stays_unauthorized() -> None:
    first = _proposal()
    second = _proposal()
    assert first == second
    assert first.canonical_bytes == second.canonical_bytes
    assert first.proposed_owner_decision == PROPOSED_OWNER_DECISION
    assert first.authorization_status == "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
    assert first.owner_token_present is False
    assert first.gate_status == "UNKNOWN_EVIDENCE_INCOMPLETE"
    assert first.safety.cloud_backtest_authorized is False
    assert first.safety.project_mutation_authorized is False


def test_proposal_canonical_round_trip_and_format_tamper() -> None:
    proposal = _proposal()
    assert (
        QCQQQOptionsDailyCapabilityGateProposal.from_json_bytes(proposal.canonical_bytes)
        == proposal
    )
    compact = json.dumps(proposal.model_dump(mode="json"), sort_keys=True).encode("utf-8")
    with pytest.raises(ValueError, match="not canonical"):
        QCQQQOptionsDailyCapabilityGateProposal.from_json_bytes(compact)


@pytest.mark.parametrize(
    ("field", "replacement", "message"),
    [
        ("required_aggregate_fields", ("contract_count",), "aggregate field"),
        ("allowed_actions_after_exact_owner_token", ("QUANTCONNECT_LOGIN",), "allowed action"),
        ("prohibited_actions", ("API",), "prohibited action"),
    ],
)
def test_policy_inventory_drift_fails_closed(
    field: str, replacement: tuple[str, ...], message: str
) -> None:
    payload = load_qc_qqq_options_daily_capability_gate_policy().policy.model_dump(mode="python")
    payload[field] = replacement
    with pytest.raises(ValidationError, match=message):
        QCQQQOptionsDailyCapabilityGatePolicy.model_validate(payload)


def test_policy_primary_range_and_session_order_fail_closed() -> None:
    payload = load_qc_qqq_options_daily_capability_gate_policy().policy.model_dump(mode="python")
    payload["run_scope"]["requested_start"] = date(2021, 2, 23)
    with pytest.raises(ValidationError, match="primary-window start"):
        QCQQQOptionsDailyCapabilityGatePolicy.model_validate(payload)

    payload = load_qc_qqq_options_daily_capability_gate_policy().policy.model_dump(mode="python")
    payload["run_scope"]["expected_sessions"] = tuple(reversed(EXPECTED_SESSIONS))
    with pytest.raises(ValidationError, match="session inventory"):
        QCQQQOptionsDailyCapabilityGatePolicy.model_validate(payload)


def test_negative_session_count_fails_closed() -> None:
    with pytest.raises(ValidationError, match="cannot be negative"):
        _session(EXPECTED_SESSIONS[0], contract_count=-1)


def test_observation_reordered_or_out_of_range_sessions_fail_closed() -> None:
    with pytest.raises(ValidationError, match="unique and ordered"):
        _observation(
            session_observations=tuple(_session(item) for item in reversed(EXPECTED_SESSIONS))
        )
    with pytest.raises(ValidationError, match="escapes the frozen range"):
        _observation(session_observations=(_session(date(2021, 3, 1)),))


def test_complete_daily_observation_allows_engineering_only() -> None:
    proposal = _proposal()
    gate = _gate(_observation(proposal=proposal), proposal=proposal)
    assert gate.decision is DailyCapabilityGateDecision.GO_FOR_DAILY_ENGINEERING_ONLY
    assert gate.reason_codes == ("ALL_FROZEN_DAILY_CAPABILITY_CHECKS_PASS",)
    assert gate.engineering_successor_allowed is True
    assert gate.full_window_cloud_run_authorized is False
    assert gate.investment_interpretation_allowed is False


def test_missing_session_is_unknown_not_pass() -> None:
    gate = _gate(
        _observation(session_observations=tuple(_session(x) for x in EXPECTED_SESSIONS[:-1]))
    )
    assert gate.decision is DailyCapabilityGateDecision.UNKNOWN_EVIDENCE_INCOMPLETE
    assert "EXPECTED_SESSION_EVIDENCE_INCOMPLETE" in gate.reason_codes
    assert gate.engineering_successor_allowed is False


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"option_chain_present": False, "contract_count": 0}, "OPTION_CHAIN_OR_CONTRACT_MISSING"),
        ({"two_sided_quote_count": 0}, "TWO_SIDED_QUOTE_MISSING"),
        ({"positive_open_interest_count": 0}, "POSITIVE_OPEN_INTEREST_MISSING"),
        ({"finite_greeks_count": 0}, "GREEKS_OR_IV_MISSING"),
        ({"finite_implied_volatility_count": 0}, "GREEKS_OR_IV_MISSING"),
    ],
)
def test_required_daily_field_absence_is_no_go(
    overrides: dict[str, int | bool], reason: str
) -> None:
    sessions = tuple(
        _session(item, **(overrides if item == EXPECTED_SESSIONS[0] else {}))
        for item in EXPECTED_SESSIONS
    )
    gate = _gate(_observation(session_observations=sessions))
    assert gate.decision is DailyCapabilityGateDecision.NO_GO_CAPABILITY_OR_ENTITLEMENT
    assert reason in gate.reason_codes
    assert gate.engineering_successor_allowed is False


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"project_id": 1}, "TARGET_PROJECT_MISMATCH"),
        ({"evaluated_end": date(2021, 2, 25)}, "REQUESTED_OR_EVALUATED_RANGE_MISMATCH"),
        ({"daily_entitlement_observed": False}, "DAILY_ENTITLEMENT_NOT_OBSERVED"),
        ({"scope_violation_detected": True}, "SCOPE_VIOLATION"),
    ],
)
def test_identity_range_entitlement_and_scope_violation_are_no_go(
    kwargs: dict[str, object], reason: str
) -> None:
    gate = _gate(_observation(**kwargs))
    assert gate.decision is DailyCapabilityGateDecision.NO_GO_CAPABILITY_OR_ENTITLEMENT
    assert reason in gate.reason_codes


@pytest.mark.parametrize(
    "kwargs",
    [
        {"free_organization_reviewed": False},
        {"exceptions": ("RESULT_SCREENSHOT_PENDING",)},
    ],
)
def test_missing_review_or_exception_is_unknown(kwargs: dict[str, object]) -> None:
    gate = _gate(_observation(**kwargs))
    assert gate.decision is DailyCapabilityGateDecision.UNKNOWN_EVIDENCE_INCOMPLETE
    assert gate.engineering_successor_allowed is False


def test_observation_and_gate_canonical_replay() -> None:
    proposal = _proposal()
    observation = _observation(proposal=proposal)
    gate = _gate(observation, proposal=proposal)
    assert (
        QCQQQOptionsDailyCapabilityRunObservation.from_json_bytes(observation.canonical_bytes)
        == observation
    )
    assert QCQQQOptionsDailyCapabilityGateRecord.from_json_bytes(gate.canonical_bytes) == gate


def test_gate_cannot_forge_successor_permission() -> None:
    gate = _gate(
        _observation(session_observations=tuple(_session(x) for x in EXPECTED_SESSIONS[:-1]))
    )
    payload = gate.model_dump(mode="python")
    payload["engineering_successor_allowed"] = True
    with pytest.raises(ValidationError, match="does not match"):
        QCQQQOptionsDailyCapabilityGateRecord.model_validate(payload)


def test_orders_fills_and_raw_rows_cannot_be_promoted() -> None:
    payload = _observation().model_dump(mode="python")
    payload["orders_submitted"] = 1
    with pytest.raises(ValidationError):
        QCQQQOptionsDailyCapabilityRunObservation.model_validate(payload)
    payload = _observation().model_dump(mode="python")
    payload["raw_rows_logged_or_exported"] = True
    with pytest.raises(ValidationError):
        QCQQQOptionsDailyCapabilityRunObservation.model_validate(payload)


def test_policy_path_is_repository_relative_and_exists() -> None:
    assert not DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_POLICY_PATH.is_absolute()
    assert (PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_POLICY_PATH).is_file()
