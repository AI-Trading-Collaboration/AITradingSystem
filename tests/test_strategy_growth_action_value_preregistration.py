from __future__ import annotations

import hashlib
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.strategy_growth_action_value_preregistration import (
    DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_PREREGISTRATION_POLICY_PATH,
    DataEvidenceLane,
    MandatoryAxis,
    MandatoryAxisOutcome,
    PreregistrationActionRequest,
    StrategyGrowthActionValuePreregistrationDecision,
    StrategyGrowthActionValuePreregistrationError,
    TerminalOutcome,
    aggregate_mandatory_axis_outcomes,
    build_strategy_growth_action_value_preregistration_decision,
    load_strategy_growth_action_value_preregistration_policy,
)

POLICY_FILE_SHA256 = "c7246611a340bbc6e948127b4a79dc9c4f46f1cdc9c0844046042990fb2647b4"
POLICY_CANONICAL_SHA256 = "73860ec7a9c4280ee07a177a09df7605d4abd4fdf798c59ad92273f494badf22"
AUTHORITY_SET_SHA256 = "d32b52257221ec980d4976f7983c38231b99f65d938a2afe2e8a26481e208cc8"
DECISION_CANONICAL_SHA256 = "04bbd2331d04ac40c54e99db62b79f33b8925022157c332a4bb40c52b3474fe8"

AUTHORITY_PATHS = (
    "AGENTS.md",
    "config/research/strategy_research_reopen_readiness_decision_v1.yaml",
    "docs/requirements/TRADING-2515_Strategy_Research_Reopen_Readiness_Decision_V1.md",
    "docs/requirements/TRADING-2516_QC_QQQ_Options_Primary_Window_Evidence_Lane_Authorization_Refresh_V1.md",
    "docs/requirements/TRADING-2541_QC_QQQ_Options_Exact_Date_Subscription_Missing_Remediation_V1.md",
    "config/research/simple_baseline_strategy_registry.yaml",
    "config/research/two_layer_strategy_boundary_contract.yaml",
    "docs/research/first_layer_channel_master_closeout.md",
    "docs/research/defensive_preservation_lane_closeout.md",
    "docs/research/two_lane_optimization_master_closeout.md",
    "config/research/strategy_style_discovery_universe_v1.yaml",
    "docs/research/trading2458_candidate_family_retirement.md",
)
AXIS_ORDER = tuple(MandatoryAxis)


def _copy_policy_authorities(tmp_path: Path) -> Path:
    relative_paths = (
        DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_PREREGISTRATION_POLICY_PATH.as_posix(),
        *AUTHORITY_PATHS,
    )
    for relative in relative_paths:
        source = PROJECT_ROOT / relative
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    return tmp_path


def _replace(path: Path, old: str, new: str) -> None:
    content = path.read_text(encoding="utf-8")
    assert old in content
    path.write_text(content.replace(old, new, 1), encoding="utf-8", newline="\n")


def _rewrite_binding_hash(policy_path: Path, old_hash: str, authority_path: Path) -> None:
    new_hash = hashlib.sha256(authority_path.read_bytes()).hexdigest()
    _replace(policy_path, old_hash, new_hash)


def _build() -> StrategyGrowthActionValuePreregistrationDecision:
    return build_strategy_growth_action_value_preregistration_decision(
        decision_id="strategy-growth-preregistration-20260822",
        evaluated_at_utc=datetime(2026, 8, 22, 3, 30, tzinfo=UTC),
    )


def _all_outcomes(
    outcome: TerminalOutcome = TerminalOutcome.PASS,
) -> tuple[MandatoryAxisOutcome, ...]:
    return tuple(
        MandatoryAxisOutcome(axis_id=axis, outcome=outcome, reason_codes=())
        for axis in AXIS_ORDER
    )


def test_policy_loads_exact_authority_identity_and_semantics() -> None:
    loaded = load_strategy_growth_action_value_preregistration_policy()

    assert loaded.policy_file_sha256 == POLICY_FILE_SHA256
    assert loaded.policy_canonical_sha256 == POLICY_CANONICAL_SHA256
    assert loaded.authority_set_sha256 == AUTHORITY_SET_SHA256
    assert len(loaded.authority_observations) == 12
    assert all(item.identity_verified for item in loaded.authority_observations)
    assert all(item.semantics_verified for item in loaded.authority_observations)
    assert sum(item.semantic_fact_count for item in loaded.authority_observations) == 21
    assert sum(item.required_snippet_count for item in loaded.authority_observations) == 28


def test_policy_freezes_growth_scope_and_selected_not_executable_lane() -> None:
    policy = load_strategy_growth_action_value_preregistration_policy().policy

    assert policy.primary_research_start.isoformat() == "2021-02-22"
    assert policy.prohibited_default_start.isoformat() == "2022-12-01"
    assert policy.preregistration_stage == "PREREGISTRATION_ONLY"
    assert policy.hypothesis.baseline_id == "equal_risk_qqq_sgov"
    assert policy.hypothesis.action_universe == ("QQQ", "SGOV")
    assert policy.hypothesis.uses_leverage_etf is False
    assert policy.hypothesis.uses_options is False
    assert policy.hypothesis.defense_is_independent_hard_gate is True
    assert policy.owner_decision.endswith(
        "retain_qqq_options_lane_and_remove_qld_selected_lane_semantics_v1"
    )
    assert (
        policy.data_lane.selected_data_lane
        == "QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE"
    )
    assert policy.data_lane.selection_status == "OWNER_RETAINED_NOT_EXECUTABLE"
    assert policy.data_lane.qqq_options_lane_selected is True
    assert (
        policy.data_lane.transport_completeness_status
        == "RECOVERED_COMPLETE_NOT_DQ_PIT_PROMOTED"
    )
    assert policy.data_lane.expected_session_count == 1202
    assert policy.data_lane.observed_session_count == 1202
    assert policy.data_lane.exact_date_recovery_session_count == 1
    assert policy.data_lane.unresolved_session_count == 0
    assert policy.data_lane.dq_pit_promoted is False
    assert policy.data_lane.data_lane_execution_authorized is False
    assert policy.data_lane.cache_mutation_authorized is False
    assert policy.threshold_policy.status == "NOT_PROVIDED"
    assert policy.threshold_policy.reviewed_policy_refs == ()
    assert tuple(item.axis_id for item in policy.mandatory_axes) == AXIS_ORDER


def test_default_decision_is_blocked_policy_input_and_keeps_actions_closed() -> None:
    decision = _build()

    assert decision.preregistration_status == "BLOCKED_POLICY_INPUT"
    assert decision.downstream_gate == "OWNER_REVIEWED_THRESHOLD_POLICY_REQUIRED"
    assert (
        decision.data_lane.selected_data_lane
        == "QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE"
    )
    assert decision.data_lane.data_lane_execution_authorized is False
    assert decision.safety.empirical_research_authorized is False
    assert decision.safety.candidate_search_authorized is False
    assert decision.safety.parameter_search_authorized is False
    assert decision.safety.backtest_authorized is False
    assert decision.safety.holdout_access_authorized is False
    assert decision.safety.cache_mutation_authorized is False
    assert decision.safety.external_action_authorized is False
    assert decision.safety.paper_allowed is False
    assert decision.safety.live_allowed is False
    assert decision.safety.broker_allowed is False
    assert decision.safety.production_effect == "none"
    assert decision.safety.broker_action == "none"


def test_decision_is_canonical_sealed_and_replayable() -> None:
    decision = _build()

    replay = StrategyGrowthActionValuePreregistrationDecision.from_json_bytes(
        decision.canonical_bytes
    )

    assert replay == decision
    assert replay.canonical_bytes == decision.canonical_bytes
    assert replay.content_sha256 == decision.compute_content_sha256()
    assert decision.canonical_sha256 == DECISION_CANONICAL_SHA256


@pytest.mark.parametrize(
    ("worse_outcome", "expected"),
    [
        (TerminalOutcome.PASS, TerminalOutcome.PASS),
        (TerminalOutcome.INSUFFICIENT, TerminalOutcome.INSUFFICIENT),
        (TerminalOutcome.FAIL, TerminalOutcome.FAIL),
        (TerminalOutcome.INVALID, TerminalOutcome.INVALID),
    ],
)
def test_terminal_outcome_priority_is_mechanical(
    worse_outcome: TerminalOutcome, expected: TerminalOutcome
) -> None:
    outcomes = list(_all_outcomes())
    outcomes[3] = MandatoryAxisOutcome(
        axis_id=outcomes[3].axis_id,
        outcome=worse_outcome,
        reason_codes=("FROZEN_RULE_RESULT",),
    )

    assert aggregate_mandatory_axis_outcomes(tuple(outcomes)) is expected


def test_invalid_cannot_be_offset_by_other_passes() -> None:
    outcomes = list(_all_outcomes())
    outcomes[0] = MandatoryAxisOutcome(
        axis_id=MandatoryAxis.NON_BETA_ACTION_VALUE,
        outcome=TerminalOutcome.FAIL,
        reason_codes=("NON_BETA_GATE_FAIL",),
    )
    outcomes[-1] = MandatoryAxisOutcome(
        axis_id=MandatoryAxis.LEVERAGE_BETA_ATTRIBUTION,
        outcome=TerminalOutcome.INVALID,
        reason_codes=("HIDDEN_LEVERAGE",),
    )

    assert aggregate_mandatory_axis_outcomes(tuple(outcomes)) is TerminalOutcome.INVALID


def test_missing_or_duplicate_mandatory_axis_is_rejected() -> None:
    outcomes = _all_outcomes()

    with pytest.raises(
        StrategyGrowthActionValuePreregistrationError,
        match="MANDATORY_AXIS_OUTCOME_SET_INVALID",
    ):
        aggregate_mandatory_axis_outcomes(outcomes[:-1])
    duplicated = (*outcomes[:-1], outcomes[0])
    with pytest.raises(
        StrategyGrowthActionValuePreregistrationError,
        match="MANDATORY_AXIS_OUTCOME_SET_INVALID",
    ):
        aggregate_mandatory_axis_outcomes(duplicated)


def test_exact_owner_retained_qqq_options_lane_can_be_redeclared_without_execution() -> None:
    decision = build_strategy_growth_action_value_preregistration_decision(
        decision_id="exact-lane",
        evaluated_at_utc=datetime(2026, 8, 22, tzinfo=UTC),
        selected_data_lanes=(
            DataEvidenceLane.QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE,
        ),
    )

    assert decision.data_lane.selection_status == "OWNER_RETAINED_NOT_EXECUTABLE"
    assert decision.data_lane.data_lane_execution_authorized is False


@pytest.mark.parametrize(
    ("lanes", "reason"),
    [
        ((), "DATA_EVIDENCE_LANE_SELECTION_REQUIRED"),
        (
            (DataEvidenceLane.QLD_CANONICAL_FULL_CACHE_DQ,),
            "DATA_EVIDENCE_LANE_SELECTION_MISMATCH",
        ),
        (
            (
                DataEvidenceLane.QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE,
                DataEvidenceLane.QLD_CANONICAL_FULL_CACHE_DQ,
            ),
            "MULTIPLE_DATA_EVIDENCE_LANES_PROHIBITED",
        ),
    ],
)
def test_missing_mismatched_or_parallel_data_lane_fails_closed(
    lanes: tuple[DataEvidenceLane, ...], reason: str
) -> None:
    with pytest.raises(StrategyGrowthActionValuePreregistrationError, match=reason):
        build_strategy_growth_action_value_preregistration_decision(
            decision_id="invalid-lane",
            evaluated_at_utc=datetime(2026, 8, 22, tzinfo=UTC),
            selected_data_lanes=lanes,
        )


def test_caller_cannot_invent_reviewed_threshold_policy_ref() -> None:
    with pytest.raises(
        StrategyGrowthActionValuePreregistrationError,
        match="THRESHOLD_POLICY_DECLARATION_MISMATCH",
    ):
        build_strategy_growth_action_value_preregistration_decision(
            decision_id="invented-threshold",
            evaluated_at_utc=datetime(2026, 8, 22, tzinfo=UTC),
            reviewed_threshold_policy_refs=("config/research/unreviewed_thresholds.yaml",),
        )


@pytest.mark.parametrize(
    ("action_field", "reason"),
    [
        ("empirical_research", "EMPIRICAL_DATA_OR_EXTERNAL_ACTION_NOT_AUTHORIZED"),
        ("candidate_search", "EMPIRICAL_DATA_OR_EXTERNAL_ACTION_NOT_AUTHORIZED"),
        ("parameter_search", "EMPIRICAL_DATA_OR_EXTERNAL_ACTION_NOT_AUTHORIZED"),
        ("backtest", "EMPIRICAL_DATA_OR_EXTERNAL_ACTION_NOT_AUTHORIZED"),
        ("holdout_access", "EMPIRICAL_DATA_OR_EXTERNAL_ACTION_NOT_AUTHORIZED"),
        ("qld_data_lane_execution", "UNSELECTED_DATA_EVIDENCE_LANE_PROHIBITED"),
        ("qqq_options_lane_execution", "EMPIRICAL_DATA_OR_EXTERNAL_ACTION_NOT_AUTHORIZED"),
        ("cache_mutation", "EMPIRICAL_DATA_OR_EXTERNAL_ACTION_NOT_AUTHORIZED"),
        ("external_action", "EMPIRICAL_DATA_OR_EXTERNAL_ACTION_NOT_AUTHORIZED"),
        ("investment_conclusion", "EMPIRICAL_DATA_OR_EXTERNAL_ACTION_NOT_AUTHORIZED"),
        ("paper", "EMPIRICAL_DATA_OR_EXTERNAL_ACTION_NOT_AUTHORIZED"),
        ("live", "EMPIRICAL_DATA_OR_EXTERNAL_ACTION_NOT_AUTHORIZED"),
        ("broker", "EMPIRICAL_DATA_OR_EXTERNAL_ACTION_NOT_AUTHORIZED"),
        ("production", "EMPIRICAL_DATA_OR_EXTERNAL_ACTION_NOT_AUTHORIZED"),
        ("use_leverage_etf", "HIDDEN_OR_EXPLICIT_LEVERAGE_PROHIBITED"),
        ("use_options", "HIDDEN_OR_EXPLICIT_LEVERAGE_PROHIBITED"),
        ("use_retired_family", "RETIRED_FAMILY_REUSE_PROHIBITED"),
        ("threshold_after_result", "THRESHOLD_AFTER_RESULT_PROHIBITED"),
    ],
)
def test_unauthorized_empirical_data_leverage_and_contamination_actions_fail_closed(
    action_field: str, reason: str
) -> None:
    request = PreregistrationActionRequest.model_validate({action_field: True})

    with pytest.raises(StrategyGrowthActionValuePreregistrationError, match=reason):
        build_strategy_growth_action_value_preregistration_decision(
            decision_id=f"unauthorized-{action_field}",
            evaluated_at_utc=datetime(2026, 8, 22, tzinfo=UTC),
            action_request=request,
        )


def test_authority_file_hash_drift_fails_closed(tmp_path: Path) -> None:
    root = _copy_policy_authorities(tmp_path)
    authority_path = root / AUTHORITY_PATHS[-1]
    authority_path.write_bytes(authority_path.read_bytes() + b"\n")

    with pytest.raises(
        StrategyGrowthActionValuePreregistrationError,
        match="authority file SHA-256 mismatch",
    ):
        load_strategy_growth_action_value_preregistration_policy(project_root=root)


def test_text_authority_semantic_drift_fails_even_when_hash_is_updated(
    tmp_path: Path,
) -> None:
    root = _copy_policy_authorities(tmp_path)
    policy_path = root / DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_PREREGISTRATION_POLICY_PATH
    authority_path = root / AUTHORITY_PATHS[-1]
    old_hash = hashlib.sha256(authority_path.read_bytes()).hexdigest()
    _replace(authority_path, "状态：`RETIRED`", "状态：`ACTIVE`")
    _rewrite_binding_hash(policy_path, old_hash, authority_path)

    with pytest.raises(
        StrategyGrowthActionValuePreregistrationError,
        match="authority required text is missing",
    ):
        load_strategy_growth_action_value_preregistration_policy(project_root=root)


def test_qqq_options_lane_authority_semantic_drift_fails_even_when_hash_is_updated(
    tmp_path: Path,
) -> None:
    root = _copy_policy_authorities(tmp_path)
    policy_path = root / DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_PREREGISTRATION_POLICY_PATH
    authority_path = (
        root
        / "docs/requirements/"
        "TRADING-2516_QC_QQQ_Options_Primary_Window_Evidence_Lane_Authorization_Refresh_V1.md"
    )
    old_hash = hashlib.sha256(authority_path.read_bytes()).hexdigest()
    _replace(
        authority_path,
        "`QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE`",
        "`QLD_CANONICAL_FULL_CACHE_DQ`",
    )
    _rewrite_binding_hash(policy_path, old_hash, authority_path)

    with pytest.raises(
        StrategyGrowthActionValuePreregistrationError,
        match="authority required text is missing",
    ):
        load_strategy_growth_action_value_preregistration_policy(project_root=root)


def test_exact_date_recovery_authority_cannot_forge_dq_pit_promotion(
    tmp_path: Path,
) -> None:
    root = _copy_policy_authorities(tmp_path)
    policy_path = root / DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_PREREGISTRATION_POLICY_PATH
    authority_path = (
        root
        / "docs/requirements/"
        "TRADING-2541_QC_QQQ_Options_Exact_Date_Subscription_Missing_Remediation_V1.md"
    )
    old_hash = hashlib.sha256(authority_path.read_bytes()).hexdigest()
    _replace(authority_path, "`dq_pit_promoted=false`", "`dq_pit_promoted=true`")
    _rewrite_binding_hash(policy_path, old_hash, authority_path)

    with pytest.raises(
        StrategyGrowthActionValuePreregistrationError,
        match="authority required text is missing",
    ):
        load_strategy_growth_action_value_preregistration_policy(project_root=root)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        (
            "primary_research_start: '2021-02-22'",
            "primary_research_start: '2022-12-01'",
            "PRIMARY research start",
        ),
        (
            "prohibited_default_start: '2022-12-01'",
            "prohibited_default_start: '2021-02-22'",
            "historical 2022-12-01 boundary",
        ),
        (
            "selected_data_lane: QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE",
            "selected_data_lane: QLD_CANONICAL_FULL_CACHE_DQ",
            "selected_data_lane",
        ),
        (
            "uses_leverage_etf: false",
            "uses_leverage_etf: true",
            "uses_leverage_etf",
        ),
    ],
)
def test_policy_cannot_drift_window_lane_or_leverage_boundary(
    tmp_path: Path, old: str, new: str, message: str
) -> None:
    root = _copy_policy_authorities(tmp_path)
    policy_path = root / DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_PREREGISTRATION_POLICY_PATH
    _replace(policy_path, old, new)

    with pytest.raises(StrategyGrowthActionValuePreregistrationError, match=message):
        load_strategy_growth_action_value_preregistration_policy(project_root=root)


def test_duplicate_policy_key_is_rejected(tmp_path: Path) -> None:
    root = _copy_policy_authorities(tmp_path)
    policy_path = root / DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_PREREGISTRATION_POLICY_PATH
    _replace(
        policy_path,
        "policy_version: 1.0.0",
        "policy_version: 1.0.0\npolicy_version: 1.0.0",
    )

    with pytest.raises(StrategyGrowthActionValuePreregistrationError, match="DUPLICATE_KEY"):
        load_strategy_growth_action_value_preregistration_policy(project_root=root)


def test_decision_tamper_cannot_forge_frozen_or_pass_state() -> None:
    decision = _build()
    payload = json.loads(decision.canonical_bytes)
    payload["preregistration_status"] = "PREREGISTRATION_FROZEN_AWAITING_DQ"
    tampered = (json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()

    with pytest.raises(
        StrategyGrowthActionValuePreregistrationError,
        match="GROWTH_PREREGISTRATION_DECISION_RECORD_INVALID",
    ):
        StrategyGrowthActionValuePreregistrationDecision.from_json_bytes(tampered)


def test_noncanonical_and_duplicate_decision_json_are_rejected() -> None:
    decision = _build()
    noncanonical = json.dumps(json.loads(decision.canonical_bytes)).encode()
    duplicate = decision.canonical_bytes.replace(
        b'{\n  "authority_observations"',
        b'{\n  "authority_set_sha256": "'
        + decision.authority_set_sha256.encode()
        + b'",\n  "authority_observations"',
        1,
    )

    with pytest.raises(
        StrategyGrowthActionValuePreregistrationError, match="not canonical JSON bytes"
    ):
        StrategyGrowthActionValuePreregistrationDecision.from_json_bytes(noncanonical)
    with pytest.raises(
        StrategyGrowthActionValuePreregistrationError, match="duplicate JSON key"
    ):
        StrategyGrowthActionValuePreregistrationDecision.from_json_bytes(duplicate)
