from __future__ import annotations

import json
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_owner_freeze_decision_pack_draft_v2 as decision_pack,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_PATH = decision_pack.DEFAULT_OWNER_FREEZE_DECISION_PACK_DRAFT_V2_PATH
SEMANTICS_PATH = decision_pack.DEFAULT_CALCULATION_SEMANTICS_PATH
DecisionPack = decision_pack.MandatoryVetoOwnerFreezeDecisionPackDraftV2
CalculationSemantics = decision_pack.MandatoryVetoCalculationSemantics
DecisionPackError = decision_pack.MandatoryVetoExactSemanticsDraftError
load_decision_pack = decision_pack.load_mandatory_veto_owner_freeze_decision_pack_draft_v2
load_semantics = decision_pack.load_mandatory_veto_calculation_semantics

EXPECTED_FILE_SHA256 = "d08480c07047e636f8b4a8208ec60406acd5debdc60f30541411310e401b789f"
EXPECTED_CANONICAL_SHA256 = (
    "99ed7dbdac82faf594633ab25be1ffb1417709030af0817fb19c4ace332dc389"
)
EXPECTED_SEMANTICS_FILE_SHA256 = (
    "813c2eb2bb0d4b4f7673048889b66fa843b739a48405cc2e87272d925dd7b0d0"
)
EXPECTED_SEMANTICS_CANONICAL_SHA256 = (
    "824ef20a66e4eba3c2841489cae8b03ff3a6cad4f73003469c086d8e09237cf1"
)
EXPECTED_PREDECESSOR_FILE_SHA256 = (
    "4f188c6e10758a32984bb92c3252507636686f97404c4491df014c1d22807479"
)
EXPECTED_PREDECESSOR_CANONICAL_SHA256 = (
    "c8838a4baef788a6b936e4e098658413e2c563e169f1ec4a5da8ec7318c9e4af"
)
EXPECTED_VETO_IDS = (
    "broad_market_risk_off_veto",
    "realized_volatility_veto",
    "scheduled_event_risk_veto",
    "underlying_trend_break_veto",
)


def _payload(path: Path = DEFAULT_PATH) -> dict[str, Any]:
    absolute = PROJECT_ROOT / path
    loaded = load_strict_yaml_text(
        absolute.read_text(encoding="utf-8"), label=str(absolute)
    )
    assert isinstance(loaded, dict)
    return loaded


def test_loader_replays_predecessor_semantics_and_exact_v2_identity() -> None:
    result = load_decision_pack()

    assert result.terminal == "OWNER_EXACT_FREEZE_REQUIRED_0_OF_4_ADMITTED"
    assert result.file_sha256 == EXPECTED_FILE_SHA256
    assert result.canonical_sha256 == EXPECTED_CANONICAL_SHA256
    assert result.predecessor.file_sha256 == EXPECTED_PREDECESSOR_FILE_SHA256
    assert result.predecessor.canonical_sha256 == EXPECTED_PREDECESSOR_CANONICAL_SHA256
    assert result.calculation_semantics.file_sha256 == EXPECTED_SEMANTICS_FILE_SHA256
    assert (
        result.calculation_semantics.canonical_sha256
        == EXPECTED_SEMANTICS_CANONICAL_SHA256
    )
    assert result.policy.target_inventory.expected_session_count == 1202


def test_shared_semantics_fail_closed_before_formula_evaluation() -> None:
    semantics = load_semantics().policy

    assert semantics.calendar_contract.target_calendar_identity == "QQQ_EXCHANGE_SESSIONS"
    assert semantics.calendar_contract.source_warmup_inventory_required is True
    assert semantics.calendar_contract.target_inventory_excludes_warmup is True
    assert semantics.calendar_contract.session_gaps_may_be_compressed is False
    assert semantics.clock_contract.availability_predicate.endswith(
        "AVAILABLE_AT_LTE_DECISION_AS_OF"
    )
    assert semantics.price_contract.sma_method == "ARITHMETIC_MEAN"
    assert semantics.price_contract.rolling_window_includes_current_session is True
    assert semantics.terminal_policy.missing_terminal == "INSUFFICIENT"
    assert semantics.terminal_policy.malformed_authority_terminal == "INVALID"
    assert (
        semantics.terminal_policy.formula_short_circuit_before_all_components_qualified
        is False
    )
    assert semantics.terminal_policy.event_empty_rows_may_prove_false is False
    assert semantics.state_contract.initial_state == "UNKNOWN"
    assert semantics.overlap_contract.orthogonality_claim == (
        "SEMANTIC_AND_INPUT_SEPARATION_ONLY_NOT_EMPIRICAL_INDEPENDENCE"
    )


def test_broad_market_v2_is_typed_spy_only_or_with_full_windows() -> None:
    broad = load_decision_pack().policy.decision_rows[0]

    assert broad.veto_id == "broad_market_risk_off_veto"
    assert all(
        field.startswith("SPY.")
        for field in broad.producer_contract.independent_input_universe
    )
    assert broad.formula_contract.combination_rule == "OR"
    assert tuple(item.operator for item in broad.formula_contract.comparisons) == (
        "LT",
        "LTE",
    )
    assert broad.formula_contract.comparisons[1].right_numeric == -0.10
    assert broad.rolling_contract.moving_average_sessions == 200
    assert broad.rolling_contract.sma_min_valid_observations == 200
    assert broad.rolling_contract.drawdown_sessions == 63
    assert broad.rolling_contract.drawdown_min_valid_observations == 63
    assert broad.rolling_contract.window_includes_current_session is True
    assert broad.entry_recovery_contract.extra_hysteresis_allowed is False


def test_volatility_v2_rejects_candidate_min_periods_and_freezes_rv_math() -> None:
    volatility = load_decision_pack().policy.decision_rows[1]
    rolling = volatility.rolling_contract

    assert volatility.formula_contract.combination_rule == "OR"
    assert rolling.vix_percentile_sessions == rolling.vix_min_valid_observations == 252
    assert rolling.vix_window_includes_current_observation is True
    assert rolling.vix_tie_method == "AVERAGE_RANK"
    assert rolling.qqq_close_observations == 21
    assert rolling.realized_volatility_return_observations == 20
    assert rolling.return_type == "SIMPLE"
    assert rolling.pct_change_fill_method == "NONE"
    assert rolling.standard_deviation_ddof == 1
    assert rolling.annualization_sessions == 252
    assert rolling.annualization_scaling == "SQRT"
    assert rolling.session_gap_compression_allowed is False
    assert volatility.component_semantics.vix_component_role == (
        "IMPLIED_VOLATILITY_STRESS_PROXY"
    )
    assert (
        volatility.component_semantics.whole_veto_may_be_described_as_pure_realized_volatility
        is False
    )


def test_event_v2_requires_revision_chain_and_three_authority_coverage() -> None:
    event = load_decision_pack().policy.decision_rows[2]

    assert event.formula_contract.combination_rule == "ANY_EVENT"
    assert event.event_contract.mapping_interval == (
        "DECISION_AS_OF_LT_SCHEDULED_FOR_LTE_NEXT_ACTION_SESSION_CLOSE"
    )
    assert event.event_contract.pre_event_qqq_sessions == 1
    assert event.event_contract.post_event_qqq_sessions == 0
    assert event.event_contract.unscheduled_interventions_in_scope is False
    assert event.revision_contract.reschedule_supersedes_prior_revision is True
    assert event.revision_contract.cancel_revision_supported is True
    assert event.revision_contract.post_decision_revision_may_rewrite_history is False
    assert event.coverage_contract.required_authorities == (
        "FEDERAL_RESERVE",
        "BLS",
        "BEA",
    )
    assert event.coverage_contract.coverage_receipt_required_to_emit_false is True
    assert event.coverage_contract.empty_rows_may_prove_false is False
    assert event.pit_contract.published_at_required is True
    assert event.pit_contract.revision_identity_required is True


def test_trend_v2_requires_pre_target_replay_and_unknown_on_missing() -> None:
    trend = load_decision_pack().policy.decision_rows[3]
    state = trend.state_contract

    assert all(
        field.startswith("QQQ.")
        for field in trend.producer_contract.independent_input_universe
    )
    assert trend.formula_contract.combination_rule == "AND"
    assert tuple(item.operator for item in trend.formula_contract.comparisons) == (
        "LT",
        "LTE",
    )
    assert state.allowed_states == ("UNKNOWN", "CLEAR", "VETO_ACTIVE")
    assert state.initial_state == "UNKNOWN"
    assert state.pre_target_replay_required is True
    assert state.known_state_required_before_target_inventory is True
    assert state.recovery_confirmation_sessions == 2
    assert state.recovery_equality == "GTE"
    assert state.missing_observation_interrupts_recovery is True
    assert state.missing_observation_next_state == "UNKNOWN"
    assert state.replay_from_affected_checkpoint_required is True


def test_v2_remains_zero_admission_and_non_executable() -> None:
    policy = load_decision_pack().policy

    assert tuple(row.veto_id for row in policy.decision_rows) == EXPECTED_VETO_IDS
    assert policy.aggregate_state.exact_semantics_objects_ready_for_owner_review == (
        EXPECTED_VETO_IDS
    )
    assert policy.aggregate_state.owner_frozen_producer_contracts == ()
    assert policy.aggregate_state.admitted_producer_contracts == ()
    assert policy.aggregate_state.unresolved_producer_contracts == EXPECTED_VETO_IDS
    for row in policy.decision_rows:
        assert row.owner_exact_freeze_granted is False
        assert row.producer_contract_admitted is False
        assert row.exact_1202_session_inventory_admitted is False
        assert row.observed_inventory_lf_sha256 is None
        assert row.series_generation_allowed is False
    assert policy.safety.producer_implementation_allowed is False
    assert policy.safety.source_contract_admission_allowed is False
    assert policy.safety.veto_series_generation_allowed is False
    assert policy.safety.r1_manifest_generation_allowed is False
    assert policy.safety.real_dq_authorized is False
    assert policy.safety.backtest_authorized is False
    assert policy.safety.orders_allowed is policy.safety.fills_allowed is False
    assert policy.safety.positions_allowed is False
    assert policy.safety.production_effect == policy.safety.broker_action == "none"


def test_canonical_round_trip_preserves_typed_v2_and_zero_admission() -> None:
    result = load_decision_pack()
    replay = DecisionPack.model_validate(json.loads(result.policy.canonical_bytes))

    assert replay.canonical_bytes == result.policy.canonical_bytes
    assert replay.canonical_sha256 == EXPECTED_CANONICAL_SHA256
    text = result.policy.canonical_bytes.decode("utf-8")
    assert '"formula_short_circuit_before_all_components_qualified": false' not in text
    assert '"owner_exact_freeze_granted": false' in text
    assert '"observed_inventory_lf_sha256": null' in text
    assert '"veto_series_sha256"' not in text
    assert '"backtest_id"' not in text


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload["decision_rows"][0]["formula_contract"].__setitem__(
                "combination_rule", "AND"
            ),
            "broad-market combination drifted",
        ),
        (
            lambda payload: payload["decision_rows"][0]["formula_contract"][
                "comparisons"
            ][0].__setitem__("operator", "LTE"),
            "formula operator tree drifted",
        ),
        (
            lambda payload: payload["decision_rows"][0]["rolling_contract"].__setitem__(
                "sma_min_valid_observations", 199
            ),
            "Input should be 200",
        ),
        (
            lambda payload: payload["decision_rows"][1]["rolling_contract"].__setitem__(
                "vix_min_valid_observations", 20
            ),
            "Input should be 252",
        ),
        (
            lambda payload: payload["decision_rows"][1]["rolling_contract"].__setitem__(
                "vix_tie_method", "DENSE_RANK"
            ),
            "AVERAGE_RANK",
        ),
        (
            lambda payload: payload["decision_rows"][2]["coverage_contract"].__setitem__(
                "empty_rows_may_prove_false", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["decision_rows"][2]["coverage_contract"][
                "required_authorities"
            ].pop(),
            "event coverage authority inventory drifted",
        ),
        (
            lambda payload: payload["decision_rows"][3]["state_contract"].__setitem__(
                "initial_state", "CLEAR"
            ),
            "UNKNOWN",
        ),
        (
            lambda payload: payload["decision_rows"][3]["state_contract"].__setitem__(
                "missing_observation_next_state", "CLEAR"
            ),
            "UNKNOWN",
        ),
        (
            lambda payload: payload["decision_rows"][0].__setitem__(
                "owner_exact_freeze_granted", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["decision_rows"][1].__setitem__(
                "observed_inventory_lf_sha256", "0" * 64
            ),
            "Input should be None",
        ),
        (
            lambda payload: payload["decision_rows"][0]["producer_contract"][
                "independent_input_universe"
            ].append("QQQ.adjusted_close"),
            "input universe drifted",
        ),
    ],
)
def test_v2_schema_rejects_formula_window_pit_state_or_admission_drift(
    mutate: Callable[[dict[str, Any]], None], match: str
) -> None:
    payload = deepcopy(_payload())
    mutate(payload)

    with pytest.raises(ValidationError, match=match):
        DecisionPack.model_validate(payload)


@pytest.mark.parametrize(
    ("path", "value", "match"),
    [
        (
            ("terminal_policy", "missing_may_be_interpreted_as_false"),
            True,
            "Input should be False",
        ),
        (
            ("terminal_policy", "formula_short_circuit_before_all_components_qualified"),
            True,
            "Input should be False",
        ),
        (
            ("calendar_contract", "same_session_action_allowed"),
            True,
            "Input should be False",
        ),
    ],
)
def test_shared_semantics_reject_missing_short_circuit_or_same_session(
    path: tuple[str, str], value: bool, match: str
) -> None:
    payload = deepcopy(_payload(SEMANTICS_PATH))
    payload[path[0]][path[1]] = value

    with pytest.raises(ValidationError, match=match):
        CalculationSemantics.model_validate(payload)


def test_v2_loader_rejects_parent_traversal() -> None:
    path = Path(
        "../AITradingSystem/config/research/"
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "owner_freeze_decision_pack_draft_v2.yaml"
    )

    with pytest.raises(DecisionPackError, match="escapes project root"):
        load_decision_pack(path=path, project_root=PROJECT_ROOT)
