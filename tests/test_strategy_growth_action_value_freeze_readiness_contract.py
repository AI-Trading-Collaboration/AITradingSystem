from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from ai_trading_system.strategy_growth_action_value_freeze_readiness_contract import (
    EpisodeInterval,
    StrategyGrowthActionValueFreezeReadinessContract,
    activation_anchors_without_left_boundary,
    active_episode_intervals,
    episode_return_value,
    load_strategy_growth_action_value_freeze_readiness_contract,
    maximum_cost_reconciliation_residual,
    merge_anchor_indices_transitively,
    missed_return_for_qqq_weight,
)

EXPECTED_FILE_SHA256 = "f563f6499c86853c791589e40cf9d1dbac04b53b0728310e3b0e08376653a3d9"
EXPECTED_CANONICAL_SHA256 = "7b7a0d19d04f52de2de4dc813cc29de4dc62e0a624e93cc486256b3071a2d8bd"
CONFIG_PATH = Path(
    "config/research/strategy_growth_action_value_threshold_exact_value_sheet_v3.yaml"
)


def test_loads_v3_with_immutable_predecessor_and_dq_successor() -> None:
    result = load_strategy_growth_action_value_freeze_readiness_contract()

    assert result.contract_file_sha256 == EXPECTED_FILE_SHA256
    assert result.contract_canonical_sha256 == EXPECTED_CANONICAL_SHA256
    assert result.contract.sheet_version == "3.0.0-draft.1"
    assert result.contract.predecessor_binding.disposition == (
        "REQUEST_NEW_VERSION_RETAINED_IMMUTABLE"
    )
    assert result.predecessor.contract.sheet_version == "2.0.0-draft.1"
    assert result.dq_successor.contract.contract_version == "2.0.0-draft.1"
    assert result.contract.dq_successor_binding.executable_authority is False


def test_v3_binds_exact_primary_inventory_before_common_intersection() -> None:
    result = load_strategy_growth_action_value_freeze_readiness_contract()
    common = result.contract.common_series_contract

    assert common.primary_window_start == "2021-02-22"
    assert common.primary_window_end == "2025-12-02"
    assert common.expected_session_count == 1202
    assert common.expected_session_inventory_lf_sha256 == (
        "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0"
    )
    assert common.exact_expected_session_set_required is True
    assert common.simultaneous_candidate_and_comparator_session_drop_outcome == "INVALID"


def test_review_dispositions_match_independent_review() -> None:
    contract = load_strategy_growth_action_value_freeze_readiness_contract().contract
    dispositions = tuple(item.predecessor_disposition for item in contract.axis_contracts)

    assert dispositions == (
        "APPROVE_EXACTLY_AS_DRAFTED",
        "REJECT_AND_REQUEST_NEW_VERSION",
        "APPROVE_EXACTLY_AS_DRAFTED",
        "REJECT_AND_REQUEST_NEW_VERSION",
        "REJECT_AND_REQUEST_NEW_VERSION",
        "REJECT_AND_REQUEST_NEW_VERSION",
        "APPROVE_EXACTLY_AS_DRAFTED",
        "APPROVE_EXACTLY_AS_DRAFTED",
    )
    assert contract.review_evidence.approved_axis_count == 4
    assert contract.review_evidence.rejected_axis_count == 4
    assert contract.review_evidence.insufficient_dq_numeric_count == 4


def test_approved_axis_values_are_unchanged_from_v2() -> None:
    result = load_strategy_growth_action_value_freeze_readiness_contract()
    v3 = result.contract.axis_contracts
    v2 = result.predecessor.contract.axis_contracts

    assert v3[0].minimum_non_beta_return_delta == v2[0].minimum_non_beta_return_delta  # type: ignore[union-attr]
    assert (
        v3[2].maximum_actual_path_drawdown_regression
        == v2[2].maximum_actual_path_drawdown_regression
    )  # type: ignore[union-attr]
    assert (
        v3[6].maximum_annualized_actual_path_turnover
        == v2[6].maximum_annualized_actual_path_turnover
    )  # type: ignore[union-attr]
    assert v3[6].maximum_cost_drag_share == v2[6].maximum_cost_drag_share  # type: ignore[union-attr]
    assert v3[7].maximum_realized_beta_increment == v2[7].maximum_realized_beta_increment  # type: ignore[union-attr]
    assert v3[7].exposure_match_tolerance == v2[7].exposure_match_tolerance  # type: ignore[union-attr]


def test_cost_reconciliation_uses_max_absolute_session_residual_for_both_series() -> None:
    residual = maximum_cost_reconciliation_residual(
        candidate_gross=(Decimal("0.0100"), Decimal("0.0200")),
        candidate_net=(Decimal("0.0090"), Decimal("0.0188")),
        candidate_modeled_cost=(Decimal("0.0010"), Decimal("0.0011")),
        comparator_gross=(Decimal("0.0050"), Decimal("0.0060")),
        comparator_net=(Decimal("0.0045"), Decimal("0.0054")),
        comparator_modeled_cost=(Decimal("0.0005"), Decimal("0.0005")),
    )

    assert residual == Decimal("0.0001")


def test_cost_reconciliation_rejects_missing_or_misaligned_series() -> None:
    with pytest.raises(ValueError, match="cannot be empty"):
        maximum_cost_reconciliation_residual(
            candidate_gross=(),
            candidate_net=(),
            candidate_modeled_cost=(),
            comparator_gross=(),
            comparator_net=(),
            comparator_modeled_cost=(),
        )
    with pytest.raises(ValueError, match="exact session length"):
        maximum_cost_reconciliation_residual(
            candidate_gross=(Decimal("0"),),
            candidate_net=(Decimal("0"), Decimal("0")),
            candidate_modeled_cost=(Decimal("0"),),
            comparator_gross=(Decimal("0"),),
            comparator_net=(Decimal("0"),),
            comparator_modeled_cost=(Decimal("0"),),
        )


def test_first_active_session_is_left_censored_not_an_anchor() -> None:
    assert activation_anchors_without_left_boundary((True, True, False, True)) == (3,)
    assert activation_anchors_without_left_boundary((False, True, True, False)) == (1,)


def test_anchor_merge_is_transitive_across_adjacent_raw_chain() -> None:
    assert merge_anchor_indices_transitively((0, 19, 38, 70), maximum_gap=20) == (
        0,
        70,
    )


def test_missed_return_formula_has_explicit_parentheses() -> None:
    result = missed_return_for_qqq_weight(
        opening_qqq_weight=Decimal("0.25"),
        qqq_return=Decimal("0.02"),
        sgov_return=Decimal("0.001"),
    )

    assert result == Decimal("0.01425")


def test_episode_intervals_define_left_right_censor_and_transitive_merge() -> None:
    merged = active_episode_intervals(
        (True, True, False, True, True, False, False, True, False),
        merge_distance=4,
    )
    right = active_episode_intervals((False, True, True), merge_distance=20)

    assert merged.left_censored is True
    assert merged.right_censored_count == 0
    assert merged.intervals == (EpisodeInterval(3, 3, 7),)
    assert right.intervals == ()
    assert right.right_censored_count == 1


def test_episode_return_value_uses_inclusive_active_interval() -> None:
    value = episode_return_value(
        (0.01, 0.02, -0.01, 0.03),
        (0.005, 0.01, -0.005, 0.02),
        EpisodeInterval(1, 1, 2),
    )

    assert value == pytest.approx((1.02 * 0.99 - 1) - (1.01 * 0.995 - 1))


def test_modified_axes_expose_exact_corrected_semantics() -> None:
    axes = load_strategy_growth_action_value_freeze_readiness_contract().contract.axis_contracts
    net = axes[1]
    false_risk = axes[3]
    dq = axes[4]
    sample = axes[5]

    assert (
        net.reconciliation_contract.aggregation
        == "MAX_ABSOLUTE_SESSION_RESIDUAL_ACROSS_BOTH_SERIES"
    )  # type: ignore[union-attr]
    assert false_risk.event_contract.first_session_active_rule == "LEFT_CENSORED_NOT_AN_ANCHOR"  # type: ignore[union-attr]
    assert false_risk.event_contract.merge_rule == "TRANSITIVE_ADJACENT_RAW_ANCHOR_CHAIN"  # type: ignore[union-attr]
    assert (
        false_risk.event_contract.future_path_use == "EX_POST_ATTRIBUTION_ONLY_NOT_DECISION_INPUT"
    )  # type: ignore[union-attr]
    assert dq.numeric_review_disposition == "INSUFFICIENT_EVIDENCE_TO_APPROVE"  # type: ignore[union-attr]
    assert (
        sample.episode_contract.episode_end_rule
        == "INCLUDE_LAST_ACTIVE_SESSION_BEFORE_DEACTIVATION"
    )  # type: ignore[union-attr]
    assert sample.episode_contract.cross_slice_double_count_allowed is False  # type: ignore[union-attr]


def test_freeze_and_all_execution_paths_remain_closed() -> None:
    contract = load_strategy_growth_action_value_freeze_readiness_contract().contract

    assert contract.terminal.threshold_bundle_frozen is False
    assert contract.terminal.dq_successor_authorized is False
    assert contract.terminal.empirical_successor_authorized is False
    assert contract.safety.dq_run_authorized is False
    assert contract.safety.empirical_research_authorized is False
    assert contract.safety.backtest_authorized is False
    assert contract.safety.paper_allowed is False
    assert contract.safety.live_allowed is False
    assert contract.safety.broker_allowed is False
    assert contract.safety.production_effect == "none"
    assert contract.safety.broker_action == "none"


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload["common_series_contract"].__setitem__(
                "expected_session_count", 1201
            ),
            "expected_session_count",
        ),
        (
            lambda payload: payload["axis_contracts"][0].__setitem__("minimum_common_sessions", 0),
            "minimum_common_sessions",
        ),
        (
            lambda payload: payload["axis_contracts"][3]["event_contract"].__setitem__(
                "first_session_active_rule", "ANCHOR"
            ),
            "first_session_active_rule",
        ),
        (
            lambda payload: payload["terminal"].__setitem__("threshold_bundle_frozen", True),
            "threshold_bundle_frozen",
        ),
    ],
)
def test_semantic_tamper_is_rejected(mutate, match: str) -> None:
    payload = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    mutate(payload)

    with pytest.raises(ValueError, match=match):
        StrategyGrowthActionValueFreezeReadinessContract.model_validate(payload)


def test_canonical_replay_is_stable() -> None:
    contract = load_strategy_growth_action_value_freeze_readiness_contract().contract
    replay = StrategyGrowthActionValueFreezeReadinessContract.model_validate_json(
        contract.canonical_bytes
    )

    assert replay == contract
    assert replay.canonical_sha256 == EXPECTED_CANONICAL_SHA256
