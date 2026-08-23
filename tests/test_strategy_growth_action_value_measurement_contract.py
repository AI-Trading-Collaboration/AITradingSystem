from __future__ import annotations

import copy
import hashlib
import math
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.strategy_growth_action_value_measurement_contract import (
    ExposureMatchedNoSignalComparatorContract,
    StrategyGrowthActionValueMeasurementContract,
    StrategyGrowthActionValueMeasurementContractError,
    activation_anchors,
    aggregate_joint_terminal,
    annualized_geometric_return,
    annualized_one_way_turnover,
    annualized_return_delta,
    circular_moving_block_bootstrap_lower_bound,
    compounded_return,
    contribution_shares,
    cost_drag_share,
    daily_ols_beta_with_intercept,
    drawdown_regression,
    event_missed_return_cost,
    false_risk_off_event_windows,
    fixed_exposure_matched_qqq_weight,
    fixed_weight_qqq_sgov_gross_returns,
    load_exposure_matched_no_signal_comparator_contract,
    load_strategy_growth_action_value_measurement_contract,
    max_drawdown,
    merge_anchor_indices,
    nearest_rank_quantile,
)

V1_FILE_SHA256 = "82f75b55bb4a9576775d4e60a9a31bc01b24d3b5b8cf270c6aabbed9e9d17e7f"
V1_CANONICAL_SHA256 = "14286008f464230921400c1def4173f34a6e9231e77c434504a5abab78451dfb"
COMPARATOR_FILE_SHA256 = "4ced9407b8b8bca7b973c34016868fbf3151017bcc0b8ab67db449a0fed3b850"
COMPARATOR_CANONICAL_SHA256 = "f429d9ffc12b227bf9fad6eed3340ca833fdb44dc179c5f29dfc8f0318d9e1cf"
V2_FILE_SHA256 = "bbb2e0ade108213269c3c9524b465836518457d932a6344887e6d8afb89ae620"
V2_CANONICAL_SHA256 = "b978e952c4767756025fc01b17f8694004e720a5bb44aa5dde893628a4d9c199"
AXIS_ORDER = (
    "NON_BETA_ACTION_VALUE",
    "NET_OF_COST_RETURN",
    "ACTUAL_PATH_DRAWDOWN_REGRESSION",
    "FALSE_RISK_OFF_COST",
    "CANONICAL_DQ_PIT",
    "SAMPLE_AND_WINDOW_DEPENDENCE",
    "ACTUAL_PATH_TURNOVER",
    "LEVERAGE_BETA_ATTRIBUTION",
)


def _loaded():
    return load_strategy_growth_action_value_measurement_contract()


def _all_outcomes(value: str = "PASS") -> tuple[tuple[str, str], ...]:
    return tuple((axis_id, value) for axis_id in AXIS_ORDER)


def test_contract_loads_exact_predecessor_comparator_and_v2_identity() -> None:
    loaded = _loaded()

    assert loaded.contract_file_sha256 == V2_FILE_SHA256
    assert loaded.contract_canonical_sha256 == V2_CANONICAL_SHA256
    assert loaded.predecessor.sheet_file_sha256 == V1_FILE_SHA256
    assert loaded.predecessor.sheet_canonical_sha256 == V1_CANONICAL_SHA256
    assert loaded.comparator.contract_file_sha256 == COMPARATOR_FILE_SHA256
    assert loaded.comparator.contract_canonical_sha256 == COMPARATOR_CANONICAL_SHA256
    assert (
        hashlib.sha256(
            (
                PROJECT_ROOT
                / "config/research/strategy_growth_action_value_threshold_exact_value_sheet_v1.yaml"
            ).read_bytes()
        ).hexdigest()
        == V1_FILE_SHA256
    )


def test_v2_is_unfrozen_and_carries_owner_adopted_review_dispositions() -> None:
    contract = _loaded().contract
    axes = {axis.axis_id: axis for axis in contract.axis_contracts}

    assert contract.sheet_status == "DRAFT_FOR_OWNER_REVIEW"
    assert contract.owner_instruction.adopted_review_conclusion == (
        "REQUEST_NEW_VERSION_BEFORE_ANY_FREEZE"
    )
    assert contract.decision_timing.threshold_bundle_frozen is False
    assert contract.terminal.status == "BLOCKED_OWNER_REVIEW_AND_DQ_AUTHORITY"
    assert contract.terminal.threshold_bundle_frozen is False
    assert all(axis.owner_review_state == "PENDING_OWNER_APPROVAL" for axis in axes.values())
    assert axes["CANONICAL_DQ_PIT"].predecessor_disposition == ("INSUFFICIENT_EVIDENCE_TO_APPROVE")
    assert all(
        axis.predecessor_disposition == "REJECT_AND_REQUEST_NEW_VERSION"
        for axis_id, axis in axes.items()
        if axis_id != "CANONICAL_DQ_PIT"
    )


def test_v2_freezes_pro_recommended_formula_corrections() -> None:
    axes = {axis.axis_id: axis for axis in _loaded().contract.axis_contracts}

    non_beta = axes["NON_BETA_ACTION_VALUE"]
    assert non_beta.bootstrap.method == "CIRCULAR_MOVING_BLOCK_BOOTSTRAP"
    assert non_beta.bootstrap.one_sided_confidence_level == Decimal("0.95")
    assert non_beta.bootstrap.lower_quantile == Decimal("0.05")
    assert non_beta.bootstrap.quantile_rule == "NEAREST_RANK_CEILING_P_TIMES_N"

    false_risk = axes["FALSE_RISK_OFF_COST"]
    assert false_risk.minimum_independent_qualifying_event_count == 10
    assert false_risk.event_contract.right_censored_anchor_rule.startswith("EXCLUDE")

    sample = axes["SAMPLE_AND_WINDOW_DEPENDENCE"]
    assert sample.minimum_independent_action_count_per_slice == 5
    assert sample.episode_contract.cross_slice_double_count_allowed is False

    turnover = axes["ACTUAL_PATH_TURNOVER"]
    assert turnover.half_turnover_multiplier_allowed is False
    assert turnover.same_session_opposite_fills_can_net is False
    assert turnover.nonpositive_cost_drag_denominator_outcome == "FAIL"

    beta = axes["LEVERAGE_BETA_ATTRIBUTION"]
    assert beta.beta_contract.method == "DAILY_OLS_SLOPE_WITH_INTERCEPT"
    assert beta.beta_contract.annualized is False
    assert beta.beta_contract.minimum_common_sessions == 252


def test_dq_numeric_values_remain_non_executable_owner_intent() -> None:
    dq = {axis.axis_id: axis for axis in _loaded().contract.axis_contracts}["CANONICAL_DQ_PIT"]

    assert dq.operational_authority_state == ("UNAVAILABLE_PENDING_INDEPENDENT_SERIAL_DQ_CONTRACT")
    assert dq.numeric_intent_draft.status == "OWNER_INTENT_ONLY_NOT_EXECUTABLE_AUTHORITY"
    assert dq.numeric_intent_draft.unknown_can_pass is False
    assert len(dq.required_serial_contract_fields) == 6
    assert _loaded().contract.safety.dq_run_authorized is False


def test_comparator_is_fixed_weight_no_signal_nonleveraged_and_nontradable() -> None:
    comparator = load_exposure_matched_no_signal_comparator_contract().contract

    assert comparator.role == "RESEARCH_ATTRIBUTION_ONLY_NOT_TRADABLE"
    assert comparator.universe.allowed_assets == ("QQQ", "SGOV")
    assert comparator.construction_contract.return_outcome_can_select_weight is False
    assert comparator.construction_contract.growth_signal_value_or_timestamp_read_allowed is False
    assert comparator.exposure_match_contract.beta_annualized is False
    assert comparator.safety.empirical_research_authorized is False
    assert comparator.safety.paper_allowed is False
    assert comparator.safety.live_allowed is False
    assert comparator.safety.broker_allowed is False


def test_contracts_are_canonical_replayable_and_duplicate_json_fails() -> None:
    loaded = _loaded()
    replay = StrategyGrowthActionValueMeasurementContract.from_json_bytes(
        loaded.contract.canonical_bytes
    )
    comparator_replay = ExposureMatchedNoSignalComparatorContract.from_json_bytes(
        loaded.comparator.contract.canonical_bytes
    )

    assert replay == loaded.contract
    assert comparator_replay == loaded.comparator.contract
    with pytest.raises(ValueError, match="duplicate JSON key"):
        StrategyGrowthActionValueMeasurementContract.from_json_bytes(b'{"x":1,"x":2}')


def test_strict_yaml_duplicate_key_is_rejected(tmp_path: Path) -> None:
    source = (
        PROJECT_ROOT / "config/research/exposure_matched_no_signal_comparator_contract_v1.yaml"
    ).read_text(encoding="utf-8")
    tampered = tmp_path / "comparator.yaml"
    tampered.write_text(
        source.replace(
            "schema_version: exposure_matched_no_signal_comparator_contract.v1\n",
            "schema_version: exposure_matched_no_signal_comparator_contract.v1\n"
            "schema_version: exposure_matched_no_signal_comparator_contract.v1\n",
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        StrategyGrowthActionValueMeasurementContractError,
        match="DUPLICATE_KEY",
    ):
        load_exposure_matched_no_signal_comparator_contract(
            contract_path=tampered, project_root=tmp_path
        )


@pytest.mark.parametrize(
    ("axis_index", "field_path", "replacement"),
    [
        (0, ("bootstrap", "one_sided_confidence_level"), Decimal("0.90")),
        (3, ("minimum_independent_qualifying_event_count",), 9),
        (5, ("minimum_independent_action_count_per_slice",), 3),
        (6, ("same_session_opposite_fills_can_net",), True),
        (7, ("beta_contract", "annualized"), True),
    ],
)
def test_pro_corrections_fail_closed_on_semantic_drift(
    axis_index: int, field_path: tuple[str, ...], replacement: object
) -> None:
    payload = copy.deepcopy(_loaded().contract.model_dump(mode="python"))
    target = payload["axis_contracts"][axis_index]
    for part in field_path[:-1]:
        target = target[part]
    target[field_path[-1]] = replacement

    with pytest.raises(ValidationError):
        StrategyGrowthActionValueMeasurementContract.model_validate(payload)


def test_unknown_contract_field_fails_closed() -> None:
    payload = copy.deepcopy(_loaded().contract.model_dump(mode="python"))
    payload["unreviewed_field"] = True

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        StrategyGrowthActionValueMeasurementContract.model_validate(payload)


def test_annualized_geometric_return_and_delta_use_separate_compounding() -> None:
    candidate = (0.01, -0.005, 0.002)
    comparator = (0.004, -0.003, 0.001)
    expected_candidate = math.prod(1.0 + value for value in candidate) ** (252 / 3) - 1
    expected_comparator = math.prod(1.0 + value for value in comparator) ** (252 / 3) - 1

    assert annualized_geometric_return(candidate) == pytest.approx(expected_candidate)
    assert annualized_return_delta(candidate, comparator) == pytest.approx(
        expected_candidate - expected_comparator
    )
    assert compounded_return(candidate) == pytest.approx(
        math.prod(1.0 + value for value in candidate) - 1
    )


def test_paired_return_identity_and_domain_fail_closed() -> None:
    with pytest.raises(
        StrategyGrowthActionValueMeasurementContractError,
        match="COMMON_SESSION_IDENTITY_INVALID",
    ):
        annualized_return_delta((0.01,), (0.01, 0.02))
    with pytest.raises(
        StrategyGrowthActionValueMeasurementContractError,
        match="RETURN_DOMAIN_INVALID",
    ):
        annualized_geometric_return((0.01, -1.0))


def test_nearest_rank_and_paired_circular_bootstrap_are_deterministic() -> None:
    assert nearest_rank_quantile(tuple(range(1, 21)), 0.05) == 1
    assert nearest_rank_quantile(tuple(range(1, 21)), 0.10) == 2
    candidate = (0.001,) * 252
    comparator = (0.0,) * 252

    first = circular_moving_block_bootstrap_lower_bound(candidate, comparator, resamples=64)
    second = circular_moving_block_bootstrap_lower_bound(candidate, comparator, resamples=64)

    assert first == second
    assert first == pytest.approx(annualized_return_delta(candidate, comparator))
    assert first > 0.0


def test_drawdown_resets_nav_and_regression_uses_absolute_depth() -> None:
    assert max_drawdown((0.10, -0.20, 0.05)) == pytest.approx(-0.20)
    assert drawdown_regression((0.10, -0.20, 0.05), (0.05, -0.10, 0.02)) == (pytest.approx(0.10))


def test_activation_anchor_and_episode_merge_keep_earliest_anchor() -> None:
    active = (False, True, True, False, True, False, False, True)
    assert activation_anchors(active) == (1, 4, 7)
    assert merge_anchor_indices((1, 4, 22, 43), merge_distance_sessions=20) == (
        1,
        22,
        43,
    )


def test_false_risk_off_windows_define_forward_offset_merge_and_right_censor() -> None:
    active = [False] * 50
    active[0] = True
    active[21] = True
    active[45] = True
    qqq_returns = (0.002,) * 50
    sgov_returns = (0.0,) * 50

    windows = false_risk_off_event_windows(active, qqq_returns, sgov_returns)

    assert tuple(item.anchor_index for item in windows) == (0, 21, 45)
    assert windows[0].forward_start_index == 1
    assert windows[0].forward_end_index_exclusive == 21
    assert windows[0].qualifies is True
    assert windows[1].qualifies is True
    assert windows[2].right_censored is True
    assert windows[2].qualifies is None


def test_event_missed_return_cost_uses_actual_opening_underweight() -> None:
    result = event_missed_return_cost((0.5, 0.5), (0.01, 0.01), (0.0, 0.0))

    assert result == pytest.approx((1.005**2) - 1.0)


def test_exposure_matched_comparator_uses_fixed_mean_weight_without_return_search() -> None:
    weight = fixed_exposure_matched_qqq_weight((0.2, 0.4, 0.6))
    returns = fixed_weight_qqq_sgov_gross_returns(weight, (0.01, -0.01), (0.001, 0.001))

    assert weight == pytest.approx(0.4)
    assert returns == pytest.approx((0.0046, -0.0034))


def test_turnover_uses_absolute_fill_notional_without_half_or_netting() -> None:
    fills = [(10.0, -10.0), *([()] * 251)]
    navs = (100.0,) * 252

    assert annualized_one_way_turnover(fills, navs) == pytest.approx(0.20)


def test_cost_drag_share_nonpositive_denominator_is_typed_fail() -> None:
    result = cost_drag_share((0.0,) * 252, (-0.0001,) * 252, (0.0002,) * 252)

    assert result.gross_non_beta_edge < 0.0
    assert result.cost_drag_share is None
    assert result.denominator_outcome == "FAIL_NONPOSITIVE"


def test_daily_ols_beta_is_nonannualized_slope_with_intercept() -> None:
    factor = tuple(((index % 21) - 10) / 10000 for index in range(252))
    dependent = tuple(0.0002 + 1.5 * value for value in factor)

    assert daily_ols_beta_with_intercept(dependent, factor) == pytest.approx(1.5)


def test_daily_ols_beta_insufficient_and_zero_variance_fail_closed() -> None:
    with pytest.raises(
        StrategyGrowthActionValueMeasurementContractError,
        match="SAMPLE_INSUFFICIENT",
    ):
        daily_ols_beta_with_intercept((0.0,) * 251, (0.001,) * 251)
    with pytest.raises(
        StrategyGrowthActionValueMeasurementContractError,
        match="factor variance is zero",
    ):
        daily_ols_beta_with_intercept((0.0,) * 252, (0.001,) * 252)


def test_contribution_share_uses_absolute_episode_values_and_all_episode_denominator() -> None:
    shares = contribution_shares(
        (1.0, -2.0, 1.0),
        ("PRIMARY_2021_PARTIAL", "RATE_HIKE_BEAR_2022", "PRIMARY_2021_PARTIAL"),
    )

    assert shares["PRIMARY_2021_PARTIAL"] == pytest.approx(0.5)
    assert shares["RATE_HIKE_BEAR_2022"] == pytest.approx(0.5)
    with pytest.raises(
        StrategyGrowthActionValueMeasurementContractError,
        match="denominator is nonpositive",
    ):
        contribution_shares((0.0,), ("PRIMARY_2021_PARTIAL",))


@pytest.mark.parametrize(
    ("axis_outcome", "expected"),
    [
        ("PASS", "GLOBAL_PASS"),
        ("INSUFFICIENT", "GLOBAL_INSUFFICIENT"),
        ("FAIL", "GLOBAL_FAIL"),
        ("INVALID", "GLOBAL_INVALID"),
    ],
)
def test_joint_terminal_is_mechanical(axis_outcome: str, expected: str) -> None:
    assert aggregate_joint_terminal(_all_outcomes(axis_outcome)) == expected


def test_joint_terminal_precedence_disallows_compensation() -> None:
    outcomes = list(_all_outcomes())
    outcomes[0] = (AXIS_ORDER[0], "INSUFFICIENT")
    outcomes[1] = (AXIS_ORDER[1], "FAIL")
    outcomes[2] = (AXIS_ORDER[2], "INVALID")

    assert aggregate_joint_terminal(outcomes) == "GLOBAL_INVALID"


def test_joint_terminal_requires_exact_complete_axis_order() -> None:
    with pytest.raises(
        StrategyGrowthActionValueMeasurementContractError,
        match="AXIS_OUTCOME_SET_INVALID",
    ):
        aggregate_joint_terminal(_all_outcomes()[:-1])


def test_every_execution_and_trading_boundary_remains_closed() -> None:
    contract = _loaded().contract
    safety = contract.safety

    assert safety.empirical_research_authorized is False
    assert safety.dq_run_authorized is False
    assert safety.cache_mutation_authorized is False
    assert safety.backtest_authorized is False
    assert safety.holdout_access_authorized is False
    assert safety.external_action_authorized is False
    assert safety.investment_conclusion_authorized is False
    assert safety.paper_allowed is False
    assert safety.live_allowed is False
    assert safety.broker_allowed is False
    assert safety.production_effect == "none"
    assert safety.broker_action == "none"
