from __future__ import annotations

from datetime import date, timedelta

import pytest

from ai_trading_system.first_layer_foundational_falsification_execution import (
    DiagnosticPlan,
    FoundationalFalsificationExecutionError,
    calendar_year_attribution,
    candidate_return_path,
    comparator_return_path,
    contiguous_episode_attribution,
    cost_sensitivity,
    leave_one_year_out,
    load_run_authorization,
    paired_circular_moving_block_bootstrap,
    sgov_carry_sensitivity,
    state_transition_attribution,
)
from ai_trading_system.research_quality.frozen_signal_value_confirmation_execution import (
    calculate_candidate_primary,
    calculate_static_comparator_primary,
)


def _plan() -> DiagnosticPlan:
    sessions = (
        date(2021, 12, 30),
        date(2021, 12, 31),
        date(2022, 1, 3),
        date(2023, 1, 3),
        date(2024, 1, 3),
        date(2025, 1, 3),
    )
    return DiagnosticPlan(
        sessions=sessions,
        qqq_prices=(100.0, 102.0, 101.0, 105.0, 103.0, 108.0),
        sgov_prices=(100.0, 100.01, 100.02, 100.03, 100.04, 100.05),
        states=("neutral", "constructive", "risk_on", "defensive", "risk_off", "neutral"),
        interval_targets=(0.0, 1.0, 1.0, 0.0, 1.0),
        long_interval_count=3,
        comparator_weight=0.6,
        action_counts={"FLAT": 3, "LONG_CALL": 3},
    )


def test_real_authorization_is_standing_scope_and_zero_external_action() -> None:
    loaded = load_run_authorization()
    assert loaded.payload["owner_decision"]["authorization_state"] == "STANDING_OWNER_SCOPE"
    assert loaded.payload["run_envelope"]["local_foundational_runs"] == 1
    for field in (
        "data_downloads",
        "cache_mutations",
        "quantconnect_actions",
        "option_backtests",
        "external_provider_actions",
        "orders",
        "fills",
        "positions",
    ):
        assert loaded.payload["run_envelope"][field] == 0


def test_interval_paths_reconcile_to_trading_2550_accounting() -> None:
    plan = _plan()
    candidate = candidate_return_path(plan.qqq_prices, plan.interval_targets, one_way_cost_bps=5.0)
    comparator = comparator_return_path(
        plan.qqq_prices, plan.comparator_weight, one_way_cost_bps=5.0
    )
    old_candidate = calculate_candidate_primary(plan.qqq_prices, plan.interval_targets)
    old_comparator = calculate_static_comparator_primary(plan.qqq_prices, plan.comparator_weight)
    assert candidate.final_value == pytest.approx(old_candidate.final_value, abs=1e-8)
    assert comparator.final_value == pytest.approx(old_comparator.final_value, abs=1e-8)
    candidate_factor = 1.0
    comparator_factor = 1.0
    for candidate_return, comparator_return in zip(
        candidate.interval_returns, comparator.interval_returns, strict=True
    ):
        candidate_factor *= 1.0 + candidate_return
        comparator_factor *= 1.0 + comparator_return
    assert candidate_factor * 100_000.0 == pytest.approx(candidate.final_value, abs=1e-8)
    assert comparator_factor * 100_000.0 == pytest.approx(comparator.final_value, abs=1e-8)


def test_candidate_rejects_nonbinary_target() -> None:
    with pytest.raises(FoundationalFalsificationExecutionError, match="non-binary"):
        candidate_return_path((100.0, 101.0), (0.5,), one_way_cost_bps=5.0)


def test_year_leave_out_and_episode_rules_are_fixed() -> None:
    plan = _plan()
    candidate = candidate_return_path(plan.qqq_prices, plan.interval_targets, one_way_cost_bps=5.0)
    comparator = comparator_return_path(
        plan.qqq_prices, plan.comparator_weight, one_way_cost_bps=5.0
    )
    years = calendar_year_attribution(plan, candidate, comparator)
    assert [row["calendar_year"] for row in years] == [2021, 2022, 2023, 2024, 2025]
    assert [row["interval_count"] for row in years] == [2, 1, 1, 1, 0]
    assert years[0]["partial_year"] is True
    assert years[-1]["partial_year"] is True
    leave_out = leave_one_year_out(plan, candidate, comparator)
    assert [row["excluded_calendar_year"] for row in leave_out] == [2021, 2022, 2023, 2024, 2025]
    episodes = contiguous_episode_attribution(plan)
    assert [(row["interval_count"], row["start_session"]) for row in episodes] == [
        (2, "2021-12-31"),
        (1, "2024-01-03"),
    ]


def test_cost_and_sgov_diagnostics_recompute_both_paths() -> None:
    plan = _plan()
    costs = cost_sensitivity(plan)
    rows = costs["rows"]
    assert [row["one_way_cost_bps"] for row in rows] == [5.0, 10.0, 15.0, 20.0]
    assert rows[-1]["candidate_net_total_return_pct"] < rows[0]["candidate_net_total_return_pct"]
    assert rows[-1]["comparator_net_total_return_pct"] < rows[0]["comparator_net_total_return_pct"]
    carry = sgov_carry_sensitivity(plan)
    zero_candidate = candidate_return_path(
        plan.qqq_prices, plan.interval_targets, one_way_cost_bps=5.0
    )
    zero_comparator = comparator_return_path(
        plan.qqq_prices, plan.comparator_weight, one_way_cost_bps=5.0
    )
    assert carry["candidate_net_total_return_pct"] > zero_candidate.net_total_return_pct
    assert carry["comparator_net_total_return_pct"] > zero_comparator.net_total_return_pct
    assert carry["sgov_trade_or_extra_cost_modeled"] is False


def test_paired_bootstrap_is_seeded_and_ordered() -> None:
    candidate = (0.01, -0.005, 0.02, 0.0, 0.004) * 5
    comparator = (0.005, -0.004, 0.01, 0.001, 0.002) * 5
    first = paired_circular_moving_block_bootstrap(
        candidate, comparator, block_lengths=(3,), seed=2555, replicates=200
    )
    second = paired_circular_moving_block_bootstrap(
        candidate, comparator, block_lengths=(3,), seed=2555, replicates=200
    )
    assert first == second
    assert first[0]["percentile_2_5"] <= first[0]["percentile_50"]
    assert first[0]["percentile_50"] <= first[0]["percentile_97_5"]


def test_state_transition_maturity_is_explicit() -> None:
    rows = state_transition_attribution(_plan())
    neutral_constructive = next(
        row for row in rows if row["from_state"] == "neutral" and row["to_state"] == "constructive"
    )
    horizons = {row["horizon_sessions"]: row for row in neutral_constructive["forward_horizons"]}
    assert horizons[1]["mature_count"] == 1
    assert horizons[5]["mature_count"] == 0
    assert horizons[5]["missing_count"] == 1
    assert horizons[5]["mean_forward_return_pct"] is None


def test_transition_rows_use_stable_state_order() -> None:
    start = date(2021, 1, 1)
    states = ("risk_on", "risk_off", "risk_on", "risk_off")
    plan = DiagnosticPlan(
        sessions=tuple(start + timedelta(days=index) for index in range(4)),
        qqq_prices=(100.0, 101.0, 102.0, 103.0),
        sgov_prices=(100.0, 100.0, 100.0, 100.0),
        states=states,
        interval_targets=(0.0, 0.0, 0.0),
        long_interval_count=0,
        comparator_weight=0.0,
        action_counts={"FLAT": 4, "LONG_CALL": 0},
    )
    rows = state_transition_attribution(plan)
    assert [(row["from_state"], row["to_state"]) for row in rows] == [
        ("risk_off", "risk_on"),
        ("risk_on", "risk_off"),
    ]
