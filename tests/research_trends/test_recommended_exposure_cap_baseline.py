from __future__ import annotations

from pathlib import Path

from portfolio_baseline_source_decision_fixtures import (
    write_paper_portfolio_fixture,
    write_portfolio_config_fixture,
    write_simulation_policy_fixture,
    write_source_binding_fixture,
)

from ai_trading_system.portfolio_baseline_source_decision import (
    build_exposure_cap_2326_task_route,
    build_portfolio_baseline_candidate_matrix,
    build_portfolio_baseline_pit_reproducibility_audit,
    build_portfolio_baseline_recommendation,
    build_portfolio_baseline_source_feasibility_matrix,
    load_simulation_policy_outputs,
    load_source_binding_outputs,
)


def test_static_etf_baseline_is_selected_before_synthetic(tmp_path: Path) -> None:
    recommendation = _recommendation(tmp_path)

    assert recommendation["selected_for_2326"] == "static_etf_allocation_baseline"


def test_synthetic_baseline_is_fallback(tmp_path: Path) -> None:
    recommendation = _recommendation(tmp_path)

    assert recommendation["fallback_baseline"] == "synthetic_observe_only_baseline"


def test_dynamic_target_baseline_is_medium_term(tmp_path: Path) -> None:
    recommendation = _recommendation(tmp_path)

    assert recommendation["recommended_medium_term_baseline"] == [
        "dynamic_strategy_target_exposure_baseline"
    ]


def test_actual_holdings_is_not_current_selected_baseline(tmp_path: Path) -> None:
    recommendation = _recommendation(tmp_path)

    assert recommendation["selected_for_2326"] != "actual_holdings_derived_baseline"
    assert (
        "actual_holdings_derived_baseline_manual_reference_only"
        in recommendation["recommended_long_term_baseline"]
    )


def test_recommendation_and_route_keep_safety_gates_closed(tmp_path: Path) -> None:
    recommendation, feasibility = _recommendation_and_feasibility(tmp_path)
    route = build_exposure_cap_2326_task_route(
        recommendation=recommendation,
        feasibility_rows=feasibility,
    )

    for payload in (recommendation, route):
        assert payload["promotion_allowed"] is False
        assert payload["paper_shadow_allowed"] is False
        assert payload["production_allowed"] is False
        assert payload["broker_action"] == "none"
    assert route["next_task"] == (
        "TRADING-2326_Source_Bound_Exposure_Cap_Dry_Run_With_Static_ETF_Baseline"
    )


def _recommendation(tmp_path: Path) -> dict[str, object]:
    recommendation, _feasibility = _recommendation_and_feasibility(tmp_path)
    return recommendation


def _recommendation_and_feasibility(
    tmp_path: Path,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    candidate_rows = build_portfolio_baseline_candidate_matrix(
        source_binding=load_source_binding_outputs(write_source_binding_fixture(tmp_path)),
        simulation_policy=load_simulation_policy_outputs(
            write_simulation_policy_fixture(tmp_path)
        ),
        portfolio_config_dir=write_portfolio_config_fixture(tmp_path),
        paper_portfolio_config=write_paper_portfolio_fixture(tmp_path),
        actual_holdings_source=None,
        allow_synthetic_baseline=True,
        target_assets=["QQQ", "SPY", "SMH"],
    )
    feasibility = build_portfolio_baseline_source_feasibility_matrix(candidate_rows)
    pit_rows = build_portfolio_baseline_pit_reproducibility_audit(candidate_rows)
    return (
        build_portfolio_baseline_recommendation(
            feasibility_rows=feasibility,
            pit_rows=pit_rows,
        ),
        feasibility,
    )
