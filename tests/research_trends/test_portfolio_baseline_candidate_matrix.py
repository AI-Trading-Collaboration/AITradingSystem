from __future__ import annotations

from pathlib import Path

from portfolio_baseline_source_decision_fixtures import (
    write_paper_portfolio_fixture,
    write_portfolio_config_fixture,
    write_simulation_policy_fixture,
    write_source_binding_fixture,
)

from ai_trading_system.portfolio_baseline_source_decision import (
    build_portfolio_baseline_candidate_matrix,
    load_simulation_policy_outputs,
    load_source_binding_outputs,
)


def test_candidate_matrix_contains_all_required_baselines(tmp_path: Path) -> None:
    rows = _candidate_rows(tmp_path)

    assert {row["baseline_id"] for row in rows} == {
        "synthetic_observe_only_baseline",
        "static_etf_allocation_baseline",
        "dynamic_strategy_target_exposure_baseline",
        "paper_portfolio_advisory_baseline",
        "actual_holdings_derived_baseline",
    }


def test_candidate_matrix_generates_synthetic_baseline(tmp_path: Path) -> None:
    row = _row(_candidate_rows(tmp_path), "synthetic_observe_only_baseline")

    assert row["baseline_type"] == "synthetic_observe_only"
    assert row["source_available"] is True
    assert row["pit_status"] == "SYNTHETIC_OBSERVE_ONLY"


def test_candidate_matrix_generates_static_etf_baseline(tmp_path: Path) -> None:
    row = _row(_candidate_rows(tmp_path), "static_etf_allocation_baseline")

    assert row["baseline_type"] == "static_etf_allocation"
    assert row["source_available"] is True
    assert set(row["target_assets_supported"]) >= {"QQQ", "SPY", "SMH"}


def test_candidate_matrix_generates_dynamic_target_baseline(tmp_path: Path) -> None:
    row = _row(_candidate_rows(tmp_path), "dynamic_strategy_target_exposure_baseline")

    assert row["baseline_type"] == "dynamic_strategy_target_exposure"
    assert row["pit_status"] == "BLOCKED"
    assert row["source_available"] is False


def test_candidate_matrix_generates_paper_portfolio_baseline(tmp_path: Path) -> None:
    row = _row(_candidate_rows(tmp_path), "paper_portfolio_advisory_baseline")

    assert row["baseline_type"] == "paper_portfolio_advisory"
    assert row["source_available"] is True
    assert row["privacy_or_account_risk"] == "MEDIUM"


def test_actual_holdings_baseline_is_not_recommended_now(tmp_path: Path) -> None:
    row = _row(_candidate_rows(tmp_path), "actual_holdings_derived_baseline")

    assert row["baseline_type"] == "actual_holdings_derived"
    assert row["source_available"] is False
    assert row["privacy_or_account_risk"] == "HIGH"
    assert row["recommended_usage"] == "not recommended for current stage"


def _candidate_rows(tmp_path: Path) -> list[dict[str, object]]:
    source_binding = load_source_binding_outputs(write_source_binding_fixture(tmp_path))
    simulation_policy = load_simulation_policy_outputs(
        write_simulation_policy_fixture(tmp_path)
    )
    return build_portfolio_baseline_candidate_matrix(
        source_binding=source_binding,
        simulation_policy=simulation_policy,
        portfolio_config_dir=write_portfolio_config_fixture(tmp_path),
        paper_portfolio_config=write_paper_portfolio_fixture(tmp_path),
        actual_holdings_source=None,
        allow_synthetic_baseline=True,
        target_assets=["QQQ", "SPY", "SMH"],
    )


def _row(
    rows: list[dict[str, object]],
    baseline_id: str,
) -> dict[str, object]:
    return next(row for row in rows if row["baseline_id"] == baseline_id)
