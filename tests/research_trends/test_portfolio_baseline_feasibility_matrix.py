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
    build_portfolio_baseline_risk_matrix,
    build_portfolio_baseline_source_feasibility_matrix,
    load_simulation_policy_outputs,
    load_source_binding_outputs,
)


def test_synthetic_baseline_is_dry_run_eligible(tmp_path: Path) -> None:
    row = _row(_feasibility_rows(tmp_path), "synthetic_observe_only_baseline")

    assert row["dry_run_eligible"] is True
    assert row["full_simulation_eligible"] is False


def test_static_etf_baseline_is_dry_run_eligible_when_config_exists(
    tmp_path: Path,
) -> None:
    row = _row(_feasibility_rows(tmp_path), "static_etf_allocation_baseline")

    assert row["source_available"] is True
    assert row["dry_run_eligible"] is True


def test_dynamic_target_exposure_is_blocked_when_artifact_missing(
    tmp_path: Path,
) -> None:
    row = _row(_feasibility_rows(tmp_path), "dynamic_strategy_target_exposure_baseline")

    assert row["dry_run_eligible"] is False
    assert "pit_target_exposure_artifact_missing" in row["blockers"]


def test_actual_holdings_baseline_has_high_privacy_risk(tmp_path: Path) -> None:
    risk_row = _row(_risk_rows(tmp_path), "actual_holdings_derived_baseline")

    assert risk_row["privacy_risk"] == "HIGH"


def test_recommended_for_2326_has_single_primary_baseline(tmp_path: Path) -> None:
    rows = _feasibility_rows(tmp_path)
    recommended = [row for row in rows if row["recommended_for_2326"]]

    assert len(recommended) == 1
    assert recommended[0]["baseline_id"] == "static_etf_allocation_baseline"


def _candidate_rows(tmp_path: Path) -> list[dict[str, object]]:
    return build_portfolio_baseline_candidate_matrix(
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


def _feasibility_rows(tmp_path: Path) -> list[dict[str, object]]:
    return build_portfolio_baseline_source_feasibility_matrix(_candidate_rows(tmp_path))


def _risk_rows(tmp_path: Path) -> list[dict[str, object]]:
    return build_portfolio_baseline_risk_matrix(_candidate_rows(tmp_path))


def _row(
    rows: list[dict[str, object]],
    baseline_id: str,
) -> dict[str, object]:
    return next(row for row in rows if row["baseline_id"] == baseline_id)
