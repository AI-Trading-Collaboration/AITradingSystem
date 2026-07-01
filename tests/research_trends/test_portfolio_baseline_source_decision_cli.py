from __future__ import annotations

from pathlib import Path

from portfolio_baseline_source_decision_fixtures import (
    read_json,
    write_paper_portfolio_fixture,
    write_portfolio_config_fixture,
    write_simulation_policy_fixture,
    write_source_binding_fixture,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_portfolio_baseline_source_decision_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "portfolio-baseline-source-decision" in result.output


def test_portfolio_baseline_source_decision_cli_writes_outputs(
    tmp_path: Path,
) -> None:
    source_binding_dir = write_source_binding_fixture(tmp_path)
    simulation_policy_dir = write_simulation_policy_fixture(tmp_path)
    portfolio_config_dir = write_portfolio_config_fixture(tmp_path)
    paper_config = write_paper_portfolio_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "portfolio-baseline-source-decision",
            "--source-binding-dir",
            str(source_binding_dir),
            "--simulation-policy-dir",
            str(simulation_policy_dir),
            "--portfolio-config-dir",
            str(portfolio_config_dir),
            "--paper-portfolio-config",
            str(paper_config),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "baseline_source_decision",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "portfolio_baseline_source_decision_summary.json",
        "portfolio_baseline_candidate_matrix.json",
        "portfolio_baseline_source_feasibility_matrix.json",
        "portfolio_baseline_pit_reproducibility_audit.json",
        "portfolio_baseline_risk_matrix.json",
        "portfolio_baseline_field_requirement_matrix.json",
        "portfolio_baseline_recommendation.json",
        "recommended_exposure_cap_simulation_baseline.json",
        "exposure_cap_2326_task_route.json",
        "portfolio_baseline_source_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    assert (docs_root / "portfolio_baseline_source_decision_report.md").exists()
    assert (docs_root / "portfolio_baseline_candidate_matrix.md").exists()
    assert (docs_root / "portfolio_baseline_pit_reproducibility_audit.md").exists()
    assert (docs_root / "recommended_exposure_cap_simulation_baseline.md").exists()

    summary = read_json(output_dir / "portfolio_baseline_source_decision_summary.json")
    assert summary["simulation_executed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert summary["selected_baseline_for_2326"] == "static_etf_allocation_baseline"

    route = read_json(output_dir / "exposure_cap_2326_task_route.json")
    assert route["next_task"] == (
        "TRADING-2326_Source_Bound_Exposure_Cap_Dry_Run_With_Static_ETF_Baseline"
    )
