from __future__ import annotations

from pathlib import Path

from source_bound_static_etf_dry_run_fixtures import (
    build_source_bound_static_etf_dry_run_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_source_bound_static_etf_dry_run_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "source-bound-exposure-cap-dry-run" in result.output


def test_source_bound_static_etf_dry_run_cli_writes_required_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_source_bound_static_etf_dry_run_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "source-bound-exposure-cap-dry-run",
            "--source-binding-dir",
            str(fixture["source_binding_dir"]),
            "--baseline-decision-dir",
            str(fixture["baseline_decision_dir"]),
            "--simulation-policy-dir",
            str(fixture["simulation_policy_dir"]),
            "--portfolio-config-dir",
            str(fixture["portfolio_config_dir"]),
            "--market-data-source",
            str(fixture["prices_path"]),
            "--rates-source",
            str(fixture["rates_path"]),
            "--policy",
            str(fixture["policy_path"]),
            "--quality-as-of",
            "2023-01-10",
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "static_etf_baseline_dry_run",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "source_bound_static_etf_dry_run_summary.json",
        "static_etf_baseline_source_report.json",
        "static_etf_baseline_exposure_schedule.json",
        "static_etf_baseline_exposure_schedule.csv",
        "risk_cap_trigger_alignment_matrix.json",
        "risk_cap_trigger_alignment_matrix.csv",
        "source_bound_static_etf_exposure_cap_dry_run_result.json",
        "source_bound_static_etf_exposure_cap_dry_run_result.csv",
        "exposure_cap_vs_no_cap_static_etf_comparison.json",
        "exposure_cap_vs_no_cap_static_etf_comparison.csv",
        "exposure_cap_binding_day_matrix.json",
        "exposure_cap_binding_day_matrix.csv",
        "exposure_cap_turnover_impact_report.json",
        "exposure_cap_cooldown_impact_report.json",
        "exposure_cap_false_risk_cap_cost_report.json",
        "exposure_cap_missed_upside_cost_report.json",
        "exposure_cap_downside_protection_proxy_report.json",
        "exposure_cap_data_quality_report.json",
        "exposure_cap_simulation_interpretation_boundary.json",
        "exposure_cap_2327_task_route.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    for filename in ("target_weights.csv", "rebalance_instruction.json", "broker_order.json"):
        assert not (output_dir / filename).exists(), filename

    summary = read_json(output_dir / "source_bound_static_etf_dry_run_summary.json")
    assert summary["selected_baseline"] == "static_etf_allocation_baseline"
    assert summary["data_quality_gate_executed"] is True
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert (docs_root / "source_bound_exposure_cap_dry_run_static_etf_report.md").exists()
