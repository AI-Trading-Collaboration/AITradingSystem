from __future__ import annotations

from pathlib import Path

from dynamic_target_exposure_cap_dry_run_fixtures import (
    build_dynamic_target_exposure_cap_dry_run_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_dynamic_target_exposure_cap_dry_run_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "source-bound-exposure-cap-dynamic-target-dry-run" in result.output


def test_dynamic_target_exposure_cap_dry_run_cli_writes_required_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_dynamic_target_exposure_cap_dry_run_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "source-bound-exposure-cap-dynamic-target-dry-run",
            "--dry-run-readiness-dir",
            str(fixture["dry_run_readiness_dir"]),
            "--timestamp-remediation-dir",
            str(fixture["timestamp_remediation_dir"]),
            "--source-remediation-dir",
            str(fixture["source_remediation_dir"]),
            "--source-binding-dir",
            str(fixture["source_binding_dir"]),
            "--simulation-policy-dir",
            str(fixture["simulation_policy_dir"]),
            "--static-dry-run-dir",
            str(fixture["static_dry_run_dir"]),
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
            "dynamic_target_baseline_dry_run",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "dynamic_target_exposure_cap_dry_run_summary.json",
        "dynamic_target_baseline_source_report.json",
        "dynamic_target_baseline_exposure_schedule.json",
        "dynamic_target_risk_cap_trigger_alignment_matrix.json",
        "dynamic_target_exposure_cap_dry_run_result.json",
        "dynamic_target_cap_vs_no_cap_comparison.json",
        "dynamic_target_strategy_overlap_report.json",
        "dynamic_target_static_vs_dynamic_comparison.json",
        "dynamic_target_data_quality_report.json",
        "dynamic_target_pit_caveat_interpretation_boundary.json",
        "dynamic_target_2333_task_route.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename

    summary = read_json(output_dir / "dynamic_target_exposure_cap_dry_run_summary.json")
    assert summary["data_quality_gate_executed"] is True
    assert summary["simulation_executed"] is True
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert (docs_root / "dynamic_target_exposure_cap_dry_run_report.md").exists()
