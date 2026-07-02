from __future__ import annotations

from pathlib import Path

from dynamic_dry_run_readiness_fixtures import (
    build_dynamic_dry_run_readiness_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_dynamic_dry_run_readiness_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "dynamic-target-baseline-dry-run-readiness-with-pit-caveat" in result.output


def test_dynamic_dry_run_readiness_cli_writes_required_outputs(tmp_path: Path) -> None:
    fixture = build_dynamic_dry_run_readiness_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "dynamic-target-baseline-dry-run-readiness-with-pit-caveat",
            "--timestamp-remediation-dir",
            str(fixture["timestamp_remediation_dir"]),
            "--source-remediation-dir",
            str(fixture["source_remediation_dir"]),
            "--dynamic-preparation-dir",
            str(fixture["dynamic_preparation_dir"]),
            "--source-binding-dir",
            str(fixture["source_binding_dir"]),
            "--simulation-policy-dir",
            str(fixture["simulation_policy_dir"]),
            "--static-dry-run-dir",
            str(fixture["static_dry_run_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "dry_run_readiness_with_pit_caveat",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "dynamic_dry_run_readiness_summary.json",
        "dynamic_dry_run_gate_checklist.json",
        "dynamic_dry_run_pit_caveat_acceptance_report.json",
        "dynamic_dry_run_wrapper_field_validation_matrix.json",
        "dynamic_dry_run_timestamp_alignment_matrix.json",
        "dynamic_dry_run_risk_cap_alignment_matrix.json",
        "dynamic_dry_run_market_data_alignment_matrix.json",
        "dynamic_dry_run_policy_compatibility_matrix.json",
        "dynamic_dry_run_input_contract.json",
        "dynamic_dry_run_data_quality_precheck.json",
        "dynamic_dry_run_interpretation_boundary.json",
        "dynamic_dry_run_2332_readiness_matrix.json",
        "dynamic_dry_run_2332_task_route.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    assert (
        docs_root / "dynamic_target_baseline_dry_run_readiness_with_pit_caveat.md"
    ).exists()

    summary = read_json(output_dir / "dynamic_dry_run_readiness_summary.json")
    assert summary["simulation_executed"] is False
    assert summary["2332_allowed"] is True
    assert summary["next_task"] == (
        "TRADING-2332_Source_Bound_Exposure_Cap_Dry_Run_With_Dynamic_Target_Baseline"
    )
    assert summary["promotion_allowed"] is False
    assert summary["broker_action"] == "none"
