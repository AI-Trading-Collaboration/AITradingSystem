from __future__ import annotations

from pathlib import Path

from dynamic_target_baseline_preparation_fixtures import (
    build_dynamic_target_baseline_preparation_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_dynamic_target_baseline_preparation_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "dynamic-target-baseline-preparation" in result.output


def test_dynamic_target_baseline_preparation_cli_writes_required_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_dynamic_target_baseline_preparation_fixture(tmp_path, source_kind="ready")
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "dynamic-target-baseline-preparation",
            "--diagnostics-dir",
            str(fixture["diagnostics_dir"]),
            "--static-dry-run-dir",
            str(fixture["static_dry_run_dir"]),
            "--baseline-decision-dir",
            str(fixture["baseline_decision_dir"]),
            "--source-binding-dir",
            str(fixture["source_binding_dir"]),
            "--simulation-policy-dir",
            str(fixture["simulation_policy_dir"]),
            "--candidate-artifact-roots",
            str(fixture["candidate_root"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "dynamic_target_baseline_preparation",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "dynamic_target_baseline_preparation_summary.json",
        "dynamic_target_source_inventory.json",
        "dynamic_target_source_gap_matrix.json",
        "dynamic_target_pit_replayability_audit.json",
        "dynamic_target_field_coverage_matrix.json",
        "dynamic_target_risk_cap_alignment_readiness.json",
        "dynamic_target_market_data_alignment_readiness.json",
        "dynamic_target_baseline_candidate_matrix.json",
        "recommended_dynamic_target_baseline_spec.json",
        "dynamic_target_baseline_2329_readiness_matrix.json",
        "dynamic_target_baseline_2329_task_route.json",
        "dynamic_target_baseline_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    assert (docs_root / "dynamic_target_baseline_preparation_report.md").exists()

    summary = read_json(output_dir / "dynamic_target_baseline_preparation_summary.json")
    assert summary["simulation_executed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert summary["aits_validate_data_executed"] is False
