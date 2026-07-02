from __future__ import annotations

from pathlib import Path

from dynamic_target_source_remediation_fixtures import (
    build_dynamic_target_source_remediation_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_dynamic_target_source_remediation_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "dynamic-target-baseline-source-remediation" in result.output


def test_dynamic_target_source_remediation_cli_writes_required_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_dynamic_target_source_remediation_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "dynamic-target-baseline-source-remediation",
            "--dynamic-preparation-dir",
            str(fixture["dynamic_preparation_dir"]),
            "--diagnostics-dir",
            str(fixture["diagnostics_dir"]),
            "--static-dry-run-dir",
            str(fixture["static_dry_run_dir"]),
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
            "source_remediation",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "dynamic_target_source_remediation_summary.json",
        "dynamic_target_source_family_ranking.json",
        "dynamic_target_gap_to_schema_matrix.json",
        "dynamic_target_remediation_action_matrix.json",
        "dynamic_target_baseline_schema_contract.json",
        "dynamic_target_schema_adapter_spec.json",
        "dynamic_target_2330_readiness_matrix.json",
        "dynamic_target_2330_task_route.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    assert (docs_root / "dynamic_target_baseline_source_remediation_report.md").exists()

    summary = read_json(output_dir / "dynamic_target_source_remediation_summary.json")
    assert summary["simulation_executed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
