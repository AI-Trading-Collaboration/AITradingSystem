from __future__ import annotations

from pathlib import Path

from dynamic_target_timestamp_remediation_fixtures import (
    build_dynamic_target_timestamp_remediation_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_dynamic_target_timestamp_remediation_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "dynamic-target-baseline-timestamp-remediation" in result.output


def test_dynamic_target_timestamp_remediation_cli_writes_required_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_dynamic_target_timestamp_remediation_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "dynamic-target-baseline-timestamp-remediation",
            "--source-remediation-dir",
            str(fixture["source_remediation_dir"]),
            "--dynamic-preparation-dir",
            str(fixture["dynamic_preparation_dir"]),
            "--diagnostics-dir",
            str(fixture["diagnostics_dir"]),
            "--source-binding-dir",
            str(fixture["source_binding_dir"]),
            "--simulation-policy-dir",
            str(fixture["simulation_policy_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "timestamp_remediation",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "dynamic_target_timestamp_remediation_summary.json",
        "dynamic_target_timestamp_gap_matrix.json",
        "dynamic_target_timestamp_remediation_policy.json",
        "dynamic_target_timestamp_derivation_matrix.json",
        "dynamic_target_known_at_semantics_report.json",
        "dynamic_target_validity_window_remediation_report.json",
        "dynamic_target_latency_policy_report.json",
        "dynamic_target_rebalance_timing_report.json",
        "dynamic_target_2331_readiness_matrix.json",
        "dynamic_target_2331_task_route.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    assert (docs_root / "dynamic_target_baseline_timestamp_remediation_report.md").exists()

    summary = read_json(output_dir / "dynamic_target_timestamp_remediation_summary.json")
    assert summary["simulation_executed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
