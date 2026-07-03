from __future__ import annotations

from pathlib import Path

from high_intensity_threshold_selection_fixtures import (
    build_high_intensity_threshold_selection_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_high_intensity_threshold_selection_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "high-intensity-risk-cap-threshold-selection" in result.output


def test_high_intensity_threshold_selection_cli_writes_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_threshold_selection_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-threshold-selection",
            "--forward-observe-plan-dir",
            str(fixture["forward_observe_plan_dir"]),
            "--dynamic-diagnostics-dir",
            str(fixture["dynamic_diagnostics_dir"]),
            "--dynamic-dry-run-dir",
            str(fixture["dynamic_dry_run_dir"]),
            "--readiness-dir",
            str(fixture["readiness_dir"]),
            "--timestamp-remediation-dir",
            str(fixture["timestamp_remediation_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "threshold_selection",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_threshold_selection_summary.json",
        "high_intensity_threshold_candidate_scoring_matrix.json",
        "high_intensity_trigger_density_guardrail.json",
        "high_intensity_threshold_selection_decision_matrix.json",
        "high_intensity_selected_trigger_rule.json",
        "high_intensity_selected_trigger_contract.json",
        "high_intensity_event_logger_input_contract.json",
        "high_intensity_2336_readiness_checklist.json",
        "high_intensity_2336_task_route.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(output_dir / "high_intensity_threshold_selection_summary.json")
    assert summary["runtime_observe_started"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert summary["selected_threshold_id"] == "COMPOSITE_HIGH_INTENSITY_RULE"
    assert (
        summary["next_task"]
        == "TRADING-2336_High_Intensity_Risk_Cap_Forward_Observe_Event_Logger"
    )
    assert (docs_root / "high_intensity_risk_cap_threshold_selection.md").exists()
