from __future__ import annotations

from pathlib import Path

from high_intensity_event_logger_fixtures import (
    build_high_intensity_event_logger_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_high_intensity_event_logger_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "high-intensity-risk-cap-forward-observe-event-logger" in result.output


def test_high_intensity_event_logger_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_event_logger_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-forward-observe-event-logger",
            "--threshold-selection-dir",
            str(fixture["threshold_selection_dir"]),
            "--forward-observe-plan-dir",
            str(fixture["forward_observe_plan_dir"]),
            "--dynamic-dry-run-dir",
            str(fixture["dynamic_dry_run_dir"]),
            "--dynamic-diagnostics-dir",
            str(fixture["dynamic_diagnostics_dir"]),
            "--readiness-dir",
            str(fixture["readiness_dir"]),
            "--timestamp-remediation-dir",
            str(fixture["timestamp_remediation_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "observe_event_logger",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_event_logger_summary.json",
        "high_intensity_selected_rule_execution_report.json",
        "high_intensity_observe_trigger_day_log.json",
        "high_intensity_observe_event_log.json",
        "high_intensity_observe_event_cluster_registry.json",
        "high_intensity_monthly_concentration_report.json",
        "high_intensity_pending_outcome_registry.json",
        "high_intensity_outcome_collection_schedule.json",
        "high_intensity_manual_review_event_queue.json",
        "high_intensity_event_logger_data_quality_report.json",
        "high_intensity_event_logger_interpretation_boundary.json",
        "high_intensity_2337_readiness_checklist.json",
        "high_intensity_2337_task_route.json",
        "high_intensity_event_logger_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(output_dir / "high_intensity_event_logger_summary.json")
    assert summary["selected_rule_id"] == "COMPOSITE_HIGH_INTENSITY_RULE"
    assert summary["event_count_after_dedup"] > 0
    assert summary["outcome_binding_executed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert (
        summary["next_task"]
        == "TRADING-2337_High_Intensity_Risk_Cap_Actual_Path_Outcome_Binder"
    )
    assert (
        docs_root / "high_intensity_risk_cap_forward_observe_event_logger.md"
    ).exists()
