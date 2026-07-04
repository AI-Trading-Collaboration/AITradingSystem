from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_dry_run_fixtures import (
    build_high_intensity_scheduler_dry_run_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_high_intensity_scheduler_dry_run_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "high-intensity-risk-cap-observe-only-scheduler-dry-run" in result.output


def test_high_intensity_scheduler_dry_run_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_scheduler_dry_run_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-scheduler-dry-run",
            "--scheduler-integration-plan-dir",
            str(fixture["scheduler_integration_plan_dir"]),
            "--runtime-dry-run-dir",
            str(fixture["runtime_dry_run_dir"]),
            "--runtime-integration-plan-dir",
            str(fixture["runtime_integration_plan_dir"]),
            "--continue-decision-dir",
            str(fixture["continue_decision_dir"]),
            "--event-logger-dir",
            str(fixture["event_logger_dir"]),
            "--threshold-selection-dir",
            str(fixture["threshold_selection_dir"]),
            "--forward-observe-plan-dir",
            str(fixture["forward_observe_plan_dir"]),
            "--dynamic-dry-run-dir",
            str(fixture["dynamic_dry_run_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "observe_only_scheduler_dry_run",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_scheduler_dry_run_summary.json",
        "high_intensity_scheduler_contract_validation_report.json",
        "high_intensity_scheduler_cycle_plan.json",
        "high_intensity_scheduler_job_dag_validation_report.json",
        "high_intensity_scheduler_event_detection_job_dry_run_result.json",
        "high_intensity_scheduler_event_append_job_dry_run_result.json",
        "high_intensity_scheduler_cluster_update_job_dry_run_result.json",
        "high_intensity_scheduler_pending_outcome_job_dry_run_result.json",
        "high_intensity_scheduler_disabled_policy_validation_report.json",
        "high_intensity_scheduler_fail_closed_safety_gate_result.json",
        "high_intensity_2346_task_route.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(output_dir / "high_intensity_scheduler_dry_run_summary.json")
    assert summary["scheduler_enabled"] is False
    assert summary["scheduler_default_enabled"] is False
    assert summary["event_append_executed"] is False
    assert summary["event_append_dry_run_executed"] is True
    assert summary["outcome_binding_executed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert (
        docs_root / "high_intensity_risk_cap_observe_only_scheduler_dry_run.md"
    ).exists()
