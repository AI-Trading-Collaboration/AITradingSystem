from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_plan_fixtures import (
    build_high_intensity_scheduler_plan_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_high_intensity_scheduler_plan_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert (
        "high-intensity-risk-cap-observe-only-runtime-scheduler-integration-plan"
        in result.output
    )


def test_high_intensity_scheduler_plan_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_scheduler_plan_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-runtime-scheduler-integration-plan",
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
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "observe_only_scheduler_integration_plan",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_scheduler_integration_plan_summary.json",
        "high_intensity_scheduler_scope_contract.json",
        "high_intensity_scheduler_cadence_plan.json",
        "high_intensity_scheduler_input_contract.json",
        "high_intensity_scheduler_event_detection_job_contract.json",
        "high_intensity_scheduler_event_append_job_contract.json",
        "high_intensity_scheduler_cluster_update_job_contract.json",
        "high_intensity_scheduler_pending_outcome_update_job_contract.json",
        "high_intensity_scheduler_outcome_update_job_contract.json",
        "high_intensity_scheduler_manual_review_context_contract.json",
        "high_intensity_scheduler_monthly_concentration_monitoring_contract.json",
        "high_intensity_scheduler_artifact_path_plan.json",
        "high_intensity_scheduler_registry_update_plan.json",
        "high_intensity_scheduler_fail_closed_safety_gate.json",
        "high_intensity_scheduler_disabled_by_default_policy.json",
        "high_intensity_scheduler_dry_run_execution_plan.json",
        "high_intensity_scheduler_failure_mode_matrix.json",
        "high_intensity_scheduler_integration_risk_register.json",
        "high_intensity_2345_readiness_checklist.json",
        "high_intensity_2345_task_route.json",
        "high_intensity_scheduler_integration_interpretation_boundary.json",
        "high_intensity_scheduler_integration_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(
        output_dir / "high_intensity_scheduler_integration_plan_summary.json"
    )
    assert summary["scheduler_enabled"] is False
    assert summary["scheduler_default_enabled"] is False
    assert summary["event_append_executed"] is False
    assert summary["outcome_binding_executed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert summary["next_task"] == (
        "TRADING-2345_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Dry_Run"
    )
    assert (
        docs_root
        / "high_intensity_risk_cap_observe_only_runtime_scheduler_integration_plan.md"
    ).exists()
    assert (docs_root / "high_intensity_scheduler_fail_closed_safety_gate.md").exists()
