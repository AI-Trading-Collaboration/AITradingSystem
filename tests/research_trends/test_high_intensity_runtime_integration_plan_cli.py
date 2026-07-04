from __future__ import annotations

from pathlib import Path

from high_intensity_runtime_integration_plan_fixtures import (
    build_high_intensity_runtime_integration_plan_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_high_intensity_runtime_integration_plan_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "high-intensity-risk-cap-observe-only-runtime-integration-plan" in result.output


def test_high_intensity_runtime_integration_plan_cli_writes_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_runtime_integration_plan_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-runtime-integration-plan",
            "--continue-decision-dir",
            str(fixture["continue_decision_dir"]),
            "--forward-outcome-review-dir",
            str(fixture["forward_outcome_review_dir"]),
            "--partial-readiness-dir",
            str(fixture["partial_readiness_dir"]),
            "--outcome-binder-dir",
            str(fixture["outcome_binder_dir"]),
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
            "observe_only_runtime_integration_plan",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_runtime_integration_plan_summary.json",
        "high_intensity_runtime_scope_contract.json",
        "high_intensity_runtime_input_contract.json",
        "high_intensity_runtime_event_detection_contract.json",
        "high_intensity_runtime_event_append_contract.json",
        "high_intensity_runtime_cluster_update_contract.json",
        "high_intensity_runtime_pending_outcome_update_contract.json",
        "high_intensity_runtime_outcome_update_job_plan.json",
        "high_intensity_runtime_manual_review_context_contract.json",
        "high_intensity_runtime_fail_closed_safety_gate.json",
        "high_intensity_2343_task_route.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(output_dir / "high_intensity_runtime_integration_plan_summary.json")
    assert summary["runtime_scheduler_enabled"] is False
    assert summary["new_event_logging_executed"] is False
    assert summary["outcome_binding_executed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert (
        docs_root / "high_intensity_risk_cap_observe_only_runtime_integration_plan.md"
    ).exists()
