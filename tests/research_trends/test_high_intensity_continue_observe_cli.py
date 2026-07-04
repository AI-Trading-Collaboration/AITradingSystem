from __future__ import annotations

from pathlib import Path

from high_intensity_continue_observe_fixtures import (
    build_high_intensity_continue_observe_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_high_intensity_continue_observe_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "high-intensity-risk-cap-continue-forward-observe-decision" in result.output


def test_high_intensity_continue_observe_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_continue_observe_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-continue-forward-observe-decision",
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
            "continue_forward_observe_decision",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_continue_observe_decision_summary.json",
        "high_intensity_continue_observe_decision_matrix.json",
        "high_intensity_selected_rule_continuation_contract.json",
        "high_intensity_observe_continuation_scope.json",
        "high_intensity_partial_coverage_carryforward_caveat.json",
        "high_intensity_monthly_concentration_monitoring_plan.json",
        "high_intensity_event_logger_continuation_contract.json",
        "high_intensity_stop_refine_archive_policy.json",
        "high_intensity_2342_task_route.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(output_dir / "high_intensity_continue_observe_decision_summary.json")
    assert summary["runtime_scheduler_enabled"] is False
    assert summary["new_event_logging_executed"] is False
    assert summary["outcome_binding_executed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert (docs_root / "high_intensity_risk_cap_continue_forward_observe_decision.md").exists()
