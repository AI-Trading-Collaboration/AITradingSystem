from __future__ import annotations

from pathlib import Path

from high_intensity_forward_outcome_review_fixtures import (
    build_high_intensity_forward_outcome_review_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_high_intensity_forward_outcome_review_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "high-intensity-risk-cap-forward-outcome-review" in result.output


def test_high_intensity_forward_outcome_review_cli_writes_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_forward_outcome_review_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-forward-outcome-review",
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
            "forward_outcome_review_with_partial_coverage_caveat",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_forward_outcome_review_summary.json",
        "high_intensity_cluster_outcome_review_matrix.json",
        "high_intensity_horizon_outcome_review_matrix.json",
        "high_intensity_false_warning_review.json",
        "high_intensity_missed_upside_review.json",
        "high_intensity_downside_capture_review.json",
        "high_intensity_manual_review_usefulness_review.json",
        "high_intensity_continue_refine_archive_decision_matrix.json",
        "high_intensity_2341_task_route.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(output_dir / "high_intensity_forward_outcome_review_summary.json")
    assert summary["outcome_binding_executed"] is False
    assert summary["original_event_log_mutated"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert (docs_root / "high_intensity_risk_cap_forward_outcome_review.md").exists()
