from __future__ import annotations

from pathlib import Path

from high_intensity_outcome_binder_fixtures import (
    build_high_intensity_outcome_binder_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_high_intensity_outcome_binder_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "high-intensity-risk-cap-actual-path-outcome-binder" in result.output


def test_high_intensity_outcome_binder_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_outcome_binder_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-actual-path-outcome-binder",
            "--event-logger-dir",
            str(fixture["event_logger_dir"]),
            "--threshold-selection-dir",
            str(fixture["threshold_selection_dir"]),
            "--forward-observe-plan-dir",
            str(fixture["forward_observe_plan_dir"]),
            "--dynamic-dry-run-dir",
            str(fixture["dynamic_dry_run_dir"]),
            "--dynamic-diagnostics-dir",
            str(fixture["dynamic_diagnostics_dir"]),
            "--market-data-source",
            str(fixture["prices_path"]),
            "--rates-path",
            str(fixture["rates_path"]),
            "--marketstack-prices-path",
            str(tmp_path / "missing_marketstack.csv"),
            "--quality-as-of",
            "2023-03-10",
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "actual_path_outcome_binder",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_outcome_binder_summary.json",
        "high_intensity_event_actual_path_outcome_matrix.json",
        "high_intensity_cluster_actual_path_outcome_matrix.json",
        "high_intensity_trigger_day_actual_path_context.json",
        "high_intensity_outcome_coverage_report.json",
        "high_intensity_false_warning_classification_report.json",
        "high_intensity_missed_upside_classification_report.json",
        "high_intensity_downside_capture_classification_report.json",
        "high_intensity_manual_review_usefulness_proxy_report.json",
        "high_intensity_actual_path_data_quality_report.json",
        "high_intensity_2339_task_route.json",
        "high_intensity_outcome_binder_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(output_dir / "high_intensity_outcome_binder_summary.json")
    assert summary["outcome_binding_executed"] is True
    assert summary["original_event_log_mutated"] is False
    assert summary["validate_data_executed"] is True
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert (docs_root / "high_intensity_risk_cap_actual_path_outcome_binder.md").exists()
