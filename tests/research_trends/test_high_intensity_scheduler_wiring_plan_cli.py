from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_wiring_plan_fixtures import (
    build_high_intensity_scheduler_wiring_plan_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_high_intensity_scheduler_wiring_plan_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert (
        "high-intensity-risk-cap-observe-only-scheduler-wiring-plan"
        in result.output
    )


def test_high_intensity_scheduler_wiring_plan_cli_writes_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_wiring_plan_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-scheduler-wiring-plan",
            "--scheduler-dry-run-dir",
            str(fixture["scheduler_dry_run_dir"]),
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
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "observe_only_scheduler_wiring_plan",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_scheduler_wiring_plan_summary.json",
        "high_intensity_scheduler_config_entry_plan.json",
        "high_intensity_scheduler_disabled_wiring_policy.json",
        "high_intensity_scheduler_manual_run_contract.json",
        "high_intensity_scheduler_dry_run_only_mode_contract.json",
        "high_intensity_scheduler_job_wiring_contract.json",
        "high_intensity_scheduler_wiring_safety_gate.json",
        "high_intensity_2347_task_route.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(output_dir / "high_intensity_scheduler_wiring_plan_summary.json")
    dry_run_contract = read_json(
        output_dir / "high_intensity_scheduler_dry_run_only_mode_contract.json"
    )
    assert summary["scheduler_enabled"] is False
    assert summary["scheduler_default_enabled"] is False
    assert summary["manual_run_only"] is True
    assert summary["dry_run_only"] is True
    assert summary["event_append_executed"] is False
    assert summary["outcome_binding_executed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert dry_run_contract["dry_run_only_mode_required"] is True
    assert (
        docs_root / "high_intensity_risk_cap_observe_only_scheduler_wiring_plan.md"
    ).exists()
