from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_wiring_plan_fixtures import (
    build_high_intensity_scheduler_wiring_plan_fixture,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_wiring_plan import (
    build_high_intensity_scheduler_config_entry_plan,
    load_high_intensity_scheduler_wiring_plan_inputs,
)


def test_scheduler_config_entry_plan_remains_disabled(tmp_path: Path) -> None:
    fixture = build_high_intensity_scheduler_wiring_plan_fixture(tmp_path)
    inputs = load_high_intensity_scheduler_wiring_plan_inputs(
        scheduler_dry_run_dir=fixture["scheduler_dry_run_dir"],
        scheduler_integration_plan_dir=fixture["scheduler_integration_plan_dir"],
        runtime_dry_run_dir=fixture["runtime_dry_run_dir"],
        runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
        continue_decision_dir=fixture["continue_decision_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
    )

    plan = build_high_intensity_scheduler_config_entry_plan(inputs)

    assert plan["scheduler_enabled"] is False
    assert plan["scheduler_default_enabled"] is False
    assert plan["manual_run_only"] is True
    assert plan["dry_run_default"] is True
    assert "paper_shadow" in plan["blocked_capabilities"]
    assert "production" in plan["blocked_capabilities"]
    assert "broker_action" in plan["blocked_capabilities"]
