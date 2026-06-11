from __future__ import annotations

from dynamic_v3_defensive_evidence_helpers import run_capture_plan_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_defensive_evidence import (
    validate_forward_pressure_capture_artifact,
)


def test_forward_pressure_capture_plan_contains_cadence_and_event_commands(tmp_path):
    fixture = run_capture_plan_fixture(tmp_path)
    plan = fixture["capture_plan"]

    assert "shadow-monitor run" in plan["daily_command_pack"]["commands"]
    assert "pressure-sample-ledger update" in plan["weekly_command_pack"]["commands"]
    assert "pressure-outcome-backfill run" in plan["event_driven_trigger_plan"]["commands"]
    assert plan["event_driven_trigger_plan"]["broker_action_allowed"] is False
    assert plan["event_driven_trigger_plan"]["auto_apply_policy"] is False
    assert plan["event_driven_trigger_plan"]["production_effect"] == "none"

    validation = validate_forward_pressure_capture_artifact(
        capture_plan_id=plan["capture_plan_id"],
        output_dir=fixture["capture_plan_dir"],
    )

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0
