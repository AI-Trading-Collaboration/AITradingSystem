from __future__ import annotations

from pathlib import Path

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    DEFAULT_CONFIRMATION_CYCLE_SCHEDULE_CONFIG_PATH,
    build_confirmation_cycle_plan,
    confirmation_cycle_plan_report_payload,
    validate_confirmation_cycle_schedule_config,
)


def test_confirmation_cycle_plan_generates_safe_command_pack(tmp_path: Path) -> None:
    validation = validate_confirmation_cycle_schedule_config(
        config_path=DEFAULT_CONFIRMATION_CYCLE_SCHEDULE_CONFIG_PATH
    )
    assert validation["status"] == "PASS"

    result = build_confirmation_cycle_plan(
        config_path=DEFAULT_CONFIRMATION_CYCLE_SCHEDULE_CONFIG_PATH,
        output_dir=tmp_path / "confirmation_cycle_plan",
    )
    pack = result["scheduled_command_pack"]
    commands = {row["step"]: row for row in pack["commands"]}

    assert result["manifest"]["status"] == "PASS"
    assert "outcome_due_scan" in commands
    assert "outcome_update_if_ready" in commands
    assert commands["outcome_update_if_ready"]["execution_mode"] == "explicit_update_only"
    assert pack["safety"]["broker_action_allowed"] is False
    assert pack["safety"]["production_effect"] == "none"
    assert pack["safety"]["auto_apply_policy"] is False

    payload = confirmation_cycle_plan_report_payload(
        plan_id=result["plan_id"],
        output_dir=tmp_path / "confirmation_cycle_plan",
    )
    assert payload["plan_id"] == result["plan_id"]
    assert "confirmation_cycle_runbook.md" in payload["confirmation_cycle_runbook_path"]
