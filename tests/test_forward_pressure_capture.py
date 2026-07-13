from __future__ import annotations

import json
from pathlib import Path

from dynamic_v3_defensive_evidence_helpers import run_capture_plan_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_forward_pressure import (
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


def test_forward_pressure_capture_validation_rejects_policy_and_output_drift(tmp_path):
    fixture = run_capture_plan_fixture(tmp_path)
    plan = fixture["capture_plan"]
    artifact_dir = Path(plan["capture_plan_dir"])
    snapshot = json.loads(
        (artifact_dir / "capture_plan_input_snapshot.json").read_text(encoding="utf-8")
    )
    with Path(snapshot["policy_bindings"][0]["path"]).open("a", encoding="utf-8") as handle:
        handle.write("\n# unauthorized byte drift\n")
    (artifact_dir / "daily_command_pack.json").write_text("{}\n", encoding="utf-8")

    validation = validate_forward_pressure_capture_artifact(
        capture_plan_id=plan["capture_plan_id"],
        output_dir=fixture["capture_plan_dir"],
    )

    assert validation["status"] == "FAIL"
    assert validation["failed_check_count"] >= 1
