from __future__ import annotations

from dynamic_v3_defensive_evidence_helpers import run_pressure_trigger_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_defensive_evidence import (
    validate_pressure_trigger_artifact,
)


def test_pressure_trigger_scan_distinguishes_quiet_and_pressure_windows(tmp_path):
    quiet = run_pressure_trigger_fixture(tmp_path / "quiet", triggered=False)
    stressed = run_pressure_trigger_fixture(tmp_path / "stressed", triggered=True)

    quiet_trigger = quiet["pressure_trigger"]
    stressed_trigger = stressed["pressure_trigger"]

    assert quiet_trigger["trigger_metrics"]["trigger_status"] == "NO_TRIGGER"
    assert quiet_trigger["triggered_actions"]["event_driven_capture_required"] is False
    assert stressed_trigger["trigger_metrics"]["trigger_status"] == "PRESSURE_TRIGGERED"
    assert stressed_trigger["triggered_actions"]["event_driven_capture_required"] is True
    assert stressed_trigger["manifest"]["broker_action_allowed"] is False

    for fixture, trigger in ((quiet, quiet_trigger), (stressed, stressed_trigger)):
        validation = validate_pressure_trigger_artifact(
            trigger_id=trigger["trigger_id"],
            output_dir=fixture["pressure_trigger_dir"],
        )
        assert validation["status"] == "PASS"
        assert validation["failed_check_count"] == 0
