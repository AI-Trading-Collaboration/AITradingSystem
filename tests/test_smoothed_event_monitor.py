from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_forward_ops_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_event_monitor_outputs_sideways_and_recovery_summary(tmp_path) -> None:
    fixture = run_smoothed_forward_ops_chain_fixture(tmp_path)
    monitor = fixture["monitor"]

    summary = monitor["event_accumulation_summary"]
    assert summary["sideways_events"] == {
        "required": 5,
        "available": 0,
        "pending": 0,
        "progress_pct": 0.0,
    }
    assert summary["recovery_events"] == {
        "required": 5,
        "available": 0,
        "pending": 0,
        "progress_pct": 0.0,
    }
    assert summary["sideways_status"] == "INSUFFICIENT_EVENTS"
    assert summary["recovery_lag_status"] == "INSUFFICIENT_EVENTS"
    assert summary["lag_warning_count"] == 0
    assert summary["recommended_action"] == "continue_event_collection"
    assert monitor["sideways_event_inventory"] == []
    assert monitor["recovery_event_inventory"] == []

    validation = system_target.validate_smoothed_event_monitor_artifact(
        monitor_id=monitor["monitor_id"],
        output_dir=tmp_path / "smoothed_event_monitor",
    )
    assert validation["status"] == "PASS"
