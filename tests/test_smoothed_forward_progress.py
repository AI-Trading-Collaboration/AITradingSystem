from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_forward_ops_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_forward_progress_tracks_targets_and_remains_not_ready(tmp_path) -> None:
    fixture = run_smoothed_forward_ops_chain_fixture(tmp_path)
    progress = fixture["progress"]

    summary = progress["smoothed_forward_progress_summary"]
    assert summary["binding_id"] == fixture["binding"]["binding_id"]
    assert summary["available_forward_events_total"] == 0
    assert summary["required_forward_events_total"] == 10
    assert summary["available_sideways_events"] == 0
    assert summary["required_sideways_events"] == 5
    assert summary["available_recovery_events"] == 0
    assert summary["required_recovery_events"] == 5
    assert summary["summary_recommendation"] == "continue_observation"

    rows = {row["target_id"]: row for row in progress["smoothed_target_progress"]}
    assert rows["smooth_3d_vs_limited"]["progress_status"] == "INSUFFICIENT_EVENTS"
    assert rows["smooth_3d_vs_limited"]["available_by_window"] == {
        "1": 0,
        "5": 0,
        "10": 0,
        "20": 0,
    }
    assert rows["smooth_3d_sideways_choppy_improvement"]["progress_status"] == (
        "INSUFFICIENT_EVENTS"
    )
    assert rows["smooth_3d_recovery_lag_watch"]["watch_only"] is True
    assert all(row["progress_status"] != "READY_FOR_REVIEW" for row in rows.values())
    assert all(row["broker_action_allowed"] is False for row in rows.values())
    assert "Dynamic Rescue Smoothed Forward Progress" in progress["reader_brief_section"]

    validation = system_target.validate_smoothed_forward_progress_artifact(
        progress_id=progress["progress_id"],
        output_dir=tmp_path / "smoothed_forward_progress",
    )
    assert validation["status"] == "PASS"
