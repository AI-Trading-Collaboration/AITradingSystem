from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_forward_ops_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_weekly_dashboard_summarizes_forward_progress(tmp_path) -> None:
    fixture = run_smoothed_forward_ops_chain_fixture(tmp_path)
    dashboard = fixture["dashboard"]

    summary = dashboard["smoothed_dashboard_summary"]
    assert summary["candidate_method"] == "smooth_weights_3d_limited_adjustment"
    assert summary["current_owner_decision"] == "continue_observation"
    assert summary["gate_decision"] == "ELIGIBLE_FOR_OWNER_APPROVAL"
    assert summary["decision_confidence"] == "LOW"
    assert summary["forward_confirmation_status"] == "IN_PROGRESS"
    assert summary["ready_for_switch_recheck"] is False
    assert summary["weekly_recommendation"] == "continue_observation"
    assert summary["broker_action_allowed"] is False
    assert summary["production_effect"] == "none"

    rows = {row["target_id"]: row for row in dashboard["smoothed_target_status_table"]["targets"]}
    assert rows["smooth_3d_vs_limited"]["status"] == "IN_PROGRESS"
    assert rows["smooth_3d_vs_limited"]["available_events"] == 0
    assert rows["smooth_3d_vs_limited"]["required_events"] == 10
    assert rows["smooth_3d_vs_limited"]["progress_pct"] == 0.0
    assert rows["smooth_3d_sideways_choppy_improvement"]["decision"] == (
        "wait_for_sideways_events"
    )
    assert rows["smooth_3d_recovery_lag_watch"]["status"] == "WATCH_ONLY"
    assert "Dynamic Rescue Smoothed Weekly Dashboard" in dashboard["reader_brief_section"]

    validation = system_target.validate_smoothed_weekly_dashboard_artifact(
        dashboard_id=dashboard["dashboard_id"],
        output_dir=tmp_path / "smoothed_weekly_dashboard",
    )
    assert validation["status"] == "PASS"
