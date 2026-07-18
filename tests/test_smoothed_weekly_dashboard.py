from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_forward_ops_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


@with_artifact_validation_session
def test_smoothed_weekly_dashboard_summarizes_forward_progress(tmp_path) -> None:
    fixture = run_smoothed_forward_ops_chain_fixture(tmp_path)
    dashboard = fixture["dashboard"]

    summary = dashboard["smoothed_dashboard_summary"]
    assert summary["candidate_method"] is None
    assert summary["current_owner_decision"] is None
    assert summary["gate_decision"] is None
    assert summary["decision_confidence"] is None
    assert summary["forward_confirmation_status"] == "NOT_REGISTERED"
    assert summary["ready_for_switch_recheck"] is False
    assert summary["weekly_recommendation"] == "request_more_forward_data"
    assert summary["broker_action_allowed"] is False
    assert summary["production_effect"] == "none"

    assert dashboard["smoothed_target_status_table"]["targets"] == []
    assert "Dynamic Rescue Smoothed Weekly Dashboard" in dashboard["reader_brief_section"]

    validation = system_target.validate_smoothed_weekly_dashboard_artifact(
        dashboard_id=dashboard["dashboard_id"],
        output_dir=tmp_path / "smoothed_weekly_dashboard",
    )
    assert validation["status"] == "PASS"
