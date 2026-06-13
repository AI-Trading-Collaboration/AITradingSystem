from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_forward_ops_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_switch_readiness_keeps_switch_execution_forbidden(tmp_path) -> None:
    fixture = run_smoothed_forward_ops_chain_fixture(tmp_path)
    recheck = fixture["recheck"]

    decision = recheck["switch_readiness_decision"]
    assert decision["candidate_method"] == "smooth_weights_3d_limited_adjustment"
    assert decision["current_owner_decision"] == "continue_observation"
    assert decision["previous_gate_decision"] == "ELIGIBLE_FOR_OWNER_APPROVAL"
    assert decision["recheck_decision"] == "WAIT_FOR_MORE_FORWARD_DATA"
    assert decision["decision_confidence"] == "LOW"
    assert decision["can_execute_switch"] is False
    assert decision["owner_decision_required"] is True
    assert decision["auto_switch"] is False
    assert decision["broker_action_allowed"] is False
    assert decision["production_effect"] == "none"

    criteria = {row["criterion"]: row for row in recheck["switch_readiness_criteria"]["criteria"]}
    assert criteria["smooth_3d_vs_limited_forward_events"]["status"] == "IN_PROGRESS"
    assert criteria["sideways_events"]["status"] == "IN_PROGRESS"
    assert criteria["recovery_lag_watch"]["actual"] == "INSUFFICIENT_EVENTS"
    assert criteria["recovery_lag_watch"]["status"] == "IN_PROGRESS"
    assert recheck["switch_readiness_criteria"]["hard_blockers"] == []
    assert "sideways_events" in recheck["switch_readiness_criteria"]["warnings"]
    assert "Dynamic Rescue Smoothed Switch Readiness" in recheck["reader_brief_section"]

    validation = system_target.validate_smoothed_switch_readiness_artifact(
        recheck_id=recheck["recheck_id"],
        output_dir=tmp_path / "smoothed_switch_readiness",
    )
    assert validation["status"] == "PASS"
