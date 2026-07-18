from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_forward_ops_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


@with_artifact_validation_session
def test_smoothed_switch_readiness_keeps_switch_execution_forbidden(tmp_path) -> None:
    fixture = run_smoothed_forward_ops_chain_fixture(tmp_path)
    recheck = fixture["recheck"]

    decision = recheck["switch_readiness_decision"]
    assert decision["candidate_method"] is None
    assert decision["current_owner_decision"] is None
    assert decision["previous_gate_decision"] is None
    assert decision["recheck_decision"] == "NO_ELIGIBLE_CANDIDATE"
    assert decision["decision_confidence"] is None
    assert decision["can_execute_switch"] is False
    assert decision["owner_decision_required"] is False
    assert decision["auto_switch"] is False
    assert decision["broker_action_allowed"] is False
    assert decision["production_effect"] == "none"

    assert recheck["switch_readiness_criteria"]["criteria"] == []
    assert recheck["switch_readiness_criteria"]["hard_blockers"] == [
        "no_eligible_candidate"
    ]
    assert recheck["switch_readiness_criteria"]["warnings"] == []
    assert "Dynamic Rescue Smoothed Switch Readiness" in recheck["reader_brief_section"]

    validation = system_target.validate_smoothed_switch_readiness_artifact(
        recheck_id=recheck["recheck_id"],
        output_dir=tmp_path / "smoothed_switch_readiness",
    )
    assert validation["status"] == "PASS"
