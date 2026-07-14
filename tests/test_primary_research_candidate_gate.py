from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_promotion_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_promotion as smoothed_promotion,
)


@smoothed_promotion._with_validation_session
def test_primary_research_candidate_gate_keeps_owner_approval_required(tmp_path) -> None:
    fixture = run_smoothed_promotion_chain_fixture(tmp_path)
    gate = fixture["gate"]

    decision = gate["gate_decision"]
    assert decision["candidate_method"] is None
    assert decision["gate_scope"] == "paper_shadow_research_only"
    assert decision["gate_decision"] == "CONTINUE_OBSERVATION"
    assert decision["decision_confidence"] == "LOW"
    assert decision["owner_approval_required"] is True
    assert decision["auto_apply"] is False
    assert decision["can_update_paper_shadow_primary_candidate"] == "NOT_ELIGIBLE"
    assert decision["can_write_official_target_weights"] is False
    assert decision["broker_action_allowed"] is False
    assert decision["production_effect"] == "none"

    criteria = gate["gate_criteria_results"]
    by_criterion = {row["criterion"]: row for row in criteria["criteria"]}
    assert by_criterion["candidate_present"]["status"] == "FAIL"
    assert by_criterion["promotion_review_decision"]["status"] == "FAIL"
    assert by_criterion["forward_confirmation"]["status"] == "FAIL"
    assert "no_eligible_candidate" in criteria["hard_blockers"]
    assert "decision_confidence_low" in criteria["warnings"]

    validation = system_target.validate_primary_research_candidate_gate_artifact(
        gate_id=gate["gate_id"],
        output_dir=tmp_path / "primary_research_candidate_gate",
    )
    assert validation["status"] == "PASS"
