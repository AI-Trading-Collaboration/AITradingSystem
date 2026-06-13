from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_promotion_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_primary_research_candidate_gate_keeps_owner_approval_required(tmp_path) -> None:
    fixture = run_smoothed_promotion_chain_fixture(tmp_path)
    gate = fixture["gate"]

    decision = gate["gate_decision"]
    assert decision["candidate_method"] == "smooth_weights_3d_limited_adjustment"
    assert decision["gate_scope"] == "paper_shadow_research_only"
    assert decision["gate_decision"] == "ELIGIBLE_FOR_OWNER_APPROVAL"
    assert decision["decision_confidence"] == "LOW"
    assert decision["owner_approval_required"] is True
    assert decision["auto_apply"] is False
    assert (
        decision["can_update_paper_shadow_primary_candidate"]
        == "OWNER_DECISION_REQUIRED"
    )
    assert decision["can_write_official_target_weights"] is False
    assert decision["broker_action_allowed"] is False
    assert decision["production_effect"] == "none"

    criteria = gate["gate_criteria_results"]
    by_criterion = {row["criterion"]: row for row in criteria["criteria"]}
    assert by_criterion["promotion_review_decision"]["status"] == "PASS"
    assert by_criterion["forward_confirmation"]["status"] == "PASS_WITH_WARNINGS"
    assert criteria["hard_blockers"] == []
    assert "forward_confirmation_in_progress" in criteria["warnings"]

    validation = system_target.validate_primary_research_candidate_gate_artifact(
        gate_id=gate["gate_id"],
        output_dir=tmp_path / "primary_research_candidate_gate",
    )
    assert validation["status"] == "PASS"
