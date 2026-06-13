from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_promotion_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_paper_shadow_primary_switch_plan_is_non_applying_and_safe(tmp_path) -> None:
    fixture = run_smoothed_promotion_chain_fixture(tmp_path)
    switch = fixture["switch_plan"]

    plan = switch["primary_switch_plan"]
    assert plan["switch_scope"] == "paper_shadow_research_only"
    assert plan["current_primary_research_candidate"] == "limited_adjustment"
    assert (
        plan["proposed_primary_research_candidate"]
        == "smooth_weights_3d_limited_adjustment"
    )
    assert plan["switch_decision"] == "OWNER_DECISION_REQUIRED"
    assert plan["auto_switch"] is False
    assert plan["requires_owner_decision"] is True
    assert plan["requires_forward_confirmation"] is True
    assert plan["rollback_method"] == "limited_adjustment"
    assert "paper_shadow_reports" in plan["effective_only_for"]

    safety = switch["primary_switch_safety_checks"]
    assert safety["status"] == "PASS"
    assert safety["safety_checks"]["not_official_target_weights"] is True
    assert safety["safety_checks"]["does_not_modify_real_portfolio"] is True
    assert safety["safety_checks"]["does_not_generate_order_ticket"] is True
    assert safety["safety_checks"]["broker_action_allowed"] is False
    assert safety["safety_checks"]["production_effect"] == "none"
    assert "Dynamic Rescue Paper Shadow Primary Switch Plan" in switch["reader_brief_section"]

    validation = system_target.validate_paper_shadow_primary_switch_artifact(
        switch_plan_id=switch["switch_plan_id"],
        output_dir=tmp_path / "paper_shadow_primary_switch",
    )
    assert validation["status"] == "PASS"
