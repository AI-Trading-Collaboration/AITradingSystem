from __future__ import annotations

from ai_trading_system.scope_narrowed_forward_observe_readiness_review import (
    build_forward_observe_stop_continue_rules,
)


def test_stop_continue_rules_cover_all_decision_buckets() -> None:
    rules = build_forward_observe_stop_continue_rules()

    assert rules["continue_observe_if"]
    assert rules["extend_observe_if"]
    assert rules["stop_observe_if"]
    assert rules["escalate_to_owner_precheck_if"]


def test_stop_continue_rules_do_not_allow_automatic_escalation_actions() -> None:
    rules = build_forward_observe_stop_continue_rules()

    forbidden = set(rules["forbidden_rules"])
    assert "auto_promotion" in forbidden
    assert "auto_paper_shadow" in forbidden
    assert "auto_broker_action" in forbidden
    assert rules["promotion_allowed"] is False
    assert rules["paper_shadow_allowed"] is False
    assert rules["production_allowed"] is False
    assert rules["broker_action"] == "none"
