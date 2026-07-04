from __future__ import annotations

from pathlib import Path

from high_intensity_continue_observe_fixtures import build_high_intensity_continue_observe_fixture

from ai_trading_system.high_intensity_risk_cap_continue_forward_observe_decision import (
    build_high_intensity_selected_rule_continuation_contract,
    load_high_intensity_continue_observe_decision_inputs,
)


def test_selected_rule_continuation_contract_preserves_hash_and_blocks_trading(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_continue_observe_fixture(tmp_path)
    inputs = load_high_intensity_continue_observe_decision_inputs(
        forward_outcome_review_dir=fixture["forward_outcome_review_dir"],
        partial_readiness_dir=fixture["partial_readiness_dir"],
        outcome_binder_dir=fixture["outcome_binder_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
    )

    contract = build_high_intensity_selected_rule_continuation_contract(inputs)

    assert contract["selected_rule_hash"] == (
        inputs["threshold_selection"]["selected_contract"]["selected_rule_hash"]
    )
    assert contract["rule_continued"] is True
    assert contract["rule_changed"] is False
    assert contract["rule_change_allowed_in_2341"] is False
    assert "automatic_exposure_cap" in contract["blocked_usage"]
    assert "target_weight_action" in contract["blocked_usage"]
    assert "paper_shadow" in contract["blocked_usage"]
    assert "broker_action" in contract["blocked_usage"]
