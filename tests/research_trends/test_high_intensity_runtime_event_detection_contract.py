from __future__ import annotations

from pathlib import Path

from high_intensity_runtime_integration_plan_fixtures import (
    build_high_intensity_runtime_integration_plan_fixture,
)

from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_integration_plan import (
    build_high_intensity_runtime_event_detection_contract,
    load_high_intensity_runtime_integration_plan_inputs,
)


def test_runtime_event_detection_contract_blocks_trading_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_runtime_integration_plan_fixture(tmp_path)
    inputs = load_high_intensity_runtime_integration_plan_inputs(
        continue_decision_dir=fixture["continue_decision_dir"],
        forward_outcome_review_dir=fixture["forward_outcome_review_dir"],
        partial_readiness_dir=fixture["partial_readiness_dir"],
        outcome_binder_dir=fixture["outcome_binder_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
    )

    contract = build_high_intensity_runtime_event_detection_contract(inputs)

    assert "risk_cap_triggered" in contract["required_fields"]
    assert "decision_timestamp" in contract["required_fields"]
    assert contract["boolean_expression"]
    assert contract["known_at_policy"] == "NEXT_SESSION_DECISION_POLICY"
    assert contract["strict_pit_ready"] is False
    assert "target_weight" in contract["blocked_outputs"]
    assert "rebalance_instruction" in contract["blocked_outputs"]
    assert contract["promotion_allowed"] is False
    assert contract["broker_action"] == "none"
