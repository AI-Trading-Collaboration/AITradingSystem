from __future__ import annotations

import pytest

from ai_trading_system.high_intensity_risk_cap_scheduler_integration_plan import (
    build_high_intensity_scheduler_fail_closed_safety_gate,
)


def test_scheduler_fail_closed_safety_gate_passes_clean_candidate() -> None:
    gate = build_high_intensity_scheduler_fail_closed_safety_gate(
        {"scheduler_enabled": False, "broker_action": "none"}
    )

    assert gate["safety_gate_status"] == "PASS"
    assert gate["safety_error_count"] == 0
    assert gate["scheduler_enabled"] is False
    assert gate["promotion_allowed"] is False
    assert gate["paper_shadow_allowed"] is False
    assert gate["production_allowed"] is False
    assert gate["broker_action"] == "none"


@pytest.mark.parametrize(
    ("candidate", "blocked_key"),
    [
        ({"scheduler_enabled": True}, "scheduler_enabled_blocked"),
        ({"scheduler_default_enabled": True}, "scheduler_default_enabled_blocked"),
        ({"target_weight": 0.25}, "target_weight_generated_blocked"),
        ({"target_weight_generated": True}, "target_weight_generated_blocked"),
        ({"rebalance_instruction": "reduce"}, "rebalance_instruction_generated_blocked"),
        ({"rebalance_instruction_generated": True}, "rebalance_instruction_generated_blocked"),
        ({"broker_action": "submit_order"}, "broker_action_requested_blocked"),
        ({"broker_action_requested": True}, "broker_action_requested_blocked"),
        ({"paper_shadow_allowed": True}, "paper_shadow_enabled_blocked"),
        ({"production_allowed": True}, "production_enabled_blocked"),
    ],
)
def test_scheduler_fail_closed_safety_gate_blocks_unsafe_candidates(
    candidate: dict[str, object],
    blocked_key: str,
) -> None:
    gate = build_high_intensity_scheduler_fail_closed_safety_gate(candidate)

    assert gate["safety_gate_status"] == "FAIL"
    assert gate["safety_error_count"] >= 1
    assert gate[blocked_key] is True
    assert gate["next_task_if_failed"] == (
        "TRADING-2345_High_Intensity_Risk_Cap_Scheduler_Safety_Remediation"
    )
    assert gate["broker_action"] == "none"
