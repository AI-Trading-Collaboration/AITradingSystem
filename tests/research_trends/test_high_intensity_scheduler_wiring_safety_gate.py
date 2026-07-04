from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_scheduler_wiring_plan import (
    build_high_intensity_scheduler_wiring_safety_gate,
)


def test_scheduler_wiring_safety_gate_blocks_scheduler_enabled() -> None:
    gate = build_high_intensity_scheduler_wiring_safety_gate(
        config_entry_plan={"scheduler_enabled": True}
    )

    assert gate["safety_gate_status"] == "FAIL_CLOSED_TRIGGERED"
    assert "scheduler_enabled" in gate["unsafe_field_violations"][0]


def test_scheduler_wiring_safety_gate_blocks_target_weight() -> None:
    gate = build_high_intensity_scheduler_wiring_safety_gate(
        config_entry_plan={"target_weight": {"QQQ": 1.0}}
    )

    assert gate["safety_gate_status"] == "FAIL_CLOSED_TRIGGERED"


def test_scheduler_wiring_safety_gate_blocks_rebalance_instruction() -> None:
    gate = build_high_intensity_scheduler_wiring_safety_gate(
        config_entry_plan={"rebalance_instruction": "SELL_QQQ"}
    )

    assert gate["safety_gate_status"] == "FAIL_CLOSED_TRIGGERED"


def test_scheduler_wiring_safety_gate_blocks_broker_action() -> None:
    gate = build_high_intensity_scheduler_wiring_safety_gate(
        config_entry_plan={"broker_action": "BUY"}
    )

    assert gate["safety_gate_status"] == "FAIL_CLOSED_TRIGGERED"


def test_scheduler_wiring_safety_gate_blocks_manual_review_trade_instruction() -> None:
    gate = build_high_intensity_scheduler_wiring_safety_gate(
        manual_review_context={"manual_review_trade_instruction": "reduce QQQ"}
    )

    assert gate["safety_gate_status"] == "FAIL_CLOSED_TRIGGERED"
    assert gate["manual_review_instruction_violations"]
