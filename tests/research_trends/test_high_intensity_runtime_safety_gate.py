from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_integration_plan import (
    evaluate_high_intensity_runtime_fail_closed_safety_gate,
)


def test_runtime_safety_gate_blocks_missing_selected_rule() -> None:
    result = evaluate_high_intensity_runtime_fail_closed_safety_gate(
        {"known_at_timestamp": "2026-06-29T00:00:00Z", "pit_policy": "PIT_APPROXIMATION_READY"}
    )

    assert result["blocked"] is True
    assert "MISSING_SELECTED_RULE" in result["blockers"]


def test_runtime_safety_gate_blocks_missing_known_at_timestamp() -> None:
    result = evaluate_high_intensity_runtime_fail_closed_safety_gate(
        {
            "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
            "pit_policy": "PIT_APPROXIMATION_READY",
        }
    )

    assert result["blocked"] is True
    assert "MISSING_KNOWN_AT_TIMESTAMP" in result["blockers"]


def test_runtime_safety_gate_blocks_target_weight_generated() -> None:
    result = evaluate_high_intensity_runtime_fail_closed_safety_gate(
        {
            "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
            "known_at_timestamp": "2026-06-29T00:00:00Z",
            "pit_policy": "PIT_APPROXIMATION_READY",
            "target_weight_generated": True,
        }
    )

    assert "TARGET_WEIGHT_GENERATED" in result["blockers"]


def test_runtime_safety_gate_blocks_broker_action_requested() -> None:
    result = evaluate_high_intensity_runtime_fail_closed_safety_gate(
        {
            "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
            "known_at_timestamp": "2026-06-29T00:00:00Z",
            "pit_policy": "PIT_APPROXIMATION_READY",
            "broker_action": "submit_order",
        }
    )

    assert "BROKER_ACTION_REQUESTED" in result["blockers"]


def test_runtime_safety_gate_blocks_paper_shadow_enabled() -> None:
    result = evaluate_high_intensity_runtime_fail_closed_safety_gate(
        {
            "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
            "known_at_timestamp": "2026-06-29T00:00:00Z",
            "pit_policy": "PIT_APPROXIMATION_READY",
            "paper_shadow_enabled": True,
        }
    )

    assert "PAPER_SHADOW_ENABLED" in result["blockers"]
