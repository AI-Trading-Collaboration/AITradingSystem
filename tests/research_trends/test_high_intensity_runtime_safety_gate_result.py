from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_dry_run import (
    build_high_intensity_runtime_fail_closed_safety_gate_result,
)


def _candidate() -> dict[str, object]:
    return {
        "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
        "as_of_timestamp": "2026-06-30T09:00:00+09:00",
        "pit_policy": "PIT_APPROXIMATION_READY",
        "broker_action": "none",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
    }


def test_safety_gate_passes_for_observe_only_candidate() -> None:
    result = build_high_intensity_runtime_fail_closed_safety_gate_result(_candidate())

    assert result["safety_gate_status"] == "PASS"
    assert result["promotion_allowed"] is False


def test_safety_gate_blocks_missing_selected_rule() -> None:
    candidate = _candidate()
    candidate["selected_rule_id"] = ""

    result = build_high_intensity_runtime_fail_closed_safety_gate_result(candidate)

    assert result["missing_selected_rule_blocked"] is True
    assert result["safety_gate_status"] == "FAIL_CLOSED_TRIGGERED"


def test_safety_gate_blocks_missing_pit_policy() -> None:
    candidate = _candidate()
    candidate["pit_policy"] = ""

    result = build_high_intensity_runtime_fail_closed_safety_gate_result(candidate)

    assert result["missing_pit_policy_blocked"] is True


def test_safety_gate_blocks_trade_related_outputs() -> None:
    for key in [
        "target_weight_generated",
        "rebalance_instruction_generated",
        "paper_shadow_enabled",
    ]:
        candidate = _candidate()
        candidate[key] = True
        result = build_high_intensity_runtime_fail_closed_safety_gate_result(candidate)
        assert result["safety_gate_status"] == "FAIL_CLOSED_TRIGGERED"


def test_safety_gate_blocks_broker_action() -> None:
    candidate = _candidate()
    candidate["broker_action"] = "submit_order"

    result = build_high_intensity_runtime_fail_closed_safety_gate_result(candidate)

    assert result["broker_action_requested_blocked"] is True
