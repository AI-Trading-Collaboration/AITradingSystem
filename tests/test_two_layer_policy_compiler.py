from __future__ import annotations

import pytest

from ai_trading_system.two_layer_policy_compiler import (
    compile_two_layer_policy,
    load_base_overlay_veto_policy,
    load_signal_usage_matrix_v2,
)


def test_compiler_applies_defensive_overlay_long_only_sum_to_one() -> None:
    policy = load_base_overlay_veto_policy()
    matrix = load_signal_usage_matrix_v2()

    result = compile_two_layer_policy(policy, {"risk_off": True}, matrix)

    assert result["status"] == "TWO_LAYER_POLICY_COMPILED_RESEARCH_ONLY"
    assert result["target_weights"] == {"QQQ": 0.3, "SGOV": 0.7, "TQQQ": 0.0}
    assert result["long_only"] is True
    assert result["sum_to_one"] is True
    assert "defensive_overlay:risk_off" in result["applied_overlays"]
    assert result["promotion_allowed"] is False


def test_compiler_blocks_diagnostic_add_risk_from_growth_overlay() -> None:
    policy = load_base_overlay_veto_policy()
    matrix = load_signal_usage_matrix_v2()

    result = compile_two_layer_policy(policy, {"add_risk": True}, matrix)

    assert result["target_weights"] == {"QQQ": 0.6, "SGOV": 0.4, "TQQQ": 0.0}
    assert result["applied_overlays"] == []
    assert result["blocked_actions"][0]["signal_name"] == "add_risk"
    assert result["blocked_actions"][0]["reason"] == "usage_contract"


def test_risk_veto_blocks_future_growth_overlay_even_if_usage_allows() -> None:
    policy = load_base_overlay_veto_policy()
    usage_matrix = _usage_matrix_for_future_growth()

    result = compile_two_layer_policy(
        policy,
        {"add_risk": True, "risk_off_veto": True, "growth_allowed": True},
        usage_matrix,
    )

    assert result["veto_active"] is True
    assert result["target_weights"] == {"QQQ": 0.6, "SGOV": 0.4, "TQQQ": 0.0}
    assert result["blocked_actions"][0]["reason"] == "risk_veto"


def test_compiler_can_apply_future_growth_overlay_when_contract_allows() -> None:
    policy = load_base_overlay_veto_policy()
    usage_matrix = _usage_matrix_for_future_growth()

    result = compile_two_layer_policy(
        policy,
        {"add_risk": True, "growth_allowed": True, "tqqq_allowed": True},
        usage_matrix,
    )

    assert result["target_weights"] == pytest.approx({"QQQ": 0.7, "SGOV": 0.3, "TQQQ": 0.0})
    assert "growth_overlay:add_risk" in result["applied_overlays"]


def test_compiler_enforces_tqqq_and_qqq_equivalent_caps() -> None:
    policy = load_base_overlay_veto_policy()
    usage_matrix = _usage_matrix_for_future_growth()

    result = compile_two_layer_policy(
        policy,
        {"risk_on_diagnostic": True, "growth_allowed": True, "tqqq_allowed": True},
        usage_matrix,
    )
    weights = result["target_weights"]

    assert weights["TQQQ"] <= 0.05
    assert weights["QQQ"] + 3 * weights["TQQQ"] <= 0.7500000001
    assert any(row["event"] == "qqq_equivalent_cap_applied" for row in result["audit_trace"])


def test_compiler_rejects_raw_indicator_inputs() -> None:
    policy = load_base_overlay_veto_policy()

    with pytest.raises(ValueError, match="raw indicators"):
        compile_two_layer_policy(policy, {"QQQ_momentum": 0.5}, {})


def _usage_matrix_for_future_growth() -> dict[str, object]:
    return {
        "signals": [
            {
                "signal_name": "add_risk",
                "allowed_usage": ["growth_overlay"],
                "blocked_usage": [],
                "diagnostic_only": False,
            },
            {
                "signal_name": "risk_on_diagnostic",
                "allowed_usage": ["growth_overlay"],
                "blocked_usage": [],
                "diagnostic_only": False,
            },
        ]
    }
