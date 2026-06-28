from __future__ import annotations

from ai_trading_system.two_layer_error_attribution import (
    attribute_two_layer_errors,
    summarize_two_layer_errors,
)


def test_false_risk_off_and_false_add_risk_attribution() -> None:
    result = attribute_two_layer_errors(
        {
            "risk_off_signal": True,
            "missed_upside": 0.03,
            "false_risk_off_cost": 0.02,
            "add_risk_signal": True,
            "false_add_risk_cost": 0.01,
            "defensive_probe_regression_count": 2,
        }
    )

    assert "false_risk_off" in result["error_types"]
    assert "false_add_risk" in result["error_types"]
    assert result["promotion_allowed"] is False


def test_missed_risk_off_late_re_risk_and_beta_only_summary() -> None:
    summary = summarize_two_layer_errors(
        [
            {
                "risk_off_signal": False,
                "avoidable_drawdown": 0.04,
                "late_risk_off_cost": 0.02,
            },
            {
                "risk_off_signal": False,
                "re_risk_delay": 3,
                "missed_recovery_upside": 0.05,
                "same_risk_static_frontier_delta": -0.01,
                "QQQ_equivalent_exposure_delta": 0.10,
                "TQQQ_beta_dependency": True,
                "2023_plus_dependency": True,
            },
        ]
    )

    assert summary["error_type_counts"]["missed_risk_off"] == 1
    assert summary["error_type_counts"]["late_re_risk"] == 1
    assert summary["error_type_counts"]["beta_only_improvement"] == 1
    assert summary["broker_action"] == "none"
