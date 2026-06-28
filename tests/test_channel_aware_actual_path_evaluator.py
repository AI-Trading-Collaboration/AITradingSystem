from __future__ import annotations

from ai_trading_system.channel_aware_actual_path_evaluator import (
    evaluate_channel_aware_actual_path,
)


def test_channel_aware_actual_path_evaluator_groups_required_metrics() -> None:
    result = evaluate_channel_aware_actual_path(
        [
            {
                "channel": "defensive",
                "false_risk_off_cost_delta": -0.02,
                "missed_risk_off_cost_delta": 0.0,
                "defensive_probe_regression_count": 0,
            },
            {
                "channel": "return_seeking_diagnostic",
                "captured_upside": 0.05,
                "false_add_risk_cost": 0.03,
                "diagnostic_only": True,
                "TQQQ_beta_dependency": True,
            },
            {
                "channel": "risk_veto",
                "blocked_false_add_risk_count": 2,
                "missed_verified_add_risk_count": 1,
            },
            {
                "channel": "second_layer_policy",
                "actual_path_return_delta": 0.01,
                "max_drawdown_delta": -0.02,
                "turnover": 0.1,
                "same_risk_static_frontier_delta": -0.01,
                "QQQ_equivalent_exposure_delta": 0.05,
            },
        ]
    )

    assert result["status"] == "CHANNEL_AWARE_ACTUAL_PATH_EVALUATION_READY"
    assert result["defensive_channel_metrics"]["row_count"] == 1
    assert result["return_seeking_diagnostic_metrics"]["diagnostic_only"] is True
    assert result["risk_veto_metrics"]["blocked_false_add_risk_count"] == 2
    assert result["second_layer_actual_path_metrics"]["actual_path_return_delta"] == 0.01
    assert result["same_risk_frontier_comparison"]["beta_only_dependency_flag"] is True
    assert result["promotion_allowed"] is False
