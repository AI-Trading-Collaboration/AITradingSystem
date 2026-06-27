from __future__ import annotations

import pandas as pd

from ai_trading_system.execution_semantics import (
    _actual_position_path,
    _attach_path_return_columns,
)


def test_close_based_signal_does_not_capture_same_day_jump() -> None:
    dates = pd.bdate_range("2026-01-02", periods=8)
    prices = pd.DataFrame(
        {
            "QQQ": [100.0, 200.0, 200.0, 200.0, 200.0, 200.0, 200.0, 200.0],
            "TQQQ": [10.0] * 8,
            "SGOV": [100.0] * 8,
        },
        index=dates,
    )
    target = pd.DataFrame(
        {
            "QQQ": [0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            "TQQQ": [0.0] * 8,
            "SGOV": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        },
        index=dates,
    )
    policy = {
        "execution_policy_id": "daily_close_next_day_v1",
        "execution_frequency": "daily",
        "signal_to_execution_lag": 1,
        "validity_period_days": 20,
        "max_turnover_per_period": 1.0,
        "cost_model": {"explicit_cost_bps": 0.0},
    }

    actual, rows = _actual_position_path(
        strategy_id="toy",
        execution_policy_id="daily_close_next_day_v1",
        target_weights=target,
        policy=policy,
    )
    _attach_path_return_columns(
        prices=prices,
        target_weights=target,
        actual_weights=actual,
        path_rows=rows,
        cost_bps=0.0,
    )

    assert rows[1]["actual_weight_qqq"] == 0.0
    assert rows[1]["portfolio_return_actual_path"] == 0.0
    assert rows[1]["portfolio_return_target_path"] == 0.0
    assert rows[2]["actual_weight_qqq"] == 1.0


def test_execution_lag_delays_actual_position_change_by_business_days() -> None:
    dates = pd.bdate_range("2026-01-02", periods=10)
    target = pd.DataFrame(
        {
            "QQQ": [0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            "TQQQ": [0.0] * 10,
            "SGOV": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        },
        index=dates,
    )
    policy = {
        "execution_policy_id": "lag_5d",
        "execution_frequency": "daily",
        "signal_to_execution_lag": 5,
        "validity_period_days": 20,
        "max_turnover_per_period": 1.0,
        "cost_model": {"explicit_cost_bps": 0.0},
    }

    _actual, rows = _actual_position_path(
        strategy_id="toy",
        execution_policy_id="lag_5d",
        target_weights=target,
        policy=policy,
    )

    assert [row["actual_weight_qqq"] for row in rows[:6]] == [0.0] * 6
    assert rows[6]["actual_weight_qqq"] == 1.0
    assert rows[6]["signal_date"] == dates[1].date().isoformat()
    assert rows[6]["execution_lag_bdays"] == 5
