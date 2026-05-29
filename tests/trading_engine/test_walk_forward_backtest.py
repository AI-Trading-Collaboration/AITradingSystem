from __future__ import annotations

from datetime import date, timedelta

from ai_trading_system.trading_engine.backtesting.transaction_cost import calculate_transaction_cost
from ai_trading_system.trading_engine.backtesting.walk_forward import generate_walk_forward_windows
from ai_trading_system.trading_engine.parameters.parameter_schema import (
    TransactionCostConfig,
    WalkForwardConfig,
)


def test_walk_forward_window_split_uses_train_validation_and_step() -> None:
    dates = tuple(date(2026, 1, 1) + timedelta(days=offset) for offset in range(20))
    config = WalkForwardConfig(
        train_window_days=5,
        validation_window_days=3,
        step_days=4,
        min_history_days=8,
    )

    windows = generate_walk_forward_windows(dates, config)

    assert [window.window_id for window in windows] == ["wf-001", "wf-002", "wf-003", "wf-004"]
    assert windows[0].train_start == date(2026, 1, 1)
    assert windows[0].train_end == date(2026, 1, 5)
    assert windows[0].validation_start == date(2026, 1, 6)
    assert windows[0].validation_end == date(2026, 1, 8)
    assert windows[1].train_start == date(2026, 1, 5)


def test_walk_forward_requires_minimum_history() -> None:
    dates = tuple(date(2026, 1, 1) + timedelta(days=offset) for offset in range(7))
    config = WalkForwardConfig(
        train_window_days=5,
        validation_window_days=3,
        step_days=4,
        min_history_days=8,
    )

    assert generate_walk_forward_windows(dates, config) == ()


def test_transaction_cost_uses_explicit_bps_assumption() -> None:
    cost = calculate_transaction_cost(
        0.50,
        TransactionCostConfig(
            commission_bps=1,
            slippage_bps=5,
            fx_cost_bps=0,
            tax_model="ignored_for_v0_1",
        ),
    )

    assert cost == 0.0003
