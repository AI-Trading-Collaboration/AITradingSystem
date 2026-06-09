from __future__ import annotations

import json
from math import prod

import pandas as pd
import pytest

from ai_trading_system.backtest.engine import summarize_long_only_backtest
from ai_trading_system.etf_portfolio.backtest_metrics import (
    STANDARDIZED_BACKTEST_METRIC_KEYS,
    build_monthly_return_table,
    build_standardized_backtest_metrics,
)


def test_standardized_metric_schema_cagr_drawdown_and_benchmark_excess() -> None:
    daily = _daily_frame([0.02, -0.01, 0.03], [0.01, 0.0, 0.02], [0.2, 0.3, 0.1])
    strategy_returns = [0.02, -0.01, 0.03]
    metrics = summarize_long_only_backtest(
        strategy_returns,
        exposures=[0.8, 0.7, 0.9],
        turnovers=[0.1, 0.2, 0.05],
    )
    monthly = build_monthly_return_table(daily, benchmark_weights={"SPY": 1.0})

    payload = build_standardized_backtest_metrics(
        daily,
        initial_nav=100_000.0,
        strategy_metrics=metrics,
        strategy_returns=strategy_returns,
        annualized_volatility=0.12,
        benchmark_metrics={"B001": {"total_return": 0.0302, "max_drawdown": -0.05}},
        primary_benchmark_id="B001",
        monthly_returns=monthly,
    )

    assert payload["metric_keys"] == list(STANDARDIZED_BACKTEST_METRIC_KEYS)
    assert set(STANDARDIZED_BACKTEST_METRIC_KEYS).issubset(payload)
    assert payload["start_date"] == "2026-01-29"
    assert payload["end_date"] == "2026-02-02"
    assert payload["trading_days"] == 3
    assert payload["final_nav"] == pytest.approx(100_000 * prod(1 + r for r in strategy_returns))
    assert payload["CAGR"] == pytest.approx(metrics.cagr)
    assert payload["max_drawdown"] == pytest.approx(metrics.max_drawdown)
    assert payload["benchmark_excess_return"] == pytest.approx(metrics.total_return - 0.0302)
    assert payload["benchmark_drawdown_reduction"] == pytest.approx(
        abs(-0.05) - abs(metrics.max_drawdown)
    )
    assert payload["best_month"]["month"] == "2026-02"
    assert payload["best_month"]["strategy_return"] == pytest.approx(0.03)
    assert payload["positive_month_ratio"] == pytest.approx(1.0)
    assert payload["average_cash_weight"] == pytest.approx(0.2)
    assert payload["average_equity_exposure"] == pytest.approx(0.8)


def test_monthly_return_table_compounds_strategy_benchmark_and_excess() -> None:
    daily = _daily_frame([0.02, -0.01, 0.03], [0.01, 0.0, 0.02], [0.2, 0.3, 0.1])

    rows = build_monthly_return_table(daily, benchmark_weights={"SPY": 1.0})

    assert rows[0]["month"] == "2026-01"
    assert rows[0]["strategy_return"] == pytest.approx(1.02 * 0.99 - 1.0)
    assert rows[0]["benchmark_return"] == pytest.approx(0.01)
    assert rows[0]["excess_return"] == pytest.approx((1.02 * 0.99 - 1.0) - 0.01)
    assert rows[0]["max_drawdown_in_month"] == pytest.approx(-0.01)
    assert rows[0]["average_equity_exposure"] == pytest.approx(0.75)
    assert rows[1]["month"] == "2026-02"
    assert rows[1]["benchmark_return"] == pytest.approx(0.02)


def test_standardized_metrics_explain_null_sharpe_for_zero_volatility() -> None:
    payload = _standardized_payload_for_returns([0.01, 0.01])

    assert payload["Sharpe"] is None
    assert payload["metric_null_reasons"]["Sharpe"] == "zero_return_volatility"


def test_standardized_metrics_explain_null_sortino_for_no_downside() -> None:
    payload = _standardized_payload_for_returns([0.01, 0.02])

    assert payload["Sortino"] is None
    assert payload["metric_null_reasons"]["Sortino"] == "no_downside_returns"


def test_standardized_metrics_explain_null_calmar_for_zero_drawdown() -> None:
    payload = _standardized_payload_for_returns([0.01, 0.02])

    assert payload["Calmar"] is None
    assert payload["metric_null_reasons"]["Calmar"] == "zero_max_drawdown"


def test_standardized_metrics_explain_null_volatility_for_insufficient_sample() -> None:
    payload = _standardized_payload_for_returns([0.01])

    assert payload["annualized_volatility"] is None
    assert payload["metric_null_reasons"]["annualized_volatility"] == "insufficient_return_sample"


def _standardized_payload_for_returns(returns: list[float]) -> dict[str, object]:
    daily = _daily_frame(returns, returns, [0.0] * len(returns))
    metrics = summarize_long_only_backtest(
        returns,
        exposures=[1.0] * len(returns),
        turnovers=[0.0] * len(returns),
    )
    monthly = build_monthly_return_table(daily, benchmark_weights={"SPY": 1.0})
    return build_standardized_backtest_metrics(
        daily,
        initial_nav=100_000.0,
        strategy_metrics=metrics,
        strategy_returns=returns,
        annualized_volatility=None,
        benchmark_metrics={"B001": {"total_return": 0.0, "max_drawdown": 0.0}},
        primary_benchmark_id="B001",
        monthly_returns=monthly,
    )


def _daily_frame(
    strategy_returns: list[float],
    benchmark_returns: list[float],
    cash_weights: list[float],
) -> pd.DataFrame:
    dates = pd.bdate_range("2026-01-29", periods=len(strategy_returns))
    running = 100_000.0
    rows = []
    for index, strategy_return in enumerate(strategy_returns):
        running *= 1.0 + strategy_return
        cash_weight = cash_weights[index]
        rows.append(
            {
                "signal_date": dates[index].date().isoformat(),
                "return_date": dates[index].date().isoformat(),
                "strategy_return": strategy_return,
                "portfolio_value": running,
                "asset_returns_json": json.dumps(
                    {"SPY": benchmark_returns[index], "CASH": 0.0},
                    sort_keys=True,
                ),
                "target_weights_json": json.dumps(
                    {"SPY": 1.0 - cash_weight, "CASH": cash_weight},
                    sort_keys=True,
                ),
            }
        )
    return pd.DataFrame(rows)
