from __future__ import annotations

from dataclasses import dataclass
from math import prod, sqrt
from statistics import mean, pstdev


@dataclass(frozen=True)
class BacktestMetrics:
    total_return: float
    cagr: float
    max_drawdown: float
    sharpe: float | None
    time_in_market: float
    turnover: float


def calculate_max_drawdown(equity_curve: list[float]) -> float:
    if not equity_curve:
        raise ValueError("equity_curve must not be empty")

    peak = equity_curve[0]
    max_drawdown = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        drawdown = (value / peak) - 1.0
        max_drawdown = min(max_drawdown, drawdown)
    return max_drawdown


def calculate_cagr(total_return: float, periods: int, periods_per_year: int = 252) -> float:
    if periods <= 0:
        raise ValueError("periods must be positive")
    years = periods / periods_per_year
    return float((1.0 + total_return) ** (1.0 / years) - 1.0)


def calculate_sharpe(
    daily_returns: list[float],
    risk_free_rate: float = 0.0,
    periods_per_year: int = 252,
) -> float | None:
    if len(daily_returns) < 2:
        return None

    excess_daily = risk_free_rate / periods_per_year
    excess_returns = [value - excess_daily for value in daily_returns]
    volatility = pstdev(excess_returns)
    if volatility == 0:
        return None
    return mean(excess_returns) / volatility * sqrt(periods_per_year)


def summarize_long_only_backtest(
    strategy_returns: list[float],
    exposures: list[float],
    turnovers: list[float],
    periods_per_year: int = 252,
) -> BacktestMetrics:
    if not strategy_returns:
        raise ValueError("strategy_returns must not be empty")
    if len(strategy_returns) != len(exposures) or len(strategy_returns) != len(turnovers):
        raise ValueError("strategy_returns, exposures, and turnovers must have the same length")

    equity_curve = []
    running_equity = 1.0
    for period_return in strategy_returns:
        running_equity *= 1.0 + period_return
        equity_curve.append(running_equity)

    total_return = prod(1.0 + period_return for period_return in strategy_returns) - 1.0
    return BacktestMetrics(
        total_return=total_return,
        cagr=calculate_cagr(total_return, len(strategy_returns), periods_per_year),
        max_drawdown=calculate_max_drawdown(equity_curve),
        sharpe=calculate_sharpe(strategy_returns, periods_per_year=periods_per_year),
        time_in_market=mean(1.0 if exposure > 0 else 0.0 for exposure in exposures),
        turnover=sum(abs(value) for value in turnovers),
    )
