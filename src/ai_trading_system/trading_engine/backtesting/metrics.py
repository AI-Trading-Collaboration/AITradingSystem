from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PortfolioMetrics:
    cumulative_return: float
    annualized_return: float
    max_drawdown: float
    volatility: float
    downside_volatility: float
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    turnover: float
    number_of_rebalances: int
    average_holding_period: float
    risk_off_hit_rate: float
    missed_upside_rate: float
    false_risk_alert_rate: float
    drawdown_reduction_ratio: float

    def to_dict(self) -> dict[str, float | int]:
        return {
            "cumulative_return": self.cumulative_return,
            "annualized_return": self.annualized_return,
            "max_drawdown": self.max_drawdown,
            "volatility": self.volatility,
            "downside_volatility": self.downside_volatility,
            "sharpe_ratio": self.sharpe_ratio,
            "sortino_ratio": self.sortino_ratio,
            "calmar_ratio": self.calmar_ratio,
            "turnover": self.turnover,
            "number_of_rebalances": self.number_of_rebalances,
            "average_holding_period": self.average_holding_period,
            "risk_off_hit_rate": self.risk_off_hit_rate,
            "missed_upside_rate": self.missed_upside_rate,
            "false_risk_alert_rate": self.false_risk_alert_rate,
            "drawdown_reduction_ratio": self.drawdown_reduction_ratio,
        }


def calculate_portfolio_metrics(
    daily_returns: pd.Series,
    equity_curve: pd.Series,
    turnover: pd.Series,
    *,
    benchmark_returns: pd.Series | None = None,
    exposure: pd.Series | None = None,
    baseline_max_drawdown: float | None = None,
) -> PortfolioMetrics:
    returns = pd.to_numeric(daily_returns, errors="coerce").dropna()
    if returns.empty:
        return _empty_metrics()
    equity = pd.to_numeric(equity_curve, errors="coerce").dropna()
    if equity.empty:
        equity = (1.0 + returns).cumprod()
    cumulative = float(equity.iloc[-1] - 1.0)
    periods = max(len(returns), 1)
    annualized = (1.0 + cumulative) ** (252.0 / periods) - 1.0 if cumulative > -1.0 else -1.0
    volatility = float(returns.std(ddof=0) * np.sqrt(252.0))
    downside = returns.loc[returns < 0.0]
    downside_vol = float(downside.std(ddof=0) * np.sqrt(252.0)) if not downside.empty else 0.0
    running_max = equity.cummax()
    drawdowns = equity / running_max - 1.0
    max_drawdown = float(drawdowns.min()) if not drawdowns.empty else 0.0
    sharpe = annualized / volatility if volatility > 0 else 0.0
    sortino = annualized / downside_vol if downside_vol > 0 else 0.0
    calmar = annualized / abs(max_drawdown) if max_drawdown < 0 else 0.0
    numeric_turnover = pd.to_numeric(turnover, errors="coerce").fillna(0.0)
    number_of_rebalances = int((numeric_turnover > 1e-12).sum())
    average_holding_period = (
        periods / number_of_rebalances if number_of_rebalances else float(periods)
    )
    decision_quality = _decision_quality(
        benchmark_returns=benchmark_returns,
        exposure=exposure,
        baseline_max_drawdown=baseline_max_drawdown,
        max_drawdown=max_drawdown,
    )
    return PortfolioMetrics(
        cumulative_return=cumulative,
        annualized_return=float(annualized),
        max_drawdown=max_drawdown,
        volatility=volatility,
        downside_volatility=downside_vol,
        sharpe_ratio=float(sharpe),
        sortino_ratio=float(sortino),
        calmar_ratio=float(calmar),
        turnover=float(numeric_turnover.sum()),
        number_of_rebalances=number_of_rebalances,
        average_holding_period=float(average_holding_period),
        risk_off_hit_rate=decision_quality["risk_off_hit_rate"],
        missed_upside_rate=decision_quality["missed_upside_rate"],
        false_risk_alert_rate=decision_quality["false_risk_alert_rate"],
        drawdown_reduction_ratio=decision_quality["drawdown_reduction_ratio"],
    )


def _decision_quality(
    *,
    benchmark_returns: pd.Series | None,
    exposure: pd.Series | None,
    baseline_max_drawdown: float | None,
    max_drawdown: float,
) -> dict[str, float]:
    if benchmark_returns is None or exposure is None:
        risk_off_hit_rate = 0.0
        missed_upside_rate = 0.0
        false_risk_alert_rate = 0.0
    else:
        aligned = pd.DataFrame(
            {
                "benchmark": pd.to_numeric(benchmark_returns, errors="coerce"),
                "exposure": pd.to_numeric(exposure, errors="coerce"),
            }
        ).dropna()
        risk_off = aligned["exposure"] < aligned["exposure"].median()
        down_days = aligned["benchmark"] < 0.0
        up_days = aligned["benchmark"] > 0.0
        risk_off_hit_rate = _ratio((risk_off & down_days).sum(), down_days.sum())
        missed_upside_rate = _ratio((risk_off & up_days).sum(), up_days.sum())
        false_risk_alert_rate = _ratio((risk_off & up_days).sum(), risk_off.sum())
    drawdown_reduction_ratio = 0.0
    if baseline_max_drawdown is not None and baseline_max_drawdown < 0.0:
        drawdown_reduction_ratio = (baseline_max_drawdown - max_drawdown) / abs(
            baseline_max_drawdown
        )
    return {
        "risk_off_hit_rate": float(risk_off_hit_rate),
        "missed_upside_rate": float(missed_upside_rate),
        "false_risk_alert_rate": float(false_risk_alert_rate),
        "drawdown_reduction_ratio": float(drawdown_reduction_ratio),
    }


def _ratio(numerator: object, denominator: object) -> float:
    denominator_float = float(denominator)
    if denominator_float <= 0.0:
        return 0.0
    return float(numerator) / denominator_float


def _empty_metrics() -> PortfolioMetrics:
    return PortfolioMetrics(
        cumulative_return=0.0,
        annualized_return=0.0,
        max_drawdown=0.0,
        volatility=0.0,
        downside_volatility=0.0,
        sharpe_ratio=0.0,
        sortino_ratio=0.0,
        calmar_ratio=0.0,
        turnover=0.0,
        number_of_rebalances=0,
        average_holding_period=0.0,
        risk_off_hit_rate=0.0,
        missed_upside_rate=0.0,
        false_risk_alert_rate=0.0,
        drawdown_reduction_ratio=0.0,
    )
