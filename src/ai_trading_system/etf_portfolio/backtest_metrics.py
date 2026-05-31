from __future__ import annotations

import json
from collections.abc import Mapping
from math import prod, sqrt
from statistics import mean, pstdev

import pandas as pd

from ai_trading_system.backtest.engine import BacktestMetrics

STANDARDIZED_BACKTEST_METRIC_KEYS: tuple[str, ...] = (
    "start_date",
    "end_date",
    "trading_days",
    "initial_nav",
    "final_nav",
    "total_return",
    "CAGR",
    "annualized_volatility",
    "max_drawdown",
    "Sharpe",
    "Sortino",
    "Calmar",
    "best_month",
    "worst_month",
    "positive_month_ratio",
    "turnover",
    "average_equity_exposure",
    "average_cash_weight",
    "benchmark_excess_return",
    "benchmark_drawdown_reduction",
)


def build_standardized_backtest_metrics(
    daily: pd.DataFrame,
    *,
    initial_nav: float,
    strategy_metrics: BacktestMetrics,
    strategy_returns: list[float],
    annualized_volatility: float | None,
    benchmark_metrics: Mapping[str, Mapping[str, object]],
    primary_benchmark_id: str,
    monthly_returns: list[dict[str, object]],
) -> dict[str, object]:
    metric_null_reasons: dict[str, str] = {}
    first_signal_date = _frame_date_at(daily, "signal_date", 0)
    last_signal_date = _frame_date_at(daily, "signal_date", -1)
    final_nav = _final_nav(daily, initial_nav, strategy_metrics.total_return)
    average_cash_weight = _average_cash_weight(daily)
    average_equity_exposure = (
        None if average_cash_weight is None else 1.0 - average_cash_weight
    )
    if average_cash_weight is None:
        metric_null_reasons["average_cash_weight"] = "target_weights_missing"
        metric_null_reasons["average_equity_exposure"] = "target_weights_missing"

    if strategy_metrics.sharpe is None:
        metric_null_reasons["Sharpe"] = _sharpe_null_reason(strategy_returns)
    if strategy_metrics.sortino is None:
        metric_null_reasons["Sortino"] = _sortino_null_reason(strategy_returns)
    if strategy_metrics.calmar is None:
        metric_null_reasons["Calmar"] = _calmar_null_reason(strategy_metrics.max_drawdown)
    standardized_volatility = annualized_volatility
    if len(strategy_returns) < 2:
        standardized_volatility = None
        metric_null_reasons["annualized_volatility"] = "insufficient_return_sample"

    monthly_strategy = [
        row for row in monthly_returns if row.get("strategy_return") is not None
    ]
    if monthly_strategy:
        best_row = max(monthly_strategy, key=lambda row: float(row["strategy_return"]))
        worst_row = min(monthly_strategy, key=lambda row: float(row["strategy_return"]))
        best_month: dict[str, object] | None = {
            "month": best_row["month"],
            "strategy_return": best_row["strategy_return"],
        }
        worst_month: dict[str, object] | None = {
            "month": worst_row["month"],
            "strategy_return": worst_row["strategy_return"],
        }
        positive_month_ratio: float | None = sum(
            1 for row in monthly_strategy if float(row["strategy_return"]) > 0.0
        ) / len(monthly_strategy)
    else:
        best_month = None
        worst_month = None
        positive_month_ratio = None
        metric_null_reasons["best_month"] = "insufficient_monthly_sample"
        metric_null_reasons["worst_month"] = "insufficient_monthly_sample"
        metric_null_reasons["positive_month_ratio"] = "insufficient_monthly_sample"

    benchmark_excess_return = None
    benchmark_drawdown_reduction = None
    primary_benchmark = benchmark_metrics.get(primary_benchmark_id)
    if primary_benchmark is None:
        metric_null_reasons["benchmark_excess_return"] = "primary_benchmark_missing"
        metric_null_reasons["benchmark_drawdown_reduction"] = "primary_benchmark_missing"
    else:
        benchmark_total_return = primary_benchmark.get("total_return")
        benchmark_max_drawdown = primary_benchmark.get("max_drawdown")
        if benchmark_total_return is None:
            metric_null_reasons["benchmark_excess_return"] = "primary_benchmark_metric_missing"
        else:
            benchmark_excess_return = strategy_metrics.total_return - float(
                benchmark_total_return
            )
        if benchmark_max_drawdown is None:
            metric_null_reasons[
                "benchmark_drawdown_reduction"
            ] = "primary_benchmark_metric_missing"
        else:
            benchmark_drawdown_reduction = abs(float(benchmark_max_drawdown)) - abs(
                strategy_metrics.max_drawdown
            )

    return {
        "schema_version": 1,
        "metric_set": "etf_backtest_standardized_v1",
        "primary_benchmark_id": primary_benchmark_id,
        "metric_keys": list(STANDARDIZED_BACKTEST_METRIC_KEYS),
        "metric_null_reasons": metric_null_reasons,
        "start_date": first_signal_date,
        "end_date": last_signal_date,
        "trading_days": int(len(daily)),
        "initial_nav": float(initial_nav),
        "final_nav": final_nav,
        "total_return": strategy_metrics.total_return,
        "CAGR": strategy_metrics.cagr,
        "annualized_volatility": standardized_volatility,
        "max_drawdown": strategy_metrics.max_drawdown,
        "Sharpe": strategy_metrics.sharpe,
        "Sortino": strategy_metrics.sortino,
        "Calmar": strategy_metrics.calmar,
        "best_month": best_month,
        "worst_month": worst_month,
        "positive_month_ratio": positive_month_ratio,
        "turnover": strategy_metrics.turnover,
        "average_equity_exposure": average_equity_exposure,
        "average_cash_weight": average_cash_weight,
        "benchmark_excess_return": benchmark_excess_return,
        "benchmark_drawdown_reduction": benchmark_drawdown_reduction,
    }


def build_monthly_return_table(
    daily: pd.DataFrame,
    *,
    benchmark_weights: Mapping[str, float] | None,
) -> list[dict[str, object]]:
    if daily.empty:
        return []
    frame = daily.copy()
    date_column = "return_date" if "return_date" in frame.columns else "signal_date"
    frame["_period_date"] = pd.to_datetime(frame[date_column], errors="coerce")
    frame = frame.dropna(subset=["_period_date"]).sort_values("_period_date")
    if frame.empty:
        return []
    frame["_month"] = frame["_period_date"].dt.to_period("M").astype(str)
    rows: list[dict[str, object]] = []
    for month, group in frame.groupby("_month", sort=True):
        strategy_returns = [
            float(value) for value in pd.to_numeric(group["strategy_return"], errors="coerce")
        ]
        strategy_returns = [value for value in strategy_returns if pd.notna(value)]
        strategy_return = _compound_return(strategy_returns) if strategy_returns else None
        benchmark_return = _compound_benchmark_return(group, benchmark_weights)
        average_equity_exposure = _average_equity_exposure_for_group(group)
        max_drawdown = (
            _max_drawdown_from_returns(strategy_returns) if strategy_returns else None
        )
        row: dict[str, object] = {
            "month": str(month),
            "strategy_return": strategy_return,
            "benchmark_return": benchmark_return,
            "excess_return": (
                None
                if strategy_return is None or benchmark_return is None
                else strategy_return - benchmark_return
            ),
            "max_drawdown_in_month": max_drawdown,
            "average_equity_exposure": average_equity_exposure,
        }
        if benchmark_return is None:
            row["benchmark_null_reason"] = (
                "benchmark_weights_missing"
                if benchmark_weights is None
                else "benchmark_returns_missing"
            )
        rows.append(row)
    return rows


def benchmark_weights_for_monthly_table(
    benchmark: object,
) -> dict[str, float] | None:
    benchmark_type = str(getattr(benchmark, "benchmark_type", ""))
    if benchmark_type == "buy_and_hold":
        symbol = getattr(benchmark, "symbol", None)
        return None if not symbol else {str(symbol): 1.0}
    if benchmark_type == "static_portfolio":
        return {
            str(symbol): float(weight)
            for symbol, weight in getattr(benchmark, "weights", {}).items()
        }
    return None


def _compound_return(returns: list[float]) -> float:
    return prod(1.0 + value for value in returns) - 1.0


def _compound_benchmark_return(
    group: pd.DataFrame,
    benchmark_weights: Mapping[str, float] | None,
) -> float | None:
    if benchmark_weights is None:
        return None
    period_returns: list[float] = []
    for _, row in group.iterrows():
        asset_returns = _json_mapping(row.get("asset_returns_json"))
        if not asset_returns:
            return None
        try:
            period_returns.append(
                sum(
                    float(weight) * float(asset_returns[str(symbol)])
                    for symbol, weight in benchmark_weights.items()
                )
            )
        except (KeyError, TypeError, ValueError):
            return None
    return _compound_return(period_returns)


def _max_drawdown_from_returns(returns: list[float]) -> float:
    if not returns:
        return 0.0
    peak = 1.0
    running = 1.0
    max_drawdown = 0.0
    for value in returns:
        running *= 1.0 + value
        peak = max(peak, running)
        max_drawdown = min(max_drawdown, running / peak - 1.0)
    return max_drawdown


def _average_cash_weight(daily: pd.DataFrame) -> float | None:
    cash_weights = [
        weights.get("CASH")
        for weights in (_json_mapping(value) for value in daily.get("target_weights_json", []))
    ]
    numeric = [float(value) for value in cash_weights if value is not None]
    return None if not numeric else mean(numeric)


def _average_equity_exposure_for_group(group: pd.DataFrame) -> float | None:
    cash_weights = [
        weights.get("CASH")
        for weights in (
            _json_mapping(value) for value in group.get("target_weights_json", [])
        )
    ]
    numeric = [float(value) for value in cash_weights if value is not None]
    return None if not numeric else 1.0 - mean(numeric)


def _final_nav(
    daily: pd.DataFrame,
    initial_nav: float,
    total_return: float,
) -> float:
    if not daily.empty and "portfolio_value" in daily.columns:
        value = pd.to_numeric(daily["portfolio_value"], errors="coerce").dropna()
        if not value.empty:
            return float(value.iloc[-1])
    return float(initial_nav) * (1.0 + float(total_return))


def _frame_date_at(daily: pd.DataFrame, column: str, index: int) -> str | None:
    if daily.empty or column not in daily.columns:
        return None
    value = daily.iloc[index][column]
    if pd.isna(value):
        return None
    return str(value)


def _sharpe_null_reason(returns: list[float]) -> str:
    if len(returns) < 2:
        return "insufficient_return_sample"
    if pstdev(returns) == 0:
        return "zero_return_volatility"
    return "metric_engine_returned_null"


def _sortino_null_reason(returns: list[float]) -> str:
    if len(returns) < 2:
        return "insufficient_return_sample"
    downside_deviation = sqrt(mean(min(value, 0.0) ** 2 for value in returns))
    if downside_deviation == 0:
        return "no_downside_returns"
    return "metric_engine_returned_null"


def _calmar_null_reason(max_drawdown: float) -> str:
    if max_drawdown == 0:
        return "zero_max_drawdown"
    return "metric_engine_returned_null"


def _json_mapping(value: object) -> dict[str, float]:
    if isinstance(value, Mapping):
        return {str(key): float(item) for key, item in value.items()}
    if value is None:
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, Mapping):
        return {}
    result: dict[str, float] = {}
    for key, item in parsed.items():
        try:
            result[str(key)] = float(item)
        except (TypeError, ValueError):
            continue
    return result
