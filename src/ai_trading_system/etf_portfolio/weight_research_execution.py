from __future__ import annotations

import json
from datetime import date
from typing import Any, Literal

import pandas as pd

from ai_trading_system.backtest.engine import BacktestMetrics, summarize_long_only_backtest
from ai_trading_system.etf_portfolio.models import ETFConfigBundle
from ai_trading_system.etf_portfolio.weight_research_unblock import B1ExecutionPolicy

ExecutionMode = Literal["naive", "controlled"]


def simulate_target_path_execution(
    *,
    prices: pd.DataFrame,
    config: ETFConfigBundle,
    target_path: pd.DataFrame,
    mode: ExecutionMode,
    execution_policy: B1ExecutionPolicy | None = None,
) -> pd.DataFrame:
    if mode == "controlled" and execution_policy is None:
        raise ValueError("controlled execution requires execution_policy")
    close_pivot = _price_pivot(prices, config.backtest.backtest.price_field)
    rows: list[dict[str, Any]] = []
    current_weights = _default_weights(config)
    portfolio_equity = 1.0
    total_cost_bps = _total_cost_bps(config)
    for _, target_row in target_path.sort_values("signal_date").iterrows():
        signal_date = date.fromisoformat(str(target_row["signal_date"]))
        execution_date = date.fromisoformat(str(target_row["execution_date"]))
        return_date = date.fromisoformat(str(target_row["return_date"]))
        target_weights = {
            str(symbol): float(weight)
            for symbol, weight in json.loads(str(target_row["target_weights_json"])).items()
        }
        if mode == "controlled":
            decision = _execution_control_decision(
                current_weights=current_weights,
                target_weights=target_weights,
                policy=execution_policy,
                total_cost_bps=total_cost_bps,
            )
        else:
            executed_delta = {
                symbol: target_weights.get(symbol, 0.0) - current_weights.get(symbol, 0.0)
                for symbol in sorted(set(target_weights) | set(current_weights))
            }
            decision = _decision_payload(
                decision="TRADE",
                reason="naive_rebalance_to_target",
                current_weights=current_weights,
                executed_delta=executed_delta,
                max_abs_drift=max((abs(value) for value in executed_delta.values()), default=0.0),
                benefit_cost_ratio=None,
            )
        post_trade_weights = decision["post_trade_weights"]
        period_returns = _period_returns(close_pivot, execution_date, return_date)
        gross_return = sum(
            post_trade_weights.get(symbol, 0.0) * period_returns.get(symbol, 0.0)
            for symbol in post_trade_weights
        )
        transaction_cost = float(decision["turnover"]) * total_cost_bps / 10_000.0
        strategy_return = gross_return - transaction_cost
        portfolio_equity *= 1.0 + strategy_return
        rows.append(
            {
                "signal_date": signal_date.isoformat(),
                "execution_date": execution_date.isoformat(),
                "return_date": return_date.isoformat(),
                "market_regime": config.backtest.backtest.regime,
                "execution_mode": mode,
                "decision": decision["decision"],
                "decision_reason": decision["reason"],
                "gross_return": gross_return,
                "transaction_cost": transaction_cost,
                "strategy_return": strategy_return,
                "portfolio_equity": portfolio_equity,
                "turnover": decision["turnover"],
                "pre_trade_weights_json": json.dumps(
                    current_weights,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "target_weights_json": json.dumps(
                    target_weights,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "post_trade_weights_json": json.dumps(
                    post_trade_weights,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "executed_delta_json": json.dumps(
                    decision["executed_delta"],
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "period_returns_json": json.dumps(
                    period_returns,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "official_target_weights": False,
                "production_effect": "none",
            }
        )
        current_weights = _drift_weights(post_trade_weights, period_returns, gross_return)
    return pd.DataFrame(rows)


def metrics_from_execution_daily(daily: pd.DataFrame) -> BacktestMetrics:
    returns = [float(value) for value in daily["strategy_return"]]
    turnovers = [float(value) for value in daily["turnover"]]
    exposures = [
        1.0 - json.loads(str(value)).get("CASH", 0.0)
        for value in daily["post_trade_weights_json"]
    ]
    return summarize_long_only_backtest(returns, exposures, turnovers)


def comparison_payload(candidate: BacktestMetrics, comparator: BacktestMetrics) -> dict[str, float]:
    return {
        "return_delta": candidate.total_return - comparator.total_return,
        "cagr_delta": candidate.cagr - comparator.cagr,
        "drawdown_reduction": abs(comparator.max_drawdown) - abs(candidate.max_drawdown),
        "sharpe_delta": (candidate.sharpe or 0.0) - (comparator.sharpe or 0.0),
        "turnover_delta": candidate.turnover - comparator.turnover,
    }


def metrics_payload(metrics: BacktestMetrics) -> dict[str, float | None]:
    return {
        "total_return": metrics.total_return,
        "cagr": metrics.cagr,
        "max_drawdown": metrics.max_drawdown,
        "sharpe": metrics.sharpe,
        "sortino": metrics.sortino,
        "calmar": metrics.calmar,
        "time_in_market": metrics.time_in_market,
        "turnover": metrics.turnover,
    }


def _execution_control_decision(
    *,
    current_weights: dict[str, float],
    target_weights: dict[str, float],
    policy: B1ExecutionPolicy,
    total_cost_bps: float,
) -> dict[str, Any]:
    drift = {
        symbol: target_weights.get(symbol, 0.0) - current_weights.get(symbol, 0.0)
        for symbol in sorted(set(target_weights) | set(current_weights))
    }
    max_abs_drift = max((abs(value) for value in drift.values()), default=0.0)
    desired_turnover = sum(abs(value) for value in drift.values())
    estimated_cost = desired_turnover * total_cost_bps / 10_000.0
    benefit_proxy = desired_turnover
    benefit_cost_ratio = float("inf") if estimated_cost == 0 else benefit_proxy / estimated_cost
    if max_abs_drift < policy.deadband_abs_weight:
        return _decision_payload(
            decision="NO_TRADE",
            reason="inside_deadband",
            current_weights=current_weights,
            executed_delta={symbol: 0.0 for symbol in drift},
            max_abs_drift=max_abs_drift,
            benefit_cost_ratio=benefit_cost_ratio,
        )
    if benefit_cost_ratio < policy.min_benefit_cost_ratio:
        return _decision_payload(
            decision="NO_TRADE",
            reason="benefit_cost_below_threshold",
            current_weights=current_weights,
            executed_delta={symbol: 0.0 for symbol in drift},
            max_abs_drift=max_abs_drift,
            benefit_cost_ratio=benefit_cost_ratio,
        )
    scale = 1.0
    if max_abs_drift > policy.max_single_asset_adjustment:
        scale = min(scale, policy.max_single_asset_adjustment / max_abs_drift)
    if desired_turnover > policy.max_daily_turnover:
        scale = min(scale, policy.max_daily_turnover / desired_turnover)
    executed_delta = {symbol: value * scale for symbol, value in drift.items()}
    reason = "full_rebalance" if scale == 1.0 else "capped_adjustment"
    return _decision_payload(
        decision="TRADE",
        reason=reason,
        current_weights=current_weights,
        executed_delta=executed_delta,
        max_abs_drift=max_abs_drift,
        benefit_cost_ratio=benefit_cost_ratio,
    )


def _decision_payload(
    *,
    decision: str,
    reason: str,
    current_weights: dict[str, float],
    executed_delta: dict[str, float],
    max_abs_drift: float,
    benefit_cost_ratio: float | None,
) -> dict[str, Any]:
    post_trade = {
        symbol: current_weights.get(symbol, 0.0) + executed_delta.get(symbol, 0.0)
        for symbol in sorted(set(current_weights) | set(executed_delta))
    }
    return {
        "decision": decision,
        "reason": reason,
        "post_trade_weights": _normalize_weights(post_trade),
        "executed_delta": executed_delta,
        "turnover": sum(abs(value) for value in executed_delta.values()),
        "max_abs_drift": max_abs_drift,
        "benefit_cost_ratio": (
            None
            if benefit_cost_ratio is None or benefit_cost_ratio == float("inf")
            else benefit_cost_ratio
        ),
    }


def _price_pivot(prices: pd.DataFrame, price_field: str) -> pd.DataFrame:
    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_price"] = pd.to_numeric(frame[price_field], errors="coerce")
    pivot = frame.pivot(index="_date", columns="symbol", values="_price").sort_index()
    return pivot.dropna(how="all")


def _period_returns(close_pivot: pd.DataFrame, left: date, right: date) -> dict[str, float]:
    left_row = close_pivot.loc[pd.Timestamp(left)]
    right_row = close_pivot.loc[pd.Timestamp(right)]
    returns: dict[str, float] = {}
    for symbol in close_pivot.columns:
        left_value = left_row.get(symbol)
        right_value = right_row.get(symbol)
        if pd.isna(left_value) or pd.isna(right_value) or float(left_value) == 0:
            continue
        returns[str(symbol)] = float(right_value) / float(left_value) - 1.0
    returns["CASH"] = 0.0
    return returns


def _drift_weights(
    weights: dict[str, float],
    period_returns: dict[str, float],
    gross_return: float,
) -> dict[str, float]:
    denominator = 1.0 + gross_return
    if denominator <= 0:
        return dict(weights)
    drifted = {
        symbol: weight * (1.0 + period_returns.get(symbol, 0.0)) / denominator
        for symbol, weight in weights.items()
    }
    return _normalize_weights(drifted)


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    total = sum(float(value) for value in weights.values())
    if total == 0:
        return dict(weights)
    return {symbol: float(value) / total for symbol, value in weights.items()}


def _default_weights(config: ETFConfigBundle) -> dict[str, float]:
    return {symbol: float(asset.default_weight) for symbol, asset in config.assets.assets.items()}


def _total_cost_bps(config: ETFConfigBundle) -> float:
    return (
        float(config.risk.transaction_costs.commission_bps)
        + float(config.risk.transaction_costs.slippage_bps)
    )
