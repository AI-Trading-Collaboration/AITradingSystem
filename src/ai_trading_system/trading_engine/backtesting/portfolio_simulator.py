from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from ai_trading_system.trading_engine.backtesting.metrics import (
    PortfolioMetrics,
    calculate_portfolio_metrics,
)
from ai_trading_system.trading_engine.backtesting.transaction_cost import calculate_transaction_cost
from ai_trading_system.trading_engine.parameters.parameter_schema import (
    ProductionParameters,
    TransactionCostConfig,
)

DEFENSIVE_ASSETS = {"CASH", "SGOV"}

# v0.1 shadow backtest uses this documented pilot mapping to translate the
# composite signal score into exposure before a later policy/config calibration.
V0_1_RISK_BUDGET_BY_SCORE: tuple[tuple[float, float], ...] = (
    (0.60, 0.85),
    (0.45, 0.65),
    (0.35, 0.45),
    (0.25, 0.30),
)
V0_1_MIN_RISK_BUDGET = 0.15


@dataclass(frozen=True)
class PortfolioSimulationResult:
    metrics: PortfolioMetrics
    daily_rows: tuple[dict[str, object], ...]
    score_rows: tuple[dict[str, object], ...] = ()
    parameter_contribution_summary: dict[str, float] | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "metrics": self.metrics.to_dict(),
            "daily_rows": list(self.daily_rows),
            "score_rows": list(self.score_rows),
            "parameter_contribution_summary": dict(self.parameter_contribution_summary or {}),
        }


@dataclass(frozen=True)
class AllocationResult:
    target: pd.DataFrame
    score_rows: tuple[dict[str, object], ...]
    parameter_contribution_summary: dict[str, float]
    daily_score: pd.Series


def simulate_parameter_portfolio(
    prices: pd.DataFrame,
    parameters: ProductionParameters,
    weights: dict[str, float],
    cost_config: TransactionCostConfig,
    *,
    start: date,
    end: date,
    signal_frames: dict[str, pd.DataFrame] | None = None,
) -> PortfolioSimulationResult:
    price_panel = _prepare_price_panel(prices, parameters.flattened_asset_universe())
    if price_panel.empty:
        return _empty_simulation_result()
    price_panel = price_panel.loc[
        (price_panel.index.date >= start) & (price_panel.index.date <= end)
    ].copy()
    if len(price_panel.index) < 3:
        return _empty_simulation_result()
    returns = price_panel.pct_change().fillna(0.0)
    feature_frame = _feature_frame(price_panel, returns, signal_frames=signal_frames)
    tradable_assets = [
        asset for asset in parameters.flattened_asset_universe() if asset not in DEFENSIVE_ASSETS
    ]
    allocation = _target_allocations(
        feature_frame,
        tradable_assets=tradable_assets,
        defensive_assets=[asset for asset in ("SGOV", "CASH") if asset in price_panel.columns],
        weights=weights,
        parameters=parameters,
    )
    target = allocation.target
    shifted_target = target.shift(1).fillna(0.0)
    turnover = shifted_target.diff().abs().sum(axis=1).fillna(shifted_target.abs().sum(axis=1))
    gross_returns = (
        shifted_target * returns.reindex(columns=shifted_target.columns).fillna(0.0)
    ).sum(axis=1)
    costs = turnover.apply(lambda value: calculate_transaction_cost(float(value), cost_config))
    strategy_returns = gross_returns - costs
    equity = (1.0 + strategy_returns).cumprod()
    benchmark_returns = returns["QQQ"] if "QQQ" in returns.columns else gross_returns
    exposure = shifted_target.loc[
        :,
        [asset for asset in tradable_assets if asset in shifted_target],
    ]
    exposure_series = (
        exposure.sum(axis=1) if not exposure.empty else pd.Series(0.0, index=returns.index)
    )
    metrics = calculate_portfolio_metrics(
        strategy_returns,
        equity,
        turnover,
        benchmark_returns=benchmark_returns,
        exposure=exposure_series,
    )
    rows: list[dict[str, object]] = []
    for timestamp in strategy_returns.index:
        rows.append(
            {
                "date": pd.Timestamp(timestamp).date().isoformat(),
                "strategy_return": float(strategy_returns.loc[timestamp]),
                "gross_return": float(gross_returns.loc[timestamp]),
                "transaction_cost": float(costs.loc[timestamp]),
                "turnover": float(turnover.loc[timestamp]),
                "equity": float(equity.loc[timestamp]),
                "risk_asset_exposure": float(exposure_series.loc[timestamp]),
                "average_composite_score": float(allocation.daily_score.loc[timestamp]),
                "portfolio_weights": {
                    str(asset): _float_or_zero(shifted_target.loc[timestamp, asset])
                    for asset in shifted_target.columns
                },
            }
        )
    return PortfolioSimulationResult(
        metrics=metrics,
        daily_rows=tuple(rows),
        score_rows=allocation.score_rows,
        parameter_contribution_summary=allocation.parameter_contribution_summary,
    )


def _prepare_price_panel(prices: pd.DataFrame, assets: tuple[str, ...]) -> pd.DataFrame:
    required = {"date", "ticker", "adj_close"}
    if not required.issubset(prices.columns):
        return pd.DataFrame()
    price_assets = [asset for asset in assets if asset != "CASH"]
    frame = prices.loc[prices["ticker"].astype(str).isin(price_assets)].copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["_date"].notna() & frame["_adj_close"].notna()]
    if frame.empty:
        return pd.DataFrame()
    panel = (
        frame.pivot_table(index="_date", columns="ticker", values="_adj_close", aggfunc="last")
        .sort_index()
        .ffill()
    )
    if "CASH" in assets:
        panel["CASH"] = 1.0
    return panel


def _feature_frame(
    price_panel: pd.DataFrame,
    returns: pd.DataFrame,
    *,
    signal_frames: dict[str, pd.DataFrame] | None = None,
) -> dict[str, pd.DataFrame]:
    trend = _normalize(price_panel.pct_change(63))
    sector_return = price_panel.pct_change(21)
    sector = _normalize(sector_return.sub(sector_return.median(axis=1), axis=0))
    macro = 1.0 - _normalize(returns.rolling(21, min_periods=5).std())
    earnings = _normalize(price_panel.pct_change(126))
    rolling_max = price_panel.rolling(126, min_periods=20).max()
    drawdown = price_panel / rolling_max - 1.0
    valuation = 1.0 - _normalize(drawdown.abs())
    event = 1.0 - _normalize(returns.clip(upper=0.0).abs().rolling(21, min_periods=5).mean())
    features = {
        "trend_momentum": trend.fillna(0.5),
        "sector_strength": sector.fillna(0.5),
        "macro_liquidity": macro.fillna(0.5),
        "earnings_quality": earnings.fillna(0.5),
        "valuation_risk": valuation.fillna(0.5),
        "event_risk": event.fillna(0.5),
    }
    if not signal_frames:
        return features
    for signal_name, raw_frame in signal_frames.items():
        if signal_name not in features:
            continue
        frame = raw_frame.copy()
        frame.index = pd.to_datetime(frame.index, errors="coerce")
        frame = frame.loc[frame.index.notna()]
        if frame.empty:
            continue
        features[signal_name] = (
            frame.reindex(index=price_panel.index, columns=price_panel.columns)
            .ffill()
            .fillna(0.5)
            .clip(lower=0.0, upper=1.0)
        )
    return features


def _target_allocations(
    feature_frame: dict[str, pd.DataFrame],
    *,
    tradable_assets: list[str],
    defensive_assets: list[str],
    weights: dict[str, float],
    parameters: ProductionParameters,
) -> AllocationResult:
    columns = tradable_assets + defensive_assets
    index = next(iter(feature_frame.values())).index
    target = pd.DataFrame(0.0, index=index, columns=columns)
    composite = pd.DataFrame(0.0, index=index, columns=tradable_assets)
    contribution_frames: dict[str, pd.DataFrame] = {}
    for feature_name, weight in weights.items():
        frame = feature_frame.get(feature_name)
        if frame is None:
            continue
        weighted_frame = frame.reindex(columns=tradable_assets).fillna(0.5) * weight
        contribution_frames[feature_name] = weighted_frame
        composite = composite.add(weighted_frame, fill_value=0.0)
    score = composite.mean(axis=1).fillna(0.0)
    risk_budget = score.apply(_risk_budget_for_score)
    positive = composite.clip(lower=0.0)
    row_sums = positive.sum(axis=1).replace(0.0, np.nan)
    allocations = positive.div(row_sums, axis=0).fillna(0.0).mul(risk_budget, axis=0)
    allocations = _apply_asset_cap(allocations, parameters.position_limits.max_single_asset_weight)
    target.loc[:, tradable_assets] = allocations
    used = target.loc[:, tradable_assets].sum(axis=1)
    residual = (1.0 - used).clip(lower=parameters.position_limits.min_cash_weight)
    defensive = _primary_defensive_asset(defensive_assets)
    if defensive is not None:
        target.loc[:, defensive] = residual
    target = target.fillna(0.0)
    return AllocationResult(
        target=target,
        score_rows=_score_attribution_rows(
            composite,
            contribution_frames,
            weights=weights,
            tradable_assets=tradable_assets,
        ),
        parameter_contribution_summary=_parameter_contribution_summary(
            contribution_frames,
            tradable_assets=tradable_assets,
        ),
        daily_score=score,
    )


def _empty_simulation_result() -> PortfolioSimulationResult:
    return PortfolioSimulationResult(
        metrics=calculate_portfolio_metrics(pd.Series(), pd.Series(), pd.Series()),
        daily_rows=(),
        score_rows=(),
        parameter_contribution_summary={},
    )


def _primary_defensive_asset(defensive_assets: list[str]) -> str | None:
    if "SGOV" in defensive_assets:
        return "SGOV"
    if "CASH" in defensive_assets:
        return "CASH"
    return None


def _normalize(frame: pd.DataFrame) -> pd.DataFrame:
    ranks = frame.rank(axis=1, pct=True)
    return ranks.clip(lower=0.0, upper=1.0)


def _risk_budget_for_score(score: float) -> float:
    for score_floor, risk_budget in V0_1_RISK_BUDGET_BY_SCORE:
        if score >= score_floor:
            return risk_budget
    return V0_1_MIN_RISK_BUDGET


def _apply_asset_cap(allocations: pd.DataFrame, cap: float) -> pd.DataFrame:
    capped = allocations.clip(upper=cap)
    overflow = allocations.sum(axis=1) - capped.sum(axis=1)
    for _ in range(4):
        room = (cap - capped).clip(lower=0.0)
        room_sum = room.sum(axis=1).replace(0.0, np.nan)
        redistribution = room.div(room_sum, axis=0).mul(overflow.clip(lower=0.0), axis=0)
        capped = (capped + redistribution.fillna(0.0)).clip(upper=cap)
        overflow = allocations.sum(axis=1) - capped.sum(axis=1)
        if float(overflow.abs().max()) <= 1e-12:
            break
    return capped


def _score_attribution_rows(
    composite: pd.DataFrame,
    contribution_frames: dict[str, pd.DataFrame],
    *,
    weights: dict[str, float],
    tradable_assets: list[str],
) -> tuple[dict[str, object], ...]:
    rows: list[dict[str, object]] = []
    for timestamp in composite.index:
        signal_date = pd.Timestamp(timestamp).date().isoformat()
        for asset in tradable_assets:
            contributions = {
                signal: _float_or_zero(frame.loc[timestamp, asset])
                for signal, frame in contribution_frames.items()
                if asset in frame.columns
            }
            signal_values = {
                signal: _signal_value_from_contribution(value, weights.get(signal, 0.0))
                for signal, value in contributions.items()
            }
            rows.append(
                {
                    "date": signal_date,
                    "asset": asset,
                    "composite_score": _float_or_zero(composite.loc[timestamp, asset]),
                    "signal_values": signal_values,
                    "signal_contributions": contributions,
                }
            )
    return tuple(rows)


def _parameter_contribution_summary(
    contribution_frames: dict[str, pd.DataFrame],
    *,
    tradable_assets: list[str],
) -> dict[str, float]:
    summary: dict[str, float] = {}
    for signal, frame in contribution_frames.items():
        values = frame.reindex(columns=tradable_assets).stack()
        summary[signal] = 0.0 if values.empty else round(float(values.mean()), 6)
    return summary


def _signal_value_from_contribution(contribution: float, weight: float) -> float:
    if weight <= 0.0:
        return 0.0
    return round(max(0.0, min(1.0, contribution / weight)), 6)


def _float_or_zero(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if np.isnan(number):
        return 0.0
    return round(number, 6)
