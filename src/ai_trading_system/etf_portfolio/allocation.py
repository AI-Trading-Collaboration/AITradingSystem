from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from ai_trading_system.etf_portfolio.models import (
    ETFAllocationRecord,
    ETFAssetsConfig,
    ETFQualityReport,
    ETFRiskConfig,
    ETFStrategyConfig,
)


def allocate_portfolio(
    signals: pd.DataFrame,
    *,
    assets: ETFAssetsConfig,
    strategy: ETFStrategyConfig,
    risk: ETFRiskConfig,
    regime: str,
    run_date: date,
    config_hash: str,
    data_quality_report: ETFQualityReport,
    previous_weights: dict[str, float] | None = None,
) -> list[ETFAllocationRecord]:
    raw_weights = _score_adjusted_default_weights(signals, assets, strategy)
    constrained_weights, constraints_applied = _apply_constraints(raw_weights, assets, risk, regime)
    if previous_weights:
        constrained_weights, rebalance_constraints = _apply_rebalance_delta(
            constrained_weights,
            previous_weights,
            strategy.model.min_rebalance_delta,
        )
        constraints_applied.extend(rebalance_constraints)
    now = datetime.now(UTC)
    signal_scores = {
        str(row["symbol"]): float(row["composite_score"]) for _, row in signals.iterrows()
    }
    records: list[ETFAllocationRecord] = []
    for symbol in assets.assets:
        previous = previous_weights.get(symbol) if previous_weights else None
        target = constrained_weights.get(symbol, 0.0)
        records.append(
            ETFAllocationRecord(
                date=run_date,
                symbol=symbol,
                target_weight=round(target, 8),
                previous_weight=None if previous is None else round(previous, 8),
                trade_delta=None if previous is None else round(target - previous, 8),
                composite_score=signal_scores.get(symbol),
                regime=regime,
                reason_codes=tuple(_allocation_reasons(symbol, target, signal_scores.get(symbol))),
                constraints_applied=tuple(constraints_applied),
                model_version=strategy.model.version,
                config_hash=config_hash,
                data_quality_status=data_quality_report.status,
                created_at=now,
            )
        )
    _assert_weights_sum_to_one(records)
    return records


def allocation_to_frame(records: list[ETFAllocationRecord]) -> pd.DataFrame:
    return pd.DataFrame([record.to_record() for record in records])


def write_allocation(records: list[ETFAllocationRecord], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    allocation_to_frame(records).to_csv(path, index=False)
    return path


def load_allocation(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"ETF allocation file does not exist: {path}")
    return pd.read_csv(path)


def latest_weights_from_file(path: Path) -> dict[str, float] | None:
    if not path.exists():
        return None
    frame = pd.read_csv(path)
    if frame.empty or "date" not in frame.columns:
        return None
    latest_date = pd.to_datetime(frame["date"], errors="coerce").max()
    if pd.isna(latest_date):
        return None
    latest = frame.loc[pd.to_datetime(frame["date"], errors="coerce") == latest_date]
    return {
        str(row["symbol"]): float(row["target_weight"])
        for _, row in latest.iterrows()
        if pd.notna(row.get("target_weight"))
    }


def weights_from_records(records: list[ETFAllocationRecord]) -> dict[str, float]:
    return {record.symbol: record.target_weight for record in records}


def _score_adjusted_default_weights(
    signals: pd.DataFrame,
    assets: ETFAssetsConfig,
    strategy: ETFStrategyConfig,
) -> dict[str, float]:
    scores = {str(row["symbol"]): float(row["composite_score"]) for _, row in signals.iterrows()}
    weights: dict[str, float] = {}
    for symbol, asset in assets.assets.items():
        if symbol == "CASH":
            continue
        score = scores.get(symbol, 50.0)
        multiplier = _score_multiplier(score, strategy)
        weights[symbol] = asset.default_weight * multiplier
    return weights


def _apply_constraints(
    raw_weights: dict[str, float],
    assets: ETFAssetsConfig,
    risk: ETFRiskConfig,
    regime: str,
) -> tuple[dict[str, float], list[str]]:
    weights = dict(raw_weights)
    applied: list[str] = []
    constraints = risk.regime_constraints[regime]
    portfolio_constraints = risk.portfolio_constraints

    for symbol, asset in assets.assets.items():
        if symbol == "CASH":
            continue
        before = weights.get(symbol, 0.0)
        capped = min(before, asset.max_weight, portfolio_constraints.max_single_asset_weight)
        capped = max(capped, asset.min_weight)
        weights[symbol] = capped
        if capped != before:
            applied.append(f"{symbol}_ASSET_CAP")

    for group, group_config in assets.risk_groups.items():
        if group == "cash":
            continue
        group_symbols = [
            symbol
            for symbol, asset in assets.assets.items()
            if asset.risk_group == group and symbol != "CASH"
        ]
        regime_cap = constraints.semiconductor_cap if group == "semiconductor" else 1.0
        cap = min(group_config.max_weight, regime_cap)
        if _scale_group(weights, group_symbols, cap):
            applied.append(f"{group.upper()}_GROUP_CAP")

    equity_symbols = [symbol for symbol in assets.assets if symbol != "CASH"]
    if _scale_group(weights, equity_symbols, constraints.equity_cap):
        applied.append("REGIME_EQUITY_CAP")

    cash_min = max(constraints.cash_min, portfolio_constraints.min_cash_weight)
    max_non_cash = max(0.0, 1.0 - cash_min)
    if _scale_group(weights, equity_symbols, max_non_cash):
        applied.append("REGIME_CASH_MIN")

    cash = 1.0 - sum(weights.get(symbol, 0.0) for symbol in equity_symbols)
    weights["CASH"] = max(0.0, min(1.0, cash))
    _normalize_cash(weights, equity_symbols)
    return weights, sorted(set(applied))


def _apply_rebalance_delta(
    weights: dict[str, float],
    previous_weights: dict[str, float],
    min_delta: float,
) -> tuple[dict[str, float], list[str]]:
    if min_delta <= 0:
        return weights, []
    adjusted = dict(weights)
    changed = False
    non_cash = [symbol for symbol in adjusted if symbol != "CASH"]
    for symbol in non_cash:
        previous = previous_weights.get(symbol)
        if previous is None:
            continue
        if abs(adjusted[symbol] - previous) < min_delta:
            adjusted[symbol] = previous
            changed = True
    if changed:
        adjusted["CASH"] = 1.0 - sum(adjusted.get(symbol, 0.0) for symbol in non_cash)
        if adjusted["CASH"] < 0:
            _scale_group(adjusted, non_cash, 1.0)
            adjusted["CASH"] = 0.0
        _normalize_cash(adjusted, non_cash)
        return adjusted, ["MIN_REBALANCE_DELTA"]
    return weights, []


def _score_multiplier(score: float, strategy: ETFStrategyConfig) -> float:
    bands = strategy.allocation.score_multipliers
    if score < 30:
        return bands["very_bearish"]
    if score < 45:
        return bands["bearish"]
    if score < 60:
        return bands["neutral"]
    if score < 75:
        return bands["bullish"]
    return bands["very_bullish"]


def _scale_group(weights: dict[str, float], symbols: list[str], cap: float) -> bool:
    total = sum(weights.get(symbol, 0.0) for symbol in symbols)
    if total <= cap + 1e-12:
        return False
    if total <= 0:
        return False
    scale = cap / total
    for symbol in symbols:
        weights[symbol] = weights.get(symbol, 0.0) * scale
    return True


def _normalize_cash(weights: dict[str, float], non_cash_symbols: list[str]) -> None:
    non_cash_total = sum(weights.get(symbol, 0.0) for symbol in non_cash_symbols)
    weights["CASH"] = 1.0 - non_cash_total
    if abs(sum(weights.values()) - 1.0) > 1e-8:
        weights["CASH"] += 1.0 - sum(weights.values())


def _allocation_reasons(symbol: str, target: float, score: float | None) -> list[str]:
    if symbol == "CASH":
        return ["CASH_ABSORBS_UNALLOCATED_WEIGHT"]
    reasons = [f"TARGET_WEIGHT_{target:.2%}"]
    if score is not None:
        if score >= 60:
            reasons.append("COMPOSITE_SUPPORTS_OVERWEIGHT")
        elif score < 45:
            reasons.append("COMPOSITE_SUPPORTS_UNDERWEIGHT")
        else:
            reasons.append("COMPOSITE_NEUTRAL")
    return reasons


def _assert_weights_sum_to_one(records: list[ETFAllocationRecord]) -> None:
    total = sum(record.target_weight for record in records)
    if abs(total - 1.0) > 1e-6:
        raise ValueError(f"ETF target weights must sum to 1.0, got {total:.8f}")
