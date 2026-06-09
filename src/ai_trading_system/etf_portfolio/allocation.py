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
    constrained_weights, constraints_applied, constraint_diagnostics = _apply_constraints(
        raw_weights,
        assets,
        risk,
        regime,
    )
    if previous_weights:
        constrained_weights, rebalance_constraints, rebalance_diagnostics = (
            _apply_rebalance_controls(
                constrained_weights,
                previous_weights,
                strategy.model.min_rebalance_delta,
                risk.portfolio_constraints.max_rebalance_trade_weight,
                risk.portfolio_constraints.max_daily_turnover,
            )
        )
        constraints_applied.extend(rebalance_constraints)
        constraint_diagnostics.extend(rebalance_diagnostics)
    constraints_applied = sorted(set(constraints_applied))
    constraint_diagnostics = _dedupe_diagnostics(constraint_diagnostics)
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
                constraint_diagnostics=tuple(constraint_diagnostics),
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
) -> tuple[dict[str, float], list[str], list[dict[str, object]]]:
    weights = dict(raw_weights)
    applied: list[str] = []
    diagnostics: list[dict[str, object]] = []
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
            diagnostics.append(
                _constraint_diagnostic(
                    constraint_id=("asset_weight_floor" if capped > before else "asset_weight_cap"),
                    asset_or_sleeve=symbol,
                    before=before,
                    after=capped,
                    reason="Configured asset or single-asset portfolio bound applied.",
                )
            )

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
        before_total = sum(weights.get(symbol, 0.0) for symbol in group_symbols)
        if _scale_group(weights, group_symbols, cap):
            applied.append(f"{group.upper()}_GROUP_CAP")
            after_total = sum(weights.get(symbol, 0.0) for symbol in group_symbols)
            diagnostics.append(
                _constraint_diagnostic(
                    constraint_id=(
                        "semiconductor_sleeve_cap" if group == "semiconductor" else "risk_group_cap"
                    ),
                    asset_or_sleeve=group,
                    before=before_total,
                    after=after_total,
                    reason="Configured sleeve/risk-group cap applied.",
                )
            )

    equity_symbols = [symbol for symbol in assets.assets if symbol != "CASH"]
    before_equity = sum(weights.get(symbol, 0.0) for symbol in equity_symbols)
    if _scale_group(weights, equity_symbols, constraints.equity_cap):
        applied.append("REGIME_EQUITY_CAP")
        diagnostics.append(
            _constraint_diagnostic(
                constraint_id="regime_equity_cap",
                asset_or_sleeve="equity",
                before=before_equity,
                after=sum(weights.get(symbol, 0.0) for symbol in equity_symbols),
                reason=f"Configured equity cap for regime {regime} applied.",
            )
        )

    cash_min = max(constraints.cash_min, portfolio_constraints.min_cash_weight)
    max_non_cash = max(0.0, 1.0 - cash_min)
    before_non_cash = sum(weights.get(symbol, 0.0) for symbol in equity_symbols)
    if _scale_group(weights, equity_symbols, max_non_cash):
        applied.append("REGIME_CASH_MIN")
        after_non_cash = sum(weights.get(symbol, 0.0) for symbol in equity_symbols)
        diagnostics.append(
            _constraint_diagnostic(
                constraint_id="cash_min_weight",
                asset_or_sleeve="CASH",
                before=1.0 - before_non_cash,
                after=1.0 - after_non_cash,
                reason=f"Configured cash minimum for regime {regime} applied.",
            )
        )

    cash = 1.0 - sum(weights.get(symbol, 0.0) for symbol in equity_symbols)
    weights["CASH"] = max(0.0, min(1.0, cash))
    _normalize_cash(weights, equity_symbols)
    return weights, sorted(set(applied)), diagnostics


def _apply_rebalance_controls(
    weights: dict[str, float],
    previous_weights: dict[str, float],
    min_delta: float,
    max_trade_weight: float,
    max_turnover: float,
) -> tuple[dict[str, float], list[str], list[dict[str, object]]]:
    adjusted, applied, diagnostics = _apply_rebalance_delta(
        weights,
        previous_weights,
        min_delta,
    )
    adjusted, trade_applied, trade_diagnostics = _apply_max_rebalance_trade_weight(
        adjusted,
        previous_weights,
        max_trade_weight,
    )
    adjusted, turnover_applied, turnover_diagnostics = _apply_max_daily_turnover(
        adjusted,
        previous_weights,
        max_turnover,
    )
    applied.extend(trade_applied)
    applied.extend(turnover_applied)
    diagnostics.extend(trade_diagnostics)
    diagnostics.extend(turnover_diagnostics)
    return adjusted, sorted(set(applied)), diagnostics


def _apply_rebalance_delta(
    weights: dict[str, float],
    previous_weights: dict[str, float],
    min_delta: float,
) -> tuple[dict[str, float], list[str], list[dict[str, object]]]:
    if min_delta <= 0:
        return weights, [], []
    adjusted = dict(weights)
    changed = False
    diagnostics: list[dict[str, object]] = []
    non_cash = [symbol for symbol in adjusted if symbol != "CASH"]
    for symbol in non_cash:
        previous = previous_weights.get(symbol)
        if previous is None:
            continue
        if abs(adjusted[symbol] - previous) < min_delta:
            before = adjusted[symbol]
            adjusted[symbol] = previous
            changed = True
            diagnostics.append(
                _constraint_diagnostic(
                    constraint_id="min_rebalance_delta",
                    asset_or_sleeve=symbol,
                    before=before,
                    after=previous,
                    reason="Configured minimum rebalance delta suppressed small trade.",
                )
            )
    if changed:
        adjusted["CASH"] = 1.0 - sum(adjusted.get(symbol, 0.0) for symbol in non_cash)
        if adjusted["CASH"] < 0:
            _scale_group(adjusted, non_cash, 1.0)
            adjusted["CASH"] = 0.0
        _normalize_cash(adjusted, non_cash)
        return adjusted, ["MIN_REBALANCE_DELTA"], diagnostics
    return weights, [], []


def _apply_max_rebalance_trade_weight(
    weights: dict[str, float],
    previous_weights: dict[str, float],
    max_trade_weight: float,
) -> tuple[dict[str, float], list[str], list[dict[str, object]]]:
    if max_trade_weight <= 0:
        return weights, [], []
    adjusted = dict(weights)
    diagnostics: list[dict[str, object]] = []
    non_cash = [symbol for symbol in adjusted if symbol != "CASH"]
    for symbol in non_cash:
        previous = previous_weights.get(symbol, 0.0)
        target = adjusted.get(symbol, 0.0)
        delta = target - previous
        clipped_delta = max(-max_trade_weight, min(max_trade_weight, delta))
        if clipped_delta == delta:
            continue
        after = previous + clipped_delta
        adjusted[symbol] = after
        diagnostics.append(
            _constraint_diagnostic(
                constraint_id="max_rebalance_trade_weight",
                asset_or_sleeve=symbol,
                before=target,
                after=after,
                reason="Configured maximum single rebalance trade weight applied.",
            )
        )
    if not diagnostics:
        return weights, [], []
    adjusted["CASH"] = 1.0 - sum(adjusted.get(symbol, 0.0) for symbol in non_cash)
    _normalize_cash(adjusted, non_cash)
    return adjusted, ["MAX_REBALANCE_TRADE_WEIGHT"], diagnostics


def _apply_max_daily_turnover(
    weights: dict[str, float],
    previous_weights: dict[str, float],
    max_turnover: float,
) -> tuple[dict[str, float], list[str], list[dict[str, object]]]:
    if max_turnover <= 0:
        return weights, [], []
    before_turnover = _turnover(weights, previous_weights)
    if before_turnover <= max_turnover + 1e-12:
        return weights, [], []
    non_cash = [symbol for symbol in weights if symbol != "CASH"]
    low = 0.0
    high = 1.0
    adjusted = dict(weights)
    for _ in range(40):
        factor = (low + high) / 2.0
        candidate = _scaled_rebalance_candidate(weights, previous_weights, non_cash, factor)
        if _turnover(candidate, previous_weights) <= max_turnover:
            low = factor
            adjusted = candidate
        else:
            high = factor
    after_turnover = _turnover(adjusted, previous_weights)
    return (
        adjusted,
        ["MAX_DAILY_TURNOVER"],
        [
            _constraint_diagnostic(
                constraint_id="max_daily_turnover",
                asset_or_sleeve="portfolio",
                before=before_turnover,
                after=after_turnover,
                reason="Configured maximum daily turnover applied.",
            )
        ],
    )


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


def _constraint_diagnostic(
    *,
    constraint_id: str,
    asset_or_sleeve: str,
    before: float,
    after: float,
    reason: str,
    severity: str = "info",
) -> dict[str, object]:
    return {
        "constraint_id": constraint_id,
        "asset_or_sleeve": asset_or_sleeve,
        "before_weight": round(float(before), 10),
        "after_weight": round(float(after), 10),
        "reason": reason,
        "severity": severity,
    }


def _dedupe_diagnostics(
    diagnostics: list[dict[str, object]],
) -> list[dict[str, object]]:
    deduped: list[dict[str, object]] = []
    seen: set[tuple[object, object, object, object]] = set()
    for diagnostic in diagnostics:
        key = (
            diagnostic.get("constraint_id"),
            diagnostic.get("asset_or_sleeve"),
            diagnostic.get("before_weight"),
            diagnostic.get("after_weight"),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(diagnostic)
    return deduped


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


def _scaled_rebalance_candidate(
    weights: dict[str, float],
    previous_weights: dict[str, float],
    non_cash_symbols: list[str],
    factor: float,
) -> dict[str, float]:
    candidate = dict(weights)
    for symbol in non_cash_symbols:
        previous = previous_weights.get(symbol, 0.0)
        target = weights.get(symbol, 0.0)
        candidate[symbol] = previous + (target - previous) * factor
    _normalize_cash(candidate, non_cash_symbols)
    return candidate


def _turnover(
    target_weights: dict[str, float],
    previous_weights: dict[str, float] | None,
) -> float:
    if previous_weights is None:
        return sum(abs(value) for key, value in target_weights.items() if key != "CASH")
    symbols = set(target_weights) | set(previous_weights)
    return sum(
        abs(target_weights.get(symbol, 0.0) - previous_weights.get(symbol, 0.0))
        for symbol in symbols
    )


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
