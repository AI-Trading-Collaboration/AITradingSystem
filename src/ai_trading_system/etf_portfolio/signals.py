from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.etf_portfolio.features import FEATURE_VERSION, select_features_for_date
from ai_trading_system.etf_portfolio.models import ETFSignalRecord, ETFStrategyConfig


def generate_signals_for_date(
    features: pd.DataFrame,
    *,
    strategy: ETFStrategyConfig,
    run_date: date,
) -> list[ETFSignalRecord]:
    selected = select_features_for_date(features, run_date)
    records: list[ETFSignalRecord] = []
    for _, row in selected.sort_values("symbol").iterrows():
        symbol = str(row["symbol"])
        if symbol == "CASH":
            continue
        trend_score, trend_reasons = trend_score_for_row(row)
        momentum_score, momentum_reasons = momentum_score_for_row(row, strategy)
        rs_score, rs_reasons = relative_strength_score_for_row(row, strategy)
        risk_score, risk_reasons = risk_score_for_row(row, strategy)
        composite = (
            strategy.scores["trend"].weight * trend_score
            + strategy.scores["momentum"].weight * momentum_score
            + strategy.scores["relative_strength"].weight * rs_score
            + strategy.scores["risk"].weight * risk_score
        )
        direction, confidence = direction_and_confidence(composite)
        records.append(
            ETFSignalRecord(
                date=run_date,
                symbol=symbol,
                trend_score=round(_clamp_score(trend_score), 4),
                momentum_score=round(_clamp_score(momentum_score), 4),
                relative_strength_score=round(_clamp_score(rs_score), 4),
                risk_score=round(_clamp_score(risk_score), 4),
                composite_score=round(_clamp_score(composite), 4),
                direction=direction,
                confidence=confidence,
                reason_codes=tuple(
                    [*trend_reasons, *momentum_reasons, *rs_reasons, *risk_reasons]
                ),
                model_version=strategy.model.version,
                feature_version=FEATURE_VERSION,
                created_at=datetime.now(UTC),
            )
        )
    return records


def signals_to_frame(signals: list[ETFSignalRecord]) -> pd.DataFrame:
    return pd.DataFrame([record.to_record() for record in signals])


def write_signals(signals: list[ETFSignalRecord], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    signals_to_frame(signals).to_csv(path, index=False)
    return path


def load_signals(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"ETF signals file does not exist: {path}")
    return pd.read_csv(path)


def select_signals_for_date(signals: pd.DataFrame, run_date: date) -> pd.DataFrame:
    frame = signals.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    selected = frame.loc[frame["_date"] == pd.Timestamp(run_date)].copy()
    if selected.empty:
        raise ValueError(f"ETF signals have no rows for {run_date.isoformat()}")
    return selected.drop(columns=["_date"])


def trend_score_for_row(row: pd.Series) -> tuple[float, list[str]]:
    score = 0.0
    reasons: list[str] = []
    for window in (20, 50, 100, 200):
        value = _optional_bool(row.get(f"above_ma_{window}"))
        if value is None:
            reasons.append(f"INSUFFICIENT_MA_{window}D")
            continue
        if value:
            score += 25.0
            reasons.append(f"PRICE_ABOVE_{window}D_MA")
        else:
            reasons.append(f"PRICE_BELOW_{window}D_MA")
    slope = _optional_float(row.get("ma_20_slope"))
    if slope is None:
        reasons.append("INSUFFICIENT_MA_20D_SLOPE")
    elif slope > 0:
        score += 5.0
        reasons.append("MA_20D_SLOPE_POSITIVE")
    elif slope < 0:
        score -= 5.0
        reasons.append("MA_20D_SLOPE_NEGATIVE")
    return _clamp_score(score), reasons


def momentum_score_for_row(
    row: pd.Series,
    strategy: ETFStrategyConfig,
) -> tuple[float, list[str]]:
    weights = {20: 0.40, 60: 0.40, 120: 0.20}
    scores: list[tuple[float, float]] = []
    reasons: list[str] = []
    for window in strategy.momentum_features.return_windows:
        value = _optional_float(row.get(f"ret_{window}d"))
        if value is None:
            reasons.append(f"INSUFFICIENT_RET_{window}D")
            continue
        scores.append((score_return(value, strategy), weights.get(window, 1.0)))
        reasons.append(_return_reason("MOMENTUM", window, value))
    if not scores:
        return 50.0, [*reasons, "MOMENTUM_NEUTRAL_INSUFFICIENT_DATA"]
    total_weight = sum(weight for _, weight in scores)
    return sum(score * weight for score, weight in scores) / total_weight, reasons


def relative_strength_score_for_row(
    row: pd.Series,
    strategy: ETFStrategyConfig,
) -> tuple[float, list[str]]:
    symbol = str(row["symbol"])
    fields = {
        "SPY": [],
        "QQQ": ["rs_vs_spy_60d"],
        "SMH": ["rs_vs_qqq_60d", "rs_vs_spy_60d"],
        "SOXX": ["rs_vs_qqq_60d", "rs_vs_smh_60d"],
    }.get(symbol, [])
    if not fields:
        return 50.0, ["RELATIVE_STRENGTH_NEUTRAL_BASELINE"]
    values: list[float] = []
    reasons: list[str] = []
    for field in fields:
        value = _optional_float(row.get(field))
        if value is None:
            reasons.append(f"INSUFFICIENT_{field.upper()}")
            continue
        values.append(score_return(value, strategy))
        reasons.append(_rs_reason(symbol, field, value))
    if not values:
        return 50.0, [*reasons, "RELATIVE_STRENGTH_NEUTRAL_INSUFFICIENT_DATA"]
    return sum(values) / len(values), reasons


def risk_score_for_row(
    row: pd.Series,
    strategy: ETFStrategyConfig,
) -> tuple[float, list[str]]:
    mapping = strategy.score_mapping
    score = 100.0
    reasons: list[str] = []
    vol = _optional_float(row.get("realized_vol_20d"))
    if vol is None:
        reasons.append("INSUFFICIENT_VOL_20D")
    else:
        penalty = _linear_penalty(vol, mapping.vol_low, mapping.vol_high, mapping.vol_max_penalty)
        score -= penalty
        reasons.append("VOLATILITY_STABLE" if penalty == 0 else "VOLATILITY_ELEVATED")
    drawdown = _optional_float(row.get("drawdown_63d"))
    if drawdown is None:
        reasons.append("INSUFFICIENT_DRAWDOWN_63D")
    else:
        penalty = _drawdown_penalty(
            drawdown,
            mapping.drawdown_low,
            mapping.drawdown_high,
            mapping.drawdown_max_penalty,
        )
        score -= penalty
        reasons.append("DRAWDOWN_CONTAINED" if penalty == 0 else "DRAWDOWN_EXTENDED")
    above_ma_200 = _optional_bool(row.get("above_ma_200"))
    if above_ma_200 is False:
        score -= mapping.below_ma_200_penalty
        reasons.append("PRICE_BELOW_200D_MA_RISK_PENALTY")
    elif above_ma_200 is True:
        reasons.append("PRICE_ABOVE_200D_MA")
    else:
        reasons.append("INSUFFICIENT_MA_200D_RISK")
    return _clamp_score(score), reasons


def score_return(value: float, strategy: ETFStrategyConfig) -> float:
    floor = strategy.score_mapping.return_score_floor
    ceiling = strategy.score_mapping.return_score_ceiling
    if value <= floor:
        return 0.0
    if value >= ceiling:
        return 100.0
    return (value - floor) / (ceiling - floor) * 100.0


def direction_and_confidence(score: float) -> tuple[str, str]:
    if score >= 75:
        return "bullish", "high"
    if score >= 60:
        return "bullish", "medium"
    if score >= 45:
        return "neutral", "medium"
    if score >= 30:
        return "bearish", "medium"
    return "bearish", "high"


def _linear_penalty(value: float, low: float, high: float, max_penalty: float) -> float:
    if value <= low:
        return 0.0
    if value >= high:
        return max_penalty
    return (value - low) / (high - low) * max_penalty


def _drawdown_penalty(value: float, low: float, high: float, max_penalty: float) -> float:
    if value >= low:
        return 0.0
    if value <= high:
        return max_penalty
    return (low - value) / (low - high) * max_penalty


def _return_reason(prefix: str, window: int, value: float) -> str:
    if value > 0:
        return f"{prefix}_{window}D_POSITIVE"
    if value < 0:
        return f"{prefix}_{window}D_NEGATIVE"
    return f"{prefix}_{window}D_FLAT"


def _rs_reason(symbol: str, field: str, value: float) -> str:
    cleaned = field.upper()
    if value > 0:
        return f"{symbol}_{cleaned}_OUTPERFORMS"
    if value < 0:
        return f"{symbol}_{cleaned}_UNDERPERFORMS"
    return f"{symbol}_{cleaned}_FLAT"


def _optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _optional_bool(value: Any) -> bool | None:
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "1.0"}:
        return True
    if text in {"false", "0", "0.0"}:
        return False
    return None


def _clamp_score(value: float) -> float:
    return min(100.0, max(0.0, float(value)))
