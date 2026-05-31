from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from ai_trading_system.etf_portfolio.features import select_features_for_date
from ai_trading_system.etf_portfolio.models import ETFRegimeRecord, ETFRiskConfig, ETFStrategyConfig
from ai_trading_system.etf_portfolio.signals import select_signals_for_date


def generate_regime_for_date(
    features: pd.DataFrame,
    signals: pd.DataFrame,
    *,
    strategy: ETFStrategyConfig,
    risk: ETFRiskConfig,
    run_date: date,
    previous_regime: str | None = None,
) -> ETFRegimeRecord:
    feature_rows = _rows_by_symbol(select_features_for_date(features, run_date))
    signal_rows = _rows_by_symbol(select_signals_for_date(signals, run_date))
    rules = risk.regime_rules
    reasons: list[str] = []

    spy = feature_rows.get("SPY")
    qqq = feature_rows.get("QQQ")
    smh = feature_rows.get("SMH")
    spy_signal = signal_rows.get("SPY")
    qqq_signal = signal_rows.get("QQQ")

    spy_above_200 = _bool_value(spy, "above_ma_200")
    qqq_above_200 = _bool_value(qqq, "above_ma_200")
    spy_dd_63 = _float_value(spy, "drawdown_63d")
    qqq_dd_63 = _float_value(qqq, "drawdown_63d")
    qqq_ret_20 = _float_value(qqq, "ret_20d")
    smh_ret_20 = _float_value(smh, "ret_20d")
    qqq_distance_ma_50 = _float_value(qqq, "pct_vs_ma_50")
    smh_dd_63 = _float_value(smh, "drawdown_63d")
    spy_trend = _float_value(spy_signal, "trend_score")
    qqq_trend = _float_value(qqq_signal, "trend_score")
    qqq_rs_spy = _float_value(qqq, "rs_vs_spy_60d")
    smh_rs_qqq = _float_value(smh, "rs_vs_qqq_60d")

    risk_off_condition = (
        (spy_above_200 is False and qqq_above_200 is False)
        or (
            spy_dd_63 is not None
            and qqq_dd_63 is not None
            and spy_dd_63 <= rules.risk_off_spy_drawdown_63d
            and qqq_dd_63 <= rules.risk_off_qqq_drawdown_63d
        )
    )
    risk_on_condition = (
        spy_above_200 is True
        and qqq_above_200 is True
        and qqq_trend is not None
        and spy_trend is not None
        and qqq_trend >= rules.risk_on_qqq_trend_min
        and spy_trend >= rules.risk_on_spy_trend_min
    )
    shock_recovery_condition = (
        previous_regime == "Risk-Off"
        and _bool_value(qqq, "above_ma_20") is True
        and qqq_ret_20 is not None
        and qqq_ret_20 > 0
        and qqq_dd_63 is not None
        and qqq_dd_63 > rules.risk_off_qqq_drawdown_63d
    )
    overheated_condition = (
        (
            _bool_value(qqq, "above_ma_50") is True
            and qqq_ret_20 is not None
            and qqq_distance_ma_50 is not None
            and qqq_ret_20 >= rules.overheated_qqq_ret_20d
            and qqq_distance_ma_50 >= rules.overheated_qqq_distance_ma_50
        )
        or (
            smh_ret_20 is not None
            and smh_dd_63 is not None
            and smh_ret_20 >= rules.overheated_smh_ret_20d
            and smh_dd_63 >= rules.overheated_smh_drawdown_63d_min
        )
    )

    if risk_off_condition:
        regime = "Risk-Off"
        reasons.append("SPY_QQQ_RISK_OFF_TREND_OR_DRAWDOWN")
    elif shock_recovery_condition:
        regime = "Shock-Recovery"
        reasons.append("PREVIOUS_RISK_OFF_QQQ_RECOVERY")
    elif overheated_condition:
        regime = "Overheated"
        reasons.append("QQQ_OR_SMH_EXTENDED")
    elif risk_on_condition:
        regime = "Risk-On"
        reasons.append("SPY_QQQ_ABOVE_200D_WITH_STRONG_TREND")
    else:
        regime = "Neutral"
        reasons.append("NEUTRAL_FALLBACK")

    risk_on_score = _score_bool(risk_on_condition)
    risk_off_score = _score_bool(risk_off_condition)
    growth_leadership_score = _leadership_score(qqq_rs_spy)
    semiconductor_leadership_score = _leadership_score(smh_rs_qqq)
    if qqq_rs_spy is not None and qqq_rs_spy > 0:
        reasons.append("QQQ_OUTPERFORMS_SPY_60D")
    if smh_rs_qqq is not None and smh_rs_qqq > 0:
        reasons.append("SMH_OUTPERFORMS_QQQ_60D")
    regime_score = _regime_score(regime, risk_on_score, risk_off_score, growth_leadership_score)
    return ETFRegimeRecord(
        date=run_date,
        regime=regime,
        regime_score=round(regime_score, 4),
        risk_on_score=risk_on_score,
        risk_off_score=risk_off_score,
        growth_leadership_score=round(growth_leadership_score, 4),
        semiconductor_leadership_score=round(semiconductor_leadership_score, 4),
        reason_codes=tuple(reasons),
        model_version=strategy.model.version,
        created_at=datetime.now(UTC),
    )


def regime_to_frame(regime: ETFRegimeRecord) -> pd.DataFrame:
    return pd.DataFrame([regime.to_record()])


def write_regime(regime: ETFRegimeRecord, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    regime_to_frame(regime).to_csv(path, index=False)
    return path


def load_regimes(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"ETF regimes file does not exist: {path}")
    return pd.read_csv(path)


def select_regime_for_date(regimes: pd.DataFrame, run_date: date) -> pd.Series:
    frame = regimes.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    selected = frame.loc[frame["_date"] == pd.Timestamp(run_date)].copy()
    if selected.empty:
        raise ValueError(f"ETF regimes have no row for {run_date.isoformat()}")
    return selected.iloc[-1]


def _rows_by_symbol(frame: pd.DataFrame) -> dict[str, pd.Series]:
    return {str(row["symbol"]): row for _, row in frame.iterrows() if "symbol" in row}


def _float_value(row: pd.Series | None, field: str) -> float | None:
    if row is None or field not in row:
        return None
    try:
        value = float(row[field])
    except (TypeError, ValueError):
        return None
    if pd.isna(value):
        return None
    return value


def _bool_value(row: pd.Series | None, field: str) -> bool | None:
    if row is None or field not in row or pd.isna(row[field]):
        return None
    text = str(row[field]).strip().lower()
    if text in {"true", "1", "1.0"}:
        return True
    if text in {"false", "0", "0.0"}:
        return False
    return None


def _score_bool(value: bool) -> float:
    return 100.0 if value else 0.0


def _leadership_score(value: float | None) -> float:
    if value is None:
        return 50.0
    return max(0.0, min(100.0, (value + 0.10) / 0.20 * 100.0))


def _regime_score(
    regime: str,
    risk_on_score: float,
    risk_off_score: float,
    growth_leadership_score: float,
) -> float:
    if regime == "Risk-Off":
        return 100.0 - risk_off_score
    if regime == "Risk-On":
        return (risk_on_score + growth_leadership_score) / 2.0
    if regime == "Overheated":
        return 70.0
    if regime == "Shock-Recovery":
        return 60.0
    return 50.0
