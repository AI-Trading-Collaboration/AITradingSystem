from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd


def build_volatility_compression_free_features(
    vix_history: pd.DataFrame,
    prices: pd.DataFrame,
    policy: Mapping[str, Any],
) -> pd.DataFrame:
    section = _mapping(policy.get("volatility_compression_free_v1"))
    realized_windows = [
        int(item) for item in section.get("realized_vol_windows_trading_days", [10, 20, 60])
    ]
    vix_windows = [int(item) for item in section.get("vix_change_windows_trading_days", [5, 20])]
    percentile_lookback = int(section.get("vix_percentile_lookback_days", 252))
    compression_max = float(section.get("compression_percentile_max", 0.25))
    stress_min = float(section.get("stress_percentile_min", 0.75))

    if vix_history.empty:
        return _empty_frame()
    vix = vix_history.copy()
    if "vix_level" not in vix.columns:
        raise ValueError("vix history must include vix_level")
    vix["date"] = pd.to_datetime(vix["date"], errors="coerce")
    vix["vix_level"] = pd.to_numeric(vix["vix_level"], errors="coerce")
    vix = vix.loc[vix["date"].notna() & vix["vix_level"].notna()].sort_values("date")
    for window in vix_windows:
        vix[f"vix_change_{window}d"] = vix["vix_level"].diff(window)
    vix["vix_percentile"] = (
        vix["vix_level"]
        .rolling(percentile_lookback, min_periods=max(20, min(percentile_lookback, 20)))
        .apply(_last_percentile, raw=False)
    )
    vix["vol_compression_score"] = (compression_max - vix["vix_percentile"]).clip(lower=0)
    vix["vol_regime"] = "normal"
    vix.loc[vix["vix_percentile"] <= compression_max, "vol_regime"] = "compression"
    vix.loc[vix["vix_percentile"] >= stress_min, "vol_regime"] = "stress"

    qqq = _qqq_realized_vol(prices, realized_windows)
    features = vix.merge(qqq, on="date", how="left")
    if {"realized_vol_20d", "realized_vol_60d"} <= set(features.columns):
        features["downside_vol_contraction"] = (
            features["realized_vol_20d"] < features["realized_vol_60d"]
        )
    else:
        features["downside_vol_contraction"] = False
    features["feature_family"] = "volatility_compression_free_v1"
    features["policy_version"] = str(policy.get("schema_version", "free_feature_policy.v1"))
    features["known_at"] = features["date"].dt.strftime("%Y-%m-%d")
    features["available_at"] = features["known_at"]
    features["decision_at"] = features["known_at"]
    features["PIT_status"] = "PIT_APPROVED"
    features["allowed_usage"] = "risk_on_veto_research,stress_detection"
    features["blocked_usage"] = "promotion,paper_shadow,production,broker"
    features["date"] = features["date"].dt.strftime("%Y-%m-%d")
    return features.sort_values("date").reset_index(drop=True)


def _qqq_realized_vol(prices: pd.DataFrame, windows: list[int]) -> pd.DataFrame:
    required = {"date", "ticker", "adj_close"}
    if prices.empty or not required <= set(prices.columns):
        return pd.DataFrame(columns=["date"])
    qqq = prices.loc[prices["ticker"] == "QQQ"].copy()
    if qqq.empty:
        return pd.DataFrame(columns=["date"])
    qqq["date"] = pd.to_datetime(qqq["date"], errors="coerce")
    qqq["adj_close"] = pd.to_numeric(qqq["adj_close"], errors="coerce")
    qqq = qqq.loc[qqq["date"].notna() & qqq["adj_close"].notna()].sort_values("date")
    qqq["return"] = qqq["adj_close"].pct_change()
    for window in windows:
        qqq[f"realized_vol_{window}d"] = qqq["return"].rolling(window).std() * np.sqrt(252)
    return qqq[["date", *[f"realized_vol_{window}d" for window in windows]]]


def _last_percentile(values: pd.Series) -> float:
    if values.empty:
        return float("nan")
    return float(values.rank(pct=True).iloc[-1])


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "date",
            "feature_family",
            "policy_version",
            "known_at",
            "available_at",
            "decision_at",
            "PIT_status",
            "allowed_usage",
            "blocked_usage",
        ]
    )
