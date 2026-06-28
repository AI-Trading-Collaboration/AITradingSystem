from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd


def build_event_risk_free_features(
    calendar_features: pd.DataFrame,
    rates_features: pd.DataFrame,
    volatility_features: pd.DataFrame,
    policy: Mapping[str, Any],
) -> pd.DataFrame:
    section = _mapping(policy.get("event_risk_free_v1"))
    lookahead = int(section.get("lookahead_days", 5))
    lookback = int(section.get("lookback_days", 5))
    event_weights = _mapping(section.get("event_weights"))
    overlay_weights = _mapping(section.get("overlay_weights"))
    score_cap = float(section.get("score_cap", 3.0))

    base_dates = _date_frame(calendar_features, rates_features, volatility_features)
    if base_dates.empty:
        return _empty_frame()
    out = base_dates.copy()
    if not calendar_features.empty:
        cal = calendar_features.copy()
        cal["date"] = pd.to_datetime(cal["date"], errors="coerce")
        out = out.merge(cal, on="date", how="left", suffixes=("", "_calendar"))
    out["calendar_event_risk_score"] = out.apply(
        lambda row: _calendar_score(row, event_weights, lookahead=lookahead, lookback=lookback),
        axis=1,
    )

    if not rates_features.empty:
        rates = rates_features[["date", "rate_stress_score"]].copy()
        rates["date"] = pd.to_datetime(rates["date"], errors="coerce")
        out = out.merge(rates, on="date", how="left")
    if not volatility_features.empty:
        vol_columns = [column for column in ("date", "vol_regime") if column in volatility_features]
        vol = volatility_features[vol_columns].copy()
        vol["date"] = pd.to_datetime(vol["date"], errors="coerce")
        out = out.merge(vol, on="date", how="left")
    rate_overlay = out.get("rate_stress_score", pd.Series(index=out.index)).fillna(0).clip(0, 1)
    vix_overlay = (out.get("vol_regime", pd.Series(index=out.index)).fillna("") == "stress").astype(
        float
    )
    out["event_risk_score"] = (
        out["calendar_event_risk_score"].fillna(0)
        + float(overlay_weights.get("rate_stress", 0.25)) * rate_overlay
        + float(overlay_weights.get("vix_stress", 0.25)) * vix_overlay
    ).clip(upper=score_cap)
    out["feature_family"] = "event_risk_free_v1"
    out["policy_version"] = str(policy.get("schema_version", "free_feature_policy.v1"))
    out["known_at"] = out["date"].dt.strftime("%Y-%m-%d")
    out["available_at"] = out["known_at"]
    out["decision_at"] = out["known_at"]
    out["PIT_status"] = (
        "PIT_WARNING_DIAGNOSTIC_ONLY"
        if "PIT_WARNING" in set(out.get("PIT_status", pd.Series(dtype=str)).dropna().astype(str))
        else "PIT_APPROVED"
    )
    out["allowed_usage"] = "event_risk_diagnostic"
    out["blocked_usage"] = "promotion,paper_shadow,production,broker"
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    keep = [
        "date",
        "feature_family",
        "policy_version",
        "calendar_event_risk_score",
        "event_risk_score",
        "known_at",
        "available_at",
        "decision_at",
        "PIT_status",
        "allowed_usage",
        "blocked_usage",
    ]
    return out[[column for column in keep if column in out.columns]].sort_values("date")


def _calendar_score(
    row: pd.Series,
    weights: Mapping[str, Any],
    *,
    lookahead: int,
    lookback: int,
) -> float:
    score = 0.0
    mapping = {
        "fomc": "fomc",
        "inflation": "cpi",
        "labor": "payrolls",
        "growth": "gdp",
    }
    for weight_name, prefix in mapping.items():
        days_to = row.get(f"days_to_{prefix}")
        days_since = row.get(f"days_since_{prefix}")
        proximity = _proximity(days_to, lookahead, days_since, lookback)
        score += float(weights.get(weight_name, 0.0)) * proximity
    pce_proximity = _proximity(
        row.get("days_to_pce"),
        lookahead,
        row.get("days_since_pce"),
        lookback,
    )
    score += float(weights.get("inflation", 0.0)) * pce_proximity
    return score


def _proximity(
    days_to: object,
    lookahead: int,
    days_since: object,
    lookback: int,
) -> float:
    candidates: list[float] = []
    for value, window in ((days_to, lookahead), (days_since, lookback)):
        if pd.isna(value):
            continue
        distance = abs(float(value))
        if distance <= window:
            candidates.append(1.0 - distance / max(window, 1))
    return max(candidates) if candidates else 0.0


def _date_frame(*frames: pd.DataFrame) -> pd.DataFrame:
    dates: list[pd.Series] = []
    for frame in frames:
        if not frame.empty and "date" in frame.columns:
            dates.append(pd.to_datetime(frame["date"], errors="coerce"))
    if not dates:
        return pd.DataFrame(columns=["date"])
    joined = pd.concat(dates).dropna().drop_duplicates().sort_values()
    return pd.DataFrame({"date": joined})


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "date",
            "feature_family",
            "policy_version",
            "calendar_event_risk_score",
            "event_risk_score",
            "known_at",
            "available_at",
            "decision_at",
            "PIT_status",
            "allowed_usage",
            "blocked_usage",
        ]
    )
