from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd


def build_rates_liquidity_free_features(
    fred_market_series: pd.DataFrame,
    policy: Mapping[str, Any],
) -> pd.DataFrame:
    section = _mapping(policy.get("rates_liquidity_free_v1"))
    windows = [int(item) for item in section.get("change_windows_trading_days", [5, 20, 60])]
    short_rate_threshold = float(section.get("short_rate_shock_abs_20d_bps", 25)) / 100.0
    dollar_threshold = float(section.get("dollar_strength_shock_abs_20d_pct", 2.5)) / 100.0
    components = _mapping(section.get("rate_stress_components"))

    if fred_market_series.empty:
        return _empty_frame()
    required = {"date", "series", "value"}
    missing = required - set(fred_market_series.columns)
    if missing:
        raise ValueError(
            f"fred market series missing columns for rates features: {sorted(missing)}"
        )

    frame = fred_market_series.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    frame = frame.loc[frame["date"].notna() & frame["value"].notna()].copy()
    wide = frame.pivot_table(index="date", columns="series", values="value", aggfunc="last")
    wide = wide.sort_index()

    for series in ("DGS2", "DGS10", "DGS3MO", "FEDFUNDS", "SOFR", "DTWEXBGS"):
        if series not in wide.columns:
            wide[series] = pd.NA
    for series in ("DGS2", "DGS10", "DGS3MO", "DTWEXBGS"):
        for window in windows:
            if series == "DTWEXBGS":
                wide[f"{series.lower()}_pct_change_{window}d"] = wide[series].pct_change(window)
            else:
                wide[f"{series.lower()}_change_{window}d"] = wide[series].diff(window)
    wide["dgs2_dgs10_spread"] = wide["DGS10"] - wide["DGS2"]
    wide["dgs3mo_dgs10_spread"] = wide["DGS10"] - wide["DGS3MO"]
    wide["short_rate_shock_flag"] = (
        wide["dgs2_change_20d"].abs() >= short_rate_threshold
        if "dgs2_change_20d" in wide
        else False
    )
    wide["dollar_strength_shock_flag"] = (
        wide["dtwexbgs_pct_change_20d"].abs() >= dollar_threshold
        if "dtwexbgs_pct_change_20d" in wide
        else False
    )
    dgs2_weight = float(components.get("dgs2_change_20d_weight", 0.4))
    dgs10_weight = float(components.get("dgs10_change_20d_weight", 0.3))
    dollar_weight = float(components.get("dollar_change_20d_weight", 0.3))
    dgs2_change = wide.get("dgs2_change_20d", pd.Series(index=wide.index))
    dgs10_change = wide.get("dgs10_change_20d", pd.Series(index=wide.index))
    dgs2_component = (dgs2_change.abs() / short_rate_threshold).fillna(0)
    dgs10_component = (dgs10_change.abs() / short_rate_threshold).fillna(0)
    dollar_component = (
        wide.get("dtwexbgs_pct_change_20d", pd.Series(index=wide.index)).abs()
        / dollar_threshold
    ).fillna(0)
    wide["rate_stress_score"] = (
        dgs2_weight * dgs2_component
        + dgs10_weight * dgs10_component
        + dollar_weight * dollar_component
    )
    wide["feature_family"] = "rates_liquidity_free_v1"
    wide["policy_version"] = str(policy.get("schema_version", "free_feature_policy.v1"))
    wide["known_at"] = wide.index.strftime("%Y-%m-%d")
    wide["available_at"] = wide["known_at"]
    wide["decision_at"] = wide["known_at"]
    wide["PIT_status"] = "PIT_APPROVED"
    wide["allowed_usage"] = "risk_on_veto_research,macro_liquidity_context"
    wide["blocked_usage"] = "promotion,paper_shadow,production,broker"
    wide = wide.reset_index()
    wide["date"] = wide["date"].dt.strftime("%Y-%m-%d")
    return wide.sort_values("date").reset_index(drop=True)


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
