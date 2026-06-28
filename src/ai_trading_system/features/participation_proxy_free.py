from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd

from ai_trading_system.post_2085_research_common import mapping, records, round_float

RATIO_SPECS = {
    "qqqe_to_qqq": ("QQQE", "QQQ"),
    "rsp_to_spy": ("RSP", "SPY"),
    "smh_to_qqq": ("SMH", "QQQ"),
    "soxx_to_qqq": ("SOXX", "QQQ"),
    "xlk_to_qqq": ("XLK", "QQQ"),
}


def build_participation_proxy_etf_ratios(
    prices: pd.DataFrame,
    registry: Mapping[str, Any],
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if prices.empty:
        return _empty_ratio_frame(), _coverage_rows({}, registry)
    required = {"date", "ticker", "adj_close"}
    missing = required - set(prices.columns)
    if missing:
        raise ValueError(f"price cache missing columns for ETF ratios: {sorted(missing)}")

    frame = prices.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["date"].notna() & frame["adj_close"].notna()].copy()
    pivot = frame.pivot_table(index="date", columns="ticker", values="adj_close", aggfunc="last")
    pivot = pivot.sort_index().ffill()
    available = set(str(column) for column in pivot.columns)

    result = pd.DataFrame(index=pivot.index)
    result.index.name = "date"
    for proxy_id, (numerator, denominator) in RATIO_SPECS.items():
        ratio_column = f"{proxy_id}_ratio"
        trend_column = f"{proxy_id}_trend_20d"
        if numerator in pivot.columns and denominator in pivot.columns:
            ratio = pivot[numerator] / pivot[denominator]
            result[ratio_column] = ratio
            result[trend_column] = ratio.pct_change(20)
        else:
            result[ratio_column] = pd.NA
            result[trend_column] = pd.NA

    trend_columns = [column for column in result.columns if column.endswith("_trend_20d")]
    result["equal_weight_vs_cap_weight_proxy"] = result["qqqe_to_qqq_trend_20d"]
    result["sector_confirmation_score"] = result[
        ["smh_to_qqq_trend_20d", "soxx_to_qqq_trend_20d", "xlk_to_qqq_trend_20d"]
    ].mean(axis=1, skipna=True)
    result["available_ratio_count"] = result[trend_columns].notna().sum(axis=1)
    result["feature_family"] = "participation_proxy_etf_ratios_v1"
    result["known_at"] = result.index.strftime("%Y-%m-%d")
    result["available_at"] = result["known_at"]
    result["decision_at"] = result["known_at"]
    result["PIT_status"] = "PIT_APPROVED_AS_PRICE_PROXY_NOT_TRUE_BREADTH"
    result["allowed_usage"] = "participation_diagnostic_only"
    result["blocked_usage"] = "model_ready_breadth,promotion,paper_shadow,production,broker"
    result = result.reset_index()
    result["date"] = result["date"].dt.strftime("%Y-%m-%d")
    coverage = _coverage_rows(available, registry)
    return result.sort_values("date").reset_index(drop=True), coverage


def build_participation_proxy_free_v2(ratio_features: pd.DataFrame) -> pd.DataFrame:
    if ratio_features.empty:
        return _empty_v2_frame()
    frame = ratio_features.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    numeric_columns = [
        "qqqe_to_qqq_trend_20d",
        "rsp_to_spy_trend_20d",
        "smh_to_qqq_trend_20d",
        "soxx_to_qqq_trend_20d",
        "xlk_to_qqq_trend_20d",
        "equal_weight_vs_cap_weight_proxy",
        "sector_confirmation_score",
    ]
    for column in numeric_columns:
        if column not in frame.columns:
            frame[column] = pd.NA
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["participation_proxy_score"] = frame[
        [
            "equal_weight_vs_cap_weight_proxy",
            "sector_confirmation_score",
            "rsp_to_spy_trend_20d",
        ]
    ].mean(axis=1, skipna=True)
    frame["participation_proxy_signal"] = frame["participation_proxy_score"].map(
        lambda value: "positive" if round_float(value) > 0 else "neutral_or_missing"
    )
    frame["feature_family"] = "participation_proxy_free_v2"
    frame["PIT_status"] = "PIT_APPROVED_AS_PRICE_PROXY_NOT_TRUE_BREADTH"
    frame["allowed_usage"] = "participation_diagnostic_only"
    frame["blocked_usage"] = "model_ready_breadth,promotion,paper_shadow,production,broker"
    keep = [
        "date",
        "feature_family",
        "participation_proxy_score",
        "participation_proxy_signal",
        "equal_weight_vs_cap_weight_proxy",
        "sector_confirmation_score",
        "available_ratio_count",
        "known_at",
        "available_at",
        "decision_at",
        "PIT_status",
        "allowed_usage",
        "blocked_usage",
    ]
    frame["date"] = frame["date"].dt.strftime("%Y-%m-%d")
    return frame[keep].sort_values("date").reset_index(drop=True)


def _coverage_rows(
    available_tickers: set[str],
    registry: Mapping[str, Any],
) -> list[dict[str, Any]]:
    configured = {
        str(proxy.get("proxy_id")): (
            str(proxy.get("numerator")),
            str(proxy.get("denominator")),
            str(proxy.get("status")),
        )
        for proxy in records(registry.get("proxies"))
    }
    rows: list[dict[str, Any]] = []
    for proxy_id, (numerator, denominator) in RATIO_SPECS.items():
        registry_status = configured.get(proxy_id, (numerator, denominator, "REGISTRY_ONLY"))[2]
        rows.append(
            {
                "proxy_id": proxy_id,
                "numerator": numerator,
                "denominator": denominator,
                "registry_status": registry_status,
                "numerator_available": numerator in available_tickers,
                "denominator_available": denominator in available_tickers,
                "ratio_available": (
                    numerator in available_tickers and denominator in available_tickers
                ),
                "true_pit_breadth": False,
            }
        )
    if "sector_etf_relative_strength" in mapping(registry).get("proxies", {}):
        return rows
    return rows


def _empty_ratio_frame() -> pd.DataFrame:
    columns = [
        "date",
        *[f"{proxy_id}_ratio" for proxy_id in RATIO_SPECS],
        *[f"{proxy_id}_trend_20d" for proxy_id in RATIO_SPECS],
        "equal_weight_vs_cap_weight_proxy",
        "sector_confirmation_score",
        "available_ratio_count",
        "feature_family",
        "known_at",
        "available_at",
        "decision_at",
        "PIT_status",
        "allowed_usage",
        "blocked_usage",
    ]
    return pd.DataFrame(columns=columns)


def _empty_v2_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "date",
            "feature_family",
            "participation_proxy_score",
            "participation_proxy_signal",
            "equal_weight_vs_cap_weight_proxy",
            "sector_confirmation_score",
            "available_ratio_count",
            "known_at",
            "available_at",
            "decision_at",
            "PIT_status",
            "allowed_usage",
            "blocked_usage",
        ]
    )
