"""Bench data: asset returns and signal inputs from a checked long-history snapshot.

The snapshot is read only through ``qqq_long_history.load_daily``, which refuses unchecked, failed
or stale snapshots. Returns on day t cover the previous trading date's close to t's close. Signal
inputs carry only what was known at t's close: the T-bill rate dated t is published the next
business day, so ``cash_rate_known`` on t is the rate dated the previous trading date.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from ai_trading_system.research_data import qqq_long_history as qlh

ASSETS: tuple[str, ...] = ("qqq", "cash", "lev2", "lev3")
# Exposure to the Nasdaq-100 that one unit of each asset carries.
EXPOSURE_FACTORS: dict[str, float] = {"qqq": 1.0, "cash": 0.0, "lev2": 2.0, "lev3": 3.0}
QqqSeries = Literal["proxy", "actual"]
QQQ_SERIES_COLUMNS: dict[str, str] = {
    "proxy": "qqq_proxy_return",
    "actual": "etf_reference_return",
}
INPUT_COLUMNS: tuple[str, ...] = (
    "qqq_return",
    "ndx_price",
    "ndx_tr",
    "vix",
    "vxn",
    "vxo",
    "cash_rate_known",
)


@dataclass(frozen=True)
class BenchData:
    returns: pd.DataFrame
    inputs: pd.DataFrame
    source: dict[str, Any]


def from_daily_frame(
    daily: pd.DataFrame,
    *,
    qqq_series: QqqSeries,
    source: Mapping[str, Any],
) -> BenchData:
    """Asset returns and signal inputs from the derived long-history frame."""

    if qqq_series not in QQQ_SERIES_COLUMNS:
        raise ValueError(f"qqq_series must be one of {sorted(QQQ_SERIES_COLUMNS)}")
    required = {
        QQQ_SERIES_COLUMNS[qqq_series],
        "cash_return",
        "lev2_return",
        "lev3_return",
        "ndx_price",
        "ndx_tr",
        "vix",
        "vxn",
        "vxo",
        "cash_rate_annual",
    }
    missing = sorted(required - set(daily.columns))
    if missing:
        raise ValueError(f"daily frame is missing columns: {missing}")
    index = pd.DatetimeIndex(daily.index, name="date")
    if not index.is_monotonic_increasing or index.has_duplicates:
        raise ValueError("daily frame dates must be unique and increasing")
    qqq = daily[QQQ_SERIES_COLUMNS[qqq_series]].astype(float)
    returns = pd.DataFrame(
        {
            "qqq": qqq.to_numpy(),
            "cash": daily["cash_return"].astype(float).to_numpy(),
            "lev2": daily["lev2_return"].astype(float).to_numpy(),
            "lev3": daily["lev3_return"].astype(float).to_numpy(),
        },
        index=index,
    )
    inputs = pd.DataFrame(
        {
            "qqq_return": qqq.to_numpy(),
            "ndx_price": daily["ndx_price"].astype(float).to_numpy(),
            "ndx_tr": daily["ndx_tr"].astype(float).to_numpy(),
            "vix": daily["vix"].astype(float).to_numpy(),
            "vxn": daily["vxn"].astype(float).to_numpy(),
            "vxo": daily["vxo"].astype(float).to_numpy(),
            "cash_rate_known": daily["cash_rate_annual"].astype(float).shift(1).to_numpy(),
        },
        index=index,
    )
    return BenchData(returns=returns, inputs=inputs, source={**source, "qqq_series": qqq_series})


def from_snapshot(snapshot_dir: Path, *, qqq_series: QqqSeries) -> BenchData:
    """Read a checked snapshot; the check's WARN items are carried into every run record."""

    daily, report = qlh.load_daily(snapshot_dir)
    warnings = sorted(
        {
            str(issue.get("code"))
            for issue in report.get("issues", [])
            if isinstance(issue, dict) and issue.get("severity") == "WARN"
        }
    )
    source = {
        "snapshot_id": str(report.get("snapshot_id", snapshot_dir.name)),
        "series_csv_sha256": report.get("series_csv_sha256"),
        "spec_sha256": report.get("spec_sha256"),
        "check_status": report.get("status"),
        "check_warnings": warnings,
    }
    return from_daily_frame(daily, qqq_series=qqq_series, source=source)
