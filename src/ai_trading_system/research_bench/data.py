"""Bench data: asset returns and signal inputs from a checked long-history snapshot.

The snapshot is read only through ``qqq_long_history.load_daily``, which refuses unchecked, failed
or stale snapshots. Returns on day t cover the previous trading date's close to t's close. Signal
inputs carry only what was known at t's close: the T-bill rate dated t is published the next
business day, so ``cash_rate_known`` on t is the rate dated the previous trading date.

``qqq_return`` is the chosen QQQ series alone; it starts in March 1999, so a lookback over it has no
value at the start of the development window. ``ndx_return`` is the same series from its first
return on and the Nasdaq-100 price index's return before that (the price index starts in 1986), so a
lookback can reach back before the window. Before March 1999 it leaves out dividends and fees, well
under 1% a year. A gap after the QQQ series has started stays a gap.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd

from ai_trading_system.research_bench.sources import (
    DEFAULT_MAX_STALE_DAYS,
    SourceSpec,
    align_source,
    known_at,
)
from ai_trading_system.research_data import qqq_long_history as qlh

ASSETS: tuple[str, ...] = ("qqq", "cash", "lev2", "lev3")
RESERVES = ("cash", "treasury_10y", "treasury_10y_shiller")
TREASURY_RESERVE = "treasury_10y"
TREASURY_SOURCE = "dgs10"
# The same ten-year par bond, priced monthly from Shiller's long rate, available since 1871.
# owner_decision:TRADING-2565:2026-10-21:treasury_1929
SHILLER_TREASURY_RESERVE = "treasury_10y_shiller"
SHILLER_TREASURY_SOURCE = "shiller_gs10"
# Ten years of semiannual payments: an instrument-definition invariant.
TREASURY_PERIODS = 20
# Percent-unit checks, not investment thresholds; reject decimal yields and implausible units.
TREASURY_MIN_YIELD_PERCENT = 0.1
TREASURY_MAX_YIELD_PERCENT = 25.0
# Exposure to the Nasdaq-100 that one unit of each asset carries.
EXPOSURE_FACTORS: dict[str, float] = {"qqq": 1.0, "cash": 0.0, "lev2": 2.0, "lev3": 3.0}
QqqSeries = Literal["proxy", "actual"]
QQQ_SERIES_COLUMNS: dict[str, str] = {
    "proxy": "qqq_proxy_return",
    "actual": "etf_reference_return",
}
INPUT_COLUMNS: tuple[str, ...] = (
    "qqq_return",
    "ndx_return",
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
    reserves: Mapping[str, pd.Series] = field(default_factory=dict)


def _check_percent(yields: pd.Series) -> None:
    observations = yields.dropna()
    if (
        (observations < TREASURY_MIN_YIELD_PERCENT) | (observations > TREASURY_MAX_YIELD_PERCENT)
    ).any():
        raise ValueError("Treasury yields must be percent")


def par_bond_price(coupon: Any, rate: Any) -> Any:
    """Price of a ten-year par bond with decimal coupon ``coupon`` and yield ``rate``."""

    discount = (1 + rate / 2) ** -TREASURY_PERIODS
    return coupon / rate * (1 - discount) + discount


def treasury_returns(yields: pd.Series, calendar: pd.DatetimeIndex) -> pd.Series:
    """Realized constant-ten-year par-bond returns from percent yields, without signal lag."""

    _check_percent(yields)
    y = known_at(yields / 100, calendar, 0, DEFAULT_MAX_STALE_DAYS)
    coupon = y.shift(1).to_numpy(dtype=float)
    rates = y.to_numpy(dtype=float)
    days = np.full(len(calendar), np.nan)
    days[1:] = (calendar[1:] - calendar[:-1]).days
    price = par_bond_price(coupon, rates)
    return pd.Series(price - 1 + coupon * days / 365, index=calendar, name=TREASURY_RESERVE)


def monthly_treasury_returns(yields: pd.Series, calendar: pd.DatetimeIndex) -> pd.Series:
    """Month M returns P(y[M-1], y[M]) - 1 + y[M-1]/12, evenly over calendar dates.

    Every date in a month receives the same daily return, compounding to the monthly return.
    Without either month's yield the whole month is empty. Only M-1 and M are read, so no date
    sees a later month. Shiller's monthly averages smooth intramonth moves, lagging about half
    a month; these are realized reserve returns, not a signal available within the month.
    """

    _check_percent(yields)
    observations = yields.dropna()
    months = pd.DatetimeIndex(observations.index).to_period("M")
    if not (months.is_monotonic_increasing and months.is_unique):
        raise ValueError("Treasury yields must have increasing, unique months")
    rates = pd.Series(observations.to_numpy(dtype=float) / 100, index=months)
    period = calendar.to_period("M")
    current = rates.reindex(period).to_numpy(dtype=float)
    previous = rates.reindex(period - 1).to_numpy(dtype=float)
    monthly = par_bond_price(previous, current) - 1 + previous / 12
    counts = pd.Series(1, index=period).groupby(level=0).transform("sum").to_numpy(dtype=float)
    daily = (1 + monthly) ** (1 / counts) - 1
    return pd.Series(daily, index=calendar, name=SHILLER_TREASURY_RESERVE)


def with_reserves(data: BenchData, frames: Mapping[str, pd.DataFrame]) -> BenchData:
    """Attach each Treasury whose source is in frames, without changing inputs or benchmarks."""

    reserves = {}
    for source, reserve, calculate in (
        (TREASURY_SOURCE, TREASURY_RESERVE, treasury_returns),
        (SHILLER_TREASURY_SOURCE, SHILLER_TREASURY_RESERVE, monthly_treasury_returns),
    ):
        if source not in frames:
            continue
        frame = frames[source]
        if "value" not in frame.columns:
            raise ValueError(f"{source} frame needs a value column")
        reserves[reserve] = calculate(frame["value"], pd.DatetimeIndex(data.returns.index))
    if not reserves:
        return data
    return BenchData(
        returns=data.returns,
        inputs=data.inputs,
        source=data.source,
        reserves={**data.reserves, **reserves},
    )


def strategy_returns(data: BenchData, reserve: str) -> pd.DataFrame:
    """Use the selected reserve in the simulator's cash slot for the strategy only."""

    if reserve not in RESERVES:
        raise ValueError(f"unknown reserve {reserve}")
    if reserve == "cash":
        return data.returns
    if reserve not in data.reserves:
        raise ValueError(f"data have no {reserve} reserve")
    traded = data.returns.copy()
    traded["cash"] = data.reserves[reserve].reindex(traded.index)
    return traded


def with_signal_sources(
    data: BenchData,
    frames: Mapping[str, pd.DataFrame],
    lags: Mapping[str, int | SourceSpec],
    *,
    snapshot_id: str,
) -> BenchData:
    """Append source columns on the bench calendar, after applying their publication lag.

    Integers are daily-source lags; specs align by frequency, with monthly sources starting
    on the day their month becomes known.
    """

    inputs = data.inputs.copy()
    calendar = pd.DatetimeIndex(inputs.index)
    for source, frame in frames.items():
        for position, column in enumerate(frame.columns):
            name = f"source:{source}:{column}"
            if name in inputs.columns:
                raise ValueError(f"duplicate signal source column {name}")
            inputs[name] = align_source(frame.iloc[:, position], calendar, lags[source])
    return BenchData(
        returns=data.returns,
        inputs=inputs,
        source={**data.source, "signal_snapshot_id": snapshot_id},
        reserves=data.reserves,
    )


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
    price = daily["ndx_price"].astype(float)
    price_return = (price / price.shift(1) - 1).to_numpy()
    before = ~np.logical_or.accumulate(qqq.notna().to_numpy())
    ndx = np.where(before, price_return, qqq.to_numpy())
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
            "ndx_return": ndx,
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


US_MARKET_SOURCE = "ff_factors_daily"
# Unit checks, not investment thresholds: no US market day moves by half its value, nor does RF
# earn half a percentage point daily; larger values suggest Ken French percent, not daily decimals.
US_MARKET_MAX_ABS_RETURN = 0.5
US_MARKET_MAX_ABS_RF = 0.005


def from_us_market(frames: Mapping[str, pd.DataFrame], *, snapshot_id: str) -> BenchData:
    """Build bench data from a checked signal snapshot's daily decimal Ken French factors.

    Read only Mkt-RF (market excess return) and RF (risk-free return) from ff_factors_daily.
    Returns contain qqq (Mkt-RF + RF), cash (RF), and synthetic lev2/lev3 financed at RF without
    fees; the leveraged columns only complete the simulator's schema and are not held in trials.
    Inputs use the market for qqq_return/ndx_return and its compounded level, starting at one,
    for ndx_price/ndx_tr. vix/vxn/vxo are NaN; cash_rate_known is the previous date's RF times 252.
    """

    if US_MARKET_SOURCE not in frames:
        raise ValueError(f"missing {US_MARKET_SOURCE} frame")
    frame = frames[US_MARKET_SOURCE]
    for column in ("Mkt-RF", "RF"):
        if column not in frame.columns:
            raise ValueError(f"missing {column} column")
    index = pd.DatetimeIndex(frame.index, name="date")
    if index.hasnans or not index.is_monotonic_increasing or index.has_duplicates:
        raise ValueError("US market dates must be unique and increasing")
    excess = frame["Mkt-RF"].astype(float).to_numpy()
    rf = frame["RF"].astype(float).to_numpy()
    if not np.isfinite(excess).all() or not np.isfinite(rf).all():
        raise ValueError("US market returns have missing or non-finite values")
    if (
        np.max(np.abs(excess)) > US_MARKET_MAX_ABS_RETURN
        or np.max(np.abs(rf)) > US_MARKET_MAX_ABS_RF
    ):
        raise ValueError("US market returns appear to be percent rather than daily decimals")

    market = excess + rf
    returns = pd.DataFrame(
        {"qqq": market, "cash": rf, "lev2": 2 * market - rf, "lev3": 3 * market - 2 * rf},
        index=index,
    )
    level = np.cumprod(1 + market) / (1 + market[0])
    inputs = pd.DataFrame(
        {
            "qqq_return": market,
            "ndx_return": market,
            "ndx_price": level,
            "ndx_tr": level,
            "vix": np.nan,
            "vxn": np.nan,
            "vxo": np.nan,
            "cash_rate_known": pd.Series(rf, index=index).shift(1).to_numpy() * 252,
        },
        index=index,
    )
    source = {"market": "us_market", "snapshot_id": snapshot_id, "returns_source": US_MARKET_SOURCE}
    return BenchData(returns=returns, inputs=inputs, source=source)
