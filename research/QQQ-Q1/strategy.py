"""QQQ-Q1: Nasdaq-100 exposure managed by realized volatility (TRADING-2567; prereg.yaml).

The target exposure is min(cap, scale × σ* / σ̂): cap 1 without leverage (QQQ and cash), cap 3 with
leverage (QQQ, 2x and 3x). σ̂ is the annualized volatility of daily Nasdaq-100 returns up to and
including the decision day, read from ``ndx_return`` (QQQ from March 1999, the price index before
that). σ* is the median of the same estimate over every date before the development window, 1986 to
1999-03-09: data no window contains, so σ* is the same number in every window and no return inside
a window can move it. The rebalance schedule is in prereg.yaml; the bench trades the target at the
next close.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

# First day of the development window (config/research/research_window_registry.yaml).
TARGET_VOL_BEFORE = pd.Timestamp("1999-03-10")
TRADING_DAYS = 252
LEVERAGED_CAP = 3.0
UNLEVERAGED_CAP = 1.0


def realized_vol_exposure(inputs: pd.DataFrame, params: Mapping[str, Any]) -> pd.Series:
    """σ̂: standard deviation of the last ``window`` daily returns."""

    returns = inputs["ndx_return"]
    vol = returns.rolling(int(params["window"])).std() * np.sqrt(TRADING_DAYS)
    return _scaled_exposure(vol, params)


def ewma_vol_exposure(inputs: pd.DataFrame, params: Mapping[str, Any]) -> pd.Series:
    """σ̂: exponentially weighted, zero mean (RiskMetrics); a half-life of 11.2 days is λ = 0.94."""

    returns = inputs["ndx_return"]
    variance = returns.pow(2).ewm(halflife=float(params["halflife"]), ignore_na=True).mean()
    vol = np.sqrt(variance * TRADING_DAYS)
    return _scaled_exposure(vol, params)


def target_vol(vol: pd.Series) -> float:
    """σ*: the median of the estimate before the development window."""

    before = vol[vol.index < TARGET_VOL_BEFORE].dropna()
    if before.empty:
        raise ValueError(f"no volatility estimate before {TARGET_VOL_BEFORE.date()}")
    return float(before.median())


def _scaled_exposure(vol: pd.Series, params: Mapping[str, Any]) -> pd.Series:
    cap = LEVERAGED_CAP if bool(params["leveraged"]) else UNLEVERAGED_CAP
    return (float(params["scale"]) * target_vol(vol) / vol).clip(upper=cap)
