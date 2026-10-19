"""Trailing sum of daily industry reversal profits from lagged compounded returns."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd


def signal(
    returns: pd.Series,
    sources: Mapping[str, pd.DataFrame],
    params: Mapping[str, Any],
) -> pd.Series:
    formation = int(params["formation"])
    window = int(params["window"])
    frame = sources["ff_49_industries_daily"]
    values = frame.to_numpy(dtype=float)
    daily = np.full(len(frame), np.nan)
    for t in range(formation, len(frame)):
        past = values[t - formation : t]
        today = values[t]
        keep = np.isfinite(past).all(axis=0) & np.isfinite(today)
        if keep.sum() < 2:
            continue
        formed = np.prod(1.0 + past[:, keep], axis=0) - 1.0
        deviation = formed - formed.mean()
        gross_half = np.abs(deviation).sum() / 2.0
        if gross_half == 0.0:
            continue
        daily[t] = np.dot(-deviation / gross_half, today[keep])
    portfolio = pd.Series(daily, index=frame.index, dtype=float)
    result = portfolio.where(np.isfinite(portfolio)).rolling(window, min_periods=window).sum()
    return result.where(np.isfinite(result)).astype(float)
