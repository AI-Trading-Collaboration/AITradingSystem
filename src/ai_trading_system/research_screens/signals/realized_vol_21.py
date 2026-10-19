"""Realized volatility: the annualised standard deviation of the asset's daily returns over the
trailing window; the screen baseline rv_21 as a module that a preregistration can pin."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

import pandas as pd

TRADING_DAYS = 252


def signal(
    returns: pd.Series,
    sources: Mapping[str, pd.DataFrame],
    params: Mapping[str, Any],
) -> pd.Series:
    # rolling keeps a missing return missing, so the value is missing until the window is full and
    # while a gap is inside it. The same expression as research_bench.screen.baseline_signals.
    return returns.rolling(int(params["window"])).std() * math.sqrt(TRADING_DAYS)
