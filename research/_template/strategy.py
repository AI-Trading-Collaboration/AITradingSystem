"""Strategy module template (TRADING-2565 S3b).

A strategy is ``(inputs, params) -> pd.Series``: for every date, the target exposure to the
Nasdaq-100 (0 to 3) given what was known at that date's close. The bench trades it at the next
close; do not shift the output. ``inputs`` has qqq_return, ndx_return, ndx_price, ndx_tr, vix, vxn,
vxo and cash_rate_known (the T-bill rate known that day). QQQ starts in March 1999, so a lookback
reads ndx_return, which reaches back to 1986 through the price index (``research_bench/data.py``).
Every numeric or boolean parameter must change some target, and no target may move when only later
data moves; the bench checks both before every run.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd


def vol_scaled_exposure(inputs: pd.DataFrame, params: Mapping[str, Any]) -> pd.Series:
    """Exposure that targets an annual volatility, capped."""

    vol = inputs["ndx_return"].rolling(int(params["window"])).std() * np.sqrt(252)
    return (float(params["target"]) / vol).clip(upper=float(params["cap"]))
