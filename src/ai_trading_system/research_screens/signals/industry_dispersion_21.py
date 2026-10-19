"""Trailing mean of daily cross-sectional dispersion in Ken French industry returns."""

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
    window = int(params["window"])
    frame = sources["ff_49_industries_daily"].replace([np.inf, -np.inf], np.nan)
    # Two observed industries are the specification's minimum for cross-sectional dispersion.
    daily = frame.std(axis=1, ddof=0).where(frame.count(axis=1) >= 2)
    result = daily.rolling(window, min_periods=window).mean()
    return result.astype(float).replace([np.inf, -np.inf], np.nan)
