"""Trailing difference between downside and upside shares of squared daily returns."""

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
    down = returns.clip(upper=0.0).pow(2).rolling(window, min_periods=window).sum()
    up = returns.clip(lower=0.0).pow(2).rolling(window, min_periods=window).sum()
    total = down + up
    result = (down - up) / total.where(total != 0.0)
    return result.where(np.isfinite(result)).astype(float)
