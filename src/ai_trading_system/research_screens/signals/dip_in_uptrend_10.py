"""Trailing short-window return while the trailing long-window return is positive, else missing."""

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
    # cumsum and rolling keep a missing return missing instead of filling it with zero, so both
    # compound returns are missing until their window is full and free of gaps. A day counts as
    # an uptrend only when the long return exists and is strictly positive.
    logs = pd.Series(np.log1p(returns.to_numpy()), index=returns.index)
    short = pd.Series(
        np.expm1(logs.rolling(int(params["short"])).sum().to_numpy()), index=returns.index
    )
    long = pd.Series(
        np.expm1(logs.rolling(int(params["long"])).sum().to_numpy()), index=returns.index
    )
    return short.where(long > 0).astype(float)
