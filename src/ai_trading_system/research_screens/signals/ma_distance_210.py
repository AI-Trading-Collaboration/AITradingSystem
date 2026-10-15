"""Moving average distance: price divided by its trailing simple average, minus one."""

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
    # cumsum and rolling keep a missing return missing instead of filling it with zero, so the
    # distance is missing until the window is full and free of gaps.
    log_price = pd.Series(np.log1p(returns.to_numpy()), index=returns.index).cumsum()
    price = pd.Series(np.exp(log_price.to_numpy()), index=returns.index)
    return price / price.rolling(int(params["window"])).mean() - 1
