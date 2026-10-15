"""Time series momentum: the compounded return of the asset over the trailing window."""

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
    # rolling keeps a missing return missing instead of filling it with zero, so the compounded
    # return is missing until the window is full or a gap leaves it.
    logs = pd.Series(np.log1p(returns.to_numpy()), index=returns.index)
    total = logs.rolling(int(params["window"])).sum()
    return pd.Series(np.expm1(total.to_numpy()), index=returns.index)
