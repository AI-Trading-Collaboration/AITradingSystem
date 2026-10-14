"""High distance: log price minus the highest log price in the trailing window, never positive."""

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
    # pandas cumsum and rolling keep a missing return missing instead of filling it with zero.
    log_price = pd.Series(np.log1p(returns.to_numpy()), index=returns.index).cumsum()
    return log_price - log_price.rolling(int(params["window"])).max()
