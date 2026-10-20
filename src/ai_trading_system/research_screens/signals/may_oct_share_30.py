"""Share of the next calendar days falling in May through October."""

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
    days = int(params["days"])
    dates = pd.DatetimeIndex(returns.index).normalize()
    counts = np.zeros(len(dates), dtype=float)
    for offset in range(1, days + 1):
        months = (dates + pd.DateOffset(days=offset)).month
        counts += (months >= 5) & (months <= 10)
    return pd.Series(counts / days, index=returns.index, dtype=float)
