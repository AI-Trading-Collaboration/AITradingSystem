"""Share of Ken French industries trading below their own trailing simple average."""

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
    # cumsum keeps a missing industry return missing instead of filling it with zero, and the
    # rolling mean with its default min_periods leaves the average missing until the window is
    # full and free of gaps, so an industry only counts when both price and average exist.
    window = int(params["window"])
    industries = sources["ff_49_industries_daily"]
    log_price = pd.DataFrame(np.log1p(industries.to_numpy()), index=industries.index).cumsum()
    price = pd.DataFrame(np.exp(log_price.to_numpy()), index=industries.index)
    average = price.rolling(window).mean()
    has_average = price.notna() & average.notna()
    below = has_average & (price < average)
    # 0 / 0 for a day where no industry has an average gives NaN.
    return below.sum(axis=1) / has_average.sum(axis=1)
