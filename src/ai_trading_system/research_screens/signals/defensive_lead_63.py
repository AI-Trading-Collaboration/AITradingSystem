"""Trailing compounded return of defensive industries minus the Ken French market return."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

DEFENSIVE = ("Food", "Beer", "Smoke", "Hshld", "Drugs", "Util")


def signal(
    returns: pd.Series,
    sources: Mapping[str, pd.DataFrame],
    params: Mapping[str, Any],
) -> pd.Series:
    window = int(params["window"])
    frame = sources["ff_49_industries_daily"]
    missing = [name for name in DEFENSIVE if name not in frame.columns]
    if missing:
        raise ValueError(f"Missing defensive industries: {', '.join(missing)}")
    defensive = frame.loc[:, list(DEFENSIVE)].replace([np.inf, -np.inf], np.nan)
    # The specification requires at least three of the six defensive industries each day.
    basket = defensive.mean(axis=1).where(defensive.count(axis=1) >= 3)
    factors = sources["ff_factors_daily"]
    market = (factors["Mkt-RF"] + factors["RF"]).reindex(frame.index)
    market = market.replace([np.inf, -np.inf], np.nan)
    basket_growth = (1.0 + basket).rolling(window, min_periods=window).apply(np.prod, raw=True)
    market_growth = (1.0 + market).rolling(window, min_periods=window).apply(np.prod, raw=True)
    result = basket_growth - market_growth
    return result.astype(float).replace([np.inf, -np.inf], np.nan)
