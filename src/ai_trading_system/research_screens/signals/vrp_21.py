"""VIX minus trailing US market realised volatility, both in annualised percentage points."""

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
    vix = sources["vixcls"]["value"].astype(float)
    factors = sources["ff_factors_daily"]
    market = factors["Mkt-RF"] + factors["RF"]
    window = int(params["window"])
    mean_square = (market**2).rolling(window).mean()
    realised = pd.Series(100 * np.sqrt(252 * mean_square.to_numpy()), index=mean_square.index)
    return (vix - realised).reindex(returns.index)
