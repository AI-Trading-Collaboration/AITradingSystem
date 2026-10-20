"""Growth leadership measured as one minus trailing compounded HML growth."""

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
    hml = sources["ff_factors_daily"]["HML"].reindex(returns.index).astype(float)
    hml = hml.replace([np.inf, -np.inf], np.nan)
    growth = (1.0 + hml).rolling(window, min_periods=window).apply(np.prod, raw=True)
    return (1.0 - growth).replace([np.inf, -np.inf], np.nan).astype(float)
