"""Credit spread change: Baa-10Y spread minus its value `window` trading days earlier."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd


def signal(
    returns: pd.Series,
    sources: Mapping[str, pd.DataFrame],
    params: Mapping[str, Any],
) -> pd.Series:
    spread = sources["baa10y"]["value"].astype(float)
    return spread - spread.shift(int(params["window"]))
