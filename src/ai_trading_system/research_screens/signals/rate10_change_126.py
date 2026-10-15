"""10-year Treasury yield minus its value `window` trading days earlier."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd


def signal(
    returns: pd.Series,
    sources: Mapping[str, pd.DataFrame],
    params: Mapping[str, Any],
) -> pd.Series:
    rate = sources["dgs10"]["value"].astype(float)
    return rate - rate.shift(int(params["window"]))
