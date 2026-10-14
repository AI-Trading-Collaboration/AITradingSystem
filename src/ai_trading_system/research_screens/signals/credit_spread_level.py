"""Credit spread level: the aligned Baa-10Y spread value itself, untransformed."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd


def signal(
    returns: pd.Series,
    sources: Mapping[str, pd.DataFrame],
    params: Mapping[str, Any],
) -> pd.Series:
    return sources["baa10y"]["value"].astype(float)
