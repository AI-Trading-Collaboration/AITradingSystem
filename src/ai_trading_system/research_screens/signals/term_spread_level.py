"""Term spread level: the aligned 10Y-minus-3M Treasury spread value itself, untransformed."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd


def signal(
    returns: pd.Series,
    sources: Mapping[str, pd.DataFrame],
    params: Mapping[str, Any],
) -> pd.Series:
    return sources["t10y3m"]["value"].astype(float)
