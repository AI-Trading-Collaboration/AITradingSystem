"""Fixed target exposure; executable bounds belong to the bench's simulator."""

from __future__ import annotations

import math
from collections.abc import Mapping
from numbers import Real
from typing import Any

import pandas as pd


def fixed_exposure(inputs: pd.DataFrame, params: Mapping[str, Any]) -> pd.Series:
    """Return one finite exposure on every input date, without shifting the decision."""

    if set(params) != {"exposure"}:
        raise ValueError("fixed_exposure takes exactly one parameter, exposure")
    exposure = params["exposure"]
    if isinstance(exposure, bool) or not isinstance(exposure, Real) or not math.isfinite(exposure):
        raise ValueError("exposure must be a finite real number")
    return pd.Series(float(exposure), index=inputs.index, dtype=float)
