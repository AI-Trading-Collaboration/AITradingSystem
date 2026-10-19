"""Trailing shift in the covariance absorption ratio of Ken French industry returns."""

from __future__ import annotations

import math
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
    short = int(params["short"])
    long = int(params["long"])
    frame = sources["ff_49_industries_daily"]
    values = frame.to_numpy(dtype=float)
    ratios = np.full(len(frame), np.nan)
    for t in range(window - 1, len(frame)):
        block = values[t - window + 1 : t + 1]
        block = block[:, np.isfinite(block).all(axis=0)]
        k = block.shape[1]
        # The specification requires two complete industries and the top fifth of eigenvalues.
        if k < 2 or window < 2:
            continue
        eigenvalues = np.linalg.eigvalsh(np.cov(block, rowvar=False, ddof=1))
        total = eigenvalues.sum()
        if total > 0.0:
            ratios[t] = eigenvalues[-math.ceil(k / 5) :].sum() / total
    ratio = pd.Series(ratios, index=frame.index, dtype=float)
    short_mean = ratio.rolling(short, min_periods=short).mean()
    history = ratio.rolling(long, min_periods=long)
    spread = history.std(ddof=1)
    result = (short_mean - history.mean()) / spread.where(spread > 0.0)
    return result.replace([np.inf, -np.inf], np.nan)
