"""Mean pairwise correlation of Ken French industry returns over a trailing window."""

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
    # One corrcoef per date: pairing 49 industries with pandas rolling().corr() over the full
    # history would need gigabytes. Only columns that are gap-free inside the window and move
    # (standard deviation above zero) enter the correlation matrix for that date.
    window = int(params["window"])
    frame = sources["ff_49_industries_daily"]
    values = frame.to_numpy(dtype=float)
    out = np.full(values.shape[0], np.nan)
    for t in range(window - 1, values.shape[0]):
        block = values[t - window + 1 : t + 1]
        keep = ~np.isnan(block).any(axis=0)
        block = block[:, keep]
        block = block[:, block.std(axis=0) > 0.0]
        k = block.shape[1]
        if k < 2:
            continue
        corr = np.corrcoef(block, rowvar=False)
        out[t] = (corr.sum() - k) / (k * (k - 1))
    return pd.Series(out, index=frame.index, dtype=float)
