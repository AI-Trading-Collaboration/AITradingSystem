"""Strict signal-source specifications and publication-time alignment (TRADING-2565 S4a)."""

from __future__ import annotations

from typing import Literal, Self

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

# S4a's approved data contract: carry a published observation for at most five calendar trading
# positions. This is a missing-data invariant, not a signal/candidate acceptance threshold.
DEFAULT_MAX_STALE_DAYS = 5


class SourceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str = Field(pattern=r"^[a-z0-9_]+$")
    provider: Literal["fred", "ken_french"]
    dataset: str = Field(min_length=1)
    frequency: Literal["daily"]
    known_lag_trading_days: int = Field(ge=0, strict=True)
    description: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_provider(self) -> Self:
        if not self.dataset.strip() or not self.description.strip():
            raise ValueError("dataset and description must not be blank")
        if self.provider == "fred" and self.known_lag_trading_days < 1:
            raise ValueError(
                "FRED observations require at least one trading day of publication lag"
            )
        if self.provider == "ken_french" and self.dataset not in {
            "F-F_Research_Data_Factors_daily",
            "49_Industry_Portfolios_daily",
        }:
            raise ValueError("unsupported Ken French daily dataset")
        return self


def known_at(
    values: pd.Series,
    calendar: pd.DatetimeIndex,
    lag: int,
    max_stale_days: int = DEFAULT_MAX_STALE_DAYS,
) -> pd.Series:
    """As-of join on calendar[i - lag], measuring staleness in trading-calendar positions.

    Drop missing observations before the join: a missing value neither replaces the last known
    value nor resets its age. Off-calendar observation dates sit at the preceding calendar position.
    """

    if lag < 0 or max_stale_days < 0:
        raise ValueError("lag and max_stale_days must be nonnegative")
    if calendar.hasnans or not calendar.is_monotonic_increasing or not calendar.is_unique:
        raise ValueError("calendar must contain increasing, unique dates")
    observations = values.dropna()
    dates = pd.DatetimeIndex(observations.index)
    if dates.hasnans or not dates.is_monotonic_increasing or not dates.is_unique:
        raise ValueError("observations must contain increasing, unique dates")
    out = np.full(len(calendar), np.nan)
    for i in range(lag, len(calendar)):
        reference_position = i - lag
        position = int(dates.searchsorted(calendar[reference_position], side="right")) - 1
        if position < 0:
            continue
        observation_position = calendar.searchsorted(dates[position], side="right") - 1
        if reference_position - observation_position > max_stale_days:
            continue
        out[i] = observations.iloc[position]
    return pd.Series(out, index=calendar, name=values.name)
