"""Strict signal-source specifications and publication-time alignment (TRADING-2565 S4a).

TRADING-2565-37 (owner_decision:TRADING-2565:2026-10-21:monthly_long_history): a monthly
observation for M is published on the first trading day on or after the first day of month
M + 1 + release_lag_months, then used known_lag_trading_days trading days later.
"""

from __future__ import annotations

from typing import Literal, Self

import numpy as np
import pandas as pd
from numpy.typing import NDArray
from pydantic import BaseModel, ConfigDict, Field, model_validator

# S4a's approved data contract: carry a published observation for at most five calendar trading
# positions. This is a missing-data invariant, not a signal/candidate acceptance threshold.
DEFAULT_MAX_STALE_DAYS = 5
# Monthly sources bridge one missing month, then empty on the second; like the daily limit,
# this is a missing-data invariant, not a signal threshold.
MONTHLY_MAX_MISSING_MONTHS = 1
SHILLER_COLUMNS = frozenset({"P", "D", "E", "CPI", "GS10"})
KEN_FRENCH_DATASETS = frozenset({"F-F_Research_Data_Factors_daily", "49_Industry_Portfolios_daily"})


class SourceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str = Field(pattern=r"^[a-z0-9_]+$")
    provider: Literal["fred", "ken_french", "shiller"]
    dataset: str = Field(min_length=1)
    frequency: Literal["daily", "monthly"]
    known_lag_trading_days: int = Field(ge=0, strict=True)
    description: str = Field(min_length=1)
    release_lag_months: int = Field(default=0, ge=0, strict=True)

    @model_validator(mode="after")
    def validate_provider(self) -> Self:
        if not self.dataset.strip() or not self.description.strip():
            raise ValueError("dataset and description must not be blank")
        if self.provider == "fred" and self.known_lag_trading_days < 1:
            raise ValueError(
                "FRED observations require at least one trading day of publication lag"
            )
        if self.provider == "ken_french" and (
            self.frequency != "daily" or self.dataset not in KEN_FRENCH_DATASETS
        ):
            raise ValueError("unsupported Ken French daily dataset")
        if self.provider == "shiller" and (
            self.frequency != "monthly" or self.dataset not in SHILLER_COLUMNS
        ):
            raise ValueError("Shiller sources are monthly columns P, D, E, CPI or GS10")
        if self.frequency == "daily" and self.release_lag_months != 0:
            raise ValueError("release_lag_months applies to monthly sources only")
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


def monthly_known_at(
    values: pd.Series,
    calendar: pd.DatetimeIndex,
    release_lag_months: int,
    lag: int,
) -> pd.Series:
    """Align monthly observations from their use date until replacement or monthly expiry."""

    if release_lag_months < 0 or lag < 0:
        raise ValueError("release_lag_months and lag must be nonnegative")
    if calendar.hasnans or not calendar.is_monotonic_increasing or not calendar.is_unique:
        raise ValueError("calendar must contain increasing, unique dates")
    observations = values.dropna()
    dates = pd.DatetimeIndex(observations.index)
    if dates.hasnans:
        raise ValueError("observations must contain valid dates")
    months = dates.to_period("M")
    if not months.is_monotonic_increasing or not months.is_unique:
        raise ValueError("observations must contain increasing, unique months")

    def used(offset: int) -> NDArray[np.int64]:
        releases = months.shift(offset + release_lag_months).to_timestamp(how="start")
        return np.asarray(calendar.searchsorted(releases, side="left"), dtype=np.int64) + lag

    starts = used(1)
    expiries = used(2 + MONTHLY_MAX_MISSING_MONTHS)
    out = np.full(len(calendar), np.nan)
    for k, start in enumerate(starts):
        following = starts[k + 1] if k + 1 < len(starts) else len(calendar)
        end = min(len(calendar), expiries[k], following)
        if start < end:
            out[start:end] = observations.iloc[k]
    return pd.Series(out, index=calendar, name=values.name)


def align_source(
    values: pd.Series, calendar: pd.DatetimeIndex, rule: int | SourceSpec
) -> pd.Series:
    """Apply the daily lag or the publication rule specified by the source's frequency."""

    if isinstance(rule, int):
        return known_at(values, calendar, rule)
    if rule.frequency == "monthly":
        return monthly_known_at(
            values, calendar, rule.release_lag_months, rule.known_lag_trading_days
        )
    return known_at(values, calendar, rule.known_lag_trading_days)
