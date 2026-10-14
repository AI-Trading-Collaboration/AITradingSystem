"""Simulation: a target decided at one close is traded at the next close, then holdings drift.

Strategies give one number per trading date: the target exposure to the Nasdaq-100 (0 to 3) that
they would hold given everything known at that date's close. The bench owns the rest:

- Timing: the target decided at close t is traded at close t+1(+delay), so the first return it
  earns is t+1 to t+2. Strategy code must not shift its own output; the look-ahead check catches
  targets that use later data.
- Holdings: an exposure maps to instruments that can be bought (QQQ, QLD, TQQQ and cash):
  0-1 is QQQ plus cash, 1-2 is QQQ plus 2x, 2-3 is 2x plus 3x. Between trades the holdings drift
  with prices; a schedule and a no-trade band decide when they are brought back to target.
- Cost: one-way turnover (half the sum of absolute weight changes) times the unified cost in bps,
  taken from the portfolio at the trade.
- Start: holdings are set at the first window date's close to the last target executable by then,
  without a cost; the first return counted is the next date's.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal

import numpy as np
import pandas as pd

from ai_trading_system.research_bench.data import ASSETS, EXPOSURE_FACTORS

Schedule = Literal["daily", "week_end", "month_end"]
SCHEDULES: tuple[Schedule, ...] = ("daily", "week_end", "month_end")
MAX_EXPOSURE = 3.0
_FACTORS = [EXPOSURE_FACTORS[asset] for asset in ASSETS]
_EPSILON = 1e-12


@dataclass(frozen=True)
class Rebalance:
    """When targets are acted on: decisions are read on schedule dates only, and a decision trades
    only when the drifted exposure is more than ``band`` (relative to the target) away from it."""

    schedule: Schedule = "daily"
    band: float = 0.0

    def __post_init__(self) -> None:
        if self.schedule not in SCHEDULES:
            raise ValueError(f"schedule must be one of {SCHEDULES}")
        if not 0.0 <= self.band < 1.0:
            raise ValueError("band must be in [0, 1)")


@dataclass(frozen=True)
class Simulation:
    """Per return date: net and gross return, exposure held during the day, turnover and cost
    of the trade at that date's close."""

    daily: pd.DataFrame
    start: pd.Timestamp

    @property
    def returns(self) -> pd.Series:
        return self.daily["return"]

    @property
    def exposure(self) -> pd.Series:
        return self.daily["exposure"]


def weights_for_exposure(exposure: float) -> list[float]:
    """Instrument weights in ``ASSETS`` order for one target exposure."""

    if not math.isfinite(exposure) or exposure < 0.0 or exposure > MAX_EXPOSURE:
        raise ValueError(f"target exposure must be in [0, {MAX_EXPOSURE}], got {exposure}")
    if exposure <= 1.0:
        return [exposure, 1.0 - exposure, 0.0, 0.0]
    if exposure <= 2.0:
        return [2.0 - exposure, 0.0, exposure - 1.0, 0.0]
    return [0.0, 0.0, 3.0 - exposure, exposure - 2.0]


def exposure_weights(exposure: pd.Series) -> pd.DataFrame:
    rows = [weights_for_exposure(float(value)) for value in exposure.to_numpy()]
    return pd.DataFrame(rows, index=exposure.index, columns=list(ASSETS))


def schedule_mask(index: pd.DatetimeIndex, schedule: Schedule) -> list[bool]:
    """True on the dates whose decisions are read: every date, or the last date of each week or
    month in the data's own calendar (the last available date always counts)."""

    if schedule == "daily":
        return [True] * len(index)
    freq = "W" if schedule == "week_end" else "M"
    periods = index.to_period(freq)
    mask = [periods[i] != periods[i + 1] for i in range(len(index) - 1)]
    return [*mask, True]


def simulate(
    returns: pd.DataFrame,
    targets: pd.Series,
    *,
    start: date,
    end: date | None,
    rebalance: Rebalance,
    cost_bps: float,
    delay_days: int = 0,
) -> Simulation:
    if list(returns.columns) != list(ASSETS):
        raise ValueError(f"returns columns must be {list(ASSETS)}")
    if not targets.index.equals(returns.index):
        raise ValueError("targets must be indexed by the same dates as the returns")
    if delay_days < 0:
        raise ValueError("delay_days must not be negative")
    if cost_bps < 0:
        raise ValueError("cost_bps must not be negative")
    index = pd.DatetimeIndex(returns.index)
    first = int(index.searchsorted(pd.Timestamp(start)))
    last = (
        len(index) - 1
        if end is None
        else int(index.searchsorted(pd.Timestamp(end), side="right")) - 1
    )
    if first >= len(index) or last <= first:
        raise ValueError(f"no return dates between {start} and {end}")
    window = returns.iloc[first + 1 : last + 1]
    if window.isna().to_numpy().any():
        bad = window[window.isna().any(axis=1)]
        column = bad.columns[bad.isna().iloc[0].to_numpy()][0]
        raise ValueError(f"{column} has no return on {bad.index[0].date()} inside the window")

    lag = 1 + delay_days
    scheduled = schedule_mask(index, rebalance.schedule)
    values = targets.to_numpy(dtype=float)
    opening = next((p for p in range(first - lag, -1, -1) if scheduled[p]), None)
    if opening is None or not math.isfinite(values[opening]):
        raise ValueError(f"no target is executable by {index[first].date()}; extend the lookback")
    weights = _checked_weights(values[opening], index[opening])
    rate = cost_bps / 10_000.0
    matrix = returns.to_numpy(dtype=float)

    rows: list[tuple[float, float, float, float, float]] = []
    for q in range(first + 1, last + 1):
        day = matrix[q]
        held = sum(w * f for w, f in zip(weights, _FACTORS, strict=True))
        gross = sum(w * r for w, r in zip(weights, day, strict=True))
        if 1.0 + gross <= 0.0:
            raise ValueError(f"portfolio lost everything on {index[q].date()}")
        drifted = [w * (1.0 + r) / (1.0 + gross) for w, r in zip(weights, day, strict=True)]
        turnover = 0.0
        weights = drifted
        decided = q - lag
        if decided >= 0 and scheduled[decided]:
            target = _checked_weights(values[decided], index[decided])
            target_exposure = sum(w * f for w, f in zip(target, _FACTORS, strict=True))
            current = sum(w * f for w, f in zip(drifted, _FACTORS, strict=True))
            if _needs_trade(target_exposure, current, rebalance.band):
                turnover = sum(abs(t - d) for t, d in zip(target, drifted, strict=True)) / 2.0
                weights = target
        cost = turnover * rate
        net = (1.0 + gross) * (1.0 - cost) - 1.0
        rows.append((net, gross, held, turnover, cost))

    daily = pd.DataFrame(
        rows,
        index=index[first + 1 : last + 1],
        columns=["return", "gross_return", "exposure", "turnover", "cost"],
    )
    return Simulation(daily=daily, start=index[first])


@dataclass(frozen=True)
class SimulationBatch:
    """One row per path, one column per return date; the same accounting as Simulation."""

    index: pd.DatetimeIndex
    start: pd.Timestamp
    returns: np.ndarray[Any, Any]
    gross_returns: np.ndarray[Any, Any]
    exposure: np.ndarray[Any, Any]
    turnover: np.ndarray[Any, Any]
    cost: np.ndarray[Any, Any]


def simulate_many(
    returns: pd.DataFrame,
    targets: np.ndarray[Any, Any],
    *,
    start: date,
    end: date | None,
    rebalance: Rebalance,
    cost_bps: float,
    delay_days: int = 0,
) -> SimulationBatch:
    """Simulate aligned target paths together, looping over dates rather than paths."""

    if list(returns.columns) != list(ASSETS):
        raise ValueError(f"returns columns must be {list(ASSETS)}")
    values = np.asarray(targets, dtype=float)
    if values.ndim != 2 or values.shape[1] != len(returns.index):
        raise ValueError("targets must have shape (paths, len(returns.index))")
    if delay_days < 0:
        raise ValueError("delay_days must not be negative")
    if cost_bps < 0:
        raise ValueError("cost_bps must not be negative")
    index = pd.DatetimeIndex(returns.index)
    first = int(index.searchsorted(pd.Timestamp(start)))
    last = (
        len(index) - 1
        if end is None
        else int(index.searchsorted(pd.Timestamp(end), side="right")) - 1
    )
    if first >= len(index) or last <= first:
        raise ValueError(f"no return dates between {start} and {end}")
    window = returns.iloc[first + 1 : last + 1]
    if window.isna().to_numpy().any():
        bad = window[window.isna().any(axis=1)]
        column = bad.columns[bad.isna().iloc[0].to_numpy()][0]
        raise ValueError(f"{column} has no return on {bad.index[0].date()} inside the window")

    lag = 1 + delay_days
    scheduled = schedule_mask(index, rebalance.schedule)
    opening = next((p for p in range(first - lag, -1, -1) if scheduled[p]), None)
    if opening is None:
        raise ValueError(
            f"path 0 no target is executable by {index[first].date()}; extend the lookback"
        )
    unusable = np.flatnonzero(~np.isfinite(values[:, opening]))
    if len(unusable):
        raise ValueError(
            f"path {int(unusable[0])} no target is executable by {index[first].date()};"
            " extend the lookback"
        )
    weights = _batch_checked_weights(values[:, opening], index[opening])
    rate = cost_bps / 10_000.0
    matrix = returns.to_numpy(dtype=float)
    shape = (values.shape[0], last - first)
    net_returns, gross_returns, exposure, turnover, costs = (np.empty(shape) for _ in range(5))
    for output_day, q in enumerate(range(first + 1, last + 1)):
        day = matrix[q]
        held = _batch_exposure(weights)
        # Preserve the scalar simulator's asset-order additions (including its initial zero).
        gross = sum(weights[:, a] * day[a] for a in range(len(ASSETS)))
        lost = np.flatnonzero(1.0 + gross <= 0.0)
        if len(lost):
            raise ValueError(f"path {int(lost[0])} lost everything on {index[q].date()}")
        drifted = weights * (1.0 + day) / (1.0 + gross[:, None])
        traded = np.zeros(values.shape[0])
        weights = drifted
        decided = q - lag
        if decided >= 0 and scheduled[decided]:
            target = _batch_checked_weights(values[:, decided], index[decided])
            target_exposure = _batch_exposure(target)
            current = _batch_exposure(drifted)
            needs_trade = (
                np.ones(values.shape[0], dtype=bool)
                if rebalance.band == 0.0
                else np.abs(target_exposure - current)
                > rebalance.band * np.maximum(target_exposure, _EPSILON)
            )
            traded[needs_trade] = (
                np.abs(target[needs_trade] - drifted[needs_trade]).sum(axis=1) / 2.0
            )
            weights = np.where(needs_trade[:, None], target, drifted)
        cost = traded * rate
        net_returns[:, output_day] = (1.0 + gross) * (1.0 - cost) - 1.0
        gross_returns[:, output_day] = gross
        exposure[:, output_day] = held
        turnover[:, output_day] = traded
        costs[:, output_day] = cost
    return SimulationBatch(
        index=index[first + 1 : last + 1],
        start=index[first],
        returns=net_returns,
        gross_returns=gross_returns,
        exposure=exposure,
        turnover=turnover,
        cost=costs,
    )


def _batch_exposure(weights: np.ndarray[Any, Any]) -> np.ndarray[Any, Any]:
    return np.asarray(sum(weights[:, a] * _FACTORS[a] for a in range(len(ASSETS))))


def _batch_checked_weights(
    values: np.ndarray[Any, Any], when: pd.Timestamp
) -> np.ndarray[Any, Any]:
    invalid = np.flatnonzero(~np.isfinite(values) | (values < 0.0) | (values > MAX_EXPOSURE))
    if len(invalid):
        path = int(invalid[0])
        try:
            _checked_weights(float(values[path]), when)
        except ValueError as exc:
            raise ValueError(f"path {path} {exc}") from None
    # The four columns are QQQ, cash, 2x, 3x, exactly as weights_for_exposure.
    return np.column_stack(
        (
            np.where(values <= 1.0, values, np.where(values <= 2.0, 2.0 - values, 0.0)),
            np.where(values <= 1.0, 1.0 - values, 0.0),
            np.where(values <= 1.0, 0.0, np.where(values <= 2.0, values - 1.0, 3.0 - values)),
            np.where(values <= 2.0, 0.0, values - 2.0),
        )
    )


def constant_targets(index: pd.Index, exposure: float) -> pd.Series:
    weights_for_exposure(exposure)
    return pd.Series(exposure, index=index, dtype=float)


def _checked_weights(value: float, when: pd.Timestamp) -> list[float]:
    try:
        return weights_for_exposure(float(value))
    except ValueError as exc:
        raise ValueError(f"target decided on {when.date()}: {exc}") from None


def _needs_trade(target: float, current: float, band: float) -> bool:
    if band == 0.0:
        return True
    return abs(target - current) > band * max(target, _EPSILON)
