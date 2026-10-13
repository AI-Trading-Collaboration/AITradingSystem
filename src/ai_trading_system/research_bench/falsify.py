"""Standard falsification (TRADING-2565 S3c): what a result has to survive before it counts.

Every check compares the strategy with its matched constant-exposure benchmark, as the main result
does, and keeps summaries only. Sections of the result (a preregistered rule reads
``<section>.<field>``):

- ``cost_<bps>``: the comparison rerun at each configured one-way cost (the unified cost included,
  so the keys do not depend on it);
- ``delay_<days>``: the comparison rerun with every trade that many more days late;
- ``paired``: strategy minus benchmark on the window's daily returns, the statistic the next four
  sections resample or cut;
- ``ci_low`` and ``ci_high``: the bounds of a stationary block bootstrap (Politis and Romano 1994)
  of the paired daily returns of strategy, benchmark and cash, at the run's confidence;
- ``leave_year_min``/``_max`` and ``leave_episode_min``/``_max``: the worst and best ``paired``
  value with one calendar year, or one fixed episode, removed from all three series (the full
  breakdown is kept under ``leave_year`` and ``leave_episode``);
- ``placebo_rank``: where the strategy's ``paired`` value ranks among same-exposure random
  timings (0 to 1, ties counted half). A placebo keeps the strategy's own target path but shuffles
  it in blocks, so its exposure distribution is the strategy's and only the timing is random. For a
  field where higher is better a rank near 1 means the timing beat chance; for volatility, near 0.

Reruns use the bench's own metrics. ``paired`` and the sections built on it annualize by trading
days (252), so their values can differ slightly from ``comparison``, which annualizes geometric
return by calendar years.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from ai_trading_system.research_bench.metrics import compare, cvar_field, summarize
from ai_trading_system.research_bench.settings import (
    BenchSettings,
    FalsificationSettings,
    LoadedSettings,
)
from ai_trading_system.research_bench.simulate import (
    Rebalance,
    Simulation,
    constant_targets,
    schedule_mask,
    simulate,
)

PAIRED_BASE_FIELDS: tuple[str, ...] = (
    "geometric_annual_return",
    "annual_volatility",
    "sharpe_vs_cash",
    "max_drawdown",
    "rolling_geometric_return_low",
)
# Sections whose fields are the paired fields.
PAIRED_SECTIONS: tuple[str, ...] = (
    "paired",
    "ci_low",
    "ci_high",
    "leave_year_min",
    "leave_year_max",
    "leave_episode_min",
    "leave_episode_max",
    "placebo_rank",
)
_CHUNK = 250
_TIE = 1e-12
Array = np.ndarray[Any, Any]


@dataclass(frozen=True)
class FalsificationInputs:
    """One finished run: its returns and targets, how it traded, and its two simulations."""

    returns: pd.DataFrame
    targets: pd.Series
    start: date
    end: date | None
    rebalance: Rebalance
    delay_days: int
    cost_bps: float
    strategy: Simulation
    benchmark: Simulation


def paired_fields(settings: BenchSettings) -> tuple[str, ...]:
    tails = [cvar_field(level) for level in settings.cvar_levels]
    return (*PAIRED_BASE_FIELDS, *tails, *(f"{tail}_relative_improvement" for tail in tails))


def cost_section(bps: float) -> str:
    return f"cost_{bps:g}".replace(".", "p")


def delay_section(days: int) -> str:
    return f"delay_{days}"


def falsify(
    inputs: FalsificationInputs, loaded: LoadedSettings, confidence: float
) -> dict[str, Any]:
    if not 0.0 < confidence < 1.0:
        raise ValueError("confidence must lie strictly between 0 and 1")
    settings = loaded.settings.falsification
    out: dict[str, Any] = {
        "settings": {**settings.model_dump(mode="json"), "confidence": confidence}
    }
    for bps in settings.cost_bps:
        out[cost_section(bps)] = _rerun(inputs, loaded, cost_bps=float(bps))
    for days in settings.delay_days:
        out[delay_section(days)] = _rerun(inputs, loaded, delay_days=inputs.delay_days + days)

    dates = inputs.strategy.returns.index
    strategy = inputs.strategy.returns.to_numpy(dtype=float)
    benchmark = inputs.benchmark.returns.reindex(dates).to_numpy(dtype=float)
    cash = inputs.returns["cash"].reindex(dates).to_numpy(dtype=float)
    observed = paired_comparison(strategy, benchmark, cash, loaded)
    out["paired"] = observed

    low, high = bootstrap_interval(strategy, benchmark, cash, loaded, confidence)
    out["ci_low"], out["ci_high"] = low, high

    out.update(leave_one_out(pd.DatetimeIndex(dates), strategy, benchmark, cash, loaded))

    placebo = placebo_comparisons(inputs, loaded)
    out["placebo_rank"] = {
        field: _rank(placebo[field], observed[field]) for field in paired_fields(loaded.settings)
    }
    return out


def paired_comparison(
    strategy: Array, benchmark: Array, cash: Array, loaded: LoadedSettings
) -> dict[str, Any]:
    """Strategy minus benchmark for one set of daily return series (1-D: floats) or many (2-D, one
    row per draw: arrays). The relative left-tail change is positive when the strategy's worst
    days are milder, as in ``metrics.compare``."""

    first = _statistics(strategy, cash, loaded)
    second = _statistics(benchmark, cash, loaded)
    out: dict[str, Any] = {}
    for field in paired_fields(loaded.settings):
        tail = field.removesuffix("_relative_improvement")
        if tail != field:
            with np.errstate(divide="ignore", invalid="ignore"):
                out[field] = (first[tail] - second[tail]) / np.abs(second[tail])
        else:
            out[field] = first[field] - second[field]
    if np.ndim(strategy) == 1:
        return {field: float(np.asarray(value)[0]) for field, value in out.items()}
    return out


def leave_one_out(
    dates: pd.DatetimeIndex, strategy: Array, benchmark: Array, cash: Array, loaded: LoadedSettings
) -> dict[str, Any]:
    """The paired comparison with each calendar year, and each fixed episode inside the window,
    removed; with the minimum and maximum of each field over the removals."""

    years = {str(year): dates.year != year for year in sorted(set(dates.year))}
    months = dates.to_period("M")
    episodes = {}
    for episode in loaded.settings.episodes:
        inside = (months >= pd.Period(episode.start, "M")) & (months <= pd.Period(episode.end, "M"))
        if inside.any():
            episodes[episode.id] = ~inside
    out: dict[str, Any] = {}
    for name, masks in (("year", years), ("episode", episodes)):
        groups = {
            group: paired_comparison(strategy[keep], benchmark[keep], cash[keep], loaded)
            for group, keep in masks.items()
            if keep.sum() > 1
        }
        out[f"leave_{name}"] = groups
        out[f"leave_{name}_min"] = _extreme(groups, min, loaded)
        out[f"leave_{name}_max"] = _extreme(groups, max, loaded)
    return out


def stationary_bootstrap_indices(
    length: int, draws: int, mean_block: float, rng: np.random.Generator
) -> Array:
    """Index paths of a stationary bootstrap, one row per draw: a block starts at a random day,
    runs on day by day (wrapping at the end) and ends with probability 1 / mean_block after each
    day."""

    if length < 1 or draws < 1 or mean_block < 1.0:
        raise ValueError("length and draws must be positive and mean_block at least 1")
    starts = rng.integers(0, length, size=(draws, length))
    new = rng.random((draws, length)) < 1.0 / mean_block
    new[:, 0] = True
    positions = np.arange(length)
    last_new = np.maximum.accumulate(np.where(new, positions, 0), axis=1)
    block_start = np.take_along_axis(starts, last_new, axis=1)
    return (block_start + (positions - last_new)) % length


def bootstrap_interval(
    strategy: Array,
    benchmark: Array,
    cash: Array,
    loaded: LoadedSettings,
    confidence: float,
) -> tuple[dict[str, float | None], dict[str, float | None]]:
    """Two-sided percentile bounds of the paired comparison under the stationary bootstrap."""

    settings: FalsificationSettings = loaded.settings.falsification
    fields = paired_fields(loaded.settings)
    rng = np.random.default_rng(settings.seed)
    collected: dict[str, list[Array]] = {field: [] for field in fields}
    remaining = settings.bootstrap_draws
    while remaining > 0:
        draws = min(_CHUNK, remaining)
        remaining -= draws
        idx = stationary_bootstrap_indices(
            len(strategy), draws, settings.bootstrap_mean_block_days, rng
        )
        paired = paired_comparison(strategy[idx], benchmark[idx], cash[idx], loaded)
        for field in fields:
            collected[field].append(np.asarray(paired[field], dtype=float))
    tail = (1.0 - confidence) / 2.0
    low: dict[str, float | None] = {}
    high: dict[str, float | None] = {}
    for field in fields:
        values = np.concatenate(collected[field])
        values = values[np.isfinite(values)]
        low[field] = float(np.quantile(values, tail)) if len(values) else None
        high[field] = float(np.quantile(values, 1.0 - tail)) if len(values) else None
    return low, high


def placebo_comparisons(inputs: FalsificationInputs, loaded: LoadedSettings) -> dict[str, Array]:
    """The paired comparison of each same-exposure random timing with the benchmark; every
    placebo is simulated exactly like the strategy."""

    benchmark = inputs.benchmark.returns.to_numpy(dtype=float)
    cash = inputs.returns["cash"].reindex(inputs.benchmark.returns.index).to_numpy(dtype=float)
    rows: list[Array] = []
    for targets in placebo_targets(inputs, loaded):
        sim = simulate(
            inputs.returns,
            targets,
            start=inputs.start,
            end=inputs.end,
            rebalance=inputs.rebalance,
            cost_bps=inputs.cost_bps,
            delay_days=inputs.delay_days,
        )
        rows.append(sim.returns.to_numpy(dtype=float))
    paired = paired_comparison(np.vstack(rows), benchmark, cash, loaded)
    return {field: np.asarray(value, dtype=float) for field, value in paired.items()}


def placebo_targets(inputs: FalsificationInputs, loaded: LoadedSettings) -> Iterator[pd.Series]:
    """Same-exposure random timings of the strategy's targets.

    The effective target path (for every date whose decision can trade in the window, the last
    decision the strategy's schedule acts on) is cut into blocks, and the blocks are put in a random
    order. Dates outside that span keep the strategy's own targets.
    """

    settings = loaded.settings.falsification
    rng = np.random.default_rng(settings.seed + 1)
    index = pd.DatetimeIndex(inputs.targets.index)
    lag = 1 + inputs.delay_days
    first = int(index.searchsorted(inputs.strategy.start))
    last = int(index.searchsorted(inputs.strategy.returns.index[-1]))
    lo, hi = first - lag, last - lag + 1
    values = inputs.targets.to_numpy(dtype=float)
    scheduled = np.asarray(schedule_mask(index, inputs.rebalance.schedule))
    acted = np.where(scheduled & np.isfinite(values), np.arange(len(values)), -1)
    acted = np.maximum.accumulate(acted)
    if lo < 0 or acted[lo] < 0:
        raise ValueError("the placebo needs the target the strategy opened with")
    path = values[acted[lo:hi]]
    blocks = [
        path[i : i + settings.placebo_block_days]
        for i in range(0, len(path), settings.placebo_block_days)
    ]
    for _ in range(settings.placebo_draws):
        order = rng.permutation(len(blocks))
        shuffled = values.copy()
        shuffled[lo:hi] = np.concatenate([blocks[i] for i in order])
        yield pd.Series(shuffled, index=index, name=inputs.targets.name)


def _statistics(returns: Array, cash: Array, loaded: LoadedSettings) -> dict[str, Array]:
    settings = loaded.settings
    per_year = settings.trading_days_per_year
    r = np.atleast_2d(np.asarray(returns, dtype=float))
    c = np.broadcast_to(np.atleast_2d(np.asarray(cash, dtype=float)), r.shape)
    days = r.shape[1]
    logs = np.log1p(r)
    excess = r - c
    equity = np.exp(np.cumsum(logs, axis=1))
    peaks = np.maximum(np.maximum.accumulate(equity, axis=1), 1.0)
    out: dict[str, Array] = {
        "geometric_annual_return": np.expm1(logs.mean(axis=1) * per_year),
        "annual_volatility": _std(r) * math.sqrt(per_year),
        "sharpe_vs_cash": _ratio(excess.mean(axis=1), _std(excess)) * math.sqrt(per_year),
        "max_drawdown": np.minimum((equity / peaks - 1.0).min(axis=1), 0.0),
        "rolling_geometric_return_low": _rolling_low(logs, loaded),
    }
    ordered = np.sort(r, axis=1)
    for level in settings.cvar_levels:
        count = max(1, math.ceil(level * days))
        out[cvar_field(level)] = ordered[:, :count].mean(axis=1)
    return out


def _std(values: Array) -> Array:
    if values.shape[1] < 2:
        return np.zeros(values.shape[0])
    return np.asarray(values.std(axis=1, ddof=1))


def _ratio(numerator: Array, denominator: Array) -> Array:
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(denominator > 0, numerator / denominator, np.nan)


def _rolling_low(logs: Array, loaded: LoadedSettings) -> Array:
    settings = loaded.settings
    per_year = settings.trading_days_per_year
    window = settings.rolling_years * per_year
    if logs.shape[1] < window:
        return np.full(logs.shape[0], np.nan)
    sums = np.cumsum(np.concatenate([np.zeros((logs.shape[0], 1)), logs], axis=1), axis=1)
    rolling = sums[:, window:] - sums[:, :-window]
    annual = np.expm1(rolling * per_year / window)
    return np.asarray(np.quantile(annual, settings.rolling_percentile, axis=1))


def _extreme(
    groups: dict[str, dict[str, Any]],
    pick: Callable[[list[float]], float],
    loaded: LoadedSettings,
) -> dict[str, float | None]:
    out: dict[str, float | None] = {}
    for field in paired_fields(loaded.settings):
        values = [float(item[field]) for item in groups.values() if math.isfinite(item[field])]
        out[field] = pick(values) if values else None
    return out


def _rank(values: Array, observed: float) -> float | None:
    """Share of placebo values below the observed one, ties counted half."""

    finite = values[np.isfinite(values)]
    if not len(finite) or not math.isfinite(observed):
        return None
    tolerance = _TIE * max(1.0, abs(observed))
    below = np.sum(finite < observed - tolerance)
    tied = np.sum(np.abs(finite - observed) <= tolerance)
    return float((below + 0.5 * tied) / len(finite))


def _rerun(
    inputs: FalsificationInputs,
    loaded: LoadedSettings,
    *,
    cost_bps: float | None = None,
    delay_days: int | None = None,
) -> dict[str, Any]:
    cost = inputs.cost_bps if cost_bps is None else cost_bps
    delay = inputs.delay_days if delay_days is None else delay_days
    sim = simulate(
        inputs.returns,
        inputs.targets,
        start=inputs.start,
        end=inputs.end,
        rebalance=inputs.rebalance,
        cost_bps=cost,
        delay_days=delay,
    )
    matched = simulate(
        inputs.returns,
        constant_targets(inputs.returns.index, float(sim.exposure.mean())),
        start=inputs.start,
        end=inputs.end,
        rebalance=inputs.rebalance,
        cost_bps=cost,
    )
    cash = inputs.returns["cash"]
    return compare(summarize(sim, cash, loaded), summarize(matched, cash, loaded), loaded)
