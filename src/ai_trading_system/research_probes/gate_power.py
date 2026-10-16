"""Gate power probe (TRADING-2565-17): can the gate recognise a real effect in one window as long
as the QQQ validation window?

Baker 2026-10-16 (decision card 先查尺子): QQQ-Q1..Q3 and the research loop's trials were all judged
by the same ruler, the eleven rules of QQQ-Q2/Q3 against the constant exposure with the same average
exposure, in one window of about ten years. This probe measures how often a rule whose effect is
known passes that ruler. It measures the ruler and tests no rule: the planted rules look ahead on
purpose (they know next month's volatility), so they never go through the bench's look-ahead check,
never enter a ledger and never count as trials. The three rules already tested (QQQ-Q1 q1a, QQQ-Q2
trend4 and ma10) are shown beside them and decide nothing.

Data: the data of trial plan T0001 (the trial mode's US market and QQQ), loaded as a trial run
loads them and cut at 2010-12-31 before any rule, window or effect is computed (the loaders derive
each column from earlier dates only); no validation or consistency window is evaluated.

- Windows: on the US market one per even start year 1930..2000, from the year's first trading date
  to 19 February ten years later (the calendar span of the QQQ validation window 2011-01-03 to
  2021-02-19); on QQQ the development window 1999-03-10..2010-12-31 alone, shown, never read for
  the verdict.
- Planted rules: at each month end the forecast is the log of next month's realized volatility
  plus noise (``noise`` times the forecast's own standard deviation times one fixed normal draw per
  month, the same draws at every level); a null rule's forecast is the draw alone. Exposure is one
  minus the forecast's expanding percentile (high forecast volatility, low exposure), rebalanced at
  month ends, as T0001's primary configuration maps its score.
- Each rule in each window is evaluated as a trial run evaluates it (``research_bench.run``):
  simulation, matched constant benchmark, comparison and the standard falsification at 0.95, plus
  the bootstrap lower bound at 0.9875 (the level of the next validation read, k = 4); then the
  eleven QQQ rules and the four US rules of ``research/_trials/prereg.yaml``.
- Leave-one-episode-out uses ``US_EPISODES`` on the US market (the bench's episodes are 2000 and
  later) and the bench's episodes on QQQ. A window without any episode has no leave-episode
  value; its two leave-episode rules count as holding and the window is flagged.

The reading (design note knowledge/claude/qqq-options/auto-loop/power-check/design-2026-10-16.md,
fixed before any run): among the planted levels whose left-tail improvement over the whole US period
is at least 20%, L is the one with the smallest improvement (on a tie the noisier one; whether the
improvement falls steadily with noise is reported); power is the share of US windows in which L
passes all eleven rules at 0.95. Below 50%: the ruler is too weak for one window; 80% or more: it
has power; in between: Baker decides. The null should pass in at most 5% of windows; above 10% is
reported as a loose gate.

Shown beside the reading, never changing it: the share over the non-overlapping windows and over the
windows without Saturday sessions (the US daily data may hold Saturdays before 1952, which give a
window more days than the validation window), each window's number of days, and the caveat that
QQQ-Q1's q1a takes its target volatility from all data before 1999-03-10, which on the US market
includes years after each window (a look-ahead the bench's check would refuse; q1a is only shown).
"""

from __future__ import annotations

import json
import math
import multiprocessing
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.checks import strategy_targets
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.falsify import (
    FalsificationInputs,
    bootstrap_interval,
    falsify,
)
from ai_trading_system.research_bench.metrics import compare, exposure_gap, summarize
from ai_trading_system.research_bench.prereg import evaluate_rules
from ai_trading_system.research_bench.run import PRIMARY_BENCHMARK, benchmark_simulations
from ai_trading_system.research_bench.settings import (
    EpisodeSpec,
    LoadedSettings,
    Window,
    load_settings,
    resolve_window,
)
from ai_trading_system.research_bench.simulate import Rebalance, schedule_mask, simulate
from ai_trading_system.research_data import qqq_long_history as qlh

# The end of the development windows: nothing later is read (no validation or consistency window).
CUTOFF = date(2010, 12, 31)
# Windows span the calendar of the QQQ validation window 2011-01-03..2021-02-19.
SPAN_END_MONTH, SPAN_END_DAY = 2, 19
US_FIRST_YEAR, US_LAST_YEAR, US_YEAR_STEP = 1930, 2000, 2
# The probe's design (fixed before any run, Baker's card 先查尺子 2026-10-16): noise levels in
# units of the signal's own standard deviation, one seed for every level, three years of forecasts
# before the first percentile.
NOISE_LEVELS: tuple[float, ...] = (0.0, 0.25, 0.5, 1.0, 1.5, 2.0, 3.0)
PLANTED_SEED = 20261016
WARMUP_MONTHS = 36
TRADING_DAYS = 252
# The gate's confidence (research/_trials/prereg.yaml) and the next validation read's (k = 4).
CONFIDENCE = 0.95
STRICT_CONFIDENCE = 0.9875
# The fixed reading (module docstring): it decides only what Claude proposes next, never a rule.
EFFECT_LINE = 0.20
POWER_LOW, POWER_HIGH = 0.5, 0.8
NULL_DESIGN, NULL_LIMIT = 0.05, 0.10
SATURDAY = 5  # pandas weekday
TRIAL_ID = "T0001"
MONTH_END = Rebalance(schedule="month_end", band=0.0)
TAIL_FIELD = "cvar_5pct_relative_improvement"
LEAVE_EPISODE = "leave_episode_min."
REAL_RULES: tuple[tuple[str, str], ...] = (
    ("QQQ-Q1", "q1a"),
    ("QQQ-Q2", "trend4"),
    ("QQQ-Q2", "ma10"),
)
NULL_RULE = "null"

# Peak and trough months, approximately, of the US market's bear markets (the S&P 500 down by
# about a fifth or more) and four shorter, sharp falls (1962, 1987, 1998, 2010), 1929 to 2010.
# Written before any run of this probe and not found from these data; used only for
# leave-one-episode-out.
US_EPISODES: tuple[EpisodeSpec, ...] = tuple(
    EpisodeSpec(id=episode_id, start=start, end=end)
    for episode_id, start, end in (
        ("crash_1929", "1929-09", "1932-06"),
        ("bear_1937", "1937-03", "1938-03"),
        ("war_1939", "1938-11", "1942-04"),
        ("postwar_1946", "1946-05", "1949-06"),
        ("bear_1956", "1956-08", "1957-10"),
        ("flash_crash_1962", "1961-12", "1962-06"),
        ("credit_crunch_1966", "1966-02", "1966-10"),
        ("bear_1968", "1968-11", "1970-05"),
        ("bear_1973", "1973-01", "1974-10"),
        ("bear_1976", "1976-09", "1978-03"),
        ("bear_1980", "1980-11", "1982-08"),
        ("crash_1987", "1987-08", "1987-12"),
        ("gulf_1990", "1990-07", "1990-10"),
        ("ltcm_1998", "1998-07", "1998-10"),
        ("dotcom_bear", "2000-03", "2002-10"),
        ("global_financial_crisis", "2007-10", "2009-03"),
        ("flash_crash_2010", "2010-04", "2010-07"),
    )
)

app = typer.Typer(
    help="查尺子（TRADING-2565-17）：门在一个 10 年窗口里认不认得出真效果。只读 2010 年以前。"
)


@dataclass(frozen=True)
class PowerRule:
    """A rule the probe evaluates: its daily targets on one market's calendar and its schedule."""

    rule_id: str
    kind: str  # "planted" or "real"
    noise: float | None
    targets: pd.Series
    rebalance: Rebalance


def cut(data: BenchData, cutoff: date = CUTOFF) -> BenchData:
    """The data up to and including ``cutoff``: nothing later reaches any computation."""

    keep = pd.DatetimeIndex(data.returns.index) <= pd.Timestamp(cutoff)
    if not keep.any():
        raise ValueError(f"no data on or before {cutoff}")
    inputs_keep = pd.DatetimeIndex(data.inputs.index) <= pd.Timestamp(cutoff)
    return BenchData(
        returns=data.returns.loc[keep],
        inputs=data.inputs.loc[inputs_keep],
        source=dict(data.source),
    )


def month_ends(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    """The bench's month-end decision dates (the last available date always counts)."""

    mask = np.asarray(schedule_mask(index, "month_end"), dtype=bool)
    return pd.DatetimeIndex(index[mask])


def next_month_log_vol(returns: pd.Series) -> pd.Series:
    """For each month end: the log of the annualized standard deviation of the daily returns after
    it, up to and including the next month end. The last month end, a month with fewer than two
    returns or a zero standard deviation give NaN."""

    index = pd.DatetimeIndex(returns.index)
    ends = month_ends(index)
    values = returns.to_numpy(dtype=float)
    positions = index.get_indexer(ends)
    out = np.full(len(ends), np.nan)
    for i in range(len(ends) - 1):
        chunk = values[positions[i] + 1 : positions[i + 1] + 1]
        chunk = chunk[np.isfinite(chunk)]
        if len(chunk) < 2:
            continue
        sd = float(np.std(chunk, ddof=1))
        if sd > 0:
            out[i] = math.log(sd * math.sqrt(TRADING_DAYS))
    return pd.Series(out, index=ends, name="next_month_log_vol")


def planted_forecast(
    log_vol: pd.Series, noise: float | None, draws: np.ndarray[Any, Any]
) -> pd.Series:
    """The planted forecast: log volatility plus ``noise`` times its standard deviation times the
    draw, or the draw alone for the null (``noise`` None); NaN wherever the log volatility is."""

    if len(draws) != len(log_vol):
        raise ValueError("one draw per month end is needed")
    values = log_vol.to_numpy(dtype=float)
    finite = np.isfinite(values)
    if noise is None:
        forecast = np.where(finite, draws, np.nan)
    else:
        if noise < 0:
            raise ValueError("noise must not be negative")
        spread = float(np.std(values[finite], ddof=1)) if finite.sum() > 1 else 0.0
        forecast = values + noise * spread * draws
    return pd.Series(forecast, index=log_vol.index, name="forecast")


def expanding_percentile(values: pd.Series, warmup: int) -> pd.Series:
    """Share of the earlier finite values below each value, ties counted half; NaN until
    ``warmup`` earlier values exist and wherever the value itself is NaN."""

    data = values.to_numpy(dtype=float)
    out = np.full(len(data), np.nan)
    seen: list[float] = []
    for i, value in enumerate(data):
        if not math.isfinite(value):
            continue
        if len(seen) >= warmup:
            prior = np.asarray(seen)
            below = float(np.sum(prior < value))
            tied = float(np.sum(prior == value))
            out[i] = (below + 0.5 * tied) / len(prior)
        seen.append(value)
    return pd.Series(out, index=values.index, name="percentile")


def planted_targets(
    inputs: pd.DataFrame,
    noise: float | None,
    *,
    seed: int = PLANTED_SEED,
    warmup: int = WARMUP_MONTHS,
) -> pd.Series:
    """Daily targets of a planted rule on the inputs' calendar: one minus the forecast's expanding
    percentile, decided at month ends and held until the next (NaN before the first decision)."""

    log_vol = next_month_log_vol(inputs["ndx_return"])
    draws = np.random.default_rng(seed).standard_normal(len(log_vol))
    score = expanding_percentile(planted_forecast(log_vol, noise, draws), warmup)
    exposure = (1.0 - score).reindex(pd.DatetimeIndex(inputs.index))
    return exposure.ffill().rename("target")


def us_windows(
    index: pd.DatetimeIndex,
    *,
    first_year: int = US_FIRST_YEAR,
    last_year: int = US_LAST_YEAR,
    step: int = US_YEAR_STEP,
) -> list[Window]:
    """One window per start year: from the year's first date in the index to 19 February ten years
    later; a window ending after the index or the cutoff is left out."""

    windows = []
    last = min(pd.Timestamp(index[-1]).date(), CUTOFF)
    for year in range(first_year, last_year + 1, step):
        position = int(index.searchsorted(pd.Timestamp(year, 1, 1)))
        if position >= len(index):
            continue
        end = date(year + 10, SPAN_END_MONTH, SPAN_END_DAY)
        if end > last:
            continue
        start = pd.Timestamp(index[position]).date()
        windows.append(Window(window_id=f"us_{year}", role="development", start=start, end=end))
    return windows


def non_overlapping(windows: Sequence[Window]) -> list[str]:
    """Window ids taken greedily from the earliest start, each starting after the last one taken
    ends."""

    taken: list[str] = []
    last_end: date | None = None
    for window in sorted(windows, key=lambda item: item.start):
        if last_end is None or window.start > last_end:
            taken.append(window.window_id)
            last_end = window.end
    return taken


def with_episodes(loaded: LoadedSettings, episodes: Sequence[EpisodeSpec]) -> LoadedSettings:
    """The same settings with other fixed episodes (leave-one-episode-out reads only these)."""

    settings = loaded.settings.model_copy(update={"episodes": tuple(episodes)})
    return replace(loaded, settings=settings)


def evaluate(
    data: BenchData,
    targets: pd.Series,
    window: Window,
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    rebalance: Rebalance,
) -> dict[str, Any]:
    """One rule in one window, evaluated as ``research_bench.run.run_strategy`` evaluates a trial
    (no look-ahead or parameter checks), with the gate's rules applied."""

    cost = loaded.cost_bps
    sim = simulate(
        data.returns,
        targets,
        start=window.start,
        end=window.end,
        rebalance=rebalance,
        cost_bps=cost,
    )
    benchmarks = benchmark_simulations(
        data,
        window,
        average_exposure=float(sim.exposure.mean()),
        rebalance=rebalance,
        cost_bps=cost,
        leveraged=False,
    )
    benchmark = benchmarks[PRIMARY_BENCHMARK]
    cash = data.returns["cash"]
    strategy_summary = summarize(sim, cash, loaded)
    benchmark_summaries = {name: summarize(item, cash, loaded) for name, item in benchmarks.items()}
    comparison = compare(strategy_summary, benchmark_summaries[PRIMARY_BENCHMARK], loaded)
    falsification = falsify(
        FalsificationInputs(
            returns=data.returns,
            targets=targets,
            start=window.start,
            end=window.end,
            rebalance=rebalance,
            delay_days=0,
            cost_bps=cost,
            strategy=sim,
            benchmark=benchmark,
        ),
        loaded,
        gate.confidence,
    )
    dates = pd.DatetimeIndex(sim.returns.index)
    strict_low, _ = bootstrap_interval(
        sim.returns.to_numpy(dtype=float),
        benchmark.returns.reindex(dates).to_numpy(dtype=float),
        cash.reindex(dates).to_numpy(dtype=float),
        loaded,
        STRICT_CONFIDENCE,
    )
    # The sections a trial run's rules read (trial.py merges the falsification and the run result).
    result = {
        **falsification,
        "strategy": strategy_summary,
        "benchmarks": benchmark_summaries,
        "comparison": comparison,
        "exposure_gap": exposure_gap(sim, float(sim.exposure.mean()), data.returns["qqq"]),
    }
    no_episode = not falsification["leave_episode"]
    rows = _rows(gate.markets["qqq"].rules, result, no_episode)
    strict = _rows(gate.markets["qqq"].rules, {**result, "ci_low": strict_low}, no_episode)
    us_rows = _rows(gate.markets["us_market"].rules, result, no_episode)
    return {
        "window_id": window.window_id,
        "start": window.start.isoformat(),
        "end": None if window.end is None else window.end.isoformat(),
        "average_exposure": float(sim.exposure.mean()),
        "return_days": len(dates),
        "saturdays": int((dates.weekday == SATURDAY).sum()),
        "episodes": list(falsification["leave_episode"]),
        "comparison": {
            key: comparison.get(key)
            for key in (TAIL_FIELD, "max_drawdown", "geometric_annual_return")
        },
        "no_episode": no_episode,
        "error": None,
        "gate": rows,
        "gate_strict": strict,
        "us_gate": us_rows,
        "holds": all(row["holds"] for row in rows),
        "holds_strict": all(row["holds"] for row in strict),
        "us_holds": all(row["holds"] for row in us_rows),
    }


def _rows(rules: Any, result: dict[str, Any], no_episode: bool) -> list[dict[str, Any]]:
    rows = evaluate_rules(rules, result)
    if no_episode:
        for row in rows:
            if row["metric"].startswith(LEAVE_EPISODE):
                row["holds"] = True
                row["no_episode"] = True
    return rows


def effect(
    data: BenchData,
    targets: pd.Series,
    window: Window,
    loaded: LoadedSettings,
    rebalance: Rebalance,
) -> dict[str, Any]:
    """The comparison alone over a whole period: a planted level's effect."""

    cost = loaded.cost_bps
    sim = simulate(
        data.returns,
        targets,
        start=window.start,
        end=window.end,
        rebalance=rebalance,
        cost_bps=cost,
    )
    benchmark = benchmark_simulations(
        data,
        window,
        average_exposure=float(sim.exposure.mean()),
        rebalance=rebalance,
        cost_bps=cost,
        leveraged=False,
    )[PRIMARY_BENCHMARK]
    cash = data.returns["cash"]
    comparison = compare(summarize(sim, cash, loaded), summarize(benchmark, cash, loaded), loaded)
    return {
        "window_id": window.window_id,
        "start": window.start.isoformat(),
        "end": None if window.end is None else window.end.isoformat(),
        "average_exposure": float(sim.exposure.mean()),
        TAIL_FIELD: comparison.get(TAIL_FIELD),
        "max_drawdown": comparison.get("max_drawdown"),
        "geometric_annual_return": comparison.get("geometric_annual_return"),
    }


def planted_rule_id(noise: float | None) -> str:
    return NULL_RULE if noise is None else f"noise_{noise:g}"


def rules_for(
    data: BenchData, root_dir: Path, noise_levels: Sequence[float] = NOISE_LEVELS
) -> list[PowerRule]:
    """The planted rules (null first, then each noise level) and the three tested rules with their
    preregistered parameters and schedules, all on the data's calendar."""

    rules = [
        PowerRule(
            rule_id=planted_rule_id(noise),
            kind="planted",
            noise=noise,
            targets=planted_targets(data.inputs, noise),
            rebalance=MONTH_END,
        )
        for noise in (None, *noise_levels)
    ]
    for question_id, strategy_id in REAL_RULES:
        question = lg.load_question(root_dir.joinpath(question_id))
        entry = question.prereg.strategy(strategy_id)
        function = lg.load_strategy(question, strategy_id)
        rules.append(
            PowerRule(
                rule_id=strategy_id,
                kind="real",
                noise=None,
                targets=strategy_targets(function, data.inputs, dict(entry.params)),
                rebalance=entry.rebalance.to_rebalance(),
            )
        )
    return rules


def reading(result: Mapping[str, Any]) -> dict[str, Any]:
    """The fixed reading of the US-market results (see the module docstring)."""

    effects = result["effects"]["us_market"]
    windows = [row for row in result["evaluations"] if row["market"] == "us_market"]
    levels = [
        (float(noise), effects[planted_rule_id(noise)][TAIL_FIELD])
        for noise in result["noise_levels"]
        if planted_rule_id(noise) in effects
    ]
    strong = [(gain, noise) for noise, gain in levels if gain is not None and gain >= EFFECT_LINE]
    gains = [gain for _, gain in levels if gain is not None]
    null_rate = share(windows, NULL_RULE, "holds")
    out: dict[str, Any] = {
        "effect_line": EFFECT_LINE,
        "monotone": all(later <= earlier for earlier, later in zip(gains, gains[1:], strict=False)),
        "null_rate": null_rate,
        "null_above_design": null_rate is not None and null_rate > NULL_DESIGN,
        "null_loose": null_rate is not None and null_rate > NULL_LIMIT,
    }
    if not strong:
        return {
            **out,
            "level": None,
            "power": None,
            "power_strict": None,
            "power_without_saturdays": None,
            "verdict": "no_level",
        }
    # The smallest improvement at or above the line; on a tie the noisier level.
    level = min(strong, key=lambda item: (item[0], -item[1]))[1]
    rule_id = planted_rule_id(level)
    plain = [row for row in windows if not row.get("saturdays")]
    power = share(windows, rule_id, "holds")
    failed = any(row["error"] for row in windows if row["rule_id"] in (rule_id, NULL_RULE))
    if failed:
        verdict = "incomplete"
    elif power is None:
        verdict = "no_level"
    elif power < POWER_LOW:
        verdict = "below_50"
    elif power >= POWER_HIGH:
        verdict = "at_least_80"
    else:
        verdict = "50_to_80"
    return {
        **out,
        "level": level,
        "level_effect": effects[rule_id][TAIL_FIELD],
        "power": power,
        "power_strict": share(windows, rule_id, "holds_strict"),
        "power_without_saturdays": share(plain, rule_id, "holds"),
        "verdict": verdict,
    }


def share(rows: Sequence[Mapping[str, Any]], rule_id: str, key: str) -> float | None:
    picked = [bool(row[key]) for row in rows if row["rule_id"] == rule_id]
    return sum(picked) / len(picked) if picked else None


_WORKER: dict[str, Any] = {}


def _init(payload: dict[str, Any]) -> None:
    _WORKER.clear()
    _WORKER.update(payload)


def _task(task: tuple[str, str, Window]) -> dict[str, Any]:
    market, rule_id, window = task
    rule: PowerRule = _WORKER["rules"][market][rule_id]
    try:
        row = evaluate(
            _WORKER["data"][market],
            rule.targets,
            window,
            _WORKER["loaded"][market],
            _WORKER["gate"],
            rule.rebalance,
        )
    except Exception as exc:  # one window must not lose a run of hours; the row says what failed
        error = f"{type(exc).__name__}: {exc}".splitlines()[0][: lg.ERROR_TEXT_LIMIT]
        row = failed_row(window, error)
    return {"market": market, "rule_id": rule_id, "kind": rule.kind, "noise": rule.noise, **row}


def failed_row(window: Window, error: str) -> dict[str, Any]:
    """A window the bench refused (for example no executable target at its start): kept in the
    result as an error that passes nothing, so one window cannot stop the whole run."""

    return {
        "window_id": window.window_id,
        "start": window.start.isoformat(),
        "end": _iso(window.end),
        "average_exposure": None,
        "return_days": None,
        "saturdays": None,
        "episodes": [],
        "comparison": {},
        "no_episode": False,
        "error": error,
        "gate": [],
        "gate_strict": [],
        "us_gate": [],
        "holds": False,
        "holds_strict": False,
        "us_holds": False,
    }


def run_power(
    datasets: Mapping[str, BenchData],
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    root_dir: Path,
    *,
    workers: int = 1,
    years: tuple[int, int, int] = (US_FIRST_YEAR, US_LAST_YEAR, US_YEAR_STEP),
    noise_levels: Sequence[float] = NOISE_LEVELS,
    progress: Callable[[int, int], None] | None = None,
) -> dict[str, Any]:
    """Every rule in every window of both markets, the planted effects and the reading."""

    for market in ("us_market", "qqq"):
        if market not in datasets:
            raise ValueError(f"missing {market} data")
        for frame in (datasets[market].returns, datasets[market].inputs):
            if pd.Timestamp(pd.DatetimeIndex(frame.index).max()).date() > CUTOFF:
                raise ValueError(f"{market} data run past {CUTOFF}; cut them first")
    if gate.confidence != CONFIDENCE:
        raise ValueError(f"the gate's confidence {gate.confidence} is not {CONFIDENCE}")
    us = datasets["us_market"]
    us_index = pd.DatetimeIndex(us.returns.index)
    windows = us_windows(us_index, first_year=years[0], last_year=years[1], step=years[2])
    if not windows:
        raise ValueError("no US window fits the data")
    qqq_window = resolve_window(loaded, "qqq_direction", "development")
    rules = {
        market: rules_for(datasets[market], root_dir, noise_levels)
        for market in ("us_market", "qqq")
    }
    settings = {"us_market": with_episodes(loaded, US_EPISODES), "qqq": loaded}
    full = Window(
        window_id="us_full",
        role="development",
        start=windows[0].start,
        end=pd.Timestamp(us_index[-1]).date(),
    )
    effects = {
        "us_market": {
            rule.rule_id: effect(us, rule.targets, full, loaded, rule.rebalance)
            for rule in rules["us_market"]
        },
        "qqq": {
            rule.rule_id: effect(datasets["qqq"], rule.targets, qqq_window, loaded, rule.rebalance)
            for rule in rules["qqq"]
        },
    }
    tasks = [
        ("us_market", rule.rule_id, window) for window in windows for rule in rules["us_market"]
    ]
    tasks += [("qqq", rule.rule_id, qqq_window) for rule in rules["qqq"]]
    payload = {
        "data": dict(datasets),
        "rules": {
            market: {rule.rule_id: rule for rule in items} for market, items in rules.items()
        },
        "loaded": settings,
        "gate": gate,
    }
    evaluations: list[dict[str, Any]] = []
    if workers <= 1:
        _init(payload)
        for done, task in enumerate(tasks, start=1):
            evaluations.append(_task(task))
            if progress is not None:
                progress(done, len(tasks))
    else:
        # Spawned workers on every platform, as on Windows where the probe runs: each worker gets
        # its own pickled copy of the data and rules.
        context = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(
            max_workers=workers, mp_context=context, initializer=_init, initargs=(payload,)
        ) as pool:
            for done, row in enumerate(pool.map(_task, tasks), start=1):
                evaluations.append(row)
                if progress is not None:
                    progress(done, len(tasks))
    trials = root_dir.joinpath(tr.TRIALS_DIR)
    result: dict[str, Any] = {
        "probe": "gate_power",
        "run": "TRADING-2565-17",
        "provenance": git_provenance(root_dir),
        "settings_sha256": loaded.sha256,
        "gate_sha256": lg.chain_digest(trials.joinpath(tr.GATE_FILE).read_bytes()),
        "plan_sha256": lg.chain_digest(trials.joinpath(f"{TRIAL_ID}.yaml").read_bytes()),
        "falsification_settings": loaded.settings.falsification.model_dump(mode="json"),
        "cutoff": CUTOFF.isoformat(),
        "confidence": CONFIDENCE,
        "strict_confidence": STRICT_CONFIDENCE,
        "noise_levels": [float(noise) for noise in noise_levels],
        "seed": PLANTED_SEED,
        "warmup_months": WARMUP_MONTHS,
        "windows": {
            "us_market": [
                {"window_id": w.window_id, "start": w.start.isoformat(), "end": _iso(w.end)}
                for w in windows
            ],
            "us_non_overlapping": non_overlapping(windows),
            "qqq": {
                "window_id": qqq_window.window_id,
                "start": qqq_window.start.isoformat(),
                "end": _iso(qqq_window.end),
            },
        },
        "episodes": {
            "us_market": [episode.model_dump() for episode in US_EPISODES],
            "qqq": [episode.model_dump() for episode in loaded.settings.episodes],
        },
        "effects": effects,
        "evaluations": evaluations,
    }
    result["summary"] = summary(result)
    result["reading"] = reading(result)
    return result


def summary(result: Mapping[str, Any]) -> dict[str, Any]:
    """Pass shares of each rule on the US windows (all and the non-overlapping ones), each gate
    rule's hold share, and each rule's QQQ development result."""

    us_rows = [row for row in result["evaluations"] if row["market"] == "us_market"]
    apart = set(result["windows"]["us_non_overlapping"])
    out: dict[str, Any] = {"us_market": {}, "qqq": {}}
    for rule_id in dict.fromkeys(row["rule_id"] for row in us_rows):
        rows = [row for row in us_rows if row["rule_id"] == rule_id]
        separate = [row for row in rows if row["window_id"] in apart]
        done = [row for row in rows if not row["error"]]
        metrics = [item["metric"] for item in done[0]["gate"]] if done else []
        out["us_market"][rule_id] = {
            "windows": len(rows),
            "errors": len(rows) - len(done),
            "holds": share(rows, rule_id, "holds"),
            "holds_strict": share(rows, rule_id, "holds_strict"),
            "us_holds": share(rows, rule_id, "us_holds"),
            "holds_non_overlapping": share(separate, rule_id, "holds"),
            "no_episode_windows": sum(bool(row["no_episode"]) for row in rows),
            "saturday_windows": sum(bool(row["saturdays"]) for row in done),
            "return_days": [min(days), max(days)] if (days := _days(done)) else None,
            "rule_holds": {
                metric: sum(
                    bool(next(item for item in row["gate"] if item["metric"] == metric)["holds"])
                    for row in done
                )
                / len(done)
                for metric in metrics
            },
        }
    for row in result["evaluations"]:
        if row["market"] == "qqq":
            out["qqq"][row["rule_id"]] = {
                "rules_held": sum(bool(item["holds"]) for item in row["gate"]),
                "rules": len(row["gate"]),
                "holds": row["holds"],
                "holds_strict": row["holds_strict"],
                "error": row["error"],
            }
    return out


def _days(rows: Sequence[Mapping[str, Any]]) -> list[int]:
    return [int(row["return_days"]) for row in rows if row.get("return_days") is not None]


def _iso(value: date | None) -> str | None:
    return None if value is None else value.isoformat()


def _clean(value: Any) -> Any:
    """JSON-safe: NaN and infinities become None."""

    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {key: _clean(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_clean(item) for item in value]
    if isinstance(value, np.floating):
        return _clean(float(value))
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.bool_):
        return bool(value)
    return value


VERDICT_TEXT = {
    "below_50": (
        "检验力低于 50%：尺子对单个 10 年窗口太没力，"
        "第一到三题的「证据不足」不能当负结果读；下一步先改尺子。"
    ),
    "50_to_80": "检验力在 50% 到 80% 之间：改尺子和换问题两样都写，Baker 定。",
    "at_least_80": "检验力至少 80%：尺子有力，之前的失败是真的负结果；下一步换问题。",
    "no_level": "没有一档假信号在整段美股历史上左尾改善达到 20%：判读条件不成立，原样报告。",
    "incomplete": "L 档或纯噪声有窗口评估出错：判读不完整，先查错误再读。",
}


def _pct(value: Any) -> str:
    return "-" if value is None else f"{float(value) * 100:.1f}%"


def render_markdown(result: Mapping[str, Any]) -> str:
    read = result["reading"]
    us = result["summary"]["us_market"]
    effects = result["effects"]["us_market"]
    windows = result["windows"]
    lines = [
        "# 查尺子结果（TRADING-2565-17）",
        "",
        f"- 数据截至 {result['cutoff']}；美股窗口 {len(windows['us_market'])} 个"
        f"（不重叠 {len(windows['us_non_overlapping'])} 个）；QQQ 只看开发窗口"
        f" {windows['qqq']['start']}..{windows['qqq']['end']}。",
        f"- 判读：{VERDICT_TEXT[read['verdict']]}",
    ]
    if read["level"] is not None:
        lines.append(
            f"- L = 噪声 {read['level']:g}（整段美股左尾改善 {_pct(read['level_effect'])}）；"
            f"11 条全过的窗口比例 {_pct(read['power'])}，"
            f"区间按 98.75% 时 {_pct(read['power_strict'])}。"
        )
    null_note = "。"
    if read["null_loose"]:
        null_note = "，超过 10%，门偏松。"
    elif read["null_above_design"]:
        null_note = "，高于设计的 5%，未到 10%。"
    lines.append(f"- 纯噪声（没有信息）11 条全过的窗口比例 {_pct(read['null_rate'])}{null_note}")
    if not read["monotone"]:
        lines.append("- 注意：左尾改善没有随噪声变大而一路变小；L 按改善最小的一档取。")
    null_row = us.get(NULL_RULE, {})
    if null_row.get("return_days"):
        low, high = null_row["return_days"]
        lines.append(
            f"- 美股窗口每个 {low} 到 {high} 个交易日（验证窗口约 2550 个）；"
            f"含周六交易的窗口 {null_row['saturday_windows']} 个，"
            f"不含周六的窗口里 L 的过门比例 {_pct(read.get('power_without_saturdays'))}"
            "（只看，不改判读）。"
        )
    lines += [
        f"- {len(windows['us_market'])} 个窗口大量重叠（不重叠的只有"
        f" {len(windows['us_non_overlapping'])} 个），比例的抽样误差不小，"
        "读的时候和不重叠那一列一起看。",
        "- 三条真规则只描述；q1a 的目标波动取 1999-03-10 以前的中位数，"
        "在美股上包含了窗口之后的年份（偷看未来，评估台的前视检查会拒绝），只能参考。",
    ]
    failed = [row for row in result["evaluations"] if row["error"]]
    if failed:
        lines.append(f"- 评估出错 {len(failed)} 个（不算过门），第一个：{failed[0]['error']}")
    lines += [
        "",
        "## 每条规则在美股窗口上",
        "",
        "| 规则 | 整段左尾改善 | 整段回撤改善 | 11 条全过 | 98.75% "
        "| 美股 4 条 | 不重叠窗口 | 无段窗口 |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for rule_id, row in us.items():
        gain = effects.get(rule_id, {})
        lines.append(
            f"| {rule_id} | {_pct(gain.get(TAIL_FIELD))} | {_pct(gain.get('max_drawdown'))} | "
            f"{_pct(row['holds'])} | {_pct(row['holds_strict'])} | {_pct(row['us_holds'])} | "
            f"{_pct(row['holds_non_overlapping'])} | {row['no_episode_windows']} |"
        )
    if read["level"] is not None:
        level_rows = us[planted_rule_id(read["level"])]["rule_holds"]
        lines += ["", "## L 档每条规则成立的窗口比例", "", "| 规则 | 成立比例 |", "|---|---|"]
        lines += [f"| {metric} | {_pct(value)} |" for metric, value in level_rows.items()]
    lines += [
        "",
        "## QQQ 开发窗口（单个窗口，只描述）",
        "",
        "| 规则 | 左尾改善 | 回撤改善 | 11 条成立 | 全过 |",
        "|---|---|---|---|---|",
    ]
    qqq_effects = result["effects"]["qqq"]
    for rule_id, row in result["summary"]["qqq"].items():
        gain = qqq_effects.get(rule_id, {})
        lines.append(
            f"| {rule_id} | {_pct(gain.get(TAIL_FIELD))} | {_pct(gain.get('max_drawdown'))} | "
            f"{row['rules_held']}/{row['rules']} | {'是' if row['holds'] else '否'} |"
        )
    lines.append("")
    return "\n".join(lines)


def write_result(result: Mapping[str, Any], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "power_result.json").write_text(
        json.dumps(_clean(dict(result)), ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    (out_dir / "power_result.md").write_text(render_markdown(result), encoding="utf-8")


def load_datasets(
    snapshot_root: Path,
) -> tuple[dict[str, BenchData], tr.TrialGate, LoadedSettings, Path]:
    """The trial plan T0001's data for both markets, cut at 2010-12-31."""

    loaded = load_settings()
    root_dir = lg.research_root(loaded)
    gate, plan = tr.load_trial(root_dir, TRIAL_ID)
    datasets = {
        market: cut(
            tr.market_data(
                market, gate, plan, snapshot_root=snapshot_root, project_root=PROJECT_ROOT
            )
        )
        for market in ("us_market", "qqq")
    }
    return datasets, gate, loaded, root_dir


@app.callback()
def main() -> None:
    """查尺子。"""


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="结果目录，写 power_result.json 和 power_result.md。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
    workers: Annotated[int, typer.Option(help="并行进程数。")] = 1,
) -> None:
    """在美股窗口和 QQQ 开发窗口上评估假信号和三条已检验的规则，写汇总和判读。"""

    try:
        datasets, gate, loaded, root_dir = load_datasets(root)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        typer.echo(f"gate_power: 数据载入失败：{exc}")
        raise typer.Exit(code=1) from None

    def progress(done: int, total: int) -> None:
        if done == total or done % 25 == 0:
            typer.echo(f"gate_power: {done}/{total}")

    typer.echo(
        "gate_power: 数据已载入并截到 2010-12-31；先算整段效果（几分钟，没有进度），再逐个窗口评估"
    )
    result = run_power(datasets, loaded, gate, root_dir, workers=workers, progress=progress)
    result["source"] = {market: dict(data.source) for market, data in datasets.items()}
    write_result(result, out)
    read = result["reading"]
    typer.echo(
        f"gate_power: 判读 {read['verdict']}；L {read['level']}；检验力 {_pct(read['power'])}；"
        f"纯噪声 {_pct(read['null_rate'])}；写入 {out / 'power_result.json'}"
    )


if __name__ == "__main__":
    app()
