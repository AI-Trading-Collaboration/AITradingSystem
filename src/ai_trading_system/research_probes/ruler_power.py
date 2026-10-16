"""Two-stage ruler probe (TRADING-2565-18): how often does a rule whose effect is known pass a
ruler that judges it on a long history first and then only checks its direction in the following
ten years?

Baker 2026-10-16 (decision card 两段尺子, after the gate power probe TRADING-2565-17 found that
the trial gate's eleven rules recognise a real effect in one ten-year window only about one time in
five): before the trial gate changes, the candidate two-stage rulers are measured with the same
planted rules, and the one that is adopted is chosen by a reading written before any run. Like
``gate_power``, this probe measures rulers and tests no rule: planted rules look ahead on purpose,
never go through the bench's look-ahead check, never enter a ledger and never count as trials. The
three rules already tested (QQQ-Q1 q1a, QQQ-Q2 trend4 and ma10) are shown after the choice and
decide nothing.

Data: the US market of trial plan T0001, loaded and cut at 2010-12-31 as ``gate_power`` loads it;
no validation or consistency window is evaluated. QQQ is not used (its development window is one
window of twelve years).

- Windows on the US market, each start year even from 1930: check windows of ten years (the
  ``gate_power`` windows, from the year's first trading date to 19 February ten years later, the
  calendar span of the QQQ validation window); long windows of 20, 30 and 40 years (from the
  year's first trading date to 31 December of the last year); and the whole 1930-2010.
- Planted rules, two families. Volatility (the ``gate_power`` family, the same seed and levels):
  the forecast is the log of next month's realized volatility plus noise. Drawdown (held out, never
  used to choose the ruler): the forecast is next month's deepest fall from a running peak plus
  noise, with its own seed. Exposure is one minus the forecast's expanding percentile, rebalanced at
  month ends, as in ``gate_power``.
- Each rule in each window is evaluated as a trial run evaluates it (simulation, matched constant
  benchmark, comparison, the standard falsification at 0.95) and the bootstrap interval of the
  left-tail improvement is kept at 0.95, 0.99 and 0.997 (0.997: about a Bonferroni correction over
  the 31 trials so far). The eleven QQQ rules of the gate ``gate_power`` measured
  (``gate_power.MEASURED_GATE``) are applied at each of the three levels (only the interval rule
  changes).

Candidate rulers: stage 1 on the long window, either the core four rules (interval lower bound
above 0, drawdown improvement at least 5%, geometric return and the 25 bp cost case not below -1%)
or all eleven, at each confidence; stage 2 on the next ten-year window, either S2 (left-tail and
drawdown point estimates both improve and the geometric return is not below -1%) or S3 (the upper
bound of the left-tail improvement's 95% interval is above 0 and the geometric return is not below
-1%). A pair is a long window of 30 or 40 years and the check window that starts the year after it
ends; a rule passes a pair when it passes both stages.

The reading (design note knowledge/claude/qqq-options/auto-loop/power-check/
ruler-v2-design-2026-10-16.md, fixed before any run):

1. In each family, L is the planted level with the smallest left-tail improvement over the whole
   US period among those at least 20% (on a tie the noisier one); the zero-effect rules are the
   null and every level whose improvement is below 5%.
2. A candidate is eligible when every zero-effect volatility rule passes at most 5% of the pairs,
   for both pair lengths. Its score is the smaller of L's pass shares over the two pair lengths.
3. The chosen candidate has the highest score; on a tie the core four before all eleven, then the
   higher confidence, then S2 before S3.
4. Holdout: on the drawdown family the chosen candidate needs L at 80% or more and every zero-effect
   rule at 5% or less, for both pair lengths.
5. Verdict: adopt (score at least 80% and the holdout holds), holdout_failed, below_80 (best score
   under 80%), none_eligible, holdout_no_level or no_level (no planted level reaches 20%), or
   incomplete (an error in any planted rule's pair windows, or a planted level without a
   whole-period effect). Only adopt leads to a change of the trial gate, which Baker approves
   (zone C); every other verdict is reported as it is.

With 16 pairs of 40+10 years and 21 of 30+10, "at most 5%" allows no false pass in 16 pairs and
one in 21, and "80%" needs 13 of 16 and 17 of 21; the summary states these counts.
"""

from __future__ import annotations

import json
import math
import multiprocessing
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import typer

from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.falsify import (
    _CHUNK,
    FalsificationInputs,
    falsify,
    paired_comparison,
    paired_fields,
    stationary_bootstrap_indices,
)
from ai_trading_system.research_bench.metrics import compare, exposure_gap, summarize
from ai_trading_system.research_bench.prereg import evaluate_rules
from ai_trading_system.research_bench.run import PRIMARY_BENCHMARK, benchmark_simulations
from ai_trading_system.research_bench.settings import LoadedSettings, Window
from ai_trading_system.research_bench.simulate import Rebalance, simulate
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import gate_power as gp

RUN_ID = "TRADING-2565-18"
# Fixed before any run (Baker's card 两段尺子 2026-10-16): ten-year check windows as in
# gate_power, long windows of 20, 30 and 40 years, pairs of a 30- or 40-year long window and the
# next check window, starts every second year from 1930.
CHECK_YEARS = 10
SPANS: tuple[int, ...] = (20, 30, 40)
PAIR_SPANS: tuple[int, ...] = (30, 40)
FIRST_YEAR, YEAR_STEP = 1930, 2
FULL_ID = "us_full"
JOURNAL_FILE = "ruler_rows.jsonl"
# The interval levels: the gate's, 0.99, and about Bonferroni over the 31 trials so far.
CONFIDENCES: tuple[float, ...] = (0.95, 0.99, 0.997)
# The held-out drawdown family has its own draws.
HOLDOUT_SEED = 20261017
DRAWDOWN = "dd_"
# The reading (module docstring).
EFFECT_LINE = gp.EFFECT_LINE
ZERO_LINE = 0.05
POWER_LINE = 0.8
FALSE_PASS_LIMIT = 0.05
TAIL_FIELD = gp.TAIL_FIELD
CI_METRIC = f"ci_low.{TAIL_FIELD}"
MDD_METRIC = "comparison.max_drawdown"
GEO_METRIC = "comparison.geometric_annual_return"
COST_METRIC = "cost_25.geometric_annual_return"
CORE_METRICS: tuple[str, ...] = (CI_METRIC, MDD_METRIC, GEO_METRIC, COST_METRIC)
STAGE1 = ("core4", "all11")
STAGE2 = ("S2", "S3")

app = typer.Typer(
    help="两段尺子（TRADING-2565-18）：长历史主检验加十年查方向，认不认得出真效果。"
    "只读 2010 年以前。"
)


def confidence_key(confidence: float) -> str:
    return f"{confidence:g}"


# --- planted rules ------------------------------------------------------------------------------


def next_month_drawdown(returns: pd.Series) -> pd.Series:
    """For each month end: the deepest fall from a running peak of the wealth over the days after
    it, up to and including the next month end, as a positive fraction (0 when it never falls
    below the month end's level or a later peak). The last month end and a month with fewer than
    two returns give NaN."""

    index = pd.DatetimeIndex(returns.index)
    ends = gp.month_ends(index)
    values = returns.to_numpy(dtype=float)
    positions = index.get_indexer(ends)
    out = np.full(len(ends), np.nan)
    for i in range(len(ends) - 1):
        chunk = values[positions[i] + 1 : positions[i + 1] + 1]
        chunk = chunk[np.isfinite(chunk)]
        if len(chunk) < 2:
            continue
        wealth = np.concatenate(([1.0], np.cumprod(1.0 + chunk)))
        peak = np.maximum.accumulate(wealth)
        out[i] = float(np.max(1.0 - wealth / peak))
    return pd.Series(out, index=ends, name="next_month_drawdown")


def drawdown_targets(
    inputs: pd.DataFrame,
    noise: float | None,
    *,
    seed: int = HOLDOUT_SEED,
    warmup: int = gp.WARMUP_MONTHS,
) -> pd.Series:
    """Daily targets of a held-out planted rule: one minus the expanding percentile of next
    month's drawdown plus noise (or the noise alone for the null), decided at month ends."""

    signal = next_month_drawdown(inputs["ndx_return"])
    draws = np.random.default_rng(seed).standard_normal(len(signal))
    score = gp.expanding_percentile(gp.planted_forecast(signal, noise, draws), warmup)
    exposure = (1.0 - score).reindex(pd.DatetimeIndex(inputs.index))
    return exposure.ffill().rename("target")


def power_rules(
    data: BenchData, root_dir: Path, noise_levels: Sequence[float] = gp.NOISE_LEVELS
) -> list[gp.PowerRule]:
    """The volatility family and the three tested rules (as ``gate_power`` builds them), then the
    drawdown family (``dd_null``, ``dd_noise_<level>``)."""

    rules = gp.rules_for(data, root_dir, noise_levels)
    rules += [
        gp.PowerRule(
            rule_id=DRAWDOWN + gp.planted_rule_id(noise),
            kind="planted",
            noise=noise,
            targets=drawdown_targets(data.inputs, noise),
            rebalance=gp.MONTH_END,
        )
        for noise in (None, *noise_levels)
    ]
    return rules


def family(rule_id: str, kind: str) -> str:
    if kind != "planted":
        return "real"
    return "drawdown" if rule_id.startswith(DRAWDOWN) else "volatility"


# --- windows ------------------------------------------------------------------------------------


def check_windows(
    index: pd.DatetimeIndex,
    *,
    first_year: int = FIRST_YEAR,
    step: int = YEAR_STEP,
    years: int = CHECK_YEARS,
) -> list[Window]:
    """One window per start year, from the year's first date in the index to 19 February
    ``years`` later; a year without a date in the index, or a window ending after the index or
    the cutoff, is left out."""

    last = min(pd.Timestamp(index[-1]).date(), gp.CUTOFF)
    windows = []
    for year in range(first_year, last.year + 1, step):
        position = int(index.searchsorted(pd.Timestamp(year, 1, 1)))
        if position >= len(index):
            continue
        start = pd.Timestamp(index[position]).date()
        end = date(year + years, gp.SPAN_END_MONTH, gp.SPAN_END_DAY)
        if start.year != year or end > last:
            continue
        windows.append(Window(window_id=f"us_{year}", role="development", start=start, end=end))
    return windows


def span_windows(
    index: pd.DatetimeIndex, span: int, *, first_year: int = FIRST_YEAR, step: int = YEAR_STEP
) -> list[Window]:
    """One window of ``span`` calendar years per start year, from the year's first date in the
    index to 31 December of its last year; a year without a date in the index, or a window ending
    after the index or the cutoff, is left out."""

    last = min(pd.Timestamp(index[-1]).date(), gp.CUTOFF)
    windows = []
    for year in range(first_year, last.year + 1, step):
        position = int(index.searchsorted(pd.Timestamp(year, 1, 1)))
        if position >= len(index):
            continue
        start = pd.Timestamp(index[position]).date()
        end = date(year + span - 1, 12, 31)
        if start.year != year or end > last:
            continue
        windows.append(
            Window(window_id=f"us{span}_{year}", role="development", start=start, end=end)
        )
    return windows


def pair_list(
    long_windows: Mapping[int, Sequence[Window]],
    checks: Sequence[Window],
    pair_spans: Sequence[int] = PAIR_SPANS,
) -> dict[int, list[tuple[str, str]]]:
    """For each pair length: (long window id, check window id) where the check window is the one
    starting in the year after the long window ends; long windows without one are left out."""

    by_year = {window.start.year: window for window in checks}
    out: dict[int, list[tuple[str, str]]] = {}
    for span in pair_spans:
        out[span] = []
        for window in long_windows.get(span, ()):
            check = by_year.get(window.start.year + span)
            if check is None or window.end is None or check.start <= window.end:
                continue
            out[span].append((window.window_id, check.window_id))
    return out


def ordered_windows(
    full: Window, long: Mapping[int, Sequence[Window]], checks: Sequence[Window], check_years: int
) -> list[tuple[Window, int | None]]:
    """Every window with its length in years (None for the whole period), longest first so the
    slowest evaluations do not come last."""

    windows: list[tuple[Window, int | None]] = [(full, None)]
    for span in sorted(long, reverse=True):
        windows += [(window, span) for window in long[span]]
    return windows + [(window, check_years) for window in checks]


# --- one evaluation -----------------------------------------------------------------------------


def bootstrap_bounds(
    strategy: np.ndarray[Any, Any],
    benchmark: np.ndarray[Any, Any],
    cash: np.ndarray[Any, Any],
    loaded: LoadedSettings,
    confidences: Sequence[float],
    field: str = TAIL_FIELD,
) -> dict[str, list[float | None]]:
    """``falsify.bootstrap_interval``'s two-sided bounds of one field at several confidences from
    one set of draws: the same seed and chunks, so each pair equals a separate call's."""

    settings = loaded.settings.falsification
    if field not in paired_fields(loaded.settings):
        raise ValueError(f"unknown paired field {field}")
    rng = np.random.default_rng(settings.seed)
    collected: list[np.ndarray[Any, Any]] = []
    remaining = settings.bootstrap_draws
    while remaining > 0:
        draws = min(_CHUNK, remaining)
        remaining -= draws
        idx = stationary_bootstrap_indices(
            len(strategy), draws, settings.bootstrap_mean_block_days, rng
        )
        paired = paired_comparison(strategy[idx], benchmark[idx], cash[idx], loaded)
        collected.append(np.asarray(paired[field], dtype=float))
    values = np.concatenate(collected)
    values = values[np.isfinite(values)]
    out: dict[str, list[float | None]] = {}
    for confidence in confidences:
        if not 0.0 < confidence < 1.0:
            raise ValueError("confidence must lie strictly between 0 and 1")
        tail = (1.0 - confidence) / 2.0
        out[confidence_key(confidence)] = (
            [float(np.quantile(values, tail)), float(np.quantile(values, 1.0 - tail))]
            if len(values)
            else [None, None]
        )
    return out


def gate_rows(rules: Any, result: dict[str, Any], no_episode: bool) -> list[dict[str, Any]]:
    """The gate's rules on one result; without any episode in the window the leave-episode rules
    count as holding and are flagged (as in ``gate_power``)."""

    rows = evaluate_rules(rules, result)
    if no_episode:
        for row in rows:
            if row["metric"].startswith(gp.LEAVE_EPISODE):
                row["holds"] = True
                row["no_episode"] = True
    return rows


def evaluate(
    data: BenchData,
    targets: pd.Series,
    window: Window,
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    rebalance: Rebalance,
    confidences: Sequence[float] = CONFIDENCES,
) -> dict[str, Any]:
    """One rule in one window, evaluated as a trial run evaluates it, with the gate's QQQ rules at
    the gate's confidence and the rules' holds with the interval at each confidence."""

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
    bounds = bootstrap_bounds(
        sim.returns.to_numpy(dtype=float),
        benchmark.returns.reindex(dates).to_numpy(dtype=float),
        cash.reindex(dates).to_numpy(dtype=float),
        loaded,
        confidences,
    )
    result = {
        **falsification,
        "strategy": strategy_summary,
        "benchmarks": benchmark_summaries,
        "comparison": comparison,
        "exposure_gap": exposure_gap(sim, float(sim.exposure.mean()), data.returns["qqq"]),
    }
    no_episode = not falsification["leave_episode"]
    rules = gate.markets["qqq"].rules
    rows = gate_rows(rules, result, no_episode)
    holds_at = {}
    for confidence in confidences:
        key = confidence_key(confidence)
        low = {**(result.get("ci_low") or {}), TAIL_FIELD: bounds[key][0]}
        holds_at[key] = {
            row["metric"]: bool(row["holds"])
            for row in gate_rows(rules, {**result, "ci_low": low}, no_episode)
        }
    return {
        "window_id": window.window_id,
        "start": window.start.isoformat(),
        "end": gp._iso(window.end),
        "average_exposure": float(sim.exposure.mean()),
        "return_days": len(dates),
        "saturdays": int((dates.weekday == gp.SATURDAY).sum()),
        "episodes": list(falsification["leave_episode"]),
        "comparison": {
            key: comparison.get(key)
            for key in (TAIL_FIELD, "max_drawdown", "geometric_annual_return")
        },
        "tail_bounds": bounds,
        "no_episode": no_episode,
        "error": None,
        "gate": rows,
        "holds_at": holds_at,
    }


def failed_row(window: Window, error: str) -> dict[str, Any]:
    """A window the bench refused or that raised: kept as an error that passes nothing."""

    return {
        "window_id": window.window_id,
        "start": window.start.isoformat(),
        "end": gp._iso(window.end),
        "average_exposure": None,
        "return_days": None,
        "saturdays": None,
        "episodes": [],
        "comparison": {},
        "tail_bounds": {},
        "no_episode": False,
        "error": error,
        "gate": [],
        "holds_at": {},
    }


# --- the two stages -----------------------------------------------------------------------------


def stage1(row: Mapping[str, Any], ruler: str, confidence: str) -> bool:
    """Stage 1 on a long window: the core four rules or all eleven, with the interval at the
    given confidence."""

    if row.get("error") or confidence not in row.get("holds_at", {}):
        return False
    holds: Mapping[str, bool] = row["holds_at"][confidence]
    metrics = CORE_METRICS if ruler == "core4" else tuple(holds)
    if not metrics or any(metric not in holds for metric in metrics):
        return False
    return all(holds[metric] for metric in metrics)


def stage2(row: Mapping[str, Any], check: str) -> bool:
    """Stage 2 on a check window: S2 (left-tail and drawdown point estimates both improve) or S3
    (the 95% interval's upper bound of the left-tail improvement is above 0), each with the
    geometric return not below -1%."""

    if row.get("error"):
        return False
    geo = row.get("holds_at", {}).get(confidence_key(gp.CONFIDENCE), {}).get(GEO_METRIC)
    if not geo:
        return False
    if check == "S2":
        comparison = row.get("comparison") or {}
        tail, drawdown = comparison.get(TAIL_FIELD), comparison.get("max_drawdown")
        return tail is not None and drawdown is not None and tail > 0 and drawdown > 0
    if check == "S3":
        bounds = (row.get("tail_bounds") or {}).get(confidence_key(gp.CONFIDENCE))
        return bounds is not None and bounds[1] is not None and bounds[1] > 0
    raise ValueError(f"unknown check {check}")


def candidates(confidences: Sequence[float] = CONFIDENCES) -> list[dict[str, Any]]:
    """Every candidate ruler in the tie order of the reading: the core four before all eleven,
    the higher confidence first, S2 before S3."""

    out = []
    for ruler in STAGE1:
        for confidence in sorted(confidences, reverse=True):
            for check in STAGE2:
                key = confidence_key(confidence)
                out.append(
                    {
                        "candidate": f"{ruler}@{key}+{check}",
                        "stage1": ruler,
                        "confidence": key,
                        "stage2": check,
                    }
                )
    return out


def _index(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str], Mapping[str, Any]]:
    return {(row["rule_id"], row["window_id"]): row for row in rows}


def pass_shares(
    result: Mapping[str, Any], candidate: Mapping[str, Any]
) -> dict[str, dict[str, Any]]:
    """For each rule: the two-stage pass share over the pairs of each length, the stage-1 share
    over the long windows of each span, the stage-1 result on the whole period and the stage-2
    share over the check windows."""

    rows = _index(result["evaluations"])
    rule_ids = list(dict.fromkeys(row["rule_id"] for row in result["evaluations"]))
    windows = result["windows"]
    out: dict[str, dict[str, Any]] = {}
    for rule_id in rule_ids:

        def one(window_id: str, stage: str, rid: str = rule_id) -> bool:
            row = rows.get((rid, window_id))
            if row is None:
                return False
            if stage == "1":
                return stage1(row, candidate["stage1"], candidate["confidence"])
            return stage2(row, candidate["stage2"])

        pairs = {}
        for span, items in windows["pairs"].items():
            hits = [one(long_id, "1") and one(check_id, "2") for long_id, check_id in items]
            pairs[str(span)] = sum(hits) / len(hits) if hits else None
        spans = {}
        for span, items in windows["long"].items():
            hits = [one(item["window_id"], "1") for item in items]
            spans[str(span)] = sum(hits) / len(hits) if hits else None
        checks = [one(item["window_id"], "2") for item in windows["check"]]
        out[rule_id] = {
            "pairs": pairs,
            "stage1": spans,
            "full": one(FULL_ID, "1"),
            "stage2": sum(checks) / len(checks) if checks else None,
        }
    return out


def pair_detail(result: Mapping[str, Any], candidate: Mapping[str, Any]) -> list[dict[str, Any]]:
    """One row per pair, in the order of the pairs: each family's L in each stage and how many
    of its zero-effect rules pass both stages (the design note's report by stage-1 start)."""

    rows = _index(result["evaluations"])
    read = result["reading"]

    def one(rule_id: str | None, window_id: str, stage: str) -> bool | None:
        if rule_id is None:
            return None
        row = rows.get((rule_id, window_id))
        if row is None:
            return False
        if stage == "1":
            return stage1(row, candidate["stage1"], candidate["confidence"])
        return stage2(row, candidate["stage2"])

    out = []
    for span, items in result["windows"]["pairs"].items():
        for long_id, check_id in items:
            item: dict[str, Any] = {"span": span, "long": long_id, "check": check_id}
            for name, level_id, zero in (
                ("volatility", read["level"], read["zero_rules"]),
                ("drawdown", read["holdout_level"], read["holdout_zero_rules"]),
            ):
                item[name] = {
                    "stage1": one(level_id, long_id, "1"),
                    "stage2": one(level_id, check_id, "2"),
                    "zero_both": sum(
                        bool(one(rule_id, long_id, "1")) and bool(one(rule_id, check_id, "2"))
                        for rule_id in zero
                    ),
                    "zero_count": len(zero),
                }
            out.append(item)
    return out


def allowed_counts(pairs: int) -> tuple[int, int]:
    """How many of ``pairs`` a zero-effect rule may pass (at most 5%) and L must pass (80%)."""

    return math.floor(FALSE_PASS_LIMIT * pairs + 1e-9), math.ceil(POWER_LINE * pairs - 1e-9)


def level(
    effects: Mapping[str, Mapping[str, Any]], prefix: str, noise_levels: Sequence[float]
) -> str | None:
    """L of one family: the level with the smallest whole-period left-tail improvement among those
    at least ``EFFECT_LINE`` (on a tie the noisier one), or None."""

    strong = []
    for noise in noise_levels:
        gain = effects.get(prefix + gp.planted_rule_id(noise), {}).get(TAIL_FIELD)
        if gain is not None and gain >= EFFECT_LINE:
            strong.append((gain, float(noise)))
    if not strong:
        return None
    return prefix + gp.planted_rule_id(min(strong, key=lambda item: (item[0], -item[1]))[1])


def zero_rules(
    effects: Mapping[str, Mapping[str, Any]], prefix: str, noise_levels: Sequence[float]
) -> list[str]:
    """The null and every level whose whole-period left-tail improvement is below ``ZERO_LINE``
    (a level without an effect value is left out here; ``choose`` reads it as incomplete)."""

    out = [prefix + gp.NULL_RULE]
    for noise in noise_levels:
        rule_id = prefix + gp.planted_rule_id(noise)
        gain = effects.get(rule_id, {}).get(TAIL_FIELD)
        if gain is not None and gain < ZERO_LINE:
            out.append(rule_id)
    return out


def _worst(shares: Mapping[str, float | None]) -> float | None:
    values = list(shares.values())
    if not values or any(value is None for value in values):
        return None
    return min(value for value in values if value is not None)


def _highest(shares: Mapping[str, float | None]) -> float | None:
    values = list(shares.values())
    if not values or any(value is None for value in values):
        return None
    return max(value for value in values if value is not None)


def choose(result: Mapping[str, Any]) -> dict[str, Any]:
    """The fixed reading (module docstring)."""

    effects = result["effects"]
    levels = result["noise_levels"]
    vol_level = level(effects, "", levels)
    dd_level = level(effects, DRAWDOWN, levels)
    vol_zero = zero_rules(effects, "", levels)
    dd_zero = zero_rules(effects, DRAWDOWN, levels)
    pair_ids = {
        window_id
        for items in result["windows"]["pairs"].values()
        for pair in items
        for window_id in pair
    }
    planted = {
        row["rule_id"]
        for row in result["evaluations"]
        if row["kind"] == "planted" and row["window_id"] in pair_ids and row["error"]
    }
    missing = [
        prefix + gp.planted_rule_id(noise)
        for prefix in ("", DRAWDOWN)
        for noise in levels
        if effects.get(prefix + gp.planted_rule_id(noise), {}).get(TAIL_FIELD) is None
    ]
    table = []
    for candidate in candidates([float(item) for item in result["confidences"]]):
        shares = pass_shares(result, candidate)
        false_pass = _highest(
            {
                f"{rule_id}:{span}": share
                for rule_id in vol_zero
                for span, share in shares.get(rule_id, {}).get("pairs", {}).items()
            }
        )
        score = None if vol_level is None else _worst(shares[vol_level]["pairs"])
        table.append(
            {
                **candidate,
                "score": score,
                "false_pass": false_pass,
                "eligible": false_pass is not None and false_pass <= FALSE_PASS_LIMIT,
                "shares": shares,
            }
        )
    out: dict[str, Any] = {
        "effect_line": EFFECT_LINE,
        "zero_line": ZERO_LINE,
        "power_line": POWER_LINE,
        "false_pass_limit": FALSE_PASS_LIMIT,
        "level": vol_level,
        "zero_rules": vol_zero,
        "holdout_level": dd_level,
        "holdout_zero_rules": dd_zero,
        "candidates": [
            {key: item[key] for key in ("candidate", "score", "false_pass", "eligible")}
            for item in table
        ],
        "chosen": None,
        "score": None,
        "holdout": None,
    }
    if planted or missing:
        return {
            **out,
            "errors": sorted(planted),
            "missing_effects": missing,
            "verdict": "incomplete",
        }
    if vol_level is None:
        return {**out, "verdict": "no_level"}
    eligible = [item for item in table if item["eligible"] and item["score"] is not None]
    if not eligible:
        return {**out, "verdict": "none_eligible"}
    # Highest score; ``table`` is in the tie order, and max keeps the first of equal scores.
    chosen = max(eligible, key=lambda item: item["score"])
    out = {**out, "chosen": chosen["candidate"], "score": chosen["score"]}
    if chosen["score"] < POWER_LINE:
        return {**out, "verdict": "below_80"}
    if dd_level is None:
        return {**out, "verdict": "holdout_no_level"}
    shares = chosen["shares"]
    holdout_score = _worst(shares[dd_level]["pairs"])
    holdout_false = _highest(
        {
            f"{rule_id}:{span}": share
            for rule_id in dd_zero
            for span, share in shares.get(rule_id, {}).get("pairs", {}).items()
        }
    )
    holds = (
        holdout_score is not None
        and holdout_score >= POWER_LINE
        and holdout_false is not None
        and holdout_false <= FALSE_PASS_LIMIT
    )
    out["holdout"] = {"score": holdout_score, "false_pass": holdout_false, "holds": holds}
    return {**out, "verdict": "adopt" if holds else "holdout_failed"}


# --- the run ------------------------------------------------------------------------------------

_WORKER: dict[str, Any] = {}


def _init(payload: dict[str, Any]) -> None:
    _WORKER.clear()
    _WORKER.update(payload)


def _task(task: tuple[str, Window, int | None]) -> dict[str, Any]:
    rule_id, window, span = task
    rule: gp.PowerRule = _WORKER["rules"][rule_id]
    try:
        row = evaluate(
            _WORKER["data"],
            rule.targets,
            window,
            _WORKER["loaded"],
            _WORKER["gate"],
            rule.rebalance,
            _WORKER["confidences"],
        )
    except Exception as exc:  # one window must not lose a run of hours; the row says what failed
        error = f"{type(exc).__name__}: {exc}".splitlines()[0][: lg.ERROR_TEXT_LIMIT]
        row = failed_row(window, error)
    return {
        "rule_id": rule_id,
        "kind": rule.kind,
        "family": family(rule_id, rule.kind),
        "noise": rule.noise,
        "span": span,
        **row,
    }


def run_ruler(
    data: BenchData,
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    root_dir: Path,
    *,
    workers: int = 1,
    first_year: int = FIRST_YEAR,
    step: int = YEAR_STEP,
    check_years: int = CHECK_YEARS,
    spans: Sequence[int] = SPANS,
    pair_spans: Sequence[int] = PAIR_SPANS,
    confidences: Sequence[float] = CONFIDENCES,
    noise_levels: Sequence[float] = gp.NOISE_LEVELS,
    progress: Callable[[int, int], None] | None = None,
    journal: Path | None = None,
) -> dict[str, Any]:
    """Every rule in every window, the planted effects over the whole period and the reading.
    With ``journal``, the run's record (as in the result, without evaluations and reading) and then
    each evaluation as it arrives are written there as JSON lines, so a run that stops part way
    keeps what it did."""

    for frame in (data.returns, data.inputs):
        if pd.Timestamp(pd.DatetimeIndex(frame.index).max()).date() > gp.CUTOFF:
            raise ValueError(f"us_market data run past {gp.CUTOFF}; cut them first")
    if gate.confidence != gp.CONFIDENCE:
        raise ValueError(f"the gate's confidence {gate.confidence} is not {gp.CONFIDENCE}")
    if gp.CONFIDENCE not in confidences:
        raise ValueError(f"the confidences must include the gate's {gp.CONFIDENCE}")
    metrics = [rule.metric for rule in gate.markets["qqq"].rules]
    missing = [metric for metric in CORE_METRICS if metric not in metrics]
    if missing:
        raise ValueError(f"the gate has no rule on {', '.join(missing)}")
    if len(set(metrics)) != len(metrics):
        raise ValueError("the gate has two rules on one metric")
    if not set(pair_spans) <= set(spans):
        raise ValueError("every pair length must be one of the spans")
    index = pd.DatetimeIndex(data.returns.index)
    checks = check_windows(index, first_year=first_year, step=step, years=check_years)
    long = {span: span_windows(index, span, first_year=first_year, step=step) for span in spans}
    if not checks or not all(long.values()):
        raise ValueError("a window length has no window that fits the data")
    pairs = pair_list(long, checks, pair_spans)
    if not all(pairs.values()):
        raise ValueError("a pair length has no pair that fits the data")
    full = Window(
        window_id=FULL_ID,
        role="development",
        start=checks[0].start,
        end=pd.Timestamp(index[-1]).date(),
    )
    rules = power_rules(data, root_dir, noise_levels)
    settings = gp.with_episodes(loaded, gp.US_EPISODES)
    effects = {
        rule.rule_id: gp.effect(data, rule.targets, full, loaded, rule.rebalance) for rule in rules
    }
    windows = ordered_windows(full, long, checks, check_years)
    tasks = [(rule.rule_id, window, span) for window, span in windows for rule in rules]
    trials = root_dir.joinpath(tr.TRIALS_DIR)

    def listing(items: Sequence[Window]) -> list[dict[str, Any]]:
        return [
            {"window_id": w.window_id, "start": w.start.isoformat(), "end": gp._iso(w.end)}
            for w in items
        ]

    # The record is taken before the evaluations, so it names the code and files that ran.
    result: dict[str, Any] = {
        "probe": "ruler_power",
        "run": RUN_ID,
        "provenance": git_provenance(root_dir),
        "settings_sha256": loaded.sha256,
        "gate_sha256": lg.chain_digest(gp.MEASURED_GATE.read_bytes()),
        "plan_sha256": lg.chain_digest(trials.joinpath(f"{gp.TRIAL_ID}.yaml").read_bytes()),
        "falsification_settings": loaded.settings.falsification.model_dump(mode="json"),
        "cutoff": gp.CUTOFF.isoformat(),
        "confidence": gp.CONFIDENCE,
        "check_years": check_years,
        "confidences": [float(item) for item in confidences],
        "noise_levels": [float(noise) for noise in noise_levels],
        "seeds": {"volatility": gp.PLANTED_SEED, "drawdown": HOLDOUT_SEED},
        "warmup_months": gp.WARMUP_MONTHS,
        "windows": {
            "full": listing([full])[0],
            "check": listing(checks),
            "long": {str(span): listing(items) for span, items in long.items()},
            "pairs": {str(span): [list(pair) for pair in items] for span, items in pairs.items()},
        },
        "episodes": [episode.model_dump() for episode in gp.US_EPISODES],
        "effects": effects,
    }
    payload = {
        "data": data,
        "rules": {rule.rule_id: rule for rule in rules},
        "loaded": settings,
        "gate": gate,
        "confidences": tuple(confidences),
    }
    evaluations: list[dict[str, Any]] = []
    sink = None if journal is None else journal.open("w", encoding="utf-8")

    def keep(done: int, row: dict[str, Any]) -> None:
        evaluations.append(row)
        if sink is not None:
            sink.write(_line(row))
            sink.flush()
        if progress is not None:
            progress(done, len(tasks))

    try:
        if sink is not None:
            sink.write(_line({**result, "tasks": len(tasks)}))
            sink.flush()
        if workers <= 1:
            _init(payload)
            for done, task in enumerate(tasks, start=1):
                keep(done, _task(task))
        else:
            context = multiprocessing.get_context("spawn")
            with ProcessPoolExecutor(
                max_workers=workers, mp_context=context, initializer=_init, initargs=(payload,)
            ) as pool:
                for done, row in enumerate(pool.map(_task, tasks), start=1):
                    keep(done, row)
    finally:
        if sink is not None:
            sink.close()
    result["evaluations"] = evaluations
    result["reading"] = choose(result)
    return result


def _line(value: Mapping[str, Any]) -> str:
    return json.dumps(gp._clean(dict(value)), ensure_ascii=False, allow_nan=False) + "\n"


# --- output -------------------------------------------------------------------------------------

VERDICT_TEXT = {
    "adopt": (
        "选中的两段尺子对 L 至少 80%、几乎没效果的规则不到 5%，留出的回撤那一族也达标："
        "写成试跑门的改动交 Baker 批（C 区）。"
    ),
    "holdout_failed": (
        "选中的尺子在波动率那一族达标，但留出的回撤那一族没达标："
        "门可能只认得一种假信号，原样报告，Baker 定。"
    ),
    "below_80": "没有一把候选尺子对 L 达到 80%：原样报告最好的那把和差距，Baker 定。",
    "none_eligible": "所有候选尺子都让几乎没效果的规则过了 5% 以上：原样报告，Baker 定。",
    "holdout_no_level": "留出的回撤那一族没有一档整段左尾改善达到 20%：验收条件不成立，原样报告。",
    "no_level": "波动率那一族没有一档整段左尾改善达到 20%：判读条件不成立，原样报告。",
    "incomplete": "假信号在配对窗口里有评估出错，或某一档没有整段效果：判读不完整，先查错误再读。",
}


def _pct(value: Any) -> str:
    return gp._pct(value)


def render_markdown(result: Mapping[str, Any]) -> str:
    read = result["reading"]
    adopted = "" if read["verdict"] == "adopt" else "（没有采用）"
    windows = result["windows"]
    pairs = windows["pairs"]
    years = result["check_years"]
    lines = [
        f"# 两段尺子结果（{RUN_ID}）",
        "",
        f"- 数据截至 {result['cutoff']}；{years} 年查方向窗口 {len(windows['check'])} 个；长窗口 "
        + "、".join(f"{span} 年 {len(items)} 个" for span, items in windows["long"].items())
        + f"；整段 {windows['full']['start']}..{windows['full']['end']}；配对 "
        + "、".join(f"{span}+{years} 年 {len(items)} 对" for span, items in pairs.items())
        + "。",
        "- 线换成对数："
        + "；".join(
            f"{span}+{years} 年 {len(items)} 对里几乎没效果的最多过 "
            f"{allowed_counts(len(items))[0]} 对、L 至少过 {allowed_counts(len(items))[1]} 对"
            for span, items in pairs.items()
        )
        + "。",
        f"- 判读：{VERDICT_TEXT[read['verdict']]}",
        f"- L（波动率族）{read['level']}；几乎没效果的：{'、'.join(read['zero_rules'])}。"
        f"留出族 L {read['holdout_level']}；"
        f"几乎没效果的：{'、'.join(read['holdout_zero_rules'])}。",
    ]
    if read["chosen"] is not None:
        lines.append(
            f"- 选中：{read['chosen']}{adopted}，"
            f"L 两段都过 {_pct(read['score'])}（两种配对取低的）。"
        )
    if read.get("holdout"):
        hold = read["holdout"]
        lines.append(
            f"- 留出族验收：L {_pct(hold['score'])}，几乎没效果的最高 {_pct(hold['false_pass'])}，"
            f"{'达标' if hold['holds'] else '没达标'}。"
        )
    if read.get("errors"):
        lines.append(f"- 出错的假信号：{'、'.join(read['errors'])}")
    if read.get("missing_effects"):
        lines.append(f"- 没有整段效果的档：{'、'.join(read['missing_effects'])}")
    failed = [row for row in result["evaluations"] if row["error"]]
    if failed:
        lines.append(f"- 评估出错 {len(failed)} 个（不算过门），第一个：{failed[0]['error']}")
    lines += [
        "- 长窗口和配对大量重叠，比例的误差大；"
        "挑尺子只用波动率族，回撤族只验收，三条真规则只描述。",
        "",
        "## 候选尺子（按并列时的先后排）",
        "",
        "| 候选 | L 两段都过 | 几乎没效果的最高 | 可选 |",
        "|---|---|---|---|",
    ]
    for item in read["candidates"]:
        lines.append(
            f"| {item['candidate']} | {_pct(item['score'])} | {_pct(item['false_pass'])} | "
            f"{'是' if item['eligible'] else '否'} |"
        )
    scored = [item for item in read["candidates"] if item["score"] is not None]
    best = max(scored, key=lambda item: item["score"]) if scored else None
    chosen = read["chosen"] or (best or (read["candidates"] or [{}])[0]).get("candidate")
    if chosen is not None:
        candidate = next(
            item for item in candidates(result["confidences"]) if item["candidate"] == chosen
        )
        shares = pass_shares(result, candidate)
        title = f"选中的尺子{adopted}" if read["chosen"] else "L 两段都过最高的候选（没有选中）"
        broken = {
            row["rule_id"]
            for row in result["evaluations"]
            if row["window_id"] == FULL_ID and row["error"]
        }
        spans = list(windows["long"])
        lines += [
            "",
            f"## {title} {chosen}：每条规则",
            "",
            "| 规则 | 整段左尾改善 | "
            + " | ".join(f"{span}+{years} 两段都过" for span in pairs)
            + " | "
            + " | ".join(f"{span} 年第一段" for span in spans)
            + f" | 整段第一段 | {years} 年查方向 |",
            "|---|---|" + "---|" * (len(pairs) + len(spans) + 2),
        ]
        for rule_id, row in shares.items():
            gain = result["effects"].get(rule_id, {}).get(TAIL_FIELD)
            lines.append(
                f"| {rule_id} | {_pct(gain)} | "
                + " | ".join(_pct(row["pairs"].get(span)) for span in pairs)
                + " | "
                + " | ".join(_pct(row["stage1"].get(span)) for span in spans)
                + f" | {'出错' if rule_id in broken else '过' if row['full'] else '不过'} | "
                + f"{_pct(row['stage2'])} |"
            )

        def mark(value: bool | None) -> str:
            return "—" if value is None else "过" if value else "不过"

        lines += [
            "",
            f"## {title} {chosen}：每对（按第一段起点）",
            "",
            "| 配对 | 长窗口 | 查方向 | L 第一段 | L 第二段 | 几乎没效果的两段都过 "
            "| 留出 L 第一段 | 留出 L 第二段 | 留出几乎没效果的两段都过 |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for item in pair_detail(result, candidate):
            vol, dd = item["volatility"], item["drawdown"]
            lines.append(
                f"| {item['span']}+{years} | {item['long']} | {item['check']} | "
                f"{mark(vol['stage1'])} | {mark(vol['stage2'])} | "
                f"{vol['zero_both']}/{vol['zero_count']} | {mark(dd['stage1'])} | "
                f"{mark(dd['stage2'])} | {dd['zero_both']}/{dd['zero_count']} |"
            )
    lines.append("")
    return "\n".join(lines)


def write_result(result: Mapping[str, Any], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "ruler_result.json").write_text(
        json.dumps(gp._clean(dict(result)), ensure_ascii=False, indent=1, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    (out_dir / "ruler_result.md").write_text(render_markdown(result), encoding="utf-8")


def load_us_market(snapshot_root: Path) -> tuple[BenchData, tr.TrialGate, LoadedSettings, Path]:
    """The trial plan T0001's US market data, cut at 2010-12-31 (``gate_power`` loads both)."""

    datasets, gate, loaded, root_dir = gp.load_datasets(snapshot_root)
    return datasets["us_market"], gate, loaded, root_dir


@app.callback()
def main() -> None:
    """两段尺子。"""


@app.command("run")
def run_command(
    out: Annotated[
        Path,
        typer.Option(
            help="结果目录，写 ruler_result.json、ruler_result.md 和边跑边写的 ruler_rows.jsonl。"
        ),
    ],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
    workers: Annotated[int, typer.Option(help="并行进程数。")] = 1,
) -> None:
    """在美股的十年、20/30/40 年和整段窗口上评估两族假信号和三条已检验的规则，
    按写定的读法挑尺子。"""

    try:
        data, gate, loaded, root_dir = load_us_market(root)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        typer.echo(f"ruler_power: 数据载入失败：{exc}")
        raise typer.Exit(code=1) from None

    def progress(done: int, total: int) -> None:
        if done == total or done % 50 == 0:
            typer.echo(f"ruler_power: {done}/{total}")

    typer.echo(
        "ruler_power: 数据已载入并截到 2010-12-31；先算整段效果（几分钟，没有进度），再逐个窗口评估"
    )
    out.mkdir(parents=True, exist_ok=True)
    result = run_ruler(
        data,
        loaded,
        gate,
        root_dir,
        workers=workers,
        progress=progress,
        journal=out / JOURNAL_FILE,
    )
    result["source"] = dict(data.source)
    write_result(result, out)
    read = result["reading"]
    typer.echo(
        f"ruler_power: 判读 {read['verdict']}；选中 {read['chosen']}；L 两段都过 "
        f"{_pct(read['score'])}；写入 {out / 'ruler_result.json'}"
    )


if __name__ == "__main__":
    app()
