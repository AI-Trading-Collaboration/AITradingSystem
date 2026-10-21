"""Measure P1's portfolio ruler using synthetic reserves, never a candidate strategy.

A holds the plan's fixed market weight and a synthetic reserve; B is the bench's
matched-average-exposure portfolio with cash. Decompose geometric excess into the
reserve's own excess and rebalancing, and measure cushions in monthly 10% declines,
subtracting quiet-period reserve excess (whole-window fallback below half the days).

The reading is fixed before any run: in each US development window, pool the three
zero families, require a Wilson 95% upper bound <= 2.5% and hedge_0.3 power >= 80%,
then take the first pair in descending q, ascending t order. Either US window may
find a threshold; missing seeds or errors in a read family make it incomplete.
QQQ development and the other hedge/control families are printed, never decide.

Synthetic positive reserves deliberately look ahead. This does not evaluate treasury_10y,
change a gate, read validation/consistency windows, count trials, write research ledgers,
or authorize production or broker actions. US market history is a proxy, not QQQ evidence.
"""

from __future__ import annotations

import json
import math
import multiprocessing
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
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
from ai_trading_system.research_bench.falsify import FalsificationInputs, _rerun, cost_section
from ai_trading_system.research_bench.metrics import DAYS_PER_YEAR, compare, summarize
from ai_trading_system.research_bench.segments import (
    Segment,
    monthly_level,
    segment_return,
    window_returns,
)
from ai_trading_system.research_bench.settings import (
    LoadedSettings,
    Window,
    load_settings,
    resolve_window,
)
from ai_trading_system.research_bench.simulate import (
    Rebalance,
    Simulation,
    constant_targets,
    simulate,
)
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import gate_false_pass as fp
from ai_trading_system.research_probes import gate_power as gp

RUN_ID = "TRADING-2565-38"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-21:p1_portfolio_ruler"
PLAN_ID = "T0002"
# Thresholds come from the review thread's requirements sections 1–3, fixed before any run.
DECLINE_LINE = 0.10
THRESHOLDS = (0.05, 0.10, 0.15)
SHARES = (0.6, 0.75)
ThresholdPair = tuple[float, float]
PAIRS = tuple((t, q) for q in reversed(SHARES) for t in THRESHOLDS)
GEOMETRIC_FLOOR = -0.010
MIN_COUNT = 4
SMALL_LOSS = -0.02
QUIET_SHARE = 0.5
POWER_FAMILY = "hedge_0.3"
POWER_LINE = 0.80
USABLE_LINE = fp.USABLE_LINE
COST_BPS = fp.COST_BPS
COST = cost_section(COST_BPS)
NOISE = 0.07
CONTROL_NOISE = 0.15
CARRY = 0.02
CORRELATION = 0.3
JOURNAL_FILE = "portfolio_ruler_rows.jsonl"
RESULT_JSON = "portfolio_ruler_result.json"
RESULT_MD = "portfolio_ruler_result.md"
STAGE1_RULES = ("P-1", "P-2", "P-3")
STAGE2_RULES = ("S2-1", "S2-2", "S2-3")

app = typer.Typer(help="P1 组合尺子探针：只用合成储备量开发窗口，不改门。")


@dataclass(frozen=True)
class Family:
    family_id: str
    role: str
    kind: str
    size: float
    noise: float
    stream: int


# Seeds are shared via fp.seed_list; each zero family draws its own stream, making pooled
# zero units independent. The three hedges share a stream (as in gate_power), so power
# differences come only from the planted size.
FAMILIES = {
    f.family_id: f
    for f in (
        Family("null_noise", "zero", "noise", 0.0, NOISE, 1),
        Family("null_carry", "zero", "carry", CARRY, NOISE, 2),
        Family("null_pos_corr", "zero", "correlated", CORRELATION, NOISE, 3),
        Family("hedge_0.2", "hedge", "hedge", 0.2, NOISE, 4),
        Family("hedge_0.3", "hedge", "hedge", 0.3, NOISE, 4),
        Family("hedge_0.5", "hedge", "hedge", 0.5, NOISE, 4),
        Family("hedge_0.3_noise_0.15", "control", "hedge", 0.3, CONTROL_NOISE, 5),
    )
}
ZERO_FAMILIES = tuple(f.family_id for f in FAMILIES.values() if f.role == "zero")
READ_FAMILIES = (*ZERO_FAMILIES, POWER_FAMILY)


def decline_segments(level: pd.Series, line: float = DECLINE_LINE) -> list[Segment]:
    """The bench's drop algorithm, with a parameterized crossing and no duration floor."""
    out: list[Segment] = []
    values = level.to_numpy(dtype=float)
    months = pd.PeriodIndex(level.index, freq="M")
    begin = 0
    while begin < len(level):
        peak = begin
        crossed = False
        for pos in range(begin + 1, len(level)):
            if values[pos] >= values[peak]:
                peak = pos
            elif values[pos] <= values[peak] * (1.0 - line):
                crossed = True
                break
        if not crossed:
            break
        recovery = next(
            (i for i in range(peak + 1, len(level)) if values[i] >= values[peak]), len(level)
        )
        trough = peak + 1 + int(np.argmin(values[peak + 1 : recovery]))
        out.append(
            Segment(
                id=f"decline_{months[peak].strftime('%Y%m')}",
                kind="decline",
                start=str(months[peak] + 1),
                end=str(months[trough]),
                peak=str(months[peak]),
                trough=str(months[trough]),
                months=months[trough].ordinal - months[peak].ordinal,
                change=float(values[trough] / values[peak] - 1.0),
            )
        )
        begin = trough + 1
    return out


def window_declines(market: pd.Series, window: Window) -> list[Segment]:
    return decline_segments(monthly_level(window_returns(market, window)))


def hedge_component(
    index: pd.DatetimeIndex, declines: Sequence[Segment], share: float
) -> np.ndarray[Any, Any]:
    out = np.zeros(len(index))
    months = index.to_period("M")
    for s in declines:
        if s.change is None:
            raise ValueError(f"{s.id}: no depth")
        monthly = (1.0 + share * abs(s.change)) ** (1.0 / s.months) - 1.0
        for month in pd.period_range(s.start, s.end, freq="M"):
            inside = np.asarray(months == month)
            count = int(inside.sum())
            if count:
                out[inside] = (1.0 + monthly) ** (1.0 / count) - 1.0
    return out


def reserve_returns(
    returns: pd.DataFrame,
    family: Family,
    seed: int,
    declines: Sequence[Segment],
    days_per_year: int,
) -> pd.Series:
    cash = returns["cash"].to_numpy(dtype=float)
    component = (
        np.random.default_rng([seed, family.stream]).standard_normal(len(returns))
        * family.noise
        / math.sqrt(days_per_year)
    )
    if family.kind == "carry":
        component += family.size / days_per_year
    elif family.kind == "correlated":
        component += family.size * (returns["qqq"].to_numpy(dtype=float) - cash)
    elif family.kind == "hedge":
        component += hedge_component(pd.DatetimeIndex(returns.index), declines, family.size)
    elif family.kind != "noise":
        raise ValueError(f"unknown family kind {family.kind}")
    return pd.Series(cash + component, index=returns.index, name="reserve")


@dataclass(frozen=True)
class Simulated:
    strategy: Simulation
    benchmark: Simulation
    traded: pd.DataFrame


def simulate_pair(
    data: BenchData,
    targets: pd.Series,
    reserve: pd.Series,
    window: Window,
    rebalance: Rebalance,
    loaded: LoadedSettings,
) -> Simulated:
    traded = data.returns.copy()
    traded["cash"] = reserve.reindex(traded.index)
    own = simulate(
        traded,
        targets,
        start=window.start,
        end=window.end,
        rebalance=rebalance,
        cost_bps=loaded.cost_bps,
    )
    benchmark = simulate(
        data.returns,
        constant_targets(data.returns.index, float(own.exposure.mean())),
        start=window.start,
        end=window.end,
        rebalance=rebalance,
        cost_bps=loaded.cost_bps,
    )
    return Simulated(own, benchmark, traded)


def geometric(series: pd.Series, sim: Simulation) -> float:
    held = series.reindex(sim.returns.index)
    if held.isna().any():
        when = pd.Timestamp(held.index[held.isna()][0]).date()
        raise ValueError(f"{series.name} has no return on {when}")
    years = (sim.returns.index[-1] - sim.start).days / DAYS_PER_YEAR
    return float(float(np.prod(1.0 + held.to_numpy(dtype=float))) ** (1.0 / years) - 1.0)


def quiet_dates(dates: pd.DatetimeIndex, declines: Sequence[Segment]) -> np.ndarray[Any, Any]:
    quiet = np.ones(len(dates), dtype=bool)
    months = dates.to_period("M")
    for s in declines:
        quiet &= ~np.asarray(
            (months >= pd.Period(s.start, freq="M")) & (months <= pd.Period(s.end, freq="M"))
        )
    return quiet


def quiet_excess(
    reserve: pd.Series, cash: pd.Series, dates: pd.DatetimeIndex, days_per_year: int
) -> float:
    if dates.empty:
        raise ValueError("quiet excess needs return dates")
    logs = np.log1p(reserve.reindex(dates).to_numpy(dtype=float)) - np.log1p(
        cash.reindex(dates).to_numpy(dtype=float)
    )
    return math.expm1(float(logs.sum()) * days_per_year / len(dates))


def cushion_rows(
    sims: Simulated,
    returns: pd.DataFrame,
    reserve: pd.Series,
    exposure: float,
    excess: float,
    declines: Sequence[Segment],
) -> list[dict[str, Any]]:
    dates = sims.strategy.returns.index
    held, cash = reserve.reindex(dates), returns["cash"].reindex(dates)
    rows = []
    for s in declines:
        own = segment_return(sims.strategy.returns, s)
        reference = segment_return(sims.benchmark.returns, s)
        difference = own - reference
        cushion = difference - (1.0 - exposure) * excess * s.months / 12.0
        counted = reference < SMALL_LOSS
        rows.append(
            {
                "id": s.id,
                "months": s.months,
                "strategy_return": own,
                "benchmark_return": reference,
                "difference": difference,
                "cushion": cushion,
                "ratio": cushion / abs(reference) if counted else None,
                "reserve_excess": (1.0 + segment_return(held, s)) / (1.0 + segment_return(cash, s))
                - 1.0,
                "counted": counted,
            }
        )
    return rows


def cushion_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    ratios = [r["ratio"] for r in rows if r["counted"]]
    positive = sum(r > 0 for r in ratios)
    return {
        "count": len(ratios),
        "median_ratio": float(np.median(ratios)) if ratios else None,
        "positive": positive,
        "positive_share": positive / len(ratios) if ratios else None,
        "min_ratio": min(ratios) if ratios else None,
        "excluded": [r["id"] for r in rows if not r["counted"]],
    }


def measure(
    data: BenchData,
    targets: pd.Series,
    reserve: pd.Series,
    window: Window,
    rebalance: Rebalance,
    exposure: float,
    declines: Sequence[Segment],
    loaded: LoadedSettings,
) -> dict[str, Any]:
    sims = simulate_pair(data, targets, reserve, window, rebalance, loaded)
    cash = data.returns["cash"]
    own = summarize(sims.strategy, cash, loaded)
    benchmark = summarize(sims.benchmark, cash, loaded)
    comparison = compare(own, benchmark, loaded)
    cost = _rerun(
        FalsificationInputs(
            returns=data.returns,
            targets=targets,
            start=window.start,
            end=window.end,
            rebalance=rebalance,
            delay_days=0,
            cost_bps=loaded.cost_bps,
            strategy=sims.strategy,
            benchmark=sims.benchmark,
            strategy_returns=sims.traded,
        ),
        loaded,
        cost_bps=COST_BPS,
    )
    legs = {
        "strategy": own["geometric_annual_return"],
        "benchmark": benchmark["geometric_annual_return"],
        "market": geometric(data.returns["qqq"], sims.strategy),
        "reserve": geometric(reserve, sims.strategy),
        "cash": geometric(cash, sims.strategy),
    }
    dates = pd.DatetimeIndex(sims.strategy.returns.index)
    quiet = quiet_dates(dates, declines)
    whole = (1.0 + legs["reserve"]) / (1.0 + legs["cash"]) - 1.0
    fallback = int(quiet.sum()) < QUIET_SHARE * len(dates)
    excess = (
        whole
        if fallback
        else quiet_excess(reserve, cash, dates[quiet], loaded.settings.trading_days_per_year)
    )
    w = exposure
    rebalancing = (legs["strategy"] - (w * legs["market"] + (1.0 - w) * legs["reserve"])) - (
        legs["benchmark"] - (w * legs["market"] + (1.0 - w) * legs["cash"])
    )
    rows = cushion_rows(sims, data.returns, reserve, exposure, excess, declines)
    return {
        "average_exposure": float(sims.strategy.exposure.mean()),
        "geometric": comparison["geometric_annual_return"],
        "geometric_cost": cost["geometric_annual_return"],
        "reserve_excess": (1.0 - w) * (legs["reserve"] - legs["cash"]),
        "rebalancing": rebalancing,
        "excess_annual": whole,
        "quiet_excess": excess,
        "quiet_days": int(quiet.sum()),
        "quiet_fallback": fallback,
        "legs": legs,
        "segments": rows,
        "summary": cushion_summary(rows),
    }


def _finite(value: Any) -> bool:
    return isinstance(value, int | float) and math.isfinite(value)


def _at_least(value: Any, line: float) -> bool:
    return _finite(value) and bool(value >= line)


def needed(share: float, count: int) -> int:
    return math.ceil(round(share * count, 9))


def cushion_holds(summary: Mapping[str, Any], t: float, q: float) -> bool:
    count = summary.get("count")
    if not isinstance(count, int) or count < MIN_COUNT:
        return False
    return _at_least(summary.get("median_ratio"), t) and _at_least(
        summary.get("positive"), needed(q, count)
    )


def stage1_rules(numbers: Mapping[str, Any], t: float, q: float) -> dict[str, bool]:
    return {
        "P-1": _at_least(numbers.get("geometric"), GEOMETRIC_FLOOR)
        and _at_least(numbers.get("geometric_cost"), GEOMETRIC_FLOOR),
        "P-2": cushion_holds(numbers["summary"], t, q),
        "P-3": _at_least(numbers.get("rebalancing"), 0.0),
    }


def stage2_rules(numbers: Mapping[str, Any]) -> dict[str, bool]:
    median = numbers["summary"].get("median_ratio")
    return {
        "S2-1": _at_least(numbers.get("geometric"), GEOMETRIC_FLOOR),
        "S2-2": _finite(median) and bool(median > 0),
        "S2-3": _at_least(numbers.get("rebalancing"), 0.0),
    }


def pair_key(pair: ThresholdPair) -> str:
    return f"t={pair[0]:.2f} q={pair[1]:.2f}"


def family_rates(rows: Sequence[Mapping[str, Any]], pair: ThresholdPair | None) -> dict[str, Any]:
    names = STAGE2_RULES if pair is None else STAGE1_RULES
    rules = dict.fromkeys(names, 0)
    passes = errors = last = 0
    for row in rows:
        if row["error"] is not None:
            errors += 1
            continue
        holds = (
            stage2_rules(row["numbers"]) if pair is None else stage1_rules(row["numbers"], *pair)
        )
        passes += all(holds.values())
        last += all(holds[name] for name in names[:-1]) and not holds[names[-1]]
        for name in names:
            rules[name] += holds[name]
    return {
        "passes": passes,
        "total": len(rows),
        "share": passes / len(rows) if rows else None,
        "errors": errors,
        "rules": rules,
        "last_rule_alone": last,
    }


def _counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    good = [r["numbers"] for r in rows if r["error"] is None]
    counts = [n["summary"]["count"] for n in good]
    return {
        "counted_segments": {"min": min(counts), "max": max(counts)} if counts else None,
        "quiet_fallbacks": sum(bool(n["quiet_fallback"]) for n in good),
    }


def window_reading(
    evaluations: Sequence[Mapping[str, Any]], key: str, seeds: int
) -> dict[str, Any]:
    rows = [r for r in evaluations if r["window_key"] == key]
    by_family = {f: [r for r in rows if r["family"] == f] for f in FAMILIES}
    incomplete = any(
        any(r["error"] is not None for r in by_family[f])
        or sorted(r["seed"] for r in by_family[f]) != fp.seed_list(seeds)
        for f in READ_FAMILIES
    )
    chosen: ThresholdPair | None = None
    pairs = {}
    for pair in PAIRS:
        zero = family_rates([r for r in rows if r["family"] in ZERO_FAMILIES], pair)
        low, high = fp.wilson(zero["passes"], zero["total"]) if zero["total"] else (None, None)
        zero.update(low=low, high=high)
        families = {f: family_rates(by_family[f], pair) for f in FAMILIES}
        power = families[POWER_FAMILY]["share"]
        usable = high is not None and high <= USABLE_LINE
        enough = _at_least(power, POWER_LINE)
        pairs[pair_key(pair)] = {
            "threshold": pair[0],
            "share": pair[1],
            "zero": zero,
            "usable": usable,
            "power": power,
            "enough": enough,
            "families": families,
        }
        if chosen is None and usable and enough:
            chosen = pair
    verdict = "incomplete" if incomplete else "threshold" if chosen else "no_threshold"
    return {
        "verdict": verdict,
        "threshold": chosen[0] if verdict == "threshold" and chosen else None,
        "share": chosen[1] if verdict == "threshold" and chosen else None,
        **_counts(rows),
        "pairs": pairs,
    }


def direction_reading(evaluations: Sequence[Mapping[str, Any]], key: str) -> dict[str, Any]:
    rows = [r for r in evaluations if r["window_key"] == key]
    return {
        **_counts(rows),
        "families": {
            f: family_rates([r for r in rows if r["family"] == f], None) for f in FAMILIES
        },
    }


def outcome(readings: Mapping[str, Any], windows: Mapping[str, ProbeWindow]) -> str:
    verdicts = [readings[key]["verdict"] for key, w in windows.items() if w.decides]
    if "incomplete" in verdicts:
        return "incomplete"
    return "threshold_found" if "threshold" in verdicts else "no_threshold"


def evaluate(
    data: BenchData,
    reserve: pd.Series,
    window: Window,
    declines: Sequence[Segment],
    config: tr.TrialConfig,
    loaded: LoadedSettings,
) -> dict[str, Any]:
    targets = strategy_targets(tr.STRATEGY_FUNCTIONS[config.function], data.inputs, config.params)
    numbers = measure(
        data,
        targets,
        reserve,
        window,
        config.rebalance.to_rebalance(),
        float(config.params["exposure"]),
        declines,
        loaded,
    )
    numbers["ratios"] = [s["ratio"] for s in numbers.pop("segments")]
    return numbers


_WORKER: dict[str, Any] = {}


def _init(payload: dict[str, Any]) -> None:
    _WORKER.clear()
    _WORKER.update(payload)


def _error(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}".splitlines()[0][: lg.ERROR_TEXT_LIMIT]


def _task(task: tuple[str, int, str]) -> dict[str, Any]:
    family_id, seed, key = task
    family = FAMILIES[family_id]
    try:
        item = _WORKER["windows"][key]
        data = _WORKER["data"][item.market]
        declines = _WORKER["declines"][key]
        loaded = _WORKER["loaded"]
        reserve = reserve_returns(
            data.returns, family, seed, declines, loaded.settings.trading_days_per_year
        )
        numbers = evaluate(data, reserve, item.window, declines, _WORKER["config"], loaded)
        error = None
    except Exception as exc:
        numbers, error = None, _error(exc)
    return dict(
        gp._clean(
            {
                "family": family_id,
                "role": family.role,
                "seed": seed,
                "window_key": key,
                "numbers": numbers,
                "error": error,
            }
        )
    )


def tasks(seeds: int, windows: Mapping[str, ProbeWindow]) -> list[tuple[str, int, str]]:
    return [(f, seed, key) for f in FAMILIES for seed in fp.seed_list(seeds) for key in windows]


@dataclass(frozen=True)
class ProbeWindow:
    key: str
    market: tr.Market
    window: Window
    decides: bool


def probe_windows(
    loaded: LoadedSettings, gate: tr.TrialGate, question_types: Sequence[str] | None = None
) -> dict[str, ProbeWindow]:
    allowed = gate.markets["us_market"].question_types()
    types = allowed if question_types is None else question_types
    windows = {}
    for question_type in types:
        if question_type not in allowed:
            raise ValueError(f"{question_type} is not a us_market question type")
        window = resolve_window(loaded, question_type, "development")
        windows[window.window_id] = ProbeWindow(window.window_id, "us_market", window, True)
    qqq = resolve_window(loaded, gate.markets["qqq"].question_type, "development")
    windows[qqq.window_id] = ProbeWindow(qqq.window_id, "qqq", qqq, False)
    for item in windows.values():
        if item.window.end is None or item.window.end > gp.CUTOFF:
            raise ValueError(f"{item.key}: window runs past {gp.CUTOFF}")
    return windows


def plan_config(plan: tr.TrialPlan) -> tr.TrialConfig:
    config = plan.config(plan.primary)
    if config.function != "fixed_exposure" or config.reserve == "cash":
        raise ValueError("the ruler requires fixed_exposure whose reserve is not cash")
    return config


def window_record(item: ProbeWindow) -> dict[str, Any]:
    return {
        "window_id": item.window.window_id,
        "market": item.market,
        "start": item.window.start.isoformat(),
        "end": gp._iso(item.window.end),
        "decides": item.decides,
    }


def segment_section(
    declines: Mapping[str, Sequence[Segment]], windows: Mapping[str, ProbeWindow]
) -> dict[str, Any]:
    return {
        key: {
            "window": window_record(item),
            "count": len(declines[key]),
            "segments": [asdict(s) for s in declines[key]],
        }
        for key, item in windows.items()
    }


def run_probe(
    datasets: Mapping[str, BenchData],
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    plan: tr.TrialPlan,
    root_dir: Path,
    *,
    seeds: int = fp.DEFAULT_SEEDS,
    workers: int = 1,
    windows: Mapping[str, ProbeWindow] | None = None,
    journal: Path | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    if seeds < 1:
        raise ValueError("at least one seed is required")
    for market, data in datasets.items():
        for frame in (data.returns, data.inputs):
            if pd.Timestamp(pd.DatetimeIndex(frame.index).max()).date() > gp.CUTOFF:
                raise ValueError(f"{market}: data run past {gp.CUTOFF}; cut them first")
    windows = probe_windows(loaded, gate) if windows is None else windows
    config = plan_config(plan)
    trials = root_dir / tr.TRIALS_DIR
    head = gp._clean(
        {
            "run_id": RUN_ID,
            "owner_decision": OWNER_DECISION,
            "plan_id": plan.trial_id,
            "plan_sha256": lg.chain_digest((trials / f"{plan.trial_id}.yaml").read_bytes()),
            "gate_sha256": lg.chain_digest((trials / tr.GATE_FILE).read_bytes()),
            "settings_sha256": loaded.sha256,
            "cost_bps": loaded.cost_bps,
            "config": {
                "id": config.id,
                "exposure": float(config.params["exposure"]),
                "reserve": config.reserve,
                "rebalance": config.rebalance.model_dump(mode="json"),
            },
            "thresholds": {
                "decline_line": DECLINE_LINE,
                "thresholds": THRESHOLDS,
                "shares": SHARES,
                "pairs": PAIRS,
                "geometric_floor": GEOMETRIC_FLOOR,
                "cost_rerun_bps": COST_BPS,
                "min_count": MIN_COUNT,
                "small_loss": SMALL_LOSS,
                "quiet_share": QUIET_SHARE,
                "power_family": POWER_FAMILY,
                "power_line": POWER_LINE,
                "usable_line": USABLE_LINE,
            },
            "families": [asdict(f) for f in FAMILIES.values()],
            "windows": {key: window_record(w) for key, w in windows.items()},
            "cutoff": gp.CUTOFF.isoformat(),
            "seed_start": fp.SEED_START,
            "seeds": seeds,
            "trading_days_per_year": loaded.settings.trading_days_per_year,
            "source": {market: dict(data.source) for market, data in datasets.items()},
            "provenance": git_provenance(root_dir),
        }
    )
    declines = {
        key: window_declines(datasets[w.market].returns["qqq"], w.window)
        for key, w in windows.items()
    }
    found = segment_section(declines, windows)
    work = tasks(seeds, windows)
    payload = {
        "data": dict(datasets),
        "windows": dict(windows),
        "declines": declines,
        "config": config,
        "loaded": loaded,
    }
    evaluations = []
    sink = None if journal is None else journal.open("x", encoding="utf-8")

    def keep(value: dict[str, Any], message: str = "") -> None:
        if sink is not None:
            sink.write(json.dumps(gp._clean(value), ensure_ascii=False, allow_nan=False) + "\n")
            sink.flush()
        if message and progress is not None:
            progress(message)

    try:
        keep(head)
        keep({"segments": found})
        if workers <= 1:
            _init(payload)
            for done, task in enumerate(work, start=1):
                row = _task(task)
                evaluations.append(row)
                keep(row, f"评估 {done}/{len(work)}")
        else:
            with ProcessPoolExecutor(
                max_workers=workers,
                mp_context=multiprocessing.get_context("spawn"),
                initializer=_init,
                initargs=(payload,),
            ) as pool:
                for done, row in enumerate(pool.map(_task, work), start=1):
                    evaluations.append(row)
                    keep(row, f"评估 {done}/{len(work)}")
    finally:
        if sink is not None:
            sink.close()
    readings = {
        key: window_reading(evaluations, key, seeds)
        if w.decides
        else direction_reading(evaluations, key)
        for key, w in windows.items()
    }
    return dict(
        gp._clean(
            {
                **head,
                "segments": found,
                "evaluations": evaluations,
                "readings": readings,
                "outcome": outcome(readings, windows),
                "complete": all(r["error"] is None for r in evaluations),
            }
        )
    )


OUTCOME_TEXT = {
    "threshold_found": (
        "至少一个窗口判出门槛：可以打第 4 节的 C 区批，门里写判出的数，"
        "P1 第一段只用判出的窗口；这不是改门或生产授权。"
    ),
    "no_threshold": "两个窗口都没有门槛：不改门，报 Baker，尺子回复盘线程重设计。",
    "incomplete": "读数不完整：先补跑、不读。",
}


def _verdict(reading: Mapping[str, Any]) -> str:
    if reading["verdict"] == "threshold":
        return f"判出 {pair_key((reading['threshold'], reading['share']))}"
    return "没有门槛" if reading["verdict"] == "no_threshold" else "不完整"


def _yes(value: bool) -> str:
    return "是" if value else "否"


def _rate(row: Mapping[str, Any]) -> str:
    return f"{row['passes']}/{row['total']}"


def _points(value: Any) -> str:
    return "-" if value is None else f"{float(value) * 100:+.2f}"


def render_markdown(result: Mapping[str, Any]) -> str:
    lines = [
        f"# P1 组合尺子探针（{RUN_ID}）",
        "",
        f"- 依据：{result['owner_decision']}；计划 {result['plan_id']} "
        f"sha256：{result['plan_sha256']}；门 sha256：{result['gate_sha256']}。",
        f"- 设置 sha256：{result['settings_sha256']}；配置：{result['config']}；"
        f"成本 {result['cost_bps']} bps，成本重跑 {COST_BPS:g} bps。",
        f"- 数据截至 {result['cutoff']}；种子从 {result['seed_start']} 起，每族 "
        f"{result['seeds']} 个；年交易日数 {result['trading_days_per_year']}。",
        f"- 门槛（运行前写定）：{result['thresholds']}。",
        "- 数据按试跑的质量核查路径载入；来源及质量状态见 JSON 的 source。"
        "储备全是合成的，美股大盘是代理；阳性族故意前视；不评估 treasury_10y、"
        "不改门、不计试验、不写研究账本，不授权生产或 broker。",
        "",
        "## 下跌段清单",
        "",
    ]
    for key, section in result["segments"].items():
        w = section["window"]
        lines += [
            f"### {key}（{w['start']}..{w['end']}，"
            f"{'判' if w['decides'] else '只印'}）：{section['count']} 段",
            "",
            "| 段 | 段内月份 | 月数 | 市场变化 |",
            "|---|---|---|---|",
        ]
        for s in section["segments"]:
            lines.append(
                f"| {s['id']} | {s['start']}..{s['end']} | {s['months']} | {gp._pct(s['change'])} |"
            )
        lines.append("")
    lines += [
        "## 判定（每个窗口按事先写死的读法：先取 q 大的，再取 t 小的）",
        "",
        "| 窗口 | t | q | 零效果误放 | 95% 上限 | 可用 | hedge_0.3 检验力 | 够 |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for key, w in result["windows"].items():
        if not w["decides"]:
            continue
        read = result["readings"][key]
        for p in read["pairs"].values():
            lines.append(
                f"| {key} | {p['threshold']:.2f} | {p['share']:.2f} | {_rate(p['zero'])} | "
                f"{gp._pct(p['zero']['high'])} | {_yes(p['usable'])} | "
                f"{gp._pct(p['power'])} | {_yes(p['enough'])} |"
            )
    lines.append("")
    for key, w in result["windows"].items():
        if w["decides"]:
            read = result["readings"][key]
            counts = read["counted_segments"]
            span = "-" if counts is None else f"{counts['min']}..{counts['max']}"
            lines.append(
                f"- {key}：{_verdict(read)}；计入的段数 {span}；"
                f"平时超额退回整窗口 {read['quiet_fallbacks']} 次。"
            )
    lines += ["", f"采纳：{OUTCOME_TEXT[result['outcome']]}", "", "## 各族通过率（只印）", ""]
    for key, w in result["windows"].items():
        read = result["readings"][key]
        if w["decides"]:
            lines += [
                f"### {key}",
                "",
                "| 族 | t | q | 三条都过 | P-1 | P-2 | P-3 | 只被 P-3 挡住 | 出错 |",
                "|---|---|---|---|---|---|---|---|---|",
            ]
            for p in read["pairs"].values():
                for f, row in p["families"].items():
                    rules = " | ".join(str(row["rules"][r]) for r in STAGE1_RULES)
                    lines.append(
                        f"| {f} | {p['threshold']:.2f} | {p['share']:.2f} | {_rate(row)} | "
                        f"{rules} | {row['last_rule_alone']} | {row['errors']} |"
                    )
        else:
            lines += [
                f"### {key}（第二段只查方向的三条）",
                "",
                "| 族 | 三条都过 | 几何 | r 中位数 | 再平衡收益 | 出错 |",
                "|---|---|---|---|---|---|",
            ]
            for f, row in read["families"].items():
                rules = " | ".join(str(row["rules"][r]) for r in STAGE2_RULES)
                lines.append(f"| {f} | {_rate(row)} | {rules} | {row['errors']} |")
        lines.append("")
    lines += [
        "## 分解（只印，各族种子的中位数，点/年）",
        "",
        "几何差 = 储备自身超额（全窗口）+ 再平衡收益；平时超额（段外）是 C 扣掉的那个。",
        "",
        "| 窗口 | 族 | 几何差 | 25 bps 几何差 | 储备自身超额（全窗口） | "
        "平时超额（段外） | 再平衡收益 | r 中位数 | 计入段数 |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    fields = ("geometric", "geometric_cost", "reserve_excess", "quiet_excess", "rebalancing")
    for key in result["windows"]:
        for f in FAMILIES:
            numbers = [
                r["numbers"]
                for r in result["evaluations"]
                if r["window_key"] == key and r["family"] == f and r["error"] is None
            ]

            def median(values: Sequence[Any]) -> float | None:
                finite = [v for v in values if _finite(v)]
                return float(np.median(finite)) if finite else None

            cells = [_points(median([n[field] for n in numbers])) for field in fields]
            ratio = median([n["summary"]["median_ratio"] for n in numbers])
            count = median([n["summary"]["count"] for n in numbers])
            cells += [
                "-" if ratio is None else f"{ratio:+.3f}",
                "-" if count is None else f"{count:g}",
            ]
            lines.append(f"| {key} | {f} | " + " | ".join(cells) + " |")
    errors = [r for r in result["evaluations"] if r["error"] is not None]
    if errors:
        lines += ["", f"## 出错（{len(errors)} 次，列前 20 次）", ""]
        for row in errors[:20]:
            lines.append(f"- {row['family']} {row['seed']} {row['window_key']}：{row['error']}")
    return "\n".join([*lines, ""])


def write_result(result: Mapping[str, Any], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_dir.joinpath(RESULT_JSON).write_text(
        json.dumps(gp._clean(dict(result)), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    out_dir.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


def load_data(
    snapshot_root: Path,
) -> tuple[dict[str, BenchData], tr.TrialGate, tr.TrialPlan, LoadedSettings, Path]:
    loaded = load_settings()
    root_dir = lg.research_root(loaded)
    gate, plan = tr.load_trial(root_dir, PLAN_ID)
    problems = tr.gate_problems(gate, loaded)
    if problems:
        raise ValueError("; ".join(problems))
    plan_config(plan)
    datasets = {}
    for market in ("us_market", "qqq"):
        data = tr.market_data(
            market, gate, plan, snapshot_root=snapshot_root, project_root=PROJECT_ROOT
        )
        if data.source.get("signal_snapshot_id") != plan.data.signal_snapshot_id:
            raise ValueError(f"{market} data are not of {plan.data.signal_snapshot_id}")
        datasets[market] = gp.cut(data)
    return datasets, gate, plan, loaded, root_dir


def check_targets(
    datasets: Mapping[str, BenchData], windows: Mapping[str, ProbeWindow], plan: tr.TrialPlan
) -> list[dict[str, Any]]:
    rows = []
    for key, item in windows.items():
        try:
            config = plan_config(plan)
            data = datasets[item.market]
            targets = strategy_targets(
                tr.STRATEGY_FUNCTIONS[config.function], data.inputs, config.params
            )
            dates = pd.DatetimeIndex(targets.index)
            inside = targets.loc[
                (dates >= pd.Timestamp(item.window.start))
                & (True if item.window.end is None else dates <= pd.Timestamp(item.window.end))
            ]
            missing = int(inside.isna().sum())
            rows.append(
                {
                    "window_key": key,
                    "window_days": len(inside),
                    "missing_days": missing,
                    "ok": len(inside) > 0 and missing == 0,
                }
            )
        except Exception as exc:
            rows.append({"window_key": key, "ok": False, "error": _error(exc)})
    return rows


def _load(
    root: Path,
) -> tuple[dict[str, BenchData], tr.TrialGate, tr.TrialPlan, LoadedSettings, Path]:
    try:
        return load_data(root)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        typer.echo(f"portfolio_ruler_check: 数据载入失败：{exc}")
        raise typer.Exit(code=1) from None


def _windows(
    loaded: LoadedSettings, gate: tr.TrialGate, question_types: Sequence[str] | None
) -> dict[str, ProbeWindow]:
    try:
        return (
            probe_windows(loaded, gate, question_types)
            if question_types
            else probe_windows(loaded, gate)
        )
    except (ValueError, KeyError) as exc:
        typer.echo(f"portfolio_ruler_check: 窗口不能用：{exc}")
        raise typer.Exit(code=1) from None


def _print_segments(found: Mapping[str, Any]) -> None:
    for key, section in found.items():
        label = "判" if section["window"]["decides"] else "只印"
        typer.echo(f"portfolio_ruler_check: {key} 下跌段 {section['count']} 段（{label}）")
        for s in section["segments"]:
            typer.echo(
                f"portfolio_ruler_check: {key} {s['id']} {s['start']}..{s['end']} "
                f"{s['months']} 个月 {gp._pct(s['change'])}"
            )


def _print_checks(rows: Sequence[Mapping[str, Any]]) -> bool:
    for row in rows:
        if "error" in row:
            typer.echo(f"portfolio_ruler_check: {row['window_key']} 算不出：{row['error']}")
        else:
            typer.echo(
                f"portfolio_ruler_check: {row['window_key']} 窗口内 {row['window_days']} 天，"
                f"缺 {row['missing_days']} 天，{'可以' if row['ok'] else '不行'}"
            )
    return all(row["ok"] for row in rows)


@app.callback()
def main() -> None:
    """只量 P1 组合尺子，不改门。"""


@app.command("check")
def check_command(
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
    question_type: Annotated[list[str] | None, typer.Option(help="美股问题类型，可重复。")] = None,
) -> None:
    datasets, gate, plan, loaded, _ = _load(root)
    windows = _windows(loaded, gate, question_type)
    declines = {
        key: window_declines(datasets[w.market].returns["qqq"], w.window)
        for key, w in windows.items()
    }
    _print_segments(segment_section(declines, windows))
    if not _print_checks(check_targets(datasets, windows, plan)):
        raise typer.Exit(code=1)


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="结果目录。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
    workers: Annotated[int, typer.Option(min=1, help="并行进程数。")] = 1,
    seeds: Annotated[int, typer.Option(min=1, help="每族种子数。")] = fp.DEFAULT_SEEDS,
    question_type: Annotated[list[str] | None, typer.Option(help="美股问题类型，可重复。")] = None,
) -> None:
    if any(out.joinpath(name).exists() for name in (JOURNAL_FILE, RESULT_JSON)):
        typer.echo("portfolio_ruler_check: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    datasets, gate, plan, loaded, root_dir = _load(root)
    windows = _windows(loaded, gate, question_type)
    if not _print_checks(check_targets(datasets, windows, plan)):
        typer.echo("portfolio_ruler_check: 开跑前检查没过，停下。")
        raise typer.Exit(code=1)
    out.mkdir(parents=True, exist_ok=True)
    result = run_probe(
        datasets,
        loaded,
        gate,
        plan,
        root_dir,
        seeds=seeds,
        workers=workers,
        windows=windows,
        journal=out / JOURNAL_FILE,
        progress=lambda message: typer.echo(f"portfolio_ruler_check: {message}"),
    )
    write_result(result, out)
    _print_segments(result["segments"])
    for key, w in windows.items():
        read = result["readings"][key]
        if w.decides:
            for p in read["pairs"].values():
                typer.echo(
                    f"portfolio_ruler_check: {key} {pair_key((p['threshold'], p['share']))} "
                    f"误放 {_rate(p['zero'])}，95% 上限 {gp._pct(p['zero']['high'])}，"
                    f"检验力 {gp._pct(p['power'])}"
                )
            typer.echo(f"portfolio_ruler_check: {key} 判定 {_verdict(read)}")
        else:
            families = read["families"]
            zero = "、".join(f"{f} {_rate(families[f])}" for f in ZERO_FAMILIES)
            typer.echo(
                f"portfolio_ruler_check: {key} 只印：hedge_0.3 方向三条都过 "
                f"{_rate(families[POWER_FAMILY])}，零效果族 {zero}"
            )
    typer.echo(f"portfolio_ruler_check: 采纳 {result['outcome']}")
    typer.echo(
        f"portfolio_ruler_check: {'完成' if result['complete'] else '有评估出错'}；结果目录 {out}"
    )


if __name__ == "__main__":
    app()
