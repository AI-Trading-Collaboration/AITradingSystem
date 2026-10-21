"""Measure slow-bear segmentation, thirteen fixed configurations and null-family false passes.

Development windows only: mechanically list monthly drops and long bulls, compare the
slow-bear track with prerun expectations, and measure standard/tight false-pass readings.
Simulation, matched benchmarks, costs, bootstrap and family summaries are the bench's
unchanged algorithms. Planted null/control rules deliberately look ahead. This probe does
not change the trial gate, reopen families, read held-out windows, count trials, write
research ledgers, or authorize production/broker actions. US market data are a proxy,
not QQQ evidence; passing data checks or this probe is not strategy eligibility.
"""

from __future__ import annotations

import json
import math
import multiprocessing
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Annotated, Any, cast

import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.checks import strategy_targets
from ai_trading_system.research_bench.data import BenchData, strategy_returns
from ai_trading_system.research_bench.falsify import (
    FalsificationInputs,
    _rerun,
    bootstrap_interval,
    cost_section,
)
from ai_trading_system.research_bench.metrics import compare, summarize
from ai_trading_system.research_bench.prereg import Rule, evaluate_rules
from ai_trading_system.research_bench.run import PRIMARY_BENCHMARK, benchmark_simulations
from ai_trading_system.research_bench.segments import DROP as DROP
from ai_trading_system.research_bench.segments import LONG_BULL_MONTHS as LONG_BULL_MONTHS
from ai_trading_system.research_bench.segments import SLOW_MONTHS as SLOW_MONTHS
from ai_trading_system.research_bench.segments import Segment as Segment
from ai_trading_system.research_bench.segments import _inside as _inside
from ai_trading_system.research_bench.segments import annual_return as annual_return
from ai_trading_system.research_bench.segments import drop_segments as drop_segments
from ai_trading_system.research_bench.segments import long_bull_segments as long_bull_segments
from ai_trading_system.research_bench.segments import long_bull_summary as long_bull_summary
from ai_trading_system.research_bench.segments import monthly_level as monthly_level
from ai_trading_system.research_bench.segments import segment_return as segment_return
from ai_trading_system.research_bench.segments import segment_rows as segment_rows
from ai_trading_system.research_bench.segments import segments as segments
from ai_trading_system.research_bench.segments import slow_bear_summary as slow_bear_summary
from ai_trading_system.research_bench.segments import window_returns as window_returns
from ai_trading_system.research_bench.settings import (
    LoadedSettings,
    Window,
    load_settings,
    resolve_window,
)
from ai_trading_system.research_bench.simulate import Rebalance, Simulation, simulate
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import backfill_check as bc
from ai_trading_system.research_probes import gate_false_pass as fp
from ai_trading_system.research_probes import gate_power as gp

RUN_ID = "TRADING-2565-34"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-21:research_loop_p2"
# Segmentation rules are in research_bench.segments (TRADING-2565-36, moved unchanged).
MIN_MEDIAN_SEGMENTS = 4
SB1 = 0.10
SB2 = 0.75
TIGHT_SB1 = 0.15
TIGHT_NONPOSITIVE = 1
FIDELITY_TOLERANCE = 1e-9
COST_BPS = fp.COST_BPS
COST = cost_section(COST_BPS)
USABLE_LINE = fp.USABLE_LINE
TRIALS = tuple(f"T000{n}" for n in range(1, 8))
POSITIVE_CONTROLS = ("fast_slow", "trend4", "slow_only")
US_1963 = Window("us_market_1963_2010", "development", date(1963, 1, 2), date(2010, 12, 31))
WINDOW_KEYS = ("us_1929", "us_1963", "us_1962", "qqq")
WINDOW_MARKETS: dict[str, tr.Market] = {
    "us_1929": "us_market",
    "us_1963": "us_market",
    "us_1962": "us_market",
    "qqq": "qqq",
}
FALSE_PASS_WINDOWS = ("us_1929", "us_1963", "qqq")
BOOTSTRAP_WINDOWS = ("us_1963", "qqq")
# Requirements' inferred dates, not fitted to a run's calculated segments.
INFERRED = {
    "us_1929": (
        ("1929-09", "1932-06"),
        ("1937-03", "1938-03"),
        ("1938-11", "1942-04"),
        ("1946-05", "1949-06"),
        ("1961-12", "1962-06"),
        ("1968-11", "1970-06"),
        ("1973-01", "1974-09"),
        ("1980-11", "1982-07"),
        ("2000-08", "2002-09"),
        ("2007-10", "2009-02"),
    ),
    "qqq": (("2000-03", "2002-09"), ("2007-10", "2008-11")),
}
INFERRED["us_1963"] = INFERRED["us_1929"][5:]
JOURNAL_FILE = "slow_bear_rows.jsonl"
RESULT_JSON = "slow_bear_result.json"
RESULT_MD = "slow_bear_result.md"

app = typer.Typer(help="慢熊判据探针：只量开发窗口，不改门、不写研究账本。")


def compare_inferred(
    found: Sequence[Segment], inferred: Sequence[tuple[str, str]]
) -> list[dict[str, Any]]:
    slow = [s for s in found if s.kind == "slow_bear"]
    used: set[str] = set()
    rows = []
    for peak, trough in inferred:
        overlapping = [
            s
            for s in slow
            if s.peak is not None and s.trough is not None and s.peak <= trough and peak <= s.trough
        ]
        exact = next((s for s in overlapping if (s.peak, s.trough) == (peak, trough)), None)
        picked = exact or next(iter(overlapping), None)
        if picked is not None:
            used.add(picked.id)
        rows.append(
            {
                "inferred": f"{peak}..{trough}",
                "found": None if picked is None else picked.id,
                "match": "same" if exact else ("shifted" if picked else "missing"),
            }
        )
    rows.extend(
        {"inferred": None, "found": s.id, "match": "extra"} for s in slow if s.id not in used
    )
    return rows


@dataclass(frozen=True)
class Check:
    metric: str
    op: str
    value: float


def slow_bear_checks(stage: str, count: int, *, tight: bool = False) -> tuple[Check, ...]:
    if stage == "stage2" or count < MIN_MEDIAN_SEGMENTS:
        return (Check("slow_bear.positive_share", ">=", 1.0),)
    if tight:
        return (
            Check("slow_bear.median_difference", ">=", TIGHT_SB1),
            Check("slow_bear.nonpositive", "<=", TIGHT_NONPOSITIVE),
        )
    return (
        Check("slow_bear.median_difference", ">=", SB1),
        Check("slow_bear.positive_share", ">=", SB2),
    )


def _finite(value: Any) -> bool:
    return isinstance(value, int | float) and math.isfinite(value)


def check_rows(checks: Sequence[Check], sections: dict[str, Any]) -> list[dict[str, Any]]:
    # evaluate_rules reads only metric/op/value: Check has the same structural interface.
    # Keep probe-only sections out of the formal preregistration Rule schema.
    rules = cast(tuple[Rule, ...], tuple(checks))
    rows = evaluate_rules(rules, sections)
    for row in rows:
        if not _finite(row["observed"]):
            row["holds"] = False
    return rows


def common_rules(gate: tr.TrialGate, market: tr.Market) -> tuple[Rule, ...]:
    return tuple(r for r in gate.markets[market].rules if "cvar" not in r.metric)


def slow_bear_track(
    stage: str, sections: dict[str, Any], gate: tr.TrialGate, *, tight: bool = False
) -> list[dict[str, Any]]:
    market: tr.Market = "us_market" if stage == "stage1" else "qqq"
    return [
        *check_rows(slow_bear_checks(stage, sections["slow_bear"]["count"], tight=tight), sections),
        *evaluate_rules(common_rules(gate, market), sections),
    ]


def crash_track(stage: str, sections: dict[str, Any], gate: tr.TrialGate) -> list[dict[str, Any]]:
    market: tr.Market = "us_market" if stage == "stage1" else "qqq"
    return evaluate_rules(gate.markets[market].rules, sections)


@dataclass(frozen=True)
class Pair:
    strategy: Simulation
    benchmark: Simulation
    comparison: dict[str, Any]
    cost: dict[str, Any]


def simulate_pair(
    data: BenchData,
    targets: pd.Series,
    window: Window,
    rebalance: Rebalance,
    loaded: LoadedSettings,
    reserve: str = "cash",
) -> Pair:
    traded = strategy_returns(data, reserve)
    own = simulate(
        traded,
        targets,
        start=window.start,
        end=window.end,
        rebalance=rebalance,
        cost_bps=loaded.cost_bps,
    )
    benchmark = benchmark_simulations(
        data,
        window,
        average_exposure=float(own.exposure.mean()),
        rebalance=rebalance,
        cost_bps=loaded.cost_bps,
        leveraged=False,
    )[PRIMARY_BENCHMARK]
    cash = data.returns["cash"]
    comparison = compare(summarize(own, cash, loaded), summarize(benchmark, cash, loaded), loaded)
    cost = _rerun(
        FalsificationInputs(
            returns=data.returns,
            targets=targets,
            start=window.start,
            end=window.end,
            rebalance=rebalance,
            delay_days=0,
            cost_bps=loaded.cost_bps,
            strategy=own,
            benchmark=benchmark,
            strategy_returns=None if reserve == "cash" else traded,
        ),
        loaded,
        cost_bps=COST_BPS,
    )
    return Pair(own, benchmark, comparison, cost)


def pair_sections(
    pair: Pair, items: Sequence[Segment]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    rows = segment_rows(pair.strategy, pair.benchmark, items)
    return {
        "comparison": pair.comparison,
        COST: pair.cost,
        "slow_bear": slow_bear_summary(rows),
    }, rows


def probe_windows(loaded: LoadedSettings, gate: tr.TrialGate) -> dict[str, Window]:
    us = gate.markets["us_market"]
    return {
        "us_1929": resolve_window(loaded, us.question_type, "development"),
        "us_1963": US_1963,
        "us_1962": resolve_window(loaded, us.question_type_for("treasury_10y"), "development"),
        "qqq": resolve_window(loaded, gate.markets["qqq"].question_type, "development"),
    }


def window_record(window: Window) -> dict[str, Any]:
    return {
        "window_id": window.window_id,
        "start": window.start.isoformat(),
        "end": gp._iso(window.end),
    }


def segment_lists(
    datasets: Mapping[str, BenchData], windows: Mapping[str, Window]
) -> dict[str, list[Segment]]:
    return {
        key: segments(window_returns(datasets[WINDOW_MARKETS[key]].returns["qqq"], windows[key]))
        for key in WINDOW_KEYS
    }


@dataclass(frozen=True)
class Item:
    id: str
    source: str
    label: str
    function: str
    params: dict[str, Any]
    rebalance: Rebalance
    reserve: str
    primary: str | None = None


def probe_items(root_dir: Path) -> list[Item]:
    items = []
    for config in bc.CONFIGS:
        entry = bc.source_entry(root_dir, config)
        items.append(
            Item(
                config.id,
                "backfill",
                f"{config.question} {config.strategy}",
                entry.function,
                entry.params,
                entry.rebalance.to_rebalance(),
                "cash",
            )
        )
    for trial_id in TRIALS:
        _, plan = tr.load_trial(root_dir, trial_id)
        primary = plan.config(plan.primary)
        items.append(
            Item(
                trial_id,
                "trial",
                f"{trial_id} {plan.primary}",
                primary.function,
                primary.params,
                primary.rebalance.to_rebalance(),
                primary.reserve,
                plan.primary,
            )
        )
    return items


def stage_windows(item: Item, gate: tr.TrialGate) -> dict[str, str]:
    us = gate.markets["us_market"]
    question_type = us.question_type_for(item.reserve)
    if question_type == us.question_type:
        key = "us_1929"
    elif question_type == us.question_type_for("treasury_10y"):
        key = "us_1962"
    else:
        raise ValueError(f"{item.id}: no probe window for {question_type}")
    return {"stage1": key, "stage2": "qqq"}


@dataclass(frozen=True)
class Expectation:
    stage1: bool
    stage1_probability: float
    stage2: bool | None
    stage2_probability: float
    overall: bool
    overall_probability: float | None


# Requirements section 7: expectations written before any run, not inferred from outcomes.
EXPECTATIONS = {
    "fast_slow": Expectation(True, 0.7, True, 0.8, True, 0.56),
    "trend4": Expectation(True, 0.7, True, 0.8, True, 0.56),
    "slow_only": Expectation(True, 0.6, True, 0.85, True, 0.5),
    "rv21": Expectation(True, 0.9, True, 0.85, True, 0.77),
    "fast_slow_weekly": Expectation(True, 0.75, True, 0.8, True, 0.6),
    "rv21_weekly": Expectation(True, 0.9, True, 0.85, True, 0.77),
    "T0001": Expectation(False, 0.8, False, 0.7, False, None),
    "T0002": Expectation(False, 0.9, False, 0.9, False, None),
    "T0003": Expectation(False, 0.8, True, 0.6, False, None),
    "T0004": Expectation(True, 0.9, True, 0.9, True, 0.81),
    "T0005": Expectation(True, 0.65, True, 0.75, True, 0.5),
    "T0006": Expectation(False, 0.85, None, 0.5, False, None),
    "T0007": Expectation(False, 0.6, None, 0.5, False, None),
}


def original_values(
    gate_rows: Sequence[Mapping[str, Any]], metrics: Sequence[str]
) -> dict[str, Any]:
    observed = {row["metric"]: row.get("observed") for row in gate_rows}
    return {metric: observed.get(metric) for metric in metrics}


def trial_source(
    records: Sequence[Mapping[str, Any]],
    trial_id: str,
    primary: str,
    market: tr.Market,
    metrics: Sequence[str],
) -> dict[str, Any] | None:
    for record in records:
        if (
            record.get("trial_id") == trial_id
            and record.get("config") == primary
            and record.get("market") == market
            and record.get("status") == "ok"
        ):
            return {
                "run_id": record["run_id"],
                "values": original_values(record.get("gate") or [], metrics),
            }
    return None


def load_backfill(path: Path) -> dict[str, Any]:
    result = json.loads(path.read_text(encoding="utf-8"))
    if (
        not isinstance(result, dict)
        or result.get("run_id") != bc.RUN_ID
        or result.get("complete") is not True
    ):
        raise ValueError(f"not a complete {bc.RUN_ID} result")
    return result


def backfill_source(
    result: Mapping[str, Any], config_id: str, metrics: Sequence[str]
) -> dict[str, Any] | None:
    for row in result.get("rows", []):
        if row.get("config") == config_id and row.get("status") == "ok":
            return {
                "run_id": f"{result['run_id']} {config_id}",
                "values": original_values(row.get("gate") or [], metrics),
            }
    return None


def fidelity(values: Mapping[str, Any], source: Mapping[str, Any] | None) -> dict[str, Any]:
    if source is None:
        return {"status": "no_record", "run_id": None, "differences": {}}
    differences = {
        metric: float(value) - float(original)
        if _finite(value) and _finite(original := source["values"].get(metric))
        else None
        for metric, value in values.items()
    }
    mismatch = any(v is None or abs(v) > FIDELITY_TOLERANCE for v in differences.values())
    return {
        "status": "mismatch" if mismatch else "match",
        "run_id": source["run_id"],
        "differences": differences,
    }


def _error(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}".splitlines()[0][: lg.ERROR_TEXT_LIMIT]


def evaluate_stage(
    item: Item,
    stage: str,
    window_key: str,
    datasets: Mapping[str, BenchData],
    windows: Mapping[str, Window],
    lists: Mapping[str, Sequence[Segment]],
    gate: tr.TrialGate,
    loaded: LoadedSettings,
    source: Mapping[str, Any] | None,
) -> dict[str, Any]:
    market = WINDOW_MARKETS[window_key]
    row: dict[str, Any] = {
        "stage": stage,
        "market": market,
        "window_key": window_key,
        "window": window_record(windows[window_key]),
    }
    try:
        data = datasets[market]
        targets = strategy_targets(tr.STRATEGY_FUNCTIONS[item.function], data.inputs, item.params)
        pair = simulate_pair(
            data, targets, windows[window_key], item.rebalance, loaded, item.reserve
        )
        sections, details = pair_sections(pair, lists[window_key])
        values = original_values(
            evaluate_rules(common_rules(gate, market), sections),
            [r.metric for r in common_rules(gate, market)],
        )
        check = fidelity(values, source)
        standard = slow_bear_track(stage, sections, gate)
        tight = slow_bear_track(stage, sections, gate, tight=True)
        row.update(
            status="ok",
            fidelity=check,
            read=check["status"] != "mismatch",
            values=values,
            average_exposure=float(pair.strategy.exposure.mean()),
            evaluated={
                "start": pair.strategy.start.date().isoformat(),
                "first_return": pd.Timestamp(pair.strategy.returns.index[0]).date().isoformat(),
                "end": pd.Timestamp(pair.strategy.returns.index[-1]).date().isoformat(),
            },
            segments=details,
            slow_bear=sections["slow_bear"],
            long_bull=long_bull_summary(details),
            standard=standard,
            standard_holds=all(r["holds"] for r in standard),
            tight=tight,
            tight_holds=all(r["holds"] for r in tight),
        )
    except Exception as exc:
        row.update(status="error", error=_error(exc), read=False)
    return dict(gp._clean(row))


def evaluate_item(
    item: Item,
    datasets: Mapping[str, BenchData],
    windows: Mapping[str, Window],
    lists: Mapping[str, Sequence[Segment]],
    gate: tr.TrialGate,
    loaded: LoadedSettings,
    sources: Mapping[str, Any],
) -> dict[str, Any]:
    keys = stage_windows(item, gate)
    stages = {
        stage: evaluate_stage(
            item, stage, keys[stage], datasets, windows, lists, gate, loaded, sources.get(stage)
        )
        for stage, _ in fp.STAGES
    }
    read = all(row["read"] for row in stages.values())
    verdict = {
        track: ("pass" if all(row[f"{track}_holds"] for row in stages.values()) else "fail")
        if read
        else "unread"
        for track in ("standard", "tight")
    }
    expected = asdict(EXPECTATIONS[item.id])
    judged = {
        stage: row["standard_holds"] == expected[stage]
        if row["read"] and expected[stage] is not None
        else None
        for stage, row in stages.items()
    }
    judged["overall"] = (verdict["standard"] == "pass") == expected["overall"] if read else None
    return {
        "config": item.id,
        "source": item.source,
        "label": item.label,
        "reserve": item.reserve,
        "stages": stages,
        "verdict": verdict,
        "expected": expected,
        "judged": judged,
    }


def positive_controls(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    picked = {
        row["config"]: row["stages"]["stage1"] for row in rows if row["config"] in POSITIVE_CONTROLS
    }
    read = all(config in picked and picked[config]["read"] for config in POSITIVE_CONTROLS)
    return {
        "configs": list(POSITIVE_CONTROLS),
        "all_read": read,
        "all_failed": read and all(not row["standard_holds"] for row in picked.values()),
    }


def needed_metrics(gate: tr.TrialGate) -> list[tuple[str, str]]:
    return list(
        dict.fromkeys(
            (section, name)
            for _, market in fp.STAGES
            for rule in gate.markets[market].rules
            for section, _, name in (rule.metric.partition("."),)
        )
    )


def evaluate_null(
    data: BenchData,
    targets: pd.Series,
    window: Window,
    items: Sequence[Segment],
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    *,
    bootstrap: bool,
) -> dict[str, Any]:
    pair = simulate_pair(data, targets, window, gp.MONTH_END, loaded)
    sections, _ = pair_sections(pair, items)
    if bootstrap:
        dates = pair.strategy.returns.index
        low, high = bootstrap_interval(
            pair.strategy.returns.to_numpy(dtype=float),
            pair.benchmark.returns.reindex(dates).to_numpy(dtype=float),
            data.returns["cash"].reindex(dates).to_numpy(dtype=float),
            loaded,
            gate.confidence,
        )
        sections.update(ci_low=low, ci_high=high)
    kept: dict[str, Any] = {}
    for section, name in needed_metrics(gate):
        if section in sections:
            kept.setdefault(section, {})[name] = sections[section].get(name)
    kept["slow_bear"] = sections["slow_bear"]
    return {
        "average_exposure": float(pair.strategy.exposure.mean()),
        "return_days": len(pair.strategy.returns),
        "effect": {field: pair.comparison[field] for field in fp.EFFECT_FIELDS},
        "sections": kept,
        "error": None,
    }


_WORKER: dict[str, Any] = {}


def _init(payload: dict[str, Any]) -> None:
    _WORKER.clear()
    _WORKER.update(payload)


def _task(task: tuple[str, int, str]) -> dict[str, Any]:
    family_id, seed, key = task
    family = fp.FAMILIES[family_id]
    try:
        data = _WORKER["data"][WINDOW_MARKETS[key]]
        targets = fp.family_targets(family, data.inputs, seed)
        row = evaluate_null(
            data,
            targets,
            _WORKER["windows"][key],
            _WORKER["lists"][key],
            _WORKER["loaded"],
            _WORKER["gate"],
            bootstrap=key in BOOTSTRAP_WINDOWS,
        )
    except Exception as exc:
        row = {
            "average_exposure": None,
            "return_days": None,
            "effect": {},
            "sections": {},
            "error": _error(exc),
        }
    return dict(
        gp._clean(
            {"family": family_id, "role": family.role, "seed": seed, "window_key": key, **row}
        )
    )


@dataclass(frozen=True)
class Reading:
    id: str
    track: str
    stage1: str


READINGS = (
    Reading("slow_1929", "slow_bear", "us_1929"),
    Reading("slow_1963", "slow_bear", "us_1963"),
    Reading("crash_1963", "crash", "us_1963"),
)


def reading_rows(
    evaluations: Sequence[Mapping[str, Any]],
    reading: Reading,
    gate: tr.TrialGate,
    *,
    tight: bool = False,
) -> list[dict[str, Any]]:
    rows = []
    for row in evaluations:
        if row["window_key"] not in (reading.stage1, "qqq"):
            continue
        stage = "stage2" if row["window_key"] == "qqq" else "stage1"
        if row["error"]:
            rules = []
        elif reading.track == "slow_bear":
            rules = slow_bear_track(stage, row["sections"], gate, tight=tight)
        else:
            rules = crash_track(stage, row["sections"], gate)
        rows.append(
            {
                "family": row["family"],
                "role": row["role"],
                "seed": row["seed"],
                "stage": stage,
                "holds": not row["error"] and all(r["holds"] for r in rules),
                "error": row["error"],
                "rules": rules,
                "effect": row["effect"],
            }
        )
    return rows


def sb_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts = dict.fromkeys(("sb1_only", "sb2_only", "both", "neither"), 0)
    zero_ids = {f.family_id for f in fp.ZERO_FAMILIES}
    for row in rows:
        if row["family"] not in zero_ids or row["stage"] != "stage1" or row["error"]:
            continue
        holds = {r["metric"]: r["holds"] for r in row["rules"]}
        first = holds.get("slow_bear.median_difference", False)
        second = holds.get("slow_bear.positive_share", False)
        key = (
            "both"
            if first and second
            else "sb1_only"
            if first
            else "sb2_only"
            if second
            else "neither"
        )
        counts[key] += 1
    return counts


def read_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    families = {
        family: fp.family_summary([r for r in rows if r["family"] == family])
        for family in dict.fromkeys(r["family"] for r in rows)
    }
    zero_ids = {f.family_id for f in fp.ZERO_FAMILIES}
    zero = fp.family_summary([r for r in rows if r["family"] in zero_ids])
    both = zero["both"]
    verdict = (
        "incomplete"
        if any(row.get("error") for row in rows) or not both["total"]
        else "usable"
        if both["high"] <= USABLE_LINE
        else "over"
    )
    return {
        "families": families,
        "zero": zero,
        "passes": both["passes"],
        "total": both["total"],
        "upper": both["high"],
        "verdict": verdict,
    }


def false_pass_readings(
    evaluations: Sequence[Mapping[str, Any]], gate: tr.TrialGate
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for reading in READINGS:
        row: dict[str, Any] = {"track": reading.track, "stage1": reading.stage1}
        if reading.track == "slow_bear":
            standard = reading_rows(evaluations, reading, gate)
            row["standard"] = {**read_rows(standard), "sb": sb_counts(standard)}
            row["tight"] = read_rows(reading_rows(evaluations, reading, gate, tight=True))
        else:
            row["gate"] = read_rows(reading_rows(evaluations, reading, gate))
        result[reading.id] = row
    slow = [result[r.id] for r in READINGS if r.track == "slow_bear"]
    if any(r["standard"]["verdict"] == "incomplete" for r in slow):
        applies = "incomplete"
    elif any(r["standard"]["verdict"] == "over" for r in slow):
        applies = "tight" if all(r["tight"]["verdict"] == "usable" for r in slow) else "neither"
    else:
        applies = "standard"
    result["applies"] = applies
    return result


def false_pass_tasks(seeds: int, control_seeds: int) -> list[tuple[str, int, str]]:
    return [
        (f.family_id, seed, key)
        for f in fp.FAMILIES.values()
        for seed in fp.seed_list(seeds if f.role == "zero" else control_seeds)
        for key in FALSE_PASS_WINDOWS
    ]


def segment_section(
    lists: Mapping[str, Sequence[Segment]], windows: Mapping[str, Window]
) -> dict[str, Any]:
    return {
        key: {
            "window": window_record(windows[key]),
            "segments": [asdict(s) for s in lists[key]],
            "inferred": compare_inferred(lists[key], INFERRED[key]) if key in INFERRED else None,
        }
        for key in WINDOW_KEYS
    }


def run_probe(
    datasets: Mapping[str, BenchData],
    null_datasets: Mapping[str, BenchData],
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    root_dir: Path,
    *,
    items: Sequence[Item],
    sources: Mapping[str, Any],
    seeds: int = fp.DEFAULT_SEEDS,
    control_seeds: int = fp.DEFAULT_CONTROL_SEEDS,
    workers: int = 1,
    windows: Mapping[str, Window] | None = None,
    journal: Path | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    if seeds < 1 or control_seeds < 0:
        raise ValueError("seeds must be positive and control_seeds nonnegative")
    for market, data in null_datasets.items():
        for frame in (data.returns, data.inputs):
            if pd.Timestamp(pd.DatetimeIndex(frame.index).max()).date() > gp.CUTOFF:
                raise ValueError(f"{market}: data run past {gp.CUTOFF}; cut them first")
    windows = probe_windows(loaded, gate) if windows is None else windows
    head = gp._clean(
        {
            "run_id": RUN_ID,
            "owner_decision": OWNER_DECISION,
            "gate_sha256": lg.chain_digest((root_dir / tr.TRIALS_DIR / tr.GATE_FILE).read_bytes()),
            "settings_sha256": loaded.sha256,
            "confidence": gate.confidence,
            "cost_bps": COST_BPS,
            "thresholds": {
                "drop": DROP,
                "slow_months": SLOW_MONTHS,
                "long_bull_months": LONG_BULL_MONTHS,
                "min_median_segments": MIN_MEDIAN_SEGMENTS,
                "sb1": SB1,
                "sb2": SB2,
                "tight_sb1": TIGHT_SB1,
                "tight_nonpositive": TIGHT_NONPOSITIVE,
                "fidelity_tolerance": FIDELITY_TOLERANCE,
                "usable_line": USABLE_LINE,
            },
            "windows": {key: window_record(windows[key]) for key in WINDOW_KEYS},
            "source": {market: dict(data.source) for market, data in datasets.items()},
            "seeds": seeds,
            "control_seeds": control_seeds,
            "provenance": git_provenance(root_dir),
        }
    )
    configs: list[dict[str, Any]] = []
    evaluations: list[dict[str, Any]] = []
    sink = None if journal is None else journal.open("x", encoding="utf-8")

    def keep(value: dict[str, Any], message: str = "") -> None:
        if sink is not None:
            sink.write(json.dumps(gp._clean(value), ensure_ascii=False, allow_nan=False) + "\n")
            sink.flush()
        if message and progress is not None:
            progress(message)

    try:
        keep(head)
        lists = segment_lists(datasets, windows)
        found = segment_section(lists, windows)
        keep({"segments": found})
        for item in items:
            row = evaluate_item(
                item, datasets, windows, lists, gate, loaded, sources.get(item.id, {})
            )
            configs.append(row)
            keep(row, f"配置 {item.id}")
        tasks = false_pass_tasks(seeds, control_seeds)
        payload = {
            "data": dict(null_datasets),
            "windows": dict(windows),
            "lists": segment_lists(null_datasets, windows),
            "loaded": loaded,
            "gate": gate,
        }
        if workers <= 1:
            _init(payload)
            for done, task in enumerate(tasks, start=1):
                row = _task(task)
                evaluations.append(row)
                keep(row, f"误放率评估 {done}/{len(tasks)}")
        else:
            with ProcessPoolExecutor(
                max_workers=workers,
                mp_context=multiprocessing.get_context("spawn"),
                initializer=_init,
                initargs=(payload,),
            ) as pool:
                for done, row in enumerate(pool.map(_task, tasks), start=1):
                    evaluations.append(row)
                    keep(row, f"误放率评估 {done}/{len(tasks)}")
    finally:
        if sink is not None:
            sink.close()
    readings = false_pass_readings(evaluations, gate)
    return dict(
        gp._clean(
            {
                **head,
                "segments": found,
                "configs": configs,
                "positive_controls": positive_controls(configs),
                "false_pass": {"evaluations": evaluations, "readings": readings},
                "complete": all(
                    s["status"] == "ok" for row in configs for s in row["stages"].values()
                )
                and readings["applies"] != "incomplete",
            }
        )
    )


MATCH_TEXT = {"same": "一致", "shifted": "起止不同", "missing": "缺少", "extra": "多出"}
KIND_TEXT = {"slow_bear": "慢熊", "fast_drop": "快跌", "long_bull": "长牛"}
VERDICT_TEXT = {"pass": "过", "fail": "不过", "unread": "不读"}
READ_TEXT = {"usable": "可用", "over": "超线", "incomplete": "不完整"}
FIDELITY_TEXT = {"match": "一致", "mismatch": "不一致", "no_record": "无原记录"}


def _points(value: Any) -> str:
    return "-" if value is None else f"{float(value) * 100:+.1f}"


def _bool_text(value: Any) -> str:
    return "不定" if value is None else "过" if value else "不过"


def _holds_text(stage: Mapping[str, Any], track: str = "standard") -> str:
    return _bool_text(stage[f"{track}_holds"]) if stage["read"] else "不读"


def render_markdown(result: Mapping[str, Any]) -> str:
    lines = [
        f"# 慢熊判据探针（{RUN_ID}）",
        "",
        f"- 依据：{result['owner_decision']}；门 sha256：{result['gate_sha256']}。",
        f"- 门槛（运行前写定）：{result['thresholds']}；成本重跑 {result['cost_bps']} bps；"
        f"置信度 {gp._pct(result['confidence'])}；设置 sha256：{result['settings_sha256']}。",
        "- 数据由试跑的数据质量核查路径载入；来源及质量状态见 JSON 的 source。"
        "美股大盘为代理，不是 QQQ 证据；只量开发窗口，不改门、不计试验，不授权生产或 broker。",
        "",
        "## 段清单",
        "",
    ]
    for key, section in result["segments"].items():
        w = section["window"]
        lines += [
            f"### {key}：{w['window_id']} {w['start']}..{w['end']}",
            "",
            "| 段 | 分类 | 高点 P | 谷底 T | 段内月份 | 变化 | 月数 |",
            "|---|---|---|---|---|---|---|",
        ]
        for s in section["segments"]:
            lines.append(
                f"| {s['id']} | {KIND_TEXT[s['kind']]} | {s['peak'] or '-'} | "
                f"{s['trough'] or '-'} | {s['start']}..{s['end']} | "
                f"{gp._pct(s['change'])} | {s['months']} |"
            )
        if section["inferred"] is not None:
            lines += ["", "| 推断的慢熊段 | 算出的段 | 对照 |", "|---|---|---|"]
            for row in section["inferred"]:
                lines.append(
                    f"| {row['inferred'] or '-'} | {row['found'] or '-'} | "
                    f"{MATCH_TEXT[row['match']]} |"
                )
        lines.append("")
    lines += [
        "## 13 个配置（慢熊轨）",
        "",
        "| 配置 | 来源 | 保真 | 第一段窗口 | 慢熊段数 | D 中位数 | D 为正占比 | D 最小 | "
        "第一段 | 第一段收紧 | 第二段 | 两段 | 两段收紧 | 预期（第一段 / 第二段 / 两段） | 对错 |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for row in result["configs"]:
        first, second = (row["stages"][stage] for stage, _ in fp.STAGES)
        fidelity_text = " / ".join(
            FIDELITY_TEXT[s["fidelity"]["status"]] if s["status"] == "ok" else "出错"
            for s in (first, second)
        )
        slow = first.get("slow_bear", {})
        expected = " / ".join(
            _bool_text(row["expected"][k]) for k in ("stage1", "stage2", "overall")
        )
        judged = " / ".join(
            "-" if v is None else "对" if v else "错" for v in row["judged"].values()
        )
        cells = [
            row["config"],
            row["label"],
            fidelity_text,
            first["window_key"],
            str(slow.get("count", "-")),
            _points(slow.get("median_difference")),
            gp._pct(slow.get("positive_share")),
            _points(slow.get("min_difference")),
            _holds_text(first),
            _holds_text(first, "tight"),
            _holds_text(second),
            VERDICT_TEXT[row["verdict"]["standard"]],
            VERDICT_TEXT[row["verdict"]["tight"]],
            expected,
            judged,
        ]
        lines.append("| " + " | ".join(cells) + " |")
        for stage in (first, second):
            if stage["status"] == "error":
                lines.append(f"\n{row['config']} {stage['stage']} 出错：{stage['error']}\n")
    if result["positive_controls"]["all_failed"]:
        lines += ["", "阳性对照全没过：判据或划段要回炉（需求第 6 节）。"]
    for key, section in result["segments"].items():
        ids = [s["id"] for s in section["segments"]]
        lines += [
            "",
            f"### {key}：每段 D（点）",
            "",
            "每格为 D（策略段内 / 基准段内；平均敞口）。",
            "",
            "| 配置 | " + " | ".join([*ids, "长牛年化差的平均 / 最差"]) + " |",
            "|---|" + "---|" * (len(ids) + 1),
        ]
        for row in result["configs"]:
            for stage in row["stages"].values():
                if stage["window_key"] != key or stage["status"] != "ok":
                    continue
                details = {s["id"]: s for s in stage["segments"]}
                cells = []
                for segment_id in ids:
                    s = details.get(segment_id)
                    cells.append(
                        "-"
                        if s is None
                        else f"{_points(s['difference'])}（{gp._pct(s['strategy_return'])} / "
                        f"{gp._pct(s['benchmark_return'])}；{gp._pct(s['average_exposure'])}）"
                    )
                bull = stage["long_bull"]
                cells.append(
                    f"{_points(bull['mean_annual_difference'])} / "
                    f"{_points(bull['worst_annual_difference'])}"
                )
                lines.append(f"| {row['config']} | " + " | ".join(cells) + " |")
    lines += [
        "",
        "## 误放率",
        "",
        "| 读数 | 一套 | 两段都过 | 95% 上限 | 判读 | 各族 95% 上限 |",
        "|---|---|---|---|---|---|",
    ]
    readings = result["false_pass"]["readings"]
    for reading in READINGS:
        sets = (
            (("standard", "标准"), ("tight", "收紧"))
            if reading.track == "slow_bear"
            else (("gate", "现在的门"),)
        )
        for key, name in sets:
            read = readings[reading.id][key]
            families = "；".join(
                f"{f} {gp._pct(v['both']['high'])}" for f, v in read["families"].items()
            )
            lines.append(
                f"| {reading.id} | {name} | {read['passes']}/{read['total']} | "
                f"{gp._pct(read['upper'])} | {READ_TEXT[read['verdict']]} | {families} |"
            )
    for reading in READINGS:
        if reading.track == "slow_bear":
            sb = readings[reading.id]["standard"]["sb"]
            lines.append(
                f"\n{reading.id}：SB1 单独 {sb['sb1_only']}，SB2 单独 {sb['sb2_only']}，"
                f"两条都过 {sb['both']}，都不过 {sb['neither']}。"
            )
    lines += ["", f"适用的一套：{readings['applies']}。", "", "## 实际评估范围", ""]
    for row in result["configs"]:
        for stage in row["stages"].values():
            if stage["status"] == "ok":
                evaluated = stage["evaluated"]
                lines.append(
                    f"- {row['config']} {stage['stage']}：请求 "
                    f"{stage['window']['start']}..{stage['window']['end']}；"
                    f"实际持仓起点 {evaluated['start']}，收益日期 "
                    f"{evaluated['first_return']}..{evaluated['end']}。"
                )
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
) -> tuple[dict[str, BenchData], dict[str, BenchData], tr.TrialGate, LoadedSettings, Path]:
    loaded = load_settings()
    root_dir = lg.research_root(loaded)
    gate, plan = tr.load_trial(root_dir, gp.TRIAL_ID)
    problems = tr.gate_problems(gate, loaded)
    if problems:
        raise ValueError("; ".join(problems))
    for trial_id in TRIALS:
        _, other = tr.load_trial(root_dir, trial_id)
        if other.data != plan.data:
            raise ValueError(f"{trial_id} does not use the data of {plan.trial_id}")
    datasets: dict[str, BenchData] = {}
    for market in ("qqq", "us_market"):
        data = tr.market_data(
            market, gate, plan, snapshot_root=snapshot_root, project_root=PROJECT_ROOT
        )
        if data.source.get("signal_snapshot_id") != plan.data.signal_snapshot_id:
            raise ValueError(f"{market} data are not of {plan.data.signal_snapshot_id}")
        datasets[market] = data
    return (
        datasets,
        {market: gp.cut(data) for market, data in datasets.items()},
        gate,
        loaded,
        root_dir,
    )


def check_targets(
    items: Sequence[Item],
    datasets: Mapping[str, BenchData],
    windows: Mapping[str, Window],
    gate: tr.TrialGate,
    loaded: LoadedSettings,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in items:
        try:
            keys = stage_windows(item, gate)
        except Exception as exc:
            rows.append({"config": item.id, "ok": False, "error": _error(exc)})
            continue
        for stage, _ in fp.STAGES:
            try:
                key = keys[stage]
                window = windows[key]
                data = datasets[WINDOW_MARKETS[key]]
                targets = strategy_targets(
                    tr.STRATEGY_FUNCTIONS[item.function], data.inputs, item.params
                )
                dates = pd.DatetimeIndex(targets.index)
                inside = targets.loc[
                    (dates >= pd.Timestamp(window.start))
                    & (True if window.end is None else dates <= pd.Timestamp(window.end))
                ]
                missing = int(inside.isna().sum())
                rows.append(
                    {
                        "config": item.id,
                        "stage": stage,
                        "window_key": key,
                        "window_days": len(inside),
                        "missing_days": missing,
                        "ok": len(inside) > 0 and missing == 0,
                    }
                )
            except Exception as exc:
                rows.append({"config": item.id, "stage": stage, "ok": False, "error": _error(exc)})
    return rows


def collect_sources(
    items: Sequence[Item],
    gate: tr.TrialGate,
    records: Sequence[Mapping[str, Any]],
    backfill: Mapping[str, Any],
) -> dict[str, Any]:
    sources: dict[str, Any] = {}
    for item in items:
        stages: dict[str, Any] = {}
        for stage, market in fp.STAGES:
            metrics = [r.metric for r in common_rules(gate, market)]
            if item.source == "backfill":
                stages[stage] = (
                    backfill_source(backfill, item.id, metrics) if stage == "stage1" else None
                )
            else:
                stages[stage] = trial_source(records, item.id, item.primary or "", market, metrics)
        sources[item.id] = stages
    return sources


def _load(
    root: Path,
) -> tuple[dict[str, BenchData], dict[str, BenchData], tr.TrialGate, LoadedSettings, Path]:
    try:
        return load_data(root)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        typer.echo(f"slow_bear_check: 数据载入失败：{exc}")
        raise typer.Exit(code=1) from None


def _print_segments(found: Mapping[str, Any]) -> None:
    for key, section in found.items():
        for s in section["segments"]:
            if s["kind"] == "long_bull":
                continue
            typer.echo(
                f"slow_bear_check: {key} {s['id']} {s['peak']}..{s['trough']} "
                f"{gp._pct(s['change'])} {s['months']} 个月 {KIND_TEXT[s['kind']]}"
            )
        for row in section["inferred"] or []:
            typer.echo(
                f"slow_bear_check: {key} 推断 {row['inferred']} 算出 {row['found']} "
                f"{MATCH_TEXT[row['match']]}"
            )


def _print_checks(rows: Sequence[Mapping[str, Any]]) -> bool:
    for row in rows:
        if "error" in row:
            typer.echo(f"slow_bear_check: {row['config']} 算不出：{row['error']}")
        else:
            typer.echo(
                f"slow_bear_check: {row['config']} {row['stage']} {row['window_key']} "
                f"窗口内 {row['window_days']} 天，缺 {row['missing_days']} 天，"
                f"{'可以' if row['ok'] else '不行'}"
            )
    return all(row["ok"] for row in rows)


@app.callback()
def main() -> None:
    """只量慢熊判据，不改试跑门。"""


@app.command("check")
def check_command(
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    datasets, _, gate, loaded, root_dir = _load(root)
    windows = probe_windows(loaded, gate)
    _print_segments(segment_section(segment_lists(datasets, windows), windows))
    if not _print_checks(check_targets(probe_items(root_dir), datasets, windows, gate, loaded)):
        raise typer.Exit(code=1)


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="结果目录。")],
    backfill_result: Annotated[Path, typer.Option(help="完整回填检验结果 JSON。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
    workers: Annotated[int, typer.Option(help="并行进程数。")] = 1,
    seeds: Annotated[int, typer.Option(help="每个零效果族的种子数。")] = fp.DEFAULT_SEEDS,
    control_seeds: Annotated[
        int, typer.Option(help="每个对照族的种子数。")
    ] = fp.DEFAULT_CONTROL_SEEDS,
) -> None:
    if any(out.joinpath(name).exists() for name in (JOURNAL_FILE, RESULT_JSON)):
        typer.echo("slow_bear_check: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        backfill = load_backfill(backfill_result)
    except (OSError, ValueError) as exc:
        typer.echo(f"slow_bear_check: 回填结果不能用：{exc}")
        raise typer.Exit(code=1) from None
    datasets, null, gate, loaded, root_dir = _load(root)
    windows = probe_windows(loaded, gate)
    items = probe_items(root_dir)
    if not _print_checks(check_targets(items, datasets, windows, gate, loaded)):
        typer.echo("slow_bear_check: 开跑前检查没过，停下。")
        raise typer.Exit(code=1)
    sources = collect_sources(items, gate, tr.trial_records(root_dir), backfill)
    out.mkdir(parents=True, exist_ok=True)
    result = run_probe(
        datasets,
        null,
        loaded,
        gate,
        root_dir,
        items=items,
        sources=sources,
        seeds=seeds,
        control_seeds=control_seeds,
        workers=workers,
        windows=windows,
        journal=out / JOURNAL_FILE,
        progress=lambda message: typer.echo(f"slow_bear_check: {message}"),
    )
    write_result(result, out)
    _print_segments(result["segments"])
    for row in result["configs"]:
        typer.echo(
            f"slow_bear_check: {row['config']} 两段 {VERDICT_TEXT[row['verdict']['standard']]}"
            f"（收紧 {VERDICT_TEXT[row['verdict']['tight']]}）"
        )
    readings = result["false_pass"]["readings"]
    for reading in READINGS:
        read = readings[reading.id]["standard" if reading.track == "slow_bear" else "gate"]
        typer.echo(
            f"slow_bear_check: 误放率 {reading.id} {read['passes']}/{read['total']}，"
            f"95% 上限 {gp._pct(read['upper'])}，{READ_TEXT[read['verdict']]}"
        )
    typer.echo(f"slow_bear_check: 适用 {readings['applies']}")
    typer.echo(f"slow_bear_check: {'完成' if result['complete'] else '有配置出错'}；结果目录 {out}")


if __name__ == "__main__":
    app()
