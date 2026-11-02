"""Seven QQQ exit rules counted on every-day entries (TRADING-2565-108).

Owner decision owner_decision:TRADING-2565:2026-11-02:exit_family_step0
approves step zero of the QQQ exit family. Use the QQQ 1999–2010 development
window, cut at 2010-12-31, treating every close as an entry. Print exit counts
and holding days only, not returns, drawdowns or price changes. Load only the
QQQ ruler, never another instrument. Decide at the day's close and sell at the
next close. This is neither a probe nor an experiment, makes no accounting
entries, and touches no daily report or broker.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from statistics import fmean, stdev
from typing import Annotated, Any, NamedTuple

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import load_settings
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.settings import LoadedSettings
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import policy_table as pt

RUN_ID = "TRADING-2565-108"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-02:exit_family_step0"
MARKET: tr.Market = "qqq"
MARKET_TEXT = pt.MARKET_TEXT[MARKET]
RULES = ("X0", "X1", "X2", "X3", "X4", "X5", "X6")
# Frozen print-only pilot under OWNER_DECISION; exit at the step-zero review.
# Any interpretation of outcomes or opening of research needs a separate decision.
STOP = 0.08
VOL_DAYS = 20
SMA_DAYS = 50
WEEKS = 52
WEEK_SIGMA = 2.0
SWING = 2
TIME_CHECK = 20
SHORT_HOLD = 60
LONG_HOLD = 120
CLUSTER_GAP = 20
CAP = {
    "X0": SHORT_HOLD,
    "X1": LONG_HOLD,
    "X2": LONG_HOLD,
    "X3": LONG_HOLD,
    "X4": LONG_HOLD,
    "X5": LONG_HOLD,
    "X6": SHORT_HOLD,
}
COUNT_KEYS = (
    "trades",
    "signal",
    "time",
    "hold_mean",
    "hold_median",
    "hold_p10",
    "hold_p90",
    "decided_day1",
    "decided_by_day5",
    "hold_mean_same_close",
    "h_match",
    "decision_days",
    "clusters",
)
CHECK_JSON = "exit_rules_counts.json"
# Step-zero input floor, not a research acceptance or promotion rule; same pilot exit.
MIN_TRADES = 2000
app = typer.Typer(help="QQQ 七条出场规则：只数次数和持有天数，不印收益，不进账本。")


class Prepared(NamedTuple):
    dates: tuple[date, ...]
    close: tuple[float, ...]
    sma: tuple[float | None, ...]
    sigma: tuple[float | None, ...]
    week: tuple[int, ...]
    drop: tuple[bool, ...]


class Exit(NamedTuple):
    decision: int | None
    sell: int
    reason: str


def moving_average(close: Sequence[float], days: int = SMA_DAYS) -> tuple[float | None, ...]:
    return tuple(
        None if p < days - 1 else math.fsum(close[p - days + 1 : p + 1]) / days
        for p in range(len(close))
    )


def daily_sigma(close: Sequence[float]) -> tuple[float | None, ...]:
    changes = [close[p] / close[p - 1] - 1.0 for p in range(1, len(close))]
    return tuple(
        None if p < VOL_DAYS else stdev(changes[p - VOL_DAYS : p]) for p in range(len(close))
    )


def week_ends(dates: Sequence[date]) -> tuple[bool, ...]:
    return tuple(
        p + 1 < len(dates) and day.isocalendar()[:2] != dates[p + 1].isocalendar()[:2]
        for p, day in enumerate(dates)
    )


def week_numbers(ends: Sequence[bool]) -> tuple[int, ...]:
    numbers: list[int] = []
    number = 0
    for end in ends:
        numbers.append(number)
        number += int(end)
    return tuple(numbers)


def weekly_drops(close: Sequence[float], ends: Sequence[bool]) -> tuple[bool, ...]:
    positions = [p for p, end in enumerate(ends) if end]
    changes = [close[b] / close[a] - 1.0 for a, b in zip(positions, positions[1:], strict=False)]
    flags = [False] * len(close)
    for j in range(WEEKS + 1, len(positions)):
        flags[positions[j]] = changes[j - 1] <= -WEEK_SIGMA * stdev(changes[j - WEEKS - 1 : j - 1])
    return tuple(flags)


def prepare(dates: Sequence[date], close: Sequence[float]) -> Prepared:
    ends = week_ends(dates)
    return Prepared(
        tuple(dates),
        tuple(close),
        moving_average(close),
        daily_sigma(close),
        week_numbers(ends),
        weekly_drops(close, ends),
    )


def exit_for(rule: str, data: Prepared, e: int, k: float) -> Exit:
    if rule not in CAP:
        raise ValueError(f"unknown rule {rule}")
    cap = CAP[rule]
    close = data.close
    if rule == "X6":
        d = e + TIME_CHECK
        if close[d] < close[e]:
            return Exit(d, d + 1, "signal")
        return Exit(None, e + cap, "time")

    sigma = data.sigma[e]
    distance = k * sigma if sigma is not None else math.nan
    high = close[e]
    break_day: int | None = None
    break_high = high
    low: float | None = None
    for d in range(e + 1, e + cap):
        high = max(high, close[d])
        signal = False
        if rule == "X0":
            signal = close[d] / close[e] <= 1.0 - STOP
        elif rule == "X1":
            signal = close[d] / close[e] <= 1.0 - distance
        elif rule == "X2":
            signal = close[d] / high <= 1.0 - distance
        elif rule == "X3":
            current, previous = data.sma[d], data.sma[d - 1]
            signal = (
                current is not None
                and previous is not None
                and close[d] < current
                and close[d - 1] < previous
            )
        elif rule == "X4":
            signal = data.drop[d] and data.week[d] > data.week[e]
        elif rule == "X5":
            if break_day is None:
                average = data.sma[d]
                if average is not None and close[d] < average:
                    break_day, break_high = d, high
                continue
            if close[d] >= break_high:
                break_day, low = None, None
                continue
            if low is None:
                s = d - SWING
                if s >= break_day + 1 and all(
                    close[s] > close[p] for p in range(s - SWING, s + SWING + 1) if p != s
                ):
                    low = min(close[break_day : s + 1])
            signal = low is not None and close[d] < low
        if signal:
            return Exit(d, d + 1, "signal")
    return Exit(None, e + cap, "time")


def entry_range(data: Prepared, first_in: int) -> range:
    n = len(data.close)
    start = next(
        (p for p in range(max(first_in, SMA_DAYS - 1, VOL_DAYS), n) if data.week[p] >= WEEKS),
        n,
    )
    return range(start, max(start, n - LONG_HOLD - 1))


def scale(data: Prepared, entries: Sequence[int]) -> float | None:
    sigmas = [s for e in entries if (s := data.sigma[e]) is not None]
    if not sigmas:
        return None
    mean = fmean(sigmas)
    if mean <= 0 or not math.isfinite(mean):
        raise ValueError("入场集平均波动必须为有限正数。")
    return STOP / mean


def nearest_rank(values: Sequence[int], share: float) -> int:
    return sorted(values)[max(math.ceil(share * len(values)) - 1, 0)]


def clusters(days: Sequence[int]) -> int:
    ordered = sorted(set(days))
    return int(bool(ordered)) + sum(
        b - a >= CLUSTER_GAP for a, b in zip(ordered, ordered[1:], strict=False)
    )


def rule_counts(exits: Mapping[int, Exit]) -> dict[str, int | float | None]:
    if not exits:
        return dict.fromkeys(COUNT_KEYS)
    holds = [x.sell - e for e, x in exits.items()]
    mean = fmean(holds)
    decisions = [x.decision for x in exits.values() if x.decision is not None]
    return {
        "trades": len(exits),
        "signal": sum(x.reason == "signal" for x in exits.values()),
        "time": sum(x.reason == "time" for x in exits.values()),
        "hold_mean": mean,
        "hold_median": nearest_rank(holds, 0.5),
        "hold_p10": nearest_rank(holds, 0.1),
        "hold_p90": nearest_rank(holds, 0.9),
        "decided_day1": sum(x.decision == e + 1 for e, x in exits.items()),
        "decided_by_day5": sum(
            x.decision is not None and x.decision <= e + 5 for e, x in exits.items()
        ),
        "hold_mean_same_close": fmean(
            (x.decision if x.decision is not None else x.sell) - e for e, x in exits.items()
        ),
        "h_match": math.floor(mean + 0.5),
        "decision_days": len(set(decisions)),
        "clusters": clusters(decisions),
    }


def check_inputs(data: BenchData, loaded: LoadedSettings, gate: tr.TrialGate) -> dict[str, Any]:
    window = pt.market_window(loaded, gate, MARKET)
    if window.end is None:
        raise ValueError("开发窗口必须有结束日期。")
    series = ec.closes(data.returns["qqq"], window.end)
    first_in = next(
        (p for p, day in enumerate(series.dates) if day >= window.start), len(series.dates)
    )
    prepared = prepare(series.dates, series.close)
    entries = entry_range(prepared, first_in)
    k = scale(prepared, entries)
    exits = {
        rule: {e: exit_for(rule, prepared, e, k if k is not None else math.nan) for e in entries}
        for rule in RULES
    }
    distances = [k * s for e in entries if k is not None and (s := prepared.sigma[e]) is not None]
    entry_set_b: list[dict[str, Any]] = []
    for touch in ec.scan(series.close, first_in):
        if not touch.counted or touch.entry is None:
            continue
        e = touch.entry
        details = None
        if e in entries:
            details = {}
            for rule in RULES:
                x = exits[rule][e]
                details[rule] = {
                    "decision": series.dates[x.decision].isoformat()
                    if x.decision is not None
                    else None,
                    "sell": series.dates[x.sell].isoformat(),
                    "hold": x.sell - e,
                    "reason": x.reason,
                }
        entry_set_b.append(
            {"entry": series.dates[e].isoformat(), "in_range": e in entries, "exits": details}
        )
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "market": MARKET,
        "window": {
            "window_id": window.window_id,
            "start": window.start.isoformat(),
            "end": window.end.isoformat(),
        },
        "first_close_date": series.dates[0].isoformat() if series.dates else None,
        "missing_days": series.missing,
        "sessions": sum(window.start <= day <= window.end for day in series.dates),
        "rule": {
            "stop": STOP,
            "vol_days": VOL_DAYS,
            "sma_days": SMA_DAYS,
            "weeks": WEEKS,
            "week_sigma": WEEK_SIGMA,
            "swing": SWING,
            "time_check": TIME_CHECK,
            "short_hold": SHORT_HOLD,
            "long_hold": LONG_HOLD,
            "cluster_gap": CLUSTER_GAP,
            "sell": "next_close",
        },
        "entry_set_a": {
            "first": series.dates[entries[0]].isoformat() if entries else None,
            "last": series.dates[entries[-1]].isoformat() if entries else None,
            "trades": len(entries),
        },
        "scale": {
            "k": k,
            "distance_mean": fmean(distances) if distances else None,
            "distance_min": min(distances) if distances else None,
            "distance_max": max(distances) if distances else None,
        },
        "exits": {rule: rule_counts(exits[rule]) for rule in RULES},
        "entry_set_b": entry_set_b,
        "ok": series.missing == 0 and len(entries) >= MIN_TRADES,
    }


def summary_line(check: Mapping[str, Any]) -> str:
    entries, exits = check["entry_set_a"], check["exits"]
    holds = "、".join(
        f"{rule} {exits[rule]['hold_mean']:.1f}"
        if exits[rule]["hold_mean"] is not None
        else f"{rule} —"
        for rule in RULES
    )
    groups = "、".join(f"{rule} {exits[rule]['clusters']}" for rule in RULES)
    return (
        f"exit_rules_counts: {MARKET_TEXT} 入场 {entries['trades']} 笔"
        f"（{entries['first']}..{entries['last']}）；平均持有 {holds} 天；"
        f"判断日簇 {groups}；输入检查 {'PASS' if check['ok'] else 'FAIL'}"
    )


def load_data(snapshot_root: Path) -> tuple[BenchData, tr.TrialGate, LoadedSettings]:
    loaded = load_settings()
    gate, plan = tr.load_trial(lg.research_root(loaded), pt.DATA_TRIAL)
    problems = tr.gate_problems(gate, loaded)
    if problems:
        raise ValueError("; ".join(problems))
    data = tr.market_data(
        MARKET, gate, plan, snapshot_root=snapshot_root, project_root=PROJECT_ROOT
    )
    problems = tr._data_problems(MARKET, gate, plan, data)
    if problems:
        raise ValueError("qqq: " + "; ".join(problems))
    return gp.cut(data), gate, loaded


def write_check(check: Mapping[str, Any], out: Path) -> None:
    text = json.dumps(check, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(CHECK_JSON).write_text(text, encoding="utf-8")


@app.callback()
def main() -> None:
    """Count exits in the QQQ development window, without investment returns."""


@app.command("check")
def check_command(
    out: Annotated[Path, typer.Option(help="次数结果目录，已有结果拒绝覆盖。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if out.joinpath(CHECK_JSON).exists():
        typer.echo("exit_rules_counts: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        data, gate, loaded = load_data(root)
        check = check_inputs(data, loaded, gate)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        lines = str(exc).splitlines()
        message = lines[0] if lines else ""
        typer.echo(f"exit_rules_counts: 数据载入失败：{message}")
        raise typer.Exit(code=1) from None
    write_check(check, out)
    typer.echo(summary_line(check))
    if not check["ok"]:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
