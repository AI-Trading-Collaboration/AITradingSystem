"""The structure-shift strategy on QQQ's other periods (TRADING-2565-122).

Under owner_decision:TRADING-2565:2026-11-03:structure_shift_other_periods,
print the frozen TRADING-2565-120 rules on the Nasdaq-100 price index in the
registered ndx_price_1986_1999 window and QQQ from 2011-01-03 to the snapshot's
last close. The latter window is a one-time period-7 charter exception.
Also print Baker's below-200-day-line exit controls and X2 line catch-up
buy-back, including on 1999–2010 after reproducing the original table.
Read only QQQ and its own index. Print only: no verdicts or accounting entries,
no gate or ruler changes, and no daily report or broker action.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Annotated, Any, NamedTuple

import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.paths import join_root
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.settings import LoadedSettings, load_settings
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import entry_exit_combo as cb
from ai_trading_system.research_probes import entry_structure_shift as es
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import entry_timing_ndx as en
from ai_trading_system.research_probes import exit_rules_after as xa
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_ndx as xn
from ai_trading_system.research_probes import exit_rules_returns as xr
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import policy_table as pt
from ai_trading_system.yaml_loader import safe_load_yaml_path

RUN_ID = "TRADING-2565-122"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-03:structure_shift_other_periods"
SPANS = ("dev", "ndx", "qqq")
QQQ_WINDOW_ID = "qqq_after_2010_owner_print"
QQQ_WINDOW_ROLE = "owner_print_only"
QQQ_READ_POLICY = "once_per_owner_decision"
QQQ_START = date(2011, 1, 3)
QQQ_END = "latest"
QQQ_SNAPSHOT_ID = en.SNAPSHOT_ID
# Frozen input checks under OWNER_DECISION, not investment thresholds.
# Review these snapshot coverage checks when the authorized print is reviewed.
QQQ_LAST_MIN = date(2026, 9, 30)
QQQ_SESSIONS = (3900, 4050)
MAX_MOVE = en.MAX_MOVE
MAX_GAP_DAYS = 5
# Entry dates printed by TRADING-2565-107, used only for the input check.
NDX_V0 = ("1988-09-07", "1988-10-17", "1998-01-29", "1998-09-03", "1998-10-16")
# The 1999–2010 table printed by TRADING-2565-120, used only for reproduction.
DEV_SIGNALS = {"k5": 20, "k2": 42}
DEV_FIRST_DAY = "1999-12-23"
DEV_TOTALS = {
    "x2_s5": 0.761935,
    "x3_s5": 0.732322,
    "x2x3_s5": 0.358581,
    "x2_s2": -0.157292,
    "x3_s2": -0.018409,
    "x2x3_s2": -0.383552,
    "x2_a": -0.087305,
    "x3_a": 0.217754,
    "x2x3_a": -0.038947,
    "hold": -0.370499,
}
UNDER = tuple(f"{x}_s5_under" for x in cb.EXITS)
LINE = ("x2_s5_line",)
STRATEGIES = (*es.STRATEGIES, *UNDER, *LINE)
STRATEGY_TEXT = {
    **es.STRATEGY_TEXT,
    **{
        f"{x}_s5_under": f"{cb.EXIT_TEXT[x]}（只在 200 日线下算），A 加结构转变买回（左右各 5 天）"
        for x in cb.EXITS
    },
    "x2_s5_line": "X2 回落 8%，A 加结构转变买回，线上响的再等 200 日线涨到响时收盘价"
    "（左右各 5 天）",
}
HOW_TEXT = {**es.HOW_TEXT, "line": "200 日线涨到响时收盘价"}
TRIPS_SHOWN = (*(f"{x}_s5" for x in cb.EXITS), *UNDER, *LINE)
YEARS_SHOWN = (*(f"{x}_s5" for x in cb.EXITS), *UNDER, *LINE, *es.PLAIN, "hold")
SPAN_TEXT = {
    "dev": "QQQ 1999–2010（开发窗口，切到 2010-12-31，就是 TRADING-2565-120 那段）",
    "ndx": en.TITLE,
    "qqq": "QQQ 2011-01-03 以后（到快照的最后一个收盘）",
}
SPAN_WINDOW = {
    "dev": "qqq_long_history_development",
    "ndx": en.WINDOW_ID,
    "qqq": QQQ_WINDOW_ID,
}
CHECK_JSON = "structure_shift_periods_check.json"
RESULT_JSON = "structure_shift_periods.json"
RESULT_MD = "structure_shift_periods.md"
RULE_TEXT = (
    "只印不判，不检验、不下结论，不是交易建议。规则照 TRADING-2565-120 写死，"
    "印纳斯达克 100 价格指数 1986–1999 和 QQQ 2011-01-03 以后，"
    "QQQ 1999–2010 先重算核对。满仓起点以前的收盘只作回看，跌破只从满仓起点算。"
    + es.RULE_TEXT.partition("已看过结果。")[2]
)
NOTE = (
    cb.NOTE.partition("对入场和出场都是看过结果的历史。")[2]
    + es.NOTE[len(cb.NOTE) :]
    + "另印两种对照：X2、X3 和 X3 站上 50 日线以前的 8% 止损，"
    "只在那个收盘低于 200 日线时才算响；结构转变买回后的 L 止损不变，主表还是原规则。"
    "另一行 X2 照原规则，响时收盘在 200 日线上方，卖出后，"
    "200 日线第一次涨到（大于或等于）X2 响那个收盘的收盘，满仓买回；"
    "v0_close 入场、结构转变和等线谁先到用谁，同一个收盘依次算 v0_close、结构转变。"
    "这种买回不加 L 止损。离 200 日线 = 出场响那个收盘 ÷ 当天的 200 日线 − 1。"
)
app = typer.Typer(help="结构转变入场和策略放到 QQQ 的别的时期：只印不判。")


class QqqInputs(NamedTuple):
    """Development and uncut closes with their scan origins and joint input check."""

    dev: ec.Closes
    dev_first_in: int
    full: ec.Closes
    first_in: int
    check: dict[str, Any]


class NdxInputs(NamedTuple):
    """Index closes and the original input check plus pinned entry dates."""

    series: ec.Closes
    check: dict[str, Any]


def registered_qqq_window(loaded: LoadedSettings) -> dict[str, Any]:
    """Refuse a registry that does not describe the authorized one-time read."""
    raw = safe_load_yaml_path(join_root(loaded.root, loaded.settings.window_registry))
    windows = raw.get("windows") if isinstance(raw, Mapping) else None
    entry = windows.get(QQQ_WINDOW_ID) if isinstance(windows, Mapping) else None
    if not isinstance(entry, Mapping):
        raise ValueError("窗口登记表里没有 qqq_after_2010_owner_print。")
    found = (
        str(entry.get("start")),
        str(entry.get("end")),
        entry.get("role"),
        entry.get("read_policy"),
    )
    if found != (QQQ_START.isoformat(), QQQ_END, QQQ_WINDOW_ROLE, QQQ_READ_POLICY):
        raise ValueError(f"窗口 qqq_after_2010_owner_print 的登记和模块不一致：{found}")
    return {
        "window_id": QQQ_WINDOW_ID,
        "role": QQQ_WINDOW_ROLE,
        "read_policy": QQQ_READ_POLICY,
        "start": QQQ_START.isoformat(),
        "end": QQQ_END,
    }


def load_qqq(
    snapshot_root: Path,
) -> tuple[BenchData, tr.TrialGate, LoadedSettings, dict[str, Any]]:
    """Use the existing checked snapshot path without cutting off later closes."""
    loaded = load_settings()
    window = registered_qqq_window(loaded)
    gate, plan = tr.load_trial(lg.research_root(loaded), pt.DATA_TRIAL)
    problems = tr.gate_problems(gate, loaded)
    if problems:
        raise ValueError("; ".join(problems))
    data = tr.market_data(
        xc.MARKET, gate, plan, snapshot_root=snapshot_root, project_root=PROJECT_ROOT
    )
    problems = tr._data_problems(xc.MARKET, gate, plan, data)
    if problems:
        raise ValueError("qqq: " + "; ".join(problems))
    return data, gate, loaded, window


def v0_entries(close: Sequence[float], first_in: int) -> list[int]:
    """Select counted closing-price entries from the existing scanner."""
    return [t.entry for t in ec.scan(close, first_in) if t.counted and t.entry is not None]


def largest_move(series: ec.Closes, start: int) -> dict[str, Any] | None:
    """Keep the first largest absolute adjacent-close return, including its sign."""
    positions = range(max(start, 1), len(series.close))
    p = max(positions, key=lambda p: abs(series.close[p] / series.close[p - 1] - 1.0), default=None)
    if p is None:
        return None
    return {
        "date": series.dates[p].isoformat(),
        "return": series.close[p] / series.close[p - 1] - 1.0,
    }


def largest_gap(series: ec.Closes, start: int) -> int | None:
    """Measure calendar gaps including the close preceding the requested start."""
    return max(
        (
            (series.dates[p] - series.dates[p - 1]).days
            for p in range(max(start, 1), len(series.dates))
        ),
        default=None,
    )


def missing_dates(returns: pd.Series) -> list[str]:
    """Ignore leading unavailable returns but list every nonfinite value thereafter."""
    found: list[str] = []
    started = False
    for day, value in zip(pd.DatetimeIndex(returns.index), returns, strict=True):
        if math.isfinite(float(value)):
            started = True
        elif started:
            found.append(day.date().isoformat())
    return found


def qqq_inputs(
    data: BenchData, loaded: LoadedSettings, gate: tr.TrialGate, window: Mapping[str, Any]
) -> QqqInputs:
    """Check development reproduction inputs and the uncut post-2010 coverage."""
    dev, _, _, _, step0 = xa.qqq_inputs(gp.cut(data), loaded, gate)
    start = date.fromisoformat(step0["start"])
    dev_first_in = next((p for p, day in enumerate(dev.dates) if day >= start), len(dev.dates))
    dev_found = tuple(dev.dates[e].isoformat() for e in v0_entries(dev.close, dev_first_in))
    dev_s0 = max(dev_first_in, ec.SMA_DAYS)
    full = ec.closes(data.returns["qqq"], date.max)
    n = len(full.dates)
    first_in = next((p for p, day in enumerate(full.dates) if day >= QQQ_START), n)
    kept = sum(day <= gp.CUTOFF for day in full.dates)
    same = full.dates[:kept] == dev.dates and full.close[:kept] == dev.close
    move = largest_move(full, first_in)
    gap = largest_gap(full, first_in)
    increasing = all(full.dates[p] > full.dates[p - 1] for p in range(1, n))
    positive = all(value > 0 for value in full.close)
    sessions = n - first_in
    snapshot_id = data.source.get("snapshot_id")
    check: dict[str, Any] = {
        **dict(window),
        "snapshot_id": snapshot_id,
        "step0": step0,
        "dev_entries": list(dev_found),
        "dev_expected": list(cb.V0_ENTRIES),
        "dev_first_scan_date": dev.dates[dev_s0].isoformat() if dev_s0 < len(dev.dates) else None,
        "same_before_cut": same,
        "cut_closes": kept,
        "missing_days": full.missing,
        "missing_dates": missing_dates(data.returns["qqq"]),
        "dates_increasing": increasing,
        "closes_positive": positive,
        "first_close_date": full.dates[first_in].isoformat() if first_in < n else None,
        "last_close_date": full.dates[-1].isoformat() if n else None,
        "sessions": sessions,
        "largest_move": move,
        "largest_gap_days": gap,
        "entries": [full.dates[e].isoformat() for e in v0_entries(full.close, first_in)],
    }
    check["ok"] = bool(
        step0["ok"]
        and dev_found == cb.V0_ENTRIES
        and dev_s0 + cb.TAIL_DAYS < len(dev.dates)
        and same
        and kept == len(dev.dates)
        and snapshot_id == QQQ_SNAPSHOT_ID
        and first_in < n
        and full.dates[first_in] == QQQ_START
        and full.missing == 0
        and increasing
        and positive
        and move is not None
        and abs(move["return"]) <= MAX_MOVE
        and gap is not None
        and gap <= MAX_GAP_DAYS
        and full.dates[-1] >= QQQ_LAST_MIN
        and QQQ_SESSIONS[0] <= sessions <= QQQ_SESSIONS[1]
    )
    return QqqInputs(dev, dev_first_in, full, first_in, check)


def ndx_inputs(levels: Sequence[tuple[date, float]], window: Mapping[str, Any]) -> NdxInputs:
    """Reuse the index checks and additionally reproduce its printed v0 entries."""
    series, flat = en.prices(levels)
    base = xn.check_inputs(
        series, flat, window, xc.entry_range(xc.prepare(series.dates, series.close), 0)
    )
    entries = tuple(series.dates[e].isoformat() for e in v0_entries(series.close, 0))
    n = len(series.dates)
    check = {
        "inputs": base,
        "first_scan_date": series.dates[ec.SMA_DAYS].isoformat() if ec.SMA_DAYS < n else None,
        "entries": list(entries),
        "expected": list(NDX_V0),
        "ok": bool(base["ok"] and entries == NDX_V0 and ec.SMA_DAYS + cb.TAIL_DAYS < n),
    }
    return NdxInputs(series, check)


Fire = Callable[[str, xc.Prepared, int], tuple[int, str] | None]


def exit_under(
    rule: str, data: xc.Prepared, line: Sequence[float | None], b: int
) -> tuple[int, str] | None:
    """Track the original exit state but decide only on closes strictly below the long line."""
    if rule not in cb.EXITS:
        raise ValueError(f"unknown exit {rule}")
    close, sma = data.close, data.sma
    high = close[b]
    first = sma[b]
    armed = first is not None and close[b] >= first
    for d in range(b + 1, len(close)):
        high = max(high, close[d])
        current, previous = sma[d], sma[d - 1]
        if current is not None and close[d] >= current:
            armed = True
        long = line[d]
        if long is None or close[d] >= long:
            continue
        if rule in ("x2", "x2x3") and close[d] / high <= 1.0 - cb.TRAIL:
            return d, "X2"
        if rule in ("x3", "x2x3"):
            if (
                armed
                and current is not None
                and previous is not None
                and close[d] < current
                and close[d - 1] < previous
            ):
                return d, "X3"
            if not armed and close[d] / close[b] <= 1.0 - cb.STOP:
                return d, "stop"
    return None


def walk(
    rule: str,
    data: xc.Prepared,
    entries: Sequence[int],
    moves: Sequence[es.Shift],
    s0: int,
    fire: Fire,
    catch_up: Sequence[float | None] | None = None,
) -> cb.Route:
    """Keep the original shift path, with injectable exits and an optional line buy-back."""
    close = data.close
    n = len(close)
    held = [False] * n
    buys, sells = [s0], []
    trips: list[dict[str, Any]] = []
    b = s0
    floor: float | None = None
    while b < n - 1:
        fired = fire(rule, data, b)
        if floor is not None:
            under = next((d for d in range(b + 1, n) if close[d] < floor), None)
            if under is not None and (fired is None or under < fired[0]):
                fired = (under, "L")
        if fired is None or fired[0] + 1 > n - 1:
            break
        d, reason = fired
        v = d + 1
        held[b + 1 : v + 1] = [True] * (v - b)
        sells.append(v)
        entry = next((e for e in entries if e > v), None)
        move = next((m for m in moves if m.entry > v), None)
        reach: int | None = None
        if catch_up is not None and reason == "X2":
            at = catch_up[d]
            if at is not None and close[d] > at:
                reach = next(
                    (
                        p
                        for p in range(v + 1, n)
                        if (now := catch_up[p]) is not None and now >= close[d]
                    ),
                    None,
                )
        r: int | None = None
        if entry is not None and (move is None or entry <= move.entry):
            r, how, floor = entry, "entry", None
        elif move is not None:
            r, how, floor = move.entry, "shift", close[move.low]
        else:
            how = "none"
        if reach is not None and (r is None or reach < r):
            r, how, floor = reach, "line", None
        last = r if r is not None else n - 1
        trips.append(
            {
                "decision": data.dates[d].isoformat(),
                "reason": reason,
                "sell": data.dates[v].isoformat(),
                "buy": data.dates[r].isoformat() if r is not None else None,
                "how": how,
                "days_out": last - v,
                "change": close[last] / close[v] - 1.0,
            }
        )
        if r is None:
            return cb.Route(tuple(held), tuple(buys), tuple(sells), tuple(trips))
        buys.append(r)
        b = r
    held[b + 1 :] = [True] * (n - 1 - b)
    return cb.Route(tuple(held), tuple(buys), tuple(sells), tuple(trips))


def span_result(series: ec.Closes, first_in: int) -> dict[str, Any]:
    """Delegate the original tables and append the four authorized control paths."""
    dates, close = series.dates, series.close
    entries = v0_entries(close, first_in)
    s0 = max(first_in, ec.SMA_DAYS)
    prepared = xc.prepare(dates, close)
    line = ec.moving_average(close)
    moves = {k: es.shifts(close, line, k, s0) for k in es.SWINGS}
    signals: dict[str, Any] = {}
    for k in es.SWINGS:
        rows = es.signal_rows(dates, close, line, moves[k], s0)
        signals[f"k{k}"] = {
            "rows": rows,
            "summary": {
                f"h{h}": es.horizon_summary(rows, h, es.any_day(dates, close, s0, h))
                for h in es.HORIZONS
            },
        }
    span = es.span_table(series, prepared, entries, moves, s0)

    def fire_under(rule: str, data: xc.Prepared, b: int) -> tuple[int, str] | None:
        return exit_under(rule, data, line, b)

    for strategy in (*UNDER, *LINE):
        rule = strategy.partition("_s5")[0]
        path = walk(
            rule,
            prepared,
            entries,
            moves[5],
            s0,
            fire_under if strategy in UNDER else cb.exit_day,
            catch_up=line if strategy in LINE else None,
        )
        values = cb.equity(close, path, s0)
        span["strategies"][strategy] = {
            **cb.metrics(dates, values, path, s0),
            "by_shift": sum(t["how"] == "shift" for t in path.trips),
            "low_stops": sum(t["reason"] == "L" for t in path.trips),
            "trips": list(path.trips),
        }
        annual = cb.year_returns(dates, values, s0)
        for year in span["years"]:
            year[strategy] = annual[year["year"]]
    positions = {day.isoformat(): p for p, day in enumerate(dates)}
    for row in span["strategies"].values():
        row["by_line"] = sum(t["how"] == "line" for t in row["trips"])
        for trip in row["trips"]:
            d = positions[trip["decision"]]
            at = line[d]
            if at is None:
                raise ValueError("出场响的收盘缺 200 日线。")
            trip["vs_line"] = close[d] / at - 1.0
    return {
        "lookback_from": dates[0].isoformat(),
        "scan_from": dates[first_in].isoformat(),
        "signals": signals,
        "span": span,
    }


def dev_check(dev: Mapping[str, Any]) -> dict[str, Any]:
    """Compare only the printed development counts, start, and six-place totals."""
    signals = {f"k{k}": len(dev["signals"][f"k{k}"]["rows"]) for k in es.SWINGS}
    first = dev["span"]["first_day"]
    totals = {s: round(dev["span"]["strategies"][s]["total"], 6) for s in es.STRATEGIES}
    return {
        "signals": signals,
        "first_day": first,
        "totals": totals,
        "ok": signals == DEV_SIGNALS and first == DEV_FIRST_DAY and totals == DEV_TOTALS,
    }


def rule_block() -> dict[str, Any]:
    """Preserve the original rule description and separately identify the new controls."""
    return {
        "swings": list(es.SWINGS),
        "swing": "close_strictly_beyond_k_closes_each_side_known_k_closes_later",
        "break": "close_below_sma200_after_close_at_or_above",
        "low": "swing_low_below_sma200_since_break_lower_than_current_low_and_last_entry_low",
        "high": "latest_swing_high_before_low",
        "shift_entry": "first_close_above_high_close",
        "shift_stop": "close_below_low_close_sell_next_close",
        "horizons": list(es.HORIZONS),
        "long_sma_days": ec.SMA_DAYS,
        "strategies": list(es.STRATEGIES),
        "entry": ec.VARIANT,
        "start": "full",
        "trail": cb.TRAIL,
        "stop": cb.STOP,
        "sma_days": xc.SMA_DAYS,
        "sell": "next_close",
        "buy_back_a": "entry_close",
        "buy_back_shift": "first_of_entry_close_or_shift_entry_close_tie_is_entry",
        "flat_return": 0.0,
        "cost": cb.COST,
        "tail_days": cb.TAIL_DAYS,
        "tail": cb.TAIL,
        "sessions_per_year": cb.SESSIONS_PER_YEAR,
        "spans": list(SPANS),
        "under": {
            "strategies": list(UNDER),
            "counts": "exit_decision_only_on_a_close_below_sma200",
            "exits": ["X2", "X3", "stop"],
            "low_stop": "unchanged",
        },
        "line": {
            "strategies": list(LINE),
            "buy_back": "first_close_where_sma200_reaches_the_x2_decision_close",
            "when": "x2_decision_close_above_sma200",
            "order": ["entry", "shift", "line"],
            "low_stop": "none_after_line_buy_back",
        },
    }


def check_table(
    data: BenchData,
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    window: Mapping[str, Any],
    levels: Sequence[tuple[date, float]],
    source: Mapping[str, Any],
    ndx_window: Mapping[str, Any],
) -> dict[str, Any]:
    """Print input checks only, without signal outcomes or strategy returns."""
    q = qqq_inputs(data, loaded, gate, window)
    ndx = ndx_inputs(levels, ndx_window)
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "source": {"qqq": {"snapshot_id": data.source.get("snapshot_id")}, "ndx": dict(source)},
        "rule": rule_block(),
        "check": {"qqq": q.check, "ndx": ndx.check},
        "ok": q.check["ok"] and ndx.check["ok"],
    }


def run_table(
    data: BenchData,
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    window: Mapping[str, Any],
    levels: Sequence[tuple[date, float]],
    source: Mapping[str, Any],
    ndx_window: Mapping[str, Any],
) -> dict[str, Any]:
    """Stop before either new period unless inputs and development reproduction pass."""
    q = qqq_inputs(data, loaded, gate, window)
    ndx = ndx_inputs(levels, ndx_window)
    result: dict[str, Any] = {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "source": {"qqq": {"snapshot_id": data.source.get("snapshot_id")}, "ndx": dict(source)},
        "rule": rule_block(),
        "check": {"qqq": q.check, "ndx": ndx.check, "dev": None},
        "complete": False,
    }
    if not q.check["ok"] or not ndx.check["ok"]:
        return result
    dev = span_result(q.dev, q.dev_first_in)
    result["check"]["dev"] = dev_check(dev)
    if not result["check"]["dev"]["ok"]:
        return result
    result["spans"] = {
        "dev": dev,
        "ndx": span_result(ndx.series, 0),
        "qqq": span_result(q.full, q.first_in),
    }
    result["complete"] = True
    return result


def _line(cells: Sequence[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def render_span(name: str, block: Mapping[str, Any]) -> list[str]:
    """Render one period's signals, controls, annual returns, and sale details."""
    span = block["span"]
    lines = [
        f"## {SPAN_TEXT[name]}",
        "",
        f"窗口 `{SPAN_WINDOW[name]}`；回看从 {block['lookback_from']}，v0_close 从 "
        f"{block['scan_from']} 起找；满仓起点 {span['first_day']}，到 {span['last_day']}，"
        f"共 {span['sessions']} 个交易日。v0_close 入场 {len(span['entries'])} 次："
        f"{'、'.join(span['entries']) or '无'}。",
        "",
    ]
    signal_headers = [
        "跌破",
        "低点 L",
        "L 在 200 日线下",
        "摆动高点 H",
        "入场",
        "入场离 200 日线",
        "风险（离 L）",
        "20 天",
        "20 天带止损",
        "当年任一天 20 天",
        "60 天",
        "60 天带止损",
        "当年任一天 60 天",
    ]
    summary_headers = [
        "天数",
        "入场",
        "算得出",
        "平均",
        "带止损平均",
        "当年任一天平均",
        "涨",
        "带止损涨",
        "高于当年任一天",
        "带止损高于当年任一天",
        "止损",
        "整段任一天平均",
        "整段任一天上涨占比",
    ]
    for k in es.SWINGS:
        signals = block["signals"][f"k{k}"]
        lines += [
            f"### 左右各 {k} 天：入场 {len(signals['rows'])} 次",
            "",
            _line(signal_headers),
            "|" + "---|" * len(signal_headers),
        ]
        for row in signals["rows"]:
            cells = [
                row["cross"],
                row["low"],
                xr.pct(row["low_below_line"]),
                row["high"],
                row["entry"],
                xr.pct(row["entry_vs_line"]),
                xr.pct(row["risk"]),
            ]
            for h in es.HORIZONS:
                cell = row[f"h{h}"]
                cells += (
                    [xr.pct(cell["plain"]), es.stop_text(cell), xr.pct(cell["year_mean"])]
                    if cell is not None
                    else ["—", "—", "—"]
                )
            lines.append(_line(cells))
        lines += ["", _line(summary_headers), "|" + "---|" * len(summary_headers)]
        for h in es.HORIZONS:
            summary = signals["summary"][f"h{h}"]
            lines.append(
                _line(
                    [
                        f"{h} 天",
                        str(summary["signals"]),
                        str(summary["settled"]),
                        xr.pct(summary["plain_mean"]),
                        xr.pct(summary["stop_mean"]),
                        xr.pct(summary["year_mean"]),
                        str(summary["plain_up"]),
                        str(summary["stop_up"]),
                        str(summary["plain_beats_year"]),
                        str(summary["stop_beats_year"]),
                        str(summary["stopped"]),
                        xr.pct(summary["any_day_mean"]),
                        cb.share_text(summary["any_day_up_share"]),
                    ]
                )
            )
        lines.append("")
    headers = [
        "策略",
        "总收益",
        "年化",
        "最大回撤",
        "回撤高点 → 低点",
        "最差 5% 的 20 日收益",
        "在场时间",
        "在场时年化",
        "卖出次数",
        "靠入场 / 结构转变 / 200 日线 / 没买回",
        "跌回 L 下卖出",
        "买回更便宜",
        "平均买回差价",
        "平均空仓天数",
    ]
    lines += ["### 策略", "", _line(headers), "|" + "---|" * len(headers)]
    for strategy in STRATEGIES:
        row = span["strategies"][strategy]
        lines.append(
            _line(
                [
                    STRATEGY_TEXT[strategy],
                    xr.pct(row["total"]),
                    xr.pct(row["cagr"]),
                    xr.pct(row["max_drawdown"]),
                    f"{row['drawdown_peak']} → {row['drawdown_trough']}",
                    xr.pct(row["worst20"]),
                    cb.share_text(row["in_market"]),
                    xr.pct(row["in_market_cagr"]),
                    str(row["sells"]),
                    f"{row['by_entry']} / {row['by_shift']} / {row['by_line']} / {row['not_back']}",
                    str(row["low_stops"]),
                    str(row["cheaper"]),
                    xr.pct(row["change_mean"]),
                    cb.days_text(row["days_out_mean"]),
                ]
            )
        )
    year_headers = ["年", *(STRATEGY_TEXT[s] for s in YEARS_SHOWN)]
    lines += [
        "",
        "每年（从上一年最后一个收盘算，第一年从满仓起点算；左右各 2 天的每年数见 JSON）：",
        "",
        _line(year_headers),
        "|" + "---|" * len(year_headers),
    ]
    for row in span["years"]:
        lines.append(_line([str(row["year"]), *(xr.pct(row[s]) for s in YEARS_SHOWN)]))
    trip_headers = [
        "策略",
        "出场响",
        "原因",
        "离 200 日线",
        "卖出",
        "买回",
        "怎么买回",
        "空仓天数",
        "买回差价",
    ]
    lines += [
        "",
        "加结构转变买回及两种对照的每次卖出和买回（原因 L = 收盘跌回 L 下；"
        "没买回的差价算到最后一个收盘；左右各 2 天的见 JSON）：",
        "",
        _line(trip_headers),
        "|" + "---|" * len(trip_headers),
    ]
    for strategy in TRIPS_SHOWN:
        for trip in span["strategies"][strategy]["trips"]:
            lines.append(
                _line(
                    [
                        STRATEGY_TEXT[strategy],
                        trip["decision"],
                        trip["reason"],
                        xr.pct(trip["vs_line"]),
                        trip["sell"],
                        trip["buy"] if trip["buy"] is not None else "—",
                        HOW_TEXT[trip["how"]],
                        str(trip["days_out"]),
                        xr.pct(trip["change"]),
                    ]
                )
            )
    return lines


def render_markdown(result: Mapping[str, Any]) -> str:
    """Print all periods with reproduction first and without investment conclusions."""
    lines = [
        "# 结构转变入场和策略放到 QQQ 的别的时期（TRADING-2565-122）",
        "",
        RULE_TEXT,
        "",
        NOTE,
        "",
        "先核对 1999–2010：入场次数、满仓起点和十行总收益都等于 TRADING-2565-120，才算两段新数据。",
        "",
    ]
    for name in SPANS:
        lines += render_span(name, result["spans"][name]) + [""]
    return "\n".join(lines).rstrip() + "\n"


def summary_line(result: Mapping[str, Any], out: Path) -> str:
    """Summarize counts and totals, putting the two newly printed periods first."""
    parts = []
    for name in ("ndx", "qqq", "dev"):
        block = result["spans"][name]
        rows = block["span"]["strategies"]
        shown = "、".join(f"{s} {xr.pct(rows[s]['total'])}" for s in (*TRIPS_SHOWN, "hold"))
        parts.append(f"{name} 左右各 5 天 {len(block['signals']['k5']['rows'])} 次，总收益 {shown}")
    return f"structure_shift_periods: {'；'.join(parts)}；结果目录 {out}"


def check_line(check: Mapping[str, Any], out: Path) -> str:
    """Report both checks with their coverage and entry counts."""
    q, n = check["check"]["qqq"], check["check"]["ndx"]
    return (
        f"structure_shift_periods check: ndx {'ok' if n['ok'] else 'FAIL'}，"
        f"{n['inputs']['closes']} 个收盘，v0_close {len(n['entries'])} 次；"
        f"qqq {'ok' if q['ok'] else 'FAIL'}，2011-01-03..{q['last_close_date']}，"
        f"{q['sessions']} 个交易日，缺 {q['missing_days']} 天，"
        f"最大间隔 {q['largest_gap_days']} 天，"
        f"v0_close {len(q['entries'])} 次；结果目录 {out}"
    )


def write_check(check: Mapping[str, Any], out: Path) -> None:
    """Write only the finite, UTF-8 input-check artifact."""
    text = json.dumps(check, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(CHECK_JSON).write_text(text, encoding="utf-8")


def write_result(result: Mapping[str, Any], out: Path) -> None:
    """Retain failed checks as JSON, rendering tables only after completion."""
    text = json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(RESULT_JSON).write_text(text, encoding="utf-8")
    if result["complete"]:
        out.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


@app.callback()
def main() -> None:
    """Print the frozen structure-shift strategy in the owner-selected periods."""


@app.command("check")
def check_command(
    out: Annotated[Path, typer.Option(help="输入检查目录，已有检查拒绝覆盖。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if out.joinpath(CHECK_JSON).exists():
        typer.echo("structure_shift_periods: 已有检查结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        data, gate, loaded, window = load_qqq(root)
        levels, source, ndx_window = en.load_data(root)
        check = check_table(data, loaded, gate, window, levels, source, ndx_window)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        lines = str(exc).splitlines()
        message = lines[0] if lines else ""
        typer.echo(f"structure_shift_periods: 数据载入失败：{message}")
        raise typer.Exit(code=1) from None
    write_check(check, out)
    typer.echo(check_line(check, out))
    if not check["ok"]:
        raise typer.Exit(code=1)


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="策略结果目录，已有结果拒绝覆盖。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any(out.joinpath(name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("structure_shift_periods: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        data, gate, loaded, window = load_qqq(root)
        levels, source, ndx_window = en.load_data(root)
        result = run_table(data, loaded, gate, window, levels, source, ndx_window)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        lines = str(exc).splitlines()
        message = lines[0] if lines else ""
        typer.echo(f"structure_shift_periods: 数据载入失败：{message}")
        raise typer.Exit(code=1) from None
    write_result(result, out)
    if not result["complete"]:
        stage = "输入检查" if result["check"]["dev"] is None else "1999–2010 重算核对"
        typer.echo(f"structure_shift_periods: {stage} FAIL，没有算两段新数据")
        raise typer.Exit(code=1)
    typer.echo(summary_line(result, out))


if __name__ == "__main__":
    app()
