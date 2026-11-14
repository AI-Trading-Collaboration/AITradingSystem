"""The three-span same-drawdown table (TRADING-2565-134).

Under owner_decision:TRADING-2565:2026-11-10:same_drawdown_table, price hold,
F0001, x2_s5_under and x2_s5_under_ladder over the Nasdaq-100 price index
1987-01-02..1999-03-09, QQQ 1999-12-23..2010-12-31 and QQQ after 2010.
One ruler: unlevered same-drawdown R1, same-average-exposure D0, and R2 levered
to hold's drawdown or volatility. Cash earns the snapshot's DTB3 return.
Rules, windows and parameters are unchanged. Check reproduces the printed
1999–2010 numbers first; a failed check prevents run from reading either window.
Immediately after reading, reproduce TRADING-2565-122 and -126's printed rows
on those windows; any mismatch prevents tables. Read only QQQ, its index and cash.
Print only: no verdicts or ledger entries, no gate or ruler changes, and no
interaction with daily reports or brokers.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Annotated, Any, NamedTuple

import numpy as np
import pandas as pd
import typer

from ai_trading_system.paths import join_root
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import segments as sg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.metrics import summarize
from ai_trading_system.research_bench.settings import LoadedSettings, load_settings
from ai_trading_system.research_bench.simulate import schedule_mask
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import entry_exit_combo as cb
from ai_trading_system.research_probes import entry_structure_shift as es
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import entry_timing_ndx as en
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_returns as xr
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import leverage_table as lt
from ai_trading_system.research_probes import structure_shift_ladder as sl
from ai_trading_system.research_probes import structure_shift_periods as sp
from ai_trading_system.yaml_loader import safe_load_yaml_path

RUN_ID = "TRADING-2565-134"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-10:same_drawdown_table"
SPANS = ("ndx", "dev", "qqq")
LINES = ("hold", "f0001", "x2_s5_under", "x2_s5_under_ladder")
RULED = LINES[1:]
SHIFT_LINES = LINES[2:]
COMMON_START = {"ndx": date(1987, 1, 2), "dev": date(1999, 12, 23), "qqq": date(2011, 1, 3)}
# Frozen print-only pilot specified by OWNER_DECISION; exit at the owner's table review.
F_GRID = tuple(round(i / 100, 2) for i in range(101))
L_GRID = tuple(round(i / 100, 2) for i in range(1, 301))
LINE_COST = cb.COST
PICK_MARGIN = 0.001
SIMPLER = "x2_s5_under"
# Financing and daily-volatility convention, shared with the existing leverage table.
TRADING_DAYS = 252
NDX_ASSETS = ("NDX_PRICE_INDEX", "CASH_DTB3")
QQQ_ASSETS = ("QQQ", "CASH_DTB3", "NDX_RETURN")
NDX_COLUMNS = ("ndx_price", "cash_return")
QQQ_COLUMNS = ("etf_reference_return", "cash_return", "ndx_return")
READ_POLICY = "once_per_owner_decision"
LEVERAGE_START = date(1999, 3, 10)
LEVERAGE_END = gp.CUTOFF
# Historical printed pins for reproduction only, never acceptance thresholds.
DEV_FIRST_DAY = "1999-12-23"
DEV_SESSIONS = 2771
DEV_PRINTED = {
    "x2_s5_under": {"total": 0.985413, "cagr": 0.064197, "max_drawdown": -0.394572},
    "x2_s5_under_ladder": {"total": 1.336565, "cagr": 0.080036, "max_drawdown": -0.460844},
    "hold": {"total": -0.370499, "cagr": -0.04112, "max_drawdown": -0.829809},
}
READ_PRINTED: dict[str, dict[str, Any]] = {
    "ndx": {
        "first_day": "1986-10-17",
        "last_day": "1999-03-09",
        "sessions": 3116,
        "rows": {
            "x2_s5_under": {"total": 6.206591, "cagr": 0.172787, "max_drawdown": -0.321774},
            "hold": {"total": 13.360477, "cagr": 0.239892, "max_drawdown": -0.399327},
        },
    },
    "qqq": {
        "first_day": "2011-01-03",
        "last_day": "2026-10-12",
        "sessions": 3966,
        "rows": {
            "x2_s5_under": {"total": 4.842585, "cagr": 0.118415, "max_drawdown": -0.385742},
            "x2_s5_under_ladder": {"total": 5.359274, "cagr": 0.12444, "max_drawdown": -0.378259},
            "hold": {"total": 14.43185, "cagr": 0.18945, "max_drawdown": -0.351178},
        },
    },
}
LINE_TEXT = {
    "hold": "买入持有",
    "f0001": "F0001 fast_slow（月末调仓）",
    "x2_s5_under": "结构转变（200 日线下出场）",
    "x2_s5_under_ladder": "结构转变加分批买回",
}
SPAN_TEXT = {"ndx": "纳指 100 1987–1999", "dev": "QQQ 1999–2010", "qqq": "QQQ 2011 年以后"}
DELEVER_TEXT = "线比买入持有深，降仓配平"
TOP_TEXT = "到顶"
CHECK_JSON = "same_drawdown_table_check.json"
RESULT_JSON = "same_drawdown_table.json"
RESULT_MD = "same_drawdown_table.md"
RULE_TEXT = (
    "只印不判，不是交易建议。四条线的规则一个数不改；真检验只有前瞻账本。"
    "先复现已印的 1999–2010 数，对上才读；只读 QQQ、它的指数和现金。"
    "纳指 100 1986–1999 对 F0001 是第一次读；"
    "加分批买回那条（-126）以前只在 QQQ 上跑过，在这段也是第一次印。"
    "读了以后先复现 TRADING-2565-122、-126 在这两段印过的行，对不上就不印表。"
    "不检验、不下结论、不进账本，不改门和尺子，不碰日报和券商。"
)
NOTE = (
    "三段共同起点依次是 1987-01-02、1999-12-23、2011-01-03；各线照自己的起点跑，"
    "共同起点以前只作回看、不量。现金拿快照的 DTB3。R1：线不比买入持有深时，"
    "按最大回撤插值配固定比例 f；更深时把线降仓配到买入持有的回撤。"
    "D0 是线减同平均敞口的固定比例对照。R2 用 0.01–3.00、每格 0.01，"
    "配到买入持有的回撤或波动；主算利差 1%、4 bps，稳健利差 2%、25 bps。"
    "结构转变线的仓位是 L 乘线当天的仓位，在线自己买卖那天和每个月底调回；"
    "线自己成交按成交额扣 0.05% 乘 L，月底另按该套 bps。"
    "F0001 和固定比例在月底决定、下一个收盘调，两种调法差一个收盘。"
    "R1/R2 只配一个最大回撤，每段只来自一次下跌。"
    "熊段表的对同回撤的对照：较浅时是线减 R1 插出的 fixed(f)，"
    "较深时是降仓线（R1 下格的 L）减买入持有，不是原线减降仓线。"
    "纳指 100 段不含分红；F0001 在这段按快照行数回看，1987 年各月底实际回看"
    "250–251 个交易日。"
)
app = typer.Typer(help="三段同回撤总账表：只印不判。")


class Span(NamedTuple):
    name: str
    dates: tuple[date, ...]
    close: tuple[float, ...]
    cash: tuple[float, ...]
    f0001: tuple[float, ...]
    first_in: int
    start: int


class Flows(NamedTuple):
    weight: tuple[float, ...]
    traded: tuple[float, ...]


class Run(NamedTuple):
    returns: tuple[float, ...]
    exposure: tuple[float, ...]
    trades: int
    bust: str | None


class Bracket(NamedTuple):
    low: int | None
    weight: float
    monotone: bool


class Inputs(NamedTuple):
    data: BenchData
    gate: tr.TrialGate
    loaded: LoadedSettings
    qqq_window: Mapping[str, Any]
    ndx_frame: pd.DataFrame
    ndx_source: Mapping[str, Any]
    windows: Mapping[str, Any]
    root_dir: Path


class SpanRuns(NamedTuple):
    span: Span
    runs: dict[str, Run]
    flows: dict[str, Flows]
    fixed: list[Run]
    delevered: dict[str, list[Run]]
    table: dict[str, Any]


def registered_windows(loaded: LoadedSettings) -> dict[str, Any]:
    """Retain the original date/role checks and require exactly the priced inputs."""
    originals = {"ndx": en.registered_window(loaded), "qqq": sp.registered_qqq_window(loaded)}
    raw = safe_load_yaml_path(join_root(loaded.root, loaded.settings.window_registry))
    result = {}
    for name, assets, column, columns in (
        ("ndx", NDX_ASSETS, en.COLUMN, NDX_COLUMNS),
        ("qqq", QQQ_ASSETS, "etf_reference_return", QQQ_COLUMNS),
    ):
        window_id = originals[name]["window_id"]
        entry = raw["windows"][window_id]
        found = (tuple(entry.get("assets", [])), entry.get("data_column"), entry.get("read_policy"))
        if found != (assets, column, READ_POLICY):
            raise ValueError(f"窗口 {window_id} 的登记和本表不一致：{found}")
        result[name] = {
            **originals[name],
            "read_policy": READ_POLICY,
            "assets": list(assets),
            "columns": list(columns),
        }
    return result


def ndx_frame(daily: pd.DataFrame) -> pd.DataFrame:
    index = pd.DatetimeIndex(daily.index)
    return daily.loc[
        (index >= pd.Timestamp(en.START)) & (index <= pd.Timestamp(en.END)), list(NDX_COLUMNS)
    ].copy()


def qqq_data(data: BenchData) -> BenchData:
    return BenchData(
        returns=data.returns.loc[:, ["qqq", "cash"]].copy(),
        inputs=data.inputs.loc[:, ["ndx_return"]].copy(),
        source=data.source,
    )


def ndx_levels(frame: pd.DataFrame) -> list[tuple[date, float]]:
    return [
        (d.date(), float(v))
        for d, v in zip(pd.DatetimeIndex(frame.index), frame["ndx_price"], strict=True)
    ]


def ndx_cash(frame: pd.DataFrame, dates: Sequence[date]) -> tuple[float, ...]:
    """Compound cash through all snapshot rows between retained index closes."""
    index = pd.DatetimeIndex(frame.index)
    wanted = pd.DatetimeIndex(list(dates))
    positions = index.get_indexer(wanted)
    if (
        len(positions) == 0
        or (positions < 0).any()
        or any(b <= a for a, b in zip(positions, positions[1:], strict=False))
    ):
        raise ValueError("纳指 100 的收盘和现金对不上。")
    rates = frame["cash_return"].to_numpy(dtype=float)
    return (
        math.nan,
        *(
            float(np.prod(1.0 + rates[a + 1 : b + 1]) - 1.0)
            for a, b in zip(positions, positions[1:], strict=False)
        ),
    )


def f0001_on(inputs: pd.DataFrame, root_dir: Path) -> pd.Series:
    kept = inputs.loc[:, ["ndx_return"]].copy()
    return lt.f0001_targets(
        BenchData(returns=pd.DataFrame(index=kept.index), inputs=kept, source={}), root_dir
    )


def ndx_targets(frame: pd.DataFrame, root_dir: Path) -> pd.Series:
    price = frame["ndx_price"]
    return f0001_on(pd.DataFrame({"ndx_return": price / price.shift(1) - 1.0}), root_dir)


def make_span(
    name: str, series: ec.Closes, cash: Sequence[float], f0001: Sequence[float], first_in: int
) -> Span:
    day = COMMON_START[name]
    if day not in series.dates:
        raise ValueError(f"共同起点 {day} 不是收盘日。")
    return Span(
        name,
        series.dates,
        series.close,
        tuple(cash),
        tuple(f0001),
        first_in,
        series.dates.index(day),
    )


def span_frame(span: Span) -> pd.DataFrame:
    close = pd.Series(span.close, index=pd.DatetimeIndex(span.dates))
    return pd.DataFrame({"qqq": close / close.shift(1) - 1.0, "cash": span.cash})


def routes(span: Span) -> dict[str, tuple[cb.Route, int]]:
    close = span.close
    entries = sp.v0_entries(close, span.first_in)
    s0 = max(span.first_in, ec.SMA_DAYS)
    prepared = xc.prepare(span.dates, close)
    line = ec.moving_average(close)
    moves = es.shifts(close, line, 5, s0)

    def fire(rule: str, data: xc.Prepared, b: int) -> tuple[int, str] | None:
        return sp.exit_under(rule, data, line, b)

    return {
        "x2_s5_under": (sp.walk("x2", prepared, entries, moves, s0, fire), s0),
        "x2_s5_under_ladder": (sl.walk_ladder(prepared, entries, moves, s0, fire).route, s0),
    }


def hold_route(span: Span) -> tuple[cb.Route, int]:
    s0 = max(span.first_in, ec.SMA_DAYS)
    held = tuple(p > s0 for p in range(len(span.dates)))
    return cb.Route(held, (s0,), (), ()), s0


def replay(
    dates: Sequence[date], close: Sequence[float], cash: Sequence[float], route: cb.Route, s0: int
) -> tuple[list[float], Flows]:
    """Replay frozen transaction decisions, with interest on unspent sale cash."""
    shares = (1.0 - LINE_COST) / close[s0]
    money = start = 0.0
    weight = [0.0] * len(close)
    traded = [0.0] * len(close)
    weight[s0] = traded[s0] = 1.0
    values = [shares * close[s0]]
    fills: dict[str, list[float]] = {}
    full: set[str] = set()
    for trip in route.trips:
        for fill in trip.get("fills", []):
            fills.setdefault(fill["date"], []).append(float(fill["share"]))
        if trip["buy"] is not None:
            full.add(trip["buy"])
    sells = set(route.sells)
    for p in range(s0 + 1, len(close)):
        money *= 1.0 + cash[p]
        before = shares * close[p] + money
        amount = 0.0
        day = dates[p].isoformat()
        if p in sells:
            amount += shares * close[p]
            money += shares * close[p] * (1.0 - LINE_COST)
            shares = 0.0
            start = money
        for part in fills.get(day, []):
            spend = min(part * start, money)
            shares += spend * (1.0 - LINE_COST) / close[p]
            money -= spend
            amount += spend
        if day in full:
            shares += money * (1.0 - LINE_COST) / close[p]
            amount += money
            money = 0.0
        after = shares * close[p] + money
        values.append(after)
        traded[p] = amount / before if before > 0 else 0.0
        weight[p] = shares * close[p] / after if after > 0 else 0.0
    return values, Flows(tuple(weight), tuple(traded))


def scaled_run(span: Span, flows: Flows, leverage: float, costs: lt.Costs) -> Run:
    """Drift scaled holdings, resetting on line trades or at the month-end close."""
    w = leverage * flows.weight[span.start]
    scheduled = schedule_mask(pd.DatetimeIndex(span.dates), "month_end")
    returns, exposure = [], []
    trades = 0
    bust = None
    for p in range(span.start + 1, len(span.dates)):
        if bust is not None:
            returns.append(0.0)
            exposure.append(0.0)
            continue
        exposure.append(w)
        market = span.close[p] / span.close[p - 1] - 1.0
        spread = max(w - 1.0, 0.0) * costs.spread / TRADING_DAYS
        gross = w * market + (1.0 - w) * span.cash[p] - spread
        if 1.0 + gross <= 0.0:
            returns.append(-1.0)
            bust = span.dates[p].isoformat()
            continue
        w = w * (1.0 + market) / (1.0 + gross)
        cost = 0.0
        if flows.traded[p] > 0:
            cost = LINE_COST * leverage * flows.traded[p]
            w = leverage * flows.weight[p]
            trades += 1
        elif scheduled[p] and p < len(span.dates) - 1:
            target = leverage * flows.weight[p]
            cost = abs(target - w) * costs.bps / 10_000
            w = target
        returns.append((1.0 + gross) * (1.0 - cost) - 1.0)
    return Run(tuple(returns), tuple(exposure), trades, bust)


def levered_run(span: Span, targets: Sequence[float], leverage: float, costs: lt.Costs) -> Run:
    frame = span_frame(span)
    levered = lt.simulate_levered(
        frame,
        pd.Series(targets, index=frame.index),
        start=span.dates[span.start],
        end=span.dates[-1],
        leverage=leverage,
        costs=costs,
    )
    daily = levered.simulation.daily
    return Run(
        tuple(float(v) for v in daily["return"]),
        tuple(float(v) for v in daily["exposure"]),
        int((daily["turnover"] > 0).sum()),
        levered.bust.date().isoformat() if levered.bust is not None else None,
    )


def fixed_run(span: Span, f: float, costs: lt.Costs = lt.MAIN) -> Run:
    return levered_run(span, [f] * len(span.dates), 1.0, costs)


def line_run(
    span: Span, line: str, flows: Mapping[str, Flows], leverage: float, costs: lt.Costs
) -> Run:
    if line == "f0001":
        return levered_run(span, span.f0001, leverage, costs)
    if line == "hold":
        return levered_run(span, [1.0] * len(span.dates), leverage, costs)
    return scaled_run(span, flows[line], leverage, costs)


def years_between(start: date, end: date) -> float:
    return (end - start).days / cb.YEAR_DAYS


def _growth(returns: Sequence[float]) -> float:
    return float(np.prod(1.0 + np.asarray(returns, dtype=float)))


def _path_stats(returns: Sequence[float], years: float) -> dict[str, Any]:
    values = np.cumprod(np.r_[1.0, 1.0 + np.asarray(returns, dtype=float)])
    drawdowns = values / np.maximum.accumulate(values) - 1.0
    trough = int(np.argmin(drawdowns))
    peak = int(np.argmax(values[: trough + 1]))
    return {
        "total": float(values[-1]) - 1.0,
        "cagr": float(values[-1]) ** (1.0 / years) - 1.0 if years > 0 else None,
        "max_drawdown": float(drawdowns[trough]),
        "peak": peak,
        "trough": trough,
    }


def measure(span: Span, run: Run) -> dict[str, Any]:
    years = years_between(span.dates[span.start], span.dates[-1])
    stats = _path_stats(run.returns, years)
    values = np.cumprod(np.r_[1.0, 1.0 + np.asarray(run.returns)])
    tails = [
        float(values[p] / values[p - cb.TAIL_DAYS] - 1.0)
        for p in range(cb.TAIL_DAYS, len(values))
        if values[p - cb.TAIL_DAYS] > 0
    ]
    return {
        "total": stats["total"],
        "cagr": stats["cagr"],
        "max_drawdown": stats["max_drawdown"],
        "drawdown_peak": span.dates[span.start + stats["peak"]].isoformat(),
        "drawdown_trough": span.dates[span.start + stats["trough"]].isoformat(),
        "worst20": xr.rank(tails, cb.TAIL) if tails else None,
        "volatility": float(np.std(run.returns, ddof=1)) * math.sqrt(TRADING_DAYS)
        if len(run.returns) > 1
        else None,
        "exposure": float(np.mean(run.exposure)) if run.exposure else None,
        "in_market": sum(w > 0 for w in run.exposure) / len(run.exposure) if run.exposure else None,
        "trades": run.trades,
        "bust": run.bust,
    }


def bracket(target: float, grid: Sequence[float], drawdowns: Sequence[float]) -> Bracket:
    if len(grid) != len(drawdowns):
        raise ValueError("格子和回撤没有对齐。")
    monotone = all(b <= a for a, b in zip(drawdowns, drawdowns[1:], strict=False))
    low = max((i for i, dd in enumerate(drawdowns) if dd >= target), default=None)
    weight = 0.0
    if low is not None and low < len(grid) - 1:
        step = drawdowns[low] - drawdowns[low + 1]
        if step > 0:
            weight = (drawdowns[low] - target) / step
    return Bracket(low, weight, monotone)


def between(found: Bracket, values: Sequence[float]) -> float | None:
    if found.low is None:
        return None
    low = found.low
    return float(values[low] + found.weight * (values[min(low + 1, len(values) - 1)] - values[low]))


def same_drawdown(
    line: Mapping[str, Any],
    hold: Mapping[str, Any],
    f_grid: Sequence[float],
    fixed: Sequence[Mapping[str, Any]],
    l_grid: Sequence[float],
    delevered: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    kind = "fixed" if line["max_drawdown"] >= hold["max_drawdown"] else "delever"
    grid, curve = (f_grid, fixed) if kind == "fixed" else (l_grid, delevered)
    target = line["max_drawdown"] if kind == "fixed" else hold["max_drawdown"]
    found = bracket(target, grid, [m["max_drawdown"] for m in curve])
    matched = between(found, [m["cagr"] for m in curve])
    low = found.low
    d1 = (
        None
        if matched is None
        else (line["cagr"] - matched if kind == "fixed" else matched - hold["cagr"])
    )
    return {
        "kind": kind,
        "low": grid[low] if low is not None else None,
        "high": grid[min(low + 1, len(grid) - 1)] if low is not None else None,
        "weight": found.weight,
        "monotone": found.monotone,
        "at": between(found, grid),
        "matched_cagr": matched,
        "d1": d1,
        "reference_d1": line["cagr"] - hold["cagr"] if kind == "delever" else None,
    }


def levered_cells(
    r1: Mapping[str, Any],
    hold: Mapping[str, Any],
    l_grid: Sequence[float],
    main: Sequence[Mapping[str, Any]],
    robust: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    result = {}
    for pairing in ("drawdown", "volatility"):
        allowed = [
            m["max_drawdown"] >= hold["max_drawdown"]
            if pairing == "drawdown"
            else m["volatility"] <= hold["volatility"]
            for m in main
        ]
        found = lt.largest_allowed(l_grid, allowed)
        i = l_grid.index(found.leverage)
        cell = {
            "leverage": found.leverage,
            "flag": found.flag,
            "monotone": found.monotone,
            "main": main[i]["cagr"] - hold["cagr"],
            "robust": robust[i]["cagr"] - hold["cagr"],
            "from_r1": False,
        }
        if pairing == "drawdown" and r1["kind"] == "delever":
            under = [j for j, x in enumerate(l_grid) if x <= 1.0]
            matched = bracket(
                hold["max_drawdown"],
                [l_grid[j] for j in under],
                [main[j]["max_drawdown"] for j in under],
            )
            robust_cagr = between(matched, [robust[j]["cagr"] for j in under])
            cell.update(
                leverage=r1["at"],
                main=r1["d1"],
                robust=None if robust_cagr is None else robust_cagr - hold["cagr"],
                from_r1=True,
            )
        result[pairing] = cell
    return result


def annualized(returns: Sequence[float]) -> float | None:
    return _growth(returns) ** (TRADING_DAYS / len(returns)) - 1.0 if len(returns) else None


def bear_months(span: Span, hold: Run) -> tuple[list[dict[str, Any]], list[bool]]:
    index = pd.DatetimeIndex(span.dates[span.start + 1 :])
    segments = [
        s for s in sg.segments(pd.Series(hold.returns, index=index)) if s.kind != "long_bull"
    ]
    rows = [
        {
            "id": s.id,
            "kind": s.kind,
            "start": s.start,
            "end": s.end,
            "peak": s.peak,
            "trough": s.trough,
            "months": s.months,
            "change": s.change,
        }
        for s in segments
    ]
    inside = [
        any(pd.Period(s.start, freq="M") <= m <= pd.Period(s.end, freq="M") for s in segments)
        for m in index.to_period("M")
    ]
    return rows, inside


def bear_row(
    run: Run, hold: Run, matched: tuple[Run, Run] | None, inside: Sequence[bool]
) -> dict[str, Any]:
    def value(line: Run, flag: bool) -> float | None:
        return annualized([r for r, keep in zip(line.returns, inside, strict=True) if keep == flag])

    def difference(a: float | None, b: float | None) -> float | None:
        return None if a is None or b is None else a - b

    result = {}
    for name, flag in (("inside", True), ("outside", False)):
        own = value(run, flag)
        result[name] = own
        result[f"{name}_vs_hold"] = difference(own, value(hold, flag))
        result[f"{name}_vs_matched"] = (
            None
            if matched is None
            else difference(value(matched[0], flag), value(matched[1], flag))
        )
    return result


def year_rows(span: Span, runs: Mapping[str, Run]) -> list[dict[str, Any]]:
    days = span.dates[span.start + 1 :]
    rows = []
    for year in sorted({d.year for d in days}):
        values = {
            line: _growth([r for d, r in zip(days, run.returns, strict=True) if d.year == year])
            - 1.0
            for line, run in runs.items()
        }
        rows.append(
            {
                "year": year,
                "hold": values["hold"],
                **{line: values[line] - values["hold"] for line in RULED},
            }
        )
    return rows


def span_table(
    span: Span, *, f_grid: Sequence[float] = F_GRID, l_grid: Sequence[float] = L_GRID
) -> SpanRuns:
    flows = {
        line: replay(span.dates, span.close, span.cash, route, s0)[1]
        for line, (route, s0) in routes(span).items()
    }
    runs = {line: line_run(span, line, flows, 1.0, lt.MAIN) for line in LINES}
    lines = {line: measure(span, run) for line, run in runs.items()}
    fixed = [fixed_run(span, f) for f in f_grid]
    fixed_metrics = [measure(span, run) for run in fixed]
    under = [x for x in l_grid if x <= 1.0]
    delevered: dict[str, list[Run]] = {}
    r1, d0, r2 = {}, {}, {}
    segments, inside = bear_months(span, runs["hold"])
    bears = {}
    for line in RULED:
        main_runs = [line_run(span, line, flows, x, lt.MAIN) for x in l_grid]
        main = [measure(span, run) for run in main_runs]
        robust = [measure(span, line_run(span, line, flows, x, lt.ROBUST)) for x in l_grid]
        delevered[line] = [run for x, run in zip(l_grid, main_runs, strict=True) if x <= 1.0]
        lower = [m for x, m in zip(l_grid, main, strict=True) if x <= 1.0]
        r1[line] = same_drawdown(lines[line], lines["hold"], f_grid, fixed_metrics, under, lower)
        exposure = lines[line]["exposure"]
        difference = lines[line]["cagr"] - measure(span, fixed_run(span, exposure))["cagr"]
        d0[line] = {
            "exposure": exposure,
            "d0": difference,
            "opposite": r1[line]["d1"] is not None and difference * r1[line]["d1"] < 0,
        }
        r2[line] = levered_cells(r1[line], lines["hold"], l_grid, main, robust)
        match = r1[line]
        pair = None
        if match["kind"] == "fixed" and match["at"] is not None:
            pair = (runs[line], fixed_run(span, match["at"]))
        elif match["kind"] == "delever" and match["low"] is not None:
            pair = (delevered[line][under.index(match["low"])], runs["hold"])
        bears[line] = bear_row(runs[line], runs["hold"], pair, inside)
    table = {
        "start": span.dates[span.start].isoformat(),
        "end": span.dates[-1].isoformat(),
        "return_days": len(runs["hold"].returns),
        "years": years_between(span.dates[span.start], span.dates[-1]),
        "lines": lines,
        "r1": r1,
        "d0": d0,
        "r2": r2,
        "bear": {"segments": segments, "rows": bears},
        "years_rows": year_rows(span, runs),
    }
    return SpanRuns(span, runs, flows, fixed, delevered, table)


def joined(parts: Sequence[tuple[Run, float]]) -> dict[str, Any]:
    years = sum(y for _, y in parts)
    stats = _path_stats([r for run, _ in parts for r in run.returns], years)
    return {"cagr": stats["cagr"], "max_drawdown": stats["max_drawdown"], "years": years}


def full_row(
    spans: Sequence[SpanRuns], f_grid: Sequence[float], l_grid: Sequence[float]
) -> dict[str, Any]:
    years = [s.table["years"] for s in spans]
    lines = {
        line: joined([(s.runs[line], y) for s, y in zip(spans, years, strict=True)])
        for line in LINES
    }
    fixed = [
        joined([(s.fixed[i], y) for s, y in zip(spans, years, strict=True)])
        for i in range(len(f_grid))
    ]
    under = [x for x in l_grid if x <= 1.0]
    r1 = {}
    for line in RULED:
        lower = [
            joined([(s.delevered[line][i], y) for s, y in zip(spans, years, strict=True)])
            for i in range(len(under))
        ]
        r1[line] = same_drawdown(lines[line], lines["hold"], f_grid, fixed, under, lower)
    return {"years": sum(years), "gap": "1999-03-10..1999-12-22", "lines": lines, "r1": r1}


def summary_rows(tables: Mapping[str, Any], full: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for line in RULED:
        d1 = {name: tables[name]["r1"][line]["d1"] for name in SPANS}
        valid = [name for name in SPANS if d1[name] is not None]
        rows.append(
            {
                "line": line,
                "d1": d1,
                "kind": {name: tables[name]["r1"][line]["kind"] for name in SPANS},
                "r2": {name: tables[name]["r2"][line]["drawdown"]["main"] for name in SPANS},
                "worst_span": min(valid, key=lambda n: d1[n]) if valid else None,
                "full_d1": full["r1"][line]["d1"],
            }
        )
    return rows


def pick(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    lows: dict[str, float | None] = {}
    for line in SHIFT_LINES:
        row = next((r for r in rows if r["line"] == line), None)
        values = [row["d1"].get(name) for name in SPANS] if row is not None else [None]
        finite_values = [float(v) for v in values if v is not None]
        lows[line] = min(finite_values) if len(finite_values) == len(SPANS) else None
    simpler, ladder = (lows[line] for line in SHIFT_LINES)
    difference = None if simpler is None or ladder is None else ladder - simpler
    # Floating-point subtraction of two percentages must preserve the inclusive boundary.
    chosen = (
        None
        if difference is None
        else (
            SHIFT_LINES[1]
            if difference >= PICK_MARGIN
            or math.isclose(difference, PICK_MARGIN, rel_tol=1e-12, abs_tol=0.0)
            else SIMPLER
        )
    )
    return {"pick": chosen, "lows": lows, "difference": difference, "card": "always"}


def opening_defined(span: Span) -> dict[str, Any]:
    scheduled = schedule_mask(pd.DatetimeIndex(span.dates), "month_end")
    opening = next((p for p in range(span.start - 1, -1, -1) if scheduled[p]), None)
    return {
        "opening": span.dates[opening].isoformat() if opening is not None else None,
        "defined": opening is not None and math.isfinite(span.f0001[opening]),
    }


def cash_missing(span: Span) -> int:
    first = min(max(span.first_in, ec.SMA_DAYS), span.start) + 1
    return sum(not math.isfinite(v) for v in span.cash[first:])


def _reproduction(span: Span, lines: Sequence[str]) -> dict[str, Any]:
    paths = routes(span)
    paths["hold"] = hold_route(span)
    rows = {}
    s0 = max(span.first_in, ec.SMA_DAYS)
    for line in lines:
        route, origin = paths[line]
        values, _ = replay(span.dates, span.close, [0.0] * len(span.dates), route, origin)
        metrics = cb.metrics(span.dates, values, route, origin)
        rows[line] = {key: round(metrics[key], 6) for key in ("total", "cagr", "max_drawdown")}
    return {
        "first_day": span.dates[s0].isoformat(),
        "last_day": span.dates[-1].isoformat(),
        "sessions": len(span.dates) - 1 - s0,
        "rows": rows,
    }


def dev_reproduction(span: Span) -> dict[str, Any]:
    found = _reproduction(span, list(DEV_PRINTED))
    found["ok"] = (
        found["rows"] == DEV_PRINTED
        and found["first_day"] == DEV_FIRST_DAY
        and found["sessions"] == DEV_SESSIONS
    )
    return found


def read_reproduction(span: Span, printed: Mapping[str, Any]) -> dict[str, Any]:
    found = _reproduction(span, list(printed["rows"]))
    found["ok"] = all(found[k] == printed[k] for k in ("first_day", "last_day", "sessions", "rows"))
    return found


def f0001_reproduction(data: BenchData, loaded: LoadedSettings, root_dir: Path) -> dict[str, Any]:
    cut = gp.cut(data)
    targets = lt.f0001_targets(cut, root_dir)
    summaries = {}
    for line, target in (("f0001", targets), ("hold", pd.Series(1.0, index=cut.returns.index))):
        sim = lt.simulate_levered(
            cut.returns, target, start=LEVERAGE_START, end=LEVERAGE_END, leverage=1.0, costs=lt.MAIN
        ).simulation
        summaries[line] = summarize(sim, cut.returns["cash"], loaded)
    rows = lt.reproduce_rows("qqq", summaries["f0001"], summaries["hold"])
    return {"rows": rows, "ok": all(row["ok"] for row in rows)}


def build_spans(inputs: Inputs) -> tuple[dict[str, Span], dict[str, Any]]:
    q = sp.qqq_inputs(inputs.data, inputs.loaded, inputs.gate, inputs.qqq_window)
    ndx = sp.ndx_inputs(ndx_levels(inputs.ndx_frame), inputs.windows["ndx"])
    targets = lt.f0001_targets(inputs.data, inputs.root_dir)
    nt = ndx_targets(inputs.ndx_frame, inputs.root_dir)

    def aligned(series: pd.Series, days: Sequence[date]) -> tuple[float, ...]:
        return tuple(float(v) for v in series.reindex(pd.DatetimeIndex(list(days))))

    spans = {
        "ndx": make_span(
            "ndx",
            ndx.series,
            ndx_cash(inputs.ndx_frame, ndx.series.dates),
            aligned(nt, ndx.series.dates),
            0,
        )
    }
    for name, series, first_in in (("dev", q.dev, q.dev_first_in), ("qqq", q.full, q.first_in)):
        cash = list(aligned(inputs.data.returns["cash"], series.dates))
        cash[0] = math.nan
        spans[name] = make_span(name, series, cash, aligned(targets, series.dates), first_in)
    checks: dict[str, dict[str, Any]] = {
        name: {
            "start": span.dates[span.start].isoformat(),
            "first_close": span.dates[0].isoformat(),
            "last_close": span.dates[-1].isoformat(),
            "return_days": len(span.dates) - 1 - span.start,
            "cash_missing": cash_missing(span),
            "f0001": opening_defined(span),
        }
        for name, span in spans.items()
    }
    return spans, {
        "qqq": q.check,
        "ndx": ndx.check,
        "spans": checks,
        "ok": bool(
            q.check["ok"]
            and ndx.check["ok"]
            and all(c["cash_missing"] == 0 and c["f0001"]["defined"] for c in checks.values())
        ),
    }


def rule_block() -> dict[str, Any]:
    return {
        "spans": list(SPANS),
        "common_start": {k: v.isoformat() for k, v in COMMON_START.items()},
        "lines": list(LINES),
        "cash": "snapshot_dtb3_cash_return",
        "f_grid": [0.0, 1.0, 0.01],
        "l_grid": [0.01, 3.0, 0.01],
        "main": list(lt.MAIN),
        "robust": list(lt.ROBUST),
        "line_cost": LINE_COST,
        "shift_reset": "line_trade_close_and_month_end",
        "pick_margin": PICK_MARGIN,
        "simpler": SIMPLER,
    }


def check_table(inputs: Inputs) -> dict[str, Any]:
    spans, checks = build_spans(inputs)
    reproduction = {
        "dev": dev_reproduction(spans["dev"]),
        "f0001": f0001_reproduction(inputs.data, inputs.loaded, inputs.root_dir),
    }
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "windows": dict(inputs.windows),
        "rule": rule_block(),
        "inputs": checks,
        "reproduction": reproduction,
        "ok": bool(checks["ok"] and all(r["ok"] for r in reproduction.values())),
    }


def run_table(
    inputs: Inputs, *, f_grid: Sequence[float] = F_GRID, l_grid: Sequence[float] = L_GRID
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "rule": rule_block(),
        "check": check_table(inputs),
        "complete": False,
    }
    if not result["check"]["ok"]:
        return result
    spans, checks = build_spans(inputs)
    result["window_reads"] = {
        name: {**inputs.windows[name], "reads": 1, "owner_decision": OWNER_DECISION}
        for name in READ_PRINTED
    }
    result["read_reproduction"] = {
        name: read_reproduction(spans[name], printed) for name, printed in READ_PRINTED.items()
    }
    if not checks["ok"] or not all(r["ok"] for r in result["read_reproduction"].values()):
        return result
    done = [span_table(spans[name], f_grid=f_grid, l_grid=l_grid) for name in SPANS]
    result["spans"] = {name: s.table for name, s in zip(SPANS, done, strict=True)}
    result["full"] = full_row(done, f_grid, l_grid)
    result["summary"] = summary_rows(result["spans"], result["full"])
    result["pick"] = pick(result["summary"])
    result["complete"] = True
    return result


def points(value: float | None) -> str:
    return "—" if value is None else f"{value * 100:+.2f} 点"


def d1_text(row: Mapping[str, Any]) -> str:
    return points(row["d1"]) + ("（降仓配平）" if row["kind"] == "delever" else "")


def d2_text(row: Mapping[str, Any], setting: str) -> str:
    return points(row[setting]) + (f"（{TOP_TEXT}）" if row["flag"] == "top" else "")


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> list[str]:
    def line(cells: Sequence[str]) -> str:
        return "| " + " | ".join(cells) + " |"

    return [line(headers), line(["---"] * len(headers)), *[line(row) for row in rows], ""]


def render_span(name: str, table: Mapping[str, Any]) -> list[str]:
    lines = [
        f"## {SPAN_TEXT[name]}",
        "",
        f"共同起点 {table['start']}..{table['end']}，收益日 {table['return_days']} 天。",
        "",
    ]
    lines += _table(
        [
            "线",
            "总收益",
            "年化",
            "最大回撤",
            "回撤高点 → 低点",
            "最差 20 日",
            "波动",
            "平均敞口",
            "在场",
            "成交天数",
            "爆仓",
        ],
        [
            [
                LINE_TEXT[line],
                *[xr.pct(table["lines"][line][k]) for k in ("total", "cagr", "max_drawdown")],
                f"{table['lines'][line]['drawdown_peak']} → "
                f"{table['lines'][line]['drawdown_trough']}",
                *[
                    xr.pct(table["lines"][line][k])
                    for k in ("worst20", "volatility", "exposure", "in_market")
                ],
                str(table["lines"][line]["trades"]),
                str(table["lines"][line]["bust"] or "—"),
            ]
            for line in LINES
        ],
    )
    lines += ["### R1、D0、R2", ""]
    lines += _table(
        [
            "线",
            "R1 D1",
            "配法",
            "下格 / 上格 / 插值",
            "单调",
            "D0",
            "反号",
            "回撤 L",
            "主算",
            "稳健",
            "波动 L",
            "主算",
            "稳健",
        ],
        [
            [
                LINE_TEXT[line],
                d1_text(table["r1"][line]),
                DELEVER_TEXT if table["r1"][line]["kind"] == "delever" else "固定比例",
                f"{table['r1'][line]['low']} / {table['r1'][line]['high']} / "
                f"{table['r1'][line]['at']}",
                str(table["r1"][line]["monotone"]),
                points(table["d0"][line]["d0"]),
                str(table["d0"][line]["opposite"]),
                *[
                    cell
                    for pairing in ("drawdown", "volatility")
                    for cell in (
                        str(table["r2"][line][pairing]["leverage"]),
                        d2_text(table["r2"][line][pairing], "main"),
                        d2_text(table["r2"][line][pairing], "robust"),
                    )
                ],
            ]
            for line in RULED
        ],
    )
    lines += ["### 熊段内外", "", "对同回撤的对照", ""]
    lines += _table(
        [
            "线",
            "熊内年化",
            "熊外年化",
            "熊内对持有",
            "熊外对持有",
            "熊内对同回撤的对照",
            "熊外对同回撤的对照",
        ],
        [
            [
                LINE_TEXT[line],
                *[
                    points(table["bear"]["rows"][line][k])
                    for k in (
                        "inside",
                        "outside",
                        "inside_vs_hold",
                        "outside_vs_hold",
                        "inside_vs_matched",
                        "outside_vs_matched",
                    )
                ],
            ]
            for line in RULED
        ],
    )
    lines += _table(
        ["熊段", "种类", "开始月", "结束月"],
        [[s["id"], s["kind"], s["start"], s["end"]] for s in table["bear"]["segments"]],
    )
    lines += ["### 逐年（有规则的线减买入持有）", ""]
    lines += _table(
        ["年", "买入持有", *[LINE_TEXT[line] for line in RULED]],
        [
            [str(row["year"]), xr.pct(row["hold"]), *[points(row[line]) for line in RULED]]
            for row in table["years_rows"]
        ],
    )
    return lines


def render_markdown(result: Mapping[str, Any]) -> str:
    lines = [
        f"# 三段同回撤总账表 · {RUN_ID}",
        "",
        OWNER_DECISION,
        "",
        RULE_TEXT,
        "",
        NOTE,
        "",
        "数据质量及输入检查：PASS；开发窗口复现：PASS。",
        "",
        "## 总表",
        "",
    ]
    lines += _table(
        ["线", *[SPAN_TEXT[n] + " D1 / 配法 / R2 主算" for n in SPANS], "最低段", "全程 D1"],
        [
            [
                LINE_TEXT[row["line"]],
                *[
                    f"{points(row['d1'][n])} / {row['kind'][n]} / {points(row['r2'][n])}"
                    for n in SPANS
                ],
                str(row["worst_span"]),
                points(row["full_d1"]),
            ]
            for row in result["summary"]
        ],
    )
    chosen = result["pick"]
    lines += [
        "## 挑哪一条",
        "",
        f"挑：{chosen['pick'] or '无'}；两条最低 D1：{chosen['lows']}；"
        f"差 {points(chosen['difference'])}。差至少 0.1 点/年挑梯子，否则挑简单线。"
        "恒发卡问 Baker 冻不冻进前瞻账本，本表不冻结。",
        "",
        "## 全程一行",
        "",
        f"三段首尾相接，年数相加；缺口 {result['full']['gap']} 不补收益。",
        "",
    ]
    lines += _table(
        ["线", "年化", "最大回撤", "R1 D1"],
        [
            [
                LINE_TEXT[line],
                xr.pct(result["full"]["lines"][line]["cagr"]),
                xr.pct(result["full"]["lines"][line]["max_drawdown"]),
                d1_text(result["full"]["r1"][line]) if line in RULED else "—",
            ]
            for line in LINES
        ],
    )
    for name in SPANS:
        lines += render_span(name, result["spans"][name])
    lines += [
        "## 读取记录",
        "",
        "读后先按现金 0 复现 TRADING-2565-122、-126 在两段印过的行，对上才印表。",
        "",
    ]
    for name, read in result["window_reads"].items():
        reproduction = result["read_reproduction"][name]
        lines.append(
            f"- {name}：{read['window_id']}；请求 {read['start']}..{read['end']}；"
            f"列 {read['columns']}；读 {read['reads']} 次；{read['owner_decision']}；"
            f"复现 {'PASS' if reproduction['ok'] else 'FAIL'}：{reproduction}"
        )
    return "\n".join(lines).rstrip() + "\n"


def check_line(check: Mapping[str, Any], out: Path) -> str:
    rows = check["inputs"]["spans"]
    details = "；".join(
        f"{name} {row['start']}..{row['last_close']}，{row['return_days']} 个收益日，"
        f"现金缺 {row['cash_missing']}，起点信号 {row['f0001']['defined']}"
        for name, row in rows.items()
    )
    return (
        f"same_drawdown_table check: {'ok' if check['ok'] else 'FAIL'}；{details}；"
        f"复现 {check['reproduction']}；结果目录 {out}"
    )


def summary_line(result: Mapping[str, Any], out: Path) -> str:
    rows = "；".join(
        f"{row['line']}：" + "、".join(f"{name} {points(row['d1'][name])}" for name in SPANS)
        for row in result["summary"]
    )
    return f"same_drawdown_table: 只印不判；{rows}；挑 {result['pick']['pick']}；结果目录 {out}"


def write_check(check: Mapping[str, Any], out: Path) -> None:
    text = json.dumps(check, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(CHECK_JSON).write_text(text, encoding="utf-8")


def write_result(result: Mapping[str, Any], out: Path) -> None:
    text = json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(RESULT_JSON).write_text(text, encoding="utf-8")
    if result["complete"]:
        out.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


def load_inputs(root: Path) -> Inputs:
    loaded = load_settings()
    windows = registered_windows(loaded)
    data, gate, loaded, window = sp.load_qqq(root)
    daily, report = qlh.load_daily(qlh.resolve_snapshot_dir(root, en.SNAPSHOT_ID))
    if report.get("snapshot_id") != en.SNAPSHOT_ID:
        raise ValueError(f"快照不是 {en.SNAPSHOT_ID}。")
    source = {
        "snapshot_id": en.SNAPSHOT_ID,
        "columns": list(NDX_COLUMNS),
        "check_status": report.get("status"),
        "series_csv_sha256": report.get("series_csv_sha256"),
    }
    return Inputs(
        qqq_data(data),
        gate,
        loaded,
        window,
        ndx_frame(daily),
        source,
        windows,
        lg.research_root(loaded),
    )


@app.callback()
def main() -> None:
    """Print the owner-authorized same-drawdown table, without promotion."""


def _refuse(out: Path) -> None:
    if out.joinpath(CHECK_JSON).exists():
        typer.echo("same_drawdown_table: 已有检查结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    if any(out.joinpath(name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("same_drawdown_table: 已有结果，拒绝再读。")
        raise typer.Exit(code=1)


def _failure(exc: Exception) -> None:
    lines = str(exc).splitlines()
    message = lines[0] if lines else ""
    typer.echo(f"same_drawdown_table: 数据载入失败：{message}")


@app.command("check")
def check_command(
    out: Annotated[Path, typer.Option(help="输入检查目录，拒绝覆盖。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。 ")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    _refuse(out)
    try:
        check = check_table(load_inputs(root))
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        _failure(exc)
        raise typer.Exit(code=1) from None
    write_check(check, out)
    typer.echo(check_line(check, out))
    if not check["ok"]:
        raise typer.Exit(code=1)


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="结果目录，已有结果拒绝再读。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。 ")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    _refuse(out)
    try:
        result = run_table(load_inputs(root))
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        _failure(exc)
        raise typer.Exit(code=1) from None
    write_result(result, out)
    if not result["complete"]:
        typer.echo(
            "same_drawdown_table: 两个窗口各读了一次，读后复现 FAIL，没有印表"
            if "window_reads" in result
            else "same_drawdown_table: 输入检查或复现 FAIL，没有读两个窗口"
        )
        raise typer.Exit(code=1)
    typer.echo(summary_line(result, out))


if __name__ == "__main__":
    app()
