"""Acceptance tests for TRADING-2565-120: a structure-shift entry after a break of the 200-day line.

Requirement: knowledge qqq-options/auto-loop/period7/entry-exit-combo/entry-structure-shift-
requirements-2026-11-03.md version 1.0 (owner decision entry_structure_shift). Synthetic data only;
no snapshot, no network.
"""

from __future__ import annotations

import ast
import json
import math
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import load_settings
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData, from_daily_frame, with_signal_sources
from ai_trading_system.research_probes import entry_exit_combo as cb
from ai_trading_system.research_probes import entry_structure_shift as ss
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import exit_rules_after as xa
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_returns as xr
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import policy_table as pt

LOADED = load_settings()
GATE = tr.load_gate(PROJECT_ROOT / "research" / "_trials" / "prereg.yaml")
_, PLAN = tr.load_trial(lg.research_root(LOADED), pt.DATA_TRIAL)
MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "entry_structure_shift.py"
PLAIN = ("x2_a", "x3_a", "x2x3_a")
SHIFTED = ("x2_s5", "x3_s5", "x2x3_s5", "x2_s2", "x3_s2", "x2x3_s2")


# Constants and imports ------------------------------------------------------------------------


def test_the_run_and_its_strategies() -> None:
    assert ss.RUN_ID == "TRADING-2565-120"
    assert ss.OWNER_DECISION == "owner_decision:TRADING-2565:2026-11-03:entry_structure_shift"
    assert ss.SWINGS == (5, 2)
    assert ss.HORIZONS == (20, 60)
    assert ss.PLAIN == PLAIN
    assert ss.SHIFTED == SHIFTED
    assert ss.STRATEGIES == (*SHIFTED, *PLAIN, "hold")
    assert list(ss.STRATEGY_TEXT) == [*SHIFTED, *PLAIN, "hold"]
    assert ss.STRATEGY_TEXT["x3_s5"] == "X3 跌破 50 日线，A 加结构转变买回（左右各 5 天）"
    assert ss.STRATEGY_TEXT["x2x3_s2"] == "X2 或 X3 先响，A 加结构转变买回（左右各 2 天）"
    assert ss.STRATEGY_TEXT["x2_a"] == "X2 回落 8%，A 只等入场买回"
    assert ss.STRATEGY_TEXT["hold"] == "买入持有"
    assert ss.HOW_TEXT == {**cb.HOW_TEXT, "shift": "结构转变"}
    assert ss.SPAN_TEXT == xa.SPAN_TEXT["qqq"]
    assert (ss.RESULT_JSON, ss.RESULT_MD) == (
        "entry_structure_shift.json",
        "entry_structure_shift.md",
    )


def test_the_rule_text_and_the_note() -> None:
    for words in (
        "前一个收盘在 200 日线上或线上，这个收盘在线下",
        "每次新的跌破重新开始找",
        "严格低于（高于）它左右各 k 个收盘",
        "要到 k 个收盘以后才知道",
        "在 200 日线下、比当前的低点 L 更低的摆动低点就成为新的 L",
        "H 是 L 之前最近的一个摆动高点",
        "第一个收盘高于 H 的收盘，就在这个收盘入场",
        "再入场要有比上次 L 更低的摆动低点",
        "收盘低于 L 的收盘，下一个收盘卖",
        "风险 = 入场收盘 ÷ L 收盘 − 1",
        "第 1 到 h − 1 个收盘里第一次低于 L",
        "k = 5 是 Baker 选的，k = 2 只作对照",
    ):
        assert words in ss.RULE_TEXT, words
    assert ss.NOTE.startswith(cb.NOTE)
    for words in (
        "这张表只有 A 版",
        "v0_close 入场和结构转变入场里先到的那个收盘买回满仓",
        "同一个收盘算 v0_close 入场",
        "收盘低于那次的 L，下一个收盘卖，和出场同一个收盘响算出场",
    ):
        assert words in ss.NOTE, words


def test_the_module_reuses_the_118_module_and_reads_only_qqq() -> None:
    text = MODULE.read_text(encoding="utf-8")
    tree = ast.parse(text)
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.update(f"{node.module}.{alias.name}" for alias in node.names)
    allowed = (
        "__future__.",
        "json",
        "math",
        "collections.",
        "datetime.",
        "pathlib.",
        "typing.",
        "typer",
        "ai_trading_system.research_bench.trial",
        "ai_trading_system.research_bench.data.BenchData",
        "ai_trading_system.research_bench.settings.LoadedSettings",
        "ai_trading_system.research_data.qqq_long_history",
        "ai_trading_system.research_probes.entry_exit_combo",
        "ai_trading_system.research_probes.entry_timing_counts",
        "ai_trading_system.research_probes.exit_rules_after",
        "ai_trading_system.research_probes.exit_rules_counts",
        "ai_trading_system.research_probes.exit_rules_returns",
    )
    for name in imported:
        assert name.startswith(allowed), name
    banned = (
        "us_market",
        "French",
        "Mkt-RF",
        "vix",
        "VIX",
        "vxn",
        "ndx",
        "NDX",
        "entry_timing_ndx",
        "ma200_forward",
        "window_reads",
        "ledger",
        "pt.",
        "policy_table",
        ".unlink(",
        "urllib",
        "requests",
        "safe_load_yaml",
        "load_daily",
        "numpy",
        "pandas",
        "cash_return",
        "def scan",
        "def prepare",
        "def moving_average",
        "def qqq_inputs",
        "def load_data",
        "def rank",
        "def mean",
        "def exit_day",
        "def buy_back",
        "def equity",
        "def metrics",
        "def year_returns",
        "def share_text",
        "def days_text",
        "0.92",
        "0.08",
        "0.0005",
    )
    for word in banned:
        assert word not in text, word
    assert text.count(".write_text(") == 2
    for call in (
        "xa.qqq_inputs(",
        "ec.scan(",
        "ec.moving_average(",
        "xc.load_data(",
        "cb.exit_day(",
        "cb.simulate(",
        "cb.equity(",
        "cb.metrics(",
        "cb.year_returns(",
    ):
        assert call in text, call


# Swing points and shifts by hand --------------------------------------------------------------


def test_swing_points_are_strict_and_need_k_closes_on_each_side() -> None:
    close = [5.0, 3.0, 1.0, 3.0, 5.0, 1.0, 1.0, 4.0, 2.0]
    assert [s for s in range(len(close)) if ss.is_low(close, s, 2)] == [2]
    assert [s for s in range(len(close)) if ss.is_low(close, s, 1)] == [2]
    assert not ss.is_low(close, 5, 1) and not ss.is_low(close, 6, 1)
    assert not ss.is_low(close, 0, 1) and not ss.is_low(close, 8, 1)
    assert [s for s in range(len(close)) if ss.is_high(close, s, 1)] == [4, 7]
    assert [s for s in range(len(close)) if ss.is_high(close, s, 2)] == [4]
    assert not ss.is_high(close, 0, 1)
    flat = [1.0, 5.0, 5.0, 1.0]
    assert not ss.is_high(flat, 1, 1) and not ss.is_high(flat, 2, 1)
    assert ss.last_high(close, 7, 1) == 4
    assert ss.last_high(close, 8, 1) == 7
    assert ss.last_high(close, 4, 1) is None
    assert ss.last_high(close, 2, 2) is None


LINE = [100.0] * 24
HAND = [
    110.0, 108.0, 99.0, 96.0, 98.0, 99.0, 95.0, 92.0, 94.0, 97.0, 99.5, 97.0,
    95.0, 93.0, 95.0, 96.0, 99.8, 96.0, 91.0, 90.0, 92.0, 94.0, 99.8, 99.9,
]  # fmt: skip


def test_two_shifts_by_hand() -> None:
    # 3 is a swing low with no swing high before it; 7 replaces it with H = 5; entry at 10.
    # 13 is a higher low than 7 and is ignored; 19 is lower, H = 16 (17 ties 15, so it is not a
    # swing high); 22 only equals H, so the entry is 23.
    assert ss.shifts(HAND, LINE, 2, 0) == [ss.Shift(2, 7, 5, 10), ss.Shift(2, 19, 16, 23)]


def test_no_break_no_shift() -> None:
    assert ss.shifts(HAND, LINE, 2, 3) == []
    late: list[float | None] = [None, None, None, *LINE[3:]]
    assert ss.shifts(HAND, late, 2, 0) == []
    with pytest.raises(ValueError, match="k must be at least 1, got 0"):
        ss.shifts(HAND, LINE, 0, 0)


def test_a_low_on_the_line_is_not_below_it_and_a_new_break_restarts() -> None:
    line = list(LINE)
    line[7] = 92.0
    assert ss.shifts(HAND, line, 2, 0) == [ss.Shift(8, 13, 10, 16), ss.Shift(8, 19, 16, 23)]


def test_closes_equal_to_the_line_or_the_last_low_do_not_count() -> None:
    on_line = list(LINE)
    on_line[2] = 99.0
    assert ss.shifts(HAND, on_line, 2, 0) == [ss.Shift(3, 7, 5, 10), ss.Shift(3, 19, 16, 23)]
    low_on_line = list(LINE)
    low_on_line[7:11] = [92.0, 94.0, 97.0, 99.5]
    assert ss.shifts(HAND, low_on_line, 2, 0) == [
        ss.Shift(11, 13, 10, 16),
        ss.Shift(11, 19, 16, 23),
    ]
    equal_low = list(HAND)
    equal_low[18:22] = [93.0, 92.0, 93.5, 94.0]
    assert ss.shifts(equal_low, LINE, 2, 0) == [ss.Shift(2, 7, 5, 10)]


def test_a_higher_low_waits_for_a_new_break() -> None:
    higher = list(HAND)
    higher[18:22] = [94.0, 93.5, 94.5, 95.0]
    assert ss.shifts(higher, LINE, 2, 0) == [ss.Shift(2, 7, 5, 10)]
    again = [*higher, 100.3]
    again[14] = 100.2
    assert ss.shifts(again, [*LINE, 100.0], 2, 0) == [
        ss.Shift(2, 7, 5, 10),
        ss.Shift(15, 19, 14, 24),
    ]


def _path(seed: int, n: int = 700) -> list[float]:
    rng = np.random.default_rng(seed)
    return [float(v) for v in 100.0 * np.cumprod(1.0 + rng.normal(0.0002, 0.02, n))]


def _own_shifts(
    close: list[float], line: tuple[float | None, ...], k: int, start: int
) -> list[ss.Shift]:
    """An independent walk: swing sets first, then events in close order."""
    n = len(close)
    lows = {
        s
        for s in range(k, n - k)
        if close[s] == min(close[s - k : s + k + 1])
        and close[s - k : s + k + 1].count(close[s]) == 1
    }
    highs = {
        s
        for s in range(k, n - k)
        if close[s] == max(close[s - k : s + k + 1])
        and close[s - k : s + k + 1].count(close[s]) == 1
    }
    out: list[ss.Shift] = []
    cross: int | None = None
    low: int | None = None
    high: int | None = None
    floor = math.inf
    for p in range(max(start, 1), n):
        a, b = line[p - 1], line[p]
        if a is not None and b is not None and close[p - 1] >= a and close[p] < b:
            cross, low, high, floor = p, None, None, math.inf
        if cross is None:
            continue
        s = p - k
        here = line[s]
        lowest = floor if low is None else close[low]
        if s in lows and s >= cross and here is not None and close[s] < here and close[s] < lowest:
            low = s
            before = [h for h in highs if h < s]
            high = max(before) if before else None
        if low is not None and high is not None and close[p] > close[high]:
            out.append(ss.Shift(cross, low, high, p))
            floor, low, high = close[low], None, None
    return out


@pytest.mark.parametrize("seed", range(12))
def test_shifts_recomputed_and_blind_to_later_closes(seed: int) -> None:
    close = _path(seed)
    line = ec.moving_average(close)
    for k in (5, 2, 1):
        full = ss.shifts(close, line, k, 200)
        assert full == _own_shifts(close, line, k, 200)
        for m in (260, 333, 420, 517, 640):
            cut = ss.shifts(close[:m], line[:m], k, 200)
            assert cut == [s for s in full if s.entry < m]
        for move in full:
            assert move.cross <= move.low < move.entry
            assert move.high < move.low
            assert move.low + k <= move.entry
            assert close[move.entry] > close[move.high]
            assert all(close[p] <= close[move.high] for p in range(move.low + k, move.entry))


def test_the_random_paths_have_shifts() -> None:
    counts = [
        len(ss.shifts(_path(seed), ec.moving_average(_path(seed)), 5, 200)) for seed in range(12)
    ]
    assert sum(counts) >= 12


# Returns by hand ------------------------------------------------------------------------------


def test_the_outcome_with_and_without_the_stop() -> None:
    close = [10.0, 8.0, 12.0, 11.0, 7.9, 9.0, 13.0, 8.0]
    move = ss.Shift(0, 1, 0, 2)
    assert ss.outcome(close, move, 1) == ss.Outcome(11.0 / 12.0 - 1.0, 11.0 / 12.0 - 1.0, None)
    assert ss.outcome(close, move, 2) == ss.Outcome(7.9 / 12.0 - 1.0, 7.9 / 12.0 - 1.0, None)
    assert ss.outcome(close, move, 3) == ss.Outcome(9.0 / 12.0 - 1.0, 9.0 / 12.0 - 1.0, 3)
    assert ss.outcome(close, move, 4) == ss.Outcome(13.0 / 12.0 - 1.0, 9.0 / 12.0 - 1.0, 3)
    assert ss.outcome(close, move, 5) == ss.Outcome(8.0 / 12.0 - 1.0, 9.0 / 12.0 - 1.0, 3)
    assert ss.outcome(close, move, 6) is None
    level = [10.0, 8.0, 12.0, 8.0, 7.0]
    assert ss.outcome(level, move, 2) == ss.Outcome(7.0 / 12.0 - 1.0, 7.0 / 12.0 - 1.0, None)


def test_any_day_by_hand() -> None:
    days = [date(2001, 12, 28), date(2001, 12, 31), date(2002, 1, 2), date(2002, 1, 3)]
    close = [1.0, 2.0, 4.0, 2.0]
    assert ss.any_day(days, close, 0, 1) == [(days[0], 1.0), (days[1], 1.0), (days[2], -0.5)]
    assert ss.any_day(days, close, 1, 2) == [(days[1], 0.0)]
    assert ss.any_day(days, close, 2, 2) == []


def test_signal_rows_and_the_summary_by_hand() -> None:
    days = [date(2001, 1, 1) + timedelta(days=i) for i in range(5)]
    days[4] = date(2002, 1, 1)
    close = [10.0, 8.0, 12.0, 15.0, 9.0]
    line: list[float | None] = [11.0, 10.0, 12.5, 14.0, 9.0]
    move = ss.Shift(1, 1, 0, 2)
    rows = ss.signal_rows(days, close, line, [move], 0)
    assert rows == [
        {
            "cross": "2001-01-02",
            "low": "2001-01-02",
            "low_below_line": 8.0 / 10.0 - 1.0,
            "high": "2001-01-01",
            "entry": "2001-01-03",
            "entry_vs_line": 12.0 / 12.5 - 1.0,
            "risk": 0.5,
            "h20": None,
            "h60": None,
        }
    ]
    with pytest.raises(ValueError, match="缺 200 日线"):
        ss.signal_rows(days, close, [None] * 5, [move], 0)
    cells: list[dict[str, Any]] = [
        {"h20": {"plain": 0.1, "stop": -0.05, "stop_day": 3, "year_mean": 0.0}},
        {"h20": {"plain": -0.02, "stop": -0.02, "stop_day": None, "year_mean": -0.03}},
        {"h20": {"plain": 0.0, "stop": 0.0, "stop_day": None, "year_mean": 0.0}},
        {"h20": None},
    ]
    every = [(days[0], 0.2), (days[1], -0.1), (days[2], 0.0), (days[3], 0.3)]
    got = ss.horizon_summary(cells, 20, every)
    assert got == {
        "signals": 4,
        "settled": 3,
        "plain_mean": pytest.approx(0.08 / 3),
        "stop_mean": pytest.approx(-0.07 / 3),
        "year_mean": pytest.approx(-0.01),
        "plain_up": 1,
        "stop_up": 0,
        "plain_beats_year": 2,
        "stop_beats_year": 1,
        "stopped": 1,
        "any_day_n": 4,
        "any_day_mean": pytest.approx(0.1),
        "any_day_up_share": 0.5,
    }
    empty = ss.horizon_summary([], 60, [])
    assert empty["settled"] == 0 and empty["plain_mean"] is None
    assert empty["any_day_up_share"] is None


# Strategy paths by hand -----------------------------------------------------------------------


def _prep(close: list[float], sma: list[float | None] | None = None) -> xc.Prepared:
    n = len(close)
    dates = tuple(date(2001, 1, 1) + timedelta(days=i) for i in range(n))
    averages = tuple(sma) if sma is not None else (None,) * n
    return xc.Prepared(dates, tuple(close), averages, (None,) * n, (0,) * n, (False,) * n)


ROUTE = [100.0, 110.0, 100.0, 99.0, 95.0, 97.0, 96.0, 94.5, 93.0, 99.0, 98.0, 97.0, 96.0]


def test_a_shift_buy_back_and_its_low_stop_by_hand() -> None:
    data = _prep(ROUTE)
    move = ss.Shift(1, 4, 1, 5)
    route = ss.simulate("x2_s5", data, [], [move], 0)
    assert route.buys == (0, 5)
    assert route.sells == (3, 8)
    d = data.dates
    assert route.trips == (
        {
            "decision": d[2].isoformat(),
            "reason": "X2",
            "sell": d[3].isoformat(),
            "buy": d[5].isoformat(),
            "how": "shift",
            "days_out": 2,
            "change": 97.0 / 99.0 - 1.0,
        },
        {
            "decision": d[7].isoformat(),
            "reason": "L",
            "sell": d[8].isoformat(),
            "buy": None,
            "how": "none",
            "days_out": 4,
            "change": 96.0 / 93.0 - 1.0,
        },
    )
    assert route.held == tuple(1 <= p <= 3 or 6 <= p <= 8 for p in range(len(ROUTE)))
    values = cb.equity(data.close, route, 0)
    assert values[-1] == pytest.approx(0.9995**4 * 0.99 * (93.0 / 97.0), rel=1e-12)


def test_the_entry_wins_a_tie_and_drops_the_low_stop() -> None:
    data = _prep(ROUTE)
    route = ss.simulate("x2_s2", data, [5], [ss.Shift(1, 4, 1, 5)], 0)
    assert [t["how"] for t in route.trips] == ["entry"]
    assert route.sells == (3,)
    earlier = ss.simulate("x2_s2", data, [9], [ss.Shift(1, 4, 1, 5)], 0)
    assert [t["how"] for t in earlier.trips] == ["shift", "entry"]
    assert earlier.buys == (0, 5, 9)
    again = ss.simulate("x2_s2", _prep([*ROUTE, 94.0, 95.0]), [9], [ss.Shift(1, 4, 1, 5)], 0)
    assert (again.sells, again.buys) == ((3, 8), (0, 5, 9))
    same_close = ss.simulate("x2_s2", data, [], [ss.Shift(1, 2, 1, 3), ss.Shift(1, 4, 1, 5)], 0)
    assert (same_close.sells, same_close.buys) == ((3, 8), (0, 5))
    higher = ss.simulate("x2_s2", data, [], [ss.Shift(1, 2, 1, 5)], 0)
    assert [(t["how"], t["reason"]) for t in higher.trips] == [("shift", "X2"), ("none", "L")]
    assert higher.sells == (3, 7)


def test_the_exit_takes_a_tie_with_the_low_stop() -> None:
    close = [100.0, 110.0, 100.0, 99.0, 95.0, 101.2, 91.0, 93.0]
    data = _prep(close)
    route = ss.simulate("x2_s5", data, [], [ss.Shift(1, 4, 1, 5)], 0)
    assert [(t["reason"], t["decision"]) for t in route.trips] == [
        ("X2", data.dates[2].isoformat()),
        ("X2", data.dates[6].isoformat()),
    ]
    alone = ss.simulate("x3_s5", data, [], [ss.Shift(1, 4, 1, 5)], 0)
    assert [t["reason"] for t in alone.trips] == ["stop"]


def test_the_plain_paths_are_the_118_paths_and_unknown_strategies_fail() -> None:
    data = _prep(ROUTE)
    for strategy in (*PLAIN, "hold"):
        got = ss.simulate(strategy, data, [6], [ss.Shift(1, 4, 1, 5)], 0)
        assert got == cb.simulate(strategy, data, [6], 0)
    for strategy in ("x2_b", "x2_s3", "x2_s5_c"):
        with pytest.raises(ValueError, match=f"unknown strategy {strategy}"):
            ss.simulate(strategy, data, [], [], 0)


# Synthetic QQQ ruler --------------------------------------------------------------------------


def _pos(index: pd.DatetimeIndex, day: str) -> int:
    position = index.get_loc(pd.Timestamp(day))
    assert isinstance(position, int)
    return position


def _qqq_data() -> BenchData:
    index = pd.bdate_range("1998-01-02", "2012-12-31", name="date")
    rng = np.random.default_rng(11)
    level = 100.0 * np.cumprod(1.0003 * (1.0 + rng.normal(0.0, 0.015, len(index))))
    for first, last, depth in (
        ("2000-04-03", "2000-05-24", 0.7),
        ("2008-09-15", "2008-11-20", 0.6),
    ):
        a, b = _pos(index, first), _pos(index, last)
        level[a : b + 1] *= np.linspace(1.0, depth, b - a + 1)
        level[b + 1 :] *= depth
    returns = np.empty(len(index))
    returns[0] = 0.0
    returns[1:] = level[1:] / level[:-1] - 1.0
    returns[index <= pd.Timestamp("1999-03-10")] = np.nan
    daily = pd.DataFrame(
        {
            "qqq_proxy_return": returns,
            "etf_reference_return": returns,
            "cash_return": 0.0001,
            "lev2_return": 2 * returns,
            "lev3_return": 3 * returns,
            "ndx_price": level,
            "ndx_tr": level,
            "vix": 20.0,
            "vxn": 25.0,
            "vxo": 21.0,
            "cash_rate_annual": 0.03,
        },
        index=index,
    )
    data = from_daily_frame(
        daily, qqq_series="actual", source={"snapshot_id": PLAN.data.snapshot_id}
    )
    return gp.cut(with_signal_sources(data, {}, {}, snapshot_id=PLAN.data.signal_snapshot_id))


QQQ = _qqq_data()
STEP0 = xc.check_inputs(QQQ, LOADED, GATE)
SERIES = ec.closes(QQQ.returns["qqq"], date(2010, 12, 31))
PREPARED = xc.prepare(SERIES.dates, SERIES.close)
LONG = ec.moving_average(SERIES.close)
FIRST_IN = next(p for p, day in enumerate(SERIES.dates) if day >= date(1999, 3, 10))
ENTRIES = [t.entry for t in ec.scan(SERIES.close, FIRST_IN) if t.counted and t.entry is not None]
S0 = max(FIRST_IN, ec.SMA_DAYS)
FOUND = tuple(SERIES.dates[e].isoformat() for e in ENTRIES)
MOVES = {k: _own_shifts(list(SERIES.close), LONG, k, S0) for k in (5, 2)}


def _same(mp: pytest.MonkeyPatch) -> None:
    a = STEP0["entry_set_a"]
    mp.setattr(xr, "STEP0_TRADES", a["trades"])
    mp.setattr(xr, "STEP0_FIRST", a["first"])
    mp.setattr(xr, "STEP0_LAST", a["last"])
    mp.setattr(xr, "STEP0_K", STEP0["scale"]["k"])
    mp.setattr(xr, "H_MATCH", {r: STEP0["exits"][r]["h_match"] for r in xc.RULES})
    mp.setattr(cb, "V0_ENTRIES", FOUND)


with pytest.MonkeyPatch.context() as _mp:
    _same(_mp)
    TABLE = ss.run_table(QQQ, LOADED, GATE)
    TABLE_118 = cb.run_table(QQQ, LOADED, GATE)


@pytest.fixture
def same_data(monkeypatch: pytest.MonkeyPatch) -> None:
    _same(monkeypatch)


def test_the_synthetic_data_are_not_the_real_ones() -> None:
    assert STEP0["entry_set_a"]["trades"] != xr.STEP0_TRADES
    assert FOUND != cb.V0_ENTRIES
    assert len(FOUND) >= 2
    assert len(MOVES[5]) >= 4 and len(MOVES[2]) > len(MOVES[5])


# Checks --------------------------------------------------------------------------------------


def test_the_real_constants_stop_the_table() -> None:
    result = ss.run_table(QQQ, LOADED, GATE)
    assert result["complete"] is False
    assert result["check"]["qqq"]["ok"] is False
    assert result["check"]["entries"]["ok"] is False
    assert list(result) == ["run_id", "owner_decision", "source", "rule", "check", "complete"]


def test_each_check_alone_stops_the_table(same_data: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cb, "V0_ENTRIES", FOUND[:-1])
    one = ss.run_table(QQQ, LOADED, GATE)
    assert (one["check"]["qqq"]["ok"], one["check"]["entries"]["ok"]) == (True, False)
    assert "span" not in one and "signals" not in one and one["complete"] is False
    monkeypatch.setattr(cb, "V0_ENTRIES", FOUND)
    monkeypatch.setattr(xr, "STEP0_K", STEP0["scale"]["k"] * 1.001)
    two = ss.run_table(QQQ, LOADED, GATE)
    assert (two["check"]["qqq"]["ok"], two["check"]["entries"]["ok"]) == (False, True)
    assert "span" not in two and "signals" not in two and two["complete"] is False


def test_the_checks_and_the_rule_block() -> None:
    assert TABLE["check"] == TABLE_118["check"]
    assert TABLE["source"] == {"snapshot_id": PLAN.data.snapshot_id}
    assert (TABLE["run_id"], TABLE["owner_decision"]) == (ss.RUN_ID, ss.OWNER_DECISION)
    assert TABLE["rule"] == {
        "swings": [5, 2],
        "swing": "close_strictly_beyond_k_closes_each_side_known_k_closes_later",
        "break": "close_below_sma200_after_close_at_or_above",
        "low": "swing_low_below_sma200_since_break_lower_than_current_low_and_last_entry_low",
        "high": "latest_swing_high_before_low",
        "shift_entry": "first_close_above_high_close",
        "shift_stop": "close_below_low_close_sell_next_close",
        "horizons": [20, 60],
        "long_sma_days": 200,
        "strategies": [*SHIFTED, *PLAIN, "hold"],
        "entry": "v0_close",
        "start": "full",
        "trail": 0.08,
        "stop": 0.08,
        "sma_days": 50,
        "sell": "next_close",
        "buy_back_a": "entry_close",
        "buy_back_shift": "first_of_entry_close_or_shift_entry_close_tie_is_entry",
        "flat_return": 0.0,
        "cost": 0.0005,
        "tail_days": 20,
        "tail": 0.05,
        "sessions_per_year": 252,
    }
    assert TABLE["complete"] is True
    assert list(TABLE) == [
        "run_id",
        "owner_decision",
        "source",
        "rule",
        "check",
        "complete",
        "signals",
        "span",
    ]


# Signals recomputed ---------------------------------------------------------------------------


@pytest.mark.parametrize("k", [5, 2])
def test_each_signal_row_recomputed(k: int) -> None:
    close, dates = SERIES.close, SERIES.dates
    rows = TABLE["signals"][f"k{k}"]["rows"]
    assert [r["entry"] for r in rows] == [dates[m.entry].isoformat() for m in MOVES[k]]
    for row, move in zip(rows, MOVES[k], strict=True):
        line_low, line_entry = LONG[move.low], LONG[move.entry]
        assert line_low is not None and line_entry is not None
        assert list(row) == [
            "cross",
            "low",
            "low_below_line",
            "high",
            "entry",
            "entry_vs_line",
            "risk",
            "h20",
            "h60",
        ]
        assert (row["cross"], row["low"], row["high"]) == (
            dates[move.cross].isoformat(),
            dates[move.low].isoformat(),
            dates[move.high].isoformat(),
        )
        assert row["low_below_line"] == pytest.approx(close[move.low] / line_low - 1.0, rel=1e-12)
        assert row["entry_vs_line"] == pytest.approx(
            close[move.entry] / line_entry - 1.0, rel=1e-12, abs=1e-15
        )
        assert row["risk"] == pytest.approx(close[move.entry] / close[move.low] - 1.0, rel=1e-12)
        assert row["low_below_line"] < 0.0 < row["risk"]
        for h in (20, 60):
            cell = row[f"h{h}"]
            e = move.entry
            if e + h > len(close) - 1:
                assert cell is None
                continue
            under = next((p for p in range(e + 1, e + h) if close[p] < close[move.low]), None)
            stop_at = e + h if under is None else under + 1
            year = [
                close[d + h] / close[d] - 1.0
                for d in range(S0, len(close) - h)
                if dates[d].year == dates[e].year
            ]
            assert cell["plain"] == pytest.approx(close[e + h] / close[e] - 1.0, rel=1e-12)
            assert cell["stop"] == pytest.approx(close[stop_at] / close[e] - 1.0, rel=1e-12)
            assert cell["stop_day"] == (None if under is None else under + 1 - e)
            assert cell["year_mean"] == pytest.approx(sum(year) / len(year), rel=1e-9)


@pytest.mark.parametrize("k", [5, 2])
def test_each_signal_summary_recomputed(k: int) -> None:
    block = TABLE["signals"][f"k{k}"]
    for h in (20, 60):
        s = block["summary"][f"h{h}"]
        cells = [r[f"h{h}"] for r in block["rows"] if r[f"h{h}"] is not None]
        every = [
            SERIES.close[d + h] / SERIES.close[d] - 1.0 for d in range(S0, len(SERIES.close) - h)
        ]
        assert (s["signals"], s["settled"]) == (len(block["rows"]), len(cells))
        assert s["plain_mean"] == pytest.approx(sum(c["plain"] for c in cells) / len(cells))
        assert s["stop_mean"] == pytest.approx(sum(c["stop"] for c in cells) / len(cells))
        assert s["year_mean"] == pytest.approx(sum(c["year_mean"] for c in cells) / len(cells))
        assert s["stopped"] == sum(c["stop_day"] is not None for c in cells)
        assert s["plain_beats_year"] == sum(c["plain"] > c["year_mean"] for c in cells)
        assert s["any_day_n"] == len(every)
        assert s["any_day_mean"] == pytest.approx(sum(every) / len(every), rel=1e-9)
        assert s["any_day_up_share"] == sum(v > 0 for v in every) / len(every)
    assert list(block["summary"]) == ["h20", "h60"]


# Every strategy recomputed day by day ---------------------------------------------------------


def _own(strategy: str) -> dict[str, Any]:
    """An independent day-by-day state machine for version A with the shift buy-back."""
    close, sma, dates = PREPARED.close, PREPARED.sma, PREPARED.dates
    n = len(close)
    rule, _, k = strategy.partition("_s")
    lows = {m.entry: m.low for m in MOVES[int(k)]}
    holding, buy, high = True, S0, close[S0]
    first = sma[S0]
    armed = first is not None and close[S0] >= first
    floor: float | None = None
    pending: tuple[int, str] | None = None
    sold = -1
    value = 0.9995
    values = [value]
    held_days = 0
    trips: list[dict[str, Any]] = []
    for p in range(S0 + 1, n):
        if holding:
            value *= close[p] / close[p - 1]
            held_days += 1
            if pending is not None:
                value *= 0.9995
                trips.append(
                    {
                        "decision": dates[pending[0]].isoformat(),
                        "reason": pending[1],
                        "sell": dates[p].isoformat(),
                    }
                )
                holding, sold, pending = False, p, None
                values.append(value)
                continue
            high = max(high, close[p])
            line, before = sma[p], sma[p - 1]
            if line is not None and close[p] >= line:
                armed = True
            below = line is not None and close[p] < line
            below_before = before is not None and close[p - 1] < before
            if rule != "x3" and close[p] / high <= 0.92:
                pending = (p, "X2")
            elif rule != "x2" and armed and below and below_before:
                pending = (p, "X3")
            elif rule != "x2" and not armed and close[p] / close[buy] <= 0.92:
                pending = (p, "stop")
            elif floor is not None and close[p] < floor:
                pending = (p, "L")
        else:
            how = None
            if p in ENTRIES:
                how, floor = "entry", None
            elif p in lows:
                how, floor = "shift", close[lows[p]]
            if how is not None:
                value *= 0.9995
                trips[-1].update(
                    {
                        "buy": dates[p].isoformat(),
                        "how": how,
                        "days_out": p - sold,
                        "change": close[p] / close[sold] - 1.0,
                    }
                )
                holding, buy, high = True, p, close[p]
                line = sma[p]
                armed = line is not None and close[p] >= line
        values.append(value)
    if not holding:
        trips[-1].update(
            {
                "buy": None,
                "how": "none",
                "days_out": n - 1 - sold,
                "change": close[n - 1] / close[sold] - 1.0,
            }
        )
    return {"values": values, "held_days": held_days, "trips": trips}


KEYS = ["decision", "reason", "sell", "buy", "how", "days_out", "change"]


@pytest.mark.parametrize("strategy", SHIFTED)
def test_each_shift_strategy_recomputed(strategy: str) -> None:
    own = _own(strategy)
    row = TABLE["span"]["strategies"][strategy]
    assert len(row["trips"]) == len(own["trips"])
    for got, want in zip(row["trips"], own["trips"], strict=True):
        assert list(got) == KEYS
        assert {k: v for k, v in got.items() if k != "change"} == {
            k: v for k, v in want.items() if k != "change"
        }
        assert got["change"] == pytest.approx(want["change"], rel=1e-12, abs=1e-15)
    values = own["values"]
    assert row["total"] == pytest.approx(values[-1] - 1.0, rel=1e-9, abs=1e-12)
    years = (SERIES.dates[-1] - SERIES.dates[S0]).days / 365.25
    assert row["cagr"] == pytest.approx(values[-1] ** (1.0 / years) - 1.0, rel=1e-9, abs=1e-12)
    peak, worst = values[0], 0.0
    for value in values:
        peak = max(peak, value)
        worst = min(worst, value / peak - 1.0)
    assert row["max_drawdown"] == pytest.approx(worst, rel=1e-9, abs=1e-12)
    tails = sorted(values[i] / values[i - 20] - 1.0 for i in range(20, len(values)))
    assert row["worst20"] == pytest.approx(tails[math.ceil(0.05 * len(tails)) - 1], abs=1e-12)
    sessions = len(SERIES.dates) - 1 - S0
    assert row["in_market"] == pytest.approx(own["held_days"] / sessions, rel=1e-15)
    trips = own["trips"]
    assert row["sells"] == len(trips)
    for how, key in (
        ("entry", "by_entry"),
        ("shift", "by_shift"),
        ("recovery", "by_recovery"),
        ("none", "not_back"),
    ):
        assert row[key] == sum(t["how"] == how for t in trips)
    assert row["low_stops"] == sum(t["reason"] == "L" for t in trips)


def test_each_year_recomputed() -> None:
    years = TABLE["span"]["years"]
    for strategy in SHIFTED:
        values = _own(strategy)["values"]
        base = 1.0
        for row in years:
            last = max(i for i, d in enumerate(SERIES.dates[S0:]) if d.year == row["year"])
            assert row[strategy] == pytest.approx(values[last] / base - 1.0, rel=1e-9, abs=1e-12)
            base = values[last]
    assert list(years[0]) == ["year", *ss.STRATEGIES]


def test_the_plain_rows_equal_the_118_table() -> None:
    span, old = TABLE["span"], TABLE_118["span"]
    for key in ("first_day", "last_day", "sessions", "entries"):
        assert span[key] == old[key]
    for strategy in (*PLAIN, "hold"):
        row = dict(span["strategies"][strategy])
        assert (row.pop("by_shift"), row.pop("low_stops")) == (0, 0)
        assert row == old["strategies"][strategy]
    for new, was in zip(span["years"], old["years"], strict=True):
        assert {k: new[k] for k in ("year", *PLAIN, "hold")} == {
            k: was[k] for k in ("year", *PLAIN, "hold")
        }


def test_the_synthetic_table_exercises_every_case() -> None:
    rows = TABLE["span"]["strategies"]
    reasons = {t["reason"] for s in SHIFTED for t in rows[s]["trips"]}
    hows = {t["how"] for s in SHIFTED for t in rows[s]["trips"]}
    assert reasons == {"X2", "X3", "stop", "L"}
    assert hows == {"entry", "shift", "none"}
    assert all(rows[s]["by_shift"] > 0 for s in SHIFTED)
    stopped = [c for k in (5, 2) for r in TABLE["signals"][f"k{k}"]["rows"] for c in (r["h20"],)]
    assert any(c is not None and c["stop_day"] is not None for c in stopped)
    assert any(c is not None and c["stop_day"] is None for c in stopped)


def test_no_nan_reaches_the_table() -> None:
    text = json.dumps(TABLE, allow_nan=False)
    assert "NaN" not in text and "Infinity" not in text


# Text -----------------------------------------------------------------------------------------


def test_the_stop_text() -> None:
    assert ss.stop_text(None) == "—"
    assert ss.stop_text({"stop": -0.031, "stop_day": None}) == "-3.10%"
    assert ss.stop_text({"stop": -0.031, "stop_day": 4}) == "-3.10%（第 4 天卖）"


def test_the_markdown() -> None:
    text = ss.render_markdown(TABLE)
    lines = text.split("\n")
    assert lines[0] == "# QQQ 跌破 200 日线后的结构转变入场（TRADING-2565-120）"
    assert lines[2] == ss.RULE_TEXT
    assert ss.NOTE in lines
    assert text.endswith("\n") and not text.endswith("\n\n")
    for k in (5, 2):
        rows = TABLE["signals"][f"k{k}"]["rows"]
        assert f"## 左右各 {k} 天：入场 {len(rows)} 次" in lines
        first = rows[0]
        cells = [
            first["cross"],
            first["low"],
            xr.pct(first["low_below_line"]),
            first["high"],
            first["entry"],
            xr.pct(first["entry_vs_line"]),
            xr.pct(first["risk"]),
            xr.pct(first["h20"]["plain"]),
            ss.stop_text(first["h20"]),
            xr.pct(first["h20"]["year_mean"]),
            xr.pct(first["h60"]["plain"]),
            ss.stop_text(first["h60"]),
            xr.pct(first["h60"]["year_mean"]),
        ]
        assert "| " + " | ".join(cells) + " |" in lines
        s = TABLE["signals"][f"k{k}"]["summary"]["h60"]
        assert (
            f"| 60 天 | {s['signals']} | {s['settled']} | {xr.pct(s['plain_mean'])} | "
            f"{xr.pct(s['stop_mean'])} | {xr.pct(s['year_mean'])} | {s['plain_up']} | "
            f"{s['stop_up']} | {s['plain_beats_year']} | {s['stop_beats_year']} | "
            f"{s['stopped']} | {xr.pct(s['any_day_mean'])} | "
            f"{cb.share_text(s['any_day_up_share'])} |"
        ) in lines
    signal_lines = sum(len(TABLE["signals"][f"k{k}"]["rows"]) for k in (5, 2))
    dated = [line for line in lines if line[2:6].isdigit() and line[6:7] == "-"]
    assert len(dated) == signal_lines
    assert all(line.count(" | ") == 12 for line in dated)
    assert lines.count("|" + "---|" * 13) == 4
    assert "|" + "---|" * 14 in lines
    assert "|" + "---|" * 8 in lines
    span = TABLE["span"]
    assert f"## 策略：{ss.SPAN_TEXT}" in lines
    head = (
        f"满仓起点 {span['first_day']}，到 {span['last_day']}，共 {span['sessions']} 个交易日。"
        f"v0_close 入场 {len(FOUND)} 次：{'、'.join(FOUND)}。"
    )
    assert head in lines
    hold = span["strategies"]["hold"]
    assert (
        f"| 买入持有 | {xr.pct(hold['total'])} | {xr.pct(hold['cagr'])} | "
        f"{xr.pct(hold['max_drawdown'])} | {hold['drawdown_peak']} → {hold['drawdown_trough']} | "
        f"{xr.pct(hold['worst20'])} | 100% | {xr.pct(hold['in_market_cagr'])} | 0 | 0 / 0 / 0 | "
        "0 | 0 | — | — |"
    ) in lines
    row = span["strategies"]["x3_s5"]
    assert any(
        line.startswith(f"| {ss.STRATEGY_TEXT['x3_s5']} | {xr.pct(row['total'])} |")
        and f"| {row['by_entry']} / {row['by_shift']} / {row['not_back']} | {row['low_stops']} |"
        in line
        for line in lines
    )
    year_lines = [line for line in lines if line[2:6].isdigit() and line[6:9] == " | "]
    assert len(year_lines) == len(span["years"])
    shown = ("x2_s5", "x3_s5", "x2x3_s5", *PLAIN, "hold")
    first_year = span["years"][0]
    assert year_lines[0] == (
        f"| {first_year['year']} | " + " | ".join(xr.pct(first_year[s]) for s in shown) + " |"
    )
    trips = sum(len(span["strategies"][s]["trips"]) for s in SHIFTED)
    assert sum(1 for line in lines if line.count(" | ") == 7 and line.startswith("| X")) == trips
    trip = next(t for t in span["strategies"]["x2_s2"]["trips"] if t["how"] == "shift")
    assert (
        f"| {ss.STRATEGY_TEXT['x2_s2']} | {trip['decision']} | {trip['reason']} | "
        f"{trip['sell']} | {trip['buy'] or '—'} | 结构转变 | "
        f"{trip['days_out']} | {xr.pct(trip['change'])} |"
    ) in lines


def test_the_summary_line(tmp_path: Path) -> None:
    rows = TABLE["span"]["strategies"]
    counts = (
        f"左右各 5 天 {len(TABLE['signals']['k5']['rows'])} 次、"
        f"左右各 2 天 {len(TABLE['signals']['k2']['rows'])} 次"
    )
    parts = "、".join(
        f"{ss.STRATEGY_TEXT[s]} 年化 {xr.pct(rows[s]['cagr'])} 回撤 "
        f"{xr.pct(rows[s]['max_drawdown'])}"
        for s in ("x2_s5", "x3_s5", "x2x3_s5", "hold")
    )
    assert ss.summary_line(TABLE, tmp_path) == (
        f"entry_structure_shift: {ss.SPAN_TEXT} 结构转变入场 {counts}；{parts}；结果目录 {tmp_path}"
    )


# Command line --------------------------------------------------------------------------------


@pytest.fixture
def loaded_cli(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []

    def load_qqq(root: Path) -> Any:
        calls.append(f"qqq {root}")
        return QQQ, GATE, LOADED

    monkeypatch.setattr(xc, "load_data", load_qqq)
    return calls


def test_run_writes_both_files_and_prints_one_line(
    loaded_cli: list[str], same_data: None, tmp_path: Path
) -> None:
    out = tmp_path / "out"
    result = CliRunner().invoke(ss.app, ["run", "--out", str(out), "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert loaded_cli == [f"qqq {tmp_path}"]
    text = out.joinpath("entry_structure_shift.json").read_text(encoding="utf-8")
    assert text == json.dumps(TABLE, indent=2, ensure_ascii=False) + "\n"
    assert out.joinpath("entry_structure_shift.md").read_text(encoding="utf-8") == (
        ss.render_markdown(TABLE)
    )
    names = sorted(p.name for p in out.iterdir())
    assert names == ["entry_structure_shift.json", "entry_structure_shift.md"]
    assert result.output == ss.summary_line(TABLE, out) + "\n"


@pytest.mark.parametrize("name", ["entry_structure_shift.json", "entry_structure_shift.md"])
def test_run_refuses_an_existing_result_without_loading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str
) -> None:
    def boom(root: Path) -> Any:
        raise AssertionError("loaded")

    monkeypatch.setattr(xc, "load_data", boom)
    tmp_path.joinpath(name).write_text("{}", encoding="utf-8")
    result = CliRunner().invoke(ss.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "entry_structure_shift: 已有结果，拒绝覆盖。\n"
    assert [p.name for p in tmp_path.iterdir()] == [name]


@pytest.mark.parametrize(
    "error", [tr.TrialError("snapshot is not checked\nmore"), OSError("gone"), KeyError("qqq")]
)
def test_a_load_failure_writes_nothing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, error: Exception
) -> None:
    def fail(root: Path) -> Any:
        raise error

    monkeypatch.setattr(xc, "load_data", fail)
    out = tmp_path / "out"
    result = CliRunner().invoke(ss.app, ["run", "--out", str(out)])
    assert result.exit_code == 1
    assert result.output == f"entry_structure_shift: 数据载入失败：{str(error).splitlines()[0]}\n"
    assert not out.exists()


def test_a_failed_check_writes_only_the_json(loaded_cli: list[str], tmp_path: Path) -> None:
    result = CliRunner().invoke(ss.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "entry_structure_shift: 输入检查 FAIL，没有算入场\n"
    assert [p.name for p in tmp_path.iterdir()] == ["entry_structure_shift.json"]
    written = json.loads(
        tmp_path.joinpath("entry_structure_shift.json").read_text(encoding="utf-8")
    )
    assert written["complete"] is False
    assert "span" not in written and "signals" not in written
