"""Acceptance tests for TRADING-2565-118: a first QQQ strategy from our entry and exit signals.

Requirement: knowledge qqq-options/auto-loop/period7/entry-exit-combo/entry-exit-combo-
requirements-2026-11-03.md version 1.0 (owner decision entry_exit_combo). Synthetic data only;
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
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import exit_rules_after as xa
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_returns as xr
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import policy_table as pt

LOADED = load_settings()
GATE = tr.load_gate(PROJECT_ROOT / "research" / "_trials" / "prereg.yaml")
_, PLAN = tr.load_trial(lg.research_root(LOADED), pt.DATA_TRIAL)
MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "entry_exit_combo.py"
SIX = ("x2_a", "x2_b", "x3_a", "x3_b", "x2x3_a", "x2x3_b")


# Constants and imports ------------------------------------------------------------------------


def test_the_run_is_the_first_entry_and_exit_strategy() -> None:
    assert cb.RUN_ID == "TRADING-2565-118"
    assert cb.OWNER_DECISION == "owner_decision:TRADING-2565:2026-11-03:entry_exit_combo"
    assert cb.EXITS == ("x2", "x3", "x2x3")
    assert cb.VERSIONS == ("a", "b")
    assert cb.STRATEGIES == (*SIX, "hold")
    assert cb.TRAIL == cb.STOP == xc.STOP == 0.08
    assert (cb.COST, cb.TAIL_DAYS, cb.TAIL) == (0.0005, 20, 0.05)
    assert (cb.YEAR_DAYS, cb.SESSIONS_PER_YEAR) == (365.25, 252)
    assert cb.V0_ENTRIES == (
        "2000-05-15",
        "2000-08-09",
        "2004-03-26",
        "2004-05-05",
        "2010-05-25",
        "2010-09-10",
    )
    assert (cb.RESULT_JSON, cb.RESULT_MD) == ("entry_exit_combo.json", "entry_exit_combo.md")
    assert cb.SPAN_TEXT == xa.SPAN_TEXT["qqq"]
    assert cb.EXIT_TEXT == {"x2": "X2 回落 8%", "x3": "X3 跌破 50 日线", "x2x3": "X2 或 X3 先响"}
    assert cb.VERSION_TEXT == {"a": "A 只等入场买回", "b": "B 回到卖出价也买回"}
    assert cb.STRATEGY_TEXT["x3_b"] == "X3 跌破 50 日线，B 回到卖出价也买回"
    assert cb.STRATEGY_TEXT["hold"] == "买入持有"
    assert list(cb.STRATEGY_TEXT) == list(cb.STRATEGIES)
    assert cb.HOW_TEXT == {"entry": "入场信号", "recovery": "回到卖出价", "none": "没买回"}


def test_the_note_says_what_is_measured_and_what_is_not() -> None:
    for words in (
        "只印不判",
        "不是交易建议",
        "看过结果的历史",
        "从第一个能判入场的收盘起满仓",
        "买入持有也从同一个收盘起",
        "v0_close",
        "10-31 冻结的原规则（用最低价碰线）不一样",
        "从买进以来的最高收盘回落 8%",
        "先有一个收盘在 50 日线上或线上方才开始算",
        "收盘比买入价跌 8% 就卖",
        "谁先响谁算",
        "出场响后下一个收盘卖",
        "A 版卖出后只等入场信号",
        "B 版另外在收盘回到卖出那天的收盘价或更高时",
        "和入场同一天算入场",
        "空仓按 0 收益算，不算利息",
        "每次买卖扣 0.05%",
        "5% 分位（取最近的名次）",
        "每年 252 个交易日",
        "负数是买回更便宜",
        "到窗口末还空仓的算到最后一个收盘",
    ):
        assert words in cb.NOTE, words


def test_the_module_reads_only_the_qqq_ruler_through_the_family() -> None:
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
        "collections.",
        "datetime.",
        "pathlib.",
        "typing.",
        "typer",
        "ai_trading_system.research_bench.trial",
        "ai_trading_system.research_bench.data.BenchData",
        "ai_trading_system.research_bench.settings.LoadedSettings",
        "ai_trading_system.research_data.qqq_long_history",
        "ai_trading_system.research_probes.entry_timing_counts",
        "ai_trading_system.research_probes.exit_rules_after",
        "ai_trading_system.research_probes.exit_rules_counts",
        "ai_trading_system.research_probes.exit_rules_returns",
    )
    for name in imported:
        assert name.startswith(allowed), name
    for required in ("entry_timing_counts", "exit_rules_after", "exit_rules_counts"):
        assert f"ai_trading_system.research_probes.{required}" in imported
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
        "def closes",
        "def prepare",
        "def moving_average",
        "def qqq_inputs",
        "def load_data",
        "def rank",
        "def mean",
    )
    for word in banned:
        assert word not in text, word
    assert text.count(".write_text(") == 2
    for call in ("xa.qqq_inputs(", "ec.scan(", "xc.load_data(", "xr.rank(", "xa.mean("):
        assert call in text, call


# Exits by hand --------------------------------------------------------------------------------


def _prep(close: list[float], sma: list[float | None] | None = None) -> xc.Prepared:
    n = len(close)
    dates = tuple(date(2001, 1, 1) + timedelta(days=i) for i in range(n))
    averages = tuple(sma) if sma is not None else (None,) * n
    return xc.Prepared(dates, tuple(close), averages, (None,) * n, (0,) * n, (False,) * n)


def test_x2_fires_at_eight_percent_off_the_highest_close() -> None:
    assert cb.exit_day("x2", _prep([100.0, 110.0, 101.2]), 0) == (2, "X2")
    assert cb.exit_day("x2", _prep([100.0, 110.0, 101.21]), 0) is None
    assert cb.exit_day("x2", _prep([100.0, 95.0, 92.0]), 0) == (2, "X2")
    assert cb.exit_day("x2", _prep([90.0, 100.0, 95.0, 92.0]), 1) == (3, "X2")


def test_x3_counts_only_after_a_close_on_or_above_the_line() -> None:
    line: list[float | None] = [100.0] * 6
    data = _prep([95.0, 99.0, 98.0, 101.0, 97.0, 96.0], line)
    assert cb.exit_day("x3", data, 0) == (5, "X3")
    assert cb.exit_day("x3", _prep([100.0, 99.0, 98.0], [100.0] * 3), 0) == (2, "X3")
    assert cb.exit_day("x3", _prep([95.0, 99.0, 98.0, 97.0], line[:4]), 0) is None
    assert cb.exit_day("x3", _prep([95.0, 100.0, 99.0, 98.0], line[:4]), 0) == (3, "X3")


def test_x3_needs_two_closes_below_after_arming() -> None:
    line: list[float | None] = [100.0] * 6
    assert cb.exit_day("x3", _prep([101.0, 99.0, 101.0, 99.0, 101.0, 99.0], line), 0) is None
    data = _prep([101.0, 99.0, 101.0, 99.0, 98.0], line[:5])
    assert cb.exit_day("x3", data, 0) == (4, "X3")


def test_before_arming_a_close_eight_percent_under_the_buy_sells() -> None:
    line: list[float | None] = [110.0] * 4
    assert cb.exit_day("x3", _prep([100.0, 95.0, 92.0, 90.0], line), 0) == (2, "stop")
    assert cb.exit_day("x3", _prep([100.0, 95.0, 92.01, 93.0], line), 0) is None


def test_after_arming_the_buy_price_stop_is_gone() -> None:
    sma: list[float | None] = [104.0, 104.0, 90.0, 90.0]
    assert cb.exit_day("x3", _prep([100.0, 105.0, 91.0, 91.0], sma), 0) is None


def test_an_undefined_line_neither_arms_nor_counts_as_below() -> None:
    sma: list[float | None] = [None, None, 100.0, 100.0]
    assert cb.exit_day("x3", _prep([100.0, 101.0, 99.0, 98.0], sma), 0) is None
    sma = [None, 100.0, 100.0, 100.0]
    assert cb.exit_day("x3", _prep([95.0, 99.0, 98.0, 97.0], sma), 0) is None


def test_x2_or_x3_takes_the_earlier_and_x2_on_the_same_close() -> None:
    line: list[float | None] = [100.0] * 5
    data = _prep([101.0, 99.0, 98.0, 90.0, 89.0], line)
    assert cb.exit_day("x3", data, 0) == (2, "X3")
    assert cb.exit_day("x2", data, 0) == (3, "X2")
    assert cb.exit_day("x2x3", data, 0) == (2, "X3")
    both = _prep([101.0, 99.0, 92.0], [100.0] * 3)
    assert cb.exit_day("x3", both, 0) == (2, "X3")
    assert cb.exit_day("x2", both, 0) == (2, "X2")
    assert cb.exit_day("x2x3", both, 0) == (2, "X2")
    early = _prep([100.0, 92.0, 91.0], [110.0] * 3)
    assert cb.exit_day("x2x3", early, 0) == (1, "X2")


def test_unknown_names_are_refused() -> None:
    data = _prep([100.0, 101.0])
    with pytest.raises(ValueError, match="unknown exit x4"):
        cb.exit_day("x4", data, 0)
    with pytest.raises(ValueError, match="unknown version c"):
        cb.buy_back("c", data, [], 0)
    with pytest.raises(ValueError, match="unknown strategy x2_c"):
        cb.simulate("x2_c", data, [], 0)


# Buying back by hand --------------------------------------------------------------------------


def test_version_a_waits_for_the_next_entry_after_the_sell() -> None:
    data = _prep([100.0, 90.0, 101.0, 102.0, 95.0, 96.0])
    assert cb.buy_back("a", data, [1, 4], 1) == (4, "entry")
    assert cb.buy_back("a", data, [1], 1) is None
    assert cb.buy_back("a", data, [0, 1, 5], 2) == (5, "entry")


def test_version_b_also_buys_at_the_selling_close_or_higher() -> None:
    data = _prep([100.0, 90.0, 89.0, 90.0, 95.0, 91.0])
    assert cb.buy_back("b", data, [], 1) == (3, "recovery")
    assert cb.buy_back("b", data, [5], 1) == (3, "recovery")
    assert cb.buy_back("b", data, [2], 1) == (2, "entry")
    assert cb.buy_back("b", data, [3], 1) == (3, "entry")
    assert cb.buy_back("b", data, [], 4) is None
    assert cb.buy_back("b", data, [5], 4) == (5, "entry")


# One path by hand ------------------------------------------------------------------------------

HAND = [100.0, 110.0, 100.0, 99.0, 105.0, 111.0, 120.0, 118.0, 100.0, 101.0, 110.0, 115.0]


def test_version_b_by_hand() -> None:
    data = _prep(HAND)
    route = cb.simulate("x2_b", data, [9], 0)
    assert route.buys == (0, 4, 10)
    assert route.sells == (3, 9)
    assert route.held == (
        False,
        True,
        True,
        True,
        False,
        True,
        True,
        True,
        True,
        True,
        False,
        True,
    )
    d = data.dates
    assert route.trips == (
        {
            "decision": d[2].isoformat(),
            "reason": "X2",
            "sell": d[3].isoformat(),
            "buy": d[4].isoformat(),
            "how": "recovery",
            "days_out": 1,
            "change": 105.0 / 99.0 - 1.0,
        },
        {
            "decision": d[8].isoformat(),
            "reason": "X2",
            "sell": d[9].isoformat(),
            "buy": d[10].isoformat(),
            "how": "recovery",
            "days_out": 1,
            "change": 110.0 / 101.0 - 1.0,
        },
    )
    values = cb.equity(data.close, route, 0)
    assert len(values) == len(HAND)
    assert values[0] == pytest.approx(0.9995, rel=1e-15)
    end = 0.9995**5 * (99.0 / 100.0) * (101.0 / 105.0) * (115.0 / 110.0)
    assert values[-1] == pytest.approx(end, rel=1e-12)
    assert values[4] == pytest.approx(values[3] * 0.9995, rel=1e-15)


def test_version_a_by_hand_and_no_buy_back_at_the_end() -> None:
    data = _prep(HAND)
    route = cb.simulate("x2_a", data, [9], 0)
    assert (route.buys, route.sells) == ((0, 9), (3,))
    assert route.trips[0]["how"] == "entry"
    assert route.trips[0]["days_out"] == 6
    assert route.trips[0]["change"] == pytest.approx(101.0 / 99.0 - 1.0, rel=1e-15)
    alone = cb.simulate("x2_a", data, [], 0)
    assert (alone.buys, alone.sells) == ((0,), (3,))
    assert alone.trips[0]["buy"] is None
    assert alone.trips[0]["how"] == "none"
    assert alone.trips[0]["days_out"] == len(HAND) - 1 - 3
    assert alone.trips[0]["change"] == pytest.approx(115.0 / 99.0 - 1.0, rel=1e-15)
    assert not any(alone.held[4:])
    values = cb.equity(data.close, alone, 0)
    assert values[-1] == pytest.approx(0.9995**2 * 0.99, rel=1e-12)


def test_an_exit_on_the_last_close_is_not_sold() -> None:
    data = _prep([100.0, 110.0, 101.0])
    route = cb.simulate("x2_b", data, [], 0)
    assert (route.buys, route.sells, route.trips) == ((0,), (), ())
    assert route.held == (False, True, True)


def test_holding_is_one_buy_on_the_first_close() -> None:
    data = _prep(HAND)
    route = cb.simulate("hold", data, [2, 5], 3)
    assert (route.buys, route.sells, route.trips) == ((3,), (), ())
    assert route.held == tuple(p > 3 for p in range(len(HAND)))
    values = cb.equity(data.close, route, 3)
    assert len(values) == len(HAND) - 3
    assert values[-1] == pytest.approx(0.9995 * 115.0 / 99.0, rel=1e-12)


# Measures by hand ----------------------------------------------------------------------------


def _route(held: list[bool], trips: tuple[dict[str, Any], ...] = ()) -> cb.Route:
    return cb.Route(tuple(held), (0,), (), trips)


def test_the_measures_by_hand() -> None:
    n = 46
    days = [date(2001, 1, 1) + timedelta(days=i) for i in range(n)]
    values = [1.0] * n
    for i in range(1, n):
        values[i] = values[i - 1] * (1.01 if i % 3 else 0.97)
    values[30] = values[29] * 0.8
    for i in range(31, n):
        values[i] = values[i - 1] * 1.002
    held = [False] + [i % 2 == 0 for i in range(1, n)]
    trips = (
        {"how": "entry", "change": -0.05, "days_out": 10},
        {"how": "recovery", "change": 0.01, "days_out": 2},
        {"how": "none", "change": 0.3, "days_out": 5},
    )
    got = cb.metrics(days, values, _route(held, trips), 0)
    years = (days[-1] - days[0]).days / 365.25
    top = max(values[:30])
    tails = sorted(values[i] / values[i - 20] - 1.0 for i in range(20, n))
    held_days = sum(held[1:])
    assert got == {
        "total": values[-1] - 1.0,
        "cagr": values[-1] ** (1.0 / years) - 1.0,
        "max_drawdown": pytest.approx(values[30] / top - 1.0, rel=1e-15),
        "drawdown_peak": days[values.index(top)].isoformat(),
        "drawdown_trough": days[30].isoformat(),
        "worst20": tails[math.ceil(0.05 * len(tails)) - 1],
        "in_market": held_days / (n - 1),
        "in_market_cagr": values[-1] ** (252 / held_days) - 1.0,
        "sells": 0,
        "by_entry": 1,
        "by_recovery": 1,
        "not_back": 1,
        "cheaper": 1,
        "change_mean": pytest.approx(-0.02, rel=1e-12),
        "days_out_mean": pytest.approx(17.0 / 3.0, rel=1e-15),
    }


def test_the_first_of_equal_drawdowns_and_offsets_from_the_start() -> None:
    days = [date(2001, 1, 1) + timedelta(days=i) for i in range(30)]
    values = [1.0, 2.0, 1.0, 2.0, 1.0] + [2.0] * 21
    got = cb.metrics(days, values, _route([False] * 30), 4)
    assert got["max_drawdown"] == -0.5
    assert (got["drawdown_peak"], got["drawdown_trough"]) == (
        days[5].isoformat(),
        days[6].isoformat(),
    )
    assert got["in_market"] == 0.0
    assert got["in_market_cagr"] is None
    assert got["change_mean"] is None
    assert got["days_out_mean"] is None
    flat = cb.metrics(days, [1.0] * 26, _route([False] * 30), 4)
    assert (flat["max_drawdown"], flat["drawdown_peak"], flat["drawdown_trough"]) == (
        0.0,
        days[4].isoformat(),
        days[4].isoformat(),
    )


def test_each_year_from_the_previous_year_s_last_close() -> None:
    days = [date(2000, 12, 28), date(2000, 12, 29), date(2001, 1, 2), date(2001, 6, 1)]
    days += [date(2001, 12, 31), date(2002, 1, 2)]
    values = [0.9995, 1.1, 1.21, 1.0, 1.32]
    assert cb.year_returns(days, values, 1) == {
        2000: pytest.approx(0.9995 - 1.0, rel=1e-12),
        2001: pytest.approx(1.0 / 0.9995 - 1.0, rel=1e-12),
        2002: pytest.approx(1.32 - 1.0, rel=1e-12),
    }
    assert list(cb.year_returns(days, values, 1)) == [2000, 2001, 2002]


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
FIRST_IN = next(p for p, day in enumerate(SERIES.dates) if day >= date(1999, 3, 10))
ENTRIES = [t.entry for t in ec.scan(SERIES.close, FIRST_IN) if t.counted and t.entry is not None]
S0 = max(FIRST_IN, ec.SMA_DAYS)
FOUND = tuple(SERIES.dates[e].isoformat() for e in ENTRIES)


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
    TABLE = cb.run_table(QQQ, LOADED, GATE)


@pytest.fixture
def same_data(monkeypatch: pytest.MonkeyPatch) -> None:
    _same(monkeypatch)


def test_the_synthetic_data_are_not_the_real_ones() -> None:
    assert STEP0["entry_set_a"]["trades"] != xr.STEP0_TRADES
    assert FOUND != cb.V0_ENTRIES
    assert len(FOUND) >= 2
    assert all(e > S0 for e in ENTRIES)


# Checks --------------------------------------------------------------------------------------


def test_the_real_constants_stop_the_table_before_any_strategy() -> None:
    result = cb.run_table(QQQ, LOADED, GATE)
    assert result["complete"] is False
    assert result["check"]["qqq"]["ok"] is False
    assert result["check"]["entries"]["ok"] is False
    assert list(result) == ["run_id", "owner_decision", "source", "rule", "check", "complete"]


def test_each_check_alone_stops_the_table(same_data: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cb, "V0_ENTRIES", FOUND[:-1])
    one = cb.run_table(QQQ, LOADED, GATE)
    assert (one["check"]["qqq"]["ok"], one["check"]["entries"]["ok"], one["complete"]) == (
        True,
        False,
        False,
    )
    assert "span" not in one
    monkeypatch.setattr(cb, "V0_ENTRIES", FOUND)
    monkeypatch.setattr(xr, "STEP0_K", STEP0["scale"]["k"] * 1.001)
    two = cb.run_table(QQQ, LOADED, GATE)
    assert (two["check"]["qqq"]["ok"], two["check"]["entries"]["ok"], two["complete"]) == (
        False,
        True,
        False,
    )
    assert "span" not in two


def test_the_checks_and_the_rule_block() -> None:
    *_, q_check = xa.qqq_inputs(QQQ, LOADED, GATE)
    assert TABLE["check"]["qqq"]["window_id"] == q_check["window_id"]
    assert TABLE["check"]["entries"] == {
        "variant": "v0_close",
        "first_scan_date": SERIES.dates[S0].isoformat(),
        "entries": list(FOUND),
        "expected": list(FOUND),
        "ok": True,
    }
    assert TABLE["source"] == {"snapshot_id": PLAN.data.snapshot_id}
    assert (TABLE["run_id"], TABLE["owner_decision"]) == (cb.RUN_ID, cb.OWNER_DECISION)
    assert TABLE["rule"] == {
        "strategies": [*SIX, "hold"],
        "entry": "v0_close",
        "start": "full",
        "trail": 0.08,
        "stop": 0.08,
        "sma_days": 50,
        "sell": "next_close",
        "buy_back_a": "entry_close",
        "buy_back_b": "entry_close_or_first_close_at_or_above_sell_close",
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
        "span",
    ]


# Every strategy recomputed day by day --------------------------------------------------------


def _own(strategy: str) -> dict[str, Any]:
    """An independent day-by-day state machine for one strategy."""
    close, sma, dates = PREPARED.close, PREPARED.sma, PREPARED.dates
    n = len(close)
    rule, _, version = strategy.partition("_")
    holding, buy, high = True, S0, close[S0]
    first = sma[S0]
    armed = first is not None and close[S0] >= first
    decided: tuple[int, str] | None = None
    sold = -1
    trades = 1
    value = 0.9995
    values = [value]
    held_days = 0
    trips: list[dict[str, Any]] = []
    for p in range(S0 + 1, n):
        if holding:
            value *= close[p] / close[p - 1]
            held_days += 1
            if decided is not None:
                value *= 0.9995
                trades += 1
                holding, sold = False, p
                trips.append(
                    {
                        "decision": dates[decided[0]].isoformat(),
                        "reason": decided[1],
                        "sell": dates[p].isoformat(),
                    }
                )
                decided = None
            elif strategy != "hold":
                high = max(high, close[p])
                line, before = sma[p], sma[p - 1]
                if line is not None and close[p] >= line:
                    armed = True
                below = line is not None and close[p] < line
                below_before = before is not None and close[p - 1] < before
                if rule != "x3" and close[p] / high <= 0.92:
                    decided = (p, "X2")
                elif rule != "x2" and armed and below and below_before:
                    decided = (p, "X3")
                elif rule != "x2" and not armed and close[p] / close[buy] <= 0.92:
                    decided = (p, "stop")
        else:
            line = sma[p]
            how = None
            if p in ENTRIES:
                how = "entry"
            elif version == "b" and close[p] >= close[sold]:
                how = "recovery"
            if how is not None:
                value *= 0.9995
                trades += 1
                trips[-1].update(
                    {
                        "buy": dates[p].isoformat(),
                        "how": how,
                        "days_out": p - sold,
                        "change": close[p] / close[sold] - 1.0,
                    }
                )
                holding, buy, high = True, p, close[p]
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
    return {"values": values, "held_days": held_days, "trips": trips, "trades": trades}


@pytest.mark.parametrize("strategy", [*SIX, "hold"])
def test_each_strategy_recomputed(strategy: str) -> None:
    own = _own(strategy)
    row = TABLE["span"]["strategies"][strategy]
    assert len(row["trips"]) == len(own["trips"])
    for got, want in zip(row["trips"], own["trips"], strict=True):
        assert list(got) == ["decision", "reason", "sell", "buy", "how", "days_out", "change"]
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
    expected = values[-1] ** (252 / own["held_days"]) - 1.0
    assert row["in_market_cagr"] == pytest.approx(expected, rel=1e-9, abs=1e-12)
    trips = own["trips"]
    assert row["sells"] == len(trips)
    assert own["trades"] == 1 + len(trips) + sum(t["how"] != "none" for t in trips)
    for how, key in (("entry", "by_entry"), ("recovery", "by_recovery"), ("none", "not_back")):
        assert row[key] == sum(t["how"] == how for t in trips)
    back = [t["change"] for t in trips if t["how"] != "none"]
    assert row["cheaper"] == sum(c < 0 for c in back)
    if back:
        assert row["change_mean"] == pytest.approx(sum(back) / len(back), rel=1e-12, abs=1e-15)
    else:
        assert row["change_mean"] is None
    if trips:
        mean_out = sum(t["days_out"] for t in trips) / len(trips)
        assert row["days_out_mean"] == pytest.approx(mean_out, rel=1e-12)
    else:
        assert row["days_out_mean"] is None


def test_each_year_recomputed() -> None:
    years = TABLE["span"]["years"]
    calendar = sorted({d.year for d in SERIES.dates[S0:]})
    assert [row["year"] for row in years] == calendar
    for strategy in cb.STRATEGIES:
        values = _own(strategy)["values"]
        base = 1.0
        for row in years:
            last = max(i for i, d in enumerate(SERIES.dates[S0:]) if d.year == row["year"])
            want = values[last] / base - 1.0
            assert row[strategy] == pytest.approx(want, rel=1e-9, abs=1e-12)
            base = values[last]
        assert list(row) == ["year", *cb.STRATEGIES]


def test_the_span_header() -> None:
    span = TABLE["span"]
    assert list(span) == ["first_day", "last_day", "sessions", "entries", "strategies", "years"]
    assert span["first_day"] == SERIES.dates[S0].isoformat()
    assert span["last_day"] == SERIES.dates[-1].isoformat() == "2010-12-31"
    assert span["sessions"] == len(SERIES.dates) - 1 - S0
    assert span["entries"] == list(FOUND)
    assert list(span["strategies"]) == list(cb.STRATEGIES)
    hold = span["strategies"]["hold"]
    assert hold["in_market"] == 1.0
    assert hold["trips"] == []
    end = 0.9995 * SERIES.close[-1] / SERIES.close[S0]
    assert hold["total"] == pytest.approx(end - 1.0, rel=1e-9)


def test_the_synthetic_table_exercises_every_case() -> None:
    rows = TABLE["span"]["strategies"]
    reasons = {t["reason"] for s in SIX for t in rows[s]["trips"]}
    hows = {t["how"] for s in SIX for t in rows[s]["trips"]}
    assert reasons == {"X2", "X3", "stop"}
    assert hows == {"entry", "recovery", "none"}
    assert all(t["how"] != "recovery" for s in SIX if s.endswith("_a") for t in rows[s]["trips"])
    assert all(t["reason"] != "stop" for t in rows["x2_b"]["trips"] + rows["x2x3_b"]["trips"])
    assert any(t["change"] < 0 for s in SIX for t in rows[s]["trips"] if t["how"] == "entry")


def test_no_nan_reaches_the_table() -> None:
    text = json.dumps(TABLE, allow_nan=False)
    assert "NaN" not in text and "Infinity" not in text


# Text -----------------------------------------------------------------------------------------


def test_the_small_texts() -> None:
    assert cb.share_text(None) == "—"
    assert cb.share_text(0.514) == "51%"
    assert cb.days_text(None) == "—"
    assert cb.days_text(42.6) == "43"


def test_the_markdown() -> None:
    text = cb.render_markdown(TABLE)
    lines = text.split("\n")
    assert lines[0] == "# QQQ 入场加出场：第一版策略（TRADING-2565-118）"
    assert lines[2] == cb.NOTE
    assert f"## {cb.SPAN_TEXT}" in lines
    span = TABLE["span"]
    head = (
        f"满仓起点 {span['first_day']}，到 {span['last_day']}，共 {span['sessions']} 个交易日。"
        f"入场信号 {len(FOUND)} 次：{'、'.join(FOUND)}。"
    )
    assert head in lines
    assert text.endswith("\n") and not text.endswith("\n\n")
    hold = span["strategies"]["hold"]
    assert (
        f"| 买入持有 | {xr.pct(hold['total'])} | {xr.pct(hold['cagr'])} | "
        f"{xr.pct(hold['max_drawdown'])} | {hold['drawdown_peak']} → {hold['drawdown_trough']} | "
        f"{xr.pct(hold['worst20'])} | 100% | {xr.pct(hold['in_market_cagr'])} | 0 | 0 / 0 / 0 | "
        "0 | — | — |"
    ) in lines
    assert "|" + "---|" * 13 in lines
    assert "|" + "---|" * 8 in lines
    trips = sum(len(span["strategies"][s]["trips"]) for s in SIX)
    rows = [line for line in lines if line.startswith("| " + cb.STRATEGY_TEXT["x2_a"])]
    assert len(rows) == 1 + len(span["strategies"]["x2_a"]["trips"])
    assert sum(1 for line in lines if line.count(" | ") == 7 and line.startswith("| X")) == trips
    year_lines = [line for line in lines if line[:4] == "| 19" or line[:4] == "| 20"]
    assert len(year_lines) == len(span["years"])
    first = span["years"][0]
    assert year_lines[0] == (
        f"| {first['year']} | " + " | ".join(xr.pct(first[s]) for s in cb.STRATEGIES) + " |"
    )
    trip = span["strategies"]["x2_a"]["trips"][0]
    assert (
        f"| {cb.STRATEGY_TEXT['x2_a']} | {trip['decision']} | {trip['reason']} | {trip['sell']} | "
        f"{trip['buy'] or '—'} | {cb.HOW_TEXT[trip['how']]} | {trip['days_out']} | "
        f"{xr.pct(trip['change'])} |"
    ) in lines
    assert "每年（从上一年最后一个收盘算，第一年从满仓起点算）：" in lines
    assert "每次卖出和买回（没买回的差价算到最后一个收盘）：" in lines


def test_the_summary_line(tmp_path: Path) -> None:
    rows = TABLE["span"]["strategies"]
    parts = "、".join(
        f"{cb.STRATEGY_TEXT[s]} 年化 {xr.pct(rows[s]['cagr'])} 回撤 "
        f"{xr.pct(rows[s]['max_drawdown'])}"
        for s in cb.STRATEGIES
    )
    assert cb.summary_line(TABLE, tmp_path) == (
        f"entry_exit_combo: {cb.SPAN_TEXT} 满仓起步，入场 {len(FOUND)} 次；{parts}；"
        f"结果目录 {tmp_path}"
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
    result = CliRunner().invoke(cb.app, ["run", "--out", str(out), "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert loaded_cli == [f"qqq {tmp_path}"]
    text = out.joinpath("entry_exit_combo.json").read_text(encoding="utf-8")
    assert text == json.dumps(TABLE, indent=2, ensure_ascii=False) + "\n"
    assert out.joinpath("entry_exit_combo.md").read_text(encoding="utf-8") == (
        cb.render_markdown(TABLE)
    )
    assert sorted(p.name for p in out.iterdir()) == ["entry_exit_combo.json", "entry_exit_combo.md"]
    assert result.output == cb.summary_line(TABLE, out) + "\n"


@pytest.mark.parametrize("name", ["entry_exit_combo.json", "entry_exit_combo.md"])
def test_run_refuses_an_existing_result_without_loading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str
) -> None:
    def boom(root: Path) -> Any:
        raise AssertionError("loaded")

    monkeypatch.setattr(xc, "load_data", boom)
    tmp_path.joinpath(name).write_text("{}", encoding="utf-8")
    result = CliRunner().invoke(cb.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "entry_exit_combo: 已有结果，拒绝覆盖。\n"
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
    result = CliRunner().invoke(cb.app, ["run", "--out", str(out)])
    assert result.exit_code == 1
    first = str(error).splitlines()[0]
    assert result.output == f"entry_exit_combo: 数据载入失败：{first}\n"
    assert not out.exists()


def test_a_failed_check_writes_only_the_json(loaded_cli: list[str], tmp_path: Path) -> None:
    result = CliRunner().invoke(cb.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "entry_exit_combo: 输入检查 FAIL，没有算策略\n"
    assert [p.name for p in tmp_path.iterdir()] == ["entry_exit_combo.json"]
    written = json.loads(tmp_path.joinpath("entry_exit_combo.json").read_text(encoding="utf-8"))
    assert written["complete"] is False
    assert "span" not in written
