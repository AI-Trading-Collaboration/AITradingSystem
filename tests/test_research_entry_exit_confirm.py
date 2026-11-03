"""Acceptance tests for TRADING-2565-119: exits and buy-backs wait for the next close.

Requirement: knowledge qqq-options/auto-loop/period7/entry-exit-combo/entry-exit-confirm-
requirements-2026-11-03.md version 1.0 (owner decision entry_exit_confirm). Synthetic data only;
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
from ai_trading_system.research_probes import entry_exit_confirm as cf
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import exit_rules_after as xa
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_returns as xr
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import policy_table as pt

LOADED = load_settings()
GATE = tr.load_gate(PROJECT_ROOT / "research" / "_trials" / "prereg.yaml")
_, PLAN = tr.load_trial(lg.research_root(LOADED), pt.DATA_TRIAL)
MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "entry_exit_confirm.py"
SIX = ("x2_a", "x2_b", "x3_a", "x3_b", "x2x3_a", "x2x3_b")
SIX_C = tuple(f"{s}_c" for s in SIX)


# Constants and imports ------------------------------------------------------------------------


def test_the_run_and_its_strategies() -> None:
    assert cf.RUN_ID == "TRADING-2565-119"
    assert cf.OWNER_DECISION == "owner_decision:TRADING-2565:2026-11-03:entry_exit_confirm"
    assert cf.PLAIN == SIX
    assert cf.CONFIRMED == SIX_C
    assert cf.STRATEGIES == (*SIX_C, *SIX, "hold")
    assert list(cf.STRATEGY_TEXT) == [*SIX_C, *SIX, "hold"]
    assert cf.STRATEGY_TEXT["x3_b_c"] == "X3 跌破 50 日线，B 回到卖出价也买回，第二天确认"
    assert cf.STRATEGY_TEXT["x2_a"] == "X2 回落 8%，A 只等入场买回，不确认"
    assert cf.STRATEGY_TEXT["hold"] == "买入持有"
    assert cf.SPAN_TEXT == xa.SPAN_TEXT["qqq"]
    assert (cf.RESULT_JSON, cf.RESULT_MD) == ("entry_exit_confirm.json", "entry_exit_confirm.md")


def test_the_note_extends_the_118_note() -> None:
    assert cf.NOTE.startswith(cb.NOTE)
    for words in (
        "比这个收盘低才在下一个收盘卖",
        "不然这次作废、接着拿",
        "之后条件再成立再看",
        "比那个收盘高才在下一个收盘买回",
        "持平都算作废",
        "入场信号本身已经要三个收盘确认，不再加",
        "入场和确认买回同一个收盘算入场",
        "最后一个收盘才成立的看不到下一个收盘，不执行，也不算作废",
        "TRADING-2565-118 的规则，同一份数据重算",
    ):
        assert words in cf.NOTE, words


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
        "xc.load_data(",
        "cb.simulate(",
        "cb.equity(",
        "cb.metrics(",
        "cb.year_returns(",
    ):
        assert call in text, call


# Exit triggers and confirmation by hand -------------------------------------------------------


def _prep(close: list[float], sma: list[float | None] | None = None) -> xc.Prepared:
    n = len(close)
    dates = tuple(date(2001, 1, 1) + timedelta(days=i) for i in range(n))
    averages = tuple(sma) if sma is not None else (None,) * n
    return xc.Prepared(dates, tuple(close), averages, (None,) * n, (0,) * n, (False,) * n)


def test_triggers_are_every_close_the_exit_holds() -> None:
    data = _prep([100.0, 110.0, 101.2, 100.0, 103.0, 101.0])
    assert cf.triggers("x2", data, 0) == [(2, "X2"), (3, "X2"), (5, "X2")]
    line: list[float | None] = [100.0] * 6
    x3 = _prep([95.0, 101.0, 99.0, 98.0, 101.0, 97.0], line)
    assert cf.triggers("x3", x3, 0) == [(3, "X3")]
    touch = _prep([95.0, 100.0, 99.0, 98.0], [100.0] * 4)
    assert cf.triggers("x3", touch, 0) == [(3, "X3")]
    stop = _prep([100.0, 95.0, 92.0, 93.0, 91.0], [110.0] * 5)
    assert cf.triggers("x3", stop, 0) == [(2, "stop"), (4, "stop")]
    both = _prep([101.0, 99.0, 92.0, 90.0], [100.0] * 4)
    assert cf.triggers("x2x3", both, 0) == [(2, "X2"), (3, "X2")]
    with pytest.raises(ValueError, match="unknown exit x4"):
        cf.triggers("x4", data, 0)


def test_the_first_trigger_is_the_118_exit_day() -> None:
    rng = np.random.default_rng(5)
    for _ in range(40):
        n = 120
        close = list(100.0 * np.cumprod(1.0 + rng.normal(0.0, 0.02, n)))
        data = xc.prepare(tuple(date(2001, 1, 1) + timedelta(days=i) for i in range(n)), close)
        for rule in cb.EXITS:
            for b in (0, 49, 60):
                found = cf.triggers(rule, data, b)
                assert (found[0] if found else None) == cb.exit_day(rule, data, b)


def test_an_exit_sells_only_after_a_lower_next_close() -> None:
    data = _prep([100.0, 110.0, 101.2, 100.0])
    assert cf.confirmed_exit("x2", data, 0) == cf.Step(3, 2, "X2", 0)
    flat = _prep([100.0, 110.0, 101.2, 101.2, 101.0])
    assert cf.confirmed_exit("x2", flat, 0) == cf.Step(4, 3, "X2", 1)
    back = _prep([100.0, 110.0, 101.2, 102.0, 105.0, 101.2, 99.0])
    assert cf.confirmed_exit("x2", back, 0) == cf.Step(6, 5, "X2", 1)


def test_a_trigger_on_the_last_close_is_neither_sold_nor_lapsed() -> None:
    assert cf.confirmed_exit("x2", _prep([100.0, 110.0, 101.2]), 0) == cf.Step(None, None, "", 0)
    up = _prep([100.0, 110.0, 101.2, 101.3])
    assert cf.confirmed_exit("x2", up, 0) == cf.Step(None, None, "", 1)
    assert cf.confirmed_exit("x2", _prep([100.0, 101.0]), 0) == cf.Step(None, None, "", 0)


def test_the_stop_and_x3_wait_for_a_lower_close_too() -> None:
    stop = _prep([100.0, 95.0, 92.0, 93.0, 91.0, 90.0], [110.0] * 6)
    assert cf.confirmed_exit("x3", stop, 0) == cf.Step(5, 4, "stop", 1)
    line: list[float | None] = [100.0] * 6
    x3 = _prep([101.0, 99.0, 98.0, 98.5, 97.0, 96.0], line)
    assert cf.confirmed_exit("x3", x3, 0) == cf.Step(4, 3, "X3", 1)


# Buy-backs by hand -----------------------------------------------------------------------------


def test_version_a_still_waits_for_the_entry() -> None:
    data = _prep([100.0, 90.0, 101.0, 102.0, 95.0, 96.0])
    assert cf.confirmed_buy_back("a", data, [1, 4], 1) == cf.Step(4, 4, "entry", 0)
    assert cf.confirmed_buy_back("a", data, [1], 1) == cf.Step(None, None, "none", 0)
    with pytest.raises(ValueError, match="unknown version c"):
        cf.confirmed_buy_back("c", data, [], 1)


def test_version_b_buys_after_a_higher_close() -> None:
    data = _prep([100.0, 90.0, 89.0, 90.0, 91.0, 92.0])
    assert cf.confirmed_buy_back("b", data, [], 1) == cf.Step(4, 3, "recovery", 0)
    flat = _prep([100.0, 90.0, 90.0, 90.0, 89.0, 90.0, 95.0])
    assert cf.confirmed_buy_back("b", flat, [], 1) == cf.Step(6, 5, "recovery", 2)


def test_the_entry_wins_on_or_before_the_confirming_close() -> None:
    data = _prep([100.0, 90.0, 89.0, 90.0, 91.0, 92.0])
    assert cf.confirmed_buy_back("b", data, [4], 1) == cf.Step(4, 4, "entry", 0)
    assert cf.confirmed_buy_back("b", data, [3], 1) == cf.Step(3, 3, "entry", 0)
    assert cf.confirmed_buy_back("b", data, [5], 1) == cf.Step(4, 3, "recovery", 0)
    lapse = _prep([100.0, 90.0, 90.0, 89.0, 95.0, 96.0])
    assert cf.confirmed_buy_back("b", lapse, [4], 1) == cf.Step(4, 4, "entry", 1)
    assert cf.confirmed_buy_back("b", lapse, [3], 1) == cf.Step(3, 3, "entry", 0)


def test_a_recovery_on_the_last_close_is_neither_bought_nor_lapsed() -> None:
    data = _prep([100.0, 90.0, 89.0, 90.0])
    assert cf.confirmed_buy_back("b", data, [], 1) == cf.Step(None, None, "none", 0)
    down = _prep([100.0, 90.0, 91.0, 90.5, 90.0])
    assert cf.confirmed_buy_back("b", down, [], 1) == cf.Step(None, None, "none", 2)


# One path by hand ------------------------------------------------------------------------------

HAND = [100.0, 110.0, 101.0, 102.0, 100.0, 99.0, 103.0, 104.0, 120.0, 110.0, 108.0, 112.0, 113.0]


def test_a_confirmed_path_by_hand() -> None:
    data = _prep(HAND)
    route, exit_lapsed, buy_lapsed = cf.simulate("x2_b_c", data, [], 0)
    assert route.buys == (0, 7, 12)
    assert route.sells == (5, 10)
    assert route.held == tuple(1 <= p <= 5 or 8 <= p <= 10 for p in range(len(HAND)))
    d = data.dates
    assert route.trips == (
        {
            "decision": d[4].isoformat(),
            "reason": "X2",
            "sell": d[5].isoformat(),
            "buy": d[7].isoformat(),
            "how": "recovery",
            "days_out": 2,
            "change": 104.0 / 99.0 - 1.0,
            "exit_lapsed": 1,
            "buy_lapsed": 0,
        },
        {
            "decision": d[9].isoformat(),
            "reason": "X2",
            "sell": d[10].isoformat(),
            "buy": d[12].isoformat(),
            "how": "recovery",
            "days_out": 2,
            "change": 113.0 / 108.0 - 1.0,
            "exit_lapsed": 0,
            "buy_lapsed": 0,
        },
    )
    assert (exit_lapsed, buy_lapsed) == (1, 0)
    values = cb.equity(data.close, route, 0)
    end = 0.9995**5 * (99.0 / 100.0) * (108.0 / 104.0)
    assert values[-1] == pytest.approx(end, rel=1e-12)


def test_the_plain_paths_are_the_118_paths() -> None:
    data = _prep(HAND)
    for strategy in cb.STRATEGIES:
        got = cf.simulate(strategy, data, [6], 0)
        assert got == (cb.simulate(strategy, data, [6], 0), None, None)
    with pytest.raises(ValueError, match="unknown strategy x2_c"):
        cf.simulate("x2_c", data, [], 0)


def test_version_a_confirmed_by_hand() -> None:
    data = _prep(HAND)
    route, exit_lapsed, buy_lapsed = cf.simulate("x2_a_c", data, [6], 0)
    assert (route.buys, route.sells) == ((0, 6), (5, 10))
    assert [t["how"] for t in route.trips] == ["entry", "none"]
    assert [t["buy_lapsed"] for t in route.trips] == [0, 0]
    assert (exit_lapsed, buy_lapsed) == (1, 0)
    alone, _, _ = cf.simulate("x2_a_c", data, [], 0)
    assert alone.trips[0]["how"] == "none"
    assert alone.trips[0]["buy"] is None
    assert not any(alone.held[6:])


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
    TABLE = cf.run_table(QQQ, LOADED, GATE)
    TABLE_118 = cb.run_table(QQQ, LOADED, GATE)


@pytest.fixture
def same_data(monkeypatch: pytest.MonkeyPatch) -> None:
    _same(monkeypatch)


def test_the_synthetic_data_are_not_the_real_ones() -> None:
    assert STEP0["entry_set_a"]["trades"] != xr.STEP0_TRADES
    assert FOUND != cb.V0_ENTRIES
    assert len(FOUND) >= 2


# Checks --------------------------------------------------------------------------------------


def test_the_real_constants_stop_the_table() -> None:
    result = cf.run_table(QQQ, LOADED, GATE)
    assert result["complete"] is False
    assert result["check"]["qqq"]["ok"] is False
    assert result["check"]["entries"]["ok"] is False
    assert list(result) == ["run_id", "owner_decision", "source", "rule", "check", "complete"]


def test_each_check_alone_stops_the_table(same_data: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cb, "V0_ENTRIES", FOUND[:-1])
    one = cf.run_table(QQQ, LOADED, GATE)
    assert (one["check"]["qqq"]["ok"], one["check"]["entries"]["ok"]) == (True, False)
    assert "span" not in one and one["complete"] is False
    monkeypatch.setattr(cb, "V0_ENTRIES", FOUND)
    monkeypatch.setattr(xr, "STEP0_K", STEP0["scale"]["k"] * 1.001)
    two = cf.run_table(QQQ, LOADED, GATE)
    assert (two["check"]["qqq"]["ok"], two["check"]["entries"]["ok"]) == (False, True)
    assert "span" not in two and two["complete"] is False


def test_the_checks_and_the_rule_block() -> None:
    assert TABLE["check"] == TABLE_118["check"]
    assert TABLE["source"] == {"snapshot_id": PLAN.data.snapshot_id}
    assert (TABLE["run_id"], TABLE["owner_decision"]) == (cf.RUN_ID, cf.OWNER_DECISION)
    assert TABLE["rule"] == {
        "strategies": [*SIX_C, *SIX, "hold"],
        "entry": "v0_close",
        "start": "full",
        "trail": 0.08,
        "stop": 0.08,
        "sma_days": 50,
        "sell": "next_close",
        "sell_confirmed": "next_close_if_lower_than_trigger_close",
        "buy_back_a": "entry_close",
        "buy_back_b": "entry_close_or_first_close_at_or_above_sell_close",
        "buy_back_b_confirmed": "entry_close_or_next_close_if_higher_than_recovery_close",
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


def test_the_plain_rows_equal_the_118_table() -> None:
    span, old = TABLE["span"], TABLE_118["span"]
    for key in ("first_day", "last_day", "sessions", "entries"):
        assert span[key] == old[key]
    for strategy in cb.STRATEGIES:
        row = dict(span["strategies"][strategy])
        assert (row.pop("exit_lapsed"), row.pop("buy_lapsed")) == (None, None)
        assert row == old["strategies"][strategy]
    for new, was in zip(span["years"], old["years"], strict=True):
        assert {k: new[k] for k in was} == was


# Every confirmed strategy recomputed day by day ----------------------------------------------


def _own(strategy: str) -> dict[str, Any]:
    """An independent day-by-day state machine for one confirmed strategy."""
    close, sma, dates = PREPARED.close, PREPARED.sma, PREPARED.dates
    n = len(close)
    rule, _, version = strategy.removesuffix("_c").partition("_")
    holding, buy, high = True, S0, close[S0]
    first = sma[S0]
    armed = first is not None and close[S0] >= first
    pending: tuple[int, str] | None = None
    waiting: int | None = None
    sold = -1
    exits_lapsed = buys_lapsed = 0
    since_buy = since_sell = 0
    value = 0.9995
    values = [value]
    held_days = 0
    trips: list[dict[str, Any]] = []
    for p in range(S0 + 1, n):
        if holding:
            value *= close[p] / close[p - 1]
            held_days += 1
            if pending is not None:
                if close[p] < close[p - 1]:
                    value *= 0.9995
                    trips.append(
                        {
                            "decision": dates[pending[0]].isoformat(),
                            "reason": pending[1],
                            "sell": dates[p].isoformat(),
                            "exit_lapsed": since_buy,
                        }
                    )
                    holding, sold, pending, waiting, since_sell = False, p, None, None, 0
                    values.append(value)
                    continue
                exits_lapsed += 1
                since_buy += 1
                pending = None
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
        else:
            how = None
            if p in ENTRIES:
                how = "entry"
            elif version == "b":
                if waiting is not None:
                    if close[p] > close[p - 1]:
                        how = "recovery"
                    else:
                        buys_lapsed += 1
                        since_sell += 1
                        waiting = None
                if how is None and close[p] >= close[sold]:
                    waiting = p
            if how is not None:
                value *= 0.9995
                trips[-1].update(
                    {
                        "buy": dates[p].isoformat(),
                        "how": how,
                        "days_out": p - sold,
                        "change": close[p] / close[sold] - 1.0,
                        "buy_lapsed": since_sell,
                    }
                )
                holding, buy, high, since_buy = True, p, close[p], 0
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
                "buy_lapsed": since_sell,
            }
        )
    return {
        "values": values,
        "held_days": held_days,
        "trips": trips,
        "exit_lapsed": exits_lapsed,
        "buy_lapsed": buys_lapsed,
    }


KEYS = [
    "decision",
    "reason",
    "sell",
    "buy",
    "how",
    "days_out",
    "change",
    "exit_lapsed",
    "buy_lapsed",
]


@pytest.mark.parametrize("strategy", SIX_C)
def test_each_confirmed_strategy_recomputed(strategy: str) -> None:
    own = _own(strategy)
    row = TABLE["span"]["strategies"][strategy]
    assert len(row["trips"]) == len(own["trips"])
    for got, want in zip(row["trips"], own["trips"], strict=True):
        assert list(got) == KEYS
        assert {k: v for k, v in got.items() if k != "change"} == {
            k: v for k, v in want.items() if k != "change"
        }
        assert got["change"] == pytest.approx(want["change"], rel=1e-12, abs=1e-15)
    assert (row["exit_lapsed"], row["buy_lapsed"]) == (own["exit_lapsed"], own["buy_lapsed"])
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
    for how, key in (("entry", "by_entry"), ("recovery", "by_recovery"), ("none", "not_back")):
        assert row[key] == sum(t["how"] == how for t in trips)
    if strategy.endswith("_a_c"):
        assert row["buy_lapsed"] == 0
        assert row["by_recovery"] == 0


def test_each_confirmed_year_recomputed() -> None:
    years = TABLE["span"]["years"]
    for strategy in SIX_C:
        values = _own(strategy)["values"]
        base = 1.0
        for row in years:
            last = max(i for i, d in enumerate(SERIES.dates[S0:]) if d.year == row["year"])
            assert row[strategy] == pytest.approx(values[last] / base - 1.0, rel=1e-9, abs=1e-12)
            base = values[last]
    assert list(years[0]) == ["year", *cf.STRATEGIES]


def test_the_synthetic_table_exercises_every_case() -> None:
    rows = TABLE["span"]["strategies"]
    reasons = {t["reason"] for s in SIX_C for t in rows[s]["trips"]}
    hows = {t["how"] for s in SIX_C for t in rows[s]["trips"]}
    assert reasons == {"X2", "X3", "stop"}
    assert hows == {"entry", "recovery", "none"}
    assert all(rows[s]["exit_lapsed"] > 0 for s in SIX_C)
    assert all(rows[s]["buy_lapsed"] > 0 for s in SIX_C if s.endswith("_b_c"))
    assert any(t["exit_lapsed"] > 0 for s in SIX_C for t in rows[s]["trips"])
    assert any(t["buy_lapsed"] > 0 for s in SIX_C for t in rows[s]["trips"])
    assert any(rows[s]["sells"] != rows[s.removesuffix("_c")]["sells"] for s in SIX_C)


def test_no_nan_reaches_the_table() -> None:
    text = json.dumps(TABLE, allow_nan=False)
    assert "NaN" not in text and "Infinity" not in text


# Text -----------------------------------------------------------------------------------------


def test_the_lapsed_text() -> None:
    assert cf.lapsed_text({"exit_lapsed": None, "buy_lapsed": None}, "x2_a") == "—"
    assert cf.lapsed_text({"exit_lapsed": 3, "buy_lapsed": 0}, "x2_a_c") == "3 / —"
    assert cf.lapsed_text({"exit_lapsed": 3, "buy_lapsed": 7}, "x2x3_b_c") == "3 / 7"


def test_the_markdown() -> None:
    text = cf.render_markdown(TABLE)
    lines = text.split("\n")
    assert lines[0] == "# QQQ 入场加出场：出场和回补要第二天收盘确认（TRADING-2565-119）"
    assert lines[2] == cf.NOTE
    assert f"## {cf.SPAN_TEXT}" in lines
    span = TABLE["span"]
    head = (
        f"满仓起点 {span['first_day']}，到 {span['last_day']}，共 {span['sessions']} 个交易日。"
        f"入场信号 {len(FOUND)} 次：{'、'.join(FOUND)}。"
    )
    assert head in lines
    assert text.endswith("\n") and not text.endswith("\n\n")
    assert "|" + "---|" * 14 in lines
    assert "|" + "---|" * 8 in lines
    assert "|" + "---|" * 9 in lines
    hold = span["strategies"]["hold"]
    assert (
        f"| 买入持有 | {xr.pct(hold['total'])} | {xr.pct(hold['cagr'])} | "
        f"{xr.pct(hold['max_drawdown'])} | {hold['drawdown_peak']} → {hold['drawdown_trough']} | "
        f"{xr.pct(hold['worst20'])} | 100% | {xr.pct(hold['in_market_cagr'])} | 0 | 0 / 0 / 0 | "
        "0 | — | — | — |"
    ) in lines
    row = span["strategies"]["x2_b_c"]
    assert any(
        line.startswith(f"| {cf.STRATEGY_TEXT['x2_b_c']} | {xr.pct(row['total'])} |")
        and line.endswith(f"| {row['exit_lapsed']} / {row['buy_lapsed']} |")
        for line in lines
    )
    year_lines = [line for line in lines if line[:4] in ("| 19", "| 20")]
    assert len(year_lines) == len(span["years"])
    first = span["years"][0]
    shown = (*SIX_C, "hold")
    assert year_lines[0] == (
        f"| {first['year']} | " + " | ".join(xr.pct(first[s]) for s in shown) + " |"
    )
    trips = sum(len(span["strategies"][s]["trips"]) for s in SIX_C)
    assert sum(1 for line in lines if line.count(" | ") == 8 and line.startswith("| X")) == trips
    trip = span["strategies"]["x3_a_c"]["trips"][0]
    assert (
        f"| {cf.STRATEGY_TEXT['x3_a_c']} | {trip['decision']} | {trip['reason']} | "
        f"{trip['sell']} | {trip['buy'] or '—'} | {cb.HOW_TEXT[trip['how']]} | "
        f"{trip['days_out']} | {xr.pct(trip['change'])} | {trip['exit_lapsed']} / — |"
    ) in lines


def test_the_summary_line(tmp_path: Path) -> None:
    rows = TABLE["span"]["strategies"]
    parts = "、".join(
        f"{cf.STRATEGY_TEXT[s]} 年化 {xr.pct(rows[s]['cagr'])} 回撤 "
        f"{xr.pct(rows[s]['max_drawdown'])}"
        for s in (*SIX_C, "hold")
    )
    assert cf.summary_line(TABLE, tmp_path) == (
        f"entry_exit_confirm: {cf.SPAN_TEXT} 满仓起步，入场 {len(FOUND)} 次；{parts}；"
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
    result = CliRunner().invoke(cf.app, ["run", "--out", str(out), "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert loaded_cli == [f"qqq {tmp_path}"]
    text = out.joinpath("entry_exit_confirm.json").read_text(encoding="utf-8")
    assert text == json.dumps(TABLE, indent=2, ensure_ascii=False) + "\n"
    assert out.joinpath("entry_exit_confirm.md").read_text(encoding="utf-8") == (
        cf.render_markdown(TABLE)
    )
    names = sorted(p.name for p in out.iterdir())
    assert names == ["entry_exit_confirm.json", "entry_exit_confirm.md"]
    assert result.output == cf.summary_line(TABLE, out) + "\n"


@pytest.mark.parametrize("name", ["entry_exit_confirm.json", "entry_exit_confirm.md"])
def test_run_refuses_an_existing_result_without_loading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str
) -> None:
    def boom(root: Path) -> Any:
        raise AssertionError("loaded")

    monkeypatch.setattr(xc, "load_data", boom)
    tmp_path.joinpath(name).write_text("{}", encoding="utf-8")
    result = CliRunner().invoke(cf.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "entry_exit_confirm: 已有结果，拒绝覆盖。\n"
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
    result = CliRunner().invoke(cf.app, ["run", "--out", str(out)])
    assert result.exit_code == 1
    assert result.output == f"entry_exit_confirm: 数据载入失败：{str(error).splitlines()[0]}\n"
    assert not out.exists()


def test_a_failed_check_writes_only_the_json(loaded_cli: list[str], tmp_path: Path) -> None:
    result = CliRunner().invoke(cf.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "entry_exit_confirm: 输入检查 FAIL，没有算策略\n"
    assert [p.name for p in tmp_path.iterdir()] == ["entry_exit_confirm.json"]
    written = json.loads(tmp_path.joinpath("entry_exit_confirm.json").read_text(encoding="utf-8"))
    assert written["complete"] is False
    assert "span" not in written
