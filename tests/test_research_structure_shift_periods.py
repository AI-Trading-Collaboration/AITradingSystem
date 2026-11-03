"""Acceptance tests for TRADING-2565-122: the structure-shift strategy on QQQ's other periods.

Requirement: knowledge qqq-options/auto-loop/period7/entry-exit-combo/structure-shift-periods-
requirements-2026-11-03.md version 1.1 (owner decision structure_shift_other_periods). Synthetic
data only; no snapshot, no network.
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
from ai_trading_system.research_bench.settings import LoadedSettings
from ai_trading_system.research_probes import entry_exit_combo as cb
from ai_trading_system.research_probes import entry_structure_shift as es
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import entry_timing_ndx as en
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_ndx as xn
from ai_trading_system.research_probes import exit_rules_returns as xr
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import policy_table as pt
from ai_trading_system.research_probes import structure_shift_periods as sp
from ai_trading_system.yaml_loader import safe_load_yaml_path

LOADED = load_settings()
GATE = tr.load_gate(PROJECT_ROOT / "research" / "_trials" / "prereg.yaml")
_, PLAN = tr.load_trial(lg.research_root(LOADED), pt.DATA_TRIAL)
MODULE = (
    PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "structure_shift_periods.py"
)
REGISTRY = PROJECT_ROOT / "config" / "research" / "research_window_registry.yaml"
POLICY = PROJECT_ROOT / "config" / "research" / "primary_research_window_policy.yaml"
SOURCE = {"snapshot_id": "qlh-20261013-0b0df53cb1a2", "column": "ndx_price", "check_status": "WARN"}
NDX_WINDOW = en.registered_window(LOADED)
QQQ_WINDOW = sp.registered_qqq_window(LOADED)
SHIFTED5 = ("x2_s5", "x3_s5", "x2x3_s5")
UNDER = ("x2_s5_under", "x3_s5_under", "x2x3_s5_under")
NEW = (*UNDER, "x2_s5_line")
RULE_120 = {
    "swings": [5, 2],
    "swing": "close_strictly_beyond_k_closes_each_side_known_k_closes_later",
    "break": "close_below_sma200_after_close_at_or_above",
    "low": "swing_low_below_sma200_since_break_lower_than_current_low_and_last_entry_low",
    "high": "latest_swing_high_before_low",
    "shift_entry": "first_close_above_high_close",
    "shift_stop": "close_below_low_close_sell_next_close",
    "horizons": [20, 60],
    "long_sma_days": 200,
    "strategies": [
        "x2_s5",
        "x3_s5",
        "x2x3_s5",
        "x2_s2",
        "x3_s2",
        "x2x3_s2",
        "x2_a",
        "x3_a",
        "x2x3_a",
        "hold",
    ],
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


# Constants and imports ------------------------------------------------------------------------


def test_the_run_its_spans_and_its_strategies() -> None:
    assert sp.RUN_ID == "TRADING-2565-122"
    assert sp.OWNER_DECISION == (
        "owner_decision:TRADING-2565:2026-11-03:structure_shift_other_periods"
    )
    assert sp.SPANS == ("dev", "ndx", "qqq")
    assert sp.UNDER == UNDER
    assert sp.LINE == ("x2_s5_line",)
    assert sp.STRATEGIES == (*es.STRATEGIES, *NEW)
    assert list(sp.STRATEGY_TEXT) == list(sp.STRATEGIES)
    assert sp.STRATEGY_TEXT["x3_s5_under"] == (
        "X3 跌破 50 日线（只在 200 日线下算），A 加结构转变买回（左右各 5 天）"
    )
    assert sp.STRATEGY_TEXT["x2_s5_line"] == (
        "X2 回落 8%，A 加结构转变买回，线上响的再等 200 日线涨到响时收盘价（左右各 5 天）"
    )
    assert {s: sp.STRATEGY_TEXT[s] for s in es.STRATEGIES} == es.STRATEGY_TEXT
    assert sp.HOW_TEXT == {**es.HOW_TEXT, "line": "200 日线涨到响时收盘价"}
    assert sp.TRIPS_SHOWN == (*SHIFTED5, *NEW)
    assert sp.YEARS_SHOWN == (*SHIFTED5, *NEW, "x2_a", "x3_a", "x2x3_a", "hold")
    assert sp.SPAN_WINDOW == {
        "dev": "qqq_long_history_development",
        "ndx": "ndx_price_1986_1999",
        "qqq": "qqq_after_2010_owner_print",
    }
    assert sp.SPAN_TEXT["ndx"] == en.TITLE
    assert (sp.CHECK_JSON, sp.RESULT_JSON, sp.RESULT_MD) == (
        "structure_shift_periods_check.json",
        "structure_shift_periods.json",
        "structure_shift_periods.md",
    )


def test_the_window_and_input_constants() -> None:
    assert (sp.QQQ_WINDOW_ID, sp.QQQ_WINDOW_ROLE, sp.QQQ_READ_POLICY) == (
        "qqq_after_2010_owner_print",
        "owner_print_only",
        "once_per_owner_decision",
    )
    assert (sp.QQQ_START, sp.QQQ_END) == (date(2011, 1, 3), "latest")
    assert sp.QQQ_SNAPSHOT_ID == "qlh-20261013-0b0df53cb1a2" == PLAN.data.snapshot_id
    assert sp.QQQ_LAST_MIN == date(2026, 9, 30)
    assert sp.QQQ_SESSIONS == (3900, 4050)
    assert (sp.MAX_MOVE, sp.MAX_GAP_DAYS) == (0.25, 5)
    assert sp.NDX_V0 == ("1988-09-07", "1988-10-17", "1998-01-29", "1998-09-03", "1998-10-16")


def test_the_1999_2010_pins_are_the_120_printed_table() -> None:
    # TRADING-2565-120's entry_structure_shift.json, run once on 2026-11-03 20:52 JST.
    assert sp.DEV_SIGNALS == {"k5": 20, "k2": 42}
    assert sp.DEV_FIRST_DAY == "1999-12-23"
    assert sp.DEV_TOTALS == {
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


def test_the_rule_text_and_the_note() -> None:
    assert sp.RULE_TEXT.endswith(es.RULE_TEXT.partition("已看过结果。")[2])
    for words in (
        "规则照 TRADING-2565-120 写死",
        "纳斯达克 100 价格指数 1986–1999",
        "QQQ 2011-01-03 以后",
        "满仓起点以前的收盘只作回看，跌破只从满仓起点算",
        "k = 5 是 Baker 选的，k = 2 只作对照",
    ):
        assert words in sp.RULE_TEXT, words
    assert sp.NOTE.startswith(cb.NOTE.partition("对入场和出场都是看过结果的历史。")[2])
    assert es.NOTE[len(cb.NOTE) :] in sp.NOTE
    for words in (
        "X2、X3 和 X3 站上 50 日线以前的 8% 止损，只在那个收盘低于 200 日线时才算响",
        "结构转变买回后的 L 止损不变",
        "主表还是原规则",
        "200 日线第一次涨到（大于或等于）X2 响那个收盘的收盘，满仓买回",
        "同一个收盘依次算 v0_close、结构转变",
        "这种买回不加 L 止损",
        "离 200 日线 = 出场响那个收盘 ÷ 当天的 200 日线 − 1",
    ):
        assert words in sp.NOTE, words


def _imports(text: str) -> set[str]:
    imported: set[str] = set()
    for node in ast.walk(ast.parse(text)):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.update(f"{node.module}.{alias.name}" for alias in node.names)
    return imported


def test_the_module_reuses_the_120_module_and_reads_only_qqq_and_its_index() -> None:
    text = MODULE.read_text(encoding="utf-8")
    allowed = (
        "__future__.",
        "json",
        "math",
        "collections.",
        "datetime.",
        "pathlib.",
        "typing.",
        "pandas",
        "typer",
        "ai_trading_system.config.PROJECT_ROOT",
        "ai_trading_system.paths.join_root",
        "ai_trading_system.research_bench.ledger",
        "ai_trading_system.research_bench.trial",
        "ai_trading_system.research_bench.data.BenchData",
        "ai_trading_system.research_bench.settings.",
        "ai_trading_system.research_data.qqq_long_history",
        "ai_trading_system.research_probes.entry_exit_combo",
        "ai_trading_system.research_probes.entry_structure_shift",
        "ai_trading_system.research_probes.entry_timing_counts",
        "ai_trading_system.research_probes.entry_timing_ndx",
        "ai_trading_system.research_probes.exit_rules_after",
        "ai_trading_system.research_probes.exit_rules_counts",
        "ai_trading_system.research_probes.exit_rules_ndx",
        "ai_trading_system.research_probes.exit_rules_returns",
        "ai_trading_system.research_probes.gate_power",
        "ai_trading_system.research_probes.policy_table",
        "ai_trading_system.yaml_loader.safe_load_yaml_path",
    )
    for name in _imports(text):
        assert name.startswith(allowed), name
    for word in (
        "us_market",
        "French",
        "Mkt-RF",
        "vix",
        "VIX",
        "vxn",
        "vxo",
        "ndx_tr",
        "lev2",
        "lev3",
        "cash_return",
        'returns["cash',
        "ma200_forward",
        "window_reads",
        "lg.append",
        ".unlink(",
        "urllib",
        "requests",
        "load_daily",
        "import numpy",
        "def scan",
        "def prepare",
        "def moving_average",
        "def shifts",
        "def signal_rows",
        "def horizon_summary",
        "def any_day",
        "def span_table",
        "def equity",
        "def metrics",
        "def year_returns",
        "def exit_day",
        "def prices",
        "0.92",
        "0.0005",
    ):
        assert word not in text, word
    assert text.count(".write_text(") == 3
    for call in (
        "xa.qqq_inputs(",
        "gp.cut(",
        "ec.closes(",
        "ec.scan(",
        "ec.moving_average(",
        "es.shifts(",
        "es.signal_rows(",
        "es.horizon_summary(",
        "es.any_day(",
        "es.span_table(",
        "cb.exit_day",
        "cb.equity(",
        "cb.metrics(",
        "cb.year_returns(",
        "en.prices(",
        "en.load_data(",
        "xn.check_inputs(",
        "tr.market_data(",
        "tr._data_problems(",
    ):
        assert call in text, call


# The window registry --------------------------------------------------------------------------


def _windows() -> dict[str, Any]:
    raw = safe_load_yaml_path(REGISTRY)
    assert isinstance(raw, dict)
    windows = raw["windows"]
    assert isinstance(windows, dict)
    return windows


def test_the_new_window_is_registered_for_this_read_only() -> None:
    entry = _windows()["qqq_after_2010_owner_print"]
    assert entry["start"] == entry["requested_start"] == entry["actual_start"] == date(2011, 1, 3)
    assert entry["actual_portfolio_start"] == date(2011, 1, 3)
    assert entry["end"] == "latest"
    assert entry["role"] == "owner_print_only"
    assert entry["question_types"] == []
    assert entry["assets"] == ["QQQ"]
    assert entry["exact_or_proxy"] == "exact"
    assert entry["data_quality_contract"] == "qqq_long_history_snapshot_check"
    assert entry["data_spec"] == "config/research/qqq_long_history.yaml"
    assert entry["data_column"] == "etf_reference_return"
    assert entry["lookback_history_from"] == date(1999, 3, 10)
    assert entry["read_policy"] == "once_per_owner_decision"
    assert entry["allowed_usage"] == ["owner_requested_print_only_read"]
    ndx = _windows()["ndx_price_1986_1999"]
    assert entry["data_source"] == ndx["data_source"]
    assert entry["blocked_usage"] == ndx["blocked_usage"]
    for caveat in (
        "overlaps_qqq_long_history_validation_and_the_consistency_window",
        "period_7_charter_one_time_exception",
        "after_this_read_2011_onward_is_seen_history_for_the_entry_timing_and_exit_families",
    ):
        assert caveat in entry["caveats"], caveat


def test_the_other_windows_and_the_policy_are_unchanged() -> None:
    windows = _windows()
    validation = windows["qqq_long_history_validation"]
    assert (validation["start"], validation["end"]) == (date(2011, 1, 3), date(2021, 2, 19))
    development = windows["qqq_long_history_development"]
    assert (development["start"], development["end"]) == (date(1999, 3, 10), date(2010, 12, 31))
    ndx = windows["ndx_price_1986_1999"]
    assert (str(ndx["start"]), str(ndx["end"]), ndx["role"]) == (
        "1986-01-02",
        "1999-03-09",
        "owner_print_only",
    )
    policy = safe_load_yaml_path(POLICY)
    for roles in policy["question_windows"].values():
        assert "qqq_after_2010_owner_print" not in roles.values()


def _note(start: str, end: str) -> str:
    text = REGISTRY.read_text(encoding="utf-8")
    block = text[text.index(start) :]
    return " ".join(block[: block.index(end)].replace("#", " ").split())


def test_the_registry_notes_name_this_reader() -> None:
    ndx = _note("# Baker's print-only read of the 200-day pullback rule", "  ndx_price_1986_1999:")
    for words in (
        "fifth read",
        "TRADING-2565-122",
        "owner_decision:TRADING-2565:2026-11-03:structure_shift_other_periods",
        "research_probes/structure_shift_periods.py",
        "TRADING-2565-117",
        "fourth read",
    ):
        assert words in ndx, words
    qqq = _note(
        "# Baker's one-time print-only read of QQQ after 2010", "  qqq_after_2010_owner_print:"
    )
    for words in (
        "TRADING-2565-122",
        "owner_decision:TRADING-2565:2026-11-03:structure_shift_other_periods",
        "research_probes/structure_shift_periods.py",
        "period 7 charter",
        "sections 5.2 and 11",
        "qqq_long_history_validation",
        "the entry-timing family and the exit family",
        "no unseen QQQ history",
        "forward ledger",
        "not affected",
    ):
        assert words in qqq, words


def test_the_registered_qqq_window_is_read_from_the_bench_registry() -> None:
    assert sp.registered_qqq_window(LOADED) == {
        "window_id": "qqq_after_2010_owner_print",
        "role": "owner_print_only",
        "read_policy": "once_per_owner_decision",
        "start": "2011-01-03",
        "end": "latest",
    }


def _loaded_with(tmp_path: Path, windows: dict[str, dict[str, Any]]) -> LoadedSettings:
    lines = ["windows:"]
    for name, entry in windows.items():
        lines.append(f"  {name}:")
        lines += [f"    {key}: {value}" for key, value in entry.items()]
    tmp_path.joinpath("registry.yaml").write_text("\n".join(lines) + "\n", encoding="utf-8")
    settings = LOADED.settings.model_copy(update={"window_registry": "registry.yaml"})
    return LoadedSettings(settings=settings, root=tmp_path, sha256=LOADED.sha256)


GOOD = {
    "start": "2011-01-03",
    "end": "latest",
    "role": "owner_print_only",
    "read_policy": "once_per_owner_decision",
}


def test_a_missing_qqq_window_refuses_the_read(tmp_path: Path) -> None:
    loaded = _loaded_with(tmp_path, {"other": GOOD})
    with pytest.raises(ValueError, match="窗口登记表里没有 qqq_after_2010_owner_print"):
        sp.registered_qqq_window(loaded)
    assert sp.registered_qqq_window(_loaded_with(tmp_path, {sp.QQQ_WINDOW_ID: GOOD}))


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("start", "2011-01-04"),
        ("end", "2021-02-19"),
        ("role", "validation"),
        ("read_policy", "once_per_preregistered_question"),
    ],
)
def test_a_changed_qqq_window_refuses_the_read(tmp_path: Path, key: str, value: str) -> None:
    loaded = _loaded_with(tmp_path, {sp.QQQ_WINDOW_ID: {**GOOD, key: value}})
    with pytest.raises(ValueError, match="窗口 qqq_after_2010_owner_print 的登记和模块不一致"):
        sp.registered_qqq_window(loaded)


# Exits and buy-backs by hand ------------------------------------------------------------------


def _prep(close: list[float], sma: list[float | None] | None = None) -> xc.Prepared:
    n = len(close)
    dates = tuple(date(2001, 1, 1) + timedelta(days=i) for i in range(n))
    averages = tuple(sma) if sma is not None else (None,) * n
    return xc.Prepared(dates, tuple(close), averages, (None,) * n, (0,) * n, (False,) * n)


def _path(seed: int, n: int = 700) -> list[float]:
    rng = np.random.default_rng(seed)
    return [float(v) for v in 100.0 * np.cumprod(1.0 + rng.normal(0.0002, 0.02, n))]


@pytest.mark.parametrize("seed", range(6))
def test_below_line_exits_with_a_line_above_every_close_are_the_118_exits(seed: int) -> None:
    close = _path(seed)
    data = xc.prepare(tuple(date(2001, 1, 1) + timedelta(days=i) for i in range(700)), close)
    above = [math.inf] * len(close)
    for rule in cb.EXITS:
        for b in range(50, 690, 37):
            assert sp.exit_under(rule, data, above, b) == cb.exit_day(rule, data, b)
    nothing = [None] * len(close)
    assert all(sp.exit_under(r, data, nothing, 60) is None for r in cb.EXITS)
    with pytest.raises(ValueError, match="unknown exit x4"):
        sp.exit_under("x4", data, above, 0)


def test_a_below_line_exit_waits_for_a_close_under_the_line() -> None:
    close = [100.0, 110.0, 101.0, 100.0, 99.0, 98.0, 97.0]
    line: list[float | None] = [90.0, 90.0, 90.0, 99.5, 99.5, 99.5, 99.5]
    data = _prep(close)
    # X2 is due from close 2 (101 / 110 = -8.2%), but only close 4 is under the line.
    assert cb.exit_day("x2", data, 0) == (2, "X2")
    assert sp.exit_under("x2", data, line, 0) == (4, "X2")
    on_line = [*line[:4], 99.0, 99.0, 99.0]
    assert sp.exit_under("x2", data, on_line, 0) == (5, "X2")
    # The highest close and the 50-day arming are still recorded above the line.
    stop = [100.0, 95.0, 91.5, 91.0, 90.0]
    assert cb.exit_day("x3", _prep(stop), 0) == (2, "stop")
    assert sp.exit_under("x3", _prep(stop), [80.0, 80.0, 80.0, 92.0, 92.0], 0) == (3, "stop")
    sma: list[float | None] = [None, 96.0, 96.0, 97.0, 98.0, 98.0]
    armed = [100.0, 97.0, 95.0, 94.0, 93.0, 92.5]
    under = [99.0, 99.0, 99.0, 99.0, 99.0, 99.0]
    assert sp.exit_under("x3", _prep(armed, sma), under, 0) == (3, "X3")


def test_walk_with_the_118_exits_is_the_120_path() -> None:
    for seed in range(6):
        close = _path(seed)
        dates = tuple(date(2001, 1, 1) + timedelta(days=i) for i in range(len(close)))
        data = xc.prepare(dates, close)
        line = ec.moving_average(close)
        entries = sp.v0_entries(close, 0)
        moves = es.shifts(close, line, 5, 200)
        for x in cb.EXITS:
            assert sp.walk(x, data, entries, moves, 200, cb.exit_day) == es.simulate(
                f"{x}_s5", data, entries, moves, 200
            )


ROUTE = [100.0, 110.0, 100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 103.0, 104.0, 105.0]
LONG: list[float | None] = [90.0, 92.0, 94.0, 96.0, 97.0, 98.0, 99.0, 100.0, 101.0, 102.0, 103.0]


def test_the_line_buy_back_by_hand() -> None:
    data = _prep(ROUTE)
    # X2 decides at close 2 (100 above the line's 94); the line reaches 100 at close 7.
    route = sp.walk("x2", data, [], [], 0, cb.exit_day, catch_up=LONG)
    assert route.sells == (3,)
    assert route.buys == (0, 7)
    d = data.dates
    assert route.trips == (
        {
            "decision": d[2].isoformat(),
            "reason": "X2",
            "sell": d[3].isoformat(),
            "buy": d[7].isoformat(),
            "how": "line",
            "days_out": 4,
            "change": 101.0 / 99.0 - 1.0,
        },
    )
    assert route.held == tuple(1 <= p <= 3 or p >= 8 for p in range(len(ROUTE)))
    without = sp.walk("x2", data, [], [], 0, cb.exit_day)
    assert [t["how"] for t in without.trips] == ["none"]


def test_the_line_buy_back_needs_an_x2_close_above_the_line() -> None:
    data = _prep(ROUTE)
    on_line = [*LONG[:2], 100.0, *LONG[3:]]
    assert [t["how"] for t in sp.walk("x2", data, [], [], 0, cb.exit_day, on_line).trips] == [
        "none"
    ]
    # Only X2 decisions count: the X3 stop never arms a line buy-back.
    stop = [100.0, 95.0, 91.5, 92.0, 93.0, 94.0, 95.0]
    line: list[float | None] = [80.0, 81.0, 82.0, 88.0, 90.0, 92.0, 93.0]
    route = sp.walk("x3", _prep(stop), [], [], 0, cb.exit_day, line)
    assert [(t["reason"], t["how"]) for t in route.trips] == [("stop", "none")]


def test_the_entry_and_the_shift_win_ties_with_the_line() -> None:
    data = _prep(ROUTE)
    tie = sp.walk("x2", data, [7], [], 0, cb.exit_day, catch_up=LONG)
    assert [t["how"] for t in tie.trips] == ["entry"]
    earlier = sp.walk("x2", data, [], [es.Shift(1, 4, 1, 5)], 0, cb.exit_day, catch_up=LONG)
    assert [t["how"] for t in earlier.trips][0] == "shift"
    same = sp.walk("x2", data, [], [es.Shift(1, 4, 1, 7)], 0, cb.exit_day, catch_up=LONG)
    assert [t["how"] for t in same.trips] == ["shift"]
    later = sp.walk("x2", data, [9], [es.Shift(1, 4, 1, 8)], 0, cb.exit_day, catch_up=LONG)
    assert [t["how"] for t in later.trips] == ["line"]
    assert later.buys == (0, 7)


def test_no_low_stop_after_a_line_buy_back() -> None:
    close = [100.0, 110.0, 100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 97.5, 98.0, 99.0]
    line: list[float | None] = [90.0, 92.0, 94.0, 96.0, 97.0, 98.0, 99.0, 100.0, 97.0, 97.0, 97.0]
    route = sp.walk("x2", _prep(close), [], [], 0, cb.exit_day, catch_up=line)
    assert route.buys == (0, 7)
    assert route.sells == (3,)


# Synthetic QQQ ruler and Nasdaq-100 index -----------------------------------------------------


def _pos(index: pd.DatetimeIndex, day: str) -> int:
    position = index.get_loc(pd.Timestamp(day))
    assert isinstance(position, int)
    return position


def _qqq_data() -> BenchData:
    index = pd.bdate_range("1998-01-02", "2016-12-30", name="date")
    rng = np.random.default_rng(11)
    level = 100.0 * np.cumprod(1.0003 * (1.0 + rng.normal(0.0, 0.015, len(index))))
    for first, last, depth in (
        ("2000-04-03", "2000-05-24", 0.7),
        ("2008-09-15", "2008-11-20", 0.6),
        ("2011-07-22", "2011-10-03", 0.8),
        ("2015-07-20", "2016-02-11", 0.8),
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
    return with_signal_sources(data, {}, {}, snapshot_id=PLAN.data.signal_snapshot_id)


NDX_INDEX = pd.bdate_range("1986-01-02", "1999-03-09", name="date")


def _ndx_levels() -> list[tuple[date, float]]:
    rng = np.random.default_rng(5)
    level = 100.0 * np.cumprod(1.0006 * (1.0 + rng.normal(0.0, 0.011, len(NDX_INDEX))))
    for first, last, depth in (
        ("1987-10-12", "1987-10-23", 0.7),
        ("1990-07-16", "1990-10-12", 0.8),
        ("1994-03-17", "1994-06-24", 0.85),
    ):
        a, b = _pos(NDX_INDEX, first), _pos(NDX_INDEX, last)
        level[a : b + 1] *= np.linspace(1.0, depth, b - a + 1)
        level[b + 1 :] *= depth
    for day in ("1988-04-01", "1995-12-25"):
        level[_pos(NDX_INDEX, day)] = level[_pos(NDX_INDEX, day) - 1]
    return [(day.date(), float(value)) for day, value in zip(NDX_INDEX, level, strict=True)]


QQQ = _qqq_data()
CUT = gp.cut(QQQ)
STEP0 = xc.check_inputs(CUT, LOADED, GATE)
DEV = ec.closes(QQQ.returns["qqq"], date(2010, 12, 31))
FULL = ec.closes(QQQ.returns["qqq"], date.max)
FIRST_IN = next(p for p, day in enumerate(FULL.dates) if day >= date(2011, 1, 3))
DEV_FOUND = tuple(
    DEV.dates[t.entry].isoformat() for t in ec.scan(DEV.close, 0) if t.counted and t.entry
)
LEVELS = _ndx_levels()
N_SERIES, N_FLAT = en.prices(LEVELS)
N_MOVE = en.largest_move(N_SERIES)
assert N_MOVE is not None
N_FOUND = tuple(
    N_SERIES.dates[t.entry].isoformat()
    for t in ec.scan(N_SERIES.close, 0)
    if t.counted and t.entry is not None
)


def _same(mp: pytest.MonkeyPatch) -> None:
    a = STEP0["entry_set_a"]
    mp.setattr(xr, "STEP0_TRADES", a["trades"])
    mp.setattr(xr, "STEP0_FIRST", a["first"])
    mp.setattr(xr, "STEP0_LAST", a["last"])
    mp.setattr(xr, "STEP0_K", STEP0["scale"]["k"])
    mp.setattr(xr, "H_MATCH", {r: STEP0["exits"][r]["h_match"] for r in xc.RULES})
    mp.setattr(cb, "V0_ENTRIES", DEV_FOUND)
    assert N_MOVE is not None
    mp.setattr(xn, "NDX_CLOSES", len(N_SERIES.dates))
    mp.setattr(xn, "NDX_FLAT_DATES", tuple(day.isoformat() for day in N_FLAT))
    mp.setattr(xn, "NDX_MOVE_DATE", N_MOVE["date"])
    mp.setattr(xn, "NDX_MOVE", round(N_MOVE["return"], 4))
    mp.setattr(sp, "NDX_V0", N_FOUND)
    mp.setattr(sp, "QQQ_LAST_MIN", FULL.dates[-1])
    sessions = len(FULL.dates) - FIRST_IN
    mp.setattr(sp, "QQQ_SESSIONS", (sessions - 1, sessions + 1))


with pytest.MonkeyPatch.context() as _mp:
    _same(_mp)
    TABLE_120 = es.run_table(CUT, LOADED, GATE)
    DEV_PINS = {
        "DEV_SIGNALS": {k: len(v["rows"]) for k, v in TABLE_120["signals"].items()},
        "DEV_FIRST_DAY": TABLE_120["span"]["first_day"],
        "DEV_TOTALS": {s: round(v["total"], 6) for s, v in TABLE_120["span"]["strategies"].items()},
    }
    for _name, _value in DEV_PINS.items():
        _mp.setattr(sp, _name, _value)
    ARGS = (QQQ, LOADED, GATE, QQQ_WINDOW, LEVELS, SOURCE, NDX_WINDOW)
    TABLE = sp.run_table(*ARGS)
    CHECK = sp.check_table(*ARGS)


@pytest.fixture
def same_data(monkeypatch: pytest.MonkeyPatch) -> None:
    _same(monkeypatch)
    for name, value in DEV_PINS.items():
        monkeypatch.setattr(sp, name, value)


def test_the_synthetic_data_are_not_the_real_ones() -> None:
    assert STEP0["entry_set_a"]["trades"] != xr.STEP0_TRADES
    assert DEV_FOUND != cb.V0_ENTRIES and len(DEV_FOUND) >= 2
    assert N_FOUND != sp.NDX_V0 and len(N_FOUND) >= 2
    assert len(N_SERIES.dates) != xn.NDX_CLOSES
    assert FULL.dates[-1] < sp.QQQ_LAST_MIN
    assert DEV_PINS["DEV_TOTALS"] != sp.DEV_TOTALS
    assert TABLE["complete"] is True


# Checks --------------------------------------------------------------------------------------


def test_the_real_constants_stop_the_table() -> None:
    result = sp.run_table(*ARGS)
    assert result["complete"] is False
    assert result["check"]["qqq"]["ok"] is False and result["check"]["ndx"]["ok"] is False
    assert result["check"]["dev"] is None
    assert list(result) == ["run_id", "owner_decision", "source", "rule", "check", "complete"]
    assert sp.check_table(*ARGS)["ok"] is False


def _qqq_ok(**changes: Any) -> bool:
    return bool(sp.qqq_inputs(QQQ, LOADED, GATE, QQQ_WINDOW).check["ok"])


@pytest.mark.parametrize(
    ("target", "name", "value"),
    [
        (cb, "V0_ENTRIES", DEV_FOUND[:-1]),
        (xr, "STEP0_K", 1.0),
        (sp, "QQQ_SNAPSHOT_ID", "qlh-other"),
        (sp, "QQQ_START", date(2011, 1, 1)),
        (sp, "QQQ_LAST_MIN", date(2017, 1, 3)),
        (sp, "QQQ_SESSIONS", (3900, 4050)),
        (sp, "MAX_GAP_DAYS", 2),
        (sp, "MAX_MOVE", 0.01),
    ],
)
def test_each_qqq_check_alone_stops_the_table(
    same_data: None, monkeypatch: pytest.MonkeyPatch, target: Any, name: str, value: Any
) -> None:
    assert sp.qqq_inputs(QQQ, LOADED, GATE, QQQ_WINDOW).check["ok"] is True
    monkeypatch.setattr(target, name, value)
    one = sp.run_table(*ARGS)
    assert (one["check"]["qqq"]["ok"], one["check"]["ndx"]["ok"]) == (False, True)
    assert one["check"]["dev"] is None and "spans" not in one and one["complete"] is False
    assert sp.check_table(*ARGS)["ok"] is False


@pytest.mark.parametrize(
    ("target", "name", "value"),
    [(sp, "NDX_V0", ("1988-09-07",)), (xn, "NDX_CLOSES", 3317), (xn, "NDX_MOVE", -0.5)],
)
def test_each_ndx_check_alone_stops_the_table(
    same_data: None, monkeypatch: pytest.MonkeyPatch, target: Any, name: str, value: Any
) -> None:
    monkeypatch.setattr(target, name, value)
    one = sp.run_table(*ARGS)
    assert (one["check"]["qqq"]["ok"], one["check"]["ndx"]["ok"]) == (True, False)
    assert "spans" not in one and one["complete"] is False


def _with_returns(change: Any) -> BenchData:
    returns = QQQ.returns.copy()
    change(returns)
    return BenchData(returns=returns, inputs=QQQ.inputs, source=QQQ.source, reserves=QQQ.reserves)


def test_a_missing_close_after_2010_stops_the_table(same_data: None) -> None:
    def gap(returns: pd.DataFrame) -> None:
        returns.loc[pd.Timestamp("2013-05-06"), "qqq"] = np.nan

    check = sp.qqq_inputs(_with_returns(gap), LOADED, GATE, QQQ_WINDOW).check
    assert check["missing_days"] == 1 and check["missing_dates"] == ["2013-05-06"]
    assert check["ok"] is False


def test_a_series_that_changes_before_the_cut_stops_the_table(
    same_data: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = gp.cut

    def other(data: BenchData, cutoff: date = gp.CUTOFF) -> BenchData:
        cut = real(data, cutoff)
        returns = cut.returns.copy()
        column = returns["qqq"].to_numpy(dtype=float, copy=True)
        column[returns.index.get_loc(pd.Timestamp("2005-03-01"))] += 1e-9
        returns["qqq"] = column
        return BenchData(returns=returns, inputs=cut.inputs, source=cut.source)

    step0 = xc.check_inputs(other(QQQ), LOADED, GATE)
    monkeypatch.setattr(xr, "STEP0_K", step0["scale"]["k"])
    monkeypatch.setattr(xr, "H_MATCH", {r: step0["exits"][r]["h_match"] for r in xc.RULES})
    monkeypatch.setattr(gp, "cut", other)
    check = sp.qqq_inputs(QQQ, LOADED, GATE, QQQ_WINDOW).check
    assert check["step0"]["ok"] is True and check["dev_entries"] == list(DEV_FOUND)
    assert check["same_before_cut"] is False and check["ok"] is False


def test_a_changed_1999_2010_table_stops_the_new_spans(
    same_data: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    totals = dict(DEV_PINS["DEV_TOTALS"])
    totals["x3_s5"] += 0.000001
    monkeypatch.setattr(sp, "DEV_TOTALS", totals)
    one = sp.run_table(*ARGS)
    assert one["check"]["qqq"]["ok"] and one["check"]["ndx"]["ok"]
    assert one["check"]["dev"]["ok"] is False
    assert "spans" not in one and one["complete"] is False
    monkeypatch.setattr(sp, "DEV_TOTALS", DEV_PINS["DEV_TOTALS"])
    monkeypatch.setattr(sp, "DEV_SIGNALS", {"k5": 1, "k2": 2})
    assert sp.run_table(*ARGS)["check"]["dev"]["ok"] is False
    monkeypatch.setattr(sp, "DEV_SIGNALS", DEV_PINS["DEV_SIGNALS"])
    monkeypatch.setattr(sp, "DEV_FIRST_DAY", "1999-12-24")
    assert sp.run_table(*ARGS)["check"]["dev"]["ok"] is False


def test_the_qqq_check_block() -> None:
    q = TABLE["check"]["qqq"]
    assert {k: q[k] for k in ("window_id", "role", "read_policy", "start", "end")} == QQQ_WINDOW
    assert q["snapshot_id"] == PLAN.data.snapshot_id
    assert q["step0"] == TABLE_120["check"]["qqq"]
    assert q["dev_entries"] == list(DEV_FOUND)
    assert q["dev_first_scan_date"] == TABLE_120["check"]["entries"]["first_scan_date"]
    assert q["same_before_cut"] is True
    assert q["cut_closes"] == len(DEV.dates)
    assert (q["missing_days"], q["missing_dates"]) == (0, [])
    assert q["dates_increasing"] is True and q["closes_positive"] is True
    assert q["first_close_date"] == "2011-01-03"
    assert q["last_close_date"] == FULL.dates[-1].isoformat()
    assert q["sessions"] == len(FULL.dates) - FIRST_IN
    moves = [abs(FULL.close[p] / FULL.close[p - 1] - 1.0) for p in range(FIRST_IN, len(FULL.close))]
    assert abs(q["largest_move"]["return"]) == pytest.approx(max(moves), rel=1e-12)
    assert q["largest_gap_days"] == 3
    assert q["entries"] == [
        FULL.dates[t.entry].isoformat()
        for t in ec.scan(FULL.close, FIRST_IN)
        if t.counted and t.entry is not None
    ]
    assert q["ok"] is True


def test_the_ndx_check_block() -> None:
    n = TABLE["check"]["ndx"]
    entries = xc.entry_range(xc.prepare(N_SERIES.dates, N_SERIES.close), 0)
    with pytest.MonkeyPatch.context() as mp:
        _same(mp)
        assert n["inputs"] == xn.check_inputs(N_SERIES, N_FLAT, NDX_WINDOW, entries)
    assert n["first_scan_date"] == N_SERIES.dates[200].isoformat()
    assert n["entries"] == list(N_FOUND)
    assert n["expected"] == list(N_FOUND)
    assert n["ok"] is True


def test_the_check_table_has_no_signals_or_returns() -> None:
    assert list(CHECK) == ["run_id", "owner_decision", "source", "rule", "check", "ok"]
    assert CHECK["check"] == {"qqq": TABLE["check"]["qqq"], "ndx": TABLE["check"]["ndx"]}
    assert CHECK["ok"] is True
    text = json.dumps(CHECK["check"])
    for word in ("signals", "spans", "total", "trips", "plain_mean"):
        assert word not in text, word


def test_the_head_and_the_rule_block() -> None:
    assert (TABLE["run_id"], TABLE["owner_decision"]) == (sp.RUN_ID, sp.OWNER_DECISION)
    assert TABLE["source"] == {"qqq": {"snapshot_id": PLAN.data.snapshot_id}, "ndx": SOURCE}
    rule = dict(TABLE["rule"])
    assert rule.pop("spans") == ["dev", "ndx", "qqq"]
    assert rule.pop("under") == {
        "strategies": list(UNDER),
        "counts": "exit_decision_only_on_a_close_below_sma200",
        "exits": ["X2", "X3", "stop"],
        "low_stop": "unchanged",
    }
    assert rule.pop("line") == {
        "strategies": ["x2_s5_line"],
        "buy_back": "first_close_where_sma200_reaches_the_x2_decision_close",
        "when": "x2_decision_close_above_sma200",
        "order": ["entry", "shift", "line"],
        "low_stop": "none_after_line_buy_back",
    }
    assert rule == RULE_120 == TABLE_120["rule"]
    assert list(TABLE) == [
        "run_id",
        "owner_decision",
        "source",
        "rule",
        "check",
        "complete",
        "spans",
    ]
    assert list(TABLE["spans"]) == ["dev", "ndx", "qqq"]
    assert TABLE["check"]["dev"] == {
        "signals": DEV_PINS["DEV_SIGNALS"],
        "first_day": DEV_PINS["DEV_FIRST_DAY"],
        "totals": DEV_PINS["DEV_TOTALS"],
        "ok": True,
    }


# The spans ------------------------------------------------------------------------------------


def _strip(row: dict[str, Any]) -> dict[str, Any]:
    out = {k: v for k, v in row.items() if k != "by_line"}
    out["trips"] = [{k: v for k, v in t.items() if k != "vs_line"} for t in row["trips"]]
    return out


def test_the_1999_2010_span_is_the_120_table() -> None:
    dev = TABLE["spans"]["dev"]
    assert dev["signals"] == TABLE_120["signals"]
    span, old = dev["span"], TABLE_120["span"]
    for key in ("first_day", "last_day", "sessions", "entries"):
        assert span[key] == old[key]
    for strategy in es.STRATEGIES:
        assert _strip(span["strategies"][strategy]) == old["strategies"][strategy]
    for new, was in zip(span["years"], old["years"], strict=True):
        assert {k: new[k] for k in was} == was
    assert (dev["lookback_from"], dev["scan_from"]) == (
        DEV.dates[0].isoformat(),
        DEV.dates[0].isoformat(),
    )


SPAN_SERIES = {"dev": (DEV, 0), "ndx": (N_SERIES, 0), "qqq": (FULL, FIRST_IN)}


@pytest.mark.parametrize("name", ["ndx", "qqq"])
def test_each_new_span_is_the_120_functions_on_its_series(name: str) -> None:
    series, first_in = SPAN_SERIES[name]
    s0 = max(first_in, 200)
    block = TABLE["spans"][name]
    line = ec.moving_average(series.close)
    entries = [t.entry for t in ec.scan(series.close, first_in) if t.counted and t.entry]
    moves = {k: es.shifts(series.close, line, k, s0) for k in (5, 2)}
    for k in (5, 2):
        rows = es.signal_rows(series.dates, series.close, line, moves[k], s0)
        assert block["signals"][f"k{k}"]["rows"] == rows
        for h in (20, 60):
            every = es.any_day(series.dates, series.close, s0, h)
            assert block["signals"][f"k{k}"]["summary"][f"h{h}"] == es.horizon_summary(
                rows, h, every
            )
    prepared = xc.prepare(series.dates, series.close)
    old = es.span_table(series, prepared, entries, moves, s0)
    span = block["span"]
    for key in ("first_day", "last_day", "sessions", "entries"):
        assert span[key] == old[key]
    for strategy in es.STRATEGIES:
        assert _strip(span["strategies"][strategy]) == old["strategies"][strategy]
    assert block["lookback_from"] == series.dates[0].isoformat()
    assert block["scan_from"] == series.dates[first_in].isoformat()


def test_the_span_starts() -> None:
    assert TABLE["spans"]["ndx"]["span"]["first_day"] == N_SERIES.dates[200].isoformat()
    assert TABLE["spans"]["qqq"]["span"]["first_day"] == "2011-01-03"
    assert TABLE["spans"]["qqq"]["lookback_from"] < "2000-01-01"
    for k in ("k5", "k2"):
        for row in TABLE["spans"]["qqq"]["signals"][k]["rows"]:
            assert row["cross"] >= "2011-01-03"
    assert TABLE["spans"]["qqq"]["span"]["entries"] == TABLE["check"]["qqq"]["entries"]
    assert TABLE["spans"]["ndx"]["span"]["entries"] == list(N_FOUND)


def _own(name: str, strategy: str) -> dict[str, Any]:
    """An independent day-by-day state machine for the below-line and line rows."""
    series, first_in = SPAN_SERIES[name]
    s0 = max(first_in, 200)
    close, dates = series.close, series.dates
    sma = xc.prepare(dates, close).sma
    long = ec.moving_average(close)
    entries = {t.entry for t in ec.scan(close, first_in) if t.counted and t.entry}
    lows = {m.entry: m.low for m in es.shifts(close, long, 5, s0)}
    rule = strategy.partition("_s5")[0]
    under = strategy.endswith("_under")
    catch = strategy.endswith("_line")
    n = len(close)
    holding, buy, high = True, s0, close[s0]
    first = sma[s0]
    armed = first is not None and close[s0] >= first
    floor: float | None = None
    target: float | None = None
    pending: tuple[int, str] | None = None
    sold = -1
    value = 0.9995
    values = [value]
    held_days = 0
    trips: list[dict[str, Any]] = []
    for p in range(s0 + 1, n):
        if holding:
            value *= close[p] / close[p - 1]
            held_days += 1
            if pending is not None:
                value *= 0.9995
                d, reason = pending
                trips.append(
                    {
                        "decision": dates[d].isoformat(),
                        "reason": reason,
                        "sell": dates[p].isoformat(),
                    }
                )
                at = long[d]
                above = at is not None and close[d] > at
                target = close[d] if catch and reason == "X2" and above else None
                holding, sold, pending = False, p, None
                values.append(value)
                continue
            high = max(high, close[p])
            line, before = sma[p], sma[p - 1]
            if line is not None and close[p] >= line:
                armed = True
            low_line = long[p]
            counts = not under or (low_line is not None and close[p] < low_line)
            below = line is not None and close[p] < line
            below_before = before is not None and close[p - 1] < before
            if counts and rule != "x3" and close[p] / high <= 0.92:
                pending = (p, "X2")
            elif counts and rule != "x2" and armed and below and below_before:
                pending = (p, "X3")
            elif counts and rule != "x2" and not armed and close[p] / close[buy] <= 0.92:
                pending = (p, "stop")
            elif floor is not None and close[p] < floor:
                pending = (p, "L")
        else:
            how = None
            if p in entries:
                how, floor = "entry", None
            elif p in lows:
                how, floor = "shift", close[lows[p]]
            elif target is not None and (now := long[p]) is not None and now >= target:
                how, floor = "line", None
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
                holding, buy, high, target = True, p, close[p], None
                fifty = sma[p]
                armed = fifty is not None and close[p] >= fifty
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
    return {"values": values, "held_days": held_days, "trips": trips, "s0": s0}


KEYS = ["decision", "reason", "sell", "buy", "how", "days_out", "change", "vs_line"]


@pytest.mark.parametrize("name", ["dev", "ndx", "qqq"])
@pytest.mark.parametrize("strategy", NEW)
def test_each_new_row_recomputed(name: str, strategy: str) -> None:
    own = _own(name, strategy)
    series = SPAN_SERIES[name][0]
    row = TABLE["spans"][name]["span"]["strategies"][strategy]
    assert len(row["trips"]) == len(own["trips"])
    for got, want in zip(row["trips"], own["trips"], strict=True):
        assert list(got) == KEYS
        assert {k: v for k, v in got.items() if k not in ("change", "vs_line")} == {
            k: v for k, v in want.items() if k != "change"
        }
        assert got["change"] == pytest.approx(want["change"], rel=1e-12, abs=1e-15)
    values = own["values"]
    s0 = own["s0"]
    assert row["total"] == pytest.approx(values[-1] - 1.0, rel=1e-9, abs=1e-12)
    years = (series.dates[-1] - series.dates[s0]).days / 365.25
    assert row["cagr"] == pytest.approx(values[-1] ** (1.0 / years) - 1.0, rel=1e-9, abs=1e-12)
    peak, worst = values[0], 0.0
    for value in values:
        peak = max(peak, value)
        worst = min(worst, value / peak - 1.0)
    assert row["max_drawdown"] == pytest.approx(worst, rel=1e-9, abs=1e-12)
    sessions = len(series.dates) - 1 - s0
    assert row["in_market"] == pytest.approx(own["held_days"] / sessions, rel=1e-15)
    trips = own["trips"]
    assert row["sells"] == len(trips)
    for how, key in (("entry", "by_entry"), ("shift", "by_shift"), ("line", "by_line")):
        assert row[key] == sum(t["how"] == how for t in trips)
    assert row["not_back"] == sum(t["how"] == "none" for t in trips)
    assert row["low_stops"] == sum(t["reason"] == "L" for t in trips)
    base = 1.0
    for year in TABLE["spans"][name]["span"]["years"]:
        last = max(i for i, d in enumerate(series.dates[s0:]) if d.year == year["year"])
        assert year[strategy] == pytest.approx(values[last] / base - 1.0, rel=1e-9, abs=1e-12)
        base = values[last]


@pytest.mark.parametrize("name", ["dev", "ndx", "qqq"])
def test_every_trip_carries_its_distance_from_the_line(name: str) -> None:
    series = SPAN_SERIES[name][0]
    long = ec.moving_average(series.close)
    position = {day.isoformat(): p for p, day in enumerate(series.dates)}
    block = TABLE["spans"][name]["span"]
    for strategy in sp.STRATEGIES:
        row = block["strategies"][strategy]
        assert row["by_line"] == sum(t["how"] == "line" for t in row["trips"])
        for trip in row["trips"]:
            d = position[trip["decision"]]
            at = long[d]
            assert at is not None
            assert trip["vs_line"] == pytest.approx(series.close[d] / at - 1.0, rel=1e-12)
    assert list(block["strategies"]) == list(sp.STRATEGIES)
    assert list(block["years"][0]) == ["year", *sp.STRATEGIES]


def test_the_synthetic_table_exercises_every_case() -> None:
    spans = TABLE["spans"]
    for name in ("dev", "ndx", "qqq"):
        rows = spans[name]["span"]["strategies"]
        assert len(spans[name]["signals"]["k5"]["rows"]) >= 2
        assert any(rows[u]["total"] != rows[u.removesuffix("_under")]["total"] for u in UNDER)
        above = [t for t in rows["x2_s5"]["trips"] if t["vs_line"] > 0]
        assert above, name
        assert all(t["vs_line"] < 0 for u in UNDER for t in rows[u]["trips"] if t["reason"] != "L")
    lines = [
        t
        for n in ("dev", "ndx", "qqq")
        for t in spans[n]["span"]["strategies"]["x2_s5_line"]["trips"]
    ]
    assert {t["how"] for t in lines} >= {"line", "shift"}
    under = {
        t["reason"]
        for n in ("dev", "ndx", "qqq")
        for u in UNDER
        for t in spans[n]["span"]["strategies"][u]["trips"]
    }
    assert under >= {"X2", "X3"}


def test_no_nan_reaches_the_table() -> None:
    text = json.dumps(TABLE, allow_nan=False)
    assert "NaN" not in text and "Infinity" not in text


# Text -----------------------------------------------------------------------------------------


def test_the_markdown() -> None:
    text = sp.render_markdown(TABLE)
    lines = text.split("\n")
    assert lines[0] == "# 结构转变入场和策略放到 QQQ 的别的时期（TRADING-2565-122）"
    assert lines[2] == sp.RULE_TEXT
    assert lines[4] == sp.NOTE
    assert text.endswith("\n") and not text.endswith("\n\n")
    heads = [line for line in lines if line.startswith("## ")]
    assert heads == [f"## {sp.SPAN_TEXT[name]}" for name in ("dev", "ndx", "qqq")]
    assert lines.count("### 策略") == 3
    for name in ("dev", "ndx", "qqq"):
        block = TABLE["spans"][name]
        span = block["span"]
        head = (
            f"窗口 `{sp.SPAN_WINDOW[name]}`；回看从 {block['lookback_from']}，v0_close 从 "
            f"{block['scan_from']} 起找；满仓起点 {span['first_day']}，到 {span['last_day']}，"
            f"共 {span['sessions']} 个交易日。v0_close 入场 {len(span['entries'])} 次："
            f"{'、'.join(span['entries']) or '无'}。"
        )
        assert head in lines
        for k in (5, 2):
            assert f"### 左右各 {k} 天：入场 {len(block['signals'][f'k{k}']['rows'])} 次" in lines
        row = span["strategies"]["x2_s5_line"]
        assert any(
            line.startswith(f"| {sp.STRATEGY_TEXT['x2_s5_line']} | {xr.pct(row['total'])} |")
            and f"| {row['by_entry']} / {row['by_shift']} / {row['by_line']} / "
            f"{row['not_back']} | {row['low_stops']} |"
            in line
            for line in lines
        )
    signal_lines = sum(
        len(TABLE["spans"][n]["signals"][f"k{k}"]["rows"]) for n in sp.SPANS for k in (5, 2)
    )
    dated = [line for line in lines if line[2:6].isdigit() and line[6:7] == "-"]
    assert len(dated) == signal_lines
    assert all(line.count(" | ") == 12 for line in dated)
    assert lines.count("|" + "---|" * 13) == 12
    assert lines.count("|" + "---|" * 14) == 3
    assert lines.count("|" + "---|" * 9) == 3
    assert lines.count("|" + "---|" * 12) == 3
    year_lines = [line for line in lines if line[2:6].isdigit() and line[6:9] == " | "]
    assert len(year_lines) == sum(len(TABLE["spans"][n]["span"]["years"]) for n in sp.SPANS)
    trips = sum(
        len(TABLE["spans"][n]["span"]["strategies"][s]["trips"])
        for n in sp.SPANS
        for s in sp.TRIPS_SHOWN
    )
    assert sum(1 for line in lines if line.count(" | ") == 8 and line.startswith("| X")) == trips
    trip = next(
        t
        for t in TABLE["spans"]["qqq"]["span"]["strategies"]["x2_s5_line"]["trips"]
        if t["how"] == "line"
    )
    assert (
        f"| {sp.STRATEGY_TEXT['x2_s5_line']} | {trip['decision']} | {trip['reason']} | "
        f"{xr.pct(trip['vs_line'])} | {trip['sell']} | {trip['buy'] or '—'} | "
        f"200 日线涨到响时收盘价 | {trip['days_out']} | {xr.pct(trip['change'])} |"
    ) in lines


def test_the_summary_and_check_lines(tmp_path: Path) -> None:
    line = sp.summary_line(TABLE, tmp_path)
    assert line.startswith("structure_shift_periods: ndx 左右各 5 天 ")
    assert line.endswith(f"；结果目录 {tmp_path}")
    for name in ("ndx", "qqq", "dev"):
        rows = TABLE["spans"][name]["span"]["strategies"]
        k5 = len(TABLE["spans"][name]["signals"]["k5"]["rows"])
        shown = "、".join(f"{s} {xr.pct(rows[s]['total'])}" for s in (*SHIFTED5, *NEW, "hold"))
        assert f"{name} 左右各 5 天 {k5} 次，总收益 {shown}" in line
    q, n = CHECK["check"]["qqq"], CHECK["check"]["ndx"]
    assert sp.check_line(CHECK, tmp_path) == (
        f"structure_shift_periods check: ndx ok，{n['inputs']['closes']} 个收盘，"
        f"v0_close {len(n['entries'])} 次；qqq ok，2011-01-03..{q['last_close_date']}，"
        f"{q['sessions']} 个交易日，缺 0 天，最大间隔 3 天，v0_close {len(q['entries'])} 次；"
        f"结果目录 {tmp_path}"
    )


# Command line --------------------------------------------------------------------------------


@pytest.fixture
def loaded_cli(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []

    def load_qqq(root: Path) -> Any:
        calls.append(f"qqq {root}")
        return QQQ, GATE, LOADED, QQQ_WINDOW

    def load_ndx(root: Path) -> Any:
        calls.append(f"ndx {root}")
        return LEVELS, SOURCE, NDX_WINDOW

    monkeypatch.setattr(sp, "load_qqq", load_qqq)
    monkeypatch.setattr(en, "load_data", load_ndx)
    return calls


def test_check_writes_only_the_check_and_prints_one_line(
    loaded_cli: list[str], same_data: None, tmp_path: Path
) -> None:
    out = tmp_path / "out"
    result = CliRunner().invoke(sp.app, ["check", "--out", str(out), "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert loaded_cli == [f"qqq {tmp_path}", f"ndx {tmp_path}"]
    assert sorted(p.name for p in out.iterdir()) == ["structure_shift_periods_check.json"]
    text = out.joinpath("structure_shift_periods_check.json").read_text(encoding="utf-8")
    assert text == json.dumps(CHECK, indent=2, ensure_ascii=False) + "\n"
    assert result.output == sp.check_line(CHECK, out) + "\n"


def test_a_failed_check_writes_the_check_and_exits_one(
    loaded_cli: list[str], tmp_path: Path
) -> None:
    result = CliRunner().invoke(sp.app, ["check", "--out", str(tmp_path)])
    assert result.exit_code == 1
    written = json.loads(
        tmp_path.joinpath("structure_shift_periods_check.json").read_text(encoding="utf-8")
    )
    assert written["ok"] is False
    assert "ndx FAIL" in result.output and "qqq FAIL" in result.output


def test_check_refuses_an_existing_check_without_loading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def boom(root: Path) -> Any:
        raise AssertionError("loaded")

    monkeypatch.setattr(sp, "load_qqq", boom)
    monkeypatch.setattr(en, "load_data", boom)
    tmp_path.joinpath("structure_shift_periods_check.json").write_text("{}", encoding="utf-8")
    result = CliRunner().invoke(sp.app, ["check", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "structure_shift_periods: 已有检查结果，拒绝覆盖。\n"


def test_run_writes_both_files_and_prints_one_line(
    loaded_cli: list[str], same_data: None, tmp_path: Path
) -> None:
    out = tmp_path / "out"
    result = CliRunner().invoke(sp.app, ["run", "--out", str(out), "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert loaded_cli == [f"qqq {tmp_path}", f"ndx {tmp_path}"]
    text = out.joinpath("structure_shift_periods.json").read_text(encoding="utf-8")
    assert text == json.dumps(TABLE, indent=2, ensure_ascii=False) + "\n"
    assert out.joinpath("structure_shift_periods.md").read_text(encoding="utf-8") == (
        sp.render_markdown(TABLE)
    )
    assert sorted(p.name for p in out.iterdir()) == [
        "structure_shift_periods.json",
        "structure_shift_periods.md",
    ]
    assert result.output == sp.summary_line(TABLE, out) + "\n"


def test_run_after_check_in_the_same_directory(
    loaded_cli: list[str], same_data: None, tmp_path: Path
) -> None:
    runner = CliRunner()
    assert runner.invoke(sp.app, ["check", "--out", str(tmp_path)]).exit_code == 0
    assert runner.invoke(sp.app, ["run", "--out", str(tmp_path)]).exit_code == 0
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "structure_shift_periods.json",
        "structure_shift_periods.md",
        "structure_shift_periods_check.json",
    ]


@pytest.mark.parametrize("name", ["structure_shift_periods.json", "structure_shift_periods.md"])
def test_run_refuses_an_existing_result_without_loading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str
) -> None:
    def boom(root: Path) -> Any:
        raise AssertionError("loaded")

    monkeypatch.setattr(sp, "load_qqq", boom)
    monkeypatch.setattr(en, "load_data", boom)
    tmp_path.joinpath(name).write_text("{}", encoding="utf-8")
    result = CliRunner().invoke(sp.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "structure_shift_periods: 已有结果，拒绝覆盖。\n"
    assert [p.name for p in tmp_path.iterdir()] == [name]


@pytest.mark.parametrize("command", ["check", "run"])
@pytest.mark.parametrize(
    "error",
    [
        tr.TrialError("snapshot is not checked\nmore"),
        OSError("gone"),
        KeyError("qqq"),
        ValueError("窗口登记表里没有 qqq_after_2010_owner_print。"),
    ],
)
def test_a_load_failure_writes_nothing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, error: Exception, command: str
) -> None:
    def fail(root: Path) -> Any:
        raise error

    monkeypatch.setattr(sp, "load_qqq", fail)
    out = tmp_path / "out"
    result = CliRunner().invoke(sp.app, [command, "--out", str(out)])
    assert result.exit_code == 1
    assert result.output == (
        f"structure_shift_periods: 数据载入失败：{str(error).splitlines()[0]}\n"
    )
    assert not out.exists()


def test_a_failed_input_check_writes_only_the_json(loaded_cli: list[str], tmp_path: Path) -> None:
    result = CliRunner().invoke(sp.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "structure_shift_periods: 输入检查 FAIL，没有算两段新数据\n"
    assert [p.name for p in tmp_path.iterdir()] == ["structure_shift_periods.json"]
    written = json.loads(tmp_path.joinpath("structure_shift_periods.json").read_text("utf-8"))
    assert written["complete"] is False and "spans" not in written


def test_a_failed_1999_2010_check_writes_only_the_json(
    loaded_cli: list[str], same_data: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(sp, "DEV_FIRST_DAY", "1999-12-24")
    result = CliRunner().invoke(sp.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "structure_shift_periods: 1999–2010 重算核对 FAIL，没有算两段新数据\n"
    written = json.loads(tmp_path.joinpath("structure_shift_periods.json").read_text("utf-8"))
    assert written["check"]["dev"]["ok"] is False and "spans" not in written
