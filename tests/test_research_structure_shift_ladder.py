"""Acceptance tests for TRADING-2565-126: the structure-shift strategy with a scaled buy-back.

Requirement: knowledge qqq-options/auto-loop/period7/entry-exit-combo/structure-shift-ladder-
requirements-2026-11-04.md version 1.0 (owner decision scaled_entry_ladder).
Synthetic data only; no snapshot, no network.
"""

from __future__ import annotations

import ast
import json
import math
import re
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
from ai_trading_system.research_probes import entry_structure_shift as es
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_returns as xr
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import policy_table as pt
from ai_trading_system.research_probes import structure_shift_ladder as sl
from ai_trading_system.research_probes import structure_shift_periods as sp
from ai_trading_system.research_probes import structure_shift_rebuy as rb
from ai_trading_system.yaml_loader import safe_load_yaml_path

LOADED = load_settings()
GATE = tr.load_gate(PROJECT_ROOT / "research" / "_trials" / "prereg.yaml")
_, PLAN = tr.load_trial(lg.research_root(LOADED), pt.DATA_TRIAL)
PROBES = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes"
MODULE = PROBES / "structure_shift_ladder.py"
REGISTRY = PROJECT_ROOT / "config" / "research" / "research_window_registry.yaml"
POLICY = PROJECT_ROOT / "config" / "research" / "primary_research_window_policy.yaml"
QQQ_WINDOW = sp.registered_qqq_window(LOADED)
LADDER = ("x2_s5_ladder", "x2_s5_under_ladder")
SHOWN = ("x2_s5", "x2_s5_ladder", "x2_s5_under", "x2_s5_under_ladder", "hold")
LEVELS = ((0.9, 0.1), (0.8, 0.2), (0.7, 0.3), (0.6, 0.4))
COST = 0.0005
ADDED = ("by_ladder", "filled_sales", "exposure")
PRINTED_122 = {
    "dev": {
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
        "x2_s5_under": 0.985413,
        "x3_s5_under": 1.092173,
        "x2x3_s5_under": 1.011555,
        "x2_s5_line": 0.676461,
    },
    "qqq": {
        "x2_s5": 1.598219,
        "x3_s5": 0.282725,
        "x2x3_s5": 0.337662,
        "x2_s2": 1.685064,
        "x3_s2": 0.393602,
        "x2x3_s2": 0.450635,
        "x2_a": 0.52942,
        "x3_a": -0.113774,
        "x2x3_a": -0.113774,
        "hold": 14.43185,
        "x2_s5_under": 4.842585,
        "x3_s5_under": 4.55145,
        "x2x3_s5_under": 4.581502,
        "x2_s5_line": 2.3364,
    },
}


# Constants and imports ------------------------------------------------------------------------


def test_the_run_and_its_rows() -> None:
    assert sl.RUN_ID == "TRADING-2565-126"
    assert sl.OWNER_DECISION == "owner_decision:TRADING-2565:2026-11-04:scaled_entry_ladder"
    assert sl.SPANS == ("dev", "qqq")
    assert sl.LEVELS == LEVELS
    assert math.isclose(sum(part for _, part in sl.LEVELS), 1.0)
    assert sl.LADDER == {"x2_s5_ladder": "x2_s5", "x2_s5_under_ladder": "x2_s5_under"}
    assert sl.SHOWN == SHOWN
    assert sl.TRIPS_SHOWN == ("x2_s5", "x2_s5_under")
    assert (sl.CHECK_JSON, sl.RESULT_JSON, sl.RESULT_MD) == (
        "structure_shift_ladder_check.json",
        "structure_shift_ladder.json",
        "structure_shift_ladder.md",
    )
    for strategy in LADDER:
        assert "分批买回" in sl.STRATEGY_TEXT[strategy]
        assert "左右各 5 天" in sl.STRATEGY_TEXT[strategy]
    assert "只在 200 日线下算" in sl.STRATEGY_TEXT["x2_s5_under_ladder"]
    assert "只在 200 日线下算" not in sl.STRATEGY_TEXT["x2_s5_ladder"]
    for strategy in sp.STRATEGIES:
        assert sl.STRATEGY_TEXT[strategy] == sp.STRATEGY_TEXT[strategy]
    assert sl.HOW_TEXT == {**sp.HOW_TEXT, "ladder": "四档买满"}
    assert sl.FULL_TEXT == {
        "ladder": "四档买满",
        "recovery": "回到卖价",
        "entry": "入场信号",
        "shift": "结构转变",
        "none": "没买满",
    }


def test_the_reprint_pins_are_the_122_printed_tables() -> None:
    for name in sl.SPANS:
        assert rb.PERIODS_TOTALS[name] == PRINTED_122[name]
    assert rb.PERIODS_FIRST_DAY["dev"] == "1999-12-23"
    assert rb.PERIODS_FIRST_DAY["qqq"] == "2011-01-03"
    assert (rb.PERIODS_SESSIONS["dev"], rb.PERIODS_SESSIONS["qqq"]) == (2771, 3966)


def test_the_rule_text_and_the_note() -> None:
    for words in (
        "只印不判",
        "不是交易建议",
        "两段都已看过 TRADING-2565-122 的结果",
        "档位和比例是 Baker 定的",
        "看过 2000–2010 的探索以后定的",
        "总收益都等于它们印的才印新行",
        "纳斯达克 100 这次不读",
        "真检验只有前瞻记录",
    ):
        assert words in sl.RULE_TEXT, words
    for words in (
        "X2 和 L 止损都算",
        "卖出那个收盘记作 X",
        "从卖出的下一个收盘起",
        "收盘不高于 0.9X、0.8X、0.7X、0.6X",
        "10%、20%、30%、40%",
        "每档只买一次，一个收盘可以成交几档",
        "0.9X 那档成交以后，第一个收盘不低于 X",
        "不管成交过几档",
        "同一个收盘算 v0_close",
        "带那次的 L 止损",
        "依次算四档买满、回到 X、v0_close 和结构转变",
        "从买满那个收盘起重新记最高收盘",
        "没买满之前不卖",
        "按成交金额扣 0.05%",
        "平均仓位",
        "离 200 日线",
    ):
        assert words in sl.NOTE, words


def _imports(text: str) -> set[str]:
    imported: set[str] = set()
    for node in ast.walk(ast.parse(text)):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.update(f"{node.module}.{alias.name}" for alias in node.names)
    return imported


def _code(text: str) -> str:
    """The module without string literals and Baker's tranche table."""
    tree = ast.parse(text)
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            node.value = ""
        if isinstance(node, ast.AnnAssign | ast.Assign):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(t, ast.Name) and t.id == "LEVELS" for t in targets):
                node.value = ast.Constant(None)
    return ast.unparse(tree)


def test_the_module_reuses_the_122_module_and_reads_only_qqq() -> None:
    text = MODULE.read_text(encoding="utf-8")
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
        "ai_trading_system.research_probes.entry_structure_shift",
        "ai_trading_system.research_probes.entry_timing_counts",
        "ai_trading_system.research_probes.exit_rules_counts",
        "ai_trading_system.research_probes.exit_rules_returns",
        "ai_trading_system.research_probes.structure_shift_periods",
        "ai_trading_system.research_probes.structure_shift_rebuy",
    )
    for name in _imports(text):
        assert name.startswith(allowed), name
    for word in (
        "entry_timing_ndx",
        "ndx_inputs",
        "load_data",
        "us_market",
        "French",
        "vix",
        "VIX",
        "vxn",
        "ndx_tr",
        "lev2",
        "lev3",
        "cash_return",
        "ma200_forward",
        "window_reads",
        "lg.append",
        ".unlink(",
        "urllib",
        "requests",
        "import numpy",
        "import pandas",
        "safe_load_yaml",
        "def exit_under",
        "def exit_day",
        "def qqq_inputs",
        "def load_qqq",
        "def registered_qqq_window",
        "def dev_check",
        "def periods_check",
        "def v0_entries",
        "def shifts",
        "def metrics",
        "def year_returns",
        "def moving_average",
        "PERIODS_TOTALS",
        "Yahoo",
        "盘中",
    ):
        assert word not in text, word
    for number in ("0.92", "0.08", "0.0005", "200", "50", "0.9", "0.6", "0.4"):
        assert re.search(rf"(?<![\d.]){re.escape(number)}(?![\d])", _code(text)) is None, number
    assert text.count(".write_text(") == 3
    for call in (
        "sp.span_result(",
        "sp.exit_under(",
        "sp.qqq_inputs(",
        "sp.load_qqq(",
        "sp.dev_check(",
        "sp.rule_block(",
        "sp.v0_entries(",
        "rb.periods_check(",
        "es.shifts(",
        "ec.moving_average(",
        "xc.prepare(",
        "cb.exit_day",
        "cb.metrics(",
        "cb.year_returns(",
        "cb.COST",
    ):
        assert call in text, call


# The window registry --------------------------------------------------------------------------


def _windows() -> dict[str, Any]:
    raw = safe_load_yaml_path(REGISTRY)
    assert isinstance(raw, dict)
    windows = raw["windows"]
    assert isinstance(windows, dict)
    return windows


def _note(start: str, end: str) -> str:
    text = REGISTRY.read_text(encoding="utf-8")
    block = text[text.index(start) :]
    return " ".join(block[: block.index(end)].replace("#", " ").split())


def test_the_registry_note_names_this_reader() -> None:
    qqq = _note(
        "# Baker's one-time print-only read of QQQ after 2010", "  qqq_after_2010_owner_print:"
    )
    for words in (
        "owner_decision:TRADING-2565:2026-11-03:structure_shift_other_periods",
        "second read",
        "owner_decision:TRADING-2565:2026-11-04:structure_shift_rebuy_above_line",
        "third read",
        "TRADING-2565-126",
        "owner_decision:TRADING-2565:2026-11-04:scaled_entry_ladder",
        "research_probes/structure_shift_ladder.py",
        "already seen history",
        "forward ledger",
    ):
        assert words in qqq, words
    assert qqq.index("second read") < qqq.index("third read")
    ndx = _note("# Baker's print-only read of the 200-day pullback rule", "  ndx_price_1986_1999:")
    assert "TRADING-2565-126" not in ndx and "scaled_entry_ladder" not in ndx
    assert "sixth read" in ndx


def test_the_windows_and_the_policy_are_unchanged() -> None:
    windows = _windows()
    entry = windows["qqq_after_2010_owner_print"]
    assert (str(entry["start"]), entry["end"], entry["role"], entry["read_policy"]) == (
        "2011-01-03",
        "latest",
        "owner_print_only",
        "once_per_owner_decision",
    )
    assert entry["allowed_usage"] == ["owner_requested_print_only_read"]
    assert entry["caveats"] == [
        "overlaps_qqq_long_history_validation_and_the_consistency_window",
        "period_7_charter_one_time_exception",
        "after_this_read_2011_onward_is_seen_history_for_the_entry_timing_and_exit_families",
        "qqq_adjusted_close_on_the_snapshot_calendar",
    ]
    ndx = windows["ndx_price_1986_1999"]
    assert (str(ndx["start"]), str(ndx["end"]), ndx["role"], ndx["read_policy"]) == (
        "1986-01-02",
        "1999-03-09",
        "owner_print_only",
        "once_per_owner_decision",
    )
    assert entry["blocked_usage"] == ndx["blocked_usage"]
    validation = windows["qqq_long_history_validation"]
    assert (validation["start"], validation["end"]) == (date(2011, 1, 3), date(2021, 2, 19))
    development = windows["qqq_long_history_development"]
    assert (development["start"], development["end"]) == (date(1999, 3, 10), date(2010, 12, 31))
    policy = safe_load_yaml_path(POLICY)
    for roles in policy["question_windows"].values():
        assert "qqq_after_2010_owner_print" not in roles.values()


# The ladder by hand ---------------------------------------------------------------------------


def _prep(close: list[float]) -> xc.Prepared:
    n = len(close)
    dates = tuple(date(2001, 1, 1) + timedelta(days=i) for i in range(n))
    return xc.prepare(dates, close)


def _day(data: xc.Prepared, p: int) -> str:
    return data.dates[p].isoformat()


def _scripted(decisions: dict[int, int], calls: list[int] | None = None) -> sp.Fire:
    """Exit decisions keyed by the buying close; record every buying close asked about."""

    def fire(rule: str, data: xc.Prepared, b: int) -> tuple[int, str] | None:
        assert rule == "x2"
        if calls is not None:
            calls.append(b)
        d = decisions.get(b)
        return (d, "X2") if d is not None else None

    return fire


def test_signal_after_by_hand() -> None:
    close = [10.0, 9.0, 8.0, 7.0, 8.0, 9.0, 10.0, 11.0]
    shift = es.Shift(cross=1, low=3, high=0, entry=6)
    assert sl.signal_after(close, [5], [shift], 2) == (5, "entry", None)
    assert sl.signal_after(close, [6], [shift], 2) == (6, "entry", None)
    assert sl.signal_after(close, [7], [shift], 2) == (6, "shift", 7.0)
    assert sl.signal_after(close, [2], [shift], 2) == (6, "shift", 7.0)
    assert sl.signal_after(close, [], [shift], 6) is None
    assert sl.signal_after(close, [], [], 0) is None


def test_two_tranches_then_back_at_the_sale_close() -> None:
    close = [100.0, 104.0, 100.0, 100.0, 95.0, 90.0, 85.0, 79.0, 101.0, 103.0]
    data = _prep(close)
    path = sl.walk_ladder(data, [], [], 0, _scripted({0: 2}))
    route = path.route
    assert route.sells == (3,) and route.buys == (0, 5, 7, 8)
    assert route.held == (False, True, True, True, False, False, True, True, True, True)
    start = (1 - COST) / 100.0 * 100.0 * (1 - COST)
    units = 0.1 * start / 90.0 + 0.2 * start / 79.0 + 0.7 * start / 101.0
    assert route.trips == (
        {
            "decision": _day(data, 2),
            "reason": "X2",
            "sell": _day(data, 3),
            "buy": _day(data, 8),
            "how": "recovery",
            "days_out": 5,
            "change": pytest.approx(start / units / 100.0 - 1.0, rel=1e-12),
            "fills": [
                {"level": 0.9, "date": _day(data, 5), "share": 0.1, "at": 0.9},
                {"level": 0.8, "date": _day(data, 7), "share": 0.2, "at": 0.79},
            ],
            "rest_share": pytest.approx(0.7, rel=1e-12),
        },
    )
    shares = (0.1 * start / 90.0 + 0.2 * start / 79.0 + 0.7 * start / 101.0) * (1 - COST)
    expected = [
        1 - COST,
        (1 - COST) * 1.04,
        (1 - COST),
        start,
        start,
        0.9 * start + 0.1 * start * (1 - COST),
        0.9 * start + (0.1 * start / 90.0) * (1 - COST) * 85.0,
        0.7 * start + (0.1 * start / 90.0 + 0.2 * start / 79.0) * (1 - COST) * 79.0,
        shares * 101.0,
        shares * 103.0,
    ]
    assert len(path.values) == len(expected)
    for mine, theirs in zip(path.values, expected, strict=True):
        assert math.isclose(mine, theirs, rel_tol=1e-12)
    w5 = 0.0
    w6 = (0.1 * start / 90.0) * (1 - COST) * 90.0 / expected[5]
    w7 = (0.1 * start / 90.0) * (1 - COST) * 85.0 / expected[6]
    w8 = (0.1 * start / 90.0 + 0.2 * start / 79.0) * (1 - COST) * 79.0 / expected[7]
    weights = [1.0, 1.0, 1.0, 0.0, w5, w6, w7, w8, 1.0]
    assert path.exposure is not None
    assert math.isclose(path.exposure, sum(weights) / 9, rel_tol=1e-12)


def test_a_level_fills_on_a_close_equal_to_it_but_not_above() -> None:
    close = [100.0, 100.0, 100.0, 90.0000001, 90.0, 120.0]
    data = _prep(close)
    trip = sl.walk_ladder(data, [], [], 0, _scripted({0: 1})).route.trips[0]
    assert [f["date"] for f in trip["fills"]] == [_day(data, 4)]
    assert trip["how"] == "recovery" and trip["buy"] == _day(data, 5)


def test_back_at_the_sale_close_needs_a_filled_tranche() -> None:
    close = [100.0, 100.0, 100.0, 101.0, 100.0, 99.0, 98.0]
    data = _prep(close)
    route = sl.walk_ladder(data, [], [], 0, _scripted({0: 1})).route
    assert route.trips[0]["how"] == "none" and route.trips[0]["buy"] is None
    assert route.trips[0]["fills"] == [] and route.trips[0]["rest_share"] is None
    assert route.trips[0]["days_out"] == 4
    assert math.isclose(route.trips[0]["change"], 98.0 / 100.0 - 1.0, rel_tol=1e-12)
    assert route.held == (False, True, True, False, False, False, False)


def test_several_tranches_on_one_close_and_the_ladder_completes() -> None:
    close = [100.0, 100.0, 100.0, 65.0, 70.0, 59.0, 62.0, 66.0]
    data = _prep(close)
    calls: list[int] = []
    path = sl.walk_ladder(data, [], [], 0, _scripted({0: 1, 5: 7}, calls))
    trip = path.route.trips[0]
    assert [(f["level"], f["date"], f["share"]) for f in trip["fills"]] == [
        (0.9, _day(data, 3), 0.1),
        (0.8, _day(data, 3), 0.2),
        (0.7, _day(data, 3), 0.3),
        (0.6, _day(data, 5), 0.4),
    ]
    assert trip["how"] == "ladder" and trip["rest_share"] is None
    assert trip["buy"] == _day(data, 5) and trip["days_out"] == 3
    units = 0.6 / 65.0 + 0.4 / 59.0
    assert math.isclose(trip["change"], 1.0 / units / 100.0 - 1.0, rel_tol=1e-12)
    assert path.route.buys == (0, 3, 5)
    assert calls == [0, 5]
    assert path.route.sells == (2,)


def test_no_sale_before_full_and_the_high_restarts_at_the_full_close() -> None:
    close = [100.0, 100.0, 100.0, 90.0, 80.0, 75.0, 100.0, 100.0, 90.0, 90.0]
    data = _prep(close)
    calls: list[int] = []
    route = sl.walk_ladder(data, [], [], 0, _scripted({0: 1, 6: 7}, calls)).route
    assert calls == [0, 6]
    assert [t["how"] for t in route.trips] == ["recovery", "none"]
    assert route.sells == (2, 8)


def test_the_signal_buys_the_rest_before_any_tranche() -> None:
    close = [100.0, 100.0, 100.0, 96.0, 94.0, 97.0, 99.0]
    data = _prep(close)
    route = sl.walk_ladder(data, [5], [], 0, _scripted({0: 1})).route
    trip = route.trips[0]
    assert trip["how"] == "entry" and trip["buy"] == _day(data, 5)
    assert trip["fills"] == [] and trip["rest_share"] == 1.0
    assert math.isclose(trip["change"], 97.0 / 100.0 - 1.0, rel_tol=1e-12)


def test_a_tranche_and_the_signal_on_one_close() -> None:
    close = [100.0, 100.0, 100.0, 95.0, 89.0, 92.0]
    data = _prep(close)
    route = sl.walk_ladder(data, [4], [], 0, _scripted({0: 1})).route
    trip = route.trips[0]
    assert [f["date"] for f in trip["fills"]] == [_day(data, 4)]
    assert trip["how"] == "entry" and trip["buy"] == _day(data, 4)
    assert math.isclose(trip["rest_share"], 0.9, rel_tol=1e-12)
    assert route.buys == (0, 4)


def test_back_at_the_sale_close_wins_over_a_shift_on_the_same_close() -> None:
    close = [100.0, 100.0, 100.0, 89.0, 95.0, 101.0, 99.0, 50.0, 49.0]
    data = _prep(close)
    shift = es.Shift(cross=2, low=3, high=1, entry=5)
    route = sl.walk_ladder(data, [], [shift], 0, _scripted({0: 1})).route
    assert route.trips[0]["how"] == "recovery"
    assert route.sells == (2,)


def test_a_shift_carries_its_low_stop_and_the_ladder_follows_a_low_stop() -> None:
    # Sale at 3 (X = 100); the close at 4 fills 0.9X; the shift at 6 buys the rest with
    # L = close[4] = 88; the close at 8 falls under L, the sale is at 9 (X = 87) and the close
    # at 10 (78) fills the new 0.9X tranche, the close at 11 is back above 87.
    close = [100.0, 100.0, 100.0, 100.0, 88.0, 92.0, 96.0, 93.0, 87.5, 87.0, 78.0, 88.0, 90.0]
    data = _prep(close)
    shift = es.Shift(cross=3, low=4, high=2, entry=6)
    route = sl.walk_ladder(data, [], [shift], 0, _scripted({0: 2})).route
    assert [t["reason"] for t in route.trips] == ["X2", "L"]
    assert [t["how"] for t in route.trips] == ["shift", "recovery"]
    assert route.sells == (3, 9)
    assert math.isclose(route.trips[0]["rest_share"], 0.9, rel_tol=1e-12)
    assert route.trips[1]["fills"][0]["date"] == _day(data, 10)
    assert route.trips[1]["buy"] == _day(data, 11)


def test_no_low_stop_after_the_ladder_or_the_recovery() -> None:
    close = [100.0, 100.0, 100.0, 89.0, 101.0, 70.0, 60.0, 55.0, 50.0]
    data = _prep(close)
    shift = es.Shift(cross=2, low=3, high=1, entry=9)
    route = sl.walk_ladder(data, [], [shift], 0, _scripted({0: 1})).route
    assert route.sells == (2,)
    assert route.trips[0]["how"] == "recovery"


def test_no_sale_on_the_last_close() -> None:
    close = [100.0, 100.0, 100.0, 100.0]
    data = _prep(close)
    path = sl.walk_ladder(data, [], [], 0, _scripted({0: 3}))
    assert path.route.sells == () and path.route.trips == ()
    assert path.route.held == (False, True, True, True)
    assert math.isclose(path.values[-1], 1 - COST, rel_tol=1e-12)
    assert path.exposure == 1.0


def test_a_sale_on_the_last_close_but_one() -> None:
    close = [100.0, 100.0, 100.0, 97.0]
    data = _prep(close)
    path = sl.walk_ladder(data, [], [], 0, _scripted({0: 2}))
    trip = path.route.trips[0]
    assert path.route.sells == (3,)
    assert trip["how"] == "none" and trip["days_out"] == 0
    assert math.isclose(trip["change"], 0.0, abs_tol=1e-12)
    assert math.isclose(path.values[-1], (1 - COST) * 0.97 * (1 - COST), rel_tol=1e-12)


def _path(seed: int, n: int = 700) -> list[float]:
    rng = np.random.default_rng(seed)
    return [float(v) for v in 100.0 * np.cumprod(1.0 + rng.normal(0.0003, 0.02, n))]


def _strip(trip: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in trip.items() if k not in ("fills", "rest_share")}


def _same_trips(mine: tuple[dict[str, Any], ...], theirs: tuple[dict[str, Any], ...]) -> None:
    assert len(mine) == len(theirs)
    for a, b in zip(mine, theirs, strict=True):
        a = _strip(a)
        assert a.keys() == b.keys()
        for key in a:
            if isinstance(a[key], float):
                assert math.isclose(a[key], b[key], rel_tol=1e-12, abs_tol=1e-15), key
            else:
                assert a[key] == b[key], key


@pytest.mark.parametrize("seed", range(4))
def test_without_a_tranche_each_row_is_its_base_row(
    seed: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    close = _path(seed)
    data = xc.prepare(tuple(date(2001, 1, 1) + timedelta(days=i) for i in range(700)), close)
    line = ec.moving_average(close)
    entries = sp.v0_entries(close, 0)
    moves = es.shifts(close, line, 5, 200)
    monkeypatch.setattr(sl, "LEVELS", ((0.01, 0.1), (0.009, 0.2), (0.008, 0.3), (0.007, 0.4)))

    def under(rule: str, prepared: xc.Prepared, b: int) -> tuple[int, str] | None:
        return sp.exit_under(rule, prepared, line, b)

    for fire, base in (
        (cb.exit_day, es.simulate("x2_s5", data, entries, moves, 200)),
        (under, sp.walk("x2", data, entries, moves, 200, under)),
    ):
        path = sl.walk_ladder(data, entries, moves, 200, fire)
        assert path.route.sells == base.sells
        assert path.route.held == base.held
        assert len(base.trips) >= 2
        _same_trips(path.route.trips, base.trips)
        values = cb.equity(close, base, 200)
        assert len(path.values) == len(values)
        for mine, theirs in zip(path.values, values, strict=True):
            assert math.isclose(mine, theirs, rel_tol=1e-12)
        held = base.held[201:]
        assert path.exposure is not None
        assert math.isclose(path.exposure, sum(held) / len(held), rel_tol=1e-12)


# Synthetic QQQ ruler --------------------------------------------------------------------------


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
        ("2001-03-01", "2001-09-21", 0.45),
        ("2008-09-15", "2008-11-20", 0.6),
        ("2011-07-22", "2011-10-03", 0.75),
        ("2013-05-01", "2013-05-20", 0.9),
        ("2015-07-20", "2016-02-11", 0.7),
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


QQQ = _qqq_data()
CUT = gp.cut(QQQ)
STEP0 = xc.check_inputs(CUT, LOADED, GATE)
DEV = ec.closes(QQQ.returns["qqq"], date(2010, 12, 31))
FULL = ec.closes(QQQ.returns["qqq"], date.max)
FIRST_IN = next(p for p, day in enumerate(FULL.dates) if day >= date(2011, 1, 3))
DEV_FOUND = tuple(
    DEV.dates[t.entry].isoformat() for t in ec.scan(DEV.close, 0) if t.counted and t.entry
)


def _same(mp: pytest.MonkeyPatch) -> None:
    a = STEP0["entry_set_a"]
    mp.setattr(xr, "STEP0_TRADES", a["trades"])
    mp.setattr(xr, "STEP0_FIRST", a["first"])
    mp.setattr(xr, "STEP0_LAST", a["last"])
    mp.setattr(xr, "STEP0_K", STEP0["scale"]["k"])
    mp.setattr(xr, "H_MATCH", {r: STEP0["exits"][r]["h_match"] for r in xc.RULES})
    mp.setattr(cb, "V0_ENTRIES", DEV_FOUND)
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
    Q_IN = sp.qqq_inputs(QQQ, LOADED, GATE, QQQ_WINDOW)
    assert Q_IN.check["ok"] is True
    SPANS_122 = {
        "dev": sp.span_result(Q_IN.dev, Q_IN.dev_first_in),
        "qqq": sp.span_result(Q_IN.full, Q_IN.first_in),
    }
    PERIOD_PINS = {
        "PERIODS_TOTALS": {
            name: {s: round(v["total"], 6) for s, v in block["span"]["strategies"].items()}
            for name, block in SPANS_122.items()
        },
        "PERIODS_FIRST_DAY": {n: b["span"]["first_day"] for n, b in SPANS_122.items()},
        "PERIODS_SESSIONS": {n: b["span"]["sessions"] for n, b in SPANS_122.items()},
    }
    for _name, _value in PERIOD_PINS.items():
        _mp.setattr(rb, _name, _value)
    ARGS = (QQQ, LOADED, GATE, QQQ_WINDOW)
    TABLE = sl.run_table(*ARGS)
    CHECK = sl.check_table(*ARGS)


@pytest.fixture
def same_data(monkeypatch: pytest.MonkeyPatch) -> None:
    _same(monkeypatch)
    for name, value in DEV_PINS.items():
        monkeypatch.setattr(sp, name, value)
    for name, value in PERIOD_PINS.items():
        monkeypatch.setattr(rb, name, value)


def test_the_synthetic_data_are_not_the_real_ones() -> None:
    assert DEV_FOUND != cb.V0_ENTRIES and len(DEV_FOUND) >= 2
    assert FULL.dates[-1] < sp.QQQ_LAST_MIN
    assert PERIOD_PINS["PERIODS_TOTALS"] != PRINTED_122
    assert PERIOD_PINS["PERIODS_SESSIONS"]["qqq"] != 3966
    assert TABLE["complete"] is True


# Checks --------------------------------------------------------------------------------------


def test_the_real_constants_stop_the_table() -> None:
    result = sl.run_table(*ARGS)
    assert result["complete"] is False
    assert result["check"]["qqq"]["ok"] is False
    assert result["check"]["dev"] is None and result["check"]["periods"] is None
    assert list(result) == ["run_id", "owner_decision", "source", "rule", "check", "complete"]
    assert sl.check_table(*ARGS)["ok"] is False


def test_the_input_check_is_the_122_check(same_data: None) -> None:
    q = sp.qqq_inputs(QQQ, LOADED, GATE, QQQ_WINDOW)
    assert CHECK["check"] == {"qqq": q.check}
    assert TABLE["check"]["qqq"] == q.check
    assert CHECK["ok"] is True


def test_a_failed_qqq_check_stops_the_table(
    same_data: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sp, "QQQ_SESSIONS", (1, 2))
    result = sl.run_table(*ARGS)
    assert result["complete"] is False and result["check"]["qqq"]["ok"] is False
    assert result["check"]["dev"] is None and result["check"]["periods"] is None
    assert sl.check_table(*ARGS)["ok"] is False


def test_a_changed_120_table_stops_the_new_rows(
    same_data: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sp, "DEV_FIRST_DAY", "1999-12-24")
    result = sl.run_table(*ARGS)
    assert result["complete"] is False and "spans" not in result
    assert result["check"]["dev"]["ok"] is False
    assert all(c["ok"] for c in result["check"]["periods"].values())


def _changed(name: str, key: str) -> dict[str, Any]:
    pins = json.loads(json.dumps(PERIOD_PINS))
    if key == "total":
        pins["PERIODS_TOTALS"][name]["x2_s5"] += 0.000001
    elif key == "first_day":
        pins["PERIODS_FIRST_DAY"][name] = "1990-01-02"
    else:
        pins["PERIODS_SESSIONS"][name] += 1
    assert isinstance(pins, dict)
    return pins


@pytest.mark.parametrize("name", ["dev", "qqq"])
@pytest.mark.parametrize("key", ["total", "first_day", "sessions"])
def test_each_reprint_check_alone_stops_the_new_rows(
    same_data: None, monkeypatch: pytest.MonkeyPatch, name: str, key: str
) -> None:
    for pin, value in _changed(name, key).items():
        monkeypatch.setattr(rb, pin, value)
    result = sl.run_table(*ARGS)
    assert result["complete"] is False and "spans" not in result
    assert result["check"]["dev"]["ok"] is True
    periods = result["check"]["periods"]
    assert list(periods) == ["dev", "qqq"]
    assert [n for n, c in periods.items() if not c["ok"]] == [name]


def test_the_periods_check_block(same_data: None) -> None:
    for name in sl.SPANS:
        assert TABLE["check"]["periods"][name] == {
            "first_day": PERIOD_PINS["PERIODS_FIRST_DAY"][name],
            "sessions": PERIOD_PINS["PERIODS_SESSIONS"][name],
            "totals": PERIOD_PINS["PERIODS_TOTALS"][name],
            "ok": True,
        }
    assert TABLE["check"]["dev"] == sp.dev_check(SPANS_122["dev"])


def test_the_check_table_has_no_signals_or_returns() -> None:
    assert list(CHECK) == ["run_id", "owner_decision", "source", "rule", "check", "ok"]
    assert CHECK["check"] == {"qqq": TABLE["check"]["qqq"]}
    text = json.dumps(CHECK["check"])
    for word in ("signals", "spans", "total", "trips", "plain_mean", "periods", "ndx"):
        assert word not in text, word


def test_the_head_and_the_rule_block() -> None:
    assert list(TABLE) == [
        "run_id",
        "owner_decision",
        "source",
        "rule",
        "check",
        "complete",
        "spans",
    ]
    assert TABLE["run_id"] == "TRADING-2565-126"
    assert TABLE["owner_decision"] == sl.OWNER_DECISION
    assert TABLE["source"] == {"qqq": {"snapshot_id": PLAN.data.snapshot_id}}
    rule = dict(TABLE["rule"])
    ladder = rule.pop("ladder")
    assert rule == sp.rule_block()
    assert ladder == {
        "strategies": list(LADDER),
        "bases": {"x2_s5_ladder": "x2_s5", "x2_s5_under_ladder": "x2_s5_under"},
        "spans": ["dev", "qqq"],
        "levels": [[0.9, 0.1], [0.8, 0.2], [0.7, 0.3], [0.6, 0.4]],
        "fill": "close_at_or_below_level_times_sale_close_buys_that_share_of_sale_cash",
        "recovery": "after_a_tranche_first_close_at_or_above_the_sale_close_buys_the_rest",
        "signal": "first_entry_or_shift_after_the_sale_buys_the_rest_tie_is_entry",
        "order": ["levels", "ladder", "recovery", "entry", "shift"],
        "after": ["X2", "L"],
        "low_stop": "kept_when_a_shift_buys_the_rest",
        "partial": "no_sale_before_full",
        "restart": "x2_high_from_the_full_close",
        "cost": "per_trade_share_of_traded_value",
    }
    assert CHECK["rule"] == TABLE["rule"]
    assert list(TABLE["check"]) == ["qqq", "dev", "periods"]
    assert list(TABLE["spans"]) == ["dev", "qqq"]


# The spans ------------------------------------------------------------------------------------


def _old(row: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in row.items() if k not in ADDED}


@pytest.mark.parametrize("name", ["dev", "qqq"])
def test_each_span_reprints_the_122_span(name: str) -> None:
    mine, theirs = TABLE["spans"][name], SPANS_122[name]
    assert list(mine) == list(theirs)
    for key in ("lookback_from", "scan_from", "signals"):
        assert mine[key] == theirs[key]
    span = mine["span"]
    assert list(span["strategies"]) == [*sp.STRATEGIES, *LADDER]
    for strategy in sp.STRATEGIES:
        row = span["strategies"][strategy]
        assert _old(row) == theirs["span"]["strategies"][strategy]
        assert (row["by_ladder"], row["filled_sales"]) == (0, 0)
        assert row["exposure"] == row["in_market"]
    for key in ("first_day", "last_day", "sessions", "entries"):
        assert span[key] == theirs["span"][key]
    for mine_year, their_year in zip(span["years"], theirs["span"]["years"], strict=True):
        assert {k: v for k, v in mine_year.items() if k not in LADDER} == their_year
        assert list(mine_year)[-2:] == list(LADDER)


def _own(series: ec.Closes, first_in: int, under: bool) -> dict[str, Any]:
    """Recompute one scaled row with an independent day-by-day state machine."""
    dates, close = series.dates, series.close
    n = len(close)
    s0 = max(first_in, 200)
    line = ec.moving_average(close)
    entries = sp.v0_entries(close, first_in)
    moves = es.shifts(close, line, 5, s0)
    shares, cash = (1 - COST) / close[s0], 0.0
    values = [shares * close[s0]]
    held: list[bool] = []
    weights: list[float] = []
    sells: list[int] = []
    trips: list[dict[str, Any]] = []
    holding, high = True, close[s0]
    floor: float | None = None
    pending: tuple[int, str] | None = None
    x = start = units = 0.0
    sale = 0
    todo: list[tuple[float, float]] = []
    signal: tuple[int, str, float | None] | None = None
    for p in range(s0 + 1, n):
        held.append(shares > 0)
        weights.append(shares * close[p - 1] / values[-1])
        c = close[p]
        if holding and pending is not None:
            cash, shares = shares * c * (1 - COST), 0.0
            holding, sale, x, start, units = False, p, c, cash, 0.0
            sells.append(p)
            trips.append(
                {
                    "decision": dates[pending[0]].isoformat(),
                    "reason": pending[1],
                    "sell": dates[p].isoformat(),
                    "fills": [],
                }
            )
            todo, pending, floor = list(LEVELS), None, None
            entry = min((e for e in entries if e > p), default=None)
            shift = min((m.entry for m in moves if m.entry > p), default=None)
            if entry is not None and (shift is None or entry <= shift):
                signal = (entry, "entry", None)
            elif shift is not None:
                low = next(m.low for m in moves if m.entry == shift)
                signal = (shift, "shift", close[low])
            else:
                signal = None
            values.append(cash)
            continue
        if holding:
            high = max(high, c)
            at = line[p]
            if c / high <= 1.0 - cb.TRAIL and (not under or (at is not None and c < at)):
                pending = (p, "X2")
            elif floor is not None and c < floor:
                pending = (p, "L")
            if pending is not None and p == n - 1:
                pending = None
            values.append(shares * c)
            continue
        for level, part in list(todo):
            if c <= level * x:
                todo.remove((level, part))
                spend = cash if not todo else part * start
                shares += spend * (1 - COST) / c
                cash -= spend
                units += spend / c
                trips[-1]["fills"].append(
                    {"level": level, "date": dates[p].isoformat(), "share": part, "at": c / x}
                )
        how: str | None = None
        if not todo:
            how = "ladder"
        elif trips[-1]["fills"] and c >= x:
            how = "recovery"
        elif signal is not None and signal[0] == p:
            how, floor = signal[1], signal[2]
        rest: float | None = None
        if how in ("recovery", "entry", "shift"):
            rest = cash / start
            shares, units, cash = shares + cash * (1 - COST) / c, units + cash / c, 0.0
        if how is not None:
            holding, high = True, c
            trips[-1].update(
                {
                    "buy": dates[p].isoformat(),
                    "how": how,
                    "days_out": p - sale,
                    "change": start / units / x - 1.0,
                    "rest_share": rest,
                }
            )
        values.append(shares * c + cash)
    if not holding:
        units += cash / close[n - 1]
        trips[-1].update(
            {
                "buy": None,
                "how": "none",
                "days_out": n - 1 - sale,
                "change": start / units / x - 1.0,
                "rest_share": None,
            }
        )
    return {
        "values": values,
        "held": (False,) * (s0 + 1) + tuple(held),
        "sells": tuple(sells),
        "trips": trips,
        "exposure": sum(weights) / len(weights),
    }


def _series(name: str) -> tuple[ec.Closes, int]:
    if name == "dev":
        return Q_IN.dev, Q_IN.dev_first_in
    return Q_IN.full, Q_IN.first_in


KEYS = ("decision", "reason", "sell", "fills", "buy", "how", "days_out", "change", "rest_share")


@pytest.mark.parametrize("name", ["dev", "qqq"])
@pytest.mark.parametrize("strategy", LADDER)
def test_each_scaled_row_recomputed(name: str, strategy: str) -> None:
    series, first_in = _series(name)
    own = _own(series, first_in, strategy == "x2_s5_under_ladder")
    row = TABLE["spans"][name]["span"]["strategies"][strategy]
    assert len(row["trips"]) == len(own["trips"])
    for mine, theirs in zip(row["trips"], own["trips"], strict=True):
        assert set(mine) == {*KEYS, "vs_line"}
        for key in KEYS:
            if isinstance(theirs[key], float):
                assert math.isclose(mine[key], theirs[key], rel_tol=1e-12), key
            elif key == "fills":
                assert len(mine[key]) == len(theirs[key])
                for a, b in zip(mine[key], theirs[key], strict=True):
                    assert a.keys() == b.keys()
                    assert (a["level"], a["date"], a["share"]) == (
                        b["level"],
                        b["date"],
                        b["share"],
                    )
                    assert math.isclose(a["at"], b["at"], rel_tol=1e-12)
            else:
                assert mine[key] == theirs[key], key
    s0 = max(first_in, 200)
    route = cb.Route(own["held"], (s0,), own["sells"], tuple(own["trips"]))
    assert math.isclose(row["total"], own["values"][-1] - 1.0, rel_tol=1e-12)
    expected = cb.metrics(series.dates, own["values"], route, s0)
    for key, value in expected.items():
        assert row[key] == value or math.isclose(row[key], value, rel_tol=1e-9), key
    assert math.isclose(row["exposure"], own["exposure"], rel_tol=1e-12)
    trips = own["trips"]
    assert row["sells"] == len(own["sells"])
    assert row["by_ladder"] == sum(t["how"] == "ladder" for t in trips)
    assert row["by_shift"] == sum(t["how"] == "shift" for t in trips)
    assert row["low_stops"] == sum(t["reason"] == "L" for t in trips)
    assert row["filled_sales"] == sum(bool(t["fills"]) for t in trips)
    assert (
        row["by_ladder"] + row["by_recovery"] + row["by_entry"] + row["by_shift"] + row["not_back"]
        == row["sells"]
    )
    annual = cb.year_returns(series.dates, own["values"], s0)
    for year in TABLE["spans"][name]["span"]["years"]:
        assert math.isclose(year[strategy], annual[year["year"]], rel_tol=1e-9, abs_tol=1e-12)


@pytest.mark.parametrize("name", ["dev", "qqq"])
def test_every_scaled_trip_carries_its_distance_from_the_line(name: str) -> None:
    series, _ = _series(name)
    line = ec.moving_average(series.close)
    positions = {d.isoformat(): p for p, d in enumerate(series.dates)}
    for strategy in LADDER:
        for trip in TABLE["spans"][name]["span"]["strategies"][strategy]["trips"]:
            d = positions[trip["decision"]]
            at = line[d]
            assert at is not None
            assert trip["vs_line"] == series.close[d] / at - 1.0
            if strategy == "x2_s5_under_ladder" and trip["reason"] == "X2":
                assert trip["vs_line"] < 0


def test_the_rows_agree_with_their_base_until_the_first_tranche() -> None:
    for name in sl.SPANS:
        rows = TABLE["spans"][name]["span"]["strategies"]
        for strategy, base in sl.LADDER.items():
            trips = rows[strategy]["trips"]
            first = next((i for i, t in enumerate(trips) if t["fills"]), len(trips))
            _same_trips(tuple(trips[:first]), tuple(rows[base]["trips"][:first]))


def test_the_synthetic_table_exercises_every_case() -> None:
    hows: set[str] = set()
    for name in sl.SPANS:
        rows = TABLE["spans"][name]["span"]["strategies"]
        for strategy in LADDER:
            assert rows[strategy]["filled_sales"] >= 1, (name, strategy)
            hows.update(t["how"] for t in rows[strategy]["trips"])
            assert rows[strategy]["trips"] != rows[sl.LADDER[strategy]]["trips"]
            assert rows[strategy]["total"] != rows[sl.LADDER[strategy]]["total"]
    assert {"ladder", "recovery", "shift"} <= hows
    shares = {
        len(t["fills"])
        for name in sl.SPANS
        for strategy in LADDER
        for t in TABLE["spans"][name]["span"]["strategies"][strategy]["trips"]
    }
    assert {0, 1, 4} <= shares


def test_no_nan_reaches_the_table() -> None:
    json.dumps(TABLE, allow_nan=False)
    json.dumps(CHECK, allow_nan=False)


# Text -----------------------------------------------------------------------------------------


def test_the_markdown() -> None:
    text = sl.render_markdown(TABLE)
    lines = text.split("\n")
    assert lines[0] == "# 结构转变策略的分批买回（TRADING-2565-126）"
    assert sl.RULE_TEXT in lines and sl.NOTE in lines
    headings = [line for line in lines if line.startswith("## ")]
    assert headings == [f"## {sp.SPAN_TEXT[name]}" for name in sl.SPANS]
    assert text.count("### 策略") == 2
    for name in sl.SPANS:
        span = TABLE["spans"][name]["span"]
        assert f"窗口 `{sp.SPAN_WINDOW[name]}`；满仓起点 {span['first_day']}" in text
        for strategy in SHOWN:
            row = span["strategies"][strategy]
            cell = (
                f"{row['by_ladder']} / {row['by_recovery']} / {row['by_entry']} / "
                f"{row['by_shift']} / {row['not_back']}"
            )
            assert f"| {sl.STRATEGY_TEXT[strategy]} | {xr.pct(row['total'])} |" in text
            assert f"| {cb.share_text(row['exposure'])} | {row['sells']} | " in text
            assert cell in text
        for strategy in LADDER:
            for trip in span["strategies"][strategy]["trips"]:
                fills = "、".join(f"{f['level']:.1f}X {f['date']}" for f in trip["fills"])
                assert (
                    f"| {sl.STRATEGY_TEXT[strategy]} | {trip['decision']} | {trip['reason']} | "
                    f"{xr.pct(trip['vs_line'])} | {trip['sell']} | {fills or '—'} |"
                ) in text
        for strategy in sl.TRIPS_SHOWN:
            for trip in span["strategies"][strategy]["trips"]:
                assert (
                    f"| {sl.STRATEGY_TEXT[strategy]} | {trip['decision']} | {trip['reason']} | "
                    f"{xr.pct(trip['vs_line'])} | {trip['sell']} |"
                ) in text
    assert "四档买满" in text and "（剩下 " in text
    assert "x3_s5_under" not in text and "纳斯达克 100 1986" not in text
    trips = sum(
        len(TABLE["spans"][n]["span"]["strategies"][s]["trips"])
        for n in sl.SPANS
        for s in (*LADDER, *sl.TRIPS_SHOWN)
    )
    years = sum(len(TABLE["spans"][n]["span"]["years"]) for n in sl.SPANS)
    assert sum(line.startswith("| ") for line in lines) == 2 * (4 + len(SHOWN)) + years + trips
    assert text.endswith("\n") and not text.endswith("\n\n")


def test_the_cells_for_fills_and_full() -> None:
    assert sl.fills_text([]) == "—"
    fills = [
        {"level": 0.9, "date": "2011-08-04", "share": 0.1, "at": 0.88},
        {"level": 0.8, "date": "2011-08-08", "share": 0.2, "at": 0.79},
    ]
    assert sl.fills_text(fills) == "0.9X 2011-08-04、0.8X 2011-08-08"
    trip: dict[str, Any] = {"how": "shift", "fills": fills, "rest_share": 0.7}
    assert sl.full_text(trip) == "结构转变（剩下 70%）"
    assert sl.full_text({"how": "entry", "fills": [], "rest_share": 1.0}) == "入场信号"
    assert sl.full_text({"how": "none", "fills": fills, "rest_share": None}) == "没买满"
    assert sl.full_text({"how": "ladder", "fills": fills * 2, "rest_share": None}) == "四档买满"


def test_the_summary_and_check_lines(tmp_path: Path) -> None:
    line = sl.summary_line(TABLE, tmp_path)
    assert line.startswith("structure_shift_ladder: qqq 总收益 ")
    assert line.endswith(f"；结果目录 {tmp_path}")
    for name in ("qqq", "dev"):
        rows = TABLE["spans"][name]["span"]["strategies"]
        shown = "、".join(f"{s} {xr.pct(rows[s]['total'])}" for s in SHOWN)
        assert f"{name} 总收益 {shown}" in line
    assert line.index("qqq 总收益") < line.index("dev 总收益")
    q = CHECK["check"]["qqq"]
    assert sl.check_line(CHECK, tmp_path) == (
        f"structure_shift_ladder check: qqq ok，2011-01-03..{q['last_close_date']}，"
        f"{q['sessions']} 个交易日，缺 0 天；结果目录 {tmp_path}"
    )


# Command line --------------------------------------------------------------------------------


@pytest.fixture
def loaded_cli(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []

    def load_qqq(root: Path) -> Any:
        calls.append(f"qqq {root}")
        return QQQ, GATE, LOADED, QQQ_WINDOW

    monkeypatch.setattr(sp, "load_qqq", load_qqq)
    return calls


def test_check_writes_only_the_check_and_prints_one_line(
    loaded_cli: list[str], same_data: None, tmp_path: Path
) -> None:
    out = tmp_path / "out"
    result = CliRunner().invoke(sl.app, ["check", "--out", str(out), "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert loaded_cli == [f"qqq {tmp_path}"]
    assert sorted(p.name for p in out.iterdir()) == ["structure_shift_ladder_check.json"]
    text = out.joinpath("structure_shift_ladder_check.json").read_text(encoding="utf-8")
    assert text == json.dumps(CHECK, indent=2, ensure_ascii=False) + "\n"
    assert result.output == sl.check_line(CHECK, out) + "\n"


def test_a_failed_check_writes_the_check_and_exits_one(
    loaded_cli: list[str], tmp_path: Path
) -> None:
    result = CliRunner().invoke(sl.app, ["check", "--out", str(tmp_path)])
    assert result.exit_code == 1
    written = json.loads(
        tmp_path.joinpath("structure_shift_ladder_check.json").read_text(encoding="utf-8")
    )
    assert written["ok"] is False
    assert "qqq FAIL" in result.output


def test_check_refuses_an_existing_check_without_loading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def boom(root: Path) -> Any:
        raise AssertionError("loaded")

    monkeypatch.setattr(sp, "load_qqq", boom)
    tmp_path.joinpath("structure_shift_ladder_check.json").write_text("{}", encoding="utf-8")
    result = CliRunner().invoke(sl.app, ["check", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "structure_shift_ladder: 已有检查结果，拒绝覆盖。\n"


def test_run_writes_both_files_and_prints_one_line(
    loaded_cli: list[str], same_data: None, tmp_path: Path
) -> None:
    out = tmp_path / "out"
    result = CliRunner().invoke(sl.app, ["run", "--out", str(out), "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert loaded_cli == [f"qqq {tmp_path}"]
    text = out.joinpath("structure_shift_ladder.json").read_text(encoding="utf-8")
    assert text == json.dumps(TABLE, indent=2, ensure_ascii=False) + "\n"
    assert out.joinpath("structure_shift_ladder.md").read_text(encoding="utf-8") == (
        sl.render_markdown(TABLE)
    )
    assert sorted(p.name for p in out.iterdir()) == [
        "structure_shift_ladder.json",
        "structure_shift_ladder.md",
    ]
    assert result.output == sl.summary_line(TABLE, out) + "\n"


def test_run_after_check_in_the_same_directory(
    loaded_cli: list[str], same_data: None, tmp_path: Path
) -> None:
    runner = CliRunner()
    assert runner.invoke(sl.app, ["check", "--out", str(tmp_path)]).exit_code == 0
    assert runner.invoke(sl.app, ["run", "--out", str(tmp_path)]).exit_code == 0
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "structure_shift_ladder.json",
        "structure_shift_ladder.md",
        "structure_shift_ladder_check.json",
    ]


@pytest.mark.parametrize("name", ["structure_shift_ladder.json", "structure_shift_ladder.md"])
def test_run_refuses_an_existing_result_without_loading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str
) -> None:
    def boom(root: Path) -> Any:
        raise AssertionError("loaded")

    monkeypatch.setattr(sp, "load_qqq", boom)
    tmp_path.joinpath(name).write_text("{}", encoding="utf-8")
    result = CliRunner().invoke(sl.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "structure_shift_ladder: 已有结果，拒绝覆盖。\n"
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
    result = CliRunner().invoke(sl.app, [command, "--out", str(out)])
    assert result.exit_code == 1
    assert result.output == f"structure_shift_ladder: 数据载入失败：{str(error).splitlines()[0]}\n"
    assert not out.exists()


def test_a_failed_input_check_writes_only_the_json(loaded_cli: list[str], tmp_path: Path) -> None:
    result = CliRunner().invoke(sl.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "structure_shift_ladder: 输入检查 FAIL，没有印新行\n"
    assert [p.name for p in tmp_path.iterdir()] == ["structure_shift_ladder.json"]
    written = json.loads(tmp_path.joinpath("structure_shift_ladder.json").read_text("utf-8"))
    assert written["complete"] is False and "spans" not in written


def test_a_failed_reprint_writes_only_the_json(
    loaded_cli: list[str], same_data: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    for pin, value in _changed("qqq", "total").items():
        monkeypatch.setattr(rb, pin, value)
    result = CliRunner().invoke(sl.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "structure_shift_ladder: 重算核对 FAIL，没有印新行\n"
    assert [p.name for p in tmp_path.iterdir()] == ["structure_shift_ladder.json"]
    written = json.loads(tmp_path.joinpath("structure_shift_ladder.json").read_text("utf-8"))
    assert written["check"]["periods"]["qqq"]["ok"] is False and "spans" not in written
