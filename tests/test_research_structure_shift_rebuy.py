"""Acceptance tests for TRADING-2565-124: the structure-shift strategy's buy-back above the line.

Requirement: knowledge qqq-options/auto-loop/period7/entry-exit-combo/structure-shift-rebuy-
requirements-2026-11-04.md version 1.0 (owner decision structure_shift_rebuy_above_line).
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
from ai_trading_system.research_probes import entry_timing_ndx as en
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_ndx as xn
from ai_trading_system.research_probes import exit_rules_returns as xr
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import policy_table as pt
from ai_trading_system.research_probes import structure_shift_periods as sp
from ai_trading_system.research_probes import structure_shift_rebuy as rb
from ai_trading_system.yaml_loader import safe_load_yaml_path

LOADED = load_settings()
GATE = tr.load_gate(PROJECT_ROOT / "research" / "_trials" / "prereg.yaml")
_, PLAN = tr.load_trial(lg.research_root(LOADED), pt.DATA_TRIAL)
MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "structure_shift_rebuy.py"
REGISTRY = PROJECT_ROOT / "config" / "research" / "research_window_registry.yaml"
POLICY = PROJECT_ROOT / "config" / "research" / "primary_research_window_policy.yaml"
SOURCE = {"snapshot_id": "qlh-20261013-0b0df53cb1a2", "column": "ndx_price", "check_status": "WARN"}
NDX_WINDOW = en.registered_window(LOADED)
QQQ_WINDOW = sp.registered_qqq_window(LOADED)
BACK = ("x2_s5_under_back1", "x2_s5_under_back2")
SHOWN = ("x2_s5", "x2_s5_under", *BACK, "x2_s5_line", "x2_a", "hold")
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
    "ndx": {
        "x2_s5": 6.897791,
        "x3_s5": 2.534258,
        "x2x3_s5": 2.732365,
        "x2_s2": 8.340913,
        "x3_s2": 4.065815,
        "x2x3_s2": 3.609067,
        "x2_a": 1.197806,
        "x3_a": 0.705183,
        "x2x3_a": 0.812665,
        "hold": 13.360477,
        "x2_s5_under": 6.206591,
        "x3_s5_under": 4.565942,
        "x2x3_s5_under": 4.565942,
        "x2_s5_line": 7.250423,
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
    assert rb.RUN_ID == "TRADING-2565-124"
    assert rb.OWNER_DECISION == (
        "owner_decision:TRADING-2565:2026-11-04:structure_shift_rebuy_above_line"
    )
    assert rb.SPANS == sp.SPANS == ("dev", "ndx", "qqq")
    assert rb.BASE == "x2_s5_under"
    assert rb.BACK == {"x2_s5_under_back1": 1, "x2_s5_under_back2": 2}
    assert rb.SHOWN == SHOWN
    assert rb.TRIPS_SHOWN == ("x2_s5_under", *BACK)
    assert (rb.CHECK_JSON, rb.RESULT_JSON, rb.RESULT_MD) == (
        "structure_shift_rebuy_check.json",
        "structure_shift_rebuy.json",
        "structure_shift_rebuy.md",
    )
    for strategy in BACK:
        assert "只在 200 日线下算" in rb.STRATEGY_TEXT[strategy]
        assert "左右各 5 天" in rb.STRATEGY_TEXT[strategy]
    assert "卖出后收盘站上 200 日线就买回" in rb.STRATEGY_TEXT["x2_s5_under_back1"]
    assert "连续两个收盘站上 200 日线才买回" in rb.STRATEGY_TEXT["x2_s5_under_back2"]
    for strategy in sp.STRATEGIES:
        assert rb.STRATEGY_TEXT[strategy] == sp.STRATEGY_TEXT[strategy]
    assert rb.HOW_TEXT == {**sp.HOW_TEXT, "back": "收盘站上 200 日线"}


def test_the_reprint_pins_are_the_122_printed_tables() -> None:
    assert rb.PERIODS_TOTALS == PRINTED_122
    for name in rb.SPANS:
        assert list(rb.PERIODS_TOTALS[name]) == list(sp.STRATEGIES)
    assert {s: PRINTED_122["dev"][s] for s in es.STRATEGIES} == sp.DEV_TOTALS
    assert rb.PERIODS_FIRST_DAY == {"dev": "1999-12-23", "ndx": "1986-10-17", "qqq": "2011-01-03"}
    assert rb.PERIODS_SESSIONS == {"dev": 2771, "ndx": 3116, "qqq": 3966}


def test_the_rule_text_and_the_note() -> None:
    for words in (
        "只印不判",
        "不是交易建议",
        "三段都已看过 TRADING-2565-122 的结果",
        "设计上卖出以后没有回场路的缺口",
        "没有可调的数",
        "总收益都等于它印的才印新行",
    ):
        assert words in rb.RULE_TEXT, words
    for words in (
        "严格高于当天的 200 日线",
        "连续两个收盘",
        "从卖出的下一个收盘起",
        "X2 和 L 止损卖出以后都用",
        "谁先到用谁",
        "依次算 v0_close、结构转变、站上 200 日线",
        "不加 L 止损",
        "离 200 日线",
    ):
        assert words in rb.NOTE, words


def _imports(text: str) -> set[str]:
    imported: set[str] = set()
    for node in ast.walk(ast.parse(text)):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.update(f"{node.module}.{alias.name}" for alias in node.names)
    return imported


def _code(text: str) -> str:
    """The module without string literals and the pinned totals of TRADING-2565-122."""
    tree = ast.parse(text)
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            node.value = ""
        if isinstance(node, ast.AnnAssign | ast.Assign):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(t, ast.Name) and t.id.startswith("PERIODS_") for t in targets):
                node.value = ast.Constant(None)
    return ast.unparse(tree)


def test_the_module_reuses_the_122_module_and_reads_only_qqq_and_its_index() -> None:
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
        "ai_trading_system.research_probes.entry_timing_ndx",
        "ai_trading_system.research_probes.exit_rules_counts",
        "ai_trading_system.research_probes.exit_rules_returns",
        "ai_trading_system.research_probes.structure_shift_periods",
    )
    for name in _imports(text):
        assert name.startswith(allowed), name
    for word in (
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
        "def qqq_inputs",
        "def ndx_inputs",
        "def load_qqq",
        "def registered_qqq_window",
        "def dev_check",
        "def v0_entries",
        "def shifts",
        "def equity",
        "def metrics",
        "def year_returns",
        "def moving_average",
    ):
        assert word not in text, word
    for number in ("0.92", "0.08", "0.0005", "200", "50"):
        assert re.search(rf"(?<![\d.]){re.escape(number)}(?![\d])", _code(text)) is None, number
    assert text.count(".write_text(") == 3
    for call in (
        "sp.span_result(",
        "sp.exit_under(",
        "sp.qqq_inputs(",
        "sp.ndx_inputs(",
        "sp.load_qqq(",
        "sp.dev_check(",
        "sp.rule_block(",
        "sp.v0_entries(",
        "es.shifts(",
        "ec.moving_average(",
        "xc.prepare(",
        "cb.equity(",
        "cb.metrics(",
        "cb.year_returns(",
        "en.load_data(",
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


def test_the_registry_notes_name_this_reader() -> None:
    ndx = _note("# Baker's print-only read of the 200-day pullback rule", "  ndx_price_1986_1999:")
    for words in (
        "fifth read",
        "TRADING-2565-122",
        "sixth read",
        "TRADING-2565-124",
        "owner_decision:TRADING-2565:2026-11-04:structure_shift_rebuy_above_line",
        "research_probes/structure_shift_rebuy.py",
        "200-day line",
    ):
        assert words in ndx, words
    assert ndx.index("fifth read") < ndx.index("sixth read")
    qqq = _note(
        "# Baker's one-time print-only read of QQQ after 2010", "  qqq_after_2010_owner_print:"
    )
    for words in (
        "TRADING-2565-122",
        "owner_decision:TRADING-2565:2026-11-03:structure_shift_other_periods",
        "second read",
        "TRADING-2565-124",
        "owner_decision:TRADING-2565:2026-11-04:structure_shift_rebuy_above_line",
        "research_probes/structure_shift_rebuy.py",
        "already seen history",
        "forward ledger",
    ):
        assert words in qqq, words
    assert qqq.index("structure_shift_other_periods") < qqq.index("second read")


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
    assert ndx["caveats"] == [
        "nasdaq_100_price_index_without_dividends",
        "qqq_etf_trades_from_1999_03_10",
        "fred_carries_the_price_index_over_some_closed_sessions",
        "the_200_day_average_warms_up_inside_the_window",
    ]
    assert entry["blocked_usage"] == ndx["blocked_usage"]
    validation = windows["qqq_long_history_validation"]
    assert (validation["start"], validation["end"]) == (date(2011, 1, 3), date(2021, 2, 19))
    development = windows["qqq_long_history_development"]
    assert (development["start"], development["end"]) == (date(1999, 3, 10), date(2010, 12, 31))
    policy = safe_load_yaml_path(POLICY)
    for roles in policy["question_windows"].values():
        assert "qqq_after_2010_owner_print" not in roles.values()
        assert "ndx_price_1986_1999" not in roles.values()


# Buy-backs above the line by hand -------------------------------------------------------------


def _prep(close: list[float]) -> xc.Prepared:
    n = len(close)
    dates = tuple(date(2001, 1, 1) + timedelta(days=i) for i in range(n))
    return xc.Prepared(dates, tuple(close), (None,) * n, (None,) * n, (0,) * n, (False,) * n)


def test_back_day_by_hand() -> None:
    close = [10.0, 9.0, 11.0, 9.0, 11.0, 12.0, 13.0]
    line = [10.0] * 7
    assert rb.back_day(close, line, 0, 1) == 2
    assert rb.back_day(close, line, 2, 1) == 4
    assert rb.back_day(close, line, 0, 2) == 5
    assert rb.back_day(close, line, 4, 2) == 6
    assert rb.back_day(close, line, 5, 2) is None
    assert rb.back_day(close, line, 6, 1) is None


def test_back_day_needs_a_close_strictly_above_and_a_known_line() -> None:
    close = [10.0, 10.0, 11.0, 11.0, 11.0]
    assert rb.back_day(close, [10.0, 10.0, 11.0, 11.0, 10.0], 0, 1) == 4
    assert rb.back_day(close, [None, None, None, 10.0, 10.0], 0, 2) == 4
    assert rb.back_day(close, [10.0, 10.0, None, 10.0, 10.0], 0, 2) == 4
    assert rb.back_day(close, [None] * 5, 0, 1) is None


def test_back_day_starts_after_the_sale_close() -> None:
    close = [12.0, 12.0, 9.0, 12.0]
    line = [10.0] * 4
    assert rb.back_day(close, line, 1, 1) == 3
    assert rb.back_day(close, line, 0, 2) is None
    assert rb.back_day(close, line, 0, 1) == 1


def _line_path() -> tuple[xc.Prepared, list[float | None]]:
    # Bought at 0; X2 fires at 3 (8% off 100, below the line), sale at 4; the close at 6 is the
    # first one above the line again, the close at 7 the second one.
    close = [100.0, 98.0, 95.0, 91.0, 90.0, 93.0, 97.0, 99.0, 101.0, 102.0]
    line: list[float | None] = [92.0, 92.0, 92.0, 92.0, 94.0, 94.0, 95.0, 95.0, 96.0, 96.0]
    return _prep(close), line


def test_the_buy_back_above_the_line_by_hand() -> None:
    data, line = _line_path()
    one = rb.walk_back(data, [], [], 0, line, 1)
    assert one.buys == (0, 6) and one.sells == (4,)
    assert one.trips == (
        {
            "decision": data.dates[3].isoformat(),
            "reason": "X2",
            "sell": data.dates[4].isoformat(),
            "buy": data.dates[6].isoformat(),
            "how": "back",
            "days_out": 2,
            "change": 97.0 / 90.0 - 1.0,
        },
    )
    assert one.held == (False, True, True, True, True, False, False, True, True, True)
    two = rb.walk_back(data, [], [], 0, line, 2)
    assert two.buys == (0, 7) and two.trips[0]["how"] == "back"
    assert two.trips[0]["change"] == 99.0 / 90.0 - 1.0


def test_an_x2_above_the_line_does_not_sell() -> None:
    data, _ = _line_path()
    route = rb.walk_back(data, [], [], 0, [80.0] * 10, 1)
    assert route.sells == () and route.trips == ()
    assert route.held == (False, *([True] * 9))


def test_the_entry_and_the_shift_win_ties_with_the_line() -> None:
    data, line = _line_path()
    route = rb.walk_back(data, [6], [], 0, line, 1)
    assert route.trips[0]["how"] == "entry" and route.buys == (0, 6)
    shift = es.Shift(cross=2, low=4, high=1, entry=6)
    route = rb.walk_back(data, [], [shift], 0, line, 1)
    assert route.trips[0]["how"] == "shift" and route.buys == (0, 6)
    route = rb.walk_back(data, [7], [es.Shift(cross=2, low=4, high=1, entry=7)], 0, line, 1)
    assert route.trips[0]["how"] == "back" and route.buys == (0, 6)
    route = rb.walk_back(data, [5], [], 0, line, 1)
    assert route.trips[0]["how"] == "entry" and route.buys == (0, 5)


def test_no_low_stop_after_a_buy_back_above_the_line() -> None:
    close = [100.0, 98.0, 95.0, 91.0, 90.0, 93.0, 97.0, 96.0, 88.0, 87.0, 86.0]
    line: list[float | None] = [92.0, 92.0, 92.0, 92.0, 94.0, 94.0, 95.0, 95.0, 85.0, 85.0, 85.0]
    data = _prep(close)
    route = rb.walk_back(data, [], [es.Shift(cross=2, low=4, high=1, entry=9)], 0, line, 1)
    assert route.buys == (0, 6) and route.sells == (4,)
    assert route.held[-1] is True


def test_the_line_buy_back_also_follows_a_low_stop() -> None:
    # Shift buy-back at 6 with L = 90 (the close at 4); the close at 8 falls under L but not 8%
    # under 97, the sale is at 9 and the close at 10 is back above the line.
    close = [100.0, 98.0, 95.0, 91.0, 90.0, 93.0, 97.0, 95.0, 89.5, 88.0, 99.0, 100.0]
    line: list[float | None] = [
        92.0,
        92.0,
        92.0,
        92.0,
        99.0,
        99.0,
        99.0,
        99.0,
        99.0,
        95.0,
        95.0,
        95.0,
    ]
    data = _prep(close)
    route = rb.walk_back(data, [], [es.Shift(cross=2, low=4, high=1, entry=6)], 0, line, 1)
    assert [t["reason"] for t in route.trips] == ["X2", "L"]
    assert [t["how"] for t in route.trips] == ["shift", "back"]
    assert route.buys == (0, 6, 10) and route.sells == (4, 9)


def _path(seed: int, n: int = 700) -> list[float]:
    rng = np.random.default_rng(seed)
    return [float(v) for v in 100.0 * np.cumprod(1.0 + rng.normal(0.0003, 0.02, n))]


@pytest.mark.parametrize("seed", range(4))
def test_without_a_close_above_the_line_the_path_is_the_122_path(seed: int) -> None:
    close = _path(seed)
    data = xc.prepare(tuple(date(2001, 1, 1) + timedelta(days=i) for i in range(700)), close)
    real = ec.moving_average(close)
    entries = sp.v0_entries(close, 0)
    moves = es.shifts(close, real, 5, 200)
    high = [None if v is None else max(close) + 1.0 for v in real]

    def fire(rule: str, prepared: xc.Prepared, b: int) -> tuple[int, str] | None:
        return sp.exit_under(rule, prepared, high, b)

    for confirm in (1, 2):
        mine = rb.walk_back(data, entries, moves, 200, high, confirm)
        assert mine == sp.walk("x2", data, entries, moves, 200, fire)


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
    TABLE_122 = sp.run_table(*ARGS)
    assert TABLE_122["complete"] is True
    PERIOD_PINS = {
        "PERIODS_TOTALS": {
            name: {s: round(v["total"], 6) for s, v in block["span"]["strategies"].items()}
            for name, block in TABLE_122["spans"].items()
        },
        "PERIODS_FIRST_DAY": {n: b["span"]["first_day"] for n, b in TABLE_122["spans"].items()},
        "PERIODS_SESSIONS": {n: b["span"]["sessions"] for n, b in TABLE_122["spans"].items()},
    }
    for _name, _value in PERIOD_PINS.items():
        _mp.setattr(rb, _name, _value)
    TABLE = rb.run_table(*ARGS)
    CHECK = rb.check_table(*ARGS)
    Q_IN = sp.qqq_inputs(QQQ, LOADED, GATE, QQQ_WINDOW)


@pytest.fixture
def same_data(monkeypatch: pytest.MonkeyPatch) -> None:
    _same(monkeypatch)
    for name, value in DEV_PINS.items():
        monkeypatch.setattr(sp, name, value)
    for name, value in PERIOD_PINS.items():
        monkeypatch.setattr(rb, name, value)


def test_the_synthetic_data_are_not_the_real_ones() -> None:
    assert DEV_FOUND != cb.V0_ENTRIES and len(DEV_FOUND) >= 2
    assert N_FOUND != sp.NDX_V0 and len(N_FOUND) >= 2
    assert FULL.dates[-1] < sp.QQQ_LAST_MIN
    assert PERIOD_PINS["PERIODS_TOTALS"] != PRINTED_122
    assert PERIOD_PINS["PERIODS_SESSIONS"] != rb.PERIODS_SESSIONS
    assert TABLE["complete"] is True


# Checks --------------------------------------------------------------------------------------


def test_the_real_constants_stop_the_table() -> None:
    result = rb.run_table(*ARGS)
    assert result["complete"] is False
    assert result["check"]["qqq"]["ok"] is False and result["check"]["ndx"]["ok"] is False
    assert result["check"]["dev"] is None and result["check"]["periods"] is None
    assert list(result) == ["run_id", "owner_decision", "source", "rule", "check", "complete"]
    assert rb.check_table(*ARGS)["ok"] is False


def test_the_input_checks_are_the_122_checks(same_data: None) -> None:
    q = sp.qqq_inputs(QQQ, LOADED, GATE, QQQ_WINDOW)
    n = sp.ndx_inputs(LEVELS, NDX_WINDOW)
    assert CHECK["check"] == {"qqq": q.check, "ndx": n.check}
    assert TABLE["check"]["qqq"] == q.check and TABLE["check"]["ndx"] == n.check
    assert CHECK["ok"] is True


def test_a_failed_qqq_check_stops_the_table(
    same_data: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sp, "QQQ_SESSIONS", (1, 2))
    result = rb.run_table(*ARGS)
    assert result["complete"] is False and result["check"]["qqq"]["ok"] is False
    assert result["check"]["ndx"]["ok"] is True
    assert result["check"]["dev"] is None and result["check"]["periods"] is None
    assert rb.check_table(*ARGS)["ok"] is False


def test_a_failed_ndx_check_stops_the_table(
    same_data: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sp, "NDX_V0", N_FOUND[1:])
    result = rb.run_table(*ARGS)
    assert result["complete"] is False and result["check"]["ndx"]["ok"] is False
    assert result["check"]["dev"] is None and result["check"]["periods"] is None


def test_a_changed_120_table_stops_the_new_rows(
    same_data: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sp, "DEV_FIRST_DAY", "1999-12-24")
    result = rb.run_table(*ARGS)
    assert result["complete"] is False and "spans" not in result
    assert result["check"]["dev"]["ok"] is False
    assert all(c["ok"] for c in result["check"]["periods"].values())


def _changed(name: str, key: str) -> dict[str, Any]:
    pins = json.loads(json.dumps(PERIOD_PINS))
    if key == "total":
        pins["PERIODS_TOTALS"][name]["x2_s5_line"] += 0.000001
    elif key == "first_day":
        pins["PERIODS_FIRST_DAY"][name] = "1990-01-02"
    else:
        pins["PERIODS_SESSIONS"][name] += 1
    assert isinstance(pins, dict)
    return pins


@pytest.mark.parametrize("name", ["dev", "ndx", "qqq"])
@pytest.mark.parametrize("key", ["total", "first_day", "sessions"])
def test_each_reprint_check_alone_stops_the_new_rows(
    same_data: None, monkeypatch: pytest.MonkeyPatch, name: str, key: str
) -> None:
    for pin, value in _changed(name, key).items():
        monkeypatch.setattr(rb, pin, value)
    result = rb.run_table(*ARGS)
    assert result["complete"] is False and "spans" not in result
    assert result["check"]["dev"]["ok"] is True
    periods = result["check"]["periods"]
    assert [n for n, c in periods.items() if not c["ok"]] == [name]


def test_a_missing_total_stops_the_new_rows(
    same_data: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    pins = json.loads(json.dumps(PERIOD_PINS["PERIODS_TOTALS"]))
    del pins["qqq"]["x2_s5_line"]
    monkeypatch.setattr(rb, "PERIODS_TOTALS", pins)
    assert rb.run_table(*ARGS)["complete"] is False


def test_the_periods_check_block(same_data: None) -> None:
    for name in rb.SPANS:
        block = TABLE["check"]["periods"][name]
        assert block == {
            "first_day": PERIOD_PINS["PERIODS_FIRST_DAY"][name],
            "sessions": PERIOD_PINS["PERIODS_SESSIONS"][name],
            "totals": PERIOD_PINS["PERIODS_TOTALS"][name],
            "ok": True,
        }
    assert TABLE["check"]["dev"] == sp.dev_check(TABLE_122["spans"]["dev"])


def test_the_check_table_has_no_signals_or_returns() -> None:
    assert list(CHECK) == ["run_id", "owner_decision", "source", "rule", "check", "ok"]
    assert CHECK["check"] == {"qqq": TABLE["check"]["qqq"], "ndx": TABLE["check"]["ndx"]}
    text = json.dumps(CHECK["check"])
    for word in ("signals", "spans", "total", "trips", "plain_mean", "periods"):
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
    assert TABLE["run_id"] == "TRADING-2565-124"
    assert TABLE["owner_decision"] == rb.OWNER_DECISION
    assert TABLE["source"] == {"qqq": {"snapshot_id": PLAN.data.snapshot_id}, "ndx": SOURCE}
    rule = dict(TABLE["rule"])
    back = rule.pop("back")
    assert rule == sp.rule_block()
    assert back == {
        "strategies": list(BACK),
        "base": "x2_s5_under",
        "confirm_closes": {"x2_s5_under_back1": 1, "x2_s5_under_back2": 2},
        "buy_back": (
            "close_ending_the_first_run_of_confirm_closes_strictly_above_sma200_after_the_sale_close"
        ),
        "after": ["X2", "L"],
        "order": ["entry", "shift", "back"],
        "low_stop": "none_after_back_buy_back",
    }
    assert CHECK["rule"] == TABLE["rule"]
    assert list(TABLE["check"]) == ["qqq", "ndx", "dev", "periods"]


# The spans ------------------------------------------------------------------------------------


def _strip(row: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in row.items() if k != "by_back"}


@pytest.mark.parametrize("name", ["dev", "ndx", "qqq"])
def test_each_span_reprints_the_122_span(name: str) -> None:
    mine, theirs = TABLE["spans"][name], TABLE_122["spans"][name]
    assert list(mine) == list(theirs)
    for key in ("lookback_from", "scan_from", "signals"):
        assert mine[key] == theirs[key]
    span = mine["span"]
    assert list(span["strategies"]) == [*sp.STRATEGIES, *BACK]
    for strategy in sp.STRATEGIES:
        assert _strip(span["strategies"][strategy]) == theirs["span"]["strategies"][strategy]
        assert span["strategies"][strategy]["by_back"] == 0
    for key in ("first_day", "last_day", "sessions", "entries"):
        assert span[key] == theirs["span"][key]
    for mine_year, their_year in zip(span["years"], theirs["span"]["years"], strict=True):
        assert {k: v for k, v in mine_year.items() if k not in BACK} == their_year
        assert list(mine_year)[-2:] == list(BACK)


def _own(series: ec.Closes, first_in: int, confirm: int) -> dict[str, Any]:
    """Recompute one buy-back row with an independent day-by-day state machine."""
    dates, close = series.dates, series.close
    n = len(close)
    s0 = max(first_in, 200)
    line = ec.moving_average(close)
    entries = set(sp.v0_entries(close, first_in))
    moves = es.shifts(close, line, 5, s0)
    shift_low = {m.entry: m.low for m in moves}
    held = [False] * n
    buys, sells = [s0], []
    trips: list[dict[str, Any]] = []
    holding, high = True, close[s0]
    floor: float | None = None
    pending: tuple[int, str] | None = None
    sale = run = 0
    for p in range(s0 + 1, n):
        if holding:
            held[p] = True
            if pending is not None:
                holding, sale, run = False, p, 0
                sells.append(p)
                trips.append(
                    {
                        "decision": dates[pending[0]].isoformat(),
                        "reason": pending[1],
                        "sell": dates[p].isoformat(),
                    }
                )
                pending = None
                continue
            high = max(high, close[p])
            at = line[p]
            if at is not None and close[p] < at and close[p] / high <= 1.0 - cb.TRAIL:
                pending = (p, "X2")
            elif floor is not None and close[p] < floor:
                pending = (p, "L")
            if pending is not None and p == n - 1:
                pending = None
            continue
        at = line[p]
        run = run + 1 if at is not None and close[p] > at else 0
        how = None
        if p in entries:
            how, floor = "entry", None
        elif p in shift_low and p > sale:
            how, floor = "shift", close[shift_low[p]]
        elif run >= confirm:
            how, floor = "back", None
        if how is not None:
            holding, high = True, close[p]
            buys.append(p)
            trips[-1].update(
                {
                    "buy": dates[p].isoformat(),
                    "how": how,
                    "days_out": p - sale,
                    "change": close[p] / close[sale] - 1.0,
                }
            )
    if not holding:
        trips[-1].update(
            {
                "buy": None,
                "how": "none",
                "days_out": n - 1 - sale,
                "change": close[n - 1] / close[sale] - 1.0,
            }
        )
    return {"held": tuple(held), "buys": tuple(buys), "sells": tuple(sells), "trips": trips}


def _series(name: str) -> tuple[ec.Closes, int]:
    if name == "dev":
        return Q_IN.dev, Q_IN.dev_first_in
    if name == "ndx":
        return N_SERIES, 0
    return Q_IN.full, Q_IN.first_in


@pytest.mark.parametrize("name", ["dev", "ndx", "qqq"])
@pytest.mark.parametrize("strategy", BACK)
def test_each_buy_back_row_recomputed(name: str, strategy: str) -> None:
    series, first_in = _series(name)
    own = _own(series, first_in, rb.BACK[strategy])
    row = TABLE["spans"][name]["span"]["strategies"][strategy]
    trips = [{k: v for k, v in t.items() if k != "vs_line"} for t in row["trips"]]
    assert len(trips) == len(own["trips"])
    for mine, theirs in zip(trips, own["trips"], strict=True):
        assert mine.keys() == theirs.keys()
        for key in mine:
            if isinstance(mine[key], float):
                assert math.isclose(mine[key], theirs[key], rel_tol=1e-12)
            else:
                assert mine[key] == theirs[key], key
    s0 = max(first_in, 200)
    route = cb.Route(own["held"], own["buys"], own["sells"], tuple(own["trips"]))
    values = cb.equity(series.close, route, s0)
    assert math.isclose(row["total"], values[-1] - 1.0, rel_tol=1e-12)
    expected = cb.metrics(series.dates, values, route, s0)
    for key, value in expected.items():
        assert row[key] == value or math.isclose(row[key], value, rel_tol=1e-12), key
    assert row["sells"] == len(own["sells"])
    assert row["by_back"] == sum(t["how"] == "back" for t in own["trips"])
    assert row["by_shift"] == sum(t["how"] == "shift" for t in own["trips"])
    assert row["low_stops"] == sum(t["reason"] == "L" for t in own["trips"])
    assert row["by_line"] == 0
    assert row["by_entry"] + row["by_shift"] + row["by_back"] + row["not_back"] == row["sells"]
    annual = cb.year_returns(series.dates, values, s0)
    for year in TABLE["spans"][name]["span"]["years"]:
        assert math.isclose(year[strategy], annual[year["year"]], rel_tol=1e-12, abs_tol=1e-15)


@pytest.mark.parametrize("name", ["dev", "ndx", "qqq"])
def test_every_back_trip_carries_its_distance_from_the_line(name: str) -> None:
    series, _ = _series(name)
    line = ec.moving_average(series.close)
    positions = {d.isoformat(): p for p, d in enumerate(series.dates)}
    for strategy in BACK:
        for trip in TABLE["spans"][name]["span"]["strategies"][strategy]["trips"]:
            d = positions[trip["decision"]]
            at = line[d]
            assert at is not None
            assert trip["vs_line"] == series.close[d] / at - 1.0
            if trip["reason"] == "X2":
                assert trip["vs_line"] < 0
            if trip["how"] == "back":
                p = positions[trip["buy"]]
                for q in range(p - rb.BACK[strategy] + 1, p + 1):
                    now = line[q]
                    assert now is not None and series.close[q] > now
                    assert q > positions[trip["sell"]]


def test_the_rows_agree_until_the_first_back_buy_back() -> None:
    for name in rb.SPANS:
        rows = TABLE["spans"][name]["span"]["strategies"]
        base = rows["x2_s5_under"]["trips"]
        for strategy in BACK:
            trips = rows[strategy]["trips"]
            first = next((i for i, t in enumerate(trips) if t["how"] == "back"), len(trips))
            assert trips[:first] == base[:first]


def test_the_synthetic_table_exercises_every_case() -> None:
    hows: set[str] = set()
    reasons: set[str] = set()
    differ = False
    for name in rb.SPANS:
        rows = TABLE["spans"][name]["span"]["strategies"]
        for strategy in BACK:
            assert rows[strategy]["by_back"] >= 1, (name, strategy)
            hows.update(t["how"] for t in rows[strategy]["trips"])
            reasons.update(t["reason"] for t in rows[strategy]["trips"])
            assert rows[strategy]["trips"] != rows["x2_s5_under"]["trips"]
        differ = differ or rows[BACK[0]]["trips"] != rows[BACK[1]]["trips"]
    assert {"back", "shift", "entry"} <= hows
    assert "X2" in reasons
    assert differ


def test_no_nan_reaches_the_table() -> None:
    json.dumps(TABLE, allow_nan=False)
    json.dumps(CHECK, allow_nan=False)


# Text -----------------------------------------------------------------------------------------


def test_the_markdown() -> None:
    text = rb.render_markdown(TABLE)
    lines = text.split("\n")
    assert lines[0] == "# 结构转变策略补「站上 200 日线就买回」（TRADING-2565-124）"
    assert rb.RULE_TEXT in lines and rb.NOTE in lines
    headings = [line for line in lines if line.startswith("## ")]
    assert headings == [f"## {sp.SPAN_TEXT[name]}" for name in rb.SPANS]
    assert text.count("### 策略") == 3
    for name in rb.SPANS:
        span = TABLE["spans"][name]["span"]
        assert f"窗口 `{sp.SPAN_WINDOW[name]}`；满仓起点 {span['first_day']}" in text
        for strategy in SHOWN:
            row = span["strategies"][strategy]
            cell = (
                f"{row['by_entry']} / {row['by_shift']} / {row['by_line']} / "
                f"{row['by_back']} / {row['not_back']}"
            )
            assert f"| {rb.STRATEGY_TEXT[strategy]} | {xr.pct(row['total'])} |" in text
            assert cell in text
        for strategy in rb.TRIPS_SHOWN:
            for trip in span["strategies"][strategy]["trips"]:
                assert (
                    f"| {rb.STRATEGY_TEXT[strategy]} | {trip['decision']} | {trip['reason']} | "
                    f"{xr.pct(trip['vs_line'])} | {trip['sell']} |"
                ) in text
    assert "收盘站上 200 日线" in text
    assert "x3_s5_under" not in text
    trips = sum(
        len(TABLE["spans"][n]["span"]["strategies"][s]["trips"])
        for n in rb.SPANS
        for s in rb.TRIPS_SHOWN
    )
    years = sum(len(TABLE["spans"][n]["span"]["years"]) for n in rb.SPANS)
    assert sum(line.startswith("| ") for line in lines) == 3 * (3 + len(SHOWN)) + years + trips
    assert text.endswith("\n") and not text.endswith("\n\n")


def test_the_summary_and_check_lines(tmp_path: Path) -> None:
    line = rb.summary_line(TABLE, tmp_path)
    assert line.startswith("structure_shift_rebuy: ndx 总收益 ")
    assert line.endswith(f"；结果目录 {tmp_path}")
    for name in ("ndx", "qqq", "dev"):
        rows = TABLE["spans"][name]["span"]["strategies"]
        shown = "、".join(f"{s} {xr.pct(rows[s]['total'])}" for s in (*rb.TRIPS_SHOWN, "hold"))
        assert f"{name} 总收益 {shown}" in line
    q, n = CHECK["check"]["qqq"], CHECK["check"]["ndx"]
    assert rb.check_line(CHECK, tmp_path) == (
        f"structure_shift_rebuy check: ndx ok，{n['inputs']['closes']} 个收盘；"
        f"qqq ok，2011-01-03..{q['last_close_date']}，{q['sessions']} 个交易日，缺 0 天；"
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
    result = CliRunner().invoke(rb.app, ["check", "--out", str(out), "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert loaded_cli == [f"qqq {tmp_path}", f"ndx {tmp_path}"]
    assert sorted(p.name for p in out.iterdir()) == ["structure_shift_rebuy_check.json"]
    text = out.joinpath("structure_shift_rebuy_check.json").read_text(encoding="utf-8")
    assert text == json.dumps(CHECK, indent=2, ensure_ascii=False) + "\n"
    assert result.output == rb.check_line(CHECK, out) + "\n"


def test_a_failed_check_writes_the_check_and_exits_one(
    loaded_cli: list[str], tmp_path: Path
) -> None:
    result = CliRunner().invoke(rb.app, ["check", "--out", str(tmp_path)])
    assert result.exit_code == 1
    written = json.loads(
        tmp_path.joinpath("structure_shift_rebuy_check.json").read_text(encoding="utf-8")
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
    tmp_path.joinpath("structure_shift_rebuy_check.json").write_text("{}", encoding="utf-8")
    result = CliRunner().invoke(rb.app, ["check", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "structure_shift_rebuy: 已有检查结果，拒绝覆盖。\n"


def test_run_writes_both_files_and_prints_one_line(
    loaded_cli: list[str], same_data: None, tmp_path: Path
) -> None:
    out = tmp_path / "out"
    result = CliRunner().invoke(rb.app, ["run", "--out", str(out), "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert loaded_cli == [f"qqq {tmp_path}", f"ndx {tmp_path}"]
    text = out.joinpath("structure_shift_rebuy.json").read_text(encoding="utf-8")
    assert text == json.dumps(TABLE, indent=2, ensure_ascii=False) + "\n"
    assert out.joinpath("structure_shift_rebuy.md").read_text(encoding="utf-8") == (
        rb.render_markdown(TABLE)
    )
    assert sorted(p.name for p in out.iterdir()) == [
        "structure_shift_rebuy.json",
        "structure_shift_rebuy.md",
    ]
    assert result.output == rb.summary_line(TABLE, out) + "\n"


def test_run_after_check_in_the_same_directory(
    loaded_cli: list[str], same_data: None, tmp_path: Path
) -> None:
    runner = CliRunner()
    assert runner.invoke(rb.app, ["check", "--out", str(tmp_path)]).exit_code == 0
    assert runner.invoke(rb.app, ["run", "--out", str(tmp_path)]).exit_code == 0
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "structure_shift_rebuy.json",
        "structure_shift_rebuy.md",
        "structure_shift_rebuy_check.json",
    ]


@pytest.mark.parametrize("name", ["structure_shift_rebuy.json", "structure_shift_rebuy.md"])
def test_run_refuses_an_existing_result_without_loading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str
) -> None:
    def boom(root: Path) -> Any:
        raise AssertionError("loaded")

    monkeypatch.setattr(sp, "load_qqq", boom)
    monkeypatch.setattr(en, "load_data", boom)
    tmp_path.joinpath(name).write_text("{}", encoding="utf-8")
    result = CliRunner().invoke(rb.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "structure_shift_rebuy: 已有结果，拒绝覆盖。\n"
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
    result = CliRunner().invoke(rb.app, [command, "--out", str(out)])
    assert result.exit_code == 1
    assert result.output == f"structure_shift_rebuy: 数据载入失败：{str(error).splitlines()[0]}\n"
    assert not out.exists()


def test_a_failed_input_check_writes_only_the_json(loaded_cli: list[str], tmp_path: Path) -> None:
    result = CliRunner().invoke(rb.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "structure_shift_rebuy: 输入检查 FAIL，没有印新行\n"
    assert [p.name for p in tmp_path.iterdir()] == ["structure_shift_rebuy.json"]
    written = json.loads(tmp_path.joinpath("structure_shift_rebuy.json").read_text("utf-8"))
    assert written["complete"] is False and "spans" not in written


def test_a_failed_reprint_writes_only_the_json(
    loaded_cli: list[str], same_data: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    for pin, value in _changed("qqq", "total").items():
        monkeypatch.setattr(rb, pin, value)
    result = CliRunner().invoke(rb.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "structure_shift_rebuy: TRADING-2565-122 重算核对 FAIL，没有印新行\n"
    assert [p.name for p in tmp_path.iterdir()] == ["structure_shift_rebuy.json"]
    written = json.loads(tmp_path.joinpath("structure_shift_rebuy.json").read_text("utf-8"))
    assert written["check"]["periods"]["qqq"]["ok"] is False and "spans" not in written
