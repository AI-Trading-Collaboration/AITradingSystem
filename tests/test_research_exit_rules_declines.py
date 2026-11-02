"""Acceptance tests for TRADING-2565-117: the QQQ exit family read decline by decline.

Requirement: knowledge qqq-options/auto-loop/period7/exit-family/exit-family-big-declines-
requirements-2026-11-02.md version 1.1 (owner decision exit_family_big_declines). Synthetic data
only; no snapshot, no network.
"""

from __future__ import annotations

import ast
import json
import math
from datetime import date
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
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import entry_timing_ndx as en
from ai_trading_system.research_probes import exit_rules_after as xa
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_declines as xd
from ai_trading_system.research_probes import exit_rules_ndx as xn
from ai_trading_system.research_probes import exit_rules_returns as xr
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import policy_table as pt
from ai_trading_system.yaml_loader import safe_load_yaml_path

LOADED = load_settings()
GATE = tr.load_gate(PROJECT_ROOT / "research" / "_trials" / "prereg.yaml")
_, PLAN = tr.load_trial(lg.research_root(LOADED), pt.DATA_TRIAL)
MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "exit_rules_declines.py"
REGISTRY = PROJECT_ROOT / "config" / "research" / "research_window_registry.yaml"
SOURCE = {"snapshot_id": "qlh-20261013-0b0df53cb1a2", "column": "ndx_price", "check_status": "WARN"}
WINDOW = en.registered_window(LOADED)
FOUR = ("X2", "X3", "X4", "X5")


# Constants and imports ------------------------------------------------------------------------


def test_the_run_is_the_decline_table_of_the_exit_family() -> None:
    assert xd.RUN_ID == "TRADING-2565-117"
    assert xd.OWNER_DECISION == "owner_decision:TRADING-2565:2026-11-02:exit_family_big_declines"
    assert xd.RULES == FOUR
    assert xd.NOT_APPLICABLE == ("X0", "X1", "X6")
    assert set(xd.RULES) | set(xd.NOT_APPLICABLE) == set(xc.RULES)
    assert xd.SPANS == ("qqq", "ndx")
    assert (xd.SWING_LINE, xd.PRE_PEAK, xd.EARLY, xd.YEAR_DAYS) == (0.15, 20, 1.0 / 3.0, 365.25)
    assert xd.TRAIL == xc.STOP == 0.08
    assert (xd.RESULT_JSON, xd.RESULT_MD) == ("exit_rules_declines.json", "exit_rules_declines.md")
    assert xd.SPAN_TEXT == xa.SPAN_TEXT
    assert xd.RULE_TEXT == {
        "X2": "移动止损（回落 8%）",
        "X3": "跌破 50 日线",
        "X4": "周急跌破位",
        "X5": "第二腿",
    }


def test_the_note_says_what_is_measured_and_what_is_not() -> None:
    for words in (
        "只印不判",
        "看过结果的历史",
        "事后按收盘 15% 的之字形",
        "不进任何规则",
        "一直拿过高点的人",
        "回落 8%",
        "波动大时买进的人宽度会更宽",
        "只用当时已知的收盘",
        "从第二天重新开始",
        "X0、X1、X6 按买入价算",
        "下一个收盘卖",
        "没响算 0",
        "三分之一（按对数）",
        "按簇取第一天",
        "截到上一个低点之后",
        "算虚惊",
        "不算错过的反弹",
    ):
        assert words in xd.NOTE, words


def test_the_module_reads_only_qqq_and_its_index_through_the_family() -> None:
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
        "bisect.",
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
        "ai_trading_system.research_probes.entry_timing_ndx",
        "ai_trading_system.research_probes.exit_rules_after",
        "ai_trading_system.research_probes.exit_rules_counts",
        "ai_trading_system.research_probes.exit_rules_ndx",
        "ai_trading_system.research_probes.exit_rules_returns",
    )
    for name in imported:
        assert name.startswith(allowed), name
    for required in ("entry_timing_ndx", "exit_rules_after", "exit_rules_counts", "exit_rules_ndx"):
        assert f"ai_trading_system.research_probes.{required}" in imported
    banned = (
        "us_market",
        "French",
        "Mkt-RF",
        "vix",
        "VIX",
        "vxn",
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
        "def exit_for",
        "def prepare",
        "def check_step0",
        "def check_inputs",
        "def qqq_inputs",
        "def prices",
        "def first_days",
        "def rank",
        "def mean",
        "def moving_average",
        "def weekly_drops",
        "numpy",
        "pandas",
    )
    for word in banned:
        assert word not in text, word
    assert text.count(".write_text(") == 2
    for call in (
        "xc.load_data(",
        "en.load_data(",
        "xa.qqq_inputs(",
        "en.prices(",
        "xc.prepare(",
        "xn.check_inputs(",
        "xa.first_days(",
    ):
        assert call in text, call


def test_the_registry_names_the_fourth_reader_and_keeps_the_window() -> None:
    text = REGISTRY.read_text(encoding="utf-8")
    block = text[text.index("# Baker's print-only read of the 200-day pullback rule") :]
    note = " ".join(block[: block.index("  ndx_price_1986_1999:")].replace("#", " ").split())
    for words in (
        "TRADING-2565-117",
        "owner_decision:TRADING-2565:2026-11-02:exit_family_big_declines",
        "research_probes/exit_rules_declines.py",
        "fourth read",
        "each decline of at least 15%",
        "research_probes/exit_rules_after.py",
    ):
        assert words in note, words
    window = safe_load_yaml_path(REGISTRY)["windows"]["ndx_price_1986_1999"]
    assert str(window["start"]) == "1986-01-02"
    assert str(window["end"]) == "1999-03-09"
    assert window["role"] == "owner_print_only"
    assert window["question_types"] == []
    assert window["read_policy"] == "once_per_owner_decision"


# The zigzag ----------------------------------------------------------------------------------


def test_the_zigzag_by_hand() -> None:
    close = [100.0, 110.0, 120.0, 102.0, 101.0, 100.0, 115.0, 99.0, 85.0, 80.0, 92.0, 93.0]
    found = xd.pivots(close)
    assert found == [(0, "trough"), (2, "peak"), (5, "trough"), (6, "peak"), (9, "trough")]
    assert xd.declines(found) == [(2, 5), (6, 9)]


def test_exactly_fifteen_percent_counts_both_ways() -> None:
    assert xd.pivots([100.0, 115.0, 97.75]) == [(0, "trough"), (1, "peak")]
    assert xd.pivots([100.0, 114.99, 98.0]) == []
    assert xd.pivots([100.0, 85.0, 97.75]) == [(0, "peak"), (1, "trough")]
    assert xd.pivots([100.0, 85.01, 97.0]) == []


def test_equal_closes_keep_the_first_one() -> None:
    assert xd.pivots([100.0, 120.0, 120.0, 102.0]) == [(0, "trough"), (1, "peak")]
    assert xd.pivots([100.0, 80.0, 80.0, 92.0]) == [(0, "peak"), (1, "trough")]
    assert xd.pivots([90.0, 120.0, 100.0, 100.0, 101.0, 115.0]) == [
        (0, "trough"),
        (1, "peak"),
        (2, "trough"),
    ]


def test_an_unconfirmed_last_leg_is_not_a_decline() -> None:
    found = xd.pivots([100.0, 120.0, 100.0, 101.0])
    assert found == [(0, "trough"), (1, "peak")]
    assert xd.declines(found) == []
    assert xd.pivots([]) == [] and xd.pivots([5.0]) == []


def test_a_fall_of_less_than_fifteen_percent_may_go_below_the_last_low() -> None:
    # 80 -> 92 confirms the low at 80; 92 -> 79 is a fall of 14.1%, so no peak is confirmed and
    # the low stays at 80 although 79 is lower. Only peaks are the extreme of both legs (3.2).
    found = xd.pivots([100.0, 80.0, 92.0, 79.0, 95.0])
    assert found == [(0, "peak"), (1, "trough")]


def test_a_series_that_starts_falling_is_cut_at_the_start() -> None:
    found = xd.pivots([100.0, 90.0, 84.0, 97.0])
    assert found == [(0, "peak"), (2, "trough")]
    assert xd.declines(found) == [(0, 2)]


def _walk(seed: int, n: int = 1500, sigma: float = 0.02) -> list[float]:
    rng = np.random.default_rng(seed)
    return [float(v) for v in 100.0 * np.cumprod(np.exp(rng.normal(0.0003, sigma, n)))]


@pytest.mark.parametrize("seed", range(6))
def test_zigzag_properties_on_random_walks(seed: int) -> None:
    close = _walk(seed)
    found = xd.pivots(close)
    assert len(found) >= 4
    kinds = [kind for _, kind in found]
    assert all(a != b for a, b in zip(kinds, kinds[1:], strict=False))
    places = [i for i, _ in found]
    assert places == sorted(places) and len(set(places)) == len(places)
    for (a, kind), (b, _) in zip(found, found[1:], strict=False):
        part = close[a : b + 1]
        if kind == "peak":
            assert close[b] / close[a] <= 0.85
            assert close[a] == max(part) and part.index(max(part)) == 0
            assert close[b] == min(part)
        else:
            assert close[b] / close[a] >= 1.15
            assert close[b] == max(part) and part.index(max(part)) == len(part) - 1
    last, kind = found[-1]
    tail = close[last:]
    if kind == "peak":
        assert min(tail) / close[last] > 0.85 or tail.index(min(tail)) > 0
    pairs = xd.declines(found)
    assert pairs == [
        (a, b) for (a, k), (b, _) in zip(found, found[1:], strict=False) if k == "peak"
    ]


# The four rules for a holder -------------------------------------------------------------------


def _dates(n: int) -> list[date]:
    return [day.date() for day in pd.bdate_range("1990-01-01", periods=n)]


def _prepared(close: list[float]) -> xc.Prepared:
    return xc.prepare(_dates(len(close)), close)


def test_x2_fires_at_eight_percent_and_starts_again_the_next_day() -> None:
    close = [100.0, 95.0, 92.0, 93.0, 100.0, 92.0, 99.0, 91.0]
    data = _prepared(close)
    assert xd.first_signal("X2", data, 0) == 2
    assert xd.first_signal("X2", data, 3) == 5
    assert xd.first_signal("X2", data, 6) == 7
    assert xd.first_signal("X2", data, 7) is None
    assert xd.signal_days("X2", data) == [2, 5, 7]


def test_an_unknown_rule_is_refused() -> None:
    with pytest.raises(ValueError, match="unknown rule"):
        xd.signal_days("X0", _prepared([100.0] * 5))


@pytest.mark.parametrize("seed", range(4))
def test_the_holder_rules_agree_with_step_zero(seed: int) -> None:
    close = _walk(seed + 40, 1200, 0.018)
    data = _prepared(close)
    days = {rule: xd.signal_days(rule, data) for rule in FOUR}
    checked = 0
    for e in range(300, 1050, 23):
        for rule in FOUR:
            if rule == "X2":
                sigma = data.sigma[e]
                assert sigma is not None
                want = xc.exit_for("X2", data, e, xc.STOP / sigma).decision
                mine = xd.first_signal("X2", data, e)
            elif rule == "X5":
                want = xc.exit_for("X5", data, e, 1.0).decision
                mine = xd.first_signal("X5", data, e)
            else:
                want = xc.exit_for(rule, data, e, 1.0).decision
                later = [d for d in days[rule] if d > e]
                if rule == "X4":
                    later = [d for d in later if data.week[d] > data.week[e]]
                mine = later[0] if later else None
            got = mine if mine is not None and mine < e + xc.CAP[rule] else None
            assert got == want, (rule, e)
            checked += want is not None
    assert checked >= 40


@pytest.mark.parametrize("seed", range(4))
def test_x5_agrees_with_step_zero_when_closes_repeat(seed: int) -> None:
    # Whole-number closes repeat often, so a close equal to the break high comes up (3.3: a
    # close at or above the break high starts the search again).
    close = [float(round(v)) for v in _walk(seed + 60, 1200, 0.018)]
    data = _prepared(close)
    checked = 0
    for e in range(300, 1050, 7):
        want = xc.exit_for("X5", data, e, 1.0).decision
        mine = xd.first_signal("X5", data, e)
        got = mine if mine is not None and mine < e + xc.CAP["X5"] else None
        assert got == want, e
        checked += want is not None
    assert checked >= 40


def test_x3_and_x4_are_the_conditions_on_every_day() -> None:
    close = _walk(9, 900, 0.018)
    data = _prepared(close)
    want3 = [
        d
        for d in range(1, len(close))
        if data.sma[d] is not None
        and data.sma[d - 1] is not None
        and close[d] < float(data.sma[d] or 0.0)
        and close[d - 1] < float(data.sma[d - 1] or 0.0)
    ]
    assert xd.signal_days("X3", data) == want3
    assert xd.signal_days("X4", data) == [d for d, flag in enumerate(data.drop) if flag]
    assert want3 and any(data.drop)


@pytest.mark.parametrize("rule", ["X2", "X5"])
def test_x2_and_x5_restart_after_each_signal(rule: str) -> None:
    data = _prepared(_walk(12, 1200, 0.02))
    days = xd.signal_days(rule, data)
    assert len(days) >= 3
    assert days[0] == xd.first_signal(rule, data, 0)
    for a, b in zip(days, days[1:], strict=False):
        assert b == xd.first_signal(rule, data, a + 1)
        assert b >= a + 2
    assert xd.first_signal(rule, data, days[-1] + 1) is None


def test_when_each_rule_has_history() -> None:
    data = _prepared(_walk(3, 700))
    ends = [p for p in range(len(data.close) - 1) if data.week[p + 1] > data.week[p]]
    assert xd.ready("X2", data) == 0
    assert xd.ready("X3", data) == xd.ready("X5", data) == xc.SMA_DAYS - 1
    assert xd.ready("X4", data) == ends[xc.WEEKS + 1]
    positions = [p for p, end in enumerate(xc.week_ends(_dates(len(data.close)))) if end]
    assert xd.ready("X4", data) == positions[xc.WEEKS + 1]
    assert not any(data.drop[: xd.ready("X4", data)])
    assert xd.ready("X4", _prepared(_walk(3, 200))) == 200


# One decline ---------------------------------------------------------------------------------


CLOSE = [100.0, 110.0, 120.0, 118.0, 115.0, 108.0, 100.0, 96.0, 90.0, 84.0, 88.0, 99.0]
DATES = _dates(len(CLOSE))


def test_a_rule_that_fires_inside_the_decline() -> None:
    row = xd.episode(CLOSE, DATES, 2, 9, None, [1, 4, 7], 0)
    assert row["status"] == "fired"
    assert row["date"] == DATES[4].isoformat()
    assert row["days_after_peak"] == 2
    assert row["fall_at"] == pytest.approx(115.0 / 120.0 - 1.0)
    assert row["fall_before"] == pytest.approx(118.0 / 120.0 - 1.0)
    assert row["done"] == pytest.approx(math.log(115.0 / 120.0) / math.log(84.0 / 120.0))
    assert row["after_sell"] == pytest.approx(84.0 / 108.0 - 1.0)
    assert row["avoided"] == pytest.approx((108.0 - 84.0) / (120.0 - 84.0))
    assert row["pre_peak"] is True
    assert list(row) == [
        "status",
        "date",
        "days_after_peak",
        "fall_at",
        "fall_before",
        "done",
        "after_sell",
        "avoided",
        "pre_peak",
    ]


def test_the_first_day_after_the_peak_has_no_fall_before() -> None:
    row = xd.episode(CLOSE, DATES, 2, 9, None, [3], 0)
    assert row["days_after_peak"] == 1 and row["fall_before"] == 0.0
    assert row["pre_peak"] is False


def test_a_silent_rule_and_a_rule_without_history() -> None:
    assert xd.episode(CLOSE, DATES, 2, 9, None, [0, 10, 11], 0) == {
        "status": "silent",
        "avoided": 0.0,
        "pre_peak": True,
    }
    assert xd.episode(CLOSE, DATES, 2, 9, None, [], 0) == {
        "status": "silent",
        "avoided": 0.0,
        "pre_peak": False,
    }
    assert xd.episode(CLOSE, DATES, 2, 9, None, [4], 3) == {"status": "no_history"}
    assert xd.episode(CLOSE, DATES, 2, 9, None, [4], 2)["status"] == "fired"


def test_firing_on_the_trough_day_avoids_nothing() -> None:
    row = xd.episode(CLOSE, DATES, 2, 9, None, [9], 0)
    assert row["status"] == "fired"
    assert row["after_sell"] == 0.0 and row["avoided"] == 0.0
    assert row["done"] == pytest.approx(1.0)
    assert row["fall_at"] == pytest.approx(84.0 / 120.0 - 1.0)


def test_the_pre_peak_window_stops_after_the_last_low() -> None:
    assert xd.window_start(30, None) == 10
    assert xd.window_start(30, 20) == 21
    assert xd.window_start(30, 5) == 10
    assert xd.window_start(12, None) == 0
    long = [100.0] * 40 + [80.0] * 2
    dates = _dates(len(long))
    assert xd.episode(long, dates, 30, 41, 20, [20], 0)["pre_peak"] is False
    assert xd.episode(long, dates, 30, 41, 20, [21], 0)["pre_peak"] is True
    assert xd.episode(long, dates, 30, 41, 20, [30], 0)["pre_peak"] is True
    assert xd.episode(long, dates, 30, 41, None, [9], 0)["pre_peak"] is False
    assert xd.episode(long, dates, 30, 41, None, [10], 0)["pre_peak"] is True
    found = [(3, "trough"), (8, "peak"), (12, "trough"), (20, "peak")]
    assert (xd.previous_low(found, 20), xd.previous_low(found, 8), xd.previous_low(found, 3)) == (
        12,
        3,
        None,
    )


def _fired(days: int, fall: float, done: float, avoided: float, pre: bool) -> dict[str, Any]:
    return {
        "status": "fired",
        "days_after_peak": days,
        "fall_at": fall,
        "done": done,
        "avoided": avoided,
        "pre_peak": pre,
    }


def test_the_summary_of_one_rule() -> None:
    rows = [
        _fired(3, -0.05, 0.2, 0.7, False),
        _fired(9, -0.12, 0.5, 0.4, True),
        {"status": "silent", "avoided": 0.0, "pre_peak": True},
        {"status": "no_history"},
        _fired(1, -0.02, 1.0 / 3.0, 0.0, True),
    ]
    x2 = [
        _fired(5, -0.08, 0.4, 0.6, False),
        _fired(6, -0.08, 0.3, 0.5, False),
        _fired(4, -0.08, 0.5, 0.5, False),
        _fired(4, -0.08, 0.5, 0.5, False),
        _fired(1, -0.08, 0.5, 0.5, False),
    ]
    got = xd.summary(rows, x2)
    assert got == {
        "n": 4,
        "no_history": 1,
        "fired": 3,
        "early": 2,
        "early_quiet": 1,
        "before_x2": 1,
        "pre_peak": 3,
        "fall_at_mean": pytest.approx((-0.05 - 0.12 - 0.02) / 3),
        "fall_at_median": -0.05,
        "done_mean": pytest.approx((0.2 + 0.5 + 1.0 / 3.0) / 3),
        "days_median": 3.0,
        "avoided_mean": pytest.approx(1.1 / 4),
    }
    assert xd.summary(rows, None)["before_x2"] is None
    empty = xd.summary([{"status": "no_history"}], None)
    assert empty["n"] == 0 and empty["avoided_mean"] is None and empty["fall_at_mean"] is None
    assert empty["fall_at_median"] is None and empty["days_median"] is None


def test_the_x5_x3_pair() -> None:
    rows = [
        {"X3": _fired(2, 0, 0, 0, False), "X5": _fired(5, 0, 0, 0, False)},
        {"X3": _fired(4, 0, 0, 0, False), "X5": _fired(4, 0, 0, 0, False)},
        {"X3": _fired(6, 0, 0, 0, False), "X5": _fired(5, 0, 0, 0, False)},
        {"X3": _fired(6, 0, 0, 0, False), "X5": {"status": "silent"}},
        {"X3": {"status": "no_history"}, "X5": {"status": "no_history"}},
    ]
    assert xd.pair(rows) == {"both": 3, "x5_not_earlier": 2}


def test_alarms_are_cluster_starts_sorted_into_three_kinds() -> None:
    spans = [(40, 60, None), (100, 130, 90)]
    assert xd.alarm_kind(41, spans) == "decline"
    assert xd.alarm_kind(60, spans) == "decline"
    assert xd.alarm_kind(40, spans) == "pre_peak"
    assert xd.alarm_kind(20, spans) == "pre_peak"
    assert xd.alarm_kind(19, spans) == "false"
    assert xd.alarm_kind(90, spans) == "false"
    assert xd.alarm_kind(91, spans) == "pre_peak"
    assert xd.alarm_kind(140, spans) == "false"
    dates = [date(2000, 1, 1) + pd.Timedelta(days=7 * i) for i in range(200)]
    found = [(10, "trough"), (40, "peak"), (60, "trough"), (100, "peak"), (130, "trough")]
    days = [5, 6, 30, 31, 45, 70, 95, 101, 125, 150, 180]
    got = xd.alarms(days, dates, found, spans)
    assert got["rows"] == [
        {"date": dates[30].isoformat(), "kind": "pre_peak"},
        {"date": dates[70].isoformat(), "kind": "false"},
        {"date": dates[95].isoformat(), "kind": "pre_peak"},
        {"date": dates[125].isoformat(), "kind": "decline"},
    ]
    assert got["outside"] == 3
    assert got["years"] == pytest.approx((dates[130] - dates[10]).days / 365.25)
    assert (got["decline"], got["pre_peak"], got["false"]) == (1, 2, 1)
    assert got["per_year"] == pytest.approx(4 / got["years"])
    assert got["false_per_year"] == pytest.approx(1 / got["years"])
    assert xd.alarms(days, dates, [], [])["outside"] == len(xa.first_days(days))
    edges = xd.alarms([10, 130], dates, found, spans)
    assert edges["rows"] == [
        {"date": dates[10].isoformat(), "kind": "false"},
        {"date": dates[130].isoformat(), "kind": "decline"},
    ]
    assert edges["outside"] == 0


# Synthetic QQQ ruler and Nasdaq-100 levels ---------------------------------------------------


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
Q_SERIES = ec.closes(QQQ.returns["qqq"], date(2010, 12, 31))
Q_PREPARED = xc.prepare(Q_SERIES.dates, Q_SERIES.close)

INDEX = pd.bdate_range("1986-01-02", "1999-03-09", name="date")


def _ndx_levels() -> list[tuple[date, float]]:
    rng = np.random.default_rng(5)
    level = 100.0 * np.cumprod(1.0006 * (1.0 + rng.normal(0.0, 0.011, len(INDEX))))
    for first, last, depth in (
        ("1987-10-12", "1987-10-23", 0.7),
        ("1990-07-16", "1990-10-12", 0.8),
    ):
        a, b = _pos(INDEX, first), _pos(INDEX, last)
        level[a : b + 1] *= np.linspace(1.0, depth, b - a + 1)
        level[b + 1 :] *= depth
    for day in ("1988-04-01", "1995-12-25"):
        level[_pos(INDEX, day)] = level[_pos(INDEX, day) - 1]
    return [(day.date(), float(value)) for day, value in zip(INDEX, level, strict=True)]


LEVELS = _ndx_levels()
N_SERIES, N_FLAT = en.prices(LEVELS)
N_PREPARED = xc.prepare(N_SERIES.dates, N_SERIES.close)
N_ENTRIES = xc.entry_range(N_PREPARED, 0)
N_MOVE = en.largest_move(N_SERIES)
assert N_MOVE is not None
SERIES = {"qqq": (Q_SERIES, Q_PREPARED), "ndx": (N_SERIES, N_PREPARED)}


@pytest.fixture
def same_data(monkeypatch: pytest.MonkeyPatch) -> None:
    a = STEP0["entry_set_a"]
    monkeypatch.setattr(xr, "STEP0_TRADES", a["trades"])
    monkeypatch.setattr(xr, "STEP0_FIRST", a["first"])
    monkeypatch.setattr(xr, "STEP0_LAST", a["last"])
    monkeypatch.setattr(xr, "STEP0_K", STEP0["scale"]["k"])
    monkeypatch.setattr(xr, "H_MATCH", {r: STEP0["exits"][r]["h_match"] for r in xc.RULES})
    assert N_MOVE is not None
    monkeypatch.setattr(xn, "NDX_CLOSES", len(N_SERIES.dates))
    monkeypatch.setattr(xn, "NDX_FLAT_DATES", tuple(day.isoformat() for day in N_FLAT))
    monkeypatch.setattr(xn, "NDX_MOVE_DATE", N_MOVE["date"])
    monkeypatch.setattr(xn, "NDX_MOVE", round(N_MOVE["return"], 4))


@pytest.fixture
def table(same_data: None) -> dict[str, Any]:
    return xd.run_table(QQQ, LOADED, GATE, LEVELS, SOURCE, WINDOW)


def test_the_synthetic_data_are_not_the_real_ones() -> None:
    assert STEP0["entry_set_a"]["trades"] != xr.STEP0_TRADES
    assert len(N_SERIES.dates) != xn.NDX_CLOSES
    assert len(N_ENTRIES) >= xn.MIN_TRADES


# Checks --------------------------------------------------------------------------------------


def test_the_real_constants_stop_the_table_before_any_decline() -> None:
    result = xd.run_table(QQQ, LOADED, GATE, LEVELS, SOURCE, WINDOW)
    assert result["complete"] is False
    assert result["check"]["qqq"]["ok"] is False
    assert result["check"]["ndx"]["ok"] is False
    assert "spans" not in result
    assert list(result) == ["run_id", "owner_decision", "source", "rule", "check", "complete"]


def test_each_span_s_check_alone_stops_the_table(
    same_data: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(xn, "NDX_CLOSES", len(N_SERIES.dates) + 1)
    one = xd.run_table(QQQ, LOADED, GATE, LEVELS, SOURCE, WINDOW)
    assert (one["check"]["qqq"]["ok"], one["check"]["ndx"]["ok"], one["complete"]) == (
        True,
        False,
        False,
    )
    monkeypatch.setattr(xn, "NDX_CLOSES", len(N_SERIES.dates))
    monkeypatch.setattr(xr, "STEP0_K", STEP0["scale"]["k"] * 1.001)
    two = xd.run_table(QQQ, LOADED, GATE, LEVELS, SOURCE, WINDOW)
    assert (two["check"]["qqq"]["ok"], two["check"]["ndx"]["ok"], two["complete"]) == (
        False,
        True,
        False,
    )
    assert "spans" not in two


def test_the_checks_are_the_family_s_own(table: dict[str, Any]) -> None:
    *_, q_check = xa.qqq_inputs(QQQ, LOADED, GATE)
    assert table["check"]["qqq"] == q_check
    assert table["check"]["ndx"] == xn.check_inputs(N_SERIES, N_FLAT, WINDOW, N_ENTRIES)
    assert table["source"] == {"qqq": {"snapshot_id": PLAN.data.snapshot_id}, "ndx": SOURCE}
    assert table["run_id"] == "TRADING-2565-117"
    assert table["owner_decision"] == xd.OWNER_DECISION
    assert table["rule"] == {
        "rules": ["X2", "X3", "X4", "X5"],
        "not_applicable": ["X0", "X1", "X6"],
        "swing_line": 0.15,
        "trail": 0.08,
        "pre_peak": 20,
        "early": 1.0 / 3.0,
        "cluster_gap": xc.CLUSTER_GAP,
        "sell": "next_close",
    }
    assert table["complete"] is True
    assert list(table["spans"]) == ["qqq", "ndx"]


# Every number recomputed --------------------------------------------------------------------


def _own_days(rule: str, data: xc.Prepared) -> list[int]:
    close = data.close
    n = len(close)
    if rule == "X3":
        return [
            d
            for d in range(1, n)
            if (a := data.sma[d]) is not None
            and (b := data.sma[d - 1]) is not None
            and close[d] < a
            and close[d - 1] < b
        ]
    if rule == "X4":
        return [d for d in range(n) if data.drop[d]]
    if rule == "X2":
        days: list[int] = []
        high, d = close[0], 1
        while d < n:
            high = max(high, close[d])
            if close[d] / high <= 0.92:
                days.append(d)
                if d + 1 >= n:
                    break
                high, d = close[d + 1], d + 2
                continue
            d += 1
        return days
    return xd.signal_days(rule, data)


def _own_episode(
    close: tuple[float, ...], p: int, q: int, q0: int | None, days: list[int], start: int
) -> dict[str, Any]:
    if p < start:
        return {"status": "no_history"}
    low = max(p - 20, 0 if q0 is None else q0 + 1)
    pre = any(low <= d <= p for d in days)
    inside = [d for d in days if p < d <= q]
    if not inside:
        return {"status": "silent", "avoided": 0.0, "pre_peak": pre}
    d = inside[0]
    sold = close[d + 1]
    return {
        "days_after_peak": d - p,
        "fall_at": close[d] / close[p] - 1.0,
        "fall_before": close[d - 1] / close[p] - 1.0,
        "done": math.log(close[d] / close[p]) / math.log(close[q] / close[p]),
        "after_sell": close[q] / sold - 1.0 if d + 1 <= q else 0.0,
        "avoided": (sold - close[q]) / (close[p] - close[q]) if d + 1 <= q else 0.0,
        "pre_peak": pre,
    }


@pytest.mark.parametrize("span", ["qqq", "ndx"])
def test_each_decline_recomputed(table: dict[str, Any], span: str) -> None:
    series, data = SERIES[span]
    close, dates = series.close, series.dates
    got = table["spans"][span]
    found = xd.pivots(close)
    pairs = xd.declines(found)
    assert got["episode_count"] == len(pairs) == len(got["episodes"]) >= 3
    assert got["pivots"] == [
        {"date": dates[i].isoformat(), "kind": kind, "close": close[i]} for i, kind in found
    ]
    assert (got["first_day"], got["last_day"]) == (dates[0].isoformat(), dates[-1].isoformat())
    days = {rule: _own_days(rule, data) for rule in FOUR}
    for rule in ("X2", "X3", "X4"):
        assert days[rule] == xd.signal_days(rule, data), rule
    for (p, q), one in zip(pairs, got["episodes"], strict=True):
        lows = [i for i, kind in found if kind == "trough" and i < p]
        q0 = lows[-1] if lows else None
        assert one["peak"] == dates[p].isoformat() and one["trough"] == dates[q].isoformat()
        assert one["truncated"] is (p == 0)
        assert one["depth"] == pytest.approx(close[q] / close[p] - 1.0)
        assert one["days"] == q - p
        assert one["rise_before"] == (
            None if q0 is None else pytest.approx(close[p] / close[q0] - 1)
        )
        assert one["rise_days"] == (None if q0 is None else p - q0)
        assert list(one["rules"]) == list(FOUR)
        for rule in FOUR:
            want = _own_episode(close, p, q, q0, days[rule], xd.ready(rule, data))
            row = one["rules"][rule]
            for key, value in want.items():
                assert row[key] == pytest.approx(value), (span, rule, key)
            if row["status"] == "fired":
                assert row["date"] == dates[p + row["days_after_peak"]].isoformat()


@pytest.mark.parametrize("span", ["qqq", "ndx"])
def test_x2_fires_in_every_decline_within_its_bounds(table: dict[str, Any], span: str) -> None:
    for one in table["spans"][span]["episodes"]:
        row = one["rules"]["X2"]
        assert row["status"] == "fired"
        assert row["fall_at"] <= -0.08
        if row["days_after_peak"] > 1:
            assert row["fall_before"] > -0.08
    rules = table["spans"][span]["rules"]
    assert rules["X2"]["fired"] == rules["X2"]["n"] == table["spans"][span]["episode_count"]


@pytest.mark.parametrize("span", ["qqq", "ndx"])
def test_each_rule_s_summary_and_alarms(table: dict[str, Any], span: str) -> None:
    series, data = SERIES[span]
    got = table["spans"][span]
    found = xd.pivots(series.close)
    spans = [
        (p, q, max([i for i, k in found if k == "trough" and i < p], default=None))
        for p, q in xd.declines(found)
    ]
    for rule in FOUR:
        rows = [one["rules"][rule] for one in got["episodes"]]
        x2 = [one["rules"]["X2"] for one in got["episodes"]]
        kept = [r for r in rows if r["status"] != "no_history"]
        fired = [r for r in kept if r["status"] == "fired"]
        one = got["rules"][rule]
        days = xd.signal_days(rule, data)
        assert one["signals"] == len(days)
        start = xd.ready(rule, data)
        assert one["ready"] == (
            series.dates[start].isoformat() if start < len(series.dates) else None
        )
        assert one["n"] == len(kept) and one["no_history"] == len(rows) - len(kept)
        assert one["fired"] == len(fired)
        assert one["early"] == sum(r["done"] <= 1 / 3 for r in fired)
        assert one["early_quiet"] == sum(r["done"] <= 1 / 3 and not r["pre_peak"] for r in fired)
        assert one["pre_peak"] == sum(r["pre_peak"] for r in kept)
        if rule == "X2":
            assert one["before_x2"] is None
        else:
            assert one["before_x2"] == sum(
                r["status"] == "fired" and r["days_after_peak"] < s["days_after_peak"]
                for r, s in zip(rows, x2, strict=True)
                if r["status"] != "no_history"
            )
        if fired:
            falls = sorted(r["fall_at"] for r in fired)
            assert one["fall_at_mean"] == pytest.approx(sum(falls) / len(falls))
            assert one["fall_at_median"] == falls[math.ceil(len(falls) / 2) - 1]
            assert one["done_mean"] == pytest.approx(sum(r["done"] for r in fired) / len(fired))
            after = sorted(float(r["days_after_peak"]) for r in fired)
            assert one["days_median"] == after[math.ceil(len(after) / 2) - 1]
        assert one["avoided_mean"] == pytest.approx(sum(r["avoided"] for r in kept) / len(kept))
        alarm = one["alarms"]
        lo, hi = found[0][0], found[-1][0]
        ordered = sorted(set(days))
        onsets = [d for i, d in enumerate(ordered) if i == 0 or d - ordered[i - 1] >= 20]
        inside = [o for o in onsets if lo <= o <= hi]
        assert alarm["outside"] == len(onsets) - len(inside)
        kinds = []
        for o in inside:
            if any(p < o <= q for p, q, _ in spans):
                kinds.append("decline")
            elif any(max(p - 20, 0 if q0 is None else q0 + 1) <= o <= p for p, _, q0 in spans):
                kinds.append("pre_peak")
            else:
                kinds.append("false")
        assert [r["kind"] for r in alarm["rows"]] == kinds
        assert [r["date"] for r in alarm["rows"]] == [series.dates[o].isoformat() for o in inside]
        years = (series.dates[hi] - series.dates[lo]).days / 365.25
        assert alarm["years"] == pytest.approx(years)
        assert alarm["per_year"] == pytest.approx(len(inside) / years)
        assert alarm["false_per_year"] == pytest.approx(kinds.count("false") / years)
        for kind in ("decline", "pre_peak", "false"):
            assert alarm[kind] == kinds.count(kind)
    both = [
        one["rules"]
        for one in got["episodes"]
        if one["rules"]["X3"]["status"] == one["rules"]["X5"]["status"] == "fired"
    ]
    assert got["x5_vs_x3"] == {
        "both": len(both),
        "x5_not_earlier": sum(
            r["X5"]["days_after_peak"] >= r["X3"]["days_after_peak"] for r in both
        ),
    }


def test_the_synthetic_table_exercises_every_case(table: dict[str, Any]) -> None:
    rows = [
        one["rules"][rule]
        for span in xd.SPANS
        for one in table["spans"][span]["episodes"]
        for rule in FOUR
    ]
    statuses = {row["status"] for row in rows}
    assert statuses == {"fired", "silent", "no_history"}
    assert any(row.get("pre_peak") for row in rows)
    assert any(row["status"] == "fired" and row["avoided"] > 0 for row in rows)
    kinds = {
        alarm["kind"]
        for span in xd.SPANS
        for rule in FOUR
        for alarm in table["spans"][span]["rules"][rule]["alarms"]["rows"]
    }
    assert kinds == {"decline", "pre_peak", "false"}


def test_no_nan_reaches_the_table(table: dict[str, Any]) -> None:
    json.dumps(table, allow_nan=False)


# Text ----------------------------------------------------------------------------------------


def test_the_cell_and_number_texts() -> None:
    assert xd.cell({"status": "no_history"}) == "缺历史"
    assert xd.cell({"status": "silent", "avoided": 0.0, "pre_peak": False}) == "没响"
    assert xd.cell({"status": "silent", "avoided": 0.0, "pre_peak": True}) == "没响＊"
    row = _fired(3, -0.0834, 0.3, 0.6149, False)
    assert xd.cell(row) == "-8.34% / 61%"
    row["pre_peak"] = True
    assert xd.cell(row) == "-8.34% / 61%＊"
    assert (xd.share_text(0.355), xd.share_text(None)) == ("36%", "—")
    assert (xd.rate_text(1.25), xd.rate_text(0.04), xd.rate_text(None)) == ("1.2", "0.0", "—")


def test_the_markdown(table: dict[str, Any]) -> None:
    md = xd.render_markdown(table)
    lines = md.split("\n")
    assert lines[0] == "# QQQ 出场族按大跌逐次看（TRADING-2565-117）"
    assert lines[2] == xd.NOTE
    assert md.endswith("。\n") and not md.endswith("\n\n")
    header = (
        "| 规则 | 是什么 | 大跌次数 | 响了 | 早 | 早而且高点前没响 | 比 X2 先响 | 平均响时已跌 | "
        "中位响时已跌 | 平均已跌占整段 | 离高点天数中位 | 平均躲掉几成 | 高点前 20 天响过 | "
        "警报每年 | 虚惊每年 | 在大跌里 / 高点前 / 虚惊 |"
    )
    assert lines.count(header) == 2
    episode_header = "| 高点 | 低点 | 跌幅 | 天数 | 之前涨了 | 上涨段天数 | X2 | X3 | X4 | X5 |"
    assert lines.count(episode_header) == 2
    for span in xd.SPANS:
        got = table["spans"][span]
        assert f"## {xd.SPAN_TEXT[span]}" in lines
        assert (
            f"收盘 {got['first_day']}..{got['last_day']}，"
            f"跌过 15% 的大跌 {got['episode_count']} 次。"
        ) in lines
        for rule in FOUR:
            one = got["rules"][rule]
            alarm = one["alarms"]
            want = (
                f"| {rule} | {xd.RULE_TEXT[rule]} | {one['n']} | {one['fired']} | {one['early']} | "
                f"{one['early_quiet']} | "
                f"{'—' if one['before_x2'] is None else one['before_x2']} | "
                f"{xr.pct(one['fall_at_mean'])} | {xr.pct(one['fall_at_median'])} | "
                f"{xd.share_text(one['done_mean'])} | "
                f"{'—' if one['days_median'] is None else format(one['days_median'], '.0f')} | "
                f"{xd.share_text(one['avoided_mean'])} | {one['pre_peak']} | "
                f"{xd.rate_text(alarm['per_year'])} | {xd.rate_text(alarm['false_per_year'])} | "
                f"{alarm['decline']} / {alarm['pre_peak']} / {alarm['false']} |"
            )
            assert want in lines, (span, rule)
        pair = got["x5_vs_x3"]
        assert (
            f"X3、X5 都响的 {pair['both']} 次里，X5 不比 X3 早的 {pair['x5_not_earlier']} 次。"
        ) in lines
        for one in got["episodes"]:
            peak = one["peak"] + ("（开头截断）" if one["truncated"] else "")
            rise_days = "—" if one["rise_days"] is None else str(one["rise_days"])
            want = (
                f"| {peak} | {one['trough']} | {xr.pct(one['depth'])} | {one['days']} | "
                f"{xr.pct(one['rise_before'])} | {rise_days} | "
                + " | ".join(xd.cell(one["rules"][rule]) for rule in FOUR)
                + " |"
            )
            assert want in lines, (span, one["peak"])
        outside = "、".join(f"{rule} {got['rules'][rule]['alarms']['outside']}" for rule in FOUR)
        assert f"第一个和最后一个拐点以外、没分类的警报：{outside}。" in lines
    rows = [line for line in lines if line.startswith("| X")]
    assert len(rows) == 8 and all(line.count("|") == 17 for line in rows)
    episodes = sum(table["spans"][span]["episode_count"] for span in xd.SPANS)
    dated = [line for line in lines if line.startswith(("| 19", "| 20"))]
    assert len(dated) == episodes and all(line.count("|") == 11 for line in dated)
    assert "每次大跌：每条规则的格写「响时已跌 / 躲掉几成」；＊ 是高点前 20 天里" in md


def test_a_truncated_first_peak_is_marked() -> None:
    result = {
        "spans": {
            span: {
                "first_day": "1990-01-01",
                "last_day": "1990-02-01",
                "episode_count": 1,
                "episodes": [
                    {
                        "peak": "1990-01-01",
                        "trough": "1990-01-05",
                        "truncated": True,
                        "depth": -0.2,
                        "days": 4,
                        "rise_before": None,
                        "rise_days": None,
                        "rules": {rule: {"status": "no_history"} for rule in FOUR},
                    }
                ],
                "rules": {
                    rule: {
                        **xd.summary([{"status": "no_history"}], None),
                        "alarms": xd.alarms([], [date(1990, 1, 1)], [], []),
                    }
                    for rule in FOUR
                },
                "x5_vs_x3": {"both": 0, "x5_not_earlier": 0},
            }
            for span in xd.SPANS
        }
    }
    md = xd.render_markdown(result)
    assert "| 1990-01-01（开头截断） | 1990-01-05 | -20.00% | 4 | — | — | 缺历史 |" in md


@pytest.mark.parametrize(
    ("close", "peak", "truncated"),
    [([120.0, 100.0, 90.0, 104.0], 0, True), ([100.0, 110.0, 93.0, 90.0, 104.0], 1, False)],
)
def test_only_a_peak_on_the_first_close_is_cut(
    close: list[float], peak: int, truncated: bool
) -> None:
    dates = tuple(_dates(len(close)))
    got = xd.span_table(ec.Closes(dates, tuple(close), 0), _prepared(close))
    assert got["episode_count"] == 1
    one = got["episodes"][0]
    assert one["peak"] == dates[peak].isoformat()
    assert one["truncated"] is truncated
    assert (one["rise_before"], one["rise_days"]) == (None, None)


def test_the_summary_line(table: dict[str, Any]) -> None:
    out = Path("o")
    line = xd.summary_line(table, out)
    q, n = table["spans"]["qqq"], table["spans"]["ndx"]
    assert line.startswith(
        f"exit_rules_declines: 按大跌逐次看：{xd.SPAN_TEXT['qqq']} "
        f"大跌 {q['episode_count']} 次，X2 躲掉 "
    )
    assert f"；{en.TITLE} 大跌 {n['episode_count']} 次，X2 躲掉 " in line
    last = n["rules"]["X5"]
    assert line.endswith(
        f"X5 躲掉 {xd.share_text(last['avoided_mean'])}、"
        f"虚惊每年 {xd.rate_text(last['alarms']['false_per_year'])}；结果目录 o"
    )
    assert line.count("躲掉") == 8 and line.count("虚惊每年") == 8


# Command line --------------------------------------------------------------------------------


@pytest.fixture
def loaded_cli(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []

    def load_qqq(root: Path) -> Any:
        calls.append(f"qqq {root}")
        return QQQ, GATE, LOADED

    def load_ndx(root: Path) -> Any:
        calls.append(f"ndx {root}")
        return LEVELS, SOURCE, WINDOW

    monkeypatch.setattr(xc, "load_data", load_qqq)
    monkeypatch.setattr(en, "load_data", load_ndx)
    return calls


def test_run_writes_both_files_and_prints_one_line(
    loaded_cli: list[str], table: dict[str, Any], tmp_path: Path
) -> None:
    out = tmp_path / "out"
    result = CliRunner().invoke(xd.app, ["run", "--out", str(out), "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert loaded_cli == [f"qqq {tmp_path}", f"ndx {tmp_path}"]
    text = out.joinpath("exit_rules_declines.json").read_text(encoding="utf-8")
    assert text == json.dumps(table, indent=2, ensure_ascii=False) + "\n"
    assert out.joinpath("exit_rules_declines.md").read_text(encoding="utf-8") == (
        xd.render_markdown(table)
    )
    assert sorted(p.name for p in out.iterdir()) == [
        "exit_rules_declines.json",
        "exit_rules_declines.md",
    ]
    assert result.output == xd.summary_line(table, out) + "\n"


@pytest.mark.parametrize("name", ["exit_rules_declines.json", "exit_rules_declines.md"])
def test_run_refuses_an_existing_result_without_loading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str
) -> None:
    def boom(root: Path) -> Any:
        raise AssertionError("loaded")

    monkeypatch.setattr(xc, "load_data", boom)
    monkeypatch.setattr(en, "load_data", boom)
    tmp_path.joinpath(name).write_text("{}", encoding="utf-8")
    result = CliRunner().invoke(xd.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "exit_rules_declines: 已有结果，拒绝覆盖。\n"
    assert [p.name for p in tmp_path.iterdir()] == [name]


@pytest.mark.parametrize(
    "failing", [("qqq", tr.TrialError("snapshot is not checked\nmore")), ("ndx", OSError("gone"))]
)
def test_a_load_failure_writes_nothing(
    loaded_cli: list[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failing: tuple[str, Exception],
) -> None:
    which, error = failing

    def fail(root: Path) -> Any:
        raise error

    monkeypatch.setattr(xc if which == "qqq" else en, "load_data", fail)
    out = tmp_path / "out"
    result = CliRunner().invoke(xd.app, ["run", "--out", str(out)])
    assert result.exit_code == 1
    first = str(error).splitlines()[0]
    assert result.output == f"exit_rules_declines: 数据载入失败：{first}\n"
    assert not out.exists()


def test_a_failed_check_writes_only_the_json(loaded_cli: list[str], tmp_path: Path) -> None:
    result = CliRunner().invoke(xd.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "exit_rules_declines: 输入检查 FAIL，没有划大跌\n"
    assert [p.name for p in tmp_path.iterdir()] == ["exit_rules_declines.json"]
    written = json.loads(tmp_path.joinpath("exit_rules_declines.json").read_text(encoding="utf-8"))
    assert written["complete"] is False
    assert "spans" not in written
