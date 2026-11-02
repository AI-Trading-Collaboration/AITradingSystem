"""Acceptance tests for TRADING-2565-108: seven QQQ exit rules, counts only.

Requirement: knowledge qqq-options/auto-loop/period7/exit-family/exit-family-step0-counts-
requirements-2026-11-02.md (owner decision exit_family_step0). Synthetic data only; no snapshot,
no network.
"""

from __future__ import annotations

import ast
import json
import math
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from statistics import stdev
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
from ai_trading_system.research_probes import exit_rules_counts as xr
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import policy_table as pt

LOADED = load_settings()
ROOT_DIR = lg.research_root(LOADED)
GATE = tr.load_gate(PROJECT_ROOT / "research" / "_trials" / "prereg.yaml")
_, PLAN = tr.load_trial(ROOT_DIR, pt.DATA_TRIAL)
SIGNAL_SNAPSHOT = PLAN.data.signal_snapshot_id
QQQ_SNAPSHOT = PLAN.data.snapshot_id
MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "exit_rules_counts.py"
GROWTH = 1.0005
DIP = 0.9


# Constants -----------------------------------------------------------------------------------


def test_the_run_is_step_zero_of_the_exit_family() -> None:
    assert xr.RUN_ID == "TRADING-2565-108"
    assert xr.OWNER_DECISION == "owner_decision:TRADING-2565:2026-11-02:exit_family_step0"
    assert xr.MARKET == "qqq"
    assert xr.MARKET_TEXT == "QQQ 1999–2010"
    assert xr.RULES == ("X0", "X1", "X2", "X3", "X4", "X5", "X6")


def test_the_rules_are_frozen() -> None:
    assert (xr.STOP, xr.VOL_DAYS, xr.SMA_DAYS, xr.WEEKS, xr.WEEK_SIGMA, xr.SWING) == (
        0.08,
        20,
        50,
        52,
        2.0,
        2,
    )
    assert (xr.TIME_CHECK, xr.SHORT_HOLD, xr.LONG_HOLD, xr.CLUSTER_GAP) == (20, 60, 120, 20)
    assert xr.CAP == {"X0": 60, "X1": 120, "X2": 120, "X3": 120, "X4": 120, "X5": 120, "X6": 60}
    assert xr.CHECK_JSON == "exit_rules_counts.json"
    assert xr.MIN_TRADES == 2000
    assert xr.COUNT_KEYS == (
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


def test_the_module_reads_only_qqq_through_the_bench() -> None:
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
        "statistics.",
        "collections.",
        "dataclasses.",
        "datetime.",
        "pathlib.",
        "typing.",
        "numpy",
        "pandas",
        "typer",
        "ai_trading_system.config.PROJECT_ROOT",
        "ai_trading_system.research_bench.ledger",
        "ai_trading_system.research_bench.load_settings",
        "ai_trading_system.research_bench.trial",
        "ai_trading_system.research_bench.data.",
        "ai_trading_system.research_bench.settings.",
        "ai_trading_system.research_data.qqq_long_history",
        "ai_trading_system.research_probes.entry_timing_counts",
        "ai_trading_system.research_probes.gate_power",
        "ai_trading_system.research_probes.policy_table",
    )
    for name in imported:
        assert name.startswith(allowed), name
    for required in ("entry_timing_counts", "gate_power", "policy_table"):
        assert f"ai_trading_system.research_probes.{required}" in imported
    banned = (
        "us_market",
        "French",
        "ma200_forward",
        "window_reads",
        "ledger.jsonl",
        ".unlink(",
        "urllib",
        "requests",
    )
    for word in banned:
        assert word not in text, word
    assert text.count(".write_text(") == 1


# Indicators ----------------------------------------------------------------------------------


def _days(n: int, start: str = "2001-01-01") -> tuple[date, ...]:
    return tuple(d.date() for d in pd.bdate_range(start, periods=n))


def test_the_fifty_day_average() -> None:
    close = [float(p) for p in range(1, 61)]
    sma = xr.moving_average(close)
    assert sma[48] is None
    assert sma[49] == pytest.approx(25.5)
    assert sma[59] == pytest.approx(35.5)
    assert xr.moving_average([1.0, 2.0, 3.0], 2) == (None, 1.5, 2.5)


def test_the_daily_sigma_uses_twenty_returns_up_to_the_day() -> None:
    rng = np.random.default_rng(7)
    close = list(100.0 * np.cumprod(1.0 + rng.normal(0.0, 0.01, 40)))
    sigma = xr.daily_sigma(close)
    returns = [close[p] / close[p - 1] - 1.0 for p in range(1, 40)]
    assert sigma[19] is None
    assert sigma[20] == pytest.approx(stdev(returns[0:20]))
    assert sigma[39] == pytest.approx(stdev(returns[19:39]))
    bumped = close[:25] + [close[25] * 1.2] + close[26:]
    changed = xr.daily_sigma(bumped)
    assert changed[24] == sigma[24]
    assert changed[25] != sigma[25]


def test_week_ends_follow_iso_weeks() -> None:
    days = (
        date(2004, 12, 29),
        date(2004, 12, 30),
        date(2004, 12, 31),  # Friday, ISO week 53 of 2004
        date(2005, 1, 3),
        date(2005, 1, 6),  # Thursday: the Friday is missing, so it ends its week
        date(2005, 1, 10),
        date(2005, 1, 11),
    )
    ends = xr.week_ends(days)
    assert ends == (False, False, True, False, True, False, False)
    assert xr.week_numbers(ends) == (0, 0, 0, 1, 1, 2, 2)


def test_the_last_day_is_never_a_week_end() -> None:
    days = _days(5, "2001-01-01")  # Monday to Friday
    assert xr.week_ends(days) == (False, False, False, False, False)


def _weekly_path(weekly: list[float]) -> tuple[tuple[date, ...], list[float]]:
    # One close per week (Fridays), one extra Monday so the last Friday is a week end.
    days = [d.date() for d in pd.date_range("2001-01-05", periods=len(weekly) + 1, freq="W-FRI")]
    days.append(date.fromordinal(days[-1].toordinal() + 3))
    close = [100.0]
    for r in weekly:
        close.append(close[-1] * (1.0 + r))
    close.append(close[-1])
    return tuple(days), close


def test_a_weekly_drop_is_at_least_two_sigmas_of_the_past_fifty_two_weeks() -> None:
    pattern = [0.01, -0.01] * 26
    sigma = stdev(pattern)
    weekly = [*pattern, -2.0 * sigma, *pattern[:51], -2.0 * sigma * 0.999]
    days, close = _weekly_path(weekly)
    drop = xr.weekly_drops(close, xr.week_ends(days))
    flagged = [p for p, d in enumerate(drop) if d]
    # Week 53 (the 53rd weekly return) is the first one that has 52 earlier returns.
    assert flagged == [53]
    assert len(close) == 107


def test_the_current_week_is_not_in_its_own_sigma() -> None:
    pattern = [0.01, -0.01] * 26
    days, close = _weekly_path([*pattern, -0.5])
    drop = xr.weekly_drops(close, xr.week_ends(days))
    assert drop[53] is True


def test_week_fifty_two_is_never_judged() -> None:
    days, close = _weekly_path([0.001] * 51 + [-0.5, 0.0])
    drop = xr.weekly_drops(close, xr.week_ends(days))
    assert not any(drop)


# Rules on prepared paths ---------------------------------------------------------------------


def _prepared(
    close: Sequence[float],
    *,
    sma: Sequence[float | None] | None = None,
    sigma: float = 0.01,
    week: Sequence[int] | None = None,
    drop: Sequence[bool] | None = None,
) -> xr.Prepared:
    n = len(close)
    return xr.Prepared(
        dates=_days(n),
        close=tuple(close),
        sma=tuple(sma) if sma is not None else (None,) * n,
        sigma=(sigma,) * n,
        week=tuple(week) if week is not None else tuple(p // 5 for p in range(n)),
        drop=tuple(drop) if drop is not None else (False,) * n,
    )


def test_x0_sells_the_close_after_a_close_eight_percent_under_entry() -> None:
    close = [100.0] * 200
    close[30] = 92.0
    assert xr.exit_for("X0", _prepared(close), 10, 1.0) == xr.Exit(30, 31, "signal")
    close[30] = 92.01
    assert xr.exit_for("X0", _prepared(close), 10, 1.0) == xr.Exit(None, 70, "time")


@pytest.mark.parametrize(("day", "expected"), [(69, xr.Exit(69, 70, "signal")), (70, None)])
def test_x0_judges_until_the_day_before_expiry(day: int, expected: xr.Exit | None) -> None:
    close = [100.0] * 200
    close[day] = 90.0
    result = xr.exit_for("X0", _prepared(close), 10, 1.0)
    assert result == (expected or xr.Exit(None, 70, "time"))


def test_x1_scales_the_stop_with_the_entry_sigma() -> None:
    close = [100.0] * 200
    close[40] = 95.0
    # distance = k x sigma = 2.5 x 0.02 = 5%: 95 is on the line.
    assert xr.exit_for("X1", _prepared(close, sigma=0.02), 10, 2.5) == xr.Exit(40, 41, "signal")
    assert xr.exit_for("X1", _prepared(close, sigma=0.0201), 10, 2.5).reason == "time"
    assert xr.exit_for("X1", _prepared(close, sigma=0.0201), 10, 2.5).sell == 130


def test_x1_uses_the_sigma_on_the_entry_day_only() -> None:
    close = [100.0] * 200
    close[40] = 95.0
    data = _prepared(close, sigma=0.02)
    later = data._replace(sigma=(0.02,) * 11 + (0.5,) * 189)
    assert xr.exit_for("X1", later, 10, 2.5) == xr.Exit(40, 41, "signal")


def test_x2_trails_the_highest_close_since_entry() -> None:
    close = [100.0] * 200
    close[20] = 120.0
    close[21:] = [114.0] * 179
    # 5% under 120 is 114: on the line.
    assert xr.exit_for("X2", _prepared(close, sigma=0.02), 10, 2.5) == xr.Exit(21, 22, "signal")
    close[21:] = [114.01] * 179
    assert xr.exit_for("X2", _prepared(close, sigma=0.02), 10, 2.5).reason == "time"


def test_x2_counts_the_entry_close_as_the_first_high() -> None:
    close = [100.0] * 200
    close[11] = 95.0
    assert xr.exit_for("X2", _prepared(close, sigma=0.02), 10, 2.5) == xr.Exit(11, 12, "signal")


def test_x3_needs_two_closes_under_the_fifty_day_average() -> None:
    close = [100.0] * 200
    sma: list[float | None] = [100.0] * 200
    close[30] = 99.0
    close[32] = 99.0
    close[33] = 99.0
    data = _prepared(close, sma=sma)
    assert xr.exit_for("X3", data, 10, 1.0) == xr.Exit(33, 34, "signal")
    equal = _prepared([100.0] * 200, sma=sma)
    assert xr.exit_for("X3", equal, 10, 1.0) == xr.Exit(None, 130, "time")


def test_x3_counts_the_entry_day_as_the_first_of_the_two() -> None:
    close = [100.0] * 200
    close[10] = 99.0
    close[11] = 99.0
    data = _prepared(close, sma=[100.0] * 200)
    assert xr.exit_for("X3", data, 10, 1.0) == xr.Exit(11, 12, "signal")


def test_x4_sells_after_a_weekly_drop_in_a_later_week() -> None:
    drop = [False] * 200
    week = [p // 5 for p in range(200)]
    drop[14] = True  # end of week 2, the entry's own week
    drop[24] = True  # end of week 4
    data = _prepared([100.0] * 200, week=week, drop=drop)
    assert xr.exit_for("X4", data, 12, 1.0) == xr.Exit(24, 25, "signal")
    assert xr.exit_for("X4", data, 15, 1.0) == xr.Exit(24, 25, "signal")
    assert xr.exit_for("X4", data, 25, 1.0) == xr.Exit(None, 145, "time")


def test_x6_sells_after_twenty_days_under_the_entry() -> None:
    close = [100.0] * 200
    close[30] = 99.99
    assert xr.exit_for("X6", _prepared(close), 10, 1.0) == xr.Exit(30, 31, "signal")
    close[30] = 100.0
    assert xr.exit_for("X6", _prepared(close), 10, 1.0) == xr.Exit(None, 70, "time")
    close[29] = 50.0
    assert xr.exit_for("X6", _prepared(close), 10, 1.0) == xr.Exit(None, 70, "time")


def test_an_unknown_rule_is_refused() -> None:
    with pytest.raises(ValueError, match="unknown rule"):
        xr.exit_for("X9", _prepared([100.0] * 200), 10, 1.0)


# X5, the second leg --------------------------------------------------------------------------


def _leg(points: dict[int, float], n: int = 200, base: float = 100.0) -> list[float]:
    close = [base] * n
    for p, value in points.items():
        close[p] = value
    return close


SMA = [100.0] * 200


def test_x5_sells_when_a_lower_high_is_followed_by_a_new_low() -> None:
    # entry 10 at 100 (high 105 on 12); break 20 (95), low 93 on 21, swing high 98 on 25
    # (confirmed on 27), close under 93 on 30.
    close = _leg({12: 105.0, 20: 95.0, 21: 93.0, 22: 94.0, 23: 96.0, 24: 97.0, 25: 98.0})
    close[26:30] = [96.0, 95.0, 94.0, 93.0]
    close[30] = 92.9
    close[31:] = [92.0] * 169
    data = _prepared(close, sma=SMA)
    assert xr.exit_for("X5", data, 10, 1.0) == xr.Exit(30, 31, "signal")


def test_x5_waits_for_the_swing_to_be_confirmed() -> None:
    # The low is broken on the swing's second confirming day: that day is the decision.
    close = _leg({12: 105.0, 20: 95.0, 21: 93.0, 22: 96.0, 23: 98.0, 24: 96.0, 25: 92.0})
    close[26:] = [90.0] * 174
    data = _prepared(close, sma=SMA)
    assert xr.exit_for("X5", data, 10, 1.0) == xr.Exit(25, 26, "signal")


def test_x5_a_slide_without_a_swing_never_sells() -> None:
    close = _leg({12: 105.0})
    close[20:] = [99.0 - 0.1 * k for k in range(180)]
    data = _prepared(close, sma=SMA)
    assert xr.exit_for("X5", data, 10, 1.0) == xr.Exit(None, 130, "time")


def test_x5_a_new_high_cancels_the_break_and_starts_over() -> None:
    close = _leg({12: 105.0, 20: 95.0, 21: 93.0, 22: 94.0, 23: 96.0, 24: 106.0})
    # After the new high (24): break 40, low 90 on 41, swing 96 on 44 (confirmed 46), 89 on 50.
    close[25:40] = [104.0] * 15
    close[40:50] = [95.0, 90.0, 92.0, 94.0, 96.0, 94.0, 93.0, 92.0, 91.0, 90.0]
    close[50] = 89.0
    close[51:] = [88.0] * 149
    data = _prepared(close, sma=SMA)
    assert xr.exit_for("X5", data, 10, 1.0) == xr.Exit(50, 51, "signal")


def test_x5_a_close_equal_to_the_high_also_cancels() -> None:
    close = _leg({12: 105.0, 20: 95.0, 21: 93.0, 22: 96.0, 23: 105.0, 24: 96.0, 25: 92.0})
    close[26:] = [100.0] * 174
    data = _prepared(close, sma=SMA)
    # 105 on 23 equals the high since entry: the break of 20 is void, and the break of 24 never
    # gets a swing high. Without the reset, 23 would be a swing and 25 a new low.
    assert xr.exit_for("X5", data, 10, 1.0) == xr.Exit(None, 130, "time")


def test_x5_the_new_high_day_is_not_a_break() -> None:
    # The high day itself sits under a rising average; the break must come after it.
    sma: list[float | None] = [100.0] * 200
    sma[24] = 110.0
    close = _leg({12: 105.0, 20: 95.0, 21: 93.0, 22: 94.0, 23: 96.0, 24: 106.0})
    close[25:] = [104.0] * 175
    data = _prepared(close, sma=sma)
    assert xr.exit_for("X5", data, 10, 1.0) == xr.Exit(None, 130, "time")


def test_x5_a_swing_high_needs_two_closes_before_it() -> None:
    # 98 on 21 is higher than the would-be swing 97 on 23: no swing, no sale.
    close = _leg({20: 95.0, 21: 98.0, 22: 96.0, 23: 97.0, 24: 96.5, 25: 96.0, 26: 94.9})
    close[27:] = [94.0] * 173
    data = _prepared(close, sma=SMA)
    assert xr.exit_for("X5", data, 10, 1.0) == xr.Exit(None, 130, "time")


def test_x5_the_break_day_is_never_the_swing_high() -> None:
    # Entry already under the average: the break is the next day, which would look like a peak
    # against the close before the entry. The swing must come after the break.
    close = _leg({9: 90.0, 10: 91.0, 11: 93.0, 12: 92.0, 13: 91.5})
    close[14:] = [91.0] * 186
    data = _prepared(close, sma=SMA)
    assert xr.exit_for("X5", data, 10, 1.0) == xr.Exit(None, 130, "time")


def test_x5_judges_until_the_day_before_expiry() -> None:
    close = _leg({12: 105.0, 120: 95.0, 121: 93.0, 122: 94.0, 123: 96.0, 124: 94.0, 125: 93.5})
    close[126] = 92.0
    data = _prepared(close, sma=SMA)
    assert xr.exit_for("X5", data, 10, 1.0) == xr.Exit(126, 127, "signal")
    late = list(close)
    late[126] = 93.5
    late[129] = 92.0
    assert xr.exit_for("X5", _prepared(late, sma=SMA), 10, 1.0) == xr.Exit(129, 130, "signal")
    later = list(close)
    later[126] = 93.5
    later[130] = 92.0
    assert xr.exit_for("X5", _prepared(later, sma=SMA), 10, 1.0) == xr.Exit(None, 130, "time")


# Entry set, scale and counts -----------------------------------------------------------------


def test_the_entry_range_starts_when_every_rule_is_defined() -> None:
    week = [0] * 100 + [52] * 300
    data = _prepared([100.0] * 400, week=week)
    entries = xr.entry_range(data, 0)
    assert (entries.start, entries.stop) == (100, 400 - 121)
    assert entries[-1] + 121 == 399
    early = _prepared([100.0] * 400, week=[52] * 400)
    assert xr.entry_range(early, 0).start == 49
    assert xr.entry_range(early, 60).start == 60


def test_the_scale_sets_the_mean_distance_to_eight_percent() -> None:
    data = _prepared([100.0] * 10)._replace(sigma=(None, 0.01, 0.02, 0.03, 0.5, 0.5) + (0.9,) * 4)
    k = xr.scale(data, range(1, 4))
    assert k == pytest.approx(0.08 / 0.02)
    assert xr.scale(data, range(0, 0)) is None


def test_nearest_rank_percentiles() -> None:
    values = list(range(10, 0, -1))
    assert (xr.nearest_rank(values, 0.1), xr.nearest_rank(values, 0.5)) == (1, 5)
    assert xr.nearest_rank(values, 0.9) == 9
    assert xr.nearest_rank([7], 0.1) == 7
    assert xr.nearest_rank([5, 4, 3, 2, 1], 0.5) == 3
    assert xr.nearest_rank(list(range(1, 8)), 0.9) == 7


def test_decision_days_form_clusters_twenty_sessions_apart() -> None:
    assert xr.clusters([5, 10, 10, 30, 49, 70]) == 3
    assert xr.clusters([]) == 0
    assert xr.clusters([3]) == 1


def test_rule_counts() -> None:
    exits = {
        10: xr.Exit(11, 12, "signal"),
        11: xr.Exit(15, 16, "signal"),
        12: xr.Exit(None, 72, "time"),
        13: xr.Exit(16, 17, "signal"),
    }
    counts = xr.rule_counts(exits)
    assert tuple(counts) == xr.COUNT_KEYS
    assert counts == {
        "trades": 4,
        "signal": 3,
        "time": 1,
        "hold_mean": (2 + 5 + 60 + 4) / 4,
        "hold_median": 4,
        "hold_p10": 2,
        "hold_p90": 60,
        "decided_day1": 1,
        "decided_by_day5": 3,
        "hold_mean_same_close": (1 + 4 + 60 + 3) / 4,
        "h_match": 18,
        "decision_days": 3,
        "clusters": 1,
    }


def test_the_matched_hold_rounds_half_up() -> None:
    # Holds are sell - entry: 2 and 3 average 2.5, 4 and 5 average 4.5.
    exits = {0: xr.Exit(None, 2, "time"), 10: xr.Exit(None, 13, "time")}
    assert xr.rule_counts(exits)["h_match"] == 3
    exits = {0: xr.Exit(None, 4, "time"), 10: xr.Exit(None, 15, "time")}
    assert xr.rule_counts(exits)["h_match"] == 5
    exits = {0: xr.Exit(None, 4, "time"), 10: xr.Exit(None, 14, "time")}
    assert xr.rule_counts(exits)["h_match"] == 4


def test_no_trades_give_empty_counts() -> None:
    assert xr.rule_counts({}) == dict.fromkeys(xr.COUNT_KEYS)


# The QQQ ruler -------------------------------------------------------------------------------


def _pos(index: pd.DatetimeIndex, day: str) -> int:
    position = index.get_loc(pd.Timestamp(day))
    assert isinstance(position, int)
    return position


QQQ_DIPS = ["2002-06-03", "2006-09-05", "2010-12-01"]
CRASH = ("2008-09-15", "2008-10-31")


def _qqq_data(hole: str | None = None) -> BenchData:
    index = pd.bdate_range("1998-01-02", "2012-12-31", name="date")
    level = GROWTH ** np.arange(len(index), dtype=float)
    a, b = _pos(index, CRASH[0]), _pos(index, CRASH[1])
    level[a : b + 1] *= np.linspace(1.0, 0.6, b - a + 1)
    level[b + 1 :] *= 0.6
    for day in QQQ_DIPS:
        level[_pos(index, day)] *= DIP
    returns = np.empty(len(index))
    returns[0] = 0.0
    returns[1:] = level[1:] / level[:-1] - 1.0
    returns[index <= pd.Timestamp("1999-03-10")] = np.nan
    if hole is not None:
        returns[_pos(index, hole)] = np.nan
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
    data = from_daily_frame(daily, qqq_series="actual", source={"snapshot_id": QQQ_SNAPSHOT})
    return gp.cut(with_signal_sources(data, {}, {}, snapshot_id=SIGNAL_SNAPSHOT))


DATA = _qqq_data()


@pytest.fixture(scope="module")
def check() -> dict[str, Any]:
    return xr.check_inputs(DATA, LOADED, GATE)


def _keys(value: Any) -> set[str]:
    if isinstance(value, dict):
        return set(value) | {k for v in value.values() for k in _keys(v)}
    if isinstance(value, list):
        return {k for v in value for k in _keys(v)}
    return set()


def test_the_check_holds_only_counts(check: dict[str, Any]) -> None:
    keys = _keys(check)
    assert keys == (
        {
            "run_id",
            "owner_decision",
            "market",
            "window",
            "window_id",
            "start",
            "end",
            "first_close_date",
            "missing_days",
            "sessions",
            "rule",
            "stop",
            "vol_days",
            "sma_days",
            "weeks",
            "week_sigma",
            "swing",
            "time_check",
            "short_hold",
            "long_hold",
            "cluster_gap",
            "sell",
            "entry_set_a",
            "first",
            "last",
            "trades",
            "scale",
            "k",
            "distance_mean",
            "distance_min",
            "distance_max",
            "exits",
            "entry_set_b",
            "entry",
            "in_range",
            "decision",
            "hold",
            "reason",
            "ok",
        }
        | set(xr.RULES)
        | set(xr.COUNT_KEYS)
    )
    for word in ("return", "gain", "loss", "drawdown", "rise", "up", "pnl"):
        assert not any(word in key for key in keys - {"run_id"} if word != "up"), word
    assert not any(key.startswith("up") or key.endswith("_up") for key in keys)
    assert check["run_id"] == xr.RUN_ID
    assert check["owner_decision"] == xr.OWNER_DECISION
    assert check["market"] == "qqq"
    assert check["rule"] == {
        "stop": 0.08,
        "vol_days": 20,
        "sma_days": 50,
        "weeks": 52,
        "week_sigma": 2.0,
        "swing": 2,
        "time_check": 20,
        "short_hold": 60,
        "long_hold": 120,
        "cluster_gap": 20,
        "sell": "next_close",
    }
    assert check["ok"] is True


def test_the_window_is_the_qqq_development_window(check: dict[str, Any]) -> None:
    window = pt.market_window(LOADED, GATE, "qqq")
    assert check["window"] == {
        "window_id": window.window_id,
        "start": window.start.isoformat(),
        "end": window.end.isoformat(),  # type: ignore[union-attr]
    }
    assert check["window"]["end"] == "2010-12-31"
    index = pd.bdate_range("1999-03-11", "2010-12-31")
    assert check["first_close_date"] == "1999-03-11"
    assert check["sessions"] == len(index)
    assert check["missing_days"] == 0


def _iso_week_ends(index: pd.DatetimeIndex) -> list[int]:
    keys = [tuple(d.isocalendar())[:2] for d in index]
    return [p for p in range(len(index) - 1) if keys[p] != keys[p + 1]]


def test_entry_set_a_runs_from_week_fifty_two_to_the_last_full_horizon(
    check: dict[str, Any],
) -> None:
    index = pd.bdate_range("1999-03-11", "2010-12-31")
    ends = _iso_week_ends(index)
    first = ends[51] + 1  # the 52 earlier week ends make it week 52
    last = len(index) - 1 - 121
    assert check["entry_set_a"] == {
        "first": index[first].date().isoformat(),
        "last": index[last].date().isoformat(),
        "trades": last - first + 1,
    }
    assert check["entry_set_a"]["trades"] >= 2000


def test_the_counts_match_the_rule_functions(check: dict[str, Any]) -> None:
    series = ec.closes(DATA.returns["qqq"], date(2010, 12, 31))
    data = xr.prepare(series.dates, series.close)
    entries = xr.entry_range(data, 0)
    k = xr.scale(data, entries)
    assert k is not None
    assert check["scale"]["k"] == pytest.approx(k)
    assert check["scale"]["distance_mean"] == pytest.approx(0.08)
    sigmas = [data.sigma[e] for e in entries]
    assert check["scale"]["distance_min"] == pytest.approx(k * min(s for s in sigmas if s))
    assert check["scale"]["distance_max"] == pytest.approx(k * max(s for s in sigmas if s))
    for rule in xr.RULES:
        expected = xr.rule_counts({e: xr.exit_for(rule, data, e, k) for e in entries})
        assert check["exits"][rule] == pytest.approx(expected), rule


def test_the_crash_makes_every_rule_except_the_time_stop_sell(check: dict[str, Any]) -> None:
    for rule in ("X0", "X1", "X2", "X3", "X4"):
        assert check["exits"][rule]["signal"] > 0, rule
    assert check["exits"]["X6"]["signal"] > 0
    assert check["exits"]["X0"]["time"] > 0


def test_entry_set_b_takes_the_counted_signals_of_the_close_rule(check: dict[str, Any]) -> None:
    index = pd.bdate_range("1999-03-11", "2010-12-31")
    entries = [index[_pos(index, d) + 3].date().isoformat() for d in QQQ_DIPS]
    rows = check["entry_set_b"]
    assert [row["entry"] for row in rows] == entries
    assert [row["in_range"] for row in rows] == [True, True, False]
    assert rows[2]["exits"] is None
    series = ec.closes(DATA.returns["qqq"], date(2010, 12, 31))
    data = xr.prepare(series.dates, series.close)
    k = xr.scale(data, xr.entry_range(data, 0))
    assert k is not None
    e = _pos(index, QQQ_DIPS[0]) + 3
    for rule in xr.RULES:
        x = xr.exit_for(rule, data, e, k)
        assert rows[0]["exits"][rule] == {
            "decision": index[x.decision].date().isoformat() if x.decision is not None else None,
            "sell": index[x.sell].date().isoformat(),
            "hold": x.sell - e,
            "reason": x.reason,
        }


def test_a_missing_day_fails_the_check() -> None:
    check = xr.check_inputs(_qqq_data(hole="2005-06-01"), LOADED, GATE)
    assert check["missing_days"] == 1
    assert check["ok"] is False


def test_fewer_than_two_thousand_entries_fail_the_check(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(xr, "MIN_TRADES", 10_000)
    assert xr.check_inputs(DATA, LOADED, GATE)["ok"] is False


# Command line --------------------------------------------------------------------------------


@pytest.fixture
def loaded_cli(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    roots: list[Path] = []

    def load_data(root: Path) -> Any:
        roots.append(root)
        return DATA, GATE, LOADED

    monkeypatch.setattr(xr, "load_data", load_data)
    return roots


def test_check_writes_the_counts_and_prints_one_line(
    loaded_cli: list[Path], check: dict[str, Any], tmp_path: Path
) -> None:
    out = tmp_path / "out"
    result = CliRunner().invoke(xr.app, ["check", "--out", str(out), "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert loaded_cli == [tmp_path]
    text = out.joinpath("exit_rules_counts.json").read_text(encoding="utf-8")
    assert text.endswith("}\n") and not text.endswith("\n\n")
    assert json.loads(text) == check
    assert text == json.dumps(check, indent=2, ensure_ascii=False) + "\n"
    assert [p.name for p in out.iterdir()] == ["exit_rules_counts.json"]
    a = check["entry_set_a"]
    x = check["exits"]
    holds = "、".join(f"{r} {x[r]['hold_mean']:.1f}" for r in xr.RULES)
    groups = "、".join(f"{r} {x[r]['clusters']}" for r in xr.RULES)
    assert result.output == (
        f"exit_rules_counts: QQQ 1999–2010 入场 {a['trades']} 笔（{a['first']}..{a['last']}）；"
        f"平均持有 {holds} 天；判断日簇 {groups}；输入检查 PASS\n"
    )
    assert xr.summary_line(check) + "\n" == result.output


def test_check_refuses_an_existing_result_without_loading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def load_data(root: Path) -> Any:
        raise AssertionError("loaded")

    monkeypatch.setattr(xr, "load_data", load_data)
    tmp_path.joinpath("exit_rules_counts.json").write_text("{}", encoding="utf-8")
    result = CliRunner().invoke(xr.app, ["check", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "exit_rules_counts: 已有结果，拒绝覆盖。\n"
    assert tmp_path.joinpath("exit_rules_counts.json").read_text(encoding="utf-8") == "{}"


def test_a_load_failure_writes_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def load_data(root: Path) -> Any:
        raise tr.TrialError("snapshot is not checked\nsecond line")

    monkeypatch.setattr(xr, "load_data", load_data)
    out = tmp_path / "out"
    result = CliRunner().invoke(xr.app, ["check", "--out", str(out)])
    assert result.exit_code == 1
    assert result.output == "exit_rules_counts: 数据载入失败：snapshot is not checked\n"
    assert not out.exists()


def test_a_failed_check_still_writes_the_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    holed = _qqq_data(hole="2005-06-01")
    monkeypatch.setattr(xr, "load_data", lambda root: (holed, GATE, LOADED))
    result = CliRunner().invoke(xr.app, ["check", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output.endswith("；输入检查 FAIL\n")
    written = json.loads(tmp_path.joinpath("exit_rules_counts.json").read_text(encoding="utf-8"))
    assert written["ok"] is False
    assert written["missing_days"] == 1


def test_the_load_reads_only_the_qqq_ruler(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[tuple[str, Path]] = []

    def market_data(market: str, gate: Any, plan: Any, **kwargs: Any) -> BenchData:
        calls.append((market, kwargs["snapshot_root"]))
        return DATA

    monkeypatch.setattr(tr, "market_data", market_data)
    monkeypatch.setattr(tr, "_data_problems", lambda market, gate, plan, data: [])
    data, gate, loaded = xr.load_data(tmp_path)
    assert calls == [("qqq", tmp_path)]
    assert data.returns.index.max() <= pd.Timestamp("2010-12-31")
    assert gate.markets["qqq"].qqq_series is not None
    assert loaded.sha256 == LOADED.sha256


def test_the_load_refuses_data_problems(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(tr, "market_data", lambda market, gate, plan, **kwargs: DATA)
    monkeypatch.setattr(tr, "_data_problems", lambda market, gate, plan, data: ["bad", "worse"])
    with pytest.raises(ValueError, match=r"^qqq: bad; worse$"):
        xr.load_data(tmp_path)


def test_no_nan_reaches_the_file(check: dict[str, Any]) -> None:
    def walk(value: Any) -> None:
        if isinstance(value, float):
            assert math.isfinite(value)
        elif isinstance(value, dict):
            for v in value.values():
                walk(v)
        elif isinstance(value, list):
            for v in value:
                walk(v)

    walk(check)
