"""Acceptance tests for TRADING-2565-104: the 200-day pullback rule on closes, counts only.

Requirement: knowledge qqq-options/auto-loop/period7/entry-timing-step0-v0-counts-requirements-
2026-11-01.md (period-7 charter section 5.2, step 0, variant 0). Synthetic data only; no snapshot,
no network.
"""

from __future__ import annotations

import ast
import json
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
from ai_trading_system.research_bench.data import (
    BenchData,
    from_daily_frame,
    from_us_market,
    with_signal_sources,
)
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import policy_table as pt

LOADED = load_settings()
ROOT_DIR = lg.research_root(LOADED)
GATE = tr.load_gate(PROJECT_ROOT / "research" / "_trials" / "prereg.yaml")
_, PLAN = tr.load_trial(ROOT_DIR, pt.DATA_TRIAL)
SIGNAL_SNAPSHOT = PLAN.data.signal_snapshot_id
QQQ_SNAPSHOT = PLAN.data.snapshot_id
MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "entry_timing_counts.py"
GROWTH = 1.0005
DIP = 0.9


# Constants -----------------------------------------------------------------------------------


def test_the_run_is_step_zero_under_the_p7_decision() -> None:
    assert ec.RUN_ID == "TRADING-2565-104"
    assert ec.OWNER_DECISION == "owner_decision:TRADING-2565:2026-11-01:research_loop_p7"
    assert ec.VARIANT == "v0_close"
    assert ec.MARKETS == ("qqq", "us_market")
    assert ec.MARKET_TEXT == {"us_market": "美股大盘 1929–2010", "qqq": "QQQ 1999–2010"}


def test_the_rule_is_the_frozen_one() -> None:
    assert (ec.SMA_DAYS, ec.SLOPE_LAG, ec.CONFIRM_DAYS, ec.COOLDOWN, ec.HOLD_DAYS) == (
        200,
        20,
        3,
        20,
        60,
    )
    assert ec.FULL_RANGE == {"qqq": "1999_2010", "us_market": "1929_2010"}
    assert ec.ERAS == (
        ("1929_1961", date(1929, 1, 1), date(1961, 12, 31)),
        ("1962_1989", date(1962, 1, 1), date(1989, 12, 31)),
        ("1990_2010", date(1990, 1, 1), date(2010, 12, 31)),
    )
    assert ec.ORIGINAL_ENTRIES == (
        "2004-10-21",
        "2007-08-20",
        "2010-05-10",
        "2010-06-15",
        "2010-08-02",
        "2010-09-09",
    )
    assert ec.CHECK_JSON == "entry_timing_counts.json"


def test_the_module_reads_only_through_the_bench() -> None:
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
        "dataclasses.",
        "datetime.",
        "pathlib.",
        "typing.",
        "numpy",
        "pandas",
        "typer",
        "ai_trading_system.research_bench.trial",
        "ai_trading_system.research_bench.data.",
        "ai_trading_system.research_bench.settings.",
        "ai_trading_system.research_data.qqq_long_history",
        "ai_trading_system.research_probes.policy_table",
    )
    for name in imported:
        assert name.startswith(allowed), name
    assert "ai_trading_system.research_probes.policy_table" in imported
    banned = ("ma200_forward", "window_reads", "ledger.jsonl", ".unlink(", "urllib", "requests")
    for word in banned:
        assert word not in text
    assert text.count(".write_text(") == 1


# Closes --------------------------------------------------------------------------------------


def test_closes_compound_from_the_first_finite_return() -> None:
    index = pd.bdate_range("2000-01-03", periods=6)
    returns = pd.Series([np.nan, np.nan, 0.1, -0.5, 1.0, 0.25], index=index)
    series = ec.closes(returns, date(2000, 1, 31))
    assert series.dates == tuple(d.date() for d in index[2:])
    assert series.close == pytest.approx((1.1, 0.55, 1.1, 1.375))
    assert series.missing == 0


def test_a_gap_after_the_first_return_is_missing_and_skipped() -> None:
    index = pd.bdate_range("2000-01-03", periods=5)
    returns = pd.Series([0.1, np.nan, 0.1, np.nan, 0.1], index=index)
    series = ec.closes(returns, date(2000, 1, 9))
    assert series.missing == 2
    assert series.dates == (index[0].date(), index[2].date(), index[4].date())
    assert series.close == pytest.approx((1.1, 1.21, 1.331))


def test_closes_stop_at_the_window_end() -> None:
    index = pd.bdate_range("2010-12-27", "2011-01-07")
    returns = pd.Series(0.01, index=index)
    returns.iloc[-1] = np.nan
    series = ec.closes(returns, date(2010, 12, 31))
    assert series.dates[-1] == date(2010, 12, 31)
    assert len(series.dates) == 5
    assert series.missing == 0


def test_the_average_is_defined_from_the_200th_close() -> None:
    close = [float(i) for i in range(1, 251)]
    averages = ec.moving_average(close)
    assert averages[198] is None
    assert averages[199] == pytest.approx(100.5)
    assert averages[249] == pytest.approx(150.5)


# Scanning ------------------------------------------------------------------------------------


def _path(days: int, dips: dict[int, float] | None = None) -> list[float]:
    """A steadily rising close, well above its 200-day average, with one-day dips below it."""

    close = [GROWTH**i for i in range(days)]
    for position, factor in (dips or {}).items():
        close[position] = GROWTH**position * factor
    return close


def _many(days: int, *positions: int) -> list[float]:
    return _path(days, dict.fromkeys(positions, DIP))


def test_a_rising_close_never_touches() -> None:
    assert ec.scan(_path(600)) == []


def test_a_one_day_dip_is_a_counted_signal_entered_three_closes_later() -> None:
    touches = ec.scan(_many(600, 300))
    assert touches == [ec.Touch(300, "signal", 303, True, True)]


def test_a_close_equal_to_the_average_touches_and_equal_closes_confirm() -> None:
    # Exact in binary: SMA[220] = (198 * 100 + 299 + 101) / 200 = 101 and SMA[200] = 100.
    close = [100.0] * 219 + [299.0, 101.0, 101.0, 101.0, 101.0] + [400.0] * 10
    averages = ec.moving_average(close)
    assert averages[220] == 101.0
    assert close[219] > averages[219]  # type: ignore[operator]
    assert ec.scan(close) == [ec.Touch(220, "signal", 223, True, True)]


def test_a_close_just_above_the_average_does_not_touch() -> None:
    close = [100.0] * 219 + [299.0, 101.01] + [400.0] * 10
    assert ec.scan(close) == []


def test_a_third_close_just_below_the_touch_days_average_fails() -> None:
    close = [100.0] * 219 + [299.0, 101.0, 101.0, 101.0, 100.99] + [400.0] * 10
    assert ec.scan(close) == [ec.Touch(220, "failed", None, True, False)]


@pytest.mark.parametrize("late", [1, 2, 3])
def test_any_close_back_under_the_average_fails(late: int) -> None:
    touches = ec.scan(_many(600, 300, 300 + late))
    assert touches[0] == ec.Touch(300, "failed", None, True, False)


def test_after_a_failure_the_search_resumes_four_days_later() -> None:
    # Dips at 300 and 301 fail the touch at 300. A dip at 303 after a close above the average at
    # 302 would be a touch, but the search resumes at 304 (the original rule resumed at t + 3).
    assert ec.scan(_many(600, 300, 301, 303)) == [ec.Touch(300, "failed", None, True, False)]
    assert ec.scan(_many(600, 300, 301, 304))[1] == ec.Touch(304, "signal", 307, True, True)


def test_after_an_entry_a_touch_needs_twenty_days() -> None:
    assert ec.scan(_many(600, 300, 322)) == [ec.Touch(300, "signal", 303, True, True)]
    assert ec.scan(_many(600, 300, 323))[1] == ec.Touch(323, "signal", 326, True, True)


def test_a_failure_takes_no_cooldown() -> None:
    touches = ec.scan(_many(600, 300, 301, 306))
    assert touches[1] == ec.Touch(306, "signal", 309, True, True)


def test_a_signal_without_a_defined_lag_is_not_counted_but_takes_the_cooldown() -> None:
    # SMA[t - 20] is first defined at t = 219.
    assert ec.scan(_many(600, 205)) == [ec.Touch(205, "signal", 208, False, False)]
    assert ec.scan(_many(600, 205, 227)) == [ec.Touch(205, "signal", 208, False, False)]
    assert ec.scan(_many(600, 205, 228))[1] == ec.Touch(228, "signal", 231, True, True)
    assert ec.scan(_many(600, 219)) == [ec.Touch(219, "signal", 222, True, True)]


def test_an_average_equal_to_twenty_days_ago_fails_the_filter() -> None:
    # The extra 200 at position 1 leaves SMA[200] = SMA[220] = 101 exactly, while SMA[201] = 100.
    close = [100.0, 300.0] + [100.0] * 217 + [299.0, 101.0, 101.0, 101.0, 101.0] + [400.0] * 10
    averages = ec.moving_average(close)
    assert (averages[200], averages[201], averages[220]) == (101.0, 100.0, 101.0)
    assert ec.scan(close) == [ec.Touch(220, "signal", 223, False, False)]


def test_a_falling_average_fails_the_filter() -> None:
    close = [1.0 / GROWTH**i for i in range(600)]
    close[399] = close[398] * 3.0
    close[401] = close[402] = close[403] = close[399]
    touches = ec.scan(close)
    assert touches[0].position == 400
    assert touches[0].outcome == "signal"
    assert not touches[0].filter and not touches[0].counted


def test_a_touch_whose_entry_is_past_the_end_is_pending() -> None:
    assert ec.scan(_many(600, 596)) == [ec.Touch(596, "signal", 599, True, True)]
    assert ec.scan(_many(600, 597)) == [ec.Touch(597, "pending", None, True, False)]
    assert ec.scan(_many(600, 599)) == [ec.Touch(599, "pending", None, True, False)]


def test_the_scan_starts_at_the_window_and_earlier_entries_take_no_cooldown() -> None:
    close = _many(600, 300, 310)
    assert ec.scan(close) == [ec.Touch(300, "signal", 303, True, True)]
    assert ec.scan(close, 305) == [ec.Touch(310, "signal", 313, True, True)]
    assert ec.scan(_many(600, 250), 120) == [ec.Touch(250, "signal", 253, True, True)]


# Holding -------------------------------------------------------------------------------------


def test_holding_starts_the_day_after_entry_and_lasts_sixty_days() -> None:
    mask = ec.held(400, [100])
    assert [p for p, h in enumerate(mask) if h] == list(range(101, 161))


def test_overlapping_holdings_count_each_day_once() -> None:
    assert sum(ec.held(400, [100, 125])) == 85


def test_holding_is_cut_at_the_last_close() -> None:
    mask = ec.held(400, [390])
    assert [p for p, h in enumerate(mask) if h] == list(range(391, 400))


# Ranges --------------------------------------------------------------------------------------


def _series(close: list[float], start: str) -> ec.Closes:
    index = pd.bdate_range(start, periods=len(close))
    return ec.Closes(tuple(d.date() for d in index), tuple(close), 0)


def test_a_range_counts_touches_by_their_day_and_held_days_by_the_day() -> None:
    close = _many(600, 300, 301, 330, 597)
    series = _series(close, "2000-01-03")
    touches = ec.scan(close)
    assert [t.outcome for t in touches] == ["failed", "signal", "pending"]
    entries = [t.entry for t in touches if t.counted and t.entry is not None]
    mask = ec.held(len(close), entries)
    whole = ec.count_range(series, touches, mask, "all", series.dates[0], series.dates[-1])
    assert whole == {
        "range": "all",
        "start": series.dates[0].isoformat(),
        "end": series.dates[-1].isoformat(),
        "sessions": 600,
        "touches": 3,
        "signals": 1,
        "failed": 1,
        "pending": 1,
        "counted": 1,
        "hold_days": 60,
        "exposure": 60 / 600,
        "open_at_end": 0,
    }
    cut = series.dates[350]
    early = ec.count_range(series, touches, mask, "early", series.dates[0], cut)
    late = ec.count_range(series, touches, mask, "late", series.dates[351], series.dates[-1])
    assert (early["touches"], early["counted"], early["hold_days"]) == (2, 1, 350 - 333)
    assert (late["touches"], late["counted"], late["hold_days"]) == (1, 0, 60 - 17)
    assert early["sessions"] + late["sessions"] == 600


@pytest.mark.parametrize(("dip", "hold_days", "open_at_end"), [(536, 60, 0), (537, 59, 1)])
def test_a_holding_that_ends_on_the_last_close_is_complete(
    dip: int, hold_days: int, open_at_end: int
) -> None:
    close = _many(600, dip)
    series = _series(close, "2000-01-03")
    touches = ec.scan(close)
    mask = ec.held(600, [dip + 3])
    row = ec.count_range(series, touches, mask, "all", series.dates[0], series.dates[-1])
    assert (row["counted"], row["hold_days"], row["open_at_end"]) == (1, hold_days, open_at_end)


def test_a_counted_signal_near_the_end_is_open_at_the_end() -> None:
    close = _many(600, 560)
    series = _series(close, "2000-01-03")
    touches = ec.scan(close)
    mask = ec.held(600, [563])
    row = ec.count_range(series, touches, mask, "all", series.dates[0], series.dates[-1])
    assert (row["counted"], row["hold_days"], row["open_at_end"]) == (1, 36, 1)


# Both rulers ---------------------------------------------------------------------------------


def _pos(index: pd.DatetimeIndex, day: str) -> int:
    position = index.get_loc(pd.Timestamp(day))
    assert isinstance(position, int)
    return position


def _closes_to_returns(index: pd.DatetimeIndex, dips: list[str]) -> np.ndarray:
    level = GROWTH ** np.arange(len(index), dtype=float)
    for day in dips:
        level[_pos(index, day)] *= DIP
    returns = np.empty(len(index))
    returns[0] = 0.0
    returns[1:] = level[1:] / level[:-1] - 1.0
    return returns


US_DIPS = [
    "1928-12-20",  # before the window: never counted, takes no cooldown
    "1940-06-03",
    "1961-12-20",  # counted in 1929_1961, held mostly in 1962
    "1975-03-03",
    "1975-04-07",  # 25 sessions later: holdings overlap
    "1999-05-03",
    "2003-02-03",
    "2003-02-05",  # fails the touch on 2003-02-03
    "2010-12-29",  # pending: its entry would be in 2011
]
QQQ_DIPS = ["2002-06-03", "2006-09-05", "2010-12-01"]


def _us_data(start: str = "1926-07-01") -> BenchData:
    index = pd.bdate_range(start, "2012-12-31", name="date")
    returns = _closes_to_returns(index, [d for d in US_DIPS if pd.Timestamp(d) >= index[0]])
    factors = pd.DataFrame({"Mkt-RF": returns, "SMB": 0.0, "HML": 0.0, "RF": 0.0}, index=index)
    data = from_us_market({"ff_factors_daily": factors}, snapshot_id=SIGNAL_SNAPSHOT)
    return gp.cut(with_signal_sources(data, {}, {}, snapshot_id=SIGNAL_SNAPSHOT))


def _holed(day: str) -> dict[str, BenchData]:
    data = DATA["us_market"]
    returns = data.returns.copy()
    returns.loc[pd.Timestamp(day), "qqq"] = np.nan
    return {
        **DATA,
        "us_market": BenchData(returns=returns, inputs=data.inputs, source=dict(data.source)),
    }


def _qqq_data() -> BenchData:
    index = pd.bdate_range("1998-01-02", "2012-12-31", name="date")
    returns = _closes_to_returns(index, QQQ_DIPS)
    returns[index <= pd.Timestamp("1999-03-10")] = np.nan
    level = 100.0 * GROWTH ** np.arange(len(index), dtype=float)
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


DATA = {"us_market": _us_data(), "qqq": _qqq_data()}


@pytest.fixture(scope="module")
def check() -> dict[str, Any]:
    return ec.check_inputs(DATA, LOADED, GATE)


def _keys(value: Any) -> set[str]:
    if isinstance(value, dict):
        return set(value) | {k for v in value.values() for k in _keys(v)}
    if isinstance(value, list):
        return {k for v in value for k in _keys(v)}
    return set()


RANGE_KEYS = {
    "range",
    "start",
    "end",
    "sessions",
    "touches",
    "signals",
    "failed",
    "pending",
    "counted",
    "hold_days",
    "exposure",
    "open_at_end",
}


def test_the_check_holds_only_counts(check: dict[str, Any]) -> None:
    assert (
        _keys(check)
        == {
            "run_id",
            "owner_decision",
            "variant",
            "rule",
            "sma_days",
            "slope_lag",
            "confirm_days",
            "cooldown",
            "hold_days",
            "markets",
            "qqq",
            "us_market",
            "window_id",
            "first_close_date",
            "first_scan_date",
            "warmed_at_start",
            "missing_days",
            "ranges",
            "entries",
            "original_rule_entries_2000_2010",
            "ok",
        }
        | RANGE_KEYS
    )
    assert set(check["markets"]["us_market"]) == {
        "window_id",
        "start",
        "end",
        "first_close_date",
        "first_scan_date",
        "warmed_at_start",
        "missing_days",
        "ranges",
    }
    assert check["run_id"] == ec.RUN_ID
    assert check["owner_decision"] == ec.OWNER_DECISION
    assert check["variant"] == "v0_close"
    assert check["rule"] == {
        "sma_days": 200,
        "slope_lag": 20,
        "confirm_days": 3,
        "cooldown": 20,
        "hold_days": 60,
    }
    assert check["ok"]


def test_the_windows_are_the_registered_development_windows(check: dict[str, Any]) -> None:
    for market in ec.MARKETS:
        window = pt.market_window(LOADED, GATE, market)
        row = check["markets"][market]
        assert row["window_id"] == window.window_id
        assert row["start"] == window.start.isoformat()
        assert row["end"] == window.end.isoformat()  # type: ignore[union-attr]
    assert check["markets"]["us_market"]["window_id"] == "us_market_1929_2010"
    assert check["markets"]["qqq"]["end"] == "2010-12-31"


def test_the_us_market_counts_and_eras(check: dict[str, Any]) -> None:
    us = check["markets"]["us_market"]
    assert us["first_close_date"] == "1926-07-01"
    assert us["first_scan_date"] == "1929-01-02"
    assert us["warmed_at_start"] is True
    assert us["missing_days"] == 0
    rows = {r["range"]: r for r in us["ranges"]}
    names = [r["range"] for r in us["ranges"]]
    assert names == ["1929_2010", "1929_1961", "1962_1989", "1990_2010"]
    whole = rows["1929_2010"]
    assert (whole["start"], whole["end"]) == ("1929-01-02", "2010-12-31")
    # The eras are cut to the window, so their sessions add up to the window's.
    assert (rows["1929_1961"]["start"], rows["1929_1961"]["end"]) == ("1929-01-02", "1961-12-31")
    assert (rows["1990_2010"]["start"], rows["1990_2010"]["end"]) == ("1990-01-01", "2010-12-31")
    assert {k: whole[k] for k in ("touches", "signals", "failed", "pending", "counted")} == {
        "touches": 7,
        "signals": 5,
        "failed": 1,
        "pending": 1,
        "counted": 5,
    }
    assert whole["hold_days"] == 4 * 60 + 25
    assert whole["open_at_end"] == 0
    index = pd.bdate_range("1929-01-02", "2010-12-31")
    assert whole["sessions"] == len(index)
    assert whole["exposure"] == whole["hold_days"] / whole["sessions"]
    assert [rows[n]["counted"] for n in ("1929_1961", "1962_1989", "1990_2010")] == [2, 2, 1]
    assert [rows[n]["touches"] for n in ("1929_1961", "1962_1989", "1990_2010")] == [2, 2, 3]
    entry = _pos(index, "1961-12-20") + 3
    held_1961 = int((index[entry + 1 : entry + 61] <= pd.Timestamp("1961-12-31")).sum())
    assert rows["1929_1961"]["hold_days"] == 60 + held_1961
    assert rows["1962_1989"]["hold_days"] == 60 - held_1961 + 85
    for key in ("sessions", "touches", "signals", "failed", "pending", "counted", "hold_days"):
        assert sum(rows[n][key] for n in ("1929_1961", "1962_1989", "1990_2010")) == whole[key]


def test_qqq_counts_from_its_first_close(check: dict[str, Any]) -> None:
    qqq = check["markets"]["qqq"]
    index = pd.bdate_range("1999-03-11", "2010-12-31")
    assert qqq["first_close_date"] == "1999-03-11"
    assert qqq["first_scan_date"] == index[200].date().isoformat()
    assert qqq["warmed_at_start"] is False
    assert qqq["missing_days"] == 0
    (row,) = qqq["ranges"]
    assert (row["range"], row["start"], row["end"]) == ("1999_2010", "1999-03-10", "2010-12-31")
    assert row["sessions"] == len(index)
    assert (row["touches"], row["signals"], row["counted"], row["open_at_end"]) == (3, 3, 3, 1)
    last = _pos(index, "2010-12-01") + 3
    assert row["hold_days"] == 120 + (len(index) - 1 - last)
    assert qqq["entries"] == [index[_pos(index, d) + 3].date().isoformat() for d in QQQ_DIPS]
    assert qqq["original_rule_entries_2000_2010"] == list(ec.ORIGINAL_ENTRIES)


def test_a_missing_day_fails_the_check() -> None:
    check = ec.check_inputs(_holed("1950-06-01"), LOADED, GATE)
    assert check["markets"]["us_market"]["missing_days"] == 1
    assert not check["ok"]


def test_a_us_market_average_undefined_at_the_window_start_fails_the_check() -> None:
    short = {**DATA, "us_market": _us_data(start="1928-04-02")}
    check = ec.check_inputs(short, LOADED, GATE)
    assert check["markets"]["us_market"]["warmed_at_start"] is False
    assert not check["ok"]


def test_the_us_market_needs_twenty_more_closes_than_the_average() -> None:
    # The window's first session needs SMA[t - 20]: 219 closes before it.
    index = pd.bdate_range("1928-01-02", "1929-01-02")
    enough = ec.check_inputs(
        {**DATA, "us_market": _us_data(start=str(index[-220].date()))}, LOADED, GATE
    )
    assert enough["markets"]["us_market"]["warmed_at_start"] is True
    short = ec.check_inputs(
        {**DATA, "us_market": _us_data(start=str(index[-219].date()))}, LOADED, GATE
    )
    assert short["markets"]["us_market"]["warmed_at_start"] is False


# Command line --------------------------------------------------------------------------------


@pytest.fixture
def loaded_cli(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    roots: list[Path] = []

    def load_data(root: Path) -> Any:
        roots.append(root)
        return DATA, GATE, LOADED, ROOT_DIR

    monkeypatch.setattr(ec, "load_data", load_data)
    return roots


def test_check_writes_the_counts_and_prints_one_line(
    loaded_cli: list[Path], check: dict[str, Any], tmp_path: Path
) -> None:
    out = tmp_path / "out"
    result = CliRunner().invoke(ec.app, ["check", "--out", str(out), "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert loaded_cli == [tmp_path]
    text = out.joinpath("entry_timing_counts.json").read_text(encoding="utf-8")
    assert text.endswith("}\n") and not text.endswith("\n\n")
    assert json.loads(text) == check
    assert text == json.dumps(check, indent=2, ensure_ascii=False) + "\n"
    assert [p.name for p in out.iterdir()] == ["entry_timing_counts.json"]
    q = check["markets"]["qqq"]["ranges"][0]
    u = {r["range"]: r for r in check["markets"]["us_market"]["ranges"]}
    w = u["1929_2010"]
    assert result.output == (
        f"entry_timing_counts: QQQ 1999–2010 触线 {q['touches']}、信号 {q['signals']}、"
        f"计入 {q['counted']}、持有 {q['hold_days']} 天；美股大盘 1929–2010 触线 {w['touches']}、"
        f"信号 {w['signals']}、计入 {w['counted']}、持有 {w['hold_days']} 天（1929–1961 计入 "
        f"{u['1929_1961']['counted']}，1962–1989 计入 {u['1962_1989']['counted']}，1990–2010 计入 "
        f"{u['1990_2010']['counted']}）；输入检查 PASS\n"
    )
    assert ec.summary_line(check) + "\n" == result.output


def test_check_refuses_an_existing_result_without_loading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def load_data(root: Path) -> Any:
        raise AssertionError("loaded")

    monkeypatch.setattr(ec, "load_data", load_data)
    tmp_path.joinpath("entry_timing_counts.json").write_text("{}", encoding="utf-8")
    result = CliRunner().invoke(ec.app, ["check", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "entry_timing_counts: 已有结果，拒绝覆盖。\n"
    assert tmp_path.joinpath("entry_timing_counts.json").read_text(encoding="utf-8") == "{}"


def test_a_load_failure_writes_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def load_data(root: Path) -> Any:
        raise ValueError("snapshot is not checked\nsecond line")

    monkeypatch.setattr(ec, "load_data", load_data)
    out = tmp_path / "out"
    result = CliRunner().invoke(ec.app, ["check", "--out", str(out)])
    assert result.exit_code == 1
    assert result.output == "entry_timing_counts: 数据载入失败：snapshot is not checked\n"
    assert not out.exists()


def test_a_failed_check_still_writes_the_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    holed = _holed("1950-06-01")
    monkeypatch.setattr(ec, "load_data", lambda root: (holed, GATE, LOADED, ROOT_DIR))
    result = CliRunner().invoke(ec.app, ["check", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output.endswith("；输入检查 FAIL\n")
    written = json.loads(tmp_path.joinpath("entry_timing_counts.json").read_text(encoding="utf-8"))
    assert written["ok"] is False
    assert written["markets"]["us_market"]["missing_days"] == 1


def test_the_load_reads_the_bench_data_like_the_leverage_table(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: list[Path] = []
    sentinel: Any = object()

    def load(root: Path) -> Any:
        calls.append(root)
        return sentinel

    monkeypatch.setattr(pt, "load_data", load)
    assert ec.load_data(tmp_path) is sentinel
    assert calls == [tmp_path]
