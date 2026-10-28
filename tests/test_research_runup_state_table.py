"""Acceptance tests for the industry run-up state table (TRADING-2565-84).

owner_decision:TRADING-2565:2026-10-29:print_only_tables, charter p6 sec. 23, the first of four
print-only tables.
F0001's fast-leg exits on the 49 Ken French industries, 1929-2010, are split by the industry's
run-up state (Greenwood, Shleifer and You 2019: two-year raw and net-of-market returns of at least
100%, five-year raw return of at least 50%), after a fidelity check against their crash
probability. Not a probe, not a trial, no ledger, no new rule simulated, not a long-bull cost
reading.

Synthetic data only: no snapshot is read. The fake signal snapshot stands in for sig-7ad8aa00b736;
the QQQ-Q3 preregistration and the bench's signals are the repository's own files. cycle_power and
longbull_tech_read are imported, not changed.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.research_bench.checks import strategy_targets
from ai_trading_system.research_bench.prereg import load_prereg
from ai_trading_system.research_bench.signal_strategy import signal_exposure
from ai_trading_system.research_bench.sources import SourceSpec
from ai_trading_system.research_data import signal_sources
from ai_trading_system.research_data.signal_sources import SignalSnapshot
from ai_trading_system.research_probes import cycle_power as cp
from ai_trading_system.research_probes import longbull_tech_read as lt
from ai_trading_system.research_probes import runup_state_table as rs

SOURCE = Path(rs.__file__).read_text(encoding="utf-8")
PREREG = Path(__file__).resolve().parents[1] / "research" / "QQQ-Q3" / "prereg.yaml"
UNCHANGED = {
    cp: "53e5286fec515c99dcb264f4ce8959afb4c4c54d970ab24b2df5d4c3843efafc",
    lt: "ad6f1e408de9d71fedc74097746347c8570bc745de0c0ab0d357ef36eb53176f",
}


def P(text: str, freq: str = "M") -> Any:
    """A month (or other) period, typed loosely so it can index the frames below."""

    return pd.Period(text, freq=freq)


def _pos(index: pd.DatetimeIndex, day: pd.Timestamp) -> int:
    at = index.get_loc(day)
    assert isinstance(at, int)
    return at


def _day(industry: rs.Industry, month: str) -> pd.Timestamp:
    return pd.Timestamp(str(industry.months.at[P(month), "date"]))


NAMES = ("Hardw", "Chips", "Softw", "Food", "Steel", "Coal", "Gold")
# Planted run-ups by industry: (first month of 24 strong months, crash after them or not).
RUNUPS = {
    "Hardw": (("1926-08", True), ("1958-01", False), ("1997-01", True)),
    "Chips": (("1949-01", True), ("1976-01", False), ("1997-06", True)),
    "Softw": (("1979-01", True), ("1997-03", True)),
    "Food": (("1934-01", True), ("1985-01", False), ("2008-06", False)),
    "Steel": (("1953-01", True), ("1971-01", True), ("2003-01", False)),
    "Coal": (("1972-06", True), ("2004-01", True)),
    "Gold": (("1932-01", True),),
}
SOFTW_FROM = pd.Timestamp("1965-07-01")
# Missing days inside a history: up to five are dropped, more leave the industry out.
GAPS = {"Hardw": ("1950-05-10",), "Softw": ("1971-03-11", "1983-06-15")}
GOLD_GAP = pd.bdate_range("1960-03-01", periods=6)


def _frames(seed: int = 11) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Ken French shaped decimal returns from 1926-07 past 2010, with planted run-ups and gaps."""

    rng = np.random.default_rng(seed)
    days = pd.bdate_range("1926-07-01", "2012-06-29")
    months = days.to_period("M")
    common = 0.0003 + rng.normal(0.0, 0.009, len(days))
    factors = pd.DataFrame({"Mkt-RF": common - 0.0001, "SMB": 0.0, "RF": 0.0001}, index=days)
    industries = {}
    for name in NAMES:
        drift = np.zeros(len(days))
        for first, crash in RUNUPS[name]:
            start = P(first, "M")
            drift[(months >= start) & (months < start + 24)] = 0.0035
            if crash:
                drift[(months >= start + 28) & (months < start + 38)] = -0.0045
        industries[name] = common + drift + rng.normal(0.0, 0.007, len(days))
    frame = pd.DataFrame(industries, index=days)
    frame.loc[frame.index < SOFTW_FROM, "Softw"] = np.nan
    for name, gaps in GAPS.items():
        frame.loc[pd.DatetimeIndex(gaps), name] = np.nan
    frame.loc[GOLD_GAP, "Gold"] = np.nan
    return factors, frame


SPECS = (
    SourceSpec(
        source_id="ff_factors_daily",
        provider="ken_french",
        dataset="F-F_Research_Data_Factors_daily",
        frequency="daily",
        known_lag_trading_days=0,
        description="factors",
    ),
    SourceSpec(
        source_id="ff_49_industries_daily",
        provider="ken_french",
        dataset="49_Industry_Portfolios_daily",
        frequency="daily",
        known_lag_trading_days=0,
        description="industries",
    ),
)


def _snapshot(factors: pd.DataFrame, industries: pd.DataFrame) -> SignalSnapshot:
    return SignalSnapshot(cp.SNAPSHOT_ID, {cp.FACTORS: factors, cp.INDUSTRIES: industries}, SPECS)


def _inputs(factors: pd.DataFrame, industries: pd.DataFrame) -> rs.Inputs:
    snapshot = _snapshot(factors, industries)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(signal_sources, "load_snapshot", lambda root, snapshot_id: snapshot)
        return rs.load_inputs()


FACTORS, INDUSTRIES = _frames()
BASE = _inputs(FACTORS, INDUSTRIES)
BUILT = rs.build(BASE)
RESULT, EXITS = rs.read(BASE)
ENTRIES = {name: load_prereg(PREREG).strategy(rule) for name, rule in rs.RULES.items()}


def _months_frame(
    days: pd.DatetimeIndex,
    fast: dict[str, float],
    *,
    state: dict[str, str] | None = None,
    nan_before: str | None = None,
) -> pd.DataFrame:
    """Month-end rows like month_targets plus state: fast 1 unless listed, slow 1."""

    periods = days.to_period("M")
    last = pd.Series(np.arange(len(days)), index=days).groupby(periods).tail(1)
    index = pd.DatetimeIndex(last.index).to_period("M")
    fast_values = np.array([fast.get(str(p), 1.0) for p in index])
    frame = pd.DataFrame(
        {
            "date": pd.DatetimeIndex(last.index),
            "pos": last.to_numpy(),
            "f0001": (fast_values + 1.0) / 2.0,
            "fast": fast_values,
            "slow": 1.0,
        },
        index=index,
    )
    if nan_before is not None:
        frame.loc[frame.index < P(nan_before, "M"), ["f0001", "fast", "slow"]] = np.nan
    states = state or {}
    frame["runup"] = False
    frame["state"] = [states.get(str(p), "N") for p in index]
    return frame


def _industry(
    returns: pd.Series,
    fast: dict[str, float],
    *,
    state: dict[str, str] | None = None,
    starts: Sequence[str] = (),
    cash: float = 0.0,
) -> rs.Industry:
    days = pd.DatetimeIndex(returns.index)
    months = _months_frame(days, fast, state=state)
    flags = pd.DataFrame(
        {"defined": True, "runup": False, "waived": False},
        index=months.index,
    )
    return rs.Industry(
        "Test",
        returns,
        pd.Series(cash, index=days),
        months,
        flags,
        [P(s, "M") for s in starts],
    )


# ----- constants: charter p6 sec. 23 and the requirement's sections 3 and 4, fixed before the run
def test_the_design_is_the_one_fixed_before_the_run() -> None:
    assert rs.RUN_ID == "TRADING-2565-84"
    assert rs.OWNER_DECISION == "owner_decision:TRADING-2565:2026-10-29:print_only_tables"
    assert rs.SNAPSHOT_ID == cp.SNAPSHOT_ID == "sig-7ad8aa00b736"
    assert (rs.INDUSTRIES, rs.FACTORS) == ("ff_49_industries_daily", "ff_factors_daily")
    assert (rs.MARKET, rs.CASH) == ("Mkt-RF", "RF")
    assert rs.LAST_DAY.isoformat() == "2010-12-31"
    assert (rs.FIRST_MONTH, rs.LAST_MONTH, rs.EPISODE_LAST) == (
        P("1929-01", "M"),
        P("2010-12", "M"),
        P("2008-12", "M"),
    )
    assert rs.QUESTION == "QQQ-Q3"
    assert rs.RULES == {"f0001": "fast_slow", "fast": "fast_only", "slow": "slow_only"}
    assert rs.LEGS == ("fast", "slow")
    assert (rs.MAX_MISSING_DAYS, rs.MAX_EXCLUDED) == (5, 10)
    assert (rs.RUNUP_MONTHS, rs.RUNUP_RAW, rs.RUNUP_NET) == (24, 1.0, 1.0)
    assert (rs.LONG_MONTHS, rs.LONG_RAW, rs.STATE_MONTHS) == (60, 0.5, 24)
    assert (rs.FOLLOW_DAYS, rs.FOLLOW_DROP) == (252, -0.20)
    assert (rs.CRASH_MONTHS, rs.CRASH_DROP, rs.LEG_SHARE) == (24, -0.40, 0.5)
    assert rs.GSY == {
        "episodes": 40,
        "crashes": 21,
        "share": 0.53,
        "unconditional": 0.14,
        "after_1970": 0.11,
    }
    assert (rs.FIDELITY_MIN, rs.FIDELITY_LOW, rs.FIDELITY_HIGH) == (15, 0.30, 0.76)
    assert (rs.MIN_R_EXITS, rs.GAP_LINE) == (60, 0.15)
    assert rs.HALVES == (("1929–1969", 1929, 1969), ("1970–2010", 1970, 2010))
    assert (rs.HALF_MIN_R, rs.DROP_YEAR_LINE, rs.EPSILON) == (20, 0.10, 1e-9)
    assert rs.TECH == ("Hardw", "Chips", "Softw")
    assert rs.LEFT_OUT == ("Other",)
    assert (rs.VOL_DAYS, rs.ANNUAL, rs.BACK_DAYS) == (63, 252, 63)
    assert (rs.MOSTLY_VOL_SHARE, rs.CONCENTRATION_LINE) == (0.5, 0.10)
    assert rs.STATES == ("R", "N", "undetermined")
    assert rs.VERDICTS == ("incomplete", "fidelity_miss", "too_few", "holds", "not_holds")
    assert (rs.CHECK, rs.EXITS, rs.SUMMARY, rs.REPORT) == (
        "runup_check.json",
        "runup_exits.jsonl",
        "runup_table.json",
        "runup_table.md",
    )


def test_imported_modules_are_unchanged() -> None:
    for module, digest in UNCHANGED.items():
        assert hashlib.sha256(Path(str(module.__file__)).read_bytes()).hexdigest() == digest


def test_source_reuses_the_bench_and_reads_nothing_after_2010() -> None:
    for line in (
        "from ai_trading_system.research_probes import cycle_power as cp",
        "from ai_trading_system.research_probes import longbull_tech_read as lt",
        "from ai_trading_system.research_bench.signal_strategy import signal_exposure",
        "from ai_trading_system.research_bench.checks import strategy_targets",
    ):
        assert line in SOURCE
    assert "lt.bench_data(" in SOURCE
    assert "segments.monthly_level(" in SOURCE
    for name in ("def simulate", "def signal_exposure", "def bench_data", "def monthly_level"):
        assert f"{name}(" not in SOURCE
    for text in ("2011-", "qqq_long_history", "simulate(", "market_data(", "forward_ledger"):
        assert text not in SOURCE
    assert SOURCE.count("load_snapshot") == 1
    imports = [line for line in SOURCE.splitlines() if line.startswith(("import ", "from "))]
    for banned in ("strategies", "screen", "forward", "qqq", "trial", "gate"):
        assert not any(banned in line for line in imports), banned


# ----- loading and data ---------------------------------------------------------------------------
def test_load_inputs_reads_the_two_frames_and_the_qqq_q3_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _snapshot(FACTORS, INDUSTRIES)
    asked: list[str] = []

    def load(root: Path, snapshot_id: str) -> SignalSnapshot:
        asked.append(snapshot_id)
        return snapshot

    monkeypatch.setattr(signal_sources, "load_snapshot", load)
    inputs = rs.load_inputs()
    assert asked == [cp.SNAPSHOT_ID]
    assert list(inputs.industries.columns) == list(NAMES)
    assert inputs.industries.index.max() <= pd.Timestamp("2010-12-31")
    assert inputs.factors.index.max() <= pd.Timestamp("2010-12-31")
    assert inputs.industries.index.min() == pd.Timestamp("1926-07-01")
    assert {name: entry.id for name, entry in inputs.entries.items()} == rs.RULES
    for entry in inputs.entries.values():
        assert entry.rebalance.schedule == "month_end"
        assert entry.function == "signal_exposure"
    assert inputs.entries["fast"].params == ENTRIES["fast"].params
    assert inputs.rules_sha256 == hashlib.sha256(PREREG.read_bytes()).hexdigest()


def test_clean_starts_at_the_first_value_and_drops_later_gaps() -> None:
    days = pd.bdate_range("1930-01-01", periods=8)
    series = pd.Series([np.nan, np.nan, 0.01, np.nan, 0.02, 0.03, np.nan, 0.04], index=days)
    cleaned, missing = rs.clean(series)
    assert list(cleaned.index) == [days[2], days[4], days[5], days[7]]
    assert cleaned.tolist() == [0.01, 0.02, 0.03, 0.04]
    assert missing == [days[3].date().isoformat(), days[6].date().isoformat()]
    with pytest.raises(ValueError, match="no returns"):
        rs.clean(pd.Series([np.nan, np.nan], index=days[:2]))
    with pytest.raises(ValueError, match="non-finite"):
        rs.clean(pd.Series([0.01, np.inf], index=days[:2]))


def test_market_is_the_excess_plus_cash_and_must_be_complete() -> None:
    market, cash = rs.market_series(FACTORS)
    cut = FACTORS.loc[FACTORS.index <= pd.Timestamp("2010-12-31")]
    pd.testing.assert_series_equal(
        market, cut["Mkt-RF"] + cut["RF"], check_names=False, check_freq=False
    )
    pd.testing.assert_series_equal(cash, cut["RF"], check_names=False, check_freq=False)
    with pytest.raises(ValueError, match="missing RF column"):
        rs.market_series(FACTORS.drop(columns="RF"))
    broken = FACTORS.copy()
    broken.iloc[100, 0] = np.nan
    with pytest.raises(ValueError, match="missing or non-finite"):
        rs.market_series(broken)


def test_month_targets_are_the_bench_targets_on_each_month_end() -> None:
    returns, _ = rs.clean(INDUSTRIES["Food"].loc[: pd.Timestamp("2010-12-31")])
    cash = FACTORS["RF"].reindex(returns.index)
    months = rs.month_targets(returns, cash, ENTRIES)
    assert list(months.columns) == ["date", "pos", "f0001", "fast", "slow"]
    data = lt.bench_data(returns, cash)
    periods = pd.DatetimeIndex(returns.index).to_period("M")
    ends = pd.Series(returns.index, index=returns.index).groupby(periods).max()
    assert list(months.index) == list(pd.PeriodIndex(ends.index, freq="M"))
    assert list(months["date"]) == list(ends)
    assert months["pos"].tolist() == [returns.index.get_loc(day) for day in ends]
    for name, entry in ENTRIES.items():
        daily = strategy_targets(signal_exposure, data.inputs, entry.params)
        expected = daily.loc[pd.DatetimeIndex(ends)].to_numpy(dtype=float)
        assert np.array_equal(months[name].to_numpy(dtype=float), expected, equal_nan=True)
    usable = months.dropna()
    assert set(usable["fast"]) <= {0.0, 1.0} and set(usable["slow"]) <= {0.0, 1.0}
    assert usable.index[0] < rs.FIRST_MONTH
    assert np.allclose(usable["f0001"], (usable["fast"] + usable["slow"]) / 2)


# ----- run-up state -------------------------------------------------------------------------------
def _level(values: dict[str, float], first: str, last: str, base: float = 1.0) -> pd.Series:
    index = pd.period_range(first, last, freq="M")
    return pd.Series([values.get(str(p), base) for p in index], index=index, dtype=float)


def test_runup_flags_are_two_year_raw_and_net_and_five_year_raw() -> None:
    level = _level(
        {"1931-01": 2.0, "1931-02": 1.99, "1933-12": 3.0, "1935-12": 6.0}, "1929-01", "1937-12"
    )
    market = _level({"1931-01": 1.0, "1933-12": 1.2, "1935-12": 1.5}, "1928-01", "1937-12")
    flags = rs.runup_flags(level, market)
    assert list(flags.columns) == ["raw", "net", "long_raw", "defined", "waived", "runup"]
    assert list(flags.index) == list(level.index)
    # fewer than 24 months: not defined
    assert not flags.loc[P("1930-12", "M"), "defined"]
    assert not flags.loc[P("1930-12", "M"), "runup"]
    row = flags.loc[P("1931-01", "M")]
    assert row["defined"] and row["waived"] and row["runup"]
    assert row["raw"] == pytest.approx(1.0) and row["net"] == pytest.approx(1.0)
    assert math.isnan(row["long_raw"])
    # just below 100% raw
    assert not flags.loc[P("1931-02", "M"), "runup"]
    # net of market: 3.0 / 1.0 - 1 = 2.0, minus 1.2 / 1.0 - 1 = 0.2 -> 1.8; no five years back
    row = flags.loc[P("1933-12", "M")]
    assert row["raw"] == pytest.approx(2.0) and row["net"] == pytest.approx(1.8)
    assert row["waived"] and row["runup"]
    # 1935-12: two years 6.0 / 3.0 - 1 = 1.0, net 1.0 - (1.5 / 1.2 - 1) = 0.75 < 1
    row = flags.loc[P("1935-12", "M")]
    assert row["raw"] == pytest.approx(1.0) and row["net"] == pytest.approx(0.75)
    assert not row["runup"] and not row["waived"]
    assert row["long_raw"] == pytest.approx(5.0)


def test_the_five_year_rule_applies_when_sixty_months_exist() -> None:
    values = {"1935-01": 1.0, "1938-01": 0.7, "1940-01": 1.45, "1940-02": 1.4}
    level = _level(values, "1935-01", "1940-12", base=0.7)
    market = _level({}, "1935-01", "1940-12")
    flags = rs.runup_flags(level, market)
    # 1940-01: two years 1.45 / 0.7 - 1 > 1, five years 1.45 / 1.0 - 1 = 0.45 < 0.5
    row = flags.loc[P("1940-01", "M")]
    assert row["raw"] > 1.0 and row["net"] > 1.0
    assert not row["waived"] and row["long_raw"] == pytest.approx(0.45)
    assert not row["runup"]
    # 1940-02: five years back is 1935-02 at 0.7 -> 2.0 - 1 >= 0.5
    assert flags.loc[P("1940-02", "M"), "runup"]


def _flags(
    runup: Sequence[str], first: str, last: str, undefined_before: str | None = None
) -> pd.DataFrame:
    index = pd.period_range(first, last, freq="M")
    defined = np.array([undefined_before is None or p >= P(undefined_before, "M") for p in index])
    hits = np.array([str(p) in runup for p in index]) & defined
    return pd.DataFrame({"defined": defined, "runup": hits}, index=index)


def test_state_looks_at_this_month_and_the_23_before() -> None:
    flags = _flags(["1935-06"], "1930-01", "1940-12", undefined_before="1931-01")
    state = rs.states(flags)
    assert state[P("1935-05", "M")] == "N"
    assert state[P("1935-06", "M")] == "R"
    assert state[P("1937-05", "M")] == "R"  # 1935-06 is the 24th month back, counted
    assert state[P("1937-06", "M")] == "N"
    # before 24 defined months: undetermined, run-up month in the window or not
    assert state[P("1931-01", "M")] == "undetermined"
    assert state[P("1932-11", "M")] == "undetermined"
    assert state[P("1932-12", "M")] == "N"
    early = rs.states(_flags(["1931-03"], "1930-01", "1932-12", undefined_before="1931-01"))
    assert early[P("1931-02", "M")] == "undetermined"
    assert early[P("1931-03", "M")] == "undetermined"
    assert early[P("1932-11", "M")] == "undetermined"
    assert early[P("1932-12", "M")] == "R"


def test_state_counts_missing_months_as_undefined() -> None:
    flags = _flags([], "1930-01", "1934-12").drop(index=[P("1932-03", "M")])
    state = rs.states(flags)
    assert state[P("1932-02", "M")] == "N"
    assert state[P("1932-04", "M")] == "undetermined"
    assert state[P("1934-02", "M")] == "undetermined"
    assert state[P("1934-03", "M")] == "N"


def test_an_episode_starts_after_24_months_without_a_run_up() -> None:
    flags = _flags(
        ["1930-01", "1930-02", "1931-06", "1933-06", "1935-07", "1935-08"], "1928-01", "1937-12"
    )
    assert rs.episode_starts(flags) == [P("1930-01", "M"), P("1935-07", "M")]
    flags = _flags(["1930-01", "1932-02"], "1928-01", "1934-12")
    assert rs.episode_starts(flags) == [P("1930-01", "M"), P("1932-02", "M")]


# ----- exits --------------------------------------------------------------------------------------
DAYS = pd.bdate_range("1928-01-02", "1931-12-31")


def test_leg_exits_are_one_to_zero_month_ends_in_the_window() -> None:
    fast = {"1928-06": 0.0, "1929-01": 0.0, "1929-02": 0.0, "1929-03": 1.0, "1931-10": 0.0}
    fast.update({"1931-11": 0.0, "1931-12": 0.0})
    months = _months_frame(DAYS, fast, nan_before="1928-03")
    assert rs.leg_exits(months, "fast") == [
        (P("1929-01", "M"), P("1929-03", "M")),
        (P("1931-10", "M"), None),
    ]
    assert rs.leg_exits(months, "slow") == []
    # the month before the first usable one does not count as 1
    months = _months_frame(DAYS, {"1929-01": 0.0}, nan_before="1929-01")
    months.loc[P("1929-01", "M"), ["f0001", "fast", "slow"]] = [0.5, 0.0, 1.0]
    assert rs.leg_exits(months, "fast") == []
    # an exit in 2010-12 is read, one after 2010 is not
    late = _months_frame(
        pd.bdate_range("2010-10-01", "2011-03-31"), {"2010-12": 0.0, "2011-02": 0.0}
    )
    assert rs.leg_exits(late, "fast") == [(P("2010-12", "M"), P("2011-01", "M"))]


def test_only_one_to_zero_counts_and_rows_with_a_missing_target_are_skipped() -> None:
    fast = {"1929-04": 0.0, "1929-06": 0.5, "1929-07": 0.0, "1929-11": 0.0}
    months = _months_frame(DAYS, fast)
    months.loc[P("1929-10", "M"), "fast"] = np.nan
    # back the very next month; 1 -> 0.5 -> 0 is no exit; 1929-10 is skipped, so 1929-11 is one
    assert rs.leg_exits(months, "fast") == [
        (P("1929-04", "M"), P("1929-05", "M")),
        (P("1929-11", "M"), P("1929-12", "M")),
    ]


def test_identity_mismatches_are_listed_inside_the_window() -> None:
    months = _months_frame(DAYS, {"1929-05": 0.0})
    assert rs.identity_mismatches(months) == []
    months.loc[P("1929-07", "M"), "f0001"] = 0.75
    months.loc[P("1928-07", "M"), "f0001"] = 0.25
    assert rs.identity_mismatches(months) == ["1929-07"]


def test_positions_map_months_to_day_positions() -> None:
    months = _months_frame(DAYS, {})
    where = rs.positions(months)
    assert where[P("1928-01", "M")] == len(pd.bdate_range("1928-01-02", "1928-01-31")) - 1
    assert all(DAYS[where[p]] == months.at[p, "date"] for p in months.index)


def test_exit_rows_measure_the_trip_and_what_followed() -> None:
    rng = np.random.default_rng(3)
    returns = pd.Series(rng.normal(0.0005, 0.01, len(DAYS)), index=DAYS)
    returns.iloc[400] = -0.30
    industry = _industry(
        returns,
        {
            "1929-03": 0.0,
            "1929-04": 0.0,
            "1931-09": 0.0,
            "1931-10": 0.0,
            "1931-11": 0.0,
            "1931-12": 0.0,
        },
        state={"1929-03": "R", "1931-09": "undetermined"},
        starts=["1928-02", "1929-01"],
        cash=0.0001,
    )
    rows = rs.exit_rows(industry, "fast")
    assert [row["month"] for row in rows] == ["1929-03", "1931-09"]
    first, second = rows
    exit_day = _day(industry, "1929-03")
    back_day = _day(industry, "1929-05")
    inside = (DAYS > exit_day) & (DAYS <= back_day)
    out = float(np.sum(np.log1p(returns[inside]) - np.log1p(0.0001)))
    assert first["out_excess"] == pytest.approx(out, abs=1e-12)
    assert first["leg_pnl"] == pytest.approx(-0.5 * out, abs=1e-12)
    assert first["whipsaw"] is (out > 0)
    at = _pos(DAYS, exit_day)
    level = np.cumprod(1.0 + returns.to_numpy())
    follow = float(np.min(level[at + 1 : at + 253]) / level[at] - 1.0)
    assert 400 in range(at + 1, at + 253)
    assert first["follow_drop"] == pytest.approx(follow, abs=1e-12)
    assert first["real_drop"] is (follow <= -0.20)
    assert first == {
        **first,
        "industry": "Test",
        "leg": "fast",
        "date": exit_day.date().isoformat(),
        "year": 1929,
        "state": "R",
        "episode": "1929-01",
        "complete": True,
        "reentry": "1929-05",
        "months_out": 2,
    }
    assert set(first) == {
        "industry",
        "leg",
        "month",
        "date",
        "year",
        "state",
        "episode",
        "complete",
        "reentry",
        "months_out",
        "out_excess",
        "whipsaw",
        "leg_pnl",
        "follow_drop",
        "real_drop",
        "vol63",
        "back63",
    }
    window = returns.to_numpy()[at - 62 : at + 1]
    assert first["vol63"] == pytest.approx(float(np.std(window, ddof=1)) * 252**0.5, rel=1e-12)
    assert first["back63"] is bool(np.max(level[at + 1 : at + 64]) >= level[at])
    # never back by the end: open, no whipsaw; fewer than 252 days left: no follow
    assert second["complete"] is False and second["reentry"] is None
    assert second["months_out"] is None and second["whipsaw"] is None
    assert second["state"] == "undetermined" and second["episode"] is None
    after = DAYS > _day(industry, "1931-09")
    assert second["out_excess"] == pytest.approx(
        float(np.sum(np.log1p(returns[after]) - np.log1p(0.0001))), abs=1e-12
    )
    assert second["follow_drop"] is None and second["real_drop"] is None
    assert second["back63"] is not None and second["vol63"] is not None


def test_vol_before_uses_63_days_up_to_the_exit_day() -> None:
    returns = np.linspace(-0.02, 0.03, 100)
    assert rs.vol_before(returns, 61) is None
    assert rs.vol_before(returns, 62) == pytest.approx(
        float(np.std(returns[:63], ddof=1)) * 252**0.5
    )
    assert rs.vol_before(returns, 80) == pytest.approx(
        float(np.std(returns[18:81], ddof=1)) * 252**0.5
    )
    changed = returns.copy()
    changed[81:] = 0.5
    assert rs.vol_before(changed, 80) == rs.vol_before(returns, 80)


def test_back63_looks_63_days_after_the_exit() -> None:
    returns = pd.Series(-0.001, index=DAYS)
    exit_day = pd.Timestamp("1930-06-30")
    at = _pos(DAYS, exit_day)
    returns.iloc[at + 63] = 0.2
    (row,) = rs.exit_rows(_industry(returns, {"1930-06": 0.0}), "fast")
    assert row["back63"] is True
    returns.iloc[at + 63] = -0.001
    returns.iloc[at + 64] = 0.2
    (row,) = rs.exit_rows(_industry(returns, {"1930-06": 0.0}), "fast")
    assert row["back63"] is False
    short = _industry(returns.loc[: DAYS[at + 62]], {"1930-06": 0.0})
    (row,) = rs.exit_rows(short, "fast")
    assert row["back63"] is None
    # the volatility before the exit does not move when what follows changes
    calm = returns.copy()
    calm.iloc[at + 1 :] = 0.0
    (other,) = rs.exit_rows(_industry(calm, {"1930-06": 0.0}), "fast")
    assert other["vol63"] == row["vol63"]
    # flat after the exit: back at the exit level counts
    assert other["back63"] is True


def test_follow_needs_252_days_and_counts_exactly_252() -> None:
    returns = pd.Series(0.001, index=DAYS)
    exit_day = pd.Timestamp("1930-06-30")
    at = _pos(DAYS, exit_day)
    returns.iloc[at + 252] = -0.40
    industry = _industry(returns, {"1930-06": 0.0})
    (row,) = rs.exit_rows(industry, "fast")
    assert row["follow_drop"] == pytest.approx(1.001**251 * 0.60 - 1.0)
    assert row["real_drop"] is True
    returns.iloc[at + 252] = 0.001
    returns.iloc[at + 253] = -0.40
    (row,) = rs.exit_rows(_industry(returns, {"1930-06": 0.0}), "fast")
    assert row["follow_drop"] == pytest.approx(0.001)
    assert row["real_drop"] is False
    short = _industry(returns.loc[: DAYS[at + 251]], {"1930-06": 0.0})
    (row,) = rs.exit_rows(short, "fast")
    assert row["follow_drop"] is None


def test_a_trip_that_loses_to_cash_is_not_a_whipsaw() -> None:
    returns = pd.Series(0.0, index=DAYS)
    industry = _industry(returns, {"1929-03": 0.0}, cash=0.0)
    (row,) = rs.exit_rows(industry, "fast")
    assert row["out_excess"] == 0.0 and row["whipsaw"] is False and row["leg_pnl"] == 0.0
    industry = _industry(returns, {"1929-03": 0.0}, cash=0.0002)
    (row,) = rs.exit_rows(industry, "fast")
    assert row["out_excess"] < 0 and row["whipsaw"] is False and row["leg_pnl"] > 0


def test_the_episode_is_the_latest_start_before_the_exit() -> None:
    returns = pd.Series(0.001, index=DAYS)
    industry = _industry(
        returns,
        {"1929-03": 0.0, "1930-08": 0.0},
        state={"1929-03": "R", "1930-08": "R"},
        starts=["1929-03", "1930-01"],
    )
    rows = rs.exit_rows(industry, "fast")
    assert [row["episode"] for row in rows] == ["1929-03", "1930-01"]


# ----- GSY fidelity -------------------------------------------------------------------------------
def _crash_path(drop: float, at_month: int) -> pd.Series:
    days = pd.bdate_range("1928-01-02", "1934-12-31")
    returns = pd.Series(0.0, index=days)
    start = pd.Timestamp("1929-06-28")
    peak = _pos(days, start) + 20
    returns.iloc[peak] = 0.10
    when = days[days.to_period("M") == P("1929-06", "M") + at_month][5]
    returns.iloc[_pos(days, when)] = drop
    return returns


@pytest.mark.parametrize(
    ("drop", "at_month", "expected"),
    [(-0.45, 6, True), (-0.35, 6, False), (-0.45, 24, True), (-0.45, 25, False)],
)
def test_a_crash_is_a_40_percent_drawdown_from_the_peak_after_the_start(
    drop: float, at_month: int, expected: bool
) -> None:
    industry = _industry(_crash_path(drop, at_month), {})
    assert rs.crashes(industry, [P("1929-06", "M")]) == [expected]


def test_a_peak_before_the_start_does_not_count_and_short_data_is_none() -> None:
    days = pd.bdate_range("1928-01-02", "1934-12-31")
    returns = pd.Series(0.0, index=days)
    returns.iloc[_pos(days, pd.Timestamp("1929-03-01"))] = 0.5
    returns.iloc[_pos(days, pd.Timestamp("1929-05-01"))] = -0.2
    returns.iloc[_pos(days, pd.Timestamp("1929-07-01"))] = -0.3
    industry = _industry(returns, {})
    # from the 1929-06 start (level 1.2) the fall is 30%; from the 1929-03 high (1.5) it is 44%
    assert rs.crashes(industry, [P("1929-06", "M"), P("1929-02", "M")]) == [False, True]
    returns.iloc[_pos(days, pd.Timestamp("1929-07-01"))] = -0.45
    assert rs.crashes(_industry(returns, {}), [P("1929-06", "M")]) == [True]
    assert rs.crashes(_industry(returns, {}), [P("1933-01", "M")]) == [None]


def test_fidelity_counts_episodes_from_1929_to_2008(monkeypatch: pytest.MonkeyPatch) -> None:
    fid = RESULT["fidelity"]
    expected = [
        (name, str(start))
        for name, industry in BUILT.industries.items()
        for start in industry.starts
        if P("1929-01", "M") <= start <= P("2008-12", "M")
    ]
    assert [(item["industry"], item["start"]) for item in fid["list"]] == expected
    counted = [item for item in fid["list"] if item["crash"] is not None]
    assert fid["episodes"] == len(counted)
    assert fid["crashes"] == sum(1 for item in counted if item["crash"])
    assert fid["share"] == pytest.approx(fid["crashes"] / fid["episodes"])
    assert fid["gsy"] == rs.GSY
    for name, industry in BUILT.industries.items():
        found = rs.crashes(
            industry, [P(item["start"], "M") for item in fid["list"] if item["industry"] == name]
        )
        assert found == [item["crash"] for item in fid["list"] if item["industry"] == name]


def test_unconditional_crash_share_uses_every_defined_month(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    every: list[tuple[str, int]] = []

    def crashes(industry: rs.Industry, starts: Sequence[pd.Period]) -> list[bool | None]:
        every.extend((industry.name, start.ordinal) for start in starts)
        return [start.year == 1970 for start in starts]

    monkeypatch.setattr(rs, "crashes", crashes)
    fid = rs.fidelity(BUILT)
    months = [
        (name, p.ordinal)
        for name, industry in BUILT.industries.items()
        for p in industry.flags.index[industry.flags["defined"].to_numpy(dtype=bool)]
        if P("1929-01", "M") <= p <= P("2008-12", "M")
    ]
    assert set(months) <= set(every)
    years = [pd.Period(ordinal=p, freq="M").year for _, p in months]
    later = sum(1 for year in years if year >= 1970)
    hits = sum(1 for year in years if year == 1970)
    assert fid["unconditional_months"] == len(months)
    assert fid["unconditional"] == pytest.approx(hits / len(months))
    assert fid["after_1970"] == pytest.approx(hits / later)


def _fake_fidelity(
    monkeypatch: pytest.MonkeyPatch, crashed: int, total: int, unknown: int = 0
) -> dict[str, Any]:
    calls = {"n": 0}

    def crashes(industry: rs.Industry, starts: Sequence[pd.Period]) -> list[bool | None]:
        out: list[bool | None] = []
        for _ in starts:
            out.append(None if calls["n"] >= total else calls["n"] < crashed)
            calls["n"] += 1
        return out

    starts = [P("1930-01", "M") + 30 * k for k in range(total + unknown)]
    one = next(iter(BUILT.industries.values()))
    fake = rs.Industry(one.name, one.returns, one.cash, one.months, one.flags.iloc[:0], starts)
    monkeypatch.setattr(rs, "crashes", crashes)
    return rs.fidelity(rs.Built({one.name: fake}, BUILT.rows, BUILT.market, BUILT.market_flags))


@pytest.mark.parametrize(
    ("crashed", "total", "passed"),
    [(6, 20, True), (5, 20, False), (15, 20, True), (16, 20, False), (8, 15, True), (7, 14, False)],
)
def test_fidelity_band(
    monkeypatch: pytest.MonkeyPatch, crashed: int, total: int, passed: bool
) -> None:
    fid = _fake_fidelity(monkeypatch, crashed, total)
    assert fid["episodes"] == total and fid["crashes"] == crashed
    assert fid["passed"] is passed


def test_fidelity_leaves_out_episodes_without_24_months(monkeypatch: pytest.MonkeyPatch) -> None:
    fid = _fake_fidelity(monkeypatch, 8, 15, unknown=3)
    assert len(fid["list"]) == 18 and [item["crash"] for item in fid["list"][15:]] == [None] * 3
    assert fid["episodes"] == 15 and fid["crashes"] == 8
    assert fid["share"] == pytest.approx(8 / 15) and fid["passed"] is True


# ----- tables -------------------------------------------------------------------------------------
def _row(
    state: str,
    whipsaw: bool | None,
    *,
    year: int = 1950,
    pnl: float = 0.01,
    drop: bool | None = False,
    industry: str = "Food",
    episode: str | None = None,
    months_out: int | None = 2,
    vol: float | None = 0.2,
    back: bool | None = None,
) -> dict[str, Any]:
    complete = whipsaw is not None
    return {
        "industry": industry,
        "leg": "fast",
        "month": f"{year}-03",
        "date": f"{year}-03-31",
        "year": year,
        "state": state,
        "episode": episode,
        "complete": complete,
        "reentry": f"{year}-05" if complete else None,
        "months_out": months_out if complete else None,
        "out_excess": -2 * pnl,
        "whipsaw": whipsaw,
        "leg_pnl": pnl,
        "follow_drop": None if drop is None else (-0.3 if drop else -0.05),
        "real_drop": drop,
        "vol63": vol,
        "back63": back,
    }


def test_group_stats_use_complete_exits() -> None:
    rows = [
        _row("R", True, pnl=-0.02, months_out=1, back=True),
        _row("R", False, pnl=0.05, drop=True, months_out=4, back=False),
        _row("R", True, pnl=-0.01, drop=None, months_out=2),
        _row("R", None, pnl=0.30, drop=True, back=True),
    ]
    stats = rs.group_stats(rows)
    assert stats == {
        "exits": 4,
        "complete": 3,
        "open": 1,
        "whipsaw": pytest.approx(2 / 3),
        "pnl_median": pytest.approx(-0.01),
        "pnl_mean": pytest.approx(0.02 / 3),
        "pnl_sum": pytest.approx(0.02),
        "follow": 2,
        "no_follow": 1,
        "real_drop": pytest.approx(0.5),
        "months_median": 2,
        # every exit with 63 days after it, open ones too
        "back63": pytest.approx(2 / 3),
    }
    empty = rs.group_stats([])
    assert empty["whipsaw"] is None and empty["pnl_median"] is None and empty["pnl_sum"] == 0.0
    assert empty["real_drop"] is None and empty["months_median"] is None
    assert empty["back63"] is None


def test_table_gap_is_n_minus_r() -> None:
    rows = [_row("R", True), _row("R", False), _row("N", True), _row("N", True), _row("N", False)]
    rows.append(_row("undetermined", True))
    part = rs.table(rows)
    assert part["gap"] == pytest.approx(2 / 3 - 1 / 2)
    assert part["undetermined"] == 1
    assert part["R"]["complete"] == 2 and part["N"]["complete"] == 3
    assert rs.table([_row("N", True)])["gap"] is None


def test_drop_year_removes_the_busiest_r_year_from_both_groups() -> None:
    rows = [
        _row("R", True, year=1950),
        _row("R", True, year=1950),
        _row("R", False, year=1960),
        _row("R", False, year=1960),
        _row("R", True, year=1970),
        _row("R", None, year=1970),
        _row("R", None, year=1970),
        _row("N", True, year=1950),
        _row("N", False, year=1960),
    ]
    dropped = rs.drop_year(rows)
    assert dropped["year"] == 1950 and dropped["removed"] == 2
    assert dropped["table"]["R"]["complete"] == 3 and dropped["table"]["N"]["complete"] == 1
    assert rs.drop_year([_row("N", True)]) == {
        "year": None,
        "removed": 0,
        "table": rs.table([_row("N", True)]),
    }


def test_episode_weighted_share_and_protection_cells() -> None:
    rows = [
        _row("R", True, episode="1950-01", pnl=-0.02, drop=False),
        _row("R", False, episode="1950-01", pnl=0.04, drop=True),
        _row("R", True, episode="1950-01", pnl=-0.01, drop=None),
        _row("R", True, episode="1970-01", industry="Steel", pnl=-0.03, drop=False),
        _row("R", None, episode="1970-01", industry="Steel", pnl=0.5, drop=True),
        _row("N", False, pnl=0.06, drop=True),
        _row("N", False, pnl=0.02, drop=True),
    ]
    weighted = rs.episode_weighted(rows)
    assert weighted == {"episodes": 2, "whipsaw": pytest.approx((2 / 3 + 1) / 2)}
    cells = rs.protection(rows)
    assert set(cells) == {"R_drop", "R_no_drop", "N_drop", "N_no_drop"}
    assert cells["R_drop"] == {
        "exits": 1,
        "pnl_sum": pytest.approx(0.04),
        "pnl_mean": pytest.approx(0.04),
    }
    assert cells["R_no_drop"]["exits"] == 2
    assert cells["R_no_drop"]["pnl_sum"] == pytest.approx(-0.05)
    assert cells["N_drop"] == {
        "exits": 2,
        "pnl_sum": pytest.approx(0.08),
        "pnl_mean": pytest.approx(0.04),
    }
    assert cells["N_no_drop"] == {"exits": 0, "pnl_sum": 0.0, "pnl_mean": None}


# (state, whipsaw, real_drop, vol63); ten complete exits put the cuts on the 4th and 7th values
VOL_ROWS = (
    ("R", True, True, 0.10),
    ("N", True, False, 0.15),
    ("undetermined", True, None, 0.20),
    ("R", False, False, 0.30),
    ("N", False, True, 0.40),
    ("R", True, None, 0.50),
    ("N", False, True, 0.60),
    ("N", True, False, 0.70),
    ("R", False, True, 0.80),
    ("N", True, None, 0.90),
)


def _vol_rows() -> list[dict[str, Any]]:
    rows = [_row(s, w, drop=d, vol=v) for s, w, d, v in VOL_ROWS]
    # an open exit and an exit without 63 days before it stay out of the cuts and the tiers
    return rows + [_row("R", None, vol=0.05), _row("R", True, vol=None)]


def _undetermined(rows: list[dict[str, Any]], *vols: float) -> list[dict[str, Any]]:
    """Moves N exits out of the tiers but keeps them in the cuts."""

    return [
        {**row, "state": "undetermined"} if row["state"] == "N" and row["vol63"] in vols else row
        for row in rows
    ]


def test_vol_tiers_split_complete_exits_in_three_and_weight_n_by_r() -> None:
    vol = rs.vol_tiers(_vol_rows())
    assert vol["cuts"] == pytest.approx([0.30, 0.60])
    low, mid, high = vol["tiers"]
    assert low == {
        "R": {"complete": 2, "whipsaw": 0.5, "real_drop": 0.5, "follow": 2},
        "N": {"complete": 1, "whipsaw": 1.0, "real_drop": 0.0, "follow": 1},
    }
    assert mid == {
        "R": {"complete": 1, "whipsaw": 1.0, "real_drop": None, "follow": 0},
        "N": {"complete": 2, "whipsaw": 0.0, "real_drop": 1.0, "follow": 2},
    }
    assert high == {
        "R": {"complete": 1, "whipsaw": 0.0, "real_drop": 1.0, "follow": 1},
        "N": {"complete": 2, "whipsaw": 1.0, "real_drop": 0.0, "follow": 1},
    }
    # whipsaw weights from R's complete exits 2/1/1, real-drop weights from R's follows 2/0/1
    assert vol["R_whipsaw"] == pytest.approx(0.5)
    assert vol["N_weighted_whipsaw"] == pytest.approx(0.75)
    assert vol["weighted_gap"] == pytest.approx(0.25)
    assert vol["R_real_drop"] == pytest.approx(2 / 3)
    assert vol["N_weighted_real_drop"] == pytest.approx(0.0)
    assert vol["weighted_drop_gap"] == pytest.approx(2 / 3)


def test_vol_tiers_without_enough_exits_or_n_in_a_weighted_tier() -> None:
    empty = rs.vol_tiers(_vol_rows()[:2] + [_row("R", None, vol=0.5)])
    assert empty == {
        "cuts": None,
        "tiers": [],
        "R_whipsaw": None,
        "N_weighted_whipsaw": None,
        "weighted_gap": None,
        "R_real_drop": None,
        "N_weighted_real_drop": None,
        "weighted_drop_gap": None,
    }
    assert rs.vol_tiers(_vol_rows()[:3])["cuts"] is not None
    # no N exit in the top tier while R has one there: no weighted N (the cuts stay)
    vol = rs.vol_tiers(_undetermined(_vol_rows(), 0.70, 0.90))
    assert vol["cuts"] == pytest.approx([0.30, 0.60])
    assert vol["tiers"][2]["N"] == {"complete": 0, "whipsaw": None, "real_drop": None, "follow": 0}
    assert vol["N_weighted_whipsaw"] is None and vol["weighted_gap"] is None
    assert vol["N_weighted_real_drop"] is None and vol["weighted_drop_gap"] is None
    # R without a complete exit: nothing to weight by
    only_n = [row for row in _vol_rows() if row["state"] != "R"]
    vol = rs.vol_tiers(only_n)
    assert vol["cuts"] is not None and vol["R_whipsaw"] is None and vol["weighted_gap"] is None


def test_vol_tiers_skip_a_tier_r_does_not_use() -> None:
    # R's middle exit has no follow: the real-drop weight there is 0 and N's empty cell is unused
    vol = rs.vol_tiers(_undetermined(_vol_rows(), 0.40, 0.60))
    assert vol["tiers"][1]["N"]["complete"] == 0 and vol["tiers"][1]["R"]["follow"] == 0
    assert vol["N_weighted_whipsaw"] is None and vol["weighted_gap"] is None
    assert vol["N_weighted_real_drop"] == pytest.approx(0.0)
    assert vol["weighted_drop_gap"] == pytest.approx(2 / 3)


@pytest.mark.parametrize(
    ("by_episode", "flag"),
    [(0.61, True), (0.60, False), (0.40, False), (0.39, True), (None, None)],
)
def test_concentration_flags_a_ten_point_spread(
    by_episode: float | None, flag: bool | None
) -> None:
    main = {"R": {"whipsaw": 0.5}}
    assert rs.concentration(main, {"whipsaw": by_episode}) == {
        "pooled": 0.5,
        "by_episode": by_episode,
        "flag": flag,
    }
    assert rs.concentration({"R": {"whipsaw": None}}, {"whipsaw": 0.5})["flag"] is None


@pytest.mark.parametrize(
    ("verdict", "weighted", "gap", "mostly"),
    [
        ("holds", 0.09, 0.2, True),
        ("holds", 0.1, 0.2, False),
        ("holds", 0.25, 0.2, False),
        ("holds", None, 0.2, None),
        ("holds", 0.05, None, None),
        ("not_holds", 0.01, 0.2, None),
        ("too_few", 0.01, 0.2, None),
    ],
)
def test_vol_reading_only_on_holds(
    verdict: str, weighted: float | None, gap: float | None, mostly: bool | None
) -> None:
    tables = {"vol": {"weighted_gap": weighted}, "main": {"gap": gap}}
    assert rs.vol_reading({"verdict": verdict}, tables) is mostly


def test_halves_are_judged_only_with_20_r_exits() -> None:
    rows = [_row("R", False, year=1940) for _ in range(20)]
    rows += [_row("N", True, year=1940), _row("N", False, year=1940)]
    rows += [_row("R", True, year=1980) for _ in range(19)] + [_row("N", False, year=1980)]
    tables = rs.tables_of(rows, BUILT)
    first, second = tables["halves"]["1929–1969"], tables["halves"]["1970–2010"]
    assert first["judged"] is True and first["ok"] is True
    assert second["judged"] is False and second["ok"] is None
    rows[-1] = _row("N", True, year=1980)
    rows += [_row("R", True, year=1990)]
    tables = rs.tables_of(rows, BUILT)
    second = tables["halves"]["1970–2010"]
    assert (
        second["judged"] is True and second["gap"] == pytest.approx(0.0) and second["ok"] is False
    )


def test_tables_split_legs_and_tech_industries() -> None:
    tables = RESULT["tables"]
    fast = [row for row in EXITS if row["leg"] == "fast"]
    slow = [row for row in EXITS if row["leg"] == "slow"]
    assert tables["main"] == json.loads(json.dumps(rs.table(fast)))
    assert tables["slow"] == json.loads(json.dumps(rs.table(slow)))
    assert slow and fast
    for label, first, last in rs.HALVES:
        part = json.loads(json.dumps(rs.table([r for r in fast if first <= r["year"] <= last])))
        assert {key: tables["halves"][label][key] for key in part} == part
    assert set(tables["tech"]) == {"Hardw", "Chips", "Softw"}
    assert tables["tech"]["Chips"] == json.loads(
        json.dumps(rs.table([row for row in fast if row["industry"] == "Chips"]))
    )
    assert {row["industry"] for row in EXITS} == set(BUILT.industries)
    assert tables["vol"] == json.loads(json.dumps(rs.vol_tiers(fast)))
    assert tables["vol"]["cuts"] is not None
    assert tables["concentration"] == json.loads(
        json.dumps(rs.concentration(rs.table(fast), rs.episode_weighted(fast)))
    )


# ----- verdict -----------------------------------------------------------------------------------
def _stats(complete: int, whipsaw: float | None) -> dict[str, Any]:
    return {"exits": complete, "complete": complete, "whipsaw": whipsaw}


def _tables(
    *,
    r: int = 100,
    open_r: int = 0,
    gap: float = 0.2,
    halves: Sequence[bool | None] = (True, True),
    drop: float = 0.15,
) -> dict[str, Any]:
    main: dict[str, Any] = {"R": _stats(r, 0.4), "N": _stats(5000, 0.4 + gap), "gap": gap}
    main["R"]["exits"] += open_r
    return {
        "main": main,
        "halves": {label: {"ok": ok} for (label, _, _), ok in zip(rs.HALVES, halves, strict=True)},
        "drop_year": {"table": {"gap": drop}},
    }


CHECK_OK = {"excluded": ["Gold"]}
FID_OK = {"passed": True}


@pytest.mark.parametrize(
    ("changes", "verdict"),
    [
        ({}, "holds"),
        ({"gap": 0.15}, "holds"),
        ({"gap": 0.1499}, "not_holds"),
        ({"drop": 0.10}, "holds"),
        ({"drop": 0.0999}, "not_holds"),
        ({"halves": (True, None)}, "holds"),
        ({"halves": (None, None)}, "holds"),
        ({"halves": (True, False)}, "not_holds"),
        ({"r": 60}, "holds"),
        ({"r": 59}, "too_few"),
        ({"r": 59, "open_r": 5}, "too_few"),
    ],
)
def test_verdict_lines(changes: dict[str, Any], verdict: str) -> None:
    decision = rs.judge(CHECK_OK, FID_OK, _tables(**changes))
    assert decision["verdict"] == verdict


def test_verdict_order() -> None:
    assert rs.judge(CHECK_OK, {"passed": False}, _tables(r=10))["verdict"] == "fidelity_miss"
    many = {"excluded": [f"I{k}" for k in range(11)]}
    decision = rs.judge(many, {"passed": False}, _tables(r=10))
    assert decision == {"verdict": "incomplete", "reasons": ["不用的行业 11 个，超过 10 个"]}
    assert (
        rs.judge({"excluded": [f"I{k}" for k in range(10)]}, FID_OK, _tables())["verdict"]
        == "holds"
    )
    none = _tables()
    none["main"]["R"]["exits"] = none["main"]["N"]["exits"] = 0
    assert rs.judge(CHECK_OK, FID_OK, none) == {
        "verdict": "incomplete",
        "reasons": ["没有快腿退出"],
    }
    missing = _tables()
    missing["main"]["gap"] = None
    assert rs.judge(CHECK_OK, FID_OK, missing)["verdict"] == "not_holds"


# ----- building, check and the read --------------------------------------------------------------
def test_build_leaves_out_gappy_or_broken_industries() -> None:
    rows = {row["name"]: row for row in BUILT.rows}
    assert list(rows) == list(NAMES)
    assert set(BUILT.industries) == set(NAMES) - {"Gold"}
    assert rows["Gold"]["used"] is False and rows["Gold"]["reason"] == "缺 6 天"
    assert rows["Softw"]["used"] is True and rows["Softw"]["missing"] == 2
    assert rows["Softw"]["first"] == "1965-07-01" and rows["Softw"]["last"] == "2010-12-31"
    assert rows["Softw"]["missing_days"] == ["1971-03-11", "1983-06-15"]
    five = INDUSTRIES.copy()
    five.loc[GOLD_GAP[:5], "Gold"] = 0.0
    five.loc[GOLD_GAP[5:], "Gold"] = 0.0
    five.loc[pd.bdate_range("1960-03-01", periods=5), "Gold"] = np.nan
    broken = five.copy()
    broken.loc[pd.Timestamp("1970-06-01"), "Food"] = 0.8
    built = rs.build(rs.Inputs(rs.cut(broken), BASE.factors, BASE.entries, BASE.rules_sha256))
    rows = {row["name"]: row for row in built.rows}
    assert rows["Gold"]["used"] is True and rows["Gold"]["missing"] == 5
    assert rows["Food"]["used"] is False and rows["Food"]["reason"].startswith("出错：")
    assert "Food" not in built.industries
    # a day the industry has but the factors lack: no cash return, the industry is left out
    factors = BASE.factors.drop(index=pd.Timestamp("1975-06-02"))
    built = rs.build(rs.Inputs(BASE.industries[["Food"]], factors, BASE.entries, "x"))
    assert built.industries == {}
    assert built.rows[0]["reason"] == "出错：missing RF on 1975-06-02"


def test_an_industry_is_built_from_its_cleaned_series() -> None:
    softw = BUILT.industries["Softw"]
    returns, _ = rs.clean(rs.cut(INDUSTRIES)["Softw"])
    pd.testing.assert_series_equal(softw.returns, returns)
    assert list(softw.months.columns) == ["date", "pos", "f0001", "fast", "slow", "runup", "state"]
    cash = FACTORS["RF"].reindex(returns.index)
    pd.testing.assert_series_equal(softw.cash, cash, check_names=False)
    targets = rs.month_targets(returns, cash, ENTRIES)
    pd.testing.assert_frame_equal(softw.months[list(targets.columns)], targets)
    from ai_trading_system.research_bench import segments

    market, _ = rs.market_series(FACTORS)
    flags = rs.runup_flags(segments.monthly_level(returns), segments.monthly_level(market))
    pd.testing.assert_frame_equal(softw.flags, flags)
    assert softw.months["runup"].tolist() == flags["runup"].reindex(softw.months.index).tolist()
    assert softw.months["state"].tolist() == rs.states(flags).reindex(softw.months.index).tolist()
    assert softw.starts == rs.episode_starts(flags)


def test_planted_run_ups_are_found() -> None:
    starts = {
        name: [str(s) for s in industry.starts] for name, industry in BUILT.industries.items()
    }
    for name, planted in RUNUPS.items():
        if name not in BUILT.industries:
            continue
        for first, _ in planted:
            begin = P(first, "M")
            assert any(begin + 1 <= P(s, "M") <= begin + 30 for s in starts[name]), (name, first)
    assert RESULT["check"]["episodes_in_window"] >= 10
    # one start falls after 2008-12: counted in episodes, not in the fidelity window
    assert any(
        start > rs.EPISODE_LAST
        for industry in BUILT.industries.values()
        for start in industry.starts
    )


def test_check_only_counts_inputs(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("check must not look past the exits")

    monkeypatch.setattr(rs, "exit_rows", forbidden)
    monkeypatch.setattr(rs, "crashes", forbidden)
    check = rs.check_summary(BUILT)
    assert json.loads(json.dumps(check)) == RESULT["check"]
    text = json.dumps(check, ensure_ascii=False)
    for word in ("whipsaw", "pnl", "follow", "crash", "complete", "reentry", "out_excess"):
        assert word not in text


def test_check_counts_match_the_exits() -> None:
    check = RESULT["check"]
    for leg in rs.LEGS:
        rows = [row for row in EXITS if row["leg"] == leg]
        assert check["exits"][leg] == {
            s: sum(1 for r in rows if r["state"] == s) for s in rs.STATES
        }
    fast = [row for row in EXITS if row["leg"] == "fast"]
    for label, first, last in rs.HALVES:
        part = [row for row in fast if first <= row["year"] <= last]
        assert check["halves"][label] == {
            s: sum(1 for r in part if r["state"] == s) for s in rs.STATES
        }
    assert list(check["decades"]) == [f"{d}s" for d in range(1920, 2011, 10)]
    assert sum(c["R"] + c["N"] + c["undetermined"] for c in check["decades"].values()) == len(fast)
    twenties = check["decades"]["1920s"]
    assert sum(twenties.values()) == sum(1 for row in fast if row["year"] == 1929)
    assert set(check["tech"]) == {"Hardw", "Chips", "Softw"}
    by_name = {row["name"]: row for row in check["industries"]}
    assert by_name["Hardw"]["fast"] == {
        s: sum(1 for r in fast if r["industry"] == "Hardw" and r["state"] == s) for s in rs.STATES
    }
    assert "fast" not in by_name["Gold"]
    assert check["used"] == 6 and check["excluded"] == ["Gold"]
    assert check["identity_mismatches"] == 0 and check["identity_first"] == []
    window = [
        industry.flags.loc[
            (industry.flags.index >= rs.FIRST_MONTH) & (industry.flags.index <= rs.LAST_MONTH)
        ]
        for industry in BUILT.industries.values()
    ]
    assert check["runup_months"] == sum(int(f["runup"].sum()) for f in window)
    assert check["waived_months"] == sum(int((f["runup"] & f["waived"]).sum()) for f in window)
    assert check["episodes"] == sum(len(i.starts) for i in BUILT.industries.values())
    assert check["episodes_in_window"] == len(RESULT["fidelity"]["list"])
    market = BUILT.market_flags
    inside = market.loc[(market.index >= rs.FIRST_MONTH) & (market.index <= rs.LAST_MONTH)]
    assert check["market_runup_months"] == int((inside["raw"] >= 1.0).sum())


def test_quartiles() -> None:
    assert rs.quartiles([]) == {"n": 0, "q25": None, "median": None, "q75": None}
    assert rs.quartiles([4.0, 1.0, 3.0, 2.0]) == {
        "n": 4,
        "q25": pytest.approx(1.75),
        "median": pytest.approx(2.5),
        "q75": pytest.approx(3.25),
    }


def test_check_prints_the_volatility_before_fast_exits_by_state() -> None:
    fast = [row for row in EXITS if row["leg"] == "fast"]
    for state in "RN":
        values = [
            row["vol63"] for row in fast if row["state"] == state and row["vol63"] is not None
        ]
        assert values
        assert RESULT["check"]["vol63"][state] == json.loads(json.dumps(rs.quartiles(values)))
    assert set(RESULT["check"]["vol63"]) == {"R", "N"}


def test_build_leaves_out_the_other_industry_like_gsy() -> None:
    assert RESULT["check"]["left_out"] == [] and BUILT.left_out == ()
    industries = BASE.industries[["Food"]].assign(Other=BASE.industries["Food"])
    built = rs.build(rs.Inputs(industries, BASE.factors, BASE.entries, "x"))
    assert built.left_out == ("Other",)
    assert [row["name"] for row in built.rows] == ["Food"] and list(built.industries) == ["Food"]
    check = rs.check_summary(built)
    assert check["left_out"] == ["Other"] and check["excluded"] == [] and check["used"] == 1


def test_the_read_has_every_part_and_dumps_as_json() -> None:
    assert set(RESULT) == {
        "run_id",
        "owner_decision",
        "snapshot_id",
        "rules_sha256",
        "check",
        "fidelity",
        "tables",
        "verdict",
        "reasons",
        "mostly_vol",
        *(["gap_ok", "halves_ok", "drop_ok"] if "gap_ok" in RESULT else []),
    }
    assert RESULT["verdict"] in rs.VERDICTS
    assert set(RESULT["tables"]) == {
        "main",
        "halves",
        "drop_year",
        "episode_weighted",
        "concentration",
        "vol",
        "protection",
        "tech",
        "slow",
    }
    json.dumps(RESULT, allow_nan=False)
    for row in EXITS:
        json.dumps(row, allow_nan=False)


def test_the_read_passes_the_verdict_to_the_vol_reading(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    def reading(decision: dict[str, Any], tables: dict[str, Any]) -> bool:
        seen.update(decision=decision, tables=tables)
        return True

    monkeypatch.setattr(rs, "vol_reading", reading)
    result, _ = rs.read(BASE)
    assert result["mostly_vol"] is True
    assert seen["decision"]["verdict"] == RESULT["verdict"]
    assert json.loads(json.dumps(seen["tables"])) == RESULT["tables"]


def test_data_after_2010_never_matter() -> None:
    factors, industries = FACTORS.copy(), INDUSTRIES.copy()
    after = industries.index > pd.Timestamp("2010-12-31")
    industries.loc[after] = -0.05
    factors.loc[after, "Mkt-RF"] = 0.3
    result, exits = rs.read(_inputs(factors, industries))
    assert result == RESULT and exits == EXITS


def test_run_up_state_never_looks_ahead() -> None:
    industries = INDUSTRIES.copy()
    later = industries.index > pd.Timestamp("1990-01-01")
    industries.loc[later, "Steel"] = industries.loc[later, "Steel"] + 0.01
    built = rs.build(_inputs(FACTORS, industries))
    before = BUILT.industries["Steel"].months.index <= P("1989-12", "M")
    pd.testing.assert_frame_equal(
        built.industries["Steel"].months.loc[before], BUILT.industries["Steel"].months.loc[before]
    )


# ----- report and commands -----------------------------------------------------------------------
def test_report_sentences() -> None:
    text = rs.render({**RESULT, "provenance": {"commit": "abc123"}})
    fid, main = RESULT["fidelity"], RESULT["tables"]["main"]
    assert text.startswith("# 行业暴涨以后的跌破可不可信（TRADING-2565-84）\n")
    assert f"## 判定：`{RESULT['verdict']}`" in text
    assert rs.READING[RESULT["verdict"]] in text
    assert "- 代码：abc123" in text
    assert (
        f"起点在 1929-01..2008-12 的暴涨段 {fid['episodes']} 段，24 个月内崩（回撤到 −40%）"
        f"{fid['crashes']} 段，{fid['share'] * 100:.1f}%；GSY 印的 40 段、21 段、53.0%。"
        "线：至少 15 段，比例在 30.0%–76.0%。"
    ) in text
    assert f"N − R（被来回打的比例）：{main['gap'] * 100:+.1f} 个百分点；线 ≥ 15 个百分点。" in text
    assert (
        "| 组 | 完整退出 | 被来回打 | 快腿得失中位（点） | 平均（点） | 合计（点） | 跟着真跌 |"
        in text
    )
    r = main["R"]
    row = f"| 暴涨（R） | {r['complete']} | {r['whipsaw'] * 100:.1f}% | "
    assert f"{row}{r['pnl_median'] * 100:+.2f} |" in text
    assert "### 1929–1969（" in text and "### 1970–2010（" in text
    assert "## 去掉一年" in text and "## 只印" in text and "## 数据和核对" in text
    assert "保护表（快腿只在暴涨时开，慢熊里丢多少的那一头；不判）：" in text
    assert "用了 6 个行业；不用的：Gold。" in text
    assert "| Gold | 1926-07-01 | 2010-12-31 | 6 | 不用（缺 6 天） | — |" in text
    assert "科技行业 Softw（次数少，只印）：" in text
    assert "## 局限" in text and text.endswith("production_effect=none；broker_action=none。\n")
    for verdict in rs.VERDICTS:
        assert (
            rs.render({**RESULT, "verdict": verdict, "provenance": {}}).count(rs.READING[verdict])
            == 1
        )
    reasons = rs.render({**RESULT, "verdict": "incomplete", "reasons": ["没有快腿退出"]})
    assert "原因：没有快腿退出。" in reasons


def test_report_vol_and_concentration_sentences() -> None:
    tables = {
        **RESULT["tables"],
        "vol": rs.vol_tiers(_vol_rows()),
        "concentration": {"pooled": 0.5, "by_episode": 0.65, "flag": True},
    }
    check = {**RESULT["check"], "left_out": ["Other"]}
    result = {**RESULT, "verdict": "holds", "tables": tables, "check": check, "mostly_vol": True}
    text = rs.render(result)
    assert "## 波动对照（只印，不进判定）\n" in text
    assert "完整的快腿退出按退出前 63 个交易日的年化波动三等分，切点 30.0%、60.0%。" in text
    assert (
        "| 波动档 | R 完整退出 | R 被来回打 | R 跟着真跌 | N 完整退出 | N 被来回打 | N 跟着真跌 |"
        in text
    )
    assert "| 低 | 2 | 50.0% | 50.0% | 1 | 100.0% | 0.0% |" in text
    assert "| 中 | 1 | 100.0% | — | 2 | 0.0% | 100.0% |" in text
    assert "| 高 | 1 | 0.0% | 100.0% | 2 | 100.0% | 0.0% |" in text
    gap = RESULT["tables"]["main"]["gap"]
    assert (
        "N 组按 R 组的波动分布加权：被来回打 75.0%（R 50.0%），N − R +25.0 个百分点，"
        f"合起来是 {'—' if gap is None else f'{gap * 100:+.1f} 个百分点'}；"
        "跟着真跌 R − N +66.7 个百分点。"
    ) in text
    assert f"{rs.READING['holds']}\n\n{rs.VOL_READING[True]}\n" in text
    assert "（合起来 50.0%）。差超过 10 个百分点：几段定的。" in text
    assert "照 GSY 不用的：Other。" in text
    vol = check["vol63"]
    assert (
        "快腿退出前 63 个交易日的年化波动（全部退出，含未完）："
        f"R {vol['R']['n']} 次，中位 {vol['R']['median'] * 100:.1f}%"
        f"（四分位 {vol['R']['q25'] * 100:.1f}%–{vol['R']['q75'] * 100:.1f}%）；"
        f"N {vol['N']['n']} 次，中位 {vol['N']['median'] * 100:.1f}%"
    ) in text
    calm = rs.render({**result, "mostly_vol": False})
    assert rs.VOL_READING[False] in calm and rs.VOL_READING[True] not in calm
    plain = rs.render(
        {
            **result,
            "mostly_vol": None,
            "check": RESULT["check"],
            "tables": {
                **tables,
                "vol": rs.vol_tiers([]),
                "concentration": {"pooled": 0.5, "by_episode": 0.55, "flag": False},
            },
        }
    )
    assert all(sentence not in plain for sentence in rs.VOL_READING.values())
    assert "完整的快腿退出不到 3 次，不分档。" in plain and "| 波动档 |" not in plain
    assert "（合起来 50.0%）。\n" in plain and "几段定的" not in plain
    assert "照 GSY 不用的：无。" in plain


def test_vol_reading_sentences() -> None:
    assert rs.VOL_READING == {
        True: "按波动加权以后，N − R 不到合起来的一半：多半是高波动的状态，不是暴涨本身；"
        "11-20 续期卡问的是这个。",
        False: "按波动加权以后，N − R 还有合起来的一半以上：不只是高波动。",
    }


def test_reading_sentences() -> None:
    assert rs.READING == {
        "incomplete": "没读：数据或核对出了问题，表照印，不判；修什么先回循环线程。",
        "fidelity_miss": "保真没过：我们的暴涨定义抓的不是 GSY 那种状态，表只印不读；"
        "`research/families.md` 新开「价格路径」一行，记「定义对不上 GSY」，停。",
        "too_few": "暴涨状态下的快腿退出太少，读不出东西；"
        "`research/families.md` 新开「价格路径」一行，记一句，停。",
        "holds": "暴涨以后的跌破更可信：11-20 续期卡问要不要在前瞻账本加一行「F0001 加暴涨状态」，"
        "连同保护表一起看；不开正式题。",
        "not_holds": "暴涨状态不让快腿的跌破更可信：`research/families.md` 新开「价格路径」一行，"
        "记一句，停。",
    }


def test_files(tmp_path: Path) -> None:
    rs.write_check(RESULT["check"], tmp_path / "c")
    assert json.loads((tmp_path / "c" / rs.CHECK).read_text(encoding="utf-8")) == RESULT["check"]
    assert sorted(p.name for p in (tmp_path / "c").iterdir()) == [rs.CHECK]
    rs.write_result(RESULT, EXITS, tmp_path / "r")
    lines = (tmp_path / "r" / rs.EXITS).read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == EXITS
    assert json.loads((tmp_path / "r" / rs.SUMMARY).read_text(encoding="utf-8")) == RESULT
    assert (tmp_path / "r" / rs.REPORT).read_text(encoding="utf-8") == rs.render(RESULT)


def test_commands(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(rs, "load_inputs", lambda: BASE)
    monkeypatch.setattr(rs, "git_provenance", lambda root: {"commit": "abc123"})
    runner = CliRunner()

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("check must not look past the exits")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(rs, "exit_rows", forbidden)
        patch.setattr(rs, "crashes", forbidden)
        out = runner.invoke(rs.app, ["check", "--out", str(tmp_path / "c")])
    assert out.exit_code == 0, out.output
    fast = RESULT["check"]["exits"]["fast"]
    assert (
        f"用了 6 个行业，不用 1 个；快腿退出 R {fast['R']}、N {fast['N']}、"
        f"不定 {fast['undetermined']}；暴涨段 {RESULT['check']['episodes_in_window']} 段"
        "（1929-01..2008-12）；"
        f"退出前 63 天波动中位 R {RESULT['check']['vol63']['R']['median'] * 100:.1f}%、"
        f"N {RESULT['check']['vol63']['N']['median'] * 100:.1f}%"
    ) in out.output
    assert sorted(p.name for p in (tmp_path / "c").iterdir()) == [rs.CHECK]
    out = runner.invoke(rs.app, ["run", "--out", str(tmp_path / "r")])
    assert out.exit_code == 0, out.output
    assert f"runup_state_table: 判定 {RESULT['verdict']}" in out.output
    assert sorted(p.name for p in (tmp_path / "r").iterdir()) == sorted(
        [rs.CHECK, rs.EXITS, rs.SUMMARY, rs.REPORT]
    )
    written = json.loads((tmp_path / "r" / rs.SUMMARY).read_text(encoding="utf-8"))
    assert written == {**RESULT, "provenance": {"commit": "abc123"}}
    assert (tmp_path / "r" / rs.CHECK).read_bytes() == (tmp_path / "c" / rs.CHECK).read_bytes()


def test_commands_stop_when_loading_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def broken() -> rs.Inputs:
        raise ValueError("no snapshot")

    monkeypatch.setattr(rs, "load_inputs", broken)
    for command in ("check", "run"):
        out = CliRunner().invoke(rs.app, [command, "--out", str(tmp_path / command)])
        assert out.exit_code == 1
        assert "runup_state_table: 数据载入失败：no snapshot" in out.output
        assert not (tmp_path / command).exists()
    monkeypatch.setattr(rs, "load_inputs", lambda: BASE)
    monkeypatch.setattr(
        rs, "market_series", lambda factors: (_ for _ in ()).throw(ValueError("bad market"))
    )
    out = CliRunner().invoke(rs.app, ["run", "--out", str(tmp_path / "m")])
    assert out.exit_code == 1 and "runup_state_table: 建数据失败：bad market" in out.output
    assert not (tmp_path / "m").exists()
