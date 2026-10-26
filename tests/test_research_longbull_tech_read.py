"""Acceptance tests for the one direct read of F0001's long-bull cost on tech (TRADING-2565-70).

TRADING-2565-71 changed only the US check's reference: the first run stopped at the check because
the seven two-decimal annual differences were hand-computed from TRADING-2565-34's one-decimal
segment totals, so the check now compares those printed totals and exposures (charter p6 sec. 17).

TRADING-2565-72: the second run stopped while building Softw's bench data, because a day inside
its history was missing. Every series now drops all its missing days, as the segmentation and
cycle_power do, and the segment check lists the dropped days. The synthetic data carry such gaps.

Synthetic data only: no snapshot is read. The fake signal snapshot stands in for
sig-7ad8aa00b736; the bench settings, trial T0001 and the QQQ-Q3 preregistration are the
repository's own files (owner_decision:TRADING-2565:2026-10-26:longbull_tech_read, charter p6
sec. 17). cycle_power, longbull_power, longbull_reprobe and longbull_realpath are unchanged.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.research_bench import segments, trial
from ai_trading_system.research_bench.data import BenchData, from_us_market
from ai_trading_system.research_bench.settings import resolve_window
from ai_trading_system.research_bench.sources import SourceSpec
from ai_trading_system.research_data import signal_sources
from ai_trading_system.research_data.signal_sources import SignalSnapshot
from ai_trading_system.research_probes import cycle_power as cp
from ai_trading_system.research_probes import longbull_power as lp
from ai_trading_system.research_probes import longbull_realpath as rr
from ai_trading_system.research_probes import longbull_reprobe as rp
from ai_trading_system.research_probes import longbull_tech_read as lt

SOURCE = Path(lt.__file__).read_text(encoding="utf-8")
UNCHANGED = {
    cp: "53e5286fec515c99dcb264f4ce8959afb4c4c54d970ab24b2df5d4c3843efafc",
    lp: "0fe7a2ecb0f9d9a8e276dce0b2b08f89be322d253d4d9b4029fcd7dc2230e904",
    rp: "3592f15ac24a1637ec0889b81ffd70eb117c3c98d3ccdecf30645b4fdd6df6ae",
    rr: "1366ad700daab3b0f918200fc796b2c7a687d68bf9f654d7898e27194725f729",
}
# Planted slow bears, by month: every series has drops and long bulls in between.
BEARS = (
    ("1929-09", "1932-05"),
    ("1937-03", "1938-02"),
    ("1946-06", "1947-04"),
    ("1957-08", "1958-01"),
    ("1966-02", "1966-09"),
    ("1973-01", "1974-08"),
    ("1981-01", "1981-12"),
    ("1990-06", "1990-12"),
    ("2000-04", "2002-08"),
    ("2007-11", "2009-01"),
)
SOFTW_FROM = pd.Timestamp("1965-07-01")
# Missing days inside a history (TRADING-2565-72): the real Softw lacks 1971-03-11.
GAPS = {"Hardw": ("1950-05-10",), "Softw": ("1971-03-11", "1983-06-15")}


def _frames(seed: int = 5) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Ken French shaped decimal returns from 1926-07 past 2010; Softw starts in 1965-07."""

    rng = np.random.default_rng(seed)
    days = pd.bdate_range("1926-07-01", "2012-06-29")
    months = days.to_period("M")
    drift = np.full(len(days), 0.0007)
    for first, last in BEARS:
        drift[(months >= pd.Period(first, "M")) & (months <= pd.Period(last, "M"))] = -0.0035
    common = drift + rng.normal(0.0, 0.008, len(days))
    factors = pd.DataFrame(
        {
            "Mkt-RF": 0.8 * common - 0.0001 + rng.normal(0.0, 0.003, len(days)),
            "SMB": 0.0,
            "RF": 0.0001,
        },
        index=days,
    )
    industries = pd.DataFrame(
        {
            name: 1.3 * common + rng.normal(0.0, 0.006, len(days))
            for name in ("Hardw", "Chips", "Softw", "Food")
        },
        index=days,
    )
    industries.loc[industries.index < SOFTW_FROM, "Softw"] = np.nan
    for name, gaps in GAPS.items():
        industries.loc[pd.DatetimeIndex(gaps), name] = np.nan
    return factors, industries


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


def _inputs(factors: pd.DataFrame, industries: pd.DataFrame) -> lt.Inputs:
    snapshot = _snapshot(factors, industries)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(signal_sources, "load_snapshot", lambda root, snapshot_id: snapshot)
        return lt.load_inputs()


FACTORS, INDUSTRIES = _frames()
BASE = _inputs(FACTORS, INDUSTRIES)
# With the real constants (TRADING-2565-34's printed totals and TRADING-2565-63's ids) synthetic
# data fail.
UNREAD = lt.read(BASE)
US = UNREAD["checks"]["us"]
TECH = UNREAD["checks"]["tech"]
PRINTED_KEYS = ("strategy", "benchmark", "exposure")
INJECT = {
    "US_LONG_BULLS": tuple(tuple(span) for span in US["long_bulls"]),
    "US_PRINTED": tuple(tuple(round(row[key], 3) for key in PRINTED_KEYS) for row in US["totals"]),
    "US_SD": round(US["sd"], 4),
    "US_ANCHORS": tuple((row["name"], row["value"]) for row in US["anchors"]),
    "TECH_LONG_BULLS": {name: tuple(row["found"]) for name, row in TECH["series"].items()},
}


def _with(patch: pytest.MonkeyPatch, **changes: Any) -> None:
    for name, value in {**INJECT, **changes}.items():
        patch.setattr(lt, name, value)


def _read(inputs: lt.Inputs = BASE, **changes: Any) -> dict[str, Any]:
    with pytest.MonkeyPatch.context() as patch:
        _with(patch, **changes)
        return lt.read(inputs)


READ = _read()
RETURNS = lt.series_returns(INDUSTRIES, FACTORS)
FRAME = cp.market_frame(INDUSTRIES, FACTORS)


def _pair(name: str) -> tuple[BenchData, lt.Pair]:
    data = lt.bench_data(RETURNS[name], cp.cut(FACTORS)["RF"])
    start = lt.window_start(data, BASE.entry)
    return data, lt.f0001_pair(data, BASE.entry, BASE.loaded, start)


# ----- constants: charter p6 sec. 17 and the spec's sections 3 to 5, fixed before the run ---------
def test_constants() -> None:
    assert lt.RUN_ID == "TRADING-2565-70"
    assert lt.OWNER_DECISION == "owner_decision:TRADING-2565:2026-10-26:longbull_tech_read"
    assert lt.SERIES == ("tech_ew", "Hardw", "Chips", "Softw")
    assert lt.VARIANTS == ("tech_ew", "three") == cp.VARIANTS
    assert lt.PRIMARY == "three"
    assert lt.CONFIDENCE == {"tech_ew": 0.99, "three": 0.997}
    assert lt.PRINT_LINE == lp.PRINT_LINE == -0.0167
    assert lt.PLANT_SE == {"tech_ew": 0.0192, "three": 0.0149}
    assert lt.US_LONG_BULLS == rr.LONG_BULLS
    # slow-bear rc-report, fast_slow on us_1929: segment returns and exposures, one-decimal percent
    assert lt.US_PRINTED == (
        (1.678, 1.469, 0.691),
        (1.170, 1.186, 0.793),
        (3.749, 3.737, 0.752),
        (0.958, 1.047, 0.737),
        (4.801, 5.747, 0.721),
        (4.338, 4.284, 0.773),
        (0.776, 0.742, 0.753),
    )
    assert lt.US_HAND == (0.0257, -0.0022, 0.0002, -0.0077, -0.0135, 0.0009, 0.0042)
    # slow-bear rc-report, the fast_slow JSON line, stages.stage1: full precision
    assert lt.US_ANCHORS == (
        ("mean_annual_difference", 0.0010994206923715666),
        ("worst_annual_difference", -0.01347890310397748),
        ("average_exposure", 0.6557715110992798),
    )
    assert lt.US_SD == lp.TARGET_SD == 0.0124
    assert (lt.PRINTED_STEP, lt.SD_STEP) == (0.0005, 0.0001)
    assert lt.US_REFERENCE == "TRADING-2565-34 印的七段段内收益和段内平均敞口（TRADING-2565-71）"
    assert not hasattr(lt, "US_SIMPLE") and not hasattr(lt, "HALF_STEP")
    assert lt.TECH_LONG_BULLS == {
        "tech_ew": tuple(
            f"long_bull_{m}"
            for m in ("194007", "194706", "196211", "197501", "198408", "199011", "200210")
        ),
        "Hardw": tuple(
            f"long_bull_{m}"
            for m in ("194007", "194705", "196207", "197410", "198110", "199308", "200210")
        ),
        "Chips": tuple(
            f"long_bull_{m}"
            for m in ("194201", "194803", "196211", "197410", "198408", "199011", "200409")
        ),
        "Softw": ("long_bull_199010", "long_bull_200210"),
    }
    assert (lt.TOLERANCE, lt.SLACK) == (1e-12, 1e-12)
    assert (lt.SUMMARY, lt.REPORT) == ("tech_read.json", "tech_read.md")
    assert lt.VERDICTS == ("loss", "undecided", "gain", "incomplete")
    # The cause of the first run's stop: the seven hand values come from the printed totals with
    # years = months / 12, so they carry the totals' rounding; their SD is the 1.24 reference.
    for (first, last), (own, reference, _), hand in zip(
        lt.US_LONG_BULLS, lt.US_PRINTED, lt.US_HAND, strict=True
    ):
        months = pd.Period(last, "M").ordinal - pd.Period(first, "M").ordinal + 1
        value = (1 + own) ** (12 / months) - (1 + reference) ** (12 / months)
        assert round(value, 4) == hand
    assert abs(float(np.std(lt.US_HAND, ddof=1)) - lt.US_SD) <= lt.SD_STEP


def test_imported_modules_are_unchanged() -> None:
    for module, digest in UNCHANGED.items():
        assert hashlib.sha256(Path(str(module.__file__)).read_bytes()).hexdigest() == digest


def test_source_reuses_the_bench_and_never_touches_qqq_data() -> None:
    for line in (
        "from ai_trading_system.research_probes import cycle_power as cp",
        "from ai_trading_system.research_probes import longbull_power as lp",
        "from ai_trading_system.research_probes import longbull_realpath as rr",
        "from ai_trading_system.research_probes import backfill_check",
        "from ai_trading_system.research_probes import gate_power as gp",
    ):
        assert line in SOURCE
    for name in (
        "def readings",
        "def long_bulls",
        "def t_bounds",
        "def judge",
        "def simulate",
        "def from_us_market",
        "def segment_rows",
        "def market_frame",
        "def us_frame",
        "def sample_sd",
        "def signal_exposure",
    ):
        assert f"{name}(" not in SOURCE
    for text in ("2011", "qqq_long_history", "from_snapshot", "load_daily", "longbull_reprobe"):
        assert text not in SOURCE
    assert SOURCE.count("load_snapshot") == 1
    assert SOURCE.count("market_data(") == 1
    assert 'tr.market_data("us_market"' in SOURCE
    imports = [line for line in SOURCE.splitlines() if line.startswith(("import ", "from "))]
    for banned in ("strategies", "screen", "forward", "qqq"):
        assert not any(banned in line for line in imports), banned


# ----- loading ------------------------------------------------------------------------------------
def test_load_inputs_reads_only_the_named_frames_and_the_bench_us_market(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _snapshot(FACTORS, INDUSTRIES)
    asked: list[str] = []

    def load(root: Path, snapshot_id: str) -> SignalSnapshot:
        asked.append(snapshot_id)
        return snapshot

    monkeypatch.setattr(signal_sources, "load_snapshot", load)
    inputs = lt.load_inputs()
    assert asked == [cp.SNAPSHOT_ID, cp.SNAPSHOT_ID]
    assert list(inputs.industries.columns) == ["Hardw", "Chips", "Softw"]
    assert list(inputs.factors.columns) == ["Mkt-RF", "RF"]
    for frame in (inputs.industries, inputs.factors, inputs.bench_us.returns):
        assert frame.index.max() <= pd.Timestamp("2010-12-31")
        assert frame.index.min() == pd.Timestamp("1926-07-01")
    assert inputs.bench_us.source["market"] == "us_market"
    assert (inputs.window.start, inputs.window.end) == (date(1929, 1, 2), date(2010, 12, 31))
    assert inputs.window.window_id == "us_market_1929_2010"
    assert inputs.entry.id == "fast_slow"
    assert inputs.entry.rebalance.schedule == "month_end"
    assert inputs.entry.rebalance.band == 0.0
    assert inputs.loaded.cost_bps == 4.0


def test_load_inputs_stops_on_another_snapshot_or_window(monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot = _snapshot(FACTORS, INDUSTRIES)
    monkeypatch.setattr(signal_sources, "load_snapshot", lambda root, snapshot_id: snapshot)
    load_trial = trial.load_trial

    def other_snapshot(root_dir: Path, trial_id: str) -> Any:
        gate, plan = load_trial(root_dir, trial_id)
        data = plan.data.model_copy(update={"signal_snapshot_id": "sig-000000000000"})
        return gate, plan.model_copy(update={"data": data})

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(trial, "load_trial", other_snapshot)
        with pytest.raises(ValueError, match="T0001 does not use sig-7ad8aa00b736"):
            lt.load_inputs()
    monkeypatch.setattr(
        lt,
        "resolve_window",
        lambda *args: dataclasses.replace(resolve_window(*args), start=date(1930, 1, 2)),
    )
    with pytest.raises(ValueError, match=r"us_market_1929_2010 is not 1929-01-02\.\.2010-12-31"):
        lt.load_inputs()


def test_series_are_cut_and_tech_ew_is_the_market_frame_mean() -> None:
    assert set(RETURNS) == set(lt.SERIES)
    for values in RETURNS.values():
        assert values.index.max() == pd.Timestamp("2010-12-31")
        assert not values.isna().any()
    assert RETURNS["Softw"].index[0] == SOFTW_FROM
    assert RETURNS["tech_ew"].index[0] == pd.Timestamp("1926-07-01")
    inside = FRAME.index
    pd.testing.assert_series_equal(
        RETURNS["tech_ew"].reindex(inside), FRAME["tech_ew"], check_names=False
    )
    cut = INDUSTRIES.loc[INDUSTRIES.index <= pd.Timestamp("2010-12-31")]
    assert RETURNS["tech_ew"].index.equals(cut.index)
    before = RETURNS["tech_ew"].index < SOFTW_FROM
    np.testing.assert_allclose(
        RETURNS["tech_ew"][before], cut.loc[before, ["Hardw", "Chips"]].mean(axis=1)
    )
    after = RETURNS["tech_ew"].index >= SOFTW_FROM
    np.testing.assert_allclose(
        RETURNS["tech_ew"][after], cut.loc[after, ["Hardw", "Chips", "Softw"]].mean(axis=1)
    )
    for name in cp.COLUMNS:
        column = cut[name].loc[cut[name].first_valid_index() :]
        assert RETURNS[name].index.equals(column.dropna().index)
        np.testing.assert_array_equal(RETURNS[name].to_numpy(), column.dropna().to_numpy())
        for day in GAPS.get(name, ()):
            assert pd.Timestamp(day) in column.index and pd.Timestamp(day) not in RETURNS[name]
    assert len(RETURNS["Softw"]) == len(cut.loc[SOFTW_FROM:]) - 2
    when = pd.Timestamp("1950-05-10")
    assert RETURNS["tech_ew"][when] == cut.at[when, "Chips"]
    with pytest.raises(ValueError, match="missing columns"):
        lt.series_returns(INDUSTRIES.drop(columns="Chips"), FACTORS)


def test_missing_days_inside_each_history_are_listed() -> None:
    assert lt.missing_days(INDUSTRIES) == {
        "tech_ew": [],
        "Hardw": ["1950-05-10"],
        "Chips": [],
        "Softw": ["1971-03-11", "1983-06-15"],
    }
    assert list(lt.missing_days(INDUSTRIES)) == list(lt.SERIES)
    industries = INDUSTRIES.copy()
    industries.loc[pd.Timestamp("2011-03-10"), "Chips"] = np.nan
    industries.loc[pd.date_range("1966-02-01", "1966-02-03"), list(cp.COLUMNS)] = np.nan
    industries.loc[pd.Timestamp("1980-01-03"), "Softw"] = np.inf
    days = lt.missing_days(industries)
    assert days["tech_ew"] == ["1966-02-01", "1966-02-02", "1966-02-03"]
    assert days["Chips"] == days["tech_ew"]
    assert days["Softw"] == ["1966-02-01", "1966-02-02", "1966-02-03", "1971-03-11", "1983-06-15"]
    returns = lt.series_returns(industries, FACTORS)
    for name in lt.SERIES:
        assert not returns[name].index.isin(pd.DatetimeIndex(days[name])).any()
    assert np.isinf(returns["Softw"][pd.Timestamp("1980-01-03")])
    assert np.isinf(returns["tech_ew"][pd.Timestamp("1980-01-03")])
    empty = INDUSTRIES.copy()
    empty["Chips"] = np.nan
    with pytest.raises(ValueError, match="Chips has no returns"):
        lt.missing_days(empty)
    with pytest.raises(ValueError, match="Chips has no returns"):
        lt.series_returns(empty, FACTORS)
    assert TECH["series"]["Softw"]["dropped"] == ["1971-03-11", "1983-06-15"]
    assert READ["checks"]["tech"]["series"]["Hardw"]["dropped"] == ["1950-05-10"]
    for row in TECH["series"].values():
        assert set(row) == {"found", "expected", "match", "dropped"}


def test_bench_data_is_the_bench_us_market_construction() -> None:
    cut = cp.cut(FACTORS)
    own = lt.bench_data(cut["Mkt-RF"] + cut["RF"], cut["RF"])
    bench = from_us_market({cp.FACTORS: cut}, snapshot_id=cp.SNAPSHOT_ID)
    np.testing.assert_allclose(own.returns.to_numpy(), bench.returns.to_numpy(), atol=1e-15)
    numeric = ["qqq_return", "ndx_return", "ndx_price", "ndx_tr", "cash_rate_known"]
    np.testing.assert_allclose(
        own.inputs[numeric].to_numpy(), bench.inputs[numeric].to_numpy(), atol=1e-12
    )
    hardw = lt.bench_data(RETURNS["Hardw"], cut["RF"])
    np.testing.assert_allclose(hardw.returns["qqq"], RETURNS["Hardw"], atol=1e-15)
    np.testing.assert_allclose(hardw.inputs["ndx_return"], RETURNS["Hardw"], atol=1e-15)
    np.testing.assert_allclose(hardw.returns["cash"], cut["RF"].reindex(RETURNS["Hardw"].index))
    with pytest.raises(ValueError, match="missing RF on 1930-01-02"):
        lt.bench_data(RETURNS["Hardw"], cut["RF"].drop(pd.Timestamp("1930-01-02")))
    broken = RETURNS["Chips"].copy()
    broken.iloc[5000] = np.nan
    with pytest.raises(ValueError, match="missing or non-finite"):
        lt.bench_data(broken, cut["RF"])


# ----- the window and the F0001 pair --------------------------------------------------------------
def test_window_start_is_1929_or_the_day_after_the_first_executable_month_end() -> None:
    cut = cp.cut(FACTORS)
    us = lt.bench_data(cut["Mkt-RF"] + cut["RF"], cut["RF"])
    assert lt.window_start(us, BASE.entry) == date(1929, 1, 2)
    softw = lt.bench_data(RETURNS["Softw"], cut["RF"])
    targets = lt.f0001_targets(softw, BASE.entry)
    index = pd.DatetimeIndex(softw.returns.index)
    month_end = np.r_[index.to_period("M")[:-1] != index.to_period("M")[1:], True]
    first = int(np.flatnonzero(month_end & np.isfinite(targets.to_numpy()))[0])
    assert first >= 251  # tsmom_252 needs a full year of Softw returns.
    assert lt.window_start(softw, BASE.entry) == index[first + 1].date()
    short = lt.bench_data(RETURNS["Softw"].iloc[:200], cut["RF"])
    with pytest.raises(ValueError, match="F0001 has no executable target"):
        lt.window_start(short, BASE.entry)


def test_f0001_runs_on_the_days_the_segmentation_uses() -> None:
    data, pair = _pair("Softw")
    dates = pd.DatetimeIndex(pair.strategy.returns.index)
    column = FRAME["Softw"].dropna()
    assert dates.equals(column.index[column.index >= dates[0]])
    assert pd.Timestamp("1983-06-15") not in data.returns.index


def test_f0001_pair_is_the_bench_pair_and_costs_four_bps_on_both_sides() -> None:
    cut = cp.cut(FACTORS)
    us = lt.bench_data(cut["Mkt-RF"] + cut["RF"], cut["RF"])
    own = lt.f0001_pair(us, BASE.entry, BASE.loaded, date(1929, 1, 2))
    bench = lt.bench_pair(BASE.bench_us, BASE.entry, BASE.loaded, BASE.window)
    pd.testing.assert_frame_equal(own.strategy.daily, bench.strategy.daily, atol=1e-15, rtol=0)
    pd.testing.assert_frame_equal(own.benchmark.daily, bench.benchmark.daily, atol=1e-15, rtol=0)
    assert own.strategy.start == bench.strategy.start == pd.Timestamp("1929-01-02")
    assert own.strategy.returns.index[-1] == pd.Timestamp("2010-12-31")
    _, hardw = _pair("Hardw")
    mean = float(hardw.strategy.exposure.mean())
    assert hardw.benchmark.exposure.iloc[0] == pytest.approx(mean)
    assert 0.0 < mean < 1.0
    for sim in (hardw.strategy, hardw.benchmark):
        np.testing.assert_allclose(sim.daily["cost"], sim.daily["turnover"] * 4.0 / 10_000)
    assert hardw.strategy.daily["turnover"].gt(0).sum() > 10


# ----- segments and readings ----------------------------------------------------------------------
def test_cut_uses_the_shared_segmentation() -> None:
    for name in lt.SERIES:
        cut = lt.cut_of(FRAME[name])
        level = segments.monthly_level(FRAME[name].dropna())
        pd.testing.assert_series_equal(cut.level, level)
        drops = segments.drop_segments(level)
        assert cut.bulls == segments.long_bull_segments(level, drops)
        assert cut.bears == [s for s in drops if s.kind == "slow_bear"]
        assert cut.spans == lp.long_bulls(level)
        assert len(cut.bulls) >= 2
    assert str(lt.cut_of(FRAME["Softw"]).level.index[0]) == "1965-07"
    assert str(lt.cut_of(FRAME["tech_ew"]).level.index[0]) == "1929-01"


@pytest.mark.parametrize("name", ["tech_ew", "Softw"])
def test_readings_count_months_from_the_segmentation_level(name: str) -> None:
    _, pair = _pair(name)
    cut = lt.cut_of(FRAME[name])
    month = lt.month_offsets(pair.strategy.returns.index, cut.level)
    first_return = pd.Period(pair.strategy.returns.index[0], freq="M")
    assert month[0] == first_return.ordinal - pd.Period(cut.level.index[0], freq="M").ordinal
    if name == "Softw":
        assert month[0] >= 12  # F0001 on Softw trades about a year after Softw starts.
    values = lt.readings_of(pair, cut)
    own = np.log1p(pair.strategy.returns.to_numpy())
    reference = np.log1p(pair.benchmark.returns.to_numpy())
    months = pd.DatetimeIndex(pair.strategy.returns.index).to_period("M")
    expected = []
    for item in cut.bulls:
        days = (months >= pd.Period(item.start, "M")) & (months <= pd.Period(item.end, "M"))
        expected.append((own[days] - reference[days]).sum() / (item.months / 12))
    np.testing.assert_allclose(values, expected, rtol=1e-12, atol=1e-15)


def test_a_long_bull_before_f0001_trades_is_an_error() -> None:
    _, pair = _pair("Softw")
    cut = lt.cut_of(FRAME["Softw"])
    first = int(lt.month_offsets(pair.strategy.returns.index, cut.level)[0])
    early = segments.Segment(
        "long_bull_196507", "long_bull", "1965-07", "1968-11", None, None, 41, 0.1
    )
    # The month F0001 starts in is not a full month; the next one is.
    for start, fails in ((0, True), (first, True), (first + 1, False)):
        bad = lt.Cut(cut.level, [early], [], [(start, start + 40)])
        if fails:
            with pytest.raises(
                ValueError, match="long_bull_196507 starts before F0001's first full month"
            ):
                lt.readings_of(pair, bad)
        else:
            assert len(lt.readings_of(pair, bad)) == 1


def test_bull_and_bear_rows_use_segment_rows() -> None:
    _, pair = _pair("Softw")
    cut = lt.cut_of(FRAME["Softw"])
    values = lt.readings_of(pair, cut)
    rows = lt.bull_rows(pair, cut, values)
    reference = segments.segment_rows(pair.strategy, pair.benchmark, cut.bulls)
    assert [row["id"] for row in rows] == [item.id for item in cut.bulls]
    for row, item, value, ref in zip(rows, cut.bulls, values, reference, strict=True):
        assert set(row) == {
            "id",
            "start",
            "end",
            "months",
            "reading",
            "simple",
            "exposure",
            "whole",
        }
        assert (row["start"], row["end"], row["months"]) == (item.start, item.end, item.months)
        assert row["reading"] == value
        assert row["simple"] == ref["annual_difference"]
        assert row["exposure"] == ref["average_exposure"]
        assert row["whole"] == pytest.approx(pair.strategy.exposure.mean())
    bears, uncovered = lt.bear_rows(pair, cut)
    first = pd.Period(pair.strategy.returns.index[0], freq="M")
    assert uncovered and all(
        pd.Period(s.start, freq="M") <= first for s in cut.bears if s.id in uncovered
    )
    assert len(bears) + len(uncovered) == len(cut.bears)
    covered = [s for s in cut.bears if s.id not in uncovered]
    expected = segments.segment_rows(pair.strategy, pair.benchmark, covered)
    for row, ref in zip(bears, expected, strict=True):
        assert set(row) == {"id", "start", "end", "strategy", "benchmark", "difference", "exposure"}
        assert row["difference"] == ref["difference"]
        assert row["strategy"] == ref["strategy_return"]
    month = pd.Period(pair.strategy.returns.index[0], freq="M")
    edge = segments.Segment(
        "slow_bear_edge", "slow_bear", str(month), str(month + 2), None, None, 3, -0.2
    )
    later = segments.Segment(
        "slow_bear_later", "slow_bear", str(month + 1), str(month + 3), None, None, 3, -0.2
    )
    rows, missed = lt.bear_rows(pair, lt.Cut(cut.level, [], [edge, later], []))
    assert [row["id"] for row in rows] == ["slow_bear_later"] and missed == ["slow_bear_edge"]


# ----- checks before any tech read ----------------------------------------------------------------
def test_us_check_passes_with_the_printed_values() -> None:
    with pytest.MonkeyPatch.context() as patch:
        _with(patch)
        us = lt.us_check(BASE)
    assert us["passed"] is True
    assert us["bulls_match"] and us["same_dates"] and us["totals_match"] and us["sd_match"]
    assert us["anchors_match"] is True
    assert "simple_match" not in us and "expected" not in us
    assert us["start"] == us["bench_start"] == "1929-01-02"
    assert all(value <= lt.TOLERANCE for value in us["gaps"].values())
    assert us["printed_exposure"] == [0.656, 0.747]
    count = len(INJECT["US_PRINTED"])
    assert len(us["totals"]) == len(us["simple"]) == len(us["long_bulls"]) == count
    assert us["hand"] == list(lt.US_HAND) and us["sd_reference"] == INJECT["US_SD"]
    factors = cp.cut(FACTORS)
    data = lt.bench_data(factors["Mkt-RF"] + factors["RF"], factors["RF"])
    pair = lt.f0001_pair(data, BASE.entry, BASE.loaded, date(1929, 1, 2))
    cut = lt.cut_of(lp.us_frame(factors)["tech_ew"])
    rows = segments.segment_rows(pair.strategy, pair.benchmark, cut.bulls)
    for total, row, printed in zip(us["totals"], rows, INJECT["US_PRINTED"], strict=True):
        assert total == {
            "strategy": row["strategy_return"],
            "benchmark": row["benchmark_return"],
            "exposure": row["average_exposure"],
            "printed": list(printed),
            "match": True,
        }
    assert us["simple"] == [row["annual_difference"] for row in rows]
    exposure = pair.strategy.exposure
    summary = segments.long_bull_summary(rows)
    assert us["anchors"] == [
        {
            "name": name,
            "value": value,
            "printed": value,
            "difference": 0.0,
            "match": True,
        }
        for name, value in (
            ("mean_annual_difference", summary["mean_annual_difference"]),
            ("worst_annual_difference", min(us["simple"])),
            ("average_exposure", float(exposure.mean())),
        )
    ]
    months = pd.DatetimeIndex(exposure.index).to_period("M")
    inside = np.zeros(len(exposure), dtype=bool)
    for start, end in INJECT["US_LONG_BULLS"]:
        inside |= (months >= pd.Period(start, "M")) & (months <= pd.Period(end, "M"))
    assert us["exposure"] == pytest.approx(float(exposure.mean()), rel=1e-12)
    assert us["bull_exposure"] == pytest.approx(float(exposure[inside].mean()), rel=1e-12)


# Each case moves the printed numbers of a few (segment, column) cells by the given shift.
@pytest.mark.parametrize(
    "cells",
    [
        ((1, 0, 0.00049), (2, 1, -0.00049), (3, 2, 0.00049)),
        ((2, 0, 0.00051),),
        ((3, 1, -0.00051),),
        ((4, 2, 0.00051),),
    ],
)
def test_us_check_allows_half_a_printed_step(cells: tuple[tuple[int, int, float], ...]) -> None:
    printed = [list(row) for row in INJECT["US_PRINTED"]]
    for segment, column, shift in cells:
        printed[segment][column] = round(US["totals"][segment][PRINTED_KEYS[column]], 8) + shift
    failed = [segment for segment, _, shift in cells if abs(shift) > 0.0005]
    with pytest.MonkeyPatch.context() as patch:
        _with(patch, US_PRINTED=tuple(tuple(row) for row in printed))
        us = lt.us_check(BASE)
    assert us["totals_match"] is (not failed) and us["passed"] is (not failed)
    assert [i for i, row in enumerate(us["totals"]) if not row["match"]] == failed
    report = lt.render({**READ, "checks": {**READ["checks"], "us": us}})
    table = report.split("### 美股大盘")[1].split("- 简单年化差段间标准差")[0]
    assert [line.split(" | ")[0] for line in table.splitlines() if " | 否 | " in line] == [
        f"| {segment + 1}" for segment in failed
    ]


@pytest.mark.parametrize(
    "shifts",
    [(5e-13, -5e-13, 5e-13), (1e-11, 0.0, 0.0), (0.0, -1e-11, 0.0), (0.0, 0.0, 1e-11)],
)
def test_us_check_holds_the_full_precision_anchors(shifts: tuple[float, float, float]) -> None:
    anchors = tuple(
        (name, value + shift)
        for (name, value), shift in zip(INJECT["US_ANCHORS"], shifts, strict=True)
    )
    with pytest.MonkeyPatch.context() as patch:
        _with(patch, US_ANCHORS=anchors)
        us = lt.us_check(BASE)
    matches = [abs(shift) <= 1e-12 for shift in shifts]
    assert us["totals_match"] and us["sd_match"]
    assert us["anchors_match"] is all(matches) and us["passed"] is all(matches)
    assert [row["match"] for row in us["anchors"]] == matches
    for row, shift in zip(us["anchors"], shifts, strict=True):
        assert row["difference"] == pytest.approx(-shift, abs=1e-15)


def test_the_hand_values_are_only_printed() -> None:
    with pytest.MonkeyPatch.context() as patch:
        _with(patch, US_HAND=(0.5, -0.5, 0.5, -0.5, 0.5, -0.5, 0.5))
        us = lt.us_check(BASE)
    assert us["passed"] is True and us["hand"] == [0.5, -0.5, 0.5, -0.5, 0.5, -0.5, 0.5]
    with pytest.MonkeyPatch.context() as patch:
        _with(patch, US_PRINTED=INJECT["US_PRINTED"][:-1])
        us = lt.us_check(BASE)
    assert not us["totals_match"] and not us["passed"]
    assert [row["printed"] for row in us["totals"]][-1] is None


def test_us_check_fails_on_a_daily_gap_or_other_segments() -> None:
    returns = BASE.bench_us.returns.copy()
    qqq = returns["qqq"].to_numpy(dtype=float, copy=True)
    qqq[returns.index.get_loc(pd.Timestamp("1975-06-02"))] += 1e-9
    returns["qqq"] = qqq
    moved = dataclasses.replace(
        BASE,
        bench_us=BenchData(returns, BASE.bench_us.inputs, BASE.bench_us.source),
    )
    with pytest.MonkeyPatch.context() as patch:
        _with(patch)
        us = lt.us_check(moved)
    assert us["same_dates"] and us["gaps"]["return"] > lt.TOLERANCE and not us["passed"]
    later = dataclasses.replace(
        BASE, window=dataclasses.replace(BASE.window, start=date(1929, 1, 3))
    )
    with pytest.MonkeyPatch.context() as patch:
        _with(patch)
        us = lt.us_check(later)
    assert us["bench_start"] == "1929-01-03" and not us["same_dates"] and not us["passed"]
    assert us["gaps"] == {"exposure": None, "return": None, "benchmark": None}
    with pytest.MonkeyPatch.context() as patch:
        _with(patch, US_LONG_BULLS=INJECT["US_LONG_BULLS"][1:])
        assert not lt.us_check(BASE)["bulls_match"]
    with pytest.MonkeyPatch.context() as patch:
        _with(patch, US_SD=INJECT["US_SD"] + 0.0002)
        us = lt.us_check(BASE)
    assert not us["sd_match"] and not us["passed"]


def test_tech_check_compares_all_four_series() -> None:
    expected = dict(INJECT["TECH_LONG_BULLS"])
    expected["Softw"] = expected["Softw"][:-1]
    with pytest.MonkeyPatch.context() as patch:
        _with(patch, TECH_LONG_BULLS=expected)
        cuts, tech = lt.tech_check(BASE.industries, BASE.factors)
    assert set(cuts) == set(lt.SERIES)
    assert [name for name, row in tech["series"].items() if not row["match"]] == ["Softw"]
    assert tech["passed"] is False


def test_failed_checks_never_build_or_simulate_tech(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("tech series must not be read")

    monkeypatch.setattr(lt, "read_series", forbidden)
    result = lt.read(BASE)
    assert result["stage"] == "checks" and result["errors"] == []
    assert result["checks"]["us"]["passed"] is False
    assert result["series"] == {}
    assert all(item["verdict"] == "incomplete" for item in result["variants"].values())
    assert result["verdict"] == "incomplete"
    for name, value in INJECT.items():
        if name != "TECH_LONG_BULLS":
            monkeypatch.setattr(lt, name, value)
    result = lt.read(BASE)  # US passes, the TRADING-2565-63 ids do not.
    assert result["checks"]["us"]["passed"] and not result["checks"]["tech"]["passed"]
    assert result["stage"] == "checks" and result["series"] == {}
    monkeypatch.setattr(lt, "TECH_LONG_BULLS", INJECT["TECH_LONG_BULLS"])
    monkeypatch.setattr(lt, "US_SD", INJECT["US_SD"] + 0.0002)
    result = lt.read(BASE)  # the segments pass, the US check does not.
    assert result["checks"]["tech"]["passed"] and not result["checks"]["us"]["passed"]
    assert result["stage"] == "checks" and result["series"] == {}


def test_an_error_in_a_check_is_recorded_without_reading(monkeypatch: pytest.MonkeyPatch) -> None:
    _with(monkeypatch)

    def broken(inputs: lt.Inputs) -> dict[str, Any]:
        raise ValueError("no target is executable by 1929-01-02 at 0.5")

    monkeypatch.setattr(lt, "us_check", broken)
    result = lt.read(BASE)
    assert result["stage"] == "checks" and result["series"] == {}
    assert result["errors"] == ["checks: ValueError: no target is executable by 1929-01-02 at …"]


# ----- one error anywhere reads nothing -----------------------------------------------------------
def _no_numbers(result: dict[str, Any], out: Path) -> None:
    lt.write_result(result, out)
    assert result["series"] == {}
    assert all(item["verdict"] == "incomplete" for item in result["variants"].values())
    assert all(item["mean"] is None for item in result["variants"].values())
    report = (out / lt.REPORT).read_text(encoding="utf-8")
    assert "的长牛段" not in report and "的慢熊段" not in report and "## 只印" not in report
    assert "没读：核对没过或出错，科技行业上的 F0001 读数没有算；修什么先回循环线程。" in report
    for error in result["errors"]:
        assert not any(ch == "." for ch in error.split(": ", 2)[-1]), error


def test_a_data_error_in_one_series_reads_nothing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _with(monkeypatch)
    original = lt.bench_data
    built: list[Any] = []

    def bench_data(returns: pd.Series, cash: pd.Series) -> BenchData:
        built.append(returns.name)
        if returns.name == "Chips":
            raise ValueError("US market returns appear to be percent rather than daily decimals")
        return original(returns, cash)

    monkeypatch.setattr(lt, "bench_data", bench_data)
    monkeypatch.setattr(lt, "f0001_pair", lambda *a, **k: pytest.fail("no tech simulation"))
    with pytest.MonkeyPatch.context() as inner:
        inner.setattr(lt, "us_check", lambda inputs: {**US, "passed": True})
        result = lt.read(BASE)
    assert result["stage"] == "series"
    assert result["errors"] == [
        "Chips: ValueError: US market returns appear to be percent rather than daily decimals"
    ]
    assert built.count("Chips") == 1 and "Softw" in built
    _no_numbers(result, tmp_path)


def test_an_infinite_day_still_stops_building_the_data(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _with(monkeypatch)
    cuts, tech = lt.tech_check(BASE.industries, BASE.factors)
    industries = BASE.industries.copy()
    industries.loc[pd.Timestamp("1980-01-03"), "Softw"] = np.inf
    monkeypatch.setattr(lt, "us_check", lambda inputs: {**US, "passed": True})
    monkeypatch.setattr(lt, "tech_check", lambda industries, factors: (cuts, tech))
    monkeypatch.setattr(lt, "f0001_pair", lambda *a, **k: pytest.fail("no tech simulation"))
    result = lt.read(dataclasses.replace(BASE, industries=industries))
    assert result["stage"] == "series" and result["series"] == {}
    assert result["errors"] == [
        f"{name}: ValueError: US market returns have missing or non-finite values"
        for name in ("tech_ew", "Softw")
    ]
    assert all(item["mean"] is None for item in result["variants"].values())
    _no_numbers(result, tmp_path)


def test_an_error_while_reading_drops_every_reading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _with(monkeypatch)
    original = lt.readings_of
    calls: list[int] = []
    progressed: list[str] = []

    def readings_of(pair: lt.Pair, cut: lt.Cut) -> Any:
        calls.append(1)
        if len(calls) == 4:
            raise ValueError("bad 0.0123 and -1.5e-3 on 1990-01-02")
        return original(pair, cut)

    monkeypatch.setattr(lt, "readings_of", readings_of)
    result = lt.read(BASE, progress=progressed.append)
    assert len(calls) == 4
    assert progressed == list(lt.SERIES)
    assert result["stage"] == "series"
    assert result["errors"] == ["Softw: ValueError: bad … and … on 1990-01-02"]
    _no_numbers(result, tmp_path)


# ----- the read --------------------------------------------------------------------------------
def test_the_read_pools_three_and_judges_with_the_fixed_rulers() -> None:
    assert READ["stage"] == "read" and READ["errors"] == []
    series = READ["series"]
    assert list(series) == list(lt.SERIES)
    assert series["Softw"]["first_day"] == "1965-07-01"
    assert series["Softw"]["start"] > "1966-06-01"
    assert series["tech_ew"]["start"] == "1929-01-02"
    assert series["tech_ew"]["first_return"] == "1929-01-03"
    for name, item in series.items():
        assert [row["id"] for row in item["long_bulls"]] == list(INJECT["TECH_LONG_BULLS"][name])
    _, softw = _pair("Softw")
    assert series["Softw"]["exposure"] == float(softw.strategy.exposure.mean())
    assert series["Softw"]["days"] == len(softw.strategy.returns)
    assert series["Softw"]["first_return"] == softw.strategy.returns.index[0].date().isoformat()
    groups = {
        "tech_ew": series["tech_ew"]["long_bulls"],
        "three": [
            row for name in ("Hardw", "Chips", "Softw") for row in series[name]["long_bulls"]
        ],
    }
    for variant, rows in groups.items():
        item = READ["variants"][variant]
        values = np.array([row["reading"] for row in rows])
        confidence = lt.CONFIDENCE[variant]
        low, high = lp.t_bounds(values, (confidence,))[f"{confidence:g}"]
        assert item["segments"] == len(rows) >= 2
        assert item["confidence"] == confidence
        assert item["mean"] == pytest.approx(values.mean())
        assert (item["low"], item["high"]) == pytest.approx((low, high))
        assert item["verdict"] == cp.judge(low, high)
        assert item["line"] == (low > -0.0167)
        se = values.std(ddof=1) / math.sqrt(len(values))
        assert item["se"] == pytest.approx(se)
        assert item["ratio"] == pytest.approx(se / lt.PLANT_SE[variant])
        assert item["simple_sd"] == pytest.approx(np.std([row["simple"] for row in rows], ddof=1))
    assert READ["variants"]["three"]["segments"] == sum(
        len(series[name]["long_bulls"]) for name in ("Hardw", "Chips", "Softw")
    )
    assert READ["verdict"] == READ["variants"]["three"]["verdict"]
    params = READ["parameters"]
    assert params["us_reference"] == lt.US_REFERENCE and params["printed_step"] == 0.0005
    assert params["us_anchors"] == dict(INJECT["US_ANCHORS"])
    assert "half_step" not in params and params["sd_step"] == 0.0001
    assert params["cost_bps"] == 4.0 and params["config"] == "fast_slow"
    assert params["rebalance"] == "month_end" and params["primary"] == "three"
    assert (params["first_day"], params["last_day"]) == ("1929-01-02", "2010-12-31")
    json.dumps(READ, allow_nan=False)


@pytest.mark.parametrize(
    ("bounds", "verdict", "line"),
    [
        ((-0.03, -0.001), "loss", False),
        ((-0.01, 0.0), "undecided", True),
        ((0.0, 0.01), "undecided", True),
        ((0.001, 0.02), "gain", True),
        ((-0.0167, 0.01), "undecided", False),
    ],
)
def test_variant_verdicts_at_the_edges(
    monkeypatch: pytest.MonkeyPatch, bounds: tuple[float, float], verdict: str, line: bool
) -> None:
    seen: list[tuple[float, ...]] = []

    def t_bounds(values: Any, confidences: Any) -> dict[str, list[float]]:
        seen.append(tuple(confidences))
        return {f"{confidences[0]:g}": list(bounds)}

    monkeypatch.setattr(lp, "t_bounds", t_bounds)
    item = lt.variant_summary("three", [0.01, -0.02, 0.0], [0.01, 0.0, -0.01])
    assert seen == [(0.997,)]
    assert item["verdict"] == verdict and item["line"] is line
    assert lt.variant_summary("tech_ew", [0.01, 0.02], [0.0, 0.0])["confidence"] == 0.99
    one = lt.variant_summary("tech_ew", [0.01], [0.01])
    assert one["verdict"] == "incomplete" and one["segments"] == 1 and one["mean"] is None


def test_the_verdict_follows_three(monkeypatch: pytest.MonkeyPatch) -> None:
    _with(monkeypatch)
    bounds = {0.99: [-0.03, -0.01], 0.997: [0.01, 0.03]}

    def t_bounds(values: Any, confidences: Any) -> dict[str, list[float]]:
        return {f"{confidences[0]:g}": bounds[confidences[0]]}

    monkeypatch.setattr(lp, "t_bounds", t_bounds)
    result = lt.read(BASE)
    assert result["variants"]["tech_ew"]["verdict"] == "loss"
    assert result["variants"]["three"]["verdict"] == result["verdict"] == "gain"
    assert f"主读数 `three`：有收益。{lt.ACTION_TEXT['gain']}" in lt.render(result)


def test_data_after_2010_never_matter() -> None:
    factors, industries = _frames()
    later = factors.index > pd.Timestamp("2010-12-31")
    factors.loc[later, "Mkt-RF"] = 0.3
    factors.loc[later, "RF"] = 0.002
    industries.loc[later] = -0.4
    moved = _read(_inputs(factors, industries))
    assert json.dumps(moved, sort_keys=True) == json.dumps(READ, sort_keys=True)


# ----- report and command -------------------------------------------------------------------------
def test_report_sentences() -> None:
    report = lt.render(READ)
    verdict = lt.VERDICT_TEXT[READ["verdict"]]
    for line in (
        "# 在科技行业上直接读一次 F0001 的长牛代价（TRADING-2565-70）",
        "- 依据：`owner_decision:TRADING-2565:2026-10-26:longbull_tech_read`，"
        "第 6 期章程第 17 节。",
        "| 变体 | 段数 | 平均读数（对数点/年） | 区间 | 判定 | 下界 > −1.67（只印） |",
        f"主读数 `three`：{verdict}。{lt.ACTION_TEXT[READ['verdict']]}",
        "### 美股大盘",
        "- 长牛段和 TRADING-2565-34 的七段逐段一致。",
        "- 逐段对 TRADING-2565-34 印的段内收益和段内平均敞口（一位小数的百分数，"
        "线是半格 0.05 个百分点）；简单年化差的手算参照只印，不判（TRADING-2565-71）。",
        "| 段 | 首月 | 末月 | F0001 段内（-34 印的 / 重算） | 对照段内（-34 印的 / 重算） "
        "| 段内敞口（-34 印的 / 重算） | 一致 | 简单年化差（手算参照 / 重算） |",
        "- 结论：通过。",
        "### 划段（和 TRADING-2565-63 印的比）",
        "| 序列 | 长牛段 | 一致 | 去掉的缺值日 |",
        "口径不同，比值偏低：TRADING-2565-63 的 1.92、1.49 是 500 条历史上点估计的标准差，"
        "这里只有一条路径的段间 t 标准误，它比真实的窄（规格第 5 节）。",
        "### Softw 的长牛段",
        "### Softw 的慢熊段（F0001 省了多少，只印）",
        "production_effect=none；broker_action=none。",
    ):
        assert line in report, line
    assert report.endswith("production_effect=none；broker_action=none。\n")
    assert "没覆盖（F0001 还没开始）：" in report
    assert lt.VERDICT_TEXT == {
        "loss": "有代价",
        "undecided": "不定",
        "gain": "有收益",
        "incomplete": "没判",
    }
    us = READ["checks"]["us"]
    total, (first, last) = us["totals"][0], us["long_bulls"][0]
    cells = [
        f"{total['printed'][i] * 100:.1f}% / {total[key] * 100:.2f}%"
        for i, key in enumerate(PRINTED_KEYS)
    ]
    assert (
        f"| 1 | {first} | {last} | {' | '.join(cells)} | 是 | "
        f"{us['hand'][0] * 100:+.2f} / {us['simple'][0] * 100:+.2f} |"
    ) in report
    assert (
        f"- 简单年化差段间标准差：{us['sd'] * 100:.2f} 点；"
        f"手算参照 {us['sd_reference'] * 100:.2f} 点；一致：是。"
    ) in report
    anchors = "；".join(
        f"{lt.ANCHOR_TEXT[row['name']]} 重算 {row['value']:.10f}，"
        f"-34 印的 {row['printed']:.10f}，差 {row['difference']:.1e}"
        for row in us["anchors"]
    )
    assert f"- 全精度核对（TRADING-2565-34 的 JSON，线 1e-12）：{anchors}；一致：是。" in report
    assert lt.ANCHOR_TEXT == {
        "mean_annual_difference": "长牛年化差的平均",
        "worst_annual_difference": "长牛年化差的最差",
        "average_exposure": "整段平均敞口",
    }
    for name, dropped in (("tech_ew", "无"), ("Softw", "1971-03-11、1983-06-15")):
        ids = "、".join(key.removeprefix("long_bull_") for key in INJECT["TECH_LONG_BULLS"][name])
        assert f"| {name} | {ids} | 是 | {dropped} |" in report
    unread = lt.render(UNREAD)
    assert "- 结论：没通过。" in unread and "## 只印" not in unread
    assert "-34 印的 0.0010994207" in unread and "；一致：否。" in unread
    assert "| 是 |" not in unread.split("### 美股大盘")[1].split("### 划段")[0]
    assert "-34 印的 1.24" not in unread and "手算参照 1.24 点" in unread
    assert "主读数 `three`：没判。没读：核对没过或出错" in unread


def test_write_result(tmp_path: Path) -> None:
    out = tmp_path / "a" / "b"
    lt.write_result(READ, out)
    assert json.loads((out / lt.SUMMARY).read_text(encoding="utf-8")) == json.loads(
        json.dumps(READ)
    )
    assert (out / lt.REPORT).read_text(encoding="utf-8") == lt.render(READ)


def test_command_runs_once_and_prints_the_verdicts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _with(monkeypatch)
    monkeypatch.setattr(lt, "load_inputs", lambda: BASE)
    out = tmp_path / "run"
    result = CliRunner().invoke(lt.app, ["run", "--out", str(out)])
    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    assert lines[0].startswith("longbull_tech_read: 数据已载入并截到 2010-12-31；美股大盘 ")
    checked = lines.index("longbull_tech_read: 美股大盘核对通过；划段核对通过")
    assert lines[checked + 1] == (
        "longbull_tech_read: 去掉的缺值日 tech_ew 0 天、Hardw 1 天、Chips 0 天、Softw 2 天"
    )
    assert [line for line in lines if line.startswith("longbull_tech_read: 读完 ")] == [
        f"longbull_tech_read: 读完 {name}" for name in lt.SERIES
    ]
    three = READ["variants"]["three"]
    assert (
        f"longbull_tech_read: three 判定 {three['verdict']}，平均读数 {three['mean'] * 100:+.2f} 点"
    ) in lines
    assert lines[-1] == (f"longbull_tech_read: 写入 {out / lt.SUMMARY} 和 {out / lt.REPORT}")
    assert json.loads((out / lt.SUMMARY).read_text(encoding="utf-8"))["stage"] == "read"
    assert sorted(path.name for path in out.iterdir()) == sorted([lt.SUMMARY, lt.REPORT])


def test_command_stops_when_loading_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def broken() -> lt.Inputs:
        raise ValueError("signal snapshot is unchecked or failed")

    monkeypatch.setattr(lt, "load_inputs", broken)
    out = tmp_path / "run"
    result = CliRunner().invoke(lt.app, ["run", "--out", str(out)])
    assert result.exit_code == 1
    assert (
        "longbull_tech_read: 数据载入失败：signal snapshot is unchecked or failed" in result.output
    )
    assert not out.exists()


def test_command_prints_check_failures(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(lt, "load_inputs", lambda: BASE)
    out = tmp_path / "run"
    result = CliRunner().invoke(lt.app, ["run", "--out", str(out)])
    assert result.exit_code == 0, result.output
    assert "longbull_tech_read: 美股大盘核对没通过；划段核对没通过" in result.output
    assert "longbull_tech_read: 去掉的缺值日 tech_ew 0 天、Hardw 1 天" in result.output
    assert "longbull_tech_read: three 判定 incomplete，平均读数 – 点" in result.output
    assert "读完" not in result.output
    assert json.loads((out / lt.SUMMARY).read_text(encoding="utf-8"))["series"] == {}
