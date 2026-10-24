"""Acceptance tests for the tech-industry long-bull cost power probe (TRADING-2565-63).

Synthetic data only: no snapshot is read, no real rule is run. The probe measures a ruler with
planted rules (owner_decision:TRADING-2565:2026-10-24:longbull_cost_probe, charter p6 sec. 14),
and reuses TRADING-2565-61's cycle_power module unchanged.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import segments
from ai_trading_system.research_bench.settings import Window
from ai_trading_system.research_data import signal_sources
from ai_trading_system.research_probes import cycle_power as cp
from ai_trading_system.research_probes import longbull_power as lb

SOURCE = Path(lb.__file__).read_text(encoding="utf-8")
CYCLE_POWER_SHA256 = "53e5286fec515c99dcb264f4ce8959afb4c4c54d970ab24b2df5d4c3843efafc"


def _frames(
    seed: int = 1, vol: float = 0.012, end: str = "2012-06-29"
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Ken French shaped frames: decimal returns, Softw empty before 1965-07, rows past 2010."""

    rng = np.random.default_rng(seed)
    days = pd.bdate_range("1926-07-01", end)
    common = rng.normal(0.0006, vol, len(days))
    data = {
        name: common + rng.normal(0.0, extra, len(days))
        for name, extra in zip(cp.COLUMNS, (0.006, 0.009, 0.012), strict=True)
    }
    industries = pd.DataFrame(data, index=days)
    industries.loc[industries.index < pd.Timestamp("1965-07-01"), "Softw"] = np.nan
    industries["Food"] = 0.0
    excess = 0.7 * common + rng.normal(0.0, vol * 0.4, len(days))
    factors = pd.DataFrame(
        {"Mkt-RF": excess - 0.0001, "SMB": 0.0, "RF": np.full(len(days), 0.0001)}, index=days
    )
    return industries, factors


INDUSTRIES, FACTORS = _frames()
TECH = cp.market_frame(INDUSTRIES, FACTORS)
US = lb.us_frame(FACTORS)
FOUND, TAIL = cp.cycles(TECH)
US_FOUND, US_TAIL = cp.cycles(US)


# ----- constants: the requirement's sections 2 to 5, fixed before any run -------------------------
def test_constants() -> None:
    assert lb.RUN_ID == "TRADING-2565-63"
    assert lb.OWNER_DECISION == "owner_decision:TRADING-2565:2026-10-24:longbull_cost_probe"
    assert (lb.MARKET, lb.US) == ("Mkt-RF", "us_market")
    assert (lb.MONTHS, lb.HISTORIES, lb.DRAWS) == (984, 500, 1000)
    # The same seed as TRADING-2565-61: the same 500 tech histories (spec sec. 0 item 5).
    assert lb.SEED == cp.SEED == 20261024
    assert lb.RATES == (0.25, 0.5, 1.0)
    assert lb.TARGET_SD == 0.0124
    assert lb.LEVELS == (0.01, 0.0, -0.01, -0.0167, -0.025, -0.0334)
    assert (lb.POWER_LEVEL, lb.PRINT_LINE) == (-0.025, -0.0167)
    assert lb.RULERS == ("t_seg", "seg_boot")
    assert lb.MIN_SEGMENTS == 2
    assert (lb.CALIBRATION, lb.JOURNAL, lb.SUMMARY, lb.REPORT) == (
        "calibration.jsonl",
        "histories.jsonl",
        "summary.json",
        "report.md",
    )
    assert cp.CONFIDENCES == (0.95, 0.99, 0.997) and cp.VARIANTS == ("tech_ew", "three")
    assert (cp.FALSE_LIMIT, cp.WILSON_LIMIT, cp.SIGN_LIMIT) == (0.05, 0.075, 0.01)
    assert (cp.READY, cp.LARGE, cp.MIN_CYCLES) == (0.8, 0.5, 8)
    assert (lb.level_key(-0.0167), lb.level_key(0.0), lb.level_key(0.01)) == (
        "-1.67",
        "+0",
        "+1",
    )
    assert [lb.rate_key(r) for r in lb.RATES] == ["0.25", "0.5", "1"]


def test_cycle_power_is_unchanged() -> None:
    source = Path(cp.__file__).read_bytes()
    assert hashlib.sha256(source).hexdigest() == CYCLE_POWER_SHA256


# ----- data: the US market borrows the tech_ew column (spec sec. 0 item 3) ------------------------
def test_us_frame_puts_the_market_in_the_tech_column() -> None:
    assert list(US.columns) == ["tech_ew", "Hardw", "Chips", "Softw", "RF"]
    days = pd.DatetimeIndex(US.index)
    assert days[0] == pd.Timestamp("1929-01-03") and days[-1] == pd.Timestamp("2010-12-31")
    market = segments.window_returns(
        FACTORS["Mkt-RF"] + FACTORS["RF"],
        Window("x", "development", cp.FIRST_DAY, cp.LAST_DAY),
    )
    np.testing.assert_array_equal(US["tech_ew"].to_numpy(), market.to_numpy())
    assert US[list(cp.COLUMNS)].isna().all().all()
    np.testing.assert_array_equal(US["RF"].to_numpy(), FACTORS["RF"].reindex(market.index))
    factors = FACTORS.copy()
    factors.loc[pd.Timestamp("1950-06-01"), "RF"] = np.nan
    with pytest.raises(ValueError, match="missing return on 1950-06-01"):
        lb.us_frame(factors)  # the market is Mkt-RF + RF, so a missing RF is a missing return
    with pytest.raises(ValueError, match="missing columns"):
        lb.us_frame(FACTORS.drop(columns=["Mkt-RF"]))


def test_the_borrowed_column_goes_through_cycles_and_history_alone() -> None:
    # Cycles on the US frame are the cycles of the market series itself.
    level = segments.monthly_level(US["tech_ew"])
    slow = [s for s in segments.drop_segments(level) if s.kind == "slow_bear"]
    assert [c.end for c in US_FOUND] == [s.trough for s in slow]
    assert len(US_FOUND) >= cp.MIN_CYCLES and US_TAIL[-1] == "2010-12"
    # A synthetic history carries the market's own daily returns; the empty columns stay empty.
    made = cp.history(US, US_FOUND, np.random.default_rng(7), 240)
    calendar = pd.DatetimeIndex(US.index).to_period("M")
    ids = {c.id: c for c in US_FOUND}
    rows = [
        US.loc[(calendar >= ids[i].start) & (calendar <= ids[i].end), "tech_ew"] for i in made.drawn
    ]
    joined = np.concatenate([r.to_numpy() for r in rows])[: len(made.frame)]
    np.testing.assert_array_equal(made.frame["tech_ew"].to_numpy(), joined)
    assert made.frame[list(cp.COLUMNS)].isna().all().all()
    part = cp.market(made.frame, "tech_ew")
    assert len(part.rows) == len(made.frame)
    # Values put in the empty columns never reach the calibration.
    filled = US.copy()
    rng = np.random.default_rng(8)
    for name in cp.COLUMNS:
        filled[name] = rng.normal(0.0, 0.05, len(filled))
    for index in (0, 3):
        assert lb.calibration_history(filled, US_FOUND, 5, index, months=240) == (
            lb.calibration_history(US, US_FOUND, 5, index, months=240)
        )


def test_load_frames_reads_the_snapshot_once(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[Any, ...]] = []

    def fake(root: Path, snapshot_id: str) -> Any:
        calls.append((root, snapshot_id))
        return SimpleNamespace(
            snapshot_id=snapshot_id,
            frames={"ff_49_industries_daily": INDUSTRIES, "ff_factors_daily": FACTORS},
        )

    monkeypatch.setattr(signal_sources, "load_snapshot", fake)
    tech, us = lb.load_frames()
    pd.testing.assert_frame_equal(tech, TECH)
    pd.testing.assert_frame_equal(us, US)
    assert calls == [(PROJECT_ROOT, "sig-7ad8aa00b736")]


def test_nothing_after_2010_reaches_the_probe() -> None:
    industries, factors = INDUSTRIES.copy(), FACTORS.copy()
    after = pd.DatetimeIndex(industries.index) > pd.Timestamp("2010-12-31")
    industries.loc[after, list(cp.COLUMNS)] = 5.0
    factors.loc[pd.DatetimeIndex(factors.index) > pd.Timestamp("2010-12-31"), ["Mkt-RF", "RF"]] = (
        np.nan
    )
    pd.testing.assert_frame_equal(cp.market_frame(industries, factors), TECH)
    pd.testing.assert_frame_equal(lb.us_frame(factors), US)
    assert lb.real_long_bulls(cp.market_frame(industries, factors), lb.us_frame(factors)) == (
        lb.real_long_bulls(TECH, US)
    )


# ----- long bulls and the calendar-year reading (spec sec. 0 items 1, 2 and 7) --------------------
def test_long_bulls_follow_the_bench_segmentation() -> None:
    made = cp.history(TECH, FOUND, np.random.default_rng(5))
    found = 0
    for name in cp.MARKETS:
        part = cp.market(made.frame, name)
        level = cp.month_level(part.risky, part.month)
        bulls = lb.long_bulls(level)
        bench = segments.long_bull_segments(level, segments.drop_segments(level))
        first = level.index[0].ordinal
        assert bulls == [
            (
                pd.Period(s.start, freq="M").ordinal - first,
                pd.Period(s.end, freq="M").ordinal - first,
            )
            for s in bench
        ]
        assert all(last - start + 1 > 36 for start, last in bulls)
        found += len(bulls)
    assert found >= 8
    assert lb.long_bulls(pd.Series(dtype=float)) == []


def test_long_bulls_are_cut_by_fast_drops_too(monkeypatch: pytest.MonkeyPatch) -> None:
    level = pd.Series(
        np.linspace(1.0, 3.0, 240), index=pd.period_range("1929-01", periods=240, freq="M")
    )

    def fake(level: pd.Series, *args: Any, **kwargs: Any) -> list[segments.Segment]:
        return [
            segments.Segment("x", "fast_drop", "1934-02", "1934-04", "1934-01", "1934-04", 3, -0.2),
            segments.Segment(
                "y", "slow_bear", "1939-02", "1940-06", "1939-01", "1940-06", 17, -0.3
            ),
        ]

    monkeypatch.setattr(segments, "drop_segments", fake)
    # 1929-01..1934-01 (61 months), 1934-05..1939-01 (57 months), 1940-07..1948-12 (102 months).
    assert lb.long_bulls(level) == [(0, 60), (64, 120), (138, 239)]


def test_span_sums_years_and_readings_by_hand() -> None:
    values = np.array([0.01, 0.02, -0.01, 0.03, 0.0, 0.05])
    month = np.array([0, 0, 1, 1, 2, 3])
    spans = [(0, 1), (3, 3)]
    sums, days = lb.span_sums(values, month, spans)
    np.testing.assert_allclose(sums, [0.05, 0.05], rtol=1e-12)
    assert days.tolist() == [4, 1]
    np.testing.assert_allclose(lb.span_years(spans), [2 / 12, 1 / 12], rtol=1e-12)
    # Calendar years (months / 12), not 252 trading days.
    np.testing.assert_allclose(lb.readings(values, month, spans), [0.3, 0.6], rtol=1e-12)


def test_simple_differences_and_the_conversion_from_the_reading() -> None:
    rng = np.random.default_rng(41)
    month = np.repeat(np.arange(60), 21)
    strategy = np.log1p(rng.normal(0.0005, 0.012, len(month)))
    benchmark = np.log1p(rng.normal(0.0004, 0.010, len(month)))
    spans = [(0, 40), (43, 59)]
    got = lb.simple_differences(strategy, benchmark, month, spans)
    years = lb.span_years(spans)
    for i, (first, last) in enumerate(spans):
        inside = (month >= first) & (month <= last)
        own = np.prod(np.exp(strategy[inside])) ** (1 / years[i]) - 1
        reference = np.prod(np.exp(benchmark[inside])) ** (1 / years[i]) - 1
        assert got[i] == pytest.approx(own - reference, abs=1e-12)
        reading = lb.readings(strategy - benchmark, month, spans)[i]
        # In one segment: simple annual difference = (1 + benchmark annual) * (e^reading - 1).
        assert got[i] == pytest.approx((1 + reference) * math.expm1(reading), abs=1e-12)
        assert np.sign(got[i]) == np.sign(reading)


def _sim(seed: int, months: int = 150) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    month = np.repeat(np.arange(months), rng.integers(19, 24, months))
    risky = rng.normal(0.0004, 0.015, len(month))
    cash = np.full(len(month), 0.0001)
    targets = np.where(rng.random(months) < 0.3, 0.25, 1.0)
    gross, held = cp.simulate(risky, cash, month, targets)
    reference, _ = cp.simulate(risky, cash, month, np.full(months, held.mean()))
    return gross, reference, month


def test_the_shift_moves_each_reading_and_both_intervals_exactly() -> None:
    picks = lb.boot_picks(4, 300, np.random.default_rng(42))
    gross, reference, month = _sim(43)
    spans = [(0, 37), (45, 88), (90, 130), (133, 149)]
    base = lb.readings(np.log(gross) - np.log(reference), month, spans)
    bounds = lb.ruler_bounds(base, picks, cp.CONFIDENCES)
    assert bounds is not None and set(bounds) == {"t_seg", "seg_boot"}
    assert set(bounds["t_seg"]) == {"0.95", "0.99", "0.997"}
    for delta in (-0.025, 0.0125):
        moved_gross = lb.shift_long_bulls(gross, month, spans, delta)
        moved = lb.readings(np.log(moved_gross) - np.log(reference), month, spans)
        np.testing.assert_allclose(moved, base + delta, rtol=0, atol=1e-12)
        shifted = lb.ruler_bounds(moved, picks, cp.CONFIDENCES)
        assert shifted is not None
        for ruler, row in bounds.items():
            for key, (low, high) in row.items():
                assert shifted[ruler][key][0] == pytest.approx(low + delta, abs=1e-12)
                assert shifted[ruler][key][1] == pytest.approx(high + delta, abs=1e-12)
    # three: segments of several markets pooled; the same shift on each market moves the pool.
    pooled: list[float] = []
    moved_pool: list[float] = []
    for seed in (44, 45, 46):
        gross, reference, month = _sim(seed)
        spans = [(0, 60), (70, 149)]
        pooled.extend(lb.readings(np.log(gross) - np.log(reference), month, spans))
        shifted_gross = lb.shift_long_bulls(gross, month, spans, -0.0167)
        moved_pool.extend(lb.readings(np.log(shifted_gross) - np.log(reference), month, spans))
    six = lb.boot_picks(6, 200, np.random.default_rng(47))
    before = lb.ruler_bounds(np.array(pooled), six, (0.99,))
    after = lb.ruler_bounds(np.array(moved_pool), six, (0.99,))
    assert before is not None and after is not None
    for ruler in lb.RULERS:
        np.testing.assert_allclose(
            after[ruler]["0.99"], np.array(before[ruler]["0.99"]) - 0.0167, rtol=0, atol=1e-12
        )


def test_the_shift_leaves_slow_bear_days_alone() -> None:
    gross, reference, month = _sim(48)
    bulls = [(0, 40), (52, 110)]
    bears = [(41, 51), (111, 149)]
    moved = lb.shift_long_bulls(gross, month, bulls, -0.03)
    before = lb.readings(np.log(gross) - np.log(reference), month, bears)
    after = lb.readings(np.log(moved) - np.log(reference), month, bears)
    np.testing.assert_array_equal(after, before)
    outside = (month > 40) & (month < 52) | (month > 110)
    np.testing.assert_array_equal(moved[outside], gross[outside])
    # One day of a segment of y years and n days is multiplied by exp(delta * y / n).
    inside = month <= 40
    factor = math.exp(-0.03 * (41 / 12) / int(inside.sum()))
    np.testing.assert_allclose(moved[inside], gross[inside] * factor, rtol=1e-14)


# ----- the two rulers -----------------------------------------------------------------------------
def test_t_critical_matches_the_tables() -> None:
    for confidence, dof, value in (
        (0.95, 1, 12.706205),
        (0.95, 2, 4.302653),
        (0.95, 5, 2.570582),
        (0.99, 5, 4.032143),
        (0.99, 10, 3.169273),
        (0.95, 30, 2.042272),
        (0.99, 30, 2.749996),
    ):
        assert lb.t_critical(confidence, dof) == pytest.approx(value, abs=2e-6)
    # 0.997 against a numerical integral of the t density.
    for dof in (3, 6, 11, 20):
        x = lb.t_critical(0.997, dof)
        grid = np.linspace(-x, x, 400001)
        density = np.exp(
            math.lgamma((dof + 1) / 2)
            - math.lgamma(dof / 2)
            - 0.5 * math.log(dof * math.pi)
            - (dof + 1) / 2 * np.log1p(grid**2 / dof)
        )
        assert float(np.trapezoid(density, grid)) == pytest.approx(0.997, abs=1e-6)
        assert lb.t_coverage(x, dof) == pytest.approx(0.997, abs=1e-12)
    assert lb.t_coverage(0.0, 4) == 0.0
    with pytest.raises(ValueError):
        lb.t_critical(1.0, 5)
    with pytest.raises(ValueError):
        lb.t_coverage(1.0, 0)


def test_t_bounds_by_hand() -> None:
    values = np.array([-0.01, 0.02, 0.005, -0.03, 0.0])
    got = lb.t_bounds(values, (0.95, 0.99))
    scale = np.std(values, ddof=1) / math.sqrt(5)
    # Five segments: 4 degrees of freedom.
    for key, critical in (("0.95", 2.776445), ("0.99", 4.604095)):
        assert got[key][0] == pytest.approx(values.mean() - critical * scale, abs=1e-8)
        assert got[key][1] == pytest.approx(values.mean() + critical * scale, abs=1e-8)


def test_boot_picks_and_bounds() -> None:
    picks = lb.boot_picks(5, 1000, np.random.default_rng(51))
    assert picks.shape == (1000, 5) and picks.min() == 0 and picks.max() == 4
    values = np.array([0.03, -0.01, 0.02, 0.0, -0.04])
    got = lb.boot_bounds(values, picks, (0.95, 0.997))
    means = values[picks].mean(axis=1)
    assert got["0.95"] == [float(x) for x in np.quantile(means, [0.025, 0.975])]
    assert got["0.997"] == [float(x) for x in np.quantile(means, [0.0015, 0.9985])]
    with pytest.raises(ValueError):
        lb.boot_picks(0, 10, np.random.default_rng(1))


def test_fewer_than_two_segments_have_no_interval() -> None:
    picks = lb.boot_picks(1, 10, np.random.default_rng(52))
    assert lb.ruler_bounds(np.array([0.01]), picks, (0.95,)) is None
    assert lb.ruler_bounds(np.array([]), None, (0.95,)) is None
    assert lb.sample_sd(np.array([0.01])) is None
    assert lb.sample_sd(np.array([0.01, 0.03])) == pytest.approx(np.std([0.01, 0.03], ddof=1))


# ----- calibration on the US market (requirement section 3) ---------------------------------------
def test_choose_rate_takes_the_nearest_and_the_smaller_on_a_tie() -> None:
    assert lb.choose_rate({0.25: 0.009, 0.5: 0.0131, 1.0: 0.02}) == 0.5
    assert lb.choose_rate({0.25: 0.0110, 0.5: 0.0138, 1.0: 0.02}) == 0.25
    assert lb.choose_rate({0.25: 0.0104, 0.5: 0.0144, 1.0: 0.02}) == 0.25  # both 0.0020 away
    assert lb.choose_rate({0.25: None, 0.5: 0.03, 1.0: 0.05}) == 0.5
    assert lb.choose_rate({0.25: 0.012, 0.5: 0.013}, target=0.0126) == 0.5
    with pytest.raises(ValueError):
        lb.choose_rate({0.25: None, 0.5: None})


def test_a_calibration_history_by_hand() -> None:
    record = lb.calibration_history(US, US_FOUND, 99, 2, months=360)
    rng = np.random.default_rng([99, 2, 1])
    made = cp.history(US, US_FOUND, rng, 360)
    part = cp.market(made.frame, "tech_ew")
    level = cp.month_level(part.risky, part.month)
    bears, bulls = cp.slow_bears(level), lb.long_bulls(level)
    uniforms = rng.random(len(level) - 1)
    assert set(record) == {"history", "drawn", "slow_bears", "long_bulls", "sd", "error"}
    assert (record["history"], record["drawn"], record["error"]) == (2, list(made.drawn), None)
    assert (record["slow_bears"], record["long_bulls"]) == (len(bears), len(bulls))
    assert set(record["sd"]) == {"0.25", "0.5", "1"}
    for rate in lb.RATES:
        targets = cp.plant_targets(bears, len(level), uniforms, rate)
        gross, held = cp.simulate(part.risky, part.cash, part.month, targets)
        reference, _ = cp.simulate(
            part.risky, part.cash, part.month, np.full(len(level), held.mean())
        )
        differences = lb.simple_differences(np.log(gross), np.log(reference), part.month, bulls)
        expected = np.std(differences, ddof=1) if len(bulls) >= 2 else None
        assert record["sd"][lb.rate_key(rate)] == pytest.approx(expected, abs=1e-15)


def test_calibrate_takes_medians_over_histories_with_two_segments() -> None:
    records = [
        {"long_bulls": 3, "slow_bears": 9, "sd": {"0.25": 0.006, "0.5": 0.010, "1": 0.019}},
        {"long_bulls": 1, "slow_bears": 8, "sd": {"0.25": None, "0.5": None, "1": None}},
        {"long_bulls": 4, "slow_bears": 9, "sd": {"0.25": 0.008, "0.5": 0.012, "1": 0.021}},
        {"long_bulls": 5, "slow_bears": 10, "sd": {"0.25": 0.007, "0.5": 0.017, "1": 0.020}},
    ]
    result = lb.calibrate(records)
    assert result["rates"]["0.5"] == {
        "histories": 3,
        "median": pytest.approx(0.012),  # the median, not the mean 0.013
        "q25": pytest.approx(0.011),
        "q75": pytest.approx(0.0145),
    }
    assert result["rates"]["0.25"]["median"] == pytest.approx(0.007)
    assert result["selected"] == 0.5
    assert result["long_bulls"]["counts"] == {"1": 1, "3": 1, "4": 1, "5": 1}
    assert result["long_bulls"]["median"] == 3.5
    assert lb.calibrate([records[1]])["selected"] is None


# ----- the tech histories -------------------------------------------------------------------------
def test_f1_is_the_plant_base_of_trading_2565_61() -> None:
    ours = lb.evaluate_history(TECH, FOUND, cp.SEED, 3, months=240, draws=20)
    theirs = cp.evaluate_history(
        TECH, FOUND, cp.SEED, 3, months=240, draws=20, blocks=(21,), confidences=(0.95,)
    )
    assert ours["drawn"] == theirs["drawn"] and ours["days"] == theirs["days"]
    assert ours["slow_bears"] == theirs["slow_bears"]
    assert ours["matched_exposure"]["1"] == theirs["matched_exposure"]


@pytest.mark.parametrize(("index", "months"), [(4, 360), (0, 240)])
def test_a_tech_history_by_hand(index: int, months: int) -> None:
    record = lb.evaluate_history(
        TECH, FOUND, 99, index, months=months, draws=50, confidences=(0.99,)
    )
    rng = np.random.default_rng([99, index])
    made = cp.history(TECH, FOUND, rng, months)
    assert (record["history"], record["drawn"], record["days"], record["error"]) == (
        index,
        list(made.drawn),
        len(made.frame),
        None,
    )
    pooled: dict[str, dict[str, list[float]]] = {
        v: {lb.rate_key(r): [] for r in lb.RATES} for v in cp.VARIANTS
    }
    for name in cp.MARKETS:
        part = cp.market(made.frame, name)
        level = cp.month_level(part.risky, part.month)
        bears, bulls = cp.slow_bears(level), lb.long_bulls(level)
        assert record["slow_bears"][name] == len(bears)
        assert record["long_bulls"][name] == len(bulls)
        uniforms = rng.random(len(level) - 1)
        variant = "tech_ew" if name == "tech_ew" else "three"
        for rate in lb.RATES:
            targets = cp.plant_targets(bears, len(level), uniforms, rate)
            gross, held = cp.simulate(part.risky, part.cash, part.month, targets)
            reference, _ = cp.simulate(
                part.risky, part.cash, part.month, np.full(len(level), held.mean())
            )
            assert record["matched_exposure"][lb.rate_key(rate)][name] == held.mean()
            pooled[variant][lb.rate_key(rate)].extend(
                lb.readings(np.log(gross) - np.log(reference), part.month, bulls)
            )
    for variant in cp.VARIANTS:
        count = len(pooled[variant]["1"])
        assert record["segments"][variant] == count
        # A variant with fewer than two segments draws no bootstrap indices.
        picks = lb.boot_picks(count, 50, rng) if count >= 2 else None
        for key, values in pooled[variant].items():
            np.testing.assert_allclose(record["readings"][variant][key], values, atol=1e-15)
            assert record["point"][variant][key] == pytest.approx(np.mean(values), abs=1e-15)
            expected = lb.ruler_bounds(np.array(values), picks, (0.99,))
            assert record["bounds"][variant][key] == expected
    # three pools the three columns' segments; the second case has one tech_ew segment only.
    assert record["segments"]["three"] > record["segments"]["tech_ew"]
    assert (record["segments"]["tech_ew"] >= 2) == (index == 4)
    if index == 0:
        assert all(b is None for b in record["bounds"]["tech_ew"].values())


# ----- the reading written before the run (requirement section 5) ---------------------------------
def _record(point: float | None, rows: dict[tuple[str, float], tuple[float, float]]) -> Any:
    if point is None:
        return {"segments": {"tech_ew": 1}, "point": {"tech_ew": {"0.5": 0.01}}, "bounds": {}}
    bounds: dict[str, dict[str, list[float]]] = {}
    for (ruler, confidence), (low, high) in rows.items():
        bounds.setdefault(ruler, {})[f"{confidence:g}"] = [point + low, point + high]
    return {
        "segments": {"tech_ew": 6},
        "point": {"tech_ew": {"0.5": point}},
        "bounds": {"tech_ew": {"0.5": bounds}},
    }


def test_ruler_rows_and_choose() -> None:
    wide = (-0.02, 0.02)
    records = []
    for i in range(100):
        rows = {
            ("t_seg", 0.95): (0.005, 0.02) if i < 10 else wide,  # 10 false gains at 0
            ("t_seg", 0.99): (-0.012, 0.012),
            ("seg_boot", 0.95): (-0.012, 0.012),
            ("seg_boot", 0.99): (-0.03, 0.03),  # -2.5 reads [-0.055, 0.005]: never decided
        }
        records.append(_record(0.03 + (0.001 if i % 2 else -0.001), rows))
    records += [_record(None, {}) for _ in range(20)]  # one segment: undecided, still counted
    table = lb.ruler_rows(records, "tech_ew", "0.5", confidences=(0.95, 0.99))
    # c0 only from histories with at least two segments.
    assert table["centre"] == pytest.approx(0.03) and table["judged"] == 100
    assert table["spread"] == pytest.approx(np.std([0.029, 0.031] * 50, ddof=1))
    picked = {(r["ruler"], r["confidence"]): r for r in table["rows"]}
    assert list(picked) == [
        ("t_seg", 0.95),
        ("t_seg", 0.99),
        ("seg_boot", 0.95),
        ("seg_boot", 0.99),
    ]
    first = picked[("t_seg", 0.95)]
    assert first["false_rate"] == pytest.approx(10 / 120) and not first["eligible"]
    assert first["wilson_upper"] == pytest.approx(cp.wilson_upper(10, 120))
    assert set(first["shares"]) == {"+1", "+0", "-1", "-1.67", "-2.5", "-3.34"}
    second = picked[("t_seg", 0.99)]
    # Each record reads point - c0 + level: +-0.001 around the level, interval +-0.012.
    assert second["shares"]["-2.5"] == {"gain": 0.0, "loss": 100 / 120}
    assert second["shares"]["-1.67"] == {"gain": 0.0, "loss": 100 / 120}
    assert second["shares"]["-1"] == {"gain": 0.0, "loss": 0.0}
    assert second["shares"]["+0"] == {"gain": 0.0, "loss": 0.0}
    assert second["shares"]["+1"] == {"gain": 0.0, "loss": 0.0}
    assert (second["score"], second["reverse_rate"]) == (pytest.approx(100 / 120), 0.0)
    assert second["false_rate"] == 0.0 and second["eligible"]
    # Lower bound above -1.67 at 0: all 100 judged histories (-0.013 or -0.011), none of the rest.
    assert second["line_share"] == pytest.approx(100 / 120)
    assert picked[("seg_boot", 0.95)]["score"] == second["score"]
    assert picked[("seg_boot", 0.99)]["score"] == 0.0 and picked[("seg_boot", 0.99)]["eligible"]
    # Tie at 100/120 between t_seg 0.99 and seg_boot 0.95: t_seg wins.
    assert lb.choose(table["rows"]) == {
        "verdict": "ready",
        "ruler": "t_seg",
        "confidence": 0.99,
        "score": pytest.approx(100 / 120),
    }


def test_wilson_and_reverse_limits_at_500_histories() -> None:
    def row(false_hits: int, reverse: int = 0) -> dict[str, Any]:
        records = []
        for i in range(500):
            bounds = (-0.015, 0.015)
            if i < false_hits:
                bounds = (0.005, 0.03)
            elif i >= 500 - reverse:
                bounds = (0.026, 0.05)  # -2.5 reads [0.001, 0.025]: a gain, and a false one at 0
            records.append(_record(0.0, {("t_seg", 0.95): bounds, ("seg_boot", 0.95): wide}))
        rows = lb.ruler_rows(records, "tech_ew", "0.5", confidences=(0.95,))["rows"]
        assert [(r["ruler"], r["false_rate"]) for r in rows[1:]] == [("seg_boot", 0.0)]
        return rows[0]

    wide = (-0.015, 0.015)

    assert row(25)["eligible"] and not row(26)["eligible"]
    # 4 false judgements in 100: 4% is under 5%, but the Wilson upper bound is over 7.5%.
    few = [
        _record(0.0, {("t_seg", 0.95): (0.005, 0.03) if i < 4 else wide, ("seg_boot", 0.95): wide})
        for i in range(100)
    ]
    first = lb.ruler_rows(few, "tech_ew", "0.5", confidences=(0.95,))["rows"][0]
    assert first["false_rate"] == pytest.approx(0.04)
    assert first["wilson_upper"] > 0.075 and not first["eligible"]
    assert row(20, 5)["reverse_rate"] == pytest.approx(0.01) and row(20, 5)["eligible"]
    assert row(19, 6)["reverse_rate"] == pytest.approx(0.012) and not row(19, 6)["eligible"]


def _row(ruler: str, confidence: float, score: float, eligible: bool = True) -> dict[str, Any]:
    return {"ruler": ruler, "confidence": confidence, "score": score, "eligible": eligible}


def test_choose_verdicts_and_ties() -> None:
    assert lb.choose([_row("t_seg", 0.95, 0.9, False)]) == {
        "verdict": "none_eligible",
        "ruler": None,
        "confidence": None,
        "score": None,
    }
    assert lb.choose([_row("t_seg", 0.95, 0.8)])["verdict"] == "ready"
    assert lb.choose([_row("t_seg", 0.95, 0.79)])["verdict"] == "large_only"
    assert lb.choose([_row("t_seg", 0.95, 0.5)])["verdict"] == "large_only"
    assert lb.choose([_row("t_seg", 0.95, 0.49)])["verdict"] == "below_50"
    picked = lb.choose(
        [
            _row("seg_boot", 0.997, 0.7),
            _row("t_seg", 0.95, 0.7),
            _row("t_seg", 0.99, 0.7),
            _row("seg_boot", 0.95, 0.9, False),
        ]
    )
    assert (picked["ruler"], picked["confidence"]) == ("t_seg", 0.99)


# ----- the run ------------------------------------------------------------------------------------
SMALL = {"histories": 4, "draws": 40, "months": 360}


@pytest.fixture(scope="module")
def small() -> dict[str, Any]:
    return lb.run_probe(TECH, US, **SMALL)


def test_run_probe_on_a_small_synthetic_run(small: dict[str, Any], tmp_path: Path) -> None:
    result = lb.run_probe(TECH, US, out_dir=tmp_path, **SMALL)
    assert json.dumps(result, sort_keys=True) == json.dumps(small, sort_keys=True)
    assert (result["run_id"], result["owner_decision"], result["snapshot_id"]) == (
        lb.RUN_ID,
        lb.OWNER_DECISION,
        cp.SNAPSHOT_ID,
    )
    assert len(result["cycles"]["tech_ew"]) == len(FOUND)
    assert result["cycles"]["us_market"] == len(US_FOUND)
    assert result["tail"]["tech_ew"] == {"first": TAIL[0], "last": "2010-12", "months": len(TAIL)}
    assert result["errors"] == {"calibration": [], "histories": []}
    calibration = result["calibration"]
    assert set(calibration["rates"]) == {"0.25", "0.5", "1"}
    assert calibration["selected"] in lb.RATES
    for name, rows in (("calibration.jsonl", 4), ("histories.jsonl", 4)):
        lines = (tmp_path / name).read_text(encoding="utf-8").splitlines()
        assert [json.loads(line)["history"] for line in lines] == list(range(rows))
    for variant in cp.VARIANTS:
        item = result["variants"][variant]
        assert item["reading"]["verdict"] in {"ready", "large_only", "below_50", "none_eligible"}
        # All three f are simulated and tabled (requirement v1.1); the selected one is judged.
        assert set(item["tables"]) == {"0.25", "0.5", "1"}
        assert all(len(t["rows"]) == 6 for t in item["tables"].values())
    assert set(result["long_bull_counts"]) == {"tech_ew", "Hardw", "Chips", "Softw", "three"}
    assert set(result["real_long_bulls"]) == {"tech_ew", "Hardw", "Chips", "Softw", "us_market"}


def test_the_us_market_prints_no_cost(small: dict[str, Any], tmp_path: Path) -> None:
    lb.run_probe(TECH, US, out_dir=tmp_path, **SMALL)
    line = json.loads((tmp_path / "calibration.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert set(line) == {"history", "drawn", "slow_bears", "long_bulls", "sd", "error"}
    assert set(small["calibration"]) == {"rates", "selected", "long_bulls", "slow_bears"}
    for row in small["calibration"]["rates"].values():
        assert set(row) == {"histories", "median", "q25", "q75"}
    text = lb.render(small)
    section = text.split("## 定标（美股大盘，只用来选 f）")[1].split("## ")[0]
    assert "c₀" not in section and "us_market" not in "".join(
        line for line in text.splitlines() if line.startswith("## ") and "·" in line
    )


def test_the_same_seed_gives_the_same_bytes(small: dict[str, Any], tmp_path: Path) -> None:
    first, second = tmp_path / "a", tmp_path / "b"
    lb.write_result(small, first)
    lb.write_result(lb.run_probe(TECH, US, **SMALL), second)
    for name in ("summary.json", "report.md"):
        assert (first / name).read_bytes() == (second / name).read_bytes()
    other = lb.run_probe(TECH, US, seed=lb.SEED + 1, **SMALL)
    assert json.dumps(other["variants"], sort_keys=True) != json.dumps(
        small["variants"], sort_keys=True
    )


def test_any_failed_history_makes_the_run_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    real_tech, real_us = lb.evaluate_history, lb.calibration_history

    def flaky(frame: pd.DataFrame, found: Any, seed: int, index: int, **kwargs: Any) -> Any:
        if index == 1:
            raise RuntimeError("boom")
        return real_tech(frame, found, seed, index, **kwargs)

    monkeypatch.setattr(lb, "evaluate_history", flaky)
    result = lb.run_probe(TECH, US, **SMALL)
    assert result["errors"] == {
        "calibration": [],
        "histories": [{"history": 1, "error": "RuntimeError: boom"}],
    }
    assert {v["reading"]["verdict"] for v in result["variants"].values()} == {"incomplete"}

    def flaky_us(frame: pd.DataFrame, found: Any, seed: int, index: int, **kwargs: Any) -> Any:
        if index == 2:
            raise ValueError("bad\nsecond line")
        return real_us(frame, found, seed, index, **kwargs)

    monkeypatch.setattr(lb, "evaluate_history", real_tech)
    monkeypatch.setattr(lb, "calibration_history", flaky_us)
    result = lb.run_probe(TECH, US, **SMALL)
    assert result["errors"]["calibration"] == [{"history": 2, "error": "ValueError: bad"}]
    assert {v["reading"]["verdict"] for v in result["variants"].values()} == {"incomplete"}


@pytest.mark.parametrize("calm_market", ["tech", "us"])
def test_fewer_than_eight_cycles_runs_nothing(
    monkeypatch: pytest.MonkeyPatch, calm_market: str
) -> None:
    industries, factors = _frames(vol=0.003)
    tech = cp.market_frame(industries, factors) if calm_market == "tech" else TECH
    us = lb.us_frame(factors) if calm_market == "us" else US
    assert len(cp.cycles(tech if calm_market == "tech" else us)[0]) < 8

    def never(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("no history is evaluated")

    monkeypatch.setattr(lb, "evaluate_history", never)
    monkeypatch.setattr(lb, "calibration_history", never)
    result = lb.run_probe(tech, us, **SMALL)
    assert result["calibration"] is None
    assert {v["reading"]["verdict"] for v in result["variants"].values()} == {"incomplete"}
    assert "incomplete" in lb.render(result)


# ----- report and command -------------------------------------------------------------------------
def test_report_lines(small: dict[str, Any]) -> None:
    text = lb.render(small)
    selected = lb.rate_key(small["calibration"]["selected"])
    others = [lb.rate_key(r) for r in lb.RATES if lb.rate_key(r) != selected]
    lines = text.splitlines()
    for line in (
        "# 科技行业长牛代价的检验力探针（TRADING-2565-63）",
        "- 依据：`owner_decision:TRADING-2565:2026-10-24:longbull_cost_probe`"
        "（第 6 期章程第 14 节）。",
        "- 数据：信号快照 `sig-7ad8aa00b736`，Hardw、Chips、Softw、Mkt-RF 和 RF，"
        "只读到 2010-12-31。",
        "- 只用埋的规则；F0001、fast_slow 和别的已有配置都没有跑。不进台账、不算试验，k 不动。",
        "- 美股大盘只用来定标假退出的频率，不印、不判任何和代价大小有关的数。",
        "## 判定",
        f"选中的 f：{selected}（每年假退出的次数）。",
        "## 定标（美股大盘，只用来选 f）",
        "## 周期和真实的长牛段（只印）",
        "## 每条历史的长牛段个数",
        "production_effect=none；broker_action=none。",
    ):
        assert line in lines, line
    # The three f tables, for both variants; only the selected one is judged (requirement v1.1).
    for variant in cp.VARIANTS:
        assert f"## {variant} · f = {selected}（判的口径）" in lines
        for key in others:
            assert f"## {variant} · f = {key}（只印）" in lines
    assert any("换成对数点约 −1.45" in line and "偏松" in line for line in lines)
    assert any("下界 > −1.67" in line and "| 合格 |" in line for line in lines)
    assert "和 TRADING-2565-61 同一批历史" in text


def test_command_writes_the_four_files(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(lb, "load_frames", lambda: (TECH, US))
    monkeypatch.setattr(lb, "HISTORIES", 2)
    monkeypatch.setattr(lb, "DRAWS", 30)
    monkeypatch.setattr(lb, "MONTHS", 240)
    out = tmp_path / "out"
    result = CliRunner().invoke(lb.app, ["run", "--out", str(out)])
    assert result.exit_code == 0, result.output
    assert sorted(p.name for p in out.iterdir()) == [
        "calibration.jsonl",
        "histories.jsonl",
        "report.md",
        "summary.json",
    ]
    assert len((out / "histories.jsonl").read_text(encoding="utf-8").splitlines()) == 2
    summary = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    assert summary["parameters"]["histories"] == 2 and summary["parameters"]["draws"] == 30
    assert "longbull_power: calibration 2/2" in result.output
    assert "longbull_power: tech_ew 判定" in result.output
    assert "longbull_power: three 判定" in result.output


def test_command_reports_a_load_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def broken() -> tuple[pd.DataFrame, pd.DataFrame]:
        raise ValueError("signal snapshot is unchecked or failed")

    monkeypatch.setattr(lb, "load_frames", broken)
    result = CliRunner().invoke(lb.app, ["run", "--out", str(tmp_path / "out")])
    assert result.exit_code == 1
    assert "longbull_power: 数据载入失败：signal snapshot is unchecked or failed" in result.output
    assert not (tmp_path / "out").exists()


def test_the_probe_reuses_cycle_power_and_runs_no_real_rule() -> None:
    imports = [line for line in SOURCE.splitlines() if line.startswith(("import ", "from "))]
    assert "from ai_trading_system.research_probes import cycle_power as cp" in imports
    for banned in (
        "research_screens",
        "signal_strategy",
        "fixed_strategy",
        "research_bench.trial",
        "research_bench.ledger",
        "research_bench.prereg",
        "research_bench.run",
        "forward_ledger",
        "qqq",
    ):
        assert not any(banned in line for line in imports), banned
    for copied in ("def history", "def cycles", "def plant_targets", "def simulate", "def _drops"):
        assert copied not in SOURCE, copied
    assert "2011" not in SOURCE
