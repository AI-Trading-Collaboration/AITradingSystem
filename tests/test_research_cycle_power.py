"""Acceptance tests for the tech-industry cycle ruler power probe (TRADING-2565-61).

Synthetic data only: no snapshot is read, no real rule is run. The probe measures a ruler with
planted rules (owner_decision:TRADING-2565:2026-10-23:tech_proxy_power_probe, charter p6 sec. 13).
"""

from __future__ import annotations

import json
import math
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import load_settings, segments
from ai_trading_system.research_bench.falsify import stationary_bootstrap_indices
from ai_trading_system.research_data import signal_sources
from ai_trading_system.research_probes import cycle_power as cp

SOURCE = Path(cp.__file__).read_text(encoding="utf-8")


def _frames(
    seed: int = 1, vol: float = 0.013, end: str = "2012-06-29"
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Ken French shaped frames: decimal returns, Softw empty before 1965-07, rows past 2010."""

    rng = np.random.default_rng(seed)
    days = pd.bdate_range("1926-07-01", end)
    common = rng.normal(0.0004, vol, len(days))
    data = {
        name: common + rng.normal(0.0, extra, len(days))
        for name, extra in zip(cp.COLUMNS, (0.006, 0.009, 0.012), strict=True)
    }
    industries = pd.DataFrame(data, index=days)
    industries.loc[industries.index < pd.Timestamp("1965-07-01"), "Softw"] = np.nan
    industries["Food"] = 0.0
    factors = pd.DataFrame({"Mkt-RF": 0.0, "RF": np.full(len(days), 0.0001)}, index=days)
    return industries, factors


INDUSTRIES, FACTORS = _frames()
FRAME = cp.market_frame(INDUSTRIES, FACTORS)
FOUND, TAIL = cp.cycles(FRAME)


# ----- constants: the requirement's sections 2 to 5, fixed before any run -------------------------
def test_constants() -> None:
    assert cp.RUN_ID == "TRADING-2565-61"
    assert cp.OWNER_DECISION == "owner_decision:TRADING-2565:2026-10-23:tech_proxy_power_probe"
    assert cp.SNAPSHOT_ID == "sig-7ad8aa00b736"
    assert (cp.INDUSTRIES, cp.FACTORS) == ("ff_49_industries_daily", "ff_factors_daily")
    assert (cp.COLUMNS, cp.CASH, cp.TECH) == (("Hardw", "Chips", "Softw"), "RF", "tech_ew")
    assert cp.MARKETS == ("tech_ew", "Hardw", "Chips", "Softw")
    assert (str(cp.FIRST_DAY), str(cp.LAST_DAY)) == ("1929-01-02", "2010-12-31")
    assert (cp.MONTHS, cp.HISTORIES, cp.DRAWS) == (984, 500, 1000)
    assert isinstance(cp.SEED, int)
    assert cp.YEAR_DAYS == 252 == load_settings().settings.trading_days_per_year
    assert cp.BLOCKS == (21, 252, 1260)
    assert cp.CONFIDENCES == (0.95, 0.99, 0.997)
    assert cp.LEVELS == (-0.02, -0.01, 0.0, 0.01, 0.02)
    assert cp.VARIANTS == ("tech_ew", "three")
    assert (cp.HIGH, cp.LOW, cp.EXIT_MONTHS, cp.REENTRY_MONTHS, cp.FALSE_MONTHS) == (
        1.0,
        0.25,
        1,
        3,
        3,
    )
    assert cp.PLANTS == {"plant_base": 1.0, "plant_noisy": 2.0}
    assert cp.JUDGED_PLANT == "plant_base"
    assert cp.COST_BPS == 25.0
    assert (cp.FALSE_LIMIT, cp.WILSON_LIMIT, cp.SIGN_LIMIT) == (0.05, 0.075, 0.01)
    assert (cp.READY, cp.LARGE, cp.MIN_CYCLES) == (0.8, 0.5, 8)
    assert cp.WILSON_Z == pytest.approx(1.959963984540054)
    assert (cp.JOURNAL, cp.SUMMARY, cp.REPORT) == ("histories.jsonl", "summary.json", "report.md")


# ----- data: the cut, the window, tech_ew ---------------------------------------------------------
def test_market_frame_cuts_at_2010_and_opens_on_the_first_day() -> None:
    assert list(FRAME.columns) == ["tech_ew", "Hardw", "Chips", "Softw", "RF"]
    days = pd.DatetimeIndex(FRAME.index)
    # 1929-01-02 is the initial holding date (segments.window_returns), not a return date.
    assert days[0] == pd.Timestamp("1929-01-03")
    assert days[-1] == pd.Timestamp("2010-12-31")
    assert not (days > pd.Timestamp("2010-12-31")).any()
    early, late = pd.Timestamp("1950-06-01"), pd.Timestamp("1990-06-01")
    assert math.isnan(FRAME.loc[early, "Softw"])
    assert FRAME.loc[early, "tech_ew"] == pytest.approx(
        (INDUSTRIES.loc[early, "Hardw"] + INDUSTRIES.loc[early, "Chips"]) / 2
    )
    assert FRAME.loc[late, "tech_ew"] == pytest.approx(
        INDUSTRIES.loc[late, list(cp.COLUMNS)].mean()
    )
    assert FRAME.loc[late, "RF"] == FACTORS.loc[late, "RF"]
    cut = cp.cut(INDUSTRIES)
    assert pd.DatetimeIndex(cut.index).max() == pd.Timestamp("2010-12-31")
    assert len(cut) == int((pd.DatetimeIndex(INDUSTRIES.index) <= pd.Timestamp("2010-12-31")).sum())


def test_nothing_after_2010_reaches_the_probe() -> None:
    changed = INDUSTRIES.copy()
    after = pd.DatetimeIndex(changed.index) > pd.Timestamp("2010-12-31")
    changed.loc[after, list(cp.COLUMNS)] = np.nan
    factors = FACTORS.copy()
    factors.loc[pd.DatetimeIndex(factors.index) > pd.Timestamp("2010-12-31"), "RF"] = np.nan
    pd.testing.assert_frame_equal(cp.market_frame(changed, factors), FRAME)
    late = INDUSTRIES.copy()
    late.loc[after, "Hardw"] = 5.0
    pd.testing.assert_frame_equal(cp.market_frame(late, FACTORS), FRAME)


def test_market_frame_refuses_missing_cash_and_empty_days() -> None:
    factors = FACTORS.copy()
    factors.loc[pd.Timestamp("1950-06-01"), "RF"] = np.nan
    with pytest.raises(ValueError, match="missing RF on 1950-06-01"):
        cp.market_frame(INDUSTRIES, factors)
    industries = INDUSTRIES.copy()
    industries.loc[pd.Timestamp("1950-06-01"), list(cp.COLUMNS)] = np.nan
    with pytest.raises(ValueError, match="missing return on 1950-06-01"):
        cp.market_frame(industries, FACTORS)
    with pytest.raises(ValueError):
        cp.market_frame(INDUSTRIES.drop(columns=["Chips"]), FACTORS)


def test_load_frame_reads_the_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[Any, ...]] = []

    def fake(root: Path, snapshot_id: str) -> Any:
        calls.append((root, snapshot_id))
        return SimpleNamespace(
            snapshot_id=snapshot_id,
            frames={"ff_49_industries_daily": INDUSTRIES, "ff_factors_daily": FACTORS},
        )

    monkeypatch.setattr(signal_sources, "load_snapshot", fake)
    pd.testing.assert_frame_equal(cp.load_frame(), FRAME)
    assert calls == [(PROJECT_ROOT, "sig-7ad8aa00b736")]


# ----- cycles -------------------------------------------------------------------------------------
def test_cycles_follow_drop_segments() -> None:
    level = segments.monthly_level(FRAME["tech_ew"])
    drops = segments.drop_segments(level)
    slow = [s for s in drops if s.kind == "slow_bear"]
    fast = [s for s in drops if s.kind == "fast_drop"]
    assert len(slow) == len(FOUND) >= cp.MIN_CYCLES and fast
    months = pd.PeriodIndex(level.index, freq="M")
    begin = months[0]
    for k, (cycle, bear) in enumerate(zip(FOUND, slow, strict=True), start=1):
        assert cycle.id == f"cycle_{k:02d}"
        assert (cycle.start, cycle.end, cycle.peak) == (str(begin), bear.trough, bear.peak)
        assert cycle.change == pytest.approx(bear.change)
        trough = pd.Period(bear.trough, freq="M")
        assert cycle.months == trough.ordinal - begin.ordinal + 1
        begin = trough + 1
    # Fast drops stay inside a cycle; the tail after the last trough is not a cycle.
    for drop in fast:
        assert any(c.start <= drop.peak and drop.trough <= c.end for c in FOUND)
    assert TAIL == [str(m) for m in months if m >= begin]
    assert TAIL[-1] == "2010-12"


def test_cycles_call_the_bench_segmentation(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake(level: pd.Series, *args: Any, **kwargs: Any) -> list[segments.Segment]:
        return [
            segments.Segment(
                "x", "slow_bear", "1930-02", "1931-05", "1930-01", "1931-05", 16, -0.3
            ),
            segments.Segment("y", "fast_drop", "1932-02", "1932-04", "1932-01", "1932-04", 3, -0.2),
            segments.Segment(
                "z", "slow_bear", "1933-02", "1934-01", "1933-01", "1934-01", 12, -0.4
            ),
        ]

    monkeypatch.setattr(segments, "drop_segments", fake)
    found, tail = cp.cycles(FRAME)
    assert [(c.id, c.start, c.end, c.peak, c.months) for c in found] == [
        ("cycle_01", "1929-01", "1931-05", "1930-01", 29),
        ("cycle_02", "1931-06", "1934-01", "1933-01", 32),
    ]
    assert tail[0] == "1934-02" and tail[-1] == "2010-12"
    assert "def _drops" not in SOURCE and "def drop_segments" not in SOURCE


# ----- synthetic histories ------------------------------------------------------------------------
def test_a_history_has_exactly_984_months_of_whole_drawn_rows() -> None:
    made = cp.history(FRAME, FOUND, np.random.default_rng(3))
    frame = made.frame
    assert list(frame.columns) == ["tech_ew", "Hardw", "Chips", "Softw", "RF", "month"]
    month = frame["month"].to_numpy()
    assert (np.diff(month) >= 0).all() and set(np.unique(month)) == set(range(984))
    ids = {c.id: c for c in FOUND}
    assert all(item in ids for item in made.drawn)
    calendar = pd.DatetimeIndex(FRAME.index).to_period("M")
    rows = []
    for item in made.drawn:
        cycle = ids[item]
        rows.append(FRAME.loc[(calendar >= cycle.start) & (calendar <= cycle.end)])
    labels = np.concatenate([pd.DatetimeIndex(r.index).to_period("M").asi8 for r in rows])
    joined = pd.concat(rows, ignore_index=True)
    # Each synthetic month is one whole calendar month of a drawn cycle.
    changes = labels[1 : len(frame)] != labels[: len(frame) - 1]
    assert (np.diff(month) == 1).tolist() == changes.tolist()
    # The drawn cycles' rows, whole and in order (all five columns from the same day), cut at the
    # end of month 983.
    assert len(joined) >= len(frame)
    pd.testing.assert_frame_equal(
        frame.drop(columns="month"), joined.iloc[: len(frame)], check_dtype=False
    )
    assert (
        sum(ids[i].months for i in made.drawn[:-1]) < 984 <= sum(ids[i].months for i in made.drawn)
    )
    short = cp.history(FRAME, FOUND, np.random.default_rng(3), months=120)
    assert set(short.frame["month"]) == set(range(120))
    with pytest.raises(ValueError):
        cp.history(FRAME, [], np.random.default_rng(3))


def test_market_keeps_the_days_with_a_value_and_numbers_its_own_months() -> None:
    frame = pd.DataFrame(
        {
            "tech_ew": [0.01, 0.02, 0.03, 0.04, 0.05, 0.06],
            "Hardw": [0.01, 0.02, 0.03, 0.04, 0.05, 0.06],
            "Chips": [0.01, 0.02, 0.03, 0.04, 0.05, 0.06],
            "Softw": [np.nan, np.nan, 0.03, np.nan, 0.05, 0.06],
            "RF": [0.001] * 6,
            "month": [0, 0, 1, 2, 3, 3],
        }
    )
    part = cp.market(frame, "Softw")
    assert part.rows.tolist() == [2, 4, 5]
    assert part.month.tolist() == [0, 1, 1]
    assert part.risky.tolist() == [0.03, 0.05, 0.06]
    assert part.cash.tolist() == [0.001] * 3
    whole = cp.market(frame, "tech_ew")
    assert whole.rows.tolist() == list(range(6)) and whole.month.tolist() == [0, 0, 1, 2, 3, 3]


def test_month_level_and_slow_bears_use_the_bench_rule() -> None:
    made = cp.history(FRAME, FOUND, np.random.default_rng(5))
    part = cp.market(made.frame, "tech_ew")
    level = cp.month_level(part.risky, part.month)
    assert len(level) == 984
    assert str(level.index[0]) == "1929-01" and str(level.index[-1]) == "2010-12"
    gross = pd.Series(1.0 + part.risky).groupby(part.month).prod()
    np.testing.assert_allclose(level.to_numpy(), np.cumprod(gross.to_numpy()), rtol=1e-12)
    bears = cp.slow_bears(level)
    expected = [
        (
            pd.Period(s.peak, freq="M").ordinal - level.index[0].ordinal,
            pd.Period(s.trough, freq="M").ordinal - level.index[0].ordinal,
        )
        for s in segments.drop_segments(level)
        if s.kind == "slow_bear"
    ]
    assert bears == expected and bears


# ----- the planted rule and the simulation --------------------------------------------------------
def test_plant_targets() -> None:
    quiet = np.ones(19)
    # Slow bear peak 3, trough 8: LOW from month 3 + 1 + 1 = 5 to month 8 + 3 = 11.
    targets = cp.plant_targets([(3, 8)], 20, quiet, 1.0)
    assert [i for i, t in enumerate(targets) if t == 0.25] == list(range(5, 12))
    assert set(targets) == {0.25, 1.0}
    # A false exit started at the end of month 0 holds months 1 to 3.
    starts = quiet.copy()
    starts[0] = 0.0
    assert [i for i, t in enumerate(cp.plant_targets([], 20, starts, 1.0)) if t == 0.25] == [
        1,
        2,
        3,
    ]
    # The chance is rate / 12 per month end: 0.1 starts one at 2 a year, not at 1 a year.
    starts = quiet.copy()
    starts[10] = 0.1
    assert (cp.plant_targets([], 20, starts, 1.0) == 1.0).all()
    assert [i for i, t in enumerate(cp.plant_targets([], 20, starts, 2.0)) if t == 0.25] == [
        11,
        12,
        13,
    ]
    # None starts at the end of a month the slow-bear rule holds LOW; one started just before
    # it merges into it (union); one near the end is cut at the last month.
    starts = quiet.copy()
    starts[10], starts[4], starts[18] = 0.0, 0.0, 0.0
    low = [i for i, t in enumerate(cp.plant_targets([(3, 8)], 20, starts, 1.0)) if t == 0.25]
    assert low == [*range(5, 12), 19]
    with pytest.raises(ValueError):
        cp.plant_targets([], 20, np.ones(20), 1.0)


def test_simulate_drifts_within_the_month_and_trades_at_month_starts() -> None:
    risky = np.array([0.1, -0.05, 0.02, 0.0])
    cash = np.array([0.01, 0.01, 0.0, 0.0])
    month = np.array([0, 0, 1, 1])
    gross, held = cp.simulate(risky, cash, month, np.array([0.5, 1.0]))
    v0 = 0.5 * 1.1 + 0.5 * 1.01
    v1 = 0.5 * 1.1 * 0.95 + 0.5 * 1.01 * 1.01
    np.testing.assert_allclose(gross, [v0, v1 / v0, 1.02, 1.0], rtol=1e-12)
    np.testing.assert_allclose(held, [0.5, 0.5 * 1.1 / v0, 1.0, 1.0], rtol=1e-12)
    costly, _ = cp.simulate(risky, cash, month, np.array([0.5, 1.0]), 25.0)
    drifted = 0.5 * 1.1 * 0.95 / v1
    np.testing.assert_allclose(
        costly, [v0, v1 / v0, 1.02 * (1 - 0.0025 * (1.0 - drifted)), 1.0], rtol=1e-12
    )
    full, _ = cp.simulate(risky, cash, month, np.array([1.0, 1.0]))
    none, _ = cp.simulate(risky, cash, month, np.array([0.0, 0.0]))
    np.testing.assert_allclose(full, 1 + risky, rtol=1e-12)
    np.testing.assert_allclose(none, 1 + cash, rtol=1e-12)


def test_a_history_record_pools_the_three_markets_by_days(monkeypatch: pytest.MonkeyPatch) -> None:
    def fixed(bears: Any, count: int, uniforms: np.ndarray, rate: float) -> np.ndarray:
        assert len(uniforms) == count - 1
        if rate == 1.0:
            return np.where(np.arange(count) % 4 < 2, cp.HIGH, cp.LOW)
        return np.full(count, cp.LOW)

    monkeypatch.setattr(cp, "plant_targets", fixed)
    record = cp.evaluate_history(
        FRAME, FOUND, 99, 4, months=120, draws=20, blocks=(21,), confidences=(0.95,)
    )
    # The history is the first thing drawn from the history's own generator [seed, index].
    made = cp.history(FRAME, FOUND, np.random.default_rng([99, 4]), 120)
    assert (record["history"], record["drawn"], record["days"]) == (
        4,
        list(made.drawn),
        len(made.frame),
    )
    assert record["error"] is None
    for plant, rate in cp.PLANTS.items():
        for cost, field in ((0.0, "point"), (25.0, "cost_25")):
            if cost and plant != "plant_base":
                continue
            pooled, days = 0.0, 0
            for name in cp.MARKETS:
                part = cp.market(made.frame, name)
                count = int(part.month.max()) + 1
                targets = fixed(None, count, np.zeros(count - 1), rate)
                gross, held = cp.simulate(part.risky, part.cash, part.month, targets)
                matched = np.full(count, held.mean())
                if cost:
                    gross, _ = cp.simulate(part.risky, part.cash, part.month, targets, cost)
                reference, _ = cp.simulate(part.risky, part.cash, part.month, matched, cost)
                logs = np.log(gross) - np.log(reference)
                if name == "tech_ew":
                    tech = 252 * logs.mean()
                    if plant == "plant_base" and not cost:
                        assert record["matched_exposure"]["tech_ew"] == pytest.approx(held.mean())
                        level = cp.month_level(part.risky, part.month)
                        assert record["slow_bears"]["tech_ew"] == len(cp.slow_bears(level))
                else:
                    pooled += logs.sum()
                    days += len(logs)
            got = record[field]
            if field == "point":
                assert got["tech_ew"][plant] == pytest.approx(tech, abs=1e-12)
                assert got["three"][plant] == pytest.approx(252 * pooled / days, abs=1e-12)
            else:
                assert got["tech_ew"] == pytest.approx(tech, abs=1e-12)
                assert got["three"] == pytest.approx(252 * pooled / days, abs=1e-12)
    low, high = record["bounds"]["three"]["plant_noisy"]["21"]["0.95"]
    assert low < high


# ----- the stationary bootstrap by blocks ---------------------------------------------------------
def test_block_draws_cover_each_path_and_sum_like_day_indices() -> None:
    rng = np.random.default_rng(11)
    starts, sizes = cp.block_draws(50, 30, 7.0, rng)
    assert starts.shape == sizes.shape and starts.shape[0] == 30
    assert (sizes >= 0).all() and (sizes.sum(axis=1) == 50).all()
    assert ((starts >= 0) & (starts < 50)).all()
    paths = cp.expand(starts, sizes, 50)
    assert paths.shape == (30, 50)
    values = np.random.default_rng(12).normal(size=50)
    np.testing.assert_allclose(
        cp.block_sums(values, starts, sizes), values[paths].sum(axis=1), rtol=0, atol=1e-12
    )
    ones, single = cp.block_draws(40, 3, 1.0, rng)
    assert (single[:, :40] == 1).all() and (single.sum(axis=1) == 40).all()
    with pytest.raises(ValueError):
        cp.block_draws(40, 3, 0.5, rng)


def test_block_draws_have_the_law_of_the_bench_bootstrap() -> None:
    length, draws, block = 200, 2000, 10.0
    _, sizes = cp.block_draws(length, draws, block, np.random.default_rng(21))
    ours = float((sizes > 0).sum(axis=1).mean())
    paths = stationary_bootstrap_indices(length, draws, block, np.random.default_rng(22))
    jumps = (paths[:, 1:] != (paths[:, :-1] + 1) % length).sum(axis=1) + 1
    bench = float(jumps.mean())
    expected = 1 + (length - 1) / block
    assert abs(ours - expected) < 0.4 and abs(bench - expected) < 0.4


# ----- the reading and the exact shift ------------------------------------------------------------
def _blocks(length: int, draws: int = 300) -> dict[int, tuple[np.ndarray, np.ndarray]]:
    rng = np.random.default_rng(31)
    return {block: cp.block_draws(length, draws, block, rng) for block in (21, 252)}


def test_reading_and_bounds_move_exactly_with_the_shift() -> None:
    rng = np.random.default_rng(32)
    length = 3000
    risky = rng.normal(0.0004, 0.015, length)
    cash = np.full(length, 0.0001)
    month = np.repeat(np.arange(length // 20), 20)
    targets = np.where(rng.random(length // 20) < 0.3, 0.25, 1.0)
    gross, held = cp.simulate(risky, cash, month, targets)
    reference, _ = cp.simulate(risky, cash, month, np.full(len(targets), held.mean()))
    count = np.ones(length)
    blocks = _blocks(length)
    base = np.log(gross) - np.log(reference)
    point = cp.reading(base, count)
    low_high = cp.bounds(base, count, blocks)
    assert set(low_high) == {"21", "252"}
    assert set(low_high["21"]) == {"0.95", "0.99", "0.997"}
    for delta in (-0.031, 0.0125):
        moved = np.log(gross * np.exp(delta / 252)) - np.log(reference)
        assert cp.reading(moved, count) == pytest.approx(point + delta, abs=1e-12)
        shifted = cp.bounds(moved, count, blocks)
        for block, row in low_high.items():
            for key, (low, high) in row.items():
                assert shifted[block][key][0] == pytest.approx(low + delta, abs=1e-12)
                assert shifted[block][key][1] == pytest.approx(high + delta, abs=1e-12)
    # three markets pooled by days with a value: the same shift on every value moves it exactly.
    pooled_count = rng.integers(1, 4, length).astype(float)
    pooled = base * pooled_count
    moved = pooled + pooled_count * 0.02 / 252
    assert cp.reading(moved, pooled_count) == pytest.approx(
        cp.reading(pooled, pooled_count) + 0.02, abs=1e-12
    )
    assert cp.reading(pooled, pooled_count) == pytest.approx(
        252 * pooled.sum() / pooled_count.sum(), rel=1e-12
    )


def test_the_reading_has_the_sign_of_the_geometric_difference() -> None:
    rng = np.random.default_rng(33)
    for _ in range(50):
        strategy = rng.normal(0.0003, 0.012, 500)
        benchmark = strategy * 0.8 + rng.normal(0.0, 0.002, 500)
        logs = np.log1p(strategy) - np.log1p(benchmark)
        geometric = np.expm1(252 * np.log1p(strategy).mean()) - np.expm1(
            252 * np.log1p(benchmark).mean()
        )
        assert np.sign(cp.reading(logs, np.ones(500))) == np.sign(geometric)


def test_iid_noise_is_judged_near_the_nominal_rate() -> None:
    rng = np.random.default_rng(34)
    hits = 0
    for _ in range(300):
        values = rng.normal(0.0, 0.01, 2000)
        blocks = {21: cp.block_draws(2000, 200, 21, rng)}
        low, high = cp.bounds(values, np.ones(2000), blocks, (0.95,))["21"]["0.95"]
        hits += cp.judge(low, high) != "undecided"
    assert 0.01 <= hits / 300 <= 0.12


def test_judge_and_wilson() -> None:
    assert cp.judge(0.001, 0.02) == "gain"
    assert cp.judge(-0.02, -0.001) == "loss"
    assert cp.judge(-0.01, 0.01) == "undecided"
    assert cp.judge(0.0, 0.01) == "undecided" and cp.judge(-0.01, 0.0) == "undecided"
    assert cp.wilson_upper(25, 500) <= 0.075 < cp.wilson_upper(26, 500)
    assert cp.wilson_upper(0, 500) == pytest.approx(
        1.959963984540054**2 / (500 + 1.959963984540054**2)
    )
    with pytest.raises(ValueError):
        cp.wilson_upper(0, 0)


# ----- the reading written before the run (requirement section 5) ---------------------------------
def _record(point: float, rows: dict[tuple[int, float], tuple[float, float]]) -> dict[str, Any]:
    bounds: dict[str, dict[str, list[float]]] = {}
    for (block, confidence), (low, high) in rows.items():
        bounds.setdefault(str(block), {})[f"{confidence:g}"] = [point + low, point + high]
    return {
        "point": {"tech_ew": {"plant_base": point}},
        "cost_25": {"tech_ew": point - 0.001},
        "bounds": {"tech_ew": {"plant_base": bounds}},
    }


def test_ruler_rows_and_choose() -> None:
    wide = (-0.02, 0.02)
    records = []
    for i in range(100):
        rows = {
            (21, 0.95): (0.005, 0.02) if i < 10 else wide,  # 10 false gains at 0
            (21, 0.99): wide,
            (1260, 0.95): wide,
            (1260, 0.99): (-0.025, 0.025),  # +2 reads [-0.005, 0.045]: never decided
        }
        records.append(_record(0.03 + (0.001 if i % 2 else -0.001), rows))
    table = cp.ruler_rows(
        records, "tech_ew", "plant_base", blocks=(21, 1260), confidences=(0.95, 0.99)
    )
    assert table["centre"] == pytest.approx(0.03)
    assert table["spread"] == pytest.approx(np.std([0.029, 0.031] * 50, ddof=1))
    assert table["cost_25"] == pytest.approx(0.029)
    rows = {(r["block"], r["confidence"]): r for r in table["rows"]}
    assert list(rows) == [(21, 0.95), (21, 0.99), (1260, 0.95), (1260, 0.99)]
    first = rows[(21, 0.95)]
    assert first["false_rate"] == pytest.approx(0.10) and not first["eligible"]
    assert first["wilson_upper"] == pytest.approx(cp.wilson_upper(10, 100))
    assert set(first["shares"]) == {"-2", "-1", "+0", "+1", "+2"}
    second = rows[(21, 0.99)]
    # The level's shift is level minus the mean reading, so each record reads point - 0.03 + level.
    assert second["shares"]["+2"] == {"gain": 0.5, "loss": 0.0}
    assert second["shares"]["-2"] == {"gain": 0.0, "loss": 0.5}
    assert second["shares"]["+0"] == {"gain": 0.0, "loss": 0.0}
    assert (second["score"], second["power_1"], second["reverse_rate"]) == (0.5, 0.0, 0.0)
    assert second["false_rate"] == 0.0 and second["eligible"]
    assert rows[(1260, 0.95)]["score"] == 0.5 and rows[(1260, 0.95)]["eligible"]
    assert rows[(1260, 0.99)]["score"] == 0.0 and rows[(1260, 0.99)]["eligible"]
    # Tie at 0.5: the longer block wins.
    assert cp.choose(table["rows"]) == {
        "verdict": "large_only",
        "block": 1260,
        "confidence": 0.95,
        "score": 0.5,
    }


def test_wilson_upper_and_power_at_one_point() -> None:
    records = []
    for i in range(100):
        # Even records decide at +1 and -1; odd ones only from +2 and at -1.
        bounds = (-0.005, 0.005) if i % 2 == 0 else (-0.015, 0.005)
        records.append(_record(0.03 + (0.001 if i % 2 else -0.001), {(252, 0.99): bounds}))
    row = cp.ruler_rows(records, "tech_ew", "plant_base", blocks=(252,), confidences=(0.99,))[
        "rows"
    ][0]
    assert row["shares"]["+1"] == {"gain": 0.5, "loss": 0.0}
    assert row["shares"]["-1"] == {"gain": 0.0, "loss": 1.0}
    assert (row["score"], row["power_1"], row["false_rate"]) == (1.0, 0.5, 0.0)
    assert row["eligible"]
    # 4 false judgements in 100: 4% is under 5%, but the Wilson upper bound is over 7.5%.
    for i in range(4):
        records[i] = _record(0.03, {(252, 0.99): (0.005, 0.02)})
    row = cp.ruler_rows(records, "tech_ew", "plant_base", blocks=(252,), confidences=(0.99,))[
        "rows"
    ][0]
    assert row["false_rate"] == pytest.approx(0.04)
    assert row["wilson_upper"] > 0.075 and not row["eligible"]


def test_reverse_judgements_make_a_ruler_ineligible() -> None:
    records = []
    for i in range(100):
        bad = (-0.05, -0.035) if i == 0 else (-0.015, 0.015)  # +2 reads [-0.03, -0.015]: loss
        records.append(_record(0.03, {(252, 0.95): bad}))
    row = cp.ruler_rows(records, "tech_ew", "plant_base", blocks=(252,), confidences=(0.95,))[
        "rows"
    ][0]
    assert row["reverse_rate"] == pytest.approx(0.01) and row["eligible"]
    records[1] = _record(0.03, {(252, 0.95): (-0.05, -0.035)})
    row = cp.ruler_rows(records, "tech_ew", "plant_base", blocks=(252,), confidences=(0.95,))[
        "rows"
    ][0]
    assert row["reverse_rate"] == pytest.approx(0.02) and not row["eligible"]


def test_false_rate_limits_at_500_histories() -> None:
    def table(false_hits: int) -> dict[str, Any]:
        records = [
            _record(0.0, {(21, 0.95): (0.005, 0.02) if i < false_hits else (-0.015, 0.015)})
            for i in range(500)
        ]
        return cp.ruler_rows(records, "tech_ew", "plant_base", blocks=(21,), confidences=(0.95,))[
            "rows"
        ][0]

    assert table(25)["eligible"] and not table(26)["eligible"]


def _row(block: int, confidence: float, score: float, eligible: bool = True) -> dict[str, Any]:
    return {"block": block, "confidence": confidence, "score": score, "eligible": eligible}


def test_choose_verdicts_and_ties() -> None:
    assert cp.choose([_row(21, 0.95, 0.9, False)]) == {
        "verdict": "none_eligible",
        "block": None,
        "confidence": None,
        "score": None,
    }
    assert cp.choose([_row(21, 0.95, 0.8)])["verdict"] == "ready"
    assert cp.choose([_row(21, 0.95, 0.79)])["verdict"] == "large_only"
    assert cp.choose([_row(21, 0.95, 0.5)])["verdict"] == "large_only"
    assert cp.choose([_row(21, 0.95, 0.49)])["verdict"] == "below_50"
    picked = cp.choose(
        [
            _row(252, 0.95, 0.7),
            _row(252, 0.997, 0.7),
            _row(1260, 0.99, 0.6),
            _row(21, 0.99, 0.9, False),
        ]
    )
    assert (picked["block"], picked["confidence"], picked["score"]) == (252, 0.997, 0.7)


# ----- the run ------------------------------------------------------------------------------------
SMALL = {"histories": 3, "draws": 40, "months": 120}


def test_run_probe_on_a_small_synthetic_run(tmp_path: Path) -> None:
    result = cp.run_probe(FRAME, journal=tmp_path / "h.jsonl", **SMALL)
    assert (result["run_id"], result["owner_decision"], result["snapshot_id"]) == (
        cp.RUN_ID,
        cp.OWNER_DECISION,
        cp.SNAPSHOT_ID,
    )
    assert len(result["cycles"]) == len(FOUND)
    assert result["cycles"][0]["id"] == "cycle_01"
    assert result["tail"] == {"first": TAIL[0], "last": "2010-12", "months": len(TAIL)}
    assert result["errors"] == []
    lines = (tmp_path / "h.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["history"] for line in lines] == [0, 1, 2]
    record = json.loads(lines[0])
    assert set(record["point"]) == {"tech_ew", "three"}
    assert set(record["bounds"]["three"]) == {"plant_base", "plant_noisy"}
    assert set(record["slow_bears"]) == {"tech_ew", "Hardw", "Chips", "Softw"}
    for variant in ("tech_ew", "three"):
        item = result["variants"][variant]
        assert item["reading"]["verdict"] in {"ready", "large_only", "below_50", "none_eligible"}
        assert set(item["tables"]) == {"plant_base", "plant_noisy"}
        assert len(item["tables"]["plant_base"]["rows"]) == 9
    counts = result["slow_bear_counts"]["tech_ew"]
    assert counts["histories"] == 3 and sum(counts["counts"].values()) == 3


def test_the_same_seed_gives_the_same_bytes(tmp_path: Path) -> None:
    first, second = tmp_path / "a", tmp_path / "b"
    cp.write_result(cp.run_probe(FRAME, **SMALL), first)
    cp.write_result(cp.run_probe(FRAME, **SMALL), second)
    for name in ("summary.json", "report.md"):
        assert (first / name).read_bytes() == (second / name).read_bytes()
    other = cp.run_probe(FRAME, seed=cp.SEED + 1, **SMALL)
    saved = json.loads((first / "summary.json").read_text(encoding="utf-8"))
    assert json.dumps(other["variants"], sort_keys=True) != json.dumps(
        saved["variants"], sort_keys=True
    )


def test_any_failed_history_makes_the_run_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    real = cp.evaluate_history

    def flaky(frame: pd.DataFrame, found: Any, seed: int, index: int, **kwargs: Any) -> Any:
        if index == 1:
            raise RuntimeError("boom")
        return real(frame, found, seed, index, **kwargs)

    monkeypatch.setattr(cp, "evaluate_history", flaky)
    result = cp.run_probe(FRAME, **SMALL)
    assert result["errors"] == [{"history": 1, "error": "RuntimeError: boom"}]
    assert {v["reading"]["verdict"] for v in result["variants"].values()} == {"incomplete"}


def test_fewer_than_eight_cycles_runs_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    calm = cp.market_frame(*_frames(vol=0.003))
    assert len(cp.cycles(calm)[0]) < 8

    def never(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("no history is evaluated")

    monkeypatch.setattr(cp, "evaluate_history", never)
    result = cp.run_probe(calm, **SMALL)
    assert {v["reading"]["verdict"] for v in result["variants"].values()} == {"incomplete"}
    assert "incomplete" in cp.render(result)


def test_pure_noise_planted_on_an_iid_market_is_judged_near_nominal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No slow bears for the planted rule: only the random false exits remain (pure noise).
    monkeypatch.setattr(cp, "slow_bears", lambda level: [])
    result = cp.run_probe(
        FRAME, histories=40, draws=200, months=240, blocks=(21,), confidences=(0.95,)
    )
    row = result["variants"]["tech_ew"]["tables"]["plant_base"]["rows"][0]
    assert (row["block"], row["confidence"]) == (21, 0.95)
    assert row["false_rate"] <= 0.15


# ----- report and command -------------------------------------------------------------------------
def test_report_lines() -> None:
    result = cp.run_probe(FRAME, **SMALL)
    text = cp.render(result)
    for line in (
        "# 科技类行业整轮尺子的检验力探针（TRADING-2565-61）",
        "- 依据：`owner_decision:TRADING-2565:2026-10-23:tech_proxy_power_probe`"
        "（第 6 期章程第 13 节）。",
        "- 数据：信号快照 `sig-7ad8aa00b736`，Hardw、Chips、Softw 和 RF，只读到 2010-12-31。",
        "- 只用埋的规则；F0001、fast_slow 和别的已有配置都没有跑。不进台账、不算试验，k 不动。",
        "## 判定",
        "## 周期",
        "## tech_ew · plant_base（判的口径）",
        "## three · plant_noisy（只印）",
        "production_effect=none；broker_action=none。",
    ):
        assert line in text.splitlines()
    assert f"{cp.TECH} 的周期 {len(FOUND)} 个" in text
    assert re.search(r"尾巴（不抽）：\d{4}-\d{2}\.\.2010-12，\d+ 个月", text)


def test_command_writes_the_three_files(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(cp, "load_frame", lambda: FRAME)
    monkeypatch.setattr(cp, "HISTORIES", 2)
    monkeypatch.setattr(cp, "DRAWS", 30)
    monkeypatch.setattr(cp, "MONTHS", 120)
    out = tmp_path / "out"
    result = CliRunner().invoke(cp.app, ["run", "--out", str(out)])
    assert result.exit_code == 0, result.output
    assert sorted(p.name for p in out.iterdir()) == ["histories.jsonl", "report.md", "summary.json"]
    assert len((out / "histories.jsonl").read_text(encoding="utf-8").splitlines()) == 2
    summary = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    assert summary["parameters"]["histories"] == 2 and summary["parameters"]["draws"] == 30
    assert "cycle_power: tech_ew 判定" in result.output
    assert "cycle_power: three 判定" in result.output


def test_command_reports_a_load_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def broken() -> pd.DataFrame:
        raise ValueError("signal snapshot is unchecked or failed")

    monkeypatch.setattr(cp, "load_frame", broken)
    result = CliRunner().invoke(cp.app, ["run", "--out", str(tmp_path / "out")])
    assert result.exit_code == 1
    assert "cycle_power: 数据载入失败：signal snapshot is unchecked or failed" in result.output
    assert not (tmp_path / "out").exists()


def test_the_probe_runs_no_real_rule_and_writes_no_ledger() -> None:
    imports = [line for line in SOURCE.splitlines() if line.startswith(("import ", "from "))]
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
    assert "2011" not in SOURCE
