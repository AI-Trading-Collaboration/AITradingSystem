from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import date
from fractions import Fraction
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import load_settings
from ai_trading_system.research_bench import segments as bench_segments
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import (
    BenchData,
    from_daily_frame,
    from_us_market,
    with_signal_sources,
)
from ai_trading_system.research_bench.metrics import summarize
from ai_trading_system.research_bench.run import (
    PRIMARY_BENCHMARK,
    RunSpec,
    benchmark_simulations,
    run_strategy,
)
from ai_trading_system.research_bench.segments import Segment, segment_return
from ai_trading_system.research_bench.settings import Window
from ai_trading_system.research_bench.simulate import (
    Rebalance,
    Simulation,
    constant_targets,
    simulate,
)
from ai_trading_system.research_probes import gate_false_pass as fp
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import portfolio_ruler_check as pr

pytestmark = pytest.mark.filterwarnings("ignore:invalid value encountered in log1p")

LOADED = load_settings()
ROOT_DIR = lg.research_root(LOADED)
TRIALS = PROJECT_ROOT / "research" / "_trials"
GATE, PLAN = tr.load_trial(ROOT_DIR, "T0002")
DAYS = LOADED.settings.trading_days_per_year
MONTH_END = Rebalance(schedule="month_end")
SEEDS = 2


def _drift(index: pd.DatetimeIndex, moves: list[tuple[str, str, float]], up: float) -> np.ndarray:
    months = index.to_period("M")
    drift = np.full(len(index), up)
    for start, end, value in moves:
        inside = (months >= pd.Period(start, freq="M")) & (months <= pd.Period(end, freq="M"))
        drift[np.asarray(inside)] = value
    return drift


def _us_data(start: str = "1926-07-01", end: str = "1936-12-31", seed: int = 5) -> BenchData:
    """Ken French-like daily market returns with a long slow bear (1929-10..1932-06), a fast
    recovery broken by two short declines, a fast drop (1935-03..1935-04) and a last decline."""

    index = pd.bdate_range(start, end, name="date")
    rng = np.random.default_rng(seed)
    moves = [
        ("1929-10", "1932-06", -0.0015),
        ("1932-07", "1934-12", 0.003),
        ("1933-08", "1933-09", -0.004),
        ("1934-04", "1934-05", -0.005),
        ("1935-03", "1935-04", -0.008),
        ("1935-05", "1935-12", 0.003),
        ("1936-05", "1936-06", -0.003),
    ]
    factors = pd.DataFrame(
        {
            "Mkt-RF": _drift(index, moves, 6e-4) + rng.normal(0.0, 0.009, len(index)) - 0.0001,
            "SMB": 0.0,
            "HML": 0.0,
            "RF": 0.0001,
        },
        index=index,
    )
    data = from_us_market({"ff_factors_daily": factors}, snapshot_id="sig-test")
    return with_signal_sources(data, {}, {}, snapshot_id="sig-test")


def _qqq_data(seed: int = 3) -> BenchData:
    index = pd.bdate_range("1994-01-03", "2010-12-31", name="date")
    rng = np.random.default_rng(seed)
    moves = [
        ("1999-07", "1999-08", -0.004),
        ("2000-04", "2002-09", -0.0015),
        ("2002-10", "2004-12", 0.003),
        ("2003-06", "2003-07", -0.004),
        ("2004-04", "2004-05", -0.004),
        ("2007-11", "2008-11", -0.002),
    ]
    r = _drift(index, moves, 6e-4) + rng.normal(0.0, 0.012, len(index))
    level = 1000.0 * np.cumprod(1.0 + r)
    daily = pd.DataFrame(
        {
            "qqq_proxy_return": r,
            "etf_reference_return": r,
            "cash_return": 0.0001,
            "lev2_return": 2 * r,
            "lev3_return": 3 * r,
            "ndx_price": level,
            "ndx_tr": level,
            "vix": 20.0,
            "vxn": 25.0,
            "vxo": 21.0,
            "cash_rate_annual": 0.03,
        },
        index=index,
    )
    data = from_daily_frame(daily, qqq_series="actual", source={"snapshot_id": "qlh-test"})
    return with_signal_sources(data, {}, {}, snapshot_id="sig-test")


US = _us_data()
QQQ = _qqq_data()
DATA = {"us_market": US, "qqq": QQQ}
US_WINDOW = Window("us_test_1929", "development", date(1929, 1, 2), date(1936, 12, 31))
QQQ_WINDOW = Window("qqq_test", "development", date(1999, 3, 10), date(2004, 12, 31))
# Short windows keep the whole-run tests fast; the real ones are probe_windows'.
WINDOWS = {
    "us_test_1929": pr.ProbeWindow("us_test_1929", "us_market", US_WINDOW, True),
    "qqq_test": pr.ProbeWindow("qqq_test", "qqq", QQQ_WINDOW, False),
}
CONFIG = pr.plan_config(PLAN)
TARGETS = constant_targets(US.inputs.index, 0.6)
DECLINES = pr.window_declines(US.returns["qqq"], US_WINDOW)
SEED = fp.seed_list(1)[0]


def _reserve(family_id: str, data: BenchData = US, **change: Any) -> pd.Series:
    family = replace(pr.FAMILIES[family_id], **change)
    declines = DECLINES if data is US else pr.window_declines(data.returns["qqq"], QQQ_WINDOW)
    return pr.reserve_returns(data.returns, family, SEED, declines, DAYS)


def _measure(reserve: pd.Series, declines: list[Segment] | None = None) -> dict[str, Any]:
    return pr.measure(
        US,
        TARGETS,
        reserve,
        US_WINDOW,
        MONTH_END,
        0.6,
        DECLINES if declines is None else declines,
        LOADED,
    )


@pytest.fixture(scope="module")
def probe_run(tmp_path_factory: pytest.TempPathFactory) -> tuple[dict[str, Any], Path]:
    journal = tmp_path_factory.mktemp("portfolio_ruler") / pr.JOURNAL_FILE
    result = pr.run_probe(
        DATA, LOADED, GATE, PLAN, ROOT_DIR, seeds=SEEDS, windows=WINDOWS, journal=journal
    )
    return result, journal


@pytest.fixture(scope="module")
def probe(probe_run: tuple[dict[str, Any], Path]) -> dict[str, Any]:
    return probe_run[0]


def _level(values: list[float], start: str = "2000-01") -> pd.Series:
    return pd.Series(values, index=pd.period_range(start, periods=len(values), freq="M"))


# --- the fixed design ----------------------------------------------------------------------------


def test_the_constants_are_the_requirements() -> None:
    assert pr.RUN_ID == "TRADING-2565-38"
    assert pr.OWNER_DECISION == "owner_decision:TRADING-2565:2026-10-21:p1_portfolio_ruler"
    assert pr.PLAN_ID == "T0002"
    assert pr.DECLINE_LINE == 0.10
    assert pr.THRESHOLDS == (0.05, 0.10, 0.15) and pr.SHARES == (0.6, 0.75)
    assert pr.PAIRS == (
        (0.05, 0.75),
        (0.10, 0.75),
        (0.15, 0.75),
        (0.05, 0.6),
        (0.10, 0.6),
        (0.15, 0.6),
    )
    assert (pr.GEOMETRIC_FLOOR, pr.MIN_COUNT, pr.SMALL_LOSS, pr.QUIET_SHARE) == (
        -0.010,
        4,
        -0.02,
        0.5,
    )
    assert (pr.POWER_FAMILY, pr.POWER_LINE, pr.USABLE_LINE) == ("hedge_0.3", 0.80, 0.025)
    assert (pr.COST_BPS, pr.COST) == (25.0, "cost_25")
    assert (pr.NOISE, pr.CONTROL_NOISE, pr.CARRY, pr.CORRELATION) == (0.07, 0.15, 0.02, 0.3)
    assert (pr.JOURNAL_FILE, pr.RESULT_JSON, pr.RESULT_MD) == (
        "portfolio_ruler_rows.jsonl",
        "portfolio_ruler_result.json",
        "portfolio_ruler_result.md",
    )


def test_the_families_are_the_requirements() -> None:
    assert [
        (f.family_id, f.role, f.kind, f.size, f.noise, f.stream) for f in pr.FAMILIES.values()
    ] == [
        ("null_noise", "zero", "noise", 0.0, 0.07, 1),
        ("null_carry", "zero", "carry", 0.02, 0.07, 2),
        ("null_pos_corr", "zero", "correlated", 0.3, 0.07, 3),
        ("hedge_0.2", "hedge", "hedge", 0.2, 0.07, 4),
        ("hedge_0.3", "hedge", "hedge", 0.3, 0.07, 4),
        ("hedge_0.5", "hedge", "hedge", 0.5, 0.07, 4),
        ("hedge_0.3_noise_0.15", "control", "hedge", 0.3, 0.15, 5),
    ]
    assert list(pr.FAMILIES) == [f.family_id for f in pr.FAMILIES.values()]
    assert pr.ZERO_FAMILIES == ("null_noise", "null_carry", "null_pos_corr")
    assert pr.READ_FAMILIES == ("null_noise", "null_carry", "null_pos_corr", "hedge_0.3")


# --- decline segments ----------------------------------------------------------------------------


@pytest.mark.parametrize("seed", range(6))
def test_at_twenty_percent_the_declines_are_the_benchs_drop_segments(seed: int) -> None:
    rng = np.random.default_rng(seed)
    level = _level(list(np.cumprod(1.0 + rng.normal(0.005, 0.06, 240))), "1950-01")
    ours = [
        (s.start, s.end, s.peak, s.trough, s.months, s.change)
        for s in pr.decline_segments(level, 0.20)
    ]
    bench = [
        (s.start, s.end, s.peak, s.trough, s.months, s.change)
        for s in bench_segments.drop_segments(level)
    ]
    assert ours == bench and len(ours) > 0


def test_every_drop_of_ten_percent_is_one_decline_with_no_minimum_length() -> None:
    found = pr.decline_segments(_level([1.0, 1.2, 1.08, 1.3, 1.17, 1.4, 1.27, 1.5]))
    assert [(s.id, s.kind, s.start, s.end, s.peak, s.trough, s.months) for s in found] == [
        ("decline_200002", "decline", "2000-03", "2000-03", "2000-02", "2000-03", 1),
        ("decline_200004", "decline", "2000-05", "2000-05", "2000-04", "2000-05", 1),
    ]
    assert found[0].change == pytest.approx(-0.1) and found[1].change == pytest.approx(-0.1)


def test_a_drop_of_exactly_the_line_opens_and_less_does_not() -> None:
    assert len(pr.decline_segments(_level([1.0, 0.9, 1.0]))) == 1
    assert pr.decline_segments(_level([1.0, 0.9001, 1.0])) == []
    assert len(pr.decline_segments(_level([1.0, 0.9001, 1.0]), 0.05)) == 1


def test_a_decline_runs_from_the_month_after_the_high_to_the_lowest_month() -> None:
    found = pr.decline_segments(_level([1.0, 1.1, 1.05, 0.95, 0.9, 0.97, 1.11, 1.0]))
    assert [(s.start, s.end, s.peak, s.trough, s.months) for s in found] == [
        ("2000-03", "2000-05", "2000-02", "2000-05", 3)
    ]
    assert found[0].change == pytest.approx(0.9 / 1.1 - 1.0)


def test_the_window_declines_are_cut_from_the_simulations_days() -> None:
    expected = pr.decline_segments(
        bench_segments.monthly_level(bench_segments.window_returns(US.returns["qqq"], US_WINDOW))
    )
    assert pr.window_declines(US.returns["qqq"], US_WINDOW) == expected
    assert len(expected) >= 4 and all(s.kind == "decline" for s in expected)


# --- synthetic reserves --------------------------------------------------------------------------


def _noise(stream: int, scale: float = 0.07, seed: int = SEED) -> np.ndarray:
    z = np.random.default_rng([seed, stream]).standard_normal(len(US.returns))
    return z * scale / math.sqrt(DAYS)


def test_the_noise_family_is_cash_plus_its_own_normal_stream() -> None:
    reserve = _reserve("null_noise")
    cash = US.returns["cash"].to_numpy()
    assert reserve.index.equals(US.returns.index) and reserve.name == "reserve"
    np.testing.assert_allclose(reserve.to_numpy(), cash + _noise(1), rtol=0, atol=1e-15)
    annual = float(np.std(reserve.to_numpy() - cash)) * math.sqrt(DAYS)
    assert annual == pytest.approx(0.07, rel=0.05)


def test_each_zero_family_draws_its_own_stream_and_the_hedges_share_one() -> None:
    cash = US.returns["cash"].to_numpy()
    market = US.returns["qqq"].to_numpy()
    np.testing.assert_allclose(
        _reserve("null_carry").to_numpy(), cash + _noise(2) + 0.02 / DAYS, rtol=0, atol=1e-15
    )
    np.testing.assert_allclose(
        _reserve("null_pos_corr").to_numpy(),
        cash + _noise(3) + 0.3 * (market - cash),
        rtol=0,
        atol=1e-15,
    )
    control = _reserve("hedge_0.3_noise_0.15", noise=0.0)
    np.testing.assert_allclose(
        _reserve("hedge_0.3_noise_0.15").to_numpy() - control.to_numpy(),
        _noise(5, 0.15),
        rtol=0,
        atol=1e-15,
    )
    for family_id in ("hedge_0.2", "hedge_0.3", "hedge_0.5"):
        planted = _reserve(family_id, noise=0.0).to_numpy()
        np.testing.assert_allclose(
            _reserve(family_id).to_numpy() - planted, _noise(4), rtol=0, atol=1e-15
        )
    assert not np.allclose(_noise(1), _noise(2))
    assert not np.allclose(_noise(4), _noise(5))
    other = pr.reserve_returns(US.returns, pr.FAMILIES["null_noise"], SEED + 1, DECLINES, DAYS)
    np.testing.assert_allclose(other.to_numpy(), cash + _noise(1, seed=SEED + 1), atol=1e-15)


def test_the_hedge_plants_its_share_of_each_depth_month_by_month() -> None:
    component = _reserve("hedge_0.3", noise=0.0) - US.returns["cash"]
    months = pd.DatetimeIndex(component.index).to_period("M")
    planted = np.zeros(len(component), dtype=bool)
    for s in DECLINES:
        assert s.change is not None
        depth = abs(s.change)
        total = 1.0
        for month in pd.period_range(s.start, s.end, freq="M"):
            days = component[months == month]
            assert days.nunique() == 1
            growth = float(np.prod(1.0 + days.to_numpy()))
            assert growth == pytest.approx((1.0 + 0.3 * depth) ** (1.0 / s.months))
            total *= growth
            planted |= np.asarray(months == month)
        assert total == pytest.approx(1.0 + 0.3 * depth)
    assert (component[~planted].abs() < 1e-15).all()


def test_the_hedge_component_is_zero_outside_and_needs_a_depth() -> None:
    index = pd.bdate_range("2000-01-03", "2000-06-30")
    segment = Segment(
        "decline_200001", "decline", "2000-02", "2000-03", "2000-01", "2000-03", 2, -0.2
    )
    out = pr.hedge_component(index, [segment], 0.5)
    months = index.to_period("M")
    inside = np.asarray((months >= pd.Period("2000-02")) & (months <= pd.Period("2000-03")))
    assert (out[~inside] == 0.0).all()
    assert float(np.prod(1.0 + out[inside])) == pytest.approx(1.1)
    with pytest.raises(ValueError, match="no depth"):
        pr.hedge_component(index, [replace(segment, change=None)], 0.5)


def test_an_unknown_kind_is_refused() -> None:
    with pytest.raises(ValueError, match="unknown family kind"):
        pr.reserve_returns(US.returns, replace(pr.FAMILIES["null_noise"], kind="x"), 1, [], DAYS)


# --- the ruler -----------------------------------------------------------------------------------


def test_the_strategy_holds_the_reserve_and_the_benchmark_is_the_primary_one() -> None:
    reserve = _reserve("null_carry")
    sims = pr.simulate_pair(US, TARGETS, reserve, US_WINDOW, MONTH_END, LOADED)
    traded = US.returns.copy()
    traded["cash"] = reserve
    own = simulate(
        traded,
        TARGETS,
        start=US_WINDOW.start,
        end=US_WINDOW.end,
        rebalance=MONTH_END,
        cost_bps=LOADED.cost_bps,
    )
    pd.testing.assert_frame_equal(sims.strategy.daily, own.daily)
    pd.testing.assert_frame_equal(sims.traded, traded)
    bench = benchmark_simulations(
        US,
        US_WINDOW,
        average_exposure=float(own.exposure.mean()),
        rebalance=MONTH_END,
        cost_bps=LOADED.cost_bps,
        leveraged=False,
    )[PRIMARY_BENCHMARK]
    pd.testing.assert_frame_equal(sims.benchmark.daily, bench.daily)


@pytest.mark.parametrize("reserve", ["treasury_10y", "cash"])
def test_the_geometric_numbers_are_what_a_trial_run_reads(reserve: str) -> None:
    rng = np.random.default_rng(11)
    series = pd.Series(0.0002 + rng.normal(0.0, 0.004, len(QQQ.returns)), index=QQQ.returns.index)
    data = replace(QQQ, reserves={"treasury_10y": series})
    spec = RunSpec(
        question_type="qqq_direction",
        strategy_id="fixed",
        params={"exposure": 0.6},
        window_role="development",
        rebalance=MONTH_END,
        reserve=reserve,
    )
    result = run_strategy(
        data, tr.STRATEGY_FUNCTIONS["fixed_exposure"], spec, LOADED, falsification_confidence=0.95
    ).to_dict()
    window = pr.probe_windows(LOADED, GATE)["qqq_long_history_development"].window
    held = series if reserve == "treasury_10y" else data.returns["cash"]
    declines = pr.window_declines(data.returns["qqq"], window)
    numbers = pr.measure(
        data,
        constant_targets(data.inputs.index, 0.6),
        held,
        window,
        MONTH_END,
        0.6,
        declines,
        LOADED,
    )
    assert numbers["geometric"] == result["comparison"]["geometric_annual_return"]
    assert (
        numbers["geometric_cost"] == result["falsification"]["cost_25"]["geometric_annual_return"]
    )


def test_the_legs_are_summarizes_geometric_on_the_strategys_dates() -> None:
    reserve = _reserve("null_carry")
    numbers = _measure(reserve)
    sims = pr.simulate_pair(US, TARGETS, reserve, US_WINDOW, MONTH_END, LOADED)
    cash = US.returns["cash"]
    legs = numbers["legs"]
    assert legs["strategy"] == summarize(sims.strategy, cash, LOADED)["geometric_annual_return"]
    assert legs["benchmark"] == summarize(sims.benchmark, cash, LOADED)["geometric_annual_return"]
    assert pr.geometric(sims.strategy.returns, sims.strategy) == pytest.approx(legs["strategy"])
    as_held = Simulation(
        daily=sims.strategy.daily.assign(**{"return": US.returns["qqq"]}), start=sims.strategy.start
    )
    market = summarize(as_held, cash, LOADED)["geometric_annual_return"]
    assert legs["market"] == pytest.approx(market, abs=1e-14)
    years = (sims.strategy.returns.index[-1] - sims.strategy.start).days / 365.25
    dated = reserve.reindex(sims.strategy.returns.index)
    assert legs["reserve"] == pytest.approx(float(np.prod(1.0 + dated)) ** (1.0 / years) - 1.0)
    assert numbers["average_exposure"] == float(sims.strategy.exposure.mean())


def test_a_missing_leg_return_is_refused() -> None:
    reserve = _reserve("null_noise")
    reserve.iloc[1000] = np.nan
    with pytest.raises(ValueError):
        _measure(reserve)
    sims = pr.simulate_pair(US, TARGETS, _reserve("null_noise"), US_WINDOW, MONTH_END, LOADED)
    with pytest.raises(ValueError, match="has no return on"):
        pr.geometric(reserve, sims.strategy)


@pytest.mark.parametrize("family_id", list(pr.FAMILIES))
def test_the_geometric_difference_is_the_reserve_excess_plus_rebalancing(family_id: str) -> None:
    numbers = _measure(_reserve(family_id))
    legs = numbers["legs"]
    assert numbers["reserve_excess"] == pytest.approx(0.4 * (legs["reserve"] - legs["cash"]))
    assert numbers["rebalancing"] == pytest.approx(
        legs["strategy"]
        - 0.6 * legs["market"]
        - 0.4 * legs["reserve"]
        - (legs["benchmark"] - 0.6 * legs["market"] - 0.4 * legs["cash"])
    )
    total = numbers["reserve_excess"] + numbers["rebalancing"]
    assert numbers["geometric"] == pytest.approx(total, abs=1e-12)
    assert numbers["excess_annual"] == pytest.approx((1 + legs["reserve"]) / (1 + legs["cash"]) - 1)


def test_cash_as_the_reserve_has_no_excess_and_its_rebalancing_is_the_geometric_difference() -> (
    None
):
    numbers = _measure(US.returns["cash"].rename("reserve"))
    assert numbers["reserve_excess"] == 0.0 and numbers["excess_annual"] == 0.0
    assert abs(numbers["quiet_excess"]) < 1e-12
    assert numbers["rebalancing"] == pytest.approx(numbers["geometric"], abs=1e-15)
    # A rebalances to 0.6, B to A's average exposure: only drift separates them.
    assert abs(numbers["geometric"]) < 1e-3


def test_the_quiet_dates_are_outside_every_segment() -> None:
    dates = pd.DatetimeIndex(US.returns.index)
    quiet = pr.quiet_dates(dates, DECLINES)
    months = dates.to_period("M")
    for s in DECLINES:
        inside = np.asarray((months >= pd.Period(s.start)) & (months <= pd.Period(s.end)))
        assert not quiet[inside].any()
    assert quiet.sum() + sum(
        int(np.asarray((months >= pd.Period(s.start)) & (months <= pd.Period(s.end))).sum())
        for s in DECLINES
    ) == len(dates)


def test_the_quiet_excess_is_the_annualized_log_excess_of_the_quiet_days() -> None:
    reserve = _reserve("null_carry")
    cash = US.returns["cash"]
    dates = pd.DatetimeIndex(US.returns.index[100:900])
    logs = np.log1p(reserve.reindex(dates).to_numpy()) - np.log1p(cash.reindex(dates).to_numpy())
    expected = math.expm1(float(logs.sum()) * DAYS / len(dates))
    assert pr.quiet_excess(reserve, cash, dates, DAYS) == pytest.approx(expected, rel=1e-12)
    with pytest.raises(ValueError):
        pr.quiet_excess(reserve, cash, dates[:0], DAYS)


def test_the_cushion_takes_out_the_quiet_excess_not_the_whole_windows() -> None:
    reserve = _reserve("hedge_0.3")
    numbers = _measure(reserve)
    sims = pr.simulate_pair(US, TARGETS, reserve, US_WINDOW, MONTH_END, LOADED)
    dates = pd.DatetimeIndex(sims.strategy.returns.index)
    quiet = pr.quiet_dates(dates, DECLINES)
    expected = pr.quiet_excess(reserve, US.returns["cash"], dates[quiet], DAYS)
    assert numbers["quiet_excess"] == expected and numbers["quiet_days"] == int(quiet.sum())
    assert numbers["quiet_fallback"] is False
    assert numbers["quiet_excess"] != pytest.approx(numbers["excess_annual"], abs=1e-3)
    held = reserve.reindex(dates)
    cash = US.returns["cash"].reindex(dates)
    for row, s in zip(numbers["segments"], DECLINES, strict=True):
        own = segment_return(sims.strategy.returns, s)
        reference = segment_return(sims.benchmark.returns, s)
        cushion = own - reference - 0.4 * expected * s.months / 12.0
        assert row["id"] == s.id and row["months"] == s.months
        assert (row["strategy_return"], row["benchmark_return"]) == (own, reference)
        assert row["difference"] == own - reference
        assert row["cushion"] == pytest.approx(cushion, abs=1e-15)
        assert row["counted"] is (reference < -0.02)
        assert row["ratio"] == (pytest.approx(cushion / abs(reference)) if row["counted"] else None)
        excess = (1 + segment_return(held, s)) / (1 + segment_return(cash, s)) - 1
        assert row["reserve_excess"] == pytest.approx(excess)


def test_a_carry_only_reserve_has_no_cushion_and_a_hedge_does() -> None:
    carry = _measure(_reserve("null_carry", noise=0.0))["summary"]
    hedge = _measure(_reserve("hedge_0.3", noise=0.0))["summary"]
    assert abs(carry["median_ratio"]) < 0.03
    assert hedge["median_ratio"] > 0.15 and hedge["positive"] == hedge["count"]


def test_too_few_quiet_days_fall_back_to_the_whole_window() -> None:
    wide = [
        Segment("decline_192901", "decline", "1929-02", "1933-12", "1929-01", "1933-12", 59, -0.5)
    ]
    numbers = _measure(_reserve("null_carry"), wide)
    assert numbers["quiet_fallback"] is True
    assert numbers["quiet_excess"] == numbers["excess_annual"]
    row = numbers["segments"][0]
    assert row["cushion"] == pytest.approx(
        row["difference"] - 0.4 * numbers["excess_annual"] * 59 / 12
    )


def _sim(values: dict[str, float]) -> Simulation:
    index = pd.DatetimeIndex([pd.Timestamp(day) for day in values])
    daily = pd.DataFrame({"return": list(values.values()), "exposure": 0.6}, index=index)
    return Simulation(daily=daily, start=index[0] - pd.Timedelta(days=1))


def test_a_segment_where_the_benchmark_loses_no_more_than_the_line_is_listed_not_counted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pr, "SMALL_LOSS", -0.5)
    days = {"2000-01-31": 0.0, "2000-02-15": 0.0, "2000-03-15": 0.0}
    segments = [
        Segment("decline_200001", "decline", "2000-02", "2000-02", "2000-01", "2000-02", 1, -0.2),
        Segment("decline_200002", "decline", "2000-03", "2000-03", "2000-02", "2000-03", 1, -0.2),
    ]
    sims = pr.Simulated(
        strategy=_sim({**days, "2000-02-15": -0.25, "2000-03-15": -0.25}),
        benchmark=_sim({**days, "2000-02-15": -0.5, "2000-03-15": -0.5000001}),
        traded=US.returns,
    )
    zero = pd.Series(0.0, index=sims.strategy.returns.index)
    rows = pr.cushion_rows(sims, pd.DataFrame({"cash": zero}), zero, 0.6, 0.0, segments)
    assert [r["counted"] for r in rows] == [False, True]
    assert rows[0]["ratio"] is None and rows[1]["ratio"] == pytest.approx(0.25000005 / 0.5000001)
    assert pr.cushion_summary(rows)["excluded"] == ["decline_200001"]


def test_the_cushion_summary() -> None:
    rows = [
        {"id": "a", "counted": True, "ratio": 0.3},
        {"id": "b", "counted": False, "ratio": None},
        {"id": "c", "counted": True, "ratio": -0.1},
        {"id": "d", "counted": True, "ratio": 0.0},
        {"id": "e", "counted": True, "ratio": 0.2},
    ]
    assert pr.cushion_summary(rows) == {
        "count": 4,
        "median_ratio": pytest.approx(0.1),
        "positive": 2,
        "positive_share": 0.5,
        "min_ratio": -0.1,
        "excluded": ["b"],
    }
    assert pr.cushion_summary([]) == {
        "count": 0,
        "median_ratio": None,
        "positive": 0,
        "positive_share": None,
        "min_ratio": None,
        "excluded": [],
    }


# --- rules ---------------------------------------------------------------------------------------


@pytest.mark.parametrize("count", range(1, 61))
def test_needed_is_q_times_n_rounded_up_exactly(count: int) -> None:
    for share in pr.SHARES:
        assert pr.needed(share, count) == math.ceil(Fraction(str(share)) * count)


def test_needed_does_not_round_a_binary_product_up() -> None:
    # 0.28 * 25 is 7.000000000000001 in binary floating point.
    assert pr.needed(0.28, 25) == 7
    assert pr.needed(0.28, 26) == 8


def _summary(count: int, median: float | None, positive: int) -> dict[str, Any]:
    return {
        "count": count,
        "median_ratio": median,
        "positive": positive,
        "positive_share": positive / count if count else None,
    }


@pytest.mark.parametrize(
    ("summary", "threshold", "share", "holds"),
    [
        (_summary(13, 0.10, 10), 0.10, 0.75, True),
        (_summary(13, 0.10, 9), 0.10, 0.75, False),
        (_summary(13, 0.10, 8), 0.10, 0.6, True),
        (_summary(13, 0.10, 7), 0.10, 0.6, False),
        (_summary(13, 0.0999, 13), 0.10, 0.6, False),
        (_summary(5, 0.2, 3), 0.10, 0.6, True),
        (_summary(5, 0.2, 3), 0.10, 0.75, False),
        (_summary(4, 0.2, 4), 0.15, 0.75, True),
        (_summary(3, 0.5, 3), 0.05, 0.6, False),
        (_summary(0, None, 0), 0.05, 0.6, False),
        (_summary(10, float("nan"), 10), 0.05, 0.6, False),
    ],
)
def test_p2_needs_four_segments_the_median_and_the_count_above_zero(
    summary: dict[str, Any], threshold: float, share: float, holds: bool
) -> None:
    assert pr.cushion_holds(summary, threshold, share) is holds


def _numbers(
    count: int = 10,
    median: float | None = 0.12,
    positive: int = 9,
    geometric: float | None = 0.0,
    cost: float | None = 0.0,
    rebalancing: float | None = 0.001,
    fallback: bool = False,
) -> dict[str, Any]:
    return {
        "geometric": geometric,
        "geometric_cost": cost,
        "rebalancing": rebalancing,
        "summary": _summary(count, median, positive),
        "quiet_fallback": fallback,
    }


def test_the_stage_one_rules() -> None:
    assert pr.stage1_rules(_numbers(), 0.10, 0.75) == {"P-1": True, "P-2": True, "P-3": True}
    assert pr.stage1_rules(_numbers(geometric=-0.010, cost=-0.010), 0.1, 0.75)["P-1"] is True
    assert pr.stage1_rules(_numbers(geometric=-0.0101), 0.1, 0.75)["P-1"] is False
    assert pr.stage1_rules(_numbers(cost=-0.0101), 0.1, 0.75)["P-1"] is False
    assert pr.stage1_rules(_numbers(cost=None), 0.1, 0.75)["P-1"] is False
    assert pr.stage1_rules(_numbers(rebalancing=0.0), 0.1, 0.75)["P-3"] is True
    assert pr.stage1_rules(_numbers(rebalancing=-1e-12), 0.1, 0.75)["P-3"] is False
    assert pr.stage1_rules(_numbers(rebalancing=None), 0.1, 0.75)["P-3"] is False
    assert pr.stage1_rules(_numbers(positive=7), 0.1, 0.75)["P-2"] is False
    assert list(pr.stage1_rules(_numbers(), 0.1, 0.75)) == list(pr.STAGE1_RULES)


def test_the_stage_two_rules_read_the_direction_only() -> None:
    assert pr.stage2_rules(_numbers(count=2, positive=1, cost=-0.5)) == {
        "S2-1": True,
        "S2-2": True,
        "S2-3": True,
    }
    assert pr.stage2_rules(_numbers(median=0.0))["S2-2"] is False
    assert pr.stage2_rules(_numbers(median=None, count=0, positive=0))["S2-2"] is False
    assert pr.stage2_rules(_numbers(geometric=-0.0101))["S2-1"] is False
    assert pr.stage2_rules(_numbers(rebalancing=-1e-9))["S2-3"] is False
    assert list(pr.stage2_rules(_numbers())) == list(pr.STAGE2_RULES)


# --- readings ------------------------------------------------------------------------------------


def _row(family: str, seed: int, numbers: dict[str, Any] | None, key: str = "w") -> dict[str, Any]:
    return {
        "family": family,
        "role": pr.FAMILIES[family].role,
        "seed": seed,
        "window_key": key,
        "numbers": numbers,
        "error": None if numbers is not None else "ValueError: boom",
    }


def _evaluations(
    passing: dict[str, int], numbers: dict[str, dict[str, Any]] | None = None, seeds: int = 150
) -> list[dict[str, Any]]:
    """Each family's first ``passing[family]`` seeds pass every pair; the rest fail P-2."""

    rows = []
    for family in pr.FAMILIES:
        for n, seed in enumerate(fp.seed_list(seeds)):
            good = (numbers or {}).get(family, _numbers(positive=10, median=0.2))
            rows.append(
                _row(family, seed, good if n < passing.get(family, 0) else _numbers(median=-0.1))
            )
    return rows


def test_family_rates_count_units_rules_and_the_last_rule_alone() -> None:
    rows = [
        _row("null_noise", 1, _numbers()),
        _row("null_noise", 2, _numbers(rebalancing=-0.001)),
        _row("null_noise", 3, _numbers(median=-0.1, rebalancing=-0.001)),
        _row("null_noise", 4, None),
    ]
    assert pr.family_rates(rows, (0.10, 0.75)) == {
        "passes": 1,
        "total": 4,
        "share": 0.25,
        "errors": 1,
        "rules": {"P-1": 3, "P-2": 2, "P-3": 1},
        "last_rule_alone": 1,
    }
    assert pr.family_rates(rows, None)["rules"] == {"S2-1": 3, "S2-2": 2, "S2-3": 1}
    assert pr.family_rates([], (0.10, 0.75))["share"] is None


def test_the_window_takes_the_larger_q_first_then_the_smaller_t() -> None:
    # hedge_0.3 passes every pair at q = 0.6 but only at t = 0.15 for q = 0.75: 8 of 10 above 0
    # holds q = 0.75 and median 0.2 holds every t, so the first pair is (0.05, 0.75).
    reading = pr.window_reading(_evaluations({"hedge_0.3": 150}), "w", 150)
    assert (reading["verdict"], reading["threshold"], reading["share"]) == (
        "threshold",
        0.05,
        0.75,
    )
    assert list(reading["pairs"]) == [pr.pair_key(pair) for pair in pr.PAIRS]
    assert pr.pair_key((0.05, 0.75)) == "t=0.05 q=0.75"
    only_six = {"hedge_0.3": _numbers(positive=7, median=0.2)}
    reading = pr.window_reading(_evaluations({"hedge_0.3": 150}, only_six), "w", 150)
    assert (reading["threshold"], reading["share"]) == (0.05, 0.6)
    pair = reading["pairs"]["t=0.05 q=0.75"]
    assert pair["power"] == 0.0 and pair["enough"] is False and pair["usable"] is True
    low_median = {"hedge_0.3": _numbers(positive=10, median=0.12)}
    reading = pr.window_reading(_evaluations({"hedge_0.3": 150}, low_median), "w", 150)
    assert (reading["threshold"], reading["share"]) == (0.05, 0.75)
    assert reading["pairs"]["t=0.15 q=0.75"]["power"] == 0.0


def test_four_false_passes_in_450_are_usable_and_five_are_not() -> None:
    passing = {"hedge_0.3": 150, "null_noise": 4}
    reading = pr.window_reading(_evaluations(passing), "w", 150)
    zero = reading["pairs"]["t=0.05 q=0.75"]["zero"]
    assert (zero["passes"], zero["total"]) == (4, 450)
    assert (zero["low"], zero["high"]) == fp.wilson(4, 450)
    assert reading["verdict"] == "threshold"
    reading = pr.window_reading(_evaluations({**passing, "null_carry": 1}), "w", 150)
    assert reading["pairs"]["t=0.05 q=0.75"]["usable"] is False
    assert reading["verdict"] == "no_threshold" and reading["threshold"] is None


def test_the_usable_line_is_the_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pr, "USABLE_LINE", 0.02)
    reading = pr.window_reading(_evaluations({"hedge_0.3": 150, "null_noise": 4}), "w", 150)
    assert reading["verdict"] == "no_threshold"
    # An upper bound exactly on the line is usable.
    monkeypatch.setattr(pr, "USABLE_LINE", fp.wilson(4, 450)[1])
    reading = pr.window_reading(_evaluations({"hedge_0.3": 150, "null_noise": 4}), "w", 150)
    assert reading["verdict"] == "threshold"


def test_power_is_hedge_three_tenths_and_needs_eighty_percent() -> None:
    reading = pr.window_reading(_evaluations({"hedge_0.3": 119, "hedge_0.5": 150}), "w", 150)
    assert reading["pairs"]["t=0.05 q=0.75"]["power"] == pytest.approx(119 / 150)
    assert reading["verdict"] == "no_threshold"
    reading = pr.window_reading(_evaluations({"hedge_0.3": 120}), "w", 150)
    assert reading["pairs"]["t=0.05 q=0.75"]["enough"] is True
    assert reading["verdict"] == "threshold"
    families = reading["pairs"]["t=0.05 q=0.75"]["families"]
    assert list(families) == list(pr.FAMILIES) and families["hedge_0.3"]["passes"] == 120


@pytest.mark.parametrize("family", ["null_noise", "null_carry", "null_pos_corr", "hedge_0.3"])
def test_an_error_or_a_missing_seed_in_a_read_family_is_incomplete(family: str) -> None:
    rows = _evaluations({"hedge_0.3": 150})
    errored = [
        {**row, "numbers": None, "error": "ValueError: x"}
        if row["family"] == family and row["seed"] == fp.seed_list(1)[0]
        else row
        for row in rows
    ]
    assert pr.window_reading(errored, "w", 150)["verdict"] == "incomplete"
    missing = [r for r in rows if not (r["family"] == family and r["seed"] == fp.seed_list(1)[0])]
    reading = pr.window_reading(missing, "w", 150)
    assert reading["verdict"] == "incomplete" and reading["threshold"] is None


def test_errors_in_the_printed_families_leave_the_reading_complete() -> None:
    rows = [
        {**row, "numbers": None, "error": "ValueError: x"} if row["family"] == "hedge_0.5" else row
        for row in _evaluations({"hedge_0.3": 150})
    ]
    reading = pr.window_reading(rows, "w", 150)
    assert reading["verdict"] == "threshold"
    assert reading["pairs"]["t=0.05 q=0.75"]["families"]["hedge_0.5"]["errors"] == 150


def test_the_reading_counts_segments_and_fallbacks_of_its_window_only() -> None:
    rows = _evaluations({"hedge_0.3": 150})
    rows[0] = {**rows[0], "numbers": _numbers(count=12, fallback=True)}
    rows.append(_row("null_noise", 1, _numbers(count=40, fallback=True), key="other"))
    reading = pr.window_reading(rows, "w", 150)
    assert reading["counted_segments"] == {"min": 10, "max": 12}
    assert reading["quiet_fallbacks"] == 1


def test_the_direction_reading_prints_every_family() -> None:
    rows = _evaluations({"hedge_0.3": 3}, seeds=3)
    reading = pr.direction_reading(rows, "w")
    assert list(reading["families"]) == list(pr.FAMILIES)
    assert reading["families"]["hedge_0.3"]["passes"] == 3
    assert reading["families"]["null_noise"]["rules"]["S2-2"] == 0
    assert reading["counted_segments"] == {"min": 10, "max": 10} and reading["quiet_fallbacks"] == 0


def test_the_outcome_needs_one_window_with_a_pair() -> None:
    windows = {
        "a": pr.ProbeWindow("a", "us_market", US_WINDOW, True),
        "b": pr.ProbeWindow("b", "us_market", US_WINDOW, True),
        "q": pr.ProbeWindow("q", "qqq", QQQ_WINDOW, False),
    }
    found = {"verdict": "threshold"}
    none = {"verdict": "no_threshold"}
    assert pr.outcome({"a": found, "b": none, "q": {}}, windows) == "threshold_found"
    assert pr.outcome({"a": none, "b": none, "q": {}}, windows) == "no_threshold"
    assert pr.outcome({"a": found, "b": {"verdict": "incomplete"}, "q": {}}, windows) == (
        "incomplete"
    )


# --- windows and data ----------------------------------------------------------------------------


def test_the_windows_are_the_gates_us_market_types_then_qqq() -> None:
    windows = pr.probe_windows(LOADED, GATE)
    assert list(windows) == [
        "us_market_1929_2010",
        "us_market_1962_2010",
        "qqq_long_history_development",
    ]
    assert [(w.market, w.decides) for w in windows.values()] == [
        ("us_market", True),
        ("us_market", True),
        ("qqq", False),
    ]
    assert windows["us_market_1929_2010"].window == Window(
        "us_market_1929_2010", "development", date(1929, 1, 2), date(2010, 12, 31)
    )
    assert windows["us_market_1962_2010"].window.start == date(1962, 1, 2)
    assert all(w.key == key and w.window.role == "development" for key, w in windows.items())


def test_a_named_question_type_limits_the_windows() -> None:
    windows = pr.probe_windows(LOADED, GATE, ["us_market_bond_trial"])
    assert list(windows) == ["us_market_1962_2010", "qqq_long_history_development"]
    with pytest.raises(ValueError, match="is not a us_market question type"):
        pr.probe_windows(LOADED, GATE, ["qqq_direction"])


def test_the_plan_config_is_t0002s_primary_and_the_ruler_needs_a_non_cash_reserve() -> None:
    config = pr.plan_config(PLAN)
    assert (config.id, config.function, config.params, config.reserve) == (
        "qqq60_t10y40",
        "fixed_exposure",
        {"exposure": 0.6},
        "treasury_10y",
    )
    _, other = tr.load_trial(ROOT_DIR, "T0001")
    with pytest.raises(ValueError, match="whose reserve is not cash"):
        pr.plan_config(other)


def test_the_loaded_data_are_t0002s_markets_cut_at_2010(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[str, str]] = []

    def market_data(market: str, gate: tr.TrialGate, plan: tr.TrialPlan, **_: Any) -> BenchData:
        seen.append((market, plan.trial_id))
        base = _qqq_data() if market == "qqq" else _us_data(end="2012-12-31")
        return replace(base, source={**base.source, "signal_snapshot_id": "sig-7ad8aa00b736"})

    monkeypatch.setattr(tr, "market_data", market_data)
    datasets, gate, plan, loaded, root_dir = pr.load_data(Path("unused"))
    assert seen == [("us_market", "T0002"), ("qqq", "T0002")]
    assert plan.trial_id == "T0002" and gate == GATE and root_dir == ROOT_DIR
    assert loaded.sha256 == LOADED.sha256
    assert datasets["us_market"].returns.index.max() <= pd.Timestamp("2010-12-31")
    assert datasets["us_market"].inputs.index.max() <= pd.Timestamp("2010-12-31")
    assert datasets["qqq"].returns.index.max() <= pd.Timestamp("2010-12-31")


def test_the_loader_refuses_other_data(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tr, "market_data", lambda *_, **__: US)
    with pytest.raises(ValueError, match="are not of sig-7ad8aa00b736"):
        pr.load_data(Path("unused"))


def test_the_target_check_counts_each_windows_days() -> None:
    rows = pr.check_targets(DATA, WINDOWS, PLAN)
    assert [(r["window_key"], r["missing_days"], r["ok"]) for r in rows] == [
        ("us_test_1929", 0, True),
        ("qqq_test", 0, True),
    ]
    assert rows[0]["window_days"] == len(US.inputs.loc["1929-01-02":"1936-12-31"])
    broken = pr.check_targets({"qqq": QQQ}, WINDOWS, PLAN)
    assert broken[0]["ok"] is False and "error" in broken[0]


# --- the run -------------------------------------------------------------------------------------


def test_the_tasks_cover_every_family_seed_and_window() -> None:
    work = pr.tasks(2, WINDOWS)
    assert work == [
        (family, seed, key) for family in pr.FAMILIES for seed in fp.seed_list(2) for key in WINDOWS
    ]


def test_each_evaluation_is_the_measured_numbers_with_ratios(probe: dict[str, Any]) -> None:
    rows = probe["evaluations"]
    assert [(r["family"], r["seed"], r["window_key"]) for r in rows] == pr.tasks(SEEDS, WINDOWS)
    row = next(r for r in rows if r["family"] == "hedge_0.3" and r["window_key"] == "us_test_1929")
    assert row["role"] == "hedge" and row["error"] is None
    reserve = pr.reserve_returns(US.returns, pr.FAMILIES["hedge_0.3"], row["seed"], DECLINES, DAYS)
    full = _measure(reserve)
    expected = {key: value for key, value in full.items() if key != "segments"}
    expected["ratios"] = [s["ratio"] for s in full["segments"]]
    assert row["numbers"] == gp._clean(expected)
    assert len(row["numbers"]["ratios"]) == len(DECLINES)


def test_the_qqq_window_reads_its_own_declines(probe: dict[str, Any]) -> None:
    row = next(r for r in probe["evaluations"] if r["window_key"] == "qqq_test")
    declines = pr.window_declines(QQQ.returns["qqq"], QQQ_WINDOW)
    assert len(row["numbers"]["ratios"]) == len(declines) > 0
    assert probe["segments"]["qqq_test"]["count"] == len(declines)


def test_a_failing_evaluation_is_an_error_row(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*_: Any, **__: Any) -> dict[str, Any]:
        raise ValueError("boom\nsecond line")

    monkeypatch.setattr(pr, "evaluate", fail)
    pr._init(
        {
            "data": DATA,
            "windows": WINDOWS,
            "declines": {"us_test_1929": DECLINES, "qqq_test": []},
            "config": CONFIG,
            "loaded": LOADED,
        }
    )
    row = pr._task(("null_noise", 7, "us_test_1929"))
    assert row == {
        "family": "null_noise",
        "role": "zero",
        "seed": 7,
        "window_key": "us_test_1929",
        "numbers": None,
        "error": "ValueError: boom",
    }


def test_the_readings_and_the_outcome_of_the_run(probe: dict[str, Any]) -> None:
    readings = probe["readings"]
    assert list(readings) == list(WINDOWS)
    deciding = readings["us_test_1929"]
    assert deciding == gp._clean(pr.window_reading(probe["evaluations"], "us_test_1929", SEEDS))
    assert readings["qqq_test"] == gp._clean(pr.direction_reading(probe["evaluations"], "qqq_test"))
    assert probe["outcome"] == pr.outcome(readings, WINDOWS)
    assert probe["complete"] is True


def test_a_failed_evaluation_leaves_the_run_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    real = pr.evaluate

    def flaky(data: BenchData, reserve: pd.Series, *args: Any) -> dict[str, Any]:
        if data is QQQ:
            raise ValueError("no qqq")
        return real(data, reserve, *args)

    monkeypatch.setattr(pr, "evaluate", flaky)
    result = pr.run_probe(DATA, LOADED, GATE, PLAN, ROOT_DIR, seeds=1, windows=WINDOWS)
    assert result["complete"] is False
    assert result["readings"]["us_test_1929"]["verdict"] != "incomplete"
    assert result["readings"]["qqq_test"]["families"]["null_noise"]["errors"] == 1


def test_parallel_workers_give_the_same_evaluations(probe: dict[str, Any]) -> None:
    result = pr.run_probe(
        DATA, LOADED, GATE, PLAN, ROOT_DIR, seeds=SEEDS, windows=WINDOWS, workers=2
    )
    assert result["evaluations"] == probe["evaluations"]
    assert result["readings"] == probe["readings"]


def test_the_data_must_be_cut_and_the_seeds_positive() -> None:
    late = {**DATA, "us_market": _us_data(start="2009-01-01", end="2011-03-31")}
    with pytest.raises(ValueError, match="cut them first"):
        pr.run_probe(late, LOADED, GATE, PLAN, ROOT_DIR, windows=WINDOWS)
    with pytest.raises(ValueError, match="at least one seed"):
        pr.run_probe(DATA, LOADED, GATE, PLAN, ROOT_DIR, seeds=0, windows=WINDOWS)


def test_the_head_names_the_plan_the_gate_the_thresholds_and_the_windows(
    probe: dict[str, Any],
) -> None:
    assert probe["run_id"] == pr.RUN_ID and probe["owner_decision"] == pr.OWNER_DECISION
    assert probe["plan_id"] == "T0002"
    assert probe["plan_sha256"] == lg.chain_digest(TRIALS.joinpath("T0002.yaml").read_bytes())
    assert probe["gate_sha256"] == lg.chain_digest(TRIALS.joinpath("prereg.yaml").read_bytes())
    assert probe["settings_sha256"] == LOADED.sha256 and probe["cost_bps"] == LOADED.cost_bps
    assert probe["config"] == {
        "id": "qqq60_t10y40",
        "exposure": 0.6,
        "reserve": "treasury_10y",
        "rebalance": {"schedule": "month_end", "band": 0.0},
    }
    assert probe["thresholds"] == {
        "decline_line": 0.1,
        "thresholds": [0.05, 0.1, 0.15],
        "shares": [0.6, 0.75],
        "pairs": [list(pair) for pair in pr.PAIRS],
        "geometric_floor": -0.01,
        "cost_rerun_bps": 25.0,
        "min_count": 4,
        "small_loss": -0.02,
        "quiet_share": 0.5,
        "power_family": "hedge_0.3",
        "power_line": 0.8,
        "usable_line": 0.025,
    }
    assert probe["families"] == [
        {
            "family_id": f.family_id,
            "role": f.role,
            "kind": f.kind,
            "size": f.size,
            "noise": f.noise,
            "stream": f.stream,
        }
        for f in pr.FAMILIES.values()
    ]
    assert probe["windows"] == {key: pr.window_record(w) for key, w in WINDOWS.items()}
    assert probe["windows"]["qqq_test"] == {
        "window_id": "qqq_test",
        "market": "qqq",
        "start": "1999-03-10",
        "end": "2004-12-31",
        "decides": False,
    }
    assert probe["cutoff"] == "2010-12-31" and probe["seed_start"] == fp.SEED_START
    assert (probe["seeds"], probe["trading_days_per_year"]) == (SEEDS, DAYS)
    assert probe["source"] == {market: DATA[market].source for market in ("us_market", "qqq")}
    assert "provenance" in probe


def test_the_segment_lists_are_in_the_result(probe: dict[str, Any]) -> None:
    found = probe["segments"]
    assert list(found) == list(WINDOWS)
    assert found["us_test_1929"] == gp._clean(
        {
            "window": pr.window_record(WINDOWS["us_test_1929"]),
            "count": len(DECLINES),
            "segments": [s.__dict__ for s in DECLINES],
        }
    )


def test_the_journal_holds_the_head_the_segments_and_the_evaluations(
    probe_run: tuple[dict[str, Any], Path],
) -> None:
    result, journal = probe_run
    lines = [json.loads(line) for line in journal.read_text(encoding="utf-8").splitlines()]
    assert lines[0]["run_id"] == pr.RUN_ID and "evaluations" not in lines[0]
    assert lines[1] == {"segments": result["segments"]}
    assert lines[2:] == result["evaluations"]


def test_the_journal_is_never_overwritten(tmp_path: Path) -> None:
    journal = tmp_path / pr.JOURNAL_FILE
    journal.write_text("", encoding="utf-8")
    with pytest.raises(FileExistsError):
        pr.run_probe(DATA, LOADED, GATE, PLAN, ROOT_DIR, seeds=1, windows=WINDOWS, journal=journal)


def test_the_probe_writes_nothing_under_the_research_root(tmp_path: Path) -> None:
    before = sorted(str(path) for path in ROOT_DIR.rglob("*") if "__pycache__" not in path.parts)
    pr.run_probe(
        DATA,
        LOADED,
        GATE,
        PLAN,
        ROOT_DIR,
        seeds=1,
        windows=WINDOWS,
        journal=tmp_path / pr.JOURNAL_FILE,
    )
    after = sorted(str(path) for path in ROOT_DIR.rglob("*") if "__pycache__" not in path.parts)
    assert after == before


# --- the report and the commands -----------------------------------------------------------------


def test_the_result_is_written_as_json_and_markdown(probe: dict[str, Any], tmp_path: Path) -> None:
    pr.write_result(probe, tmp_path)
    assert json.loads(tmp_path.joinpath(pr.RESULT_JSON).read_text(encoding="utf-8")) == probe
    text = tmp_path.joinpath(pr.RESULT_MD).read_text(encoding="utf-8")
    assert text.startswith("# P1 组合尺子探针（TRADING-2565-38）")
    assert f"### us_test_1929（1929-01-02..1936-12-31，判）：{len(DECLINES)} 段" in text
    assert "### qqq_test（1999-03-10..2004-12-31，只印）：" in text
    assert "| 段 | 段内月份 | 月数 | 市场变化 |" in text
    assert "## 判定（每个窗口按事先写死的读法：先取 q 大的，再取 t 小的）" in text
    assert "| us_test_1929 | 0.05 | 0.75 |" in text and "| us_test_1929 | 0.15 | 0.60 |" in text
    assert "- us_test_1929：" in text and "平时超额退回整窗口 0 次" in text
    assert "采纳：" in text and "## 各族通过率（只印）" in text
    assert "| hedge_0.3_noise_0.15 | 0.15 | 0.60 |" in text
    assert "### qqq_test（第二段只查方向" in text
    assert "## 分解（只印，各族种子的中位数，点/年）" in text
    assert "储备自身超额（全窗口）" in text and "平时超额（段外）" in text
    assert "## 出错" not in text


def test_the_report_names_a_chosen_pair_and_lists_errors(probe: dict[str, Any]) -> None:
    reading = {**probe["readings"]["us_test_1929"], "verdict": "threshold"}
    reading.update(threshold=0.1, share=0.75)
    rows = [{**probe["evaluations"][0], "numbers": None, "error": "ValueError: x"}]
    changed = {
        **probe,
        "readings": {**probe["readings"], "us_test_1929": reading},
        "outcome": "threshold_found",
        "evaluations": rows + probe["evaluations"][1:],
    }
    text = pr.render_markdown(changed)
    assert "- us_test_1929：判出 t=0.10 q=0.75；" in text
    assert pr.OUTCOME_TEXT["threshold_found"] in text
    assert "## 出错（1 次，列前 20 次）" in text and "ValueError: x" in text


def _loaded_data(_: Path) -> Any:
    return DATA, GATE, PLAN, LOADED, ROOT_DIR


def test_the_command_refuses_to_overwrite(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    called: list[Path] = []
    monkeypatch.setattr(pr, "load_data", lambda root: called.append(root))
    tmp_path.joinpath(pr.RESULT_JSON).write_text("{}", encoding="utf-8")
    outcome = CliRunner().invoke(pr.app, ["run", "--out", str(tmp_path)])
    assert outcome.exit_code == 1 and "拒绝覆盖" in outcome.output and called == []


def test_the_commands_stop_when_the_data_do_not_load(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fail(_: Path) -> Any:
        raise ValueError("no snapshot")

    monkeypatch.setattr(pr, "load_data", fail)
    out = tmp_path / "out"
    for args in (["check"], ["run", "--out", str(out)]):
        outcome = CliRunner().invoke(pr.app, args)
        assert outcome.exit_code == 1
        assert "portfolio_ruler_check: 数据载入失败：no snapshot" in outcome.output
    assert not out.exists()


def test_an_unknown_question_type_stops_the_commands(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pr, "load_data", _loaded_data)
    outcome = CliRunner().invoke(pr.app, ["check", "--question-type", "qqq_direction"])
    assert outcome.exit_code == 1 and "portfolio_ruler_check: 窗口不能用：" in outcome.output


def test_the_check_prints_the_declines_and_the_targets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pr, "load_data", _loaded_data)
    seen: list[Any] = []

    def windows(loaded: Any, gate: Any, question_types: Any = None) -> Any:
        seen.append(question_types)
        return WINDOWS

    monkeypatch.setattr(pr, "probe_windows", windows)
    outcome = CliRunner().invoke(
        pr.app, ["check", "--question-type", "us_market_trial", "--question-type", "x"]
    )
    assert outcome.exit_code == 0, outcome.output
    assert seen == [["us_market_trial", "x"]]
    first = DECLINES[0]
    assert f"portfolio_ruler_check: us_test_1929 下跌段 {len(DECLINES)} 段（判）" in outcome.output
    assert (
        f"portfolio_ruler_check: us_test_1929 {first.id} {first.start}..{first.end} "
        f"{first.months} 个月 {gp._pct(first.change)}"
    ) in outcome.output
    assert (
        "portfolio_ruler_check: qqq_test 下跌段" in outcome.output and "（只印）" in outcome.output
    )
    assert "portfolio_ruler_check: us_test_1929 窗口内" in outcome.output
    assert "缺 0 天，可以" in outcome.output


def test_the_run_stops_when_the_check_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    early = {**DATA, "us_market": _us_data(end="1928-12-31")}
    monkeypatch.setattr(pr, "load_data", lambda _: (early, GATE, PLAN, LOADED, ROOT_DIR))
    monkeypatch.setattr(pr, "probe_windows", lambda *_: WINDOWS)
    monkeypatch.setattr(pr, "run_probe", lambda *_, **__: pytest.fail("the probe ran"))
    out = tmp_path / "out"
    outcome = CliRunner().invoke(pr.app, ["run", "--out", str(out)])
    assert outcome.exit_code == 1 and "开跑前检查没过" in outcome.output
    assert "portfolio_ruler_check: us_test_1929 窗口内 0 天，缺 0 天，不行" in outcome.output
    assert not out.joinpath(pr.JOURNAL_FILE).exists()


def test_the_command_runs_the_probe_and_writes_the_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(pr, "load_data", _loaded_data)
    monkeypatch.setattr(pr, "probe_windows", lambda *_: WINDOWS)
    seen: dict[str, Any] = {}
    real = pr.run_probe

    def small(*args: Any, **kwargs: Any) -> dict[str, Any]:
        seen.update(kwargs)
        return real(*args, **{**kwargs, "seeds": 1})

    monkeypatch.setattr(pr, "run_probe", small)
    out = tmp_path / "out"
    outcome = CliRunner().invoke(pr.app, ["run", "--out", str(out), "--workers", "1"])
    assert outcome.exit_code == 0, outcome.output
    assert (seen["seeds"], seen["workers"], seen["windows"]) == (150, 1, WINDOWS)
    assert seen["journal"] == out / pr.JOURNAL_FILE
    assert "portfolio_ruler_check: us_test_1929 t=0.05 q=0.75 误放" in outcome.output
    assert "portfolio_ruler_check: us_test_1929 判定 " in outcome.output
    assert "portfolio_ruler_check: qqq_test 只印：hedge_0.3 方向三条都过" in outcome.output
    assert "portfolio_ruler_check: 采纳 " in outcome.output and "完成" in outcome.output
    result = json.loads(out.joinpath(pr.RESULT_JSON).read_text(encoding="utf-8"))
    assert result["seeds"] == 1 and len(result["evaluations"]) == len(pr.FAMILIES) * 2
    assert out.joinpath(pr.JOURNAL_FILE).exists() and out.joinpath(pr.RESULT_MD).exists()
