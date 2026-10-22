"""TRADING-2565-51: H-B conditional reserve probe on synthetic data shaped like the long history."""

from __future__ import annotations

import hashlib
import itertools
import json
import math
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from test_research_policy_table import LOADED, PROVENANCE, ROOT_DIR
from typer.testing import CliRunner

from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import (
    ASSETS,
    BenchData,
    from_daily_frame,
    from_us_market,
    with_reserves,
    with_signal_sources,
)
from ai_trading_system.research_bench.prereg import evaluate_rules
from ai_trading_system.research_bench.screen_settings import load_screen_settings
from ai_trading_system.research_bench.settings import EpisodeSpec
from ai_trading_system.research_bench.simulate import Rebalance, Simulation, schedule_mask, simulate
from ai_trading_system.research_probes import conditional_reserve as cr
from ai_trading_system.research_probes import gate_power as gp

pytestmark = pytest.mark.filterwarnings("ignore:invalid value encountered in log1p")

GATE, PLAN = tr.load_trial(ROOT_DIR, cr.PLAN_ID)
SPECS = {s.source_id: s for s in load_screen_settings().settings.sources}
NS = {"s": "http://www.w3.org/2000/svg"}


def _monthly(start: str, end: str, seed: int, level: float) -> pd.DataFrame:
    months = pd.date_range(start, end, freq="MS")
    rng = np.random.default_rng(seed)
    step = np.arange(len(months))
    values = level + 2.0 * np.sin(step / 40.0) + np.cumsum(rng.normal(0.0, 0.05, len(months)))
    return pd.DataFrame({"value": np.clip(values, 0.5, 20.0)}, index=months)


FRAMES = {
    "aaa_monthly": _monthly("1919-01-01", "2012-12-01", 7, 5.0),
    "shiller_gs10": _monthly("1871-01-01", "2012-12-01", 8, 4.5),
}


def _attach(data: BenchData) -> BenchData:
    lags = {k: SPECS[k] for k in FRAMES}
    sourced = with_signal_sources(data, FRAMES, lags, snapshot_id=PLAN.data.signal_snapshot_id)
    return with_reserves(sourced, FRAMES)


def _us_data(seed: int = 5) -> BenchData:
    """Market returns with month-by-month volatility and a cash rate that rises and falls."""

    index = pd.bdate_range("1926-07-01", "2012-12-31", name="date")
    rng = np.random.default_rng(seed)
    codes, uniques = pd.factorize(index.to_period("M"))
    vol = np.exp(rng.normal(math.log(0.01), 0.5, len(uniques)))
    rf = 0.0001 * (1.0 + 0.5 * np.sin(2 * math.pi * np.arange(len(uniques)) / 60.0))
    factors = pd.DataFrame(
        {
            "Mkt-RF": rng.normal(0.0003, 1.0, len(index)) * vol[codes],
            "SMB": 0.0,
            "HML": 0.0,
            "RF": rf[codes],
        },
        index=index,
    )
    snapshot = PLAN.data.signal_snapshot_id
    return _attach(from_us_market({"ff_factors_daily": factors}, snapshot_id=snapshot))


def _qqq_data(seed: int = 3) -> BenchData:
    index = pd.bdate_range("1994-01-03", "2012-12-31", name="date")
    rng = np.random.default_rng(seed)
    r = rng.normal(0.0004, 0.018, len(index))
    price = 100 * np.cumprod(1 + r)
    daily = pd.DataFrame(
        {
            "etf_reference_return": np.where(index >= "1999-03-10", r, np.nan),
            "qqq_proxy_return": r,
            "cash_return": 0.0001,
            "lev2_return": 2 * r,
            "lev3_return": 3 * r,
            "ndx_price": price,
            "ndx_tr": price,
            "vix": 20.0,
            "vxn": 25.0,
            "vxo": 20.0,
            "cash_rate_annual": 0.025,
        },
        index=index,
    )
    data = from_daily_frame(
        daily, qqq_series="actual", source={"snapshot_id": PLAN.data.snapshot_id}
    )
    return _attach(data)


RAW = {"us_market": _us_data(), "qqq": _qqq_data()}
DATA = {market: cr.cut(data) for market, data in RAW.items()}
WINDOWS = {market: cr.market_window(LOADED, GATE, market) for market in cr.MARKETS}
US_RULES = cr._rules(GATE, "us_market", WINDOWS["us_market"])


def _record(found: dict[str, Any]) -> dict[str, Any]:
    """A record in the published shape, taken from a same-data T0008 run."""

    portfolio = found["portfolio"]
    return {
        "comparison": found["comparison"]["geometric_annual_return"],
        "cost_25": found["cost_25"]["geometric_annual_return"],
        **{k: portfolio[k] for k in ("median_ratio", "positive_share", "rebalance_return")},
        "segment_count": portfolio["segment_count"],
        "excluded": list(portfolio["excluded"]),
        "ratios": {row["id"]: row["ratio"] for row in portfolio["segments"]},
    }


@pytest.fixture(scope="module")
def found() -> dict[str, Any]:
    return {
        market: cr.t0008_run(DATA[market], GATE, PLAN, market, LOADED, WINDOWS[market])
        for market in cr.MARKETS
    }


@pytest.fixture(scope="module")
def records(found: dict[str, Any]) -> dict[str, Any]:
    return {market: _record(found[market]) for market in cr.MARKETS}


@pytest.fixture(scope="module")
def run(records: dict[str, Any], tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    out = tmp_path_factory.mktemp("h_b")
    result = cr.run_probe(
        DATA, LOADED, GATE, PLAN, ROOT_DIR, provenance=PROVENANCE, out_dir=out, records=records
    )
    return {"result": result, "out": out}


# --- the state -------------------------------------------------------------------------------

DAYS = pd.bdate_range("2000-01-03", "2001-12-31")
MONTH_ENDS = DAYS[np.asarray(schedule_mask(DAYS, "month_end"), dtype=bool)]


def _inputs(month_values: list[float], noise: bool = True) -> pd.DataFrame:
    """One value per month, the month-end day carrying it; other days carry noise."""

    codes, _ = pd.factorize(DAYS.to_period("M"))
    values = np.asarray(month_values, dtype=float)[codes]
    if noise:
        values = values + np.where(DAYS.isin(MONTH_ENDS), 0.0, 100.0 * np.sin(np.arange(len(DAYS))))
    return pd.DataFrame({cr.STATE_COLUMN: values, "other": 1.0}, index=DAYS)


# 2000-01 to 2001-12; month 15 falls (to Treasury), months 16 and 20 equal six months before.
MONTH_VALUES = [
    float(v)
    for v in "5.0 5.2 5.1 4.9 5.0 5.3 5.1 5.2 5.1 4.9 5.0 5.4 "
    "5.1 5.2 5.5 4.0 5.0 5.3 5.3 5.2 5.5 4.0 5.0 5.3".split()
]


def _expected_states(values: list[float]) -> list[float]:
    found = []
    for k in range(len(values)):
        if k < 6 or math.isnan(values[k]) or math.isnan(values[k - 6]):
            found.append(math.nan)
        else:
            found.append(1.0 if values[k] - values[k - 6] < 0 else 0.0)
    return found


def test_the_state_is_the_six_month_end_change_and_holds_until_the_next_month_end() -> None:
    state = cr.reserve_state(_inputs(MONTH_VALUES), cr.state_params())
    assert state.index.equals(DAYS)
    expected = _expected_states(MONTH_VALUES)
    # A zero change keeps cash; only a fall moves the reserve to Treasury.
    assert MONTH_VALUES[16] == MONTH_VALUES[10] and MONTH_VALUES[20] == MONTH_VALUES[14]
    assert expected[16] == expected[20] == 0.0 and expected[15] == 1.0
    assert state.loc[: MONTH_ENDS[6] - pd.Timedelta(days=1)].isna().all()
    for k, end in enumerate(MONTH_ENDS):
        following = (
            MONTH_ENDS[k + 1] if k + 1 < len(MONTH_ENDS) else DAYS[-1] + pd.Timedelta(days=1)
        )
        held = state.loc[(state.index >= end) & (state.index < following)].to_numpy()
        if math.isnan(expected[k]):
            assert np.isnan(held).all()
        else:
            assert (held == expected[k]).all(), end
    # Only month-end values count: the same month ends without the noise give the same state.
    assert state.equals(cr.reserve_state(_inputs(MONTH_VALUES, noise=False), cr.state_params()))


def test_a_missing_month_end_leaves_its_own_month_and_the_month_six_later_without_a_state() -> None:
    values = list(MONTH_VALUES)
    values[8] = math.nan
    state = cr.reserve_state(_inputs(values, noise=False), cr.state_params())
    at_ends = state.loc[MONTH_ENDS].to_numpy()
    expected = _expected_states(values)
    assert np.isnan(at_ends[8]) and np.isnan(at_ends[14])
    assert np.array_equal(at_ends, np.asarray(expected), equal_nan=True)


def test_the_months_parameter_and_the_column_are_used() -> None:
    inputs = _inputs(MONTH_VALUES, noise=False)
    two = cr.reserve_state(inputs, cr.state_params(months=2)).loc[MONTH_ENDS].to_numpy()
    assert np.isnan(two[:2]).all()
    assert list(two[2:]) == [
        1.0 if MONTH_VALUES[k] - MONTH_VALUES[k - 2] < 0 else 0.0 for k in range(2, 24)
    ]
    other = cr.reserve_state(inputs, cr.state_params(column="other"))
    assert (other.loc[MONTH_ENDS[6] :] == 0.0).all()
    assert cr.state_params() == {"column": "source:aaa_monthly:value", "months": 6}


@pytest.mark.parametrize(
    ("params", "message"),
    [
        ({"column": cr.STATE_COLUMN}, "column and months"),
        ({"column": cr.STATE_COLUMN, "months": 6, "lag": 1}, "column and months"),
        ({"column": cr.STATE_COLUMN, "months": 0}, "positive integer"),
        ({"column": cr.STATE_COLUMN, "months": 6.0}, "positive integer"),
        ({"column": cr.STATE_COLUMN, "months": True}, "positive integer"),
        ({"column": "missing", "months": 6}, "no column missing"),
    ],
)
def test_state_parameters_are_checked(params: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        cr.reserve_state(_inputs(MONTH_VALUES), params)


# --- the simulator ---------------------------------------------------------------------------

SIM_DAYS = pd.bdate_range("2001-01-01", "2003-06-30")


def _market(seed: int = 4) -> tuple[pd.DataFrame, pd.Series, pd.Series]:
    rng = np.random.default_rng(seed)
    r = rng.normal(0.0004, 0.012, len(SIM_DAYS))
    returns = pd.DataFrame(
        {"qqq": r, "cash": 0.0001, "lev2": 2 * r, "lev3": 3 * r}, index=SIM_DAYS
    )[list(ASSETS)]
    treasury = pd.Series(rng.normal(0.0002, 0.004, len(SIM_DAYS)), index=SIM_DAYS)
    targets = pd.Series(rng.uniform(0.0, 1.6, len(SIM_DAYS)), index=SIM_DAYS)
    return returns, treasury, targets


@pytest.mark.parametrize("value", [0.0, 1.0])
def test_a_constant_state_is_the_bench_with_cash_or_treasury_in_the_cash_slot(value: float) -> None:
    returns, treasury, targets = _market()
    traded = returns.assign(cash=treasury) if value == 1.0 else returns
    state = pd.Series(value, index=SIM_DAYS)
    cases = itertools.product(
        ("daily", "week_end", "month_end"), (0.0, 0.1), (0, 2), (0.0, 4.0, 25.0)
    )
    for schedule, band, delay, cost in cases:
        kwargs: dict[str, Any] = {
            "start": date(2001, 3, 1),
            "end": date(2003, 5, 30),
            "rebalance": Rebalance(schedule, band),  # type: ignore[arg-type]
            "cost_bps": cost,
            "delay_days": delay,
        }
        ours = cr.simulate_reserve(returns, treasury, targets, state, **kwargs)
        bench = simulate(traded, targets, **kwargs)
        assert cr.same_days(ours, bench), (schedule, band, delay, cost)
        assert ours.start == bench.start
        assert (ours.daily["treasury"] == value).all()


def _flat() -> tuple[pd.DataFrame, pd.Series, pd.Series, pd.Series]:
    returns = pd.DataFrame(0.0, index=SIM_DAYS, columns=list(ASSETS))
    treasury = pd.Series(0.0, index=SIM_DAYS)
    targets = pd.Series(0.6, index=SIM_DAYS)
    state = pd.Series(0.0, index=SIM_DAYS)
    state.loc["2001-02-28":"2001-04-27"] = 1.0
    return returns, treasury, targets, state


def test_switching_the_reserve_trades_the_forty_percent_and_pays_for_it() -> None:
    returns, treasury, targets, state = _flat()
    sim = cr.simulate_reserve(
        returns,
        treasury,
        targets,
        state,
        start=date(2001, 2, 1),
        end=date(2001, 6, 29),
        rebalance=cr.REBALANCE,
        cost_bps=25.0,
    )
    traded = sim.daily.loc[sim.daily["turnover"] > 0]
    # Decided on the February and April month ends, traded the next day, held from the day after.
    assert list(traded.index) == [pd.Timestamp("2001-03-01"), pd.Timestamp("2001-05-01")]
    assert traded["turnover"].tolist() == pytest.approx([0.4, 0.4], abs=1e-15)
    assert traded["cost"].tolist() == pytest.approx([0.4 * 0.0025] * 2, abs=1e-15)
    assert sim.daily["return"].loc[traded.index].tolist() == pytest.approx([-0.001] * 2)
    held = sim.daily["treasury"]
    assert (held.loc[:"2001-03-01"] == 0.0).all()
    assert (held.loc["2001-03-02":"2001-05-01"] == 1.0).all()
    assert (held.loc["2001-05-02":] == 0.0).all()
    assert cr.switches(sim) == 2
    assert (sim.daily["exposure"] == 0.6).all()


def test_a_state_change_trades_inside_the_band_and_nothing_else_does() -> None:
    returns, treasury, targets, state = _flat()
    returns["qqq"] = 0.001
    sim = cr.simulate_reserve(
        returns,
        treasury,
        targets,
        state,
        start=date(2001, 2, 1),
        end=date(2001, 6, 29),
        rebalance=Rebalance("month_end", 0.5),
        cost_bps=4.0,
    )
    traded = sim.daily.index[sim.daily["turnover"] > 0]
    assert list(traded) == [pd.Timestamp("2001-03-01"), pd.Timestamp("2001-05-01")]
    # The trade goes back to the target, so the next day holds exactly 0.6.
    assert sim.daily["exposure"].loc["2001-03-02"] == pytest.approx(0.6, abs=1e-15)
    assert sim.daily["exposure"].loc["2001-03-01"] > 0.6
    fixed = cr.simulate_reserve(
        returns,
        treasury,
        targets,
        pd.Series(0.0, index=SIM_DAYS),
        start=date(2001, 2, 1),
        end=date(2001, 6, 29),
        rebalance=Rebalance("month_end", 0.5),
        cost_bps=4.0,
    )
    assert (fixed.daily["turnover"] == 0).all()


def test_missing_inputs_are_refused() -> None:
    returns, treasury, targets, state = _flat()
    kwargs: dict[str, Any] = {
        "start": date(2001, 2, 1),
        "end": date(2001, 6, 29),
        "rebalance": cr.REBALANCE,
        "cost_bps": 4.0,
    }
    gap = state.copy()
    gap.loc["2001-03-30"] = math.nan
    with pytest.raises(ValueError, match="no reserve state on 2001-03-30"):
        cr.simulate_reserve(returns, treasury, targets, gap, **kwargs)
    early = state.copy()
    early.loc["2001-01-31"] = math.nan
    with pytest.raises(ValueError, match="no reserve state on 2001-01-31"):
        cr.simulate_reserve(returns, treasury, targets, early, **kwargs)
    hole = treasury.copy()
    hole.loc["2001-04-10"] = math.nan
    with pytest.raises(ValueError, match="treasury has no return on 2001-04-10"):
        cr.simulate_reserve(returns, hole, targets, state, **kwargs)
    # Outside the window a missing Treasury return does not matter.
    before = treasury.copy()
    before.loc["2001-01-10"] = math.nan
    cr.simulate_reserve(returns, before, targets, state, **kwargs)
    for name, value in (("treasury", treasury), ("state", state), ("targets", targets)):
        args = {"treasury": treasury, "targets": targets, "state": state, name: value.iloc[1:]}
        with pytest.raises(ValueError, match=f"{name} must be indexed"):
            cr.simulate_reserve(returns, **args, **kwargs)
    with pytest.raises(ValueError, match="returns columns"):
        cr.simulate_reserve(
            returns[["cash", "qqq", "lev2", "lev3"]], treasury, targets, state, **kwargs
        )
    with pytest.raises(ValueError, match="delay_days"):
        cr.simulate_reserve(returns, treasury, targets, state, **kwargs, delay_days=-1)
    with pytest.raises(ValueError, match="cost_bps"):
        cr.simulate_reserve(returns, treasury, targets, state, **{**kwargs, "cost_bps": -1.0})


def test_the_reserve_leg_takes_treasury_on_held_days_and_cash_otherwise() -> None:
    returns, treasury, targets, state = _flat()
    treasury = pd.Series(0.003, index=SIM_DAYS)
    returns["cash"] = 0.0001
    sim = cr.simulate_reserve(
        returns,
        treasury,
        targets,
        state,
        start=date(2001, 2, 1),
        end=date(2001, 6, 29),
        rebalance=cr.REBALANCE,
        cost_bps=4.0,
    )
    leg = cr.reserve_leg(sim, treasury, returns["cash"])
    assert leg.index.equals(sim.daily.index)
    assert (leg.loc["2001-03-02":"2001-05-01"] == 0.003).all()
    assert (leg.loc[:"2001-03-01"] == 0.0001).all() and (leg.loc["2001-05-02":] == 0.0001).all()


def test_months_held_reads_the_state_of_the_month_end_before() -> None:
    state = cr.reserve_state(_inputs(MONTH_VALUES, noise=False), cr.state_params())
    at_ends = state.loc[MONTH_ENDS].to_numpy()
    # 2001-04 (index 15) is decided at the 2001-04 month end and governs 2001-05.
    assert at_ends[15] == 1.0 and at_ends[16] == 0.0
    assert cr.months_held(state, "2001-05", "2001-05") == (1, 1)
    assert cr.months_held(state, "2001-04", "2001-06") == (int(at_ends[14] == 1.0) + 1, 3)
    held, months = cr.months_held(state, "2000-08", "2001-12")
    assert months == 17
    assert held == int(np.nansum(at_ends[6:22] == 1.0))
    # Months after the last month end and before the first state count as not held.
    assert cr.months_held(state, "2002-01", "2002-02") == (int(at_ends[23] == 1.0), 2)
    assert cr.months_held(state, "2000-01", "2000-07") == (0, 7)


# --- r′: the held deduction (hb_row1_held_deduction) ------------------------------------------

HELD_DAYS = pd.bdate_range("2001-01-01", "2001-06-29")
HELD_SEGMENTS = [
    EpisodeSpec(id="decline_200102", start="2001-03", end="2001-03"),
    EpisodeSpec(id="decline_200103", start="2001-04", end="2001-05"),
]
HELD_ROWS = [
    {
        "id": "decline_200102",
        "start": "2001-03",
        "end": "2001-03",
        "difference": 0.004,
        "benchmark_return": -0.01,
        "counted": False,
    },
    {
        "id": "decline_200103",
        "start": "2001-04",
        "end": "2001-05",
        "difference": 0.006,
        "benchmark_return": -0.08,
        "counted": True,
    },
]


def _held(*spans: tuple[str, str | None]) -> Simulation:
    held = pd.Series(0.0, index=HELD_DAYS)
    for first, last in spans:
        held.loc[first:last] = 1.0
    return Simulation(daily=pd.DataFrame({"treasury": held}), start=pd.Timestamp("2000-12-29"))


def _deduct(sim: Simulation, treasury: pd.Series, cash: pd.Series) -> dict[str, Any]:
    reserve = cr.reserve_leg(sim, treasury, cash)
    return cr.held_deduction(sim, reserve, cash, HELD_ROWS, HELD_SEGMENTS, 0.0123, 250)


def test_the_held_deduction_counts_held_days_and_takes_the_excess_from_quiet_held_days() -> None:
    sim = _held(("2001-01-15", "2001-02-14"), ("2001-04-10", "2001-04-30"), ("2001-06-01", None))
    treasury = pd.Series(np.linspace(0.0002, 0.0009, len(HELD_DAYS)), index=HELD_DAYS)
    cash = pd.Series(0.0001, index=HELD_DAYS)
    found = _deduct(sim, treasury, cash)
    quiet = [
        *pd.bdate_range("2001-01-15", "2001-02-14"),
        *pd.bdate_range("2001-06-01", "2001-06-29"),
    ]
    logs = sum(math.log1p(treasury[d]) - math.log1p(cash[d]) for d in quiet)
    excess = math.expm1(logs * 250 / len(quiet))
    assert (found["excess"], found["days"], found["fallback"]) == (
        pytest.approx(excess, rel=1e-12),
        len(quiet),
        False,
    )
    held_days = len(pd.bdate_range("2001-04-10", "2001-04-30"))
    deduction = 0.4 * excess * held_days / 250
    assert found["segments"] == [
        {
            "id": "decline_200102",
            "held_days": 0,
            "days": len(pd.bdate_range("2001-03-01", "2001-03-31")),
            "deduction": 0.0,
            "cushion": 0.004,
            "ratio": None,
        },
        {
            "id": "decline_200103",
            "held_days": held_days,
            "days": len(pd.bdate_range("2001-04-01", "2001-05-31")),
            "deduction": pytest.approx(deduction, rel=1e-12),
            "cushion": pytest.approx(0.006 - deduction, rel=1e-12),
            "ratio": pytest.approx((0.006 - deduction) / 0.08, rel=1e-12),
        },
    ]
    # Treasury days that are not held, or held inside a segment, never enter the excess.
    moved = treasury.copy()
    moved.loc["2001-02-15":"2001-05-31"] += 0.01
    assert _deduct(sim, moved, cash)["excess"] == found["excess"]


def test_the_held_deduction_falls_back_when_no_quiet_day_is_held() -> None:
    sim = _held(("2001-04-10", "2001-04-30"))
    treasury = pd.Series(0.0004, index=HELD_DAYS)
    cash = pd.Series(0.0001, index=HELD_DAYS)
    found = _deduct(sim, treasury, cash)
    assert (found["excess"], found["days"], found["fallback"]) == (0.0123, 0, True)
    row = found["segments"][1]
    assert row["deduction"] == pytest.approx(0.4 * 0.0123 * row["held_days"] / 250, rel=1e-12)
    nothing = _deduct(_held(), treasury, cash)
    assert nothing["fallback"] and [r["deduction"] for r in nothing["segments"]] == [0.0, 0.0]
    assert nothing["segments"][1]["ratio"] == pytest.approx(0.006 / 0.08)


def test_same_numbers_compares_nested_values_within_the_tolerance() -> None:
    assert cr.same_numbers(
        {"a": [1.0, {"b": 2}], "c": None}, {"a": (1.0 + 1e-13, {"b": 2}), "c": None}
    )
    assert not cr.same_numbers({"a": 1.0}, {"a": 1.0 + 1e-11})
    assert not cr.same_numbers({"a": 1.0}, {"a": 1.0, "b": 2.0})
    assert not cr.same_numbers([1.0], [1.0, 2.0])
    assert cr.same_numbers(math.nan, math.nan) and not cr.same_numbers(math.nan, 0.0)
    assert not cr.same_numbers(True, 1) and cr.same_numbers(True, True)
    assert not cr.same_numbers("x", "y") and cr.same_numbers(0.1, 0.1 + 1e-11, tolerance=1e-10)


# --- the T0008 record ------------------------------------------------------------------------


def _found(record: dict[str, Any]) -> dict[str, Any]:
    segments = [
        {"id": k, "ratio": v, "counted": v is not None} for k, v in record["ratios"].items()
    ]
    return {
        "comparison": {"geometric_annual_return": record["comparison"]},
        "cost_25": {"geometric_annual_return": record.get("cost_25", 0.0)},
        "portfolio": {
            "median_ratio": record["median_ratio"],
            "positive_share": record.get("positive_share", 0.5),
            "rebalance_return": record["rebalance_return"],
            "segment_count": record["segment_count"],
            "excluded": list(record["excluded"]),
            "segments": segments,
        },
    }


def test_the_record_is_the_published_t0008_report() -> None:
    # trial-r0055 (US) and trial-r0056 (QQQ) as printed: four places for the lines, three for r.
    assert cr.T0008_RECORD == {
        "us_market": {
            "comparison": 0.0075,
            "cost_25": 0.0075,
            "median_ratio": 0.0017,
            "positive_share": 0.5,
            "rebalance_return": 0.0002,
            "segment_count": 24,
            "excluded": ["decline_198011"],
            "ratios": {
                "decline_192908": -0.029,
                "decline_193208": 0.036,
                "decline_193308": 0.015,
                "decline_193401": 0.057,
                "decline_193702": 0.007,
                "decline_193812": 0.022,
                "decline_193909": -0.054,
                "decline_194605": -0.026,
                "decline_194805": 0.043,
                "decline_195707": 0.236,
                "decline_196112": 0.018,
                "decline_196601": -0.284,
                "decline_196811": -0.353,
                "decline_197212": -0.188,
                "decline_197506": -0.222,
                "decline_197808": -0.103,
                "decline_198001": -0.672,
                "decline_198011": None,
                "decline_198306": -1.720,
                "decline_198708": -0.004,
                "decline_199005": -0.015,
                "decline_199806": 0.035,
                "decline_200003": 0.188,
                "decline_200710": 0.100,
                "decline_201004": 0.283,
            },
        },
        "qqq": {
            "comparison": 0.0134,
            "median_ratio": 0.0546,
            "rebalance_return": 0.0012,
            "segment_count": 6,
            "excluded": [],
            "ratios": {
                "decline_200003": 0.084,
                "decline_200211": 0.025,
                "decline_200412": -0.005,
                "decline_200601": -0.295,
                "decline_200710": 0.157,
                "decline_201004": 0.303,
            },
        },
    }


def test_the_published_t0008_record_is_consistent_with_itself() -> None:
    us, qqq = cr.T0008_RECORD["us_market"], cr.T0008_RECORD["qqq"]
    for record in (us, qqq):
        counted = [v for v in record["ratios"].values() if v is not None]
        assert record["segment_count"] == len(counted)
        assert record["excluded"] == [k for k, v in record["ratios"].items() if v is None]
        assert abs(float(np.median(counted)) - record["median_ratio"]) <= 0.0005
    assert us["positive_share"] == sum(v > 0 for v in us["ratios"].values() if v is not None) / 24
    assert len(us["ratios"]) == 25 and us["excluded"] == ["decline_198011"]
    assert set(qqq) == {
        "comparison",
        "median_ratio",
        "rebalance_return",
        "segment_count",
        "excluded",
        "ratios",
    }


@pytest.mark.parametrize("market", cr.MARKETS)
def test_record_differences_at_the_printed_places(market: str) -> None:
    record = cr.T0008_RECORD[market]
    assert cr.record_differences(_found(record), market) == []
    found = _found(record)
    found["comparison"]["geometric_annual_return"] += 0.00004
    found["portfolio"]["rebalance_return"] -= 0.00004
    first = next(r for r in found["portfolio"]["segments"] if r["counted"])
    first["ratio"] += 0.0004
    assert cr.record_differences(found, market) == []
    found["comparison"]["geometric_annual_return"] += 0.00002
    first["ratio"] += 0.0002
    differences = cr.record_differences(found, market)
    assert len(differences) == 2
    assert differences[0].startswith("comparison ")
    assert differences[1].startswith(f"{first['id']} r ")


def test_record_differences_name_counts_exclusions_and_segments() -> None:
    record = cr.T0008_RECORD["us_market"]
    found = _found(record)
    found["portfolio"]["segment_count"] = 23
    found["portfolio"]["excluded"] = ["decline_198011", "decline_198306"]
    row = next(r for r in found["portfolio"]["segments"] if r["id"] == "decline_198306")
    row["ratio"] = None
    differences = cr.record_differences(found, "us_market")
    assert "计入段数 23（记录 24）" in differences
    assert any(d.startswith("不计入的段") for d in differences)
    assert "decline_198306 r —（记录 -1.72）" in differences
    found["portfolio"]["segments"] = found["portfolio"]["segments"][1:]
    assert cr.record_differences(found, "us_market")[-1] == "下跌段和记录不同"
    cost = _found(record)
    cost["cost_25"]["geometric_annual_return"] = 0.0076
    assert cr.record_differences(cost, "us_market") == ["cost_25 0.0076（记录 0.0075）"]
    # QQQ printed no cost line, so its record has none to compare.
    qqq = _found(cr.T0008_RECORD["qqq"])
    qqq["cost_25"]["geometric_annual_return"] = 0.5
    assert cr.record_differences(qqq, "qqq") == []


# --- rows 1 to 4 -----------------------------------------------------------------------------


def _segment(sid: str, ratio: float | None) -> dict[str, Any]:
    return {
        "id": sid,
        "start": f"{sid[8:12]}-{sid[12:14]}",
        "counted": ratio is not None,
        "ratio": ratio,
    }


def _values(
    ratios: dict[str, float | None],
    geometric: float = 0.0,
    held: dict[str, float | None] | None = None,
    cash: tuple[str, ...] = (),
    **portfolio: float,
) -> dict[str, Any]:
    """r′ (held) is r unless given; the segments in cash hold the Treasury 0 months, the rest 1."""

    segments = [_segment(k, v) for k, v in ratios.items()]
    counted = [v for v in ratios.values() if v is not None]
    return {
        "held": {
            "excess": 0.01,
            "days": 100,
            "fallback": False,
            "segments": [
                {"id": k, "ratio": v, "held_months": 0 if k in cash else 1}
                for k, v in (held or ratios).items()
            ],
        },
        "comparison": {"geometric_annual_return": geometric},
        "cost_25": {"geometric_annual_return": geometric - 0.001},
        "portfolio": {
            "segments": segments,
            "segment_count": len(counted),
            "median_ratio": float(np.median(counted)) if counted else None,
            "positive_share": sum(v > 0 for v in counted) / len(counted) if counted else None,
            "rebalance_return": 0.0,
            **portfolio,
        },
    }


ROW1 = [f"decline_19{y}01" for y in range(66, 91, 3)]
BASE = {
    "decline_192908": -0.1,
    "decline_196512": -0.2,
    **{sid: -0.01 for sid in ROW1},
    "decline_199101": -0.2,
    "decline_200710": 0.1,
}


def _with(changes: dict[str, float | None]) -> dict[str, float | None]:
    return {**BASE, **changes}


@pytest.mark.parametrize(
    ("positive", "holds", "closes", "six"),
    [
        (2, False, True, False),
        (3, True, False, False),
        (5, True, False, False),
        (6, False, False, True),
    ],
)
def test_row_one_counts_positive_segments_in_1966_to_1990(
    positive: int, holds: bool, closes: bool, six: bool
) -> None:
    ratios = _with({sid: 0.02 for sid in ROW1[:positive]})
    # Segments just outside the span never count, whatever their sign.
    ratios.update({"decline_196512": 0.5, "decline_199101": 0.5})
    row1 = cr.row_readings(_values(ratios), _values(BASE), US_RULES)[0]
    assert row1["segments"] == ROW1
    assert (row1["count"], row1["positive"]) == (9, positive)
    assert (row1["base_count"], row1["base_positive"]) == (9, 0)
    assert (row1["holds"], row1["closes_p1"], row1["six_or_more"]) == (holds, closes, six)


def test_row_one_floor_is_strictly_below_minus_five_hundredths() -> None:
    at = _with({sid: 0.02 for sid in ROW1[:3]})
    at.update({sid: -0.05 for sid in ROW1[3:]})
    row1 = cr.row_readings(_values(at), _values(BASE), US_RULES)[0]
    assert row1["holds"] and row1["below_floor"] == [] and row1["min_ratio"] == -0.05
    at[ROW1[4]] = -0.0500001
    row1 = cr.row_readings(_values(at), _values(BASE), US_RULES)[0]
    assert not row1["holds"] and row1["closes_p1"] and row1["below_floor"] == [ROW1[4]]
    # An uncounted segment in the span is no reading.
    at[ROW1[4]] = None
    row1 = cr.row_readings(_values(at), _values(BASE), US_RULES)[0]
    assert row1["holds"] and row1["count"] == 8
    empty = {k: (None if k in ROW1 else v) for k, v in BASE.items()}
    row1 = cr.row_readings(_values(empty), _values(BASE), US_RULES)[0]
    assert not row1["holds"] and row1["min_ratio"] is None


def test_row_one_reads_r_prime_and_row_two_keeps_r() -> None:
    held = _with({sid: 0.02 for sid in ROW1[:4]})
    held["decline_192908"] = 0.5
    # By r every segment in the span is below the floor; by r′ four are positive.
    values = _values(_with({sid: -0.2 for sid in ROW1}), held=held)
    base = _values(BASE, held=_with({ROW1[0]: 0.03}))
    base["held"].update(excess=0.02, days=50, fallback=True)
    row1, row2 = cr.row_readings(values, base, US_RULES)[:2]
    assert (row1["reads"], row1["decision"]) == ("r′", cr.HELD_DEDUCTION)
    assert cr.HELD_DEDUCTION == "owner_decision:TRADING-2565:2026-10-22:hb_row1_held_deduction"
    assert row1["ratios"] == {sid: held[sid] for sid in ROW1}
    assert (row1["positive"], row1["below_floor"], row1["min_ratio"]) == (4, [], -0.01)
    assert row1["holds"] and not row1["closes_p1"]
    assert (row1["base_count"], row1["base_positive"]) == (9, 1)
    assert (row1["excess"], row1["excess_days"], row1["fallback"]) == (0.01, 100, False)
    assert [p["ratio"] for p in row2["pairs"]] == [-0.1, 0.1] and row2["holds"]


def test_row_one_counts_a_segment_that_never_held_the_treasury_as_zero() -> None:
    # Two segments held the Treasury 0 months: a tiny positive and a value below the floor.
    held = _with({sid: 0.02 for sid in ROW1[:3]})
    held.update({ROW1[3]: 0.001, ROW1[4]: -0.06, ROW1[5]: 0.001})
    cash = (ROW1[3], ROW1[4])
    row1 = cr.row_readings(_values(BASE, held=held, cash=cash), _values(BASE), US_RULES)[0]
    assert cr.CASH_ZERO == "owner_decision:TRADING-2565:2026-10-22:hb_row1_cash_zero"
    assert row1["cash_zero"] == list(cash)
    assert row1["ratios"][ROW1[3]] == 0.0 and row1["ratios"][ROW1[4]] == 0.0
    # A segment that held the Treasury counts by its sign, however small.
    assert row1["positive"] == 4 and row1["below_floor"] == [] and row1["min_ratio"] == -0.01
    assert row1["holds"] and not row1["closes_p1"] and not row1["six_or_more"]
    # Without the rule the same numbers read 5 positive and one below the floor.
    plain = cr.row_readings(_values(BASE, held=held), _values(BASE), US_RULES)[0]
    assert plain["cash_zero"] == [] and plain["positive"] == 5
    assert plain["below_floor"] == [ROW1[4]] and plain["closes_p1"]
    # Six counted as positive only through the zeroed segments stays at three.
    six = _with({sid: 0.02 for sid in ROW1[:6]})
    row1 = cr.row_readings(_values(BASE, held=six, cash=tuple(ROW1[3:6])), _values(BASE), US_RULES)[
        0
    ]
    assert (row1["positive"], row1["six_or_more"], row1["holds"]) == (3, False, True)
    # T0008's comparison counts follow the same rule.
    base = _values(BASE, held=_with({ROW1[0]: 0.03, ROW1[1]: 0.03}), cash=(ROW1[1],))
    assert cr.row_readings(_values(BASE), base, US_RULES)[0]["base_positive"] == 1


def test_the_reserve_path_carries_each_segments_held_months_for_row_one() -> None:
    data, window = DATA["qqq"], WINDOWS["qqq"]
    segments = cr.declines(GATE, "qqq", window)
    state = cr.reserve_state(data.inputs, cr.state_params())
    path = cr.reserve_path(data, state, window, LOADED, segments, fallback=0.01)
    found = [
        (r["id"], r["held_months"], cr.months_held(path.state, s.start, s.end))
        for r, s in zip(path.values["held"]["segments"], segments, strict=True)
    ]
    assert [sid for sid, _, _ in found] == [s.id for s in segments]
    assert all(months == held for _, months, (held, _) in found)
    assert any(0 < months < total for _, months, (_, total) in found)


def test_row_two_compares_counted_pairs_before_1963_and_after_1997() -> None:
    base = _values(
        {"decline_192908": 0.0, "decline_195707": 0.1, "decline_196512": 0.0, "decline_200710": 0.0}
    )
    ratios = {
        "decline_192908": 0.05,
        "decline_195707": None,
        "decline_196512": 0.9,
        "decline_200710": -0.05,
    }
    row2 = cr.row_readings(_values(ratios), base, US_RULES)[1]
    assert [p["id"] for p in row2["pairs"]] == ["decline_192908", "decline_200710"]
    assert row2["skipped"] == ["decline_195707"]
    assert row2["outside"] == [] and row2["holds"]
    ratios["decline_200710"] = -0.0500001
    row2 = cr.row_readings(_values(ratios), base, US_RULES)[1]
    assert row2["outside"] == ["decline_200710"] and not row2["holds"]
    assert row2["pairs"][1]["change"] == pytest.approx(-0.0500001)
    none = cr.row_readings(
        _values({"decline_196512": 0.1}), _values({"decline_196512": 0.1}), US_RULES
    )
    assert none[1]["pairs"] == [] and not none[1]["holds"]


def test_row_three_reads_the_three_p2_rules_and_row_four_the_geometric_floor() -> None:
    ratios = _with({sid: 0.2 for sid in ROW1})
    values = _values(ratios, geometric=-0.010, median_ratio=0.10, positive_share=0.75)
    readings = cr.row_readings(values, _values(BASE), US_RULES)
    row3, row4 = readings[2], readings[3]
    assert [g["metric"] for g in row3["rules"]] == list(cr.ROW3_METRICS)
    assert row3["holds"]
    assert row3["gate"] == evaluate_rules(
        US_RULES, {k: values[k] for k in ("comparison", "cost_25", "portfolio")}
    )
    assert not row3["gate_holds"]
    assert row4["holds"] and row4["geometric_difference"] == -0.010 and row4["cost_25"] == -0.011
    values = _values(ratios, geometric=-0.0100001, median_ratio=0.0999, positive_share=0.75)
    readings = cr.row_readings(values, _values(BASE), US_RULES)
    assert not readings[2]["holds"] and not readings[3]["holds"]
    assert cr.first_four(readings) is False
    assert cr.first_four(readings[:3]) is None
    good = cr.row_readings(
        _values(
            _with({sid: 0.02 for sid in ROW1[:4]}),
            geometric=0.0,
            median_ratio=0.2,
            positive_share=0.8,
        ),
        _values(BASE),
        US_RULES,
    )
    assert [r["holds"] for r in good] == [True, True, True, True] and cr.first_four(good) is True


def test_the_rules_are_the_gates_own() -> None:
    assert {r.metric for r in US_RULES} >= set(cr.ROW3_METRICS)
    assert {r.metric: r.value for r in US_RULES if r.metric in cr.ROW3_METRICS} == {
        "portfolio.segment_count": 4.0,
        "portfolio.median_ratio": 0.10,
        "portfolio.positive_share": 0.75,
    }
    assert WINDOWS["us_market"].window_id == "us_market_1929_2010"
    assert WINDOWS["qqq"].window_id == "qqq_long_history_development"


def test_cut_keeps_the_reserves_to_the_last_return_date() -> None:
    for market, data in RAW.items():
        assert gp.cut(data).reserves == {}
        kept = DATA[market]
        last = kept.returns.index[-1]
        assert last == pd.Timestamp("2010-12-31")
        assert set(kept.reserves) == set(data.reserves) and cr.RESERVE in kept.reserves
        for name, series in kept.reserves.items():
            assert series.index[-1] <= last
            assert series.equals(data.reserves[name].loc[data.reserves[name].index <= last])


def test_state_checks_find_missing_month_ends_and_a_missing_opening_state() -> None:
    data, window = DATA["qqq"], WINDOWS["qqq"]
    assert cr.state_checks(data, window, LOADED) == []

    def spoiled(start: str, end: str) -> Any:
        inputs = data.inputs.copy()
        inputs.loc[start:end, cr.STATE_COLUMN] = math.nan
        return BenchData(data.returns, inputs, data.source, data.reserves)

    # 2005-06 has no value: its own month end and 2005-12 (six later) have no state.
    assert cr.state_checks(spoiled("2005-06-01", "2005-06-30"), window, LOADED) == [
        "2 个月底没有状态"
    ]
    # 1998-08 has no value, so the 1999-02 month end before the window opens has no state.
    assert cr.state_checks(spoiled("1998-08-01", "1998-08-31"), window, LOADED) == [
        "窗口开头没有可用的状态"
    ]


def test_equivalence_reports_a_t0008_run_that_is_not_the_reserve_path(
    found: dict[str, Any],
) -> None:
    data, window = DATA["qqq"], WINDOWS["qqq"]
    segments = cr.declines(GATE, "qqq", window)
    good = cr.equivalence(data, window, LOADED, segments, found["qqq"])
    assert good == dict.fromkeys(
        ("treasury_4bps", "treasury_25bps", "cash_4bps", "cash_25bps", "t0008_path"), True
    )
    off = json.loads(json.dumps(found["qqq"]))
    off["portfolio"]["median_ratio"] += 2e-12
    assert cr.equivalence(data, window, LOADED, segments, off)["t0008_path"] is False


@pytest.mark.parametrize("broken", ["none", "checks", "equivalence", "record"])
def test_the_probe_reads_h_b_only_after_every_check_passes(
    broken: str,
    found: dict[str, Any],
    records: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(cr, "t0008_run", lambda data, gate, plan, market, loaded, w: found[market])
    problems = ["x"] if broken == "checks" else []
    monkeypatch.setattr(cr, "state_checks", lambda data, window, loaded: list(problems))
    same = {"treasury_4bps": True, "t0008_path": broken != "equivalence"}
    monkeypatch.setattr(cr, "equivalence", lambda *args: dict(same))
    used = None if broken == "record" else records
    for out_dir in (None, tmp_path / "out"):
        result = cr.run_probe(
            DATA,
            LOADED,
            GATE,
            PLAN,
            ROOT_DIR,
            provenance=PROVENANCE,
            out_dir=out_dir if broken != "none" else None,
            records=used,
        )
        assert result["complete"] is (broken == "none")
        assert result["stopped"] is (broken == "record")
        assert "h_b" not in result and "readings" not in result
        assert not (tmp_path / "out").exists()
        assert result["t0008"] == json.loads(json.dumps(found))
        text = cr.render(result)
        if broken == "none":
            assert "开跑前核对都过了。" in text
        else:
            assert "没有读完：停下核对或等同核对没过。" in text


# --- the whole run on synthetic data ---------------------------------------------------------


def test_the_run_checks_before_it_reads(run: dict[str, Any]) -> None:
    result = run["result"]
    assert result["complete"] and not result["stopped"]
    assert result["stop"] == {"us_market": [], "qqq": []}
    assert result["checks"] == {"us_market": [], "qqq": []}
    for market in cr.MARKETS:
        assert result["equivalence"][market] == {
            "treasury_4bps": True,
            "treasury_25bps": True,
            "cash_4bps": True,
            "cash_25bps": True,
            "t0008_path": True,
        }
        # The H-B path with the state fixed at 1 is the bench's own T0008 run.
        assert cr.same_numbers(
            {k: result["h_b"][market]["t0008"][k] for k in ("comparison", "cost_25", "portfolio")},
            result["t0008"][market],
        )
    assert result["production_effect"] == "none" and result["broker_action"] == "none"
    assert result["owner_decision"] == "owner_decision:TRADING-2565:2026-10-22:research_loop_p4"


def test_the_summary_rows_count_held_months_and_switches(run: dict[str, Any]) -> None:
    result = run["result"]
    for market in cr.MARKETS:
        rows = result["h_b"][market]
        t0008, h_b = rows["t0008"], rows["h_b"]
        assert t0008["held_months"] == t0008["months"] and t0008["switches"] == 0
        assert 0 < h_b["held_months"] < h_b["months"] and h_b["switches"] > 0
        window = WINDOWS[market]
        days = DATA[market].returns.index
        held_days = days[(days > pd.Timestamp(window.start)) & (days <= pd.Timestamp(window.end))]
        state = cr.reserve_state(DATA[market].inputs, cr.state_params())
        first, last = (d.strftime("%Y-%m") for d in (held_days[0], held_days[-1]))
        assert (h_b["held_months"], h_b["months"]) == cr.months_held(state, first, last)
        assert h_b["months"] == t0008["months"] == len(pd.period_range(first, last, freq="M"))
        years = len(held_days) / LOADED.settings.trading_days_per_year
        assert h_b["switches_per_year"] == pytest.approx(h_b["switches"] / years)
        assert h_b["strategy"]["average_exposure"] == pytest.approx(0.6, abs=0.01)


def test_the_segment_table_puts_the_deduction_beside_each_reading(run: dict[str, Any]) -> None:
    result = run["result"]
    for market in cr.MARKETS:
        rows = result["segments"][market]
        base = {r["id"]: r for r in result["t0008"][market]["portfolio"]["segments"]}
        assert [r["id"] for r in rows] == list(base)
        for row in rows:
            assert row["deduction"] == pytest.approx(row["difference"] - row["cushion"], abs=1e-15)
            assert row["base_ratio"] == base[row["id"]]["ratio"]
            assert row["months_counted"] == len(pd.period_range(row["start"], row["end"], freq="M"))
            assert 0 <= row["held_months"] <= row["months_counted"]
            if row["counted"]:
                assert row["ratio"] == pytest.approx(row["cushion"] / abs(row["benchmark_return"]))
            else:
                assert row["ratio"] is None


def test_the_segment_table_puts_the_held_deduction_beside_the_rulers(run: dict[str, Any]) -> None:
    result = run["result"]
    dpy = LOADED.settings.trading_days_per_year
    for market in cr.MARKETS:
        found = result["h_b"][market]
        t0008, h_b = found["t0008"], found["h_b"]
        # T0008 always holds the Treasury: its e_T is the ruler's own quiet excess.
        assert t0008["held"] == {
            "excess": pytest.approx(t0008["portfolio"]["quiet_excess"], rel=1e-12),
            "days": t0008["portfolio"]["quiet_days"],
            "fallback": False,
        }
        assert 0 < h_b["held"]["days"] < h_b["portfolio"]["quiet_days"]
        assert not h_b["held"]["fallback"]
        base = {r["id"]: r for r in result["t0008"][market]["portfolio"]["segments"]}
        for row in result["segments"][market]:
            assert 0 <= row["held_days"] <= row["days"]
            assert row["held_deduction"] == pytest.approx(
                0.4 * h_b["held"]["excess"] * row["held_days"] / dpy, rel=1e-12, abs=1e-18
            )
            assert row["held_cushion"] == pytest.approx(
                row["difference"] - row["held_deduction"], abs=1e-15
            )
            other = base[row["id"]]
            if row["counted"]:
                assert row["held_ratio"] == pytest.approx(
                    row["held_cushion"] / abs(row["benchmark_return"])
                )
                cushion = other["difference"] - 0.4 * t0008["held"]["excess"] * row["days"] / dpy
                assert row["base_held_ratio"] == pytest.approx(
                    cushion / abs(other["benchmark_return"])
                )
            else:
                assert row["held_ratio"] is None and row["base_held_ratio"] is None
            if row["held_months"] == 0:
                assert row["held_days"] <= 2


def test_the_readings_are_rows_one_to_four_of_the_us_values(run: dict[str, Any]) -> None:
    result = run["result"]
    us, table = result["h_b"]["us_market"], result["segments"]["us_market"]

    def values(name: str, column: str, months: str) -> dict[str, Any]:
        held = [{"id": r["id"], "ratio": r[column], "held_months": r[months]} for r in table]
        return {**us[name], "held": {**us[name]["held"], "segments": held}}

    # Row 1 reads the table's r′ columns; rows 2 to 4 the summary rows.
    # T0008 holds the Treasury every month of every segment.
    expected = cr.row_readings(
        values("h_b", "held_ratio", "held_months"),
        values("t0008", "base_held_ratio", "months_counted"),
        US_RULES,
    )
    # The synthetic US market holds cash all through decline_197808.
    zero = [
        r["id"]
        for r in table
        if r["counted"] and "1966-01" <= r["start"] <= "1990-12" and r["held_months"] == 0
    ]
    assert result["readings"][0]["cash_zero"] == zero == ["decline_197808"]
    assert result["readings"][0]["ratios"]["decline_197808"] == 0.0
    assert next(r for r in table if r["id"] == "decline_197808")["held_ratio"] != 0.0
    assert json.loads(json.dumps(gp._clean(expected))) == result["readings"]
    assert [r["row"] for r in result["readings"]] == [1, 2, 3, 4]
    assert result["first_four"] == cr.first_four(result["readings"])


def test_the_rf_variant_is_printed_for_the_us_only(run: dict[str, Any]) -> None:
    rf = run["result"]["rf"]
    assert set(rf) == {"summary", "segments", "readings"}
    assert 0 < rf["summary"]["held_months"] < rf["summary"]["months"]
    state = cr.reserve_state(DATA["us_market"].inputs, cr.state_params(cr.RF_COLUMN))
    assert rf["summary"]["held_months"] == cr.months_held(state, "1929-01", "2010-12")[0]
    assert [r["row"] for r in rf["readings"]] == [1, 2, 3, 4]
    assert rf["summary"]["switches"] != run["result"]["h_b"]["us_market"]["h_b"]["switches"]


def test_qqq_is_read_for_direction_only(run: dict[str, Any]) -> None:
    result = run["result"]
    direction = result["direction_only"]
    assert set(direction) == {"t0008", "h_b"}
    qqq_rules = cr._rules(GATE, "qqq", WINDOWS["qqq"])
    assert [g["metric"] for g in direction["h_b"]] == [r.metric for r in qqq_rules]
    assert {r.metric for r in qqq_rules} == {
        "comparison.geometric_annual_return",
        "portfolio.median_ratio",
        "portfolio.rebalance_return",
    }
    assert "qqq" not in json.dumps(result["readings"])


def _svg(path: Path) -> ET.Element:
    return ET.parse(path).getroot()


def test_six_figures_with_the_closeups_each_market_needs(run: dict[str, Any]) -> None:
    result, out = run["result"], run["out"]
    names = []
    for market in cr.MARKETS:
        records = result["figures"][market]
        assert [r["name"] for r in records] == [f"run-{market}-{p}.svg" for p in "ABC"]
        for record in records:
            data = (out / cr.FIGURES / record["name"]).read_bytes()
            assert record["sha256"] == hashlib.sha256(data).hexdigest()
            assert record["bytes"] == len(data)
            names.append(record["name"])
        panels = list(records[1]["counts"]["panels"])
        rows = result["segments"][market]
        if market == "us_market":
            assert panels == [
                r["id"] for r in rows if r["counted"] and "1966-01" <= r["start"] <= "1990-12"
            ]
        else:
            assert panels == [r["id"] for r in rows]
        window = records[0]["counts"]["window"]
        switches = result["h_b"][market]["h_b"]["switches"]
        assert sum(window["compared"]["reserve"].values()) == switches
        assert window["base"] == {"reserve": {"exit": 0, "entry": 0}}
    assert sorted(p.name for p in (out / cr.FIGURES).iterdir()) == sorted(names)


def test_figure_b_prints_each_rules_ratio_and_held_months(run: dict[str, Any]) -> None:
    result, out = run["result"], run["out"]
    root = _svg(out / cr.FIGURES / "run-qqq-B.svg")
    text = [t.text or "" for t in root.iter(f"{{{NS['s']}}}text")]
    for row in result["segments"]["qqq"]:
        ratio = "—" if row["held_ratio"] is None else f"{row['held_ratio']:+.3f}"
        held = f"{row['held_months']}/{row['months_counted']}"
        assert f"{cr.RULES['h_b']}：r′ {ratio}，段内持国债 {held} 月" in text
        base = "—" if row["base_held_ratio"] is None else f"{row['base_held_ratio']:+.3f}"
        assert f"{cr.RULES['t0008']}：r′ {base}，段内持国债 {row['months_counted']}/" in "\n".join(
            text
        )
    assert any(cr.HELD_DEDUCTION in t for t in text)


def test_the_report_carries_the_checks_rows_tables_and_figures(run: dict[str, Any]) -> None:
    text = cr.render(run["result"])
    assert text.startswith("# TRADING-2565-51：H-B 条件化储备（读数）")
    for heading in (
        "## 核对",
        "## 第 1 到 4 行读数（章程第 3.2 节判法表，机械比较）",
        "## 美股大盘 1929–2010",
        "## QQQ 1999–2010",
        "## 只印：同一规则改用 Ken French RF 的 6 个月变化当状态（美股大盘，不判）",
        "## 图",
    ):
        assert heading in text
    assert "T0008 对记录 相同" in text
    columns = (
        "段 | 月份 | 月数 | 持国债（月） | 持国债（交易日） | B 段内 | A 段内 | ΔD | 尺子扣的 | "
        "按持有扣的 | C | r | r′ | T0008 r | T0008 r′"
    )
    assert f"| {columns} |" in text
    assert (
        "- 第 1 行读 r′，按实际持有扣（owner_decision:TRADING-2565:2026-10-22:"
        "hb_row1_held_deduction，Baker 选「按实际持有扣」）；尺子、门和第 2 到 4 行照旧读 r。"
    ) in text
    assert "r′ > 0 的段 3 到 5，且没有一段 r′ < −0.05（持国债 0 月的段记 0）" in text
    assert "；持国债 0 月、记 0 的段：decline_197808 |" in text
    assert (
        "- 第 1 行里段内持国债 0 月的段 r′ 记 0，不算正也不算负（owner_decision:TRADING-2565:"
        "2026-10-22:hb_row1_cash_zero，Baker 选「记 0，不数」）；段表照旧印算出来的 r′。"
    ) in text
    readings = run["result"]["readings"][0]
    assert readings["positive"] != readings["count"]
    low = "—" if readings["min_ratio"] is None else f"{readings['min_ratio']:+.3f}"
    assert (
        f"1966–1990 计入 {readings['count']} 段，r′ > 0 的 {readings['positive']} 段"
        f"（T0008 同样扣：计入 {readings['base_count']} 段、r′ > 0 的 "
        f"{readings['base_positive']} 段），最小 r′ {low}；r′ < −0.05 的段："
    ) in text
    assert (
        "- 第 1 行的 e_T（国债对现金的平时超额，只用段外且 H-B 持国债的交易日）："
        f"{readings['excess'] * 100:+.2f} 点/年，{readings['excess_days']} 个交易日"
    ) in text
    assert text.count("按实际持有扣的 e_T（段外且持国债的交易日）：") == 2
    assert "退回" in cr._excess(0.0123, 0, True) and "+1.23 点/年" in cr._excess(0.0123, 0, True)
    assert "QQQ 第二段（只查方向，只印）" in text
    for market in cr.MARKETS:
        for record in run["result"]["figures"][market]:
            assert f"{record['name']}：sha256 {record['sha256']}" in text
    assert "0.0197" not in text and "e-05" not in text


# --- the command line ------------------------------------------------------------------------


@pytest.fixture
def cli(monkeypatch: pytest.MonkeyPatch) -> CliRunner:
    monkeypatch.setattr(cr, "load_inputs", lambda root: (DATA, GATE, PLAN, LOADED, ROOT_DIR))
    monkeypatch.setattr(cr, "git_provenance", lambda root: PROVENANCE)
    return CliRunner()


def test_check_passes_when_t0008_matches_its_record(
    cli: CliRunner, records: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(cr, "T0008_RECORD", records)
    result = cli.invoke(cr.app, ["check"])
    assert result.exit_code == 0, result.output
    assert "H-B 条件化储备（只核对）" in result.output
    assert "开跑前核对都过了。" in result.output
    assert not list(tmp_path.iterdir())


def test_run_stops_on_the_published_record_and_writes_no_readings(
    cli: CliRunner, tmp_path: Path
) -> None:
    out = tmp_path / "out"
    result = cli.invoke(cr.app, ["run", "--out", str(out)])
    assert result.exit_code == 1, result.output
    assert "没有读完" in result.output
    found = json.loads((out / cr.RUN_JSON).read_text(encoding="utf-8"))
    assert found["stopped"] and not found["complete"]
    assert found["stop"]["us_market"] and found["stop"]["qqq"]
    assert "h_b" not in found and "readings" not in found
    assert not (out / cr.FIGURES).exists()
    report = (out / cr.RUN_MD).read_text(encoding="utf-8")
    assert "**同一次运行的 T0008 和它的记录不同（美股大盘 1929–2010）：**" in report
    assert "T0008 对记录 不同" in report


def test_run_refuses_to_overwrite_before_loading(
    cli: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(root: Path) -> Any:
        raise AssertionError("loaded")

    monkeypatch.setattr(cr, "load_inputs", refuse)
    for name in (cr.RUN_JSON, cr.RUN_MD, cr.FIGURES):
        out = tmp_path / name.replace(".", "_")
        out.mkdir()
        (out / name).write_text("x", encoding="utf-8")
        result = cli.invoke(cr.app, ["run", "--out", str(out)])
        assert result.exit_code == 1
        assert "拒绝覆盖" in result.output
        assert (out / name).read_text(encoding="utf-8") == "x"


def test_a_load_failure_exits_with_its_first_line(
    cli: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(root: Path) -> Any:
        raise ValueError("us_market: no treasury_10y_shiller reserve\nmore")

    monkeypatch.setattr(cr, "load_inputs", broken)
    result = cli.invoke(cr.app, ["check"])
    assert result.exit_code == 1
    assert (
        result.output
        == "conditional_reserve: 数据载入失败：us_market: no treasury_10y_shiller reserve\n"
    )


def test_the_module_reads_no_research_record_or_validation_window() -> None:
    source = Path(cr.__file__).read_text(encoding="utf-8")
    for forbidden in ("window_reads", "runs/", "_screens", "data/raw", "validation_", "_forward"):
        assert forbidden not in source
