"""Acceptance tests for the leverage print-only table (TRADING-2565-101).

Written before the module (the loop thread's tests, pi's module), from requirement v1.1 in the
project files (`knowledge/claude/research-review/leverage-table-requirements-2026-10-30.md`,
sha256 eb42183c…b504, sections 2, 3, 4 and 6) and section 25 of the period-6 charter. Every input
is synthetic: no snapshot, no QQQ return and no research record is read.
"""

from __future__ import annotations

import json
import math
import re
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
from ai_trading_system.research_bench.checks import strategy_targets
from ai_trading_system.research_bench.data import (
    BenchData,
    from_daily_frame,
    from_us_market,
    with_signal_sources,
)
from ai_trading_system.research_bench.metrics import summarize
from ai_trading_system.research_bench.simulate import (
    Rebalance,
    Simulation,
    constant_targets,
    simulate,
)
from ai_trading_system.research_probes import backfill_check as bc
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import leverage_table as lt
from ai_trading_system.research_probes import policy_table as pt

pytestmark = pytest.mark.filterwarnings("ignore:invalid value encountered in log1p")

LOADED = load_settings()
ROOT_DIR = lg.research_root(LOADED)
PREREG = PROJECT_ROOT / "research" / "_trials" / "prereg.yaml"
GATE = tr.load_gate(PREREG)
_, PLAN = tr.load_trial(ROOT_DIR, pt.DATA_TRIAL)
SIGNAL_SNAPSHOT = PLAN.data.signal_snapshot_id
QQQ_SNAPSHOT = PLAN.data.snapshot_id
MONTH_END = Rebalance(schedule="month_end", band=0.0)
# A small grid that holds every leverage the table names (1.0, 1.25, 1.5, 2.0 and the cap 2.0).
TEST_GRID = (1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0)
PROVENANCE = {"commit": "abc1234", "dirty": False}

# QQQ-Q3-r0001 (development window, fast_slow, 4 bps), full precision: the requirement's
# section 3.5, first self-check. Copied here from the record; neither the test nor the module
# opens it.
R0001_F0001 = {
    "geometric_annual_return": 0.042236272246680695,
    "annual_volatility": 0.18631682590323612,
    "max_drawdown": -0.5020165649999986,
    "average_exposure": 0.5941523375781023,
    "annual_turnover": 3.5222403413498977,
}
R0001_BUY_HOLD = {
    "geometric_annual_return": 0.008568284208556598,
    "annual_volatility": 0.33310343608383863,
    "max_drawdown": -0.8298086606243703,
}
# The decision table TRADING-2565-42 (policy-table/rc-report.md, rows "固定敞口 1.0" and
# "F0001 QQQ-Q3 fast_slow" of the US-market table): each printed number's rounding interval.
POLICY_F0001 = {
    "geometric_annual_return": (0.0875, 0.0885),
    "max_drawdown": (-0.4835, -0.4825),
    "average_exposure": (0.655, 0.665),
    "annual_turnover": (3.115, 3.125),
}
POLICY_BUY_HOLD = {
    "geometric_annual_return": (0.0885, 0.0895),
    "max_drawdown": (-0.8415, -0.8405),
}


def _us_data(start: str = "1926-07-01", end: str = "2012-12-31", seed: int = 5) -> BenchData:
    """Daily market returns with volatility that changes month by month, running past 2010."""

    index = pd.bdate_range(start, end, name="date")
    rng = np.random.default_rng(seed)
    codes, uniques = pd.factorize(index.to_period("M"))
    monthly_vol = np.exp(rng.normal(math.log(0.01), 0.5, len(uniques)))
    factors = pd.DataFrame(
        {
            "Mkt-RF": rng.normal(0.0003, 1.0, len(index)) * monthly_vol[codes],
            "SMB": 0.0,
            "HML": 0.0,
            "RF": 0.0001,
        },
        index=index,
    )
    data = from_us_market({"ff_factors_daily": factors}, snapshot_id=SIGNAL_SNAPSHOT)
    return with_signal_sources(data, {}, {}, snapshot_id=SIGNAL_SNAPSHOT)


def _qqq_data(end: str = "2012-12-31", seed: int = 3) -> BenchData:
    """Daily QQQ returns with a long bear in 2000-2002, so the drawdown match lands above 2."""

    index = pd.bdate_range("1994-01-03", end, name="date")
    rng = np.random.default_rng(seed)
    r = rng.normal(0.0004, 0.015, len(index))
    r[(index >= pd.Timestamp("2000-03-27")) & (index <= pd.Timestamp("2002-10-09"))] -= 0.003
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
    data = from_daily_frame(daily, qqq_series="actual", source={"snapshot_id": QQQ_SNAPSHOT})
    return with_signal_sources(data, {}, {}, snapshot_id=SIGNAL_SNAPSHOT)


RAW = {"us_market": _us_data(), "qqq": _qqq_data()}
DATA = {market: gp.cut(data) for market, data in RAW.items()}
ENTRY = bc.source_entry(ROOT_DIR, bc.CONFIGS[0])


def _targets(data: BenchData) -> pd.Series:
    return strategy_targets(tr.STRATEGY_FUNCTIONS[ENTRY.function], data.inputs, ENTRY.params)


def _window(market: Any) -> Any:
    return pt.market_window(LOADED, GATE, market)


def _bench(market: str, targets: pd.Series | None = None) -> Simulation:
    data = DATA[market]
    window = _window(market)
    return simulate(
        data.returns,
        _targets(data) if targets is None else targets,
        start=window.start,
        end=window.end,
        rebalance=MONTH_END,
        cost_bps=LOADED.cost_bps,
    )


def _syn_reproduce() -> dict[str, Any]:
    """REPRODUCE built from the bench on the synthetic data, so the first self-check passes."""

    out: dict[str, Any] = {}
    for market in lt.MARKETS:
        cash = DATA[market].returns["cash"]
        one = summarize(_bench(market), cash, LOADED)
        hold = summarize(
            _bench(market, constant_targets(DATA[market].returns.index, 1.0)), cash, LOADED
        )
        out[market] = {
            line: {
                metric: (found[metric] - 1e-9, found[metric] + 1e-9)
                for metric in lt.REPRODUCE[market][line]
            }
            for line, found in (("f0001", one), ("buy_and_hold", hold))
        }
    return out


SYN_REPRODUCE = _syn_reproduce()
REAL_REPRODUCE = lt.REPRODUCE


@pytest.fixture(scope="module")
def probe() -> dict[str, Any]:
    return lt.run_probe(
        DATA,
        LOADED,
        GATE,
        ROOT_DIR,
        provenance=PROVENANCE,
        grid=TEST_GRID,
        reproduce=SYN_REPRODUCE,
    )


@pytest.fixture(scope="module")
def stopped() -> dict[str, Any]:
    """The real constants on synthetic data: the first self-check fails."""

    return lt.run_probe(DATA, LOADED, GATE, ROOT_DIR, provenance=PROVENANCE, grid=TEST_GRID)


def _row(result: dict[str, Any], market: str, setting: str, row_id: str) -> dict[str, Any]:
    return next(r for r in result["markets"][market]["table"][setting] if r["id"] == row_id)


def _point(result: dict[str, Any], market: str, leverage: float) -> dict[str, Any]:
    return next(p for p in result["markets"][market]["curve"] if p["leverage"] == leverage)


def _match_dict(match: Any) -> dict[str, Any]:
    return {
        "leverage": match.leverage,
        "flag": match.flag,
        "monotone": match.monotone,
        "switches": list(match.switches),
    }


# --- constants ------------------------------------------------------------------------------


def test_the_run_is_registered_under_the_cards_decision() -> None:
    assert lt.RUN_ID == "TRADING-2565-101"
    assert lt.OWNER_DECISION == "owner_decision:TRADING-2565:2026-10-30:leverage_table"
    assert lt.DATA_TRIAL == pt.DATA_TRIAL
    assert lt.MARKETS == ("qqq", "us_market")


def test_the_grid_runs_from_one_to_three_in_hundredths() -> None:
    assert lt.GRID == tuple(round(1.0 + i / 100, 2) for i in range(201))
    assert lt.GRID[0] == 1.0 and lt.GRID[-1] == 3.0
    assert lt.TABLE_LEVERAGE == (1.0, 1.25, 1.5, 2.0)
    assert lt.DAILY_RESET_LEVERAGE == 2.0
    assert lt.CAP_SENSITIVITY == 2.0


def test_the_costs_are_the_benchs() -> None:
    """Section 3.2: main 1% spread and the bench's unified 4 bps; robust 2% and 25 bps."""

    assert LOADED.cost_bps == 4.0
    assert lt.MAIN == lt.Costs(spread=0.01, bps=4.0)
    assert lt.ROBUST == lt.Costs(spread=0.02, bps=25.0)
    assert lt.SENSITIVITIES == {
        "spread_only": lt.Costs(spread=0.02, bps=4.0),
        "cost_only": lt.Costs(spread=0.01, bps=25.0),
        "no_spread": lt.Costs(spread=0.0, bps=4.0),
    }


def test_the_lines_and_readings_are_fixed() -> None:
    assert lt.LINE_1 == 0.010
    assert lt.LINE_2 == 0.0
    assert lt.NEAR_HIGH == 0.05
    assert lt.ROLLING_YEARS == (5, 10)
    assert lt.BOUND_TOLERANCE == 0.10
    # Calendar-year bounds: on the Ken French calendar the eras are 1929-01-02..1961-12-29,
    # 1962-01-02..1989-12-29 and 1990-01-02..2010-12-31, as the requirement writes them.
    assert lt.ERAS == (
        ("1929_1961", date(1929, 1, 1), date(1961, 12, 31)),
        ("1962_1989", date(1962, 1, 1), date(1989, 12, 31)),
        ("1990_2010", date(1990, 1, 1), date(2010, 12, 31)),
    )


def test_the_texts_are_the_requirements() -> None:
    assert lt.HOLDS_TEXT == (
        "F0001 加杠杆到买入持有的回撤或波动，全周期两把尺子都多挣 1 点以上，"
        "多付 1 点利差、成本 25 bps 也不输"
    )
    assert lt.NOT_HOLDS_TEXT == "同回撤（同波动）下加杠杆的 F0001 没有稳稳超过买入持有"
    assert lt.STOP_REPRODUCE == "L = 1 复现不了印过的数，不判"
    assert lt.STOP_BOUND == "L* 超过上界，多半是实现的错，不判"
    assert lt.ERA_NOTE == (
        "L* 是 1929–2010 全周期定的，1929–32 在里面，后两段用的是前一段定的杠杆。"
    )
    assert lt.OLD_ERA_TEXT == "全周期的多挣主要来自 1929–1961"
    assert lt.LIMITATIONS == (
        "样本里定的 L* 和 L_v",
        "两把尺子的窗口都含趋势最占便宜的长熊（1929–32、2000–02、2008）",
        "2011–2021 长牛没读",
        "没有趋势信息时也判 `holds` 约 5%–7%（不加 L_v 约 10%–15%；合成模拟，推断）",
    )


def test_the_qqq_constants_are_r0001_at_full_precision() -> None:
    qqq = lt.REPRODUCE["qqq"]
    assert set(qqq) == {"f0001", "buy_and_hold"}
    for line, printed in (("f0001", R0001_F0001), ("buy_and_hold", R0001_BUY_HOLD)):
        assert set(qqq[line]) == set(printed)
        for metric, value in printed.items():
            low, high = qqq[line][metric]
            assert low == pytest.approx(value - 1e-6, abs=1e-15)
            assert high == pytest.approx(value + 1e-6, abs=1e-15)


def test_the_us_constants_are_the_decision_tables_rounding_intervals() -> None:
    us = lt.REPRODUCE["us_market"]
    assert us["f0001"] == {k: pytest.approx(v) for k, v in POLICY_F0001.items()}
    assert us["buy_and_hold"] == {k: pytest.approx(v) for k, v in POLICY_BUY_HOLD.items()}


def test_f0001_is_qqq_q3_fast_slow_unchanged() -> None:
    entry = lt.f0001_entry(ROOT_DIR)
    assert entry == ENTRY
    assert entry.rebalance.to_rebalance() == MONTH_END
    assert entry.delay_days == 0
    targets = lt.f0001_targets(DATA["qqq"], ROOT_DIR)
    pd.testing.assert_series_equal(targets, _targets(DATA["qqq"]))


# --- the levered simulation -------------------------------------------------------------------


def _small(seed: int = 11, days: int = 400, cash: float = 0.0001) -> tuple[pd.DataFrame, Any]:
    """Bench-shaped returns over a short weekday calendar, and its window."""

    index = pd.bdate_range("2001-01-01", periods=days, name="date")
    rng = np.random.default_rng(seed)
    market = rng.normal(0.0003, 0.012, days)
    returns = pd.DataFrame(
        {"qqq": market, "cash": cash, "lev2": 2 * market - cash, "lev3": 3 * market - 2 * cash},
        index=index,
    )
    return returns, (index[25].date(), index[-1].date())


def _monthly_targets(index: pd.Index, seed: int = 2) -> pd.Series:
    """0, 0.5 or 1, decided per month (constant inside a month, like F0001 read at month end)."""

    rng = np.random.default_rng(seed)
    months = pd.DatetimeIndex(index).to_period("M")
    values = {m: float(rng.choice([0.0, 0.5, 1.0])) for m in months.unique()}
    return pd.Series([values[m] for m in months], index=index, dtype=float)


def _levered(
    returns: pd.DataFrame,
    targets: pd.Series,
    window: tuple[date, date],
    leverage: float,
    costs: Any,
    *,
    daily_reset: bool = False,
) -> Any:
    return lt.simulate_levered(
        returns,
        targets,
        start=window[0],
        end=window[1],
        leverage=leverage,
        costs=costs,
        daily_reset=daily_reset,
    )


def test_at_leverage_one_the_days_are_the_benchs() -> None:
    """Requirement section 7: at L = 1 every day equals the bench's simulate (4 bps)."""

    returns, window = _small()
    targets = _monthly_targets(returns.index)
    ours = _levered(returns, targets, window, 1.0, lt.MAIN)
    assert ours.bust is None
    theirs = simulate(
        returns, targets, start=window[0], end=window[1], rebalance=MONTH_END, cost_bps=4.0
    )
    assert ours.simulation.start == theirs.start
    for column in ("return", "gross_return", "exposure", "turnover", "cost"):
        np.testing.assert_allclose(
            ours.simulation.daily[column].to_numpy(),
            theirs.daily[column].to_numpy(),
            rtol=0,
            atol=1e-12,
        )
    assert ours.simulation.daily.index.equals(theirs.daily.index)
    assert (ours.simulation.daily["spread"] == 0.0).all()


def test_at_leverage_one_every_summary_field_is_the_benchs() -> None:
    """Review section 8: the whole-period numbers come from summarize, calendar years."""

    returns, window = _small()
    targets = _monthly_targets(returns.index)
    cash = returns["cash"]
    ours = summarize(_levered(returns, targets, window, 1.0, lt.MAIN).simulation, cash, LOADED)
    theirs = summarize(
        simulate(
            returns, targets, start=window[0], end=window[1], rebalance=MONTH_END, cost_bps=4.0
        ),
        cash,
        LOADED,
    )
    assert set(ours) == set(theirs)
    for key, value in theirs.items():
        if isinstance(value, float):
            assert ours[key] == pytest.approx(value, rel=1e-10, abs=1e-12), key
        else:
            assert ours[key] == value, key


def test_between_trades_the_levered_line_is_linear_in_leverage() -> None:
    """Requirement section 7: with no cash rate, spread or cost, a holding period's return at L
    is exactly L times the return at L = 1 (this is what the section 3.5 bound rests on)."""

    returns, window = _small(cash=0.0)
    targets = pd.Series(1.0, index=returns.index)
    free = lt.Costs(spread=0.0, bps=0.0)
    one = _levered(returns, targets, window, 1.0, free).simulation.returns
    two = _levered(returns, targets, window, 2.3, free).simulation.returns
    index = one.index
    months = index.to_period("M")
    # The trade decided at a month's last date is made at the next date's close.
    traded = [i + 1 for i in range(len(index) - 1) if months[i] != months[i + 1]]
    edges = [0, *[t + 1 for t in traded if t + 1 < len(index)], len(index)]
    for a, b in zip(edges[:-1], edges[1:], strict=True):
        r1 = float(np.prod(1.0 + one.iloc[a:b].to_numpy()) - 1.0)
        r2 = float(np.prod(1.0 + two.iloc[a:b].to_numpy()) - 1.0)
        assert r2 == pytest.approx(2.3 * r1, rel=1e-10, abs=1e-12)


def test_the_spread_is_paid_only_on_borrowed_money() -> None:
    returns, window = _small()
    targets = _monthly_targets(returns.index)
    costs = lt.Costs(spread=0.02, bps=0.0)
    daily = _levered(returns, targets, window, 1.5, costs).simulation.daily
    w = daily["exposure"]
    market = returns["qqq"].reindex(daily.index)
    cash = returns["cash"].reindex(daily.index)
    expected_spread = np.maximum(w - 1.0, 0.0) * 0.02 / 252
    np.testing.assert_allclose(daily["spread"], expected_spread, rtol=0, atol=1e-15)
    np.testing.assert_allclose(
        daily["gross_return"], w * market + (1.0 - w) * cash - expected_spread, atol=1e-15
    )
    assert (daily.loc[w <= 1.0, "spread"] == 0.0).all()
    assert (daily.loc[w > 1.0, "spread"] > 0.0).any()


def test_the_drifted_leverage_is_brought_back_each_month_and_costed() -> None:
    """Review section 6: with e fixed at 1 and L = 2, each month's trade is |L e - drifted w|."""

    returns, window = _small()
    targets = pd.Series(1.0, index=returns.index)
    daily = _levered(returns, targets, window, 2.0, lt.MAIN).simulation.daily
    w = daily["exposure"].to_numpy()
    gross = daily["gross_return"].to_numpy()
    market = returns["qqq"].reindex(daily.index).to_numpy()
    drifted = w * (1.0 + market) / (1.0 + gross)
    traded = daily["turnover"].to_numpy() > 0.0
    assert traded.sum() >= 15
    np.testing.assert_allclose(
        daily["turnover"].to_numpy()[traded], np.abs(2.0 - drifted[traded]), atol=1e-12
    )
    np.testing.assert_allclose(
        daily["cost"].to_numpy(), daily["turnover"].to_numpy() * 4.0 / 10_000.0, atol=1e-15
    )
    after = np.flatnonzero(traded) + 1
    after = after[after < len(w)]
    np.testing.assert_allclose(w[after], 2.0, atol=1e-12)
    np.testing.assert_allclose(
        daily["return"].to_numpy(),
        (1.0 + gross) * (1.0 - daily["cost"].to_numpy()) - 1.0,
        atol=1e-15,
    )


def test_a_daily_reset_line_opens_every_day_at_l_times_e() -> None:
    returns, window = _small()
    targets = _monthly_targets(returns.index)
    month = _levered(returns, targets, window, 2.0, lt.MAIN).simulation.daily
    daily = _levered(returns, targets, window, 2.0, lt.MAIN, daily_reset=True).simulation.daily
    # e in force each day: the month-end line opens every day at its own level times L only on the
    # days right after a trade; the daily-reset line does it every day.
    executed = (month["exposure"] / 2.0).where(month["turnover"].shift(1, fill_value=1.0) > 0.0)
    executed = executed.ffill()
    np.testing.assert_allclose(daily["exposure"], 2.0 * executed, atol=1e-12)
    # A day held at w = 0 or w = 1 does not drift; every other day is brought back at its close.
    w = daily["exposure"]
    drifting = (w > 0.0) & ((w - 1.0).abs() > 1e-9)
    assert drifting.sum() > 50
    assert (daily.loc[drifting, "turnover"] > 0.0).all()


def test_a_constant_target_above_one_is_a_matched_fixed_position() -> None:
    returns, window = _small()
    targets = pd.Series(1.65, index=returns.index)
    daily = _levered(returns, targets, window, 1.0, lt.MAIN).simulation.daily
    assert daily["exposure"].iloc[0] == pytest.approx(1.65)
    assert (daily["spread"] > 0.0).any()


def test_a_bust_day_ends_the_line_at_zero() -> None:
    returns, window = _small()
    crash = returns.index[200]
    returns.loc[crash, "qqq"] = -0.40
    targets = pd.Series(1.0, index=returns.index)
    run = _levered(returns, targets, window, 3.0, lt.MAIN)
    assert run.bust == crash
    r = run.simulation.returns
    assert r.loc[crash] == -1.0
    assert (r.loc[r.index > crash] == 0.0).all()
    stats = lt.window_stats(r, run.simulation.start)
    assert stats["geometric_annual_return"] == -1.0
    assert stats["max_drawdown"] == -1.0


# --- window numbers -----------------------------------------------------------------------------


def test_window_stats_are_summarizes_calendar_year_numbers() -> None:
    sim = _bench("qqq")
    full = summarize(sim, DATA["qqq"].returns["cash"], LOADED)
    stats = lt.window_stats(sim.returns, sim.start)
    assert stats["years"] == pytest.approx(full["years"], rel=1e-12)
    assert stats["return_days"] == full["return_days"]
    assert stats["first_return_date"] == full["first_return_date"]
    assert stats["last_return_date"] == full["last_return_date"]
    assert stats["geometric_annual_return"] == pytest.approx(
        full["geometric_annual_return"], rel=1e-12
    )
    assert stats["max_drawdown"] == pytest.approx(full["max_drawdown"], rel=1e-12)


def test_window_stats_take_the_anchor_as_the_starting_point() -> None:
    index = pd.bdate_range("2003-01-01", periods=10)
    r = pd.Series([-0.1] + [0.0] * 9, index=index)
    stats = lt.window_stats(r, pd.Timestamp("2002-12-31"))
    assert stats["max_drawdown"] == pytest.approx(-0.1)
    years = (index[-1] - pd.Timestamp("2002-12-31")).days / 365.25
    assert stats["geometric_annual_return"] == pytest.approx(0.9 ** (1 / years) - 1)


# --- matching -----------------------------------------------------------------------------------

GRID5 = (1.0, 1.5, 2.0, 2.5, 3.0)


@pytest.mark.parametrize(
    ("allowed", "leverage", "flag", "monotone", "switches"),
    [
        ([True, True, True, False, False], 2.0, "ok", True, (2.5,)),
        ([True] * 5, 3.0, "top", True, ()),
        ([False] * 5, 1.0, "bottom", True, ()),
        ([True, False, True, False, False], 2.0, "ok", False, (1.5, 2.0, 2.5)),
        ([False, True, False, False, False], 1.5, "ok", False, (1.5, 2.0)),
    ],
)
def test_the_largest_allowed_leverage_on_the_grid(
    allowed: list[bool], leverage: float, flag: str, monotone: bool, switches: tuple[float, ...]
) -> None:
    """Section 3.1: the largest grid point allowed; monotone when the allowed points are the
    first ones; switches are the grid points where allowed changes."""

    found = lt.largest_allowed(GRID5, allowed)
    assert found == lt.Match(leverage=leverage, flag=flag, monotone=monotone, switches=switches)


def test_the_range_where_a_line_holds() -> None:
    assert lt.range_where(GRID5, [True, True, False, False, False]) == {
        "low": 1.0,
        "high": 1.5,
        "contiguous": True,
    }
    assert lt.range_where(GRID5, [False, True, False, True, True]) == {
        "low": 1.5,
        "high": 3.0,
        "contiguous": False,
    }
    assert lt.range_where(GRID5, [False] * 5) is None


def test_the_drawdown_bound_is_the_printed_one() -> None:
    """Requirement section 3.5: 2.54 on QQQ (r0001), 2.79 on the US market (decision table)."""

    qqq = lt.drawdown_bound(R0001_BUY_HOLD["max_drawdown"], R0001_F0001["max_drawdown"])
    assert qqq == pytest.approx(math.log(1 - 0.8298086606243703) / math.log(1 - 0.5020165649999986))
    assert round(qqq, 2) == 2.54
    assert lt.drawdown_bound(-0.841, -0.483) == pytest.approx(2.787, abs=0.001)


@pytest.mark.parametrize(
    ("leverage", "bound", "problem"),
    [(2.25, 2.54, False), (2.63, 2.54, False), (2.65, 2.54, True), (3.0, 2.79, True)],
)
def test_the_bound_check_allows_a_tenth(leverage: float, bound: float, problem: bool) -> None:
    assert lt.bound_problem(leverage, bound) is problem


# --- the first self-check -----------------------------------------------------------------------


def test_reproduce_rows_compare_every_pinned_number() -> None:
    one = {k: v for k, v in R0001_F0001.items()}
    hold = {k: v for k, v in R0001_BUY_HOLD.items()}
    rows = lt.reproduce_rows("qqq", one, hold)
    assert [(r["line"], r["metric"]) for r in rows] == [
        *(("f0001", m) for m in R0001_F0001),
        *(("buy_and_hold", m) for m in R0001_BUY_HOLD),
    ]
    assert all(r["ok"] and r["market"] == "qqq" for r in rows)
    off = lt.reproduce_rows("qqq", {**one, "geometric_annual_return": 0.042238}, hold)
    bad = [r for r in off if not r["ok"]]
    assert [(r["line"], r["metric"]) for r in bad] == [("f0001", "geometric_annual_return")]
    assert bad[0]["value"] == 0.042238
    assert bad[0]["low"] == pytest.approx(0.042236272246680695 - 1e-6)


def test_reproduce_rows_take_the_us_rounding_intervals() -> None:
    one = {
        "geometric_annual_return": 0.0884,
        "max_drawdown": -0.4831,
        "average_exposure": 0.6612,
        "annual_turnover": 3.1204,
        "annual_volatility": 0.1,
    }
    hold = {"geometric_annual_return": 0.0886, "max_drawdown": -0.8409, "annual_volatility": 0.2}
    rows = lt.reproduce_rows("us_market", one, hold)
    assert len(rows) == 6 and all(r["ok"] for r in rows)
    rows = lt.reproduce_rows("us_market", {**one, "geometric_annual_return": 0.0886}, hold)
    assert [r["metric"] for r in rows if not r["ok"]] == ["geometric_annual_return"]


# --- the verdict --------------------------------------------------------------------------------


def _ds(qd: float, qv: float, ud: float, uv: float) -> dict[str, dict[str, float]]:
    return {
        "qqq": {"drawdown": qd, "volatility": qv},
        "us_market": {"drawdown": ud, "volatility": uv},
    }


def test_holds_needs_all_four_main_differences_and_all_four_robust_ones() -> None:
    result = lt.verdict(_ds(0.010, 0.02, 0.05, 0.03), _ds(0.0, 0.001, 0.02, 0.01))
    assert result["verdict"] == "holds"
    assert result["line_1"] and result["line_2"]
    assert result["failed"] == []
    assert result["text"] == lt.HOLDS_TEXT


def test_not_holds_names_every_failing_ruler_pairing_and_line() -> None:
    result = lt.verdict(_ds(0.0099999, 0.02, 0.05, 0.03), _ds(-0.0271, -1e-12, 0.02, 0.01))
    assert result["verdict"] == "not_holds"
    assert not result["line_1"] and not result["line_2"]
    assert result["failed"] == [
        {"line": "line_1", "market": "qqq", "pairing": "drawdown", "value": 0.0099999},
        {"line": "line_2", "market": "qqq", "pairing": "drawdown", "value": -0.0271},
        {"line": "line_2", "market": "qqq", "pairing": "volatility", "value": -1e-12},
    ]
    assert result["text"] == lt.NOT_HOLDS_TEXT + "：" + "；".join(
        lt.failure_text(item) for item in result["failed"]
    )


def test_the_failure_text_names_ruler_pairing_line_and_points() -> None:
    item = {"line": "line_2", "market": "qqq", "pairing": "drawdown", "value": -0.0271}
    assert lt.failure_text(item) == "QQQ 1999–2010 按回撤配平，线 2（稳健 ≥ 0）-2.71 点"
    item = {"line": "line_1", "market": "us_market", "pairing": "volatility", "value": 0.0042}
    assert lt.failure_text(item) == "美股大盘 1929–2010 按波动配平，线 1（主算 ≥ +1 点）+0.42 点"


@pytest.mark.parametrize(
    ("difference", "text"),
    [
        (0.0101, "用按日重置的产品会多 1.01 点/年"),
        (-0.0123, "用按日重置的产品会少 1.23 点/年"),
        (0.01, None),
        (-0.0099, None),
    ],
)
def test_the_daily_reset_reading(difference: float, text: str | None) -> None:
    assert lt.daily_reset_text(difference) == text


def _eras(d2: float, d3: float) -> list[dict[str, Any]]:
    return [
        {"era": era, "difference": {"drawdown": d, "volatility": 0.0}}
        for era, d in (("1929_1961", 0.08), ("1962_1989", d2), ("1990_2010", d3))
    ]


def test_the_old_era_reading() -> None:
    assert lt.old_era_text(0.02, _eras(0.0099, -0.01)) == lt.OLD_ERA_TEXT
    assert lt.old_era_text(0.02, _eras(0.01, -0.01)) is None
    assert lt.old_era_text(0.0099, _eras(0.0, 0.0)) is None


# --- rolling windows and eras ---------------------------------------------------------------------


def test_rolling_windows_start_at_month_ends_and_need_full_length() -> None:
    anchor = pd.Timestamp("1999-12-31")
    index = pd.bdate_range("2000-01-03", "2012-12-31")
    a = pd.Series(0.0004, index=index)
    b = pd.Series(0.0, index=index)
    crash = pd.Timestamp("2003-06-02")
    b.iloc[int(index.searchsorted(crash))] = -0.2
    out = lt.rolling_windows(a, b, anchor, 5)
    calendar = pd.DatetimeIndex([anchor]).append(index)
    month_end = [
        calendar[i]
        for i in range(len(calendar))
        if i == len(calendar) - 1 or calendar[i].month != calendar[i + 1].month
    ]
    starts = [d for d in month_end if d + pd.DateOffset(years=5) <= index[-1]]
    assert out["years"] == 5
    assert out["all"]["count"] == len(starts)
    expected = []
    near = []
    for d in starts:
        end = index[index <= d + pd.DateOffset(years=5)][-1]
        inside = (index > d) & (index <= end)
        ga = lt.window_stats(a[inside], d)["geometric_annual_return"]
        gb = lt.window_stats(b[inside], d)["geometric_annual_return"]
        expected.append(ga - gb)
        if d < crash:
            near.append(ga - gb)
    assert out["all"]["worst"] == pytest.approx(min(expected))
    assert out["all"]["median"] == pytest.approx(float(np.median(expected)))
    assert out["all"]["p10"] == pytest.approx(float(np.percentile(expected, 10)))
    assert out["all"]["beat_share"] == pytest.approx(np.mean(np.array(expected) > 0))
    assert out["near_high"]["count"] == len(near)
    assert out["near_high"]["median"] == pytest.approx(float(np.median(near)))


def test_rolling_windows_without_a_full_window_are_empty() -> None:
    index = pd.bdate_range("2000-01-03", "2003-12-31")
    a = pd.Series(0.0, index=index)
    out = lt.rolling_windows(a, a, pd.Timestamp("1999-12-31"), 5)
    assert out["all"] == {
        "count": 0,
        "worst": None,
        "p10": None,
        "median": None,
        "beat_share": None,
    }
    assert out["near_high"]["count"] == 0


def test_era_rows_cut_the_continuous_run_at_the_era_dates() -> None:
    index = pd.bdate_range("1929-01-03", "2010-12-31")
    rng = np.random.default_rng(9)
    lines = {
        "buy_and_hold": pd.Series(rng.normal(0.0003, 0.01, len(index)), index=index),
        "drawdown": pd.Series(rng.normal(0.0004, 0.015, len(index)), index=index),
        "volatility": pd.Series(rng.normal(0.0003, 0.012, len(index)), index=index),
    }
    anchor = pd.Timestamp("1929-01-02")
    rows = lt.era_rows(lines, anchor)
    assert [r["era"] for r in rows] == ["1929_1961", "1962_1989", "1990_2010"]
    assert rows[0]["anchor"] == "1929-01-02"
    assert rows[1]["anchor"] == "1961-12-29"
    assert rows[2]["anchor"] == "1989-12-29"
    for row, (_, start, end) in zip(rows, lt.ERAS, strict=True):
        inside = (index >= pd.Timestamp(start)) & (index <= pd.Timestamp(end))
        assert row["return_days"] == int(inside.sum())
        era_anchor = pd.Timestamp(row["anchor"])
        for name, series in lines.items():
            assert row[name] == lt.window_stats(series[inside], era_anchor)
        for pairing in ("drawdown", "volatility"):
            assert row["difference"][pairing] == pytest.approx(
                row[pairing]["geometric_annual_return"]
                - row["buy_and_hold"]["geometric_annual_return"]
            )


# --- check ----------------------------------------------------------------------------------------


def test_check_counts_inputs_only() -> None:
    check = lt.check_inputs(DATA, LOADED, GATE, ROOT_DIR)
    assert check["run_id"] == lt.RUN_ID
    assert check["ok"]
    assert list(check["markets"]) == list(lt.MARKETS)
    us, qqq = check["markets"]["us_market"], check["markets"]["qqq"]
    assert (us["window_id"], us["start"], us["end"]) == (
        "us_market_1929_2010",
        "1929-01-02",
        "2010-12-31",
    )
    assert (qqq["window_id"], qqq["start"], qqq["end"]) == (
        "qqq_long_history_development",
        "1999-03-10",
        "2010-12-31",
    )
    for market, found in check["markets"].items():
        sim = _bench(market)
        assert found["return_days"] == len(sim.returns)
        assert found["first_return_date"] == sim.returns.index[0].date().isoformat()
        assert found["missing_days"] == 0 and found["target_defined_at_start"]
        assert found["first_target_date"] <= found["start"]
        assert sum(found["months_by_exposure"].values()) > 0
        assert set(found["months_by_exposure"]) == {"0", "0.5", "1", "other"}
        assert found["months_by_exposure"]["other"] == 0
        assert found["rebalances"] == pytest.approx(found["return_days"] / 21, rel=0.1)
        assert set(found["rolling"]) == {"5", "10"}
    assert [e["era"] for e in us["eras"]] == ["1929_1961", "1962_1989", "1990_2010"]
    assert sum(e["return_days"] for e in us["eras"]) == us["return_days"]
    assert qqq["eras"] == []
    text = json.dumps(check)
    for word in ("geometric", "drawdown", "volatility", "difference"):
        assert word not in text


@pytest.mark.parametrize("column", ["qqq", "cash"])
def test_check_fails_a_missing_day_inside_a_window(column: str) -> None:
    returns = DATA["qqq"].returns.copy()
    returns.loc[pd.Timestamp("2005-06-01"), column] = np.nan
    holed = {
        **DATA,
        "qqq": BenchData(
            returns=returns, inputs=DATA["qqq"].inputs, source=dict(DATA["qqq"].source)
        ),
    }
    check = lt.check_inputs(holed, LOADED, GATE, ROOT_DIR)
    assert not check["ok"]
    assert check["markets"]["qqq"]["missing_days"] == 1
    assert check["markets"]["us_market"]["missing_days"] == 0


def test_check_fails_a_signal_without_enough_history() -> None:
    short = {**DATA, "us_market": gp.cut(_us_data(start="1928-09-03"))}
    check = lt.check_inputs(short, LOADED, GATE, ROOT_DIR)
    assert not check["ok"]
    assert not check["markets"]["us_market"]["target_defined_at_start"]
    assert check["markets"]["us_market"]["first_target_date"] > "1929-01-02"


# --- the run: self-checks -------------------------------------------------------------------------


def test_the_real_constants_stop_the_run_on_synthetic_data(stopped: dict[str, Any]) -> None:
    assert stopped["stopped"]["reason"] == "reproduce"
    assert stopped["stopped"]["text"] == lt.STOP_REPRODUCE
    assert stopped["markets"] == {}
    assert stopped["differences"] is None and stopped["verdict"] is None
    assert not stopped["complete"]
    assert stopped["reproduce"] and not all(r["ok"] for r in stopped["reproduce"])
    text = json.dumps({k: v for k, v in stopped.items() if k != "check"})
    for word in ("curve", "drawdown_match", "volatility_match", "rolling", "eras"):
        assert word not in text


def test_a_bound_failure_stops_with_only_l_star_and_the_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(lt, "BOUND_TOLERANCE", -10.0)
    result = lt.run_probe(
        DATA, LOADED, GATE, ROOT_DIR, provenance=PROVENANCE, grid=TEST_GRID, reproduce=SYN_REPRODUCE
    )
    assert result["stopped"]["reason"] == "bound"
    assert result["stopped"]["text"] == lt.STOP_BOUND
    assert set(result["stopped"]["bound"]) == set(lt.MARKETS)
    for found in result["stopped"]["bound"].values():
        assert set(found) == {"leverage", "bound", "problem"}
        assert found["problem"] is True
    assert result["markets"] == {} and result["verdict"] is None
    assert all(r["ok"] for r in result["reproduce"])


def test_a_grid_without_the_tables_leverages_is_refused() -> None:
    with pytest.raises(ValueError, match="grid"):
        lt.run_probe(
            DATA,
            LOADED,
            GATE,
            ROOT_DIR,
            provenance=PROVENANCE,
            grid=(1.0, 1.5, 2.0, 3.0),
            reproduce=SYN_REPRODUCE,
        )


# --- the run: numbers -----------------------------------------------------------------------------


def test_the_run_passes_both_self_checks(probe: dict[str, Any]) -> None:
    assert probe["stopped"] is None
    assert probe["complete"]
    assert all(r["ok"] for r in probe["reproduce"])
    assert probe["run_id"] == lt.RUN_ID and probe["owner_decision"] == lt.OWNER_DECISION
    assert probe["provenance"] == PROVENANCE
    assert probe["grid"] == list(TEST_GRID)
    assert probe["check"] == lt.check_inputs(DATA, LOADED, GATE, ROOT_DIR)
    for market in lt.MARKETS:
        found = probe["markets"][market]
        assert found["bound"] == pytest.approx(
            lt.drawdown_bound(found["buy_and_hold"]["max_drawdown"], found["one"]["max_drawdown"])
        )
        assert not lt.bound_problem(found["drawdown_match"]["leverage"], found["bound"])


@pytest.mark.parametrize("market", ["qqq", "us_market"])
def test_the_curve_is_the_levered_simulation(probe: dict[str, Any], market: str) -> None:
    found = probe["markets"][market]
    assert [p["leverage"] for p in found["curve"]] == list(TEST_GRID)
    data = DATA[market]
    window = _window(market)
    cash = data.returns["cash"]
    hold = found["buy_and_hold"]
    for setting, costs in (("main", lt.MAIN), ("robust", lt.ROBUST)):
        run = lt.simulate_levered(
            data.returns,
            _targets(data),
            start=window.start,
            end=window.end,
            leverage=1.75,
            costs=costs,
        )
        full = summarize(run.simulation, cash, LOADED)
        point = _point(probe, market, 1.75)[setting]
        assert point["geometric_annual_return"] == pytest.approx(full["geometric_annual_return"])
        assert point["max_drawdown"] == pytest.approx(full["max_drawdown"])
        assert point["annual_volatility"] == pytest.approx(full["annual_volatility"])
    for point in found["curve"]:
        assert point["main"]["difference"] == pytest.approx(
            point["main"]["geometric_annual_return"] - hold["geometric_annual_return"]
        )
    one = _point(probe, market, 1.0)["main"]
    assert one["geometric_annual_return"] == pytest.approx(found["one"]["geometric_annual_return"])


@pytest.mark.parametrize("market", ["qqq", "us_market"])
def test_l_star_and_l_v_are_the_largest_allowed_grid_points(
    probe: dict[str, Any], market: str
) -> None:
    found = probe["markets"][market]
    hold = found["buy_and_hold"]
    curve = found["curve"]
    by_drawdown = lt.largest_allowed(
        TEST_GRID, [p["main"]["max_drawdown"] >= hold["max_drawdown"] for p in curve]
    )
    by_volatility = lt.largest_allowed(
        TEST_GRID, [p["main"]["annual_volatility"] <= hold["annual_volatility"] for p in curve]
    )
    assert found["drawdown_match"] == _match_dict(by_drawdown)
    assert found["volatility_match"] == _match_dict(by_volatility)


@pytest.mark.parametrize("market", ["qqq", "us_market"])
def test_the_verdict_numbers_are_read_at_the_matched_leverages(
    probe: dict[str, Any], market: str
) -> None:
    found = probe["markets"][market]
    for pairing in ("drawdown", "volatility"):
        leverage = found[f"{pairing}_match"]["leverage"]
        point = _point(probe, market, leverage)
        assert probe["differences"]["main"][market][pairing] == point["main"]["difference"]
        assert probe["differences"]["robust"][market][pairing] == point["robust"]["difference"]
    assert probe["verdict"] == lt.verdict(
        probe["differences"]["main"], probe["differences"]["robust"]
    )


@pytest.mark.parametrize("market", ["qqq", "us_market"])
def test_the_ranges_read_the_curve(probe: dict[str, Any], market: str) -> None:
    curve = probe["markets"][market]["curve"]
    ranges = probe["markets"][market]["ranges"]
    assert ranges["main"] == lt.range_where(
        TEST_GRID, [p["main"]["difference"] >= lt.LINE_1 for p in curve]
    )
    assert ranges["robust"] == lt.range_where(
        TEST_GRID, [p["robust"]["difference"] >= lt.LINE_2 for p in curve]
    )


def test_the_full_period_table_has_every_line(probe: dict[str, Any]) -> None:
    ids = [
        "buy_and_hold",
        "f0001_1.0",
        "f0001_1.25",
        "f0001_1.5",
        "f0001_2.0",
        "f0001_drawdown",
        "f0001_volatility",
        "matched_1.0",
        "matched_1.25",
        "matched_1.5",
        "matched_2.0",
        "matched_drawdown",
        "matched_volatility",
        "daily_2.0",
        "daily_drawdown",
    ]
    for market in lt.MARKETS:
        found = probe["markets"][market]
        for setting in ("main", "robust"):
            rows = found["table"][setting]
            assert [r["id"] for r in rows] == ids
            star = found["drawdown_match"]["leverage"]
            assert _row(probe, market, setting, "f0001_drawdown")["leverage"] == star
            assert _row(probe, market, setting, "daily_drawdown")["leverage"] == star
            vee = found["volatility_match"]["leverage"]
            assert _row(probe, market, setting, "f0001_volatility")["leverage"] == vee
            hold = _row(probe, market, setting, "buy_and_hold")
            assert hold["difference"] == 0.0 and hold["matched_difference"] is None
            for row in rows:
                summary = row["summary"]
                assert row["difference"] == pytest.approx(
                    summary["geometric_annual_return"] - hold["summary"]["geometric_annual_return"]
                )
                assert {"above_one_share", "annual_spread"} <= set(summary)
            for suffix in ("1.0", "1.25", "1.5", "2.0", "drawdown", "volatility"):
                own = _row(probe, market, setting, f"f0001_{suffix}")
                matched = _row(probe, market, setting, f"matched_{suffix}")
                assert matched["constant"] == pytest.approx(own["summary"]["average_exposure"])
                assert own["matched_difference"] == pytest.approx(
                    own["summary"]["geometric_annual_return"]
                    - matched["summary"]["geometric_annual_return"]
                )


def test_the_robust_table_keeps_the_main_leverages(probe: dict[str, Any]) -> None:
    for market in lt.MARKETS:
        main = {r["id"]: r["leverage"] for r in probe["markets"][market]["table"]["main"]}
        robust = {r["id"]: r["leverage"] for r in probe["markets"][market]["table"]["robust"]}
        assert main == robust


def test_the_sensitivities_keep_the_matched_leverages(probe: dict[str, Any]) -> None:
    for market in lt.MARKETS:
        found = probe["markets"][market]
        sens = found["sensitivities"]
        assert list(sens) == ["spread_only", "cost_only", "no_spread", "cap_2"]
        data = DATA[market]
        window = _window(market)
        cash = data.returns["cash"]
        hold = found["buy_and_hold"]["geometric_annual_return"]
        for name, costs in lt.SENSITIVITIES.items():
            for pairing in ("drawdown", "volatility"):
                run = lt.simulate_levered(
                    data.returns,
                    _targets(data),
                    start=window.start,
                    end=window.end,
                    leverage=found[f"{pairing}_match"]["leverage"],
                    costs=costs,
                )
                geo = summarize(run.simulation, cash, LOADED)["geometric_annual_return"]
                assert sens[name][pairing] == pytest.approx(geo - hold)
        for pairing in ("drawdown", "volatility"):
            capped = min(found[f"{pairing}_match"]["leverage"], lt.CAP_SENSITIVITY)
            assert sens["cap_2"][pairing] == _point(probe, market, capped)["main"]["difference"]
    # The synthetic QQQ bear puts L* above the cap, so the cap is read at 2.
    assert probe["markets"]["qqq"]["drawdown_match"]["leverage"] > lt.CAP_SENSITIVITY


def test_the_daily_reset_rows(probe: dict[str, Any]) -> None:
    for market in lt.MARKETS:
        found = probe["markets"][market]
        rows = found["daily_reset"]
        star = found["drawdown_match"]["leverage"]
        assert [r["leverage"] for r in rows] == [2.0, star]
        for row in rows:
            assert row["difference"] == pytest.approx(row["daily"] - row["month_end"])
            assert row["text"] == lt.daily_reset_text(row["difference"])
        assert rows[0]["month_end"] == _point(probe, market, 2.0)["main"]["geometric_annual_return"]


def test_rolling_windows_and_eras_in_the_run(probe: dict[str, Any]) -> None:
    for market in lt.MARKETS:
        rolling = probe["markets"][market]["rolling"]
        assert [r["years"] for r in rolling] == [5, 10]
        counts = probe["check"]["markets"][market]["rolling"]
        assert [r["all"]["count"] for r in rolling] == [counts["5"], counts["10"]]
        assert all(r["near_high"]["count"] <= r["all"]["count"] for r in rolling)
    assert probe["markets"]["qqq"]["eras"] == []
    eras = probe["markets"]["us_market"]["eras"]
    assert [e["era"] for e in eras] == ["1929_1961", "1962_1989", "1990_2010"]
    assert probe["markets"]["us_market"]["old_era_text"] == lt.old_era_text(
        probe["differences"]["main"]["us_market"]["drawdown"], eras
    )


def test_the_run_records_no_bust_on_tame_data(probe: dict[str, Any]) -> None:
    for market in lt.MARKETS:
        assert probe["markets"][market]["busts"] == []


def test_spoiled_data_after_2010_change_nothing(probe: dict[str, Any]) -> None:
    raw = RAW["qqq"]
    late = pd.DatetimeIndex(raw.returns.index) > pd.Timestamp("2010-12-31")
    returns = raw.returns.copy()
    returns.loc[late, "qqq"] = -0.05
    inputs = raw.inputs.copy()
    inputs_late = pd.DatetimeIndex(inputs.index) > pd.Timestamp("2010-12-31")
    numeric = inputs.select_dtypes("number").columns
    inputs.loc[inputs_late, numeric] = inputs.loc[inputs_late, numeric] * 0.5
    garbled = gp.cut(BenchData(returns=returns, inputs=inputs, source=dict(raw.source)))
    again = lt.run_probe(
        {**DATA, "qqq": garbled},
        LOADED,
        GATE,
        ROOT_DIR,
        provenance=PROVENANCE,
        grid=TEST_GRID,
        reproduce=SYN_REPRODUCE,
    )
    assert again["markets"]["qqq"] == probe["markets"]["qqq"]


def test_the_result_is_plain_json(
    probe: dict[str, Any], stopped: dict[str, Any], tmp_path: Path
) -> None:
    for result in (probe, stopped):
        text = json.dumps(result, allow_nan=False)
        assert json.loads(text) == json.loads(json.dumps(result, allow_nan=False))
    lt.write_result(probe, tmp_path)
    assert (
        json.loads(tmp_path.joinpath("leverage_table.json").read_text(encoding="utf-8"))["run_id"]
        == lt.RUN_ID
    )
    assert tmp_path.joinpath("leverage_table.md").read_text(encoding="utf-8") == lt.render_markdown(
        probe
    )


# --- the report -----------------------------------------------------------------------------------

SECTIONS = (
    "## 判定",
    "## 全周期表",
    "## D(L) 曲线",
    "## 滚动窗口",
    "## 年代段（美股大盘）",
    "## 敏感度",
    "## 局限",
)


def test_the_report_has_its_sections_in_order(probe: dict[str, Any]) -> None:
    text = lt.render_markdown(probe)
    positions = [text.index(section) for section in SECTIONS]
    assert positions == sorted(positions)
    assert text.startswith("# ")
    assert lt.RUN_ID in text and lt.OWNER_DECISION in text
    assert probe["verdict"]["text"] in text
    assert lt.ERA_NOTE in text
    for line in lt.LIMITATIONS:
        assert line in text


def test_the_report_prints_the_matched_leverages_and_bounds(probe: dict[str, Any]) -> None:
    text = lt.render_markdown(probe)
    for market in lt.MARKETS:
        found = probe["markets"][market]
        assert f"{found['drawdown_match']['leverage']:.2f}" in text
        assert f"{found['volatility_match']['leverage']:.2f}" in text
        assert f"{found['bound']:.2f}" in text


def test_the_report_has_no_daily_rows(probe: dict[str, Any]) -> None:
    text = lt.render_markdown(probe)
    assert len(text.splitlines()) < 400
    assert not re.search(r"\b20\d\d-\d\d-\d\d\b.*\b20\d\d-\d\d-\d\d\b.*\b20\d\d-\d\d-\d\d\b", text)


def test_a_stopped_report_prints_only_the_self_check(stopped: dict[str, Any]) -> None:
    text = lt.render_markdown(stopped)
    assert lt.STOP_REPRODUCE in text
    for section in SECTIONS[1:]:
        assert section not in text


# --- the module -----------------------------------------------------------------------------------


def test_the_module_reads_no_research_record() -> None:
    source = Path(lt.__file__).read_text(encoding="utf-8")
    assert "runs" not in re.findall(r"\w+", source)
    assert "trial_records" not in source and "window_reads" not in source
    assert "ledger.jsonl" not in source


def test_load_data_is_the_policy_tables_cut_at_2010(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pt, "load_settings", lambda: LOADED)
    monkeypatch.setattr(tr, "market_data", lambda market, *a, **k: RAW[market])
    datasets, gate, loaded, root_dir = lt.load_data(Path("unused"))
    assert set(datasets) == set(lt.MARKETS)
    assert root_dir == ROOT_DIR
    for data in datasets.values():
        assert data.returns.index.max() <= pd.Timestamp("2010-12-31")
        assert data.inputs.index.max() <= pd.Timestamp("2010-12-31")


# --- command line ---------------------------------------------------------------------------------


@pytest.fixture
def cli(
    monkeypatch: pytest.MonkeyPatch, probe: dict[str, Any], stopped: dict[str, Any]
) -> list[dict[str, Any]]:
    """The command line on the module fixtures' results: no data are loaded, nothing is rerun.
    The command must pass the module's GRID and REPRODUCE as they are when it runs."""

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(lt, "load_data", lambda root: (DATA, GATE, LOADED, ROOT_DIR))
    monkeypatch.setattr(lt, "git_provenance", lambda root: PROVENANCE)
    monkeypatch.setattr(lt, "GRID", TEST_GRID)
    monkeypatch.setattr(lt, "REPRODUCE", SYN_REPRODUCE)

    def run_probe(*args: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return probe if kwargs["reproduce"] is SYN_REPRODUCE else stopped

    monkeypatch.setattr(lt, "run_probe", run_probe)
    return calls


def test_check_command_writes_the_counts(cli: list[dict[str, Any]], tmp_path: Path) -> None:
    result = CliRunner().invoke(lt.app, ["check", "--out", str(tmp_path)])
    assert result.exit_code == 0, result.output
    written = json.loads(tmp_path.joinpath("leverage_check.json").read_text(encoding="utf-8"))
    assert written == lt.check_inputs(DATA, LOADED, GATE, ROOT_DIR)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["leverage_check.json"]
    assert result.output.count("\n") <= 3


def test_run_command_writes_the_table(cli: list[dict[str, Any]], tmp_path: Path) -> None:
    CliRunner().invoke(lt.app, ["check", "--out", str(tmp_path)])
    result = CliRunner().invoke(lt.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert len(cli) == 1
    assert tuple(cli[0]["grid"]) == TEST_GRID and cli[0]["reproduce"] is SYN_REPRODUCE
    assert cli[0]["provenance"] == PROVENANCE
    written = json.loads(tmp_path.joinpath("leverage_check.json").read_text(encoding="utf-8"))
    assert written == lt.check_inputs(DATA, LOADED, GATE, ROOT_DIR)
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "leverage_check.json",
        "leverage_table.json",
        "leverage_table.md",
    ]
    table = json.loads(tmp_path.joinpath("leverage_table.json").read_text(encoding="utf-8"))
    assert table["verdict"]["verdict"] in {"holds", "not_holds"}
    assert table["verdict"]["verdict"] in result.output


def test_run_command_refuses_to_overwrite(cli: list[dict[str, Any]], tmp_path: Path) -> None:
    tmp_path.joinpath("leverage_table.json").write_text("{}", encoding="utf-8")
    result = CliRunner().invoke(lt.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert cli == []
    assert tmp_path.joinpath("leverage_table.json").read_text(encoding="utf-8") == "{}"


def test_run_command_stops_on_a_failed_self_check(
    cli: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(lt, "REPRODUCE", REAL_REPRODUCE)
    result = CliRunner().invoke(lt.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert lt.STOP_REPRODUCE in result.output
    table = json.loads(tmp_path.joinpath("leverage_table.json").read_text(encoding="utf-8"))
    assert table["stopped"]["reason"] == "reproduce" and table["markets"] == {}
