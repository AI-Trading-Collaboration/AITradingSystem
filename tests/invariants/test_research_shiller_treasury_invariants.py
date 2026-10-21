"""Invariants of the Treasury priced month by month from Shiller's rate (TRADING-2565-40).

Acceptance tests written before the code (owner decision
``owner_decision:TRADING-2565:2026-10-21:treasury_1929``). All data is synthetic.

64. The bond earns its coupon and reprices on yields only: a constant yield y compounds to
    (1 + y / 12) ** 12 - 1 a year, every month is the par-bond move from last month's yield plus
    a twelfth of last month's yield, and its dates share one daily rate.
65. No date sees a later month: changing the yields from any month on leaves every earlier date's
    return as it was, and a month without both yields is empty.
66. T0002 stays reproducible and T0008 lands on the ruler's window: treasury_10y is the
    pre-batch formula to the bit, the gate maps only treasury_10y to the 1962 window, and the
    Shiller reserve resolves to us_market_1929_2010 on the US market and the QQQ development
    window on QQQ, both named by the portfolio rules with their declines listed.
67. The stand-in check's use rule is fixed before the run (calendar-year correlation 0.90, 12
    agreeing declines) and T0008 holds only the Shiller Treasury at T0002's weights.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import (
    SHILLER_TREASURY_RESERVE,
    TREASURY_RESERVE,
    monthly_treasury_returns,
    treasury_returns,
)
from ai_trading_system.research_bench.settings import load_settings, resolve_window
from ai_trading_system.research_bench.sources import DEFAULT_MAX_STALE_DAYS, known_at
from ai_trading_system.research_probes import treasury_stand_in as ts

LOADED = load_settings()
GATE_FILE = PROJECT_ROOT / "research/_trials/prereg.yaml"
PLAN_FILE = PROJECT_ROOT / "research/_trials/T0008.yaml"


def _yields(seed: int, months: int = 360, start: str = "1928-12-01") -> pd.Series:
    steps = np.random.default_rng(seed).normal(0.0, 0.2, months)
    level = np.clip(4.0 + np.cumsum(steps), 0.5, 20.0)
    return pd.Series(level, index=pd.date_range(start, periods=months, freq="MS"))


CALENDAR = pd.bdate_range("1929-01-02", "1957-11-29")


def _price(coupon: float, rate: float) -> float:
    half = 1.0 + rate / 2.0
    return sum((coupon / 2.0) / half**k for k in range(1, 21)) + 1.0 / half**20


# 64. The bond earns its coupon and reprices on yields only ----------------------------------------


@pytest.mark.parametrize("percent", [0.5, 2.5, 6.0, 15.0])
def test_a_constant_yield_compounds_to_its_monthly_coupon(percent: float) -> None:
    yields = pd.Series(percent, index=pd.date_range("1928-12-01", periods=14, freq="MS"))
    calendar = pd.bdate_range("1929-01-02", "1929-12-31")
    out = monthly_treasury_returns(yields, calendar)
    assert float(np.prod(1.0 + out.to_numpy())) - 1.0 == pytest.approx(
        (1.0 + percent / 1200) ** 12 - 1.0, abs=1e-12
    )


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_each_month_is_the_par_bond_move_plus_a_twelfth_coupon(seed: int) -> None:
    yields = _yields(seed)
    out = monthly_treasury_returns(yields, CALENDAR)
    months = CALENDAR.to_period("M")
    rates = pd.Series(yields.to_numpy() / 100, index=yields.index.to_period("M"))
    for month in months.unique():
        inside = out.to_numpy()[np.asarray(months == month)]
        assert np.unique(inside).size == 1
        before, now = rates[month - 1], rates[month]
        expected = _price(before, now) - 1.0 + before / 12
        assert float(np.prod(1.0 + inside)) - 1.0 == pytest.approx(expected, abs=1e-12)


# 65. No date sees a later month ------------------------------------------------------------------


@pytest.mark.parametrize("cut", [1, 37, 200, 340])
def test_changing_later_yields_leaves_earlier_dates(cut: int) -> None:
    yields = _yields(7)
    changed = yields.copy()
    changed.iloc[cut:] = changed.iloc[cut:] + 3.0
    a = monthly_treasury_returns(yields, CALENDAR)
    b = monthly_treasury_returns(changed, CALENDAR)
    first = yields.index[cut]
    assert a.loc[a.index < first].equals(b.loc[b.index < first])
    assert not a.loc[a.index >= first].equals(b.loc[b.index >= first])


def test_a_month_without_both_yields_is_empty() -> None:
    yields = _yields(8)
    gap = yields.index[50]
    out = monthly_treasury_returns(yields.drop(gap), CALENDAR)
    month = gap.to_period("M")
    months = CALENDAR.to_period("M")
    assert out[np.asarray(months == month)].isna().all()
    assert out[np.asarray(months == month + 1)].isna().all()
    assert out[np.asarray((months != month) & (months != month + 1))].notna().all()


# 66. T0002 stays reproducible and T0008 lands on the ruler's window ------------------------------


def _treasury_before(yields: pd.Series, calendar: pd.DatetimeIndex) -> pd.Series:
    """treasury_returns as it was before TRADING-2565-40."""

    y = known_at(yields / 100, calendar, 0, DEFAULT_MAX_STALE_DAYS)
    coupon = y.shift(1).to_numpy(dtype=float)
    rates = y.to_numpy(dtype=float)
    days = np.full(len(calendar), np.nan)
    days[1:] = (calendar[1:] - calendar[:-1]).days
    discount = (1 + rates / 2) ** -20
    price = coupon / rates * (1 - discount) + discount
    return pd.Series(price - 1 + coupon * days / 365, index=calendar, name="treasury_10y")


def test_the_daily_treasury_is_the_formula_before_the_batch() -> None:
    calendar = pd.bdate_range("1962-01-02", "1970-12-31")
    steps = np.random.default_rng(3).normal(0.0, 0.03, len(calendar))
    yields = pd.Series(np.clip(4.0 + np.cumsum(steps), 1.0, 15.0), index=calendar)
    yields.iloc[100:103] = np.nan
    assert treasury_returns(yields, calendar).equals(_treasury_before(yields, calendar))


def test_the_shiller_treasury_runs_on_the_windows_the_ruler_names() -> None:
    gate = tr.load_gate(GATE_FILE)
    us, qqq = gate.markets["us_market"], gate.markets["qqq"]
    assert us.reserve_question_types == {TREASURY_RESERVE: "us_market_bond_trial"}
    assert qqq.reserve_question_types is None
    for entry, window_id in (
        (us, "us_market_1929_2010"),
        (qqq, "qqq_long_history_development"),
    ):
        question_type = entry.question_type_for(SHILLER_TREASURY_RESERVE)
        assert resolve_window(LOADED, question_type, "development").window_id == window_id
        assert entry.portfolio_rules_for(window_id) is not None
        assert entry.decline_list(window_id)
    assert us.portfolio_rules_for("us_market_1962_2010") is None


# 67. The use rule is fixed and T0008 holds only the Shiller Treasury ------------------------------


def test_the_stand_in_rule_and_the_plan_are_fixed() -> None:
    assert (ts.MIN_CORRELATION, ts.MIN_AGREE) == (0.90, 12)
    assert [ts.use_rule(c, a) for c, a in ((0.90, 12), (0.8999, 15), (0.99, 11))] == [
        "usable",
        "diverges",
        "diverges",
    ]
    gate = tr.load_gate(GATE_FILE)
    plan = tr.load_plan(PLAN_FILE, gate)
    assert plan.owner_decision == "owner_decision:TRADING-2565:2026-10-21:treasury_1929"
    assert (plan.candidate, plan.from_validation_window) == (None, True)
    assert plan.data.signal_snapshot_id == "sig-ed9e988ffe7c"
    assert [(c.function, c.reserve, c.params["exposure"]) for c in plan.configs] == [
        ("fixed_exposure", SHILLER_TREASURY_RESERVE, 0.6),
        ("fixed_exposure", SHILLER_TREASURY_RESERVE, 0.5),
        ("fixed_exposure", SHILLER_TREASURY_RESERVE, 0.7),
    ]
    assert plan.primary == plan.configs[0].id
    assert all(tr.uses_portfolio_ruler(c) for c in plan.configs)
