"""Acceptance tests for the Treasury priced month by month from Shiller's rate (TRADING-2565-40).

Written before the code (owner decision ``owner_decision:TRADING-2565:2026-10-21:treasury_1929``,
recorded in ``docs/requirements/TRADING-2565_Research_Bench.md``). The reserve
``treasury_10y_shiller`` is the ten-year par bond of ``treasury_10y`` priced from monthly yields:
month M returns P(y[M-1], y[M]) - 1 + y[M-1] / 12, spread evenly over the month's dates. Every test
uses synthetic data.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from ai_trading_system.research_bench.data import (
    RESERVES,
    SHILLER_TREASURY_RESERVE,
    SHILLER_TREASURY_SOURCE,
    TREASURY_RESERVE,
    TREASURY_SOURCE,
    BenchData,
    monthly_treasury_returns,
    par_bond_price,
    strategy_returns,
    treasury_returns,
    with_reserves,
)


def _months(start: str, values: list[float]) -> pd.Series:
    return pd.Series(values, index=pd.date_range(start, periods=len(values), freq="MS"))


def _compounded(daily: pd.Series) -> pd.Series:
    months = pd.DatetimeIndex(daily.index).to_period("M")
    return (1.0 + daily).groupby(months).prod() - 1.0


def _bench(calendar: pd.DatetimeIndex) -> BenchData:
    returns = pd.DataFrame({"qqq": 0.0, "cash": 0.0001}, index=calendar)
    return BenchData(returns=returns, inputs=pd.DataFrame(index=calendar), source={})


def test_the_reserve_is_known_by_name_and_source() -> None:
    assert RESERVES == ("cash", "treasury_10y", "treasury_10y_shiller")
    assert (SHILLER_TREASURY_RESERVE, SHILLER_TREASURY_SOURCE) == (
        "treasury_10y_shiller",
        "shiller_gs10",
    )
    assert (TREASURY_RESERVE, TREASURY_SOURCE) == ("treasury_10y", "dgs10")


def test_a_constant_yield_earns_a_twelfth_of_it_each_month_spread_over_the_dates() -> None:
    calendar = pd.bdate_range("1929-01-02", "1929-03-29")
    out = monthly_treasury_returns(_months("1928-12-01", [5.0] * 4), calendar)

    assert out.name == SHILLER_TREASURY_RESERVE
    assert out.index.equals(calendar)
    assert _compounded(out).tolist() == pytest.approx([0.05 / 12] * 3, abs=1e-14)
    for month in ("1929-01", "1929-02", "1929-03"):
        inside = out.loc[month]
        assert inside.nunique() == 1
        assert (1.0 + inside.iloc[0]) ** len(inside) - 1.0 == pytest.approx(0.05 / 12, abs=1e-14)


def test_a_month_reprices_the_par_bond_from_the_last_month_s_yield() -> None:
    calendar = pd.bdate_range("1931-01-01", "1931-03-31")
    out = _compounded(
        monthly_treasury_returns(_months("1930-12-01", [3.0, 3.5, 3.2, 3.2]), calendar)
    )

    january = par_bond_price(0.03, 0.035) - 1.0 + 0.03 / 12
    february = par_bond_price(0.035, 0.032) - 1.0 + 0.035 / 12
    assert out.tolist()[:2] == pytest.approx([january, february], abs=1e-12)
    assert out.iloc[2] == pytest.approx(0.032 / 12, abs=1e-14)
    # About -4% for half a point at 3%: the duration of a ten-year bond.
    assert january == pytest.approx(-0.039, abs=0.003)


def test_the_price_is_the_daily_treasury_s_price() -> None:
    # The same ten-year par bond as treasury_10y, whose day adds a calendar day's coupon.
    calendar = pd.DatetimeIndex(["2001-01-04", "2001-01-05"])
    daily = treasury_returns(pd.Series([5.0, 5.4], index=calendar), calendar)
    assert daily.iloc[1] == pytest.approx(par_bond_price(0.05, 0.054) - 1 + 0.05 / 365, abs=1e-15)
    half = 1.0 + 0.054 / 2.0
    exact = sum(0.025 / half**k for k in range(1, 21)) + 1.0 / half**20
    assert par_bond_price(0.05, 0.054) == pytest.approx(exact, abs=1e-14)


def test_a_month_without_both_yields_is_empty() -> None:
    calendar = pd.bdate_range("1940-01-01", "1940-04-30")
    yields = _months("1939-12-01", [2.5, 2.4, np.nan, 2.3, 2.2])
    out = monthly_treasury_returns(yields, calendar)

    assert out.loc["1940-01"].notna().all()
    assert out.loc["1940-02"].isna().all()  # no February yield
    assert out.loc["1940-03"].isna().all()  # no yield for the month before
    assert out.loc["1940-04"].notna().all()
    first = monthly_treasury_returns(_months("1940-01-01", [2.5, 2.4]), calendar)
    assert first.loc["1940-01"].isna().all()  # nothing before the first month


def test_no_date_sees_a_later_month() -> None:
    calendar = pd.bdate_range("1950-01-02", "1950-12-29")
    base = _months("1949-12-01", list(np.linspace(2.0, 3.0, 13)))
    changed = base.copy()
    changed.loc["1950-07-01":] = 9.0
    a = monthly_treasury_returns(base, calendar)
    b = monthly_treasury_returns(changed, calendar)
    assert a.loc[:"1950-06-30"].equals(b.loc[:"1950-06-30"])
    assert not a.loc["1950-07"].equals(b.loc["1950-07"])


def test_bad_yields_are_refused() -> None:
    calendar = pd.bdate_range("1930-01-01", "1930-02-28")
    with pytest.raises(ValueError, match="Treasury yields must be percent"):
        monthly_treasury_returns(_months("1929-12-01", [0.05, 0.05, 0.05]), calendar)
    twice = pd.Series(
        [3.0, 3.1, 3.2], index=pd.DatetimeIndex(["1930-01-01", "1930-01-15", "1930-02-01"])
    )
    with pytest.raises(ValueError, match="increasing, unique months"):
        monthly_treasury_returns(twice, calendar)


def test_both_reserves_attach_and_the_daily_one_is_unchanged() -> None:
    calendar = pd.bdate_range("1962-01-02", "1963-12-31")
    daily = pd.DataFrame({"value": np.linspace(4.0, 4.5, len(calendar))}, index=calendar)
    monthly = pd.DataFrame(
        {"value": np.linspace(4.0, 4.5, 25)},
        index=pd.date_range("1961-12-01", periods=25, freq="MS"),
    )
    data = _bench(calendar)

    both = with_reserves(data, {TREASURY_SOURCE: daily, SHILLER_TREASURY_SOURCE: monthly})
    assert set(both.reserves) == {TREASURY_RESERVE, SHILLER_TREASURY_RESERVE}
    assert both.reserves[TREASURY_RESERVE].equals(treasury_returns(daily["value"], calendar))
    assert both.reserves[SHILLER_TREASURY_RESERVE].equals(
        monthly_treasury_returns(monthly["value"], calendar)
    )
    alone = with_reserves(data, {SHILLER_TREASURY_SOURCE: monthly})
    assert set(alone.reserves) == {SHILLER_TREASURY_RESERVE}
    assert with_reserves(data, {}) is data
    with pytest.raises(ValueError, match="shiller_gs10 frame needs a value column"):
        with_reserves(data, {SHILLER_TREASURY_SOURCE: monthly.rename(columns={"value": "x"})})
    traded = strategy_returns(both, SHILLER_TREASURY_RESERVE)
    assert traded["cash"].equals(both.reserves[SHILLER_TREASURY_RESERVE].reindex(calendar))
    assert traded["qqq"].equals(data.returns["qqq"])
    assert not math.isnan(traded["cash"].iloc[0])
