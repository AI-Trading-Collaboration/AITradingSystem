from __future__ import annotations

from datetime import date

from ai_trading_system.trading_calendar import (
    is_us_equity_trading_day,
    previous_us_equity_trading_day,
    us_equity_full_day_holidays,
    us_equity_market_session,
)


def test_us_equity_market_session_identifies_weekend() -> None:
    session = us_equity_market_session(date(2026, 5, 10))

    assert session.session_status == "CLOSED_MARKET"
    assert session.reason == "weekend"
    assert session.previous_trading_day == date(2026, 5, 8)


def test_us_equity_calendar_covers_regular_2026_holidays() -> None:
    holidays = us_equity_full_day_holidays(2026)

    assert holidays[date(2026, 7, 3)] == "Independence Day"
    assert holidays[date(2026, 11, 26)] == "Thanksgiving Day"
    assert not is_us_equity_trading_day(date(2026, 7, 3))
    assert not is_us_equity_trading_day(date(2026, 11, 26))


def test_previous_trading_day_skips_holiday_weekend() -> None:
    assert previous_us_equity_trading_day(date(2026, 7, 6)) == date(2026, 7, 2)


def test_regular_weekday_is_trading_day() -> None:
    session = us_equity_market_session(date(2026, 5, 8))

    assert session.session_status == "TRADING_DAY"
    assert session.is_trading_day is True
    assert session.reason == "regular_trading_day"


def test_new_years_day_saturday_is_not_observed_on_prior_friday() -> None:
    session = us_equity_market_session(date(2027, 12, 31))

    assert session.session_status == "TRADING_DAY"
    assert is_us_equity_trading_day(date(2027, 12, 31))
