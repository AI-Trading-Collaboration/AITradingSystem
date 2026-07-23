from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from ai_trading_system.trading_calendar import (
    current_us_equity_market_date,
    is_us_equity_trading_day,
    latest_completed_us_equity_trading_day,
    previous_us_equity_trading_day,
    resolve_default_data_quality_as_of,
    us_equity_full_day_holidays,
    us_equity_market_session,
    us_equity_partial_trading_days,
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


def test_juneteenth_closure_starts_in_2022_without_rewriting_2021_history() -> None:
    assert date(2021, 6, 18) not in us_equity_full_day_holidays(2021)
    assert is_us_equity_trading_day(date(2021, 6, 18))
    assert us_equity_market_session(date(2021, 6, 18)).session_status == "TRADING_DAY"

    assert us_equity_full_day_holidays(2022)[date(2022, 6, 20)] == (
        "Juneteenth National Independence Day"
    )
    assert not is_us_equity_trading_day(date(2022, 6, 20))


def test_primary_research_window_start_is_a_regular_trading_day() -> None:
    session = us_equity_market_session(date(2021, 2, 22))

    assert session.session_status == "TRADING_DAY"
    assert session.is_trading_day is True


def test_previous_trading_day_skips_holiday_weekend() -> None:
    assert previous_us_equity_trading_day(date(2026, 7, 6)) == date(2026, 7, 2)


def test_regular_weekday_is_trading_day() -> None:
    session = us_equity_market_session(date(2026, 5, 8))

    assert session.session_status == "TRADING_DAY"
    assert session.is_trading_day is True
    assert session.reason == "regular_trading_day"
    assert session.session_kind == "NORMAL_TRADING_DAY"


def test_partial_trading_day_uses_early_close_session() -> None:
    partial_days = us_equity_partial_trading_days(2026)
    session = us_equity_market_session(date(2026, 11, 27))

    assert partial_days[date(2026, 11, 27)] == "Day after Thanksgiving early close"
    assert session.session_status == "PARTIAL_TRADING_DAY"
    assert session.session_kind == "PARTIAL_TRADING_DAY"
    assert session.is_trading_day is True
    assert session.close_time is not None
    assert session.close_time.isoformat() == "13:00:00"


def test_new_years_day_saturday_is_not_observed_on_prior_friday() -> None:
    session = us_equity_market_session(date(2027, 12, 31))

    assert session.session_status == "TRADING_DAY"
    assert is_us_equity_trading_day(date(2027, 12, 31))


def test_latest_completed_trading_day_uses_new_york_close_not_local_date() -> None:
    tokyo_morning_after_us_close = datetime(
        2026,
        5,
        12,
        10,
        44,
        tzinfo=ZoneInfo("Asia/Tokyo"),
    )

    assert latest_completed_us_equity_trading_day(tokyo_morning_after_us_close) == date(
        2026,
        5,
        11,
    )


def test_current_market_date_uses_new_york_timezone() -> None:
    tokyo_morning = datetime(2026, 5, 12, 10, 44, tzinfo=ZoneInfo("Asia/Tokyo"))

    assert current_us_equity_market_date(tokyo_morning) == date(2026, 5, 11)


def test_latest_completed_trading_day_waits_for_post_close_buffer() -> None:
    new_york = ZoneInfo("America/New_York")

    assert latest_completed_us_equity_trading_day(
        datetime(2026, 5, 11, 16, 29, tzinfo=new_york)
    ) == date(2026, 5, 8)
    assert latest_completed_us_equity_trading_day(
        datetime(2026, 5, 11, 16, 30, tzinfo=new_york)
    ) == date(2026, 5, 11)


def test_latest_completed_trading_day_skips_weekends_and_holidays() -> None:
    new_york = ZoneInfo("America/New_York")

    assert latest_completed_us_equity_trading_day(
        datetime(2026, 5, 10, 12, 0, tzinfo=new_york)
    ) == date(2026, 5, 8)
    assert latest_completed_us_equity_trading_day(
        datetime(2026, 7, 3, 12, 0, tzinfo=new_york)
    ) == date(2026, 7, 2)


def test_latest_completed_trading_day_rejects_naive_datetime() -> None:
    try:
        latest_completed_us_equity_trading_day(datetime(2026, 5, 11, 16, 30))
    except ValueError as exc:
        assert "timezone-aware" in str(exc)
    else:
        raise AssertionError("expected timezone-aware validation")


def test_default_data_quality_as_of_waits_for_three_hour_provider_buffer() -> None:
    new_york = ZoneInfo("America/New_York")

    assert resolve_default_data_quality_as_of(
        datetime(2026, 5, 11, 18, 59, 59, tzinfo=new_york)
    ) == date(2026, 5, 8)
    assert resolve_default_data_quality_as_of(
        datetime(2026, 5, 11, 19, 0, tzinfo=new_york)
    ) == date(2026, 5, 11)


def test_default_data_quality_as_of_uses_partial_close_and_market_holidays() -> None:
    new_york = ZoneInfo("America/New_York")

    assert resolve_default_data_quality_as_of(
        datetime(2026, 11, 27, 15, 59, 59, tzinfo=new_york)
    ) == date(2026, 11, 25)
    assert resolve_default_data_quality_as_of(
        datetime(2026, 11, 27, 16, 0, tzinfo=new_york)
    ) == date(2026, 11, 27)
    assert resolve_default_data_quality_as_of(datetime(2026, 7, 3, 20, 0, tzinfo=new_york)) == date(
        2026, 7, 2
    )


def test_default_data_quality_as_of_rejects_naive_observed_at() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        resolve_default_data_quality_as_of(datetime(2026, 5, 11, 19, 0))
