from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from ai_trading_system.market_calendar_freshness import (
    resolve_us_equity_market_freshness,
)


def test_market_calendar_freshness_normal_completed_trading_day() -> None:
    new_york = ZoneInfo("America/New_York")

    context = resolve_us_equity_market_freshness(
        requested_as_of=date(2024, 4, 19),
        observed_at=datetime(2024, 4, 19, 16, 31, tzinfo=new_york),
    )

    assert context.latest_complete_market_date == date(2024, 4, 19)
    assert context.freshness_reference_date == date(2024, 4, 19)
    assert context.market_calendar_status == "TRADING_DAY"
    assert context.market_session_kind == "NORMAL_TRADING_DAY"
    assert context.calendar_adjustment_reason == "requested_market_date_within_complete_range"
    assert context.calendar_adjusted_staleness is False


def test_market_calendar_freshness_weekend_uses_previous_trading_day() -> None:
    new_york = ZoneInfo("America/New_York")

    context = resolve_us_equity_market_freshness(
        requested_as_of=date(2024, 4, 20),
        observed_at=datetime(2024, 4, 22, 12, 0, tzinfo=new_york),
    )

    assert context.latest_complete_market_date == date(2024, 4, 19)
    assert context.freshness_reference_date == date(2024, 4, 19)
    assert context.market_calendar_status == "CLOSED_MARKET"
    assert context.market_session_kind == "WEEKEND"
    assert (
        context.calendar_adjustment_reason
        == "non_trading_requested_date_uses_previous_trading_day"
    )
    assert context.calendar_adjusted_staleness is True


def test_market_calendar_freshness_holiday_uses_previous_trading_day() -> None:
    new_york = ZoneInfo("America/New_York")

    context = resolve_us_equity_market_freshness(
        requested_as_of=date(2024, 7, 4),
        observed_at=datetime(2024, 7, 5, 16, 31, tzinfo=new_york),
    )

    assert context.latest_complete_market_date == date(2024, 7, 5)
    assert context.freshness_reference_date == date(2024, 7, 3)
    assert context.market_calendar_status == "CLOSED_MARKET"
    assert context.market_session_kind == "US_MARKET_HOLIDAY"
    assert context.market_calendar_reason == "Independence Day"
    assert (
        context.calendar_adjustment_reason
        == "non_trading_requested_date_uses_previous_trading_day"
    )


def test_market_calendar_freshness_normal_vendor_delay_window() -> None:
    new_york = ZoneInfo("America/New_York")

    context = resolve_us_equity_market_freshness(
        requested_as_of=date(2024, 4, 22),
        observed_at=datetime(2024, 4, 22, 16, 5, tzinfo=new_york),
    )

    assert context.latest_complete_market_date == date(2024, 4, 19)
    assert context.freshness_reference_date == date(2024, 4, 19)
    assert context.market_session_kind == "NORMAL_TRADING_DAY"
    assert context.calendar_adjustment_reason == "data_vendor_delay_window"
    assert context.calendar_adjusted_staleness is True


def test_market_calendar_freshness_partial_trading_day_close() -> None:
    new_york = ZoneInfo("America/New_York")

    before_ready = resolve_us_equity_market_freshness(
        requested_as_of=date(2026, 11, 27),
        observed_at=datetime(2026, 11, 27, 13, 5, tzinfo=new_york),
    )
    after_ready = resolve_us_equity_market_freshness(
        requested_as_of=date(2026, 11, 27),
        observed_at=datetime(2026, 11, 27, 13, 31, tzinfo=new_york),
    )

    assert before_ready.market_calendar_status == "PARTIAL_TRADING_DAY"
    assert before_ready.market_session_kind == "PARTIAL_TRADING_DAY"
    assert before_ready.latest_complete_market_date == date(2026, 11, 25)
    assert before_ready.freshness_reference_date == date(2026, 11, 25)
    assert before_ready.calendar_adjustment_reason == "data_vendor_delay_window"
    assert after_ready.latest_complete_market_date == date(2026, 11, 27)
    assert after_ready.freshness_reference_date == date(2026, 11, 27)
    assert after_ready.calendar_adjusted_staleness is False


def test_market_calendar_freshness_rejects_naive_observed_at() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        resolve_us_equity_market_freshness(
            requested_as_of=date(2024, 4, 19),
            observed_at=datetime(2024, 4, 19, 16, 31),
        )
