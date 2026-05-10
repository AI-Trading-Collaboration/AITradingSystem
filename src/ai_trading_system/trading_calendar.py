from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

NYSE_REGULAR_HOLIDAY_CALENDAR_SOURCE = (
    "NYSE regular full-day holiday rules: weekends, New Year's Day, "
    "Martin Luther King Jr. Day, Washington's Birthday, Good Friday, "
    "Memorial Day, Juneteenth, Independence Day, Labor Day, Thanksgiving Day, "
    "and Christmas Day. Does not include unscheduled special closures."
)


@dataclass(frozen=True)
class MarketSession:
    as_of: date
    market: str
    session_status: str
    is_trading_day: bool
    reason: str
    previous_trading_day: date
    calendar_source: str = NYSE_REGULAR_HOLIDAY_CALENDAR_SOURCE


def us_equity_market_session(as_of: date) -> MarketSession:
    holiday_name = us_equity_full_day_holidays(as_of.year).get(as_of)
    if as_of.weekday() >= 5:
        return MarketSession(
            as_of=as_of,
            market="US_EQUITY",
            session_status="CLOSED_MARKET",
            is_trading_day=False,
            reason="weekend",
            previous_trading_day=previous_us_equity_trading_day(as_of),
        )
    if holiday_name is not None:
        return MarketSession(
            as_of=as_of,
            market="US_EQUITY",
            session_status="CLOSED_MARKET",
            is_trading_day=False,
            reason=holiday_name,
            previous_trading_day=previous_us_equity_trading_day(as_of),
        )
    return MarketSession(
        as_of=as_of,
        market="US_EQUITY",
        session_status="TRADING_DAY",
        is_trading_day=True,
        reason="regular_trading_day",
        previous_trading_day=previous_us_equity_trading_day(as_of),
    )


def is_us_equity_trading_day(value: date) -> bool:
    return value.weekday() < 5 and value not in us_equity_full_day_holidays(value.year)


def previous_us_equity_trading_day(value: date) -> date:
    candidate = value - timedelta(days=1)
    while not is_us_equity_trading_day(candidate):
        candidate -= timedelta(days=1)
    return candidate


def us_equity_full_day_holidays(year: int) -> dict[date, str]:
    holidays: dict[date, str] = {}
    for actual_year in (year - 1, year, year + 1):
        for holiday_date, holiday_name in _regular_holiday_dates(actual_year):
            observed_date = _observed_fixed_holiday(holiday_date, holiday_name)
            if observed_date is not None and observed_date.year == year:
                holidays[observed_date] = holiday_name
    return holidays


def _regular_holiday_dates(year: int) -> tuple[tuple[date, str], ...]:
    return (
        (date(year, 1, 1), "New Year's Day"),
        (_nth_weekday(year, 1, 0, 3), "Martin Luther King Jr. Day"),
        (_nth_weekday(year, 2, 0, 3), "Washington's Birthday"),
        (_easter_sunday(year) - timedelta(days=2), "Good Friday"),
        (_last_weekday(year, 5, 0), "Memorial Day"),
        (date(year, 6, 19), "Juneteenth National Independence Day"),
        (date(year, 7, 4), "Independence Day"),
        (_nth_weekday(year, 9, 0, 1), "Labor Day"),
        (_nth_weekday(year, 11, 3, 4), "Thanksgiving Day"),
        (date(year, 12, 25), "Christmas Day"),
    )


def _observed_fixed_holiday(value: date, holiday_name: str) -> date | None:
    if holiday_name == "New Year's Day" and value.weekday() == 5:
        return None
    if value.weekday() == 5:
        return value - timedelta(days=1)
    if value.weekday() == 6:
        return value + timedelta(days=1)
    return value


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    candidate = date(year, month, 1)
    offset = (weekday - candidate.weekday()) % 7
    return candidate + timedelta(days=offset + (n - 1) * 7)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        candidate = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        candidate = date(year, month + 1, 1) - timedelta(days=1)
    offset = (candidate.weekday() - weekday) % 7
    return candidate - timedelta(days=offset)


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    weekday_adjustment = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * weekday_adjustment) // 451
    month = (h + weekday_adjustment - 7 * m + 114) // 31
    day = ((h + weekday_adjustment - 7 * m + 114) % 31) + 1
    return date(year, month, day)
