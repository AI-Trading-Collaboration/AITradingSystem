from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

from ai_trading_system.trading_calendar import (
    US_EQUITY_DEFAULT_POST_CLOSE_BUFFER,
    US_EQUITY_MARKET_TIMEZONE,
    latest_completed_us_equity_trading_day,
    us_equity_market_session,
)


@dataclass(frozen=True)
class MarketCalendarFreshnessContext:
    market: str
    requested_as_of: date
    latest_complete_market_date: date
    freshness_reference_date: date
    market_calendar_status: str
    market_calendar_reason: str
    market_session_kind: str
    calendar_adjustment_reason: str
    calendar_adjusted_staleness: bool
    calendar_source: str
    market_close_time: str
    data_ready_time: str
    data_vendor_delay_minutes: int
    observed_at: datetime

    def to_report_dict(self) -> dict[str, Any]:
        return {
            "market": self.market,
            "requested_as_of": self.requested_as_of.isoformat(),
            "latest_complete_market_date": self.latest_complete_market_date.isoformat(),
            "freshness_reference_date": self.freshness_reference_date.isoformat(),
            "market_calendar_status": self.market_calendar_status,
            "market_calendar_reason": self.market_calendar_reason,
            "market_session_kind": self.market_session_kind,
            "calendar_adjustment_reason": self.calendar_adjustment_reason,
            "calendar_adjusted_staleness": self.calendar_adjusted_staleness,
            "calendar_source": self.calendar_source,
            "market_close_time": self.market_close_time,
            "data_ready_time": self.data_ready_time,
            "data_vendor_delay_minutes": self.data_vendor_delay_minutes,
            "observed_at": self.observed_at.isoformat(),
            "production_effect": "none",
        }


def resolve_us_equity_market_freshness(
    *,
    requested_as_of: date,
    observed_at: datetime | None = None,
    post_close_buffer: timedelta = US_EQUITY_DEFAULT_POST_CLOSE_BUFFER,
) -> MarketCalendarFreshnessContext:
    current = observed_at or datetime.now(tz=UTC)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    if post_close_buffer < timedelta(0):
        raise ValueError("post_close_buffer must be non-negative")

    session = us_equity_market_session(requested_as_of)
    requested_market_reference = (
        requested_as_of if session.is_trading_day else session.previous_trading_day
    )
    latest_complete = latest_completed_us_equity_trading_day(
        current,
        post_close_buffer=post_close_buffer,
    )
    freshness_reference = min(requested_market_reference, latest_complete)
    close_at = _session_close_at(requested_as_of)
    ready_at = close_at + post_close_buffer if close_at is not None else None
    adjustment_reason = _calendar_adjustment_reason(
        requested_as_of=requested_as_of,
        requested_market_reference=requested_market_reference,
        latest_complete_market_date=latest_complete,
        observed_at=current,
        close_at=close_at,
        ready_at=ready_at,
        session_kind=session.session_kind,
    )
    return MarketCalendarFreshnessContext(
        market=session.market,
        requested_as_of=requested_as_of,
        latest_complete_market_date=latest_complete,
        freshness_reference_date=freshness_reference,
        market_calendar_status=session.session_status,
        market_calendar_reason=session.reason,
        market_session_kind=session.session_kind,
        calendar_adjustment_reason=adjustment_reason,
        calendar_adjusted_staleness=freshness_reference != requested_as_of,
        calendar_source=session.calendar_source,
        market_close_time="" if close_at is None else close_at.isoformat(),
        data_ready_time="" if ready_at is None else ready_at.isoformat(),
        data_vendor_delay_minutes=int(post_close_buffer.total_seconds() // 60),
        observed_at=current,
    )


def _session_close_at(value: date) -> datetime | None:
    session = us_equity_market_session(value)
    if not session.is_trading_day or session.close_time is None:
        return None
    return datetime.combine(
        value,
        session.close_time,
        tzinfo=US_EQUITY_MARKET_TIMEZONE,
    )


def _calendar_adjustment_reason(
    *,
    requested_as_of: date,
    requested_market_reference: date,
    latest_complete_market_date: date,
    observed_at: datetime,
    close_at: datetime | None,
    ready_at: datetime | None,
    session_kind: str,
) -> str:
    if requested_market_reference != requested_as_of:
        if requested_market_reference > latest_complete_market_date:
            return "non_trading_requested_date_after_latest_complete_market_date"
        return "non_trading_requested_date_uses_previous_trading_day"
    if requested_market_reference <= latest_complete_market_date:
        return "requested_market_date_within_complete_range"

    market_now = observed_at.astimezone(US_EQUITY_MARKET_TIMEZONE)
    if close_at is not None and ready_at is not None and requested_as_of == market_now.date():
        if market_now < close_at:
            if session_kind == "PARTIAL_TRADING_DAY":
                return "partial_trading_day_before_early_close"
            return "normal_trading_day_before_regular_close"
        if market_now < ready_at:
            return "data_vendor_delay_window"
    if session_kind == "PARTIAL_TRADING_DAY":
        return "partial_trading_day_after_latest_complete_market_date"
    return "requested_as_of_after_latest_complete_market_date"
