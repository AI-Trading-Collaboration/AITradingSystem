from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

IBKR_CONTRACT_DETAILS_SOURCE = "ibkr_contract_details"
FALLBACK_UNKNOWN_SOURCE = "fallback_unknown"


class MarketSessionStatus(StrEnum):
    REGULAR = "REGULAR"
    OUTSIDE_RTH = "OUTSIDE_RTH"
    CLOSED = "CLOSED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class MarketSessionGuardResult:
    symbol: str
    checked_at: str
    market_session_status: MarketSessionStatus
    can_submit_controlled_fill: bool
    reason: str
    source: str
    exchange_timezone: str
    trading_hours_source: str
    liquid_hours_source: str

    def as_payload(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "checked_at": self.checked_at,
            "market_session_status": self.market_session_status.value,
            "can_submit_controlled_fill": self.can_submit_controlled_fill,
            "reason": self.reason,
            "source": self.source,
            "exchange_timezone": self.exchange_timezone,
            "trading_hours_source": self.trading_hours_source,
            "liquid_hours_source": self.liquid_hours_source,
        }


@dataclass(frozen=True)
class _SessionInterval:
    start: datetime
    end: datetime

    def contains(self, value: datetime) -> bool:
        return self.start <= value < self.end

    def covers_date(self, value: datetime) -> bool:
        return self.start.date() <= value.date() <= self.end.date()


@dataclass(frozen=True)
class _ParsedHours:
    intervals: tuple[_SessionInterval, ...]
    closed_dates: frozenset[Any]
    segment_dates: frozenset[Any]
    error: str | None = None

    def contains(self, value: datetime) -> bool:
        return any(interval.contains(value) for interval in self.intervals)

    def has_segment_for(self, value: datetime) -> bool:
        value_date = value.date()
        return (
            value_date in self.closed_dates
            or value_date in self.segment_dates
            or any(interval.covers_date(value) for interval in self.intervals)
        )


def evaluate_market_session(
    *,
    symbol: str,
    as_of: datetime,
    trading_hours: str | None,
    liquid_hours: str | None,
    exchange_timezone: str | None,
) -> MarketSessionGuardResult:
    normalized_symbol = symbol.strip().upper()
    trading_source = (
        IBKR_CONTRACT_DETAILS_SOURCE if _has_text(trading_hours) else FALLBACK_UNKNOWN_SOURCE
    )
    liquid_source = (
        IBKR_CONTRACT_DETAILS_SOURCE if _has_text(liquid_hours) else FALLBACK_UNKNOWN_SOURCE
    )
    timezone_name = _normalized_timezone(exchange_timezone)
    if timezone_name is None:
        return _result(
            symbol=normalized_symbol,
            as_of=as_of,
            status=MarketSessionStatus.UNKNOWN,
            can_submit=False,
            reason="exchange timezone is missing",
            source=FALLBACK_UNKNOWN_SOURCE,
            exchange_timezone="unknown",
            trading_hours_source=trading_source,
            liquid_hours_source=liquid_source,
        )
    try:
        exchange_tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return _result(
            symbol=normalized_symbol,
            as_of=as_of,
            status=MarketSessionStatus.UNKNOWN,
            can_submit=False,
            reason=f"exchange timezone is not available: {timezone_name}",
            source=FALLBACK_UNKNOWN_SOURCE,
            exchange_timezone=timezone_name,
            trading_hours_source=trading_source,
            liquid_hours_source=liquid_source,
        )

    local_as_of = _as_exchange_time(as_of, exchange_tz)
    if not _has_text(trading_hours):
        return _result(
            symbol=normalized_symbol,
            as_of=local_as_of,
            status=MarketSessionStatus.UNKNOWN,
            can_submit=False,
            reason="IBKR contract details tradingHours is missing",
            source=FALLBACK_UNKNOWN_SOURCE,
            exchange_timezone=timezone_name,
            trading_hours_source=trading_source,
            liquid_hours_source=liquid_source,
        )

    trading = _parse_ibkr_hours(str(trading_hours), exchange_tz)
    if trading.error is not None:
        return _result(
            symbol=normalized_symbol,
            as_of=local_as_of,
            status=MarketSessionStatus.UNKNOWN,
            can_submit=False,
            reason=f"IBKR tradingHours could not be parsed: {trading.error}",
            source=IBKR_CONTRACT_DETAILS_SOURCE,
            exchange_timezone=timezone_name,
            trading_hours_source=trading_source,
            liquid_hours_source=liquid_source,
        )

    if local_as_of.date() in trading.closed_dates:
        return _result(
            symbol=normalized_symbol,
            as_of=local_as_of,
            status=MarketSessionStatus.CLOSED,
            can_submit=False,
            reason="IBKR tradingHours reports CLOSED for the exchange date",
            source=IBKR_CONTRACT_DETAILS_SOURCE,
            exchange_timezone=timezone_name,
            trading_hours_source=trading_source,
            liquid_hours_source=liquid_source,
        )

    if not _has_text(liquid_hours):
        if trading.contains(local_as_of):
            reason = "IBKR liquidHours is missing while tradingHours is open"
        elif trading.has_segment_for(local_as_of):
            reason = "outside IBKR tradingHours and liquidHours is missing"
        else:
            reason = "IBKR tradingHours has no segment for the exchange date"
        return _result(
            symbol=normalized_symbol,
            as_of=local_as_of,
            status=MarketSessionStatus.UNKNOWN,
            can_submit=False,
            reason=reason,
            source=FALLBACK_UNKNOWN_SOURCE,
            exchange_timezone=timezone_name,
            trading_hours_source=trading_source,
            liquid_hours_source=liquid_source,
        )

    liquid = _parse_ibkr_hours(str(liquid_hours), exchange_tz)
    if liquid.error is not None:
        return _result(
            symbol=normalized_symbol,
            as_of=local_as_of,
            status=MarketSessionStatus.UNKNOWN,
            can_submit=False,
            reason=f"IBKR liquidHours could not be parsed: {liquid.error}",
            source=IBKR_CONTRACT_DETAILS_SOURCE,
            exchange_timezone=timezone_name,
            trading_hours_source=trading_source,
            liquid_hours_source=liquid_source,
        )

    if liquid.contains(local_as_of):
        return _result(
            symbol=normalized_symbol,
            as_of=local_as_of,
            status=MarketSessionStatus.REGULAR,
            can_submit=True,
            reason="as_of is inside IBKR liquidHours regular trading session",
            source=IBKR_CONTRACT_DETAILS_SOURCE,
            exchange_timezone=timezone_name,
            trading_hours_source=trading_source,
            liquid_hours_source=liquid_source,
        )

    if trading.contains(local_as_of):
        return _result(
            symbol=normalized_symbol,
            as_of=local_as_of,
            status=MarketSessionStatus.OUTSIDE_RTH,
            can_submit=False,
            reason="as_of is inside IBKR tradingHours but outside liquidHours regular session",
            source=IBKR_CONTRACT_DETAILS_SOURCE,
            exchange_timezone=timezone_name,
            trading_hours_source=trading_source,
            liquid_hours_source=liquid_source,
        )

    if trading.has_segment_for(local_as_of):
        return _result(
            symbol=normalized_symbol,
            as_of=local_as_of,
            status=MarketSessionStatus.CLOSED,
            can_submit=False,
            reason="as_of is outside IBKR tradingHours for the exchange date",
            source=IBKR_CONTRACT_DETAILS_SOURCE,
            exchange_timezone=timezone_name,
            trading_hours_source=trading_source,
            liquid_hours_source=liquid_source,
        )

    return _result(
        symbol=normalized_symbol,
        as_of=local_as_of,
        status=MarketSessionStatus.UNKNOWN,
        can_submit=False,
        reason="IBKR hours do not include the exchange date",
        source=FALLBACK_UNKNOWN_SOURCE,
        exchange_timezone=timezone_name,
        trading_hours_source=trading_source,
        liquid_hours_source=liquid_source,
    )


def _result(
    *,
    symbol: str,
    as_of: datetime,
    status: MarketSessionStatus,
    can_submit: bool,
    reason: str,
    source: str,
    exchange_timezone: str,
    trading_hours_source: str,
    liquid_hours_source: str,
) -> MarketSessionGuardResult:
    return MarketSessionGuardResult(
        symbol=symbol,
        checked_at=as_of.isoformat(),
        market_session_status=status,
        can_submit_controlled_fill=can_submit,
        reason=reason,
        source=source,
        exchange_timezone=exchange_timezone,
        trading_hours_source=trading_hours_source,
        liquid_hours_source=liquid_hours_source,
    )


def _parse_ibkr_hours(raw_hours: str, exchange_tz: ZoneInfo) -> _ParsedHours:
    intervals: list[_SessionInterval] = []
    closed_dates: set[Any] = set()
    segment_dates: set[Any] = set()
    for raw_segment in raw_hours.split(";"):
        segment = raw_segment.strip()
        if not segment:
            continue
        if ":" not in segment:
            return _ParsedHours(
                tuple(intervals),
                frozenset(closed_dates),
                frozenset(segment_dates),
                f"segment is missing date separator: {segment}",
            )
        raw_date, raw_windows = segment.split(":", 1)
        try:
            segment_date = datetime.strptime(raw_date, "%Y%m%d").date()
        except ValueError as exc:
            return _ParsedHours(
                tuple(intervals),
                frozenset(closed_dates),
                frozenset(segment_dates),
                f"invalid date {raw_date}: {exc}",
            )
        segment_dates.add(segment_date)
        windows = raw_windows.strip()
        if windows.upper() == "CLOSED":
            closed_dates.add(segment_date)
            continue
        for raw_window in windows.split(","):
            window = raw_window.strip()
            if not window:
                continue
            if "-" not in window:
                return _ParsedHours(
                    tuple(intervals),
                    frozenset(closed_dates),
                    frozenset(segment_dates),
                    f"window is missing range separator: {window}",
                )
            raw_start, raw_end = window.split("-", 1)
            start = _parse_endpoint(raw_start, default_date=segment_date, exchange_tz=exchange_tz)
            end = _parse_endpoint(raw_end, default_date=segment_date, exchange_tz=exchange_tz)
            if start is None or end is None:
                return _ParsedHours(
                    tuple(intervals),
                    frozenset(closed_dates),
                    frozenset(segment_dates),
                    f"invalid time window: {window}",
                )
            if end <= start:
                end = end + timedelta(days=1)
            intervals.append(_SessionInterval(start=start, end=end))
    if not intervals and not closed_dates:
        return _ParsedHours(
            tuple(),
            frozenset(),
            frozenset(segment_dates),
            "no usable open or CLOSED segments",
        )
    return _ParsedHours(tuple(intervals), frozenset(closed_dates), frozenset(segment_dates))


def _parse_endpoint(
    token: str,
    *,
    default_date: Any,
    exchange_tz: ZoneInfo,
) -> datetime | None:
    value = token.strip()
    if ":" in value:
        raw_date, raw_time = value.split(":", 1)
        try:
            endpoint_date = datetime.strptime(raw_date, "%Y%m%d").date()
        except ValueError:
            return None
    else:
        endpoint_date = default_date
        raw_time = value
    parsed_time = _parse_hhmm(raw_time)
    if parsed_time is None:
        return None
    if raw_time == "2400":
        endpoint_date = endpoint_date + timedelta(days=1)
    return datetime.combine(endpoint_date, parsed_time, tzinfo=exchange_tz)


def _parse_hhmm(value: str) -> time | None:
    raw = value.strip()
    if raw == "2400":
        return time(0, 0)
    if len(raw) != 4 or not raw.isdigit():
        return None
    hour = int(raw[:2])
    minute = int(raw[2:])
    if hour > 23 or minute > 59:
        return None
    return time(hour, minute)


def _as_exchange_time(value: datetime, exchange_tz: ZoneInfo) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=exchange_tz)
    return value.astimezone(exchange_tz)


def _normalized_timezone(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _has_text(value: str | None) -> bool:
    return value is not None and bool(value.strip())
