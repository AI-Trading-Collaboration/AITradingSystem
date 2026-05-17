from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from ai_trading_system.trading_engine.market_data.market_session_guard import (
    FALLBACK_UNKNOWN_SOURCE,
    MarketSessionStatus,
    evaluate_market_session,
)

CONTRACT_DETAILS_SOURCE = "ibkr_contract_details"


def test_trading_hours_closed_blocks_controlled_fill() -> None:
    result = evaluate_market_session(
        symbol="NVDA",
        as_of=datetime(2026, 5, 18, 10, 0, tzinfo=ZoneInfo("US/Eastern")),
        trading_hours="20260518:CLOSED",
        liquid_hours="20260518:CLOSED",
        exchange_timezone="US/Eastern",
    )

    assert result.market_session_status == MarketSessionStatus.CLOSED
    assert result.can_submit_controlled_fill is False
    assert result.source == CONTRACT_DETAILS_SOURCE
    assert "CLOSED" in result.reason


def test_liquid_hours_regular_session_allows_controlled_fill() -> None:
    result = evaluate_market_session(
        symbol="nvda",
        as_of=datetime(2026, 5, 18, 10, 0, tzinfo=ZoneInfo("US/Eastern")),
        trading_hours="20260518:0400-20260518:2000",
        liquid_hours="20260518:0930-20260518:1600",
        exchange_timezone="US/Eastern",
    )

    assert result.symbol == "NVDA"
    assert result.market_session_status == MarketSessionStatus.REGULAR
    assert result.can_submit_controlled_fill is True
    assert result.trading_hours_source == CONTRACT_DETAILS_SOURCE
    assert result.liquid_hours_source == CONTRACT_DETAILS_SOURCE


def test_trading_hours_without_liquid_hours_is_outside_rth() -> None:
    result = evaluate_market_session(
        symbol="NVDA",
        as_of=datetime(2026, 5, 18, 8, 0, tzinfo=ZoneInfo("US/Eastern")),
        trading_hours="20260518:0400-20260518:2000",
        liquid_hours="20260518:0930-20260518:1600",
        exchange_timezone="US/Eastern",
    )

    assert result.market_session_status == MarketSessionStatus.OUTSIDE_RTH
    assert result.can_submit_controlled_fill is False
    assert "outside liquidHours" in result.reason


def test_missing_contract_details_is_unknown_and_blocked() -> None:
    result = evaluate_market_session(
        symbol="NVDA",
        as_of=datetime(2026, 5, 18, 10, 0, tzinfo=ZoneInfo("US/Eastern")),
        trading_hours=None,
        liquid_hours=None,
        exchange_timezone="US/Eastern",
    )

    assert result.market_session_status == MarketSessionStatus.UNKNOWN
    assert result.can_submit_controlled_fill is False
    assert result.source == FALLBACK_UNKNOWN_SOURCE
    assert result.trading_hours_source == FALLBACK_UNKNOWN_SOURCE
    assert result.liquid_hours_source == FALLBACK_UNKNOWN_SOURCE
