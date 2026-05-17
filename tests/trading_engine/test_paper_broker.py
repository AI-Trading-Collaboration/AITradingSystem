from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ai_trading_system.trading_engine.execution import PaperBroker
from ai_trading_system.trading_engine.portfolio import PaperPortfolio
from ai_trading_system.trading_engine.schemas import (
    AssetType,
    MarketSnapshot,
    OrderIntent,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)


def test_paper_broker_submits_limit_buy_order() -> None:
    broker = PaperBroker(portfolio=PaperPortfolio(100000.0))

    order = broker.submit_order(_intent(target_quantity=5, target_notional_usd=None))

    assert order.status == OrderStatus.SUBMITTED
    assert order.quantity == 5
    assert broker.list_open_orders() == [order]


def test_paper_broker_fills_buy_when_limit_touched() -> None:
    broker = PaperBroker(portfolio=PaperPortfolio(100000.0))
    order = broker.submit_order(_intent(target_quantity=5, target_notional_usd=None))

    reports = broker.process_market_snapshot(_snapshot(low=99.0, high=101.0, open=99.5))

    assert len(reports) == 1
    assert reports[0].status == OrderStatus.FILLED
    assert broker.get_order(order.broker_order_id).status == OrderStatus.FILLED
    state = broker.get_portfolio_state(prices={"TSM": 101.0})
    position = state.position_for("TSM")
    assert position is not None
    assert position.quantity == 5
    assert state.cash_usd == pytest.approx(99502.5)


def test_paper_broker_keeps_order_open_when_limit_not_touched() -> None:
    broker = PaperBroker(portfolio=PaperPortfolio(100000.0))
    order = broker.submit_order(_intent(target_quantity=5, target_notional_usd=None))

    reports = broker.process_market_snapshot(_snapshot(low=101.0, high=102.0, open=101.5))

    assert reports == []
    assert broker.get_order(order.broker_order_id).status == OrderStatus.SUBMITTED


def test_paper_broker_sell_reduces_position_after_fill() -> None:
    portfolio = PaperPortfolio(100000.0)
    portfolio.apply_fill(symbol="TSM", side=OrderSide.BUY, quantity=5, price=100.0, fees=0.0)
    broker = PaperBroker(portfolio=portfolio)
    order = broker.submit_order(
        _intent(side=OrderSide.SELL, target_quantity=2, target_notional_usd=None, limit_price=110.0)
    )

    reports = broker.process_market_snapshot(_snapshot(low=108.0, high=111.0, open=109.0))

    assert reports[0].broker_order_id == order.broker_order_id
    state = broker.get_portfolio_state(prices={"TSM": 110.0})
    position = state.position_for("TSM")
    assert position is not None
    assert position.quantity == 3


def test_paper_broker_cancels_open_order_and_rejects_cancel_after_fill() -> None:
    broker = PaperBroker(portfolio=PaperPortfolio(100000.0))
    open_order = broker.submit_order(_intent(target_quantity=5, target_notional_usd=None))

    cancelled = broker.cancel_order(open_order.broker_order_id)

    assert cancelled.status == OrderStatus.CANCELLED
    filled_order = broker.submit_order(_intent(target_quantity=1, target_notional_usd=None))
    broker.process_market_snapshot(_snapshot(low=99.0, high=101.0, open=100.0))
    with pytest.raises(ValueError):
        broker.cancel_order(filled_order.broker_order_id)


def _intent(**overrides: object) -> OrderIntent:
    values = {
        "strategy_id": "broker_test_strategy",
        "strategy_version": "v1",
        "run_id": "run_2026_05_17",
        "symbol": "TSM",
        "asset_type": AssetType.STOCK,
        "side": OrderSide.BUY,
        "order_type": OrderType.LIMIT,
        "time_in_force": TimeInForce.DAY,
        "target_notional_usd": 1000.0,
        "limit_price": 100.0,
        "confidence": 0.75,
        "score_snapshot_id": "score_snapshot_1",
    }
    values.update(overrides)
    return OrderIntent.model_validate(values)


def _snapshot(
    *,
    low: float,
    high: float,
    open: float,
    last: float = 101.0,
) -> MarketSnapshot:
    return MarketSnapshot(
        symbol="TSM",
        timestamp=datetime(2026, 5, 17, 20, 0, tzinfo=UTC),
        open=open,
        high=high,
        low=low,
        last=last,
    )
