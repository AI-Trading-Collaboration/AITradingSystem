from __future__ import annotations

from dataclasses import dataclass

from ai_trading_system.trading_engine.schemas.broker_order import BrokerOrder
from ai_trading_system.trading_engine.schemas.market import MarketSnapshot
from ai_trading_system.trading_engine.schemas.order_intent import OrderSide


@dataclass(frozen=True)
class SimulatedFill:
    broker_order_id: str
    quantity: int
    price: float
    fees: float
    slippage_bps: float


def simulate_limit_fill(
    *,
    order: BrokerOrder,
    snapshot: MarketSnapshot,
    commission_bps: float,
    slippage_bps: float,
) -> SimulatedFill | None:
    if order.symbol != snapshot.symbol:
        return None
    if order.side == OrderSide.BUY and snapshot.low > order.limit_price:
        return None
    if order.side == OrderSide.SELL and snapshot.high < order.limit_price:
        return None

    raw_price = _raw_fill_price(order, snapshot)
    if order.side == OrderSide.BUY:
        fill_price = min(order.limit_price, raw_price * (1 + slippage_bps / 10000.0))
        realized_slippage = ((fill_price - order.limit_price) / order.limit_price) * 10000.0
    else:
        fill_price = max(order.limit_price, raw_price * (1 - slippage_bps / 10000.0))
        realized_slippage = ((order.limit_price - fill_price) / order.limit_price) * 10000.0
    notional = fill_price * order.quantity
    fees = notional * commission_bps / 10000.0
    return SimulatedFill(
        broker_order_id=order.broker_order_id,
        quantity=order.quantity,
        price=fill_price,
        fees=fees,
        slippage_bps=realized_slippage,
    )


def _raw_fill_price(order: BrokerOrder, snapshot: MarketSnapshot) -> float:
    if order.side == OrderSide.BUY:
        if snapshot.open <= order.limit_price:
            return snapshot.open
        return order.limit_price
    if snapshot.open >= order.limit_price:
        return snapshot.open
    return order.limit_price
