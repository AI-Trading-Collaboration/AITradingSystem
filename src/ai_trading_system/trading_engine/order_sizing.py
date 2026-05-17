from __future__ import annotations

from ai_trading_system.trading_engine.schemas.order_intent import OrderIntent


def resolve_order_quantity(order_intent: OrderIntent) -> int:
    if order_intent.target_quantity is not None:
        return order_intent.target_quantity
    if order_intent.target_notional_usd is None:
        raise ValueError("target_quantity or target_notional_usd is required")
    quantity = int(order_intent.target_notional_usd // order_intent.limit_price)
    if quantity <= 0:
        raise ValueError("target_notional_usd is too small for the limit price")
    return quantity
