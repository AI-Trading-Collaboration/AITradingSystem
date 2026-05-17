from __future__ import annotations

from ai_trading_system.trading_engine.schemas.broker_order import OrderStatus

ALLOWED_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.CREATED: frozenset({OrderStatus.RISK_CHECKED, OrderStatus.REJECTED}),
    OrderStatus.RISK_CHECKED: frozenset({OrderStatus.SUBMITTED, OrderStatus.REJECTED}),
    OrderStatus.SUBMITTED: frozenset(
        {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.EXPIRED,
            OrderStatus.REJECTED,
        }
    ),
    OrderStatus.PARTIALLY_FILLED: frozenset(
        {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.EXPIRED}
    ),
    OrderStatus.FILLED: frozenset(),
    OrderStatus.CANCELLED: frozenset(),
    OrderStatus.REJECTED: frozenset(),
    OrderStatus.EXPIRED: frozenset(),
}


def can_transition(current_status: OrderStatus, next_status: OrderStatus) -> bool:
    return next_status in ALLOWED_TRANSITIONS[current_status]


def assert_transition(current_status: OrderStatus, next_status: OrderStatus) -> None:
    if not can_transition(current_status, next_status):
        raise ValueError(f"invalid order transition: {current_status} -> {next_status}")
