from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field

from ai_trading_system.trading_engine.schemas.order_intent import OrderSide, OrderType


class OrderStatus(StrEnum):
    CREATED = "CREATED"
    RISK_CHECKED = "RISK_CHECKED"
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class BrokerOrder(BaseModel):
    broker_order_id: str = Field(min_length=1)
    client_order_id: str = Field(min_length=1)
    intent_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    side: OrderSide
    order_type: OrderType
    limit_price: float = Field(gt=0)
    quantity: int = Field(gt=0)
    status: OrderStatus
    submitted_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None
    filled_quantity: int = Field(default=0, ge=0)
    avg_fill_price: float | None = Field(default=None, gt=0)
