from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field

from ai_trading_system.trading_engine.schemas.broker_order import OrderStatus


class ExecutionMode(StrEnum):
    PAPER = "PAPER"


class ExecutionReport(BaseModel):
    intent_id: str = Field(min_length=1)
    broker_order_id: str | None = None
    status: OrderStatus
    submitted_at: datetime | None = None
    completed_at: datetime | None = None
    requested_quantity: int = Field(ge=0)
    filled_quantity: int = Field(default=0, ge=0)
    avg_fill_price: float | None = Field(default=None, gt=0)
    fees: float = Field(default=0.0, ge=0)
    slippage_bps: float = 0.0
    execution_mode: ExecutionMode = ExecutionMode.PAPER
    rejection_reason: str | None = None

    @classmethod
    def rejected(
        cls,
        *,
        intent_id: str,
        requested_quantity: int = 0,
        rejection_reason: str,
    ) -> ExecutionReport:
        now = datetime.now(UTC)
        return cls(
            intent_id=intent_id,
            status=OrderStatus.REJECTED,
            submitted_at=now,
            completed_at=now,
            requested_quantity=requested_quantity,
            rejection_reason=rejection_reason,
        )
