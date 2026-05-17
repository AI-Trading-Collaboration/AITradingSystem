from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

from ai_trading_system.trading_engine.schemas.order_intent import (
    AssetType,
    OrderIntent,
    OrderSide,
    OrderType,
    TimeInForce,
)


class DecisionCandidate(BaseModel):
    """Neutral input boundary for trend systems that want to propose a trade."""

    created_at: datetime | None = None
    strategy_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    asset_type: AssetType
    side: OrderSide
    target_notional_usd: float = Field(gt=0)
    limit_price: float = Field(gt=0)
    confidence: float = Field(ge=0, le=1)
    score_snapshot_id: str = Field(min_length=1)
    reason_codes: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


def build_order_intent(candidate: DecisionCandidate) -> OrderIntent:
    return OrderIntent(
        created_at=candidate.created_at or datetime.now(UTC),
        strategy_id=candidate.strategy_id,
        strategy_version=candidate.strategy_version,
        run_id=candidate.run_id,
        symbol=candidate.symbol,
        asset_type=candidate.asset_type,
        side=candidate.side,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.DAY,
        target_notional_usd=candidate.target_notional_usd,
        limit_price=candidate.limit_price,
        confidence=candidate.confidence,
        score_snapshot_id=candidate.score_snapshot_id,
        reason_codes=candidate.reason_codes,
        metadata=candidate.metadata,
    )
