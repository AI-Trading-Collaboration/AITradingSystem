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
from ai_trading_system.trading_engine.schemas.order_intent_candidate import (
    OrderIntentCandidate,
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


def build_order_intent_from_candidate(
    candidate: OrderIntentCandidate,
    *,
    mode: str = "paper",
) -> OrderIntent:
    if mode != "paper":
        raise RuntimeError("OrderIntentCandidate conversion is allowed only in paper mode")
    if candidate.mode != "paper":
        raise RuntimeError("OrderIntentCandidate mode must be paper")
    if candidate.blocked:
        blockers = ", ".join(candidate.blocked_by) or "blocked"
        raise RuntimeError(f"blocked OrderIntentCandidate cannot convert: {blockers}")
    missing = candidate.missing_order_intent_fields()
    if missing:
        raise ValueError(
            "unblocked OrderIntentCandidate is missing required fields: "
            + ", ".join(missing)
        )
    if candidate.symbol is None or candidate.asset_type is None or candidate.side is None:
        raise ValueError("OrderIntentCandidate is missing symbol, asset_type or side")
    if candidate.limit_price is None:
        raise ValueError("OrderIntentCandidate is missing limit_price")
    metadata = dict(candidate.metadata)
    metadata.setdefault("source_candidate_id", candidate.candidate_id)
    metadata.setdefault("candidate_mode", candidate.mode)
    metadata.setdefault("production_effect", candidate.production_effect)
    return OrderIntent(
        created_at=candidate.created_at,
        strategy_id=candidate.strategy_id,
        strategy_version=candidate.strategy_version,
        run_id=candidate.run_id,
        symbol=candidate.symbol,
        asset_type=candidate.asset_type,
        side=candidate.side,
        order_type=candidate.order_type,
        time_in_force=candidate.time_in_force,
        target_quantity=candidate.target_quantity,
        target_notional_usd=candidate.target_notional_usd,
        limit_price=candidate.limit_price,
        confidence=candidate.confidence,
        score_snapshot_id=candidate.score_snapshot_id,
        reason_codes=candidate.reason_codes,
        risk_constraints=candidate.risk_constraints,
        metadata=metadata,
    )
