from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from ai_trading_system.trading_engine.schemas.order_intent import (
    AssetType,
    OrderSide,
    OrderType,
    RiskConstraints,
    TimeInForce,
)


class OrderIntentCandidate(BaseModel):
    schema_version: str = "1.0"
    candidate_id: str = Field(min_length=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    strategy_id: str = Field(default="daily_decision_bus", min_length=1)
    strategy_version: str = Field(default="candidate_schema_v1", min_length=1)
    run_id: str = Field(min_length=1)
    symbol: str | None = Field(default=None, pattern=r"^[A-Za-z0-9.^-]+$")
    asset_type: AssetType | None = None
    side: OrderSide | None = None
    order_type: OrderType = OrderType.LIMIT
    time_in_force: TimeInForce = TimeInForce.DAY
    target_quantity: int | None = Field(default=None, gt=0)
    target_notional_usd: float | None = Field(default=None, gt=0)
    limit_price: float | None = Field(default=None, gt=0)
    confidence: float = Field(default=0.0, ge=0, le=1)
    score_snapshot_id: str = Field(default="missing", min_length=1)
    reason_codes: list[str] = Field(default_factory=list)
    risk_constraints: RiskConstraints = Field(default_factory=RiskConstraints)
    mode: Literal["paper", "real"] = "paper"
    blocked: bool = True
    blocked_by: list[str] = Field(default_factory=list)
    production_effect: Literal["none"] = "none"
    source_refs: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return value.upper()

    @field_validator("reason_codes", "blocked_by")
    @classmethod
    def validate_non_empty_codes(cls, value: list[str]) -> list[str]:
        empty_codes = [code for code in value if not code.strip()]
        if empty_codes:
            raise ValueError("code lists must not contain empty values")
        return value

    def missing_order_intent_fields(self) -> tuple[str, ...]:
        missing: list[str] = []
        if self.symbol is None:
            missing.append("symbol")
        if self.asset_type is None:
            missing.append("asset_type")
        if self.side is None:
            missing.append("side")
        if self.limit_price is None:
            missing.append("limit_price")
        if self.target_quantity is None and self.target_notional_usd is None:
            missing.append("target_quantity_or_target_notional_usd")
        return tuple(missing)
