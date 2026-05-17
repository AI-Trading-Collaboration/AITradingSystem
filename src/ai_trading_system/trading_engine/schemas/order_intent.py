from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Self
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class AssetType(StrEnum):
    STOCK = "stock"
    ETF = "etf"


class OrderType(StrEnum):
    LIMIT = "LIMIT"


class TimeInForce(StrEnum):
    DAY = "DAY"


class RiskConstraints(BaseModel):
    max_position_pct: float | None = Field(default=None, gt=0, le=1)
    max_order_notional_pct: float | None = Field(default=None, gt=0, le=1)
    no_trade_before_earnings_hours: int | None = Field(default=None, ge=0)


class OrderIntent(BaseModel):
    schema_version: str = Field(default="1.0", min_length=1)
    intent_id: str = Field(default_factory=lambda: str(uuid4()), min_length=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    strategy_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1, pattern=r"^[A-Za-z0-9.^-]+$")
    asset_type: AssetType
    side: OrderSide
    order_type: OrderType
    time_in_force: TimeInForce
    target_quantity: int | None = Field(default=None, gt=0)
    target_notional_usd: float | None = Field(default=None, gt=0)
    limit_price: float = Field(gt=0)
    confidence: float = Field(ge=0, le=1)
    score_snapshot_id: str = Field(min_length=1)
    reason_codes: list[str] = Field(default_factory=list)
    risk_constraints: RiskConstraints = Field(default_factory=RiskConstraints)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.upper()

    @field_validator("reason_codes")
    @classmethod
    def validate_reason_codes(cls, value: list[str]) -> list[str]:
        empty_codes = [code for code in value if not code.strip()]
        if empty_codes:
            raise ValueError("reason_codes must not contain empty values")
        return value

    @model_validator(mode="after")
    def validate_target(self) -> Self:
        if self.target_quantity is None and self.target_notional_usd is None:
            raise ValueError("target_quantity or target_notional_usd is required")
        return self

    @property
    def requested_notional_usd(self) -> float:
        if self.target_notional_usd is not None:
            return self.target_notional_usd
        if self.target_quantity is None:
            raise ValueError("target quantity is required to derive notional")
        return float(self.target_quantity) * self.limit_price

    @property
    def duplicate_key(self) -> str:
        return f"{self.strategy_id}|{self.symbol}|{self.side.value}"
