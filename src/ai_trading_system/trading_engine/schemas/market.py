from __future__ import annotations

from datetime import UTC, date, datetime

from pydantic import BaseModel, Field, field_validator


class MarketSnapshot(BaseModel):
    symbol: str = Field(min_length=1)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    last: float = Field(gt=0)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.upper()


class MarketContext(BaseModel):
    as_of: date
    prices: dict[str, float] = Field(default_factory=dict)
    daily_trade_counts: dict[str, int] = Field(default_factory=dict)
    event_blocked_symbols: dict[str, str] = Field(default_factory=dict)

    def price_for(self, symbol: str) -> float | None:
        return self.prices.get(symbol.upper())

    def duplicate_count_for(self, key: str) -> int:
        return self.daily_trade_counts.get(key, 0)
