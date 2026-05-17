from __future__ import annotations

from pydantic import BaseModel, Field


class BrokerPosition(BaseModel):
    symbol: str = Field(min_length=1)
    quantity: int
    avg_cost: float = Field(ge=0)
    market_price: float = Field(ge=0)
    market_value: float
    unrealized_pnl: float
