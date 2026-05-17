from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field

from ai_trading_system.trading_engine.schemas.broker_position import BrokerPosition


class PortfolioState(BaseModel):
    as_of: datetime = Field(default_factory=lambda: datetime.now(UTC))
    cash_usd: float
    equity_value_usd: float
    gross_exposure_usd: float
    net_exposure_usd: float
    realized_pnl_usd: float = 0.0
    positions: list[BrokerPosition] = Field(default_factory=list)

    def position_for(self, symbol: str) -> BrokerPosition | None:
        normalized = symbol.upper()
        for position in self.positions:
            if position.symbol.upper() == normalized:
                return position
        return None
