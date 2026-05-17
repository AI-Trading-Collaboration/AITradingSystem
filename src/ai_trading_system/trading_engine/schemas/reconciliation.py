from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, Field

from ai_trading_system.trading_engine.schemas.portfolio_state import PortfolioState
from ai_trading_system.trading_engine.schemas.risk_result import RiskSeverity


class ReconciliationIssue(BaseModel):
    field: str = Field(min_length=1)
    severity: RiskSeverity = RiskSeverity.BLOCK
    symbol: str | None = None
    expected: float | int | str | None = None
    actual: float | int | str | None = None
    message: str = Field(min_length=1)


class ReconciliationResult(BaseModel):
    schema_version: str = "1.0"
    checked_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    status: RiskSeverity
    production_effect: Literal["none"] = "none"
    source: str = Field(min_length=1)
    initial_cash_usd: float = Field(gt=0)
    tolerance_cash_usd: float = Field(default=0.01, ge=0)
    tolerance_avg_cost_usd: float = Field(default=0.0001, ge=0)
    expected_portfolio: PortfolioState
    actual_portfolio: PortfolioState
    issues: list[ReconciliationIssue] = Field(default_factory=list)
