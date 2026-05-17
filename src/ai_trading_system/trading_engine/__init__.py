"""Paper-only trading execution subsystem.

The trading engine is intentionally isolated from the trend scoring system. It
accepts standard OrderIntent objects, applies pre-trade risk checks, and routes
approved orders to the paper broker only.
"""

from ai_trading_system.trading_engine.execution.execution_service import ExecutionService
from ai_trading_system.trading_engine.execution.paper_broker import PaperBroker
from ai_trading_system.trading_engine.portfolio.paper_portfolio import PaperPortfolio
from ai_trading_system.trading_engine.risk.pre_trade_checker import PreTradeRiskChecker
from ai_trading_system.trading_engine.schemas import (
    BrokerOrder,
    BrokerPosition,
    ExecutionReport,
    MarketContext,
    MarketSnapshot,
    OrderIntent,
    PortfolioState,
    RiskCheckResult,
)

__all__ = [
    "BrokerOrder",
    "BrokerPosition",
    "ExecutionReport",
    "ExecutionService",
    "MarketContext",
    "MarketSnapshot",
    "OrderIntent",
    "PaperBroker",
    "PaperPortfolio",
    "PortfolioState",
    "PreTradeRiskChecker",
    "RiskCheckResult",
]
