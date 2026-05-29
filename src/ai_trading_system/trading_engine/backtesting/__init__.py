"""Small backtesting helpers for observe-only trading-engine reports."""

from ai_trading_system.trading_engine.backtesting.metrics import (
    PortfolioMetrics,
    calculate_portfolio_metrics,
)
from ai_trading_system.trading_engine.backtesting.portfolio_simulator import (
    PortfolioSimulationResult,
    simulate_parameter_portfolio,
)
from ai_trading_system.trading_engine.backtesting.transaction_cost import (
    calculate_transaction_cost,
)
from ai_trading_system.trading_engine.backtesting.walk_forward import (
    WalkForwardWindow,
    generate_walk_forward_windows,
)

__all__ = [
    "PortfolioMetrics",
    "PortfolioSimulationResult",
    "WalkForwardWindow",
    "calculate_portfolio_metrics",
    "calculate_transaction_cost",
    "generate_walk_forward_windows",
    "simulate_parameter_portfolio",
]
