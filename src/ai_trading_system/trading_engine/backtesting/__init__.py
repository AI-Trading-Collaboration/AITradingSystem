"""Small backtesting helpers for observe-only trading-engine reports."""

from typing import Any

__all__ = [
    "PortfolioMetrics",
    "PortfolioSimulationResult",
    "WalkForwardWindow",
    "calculate_portfolio_metrics",
    "calculate_transaction_cost",
    "generate_walk_forward_windows",
    "simulate_parameter_portfolio",
]


def __getattr__(name: str) -> Any:
    if name in {"PortfolioMetrics", "calculate_portfolio_metrics"}:
        from ai_trading_system.trading_engine.backtesting.metrics import (
            PortfolioMetrics,
            calculate_portfolio_metrics,
        )

        return {
            "PortfolioMetrics": PortfolioMetrics,
            "calculate_portfolio_metrics": calculate_portfolio_metrics,
        }[name]
    if name in {"PortfolioSimulationResult", "simulate_parameter_portfolio"}:
        from ai_trading_system.trading_engine.backtesting.portfolio_simulator import (
            PortfolioSimulationResult,
            simulate_parameter_portfolio,
        )

        return {
            "PortfolioSimulationResult": PortfolioSimulationResult,
            "simulate_parameter_portfolio": simulate_parameter_portfolio,
        }[name]
    if name == "calculate_transaction_cost":
        from ai_trading_system.trading_engine.backtesting.transaction_cost import (
            calculate_transaction_cost,
        )

        return calculate_transaction_cost
    if name in {"WalkForwardWindow", "generate_walk_forward_windows"}:
        from ai_trading_system.trading_engine.backtesting.walk_forward import (
            WalkForwardWindow,
            generate_walk_forward_windows,
        )

        return {
            "WalkForwardWindow": WalkForwardWindow,
            "generate_walk_forward_windows": generate_walk_forward_windows,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
