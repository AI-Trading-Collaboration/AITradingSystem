from __future__ import annotations

from ai_trading_system.trading_engine.parameters.parameter_schema import TransactionCostConfig


def calculate_transaction_cost(
    turnover: float,
    cost_config: TransactionCostConfig,
) -> float:
    bps = cost_config.commission_bps + cost_config.slippage_bps + cost_config.fx_cost_bps
    return max(0.0, turnover) * bps / 10_000.0
