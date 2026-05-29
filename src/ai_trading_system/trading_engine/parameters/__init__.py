"""Observe-only shadow parameter backtest framework."""

from ai_trading_system.trading_engine.parameters.parameter_diff import diff_parameters
from ai_trading_system.trading_engine.parameters.parameter_loader import (
    DEFAULT_PRODUCTION_PARAMETERS_PATH,
    DEFAULT_PROMOTION_RULES_PATH,
    DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    load_production_parameters,
    load_promotion_rules,
    load_shadow_backtest_config,
)
from ai_trading_system.trading_engine.parameters.shadow_backtest import (
    build_shadow_backtest_summary,
    run_shadow_parameter_backtest,
)

__all__ = [
    "DEFAULT_PRODUCTION_PARAMETERS_PATH",
    "DEFAULT_PROMOTION_RULES_PATH",
    "DEFAULT_SHADOW_BACKTEST_CONFIG_PATH",
    "build_shadow_backtest_summary",
    "diff_parameters",
    "load_production_parameters",
    "load_promotion_rules",
    "load_shadow_backtest_config",
    "run_shadow_parameter_backtest",
]
