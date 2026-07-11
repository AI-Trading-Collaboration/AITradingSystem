from ai_trading_system.interfaces.cli.etf_portfolio import data as data_commands
from ai_trading_system.interfaces.cli.etf_portfolio import data_quality as data_quality_commands
from ai_trading_system.interfaces.cli.etf_portfolio import operations as operations_commands
from ai_trading_system.interfaces.cli.etf_portfolio import reporting as reporting_commands
from ai_trading_system.interfaces.cli.etf_portfolio import weekly_review as weekly_review_commands
from ai_trading_system.interfaces.cli.etf_portfolio.registration import etf_app

__all__ = [
    "data_commands",
    "data_quality_commands",
    "etf_app",
    "operations_commands",
    "reporting_commands",
    "weekly_review_commands",
]
