from __future__ import annotations

from ai_trading_system.config import load_portfolio, load_universe


def test_universe_config_includes_daily_frequency_and_core_watchlist() -> None:
    config = load_universe()

    assert config.market.decision_frequency == "daily"
    assert config.ai_chain["core_watchlist"] == ["MSFT", "GOOG", "TSM", "INTC", "AMD", "NVDA"]


def test_portfolio_config_loads_total_asset_budget() -> None:
    config = load_portfolio()

    assert config.decision.frequency == "daily"
    assert config.portfolio.total_risk_asset_min == 0.60
    assert config.portfolio.total_risk_asset_max == 0.80
