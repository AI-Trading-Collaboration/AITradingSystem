from __future__ import annotations

from ai_trading_system.config import (
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_features,
    load_portfolio,
    load_scoring_rules,
    load_universe,
)


def test_universe_config_includes_daily_frequency_and_core_watchlist() -> None:
    config = load_universe()

    assert config.market.decision_frequency == "daily"
    assert config.ai_chain["core_watchlist"] == ["MSFT", "GOOG", "TSM", "INTC", "AMD", "NVDA"]


def test_portfolio_config_loads_total_asset_budget() -> None:
    config = load_portfolio()

    assert config.decision.frequency == "daily"
    assert config.portfolio.total_risk_asset_min == 0.60
    assert config.portfolio.total_risk_asset_max == 0.80


def test_data_quality_config_loads_thresholds() -> None:
    config = load_data_quality()

    assert config.prices.max_stale_calendar_days == 7
    assert config.prices.suspicious_daily_return_abs == 0.20
    assert config.prices.ticker_return_threshold_overrides["^VIX"].extreme_daily_return_abs == 2.00
    assert config.rates.min_plausible_value == -1.0
    assert config.rates.max_plausible_value == 25.0


def test_feature_config_loads_market_feature_windows() -> None:
    config = load_features()

    assert config.moving_average_windows == [20, 50, 100, 200]
    assert config.return_windows == [1, 5, 20]
    assert config.vix.ticker == "^VIX"
    assert config.core_breadth.long_moving_average_window == 200


def test_scoring_rules_config_loads_weights_and_placeholders() -> None:
    config = load_scoring_rules()

    assert config.weights["trend"] == 25
    assert config.minimum_signal_coverage == 0.50
    assert config.trend.signals[0].subject == "SPY"
    assert config.placeholders["valuation"].score == 50
    assert "MVP 阶段占位" in config.placeholders["valuation"].reason


def test_configured_price_tickers_defaults_to_core_universe() -> None:
    config = load_universe()

    tickers = configured_price_tickers(config)

    assert tickers == [
        "SPY",
        "QQQ",
        "SMH",
        "SOXX",
        "TLT",
        "SHY",
        "^VIX",
        "DX-Y.NYB",
        "MSFT",
        "GOOG",
        "TSM",
        "INTC",
        "AMD",
        "NVDA",
    ]


def test_configured_price_tickers_can_include_full_ai_chain_without_duplicates() -> None:
    config = load_universe()

    tickers = configured_price_tickers(config, include_full_ai_chain=True)

    assert tickers.count("NVDA") == 1
    assert tickers.count("MSFT") == 1
    assert "ASML" in tickers
    assert "AMZN" in tickers


def test_configured_rate_series() -> None:
    config = load_universe()

    assert configured_rate_series(config) == ["DGS2", "DGS10"]
