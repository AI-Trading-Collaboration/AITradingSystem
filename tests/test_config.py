from __future__ import annotations

from ai_trading_system.config import (
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_data_sources,
    load_features,
    load_fundamental_metrics,
    load_industry_chain,
    load_market_regimes,
    load_portfolio,
    load_risk_events,
    load_scoring_rules,
    load_sec_companies,
    load_universe,
    load_watchlist,
    market_regime_by_id,
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


def test_data_sources_config_loads_current_and_planned_sources() -> None:
    config = load_data_sources()
    source_ids = {source.source_id for source in config.sources}

    assert "yahoo_finance_daily_prices" in source_ids
    assert "fred_daily_rates" in source_ids
    assert "sec_company_facts" in source_ids
    assert any(source.status == "planned" for source in config.sources)


def test_sec_companies_config_covers_core_watchlist() -> None:
    config = load_sec_companies()
    by_ticker = {company.ticker: company for company in config.companies}

    assert {"MSFT", "GOOG", "TSM", "INTC", "AMD", "NVDA"}.issubset(by_ticker)
    assert by_ticker["NVDA"].cik == "0001045810"
    assert by_ticker["TSM"].expected_taxonomies == ["ifrs-full", "dei"]
    assert by_ticker["TSM"].sec_metric_periods == ["annual"]


def test_fundamental_metrics_config_loads_sec_metric_mappings() -> None:
    config = load_fundamental_metrics()
    metric_ids = {metric.metric_id for metric in config.metrics}

    assert {"revenue", "gross_profit", "operating_income", "net_income"}.issubset(metric_ids)
    assert any(
        concept.taxonomy == "us-gaap" and concept.concept == "Revenues"
        for metric in config.metrics
        for concept in metric.concepts
    )
    assert any(
        concept.taxonomy == "ifrs-full" and concept.unit == "TWD"
        for metric in config.metrics
        for concept in metric.concepts
    )
    assert any(
        metric.metric_id == "capex" and concept.concept == "PaymentsToAcquireProductiveAssets"
        for metric in config.metrics
        for concept in metric.concepts
    )
    assert {metric.metric_id for metric in config.supporting_metrics} == {
        "cost_of_revenue"
    }
    assert any(
        derived.metric_id == "gross_profit"
        and derived.minuend_metric_id == "revenue"
        and derived.subtrahend_metric_id == "cost_of_revenue"
        for derived in config.derived_metrics
    )


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


def test_watchlist_config_covers_core_watchlist() -> None:
    universe = load_universe()
    watchlist = load_watchlist()
    active_tickers = {item.ticker for item in watchlist.items if item.active}

    assert set(universe.ai_chain["core_watchlist"]).issubset(active_tickers)
    assert all(item.competence_reason for item in watchlist.items)


def test_industry_chain_config_covers_watchlist_nodes() -> None:
    watchlist = load_watchlist()
    industry_chain = load_industry_chain()
    node_ids = {node.node_id for node in industry_chain.nodes}
    watchlist_node_ids = {
        node_id
        for item in watchlist.items
        for node_id in item.ai_chain_nodes
    }

    assert watchlist_node_ids.issubset(node_ids)


def test_market_regimes_default_to_ai_after_chatgpt() -> None:
    config = load_market_regimes()

    default_regime = market_regime_by_id(config, config.default_backtest_regime)

    assert default_regime.regime_id == "ai_after_chatgpt"
    assert default_regime.start_date.isoformat() == "2022-12-01"
    assert default_regime.anchor_date.isoformat() == "2022-11-30"
    assert "ChatGPT" in default_regime.anchor_event


def test_risk_events_config_loads_levels_and_rules() -> None:
    config = load_risk_events()

    assert {level.level for level in config.levels} == {"L1", "L2", "L3"}
    assert config.event_rules
    assert any(rule.event_id == "ai_chip_export_control_upgrade" for rule in config.event_rules)
