from __future__ import annotations

from ai_trading_system.config import (
    configured_price_tickers,
    configured_rate_series,
    load_backtest_validation_policy,
    load_data_quality,
    load_data_sources,
    load_features,
    load_fundamental_features,
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
    assert config.ai_chain["core_watchlist"] == [
        "MSFT",
        "GOOG",
        "TSM",
        "INTC",
        "AMD",
        "NVDA",
        "AMZN",
        "META",
        "AVGO",
        "MRVL",
        "ASML",
        "AMAT",
        "LRCX",
        "MU",
        "PLTR",
        "CRM",
        "NOW",
    ]


def test_portfolio_config_loads_total_asset_budget() -> None:
    config = load_portfolio()

    assert config.decision.frequency == "daily"
    assert config.portfolio.total_risk_asset_min == 0.60
    assert config.portfolio.total_risk_asset_max == 0.80
    assert config.risk_budget.enabled is True
    assert config.risk_budget.market_stress.stress_max_position == 0.45
    assert config.risk_budget.concentration.max_single_ticker_share_of_ai == 0.30


def test_data_quality_config_loads_thresholds() -> None:
    config = load_data_quality()

    assert config.prices.max_stale_calendar_days == 7
    assert config.prices.suspicious_daily_return_abs == 0.20
    assert config.prices.consistency_start_date is not None
    assert config.prices.consistency_start_date.isoformat() == "2022-12-01"
    assert config.prices.volume_optional_tickers == ["^VIX"]
    assert config.prices.known_split_events["NVDA"][0].ratio == 10
    assert config.prices.secondary_source_self_check_fail_closed is False
    assert config.prices.ticker_return_threshold_overrides["^VIX"].extreme_daily_return_abs == 2.00
    assert config.rates.min_plausible_value == -1.0
    assert config.rates.max_plausible_value == 25.0
    assert config.rates.consistency_start_date is not None
    assert config.rates.consistency_start_date.isoformat() == "2022-12-01"
    assert config.rates.series_overrides["DTWEXBGS"].max_stale_calendar_days == 14
    assert config.rates.series_overrides["DTWEXBGS"].max_plausible_value == 250.0
    assert config.rates.series_overrides["DTWEXBGS"].extreme_daily_change_abs == 5.0


def test_data_sources_config_loads_current_and_planned_sources() -> None:
    config = load_data_sources()
    source_ids = {source.source_id for source in config.sources}

    assert "yahoo_finance_daily_prices" in source_ids
    assert "fred_daily_rates" in source_ids
    assert "sec_company_facts" in source_ids
    assert any(source.status == "planned" for source in config.sources)


def test_sec_companies_config_covers_core_watchlist() -> None:
    universe = load_universe()
    config = load_sec_companies()
    by_ticker = {company.ticker: company for company in config.companies}

    assert set(universe.ai_chain["core_watchlist"]).issubset(by_ticker)
    assert by_ticker["NVDA"].cik == "0001045810"
    assert by_ticker["TSM"].expected_taxonomies == ["ifrs-full", "dei"]
    assert by_ticker["TSM"].sec_metric_periods == ["annual", "quarterly"]
    assert by_ticker["ASML"].sec_metric_periods == ["annual"]


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


def test_fundamental_features_config_loads_ratio_formulas() -> None:
    config = load_fundamental_features()
    by_id = {feature.feature_id: feature for feature in config.features}

    assert {"gross_margin", "operating_margin", "net_margin"}.issubset(by_id)
    assert by_id["gross_margin"].numerator_metric_id == "gross_profit"
    assert by_id["gross_margin"].denominator_metric_id == "revenue"
    assert by_id["capex_intensity"].unit == "ratio"
    assert by_id["capex_intensity"].preferred_periods == ["annual"]


def test_feature_config_loads_market_feature_windows() -> None:
    config = load_features()

    assert config.moving_average_windows == [20, 50, 100, 200]
    assert config.return_windows == [1, 5, 20]
    assert config.vix.ticker == "^VIX"
    assert config.rates.change_series == ["DGS2", "DGS10"]
    assert config.rates.return_windows == [20]
    assert config.rates.return_series == ["DTWEXBGS"]
    assert config.core_breadth.long_moving_average_window == 200


def test_scoring_rules_config_loads_weights_and_placeholders() -> None:
    config = load_scoring_rules()

    assert config.policy_metadata.version == "scoring_rules_v1"
    assert config.weights["trend"] == 25
    assert config.minimum_signal_coverage == 0.50
    assert config.position_bands[0].min_score == 80
    assert config.position_bands[-1].min_score == 0
    assert config.daily_conclusion.aggressive_score_min == 65
    assert config.confidence_policy.position_cap_bands[1].cap_multiplier == 0.85
    assert config.source_type_confidence.placeholder == 0.25
    assert config.source_type_confidence.llm_formal_assessment == 0.65
    assert config.trend.signals[0].subject == "SPY"
    assert config.placeholders["valuation"].score == 50
    assert "MVP 阶段占位" in config.placeholders["valuation"].reason


def test_backtest_validation_policy_loads_governed_thresholds() -> None:
    config = load_backtest_validation_policy()

    assert config.policy_metadata.version == "backtest_validation_policy_v1"
    assert config.data_credibility.component_coverage_min == 0.90
    assert config.robustness.default_weight_perturbation_pct == 0.20
    assert config.robustness.default_oos_split_ratio == 0.70
    assert config.robustness.full_exposure_time_in_market_min == 0.95
    assert config.robustness.volatility_target_annual_volatility == 0.25
    assert config.robustness.volatility_target_lookback_days == 20
    assert config.robustness.oos_material_underperformance_total_return_delta == 0.05
    assert config.robustness.candidate_random_baseline_min_percentile == 0.90
    assert config.robustness.candidate_blocking_data_credibility_grades == ["C"]
    assert config.robustness.candidate_min_component_coverage == 0.90
    assert config.robustness.candidate_max_placeholder_share == 0.0
    assert config.robustness.candidate_blocking_component_source_types == [
        "insufficient_data",
        "placeholder",
    ]
    assert config.robustness.candidate_require_bootstrap_ci is True
    assert config.robustness.candidate_label_horizon_days == 20
    assert config.robustness.candidate_embargo_days == 5
    assert config.robustness.candidate_min_independent_windows == 6
    assert config.robustness.bootstrap_iterations == 200
    assert config.robustness.bootstrap_block_size_days == 20
    assert config.promotion.min_lag_sensitivity_days == 3
    assert "same_turnover_random_strategy" in (
        config.promotion.required_robustness_categories
    )


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
        "MSFT",
        "GOOG",
        "TSM",
        "INTC",
        "AMD",
        "NVDA",
        "AMZN",
        "META",
        "AVGO",
        "MRVL",
        "ASML",
        "AMAT",
        "LRCX",
        "MU",
        "PLTR",
        "CRM",
        "NOW",
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

    assert configured_rate_series(config) == ["DGS2", "DGS10", "DTWEXBGS"]


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
