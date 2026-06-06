from __future__ import annotations

from pathlib import Path

from ai_trading_system.config import (
    DEFAULT_BACKTEST_VALIDATION_POLICY_CONFIG_PATH,
    DEFAULT_CONFIG_PATH,
    DEFAULT_DATA_QUALITY_CONFIG_PATH,
    DEFAULT_DATA_SOURCES_CONFIG_PATH,
    DEFAULT_FEATURE_CONFIG_PATH,
    DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    DEFAULT_PORTFOLIO_CONFIG_PATH,
    DEFAULT_SCORING_RULES_CONFIG_PATH,
    DEFAULT_WATCHLIST_CONFIG_PATH,
)
from ai_trading_system.feedback_sample_policy import DEFAULT_FEEDBACK_SAMPLE_POLICY_PATH
from ai_trading_system.llm_request_profiles import DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH
from ai_trading_system.weight_calibration import (
    DEFAULT_APPROVED_CALIBRATION_OVERLAY_PATH,
    DEFAULT_WEIGHT_PROFILE_PATH,
)


def base_trace_config_paths() -> dict[str, Path]:
    return {
        "universe": DEFAULT_CONFIG_PATH,
        "portfolio": DEFAULT_PORTFOLIO_CONFIG_PATH,
        "data_quality": DEFAULT_DATA_QUALITY_CONFIG_PATH,
        "features": DEFAULT_FEATURE_CONFIG_PATH,
        "scoring_rules": DEFAULT_SCORING_RULES_CONFIG_PATH,
        "weight_profile": DEFAULT_WEIGHT_PROFILE_PATH,
        "calibration_overlay": DEFAULT_APPROVED_CALIBRATION_OVERLAY_PATH,
        "llm_request_profiles": DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH,
        "watchlist": DEFAULT_WATCHLIST_CONFIG_PATH,
        "industry_chain": DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
        "data_sources": DEFAULT_DATA_SOURCES_CONFIG_PATH,
    }


def daily_trace_config_paths(
    *,
    sec_companies_path: Path,
    sec_metrics_path: Path,
    fundamental_feature_config_path: Path,
    risk_events_path: Path,
    execution_policy_path: Path,
    rule_cards_path: Path,
    feature_availability_path: Path,
) -> dict[str, Path]:
    return {
        **base_trace_config_paths(),
        "market_regimes": DEFAULT_MARKET_REGIMES_CONFIG_PATH,
        "sec_companies": sec_companies_path,
        "fundamental_metrics": sec_metrics_path,
        "fundamental_features": fundamental_feature_config_path,
        "risk_events": risk_events_path,
        "execution_policy": execution_policy_path,
        "rule_cards": rule_cards_path,
        "feature_availability": feature_availability_path,
    }


def backtest_trace_config_paths(
    *,
    regimes_path: Path,
    benchmark_policy_path: Path,
    sec_companies_path: Path,
    sec_metrics_path: Path,
    fundamental_feature_config_path: Path,
    risk_events_path: Path,
    watchlist_lifecycle_path: Path,
    rule_cards_path: Path,
    feature_availability_path: Path,
) -> dict[str, Path]:
    return {
        **base_trace_config_paths(),
        "market_regimes": regimes_path,
        "benchmark_policy": benchmark_policy_path,
        "backtest_validation_policy": DEFAULT_BACKTEST_VALIDATION_POLICY_CONFIG_PATH,
        "feedback_sample_policy": DEFAULT_FEEDBACK_SAMPLE_POLICY_PATH,
        "sec_companies": sec_companies_path,
        "fundamental_metrics": sec_metrics_path,
        "fundamental_features": fundamental_feature_config_path,
        "risk_events": risk_events_path,
        "watchlist_lifecycle": watchlist_lifecycle_path,
        "rule_cards": rule_cards_path,
        "feature_availability": feature_availability_path,
    }
