from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, Field, model_validator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "universe.yaml"
DEFAULT_PORTFOLIO_CONFIG_PATH = PROJECT_ROOT / "config" / "portfolio.yaml"
DEFAULT_DATA_QUALITY_CONFIG_PATH = PROJECT_ROOT / "config" / "data_quality.yaml"
DEFAULT_FEATURE_CONFIG_PATH = PROJECT_ROOT / "config" / "features.yaml"
DEFAULT_SCORING_RULES_CONFIG_PATH = PROJECT_ROOT / "config" / "scoring_rules.yaml"
DEFAULT_WATCHLIST_CONFIG_PATH = PROJECT_ROOT / "config" / "watchlist.yaml"
DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH = PROJECT_ROOT / "config" / "industry_chain.yaml"
DEFAULT_MARKET_REGIMES_CONFIG_PATH = PROJECT_ROOT / "config" / "market_regimes.yaml"


class MarketUniverse(BaseModel):
    decision_frequency: str = "daily"
    benchmarks: list[str] = Field(default_factory=list)
    defensive: list[str] = Field(default_factory=list)


class MacroUniverse(BaseModel):
    volatility: list[str] = Field(default_factory=list)
    rates: list[str] = Field(default_factory=list)
    currency: list[str] = Field(default_factory=list)


class UniverseConfig(BaseModel):
    market: MarketUniverse
    macro: MacroUniverse
    ai_chain: dict[str, list[str]]
    scoring_weights: dict[str, float]


class WatchlistItem(BaseModel):
    ticker: str = Field(min_length=1)
    company_name: str = Field(min_length=1)
    instrument_type: Literal["single_stock", "etf", "macro_proxy"]
    sector: str = Field(min_length=1)
    ai_chain_nodes: list[str] = Field(default_factory=list)
    competence_score: float = Field(ge=0, le=100)
    competence_reason: str = Field(min_length=1)
    default_risk_level: Literal["low", "medium", "high", "critical"]
    thesis_required: bool
    active: bool = True
    notes: str = ""


class WatchlistConfig(BaseModel):
    items: list[WatchlistItem]


class IndustryChainNodeConfig(BaseModel):
    node_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    parent_node_ids: list[str] = Field(default_factory=list)
    leading_indicators: list[str] = Field(default_factory=list)
    related_tickers: list[str] = Field(default_factory=list)
    impact_horizon: Literal["short", "medium", "long"]
    cash_flow_relevance: Literal["low", "medium", "high"]
    sentiment_relevance: Literal["low", "medium", "high"]


class IndustryChainConfig(BaseModel):
    nodes: list[IndustryChainNodeConfig]


class MarketRegimeConfig(BaseModel):
    regime_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    start_date: date
    anchor_date: date
    anchor_event: str = Field(min_length=1)
    description: str = Field(min_length=1)
    primary: bool = False


class MarketRegimesConfig(BaseModel):
    default_backtest_regime: str = Field(min_length=1)
    regimes: list[MarketRegimeConfig] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_regimes(self) -> Self:
        seen: set[str] = set()
        duplicate_ids: set[str] = set()
        for regime in self.regimes:
            if regime.regime_id in seen:
                duplicate_ids.add(regime.regime_id)
            seen.add(regime.regime_id)

        if duplicate_ids:
            duplicates = ", ".join(sorted(duplicate_ids))
            raise ValueError(f"market regime ids must be unique: {duplicates}")

        if self.default_backtest_regime not in seen:
            raise ValueError(
                "default_backtest_regime must match one configured market regime id"
            )

        return self


class DecisionConfig(BaseModel):
    frequency: str
    market_timezone: str
    report_timezone: str
    timing: str


class PortfolioBudgetConfig(BaseModel):
    total_risk_asset_min: float
    total_risk_asset_max: float


class PositionLimitsConfig(BaseModel):
    max_single_stock_in_ai_bucket: float
    max_total_ai_exposure: float


class PortfolioConfig(BaseModel):
    decision: DecisionConfig
    portfolio: PortfolioBudgetConfig
    position_limits: PositionLimitsConfig


class PriceReturnThresholdOverrideConfig(BaseModel):
    suspicious_daily_return_abs: float | None = Field(default=None, gt=0)
    extreme_daily_return_abs: float | None = Field(default=None, gt=0)


class PriceQualityConfig(BaseModel):
    max_stale_calendar_days: int = Field(gt=0)
    suspicious_daily_return_abs: float = Field(gt=0)
    extreme_daily_return_abs: float = Field(gt=0)
    suspicious_adjustment_ratio_change_abs: float = Field(gt=0)
    ticker_return_threshold_overrides: dict[str, PriceReturnThresholdOverrideConfig] = Field(
        default_factory=dict
    )

    @model_validator(mode="after")
    def validate_return_thresholds(self) -> Self:
        if self.suspicious_daily_return_abs >= self.extreme_daily_return_abs:
            raise ValueError("prices suspicious_daily_return_abs must be below extreme threshold")

        for ticker, override in self.ticker_return_threshold_overrides.items():
            suspicious = override.suspicious_daily_return_abs or self.suspicious_daily_return_abs
            extreme = override.extreme_daily_return_abs or self.extreme_daily_return_abs
            if suspicious >= extreme:
                raise ValueError(
                    f"prices return thresholds for {ticker} must keep suspicious below extreme"
                )
        return self


class RateQualityConfig(BaseModel):
    max_stale_calendar_days: int = Field(gt=0)
    min_plausible_value: float
    max_plausible_value: float
    suspicious_daily_change_abs: float = Field(gt=0)
    extreme_daily_change_abs: float = Field(gt=0)


class DataQualityConfig(BaseModel):
    prices: PriceQualityConfig
    rates: RateQualityConfig


class RelativeStrengthPairConfig(BaseModel):
    numerator: str
    denominator: str


class VixFeatureConfig(BaseModel):
    ticker: str
    moving_average_window: int = Field(gt=0)
    percentile_window: int = Field(gt=0)


class RateFeatureConfig(BaseModel):
    change_windows: list[int]


class CoreBreadthFeatureConfig(BaseModel):
    long_moving_average_window: int = Field(gt=0)


class FeatureConfig(BaseModel):
    moving_average_windows: list[int]
    return_windows: list[int]
    relative_strength_pairs: list[RelativeStrengthPairConfig]
    vix: VixFeatureConfig
    rates: RateFeatureConfig
    core_breadth: CoreBreadthFeatureConfig


class ScoreSignalConfig(BaseModel):
    subject: str
    feature: str
    points: float = Field(gt=0)
    bullish_above: float | None = None
    bullish_below: float | None = None
    bearish_above: float | None = None
    bearish_below: float | None = None
    scale_min: float | None = None
    scale_max: float | None = None


class ScoreModuleRuleConfig(BaseModel):
    neutral_score: float = Field(ge=0, le=100)
    signals: list[ScoreSignalConfig]


class PlaceholderScoreConfig(BaseModel):
    score: float = Field(ge=0, le=100)
    reason: str


class PositionChangeConfig(BaseModel):
    minimum_action_delta: float = Field(ge=0, le=1)


class ScoringRulesConfig(BaseModel):
    weights: dict[str, float]
    minimum_signal_coverage: float = Field(ge=0, le=1)
    trend: ScoreModuleRuleConfig
    macro_liquidity: ScoreModuleRuleConfig
    risk_sentiment: ScoreModuleRuleConfig
    placeholders: dict[str, PlaceholderScoreConfig]
    position_change: PositionChangeConfig


def load_universe(path: Path | str = DEFAULT_CONFIG_PATH) -> UniverseConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw: dict[str, Any] = yaml.safe_load(file)
    return UniverseConfig.model_validate(raw)


def load_watchlist(path: Path | str = DEFAULT_WATCHLIST_CONFIG_PATH) -> WatchlistConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw: dict[str, Any] = yaml.safe_load(file)
    return WatchlistConfig.model_validate(raw)


def load_industry_chain(
    path: Path | str = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
) -> IndustryChainConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw: dict[str, Any] = yaml.safe_load(file)
    return IndustryChainConfig.model_validate(raw)


def load_market_regimes(
    path: Path | str = DEFAULT_MARKET_REGIMES_CONFIG_PATH,
) -> MarketRegimesConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw: dict[str, Any] = yaml.safe_load(file)
    return MarketRegimesConfig.model_validate(raw)


def market_regime_by_id(
    config: MarketRegimesConfig,
    regime_id: str,
) -> MarketRegimeConfig:
    for regime in config.regimes:
        if regime.regime_id == regime_id:
            return regime
    configured_ids = ", ".join(regime.regime_id for regime in config.regimes)
    raise ValueError(f"unknown market regime '{regime_id}', available: {configured_ids}")


def load_portfolio(path: Path | str = DEFAULT_PORTFOLIO_CONFIG_PATH) -> PortfolioConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw: dict[str, Any] = yaml.safe_load(file)
    return PortfolioConfig.model_validate(raw)


def load_data_quality(
    path: Path | str = DEFAULT_DATA_QUALITY_CONFIG_PATH,
) -> DataQualityConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw: dict[str, Any] = yaml.safe_load(file)
    return DataQualityConfig.model_validate(raw)


def load_features(path: Path | str = DEFAULT_FEATURE_CONFIG_PATH) -> FeatureConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw: dict[str, Any] = yaml.safe_load(file)
    return FeatureConfig.model_validate(raw)


def load_scoring_rules(
    path: Path | str = DEFAULT_SCORING_RULES_CONFIG_PATH,
) -> ScoringRulesConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw: dict[str, Any] = yaml.safe_load(file)
    return ScoringRulesConfig.model_validate(raw)


def configured_price_tickers(
    config: UniverseConfig,
    include_full_ai_chain: bool = False,
) -> list[str]:
    tickers: list[str] = []
    tickers.extend(config.market.benchmarks)
    tickers.extend(config.market.defensive)
    tickers.extend(config.macro.volatility)
    tickers.extend(config.macro.currency)
    tickers.extend(config.ai_chain.get("core_watchlist", []))

    if include_full_ai_chain:
        for group_name, group_tickers in config.ai_chain.items():
            if group_name != "core_watchlist":
                tickers.extend(group_tickers)

    return dedupe_preserving_order(tickers)


def configured_rate_series(config: UniverseConfig) -> list[str]:
    return dedupe_preserving_order(config.macro.rates)


def dedupe_preserving_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    unique_items: list[str] = []
    for item in items:
        if item not in seen:
            unique_items.append(item)
            seen.add(item)
    return unique_items
