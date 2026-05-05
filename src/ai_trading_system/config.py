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
DEFAULT_BENCHMARK_POLICY_CONFIG_PATH = PROJECT_ROOT / "config" / "benchmark_policy.yaml"
DEFAULT_SCENARIO_LIBRARY_CONFIG_PATH = PROJECT_ROOT / "config" / "scenario_library.yaml"
DEFAULT_CATALYST_CALENDAR_CONFIG_PATH = PROJECT_ROOT / "config" / "catalyst_calendar.yaml"
DEFAULT_EXECUTION_POLICY_CONFIG_PATH = PROJECT_ROOT / "config" / "execution_policy.yaml"
DEFAULT_RISK_EVENTS_CONFIG_PATH = PROJECT_ROOT / "config" / "risk_events.yaml"
DEFAULT_DATA_SOURCES_CONFIG_PATH = PROJECT_ROOT / "config" / "data_sources.yaml"
DEFAULT_SEC_COMPANIES_CONFIG_PATH = PROJECT_ROOT / "config" / "sec_companies.yaml"
DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH = PROJECT_ROOT / "config" / "fundamental_metrics.yaml"
DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "fundamental_features.yaml"
)


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


class RiskEventLevelConfig(BaseModel):
    level: Literal["L1", "L2", "L3"]
    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    default_action: str = Field(min_length=1)
    target_ai_exposure_multiplier: float = Field(ge=0, le=1)
    requires_manual_review: bool


class RiskEventRuleConfig(BaseModel):
    event_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    name: str = Field(min_length=1)
    level: Literal["L1", "L2", "L3"]
    description: str = Field(min_length=1)
    affected_nodes: list[str] = Field(min_length=1)
    related_tickers: list[str] = Field(default_factory=list)
    trigger_examples: list[str] = Field(min_length=1)
    recommended_actions: list[str] = Field(min_length=1)
    escalation_conditions: list[str] = Field(default_factory=list)
    deescalation_conditions: list[str] = Field(default_factory=list)
    active: bool = True


class RiskEventsConfig(BaseModel):
    levels: list[RiskEventLevelConfig] = Field(min_length=1)
    event_rules: list[RiskEventRuleConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_levels_and_rules(self) -> Self:
        seen_levels: set[str] = set()
        duplicate_levels: set[str] = set()
        for level in self.levels:
            if level.level in seen_levels:
                duplicate_levels.add(level.level)
            seen_levels.add(level.level)

        if duplicate_levels:
            duplicates = ", ".join(sorted(duplicate_levels))
            raise ValueError(f"risk event levels must be unique: {duplicates}")

        missing_levels = {"L1", "L2", "L3"} - seen_levels
        if missing_levels:
            missing = ", ".join(sorted(missing_levels))
            raise ValueError(f"risk event levels must include: {missing}")

        seen_rules: set[str] = set()
        duplicate_rules: set[str] = set()
        for rule in self.event_rules:
            if rule.event_id in seen_rules:
                duplicate_rules.add(rule.event_id)
            seen_rules.add(rule.event_id)
            if rule.level not in seen_levels:
                raise ValueError(f"risk event rule {rule.event_id} references unknown level")

        if duplicate_rules:
            duplicates = ", ".join(sorted(duplicate_rules))
            raise ValueError(f"risk event rule ids must be unique: {duplicates}")

        return self


class DataSourceLlmPermissionConfig(BaseModel):
    license_scope: str = "unknown"
    personal_use_only: bool = True
    external_llm_allowed: bool = False
    cache_allowed: bool = False
    redistribution_allowed: bool = False
    max_content_sent_level: Literal[
        "metadata_only",
        "short_excerpt",
        "summary_only",
        "full_text",
    ] = "metadata_only"
    approval_ref: str = ""
    reviewed_at: date | None = None


class DataSourceConfig(BaseModel):
    source_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    provider: str = Field(min_length=1)
    source_type: Literal[
        "primary_source",
        "paid_vendor",
        "public_convenience",
        "manual_input",
    ]
    status: Literal["active", "planned", "inactive"]
    domains: list[
        Literal[
            "market_prices",
            "macro_rates",
            "fundamentals",
            "valuation",
            "news_events",
            "trade_records",
            "portfolio_positions",
            "trade_thesis",
            "risk_events",
        ]
    ] = Field(min_length=1)
    endpoint: str = ""
    adapter: str = ""
    cadence: Literal["daily", "weekly", "monthly", "quarterly", "event_driven", "manual"]
    requires_credentials: bool = False
    cache_paths: list[str] = Field(default_factory=list)
    primary_for: list[str] = Field(default_factory=list)
    audit_fields: list[str] = Field(default_factory=list)
    validation_checks: list[str] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)
    llm_permission: DataSourceLlmPermissionConfig = Field(
        default_factory=DataSourceLlmPermissionConfig
    )
    owner_notes: str = ""


class DataSourcesConfig(BaseModel):
    sources: list[DataSourceConfig] = Field(min_length=1)


def _default_sec_metric_periods() -> list[Literal["annual", "quarterly"]]:
    return ["annual", "quarterly"]


class SecCompanyConfig(BaseModel):
    ticker: str = Field(min_length=1)
    cik: str = Field(pattern=r"^\d{10}$")
    company_name: str = Field(min_length=1)
    active: bool = True
    expected_taxonomies: list[str] = Field(default_factory=list)
    sec_metric_periods: list[Literal["annual", "quarterly"]] = Field(
        default_factory=_default_sec_metric_periods,
        min_length=1,
    )
    notes: str = ""

    @model_validator(mode="after")
    def validate_sec_company(self) -> Self:
        self.ticker = self.ticker.upper()
        duplicate_periods = _duplicates(self.sec_metric_periods)
        if duplicate_periods:
            raise ValueError(
                f"SEC company {self.ticker} has duplicate metric periods: "
                f"{', '.join(duplicate_periods)}"
            )
        return self


class SecCompaniesConfig(BaseModel):
    companies: list[SecCompanyConfig] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_company_ids(self) -> Self:
        tickers: set[str] = set()
        duplicate_tickers: set[str] = set()
        ciks: set[str] = set()
        duplicate_ciks: set[str] = set()

        for company in self.companies:
            if company.ticker in tickers:
                duplicate_tickers.add(company.ticker)
            tickers.add(company.ticker)

            if company.cik in ciks:
                duplicate_ciks.add(company.cik)
            ciks.add(company.cik)

        if duplicate_tickers:
            raise ValueError(
                f"SEC company tickers must be unique: {', '.join(sorted(duplicate_tickers))}"
            )
        if duplicate_ciks:
            raise ValueError(
                f"SEC company CIKs must be unique: {', '.join(sorted(duplicate_ciks))}"
            )
        return self


class FundamentalMetricConceptConfig(BaseModel):
    taxonomy: str = Field(min_length=1)
    concept: str = Field(min_length=1)
    unit: str = Field(min_length=1)


class FundamentalMetricConfig(BaseModel):
    metric_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    preferred_periods: list[Literal["annual", "quarterly"]] = Field(min_length=1)
    concepts: list[FundamentalMetricConceptConfig] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_metric_mapping(self) -> Self:
        duplicate_periods = _duplicates(self.preferred_periods)
        if duplicate_periods:
            raise ValueError(
                f"fundamental metric {self.metric_id} has duplicate periods: "
                f"{', '.join(duplicate_periods)}"
            )

        concept_keys = [
            f"{concept.taxonomy}:{concept.concept}:{concept.unit}"
            for concept in self.concepts
        ]
        duplicate_concepts = _duplicates(concept_keys)
        if duplicate_concepts:
            raise ValueError(
                f"fundamental metric {self.metric_id} has duplicate concepts: "
                f"{', '.join(duplicate_concepts)}"
            )
        return self


class FundamentalDerivedMetricConfig(BaseModel):
    metric_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    operation: Literal["difference"]
    minuend_metric_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    subtrahend_metric_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")

    @model_validator(mode="after")
    def validate_derived_metric(self) -> Self:
        if self.metric_id in {self.minuend_metric_id, self.subtrahend_metric_id}:
            raise ValueError(
                f"derived metric {self.metric_id} must not reference itself as a component"
            )
        if self.minuend_metric_id == self.subtrahend_metric_id:
            raise ValueError(
                f"derived metric {self.metric_id} must use two distinct components"
            )
        return self


class FundamentalMetricsConfig(BaseModel):
    metrics: list[FundamentalMetricConfig] = Field(min_length=1)
    supporting_metrics: list[FundamentalMetricConfig] = Field(default_factory=list)
    derived_metrics: list[FundamentalDerivedMetricConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_metric_ids(self) -> Self:
        seen: set[str] = set()
        duplicate_ids: set[str] = set()
        for metric in [*self.metrics, *self.supporting_metrics]:
            if metric.metric_id in seen:
                duplicate_ids.add(metric.metric_id)
            seen.add(metric.metric_id)

        if duplicate_ids:
            raise ValueError(
                f"fundamental metric ids must be unique: {', '.join(sorted(duplicate_ids))}"
            )

        output_metric_ids = {metric.metric_id for metric in self.metrics}
        available_metric_ids = {
            metric.metric_id for metric in [*self.metrics, *self.supporting_metrics]
        }
        seen_derived_ids: set[str] = set()
        duplicate_derived_ids: set[str] = set()
        for derived_metric in self.derived_metrics:
            if derived_metric.metric_id in seen_derived_ids:
                duplicate_derived_ids.add(derived_metric.metric_id)
            seen_derived_ids.add(derived_metric.metric_id)
            if derived_metric.metric_id not in output_metric_ids:
                raise ValueError(
                    f"derived metric {derived_metric.metric_id} must target an output metric"
                )
            missing_components = sorted(
                {
                    derived_metric.minuend_metric_id,
                    derived_metric.subtrahend_metric_id,
                }
                - available_metric_ids
            )
            if missing_components:
                raise ValueError(
                    f"derived metric {derived_metric.metric_id} references unknown "
                    f"components: {', '.join(missing_components)}"
                )

        if duplicate_derived_ids:
            raise ValueError(
                "derived fundamental metric ids must be unique: "
                f"{', '.join(sorted(duplicate_derived_ids))}"
            )
        return self


class FundamentalRatioFeatureConfig(BaseModel):
    feature_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    numerator_metric_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    denominator_metric_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    preferred_periods: list[Literal["annual", "quarterly"]] = Field(min_length=1)
    unit: Literal["ratio"] = "ratio"

    @model_validator(mode="after")
    def validate_ratio_feature(self) -> Self:
        duplicate_periods = _duplicates(self.preferred_periods)
        if duplicate_periods:
            raise ValueError(
                f"fundamental feature {self.feature_id} has duplicate periods: "
                f"{', '.join(duplicate_periods)}"
            )
        if self.numerator_metric_id == self.denominator_metric_id:
            raise ValueError(
                f"fundamental feature {self.feature_id} numerator and denominator "
                "must be distinct"
            )
        return self


class FundamentalFeaturesConfig(BaseModel):
    features: list[FundamentalRatioFeatureConfig] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_feature_ids(self) -> Self:
        feature_ids = [feature.feature_id for feature in self.features]
        duplicate_ids = _duplicates(feature_ids)
        if duplicate_ids:
            raise ValueError(
                f"fundamental feature ids must be unique: {', '.join(duplicate_ids)}"
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

    @model_validator(mode="after")
    def validate_budget_range(self) -> Self:
        if not 0 <= self.total_risk_asset_min <= self.total_risk_asset_max <= 1:
            raise ValueError(
                "portfolio total risk asset budget must satisfy 0 <= min <= max <= 1"
            )
        return self


class PositionLimitsConfig(BaseModel):
    max_single_stock_in_ai_bucket: float
    max_total_ai_exposure: float


class MarketStressRiskBudgetConfig(BaseModel):
    elevated_vix_current: float = Field(default=25.0, ge=0)
    stress_vix_current: float = Field(default=32.0, ge=0)
    elevated_vix_percentile: float = Field(default=0.65, ge=0, le=1)
    stress_vix_percentile: float = Field(default=0.85, ge=0, le=1)
    elevated_max_position: float = Field(default=0.70, ge=0, le=1)
    stress_max_position: float = Field(default=0.45, ge=0, le=1)

    @model_validator(mode="after")
    def validate_market_stress_thresholds(self) -> Self:
        if self.stress_vix_current < self.elevated_vix_current:
            raise ValueError("stress_vix_current must be >= elevated_vix_current")
        if self.stress_vix_percentile < self.elevated_vix_percentile:
            raise ValueError("stress_vix_percentile must be >= elevated_vix_percentile")
        if self.stress_max_position > self.elevated_max_position:
            raise ValueError("stress_max_position must be <= elevated_max_position")
        return self


class ConcentrationRiskBudgetConfig(BaseModel):
    max_single_ticker_share_of_ai: float = Field(default=0.30, ge=0, le=1)
    max_industry_node_share_of_ai: float = Field(default=0.50, ge=0, le=1)
    max_correlation_cluster_share_of_ai: float = Field(default=0.60, ge=0, le=1)
    min_etf_beta_coverage: float = Field(default=0.80, ge=0, le=1)
    concentration_max_position: float = Field(default=0.70, ge=0, le=1)
    missing_etf_beta_max_position: float = Field(default=0.70, ge=0, le=1)


class MacroRiskAssetBudgetConfig(BaseModel):
    enabled: bool = True
    vix_subject: str = "^VIX"
    vix_current_feature: str = "vix_current"
    vix_percentile_feature: str = "vix_percentile_252"
    rate_subject: str = "DGS10"
    rate_change_feature: str = "rate_change_20d"
    dollar_subject: str = "DTWEXBGS"
    dollar_return_feature: str = "return_20d"
    elevated_vix_current: float = Field(default=25.0, ge=0)
    stress_vix_current: float = Field(default=32.0, ge=0)
    elevated_vix_percentile: float = Field(default=0.65, ge=0, le=1)
    stress_vix_percentile: float = Field(default=0.85, ge=0, le=1)
    elevated_rate_change_20d: float = Field(default=0.25, ge=0)
    stress_rate_change_20d: float = Field(default=0.50, ge=0)
    elevated_dollar_return_20d: float = Field(default=0.03, ge=0)
    stress_dollar_return_20d: float = Field(default=0.06, ge=0)
    elevated_total_risk_asset_min: float = Field(default=0.50, ge=0, le=1)
    elevated_total_risk_asset_max: float = Field(default=0.70, ge=0, le=1)
    stress_total_risk_asset_min: float = Field(default=0.35, ge=0, le=1)
    stress_total_risk_asset_max: float = Field(default=0.55, ge=0, le=1)

    @model_validator(mode="after")
    def validate_macro_budget_thresholds(self) -> Self:
        if self.stress_vix_current < self.elevated_vix_current:
            raise ValueError("stress_vix_current must be >= elevated_vix_current")
        if self.stress_vix_percentile < self.elevated_vix_percentile:
            raise ValueError("stress_vix_percentile must be >= elevated_vix_percentile")
        if self.stress_rate_change_20d < self.elevated_rate_change_20d:
            raise ValueError("stress_rate_change_20d must be >= elevated_rate_change_20d")
        if self.stress_dollar_return_20d < self.elevated_dollar_return_20d:
            raise ValueError(
                "stress_dollar_return_20d must be >= elevated_dollar_return_20d"
            )
        if self.elevated_total_risk_asset_min > self.elevated_total_risk_asset_max:
            raise ValueError(
                "elevated total risk asset budget must satisfy min <= max"
            )
        if self.stress_total_risk_asset_min > self.stress_total_risk_asset_max:
            raise ValueError("stress total risk asset budget must satisfy min <= max")
        if self.stress_total_risk_asset_max > self.elevated_total_risk_asset_max:
            raise ValueError(
                "stress_total_risk_asset_max must be <= elevated_total_risk_asset_max"
            )
        return self


class RiskBudgetConfig(BaseModel):
    enabled: bool = True
    market_stress: MarketStressRiskBudgetConfig = Field(
        default_factory=MarketStressRiskBudgetConfig
    )
    concentration: ConcentrationRiskBudgetConfig = Field(
        default_factory=ConcentrationRiskBudgetConfig
    )


class PortfolioConfig(BaseModel):
    decision: DecisionConfig
    portfolio: PortfolioBudgetConfig
    position_limits: PositionLimitsConfig
    risk_budget: RiskBudgetConfig = Field(default_factory=RiskBudgetConfig)
    macro_risk_asset_budget: MacroRiskAssetBudgetConfig = Field(
        default_factory=MacroRiskAssetBudgetConfig
    )


class PriceReturnThresholdOverrideConfig(BaseModel):
    suspicious_daily_return_abs: float | None = Field(default=None, gt=0)
    extreme_daily_return_abs: float | None = Field(default=None, gt=0)


class PriceQualityConfig(BaseModel):
    max_stale_calendar_days: int = Field(gt=0)
    suspicious_daily_return_abs: float = Field(gt=0)
    extreme_daily_return_abs: float = Field(gt=0)
    suspicious_adjustment_ratio_change_abs: float = Field(gt=0)
    consistency_start_date: date | None = None
    secondary_source_min_overlap_ratio: float = Field(default=0.80, ge=0, le=1)
    secondary_source_adj_close_warning_pct: float = Field(default=0.01, gt=0)
    secondary_source_adj_close_error_pct: float = Field(default=0.05, gt=0)
    secondary_source_self_check_fail_closed: bool = False
    secondary_source_excluded_tickers: list[str] = Field(default_factory=lambda: ["^VIX"])
    ticker_return_threshold_overrides: dict[str, PriceReturnThresholdOverrideConfig] = Field(
        default_factory=dict
    )

    @model_validator(mode="after")
    def validate_return_thresholds(self) -> Self:
        if self.suspicious_daily_return_abs >= self.extreme_daily_return_abs:
            raise ValueError("prices suspicious_daily_return_abs must be below extreme threshold")
        if self.secondary_source_adj_close_warning_pct >= self.secondary_source_adj_close_error_pct:
            raise ValueError(
                "prices secondary source warning threshold must be below error threshold"
            )

        for ticker, override in self.ticker_return_threshold_overrides.items():
            suspicious = override.suspicious_daily_return_abs or self.suspicious_daily_return_abs
            extreme = override.extreme_daily_return_abs or self.extreme_daily_return_abs
            if suspicious >= extreme:
                raise ValueError(
                    f"prices return thresholds for {ticker} must keep suspicious below extreme"
                )
        return self


class RateSeriesQualityOverrideConfig(BaseModel):
    min_plausible_value: float | None = None
    max_plausible_value: float | None = None
    suspicious_daily_change_abs: float | None = Field(default=None, gt=0)
    extreme_daily_change_abs: float | None = Field(default=None, gt=0)


class RateQualityConfig(BaseModel):
    max_stale_calendar_days: int = Field(gt=0)
    min_plausible_value: float
    max_plausible_value: float
    suspicious_daily_change_abs: float = Field(gt=0)
    extreme_daily_change_abs: float = Field(gt=0)
    consistency_start_date: date | None = None
    series_overrides: dict[str, RateSeriesQualityOverrideConfig] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_rate_thresholds(self) -> Self:
        if self.min_plausible_value >= self.max_plausible_value:
            raise ValueError("rates min_plausible_value must be below max_plausible_value")
        if self.suspicious_daily_change_abs >= self.extreme_daily_change_abs:
            raise ValueError("rates suspicious_daily_change_abs must be below extreme threshold")

        for series, override in self.series_overrides.items():
            min_value = (
                override.min_plausible_value
                if override.min_plausible_value is not None
                else self.min_plausible_value
            )
            max_value = (
                override.max_plausible_value
                if override.max_plausible_value is not None
                else self.max_plausible_value
            )
            if min_value >= max_value:
                raise ValueError(f"rates plausible range for {series} must keep min below max")
            suspicious_change = (
                override.suspicious_daily_change_abs
                if override.suspicious_daily_change_abs is not None
                else self.suspicious_daily_change_abs
            )
            extreme_change = (
                override.extreme_daily_change_abs
                if override.extreme_daily_change_abs is not None
                else self.extreme_daily_change_abs
            )
            if suspicious_change >= extreme_change:
                raise ValueError(
                    f"rates daily change thresholds for {series} must keep suspicious below extreme"
                )
        return self


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
    change_series: list[str] = Field(default_factory=list)
    change_windows: list[int]
    return_windows: list[int] = Field(default_factory=list)
    return_series: list[str] = Field(default_factory=list)


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


class ValuationPositionGateConfig(BaseModel):
    expensive_or_crowded_max_position: float = Field(default=0.70, ge=0, le=1)
    extreme_overheated_max_position: float = Field(default=0.40, ge=0, le=1)


class ThesisPositionGateConfig(BaseModel):
    warning_max_position: float = Field(default=0.70, ge=0, le=1)
    failure_max_position: float = Field(default=0.0, ge=0, le=1)


class DataConfidencePositionGateConfig(BaseModel):
    data_quality_warning_max_position: float = Field(default=0.80, ge=0, le=1)
    insufficient_data_max_position: float = Field(default=0.60, ge=0, le=1)
    placeholder_max_position: float = Field(default=0.80, ge=0, le=1)


class PositionGateRulesConfig(BaseModel):
    valuation: ValuationPositionGateConfig = Field(
        default_factory=ValuationPositionGateConfig
    )
    thesis: ThesisPositionGateConfig = Field(default_factory=ThesisPositionGateConfig)
    data_confidence: DataConfidencePositionGateConfig = Field(
        default_factory=DataConfidencePositionGateConfig
    )


class ScoringRulesConfig(BaseModel):
    weights: dict[str, float]
    minimum_signal_coverage: float = Field(ge=0, le=1)
    trend: ScoreModuleRuleConfig
    fundamentals: ScoreModuleRuleConfig | None = None
    macro_liquidity: ScoreModuleRuleConfig
    risk_sentiment: ScoreModuleRuleConfig
    valuation: ScoreModuleRuleConfig | None = None
    policy_geopolitics: ScoreModuleRuleConfig | None = None
    placeholders: dict[str, PlaceholderScoreConfig]
    position_change: PositionChangeConfig
    position_gates: PositionGateRulesConfig = Field(default_factory=PositionGateRulesConfig)


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


def load_risk_events(
    path: Path | str = DEFAULT_RISK_EVENTS_CONFIG_PATH,
) -> RiskEventsConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw: dict[str, Any] = yaml.safe_load(file)
    return RiskEventsConfig.model_validate(raw)


def load_data_sources(
    path: Path | str = DEFAULT_DATA_SOURCES_CONFIG_PATH,
) -> DataSourcesConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw: dict[str, Any] = yaml.safe_load(file)
    return DataSourcesConfig.model_validate(raw)


def load_sec_companies(
    path: Path | str = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
) -> SecCompaniesConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw: dict[str, Any] = yaml.safe_load(file)
    return SecCompaniesConfig.model_validate(raw)


def load_fundamental_metrics(
    path: Path | str = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
) -> FundamentalMetricsConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw: dict[str, Any] = yaml.safe_load(file)
    return FundamentalMetricsConfig.model_validate(raw)


def load_fundamental_features(
    path: Path | str = DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
) -> FundamentalFeaturesConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw: dict[str, Any] = yaml.safe_load(file)
    return FundamentalFeaturesConfig.model_validate(raw)


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


def _duplicates(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: list[str] = []
    for item in items:
        if item in seen and item not in duplicates:
            duplicates.append(item)
        seen.add(item)
    return duplicates
