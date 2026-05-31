from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

ETF_CONFIG_ROOT = PROJECT_ROOT / "config" / "etf_portfolio"
DEFAULT_ETF_ASSETS_CONFIG_PATH = ETF_CONFIG_ROOT / "assets.yaml"
DEFAULT_ETF_STRATEGY_CONFIG_PATH = ETF_CONFIG_ROOT / "strategy.yaml"
DEFAULT_ETF_RISK_CONFIG_PATH = ETF_CONFIG_ROOT / "risk.yaml"
DEFAULT_ETF_BACKTEST_CONFIG_PATH = ETF_CONFIG_ROOT / "backtest.yaml"
DEFAULT_ETF_P1_CONFIG_PATH = ETF_CONFIG_ROOT / "p1.yaml"
DEFAULT_ETF_P2_CONFIG_PATH = ETF_CONFIG_ROOT / "p2.yaml"

DEFAULT_ETF_PRICE_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_ETF_FEATURE_PATH = PROJECT_ROOT / "data" / "etf_portfolio" / "features.csv"
DEFAULT_ETF_SIGNAL_PATH = PROJECT_ROOT / "data" / "etf_portfolio" / "signals.csv"
DEFAULT_ETF_REGIME_PATH = PROJECT_ROOT / "data" / "etf_portfolio" / "regimes.csv"
DEFAULT_ETF_TARGET_PATH = PROJECT_ROOT / "data" / "etf_portfolio" / "target_weights.csv"
DEFAULT_ETF_LEDGER_PATH = PROJECT_ROOT / "data" / "simulation" / "etf_ledger.csv"
DEFAULT_ETF_REPORT_DIR = PROJECT_ROOT / "reports" / "etf_portfolio"
DEFAULT_ETF_BACKTEST_DIR = DEFAULT_ETF_REPORT_DIR / "backtests"
DEFAULT_ETF_P2_MANIFEST_PATH = (
    PROJECT_ROOT / "data" / "etf_portfolio" / "p2" / "source_manifest.csv"
)


class PolicyMetadata(BaseModel):
    version: str = Field(min_length=1)
    status: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    rationale: str = ""
    validation: str = ""
    review_condition: str = ""


class ETFAssetConfig(BaseModel):
    name: str = Field(min_length=1)
    asset_type: Literal["ETF", "CASH"]
    sleeve: str = Field(min_length=1)
    role: str = Field(min_length=1)
    benchmark: str | None = None
    tradeable: bool
    min_weight: float = Field(ge=0, le=1)
    max_weight: float = Field(ge=0, le=1)
    default_weight: float = Field(ge=0, le=1)
    risk_group: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_weight_order(self) -> Self:
        if self.min_weight > self.default_weight or self.default_weight > self.max_weight:
            raise ValueError(
                "asset weights must satisfy min_weight <= default_weight <= max_weight"
            )
        return self


class ETFRiskGroupConfig(BaseModel):
    max_weight: float = Field(ge=0, le=1)


class ETFAssetsConfig(BaseModel):
    policy_metadata: PolicyMetadata
    assets: dict[str, ETFAssetConfig] = Field(min_length=1)
    risk_groups: dict[str, ETFRiskGroupConfig] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_assets(self) -> Self:
        if "CASH" not in self.assets:
            raise ValueError("ETF asset config must include CASH")
        total_default = sum(asset.default_weight for asset in self.assets.values())
        if abs(total_default - 1.0) > 1e-6:
            raise ValueError("ETF default weights must sum to 1.0")
        missing_groups = sorted(
            {asset.risk_group for asset in self.assets.values()} - set(self.risk_groups)
        )
        if missing_groups:
            raise ValueError(
                f"ETF assets reference missing risk groups: {', '.join(missing_groups)}"
            )
        return self

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(self.assets)

    @property
    def tradeable_symbols(self) -> tuple[str, ...]:
        return tuple(symbol for symbol, asset in self.assets.items() if asset.tradeable)


class ScoreWeightConfig(BaseModel):
    weight: float = Field(ge=0, le=1)


class ETFModelConfig(BaseModel):
    name: str = Field(min_length=1)
    version: str = Field(min_length=1)
    signal_execution_lag_days: int = Field(ge=1)
    rebalance_frequency: Literal["daily"]
    min_rebalance_delta: float = Field(ge=0, le=1)


class ETFTrendFeatureConfig(BaseModel):
    ma_windows: list[int] = Field(min_length=1)
    slope_window: int = Field(gt=0)


class ETFMomentumFeatureConfig(BaseModel):
    return_windows: list[int] = Field(min_length=1)


class ETFRiskFeatureConfig(BaseModel):
    vol_windows: list[int] = Field(min_length=1)
    drawdown_windows: list[int] = Field(min_length=1)


class ETFRelativeStrengthPairConfig(BaseModel):
    numerator: str = Field(min_length=1)
    denominator: str = Field(min_length=1)
    meaning: str = Field(min_length=1)


class ETFRelativeStrengthConfig(BaseModel):
    method: Literal["ratio_momentum"] = "ratio_momentum"
    windows: list[int] = Field(min_length=1)
    pairs: list[ETFRelativeStrengthPairConfig] = Field(min_length=1)


class ETFScoreMappingConfig(BaseModel):
    return_score_floor: float
    return_score_ceiling: float
    vol_low: float = Field(ge=0)
    vol_high: float = Field(gt=0)
    vol_max_penalty: float = Field(ge=0, le=100)
    drawdown_low: float = Field(le=0)
    drawdown_high: float = Field(le=0)
    drawdown_max_penalty: float = Field(ge=0, le=100)
    below_ma_200_penalty: float = Field(ge=0, le=100)

    @model_validator(mode="after")
    def validate_thresholds(self) -> Self:
        if self.return_score_floor >= self.return_score_ceiling:
            raise ValueError("return score floor must be below ceiling")
        if self.vol_low >= self.vol_high:
            raise ValueError("vol_low must be below vol_high")
        if self.drawdown_high >= self.drawdown_low:
            raise ValueError("drawdown_high must be more negative than drawdown_low")
        return self


class ETFAllocationConfig(BaseModel):
    score_multipliers: dict[str, float]


class ETFDataQualityThresholdConfig(BaseModel):
    suspicious_daily_return_abs: float = Field(gt=0)
    extreme_daily_return_abs: float = Field(gt=0)

    @model_validator(mode="after")
    def validate_thresholds(self) -> Self:
        if self.suspicious_daily_return_abs >= self.extreme_daily_return_abs:
            raise ValueError("suspicious daily return threshold must be below extreme threshold")
        return self


class ETFStrategyConfig(BaseModel):
    policy_metadata: PolicyMetadata
    model: ETFModelConfig
    scores: dict[str, ScoreWeightConfig]
    trend_features: ETFTrendFeatureConfig
    momentum_features: ETFMomentumFeatureConfig
    risk_features: ETFRiskFeatureConfig
    relative_strength: ETFRelativeStrengthConfig
    score_mapping: ETFScoreMappingConfig
    allocation: ETFAllocationConfig
    data_quality: ETFDataQualityThresholdConfig

    @model_validator(mode="after")
    def validate_score_weights(self) -> Self:
        expected = {"trend", "momentum", "relative_strength", "risk"}
        if set(self.scores) != expected:
            raise ValueError(
                "ETF score weights must define trend, momentum, relative_strength, risk"
            )
        total = sum(item.weight for item in self.scores.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError("ETF score weights must sum to 1.0")
        return self


class ETFRegimeConstraintConfig(BaseModel):
    equity_cap: float = Field(ge=0, le=1)
    cash_min: float = Field(ge=0, le=1)
    semiconductor_cap: float = Field(ge=0, le=1)
    satellite_cap: float = Field(ge=0, le=1)


class ETFPortfolioConstraintConfig(BaseModel):
    max_single_asset_weight: float = Field(ge=0, le=1)
    max_daily_turnover: float = Field(ge=0, le=1)
    max_rebalance_trade_weight: float = Field(ge=0, le=1)
    min_cash_weight: float = Field(ge=0, le=1)
    long_only: bool
    allow_leverage: bool


class ETFTransactionCostConfig(BaseModel):
    commission_bps: float = Field(ge=0)
    slippage_bps: float = Field(ge=0)


class ETFRegimeRuleConfig(BaseModel):
    risk_off_spy_drawdown_63d: float = Field(le=0)
    risk_off_qqq_drawdown_63d: float = Field(le=0)
    risk_on_qqq_trend_min: float = Field(ge=0, le=100)
    risk_on_spy_trend_min: float = Field(ge=0, le=100)
    overheated_qqq_ret_20d: float = Field(ge=0)
    overheated_qqq_distance_ma_50: float = Field(ge=0)
    overheated_smh_ret_20d: float = Field(ge=0)
    overheated_smh_drawdown_63d_min: float = Field(le=0)


class ETFRiskConfig(BaseModel):
    policy_metadata: PolicyMetadata
    regime_constraints: dict[str, ETFRegimeConstraintConfig]
    portfolio_constraints: ETFPortfolioConstraintConfig
    transaction_costs: ETFTransactionCostConfig
    regime_rules: ETFRegimeRuleConfig

    @model_validator(mode="after")
    def validate_regime_constraints(self) -> Self:
        expected = {"Risk-On", "Neutral", "Risk-Off", "Shock-Recovery", "Overheated"}
        missing = expected - set(self.regime_constraints)
        if missing:
            raise ValueError(
                f"ETF risk config missing regime constraints: {', '.join(sorted(missing))}"
            )
        if not self.portfolio_constraints.long_only:
            raise ValueError("ETF P0 requires long_only=true")
        if self.portfolio_constraints.allow_leverage:
            raise ValueError("ETF P0 requires allow_leverage=false")
        return self


class ETFBacktestSettings(BaseModel):
    regime: str = Field(min_length=1)
    start_date: date
    end_date: date | None = None
    warmup_start_date: date
    initial_capital: float = Field(gt=0)
    price_field: Literal["adj_close", "close"]
    execution_price: Literal["next_close"]
    signal_lag_days: int = Field(ge=1)
    rebalance_frequency: Literal["daily"]
    benchmark_assets: list[str] = Field(min_length=1)
    baselines: list[str] = Field(min_length=1)
    benchmarks: dict[str, ETFBenchmarkConfig] = Field(default_factory=dict)
    primary_benchmark_id: str = Field(default="B001", min_length=1)

    @model_validator(mode="after")
    def validate_primary_benchmark(self) -> Self:
        if self.benchmarks and self.primary_benchmark_id not in self.benchmarks:
            raise ValueError("primary_benchmark_id must reference a configured benchmark")
        return self


class ETFBenchmarkConfig(BaseModel):
    name: str = Field(min_length=1)
    benchmark_type: Literal[
        "buy_and_hold",
        "static_portfolio",
        "moving_average",
        "risk_off_cash_switch",
    ]
    symbol: str | None = None
    signal_symbol: str | None = None
    cash_symbol: str = "CASH"
    weights: dict[str, float] = Field(default_factory=dict)
    short_window: int | None = Field(default=None, gt=0)
    long_window: int | None = Field(default=None, gt=0)
    description: str = ""

    @model_validator(mode="after")
    def validate_benchmark_definition(self) -> Self:
        if self.benchmark_type == "buy_and_hold" and not self.symbol:
            raise ValueError("buy_and_hold benchmark requires symbol")
        if self.benchmark_type == "static_portfolio":
            if not self.weights:
                raise ValueError("static_portfolio benchmark requires weights")
            total_weight = sum(float(value) for value in self.weights.values())
            if abs(total_weight - 1.0) > 1e-6:
                raise ValueError("static_portfolio benchmark weights must sum to 1.0")
        if self.benchmark_type == "moving_average":
            if not self.symbol:
                raise ValueError("moving_average benchmark requires symbol")
            if self.short_window is None or self.long_window is None:
                raise ValueError("moving_average benchmark requires short_window and long_window")
            if self.short_window >= self.long_window:
                raise ValueError("moving_average short_window must be below long_window")
        if self.benchmark_type == "risk_off_cash_switch":
            if not self.symbol or not self.signal_symbol:
                raise ValueError("risk_off_cash_switch benchmark requires symbol and signal_symbol")
            if self.long_window is None:
                raise ValueError("risk_off_cash_switch benchmark requires long_window")
        return self


class ETFBacktestConfig(BaseModel):
    policy_metadata: PolicyMetadata
    backtest: ETFBacktestSettings

    @model_validator(mode="after")
    def validate_dates(self) -> Self:
        if self.backtest.warmup_start_date > self.backtest.start_date:
            raise ValueError("warmup_start_date must be <= start_date")
        return self


class ETFSatelliteStockConfig(BaseModel):
    benchmark_etf: str = Field(min_length=1)
    max_weight: float = Field(gt=0, le=1)
    group: str = Field(min_length=1)


class ETFSatelliteRuleConfig(BaseModel):
    trend_score_min: float = Field(ge=0, le=100)
    relative_strength_score_min: float = Field(ge=0, le=100)
    risk_score_min: float = Field(ge=0, le=100)
    total_satellite_cap: float = Field(ge=0, le=1)
    default_substitution_weight: float = Field(ge=0, le=1)


class ETFConfirmationConfig(BaseModel):
    semiconductor_pairs: list[str] = Field(default_factory=list)
    mega_cap_pairs: list[str] = Field(default_factory=list)
    score_positive_min: float = Field(ge=0, le=100)


class ETFEventCalendarItemConfig(BaseModel):
    event_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    event_date: date
    affected_symbols: list[str] = Field(default_factory=list)
    severity: Literal["low", "medium", "high"] = "medium"


class ETFEventCalendarConfig(BaseModel):
    warning_window_days: int = Field(ge=0)
    events: list[ETFEventCalendarItemConfig] = Field(default_factory=list)


class ETFGovernanceConfig(BaseModel):
    allowed_model_states: list[Literal["candidate", "shadow", "production", "retired"]]
    min_shadow_observations: int = Field(gt=0)
    manual_review_required: bool
    auto_promotion: bool

    @model_validator(mode="after")
    def validate_governance(self) -> Self:
        if self.auto_promotion:
            raise ValueError("ETF P1 baseline must keep auto_promotion=false")
        if not self.manual_review_required:
            raise ValueError("ETF P1 baseline must require manual review")
        return self


class ETFP1Config(BaseModel):
    policy_metadata: PolicyMetadata
    satellite_stocks: dict[str, ETFSatelliteStockConfig] = Field(default_factory=dict)
    satellite_rules: ETFSatelliteRuleConfig
    confirmation: ETFConfirmationConfig
    event_calendar: ETFEventCalendarConfig
    governance: ETFGovernanceConfig


class ETFP2SourceConfig(BaseModel):
    provider_status: Literal["not_configured", "manual_input", "connected"]
    source_level: str = Field(min_length=1)
    input_path: str = Field(min_length=1)
    as_of_column: str = Field(min_length=1)
    available_time_column: str = Field(min_length=1)
    required_columns: list[str] = Field(min_length=1)


class ETFP2AdvancedRiskConfig(BaseModel):
    covariance_window: int = Field(gt=1)
    concentration_warning_weight: float = Field(ge=0, le=1)
    volatility_warning: float = Field(gt=0)
    correlation_warning: float = Field(ge=0, le=1)


class ETFP2OptionsRiskConfig(BaseModel):
    source_symbol: str = Field(min_length=1)
    lookback_days: int = Field(gt=1)
    min_history_days: int = Field(gt=1)
    elevated_iv_rank: float = Field(ge=0, le=1)
    stress_iv_rank: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_options_thresholds(self) -> Self:
        if self.min_history_days > self.lookback_days:
            raise ValueError("ETF P2 options min_history_days must be <= lookback_days")
        if self.elevated_iv_rank > self.stress_iv_rank:
            raise ValueError("ETF P2 elevated_iv_rank must be <= stress_iv_rank")
        return self


class ETFP2NewsThemeConfig(BaseModel):
    neutral_sentiment_score: float = Field(ge=-1, le=1)
    default_relevance_score: float = Field(ge=0, le=1)
    require_explicit_sentiment: bool
    tracking_lookback_days: int = Field(gt=0)
    max_report_rows: int = Field(gt=0)
    candidate_only: bool
    auto_promotion: bool

    @model_validator(mode="after")
    def validate_news_theme_boundary(self) -> Self:
        if not self.candidate_only:
            raise ValueError("ETF P2 news theme tracking must stay candidate_only")
        if self.auto_promotion:
            raise ValueError("ETF P2 news theme tracking must keep auto_promotion=false")
        return self


class ETFP2EdgarTextAnalysisConfig(BaseModel):
    min_text_characters: int = Field(gt=0)
    candidate_only: bool
    auto_promotion: bool
    topic_keywords: dict[str, list[str]] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_edgar_text_analysis_boundary(self) -> Self:
        if not self.candidate_only:
            raise ValueError("ETF P2 EDGAR text analysis must stay candidate_only")
        if self.auto_promotion:
            raise ValueError("ETF P2 EDGAR text analysis must keep auto_promotion=false")
        for topic, keywords in self.topic_keywords.items():
            if not str(topic).strip():
                raise ValueError("ETF P2 EDGAR text analysis topics must not be blank")
            if not keywords or any(not str(keyword).strip() for keyword in keywords):
                raise ValueError(
                    "ETF P2 EDGAR text analysis keywords must be non-empty strings"
                )
        return self


class ETFP2WalkForwardConfig(BaseModel):
    min_completed_runs: int = Field(gt=0)
    min_distinct_model_versions: int = Field(gt=0)


class ETFP2MLRankingConfig(BaseModel):
    composite_weight: float = Field(ge=0, le=1)
    relative_strength_weight: float = Field(ge=0, le=1)
    risk_weight: float = Field(ge=0, le=1)
    candidate_only: bool
    auto_promotion: bool

    @model_validator(mode="after")
    def validate_ml_boundary(self) -> Self:
        total = self.composite_weight + self.relative_strength_weight + self.risk_weight
        if abs(total - 1.0) > 1e-6:
            raise ValueError("ETF P2 ML ranking weights must sum to 1.0")
        if not self.candidate_only:
            raise ValueError("ETF P2 ML ranking must stay candidate_only")
        if self.auto_promotion:
            raise ValueError("ETF P2 ML ranking must keep auto_promotion=false")
        return self


class ETFP2WeightOptimizerConfig(BaseModel):
    lookback_days: int = Field(gt=1)
    min_history_days: int = Field(gt=1)
    signal_score_weight: float = Field(ge=0, le=1)
    risk_adjusted_return_weight: float = Field(ge=0, le=1)
    inverse_volatility_weight: float = Field(ge=0, le=1)
    max_candidate_weight: float = Field(gt=0, le=1)
    min_cash_weight: float = Field(ge=0, le=1)
    candidate_only: bool
    auto_promotion: bool

    @model_validator(mode="after")
    def validate_weight_optimizer_boundary(self) -> Self:
        if self.min_history_days > self.lookback_days:
            raise ValueError("ETF P2 weight optimizer min_history_days must be <= lookback_days")
        total = (
            self.signal_score_weight
            + self.risk_adjusted_return_weight
            + self.inverse_volatility_weight
        )
        if abs(total - 1.0) > 1e-6:
            raise ValueError("ETF P2 weight optimizer component weights must sum to 1.0")
        if not self.candidate_only:
            raise ValueError("ETF P2 weight optimizer must stay candidate_only")
        if self.auto_promotion:
            raise ValueError("ETF P2 weight optimizer must keep auto_promotion=false")
        return self


class ETFP2EnsembleConfig(BaseModel):
    p0_signal_weight: float = Field(ge=0, le=1)
    ml_candidate_weight: float = Field(ge=0, le=1)
    candidate_only: bool
    auto_promotion: bool

    @model_validator(mode="after")
    def validate_ensemble_boundary(self) -> Self:
        total = self.p0_signal_weight + self.ml_candidate_weight
        if abs(total - 1.0) > 1e-6:
            raise ValueError("ETF P2 ensemble weights must sum to 1.0")
        if not self.candidate_only:
            raise ValueError("ETF P2 ensemble must stay candidate_only")
        if self.auto_promotion:
            raise ValueError("ETF P2 ensemble must keep auto_promotion=false")
        return self


class ETFP2LiveInterfaceConfig(BaseModel):
    enabled: bool
    paper_only: bool
    read_only: bool
    broker_routing_allowed: bool
    multi_account_enabled: bool

    @model_validator(mode="after")
    def validate_live_boundary(self) -> Self:
        if self.enabled:
            raise ValueError("ETF P2 live interface baseline must keep enabled=false")
        if not self.paper_only:
            raise ValueError("ETF P2 live interface baseline must keep paper_only=true")
        if not self.read_only:
            raise ValueError("ETF P2 live interface baseline must keep read_only=true")
        if self.broker_routing_allowed:
            raise ValueError(
                "ETF P2 live interface baseline must keep broker_routing_allowed=false"
            )
        if self.multi_account_enabled:
            raise ValueError("ETF P2 live interface baseline must keep multi_account_enabled=false")
        return self


class ETFP2Config(BaseModel):
    policy_metadata: PolicyMetadata
    sources: dict[
        Literal["edgar_text", "news_themes", "options_iv_skew", "etf_holdings"],
        ETFP2SourceConfig,
    ]
    advanced_risk: ETFP2AdvancedRiskConfig
    options_risk: ETFP2OptionsRiskConfig
    news_themes: ETFP2NewsThemeConfig
    edgar_text_analysis: ETFP2EdgarTextAnalysisConfig
    walk_forward: ETFP2WalkForwardConfig
    ml_ranking: ETFP2MLRankingConfig
    weight_optimizer: ETFP2WeightOptimizerConfig
    ensemble: ETFP2EnsembleConfig
    live_interface: ETFP2LiveInterfaceConfig

    @model_validator(mode="after")
    def validate_sources(self) -> Self:
        expected = {"edgar_text", "news_themes", "options_iv_skew", "etf_holdings"}
        missing = expected - set(self.sources)
        if missing:
            raise ValueError(f"ETF P2 config missing sources: {', '.join(sorted(missing))}")
        return self


class ETFConfigBundle(BaseModel):
    assets: ETFAssetsConfig
    strategy: ETFStrategyConfig
    risk: ETFRiskConfig
    backtest: ETFBacktestConfig
    p1: ETFP1Config | None = None
    p2: ETFP2Config | None = None
    config_hash: str


@dataclass(frozen=True)
class ETFValidationIssue:
    severity: Literal["ERROR", "WARNING", "INFO"]
    code: str
    message: str
    rows: int | None = None
    sample: str = ""


@dataclass(frozen=True)
class ETFQualityReport:
    checked_at: datetime
    as_of: date | None
    status: str
    row_count: int
    symbols: tuple[str, ...]
    min_date: date | None
    max_date: date | None
    checksum: str | None
    issues: tuple[ETFValidationIssue, ...] = ()

    @property
    def passed(self) -> bool:
        return not any(issue.severity == "ERROR" for issue in self.issues)


@dataclass(frozen=True)
class ETFSignalRecord:
    date: date
    symbol: str
    trend_score: float
    momentum_score: float
    relative_strength_score: float
    risk_score: float
    composite_score: float
    direction: str
    confidence: str
    reason_codes: tuple[str, ...]
    model_version: str
    feature_version: str
    created_at: datetime

    def to_record(self) -> dict[str, object]:
        return {
            "date": self.date.isoformat(),
            "symbol": self.symbol,
            "trend_score": self.trend_score,
            "momentum_score": self.momentum_score,
            "relative_strength_score": self.relative_strength_score,
            "risk_score": self.risk_score,
            "composite_score": self.composite_score,
            "direction": self.direction,
            "confidence": self.confidence,
            "reason_codes": json.dumps(list(self.reason_codes), ensure_ascii=False),
            "model_version": self.model_version,
            "feature_version": self.feature_version,
            "created_at": self.created_at.isoformat(),
        }


@dataclass(frozen=True)
class ETFRegimeRecord:
    date: date
    regime: str
    regime_score: float
    risk_on_score: float
    risk_off_score: float
    growth_leadership_score: float
    semiconductor_leadership_score: float
    reason_codes: tuple[str, ...]
    model_version: str
    created_at: datetime

    def to_record(self) -> dict[str, object]:
        return {
            "date": self.date.isoformat(),
            "regime": self.regime,
            "regime_score": self.regime_score,
            "risk_on_score": self.risk_on_score,
            "risk_off_score": self.risk_off_score,
            "growth_leadership_score": self.growth_leadership_score,
            "semiconductor_leadership_score": self.semiconductor_leadership_score,
            "reason_codes": json.dumps(list(self.reason_codes), ensure_ascii=False),
            "model_version": self.model_version,
            "created_at": self.created_at.isoformat(),
        }


@dataclass(frozen=True)
class ETFAllocationRecord:
    date: date
    symbol: str
    target_weight: float
    previous_weight: float | None
    trade_delta: float | None
    composite_score: float | None
    regime: str
    reason_codes: tuple[str, ...]
    constraints_applied: tuple[str, ...]
    model_version: str
    config_hash: str
    data_quality_status: str
    created_at: datetime
    constraint_diagnostics: tuple[dict[str, object], ...] = ()

    def to_record(self) -> dict[str, object]:
        return {
            "date": self.date.isoformat(),
            "symbol": self.symbol,
            "target_weight": self.target_weight,
            "previous_weight": self.previous_weight,
            "trade_delta": self.trade_delta,
            "composite_score": self.composite_score,
            "regime": self.regime,
            "reason_codes": json.dumps(list(self.reason_codes), ensure_ascii=False),
            "constraints_applied": json.dumps(list(self.constraints_applied), ensure_ascii=False),
            "constraint_diagnostics": json.dumps(
                list(self.constraint_diagnostics),
                ensure_ascii=False,
                sort_keys=True,
            ),
            "model_version": self.model_version,
            "config_hash": self.config_hash,
            "data_quality_status": self.data_quality_status,
            "created_at": self.created_at.isoformat(),
        }


def load_etf_config_bundle(
    assets_path: Path = DEFAULT_ETF_ASSETS_CONFIG_PATH,
    strategy_path: Path = DEFAULT_ETF_STRATEGY_CONFIG_PATH,
    risk_path: Path = DEFAULT_ETF_RISK_CONFIG_PATH,
    backtest_path: Path = DEFAULT_ETF_BACKTEST_CONFIG_PATH,
    p1_path: Path = DEFAULT_ETF_P1_CONFIG_PATH,
    p2_path: Path = DEFAULT_ETF_P2_CONFIG_PATH,
) -> ETFConfigBundle:
    raw_assets = _load_yaml(assets_path)
    raw_strategy = _load_yaml(strategy_path)
    raw_risk = _load_yaml(risk_path)
    raw_backtest = _load_yaml(backtest_path)
    raw_p1 = _load_yaml(p1_path) if p1_path.exists() else None
    raw_p2 = _load_yaml(p2_path) if p2_path.exists() else None
    config_hash = _config_hash(
        {
            "assets": raw_assets,
            "strategy": raw_strategy,
            "risk": raw_risk,
            "backtest": raw_backtest,
            "p1": raw_p1,
            "p2": raw_p2,
        }
    )
    return ETFConfigBundle(
        assets=ETFAssetsConfig.model_validate(raw_assets),
        strategy=ETFStrategyConfig.model_validate(raw_strategy),
        risk=ETFRiskConfig.model_validate(raw_risk),
        backtest=ETFBacktestConfig.model_validate(raw_backtest),
        p1=ETFP1Config.model_validate(raw_p1) if raw_p1 is not None else None,
        p2=ETFP2Config.model_validate(raw_p2) if raw_p2 is not None else None,
        config_hash=config_hash,
    )


def _load_yaml(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError(f"ETF config must be a YAML mapping: {path}")
    return raw


def _config_hash(payload: dict[str, Any]) -> str:
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return sha256(normalized.encode("utf-8")).hexdigest()


def dataframe_checksum(records: list[dict[str, object]]) -> str:
    normalized = json.dumps(records, ensure_ascii=False, sort_keys=True, default=str)
    return sha256(normalized.encode("utf-8")).hexdigest()


def json_dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
