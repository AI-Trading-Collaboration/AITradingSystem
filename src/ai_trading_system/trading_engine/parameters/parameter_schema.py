from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Any, Literal, Self

from pydantic import BaseModel, Field, field_validator, model_validator

WEIGHT_SUM_TOLERANCE = 1e-9

DataQualityStatus = Literal["OK", "LIMITED", "INSUFFICIENT_DATA", "FAILED"]
PromotionStatus = Literal["rejected", "watch", "candidate", "manual_review_required"]


class PositionLimits(BaseModel):
    max_single_asset_weight: float = Field(gt=0.0, le=1.0)
    max_sector_weight: float = Field(gt=0.0, le=1.0)
    min_cash_weight: float = Field(ge=0.0, le=1.0)


class ProductionParameters(BaseModel):
    version: str = Field(min_length=1)
    created_at: str = Field(min_length=1)
    owner: str = Field(default="system")
    status: str = Field(default="pilot_baseline")
    production_effect: str = Field(default="production")
    rationale: str = Field(default="")
    asset_universe: dict[str, tuple[str, ...]] | tuple[str, ...]
    decision_frequency: Literal["daily"]
    rebalance_frequency: Literal["weekly", "daily", "monthly"]
    risk_profile: str = Field(min_length=1)
    weights: dict[str, float]
    hard_gates: dict[str, dict[str, Any]] = Field(default_factory=dict)
    position_limits: PositionLimits

    @model_validator(mode="after")
    def validate_weights(self) -> Self:
        _validate_weight_mapping(self.weights)
        return self

    @field_validator("asset_universe", mode="before")
    @classmethod
    def normalize_asset_universe(cls, value: object) -> object:
        if isinstance(value, list):
            return tuple(str(item) for item in value)
        if isinstance(value, dict):
            return {
                str(key): tuple(str(item) for item in raw_items)
                for key, raw_items in value.items()
                if isinstance(raw_items, list | tuple)
            }
        return value

    def flattened_asset_universe(self) -> tuple[str, ...]:
        if isinstance(self.asset_universe, tuple):
            return tuple(dict.fromkeys(self.asset_universe))
        assets: list[str] = []
        for group_assets in self.asset_universe.values():
            assets.extend(group_assets)
        return tuple(dict.fromkeys(assets))

    def sector_for_asset(self, asset: str) -> str:
        if isinstance(self.asset_universe, tuple):
            return "core"
        for sector, assets in self.asset_universe.items():
            if asset in assets:
                return sector
        return "unclassified"


class WalkForwardConfig(BaseModel):
    train_window_days: int = Field(gt=0)
    validation_window_days: int = Field(gt=0)
    step_days: int = Field(gt=0)
    min_history_days: int = Field(gt=0)


class TransactionCostConfig(BaseModel):
    commission_bps: float = Field(ge=0.0)
    slippage_bps: float = Field(ge=0.0)
    fx_cost_bps: float = Field(ge=0.0)
    tax_model: str = Field(min_length=1)


class SearchSpaceEntry(BaseModel):
    min: float = Field(ge=0.0, le=1.0)
    max: float = Field(ge=0.0, le=1.0)
    step: float = Field(gt=0.0, le=1.0)

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        if self.max < self.min:
            raise ValueError("search space max must be greater than or equal to min")
        return self


class SearchConstraints(BaseModel):
    total_weight_sum: float = Field(gt=0.0, le=1.0)
    max_single_weight: float = Field(gt=0.0, le=1.0)
    min_single_weight: float = Field(ge=0.0, le=1.0)
    max_daily_parameter_delta: float = Field(ge=0.0, le=1.0)
    max_weekly_parameter_delta: float = Field(ge=0.0, le=1.0)


class ParameterChangeGuardrails(BaseModel):
    max_abs_change_per_weight: float = Field(ge=0.0, le=1.0)
    max_total_l1_change: float = Field(ge=0.0, le=2.0)
    require_reason_for_each_change: bool = True


class HardGateTuning(BaseModel):
    enabled: bool = False
    reason: str = Field(min_length=1)


class ShadowSearchConfig(BaseModel):
    algorithm: Literal["bounded_grid"] = "bounded_grid"
    max_candidates: int = Field(gt=0)
    hard_gate_tuning: HardGateTuning
    search_space: dict[str, SearchSpaceEntry]
    constraints: SearchConstraints
    parameter_change_guardrails: ParameterChangeGuardrails


class DataPathsConfig(BaseModel):
    prices_path: str
    rates_path: str
    download_manifest_path: str
    secondary_prices_path: str
    data_quality_report_dir: str
    signal_snapshot_dir: str = "artifacts/signal_snapshots"


class OutputPathsConfig(BaseModel):
    shadow_backtest_dir: str
    shadow_parameters_dir: str
    candidate_parameters_dir: str
    parameter_promotion_dir: str
    report_alias_dir: str


class MarketRegimeConfig(BaseModel):
    id: str = Field(min_length=1)
    anchor_event: str = Field(min_length=1)
    anchor_date: date
    default_backtest_start: date


class InsufficientHistoryRule(BaseModel):
    min_days: int = Field(gt=0)
    status: Literal["INSUFFICIENT_DATA"]


class MissingPriceDataRule(BaseModel):
    max_missing_ratio: float = Field(ge=0.0, le=1.0)
    status: Literal["LIMITED"]


class SimpleStatusRule(BaseModel):
    status: Literal["LIMITED", "FAILED", "INSUFFICIENT_DATA", "OK"]


DEFAULT_BACKTEST_INPUT_CACHE_FRESHNESS_MAX_AGE_DAYS: dict[str, int] = {
    # Pilot defaults are used only when legacy shadow configs predate the explicit
    # cache freshness policy. The repository config pins the same values for audit.
    "price_data": 3,
    "signal_snapshot": 3,
    "macro_data": 7,
}


class CacheFreshnessRule(BaseModel):
    max_age_days: dict[str, int] = Field(
        default_factory=lambda: dict(DEFAULT_BACKTEST_INPUT_CACHE_FRESHNESS_MAX_AGE_DAYS)
    )

    @field_validator("max_age_days")
    @classmethod
    def validate_max_age_days(cls, value: dict[str, int]) -> dict[str, int]:
        invalid = [name for name, days in value.items() if days < 0]
        if invalid:
            raise ValueError(
                "cache freshness max_age_days must be non-negative: " + ", ".join(invalid)
            )
        merged = dict(DEFAULT_BACKTEST_INPUT_CACHE_FRESHNESS_MAX_AGE_DAYS)
        merged.update(value)
        return merged


class DataQualityRulesConfig(BaseModel):
    insufficient_history: InsufficientHistoryRule
    missing_price_data: MissingPriceDataRule
    missing_required_asset: SimpleStatusRule
    missing_signal_snapshot: SimpleStatusRule
    cache_freshness: CacheFreshnessRule = Field(default_factory=CacheFreshnessRule)


class ShadowBacktestConfig(BaseModel):
    version: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    status: str = Field(min_length=1)
    production_effect: Literal["none"]
    manual_review_required: Literal[True]
    auto_promotion: Literal[False]
    observe_only: Literal[True]
    rationale: str = Field(min_length=1)
    intended_effect: str = Field(min_length=1)
    validation_evidence: str = Field(min_length=1)
    review_condition: str = Field(min_length=1)
    market_regime: MarketRegimeConfig
    data: DataPathsConfig
    baseline_parameters_path: str
    promotion_rules_path: str
    output: OutputPathsConfig
    walk_forward: WalkForwardConfig
    backtest_frequency: Literal["daily"]
    rebalance_frequency: Literal["weekly"]
    signal_evaluation_frequency: Literal["daily"]
    transaction_cost: TransactionCostConfig
    search: ShadowSearchConfig
    data_quality_rules: DataQualityRulesConfig
    point_in_time_status: dict[str, str]


class PromotionRulesConfig(BaseModel):
    version: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    status: str = Field(min_length=1)
    production_effect: Literal["none"]
    manual_review_required: Literal[True]
    auto_promotion: Literal[False]
    observe_only: Literal[True]
    rationale: str = Field(min_length=1)
    intended_effect: str = Field(min_length=1)
    validation_evidence: str = Field(min_length=1)
    review_condition: str = Field(min_length=1)
    promotion_status: tuple[PromotionStatus, ...]
    promotion_criteria: dict[str, Any]
    hard_rejection_rules: tuple[str, ...]

    @model_validator(mode="after")
    def validate_statuses(self) -> Self:
        required: set[PromotionStatus] = {
            "rejected",
            "watch",
            "candidate",
            "manual_review_required",
        }
        if set(self.promotion_status) != required:
            raise ValueError("promotion_status must include the supported status enum")
        return self


def _validate_weight_mapping(weights: Mapping[str, float]) -> None:
    if not weights:
        raise ValueError("weights must not be empty")
    invalid = [name for name, value in weights.items() if value < 0.0 or value > 1.0]
    if invalid:
        raise ValueError("weights must be between 0 and 1: " + ", ".join(sorted(invalid)))
    total = sum(weights.values())
    if abs(total - 1.0) > WEIGHT_SUM_TOLERANCE:
        raise ValueError(f"weights must sum to 1.0, got {total:.12f}")
