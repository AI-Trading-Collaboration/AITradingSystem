from __future__ import annotations

import json
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any, Self

from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.models import (
    ETFConfigBundle,
    PolicyMetadata,
    load_etf_config_bundle,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "weight_search.yaml"
)

WEIGHT_SEARCH_SCHEMA_VERSION = "etf_weight_search_v1"

WEIGHT_CALIBRATION_SAFETY = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
}


class WeightCalibrationError(ValueError):
    """Raised when ETF weight calibration config violates governance requirements."""


class ETFWeightCalibrationSafety(BaseModel):
    observe_only: bool
    candidate_only: bool
    production_effect: str = Field(min_length=1)
    broker_action: str = Field(min_length=1)
    manual_review_required: bool

    @model_validator(mode="after")
    def validate_safety_boundary(self) -> Self:
        for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
            if getattr(self, field) != expected:
                raise ValueError(f"ETF weight calibration safety {field} must be {expected!r}")
        return self


class ETFWeightConstraint(BaseModel):
    min: float = Field(ge=0, le=1)
    max: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_order(self) -> Self:
        if self.min > self.max:
            raise ValueError("ETF weight constraint min must be <= max")
        return self


class ETFWeightSleeveConstraints(BaseModel):
    equity_total_max: float = Field(ge=0, le=1)
    semiconductor_total_max: float = Field(ge=0, le=1)
    cash_min_when_risk_off: float = Field(ge=0, le=1)


class ETFWeightObjectivePolicy(BaseModel):
    description: str = Field(min_length=1)
    policy_status: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    rationale: str = Field(min_length=1)
    intended_effect: str = Field(min_length=1)
    validation_evidence: str = Field(min_length=1)
    review_condition: str = Field(min_length=1)
    component_weights: dict[str, float] = Field(min_length=1)
    component_scales: dict[str, float] = Field(default_factory=dict)
    hard_blockers: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_policy_numbers(self) -> Self:
        total = sum(float(value) for value in self.component_weights.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError("ETF weight objective component_weights must sum to 1.0")
        for key, value in self.component_scales.items():
            if float(value) <= 0:
                raise ValueError(f"ETF weight objective component scale must be positive: {key}")
        normalized_blockers = [str(item).strip() for item in self.hard_blockers]
        if any(not item for item in normalized_blockers):
            raise ValueError("ETF weight objective hard_blockers must not contain blanks")
        if len(normalized_blockers) != len(set(normalized_blockers)):
            raise ValueError("ETF weight objective hard_blockers must be unique")
        self.hard_blockers = normalized_blockers
        return self


class ETFWeightBenchmarkSet(BaseModel):
    description: str = Field(min_length=1)
    benchmark_ids: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_unique_benchmarks(self) -> Self:
        normalized = [str(item).strip() for item in self.benchmark_ids]
        if any(not item for item in normalized):
            raise ValueError("ETF weight benchmark set ids must not be blank")
        if len(normalized) != len(set(normalized)):
            raise ValueError("ETF weight benchmark set ids must be unique")
        self.benchmark_ids = normalized
        return self


class ETFWalkForwardWindow(BaseModel):
    window_id: str = Field(min_length=1)
    start_date: date
    end_date: date
    description: str = ""

    @model_validator(mode="after")
    def validate_date_order(self) -> Self:
        if self.start_date > self.end_date:
            raise ValueError("ETF walk-forward window start_date must be <= end_date")
        return self


class ETFRegimeSplitConfig(BaseModel):
    description: str = Field(min_length=1)
    start_date: date | None = None
    end_date: date | None = None
    regime_label: str | None = None
    signal: str | None = None

    @model_validator(mode="after")
    def validate_date_order(self) -> Self:
        if self.start_date is not None and self.end_date is not None:
            if self.start_date > self.end_date:
                raise ValueError("ETF regime split start_date must be <= end_date")
        if not self.regime_label and not self.signal:
            raise ValueError("ETF regime split requires regime_label or signal")
        return self


class ETFWeightSearchDefinition(BaseModel):
    search_id: str = Field(min_length=1)
    description: str = Field(min_length=1)
    universe: list[str] = Field(min_length=1)
    weight_constraints: dict[str, ETFWeightConstraint] = Field(min_length=1)
    sleeve_constraints: ETFWeightSleeveConstraints
    grid_step: float = Field(gt=0, le=0.25)
    max_candidate_count: int = Field(gt=0, le=10_000)
    objective_policy: str = Field(min_length=1)
    benchmark_set: str = Field(min_length=1)
    backtest_start_date: date
    backtest_end_date: date | None = None
    walk_forward_windows: list[ETFWalkForwardWindow] = Field(default_factory=list)
    regime_splits: dict[str, ETFRegimeSplitConfig] = Field(default_factory=dict)
    safety: ETFWeightCalibrationSafety

    @model_validator(mode="after")
    def validate_search_definition(self) -> Self:
        self.universe = _normalized_unique_symbols(self.universe, "universe")
        if "CASH" not in self.universe:
            raise ValueError("ETF weight search universe must include CASH")
        constraint_symbols = set(self.weight_constraints)
        universe_symbols = set(self.universe)
        missing = sorted(universe_symbols - constraint_symbols)
        extra = sorted(constraint_symbols - universe_symbols)
        if missing or extra:
            raise ValueError(
                "ETF weight search constraints must match universe; "
                f"missing={missing}; extra={extra}"
            )
        if self.backtest_end_date is not None and self.backtest_start_date > self.backtest_end_date:
            raise ValueError("ETF weight search backtest_start_date must be <= backtest_end_date")
        _raise_if_invalid_grid_step(self.grid_step)
        return self


class ETFWeightSearchRegistry(BaseModel):
    schema_version: str = Field(min_length=1)
    policy_metadata: PolicyMetadata
    objective_policies: dict[str, ETFWeightObjectivePolicy] = Field(min_length=1)
    benchmark_sets: dict[str, ETFWeightBenchmarkSet] = Field(min_length=1)
    weight_searches: dict[str, ETFWeightSearchDefinition] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_registry_references(self) -> Self:
        if self.schema_version != WEIGHT_SEARCH_SCHEMA_VERSION:
            raise ValueError(
                f"ETF weight search schema_version must be {WEIGHT_SEARCH_SCHEMA_VERSION}"
            )
        for key, search in self.weight_searches.items():
            if search.search_id != key:
                raise ValueError(
                    "ETF weight search mapping key must match search_id: "
                    f"{key} != {search.search_id}"
                )
            if search.objective_policy not in self.objective_policies:
                raise ValueError(
                    f"ETF weight search references unknown objective_policy: "
                    f"{search.objective_policy}"
                )
            if search.benchmark_set not in self.benchmark_sets:
                raise ValueError(
                    f"ETF weight search references unknown benchmark_set: {search.benchmark_set}"
                )
        return self

    @property
    def config_hash(self) -> str:
        return _config_hash(self.model_dump(mode="json"))


def load_weight_search_registry(
    path: Path = DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
    *,
    etf_config: ETFConfigBundle | None = None,
) -> ETFWeightSearchRegistry:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise WeightCalibrationError(f"ETF weight search config must be a YAML mapping: {path}")
    try:
        registry = ETFWeightSearchRegistry.model_validate(raw)
        validate_weight_search_registry(
            registry,
            etf_config=etf_config or load_etf_config_bundle(),
        )
    except ValueError as exc:
        raise WeightCalibrationError(str(exc)) from exc
    return registry


def load_weight_search_definition(
    search_id: str,
    path: Path = DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
    *,
    etf_config: ETFConfigBundle | None = None,
) -> ETFWeightSearchDefinition:
    registry = load_weight_search_registry(path, etf_config=etf_config)
    try:
        return registry.weight_searches[search_id]
    except KeyError as exc:
        raise WeightCalibrationError(f"unknown ETF weight search: {search_id}") from exc


def validate_weight_search_registry(
    registry: ETFWeightSearchRegistry,
    *,
    etf_config: ETFConfigBundle,
) -> None:
    available_symbols = set(etf_config.assets.assets)
    configured_benchmark_ids = set(etf_config.backtest.backtest.benchmarks)
    issues: list[str] = []
    for search in registry.weight_searches.values():
        missing_symbols = sorted(set(search.universe) - available_symbols)
        if missing_symbols:
            issues.append(
                f"{search.search_id} universe references unknown symbols: "
                f"{', '.join(missing_symbols)}"
            )
        benchmark_set = registry.benchmark_sets[search.benchmark_set]
        missing_benchmarks = sorted(set(benchmark_set.benchmark_ids) - configured_benchmark_ids)
        if missing_benchmarks:
            issues.append(
                f"{search.search_id} benchmark_set {search.benchmark_set} references "
                f"unknown ETF benchmark ids: {', '.join(missing_benchmarks)}"
            )
    if issues:
        raise WeightCalibrationError("; ".join(issues))


def weight_search_config_to_json(registry: ETFWeightSearchRegistry) -> str:
    return (
        json.dumps(registry.model_dump(mode="json"), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n"
    )


def _normalized_unique_symbols(values: list[str], field_name: str) -> list[str]:
    normalized = [str(item).strip().upper() for item in values]
    if any(not item for item in normalized):
        raise ValueError(f"ETF weight search {field_name} must not contain blank symbols")
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"ETF weight search {field_name} must be unique")
    return normalized


def _raise_if_invalid_grid_step(grid_step: float) -> None:
    inverse = 1.0 / float(grid_step)
    if abs(inverse - round(inverse)) > 1e-8:
        raise ValueError("ETF weight search grid_step must divide 1.0 exactly")


def _config_hash(payload: dict[str, Any]) -> str:
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return sha256(normalized.encode("utf-8")).hexdigest()
