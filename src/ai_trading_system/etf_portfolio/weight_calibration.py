from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from math import prod, sqrt
from pathlib import Path
from statistics import mean, pstdev
from tempfile import TemporaryDirectory
from typing import Any, Self

import pandas as pd
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.backtest.engine import BacktestMetrics, summarize_long_only_backtest
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.backtest import calculate_portfolio_accounting_step
from ai_trading_system.etf_portfolio.data import standardize_price_frame, validate_price_data
from ai_trading_system.etf_portfolio.models import (
    ETFConfigBundle,
    ETFQualityReport,
    PolicyMetadata,
    load_etf_config_bundle,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "weight_search.yaml"
)
DEFAULT_WEIGHT_CALIBRATION_PRESET_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "weight_calibration_presets.yaml"
)
DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "weight_calibration"
)
DEFAULT_ETF_WEIGHT_CALIBRATION_DATA_DIR = (
    PROJECT_ROOT / "data" / "etf_portfolio" / "weight_calibration"
)
DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH = (
    DEFAULT_ETF_WEIGHT_CALIBRATION_DATA_DIR / "candidate_weight_registry.json"
)
DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH = (
    DEFAULT_ETF_WEIGHT_CALIBRATION_DATA_DIR / "forward_enrollments.json"
)
DEFAULT_WEIGHT_FORWARD_EVIDENCE_DIR = (
    DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR / "evidence"
)
DEFAULT_WEIGHT_OVERFIT_DIAGNOSTICS_DIR = (
    DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR / "overfit_diagnostics"
)
DEFAULT_WEIGHT_TOP_CANDIDATE_EXPORT_DIR = (
    DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR / "top_candidates"
)
DEFAULT_WEIGHT_CANDIDATE_COMPARISON_DIR = (
    DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR / "comparison"
)
DEFAULT_WEIGHT_REGIME_ROBUSTNESS_DIR = (
    DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR / "regime_robustness"
)
DEFAULT_WEIGHT_OVERFIT_EXPLANATION_DIR = (
    DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR / "overfit_explanations"
)
DEFAULT_WEIGHT_INITIAL_RECOMMENDATION_DIR = (
    DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR / "recommendations"
)
DEFAULT_WEIGHT_PROPOSAL_DIR = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR / "proposals"
DEFAULT_WEIGHT_DUAL_TRACK_REPORT_DIR = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR / "reports"
DEFAULT_WEIGHT_CALIBRATION_VALIDATION_DIR = (
    DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR / "validation"
)

WEIGHT_SEARCH_SCHEMA_VERSION = "etf_weight_search_v1"
WEIGHT_CALIBRATION_PRESET_SCHEMA_VERSION = "etf_weight_calibration_presets_v1"
WEIGHT_SEARCH_RUN_SCHEMA_VERSION = "etf_weight_search_run_v1"
CANDIDATE_WEIGHT_REGISTRY_SCHEMA_VERSION = "etf_candidate_weight_registry_v1"
WEIGHT_FORWARD_ENROLLMENT_SCHEMA_VERSION = "etf_weight_forward_enrollment_v1"
WEIGHT_BACKTEST_FORWARD_EVIDENCE_SCHEMA_VERSION = (
    "etf_weight_backtest_forward_evidence_v1"
)
WEIGHT_OVERFIT_DIAGNOSTICS_SCHEMA_VERSION = "etf_weight_overfit_diagnostics_v1"
WEIGHT_TOP_CANDIDATE_EXPORT_SCHEMA_VERSION = "etf_weight_top_candidate_export_v1"
WEIGHT_CANDIDATE_COMPARISON_SCHEMA_VERSION = "etf_weight_candidate_comparison_v1"
WEIGHT_REGIME_ROBUSTNESS_SCHEMA_VERSION = "etf_weight_regime_robustness_v1"
WEIGHT_OVERFIT_EXPLANATION_SCHEMA_VERSION = "etf_weight_overfit_explanation_v1"
WEIGHT_INITIAL_RECOMMENDATION_SCHEMA_VERSION = (
    "etf_weight_initial_recommendation_report_v1"
)
WEIGHT_PROPOSAL_SCHEMA_VERSION = "etf_weight_candidate_proposals_v1"
WEIGHT_DUAL_TRACK_REPORT_SCHEMA_VERSION = "etf_weight_dual_track_calibration_report_v1"
WEIGHT_CALIBRATION_VALIDATION_SCHEMA_VERSION = "etf_weight_dual_track_validation_v1"

WEIGHT_CALIBRATION_SAFETY = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
}

ALLOWED_CANDIDATE_WEIGHT_STATUSES = frozenset(
    {"candidate", "shadow_ready", "blocked", "rejected", "needs_more_data"}
)
FORWARD_ENROLLABLE_CANDIDATE_STATUSES = frozenset({"candidate", "shadow_ready"})
ALLOWED_WEIGHT_FORWARD_ENROLLMENT_STATUSES = frozenset(
    {"active", "needs_more_forward_data", "paused", "archived"}
)
WEIGHT_FORWARD_EVIDENCE_STATUSES = frozenset(
    {
        "consistent",
        "forward_better_than_backtest",
        "forward_worse_than_backtest",
        "needs_more_forward_data",
        "mixed",
        "blocked",
    }
)

# Pilot comparison policy for TRADING-071F evidence triage only. These thresholds do
# not approve baseline changes; TRADING-071G/H/K add later risk/proposal gates.
WEIGHT_FORWARD_EVIDENCE_POLICY = {
    "policy_id": "etf_weight_backtest_forward_evidence_v1",
    "owner": "TRADING-071F",
    "status": "pilot_baseline",
    "rationale": (
        "Compare historical expectations with real forward evidence without treating "
        "either side as production approval."
    ),
    "min_forward_days": 20,
    "return_gap_tolerance": 0.02,
    "drawdown_gap_tolerance": 0.02,
    "turnover_gap_tolerance": 0.25,
    "stability_gap_tolerance": 0.10,
}

# Pilot diagnostics policy for TRADING-071G. Scores are explainability/risk triage
# inputs only and cannot promote or apply ETF weights.
WEIGHT_OVERFIT_DIAGNOSTICS_POLICY = {
    "policy_id": "etf_weight_overfit_diagnostics_v1",
    "owner": "TRADING-071G",
    "status": "pilot_baseline",
    "rationale": (
        "Flag historically attractive ETF weight candidates that may be unstable, "
        "over-concentrated, benchmark-dependent, or diverging forward."
    ),
    "component_weights": {
        "performance_concentration": 0.15,
        "single_period_dependency": 0.15,
        "regime_fragility": 0.15,
        "turnover_instability": 0.10,
        "constraint_hit_instability": 0.10,
        "weight_extremeness": 0.10,
        "benchmark_dependency": 0.10,
        "forward_backtest_divergence": 0.15,
    },
    "risk_band_thresholds": {
        "medium": 0.25,
        "high": 0.50,
        "critical": 0.75,
    },
    "turnover_reference": 1.0,
    "constraint_hit_reference": 0.25,
    "performance_concentration_reference": 0.70,
    "weight_extremeness_reference": 0.70,
}

ALLOWED_WEIGHT_PROPOSAL_TYPES = frozenset(
    {
        "continue_forward_observation",
        "reject_weight_set",
        "defer_until_more_forward_data",
        "propose_extended_shadow",
        "propose_manual_baseline_review",
    }
)
DISALLOWED_WEIGHT_PROPOSAL_TYPES = frozenset(
    {"apply_weight_set", "promote_to_production", "enable_broker_action"}
)

# Operational fail-closed bounds for TRADING-071K. These do not rank candidates;
# they only reject search definitions that are too broad to audit as a validation gate.
MIN_BOUNDED_WEIGHT_SEARCH_GRID_STEP = 0.01
MAX_BOUNDED_WEIGHT_SEARCH_CANDIDATES = 10_000

# Pilot proposal policy for TRADING-071H. It only routes candidate-only review
# actions and cannot apply weights or approve production changes.
WEIGHT_PROPOSAL_POLICY = {
    "policy_id": "etf_weight_candidate_proposals_v1",
    "owner": "TRADING-071H",
    "status": "pilot_baseline",
    "historical_score_manual_review_floor": 0.55,
    "high_overfit_bands": ["high", "critical"],
}


class WeightCalibrationError(ValueError):
    """Raised when ETF weight calibration config violates governance requirements."""


@dataclass(frozen=True)
class ETFWeightSearchRun:
    run_id: str
    payload: dict[str, Any]


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
    hard_blocker_thresholds: dict[str, float] = Field(default_factory=dict)
    hard_blockers: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_policy_numbers(self) -> Self:
        total = sum(float(value) for value in self.component_weights.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError("ETF weight objective component_weights must sum to 1.0")
        for key, value in self.component_scales.items():
            if float(value) <= 0:
                raise ValueError(f"ETF weight objective component scale must be positive: {key}")
        for key, value in self.hard_blocker_thresholds.items():
            if float(value) < 0:
                raise ValueError(
                    f"ETF weight objective hard blocker threshold must be non-negative: {key}"
                )
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


class ETFWeightCalibrationPreset(BaseModel):
    preset_id: str = Field(min_length=1)
    description: str = Field(min_length=1)
    start_date: str = Field(min_length=1)
    end_date_policy: str = Field(min_length=1)
    minimum_required_assets: list[str] = Field(min_length=1)
    minimum_coverage_ratio: float = Field(gt=0, le=1)
    benchmark_set: str = Field(min_length=1)
    intended_use: str = Field(min_length=1)
    safety: ETFWeightCalibrationSafety

    @model_validator(mode="after")
    def validate_preset(self) -> Self:
        self.minimum_required_assets = _normalized_unique_symbols(
            self.minimum_required_assets,
            "minimum_required_assets",
        )
        if not _valid_preset_start_date_policy(self.start_date):
            raise ValueError(
                "ETF weight calibration preset start_date must be YYYY-MM-DD, "
                "rolling_<N>y, or earliest_available"
            )
        if not _valid_preset_end_date_policy(self.end_date_policy):
            raise ValueError(
                "ETF weight calibration preset end_date_policy must be "
                "latest_available_or_as_of, as_of, or fixed:YYYY-MM-DD"
            )
        if self.end_date_policy.startswith("fixed:"):
            resolved = resolve_weight_calibration_preset(self)
            if resolved["start_date"] > resolved["end_date"]:
                raise ValueError("ETF weight calibration preset start_date must be <= end_date")
        return self


class ETFWeightCalibrationPresetRegistry(BaseModel):
    schema_version: str = Field(min_length=1)
    policy_metadata: PolicyMetadata
    presets: dict[str, ETFWeightCalibrationPreset] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_registry(self) -> Self:
        if self.schema_version != WEIGHT_CALIBRATION_PRESET_SCHEMA_VERSION:
            raise ValueError(
                "ETF weight calibration preset schema_version must be "
                f"{WEIGHT_CALIBRATION_PRESET_SCHEMA_VERSION}"
            )
        for key, preset in self.presets.items():
            if preset.preset_id != key:
                raise ValueError(
                    "ETF weight calibration preset mapping key must match preset_id: "
                    f"{key} != {preset.preset_id}"
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


def load_weight_calibration_preset_registry(
    path: Path = DEFAULT_WEIGHT_CALIBRATION_PRESET_CONFIG_PATH,
    *,
    etf_config: ETFConfigBundle | None = None,
    weight_search_registry: ETFWeightSearchRegistry | None = None,
) -> ETFWeightCalibrationPresetRegistry:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise WeightCalibrationError(
            f"ETF weight calibration presets config must be a YAML mapping: {path}"
        )
    try:
        registry = ETFWeightCalibrationPresetRegistry.model_validate(raw)
        validate_weight_calibration_preset_registry(
            registry,
            etf_config=etf_config or load_etf_config_bundle(),
            weight_search_registry=weight_search_registry or load_weight_search_registry(),
        )
    except ValueError as exc:
        raise WeightCalibrationError(str(exc)) from exc
    return registry


def load_weight_calibration_preset(
    preset_id: str,
    path: Path = DEFAULT_WEIGHT_CALIBRATION_PRESET_CONFIG_PATH,
    *,
    etf_config: ETFConfigBundle | None = None,
    weight_search_registry: ETFWeightSearchRegistry | None = None,
) -> ETFWeightCalibrationPreset:
    registry = load_weight_calibration_preset_registry(
        path,
        etf_config=etf_config,
        weight_search_registry=weight_search_registry,
    )
    try:
        return registry.presets[preset_id]
    except KeyError as exc:
        raise WeightCalibrationError(
            f"unknown ETF weight calibration preset: {preset_id}"
        ) from exc


def validate_weight_calibration_preset_registry(
    registry: ETFWeightCalibrationPresetRegistry,
    *,
    etf_config: ETFConfigBundle,
    weight_search_registry: ETFWeightSearchRegistry,
) -> None:
    available_symbols = set(etf_config.assets.assets) | {"CASH"}
    benchmark_sets = set(weight_search_registry.benchmark_sets)
    issues = []
    for preset in registry.presets.values():
        missing_symbols = sorted(set(preset.minimum_required_assets) - available_symbols)
        if missing_symbols:
            issues.append(
                f"{preset.preset_id} minimum_required_assets references unknown symbols: "
                f"{', '.join(missing_symbols)}"
            )
        if preset.benchmark_set not in benchmark_sets:
            issues.append(
                f"{preset.preset_id} references unknown benchmark_set: "
                f"{preset.benchmark_set}"
            )
    if issues:
        raise WeightCalibrationError("; ".join(issues))


def resolve_weight_calibration_preset(
    preset: ETFWeightCalibrationPreset,
    *,
    as_of: date | None = None,
    available_start: date | None = None,
    available_end: date | None = None,
) -> dict[str, Any]:
    end_date = _resolve_preset_end_date(
        preset.end_date_policy,
        as_of=as_of,
        available_end=available_end,
    )
    start_date = _resolve_preset_start_date(
        preset.start_date,
        end_date=end_date,
        available_start=available_start,
    )
    if start_date > end_date:
        raise WeightCalibrationError(
            f"ETF weight calibration preset {preset.preset_id} resolves to invalid range: "
            f"{start_date.isoformat()} > {end_date.isoformat()}"
        )
    return {
        "preset_id": preset.preset_id,
        "description": preset.description,
        "start_date": start_date,
        "end_date": end_date,
        "end_date_policy": preset.end_date_policy,
        "minimum_required_assets": list(preset.minimum_required_assets),
        "minimum_coverage_ratio": preset.minimum_coverage_ratio,
        "benchmark_set": preset.benchmark_set,
        "intended_use": preset.intended_use,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }


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


def generate_weight_candidates(
    search: ETFWeightSearchDefinition,
    *,
    etf_config: ETFConfigBundle,
    max_candidates: int | None = None,
) -> tuple[list[dict[str, float]], dict[str, Any]]:
    candidate_limit = int(max_candidates or search.max_candidate_count)
    if candidate_limit <= 0:
        raise WeightCalibrationError("ETF weight search max_candidates must be positive")
    if candidate_limit > search.max_candidate_count:
        raise WeightCalibrationError(
            "ETF weight search max_candidates cannot exceed config max_candidate_count"
        )
    units = int(round(1.0 / search.grid_step))
    constraints = {
        symbol: (
            int(round(search.weight_constraints[symbol].min * units)),
            int(round(search.weight_constraints[symbol].max * units)),
        )
        for symbol in search.universe
    }
    non_cash_symbols = [symbol for symbol in search.universe if symbol != "CASH"]
    semiconductor_symbols = {
        symbol
        for symbol in non_cash_symbols
        if etf_config.assets.assets[symbol].risk_group == "semiconductor"
    }
    valid_candidates: list[dict[str, float]] = []

    def visit(index: int, current: dict[str, int]) -> None:
        if index == len(non_cash_symbols):
            cash_units = units - sum(current.values())
            cash_min, cash_max = constraints["CASH"]
            if cash_units < cash_min or cash_units > cash_max:
                return
            candidate_units = {**current, "CASH": cash_units}
            candidate = {
                symbol: round(candidate_units[symbol] / units, 10)
                for symbol in search.universe
            }
            if not _candidate_satisfies_sleeves(
                candidate,
                search=search,
                semiconductor_symbols=semiconductor_symbols,
            ):
                return
            valid_candidates.append(candidate)
            return
        symbol = non_cash_symbols[index]
        min_units, max_units = constraints[symbol]
        for value in range(min_units, max_units + 1):
            current[symbol] = value
            if sum(current.values()) <= units:
                visit(index + 1, current)
        current.pop(symbol, None)

    visit(0, {})
    baseline_weights = _default_weights(etf_config, search.universe)
    valid_candidates.sort(key=lambda weights: _candidate_selection_key(weights, baseline_weights))
    selected = valid_candidates[:candidate_limit]
    return selected, {
        "selection_method": "deterministic_bounded_grid_baseline_proximity_v1",
        "grid_step": search.grid_step,
        "total_valid_candidate_count": len(valid_candidates),
        "evaluated_candidate_count": len(selected),
        "max_candidate_count": search.max_candidate_count,
        "effective_candidate_limit": candidate_limit,
        "truncated_by_candidate_limit": len(valid_candidates) > len(selected),
    }


def run_historical_weight_search(
    prices: pd.DataFrame,
    *,
    etf_config: ETFConfigBundle,
    quality_report: ETFQualityReport,
    registry: ETFWeightSearchRegistry,
    search_id: str,
    start: date | None = None,
    end: date | None = None,
    range_preset: Mapping[str, Any] | None = None,
    max_candidates: int | None = None,
    generated_at: datetime | None = None,
) -> ETFWeightSearchRun:
    if not quality_report.passed:
        raise WeightCalibrationError(
            f"ETF weight calibration requires passed data quality gate: {quality_report.status}"
        )
    try:
        search = registry.weight_searches[search_id]
    except KeyError as exc:
        raise WeightCalibrationError(f"unknown ETF weight search: {search_id}") from exc
    generated = generated_at or datetime.now(UTC)
    run_start = start or search.backtest_start_date
    run_end = end or search.backtest_end_date or _latest_price_date(prices)
    if run_start >= run_end:
        raise WeightCalibrationError("ETF weight search start date must be before end date")
    candidates, generation = generate_weight_candidates(
        search,
        etf_config=etf_config,
        max_candidates=max_candidates,
    )
    baseline_weights = _default_weights(etf_config, search.universe)
    baseline = _run_static_weight_backtest(
        prices,
        weights=baseline_weights,
        config=etf_config,
        start=run_start,
        end=run_end,
    )
    benchmark_results = _run_benchmark_set_backtests(
        prices,
        config=etf_config,
        registry=registry,
        search=search,
        start=run_start,
        end=run_end,
    )
    objective = registry.objective_policies[search.objective_policy]
    baseline_daily = baseline["daily"]
    ranked_rows = []
    candidate_weight_sets = []
    metrics_rows = []
    robustness_payloads = []
    for index, weights in enumerate(candidates, start=1):
        candidate_id = f"weight_set_{index:04d}"
        backtest = _run_static_weight_backtest(
            prices,
            weights=weights,
            config=etf_config,
            start=run_start,
            end=run_end,
        )
        robustness = build_weight_robustness_evaluation(
            candidate_id=candidate_id,
            candidate_daily=backtest["daily"],
            baseline_daily=baseline_daily,
            weights=weights,
            search=search,
            etf_config=etf_config,
            objective=objective,
        )
        metric_row = _candidate_metric_row(
            candidate_id=candidate_id,
            weights=weights,
            candidate_metrics=backtest["metrics"],
            baseline_metrics=baseline["metrics"],
            benchmark_results=benchmark_results,
            objective=objective,
            search=search,
            semiconductor_symbols=_semiconductor_symbols(etf_config, search.universe),
            baseline_weights=baseline_weights,
            robustness_score=float(robustness["summary"]["stability_score"]),
            robustness_summary=robustness["summary"],
        )
        robustness_payloads.append(robustness)
        candidate_weight_sets.append(
            {
                "candidate_id": candidate_id,
                "weights": weights,
                "robustness_summary": robustness["summary"],
                "safety": dict(WEIGHT_CALIBRATION_SAFETY),
                **WEIGHT_CALIBRATION_SAFETY,
            }
        )
        metrics_rows.append(metric_row)
        ranked_rows.append(
            {
                "candidate_id": candidate_id,
                "candidate_score": metric_row["candidate_score"],
                "candidate_status": metric_row["candidate_status"],
                "hard_blockers": metric_row["hard_blockers"],
                "ranking_reason": metric_row["ranking_reason"],
                "weights": weights,
                "observe_only": True,
                "candidate_only": True,
                "production_effect": "none",
                "broker_action": "none",
                "manual_review_required": True,
            }
        )
    ranking = sorted(
        ranked_rows,
        key=lambda row: (
            0 if row["candidate_status"] == "ranked" else 1,
            -float(row["candidate_score"]),
            str(row["candidate_id"]),
        ),
    )
    for rank, row in enumerate(ranking, start=1):
        row["rank"] = rank
    rank_by_candidate = {row["candidate_id"]: row["rank"] for row in ranking}
    for row in candidate_weight_sets:
        row["rank"] = rank_by_candidate[row["candidate_id"]]
    for row in metrics_rows:
        row["rank"] = rank_by_candidate[row["candidate_id"]]
    blocked_candidates = [
        row for row in ranking if row["candidate_status"] in {"blocked", "rejected"}
    ]
    run_id = f"etf-weight-search-{generated.strftime('%Y%m%dT%H%M%SZ')}"
    payload = {
        "schema_version": WEIGHT_SEARCH_RUN_SCHEMA_VERSION,
        "report_type": "etf_weight_search_run",
        "search_run_id": run_id,
        "search_id": search.search_id,
        "search_config_hash": registry.config_hash,
        "generated_at": generated.isoformat(),
        "market_regime": etf_config.backtest.backtest.regime,
        "historical_range_preset": _range_preset_payload(range_preset or {}),
        "requested_date_range": {
            "start": run_start.isoformat(),
            "end": run_end.isoformat(),
        },
        "data_quality_status": quality_report.status,
        "baseline_weight_set": {
            "weight_set_id": "current_default_weights",
            "weights": baseline_weights,
            "metrics": _metrics_payload(baseline["metrics"]),
        },
        "candidate_generation": generation,
        "objective_policy": {
            "policy_id": search.objective_policy,
            "policy_status": objective.policy_status,
            "component_weights": dict(objective.component_weights),
            "component_scales": dict(objective.component_scales),
            "hard_blocker_thresholds": dict(objective.hard_blocker_thresholds),
            "hard_blockers": list(objective.hard_blockers),
        },
        "benchmark_set": {
            "benchmark_set_id": search.benchmark_set,
            "benchmark_ids": list(registry.benchmark_sets[search.benchmark_set].benchmark_ids),
            "benchmark_metrics": benchmark_results,
        },
        "candidate_weight_sets": candidate_weight_sets,
        "metrics": metrics_rows,
        "ranking": ranking,
        "blocked_candidates": blocked_candidates,
        "robustness_evaluation": {
            "schema_version": "etf_weight_robustness_v1",
            "evaluation_modes": [
                "full_period",
                "walk_forward_windows",
                "risk_on_periods",
                "neutral_periods",
                "risk_off_periods",
                "high_volatility_periods",
                "semiconductor_leadership_periods",
                "growth_underperformance_periods",
            ],
            "candidate_evaluations": robustness_payloads,
            "summary": _robustness_run_summary(robustness_payloads),
        },
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_weight_search_run_payload(payload)
    return ETFWeightSearchRun(run_id=run_id, payload=payload)


def validate_weight_search_run_payload(payload: Mapping[str, Any]) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_SEARCH_RUN_SCHEMA_VERSION:
        issues.append("schema_version")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(field)
    if payload.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if payload.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    generation = _mapping(payload.get("candidate_generation"))
    if int(generation.get("evaluated_candidate_count", 0)) <= 0:
        issues.append("candidate_generation.evaluated_candidate_count")
    for candidate in _records(payload.get("candidate_weight_sets")):
        weights = _mapping(candidate.get("weights"))
        if abs(sum(float(value) for value in weights.values()) - 1.0) > 1e-6:
            issues.append(f"{candidate.get('candidate_id')}.weights_sum")
        for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
            if candidate.get(field) != expected:
                issues.append(f"{candidate.get('candidate_id')}.{field}")
    if not _records(payload.get("ranking")):
        issues.append("ranking")
    if issues:
        raise WeightCalibrationError(
            "ETF weight search run payload validation failed: " + ", ".join(issues)
        )


def write_weight_search_run(
    run: ETFWeightSearchRun,
    *,
    report_root: Path = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    data_root: Path = DEFAULT_ETF_WEIGHT_CALIBRATION_DATA_DIR,
) -> dict[str, Path]:
    payload = run.payload
    validate_weight_search_run_payload(payload)
    report_dir = report_root / run.run_id
    data_dir = data_root / run.run_id
    report_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    summary_json = report_dir / "summary.json"
    summary_md = report_dir / "summary.md"
    metrics_csv = report_dir / "metrics.csv"
    ranking_json = report_dir / "ranking.json"
    robustness_json = report_dir / "robustness.json"
    candidates_json = data_dir / "candidate_weight_sets.json"
    candidates_csv = data_dir / "candidate_weight_sets.csv"
    summary_json.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_md.write_text(render_weight_search_run_markdown(payload), encoding="utf-8")
    metrics_frame = pd.DataFrame(payload["metrics"])
    metrics_frame.to_csv(metrics_csv, index=False)
    ranking_json.write_text(
        json.dumps(payload["ranking"], ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    robustness_json.write_text(
        json.dumps(
            payload["robustness_evaluation"],
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    candidates_json.write_text(
        json.dumps(
            payload["candidate_weight_sets"],
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "candidate_id": candidate["candidate_id"],
                "rank": candidate["rank"],
                **{
                    f"weight_{symbol}": weight
                    for symbol, weight in dict(candidate["weights"]).items()
                },
                "observe_only": True,
                "candidate_only": True,
                "production_effect": "none",
                "broker_action": "none",
                "manual_review_required": True,
            }
            for candidate in payload["candidate_weight_sets"]
        ]
    ).to_csv(candidates_csv, index=False)
    return {
        "report_dir": report_dir,
        "data_dir": data_dir,
        "summary_json": summary_json,
        "summary_md": summary_md,
        "metrics_csv": metrics_csv,
        "ranking_json": ranking_json,
        "robustness_json": robustness_json,
        "candidates_json": candidates_json,
        "candidates_csv": candidates_csv,
    }


def find_latest_weight_search_run_dir(output_root: Path) -> Path:
    if not output_root.exists():
        raise WeightCalibrationError(f"ETF weight search output dir does not exist: {output_root}")
    candidates = [
        item for item in output_root.iterdir() if item.is_dir() and (item / "summary.json").exists()
    ]
    if not candidates:
        raise WeightCalibrationError(f"no ETF weight search runs found under {output_root}")
    return max(candidates, key=lambda item: (item / "summary.json").stat().st_mtime)


def read_weight_search_run_payload(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "summary.json"
    if not path.exists():
        raise WeightCalibrationError(f"ETF weight search summary not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise WeightCalibrationError(f"ETF weight search summary must be a JSON object: {path}")
    validate_weight_search_run_payload(payload)
    return payload


def build_weight_top_candidate_export(
    search_payload: Mapping[str, Any],
    *,
    top: int = 10,
    overfit_payload: Mapping[str, Any] | None = None,
    source_paths: Mapping[str, str] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    validate_weight_search_run_payload(search_payload)
    if overfit_payload:
        validate_weight_overfit_diagnostics_payload(overfit_payload)
    if top <= 0:
        raise WeightCalibrationError("top must be positive")
    generated = generated_at or datetime.now(UTC)
    ranked = sorted(
        _records(search_payload.get("ranking")),
        key=lambda row: (int(row.get("rank") or 999_999), str(row.get("candidate_id"))),
    )[:top]
    candidates = [
        _weight_top_candidate_export_record(
            search_payload,
            ranked_row,
            overfit_payload=overfit_payload or {},
            generated_at=generated,
        )
        for ranked_row in ranked
    ]
    payload = {
        "schema_version": WEIGHT_TOP_CANDIDATE_EXPORT_SCHEMA_VERSION,
        "report_type": "etf_weight_top_candidate_export",
        "export_id": f"etf-weight-top-candidates-{search_payload.get('search_run_id')}",
        "search_run_id": search_payload.get("search_run_id"),
        "search_id": search_payload.get("search_id"),
        "search_config_hash": search_payload.get("search_config_hash"),
        "generated_at": generated.isoformat(),
        "top_n": top,
        "exported_candidate_count": len(candidates),
        "market_regime": search_payload.get("market_regime"),
        "historical_range_preset": dict(_mapping(search_payload.get("historical_range_preset"))),
        "requested_date_range": dict(_mapping(search_payload.get("requested_date_range"))),
        "data_quality_status": search_payload.get("data_quality_status"),
        "benchmark_set": dict(_mapping(search_payload.get("benchmark_set"))),
        "candidates": candidates,
        "source_paths": dict(source_paths or {}),
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_weight_top_candidate_export(payload)
    return payload


def validate_weight_top_candidate_export(payload: Mapping[str, Any]) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_TOP_CANDIDATE_EXPORT_SCHEMA_VERSION:
        issues.append("schema_version")
    if payload.get("report_type") != "etf_weight_top_candidate_export":
        issues.append("report_type")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(field)
    safety = _mapping(payload.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if payload.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if payload.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    candidates = _records(payload.get("candidates"))
    if int(payload.get("exported_candidate_count") or 0) != len(candidates):
        issues.append("exported_candidate_count")
    for candidate in candidates:
        validate_weight_top_candidate_export_record(candidate)
    if issues:
        raise WeightCalibrationError(
            "ETF top candidate export validation failed: "
            + ", ".join(str(issue) for issue in issues)
        )


def validate_weight_top_candidate_export_record(record: Mapping[str, Any]) -> None:
    issues = []
    required = {
        "rank",
        "weight_set_id",
        "weights",
        "historical_score",
        "cagr",
        "max_drawdown",
        "sharpe",
        "sortino",
        "calmar",
        "turnover",
        "semiconductor_exposure",
        "cash_exposure",
        "benchmark_excess_return",
        "drawdown_reduction_vs_QQQ",
        "overfit_risk",
        "forward_readiness_status",
        "blockers",
        "warnings",
        "safety",
    }
    missing = sorted(required - set(record))
    if missing:
        issues.extend(missing)
    weights = _json_float_mapping(record.get("weights"))
    if weights and abs(sum(weights.values()) - 1.0) > 1e-6:
        issues.append("weights_sum")
    if record.get("forward_readiness_status") not in {
        "shadow_ready",
        "needs_manual_review",
        "needs_more_historical_validation",
        "blocked_by_risk",
        "blocked_by_data_quality",
        "blocked_by_overfit_risk",
    }:
        issues.append("forward_readiness_status")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if record.get(field) != expected:
            issues.append(field)
    safety = _mapping(record.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if issues:
        raise WeightCalibrationError(
            "ETF top candidate export record validation failed: "
            + ", ".join(str(issue) for issue in issues)
        )


def write_weight_top_candidate_export(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_TOP_CANDIDATE_EXPORT_DIR,
) -> dict[str, Path]:
    validate_weight_top_candidate_export(payload)
    run_id = _artifact_stem(str(payload.get("search_run_id") or "unknown"))
    top_n = int(payload.get("top_n") or len(_records(payload.get("candidates"))))
    stem = f"top_weight_candidates_{run_id}_top{top_n}"
    json_path = output_dir / f"{stem}.json"
    csv_path = output_dir / f"{stem}.csv"
    markdown_path = output_dir / f"{stem}.md"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    pd.DataFrame(_weight_top_candidate_export_csv_rows(payload)).to_csv(
        csv_path,
        index=False,
    )
    markdown_path.write_text(
        render_weight_top_candidate_export_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_path, "csv": csv_path, "markdown": markdown_path}


def render_weight_top_candidate_export_markdown(payload: Mapping[str, Any]) -> str:
    requested = _mapping(payload.get("requested_date_range"))
    lines = [
        "# ETF Weight Top-N Candidate Export",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "- 本导出只列出 candidate-only historical weights，不应用权重。",
        "",
        "## Run Metadata",
        "",
        f"- Search Run ID: {payload.get('search_run_id')}",
        f"- Search ID: {payload.get('search_id')}",
        f"- Market Regime: {payload.get('market_regime')}",
        f"- Requested Date Range: {requested.get('start')} to {requested.get('end')}",
        f"- Data Quality Status: {payload.get('data_quality_status')}",
        f"- Exported Candidate Count: {payload.get('exported_candidate_count')}",
        "",
        "| Rank | Weight Set | Weights | Score | CAGR | Max DD | Sharpe | "
        "QQQ DD Reduction | Overfit | Readiness | Blockers |",
        "|---:|---|---|---:|---:|---:|---:|---:|---|---|---|",
    ]
    for candidate in _records(payload.get("candidates")):
        lines.append(
            f"| {candidate.get('rank')} | {candidate.get('weight_set_id')} | "
            f"{candidate.get('weights')} | {_fmt_number(candidate.get('historical_score'))} | "
            f"{_fmt_pct(candidate.get('cagr'))} | {_fmt_pct(candidate.get('max_drawdown'))} | "
            f"{_fmt_number(candidate.get('sharpe'))} | "
            f"{_fmt_pct(candidate.get('drawdown_reduction_vs_QQQ'))} | "
            f"{candidate.get('overfit_risk')} | "
            f"{candidate.get('forward_readiness_status')} | "
            f"{', '.join(candidate.get('blockers') or []) or 'none'} |"
        )
    if not _records(payload.get("candidates")):
        lines.append(
            "| n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | "
            "needs_more_historical_validation | none |"
        )
    return "\n".join(lines) + "\n"


def build_weight_candidate_comparison_table(
    search_payload: Mapping[str, Any],
    *,
    top_export_payload: Mapping[str, Any] | None = None,
    top: int = 10,
    source_paths: Mapping[str, str] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    validate_weight_search_run_payload(search_payload)
    if top_export_payload:
        validate_weight_top_candidate_export(top_export_payload)
    generated = generated_at or datetime.now(UTC)
    top_export = top_export_payload or build_weight_top_candidate_export(
        search_payload,
        top=top,
        generated_at=generated,
    )
    baseline = _mapping(search_payload.get("baseline_weight_set"))
    baseline_metrics = _mapping(baseline.get("metrics"))
    qqq_metrics = _benchmark_metrics_for_symbol(search_payload, "QQQ")
    rows = []
    rows.append(
        _weight_comparison_row(
            candidate_id="current_baseline",
            row_type="current_baseline",
            weights=_json_float_mapping(baseline.get("weights")),
            metrics=baseline_metrics,
            baseline_metrics=baseline_metrics,
            qqq_metrics=qqq_metrics,
            overfit_risk="not_applicable",
            forward_readiness_status="benchmark_reference",
        )
    )
    rows.extend(
        _benchmark_weight_comparison_rows(
            search_payload,
            baseline_metrics=baseline_metrics,
            qqq_metrics=qqq_metrics,
        )
    )
    rows.extend(
        _top_candidate_comparison_rows(
            search_payload,
            top_export,
            baseline_metrics=baseline_metrics,
            qqq_metrics=qqq_metrics,
        )
    )
    rows = _sort_weight_comparison_rows(rows)
    payload = {
        "schema_version": WEIGHT_CANDIDATE_COMPARISON_SCHEMA_VERSION,
        "report_type": "etf_weight_candidate_comparison",
        "comparison_id": f"etf-weight-candidate-comparison-{search_payload.get('search_run_id')}",
        "search_run_id": search_payload.get("search_run_id"),
        "search_id": search_payload.get("search_id"),
        "search_config_hash": search_payload.get("search_config_hash"),
        "generated_at": generated.isoformat(),
        "market_regime": search_payload.get("market_regime"),
        "historical_range_preset": dict(_mapping(search_payload.get("historical_range_preset"))),
        "requested_date_range": dict(_mapping(search_payload.get("requested_date_range"))),
        "data_quality_status": search_payload.get("data_quality_status"),
        "row_count": len(rows),
        "comparison_rows": rows,
        "source_paths": dict(source_paths or {}),
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_weight_candidate_comparison_table(payload)
    return payload


def validate_weight_candidate_comparison_table(payload: Mapping[str, Any]) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_CANDIDATE_COMPARISON_SCHEMA_VERSION:
        issues.append("schema_version")
    if payload.get("report_type") != "etf_weight_candidate_comparison":
        issues.append("report_type")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(field)
    safety = _mapping(payload.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    rows = _records(payload.get("comparison_rows"))
    if int(payload.get("row_count") or 0) != len(rows):
        issues.append("row_count")
    row_ids = {str(row.get("candidate_id")) for row in rows}
    for required in {"current_baseline", "buy_hold_QQQ", "buy_hold_SPY", "buy_hold_SMH"}:
        if required not in row_ids:
            issues.append(f"missing_row.{required}")
    for row in rows:
        validate_weight_candidate_comparison_row(row)
    if payload.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if payload.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    if issues:
        raise WeightCalibrationError(
            "ETF candidate comparison table validation failed: "
            + ", ".join(str(issue) for issue in issues)
        )


def validate_weight_candidate_comparison_row(row: Mapping[str, Any]) -> None:
    issues = []
    required = {
        "candidate_id",
        "weights",
        "total_return",
        "CAGR",
        "volatility",
        "max_drawdown",
        "Sharpe",
        "Sortino",
        "Calmar",
        "turnover",
        "average_cash",
        "average_semiconductor_exposure",
        "excess_return_vs_baseline",
        "excess_return_vs_QQQ",
        "drawdown_reduction_vs_QQQ",
        "overfit_risk",
        "forward_readiness_status",
        "metric_null_reasons",
        "safety",
    }
    missing = sorted(required - set(row))
    if missing:
        issues.extend(missing)
    if not _text(row.get("candidate_id")):
        issues.append("candidate_id")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if row.get(field) != expected:
            issues.append(field)
    safety = _mapping(row.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if issues:
        raise WeightCalibrationError(
            "ETF candidate comparison row validation failed: "
            + ", ".join(str(issue) for issue in issues)
        )


def write_weight_candidate_comparison_table(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_CANDIDATE_COMPARISON_DIR,
) -> dict[str, Path]:
    validate_weight_candidate_comparison_table(payload)
    run_id = _artifact_stem(str(payload.get("search_run_id") or "unknown"))
    stem = f"candidate_weight_comparison_{run_id}"
    json_path = output_dir / f"{stem}.json"
    csv_path = output_dir / f"{stem}.csv"
    markdown_path = output_dir / f"{stem}.md"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    pd.DataFrame(_weight_candidate_comparison_csv_rows(payload)).to_csv(
        csv_path,
        index=False,
    )
    markdown_path.write_text(
        render_weight_candidate_comparison_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_path, "csv": csv_path, "markdown": markdown_path}


def render_weight_candidate_comparison_markdown(payload: Mapping[str, Any]) -> str:
    requested = _mapping(payload.get("requested_date_range"))
    lines = [
        "# ETF Weight Candidate Comparison Table",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "- 本比较表只用于 candidate selection review，不应用权重。",
        "",
        "## Run Metadata",
        "",
        f"- Search Run ID: {payload.get('search_run_id')}",
        f"- Market Regime: {payload.get('market_regime')}",
        f"- Requested Date Range: {requested.get('start')} to {requested.get('end')}",
        f"- Data Quality Status: {payload.get('data_quality_status')}",
        "",
        "| Candidate | Type | Weights | Total Return | CAGR | Volatility | Max DD | "
        "Sharpe | Excess vs Baseline | Excess vs QQQ | Readiness |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in _records(payload.get("comparison_rows")):
        lines.append(
            f"| {row.get('candidate_id')} | {row.get('row_type')} | "
            f"{row.get('weights')} | {_fmt_pct(row.get('total_return'))} | "
            f"{_fmt_pct(row.get('CAGR'))} | {_fmt_pct(row.get('volatility'))} | "
            f"{_fmt_pct(row.get('max_drawdown'))} | {_fmt_number(row.get('Sharpe'))} | "
            f"{_fmt_pct(row.get('excess_return_vs_baseline'))} | "
            f"{_fmt_pct(row.get('excess_return_vs_QQQ'))} | "
            f"{row.get('forward_readiness_status')} |"
        )
    return "\n".join(lines) + "\n"


def build_weight_regime_robustness_heatmap(
    search_payload: Mapping[str, Any],
    *,
    top_export_payload: Mapping[str, Any] | None = None,
    top: int = 10,
    source_paths: Mapping[str, str] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    validate_weight_search_run_payload(search_payload)
    if top_export_payload:
        validate_weight_top_candidate_export(top_export_payload)
    generated = generated_at or datetime.now(UTC)
    top_export = top_export_payload or build_weight_top_candidate_export(
        search_payload,
        top=top,
        generated_at=generated,
    )
    matrix = _weight_regime_robustness_matrix(search_payload, top_export)
    payload = {
        "schema_version": WEIGHT_REGIME_ROBUSTNESS_SCHEMA_VERSION,
        "report_type": "etf_weight_regime_robustness_heatmap",
        "heatmap_id": f"etf-weight-regime-robustness-{search_payload.get('search_run_id')}",
        "search_run_id": search_payload.get("search_run_id"),
        "search_id": search_payload.get("search_id"),
        "search_config_hash": search_payload.get("search_config_hash"),
        "generated_at": generated.isoformat(),
        "market_regime": search_payload.get("market_regime"),
        "historical_range_preset": dict(_mapping(search_payload.get("historical_range_preset"))),
        "requested_date_range": dict(_mapping(search_payload.get("requested_date_range"))),
        "regimes": list(_required_heatmap_regimes()),
        "candidate_count": len(_records(top_export.get("candidates"))),
        "matrix_row_count": len(matrix),
        "matrix": matrix,
        "source_paths": dict(source_paths or {}),
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_weight_regime_robustness_heatmap(payload)
    return payload


def validate_weight_regime_robustness_heatmap(payload: Mapping[str, Any]) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_REGIME_ROBUSTNESS_SCHEMA_VERSION:
        issues.append("schema_version")
    if payload.get("report_type") != "etf_weight_regime_robustness_heatmap":
        issues.append("report_type")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(field)
    safety = _mapping(payload.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    matrix = _records(payload.get("matrix"))
    if int(payload.get("matrix_row_count") or 0) != len(matrix):
        issues.append("matrix_row_count")
    expected_rows = int(payload.get("candidate_count") or 0) * len(_required_heatmap_regimes())
    if len(matrix) != expected_rows:
        issues.append("candidate_regime_matrix_complete")
    for row in matrix:
        validate_weight_regime_robustness_row(row)
    if payload.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if payload.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    if issues:
        raise WeightCalibrationError(
            "ETF regime robustness heatmap validation failed: "
            + ", ".join(str(issue) for issue in issues)
        )


def validate_weight_regime_robustness_row(row: Mapping[str, Any]) -> None:
    issues = []
    required = {
        "candidate_id",
        "weight_set_id",
        "regime",
        "return",
        "excess_return_vs_baseline",
        "max_drawdown",
        "volatility",
        "Sharpe",
        "turnover",
        "constraint_hit_rate",
        "sample_count",
        "confidence_warning",
        "safety",
    }
    missing = sorted(required - set(row))
    if missing:
        issues.extend(missing)
    if row.get("regime") not in set(_required_heatmap_regimes()):
        issues.append("regime")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if row.get(field) != expected:
            issues.append(field)
    safety = _mapping(row.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if issues:
        raise WeightCalibrationError(
            "ETF regime robustness heatmap row validation failed: "
            + ", ".join(str(issue) for issue in issues)
        )


def write_weight_regime_robustness_heatmap(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_REGIME_ROBUSTNESS_DIR,
) -> dict[str, Path]:
    validate_weight_regime_robustness_heatmap(payload)
    run_id = _artifact_stem(str(payload.get("search_run_id") or "unknown"))
    stem = f"regime_robustness_heatmap_{run_id}"
    json_path = output_dir / f"{stem}.json"
    csv_path = output_dir / f"{stem}.csv"
    markdown_path = output_dir / f"{stem}.md"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    pd.DataFrame(_weight_regime_robustness_csv_rows(payload)).to_csv(
        csv_path,
        index=False,
    )
    markdown_path.write_text(
        render_weight_regime_robustness_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_path, "csv": csv_path, "markdown": markdown_path}


def render_weight_regime_robustness_markdown(payload: Mapping[str, Any]) -> str:
    table_header = (
        "| Candidate | Regime | Return | Excess vs Baseline | Max DD | "
        "Volatility | Sharpe | Turnover | Samples | Warning |"
    )
    lines = [
        "# ETF Weight Regime Robustness Heatmap Data",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "- 本 heatmap 数据只用于 robustness review，不应用权重。",
        "",
        f"- Search Run ID: {payload.get('search_run_id')}",
        f"- Matrix Rows: {payload.get('matrix_row_count')}",
        "",
        table_header,
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in _records(payload.get("matrix")):
        lines.append(
            f"| {row.get('weight_set_id')} | {row.get('regime')} | "
            f"{_fmt_pct(row.get('return'))} | "
            f"{_fmt_pct(row.get('excess_return_vs_baseline'))} | "
            f"{_fmt_pct(row.get('max_drawdown'))} | "
            f"{_fmt_pct(row.get('volatility'))} | "
            f"{_fmt_number(row.get('Sharpe'))} | "
            f"{_fmt_number(row.get('turnover'))} | "
            f"{row.get('sample_count')} | {row.get('confidence_warning')} |"
        )
    return "\n".join(lines) + "\n"


def load_candidate_weight_registry(
    path: Path = DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
) -> dict[str, Any]:
    if not path.exists():
        return _empty_candidate_weight_registry()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise WeightCalibrationError(f"candidate weight registry must be a JSON object: {path}")
    validate_candidate_weight_registry(payload)
    return payload


def write_candidate_weight_registry(
    registry: Mapping[str, Any],
    path: Path = DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
) -> Path:
    validate_candidate_weight_registry(registry)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(registry, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def register_candidate_weight_sets(
    search_payload: Mapping[str, Any],
    *,
    registry_path: Path = DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
    top: int | None = None,
    weight_set_ids: list[str] | None = None,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    validate_weight_search_run_payload(search_payload)
    registry = load_candidate_weight_registry(registry_path)
    selected = _selected_ranked_candidates(
        search_payload,
        top=top,
        weight_set_ids=weight_set_ids,
    )
    existing = {
        str(record.get("weight_set_id")): record
        for record in _records(registry.get("weight_sets"))
    }
    generated = created_at or datetime.now(UTC)
    for ranked in selected:
        record = _candidate_weight_record_from_search(
            search_payload,
            ranked,
            created_at=generated,
        )
        existing[record["weight_set_id"]] = record
    weight_sets = sorted(existing.values(), key=lambda item: str(item.get("weight_set_id")))
    registry = {
        "schema_version": CANDIDATE_WEIGHT_REGISTRY_SCHEMA_VERSION,
        "registry_type": "etf_candidate_weight_sets",
        "updated_at": generated.isoformat(),
        "candidate_count": len(weight_sets),
        "weight_sets": weight_sets,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_candidate_weight_registry(registry)
    write_candidate_weight_registry(registry, registry_path)
    return registry


def validate_candidate_weight_registry(registry: Mapping[str, Any]) -> None:
    issues = []
    if registry.get("schema_version") != CANDIDATE_WEIGHT_REGISTRY_SCHEMA_VERSION:
        issues.append("schema_version")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if registry.get(field) != expected:
            issues.append(field)
    seen = set()
    for record in _records(registry.get("weight_sets")):
        weight_set_id = str(record.get("weight_set_id") or "")
        if not weight_set_id:
            issues.append("weight_set_id")
        if weight_set_id in seen:
            issues.append(f"duplicate_weight_set_id:{weight_set_id}")
        seen.add(weight_set_id)
        try:
            validate_candidate_weight_record(record)
        except WeightCalibrationError as exc:
            issues.append(f"{weight_set_id}:{exc}")
    if issues:
        raise WeightCalibrationError(
            "ETF candidate weight registry validation failed: " + ", ".join(issues)
        )


def validate_candidate_weight_record(record: Mapping[str, Any]) -> None:
    required = (
        "weight_set_id",
        "source_search_run_id",
        "rank",
        "status",
        "weights",
        "metrics_summary",
        "robustness_summary",
        "blockers",
        "selection_reason",
        "config_hash",
        "created_at",
        "safety",
    )
    issues = [field for field in required if field not in record]
    status = str(record.get("status") or "")
    if status not in ALLOWED_CANDIDATE_WEIGHT_STATUSES:
        issues.append("status")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if record.get(field) != expected:
            issues.append(field)
    safety = _mapping(record.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    weights = _mapping(record.get("weights"))
    if abs(sum(float(value) for value in weights.values()) - 1.0) > 1e-6:
        issues.append("weights_sum")
    blockers = [str(item) for item in record.get("blockers") or []]
    if status == "shadow_ready" and blockers:
        issues.append("blocked_candidate_cannot_be_shadow_ready")
    if issues:
        raise WeightCalibrationError("; ".join(issues))


def update_candidate_weight_status(
    registry: Mapping[str, Any],
    *,
    weight_set_id: str,
    status: str,
    updated_at: datetime | None = None,
) -> dict[str, Any]:
    if status not in ALLOWED_CANDIDATE_WEIGHT_STATUSES:
        raise WeightCalibrationError(f"unsupported candidate weight status: {status}")
    payload = json.loads(json.dumps(registry, default=str))
    records = _records(payload.get("weight_sets"))
    matched = False
    for record in records:
        if str(record.get("weight_set_id")) != weight_set_id:
            continue
        record["status"] = status
        record["updated_at"] = (updated_at or datetime.now(UTC)).isoformat()
        validate_candidate_weight_record(record)
        matched = True
    if not matched:
        raise WeightCalibrationError(f"unknown candidate weight_set_id: {weight_set_id}")
    payload["weight_sets"] = records
    payload["updated_at"] = (updated_at or datetime.now(UTC)).isoformat()
    validate_candidate_weight_registry(payload)
    return payload


def load_weight_forward_enrollments(
    path: Path = DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
) -> dict[str, Any]:
    if not path.exists():
        return _empty_weight_forward_enrollment_registry()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise WeightCalibrationError(
            f"ETF weight forward enrollment registry must be a JSON object: {path}"
        )
    validate_weight_forward_enrollment_registry(payload)
    return payload


def write_weight_forward_enrollments(
    registry: Mapping[str, Any],
    path: Path = DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
) -> Path:
    validate_weight_forward_enrollment_registry(registry)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(registry, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def enroll_candidate_weights_forward(
    candidate_registry: Mapping[str, Any],
    *,
    enrollment_path: Path = DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
    top: int | None = None,
    weight_set_ids: list[str] | None = None,
    enrolled_at: datetime | None = None,
) -> dict[str, Any]:
    validate_candidate_weight_registry(candidate_registry)
    selected = _selected_candidate_weight_records(
        candidate_registry,
        top=top,
        weight_set_ids=weight_set_ids,
    )
    if not selected:
        raise WeightCalibrationError("no ETF candidate weight sets selected for enrollment")
    for record in selected:
        _raise_if_candidate_weight_forward_enrollable(record)
    existing_registry = load_weight_forward_enrollments(enrollment_path)
    existing = {
        str(record.get("weight_set_id")): record
        for record in _records(existing_registry.get("enrollments"))
    }
    generated = enrolled_at or datetime.now(UTC)
    changed = False
    selected_ids = [str(record.get("weight_set_id")) for record in selected]
    selected_by_id = {str(record.get("weight_set_id")): record for record in selected}
    for candidate in selected:
        weight_set_id = str(candidate.get("weight_set_id"))
        if weight_set_id in existing:
            continue
        enrollment = _weight_forward_enrollment_record(
            candidate,
            enrollment_path=enrollment_path,
            enrolled_at=generated,
        )
        existing[weight_set_id] = enrollment
        changed = True
    enrollments = sorted(
        existing.values(),
        key=lambda item: (int(item.get("rank") or 999_999), str(item.get("weight_set_id"))),
    )
    registry = {
        "schema_version": WEIGHT_FORWARD_ENROLLMENT_SCHEMA_VERSION,
        "registry_type": "etf_weight_forward_enrollments",
        "updated_at": generated.isoformat() if changed else existing_registry.get("updated_at"),
        "enrollment_count": len(enrollments),
        "enrollments": enrollments,
        "latest_selection": {
            "selected_at": generated.isoformat(),
            "weight_set_ids": selected_ids,
            "enrollment_results": [
                _weight_forward_enrollment_result(
                    existing[weight_set_id],
                    selected_by_id[weight_set_id],
                )
                for weight_set_id in selected_ids
            ],
        },
        "shared_shadow_registry_mutated": False,
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_weight_forward_enrollment_registry(registry)
    write_weight_forward_enrollments(registry, enrollment_path)
    return registry


def enroll_top_weight_candidates_forward(
    search_payload: Mapping[str, Any],
    *,
    top_export_payload: Mapping[str, Any] | None = None,
    comparison_payload: Mapping[str, Any] | None = None,
    source_paths: Mapping[str, str] | None = None,
    enrollment_path: Path = DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
    top: int | None = None,
    weight_set_ids: list[str] | None = None,
    enrolled_at: datetime | None = None,
) -> dict[str, Any]:
    validate_weight_search_run_payload(search_payload)
    if top_export_payload:
        validate_weight_top_candidate_export(top_export_payload)
    if comparison_payload:
        validate_weight_candidate_comparison_table(comparison_payload)
    top_payload = dict(top_export_payload or {})
    generated = enrolled_at or datetime.now(UTC)
    if not top_payload:
        top_payload = build_weight_top_candidate_export(
            search_payload,
            top=top or 3,
            generated_at=generated,
        )
    selected = _selected_top_export_candidates(
        top_payload,
        top=top,
        weight_set_ids=weight_set_ids,
    )
    if not selected:
        raise WeightCalibrationError("no ETF top weight candidates selected for enrollment")
    source_links = dict(source_paths or {})
    records = [
        _candidate_weight_record_from_shadow_ready_top_candidate(
            search_payload,
            candidate,
            source_links=source_links,
            created_at=generated,
        )
        for candidate in selected
    ]
    transient_registry = {
        "schema_version": CANDIDATE_WEIGHT_REGISTRY_SCHEMA_VERSION,
        "registry_type": "etf_candidate_weight_sets",
        "updated_at": generated.isoformat(),
        "candidate_count": len(records),
        "weight_sets": records,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_candidate_weight_registry(transient_registry)
    return enroll_candidate_weights_forward(
        transient_registry,
        enrollment_path=enrollment_path,
        top=len(records),
        enrolled_at=generated,
    )


def validate_weight_forward_enrollment_registry(registry: Mapping[str, Any]) -> None:
    issues = []
    if registry.get("schema_version") != WEIGHT_FORWARD_ENROLLMENT_SCHEMA_VERSION:
        issues.append("schema_version")
    if registry.get("registry_type") != "etf_weight_forward_enrollments":
        issues.append("registry_type")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if registry.get(field) != expected:
            issues.append(field)
    safety = _mapping(registry.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if registry.get("shared_shadow_registry_mutated") is not False:
        issues.append("shared_shadow_registry_mutated")
    if registry.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if registry.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    seen_weight_sets: set[str] = set()
    seen_shadow_ids: set[str] = set()
    enrollments = _records(registry.get("enrollments"))
    for record in enrollments:
        weight_set_id = str(record.get("weight_set_id") or "")
        shadow_id = str(record.get("shadow_id") or "")
        if weight_set_id in seen_weight_sets:
            issues.append(f"duplicate_weight_set_id:{weight_set_id}")
        if shadow_id in seen_shadow_ids:
            issues.append(f"duplicate_shadow_id:{shadow_id}")
        seen_weight_sets.add(weight_set_id)
        seen_shadow_ids.add(shadow_id)
        try:
            validate_weight_forward_enrollment_record(record)
        except WeightCalibrationError as exc:
            issues.append(f"{weight_set_id}:{exc}")
    if int(registry.get("enrollment_count") or 0) != len(enrollments):
        issues.append("enrollment_count")
    if issues:
        raise WeightCalibrationError(
            "ETF weight forward enrollment validation failed: "
            + ", ".join(str(issue) for issue in issues)
        )


def validate_weight_forward_enrollment_record(record: Mapping[str, Any]) -> None:
    required = (
        "shadow_id",
        "weight_set_id",
        "source_search_run_id",
        "source_candidate_id",
        "rank",
        "status",
        "weights",
        "metrics_summary",
        "robustness_summary",
        "blockers",
        "selection_reason",
        "config_hash",
        "enrolled_at",
        "enrollment_date",
        "forward_tracking_link",
        "tracking_state",
        "shadow_record",
        "shared_shadow_registry_mutated",
        "production_weights_mutated",
        "applied_weight_set",
        "production_promotion_allowed",
        "safety",
    )
    issues = [field for field in required if field not in record]
    status = str(record.get("status") or "")
    if status not in ALLOWED_WEIGHT_FORWARD_ENROLLMENT_STATUSES:
        issues.append("status")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if record.get(field) != expected:
            issues.append(field)
    safety = _mapping(record.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if record.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if record.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    if record.get("production_promotion_allowed") is not False:
        issues.append("production_promotion_allowed")
    weights = _mapping(record.get("weights"))
    if abs(sum(float(value) for value in weights.values()) - 1.0) > 1e-6:
        issues.append("weights_sum")
    blockers = [str(item) for item in record.get("blockers") or []]
    if blockers:
        issues.append("blocked_candidate_cannot_enroll_forward")
    shadow = _mapping(record.get("shadow_record"))
    if shadow.get("weight_set_id") != record.get("weight_set_id"):
        issues.append("shadow_record.weight_set_id")
    if shadow.get("shadow_id") != record.get("shadow_id"):
        issues.append("shadow_record.shadow_id")
    if shadow.get("status") != "active":
        issues.append("shadow_record.status")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if shadow.get(field) != expected:
            issues.append(f"shadow_record.{field}")
    tracking = _mapping(record.get("tracking_state"))
    if tracking.get("tracking_status") != "active":
        issues.append("tracking_state.tracking_status")
    if not str(record.get("forward_tracking_link") or ""):
        issues.append("forward_tracking_link")
    if issues:
        raise WeightCalibrationError("; ".join(issues))


def build_backtest_forward_evidence_aggregation(
    *,
    as_of: date,
    candidate_registry: Mapping[str, Any],
    forward_enrollments: Mapping[str, Any],
    search_payload: Mapping[str, Any] | None = None,
    forward_dashboard: Mapping[str, Any] | None = None,
    weekly_review: Mapping[str, Any] | None = None,
    decision_journal: Mapping[str, Any] | None = None,
    parameter_review: Mapping[str, Any] | None = None,
    generated_at: datetime | None = None,
    source_paths: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    validate_candidate_weight_registry(candidate_registry)
    validate_weight_forward_enrollment_registry(forward_enrollments)
    if search_payload:
        validate_weight_search_run_payload(search_payload)
    generated = generated_at or datetime.now(UTC)
    records = [
        _backtest_forward_evidence_record(
            enrollment,
            as_of=as_of,
            candidate_registry=candidate_registry,
            search_payload=search_payload or {},
            forward_dashboard=forward_dashboard or {},
            weekly_review=weekly_review or {},
            decision_journal=decision_journal or {},
            parameter_review=parameter_review or {},
            generated_at=generated,
            source_paths=source_paths or {},
        )
        for enrollment in _records(forward_enrollments.get("enrollments"))
    ]
    status_counts = _status_counts(records)
    if not records:
        status = "needs_more_forward_data"
        reason = "NO_FORWARD_ENROLLMENTS"
    elif status_counts.get("blocked"):
        status = "blocked"
        reason = "BLOCKED_CANDIDATE_EVIDENCE"
    elif status_counts.get("forward_worse_than_backtest"):
        status = "forward_worse_than_backtest"
        reason = "FORWARD_UNDERPERFORMANCE_PRESENT"
    elif status_counts.get("mixed"):
        status = "mixed"
        reason = "MIXED_FORWARD_EVIDENCE"
    elif status_counts.get("needs_more_forward_data") == len(records):
        status = "needs_more_forward_data"
        reason = "INSUFFICIENT_FORWARD_EVIDENCE"
    elif status_counts.get("forward_better_than_backtest"):
        status = "forward_better_than_backtest"
        reason = "FORWARD_OUTPERFORMANCE_PRESENT"
    else:
        status = "consistent"
        reason = "FORWARD_EVIDENCE_WITHIN_TOLERANCE"
    payload = {
        "schema_version": WEIGHT_BACKTEST_FORWARD_EVIDENCE_SCHEMA_VERSION,
        "report_type": "etf_weight_backtest_forward_evidence",
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": status,
        "reason": reason,
        "policy": dict(WEIGHT_FORWARD_EVIDENCE_POLICY),
        "candidate_count": len(_records(candidate_registry.get("weight_sets"))),
        "enrollment_count": len(_records(forward_enrollments.get("enrollments"))),
        "evidence_record_count": len(records),
        "status_counts": status_counts,
        "source_paths": dict(source_paths or {}),
        "evidence_records": records,
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_backtest_forward_evidence_payload(payload)
    return payload


def validate_backtest_forward_evidence_payload(payload: Mapping[str, Any]) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_BACKTEST_FORWARD_EVIDENCE_SCHEMA_VERSION:
        issues.append("schema_version")
    if payload.get("report_type") != "etf_weight_backtest_forward_evidence":
        issues.append("report_type")
    status = str(payload.get("status") or "")
    if status not in WEIGHT_FORWARD_EVIDENCE_STATUSES:
        issues.append("status")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(field)
    safety = _mapping(payload.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if payload.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if payload.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    records = _records(payload.get("evidence_records"))
    if int(payload.get("evidence_record_count") or 0) != len(records):
        issues.append("evidence_record_count")
    for record in records:
        try:
            validate_backtest_forward_evidence_record(record)
        except WeightCalibrationError as exc:
            issues.append(f"{record.get('weight_set_id')}:{exc}")
    if issues:
        raise WeightCalibrationError(
            "ETF weight backtest-forward evidence validation failed: "
            + ", ".join(str(issue) for issue in issues)
        )


def validate_backtest_forward_evidence_record(record: Mapping[str, Any]) -> None:
    required = (
        "evidence_id",
        "weight_set_id",
        "shadow_id",
        "source_search_run_id",
        "source_candidate_id",
        "rank",
        "forward_days",
        "backtest_expected_return",
        "forward_realized_return",
        "expectation_gap",
        "backtest_expected_drawdown",
        "forward_realized_drawdown",
        "drawdown_gap",
        "backtest_expected_turnover",
        "forward_realized_turnover",
        "turnover_gap",
        "backtest_expected_stability",
        "forward_realized_stability",
        "stability_gap",
        "evidence_status",
        "status_reasons",
        "source_links",
        "safety",
    )
    issues = [field for field in required if field not in record]
    status = str(record.get("evidence_status") or "")
    if status not in WEIGHT_FORWARD_EVIDENCE_STATUSES:
        issues.append("evidence_status")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if record.get(field) != expected:
            issues.append(field)
    safety = _mapping(record.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if record.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if record.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    if not isinstance(record.get("status_reasons"), list):
        issues.append("status_reasons")
    if not isinstance(record.get("source_links"), Mapping):
        issues.append("source_links")
    if status not in {"needs_more_forward_data", "blocked"}:
        for field in (
            "forward_realized_return",
            "expectation_gap",
            "forward_realized_drawdown",
            "drawdown_gap",
        ):
            if record.get(field) is None:
                issues.append(field)
    if issues:
        raise WeightCalibrationError("; ".join(issues))


def write_backtest_forward_evidence_aggregation(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_FORWARD_EVIDENCE_DIR,
) -> dict[str, Path]:
    validate_backtest_forward_evidence_payload(payload)
    as_of = str(payload.get("as_of"))
    json_path = output_dir / f"backtest_forward_evidence_{as_of}.json"
    markdown_path = output_dir / f"backtest_forward_evidence_{as_of}.md"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(
        render_backtest_forward_evidence_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_path, "markdown": markdown_path}


def render_backtest_forward_evidence_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Weight Backtest vs Forward Evidence",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "- 本报告只比较 historical expectation 与 forward evidence，不应用权重。",
        "",
        "## Summary",
        "",
        f"- Status: {payload.get('status')}",
        f"- Reason: {payload.get('reason')}",
        f"- As Of: {payload.get('as_of')}",
        f"- Evidence Records: {payload.get('evidence_record_count')}",
        f"- Status Counts: {payload.get('status_counts')}",
        "",
        "## Candidate Evidence",
        "",
        (
            "| Weight Set | Forward Days | Expected Return | Forward Return | "
            "Gap | Expected MDD | Forward MDD | Evidence Status |"
        ),
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for record in _records(payload.get("evidence_records")):
        lines.append(
            f"| {record.get('weight_set_id')} | {record.get('forward_days')} | "
            f"{_fmt_pct(record.get('backtest_expected_return'))} | "
            f"{_fmt_pct(record.get('forward_realized_return'))} | "
            f"{_fmt_pct(record.get('expectation_gap'))} | "
            f"{_fmt_pct(record.get('backtest_expected_drawdown'))} | "
            f"{_fmt_pct(record.get('forward_realized_drawdown'))} | "
            f"{record.get('evidence_status')} |"
        )
    if not _records(payload.get("evidence_records")):
        lines.append("| none | 0 | n/a | n/a | n/a | n/a | n/a | needs_more_forward_data |")
    lines.extend(["", "## Source Links", ""])
    source_paths = _mapping(payload.get("source_paths"))
    if source_paths:
        for key, value in sorted(source_paths.items()):
            lines.append(f"- {key}: `{value}`")
    else:
        lines.append("- no explicit source paths supplied")
    return "\n".join(lines) + "\n"


def build_weight_overfit_diagnostics(
    *,
    candidate_registry: Mapping[str, Any],
    search_payload: Mapping[str, Any] | None = None,
    evidence_payload: Mapping[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    validate_candidate_weight_registry(candidate_registry)
    if search_payload:
        validate_weight_search_run_payload(search_payload)
    if evidence_payload:
        validate_backtest_forward_evidence_payload(evidence_payload)
    generated = generated_at or datetime.now(UTC)
    candidate_diagnostics = [
        _weight_overfit_candidate_diagnostics(
            candidate,
            search_payload=search_payload or {},
            evidence_payload=evidence_payload or {},
        )
        for candidate in _records(candidate_registry.get("weight_sets"))
    ]
    risk_counts = _risk_band_counts(candidate_diagnostics)
    payload = {
        "schema_version": WEIGHT_OVERFIT_DIAGNOSTICS_SCHEMA_VERSION,
        "report_type": "etf_weight_overfit_diagnostics",
        "status": "available" if candidate_diagnostics else "needs_more_data",
        "generated_at": generated.isoformat(),
        "policy": dict(WEIGHT_OVERFIT_DIAGNOSTICS_POLICY),
        "candidate_count": len(candidate_diagnostics),
        "risk_counts": risk_counts,
        "highest_risk_candidate": _highest_risk_candidate(candidate_diagnostics),
        "candidate_diagnostics": candidate_diagnostics,
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_weight_overfit_diagnostics_payload(payload)
    return payload


def validate_weight_overfit_diagnostics_payload(payload: Mapping[str, Any]) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_OVERFIT_DIAGNOSTICS_SCHEMA_VERSION:
        issues.append("schema_version")
    if payload.get("report_type") != "etf_weight_overfit_diagnostics":
        issues.append("report_type")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(field)
    safety = _mapping(payload.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if payload.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if payload.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    records = _records(payload.get("candidate_diagnostics"))
    if int(payload.get("candidate_count") or 0) != len(records):
        issues.append("candidate_count")
    for record in records:
        try:
            validate_weight_overfit_candidate_diagnostics(record)
        except WeightCalibrationError as exc:
            issues.append(f"{record.get('weight_set_id')}:{exc}")
    if issues:
        raise WeightCalibrationError(
            "ETF weight overfit diagnostics validation failed: "
            + ", ".join(str(issue) for issue in issues)
        )


def validate_weight_overfit_candidate_diagnostics(record: Mapping[str, Any]) -> None:
    required = (
        "weight_set_id",
        "source_search_run_id",
        "source_candidate_id",
        "rank",
        "overfit_risk_score",
        "overfit_risk_band",
        "component_diagnostics",
        "reason_codes",
        "safety",
    )
    issues = [field for field in required if field not in record]
    band = str(record.get("overfit_risk_band") or "")
    if band not in {"low", "medium", "high", "critical"}:
        issues.append("overfit_risk_band")
    score = _float_or_none(record.get("overfit_risk_score"))
    if score is None or score < 0.0 or score > 1.0:
        issues.append("overfit_risk_score")
    components = _mapping(record.get("component_diagnostics"))
    expected = set(WEIGHT_OVERFIT_DIAGNOSTICS_POLICY["component_weights"])
    if set(components) != expected:
        issues.append("component_diagnostics")
    for field, expected_value in WEIGHT_CALIBRATION_SAFETY.items():
        if record.get(field) != expected_value:
            issues.append(field)
    safety = _mapping(record.get("safety"))
    for field, expected_value in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected_value:
            issues.append(f"safety.{field}")
    if issues:
        raise WeightCalibrationError("; ".join(issues))


def write_weight_overfit_diagnostics(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_OVERFIT_DIAGNOSTICS_DIR,
) -> dict[str, Path]:
    validate_weight_overfit_diagnostics_payload(payload)
    timestamp = str(payload.get("generated_at", "unknown")).replace(":", "").replace("+", "")
    stem = f"overfit_diagnostics_{timestamp[:15]}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(render_weight_overfit_diagnostics_markdown(payload), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}


def render_weight_overfit_diagnostics_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Weight Overfit Diagnostics",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "- 本报告只标记 overfit/stability risk，不应用权重。",
        "",
        "## Summary",
        "",
        f"- Status: {payload.get('status')}",
        f"- Candidate Count: {payload.get('candidate_count')}",
        f"- Risk Counts: {payload.get('risk_counts')}",
        f"- Highest Risk Candidate: {payload.get('highest_risk_candidate')}",
        "",
        "## Candidate Diagnostics",
        "",
        "| Weight Set | Rank | Risk Score | Risk Band | Reasons |",
        "|---|---:|---:|---|---|",
    ]
    for record in _records(payload.get("candidate_diagnostics")):
        lines.append(
            f"| {record.get('weight_set_id')} | {record.get('rank')} | "
            f"{_fmt_number(record.get('overfit_risk_score'))} | "
            f"{record.get('overfit_risk_band')} | "
            f"{', '.join(str(item) for item in record.get('reason_codes') or [])} |"
        )
    if not _records(payload.get("candidate_diagnostics")):
        lines.append("| none | 0 | n/a | needs_more_data | no candidate registry records |")
    return "\n".join(lines) + "\n"


def weight_overfit_risk_band(score: float) -> str:
    thresholds = _mapping(WEIGHT_OVERFIT_DIAGNOSTICS_POLICY["risk_band_thresholds"])
    if score >= float(thresholds["critical"]):
        return "critical"
    if score >= float(thresholds["high"]):
        return "high"
    if score >= float(thresholds["medium"]):
        return "medium"
    return "low"


def build_weight_overfit_explanations(
    search_payload: Mapping[str, Any],
    *,
    top_export_payload: Mapping[str, Any] | None = None,
    overfit_payload: Mapping[str, Any] | None = None,
    top: int = 10,
    source_paths: Mapping[str, str] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    validate_weight_search_run_payload(search_payload)
    if top_export_payload:
        validate_weight_top_candidate_export(top_export_payload)
    if overfit_payload:
        validate_weight_overfit_diagnostics_payload(overfit_payload)
    if top <= 0:
        raise WeightCalibrationError("top must be positive")
    generated = generated_at or datetime.now(UTC)
    top_payload = dict(top_export_payload or {})
    if not top_payload:
        top_payload = build_weight_top_candidate_export(
            search_payload,
            top=top,
            overfit_payload=overfit_payload,
            generated_at=generated,
        )
    explanations = [
        _weight_overfit_explanation_record(
            search_payload,
            candidate,
            overfit_payload=overfit_payload or {},
            generated_at=generated,
        )
        for candidate in _records(top_payload.get("candidates"))[:top]
    ]
    payload = {
        "schema_version": WEIGHT_OVERFIT_EXPLANATION_SCHEMA_VERSION,
        "report_type": "etf_weight_overfit_explanation",
        "status": "available" if explanations else "needs_more_data",
        "generated_at": generated.isoformat(),
        "search_run_id": search_payload.get("search_run_id"),
        "search_id": search_payload.get("search_id"),
        "search_config_hash": search_payload.get("search_config_hash"),
        "top_n": top,
        "candidate_count": len(explanations),
        "market_regime": search_payload.get("market_regime"),
        "historical_range_preset": dict(_mapping(search_payload.get("historical_range_preset"))),
        "requested_date_range": dict(_mapping(search_payload.get("requested_date_range"))),
        "data_quality_status": search_payload.get("data_quality_status"),
        "policy": dict(WEIGHT_OVERFIT_DIAGNOSTICS_POLICY),
        "explanations": explanations,
        "source_paths": dict(source_paths or {}),
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_weight_overfit_explanations(payload)
    return payload


def validate_weight_overfit_explanations(payload: Mapping[str, Any]) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_OVERFIT_EXPLANATION_SCHEMA_VERSION:
        issues.append("schema_version")
    if payload.get("report_type") != "etf_weight_overfit_explanation":
        issues.append("report_type")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(field)
    safety = _mapping(payload.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if payload.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if payload.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    records = _records(payload.get("explanations"))
    if int(payload.get("candidate_count") or 0) != len(records):
        issues.append("candidate_count")
    for record in records:
        try:
            validate_weight_overfit_explanation_record(record)
        except WeightCalibrationError as exc:
            issues.append(f"{record.get('weight_set_id')}:{exc}")
    if issues:
        raise WeightCalibrationError(
            "ETF weight overfit explanation validation failed: "
            + ", ".join(str(issue) for issue in issues)
        )


def validate_weight_overfit_explanation_record(record: Mapping[str, Any]) -> None:
    required = {
        "candidate_id",
        "weight_set_id",
        "source_candidate_id",
        "rank",
        "overfit_risk_score",
        "overfit_risk_band",
        "top_overfit_reasons",
        "supporting_metrics",
        "blocking_metrics",
        "manual_review_note",
        "safety",
    }
    issues = sorted(required - set(record))
    if record.get("overfit_risk_band") not in {"low", "medium", "high", "critical"}:
        issues.append("overfit_risk_band")
    score = _float_or_none(record.get("overfit_risk_score"))
    if score is None or score < 0.0 or score > 1.0:
        issues.append("overfit_risk_score")
    if not _records(record.get("top_overfit_reasons")):
        issues.append("top_overfit_reasons")
    if not isinstance(record.get("blocking_metrics"), list):
        issues.append("blocking_metrics")
    supporting = _mapping(record.get("supporting_metrics"))
    expected_components = set(WEIGHT_OVERFIT_DIAGNOSTICS_POLICY["component_weights"])
    if set(supporting) != expected_components:
        issues.append("supporting_metrics")
    if not str(record.get("manual_review_note") or "").strip():
        issues.append("manual_review_note")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if record.get(field) != expected:
            issues.append(field)
    safety = _mapping(record.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if issues:
        raise WeightCalibrationError("; ".join(str(issue) for issue in issues))


def write_weight_overfit_explanations(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_OVERFIT_EXPLANATION_DIR,
) -> dict[str, Path]:
    validate_weight_overfit_explanations(payload)
    run_id = _artifact_stem(str(payload.get("search_run_id") or "unknown"))
    stem = f"overfit_explanations_{run_id}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(render_weight_overfit_explanations_markdown(payload), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}


def render_weight_overfit_explanations_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Weight Overfit Risk Explanation",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "- 本说明只解释 candidate overfit risk，不应用权重。",
        "",
        "## Summary",
        "",
        f"- Search Run ID: {payload.get('search_run_id')}",
        f"- Candidate Count: {payload.get('candidate_count')}",
        f"- Data Quality Status: {payload.get('data_quality_status')}",
        "",
        "## Candidate Explanation Table",
        "",
        "| Candidate | Rank | Risk Band | Top Reasons | Blocking Metrics | Readiness |",
        "|---|---:|---|---|---:|---|",
    ]
    for record in _records(payload.get("explanations")):
        reasons = [
            str(_mapping(reason).get("reason_id"))
            for reason in _records(record.get("top_overfit_reasons"))
        ]
        lines.append(
            f"| {record.get('weight_set_id')} | {record.get('rank')} | "
            f"{record.get('overfit_risk_band')} | {', '.join(reasons)} | "
            f"{len(_records(record.get('blocking_metrics')))} | "
            f"{record.get('forward_readiness_status')} |"
        )
    if not _records(payload.get("explanations")):
        lines.append("| none | 0 | needs_more_data | none | 0 | needs_more_data |")
    lines.extend(["", "## Manual Review Notes", ""])
    for record in _records(payload.get("explanations")):
        lines.append(f"### {record.get('weight_set_id')}")
        lines.append("")
        lines.append(f"- Risk Band: {record.get('overfit_risk_band')}")
        lines.append(f"- Manual Review Note: {record.get('manual_review_note')}")
        lines.append("- Top Overfit Reasons:")
        for reason in _records(record.get("top_overfit_reasons")):
            reason_map = _mapping(reason)
            lines.append(
                "  - "
                + str(reason_map.get("reason_id"))
                + ": "
                + str(reason_map.get("explanation"))
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def build_weight_initial_recommendation_report(
    search_payload: Mapping[str, Any],
    *,
    top_export_payload: Mapping[str, Any] | None = None,
    comparison_payload: Mapping[str, Any] | None = None,
    regime_robustness_payload: Mapping[str, Any] | None = None,
    overfit_explanation_payload: Mapping[str, Any] | None = None,
    enrollment_payload: Mapping[str, Any] | None = None,
    top: int = 10,
    source_paths: Mapping[str, str] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    validate_weight_search_run_payload(search_payload)
    if top <= 0:
        raise WeightCalibrationError("top must be positive")
    generated = generated_at or datetime.now(UTC)
    top_payload = dict(top_export_payload or {})
    if top_payload:
        validate_weight_top_candidate_export(top_payload)
    else:
        top_payload = build_weight_top_candidate_export(
            search_payload,
            top=top,
            generated_at=generated,
        )
    comparison = dict(comparison_payload or {})
    if comparison:
        validate_weight_candidate_comparison_table(comparison)
    else:
        comparison = build_weight_candidate_comparison_table(
            search_payload,
            top_export_payload=top_payload,
            top=top,
            generated_at=generated,
        )
    regime = dict(regime_robustness_payload or {})
    if regime:
        validate_weight_regime_robustness_heatmap(regime)
    else:
        regime = build_weight_regime_robustness_heatmap(
            search_payload,
            top_export_payload=top_payload,
            top=top,
            generated_at=generated,
        )
    overfit = dict(overfit_explanation_payload or {})
    if overfit:
        validate_weight_overfit_explanations(overfit)
    else:
        overfit = build_weight_overfit_explanations(
            search_payload,
            top_export_payload=top_payload,
            top=top,
            generated_at=generated,
        )
    if enrollment_payload:
        validate_weight_forward_enrollment_registry(enrollment_payload)
    candidates = _records(top_payload.get("candidates"))[:top]
    payload = {
        "schema_version": WEIGHT_INITIAL_RECOMMENDATION_SCHEMA_VERSION,
        "report_type": "etf_weight_initial_recommendation_report",
        "status": "available" if candidates else "needs_more_data",
        "generated_at": generated.isoformat(),
        "recommendation_mode": "candidate_only_shadow_review",
        "safety_banner": dict(WEIGHT_CALIBRATION_SAFETY),
        "run_metadata": _weight_recommendation_run_metadata(search_payload),
        "data_range_and_preset": {
            "market_regime": search_payload.get("market_regime"),
            "historical_range_preset": dict(
                _mapping(search_payload.get("historical_range_preset"))
            ),
            "requested_date_range": dict(_mapping(search_payload.get("requested_date_range"))),
            "data_quality_status": search_payload.get("data_quality_status"),
        },
        "search_constraints": _weight_recommendation_search_constraints(search_payload),
        "top_n_candidates": candidates,
        "benchmark_comparison": _weight_recommendation_benchmark_comparison(comparison),
        "regime_robustness": _weight_recommendation_regime_robustness(regime),
        "overfit_explanations": _weight_recommendation_overfit_explanations(overfit),
        "forward_readiness": _weight_recommendation_forward_readiness(candidates),
        "shadow_enrollment_recommendations": _weight_shadow_enrollment_recommendations(
            candidates,
            enrollment_payload=enrollment_payload or {},
        ),
        "manual_review_notes": _weight_initial_recommendation_manual_notes(candidates),
        "source_artifacts": dict(source_paths or {}),
        "next_steps": _weight_initial_recommendation_next_steps(candidates),
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_weight_initial_recommendation_report(payload)
    return payload


def validate_weight_initial_recommendation_report(payload: Mapping[str, Any]) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_INITIAL_RECOMMENDATION_SCHEMA_VERSION:
        issues.append("schema_version")
    if payload.get("report_type") != "etf_weight_initial_recommendation_report":
        issues.append("report_type")
    required_sections = {
        "safety_banner",
        "run_metadata",
        "data_range_and_preset",
        "search_constraints",
        "top_n_candidates",
        "benchmark_comparison",
        "regime_robustness",
        "overfit_explanations",
        "forward_readiness",
        "shadow_enrollment_recommendations",
        "manual_review_notes",
        "source_artifacts",
        "next_steps",
    }
    issues.extend(sorted(required_sections - set(payload)))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(field)
    safety = _mapping(payload.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    banner = _mapping(payload.get("safety_banner"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if banner.get(field) != expected:
            issues.append(f"safety_banner.{field}")
    if payload.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if payload.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    shadow = _mapping(payload.get("shadow_enrollment_recommendations"))
    if shadow.get("production_effect") != "none" or shadow.get("broker_action") != "none":
        issues.append("shadow_enrollment_recommendations.safety")
    if issues:
        raise WeightCalibrationError(
            "ETF weight initial recommendation report validation failed: "
            + ", ".join(str(issue) for issue in issues)
        )


def write_weight_initial_recommendation_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_INITIAL_RECOMMENDATION_DIR,
) -> dict[str, Path]:
    validate_weight_initial_recommendation_report(payload)
    run_id = _artifact_stem(str(_mapping(payload.get("run_metadata")).get("search_run_id")))
    stem = f"initial_weight_recommendation_{run_id}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(
        render_weight_initial_recommendation_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_path, "markdown": markdown_path}


def render_weight_initial_recommendation_markdown(payload: Mapping[str, Any]) -> str:
    metadata = _mapping(payload.get("run_metadata"))
    data_range = _mapping(payload.get("data_range_and_preset"))
    shadow = _mapping(payload.get("shadow_enrollment_recommendations"))
    lines = [
        "# ETF Initial Weight Recommendation Report",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "- 本报告只给出 candidate-only shadow observation 建议，不应用 ETF weights。",
        "",
        "## Run Metadata",
        "",
        f"- Search Run ID: {metadata.get('search_run_id')}",
        f"- Search Config Hash: `{metadata.get('search_config_hash')}`",
        f"- Generated At: {payload.get('generated_at')}",
        "",
        "## Data Range And Preset",
        "",
        f"- Market Regime: {data_range.get('market_regime')}",
        f"- Preset: {_mapping(data_range.get('historical_range_preset')).get('preset_id')}",
        f"- Requested Date Range: {data_range.get('requested_date_range')}",
        f"- Data Quality Status: {data_range.get('data_quality_status')}",
        "",
        "## Top-N Candidates",
        "",
        "| Rank | Weight Set | Weights | Score | Overfit | Readiness |",
        "|---:|---|---|---:|---|---|",
    ]
    for candidate in _records(payload.get("top_n_candidates")):
        lines.append(
            f"| {candidate.get('rank')} | {candidate.get('weight_set_id')} | "
            f"{json.dumps(candidate.get('weights') or {}, sort_keys=True)} | "
            f"{_fmt_number(candidate.get('historical_score'))} | "
            f"{candidate.get('overfit_risk')} | "
            f"{candidate.get('forward_readiness_status')} |"
        )
    lines.extend(
        [
            "",
            "## Benchmark Comparison",
            "",
            "| Candidate | Type | Total Return | CAGR | Max DD | Excess vs QQQ | Readiness |",
            "|---|---|---:|---:|---:|---:|---|",
        ]
    )
    for row in _records(_mapping(payload.get("benchmark_comparison")).get("rows"))[:12]:
        lines.append(
            f"| {row.get('candidate_id')} | {row.get('row_type')} | "
            f"{_fmt_pct(row.get('total_return'))} | {_fmt_pct(row.get('CAGR'))} | "
            f"{_fmt_pct(row.get('max_drawdown'))} | "
            f"{_fmt_pct(row.get('excess_return_vs_QQQ'))} | "
            f"{row.get('forward_readiness_status')} |"
        )
    lines.extend(
        [
            "",
            "## Regime Robustness",
            "",
            "| Weight Set | Available | Missing | Warnings | Worst Drawdown |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in _records(_mapping(payload.get("regime_robustness")).get("candidate_summary")):
        lines.append(
            f"| {row.get('weight_set_id')} | {row.get('available_regime_count')} | "
            f"{row.get('missing_regime_count')} | {row.get('warning_count')} | "
            f"{_fmt_pct(row.get('worst_max_drawdown'))} |"
        )
    lines.extend(
        [
            "",
            "## Overfit Risk Explanations",
            "",
            "| Weight Set | Risk Band | Top Reasons | Manual Review Note |",
            "|---|---|---|---|",
        ]
    )
    for record in _records(_mapping(payload.get("overfit_explanations")).get("records")):
        reasons = [
            str(_mapping(reason).get("reason_id"))
            for reason in _records(record.get("top_overfit_reasons"))
        ]
        lines.append(
            f"| {record.get('weight_set_id')} | {record.get('overfit_risk_band')} | "
            f"{', '.join(reasons)} | {record.get('manual_review_note')} |"
        )
    lines.extend(
        [
            "",
            "## Shadow Enrollment Recommendation",
            "",
            f"- Suggested Action: {shadow.get('suggested_action')}",
            f"- Recommended Weight Sets: {shadow.get('recommended_weight_set_ids')}",
            f"- Blocked Candidate Count: {shadow.get('blocked_candidate_count')}",
            f"- Already Enrolled Count: {shadow.get('already_enrolled_count')}",
            "",
            "## Manual Review Notes",
            "",
        ]
    )
    for note in payload.get("manual_review_notes") or []:
        lines.append(f"- {note}")
    lines.extend(["", "## Source Artifacts", ""])
    for key, value in _mapping(payload.get("source_artifacts")).items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Next Steps", ""])
    for step in payload.get("next_steps") or []:
        lines.append(f"- {step}")
    return "\n".join(lines) + "\n"


def build_candidate_weight_proposals(
    *,
    candidate_registry: Mapping[str, Any],
    evidence_payload: Mapping[str, Any] | None = None,
    overfit_payload: Mapping[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    validate_candidate_weight_registry(candidate_registry)
    if evidence_payload:
        validate_backtest_forward_evidence_payload(evidence_payload)
    if overfit_payload:
        validate_weight_overfit_diagnostics_payload(overfit_payload)
    generated = generated_at or datetime.now(UTC)
    proposals = [
        _candidate_weight_proposal(
            candidate,
            evidence_payload=evidence_payload or {},
            overfit_payload=overfit_payload or {},
            generated_at=generated,
        )
        for candidate in _records(candidate_registry.get("weight_sets"))
    ]
    payload = {
        "schema_version": WEIGHT_PROPOSAL_SCHEMA_VERSION,
        "report_type": "etf_weight_candidate_proposals",
        "status": "available" if proposals else "needs_more_data",
        "generated_at": generated.isoformat(),
        "policy": dict(WEIGHT_PROPOSAL_POLICY),
        "proposal_count": len(proposals),
        "proposal_type_counts": _proposal_type_counts(proposals),
        "proposals": proposals,
        "disallowed_proposal_types": sorted(DISALLOWED_WEIGHT_PROPOSAL_TYPES),
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_candidate_weight_proposals_payload(payload)
    return payload


def validate_candidate_weight_proposals_payload(payload: Mapping[str, Any]) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_PROPOSAL_SCHEMA_VERSION:
        issues.append("schema_version")
    if payload.get("report_type") != "etf_weight_candidate_proposals":
        issues.append("report_type")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(field)
    safety = _mapping(payload.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if payload.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if payload.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    proposals = _records(payload.get("proposals"))
    if int(payload.get("proposal_count") or 0) != len(proposals):
        issues.append("proposal_count")
    for proposal in proposals:
        try:
            validate_candidate_weight_proposal(proposal)
        except WeightCalibrationError as exc:
            issues.append(f"{proposal.get('proposal_id')}:{exc}")
    if issues:
        raise WeightCalibrationError(
            "ETF weight candidate proposal validation failed: "
            + ", ".join(str(issue) for issue in issues)
        )


def validate_candidate_weight_proposal(proposal: Mapping[str, Any]) -> None:
    required = (
        "proposal_id",
        "weight_set_id",
        "proposal_type",
        "supporting_evidence",
        "blocking_evidence",
        "historical_score",
        "forward_evidence_status",
        "overfit_risk",
        "manual_review_required",
        "safety",
    )
    issues = [field for field in required if field not in proposal]
    proposal_type = str(proposal.get("proposal_type") or "")
    if proposal_type in DISALLOWED_WEIGHT_PROPOSAL_TYPES:
        issues.append("unsafe_proposal_type")
    if proposal_type not in ALLOWED_WEIGHT_PROPOSAL_TYPES:
        issues.append("proposal_type")
    if not isinstance(proposal.get("supporting_evidence"), list):
        issues.append("supporting_evidence")
    if not isinstance(proposal.get("blocking_evidence"), list):
        issues.append("blocking_evidence")
    if not isinstance(proposal.get("overfit_risk"), Mapping):
        issues.append("overfit_risk")
    if proposal.get("manual_review_required") is not True:
        issues.append("manual_review_required")
    if proposal.get("application_allowed") is not False:
        issues.append("application_allowed")
    if proposal.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if proposal.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if proposal.get(field) != expected:
            issues.append(field)
    safety = _mapping(proposal.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if issues:
        raise WeightCalibrationError("; ".join(issues))


def write_candidate_weight_proposals(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_PROPOSAL_DIR,
) -> dict[str, Path]:
    validate_candidate_weight_proposals_payload(payload)
    timestamp = str(payload.get("generated_at", "unknown")).replace(":", "").replace("+", "")
    stem = f"candidate_weight_proposals_{timestamp[:15]}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(render_candidate_weight_proposals_markdown(payload), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}


def render_candidate_weight_proposals_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Weight Candidate Proposals",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "- 本报告只生成 proposal-only recommendation，不应用权重。",
        "",
        "## Summary",
        "",
        f"- Status: {payload.get('status')}",
        f"- Proposal Count: {payload.get('proposal_count')}",
        f"- Proposal Type Counts: {payload.get('proposal_type_counts')}",
        "",
        "| Proposal | Weight Set | Type | Historical Score | Forward Status | Overfit Risk |",
        "|---|---|---|---:|---|---|",
    ]
    for proposal in _records(payload.get("proposals")):
        risk = _mapping(proposal.get("overfit_risk"))
        lines.append(
            f"| {proposal.get('proposal_id')} | {proposal.get('weight_set_id')} | "
            f"{proposal.get('proposal_type')} | "
            f"{_fmt_number(proposal.get('historical_score'))} | "
            f"{proposal.get('forward_evidence_status')} | "
            f"{risk.get('overfit_risk_band')} |"
        )
    if not _records(payload.get("proposals")):
        lines.append("| none | none | defer_until_more_forward_data | n/a | n/a | n/a |")
    return "\n".join(lines) + "\n"


def build_dual_track_weight_calibration_report(
    *,
    as_of: date,
    candidate_registry: Mapping[str, Any],
    forward_enrollments: Mapping[str, Any] | None = None,
    search_payload: Mapping[str, Any] | None = None,
    evidence_payload: Mapping[str, Any] | None = None,
    overfit_payload: Mapping[str, Any] | None = None,
    proposals_payload: Mapping[str, Any] | None = None,
    source_paths: Mapping[str, str] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    validate_candidate_weight_registry(candidate_registry)
    if forward_enrollments:
        validate_weight_forward_enrollment_registry(forward_enrollments)
    if search_payload:
        validate_weight_search_run_payload(search_payload)
    if evidence_payload:
        validate_backtest_forward_evidence_payload(evidence_payload)
    if overfit_payload:
        validate_weight_overfit_diagnostics_payload(overfit_payload)
    if proposals_payload:
        validate_candidate_weight_proposals_payload(proposals_payload)
    generated = generated_at or datetime.now(UTC)
    payload = {
        "schema_version": WEIGHT_DUAL_TRACK_REPORT_SCHEMA_VERSION,
        "report_type": "etf_weight_dual_track_calibration_report",
        "report_id": f"etf-weight-dual-track-calibration-{as_of.isoformat()}",
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": _dual_track_report_status(
            candidate_registry=candidate_registry,
            evidence_payload=evidence_payload or {},
            proposals_payload=proposals_payload or {},
        ),
        "summary": _dual_track_report_summary(
            candidate_registry=candidate_registry,
            forward_enrollments=forward_enrollments or {},
            evidence_payload=evidence_payload or {},
            overfit_payload=overfit_payload or {},
            proposals_payload=proposals_payload or {},
        ),
        "search_configuration": _dual_track_search_configuration(search_payload or {}),
        "top_historical_candidates": _dual_track_top_candidates(
            candidate_registry=candidate_registry,
            search_payload=search_payload or {},
        ),
        "walk_forward_regime_robustness": _dual_track_robustness(search_payload or {}),
        "overfit_diagnostics": _dual_track_overfit(overfit_payload or {}),
        "forward_evidence_comparison": _dual_track_forward_evidence(evidence_payload or {}),
        "candidate_registry_status": _dual_track_registry_status(
            candidate_registry,
            forward_enrollments=forward_enrollments or {},
        ),
        "proposal_scorecard": _dual_track_proposal_scorecard(proposals_payload or {}),
        "manual_review_package": _dual_track_manual_review_package(
            candidate_registry=candidate_registry,
            evidence_payload=evidence_payload or {},
            overfit_payload=overfit_payload or {},
            proposals_payload=proposals_payload or {},
        ),
        "source_report_links": _dual_track_source_links(source_paths or {}),
        "next_steps": _dual_track_next_steps(
            candidate_registry=candidate_registry,
            evidence_payload=evidence_payload or {},
            proposals_payload=proposals_payload or {},
        ),
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_dual_track_weight_calibration_report(payload)
    return payload


def validate_dual_track_weight_calibration_report(payload: Mapping[str, Any]) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_DUAL_TRACK_REPORT_SCHEMA_VERSION:
        issues.append("schema_version")
    if payload.get("report_type") != "etf_weight_dual_track_calibration_report":
        issues.append("report_type")
    if payload.get("status") not in {
        "manual_review_ready",
        "observe_only",
        "needs_more_forward_data",
        "blocked",
        "needs_more_data",
    }:
        issues.append("status")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(field)
    safety = _mapping(payload.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if payload.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if payload.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    scorecard = _mapping(payload.get("proposal_scorecard"))
    for proposal in _records(scorecard.get("proposals")):
        try:
            validate_candidate_weight_proposal(proposal)
        except WeightCalibrationError as exc:
            issues.append(f"proposal_scorecard.{proposal.get('proposal_id')}:{exc}")
    manual_review = _mapping(payload.get("manual_review_package"))
    if manual_review.get("manual_review_required") is not True:
        issues.append("manual_review_package.manual_review_required")
    if manual_review.get("application_allowed") is not False:
        issues.append("manual_review_package.application_allowed")
    if issues:
        raise WeightCalibrationError(
            "ETF dual-track calibration report validation failed: "
            + ", ".join(str(issue) for issue in issues)
        )


def write_dual_track_weight_calibration_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_DUAL_TRACK_REPORT_DIR,
) -> dict[str, Path]:
    validate_dual_track_weight_calibration_report(payload)
    as_of = str(payload.get("as_of") or "unknown")
    stem = f"dual_track_calibration_{as_of}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(
        render_dual_track_weight_calibration_report_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_path, "markdown": markdown_path}


def render_dual_track_weight_calibration_report_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    search = _mapping(payload.get("search_configuration"))
    robustness = _mapping(payload.get("walk_forward_regime_robustness"))
    overfit = _mapping(payload.get("overfit_diagnostics"))
    forward = _mapping(payload.get("forward_evidence_comparison"))
    registry = _mapping(payload.get("candidate_registry_status"))
    proposals = _mapping(payload.get("proposal_scorecard"))
    manual = _mapping(payload.get("manual_review_package"))
    lines = [
        "# ETF Weight Dual-Track Calibration Report",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "- 本报告只生成 candidate initial weight review package，不应用权重。",
        "",
        "## Summary",
        "",
        f"- Status: {payload.get('status')}",
        f"- As Of: {payload.get('as_of')}",
        f"- Candidate Count: {summary.get('candidate_count')}",
        f"- Enrolled Count: {summary.get('enrolled_count')}",
        f"- Top Candidate: {summary.get('top_candidate_id')}",
        f"- Forward Evidence Status: {summary.get('dominant_forward_evidence_status')}",
        f"- Highest Overfit Risk: {summary.get('highest_overfit_risk_band')}",
        f"- Manual Review Proposals: {summary.get('manual_review_proposal_count')}",
        "",
        "## Search Configuration",
        "",
        f"- Search ID: {search.get('search_id')}",
        f"- Search Config Hash: `{search.get('search_config_hash')}`",
        f"- Market Regime: {search.get('market_regime')}",
        (
            "- Requested Date Range: "
            f"{_mapping(search.get('requested_date_range')).get('start')} to "
            f"{_mapping(search.get('requested_date_range')).get('end')}"
        ),
        f"- Data Quality Status: {search.get('data_quality_status')}",
        f"- Candidate Generation: {search.get('candidate_generation')}",
        "",
        "## Top Historical Candidates",
        "",
        "| Rank | Weight Set | Candidate | Score | Status | Weights |",
        "|---:|---|---|---:|---|---|",
    ]
    for candidate in _records(payload.get("top_historical_candidates")):
        lines.append(
            f"| {candidate.get('rank')} | {candidate.get('weight_set_id')} | "
            f"{candidate.get('source_candidate_id')} | "
            f"{_fmt_number(candidate.get('candidate_score'))} | "
            f"{candidate.get('status')} | {candidate.get('weights')} |"
        )
    if not _records(payload.get("top_historical_candidates")):
        lines.append("| n/a | n/a | n/a | n/a | needs_more_data | n/a |")
    lines.extend(
        [
            "",
            "## Walk-Forward / Regime Robustness",
            "",
            f"- Status: {robustness.get('status')}",
            f"- Run Summary: {robustness.get('summary')}",
            "",
            "## Overfit Diagnostics",
            "",
            f"- Status: {overfit.get('status')}",
            f"- Risk Counts: {overfit.get('risk_counts')}",
            f"- Highest Risk Candidate: {overfit.get('highest_risk_candidate')}",
            "",
            "## Forward Evidence Comparison",
            "",
            f"- Status: {forward.get('status')}",
            f"- Status Counts: {forward.get('status_counts')}",
            f"- Evidence Record Count: {forward.get('evidence_record_count')}",
            "",
            "## Candidate Registry Status",
            "",
            f"- Candidate Count: {registry.get('candidate_count')}",
            f"- Status Counts: {registry.get('status_counts')}",
            f"- Enrollment Count: {registry.get('enrollment_count')}",
            "",
            "## Proposal Scorecard",
            "",
            f"- Status: {proposals.get('status')}",
            f"- Proposal Type Counts: {proposals.get('proposal_type_counts')}",
            "",
            "| Weight Set | Proposal Type | Forward Status | Overfit Risk |",
            "|---|---|---|---|",
        ]
    )
    for proposal in _records(proposals.get("proposals")):
        risk = _mapping(proposal.get("overfit_risk"))
        lines.append(
            f"| {proposal.get('weight_set_id')} | {proposal.get('proposal_type')} | "
            f"{proposal.get('forward_evidence_status')} | "
            f"{risk.get('overfit_risk_band')} |"
        )
    if not _records(proposals.get("proposals")):
        lines.append("| n/a | defer_until_more_forward_data | needs_more_data | n/a |")
    lines.extend(
        [
            "",
            "## Manual Review Package",
            "",
            f"- Manual Review Required: {manual.get('manual_review_required')}",
            f"- Application Allowed: {manual.get('application_allowed')}",
            f"- Candidate Shortlist: {manual.get('candidate_shortlist')}",
            f"- Blocking Notes: {manual.get('blocking_notes')}",
            "",
            "## Source Report Links",
            "",
            "| Source | Path | Exists |",
            "|---|---|---|",
        ]
    )
    for source in _records(payload.get("source_report_links")):
        lines.append(
            f"| {source.get('source_type')} | `{source.get('path')}` | "
            f"{source.get('exists')} |"
        )
    if not _records(payload.get("source_report_links")):
        lines.append("| none | n/a | false |")
    lines.extend(
        [
            "",
            "## Next Steps",
            "",
        ]
    )
    for step in payload.get("next_steps") or []:
        lines.append(f"- {step}")
    return "\n".join(lines) + "\n"


def build_dual_track_weight_calibration_validation_report(
    *,
    search_config_path: Path = DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    proposals_payload: Mapping[str, Any] | None = None,
    report_payload: Mapping[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    checks: list[dict[str, Any]] = []
    search_registry: ETFWeightSearchRegistry | None = None
    config_error = ""
    try:
        search_registry = load_weight_search_registry(search_config_path)
    except WeightCalibrationError as exc:
        config_error = str(exc)

    _append_weight_calibration_validation_check(
        checks,
        "weight_search_config_valid",
        search_registry is not None,
        (
            "Weight search config loads with mandatory safety fields."
            if search_registry is not None
            else f"Weight search config failed validation: {config_error}"
        ),
        {"search_config_path": str(search_config_path)},
    )
    bounded_pass, bounded_message, bounded_details = _weight_search_registry_bounded_check(
        search_registry
    )
    _append_weight_calibration_validation_check(
        checks,
        "weight_search_bounded",
        bounded_pass,
        bounded_message,
        bounded_details,
    )

    sample_pipeline: dict[str, Any] = {}
    sample_error = ""
    if search_registry is None or not bounded_pass:
        sample_error = (
            "Sample pipeline skipped because the weight search config is invalid "
            "or unbounded."
        )
    else:
        try:
            sample_pipeline = _weight_calibration_validation_sample_pipeline(
                generated,
                search_config_path=search_config_path,
            )
        except WeightCalibrationError as exc:
            sample_error = str(exc)

    _append_weight_calibration_validation_check(
        checks,
        "historical_search_engine_available",
        bool(sample_pipeline.get("search_run")) and not sample_error,
        (
            "Sample bounded historical search run validates."
            if not sample_error
            else f"Sample historical search failed: {sample_error}"
        ),
    )
    _append_weight_calibration_validation_check(
        checks,
        "walk_forward_regime_robustness_available",
        _sample_pipeline_has_robustness(sample_pipeline) and not sample_error,
        (
            "Sample search run includes walk-forward and regime robustness slices."
            if not sample_error
            else f"Robustness sample failed: {sample_error}"
        ),
    )
    _append_weight_calibration_validation_check(
        checks,
        "candidate_weight_registry_available",
        bool(sample_pipeline.get("candidate_registry")) and not sample_error,
        (
            "Candidate weight registry validates sample candidate-only records."
            if not sample_error
            else f"Candidate registry sample failed: {sample_error}"
        ),
    )
    _append_weight_calibration_validation_check(
        checks,
        "forward_enrollment_available",
        bool(sample_pipeline.get("forward_enrollments")) and not sample_error,
        (
            "Forward enrollment validates sample shadow observation records."
            if not sample_error
            else f"Forward enrollment sample failed: {sample_error}"
        ),
    )
    _append_weight_calibration_validation_check(
        checks,
        "backtest_forward_aggregator_available",
        bool(sample_pipeline.get("evidence")) and not sample_error,
        (
            "Backtest-vs-forward evidence aggregation validates sample gaps."
            if not sample_error
            else f"Evidence aggregation sample failed: {sample_error}"
        ),
    )
    _append_weight_calibration_validation_check(
        checks,
        "overfit_diagnostics_available",
        bool(sample_pipeline.get("overfit")) and not sample_error,
        (
            "Overfit diagnostics validate sample stability and risk bands."
            if not sample_error
            else f"Overfit diagnostics sample failed: {sample_error}"
        ),
    )
    _append_weight_calibration_validation_check(
        checks,
        "proposal_generator_available",
        bool(sample_pipeline.get("proposals")) and not sample_error,
        (
            "Candidate weight proposal generator validates proposal-only actions."
            if not sample_error
            else f"Proposal generator sample failed: {sample_error}"
        ),
    )
    _append_weight_calibration_validation_check(
        checks,
        "report_generator_available",
        bool(sample_pipeline.get("report")) and not sample_error,
        (
            "Dual-track calibration report validates sample manual review package."
            if not sample_error
            else f"Report generator sample failed: {sample_error}"
        ),
    )
    _append_weight_calibration_validation_check(
        checks,
        "reader_brief_integration_available",
        *_weight_calibration_reader_brief_registry_check(report_registry_path),
    )
    _append_weight_calibration_validation_check(
        checks,
        "unsafe_proposal_types_blocked",
        _unsafe_weight_calibration_proposal_type_is_blocked(),
        "Unsafe weight proposal types are rejected by proposal validation.",
    )

    proposal_payload_to_validate = (
        _mapping(sample_pipeline.get("proposals"))
        if proposals_payload is None
        else proposals_payload
    )
    report_payload_to_validate = (
        _mapping(sample_pipeline.get("report")) if report_payload is None else report_payload
    )
    checks.extend(
        _weight_calibration_proposal_payload_validation_checks(proposal_payload_to_validate)
    )
    checks.extend(_weight_calibration_report_payload_validation_checks(report_payload_to_validate))

    status = "PASS" if all(check.get("status") == "PASS" for check in checks) else "FAIL"
    payload = {
        "schema_version": WEIGHT_CALIBRATION_VALIDATION_SCHEMA_VERSION,
        "report_type": "etf_weight_dual_track_validation",
        "status": status,
        "generated_at": generated.isoformat(),
        "validation_mode": "config_sample_pipeline_and_payload_checks",
        "search_config_path": str(search_config_path),
        "report_registry_path": str(report_registry_path),
        "check_count": len(checks),
        "failed_check_count": sum(1 for check in checks if check.get("status") != "PASS"),
        "checks": checks,
        "production_weights_mutated": False,
        "baseline_config_mutated": False,
        "broker_actions_created": False,
        "proposal_only": True,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_dual_track_weight_calibration_validation_report(payload)
    return payload


def validate_dual_track_weight_calibration_validation_report(payload: Mapping[str, Any]) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_CALIBRATION_VALIDATION_SCHEMA_VERSION:
        issues.append("schema_version")
    if payload.get("report_type") != "etf_weight_dual_track_validation":
        issues.append("report_type")
    if payload.get("status") not in {"PASS", "FAIL"}:
        issues.append("status")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(field)
    safety = _mapping(payload.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if payload.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if payload.get("baseline_config_mutated") is not False:
        issues.append("baseline_config_mutated")
    if payload.get("broker_actions_created") is not False:
        issues.append("broker_actions_created")
    if payload.get("proposal_only") is not True:
        issues.append("proposal_only")
    checks = _records(payload.get("checks"))
    if int(payload.get("check_count") or 0) != len(checks):
        issues.append("check_count")
    failed_count = sum(1 for check in checks if check.get("status") != "PASS")
    if int(payload.get("failed_check_count") or 0) != failed_count:
        issues.append("failed_check_count")
    for check in checks:
        if check.get("status") not in {"PASS", "FAIL"}:
            issues.append(f"{check.get('check_id')}.status")
        if not _text(check.get("check_id")):
            issues.append("check_id")
    if issues:
        raise WeightCalibrationError(
            "ETF dual-track weight calibration validation failed: "
            + ", ".join(str(issue) for issue in issues)
        )


def write_dual_track_weight_calibration_validation_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_CALIBRATION_VALIDATION_DIR,
) -> dict[str, Path]:
    validate_dual_track_weight_calibration_validation_report(payload)
    timestamp = str(payload.get("generated_at", "unknown")).replace(":", "").replace("+", "")
    stem = f"weight_calibration_validation_{timestamp[:15]}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(
        render_dual_track_weight_calibration_validation_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_path, "markdown": markdown_path}


def render_dual_track_weight_calibration_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    lines = [
        "# ETF Weight Dual-Track Calibration Validation Gate",
        "",
        f"- Status: {payload.get('status')}",
        f"- Generated At: {payload.get('generated_at')}",
        "- Safety: observe_only=true, candidate_only=true, production_effect=none, "
        "broker_action=none, manual_review_required=true",
        "- 本 gate 只校验 TRADING-071 candidate-only workflow，不应用 ETF weights。",
        "",
        "| Check | Status | Message |",
        "|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"| {check.get('check_id')} | {check.get('status')} | "
            f"{check.get('message')} |"
        )
    return "\n".join(lines) + "\n"


def _append_weight_calibration_validation_check(
    checks: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    message: str,
    details: Mapping[str, Any] | None = None,
) -> None:
    checks.append(
        {
            "check_id": check_id,
            "status": "PASS" if passed else "FAIL",
            "message": message,
            "details": dict(details or {}),
        }
    )


def _weight_search_registry_bounded_check(
    registry: ETFWeightSearchRegistry | None,
) -> tuple[bool, str, dict[str, Any]]:
    if registry is None:
        return False, "Weight search registry is unavailable, so bounds cannot be confirmed.", {}
    issues = []
    details: dict[str, Any] = {}
    for search_id, search in registry.weight_searches.items():
        details[search_id] = {
            "grid_step": search.grid_step,
            "max_candidate_count": search.max_candidate_count,
            "universe": list(search.universe),
        }
        if search.grid_step < MIN_BOUNDED_WEIGHT_SEARCH_GRID_STEP:
            issues.append(f"{search_id}.grid_step_too_fine")
        if search.max_candidate_count <= 0:
            issues.append(f"{search_id}.max_candidate_count")
        if search.max_candidate_count > MAX_BOUNDED_WEIGHT_SEARCH_CANDIDATES:
            issues.append(f"{search_id}.max_candidate_count_unbounded")
        if not search.universe or "CASH" not in search.universe:
            issues.append(f"{search_id}.universe")
        if set(search.weight_constraints) != set(search.universe):
            issues.append(f"{search_id}.weight_constraints")
    return (
        not issues,
        (
            "Weight search definitions are bounded by grid_step, universe, constraints, "
            "and candidate cap."
            if not issues
            else "Weight search definitions are not bounded: " + ", ".join(issues)
        ),
        details,
    )


def _weight_calibration_validation_sample_pipeline(
    generated_at: datetime,
    *,
    search_config_path: Path,
) -> dict[str, Any]:
    config = load_etf_config_bundle()
    search_registry = load_weight_search_registry(search_config_path, etf_config=config)
    prices, metadata_issues = standardize_price_frame(
        _weight_calibration_validation_prices(),
        assets=config.assets,
        source_name="validation_fixture",
    )
    quality_report = validate_price_data(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        as_of=date.fromisoformat(str(prices["date"].max())),
        extra_issues=metadata_issues,
    )
    if not quality_report.passed:
        raise WeightCalibrationError(
            "ETF weight calibration validation sample price quality failed: "
            + quality_report.status
        )
    run = run_historical_weight_search(
        prices,
        etf_config=config,
        quality_report=quality_report,
        registry=search_registry,
        search_id="etf_initial_weight_search_v1",
        start=date(2022, 12, 1),
        end=date(2022, 12, 20),
        max_candidates=4,
        generated_at=generated_at,
    )
    with TemporaryDirectory(prefix="etf_weight_calibration_validation_") as tmp:
        tmp_root = Path(tmp)
        candidate_registry = register_candidate_weight_sets(
            run.payload,
            registry_path=tmp_root / "candidate_weight_registry.json",
            top=1,
            created_at=generated_at,
        )
        forward_enrollments = enroll_candidate_weights_forward(
            candidate_registry,
            enrollment_path=tmp_root / "forward_enrollments.json",
            top=1,
            enrolled_at=generated_at,
        )
    forward_dashboard = _weight_calibration_validation_forward_dashboard(
        candidate_registry,
        forward_enrollments,
    )
    evidence = build_backtest_forward_evidence_aggregation(
        as_of=generated_at.date(),
        candidate_registry=candidate_registry,
        forward_enrollments=forward_enrollments,
        search_payload=run.payload,
        forward_dashboard=forward_dashboard,
        source_paths={
            "historical_search": "validation://weight_search_summary.json",
            "candidate_registry": "validation://candidate_weight_registry.json",
            "forward_enrollment": "validation://forward_enrollments.json",
            "forward_dashboard": "validation://forward_dashboard.json",
        },
        generated_at=generated_at,
    )
    overfit = build_weight_overfit_diagnostics(
        candidate_registry=candidate_registry,
        search_payload=run.payload,
        evidence_payload=evidence,
        generated_at=generated_at,
    )
    proposals = build_candidate_weight_proposals(
        candidate_registry=candidate_registry,
        evidence_payload=evidence,
        overfit_payload=overfit,
        generated_at=generated_at,
    )
    report = build_dual_track_weight_calibration_report(
        as_of=generated_at.date(),
        candidate_registry=candidate_registry,
        forward_enrollments=forward_enrollments,
        search_payload=run.payload,
        evidence_payload=evidence,
        overfit_payload=overfit,
        proposals_payload=proposals,
        source_paths={
            "historical_search": "validation://weight_search_summary.json",
            "candidate_registry": "validation://candidate_weight_registry.json",
            "forward_enrollment": "validation://forward_enrollments.json",
            "backtest_forward_evidence": "validation://backtest_forward_evidence.json",
            "overfit_diagnostics": "validation://overfit_diagnostics.json",
            "candidate_weight_proposals": "validation://candidate_weight_proposals.json",
        },
        generated_at=generated_at,
    )
    return {
        "search_run": run.payload,
        "candidate_registry": candidate_registry,
        "forward_enrollments": forward_enrollments,
        "forward_dashboard": forward_dashboard,
        "evidence": evidence,
        "overfit": overfit,
        "proposals": proposals,
        "report": report,
    }


def _weight_calibration_validation_prices(days: int = 360) -> pd.DataFrame:
    dates = pd.bdate_range("2022-01-03", periods=days)
    rows = []
    for symbol_index, symbol in enumerate(["SPY", "QQQ", "SMH", "SOXX"]):
        for index, current_date in enumerate(dates):
            price = 100.0 + index * (80.0 / max(days - 1, 1)) + symbol_index
            rows.append(
                {
                    "date": current_date.date().isoformat(),
                    "symbol": symbol,
                    "open": price,
                    "high": price * 1.01,
                    "low": price * 0.99,
                    "close": price,
                    "adj_close": price,
                    "volume": 1_000_000,
                    "source": "validation_fixture",
                    "created_at": "2026-06-02T00:00:00+00:00",
                }
            )
    return pd.DataFrame(rows)


def _weight_calibration_validation_forward_dashboard(
    candidate_registry: Mapping[str, Any],
    forward_enrollments: Mapping[str, Any],
) -> dict[str, Any]:
    candidate = _records(candidate_registry.get("weight_sets"))[0]
    enrollment = _records(forward_enrollments.get("enrollments"))[0]
    metrics = _mapping(candidate.get("metrics_summary"))
    robustness = _mapping(candidate.get("robustness_summary"))
    expected_return = float(metrics.get("total_return") or 0.0)
    expected_drawdown = float(metrics.get("max_drawdown") or 0.0)
    expected_turnover = float(metrics.get("turnover_vs_baseline") or 0.0)
    expected_stability = float(robustness.get("stability_score") or 0.0)
    return {
        "schema_version": "etf_forward_dashboard_v1",
        "report_type": "etf_forward_dashboard",
        "status": "AVAILABLE",
        "as_of": "2026-06-02",
        "candidate_summary_table": [
            {
                "weight_set_id": candidate.get("weight_set_id"),
                "shadow_id": enrollment.get("shadow_id"),
                "candidate_id": candidate.get("source_candidate_id"),
                "days_since_enrollment": 25,
                "return_since_enrollment": expected_return + 0.02,
                "max_drawdown_since_enrollment": expected_drawdown,
                "turnover_since_enrollment": expected_turnover,
                "weight_stability_score": expected_stability,
                "metric_null_reasons": {},
            }
        ],
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }


def _sample_pipeline_has_robustness(sample_pipeline: Mapping[str, Any]) -> bool:
    search_payload = _mapping(sample_pipeline.get("search_run"))
    robustness = _mapping(search_payload.get("robustness_evaluation"))
    evaluations = _records(robustness.get("candidate_evaluations"))
    if not evaluations:
        return False
    return any(_records(item.get("slice_metrics")) for item in evaluations)


def _weight_calibration_reader_brief_registry_check(
    report_registry_path: Path,
) -> tuple[bool, str, dict[str, Any]]:
    try:
        registry = load_report_registry(report_registry_path)
    except (FileNotFoundError, ValueError) as exc:
        return False, f"Report registry unavailable: {exc}", {}
    for report in _records(registry.get("reports")):
        if _text(report.get("report_id")) != "etf_weight_dual_track_calibration_report":
            continue
        visible = report.get("include_in_reader_brief") is True
        has_command = "weight-calibration report" in _text(report.get("command"))
        passed = visible and has_command
        return (
            passed,
            (
                "etf_weight_dual_track_calibration_report is visible to Reader Brief."
                if passed
                else (
                    "etf_weight_dual_track_calibration_report is missing Reader Brief "
                    "visibility metadata."
                )
            ),
            {
                "include_in_reader_brief": report.get("include_in_reader_brief"),
                "command": report.get("command"),
            },
        )
    return False, "etf_weight_dual_track_calibration_report is missing from report registry.", {}


def _unsafe_weight_calibration_proposal_type_is_blocked() -> bool:
    try:
        validate_candidate_weight_proposal(_unsafe_weight_calibration_proposal())
    except WeightCalibrationError:
        return True
    return False


def _unsafe_weight_calibration_proposal() -> dict[str, Any]:
    return {
        "proposal_id": "unsafe-weight-calibration-proposal",
        "weight_set_id": "unsafe_weight_set",
        "proposal_type": "apply_weight_set",
        "supporting_evidence": [],
        "blocking_evidence": ["unsafe proposal type must be blocked"],
        "historical_score": 1.0,
        "forward_evidence_status": "consistent",
        "overfit_risk": {
            "overfit_risk_score": 0.0,
            "overfit_risk_band": "low",
        },
        "manual_review_required": True,
        "application_allowed": False,
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "created_at": datetime(2026, 6, 2, tzinfo=UTC).isoformat(),
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }


def _weight_calibration_proposal_payload_validation_checks(
    proposals_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    try:
        validate_candidate_weight_proposals_payload(proposals_payload)
        _append_weight_calibration_validation_check(
            checks,
            "proposal_payload_schema_valid",
            True,
            "Candidate weight proposal payload schema and safety fields are valid.",
        )
    except WeightCalibrationError as exc:
        _append_weight_calibration_validation_check(
            checks,
            "proposal_payload_schema_valid",
            False,
            f"Candidate weight proposal payload failed validation: {exc}",
        )
    safety = _mapping(proposals_payload.get("safety"))
    proposals = _records(proposals_payload.get("proposals"))
    _append_weight_calibration_validation_check(
        checks,
        "proposal_payload_production_effect_none",
        _text(proposals_payload.get("production_effect")) == "none"
        and _text(safety.get("production_effect")) == "none"
        and _all_weight_proposals_match(proposals, "production_effect", "none"),
        "Proposal payload and proposals keep production_effect=none.",
    )
    _append_weight_calibration_validation_check(
        checks,
        "proposal_payload_broker_action_none",
        _text(proposals_payload.get("broker_action")) == "none"
        and _text(safety.get("broker_action")) == "none"
        and _all_weight_proposals_match(proposals, "broker_action", "none"),
        "Proposal payload and proposals keep broker_action=none.",
    )
    _append_weight_calibration_validation_check(
        checks,
        "proposal_payload_manual_review_required_true",
        proposals_payload.get("manual_review_required") is True
        and safety.get("manual_review_required") is True
        and _all_weight_proposals_match(proposals, "manual_review_required", True),
        "Proposal payload and proposals keep manual_review_required=true.",
    )
    _append_weight_calibration_validation_check(
        checks,
        "unsafe_proposal_types_absent",
        not _weight_payload_contains_unsafe_proposal_type(proposals),
        "Proposal payload does not contain unsafe proposal types.",
    )
    _append_weight_calibration_validation_check(
        checks,
        "proposals_evidence_linked",
        _weight_proposals_evidence_linked(proposals),
        "Each proposal has supporting or blocking evidence.",
    )
    _append_weight_calibration_validation_check(
        checks,
        "proposal_only_behavior",
        proposals_payload.get("production_weights_mutated") is False
        and proposals_payload.get("applied_weight_set") is None
        and all(proposal.get("application_allowed") is False for proposal in proposals)
        and all(proposal.get("production_weights_mutated") is False for proposal in proposals)
        and all(proposal.get("applied_weight_set") is None for proposal in proposals),
        "Proposal payload does not apply weights or mutate production state.",
    )
    return checks


def _weight_calibration_report_payload_validation_checks(
    report_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    try:
        validate_dual_track_weight_calibration_report(report_payload)
        _append_weight_calibration_validation_check(
            checks,
            "report_payload_schema_valid",
            True,
            "Dual-track calibration report schema and safety fields are valid.",
        )
    except WeightCalibrationError as exc:
        _append_weight_calibration_validation_check(
            checks,
            "report_payload_schema_valid",
            False,
            f"Dual-track calibration report payload failed validation: {exc}",
        )
    safety = _mapping(report_payload.get("safety"))
    proposals = _weight_report_proposals(report_payload)
    manual_review = _mapping(report_payload.get("manual_review_package"))
    _append_weight_calibration_validation_check(
        checks,
        "report_payload_production_effect_none",
        _text(report_payload.get("production_effect")) == "none"
        and _text(safety.get("production_effect")) == "none"
        and _all_weight_proposals_match(proposals, "production_effect", "none"),
        "Report payload and embedded proposals keep production_effect=none.",
    )
    _append_weight_calibration_validation_check(
        checks,
        "report_payload_broker_action_none",
        _text(report_payload.get("broker_action")) == "none"
        and _text(safety.get("broker_action")) == "none"
        and _all_weight_proposals_match(proposals, "broker_action", "none"),
        "Report payload and embedded proposals keep broker_action=none.",
    )
    _append_weight_calibration_validation_check(
        checks,
        "report_payload_manual_review_required_true",
        report_payload.get("manual_review_required") is True
        and safety.get("manual_review_required") is True
        and manual_review.get("manual_review_required") is True
        and _all_weight_proposals_match(proposals, "manual_review_required", True),
        "Report payload keeps manual_review_required=true.",
    )
    _append_weight_calibration_validation_check(
        checks,
        "report_payload_unsafe_proposal_types_absent",
        not _weight_payload_contains_unsafe_proposal_type(proposals),
        "Report payload does not contain unsafe proposal types.",
    )
    _append_weight_calibration_validation_check(
        checks,
        "report_payload_proposal_only_behavior",
        report_payload.get("production_weights_mutated") is False
        and report_payload.get("applied_weight_set") is None
        and manual_review.get("application_allowed") is False
        and all(proposal.get("application_allowed") is False for proposal in proposals),
        "Report payload remains proposal-only and does not apply candidate weights.",
    )
    return checks


def _all_weight_proposals_match(
    proposals: list[Mapping[str, Any]],
    field: str,
    expected: object,
) -> bool:
    return all(proposal.get(field) == expected for proposal in proposals)


def _weight_payload_contains_unsafe_proposal_type(proposals: list[Mapping[str, Any]]) -> bool:
    return any(
        _text(proposal.get("proposal_type")) in DISALLOWED_WEIGHT_PROPOSAL_TYPES
        for proposal in proposals
    )


def _weight_report_proposals(report_payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return _records(_mapping(report_payload.get("proposal_scorecard")).get("proposals"))


def _weight_proposals_evidence_linked(proposals: list[Mapping[str, Any]]) -> bool:
    if not proposals:
        return False
    return all(
        bool(proposal.get("supporting_evidence") or proposal.get("blocking_evidence"))
        for proposal in proposals
    )


def render_weight_search_run_markdown(payload: Mapping[str, Any]) -> str:
    generation = _mapping(payload.get("candidate_generation"))
    baseline = _mapping(payload.get("baseline_weight_set"))
    baseline_metrics = _mapping(baseline.get("metrics"))
    lines = [
        f"# ETF Weight Calibration Search - {payload.get('search_run_id')}",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "- 本报告只生成 candidate initial weights，不修改 production baseline weights。",
        "",
        "## Search Configuration",
        "",
        f"- Search ID: {payload.get('search_id')}",
        f"- Search Config Hash: `{payload.get('search_config_hash')}`",
        f"- Market Regime: {payload.get('market_regime')}",
        (
            "- Requested Date Range: "
            f"{_mapping(payload.get('requested_date_range')).get('start')} to "
            f"{_mapping(payload.get('requested_date_range')).get('end')}"
        ),
        f"- Data Quality Status: {payload.get('data_quality_status')}",
        (
            "- Candidate Grid: "
            f"{generation.get('evaluated_candidate_count')} evaluated / "
            f"{generation.get('total_valid_candidate_count')} valid; "
            f"method={generation.get('selection_method')}"
        ),
        "",
        "## Baseline",
        "",
        f"- Baseline Weight Set: {baseline.get('weight_set_id')}",
        (
            "- Baseline Metrics: "
            f"total_return={_fmt_pct(baseline_metrics.get('total_return'))}; "
            f"max_drawdown={_fmt_pct(baseline_metrics.get('max_drawdown'))}; "
            f"Sharpe={_fmt_number(baseline_metrics.get('sharpe'))}"
        ),
        "",
        "## Top Historical Candidates",
        "",
        (
            "| Rank | Candidate | Score | Status | Total Return | Excess vs Baseline | "
            "Max Drawdown | Turnover vs Baseline | Blockers |"
        ),
        "|---:|---|---:|---|---:|---:|---:|---:|---|",
    ]
    metrics_by_id = {row["candidate_id"]: row for row in _records(payload.get("metrics"))}
    for row in _records(payload.get("ranking"))[:10]:
        metrics = metrics_by_id.get(str(row.get("candidate_id")), {})
        lines.append(
            "| "
            f"{row.get('rank')} | "
            f"{row.get('candidate_id')} | "
            f"{_fmt_number(row.get('candidate_score'))} | "
            f"{row.get('candidate_status')} | "
            f"{_fmt_pct(metrics.get('total_return'))} | "
            f"{_fmt_pct(metrics.get('excess_return_vs_baseline'))} | "
            f"{_fmt_pct(metrics.get('max_drawdown'))} | "
            f"{_fmt_number(metrics.get('turnover_vs_baseline'))} | "
            f"{', '.join(row.get('hard_blockers') or []) or 'none'} |"
        )
    lines.extend(
        [
            "",
            "## Runtime Outputs",
            "",
            "- `summary.json` / `summary.md`: historical search run payload.",
            "- `metrics.csv`: candidate scoring and risk metrics.",
            "- `ranking.json`: deterministic candidate ranking.",
            "- `robustness.json`: walk-forward and regime-slice robustness diagnostics.",
            "- `candidate_weight_sets.json/csv`: candidate-only weight sets.",
        ]
    )
    return "\n".join(lines) + "\n"


def weight_search_config_to_json(registry: ETFWeightSearchRegistry) -> str:
    return (
        json.dumps(registry.model_dump(mode="json"), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n"
    )


def build_weight_robustness_evaluation(
    *,
    candidate_id: str,
    candidate_daily: pd.DataFrame,
    baseline_daily: pd.DataFrame,
    weights: Mapping[str, float],
    search: ETFWeightSearchDefinition,
    etf_config: ETFConfigBundle,
    objective: ETFWeightObjectivePolicy,
) -> dict[str, Any]:
    semiconductor_symbols = _semiconductor_symbols(etf_config, search.universe)
    slice_definitions = _robustness_slice_definitions(
        candidate_daily,
        search=search,
    )
    slice_metrics = [
        _metrics_for_slice(
            candidate_daily,
            baseline_daily=baseline_daily,
            weights=weights,
            semiconductor_symbols=semiconductor_symbols,
            slice_definition=slice_definition,
        )
        for slice_definition in slice_definitions
    ]
    summary = summarize_weight_robustness(
        slice_metrics,
        objective=objective,
    )
    return {
        "candidate_id": candidate_id,
        "schema_version": "etf_weight_candidate_robustness_v1",
        "slice_method": "price_derived_regime_proxy_v1",
        "slice_metrics": slice_metrics,
        "summary": summary,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }


def summarize_weight_robustness(
    slice_metrics: list[dict[str, Any]],
    *,
    objective: ETFWeightObjectivePolicy,
) -> dict[str, Any]:
    valid = [
        item
        for item in slice_metrics
        if item.get("status") == "AVAILABLE" and item.get("slice_type") != "full_period"
    ]
    if not valid:
        return {
            "status": "INSUFFICIENT_DATA",
            "evaluated_slice_count": 0,
            "positive_excess_slice_count": 0,
            "weak_slice_count": 0,
            "insufficient_slice_count": len(slice_metrics),
            "stability_score": 0.0,
            "weakest_slice_id": None,
            "reason_codes": ["NO_AVAILABLE_ROBUSTNESS_SLICES"],
        }
    thresholds = objective.hard_blocker_thresholds
    drawdown_tolerance = float(thresholds.get("slice_drawdown_worsening_tolerance", 0.02))
    dispersion_scale = float(thresholds.get("robustness_dispersion_scale", 0.10))
    positive_excess = [
        item for item in valid if float(item.get("excess_return_vs_baseline") or 0.0) >= 0.0
    ]
    weak_slices = [
        item
        for item in valid
        if float(item.get("excess_return_vs_baseline") or 0.0) < 0.0
        or float(item.get("drawdown_reduction_vs_baseline") or 0.0) < -drawdown_tolerance
    ]
    excess_values = [float(item.get("excess_return_vs_baseline") or 0.0) for item in valid]
    dispersion = pstdev(excess_values) if len(excess_values) > 1 else 0.0
    positive_ratio = len(positive_excess) / len(valid)
    drawdown_ok_ratio = 1.0 - sum(
        1
        for item in valid
        if float(item.get("drawdown_reduction_vs_baseline") or 0.0) < -drawdown_tolerance
    ) / len(valid)
    dispersion_score = _penalty_score(dispersion, dispersion_scale)
    stability_score = (
        0.50 * positive_ratio
        + 0.30 * drawdown_ok_ratio
        + 0.20 * dispersion_score
    )
    weakest = min(
        valid,
        key=lambda item: (
            float(item.get("excess_return_vs_baseline") or 0.0),
            float(item.get("drawdown_reduction_vs_baseline") or 0.0),
            str(item.get("slice_id")),
        ),
    )
    reason_codes = []
    if weak_slices:
        reason_codes.append("WEAK_ROBUSTNESS_SLICES_PRESENT")
    if dispersion > dispersion_scale:
        reason_codes.append("HIGH_EXCESS_RETURN_DISPERSION")
    if not reason_codes:
        reason_codes.append("ROBUSTNESS_SLICES_ACCEPTABLE")
    return {
        "status": "AVAILABLE",
        "evaluated_slice_count": len(valid),
        "positive_excess_slice_count": len(positive_excess),
        "weak_slice_count": len(weak_slices),
        "insufficient_slice_count": sum(
            1 for item in slice_metrics if item.get("status") != "AVAILABLE"
        ),
        "stability_score": round(max(0.0, min(1.0, stability_score)), 6),
        "positive_excess_ratio": round(positive_ratio, 6),
        "drawdown_ok_ratio": round(drawdown_ok_ratio, 6),
        "excess_return_dispersion": dispersion,
        "weakest_slice_id": weakest.get("slice_id"),
        "reason_codes": reason_codes,
    }


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


def _valid_preset_start_date_policy(value: str) -> bool:
    text = str(value).strip()
    if text == "earliest_available":
        return True
    if text.startswith("rolling_") and text.endswith("y"):
        years = text.removeprefix("rolling_").removesuffix("y")
        return years.isdigit() and int(years) > 0
    try:
        date.fromisoformat(text)
    except ValueError:
        return False
    return True


def _valid_preset_end_date_policy(value: str) -> bool:
    text = str(value).strip()
    if text in {"latest_available_or_as_of", "as_of"}:
        return True
    if not text.startswith("fixed:"):
        return False
    try:
        date.fromisoformat(text.removeprefix("fixed:"))
    except ValueError:
        return False
    return True


def _resolve_preset_end_date(
    policy: str,
    *,
    as_of: date | None,
    available_end: date | None,
) -> date:
    text = str(policy).strip()
    if text == "latest_available_or_as_of":
        return available_end or as_of or date.today()
    if text == "as_of":
        return as_of or date.today()
    if text.startswith("fixed:"):
        return date.fromisoformat(text.removeprefix("fixed:"))
    raise WeightCalibrationError(f"unsupported ETF weight calibration end_date_policy: {policy}")


def _resolve_preset_start_date(
    policy: str,
    *,
    end_date: date,
    available_start: date | None,
) -> date:
    text = str(policy).strip()
    if text == "earliest_available":
        if available_start is None:
            raise WeightCalibrationError(
                "ETF weight calibration preset earliest_available requires available_start"
            )
        return available_start
    if text.startswith("rolling_") and text.endswith("y"):
        years = int(text.removeprefix("rolling_").removesuffix("y"))
        return _shift_years(end_date, -years)
    return date.fromisoformat(text)


def _shift_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year + years)
    except ValueError:
        return value.replace(year=value.year + years, day=28)


def _range_preset_payload(preset: Mapping[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in preset.items():
        if isinstance(value, date):
            payload[key] = value.isoformat()
        elif isinstance(value, Mapping):
            payload[key] = dict(value)
        else:
            payload[key] = value
    return payload


def _config_hash(payload: dict[str, Any]) -> str:
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return sha256(normalized.encode("utf-8")).hexdigest()


def _run_static_weight_backtest(
    prices: pd.DataFrame,
    *,
    weights: Mapping[str, float],
    config: ETFConfigBundle,
    start: date,
    end: date,
) -> dict[str, Any]:
    close_pivot = _price_pivot(prices, config.backtest.backtest.price_field)
    trading_dates = [
        item.date()
        for item in close_pivot.index
        if start <= item.date() <= end
    ]
    signal_lag_days = int(config.backtest.backtest.signal_lag_days)
    if len(trading_dates) < signal_lag_days + 2:
        raise WeightCalibrationError("ETF weight search has too few trading dates")
    rows: list[dict[str, Any]] = []
    returns: list[float] = []
    turnovers: list[float] = []
    exposures: list[float] = []
    previous_weights: dict[str, float] | None = None
    equity = 1.0
    clean_weights = {str(symbol): float(value) for symbol, value in weights.items()}
    for index, signal_date in enumerate(trading_dates):
        execution_index = index + signal_lag_days
        return_index = execution_index + 1
        if return_index >= len(trading_dates):
            break
        execution_date = trading_dates[execution_index]
        return_date = trading_dates[return_index]
        accounting = calculate_portfolio_accounting_step(
            close_pivot,
            signal_date=signal_date,
            execution_date=execution_date,
            return_date=return_date,
            target_weights=clean_weights,
            previous_weights=previous_weights,
            asset_symbols=tuple(config.assets.assets),
            total_cost_bps=_total_cost_bps(config),
            starting_equity=equity,
        )
        equity = accounting.ending_equity
        returns.append(accounting.strategy_return)
        turnovers.append(accounting.turnover)
        exposures.append(1.0 - clean_weights.get("CASH", 0.0))
        rows.append(
            {
                "signal_date": signal_date.isoformat(),
                "execution_date": execution_date.isoformat(),
                "return_date": return_date.isoformat(),
                "strategy_return": accounting.strategy_return,
                "gross_return": accounting.gross_return,
                "turnover": accounting.turnover,
                "transaction_cost": accounting.transaction_cost,
                "portfolio_equity": equity,
                "target_weights_json": json.dumps(
                    clean_weights,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "asset_returns_json": json.dumps(
                    accounting.period_returns,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "asset_contributions_json": json.dumps(
                    accounting.asset_contributions,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            }
        )
        previous_weights = clean_weights
    metrics = summarize_long_only_backtest(returns, exposures, turnovers)
    return {
        "daily": pd.DataFrame(rows),
        "metrics": metrics,
        "first_signal_date": rows[0]["signal_date"],
        "last_signal_date": rows[-1]["signal_date"],
        "row_count": len(rows),
    }


def _run_benchmark_set_backtests(
    prices: pd.DataFrame,
    *,
    config: ETFConfigBundle,
    registry: ETFWeightSearchRegistry,
    search: ETFWeightSearchDefinition,
    start: date,
    end: date,
) -> dict[str, dict[str, Any]]:
    benchmark_set = registry.benchmark_sets[search.benchmark_set]
    benchmark_results: dict[str, dict[str, Any]] = {}
    for benchmark_id in benchmark_set.benchmark_ids:
        benchmark = config.backtest.backtest.benchmarks.get(benchmark_id)
        if benchmark is None:
            benchmark_results[benchmark_id] = {
                "status": "MISSING",
                "metric_null_reasons": {"benchmark": "benchmark_id_missing"},
            }
            continue
        try:
            weights = _benchmark_static_weights(benchmark)
            result = _run_static_weight_backtest(
                prices,
                weights=weights,
                config=config,
                start=start,
                end=end,
            )
        except WeightCalibrationError as exc:
            benchmark_results[benchmark_id] = {
                "status": "MISSING",
                "benchmark_name": benchmark.name,
                "metric_null_reasons": {"benchmark": str(exc)},
            }
            continue
        benchmark_results[benchmark_id] = {
            "status": "AVAILABLE",
            "benchmark_name": benchmark.name,
            "benchmark_type": benchmark.benchmark_type,
            **_metrics_payload(result["metrics"]),
        }
    return benchmark_results


def _candidate_metric_row(
    *,
    candidate_id: str,
    weights: Mapping[str, float],
    candidate_metrics: BacktestMetrics,
    baseline_metrics: BacktestMetrics,
    benchmark_results: Mapping[str, Mapping[str, Any]],
    objective: ETFWeightObjectivePolicy,
    search: ETFWeightSearchDefinition,
    semiconductor_symbols: set[str],
    baseline_weights: Mapping[str, float],
    robustness_score: float,
    robustness_summary: Mapping[str, Any],
) -> dict[str, Any]:
    benchmark_comparison = _benchmark_comparison(candidate_metrics, benchmark_results)
    turnover_vs_baseline = sum(
        abs(float(weights.get(symbol, 0.0)) - float(baseline_weights.get(symbol, 0.0)))
        for symbol in set(weights) | set(baseline_weights)
    )
    excess_return = candidate_metrics.total_return - baseline_metrics.total_return
    drawdown_reduction = abs(baseline_metrics.max_drawdown) - abs(
        candidate_metrics.max_drawdown
    )
    component_scores = _historical_component_scores(
        candidate_metrics=candidate_metrics,
        excess_return=excess_return,
        drawdown_reduction=drawdown_reduction,
        turnover_vs_baseline=turnover_vs_baseline,
        weights=weights,
        objective=objective,
        robustness_score=robustness_score,
    )
    hard_blockers = _candidate_hard_blockers(
        weights=weights,
        candidate_metrics=candidate_metrics,
        benchmark_comparison=benchmark_comparison,
        turnover_vs_baseline=turnover_vs_baseline,
        objective=objective,
        search=search,
        semiconductor_symbols=semiconductor_symbols,
    )
    candidate_score = (
        0.0
        if hard_blockers
        else sum(
            float(objective.component_weights[name]) * float(component_scores[name])
            for name in objective.component_weights
        )
    )
    status = "ranked"
    if hard_blockers:
        status = "blocked" if _blocking_safety_issue(hard_blockers) else "rejected"
    return {
        "candidate_id": candidate_id,
        "candidate_status": status,
        "candidate_score": round(candidate_score, 6),
        "total_return": candidate_metrics.total_return,
        "CAGR": candidate_metrics.cagr,
        "max_drawdown": candidate_metrics.max_drawdown,
        "Sharpe": candidate_metrics.sharpe,
        "Sortino": candidate_metrics.sortino,
        "Calmar": candidate_metrics.calmar,
        "turnover": candidate_metrics.turnover,
        "turnover_vs_baseline": turnover_vs_baseline,
        "excess_return_vs_baseline": excess_return,
        "drawdown_reduction_vs_baseline": drawdown_reduction,
        "benchmark_comparison": benchmark_comparison,
        "component_scores": component_scores,
        "robustness_summary": dict(robustness_summary),
        "regime_robustness_score": robustness_score,
        "hard_blockers": hard_blockers,
        "ranking_reason": _ranking_reasons(component_scores, hard_blockers),
        "weights": dict(weights),
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _empty_candidate_weight_registry() -> dict[str, Any]:
    return {
        "schema_version": CANDIDATE_WEIGHT_REGISTRY_SCHEMA_VERSION,
        "registry_type": "etf_candidate_weight_sets",
        "updated_at": None,
        "candidate_count": 0,
        "weight_sets": [],
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }


def _empty_weight_forward_enrollment_registry() -> dict[str, Any]:
    return {
        "schema_version": WEIGHT_FORWARD_ENROLLMENT_SCHEMA_VERSION,
        "registry_type": "etf_weight_forward_enrollments",
        "updated_at": None,
        "enrollment_count": 0,
        "enrollments": [],
        "latest_selection": {"selected_at": None, "weight_set_ids": []},
        "shared_shadow_registry_mutated": False,
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }


def _selected_candidate_weight_records(
    registry: Mapping[str, Any],
    *,
    top: int | None,
    weight_set_ids: list[str] | None,
) -> list[dict[str, Any]]:
    records = sorted(
        _records(registry.get("weight_sets")),
        key=lambda item: (int(item.get("rank") or 999_999), str(item.get("weight_set_id"))),
    )
    requested = {str(item) for item in weight_set_ids or []}
    if requested:
        selected = [
            record
            for record in records
            if str(record.get("weight_set_id")) in requested
            or str(record.get("source_candidate_id")) in requested
        ]
        matched = {
            str(record.get("weight_set_id")) for record in selected
        } | {str(record.get("source_candidate_id")) for record in selected}
        missing = sorted(requested - matched)
        if missing:
            raise WeightCalibrationError(
                "unknown candidate weight set id(s): " + ", ".join(missing)
            )
        return selected
    limit = top or 1
    if limit <= 0:
        raise WeightCalibrationError("top must be positive")
    return records[:limit]


def _selected_top_export_candidates(
    top_export_payload: Mapping[str, Any],
    *,
    top: int | None,
    weight_set_ids: list[str] | None,
) -> list[dict[str, Any]]:
    candidates = sorted(
        _records(top_export_payload.get("candidates")),
        key=lambda item: (int(item.get("rank") or 999_999), str(item.get("weight_set_id"))),
    )
    requested = {str(item) for item in weight_set_ids or []}
    if requested:
        selected = [
            candidate
            for candidate in candidates
            if str(candidate.get("weight_set_id")) in requested
            or str(candidate.get("source_candidate_id")) in requested
        ]
        matched = {
            str(candidate.get("weight_set_id")) for candidate in selected
        } | {str(candidate.get("source_candidate_id")) for candidate in selected}
        missing = sorted(requested - matched)
        if missing:
            raise WeightCalibrationError(
                "unknown top candidate weight set id(s): " + ", ".join(missing)
            )
        return selected
    limit = top or 3
    if limit <= 0:
        raise WeightCalibrationError("top must be positive")
    return candidates[:limit]


def _candidate_weight_record_from_shadow_ready_top_candidate(
    search_payload: Mapping[str, Any],
    top_candidate: Mapping[str, Any],
    *,
    source_links: Mapping[str, str],
    created_at: datetime,
) -> dict[str, Any]:
    weight_set_id = str(top_candidate.get("weight_set_id"))
    readiness = str(top_candidate.get("forward_readiness_status") or "")
    if readiness != "shadow_ready":
        raise WeightCalibrationError(
            "top weight candidate cannot enroll unless forward_readiness_status is "
            f"shadow_ready: {weight_set_id}"
        )
    if top_candidate.get("blockers"):
        raise WeightCalibrationError(
            f"blocked top weight candidate cannot enroll: {weight_set_id}"
        )
    ranked = _ranked_row_for_top_candidate(search_payload, top_candidate)
    if not ranked:
        raise WeightCalibrationError(
            f"top weight candidate is not linked to search ranking: {weight_set_id}"
        )
    record = _candidate_weight_record_from_search(
        search_payload,
        ranked,
        created_at=created_at,
    )
    record["status"] = "shadow_ready"
    record["selection_reason"] = "top_weight_candidate_shadow_enrollment"
    record["forward_readiness_status"] = readiness
    record["warnings"] = [str(item) for item in top_candidate.get("warnings") or []]
    record["source_links"] = dict(source_links)
    record["source_report_path"] = source_links.get("top_candidate_export", "")
    validate_candidate_weight_record(record)
    return record


def _raise_if_candidate_weight_forward_enrollable(record: Mapping[str, Any]) -> None:
    validate_candidate_weight_record(record)
    weight_set_id = str(record.get("weight_set_id"))
    status = str(record.get("status"))
    if status not in FORWARD_ENROLLABLE_CANDIDATE_STATUSES:
        raise WeightCalibrationError(
            "candidate weight set cannot enroll forward unless status is "
            f"candidate/shadow_ready: {weight_set_id}"
        )
    if record.get("blockers"):
        raise WeightCalibrationError(
            f"blocked candidate weight set cannot enroll forward: {weight_set_id}"
        )


def _weight_forward_enrollment_result(
    enrollment: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> dict[str, Any]:
    result = {
        "enrollment_id": enrollment.get("enrollment_id") or enrollment.get("shadow_id"),
        "weight_set_id": enrollment.get("weight_set_id"),
        "shadow_candidate_id": enrollment.get("shadow_candidate_id")
        or enrollment.get("shadow_id"),
        "status": enrollment.get("status"),
        "blockers": [str(item) for item in enrollment.get("blockers") or []],
        "warnings": [str(item) for item in enrollment.get("warnings") or []],
        "source_links": dict(
            _mapping(enrollment.get("source_links"))
            or _mapping(candidate.get("source_links"))
        ),
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    return result


def _weight_forward_enrollment_record(
    candidate: Mapping[str, Any],
    *,
    enrollment_path: Path,
    enrolled_at: datetime,
) -> dict[str, Any]:
    timestamp = enrolled_at.isoformat()
    enrollment_date = enrolled_at.date().isoformat()
    weight_set_id = str(candidate.get("weight_set_id"))
    shadow_id = _weight_forward_shadow_id(weight_set_id)
    enrollment_hash = sha256(weight_set_id.encode("utf-8")).hexdigest()[:16]
    enrollment_id = f"etf_weight_enrollment_{enrollment_hash}"
    tracking_link = f"{enrollment_path}#{shadow_id}"
    source_links = dict(_mapping(candidate.get("source_links")))
    warnings = [str(item) for item in candidate.get("warnings") or []]
    tracking_state = {
        "tracking_status": "active",
        "tracking_started_at": timestamp,
        "tracking_start_date": enrollment_date,
        "cadence": "daily",
        "evidence_status": "needs_more_forward_data",
        "forward_tracking_link": tracking_link,
        "weekly_review_task": "TRADING-068",
        "decision_journal_task": "TRADING-069",
        "parameter_review_task": "TRADING-070",
        "calibration_task": "TRADING-071E",
    }
    shadow_record = {
        "record_type": "etf_weight_calibration_shadow_candidate",
        "shadow_id": shadow_id,
        "shadow_candidate_id": shadow_id,
        "candidate_id": str(candidate.get("source_candidate_id") or weight_set_id),
        "weight_set_id": weight_set_id,
        "source_search_run_id": candidate.get("source_search_run_id"),
        "source_candidate_id": candidate.get("source_candidate_id"),
        "rank": candidate.get("rank"),
        "weights": dict(_mapping(candidate.get("weights"))),
        "status": "active",
        "enrolled_at": timestamp,
        "enrollment_date": enrollment_date,
        "evaluation_schedule": {
            "cadence": "daily",
            "start_date": enrollment_date,
            "weekly_review_task": "TRADING-068",
        },
        "tracking_state": dict(tracking_state),
        "source_links": dict(source_links),
        "warnings": list(warnings),
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
        "production_weights_mutated": False,
        "applied_weight_set": None,
    }
    record = {
        "enrollment_id": enrollment_id,
        "shadow_id": shadow_id,
        "shadow_candidate_id": shadow_id,
        "weight_set_id": weight_set_id,
        "source_search_run_id": candidate.get("source_search_run_id"),
        "source_candidate_id": candidate.get("source_candidate_id"),
        "rank": int(candidate.get("rank") or 0),
        "status": "active",
        "weights": dict(_mapping(candidate.get("weights"))),
        "metrics_summary": dict(_mapping(candidate.get("metrics_summary"))),
        "robustness_summary": dict(_mapping(candidate.get("robustness_summary"))),
        "blockers": [str(item) for item in candidate.get("blockers") or []],
        "warnings": warnings,
        "selection_reason": candidate.get("selection_reason"),
        "config_hash": candidate.get("config_hash"),
        "source_links": source_links,
        "enrolled_at": timestamp,
        "enrollment_date": enrollment_date,
        "forward_tracking_link": tracking_link,
        "tracking_state": tracking_state,
        "shadow_record": shadow_record,
        "shared_shadow_registry_mutated": False,
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "production_promotion_allowed": False,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_weight_forward_enrollment_record(record)
    return record


def _weight_forward_shadow_id(weight_set_id: str) -> str:
    return f"etf_weight_shadow_{sha256(weight_set_id.encode('utf-8')).hexdigest()[:16]}"


def _backtest_forward_evidence_record(
    enrollment: Mapping[str, Any],
    *,
    as_of: date,
    candidate_registry: Mapping[str, Any],
    search_payload: Mapping[str, Any],
    forward_dashboard: Mapping[str, Any],
    weekly_review: Mapping[str, Any],
    decision_journal: Mapping[str, Any],
    parameter_review: Mapping[str, Any],
    generated_at: datetime,
    source_paths: Mapping[str, str],
) -> dict[str, Any]:
    weight_set_id = str(enrollment.get("weight_set_id"))
    candidate = _candidate_weight_record_for_id(candidate_registry, weight_set_id)
    historical = _historical_expectation_for_candidate(
        candidate,
        search_payload=search_payload,
    )
    forward_row = _forward_row_for_enrollment(enrollment, forward_dashboard)
    forward = _forward_realized_metrics(forward_row)
    gaps = _evidence_gaps(historical, forward)
    source_links = _evidence_source_links(
        enrollment,
        forward_row=forward_row,
        weekly_review=weekly_review,
        decision_journal=decision_journal,
        parameter_review=parameter_review,
        source_paths=source_paths,
    )
    status, reasons = _forward_evidence_status(
        candidate=candidate,
        enrollment=enrollment,
        forward_row=forward_row,
        gaps=gaps,
    )
    record = {
        "evidence_id": _stable_evidence_id(weight_set_id, as_of),
        "weight_set_id": weight_set_id,
        "shadow_id": enrollment.get("shadow_id"),
        "source_search_run_id": enrollment.get("source_search_run_id"),
        "source_candidate_id": enrollment.get("source_candidate_id"),
        "rank": int(enrollment.get("rank") or 0),
        "generated_at": generated_at.isoformat(),
        "as_of": as_of.isoformat(),
        "candidate_status": candidate.get("status"),
        "enrollment_status": enrollment.get("status"),
        "forward_days": int(forward.get("forward_days") or 0),
        "backtest_expected_return": historical["return"],
        "forward_realized_return": forward["return"],
        "expectation_gap": gaps["return_gap"],
        "backtest_expected_drawdown": historical["drawdown"],
        "forward_realized_drawdown": forward["drawdown"],
        "drawdown_gap": gaps["drawdown_gap"],
        "backtest_expected_turnover": historical["turnover"],
        "forward_realized_turnover": forward["turnover"],
        "turnover_gap": gaps["turnover_gap"],
        "backtest_expected_stability": historical["stability"],
        "forward_realized_stability": forward["stability"],
        "stability_gap": gaps["stability_gap"],
        "evidence_status": status,
        "status_reasons": reasons,
        "metric_null_reasons": forward.get("metric_null_reasons", {}),
        "source_links": source_links,
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_backtest_forward_evidence_record(record)
    return record


def _candidate_weight_record_for_id(
    registry: Mapping[str, Any],
    weight_set_id: str,
) -> dict[str, Any]:
    for record in _records(registry.get("weight_sets")):
        if str(record.get("weight_set_id")) == weight_set_id:
            return record
    raise WeightCalibrationError(f"candidate weight_set_id not found: {weight_set_id}")


def _historical_expectation_for_candidate(
    candidate: Mapping[str, Any],
    *,
    search_payload: Mapping[str, Any],
) -> dict[str, float | None]:
    metrics = dict(_mapping(candidate.get("metrics_summary")))
    source_candidate_id = str(candidate.get("source_candidate_id") or "")
    if search_payload:
        search_metrics = _optional_candidate_metrics(search_payload, source_candidate_id)
        if search_metrics:
            metrics.update(search_metrics)
    robustness = _mapping(candidate.get("robustness_summary"))
    if search_payload:
        candidate_payload = _optional_candidate_weight_set(search_payload, source_candidate_id)
        robustness = _mapping(candidate_payload.get("robustness_summary")) or robustness
    return {
        "return": _float_or_none(metrics.get("total_return")),
        "drawdown": _float_or_none(metrics.get("max_drawdown")),
        "turnover": _float_or_none(metrics.get("turnover_vs_baseline")),
        "stability": _float_or_none(robustness.get("stability_score")),
    }


def _forward_realized_metrics(row: Mapping[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {
            "forward_days": 0,
            "return": None,
            "drawdown": None,
            "turnover": None,
            "stability": None,
            "metric_null_reasons": {"forward_row": "FORWARD_EVIDENCE_NOT_FOUND"},
        }
    days = int(_float_or_none(row.get("days_since_enrollment")) or 0)
    metric_null_reasons = dict(_mapping(row.get("metric_null_reasons")))
    metrics = {
        "forward_days": days,
        "return": _float_or_none(row.get("return_since_enrollment")),
        "drawdown": _float_or_none(row.get("max_drawdown_since_enrollment")),
        "turnover": _float_or_none(row.get("turnover_since_enrollment")),
        "stability": _float_or_none(row.get("weight_stability_score")),
        "metric_null_reasons": metric_null_reasons,
    }
    for field, value in metrics.items():
        if field in {"forward_days", "metric_null_reasons"}:
            continue
        if value is None:
            metric_null_reasons.setdefault(field, f"{field} unavailable in forward evidence")
    return metrics


def _evidence_gaps(
    historical: Mapping[str, float | None],
    forward: Mapping[str, Any],
) -> dict[str, float | None]:
    historical_drawdown = _float_or_none(historical.get("drawdown"))
    forward_drawdown = _float_or_none(forward.get("drawdown"))
    drawdown_gap = None
    if historical_drawdown is not None and forward_drawdown is not None:
        drawdown_gap = abs(forward_drawdown) - abs(historical_drawdown)
    return {
        "return_gap": _subtract_optional(forward.get("return"), historical.get("return")),
        "drawdown_gap": drawdown_gap,
        "turnover_gap": _subtract_optional(forward.get("turnover"), historical.get("turnover")),
        "stability_gap": _subtract_optional(forward.get("stability"), historical.get("stability")),
    }


def _forward_evidence_status(
    *,
    candidate: Mapping[str, Any],
    enrollment: Mapping[str, Any],
    forward_row: Mapping[str, Any] | None,
    gaps: Mapping[str, float | None],
) -> tuple[str, list[str]]:
    if str(candidate.get("status")) in {"blocked", "rejected"} or candidate.get("blockers"):
        return "blocked", ["candidate_weight_set_blocked"]
    if str(enrollment.get("status")) not in {"active", "needs_more_forward_data"}:
        return "blocked", [f"enrollment_status_not_active:{enrollment.get('status')}"]
    if not forward_row:
        return "needs_more_forward_data", ["forward_evidence_not_found"]
    minimum_days = int(WEIGHT_FORWARD_EVIDENCE_POLICY["min_forward_days"])
    days = int(_float_or_none(forward_row.get("days_since_enrollment")) or 0)
    if days < minimum_days:
        return "needs_more_forward_data", [f"forward_days_below_minimum:{days}<{minimum_days}"]
    if gaps.get("return_gap") is None or gaps.get("drawdown_gap") is None:
        return "needs_more_forward_data", ["required_forward_gap_metric_missing"]
    return_gap = float(gaps["return_gap"])
    drawdown_gap = float(gaps["drawdown_gap"])
    turnover_gap = _float_or_none(gaps.get("turnover_gap")) or 0.0
    stability_gap = _float_or_none(gaps.get("stability_gap")) or 0.0
    return_tolerance = float(WEIGHT_FORWARD_EVIDENCE_POLICY["return_gap_tolerance"])
    drawdown_tolerance = float(WEIGHT_FORWARD_EVIDENCE_POLICY["drawdown_gap_tolerance"])
    turnover_tolerance = float(WEIGHT_FORWARD_EVIDENCE_POLICY["turnover_gap_tolerance"])
    stability_tolerance = float(WEIGHT_FORWARD_EVIDENCE_POLICY["stability_gap_tolerance"])
    better = return_gap >= return_tolerance
    worse = return_gap <= -return_tolerance
    risk_worse = (
        drawdown_gap > drawdown_tolerance
        or turnover_gap > turnover_tolerance
        or stability_gap < -stability_tolerance
    )
    risk_better = (
        drawdown_gap < -drawdown_tolerance
        or turnover_gap < -turnover_tolerance
        or stability_gap > stability_tolerance
    )
    reasons = [
        f"return_gap={return_gap:.4f}",
        f"drawdown_gap={drawdown_gap:.4f}",
        f"turnover_gap={turnover_gap:.4f}",
        f"stability_gap={stability_gap:.4f}",
    ]
    if better and risk_worse:
        return "mixed", [*reasons, "return_better_but_risk_worse"]
    if worse or risk_worse:
        return "forward_worse_than_backtest", reasons
    if better and not risk_worse:
        return "forward_better_than_backtest", reasons
    if risk_better:
        return "forward_better_than_backtest", reasons
    return "consistent", reasons


def _forward_row_for_enrollment(
    enrollment: Mapping[str, Any],
    forward_dashboard: Mapping[str, Any],
) -> dict[str, Any] | None:
    identifiers = {
        str(enrollment.get("weight_set_id")),
        str(enrollment.get("shadow_id")),
        str(enrollment.get("source_candidate_id")),
        str(_mapping(enrollment.get("shadow_record")).get("candidate_id")),
    }
    identifiers = {item for item in identifiers if item and item != "None"}
    for row in _records(forward_dashboard.get("candidate_summary_table")):
        row_ids = {
            str(row.get("weight_set_id")),
            str(row.get("shadow_id")),
            str(row.get("candidate_id")),
            str(row.get("source_candidate_id")),
        }
        if identifiers & {item for item in row_ids if item and item != "None"}:
            return row
    return None


def _evidence_source_links(
    enrollment: Mapping[str, Any],
    *,
    forward_row: Mapping[str, Any] | None,
    weekly_review: Mapping[str, Any],
    decision_journal: Mapping[str, Any],
    parameter_review: Mapping[str, Any],
    source_paths: Mapping[str, str],
) -> dict[str, Any]:
    weight_set_id = str(enrollment.get("weight_set_id"))
    source_candidate_id = str(enrollment.get("source_candidate_id"))
    return {
        "historical_search": source_paths.get("historical_search", ""),
        "candidate_registry": source_paths.get("candidate_registry", ""),
        "forward_enrollment": source_paths.get("forward_enrollment", ""),
        "forward_dashboard": source_paths.get("forward_dashboard", ""),
        "weekly_review": source_paths.get("weekly_review", ""),
        "decision_journal": source_paths.get("decision_journal", ""),
        "parameter_review": source_paths.get("parameter_review", ""),
        "forward_row_found": forward_row is not None,
        "weekly_review_links": _records_matching_candidate(
            weekly_review,
            candidate_ids={weight_set_id, source_candidate_id, str(enrollment.get("shadow_id"))},
        ),
        "decision_journal_links": _records_matching_candidate(
            decision_journal,
            candidate_ids={weight_set_id, source_candidate_id},
        ),
        "parameter_review_links": _records_matching_candidate(
            parameter_review,
            candidate_ids={weight_set_id, source_candidate_id},
        ),
    }


def _records_matching_candidate(
    payload: Mapping[str, Any],
    *,
    candidate_ids: set[str],
) -> list[dict[str, Any]]:
    ids = {item for item in candidate_ids if item and item != "None"}
    if not payload or not ids:
        return []
    matches: list[dict[str, Any]] = []
    for key in (
        "candidate_summary_table",
        "active_candidates",
        "manual_review_actions",
        "entries",
        "evidence_records",
    ):
        for row in _records(payload.get(key)):
            row_text = json.dumps(row, ensure_ascii=False, sort_keys=True)
            if any(candidate_id in row_text for candidate_id in ids):
                matches.append(row)
    sections = _mapping(payload.get("sections"))
    for section in sections.values():
        if isinstance(section, Mapping):
            matches.extend(_records_matching_candidate(section, candidate_ids=ids))
    return matches


def _status_counts(records: list[Mapping[str, Any]]) -> dict[str, int]:
    counts = {status: 0 for status in sorted(WEIGHT_FORWARD_EVIDENCE_STATUSES)}
    for record in records:
        status = str(record.get("evidence_status") or "needs_more_forward_data")
        if status not in counts:
            counts[status] = 0
        counts[status] += 1
    return counts


def _optional_candidate_metrics(
    search_payload: Mapping[str, Any],
    candidate_id: str,
) -> dict[str, Any]:
    for row in _records(search_payload.get("metrics")):
        if str(row.get("candidate_id")) == candidate_id:
            return row
    return {}


def _optional_candidate_weight_set(
    search_payload: Mapping[str, Any],
    candidate_id: str,
) -> dict[str, Any]:
    for row in _records(search_payload.get("candidate_weight_sets")):
        if str(row.get("candidate_id")) == candidate_id:
            return row
    return {}


def _stable_evidence_id(weight_set_id: str, as_of: date) -> str:
    basis = f"{weight_set_id}|{as_of.isoformat()}"
    return "etf-weight-evidence-" + sha256(basis.encode("utf-8")).hexdigest()[:12]


def _weight_overfit_candidate_diagnostics(
    candidate: Mapping[str, Any],
    *,
    search_payload: Mapping[str, Any],
    evidence_payload: Mapping[str, Any],
) -> dict[str, Any]:
    component_diagnostics = {
        "performance_concentration": _performance_concentration_diagnostic(
            candidate,
            search_payload,
        ),
        "single_period_dependency": _single_period_dependency_diagnostic(
            candidate,
            search_payload,
        ),
        "regime_fragility": _regime_fragility_diagnostic(candidate),
        "turnover_instability": _turnover_instability_diagnostic(candidate),
        "constraint_hit_instability": _constraint_hit_instability_diagnostic(
            candidate,
            search_payload,
        ),
        "weight_extremeness": _weight_extremeness_diagnostic(candidate),
        "benchmark_dependency": _benchmark_dependency_diagnostic(candidate),
        "forward_backtest_divergence": _forward_backtest_divergence_diagnostic(
            candidate,
            evidence_payload,
        ),
    }
    weights = _mapping(WEIGHT_OVERFIT_DIAGNOSTICS_POLICY["component_weights"])
    risk_score = sum(
        float(weights[key]) * float(component_diagnostics[key]["risk_score"])
        for key in weights
    )
    risk_score = round(max(0.0, min(1.0, risk_score)), 6)
    reason_codes = [
        reason
        for diagnostic in component_diagnostics.values()
        for reason in diagnostic.get("reason_codes", [])
    ]
    return {
        "weight_set_id": candidate.get("weight_set_id"),
        "source_search_run_id": candidate.get("source_search_run_id"),
        "source_candidate_id": candidate.get("source_candidate_id"),
        "rank": int(candidate.get("rank") or 0),
        "overfit_risk_score": risk_score,
        "overfit_risk_band": weight_overfit_risk_band(risk_score),
        "component_diagnostics": component_diagnostics,
        "reason_codes": sorted(set(reason_codes)) or ["NO_OVERFIT_RISK_FLAGS"],
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }


def _performance_concentration_diagnostic(
    candidate: Mapping[str, Any],
    search_payload: Mapping[str, Any],
) -> dict[str, Any]:
    returns = _available_slice_values(candidate, search_payload, "excess_return_vs_baseline")
    positives = [max(0.0, value) for value in returns]
    total_positive = sum(positives)
    concentration = 0.0 if total_positive <= 0 else max(positives) / total_positive
    reference = float(WEIGHT_OVERFIT_DIAGNOSTICS_POLICY["performance_concentration_reference"])
    score = _positive_scaled(concentration - 0.30, reference - 0.30)
    return _component_result(
        "performance_concentration",
        score,
        {"positive_slice_concentration": concentration, "available_slice_count": len(returns)},
        ["PERFORMANCE_CONCENTRATED_IN_FEW_SLICES"] if score >= 0.5 else [],
    )


def _single_period_dependency_diagnostic(
    candidate: Mapping[str, Any],
    search_payload: Mapping[str, Any],
) -> dict[str, Any]:
    values = _available_slice_values(candidate, search_payload, "return")
    if len(values) < 2:
        score = 0.0
        metrics = {"available_slice_count": len(values), "dispersion": 0.0}
    else:
        average_abs = sum(abs(value) for value in values) / len(values)
        dispersion = 0.0 if average_abs == 0 else (max(values) - min(values)) / average_abs
        score = max(0.0, min(1.0, dispersion / 4.0))
        metrics = {"available_slice_count": len(values), "dispersion": dispersion}
    return _component_result(
        "single_period_dependency",
        score,
        metrics,
        ["SINGLE_PERIOD_DEPENDENCY"] if score >= 0.5 else [],
    )


def _regime_fragility_diagnostic(candidate: Mapping[str, Any]) -> dict[str, Any]:
    robustness = _mapping(candidate.get("robustness_summary"))
    stability = _float_or_none(robustness.get("stability_score"))
    weak_count = int(_float_or_none(robustness.get("weak_slice_count")) or 0)
    score = max(0.0, min(1.0, 1.0 - (stability if stability is not None else 0.0)))
    if weak_count:
        score = max(score, min(1.0, weak_count / 4.0))
    return _component_result(
        "regime_fragility",
        score,
        {"stability_score": stability, "weak_slice_count": weak_count},
        ["REGIME_FRAGILITY"] if score >= 0.5 else [],
    )


def _turnover_instability_diagnostic(candidate: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _mapping(candidate.get("metrics_summary"))
    turnover = _float_or_none(metrics.get("turnover_vs_baseline")) or 0.0
    reference = float(WEIGHT_OVERFIT_DIAGNOSTICS_POLICY["turnover_reference"])
    score = max(0.0, min(1.0, turnover / reference))
    return _component_result(
        "turnover_instability",
        score,
        {"turnover_vs_baseline": turnover},
        ["HIGH_TURNOVER_INSTABILITY"] if score >= 0.5 else [],
    )


def _constraint_hit_instability_diagnostic(
    candidate: Mapping[str, Any],
    search_payload: Mapping[str, Any],
) -> dict[str, Any]:
    rates = _available_slice_values(candidate, search_payload, "constraint_hit_rate")
    max_rate = max(rates) if rates else 0.0
    reference = float(WEIGHT_OVERFIT_DIAGNOSTICS_POLICY["constraint_hit_reference"])
    score = max(0.0, min(1.0, max_rate / reference)) if reference > 0 else 0.0
    return _component_result(
        "constraint_hit_instability",
        score,
        {"max_constraint_hit_rate": max_rate, "available_slice_count": len(rates)},
        ["CONSTRAINT_HIT_INSTABILITY"] if score >= 0.5 else [],
    )


def _weight_extremeness_diagnostic(candidate: Mapping[str, Any]) -> dict[str, Any]:
    weights = {key: float(value) for key, value in _mapping(candidate.get("weights")).items()}
    max_weight = max(weights.values()) if weights else 0.0
    herfindahl = sum(value * value for value in weights.values())
    reference = float(WEIGHT_OVERFIT_DIAGNOSTICS_POLICY["weight_extremeness_reference"])
    max_score = max(0.0, min(1.0, max_weight / reference))
    herfindahl_score = max(0.0, min(1.0, (herfindahl - 0.20) / 0.80))
    score = max(max_score, herfindahl_score)
    return _component_result(
        "weight_extremeness",
        score,
        {"max_weight": max_weight, "herfindahl": herfindahl},
        ["EXTREME_WEIGHT_CONCENTRATION"] if score >= 0.75 else [],
    )


def _benchmark_dependency_diagnostic(candidate: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _mapping(candidate.get("metrics_summary"))
    benchmark = _mapping(metrics.get("benchmark_comparison"))
    if not benchmark:
        return _component_result(
            "benchmark_dependency",
            1.0,
            {"available_benchmark_count": 0, "non_positive_excess_count": 0},
            ["NO_BENCHMARK_COMPARISON"],
        )
    excess_values = [
        _float_or_none(_mapping(row).get("excess_return"))
        for row in benchmark.values()
        if isinstance(row, Mapping)
    ]
    excess_values = [value for value in excess_values if value is not None]
    if not excess_values:
        score = 1.0
    else:
        score = sum(1 for value in excess_values if value <= 0) / len(excess_values)
    return _component_result(
        "benchmark_dependency",
        score,
        {
            "available_benchmark_count": len(excess_values),
            "non_positive_excess_count": sum(1 for value in excess_values if value <= 0),
        },
        ["BENCHMARK_DEPENDENCY"] if score >= 0.5 else [],
    )


def _forward_backtest_divergence_diagnostic(
    candidate: Mapping[str, Any],
    evidence_payload: Mapping[str, Any],
) -> dict[str, Any]:
    record = _evidence_record_for_weight_set(
        evidence_payload,
        str(candidate.get("weight_set_id")),
    )
    if not record:
        return _component_result(
            "forward_backtest_divergence",
            0.25,
            {"evidence_status": "missing"},
            ["FORWARD_EVIDENCE_MISSING"],
        )
    status = str(record.get("evidence_status"))
    gap = abs(_float_or_none(record.get("expectation_gap")) or 0.0)
    drawdown_gap = max(0.0, _float_or_none(record.get("drawdown_gap")) or 0.0)
    base_score = {
        "consistent": 0.0,
        "forward_better_than_backtest": 0.10,
        "needs_more_forward_data": 0.25,
        "mixed": 0.60,
        "forward_worse_than_backtest": 0.85,
        "blocked": 1.0,
    }.get(status, 0.25)
    score = max(base_score, min(1.0, gap / 0.10), min(1.0, drawdown_gap / 0.10))
    return _component_result(
        "forward_backtest_divergence",
        score,
        {
            "evidence_status": status,
            "expectation_gap_abs": gap,
            "drawdown_gap": drawdown_gap,
        },
        ["FORWARD_BACKTEST_DIVERGENCE"] if score >= 0.5 else [],
    )


def _available_slice_values(
    candidate: Mapping[str, Any],
    search_payload: Mapping[str, Any],
    field: str,
) -> list[float]:
    source_candidate_id = str(candidate.get("source_candidate_id"))
    robustness = _mapping(search_payload.get("robustness_evaluation"))
    for payload in _records(robustness.get("candidate_evaluations")):
        if str(payload.get("candidate_id")) != source_candidate_id:
            continue
        values = [
            _float_or_none(slice_metric.get(field))
            for slice_metric in _records(payload.get("slice_metrics"))
            if slice_metric.get("status") == "AVAILABLE"
        ]
        return [value for value in values if value is not None]
    return []


def _component_result(
    diagnostic_id: str,
    score: float,
    metrics: Mapping[str, Any],
    reason_codes: list[str],
) -> dict[str, Any]:
    normalized = round(max(0.0, min(1.0, float(score))), 6)
    return {
        "diagnostic_id": diagnostic_id,
        "risk_score": normalized,
        "risk_band": weight_overfit_risk_band(normalized),
        "metrics": dict(metrics),
        "reason_codes": reason_codes,
    }


def _risk_band_counts(records: list[Mapping[str, Any]]) -> dict[str, int]:
    counts = {"low": 0, "medium": 0, "high": 0, "critical": 0}
    for record in records:
        band = str(record.get("overfit_risk_band"))
        if band in counts:
            counts[band] += 1
    return counts


def _highest_risk_candidate(records: list[Mapping[str, Any]]) -> dict[str, Any] | None:
    if not records:
        return None
    selected = max(
        records,
        key=lambda record: (
            float(record.get("overfit_risk_score") or 0.0),
            str(record.get("weight_set_id")),
        ),
    )
    return {
        "weight_set_id": selected.get("weight_set_id"),
        "overfit_risk_score": selected.get("overfit_risk_score"),
        "overfit_risk_band": selected.get("overfit_risk_band"),
    }


def _evidence_record_for_weight_set(
    evidence_payload: Mapping[str, Any],
    weight_set_id: str,
) -> dict[str, Any]:
    for record in _records(evidence_payload.get("evidence_records")):
        if str(record.get("weight_set_id")) == weight_set_id:
            return record
    return {}


def _candidate_weight_proposal(
    candidate: Mapping[str, Any],
    *,
    evidence_payload: Mapping[str, Any],
    overfit_payload: Mapping[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    weight_set_id = str(candidate.get("weight_set_id"))
    evidence_record = _evidence_record_for_weight_set(evidence_payload, weight_set_id)
    overfit_record = _overfit_record_for_weight_set(overfit_payload, weight_set_id)
    candidate_metrics = _mapping(candidate.get("metrics_summary"))
    historical_score = _float_or_none(candidate_metrics.get("candidate_score"))
    forward_status = str(evidence_record.get("evidence_status") or "needs_more_forward_data")
    risk_band = str(overfit_record.get("overfit_risk_band") or "medium")
    risk_score = _float_or_none(overfit_record.get("overfit_risk_score"))
    proposal_type, supporting, blocking = _proposal_type_and_evidence(
        candidate=candidate,
        historical_score=historical_score,
        forward_status=forward_status,
        overfit_record=overfit_record,
    )
    proposal = {
        "proposal_id": _stable_proposal_id(weight_set_id, generated_at),
        "weight_set_id": weight_set_id,
        "proposal_type": proposal_type,
        "supporting_evidence": supporting,
        "blocking_evidence": blocking,
        "historical_score": historical_score,
        "forward_evidence_status": forward_status,
        "overfit_risk": {
            "overfit_risk_score": risk_score,
            "overfit_risk_band": risk_band,
            "reason_codes": overfit_record.get("reason_codes", []),
        },
        "manual_review_required": True,
        "application_allowed": False,
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "source_search_run_id": candidate.get("source_search_run_id"),
        "source_candidate_id": candidate.get("source_candidate_id"),
        "generated_at": generated_at.isoformat(),
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_candidate_weight_proposal(proposal)
    return proposal


def _proposal_type_and_evidence(
    *,
    candidate: Mapping[str, Any],
    historical_score: float | None,
    forward_status: str,
    overfit_record: Mapping[str, Any],
) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
    supporting = []
    blocking = []
    weight_set_id = str(candidate.get("weight_set_id"))
    blockers = [str(item) for item in candidate.get("blockers") or []]
    if historical_score is not None:
        supporting.append(
            {
                "evidence_type": "historical_score",
                "value": historical_score,
                "reason_code": "HISTORICAL_SCORE_AVAILABLE",
            }
        )
    risk_band = str(overfit_record.get("overfit_risk_band") or "medium")
    if overfit_record:
        supporting.append(
            {
                "evidence_type": "overfit_diagnostics",
                "value": risk_band,
                "reason_code": "OVERFIT_DIAGNOSTICS_AVAILABLE",
            }
        )
    if blockers or str(candidate.get("status")) in {"blocked", "rejected"}:
        blocking.append(
            {
                "evidence_type": "candidate_registry",
                "value": blockers or [candidate.get("status")],
                "reason_code": "CANDIDATE_REGISTRY_BLOCKED",
            }
        )
        return "reject_weight_set", supporting, blocking
    if risk_band in WEIGHT_PROPOSAL_POLICY["high_overfit_bands"]:
        blocking.append(
            {
                "evidence_type": "overfit_risk",
                "value": risk_band,
                "reason_code": "HIGH_OVERFIT_RISK",
            }
        )
        return "reject_weight_set", supporting, blocking
    if forward_status in {"blocked", "forward_worse_than_backtest"}:
        blocking.append(
            {
                "evidence_type": "forward_evidence",
                "value": forward_status,
                "reason_code": "FORWARD_EVIDENCE_NEGATIVE",
            }
        )
        return "reject_weight_set", supporting, blocking
    if forward_status == "needs_more_forward_data":
        blocking.append(
            {
                "evidence_type": "forward_evidence",
                "value": forward_status,
                "reason_code": "INSUFFICIENT_FORWARD_EVIDENCE",
            }
        )
        return "defer_until_more_forward_data", supporting, blocking
    if forward_status == "mixed":
        return "propose_extended_shadow", supporting, blocking
    score_floor = float(WEIGHT_PROPOSAL_POLICY["historical_score_manual_review_floor"])
    if (
        historical_score is not None
        and historical_score >= score_floor
        and forward_status in {"consistent", "forward_better_than_backtest"}
        and risk_band in {"low", "medium"}
    ):
        supporting.append(
            {
                "evidence_type": "manual_review_readiness",
                "value": weight_set_id,
                "reason_code": "HISTORICAL_AND_FORWARD_EVIDENCE_SUPPORT_REVIEW",
            }
        )
        return "propose_manual_baseline_review", supporting, blocking
    return "continue_forward_observation", supporting, blocking


def _overfit_record_for_weight_set(
    overfit_payload: Mapping[str, Any],
    weight_set_id: str,
) -> dict[str, Any]:
    for record in _records(overfit_payload.get("candidate_diagnostics")):
        if str(record.get("weight_set_id")) == weight_set_id:
            return record
    return {}


def _proposal_type_counts(proposals: list[Mapping[str, Any]]) -> dict[str, int]:
    counts = {proposal_type: 0 for proposal_type in sorted(ALLOWED_WEIGHT_PROPOSAL_TYPES)}
    for proposal in proposals:
        proposal_type = str(proposal.get("proposal_type"))
        counts[proposal_type] = counts.get(proposal_type, 0) + 1
    return counts


def _stable_proposal_id(weight_set_id: str, generated_at: datetime) -> str:
    basis = f"{weight_set_id}|{generated_at.isoformat()}"
    return "etf-weight-proposal-" + sha256(basis.encode("utf-8")).hexdigest()[:12]


def _dual_track_report_status(
    *,
    candidate_registry: Mapping[str, Any],
    evidence_payload: Mapping[str, Any],
    proposals_payload: Mapping[str, Any],
) -> str:
    proposal_counts = _mapping(proposals_payload.get("proposal_type_counts"))
    candidate_count = len(_records(candidate_registry.get("weight_sets")))
    if candidate_count == 0:
        return "needs_more_data"
    if int(proposal_counts.get("propose_manual_baseline_review") or 0) > 0:
        return "manual_review_ready"
    if int(proposal_counts.get("defer_until_more_forward_data") or 0) > 0:
        return "needs_more_forward_data"
    if int(proposal_counts.get("reject_weight_set") or 0) == candidate_count:
        return "blocked"
    evidence_status = str(evidence_payload.get("status") or "")
    if evidence_status == "needs_more_forward_data" or not evidence_payload:
        return "needs_more_forward_data"
    return "observe_only"


def _dual_track_report_summary(
    *,
    candidate_registry: Mapping[str, Any],
    forward_enrollments: Mapping[str, Any],
    evidence_payload: Mapping[str, Any],
    overfit_payload: Mapping[str, Any],
    proposals_payload: Mapping[str, Any],
) -> dict[str, Any]:
    candidates = _records(candidate_registry.get("weight_sets"))
    proposals = _records(proposals_payload.get("proposals"))
    top = min(
        candidates,
        key=lambda item: (int(item.get("rank") or 999_999), str(item.get("weight_set_id"))),
        default={},
    )
    highest = _mapping(overfit_payload.get("highest_risk_candidate"))
    return {
        "candidate_count": len(candidates),
        "enrolled_count": len(_records(forward_enrollments.get("enrollments"))),
        "evidence_record_count": int(evidence_payload.get("evidence_record_count") or 0),
        "proposal_count": len(proposals),
        "manual_review_proposal_count": sum(
            1
            for proposal in proposals
            if proposal.get("proposal_type") == "propose_manual_baseline_review"
        ),
        "top_candidate_id": top.get("weight_set_id"),
        "dominant_forward_evidence_status": evidence_payload.get("status", "needs_more_data"),
        "highest_overfit_risk_band": highest.get("overfit_risk_band"),
        "proposal_type_counts": dict(_mapping(proposals_payload.get("proposal_type_counts"))),
    }


def _dual_track_search_configuration(search_payload: Mapping[str, Any]) -> dict[str, Any]:
    if not search_payload:
        return {
            "status": "missing",
            "search_id": None,
            "search_config_hash": None,
            "market_regime": None,
            "requested_date_range": {"start": None, "end": None},
            "data_quality_status": "missing",
            "candidate_generation": {},
        }
    return {
        "status": "available",
        "search_id": search_payload.get("search_id"),
        "search_run_id": search_payload.get("search_run_id"),
        "search_config_hash": search_payload.get("search_config_hash"),
        "market_regime": search_payload.get("market_regime"),
        "requested_date_range": dict(_mapping(search_payload.get("requested_date_range"))),
        "data_quality_status": search_payload.get("data_quality_status"),
        "candidate_generation": dict(_mapping(search_payload.get("candidate_generation"))),
        "objective_policy": dict(_mapping(search_payload.get("objective_policy"))),
        "benchmark_set": dict(_mapping(search_payload.get("benchmark_set"))),
    }


def _dual_track_top_candidates(
    *,
    candidate_registry: Mapping[str, Any],
    search_payload: Mapping[str, Any],
    top: int = 5,
) -> list[dict[str, Any]]:
    registry_by_source = {
        str(record.get("source_candidate_id")): record
        for record in _records(candidate_registry.get("weight_sets"))
    }
    rows: list[dict[str, Any]] = []
    if search_payload:
        weights_by_candidate = {
            str(record.get("candidate_id")): record
            for record in _records(search_payload.get("candidate_weight_sets"))
        }
        metrics_by_candidate = {
            str(record.get("candidate_id")): record
            for record in _records(search_payload.get("metrics"))
        }
        for ranked in _records(search_payload.get("ranking"))[:top]:
            candidate_id = str(ranked.get("candidate_id"))
            registry_record = registry_by_source.get(candidate_id, {})
            weights_record = weights_by_candidate.get(candidate_id, {})
            metrics = metrics_by_candidate.get(candidate_id, {})
            rows.append(
                {
                    "rank": ranked.get("rank"),
                    "weight_set_id": registry_record.get(
                        "weight_set_id",
                        _weight_set_id(search_payload, ranked),
                    ),
                    "source_candidate_id": candidate_id,
                    "candidate_score": ranked.get("candidate_score"),
                    "status": registry_record.get("status", ranked.get("candidate_status")),
                    "weights": dict(_mapping(weights_record.get("weights"))),
                    "metrics_summary": dict(metrics),
                    "blockers": list(ranked.get("hard_blockers") or []),
                }
            )
        return rows
    for record in sorted(
        _records(candidate_registry.get("weight_sets")),
        key=lambda item: (int(item.get("rank") or 999_999), str(item.get("weight_set_id"))),
    )[:top]:
        rows.append(
            {
                "rank": record.get("rank"),
                "weight_set_id": record.get("weight_set_id"),
                "source_candidate_id": record.get("source_candidate_id"),
                "candidate_score": _mapping(record.get("metrics_summary")).get("candidate_score"),
                "status": record.get("status"),
                "weights": dict(_mapping(record.get("weights"))),
                "metrics_summary": dict(_mapping(record.get("metrics_summary"))),
                "blockers": list(record.get("blockers") or []),
            }
        )
    return rows


def _dual_track_robustness(search_payload: Mapping[str, Any]) -> dict[str, Any]:
    robustness = _mapping(search_payload.get("robustness_evaluation"))
    if not robustness:
        return {
            "status": "missing",
            "summary": {},
            "candidate_summaries": [],
        }
    candidate_summaries = []
    for evaluation in _records(robustness.get("candidate_evaluations"))[:10]:
        candidate_summaries.append(
            {
                "candidate_id": evaluation.get("candidate_id"),
                "summary": dict(_mapping(evaluation.get("summary"))),
                "slice_count": len(_records(evaluation.get("slice_metrics"))),
                "weakest_slice_id": _mapping(evaluation.get("summary")).get("weakest_slice_id"),
            }
        )
    return {
        "status": "available",
        "evaluation_modes": list(robustness.get("evaluation_modes") or []),
        "summary": dict(_mapping(robustness.get("summary"))),
        "candidate_summaries": candidate_summaries,
    }


def _dual_track_overfit(overfit_payload: Mapping[str, Any]) -> dict[str, Any]:
    if not overfit_payload:
        return {
            "status": "missing",
            "risk_counts": {},
            "highest_risk_candidate": None,
            "candidate_diagnostics": [],
        }
    return {
        "status": overfit_payload.get("status", "available"),
        "risk_counts": dict(_mapping(overfit_payload.get("risk_counts"))),
        "highest_risk_candidate": overfit_payload.get("highest_risk_candidate"),
        "candidate_diagnostics": _records(overfit_payload.get("candidate_diagnostics"))[:10],
    }


def _dual_track_forward_evidence(evidence_payload: Mapping[str, Any]) -> dict[str, Any]:
    if not evidence_payload:
        return {
            "status": "missing",
            "status_counts": {},
            "evidence_record_count": 0,
            "evidence_records": [],
        }
    return {
        "status": evidence_payload.get("status"),
        "reason": evidence_payload.get("reason"),
        "status_counts": dict(_mapping(evidence_payload.get("status_counts"))),
        "evidence_record_count": evidence_payload.get("evidence_record_count"),
        "evidence_records": _records(evidence_payload.get("evidence_records"))[:10],
    }


def _dual_track_registry_status(
    candidate_registry: Mapping[str, Any],
    *,
    forward_enrollments: Mapping[str, Any],
) -> dict[str, Any]:
    candidates = _records(candidate_registry.get("weight_sets"))
    status_counts = _value_counts(candidates, "status")
    return {
        "candidate_count": len(candidates),
        "status_counts": status_counts,
        "enrollment_count": len(_records(forward_enrollments.get("enrollments"))),
        "registry_updated_at": candidate_registry.get("updated_at"),
        "enrollment_updated_at": forward_enrollments.get("updated_at"),
    }


def _dual_track_proposal_scorecard(proposals_payload: Mapping[str, Any]) -> dict[str, Any]:
    if not proposals_payload:
        return {
            "status": "missing",
            "proposal_count": 0,
            "proposal_type_counts": {},
            "proposals": [],
        }
    return {
        "status": proposals_payload.get("status"),
        "proposal_count": proposals_payload.get("proposal_count"),
        "proposal_type_counts": dict(_mapping(proposals_payload.get("proposal_type_counts"))),
        "proposals": _records(proposals_payload.get("proposals")),
    }


def _dual_track_manual_review_package(
    *,
    candidate_registry: Mapping[str, Any],
    evidence_payload: Mapping[str, Any],
    overfit_payload: Mapping[str, Any],
    proposals_payload: Mapping[str, Any],
) -> dict[str, Any]:
    shortlist = []
    proposals = _records(proposals_payload.get("proposals"))
    selected_proposals = [
        proposal
        for proposal in proposals
        if proposal.get("proposal_type")
        in {
            "propose_manual_baseline_review",
            "propose_extended_shadow",
            "continue_forward_observation",
        }
    ]
    if not selected_proposals:
        selected_proposals = proposals[:3]
    for proposal in selected_proposals[:5]:
        weight_set_id = str(proposal.get("weight_set_id"))
        candidate = _candidate_record_for_weight_set(candidate_registry, weight_set_id)
        evidence = _evidence_record_for_weight_set(evidence_payload, weight_set_id)
        overfit = _overfit_record_for_weight_set(overfit_payload, weight_set_id)
        shortlist.append(
            {
                "weight_set_id": weight_set_id,
                "proposal_type": proposal.get("proposal_type"),
                "weights": dict(_mapping(candidate.get("weights"))),
                "historical_score": proposal.get("historical_score"),
                "forward_evidence_status": proposal.get("forward_evidence_status"),
                "overfit_risk_band": _mapping(proposal.get("overfit_risk")).get(
                    "overfit_risk_band"
                ),
                "expectation_gap": evidence.get("expectation_gap"),
                "overfit_reason_codes": list(overfit.get("reason_codes") or []),
            }
        )
    blocking_notes = [
        {
            "weight_set_id": proposal.get("weight_set_id"),
            "blocking_evidence": proposal.get("blocking_evidence"),
        }
        for proposal in proposals
        if proposal.get("blocking_evidence")
    ]
    return {
        "manual_review_required": True,
        "application_allowed": False,
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "candidate_shortlist": shortlist,
        "blocking_notes": blocking_notes,
        "review_questions": [
            "historical candidate 是否在 forward evidence 中保持一致？",
            "overfit risk 是否低到足以进入 extended shadow 或 manual baseline review？",
            "是否需要继续收集 forward data 而不是应用权重？",
        ],
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }


def _dual_track_source_links(source_paths: Mapping[str, str]) -> list[dict[str, Any]]:
    links = []
    for source_type, path_text in sorted(source_paths.items()):
        path = Path(str(path_text))
        links.append(
            {
                "source_type": source_type,
                "path": str(path),
                "exists": path.exists(),
            }
        )
    return links


def _dual_track_next_steps(
    *,
    candidate_registry: Mapping[str, Any],
    evidence_payload: Mapping[str, Any],
    proposals_payload: Mapping[str, Any],
) -> list[str]:
    if not _records(candidate_registry.get("weight_sets")):
        return ["Run historical search and register candidate initial weight sets."]
    proposal_counts = _mapping(proposals_payload.get("proposal_type_counts"))
    if int(proposal_counts.get("propose_manual_baseline_review") or 0) > 0:
        return [
            "人工复核 proposal package；确认是否继续 extended shadow，不得自动替换 baseline。",
            "继续收集 forward evidence，并把 owner review 写入 decision journal。",
        ]
    if int(proposal_counts.get("defer_until_more_forward_data") or 0) > 0:
        return [
            "继续 forward observation，等待足够 forward days 后重新运行 aggregate-evidence。",
            "不要用 historical score 单独推动 baseline review。",
        ]
    if evidence_payload.get("status") == "forward_worse_than_backtest":
        return ["复核 forward underperformance 和 overfit diagnostics，优先考虑 reject or defer。"]
    return ["继续 observe-only tracking，并在下一次 weekly review 中复核 candidate shortlist。"]


def _candidate_record_for_weight_set(
    candidate_registry: Mapping[str, Any],
    weight_set_id: str,
) -> dict[str, Any]:
    for record in _records(candidate_registry.get("weight_sets")):
        if str(record.get("weight_set_id")) == weight_set_id:
            return record
    return {}


def _value_counts(records: list[Mapping[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in records:
        key = str(record.get(field) or "missing")
        counts[key] = counts.get(key, 0) + 1
    return counts


def _selected_ranked_candidates(
    search_payload: Mapping[str, Any],
    *,
    top: int | None,
    weight_set_ids: list[str] | None,
) -> list[dict[str, Any]]:
    ranking = _records(search_payload.get("ranking"))
    requested = {str(item) for item in weight_set_ids or []}
    if requested:
        selected = [
            row
            for row in ranking
            if str(row.get("candidate_id")) in requested
            or _weight_set_id(search_payload, row) in requested
        ]
        matched = {
            str(row.get("candidate_id")) for row in selected
        } | {_weight_set_id(search_payload, row) for row in selected}
        missing = sorted(requested - matched)
        if missing:
            raise WeightCalibrationError(
                "unknown candidate weight set id(s): " + ", ".join(missing)
            )
        return selected
    limit = top or 1
    if limit <= 0:
        raise WeightCalibrationError("top must be positive")
    return ranking[:limit]


def _candidate_weight_record_from_search(
    search_payload: Mapping[str, Any],
    ranked: Mapping[str, Any],
    *,
    created_at: datetime,
) -> dict[str, Any]:
    candidate_id = str(ranked.get("candidate_id"))
    candidate = _candidate_weight_set(search_payload, candidate_id)
    metrics = _candidate_metrics(search_payload, candidate_id)
    blockers = [str(item) for item in ranked.get("hard_blockers") or []]
    candidate_status = str(ranked.get("candidate_status"))
    if blockers:
        status = "blocked" if candidate_status == "blocked" else "rejected"
    elif _mapping(metrics.get("robustness_summary")).get("status") == "INSUFFICIENT_DATA":
        status = "needs_more_data"
    else:
        status = "candidate"
    record = {
        "weight_set_id": _weight_set_id(search_payload, ranked),
        "source_search_run_id": search_payload.get("search_run_id"),
        "source_candidate_id": candidate_id,
        "rank": int(ranked.get("rank")),
        "status": status,
        "weights": dict(candidate.get("weights") or ranked.get("weights") or {}),
        "metrics_summary": {
            "candidate_score": ranked.get("candidate_score"),
            "candidate_status": ranked.get("candidate_status"),
            "total_return": metrics.get("total_return"),
            "excess_return_vs_baseline": metrics.get("excess_return_vs_baseline"),
            "max_drawdown": metrics.get("max_drawdown"),
            "turnover_vs_baseline": metrics.get("turnover_vs_baseline"),
            "component_scores": metrics.get("component_scores"),
        },
        "robustness_summary": dict(
            _mapping(
                candidate.get("robustness_summary")
                or metrics.get("robustness_summary")
            )
        ),
        "blockers": blockers,
        "selection_reason": (
            "top_historical_weight_search_rank"
            if not blockers
            else "historical_search_candidate_blocked"
        ),
        "config_hash": search_payload.get("search_config_hash"),
        "created_at": created_at.isoformat(),
        "source_report_path": "",
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_candidate_weight_record(record)
    return record


def _weight_top_candidate_export_record(
    search_payload: Mapping[str, Any],
    ranked: Mapping[str, Any],
    *,
    overfit_payload: Mapping[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    candidate = _candidate_weight_record_from_search(
        search_payload,
        ranked,
        created_at=generated_at,
    )
    candidate_id = str(candidate.get("source_candidate_id"))
    weight_set_id = str(candidate.get("weight_set_id"))
    metrics = _candidate_metrics(search_payload, candidate_id)
    overfit = _overfit_record_for_weight_set(overfit_payload, weight_set_id)
    if not overfit:
        overfit = _weight_overfit_candidate_diagnostics(
            candidate,
            search_payload=search_payload,
            evidence_payload={},
        )
    weights = _json_float_mapping(candidate.get("weights"))
    blockers = [str(item) for item in candidate.get("blockers") or []]
    overfit_band = str(overfit.get("overfit_risk_band") or "medium")
    if overfit_band in {"high", "critical"}:
        blockers = sorted(set(blockers + [f"OVERFIT_RISK_{overfit_band.upper()}"]))
    qqq_comparison = _benchmark_comparison_for_symbol(metrics, "QQQ")
    warnings = _weight_top_candidate_warnings(
        ranked,
        overfit=overfit,
        qqq_comparison=qqq_comparison,
    )
    record = {
        "rank": int(ranked.get("rank") or 0),
        "weight_set_id": weight_set_id,
        "source_search_run_id": candidate.get("source_search_run_id"),
        "source_candidate_id": candidate_id,
        "weights": weights,
        "historical_score": _float_or_none(ranked.get("candidate_score")),
        "cagr": _float_or_none(metrics.get("CAGR")),
        "max_drawdown": _float_or_none(metrics.get("max_drawdown")),
        "sharpe": _float_or_none(metrics.get("Sharpe")),
        "sortino": _float_or_none(metrics.get("Sortino")),
        "calmar": _float_or_none(metrics.get("Calmar")),
        "turnover": _float_or_none(metrics.get("turnover")),
        "semiconductor_exposure": float(weights.get("SMH", 0.0))
        + float(weights.get("SOXX", 0.0)),
        "cash_exposure": float(weights.get("CASH", 0.0)),
        "benchmark_excess_return": _float_or_none(
            qqq_comparison.get("excess_return")
            if qqq_comparison
            else metrics.get("excess_return_vs_baseline")
        ),
        "drawdown_reduction_vs_QQQ": _float_or_none(
            qqq_comparison.get("drawdown_reduction")
        ),
        "overfit_risk": overfit_band,
        "overfit_risk_score": _float_or_none(overfit.get("overfit_risk_score")),
        "forward_readiness_status": _weight_forward_readiness_status(
            candidate,
            overfit,
            blockers=blockers,
        ),
        "blockers": blockers,
        "warnings": warnings,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_weight_top_candidate_export_record(record)
    return record


def _weight_top_candidate_warnings(
    ranked: Mapping[str, Any],
    *,
    overfit: Mapping[str, Any],
    qqq_comparison: Mapping[str, Any],
) -> list[str]:
    warnings = [str(item) for item in ranked.get("ranking_reason") or []]
    reasons = [
        str(item)
        for item in overfit.get("reason_codes") or []
        if str(item) != "NO_OVERFIT_RISK_FLAGS"
    ]
    warnings.extend(reasons)
    if not qqq_comparison:
        warnings.append("QQQ_BENCHMARK_COMPARISON_MISSING")
    return sorted(set(item for item in warnings if item))


def _weight_overfit_explanation_record(
    search_payload: Mapping[str, Any],
    top_candidate: Mapping[str, Any],
    *,
    overfit_payload: Mapping[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    weight_set_id = str(top_candidate.get("weight_set_id"))
    overfit = _overfit_record_for_weight_set(overfit_payload, weight_set_id)
    if not overfit:
        ranked = _ranked_row_for_top_candidate(search_payload, top_candidate)
        candidate = (
            _candidate_weight_record_from_search(
                search_payload,
                ranked,
                created_at=generated_at,
            )
            if ranked
            else _candidate_record_from_top_candidate(top_candidate)
        )
        overfit = _weight_overfit_candidate_diagnostics(
            candidate,
            search_payload=search_payload,
            evidence_payload={},
        )
    band = str(overfit.get("overfit_risk_band") or top_candidate.get("overfit_risk") or "medium")
    record = {
        "candidate_id": weight_set_id,
        "weight_set_id": weight_set_id,
        "source_search_run_id": top_candidate.get("source_search_run_id")
        or overfit.get("source_search_run_id")
        or search_payload.get("search_run_id"),
        "source_candidate_id": top_candidate.get("source_candidate_id")
        or overfit.get("source_candidate_id"),
        "rank": int(top_candidate.get("rank") or overfit.get("rank") or 0),
        "overfit_risk_score": _float_or_none(overfit.get("overfit_risk_score")),
        "overfit_risk_band": band,
        "reason_codes": [str(item) for item in overfit.get("reason_codes") or []],
        "top_overfit_reasons": _top_overfit_reason_explanations(overfit),
        "supporting_metrics": _overfit_supporting_metrics(overfit),
        "blocking_metrics": _blocking_overfit_metrics(overfit),
        "forward_readiness_status": top_candidate.get("forward_readiness_status"),
        "blockers": [str(item) for item in top_candidate.get("blockers") or []],
        "warnings": [str(item) for item in top_candidate.get("warnings") or []],
        "manual_review_note": _overfit_manual_review_note(
            band,
            blockers=[str(item) for item in top_candidate.get("blockers") or []],
        ),
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_weight_overfit_explanation_record(record)
    return record


def _ranked_row_for_top_candidate(
    search_payload: Mapping[str, Any],
    top_candidate: Mapping[str, Any],
) -> dict[str, Any]:
    weight_set_id = str(top_candidate.get("weight_set_id") or "")
    source_candidate_id = str(top_candidate.get("source_candidate_id") or "")
    for ranked in _records(search_payload.get("ranking")):
        if str(ranked.get("candidate_id")) == source_candidate_id:
            return dict(ranked)
        if _weight_set_id(search_payload, ranked) == weight_set_id:
            return dict(ranked)
    return {}


def _candidate_record_from_top_candidate(top_candidate: Mapping[str, Any]) -> dict[str, Any]:
    readiness = str(top_candidate.get("forward_readiness_status") or "")
    status = "candidate" if readiness == "shadow_ready" else "blocked"
    return {
        "weight_set_id": top_candidate.get("weight_set_id"),
        "source_search_run_id": top_candidate.get("source_search_run_id"),
        "source_candidate_id": top_candidate.get("source_candidate_id"),
        "rank": int(top_candidate.get("rank") or 0),
        "status": status,
        "weights": _json_float_mapping(top_candidate.get("weights")),
        "metrics_summary": {
            "candidate_score": top_candidate.get("historical_score"),
            "max_drawdown": top_candidate.get("max_drawdown"),
            "turnover_vs_baseline": top_candidate.get("turnover"),
            "benchmark_comparison": {},
        },
        "robustness_summary": {},
        "blockers": [str(item) for item in top_candidate.get("blockers") or []],
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }


def _top_overfit_reason_explanations(
    overfit: Mapping[str, Any],
    *,
    limit: int = 3,
) -> list[dict[str, Any]]:
    components = [
        (component_id, _mapping(component))
        for component_id, component in _mapping(overfit.get("component_diagnostics")).items()
    ]
    components.sort(
        key=lambda item: (-float(item[1].get("risk_score") or 0.0), str(item[0])),
    )
    selected = [
        item
        for item in components
        if float(item[1].get("risk_score") or 0.0) >= 0.25
        or bool(item[1].get("reason_codes") or [])
    ][:limit]
    if not selected:
        return [
            {
                "reason_id": "NO_MATERIAL_OVERFIT_FLAGS",
                "risk_score": 0.0,
                "risk_band": "low",
                "reason_codes": ["NO_OVERFIT_RISK_FLAGS"],
                "explanation": (
                    "未发现明显的 overfit risk flags；仍需 forward shadow observation，"
                    "不能直接应用为生产权重。"
                ),
                "metrics": {},
            }
        ]
    return [
        {
            "reason_id": component_id,
            "risk_score": _float_or_none(component.get("risk_score")),
            "risk_band": component.get("risk_band"),
            "reason_codes": [str(item) for item in component.get("reason_codes") or []],
            "explanation": _overfit_reason_text(component_id),
            "metrics": dict(_mapping(component.get("metrics"))),
        }
        for component_id, component in selected
    ]


def _overfit_supporting_metrics(overfit: Mapping[str, Any]) -> dict[str, Any]:
    supporting = {}
    for component_id in WEIGHT_OVERFIT_DIAGNOSTICS_POLICY["component_weights"]:
        component = _mapping(_mapping(overfit.get("component_diagnostics")).get(component_id))
        supporting[component_id] = {
            "risk_score": _float_or_none(component.get("risk_score")),
            "risk_band": component.get("risk_band"),
            "reason_codes": [str(item) for item in component.get("reason_codes") or []],
            "metrics": dict(_mapping(component.get("metrics"))),
        }
    return supporting


def _blocking_overfit_metrics(overfit: Mapping[str, Any]) -> list[dict[str, Any]]:
    blocking = []
    for component_id, component in _mapping(overfit.get("component_diagnostics")).items():
        component_map = _mapping(component)
        risk_score = _float_or_none(component_map.get("risk_score")) or 0.0
        reason_codes = [str(item) for item in component_map.get("reason_codes") or []]
        if risk_score < 0.50 and not reason_codes:
            continue
        blocking.append(
            {
                "metric_id": component_id,
                "risk_score": risk_score,
                "risk_band": component_map.get("risk_band"),
                "reason_codes": reason_codes,
                "metrics": dict(_mapping(component_map.get("metrics"))),
            }
        )
    return sorted(blocking, key=lambda item: (-float(item["risk_score"]), item["metric_id"]))


def _overfit_reason_text(component_id: str) -> str:
    reason_text = {
        "performance_concentration": (
            "历史超额收益集中在少数 robustness slices；需要确认候选不是单一窗口驱动。"
        ),
        "single_period_dependency": (
            "不同历史切片的收益分布差异较大；需要复核是否依赖特定行情阶段。"
        ),
        "regime_fragility": (
            "robustness summary 显示稳定性不足或弱切片较多；需要重点查看弱 regime。"
        ),
        "turnover_instability": (
            "相对 baseline 的 turnover 偏高；可能降低 forward shadow 的可执行性。"
        ),
        "constraint_hit_instability": (
            "历史切片中约束命中率偏高；说明候选可能靠边界条件取得表现。"
        ),
        "weight_extremeness": (
            "权重集中度偏高；需要确认单一 sleeve 或资产暴露没有形成不可接受依赖。"
        ),
        "benchmark_dependency": (
            "相对 required benchmarks 的超额表现不稳定；需要避免只优于单一参照物。"
        ),
        "forward_backtest_divergence": (
            "forward evidence 缺失、不足或与历史预期分歧；进入 shadow 前需要继续观察。"
        ),
    }
    return reason_text.get(component_id, "需要人工复核该 overfit diagnostic component。")


def _overfit_manual_review_note(
    overfit_risk_band: str,
    *,
    blockers: list[str],
) -> str:
    if overfit_risk_band in {"high", "critical"}:
        return (
            "该候选应保持 blocked_by_overfit_risk，除非人工复核能解释高风险组件并补充"
            " forward shadow validation evidence。"
        )
    if blockers:
        return (
            "该候选存在 historical blockers，不能静默进入 shadow；需要逐项复核 blockers、"
            "data quality 和 benchmark comparison。"
        )
    if overfit_risk_band == "medium":
        return (
            "该候选可作为人工复核对象，但进入 shadow 前应确认主要 overfit reasons、"
            "weak regimes 和 benchmark excess 是否可接受。"
        )
    return (
        "未发现明显 overfit 阻断项；该结论只支持 candidate-only forward shadow review，"
        "不支持 production weight replacement。"
    )


def _weight_recommendation_run_metadata(search_payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "search_run_id": search_payload.get("search_run_id"),
        "search_id": search_payload.get("search_id"),
        "search_config_hash": search_payload.get("search_config_hash"),
        "generated_at": search_payload.get("generated_at"),
        "candidate_count": len(_records(search_payload.get("candidate_weight_sets"))),
        "ranking_count": len(_records(search_payload.get("ranking"))),
        "market_regime": search_payload.get("market_regime"),
    }


def _weight_recommendation_search_constraints(
    search_payload: Mapping[str, Any],
) -> dict[str, Any]:
    generation = _mapping(search_payload.get("candidate_generation"))
    benchmark = _mapping(search_payload.get("benchmark_set"))
    return {
        "search_config_hash": search_payload.get("search_config_hash"),
        "universe": generation.get("universe"),
        "grid_step": generation.get("grid_step"),
        "total_valid_candidate_count": generation.get("total_valid_candidate_count"),
        "evaluated_candidate_count": generation.get("evaluated_candidate_count"),
        "benchmark_set_id": benchmark.get("benchmark_set_id"),
        "benchmark_ids": benchmark.get("benchmark_ids"),
        "bounded_search": True,
        "production_effect": "none",
        "broker_action": "none",
    }


def _weight_recommendation_benchmark_comparison(
    comparison_payload: Mapping[str, Any],
) -> dict[str, Any]:
    rows = _records(comparison_payload.get("comparison_rows"))
    return {
        "schema_version": comparison_payload.get("schema_version"),
        "row_count": len(rows),
        "rows": rows,
    }


def _weight_recommendation_regime_robustness(
    regime_payload: Mapping[str, Any],
) -> dict[str, Any]:
    matrix = _records(regime_payload.get("matrix"))
    by_weight_set: dict[str, list[dict[str, Any]]] = {}
    for row in matrix:
        by_weight_set.setdefault(str(row.get("weight_set_id")), []).append(row)
    summary = []
    for weight_set_id, rows in sorted(by_weight_set.items()):
        available = [row for row in rows if row.get("status") == "AVAILABLE"]
        drawdowns = [
            _float_or_none(row.get("max_drawdown"))
            for row in available
            if _float_or_none(row.get("max_drawdown")) is not None
        ]
        summary.append(
            {
                "weight_set_id": weight_set_id,
                "available_regime_count": len(available),
                "missing_regime_count": sum(1 for row in rows if row.get("status") != "AVAILABLE"),
                "warning_count": sum(1 for row in rows if row.get("confidence_warning")),
                "worst_max_drawdown": min(drawdowns) if drawdowns else None,
            }
        )
    return {
        "schema_version": regime_payload.get("schema_version"),
        "matrix_row_count": len(matrix),
        "candidate_summary": summary,
    }


def _weight_recommendation_overfit_explanations(
    overfit_payload: Mapping[str, Any],
) -> dict[str, Any]:
    records = _records(overfit_payload.get("explanations"))
    counts: dict[str, int] = {}
    for record in records:
        band = str(record.get("overfit_risk_band") or "missing")
        counts[band] = counts.get(band, 0) + 1
    return {
        "schema_version": overfit_payload.get("schema_version"),
        "record_count": len(records),
        "risk_counts": counts,
        "records": records,
    }


def _weight_recommendation_forward_readiness(
    candidates: list[Mapping[str, Any]],
) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for candidate in candidates:
        status = str(candidate.get("forward_readiness_status") or "missing")
        counts[status] = counts.get(status, 0) + 1
    return {
        "candidate_count": len(candidates),
        "status_counts": counts,
        "shadow_ready_weight_set_ids": [
            str(candidate.get("weight_set_id"))
            for candidate in candidates
            if candidate.get("forward_readiness_status") == "shadow_ready"
            and not candidate.get("blockers")
        ],
    }


def _weight_shadow_enrollment_recommendations(
    candidates: list[Mapping[str, Any]],
    *,
    enrollment_payload: Mapping[str, Any],
) -> dict[str, Any]:
    ready = [
        candidate
        for candidate in candidates
        if candidate.get("forward_readiness_status") == "shadow_ready"
        and not candidate.get("blockers")
    ]
    enrolled_ids = {
        str(record.get("weight_set_id"))
        for record in _records(enrollment_payload.get("enrollments"))
    }
    recommended_ids = [str(candidate.get("weight_set_id")) for candidate in ready[:3]]
    return {
        "suggested_action": (
            "enroll_top_shadow_ready"
            if recommended_ids
            else "needs_manual_review_before_shadow_enrollment"
        ),
        "recommended_weight_set_ids": recommended_ids,
        "already_enrolled_weight_set_ids": [
            weight_set_id for weight_set_id in recommended_ids if weight_set_id in enrolled_ids
        ],
        "already_enrolled_count": sum(1 for item in recommended_ids if item in enrolled_ids),
        "blocked_candidate_count": sum(
            1
            for candidate in candidates
            if candidate.get("forward_readiness_status") != "shadow_ready"
            or candidate.get("blockers")
        ),
        "command": (
            "aits etf weight-calibration enroll-top --latest --top "
            f"{min(3, len(recommended_ids))}"
            if recommended_ids
            else None
        ),
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
    }


def _weight_initial_recommendation_manual_notes(
    candidates: list[Mapping[str, Any]],
) -> list[str]:
    if not candidates:
        return ["缺少 Top-N candidates；需要先运行 historical search / export-top。"]
    notes = [
        "本报告不推荐 production replacement；所有候选只可进入 forward shadow observation。",
        "人工复核时必须同时查看 benchmark comparison、regime robustness 和 overfit explanation。",
    ]
    blocked = [
        str(candidate.get("weight_set_id"))
        for candidate in candidates
        if candidate.get("forward_readiness_status") != "shadow_ready"
        or candidate.get("blockers")
    ]
    if blocked:
        notes.append(
            "以下候选不可 enroll，需先处理 blockers 或 overfit risk: "
            + ", ".join(blocked)
        )
    return notes


def _weight_initial_recommendation_next_steps(
    candidates: list[Mapping[str, Any]],
) -> list[str]:
    ready_count = sum(
        1
        for candidate in candidates
        if candidate.get("forward_readiness_status") == "shadow_ready"
        and not candidate.get("blockers")
    )
    steps = [
        "人工复核 Top-N 权重、benchmark comparison、regime robustness 和 overfit explanation。",
    ]
    if ready_count:
        steps.append("对 shadow_ready 候选运行 enroll-top / enroll，开始 forward observation。")
    else:
        steps.append("暂无 shadow_ready 候选；需要扩大历史验证或处理 blockers。")
    steps.append("等待 forward evidence 后再进入 baseline review；不得自动 promotion。")
    return steps


def _weight_forward_readiness_status(
    candidate: Mapping[str, Any],
    overfit: Mapping[str, Any],
    *,
    blockers: list[str],
) -> str:
    status = str(candidate.get("status") or "")
    if any(
        "DATA_QUALITY" in blocker
        or "NO_LOOKAHEAD" in blocker
        or "CREDIBILITY" in blocker
        for blocker in blockers
    ):
        return "blocked_by_data_quality"
    if str(overfit.get("overfit_risk_band")) in {"high", "critical"}:
        return "blocked_by_overfit_risk"
    if blockers or status in {"blocked", "rejected"}:
        return "blocked_by_risk"
    if status == "needs_more_data":
        return "needs_more_historical_validation"
    if status in {"candidate", "shadow_ready"}:
        return "shadow_ready"
    return "needs_manual_review"


def _benchmark_comparison_for_symbol(
    metrics: Mapping[str, Any],
    symbol: str,
) -> dict[str, Any]:
    target = symbol.upper()
    comparisons = _mapping(metrics.get("benchmark_comparison"))
    for benchmark_id, comparison in comparisons.items():
        row = _mapping(comparison)
        name = str(row.get("benchmark_name") or benchmark_id).upper()
        if target in name:
            return dict(row)
    return {}


def _weight_top_candidate_export_csv_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for candidate in _records(payload.get("candidates")):
        row = {
            key: candidate.get(key)
            for key in [
                "rank",
                "weight_set_id",
                "source_search_run_id",
                "source_candidate_id",
                "historical_score",
                "cagr",
                "max_drawdown",
                "sharpe",
                "sortino",
                "calmar",
                "turnover",
                "semiconductor_exposure",
                "cash_exposure",
                "benchmark_excess_return",
                "drawdown_reduction_vs_QQQ",
                "overfit_risk",
                "overfit_risk_score",
                "forward_readiness_status",
                "observe_only",
                "candidate_only",
                "production_effect",
                "broker_action",
                "manual_review_required",
            ]
        }
        row["weights"] = json.dumps(
            candidate.get("weights") or {},
            ensure_ascii=False,
            sort_keys=True,
        )
        row["blockers"] = json.dumps(candidate.get("blockers") or [], ensure_ascii=False)
        row["warnings"] = json.dumps(candidate.get("warnings") or [], ensure_ascii=False)
        rows.append(row)
    return rows


def _weight_comparison_row(
    *,
    candidate_id: str,
    row_type: str,
    weights: Mapping[str, float],
    metrics: Mapping[str, Any],
    baseline_metrics: Mapping[str, Any],
    qqq_metrics: Mapping[str, Any],
    overfit_risk: str,
    forward_readiness_status: str,
    sort_group: int = 0,
    sort_rank: int = 0,
) -> dict[str, Any]:
    metric_null_reasons = {}
    total_return = _float_or_none(metrics.get("total_return"))
    cagr = _float_or_none(metrics.get("CAGR") or metrics.get("cagr"))
    volatility = _float_or_none(metrics.get("volatility"))
    max_drawdown = _float_or_none(metrics.get("max_drawdown"))
    sharpe = _float_or_none(metrics.get("Sharpe") or metrics.get("sharpe"))
    sortino = _float_or_none(metrics.get("Sortino") or metrics.get("sortino"))
    calmar = _float_or_none(metrics.get("Calmar") or metrics.get("calmar"))
    turnover = _float_or_none(metrics.get("turnover"))
    for key, value in {
        "total_return": total_return,
        "CAGR": cagr,
        "volatility": volatility,
        "max_drawdown": max_drawdown,
        "Sharpe": sharpe,
        "Sortino": sortino,
        "Calmar": calmar,
        "turnover": turnover,
    }.items():
        if value is None:
            metric_null_reasons[key] = "metric_not_available"
    baseline_return = _float_or_none(baseline_metrics.get("total_return"))
    qqq_return = _float_or_none(qqq_metrics.get("total_return"))
    qqq_drawdown = _float_or_none(qqq_metrics.get("max_drawdown"))
    weights_dict = {str(symbol): float(weight) for symbol, weight in weights.items()}
    return {
        "candidate_id": candidate_id,
        "row_type": row_type,
        "weights": weights_dict,
        "total_return": total_return,
        "CAGR": cagr,
        "volatility": volatility,
        "max_drawdown": max_drawdown,
        "Sharpe": sharpe,
        "Sortino": sortino,
        "Calmar": calmar,
        "turnover": turnover,
        "average_cash": float(weights_dict.get("CASH", 0.0)),
        "average_semiconductor_exposure": float(weights_dict.get("SMH", 0.0))
        + float(weights_dict.get("SOXX", 0.0)),
        "excess_return_vs_baseline": _subtract_optional(total_return, baseline_return),
        "excess_return_vs_QQQ": _subtract_optional(total_return, qqq_return),
        "drawdown_reduction_vs_QQQ": (
            None
            if max_drawdown is None or qqq_drawdown is None
            else abs(qqq_drawdown) - abs(max_drawdown)
        ),
        "overfit_risk": overfit_risk,
        "forward_readiness_status": forward_readiness_status,
        "metric_null_reasons": metric_null_reasons,
        "sort_group": sort_group,
        "sort_rank": sort_rank,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }


def _benchmark_weight_comparison_rows(
    search_payload: Mapping[str, Any],
    *,
    baseline_metrics: Mapping[str, Any],
    qqq_metrics: Mapping[str, Any],
) -> list[dict[str, Any]]:
    etf_config = load_etf_config_bundle()
    benchmark_metrics = _mapping(
        _mapping(search_payload.get("benchmark_set")).get("benchmark_metrics")
    )
    rows = []
    required_symbols = {"QQQ": 1, "SPY": 2, "SMH": 3}
    static_rank = 10
    for benchmark_id, metrics in benchmark_metrics.items():
        benchmark = etf_config.backtest.backtest.benchmarks.get(str(benchmark_id))
        if benchmark is None or _mapping(metrics).get("status") != "AVAILABLE":
            continue
        weights = _benchmark_static_weights(benchmark)
        symbol = str(getattr(benchmark, "symbol", "") or "").upper()
        name = str(getattr(benchmark, "name", benchmark_id))
        if symbol in required_symbols and str(benchmark.benchmark_type) == "buy_and_hold":
            rows.append(
                _weight_comparison_row(
                    candidate_id=f"buy_hold_{symbol}",
                    row_type="benchmark",
                    weights=weights,
                    metrics=_mapping(metrics),
                    baseline_metrics=baseline_metrics,
                    qqq_metrics=qqq_metrics,
                    overfit_risk="not_applicable",
                    forward_readiness_status="benchmark_reference",
                    sort_group=1,
                    sort_rank=required_symbols[symbol],
                )
            )
        elif str(benchmark.benchmark_type) == "static_portfolio":
            rows.append(
                _weight_comparison_row(
                    candidate_id=name,
                    row_type="static_reference_candidate",
                    weights=weights,
                    metrics=_mapping(metrics),
                    baseline_metrics=baseline_metrics,
                    qqq_metrics=qqq_metrics,
                    overfit_risk="not_applicable",
                    forward_readiness_status="benchmark_reference",
                    sort_group=2,
                    sort_rank=static_rank,
                )
            )
            static_rank += 1
    return rows


def _top_candidate_comparison_rows(
    search_payload: Mapping[str, Any],
    top_export_payload: Mapping[str, Any],
    *,
    baseline_metrics: Mapping[str, Any],
    qqq_metrics: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows = []
    for candidate in _records(top_export_payload.get("candidates")):
        candidate_id = str(candidate.get("source_candidate_id"))
        metrics = _candidate_metrics(search_payload, candidate_id)
        rows.append(
            _weight_comparison_row(
                candidate_id=str(candidate.get("weight_set_id")),
                row_type="top_N_weight_candidate",
                weights=_json_float_mapping(candidate.get("weights")),
                metrics=metrics,
                baseline_metrics=baseline_metrics,
                qqq_metrics=qqq_metrics,
                overfit_risk=str(candidate.get("overfit_risk")),
                forward_readiness_status=str(candidate.get("forward_readiness_status")),
                sort_group=3,
                sort_rank=int(candidate.get("rank") or 999_999),
            )
        )
    return rows


def _benchmark_metrics_for_symbol(
    search_payload: Mapping[str, Any],
    symbol: str,
) -> dict[str, Any]:
    etf_config = load_etf_config_bundle()
    target = symbol.upper()
    benchmark_metrics = _mapping(
        _mapping(search_payload.get("benchmark_set")).get("benchmark_metrics")
    )
    for benchmark_id, metrics in benchmark_metrics.items():
        benchmark = etf_config.backtest.backtest.benchmarks.get(str(benchmark_id))
        if benchmark is None:
            continue
        if str(getattr(benchmark, "symbol", "") or "").upper() == target:
            return dict(_mapping(metrics))
    return {}


def _sort_weight_comparison_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            int(row.get("sort_group") if row.get("sort_group") is not None else 999),
            int(row.get("sort_rank") if row.get("sort_rank") is not None else 999_999),
            str(row.get("candidate_id")),
        ),
    )


def _weight_candidate_comparison_csv_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in _records(payload.get("comparison_rows")):
        flat = dict(row)
        flat["weights"] = json.dumps(row.get("weights") or {}, ensure_ascii=False, sort_keys=True)
        flat["metric_null_reasons"] = json.dumps(
            row.get("metric_null_reasons") or {},
            ensure_ascii=False,
            sort_keys=True,
        )
        flat.pop("safety", None)
        rows.append(flat)
    return rows


def _required_heatmap_regimes() -> tuple[str, ...]:
    return (
        "risk_on",
        "neutral",
        "risk_off",
        "growth_leadership",
        "semiconductor_leadership",
        "high_volatility",
        "growth_underperformance",
    )


def _weight_regime_robustness_matrix(
    search_payload: Mapping[str, Any],
    top_export_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    evaluations = {
        str(item.get("candidate_id")): item
        for item in _records(
            _mapping(search_payload.get("robustness_evaluation")).get("candidate_evaluations")
        )
    }
    rows = []
    for candidate in _records(top_export_payload.get("candidates")):
        candidate_id = str(candidate.get("source_candidate_id"))
        evaluation = _mapping(evaluations.get(candidate_id))
        slices = {
            _heatmap_regime_for_slice(str(item.get("slice_id"))): item
            for item in _records(evaluation.get("slice_metrics"))
            if _heatmap_regime_for_slice(str(item.get("slice_id")))
        }
        for regime in _required_heatmap_regimes():
            rows.append(
                _weight_regime_robustness_row(
                    candidate,
                    regime=regime,
                    slice_metrics=_mapping(slices.get(regime)),
                )
            )
    return rows


def _heatmap_regime_for_slice(slice_id: str) -> str:
    mapping = {
        "risk_on_periods": "risk_on",
        "neutral_periods": "neutral",
        "risk_off_periods": "risk_off",
        "growth_leadership_periods": "growth_leadership",
        "semiconductor_leadership_periods": "semiconductor_leadership",
        "high_volatility_periods": "high_volatility",
        "growth_underperformance_periods": "growth_underperformance",
    }
    return mapping.get(slice_id, "")


def _weight_regime_robustness_row(
    candidate: Mapping[str, Any],
    *,
    regime: str,
    slice_metrics: Mapping[str, Any],
) -> dict[str, Any]:
    status = str(slice_metrics.get("status") or "MISSING")
    warning = ""
    if not slice_metrics:
        warning = "REGIME_SLICE_MISSING"
    elif status != "AVAILABLE":
        warning = status
    row = {
        "candidate_id": candidate.get("source_candidate_id"),
        "weight_set_id": candidate.get("weight_set_id"),
        "rank": candidate.get("rank"),
        "regime": regime,
        "status": status,
        "return": _float_or_none(slice_metrics.get("return")),
        "excess_return_vs_baseline": _float_or_none(
            slice_metrics.get("excess_return_vs_baseline")
        ),
        "max_drawdown": _float_or_none(slice_metrics.get("max_drawdown")),
        "volatility": _float_or_none(slice_metrics.get("volatility")),
        "Sharpe": _float_or_none(slice_metrics.get("Sharpe")),
        "turnover": _float_or_none(slice_metrics.get("turnover")),
        "constraint_hit_rate": _float_or_none(slice_metrics.get("constraint_hit_rate")),
        "sample_count": int(_float_or_none(slice_metrics.get("row_count")) or 0),
        "confidence_warning": warning,
        "metric_null_reasons": dict(_mapping(slice_metrics.get("metric_null_reasons"))),
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_weight_regime_robustness_row(row)
    return row


def _weight_regime_robustness_csv_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in _records(payload.get("matrix")):
        flat = dict(row)
        flat["metric_null_reasons"] = json.dumps(
            row.get("metric_null_reasons") or {},
            ensure_ascii=False,
            sort_keys=True,
        )
        flat.pop("safety", None)
        rows.append(flat)
    return rows


def _candidate_weight_set(
    search_payload: Mapping[str, Any],
    candidate_id: str,
) -> dict[str, Any]:
    for candidate in _records(search_payload.get("candidate_weight_sets")):
        if str(candidate.get("candidate_id")) == candidate_id:
            return candidate
    raise WeightCalibrationError(
        f"candidate weight set missing from search payload: {candidate_id}"
    )


def _candidate_metrics(
    search_payload: Mapping[str, Any],
    candidate_id: str,
) -> dict[str, Any]:
    for row in _records(search_payload.get("metrics")):
        if str(row.get("candidate_id")) == candidate_id:
            return row
    raise WeightCalibrationError(f"candidate metrics missing from search payload: {candidate_id}")


def _weight_set_id(search_payload: Mapping[str, Any], ranked: Mapping[str, Any]) -> str:
    return f"{search_payload.get('search_run_id')}:{ranked.get('candidate_id')}"


def _historical_component_scores(
    *,
    candidate_metrics: BacktestMetrics,
    excess_return: float,
    drawdown_reduction: float,
    turnover_vs_baseline: float,
    weights: Mapping[str, float],
    objective: ETFWeightObjectivePolicy,
    robustness_score: float,
) -> dict[str, float]:
    scales = objective.component_scales
    sharpe = candidate_metrics.sharpe or 0.0
    nonzero_weights = sum(1 for value in weights.values() if float(value) > 1e-9)
    simplicity = 1.0 - max(0.0, (nonzero_weights - 1) / max(len(weights) - 1, 1))
    return {
        "excess_return_vs_baseline_score": _positive_scaled(
            excess_return,
            scales.get("excess_return_vs_baseline", 0.10),
        ),
        "drawdown_reduction_score": _positive_scaled(
            drawdown_reduction,
            scales.get("drawdown_reduction", 0.10),
        ),
        "risk_adjusted_return_score": _positive_scaled(
            sharpe,
            scales.get("sharpe", 2.0),
        ),
        "turnover_penalty_score": _penalty_score(
            turnover_vs_baseline,
            scales.get("turnover", 1.0),
        ),
        "regime_robustness_score": max(0.0, min(1.0, robustness_score)),
        "simplicity_score": max(0.0, min(1.0, simplicity)),
    }


def _robustness_slice_definitions(
    daily: pd.DataFrame,
    *,
    search: ETFWeightSearchDefinition,
) -> list[dict[str, Any]]:
    frame = _daily_with_slice_features(daily)
    definitions: list[dict[str, Any]] = [
        {
            "slice_id": "full_period",
            "slice_type": "full_period",
            "description": "Full requested historical period.",
            "mask": pd.Series([True] * len(frame), index=frame.index),
        }
    ]
    for window in search.walk_forward_windows:
        signal_dates = pd.to_datetime(frame["signal_date"], errors="coerce")
        definitions.append(
            {
                "slice_id": window.window_id,
                "slice_type": "walk_forward_window",
                "description": window.description,
                "start_date": window.start_date.isoformat(),
                "end_date": window.end_date.isoformat(),
                "mask": (
                    (signal_dates >= pd.Timestamp(window.start_date))
                    & (signal_dates <= pd.Timestamp(window.end_date))
                ),
            }
        )
    regime_masks = {
        "risk_on_periods": frame["_risk_proxy"] == "risk_on",
        "neutral_periods": frame["_risk_proxy"] == "neutral",
        "risk_off_periods": frame["_risk_proxy"] == "risk_off",
        "high_volatility_periods": frame["_high_volatility"],
        "semiconductor_leadership_periods": frame["_semiconductor_leadership"],
        "growth_underperformance_periods": frame["_growth_underperformance"],
    }
    for split_id, split in search.regime_splits.items():
        definitions.append(
            {
                "slice_id": split_id,
                "slice_type": "regime_slice",
                "description": split.description,
                "regime_label": split.regime_label,
                "signal": split.signal,
                "mask": regime_masks.get(
                    split_id,
                    pd.Series([False] * len(frame), index=frame.index),
                ),
            }
        )
    return definitions


def _metrics_for_slice(
    daily: pd.DataFrame,
    *,
    baseline_daily: pd.DataFrame,
    weights: Mapping[str, float],
    semiconductor_symbols: set[str],
    slice_definition: Mapping[str, Any],
) -> dict[str, Any]:
    mask = slice_definition.get("mask")
    if not isinstance(mask, pd.Series):
        selected = daily.iloc[0:0].copy()
    else:
        selected = daily.loc[mask.reindex(daily.index, fill_value=False)].copy()
    common = {
        "slice_id": slice_definition.get("slice_id"),
        "slice_type": slice_definition.get("slice_type"),
        "description": slice_definition.get("description", ""),
        "row_count": int(len(selected)),
    }
    if len(selected) < 2:
        return {
            **common,
            "status": "INSUFFICIENT_DATA",
            "return": None,
            "excess_return_vs_baseline": None,
            "max_drawdown": None,
            "volatility": None,
            "Sharpe": None,
            "Sortino": None,
            "Calmar": None,
            "turnover": None,
            "cash_exposure": float(weights.get("CASH", 0.0)),
            "semiconductor_exposure": sum(
                float(weights.get(symbol, 0.0)) for symbol in semiconductor_symbols
            ),
            "constraint_hit_rate": None,
            "metric_null_reasons": {
                "slice": "insufficient rows for robustness slice metrics"
            },
        }
    returns = [
        float(value)
        for value in pd.to_numeric(selected["strategy_return"], errors="coerce")
        if pd.notna(value)
    ]
    turnovers = [
        float(value)
        for value in pd.to_numeric(selected["turnover"], errors="coerce")
        if pd.notna(value)
    ]
    if len(turnovers) != len(returns):
        turnovers = [0.0] * len(returns)
    exposure = 1.0 - float(weights.get("CASH", 0.0))
    metrics = summarize_long_only_backtest(
        returns,
        [exposure] * len(returns),
        turnovers,
    )
    baseline_returns = _matching_baseline_returns(selected, baseline_daily)
    baseline_return = _compound_returns(baseline_returns) if baseline_returns else None
    candidate_return = metrics.total_return
    drawdown_reduction = None
    if baseline_returns:
        baseline_metrics = summarize_long_only_backtest(
            baseline_returns,
            [exposure] * len(baseline_returns),
            [0.0] * len(baseline_returns),
        )
        drawdown_reduction = abs(baseline_metrics.max_drawdown) - abs(metrics.max_drawdown)
    volatility = pstdev(returns) * sqrt(252) if len(returns) > 1 else None
    return {
        **common,
        "status": "AVAILABLE",
        "return": candidate_return,
        "baseline_return": baseline_return,
        "excess_return_vs_baseline": (
            None if baseline_return is None else candidate_return - baseline_return
        ),
        "max_drawdown": metrics.max_drawdown,
        "drawdown_reduction_vs_baseline": drawdown_reduction,
        "volatility": volatility,
        "Sharpe": metrics.sharpe,
        "Sortino": metrics.sortino,
        "Calmar": metrics.calmar,
        "turnover": metrics.turnover,
        "cash_exposure": float(weights.get("CASH", 0.0)),
        "semiconductor_exposure": sum(
            float(weights.get(symbol, 0.0)) for symbol in semiconductor_symbols
        ),
        "constraint_hit_rate": 0.0,
        "metric_null_reasons": {},
    }


def _candidate_hard_blockers(
    *,
    weights: Mapping[str, float],
    candidate_metrics: BacktestMetrics,
    benchmark_comparison: Mapping[str, Any],
    turnover_vs_baseline: float,
    objective: ETFWeightObjectivePolicy,
    search: ETFWeightSearchDefinition,
    semiconductor_symbols: set[str],
) -> list[str]:
    thresholds = objective.hard_blocker_thresholds
    blockers = []
    if not benchmark_comparison:
        blockers.append("NO_BENCHMARK_COMPARISON")
    max_drawdown_abs = float(thresholds.get("max_drawdown_abs", 0.35))
    if abs(candidate_metrics.max_drawdown) > max_drawdown_abs:
        blockers.append("MAX_DRAWDOWN_TOO_HIGH")
    max_turnover = float(thresholds.get("max_turnover_vs_baseline", 0.80))
    if turnover_vs_baseline > max_turnover:
        blockers.append("TURNOVER_TOO_HIGH")
    semiconductor_total = sum(float(weights.get(symbol, 0.0)) for symbol in semiconductor_symbols)
    if semiconductor_total > search.sleeve_constraints.semiconductor_total_max + 1e-8:
        blockers.append("SEMICONDUCTOR_CAP_VIOLATED")
    cash_weight = float(weights.get("CASH", 0.0))
    if cash_weight < search.weight_constraints["CASH"].min - 1e-8:
        blockers.append("CASH_CONSTRAINT_VIOLATED")
    if search.safety.production_effect != "none":
        blockers.append("UNSAFE_PRODUCTION_EFFECT")
    if search.safety.broker_action != "none":
        blockers.append("BROKER_ACTION_NOT_NONE")
    return [item for item in blockers if item in objective.hard_blockers or item.startswith("NO_")]


def _daily_with_slice_features(daily: pd.DataFrame) -> pd.DataFrame:
    frame = daily.copy()
    asset_returns = [_json_float_mapping(value) for value in frame["asset_returns_json"]]
    spy = [item.get("SPY", 0.0) for item in asset_returns]
    qqq = [item.get("QQQ", 0.0) for item in asset_returns]
    smh = [item.get("SMH", 0.0) for item in asset_returns]
    soxx = [item.get("SOXX", 0.0) for item in asset_returns]
    broad = [(left + right) / 2.0 for left, right in zip(spy, qqq, strict=True)]
    semiconductor = [
        (left + right) / 2.0 for left, right in zip(smh, soxx, strict=True)
    ]
    abs_broad = [abs(value) for value in broad]
    vol_cutoff = _median(abs_broad)
    risk_proxy = []
    for spy_return, qqq_return in zip(spy, qqq, strict=True):
        if spy_return > 0 and qqq_return > 0:
            risk_proxy.append("risk_on")
        elif spy_return < 0 and qqq_return < 0:
            risk_proxy.append("risk_off")
        else:
            risk_proxy.append("neutral")
    frame["_risk_proxy"] = risk_proxy
    frame["_high_volatility"] = [value >= vol_cutoff and value > 0 for value in abs_broad]
    frame["_semiconductor_leadership"] = [
        semi_return > broad_return
        for semi_return, broad_return in zip(semiconductor, broad, strict=True)
    ]
    frame["_growth_underperformance"] = [
        qqq_return < spy_return for qqq_return, spy_return in zip(qqq, spy, strict=True)
    ]
    return frame


def _matching_baseline_returns(
    selected: pd.DataFrame,
    baseline_daily: pd.DataFrame,
) -> list[float]:
    if selected.empty or baseline_daily.empty:
        return []
    baseline_by_signal = {
        str(row["signal_date"]): float(row["strategy_return"])
        for _, row in baseline_daily.iterrows()
    }
    returns = []
    for _, row in selected.iterrows():
        value = baseline_by_signal.get(str(row["signal_date"]))
        if value is not None:
            returns.append(value)
    return returns


def _compound_returns(returns: list[float]) -> float:
    if not returns:
        return 0.0
    return prod(1.0 + value for value in returns) - 1.0


def _robustness_run_summary(robustness_payloads: list[dict[str, Any]]) -> dict[str, Any]:
    summaries = [_mapping(payload.get("summary")) for payload in robustness_payloads]
    available = [item for item in summaries if item.get("status") == "AVAILABLE"]
    if not available:
        return {
            "status": "INSUFFICIENT_DATA",
            "candidate_count": len(summaries),
            "average_stability_score": 0.0,
            "weak_candidate_count": 0,
            "weakest_candidate_id": None,
        }
    scores = [float(item.get("stability_score") or 0.0) for item in available]
    weakest_payload = min(
        robustness_payloads,
        key=lambda payload: (
            float(_mapping(payload.get("summary")).get("stability_score") or 0.0),
            str(payload.get("candidate_id")),
        ),
    )
    return {
        "status": "AVAILABLE",
        "candidate_count": len(summaries),
        "average_stability_score": mean(scores),
        "weak_candidate_count": sum(1 for score in scores if score < 0.5),
        "weakest_candidate_id": weakest_payload.get("candidate_id"),
    }


def _benchmark_comparison(
    candidate_metrics: BacktestMetrics,
    benchmark_results: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    comparison = {}
    for benchmark_id, benchmark in benchmark_results.items():
        if benchmark.get("status") != "AVAILABLE":
            continue
        benchmark_return = _float_or_none(benchmark.get("total_return"))
        benchmark_drawdown = _float_or_none(benchmark.get("max_drawdown"))
        if benchmark_return is None or benchmark_drawdown is None:
            continue
        comparison[benchmark_id] = {
            "benchmark_name": benchmark.get("benchmark_name"),
            "excess_return": candidate_metrics.total_return - benchmark_return,
            "drawdown_reduction": abs(benchmark_drawdown) - abs(
                candidate_metrics.max_drawdown
            ),
        }
    return comparison


def _benchmark_static_weights(benchmark: Any) -> dict[str, float]:
    benchmark_type = str(benchmark.benchmark_type)
    if benchmark_type == "buy_and_hold":
        return {str(benchmark.symbol): 1.0}
    if benchmark_type == "static_portfolio":
        return {str(symbol): float(weight) for symbol, weight in benchmark.weights.items()}
    raise WeightCalibrationError(
        f"benchmark_type {benchmark_type} is not available for TRADING-071B static search"
    )


def _candidate_satisfies_sleeves(
    weights: Mapping[str, float],
    *,
    search: ETFWeightSearchDefinition,
    semiconductor_symbols: set[str],
) -> bool:
    non_cash_total = sum(float(value) for symbol, value in weights.items() if symbol != "CASH")
    if non_cash_total > search.sleeve_constraints.equity_total_max + 1e-8:
        return False
    semiconductor_total = sum(float(weights.get(symbol, 0.0)) for symbol in semiconductor_symbols)
    return semiconductor_total <= search.sleeve_constraints.semiconductor_total_max + 1e-8


def _candidate_selection_key(
    weights: Mapping[str, float],
    baseline_weights: Mapping[str, float],
) -> tuple[float, float, float, tuple[tuple[str, float], ...]]:
    baseline_distance = sum(
        abs(float(weights.get(symbol, 0.0)) - float(baseline_weights.get(symbol, 0.0)))
        for symbol in set(weights) | set(baseline_weights)
    )
    concentration = max(float(value) for value in weights.values())
    cash_distance = abs(float(weights.get("CASH", 0.0)) - float(baseline_weights.get("CASH", 0.0)))
    return (
        round(baseline_distance, 10),
        round(concentration, 10),
        round(cash_distance, 10),
        tuple(sorted((str(symbol), float(weight)) for symbol, weight in weights.items())),
    )


def _default_weights(config: ETFConfigBundle, universe: list[str]) -> dict[str, float]:
    weights = {
        symbol: float(config.assets.assets[symbol].default_weight)
        for symbol in universe
    }
    total = sum(weights.values())
    if abs(total - 1.0) > 1e-6:
        raise WeightCalibrationError("ETF default weights must sum to 1.0 for search universe")
    return weights


def _semiconductor_symbols(config: ETFConfigBundle, universe: list[str]) -> set[str]:
    return {
        symbol
        for symbol in universe
        if symbol in config.assets.assets
        and config.assets.assets[symbol].risk_group == "semiconductor"
    }


def _metrics_payload(metrics: BacktestMetrics) -> dict[str, float | None]:
    return {
        "total_return": metrics.total_return,
        "CAGR": metrics.cagr,
        "max_drawdown": metrics.max_drawdown,
        "sharpe": metrics.sharpe,
        "sortino": metrics.sortino,
        "calmar": metrics.calmar,
        "time_in_market": metrics.time_in_market,
        "turnover": metrics.turnover,
    }


def _ranking_reasons(
    component_scores: Mapping[str, float],
    hard_blockers: list[str],
) -> list[str]:
    if hard_blockers:
        return [f"HARD_BLOCKER:{blocker}" for blocker in hard_blockers]
    ordered = sorted(component_scores.items(), key=lambda item: (-float(item[1]), item[0]))
    return [f"{key}={value:.3f}" for key, value in ordered[:3]]


def _blocking_safety_issue(blockers: list[str]) -> bool:
    return any(blocker.startswith(("UNSAFE_", "BROKER_", "NO_")) for blocker in blockers)


def _positive_scaled(value: float, scale: float) -> float:
    if scale <= 0:
        return 0.0
    return max(0.0, min(1.0, float(value) / float(scale)))


def _penalty_score(value: float, scale: float) -> float:
    if scale <= 0:
        return 0.0
    return max(0.0, min(1.0, 1.0 - float(value) / float(scale)))


def _price_pivot(prices: pd.DataFrame, price_field: str) -> pd.DataFrame:
    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_price"] = pd.to_numeric(frame[price_field], errors="coerce")
    pivot = frame.pivot(index="_date", columns="symbol", values="_price").sort_index()
    return pivot.dropna(how="all")


def _latest_price_date(prices: pd.DataFrame) -> date:
    parsed = pd.to_datetime(prices["date"], errors="coerce").dropna()
    if parsed.empty:
        raise WeightCalibrationError("ETF weight search price data has no valid dates")
    return parsed.max().date()


def _total_cost_bps(config: ETFConfigBundle) -> float:
    costs = config.risk.transaction_costs
    return costs.commission_bps + costs.slippage_bps


def _float_or_none(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed:
        return None
    return parsed


def _subtract_optional(left: object, right: object) -> float | None:
    left_value = _float_or_none(left)
    right_value = _float_or_none(right)
    if left_value is None or right_value is None:
        return None
    return left_value - right_value


def _json_float_mapping(value: object) -> dict[str, float]:
    if isinstance(value, Mapping):
        parsed = value
    else:
        try:
            parsed = json.loads(str(value))
        except (TypeError, json.JSONDecodeError):
            return {}
    if not isinstance(parsed, Mapping):
        return {}
    result = {}
    for key, item in parsed.items():
        parsed_value = _float_or_none(item)
        if parsed_value is not None:
            result[str(key)] = parsed_value
    return result


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: object) -> str:
    return "" if value is None else str(value)


def _artifact_stem(value: object) -> str:
    text = str(value).strip().replace(":", "_").replace("/", "_").replace("\\", "_")
    return "".join(
        character if character.isalnum() or character in "._-" else "_"
        for character in text
    )


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _fmt_pct(value: object) -> str:
    parsed = _float_or_none(value)
    return "n/a" if parsed is None else f"{parsed:.2%}"


def _fmt_number(value: object) -> str:
    parsed = _float_or_none(value)
    return "n/a" if parsed is None else f"{parsed:.3f}"
