from __future__ import annotations

import hashlib
import itertools
import json
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, Self

import pandas as pd
import yaml
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.weight_calibration import (
    WeightProfile,
    load_weight_profile,
)

SCHEMA_VERSION = 1
DEFAULT_SHADOW_WEIGHT_PROFILE_MANIFEST_PATH = (
    PROJECT_ROOT / "config" / "weights" / "shadow_weight_profiles.yaml"
)
DEFAULT_SHADOW_POSITION_GATE_PROFILE_MANIFEST_PATH = (
    PROJECT_ROOT / "config" / "weights" / "shadow_position_gate_profiles.yaml"
)
DEFAULT_SHADOW_PARAMETER_SEARCH_SPACE_PATH = (
    PROJECT_ROOT / "config" / "weights" / "shadow_parameter_search_space.yaml"
)
DEFAULT_SHADOW_PARAMETER_OBJECTIVE_PATH = (
    PROJECT_ROOT / "config" / "weights" / "shadow_parameter_objective.yaml"
)
DEFAULT_SHADOW_PARAMETER_PROMOTION_CONTRACT_PATH = (
    PROJECT_ROOT / "config" / "weights" / "shadow_parameter_promotion_contract.yaml"
)
DEFAULT_SHADOW_WEIGHT_PROFILE_OBSERVATION_LEDGER_PATH = (
    PROJECT_ROOT / "data" / "processed" / "shadow_weight_profile_observations.csv"
)
DEFAULT_SHADOW_WEIGHT_PROFILE_REPORT_DIR = PROJECT_ROOT / "outputs" / "reports"
DEFAULT_SHADOW_PARAMETER_SEARCH_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "parameter_search"
DEFAULT_DECISION_SNAPSHOT_SEARCH_DIR = PROJECT_ROOT / "data" / "processed" / "decision_snapshots"
PRODUCTION_OBSERVED_GATE_PROFILE_ID = "production_observed_gates"
SHADOW_PARAMETER_SEARCH_RESOLVER_VERSION = "shadow_parameter_search_v2"

ShadowProfileStatus = Literal["shadow", "candidate", "retired"]

OBSERVATION_COLUMNS = (
    "as_of",
    "generated_at",
    "profile_id",
    "profile_version",
    "profile_status",
    "production_effect",
    "weight_profile_id",
    "weight_profile_version",
    "gate_profile_id",
    "gate_profile_version",
    "production_score",
    "shadow_score",
    "score_delta_vs_production",
    "production_model_band",
    "shadow_model_band",
    "production_final_band",
    "shadow_final_band",
    "production_model_target_position",
    "production_gated_target_position",
    "shadow_model_target_position",
    "shadow_gated_target_position",
    "gate_cap_max_position",
    "gate_cap_sources",
    "gate_cap_overrides_json",
    "target_weights_json",
    "source_snapshot_path",
)

PERFORMANCE_COLUMNS = (
    "as_of",
    "profile_id",
    "profile_version",
    "horizon_days",
    "outcome_end_date",
    "outcome_status",
    "outcome_reason",
    "asset_return",
    "production_gated_target_position",
    "shadow_gated_target_position",
    "production_turnover",
    "shadow_turnover",
    "production_position_return",
    "shadow_position_return",
    "excess_position_return",
)


class ShadowWeightProfile(BaseModel):
    profile_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    version: str = Field(min_length=1)
    status: ShadowProfileStatus
    owner: str = Field(min_length=1)
    production_effect: Literal["none"] = "none"
    rationale: str = Field(min_length=1)
    review_after_reports: int = Field(gt=0)
    target_weights: dict[str, float]
    metadata: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_target_weights(self) -> Self:
        _validate_weight_mapping(self.target_weights)
        return self


class ShadowWeightProfileManifest(BaseModel):
    version: str = Field(min_length=1)
    status: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    production_effect: Literal["none"] = "none"
    source_weight_profile_path: str = Field(min_length=1)
    label_horizon_days: int = Field(gt=0)
    rationale: str = Field(min_length=1)
    review_after_reports: int = Field(gt=0)
    profiles: tuple[ShadowWeightProfile, ...]

    @model_validator(mode="after")
    def validate_profiles(self) -> Self:
        if not self.profiles:
            raise ValueError("shadow weight profile manifest requires profiles")
        seen: set[str] = set()
        duplicates: list[str] = []
        for profile in self.profiles:
            if profile.profile_id in seen:
                duplicates.append(profile.profile_id)
            seen.add(profile.profile_id)
        if duplicates:
            raise ValueError(
                "shadow weight profile ids must be unique: " + ", ".join(duplicates)
            )
        return self


class ShadowPositionGateProfile(BaseModel):
    profile_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    version: str = Field(min_length=1)
    status: ShadowProfileStatus
    owner: str = Field(min_length=1)
    production_effect: Literal["none"] = "none"
    rationale: str = Field(min_length=1)
    review_after_reports: int = Field(gt=0)
    gate_cap_overrides: dict[str, float] = Field(default_factory=dict)
    metadata: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_gate_cap_overrides(self) -> Self:
        empty_keys = [key for key in self.gate_cap_overrides if not key.strip()]
        if empty_keys:
            raise ValueError("gate_cap_overrides keys must not be empty")
        if "score_model" in self.gate_cap_overrides:
            raise ValueError(
                "score_model cap comes from score-to-position bands; "
                "do not override it in shadow gate profiles"
            )
        out_of_bounds = [
            key
            for key, value in self.gate_cap_overrides.items()
            if value < 0.0 or value > 1.0
        ]
        if out_of_bounds:
            raise ValueError(
                "gate_cap_overrides must be between 0 and 1: "
                + ", ".join(sorted(out_of_bounds))
            )
        return self


class ShadowPositionGateProfileManifest(BaseModel):
    version: str = Field(min_length=1)
    status: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    production_effect: Literal["none"] = "none"
    source_policy_paths: tuple[str, ...] = Field(default_factory=tuple)
    rationale: str = Field(min_length=1)
    review_after_reports: int = Field(gt=0)
    profiles: tuple[ShadowPositionGateProfile, ...]

    @model_validator(mode="after")
    def validate_profiles(self) -> Self:
        if not self.profiles:
            raise ValueError("shadow position gate profile manifest requires profiles")
        seen: set[str] = set()
        duplicates: list[str] = []
        for profile in self.profiles:
            if profile.profile_id in seen:
                duplicates.append(profile.profile_id)
            seen.add(profile.profile_id)
        if duplicates:
            raise ValueError(
                "shadow position gate profile ids must be unique: "
                + ", ".join(duplicates)
            )
        return self


class ShadowWeightGridConfig(BaseModel):
    enabled: bool = True
    signal_values: dict[str, tuple[float, ...]] = Field(default_factory=dict)
    max_candidates: int = Field(default=1000, gt=0)

    @model_validator(mode="after")
    def validate_signal_values(self) -> Self:
        for signal, values in self.signal_values.items():
            if not signal.strip():
                raise ValueError("weight_grid signal keys must not be empty")
            if not values:
                raise ValueError(f"weight_grid signal has no values: {signal}")
            bad_values = [value for value in values if value < 0.0 or value > 1.0]
            if bad_values:
                raise ValueError(
                    f"weight_grid values must be between 0 and 1 for {signal}"
                )
        return self


class ShadowGateGridConfig(BaseModel):
    enabled: bool = False
    cap_values: dict[str, tuple[float, ...]] = Field(default_factory=dict)
    max_candidates: int = Field(default=1000, gt=0)

    @model_validator(mode="after")
    def validate_cap_values(self) -> Self:
        for gate_id, values in self.cap_values.items():
            if not gate_id.strip():
                raise ValueError("gate_grid cap keys must not be empty")
            if gate_id == "score_model":
                raise ValueError(
                    "score_model cap comes from score-to-position bands; "
                    "do not override it in gate_grid"
                )
            if not values:
                raise ValueError(f"gate_grid cap has no values: {gate_id}")
            bad_values = [value for value in values if value < 0.0 or value > 1.0]
            if bad_values:
                raise ValueError(
                    f"gate_grid values must be between 0 and 1 for {gate_id}"
                )
        return self


class ShadowParameterSearchSpaceManifest(BaseModel):
    version: str = Field(min_length=1)
    status: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    production_effect: Literal["none"] = "none"
    source_weight_profile_path: str = Field(min_length=1)
    shadow_weight_profile_manifest_path: str | None = None
    shadow_gate_profile_manifest_path: str | None = None
    include_source_weight_profile: bool = True
    include_shadow_weight_profiles: bool = True
    include_production_observed_gate_profile: bool = True
    include_shadow_gate_profiles: bool = True
    weight_grid: ShadowWeightGridConfig = Field(default_factory=ShadowWeightGridConfig)
    gate_grid: ShadowGateGridConfig = Field(default_factory=ShadowGateGridConfig)
    rationale: str = Field(min_length=1)
    review_after_reports: int = Field(gt=0)


class ShadowParameterObjectiveConfig(BaseModel):
    version: str = Field(min_length=1)
    status: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    production_effect: Literal["none"] = "none"
    primary_metric: Literal["objective_score"] = "objective_score"
    excess_return_weight: float = 1.0
    shadow_return_weight: float = 0.0
    excess_drawdown_penalty: float = Field(default=0.50, ge=0)
    excess_turnover_penalty: float = Field(default=0.00, ge=0)
    missing_sample_penalty: float = Field(default=0.00, ge=0)
    gate_relaxation_penalty: float = Field(default=0.00, ge=0)
    weight_distance_penalty: float = Field(default=0.00, ge=0)
    changed_dimension_penalty: float = Field(default=0.00, ge=0)
    max_l1_distance_from_production: float | None = Field(default=None, ge=0)
    max_single_factor_step: float | None = Field(default=None, ge=0)
    min_available_samples: int = Field(default=1, ge=0)
    require_positive_excess: bool = False
    top_n: int = Field(default=20, gt=0)
    rationale: str = Field(min_length=1)


class ShadowParameterPromotionContractConfig(BaseModel):
    version: str = Field(min_length=1)
    status: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    production_effect: Literal["none"] = "none"
    rationale: str = Field(min_length=1)
    min_available_samples: int = Field(default=30, ge=0)
    require_search_eligible_best: bool = True
    require_positive_excess: bool = True
    max_drawdown_degradation: float | None = Field(default=None, ge=0)
    max_shadow_turnover: float | None = Field(default=None, ge=0)
    gate_primary_driver_requires_cap_review: bool = True
    required_forward_shadow_available_samples: int = Field(default=30, ge=0)
    owner_approval_required: bool = True
    rollback_condition_required: bool = True
    approved_hard_allowed: bool = False


@dataclass(frozen=True)
class ShadowWeightCandidate:
    candidate_id: str
    version: str
    source: str
    target_weights: dict[str, float]


@dataclass(frozen=True)
class ShadowGateCandidate:
    candidate_id: str
    version: str
    source: str
    gate_cap_overrides: dict[str, float]


@dataclass(frozen=True)
class PositionBand:
    min_score: float
    min_position: float
    max_position: float
    label: str


@dataclass(frozen=True)
class ShadowWeightObservation:
    as_of: date
    generated_at: datetime
    profile_id: str
    profile_version: str
    profile_status: str
    production_effect: str
    weight_profile_id: str
    weight_profile_version: str
    gate_profile_id: str
    gate_profile_version: str
    production_score: float
    shadow_score: float
    production_model_band: dict[str, Any]
    shadow_model_band: dict[str, Any]
    production_final_band: dict[str, Any]
    shadow_final_band: dict[str, Any]
    gate_cap_max_position: float
    gate_cap_sources: tuple[str, ...]
    gate_cap_overrides: dict[str, float]
    target_weights: dict[str, float]
    source_snapshot_path: Path

    @property
    def score_delta_vs_production(self) -> float:
        return self.shadow_score - self.production_score

    @property
    def shadow_model_target_position(self) -> float:
        return _band_midpoint(self.shadow_model_band)

    @property
    def shadow_gated_target_position(self) -> float:
        return _band_midpoint(self.shadow_final_band)

    @property
    def production_model_target_position(self) -> float:
        return _band_midpoint(self.production_model_band)

    @property
    def production_gated_target_position(self) -> float:
        return _band_midpoint(self.production_final_band)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "generated_at": self.generated_at.isoformat(),
            "profile_id": self.profile_id,
            "profile_version": self.profile_version,
            "profile_status": self.profile_status,
            "production_effect": self.production_effect,
            "weight_profile_id": self.weight_profile_id,
            "weight_profile_version": self.weight_profile_version,
            "gate_profile_id": self.gate_profile_id,
            "gate_profile_version": self.gate_profile_version,
            "production_score": self.production_score,
            "shadow_score": self.shadow_score,
            "score_delta_vs_production": self.score_delta_vs_production,
            "production_model_band": _band_label(self.production_model_band),
            "shadow_model_band": _band_label(self.shadow_model_band),
            "production_final_band": _band_label(self.production_final_band),
            "shadow_final_band": _band_label(self.shadow_final_band),
            "production_model_target_position": self.production_model_target_position,
            "production_gated_target_position": self.production_gated_target_position,
            "shadow_model_target_position": self.shadow_model_target_position,
            "shadow_gated_target_position": self.shadow_gated_target_position,
            "gate_cap_max_position": self.gate_cap_max_position,
            "gate_cap_sources": ",".join(self.gate_cap_sources),
            "gate_cap_overrides_json": json.dumps(
                self.gate_cap_overrides,
                ensure_ascii=False,
                sort_keys=True,
            ),
            "target_weights_json": json.dumps(
                self.target_weights,
                ensure_ascii=False,
                sort_keys=True,
            ),
            "source_snapshot_path": str(self.source_snapshot_path),
        }


@dataclass(frozen=True)
class ShadowWeightProfileRunReport:
    as_of: date
    generated_at: datetime
    manifest_path: Path
    gate_manifest_path: Path | None
    gate_manifest_version: str | None
    source_weight_profile_path: Path
    decision_snapshot_path: Path
    observation_ledger_path: Path | None
    prediction_ledger_path: Path | None
    manifest: ShadowWeightProfileManifest
    production_score: float
    production_model_band: dict[str, Any]
    production_final_band: dict[str, Any]
    observations: tuple[ShadowWeightObservation, ...]
    warnings: tuple[str, ...]

    @property
    def status(self) -> str:
        if self.warnings:
            return "PASS_WITH_LIMITATIONS"
        return "PASS"

    @property
    def production_effect(self) -> str:
        return "none"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "shadow_weight_profile_run",
            "status": self.status,
            "production_effect": self.production_effect,
            "as_of": self.as_of.isoformat(),
            "generated_at": self.generated_at.isoformat(),
            "manifest_path": str(self.manifest_path),
            "manifest_version": self.manifest.version,
            "gate_manifest_path": (
                None if self.gate_manifest_path is None else str(self.gate_manifest_path)
            ),
            "gate_manifest_version": self.gate_manifest_version,
            "source_weight_profile_path": str(self.source_weight_profile_path),
            "decision_snapshot_path": str(self.decision_snapshot_path),
            "observation_ledger_path": (
                None if self.observation_ledger_path is None else str(self.observation_ledger_path)
            ),
            "prediction_ledger_path": (
                None if self.prediction_ledger_path is None else str(self.prediction_ledger_path)
            ),
            "production_score": self.production_score,
            "production_model_band": self.production_model_band,
            "production_final_band": self.production_final_band,
            "profile_count": len(self.observations),
            "warnings": list(self.warnings),
            "observations": [observation.to_dict() for observation in self.observations],
        }


@dataclass(frozen=True)
class ShadowWeightPerformanceRow:
    as_of: date
    profile_id: str
    profile_version: str
    horizon_days: int
    outcome_end_date: date | None
    outcome_status: str
    outcome_reason: str
    asset_return: float | None
    production_gated_target_position: float | None
    shadow_gated_target_position: float | None
    production_turnover: float | None
    shadow_turnover: float | None
    production_position_return: float | None
    shadow_position_return: float | None

    @property
    def excess_position_return(self) -> float | None:
        if self.production_position_return is None or self.shadow_position_return is None:
            return None
        return self.shadow_position_return - self.production_position_return

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "profile_id": self.profile_id,
            "profile_version": self.profile_version,
            "horizon_days": self.horizon_days,
            "outcome_end_date": (
                "" if self.outcome_end_date is None else self.outcome_end_date.isoformat()
            ),
            "outcome_status": self.outcome_status,
            "outcome_reason": self.outcome_reason,
            "asset_return": _blank_if_none(self.asset_return),
            "production_gated_target_position": _blank_if_none(
                self.production_gated_target_position
            ),
            "shadow_gated_target_position": _blank_if_none(
                self.shadow_gated_target_position
            ),
            "production_turnover": _blank_if_none(self.production_turnover),
            "shadow_turnover": _blank_if_none(self.shadow_turnover),
            "production_position_return": _blank_if_none(
                self.production_position_return
            ),
            "shadow_position_return": _blank_if_none(self.shadow_position_return),
            "excess_position_return": _blank_if_none(self.excess_position_return),
        }


@dataclass(frozen=True)
class ShadowWeightPerformanceSummary:
    profile_id: str
    profile_version: str
    total_count: int
    available_count: int
    pending_count: int
    missing_count: int
    production_total_return: float | None
    shadow_total_return: float | None
    excess_total_return: float | None
    production_max_drawdown: float | None
    shadow_max_drawdown: float | None
    production_turnover: float
    shadow_turnover: float
    shadow_beats_production_rate: float | None


@dataclass(frozen=True)
class ShadowWeightPerformanceReport:
    as_of: date
    since: date | None
    observation_ledger_path: Path
    prices_path: Path
    strategy_ticker: str
    horizon_days: int
    cost_bps: float
    slippage_bps: float
    rows: tuple[ShadowWeightPerformanceRow, ...]
    summaries: tuple[ShadowWeightPerformanceSummary, ...]
    warnings: tuple[str, ...]

    @property
    def status(self) -> str:
        if self.warnings or not self.rows:
            return "PASS_WITH_LIMITATIONS"
        return "PASS"

    @property
    def best_profile(self) -> ShadowWeightPerformanceSummary | None:
        comparable = [
            summary
            for summary in self.summaries
            if summary.excess_total_return is not None and summary.available_count > 0
        ]
        if not comparable:
            return None
        return max(comparable, key=lambda summary: summary.excess_total_return or 0.0)

    @property
    def best_positive_profile(self) -> ShadowWeightPerformanceSummary | None:
        best = self.best_profile
        if best is None or best.excess_total_return is None:
            return None
        if best.excess_total_return <= 0.0:
            return None
        return best


@dataclass(frozen=True)
class ShadowParameterSearchTrial:
    trial_id: str
    weight_candidate_id: str
    weight_candidate_version: str
    gate_candidate_id: str
    gate_candidate_version: str
    target_weights: dict[str, float]
    gate_cap_overrides: dict[str, float]
    total_count: int
    available_count: int
    pending_count: int
    missing_count: int
    production_total_return: float | None
    shadow_total_return: float | None
    excess_total_return: float | None
    production_max_drawdown: float | None
    shadow_max_drawdown: float | None
    production_turnover: float
    shadow_turnover: float
    shadow_beats_production_rate: float | None
    weight_l1_distance_from_production: float
    max_single_factor_step: float
    gate_relaxation_distance: float
    changed_dimension_count: int
    objective_score: float | None
    eligible: bool
    ineligibility_reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "trial_id": self.trial_id,
            "weight_candidate_id": self.weight_candidate_id,
            "weight_candidate_version": self.weight_candidate_version,
            "gate_candidate_id": self.gate_candidate_id,
            "gate_candidate_version": self.gate_candidate_version,
            "total_count": self.total_count,
            "available_count": self.available_count,
            "pending_count": self.pending_count,
            "missing_count": self.missing_count,
            "production_total_return": _blank_if_none(self.production_total_return),
            "shadow_total_return": _blank_if_none(self.shadow_total_return),
            "excess_total_return": _blank_if_none(self.excess_total_return),
            "production_max_drawdown": _blank_if_none(self.production_max_drawdown),
            "shadow_max_drawdown": _blank_if_none(self.shadow_max_drawdown),
            "production_turnover": self.production_turnover,
            "shadow_turnover": self.shadow_turnover,
            "shadow_beats_production_rate": _blank_if_none(
                self.shadow_beats_production_rate
            ),
            "weight_l1_distance_from_production": self.weight_l1_distance_from_production,
            "max_single_factor_step": self.max_single_factor_step,
            "gate_relaxation_distance": self.gate_relaxation_distance,
            "changed_dimension_count": self.changed_dimension_count,
            "objective_score": _blank_if_none(self.objective_score),
            "eligible": self.eligible,
            "ineligibility_reason": self.ineligibility_reason,
            "target_weights_json": json.dumps(
                self.target_weights,
                ensure_ascii=False,
                sort_keys=True,
            ),
            "gate_cap_overrides_json": json.dumps(
                self.gate_cap_overrides,
                ensure_ascii=False,
                sort_keys=True,
            ),
        }


@dataclass(frozen=True)
class ShadowParameterCapAttribution:
    gate_id: str
    selected_cap_value: float
    cap_only_trial: ShadowParameterSearchTrial
    excess_delta_vs_baseline: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "gate_id": self.gate_id,
            "selected_cap_value": self.selected_cap_value,
            "cap_only_trial_id": self.cap_only_trial.trial_id,
            "cap_only_excess_total_return": self.cap_only_trial.excess_total_return,
            "excess_delta_vs_baseline": self.excess_delta_vs_baseline,
            "cap_only_shadow_max_drawdown": self.cap_only_trial.shadow_max_drawdown,
            "cap_only_shadow_turnover": self.cap_only_trial.shadow_turnover,
        }


@dataclass(frozen=True)
class ShadowParameterPositionChangeRow:
    as_of: date
    outcome_status: str
    outcome_end_date: date | None
    asset_return: float | None
    production_position: float
    candidate_position: float
    position_delta: float
    production_binding_gates: tuple[str, ...]
    candidate_binding_gates: tuple[str, ...]
    production_position_return: float | None
    candidate_position_return: float | None
    return_impact: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "outcome_status": self.outcome_status,
            "outcome_end_date": (
                "" if self.outcome_end_date is None else self.outcome_end_date.isoformat()
            ),
            "asset_return": _blank_if_none(self.asset_return),
            "production_position": self.production_position,
            "candidate_position": self.candidate_position,
            "position_delta": self.position_delta,
            "production_binding_gates": ",".join(self.production_binding_gates),
            "candidate_binding_gates": ",".join(self.candidate_binding_gates),
            "production_position_return": _blank_if_none(
                self.production_position_return
            ),
            "candidate_position_return": _blank_if_none(
                self.candidate_position_return
            ),
            "return_impact": _blank_if_none(self.return_impact),
        }


@dataclass(frozen=True)
class ShadowParameterFactorialAttribution:
    selected_trial_id: str
    selected_trial_eligible: bool
    baseline_trial: ShadowParameterSearchTrial
    weight_only_trial: ShadowParameterSearchTrial
    gate_only_trial: ShadowParameterSearchTrial
    combined_trial: ShadowParameterSearchTrial

    @property
    def baseline_excess_delta(self) -> float | None:
        return _trial_excess_delta(self.baseline_trial, self.baseline_trial)

    @property
    def weight_only_excess_delta(self) -> float | None:
        return _trial_excess_delta(self.weight_only_trial, self.baseline_trial)

    @property
    def gate_only_excess_delta(self) -> float | None:
        return _trial_excess_delta(self.gate_only_trial, self.baseline_trial)

    @property
    def combined_excess_delta(self) -> float | None:
        return _trial_excess_delta(self.combined_trial, self.baseline_trial)

    @property
    def interaction_excess_delta(self) -> float | None:
        values = (
            self.weight_only_excess_delta,
            self.gate_only_excess_delta,
            self.combined_excess_delta,
        )
        if any(value is None for value in values):
            return None
        weight_delta, gate_delta, combined_delta = values
        return combined_delta - weight_delta - gate_delta

    @property
    def primary_driver(self) -> str:
        weight_delta = abs(self.weight_only_excess_delta or 0.0)
        gate_delta = abs(self.gate_only_excess_delta or 0.0)
        interaction_delta = abs(self.interaction_excess_delta or 0.0)
        if gate_delta >= weight_delta and gate_delta >= interaction_delta:
            return "gate"
        if weight_delta >= gate_delta and weight_delta >= interaction_delta:
            return "weight"
        return "interaction"

    def to_dict(self) -> dict[str, Any]:
        return {
            "selected_trial_id": self.selected_trial_id,
            "selected_trial_eligible": self.selected_trial_eligible,
            "primary_driver": self.primary_driver,
            "baseline_trial_id": self.baseline_trial.trial_id,
            "weight_only_trial_id": self.weight_only_trial.trial_id,
            "gate_only_trial_id": self.gate_only_trial.trial_id,
            "combined_trial_id": self.combined_trial.trial_id,
            "weight_only_excess_delta": self.weight_only_excess_delta,
            "gate_only_excess_delta": self.gate_only_excess_delta,
            "combined_excess_delta": self.combined_excess_delta,
            "interaction_excess_delta": self.interaction_excess_delta,
        }


@dataclass(frozen=True)
class ShadowParameterSearchReport:
    run_id: str
    generated_at: datetime
    start: date
    end: date
    decision_snapshot_path: Path
    prices_path: Path
    source_weight_profile_path: Path
    search_space_path: Path
    objective_path: Path
    output_dir: Path
    strategy_ticker: str
    horizon_days: int
    cost_bps: float
    slippage_bps: float
    search_space: ShadowParameterSearchSpaceManifest
    objective: ShadowParameterObjectiveConfig
    search_space_checksum: str
    objective_checksum: str
    source_weight_profile_checksum: str
    prices_checksum: str
    decision_snapshot_checksum: str
    resolver_version: str
    git_commit_sha: str | None
    git_worktree_dirty: bool | None
    snapshot_count: int
    weight_candidate_count: int
    gate_candidate_count: int
    trials: tuple[ShadowParameterSearchTrial, ...]
    pareto_front: tuple[ShadowParameterSearchTrial, ...]
    factorial_attribution: ShadowParameterFactorialAttribution | None
    cap_attribution: tuple[ShadowParameterCapAttribution, ...]
    position_change_rows: tuple[ShadowParameterPositionChangeRow, ...]
    warnings: tuple[str, ...]

    @property
    def status(self) -> str:
        if self.warnings or self.best_trial is None:
            return "PASS_WITH_LIMITATIONS"
        return "PASS"

    @property
    def best_trial(self) -> ShadowParameterSearchTrial | None:
        return _best_search_trial(self.trials, eligible_only=True)

    @property
    def best_diagnostic_trial(self) -> ShadowParameterSearchTrial | None:
        return _best_search_trial(self.trials, eligible_only=False)


@dataclass(frozen=True)
class ShadowParameterPromotionCheck:
    check_id: str
    status: str
    reason: str
    evidence_ref: str

    def to_dict(self) -> dict[str, str]:
        return {
            "check_id": self.check_id,
            "status": self.status,
            "reason": self.reason,
            "evidence_ref": self.evidence_ref,
        }


@dataclass(frozen=True)
class ShadowParameterPromotionReport:
    generated_at: datetime
    search_output_dir: Path
    contract_path: Path
    contract: ShadowParameterPromotionContractConfig
    search_manifest: dict[str, Any]
    selected_trial_id: str | None
    selected_trial: dict[str, Any] | None
    checks: tuple[ShadowParameterPromotionCheck, ...]
    production_effect: str = "none"

    @property
    def status(self) -> str:
        if any(check.status == "FAIL" for check in self.checks):
            return "NOT_PROMOTABLE"
        if any(check.status == "MISSING" for check in self.checks):
            return "READY_FOR_FORWARD_SHADOW"
        return "READY_FOR_OWNER_REVIEW"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "shadow_parameter_promotion_contract",
            "status": self.status,
            "production_effect": self.production_effect,
            "generated_at": self.generated_at.isoformat(),
            "search_output_dir": str(self.search_output_dir),
            "contract_path": str(self.contract_path),
            "contract": self.contract.model_dump(mode="json"),
            "search_run_id": self.search_manifest.get("run_id"),
            "selected_trial_id": self.selected_trial_id,
            "checks": [check.to_dict() for check in self.checks],
        }


def default_shadow_weight_profile_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"shadow_weight_profiles_{as_of.isoformat()}.md"


def default_shadow_weight_performance_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"shadow_weight_performance_{as_of.isoformat()}.md"


def default_shadow_weight_performance_csv_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"shadow_weight_performance_{as_of.isoformat()}.csv"


def default_shadow_parameter_search_output_dir(output_root: Path, run_id: str) -> Path:
    return output_root / run_id


def default_shadow_parameter_promotion_report_path(output_dir: Path, run_id: str) -> Path:
    return output_dir / f"shadow_parameter_promotion_{run_id}.md"


def load_shadow_weight_profile_manifest(
    path: Path | str = DEFAULT_SHADOW_WEIGHT_PROFILE_MANIFEST_PATH,
    *,
    source_profile_path: Path | None = None,
) -> tuple[ShadowWeightProfileManifest, WeightProfile, Path]:
    manifest_path = Path(path)
    raw = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"shadow weight profile manifest must be a mapping: {path}")
    manifest = ShadowWeightProfileManifest.model_validate(raw)
    resolved_source_path = source_profile_path or _project_path(
        manifest.source_weight_profile_path
    )
    source_profile = load_weight_profile(resolved_source_path)
    for profile in manifest.profiles:
        _validate_profile_against_source(profile, source_profile)
    return manifest, source_profile, resolved_source_path


def load_shadow_position_gate_profile_manifest(
    path: Path | str = DEFAULT_SHADOW_POSITION_GATE_PROFILE_MANIFEST_PATH,
) -> ShadowPositionGateProfileManifest:
    manifest_path = Path(path)
    raw = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"shadow position gate profile manifest must be a mapping: {path}")
    return ShadowPositionGateProfileManifest.model_validate(raw)


def load_shadow_parameter_search_space(
    path: Path | str = DEFAULT_SHADOW_PARAMETER_SEARCH_SPACE_PATH,
) -> ShadowParameterSearchSpaceManifest:
    manifest_path = Path(path)
    raw = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"shadow parameter search space must be a mapping: {path}")
    return ShadowParameterSearchSpaceManifest.model_validate(raw)


def load_shadow_parameter_objective(
    path: Path | str = DEFAULT_SHADOW_PARAMETER_OBJECTIVE_PATH,
) -> ShadowParameterObjectiveConfig:
    objective_path = Path(path)
    raw = yaml.safe_load(objective_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"shadow parameter objective must be a mapping: {path}")
    return ShadowParameterObjectiveConfig.model_validate(raw)


def load_shadow_parameter_promotion_contract(
    path: Path | str = DEFAULT_SHADOW_PARAMETER_PROMOTION_CONTRACT_PATH,
) -> ShadowParameterPromotionContractConfig:
    contract_path = Path(path)
    raw = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"shadow parameter promotion contract must be a mapping: {path}")
    return ShadowParameterPromotionContractConfig.model_validate(raw)


def build_shadow_weight_profile_run_report(
    *,
    as_of: date,
    decision_snapshot_path: Path,
    manifest_path: Path = DEFAULT_SHADOW_WEIGHT_PROFILE_MANIFEST_PATH,
    gate_manifest_path: Path | None = None,
    scoring_rules_path: Path | None = None,
    observation_ledger_path: Path | None = None,
    prediction_ledger_path: Path | None = None,
    generated_at: datetime | None = None,
) -> ShadowWeightProfileRunReport:
    manifest, _source_profile, source_profile_path = load_shadow_weight_profile_manifest(
        manifest_path
    )
    snapshot = _read_json_object(decision_snapshot_path)
    generated = generated_at or datetime.now(tz=UTC)
    component_scores = _component_scores(snapshot)
    production_score = _float_required((snapshot.get("scores") or {}).get("overall_score"))
    production_model_band = _dict_value(
        (snapshot.get("positions") or {}).get("model_risk_asset_ai_band")
    )
    production_final_band = _dict_value(
        (snapshot.get("positions") or {}).get("final_risk_asset_ai_band")
    )
    if gate_manifest_path is None:
        gate_manifest = None
        gate_profiles = (_production_observed_gate_profile(),)
    else:
        gate_manifest = load_shadow_position_gate_profile_manifest(gate_manifest_path)
        gate_profiles = gate_manifest.profiles
    position_bands = _load_position_bands(scoring_rules_path)
    warnings: list[str] = []
    observations: list[ShadowWeightObservation] = []
    for profile in manifest.profiles:
        if profile.status != "shadow":
            warnings.append(
                f"profile {profile.profile_id} status={profile.status}，本轮只作观察。"
            )
        for gate_profile in gate_profiles:
            if gate_profile.status != "shadow":
                warnings.append(
                    f"gate profile {gate_profile.profile_id} "
                    f"status={gate_profile.status}，本轮只作观察。"
                )
            missing_gate_ids = _missing_gate_override_ids(
                snapshot,
                gate_profile.gate_cap_overrides,
            )
            if missing_gate_ids:
                warnings.append(
                    f"gate profile {gate_profile.profile_id} 覆盖了本次 snapshot "
                    f"未出现的 gate：{', '.join(missing_gate_ids)}。"
                )
            shadow_score = sum(
                component_scores[signal] * weight
                for signal, weight in profile.target_weights.items()
            )
            shadow_model_band = _band_for_score(shadow_score, position_bands)
            gate_cap_max_position, gate_cap_sources = _gate_cap(
                snapshot,
                gate_cap_overrides=gate_profile.gate_cap_overrides,
            )
            shadow_final_band = _apply_gate_cap(
                shadow_model_band,
                gate_cap_max_position,
            )
            observations.append(
                ShadowWeightObservation(
                    as_of=as_of,
                    generated_at=generated,
                    profile_id=_combined_shadow_profile_id(profile, gate_profile),
                    profile_version=_combined_shadow_profile_version(
                        profile,
                        gate_profile,
                    ),
                    profile_status=_combined_shadow_profile_status(
                        profile,
                        gate_profile,
                    ),
                    production_effect=profile.production_effect,
                    weight_profile_id=profile.profile_id,
                    weight_profile_version=profile.version,
                    gate_profile_id=gate_profile.profile_id,
                    gate_profile_version=gate_profile.version,
                    production_score=production_score,
                    shadow_score=shadow_score,
                    production_model_band=production_model_band,
                    shadow_model_band=shadow_model_band,
                    production_final_band=production_final_band,
                    shadow_final_band=shadow_final_band,
                    gate_cap_max_position=gate_cap_max_position,
                    gate_cap_sources=gate_cap_sources,
                    gate_cap_overrides=dict(gate_profile.gate_cap_overrides),
                    target_weights=dict(profile.target_weights),
                    source_snapshot_path=decision_snapshot_path,
                )
            )
    if not observations:
        warnings.append("shadow weight profile manifest 没有可观察 profile。")
    return ShadowWeightProfileRunReport(
        as_of=as_of,
        generated_at=generated,
        manifest_path=manifest_path,
        gate_manifest_path=gate_manifest_path,
        gate_manifest_version=None if gate_manifest is None else gate_manifest.version,
        source_weight_profile_path=source_profile_path,
        decision_snapshot_path=decision_snapshot_path,
        observation_ledger_path=observation_ledger_path,
        prediction_ledger_path=prediction_ledger_path,
        manifest=manifest,
        production_score=production_score,
        production_model_band=production_model_band,
        production_final_band=production_final_band,
        observations=tuple(observations),
        warnings=tuple(dict.fromkeys(warnings)),
    )


def write_shadow_weight_observation_ledger(
    report: ShadowWeightProfileRunReport,
    output_path: Path = DEFAULT_SHADOW_WEIGHT_PROFILE_OBSERVATION_LEDGER_PATH,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    new_frame = pd.DataFrame([observation.to_dict() for observation in report.observations])
    for column in OBSERVATION_COLUMNS:
        if column not in new_frame.columns:
            new_frame[column] = ""
    new_frame = new_frame.loc[:, list(OBSERVATION_COLUMNS)]
    if output_path.exists():
        existing = pd.read_csv(output_path, dtype=str, keep_default_na=False)
        missing = set(OBSERVATION_COLUMNS) - set(existing.columns)
        for column in sorted(missing):
            existing[column] = ""
        current_keys = set(zip(new_frame["as_of"], new_frame["profile_id"], strict=True))
        existing = existing.loc[
            [
                (as_of, profile_id) not in current_keys
                for as_of, profile_id in zip(
                    existing["as_of"],
                    existing["profile_id"],
                    strict=True,
                )
            ]
        ]
        frame = pd.concat(
            [existing.loc[:, list(OBSERVATION_COLUMNS)], new_frame],
            ignore_index=True,
        )
    else:
        frame = new_frame
    frame = frame.sort_values(["as_of", "profile_id"])
    frame.to_csv(output_path, index=False)
    return output_path


def render_shadow_weight_profile_report(report: ShadowWeightProfileRunReport) -> str:
    lines = [
        "# Shadow Weight Profiles 观察报告",
        "",
        f"- 状态：{report.status}",
        "- production_effect：none",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- Manifest：`{report.manifest_path}`",
        f"- Manifest version：{report.manifest.version}",
        f"- Gate manifest：`{report.gate_manifest_path}`",
        f"- Gate manifest version：{report.gate_manifest_version or 'production_observed'}",
        f"- Source weight profile：`{report.source_weight_profile_path}`",
        f"- Decision snapshot：`{report.decision_snapshot_path}`",
        f"- Observation ledger：`{report.observation_ledger_path}`",
        f"- Prediction ledger：`{report.prediction_ledger_path}`",
        f"- 主线评分：{report.production_score:.2f}",
        f"- 主线模型仓位：{_band_label(report.production_model_band)}",
        f"- 主线最终仓位：{_band_label(report.production_final_band)}",
        "",
        "## 治理边界",
        "",
        "- 本报告只读比较 shadow 权重参数，不修改生产 `weight_profile_current.yaml`、"
        "approved overlay、正式 `prediction_ledger.csv`、日报结论或仓位 gate。",
        "- 若传入 shadow gate manifest，本报告只在隔离 observation/prediction ledger "
        "中覆盖已观察 gate cap；不改生产 `scoring_rules.yaml` 或 `portfolio.yaml`。",
        "- shadow profile 的好坏只能进入长期 observation/outcome；是否替换生产权重需要"
        "另行定义 owner approval、promotion 和 rollback 条件。",
        "",
        "## Profile 对比",
        "",
        (
            "| Profile | Version | Weight profile | Gate profile | Shadow score | "
            "Δ vs production | Model band | Gated band | Gate cap | Gate overrides |"
        ),
        "|---|---|---|---|---:|---:|---|---|---:|---|",
    ]
    for observation in report.observations:
        lines.append(
            "| "
            f"`{observation.profile_id}` | "
            f"`{observation.profile_version}` | "
            f"`{observation.weight_profile_id}` | "
            f"`{observation.gate_profile_id}` | "
            f"{observation.shadow_score:.2f} | "
            f"{observation.score_delta_vs_production:+.2f} | "
            f"{_band_label(observation.shadow_model_band)} | "
            f"{_band_label(observation.shadow_final_band)} | "
            f"{observation.gate_cap_max_position:.0%} | "
            f"{_format_gate_overrides(observation.gate_cap_overrides)} |"
        )
    lines.extend(["", "## 警告", ""])
    if report.warnings:
        lines.extend(f"- {warning}" for warning in report.warnings)
    else:
        lines.append("- 无")
    return "\n".join(lines).rstrip() + "\n"


def write_shadow_weight_profile_report(
    report: ShadowWeightProfileRunReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_shadow_weight_profile_report(report), encoding="utf-8")
    return output_path


def build_shadow_weight_performance_report(
    *,
    as_of: date,
    since: date | None = None,
    observation_ledger_path: Path = DEFAULT_SHADOW_WEIGHT_PROFILE_OBSERVATION_LEDGER_PATH,
    prices_path: Path = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    strategy_ticker: str = "SMH",
    horizon_days: int = 1,
    cost_bps: float = 5.0,
    slippage_bps: float = 0.0,
) -> ShadowWeightPerformanceReport:
    if horizon_days <= 0:
        raise ValueError("horizon_days must be positive")
    if cost_bps < 0 or slippage_bps < 0:
        raise ValueError("cost_bps and slippage_bps must be non-negative")
    observations = _load_observation_rows(observation_ledger_path, as_of, since=since)
    price_series = _strategy_price_series(prices_path, strategy_ticker)
    cost_rate = (cost_bps + slippage_bps) / 10_000.0
    rows: list[ShadowWeightPerformanceRow] = []
    warnings: list[str] = []
    previous_positions: dict[str, tuple[float, float]] = {}
    for observation in observations:
        profile_id = str(observation["profile_id"])
        profile_version = str(observation["profile_version"])
        signal_date = date.fromisoformat(str(observation["as_of"]))
        production_position = _float_or_none(
            observation.get("production_gated_target_position")
        )
        shadow_position = _float_or_none(observation.get("shadow_gated_target_position"))
        if production_position is None or shadow_position is None:
            row = ShadowWeightPerformanceRow(
                as_of=signal_date,
                profile_id=profile_id,
                profile_version=profile_version,
                horizon_days=horizon_days,
                outcome_end_date=None,
                outcome_status="MISSING_DATA",
                outcome_reason="observation 缺少 production/shadow gate 后仓位",
                asset_return=None,
                production_gated_target_position=production_position,
                shadow_gated_target_position=shadow_position,
                production_turnover=None,
                shadow_turnover=None,
                production_position_return=None,
                shadow_position_return=None,
            )
            rows.append(row)
            continue
        outcome = _horizon_return(
            price_series,
            signal_date=signal_date,
            as_of=as_of,
            horizon_days=horizon_days,
        )
        if outcome["status"] != "AVAILABLE":
            rows.append(
                ShadowWeightPerformanceRow(
                    as_of=signal_date,
                    profile_id=profile_id,
                    profile_version=profile_version,
                    horizon_days=horizon_days,
                    outcome_end_date=outcome["end_date"],
                    outcome_status=str(outcome["status"]),
                    outcome_reason=str(outcome["reason"]),
                    asset_return=None,
                    production_gated_target_position=production_position,
                    shadow_gated_target_position=shadow_position,
                    production_turnover=None,
                    shadow_turnover=None,
                    production_position_return=None,
                    shadow_position_return=None,
                )
            )
            continue
        previous_production, previous_shadow = previous_positions.get(
            profile_id,
            (0.0, 0.0),
        )
        production_turnover = abs(production_position - previous_production)
        shadow_turnover = abs(shadow_position - previous_shadow)
        asset_return = _float_required(outcome["return"])
        rows.append(
            ShadowWeightPerformanceRow(
                as_of=signal_date,
                profile_id=profile_id,
                profile_version=profile_version,
                horizon_days=horizon_days,
                outcome_end_date=outcome["end_date"],
                outcome_status="AVAILABLE",
                outcome_reason="",
                asset_return=asset_return,
                production_gated_target_position=production_position,
                shadow_gated_target_position=shadow_position,
                production_turnover=production_turnover,
                shadow_turnover=shadow_turnover,
                production_position_return=(
                    production_position * asset_return - production_turnover * cost_rate
                ),
                shadow_position_return=(
                    shadow_position * asset_return - shadow_turnover * cost_rate
                ),
            )
        )
        previous_positions[profile_id] = (production_position, shadow_position)
    summaries = _performance_summaries(rows)
    if not rows:
        warnings.append("shadow weight observation ledger 没有可评估记录。")
    if not any(row.outcome_status == "AVAILABLE" for row in rows):
        warnings.append("当前没有 available shadow weight performance 样本。")
    return ShadowWeightPerformanceReport(
        as_of=as_of,
        since=since,
        observation_ledger_path=observation_ledger_path,
        prices_path=prices_path,
        strategy_ticker=strategy_ticker,
        horizon_days=horizon_days,
        cost_bps=cost_bps,
        slippage_bps=slippage_bps,
        rows=tuple(rows),
        summaries=summaries,
        warnings=tuple(dict.fromkeys(warnings)),
    )


def write_shadow_weight_performance_csv(
    report: ShadowWeightPerformanceReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame([row.to_dict() for row in report.rows])
    for column in PERFORMANCE_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    frame = frame.loc[:, list(PERFORMANCE_COLUMNS)]
    frame.to_csv(output_path, index=False)
    return output_path


def render_shadow_weight_performance_report(
    report: ShadowWeightPerformanceReport,
    *,
    csv_path: Path | None = None,
) -> str:
    best = report.best_positive_profile
    lines = [
        "# Shadow Weight Performance 评估报告",
        "",
        f"- 状态：{report.status}",
        "- production_effect：none",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 起始日期：{report.since.isoformat() if report.since else '全部'}",
        f"- Strategy ticker：{report.strategy_ticker}",
        f"- Horizon：{report.horizon_days}D",
        f"- 单边成本：{report.cost_bps:.1f} bps",
        f"- 线性滑点：{report.slippage_bps:.1f} bps",
        f"- Observation ledger：`{report.observation_ledger_path}`",
        f"- Prices：`{report.prices_path}`",
        f"- 机器可读 performance：`{csv_path}`",
        "",
        "## 治理边界",
        "",
        "- 本报告只把 shadow 权重和 shadow gate profile 的观察仓位转成验证期收益、"
        "回撤、换手和成本对比；不修改生产 `weight_profile_current.yaml`、"
        "approved overlay、正式 prediction ledger、日报结论或仓位 gate。",
        "- 当前结论只能作为 validation-only 调参方向；production 替换仍需要 promotion floor、"
        "forward shadow、owner approval 和 rollback 条件。",
        "",
        "## 最优候选",
        "",
    ]
    if best is None:
        if report.best_profile is None:
            lines.append("- 当前没有可比较的 available 样本。")
        else:
            lines.append(
                "- 当前没有产生正向 position-weighted excess return 的 shadow profile。"
            )
            lines.append(
                "- 最高 excess total return："
                f"{_format_pct(report.best_profile.excess_total_return)}"
            )
    else:
        lines.extend(
            [
                f"- Return-leading profile：`{best.profile_id}` / `{best.profile_version}`",
                f"- Shadow total return：{_format_pct(best.shadow_total_return)}",
                f"- Production total return：{_format_pct(best.production_total_return)}",
                f"- Excess total return：{_format_pct(best.excess_total_return)}",
                f"- Shadow max drawdown：{_format_pct(best.shadow_max_drawdown)}",
                f"- Production max drawdown：{_format_pct(best.production_max_drawdown)}",
                f"- Shadow turnover：{best.shadow_turnover:.2f}",
                f"- Production turnover：{best.production_turnover:.2f}",
                (
                    "- Shadow beats production rate："
                    f"{_format_pct(best.shadow_beats_production_rate)}"
                ),
            ]
        )
    lines.extend(
        [
            "",
            "## Profile 对比",
            "",
            (
                "| Profile | Version | Available | Pending | Missing | Production return | "
                "Shadow return | Excess | Production MDD | Shadow MDD | "
                "Production turnover | Shadow turnover | Beat rate |"
            ),
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for summary in report.summaries:
        lines.append(
            "| "
            f"`{summary.profile_id}` | "
            f"`{summary.profile_version}` | "
            f"{summary.available_count} | "
            f"{summary.pending_count} | "
            f"{summary.missing_count} | "
            f"{_format_pct(summary.production_total_return)} | "
            f"{_format_pct(summary.shadow_total_return)} | "
            f"{_format_pct(summary.excess_total_return)} | "
            f"{_format_pct(summary.production_max_drawdown)} | "
            f"{_format_pct(summary.shadow_max_drawdown)} | "
            f"{summary.production_turnover:.2f} | "
            f"{summary.shadow_turnover:.2f} | "
            f"{_format_pct(summary.shadow_beats_production_rate)} |"
        )
    lines.extend(["", "## 警告", ""])
    if report.warnings:
        lines.extend(f"- {warning}" for warning in report.warnings)
    else:
        lines.append("- 无")
    return "\n".join(lines).rstrip() + "\n"


def write_shadow_weight_performance_report(
    report: ShadowWeightPerformanceReport,
    output_path: Path,
    *,
    csv_path: Path | None = None,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_shadow_weight_performance_report(report, csv_path=csv_path),
        encoding="utf-8",
    )
    return output_path


def build_shadow_parameter_search_report(
    *,
    run_id: str,
    start: date,
    end: date,
    decision_snapshot_path: Path = DEFAULT_DECISION_SNAPSHOT_SEARCH_DIR,
    prices_path: Path = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    search_space_path: Path = DEFAULT_SHADOW_PARAMETER_SEARCH_SPACE_PATH,
    objective_path: Path = DEFAULT_SHADOW_PARAMETER_OBJECTIVE_PATH,
    output_dir: Path | None = None,
    strategy_ticker: str = "SMH",
    horizon_days: int = 1,
    cost_bps: float = 5.0,
    slippage_bps: float = 0.0,
    max_trials: int | None = None,
    generated_at: datetime | None = None,
) -> ShadowParameterSearchReport:
    if start > end:
        raise ValueError("start must be <= end")
    if horizon_days <= 0:
        raise ValueError("horizon_days must be positive")
    if cost_bps < 0 or slippage_bps < 0:
        raise ValueError("cost_bps and slippage_bps must be non-negative")
    if max_trials is not None and max_trials <= 0:
        raise ValueError("max_trials must be positive when provided")

    search_space = load_shadow_parameter_search_space(search_space_path)
    objective = load_shadow_parameter_objective(objective_path)
    source_profile_path = _project_path(search_space.source_weight_profile_path)
    source_profile = load_weight_profile(source_profile_path)
    snapshots = _load_search_snapshots(decision_snapshot_path, start, end)
    if not snapshots:
        raise ValueError(
            "no decision snapshots found for search window: "
            f"{start.isoformat()}..{end.isoformat()}"
        )
    position_bands = _load_position_bands(None)
    price_series = _strategy_price_series(prices_path, strategy_ticker)
    cost_rate = (cost_bps + slippage_bps) / 10_000.0
    outcomes_by_signal_date = {
        signal_date: _horizon_return(
            price_series,
            signal_date=signal_date,
            as_of=end,
            horizon_days=horizon_days,
        )
        for signal_date, _path, _snapshot in snapshots
    }
    weight_candidates = _build_search_weight_candidates(
        search_space,
        source_profile,
    )
    gate_candidates = _build_search_gate_candidates(search_space)
    trials: list[ShadowParameterSearchTrial] = []
    warnings: list[str] = []
    for index, (weight_candidate, gate_candidate) in enumerate(
        itertools.product(weight_candidates, gate_candidates),
        start=1,
    ):
        if max_trials is not None and index > max_trials:
            warnings.append(
                f"max_trials={max_trials} 已截断搜索空间；未评估剩余组合。"
            )
            break
        trials.append(
            _evaluate_search_trial(
                snapshots=snapshots,
                weight_candidate=weight_candidate,
                gate_candidate=gate_candidate,
                position_bands=position_bands,
                outcomes_by_signal_date=outcomes_by_signal_date,
                cost_rate=cost_rate,
                objective=objective,
                production_weights=source_profile.base_weights,
            )
        )
    if not trials:
        warnings.append("搜索空间没有生成任何 trial。")
    if not any(trial.eligible for trial in trials):
        warnings.append("当前没有符合 objective 约束的 eligible trial。")
    factorial_attribution, attribution_warning = _build_factorial_attribution(trials)
    if attribution_warning:
        warnings.append(attribution_warning)
    cap_attribution = _build_cap_level_attribution(
        trials=trials,
        snapshots=snapshots,
        source_profile=source_profile,
        position_bands=position_bands,
        outcomes_by_signal_date=outcomes_by_signal_date,
        cost_rate=cost_rate,
        objective=objective,
    )
    position_change_rows = _build_position_change_rows(
        trials=trials,
        snapshots=snapshots,
        position_bands=position_bands,
        outcomes_by_signal_date=outcomes_by_signal_date,
        cost_rate=cost_rate,
    )
    return ShadowParameterSearchReport(
        run_id=run_id,
        generated_at=generated_at or datetime.now(tz=UTC),
        start=start,
        end=end,
        decision_snapshot_path=decision_snapshot_path,
        prices_path=prices_path,
        source_weight_profile_path=source_profile_path,
        search_space_path=search_space_path,
        objective_path=objective_path,
        output_dir=output_dir or default_shadow_parameter_search_output_dir(
            DEFAULT_SHADOW_PARAMETER_SEARCH_OUTPUT_ROOT,
            run_id,
        ),
        strategy_ticker=strategy_ticker,
        horizon_days=horizon_days,
        cost_bps=cost_bps,
        slippage_bps=slippage_bps,
        search_space=search_space,
        objective=objective,
        search_space_checksum=_sha256_file(search_space_path),
        objective_checksum=_sha256_file(objective_path),
        source_weight_profile_checksum=_sha256_file(source_profile_path),
        prices_checksum=_sha256_file(prices_path),
        decision_snapshot_checksum=_sha256_search_snapshots(snapshots),
        resolver_version=SHADOW_PARAMETER_SEARCH_RESOLVER_VERSION,
        git_commit_sha=_git_commit_sha(),
        git_worktree_dirty=_git_worktree_dirty(),
        snapshot_count=len(snapshots),
        weight_candidate_count=len(weight_candidates),
        gate_candidate_count=len(gate_candidates),
        trials=tuple(trials),
        pareto_front=_pareto_front(trials),
        factorial_attribution=factorial_attribution,
        cap_attribution=cap_attribution,
        position_change_rows=position_change_rows,
        warnings=tuple(dict.fromkeys(warnings)),
    )


def write_shadow_parameter_search_bundle(
    report: ShadowParameterSearchReport,
) -> dict[str, Path]:
    output_dir = report.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    trials_path = output_dir / "trials.csv"
    pareto_path = output_dir / "pareto_front.csv"
    best_path = output_dir / "best_profiles.yaml"
    manifest_path = output_dir / "manifest.json"
    report_path = output_dir / "search_report.md"

    pd.DataFrame([trial.to_dict() for trial in report.trials]).to_csv(
        trials_path,
        index=False,
    )
    pd.DataFrame([trial.to_dict() for trial in report.pareto_front]).to_csv(
        pareto_path,
        index=False,
    )
    best_path.write_text(
        yaml.safe_dump(
            _best_profile_payload(report),
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    manifest_path.write_text(
        json.dumps(_search_manifest_payload(report), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    report_path.write_text(render_shadow_parameter_search_report(report), encoding="utf-8")
    return {
        "output_dir": output_dir,
        "trials_csv": trials_path,
        "pareto_front_csv": pareto_path,
        "best_profiles_yaml": best_path,
        "manifest_json": manifest_path,
        "search_report": report_path,
    }


def build_shadow_parameter_promotion_report(
    *,
    search_output_dir: Path,
    contract_path: Path = DEFAULT_SHADOW_PARAMETER_PROMOTION_CONTRACT_PATH,
    generated_at: datetime | None = None,
) -> ShadowParameterPromotionReport:
    contract = load_shadow_parameter_promotion_contract(contract_path)
    manifest_path = search_output_dir / "manifest.json"
    trials_path = search_output_dir / "trials.csv"
    manifest = _read_json_object(manifest_path)
    if not trials_path.exists():
        raise FileNotFoundError(f"shadow parameter trials CSV not found: {trials_path}")
    trials = pd.read_csv(trials_path)
    selected_trial_id = manifest.get("best_trial_id")
    diagnostic_trial_id = manifest.get("best_diagnostic_trial_id")
    if selected_trial_id is None:
        selected_trial_id = diagnostic_trial_id
    selected_trial = _trial_row_by_id(trials, selected_trial_id)
    checks = _build_shadow_parameter_promotion_checks(
        contract=contract,
        manifest=manifest,
        selected_trial=selected_trial,
        selected_trial_id=selected_trial_id,
        has_eligible_best=manifest.get("best_trial_id") is not None,
    )
    return ShadowParameterPromotionReport(
        generated_at=generated_at or datetime.now(tz=UTC),
        search_output_dir=search_output_dir,
        contract_path=contract_path,
        contract=contract,
        search_manifest=manifest,
        selected_trial_id=selected_trial_id,
        selected_trial=selected_trial,
        checks=tuple(checks),
    )


def write_shadow_parameter_promotion_report(
    report: ShadowParameterPromotionReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_shadow_parameter_promotion_report(report),
        encoding="utf-8",
    )
    summary_path = output_path.with_suffix(".json")
    summary_path.write_text(
        json.dumps(report.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path


def render_shadow_parameter_promotion_report(
    report: ShadowParameterPromotionReport,
) -> str:
    lines = [
        "# Shadow Parameter Promotion Contract 报告",
        "",
        f"- 状态：{report.status}",
        "- production_effect：none",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- Search output：`{report.search_output_dir}`",
        f"- Search run id：`{report.search_manifest.get('run_id', 'unknown')}`",
        f"- Contract：`{report.contract_path}`",
        f"- Contract version：`{report.contract.version}`",
        f"- Selected trial：`{report.selected_trial_id or 'none'}`",
        "",
        "## Contract Checks",
        "",
        "| Check | 状态 | Evidence | 说明 |",
        "|---|---|---|---|",
    ]
    for check in report.checks:
        lines.append(
            "| "
            f"`{check.check_id}` | "
            f"`{check.status}` | "
            f"{_escape_markdown_table(check.evidence_ref)} | "
            f"{_escape_markdown_table(check.reason)} |"
        )
    lines.extend(
        [
            "",
            "## 治理边界",
            "",
            "- 本报告只把 search output 放入独立 promotion contract 检查；"
            "不修改 production weight、approved overlay、正式 prediction ledger、"
            "日报结论或仓位 gate。",
            "- `READY_FOR_FORWARD_SHADOW` 只表示可以继续收集前向 shadow 证据，"
            "不表示 owner approval 或 production effect。",
            "- `READY_FOR_OWNER_REVIEW` 仍需要 owner approval 和 rollback condition；"
            "`approved_hard` 在执行链路未打通前保持不可用。",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def render_shadow_parameter_search_report(
    report: ShadowParameterSearchReport,
) -> str:
    best = report.best_trial
    top_trials = sorted(
        (
            trial
            for trial in report.trials
            if trial.eligible and trial.objective_score is not None
        ),
        key=lambda trial: trial.objective_score or float("-inf"),
        reverse=True,
    )[: report.objective.top_n]
    lines = [
        "# Shadow Parameter Search 报告",
        "",
        f"- 状态：{report.status}",
        "- production_effect：none",
        f"- Run ID：`{report.run_id}`",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- 搜索区间：{report.start.isoformat()} 至 {report.end.isoformat()}",
        f"- Decision snapshots：`{report.decision_snapshot_path}`",
        f"- Prices：`{report.prices_path}`",
        f"- Strategy ticker：{report.strategy_ticker}",
        f"- Horizon：{report.horizon_days}D",
        f"- 单边成本：{report.cost_bps:.1f} bps",
        f"- 线性滑点：{report.slippage_bps:.1f} bps",
        f"- Search space：`{report.search_space_path}`",
        f"- Search space checksum：`{report.search_space_checksum}`",
        f"- Objective：`{report.objective_path}`",
        f"- Objective checksum：`{report.objective_checksum}`",
        f"- Source weight profile：`{report.source_weight_profile_path}`",
        f"- Source weight checksum：`{report.source_weight_profile_checksum}`",
        f"- Prices checksum：`{report.prices_checksum}`",
        f"- Decision snapshot checksum：`{report.decision_snapshot_checksum}`",
        f"- Resolver version：`{report.resolver_version}`",
        f"- Git commit：`{report.git_commit_sha or 'unknown'}`",
        f"- Git worktree dirty：`{report.git_worktree_dirty}`",
        (
            "- 搜索方式："
            f"weight_grid={'on' if report.search_space.weight_grid.enabled else 'off'}, "
            f"gate_grid={'on' if report.search_space.gate_grid.enabled else 'off'}, "
            "exhaustive_grid_with_optional_manifest_seeds"
        ),
        f"- Snapshot count：{report.snapshot_count}",
        f"- Weight candidates：{report.weight_candidate_count}",
        f"- Gate candidates：{report.gate_candidate_count}",
        f"- Trial count：{len(report.trials)}",
        f"- Pareto front count：{len(report.pareto_front)}",
        f"- Output dir：`{report.output_dir}`",
        "",
        "## 治理边界",
        "",
        "- 本搜索只寻找当前输入区间、当前搜索空间和当前目标函数下的 "
        "in-sample validation-only 最优候选。",
        "- 搜索结果不得直接写入生产 `weight_profile_current.yaml`、"
        "`scoring_rules.yaml`、`portfolio.yaml`、approved overlay、正式 "
        "prediction ledger、日报结论或仓位 gate。",
        "- 生产替换仍需要 walk-forward / forward shadow、promotion floor、"
        "owner approval 和 rollback 条件。",
        "",
        render_shadow_parameter_trial_card(report).rstrip(),
        "",
        "## 最优候选",
        "",
    ]
    if best is None:
        lines.append("- 当前没有 eligible trial。")
        diagnostic = report.best_diagnostic_trial
        if diagnostic is not None:
            lines.extend(
                [
                    (
                        "- 诊断领先 trial："
                        f"`{diagnostic.trial_id}`（未达 objective 准入："
                        f"{diagnostic.ineligibility_reason or 'not_eligible'}）"
                    ),
                    f"- 诊断 shadow return：{_format_pct(diagnostic.shadow_total_return)}",
                    f"- 诊断 production return：{_format_pct(diagnostic.production_total_return)}",
                    f"- 诊断 excess：{_format_pct(diagnostic.excess_total_return)}",
                    f"- 诊断 shadow MDD：{_format_pct(diagnostic.shadow_max_drawdown)}",
                ]
            )
    else:
        lines.extend(
            [
                f"- Trial：`{best.trial_id}`",
                f"- Weight candidate：`{best.weight_candidate_id}`",
                f"- Gate candidate：`{best.gate_candidate_id}`",
                f"- Objective score：{_format_score(best.objective_score)}",
                f"- Shadow total return：{_format_pct(best.shadow_total_return)}",
                f"- Production total return：{_format_pct(best.production_total_return)}",
                f"- Excess total return：{_format_pct(best.excess_total_return)}",
                f"- Shadow max drawdown：{_format_pct(best.shadow_max_drawdown)}",
                f"- Production max drawdown：{_format_pct(best.production_max_drawdown)}",
                f"- Shadow turnover：{best.shadow_turnover:.2f}",
                f"- Production turnover：{best.production_turnover:.2f}",
                (
                    "- Shadow beats production rate："
                    f"{_format_pct(best.shadow_beats_production_rate)}"
                ),
                (
                    "- Target weights："
                    f"{_format_weight_mapping(best.target_weights)}"
                ),
                (
                    "- Gate overrides："
                    f"{_format_gate_overrides(best.gate_cap_overrides)}"
                ),
            ]
        )
    lines.extend(["", "## Factorial Attribution", ""])
    if report.factorial_attribution is None:
        lines.append("- 未生成 factorial attribution。")
    else:
        attribution = report.factorial_attribution
        selected_status = (
            "eligible"
            if attribution.selected_trial_eligible
            else "diagnostic_only_not_eligible"
        )
        lines.extend(
            [
                f"- Selected trial：`{attribution.selected_trial_id}`",
                f"- Selected status：`{selected_status}`",
                f"- Primary driver：`{attribution.primary_driver}`",
                (
                    "- 解读：`weight_only` 只替换权重、保留 production observed gate；"
                    "`gate_only` 保留 production 权重、只替换 gate；"
                    "`combined` 为 selected trial。"
                ),
                "",
                (
                    "| Cell | Trial | Weight | Gate | Shadow return | Excess | "
                    "Δ excess vs baseline | Shadow MDD | Turnover |"
                ),
                "|---|---|---|---|---:|---:|---:|---:|---:|",
                _factorial_row(
                    "baseline",
                    attribution.baseline_trial,
                    attribution.baseline_excess_delta,
                ),
                _factorial_row(
                    "weight_only",
                    attribution.weight_only_trial,
                    attribution.weight_only_excess_delta,
                ),
                _factorial_row(
                    "gate_only",
                    attribution.gate_only_trial,
                    attribution.gate_only_excess_delta,
                ),
                _factorial_row(
                    "combined",
                    attribution.combined_trial,
                    attribution.combined_excess_delta,
                ),
                (
                    "| interaction | "
                    f"`{attribution.combined_trial.trial_id}` | "
                    "`combined - weight_only - gate_only` | "
                    "attribution residual | "
                    "NA | NA | "
                    f"{_format_pct(attribution.interaction_excess_delta)} | "
                    "NA | NA |"
                ),
            ]
        )
    lines.extend(["", "## Cap-Level Attribution", ""])
    if not report.cap_attribution:
        lines.append("- 未生成 cap-level attribution。")
    else:
        primary_cap = max(
            report.cap_attribution,
            key=lambda item: abs(item.excess_delta_vs_baseline or 0.0),
        )
        lines.extend(
            [
                f"- Primary gate cap：`{primary_cap.gate_id}`",
                "- 解读：每行保留 production 权重，只替换 selected trial 中对应单个 gate cap，"
                "用于判断 gate 主导收益来自哪个 cap。",
                "",
                (
                    "| Gate cap | Selected value | Cap-only trial | "
                    "Excess | Δ excess vs baseline | Shadow MDD | Turnover |"
                ),
                "|---|---:|---|---:|---:|---:|---:|",
            ]
        )
        for item in report.cap_attribution:
            trial = item.cap_only_trial
            lines.append(
                "| "
                f"`{item.gate_id}` | "
                f"{item.selected_cap_value:.2f} | "
                f"`{trial.trial_id}` | "
                f"{_format_pct(trial.excess_total_return)} | "
                f"{_format_pct(item.excess_delta_vs_baseline)} | "
                f"{_format_pct(trial.shadow_max_drawdown)} | "
                f"{trial.shadow_turnover:.2f} |"
            )
    lines.extend(["", "## Position Change Attribution", ""])
    if not report.position_change_rows:
        lines.append("- 未生成最终仓位变化解释。")
    else:
        lines.extend(
            [
                (
                    "| Date | Status | Production position | Candidate position | "
                    "Delta | Production binding gate | Candidate binding gate | "
                    "Asset return | Return impact |"
                ),
                "|---|---|---:|---:|---:|---|---|---:|---:|",
            ]
        )
        for row in report.position_change_rows[:50]:
            lines.append(
                "| "
                f"{row.as_of.isoformat()} | "
                f"`{row.outcome_status}` | "
                f"{row.production_position:.2%} | "
                f"{row.candidate_position:.2%} | "
                f"{row.position_delta:.2%} | "
                f"{_escape_markdown_table(', '.join(row.production_binding_gates) or 'none')} | "
                f"{_escape_markdown_table(', '.join(row.candidate_binding_gates) or 'none')} | "
                f"{_format_pct(row.asset_return)} | "
                f"{_format_pct(row.return_impact)} |"
            )
        if len(report.position_change_rows) > 50:
            lines.append(
                f"- 仅展示前 50 行；完整行数：{len(report.position_change_rows)}。"
            )
    lines.extend(
        [
            "",
            "## Top Trials",
            "",
            (
                "| Rank | Trial | Weight | Gate | Objective | Available | Pending | "
                "Missing | Shadow return | Production return | Excess | Shadow MDD | "
                "Turnover | Beat rate |"
            ),
            "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for rank, trial in enumerate(top_trials, start=1):
        lines.append(
            "| "
            f"{rank} | "
            f"`{trial.trial_id}` | "
            f"`{trial.weight_candidate_id}` | "
            f"`{trial.gate_candidate_id}` | "
            f"{_format_score(trial.objective_score)} | "
            f"{trial.available_count} | "
            f"{trial.pending_count} | "
            f"{trial.missing_count} | "
            f"{_format_pct(trial.shadow_total_return)} | "
            f"{_format_pct(trial.production_total_return)} | "
            f"{_format_pct(trial.excess_total_return)} | "
            f"{_format_pct(trial.shadow_max_drawdown)} | "
            f"{trial.shadow_turnover:.2f} | "
            f"{_format_pct(trial.shadow_beats_production_rate)} |"
        )
    lines.extend(["", "## 警告", ""])
    if report.warnings:
        lines.extend(f"- {warning}" for warning in report.warnings)
    else:
        lines.append("- 无")
    return "\n".join(lines).rstrip() + "\n"


def render_shadow_parameter_trial_card(report: ShadowParameterSearchReport) -> str:
    trial = report.best_trial or report.best_diagnostic_trial
    if trial is None:
        return "\n".join(
            [
                "## Trial Card",
                "",
                "| 项目 | 内容 |",
                "|---|---|",
                "| Trial ID | none |",
                "| 状态 | no_trial |",
                "| 是否影响 production | 否，`production_effect=none` |",
                "| 说明 | 没有可排序 trial；先检查输入样本、搜索空间和 objective。 |",
            ]
        )

    status = "eligible-best" if report.best_trial is not None else "diagnostic-leading"
    if not trial.eligible:
        status = f"{status} / not eligible: {trial.ineligibility_reason or 'not_eligible'}"
    attribution = _trial_card_attribution_summary(report)
    position_change = _trial_card_position_change_summary(report)
    lines = [
        "## Trial Card",
        "",
        "| 项目 | 内容 |",
        "|---|---|",
        f"| Trial ID | `{trial.trial_id}` |",
        f"| 状态 | `{_escape_markdown_table(status)}` |",
        (
            "| 是否影响 production | 否，`production_effect=none`；不能修改生产权重、"
            "gate、approved overlay、正式 ledger 或日报结论 |"
        ),
        f"| Weight candidate | `{trial.weight_candidate_id}` |",
        f"| Gate candidate | `{trial.gate_candidate_id}` |",
        f"| Objective | {_format_score(trial.objective_score)} |",
        (
            f"| 样本 | available={trial.available_count}；"
            f"pending={trial.pending_count}；missing={trial.missing_count} |"
        ),
        (
            f"| 收益 | shadow={_format_pct(trial.shadow_total_return)}；"
            f"production={_format_pct(trial.production_total_return)}；"
            f"excess={_format_pct(trial.excess_total_return)} |"
        ),
        (
            f"| 风险/交易 | shadow MDD={_format_pct(trial.shadow_max_drawdown)}；"
            f"turnover={trial.shadow_turnover:.2f}；"
            f"beat rate={_format_pct(trial.shadow_beats_production_rate)} |"
        ),
        (
            "| 改了什么：weights | "
            f"{_escape_markdown_table(_format_weight_mapping(trial.target_weights))} |"
        ),
        (
            "| 改了什么：gate caps | "
            f"{_escape_markdown_table(_format_gate_overrides(trial.gate_cap_overrides))} |"
        ),
        f"| 收益归因 | {_escape_markdown_table(attribution)} |",
        f"| 仓位变化 | {_escape_markdown_table(position_change)} |",
        "",
        "### Trial Card 边界",
        "",
        "- 不能说明生产权重应该立即修改。",
        "- 不能说明 relaxed gate 已通过治理。",
        "- 不能替代 forward shadow、promotion contract、owner approval 和 rollback condition。",
    ]
    return "\n".join(lines)


def _trial_card_attribution_summary(report: ShadowParameterSearchReport) -> str:
    parts: list[str] = []
    if report.factorial_attribution is not None:
        attribution = report.factorial_attribution
        parts.append(f"primary_driver={attribution.primary_driver}")
        parts.append(
            "weight_only Δexcess="
            f"{_format_pct(attribution.weight_only_excess_delta)}"
        )
        parts.append(
            "gate_only Δexcess="
            f"{_format_pct(attribution.gate_only_excess_delta)}"
        )
        parts.append(
            "interaction Δexcess="
            f"{_format_pct(attribution.interaction_excess_delta)}"
        )
    if report.cap_attribution:
        primary_cap = max(
            report.cap_attribution,
            key=lambda item: abs(item.excess_delta_vs_baseline or 0.0),
        )
        parts.append(
            f"primary_gate_cap={primary_cap.gate_id} "
            f"({_format_pct(primary_cap.excess_delta_vs_baseline)})"
        )
    return "；".join(parts) if parts else "未生成 attribution。"


def _trial_card_position_change_summary(report: ShadowParameterSearchReport) -> str:
    changed = [
        row for row in report.position_change_rows if abs(row.position_delta) > 1e-9
    ]
    if not changed:
        return "未改变最终仓位；查看 Position Change Attribution。"
    available = [row for row in changed if row.return_impact is not None]
    if not available:
        return f"{len(changed)} 个 signal date 改变最终仓位；return impact 尚不可用。"
    largest = max(available, key=lambda row: abs(row.return_impact or 0.0))
    return (
        f"{len(changed)} 个 signal date 改变最终仓位；"
        f"最大 return impact 日期 {largest.as_of.isoformat()}="
        f"{_format_pct(largest.return_impact)}；完整明细见 Position Change Attribution。"
    )


def build_shadow_weight_prediction_records(
    report: ShadowWeightProfileRunReport,
    *,
    snapshot: dict[str, Any],
    trace_bundle: dict[str, Any],
    trace_bundle_path: Path,
    features_path: Path,
    data_quality_report_path: Path,
) -> tuple[dict[str, Any], ...]:
    from ai_trading_system.prediction_ledger import (
        build_prediction_record_from_decision_snapshot,
    )

    records: list[dict[str, Any]] = []
    for observation in report.observations:
        candidate_id = (
            f"shadow_weight_profile:{observation.profile_id}:"
            f"{observation.profile_version}"
        )
        record = build_prediction_record_from_decision_snapshot(
            snapshot=snapshot,
            trace_bundle=trace_bundle,
            trace_bundle_path=trace_bundle_path,
            features_path=features_path,
            data_quality_report_path=data_quality_report_path,
            candidate_id=candidate_id,
            production_effect="none",
            label_horizon_days=report.manifest.label_horizon_days,
        )
        record["score"] = observation.shadow_score
        record["signal"] = _band_label(observation.shadow_final_band)
        record["model_target_position"] = observation.shadow_model_target_position
        record["gated_target_position"] = observation.shadow_gated_target_position
        record["execution_assumption"] = (
            "shadow_weight_profile_no_order_no_position_change"
        )
        records.append(record)
    return tuple(records)


def _build_search_weight_candidates(
    search_space: ShadowParameterSearchSpaceManifest,
    source_profile: WeightProfile,
) -> tuple[ShadowWeightCandidate, ...]:
    candidates: list[ShadowWeightCandidate] = []
    seen: set[tuple[tuple[str, float], ...]] = set()

    def add_candidate(candidate: ShadowWeightCandidate) -> None:
        _validate_weight_mapping(candidate.target_weights)
        _validate_weight_candidate_against_source(candidate, source_profile)
        key = _weight_key(candidate.target_weights)
        if key in seen:
            return
        seen.add(key)
        candidates.append(candidate)

    if search_space.include_source_weight_profile:
        add_candidate(
            ShadowWeightCandidate(
                candidate_id="source_current",
                version=source_profile.version,
                source="source_weight_profile",
                target_weights=dict(source_profile.base_weights),
            )
        )
    if (
        search_space.include_shadow_weight_profiles
        and search_space.shadow_weight_profile_manifest_path
    ):
        manifest, _source_profile, _source_path = load_shadow_weight_profile_manifest(
            _project_path(search_space.shadow_weight_profile_manifest_path),
            source_profile_path=_project_path(search_space.source_weight_profile_path),
        )
        for profile in manifest.profiles:
            add_candidate(
                ShadowWeightCandidate(
                    candidate_id=profile.profile_id,
                    version=profile.version,
                    source="shadow_weight_profile_manifest",
                    target_weights=dict(profile.target_weights),
                )
            )
    if search_space.weight_grid.enabled:
        for index, weights in enumerate(
            _iter_weight_grid(search_space.weight_grid, source_profile),
            start=1,
        ):
            add_candidate(
                ShadowWeightCandidate(
                    candidate_id=f"grid_weight_{index:04d}",
                    version=search_space.version,
                    source="weight_grid",
                    target_weights=weights,
                )
            )
            if index >= search_space.weight_grid.max_candidates:
                break
    if not candidates:
        raise ValueError("shadow parameter search generated no weight candidates")
    return tuple(candidates)


def _build_search_gate_candidates(
    search_space: ShadowParameterSearchSpaceManifest,
) -> tuple[ShadowGateCandidate, ...]:
    candidates: list[ShadowGateCandidate] = []
    seen: set[tuple[tuple[str, float], ...]] = set()

    def add_candidate(candidate: ShadowGateCandidate) -> None:
        _validate_gate_candidate(candidate)
        key = _gate_key(candidate.gate_cap_overrides)
        if key in seen:
            return
        seen.add(key)
        candidates.append(candidate)

    if search_space.include_production_observed_gate_profile:
        add_candidate(
            ShadowGateCandidate(
                candidate_id=PRODUCTION_OBSERVED_GATE_PROFILE_ID,
                version="production_observed",
                source="production_decision_snapshot",
                gate_cap_overrides={},
            )
        )
    if (
        search_space.include_shadow_gate_profiles
        and search_space.shadow_gate_profile_manifest_path
    ):
        manifest = load_shadow_position_gate_profile_manifest(
            _project_path(search_space.shadow_gate_profile_manifest_path)
        )
        for profile in manifest.profiles:
            add_candidate(
                ShadowGateCandidate(
                    candidate_id=profile.profile_id,
                    version=profile.version,
                    source="shadow_gate_profile_manifest",
                    gate_cap_overrides=dict(profile.gate_cap_overrides),
                )
            )
    if search_space.gate_grid.enabled:
        for index, gate_overrides in enumerate(
            _iter_gate_grid(search_space.gate_grid),
            start=1,
        ):
            add_candidate(
                ShadowGateCandidate(
                    candidate_id=f"grid_gate_{index:04d}",
                    version=search_space.version,
                    source="gate_grid",
                    gate_cap_overrides=gate_overrides,
                )
            )
            if index >= search_space.gate_grid.max_candidates:
                break
    if not candidates:
        raise ValueError("shadow parameter search generated no gate candidates")
    return tuple(candidates)


def _evaluate_search_trial(
    *,
    snapshots: tuple[tuple[date, Path, dict[str, Any]], ...],
    weight_candidate: ShadowWeightCandidate,
    gate_candidate: ShadowGateCandidate,
    position_bands: tuple[PositionBand, ...],
    outcomes_by_signal_date: Mapping[date, dict[str, Any]],
    cost_rate: float,
    objective: ShadowParameterObjectiveConfig,
    production_weights: Mapping[str, float],
) -> ShadowParameterSearchTrial:
    production_returns: list[float] = []
    shadow_returns: list[float] = []
    excess_returns: list[float] = []
    pending_count = 0
    missing_count = 0
    production_turnover_sum = 0.0
    shadow_turnover_sum = 0.0
    previous_production = 0.0
    previous_shadow = 0.0
    gate_relaxation_distance_sum = 0.0
    weight_l1_distance, max_single_factor_step, changed_weight_dimensions = (
        _weight_distance_metrics(weight_candidate.target_weights, production_weights)
    )
    for signal_date, _path, snapshot in snapshots:
        production_position = _band_midpoint(
            _dict_value((snapshot.get("positions") or {}).get("final_risk_asset_ai_band"))
        )
        component_scores = _component_scores(snapshot)
        shadow_score = sum(
            component_scores[signal] * weight
            for signal, weight in weight_candidate.target_weights.items()
        )
        shadow_model_band = _band_for_score(shadow_score, position_bands)
        gate_cap_max_position, _gate_cap_sources = _gate_cap(
            snapshot,
            gate_cap_overrides=gate_candidate.gate_cap_overrides,
        )
        gate_relaxation_distance_sum += _gate_relaxation_distance(
            snapshot,
            gate_candidate.gate_cap_overrides,
        )
        shadow_final_band = _apply_gate_cap(shadow_model_band, gate_cap_max_position)
        shadow_position = _band_midpoint(shadow_final_band)
        outcome = outcomes_by_signal_date[signal_date]
        if outcome["status"] == "PENDING":
            pending_count += 1
            continue
        if outcome["status"] != "AVAILABLE":
            missing_count += 1
            continue
        asset_return = _float_required(outcome["return"])
        production_turnover = abs(production_position - previous_production)
        shadow_turnover = abs(shadow_position - previous_shadow)
        production_position_return = (
            production_position * asset_return - production_turnover * cost_rate
        )
        shadow_position_return = (
            shadow_position * asset_return - shadow_turnover * cost_rate
        )
        production_returns.append(production_position_return)
        shadow_returns.append(shadow_position_return)
        excess_returns.append(shadow_position_return - production_position_return)
        production_turnover_sum += production_turnover
        shadow_turnover_sum += shadow_turnover
        previous_production = production_position
        previous_shadow = shadow_position
    production_total_return = _compound_returns(production_returns)
    shadow_total_return = _compound_returns(shadow_returns)
    excess_total_return = (
        None
        if production_total_return is None or shadow_total_return is None
        else shadow_total_return - production_total_return
    )
    production_max_drawdown = _max_drawdown_from_returns(production_returns)
    shadow_max_drawdown = _max_drawdown_from_returns(shadow_returns)
    gate_relaxation_distance = (
        0.0
        if not snapshots
        else gate_relaxation_distance_sum / len(snapshots)
    )
    changed_dimension_count = changed_weight_dimensions + len(
        gate_candidate.gate_cap_overrides
    )
    objective_score = _search_objective_score(
        objective=objective,
        total_count=len(snapshots),
        missing_count=missing_count,
        production_total_return=production_total_return,
        shadow_total_return=shadow_total_return,
        excess_total_return=excess_total_return,
        production_max_drawdown=production_max_drawdown,
        shadow_max_drawdown=shadow_max_drawdown,
        production_turnover=production_turnover_sum,
        shadow_turnover=shadow_turnover_sum,
        weight_l1_distance=weight_l1_distance,
        gate_relaxation_distance=gate_relaxation_distance,
        changed_dimension_count=changed_dimension_count,
    )
    eligible, ineligibility_reason = _search_trial_eligibility(
        objective=objective,
        available_count=len(shadow_returns),
        excess_total_return=excess_total_return,
        objective_score=objective_score,
        weight_l1_distance=weight_l1_distance,
        max_single_factor_step=max_single_factor_step,
    )
    return ShadowParameterSearchTrial(
        trial_id=(
            f"{weight_candidate.candidate_id}__{gate_candidate.candidate_id}"
        ),
        weight_candidate_id=weight_candidate.candidate_id,
        weight_candidate_version=weight_candidate.version,
        gate_candidate_id=gate_candidate.candidate_id,
        gate_candidate_version=gate_candidate.version,
        target_weights=dict(weight_candidate.target_weights),
        gate_cap_overrides=dict(gate_candidate.gate_cap_overrides),
        total_count=len(snapshots),
        available_count=len(shadow_returns),
        pending_count=pending_count,
        missing_count=missing_count,
        production_total_return=production_total_return,
        shadow_total_return=shadow_total_return,
        excess_total_return=excess_total_return,
        production_max_drawdown=production_max_drawdown,
        shadow_max_drawdown=shadow_max_drawdown,
        production_turnover=production_turnover_sum,
        shadow_turnover=shadow_turnover_sum,
        shadow_beats_production_rate=(
            None
            if not excess_returns
            else sum(value > 0.0 for value in excess_returns) / len(excess_returns)
        ),
        weight_l1_distance_from_production=weight_l1_distance,
        max_single_factor_step=max_single_factor_step,
        gate_relaxation_distance=gate_relaxation_distance,
        changed_dimension_count=changed_dimension_count,
        objective_score=objective_score,
        eligible=eligible,
        ineligibility_reason=ineligibility_reason,
    )


def _best_search_trial(
    trials: tuple[ShadowParameterSearchTrial, ...] | list[ShadowParameterSearchTrial],
    *,
    eligible_only: bool,
) -> ShadowParameterSearchTrial | None:
    candidates = [
        trial
        for trial in trials
        if trial.objective_score is not None and (trial.eligible or not eligible_only)
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda trial: trial.objective_score or float("-inf"))


def _build_factorial_attribution(
    trials: list[ShadowParameterSearchTrial],
) -> tuple[ShadowParameterFactorialAttribution | None, str]:
    selected = _best_search_trial(trials, eligible_only=True)
    selected_eligible = selected is not None
    if selected is None:
        selected = _best_search_trial(trials, eligible_only=False)
    if selected is None:
        return None, "没有可用于 factorial attribution 的 objective trial。"

    baseline = _find_trial(
        trials,
        weight_candidate_id="source_current",
        gate_candidate_id=PRODUCTION_OBSERVED_GATE_PROFILE_ID,
    )
    weight_only = _find_trial(
        trials,
        weight_candidate_id=selected.weight_candidate_id,
        gate_candidate_id=PRODUCTION_OBSERVED_GATE_PROFILE_ID,
    )
    gate_only = _find_trial(
        trials,
        weight_candidate_id="source_current",
        gate_candidate_id=selected.gate_candidate_id,
    )
    if baseline is None or weight_only is None or gate_only is None:
        missing = []
        if baseline is None:
            missing.append("source_current x production_observed_gates")
        if weight_only is None:
            missing.append(
                f"{selected.weight_candidate_id} x {PRODUCTION_OBSERVED_GATE_PROFILE_ID}"
            )
        if gate_only is None:
            missing.append(f"source_current x {selected.gate_candidate_id}")
        return None, "factorial attribution 缺少必要 trial：" + "；".join(missing)

    return (
        ShadowParameterFactorialAttribution(
            selected_trial_id=selected.trial_id,
            selected_trial_eligible=selected_eligible,
            baseline_trial=baseline,
            weight_only_trial=weight_only,
            gate_only_trial=gate_only,
            combined_trial=selected,
        ),
        "",
    )


def _find_trial(
    trials: list[ShadowParameterSearchTrial],
    *,
    weight_candidate_id: str,
    gate_candidate_id: str,
) -> ShadowParameterSearchTrial | None:
    for trial in trials:
        if (
            trial.weight_candidate_id == weight_candidate_id
            and trial.gate_candidate_id == gate_candidate_id
        ):
            return trial
    return None


def _trial_excess_delta(
    trial: ShadowParameterSearchTrial,
    baseline: ShadowParameterSearchTrial,
) -> float | None:
    if trial.excess_total_return is None or baseline.excess_total_return is None:
        return None
    return trial.excess_total_return - baseline.excess_total_return


def _build_cap_level_attribution(
    *,
    trials: list[ShadowParameterSearchTrial],
    snapshots: tuple[tuple[date, Path, dict[str, Any]], ...],
    source_profile: WeightProfile,
    position_bands: tuple[PositionBand, ...],
    outcomes_by_signal_date: Mapping[date, dict[str, Any]],
    cost_rate: float,
    objective: ShadowParameterObjectiveConfig,
) -> tuple[ShadowParameterCapAttribution, ...]:
    selected = _best_search_trial(trials, eligible_only=True)
    if selected is None:
        selected = _best_search_trial(trials, eligible_only=False)
    if selected is None or not selected.gate_cap_overrides:
        return ()
    baseline = _find_trial(
        trials,
        weight_candidate_id="source_current",
        gate_candidate_id=PRODUCTION_OBSERVED_GATE_PROFILE_ID,
    )
    if baseline is None:
        return ()
    source_weight = ShadowWeightCandidate(
        candidate_id="source_current",
        version=source_profile.version,
        source="source_weight_profile",
        target_weights=dict(source_profile.base_weights),
    )
    rows: list[ShadowParameterCapAttribution] = []
    for gate_id, cap_value in sorted(selected.gate_cap_overrides.items()):
        cap_trial = _evaluate_search_trial(
            snapshots=snapshots,
            weight_candidate=source_weight,
            gate_candidate=ShadowGateCandidate(
                candidate_id=f"cap_only_{gate_id}",
                version=selected.gate_candidate_version,
                source="cap_level_ablation",
                gate_cap_overrides={gate_id: cap_value},
            ),
            position_bands=position_bands,
            outcomes_by_signal_date=outcomes_by_signal_date,
            cost_rate=cost_rate,
            objective=objective,
            production_weights=source_profile.base_weights,
        )
        rows.append(
            ShadowParameterCapAttribution(
                gate_id=gate_id,
                selected_cap_value=cap_value,
                cap_only_trial=cap_trial,
                excess_delta_vs_baseline=_trial_excess_delta(cap_trial, baseline),
            )
        )
    return tuple(rows)


def _build_position_change_rows(
    *,
    trials: list[ShadowParameterSearchTrial],
    snapshots: tuple[tuple[date, Path, dict[str, Any]], ...],
    position_bands: tuple[PositionBand, ...],
    outcomes_by_signal_date: Mapping[date, dict[str, Any]],
    cost_rate: float,
) -> tuple[ShadowParameterPositionChangeRow, ...]:
    selected = _best_search_trial(trials, eligible_only=True)
    if selected is None:
        selected = _best_search_trial(trials, eligible_only=False)
    if selected is None:
        return ()
    rows: list[ShadowParameterPositionChangeRow] = []
    previous_production = 0.0
    previous_candidate = 0.0
    for signal_date, _path, snapshot in snapshots:
        production_position = _band_midpoint(
            _dict_value((snapshot.get("positions") or {}).get("final_risk_asset_ai_band"))
        )
        production_gate_cap, production_gate_sources = _gate_cap(snapshot)
        _ = production_gate_cap
        component_scores = _component_scores(snapshot)
        candidate_score = sum(
            component_scores[signal] * weight
            for signal, weight in selected.target_weights.items()
        )
        candidate_model_band = _band_for_score(candidate_score, position_bands)
        candidate_gate_cap, candidate_gate_sources = _gate_cap(
            snapshot,
            gate_cap_overrides=selected.gate_cap_overrides,
        )
        candidate_final_band = _apply_gate_cap(candidate_model_band, candidate_gate_cap)
        candidate_position = _band_midpoint(candidate_final_band)
        outcome = outcomes_by_signal_date[signal_date]
        asset_return = _float_or_none(outcome.get("return"))
        production_position_return: float | None = None
        candidate_position_return: float | None = None
        return_impact: float | None = None
        if outcome.get("status") == "AVAILABLE" and asset_return is not None:
            production_turnover = abs(production_position - previous_production)
            candidate_turnover = abs(candidate_position - previous_candidate)
            production_position_return = (
                production_position * asset_return - production_turnover * cost_rate
            )
            candidate_position_return = (
                candidate_position * asset_return - candidate_turnover * cost_rate
            )
            return_impact = candidate_position_return - production_position_return
            previous_production = production_position
            previous_candidate = candidate_position
        rows.append(
            ShadowParameterPositionChangeRow(
                as_of=signal_date,
                outcome_status=str(outcome.get("status") or "UNKNOWN"),
                outcome_end_date=outcome.get("end_date"),
                asset_return=asset_return,
                production_position=production_position,
                candidate_position=candidate_position,
                position_delta=candidate_position - production_position,
                production_binding_gates=production_gate_sources,
                candidate_binding_gates=candidate_gate_sources,
                production_position_return=production_position_return,
                candidate_position_return=candidate_position_return,
                return_impact=return_impact,
            )
        )
    return tuple(rows)


def _weight_distance_metrics(
    candidate_weights: Mapping[str, float],
    production_weights: Mapping[str, float],
) -> tuple[float, float, int]:
    signals = sorted(set(candidate_weights) | set(production_weights))
    deltas = [
        abs(float(candidate_weights.get(signal, 0.0)) - float(production_weights.get(signal, 0.0)))
        for signal in signals
    ]
    return sum(deltas), max(deltas, default=0.0), sum(delta > 1e-9 for delta in deltas)


def _observed_gate_caps(snapshot: dict[str, Any]) -> dict[str, float]:
    raw_gates = (snapshot.get("positions") or {}).get("position_gates")
    if not isinstance(raw_gates, list):
        return {}
    caps: dict[str, float] = {}
    for gate in raw_gates:
        if not isinstance(gate, dict):
            continue
        gate_id = str(gate.get("gate_id") or "")
        if not gate_id or gate_id == "score_model":
            continue
        cap = _float_or_none(gate.get("max_position"))
        if cap is not None:
            caps[gate_id] = cap
    return caps


def _gate_relaxation_distance(
    snapshot: dict[str, Any],
    gate_cap_overrides: Mapping[str, float],
) -> float:
    observed = _observed_gate_caps(snapshot)
    distance = 0.0
    for gate_id, override in gate_cap_overrides.items():
        base = observed.get(gate_id)
        if base is None:
            continue
        distance += max(0.0, override - base)
    return distance


def _validate_weight_mapping(weights: dict[str, float]) -> None:
    if not weights:
        raise ValueError("target_weights must not be empty")
    empty_keys = [key for key in weights if not key.strip()]
    if empty_keys:
        raise ValueError("target_weights keys must not be empty")
    negative_keys = [key for key, value in weights.items() if value < 0.0]
    if negative_keys:
        raise ValueError(
            "target_weights must be non-negative: " + ", ".join(sorted(negative_keys))
        )
    if abs(sum(weights.values()) - 1.0) > 1e-6:
        raise ValueError("target_weights must sum to 1.0")


def _validate_profile_against_source(
    profile: ShadowWeightProfile,
    source_profile: WeightProfile,
) -> None:
    source_signals = set(source_profile.base_weights)
    profile_signals = set(profile.target_weights)
    missing = sorted(source_signals - profile_signals)
    unknown = sorted(profile_signals - source_signals)
    if missing or unknown:
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if unknown:
            details.append("unknown " + ", ".join(unknown))
        raise ValueError(
            f"shadow profile {profile.profile_id} signal mismatch: "
            + "; ".join(details)
        )
    out_of_bounds = [
        signal
        for signal, weight in profile.target_weights.items()
        if weight < source_profile.bounds.min_weight or weight > source_profile.bounds.max_weight
    ]
    if out_of_bounds:
        raise ValueError(
            f"shadow profile {profile.profile_id} weights outside source bounds: "
            + ", ".join(sorted(out_of_bounds))
        )


def _validate_weight_candidate_against_source(
    candidate: ShadowWeightCandidate,
    source_profile: WeightProfile,
) -> None:
    source_signals = set(source_profile.base_weights)
    candidate_signals = set(candidate.target_weights)
    missing = sorted(source_signals - candidate_signals)
    unknown = sorted(candidate_signals - source_signals)
    if missing or unknown:
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if unknown:
            details.append("unknown " + ", ".join(unknown))
        raise ValueError(
            f"weight candidate {candidate.candidate_id} signal mismatch: "
            + "; ".join(details)
        )
    out_of_bounds = [
        signal
        for signal, weight in candidate.target_weights.items()
        if weight < source_profile.bounds.min_weight or weight > source_profile.bounds.max_weight
    ]
    if out_of_bounds:
        raise ValueError(
            f"weight candidate {candidate.candidate_id} outside source bounds: "
            + ", ".join(sorted(out_of_bounds))
        )


def _validate_gate_candidate(candidate: ShadowGateCandidate) -> None:
    empty_keys = [key for key in candidate.gate_cap_overrides if not key.strip()]
    if empty_keys:
        raise ValueError("gate candidate override keys must not be empty")
    if "score_model" in candidate.gate_cap_overrides:
        raise ValueError(
            "score_model cap comes from score-to-position bands; "
            f"do not override it in gate candidate {candidate.candidate_id}"
        )
    out_of_bounds = [
        gate_id
        for gate_id, cap in candidate.gate_cap_overrides.items()
        if cap < 0.0 or cap > 1.0
    ]
    if out_of_bounds:
        raise ValueError(
            f"gate candidate {candidate.candidate_id} caps outside [0, 1]: "
            + ", ".join(sorted(out_of_bounds))
        )


def _iter_weight_grid(
    grid: ShadowWeightGridConfig,
    source_profile: WeightProfile,
) -> tuple[dict[str, float], ...]:
    source_signals = tuple(source_profile.base_weights)
    if set(grid.signal_values) != set(source_signals):
        missing = sorted(set(source_signals) - set(grid.signal_values))
        unknown = sorted(set(grid.signal_values) - set(source_signals))
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if unknown:
            details.append("unknown " + ", ".join(unknown))
        raise ValueError("weight_grid signals must match source profile: " + "; ".join(details))
    weights: list[dict[str, float]] = []
    value_lists = [tuple(grid.signal_values[signal]) for signal in source_signals]
    for values in itertools.product(*value_lists):
        if abs(sum(values) - 1.0) > 1e-9:
            continue
        weights.append(
            {signal: float(value) for signal, value in zip(source_signals, values, strict=True)}
        )
        if len(weights) >= grid.max_candidates:
            break
    return tuple(weights)


def _iter_gate_grid(grid: ShadowGateGridConfig) -> tuple[dict[str, float], ...]:
    if not grid.cap_values:
        raise ValueError("gate_grid enabled but cap_values is empty")
    gate_ids = tuple(grid.cap_values)
    candidates: list[dict[str, float]] = []
    value_lists = [tuple(grid.cap_values[gate_id]) for gate_id in gate_ids]
    for values in itertools.product(*value_lists):
        candidates.append(
            {
                gate_id: float(value)
                for gate_id, value in zip(gate_ids, values, strict=True)
            }
        )
        if len(candidates) >= grid.max_candidates:
            break
    return tuple(candidates)


def _weight_key(weights: Mapping[str, float]) -> tuple[tuple[str, float], ...]:
    return tuple((signal, round(value, 10)) for signal, value in sorted(weights.items()))


def _gate_key(overrides: Mapping[str, float]) -> tuple[tuple[str, float], ...]:
    return tuple((gate_id, round(value, 10)) for gate_id, value in sorted(overrides.items()))


def _load_search_snapshots(
    path: Path,
    start: date,
    end: date,
) -> tuple[tuple[date, Path, dict[str, Any]], ...]:
    paths = (path,) if path.is_file() else tuple(sorted(path.glob("decision_snapshot_*.json")))
    snapshots: list[tuple[date, Path, dict[str, Any]]] = []
    for snapshot_path in paths:
        snapshot = _read_json_object(snapshot_path)
        signal_date_raw = snapshot.get("signal_date")
        if not isinstance(signal_date_raw, str):
            continue
        signal_date = date.fromisoformat(signal_date_raw)
        if start <= signal_date <= end:
            snapshots.append((signal_date, snapshot_path, snapshot))
    return tuple(sorted(snapshots, key=lambda item: item[0]))


def _search_objective_score(
    *,
    objective: ShadowParameterObjectiveConfig,
    total_count: int,
    missing_count: int,
    production_total_return: float | None,
    shadow_total_return: float | None,
    excess_total_return: float | None,
    production_max_drawdown: float | None,
    shadow_max_drawdown: float | None,
    production_turnover: float,
    shadow_turnover: float,
    weight_l1_distance: float,
    gate_relaxation_distance: float,
    changed_dimension_count: int,
) -> float | None:
    if shadow_total_return is None or excess_total_return is None:
        return None
    production_drawdown = abs(production_max_drawdown or 0.0)
    shadow_drawdown = abs(shadow_max_drawdown or 0.0)
    excess_drawdown = max(0.0, shadow_drawdown - production_drawdown)
    excess_turnover = max(0.0, shadow_turnover - production_turnover)
    missing_ratio = 0.0 if total_count <= 0 else missing_count / total_count
    _ = production_total_return
    return (
        objective.excess_return_weight * excess_total_return
        + objective.shadow_return_weight * shadow_total_return
        - objective.excess_drawdown_penalty * excess_drawdown
        - objective.excess_turnover_penalty * excess_turnover
        - objective.missing_sample_penalty * missing_ratio
        - objective.gate_relaxation_penalty * gate_relaxation_distance
        - objective.weight_distance_penalty * weight_l1_distance
        - objective.changed_dimension_penalty * changed_dimension_count
    )


def _search_trial_eligibility(
    *,
    objective: ShadowParameterObjectiveConfig,
    available_count: int,
    excess_total_return: float | None,
    objective_score: float | None,
    weight_l1_distance: float,
    max_single_factor_step: float,
) -> tuple[bool, str]:
    if objective_score is None:
        return False, "objective_score_missing"
    if available_count < objective.min_available_samples:
        return False, "available_samples_below_objective_floor"
    if objective.require_positive_excess and (
        excess_total_return is None or excess_total_return <= 0.0
    ):
        return False, "positive_excess_required"
    if (
        objective.max_l1_distance_from_production is not None
        and weight_l1_distance > objective.max_l1_distance_from_production + 1e-9
    ):
        return False, "weight_l1_distance_above_objective_limit"
    if (
        objective.max_single_factor_step is not None
        and max_single_factor_step > objective.max_single_factor_step + 1e-9
    ):
        return False, "single_factor_step_above_objective_limit"
    return True, ""


def _pareto_front(
    trials: list[ShadowParameterSearchTrial],
) -> tuple[ShadowParameterSearchTrial, ...]:
    eligible = [
        trial
        for trial in trials
        if trial.eligible
        and trial.excess_total_return is not None
        and trial.shadow_max_drawdown is not None
    ]
    metric_representatives: dict[tuple[float, float, float], ShadowParameterSearchTrial] = {}
    for trial in eligible:
        metric_representatives.setdefault(_pareto_metric_key(trial), trial)
    front_keys: set[tuple[float, float, float]] = set()
    representatives = tuple(metric_representatives.values())
    for trial in representatives:
        dominated = False
        for other in representatives:
            if other is trial:
                continue
            if _dominates(other, trial):
                dominated = True
                break
        if not dominated:
            front_keys.add(_pareto_metric_key(trial))
    return tuple(
        sorted(
            [trial for trial in eligible if _pareto_metric_key(trial) in front_keys],
            key=lambda item: item.objective_score or float("-inf"),
            reverse=True,
        )
    )


def _pareto_metric_key(trial: ShadowParameterSearchTrial) -> tuple[float, float, float]:
    return (
        round(_float_required(trial.excess_total_return), 12),
        round(_float_required(trial.shadow_max_drawdown), 12),
        round(trial.shadow_turnover, 12),
    )


def _dominates(
    left: ShadowParameterSearchTrial,
    right: ShadowParameterSearchTrial,
) -> bool:
    left_excess = _float_required(left.excess_total_return)
    right_excess = _float_required(right.excess_total_return)
    left_drawdown = _float_required(left.shadow_max_drawdown)
    right_drawdown = _float_required(right.shadow_max_drawdown)
    left_turnover = left.shadow_turnover
    right_turnover = right.shadow_turnover
    at_least_equal = (
        left_excess >= right_excess
        and left_drawdown >= right_drawdown
        and left_turnover <= right_turnover
    )
    strictly_better = (
        left_excess > right_excess
        or left_drawdown > right_drawdown
        or left_turnover < right_turnover
    )
    return at_least_equal and strictly_better


def _best_profile_payload(report: ShadowParameterSearchReport) -> dict[str, Any]:
    best = report.best_trial
    payload: dict[str, Any] = {
        "version": "shadow_parameter_search_best_v1",
        "production_effect": "none",
        "run_id": report.run_id,
        "generated_at": report.generated_at.isoformat(),
        "search_window": {
            "start": report.start.isoformat(),
            "end": report.end.isoformat(),
        },
        "source": {
            "search_space": str(report.search_space_path),
            "objective": str(report.objective_path),
            "source_weight_profile": str(report.source_weight_profile_path),
            "search_space_checksum": report.search_space_checksum,
            "objective_checksum": report.objective_checksum,
            "source_weight_profile_checksum": report.source_weight_profile_checksum,
            "prices_checksum": report.prices_checksum,
            "decision_snapshot_checksum": report.decision_snapshot_checksum,
            "resolver_version": report.resolver_version,
            "git_commit_sha": report.git_commit_sha,
            "git_worktree_dirty": report.git_worktree_dirty,
        },
    }
    if best is None:
        payload["selected_profile"] = None
        diagnostic = report.best_diagnostic_trial
        if diagnostic is not None:
            payload["diagnostic_leading_trial"] = {
                "trial_id": diagnostic.trial_id,
                "ineligibility_reason": diagnostic.ineligibility_reason,
                "objective_score": diagnostic.objective_score,
                "available_count": diagnostic.available_count,
                "pending_count": diagnostic.pending_count,
                "missing_count": diagnostic.missing_count,
                "target_weights": diagnostic.target_weights,
                "gate_cap_overrides": diagnostic.gate_cap_overrides,
                "production_effect": "none",
            }
        return payload
    payload["selected_profile"] = {
        "trial_id": best.trial_id,
        "weight_candidate_id": best.weight_candidate_id,
        "gate_candidate_id": best.gate_candidate_id,
        "target_weights": best.target_weights,
        "gate_cap_overrides": best.gate_cap_overrides,
        "performance": {
            "objective_score": best.objective_score,
            "available_count": best.available_count,
            "pending_count": best.pending_count,
            "missing_count": best.missing_count,
            "shadow_total_return": best.shadow_total_return,
            "production_total_return": best.production_total_return,
            "excess_total_return": best.excess_total_return,
            "shadow_max_drawdown": best.shadow_max_drawdown,
            "production_max_drawdown": best.production_max_drawdown,
            "shadow_turnover": best.shadow_turnover,
            "production_turnover": best.production_turnover,
            "shadow_beats_production_rate": best.shadow_beats_production_rate,
        },
        "shadow_weight_profile_entry": {
            "profile_id": f"search_weight_{best.trial_id}",
            "version": report.run_id,
            "status": "candidate",
            "owner": "system_validation",
            "production_effect": "none",
            "rationale": (
                "Generated by validation-only shadow parameter search; "
                "not approved for production."
            ),
            "review_after_reports": report.search_space.review_after_reports,
            "target_weights": best.target_weights,
            "metadata": {"source_run_id": report.run_id},
        },
        "shadow_gate_profile_entry": {
            "profile_id": f"search_gate_{best.trial_id}",
            "version": report.run_id,
            "status": "candidate",
            "owner": "system_validation",
            "production_effect": "none",
            "rationale": (
                "Generated by validation-only shadow parameter search; "
                "not approved for production."
            ),
            "review_after_reports": report.search_space.review_after_reports,
            "gate_cap_overrides": best.gate_cap_overrides,
            "metadata": {"source_run_id": report.run_id},
        },
    }
    return payload


def _trial_row_by_id(
    trials: pd.DataFrame,
    trial_id: object,
) -> dict[str, Any] | None:
    if trial_id is None or "trial_id" not in trials.columns:
        return None
    rows = trials.loc[trials["trial_id"].astype(str) == str(trial_id)]
    if rows.empty:
        return None
    return rows.iloc[0].to_dict()


def _build_shadow_parameter_promotion_checks(
    *,
    contract: ShadowParameterPromotionContractConfig,
    manifest: dict[str, Any],
    selected_trial: dict[str, Any] | None,
    selected_trial_id: object,
    has_eligible_best: bool,
) -> list[ShadowParameterPromotionCheck]:
    checks: list[ShadowParameterPromotionCheck] = []
    if selected_trial is None:
        checks.append(
            ShadowParameterPromotionCheck(
                check_id="search_selected",
                status="FAIL",
                reason="search output 没有可读取的 selected 或 diagnostic trial。",
                evidence_ref="manifest.json + trials.csv",
            )
        )
        return checks
    if contract.require_search_eligible_best and not has_eligible_best:
        checks.append(
            ShadowParameterPromotionCheck(
                check_id="search_selected",
                status="FAIL",
                reason=(
                    "当前 search 没有 eligible best trial；diagnostic-leading 不能进入生产晋级。"
                ),
                evidence_ref=f"trial:{selected_trial_id}",
            )
        )
    else:
        checks.append(
            ShadowParameterPromotionCheck(
                check_id="search_selected",
                status="PASS",
                reason="search output 存在 contract 可评估的 selected trial。",
                evidence_ref=f"trial:{selected_trial_id}",
            )
        )

    available = _row_int(selected_trial, "available_count")
    checks.append(
        ShadowParameterPromotionCheck(
            check_id="available_sample_floor",
            status=(
                "PASS"
                if available >= contract.min_available_samples
                else "FAIL"
            ),
            reason=(
                f"available={available}，contract floor={contract.min_available_samples}。"
            ),
            evidence_ref="trials.csv:available_count",
        )
    )

    excess = _row_float(selected_trial, "excess_total_return")
    if contract.require_positive_excess and (excess is None or excess <= 0.0):
        checks.append(
            ShadowParameterPromotionCheck(
                check_id="positive_excess",
                status="FAIL",
                reason="contract 要求正 excess return，但 selected trial 未满足。",
                evidence_ref="trials.csv:excess_total_return",
            )
        )
    else:
        checks.append(
            ShadowParameterPromotionCheck(
                check_id="positive_excess",
                status="PASS",
                reason=f"selected trial excess={_format_pct(excess)}。",
                evidence_ref="trials.csv:excess_total_return",
            )
        )

    shadow_drawdown = abs(_row_float(selected_trial, "shadow_max_drawdown") or 0.0)
    production_drawdown = abs(
        _row_float(selected_trial, "production_max_drawdown") or 0.0
    )
    degradation = max(0.0, shadow_drawdown - production_drawdown)
    if contract.max_drawdown_degradation is not None:
        checks.append(
            ShadowParameterPromotionCheck(
                check_id="drawdown_degradation",
                status=(
                    "PASS"
                    if degradation <= contract.max_drawdown_degradation + 1e-12
                    else "FAIL"
                ),
                reason=(
                    f"drawdown degradation={degradation:.2%}，"
                    f"limit={contract.max_drawdown_degradation:.2%}。"
                ),
                evidence_ref="trials.csv:shadow_max_drawdown",
            )
        )

    shadow_turnover = _row_float(selected_trial, "shadow_turnover") or 0.0
    if contract.max_shadow_turnover is not None:
        checks.append(
            ShadowParameterPromotionCheck(
                check_id="shadow_turnover",
                status=(
                    "PASS"
                    if shadow_turnover <= contract.max_shadow_turnover + 1e-12
                    else "FAIL"
                ),
                reason=(
                    f"shadow turnover={shadow_turnover:.2f}，"
                    f"limit={contract.max_shadow_turnover:.2f}。"
                ),
                evidence_ref="trials.csv:shadow_turnover",
            )
        )

    factorial = manifest.get("factorial_attribution")
    primary_driver = (
        factorial.get("primary_driver") if isinstance(factorial, dict) else None
    )
    if contract.gate_primary_driver_requires_cap_review and primary_driver == "gate":
        checks.append(
            ShadowParameterPromotionCheck(
                check_id="gate_driver_cap_review",
                status="MISSING",
                reason=(
                    "factorial attribution 显示 primary driver=gate；"
                    "需要 cap-level attribution 和 forward shadow 证明不是单纯放松风险约束。"
                ),
                evidence_ref="manifest.json:factorial_attribution",
            )
        )
    else:
        checks.append(
            ShadowParameterPromotionCheck(
                check_id="gate_driver_cap_review",
                status="PASS",
                reason=f"primary driver={primary_driver or 'unknown'}。",
                evidence_ref="manifest.json:factorial_attribution",
            )
        )

    if contract.required_forward_shadow_available_samples > 0:
        checks.append(
            ShadowParameterPromotionCheck(
                check_id="forward_shadow",
                status="MISSING",
                reason=(
                    "尚未接入独立 forward shadow outcome；"
                    "contract 要求 available>="
                    f"{contract.required_forward_shadow_available_samples}。"
                ),
                evidence_ref="prediction_outcomes:missing",
            )
        )

    checks.append(
        ShadowParameterPromotionCheck(
            check_id="approved_hard_boundary",
            status="PASS",
            reason=(
                "approved_hard_allowed=false，hard effects 在下游执行链路未接入前保持不可用。"
                if not contract.approved_hard_allowed
                else "contract 允许 approved_hard；必须另有执行链路测试证明。"
            ),
            evidence_ref="contract.approved_hard_allowed",
        )
    )
    return checks


def _row_float(row: Mapping[str, Any], key: str) -> float | None:
    value = row.get(key)
    if pd.isna(value):
        return None
    return _float_or_none(value)


def _row_int(row: Mapping[str, Any], key: str) -> int:
    value = row.get(key)
    if pd.isna(value):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _search_manifest_payload(report: ShadowParameterSearchReport) -> dict[str, Any]:
    best = report.best_trial
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "shadow_parameter_search",
        "status": report.status,
        "production_effect": "none",
        "run_id": report.run_id,
        "generated_at": report.generated_at.isoformat(),
        "search_window": {
            "start": report.start.isoformat(),
            "end": report.end.isoformat(),
        },
        "strategy_ticker": report.strategy_ticker,
        "horizon_days": report.horizon_days,
        "cost_bps": report.cost_bps,
        "slippage_bps": report.slippage_bps,
        "decision_snapshot_path": str(report.decision_snapshot_path),
        "prices_path": str(report.prices_path),
        "source_weight_profile_path": str(report.source_weight_profile_path),
        "search_space_path": str(report.search_space_path),
        "objective_path": str(report.objective_path),
        "search_space_checksum": report.search_space_checksum,
        "objective_checksum": report.objective_checksum,
        "source_weight_profile_checksum": report.source_weight_profile_checksum,
        "prices_checksum": report.prices_checksum,
        "decision_snapshot_checksum": report.decision_snapshot_checksum,
        "resolver_version": report.resolver_version,
        "git_commit_sha": report.git_commit_sha,
        "git_worktree_dirty": report.git_worktree_dirty,
        "search_algorithm": "exhaustive_grid_with_optional_manifest_seeds",
        "weight_grid_enabled": report.search_space.weight_grid.enabled,
        "gate_grid_enabled": report.search_space.gate_grid.enabled,
        "snapshot_count": report.snapshot_count,
        "weight_candidate_count": report.weight_candidate_count,
        "gate_candidate_count": report.gate_candidate_count,
        "trial_count": len(report.trials),
        "pareto_front_count": len(report.pareto_front),
        "best_trial_id": None if best is None else best.trial_id,
        "best_objective_score": None if best is None else best.objective_score,
        "best_diagnostic_trial_id": (
            None
            if report.best_diagnostic_trial is None
            else report.best_diagnostic_trial.trial_id
        ),
        "factorial_attribution": (
            None
            if report.factorial_attribution is None
            else report.factorial_attribution.to_dict()
        ),
        "cap_attribution": [item.to_dict() for item in report.cap_attribution],
        "position_change_rows": [
            row.to_dict() for row in report.position_change_rows
        ],
        "warnings": list(report.warnings),
    }


def _production_observed_gate_profile() -> ShadowPositionGateProfile:
    return ShadowPositionGateProfile(
        profile_id=PRODUCTION_OBSERVED_GATE_PROFILE_ID,
        version="production_observed",
        status="shadow",
        owner="system_validation",
        production_effect="none",
        rationale="使用 production decision snapshot 中已观察到的 gate cap。",
        review_after_reports=1,
        gate_cap_overrides={},
        metadata={"isolation": "shadow_observation_only"},
    )


def _combined_shadow_profile_id(
    weight_profile: ShadowWeightProfile,
    gate_profile: ShadowPositionGateProfile,
) -> str:
    if gate_profile.profile_id == PRODUCTION_OBSERVED_GATE_PROFILE_ID:
        return weight_profile.profile_id
    return f"{weight_profile.profile_id}__{gate_profile.profile_id}"


def _combined_shadow_profile_version(
    weight_profile: ShadowWeightProfile,
    gate_profile: ShadowPositionGateProfile,
) -> str:
    if gate_profile.profile_id == PRODUCTION_OBSERVED_GATE_PROFILE_ID:
        return weight_profile.version
    return f"{weight_profile.version}+{gate_profile.version}"


def _combined_shadow_profile_status(
    weight_profile: ShadowWeightProfile,
    gate_profile: ShadowPositionGateProfile,
) -> str:
    if weight_profile.status == "retired" or gate_profile.status == "retired":
        return "retired"
    if weight_profile.status == "candidate" or gate_profile.status == "candidate":
        return "candidate"
    return "shadow"


def _load_observation_rows(
    path: Path,
    as_of: date,
    *,
    since: date | None,
) -> tuple[dict[str, Any], ...]:
    if not path.exists():
        raise FileNotFoundError(f"shadow weight observation ledger not found: {path}")
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    required = {"as_of", "profile_id", "profile_version"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(
            "shadow weight observation ledger missing columns: "
            + ", ".join(sorted(missing))
        )
    for column in ("production_gated_target_position", "shadow_gated_target_position"):
        if column not in frame.columns:
            frame[column] = ""
    frame["_as_of"] = pd.to_datetime(frame["as_of"], errors="coerce")
    frame = frame.loc[frame["_as_of"].notna()].copy()
    frame = frame.loc[frame["_as_of"].dt.date <= as_of].copy()
    if since is not None:
        frame = frame.loc[frame["_as_of"].dt.date >= since].copy()
    frame = frame.sort_values(["_as_of", "profile_id"])
    return tuple(frame.drop(columns=["_as_of"]).to_dict(orient="records"))


def _strategy_price_series(path: Path, ticker: str) -> pd.Series:
    prices = pd.read_csv(path)
    required = {"date", "ticker", "adj_close"}
    missing = required - set(prices.columns)
    if missing:
        raise ValueError("prices CSV missing columns: " + ", ".join(sorted(missing)))
    frame = prices.loc[prices["ticker"] == ticker].copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["_date"].notna() & frame["_adj_close"].notna()].copy()
    if frame.empty:
        raise ValueError(f"prices CSV contains no rows for strategy ticker: {ticker}")
    return frame.sort_values("_date").set_index("_date")["_adj_close"]


def _horizon_return(
    series: pd.Series,
    *,
    signal_date: date,
    as_of: date,
    horizon_days: int,
) -> dict[str, Any]:
    if signal_date not in set(series.index.date):
        return {
            "status": "MISSING_DATA",
            "reason": f"signal_date 无策略 ticker 收盘价：{signal_date.isoformat()}",
            "end_date": None,
            "return": None,
        }
    start_index = _date_position(series, signal_date)
    end_index = start_index + horizon_days
    if end_index >= len(series):
        return {
            "status": "PENDING",
            "reason": "价格历史尚未覆盖完整观察窗口",
            "end_date": None,
            "return": None,
        }
    end_date = series.index[end_index].date()
    if end_date > as_of:
        return {
            "status": "PENDING",
            "reason": "观察窗口结束日在本次 as_of 之后",
            "end_date": end_date,
            "return": None,
        }
    window = series.iloc[start_index : end_index + 1]
    return {
        "status": "AVAILABLE",
        "reason": "",
        "end_date": end_date,
        "return": float(window.iloc[-1] / window.iloc[0] - 1.0),
    }


def _performance_summaries(
    rows: list[ShadowWeightPerformanceRow],
) -> tuple[ShadowWeightPerformanceSummary, ...]:
    grouped: dict[tuple[str, str], list[ShadowWeightPerformanceRow]] = {}
    for row in rows:
        grouped.setdefault((row.profile_id, row.profile_version), []).append(row)
    summaries: list[ShadowWeightPerformanceSummary] = []
    for (profile_id, profile_version), group_rows in sorted(grouped.items()):
        available = [row for row in group_rows if row.outcome_status == "AVAILABLE"]
        production_returns = [
            _float_required(row.production_position_return)
            for row in available
            if row.production_position_return is not None
        ]
        shadow_returns = [
            _float_required(row.shadow_position_return)
            for row in available
            if row.shadow_position_return is not None
        ]
        excess_returns = [
            _float_required(row.excess_position_return)
            for row in available
            if row.excess_position_return is not None
        ]
        summaries.append(
            ShadowWeightPerformanceSummary(
                profile_id=str(profile_id),
                profile_version=str(profile_version),
                total_count=len(group_rows),
                available_count=len(available),
                pending_count=sum(
                    row.outcome_status == "PENDING" for row in group_rows
                ),
                missing_count=sum(
                    row.outcome_status == "MISSING_DATA" for row in group_rows
                ),
                production_total_return=_compound_returns(production_returns),
                shadow_total_return=_compound_returns(shadow_returns),
                excess_total_return=(
                    None
                    if not production_returns or not shadow_returns
                    else _compound_returns(shadow_returns)
                    - _compound_returns(production_returns)
                ),
                production_max_drawdown=_max_drawdown_from_returns(production_returns),
                shadow_max_drawdown=_max_drawdown_from_returns(shadow_returns),
                production_turnover=sum(
                    row.production_turnover or 0.0 for row in available
                ),
                shadow_turnover=sum(row.shadow_turnover or 0.0 for row in available),
                shadow_beats_production_rate=(
                    None
                    if not excess_returns
                    else sum(value > 0.0 for value in excess_returns)
                    / len(excess_returns)
                ),
            )
        )
    return tuple(sorted(summaries, key=lambda item: item.profile_id))


def _compound_returns(values: list[float]) -> float | None:
    if not values:
        return None
    equity = 1.0
    for value in values:
        equity *= 1.0 + value
    return equity - 1.0


def _max_drawdown_from_returns(values: list[float]) -> float | None:
    if not values:
        return None
    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for value in values:
        equity *= 1.0 + value
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity / peak - 1.0)
    return max_drawdown


def _date_position(series: pd.Series, value: date) -> int:
    matches = series.index[series.index.date == value]
    if len(matches) == 0:
        raise ValueError(f"date not found in series: {value.isoformat()}")
    return int(series.index.get_loc(matches[0]))


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON file must contain an object: {path}")
    return payload


def _component_scores(snapshot: dict[str, Any]) -> dict[str, float]:
    raw_components = (snapshot.get("scores") or {}).get("components")
    if not isinstance(raw_components, list) or not raw_components:
        raise ValueError("decision snapshot missing scores.components")
    scores: dict[str, float] = {}
    for item in raw_components:
        if not isinstance(item, dict):
            continue
        name = str(item.get("component") or "")
        if not name:
            continue
        scores[name] = _float_required(item.get("score"))
    if not scores:
        raise ValueError("decision snapshot contains no component scores")
    return scores


def _missing_gate_override_ids(
    snapshot: dict[str, Any],
    gate_cap_overrides: Mapping[str, float],
) -> tuple[str, ...]:
    raw_gates = (snapshot.get("positions") or {}).get("position_gates")
    if not isinstance(raw_gates, list):
        return tuple(sorted(gate_cap_overrides))
    observed = {
        str(gate.get("gate_id") or "")
        for gate in raw_gates
        if isinstance(gate, dict) and str(gate.get("gate_id") or "")
    }
    return tuple(sorted(set(gate_cap_overrides) - observed))


def _gate_cap(
    snapshot: dict[str, Any],
    *,
    gate_cap_overrides: Mapping[str, float] | None = None,
) -> tuple[float, tuple[str, ...]]:
    raw_gates = (snapshot.get("positions") or {}).get("position_gates")
    overrides = gate_cap_overrides or {}
    if not isinstance(raw_gates, list):
        return 1.0, ()
    cap = 1.0
    sources: list[str] = []
    for gate in raw_gates:
        if not isinstance(gate, dict):
            continue
        if str(gate.get("gate_id") or "") == "score_model":
            continue
        gate_id = str(gate.get("gate_id") or gate.get("label") or "unknown")
        observed_position = _float_or_none(gate.get("max_position"))
        max_position = overrides.get(gate_id, observed_position)
        if max_position is None:
            continue
        source = gate_id
        if gate_id in overrides and observed_position is not None:
            source = f"{gate_id}:{observed_position:.0%}->{max_position:.0%}"
        if max_position < cap:
            cap = max_position
            sources = [source]
        elif abs(max_position - cap) <= 1e-9:
            sources.append(source)
    return cap, tuple(dict.fromkeys(sources))


def _load_position_bands(scoring_rules_path: Path | None) -> tuple[PositionBand, ...]:
    path = scoring_rules_path or PROJECT_ROOT / "config" / "scoring_rules.yaml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or not isinstance(raw.get("position_bands"), list):
        raise ValueError(f"scoring rules missing position_bands: {path}")
    bands = []
    for item in raw["position_bands"]:
        if not isinstance(item, dict):
            continue
        bands.append(
            PositionBand(
                min_score=_float_required(item.get("min_score")),
                min_position=_float_required(item.get("min_position")),
                max_position=_float_required(item.get("max_position")),
                label=str(item.get("label") or "unknown"),
            )
        )
    if not bands:
        raise ValueError(f"scoring rules contain no valid position_bands: {path}")
    return tuple(sorted(bands, key=lambda band: band.min_score, reverse=True))


def _band_for_score(score: float, bands: tuple[PositionBand, ...]) -> dict[str, Any]:
    for band in bands:
        if score >= band.min_score:
            return {
                "label": band.label,
                "min_position": band.min_position,
                "max_position": band.max_position,
            }
    band = bands[-1]
    return {
        "label": band.label,
        "min_position": band.min_position,
        "max_position": band.max_position,
    }


def _apply_gate_cap(band: dict[str, Any], gate_cap_max_position: float) -> dict[str, Any]:
    max_position = min(_float_required(band.get("max_position")), gate_cap_max_position)
    min_position = min(_float_required(band.get("min_position")), max_position)
    label = str(band.get("label") or "unknown")
    if max_position < _float_required(band.get("max_position")):
        label = f"{label}/仓位受限"
    return {
        "label": label,
        "min_position": min_position,
        "max_position": max_position,
    }


def _band_midpoint(band: dict[str, Any]) -> float:
    min_position = _float_required(band.get("min_position"))
    max_position = _float_required(band.get("max_position"))
    return (min_position + max_position) / 2.0


def _band_label(band: dict[str, Any]) -> str:
    label = str(band.get("label") or "unknown")
    min_position = _float_or_none(band.get("min_position"))
    max_position = _float_or_none(band.get("max_position"))
    if min_position is None or max_position is None:
        return label
    return f"{label} {min_position:.0%}-{max_position:.0%}"


def _dict_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _float_required(value: Any) -> float:
    parsed = _float_or_none(value)
    if parsed is None:
        raise ValueError(f"expected numeric value, got {value!r}")
    return parsed


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _blank_if_none(value: Any) -> Any:
    return "" if value is None else value


def _format_pct(value: float | None) -> str:
    if value is None:
        return "NA"
    return f"{value:.2%}"


def _format_score(value: float | None) -> str:
    if value is None:
        return "NA"
    return f"{value:.4f}"


def _format_weight_mapping(weights: Mapping[str, float]) -> str:
    return ", ".join(f"{signal}={value:.0%}" for signal, value in sorted(weights.items()))


def _factorial_row(
    cell: str,
    trial: ShadowParameterSearchTrial,
    excess_delta: float | None,
) -> str:
    return (
        "| "
        f"{cell} | "
        f"`{trial.trial_id}` | "
        f"`{trial.weight_candidate_id}` | "
        f"`{trial.gate_candidate_id}` | "
        f"{_format_pct(trial.shadow_total_return)} | "
        f"{_format_pct(trial.excess_total_return)} | "
        f"{_format_pct(excess_delta)} | "
        f"{_format_pct(trial.shadow_max_drawdown)} | "
        f"{trial.shadow_turnover:.2f} |"
    )


def _format_gate_overrides(overrides: Mapping[str, float]) -> str:
    if not overrides:
        return "production observed"
    return ", ".join(
        f"{gate_id}={value:.2f}" for gate_id, value in sorted(overrides.items())
    )


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_search_snapshots(
    snapshots: tuple[tuple[date, Path, dict[str, Any]], ...],
) -> str:
    digest = hashlib.sha256()
    for signal_date, path, _snapshot in snapshots:
        digest.update(signal_date.isoformat().encode("utf-8"))
        digest.update(str(path).encode("utf-8"))
        digest.update(_sha256_file(path).encode("utf-8"))
    return digest.hexdigest()


def _git_commit_sha() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    sha = result.stdout.strip()
    return sha or None


def _git_worktree_dirty() -> bool | None:
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    return bool(result.stdout.strip())


def _project_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path
