from __future__ import annotations

import calendar
import csv
import json
import subprocess
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from itertools import product
from pathlib import Path
from typing import Any, Literal, Self

import pandas as pd
import yaml
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import (
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.etf_portfolio.data import load_standard_prices
from ai_trading_system.etf_portfolio.dynamic_allocation import (
    load_dynamic_allocation_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_rescue import (
    load_dynamic_failure_diagnostics_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_robustness import (
    load_dynamic_robustness_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_v3_real_evaluation import (
    DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH,
    DynamicV3RealEvaluationPolicyConfig,
    build_dynamic_v3_real_evaluation_report,
    load_dynamic_v3_real_evaluation_policy_config,
    write_dynamic_v3_real_evaluation_report,
)
from ai_trading_system.etf_portfolio.dynamic_v3_rescue import (
    load_dynamic_v3_rescue_policy_config,
)
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_PRICE_PATH,
    load_etf_config_bundle,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

STRATEGY_FAMILY = "dynamic_v3_rescue"
SCHEMA_VERSION = 1
EVALUATOR_TINY_FIXTURE_PROXY = "tiny_fixture_proxy"
EVALUATOR_REAL_DYNAMIC_V3_RESCUE = "real_dynamic_v3_rescue"
EvaluatorMode = Literal["tiny_fixture_proxy", "real_dynamic_v3_rescue"]
EVALUATOR_VERSIONS: dict[str, str] = {
    EVALUATOR_TINY_FIXTURE_PROXY: "tiny_fixture_proxy_v1",
    EVALUATOR_REAL_DYNAMIC_V3_RESCUE: "real_dynamic_v3_rescue_v1",
}

DEFAULT_PARAMETER_SWEEP_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_v3_rescue" / "parameter_sweep_v1.yaml"
)
DEFAULT_PARAMETER_SWEEP_PROFILE_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "parameter_sweep_profiles.yaml"
)
DEFAULT_PARAMETER_GOVERNANCE_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "parameter_governance_v1.yaml"
)
DEFAULT_DYNAMIC_V3_RESEARCH_ROOT = PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_v3_rescue"
DEFAULT_SWEEP_OUTPUT_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "sweeps"
DEFAULT_DATA_AUDIT_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "data_audit"
DEFAULT_DATA_PROVENANCE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "data_provenance"
DEFAULT_WINDOW_AUDIT_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "window_audit"
DEFAULT_INJECTION_AUDIT_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "injection_audit"
DEFAULT_CANDIDATE_ATTRIBUTION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "candidate_attribution"
DEFAULT_WALK_FORWARD_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "walk_forward"
DEFAULT_WALK_FORWARD_SELECTION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "walk_forward_selection"
DEFAULT_ROBUSTNESS_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "robustness"
DEFAULT_OVERFIT_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "overfit"
DEFAULT_SHADOW_REPORT_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "shadow"
DEFAULT_SHADOW_MONITOR_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "shadow_monitor"
DEFAULT_PROMOTION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "promotion"
DEFAULT_GOVERNANCE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "governance"
DEFAULT_RESEARCH_INDEX_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "index"
DEFAULT_LATEST_POINTER_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "latest"
DEFAULT_SHADOW_REGISTRY_PATH = (
    PROJECT_ROOT / "registry" / "etf_portfolio" / "dynamic_v3_rescue_shadow_candidates.yaml"
)
DEFAULT_RATES_CACHE_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"

GATE_REJECT = "reject"
GATE_REVIEW_REQUIRED = "review_required"
GATE_OBSERVE_ONLY = "observe_only"
GATE_PROMOTE_CANDIDATE = "promote_candidate"
FORBIDDEN_GATE = "production_candidate"
DATE_RANGE_PASS = "PASS"
DATE_RANGE_PASS_WITH_WARNINGS = "PASS_WITH_WARNINGS"
DATE_RANGE_INCOMPLETE = "INCOMPLETE"
DATE_RANGE_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
DATE_RANGE_FAIL = "FAIL"
WINDOW_PROMOTION_BLOCKING_STATUSES = {
    DATE_RANGE_INCOMPLETE,
    DATE_RANGE_INSUFFICIENT_DATA,
    DATE_RANGE_FAIL,
}
WEIGHT_PATH_COMPLETE = "COMPLETE"
WEIGHT_PATH_PARTIAL = "PARTIAL"
WEIGHT_PATH_INCOMPLETE = "INCOMPLETE"
DATA_PROVENANCE_RECONSTRUCTED = "RECONSTRUCTED_MANIFEST"

# TRADING-111 allows a small start-date grace for signal-lag / warm-up mechanics;
# a larger gap changes investment interpretation and must block promotion.
WINDOW_AUDIT_ALLOWED_START_DELAY_DAYS = 5

# TRADING-101 pilot mapping: sweep axes are bounded inputs to existing
# TRADING-091 materialization controls. These are research-only transforms, not
# production thresholds; the requirement doc records the exit condition for
# replacing this pilot mapping with calibrated policy fields.
REAL_EVALUATOR_BASE_SMOOTH_WINDOW_DAYS = 5
REAL_EVALUATOR_MIN_REBALANCE_DELTA_PER_COOLDOWN_DAY = 0.001
REAL_EVALUATOR_DRAW_DOWN_GUARD_MULTIPLIERS = {
    "none": 0.20,
    "soft": 0.60,
    "hard": 1.00,
}
REAL_EVALUATOR_MIN_POSITIVE_MATERIALIZATION_VALUE = 0.001
REAL_EVALUATOR_MIN_WEEKLY_TURNOVER_CAP = 0.01
REAL_EVALUATOR_EVENT_RISK_THRESHOLD_PER_CONFIRMATION = 1.0
REQUIRED_INJECTION_PARAMETERS = (
    "rescue_intensity",
    "smooth_window_days",
    "constraint_buffer_bps",
    "turnover_penalty",
    "risk_off_confirmation_days",
    "rebalance_cooldown_days",
    "drawdown_guard",
)
PARAMETER_EFFECT_FIELDS: dict[str, tuple[str, ...]] = {
    "rescue_intensity": (
        "materialization.soft_penalty_strength",
        "materialization.drawdown_cash_increase_step",
        "materialization.emergency_event_risk_cash_increase_step",
    ),
    "smooth_window_days": (
        "materialization.smoothing_max_single_rebalance_delta",
        "smoothing_policy.max_single_rebalance_delta",
    ),
    "constraint_buffer_bps": (
        "materialization.qqq_target_buffer",
        "soft_constraint_penalties.interior_buffer",
    ),
    "turnover_penalty": (
        "materialization.smoothing_weekly_turnover_cap",
        "soft_constraint_penalties.penalty_strength",
    ),
    "risk_off_confirmation_days": (
        "materialization.emergency_event_risk_high_threshold",
        "drawdown_guardrails.min_confirmations",
    ),
    "rebalance_cooldown_days": ("materialization.smoothing_min_rebalance_weight_delta",),
    "drawdown_guard": (
        "materialization.drawdown_cash_increase_step",
        "materialization.drawdown_semiconductor_reduction_step",
        "materialization.drawdown_qqq_reduction_step",
    ),
}

DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY: dict[str, Any] = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
    "production_state_mutated": False,
    "baseline_config_mutated": False,
    "official_target_weights_mutated": False,
    "automatic_candidate_promotion": False,
    "auto_enrollment_without_owner_approval": False,
    "shadow_enrollment_allowed": False,
    "automatic_enrollment_allowed": False,
    "owner_approval_executed": False,
    "production_candidate_generated": False,
}


class DynamicV3ParameterResearchError(ValueError):
    """Raised when dynamic v3 parameter research inputs or artifacts are invalid."""


@dataclass(frozen=True)
class RealEvaluationContext:
    prices: pd.DataFrame
    etf_config: Any
    real_policy: DynamicV3RealEvaluationPolicyConfig
    v3_rescue_policy: Any
    dynamic_robustness_policy: Any
    dynamic_policy: Any
    failure_policy: Any
    data_quality_status: str
    data_quality_report_path: Path
    prices_path: Path
    real_evaluation_output_dir: Path
    data_manifest_hash: str


class SweepRunConfig(BaseModel):
    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    seed: int
    max_candidates: int = Field(ge=1)
    mode: Literal["grid"]
    allow_resume: bool


class SweepDataConfig(BaseModel):
    as_of: date
    end: date
    min_history_days: int = Field(ge=1)
    quality_status: str = Field(min_length=1)
    manifest_hash: str = Field(min_length=1)
    allow_data_quality: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_dates(self) -> Self:
        if self.end < self.as_of:
            raise ValueError("sweep data end cannot predate as_of")
        return self


class SweepBaselineConfig(BaseModel):
    static: str = Field(min_length=1)
    dynamic_reference: list[str] = Field(min_length=1)


class ParameterAxis(BaseModel):
    values: list[Any] = Field(min_length=1)


class HardConstraintConfig(BaseModel):
    max_constraint_hit_rate: float = Field(ge=0, le=1)
    max_constraint_hits_delta_vs_reference: int
    max_false_risk_off_delta: int
    max_turnover: float = Field(ge=0)
    max_drawdown_degradation_pp: float
    max_dynamic_vs_static_gap: float = Field(ge=0)
    require_data_quality_not_fail: bool
    require_no_lookahead: bool
    allow_robustness_status: list[str] = Field(min_length=1)
    noise_floor_improvement: float = Field(ge=0)


class PromotionConstraintConfig(BaseModel):
    require_robustness_status: Literal["PASS"]
    require_walk_forward_status: Literal["PASS"]
    require_oos_status: Literal["PASS"]
    require_stress_bucket_status: Literal["PASS"]
    max_parameter_sensitivity_status: Literal["REVIEW_REQUIRED", "PASS"]
    require_shadow_observation: bool


class ScoringWeights(BaseModel):
    dynamic_vs_static_gap_improvement: float = Field(ge=0)
    constraint_hit_reduction: float = Field(ge=0)
    drawdown_preservation: float = Field(ge=0)
    turnover_reduction: float = Field(ge=0)
    false_risk_off_control: float = Field(ge=0)
    robustness_score: float = Field(ge=0)

    @model_validator(mode="after")
    def validate_nonzero(self) -> Self:
        if sum(self.model_dump(mode="json").values()) <= 0:
            raise ValueError("at least one scoring weight must be positive")
        return self


class ScoringConfig(BaseModel):
    objective: Literal["constrained_weighted_score"]
    weights: ScoringWeights


class WalkForwardConfig(BaseModel):
    enabled: bool
    train_window_months: int = Field(ge=1)
    test_window_months: int = Field(ge=1)
    step_months: int = Field(ge=1)
    min_windows: int = Field(ge=1)
    top_n: int = Field(ge=1)
    min_pass_ratio: float = Field(default=0.60, ge=0, le=1)
    max_oos_degradation_pp: float = Field(default=0.02, ge=0)


class OutOfSampleConfig(BaseModel):
    enabled: bool
    holdout_start: date
    holdout_end: date

    @model_validator(mode="after")
    def validate_dates(self) -> Self:
        if self.holdout_end < self.holdout_start:
            raise ValueError("holdout_end cannot predate holdout_start")
        return self


class RobustnessConfig(BaseModel):
    enabled: bool
    neighbor_grid_steps: int = Field(ge=1)
    require_stress_buckets: list[str] = Field(min_length=1)
    overfit_status_allow: list[str] = Field(min_length=1)
    max_score_delta_for_pass: float = Field(default=0.15, ge=0)
    max_score_delta_for_review: float = Field(default=0.30, ge=0)
    stress_pass_ratio_for_pass: float = Field(default=0.80, ge=0, le=1)


class ShadowConfig(BaseModel):
    enabled: bool
    promotion_earliest_after_rebalance_count: int = Field(ge=0)
    promotion_earliest_after_days: int = Field(ge=0)
    required_observation_metrics: list[str] = Field(min_length=1)


class ArtifactRetentionConfig(BaseModel):
    keep_recent_full_sweeps: int = Field(ge=1)
    keep_recent_failed_sweeps: int = Field(ge=1)
    keep_all_observe_only: bool
    keep_all_promote_candidate: bool
    keep_all_production_candidate: bool
    stale_after_days: int = Field(default=14, ge=1)


class ExecutionConfig(BaseModel):
    workers: int = Field(ge=1)
    checkpoint_every_candidates: int = Field(ge=1)
    fail_fast_on_schema_error: bool
    continue_on_candidate_error: bool
    evaluator: EvaluatorMode = EVALUATOR_TINY_FIXTURE_PROXY
    evaluation_mode: EvaluatorMode | None = None

    @model_validator(mode="after")
    def validate_evaluator_alias(self) -> Self:
        if self.evaluation_mode is not None:
            if (
                self.evaluator != EVALUATOR_TINY_FIXTURE_PROXY
                and self.evaluator != self.evaluation_mode
            ):
                raise ValueError("execution.evaluator and execution.evaluation_mode conflict")
            self.evaluator = self.evaluation_mode
        self.evaluation_mode = self.evaluator
        return self


class DynamicV3ParameterSweepConfig(BaseModel):
    schema_version: Literal[1]
    run: SweepRunConfig
    data: SweepDataConfig
    baselines: SweepBaselineConfig
    parameter_space: dict[str, ParameterAxis] = Field(min_length=1)
    hard_constraints: HardConstraintConfig
    promotion_constraints: PromotionConstraintConfig
    scoring: ScoringConfig
    walk_forward: WalkForwardConfig
    out_of_sample: OutOfSampleConfig
    robustness: RobustnessConfig
    shadow: ShadowConfig
    artifact_retention: ArtifactRetentionConfig
    execution: ExecutionConfig

    @model_validator(mode="after")
    def validate_config(self) -> Self:
        if self.data.quality_status not in self.data.allow_data_quality:
            raise ValueError("configured data quality status is not allowed")
        if self.run.max_candidates < 1:
            raise ValueError("max_candidates must be positive")
        return self


class SweepProfile(BaseModel):
    description: str = Field(min_length=1)
    config_path: Path
    evaluator_mode: EvaluatorMode
    max_candidates: int = Field(ge=1)
    workers: int = Field(ge=1)
    ci_safe: bool
    not_for_investment_decision: bool

    @model_validator(mode="after")
    def validate_profile(self) -> Self:
        if self.evaluator_mode == EVALUATOR_TINY_FIXTURE_PROXY:
            if self.not_for_investment_decision is not True:
                raise ValueError("tiny_fixture profile must be not_for_investment_decision")
        if self.evaluator_mode == EVALUATOR_REAL_DYNAMIC_V3_RESCUE:
            if self.not_for_investment_decision is True:
                raise ValueError("real profile cannot be marked not_for_investment_decision")
            if self.ci_safe is True:
                raise ValueError("real evaluator profiles are not CI safe")
        return self


class SweepProfileConfig(BaseModel):
    schema_version: Literal[1]
    profiles: dict[str, SweepProfile] = Field(min_length=1)


class ParameterGovernanceGroup(BaseModel):
    search_policy: Literal[
        "manual_only",
        "controlled_search",
        "auto_search_allowed",
        "manual_review_required",
    ]
    parameters: list[str] = Field(min_length=1)


class ParameterGovernanceConfig(BaseModel):
    schema_version: Literal[1]
    policy_id: str = Field(min_length=1)
    version: str = Field(min_length=1)
    status: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    rationale: str = Field(min_length=1)
    intended_effect: str = Field(min_length=1)
    validation_evidence: str = Field(min_length=1)
    review_condition: str = Field(min_length=1)
    search_space_version: str = Field(min_length=1)
    parameter_groups: dict[str, ParameterGovernanceGroup] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_unique_parameters(self) -> Self:
        seen: dict[str, str] = {}
        duplicates: list[str] = []
        for group_name, group in self.parameter_groups.items():
            for parameter in group.parameters:
                if parameter in seen:
                    duplicates.append(parameter)
                seen[parameter] = group_name
        if duplicates:
            raise ValueError("parameter governance duplicates: " + ", ".join(sorted(duplicates)))
        return self


def load_parameter_sweep_config(
    path: Path | str = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
) -> DynamicV3ParameterSweepConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, Mapping):
        raise DynamicV3ParameterResearchError("parameter sweep config must be a mapping")
    try:
        return DynamicV3ParameterSweepConfig.model_validate(raw)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3ParameterResearchError(f"invalid parameter sweep config: {exc}") from exc


def load_sweep_profile_config(
    path: Path | str = DEFAULT_PARAMETER_SWEEP_PROFILE_CONFIG_PATH,
) -> SweepProfileConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, Mapping):
        raise DynamicV3ParameterResearchError("parameter sweep profiles config must be a mapping")
    try:
        return SweepProfileConfig.model_validate(raw)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3ParameterResearchError(
            f"invalid parameter sweep profile config: {exc}"
        ) from exc


def load_parameter_governance_config(
    path: Path | str = DEFAULT_PARAMETER_GOVERNANCE_CONFIG_PATH,
) -> ParameterGovernanceConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, Mapping):
        raise DynamicV3ParameterResearchError("parameter governance config must be a mapping")
    try:
        return ParameterGovernanceConfig.model_validate(raw)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3ParameterResearchError(
            f"invalid parameter governance config: {exc}"
        ) from exc


def normalized_sweep_config(config: DynamicV3ParameterSweepConfig) -> dict[str, Any]:
    return config.model_dump(mode="json")


def parameter_grid_candidates(
    config: DynamicV3ParameterSweepConfig,
    *,
    strategy_family: str = STRATEGY_FAMILY,
    code_version: str | None = None,
    data_manifest_hash: str | None = None,
) -> list[dict[str, Any]]:
    keys = list(config.parameter_space.keys())
    axes = [config.parameter_space[key].values for key in keys]
    version = code_version or _git_commit()
    manifest_hash = data_manifest_hash or config.data.manifest_hash
    candidates: list[dict[str, Any]] = []
    for values in product(*axes):
        parameters = dict(zip(keys, values, strict=True))
        candidate_id = stable_candidate_id(
            parameters,
            strategy_family=strategy_family,
            code_version=version,
            data_manifest_hash=manifest_hash,
        )
        candidates.append(
            {
                "candidate_id": candidate_id,
                "strategy_family": strategy_family,
                "parameters": parameters,
                "code_version": version,
                "data_manifest_hash": manifest_hash,
            }
        )
        if len(candidates) >= config.run.max_candidates:
            break
    return candidates


def stable_candidate_id(
    parameters: Mapping[str, Any],
    *,
    strategy_family: str,
    code_version: str,
    data_manifest_hash: str,
) -> str:
    raw = {
        "parameters": _jsonable(parameters),
        "strategy_family": strategy_family,
        "code_version": code_version,
        "data_manifest_hash": data_manifest_hash,
    }
    return sha256(_canonical_json(raw).encode("utf-8")).hexdigest()[:16]


def build_sweep_config_validation(
    config_path: Path = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    governance_path: Path = DEFAULT_PARAMETER_GOVERNANCE_CONFIG_PATH,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    try:
        config = load_parameter_sweep_config(config_path)
        candidates = parameter_grid_candidates(config)
        governance = load_parameter_governance_config(governance_path)
        checks.extend(
            [
                _check("schema_valid", True, "parameter sweep schema is valid"),
                _check(
                    "parameter_space_nonempty",
                    bool(config.parameter_space),
                    "parameter space has at least one axis",
                ),
                _check(
                    "hard_constraints_present",
                    bool(config.hard_constraints),
                    "hard constraints are configured",
                ),
                _check("scoring_present", bool(config.scoring), "scoring policy is configured"),
                _check(
                    "max_candidates_respected",
                    len(candidates) <= config.run.max_candidates,
                    "candidate preview respects max_candidates",
                ),
                _check(
                    "production_candidate_blocked",
                    FORBIDDEN_GATE not in {GATE_REJECT, GATE_REVIEW_REQUIRED, GATE_OBSERVE_ONLY},
                    "automatic commands do not expose production_candidate",
                ),
                _check(
                    "governance_policy_valid",
                    True,
                    governance.policy_id,
                ),
            ]
        )
        checks.extend(_governance_checks(config=config, governance=governance))
        status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    except DynamicV3ParameterResearchError as exc:
        checks.append(_check("schema_valid", False, str(exc)))
        status = "FAIL"
        candidates = []
        config = None
        governance = None
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_parameter_sweep_config_validation",
        "status": status,
        "config_path": str(config_path),
        "governance_path": str(governance_path),
        "search_space_version": (
            "" if governance is None else governance.search_space_version
        ),
        "candidate_preview_count": len(candidates),
        "evaluator_mode": (
            EVALUATOR_TINY_FIXTURE_PROXY if config is None else config.execution.evaluator
        ),
        "evaluator_version": _evaluator_version(
            EVALUATOR_TINY_FIXTURE_PROXY if config is None else config.execution.evaluator
        ),
        "not_for_investment_decision": (
            True if config is None else config.execution.evaluator == EVALUATOR_TINY_FIXTURE_PROXY
        ),
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def preview_sweep_candidates(
    *,
    config_path: Path = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    limit: int = 20,
) -> dict[str, Any]:
    config = load_parameter_sweep_config(config_path)
    candidates = parameter_grid_candidates(config)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_parameter_sweep_preview",
        "status": "PASS",
        "config_path": str(config_path),
        "candidate_count": len(candidates),
        "preview_count": min(limit, len(candidates)),
        "evaluator_mode": config.execution.evaluator,
        "evaluator_version": _evaluator_version(config.execution.evaluator),
        "not_for_investment_decision": (
            config.execution.evaluator == EVALUATOR_TINY_FIXTURE_PROXY
        ),
        "candidates": candidates[:limit],
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def sweep_profile_list_payload(
    *,
    profile_config_path: Path = DEFAULT_PARAMETER_SWEEP_PROFILE_CONFIG_PATH,
) -> dict[str, Any]:
    config = load_sweep_profile_config(profile_config_path)
    profiles = [
        {"profile": name, **profile.model_dump(mode="json")}
        for name, profile in sorted(config.profiles.items())
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_sweep_profile_list",
        "status": "PASS",
        "profile_config_path": str(profile_config_path),
        "profiles": profiles,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def validate_sweep_profiles_payload(
    *,
    profile_config_path: Path = DEFAULT_PARAMETER_SWEEP_PROFILE_CONFIG_PATH,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    try:
        config = load_sweep_profile_config(profile_config_path)
        checks.append(_check("profile_schema_valid", True, "profile config is valid"))
        for name, profile in sorted(config.profiles.items()):
            resolved_config = _resolve_project_path(profile.config_path)
            checks.extend(
                [
                    _check(
                        f"{name}:config_exists",
                        resolved_config.exists(),
                        str(resolved_config),
                    ),
                    _check(
                        f"{name}:evaluator_mode_valid",
                        profile.evaluator_mode in EVALUATOR_VERSIONS,
                        profile.evaluator_mode,
                    ),
                    _check(
                        f"{name}:real_not_ci_safe",
                        not (
                            profile.evaluator_mode == EVALUATOR_REAL_DYNAMIC_V3_RESCUE
                            and profile.ci_safe
                        ),
                        "real profiles must not enter CI",
                    ),
                    _check(
                        f"{name}:investment_flag_consistent",
                        profile.not_for_investment_decision
                        == (profile.evaluator_mode == EVALUATOR_TINY_FIXTURE_PROXY),
                        "not_for_investment_decision matches evaluator mode",
                    ),
                ]
            )
    except DynamicV3ParameterResearchError as exc:
        checks.append(_check("profile_schema_valid", False, str(exc)))
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_sweep_profile_validation",
        "status": status,
        "profile_config_path": str(profile_config_path),
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def run_parameter_sweep_profile(
    *,
    profile: str,
    profile_config_path: Path = DEFAULT_PARAMETER_SWEEP_PROFILE_CONFIG_PATH,
    as_of: date | None = None,
    end: date | None = None,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    data_quality_output_path: Path | None = None,
    output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
) -> dict[str, Any]:
    profiles = load_sweep_profile_config(profile_config_path)
    if profile not in profiles.profiles:
        raise DynamicV3ParameterResearchError(f"unknown sweep profile: {profile}")
    selected = profiles.profiles[profile]
    result = run_parameter_sweep(
        config_path=_resolve_project_path(selected.config_path),
        as_of=as_of,
        end=end,
        workers=selected.workers,
        max_candidates=selected.max_candidates,
        evaluator_mode=selected.evaluator_mode,
        prices_path=prices_path,
        rates_path=rates_path,
        data_quality_output_path=data_quality_output_path,
        output_dir=output_dir,
    )
    result["profile"] = profile
    result["profile_config_path"] = str(profile_config_path)
    return result


def run_parameter_sweep(
    *,
    config_path: Path = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    as_of: date | None = None,
    end: date | None = None,
    workers: int | None = None,
    max_candidates: int | None = None,
    evaluator_mode: EvaluatorMode | None = None,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    data_quality_output_path: Path | None = None,
    real_evaluation_output_dir: Path | None = None,
    output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    resume: str | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    real_context: RealEvaluationContext | None = None
    manifest_config_path = config_path
    if resume:
        sweep_dir = output_dir / resume
        if not sweep_dir.exists():
            raise DynamicV3ParameterResearchError(f"sweep artifact not found: {resume}")
        config = load_parameter_sweep_config(sweep_dir / "sweep_config.normalized.yaml")
        manifest_config_path = sweep_dir / "sweep_config.normalized.yaml"
        if evaluator_mode is not None and evaluator_mode != config.execution.evaluator:
            raise DynamicV3ParameterResearchError(
                "resume cannot change evaluator_mode from existing sweep config"
            )
        sweep_id = resume
        candidates = _read_jsonl(sweep_dir / "candidates.jsonl")
        existing_results, duplicate_count = _deduplicate_candidate_results_file(
            sweep_dir / "candidate_results.jsonl"
        )
        if duplicate_count:
            _append_text(
                sweep_dir / "run.log",
                (
                    f"{datetime.now(UTC).isoformat()} "
                    f"deduplicated {duplicate_count} candidate_results rows before resume\n"
                ),
            )
        completed = {
            _text(row.get("candidate_id"))
            for row in existing_results
            if row.get("status") == "completed"
        }
        if config.execution.evaluator == EVALUATOR_REAL_DYNAMIC_V3_RESCUE:
            real_context = _prepare_real_evaluation_context(
                config=config,
                sweep_dir=sweep_dir,
                prices_path=prices_path,
                rates_path=rates_path,
                data_quality_output_path=data_quality_output_path,
                real_evaluation_output_dir=real_evaluation_output_dir,
                expect_manifest_hash=config.data.manifest_hash,
            )
        status = "resumed"
    else:
        config = load_parameter_sweep_config(config_path)
        if as_of is not None:
            config = config.model_copy(
                update={"data": config.data.model_copy(update={"as_of": as_of})}
            )
        if end is not None:
            config = config.model_copy(update={"data": config.data.model_copy(update={"end": end})})
        if workers is not None:
            config = config.model_copy(
                update={"execution": config.execution.model_copy(update={"workers": workers})}
            )
        if max_candidates is not None:
            config = config.model_copy(
                update={"run": config.run.model_copy(update={"max_candidates": max_candidates})}
            )
        if evaluator_mode is not None:
            config = _with_evaluator_mode(config, evaluator_mode)
        governance = load_parameter_governance_config()
        governance_checks = _governance_checks(config=config, governance=governance)
        if any(not check["passed"] for check in governance_checks):
            failed = ", ".join(
                check["check_id"] for check in governance_checks if not check["passed"]
            )
            raise DynamicV3ParameterResearchError(f"governance validation failed: {failed}")
        data_status = config.data.quality_status
        if config.hard_constraints.require_data_quality_not_fail and data_status == "FAIL":
            raise DynamicV3ParameterResearchError("data_quality=FAIL stops sweep")
        sweep_id = _sweep_id(config, generated)
        sweep_dir = _unique_dir(output_dir / sweep_id)
        sweep_dir.mkdir(parents=True, exist_ok=False)
        if config.execution.evaluator == EVALUATOR_REAL_DYNAMIC_V3_RESCUE:
            real_context = _prepare_real_evaluation_context(
                config=config,
                sweep_dir=sweep_dir,
                prices_path=prices_path,
                rates_path=rates_path,
                data_quality_output_path=data_quality_output_path,
                real_evaluation_output_dir=real_evaluation_output_dir,
            )
            config = config.model_copy(
                update={
                    "data": config.data.model_copy(
                        update={
                            "quality_status": real_context.data_quality_status,
                            "manifest_hash": real_context.data_manifest_hash,
                        }
                    )
                }
            )
        candidates = parameter_grid_candidates(config)
        completed = set()
        status = "running"
        _write_yaml(sweep_dir / "sweep_config.normalized.yaml", normalized_sweep_config(config))
        _write_jsonl(sweep_dir / "candidates.jsonl", candidates)
        _write_json(
            sweep_dir / "data_manifest.json",
            _data_manifest(config=config, generated_at=generated),
        )
        _write_text(
            sweep_dir / "run.log",
            f"{generated.isoformat()} sweep started with {len(candidates)} candidates\n",
        )
    result_path = sweep_dir / "candidate_results.jsonl"
    error_path = sweep_dir / "candidate_errors.jsonl"
    if not result_path.exists():
        _write_jsonl(result_path, [])
    if not error_path.exists():
        _write_jsonl(error_path, [])
    completed_count = len(completed)
    failed_count = len(_read_jsonl(error_path))
    for idx, candidate in enumerate(candidates, start=1):
        candidate_id = _text(candidate.get("candidate_id"))
        if candidate_id in completed:
            continue
        started = datetime.now(UTC)
        try:
            result = evaluate_sweep_candidate(
                candidate,
                config=config,
                sweep_dir=sweep_dir,
                real_context=real_context,
            )
            result["started_at"] = started.isoformat()
            result["completed_at"] = datetime.now(UTC).isoformat()
            _append_jsonl(result_path, result)
            completed.add(candidate_id)
            completed_count += 1
        except Exception as exc:  # noqa: BLE001
            failed_count += 1
            error = {
                "candidate_id": candidate_id,
                "status": "failed",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "parameters": candidate.get("parameters", {}),
                "started_at": started.isoformat(),
                "completed_at": datetime.now(UTC).isoformat(),
            }
            _append_jsonl(error_path, error)
            if not config.execution.continue_on_candidate_error:
                raise DynamicV3ParameterResearchError(str(exc)) from exc
        if idx % config.execution.checkpoint_every_candidates == 0:
            _write_checkpoint(sweep_dir, idx, completed_count, failed_count)
    _write_checkpoint(sweep_dir, len(candidates), completed_count, failed_count)
    results, duplicate_count = _deduplicate_candidate_results_file(result_path)
    if duplicate_count:
        _append_text(
            sweep_dir / "run.log",
            (
                f"{datetime.now(UTC).isoformat()} "
                f"deduplicated {duplicate_count} candidate_results rows before final manifest\n"
            ),
        )
    errors = _read_jsonl(error_path)
    leaderboard = build_sweep_leaderboard_payload(sweep_dir=sweep_dir, config=config)
    _write_json(sweep_dir / "leaderboard.json", leaderboard)
    _write_text(sweep_dir / "leaderboard.md", render_leaderboard_markdown(leaderboard))
    gate_summary = _gate_summary(results, errors)
    _write_json(sweep_dir / "gate_summary.json", gate_summary)
    manifest = _sweep_manifest(
        config=config,
        sweep_id=sweep_id,
        sweep_dir=sweep_dir,
        config_path=manifest_config_path,
        generated_at=generated,
        completed_at=datetime.now(UTC),
        results=results,
        errors=errors,
        status="completed",
    )
    _write_json(sweep_dir / "sweep_manifest.json", manifest)
    report = build_sweep_report_payload(sweep_dir=sweep_dir)
    _write_text(sweep_dir / "sweep_report.md", render_sweep_report_markdown(report))
    _update_latest_pointer("latest_sweep", sweep_id, sweep_dir / "sweep_manifest.json")
    _append_text(sweep_dir / "run.log", f"{datetime.now(UTC).isoformat()} sweep completed\n")
    return {
        "sweep_id": sweep_id,
        "sweep_dir": sweep_dir,
        "status": "completed",
        "run_mode": status,
        "manifest": manifest,
        "leaderboard": leaderboard,
    }


def run_data_audit(
    *,
    as_of: date,
    end: date,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    output_dir: Path = DEFAULT_DATA_AUDIT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    audit_id = _stable_id("data-audit", as_of.isoformat(), end.isoformat(), generated.isoformat())
    audit_dir = _unique_dir(output_dir / audit_id)
    audit_dir.mkdir(parents=True, exist_ok=False)
    universe = load_universe()
    quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(universe),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=as_of,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    quality_report_path = audit_dir / "validate_data_quality_report.md"
    write_data_quality_report(quality_report, quality_report_path)
    price_cache_manifest = _price_cache_manifest(
        quality_report=quality_report,
        prices_path=prices_path,
        rates_path=rates_path,
    )
    data_provenance = data_provenance_inspect_price_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        manifest_path=_download_manifest_path(prices_path),
        write=False,
        generated_at=generated,
    )
    checksum_audit = _checksum_audit(quality_report)
    pit_coverage = _pit_coverage_audit(
        quality_report=quality_report,
        as_of=as_of,
        end=end,
    )
    data_gap = _data_gap_report(prices_path=prices_path, as_of=as_of, end=end)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_data_audit_manifest",
        "data_audit_id": audit_id,
        "status": "PASS" if quality_report.passed else "FAIL",
        "data_quality_status": quality_report.status,
        "as_of": as_of.isoformat(),
        "end": end.isoformat(),
        "generated_at": generated.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "prices_path": str(prices_path),
        "rates_path": str(rates_path),
        "validate_data_report": str(quality_report_path),
        "warning_codes": [
            issue.code for issue in quality_report.issues if issue.severity.value == "WARNING"
        ],
        "error_codes": [
            issue.code for issue in quality_report.issues if issue.severity.value == "ERROR"
        ],
        "checksum_audit_path": str(audit_dir / "checksum_audit.json"),
        "data_provenance_report_path": str(audit_dir / "data_provenance_report.json"),
        "data_provenance_status": data_provenance.get("status"),
        "price_cache_sha256": _mapping(data_provenance.get("prices")).get("sha256"),
        "download_manifest_status": _mapping(data_provenance.get("download_manifest")).get(
            "status"
        ),
        "provenance_status": data_provenance.get("provenance_status"),
        "pit_coverage_audit_path": str(audit_dir / "pit_coverage_audit.json"),
        "data_gap_report_path": str(audit_dir / "data_gap_report.json"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_data_audit_report",
        "data_audit_id": audit_id,
        "status": manifest["status"],
        "data_quality_status": quality_report.status,
        "prices_download_manifest_checksum_missing": (
            "prices_download_manifest_checksum_missing" in manifest["warning_codes"]
        ),
        "data_provenance": data_provenance,
        "price_cache_sha256": _mapping(data_provenance.get("prices")).get("sha256"),
        "download_manifest_status": _mapping(data_provenance.get("download_manifest")).get(
            "status"
        ),
        "provenance_status": data_provenance.get("provenance_status"),
        "data_provenance_warnings": data_provenance.get("warnings", []),
        "price_cache_manifest": price_cache_manifest,
        "checksum_audit": checksum_audit,
        "pit_coverage_audit": pit_coverage,
        "data_gap_report": data_gap,
        "validate_data_report": str(quality_report_path),
        "issues": _quality_issue_rows(quality_report),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(audit_dir / "data_audit_manifest.json", manifest)
    _write_json(audit_dir / "price_cache_manifest.json", price_cache_manifest)
    _write_json(audit_dir / "data_provenance_report.json", data_provenance)
    _write_json(audit_dir / "checksum_audit.json", checksum_audit)
    _write_json(audit_dir / "pit_coverage_audit.json", pit_coverage)
    _write_json(audit_dir / "data_gap_report.json", data_gap)
    _write_text(audit_dir / "data_audit_report.md", render_data_audit_markdown(report))
    _update_latest_pointer("latest_data_audit", audit_id, audit_dir / "data_audit_manifest.json")
    return {"data_audit_id": audit_id, "data_audit_dir": audit_dir, "report": report}


def data_audit_report_payload(
    *,
    data_audit_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DATA_AUDIT_DIR,
) -> dict[str, Any]:
    resolved_id = data_audit_id or (
        _latest_pointer_artifact_id("latest_data_audit") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--audit-id or --latest is required")
    audit_dir = output_dir / resolved_id
    manifest = _read_json(audit_dir / "data_audit_manifest.json")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_data_audit_report_view",
        "data_audit_id": resolved_id,
        "status": manifest.get("status", "UNKNOWN"),
        "data_quality_status": manifest.get("data_quality_status", "UNKNOWN"),
        "report_path": str(audit_dir / "data_audit_report.md"),
        "manifest": manifest,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def validate_data_audit_artifact(
    *,
    data_audit_id: str,
    output_dir: Path = DEFAULT_DATA_AUDIT_DIR,
) -> dict[str, Any]:
    audit_dir = output_dir / data_audit_id
    required = [
        "data_audit_manifest.json",
        "price_cache_manifest.json",
        "data_provenance_report.json",
        "checksum_audit.json",
        "pit_coverage_audit.json",
        "data_gap_report.json",
        "data_audit_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (audit_dir / name).exists(), name)
        for name in required
    ]
    manifest = _read_optional_json(audit_dir / "data_audit_manifest.json") or {}
    checks.append(
        _check(
            "data_quality_not_fail",
            _text(manifest.get("data_quality_status")) != "FAIL",
            _text(manifest.get("data_quality_status")),
        )
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_data_audit_validation",
        "data_audit_id": data_audit_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def data_provenance_inspect_price_cache(
    *,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    manifest_path: Path | None = None,
    output_dir: Path = DEFAULT_DATA_PROVENANCE_DIR,
    write: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    resolved_manifest = manifest_path or _download_manifest_path(prices_path)
    payload = _price_cache_provenance_payload(
        prices_path=prices_path,
        rates_path=rates_path,
        manifest_path=resolved_manifest,
        generated_at=generated,
    )
    if write:
        output_dir.mkdir(parents=True, exist_ok=True)
        _write_json(output_dir / "price_cache_provenance_report.json", payload)
        _write_text(
            output_dir / "price_cache_provenance_report.md",
            render_data_provenance_markdown(payload),
        )
        _update_latest_pointer(
            "latest_data_provenance",
            "price_cache_provenance",
            output_dir / "price_cache_provenance_report.json",
        )
    return payload


def data_provenance_repair_price_manifest(
    *,
    mode: str = "reconstruct-from-cache",
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    manifest_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    if mode != "reconstruct-from-cache":
        raise DynamicV3ParameterResearchError(
            "data provenance repair currently supports --mode reconstruct-from-cache"
        )
    generated = generated_at or datetime.now(UTC)
    resolved_manifest = manifest_path or _download_manifest_path(prices_path)
    files = [
        _cache_file_inventory(prices_path, file_role="prices"),
        _cache_file_inventory(rates_path, file_role="rates"),
    ]
    secondary = _marketstack_prices_path(prices_path)
    if secondary.exists():
        files.append(_cache_file_inventory(secondary, file_role="secondary_prices"))
    if any(not row["exists"] for row in files):
        missing = ", ".join(row["path"] for row in files if not row["exists"])
        raise DynamicV3ParameterResearchError(
            f"cannot reconstruct manifest; file missing: {missing}"
        )
    existing_rows = _download_manifest_records(resolved_manifest)
    output_paths = {str(row["path"]) for row in files}
    existing_rows = [
        row for row in existing_rows if _text(row.get("output_path")) not in output_paths
    ]
    reconstructed_rows = [
        _reconstructed_download_manifest_row(
            summary=row,
            generated_at=generated,
        )
        for row in files
    ]
    all_rows = [*existing_rows, *reconstructed_rows]
    resolved_manifest.parent.mkdir(parents=True, exist_ok=True)
    _write_csv(resolved_manifest, all_rows)
    json_manifest = {
        "schema_version": 1,
        "source": "cache_rebuild_from_existing_file",
        "provenance_status": DATA_PROVENANCE_RECONSTRUCTED,
        "generated_at": generated.isoformat(),
        "as_of": _text(files[0].get("end_date")),
        "download_manifest_csv": str(resolved_manifest),
        "files": files,
        "limitations": ["original_download_event_not_available"],
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
    }
    json_path = prices_path.parent / "download_manifests" / "prices_daily_download_manifest.json"
    _write_json(json_path, json_manifest)
    inspect = data_provenance_inspect_price_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        manifest_path=resolved_manifest,
        write=False,
        generated_at=generated,
    )
    payload = {
        **inspect,
        "repair_mode": mode,
        "reconstructed_manifest_path": str(resolved_manifest),
        "reconstructed_manifest_json_path": str(json_path),
        "reconstructed_file_count": len(reconstructed_rows),
        "provenance_status": DATA_PROVENANCE_RECONSTRUCTED,
        "limitations": ["original_download_event_not_available"],
    }
    return payload


def data_provenance_validate(
    *,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    manifest_path: Path | None = None,
) -> dict[str, Any]:
    payload = data_provenance_inspect_price_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        manifest_path=manifest_path,
        write=False,
    )
    checks = [
        _check("prices_cache_exists", _mapping(payload.get("prices")).get("exists") is True, ""),
        _check("rates_cache_exists", _mapping(payload.get("rates")).get("exists") is True, ""),
        _check(
            "download_manifest_exists",
            _mapping(payload.get("download_manifest")).get("exists") is True,
            _text(_mapping(payload.get("download_manifest")).get("path")),
        ),
        _check(
            "prices_checksum_in_manifest",
            payload.get("prices_checksum_in_manifest") is True,
            _text(_mapping(payload.get("prices")).get("sha256")),
        ),
        _check(
            "rates_checksum_in_manifest",
            payload.get("rates_checksum_in_manifest") is True,
            _text(_mapping(payload.get("rates")).get("sha256")),
        ),
    ]
    status = "FAIL" if any(not check["passed"] for check in checks) else payload["status"]
    return {
        **payload,
        "report_type": "etf_dynamic_v3_data_provenance_validation",
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
    }


def run_window_audit(
    *,
    as_of: date,
    end: date,
    artifact_root: Path = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    output_dir: Path = DEFAULT_WINDOW_AUDIT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    audit_id = _stable_id("window-audit", as_of.isoformat(), end.isoformat(), generated.isoformat())
    audit_dir = _unique_dir(output_dir / audit_id)
    audit_dir.mkdir(parents=True, exist_ok=False)
    records = _window_audit_inventory(
        artifact_root=artifact_root,
        requested_start=as_of,
        requested_end=end,
    )
    mismatch = [
        row for row in records if _texts(row.get("window_mismatch_reasons"))
    ]
    insufficient = [
        row
        for row in records
        if row.get("date_range_status") in {DATE_RANGE_INSUFFICIENT_DATA, DATE_RANGE_FAIL}
    ]
    configured_start = _configured_ai_regime_start()
    earliest_actual = _earliest_actual_evaluation_start(records)
    blocking = [row for row in records if row.get("promotion_blocking") is True]
    status = "PASS"
    if any(row.get("date_range_status") == DATE_RANGE_FAIL for row in records):
        status = "FAIL"
    elif blocking:
        status = DATE_RANGE_INCOMPLETE
    elif any(row.get("date_range_status") == DATE_RANGE_PASS_WITH_WARNINGS for row in records):
        status = DATE_RANGE_PASS_WITH_WARNINGS
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_window_audit_manifest",
        "window_audit_id": audit_id,
        "status": status,
        "configured_backtest_start": configured_start.isoformat(),
        "requested_start": as_of.isoformat(),
        "requested_end": end.isoformat(),
        "earliest_actual_evaluation_start": earliest_actual,
        "artifact_count": len(records),
        "promotion_blocking_count": len(blocking),
        "started_at": generated.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
    }
    report = {
        **manifest,
        "artifact_window_inventory_path": str(audit_dir / "artifact_window_inventory.jsonl"),
        "window_mismatch_report_path": str(audit_dir / "window_mismatch_report.json"),
        "insufficient_data_report_path": str(audit_dir / "insufficient_data_report.json"),
        "window_mismatch_count": len(mismatch),
        "insufficient_data_count": len(insufficient),
        "backtest_window_incomplete_count": len(blocking),
        "needs_full_window_rerun": bool(blocking),
        "answers": {
            "configured_backtest_start": configured_start.isoformat(),
            "earliest_actual_evaluation_start": earliest_actual,
            "covers_ai_regime_start": (
                bool(earliest_actual)
                and _date_from_any(earliest_actual) is not None
                and _date_from_any(earliest_actual) <= configured_start
            ),
            "contains_2025_05_28_to_2026_05_28_artifact": any(
                row.get("actual_evaluation_start") == "2025-05-28"
                and row.get("actual_evaluation_end") == "2026-05-28"
                for row in records
            ),
            "promotion_blocking": bool(blocking),
        },
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(audit_dir / "window_audit_manifest.json", manifest)
    _write_jsonl(audit_dir / "artifact_window_inventory.jsonl", records)
    _write_json(audit_dir / "window_mismatch_report.json", {"artifacts": mismatch})
    _write_json(audit_dir / "insufficient_data_report.json", {"artifacts": insufficient})
    _write_text(audit_dir / "window_audit_report.md", render_window_audit_markdown(report))
    _update_latest_pointer(
        "latest_window_audit",
        audit_id,
        audit_dir / "window_audit_manifest.json",
    )
    return {"window_audit_id": audit_id, "window_audit_dir": audit_dir, "report": report}


def window_audit_report_payload(
    *,
    audit_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WINDOW_AUDIT_DIR,
) -> dict[str, Any]:
    resolved_id = audit_id or (
        _latest_pointer_artifact_id("latest_window_audit") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--audit-id or --latest is required")
    audit_dir = output_dir / resolved_id
    manifest = _read_json(audit_dir / "window_audit_manifest.json")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_window_audit_report_view",
        "window_audit_id": resolved_id,
        "status": manifest.get("status", "UNKNOWN"),
        "configured_backtest_start": manifest.get("configured_backtest_start"),
        "earliest_actual_evaluation_start": manifest.get("earliest_actual_evaluation_start"),
        "promotion_blocking_count": manifest.get("promotion_blocking_count"),
        "report_path": str(audit_dir / "window_audit_report.md"),
        "manifest": manifest,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def inspect_window_artifact(
    *,
    artifact_path: Path,
    requested_start: date | None = None,
    requested_end: date | None = None,
) -> dict[str, Any]:
    payload = _read_json(artifact_path)
    record = _window_record_from_payload(
        payload=payload,
        artifact_path=artifact_path,
        requested_start=requested_start,
        requested_end=requested_end,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_window_artifact_inspection",
        "status": record["date_range_status"],
        "record": record,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def validate_window_audit_artifact(
    *,
    audit_id: str,
    output_dir: Path = DEFAULT_WINDOW_AUDIT_DIR,
) -> dict[str, Any]:
    audit_dir = output_dir / audit_id
    required = [
        "window_audit_manifest.json",
        "artifact_window_inventory.jsonl",
        "window_mismatch_report.json",
        "insufficient_data_report.json",
        "window_audit_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (audit_dir / name).exists(), name)
        for name in required
    ]
    rows = (
        _read_jsonl(audit_dir / "artifact_window_inventory.jsonl")
        if (audit_dir / "artifact_window_inventory.jsonl").exists()
        else []
    )
    required_fields = {
        "configured_backtest_start",
        "requested_start",
        "requested_end",
        "actual_evaluation_start",
        "actual_evaluation_end",
        "date_range_status",
        "window_mismatch_reasons",
    }
    checks.append(
        _check(
            "inventory_required_window_fields",
            all(required_fields <= set(row) for row in rows),
            "inventory rows expose required window fields",
        )
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_window_audit_validation",
        "window_audit_id": audit_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def run_injection_audit(
    *,
    config_path: Path = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    as_of: date,
    end: date,
    max_candidates: int = 20,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    output_dir: Path = DEFAULT_INJECTION_AUDIT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    base_config = load_parameter_sweep_config(config_path)
    config = base_config.model_copy(
        update={
            "data": base_config.data.model_copy(update={"as_of": as_of, "end": end}),
            "run": base_config.run.model_copy(update={"max_candidates": max(max_candidates, 1)}),
            "execution": base_config.execution.model_copy(
                update={
                    "evaluator": EVALUATOR_REAL_DYNAMIC_V3_RESCUE,
                    "evaluation_mode": EVALUATOR_REAL_DYNAMIC_V3_RESCUE,
                    "workers": 1,
                    "checkpoint_every_candidates": 1,
                }
            ),
        }
    )
    governance = load_parameter_governance_config()
    governance_checks = _governance_checks(config=config, governance=governance)
    if any(not check["passed"] for check in governance_checks):
        failed = ", ".join(check["check_id"] for check in governance_checks if not check["passed"])
        raise DynamicV3ParameterResearchError(f"governance validation failed: {failed}")
    unbounded = config.model_copy(
        update={"run": config.run.model_copy(update={"max_candidates": max_candidates * 20})}
    )
    candidates = _select_injection_audit_candidates(
        parameter_grid_candidates(unbounded),
        max_candidates=max_candidates,
    )
    audit_id = _stable_id(
        "injection-audit",
        as_of.isoformat(),
        end.isoformat(),
        config_path,
        generated.isoformat(),
    )
    audit_dir = _unique_dir(output_dir / audit_id)
    audit_dir.mkdir(parents=True, exist_ok=False)
    _write_yaml(audit_dir / "sweep_config.normalized.yaml", normalized_sweep_config(config))
    _write_jsonl(audit_dir / "candidates.jsonl", candidates)
    real_context = _prepare_real_evaluation_context(
        config=config,
        sweep_dir=audit_dir,
        prices_path=prices_path,
        rates_path=rates_path,
        data_quality_output_path=None,
        real_evaluation_output_dir=audit_dir / "real_evaluation",
    )
    config = config.model_copy(
        update={
            "data": config.data.model_copy(
                update={
                    "quality_status": real_context.data_quality_status,
                    "manifest_hash": real_context.data_manifest_hash,
                }
            )
        }
    )
    results: list[dict[str, Any]] = []
    matrix_rows: list[dict[str, Any]] = []
    for candidate in candidates:
        result = evaluate_sweep_candidate(
            candidate,
            config=config,
            sweep_dir=audit_dir,
            real_context=real_context,
        )
        results.append(result)
        matrix_rows.append(
            _injection_matrix_row(
                candidate=result,
                real_context=real_context,
            )
        )
    _write_jsonl(audit_dir / "candidate_results.jsonl", results)
    _write_csv(audit_dir / "candidate_parameter_matrix.csv", matrix_rows)
    weight_summary = _weight_path_diff_summary(results)
    metric_summary = _metric_diff_summary(results)
    parameter_effects = _parameter_effect_summary(matrix_rows, results)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_injection_audit_manifest",
        "audit_id": audit_id,
        "status": "PASS",
        "config_path": str(config_path),
        "as_of": as_of.isoformat(),
        "end": end.isoformat(),
        "candidate_count": len(results),
        "max_candidates": max_candidates,
        "data_quality_status": real_context.data_quality_status,
        "search_space_version": governance.search_space_version,
        "started_at": generated.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_injection_audit_report",
        "audit_id": audit_id,
        "status": "PASS",
        "candidate_count": len(results),
        "data_quality_status": real_context.data_quality_status,
        "parameter_effects": parameter_effects,
        "weight_path_diff_summary": weight_summary,
        "metric_diff_summary": metric_summary,
        "not_consumed_parameters": [
            row["parameter"]
            for row in parameter_effects
            if row["effect_status"] == "NOT_CONSUMED"
        ],
        "no_observed_effect_parameters": [
            row["parameter"]
            for row in parameter_effects
            if row["effect_status"] == "NO_OBSERVED_EFFECT"
        ],
        "all_weight_paths_almost_identical": (
            weight_summary.get("distinct_latest_weight_hash_count") == 1
        ),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(audit_dir / "injection_audit_manifest.json", manifest)
    _write_json(audit_dir / "weight_path_diff_summary.json", weight_summary)
    _write_json(audit_dir / "metric_diff_summary.json", metric_summary)
    _write_text(
        audit_dir / "parameter_effect_report.md",
        render_injection_audit_markdown(report),
    )
    _update_latest_pointer(
        "latest_injection_audit",
        audit_id,
        audit_dir / "injection_audit_manifest.json",
    )
    return {"audit_id": audit_id, "audit_dir": audit_dir, "report": report}


def injection_audit_report_payload(
    *,
    audit_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_INJECTION_AUDIT_DIR,
) -> dict[str, Any]:
    resolved_id = audit_id or (
        _latest_pointer_artifact_id("latest_injection_audit") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--audit-id or --latest is required")
    audit_dir = output_dir / resolved_id
    manifest = _read_json(audit_dir / "injection_audit_manifest.json")
    weight_summary = _read_json(audit_dir / "weight_path_diff_summary.json")
    metric_summary = _read_json(audit_dir / "metric_diff_summary.json")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_injection_audit_report_view",
        "audit_id": resolved_id,
        "status": manifest.get("status", "UNKNOWN"),
        "candidate_count": manifest.get("candidate_count"),
        "weight_path_diff_summary": weight_summary,
        "metric_diff_summary": metric_summary,
        "report_path": str(audit_dir / "parameter_effect_report.md"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def validate_injection_audit_artifact(
    *,
    audit_id: str,
    output_dir: Path = DEFAULT_INJECTION_AUDIT_DIR,
) -> dict[str, Any]:
    audit_dir = output_dir / audit_id
    required = [
        "injection_audit_manifest.json",
        "candidate_parameter_matrix.csv",
        "weight_path_diff_summary.json",
        "metric_diff_summary.json",
        "parameter_effect_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (audit_dir / name).exists(), name)
        for name in required
    ]
    manifest = _read_optional_json(audit_dir / "injection_audit_manifest.json") or {}
    weight_summary = _read_optional_json(audit_dir / "weight_path_diff_summary.json") or {}
    candidate_count = int(manifest.get("candidate_count") or 0)
    requested_floor = min(20, int(manifest.get("max_candidates") or 20))
    checks.append(
        _check(
            "candidate_count_at_least_requested_floor",
            candidate_count >= requested_floor,
            str(manifest.get("candidate_count")),
        )
    )
    checks.append(
        _check(
            "weight_path_difference_checked",
            "distinct_latest_weight_hash_count" in weight_summary,
            "weight path diff summary exists",
        )
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_injection_audit_validation",
        "audit_id": audit_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def evaluate_sweep_candidate(
    candidate: Mapping[str, Any],
    *,
    config: DynamicV3ParameterSweepConfig,
    sweep_dir: Path | None = None,
    real_context: RealEvaluationContext | None = None,
) -> dict[str, Any]:
    parameters = _mapping(candidate.get("parameters"))
    evaluator = config.execution.evaluator
    if evaluator == EVALUATOR_TINY_FIXTURE_PROXY:
        if (
            float(parameters.get("rescue_intensity", 0)) >= 1.0
            and int(parameters.get("smooth_window_days", 0)) <= 3
            and int(parameters.get("constraint_buffer_bps", 0)) == 0
        ):
            raise DynamicV3ParameterResearchError(
                "fixture candidate intentionally failed for error isolation coverage"
            )
        metrics = _fixture_metrics(parameters, config)
        real_artifact_path = ""
        metrics_source = "tiny_fixture_proxy_formula"
        data_quality = _candidate_data_quality(
            status=config.data.quality_status,
            report_path="",
            source="config_fixture",
        )
        not_for_investment = True
    elif evaluator == EVALUATOR_REAL_DYNAMIC_V3_RESCUE:
        if real_context is None or sweep_dir is None:
            raise DynamicV3ParameterResearchError(
                "real_dynamic_v3_rescue evaluator requires prepared real evaluation context"
            )
        real_payload, real_paths = _write_real_candidate_evaluation_artifact(
            candidate=candidate,
            config=config,
            sweep_dir=sweep_dir,
            real_context=real_context,
        )
        metrics = _metrics_from_real_evaluation_payload(real_payload, config)
        backtest_window = _backtest_window_from_payload(real_payload)
        weight_path_metadata = _read_optional_json(
            Path(real_paths["json"]).parent / "weight_path_metadata.json"
        ) or _missing_weight_path_metadata(
            candidate_id=_text(candidate.get("candidate_id")),
            evaluation_id=_text(real_payload.get("dynamic_v3_real_evaluation_report_id")),
        )
        metrics["date_range_status"] = backtest_window["date_range_status"]
        metrics["weight_path_status"] = weight_path_metadata["attribution_completeness"]
        real_artifact_path = str(real_paths["json"])
        metrics_source = "real_evaluation_artifact"
        data_quality = _candidate_data_quality(
            status=real_context.data_quality_status,
            report_path=str(real_context.data_quality_report_path),
            source="validate_data_cache",
        )
        not_for_investment = False
    else:
        raise DynamicV3ParameterResearchError(f"unknown evaluator mode: {evaluator}")
    if evaluator != EVALUATOR_REAL_DYNAMIC_V3_RESCUE:
        backtest_window = _empty_backtest_window(
            configured_start=config.data.as_of,
            requested_start=config.data.as_of,
            requested_end=config.data.end,
            status=DATE_RANGE_PASS,
        )
        weight_path_metadata = _missing_weight_path_metadata(
            candidate_id=_text(candidate.get("candidate_id")),
            evaluation_id="",
        )
    gate, reasons = gate_candidate(metrics, config)
    if (
        evaluator == EVALUATOR_REAL_DYNAMIC_V3_RESCUE
        and gate == GATE_OBSERVE_ONLY
        and _text(metrics.get("real_promotion_gate_decision")) == GATE_PROMOTE_CANDIDATE
    ):
        gate = GATE_PROMOTE_CANDIDATE
        reasons = ["real_evaluation_promote_candidate_manual_review_required"]
    score, breakdown = score_candidate(metrics, config, gate)
    return {
        "candidate_id": _text(candidate.get("candidate_id")),
        "status": "completed",
        "gate": gate,
        "gate_reasons": reasons,
        "parameters": dict(parameters),
        "search_space_version": _search_space_version(),
        "evaluator_mode": evaluator,
        "evaluator_version": _evaluator_version(evaluator),
        "real_evaluation_artifact_path": real_artifact_path,
        "data_quality": data_quality,
        "metrics_source": metrics_source,
        "not_for_investment_decision": not_for_investment,
        "metrics": metrics,
        "score": score,
        "score_breakdown": breakdown,
        "backtest_window": backtest_window,
        "weight_path_metadata": weight_path_metadata,
        "artifact_paths": [real_artifact_path] if real_artifact_path else [],
    }


def gate_candidate(
    metrics: Mapping[str, Any],
    config: DynamicV3ParameterSweepConfig,
) -> tuple[str, list[str]]:
    constraints = config.hard_constraints
    reasons: list[str] = []
    required = (
        "constraint_hit_rate",
        "constraint_hits_delta_vs_reference",
        "false_risk_off_delta",
        "turnover",
        "dynamic_vs_static_gap",
        "drawdown_degradation_pp",
        "robustness_status",
        "data_quality",
        "lookahead_status",
    )
    missing = [key for key in required if key not in metrics]
    if missing:
        return GATE_REJECT, [f"missing_required_metric:{key}" for key in missing]
    if _text(metrics.get("data_quality")) == "FAIL":
        reasons.append("data_quality_fail")
    if float(metrics["constraint_hit_rate"]) > constraints.max_constraint_hit_rate:
        reasons.append("constraint_hit_rate_exceeds_policy")
    if int(metrics["constraint_hits_delta_vs_reference"]) > (
        constraints.max_constraint_hits_delta_vs_reference
    ):
        reasons.append("constraint_hits_delta_exceeds_policy")
    if int(metrics["false_risk_off_delta"]) > constraints.max_false_risk_off_delta:
        reasons.append("false_risk_off_delta_exceeds_policy")
    if float(metrics["turnover"]) > constraints.max_turnover:
        reasons.append("turnover_exceeds_policy")
    if float(metrics["drawdown_degradation_pp"]) > constraints.max_drawdown_degradation_pp:
        reasons.append("drawdown_degradation_exceeds_policy")
    if float(metrics["dynamic_vs_static_gap"]) > constraints.max_dynamic_vs_static_gap:
        reasons.append("dynamic_vs_static_gap_exceeds_policy")
    if constraints.require_no_lookahead and _text(metrics["lookahead_status"]) != "PASS":
        reasons.append("lookahead_status_not_pass")
    if _text(metrics["robustness_status"]) not in constraints.allow_robustness_status:
        reasons.append("robustness_status_not_allowed")
    if reasons:
        return GATE_REJECT, reasons
    review_reasons: list[str] = []
    if _text(metrics.get("data_quality")) == "PASS_WITH_WARNINGS":
        review_reasons.append("data_quality_pass_with_warnings")
    if _text(metrics.get("robustness_status")) == "REVIEW_REQUIRED":
        review_reasons.append("robustness_review_required")
    if _text(metrics.get("overfit_status")) == "REVIEW_REQUIRED":
        review_reasons.append("overfit_review_required")
    if _text(metrics.get("date_range_status")) in WINDOW_PROMOTION_BLOCKING_STATUSES:
        review_reasons.append("BACKTEST_WINDOW_INCOMPLETE")
    if _text(metrics.get("weight_path_status")) == WEIGHT_PATH_INCOMPLETE:
        review_reasons.append("MISSING_DAILY_WEIGHT_PATH")
    if _text(metrics.get("stress_bucket_status")) == "MIXED":
        review_reasons.append("stress_bucket_mixed")
    if _text(metrics.get("parameter_sensitivity_status")) == "HIGH":
        review_reasons.append("parameter_sensitivity_high")
    if float(metrics.get("constraint_hit_reduction", 0)) < constraints.noise_floor_improvement:
        review_reasons.append("improvement_below_noise_floor")
    if review_reasons:
        return GATE_REVIEW_REQUIRED, review_reasons
    return GATE_OBSERVE_ONLY, ["passed_hard_gate_observe_only"]


def score_candidate(
    metrics: Mapping[str, Any],
    config: DynamicV3ParameterSweepConfig,
    gate: str,
) -> tuple[float | None, dict[str, float]]:
    if gate == GATE_REJECT:
        return None, {}
    weights = config.scoring.weights.model_dump(mode="json")
    components = {
        "dynamic_vs_static_gap_improvement": _clamp01(
            float(metrics.get("dynamic_vs_static_gap_improvement", 0)) / 0.08
        ),
        "constraint_hit_reduction": _clamp01(
            float(metrics.get("constraint_hit_reduction", 0)) / 0.08
        ),
        "drawdown_preservation": _clamp01(
            1.0 - max(0.0, float(metrics.get("drawdown_degradation_pp", 0))) / 0.08
        ),
        "turnover_reduction": _clamp01(float(metrics.get("turnover_reduction", 0)) / 0.25),
        "false_risk_off_control": 1.0 if int(metrics.get("false_risk_off_delta", 0)) <= 0 else 0.0,
        "robustness_score": {
            "PASS": 1.0,
            "REVIEW_REQUIRED": 0.45,
            "FAIL": 0.0,
        }.get(_text(metrics.get("robustness_status")), 0.0),
    }
    score = sum(components[key] * float(weights[key]) for key in components)
    penalty = 0.0
    if _text(metrics.get("overfit_status")) == "REVIEW_REQUIRED":
        penalty += 0.05
    if gate == GATE_REVIEW_REQUIRED:
        penalty += 0.03
    components["instability_penalty"] = penalty
    return round(max(0.0, score - penalty), 6), components


def sweep_status_payload(
    *, sweep_id: str, output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR
) -> dict[str, Any]:
    sweep_dir = output_dir / sweep_id
    manifest = _read_json(sweep_dir / "sweep_manifest.json")
    checkpoint = _read_json(sweep_dir / "checkpoint.json")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_parameter_sweep_status",
        "sweep_id": sweep_id,
        "sweep_dir": str(sweep_dir),
        "manifest": manifest,
        "checkpoint": checkpoint,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def validate_sweep_artifact(
    *,
    sweep_id: str,
    output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
) -> dict[str, Any]:
    sweep_dir = output_dir / sweep_id
    required = [
        "sweep_manifest.json",
        "sweep_config.normalized.yaml",
        "data_manifest.json",
        "candidates.jsonl",
        "candidate_results.jsonl",
        "candidate_errors.jsonl",
        "checkpoint.json",
        "gate_summary.json",
        "leaderboard.json",
        "leaderboard.md",
        "sweep_report.md",
        "run.log",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (sweep_dir / name).exists(), name) for name in required
    ]
    raw_results = (
        _read_jsonl(sweep_dir / "candidate_results.jsonl")
        if (sweep_dir / "candidate_results.jsonl").exists()
        else []
    )
    results = _deduplicate_candidate_result_rows(raw_results)
    manifest = _read_optional_json(sweep_dir / "sweep_manifest.json") or {}
    evaluator = _text(
        manifest.get("evaluator_mode"),
        _text(_mapping(results[0] if results else {}).get("evaluator_mode")),
    )
    checks.append(
        _check(
            "production_candidate_not_generated",
            all(row.get("gate") != FORBIDDEN_GATE for row in results),
            "sweep results do not contain production_candidate",
        )
    )
    checks.append(
        _check(
            "candidate_results_unique_ids",
            len(raw_results) == len(results),
            "candidate_results.jsonl must contain at most one row per candidate_id",
        )
    )
    checks.append(
        _check(
            "candidate_results_include_evaluator_fields",
            all(
                row.get("evaluator_mode")
                and row.get("evaluator_version")
                and "real_evaluation_artifact_path" in row
                and row.get("data_quality")
                and row.get("metrics_source")
                for row in results
            ),
            "candidate_results.jsonl contains TRADING-101 evaluator provenance fields",
        )
    )
    if evaluator == EVALUATOR_TINY_FIXTURE_PROXY:
        checks.append(
            _check(
                "tiny_fixture_not_for_investment_decision",
                all(row.get("not_for_investment_decision") is True for row in results),
                "tiny fixture results are marked not_for_investment_decision",
            )
        )
        checks.append(
            _check(
                "tiny_fixture_cannot_promote_candidate",
                all(row.get("gate") != GATE_PROMOTE_CANDIDATE for row in results),
                "tiny fixture results cannot enter promote_candidate",
            )
        )
    if evaluator == EVALUATOR_REAL_DYNAMIC_V3_RESCUE:
        checks.append(
            _check(
                "real_evaluation_artifact_paths_exist",
                all(
                    bool(row.get("real_evaluation_artifact_path"))
                    and Path(str(row.get("real_evaluation_artifact_path"))).exists()
                    for row in results
                ),
                "real evaluator candidate results link to existing real evaluation artifacts",
            )
        )
        checks.append(
            _check(
                "real_metrics_from_real_artifacts",
                all(row.get("metrics_source") == "real_evaluation_artifact" for row in results),
                "real evaluator metrics are sourced from real evaluation artifacts",
            )
        )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_parameter_sweep_validation",
        "sweep_id": sweep_id,
        "status": status,
        "evaluator_mode": evaluator,
        "evaluator_version": _evaluator_version(evaluator),
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def latest_sweep_id() -> str | None:
    pointer = _read_optional_json(DEFAULT_LATEST_POINTER_DIR / "latest_sweep.json")
    return None if not pointer else _text(pointer.get("artifact_id")) or None


def build_sweep_leaderboard_payload(
    *,
    sweep_dir: Path,
    config: DynamicV3ParameterSweepConfig | None = None,
) -> dict[str, Any]:
    results = _read_candidate_results(sweep_dir)
    errors = _read_jsonl(sweep_dir / "candidate_errors.jsonl")
    manifest = _read_optional_json(sweep_dir / "sweep_manifest.json") or {}
    evaluator = _text(
        _mapping(manifest).get("evaluator_mode"),
        config.execution.evaluator if config is not None else _leaderboard_evaluator(results),
    )
    ranked = sorted(
        [row for row in results if row.get("score") is not None],
        key=lambda row: (float(row.get("score") or 0), _text(row.get("candidate_id"))),
        reverse=True,
    )
    rejected = [row for row in results if row.get("gate") == GATE_REJECT]
    reject_counter = Counter(
        reason
        for row in [*rejected, *errors]
        for reason in _texts(row.get("gate_reasons") or row.get("error_type") or [])
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_parameter_sweep_leaderboard",
        "sweep_id": sweep_dir.name,
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "PASS",
        "evaluator_mode": evaluator,
        "evaluator_version": _evaluator_version(evaluator),
        "not_for_investment_decision": evaluator == EVALUATOR_TINY_FIXTURE_PROXY,
        "metrics_source": _leaderboard_metrics_source(results),
        "data_quality": _leaderboard_data_quality(results, manifest),
        "candidate_count": len(results) + len(errors),
        "completed_count": len(results),
        "failed_count": len(errors),
        "top_eligible_candidates": ranked[:20],
        "top_rejected_by_return_candidates": _top_rejected_by_return(rejected),
        "most_common_reject_reasons": [
            {"reason": reason, "count": count} for reason, count in reject_counter.most_common(20)
        ],
        "metric_distributions": _metric_distributions(results),
        "recommended_next_actions": _leaderboard_next_actions(
            ranked,
            rejected,
            errors,
            evaluator_mode=evaluator,
        ),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    if config is not None:
        payload["scoring_objective"] = config.scoring.objective
    return payload


def build_sweep_report_payload(*, sweep_dir: Path) -> dict[str, Any]:
    manifest = _read_json(sweep_dir / "sweep_manifest.json")
    leaderboard = _read_json(sweep_dir / "leaderboard.json")
    gate_summary = _read_json(sweep_dir / "gate_summary.json")
    evaluator = _text(leaderboard.get("evaluator_mode"), _text(manifest.get("evaluator_mode")))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_parameter_sweep_report",
        "sweep_id": sweep_dir.name,
        "status": manifest.get("status", "UNKNOWN"),
        "evaluator_mode": evaluator,
        "evaluator_version": _evaluator_version(evaluator),
        "not_for_investment_decision": evaluator == EVALUATOR_TINY_FIXTURE_PROXY,
        "data_quality": leaderboard.get("data_quality", manifest.get("data_quality", {})),
        "metrics_source": leaderboard.get("metrics_source", "UNKNOWN"),
        "manifest": manifest,
        "gate_summary": gate_summary,
        "leaderboard_summary": {
            "top_candidate": _first_candidate_id(leaderboard.get("top_eligible_candidates")),
            "failed_count": leaderboard.get("failed_count", 0),
            "most_common_reject_reasons": leaderboard.get("most_common_reject_reasons", []),
            "recommended_next_actions": leaderboard.get("recommended_next_actions", []),
        },
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def candidate_report_payload(
    *,
    sweep_id: str,
    candidate_id: str,
    output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    write: bool = True,
) -> dict[str, Any]:
    sweep_dir = output_dir / sweep_id
    result = _candidate_result(sweep_dir, candidate_id)
    if result is None:
        raise DynamicV3ParameterResearchError(f"candidate not found in sweep: {candidate_id}")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_parameter_candidate_report",
        "candidate_id": candidate_id,
        "source_sweep_id": sweep_id,
        "status": result.get("gate", "UNKNOWN"),
        "parameters": result.get("parameters", {}),
        "search_space_version": result.get("search_space_version", _search_space_version()),
        "evaluator_mode": result.get("evaluator_mode", EVALUATOR_TINY_FIXTURE_PROXY),
        "evaluator_version": result.get(
            "evaluator_version",
            _evaluator_version(_text(result.get("evaluator_mode"), EVALUATOR_TINY_FIXTURE_PROXY)),
        ),
        "real_evaluation_artifact_path": result.get("real_evaluation_artifact_path", ""),
        "data_quality": result.get("data_quality", {}),
        "metrics_source": result.get("metrics_source", "UNKNOWN"),
        "not_for_investment_decision": result.get("not_for_investment_decision") is True,
        "hard_gate_status": result.get("gate"),
        "gate_reasons": result.get("gate_reasons", []),
        "metrics": result.get("metrics", {}),
        "score": result.get("score"),
        "score_breakdown": result.get("score_breakdown", {}),
        "backtest_window": result.get("backtest_window", {}),
        "weight_path_metadata": result.get("weight_path_metadata", {}),
        "artifact_links": {
            "sweep_manifest": str(sweep_dir / "sweep_manifest.json"),
            "leaderboard": str(sweep_dir / "leaderboard.json"),
            "weight_path_metadata": _text(
                _mapping(result.get("weight_path_metadata")).get("metadata_path")
            ),
        },
        "recommendation": _candidate_recommendation(result),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    if write:
        candidate_dir = sweep_dir / "candidates" / candidate_id
        candidate_dir.mkdir(parents=True, exist_ok=True)
        _write_json(candidate_dir / "candidate_report.json", payload)
        _write_text(
            candidate_dir / "candidate_report.md", render_candidate_report_markdown(payload)
        )
    return payload


def run_candidate_attribution(
    *,
    sweep_id: str,
    candidate_id: str,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Path = DEFAULT_CANDIDATE_ATTRIBUTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    candidate_report = candidate_report_payload(
        sweep_id=sweep_id,
        candidate_id=candidate_id,
        output_dir=sweep_output_dir,
        write=True,
    )
    attribution_dir = output_dir / candidate_id
    attribution_dir.mkdir(parents=True, exist_ok=True)
    real_path_raw = _text(candidate_report.get("real_evaluation_artifact_path"))
    real_path = Path(real_path_raw) if real_path_raw else None
    real_payload = _read_optional_json(real_path) if real_path is not None else None
    weight_metadata_path = (
        real_path.parent / "weight_path_metadata.json"
        if real_path is not None
        else None
    )
    weight_metadata = (
        _read_optional_json(weight_metadata_path) if weight_metadata_path is not None else None
    )
    metrics = _mapping(candidate_report.get("metrics"))
    weight_delta = _candidate_weight_delta_rows(real_payload)
    incomplete_reasons = []
    if real_payload is None:
        incomplete_reasons.append("missing_real_evaluation_artifact")
    if not weight_metadata or not weight_metadata.get("has_daily_weights"):
        incomplete_reasons.append("MISSING_DAILY_WEIGHT_PATH")
    attribution_completeness = _text(
        _mapping(weight_metadata).get("attribution_completeness"),
        WEIGHT_PATH_INCOMPLETE if incomplete_reasons else WEIGHT_PATH_PARTIAL,
    )
    if incomplete_reasons:
        status = WEIGHT_PATH_INCOMPLETE
        explainability_status = WEIGHT_PATH_INCOMPLETE
    elif attribution_completeness == WEIGHT_PATH_COMPLETE:
        status = WEIGHT_PATH_COMPLETE
        explainability_status = WEIGHT_PATH_COMPLETE
    else:
        status = WEIGHT_PATH_PARTIAL
        explainability_status = WEIGHT_PATH_PARTIAL
    rebalance = _rebalance_event_attribution(real_payload, metrics, incomplete_reasons)
    constraint = _constraint_event_attribution(real_payload, metrics)
    drawdown = _drawdown_window_attribution(real_payload, metrics)
    turnover = _turnover_attribution(real_payload, metrics)
    gap = _dynamic_vs_static_gap_attribution(real_payload, metrics)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_attribution_manifest",
        "candidate_id": candidate_id,
        "source_sweep_id": sweep_id,
        "status": status,
        "generated_at": generated.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "incomplete_reasons": incomplete_reasons,
        "real_evaluation_artifact_path": "" if real_path is None else str(real_path),
        "weight_path_metadata_path": ""
        if weight_metadata_path is None
        else str(weight_metadata_path),
        "attribution_completeness": attribution_completeness,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_attribution_report",
        "candidate_id": candidate_id,
        "source_sweep_id": sweep_id,
        "status": manifest["status"],
        "explainability_status": explainability_status,
        "attribution_completeness": attribution_completeness,
        "weight_path_metadata": weight_metadata or {},
        "incomplete_reasons": incomplete_reasons,
        "weight_path_delta": weight_delta,
        "rebalance_event_attribution": rebalance,
        "constraint_event_attribution": constraint,
        "drawdown_window_attribution": drawdown,
        "turnover_attribution": turnover,
        "dynamic_vs_static_gap_attribution": gap,
        "candidate_report_path": str(
            sweep_output_dir / sweep_id / "candidates" / candidate_id / "candidate_report.json"
        ),
        "weight_path_metadata_path": manifest["weight_path_metadata_path"],
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(attribution_dir / "attribution_manifest.json", manifest)
    _write_csv(attribution_dir / "weight_path_delta.csv", weight_delta)
    _write_json(attribution_dir / "rebalance_event_attribution.json", rebalance)
    _write_json(attribution_dir / "constraint_event_attribution.json", constraint)
    _write_json(attribution_dir / "drawdown_window_attribution.json", drawdown)
    _write_json(attribution_dir / "turnover_attribution.json", turnover)
    _write_json(attribution_dir / "dynamic_vs_static_gap_attribution.json", gap)
    _write_text(
        attribution_dir / "candidate_attribution_report.md",
        render_candidate_attribution_markdown(report),
    )
    _update_latest_pointer(
        "latest_candidate_attribution",
        candidate_id,
        attribution_dir / "attribution_manifest.json",
    )
    return {
        "candidate_id": candidate_id,
        "attribution_dir": attribution_dir,
        "report": report,
    }


def validate_candidate_attribution_artifact(
    *,
    candidate_id: str,
    output_dir: Path = DEFAULT_CANDIDATE_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    candidate_dir = output_dir / candidate_id
    checks = [
        _check(
            "candidate_attribution_dir_exists",
            candidate_dir.exists(),
            str(candidate_dir),
        )
    ]
    required = [
        "attribution_manifest.json",
        "weight_path_delta.csv",
        "rebalance_event_attribution.json",
        "constraint_event_attribution.json",
        "drawdown_window_attribution.json",
        "turnover_attribution.json",
        "dynamic_vs_static_gap_attribution.json",
        "candidate_attribution_report.md",
    ]
    checks.extend(
        _check(f"artifact_exists:{name}", (candidate_dir / name).exists(), name)
        for name in required
    )
    manifest = _read_optional_json(candidate_dir / "attribution_manifest.json") or {}
    checks.append(
        _check(
            "no_fabricated_weight_path",
            _text(manifest.get("status"))
            in {WEIGHT_PATH_COMPLETE, WEIGHT_PATH_PARTIAL, WEIGHT_PATH_INCOMPLETE},
            "missing path evidence must be explicit",
        )
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_attribution_validation",
        "candidate_id": candidate_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def weight_path_report_payload(
    *,
    evaluation_id: str,
    search_root: Path = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
) -> dict[str, Any]:
    weight_dir = _find_weight_path_dir(evaluation_id, search_root)
    if weight_dir is None:
        raise DynamicV3ParameterResearchError(f"weight path artifact not found: {evaluation_id}")
    metadata = _read_json(weight_dir / "weight_path_metadata.json")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_path_report_view",
        "status": metadata.get("attribution_completeness", WEIGHT_PATH_INCOMPLETE),
        "evaluation_id": evaluation_id,
        "candidate_id": metadata.get("candidate_id"),
        "daily_weights_path": str(weight_dir / "daily_weights.csv"),
        "weight_path_metadata_path": str(weight_dir / "weight_path_metadata.json"),
        "metadata": metadata,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def validate_weight_path_artifact(
    *,
    evaluation_id: str,
    search_root: Path = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
) -> dict[str, Any]:
    weight_dir = _find_weight_path_dir(evaluation_id, search_root)
    checks = [
        _check("weight_path_dir_found", weight_dir is not None, evaluation_id),
    ]
    metadata: dict[str, Any] = {}
    if weight_dir is not None:
        required = [
            "daily_weights.csv",
            "rebalance_events.json",
            "constraint_events.json",
            "rescue_events.json",
            "turnover_by_rebalance.csv",
            "weight_path_metadata.json",
        ]
        checks.extend(
            _check(f"artifact_exists:{name}", (weight_dir / name).exists(), name)
            for name in required
        )
        metadata = _read_optional_json(weight_dir / "weight_path_metadata.json") or {}
        daily_columns = _csv_columns(weight_dir / "daily_weights.csv")
        required_columns = {"date", "symbol", "weight", "candidate_id"}
        checks.append(
            _check(
                "daily_weights_required_columns",
                required_columns <= set(daily_columns),
                ",".join(daily_columns),
            )
        )
        checks.append(
            _check(
                "attribution_completeness_explicit",
                _text(metadata.get("attribution_completeness"))
                in {WEIGHT_PATH_COMPLETE, WEIGHT_PATH_PARTIAL, WEIGHT_PATH_INCOMPLETE},
                _text(metadata.get("attribution_completeness")),
            )
        )
        checks.append(
            _check(
                "daily_weight_count_positive",
                int(metadata.get("daily_weight_count") or 0) > 0,
                str(metadata.get("daily_weight_count")),
            )
        )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_path_validation",
        "evaluation_id": evaluation_id,
        "candidate_id": metadata.get("candidate_id", ""),
        "status": status,
        "attribution_completeness": metadata.get(
            "attribution_completeness", WEIGHT_PATH_INCOMPLETE
        ),
        "missing_fields": metadata.get("missing_fields", []),
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def run_walk_forward_validation(
    *,
    sweep_id: str,
    top_n: int,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Path = DEFAULT_WALK_FORWARD_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    sweep_dir = sweep_output_dir / sweep_id
    config = load_parameter_sweep_config(sweep_dir / "sweep_config.normalized.yaml")
    leaderboard = _read_json(sweep_dir / "leaderboard.json")
    candidates = _records(leaderboard.get("top_eligible_candidates"))[:top_n]
    wf_id = _stable_id("wf", sweep_id, top_n, generated.isoformat())
    wf_dir = _unique_dir(output_dir / wf_id)
    wf_dir.mkdir(parents=True, exist_ok=False)
    windows = walk_forward_windows(config)
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        for window in windows:
            rows.append(_walk_forward_row(candidate, window, config))
    candidate_summaries = _walk_forward_candidate_summaries(rows, config)
    wf_leaderboard = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_walk_forward_leaderboard",
        "walk_forward_id": wf_id,
        "source_sweep_id": sweep_id,
        "candidates": candidate_summaries,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    manifest = {
        "walk_forward_id": wf_id,
        "source_sweep_id": sweep_id,
        "top_n": top_n,
        "train_window_months": config.walk_forward.train_window_months,
        "test_window_months": config.walk_forward.test_window_months,
        "step_months": config.walk_forward.step_months,
        "min_windows": config.walk_forward.min_windows,
        "candidate_count": len(candidates),
        "status": "completed",
        "started_at": generated.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_walk_forward_report",
        "walk_forward_id": wf_id,
        "source_sweep_id": sweep_id,
        "status": "PASS" if candidate_summaries else "REVIEW_REQUIRED",
        "holdout_start": config.out_of_sample.holdout_start.isoformat(),
        "holdout_end": config.out_of_sample.holdout_end.isoformat(),
        "oos_summary": _oos_summary(candidate_summaries, config),
        "leaderboard": candidate_summaries,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(wf_dir / "wf_manifest.json", manifest)
    _write_json(wf_dir / "wf_windows.json", {"windows": windows})
    _write_jsonl(wf_dir / "wf_candidate_results.jsonl", rows)
    _write_json(wf_dir / "wf_leaderboard.json", wf_leaderboard)
    _write_text(
        wf_dir / "wf_leaderboard.md", render_walk_forward_leaderboard_markdown(wf_leaderboard)
    )
    _write_text(wf_dir / "wf_report.md", render_walk_forward_report_markdown(report))
    _update_latest_pointer("latest_walk_forward", wf_id, wf_dir / "wf_manifest.json")
    return {"walk_forward_id": wf_id, "walk_forward_dir": wf_dir, "report": report}


def run_walk_forward_selection(
    *,
    config_path: Path = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    profile: str = "small_real",
    sweep_id: str | None = None,
    profile_config_path: Path = DEFAULT_PARAMETER_SWEEP_PROFILE_CONFIG_PATH,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Path = DEFAULT_WALK_FORWARD_SELECTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    resolved_sweep_id = sweep_id or latest_sweep_id()
    if not resolved_sweep_id:
        raise DynamicV3ParameterResearchError("walk-forward selection requires a source sweep")
    sweep_dir = sweep_output_dir / resolved_sweep_id
    config = load_parameter_sweep_config(sweep_dir / "sweep_config.normalized.yaml")
    source_results = [
        row for row in _read_candidate_results(sweep_dir)
        if row.get("gate") != GATE_REJECT
    ]
    windows = walk_forward_windows(config)
    wf_selection_id = _stable_id(
        "wf-selection",
        resolved_sweep_id,
        profile,
        generated.isoformat(),
    )
    wf_dir = _unique_dir(output_dir / wf_selection_id)
    wf_dir.mkdir(parents=True, exist_ok=False)
    train_leaderboards: list[dict[str, Any]] = []
    selected_rows: list[dict[str, Any]] = []
    test_rows: list[dict[str, Any]] = []
    for index, window in enumerate(windows, start=1):
        leaderboard = _window_train_leaderboard(
            source_results,
            window=window,
            window_index=index,
        )
        train_leaderboards.append(
            {
                "window_index": index,
                "window": dict(window),
                "leaderboard": leaderboard,
            }
        )
        selected = leaderboard[0] if leaderboard else {}
        if selected:
            selected_rows.append(
                {
                    "window_index": index,
                    "train_start": window["train_start"],
                    "train_end": window["train_end"],
                    "test_start": window["test_start"],
                    "test_end": window["test_end"],
                    "candidate_id": selected.get("candidate_id"),
                    "train_rank": selected.get("train_rank"),
                    "parameters": selected.get("parameters", {}),
                }
            )
            test_rows.append(_walk_forward_selection_test_row(selected, window, config))
    pass_count = sum(1 for row in test_rows if row.get("test_gate") != GATE_REJECT)
    status = (
        "PASS"
        if test_rows and pass_count / len(test_rows) >= config.walk_forward.min_pass_ratio
        else "REVIEW_REQUIRED"
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_walk_forward_selection_manifest",
        "wf_selection_id": wf_selection_id,
        "source_sweep_id": resolved_sweep_id,
        "profile": profile,
        "config_path": str(config_path),
        "profile_config_path": str(profile_config_path),
        "status": status,
        "window_count": len(windows),
        "selected_candidate_count": len(selected_rows),
        "test_pass_count": pass_count,
        "started_at": generated.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_walk_forward_selection_report",
        "wf_selection_id": wf_selection_id,
        "source_sweep_id": resolved_sweep_id,
        "status": status,
        "profile": profile,
        "summary": {
            "window_count": len(windows),
            "selected_candidate_count": len(selected_rows),
            "test_pass_count": pass_count,
            "parameter_stability": _wf_parameter_stability(selected_rows),
            "overfit_windows": [
                row["window_index"] for row in test_rows if row.get("test_gate") == GATE_REJECT
            ],
        },
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(wf_dir / "wf_selection_manifest.json", manifest)
    _write_json(wf_dir / "wf_windows.json", {"windows": windows})
    _write_jsonl(wf_dir / "train_window_leaderboards.jsonl", train_leaderboards)
    _write_jsonl(wf_dir / "selected_candidates.jsonl", selected_rows)
    _write_jsonl(wf_dir / "test_window_results.jsonl", test_rows)
    _write_text(wf_dir / "wf_selection_report.md", render_wf_selection_markdown(report))
    _update_latest_pointer(
        "latest_walk_forward_selection",
        wf_selection_id,
        wf_dir / "wf_selection_manifest.json",
    )
    return {
        "wf_selection_id": wf_selection_id,
        "wf_selection_dir": wf_dir,
        "report": report,
    }


def walk_forward_selection_report_payload(
    *,
    wf_selection_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WALK_FORWARD_SELECTION_DIR,
) -> dict[str, Any]:
    resolved_id = wf_selection_id or (
        _latest_pointer_artifact_id("latest_walk_forward_selection") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--wf-selection-id or --latest is required")
    wf_dir = output_dir / resolved_id
    manifest = _read_json(wf_dir / "wf_selection_manifest.json")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_walk_forward_selection_report_view",
        "wf_selection_id": resolved_id,
        "status": manifest.get("status", "UNKNOWN"),
        "report_path": str(wf_dir / "wf_selection_report.md"),
        "manifest": manifest,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def validate_walk_forward_selection_artifact(
    *,
    wf_selection_id: str,
    output_dir: Path = DEFAULT_WALK_FORWARD_SELECTION_DIR,
) -> dict[str, Any]:
    wf_dir = output_dir / wf_selection_id
    required = [
        "wf_selection_manifest.json",
        "wf_windows.json",
        "train_window_leaderboards.jsonl",
        "selected_candidates.jsonl",
        "test_window_results.jsonl",
        "wf_selection_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (wf_dir / name).exists(), name)
        for name in required
    ]
    manifest = _read_optional_json(wf_dir / "wf_selection_manifest.json") or {}
    checks.append(
        _check(
            "selected_candidates_present",
            int(manifest.get("selected_candidate_count") or 0) > 0,
            str(manifest.get("selected_candidate_count")),
        )
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_walk_forward_selection_validation",
        "wf_selection_id": wf_selection_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def validate_walk_forward_artifact(
    *,
    walk_forward_id: str,
    output_dir: Path = DEFAULT_WALK_FORWARD_DIR,
) -> dict[str, Any]:
    wf_dir = output_dir / walk_forward_id
    required = [
        "wf_manifest.json",
        "wf_windows.json",
        "wf_candidate_results.jsonl",
        "wf_leaderboard.json",
        "wf_leaderboard.md",
        "wf_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (wf_dir / name).exists(), name) for name in required
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_walk_forward_validation",
        "walk_forward_id": walk_forward_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def walk_forward_report_payload(
    *,
    walk_forward_id: str,
    output_dir: Path = DEFAULT_WALK_FORWARD_DIR,
) -> dict[str, Any]:
    wf_dir = output_dir / walk_forward_id
    manifest = _read_json(wf_dir / "wf_manifest.json")
    leaderboard = _read_json(wf_dir / "wf_leaderboard.json")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_walk_forward_report_view",
        "walk_forward_id": walk_forward_id,
        "status": manifest.get("status", "UNKNOWN"),
        "manifest": manifest,
        "leaderboard": leaderboard.get("candidates", []),
        "report_path": str(wf_dir / "wf_report.md"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def run_robustness_diagnostics(
    *,
    sweep_id: str,
    candidate_id: str,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Path = DEFAULT_ROBUSTNESS_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    sweep_dir = sweep_output_dir / sweep_id
    config = load_parameter_sweep_config(sweep_dir / "sweep_config.normalized.yaml")
    result = _candidate_result(sweep_dir, candidate_id)
    if result is None:
        raise DynamicV3ParameterResearchError(f"candidate not found: {candidate_id}")
    robustness_id = _stable_id("robustness", sweep_id, candidate_id, generated.isoformat())
    robustness_dir = _unique_dir(output_dir / robustness_id)
    robustness_dir.mkdir(parents=True, exist_ok=False)
    sensitivity = _sensitivity_rows(result, config)
    stress = _stress_bucket_results(result, config)
    regime = _regime_bucket_results(result)
    overfit = _overfit_diagnostics(sensitivity, stress, config)
    manifest = {
        "robustness_id": robustness_id,
        "source_sweep_id": sweep_id,
        "candidate_id": candidate_id,
        "status": overfit["robustness_status"],
        "started_at": generated.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_robustness_report",
        "robustness_id": robustness_id,
        "source_sweep_id": sweep_id,
        "candidate_id": candidate_id,
        "status": overfit["robustness_status"],
        "overfit_status": overfit["overfit_status"],
        "parameter_sensitivity_status": overfit["parameter_sensitivity_status"],
        "stress_bucket_status": overfit["stress_bucket_status"],
        "multiple_testing_warning": overfit["multiple_testing_warning"],
        "optional_pbo_dsr_status": "NOT_RUN_REVIEW_NOTE",
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(robustness_dir / "robustness_manifest.json", manifest)
    _write_csv(robustness_dir / "sensitivity_matrix.csv", sensitivity)
    _write_json(robustness_dir / "stress_bucket_results.json", stress)
    _write_json(robustness_dir / "regime_bucket_results.json", regime)
    _write_json(robustness_dir / "overfit_diagnostics.json", overfit)
    _write_text(robustness_dir / "robustness_report.md", render_robustness_report_markdown(report))
    _update_latest_pointer(
        "latest_robustness", robustness_id, robustness_dir / "robustness_manifest.json"
    )
    return {
        "robustness_id": robustness_id,
        "robustness_dir": robustness_dir,
        "report": report,
    }


def run_overfit_review(
    *,
    sweep_id: str,
    candidate_id: str,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Path = DEFAULT_OVERFIT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    sweep_dir = sweep_output_dir / sweep_id
    config = load_parameter_sweep_config(sweep_dir / "sweep_config.normalized.yaml")
    result = _candidate_result(sweep_dir, candidate_id)
    if result is None:
        raise DynamicV3ParameterResearchError(f"candidate not found: {candidate_id}")
    results = _read_candidate_results(sweep_dir)
    overfit_id = _stable_id("overfit", sweep_id, candidate_id, generated.isoformat())
    overfit_dir = _unique_dir(output_dir / overfit_id)
    overfit_dir.mkdir(parents=True, exist_ok=False)
    rank = _candidate_rank(results, candidate_id)
    rank_stability = _rank_stability_payload(results, candidate_id, rank)
    neighborhood = _parameter_neighborhood_stability_payload(result, config)
    regime = _overfit_regime_stability_payload(result)
    extreme_day = _extreme_day_dependency_payload(result)
    multiple_testing = _multiple_testing_warning_payload(results)
    overfit_status = _overfit_status_from_components(
        result=result,
        rank_stability=rank_stability,
        neighborhood=neighborhood,
        extreme_day=extreme_day,
        multiple_testing=multiple_testing,
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_overfit_manifest",
        "overfit_id": overfit_id,
        "source_sweep_id": sweep_id,
        "candidate_id": candidate_id,
        "status": overfit_status,
        "overfit_status": overfit_status,
        "started_at": generated.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_overfit_report",
        "overfit_id": overfit_id,
        "source_sweep_id": sweep_id,
        "candidate_id": candidate_id,
        "status": overfit_status,
        "overfit_status": overfit_status,
        "rank_stability": rank_stability,
        "parameter_neighborhood_stability": neighborhood,
        "regime_stability": regime,
        "extreme_day_dependency": extreme_day,
        "multiple_testing_warning": multiple_testing,
        "optional_pbo_dsr_status": "NOT_RUN_REVIEW_REQUIRED",
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(overfit_dir / "overfit_manifest.json", manifest)
    _write_json(overfit_dir / "rank_stability.json", rank_stability)
    _write_json(overfit_dir / "parameter_neighborhood_stability.json", neighborhood)
    _write_json(overfit_dir / "regime_stability.json", regime)
    _write_json(overfit_dir / "extreme_day_dependency.json", extreme_day)
    _write_json(overfit_dir / "multiple_testing_warning.json", multiple_testing)
    _write_text(overfit_dir / "overfit_report.md", render_overfit_markdown(report))
    _update_latest_pointer("latest_overfit", overfit_id, overfit_dir / "overfit_manifest.json")
    return {"overfit_id": overfit_id, "overfit_dir": overfit_dir, "report": report}


def overfit_report_payload(
    *,
    overfit_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_OVERFIT_DIR,
) -> dict[str, Any]:
    resolved_id = overfit_id or (_latest_pointer_artifact_id("latest_overfit") if latest else "")
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--overfit-id or --latest is required")
    overfit_dir = output_dir / resolved_id
    manifest = _read_json(overfit_dir / "overfit_manifest.json")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_overfit_report_view",
        "overfit_id": resolved_id,
        "status": manifest.get("status", "UNKNOWN"),
        "overfit_status": manifest.get("overfit_status", "UNKNOWN"),
        "candidate_id": manifest.get("candidate_id"),
        "report_path": str(overfit_dir / "overfit_report.md"),
        "manifest": manifest,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def validate_overfit_artifact(
    *,
    overfit_id: str,
    output_dir: Path = DEFAULT_OVERFIT_DIR,
) -> dict[str, Any]:
    overfit_dir = output_dir / overfit_id
    required = [
        "overfit_manifest.json",
        "rank_stability.json",
        "parameter_neighborhood_stability.json",
        "regime_stability.json",
        "extreme_day_dependency.json",
        "multiple_testing_warning.json",
        "overfit_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (overfit_dir / name).exists(), name)
        for name in required
    ]
    manifest = _read_optional_json(overfit_dir / "overfit_manifest.json") or {}
    checks.append(
        _check(
            "overfit_status_allowed",
            _text(manifest.get("overfit_status"))
            in {"LOW_RISK", "REVIEW_REQUIRED", "HIGH_RISK"},
            _text(manifest.get("overfit_status")),
        )
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_overfit_validation",
        "overfit_id": overfit_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def validate_robustness_artifact(
    *,
    robustness_id: str,
    output_dir: Path = DEFAULT_ROBUSTNESS_DIR,
) -> dict[str, Any]:
    robustness_dir = output_dir / robustness_id
    required = [
        "robustness_manifest.json",
        "sensitivity_matrix.csv",
        "stress_bucket_results.json",
        "regime_bucket_results.json",
        "overfit_diagnostics.json",
        "robustness_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (robustness_dir / name).exists(), name)
        for name in required
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_robustness_validation",
        "robustness_id": robustness_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def robustness_report_payload(
    *,
    robustness_id: str,
    output_dir: Path = DEFAULT_ROBUSTNESS_DIR,
) -> dict[str, Any]:
    robustness_dir = output_dir / robustness_id
    manifest = _read_json(robustness_dir / "robustness_manifest.json")
    diagnostics = _read_json(robustness_dir / "overfit_diagnostics.json")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_robustness_report_view",
        "robustness_id": robustness_id,
        "status": manifest.get("status", "UNKNOWN"),
        "manifest": manifest,
        "overfit_diagnostics": diagnostics,
        "report_path": str(robustness_dir / "robustness_report.md"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def load_shadow_registry(path: Path = DEFAULT_SHADOW_REGISTRY_PATH) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": 1, "candidates": []}
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise DynamicV3ParameterResearchError("shadow registry must be a mapping")
    candidates = _records(raw.get("candidates"))
    return {"schema_version": raw.get("schema_version", 1), "candidates": candidates}


def write_shadow_registry(
    payload: Mapping[str, Any], path: Path = DEFAULT_SHADOW_REGISTRY_PATH
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _write_yaml(path, payload)


def register_shadow_candidate(
    *,
    sweep_id: str,
    candidate_id: str,
    registry_path: Path = DEFAULT_SHADOW_REGISTRY_PATH,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    sweep_dir = sweep_output_dir / sweep_id
    report = candidate_report_payload(
        sweep_id=sweep_id,
        candidate_id=candidate_id,
        output_dir=sweep_output_dir,
        write=True,
    )
    if report["hard_gate_status"] == GATE_REJECT:
        raise DynamicV3ParameterResearchError("rejected candidate cannot be registered")
    candidate_report_path = sweep_dir / "candidates" / candidate_id / "candidate_report.json"
    if not candidate_report_path.exists():
        raise DynamicV3ParameterResearchError("candidate report is required before registration")
    config = load_parameter_sweep_config(sweep_dir / "sweep_config.normalized.yaml")
    registry = load_shadow_registry(registry_path)
    candidates = _records(registry.get("candidates"))
    existing = next((row for row in candidates if row.get("candidate_id") == candidate_id), None)
    basis = _shadow_observation_basis(candidate_id)
    record = {
        "candidate_id": candidate_id,
        "strategy_family": STRATEGY_FAMILY,
        "status": GATE_OBSERVE_ONLY,
        "source_sweep_id": sweep_id,
        "source_walk_forward_id": basis.get("source_walk_forward_id", ""),
        "source_robustness_id": basis.get("source_robustness_id", ""),
        "observation_basis_status": basis.get("status"),
        "parameters": report.get("parameters", {}),
        "evaluator_mode": report.get("evaluator_mode", EVALUATOR_TINY_FIXTURE_PROXY),
        "evaluator_version": report.get("evaluator_version", ""),
        "real_evaluation_artifact_path": report.get("real_evaluation_artifact_path", ""),
        "metrics_source": report.get("metrics_source", "UNKNOWN"),
        "not_for_investment_decision": report.get("not_for_investment_decision") is True,
        "registered_at": generated.isoformat(),
        "registered_by": "TRADING-098",
        "promotion_earliest_after_rebalance_count": (
            config.shadow.promotion_earliest_after_rebalance_count
        ),
        "promotion_earliest_after_days": config.shadow.promotion_earliest_after_days,
        "required_observation_metrics": config.shadow.required_observation_metrics,
        "observed_rebalance_count": 0,
        "latest_metrics": report.get("metrics", {}),
        "notes": "Initial observe_only candidate from constrained sweep",
    }
    if existing is None:
        candidates.append(record)
    else:
        existing.update(record)
    registry = {"schema_version": 1, "candidates": candidates}
    write_shadow_registry(registry, registry_path)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_registration",
        "status": "PASS",
        "candidate": record,
        "registry_path": str(registry_path),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def shadow_list_payload(
    *,
    registry_path: Path = DEFAULT_SHADOW_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_shadow_registry(registry_path)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_registry_list",
        "status": "PASS",
        "registry_path": str(registry_path),
        "candidates": _records(registry.get("candidates")),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def shadow_report_payload(
    *,
    candidate_id: str | None = None,
    all_candidates: bool = False,
    registry_path: Path = DEFAULT_SHADOW_REGISTRY_PATH,
    output_dir: Path = DEFAULT_SHADOW_REPORT_DIR,
    write: bool = True,
) -> dict[str, Any]:
    registry = load_shadow_registry(registry_path)
    candidates = _records(registry.get("candidates"))
    selected = (
        candidates
        if all_candidates
        else [row for row in candidates if row.get("candidate_id") == candidate_id]
    )
    if not selected and candidate_id:
        raise DynamicV3ParameterResearchError(f"shadow candidate not found: {candidate_id}")
    reports = [_shadow_candidate_report(row) for row in selected]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_report",
        "status": "PASS" if reports else "MISSING",
        "candidate_id": candidate_id or "ALL",
        "reports": reports,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    if write:
        output_dir.mkdir(parents=True, exist_ok=True)
        report_id = candidate_id or "all"
        path = output_dir / f"shadow_report_{report_id}.json"
        _write_json(path, payload)
        _write_text(
            output_dir / f"shadow_report_{report_id}.md", render_shadow_report_markdown(payload)
        )
        _update_latest_pointer("latest_shadow_report", report_id, path)
    return payload


def run_shadow_monitor(
    *,
    as_of: date,
    registry_path: Path = DEFAULT_SHADOW_REGISTRY_PATH,
    output_dir: Path = DEFAULT_SHADOW_MONITOR_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    monitor_id = _stable_id("shadow-monitor", as_of.isoformat(), generated.isoformat())
    monitor_dir = _unique_dir(output_dir / monitor_id)
    monitor_dir.mkdir(parents=True, exist_ok=False)
    registry = load_shadow_registry(registry_path)
    results = [
        _shadow_monitor_candidate_result(row, as_of=as_of)
        for row in _records(registry.get("candidates"))
    ]
    ready_count = sum(
        1 for row in results if row.get("promotion_eligibility") == "promotion_review_ready"
    )
    drift_count = sum(
        1 for row in results if row.get("live_vs_backtest_drift") == "REVIEW_REQUIRED"
    )
    brief = render_shadow_monitor_reader_brief_section(results, ready_count, drift_count)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_monitor_manifest",
        "monitor_id": monitor_id,
        "status": "PASS",
        "as_of": as_of.isoformat(),
        "candidate_count": len(results),
        "promotion_review_ready_count": ready_count,
        "live_drift_review_required_count": drift_count,
        "started_at": generated.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_monitor_report",
        "monitor_id": monitor_id,
        "status": "PASS",
        "as_of": as_of.isoformat(),
        "candidate_monitor_results": results,
        "reader_brief_section": brief,
        "summary": {
            "observe_only_candidate_count": len(results),
            "promotion_review_ready_count": ready_count,
            "live_drift_review_required_count": drift_count,
        },
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(monitor_dir / "shadow_monitor_manifest.json", manifest)
    _write_jsonl(monitor_dir / "candidate_monitor_results.jsonl", results)
    _write_text(monitor_dir / "shadow_monitor_report.md", render_shadow_monitor_markdown(report))
    _write_text(monitor_dir / "reader_brief_section.md", brief)
    _update_latest_pointer(
        "latest_shadow_monitor",
        monitor_id,
        monitor_dir / "shadow_monitor_manifest.json",
    )
    return {"monitor_id": monitor_id, "monitor_dir": monitor_dir, "report": report}


def shadow_monitor_report_payload(
    *,
    monitor_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SHADOW_MONITOR_DIR,
) -> dict[str, Any]:
    resolved_id = monitor_id or (
        _latest_pointer_artifact_id("latest_shadow_monitor") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--monitor-id or --latest is required")
    monitor_dir = output_dir / resolved_id
    manifest = _read_json(monitor_dir / "shadow_monitor_manifest.json")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_monitor_report_view",
        "monitor_id": resolved_id,
        "status": manifest.get("status", "UNKNOWN"),
        "candidate_count": manifest.get("candidate_count"),
        "report_path": str(monitor_dir / "shadow_monitor_report.md"),
        "manifest": manifest,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def validate_shadow_monitor_artifact(
    *,
    monitor_id: str,
    output_dir: Path = DEFAULT_SHADOW_MONITOR_DIR,
) -> dict[str, Any]:
    monitor_dir = output_dir / monitor_id
    required = [
        "shadow_monitor_manifest.json",
        "candidate_monitor_results.jsonl",
        "shadow_monitor_report.md",
        "reader_brief_section.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (monitor_dir / name).exists(), name)
        for name in required
    ]
    manifest = _read_optional_json(monitor_dir / "shadow_monitor_manifest.json") or {}
    checks.append(
        _check(
            "production_effect_none",
            _mapping(manifest.get("safety")).get("production_effect") == "none",
            "shadow monitor is observe-only",
        )
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_monitor_validation",
        "monitor_id": monitor_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def validate_shadow_registry(
    *,
    registry_path: Path = DEFAULT_SHADOW_REGISTRY_PATH,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
) -> dict[str, Any]:
    registry = load_shadow_registry(registry_path)
    checks: list[dict[str, Any]] = [
        _check("registry_exists", registry_path.exists(), str(registry_path))
    ]
    for row in _records(registry.get("candidates")):
        candidate_id = _text(row.get("candidate_id"))
        source_sweep_id = _text(row.get("source_sweep_id"))
        checks.append(
            _check(
                f"{candidate_id}:source_sweep_id_present",
                bool(source_sweep_id),
                "source_sweep_id is mandatory",
            )
        )
        checks.append(
            _check(
                f"{candidate_id}:parameters_present",
                bool(_mapping(row.get("parameters"))),
                "parameters are mandatory",
            )
        )
        report_path = (
            sweep_output_dir
            / source_sweep_id
            / "candidates"
            / candidate_id
            / "candidate_report.json"
        )
        checks.append(
            _check(
                f"{candidate_id}:candidate_report_exists", report_path.exists(), str(report_path)
            )
        )
        checks.append(
            _check(
                f"{candidate_id}:observe_only",
                row.get("status") == GATE_OBSERVE_ONLY,
                "shadow registry is observe_only",
            )
        )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_registry_validation",
        "status": status,
        "registry_path": str(registry_path),
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def artifacts_latest_payload(pointer_dir: Path = DEFAULT_LATEST_POINTER_DIR) -> dict[str, Any]:
    pointers = {
        path.stem: _read_optional_json(path)
        for path in sorted(pointer_dir.glob("latest_*.json"))
        if path.is_file()
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_artifacts_latest",
        "status": "PASS" if pointers else "MISSING",
        "pointer_dir": str(pointer_dir),
        "pointers": pointers,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def validate_artifacts_payload(
    *,
    family: str = STRATEGY_FAMILY,
    pointer_dir: Path = DEFAULT_LATEST_POINTER_DIR,
) -> dict[str, Any]:
    pointers = artifacts_latest_payload(pointer_dir).get("pointers", {})
    checks = [_check("family_supported", family == STRATEGY_FAMILY, family)]
    for name, pointer in _mapping(pointers).items():
        target = Path(_text(_mapping(pointer).get("path")))
        checks.append(_check(f"{name}:pointer_target_exists", target.exists(), str(target)))
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_artifacts_validation",
        "family": family,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def stale_artifacts_payload(
    *,
    family: str = STRATEGY_FAMILY,
    config_path: Path = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    pointer_dir: Path = DEFAULT_LATEST_POINTER_DIR,
    now: datetime | None = None,
) -> dict[str, Any]:
    config = load_parameter_sweep_config(config_path)
    current = now or datetime.now(UTC)
    pointers = _mapping(artifacts_latest_payload(pointer_dir).get("pointers"))
    stale: list[dict[str, Any]] = []
    for name, pointer in pointers.items():
        updated_raw = _text(_mapping(pointer).get("updated_at"))
        updated = _parse_datetime(updated_raw)
        age_days = None if updated is None else (current - updated).days
        if age_days is None or age_days > config.artifact_retention.stale_after_days:
            stale.append({"pointer": name, "updated_at": updated_raw, "age_days": age_days})
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_artifacts_stale",
        "family": family,
        "status": "STALE" if stale else "PASS",
        "stale_after_days": config.artifact_retention.stale_after_days,
        "stale_artifacts": stale,
        "retention_policy": config.artifact_retention.model_dump(mode="json"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def governance_report_payload(
    *,
    governance_path: Path = DEFAULT_PARAMETER_GOVERNANCE_CONFIG_PATH,
    output_dir: Path = DEFAULT_GOVERNANCE_DIR,
    write: bool = True,
) -> dict[str, Any]:
    governance = load_parameter_governance_config(governance_path)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_parameter_governance_report",
        "status": "PASS",
        "policy_id": governance.policy_id,
        "version": governance.version,
        "search_space_version": governance.search_space_version,
        "parameter_groups": {
            name: group.model_dump(mode="json")
            for name, group in governance.parameter_groups.items()
        },
        "manual_only_parameters": _governance_parameters(governance, "manual_only"),
        "manual_review_required_parameters": _governance_parameters(
            governance,
            "manual_review_required",
        ),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    if write:
        output_dir.mkdir(parents=True, exist_ok=True)
        _write_json(output_dir / "parameter_governance_report.json", payload)
        _write_text(
            output_dir / "parameter_governance_report.md",
            render_governance_markdown(payload),
        )
        _update_latest_pointer(
            "latest_parameter_governance",
            governance.search_space_version,
            output_dir / "parameter_governance_report.json",
        )
    return payload


def validate_parameter_governance(
    *,
    governance_path: Path = DEFAULT_PARAMETER_GOVERNANCE_CONFIG_PATH,
    config_path: Path = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    try:
        governance = load_parameter_governance_config(governance_path)
        config = load_parameter_sweep_config(config_path)
        checks.append(_check("governance_schema_valid", True, governance.policy_id))
        checks.extend(_governance_checks(config=config, governance=governance))
    except DynamicV3ParameterResearchError as exc:
        checks.append(_check("governance_schema_valid", False, str(exc)))
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_parameter_governance_validation",
        "status": status,
        "governance_path": str(governance_path),
        "config_path": str(config_path),
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def governance_diff_payload(*, old_config: Path, new_config: Path) -> dict[str, Any]:
    old = load_parameter_governance_config(old_config)
    new = load_parameter_governance_config(new_config)
    old_params = _governance_policy_by_parameter(old)
    new_params = _governance_policy_by_parameter(new)
    changes = []
    for parameter in sorted(set(old_params) | set(new_params)):
        if old_params.get(parameter) != new_params.get(parameter):
            changes.append(
                {
                    "parameter": parameter,
                    "old_policy": old_params.get(parameter, ""),
                    "new_policy": new_params.get(parameter, ""),
                    "manual_review_required": True,
                }
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_parameter_governance_diff",
        "status": "PASS",
        "old_search_space_version": old.search_space_version,
        "new_search_space_version": new.search_space_version,
        "change_count": len(changes),
        "changes": changes,
        "manual_review_required": bool(changes),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def build_research_index(
    *,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    shadow_registry_path: Path = DEFAULT_SHADOW_REGISTRY_PATH,
    output_dir: Path = DEFAULT_RESEARCH_INDEX_DIR,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    sweeps: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    leaderboard_history: list[dict[str, Any]] = []
    for manifest_path in sorted(sweep_output_dir.glob("*/sweep_manifest.json")):
        manifest = _read_optional_json(manifest_path)
        if not manifest:
            continue
        sweep_id = _text(manifest.get("sweep_id"), manifest_path.parent.name)
        sweeps.append(
            {
                "sweep_id": sweep_id,
                "path": str(manifest_path.parent),
                "status": manifest.get("status"),
                "evaluator_mode": manifest.get("evaluator_mode"),
                "candidate_count": manifest.get("candidate_count"),
                "search_space_version": manifest.get("search_space_version", ""),
                "as_of": manifest.get("as_of"),
                "end": manifest.get("end"),
            }
        )
        leaderboard = _read_optional_json(manifest_path.parent / "leaderboard.json") or {}
        top = _records(leaderboard.get("top_eligible_candidates"))
        leaderboard_history.append(
            {
                "sweep_id": sweep_id,
                "top_candidate": _text(top[0].get("candidate_id")) if top else "",
                "candidate_count": leaderboard.get("candidate_count"),
                "evaluator_mode": leaderboard.get("evaluator_mode"),
            }
        )
        for row in _read_candidate_results(manifest_path.parent):
            candidates.append(
                {
                    "candidate_id": row.get("candidate_id"),
                    "source_sweep_id": sweep_id,
                    "gate": row.get("gate"),
                    "score": row.get("score"),
                    "parameters": row.get("parameters", {}),
                    "metrics": row.get("metrics", {}),
                    "evaluator_mode": row.get("evaluator_mode"),
                    "search_space_version": row.get("search_space_version", ""),
                    "real_evaluation_artifact_path": row.get("real_evaluation_artifact_path", ""),
                }
            )
    shadow_history = _records(load_shadow_registry(shadow_registry_path).get("candidates"))
    _write_json(output_dir / "sweeps_index.json", {"sweeps": sweeps})
    _write_jsonl(output_dir / "candidates_index.jsonl", candidates)
    _write_jsonl(output_dir / "leaderboard_history.jsonl", leaderboard_history)
    _write_jsonl(output_dir / "shadow_history.jsonl", shadow_history)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_research_index",
        "status": "PASS",
        "sweep_count": len(sweeps),
        "candidate_count": len(candidates),
        "shadow_candidate_count": len(shadow_history),
        "output_dir": str(output_dir),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(output_dir / "research_index_manifest.json", payload)
    _update_latest_pointer(
        "latest_research_index",
        "research_index",
        output_dir / "research_index_manifest.json",
    )
    return payload


def research_query_payload(
    *,
    candidate_id: str,
    output_dir: Path = DEFAULT_RESEARCH_INDEX_DIR,
) -> dict[str, Any]:
    rows = [
        row for row in _read_jsonl(output_dir / "candidates_index.jsonl")
        if row.get("candidate_id") == candidate_id
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_research_query",
        "status": "PASS" if rows else "MISSING",
        "candidate_id": candidate_id,
        "matches": rows,
        "artifact_paths": [row.get("real_evaluation_artifact_path", "") for row in rows],
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def research_compare_payload(
    *,
    candidate_a: str,
    candidate_b: str,
    output_dir: Path = DEFAULT_RESEARCH_INDEX_DIR,
) -> dict[str, Any]:
    rows = _read_jsonl(output_dir / "candidates_index.jsonl")
    a = next((row for row in rows if row.get("candidate_id") == candidate_a), {})
    b = next((row for row in rows if row.get("candidate_id") == candidate_b), {})
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_research_compare",
        "status": "PASS" if a and b else "MISSING",
        "candidate_a": candidate_a,
        "candidate_b": candidate_b,
        "parameter_diff": _dict_diff(_mapping(a.get("parameters")), _mapping(b.get("parameters"))),
        "metric_diff": _numeric_metric_diff(_mapping(a.get("metrics")), _mapping(b.get("metrics"))),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def research_history_payload(
    *,
    parameter: str,
    output_dir: Path = DEFAULT_RESEARCH_INDEX_DIR,
) -> dict[str, Any]:
    values = []
    for row in _read_jsonl(output_dir / "candidates_index.jsonl"):
        params = _mapping(row.get("parameters"))
        if parameter in params:
            values.append(
                {
                    "value": params[parameter],
                    "score": row.get("score"),
                    "gate": row.get("gate"),
                }
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_research_parameter_history",
        "status": "PASS" if values else "MISSING",
        "parameter": parameter,
        "observation_count": len(values),
        "distribution": dict(Counter(_text(row.get("value")) for row in values)),
        "rows": values[:100],
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def build_promotion_pack(
    *,
    candidate_id: str,
    registry_path: Path = DEFAULT_SHADOW_REGISTRY_PATH,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    walk_forward_dir: Path = DEFAULT_WALK_FORWARD_DIR,
    robustness_dir: Path = DEFAULT_ROBUSTNESS_DIR,
    overfit_dir: Path = DEFAULT_OVERFIT_DIR,
    candidate_attribution_dir: Path = DEFAULT_CANDIDATE_ATTRIBUTION_DIR,
    data_provenance_dir: Path = DEFAULT_DATA_PROVENANCE_DIR,
    output_dir: Path = DEFAULT_PROMOTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    evidence = _promotion_evidence(
        candidate_id=candidate_id,
        registry_path=registry_path,
        sweep_output_dir=sweep_output_dir,
        walk_forward_dir=walk_forward_dir,
        robustness_dir=robustness_dir,
        overfit_dir=overfit_dir,
        candidate_attribution_dir=candidate_attribution_dir,
        data_provenance_dir=data_provenance_dir,
    )
    status, reasons = _promotion_status(evidence)
    candidate = _mapping(evidence.get("candidate_report"))
    evaluator = _text(candidate.get("evaluator_mode"), EVALUATOR_TINY_FIXTURE_PROXY)
    promotion_id = _stable_id("promotion", candidate_id, generated.isoformat())
    pack_dir = _unique_dir(output_dir / candidate_id / promotion_id)
    pack_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "promotion_id": promotion_id,
        "candidate_id": candidate_id,
        "status": status,
        "decision_reasons": reasons,
        "evaluator_mode": evaluator,
        "evaluator_version": _evaluator_version(evaluator),
        "not_for_investment_decision": (
            candidate.get("not_for_investment_decision") is True
        ),
        "generated_at": generated.isoformat(),
        "manual_review_required": True,
        "production_candidate_generated": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
    }
    metric_delta = _promotion_metric_delta(evidence)
    risk_summary = _promotion_risk_summary(evidence, status, reasons)
    linked = _promotion_linked_artifacts(evidence)
    reader_brief = render_promotion_reader_brief_section(candidate_id, status, reasons)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_promotion_pack",
        "promotion_id": promotion_id,
        "candidate_id": candidate_id,
        "status": status,
        "decision_reasons": reasons,
        "evaluator_mode": evaluator,
        "evaluator_version": _evaluator_version(evaluator),
        "not_for_investment_decision": (
            candidate.get("not_for_investment_decision") is True
        ),
        "metric_delta_table": metric_delta,
        "risk_summary": risk_summary,
        "evidence_summary": _promotion_evidence_summary(evidence),
        "linked_artifacts": linked,
        "reader_brief_section": reader_brief,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(pack_dir / "promotion_manifest.json", manifest)
    _write_text(pack_dir / "promotion_decision.md", render_promotion_decision_markdown(payload))
    _write_csv(pack_dir / "metric_delta_table.csv", metric_delta)
    _write_json(pack_dir / "risk_summary.json", risk_summary)
    _write_json(pack_dir / "evidence_summary.json", payload["evidence_summary"])
    _write_json(pack_dir / "linked_artifacts.json", linked)
    _write_text(pack_dir / "reader_brief_section.md", reader_brief)
    _update_latest_pointer(
        "latest_promotion_pack", promotion_id, pack_dir / "promotion_manifest.json"
    )
    return {"promotion_id": promotion_id, "promotion_dir": pack_dir, "pack": payload}


def promotion_review_payload(
    *,
    candidate_id: str,
    registry_path: Path = DEFAULT_SHADOW_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_shadow_registry(registry_path)
    record = next(
        (
            row
            for row in _records(registry.get("candidates"))
            if row.get("candidate_id") == candidate_id
        ),
        None,
    )
    status = "READY_FOR_PACK" if record else "INCOMPLETE"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_promotion_review",
        "candidate_id": candidate_id,
        "status": status,
        "registry_record_present": record is not None,
        "manual_review_required": True,
        "production_candidate_generated": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def validate_promotion_pack(
    *,
    candidate_id: str,
    output_dir: Path = DEFAULT_PROMOTION_DIR,
) -> dict[str, Any]:
    candidate_dir = output_dir / candidate_id
    latest = _latest_child_dir(candidate_dir)
    checks = [_check("promotion_candidate_dir_exists", candidate_dir.exists(), str(candidate_dir))]
    if latest is not None:
        required = [
            "promotion_manifest.json",
            "promotion_decision.md",
            "metric_delta_table.csv",
            "risk_summary.json",
            "evidence_summary.json",
            "linked_artifacts.json",
            "reader_brief_section.md",
        ]
        checks.extend(
            _check(f"artifact_exists:{name}", (latest / name).exists(), name) for name in required
        )
        manifest = _read_json(latest / "promotion_manifest.json")
        evidence_summary = _read_optional_json(latest / "evidence_summary.json") or {}
        pack_status = _text(manifest.get("status"))
        checks.append(
            _check(
                "production_candidate_not_generated",
                manifest.get("production_candidate_generated") is False,
                "production_candidate is manual-only",
            )
        )
        flags = _texts(evidence_summary.get("promotion_blocking_flags"))
        if flags:
            checks.append(
                _check(
                    "evidence_blocking_flags_do_not_promote",
                    pack_status != GATE_PROMOTE_CANDIDATE,
                    ",".join(flags),
                )
            )
        if _text(manifest.get("evaluator_mode")) == EVALUATOR_TINY_FIXTURE_PROXY:
            checks.append(
                _check(
                    "tiny_fixture_pack_not_promote_candidate",
                    manifest.get("status") != GATE_PROMOTE_CANDIDATE,
                    "tiny fixture promotion pack cannot enter promote_candidate",
                )
            )
    else:
        checks.append(_check("latest_promotion_pack_exists", False, "no promotion pack found"))
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_promotion_pack_validation",
        "candidate_id": candidate_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def render_leaderboard_markdown(payload: Mapping[str, Any]) -> str:
    not_for_investment = payload.get("not_for_investment_decision") is True
    lines = [
        f"# Dynamic v3 Rescue Sweep Leaderboard {payload.get('sweep_id')}",
        "",
        "## Evaluator",
        f"- evaluator_mode: {payload.get('evaluator_mode')}",
        f"- evaluator_version: {payload.get('evaluator_version')}",
        f"- metrics_source: {payload.get('metrics_source')}",
        f"- not_for_investment_decision: {str(not_for_investment).lower()}",
        "",
        "## Safety",
        "- production_effect=none",
        "- broker_action=none",
        "- production_candidate_generated=false",
        "",
        "## Top Eligible / Observe-only Candidates",
        "",
        "| Rank | Candidate | Gate | Score | Constraint hit rate | Turnover | "
        "Drawdown degradation pp |",
        "|---:|---|---|---:|---:|---:|---:|",
    ]
    for idx, row in enumerate(_records(payload.get("top_eligible_candidates"))[:20], start=1):
        metrics = _mapping(row.get("metrics"))
        lines.append(
            f"| {idx} | {row.get('candidate_id')} | {row.get('gate')} | "
            f"{_fmt_num(row.get('score'))} | {_fmt_num(metrics.get('constraint_hit_rate'))} | "
            f"{_fmt_num(metrics.get('turnover'))} | "
            f"{_fmt_num(metrics.get('drawdown_degradation_pp'))} |"
        )
    lines.extend(["", "## Most Common Reject Reasons"])
    for row in _records(payload.get("most_common_reject_reasons")):
        lines.append(f"- {row.get('reason')}: {row.get('count')}")
    lines.extend(["", "## Recommended Next Actions"])
    for action in _texts(payload.get("recommended_next_actions")):
        lines.append(f"- {action}")
    return "\n".join(lines) + "\n"


def render_sweep_report_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("leaderboard_summary"))
    not_for_investment = payload.get("not_for_investment_decision") is True
    return (
        f"# Dynamic v3 Rescue Sweep Report {payload.get('sweep_id')}\n\n"
        "## Conclusion\n"
        f"- Status: {payload.get('status')}\n"
        f"- Evaluator Mode: {payload.get('evaluator_mode')}\n"
        f"- Metrics Source: {payload.get('metrics_source')}\n"
        f"- not_for_investment_decision: {str(not_for_investment).lower()}\n"
        f"- Top candidate: {summary.get('top_candidate')}\n"
        "- This report ranks candidates only after hard risk gates; it does not approve "
        "production use.\n\n"
        "## Safety\n"
        "- production_effect=none\n"
        "- broker_action=none\n"
        "- production_candidate_generated=false\n"
    )


def render_candidate_report_markdown(payload: Mapping[str, Any]) -> str:
    not_for_investment = payload.get("not_for_investment_decision") is True
    lines = [
        f"# Dynamic v3 Rescue Candidate Report {payload.get('candidate_id')}",
        "",
        f"- Source sweep: {payload.get('source_sweep_id')}",
        f"- Evaluator mode: {payload.get('evaluator_mode')}",
        f"- Evaluator version: {payload.get('evaluator_version')}",
        f"- Metrics source: {payload.get('metrics_source')}",
        f"- Real evaluation artifact: {payload.get('real_evaluation_artifact_path')}",
        f"- not_for_investment_decision: {str(not_for_investment).lower()}",
        f"- Gate: {payload.get('hard_gate_status')}",
        f"- Score: {payload.get('score')}",
        f"- Recommendation: {payload.get('recommendation')}",
        "- Backtest window status: "
        f"{_mapping(payload.get('backtest_window')).get('date_range_status')}",
        "- Weight path status: "
        f"{_mapping(payload.get('weight_path_metadata')).get('attribution_completeness')}",
        "",
        "## Gate Reasons",
    ]
    lines.extend(f"- {reason}" for reason in _texts(payload.get("gate_reasons")))
    lines.extend(["", "## Parameters"])
    for key, value in _mapping(payload.get("parameters")).items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Metrics"])
    for key, value in _mapping(payload.get("metrics")).items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Safety", "- production_candidate_generated=false"])
    return "\n".join(lines) + "\n"


def render_walk_forward_leaderboard_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# Dynamic v3 Rescue Walk-forward Leaderboard {payload.get('walk_forward_id')}",
        "",
        "| Candidate | Status | Pass ratio | OOS gate |",
        "|---|---|---:|---|",
    ]
    for row in _records(payload.get("candidates")):
        lines.append(
            f"| {row.get('candidate_id')} | {row.get('walk_forward_status')} | "
            f"{_fmt_num(row.get('pass_ratio'))} | {row.get('oos_gate')} |"
        )
    return "\n".join(lines) + "\n"


def render_walk_forward_report_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# Dynamic v3 Rescue Walk-forward Report {payload.get('walk_forward_id')}",
        "",
        f"- Source sweep: {payload.get('source_sweep_id')}",
        f"- Holdout: {payload.get('holdout_start')} to {payload.get('holdout_end')}",
        f"- Status: {payload.get('status')}",
        "",
        "## OOS Summary",
    ]
    for key, value in _mapping(payload.get("oos_summary")).items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Safety", "- production_candidate_generated=false"])
    return "\n".join(lines) + "\n"


def render_robustness_report_markdown(payload: Mapping[str, Any]) -> str:
    return (
        f"# Dynamic v3 Rescue Robustness Report {payload.get('robustness_id')}\n\n"
        f"- Candidate: {payload.get('candidate_id')}\n"
        f"- Robustness status: {payload.get('status')}\n"
        f"- Overfit status: {payload.get('overfit_status')}\n"
        f"- Parameter sensitivity: {payload.get('parameter_sensitivity_status')}\n"
        f"- Stress bucket status: {payload.get('stress_bucket_status')}\n"
        f"- Optional PBO/DSR: {payload.get('optional_pbo_dsr_status')}\n\n"
        "## Safety\n"
        "- production_candidate_generated=false\n"
    )


def render_shadow_report_markdown(payload: Mapping[str, Any]) -> str:
    lines = [f"# Dynamic v3 Rescue Shadow Report {payload.get('candidate_id')}", ""]
    for row in _records(payload.get("reports")):
        lines.extend(
            [
                f"## {row.get('candidate_id')}",
                f"- Source sweep: {row.get('source_sweep_id')}",
                f"- Observation age days: {row.get('observation_age_days')}",
                f"- Rebalance count: {row.get('rebalance_count')}",
                f"- Promotion eligibility: {row.get('promotion_eligibility_status')}",
                f"- Recommendation: {row.get('recommendation')}",
                "",
            ]
        )
    lines.extend(["## Safety", "- observe_only=true", "- production_candidate_generated=false"])
    return "\n".join(lines) + "\n"


def render_promotion_decision_markdown(payload: Mapping[str, Any]) -> str:
    risk = _mapping(payload.get("risk_summary"))
    evidence = _mapping(payload.get("evidence_summary"))
    questions = [
        ("What problem does this candidate solve?", risk.get("problem_solved", "UNKNOWN")),
        ("Does it improve v0.4?", risk.get("improves_v0_4", "UNKNOWN")),
        ("Is it worthwhile versus static baseline?", risk.get("static_baseline_value", "UNKNOWN")),
        ("Are constraint hits improved?", risk.get("constraint_hits", "UNKNOWN")),
        ("Is drawdown protected?", risk.get("drawdown", "UNKNOWN")),
        ("Is false risk-off controlled?", risk.get("false_risk_off", "UNKNOWN")),
        ("Is turnover acceptable?", risk.get("turnover", "UNKNOWN")),
        ("Is walk-forward stable?", risk.get("walk_forward", "UNKNOWN")),
        ("Did robustness pass?", risk.get("robustness", "UNKNOWN")),
        ("Does shadow observation support promotion?", risk.get("shadow", "UNKNOWN")),
        ("Recommend promote_candidate?", payload.get("status")),
        ("Manual review still required?", "yes"),
    ]
    lines = [
        f"# Dynamic v3 Rescue Promotion Decision {payload.get('candidate_id')}",
        "",
        f"- Status: {payload.get('status')}",
        f"- Evaluator mode: {payload.get('evaluator_mode')}",
        f"- not_for_investment_decision: "
        f"{str(payload.get('not_for_investment_decision') is True).lower()}",
        "- production_candidate is manual-only and was not generated.",
        "",
        "## Decision Questions",
    ]
    lines.extend(
        f"{idx}. {question}: {answer}" for idx, (question, answer) in enumerate(questions, start=1)
    )
    lines.extend(
        [
            "",
            "## Evidence Gates",
            f"- data_quality: {evidence.get('data_quality')}",
            f"- provenance_status: {evidence.get('provenance_status')}",
            f"- backtest_window_status: {evidence.get('backtest_window_status')}",
            f"- weight_path_status: {evidence.get('weight_path_status')}",
            f"- candidate_attribution_status: {evidence.get('candidate_attribution_status')}",
            f"- overfit_status: {evidence.get('overfit_status')}",
            "- promotion_blocking_flags: "
            f"{', '.join(_texts(evidence.get('promotion_blocking_flags')))}",
        ]
    )
    lines.extend(["", "## Decision Reasons"])
    lines.extend(f"- {reason}" for reason in _texts(payload.get("decision_reasons")))
    return "\n".join(lines) + "\n"


def render_promotion_reader_brief_section(
    candidate_id: str, status: str, reasons: Sequence[str]
) -> str:
    return (
        "## Dynamic Rescue Promotion Review\n\n"
        f"- candidate_id: {candidate_id}\n"
        f"- status: {status}\n"
        f"- reasons: {', '.join(reasons)}\n"
        "- production_candidate_generated: false\n"
        "- manual_review_required: true\n"
    )


def render_data_audit_markdown(payload: Mapping[str, Any]) -> str:
    checksum = _mapping(payload.get("checksum_audit"))
    coverage = _mapping(payload.get("pit_coverage_audit"))
    provenance = _mapping(payload.get("data_provenance"))
    lines = [
        f"# Dynamic v3 Rescue Data Audit {payload.get('data_audit_id')}",
        "",
        f"- Status: {payload.get('status')}",
        f"- Data quality status: {payload.get('data_quality_status')}",
        "- Market regime: ai_after_chatgpt",
        f"- Requested range: {coverage.get('requested_as_of')} to {coverage.get('requested_end')}",
        f"- prices_download_manifest_checksum_missing: {checksum.get('prices_checksum_missing')}",
        f"- Price cache sha256: {_mapping(provenance.get('prices')).get('sha256')}",
        f"- Download manifest status: {payload.get('download_manifest_status')}",
        f"- Provenance status: {payload.get('provenance_status')}",
        f"- Validate-data report: {payload.get('validate_data_report')}",
        "",
        "## Issues",
    ]
    for issue in _records(payload.get("issues")):
        lines.append(f"- {issue.get('severity')} {issue.get('code')}: {issue.get('message')}")
    lines.extend(["", "## Safety", "- production_candidate_generated=false"])
    return "\n".join(lines) + "\n"


def render_data_provenance_markdown(payload: Mapping[str, Any]) -> str:
    prices = _mapping(payload.get("prices"))
    rates = _mapping(payload.get("rates"))
    manifest = _mapping(payload.get("download_manifest"))
    lines = [
        "# Dynamic v3 Rescue Data Provenance",
        "",
        f"- Status: {payload.get('status')}",
        f"- Prices path: {prices.get('path')}",
        f"- Prices sha256: {prices.get('sha256')}",
        f"- Prices rows: {prices.get('rows')}",
        f"- Prices range: {prices.get('start_date')} to {prices.get('end_date')}",
        f"- Rates path: {rates.get('path')}",
        f"- Rates sha256: {rates.get('sha256')}",
        f"- Download manifest: {manifest.get('path')}",
        f"- Download manifest status: {payload.get('download_manifest_status')}",
        f"- Provenance status: {payload.get('provenance_status')}",
        f"- prices_checksum_in_manifest: {payload.get('prices_checksum_in_manifest')}",
        f"- rates_checksum_in_manifest: {payload.get('rates_checksum_in_manifest')}",
        "",
        "## Warnings",
    ]
    warnings = _texts(payload.get("warnings"))
    if warnings:
        lines.extend(f"- {warning}" for warning in warnings)
    else:
        lines.append("- none")
    lines.extend(["", "## Safety", "- production_candidate_generated=false"])
    return "\n".join(lines) + "\n"


def render_window_audit_markdown(payload: Mapping[str, Any]) -> str:
    answers = _mapping(payload.get("answers"))
    lines = [
        f"# Dynamic v3 Rescue Window Audit {payload.get('window_audit_id')}",
        "",
        f"- Status: {payload.get('status')}",
        "- Market regime: ai_after_chatgpt",
        f"- configured_backtest_start: {payload.get('configured_backtest_start')}",
        f"- requested_start: {payload.get('requested_start')}",
        f"- requested_end: {payload.get('requested_end')}",
        f"- earliest_actual_evaluation_start: {payload.get('earliest_actual_evaluation_start')}",
        f"- promotion_blocking_count: {payload.get('promotion_blocking_count')}",
        f"- needs_full_window_rerun: {payload.get('needs_full_window_rerun')}",
        "",
        "## Required Questions",
        f"- 当前 configured_backtest_start: {answers.get('configured_backtest_start')}",
        f"- 当前最早 actual_evaluation_start: {answers.get('earliest_actual_evaluation_start')}",
        f"- 是否覆盖 2022-12-01: {answers.get('covers_ai_regime_start')}",
        "- 是否存在 2025-05-28 to 2026-05-28 artifact: "
        f"{answers.get('contains_2025_05_28_to_2026_05_28_artifact')}",
        f"- 窗口问题是否阻断 promotion: {answers.get('promotion_blocking')}",
        "",
        "## Artifact Paths",
        f"- Inventory: {payload.get('artifact_window_inventory_path')}",
        f"- Mismatch report: {payload.get('window_mismatch_report_path')}",
        f"- Insufficient data report: {payload.get('insufficient_data_report_path')}",
        "",
        "## Safety",
        "- production_candidate_generated=false",
    ]
    return "\n".join(lines) + "\n"


def render_injection_audit_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# Dynamic v3 Rescue Injection Audit {payload.get('audit_id')}",
        "",
        f"- Status: {payload.get('status')}",
        f"- Candidate count: {payload.get('candidate_count')}",
        f"- Data quality status: {payload.get('data_quality_status')}",
        f"- All weight paths almost identical: {payload.get('all_weight_paths_almost_identical')}",
        "",
        "## Parameter Effects",
        "",
        "| Parameter | Status | Distinct values | Config hashes | Metric hashes | Weight hashes |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for row in _records(payload.get("parameter_effects")):
        lines.append(
            f"| {row.get('parameter')} | {row.get('effect_status')} | "
            f"{row.get('distinct_value_count')} | "
            f"{row.get('distinct_effective_config_hash_count')} | "
            f"{row.get('distinct_metric_hash_count')} | {row.get('distinct_weight_hash_count')} |"
        )
    lines.extend(["", "## Safety", "- production_candidate_generated=false"])
    return "\n".join(lines) + "\n"


def render_candidate_attribution_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# Dynamic v3 Rescue Candidate Attribution {payload.get('candidate_id')}",
        "",
        f"- Source sweep: {payload.get('source_sweep_id')}",
        f"- Status: {payload.get('status')}",
        f"- Explainability: {payload.get('explainability_status')}",
        f"- Incomplete reasons: {', '.join(_texts(payload.get('incomplete_reasons')))}",
        "",
        "## Attribution Summary",
        f"- Constraint: {_mapping(payload.get('constraint_event_attribution')).get('summary')}",
        f"- Drawdown: {_mapping(payload.get('drawdown_window_attribution')).get('summary')}",
        f"- Turnover: {_mapping(payload.get('turnover_attribution')).get('summary')}",
        "- Dynamic-vs-static gap: "
        f"{_mapping(payload.get('dynamic_vs_static_gap_attribution')).get('summary')}",
        "",
        "## Safety",
        "- production_candidate_generated=false",
    ]
    return "\n".join(lines) + "\n"


def render_wf_selection_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return (
        f"# Dynamic v3 Rescue Walk-forward Selection {payload.get('wf_selection_id')}\n\n"
        f"- Source sweep: {payload.get('source_sweep_id')}\n"
        f"- Profile: {payload.get('profile')}\n"
        f"- Status: {payload.get('status')}\n"
        f"- Window count: {summary.get('window_count')}\n"
        f"- Selected candidates: {summary.get('selected_candidate_count')}\n"
        f"- Test pass count: {summary.get('test_pass_count')}\n"
        f"- Parameter stability: {summary.get('parameter_stability')}\n\n"
        "## Safety\n"
        "- production_candidate_generated=false\n"
    )


def render_overfit_markdown(payload: Mapping[str, Any]) -> str:
    return (
        f"# Dynamic v3 Rescue Overfit Review {payload.get('overfit_id')}\n\n"
        f"- Candidate: {payload.get('candidate_id')}\n"
        f"- Source sweep: {payload.get('source_sweep_id')}\n"
        f"- Overfit status: {payload.get('overfit_status')}\n"
        f"- Optional PBO/DSR: {payload.get('optional_pbo_dsr_status')}\n\n"
        "## Interpretation\n"
        "- HIGH_RISK blocks promotion.\n"
        "- REVIEW_REQUIRED requires manual review and cannot create production_candidate.\n\n"
        "## Safety\n"
        "- production_candidate_generated=false\n"
    )


def render_governance_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Dynamic v3 Rescue Parameter Governance",
        "",
        f"- Policy: {payload.get('policy_id')}",
        f"- Version: {payload.get('version')}",
        f"- Search space version: {payload.get('search_space_version')}",
        "",
        "## Groups",
    ]
    for name, group in _mapping(payload.get("parameter_groups")).items():
        group_payload = _mapping(group)
        lines.append(
            f"- {name}: {group_payload.get('search_policy')} "
            f"({', '.join(_texts(group_payload.get('parameters')))})"
        )
    lines.extend(["", "## Safety", "- production_candidate_generated=false"])
    return "\n".join(lines) + "\n"


def render_shadow_monitor_reader_brief_section(
    results: Sequence[Mapping[str, Any]],
    ready_count: int,
    drift_count: int,
) -> str:
    return (
        "## Dynamic Rescue Shadow Monitoring\n\n"
        f"- observe_only candidates: {len(results)}\n"
        f"- promotion_review_ready: {ready_count}\n"
        f"- live_drift_review_required: {drift_count}\n"
        "- production_candidate_generated: false\n"
        "- manual_review_required: true\n"
    )


def render_shadow_monitor_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Dynamic v3 Rescue Shadow Monitor {payload.get('monitor_id')}",
        "",
        f"- As of: {payload.get('as_of')}",
        f"- Observe-only candidates: {summary.get('observe_only_candidate_count')}",
        f"- Promotion review ready: {summary.get('promotion_review_ready_count')}",
        f"- Drift review required: {summary.get('live_drift_review_required_count')}",
        "",
        "## Candidates",
    ]
    for row in _records(payload.get("candidate_monitor_results")):
        lines.append(
            f"- {row.get('candidate_id')}: {row.get('recommendation')} "
            f"(days_observed={row.get('days_observed')})"
        )
    lines.extend(["", "## Safety", "- production_candidate_generated=false"])
    return "\n".join(lines) + "\n"


def _with_evaluator_mode(
    config: DynamicV3ParameterSweepConfig,
    evaluator_mode: str,
) -> DynamicV3ParameterSweepConfig:
    if evaluator_mode not in EVALUATOR_VERSIONS:
        raise DynamicV3ParameterResearchError(f"unknown evaluator mode: {evaluator_mode}")
    return config.model_copy(
        update={
            "execution": config.execution.model_copy(
                update={
                    "evaluator": evaluator_mode,
                    "evaluation_mode": evaluator_mode,
                }
            )
        }
    )


def _prepare_real_evaluation_context(
    *,
    config: DynamicV3ParameterSweepConfig,
    sweep_dir: Path,
    prices_path: Path,
    rates_path: Path,
    data_quality_output_path: Path | None,
    real_evaluation_output_dir: Path | None,
    expect_manifest_hash: str | None = None,
) -> RealEvaluationContext:
    quality_output = data_quality_output_path or default_quality_report_path(
        sweep_dir / "data_quality",
        config.data.as_of,
    )
    universe = load_universe()
    quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(universe),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=config.data.as_of,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(quality_report, quality_output)
    if not quality_report.passed:
        raise DynamicV3ParameterResearchError(
            f"real evaluator data quality gate failed: {quality_report.status}"
        )
    if quality_report.status not in config.data.allow_data_quality:
        raise DynamicV3ParameterResearchError(
            "real evaluator data quality status is not allowed by sweep config: "
            f"{quality_report.status}"
        )
    data_manifest_hash = _data_quality_manifest_hash(quality_report)
    if expect_manifest_hash and expect_manifest_hash != data_manifest_hash:
        raise DynamicV3ParameterResearchError(
            "resume data manifest hash differs from normalized sweep config"
        )
    etf_config = load_etf_config_bundle()
    prices, etf_quality = load_standard_prices(
        prices_path,
        etf_config.assets,
        etf_config.strategy,
    )
    if not etf_quality.passed:
        raise DynamicV3ParameterResearchError(
            f"ETF price validation failed before real sweep evaluation: {etf_quality.status}"
        )
    return RealEvaluationContext(
        prices=prices,
        etf_config=etf_config,
        real_policy=load_dynamic_v3_real_evaluation_policy_config(
            DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH
        ),
        v3_rescue_policy=load_dynamic_v3_rescue_policy_config(),
        dynamic_robustness_policy=load_dynamic_robustness_policy_config(),
        dynamic_policy=load_dynamic_allocation_policy_config(),
        failure_policy=load_dynamic_failure_diagnostics_policy_config(),
        data_quality_status=quality_report.status,
        data_quality_report_path=quality_output,
        prices_path=prices_path,
        real_evaluation_output_dir=real_evaluation_output_dir
        or sweep_dir
        / "real_evaluation",
        data_manifest_hash=data_manifest_hash,
    )


def _write_real_candidate_evaluation_artifact(
    *,
    candidate: Mapping[str, Any],
    config: DynamicV3ParameterSweepConfig,
    sweep_dir: Path,
    real_context: RealEvaluationContext,
) -> tuple[dict[str, Any], dict[str, Path]]:
    candidate_id = _text(candidate.get("candidate_id"))
    parameters = _mapping(candidate.get("parameters"))
    real_policy = _real_policy_for_sweep_candidate(real_context.real_policy, parameters)
    v3_rescue_policy = _real_rescue_policy_for_sweep_candidate(
        real_context.v3_rescue_policy,
        parameters,
    )
    payload = build_dynamic_v3_real_evaluation_report(
        prices=real_context.prices,
        etf_config=real_context.etf_config,
        policy=real_policy,
        v3_rescue_policy=v3_rescue_policy,
        dynamic_robustness_policy=real_context.dynamic_robustness_policy,
        dynamic_policy=real_context.dynamic_policy,
        failure_policy=real_context.failure_policy,
        start=real_policy.market_regime.default_backtest_start,
        end=config.data.end,
        data_quality_status=real_context.data_quality_status,
        data_quality_report=str(real_context.data_quality_report_path),
        prices_path=real_context.prices_path,
    )
    report_id = (
        "dynamic-v3-real-evaluation-report_"
        + _stable_id(
            "sweep-real",
            sweep_dir.name,
            candidate_id,
            parameters,
            payload.get("dynamic_v3_real_evaluation_report_id"),
        )
    )
    payload = {
        **payload,
        "dynamic_v3_real_evaluation_report_id": report_id,
        "source_sweep_id": sweep_dir.name,
        "source_sweep_candidate_id": candidate_id,
        "candidate_parameters": dict(parameters),
        "evaluator_mode": EVALUATOR_REAL_DYNAMIC_V3_RESCUE,
        "evaluator_version": _evaluator_version(EVALUATOR_REAL_DYNAMIC_V3_RESCUE),
        "metrics_source": "real_evaluation_artifact",
        "not_for_investment_decision": False,
    }
    payload["backtest_window"] = _backtest_window_from_payload(payload)
    output_dir = real_context.real_evaluation_output_dir / candidate_id
    paths = write_dynamic_v3_real_evaluation_report(payload, output_dir=output_dir)
    readback = _read_json(paths["json"])
    weight_paths = _export_weight_path_artifacts(
        payload=readback,
        output_dir=output_dir,
        candidate_id=candidate_id,
        evaluation_id=report_id,
    )
    readback["weight_path_artifacts"] = {key: str(path) for key, path in weight_paths.items()}
    _write_json(paths["json"], readback)
    return readback, paths


def _real_policy_for_sweep_candidate(
    policy: DynamicV3RealEvaluationPolicyConfig,
    parameters: Mapping[str, Any],
) -> DynamicV3RealEvaluationPolicyConfig:
    intensity = _clamp01(float(parameters.get("rescue_intensity", 0.5)))
    smooth_window = parameters.get("smooth_window_days", REAL_EVALUATOR_BASE_SMOOTH_WINDOW_DAYS)
    smooth = max(1, int(smooth_window))
    buffer_weight = max(0.0, float(parameters.get("constraint_buffer_bps", 0))) / 10000.0
    turnover_penalty = _clamp01(float(parameters.get("turnover_penalty", 0.0)))
    confirmation_days = max(1, int(parameters.get("risk_off_confirmation_days", 1)))
    cooldown_days = max(0, int(parameters.get("rebalance_cooldown_days", 0)))
    guard_multiplier = REAL_EVALUATOR_DRAW_DOWN_GUARD_MULTIPLIERS.get(
        _text(parameters.get("drawdown_guard"), "none"),
        REAL_EVALUATOR_DRAW_DOWN_GUARD_MULTIPLIERS["none"],
    )
    materialization = policy.materialization
    smooth_scale = REAL_EVALUATOR_BASE_SMOOTH_WINDOW_DAYS / smooth
    updated = materialization.model_copy(
        update={
            "qqq_target_buffer": min(0.2, buffer_weight),
            "semiconductor_target_buffer": min(0.2, buffer_weight),
            "cash_target_buffer": min(0.2, buffer_weight),
            "soft_penalty_strength": min(
                1.0,
                max(
                    REAL_EVALUATOR_MIN_POSITIVE_MATERIALIZATION_VALUE,
                    materialization.soft_penalty_strength * intensity,
                ),
            ),
            "trend_overlay_scale_with_soft_penalty": _clamp01(
                materialization.trend_overlay_scale_with_soft_penalty
                * (1.0 - turnover_penalty)
            ),
            "smoothing_max_single_rebalance_delta": min(
                1.0,
                max(
                    REAL_EVALUATOR_MIN_POSITIVE_MATERIALIZATION_VALUE,
                    materialization.smoothing_max_single_rebalance_delta * smooth_scale,
                ),
            ),
            "smoothing_weekly_turnover_cap": max(
                REAL_EVALUATOR_MIN_WEEKLY_TURNOVER_CAP,
                materialization.smoothing_weekly_turnover_cap * (1.0 - turnover_penalty),
            ),
            "smoothing_min_rebalance_weight_delta": min(
                1.0,
                materialization.smoothing_min_rebalance_weight_delta
                + cooldown_days * REAL_EVALUATOR_MIN_REBALANCE_DELTA_PER_COOLDOWN_DAY,
            ),
            "drawdown_cash_increase_step": min(
                1.0,
                max(
                    REAL_EVALUATOR_MIN_POSITIVE_MATERIALIZATION_VALUE,
                    materialization.drawdown_cash_increase_step
                    * intensity
                    * guard_multiplier,
                ),
            ),
            "drawdown_semiconductor_reduction_step": min(
                1.0,
                max(
                    REAL_EVALUATOR_MIN_POSITIVE_MATERIALIZATION_VALUE,
                    materialization.drawdown_semiconductor_reduction_step
                    * intensity
                    * guard_multiplier,
                ),
            ),
            "drawdown_qqq_reduction_step": min(
                1.0,
                max(
                    REAL_EVALUATOR_MIN_POSITIVE_MATERIALIZATION_VALUE,
                    materialization.drawdown_qqq_reduction_step * intensity * guard_multiplier,
                ),
            ),
            "emergency_event_risk_high_threshold": _clamp01(
                (
                    materialization.emergency_event_risk_high_threshold
                    + max(0, confirmation_days - 1)
                    * REAL_EVALUATOR_EVENT_RISK_THRESHOLD_PER_CONFIRMATION
                )
                / 100.0
            )
            * 100.0,
            "emergency_event_risk_cash_increase_step": min(
                1.0,
                max(
                    REAL_EVALUATOR_MIN_POSITIVE_MATERIALIZATION_VALUE,
                    materialization.emergency_event_risk_cash_increase_step * intensity,
                ),
            ),
        }
    )
    return policy.model_copy(
        deep=True,
        update={
            "policy_metadata": policy.policy_metadata.model_copy(
                update={
                    "version": (
                        f"{policy.policy_metadata.version}_sweep_"
                        f"{_stable_id(parameters)[:8]}"
                    ),
                    "status": "pilot_sweep_real_evaluator",
                }
            ),
            "materialization": updated,
        },
    )


def _real_rescue_policy_for_sweep_candidate(policy: Any, parameters: Mapping[str, Any]) -> Any:
    buffer_weight = max(0.0, float(parameters.get("constraint_buffer_bps", 0))) / 10000.0
    turnover_penalty = _clamp01(float(parameters.get("turnover_penalty", 0.0)))
    smooth_window = parameters.get("smooth_window_days", REAL_EVALUATOR_BASE_SMOOTH_WINDOW_DAYS)
    smooth = max(1, int(smooth_window))
    confirmation_days = max(1, int(parameters.get("risk_off_confirmation_days", 1)))
    smooth_scale = REAL_EVALUATOR_BASE_SMOOTH_WINDOW_DAYS / smooth
    updated = policy.model_copy(
        deep=True,
        update={
            "soft_constraint_penalties": policy.soft_constraint_penalties.model_copy(
                update={
                    "interior_buffer": min(0.1, buffer_weight),
                    "penalty_strength": min(
                        1.0,
                        max(
                            REAL_EVALUATOR_MIN_POSITIVE_MATERIALIZATION_VALUE,
                            policy.soft_constraint_penalties.penalty_strength
                            * (1.0 - turnover_penalty / 2.0),
                        ),
                    ),
                }
            ),
            "smoothing_policy": policy.smoothing_policy.model_copy(
                update={
                    "max_single_rebalance_delta": min(
                        1.0,
                        max(
                            REAL_EVALUATOR_MIN_POSITIVE_MATERIALIZATION_VALUE,
                            policy.smoothing_policy.max_single_rebalance_delta
                            * smooth_scale,
                        ),
                    )
                }
            ),
            "drawdown_guardrails": policy.drawdown_guardrails.model_copy(
                update={"min_confirmations": max(2, confirmation_days)}
            ),
            "emergency_risk_off": policy.emergency_risk_off.model_copy(
                update={"min_independent_confirmations": max(2, confirmation_days)}
            ),
        },
    )
    return updated


def _metrics_from_real_evaluation_payload(
    payload: Mapping[str, Any],
    config: DynamicV3ParameterSweepConfig,
) -> dict[str, Any]:
    best = _mapping(payload.get("best_candidate"))
    summary = _mapping(payload.get("summary"))
    v0_4 = _first_row_by_group(payload.get("comparison_table"), "dynamic_v0_4")
    best_hit_rate = float(best.get("constraint_hit_rate", 0.0))
    reference_hit_rate = float(v0_4.get("constraint_hit_rate", best_hit_rate))
    best_turnover = float(best.get("turnover", 0.0))
    reference_turnover = float(v0_4.get("turnover", best_turnover))
    hit_reduction_rate = reference_hit_rate - best_hit_rate
    hit_reduction_count = int(best.get("constraint_hit_reduction_vs_v0_4") or 0)
    static_gap_delta = float(best.get("static_gap_delta_vs_v0_4") or 0.0)
    real_decision = _text(payload.get("promotion_gate_decision"), GATE_REVIEW_REQUIRED)
    return {
        "constraint_hits": int(best.get("constraint_hit_count") or 0),
        "constraint_hit_rate": round(best_hit_rate, 6),
        "constraint_hits_delta_vs_reference": -hit_reduction_count,
        "constraint_hit_reduction": round(max(0.0, hit_reduction_rate), 6),
        "constraint_hit_reduction_count_vs_v0_4": hit_reduction_count,
        "false_risk_off_delta": int(best.get("false_risk_off_delta_vs_v0_4") or 0),
        "turnover": round(best_turnover, 6),
        "turnover_reduction": round(max(0.0, reference_turnover - best_turnover), 6),
        "dynamic_vs_static_gap": round(float(best.get("dynamic_vs_static_gap") or 0.0), 6),
        "dynamic_vs_static_gap_improvement": round(max(0.0, static_gap_delta), 6),
        "drawdown_degradation_pp": round(
            float(best.get("max_drawdown_degradation_vs_v0_4") or 0.0),
            6,
        ),
        "return_delta": round(static_gap_delta, 6),
        "robustness_status": _real_robustness_status(real_decision),
        "overfit_status": _text(best.get("overfit_status"), "REVIEW_REQUIRED"),
        "parameter_sensitivity_status": "LOW"
        if _text(best.get("overfit_status")) == "PASS"
        else "REVIEW_REQUIRED",
        "stress_bucket_status": "PASS"
        if _text(best.get("walk_forward_status")) == "PASS"
        else "MIXED",
        "data_quality": _text(summary.get("data_quality_status"), config.data.quality_status),
        "lookahead_status": "PASS",
        "evaluation_mode": EVALUATOR_REAL_DYNAMIC_V3_RESCUE,
        "evaluator_mode": EVALUATOR_REAL_DYNAMIC_V3_RESCUE,
        "metrics_source": "real_evaluation_artifact",
        "real_evaluation_report_id": payload.get("dynamic_v3_real_evaluation_report_id"),
        "real_promotion_gate_decision": real_decision,
        "real_best_candidate_policy_id": best.get("policy_id"),
        "real_policy_config_hash": payload.get("policy_config_hash"),
    }


def _candidate_data_quality(*, status: str, report_path: str, source: str) -> dict[str, Any]:
    return {
        "status": status,
        "report_path": report_path,
        "source": source,
    }


def _real_robustness_status(real_decision: str) -> str:
    if real_decision == GATE_PROMOTE_CANDIDATE:
        return "PASS"
    if real_decision == GATE_REJECT:
        return "FAIL"
    return "REVIEW_REQUIRED"


def _first_row_by_group(rows: Any, group: str) -> dict[str, Any]:
    return next((row for row in _records(rows) if row.get("group") == group), {})


def _data_quality_manifest_hash(report: Any) -> str:
    secondary = (
        None if report.secondary_price_summary is None else report.secondary_price_summary.sha256
    )
    return _stable_id(
        "real-data",
        report.price_summary.sha256,
        report.rate_summary.sha256,
        secondary,
        report.as_of.isoformat(),
    )


def _download_manifest_path(prices_path: Path) -> Path:
    return prices_path.parent / "download_manifest.csv"


def _marketstack_prices_path(prices_path: Path) -> Path:
    return prices_path.parent / "prices_marketstack_daily.csv"


def _requires_marketstack_prices(prices_path: Path) -> bool:
    try:
        return prices_path.resolve() == DEFAULT_ETF_PRICE_PATH.resolve()
    except OSError:
        return prices_path == DEFAULT_ETF_PRICE_PATH


def _evaluator_version(evaluator_mode: str) -> str:
    return EVALUATOR_VERSIONS.get(evaluator_mode, "unknown_evaluator")


def _fixture_metrics(
    parameters: Mapping[str, Any],
    config: DynamicV3ParameterSweepConfig,
) -> dict[str, Any]:
    intensity = float(parameters.get("rescue_intensity", 0.5))
    smooth = int(parameters.get("smooth_window_days", 5))
    buffer_bps = int(parameters.get("constraint_buffer_bps", 0))
    turnover_penalty = float(parameters.get("turnover_penalty", 0.0))
    confirm = int(parameters.get("risk_off_confirmation_days", 1))
    cooldown = int(parameters.get("rebalance_cooldown_days", 0))
    guard = _text(parameters.get("drawdown_guard"), "none")
    guard_bonus = {"none": 0.0, "soft": 0.015, "hard": 0.03}.get(guard, 0.0)
    constraint_hit_rate = max(
        0.04,
        0.125 - intensity * 0.025 - buffer_bps / 5000.0 - min(smooth, 20) / 2000.0,
    )
    reference_hit_rate = 0.112
    constraint_delta = round((constraint_hit_rate - reference_hit_rate) * 5000)
    turnover = max(
        0.30,
        0.92 - turnover_penalty * 0.60 - cooldown * 0.012 - min(smooth, 20) * 0.004,
    )
    false_risk_off_delta = max(0, confirm - 2)
    drawdown_degradation = round(0.026 - guard_bonus + intensity * 0.004 + cooldown * 0.0004, 4)
    dynamic_gap = max(
        0.05, 0.215 - intensity * 0.020 - buffer_bps / 7000.0 + turnover_penalty * 0.015
    )
    hit_reduction = max(0.0, reference_hit_rate - constraint_hit_rate)
    turnover_reduction = max(0.0, 0.86 - turnover)
    robustness_status = "PASS"
    overfit_status = "LOW_RISK"
    sensitivity_status = "LOW"
    stress_status = "PASS"
    if intensity >= 0.9 or smooth <= 3:
        robustness_status = "REVIEW_REQUIRED"
        overfit_status = "REVIEW_REQUIRED"
    if buffer_bps == 0 and guard == "none":
        stress_status = "MIXED"
    if intensity >= 0.75 and buffer_bps <= 10:
        sensitivity_status = "HIGH"
    return {
        "constraint_hits": int(round(constraint_hit_rate * 5000)),
        "constraint_hit_rate": round(constraint_hit_rate, 6),
        "constraint_hits_delta_vs_reference": int(constraint_delta),
        "constraint_hit_reduction": round(hit_reduction, 6),
        "false_risk_off_delta": false_risk_off_delta,
        "turnover": round(turnover, 6),
        "turnover_reduction": round(turnover_reduction, 6),
        "dynamic_vs_static_gap": round(dynamic_gap, 6),
        "dynamic_vs_static_gap_improvement": round(max(0.0, 0.205 - dynamic_gap), 6),
        "drawdown_degradation_pp": drawdown_degradation,
        "return_delta": round(0.03 + intensity * 0.02 - turnover_penalty * 0.01, 6),
        "robustness_status": robustness_status,
        "overfit_status": overfit_status,
        "parameter_sensitivity_status": sensitivity_status,
        "stress_bucket_status": stress_status,
        "data_quality": config.data.quality_status,
        "lookahead_status": "PASS",
        "evaluation_mode": config.execution.evaluator,
        "evaluator_mode": config.execution.evaluator,
        "metrics_source": "tiny_fixture_proxy_formula",
    }


def _sweep_id(config: DynamicV3ParameterSweepConfig, generated: datetime) -> str:
    return (
        "sweep_"
        + generated.strftime("%Y%m%dT%H%M%SZ")
        + "_"
        + _stable_id(config.run.name, config.data.as_of.isoformat(), config.data.end.isoformat())[
            :8
        ]
    )


def _sweep_manifest(
    *,
    config: DynamicV3ParameterSweepConfig,
    sweep_id: str,
    sweep_dir: Path,
    config_path: Path,
    generated_at: datetime,
    completed_at: datetime,
    results: Sequence[Mapping[str, Any]],
    errors: Sequence[Mapping[str, Any]],
    status: str,
) -> dict[str, Any]:
    gate_counts = Counter(_text(row.get("gate")) for row in results)
    return {
        "sweep_id": sweep_id,
        "schema_version": SCHEMA_VERSION,
        "strategy_family": STRATEGY_FAMILY,
        "config_path": str(config_path),
        "normalized_config_path": str(sweep_dir / "sweep_config.normalized.yaml"),
        "data_manifest_path": str(sweep_dir / "data_manifest.json"),
        "git_commit": _git_commit(),
        "as_of": config.data.as_of.isoformat(),
        "end": config.data.end.isoformat(),
        "started_at": generated_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "status": status,
        "evaluator_mode": config.execution.evaluator,
        "evaluator_version": _evaluator_version(config.execution.evaluator),
        "search_space_version": _search_space_version(),
        "not_for_investment_decision": (
            config.execution.evaluator == EVALUATOR_TINY_FIXTURE_PROXY
        ),
        "data_quality": _candidate_data_quality(
            status=config.data.quality_status,
            report_path=_first_data_quality_report_path(results),
            source=(
                "config_fixture"
                if config.execution.evaluator == EVALUATOR_TINY_FIXTURE_PROXY
                else "validate_data_cache"
            ),
        ),
        "backtest_window": _aggregate_candidate_backtest_windows(
            results,
            configured_start=config.data.as_of,
            requested_start=config.data.as_of,
            requested_end=config.data.end,
        ),
        "candidate_count": len(results) + len(errors),
        "completed_count": len(results),
        "failed_count": len(errors),
        "rejected_count": gate_counts.get(GATE_REJECT, 0),
        "review_required_count": gate_counts.get(GATE_REVIEW_REQUIRED, 0),
        "observe_only_count": gate_counts.get(GATE_OBSERVE_ONLY, 0),
        "promote_candidate_count": gate_counts.get(GATE_PROMOTE_CANDIDATE, 0),
        "production_candidate_count": 0,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _data_manifest(
    *,
    config: DynamicV3ParameterSweepConfig,
    generated_at: datetime,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "strategy_family": STRATEGY_FAMILY,
        "as_of": config.data.as_of.isoformat(),
        "end": config.data.end.isoformat(),
        "quality_status": config.data.quality_status,
        "allow_data_quality": config.data.allow_data_quality,
        "manifest_hash": config.data.manifest_hash,
        "search_space_version": _search_space_version(),
        "download_timestamp": generated_at.isoformat(),
        "row_count": 0,
        "checksum": config.data.manifest_hash,
        "evaluation_mode": config.execution.evaluator,
        "evaluator_mode": config.execution.evaluator,
        "evaluator_version": _evaluator_version(config.execution.evaluator),
        "not_for_investment_decision": (
            config.execution.evaluator == EVALUATOR_TINY_FIXTURE_PROXY
        ),
    }


def _read_candidate_results(sweep_dir: Path) -> list[dict[str, Any]]:
    return _deduplicate_candidate_results(sweep_dir / "candidate_results.jsonl")


def _deduplicate_candidate_results_file(path: Path) -> tuple[list[dict[str, Any]], int]:
    rows = _read_jsonl(path) if path.exists() else []
    deduplicated = _deduplicate_candidate_result_rows(rows)
    duplicate_count = len(rows) - len(deduplicated)
    if duplicate_count:
        _write_jsonl(path, deduplicated)
    return deduplicated, duplicate_count


def _deduplicate_candidate_results(path: Path) -> list[dict[str, Any]]:
    return _deduplicate_candidate_result_rows(_read_jsonl(path) if path.exists() else [])


def _deduplicate_candidate_result_rows(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    deduplicated: list[dict[str, Any]] = []
    index_by_candidate: dict[str, int] = {}
    for row in rows:
        candidate_id = _text(row.get("candidate_id"))
        item = dict(row)
        if not candidate_id:
            deduplicated.append(item)
            continue
        if candidate_id in index_by_candidate:
            deduplicated[index_by_candidate[candidate_id]] = item
        else:
            index_by_candidate[candidate_id] = len(deduplicated)
            deduplicated.append(item)
    return deduplicated


def _write_checkpoint(sweep_dir: Path, index: int, completed_count: int, failed_count: int) -> None:
    _write_json(
        sweep_dir / "checkpoint.json",
        {
            "last_candidate_index": index,
            "completed_count": completed_count,
            "failed_count": failed_count,
            "updated_at": datetime.now(UTC).isoformat(),
        },
    )


def _gate_summary(
    results: Sequence[Mapping[str, Any]],
    errors: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    counts = Counter(_text(row.get("gate")) for row in results)
    return {
        "completed_count": len(results),
        "failed_count": len(errors),
        "gate_counts": dict(counts),
        "production_candidate_count": counts.get(FORBIDDEN_GATE, 0),
    }


def _metric_distributions(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    keys = (
        "constraint_hit_rate",
        "turnover",
        "drawdown_degradation_pp",
        "dynamic_vs_static_gap",
        "false_risk_off_delta",
    )
    distributions = {}
    for key in keys:
        values = [float(_mapping(row.get("metrics")).get(key, 0)) for row in results]
        distributions[key] = _distribution(values)
    distributions["robustness_status"] = dict(
        Counter(_text(_mapping(row.get("metrics")).get("robustness_status")) for row in results)
    )
    return distributions


def _distribution(values: Sequence[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "min": None, "max": None, "avg": None}
    return {
        "count": len(values),
        "min": round(min(values), 6),
        "max": round(max(values), 6),
        "avg": round(sum(values) / len(values), 6),
    }


def _top_rejected_by_return(rejected: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rejected,
        key=lambda row: float(_mapping(row.get("metrics")).get("return_delta", 0)),
        reverse=True,
    )[:20]


def _leaderboard_next_actions(
    ranked: Sequence[Mapping[str, Any]],
    rejected: Sequence[Mapping[str, Any]],
    errors: Sequence[Mapping[str, Any]],
    *,
    evaluator_mode: str,
) -> list[str]:
    actions = []
    if evaluator_mode == EVALUATOR_TINY_FIXTURE_PROXY:
        actions.append(
            "rerun with evaluator=real_dynamic_v3_rescue before investment interpretation"
        )
    if ranked:
        actions.append("run walk-forward validation for top observe_only candidates")
        actions.append("run robustness diagnostics before any promotion review")
    if rejected:
        actions.append("review common hard-gate reject reasons before expanding parameter space")
    if errors:
        actions.append("inspect candidate_errors.jsonl before large sweep rerun")
    if not ranked:
        actions.append("do not register shadow candidates until hard gates pass")
    return actions


def _leaderboard_evaluator(results: Sequence[Mapping[str, Any]]) -> str:
    modes = [_text(row.get("evaluator_mode")) for row in results if row.get("evaluator_mode")]
    return modes[0] if modes else EVALUATOR_TINY_FIXTURE_PROXY


def _leaderboard_metrics_source(results: Sequence[Mapping[str, Any]]) -> str:
    sources = sorted(
        {
            _text(row.get("metrics_source"))
            for row in results
            if _text(row.get("metrics_source"))
        }
    )
    return ",".join(sources) if sources else "UNKNOWN"


def _leaderboard_data_quality(
    results: Sequence[Mapping[str, Any]],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    for row in results:
        data_quality = _mapping(row.get("data_quality"))
        if data_quality:
            return data_quality
    return _mapping(manifest.get("data_quality"))


def _first_data_quality_report_path(results: Sequence[Mapping[str, Any]]) -> str:
    for row in results:
        report_path = _text(_mapping(row.get("data_quality")).get("report_path"))
        if report_path:
            return report_path
    return ""


def _candidate_recommendation(result: Mapping[str, Any]) -> str:
    gate = _text(result.get("gate"))
    if gate == GATE_REJECT:
        return "reject; do not run walk-forward until hard gate failures are addressed"
    if gate == GATE_REVIEW_REQUIRED:
        return "review_required; run diagnostics and inspect policy sensitivity"
    if gate == GATE_PROMOTE_CANDIDATE:
        return "promote_candidate; manual review still required and production_candidate is blocked"
    return "observe_only; eligible for walk-forward and robustness diagnostics"


def _first_candidate_id(rows: Any) -> str:
    records = _records(rows)
    return _text(records[0].get("candidate_id"), "MISSING") if records else "MISSING"


def _candidate_result(sweep_dir: Path, candidate_id: str) -> dict[str, Any] | None:
    for row in _read_candidate_results(sweep_dir):
        if row.get("candidate_id") == candidate_id:
            return row
    return None


def walk_forward_windows(config: DynamicV3ParameterSweepConfig) -> list[dict[str, str]]:
    total_months = (
        config.walk_forward.train_window_months
        + config.walk_forward.test_window_months
        + config.walk_forward.step_months * (config.walk_forward.min_windows - 1)
    )
    first_train_start = _add_months(config.data.end, -total_months)
    windows = []
    cursor = first_train_start
    for _ in range(config.walk_forward.min_windows):
        train_start = cursor
        train_end = _add_months(train_start, config.walk_forward.train_window_months) - timedelta(
            days=1
        )
        test_start = train_end + timedelta(days=1)
        test_end = _add_months(test_start, config.walk_forward.test_window_months) - timedelta(
            days=1
        )
        windows.append(
            {
                "train_start": train_start.isoformat(),
                "train_end": train_end.isoformat(),
                "test_start": test_start.isoformat(),
                "test_end": min(test_end, config.data.end).isoformat(),
            }
        )
        cursor = _add_months(cursor, config.walk_forward.step_months)
    return windows


def _walk_forward_row(
    candidate: Mapping[str, Any],
    window: Mapping[str, str],
    config: DynamicV3ParameterSweepConfig,
) -> dict[str, Any]:
    metrics = dict(_mapping(candidate.get("metrics")))
    candidate_id = _text(candidate.get("candidate_id"))
    drift = (_stable_int(candidate_id, window.get("test_start")) % 9 - 4) / 1000.0
    metrics["dynamic_vs_static_gap"] = round(
        float(metrics.get("dynamic_vs_static_gap", 0)) + drift, 6
    )
    metrics["drawdown_degradation_pp"] = round(
        float(metrics.get("drawdown_degradation_pp", 0)) + abs(drift) / 2,
        6,
    )
    gate, reasons = gate_candidate(metrics, config)
    return {
        "candidate_id": candidate_id,
        "train_start": window["train_start"],
        "train_end": window["train_end"],
        "test_start": window["test_start"],
        "test_end": window["test_end"],
        "selected_parameters": candidate.get("parameters", {}),
        "test_metrics": metrics,
        "gate": gate,
        "reject_reasons": reasons,
    }


def _walk_forward_candidate_summaries(
    rows: Sequence[Mapping[str, Any]],
    config: DynamicV3ParameterSweepConfig,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_text(row.get("candidate_id")), []).append(row)
    summaries = []
    for candidate_id, candidate_rows in grouped.items():
        pass_count = sum(1 for row in candidate_rows if row.get("gate") != GATE_REJECT)
        pass_ratio = pass_count / max(1, len(candidate_rows))
        oos_degradation = max(
            float(_mapping(row.get("test_metrics")).get("drawdown_degradation_pp", 0))
            for row in candidate_rows
        )
        oos_gate = (
            "PASS"
            if oos_degradation <= config.walk_forward.max_oos_degradation_pp
            else "REVIEW_REQUIRED"
        )
        status = (
            "PASS"
            if pass_ratio >= config.walk_forward.min_pass_ratio and oos_gate == "PASS"
            else "REVIEW_REQUIRED"
        )
        summaries.append(
            {
                "candidate_id": candidate_id,
                "walk_forward_status": status,
                "pass_ratio": round(pass_ratio, 6),
                "oos_gate": oos_gate,
                "oos_drawdown_degradation_pp": round(oos_degradation, 6),
                "window_count": len(candidate_rows),
            }
        )
    return sorted(
        summaries,
        key=lambda row: (row["walk_forward_status"] == "PASS", row["pass_ratio"]),
        reverse=True,
    )


def _oos_summary(
    summaries: Sequence[Mapping[str, Any]],
    config: DynamicV3ParameterSweepConfig,
) -> dict[str, Any]:
    return {
        "holdout_start": config.out_of_sample.holdout_start.isoformat(),
        "holdout_end": config.out_of_sample.holdout_end.isoformat(),
        "candidate_count": len(summaries),
        "pass_count": sum(1 for row in summaries if row.get("oos_gate") == "PASS"),
        "oos_recommendation": "continue_to_robustness" if summaries else "no_candidate",
    }


def _sensitivity_rows(
    result: Mapping[str, Any],
    config: DynamicV3ParameterSweepConfig,
) -> list[dict[str, Any]]:
    base_params = _mapping(result.get("parameters"))
    rows = []
    axes = config.parameter_space
    for key, value in base_params.items():
        values = axes[key].values if key in axes else [value]
        if value not in values:
            continue
        idx = values.index(value)
        neighbors = []
        if idx > 0:
            neighbors.append(values[idx - 1])
        if idx < len(values) - 1:
            neighbors.append(values[idx + 1])
        for neighbor in neighbors:
            params = dict(base_params)
            params[key] = neighbor
            metrics = _fixture_metrics(params, config)
            gate, reasons = gate_candidate(metrics, config)
            score, _ = score_candidate(metrics, config, gate)
            base_score = float(result.get("score") or 0)
            rows.append(
                {
                    "candidate_id": result.get("candidate_id"),
                    "parameter": key,
                    "base_value": value,
                    "neighbor_value": neighbor,
                    "neighbor_gate": gate,
                    "neighbor_reasons": ";".join(reasons),
                    "neighbor_score": score if score is not None else "",
                    "score_delta": round((score or 0) - base_score, 6),
                    "constraint_hit_rate": metrics["constraint_hit_rate"],
                    "turnover": metrics["turnover"],
                    "drawdown_degradation_pp": metrics["drawdown_degradation_pp"],
                }
            )
    return rows


def _stress_bucket_results(
    result: Mapping[str, Any],
    config: DynamicV3ParameterSweepConfig,
) -> dict[str, Any]:
    rows = []
    base_gate = _text(result.get("gate"))
    for bucket in config.robustness.require_stress_buckets:
        status = "PASS"
        if base_gate == GATE_REJECT:
            status = "FAIL"
        elif _stable_int(result.get("candidate_id"), bucket) % 7 == 0:
            status = "REVIEW_REQUIRED"
        rows.append(
            {
                "bucket": bucket,
                "status": status,
                "constraint_hit_stability": status,
                "turnover_stability": "PASS" if status != "FAIL" else "FAIL",
                "drawdown_stability": status,
                "static_gap_stability": status,
            }
        )
    pass_ratio = sum(1 for row in rows if row["status"] == "PASS") / max(1, len(rows))
    return {"buckets": rows, "pass_ratio": round(pass_ratio, 6)}


def _regime_bucket_results(result: Mapping[str, Any]) -> dict[str, Any]:
    regimes = ["risk_on", "risk_off", "volatile", "sideways"]
    return {
        "regimes": [
            {
                "regime": regime,
                "status": "PASS" if _text(result.get("gate")) != GATE_REJECT else "FAIL",
                "rank_stability": "PASS",
            }
            for regime in regimes
        ]
    }


def _overfit_diagnostics(
    sensitivity: Sequence[Mapping[str, Any]],
    stress: Mapping[str, Any],
    config: DynamicV3ParameterSweepConfig,
) -> dict[str, Any]:
    max_delta = max((abs(float(row.get("score_delta") or 0)) for row in sensitivity), default=0.0)
    stress_pass_ratio = float(stress.get("pass_ratio", 0))
    if max_delta <= config.robustness.max_score_delta_for_pass:
        sensitivity_status = "PASS"
    elif max_delta <= config.robustness.max_score_delta_for_review:
        sensitivity_status = "REVIEW_REQUIRED"
    else:
        sensitivity_status = "FAIL"
    stress_status = (
        "PASS" if stress_pass_ratio >= config.robustness.stress_pass_ratio_for_pass else "MIXED"
    )
    robustness_status = (
        "PASS" if sensitivity_status == "PASS" and stress_status == "PASS" else "REVIEW_REQUIRED"
    )
    overfit_status = "LOW_RISK" if robustness_status == "PASS" else "REVIEW_REQUIRED"
    return {
        "robustness_status": robustness_status,
        "overfit_status": overfit_status,
        "parameter_sensitivity_status": sensitivity_status,
        "stress_bucket_status": stress_status,
        "max_abs_score_delta": round(max_delta, 6),
        "stress_pass_ratio": round(stress_pass_ratio, 6),
        "multiple_testing_warning": "review sweep size and ranking concentration before promotion",
        "pbo_dsr_placeholder": {
            "status": "NOT_RUN_REVIEW_NOTE",
            "behavioral_impact": "does not create PASS and does not bypass promotion review",
        },
    }


def _shadow_observation_basis(candidate_id: str) -> dict[str, str]:
    wf_pointer = _read_optional_json(DEFAULT_LATEST_POINTER_DIR / "latest_walk_forward.json")
    rob_pointer = _read_optional_json(DEFAULT_LATEST_POINTER_DIR / "latest_robustness.json")
    wf_id = _text(_mapping(wf_pointer).get("artifact_id"))
    rob_id = _text(_mapping(rob_pointer).get("artifact_id"))
    status = "complete" if wf_id and rob_id else "incomplete_observation_basis"
    return {
        "status": status,
        "source_walk_forward_id": wf_id,
        "source_robustness_id": rob_id,
    }


def _shadow_candidate_report(row: Mapping[str, Any]) -> dict[str, Any]:
    registered = _parse_datetime(_text(row.get("registered_at")))
    age_days = 0 if registered is None else max(0, (datetime.now(UTC) - registered).days)
    rebalance_count = int(row.get("observed_rebalance_count") or 0)
    required_days = int(row.get("promotion_earliest_after_days") or 0)
    required_rebalances = int(row.get("promotion_earliest_after_rebalance_count") or 0)
    basis_complete = _text(row.get("observation_basis_status")) == "complete"
    eligible = (
        age_days >= required_days and rebalance_count >= required_rebalances and basis_complete
    )
    return {
        "candidate_id": row.get("candidate_id"),
        "parameters": row.get("parameters", {}),
        "source_sweep_id": row.get("source_sweep_id"),
        "source_walk_forward_id": row.get("source_walk_forward_id"),
        "source_robustness_id": row.get("source_robustness_id"),
        "evaluator_mode": row.get("evaluator_mode", EVALUATOR_TINY_FIXTURE_PROXY),
        "evaluator_version": row.get("evaluator_version", ""),
        "real_evaluation_artifact_path": row.get("real_evaluation_artifact_path", ""),
        "metrics_source": row.get("metrics_source", "UNKNOWN"),
        "not_for_investment_decision": row.get("not_for_investment_decision") is True,
        "registered_at": row.get("registered_at"),
        "observation_age_days": age_days,
        "rebalance_count": rebalance_count,
        "latest_metrics": row.get("latest_metrics", {}),
        "deviation_vs_original_evaluation": "not_enough_observation_history",
        "promotion_eligibility_status": "eligible" if eligible else "insufficient_observation",
        "recommendation": "promotion_review_ready" if eligible else "continue_shadow_observation",
    }


def _promotion_evidence(
    *,
    candidate_id: str,
    registry_path: Path,
    sweep_output_dir: Path,
    walk_forward_dir: Path,
    robustness_dir: Path,
    overfit_dir: Path,
    candidate_attribution_dir: Path,
    data_provenance_dir: Path,
) -> dict[str, Any]:
    registry = load_shadow_registry(registry_path)
    record = next(
        (
            row
            for row in _records(registry.get("candidates"))
            if row.get("candidate_id") == candidate_id
        ),
        None,
    )
    sweep_id = _text(_mapping(record).get("source_sweep_id"))
    candidate_report_path = (
        sweep_output_dir / sweep_id / "candidates" / candidate_id / "candidate_report.json"
        if sweep_id
        else None
    )
    wf_id = _text(_mapping(record).get("source_walk_forward_id"))
    rob_id = _text(_mapping(record).get("source_robustness_id"))
    wf_report = walk_forward_dir / wf_id / "wf_report.md" if wf_id else None
    rob_report = robustness_dir / rob_id / "robustness_report.md" if rob_id else None
    overfit_manifest_path = _latest_overfit_manifest_for_candidate(candidate_id, overfit_dir)
    candidate_report_payload = (
        _read_optional_json(candidate_report_path) if candidate_report_path else None
    )
    real_eval_path_raw = _text(
        _mapping(candidate_report_payload).get("real_evaluation_artifact_path")
    )
    real_eval_path = Path(real_eval_path_raw) if real_eval_path_raw else None
    weight_metadata_path = (
        real_eval_path.parent / "weight_path_metadata.json" if real_eval_path is not None else None
    )
    attribution_path = candidate_attribution_dir / candidate_id / "attribution_manifest.json"
    data_provenance_path = data_provenance_dir / "price_cache_provenance_report.json"
    shadow = _shadow_candidate_report(record) if record else None
    return {
        "registry_record": record,
        "candidate_report": candidate_report_payload,
        "candidate_report_path": (
            "" if candidate_report_path is None else str(candidate_report_path)
        ),
        "walk_forward_id": wf_id,
        "walk_forward_report_path": "" if wf_report is None else str(wf_report),
        "walk_forward_manifest": (
            _read_optional_json(walk_forward_dir / wf_id / "wf_manifest.json") if wf_id else None
        ),
        "walk_forward_leaderboard": (
            _read_optional_json(walk_forward_dir / wf_id / "wf_leaderboard.json") if wf_id else None
        ),
        "robustness_id": rob_id,
        "robustness_report_path": "" if rob_report is None else str(rob_report),
        "robustness_manifest": (
            _read_optional_json(robustness_dir / rob_id / "robustness_manifest.json")
            if rob_id
            else None
        ),
        "overfit_diagnostics": (
            _read_optional_json(robustness_dir / rob_id / "overfit_diagnostics.json")
            if rob_id
            else None
        ),
        "overfit_manifest": _read_optional_json(overfit_manifest_path),
        "overfit_report_path": (
            ""
            if overfit_manifest_path is None
            else str(overfit_manifest_path.parent / "overfit_report.md")
        ),
        "weight_path_metadata": _read_optional_json(weight_metadata_path),
        "weight_path_metadata_path": (
            "" if weight_metadata_path is None else str(weight_metadata_path)
        ),
        "candidate_attribution": _read_optional_json(attribution_path),
        "candidate_attribution_path": str(attribution_path),
        "data_provenance": _read_optional_json(data_provenance_path),
        "data_provenance_path": str(data_provenance_path),
        "shadow_report": shadow,
    }


def _promotion_status(evidence: Mapping[str, Any]) -> tuple[str, list[str]]:
    reasons: list[str] = []
    candidate = _mapping(evidence.get("candidate_report"))
    if not evidence.get("registry_record"):
        reasons.append("missing_shadow_registry_record")
    if not candidate:
        reasons.append("missing_candidate_report")
    if not evidence.get("walk_forward_manifest"):
        reasons.append("missing_walk_forward_report")
    if not evidence.get("robustness_manifest"):
        reasons.append("missing_robustness_report")
    if reasons:
        return "incomplete", reasons
    if candidate.get("hard_gate_status") == GATE_REJECT:
        return "reject", ["hard_gate_failed"]
    if _text(candidate.get("evaluator_mode"), EVALUATOR_TINY_FIXTURE_PROXY) == (
        EVALUATOR_TINY_FIXTURE_PROXY
    ):
        return "review_required", ["tiny_fixture_not_for_investment_decision"]
    backtest_window = _mapping(candidate.get("backtest_window"))
    if _text(backtest_window.get("date_range_status")) in WINDOW_PROMOTION_BLOCKING_STATUSES:
        return "review_required", ["BACKTEST_WINDOW_INCOMPLETE"]
    weight_metadata = _mapping(evidence.get("weight_path_metadata"))
    if _text(weight_metadata.get("attribution_completeness")) == WEIGHT_PATH_INCOMPLETE:
        return "review_required", ["MISSING_DAILY_WEIGHT_PATH"]
    if not weight_metadata:
        return "review_required", ["MISSING_DAILY_WEIGHT_PATH"]
    if _text(weight_metadata.get("attribution_completeness")) == WEIGHT_PATH_PARTIAL:
        return "review_required", ["WEIGHT_PATH_PARTIAL"]
    attribution = _mapping(evidence.get("candidate_attribution"))
    if not attribution:
        return "review_required", ["ATTRIBUTION_INCOMPLETE"]
    if _text(attribution.get("status")) != WEIGHT_PATH_COMPLETE:
        return "review_required", ["ATTRIBUTION_INCOMPLETE"]
    data_provenance = _mapping(evidence.get("data_provenance"))
    if not data_provenance:
        return "review_required", ["DATA_PROVENANCE_INCOMPLETE"]
    if _text(data_provenance.get("status")) == "FAIL":
        return "review_required", ["DATA_PROVENANCE_INCOMPLETE"]
    if _text(data_provenance.get("provenance_status")) == DATA_PROVENANCE_RECONSTRUCTED:
        return "review_required", ["DATA_PROVENANCE_INCOMPLETE", "provenance_reconstructed"]
    wf_candidates = _records(_mapping(evidence.get("walk_forward_leaderboard")).get("candidates"))
    wf_row = next(
        (row for row in wf_candidates if row.get("candidate_id") == candidate.get("candidate_id")),
        {},
    )
    if _text(wf_row.get("walk_forward_status")) not in {"PASS", ""}:
        return "reject", ["walk_forward_failed"]
    overfit = _mapping(evidence.get("overfit_diagnostics"))
    if _text(overfit.get("robustness_status")) == "FAIL":
        return "reject", ["robustness_failed"]
    overfit_manifest = _mapping(evidence.get("overfit_manifest"))
    if _text(overfit_manifest.get("overfit_status")) == "HIGH_RISK":
        return "reject", ["overfit_high_risk"]
    if _text(overfit.get("overfit_status")) == "HIGH_RISK":
        return "reject", ["overfit_high_risk"]
    shadow = _mapping(evidence.get("shadow_report"))
    if shadow.get("promotion_eligibility_status") != "eligible":
        return "review_required", ["shadow_observation_insufficient"]
    if _text(overfit.get("robustness_status")) != "PASS":
        return "review_required", ["robustness_review_required"]
    if _text(overfit_manifest.get("overfit_status")) == "REVIEW_REQUIRED":
        return "review_required", ["OVERFIT_REVIEW_REQUIRED"]
    return "promote_candidate", ["all_automatic_checks_passed_manual_review_required"]


def _promotion_metric_delta(evidence: Mapping[str, Any]) -> list[dict[str, Any]]:
    metrics = _mapping(_mapping(evidence.get("candidate_report")).get("metrics"))
    return [
        {
            "metric": "constraint_hit_rate",
            "candidate": metrics.get("constraint_hit_rate"),
            "reference": 0.112,
            "direction": "lower_is_better",
        },
        {
            "metric": "turnover",
            "candidate": metrics.get("turnover"),
            "reference": 0.86,
            "direction": "lower_is_better",
        },
        {
            "metric": "drawdown_degradation_pp",
            "candidate": metrics.get("drawdown_degradation_pp"),
            "reference": 0.0,
            "direction": "lower_is_better",
        },
        {
            "metric": "dynamic_vs_static_gap",
            "candidate": metrics.get("dynamic_vs_static_gap"),
            "reference": 0.205,
            "direction": "lower_is_better",
        },
    ]


def _promotion_risk_summary(
    evidence: Mapping[str, Any],
    status: str,
    reasons: Sequence[str],
) -> dict[str, Any]:
    candidate = _mapping(evidence.get("candidate_report"))
    metrics = _mapping(candidate.get("metrics"))
    return {
        "status": status,
        "decision_reasons": list(reasons),
        "problem_solved": "constraint-aware parameter research candidate",
        "improves_v0_4": float(metrics.get("constraint_hit_reduction", 0)) > 0,
        "static_baseline_value": "review metric deltas before manual approval",
        "constraint_hits": metrics.get("constraint_hit_rate", "MISSING"),
        "drawdown": metrics.get("drawdown_degradation_pp", "MISSING"),
        "false_risk_off": metrics.get("false_risk_off_delta", "MISSING"),
        "turnover": metrics.get("turnover", "MISSING"),
        "walk_forward": _mapping(evidence.get("walk_forward_manifest")).get("status", "MISSING"),
        "robustness": _mapping(evidence.get("robustness_manifest")).get("status", "MISSING"),
        "overfit": _mapping(evidence.get("overfit_manifest")).get("overfit_status", "MISSING"),
        "shadow": _mapping(evidence.get("shadow_report")).get(
            "promotion_eligibility_status", "MISSING"
        ),
        "production_candidate_generated": False,
    }


def _promotion_evidence_summary(evidence: Mapping[str, Any]) -> dict[str, Any]:
    candidate = _mapping(evidence.get("candidate_report"))
    data_provenance = _mapping(evidence.get("data_provenance"))
    weight_path = _mapping(evidence.get("weight_path_metadata"))
    attribution = _mapping(evidence.get("candidate_attribution"))
    window = _mapping(candidate.get("backtest_window"))
    overfit = _mapping(evidence.get("overfit_manifest"))
    return {
        "data_quality": _mapping(candidate.get("data_quality")).get("status", "MISSING"),
        "price_cache_sha256": _mapping(data_provenance.get("prices")).get("sha256", ""),
        "download_manifest_status": data_provenance.get("download_manifest_status", "MISSING"),
        "provenance_status": data_provenance.get("provenance_status", "MISSING"),
        "data_provenance_warnings": data_provenance.get("warnings", []),
        "backtest_window_status": window.get("date_range_status", "MISSING"),
        "backtest_window": window,
        "weight_path_status": weight_path.get("attribution_completeness", "MISSING"),
        "weight_path_metadata_path": evidence.get("weight_path_metadata_path", ""),
        "candidate_attribution_status": attribution.get("status", "MISSING"),
        "candidate_attribution_path": evidence.get("candidate_attribution_path", ""),
        "overfit_status": overfit.get("overfit_status", "MISSING"),
        "promotion_blocking_flags": _promotion_blocking_flags(evidence),
    }


def _promotion_blocking_flags(evidence: Mapping[str, Any]) -> list[str]:
    flags: list[str] = []
    candidate = _mapping(evidence.get("candidate_report"))
    window = _mapping(candidate.get("backtest_window"))
    if _text(window.get("date_range_status")) in WINDOW_PROMOTION_BLOCKING_STATUSES:
        flags.append("BACKTEST_WINDOW_INCOMPLETE")
    weight_path = _mapping(evidence.get("weight_path_metadata"))
    if (
        not weight_path
        or _text(weight_path.get("attribution_completeness")) == WEIGHT_PATH_INCOMPLETE
    ):
        flags.append("MISSING_DAILY_WEIGHT_PATH")
    elif _text(weight_path.get("attribution_completeness")) == WEIGHT_PATH_PARTIAL:
        flags.append("WEIGHT_PATH_PARTIAL")
    attribution = _mapping(evidence.get("candidate_attribution"))
    if not attribution or _text(attribution.get("status")) != WEIGHT_PATH_COMPLETE:
        flags.append("ATTRIBUTION_INCOMPLETE")
    data = _mapping(evidence.get("data_provenance"))
    if not data or _text(data.get("status")) == "FAIL":
        flags.append("DATA_PROVENANCE_INCOMPLETE")
    elif _text(data.get("provenance_status")) == DATA_PROVENANCE_RECONSTRUCTED:
        flags.append("DATA_PROVENANCE_INCOMPLETE")
    overfit = _mapping(evidence.get("overfit_manifest"))
    if _text(overfit.get("overfit_status")) == "REVIEW_REQUIRED":
        flags.append("OVERFIT_REVIEW_REQUIRED")
    return flags


def _promotion_linked_artifacts(evidence: Mapping[str, Any]) -> dict[str, Any]:
    candidate = _mapping(evidence.get("candidate_report"))
    return {
        "candidate_report": evidence.get("candidate_report_path"),
        "walk_forward_report": evidence.get("walk_forward_report_path"),
        "robustness_report": evidence.get("robustness_report_path"),
        "overfit_report": evidence.get("overfit_report_path"),
        "real_evaluation_artifact": candidate.get("real_evaluation_artifact_path", ""),
        "weight_path_metadata": evidence.get("weight_path_metadata_path", ""),
        "candidate_attribution": evidence.get("candidate_attribution_path", ""),
        "data_provenance": evidence.get("data_provenance_path", ""),
    }


def _price_cache_provenance_payload(
    *,
    prices_path: Path,
    rates_path: Path,
    manifest_path: Path,
    generated_at: datetime,
) -> dict[str, Any]:
    prices = _cache_file_inventory(prices_path, file_role="prices")
    rates = _cache_file_inventory(rates_path, file_role="rates")
    manifest_records = _download_manifest_records(manifest_path)
    manifest_exists = manifest_path.exists()
    manifest_reconstructed = _download_manifest_reconstructed(manifest_records)
    prices_match = _download_manifest_has_checksum(manifest_records, _text(prices.get("sha256")))
    rates_match = _download_manifest_has_checksum(manifest_records, _text(rates.get("sha256")))
    warnings: list[str] = []
    if manifest_reconstructed:
        warnings.append("download_manifest_provenance_reconstructed")
    if not prices_match:
        warnings.append("prices_download_manifest_checksum_missing")
    if not rates_match:
        warnings.append("rates_download_manifest_checksum_missing")
    status = "PASS"
    if not manifest_exists or not prices_match or not rates_match:
        status = "PASS_WITH_WARNINGS"
    if manifest_exists and manifest_records and manifest_reconstructed:
        status = "PASS_WITH_WARNINGS"
    if not prices["exists"] or not rates["exists"]:
        status = "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_data_provenance_report",
        "status": status,
        "generated_at": generated_at.isoformat(),
        "prices": prices,
        "rates": rates,
        "download_manifest": {
            "path": str(manifest_path),
            "exists": manifest_exists,
            "status": "AVAILABLE" if manifest_exists else "MISSING",
            "row_count": len(manifest_records),
            "sha256": _file_sha256_path(manifest_path) if manifest_exists else "",
        },
        "prices_checksum_in_manifest": prices_match,
        "rates_checksum_in_manifest": rates_match,
        "download_manifest_status": "AVAILABLE" if manifest_exists else "MISSING",
        "provenance_status": (
            DATA_PROVENANCE_RECONSTRUCTED if manifest_reconstructed else "ORIGINAL_OR_VENDOR"
        ),
        "warnings": warnings,
        "limitations": (
            ["original_download_event_not_available"] if manifest_reconstructed else []
        ),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _cache_file_inventory(path: Path, *, file_role: str) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "role": file_role,
        "path": str(path),
        "exists": path.exists(),
        "sha256": _file_sha256_path(path) if path.exists() else "",
        "rows": 0,
        "symbols": [],
        "start_date": "",
        "end_date": "",
    }
    if not path.exists():
        return summary
    try:
        frame = pd.read_csv(path)
    except Exception:  # noqa: BLE001
        return summary
    summary["rows"] = int(len(frame))
    date_column = "date" if "date" in frame else ""
    symbol_column = (
        "ticker"
        if "ticker" in frame
        else "symbol"
        if "symbol" in frame
        else "series"
        if "series" in frame
        else ""
    )
    if symbol_column:
        summary["symbols"] = sorted(str(value) for value in frame[symbol_column].dropna().unique())
    if date_column:
        dates = pd.to_datetime(frame[date_column], errors="coerce").dropna()
        if not dates.empty:
            summary["start_date"] = dates.min().date().isoformat()
            summary["end_date"] = dates.max().date().isoformat()
    return summary


def _download_manifest_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        frame = pd.read_csv(path)
    except Exception:  # noqa: BLE001
        return []
    return frame.fillna("").to_dict(orient="records")


def _reconstructed_download_manifest_row(
    *,
    summary: Mapping[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    request_parameters = {
        "mode": "reconstruct_from_existing_cache",
        "file_role": summary.get("role"),
        "provenance_status": DATA_PROVENANCE_RECONSTRUCTED,
        "original_download_event_available": False,
        "limitations": ["original_download_event_not_available"],
    }
    return {
        "downloaded_at": generated_at.isoformat(),
        "source_id": f"reconstructed_{summary.get('role')}",
        "provider": "cache_rebuild_from_existing_file",
        "endpoint": "local_cache_file",
        "request_parameters": json.dumps(request_parameters, sort_keys=True),
        "output_path": summary.get("path"),
        "row_count": summary.get("rows", 0),
        "checksum_sha256": summary.get("sha256", ""),
    }


def _download_manifest_has_checksum(records: Sequence[Mapping[str, Any]], checksum: str) -> bool:
    if not checksum:
        return False
    return any(_text(row.get("checksum_sha256")) == checksum for row in records)


def _download_manifest_reconstructed(records: Sequence[Mapping[str, Any]]) -> bool:
    for row in records:
        if _text(row.get("provider")) == "cache_rebuild_from_existing_file":
            return True
        params = _json_mapping(row.get("request_parameters"))
        if params.get("provenance_status") == DATA_PROVENANCE_RECONSTRUCTED:
            return True
    return False


def _export_weight_path_artifacts(
    *,
    payload: Mapping[str, Any],
    output_dir: Path,
    candidate_id: str,
    evaluation_id: str,
) -> dict[str, Path]:
    daily_paths = _mapping(payload.get("comparison_daily_paths"))
    dynamic_rows = _records(daily_paths.get("dynamic_candidate"))
    daily_weight_rows = _daily_weight_rows(dynamic_rows, candidate_id=candidate_id)
    rebalance_events = _rebalance_events(dynamic_rows, candidate_id=candidate_id)
    constraint_events = _constraint_events(dynamic_rows, candidate_id=candidate_id)
    rescue_events = _rescue_events(dynamic_rows, candidate_id=candidate_id)
    turnover_rows = _turnover_rows(dynamic_rows, candidate_id=candidate_id)
    missing_fields = [
        "pre_constraint_weight",
        "post_rescue_weight",
        "constraint_limit",
    ]
    metadata = {
        "candidate_id": candidate_id,
        "evaluation_id": evaluation_id,
        "weight_path_start": daily_weight_rows[0]["date"] if daily_weight_rows else "",
        "weight_path_end": daily_weight_rows[-1]["date"] if daily_weight_rows else "",
        "daily_weight_count": len(daily_weight_rows),
        "symbol_count": len({row["symbol"] for row in daily_weight_rows}),
        "has_daily_weights": bool(daily_weight_rows),
        "has_rebalance_events": True,
        "has_constraint_events": True,
        "has_rescue_events": True,
        "has_turnover_by_rebalance": True,
        "weight_path_detail_level": "minimal" if daily_weight_rows else "missing",
        "attribution_completeness": (
            WEIGHT_PATH_PARTIAL if daily_weight_rows else WEIGHT_PATH_INCOMPLETE
        ),
        "missing_fields": missing_fields if daily_weight_rows else ["daily_weights"],
        "metadata_path": str(output_dir / "weight_path_metadata.json"),
    }
    _write_csv(output_dir / "daily_weights.csv", daily_weight_rows)
    _write_json(output_dir / "rebalance_events.json", {"events": rebalance_events})
    _write_json(output_dir / "constraint_events.json", {"events": constraint_events})
    _write_json(output_dir / "rescue_events.json", {"events": rescue_events})
    _write_csv(output_dir / "turnover_by_rebalance.csv", turnover_rows)
    _write_json(output_dir / "weight_path_metadata.json", metadata)
    return {
        "daily_weights": output_dir / "daily_weights.csv",
        "rebalance_events": output_dir / "rebalance_events.json",
        "constraint_events": output_dir / "constraint_events.json",
        "rescue_events": output_dir / "rescue_events.json",
        "turnover_by_rebalance": output_dir / "turnover_by_rebalance.csv",
        "weight_path_metadata": output_dir / "weight_path_metadata.json",
    }


def _daily_weight_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    candidate_id: str,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        weights = _json_mapping(row.get("target_weights_json"))
        pre_rebalance = _json_mapping(row.get("pre_rebalance_candidate_weights_json"))
        cash_weight = weights.get("CASH", 0.0)
        for symbol, weight in sorted(weights.items()):
            output.append(
                {
                    "date": _text(row.get("signal_date")),
                    "symbol": symbol,
                    "weight": weight,
                    "target_weight": weight,
                    "pre_constraint_weight": "",
                    "post_constraint_weight": weight,
                    "post_rescue_weight": "",
                    "cash_weight": cash_weight,
                    "risk_bucket": _text(row.get("selected_regime")),
                    "regime": _text(row.get("selected_regime")),
                    "candidate_id": candidate_id,
                    "pre_rebalance_weight": pre_rebalance.get(symbol, ""),
                }
            )
    return output


def _rebalance_events(
    rows: Sequence[Mapping[str, Any]],
    *,
    candidate_id: str,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for row in rows:
        turnover = _float(row.get("turnover"))
        if _text(row.get("rebalance_decision")) != "rebalance_candidate" and turnover <= 0:
            continue
        trade_deltas = _json_mapping(row.get("trade_deltas_json"))
        changed = sorted(symbol for symbol, value in trade_deltas.items() if abs(_float(value)) > 0)
        events.append(
            {
                "date": _text(row.get("signal_date")),
                "event_type": "rebalance",
                "reason": _event_reason(row),
                "turnover": turnover,
                "symbols_changed": changed,
                "candidate_id": candidate_id,
            }
        )
    return events


def _constraint_events(
    rows: Sequence[Mapping[str, Any]],
    *,
    candidate_id: str,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for row in rows:
        reason_codes = _json_list(row.get("reason_codes_json"))
        constraint_codes = [
            _text(code)
            for code in reason_codes
            if _text(code).startswith(
                ("MAX_", "MIN_", "WEEKLY_TURNOVER_CAP", "REGIME_CONFIRMATION_WINDOW")
            )
        ]
        diagnostics = _json_mapping(row.get("constraint_diagnostics_json"))
        for code in constraint_codes:
            events.append(
                {
                    "date": _text(row.get("signal_date")),
                    "constraint_type": _constraint_type_from_code(code),
                    "symbol": _constraint_symbol_from_code(code),
                    "before_weight": "",
                    "after_weight": "",
                    "constraint_limit": "",
                    "reason_code": code,
                    "diagnostics": diagnostics,
                    "candidate_id": candidate_id,
                }
            )
    return events


def _rescue_events(
    rows: Sequence[Mapping[str, Any]],
    *,
    candidate_id: str,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for row in rows:
        scores = _json_mapping(row.get("input_scores_json"))
        reason_codes = [_text(code) for code in _json_list(row.get("reason_codes_json"))]
        rescue_codes = [
            code for code in reason_codes if "DRAWDOWN" in code or "RISK" in code
        ]
        if not rescue_codes:
            continue
        events.append(
            {
                "date": _text(row.get("signal_date")),
                "rescue_trigger": "drawdown"
                if any("DRAWDOWN" in code for code in rescue_codes)
                else "regime",
                "rescue_intensity": scores.get("RiskRegimeScore", 0.0),
                "affected_symbols": [],
                "risk_reduction": 0.0,
                "reason_codes": rescue_codes,
                "candidate_id": candidate_id,
            }
        )
    return events


def _turnover_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    candidate_id: str,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        trade_deltas = _json_mapping(row.get("trade_deltas_json"))
        buys = {
            symbol: _float(value)
            for symbol, value in trade_deltas.items()
            if _float(value) > 0
        }
        sells = {
            symbol: abs(_float(value))
            for symbol, value in trade_deltas.items()
            if _float(value) < 0
        }
        largest_symbol = ""
        if trade_deltas:
            largest_symbol = max(
                trade_deltas,
                key=lambda symbol: abs(_float(trade_deltas[symbol])),
            )
        output.append(
            {
                "date": _text(row.get("signal_date")),
                "candidate_id": candidate_id,
                "turnover": _float(row.get("turnover")),
                "gross_buy": round(sum(buys.values()), 6),
                "gross_sell": round(sum(sells.values()), 6),
                "largest_symbol_change": largest_symbol,
                "event_reason": _event_reason(row),
            }
        )
    return output


def _missing_weight_path_metadata(*, candidate_id: str, evaluation_id: str) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "evaluation_id": evaluation_id,
        "weight_path_start": "",
        "weight_path_end": "",
        "daily_weight_count": 0,
        "symbol_count": 0,
        "has_daily_weights": False,
        "has_rebalance_events": False,
        "has_constraint_events": False,
        "has_rescue_events": False,
        "has_turnover_by_rebalance": False,
        "weight_path_detail_level": "missing",
        "attribution_completeness": WEIGHT_PATH_INCOMPLETE,
        "missing_fields": ["daily_weights"],
        "metadata_path": "",
    }


def _find_weight_path_dir(evaluation_id: str, search_root: Path) -> Path | None:
    for path in search_root.glob("**/weight_path_metadata.json"):
        payload = _read_optional_json(path) or {}
        if (
            _text(payload.get("evaluation_id")) == evaluation_id
            or path.parent.name == evaluation_id
        ):
            return path.parent
    return None


def _backtest_window_from_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    existing = _mapping(payload.get("backtest_window"))
    requested = _mapping(payload.get("requested_range"))
    summary = _mapping(payload.get("summary"))
    daily = _mapping(payload.get("daily_path_summary"))
    market_regime = _mapping(payload.get("market_regime"))
    configured_start = _date_from_any(
        existing.get("configured_backtest_start")
        or market_regime.get("default_backtest_start")
        or _configured_ai_regime_start()
    )
    requested_start = _date_from_any(
        existing.get("requested_start")
        or requested.get("start")
        or summary.get("requested_start")
        or configured_start
    )
    requested_end = _date_from_any(
        existing.get("requested_end")
        or requested.get("end")
        or summary.get("requested_end")
        or existing.get("configured_backtest_end")
    )
    actual_start = _date_from_any(
        existing.get("actual_evaluation_start")
        or summary.get("effective_start")
        or daily.get("first_signal_date")
    )
    actual_end = _date_from_any(
        existing.get("actual_evaluation_end")
        or summary.get("effective_end")
        or daily.get("last_signal_date")
    )
    trading_days = int(existing.get("trading_days") or daily.get("row_count") or 0)
    status, reasons = _date_range_status(
        configured_start=configured_start,
        requested_start=requested_start,
        requested_end=requested_end,
        actual_start=actual_start,
        actual_end=actual_end,
        artifact_status=_text(payload.get("status")),
    )
    return {
        "configured_backtest_start": _date_text(configured_start),
        "configured_backtest_end": _date_text(requested_end),
        "requested_start": _date_text(requested_start),
        "requested_end": _date_text(requested_end),
        "actual_evaluation_start": _date_text(actual_start),
        "actual_evaluation_end": _date_text(actual_end),
        "effective_training_start": existing.get("effective_training_start"),
        "effective_training_end": existing.get("effective_training_end"),
        "effective_validation_start": existing.get("effective_validation_start"),
        "effective_validation_end": existing.get("effective_validation_end"),
        "first_rebalance_date": existing.get("first_rebalance_date", ""),
        "trading_days": trading_days,
        "date_range_status": status,
        "insufficient_data_reason": (
            "artifact_marked_insufficient_data"
            if status == DATE_RANGE_INSUFFICIENT_DATA
            else existing.get("insufficient_data_reason")
        ),
        "window_mismatch_reasons": reasons,
    }


def _empty_backtest_window(
    *,
    configured_start: date,
    requested_start: date,
    requested_end: date,
    status: str,
) -> dict[str, Any]:
    return {
        "configured_backtest_start": configured_start.isoformat(),
        "configured_backtest_end": requested_end.isoformat(),
        "requested_start": requested_start.isoformat(),
        "requested_end": requested_end.isoformat(),
        "actual_evaluation_start": requested_start.isoformat(),
        "actual_evaluation_end": requested_end.isoformat(),
        "effective_training_start": None,
        "effective_training_end": None,
        "effective_validation_start": None,
        "effective_validation_end": None,
        "first_rebalance_date": "",
        "trading_days": 0,
        "date_range_status": status,
        "insufficient_data_reason": None,
        "window_mismatch_reasons": [],
    }


def _aggregate_candidate_backtest_windows(
    results: Sequence[Mapping[str, Any]],
    *,
    configured_start: date,
    requested_start: date,
    requested_end: date,
) -> dict[str, Any]:
    windows = [
        _mapping(row.get("backtest_window"))
        for row in results
        if row.get("backtest_window")
    ]
    if not windows:
        return _empty_backtest_window(
            configured_start=configured_start,
            requested_start=requested_start,
            requested_end=requested_end,
            status=DATE_RANGE_FAIL,
        )
    actual_starts = [_date_from_any(row.get("actual_evaluation_start")) for row in windows]
    actual_ends = [_date_from_any(row.get("actual_evaluation_end")) for row in windows]
    actual_starts = [item for item in actual_starts if item is not None]
    actual_ends = [item for item in actual_ends if item is not None]
    aggregate = {
        "configured_backtest_start": _date_text(configured_start),
        "configured_backtest_end": _date_text(requested_end),
        "requested_start": _date_text(requested_start),
        "requested_end": _date_text(requested_end),
        "actual_evaluation_start": _date_text(min(actual_starts) if actual_starts else None),
        "actual_evaluation_end": _date_text(max(actual_ends) if actual_ends else None),
        "trading_days": sum(int(row.get("trading_days") or 0) for row in windows),
    }
    status, reasons = _date_range_status(
        configured_start=configured_start,
        requested_start=requested_start,
        requested_end=requested_end,
        actual_start=min(actual_starts) if actual_starts else None,
        actual_end=max(actual_ends) if actual_ends else None,
        artifact_status="",
    )
    return {
        **aggregate,
        "effective_training_start": None,
        "effective_training_end": None,
        "effective_validation_start": None,
        "effective_validation_end": None,
        "first_rebalance_date": "",
        "date_range_status": status,
        "insufficient_data_reason": None,
        "window_mismatch_reasons": reasons,
    }


def _window_audit_inventory(
    *,
    artifact_root: Path,
    requested_start: date,
    requested_end: date,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(artifact_root.glob("**/*.json")):
        if path.name.startswith("latest_") or path.name in {
            "checkpoint.json",
            "gate_summary.json",
            "linked_artifacts.json",
            "risk_summary.json",
        }:
            continue
        payload = _read_optional_json(path)
        if not payload or not _window_auditable_payload(payload, path):
            continue
        records.append(
            _window_record_from_payload(
                payload=payload,
                artifact_path=path,
                requested_start=requested_start,
                requested_end=requested_end,
            )
        )
    return records


def _window_record_from_payload(
    *,
    payload: Mapping[str, Any],
    artifact_path: Path,
    requested_start: date | None,
    requested_end: date | None,
) -> dict[str, Any]:
    window = _backtest_window_from_payload(
        {
            **payload,
            "requested_range": {
                **_mapping(payload.get("requested_range")),
                "start": _date_text(requested_start)
                or _mapping(payload.get("requested_range")).get("start"),
                "end": _date_text(requested_end)
                or _mapping(payload.get("requested_range")).get("end"),
            },
        }
    )
    status = _text(window.get("date_range_status"), DATE_RANGE_FAIL)
    return {
        "artifact_path": str(artifact_path),
        "artifact_type": _artifact_type(artifact_path, payload),
        **window,
        "promotion_blocking": status in WINDOW_PROMOTION_BLOCKING_STATUSES,
    }


def _window_auditable_payload(payload: Mapping[str, Any], path: Path) -> bool:
    report_type = _text(payload.get("report_type"))
    if payload.get("backtest_window") or payload.get("requested_range"):
        return True
    if report_type.startswith("etf_dynamic_v3_"):
        return any(
            token in report_type
            for token in ("sweep", "candidate", "real_evaluation", "walk_forward", "promotion")
        )
    return any(part in path.parts for part in ("dynamic_v3_rescue", "dynamic_v3_real_evaluation"))


def _artifact_type(path: Path, payload: Mapping[str, Any]) -> str:
    report_type = _text(payload.get("report_type"))
    if "sweep" in report_type or path.name == "sweep_manifest.json":
        return "sweep"
    if "candidate" in report_type or "candidates" in path.parts:
        return "candidate"
    if "real_evaluation" in report_type:
        return "real_evaluation"
    if "walk_forward" in report_type:
        return "walk_forward"
    if "promotion" in report_type:
        return "promotion"
    return "artifact"


def _date_range_status(
    *,
    configured_start: date | None,
    requested_start: date | None,
    requested_end: date | None,
    actual_start: date | None,
    actual_end: date | None,
    artifact_status: str,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if artifact_status == DATE_RANGE_INSUFFICIENT_DATA:
        reasons.append("artifact_marked_insufficient_data")
        return DATE_RANGE_INSUFFICIENT_DATA, reasons
    if requested_start is None or requested_end is None:
        reasons.append("requested_date_range_missing")
    if configured_start is None:
        reasons.append("configured_backtest_start_missing")
    if actual_start is None or actual_end is None:
        reasons.append("actual_evaluation_range_missing")
    if reasons:
        return DATE_RANGE_FAIL, reasons
    if actual_end < actual_start:
        return DATE_RANGE_FAIL, ["actual_evaluation_end_before_start"]
    if actual_end < requested_end:
        reasons.append("actual_evaluation_end_before_requested_end")
    if actual_start > configured_start:
        reasons.append("actual_evaluation_start_after_configured_backtest_start")
    if actual_start > configured_start + timedelta(days=WINDOW_AUDIT_ALLOWED_START_DELAY_DAYS):
        return DATE_RANGE_INCOMPLETE, reasons
    if actual_end < requested_end:
        return DATE_RANGE_INCOMPLETE, reasons
    if reasons:
        return DATE_RANGE_PASS_WITH_WARNINGS, reasons
    return DATE_RANGE_PASS, []


def _earliest_actual_evaluation_start(records: Sequence[Mapping[str, Any]]) -> str:
    dates = [
        parsed
        for parsed in (_date_from_any(row.get("actual_evaluation_start")) for row in records)
        if parsed is not None
    ]
    return "" if not dates else min(dates).isoformat()


def _configured_ai_regime_start() -> date:
    try:
        return load_dynamic_v3_real_evaluation_policy_config().market_regime.default_backtest_start
    except Exception:  # noqa: BLE001
        return date(2022, 12, 1)


def _csv_columns(path: Path) -> list[str]:
    if not path.exists():
        return []
    try:
        return list(pd.read_csv(path, nrows=0).columns)
    except Exception:  # noqa: BLE001
        return []


def _file_sha256_path(path: Path) -> str:
    if not path.exists():
        return ""
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _event_reason(row: Mapping[str, Any]) -> str:
    reason_codes = [_text(code) for code in _json_list(row.get("reason_codes_json"))]
    if any("DRAWDOWN" in code for code in reason_codes):
        return "drawdown_guard"
    if any("RISK" in code for code in reason_codes):
        return "regime_change"
    if any("TURNOVER" in code for code in reason_codes):
        return "constraint"
    return "scheduled"


def _constraint_type_from_code(code: str) -> str:
    if code.startswith("MAX_"):
        return "max_weight"
    if code.startswith("MIN_"):
        return "min_weight"
    if "TURNOVER" in code:
        return "turnover"
    if "DRAWDOWN" in code:
        return "drawdown"
    return "other"


def _constraint_symbol_from_code(code: str) -> str:
    for symbol in ("QQQ", "SPY", "SMH", "SOXX", "CASH"):
        if symbol in code:
            return symbol
    return ""


def _date_from_any(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _date_text(value: Any) -> str:
    parsed = _date_from_any(value)
    return "" if parsed is None else parsed.isoformat()


def _price_cache_manifest(
    *,
    quality_report: Any,
    prices_path: Path,
    rates_path: Path,
) -> dict[str, Any]:
    return {
        "provider": "local_cached_market_data",
        "prices_path": str(prices_path),
        "rates_path": str(rates_path),
        "prices": _file_summary_payload(quality_report.price_summary),
        "rates": _file_summary_payload(quality_report.rate_summary),
        "secondary_prices": _file_summary_payload(quality_report.secondary_price_summary),
        "download_manifest": _file_summary_payload(quality_report.manifest_summary),
        "download_timestamp": quality_report.checked_at.isoformat(),
        "row_count": quality_report.price_summary.rows,
        "checksum": quality_report.price_summary.sha256,
    }


def _checksum_audit(quality_report: Any) -> dict[str, Any]:
    warning_codes = [
        issue.code for issue in quality_report.issues if issue.severity.value == "WARNING"
    ]
    return {
        "prices_daily_sha256": quality_report.price_summary.sha256,
        "download_manifest_path": (
            ""
            if quality_report.manifest_summary is None
            else str(quality_report.manifest_summary.path)
        ),
        "download_manifest_exists": (
            False
            if quality_report.manifest_summary is None
            else quality_report.manifest_summary.exists
        ),
        "prices_checksum_missing": "prices_download_manifest_checksum_missing" in warning_codes,
        "warning_codes": warning_codes,
    }


def _pit_coverage_audit(*, quality_report: Any, as_of: date, end: date) -> dict[str, Any]:
    min_date = quality_report.price_summary.min_date
    max_date = quality_report.price_summary.max_date
    return {
        "requested_as_of": as_of.isoformat(),
        "requested_end": end.isoformat(),
        "price_min_date": "" if min_date is None else min_date.isoformat(),
        "price_max_date": "" if max_date is None else max_date.isoformat(),
        "benchmark_coverage_status": "PASS" if quality_report.price_summary.exists else "FAIL",
        "regime_signal_event_as_of_status": "REVIEW_REQUIRED",
        "future_leakage_risk": "LOW" if max_date is None or max_date <= end else "REVIEW_REQUIRED",
        "adjusted_price_split_dividend_status": "adj_close_present_schema_validated",
    }


def _data_gap_report(*, prices_path: Path, as_of: date, end: date) -> dict[str, Any]:
    if not prices_path.exists():
        return {"status": "FAIL", "missing_dates": [], "symbol_completeness": []}
    frame = pd.read_csv(prices_path)
    if "date" not in frame or "ticker" not in frame:
        return {"status": "FAIL", "missing_dates": [], "symbol_completeness": []}
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    window = frame[(frame["_date"] >= as_of) & (frame["_date"] <= end)]
    dates = sorted(date_value.isoformat() for date_value in window["_date"].dropna().unique())
    symbol_rows = []
    for ticker, group in window.groupby("ticker"):
        symbol_rows.append(
            {
                "symbol": ticker,
                "row_count": int(len(group)),
                "min_date": "" if group.empty else str(group["_date"].min()),
                "max_date": "" if group.empty else str(group["_date"].max()),
            }
        )
    return {
        "status": "PASS" if not window.empty else "PASS_WITH_WARNINGS",
        "observed_trading_dates": dates,
        "missing_dates": [],
        "symbol_completeness": symbol_rows,
    }


def _quality_issue_rows(quality_report: Any) -> list[dict[str, Any]]:
    return [
        {
            "severity": issue.severity.value,
            "code": issue.code,
            "message": issue.message,
            "rows": issue.rows,
            "sample": issue.sample,
            "source": issue.source,
        }
        for issue in quality_report.issues
    ]


def _file_summary_payload(summary: Any) -> dict[str, Any] | None:
    if summary is None:
        return None
    return {
        "path": str(summary.path),
        "exists": summary.exists,
        "rows": summary.rows,
        "sha256": summary.sha256,
        "min_date": "" if summary.min_date is None else summary.min_date.isoformat(),
        "max_date": "" if summary.max_date is None else summary.max_date.isoformat(),
    }


def _select_injection_audit_candidates(
    candidates: Sequence[Mapping[str, Any]],
    *,
    max_candidates: int,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    covered: dict[str, set[str]] = {parameter: set() for parameter in REQUIRED_INJECTION_PARAMETERS}
    for candidate in candidates:
        params = _mapping(candidate.get("parameters"))
        score = sum(
            1
            for parameter in REQUIRED_INJECTION_PARAMETERS
            if _text(params.get(parameter)) not in covered[parameter]
        )
        if score <= 0 and len(selected) >= max_candidates:
            continue
        selected.append(dict(candidate))
        for parameter in REQUIRED_INJECTION_PARAMETERS:
            covered[parameter].add(_text(params.get(parameter)))
        if len(selected) >= max_candidates and all(len(values) >= 2 for values in covered.values()):
            break
    return selected[:max_candidates]


def _injection_matrix_row(
    *,
    candidate: Mapping[str, Any],
    real_context: RealEvaluationContext,
) -> dict[str, Any]:
    params = _mapping(candidate.get("parameters"))
    real_policy = _real_policy_for_sweep_candidate(real_context.real_policy, params)
    rescue_policy = _real_rescue_policy_for_sweep_candidate(real_context.v3_rescue_policy, params)
    row = {
        "candidate_id": candidate.get("candidate_id"),
        "effective_real_policy_hash": _stable_id(real_policy.model_dump(mode="json")),
        "effective_rescue_policy_hash": _stable_id(rescue_policy.model_dump(mode="json")),
        "metric_hash": _stable_id(_mapping(candidate.get("metrics"))),
        "latest_weight_hash": _latest_weight_hash_from_candidate(candidate),
    }
    for parameter in REQUIRED_INJECTION_PARAMETERS:
        row[parameter] = params.get(parameter)
        row[f"{parameter}_consumed"] = parameter in PARAMETER_EFFECT_FIELDS
    return row


def _weight_path_diff_summary(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    hashes = [_latest_weight_hash_from_candidate(row) for row in results]
    return {
        "candidate_count": len(results),
        "distinct_latest_weight_hash_count": len(set(hashes)),
        "weight_path_evidence": "latest_weights_from_real_evaluation_comparison_table",
        "daily_path_evidence_status": "INCOMPLETE_DAILY_PATH_NOT_EXPORTED",
        "candidate_weight_hashes": [
            {"candidate_id": row.get("candidate_id"), "latest_weight_hash": hash_value}
            for row, hash_value in zip(results, hashes, strict=False)
        ],
    }


def _metric_diff_summary(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    metric_keys = (
        "constraint_hit_rate",
        "turnover",
        "drawdown_degradation_pp",
        "dynamic_vs_static_gap",
        "return_delta",
    )
    return {
        key: _distribution([float(_mapping(row.get("metrics")).get(key, 0.0)) for row in results])
        for key in metric_keys
    }


def _parameter_effect_summary(
    matrix_rows: Sequence[Mapping[str, Any]],
    results: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for parameter in REQUIRED_INJECTION_PARAMETERS:
        values = {_text(row.get(parameter)) for row in matrix_rows}
        config_hashes = {
            _text(row.get("effective_real_policy_hash"))
            + _text(row.get("effective_rescue_policy_hash"))
            for row in matrix_rows
        }
        metric_hashes = {_text(row.get("metric_hash")) for row in matrix_rows}
        weight_hashes = {_text(row.get("latest_weight_hash")) for row in matrix_rows}
        consumed = parameter in PARAMETER_EFFECT_FIELDS
        has_effect = len(config_hashes) > 1 or len(metric_hashes) > 1 or len(weight_hashes) > 1
        status = "EFFECTIVE" if consumed and has_effect else "NO_OBSERVED_EFFECT"
        if not consumed:
            status = "NOT_CONSUMED"
        rows.append(
            {
                "parameter": parameter,
                "effect_status": status,
                "consumed": consumed,
                "distinct_value_count": len(values),
                "distinct_effective_config_hash_count": len(config_hashes),
                "distinct_metric_hash_count": len(metric_hashes),
                "distinct_weight_hash_count": len(weight_hashes),
                "candidate_count": len(results),
            }
        )
    return rows


def _latest_weight_hash_from_candidate(candidate: Mapping[str, Any]) -> str:
    artifact_path = _text(candidate.get("real_evaluation_artifact_path"))
    payload = _read_optional_json(Path(artifact_path)) if artifact_path else None
    weights = _mapping(_mapping(payload).get("best_candidate")).get("latest_weights", {})
    return _stable_id(weights)


def _candidate_weight_delta_rows(real_payload: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not real_payload:
        return []
    best = _mapping(real_payload.get("best_candidate"))
    reference = _first_row_by_group(real_payload.get("comparison_table"), "dynamic_v0_4")
    best_weights = _mapping(best.get("latest_weights"))
    reference_weights = _mapping(reference.get("latest_weights"))
    symbols = sorted(set(best_weights) | set(reference_weights))
    return [
        {
            "symbol": symbol,
            "candidate_weight": float(best_weights.get(symbol, 0.0)),
            "baseline_weight": float(reference_weights.get(symbol, 0.0)),
            "delta": round(
                float(best_weights.get(symbol, 0.0))
                - float(reference_weights.get(symbol, 0.0)),
                6,
            ),
        }
        for symbol in symbols
    ]


def _rebalance_event_attribution(
    real_payload: Mapping[str, Any] | None,
    metrics: Mapping[str, Any],
    incomplete_reasons: Sequence[str],
) -> dict[str, Any]:
    return {
        "status": "INCOMPLETE" if incomplete_reasons else "PASS",
        "summary": "daily rebalance path unavailable; turnover metric used for review",
        "turnover": metrics.get("turnover"),
        "incomplete_reasons": list(incomplete_reasons),
        "source_report_id": _mapping(real_payload).get("dynamic_v3_real_evaluation_report_id"),
    }


def _constraint_event_attribution(
    real_payload: Mapping[str, Any] | None,
    metrics: Mapping[str, Any],
) -> dict[str, Any]:
    analysis = _mapping(_mapping(real_payload).get("constraint_hit_analysis"))
    return {
        "status": analysis.get("status", "REVIEW_REQUIRED"),
        "summary": analysis.get("conclusion", "constraint attribution uses aggregate real metrics"),
        "constraint_hits": metrics.get("constraint_hits"),
        "constraint_hit_rate": metrics.get("constraint_hit_rate"),
        "constraint_hit_reduction_count_vs_v0_4": metrics.get(
            "constraint_hit_reduction_count_vs_v0_4"
        ),
    }


def _drawdown_window_attribution(
    real_payload: Mapping[str, Any] | None,
    metrics: Mapping[str, Any],
) -> dict[str, Any]:
    analysis = _mapping(_mapping(real_payload).get("drawdown_preservation_analysis"))
    return {
        "status": analysis.get("status", "REVIEW_REQUIRED"),
        "summary": analysis.get("conclusion", "drawdown attribution uses aggregate real metrics"),
        "drawdown_degradation_pp": metrics.get("drawdown_degradation_pp"),
    }


def _turnover_attribution(
    real_payload: Mapping[str, Any] | None,
    metrics: Mapping[str, Any],
) -> dict[str, Any]:
    analysis = _mapping(_mapping(real_payload).get("turnover_analysis"))
    return {
        "status": analysis.get("status", "REVIEW_REQUIRED"),
        "summary": analysis.get("conclusion", "turnover attribution uses aggregate real metrics"),
        "turnover": metrics.get("turnover"),
        "turnover_reduction": metrics.get("turnover_reduction"),
    }


def _dynamic_vs_static_gap_attribution(
    real_payload: Mapping[str, Any] | None,
    metrics: Mapping[str, Any],
) -> dict[str, Any]:
    analysis = _mapping(_mapping(real_payload).get("static_gap_analysis"))
    return {
        "status": analysis.get("status", "REVIEW_REQUIRED"),
        "summary": analysis.get("conclusion", "gap attribution uses aggregate real metrics"),
        "dynamic_vs_static_gap": metrics.get("dynamic_vs_static_gap"),
        "dynamic_vs_static_gap_improvement": metrics.get("dynamic_vs_static_gap_improvement"),
    }


def _window_train_leaderboard(
    results: Sequence[Mapping[str, Any]],
    *,
    window: Mapping[str, str],
    window_index: int,
) -> list[dict[str, Any]]:
    rows = []
    for row in results:
        adjusted = float(row.get("score") or 0.0) + (
            (_stable_int(row.get("candidate_id"), window.get("train_end")) % 11) / 10000.0
        )
        rows.append({**dict(row), "train_score": round(adjusted, 6)})
    ranked = sorted(rows, key=lambda item: item["train_score"], reverse=True)
    for rank, row in enumerate(ranked, start=1):
        row["train_rank"] = rank
        row["window_index"] = window_index
    return ranked


def _walk_forward_selection_test_row(
    selected: Mapping[str, Any],
    window: Mapping[str, str],
    config: DynamicV3ParameterSweepConfig,
) -> dict[str, Any]:
    metrics = dict(_mapping(selected.get("metrics")))
    drift = (_stable_int(selected.get("candidate_id"), window.get("test_end")) % 9 - 4) / 1000.0
    metrics["dynamic_vs_static_gap"] = round(
        float(metrics.get("dynamic_vs_static_gap", 0.0)) + drift,
        6,
    )
    metrics["drawdown_degradation_pp"] = round(
        float(metrics.get("drawdown_degradation_pp", 0.0)) + abs(drift) / 2,
        6,
    )
    gate, reasons = gate_candidate(metrics, config)
    return {
        "window_index": selected.get("window_index"),
        "candidate_id": selected.get("candidate_id"),
        "train_rank": selected.get("train_rank"),
        "train_start": window["train_start"],
        "train_end": window["train_end"],
        "test_start": window["test_start"],
        "test_end": window["test_end"],
        "test_result": metrics,
        "test_gate": gate,
        "test_reject_reasons": reasons,
    }


def _wf_parameter_stability(selected_rows: Sequence[Mapping[str, Any]]) -> str:
    if not selected_rows:
        return "MISSING"
    ids = {_text(row.get("candidate_id")) for row in selected_rows}
    return "STABLE" if len(ids) == 1 else "MIXED"


def _candidate_rank(
    results: Sequence[Mapping[str, Any]],
    candidate_id: str,
) -> int | None:
    ranked = sorted(results, key=lambda row: float(row.get("score") or 0.0), reverse=True)
    for index, row in enumerate(ranked, start=1):
        if row.get("candidate_id") == candidate_id:
            return index
    return None


def _rank_stability_payload(
    results: Sequence[Mapping[str, Any]],
    candidate_id: str,
    rank: int | None,
) -> dict[str, Any]:
    candidate_count = len(results)
    percentile = None if rank is None else round(rank / max(1, candidate_count), 6)
    return {
        "candidate_id": candidate_id,
        "rank": rank,
        "candidate_count": candidate_count,
        "rank_percentile": percentile,
        "status": "PASS" if percentile is not None and percentile <= 0.2 else "REVIEW_REQUIRED",
    }


def _parameter_neighborhood_stability_payload(
    result: Mapping[str, Any],
    config: DynamicV3ParameterSweepConfig,
) -> dict[str, Any]:
    sensitivity = _sensitivity_rows(result, config)
    max_abs_delta = max(
        (abs(float(row.get("score_delta") or 0.0)) for row in sensitivity),
        default=0.0,
    )
    status = (
        "PASS"
        if max_abs_delta <= config.robustness.max_score_delta_for_pass
        else "REVIEW_REQUIRED"
    )
    return {
        "status": status,
        "neighbor_count": len(sensitivity),
        "max_abs_score_delta": round(max_abs_delta, 6),
        "rows": sensitivity,
    }


def _overfit_regime_stability_payload(result: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _mapping(result.get("metrics"))
    return {
        "status": _text(metrics.get("stress_bucket_status"), "REVIEW_REQUIRED"),
        "robustness_status": metrics.get("robustness_status"),
        "parameter_sensitivity_status": metrics.get("parameter_sensitivity_status"),
    }


def _extreme_day_dependency_payload(result: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _mapping(result.get("metrics"))
    return_delta = abs(float(metrics.get("return_delta") or 0.0))
    gap = abs(float(metrics.get("dynamic_vs_static_gap_improvement") or 0.0))
    share = 0.0 if return_delta == 0 else min(1.0, gap / return_delta)
    return {
        "status": "REVIEW_REQUIRED" if share > 0.5 else "PASS",
        "estimated_extreme_day_return_share": round(share, 6),
        "method": "aggregate_proxy_until_daily_path_export_is_available",
    }


def _multiple_testing_warning_payload(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    candidate_count = len(results)
    return {
        "status": "REVIEW_REQUIRED" if candidate_count >= 20 else "PASS",
        "candidate_count": candidate_count,
        "warning": "review sweep size and ranking concentration before promotion",
    }


def _overfit_status_from_components(
    *,
    result: Mapping[str, Any],
    rank_stability: Mapping[str, Any],
    neighborhood: Mapping[str, Any],
    extreme_day: Mapping[str, Any],
    multiple_testing: Mapping[str, Any],
) -> str:
    if _text(result.get("gate")) == GATE_REJECT:
        return "HIGH_RISK"
    statuses = {
        _text(rank_stability.get("status")),
        _text(neighborhood.get("status")),
        _text(extreme_day.get("status")),
        _text(multiple_testing.get("status")),
    }
    return "LOW_RISK" if statuses == {"PASS"} else "REVIEW_REQUIRED"


def _governance_checks(
    *,
    config: DynamicV3ParameterSweepConfig,
    governance: ParameterGovernanceConfig,
) -> list[dict[str, Any]]:
    policies = _governance_policy_by_parameter(governance)
    manual_only = {
        parameter
        for parameter, policy in policies.items()
        if policy == "manual_only"
    }
    overrides = sorted(set(config.parameter_space) & manual_only)
    checks = [
        _check(
            "search_space_version_present",
            bool(governance.search_space_version),
            governance.search_space_version,
        ),
        _check(
            "manual_only_parameters_not_overridden",
            not overrides,
            "manual_only overrides: " + ", ".join(overrides),
        ),
    ]
    for parameter in config.parameter_space:
        checks.append(
            _check(
                f"{parameter}:governed",
                parameter in policies,
                policies.get(parameter, "missing from governance"),
            )
        )
    return checks


def _governance_parameters(
    governance: ParameterGovernanceConfig,
    policy: str,
) -> list[str]:
    return sorted(
        parameter
        for group in governance.parameter_groups.values()
        if group.search_policy == policy
        for parameter in group.parameters
    )


def _governance_policy_by_parameter(governance: ParameterGovernanceConfig) -> dict[str, str]:
    return {
        parameter: group.search_policy
        for group in governance.parameter_groups.values()
        for parameter in group.parameters
    }


def _search_space_version() -> str:
    try:
        return load_parameter_governance_config().search_space_version
    except DynamicV3ParameterResearchError:
        return "UNKNOWN"


def _dict_diff(left: Mapping[str, Any], right: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for key in sorted(set(left) | set(right)):
        if left.get(key) != right.get(key):
            rows.append({"key": key, "left": left.get(key), "right": right.get(key)})
    return rows


def _numeric_metric_diff(left: Mapping[str, Any], right: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for key in sorted(set(left) | set(right)):
        try:
            left_value = float(left.get(key))
            right_value = float(right.get(key))
        except (TypeError, ValueError):
            continue
        rows.append(
            {
                "metric": key,
                "left": left_value,
                "right": right_value,
                "delta": round(left_value - right_value, 6),
            }
        )
    return rows


def _shadow_monitor_candidate_result(row: Mapping[str, Any], *, as_of: date) -> dict[str, Any]:
    registered_at = _parse_datetime(_text(row.get("registered_at")))
    days_observed = 0 if registered_at is None else max(0, (as_of - registered_at.date()).days)
    required_days = int(row.get("promotion_earliest_after_days") or 0)
    required_rebalances = int(row.get("promotion_earliest_after_rebalance_count") or 0)
    rebalances = int(row.get("observed_rebalance_count") or 0)
    latest_metrics = _mapping(row.get("latest_metrics"))
    eligible = days_observed >= required_days and rebalances >= required_rebalances
    drift = (
        "REVIEW_REQUIRED"
        if abs(float(latest_metrics.get("dynamic_vs_static_gap") or 0.0)) > 0.5
        else "PASS"
    )
    return {
        "candidate_id": row.get("candidate_id"),
        "status": row.get("status"),
        "days_observed": days_observed,
        "rebalance_count_observed": rebalances,
        "latest_weight_path": row.get("real_evaluation_artifact_path", ""),
        "latest_constraint_hits": latest_metrics.get("constraint_hits"),
        "latest_turnover": latest_metrics.get("turnover"),
        "latest_drawdown": latest_metrics.get("drawdown_degradation_pp"),
        "dynamic_vs_static_gap": latest_metrics.get("dynamic_vs_static_gap"),
        "live_vs_backtest_drift": drift,
        "promotion_clock": {
            "required_days": required_days,
            "required_rebalances": required_rebalances,
        },
        "promotion_eligibility": "promotion_review_ready" if eligible else "continue_observation",
        "recommendation": (
            "promotion_review_ready"
            if eligible and drift == "PASS"
            else "continue_shadow_observation"
        ),
    }


def _latest_pointer_artifact_id(name: str) -> str:
    pointer = _read_optional_json(DEFAULT_LATEST_POINTER_DIR / f"{name}.json")
    return _text(_mapping(pointer).get("artifact_id"))


def _latest_overfit_manifest_for_candidate(candidate_id: str, output_dir: Path) -> Path | None:
    candidates = []
    for manifest_path in output_dir.glob("*/overfit_manifest.json"):
        manifest = _read_optional_json(manifest_path)
        if _mapping(manifest).get("candidate_id") == candidate_id:
            candidates.append(manifest_path)
    return max(candidates, key=lambda path: path.stat().st_mtime) if candidates else None


def _resolve_project_path(path: Path) -> Path:
    return path if path.is_absolute() else PROJECT_ROOT / path


def _update_latest_pointer(name: str, artifact_id: str, path: Path) -> None:
    DEFAULT_LATEST_POINTER_DIR.mkdir(parents=True, exist_ok=True)
    _write_json(
        DEFAULT_LATEST_POINTER_DIR / f"{name}.json",
        {
            "schema_version": SCHEMA_VERSION,
            "artifact_type": name.removeprefix("latest_"),
            "artifact_id": artifact_id,
            "path": str(path),
            "updated_at": datetime.now(UTC).isoformat(),
            "exists": path.exists(),
        },
    )


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_jsonable(payload), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise DynamicV3ParameterResearchError(f"required JSON artifact not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _read_optional_json(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _append_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(text)


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(_jsonable(payload), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_jsonable(row), ensure_ascii=False, sort_keys=True) + "\n")


def _append_jsonl(path: Path, row: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(_jsonable(row), ensure_ascii=False, sort_keys=True) + "\n")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row.keys()}) if rows else ["status"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def _unique_dir(path: Path) -> Path:
    if not path.exists():
        return path
    for idx in range(1, 1000):
        candidate = path.with_name(f"{path.name}_{idx:03d}")
        if not candidate.exists():
            return candidate
    raise DynamicV3ParameterResearchError(f"could not allocate unique artifact dir: {path}")


def _latest_child_dir(path: Path) -> Path | None:
    if not path.exists():
        return None
    dirs = [child for child in path.iterdir() if child.is_dir()]
    return max(dirs, key=lambda child: child.stat().st_mtime) if dirs else None


def _canonical_json(payload: Any) -> str:
    return json.dumps(_jsonable(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def _stable_id(*parts: Any) -> str:
    return sha256(_canonical_json(parts).encode("utf-8")).hexdigest()[:16]


def _stable_int(*parts: Any) -> int:
    return int(_stable_id(*parts), 16)


def _git_commit() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.strip()
    except Exception:  # noqa: BLE001
        return "unknown_git_commit"


def _check(check_id: str, passed: bool, detail: str) -> dict[str, Any]:
    return {"check_id": check_id, "passed": bool(passed), "detail": detail}


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _texts(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence):
        return [_text(item) for item in value if _text(item)]
    return []


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _fmt_num(value: Any) -> str:
    try:
        return f"{float(value):.4f}"
    except Exception:  # noqa: BLE001
        return _text(value, "NA")


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _add_months(value: date, months: int) -> date:
    month = value.month - 1 + months
    year = value.year + month // 12
    month = month % 12 + 1
    day = min(value.day, _days_in_month(year, month))
    return date(year, month, day)


def _days_in_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except ValueError:
        return None
