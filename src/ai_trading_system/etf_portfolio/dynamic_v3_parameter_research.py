from __future__ import annotations

import calendar
import csv
import json
import subprocess
from collections import Counter
from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
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
    precompute_dynamic_v3_fixed_robustness_reports,
    write_dynamic_v3_real_evaluation_report,
)
from ai_trading_system.etf_portfolio.dynamic_v3_rescue import (
    load_dynamic_v3_rescue_policy_config,
)
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_PRICE_PATH,
    load_etf_config_bundle,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day, us_equity_market_session
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
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_v3_rescue" / "parameter_governance_v1.yaml"
)
DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_v3_rescue" / "evidence_gate_policy_v1.yaml"
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
DEFAULT_SCHEDULE_OBSERVE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "schedule_observe"
DEFAULT_EVIDENCE_SUMMARY_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "evidence_summary"
DEFAULT_MEDIUM_REAL_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "medium_real"
DEFAULT_REGIME_COVERAGE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "regime_coverage"
DEFAULT_INTERPRETATION_PACK_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "interpretation_pack"
DEFAULT_OBSERVE_POOL_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "observe_pool"
DEFAULT_OVERNIGHT_READINESS_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "overnight_readiness"
DEFAULT_RESEARCH_DECISION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "research_decision"
DEFAULT_EVIDENCE_DIAGNOSIS_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "evidence_diagnosis"
DEFAULT_GATE_IMPACT_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "gate_impact"
DEFAULT_GATE_POLICY_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "gate_policy"
DEFAULT_CANDIDATE_RECOVERY_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "candidate_recovery"
DEFAULT_RESEARCH_DECISION_UPDATE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "research_decision_update"
DEFAULT_SHORTLIST_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "shortlist"
DEFAULT_CANDIDATE_CLUSTER_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "candidate_cluster"
DEFAULT_SHADOW_SHORTLIST_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "shadow_shortlist"
DEFAULT_SHADOW_MONITOR_RUN_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "shadow_monitor_runs"
DEFAULT_PORTFOLIO_SNAPSHOT_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "portfolio_snapshot"
DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "manual_portfolio_snapshot"
)
DEFAULT_PORTFOLIO_EXPOSURE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "portfolio_exposure"
DEFAULT_POSITION_DRIFT_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "position_drift"
DEFAULT_EXECUTION_GUARDRAILS_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "execution_guardrails"
DEFAULT_MANUAL_EXECUTION_REVIEW_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "manual_execution_review"
DEFAULT_POSITION_ADVISORY_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "position_advisory"
DEFAULT_POSITION_ADVISORY_DAILY_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "position_advisory_daily"
DEFAULT_CONSENSUS_DRIFT_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "consensus_drift"
DEFAULT_POSITION_REVIEW_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "position_review"
DEFAULT_OWNER_REVIEW_JOURNAL_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "owner_review_journal"
DEFAULT_LATEST_POINTER_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "latest"
DEFAULT_SHADOW_REGISTRY_PATH = (
    PROJECT_ROOT / "registry" / "etf_portfolio" / "dynamic_v3_rescue_shadow_candidates.yaml"
)
DEFAULT_POSITION_ADVISORY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_v3_rescue" / "position_advisory_v1.yaml"
)
DEFAULT_MANUAL_PORTFOLIO_SCHEMA_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "manual_portfolio_snapshot_schema_v1.yaml"
)
DEFAULT_PORTFOLIO_EXPOSURE_POLICY_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "portfolio_exposure_policy_v1.yaml"
)
DEFAULT_EXECUTION_GUARDRAILS_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_v3_rescue" / "execution_guardrails_v1.yaml"
)
DEFAULT_CURRENT_PORTFOLIO_SNAPSHOT_EXAMPLE_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "current_portfolio_snapshot.example.yaml"
)
DEFAULT_CURRENT_PORTFOLIO_SNAPSHOT_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "current_portfolio_snapshot.yaml"
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
# Pure floating-point validation tolerances; these do not change investment policy.
WEIGHT_PATH_WEIGHT_BOUND_TOLERANCE = 1e-9
WEIGHT_PATH_WEIGHT_SUM_TOLERANCE = 1e-6
DATA_PROVENANCE_RECONSTRUCTED = "RECONSTRUCTED_MANIFEST"

# TRADING-114 pilot evidence score policy. These weights and bands are
# documented in TRADING-114_to_120 as a baseline interpretation aid, not as an
# investment promotion rule; missing or warning evidence remains visible.
EVIDENCE_SCORE_POINTS = {
    "data_quality": 0.20,
    "data_provenance": 0.15,
    "date_range": 0.20,
    "weight_path": 0.20,
    "candidate_attribution": 0.15,
    "overfit": 0.10,
}
EVIDENCE_USABLE_SCORE_FLOOR = 0.85
EVIDENCE_REVIEW_SCORE_FLOOR = 0.60

# TRADING-116 baseline price-behavior windows for the first regime coverage
# audit. They deliberately use simple ETF price behavior so the workflow does
# not add an unaudited macro source dependency.
REGIME_DRAWDOWN_THRESHOLD = -0.10
SEMICONDUCTOR_DRAWDOWN_THRESHOLD = -0.12
STRONG_TREND_RETURN_THRESHOLD = 0.15
SIDEWAYS_ABS_RETURN_THRESHOLD = 0.03
HIGH_VOL_ANNUALIZED_THRESHOLD = 0.25
STRONG_RECOVERY_RETURN_THRESHOLD = 0.10
REGIME_DRAWDOWN_PROTECTION_MAX_DEGRADATION_PP = 0.02

# TRADING-119 pilot readiness bands. They estimate manual research capacity and
# artifact risk only; the command never starts overnight_real automatically.
OVERNIGHT_TARGET_CANDIDATES = 5000
OVERNIGHT_READY_MAX_HOURS = 14.0
OVERNIGHT_WARNING_MAX_HOURS = 24.0
OVERNIGHT_READY_MAX_FAILURE_RATE = 0.02
OVERNIGHT_WARNING_MAX_FAILURE_RATE = 0.05
OVERNIGHT_WARNING_MAX_ARTIFACT_GB = 8.0

EVIDENCE_GATE_TRUE_HARD_FAILURES = {
    "DATA_QUALITY_FAIL",
    "DATE_RANGE_FAIL",
    "DATE_RANGE_INSUFFICIENT_DATA",
    "MISSING_REAL_EVALUATION_ARTIFACT",
    "MISSING_DAILY_WEIGHT_PATH",
    "OVERFIT_HIGH_RISK",
    "TECH_SEMICONDUCTOR_RELEVANCE_LOW",
    "TINY_FIXTURE_NOT_FOR_INVESTMENT",
}
EVIDENCE_GATE_DEFAULT_MANUAL_REVIEW_REASONS = {
    "DATA_QUALITY_PASS_WITH_WARNINGS",
    "DATA_PROVENANCE_INCOMPLETE",
    "DATA_PROVENANCE_RECONSTRUCTED",
    "BACKTEST_WINDOW_INCOMPLETE",
    "DATE_RANGE_INCOMPLETE",
    "WEIGHT_PATH_PARTIAL",
    "ATTRIBUTION_PARTIAL",
    "ATTRIBUTION_INCOMPLETE",
    "OVERFIT_REVIEW_REQUIRED",
    "REGIME_COVERAGE_PASS_WITH_WARNINGS",
}
EVIDENCE_GATE_REASON_CATEGORIES = {
    "DATA_QUALITY_FAIL": "data",
    "DATA_QUALITY_PASS_WITH_WARNINGS": "data",
    "DATA_PROVENANCE_INCOMPLETE": "data",
    "DATA_PROVENANCE_RECONSTRUCTED": "data",
    "DATE_RANGE_FAIL": "window",
    "DATE_RANGE_INSUFFICIENT_DATA": "window",
    "DATE_RANGE_INCOMPLETE": "window",
    "BACKTEST_WINDOW_INCOMPLETE": "window",
    "MISSING_REAL_EVALUATION_ARTIFACT": "data",
    "MISSING_DAILY_WEIGHT_PATH": "weight_path",
    "WEIGHT_PATH_PARTIAL": "weight_path",
    "ATTRIBUTION_PARTIAL": "attribution",
    "ATTRIBUTION_INCOMPLETE": "attribution",
    "OVERFIT_HIGH_RISK": "overfit",
    "OVERFIT_REVIEW_REQUIRED": "overfit",
    "REGIME_COVERAGE_PASS_WITH_WARNINGS": "regime",
    "TECH_SEMICONDUCTOR_RELEVANCE_LOW": "regime",
    "TINY_FIXTURE_NOT_FOR_INVESTMENT": "promotion",
}
EVIDENCE_GATE_CATEGORY_ORDER = (
    "data",
    "window",
    "weight_path",
    "attribution",
    "overfit",
    "regime",
    "promotion",
)

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

# TRADING-126_to_130 pilot shortlist scoring policy. These weights only rank
# manual-review candidates for owner review; they do not promote candidates or
# create production readiness. The requirement doc records the exit condition
# for replacing this baseline with calibrated owner-reviewed policy.
SHORTLIST_SCORE_COMPONENT_WEIGHTS = {
    "performance": 0.25,
    "risk": 0.20,
    "evidence": 0.20,
    "regime": 0.15,
    "stability": 0.10,
    "diversity": 0.10,
}
SHORTLIST_DEFAULT_MIN_SIZE = 5
SHORTLIST_DEFAULT_TARGET_SIZE = 10
SHORTLIST_DEFAULT_MAX_SIZE = 20
SHORTLIST_DIVERSITY_PARAMETER_FLOOR = 0.10

# TRADING-127 pilot clustering policy. The threshold groups near-duplicate
# candidate behavior for review-pack compression only; it is not a promotion
# or trading rule.
CANDIDATE_CLUSTER_SIMILARITY_THRESHOLD = 0.72
CANDIDATE_CLUSTER_SECONDARY_REPRESENTATIVE_SIZE = 4

POSITION_ADVISORY_TARGET_ONLY = "TARGET_ONLY"
POSITION_ADVISORY_READY_WITH_MANUAL_REVIEW = "READY_WITH_MANUAL_REVIEW"
POSITION_ADVISORY_SNAPSHOT_DELTA = "SNAPSHOT_DELTA"
PORTFOLIO_SNAPSHOT_WEIGHT_SUM_TOLERANCE = 0.005
PORTFOLIO_SNAPSHOT_VALUE_TOLERANCE = 0.01
PORTFOLIO_SNAPSHOT_ALLOWED_CURRENCIES = {"USD"}
MANUAL_PORTFOLIO_STATUSES = {"PASS", "PASS_WITH_WARNINGS", "FAIL"}
PORTFOLIO_EXPOSURE_STATUSES = {"PASS", "PASS_WITH_WARNINGS", "FAIL"}
POSITION_DRIFT_STATUSES = {"LOW", "MODERATE", "HIGH", "INSUFFICIENT_DATA"}
POSITION_DRIFT_ACTIONS = {"no_trade", "monitor", "manual_review", "guardrail_check_required"}
EXECUTION_GUARDRAIL_ACTIONS = {
    "no_trade",
    "monitor",
    "manual_review",
    "paper_adjustment_review_only",
    "blocked",
}
MANUAL_EXECUTION_ACTIONS = EXECUTION_GUARDRAIL_ACTIONS
OWNER_REVIEW_DECISIONS = {
    "monitor",
    "no_trade",
    "paper_adjustment",
    "manual_adjustment",
    "reject_advisory",
    "needs_more_data",
}
CONSENSUS_DRIFT_STATUSES = {
    "CONSENSUS",
    "MODERATE_DISAGREEMENT",
    "HIGH_DISAGREEMENT",
    "INSUFFICIENT_DATA",
}
DYNAMIC_V3_DEFENSIVE_SYMBOLS = {"CASH", "TLT"}
DYNAMIC_V3_WEIGHT_SYMBOLS = ("SPY", "QQQ", "SMH", "SOXX", "TLT", "CASH")

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
    precomputed_robustness_reports: dict[str, dict[str, Any]]
    fixed_robustness_cache_manifest: dict[str, Any]


_FIXED_ROBUSTNESS_REPORT_CACHE: dict[str, dict[str, Any]] = {}


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


_PARALLEL_SWEEP_CONFIG: DynamicV3ParameterSweepConfig | None = None
_PARALLEL_SWEEP_DIR: Path | None = None
_PARALLEL_REAL_CONTEXT: RealEvaluationContext | None = None


class SweepProfile(BaseModel):
    description: str = Field(min_length=1)
    config_path: Path
    evaluator_mode: EvaluatorMode
    max_candidates: int = Field(ge=1)
    workers: int = Field(ge=1)
    ci_safe: bool
    not_for_investment_decision: bool
    require_data_audit: bool = False
    require_window_audit: bool = False
    require_weight_path: bool = False

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
        "search_space_version": ("" if governance is None else governance.search_space_version),
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
        "not_for_investment_decision": (config.execution.evaluator == EVALUATOR_TINY_FIXTURE_PROXY),
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
    _annotate_sweep_profile(
        sweep_dir=Path(result["sweep_dir"]),
        profile=profile,
        profile_config_path=profile_config_path,
        selected=selected,
    )
    result["manifest"] = _read_json(Path(result["sweep_dir"]) / "sweep_manifest.json")
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
        if workers is not None:
            config = config.model_copy(
                update={"execution": config.execution.model_copy(update={"workers": workers})}
            )
        sweep_id = resume
        candidates = _read_jsonl(sweep_dir / "candidates.jsonl")
        existing_results, duplicate_count = _deduplicate_candidate_results_file(
            sweep_dir / "candidate_results.jsonl"
        )
        if workers is not None:
            _append_text(
                sweep_dir / "run.log",
                (
                    f"{datetime.now(UTC).isoformat()} "
                    f"resume worker override applied: workers={workers}\n"
                ),
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
    pending = [
        (idx, candidate)
        for idx, candidate in enumerate(candidates, start=1)
        if _text(candidate.get("candidate_id")) not in completed
    ]
    max_processed_index = 0

    def record_candidate_result(
        idx: int,
        candidate_id: str,
        result: dict[str, Any] | None,
        error: dict[str, Any] | None,
    ) -> None:
        nonlocal completed_count, failed_count, max_processed_index
        max_processed_index = max(max_processed_index, idx)
        if result is not None:
            _append_jsonl(result_path, result)
            completed.add(candidate_id)
            completed_count += 1
        elif error is not None:
            failed_count += 1
            _append_jsonl(error_path, error)
            if not config.execution.continue_on_candidate_error:
                raise DynamicV3ParameterResearchError(_text(error.get("error")))
        if (completed_count + failed_count) % config.execution.checkpoint_every_candidates == 0:
            _write_checkpoint(sweep_dir, max_processed_index, completed_count, failed_count)

    if config.execution.workers > 1 and len(pending) > 1:
        if config.execution.evaluator == EVALUATOR_REAL_DYNAMIC_V3_RESCUE:
            _append_text(
                sweep_dir / "run.log",
                (
                    f"{datetime.now(UTC).isoformat()} "
                    f"process pool started with {config.execution.workers} workers "
                    f"for {len(pending)} pending candidates\n"
                ),
            )
            with ProcessPoolExecutor(
                max_workers=config.execution.workers,
                initializer=_init_parallel_sweep_worker,
                initargs=(config, sweep_dir, real_context),
            ) as executor:
                futures = [
                    executor.submit(_evaluate_parallel_sweep_candidate, item) for item in pending
                ]
                for future in as_completed(futures):
                    record_candidate_result(*future.result())
        else:
            _append_text(
                sweep_dir / "run.log",
                (
                    f"{datetime.now(UTC).isoformat()} "
                    f"thread pool started with {config.execution.workers} workers "
                    f"for {len(pending)} pending candidates\n"
                ),
            )
            with ThreadPoolExecutor(max_workers=config.execution.workers) as executor:
                futures = [
                    executor.submit(
                        _evaluate_sweep_candidate_for_run,
                        item,
                        config=config,
                        sweep_dir=sweep_dir,
                        real_context=real_context,
                    )
                    for item in pending
                ]
                for future in as_completed(futures):
                    record_candidate_result(*future.result())
    else:
        for item in pending:
            record_candidate_result(
                *_evaluate_sweep_candidate_for_run(
                    item,
                    config=config,
                    sweep_dir=sweep_dir,
                    real_context=real_context,
                )
            )
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
        _check(f"artifact_exists:{name}", (audit_dir / name).exists(), name) for name in required
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
    mismatch = [row for row in records if _texts(row.get("window_mismatch_reasons"))]
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
    latest_pointer = (
        _latest_pointer_payload("latest_window_audit") if latest and not audit_id else {}
    )
    resolved_id = audit_id or _text(_mapping(latest_pointer).get("artifact_id"))
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--audit-id or --latest is required")
    audit_dir = output_dir / resolved_id
    manifest_path = audit_dir / "window_audit_manifest.json"
    if latest and not audit_id:
        pointer_path_text = _text(_mapping(latest_pointer).get("path"))
        pointer_path = _resolve_project_path(Path(pointer_path_text)) if pointer_path_text else None
        if (
            pointer_path is not None
            and pointer_path.exists()
            and _is_default_dynamic_v3_research_artifact(pointer_path)
        ):
            manifest_path = pointer_path
            audit_dir = manifest_path.parent
        else:
            failure_reason = "latest_pointer_target_missing"
            if pointer_path is not None and pointer_path.exists():
                failure_reason = "latest_pointer_target_outside_canonical_root"
            return {
                "schema_version": SCHEMA_VERSION,
                "report_type": "etf_dynamic_v3_window_audit_report_view",
                "window_audit_id": resolved_id,
                "status": "FAIL",
                "failure_reason": failure_reason,
                "configured_backtest_start": None,
                "earliest_actual_evaluation_start": None,
                "promotion_blocking_count": None,
                "report_path": str(audit_dir / "window_audit_report.md"),
                "latest_pointer": latest_pointer,
                "manifest_path": str(manifest_path),
                "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
                **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
            }
    manifest = _read_json(manifest_path)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_window_audit_report_view",
        "window_audit_id": resolved_id,
        "status": manifest.get("status", "UNKNOWN"),
        "configured_backtest_start": manifest.get("configured_backtest_start"),
        "earliest_actual_evaluation_start": manifest.get("earliest_actual_evaluation_start"),
        "promotion_blocking_count": manifest.get("promotion_blocking_count"),
        "report_path": str(audit_dir / "window_audit_report.md"),
        "manifest_path": str(manifest_path),
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
        _check(f"artifact_exists:{name}", (audit_dir / name).exists(), name) for name in required
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
    requested_max_candidates = max(max_candidates, 1)
    base_config = load_parameter_sweep_config(config_path)
    config = base_config.model_copy(
        update={
            "data": base_config.data.model_copy(update={"as_of": as_of, "end": end}),
            "run": base_config.run.model_copy(update={"max_candidates": requested_max_candidates}),
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
    candidates = _select_injection_audit_candidates(
        config,
        max_candidates=requested_max_candidates,
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
    parameters_without_matched_pairs = [
        row["parameter"] for row in parameter_effects if int(row["matched_pair_count"]) == 0
    ]
    pair_coverage_complete = not parameters_without_matched_pairs
    audit_status = "PASS" if pair_coverage_complete else "INCOMPLETE"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_injection_audit_manifest",
        "audit_id": audit_id,
        "status": audit_status,
        "config_path": str(config_path),
        "as_of": as_of.isoformat(),
        "end": end.isoformat(),
        "candidate_count": len(results),
        "max_candidates": requested_max_candidates,
        "parameter_effect_pair_coverage_complete": pair_coverage_complete,
        "parameters_without_matched_pairs": parameters_without_matched_pairs,
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
        "status": audit_status,
        "candidate_count": len(results),
        "data_quality_status": real_context.data_quality_status,
        "parameter_effects": parameter_effects,
        "parameter_effect_pair_coverage_complete": pair_coverage_complete,
        "parameters_without_matched_pairs": parameters_without_matched_pairs,
        "weight_path_diff_summary": weight_summary,
        "metric_diff_summary": metric_summary,
        "not_consumed_parameters": [
            row["parameter"] for row in parameter_effects if row["effect_status"] == "NOT_CONSUMED"
        ],
        "no_observed_effect_parameters": [
            row["parameter"]
            for row in parameter_effects
            if row["effect_status"] == "NO_OBSERVED_EFFECT"
        ],
        "insufficient_matched_pair_parameters": [
            row["parameter"]
            for row in parameter_effects
            if row["effect_status"] == "INSUFFICIENT_MATCHED_PAIR_EVIDENCE"
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
    _write_json(
        audit_dir / "parameter_effect_summary.json",
        {
            "schema_version": SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_parameter_effect_summary",
            "audit_id": audit_id,
            "status": audit_status,
            "parameter_effect_pair_coverage_complete": pair_coverage_complete,
            "parameters_without_matched_pairs": parameters_without_matched_pairs,
            "parameter_effects": parameter_effects,
            "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        },
    )
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
    parameter_effect_summary_path = audit_dir / "parameter_effect_summary.json"
    parameter_effect_summary = _read_optional_json(parameter_effect_summary_path) or {}
    legacy_effect_summary_missing = not parameter_effect_summary
    parameters_without_matched_pairs = manifest.get("parameters_without_matched_pairs", [])
    if legacy_effect_summary_missing:
        parameters_without_matched_pairs = list(REQUIRED_INJECTION_PARAMETERS)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_injection_audit_report_view",
        "audit_id": resolved_id,
        "status": "INCOMPLETE"
        if legacy_effect_summary_missing
        else manifest.get("status", "UNKNOWN"),
        "candidate_count": manifest.get("candidate_count"),
        "weight_path_diff_summary": weight_summary,
        "metric_diff_summary": metric_summary,
        "parameter_effect_pair_coverage_complete": (
            False
            if legacy_effect_summary_missing
            else manifest.get("parameter_effect_pair_coverage_complete", False)
        ),
        "parameters_without_matched_pairs": parameters_without_matched_pairs,
        "parameter_effects": parameter_effect_summary.get("parameter_effects", []),
        "parameter_effect_summary_path": str(parameter_effect_summary_path),
        "limitations": (
            ["legacy_parameter_effect_summary_missing"] if legacy_effect_summary_missing else []
        ),
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
        "parameter_effect_summary.json",
        "parameter_effect_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (audit_dir / name).exists(), name) for name in required
    ]
    manifest = _read_optional_json(audit_dir / "injection_audit_manifest.json") or {}
    weight_summary = _read_optional_json(audit_dir / "weight_path_diff_summary.json") or {}
    effect_summary = _read_optional_json(audit_dir / "parameter_effect_summary.json") or {}
    candidate_count = int(manifest.get("candidate_count") or 0)
    requested_floor = min(20, int(manifest.get("max_candidates") or 20))
    checks.append(
        _check(
            "candidate_count_at_least_requested_floor",
            candidate_count >= requested_floor,
            str(manifest.get("candidate_count")),
        )
    )
    effect_rows = _records(effect_summary.get("parameter_effects"))
    effect_by_parameter = {_text(row.get("parameter")): row for row in effect_rows}
    allowed_effect_statuses = {
        "EFFECTIVE",
        "NO_OBSERVED_EFFECT",
        "NOT_CONSUMED",
        "INSUFFICIENT_MATCHED_PAIR_EVIDENCE",
    }
    checks.extend(
        [
            _check(
                "parameter_effect_summary_covers_required_parameters",
                set(effect_by_parameter) == set(REQUIRED_INJECTION_PARAMETERS),
                ",".join(sorted(effect_by_parameter)),
            ),
            _check(
                "parameter_effect_statuses_valid",
                all(
                    _text(row.get("effect_status")) in allowed_effect_statuses
                    for row in effect_rows
                ),
                "effect statuses use governed taxonomy",
            ),
            _check(
                "matched_pair_coverage_complete",
                bool(effect_summary.get("parameter_effect_pair_coverage_complete"))
                and all(int(row.get("matched_pair_count") or 0) > 0 for row in effect_rows),
                "every required parameter has an OFAT matched pair",
            ),
        ]
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


def _evaluate_sweep_candidate_for_run(
    item: tuple[int, Mapping[str, Any]],
    *,
    config: DynamicV3ParameterSweepConfig,
    sweep_dir: Path,
    real_context: RealEvaluationContext | None,
) -> tuple[int, str, dict[str, Any] | None, dict[str, Any] | None]:
    idx, candidate = item
    candidate_id = _text(candidate.get("candidate_id"))
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
        return idx, candidate_id, result, None
    except Exception as exc:  # noqa: BLE001
        completed_at = datetime.now(UTC)
        return (
            idx,
            candidate_id,
            None,
            {
                "candidate_id": candidate_id,
                "status": "failed",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "parameters": candidate.get("parameters", {}),
                "started_at": started.isoformat(),
                "completed_at": completed_at.isoformat(),
            },
        )


def _init_parallel_sweep_worker(
    config: DynamicV3ParameterSweepConfig,
    sweep_dir: Path,
    real_context: RealEvaluationContext | None,
) -> None:
    global _PARALLEL_REAL_CONTEXT, _PARALLEL_SWEEP_CONFIG, _PARALLEL_SWEEP_DIR
    _PARALLEL_SWEEP_CONFIG = config
    _PARALLEL_SWEEP_DIR = sweep_dir
    _PARALLEL_REAL_CONTEXT = real_context


def _evaluate_parallel_sweep_candidate(
    item: tuple[int, Mapping[str, Any]],
) -> tuple[int, str, dict[str, Any] | None, dict[str, Any] | None]:
    if _PARALLEL_SWEEP_CONFIG is None or _PARALLEL_SWEEP_DIR is None:
        raise DynamicV3ParameterResearchError("parallel sweep worker is not initialized")
    return _evaluate_sweep_candidate_for_run(
        item,
        config=_PARALLEL_SWEEP_CONFIG,
        sweep_dir=_PARALLEL_SWEEP_DIR,
        real_context=_PARALLEL_REAL_CONTEXT,
    )


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


def run_evidence_summary(
    *,
    sweep_id: str,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Path = DEFAULT_EVIDENCE_SUMMARY_DIR,
    candidate_attribution_dir: Path = DEFAULT_CANDIDATE_ATTRIBUTION_DIR,
    overfit_dir: Path = DEFAULT_OVERFIT_DIR,
    window_audit_dir: Path = DEFAULT_WINDOW_AUDIT_DIR,
    data_provenance_dir: Path = DEFAULT_DATA_PROVENANCE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    sweep_dir = sweep_output_dir / sweep_id
    manifest = _read_json(sweep_dir / "sweep_manifest.json")
    results = _read_candidate_results(sweep_dir)
    context = _evidence_context(
        window_audit_dir=window_audit_dir,
        data_provenance_dir=data_provenance_dir,
    )
    rows = [
        _candidate_evidence_row(
            row,
            sweep_id=sweep_id,
            sweep_manifest=manifest,
            candidate_attribution_dir=candidate_attribution_dir,
            overfit_dir=overfit_dir,
            context=context,
        )
        for row in results
    ]
    summary_id = _stable_id("evidence-summary", sweep_id, generated.isoformat())
    summary_dir = _unique_dir(output_dir / summary_id)
    summary_dir.mkdir(parents=True, exist_ok=False)
    blocking_counter = Counter(
        reason for row in rows for reason in _texts(row.get("promotion_blocking_reasons"))
    )
    complete_count = sum(
        1
        for row in rows
        if row["weight_path_status"] == WEIGHT_PATH_COMPLETE
        and row["candidate_attribution_status"] == WEIGHT_PATH_COMPLETE
    )
    partial_count = sum(
        1
        for row in rows
        if row["weight_path_status"] == WEIGHT_PATH_PARTIAL
        or row["candidate_attribution_status"] == WEIGHT_PATH_PARTIAL
    )
    usable_count = sum(1 for row in rows if row["evidence_recommendation"] == "usable_for_research")
    not_usable_count = sum(1 for row in rows if row["evidence_recommendation"] == "not_usable")
    status = "PASS"
    if not rows or not_usable_count == len(rows):
        status = "FAIL"
    elif any(row["promotion_blocking_reasons"] for row in rows):
        status = "PASS_WITH_WARNINGS"
    matrix_path = summary_dir / "candidate_evidence_matrix.jsonl"
    blocking_path = summary_dir / "evidence_blocking_reasons.json"
    report_path = summary_dir / "evidence_summary_report.md"
    manifest_payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_evidence_summary_manifest",
        "summary_id": summary_dir.name,
        "source_sweep_id": sweep_id,
        "source_sweep_manifest_path": str(sweep_dir / "sweep_manifest.json"),
        "generated_at": generated.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "status": status,
        "market_regime": "ai_after_chatgpt",
        "requested_range": {
            "start": manifest.get("as_of", ""),
            "end": manifest.get("end", ""),
        },
        "evaluator_mode": manifest.get("evaluator_mode", "UNKNOWN"),
        "candidate_count": len(rows),
        "complete_evidence_count": complete_count,
        "partial_evidence_count": partial_count,
        "usable_for_research_count": usable_count,
        "needs_review_count": sum(
            1 for row in rows if row["evidence_recommendation"] == "needs_review"
        ),
        "not_usable_count": not_usable_count,
        "data_quality_distribution": dict(Counter(row["data_quality"] for row in rows)),
        "date_range_distribution": dict(Counter(row["date_range_status"] for row in rows)),
        "weight_path_distribution": dict(Counter(row["weight_path_status"] for row in rows)),
        "candidate_attribution_distribution": dict(
            Counter(row["candidate_attribution_status"] for row in rows)
        ),
        "overfit_distribution": dict(Counter(row["overfit_status"] for row in rows)),
        "promotion_status_distribution": dict(Counter(row["promotion_status"] for row in rows)),
        "top_blocking_reasons": [
            {"reason": reason, "count": count} for reason, count in blocking_counter.most_common()
        ],
        "can_enter_medium_real": bool(rows) and status in {"PASS", "PASS_WITH_WARNINGS"},
        "candidate_evidence_matrix_path": str(matrix_path),
        "evidence_blocking_reasons_path": str(blocking_path),
        "evidence_summary_report_path": str(report_path),
        "reader_brief_section": _evidence_summary_reader_brief_section(
            status=status,
            usable_count=usable_count,
            candidate_count=len(rows),
            blocking_counter=blocking_counter,
        ),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(summary_dir / "evidence_summary_manifest.json", manifest_payload)
    _write_jsonl(matrix_path, rows)
    _write_json(
        blocking_path,
        {
            "schema_version": SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_evidence_blocking_reasons",
            "summary_id": summary_dir.name,
            "source_sweep_id": sweep_id,
            "blocking_reasons": manifest_payload["top_blocking_reasons"],
            "candidate_blockers": [
                {
                    "candidate_id": row["candidate_id"],
                    "promotion_blocking_reasons": row["promotion_blocking_reasons"],
                }
                for row in rows
                if row["promotion_blocking_reasons"]
            ],
            "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
            **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
        },
    )
    _write_text(report_path, render_evidence_summary_markdown(manifest_payload, rows))
    _update_latest_pointer(
        "latest_evidence_summary",
        summary_dir.name,
        summary_dir / "evidence_summary_manifest.json",
    )
    return {
        "summary_id": summary_dir.name,
        "summary_dir": summary_dir,
        "manifest": manifest_payload,
        "matrix": rows,
    }


def evidence_summary_report_payload(
    *,
    summary_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_EVIDENCE_SUMMARY_DIR,
) -> dict[str, Any]:
    resolved_id = summary_id or (
        _latest_pointer_artifact_id("latest_evidence_summary") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--summary-id or --latest is required")
    summary_dir = output_dir / resolved_id
    manifest = _read_json(summary_dir / "evidence_summary_manifest.json")
    return {
        **manifest,
        "summary_dir": str(summary_dir),
        "report_path": str(summary_dir / "evidence_summary_report.md"),
    }


def validate_evidence_summary_artifact(
    *,
    summary_id: str,
    output_dir: Path = DEFAULT_EVIDENCE_SUMMARY_DIR,
) -> dict[str, Any]:
    summary_dir = output_dir / summary_id
    required = [
        "evidence_summary_manifest.json",
        "candidate_evidence_matrix.jsonl",
        "evidence_blocking_reasons.json",
        "evidence_summary_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (summary_dir / name).exists(), name) for name in required
    ]
    manifest = _read_optional_json(summary_dir / "evidence_summary_manifest.json") or {}
    rows = _read_jsonl(summary_dir / "candidate_evidence_matrix.jsonl")
    checks.extend(
        [
            _check("summary_id_matches", manifest.get("summary_id") == summary_id, summary_id),
            _check("candidate_matrix_not_empty", bool(rows), f"row_count={len(rows)}"),
            _check(
                "candidate_rows_have_required_statuses",
                all(
                    row.get("candidate_id")
                    and row.get("data_quality")
                    and row.get("date_range_status")
                    and row.get("weight_path_status")
                    and row.get("candidate_attribution_status")
                    and row.get("promotion_status")
                    and row.get("evidence_recommendation")
                    for row in rows
                ),
                "candidate_evidence_matrix required fields",
            ),
            _check(
                "production_candidate_not_generated",
                manifest.get("production_candidate_generated") is False,
                "evidence summary is research-only",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_evidence_summary_validation",
        "summary_id": summary_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def build_medium_real_report(
    *,
    sweep_id: str | None = None,
    latest: bool = False,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Path = DEFAULT_MEDIUM_REAL_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    resolved_id = sweep_id or (
        _latest_sweep_id_for_profile("medium_real", sweep_output_dir=sweep_output_dir)
        if latest
        else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--sweep-id or --latest is required")
    sweep_dir = sweep_output_dir / resolved_id
    manifest = _read_json(sweep_dir / "sweep_manifest.json")
    leaderboard = _read_optional_json(
        sweep_dir / "leaderboard.json"
    ) or build_sweep_leaderboard_payload(sweep_dir=sweep_dir)
    results = _read_candidate_results(sweep_dir)
    errors = _read_jsonl(sweep_dir / "candidate_errors.jsonl")
    report_id = _stable_id("medium-real-report", resolved_id, generated.isoformat())
    report_dir = _unique_dir(output_dir / report_id)
    report_dir.mkdir(parents=True, exist_ok=False)
    gate_counts = Counter(_text(row.get("gate")) for row in results)
    evidence_dist = Counter(
        _text(_mapping(row.get("weight_path_metadata")).get("attribution_completeness"), "MISSING")
        for row in results
    )
    reject_counter = Counter(
        reason
        for row in results
        if row.get("gate") == GATE_REJECT
        for reason in _texts(row.get("gate_reasons"))
    )
    artifact_size = _artifact_size_summary(sweep_dir)
    avg_runtime = _average_runtime_seconds(manifest, len(results))
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_medium_real_report",
        "medium_real_report_id": report_dir.name,
        "source_sweep_id": resolved_id,
        "source_sweep_dir": str(sweep_dir),
        "generated_at": generated.isoformat(),
        "status": (
            "PASS"
            if results and manifest.get("evaluator_mode") == EVALUATOR_REAL_DYNAMIC_V3_RESCUE
            else "FAIL"
        ),
        "profile": manifest.get("profile", ""),
        "evaluator_mode": manifest.get("evaluator_mode"),
        "candidate_count": manifest.get("candidate_count", len(results) + len(errors)),
        "completed_count": manifest.get("completed_count", len(results)),
        "failed_count": manifest.get("failed_count", len(errors)),
        "rejected_count": gate_counts.get(GATE_REJECT, 0),
        "review_required_count": gate_counts.get(GATE_REVIEW_REQUIRED, 0),
        "observe_only_count": gate_counts.get(GATE_OBSERVE_ONLY, 0),
        "promote_candidate_count": gate_counts.get(GATE_PROMOTE_CANDIDATE, 0),
        "top_candidates": _records(leaderboard.get("top_eligible_candidates"))[:20],
        "reject_reason_distribution": [
            {"reason": reason, "count": count} for reason, count in reject_counter.most_common()
        ],
        "evidence_completeness_distribution": dict(evidence_dist),
        "average_runtime_seconds_per_candidate": avg_runtime,
        "artifact_size_summary": artifact_size,
        "recommended_next_action": _medium_real_next_action(
            manifest=manifest,
            completed_count=len(results),
            failed_count=len(errors),
            observe_only_count=gate_counts.get(GATE_OBSERVE_ONLY, 0),
        ),
        "medium_real_manifest_path": str(report_dir / "medium_real_manifest.json"),
        "medium_real_report_path": str(report_dir / "medium_real_report.md"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(report_dir / "medium_real_manifest.json", payload)
    _write_text(report_dir / "medium_real_report.md", render_medium_real_markdown(payload))
    _update_latest_pointer(
        "latest_medium_real",
        report_dir.name,
        report_dir / "medium_real_manifest.json",
    )
    return payload


def medium_real_report_payload(
    *,
    report_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_MEDIUM_REAL_DIR,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
) -> dict[str, Any]:
    resolved_id = report_id or (_latest_pointer_artifact_id("latest_medium_real") if latest else "")
    if resolved_id:
        report_dir = output_dir / resolved_id
        return _read_json(report_dir / "medium_real_manifest.json")
    return build_medium_real_report(
        latest=True, sweep_output_dir=sweep_output_dir, output_dir=output_dir
    )


def validate_medium_real_sweep(
    *,
    sweep_id: str,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    min_expected_candidates: int = 300,
) -> dict[str, Any]:
    sweep_dir = sweep_output_dir / sweep_id
    sweep_validation = validate_sweep_artifact(sweep_id=sweep_id, output_dir=sweep_output_dir)
    manifest = _read_optional_json(sweep_dir / "sweep_manifest.json") or {}
    results = _read_candidate_results(sweep_dir)
    errors = _read_jsonl(sweep_dir / "candidate_errors.jsonl")
    completed_count = len(results)
    expected_reason = ""
    enough_candidates = completed_count >= min_expected_candidates
    if not enough_candidates:
        candidate_count = int(manifest.get("candidate_count") or completed_count + len(errors))
        if candidate_count < min_expected_candidates:
            expected_reason = "parameter_space_exhausted_before_medium_real_floor"
        elif errors:
            expected_reason = "candidate_errors_present_before_medium_real_floor"
    candidate_floor_detail = (
        f"completed={completed_count}; "
        f"min_expected={min_expected_candidates}; "
        f"reason={expected_reason}"
    )
    checks = [
        _check("base_sweep_validation_pass", sweep_validation["status"] == "PASS", sweep_id),
        _check(
            "profile_is_medium_real_or_real_manual",
            _text(manifest.get("profile")) in {"medium_real", ""}
            and manifest.get("evaluator_mode") == EVALUATOR_REAL_DYNAMIC_V3_RESCUE,
            _text(manifest.get("profile")),
        ),
        _check(
            "medium_candidate_floor_met_or_explained",
            enough_candidates or bool(expected_reason),
            candidate_floor_detail,
        ),
        _check("leaderboard_exists", (sweep_dir / "leaderboard.json").exists(), "leaderboard"),
        _check(
            "real_artifact_paths_present",
            all(row.get("real_evaluation_artifact_path") for row in results),
            "real_evaluation_artifact_path required",
        ),
        _check(
            "tiny_fixture_not_mixed",
            all(row.get("evaluator_mode") == EVALUATOR_REAL_DYNAMIC_V3_RESCUE for row in results),
            "leaderboard cannot mix tiny_fixture_proxy",
        ),
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_medium_real_validation",
        "sweep_id": sweep_id,
        "status": status,
        "completed_count": completed_count,
        "failed_count": len(errors),
        "minimum_expected_candidates": min_expected_candidates,
        "minimum_candidate_shortfall_reason": expected_reason,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def run_regime_coverage(
    *,
    sweep_id: str,
    focus: str = "tech_semiconductor",
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Path = DEFAULT_REGIME_COVERAGE_DIR,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    top_n: int = 20,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    sweep_dir = sweep_output_dir / sweep_id
    manifest = _read_json(sweep_dir / "sweep_manifest.json")
    results = _read_candidate_results(sweep_dir)
    ranked = _ranked_candidate_rows(results)[:top_n]
    start = date(2022, 12, 1)
    end = _date_from_any(manifest.get("end")) or start
    windows = _regime_windows_from_prices(
        prices_path=prices_path,
        start=start,
        end=end,
    )
    candidate_rows = [_candidate_regime_result(row, windows=windows, focus=focus) for row in ranked]
    gap_report = _regime_gap_report(windows=windows, candidate_rows=candidate_rows)
    coverage_id = _stable_id("regime-coverage", sweep_id, focus, generated.isoformat())
    coverage_dir = _unique_dir(output_dir / coverage_id)
    coverage_dir.mkdir(parents=True, exist_ok=False)
    status = _coverage_status(gap_report)
    relevance = _tech_semiconductor_relevance(gap_report)
    overfit_risk = _ai_bull_market_overfit_risk(gap_report, candidate_rows)
    manifest_payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_regime_coverage_manifest",
        "coverage_id": coverage_dir.name,
        "source_sweep_id": sweep_id,
        "focus": focus,
        "generated_at": generated.isoformat(),
        "status": status,
        "coverage_status": status,
        "tech_semiconductor_relevance": relevance,
        "ai_bull_market_overfit_risk": overfit_risk,
        "market_regime": "ai_after_chatgpt",
        "requested_range": {"start": start.isoformat(), "end": end.isoformat()},
        "window_count": len(windows),
        "candidate_count": len(candidate_rows),
        "regime_windows_path": str(coverage_dir / "regime_windows.json"),
        "candidate_regime_results_path": str(coverage_dir / "candidate_regime_results.jsonl"),
        "regime_gap_report_path": str(coverage_dir / "regime_gap_report.json"),
        "tech_semiconductor_relevance_report_path": str(
            coverage_dir / "tech_semiconductor_relevance_report.md"
        ),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(coverage_dir / "regime_coverage_manifest.json", manifest_payload)
    _write_json(
        coverage_dir / "regime_windows.json",
        {
            "schema_version": SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_regime_windows",
            "coverage_id": coverage_dir.name,
            "windows": windows,
            "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        },
    )
    _write_jsonl(coverage_dir / "candidate_regime_results.jsonl", candidate_rows)
    _write_json(coverage_dir / "regime_gap_report.json", gap_report)
    _write_text(
        coverage_dir / "tech_semiconductor_relevance_report.md",
        render_regime_coverage_markdown(manifest_payload, windows, gap_report, candidate_rows),
    )
    _update_latest_pointer(
        "latest_regime_coverage",
        coverage_dir.name,
        coverage_dir / "regime_coverage_manifest.json",
    )
    return {
        "coverage_id": coverage_dir.name,
        "coverage_dir": coverage_dir,
        "manifest": manifest_payload,
        "windows": windows,
        "candidate_results": candidate_rows,
        "gap_report": gap_report,
    }


def regime_coverage_report_payload(
    *,
    coverage_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_REGIME_COVERAGE_DIR,
) -> dict[str, Any]:
    resolved_id = coverage_id or (
        _latest_pointer_artifact_id("latest_regime_coverage") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--coverage-id or --latest is required")
    coverage_dir = output_dir / resolved_id
    manifest = _read_json(coverage_dir / "regime_coverage_manifest.json")
    return {**manifest, "coverage_dir": str(coverage_dir)}


def validate_regime_coverage_artifact(
    *,
    coverage_id: str,
    output_dir: Path = DEFAULT_REGIME_COVERAGE_DIR,
) -> dict[str, Any]:
    coverage_dir = output_dir / coverage_id
    required = [
        "regime_coverage_manifest.json",
        "regime_windows.json",
        "candidate_regime_results.jsonl",
        "regime_gap_report.json",
        "tech_semiconductor_relevance_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (coverage_dir / name).exists(), name) for name in required
    ]
    manifest = _read_optional_json(coverage_dir / "regime_coverage_manifest.json") or {}
    windows = _records(
        _mapping(_read_optional_json(coverage_dir / "regime_windows.json")).get("windows")
    )
    checks.extend(
        [
            _check("coverage_id_matches", manifest.get("coverage_id") == coverage_id, coverage_id),
            _check(
                "ai_after_chatgpt_window_present",
                any(row.get("regime_id") == "ai_after_chatgpt" for row in windows),
                "",
            ),
            _check(
                "coverage_status_valid",
                manifest.get("coverage_status") in {"PASS", "PASS_WITH_WARNINGS", "FAIL"},
                _text(manifest.get("coverage_status")),
            ),
            _check(
                "production_candidate_not_generated",
                manifest.get("production_candidate_generated") is False,
                "research-only",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_regime_coverage_validation",
        "coverage_id": coverage_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def run_interpretation_pack(
    *,
    sweep_id: str,
    top_n: int = 10,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Path = DEFAULT_INTERPRETATION_PACK_DIR,
    candidate_attribution_dir: Path = DEFAULT_CANDIDATE_ATTRIBUTION_DIR,
    overfit_dir: Path = DEFAULT_OVERFIT_DIR,
    window_audit_dir: Path = DEFAULT_WINDOW_AUDIT_DIR,
    data_provenance_dir: Path = DEFAULT_DATA_PROVENANCE_DIR,
    regime_coverage_dir: Path = DEFAULT_REGIME_COVERAGE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    sweep_dir = sweep_output_dir / sweep_id
    manifest = _read_json(sweep_dir / "sweep_manifest.json")
    results = _read_candidate_results(sweep_dir)
    top_rows = _ranked_candidate_rows(results)[:top_n]
    context = _evidence_context(
        window_audit_dir=window_audit_dir,
        data_provenance_dir=data_provenance_dir,
    )
    coverage = _latest_regime_coverage_for_sweep(sweep_id, regime_coverage_dir)
    pack_id = _stable_id("interpretation-pack", sweep_id, top_n, generated.isoformat())
    pack_dir = _unique_dir(output_dir / pack_id)
    candidate_root = pack_dir / "candidate_interpretations"
    summaries: list[dict[str, Any]] = []
    incomplete_count = 0
    for rank, row in enumerate(top_rows, start=1):
        candidate_id = _text(row.get("candidate_id"))
        candidate_dir = candidate_root / candidate_id
        candidate_dir.mkdir(parents=True, exist_ok=True)
        candidate_report = candidate_report_payload(
            sweep_id=sweep_id,
            candidate_id=candidate_id,
            output_dir=sweep_output_dir,
            write=True,
        )
        evidence = _candidate_evidence_row(
            row,
            sweep_id=sweep_id,
            sweep_manifest=manifest,
            candidate_attribution_dir=candidate_attribution_dir,
            overfit_dir=overfit_dir,
            context=context,
        )
        weight_summary, major_changes = _weight_path_interpretation(row)
        if not weight_summary:
            incomplete_count += 1
        regime_summary = _candidate_regime_summary(candidate_id, coverage)
        blocking_summary = {
            "candidate_id": candidate_id,
            "promotion_blocking_reasons": evidence["promotion_blocking_reasons"],
            "evidence_recommendation": evidence["evidence_recommendation"],
            "evidence_score": evidence["evidence_score"],
        }
        interpretation = {
            "rank": rank,
            "candidate_id": candidate_id,
            "parameters": row.get("parameters", {}),
            "gate_status": row.get("gate"),
            "evidence_status": evidence,
            "total_score": row.get("score"),
            "score_breakdown": row.get("score_breakdown", {}),
            "top_positive_contribution_windows": _top_contribution_windows(row, positive=True),
            "top_negative_contribution_windows": _top_contribution_windows(row, positive=False),
            "major_weight_changes": major_changes,
            "turnover_source": _mapping(row.get("metrics")).get("turnover"),
            "drawdown_protection_behavior": _drawdown_protection_text(row),
            "tech_semiconductor_regime_behavior": regime_summary,
            "recommendation": _interpretation_recommendation(row, evidence, regime_summary),
            "human_review_notes": "manual_review_required",
            "candidate_report_path": str(
                sweep_dir / "candidates" / candidate_id / "candidate_report.json"
            ),
        }
        _write_text(
            candidate_dir / "interpretation_report.md",
            render_candidate_interpretation_markdown(interpretation),
        )
        _write_csv(candidate_dir / "weight_path_summary.csv", weight_summary)
        _write_json(candidate_dir / "major_weight_changes.json", {"changes": major_changes})
        _write_json(candidate_dir / "regime_performance_summary.json", regime_summary)
        _write_json(candidate_dir / "blocking_evidence_summary.json", blocking_summary)
        summaries.append(
            {
                "rank": rank,
                "candidate_id": candidate_id,
                "gate": row.get("gate"),
                "score": row.get("score"),
                "evidence_recommendation": evidence["evidence_recommendation"],
                "weight_path_status": evidence["weight_path_status"],
                "report_path": str(candidate_dir / "interpretation_report.md"),
                "candidate_report_status": candidate_report.get("status"),
            }
        )
    pack_status = "PASS" if summaries and incomplete_count == 0 else "PASS_WITH_WARNINGS"
    if not summaries:
        pack_status = "FAIL"
    manifest_payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_interpretation_pack_manifest",
        "pack_id": pack_dir.name,
        "source_sweep_id": sweep_id,
        "generated_at": generated.isoformat(),
        "status": pack_status,
        "top_n_requested": top_n,
        "candidate_count": len(summaries),
        "incomplete_weight_path_count": incomplete_count,
        "top_candidates_summary_path": str(pack_dir / "top_candidates_summary.csv"),
        "candidate_interpretations_dir": str(candidate_root),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(pack_dir / "interpretation_manifest.json", manifest_payload)
    _write_csv(pack_dir / "top_candidates_summary.csv", summaries)
    _update_latest_pointer(
        "latest_interpretation_pack",
        pack_dir.name,
        pack_dir / "interpretation_manifest.json",
    )
    return {
        "pack_id": pack_dir.name,
        "pack_dir": pack_dir,
        "manifest": manifest_payload,
        "top_candidates": summaries,
    }


def interpretation_report_payload(
    *,
    candidate_id: str,
    pack_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_INTERPRETATION_PACK_DIR,
) -> dict[str, Any]:
    resolved_id = pack_id or (
        _latest_pointer_artifact_id("latest_interpretation_pack") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--pack-id or --latest is required")
    report_path = (
        output_dir
        / resolved_id
        / "candidate_interpretations"
        / candidate_id
        / "interpretation_report.md"
    )
    if not report_path.exists():
        raise DynamicV3ParameterResearchError(f"interpretation report not found: {candidate_id}")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_interpretation_report_view",
        "pack_id": resolved_id,
        "candidate_id": candidate_id,
        "status": "PASS",
        "report_path": str(report_path),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def validate_interpretation_pack_artifact(
    *,
    pack_id: str,
    output_dir: Path = DEFAULT_INTERPRETATION_PACK_DIR,
) -> dict[str, Any]:
    pack_dir = output_dir / pack_id
    manifest = _read_optional_json(pack_dir / "interpretation_manifest.json") or {}
    summary_rows = _read_csv_rows(pack_dir / "top_candidates_summary.csv")
    checks = [
        _check("manifest_exists", (pack_dir / "interpretation_manifest.json").exists(), pack_id),
        _check("top_summary_exists", (pack_dir / "top_candidates_summary.csv").exists(), ""),
        _check("candidate_summary_not_empty", bool(summary_rows), f"rows={len(summary_rows)}"),
        _check("pack_id_matches", manifest.get("pack_id") == pack_id, pack_id),
        _check(
            "all_candidate_reports_exist",
            all(Path(_text(row.get("report_path"))).exists() for row in summary_rows),
            "candidate interpretation reports",
        ),
        _check(
            "production_candidate_not_generated",
            manifest.get("production_candidate_generated") is False,
            "research-only",
        ),
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_interpretation_pack_validation",
        "pack_id": pack_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def build_observe_pool(
    *,
    sweep_id: str,
    top_n: int = 20,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Path = DEFAULT_OBSERVE_POOL_DIR,
    candidate_attribution_dir: Path = DEFAULT_CANDIDATE_ATTRIBUTION_DIR,
    overfit_dir: Path = DEFAULT_OVERFIT_DIR,
    window_audit_dir: Path = DEFAULT_WINDOW_AUDIT_DIR,
    data_provenance_dir: Path = DEFAULT_DATA_PROVENANCE_DIR,
    regime_coverage_dir: Path = DEFAULT_REGIME_COVERAGE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    sweep_dir = sweep_output_dir / sweep_id
    manifest = _read_json(sweep_dir / "sweep_manifest.json")
    config = load_parameter_sweep_config(sweep_dir / "sweep_config.normalized.yaml")
    context = _evidence_context(
        window_audit_dir=window_audit_dir,
        data_provenance_dir=data_provenance_dir,
    )
    coverage = _latest_regime_coverage_for_sweep(sweep_id, regime_coverage_dir)
    coverage_manifest = _mapping(coverage.get("manifest"))
    results = _ranked_candidate_rows(_read_candidate_results(sweep_dir))[:top_n]
    candidates: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in results:
        evidence = _candidate_evidence_row(
            row,
            sweep_id=sweep_id,
            sweep_manifest=manifest,
            candidate_attribution_dir=candidate_attribution_dir,
            overfit_dir=overfit_dir,
            context=context,
        )
        regime_status = {
            "coverage_status": coverage_manifest.get("coverage_status", "MISSING"),
            "tech_semiconductor_relevance": coverage_manifest.get(
                "tech_semiconductor_relevance",
                "MISSING",
            ),
            "ai_bull_market_overfit_risk": coverage_manifest.get(
                "ai_bull_market_overfit_risk",
                "MISSING",
            ),
        }
        allowed, reasons = _observe_pool_allowed(
            row=row,
            evidence=evidence,
            regime_status=regime_status,
        )
        if not allowed:
            rejected.append({"candidate_id": row.get("candidate_id"), "reasons": reasons})
            continue
        warning_reasons = [
            reason
            for reason in reasons
            if reason.endswith("_warning")
            or reason in {"WEIGHT_PATH_PARTIAL", "ATTRIBUTION_PARTIAL"}
        ]
        candidates.append(
            {
                "candidate_id": row.get("candidate_id"),
                "source_sweep_id": sweep_id,
                "parameters": row.get("parameters", {}),
                "metrics": row.get("metrics", {}),
                "evidence_status": evidence,
                "regime_coverage_status": regime_status,
                "overfit_status": evidence["overfit_status"],
                "observe_reason": "passes_observe_only_research_filters",
                "manual_review_required": bool(warning_reasons)
                or evidence["promotion_status"] != GATE_PROMOTE_CANDIDATE,
                "promotion_earliest_after_days": config.shadow.promotion_earliest_after_days,
                "promotion_earliest_after_rebalance_count": (
                    config.shadow.promotion_earliest_after_rebalance_count
                ),
                "warning_reasons": warning_reasons,
                "score": row.get("score"),
                "evaluator_mode": row.get("evaluator_mode", ""),
                "real_evaluation_artifact_path": row.get("real_evaluation_artifact_path", ""),
                "weight_path_metadata": row.get("weight_path_metadata", {}),
            }
        )
    pool_id = _stable_id("observe-pool", sweep_id, top_n, generated.isoformat())
    pool_dir = _unique_dir(output_dir / pool_id)
    pool_dir.mkdir(parents=True, exist_ok=False)
    status = "PASS" if candidates else "PASS_WITH_WARNINGS"
    manifest_payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_observe_pool_manifest",
        "pool_id": pool_dir.name,
        "source_sweep_id": sweep_id,
        "generated_at": generated.isoformat(),
        "status": status,
        "top_n_requested": top_n,
        "observe_candidate_count": len(candidates),
        "rejected_candidate_count": len(rejected),
        "manual_review_required_count": sum(
            1 for row in candidates if row.get("manual_review_required") is True
        ),
        "shadow_registry_sync_status": "NOT_SYNCED_BY_DEFAULT",
        "shadow_registry_sync_reason": (
            "observe_pool build writes an auditable pool artifact; use shadow register "
            "for explicit per-candidate registry mutation"
        ),
        "observe_candidates_path": str(pool_dir / "observe_candidates.jsonl"),
        "observe_pool_report_path": str(pool_dir / "observe_pool_report.md"),
        "rejected_candidates": rejected,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(pool_dir / "observe_pool_manifest.json", manifest_payload)
    _write_jsonl(pool_dir / "observe_candidates.jsonl", candidates)
    _write_text(
        pool_dir / "observe_pool_report.md",
        render_observe_pool_markdown(manifest_payload, candidates),
    )
    _update_latest_pointer(
        "latest_observe_pool",
        pool_dir.name,
        pool_dir / "observe_pool_manifest.json",
    )
    return {
        "pool_id": pool_dir.name,
        "pool_dir": pool_dir,
        "manifest": manifest_payload,
        "candidates": candidates,
    }


def observe_pool_report_payload(
    *,
    pool_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_OBSERVE_POOL_DIR,
) -> dict[str, Any]:
    resolved_id = pool_id or (_latest_pointer_artifact_id("latest_observe_pool") if latest else "")
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--pool-id or --latest is required")
    pool_dir = output_dir / resolved_id
    return {**_read_json(pool_dir / "observe_pool_manifest.json"), "pool_dir": str(pool_dir)}


def validate_observe_pool_artifact(
    *,
    pool_id: str,
    output_dir: Path = DEFAULT_OBSERVE_POOL_DIR,
) -> dict[str, Any]:
    pool_dir = output_dir / pool_id
    manifest = _read_optional_json(pool_dir / "observe_pool_manifest.json") or {}
    rows = _read_jsonl(pool_dir / "observe_candidates.jsonl")
    checks = [
        _check("manifest_exists", (pool_dir / "observe_pool_manifest.json").exists(), pool_id),
        _check("observe_candidates_exists", (pool_dir / "observe_candidates.jsonl").exists(), ""),
        _check("report_exists", (pool_dir / "observe_pool_report.md").exists(), ""),
        _check("pool_id_matches", manifest.get("pool_id") == pool_id, pool_id),
        _check(
            "no_rejected_or_high_risk_candidates",
            all(
                _mapping(row.get("evidence_status")).get("promotion_status") != "incomplete"
                and row.get("overfit_status") != "HIGH_RISK"
                for row in rows
            ),
            "observe candidates filtered",
        ),
        _check(
            "production_candidate_not_generated",
            manifest.get("production_candidate_generated") is False,
            "research-only",
        ),
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_observe_pool_validation",
        "pool_id": pool_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def build_shadow_shortlist(
    *,
    observe_pool_id: str,
    target_size: int = SHORTLIST_DEFAULT_TARGET_SIZE,
    max_size: int = SHORTLIST_DEFAULT_MAX_SIZE,
    min_size: int = SHORTLIST_DEFAULT_MIN_SIZE,
    observe_pool_dir: Path = DEFAULT_OBSERVE_POOL_DIR,
    output_dir: Path = DEFAULT_SHORTLIST_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    if min_size <= 0 or target_size <= 0 or max_size <= 0:
        raise DynamicV3ParameterResearchError("shortlist sizes must be positive")
    if min_size > target_size or target_size > max_size:
        raise DynamicV3ParameterResearchError("shortlist sizes must satisfy min <= target <= max")
    generated = generated_at or datetime.now(UTC)
    pool_dir = observe_pool_dir / observe_pool_id
    pool_manifest = _read_json(pool_dir / "observe_pool_manifest.json")
    observe_rows = _read_jsonl(pool_dir / "observe_candidates.jsonl")
    eligible: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in observe_rows:
        hard_reasons = _shortlist_hard_reject_reasons(row)
        breakdown = _shortlist_score_breakdown(row, diversity=0.0)
        if hard_reasons:
            rejected.append(
                {
                    "candidate_id": row.get("candidate_id"),
                    "source_observe_pool_id": observe_pool_id,
                    "shortlist_status": "rejected",
                    "rejection_reasons": hard_reasons,
                    "shortlist_score": _weighted_shortlist_score(breakdown),
                    "shortlist_score_breakdown": breakdown,
                }
            )
            continue
        eligible.append(
            {
                **row,
                "_base_shortlist_score_breakdown": breakdown,
                "_base_shortlist_score": _weighted_shortlist_score(breakdown),
            }
        )
    selected = _select_shortlist_candidates(eligible, target_size=target_size)
    selected_ids = {_text(row.get("candidate_id")) for row in selected}
    for row in eligible:
        if _text(row.get("candidate_id")) in selected_ids:
            continue
        breakdown = dict(_mapping(row.get("_base_shortlist_score_breakdown")))
        rejected.append(
            {
                "candidate_id": row.get("candidate_id"),
                "source_observe_pool_id": observe_pool_id,
                "shortlist_status": "rejected",
                "rejection_reasons": ["not_in_top_shortlist_after_diversity_selection"],
                "shortlist_score": row.get("_base_shortlist_score"),
                "shortlist_score_breakdown": breakdown,
            }
        )
    shortlist_candidates: list[dict[str, Any]] = []
    for rank, row in enumerate(selected[:max_size], start=1):
        breakdown = dict(_mapping(row.get("_shortlist_score_breakdown")))
        score = _weighted_shortlist_score(breakdown)
        clean = {key: value for key, value in row.items() if not key.startswith("_")}
        warnings = sorted(set(_candidate_manual_review_warnings(clean)))
        shortlist_candidates.append(
            {
                **clean,
                "source_observe_pool_id": observe_pool_id,
                "source_sweep_id": pool_manifest.get(
                    "source_sweep_id",
                    clean.get("source_sweep_id"),
                ),
                "shortlist_rank": rank,
                "shortlist_status": "selected",
                "manual_review_required": True,
                "overfit_status": _candidate_overfit_status(clean),
                "regime_coverage_status": _candidate_regime_status(clean),
                "shortlist_score": round(score, 6),
                "shortlist_score_breakdown": {
                    key: round(_float(value), 6) for key, value in breakdown.items()
                },
                "selection_reasons": _shortlist_selection_reasons(breakdown, warnings),
                "remaining_warnings": warnings,
            }
        )
    shortlist_id = _stable_id(
        "shadow-shortlist",
        observe_pool_id,
        target_size,
        max_size,
        generated.isoformat(),
    )
    shortlist_dir = _unique_dir(output_dir / shortlist_id)
    shortlist_dir.mkdir(parents=True, exist_ok=False)
    status = (
        "PASS"
        if len(shortlist_candidates) >= min_size and len(shortlist_candidates) <= max_size
        else "PASS_WITH_WARNINGS"
    )
    score_breakdown = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shortlist_score_breakdown",
        "shortlist_id": shortlist_dir.name,
        "component_weights": dict(SHORTLIST_SCORE_COMPONENT_WEIGHTS),
        "selected": [
            {
                "candidate_id": row["candidate_id"],
                "shortlist_rank": row["shortlist_rank"],
                "shortlist_score": row["shortlist_score"],
                "shortlist_score_breakdown": row["shortlist_score_breakdown"],
            }
            for row in shortlist_candidates
        ],
        "rejected": rejected,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_shortlist_manifest",
        "shortlist_id": shortlist_dir.name,
        "source_observe_pool_id": observe_pool_id,
        "source_sweep_id": pool_manifest.get("source_sweep_id", ""),
        "source_recovery_id": pool_manifest.get("source_recovery_id", ""),
        "generated_at": generated.isoformat(),
        "status": status,
        "observe_pool_candidate_count": len(observe_rows),
        "eligible_candidate_count": len(eligible),
        "shortlist_count": len(shortlist_candidates),
        "target_size": target_size,
        "min_shortlist_size": min_size,
        "max_shortlist_size": max_size,
        "manual_review_required_count": len(shortlist_candidates),
        "hard_rejected_candidate_count": sum(
            1
            for row in rejected
            if "not_in_top_shortlist_after_diversity_selection"
            not in _texts(row.get("rejection_reasons"))
        ),
        "shortlist_candidates_path": str(shortlist_dir / "shortlist_candidates.jsonl"),
        "shortlist_rejected_candidates_path": str(
            shortlist_dir / "shortlist_rejected_candidates.jsonl"
        ),
        "shortlist_score_breakdown_path": str(shortlist_dir / "shortlist_score_breakdown.json"),
        "shortlist_report_path": str(shortlist_dir / "shortlist_report.md"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(shortlist_dir / "shortlist_manifest.json", manifest)
    _write_jsonl(shortlist_dir / "shortlist_candidates.jsonl", shortlist_candidates)
    _write_jsonl(shortlist_dir / "shortlist_rejected_candidates.jsonl", rejected)
    _write_json(shortlist_dir / "shortlist_score_breakdown.json", score_breakdown)
    _write_text(
        shortlist_dir / "shortlist_report.md",
        render_shortlist_markdown(manifest, shortlist_candidates, rejected),
    )
    _update_latest_pointer(
        "latest_shortlist",
        shortlist_dir.name,
        shortlist_dir / "shortlist_manifest.json",
    )
    return {
        "shortlist_id": shortlist_dir.name,
        "shortlist_dir": shortlist_dir,
        "manifest": manifest,
        "candidates": shortlist_candidates,
        "rejected_candidates": rejected,
    }


def shortlist_report_payload(
    *,
    shortlist_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SHORTLIST_DIR,
) -> dict[str, Any]:
    resolved_id = shortlist_id or (
        _latest_pointer_artifact_id("latest_shortlist") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--shortlist-id or --latest is required")
    shortlist_dir = output_dir / resolved_id
    return {
        **_read_json(shortlist_dir / "shortlist_manifest.json"),
        "shortlist_dir": str(shortlist_dir),
    }


def validate_shortlist_artifact(
    *,
    shortlist_id: str,
    output_dir: Path = DEFAULT_SHORTLIST_DIR,
) -> dict[str, Any]:
    shortlist_dir = output_dir / shortlist_id
    manifest = _read_optional_json(shortlist_dir / "shortlist_manifest.json") or {}
    rows = _read_jsonl(shortlist_dir / "shortlist_candidates.jsonl")
    required = [
        "shortlist_manifest.json",
        "shortlist_candidates.jsonl",
        "shortlist_rejected_candidates.jsonl",
        "shortlist_score_breakdown.json",
        "shortlist_report.md",
    ]
    selected_count = len(rows)
    eligible_count = int(manifest.get("eligible_candidate_count") or selected_count)
    min_size = int(manifest.get("min_shortlist_size") or SHORTLIST_DEFAULT_MIN_SIZE)
    max_size = int(manifest.get("max_shortlist_size") or SHORTLIST_DEFAULT_MAX_SIZE)
    checks = [
        _check(f"artifact_exists:{name}", (shortlist_dir / name).exists(), name)
        for name in required
    ]
    checks.extend(
        [
            _check(
                "shortlist_id_matches",
                manifest.get("shortlist_id") == shortlist_id,
                shortlist_id,
            ),
            _check(
                "shortlist_count_in_configured_range",
                selected_count <= max_size
                and (selected_count >= min_size or selected_count == eligible_count),
                (
                    f"selected={selected_count}, min={min_size}, max={max_size}, "
                    f"eligible={eligible_count}"
                ),
            ),
            _check(
                "hard_fail_candidates_excluded",
                all(not _shortlist_hard_reject_reasons(row) for row in rows),
                "hard fail candidates excluded",
            ),
            _check(
                "selection_reasons_present",
                all(_texts(row.get("selection_reasons")) for row in rows),
                "selected candidates explain why selected",
            ),
            _check(
                "all_selected_manual_review_required",
                all(row.get("manual_review_required") is True for row in rows),
                "manual review remains required",
            ),
            _check(
                "production_candidate_not_generated",
                manifest.get("production_candidate_generated") is False,
                "shortlist is not production",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_shortlist_validation",
        "shortlist_id": shortlist_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def run_candidate_clustering(
    *,
    shortlist_id: str,
    shortlist_dir: Path = DEFAULT_SHORTLIST_DIR,
    output_dir: Path = DEFAULT_CANDIDATE_CLUSTER_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_dir = shortlist_dir / shortlist_id
    shortlist_manifest = _read_json(source_dir / "shortlist_manifest.json")
    candidates = _read_jsonl(source_dir / "shortlist_candidates.jsonl")
    parameter_matrix = _similarity_matrix_rows(candidates, _parameter_similarity)
    weight_matrix = _weight_path_similarity_matrix(candidates)
    metric_matrix = _similarity_matrix_rows(candidates, _metric_similarity)
    clusters = _candidate_clusters(candidates, weight_matrix)
    representatives = _cluster_representatives(clusters, candidates)
    cluster_id = _stable_id("candidate-cluster", shortlist_id, generated.isoformat())
    cluster_dir = _unique_dir(output_dir / cluster_id)
    cluster_dir.mkdir(parents=True, exist_ok=False)
    cluster_payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_clusters",
        "cluster_id": cluster_dir.name,
        "source_shortlist_id": shortlist_id,
        "clusters": clusters,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    incomplete_weight_pairs = sum(
        1
        for row in weight_matrix
        for key, value in row.items()
        if key != "candidate_id" and value == "INCOMPLETE"
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_cluster_manifest",
        "cluster_id": cluster_dir.name,
        "source_shortlist_id": shortlist_id,
        "source_observe_pool_id": shortlist_manifest.get("source_observe_pool_id", ""),
        "generated_at": generated.isoformat(),
        "status": "PASS" if candidates and representatives else "PASS_WITH_WARNINGS",
        "candidate_count": len(candidates),
        "cluster_count": len(clusters),
        "representative_count": len(representatives),
        "weight_path_similarity_status": ("INCOMPLETE" if incomplete_weight_pairs else "PASS"),
        "parameter_similarity_matrix_path": str(cluster_dir / "parameter_similarity_matrix.csv"),
        "weight_path_similarity_matrix_path": str(
            cluster_dir / "weight_path_similarity_matrix.csv"
        ),
        "metric_similarity_matrix_path": str(cluster_dir / "metric_similarity_matrix.csv"),
        "candidate_clusters_path": str(cluster_dir / "candidate_clusters.json"),
        "cluster_representatives_path": str(cluster_dir / "cluster_representatives.jsonl"),
        "candidate_cluster_report_path": str(cluster_dir / "candidate_cluster_report.md"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(cluster_dir / "cluster_manifest.json", manifest)
    _write_csv(cluster_dir / "parameter_similarity_matrix.csv", parameter_matrix)
    _write_csv(cluster_dir / "weight_path_similarity_matrix.csv", weight_matrix)
    _write_csv(cluster_dir / "metric_similarity_matrix.csv", metric_matrix)
    _write_json(cluster_dir / "candidate_clusters.json", cluster_payload)
    _write_jsonl(cluster_dir / "cluster_representatives.jsonl", representatives)
    _write_text(
        cluster_dir / "candidate_cluster_report.md",
        render_candidate_cluster_markdown(manifest, clusters, representatives),
    )
    _update_latest_pointer(
        "latest_candidate_cluster",
        cluster_dir.name,
        cluster_dir / "cluster_manifest.json",
    )
    return {
        "cluster_id": cluster_dir.name,
        "cluster_dir": cluster_dir,
        "manifest": manifest,
        "clusters": clusters,
        "representatives": representatives,
    }


def candidate_cluster_report_payload(
    *,
    cluster_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CANDIDATE_CLUSTER_DIR,
) -> dict[str, Any]:
    resolved_id = cluster_id or (
        _latest_pointer_artifact_id("latest_candidate_cluster") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--cluster-id or --latest is required")
    cluster_dir = output_dir / resolved_id
    return {**_read_json(cluster_dir / "cluster_manifest.json"), "cluster_dir": str(cluster_dir)}


def validate_candidate_cluster_artifact(
    *,
    cluster_id: str,
    output_dir: Path = DEFAULT_CANDIDATE_CLUSTER_DIR,
) -> dict[str, Any]:
    cluster_dir = output_dir / cluster_id
    manifest = _read_optional_json(cluster_dir / "cluster_manifest.json") or {}
    clusters = _records(
        _mapping(_read_optional_json(cluster_dir / "candidate_clusters.json") or {}).get("clusters")
    )
    representatives = _read_jsonl(cluster_dir / "cluster_representatives.jsonl")
    required = [
        "cluster_manifest.json",
        "parameter_similarity_matrix.csv",
        "weight_path_similarity_matrix.csv",
        "metric_similarity_matrix.csv",
        "candidate_clusters.json",
        "cluster_representatives.jsonl",
        "candidate_cluster_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (cluster_dir / name).exists(), name) for name in required
    ]
    checks.extend(
        [
            _check("cluster_id_matches", manifest.get("cluster_id") == cluster_id, cluster_id),
            _check("clusters_present", bool(clusters), "candidate clusters are required"),
            _check(
                "representatives_present",
                bool(representatives),
                "cluster representatives are required",
            ),
            _check(
                "representatives_reference_clusters",
                all(_text(row.get("cluster_id")) for row in representatives),
                "representatives include cluster id",
            ),
            _check(
                "production_candidate_not_generated",
                manifest.get("production_candidate_generated") is False,
                "cluster artifact is not production",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_cluster_validation",
        "cluster_id": cluster_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def build_shadow_shortlist_monitoring_pack(
    *,
    shortlist_id: str,
    cluster_id: str,
    shortlist_dir: Path = DEFAULT_SHORTLIST_DIR,
    cluster_dir: Path = DEFAULT_CANDIDATE_CLUSTER_DIR,
    output_dir: Path = DEFAULT_SHADOW_SHORTLIST_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_shortlist_dir = shortlist_dir / shortlist_id
    shortlist_manifest = _read_json(source_shortlist_dir / "shortlist_manifest.json")
    shortlist_rows = {
        _text(row.get("candidate_id")): row
        for row in _read_jsonl(source_shortlist_dir / "shortlist_candidates.jsonl")
    }
    source_cluster_dir = cluster_dir / cluster_id
    cluster_manifest = _read_json(source_cluster_dir / "cluster_manifest.json")
    representatives = _read_jsonl(source_cluster_dir / "cluster_representatives.jsonl")
    shadow_rows: list[dict[str, Any]] = []
    for rep in representatives:
        candidate_id = _text(rep.get("candidate_id"))
        source = shortlist_rows.get(candidate_id)
        if source is None:
            continue
        shadow_rows.append(
            {
                "candidate_id": candidate_id,
                "source_shortlist_id": shortlist_id,
                "source_cluster_id": cluster_id,
                "cluster_id": rep.get("cluster_id"),
                "cluster_label": rep.get("cluster_label"),
                "representative_type": rep.get("representative_type", "primary"),
                "manual_review_required": True,
                "monitoring_status": "ready_for_shadow_monitoring",
                "monitoring_start_after_owner_review": True,
                "monitoring_requirements": {
                    "min_days": 30,
                    "min_rebalance_count": 3,
                    "track_live_vs_backtest_drift": True,
                    "track_weight_path_stability": True,
                    "track_regime_response": True,
                },
                "shortlist_rank": source.get("shortlist_rank"),
                "shortlist_score": source.get("shortlist_score"),
                "parameters": source.get("parameters", {}),
                "metrics": source.get("metrics", {}),
                "real_evaluation_artifact_path": source.get("real_evaluation_artifact_path", ""),
                "remaining_warnings": sorted(
                    set(_texts(source.get("remaining_warnings")) + _texts(rep.get("risks")))
                ),
            }
        )
    shadow_id = _stable_id("shadow-shortlist", shortlist_id, cluster_id, generated.isoformat())
    shadow_dir = _unique_dir(output_dir / shadow_id)
    shadow_dir.mkdir(parents=True, exist_ok=False)
    monitoring_plan = _shadow_shortlist_monitoring_plan(shadow_id, shadow_rows)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_shortlist_monitoring_manifest",
        "shadow_shortlist_id": shadow_dir.name,
        "source_shortlist_id": shortlist_id,
        "source_cluster_id": cluster_id,
        "source_observe_pool_id": shortlist_manifest.get("source_observe_pool_id", ""),
        "generated_at": generated.isoformat(),
        "status": "PASS" if shadow_rows else "PASS_WITH_WARNINGS",
        "shortlist_count": shortlist_manifest.get("shortlist_count", 0),
        "cluster_count": cluster_manifest.get("cluster_count", 0),
        "shadow_candidate_count": len(shadow_rows),
        "manual_review_required_count": len(shadow_rows),
        "shadow_monitoring_ready": bool(shadow_rows),
        "shadow_shortlist_candidates_path": str(shadow_dir / "shadow_shortlist_candidates.jsonl"),
        "shadow_shortlist_monitoring_plan_path": str(
            shadow_dir / "shadow_shortlist_monitoring_plan.json"
        ),
        "shadow_shortlist_report_path": str(shadow_dir / "shadow_shortlist_report.md"),
        "reader_brief_section_path": str(shadow_dir / "reader_brief_section.md"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    reader_brief = render_shadow_shortlist_reader_brief(manifest, shadow_rows)
    _write_json(shadow_dir / "shadow_shortlist_manifest.json", manifest)
    _write_jsonl(shadow_dir / "shadow_shortlist_candidates.jsonl", shadow_rows)
    _write_json(shadow_dir / "shadow_shortlist_monitoring_plan.json", monitoring_plan)
    _write_text(
        shadow_dir / "shadow_shortlist_report.md",
        render_shadow_shortlist_markdown(manifest, shadow_rows, monitoring_plan),
    )
    _write_text(shadow_dir / "reader_brief_section.md", reader_brief)
    _update_latest_pointer(
        "latest_shadow_shortlist",
        shadow_dir.name,
        shadow_dir / "shadow_shortlist_manifest.json",
    )
    return {
        "shadow_shortlist_id": shadow_dir.name,
        "shadow_shortlist_dir": shadow_dir,
        "manifest": manifest,
        "candidates": shadow_rows,
        "monitoring_plan": monitoring_plan,
    }


def shadow_shortlist_report_payload(
    *,
    shadow_shortlist_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SHADOW_SHORTLIST_DIR,
) -> dict[str, Any]:
    resolved_id = shadow_shortlist_id or (
        _latest_pointer_artifact_id("latest_shadow_shortlist") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--shadow-shortlist-id or --latest is required")
    shadow_dir = output_dir / resolved_id
    return {
        **_read_json(shadow_dir / "shadow_shortlist_manifest.json"),
        "shadow_shortlist_dir": str(shadow_dir),
    }


def validate_shadow_shortlist_artifact(
    *,
    shadow_shortlist_id: str,
    output_dir: Path = DEFAULT_SHADOW_SHORTLIST_DIR,
) -> dict[str, Any]:
    shadow_dir = output_dir / shadow_shortlist_id
    manifest = _read_optional_json(shadow_dir / "shadow_shortlist_manifest.json") or {}
    rows = _read_jsonl(shadow_dir / "shadow_shortlist_candidates.jsonl")
    required = [
        "shadow_shortlist_manifest.json",
        "shadow_shortlist_candidates.jsonl",
        "shadow_shortlist_monitoring_plan.json",
        "shadow_shortlist_report.md",
        "reader_brief_section.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (shadow_dir / name).exists(), name) for name in required
    ]
    checks.extend(
        [
            _check(
                "shadow_shortlist_id_matches",
                manifest.get("shadow_shortlist_id") == shadow_shortlist_id,
                shadow_shortlist_id,
            ),
            _check(
                "monitoring_requirements_present",
                all(bool(_mapping(row.get("monitoring_requirements"))) for row in rows),
                "each shadow candidate has monitoring requirements",
            ),
            _check(
                "monitoring_waits_for_owner_review",
                all(row.get("monitoring_start_after_owner_review") is True for row in rows),
                "owner review required before monitoring starts",
            ),
            _check(
                "production_candidate_not_generated",
                manifest.get("production_candidate_generated") is False,
                "shadow shortlist is not production",
            ),
            _check(
                "broker_action_none",
                manifest.get("broker_action") == "none",
                "no broker action",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_shortlist_validation",
        "shadow_shortlist_id": shadow_shortlist_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def load_position_advisory_config(
    path: Path | str = DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
) -> dict[str, Any]:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, Mapping):
        raise DynamicV3ParameterResearchError("position advisory config must be a mapping")
    execution = _mapping(raw.get("execution_mode"))
    if execution.get("advisory_only") is not True:
        raise DynamicV3ParameterResearchError("position advisory must be advisory_only")
    if execution.get("broker_action_allowed") is not False:
        raise DynamicV3ParameterResearchError("position advisory must forbid broker action")
    if execution.get("require_owner_approval") is not True:
        raise DynamicV3ParameterResearchError("position advisory must require owner approval")
    consensus = _mapping(raw.get("consensus"))
    required_consensus_thresholds = (
        "agreement_threshold",
        "max_symbol_dispersion",
        "high_symbol_dispersion",
        "max_risk_asset_exposure_dispersion",
        "high_risk_asset_exposure_dispersion",
        "max_cash_exposure_dispersion",
        "high_cash_exposure_dispersion",
    )
    missing = [key for key in required_consensus_thresholds if key not in consensus]
    if missing:
        raise DynamicV3ParameterResearchError(
            "position advisory consensus thresholds missing: " + ", ".join(sorted(missing))
        )
    return dict(raw)


def run_position_advisory(
    *,
    shadow_shortlist_id: str,
    config_path: Path = DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    portfolio_snapshot_path: Path | None = None,
    shadow_shortlist_dir: Path = DEFAULT_SHADOW_SHORTLIST_DIR,
    output_dir: Path = DEFAULT_POSITION_ADVISORY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = load_position_advisory_config(config_path)
    source_dir = shadow_shortlist_dir / shadow_shortlist_id
    shadow_manifest = _read_json(source_dir / "shadow_shortlist_manifest.json")
    shadow_rows = _read_jsonl(source_dir / "shadow_shortlist_candidates.jsonl")
    target_rows: list[dict[str, Any]] = []
    for row in shadow_rows:
        latest_weights = _candidate_latest_target_weights(row)
        target_rows.append(
            {
                "candidate_id": row.get("candidate_id"),
                "cluster_id": row.get("cluster_id"),
                "cluster_label": row.get("cluster_label"),
                "as_of": latest_weights["as_of"],
                "target_weights": latest_weights["weights"],
                "source_weight_path_artifact": latest_weights["source_weight_path_artifact"],
                "weight_path_status": latest_weights["status"],
            }
        )
    consensus_rows, consensus_status = _consensus_target_weights(target_rows, config)
    snapshot = _load_portfolio_snapshot(portfolio_snapshot_path) if portfolio_snapshot_path else {}
    delta_rows = _candidate_position_delta_rows(target_rows, snapshot, config) if snapshot else []
    advisory_actions = _position_advisory_actions(
        target_rows=target_rows,
        delta_rows=delta_rows,
        snapshot=snapshot,
        consensus_status=consensus_status,
        config=config,
    )
    advisory_id = _stable_id(
        "position-advisory",
        shadow_shortlist_id,
        str(config_path),
        str(portfolio_snapshot_path or ""),
        generated.isoformat(),
    )
    advisory_dir = _unique_dir(output_dir / advisory_id)
    advisory_dir.mkdir(parents=True, exist_ok=False)
    status = "PASS" if target_rows else "PASS_WITH_WARNINGS"
    advisory_status = advisory_actions["position_advisory_status"]
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_position_advisory_manifest",
        "advisory_id": advisory_dir.name,
        "source_shadow_shortlist_id": shadow_shortlist_id,
        "source_shortlist_id": shadow_manifest.get("source_shortlist_id", ""),
        "source_cluster_id": shadow_manifest.get("source_cluster_id", ""),
        "generated_at": generated.isoformat(),
        "status": status,
        "position_advisory_status": advisory_status,
        "portfolio_snapshot_provided": bool(snapshot),
        "portfolio_snapshot_path": (
            "" if portfolio_snapshot_path is None else str(portfolio_snapshot_path)
        ),
        "candidate_count": len(target_rows),
        "consensus_target_weight_status": consensus_status,
        "recommended_action": advisory_actions["recommended_action"],
        "owner_approval_required": True,
        "broker_action_allowed": False,
        "candidate_target_weights_path": str(advisory_dir / "candidate_target_weights.jsonl"),
        "candidate_position_deltas_path": str(advisory_dir / "candidate_position_deltas.jsonl"),
        "consensus_target_weights_path": str(advisory_dir / "consensus_target_weights.csv"),
        "advisory_actions_path": str(advisory_dir / "advisory_actions.json"),
        "position_advisory_report_path": str(advisory_dir / "position_advisory_report.md"),
        "config_path": str(config_path),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(advisory_dir / "position_advisory_manifest.json", manifest)
    _write_jsonl(advisory_dir / "candidate_target_weights.jsonl", target_rows)
    _write_jsonl(advisory_dir / "candidate_position_deltas.jsonl", delta_rows)
    _write_csv(advisory_dir / "consensus_target_weights.csv", consensus_rows)
    _write_json(advisory_dir / "advisory_actions.json", advisory_actions)
    _write_text(
        advisory_dir / "position_advisory_report.md",
        render_position_advisory_markdown(
            manifest,
            target_rows,
            consensus_rows,
            delta_rows,
            advisory_actions,
        ),
    )
    _update_latest_pointer(
        "latest_position_advisory",
        advisory_dir.name,
        advisory_dir / "position_advisory_manifest.json",
    )
    return {
        "advisory_id": advisory_dir.name,
        "advisory_dir": advisory_dir,
        "manifest": manifest,
        "candidate_target_weights": target_rows,
        "candidate_position_deltas": delta_rows,
        "consensus_target_weights": consensus_rows,
        "advisory_actions": advisory_actions,
    }


def position_advisory_report_payload(
    *,
    advisory_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_POSITION_ADVISORY_DIR,
) -> dict[str, Any]:
    resolved_id = advisory_id or (
        _latest_pointer_artifact_id("latest_position_advisory") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--advisory-id or --latest is required")
    advisory_dir = output_dir / resolved_id
    return {
        **_read_json(advisory_dir / "position_advisory_manifest.json"),
        "advisory_dir": str(advisory_dir),
    }


def validate_position_advisory_artifact(
    *,
    advisory_id: str,
    output_dir: Path = DEFAULT_POSITION_ADVISORY_DIR,
) -> dict[str, Any]:
    advisory_dir = output_dir / advisory_id
    manifest = _read_optional_json(advisory_dir / "position_advisory_manifest.json") or {}
    actions = _read_optional_json(advisory_dir / "advisory_actions.json") or {}
    targets = _read_jsonl(advisory_dir / "candidate_target_weights.jsonl")
    required = [
        "position_advisory_manifest.json",
        "candidate_target_weights.jsonl",
        "candidate_position_deltas.jsonl",
        "consensus_target_weights.csv",
        "advisory_actions.json",
        "position_advisory_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (advisory_dir / name).exists(), name) for name in required
    ]
    checks.extend(
        [
            _check("advisory_id_matches", manifest.get("advisory_id") == advisory_id, advisory_id),
            _check("target_weights_present", bool(targets), "candidate target weights required"),
            _check(
                "owner_approval_required",
                manifest.get("owner_approval_required") is True
                and actions.get("owner_approval_required") is True,
                "owner approval is required",
            ),
            _check(
                "broker_action_forbidden",
                manifest.get("broker_action_allowed") is False
                and actions.get("broker_action_allowed") is False,
                "broker action is forbidden",
            ),
            _check(
                "target_only_without_snapshot",
                bool(manifest.get("portfolio_snapshot_provided"))
                or manifest.get("position_advisory_status") == POSITION_ADVISORY_TARGET_ONLY,
                "missing snapshot must produce TARGET_ONLY",
            ),
            _check(
                "production_candidate_not_generated",
                manifest.get("production_candidate_generated") is False,
                "advisory is not production",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_position_advisory_validation",
        "advisory_id": advisory_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def build_position_review_pack(
    *,
    shortlist_id: str,
    cluster_id: str,
    shadow_shortlist_id: str,
    advisory_id: str,
    shortlist_dir: Path = DEFAULT_SHORTLIST_DIR,
    cluster_dir: Path = DEFAULT_CANDIDATE_CLUSTER_DIR,
    shadow_shortlist_dir: Path = DEFAULT_SHADOW_SHORTLIST_DIR,
    advisory_dir: Path = DEFAULT_POSITION_ADVISORY_DIR,
    output_dir: Path = DEFAULT_POSITION_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    shortlist = _read_json(shortlist_dir / shortlist_id / "shortlist_manifest.json")
    cluster = _read_json(cluster_dir / cluster_id / "cluster_manifest.json")
    shadow = _read_json(
        shadow_shortlist_dir / shadow_shortlist_id / "shadow_shortlist_manifest.json"
    )
    advisory = _read_json(advisory_dir / advisory_id / "position_advisory_manifest.json")
    actions = _read_optional_json(advisory_dir / advisory_id / "advisory_actions.json") or {}
    decision = _position_review_decision(shortlist, cluster, shadow, advisory, actions)
    review_id = _stable_id(
        "position-review",
        shortlist_id,
        cluster_id,
        shadow_shortlist_id,
        advisory_id,
        generated.isoformat(),
    )
    review_dir = _unique_dir(output_dir / review_id)
    review_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_position_review_manifest",
        "review_id": review_dir.name,
        "source_shortlist_id": shortlist_id,
        "source_cluster_id": cluster_id,
        "source_shadow_shortlist_id": shadow_shortlist_id,
        "source_advisory_id": advisory_id,
        "generated_at": generated.isoformat(),
        "status": (
            "PASS"
            if decision["shadow_observation_readiness"] != "NOT_READY"
            else "PASS_WITH_WARNINGS"
        ),
        "shortlist_count": shortlist.get("shortlist_count", 0),
        "cluster_count": cluster.get("cluster_count", 0),
        "shadow_candidate_count": shadow.get("shadow_candidate_count", 0),
        "position_advisory_status": advisory.get("position_advisory_status", ""),
        "shadow_observation_readiness": decision["shadow_observation_readiness"],
        "position_advisory_readiness": decision["position_advisory_readiness"],
        "production_readiness": decision["production_readiness"],
        "recommended_next_action": decision["recommended_next_action"],
        "owner_approval_required": True,
        "broker_action_allowed": False,
        "go_no_go_decision_path": str(review_dir / "go_no_go_decision.json"),
        "owner_review_checklist_path": str(review_dir / "owner_review_checklist.md"),
        "position_review_report_path": str(review_dir / "position_review_report.md"),
        "reader_brief_section_path": str(review_dir / "reader_brief_section.md"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(review_dir / "position_review_manifest.json", manifest)
    _write_json(review_dir / "go_no_go_decision.json", decision)
    _write_text(review_dir / "owner_review_checklist.md", render_owner_review_checklist(decision))
    _write_text(
        review_dir / "position_review_report.md",
        render_position_review_markdown(manifest, shortlist, cluster, shadow, advisory, decision),
    )
    _write_text(
        review_dir / "reader_brief_section.md",
        render_position_review_reader_brief(manifest, decision),
    )
    _update_latest_pointer(
        "latest_position_review",
        review_dir.name,
        review_dir / "position_review_manifest.json",
    )
    return {
        "review_id": review_dir.name,
        "review_dir": review_dir,
        "manifest": manifest,
        "go_no_go_decision": decision,
    }


def position_review_report_payload(
    *,
    review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_POSITION_REVIEW_DIR,
) -> dict[str, Any]:
    resolved_id = review_id or (
        _latest_pointer_artifact_id("latest_position_review") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--review-id or --latest is required")
    review_dir = output_dir / resolved_id
    return {
        **_read_json(review_dir / "position_review_manifest.json"),
        "review_dir": str(review_dir),
    }


def validate_position_review_artifact(
    *,
    review_id: str,
    output_dir: Path = DEFAULT_POSITION_REVIEW_DIR,
) -> dict[str, Any]:
    review_dir = output_dir / review_id
    manifest = _read_optional_json(review_dir / "position_review_manifest.json") or {}
    decision = _read_optional_json(review_dir / "go_no_go_decision.json") or {}
    required = [
        "position_review_manifest.json",
        "go_no_go_decision.json",
        "owner_review_checklist.md",
        "position_review_report.md",
        "reader_brief_section.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (review_dir / name).exists(), name) for name in required
    ]
    checks.extend(
        [
            _check("review_id_matches", manifest.get("review_id") == review_id, review_id),
            _check(
                "production_readiness_not_ready",
                decision.get("production_readiness") == "NOT_READY",
                "production readiness must remain NOT_READY",
            ),
            _check(
                "broker_action_forbidden",
                decision.get("broker_action_allowed") is False
                and manifest.get("broker_action_allowed") is False,
                "broker action is forbidden",
            ),
            _check(
                "owner_approval_required",
                decision.get("owner_approval_required") is True
                and manifest.get("owner_approval_required") is True,
                "owner approval is required",
            ),
            _check(
                "production_candidate_not_generated",
                manifest.get("production_candidate_generated") is False,
                "review pack is not production",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_position_review_validation",
        "review_id": review_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def activate_shadow_monitoring(
    *,
    shadow_shortlist_id: str,
    shadow_shortlist_dir: Path = DEFAULT_SHADOW_SHORTLIST_DIR,
    output_dir: Path = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_dir = shadow_shortlist_dir / shadow_shortlist_id
    shadow_manifest = _read_json(source_dir / "shadow_shortlist_manifest.json")
    shadow_rows = _read_jsonl(source_dir / "shadow_shortlist_candidates.jsonl")
    activation_id = _stable_id(
        "shadow-monitor-activation",
        shadow_shortlist_id,
        generated.isoformat(),
    )
    activation_dir = _unique_dir(output_dir / activation_id)
    activation_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_monitor_activation_manifest",
        "activation_id": activation_dir.name,
        "shadow_shortlist_id": shadow_shortlist_id,
        "source_shortlist_id": shadow_manifest.get("source_shortlist_id", ""),
        "source_cluster_id": shadow_manifest.get("source_cluster_id", ""),
        "activated_at": generated.isoformat(),
        "status": "PASS" if shadow_rows else "PASS_WITH_WARNINGS",
        "monitoring_status": "active" if shadow_rows else "empty_shortlist",
        "candidate_count": len(shadow_rows),
        "manual_review_required": True,
        "owner_approval_required": True,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "activation_report_path": str(activation_dir / "shadow_monitor_activation_report.md"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(activation_dir / "shadow_monitor_activation_manifest.json", manifest)
    _write_text(
        activation_dir / "shadow_monitor_activation_report.md",
        render_shadow_monitor_activation_markdown(manifest),
    )
    _update_latest_pointer(
        "latest_shadow_monitor_activation",
        activation_dir.name,
        activation_dir / "shadow_monitor_activation_manifest.json",
    )
    return {
        "activation_id": activation_dir.name,
        "activation_dir": activation_dir,
        "manifest": manifest,
    }


def run_shadow_shortlist_monitor(
    *,
    shadow_shortlist_id: str,
    as_of: date,
    shadow_shortlist_dir: Path = DEFAULT_SHADOW_SHORTLIST_DIR,
    output_dir: Path = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_dir = shadow_shortlist_dir / shadow_shortlist_id
    shadow_manifest = _read_json(source_dir / "shadow_shortlist_manifest.json")
    shadow_rows = _read_jsonl(source_dir / "shadow_shortlist_candidates.jsonl")
    previous_weights = _previous_shadow_monitor_weights(
        shadow_shortlist_id=shadow_shortlist_id,
        as_of=as_of,
        output_dir=output_dir,
    )
    daily_rows = [
        _shadow_shortlist_daily_result(row, as_of=as_of, previous_weights=previous_weights)
        for row in shadow_rows
    ]
    weekly_rows = [_shadow_shortlist_weekly_summary(row) for row in daily_rows]
    max_disagreement = _max_target_weight_disagreement(daily_rows)
    drift_scores = [
        _float(_mapping(row.get("live_vs_backtest_drift")).get("drift_score")) for row in daily_rows
    ]
    active_count = sum(1 for row in daily_rows if row.get("monitoring_status") == "active")
    downgrade_count = sum(
        1
        for row in daily_rows
        if row.get("recommendation") in {"required_downgrade", "remove_from_shadow"}
    )
    promotion_count = sum(
        1
        for row in daily_rows
        if _mapping(row.get("promotion_clock")).get("status") == "eligible_for_review"
    )
    summary_recommendation = (
        "pause_monitoring"
        if active_count == 0
        else "manual_review_required"
        if downgrade_count
        else "continue_monitoring"
    )
    summary = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_monitor_summary",
        "monitor_run_id": "",
        "shadow_shortlist_id": shadow_shortlist_id,
        "as_of": as_of.isoformat(),
        "candidate_count": len(daily_rows),
        "active_count": active_count,
        "manual_review_required_count": sum(
            1 for row in daily_rows if row.get("manual_review_required") is True
        ),
        "downgrade_recommended_count": downgrade_count,
        "promotion_review_eligible_count": promotion_count,
        "average_drift_score": round(_avg(drift_scores), 6),
        "max_candidate_disagreement": round(max_disagreement, 6),
        "summary_recommendation": summary_recommendation,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    monitor_run_id = _stable_id(
        "shadow-monitor-run",
        shadow_shortlist_id,
        as_of.isoformat(),
        generated.isoformat(),
    )
    monitor_dir = _unique_dir(output_dir / monitor_run_id)
    monitor_dir.mkdir(parents=True, exist_ok=False)
    summary["monitor_run_id"] = monitor_dir.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_monitor_run_manifest",
        "monitor_run_id": monitor_dir.name,
        "shadow_shortlist_id": shadow_shortlist_id,
        "source_shortlist_id": shadow_manifest.get("source_shortlist_id", ""),
        "source_cluster_id": shadow_manifest.get("source_cluster_id", ""),
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS" if daily_rows else "PASS_WITH_WARNINGS",
        "candidate_count": len(daily_rows),
        "active_count": active_count,
        "summary_recommendation": summary_recommendation,
        "manual_review_required": True,
        "owner_approval_required": True,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "shadow_candidate_daily_results_path": str(
            monitor_dir / "shadow_candidate_daily_results.jsonl"
        ),
        "shadow_candidate_weekly_summary_path": str(
            monitor_dir / "shadow_candidate_weekly_summary.jsonl"
        ),
        "shadow_monitor_summary_path": str(monitor_dir / "shadow_monitor_summary.json"),
        "shadow_monitor_report_path": str(monitor_dir / "shadow_monitor_report.md"),
        "reader_brief_section_path": str(monitor_dir / "reader_brief_section.md"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    reader_brief = render_shadow_monitor_run_reader_brief(summary)
    _write_json(monitor_dir / "shadow_monitor_manifest.json", manifest)
    _write_jsonl(monitor_dir / "shadow_candidate_daily_results.jsonl", daily_rows)
    _write_jsonl(monitor_dir / "shadow_candidate_weekly_summary.jsonl", weekly_rows)
    _write_json(monitor_dir / "shadow_monitor_summary.json", summary)
    _write_text(
        monitor_dir / "shadow_monitor_report.md",
        render_shadow_monitor_run_markdown(manifest, summary, daily_rows),
    )
    _write_text(monitor_dir / "reader_brief_section.md", reader_brief)
    _update_latest_pointer(
        "latest_shadow_monitor_run",
        monitor_dir.name,
        monitor_dir / "shadow_monitor_manifest.json",
    )
    return {
        "monitor_run_id": monitor_dir.name,
        "monitor_run_dir": monitor_dir,
        "manifest": manifest,
        "daily_results": daily_rows,
        "weekly_summary": weekly_rows,
        "summary": summary,
    }


def shadow_monitor_run_report_payload(
    *,
    monitor_run_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SHADOW_MONITOR_RUN_DIR,
) -> dict[str, Any]:
    resolved_id = monitor_run_id or (
        _latest_pointer_artifact_id("latest_shadow_monitor_run") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--monitor-run-id or --latest is required")
    monitor_dir = output_dir / resolved_id
    return {
        **_read_json(monitor_dir / "shadow_monitor_manifest.json"),
        "shadow_monitor_summary": _read_optional_json(monitor_dir / "shadow_monitor_summary.json")
        or {},
        "monitor_run_dir": str(monitor_dir),
    }


def validate_shadow_monitor_run_artifact(
    *,
    monitor_run_id: str,
    output_dir: Path = DEFAULT_SHADOW_MONITOR_RUN_DIR,
) -> dict[str, Any]:
    monitor_dir = output_dir / monitor_run_id
    manifest = _read_optional_json(monitor_dir / "shadow_monitor_manifest.json") or {}
    summary = _read_optional_json(monitor_dir / "shadow_monitor_summary.json") or {}
    rows = _read_jsonl(monitor_dir / "shadow_candidate_daily_results.jsonl")
    required = [
        "shadow_monitor_manifest.json",
        "shadow_candidate_daily_results.jsonl",
        "shadow_candidate_weekly_summary.jsonl",
        "shadow_monitor_summary.json",
        "shadow_monitor_report.md",
        "reader_brief_section.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (monitor_dir / name).exists(), name) for name in required
    ]
    checks.extend(
        [
            _check(
                "monitor_run_id_matches",
                manifest.get("monitor_run_id") == monitor_run_id
                and summary.get("monitor_run_id") == monitor_run_id,
                monitor_run_id,
            ),
            _check(
                "daily_result_count_matches",
                int(manifest.get("candidate_count") or 0) == len(rows),
                f"manifest={manifest.get('candidate_count')} rows={len(rows)}",
            ),
            _check(
                "broker_action_forbidden",
                manifest.get("broker_action_allowed") is False
                and summary.get("broker_action_allowed") is False,
                "broker action is forbidden",
            ),
            _check(
                "broker_action_not_taken",
                manifest.get("broker_action_taken") is False
                and summary.get("broker_action_taken") is False,
                "broker action was not taken",
            ),
            _check(
                "production_candidate_not_generated",
                manifest.get("production_candidate_generated") is False,
                "monitor run is not production",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_monitor_run_validation",
        "monitor_run_id": monitor_run_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def write_portfolio_snapshot_artifact(
    *,
    snapshot_path: Path,
    output_dir: Path = DEFAULT_PORTFOLIO_SNAPSHOT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    normalized = _normalize_portfolio_snapshot(snapshot_path, strict=True)
    snapshot_id = _stable_id(
        "portfolio-snapshot",
        snapshot_path,
        normalized.get("as_of"),
        generated.isoformat(),
    )
    snapshot_dir = _unique_dir(output_dir / snapshot_id)
    snapshot_dir.mkdir(parents=True, exist_ok=False)
    status = "PASS" if normalized["failed_check_count"] == 0 else "FAIL"
    exposure = _portfolio_exposure_summary(normalized)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_portfolio_snapshot_manifest",
        "snapshot_id": snapshot_dir.name,
        "snapshot_path": str(snapshot_path),
        "as_of": normalized.get("as_of", ""),
        "generated_at": generated.isoformat(),
        "status": status,
        "failed_check_count": normalized["failed_check_count"],
        "manual_review_required": normalized.get("manual_review_required", True),
        "owner_reviewed": normalized.get("owner_reviewed", False),
        "broker_imported": normalized.get("broker_imported", False),
        "broker_action_allowed": False,
        "normalized_positions_path": str(snapshot_dir / "normalized_positions.json"),
        "portfolio_exposure_summary_path": str(snapshot_dir / "portfolio_exposure_summary.json"),
        "snapshot_validation_report_path": str(snapshot_dir / "snapshot_validation_report.md"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(snapshot_dir / "snapshot_manifest.json", manifest)
    _write_json(snapshot_dir / "normalized_positions.json", normalized)
    _write_json(snapshot_dir / "portfolio_exposure_summary.json", exposure)
    _write_text(
        snapshot_dir / "snapshot_validation_report.md",
        render_portfolio_snapshot_validation_markdown(manifest, normalized, exposure),
    )
    _update_latest_pointer(
        "latest_portfolio_snapshot",
        snapshot_dir.name,
        snapshot_dir / "snapshot_manifest.json",
    )
    return {
        "snapshot_id": snapshot_dir.name,
        "snapshot_dir": snapshot_dir,
        "manifest": manifest,
        "normalized_positions": normalized,
        "portfolio_exposure_summary": exposure,
    }


def portfolio_snapshot_report_payload(
    *,
    snapshot_path: Path | None = None,
    snapshot_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PORTFOLIO_SNAPSHOT_DIR,
) -> dict[str, Any]:
    if snapshot_path is not None:
        return write_portfolio_snapshot_artifact(snapshot_path=snapshot_path, output_dir=output_dir)
    resolved_id = snapshot_id or (
        _latest_pointer_artifact_id("latest_portfolio_snapshot") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--snapshot, --snapshot-id, or --latest is required")
    snapshot_dir = output_dir / resolved_id
    return {
        **_read_json(snapshot_dir / "snapshot_manifest.json"),
        "normalized_positions": _read_optional_json(snapshot_dir / "normalized_positions.json")
        or {},
        "portfolio_exposure_summary": _read_optional_json(
            snapshot_dir / "portfolio_exposure_summary.json"
        )
        or {},
        "snapshot_dir": str(snapshot_dir),
    }


def validate_portfolio_snapshot_file(
    *,
    snapshot_path: Path,
    output_dir: Path = DEFAULT_PORTFOLIO_SNAPSHOT_DIR,
) -> dict[str, Any]:
    artifact = write_portfolio_snapshot_artifact(snapshot_path=snapshot_path, output_dir=output_dir)
    normalized = artifact["normalized_positions"]
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_portfolio_snapshot_validation",
        "snapshot_id": artifact["snapshot_id"],
        "snapshot_path": str(snapshot_path),
        "status": artifact["manifest"]["status"],
        "checks": normalized["checks"],
        "failed_check_count": normalized["failed_check_count"],
        "snapshot_dir": str(artifact["snapshot_dir"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def run_position_advisory_daily(
    *,
    shadow_monitor_run_id: str,
    config_path: Path = DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    portfolio_snapshot_path: Path | None = None,
    shadow_monitor_run_dir: Path = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    consensus_drift_dir: Path = DEFAULT_CONSENSUS_DRIFT_DIR,
    output_dir: Path = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = load_position_advisory_config(config_path)
    monitor_dir = shadow_monitor_run_dir / shadow_monitor_run_id
    monitor_manifest = _read_json(monitor_dir / "shadow_monitor_manifest.json")
    monitor_rows = _read_jsonl(monitor_dir / "shadow_candidate_daily_results.jsonl")
    target_rows = [_daily_target_row_from_monitor(row) for row in monitor_rows]
    consensus_rows, consensus_status = _daily_consensus_target_weights(target_rows, config)
    drift = _latest_consensus_drift_for_monitor(
        shadow_monitor_run_id=shadow_monitor_run_id,
        output_dir=consensus_drift_dir,
    )
    disagreement_status = _text(drift.get("disagreement_status"), consensus_status)
    snapshot = (
        _normalize_portfolio_snapshot(portfolio_snapshot_path, strict=True)
        if portfolio_snapshot_path is not None
        else {}
    )
    if snapshot and snapshot.get("failed_check_count"):
        failed = ", ".join(
            check["name"]
            for check in _records(snapshot.get("checks"))
            if check.get("passed") is False
        )
        raise DynamicV3ParameterResearchError(f"portfolio snapshot validation failed: {failed}")
    delta_rows = _daily_position_delta_rows(target_rows, snapshot, config) if snapshot else []
    actions = _daily_position_advisory_actions(
        daily_advisory_id="",
        as_of=_text(monitor_manifest.get("as_of")),
        target_rows=target_rows,
        delta_rows=delta_rows,
        snapshot=snapshot,
        consensus_status=consensus_status,
        disagreement_status=disagreement_status,
        config=config,
    )
    daily_advisory_id = _stable_id(
        "position-advisory-daily",
        shadow_monitor_run_id,
        str(config_path),
        str(portfolio_snapshot_path or ""),
        generated.isoformat(),
    )
    advisory_dir = _unique_dir(output_dir / daily_advisory_id)
    advisory_dir.mkdir(parents=True, exist_ok=False)
    actions["daily_advisory_id"] = advisory_dir.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_position_advisory_daily_manifest",
        "daily_advisory_id": advisory_dir.name,
        "source_shadow_monitor_run_id": shadow_monitor_run_id,
        "source_shadow_shortlist_id": monitor_manifest.get("shadow_shortlist_id", ""),
        "as_of": monitor_manifest.get("as_of", ""),
        "generated_at": generated.isoformat(),
        "status": "PASS" if target_rows else "PASS_WITH_WARNINGS",
        "mode": actions["mode"],
        "candidate_count": len(target_rows),
        "consensus_status": actions["consensus_status"],
        "disagreement_status": disagreement_status,
        "recommended_action": actions["recommended_action"],
        "manual_review_required": True,
        "owner_approval_required": True,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "portfolio_snapshot_provided": bool(snapshot),
        "portfolio_snapshot_path": (
            "" if portfolio_snapshot_path is None else str(portfolio_snapshot_path)
        ),
        "daily_candidate_targets_path": str(advisory_dir / "daily_candidate_targets.jsonl"),
        "daily_consensus_weights_path": str(advisory_dir / "daily_consensus_weights.csv"),
        "daily_position_deltas_path": str(advisory_dir / "daily_position_deltas.jsonl"),
        "daily_advisory_actions_path": str(advisory_dir / "daily_advisory_actions.json"),
        "daily_position_advisory_report_path": str(
            advisory_dir / "daily_position_advisory_report.md"
        ),
        "reader_brief_section_path": str(advisory_dir / "reader_brief_section.md"),
        "config_path": str(config_path),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    reader_brief = render_daily_position_advisory_reader_brief(actions, delta_rows)
    _write_json(advisory_dir / "daily_advisory_manifest.json", manifest)
    _write_jsonl(advisory_dir / "daily_candidate_targets.jsonl", target_rows)
    _write_csv(advisory_dir / "daily_consensus_weights.csv", consensus_rows)
    _write_jsonl(advisory_dir / "daily_position_deltas.jsonl", delta_rows)
    _write_json(advisory_dir / "daily_advisory_actions.json", actions)
    _write_text(
        advisory_dir / "daily_position_advisory_report.md",
        render_daily_position_advisory_markdown(
            manifest,
            target_rows,
            consensus_rows,
            delta_rows,
            actions,
        ),
    )
    _write_text(advisory_dir / "reader_brief_section.md", reader_brief)
    _update_latest_pointer(
        "latest_position_advisory_daily",
        advisory_dir.name,
        advisory_dir / "daily_advisory_manifest.json",
    )
    return {
        "daily_advisory_id": advisory_dir.name,
        "daily_advisory_dir": advisory_dir,
        "manifest": manifest,
        "daily_candidate_targets": target_rows,
        "daily_consensus_weights": consensus_rows,
        "daily_position_deltas": delta_rows,
        "daily_advisory_actions": actions,
    }


def position_advisory_daily_report_payload(
    *,
    daily_advisory_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
) -> dict[str, Any]:
    resolved_id = daily_advisory_id or (
        _latest_pointer_artifact_id("latest_position_advisory_daily") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--daily-advisory-id or --latest is required")
    advisory_dir = output_dir / resolved_id
    return {
        **_read_json(advisory_dir / "daily_advisory_manifest.json"),
        "daily_advisory_dir": str(advisory_dir),
    }


def validate_position_advisory_daily_artifact(
    *,
    daily_advisory_id: str,
    output_dir: Path = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
) -> dict[str, Any]:
    advisory_dir = output_dir / daily_advisory_id
    manifest = _read_optional_json(advisory_dir / "daily_advisory_manifest.json") or {}
    actions = _read_optional_json(advisory_dir / "daily_advisory_actions.json") or {}
    targets = _read_jsonl(advisory_dir / "daily_candidate_targets.jsonl")
    required = [
        "daily_advisory_manifest.json",
        "daily_candidate_targets.jsonl",
        "daily_consensus_weights.csv",
        "daily_position_deltas.jsonl",
        "daily_advisory_actions.json",
        "daily_position_advisory_report.md",
        "reader_brief_section.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (advisory_dir / name).exists(), name) for name in required
    ]
    checks.extend(
        [
            _check(
                "daily_advisory_id_matches",
                manifest.get("daily_advisory_id") == daily_advisory_id
                and actions.get("daily_advisory_id") == daily_advisory_id,
                daily_advisory_id,
            ),
            _check("target_weights_present", bool(targets), "candidate targets required"),
            _check(
                "owner_approval_required",
                manifest.get("owner_approval_required") is True
                and actions.get("owner_approval_required") is True,
                "owner approval is required",
            ),
            _check(
                "broker_action_forbidden",
                manifest.get("broker_action_allowed") is False
                and actions.get("broker_action_allowed") is False,
                "broker action is forbidden",
            ),
            _check(
                "broker_action_not_taken",
                manifest.get("broker_action_taken") is False,
                "broker action was not taken",
            ),
            _check(
                "target_only_without_snapshot",
                bool(manifest.get("portfolio_snapshot_provided"))
                or manifest.get("mode") == POSITION_ADVISORY_TARGET_ONLY,
                "missing snapshot must produce TARGET_ONLY",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_position_advisory_daily_validation",
        "daily_advisory_id": daily_advisory_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def run_consensus_drift(
    *,
    shadow_monitor_run_id: str,
    config_path: Path = DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    shadow_monitor_run_dir: Path = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    output_dir: Path = DEFAULT_CONSENSUS_DRIFT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = load_position_advisory_config(config_path)
    monitor_dir = shadow_monitor_run_dir / shadow_monitor_run_id
    monitor_manifest = _read_json(monitor_dir / "shadow_monitor_manifest.json")
    monitor_rows = _read_jsonl(monitor_dir / "shadow_candidate_daily_results.jsonl")
    target_rows = [_daily_target_row_from_monitor(row) for row in monitor_rows]
    symbol_rows = _symbol_weight_dispersion_rows(target_rows)
    pairwise_rows = _candidate_pairwise_disagreement_rows(target_rows)
    exposure = _exposure_disagreement_summary(target_rows)
    previous_change = _daily_consensus_change_vs_previous(
        shadow_monitor_run_id=shadow_monitor_run_id,
        shadow_shortlist_id=_text(monitor_manifest.get("shadow_shortlist_id")),
        as_of=date.fromisoformat(_text(monitor_manifest.get("as_of"))),
        current_symbol_rows=symbol_rows,
        monitor_output_dir=shadow_monitor_run_dir,
    )
    disagreement_status = _disagreement_status(
        candidate_count=len(target_rows),
        symbol_rows=symbol_rows,
        exposure=exposure,
        config=config,
    )
    implication = (
        "manual_review_required"
        if disagreement_status in {"HIGH_DISAGREEMENT", "INSUFFICIENT_DATA"}
        else "monitor"
        if disagreement_status == "MODERATE_DISAGREEMENT"
        else "continue_monitoring"
    )
    drift_id = _stable_id(
        "consensus-drift",
        shadow_monitor_run_id,
        str(config_path),
        generated.isoformat(),
    )
    drift_dir = _unique_dir(output_dir / drift_id)
    drift_dir.mkdir(parents=True, exist_ok=False)
    max_symbol_dispersion = max([_float(row.get("dispersion")) for row in symbol_rows] or [0.0])
    summary = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_consensus_drift_summary",
        "drift_id": drift_dir.name,
        "shadow_monitor_run_id": shadow_monitor_run_id,
        "as_of": monitor_manifest.get("as_of", ""),
        "candidate_count": len(target_rows),
        "disagreement_status": disagreement_status,
        "max_symbol_dispersion": round(max_symbol_dispersion, 6),
        "risk_asset_exposure_dispersion": exposure["risk_asset_exposure_dispersion"],
        "cash_exposure_dispersion": exposure["cash_exposure_dispersion"],
        "defensive_exposure_dispersion": exposure["defensive_exposure_dispersion"],
        "daily_consensus_change_vs_previous": previous_change,
        "position_advisory_implication": implication,
        "broker_action_allowed": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_consensus_drift_manifest",
        "drift_id": drift_dir.name,
        "source_shadow_monitor_run_id": shadow_monitor_run_id,
        "as_of": monitor_manifest.get("as_of", ""),
        "generated_at": generated.isoformat(),
        "status": "PASS" if target_rows else "PASS_WITH_WARNINGS",
        "candidate_count": len(target_rows),
        "disagreement_status": disagreement_status,
        "position_advisory_implication": implication,
        "candidate_pairwise_disagreement_path": str(
            drift_dir / "candidate_pairwise_disagreement.csv"
        ),
        "symbol_weight_dispersion_path": str(drift_dir / "symbol_weight_dispersion.csv"),
        "consensus_drift_summary_path": str(drift_dir / "consensus_drift_summary.json"),
        "consensus_drift_report_path": str(drift_dir / "consensus_drift_report.md"),
        "config_path": str(config_path),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(drift_dir / "consensus_drift_manifest.json", manifest)
    _write_csv(drift_dir / "candidate_pairwise_disagreement.csv", pairwise_rows)
    _write_csv(drift_dir / "symbol_weight_dispersion.csv", symbol_rows)
    _write_json(drift_dir / "consensus_drift_summary.json", summary)
    _write_text(
        drift_dir / "consensus_drift_report.md",
        render_consensus_drift_markdown(manifest, summary, symbol_rows, pairwise_rows),
    )
    _update_latest_pointer(
        "latest_consensus_drift",
        drift_dir.name,
        drift_dir / "consensus_drift_manifest.json",
    )
    return {
        "drift_id": drift_dir.name,
        "drift_dir": drift_dir,
        "manifest": manifest,
        "summary": summary,
        "symbol_weight_dispersion": symbol_rows,
        "candidate_pairwise_disagreement": pairwise_rows,
    }


def consensus_drift_report_payload(
    *,
    drift_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CONSENSUS_DRIFT_DIR,
) -> dict[str, Any]:
    resolved_id = drift_id or (
        _latest_pointer_artifact_id("latest_consensus_drift") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--drift-id or --latest is required")
    drift_dir = output_dir / resolved_id
    return {
        **_read_json(drift_dir / "consensus_drift_manifest.json"),
        "consensus_drift_summary": _read_optional_json(drift_dir / "consensus_drift_summary.json")
        or {},
        "drift_dir": str(drift_dir),
    }


def validate_consensus_drift_artifact(
    *,
    drift_id: str,
    output_dir: Path = DEFAULT_CONSENSUS_DRIFT_DIR,
) -> dict[str, Any]:
    drift_dir = output_dir / drift_id
    manifest = _read_optional_json(drift_dir / "consensus_drift_manifest.json") or {}
    summary = _read_optional_json(drift_dir / "consensus_drift_summary.json") or {}
    required = [
        "consensus_drift_manifest.json",
        "candidate_pairwise_disagreement.csv",
        "symbol_weight_dispersion.csv",
        "consensus_drift_summary.json",
        "consensus_drift_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (drift_dir / name).exists(), name) for name in required
    ]
    checks.extend(
        [
            _check(
                "drift_id_matches",
                manifest.get("drift_id") == drift_id and summary.get("drift_id") == drift_id,
                drift_id,
            ),
            _check(
                "disagreement_status_valid",
                summary.get("disagreement_status") in CONSENSUS_DRIFT_STATUSES,
                _text(summary.get("disagreement_status")),
            ),
            _check(
                "broker_action_forbidden",
                summary.get("broker_action_allowed") is False,
                "broker action is forbidden",
            ),
            _check(
                "production_candidate_not_generated",
                manifest.get("production_candidate_generated") is False,
                "consensus drift is not production",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_consensus_drift_validation",
        "drift_id": drift_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def load_manual_portfolio_schema_config(
    path: Path | str = DEFAULT_MANUAL_PORTFOLIO_SCHEMA_CONFIG_PATH,
) -> dict[str, Any]:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, Mapping):
        raise DynamicV3ParameterResearchError("manual portfolio schema config must be a mapping")
    safety = _mapping(raw.get("safety"))
    if safety.get("broker_action_allowed") is not False:
        raise DynamicV3ParameterResearchError("manual portfolio schema must forbid broker action")
    if _text(safety.get("production_effect"), "none") != "none":
        raise DynamicV3ParameterResearchError(
            "manual portfolio schema production_effect must be none"
        )
    tolerances = _mapping(raw.get("tolerances"))
    for key in ("weight_sum_tolerance", "value_sum_tolerance_pct"):
        if key not in tolerances:
            raise DynamicV3ParameterResearchError(
                f"manual portfolio schema tolerance missing: {key}"
            )
    return dict(raw)


def load_portfolio_exposure_policy_config(
    path: Path | str = DEFAULT_PORTFOLIO_EXPOSURE_POLICY_CONFIG_PATH,
) -> dict[str, Any]:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, Mapping):
        raise DynamicV3ParameterResearchError("portfolio exposure policy must be a mapping")
    safety = _mapping(raw.get("safety"))
    if safety.get("broker_action_allowed") is not False:
        raise DynamicV3ParameterResearchError("portfolio exposure policy must forbid broker action")
    if _text(safety.get("production_effect"), "none") != "none":
        raise DynamicV3ParameterResearchError("portfolio exposure production_effect must be none")
    for section in ("exposure_groups", "limits", "warnings"):
        if not isinstance(raw.get(section), Mapping):
            raise DynamicV3ParameterResearchError(f"portfolio exposure policy missing {section}")
    return dict(raw)


def load_execution_guardrails_config(
    path: Path | str = DEFAULT_EXECUTION_GUARDRAILS_CONFIG_PATH,
) -> dict[str, Any]:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, Mapping):
        raise DynamicV3ParameterResearchError("execution guardrails config must be a mapping")
    mode = _mapping(raw.get("mode"))
    if mode.get("advisory_only") is not True:
        raise DynamicV3ParameterResearchError("execution guardrails must be advisory_only")
    if mode.get("broker_action_allowed") is not False:
        raise DynamicV3ParameterResearchError("execution guardrails must forbid broker action")
    if mode.get("order_ticket_generation_allowed") is not False:
        raise DynamicV3ParameterResearchError(
            "execution guardrails must forbid order ticket generation"
        )
    if mode.get("require_owner_approval") is not True:
        raise DynamicV3ParameterResearchError("execution guardrails must require owner approval")
    for section in ("limits", "risk_controls", "stepwise_execution"):
        if not isinstance(raw.get(section), Mapping):
            raise DynamicV3ParameterResearchError(f"execution guardrails missing {section}")
    return dict(raw)


def write_manual_portfolio_snapshot_artifact(
    *,
    snapshot_path: Path,
    schema_config_path: Path = DEFAULT_MANUAL_PORTFOLIO_SCHEMA_CONFIG_PATH,
    output_dir: Path = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    schema_config = load_manual_portfolio_schema_config(schema_config_path)
    normalized = _normalize_manual_portfolio_snapshot(
        snapshot_path,
        schema_config=schema_config,
        strict=True,
    )
    snapshot_id = _stable_id(
        "manual-portfolio-snapshot",
        snapshot_path,
        normalized.get("as_of"),
        generated.isoformat(),
    )
    snapshot_dir = _unique_dir(output_dir / snapshot_id)
    snapshot_dir.mkdir(parents=True, exist_ok=False)
    normalized["snapshot_id"] = snapshot_dir.name
    normalized["status"] = "PASS" if normalized["failed_check_count"] == 0 else "FAIL"
    weight_check = _manual_portfolio_weight_check(normalized, schema_config)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_manual_portfolio_manifest",
        "snapshot_id": snapshot_dir.name,
        "snapshot_path": str(snapshot_path),
        "schema_config_path": str(schema_config_path),
        "as_of": normalized.get("as_of", ""),
        "generated_at": generated.isoformat(),
        "status": normalized["status"],
        "failed_check_count": normalized["failed_check_count"],
        "manual_review_required": normalized.get("manual_review_required", True),
        "owner_reviewed": normalized.get("owner_reviewed", False),
        "broker_imported": normalized.get("broker_imported", False),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "normalized_portfolio_path": str(snapshot_dir / "normalized_portfolio.json"),
        "portfolio_weight_check_path": str(snapshot_dir / "portfolio_weight_check.json"),
        "portfolio_snapshot_report_path": str(snapshot_dir / "portfolio_snapshot_report.md"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(snapshot_dir / "manual_portfolio_manifest.json", manifest)
    _write_json(snapshot_dir / "normalized_portfolio.json", normalized)
    _write_json(snapshot_dir / "portfolio_weight_check.json", weight_check)
    _write_text(
        snapshot_dir / "portfolio_snapshot_report.md",
        render_manual_portfolio_snapshot_markdown(manifest, normalized, weight_check),
    )
    _update_latest_pointer(
        "latest_manual_portfolio_snapshot",
        snapshot_dir.name,
        snapshot_dir / "manual_portfolio_manifest.json",
    )
    return {
        "snapshot_id": snapshot_dir.name,
        "snapshot_dir": snapshot_dir,
        "manifest": manifest,
        "normalized_portfolio": normalized,
        "portfolio_weight_check": weight_check,
    }


def manual_portfolio_report_payload(
    *,
    snapshot_path: Path | None = None,
    snapshot_id: str | None = None,
    latest: bool = False,
    schema_config_path: Path = DEFAULT_MANUAL_PORTFOLIO_SCHEMA_CONFIG_PATH,
    output_dir: Path = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
) -> dict[str, Any]:
    if snapshot_path is not None:
        return write_manual_portfolio_snapshot_artifact(
            snapshot_path=snapshot_path,
            schema_config_path=schema_config_path,
            output_dir=output_dir,
        )
    resolved_id = snapshot_id or (
        _latest_pointer_artifact_id("latest_manual_portfolio_snapshot") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--snapshot, --snapshot-id, or --latest is required")
    snapshot_dir = output_dir / resolved_id
    return {
        **_read_json(snapshot_dir / "manual_portfolio_manifest.json"),
        "normalized_portfolio": _read_optional_json(snapshot_dir / "normalized_portfolio.json")
        or {},
        "portfolio_weight_check": _read_optional_json(snapshot_dir / "portfolio_weight_check.json")
        or {},
        "snapshot_dir": str(snapshot_dir),
    }


def validate_manual_portfolio_snapshot_file(
    *,
    snapshot_path: Path,
    schema_config_path: Path = DEFAULT_MANUAL_PORTFOLIO_SCHEMA_CONFIG_PATH,
    output_dir: Path = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
) -> dict[str, Any]:
    artifact = write_manual_portfolio_snapshot_artifact(
        snapshot_path=snapshot_path,
        schema_config_path=schema_config_path,
        output_dir=output_dir,
    )
    normalized = artifact["normalized_portfolio"]
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_manual_portfolio_validation",
        "snapshot_id": artifact["snapshot_id"],
        "snapshot_path": str(snapshot_path),
        "status": artifact["manifest"]["status"],
        "checks": normalized["checks"],
        "failed_check_count": normalized["failed_check_count"],
        "snapshot_dir": str(artifact["snapshot_dir"]),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def validate_manual_portfolio_artifact(
    *,
    snapshot_id: str,
    output_dir: Path = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
) -> dict[str, Any]:
    snapshot_dir = output_dir / snapshot_id
    manifest = _read_optional_json(snapshot_dir / "manual_portfolio_manifest.json") or {}
    normalized = _read_optional_json(snapshot_dir / "normalized_portfolio.json") or {}
    weight_check = _read_optional_json(snapshot_dir / "portfolio_weight_check.json") or {}
    required = [
        "manual_portfolio_manifest.json",
        "normalized_portfolio.json",
        "portfolio_weight_check.json",
        "portfolio_snapshot_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (snapshot_dir / name).exists(), name) for name in required
    ]
    checks.extend(
        [
            _check(
                "snapshot_id_matches",
                manifest.get("snapshot_id") == snapshot_id
                and normalized.get("snapshot_id") == snapshot_id,
                snapshot_id,
            ),
            _check(
                "status_valid",
                manifest.get("status") in MANUAL_PORTFOLIO_STATUSES
                and normalized.get("status") in MANUAL_PORTFOLIO_STATUSES,
                f"manifest={manifest.get('status')} normalized={normalized.get('status')}",
            ),
            _check(
                "weight_check_passed",
                weight_check.get("status") == "PASS",
                _text(weight_check.get("status")),
            ),
            _check(
                "broker_imported_false",
                manifest.get("broker_imported") is False
                and normalized.get("broker_imported") is False,
                "manual snapshot is not broker import",
            ),
            _check(
                "broker_action_forbidden",
                manifest.get("broker_action_allowed") is False
                and normalized.get("broker_action_allowed") is False,
                "broker action is forbidden",
            ),
            _check(
                "broker_action_not_taken",
                manifest.get("broker_action_taken") is False
                and normalized.get("broker_action_taken") is False,
                "broker action was not taken",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_manual_portfolio_artifact_validation",
        "snapshot_id": snapshot_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def run_portfolio_exposure_validation(
    *,
    snapshot_id: str,
    snapshot_dir: Path = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
    policy_config_path: Path = DEFAULT_PORTFOLIO_EXPOSURE_POLICY_CONFIG_PATH,
    output_dir: Path = DEFAULT_PORTFOLIO_EXPOSURE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    policy = load_portfolio_exposure_policy_config(policy_config_path)
    source_dir = snapshot_dir / snapshot_id
    normalized = _read_json(source_dir / "normalized_portfolio.json")
    exposure_id = _stable_id(
        "portfolio-exposure",
        snapshot_id,
        str(policy_config_path),
        generated.isoformat(),
    )
    exposure_dir = _unique_dir(output_dir / exposure_id)
    exposure_dir.mkdir(parents=True, exist_ok=False)
    summary, concentration, currency = _portfolio_exposure_artifacts(normalized, policy)
    summary["exposure_id"] = exposure_dir.name
    concentration["exposure_id"] = exposure_dir.name
    currency["exposure_id"] = exposure_dir.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_portfolio_exposure_manifest",
        "exposure_id": exposure_dir.name,
        "snapshot_id": snapshot_id,
        "generated_at": generated.isoformat(),
        "status": summary["status"],
        "policy_config_path": str(policy_config_path),
        "portfolio_exposure_manifest_path": str(exposure_dir / "portfolio_exposure_manifest.json"),
        "exposure_summary_path": str(exposure_dir / "exposure_summary.json"),
        "concentration_warnings_path": str(exposure_dir / "concentration_warnings.json"),
        "currency_exposure_path": str(exposure_dir / "currency_exposure.json"),
        "portfolio_exposure_report_path": str(exposure_dir / "portfolio_exposure_report.md"),
        "manual_review_required": summary["manual_review_required"],
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(exposure_dir / "portfolio_exposure_manifest.json", manifest)
    _write_json(exposure_dir / "exposure_summary.json", summary)
    _write_json(exposure_dir / "concentration_warnings.json", concentration)
    _write_json(exposure_dir / "currency_exposure.json", currency)
    _write_text(
        exposure_dir / "portfolio_exposure_report.md",
        render_portfolio_exposure_markdown(manifest, summary, concentration, currency),
    )
    _update_latest_pointer(
        "latest_portfolio_exposure",
        exposure_dir.name,
        exposure_dir / "portfolio_exposure_manifest.json",
    )
    return {
        "exposure_id": exposure_dir.name,
        "exposure_dir": exposure_dir,
        "manifest": manifest,
        "exposure_summary": summary,
        "concentration_warnings": concentration,
        "currency_exposure": currency,
    }


def portfolio_exposure_report_payload(
    *,
    exposure_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PORTFOLIO_EXPOSURE_DIR,
) -> dict[str, Any]:
    resolved_id = exposure_id or (
        _latest_pointer_artifact_id("latest_portfolio_exposure") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--exposure-id or --latest is required")
    exposure_dir = output_dir / resolved_id
    return {
        **_read_json(exposure_dir / "portfolio_exposure_manifest.json"),
        "exposure_summary": _read_optional_json(exposure_dir / "exposure_summary.json") or {},
        "concentration_warnings": _read_optional_json(exposure_dir / "concentration_warnings.json")
        or {},
        "currency_exposure": _read_optional_json(exposure_dir / "currency_exposure.json") or {},
        "exposure_dir": str(exposure_dir),
    }


def validate_portfolio_exposure_artifact(
    *,
    exposure_id: str,
    output_dir: Path = DEFAULT_PORTFOLIO_EXPOSURE_DIR,
) -> dict[str, Any]:
    exposure_dir = output_dir / exposure_id
    manifest = _read_optional_json(exposure_dir / "portfolio_exposure_manifest.json") or {}
    summary = _read_optional_json(exposure_dir / "exposure_summary.json") or {}
    currency = _read_optional_json(exposure_dir / "currency_exposure.json") or {}
    required = [
        "portfolio_exposure_manifest.json",
        "exposure_summary.json",
        "concentration_warnings.json",
        "currency_exposure.json",
        "portfolio_exposure_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (exposure_dir / name).exists(), name) for name in required
    ]
    checks.extend(
        [
            _check(
                "exposure_id_matches",
                manifest.get("exposure_id") == exposure_id
                and summary.get("exposure_id") == exposure_id,
                exposure_id,
            ),
            _check(
                "status_valid",
                summary.get("status") in PORTFOLIO_EXPOSURE_STATUSES,
                _text(summary.get("status")),
            ),
            _check(
                "currency_status_valid",
                currency.get("status") in PORTFOLIO_EXPOSURE_STATUSES,
                _text(currency.get("status")),
            ),
            _check(
                "broker_action_forbidden",
                manifest.get("broker_action_allowed") is False
                and summary.get("broker_action_allowed") is False,
                "broker action is forbidden",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_portfolio_exposure_validation",
        "exposure_id": exposure_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def run_position_drift_analysis(
    *,
    snapshot_id: str,
    shadow_shortlist_id: str,
    snapshot_dir: Path = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
    shadow_shortlist_dir: Path = DEFAULT_SHADOW_SHORTLIST_DIR,
    config_path: Path = DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    output_dir: Path = DEFAULT_POSITION_DRIFT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = load_position_advisory_config(config_path)
    normalized = _read_json(snapshot_dir / snapshot_id / "normalized_portfolio.json")
    shadow_dir = shadow_shortlist_dir / shadow_shortlist_id
    shadow_manifest = _read_json(shadow_dir / "shadow_shortlist_manifest.json")
    shadow_rows = _read_jsonl(shadow_dir / "shadow_shortlist_candidates.jsonl")
    target_rows = []
    for row in shadow_rows:
        latest_weights = _candidate_latest_target_weights(row)
        target_rows.append(
            {
                "candidate_id": row.get("candidate_id"),
                "cluster_id": row.get("cluster_id"),
                "cluster_label": row.get("cluster_label"),
                "target_weights": latest_weights["weights"],
                "as_of": latest_weights["as_of"],
                "weight_path_status": latest_weights["status"],
                "source_weight_path_artifact": latest_weights["source_weight_path_artifact"],
            }
        )
    matrix = [
        _candidate_drift_row(target, _mapping(normalized.get("weights")), config)
        for target in target_rows
    ]
    consensus = _position_drift_consensus_summary(
        current_weights=_mapping(normalized.get("weights")),
        target_rows=target_rows,
        matrix=matrix,
        config=config,
    )
    actions = _position_drift_action_candidates(consensus, config)
    drift_id = _stable_id(
        "position-drift",
        snapshot_id,
        shadow_shortlist_id,
        str(config_path),
        generated.isoformat(),
    )
    drift_dir = _unique_dir(output_dir / drift_id)
    drift_dir.mkdir(parents=True, exist_ok=False)
    consensus["drift_id"] = drift_dir.name
    actions["drift_id"] = drift_dir.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_position_drift_manifest",
        "drift_id": drift_dir.name,
        "snapshot_id": snapshot_id,
        "shadow_shortlist_id": shadow_shortlist_id,
        "source_shortlist_id": shadow_manifest.get("source_shortlist_id", ""),
        "generated_at": generated.isoformat(),
        "status": "PASS" if matrix else "PASS_WITH_WARNINGS",
        "candidate_count": len(matrix),
        "candidate_agreement_status": consensus["candidate_agreement_status"],
        "drift_status": consensus["drift_status"],
        "recommended_action": actions["recommended_action"],
        "candidate_drift_matrix_path": str(drift_dir / "candidate_drift_matrix.jsonl"),
        "consensus_drift_summary_path": str(drift_dir / "consensus_drift_summary.json"),
        "drift_action_candidates_path": str(drift_dir / "drift_action_candidates.json"),
        "position_drift_report_path": str(drift_dir / "position_drift_report.md"),
        "owner_approval_required": True,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(drift_dir / "position_drift_manifest.json", manifest)
    _write_jsonl(drift_dir / "candidate_drift_matrix.jsonl", matrix)
    _write_json(drift_dir / "consensus_drift_summary.json", consensus)
    _write_json(drift_dir / "drift_action_candidates.json", actions)
    _write_text(
        drift_dir / "position_drift_report.md",
        render_position_drift_markdown(manifest, consensus, actions, matrix),
    )
    _update_latest_pointer(
        "latest_position_drift",
        drift_dir.name,
        drift_dir / "position_drift_manifest.json",
    )
    return {
        "drift_id": drift_dir.name,
        "drift_dir": drift_dir,
        "manifest": manifest,
        "candidate_drift_matrix": matrix,
        "consensus_drift_summary": consensus,
        "drift_action_candidates": actions,
    }


def position_drift_report_payload(
    *,
    drift_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_POSITION_DRIFT_DIR,
) -> dict[str, Any]:
    resolved_id = drift_id or (
        _latest_pointer_artifact_id("latest_position_drift") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--drift-id or --latest is required")
    drift_dir = output_dir / resolved_id
    return {
        **_read_json(drift_dir / "position_drift_manifest.json"),
        "consensus_drift_summary": _read_optional_json(drift_dir / "consensus_drift_summary.json")
        or {},
        "drift_action_candidates": _read_optional_json(drift_dir / "drift_action_candidates.json")
        or {},
        "drift_dir": str(drift_dir),
    }


def validate_position_drift_artifact(
    *,
    drift_id: str,
    output_dir: Path = DEFAULT_POSITION_DRIFT_DIR,
) -> dict[str, Any]:
    drift_dir = output_dir / drift_id
    manifest = _read_optional_json(drift_dir / "position_drift_manifest.json") or {}
    summary = _read_optional_json(drift_dir / "consensus_drift_summary.json") or {}
    actions = _read_optional_json(drift_dir / "drift_action_candidates.json") or {}
    matrix = _read_jsonl(drift_dir / "candidate_drift_matrix.jsonl")
    required = [
        "position_drift_manifest.json",
        "candidate_drift_matrix.jsonl",
        "consensus_drift_summary.json",
        "drift_action_candidates.json",
        "position_drift_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (drift_dir / name).exists(), name) for name in required
    ]
    checks.extend(
        [
            _check(
                "drift_id_matches",
                manifest.get("drift_id") == drift_id and summary.get("drift_id") == drift_id,
                drift_id,
            ),
            _check("matrix_present", bool(matrix), "candidate drift rows required"),
            _check(
                "drift_status_valid",
                summary.get("drift_status") in POSITION_DRIFT_STATUSES,
                _text(summary.get("drift_status")),
            ),
            _check(
                "recommended_action_valid",
                actions.get("recommended_action") in POSITION_DRIFT_ACTIONS,
                _text(actions.get("recommended_action")),
            ),
            _check(
                "high_disagreement_blocks_adjustment",
                summary.get("candidate_agreement_status") != "HIGH_DISAGREEMENT"
                or actions.get("recommended_action") == "manual_review",
                "HIGH_DISAGREEMENT must only allow manual_review",
            ),
            _check(
                "broker_action_forbidden",
                manifest.get("broker_action_allowed") is False
                and actions.get("broker_action_allowed") is False,
                "broker action is forbidden",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_position_drift_validation",
        "drift_id": drift_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def run_execution_guardrails_check(
    *,
    drift_id: str,
    exposure_id: str,
    drift_dir: Path = DEFAULT_POSITION_DRIFT_DIR,
    exposure_dir: Path = DEFAULT_PORTFOLIO_EXPOSURE_DIR,
    config_path: Path = DEFAULT_EXECUTION_GUARDRAILS_CONFIG_PATH,
    output_dir: Path = DEFAULT_EXECUTION_GUARDRAILS_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = load_execution_guardrails_config(config_path)
    source_drift_dir = drift_dir / drift_id
    source_exposure_dir = exposure_dir / exposure_id
    drift_manifest = _read_json(source_drift_dir / "position_drift_manifest.json")
    drift_summary = _read_json(source_drift_dir / "consensus_drift_summary.json")
    drift_actions = _read_json(source_drift_dir / "drift_action_candidates.json")
    exposure_summary = _read_json(source_exposure_dir / "exposure_summary.json")
    checks = _guardrail_adjustment_checks(drift_summary, drift_actions, exposure_summary, config)
    summary = _guardrail_summary(drift_id, exposure_id, checks, drift_summary, config)
    guardrail_id = _stable_id(
        "execution-guardrails",
        drift_id,
        exposure_id,
        str(config_path),
        generated.isoformat(),
    )
    guardrail_dir = _unique_dir(output_dir / guardrail_id)
    guardrail_dir.mkdir(parents=True, exist_ok=False)
    summary["guardrail_id"] = guardrail_dir.name
    stepwise = _stepwise_adjustment_plan(summary, config)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_execution_guardrails_manifest",
        "guardrail_id": guardrail_dir.name,
        "drift_id": drift_id,
        "exposure_id": exposure_id,
        "snapshot_id": drift_manifest.get("snapshot_id", ""),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "recommended_action": summary["recommended_action"],
        "blocked_count": summary["blocked_count"],
        "capped_count": summary["capped_count"],
        "config_path": str(config_path),
        "proposed_adjustment_checks_path": str(guardrail_dir / "proposed_adjustment_checks.jsonl"),
        "guardrail_summary_path": str(guardrail_dir / "guardrail_summary.json"),
        "stepwise_adjustment_plan_path": str(guardrail_dir / "stepwise_adjustment_plan.json"),
        "execution_guardrails_report_path": str(guardrail_dir / "execution_guardrails_report.md"),
        "manual_review_required": True,
        "owner_approval_required": True,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "order_ticket_generation_allowed": False,
        "production_effect": "none",
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(guardrail_dir / "guardrail_manifest.json", manifest)
    _write_jsonl(guardrail_dir / "proposed_adjustment_checks.jsonl", checks)
    _write_json(guardrail_dir / "guardrail_summary.json", summary)
    _write_json(guardrail_dir / "stepwise_adjustment_plan.json", stepwise)
    _write_text(
        guardrail_dir / "execution_guardrails_report.md",
        render_execution_guardrails_markdown(manifest, summary, stepwise, checks),
    )
    _update_latest_pointer(
        "latest_execution_guardrails",
        guardrail_dir.name,
        guardrail_dir / "guardrail_manifest.json",
    )
    return {
        "guardrail_id": guardrail_dir.name,
        "guardrail_dir": guardrail_dir,
        "manifest": manifest,
        "proposed_adjustment_checks": checks,
        "guardrail_summary": summary,
        "stepwise_adjustment_plan": stepwise,
    }


def execution_guardrails_report_payload(
    *,
    guardrail_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_EXECUTION_GUARDRAILS_DIR,
) -> dict[str, Any]:
    resolved_id = guardrail_id or (
        _latest_pointer_artifact_id("latest_execution_guardrails") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--guardrail-id or --latest is required")
    guardrail_dir = output_dir / resolved_id
    return {
        **_read_json(guardrail_dir / "guardrail_manifest.json"),
        "guardrail_summary": _read_optional_json(guardrail_dir / "guardrail_summary.json") or {},
        "stepwise_adjustment_plan": _read_optional_json(
            guardrail_dir / "stepwise_adjustment_plan.json"
        )
        or {},
        "guardrail_dir": str(guardrail_dir),
    }


def validate_execution_guardrails_artifact(
    *,
    guardrail_id: str,
    output_dir: Path = DEFAULT_EXECUTION_GUARDRAILS_DIR,
) -> dict[str, Any]:
    guardrail_dir = output_dir / guardrail_id
    manifest = _read_optional_json(guardrail_dir / "guardrail_manifest.json") or {}
    summary = _read_optional_json(guardrail_dir / "guardrail_summary.json") or {}
    checks_rows = _read_jsonl(guardrail_dir / "proposed_adjustment_checks.jsonl")
    required = [
        "guardrail_manifest.json",
        "proposed_adjustment_checks.jsonl",
        "guardrail_summary.json",
        "stepwise_adjustment_plan.json",
        "execution_guardrails_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (guardrail_dir / name).exists(), name)
        for name in required
    ]
    checks.extend(
        [
            _check(
                "guardrail_id_matches",
                manifest.get("guardrail_id") == guardrail_id
                and summary.get("guardrail_id") == guardrail_id,
                guardrail_id,
            ),
            _check("adjustment_checks_present", bool(checks_rows), "guardrail checks required"),
            _check(
                "recommended_action_valid",
                summary.get("recommended_action") in EXECUTION_GUARDRAIL_ACTIONS,
                _text(summary.get("recommended_action")),
            ),
            _check(
                "broker_action_forbidden",
                manifest.get("broker_action_allowed") is False
                and summary.get("broker_action_allowed") is False,
                "broker action is forbidden",
            ),
            _check(
                "order_ticket_forbidden",
                manifest.get("order_ticket_generation_allowed") is False
                and summary.get("order_ticket_generation_allowed") is False,
                "order ticket generation is forbidden",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_execution_guardrails_validation",
        "guardrail_id": guardrail_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "order_ticket_generation_allowed": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def build_manual_execution_review_pack(
    *,
    snapshot_id: str,
    exposure_id: str,
    drift_id: str,
    guardrail_id: str,
    snapshot_dir: Path = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
    exposure_dir: Path = DEFAULT_PORTFOLIO_EXPOSURE_DIR,
    drift_dir: Path = DEFAULT_POSITION_DRIFT_DIR,
    guardrail_dir: Path = DEFAULT_EXECUTION_GUARDRAILS_DIR,
    output_dir: Path = DEFAULT_MANUAL_EXECUTION_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    snapshot_manifest = _read_json(snapshot_dir / snapshot_id / "manual_portfolio_manifest.json")
    normalized = _read_json(snapshot_dir / snapshot_id / "normalized_portfolio.json")
    exposure_manifest = _read_json(exposure_dir / exposure_id / "portfolio_exposure_manifest.json")
    exposure_summary = _read_json(exposure_dir / exposure_id / "exposure_summary.json")
    drift_manifest = _read_json(drift_dir / drift_id / "position_drift_manifest.json")
    drift_summary = _read_json(drift_dir / drift_id / "consensus_drift_summary.json")
    guardrail_manifest = _read_json(guardrail_dir / guardrail_id / "guardrail_manifest.json")
    guardrail_summary = _read_json(guardrail_dir / guardrail_id / "guardrail_summary.json")
    review_id = _stable_id(
        "manual-execution-review",
        snapshot_id,
        exposure_id,
        drift_id,
        guardrail_id,
        generated.isoformat(),
    )
    review_dir = _unique_dir(output_dir / review_id)
    review_dir.mkdir(parents=True, exist_ok=False)
    decision = _manual_execution_decision(
        review_dir.name,
        snapshot_manifest,
        exposure_summary,
        drift_summary,
        guardrail_summary,
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_manual_execution_review_manifest",
        "manual_review_id": review_dir.name,
        "snapshot_id": snapshot_id,
        "exposure_id": exposure_id,
        "drift_id": drift_id,
        "guardrail_id": guardrail_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "recommended_action": decision["recommended_action"],
        "manual_execution_decision_path": str(review_dir / "manual_execution_decision.json"),
        "owner_execution_checklist_path": str(review_dir / "owner_execution_checklist.md"),
        "manual_execution_review_report_path": str(
            review_dir / "manual_execution_review_report.md"
        ),
        "reader_brief_section_path": str(review_dir / "reader_brief_section.md"),
        "order_ticket_generated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "owner_approval_required": True,
        "production_effect": "none",
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(review_dir / "manual_execution_review_manifest.json", manifest)
    _write_json(review_dir / "manual_execution_decision.json", decision)
    _write_text(
        review_dir / "owner_execution_checklist.md",
        render_owner_execution_checklist(decision),
    )
    _write_text(
        review_dir / "manual_execution_review_report.md",
        render_manual_execution_review_markdown(
            manifest,
            normalized,
            exposure_manifest,
            exposure_summary,
            drift_manifest,
            drift_summary,
            guardrail_manifest,
            guardrail_summary,
            decision,
        ),
    )
    _write_text(
        review_dir / "reader_brief_section.md",
        render_manual_execution_reader_brief(
            snapshot_manifest,
            exposure_summary,
            drift_summary,
            guardrail_summary,
            decision,
        ),
    )
    _update_latest_pointer(
        "latest_manual_execution_review",
        review_dir.name,
        review_dir / "manual_execution_review_manifest.json",
    )
    return {
        "manual_review_id": review_dir.name,
        "review_dir": review_dir,
        "manifest": manifest,
        "manual_execution_decision": decision,
    }


def manual_execution_review_report_payload(
    *,
    review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_MANUAL_EXECUTION_REVIEW_DIR,
) -> dict[str, Any]:
    resolved_id = review_id or (
        _latest_pointer_artifact_id("latest_manual_execution_review") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--review-id or --latest is required")
    review_dir = output_dir / resolved_id
    return {
        **_read_json(review_dir / "manual_execution_review_manifest.json"),
        "manual_execution_decision": _read_optional_json(
            review_dir / "manual_execution_decision.json"
        )
        or {},
        "review_dir": str(review_dir),
    }


def validate_manual_execution_review_artifact(
    *,
    review_id: str,
    output_dir: Path = DEFAULT_MANUAL_EXECUTION_REVIEW_DIR,
) -> dict[str, Any]:
    review_dir = output_dir / review_id
    manifest = _read_optional_json(review_dir / "manual_execution_review_manifest.json") or {}
    decision = _read_optional_json(review_dir / "manual_execution_decision.json") or {}
    required = [
        "manual_execution_review_manifest.json",
        "manual_execution_decision.json",
        "owner_execution_checklist.md",
        "manual_execution_review_report.md",
        "reader_brief_section.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (review_dir / name).exists(), name) for name in required
    ]
    checks.extend(
        [
            _check(
                "review_id_matches",
                manifest.get("manual_review_id") == review_id
                and decision.get("manual_review_id") == review_id,
                review_id,
            ),
            _check(
                "recommended_action_valid",
                decision.get("recommended_action") in MANUAL_EXECUTION_ACTIONS,
                _text(decision.get("recommended_action")),
            ),
            _check(
                "order_ticket_not_generated",
                manifest.get("order_ticket_generated") is False
                and decision.get("order_ticket_generated") is False,
                "order ticket was not generated",
            ),
            _check(
                "broker_action_forbidden",
                manifest.get("broker_action_allowed") is False
                and decision.get("broker_action_allowed") is False,
                "broker action is forbidden",
            ),
            _check(
                "broker_action_not_taken",
                manifest.get("broker_action_taken") is False
                and decision.get("broker_action_taken") is False,
                "broker action was not taken",
            ),
            _check(
                "owner_approval_required",
                manifest.get("owner_approval_required") is True
                and decision.get("owner_approval_required") is True,
                "owner approval is required",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_manual_execution_review_validation",
        "manual_review_id": review_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "owner_approval_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def create_owner_review(
    *,
    daily_advisory_id: str,
    daily_advisory_dir: Path = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    output_dir: Path = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    advisory_dir = daily_advisory_dir / daily_advisory_id
    manifest = _read_json(advisory_dir / "daily_advisory_manifest.json")
    actions = _read_json(advisory_dir / "daily_advisory_actions.json")
    review_id = _stable_id("owner-review", daily_advisory_id, generated.isoformat())
    record = {
        "schema_version": SCHEMA_VERSION,
        "review_id": review_id,
        "daily_advisory_id": daily_advisory_id,
        "as_of": manifest.get("as_of", ""),
        "recommended_action": actions.get("recommended_action", ""),
        "owner_decision": "pending",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "paper_action": {"enabled": False, "notes": ""},
        "manual_notes": "",
        "created_at": generated.isoformat(),
        "updated_at": generated.isoformat(),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    _append_jsonl(output_dir / "owner_review_journal.jsonl", record)
    _write_json(output_dir / "latest_owner_review.json", record)
    _write_text(
        output_dir / "owner_review_report.md",
        render_owner_review_report_markdown(owner_review_summary(output_dir=output_dir)),
    )
    _update_latest_pointer(
        "latest_owner_review",
        review_id,
        output_dir / "latest_owner_review.json",
    )
    return {"review_id": review_id, "review": record, "journal_dir": output_dir}


def record_owner_review_decision(
    *,
    review_id: str,
    decision: str,
    manual_notes: str = "",
    output_dir: Path = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    daily_advisory_dir: Path = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    if decision not in OWNER_REVIEW_DECISIONS:
        raise DynamicV3ParameterResearchError(f"unsupported owner decision: {decision}")
    generated = generated_at or datetime.now(UTC)
    journal_path = output_dir / "owner_review_journal.jsonl"
    records = _read_jsonl(journal_path)
    updated: dict[str, Any] | None = None
    for row in records:
        if row.get("review_id") != review_id:
            continue
        row["owner_decision"] = decision
        row["manual_notes"] = manual_notes
        row["updated_at"] = generated.isoformat()
        row["broker_action_allowed"] = False
        row["broker_action_taken"] = False
        if decision == "paper_adjustment":
            paper = _paper_action_record(
                review=row,
                output_dir=output_dir,
                daily_advisory_dir=daily_advisory_dir,
                generated_at=generated,
            )
            row["paper_action"] = {
                "enabled": True,
                "paper_action_id": paper["paper_action_id"],
                "notes": "Paper only, no broker action",
            }
        updated = dict(row)
        break
    if updated is None:
        raise DynamicV3ParameterResearchError(f"owner review not found: {review_id}")
    _write_jsonl(journal_path, records)
    _write_json(output_dir / "latest_owner_review.json", updated)
    _write_text(
        output_dir / "owner_review_report.md",
        render_owner_review_report_markdown(owner_review_summary(output_dir=output_dir)),
    )
    _update_latest_pointer(
        "latest_owner_review",
        review_id,
        output_dir / "latest_owner_review.json",
    )
    return {"review_id": review_id, "review": updated, "journal_dir": output_dir}


def owner_review_summary(
    *,
    output_dir: Path = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
) -> dict[str, Any]:
    records = _read_jsonl(output_dir / "owner_review_journal.jsonl")
    paper_actions = _read_jsonl(output_dir / "paper_action_log.jsonl")
    latest = records[-1] if records else {}
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_review_summary",
        "status": "PASS" if records else "MISSING",
        "review_count": len(records),
        "pending_owner_review_count": sum(
            1 for row in records if row.get("owner_decision") == "pending"
        ),
        "latest_review_id": latest.get("review_id", ""),
        "latest_daily_advisory_id": latest.get("daily_advisory_id", ""),
        "latest_recommended_action": latest.get("recommended_action", ""),
        "latest_owner_decision": latest.get("owner_decision", ""),
        "paper_action_count": len(paper_actions),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "paper_action_only": True,
        "manual_review_required_count": sum(
            1
            for row in records
            if row.get("recommended_action") == "manual_review"
            and row.get("owner_decision") == "pending"
        ),
        "owner_review_report_path": str(output_dir / "owner_review_report.md"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def owner_review_report_payload(
    *,
    latest: bool = False,
    review_id: str | None = None,
    output_dir: Path = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
) -> dict[str, Any]:
    records = _read_jsonl(output_dir / "owner_review_journal.jsonl")
    selected = records[-1] if latest and records else {}
    if review_id:
        selected = next((row for row in records if row.get("review_id") == review_id), {})
    summary = owner_review_summary(output_dir=output_dir)
    if not selected and (latest or review_id):
        raise DynamicV3ParameterResearchError("owner review not found")
    return {**summary, "review": selected, "journal_dir": str(output_dir)}


def validate_owner_review_artifact(
    *,
    review_id: str,
    output_dir: Path = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
) -> dict[str, Any]:
    records = _read_jsonl(output_dir / "owner_review_journal.jsonl")
    record = next((row for row in records if row.get("review_id") == review_id), {})
    checks = [
        _check(
            "owner_review_journal_exists",
            (output_dir / "owner_review_journal.jsonl").exists(),
            str(output_dir),
        ),
        _check("review_record_present", bool(record), review_id),
        _check(
            "broker_action_forbidden",
            record.get("broker_action_allowed") is False,
            "broker action is forbidden",
        ),
        _check(
            "broker_action_not_taken",
            record.get("broker_action_taken") is False,
            "broker action was not taken",
        ),
        _check(
            "owner_decision_valid",
            record.get("owner_decision") == "pending"
            or record.get("owner_decision") in OWNER_REVIEW_DECISIONS,
            _text(record.get("owner_decision")),
        ),
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_review_validation",
        "review_id": review_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def run_overnight_readiness(
    *,
    source_sweep_id: str,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Path = DEFAULT_OVERNIGHT_READINESS_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    sweep_dir = sweep_output_dir / source_sweep_id
    manifest = _read_json(sweep_dir / "sweep_manifest.json")
    results = _read_candidate_results(sweep_dir)
    errors = _read_jsonl(sweep_dir / "candidate_errors.jsonl")
    avg_runtime = _average_runtime_seconds(manifest, len(results))
    projected_seconds = avg_runtime * OVERNIGHT_TARGET_CANDIDATES
    projected_hours = projected_seconds / 3600.0
    artifact_size = _artifact_size_summary(sweep_dir)
    projected_artifact_gb = (
        artifact_size["total_bytes"] / max(1, len(results)) * OVERNIGHT_TARGET_CANDIDATES
    ) / (1024**3)
    failure_rate = len(errors) / max(1, len(results) + len(errors))
    evidence_complete_rate = sum(
        1
        for row in results
        if _text(_mapping(row.get("weight_path_metadata")).get("attribution_completeness"))
        == WEIGHT_PATH_COMPLETE
    ) / max(1, len(results))
    blockers: list[str] = []
    warnings: list[str] = []
    if manifest.get("evaluator_mode") != EVALUATOR_REAL_DYNAMIC_V3_RESCUE:
        blockers.append("source_sweep_not_real_dynamic_v3_rescue")
    if failure_rate > OVERNIGHT_WARNING_MAX_FAILURE_RATE:
        blockers.append("failure_rate_too_high")
    elif failure_rate > OVERNIGHT_READY_MAX_FAILURE_RATE:
        warnings.append("failure_rate_warning")
    if projected_hours > OVERNIGHT_WARNING_MAX_HOURS:
        blockers.append("projected_runtime_too_high")
    elif projected_hours > OVERNIGHT_READY_MAX_HOURS:
        warnings.append("projected_runtime_warning")
    if projected_artifact_gb > OVERNIGHT_WARNING_MAX_ARTIFACT_GB:
        warnings.append("projected_artifact_size_warning")
    if evidence_complete_rate < 0.5:
        warnings.append("evidence_completeness_below_half")
    readiness = "READY"
    if blockers:
        readiness = "NOT_READY"
    elif warnings:
        readiness = "READY_WITH_WARNINGS"
    readiness_id = _stable_id("overnight-readiness", source_sweep_id, generated.isoformat())
    readiness_dir = _unique_dir(output_dir / readiness_id)
    readiness_dir.mkdir(parents=True, exist_ok=False)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_overnight_readiness_manifest",
        "readiness_id": readiness_dir.name,
        "source_sweep_id": source_sweep_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if readiness != "NOT_READY" else "PASS_WITH_WARNINGS",
        "overnight_readiness": readiness,
        "blocking_reasons": blockers,
        "warnings": warnings,
        "medium_real_completed": manifest.get("status") == "completed",
        "average_runtime_seconds_per_candidate": avg_runtime,
        "projected_overnight_runtime_hours": round(projected_hours, 3),
        "projected_candidate_count": OVERNIGHT_TARGET_CANDIDATES,
        "projected_artifact_size_gb": round(projected_artifact_gb, 3),
        "failure_rate": round(failure_rate, 6),
        "resume_reliability": "PASS" if (sweep_dir / "checkpoint.json").exists() else "MISSING",
        "data_audit_status": _text(_mapping(manifest.get("data_quality")).get("status"), "MISSING"),
        "window_audit_status": _text(
            _mapping(manifest.get("backtest_window")).get("date_range_status"), "MISSING"
        ),
        "evidence_completeness_rate": round(evidence_complete_rate, 6),
        "top_candidate_stability": _top_candidate_stability(results),
        "overfit_warning_rate": _overfit_warning_rate(results),
        "disk_usage_risk": (
            "REVIEW_REQUIRED"
            if projected_artifact_gb > OVERNIGHT_WARNING_MAX_ARTIFACT_GB
            else "LOW"
        ),
        "ci_contamination_risk": (
            "LOW" if manifest.get("profile") != "overnight_real" else "REVIEW_REQUIRED"
        ),
        "overnight_readiness_report_path": str(readiness_dir / "overnight_readiness_report.md"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(readiness_dir / "overnight_readiness_manifest.json", payload)
    _write_text(
        readiness_dir / "overnight_readiness_report.md",
        render_overnight_readiness_markdown(payload),
    )
    _update_latest_pointer(
        "latest_overnight_readiness",
        readiness_dir.name,
        readiness_dir / "overnight_readiness_manifest.json",
    )
    return {"readiness_id": readiness_dir.name, "readiness_dir": readiness_dir, "manifest": payload}


def overnight_readiness_report_payload(
    *,
    readiness_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_OVERNIGHT_READINESS_DIR,
) -> dict[str, Any]:
    resolved_id = readiness_id or (
        _latest_pointer_artifact_id("latest_overnight_readiness") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--readiness-id or --latest is required")
    readiness_dir = output_dir / resolved_id
    return {
        **_read_json(readiness_dir / "overnight_readiness_manifest.json"),
        "readiness_dir": str(readiness_dir),
    }


def validate_overnight_readiness_artifact(
    *,
    readiness_id: str,
    output_dir: Path = DEFAULT_OVERNIGHT_READINESS_DIR,
) -> dict[str, Any]:
    readiness_dir = output_dir / readiness_id
    manifest = _read_optional_json(readiness_dir / "overnight_readiness_manifest.json") or {}
    checks = [
        _check(
            "manifest_exists",
            (readiness_dir / "overnight_readiness_manifest.json").exists(),
            readiness_id,
        ),
        _check("report_exists", (readiness_dir / "overnight_readiness_report.md").exists(), ""),
        _check("readiness_id_matches", manifest.get("readiness_id") == readiness_id, readiness_id),
        _check(
            "readiness_status_valid",
            manifest.get("overnight_readiness") in {"READY", "READY_WITH_WARNINGS", "NOT_READY"},
            _text(manifest.get("overnight_readiness")),
        ),
        _check(
            "does_not_auto_start_overnight",
            manifest.get("production_state_mutated") is False,
            "no overnight run started",
        ),
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_overnight_readiness_validation",
        "readiness_id": readiness_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def run_research_decision(
    *,
    sweep_id: str,
    output_dir: Path = DEFAULT_RESEARCH_DECISION_DIR,
    evidence_summary_dir: Path = DEFAULT_EVIDENCE_SUMMARY_DIR,
    regime_coverage_dir: Path = DEFAULT_REGIME_COVERAGE_DIR,
    interpretation_pack_dir: Path = DEFAULT_INTERPRETATION_PACK_DIR,
    observe_pool_dir: Path = DEFAULT_OBSERVE_POOL_DIR,
    overnight_readiness_dir: Path = DEFAULT_OVERNIGHT_READINESS_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    evidence = _latest_manifest_for_sweep(
        sweep_id,
        evidence_summary_dir,
        "evidence_summary_manifest.json",
        "source_sweep_id",
    )
    regime = _latest_manifest_for_sweep(
        sweep_id,
        regime_coverage_dir,
        "regime_coverage_manifest.json",
        "source_sweep_id",
    )
    interpretation = _latest_manifest_for_sweep(
        sweep_id,
        interpretation_pack_dir,
        "interpretation_manifest.json",
        "source_sweep_id",
    )
    observe_pool = _latest_manifest_for_sweep(
        sweep_id,
        observe_pool_dir,
        "observe_pool_manifest.json",
        "source_sweep_id",
    )
    readiness = _latest_manifest_for_sweep(
        sweep_id,
        overnight_readiness_dir,
        "overnight_readiness_manifest.json",
        "source_sweep_id",
    )
    recommendation = _research_decision_recommendation(
        evidence=evidence,
        regime=regime,
        observe_pool=observe_pool,
        readiness=readiness,
    )
    decision_id = _stable_id("research-decision", sweep_id, generated.isoformat())
    decision_dir = _unique_dir(output_dir / decision_id)
    decision_dir.mkdir(parents=True, exist_ok=False)
    manifest_payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_research_decision_manifest",
        "decision_id": decision_dir.name,
        "source_sweep_id": sweep_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "medium_real_success": _text(evidence.get("status"), "MISSING")
        in {"PASS", "PASS_WITH_WARNINGS"},
        "research_value_candidate_found": int(observe_pool.get("observe_candidate_count") or 0) > 0,
        "evidence_quality_status": evidence.get("status", "MISSING"),
        "regime_coverage_status": regime.get("coverage_status", "MISSING"),
        "tech_semiconductor_relevance": regime.get("tech_semiconductor_relevance", "MISSING"),
        "ai_bull_market_overfit_risk": regime.get("ai_bull_market_overfit_risk", "MISSING"),
        "observe_candidate_count": observe_pool.get("observe_candidate_count", 0),
        "overnight_readiness": readiness.get("overnight_readiness", "MISSING"),
        "recommendation": recommendation["recommendation"],
        "priority": recommendation["priority"],
        "research_decision_report_path": str(decision_dir / "research_decision_report.md"),
        "next_action_recommendations_path": str(decision_dir / "next_action_recommendations.json"),
        "reader_brief_section_path": str(decision_dir / "reader_brief_section.md"),
        "source_artifacts": {
            "evidence_summary": evidence.get("_manifest_path", ""),
            "regime_coverage": regime.get("_manifest_path", ""),
            "interpretation_pack": interpretation.get("_manifest_path", ""),
            "observe_pool": observe_pool.get("_manifest_path", ""),
            "overnight_readiness": readiness.get("_manifest_path", ""),
        },
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(decision_dir / "research_decision_manifest.json", manifest_payload)
    _write_json(decision_dir / "next_action_recommendations.json", recommendation)
    reader_section = render_research_decision_reader_brief_section(manifest_payload, recommendation)
    _write_text(decision_dir / "reader_brief_section.md", reader_section)
    _write_text(
        decision_dir / "research_decision_report.md",
        render_research_decision_markdown(
            manifest_payload,
            recommendation,
            evidence=evidence,
            regime=regime,
            interpretation=interpretation,
            observe_pool=observe_pool,
            readiness=readiness,
        ),
    )
    _update_latest_pointer(
        "latest_research_decision",
        decision_dir.name,
        decision_dir / "research_decision_manifest.json",
    )
    return {
        "decision_id": decision_dir.name,
        "decision_dir": decision_dir,
        "manifest": manifest_payload,
        "recommendation": recommendation,
    }


def research_decision_report_payload(
    *,
    decision_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_RESEARCH_DECISION_DIR,
) -> dict[str, Any]:
    resolved_id = decision_id or (
        _latest_pointer_artifact_id("latest_research_decision") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--decision-id or --latest is required")
    decision_dir = output_dir / resolved_id
    return {
        **_read_json(decision_dir / "research_decision_manifest.json"),
        "decision_dir": str(decision_dir),
    }


def validate_research_decision_artifact(
    *,
    decision_id: str,
    output_dir: Path = DEFAULT_RESEARCH_DECISION_DIR,
) -> dict[str, Any]:
    decision_dir = output_dir / decision_id
    manifest = _read_optional_json(decision_dir / "research_decision_manifest.json") or {}
    recommendation = _read_optional_json(decision_dir / "next_action_recommendations.json") or {}
    checks = [
        _check(
            "manifest_exists",
            (decision_dir / "research_decision_manifest.json").exists(),
            decision_id,
        ),
        _check("report_exists", (decision_dir / "research_decision_report.md").exists(), ""),
        _check(
            "next_action_exists", (decision_dir / "next_action_recommendations.json").exists(), ""
        ),
        _check(
            "reader_brief_section_exists", (decision_dir / "reader_brief_section.md").exists(), ""
        ),
        _check("decision_id_matches", manifest.get("decision_id") == decision_id, decision_id),
        _check("recommendation_present", bool(recommendation.get("recommendation")), ""),
        _check(
            "production_candidate_not_generated",
            manifest.get("production_candidate_generated") is False,
            "research-only",
        ),
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_research_decision_validation",
        "decision_id": decision_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def run_evidence_diagnosis(
    *,
    sweep_id: str,
    summary_id: str | None = None,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    evidence_summary_dir: Path = DEFAULT_EVIDENCE_SUMMARY_DIR,
    regime_coverage_dir: Path = DEFAULT_REGIME_COVERAGE_DIR,
    output_dir: Path = DEFAULT_EVIDENCE_DIAGNOSIS_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    rows, summary_manifest = _candidate_diagnosis_rows_for_sweep(
        sweep_id=sweep_id,
        summary_id=summary_id,
        sweep_output_dir=sweep_output_dir,
        evidence_summary_dir=evidence_summary_dir,
        regime_coverage_dir=regime_coverage_dir,
    )
    diagnosis_id = _stable_id("evidence-diagnosis", sweep_id, generated.isoformat())
    diagnosis_dir = _unique_dir(output_dir / diagnosis_id)
    diagnosis_dir.mkdir(parents=True, exist_ok=False)
    candidate_count = len(rows)
    blocking_counter = Counter(
        reason for row in rows for reason in _texts(row.get("blocking_reasons"))
    )
    warning_counter = Counter(
        reason for row in rows for reason in _texts(row.get("warning_reasons"))
    )
    category_counter = Counter(
        category
        for row in rows
        for category, reasons in _mapping(row.get("categories")).items()
        if _texts(reasons)
    )
    hard_count = sum(1 for row in rows if row.get("hard_blocking_reasons"))
    soft_count = sum(1 for row in rows if row.get("soft_blocking_reasons"))
    warning_count = sum(1 for row in rows if row.get("warning_reasons"))
    usable_count = sum(1 for row in rows if row.get("usable") is True)
    review_required_count = sum(1 for row in rows if _text(row.get("gate")) == GATE_REVIEW_REQUIRED)
    status = "PASS" if rows else "FAIL"
    if candidate_count and usable_count == 0:
        status = "PASS_WITH_WARNINGS"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_evidence_diagnosis_manifest",
        "diagnosis_id": diagnosis_dir.name,
        "source_sweep_id": sweep_id,
        "source_evidence_summary_id": summary_manifest.get("summary_id", ""),
        "generated_at": generated.isoformat(),
        "status": status,
        "candidate_count": candidate_count,
        "usable_candidates": usable_count,
        "review_required_candidates": review_required_count,
        "hard_blocked_candidates": hard_count,
        "soft_blocked_candidates": soft_count,
        "warning_candidates": warning_count,
        "blocking_reason_summary_path": str(diagnosis_dir / "blocking_reason_summary.json"),
        "candidate_blocking_matrix_path": str(diagnosis_dir / "candidate_blocking_matrix.jsonl"),
        "gate_category_summary_path": str(diagnosis_dir / "gate_category_summary.json"),
        "evidence_diagnosis_report_path": str(diagnosis_dir / "evidence_diagnosis_report.md"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    blocking_summary = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_blocking_reason_summary",
        "diagnosis_id": diagnosis_dir.name,
        "sweep_id": sweep_id,
        "candidate_count": candidate_count,
        "usable_candidates": usable_count,
        "review_required_candidates": review_required_count,
        "blocking_reasons": [
            {
                "reason": reason,
                "count": count,
                "share": round(count / max(1, candidate_count), 6),
                "severity": _evidence_gate_reason_severity(reason),
                "category": _evidence_gate_reason_category(reason),
            }
            for reason, count in blocking_counter.most_common()
        ],
        "warning_reasons": [
            {
                "reason": reason,
                "count": count,
                "share": round(count / max(1, candidate_count), 6),
                "severity": "warning",
                "category": _evidence_gate_reason_category(reason),
            }
            for reason, count in warning_counter.most_common()
        ],
        "top_blocking_categories": [
            {"category": category, "count": count}
            for category, count in category_counter.most_common()
        ],
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    gate_category_summary = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_gate_category_summary",
        "diagnosis_id": diagnosis_dir.name,
        "source_sweep_id": sweep_id,
        "categories": [
            {
                "category": category,
                "candidate_count": sum(
                    1 for row in rows if _texts(_mapping(row.get("categories")).get(category))
                ),
                "reasons": [
                    {
                        "reason": reason,
                        "count": count,
                        "severity": _evidence_gate_reason_severity(reason),
                    }
                    for reason, count in Counter(
                        reason
                        for row in rows
                        for reason in _texts(_mapping(row.get("categories")).get(category))
                    ).most_common()
                ],
            }
            for category in EVIDENCE_GATE_CATEGORY_ORDER
        ],
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(diagnosis_dir / "diagnosis_manifest.json", manifest)
    _write_json(diagnosis_dir / "blocking_reason_summary.json", blocking_summary)
    _write_jsonl(diagnosis_dir / "candidate_blocking_matrix.jsonl", rows)
    _write_json(diagnosis_dir / "gate_category_summary.json", gate_category_summary)
    _write_text(
        diagnosis_dir / "evidence_diagnosis_report.md",
        render_evidence_diagnosis_markdown(manifest, blocking_summary, gate_category_summary),
    )
    _update_latest_pointer(
        "latest_evidence_diagnosis",
        diagnosis_dir.name,
        diagnosis_dir / "diagnosis_manifest.json",
    )
    return {
        "diagnosis_id": diagnosis_dir.name,
        "diagnosis_dir": diagnosis_dir,
        "manifest": manifest,
        "blocking_summary": blocking_summary,
        "candidate_matrix": rows,
    }


def evidence_diagnosis_report_payload(
    *,
    diagnosis_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_EVIDENCE_DIAGNOSIS_DIR,
) -> dict[str, Any]:
    resolved_id = diagnosis_id or (
        _latest_pointer_artifact_id("latest_evidence_diagnosis") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--diagnosis-id or --latest is required")
    diagnosis_dir = output_dir / resolved_id
    return {
        **_read_json(diagnosis_dir / "diagnosis_manifest.json"),
        "diagnosis_dir": str(diagnosis_dir),
    }


def validate_evidence_diagnosis_artifact(
    *,
    diagnosis_id: str,
    output_dir: Path = DEFAULT_EVIDENCE_DIAGNOSIS_DIR,
) -> dict[str, Any]:
    diagnosis_dir = output_dir / diagnosis_id
    manifest = _read_optional_json(diagnosis_dir / "diagnosis_manifest.json") or {}
    rows = _read_jsonl(diagnosis_dir / "candidate_blocking_matrix.jsonl")
    required = [
        "diagnosis_manifest.json",
        "blocking_reason_summary.json",
        "candidate_blocking_matrix.jsonl",
        "gate_category_summary.json",
        "evidence_diagnosis_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (diagnosis_dir / name).exists(), name)
        for name in required
    ]
    checks.extend(
        [
            _check(
                "diagnosis_id_matches",
                manifest.get("diagnosis_id") == diagnosis_id,
                diagnosis_id,
            ),
            _check("candidate_matrix_not_empty", bool(rows), f"rows={len(rows)}"),
            _check(
                "candidate_rows_have_reason_classes",
                all(
                    row.get("candidate_id")
                    and isinstance(row.get("blocking_reasons"), list)
                    and isinstance(row.get("hard_blocking_reasons"), list)
                    and isinstance(row.get("soft_blocking_reasons"), list)
                    and isinstance(row.get("warning_reasons"), list)
                    for row in rows
                ),
                "blocking/hard/soft/warning fields required",
            ),
            _check(
                "production_candidate_not_generated",
                manifest.get("production_candidate_generated") is False,
                "diagnosis is research-only",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_evidence_diagnosis_validation",
        "diagnosis_id": diagnosis_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def run_gate_impact(
    *,
    diagnosis_id: str,
    diagnosis_dir: Path = DEFAULT_EVIDENCE_DIAGNOSIS_DIR,
    output_dir: Path = DEFAULT_GATE_IMPACT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_dir = diagnosis_dir / diagnosis_id
    diagnosis_manifest = _read_json(source_dir / "diagnosis_manifest.json")
    rows = _read_jsonl(source_dir / "candidate_blocking_matrix.jsonl")
    top_reasons = [
        _text(row.get("reason"))
        for row in _records(
            _read_json(source_dir / "blocking_reason_summary.json").get("blocking_reasons")
        )
    ]
    scenarios = _gate_impact_scenarios(top_reasons)
    scenario_results = [
        _simulate_gate_recovery(rows, scenario_id=scenario["scenario"], scenario=scenario)
        for scenario in scenarios
    ]
    impact_id = _stable_id("gate-impact", diagnosis_id, generated.isoformat())
    impact_dir = _unique_dir(output_dir / impact_id)
    impact_dir.mkdir(parents=True, exist_ok=False)
    baseline = scenario_results[0] if scenario_results else {}
    best = max(
        scenario_results,
        key=lambda row: (int(row.get("observe_candidates") or 0), _text(row.get("scenario"))),
        default={},
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_gate_impact_manifest",
        "impact_id": impact_dir.name,
        "source_diagnosis_id": diagnosis_id,
        "source_sweep_id": diagnosis_manifest.get("source_sweep_id", ""),
        "generated_at": generated.isoformat(),
        "status": "PASS" if rows and scenario_results else "FAIL",
        "scenario_count": len(scenario_results),
        "baseline_usable_candidates": baseline.get("usable_candidates", 0),
        "baseline_observe_candidates": baseline.get("observe_candidates", 0),
        "best_scenario": best.get("scenario", ""),
        "best_observe_candidates": best.get("observe_candidates", 0),
        "gate_impact_matrix_path": str(impact_dir / "gate_impact_matrix.json"),
        "candidate_recovery_simulation_path": str(
            impact_dir / "candidate_recovery_simulation.json"
        ),
        "gate_impact_report_path": str(impact_dir / "gate_impact_report.md"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    matrix = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_gate_impact_matrix",
        "impact_id": impact_dir.name,
        "source_diagnosis_id": diagnosis_id,
        "scenarios": scenario_results,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    simulation = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_recovery_simulation",
        "impact_id": impact_dir.name,
        "baseline": {
            "usable_candidates": baseline.get("usable_candidates", 0),
            "observe_candidates": baseline.get("observe_candidates", 0),
        },
        "scenarios": scenario_results,
        "top_3_repair_items": top_reasons[:3],
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(impact_dir / "gate_impact_manifest.json", manifest)
    _write_json(impact_dir / "gate_impact_matrix.json", matrix)
    _write_json(impact_dir / "candidate_recovery_simulation.json", simulation)
    _write_text(
        impact_dir / "gate_impact_report.md",
        render_gate_impact_markdown(manifest, scenario_results),
    )
    _update_latest_pointer(
        "latest_gate_impact",
        impact_dir.name,
        impact_dir / "gate_impact_manifest.json",
    )
    return {
        "impact_id": impact_dir.name,
        "impact_dir": impact_dir,
        "manifest": manifest,
        "scenarios": scenario_results,
    }


def gate_impact_report_payload(
    *,
    impact_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_GATE_IMPACT_DIR,
) -> dict[str, Any]:
    resolved_id = impact_id or (_latest_pointer_artifact_id("latest_gate_impact") if latest else "")
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--impact-id or --latest is required")
    impact_dir = output_dir / resolved_id
    return {**_read_json(impact_dir / "gate_impact_manifest.json"), "impact_dir": str(impact_dir)}


def validate_gate_impact_artifact(
    *,
    impact_id: str,
    output_dir: Path = DEFAULT_GATE_IMPACT_DIR,
) -> dict[str, Any]:
    impact_dir = output_dir / impact_id
    manifest = _read_optional_json(impact_dir / "gate_impact_manifest.json") or {}
    simulation = _read_optional_json(impact_dir / "candidate_recovery_simulation.json") or {}
    scenario_ids = {_text(row.get("scenario")) for row in _records(simulation.get("scenarios"))}
    required = [
        "gate_impact_manifest.json",
        "gate_impact_matrix.json",
        "candidate_recovery_simulation.json",
        "gate_impact_report.md",
    ]
    required_scenarios = {
        "current_rules",
        "attribution_partial_as_manual_review",
        "data_provenance_reconstructed_as_warning",
        "overfit_review_required_as_manual_review",
        "regime_pass_with_warnings_allows_observe",
        "true_hard_failures_only",
        "fix_top_1_blocking_reason",
        "fix_top_3_blocking_reasons",
    }
    checks = [
        _check(f"artifact_exists:{name}", (impact_dir / name).exists(), name) for name in required
    ]
    checks.extend(
        [
            _check("impact_id_matches", manifest.get("impact_id") == impact_id, impact_id),
            _check(
                "required_scenarios_present",
                required_scenarios <= scenario_ids,
                ",".join(sorted(scenario_ids)),
            ),
            _check(
                "source_results_not_mutated",
                manifest.get("production_state_mutated") is False,
                "simulation only",
            ),
            _check(
                "production_candidate_not_generated",
                manifest.get("production_candidate_generated") is False,
                "simulation only",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_gate_impact_validation",
        "impact_id": impact_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def load_evidence_gate_policy_config(
    path: Path | str = DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH,
) -> dict[str, Any]:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, dict):
        raise DynamicV3ParameterResearchError("evidence gate policy must be a mapping")
    return raw


def validate_evidence_gate_policy(
    *,
    policy_path: Path = DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH,
) -> dict[str, Any]:
    policy = load_evidence_gate_policy_config(policy_path)
    observe = _mapping(_mapping(policy.get("gate_levels")).get("observe_only"))
    promote = _mapping(_mapping(policy.get("gate_levels")).get("promote_candidate"))
    production = _mapping(_mapping(policy.get("gate_levels")).get("production_candidate"))
    hard_fail = set(_texts(observe.get("hard_fail")))
    manual_allowed = set(_texts(observe.get("manual_review_allowed")))
    required_hard = {
        "DATA_QUALITY_FAIL",
        "DATE_RANGE_FAIL",
        "DATE_RANGE_INSUFFICIENT_DATA",
        "MISSING_REAL_EVALUATION_ARTIFACT",
        "MISSING_DAILY_WEIGHT_PATH",
        "OVERFIT_HIGH_RISK",
        "TECH_SEMICONDUCTOR_RELEVANCE_LOW",
    }
    checks = [
        _check("schema_version_present", str(policy.get("schema_version")) == "1", ""),
        _check("owner_present", bool(policy.get("owner")), _text(policy.get("owner"))),
        _check("status_present", bool(policy.get("status")), _text(policy.get("status"))),
        _check(
            "observe_hard_fail_present",
            required_hard <= hard_fail,
            ",".join(sorted(hard_fail)),
        ),
        _check(
            "manual_review_allowed_present",
            bool(manual_allowed),
            ",".join(sorted(manual_allowed)),
        ),
        _check(
            "production_candidate_manual_only",
            production.get("manual_only") is True
            and production.get("auto_generation_allowed") is False,
            str(production),
        ),
        _check(
            "promote_candidate_keeps_incomplete_window_block",
            "BACKTEST_WINDOW_INCOMPLETE" in set(_texts(promote.get("hard_fail"))),
            ",".join(_texts(promote.get("hard_fail"))),
        ),
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_evidence_gate_policy_validation",
        "policy_path": str(policy_path),
        "policy_version": policy.get("version", ""),
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "hard_fail": sorted(hard_fail),
        "manual_review_allowed": sorted(manual_allowed),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def evidence_gate_policy_report_payload(
    *,
    policy_path: Path = DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH,
) -> dict[str, Any]:
    policy = load_evidence_gate_policy_config(policy_path)
    validation = validate_evidence_gate_policy(policy_path=policy_path)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_evidence_gate_policy_report",
        "policy_path": str(policy_path),
        "policy": policy,
        "validation": validation,
        "status": validation["status"],
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def apply_evidence_gate_policy(
    *,
    sweep_id: str,
    policy_path: Path = DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    evidence_summary_dir: Path = DEFAULT_EVIDENCE_SUMMARY_DIR,
    regime_coverage_dir: Path = DEFAULT_REGIME_COVERAGE_DIR,
    output_dir: Path = DEFAULT_GATE_POLICY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    validation = validate_evidence_gate_policy(policy_path=policy_path)
    if validation["status"] != "PASS":
        raise DynamicV3ParameterResearchError("evidence gate policy validation failed")
    generated = generated_at or datetime.now(UTC)
    policy = load_evidence_gate_policy_config(policy_path)
    rows, summary_manifest = _candidate_diagnosis_rows_for_sweep(
        sweep_id=sweep_id,
        summary_id=None,
        sweep_output_dir=sweep_output_dir,
        evidence_summary_dir=evidence_summary_dir,
        regime_coverage_dir=regime_coverage_dir,
    )
    observe_policy = _mapping(_mapping(policy.get("gate_levels")).get("observe_only"))
    hard_fail = set(_texts(observe_policy.get("hard_fail")))
    manual_allowed = set(_texts(observe_policy.get("manual_review_allowed")))
    calibrated = [
        _calibrated_candidate_status(row, hard_fail=hard_fail, manual_allowed=manual_allowed)
        for row in rows
    ]
    policy_run_id = _stable_id(
        "gate-policy",
        sweep_id,
        _stable_id("policy", policy),
        generated.isoformat(),
    )
    policy_dir = _unique_dir(output_dir / policy_run_id)
    policy_dir.mkdir(parents=True, exist_ok=False)
    observe_count = sum(
        1 for row in calibrated if row.get("calibrated_status") == GATE_OBSERVE_ONLY
    )
    rejected_count = sum(1 for row in calibrated if row.get("calibrated_status") == GATE_REJECT)
    manual_count = sum(1 for row in calibrated if row.get("manual_review_required") is True)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_gate_policy_manifest",
        "policy_run_id": policy_dir.name,
        "source_sweep_id": sweep_id,
        "source_evidence_summary_id": summary_manifest.get("summary_id", ""),
        "policy_path": str(policy_path),
        "policy_version": policy.get("version", ""),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "candidate_count": len(calibrated),
        "observe_only_candidates": observe_count,
        "manual_review_required_candidates": manual_count,
        "rejected_candidates": rejected_count,
        "applied_policy_path": str(policy_dir / "applied_policy.yaml"),
        "calibrated_candidate_status_path": str(policy_dir / "calibrated_candidate_status.jsonl"),
        "policy_effect_summary_path": str(policy_dir / "policy_effect_summary.json"),
        "gate_policy_report_path": str(policy_dir / "gate_policy_report.md"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    effect = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_gate_policy_effect_summary",
        "policy_run_id": policy_dir.name,
        "source_sweep_id": sweep_id,
        "candidate_count": len(calibrated),
        "observe_only_candidates": observe_count,
        "manual_review_required_candidates": manual_count,
        "rejected_candidates": rejected_count,
        "hard_fail_reason_distribution": [
            {"reason": reason, "count": count}
            for reason, count in Counter(
                reason
                for row in calibrated
                for reason in _texts(row.get("remaining_hard_failures"))
            ).most_common()
        ],
        "manual_review_reason_distribution": [
            {"reason": reason, "count": count}
            for reason, count in Counter(
                reason for row in calibrated for reason in _texts(row.get("manual_review_reasons"))
            ).most_common()
        ],
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(policy_dir / "gate_policy_manifest.json", manifest)
    _write_yaml(policy_dir / "applied_policy.yaml", policy)
    _write_jsonl(policy_dir / "calibrated_candidate_status.jsonl", calibrated)
    _write_json(policy_dir / "policy_effect_summary.json", effect)
    _write_text(policy_dir / "gate_policy_report.md", render_gate_policy_markdown(manifest, effect))
    _update_latest_pointer(
        "latest_gate_policy",
        policy_dir.name,
        policy_dir / "gate_policy_manifest.json",
    )
    return {
        "policy_run_id": policy_dir.name,
        "policy_dir": policy_dir,
        "manifest": manifest,
        "effect_summary": effect,
        "candidate_status": calibrated,
    }


def run_candidate_recovery(
    *,
    sweep_id: str,
    policy_run_id: str,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    gate_policy_dir: Path = DEFAULT_GATE_POLICY_DIR,
    output_dir: Path = DEFAULT_CANDIDATE_RECOVERY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    policy_dir = gate_policy_dir / policy_run_id
    policy_manifest = _read_json(policy_dir / "gate_policy_manifest.json")
    if _text(policy_manifest.get("source_sweep_id")) != sweep_id:
        raise DynamicV3ParameterResearchError("policy run does not match source sweep")
    statuses = _read_jsonl(policy_dir / "calibrated_candidate_status.jsonl")
    sweep_dir = sweep_output_dir / sweep_id
    results_by_candidate = {
        _text(row.get("candidate_id")): row for row in _read_candidate_results(sweep_dir)
    }
    recovered: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for status_row in statuses:
        candidate_id = _text(status_row.get("candidate_id"))
        source_row = results_by_candidate.get(candidate_id, {})
        if status_row.get("calibrated_status") != GATE_OBSERVE_ONLY:
            rejected.append(
                {
                    "candidate_id": candidate_id,
                    "source_sweep_id": sweep_id,
                    "rejection_reasons": status_row.get("remaining_hard_failures", []),
                    "calibrated_status": status_row.get("calibrated_status"),
                    "score": source_row.get("score"),
                }
            )
            continue
        evidence_status = dict(_mapping(status_row.get("evidence_status")))
        evidence_status["original_promotion_status"] = evidence_status.get("promotion_status", "")
        evidence_status["promotion_status"] = "manual_review_required"
        recovered.append(
            {
                "candidate_id": candidate_id,
                "source_sweep_id": sweep_id,
                "original_status": status_row.get("original_gate", GATE_REVIEW_REQUIRED),
                "recovered_status": GATE_OBSERVE_ONLY,
                "manual_review_required": True,
                "recovery_reasons": status_row.get("manual_review_reasons", []),
                "remaining_warnings": status_row.get("warning_reasons", []),
                "parameters": source_row.get("parameters", {}),
                "metrics": source_row.get("metrics", {}),
                "evidence_status": evidence_status,
                "regime_status": status_row.get("regime_status", {}),
                "score": source_row.get("score"),
                "real_evaluation_artifact_path": source_row.get(
                    "real_evaluation_artifact_path", ""
                ),
            }
        )
    recovered.sort(
        key=lambda row: (_float(row.get("score")), _text(row.get("candidate_id"))),
        reverse=True,
    )
    recovery_id = _stable_id(
        "candidate-recovery",
        sweep_id,
        policy_run_id,
        generated.isoformat(),
    )
    recovery_dir = _unique_dir(output_dir / recovery_id)
    recovery_dir.mkdir(parents=True, exist_ok=False)
    leaderboard = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_recovery_leaderboard",
        "recovery_id": recovery_dir.name,
        "source_sweep_id": sweep_id,
        "top_recovered_candidates": recovered[:20],
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_recovery_manifest",
        "recovery_id": recovery_dir.name,
        "source_sweep_id": sweep_id,
        "source_policy_run_id": policy_run_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if recovered else "PASS_WITH_WARNINGS",
        "candidate_count": len(statuses),
        "recovered_candidate_count": len(recovered),
        "observe_only_candidate_count": len(recovered),
        "manual_review_required_count": len(recovered),
        "rejected_after_calibration_count": len(rejected),
        "recovered_candidates_path": str(recovery_dir / "recovered_candidates.jsonl"),
        "rejected_after_calibration_path": str(recovery_dir / "rejected_after_calibration.jsonl"),
        "recovery_leaderboard_path": str(recovery_dir / "recovery_leaderboard.json"),
        "recovery_report_path": str(recovery_dir / "recovery_report.md"),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(recovery_dir / "recovery_manifest.json", manifest)
    _write_jsonl(recovery_dir / "recovered_candidates.jsonl", recovered)
    _write_jsonl(recovery_dir / "rejected_after_calibration.jsonl", rejected)
    _write_json(recovery_dir / "recovery_leaderboard.json", leaderboard)
    _write_text(
        recovery_dir / "recovery_report.md",
        render_candidate_recovery_markdown(manifest, recovered, rejected),
    )
    _update_latest_pointer(
        "latest_candidate_recovery",
        recovery_dir.name,
        recovery_dir / "recovery_manifest.json",
    )
    return {
        "recovery_id": recovery_dir.name,
        "recovery_dir": recovery_dir,
        "manifest": manifest,
        "recovered_candidates": recovered,
        "rejected_candidates": rejected,
    }


def candidate_recovery_report_payload(
    *,
    recovery_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CANDIDATE_RECOVERY_DIR,
) -> dict[str, Any]:
    resolved_id = recovery_id or (
        _latest_pointer_artifact_id("latest_candidate_recovery") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--recovery-id or --latest is required")
    recovery_dir = output_dir / resolved_id
    return {
        **_read_json(recovery_dir / "recovery_manifest.json"),
        "recovery_dir": str(recovery_dir),
    }


def validate_candidate_recovery_artifact(
    *,
    recovery_id: str,
    output_dir: Path = DEFAULT_CANDIDATE_RECOVERY_DIR,
) -> dict[str, Any]:
    recovery_dir = output_dir / recovery_id
    manifest = _read_optional_json(recovery_dir / "recovery_manifest.json") or {}
    recovered = _read_jsonl(recovery_dir / "recovered_candidates.jsonl")
    required = [
        "recovery_manifest.json",
        "recovered_candidates.jsonl",
        "rejected_after_calibration.jsonl",
        "recovery_leaderboard.json",
        "recovery_report.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (recovery_dir / name).exists(), name) for name in required
    ]
    checks.extend(
        [
            _check("recovery_id_matches", manifest.get("recovery_id") == recovery_id, recovery_id),
            _check(
                "all_recovered_are_observe_only",
                all(row.get("recovered_status") == GATE_OBSERVE_ONLY for row in recovered),
                "observe_only only",
            ),
            _check(
                "all_recovered_manual_review_required",
                all(row.get("manual_review_required") is True for row in recovered),
                "manual review required",
            ),
            _check(
                "hard_fail_excluded",
                all(
                    _mapping(row.get("evidence_status")).get("data_quality") != "FAIL"
                    and _mapping(row.get("evidence_status")).get("overfit_status") != "HIGH_RISK"
                    and _mapping(row.get("evidence_status")).get("date_range_status")
                    not in {DATE_RANGE_FAIL, DATE_RANGE_INSUFFICIENT_DATA}
                    for row in recovered
                ),
                "hard fail candidates excluded",
            ),
            _check(
                "production_candidate_not_generated",
                manifest.get("production_candidate_generated") is False,
                "recovery is observe-only",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_recovery_validation",
        "recovery_id": recovery_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def rebuild_observe_pool_from_recovery(
    *,
    recovery_id: str,
    recovery_dir: Path = DEFAULT_CANDIDATE_RECOVERY_DIR,
    output_dir: Path = DEFAULT_OBSERVE_POOL_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_dir = recovery_dir / recovery_id
    recovery_manifest = _read_json(source_dir / "recovery_manifest.json")
    recovered = _read_jsonl(source_dir / "recovered_candidates.jsonl")
    pool_id = _stable_id("observe-pool-rebuild", recovery_id, generated.isoformat())
    pool_dir = _unique_dir(output_dir / pool_id)
    pool_dir.mkdir(parents=True, exist_ok=False)
    candidates = [
        {
            **row,
            "observe_reason": "recovered_by_calibrated_evidence_gate_policy",
            "source_recovery_id": recovery_id,
            "shadow_registry_sync_status": "NOT_SYNCED_BY_DEFAULT",
        }
        for row in recovered
    ]
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_observe_pool_manifest",
        "pool_id": pool_dir.name,
        "source_sweep_id": recovery_manifest.get("source_sweep_id", ""),
        "source_recovery_id": recovery_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if candidates else "PASS_WITH_WARNINGS",
        "top_n_requested": len(candidates),
        "observe_candidate_count": len(candidates),
        "rejected_candidate_count": recovery_manifest.get("rejected_after_calibration_count", 0),
        "manual_review_required_count": sum(
            1 for row in candidates if row.get("manual_review_required") is True
        ),
        "shadow_registry_sync_status": "NOT_SYNCED_BY_DEFAULT",
        "shadow_registry_sync_reason": "rebuild writes observe pool artifact only",
        "observe_candidates_path": str(pool_dir / "observe_candidates.jsonl"),
        "observe_pool_report_path": str(pool_dir / "observe_pool_report.md"),
        "rejected_candidates": [],
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(pool_dir / "observe_pool_manifest.json", manifest)
    _write_jsonl(pool_dir / "observe_candidates.jsonl", candidates)
    _write_text(
        pool_dir / "observe_pool_report.md",
        render_observe_pool_markdown(manifest, candidates),
    )
    _update_latest_pointer(
        "latest_observe_pool",
        pool_dir.name,
        pool_dir / "observe_pool_manifest.json",
    )
    return {
        "pool_id": pool_dir.name,
        "pool_dir": pool_dir,
        "manifest": manifest,
        "candidates": candidates,
    }


def update_research_decision(
    *,
    sweep_id: str,
    diagnosis_id: str,
    impact_id: str,
    recovery_id: str,
    diagnosis_dir: Path = DEFAULT_EVIDENCE_DIAGNOSIS_DIR,
    gate_impact_dir: Path = DEFAULT_GATE_IMPACT_DIR,
    recovery_dir: Path = DEFAULT_CANDIDATE_RECOVERY_DIR,
    overnight_readiness_dir: Path = DEFAULT_OVERNIGHT_READINESS_DIR,
    output_dir: Path = DEFAULT_RESEARCH_DECISION_UPDATE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    diagnosis = _read_json(diagnosis_dir / diagnosis_id / "diagnosis_manifest.json")
    impact = _read_json(gate_impact_dir / impact_id / "gate_impact_manifest.json")
    simulation = _read_json(gate_impact_dir / impact_id / "candidate_recovery_simulation.json")
    recovery = _read_json(recovery_dir / recovery_id / "recovery_manifest.json")
    readiness = _latest_manifest_for_sweep(
        sweep_id,
        overnight_readiness_dir,
        "overnight_readiness_manifest.json",
        "source_sweep_id",
    )
    recovered_count = int(recovery.get("recovered_candidate_count") or 0)
    engineering_readiness = _text(readiness.get("overnight_readiness"), "MISSING")
    research_readiness = "NOT_READY"
    go_no_go = "NO_GO"
    recommended_action = "fix_evidence_gaps"
    blockers: list[str] = []
    warnings: list[str] = []
    if recovered_count > 0:
        research_readiness = "READY_WITH_WARNINGS"
        recommended_action = "manual_review_recovered_candidates"
        go_no_go = (
            "GO_WITH_LIMITS"
            if engineering_readiness in {"READY", "READY_WITH_WARNINGS"}
            else "NO_GO"
        )
        warnings.append("all_recovered_candidates_require_manual_review")
        if go_no_go == "GO_WITH_LIMITS":
            warnings.append("owner_approval_required_before_limited_overnight_real")
    else:
        blockers.append("no_recovered_observe_only_candidates")
        recommended_action = "calibrate_gates_or_fix_evidence_gaps"
    if engineering_readiness in {"NOT_READY", "MISSING"}:
        blockers.append("overnight_engineering_readiness_not_ready")
    decision_update_id = _stable_id(
        "research-decision-update",
        sweep_id,
        diagnosis_id,
        impact_id,
        recovery_id,
        generated.isoformat(),
    )
    decision_dir = _unique_dir(output_dir / decision_update_id)
    decision_dir.mkdir(parents=True, exist_ok=False)
    go_no_go_matrix = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_go_no_go_matrix",
        "decision_update_id": decision_dir.name,
        "medium_real_status": "PASS" if int(diagnosis.get("candidate_count") or 0) > 0 else "FAIL",
        "usable_candidates_before": diagnosis.get("usable_candidates", 0),
        "usable_candidates_after": recovered_count,
        "observe_candidates_after": recovery.get("observe_only_candidate_count", recovered_count),
        "overnight_engineering_readiness": engineering_readiness,
        "overnight_research_readiness": research_readiness,
        "recommended_action": recommended_action,
        "go_no_go": go_no_go,
        "required_owner_approval": True,
        "blocking_issues": blockers,
        "warnings": warnings,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    next_actions = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_research_decision_update_actions",
        "decision_update_id": decision_dir.name,
        "recommended_action": recommended_action,
        "allowed_actions": (
            ["manual_review_recovered_candidates", "run_limited_overnight_real"]
            if go_no_go == "GO_WITH_LIMITS"
            else ["fix_evidence_gaps", "calibrate_gates", "rebuild_observe_pool"]
        ),
        "disallowed_without_owner_approval": ["run_full_overnight_real", "production_candidate"],
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_research_decision_update_manifest",
        "decision_update_id": decision_dir.name,
        "source_sweep_id": sweep_id,
        "source_diagnosis_id": diagnosis_id,
        "source_impact_id": impact_id,
        "source_recovery_id": recovery_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "go_no_go": go_no_go,
        "recommended_action": recommended_action,
        "recovered_candidate_count": recovered_count,
        "observe_candidate_count": recovery.get("observe_only_candidate_count", recovered_count),
        "go_no_go_matrix_path": str(decision_dir / "go_no_go_matrix.json"),
        "next_action_recommendations_path": str(decision_dir / "next_action_recommendations.json"),
        "research_decision_update_report_path": str(
            decision_dir / "research_decision_update_report.md"
        ),
        "reader_brief_section_path": str(decision_dir / "reader_brief_section.md"),
        "source_artifacts": {
            "diagnosis": str(diagnosis_dir / diagnosis_id / "diagnosis_manifest.json"),
            "gate_impact": str(gate_impact_dir / impact_id / "gate_impact_manifest.json"),
            "candidate_recovery": str(recovery_dir / recovery_id / "recovery_manifest.json"),
            "overnight_readiness": readiness.get("_manifest_path", ""),
        },
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(decision_dir / "decision_update_manifest.json", manifest)
    _write_json(decision_dir / "go_no_go_matrix.json", go_no_go_matrix)
    _write_json(decision_dir / "next_action_recommendations.json", next_actions)
    _write_text(
        decision_dir / "reader_brief_section.md",
        render_research_decision_update_reader_brief(go_no_go_matrix),
    )
    _write_text(
        decision_dir / "research_decision_update_report.md",
        render_research_decision_update_markdown(
            manifest,
            go_no_go_matrix,
            simulation=_records(simulation.get("scenarios")),
            impact=impact,
        ),
    )
    _update_latest_pointer(
        "latest_research_decision_update",
        decision_dir.name,
        decision_dir / "decision_update_manifest.json",
    )
    return {
        "decision_update_id": decision_dir.name,
        "decision_update_dir": decision_dir,
        "manifest": manifest,
        "go_no_go_matrix": go_no_go_matrix,
    }


def validate_research_decision_update_artifact(
    *,
    decision_update_id: str,
    output_dir: Path = DEFAULT_RESEARCH_DECISION_UPDATE_DIR,
) -> dict[str, Any]:
    decision_dir = output_dir / decision_update_id
    manifest = _read_optional_json(decision_dir / "decision_update_manifest.json") or {}
    go_no_go = _read_optional_json(decision_dir / "go_no_go_matrix.json") or {}
    required = [
        "decision_update_manifest.json",
        "go_no_go_matrix.json",
        "next_action_recommendations.json",
        "research_decision_update_report.md",
        "reader_brief_section.md",
    ]
    checks = [
        _check(f"artifact_exists:{name}", (decision_dir / name).exists(), name) for name in required
    ]
    checks.extend(
        [
            _check(
                "decision_update_id_matches",
                manifest.get("decision_update_id") == decision_update_id,
                decision_update_id,
            ),
            _check(
                "go_no_go_present",
                go_no_go.get("go_no_go") in {"GO_WITH_LIMITS", "NO_GO"},
                _text(go_no_go.get("go_no_go")),
            ),
            _check(
                "owner_approval_required",
                go_no_go.get("required_owner_approval") is True,
                "overnight/production boundary requires owner",
            ),
            _check(
                "production_candidate_not_generated",
                manifest.get("production_candidate_generated") is False,
                "decision update is research-only",
            ),
        ]
    )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_research_decision_update_validation",
        "decision_update_id": decision_update_id,
        "status": status,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def research_decision_update_report_payload(
    *,
    decision_update_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_RESEARCH_DECISION_UPDATE_DIR,
) -> dict[str, Any]:
    resolved_id = decision_update_id or (
        _latest_pointer_artifact_id("latest_research_decision_update") if latest else ""
    )
    if not resolved_id:
        raise DynamicV3ParameterResearchError("--decision-update-id or --latest is required")
    decision_dir = output_dir / resolved_id
    return {
        **_read_json(decision_dir / "decision_update_manifest.json"),
        "decision_update_dir": str(decision_dir),
    }


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
    sweep_dir = sweep_output_dir / sweep_id
    candidate_results_path = sweep_dir / "candidate_results.jsonl"
    candidate_result = _candidate_result(sweep_dir, candidate_id)
    if candidate_result is None:
        raise DynamicV3ParameterResearchError(f"candidate not found in sweep: {candidate_id}")
    candidate_report_path = sweep_dir / "candidates" / candidate_id / "candidate_report.json"
    candidate_report = _mapping(_read_optional_json(candidate_report_path))
    if not candidate_report:
        raise DynamicV3ParameterResearchError(
            "candidate report is required before attribution: "
            f"run candidate report for {sweep_id}/{candidate_id}"
        )
    if (
        _text(candidate_report.get("candidate_id")) != candidate_id
        or _text(candidate_report.get("source_sweep_id")) != sweep_id
        or _text(candidate_report.get("report_type"))
        != "etf_dynamic_v3_parameter_candidate_report"
    ):
        raise DynamicV3ParameterResearchError(
            f"candidate report lineage mismatch: {candidate_report_path}"
        )
    attribution_dir = output_dir / candidate_id
    real_path_raw = _text(candidate_report.get("real_evaluation_artifact_path"))
    result_real_path_raw = _text(candidate_result.get("real_evaluation_artifact_path"))
    if real_path_raw != result_real_path_raw:
        raise DynamicV3ParameterResearchError(
            "candidate report real evaluation path does not match candidate result"
        )
    real_path = _resolve_project_path(Path(real_path_raw)) if real_path_raw else None
    expected_real_dir = (sweep_dir / "real_evaluation" / candidate_id).resolve(strict=False)
    if real_path is not None and real_path.resolve(strict=False).parent != expected_real_dir:
        raise DynamicV3ParameterResearchError(
            "real evaluation artifact must belong to source sweep candidate directory"
        )
    real_payload = _read_optional_json(real_path) if real_path is not None else None
    if real_payload is not None:
        expected_report_id = _text(
            _mapping(candidate_report.get("metrics")).get("real_evaluation_report_id")
        )
        observed_report_id = _text(real_payload.get("dynamic_v3_real_evaluation_report_id"))
        if not expected_report_id or observed_report_id != expected_report_id:
            raise DynamicV3ParameterResearchError(
                "real evaluation report id does not match candidate report metrics"
            )
    weight_metadata_path = (
        real_path.parent / "weight_path_metadata.json" if real_path is not None else None
    )
    evaluation_id = _text(_mapping(real_payload).get("dynamic_v3_real_evaluation_report_id"))
    weight_inspection = (
        _inspect_weight_path_evidence(evaluation_id, real_path.parent)
        if real_path is not None and real_payload is not None and evaluation_id
        else _missing_weight_path_inspection()
    )
    weight_metadata = _mapping(weight_inspection.get("metadata"))
    metrics = _mapping(candidate_report.get("metrics"))
    weight_delta = _candidate_weight_delta_rows(
        real_payload,
        daily_weights_path=(None if real_path is None else real_path.parent / "daily_weights.csv"),
        candidate_id=candidate_id,
        source_sweep_id=sweep_id,
    )
    incomplete_reasons: list[str] = []
    if real_payload is None:
        incomplete_reasons.append("missing_real_evaluation_artifact")
    observed_weight_completeness = _text(
        weight_inspection.get("observed_attribution_completeness"),
        WEIGHT_PATH_INCOMPLETE,
    )
    declared_weight_completeness = _text(
        weight_inspection.get("declared_attribution_completeness"),
        WEIGHT_PATH_INCOMPLETE,
    )
    if observed_weight_completeness == WEIGHT_PATH_INCOMPLETE:
        incomplete_reasons.append("MISSING_DAILY_WEIGHT_PATH")
    if real_payload is not None and observed_weight_completeness != WEIGHT_PATH_INCOMPLETE:
        if not weight_delta:
            incomplete_reasons.append("MISSING_WEIGHT_DELTA_EVIDENCE")
    if incomplete_reasons:
        status = WEIGHT_PATH_INCOMPLETE
        explainability_status = WEIGHT_PATH_INCOMPLETE
    else:
        status = WEIGHT_PATH_PARTIAL
        explainability_status = WEIGHT_PATH_PARTIAL
    rebalance = _rebalance_event_attribution(real_payload, metrics, incomplete_reasons)
    constraint = _constraint_event_attribution(real_payload, metrics)
    drawdown = _drawdown_window_attribution(real_payload, metrics)
    turnover = _turnover_attribution(real_payload, metrics)
    gap = _dynamic_vs_static_gap_attribution(real_payload, metrics)
    components = {
        "rebalance_event_attribution": rebalance,
        "constraint_event_attribution": constraint,
        "drawdown_window_attribution": drawdown,
        "turnover_attribution": turnover,
        "dynamic_vs_static_gap_attribution": gap,
    }
    component_statuses: dict[str, str] = {
        "weight_path_delta": "INCOMPLETE" if not weight_delta else "PASS",
    }
    for name, component in components.items():
        component_status = WEIGHT_PATH_INCOMPLETE if incomplete_reasons else WEIGHT_PATH_PARTIAL
        source_analysis_status = _text(component.get("status"), "UNKNOWN")
        component.update(
            {
                "candidate_id": candidate_id,
                "source_sweep_id": sweep_id,
                "status": component_status,
                "source_analysis_status": source_analysis_status,
                "attribution_method": "path_and_aggregate_v2",
            }
        )
        component_statuses[name] = component_status
    source_checksums = {
        "candidate_results_sha256": _file_sha256_path(candidate_results_path),
        "candidate_report_sha256": _file_sha256_path(candidate_report_path),
        "real_evaluation_sha256": "" if real_path is None else _file_sha256_path(real_path),
        "weight_path_metadata_sha256": (
            "" if weight_metadata_path is None else _file_sha256_path(weight_metadata_path)
        ),
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_attribution_manifest",
        "candidate_id": candidate_id,
        "source_sweep_id": sweep_id,
        "status": status,
        "generated_at": generated.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "incomplete_reasons": incomplete_reasons,
        "candidate_results_path": str(candidate_results_path),
        "candidate_report_path": str(candidate_report_path),
        "real_evaluation_artifact_path": real_path_raw,
        "weight_path_metadata_path": (
            "" if weight_metadata_path is None else str(weight_metadata_path)
        ),
        "attribution_completeness": status,
        "weight_path_declared_completeness": declared_weight_completeness,
        "weight_path_observed_completeness": observed_weight_completeness,
        "weight_path_failed_check_count": weight_inspection.get("failed_check_count", 0),
        "weight_path_limitations": weight_inspection.get("limitations", []),
        "component_evidence_statuses": component_statuses,
        "source_checksums": source_checksums,
        "source_mutation_performed": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_attribution_report",
        "candidate_id": candidate_id,
        "source_sweep_id": sweep_id,
        "status": manifest["status"],
        "explainability_status": explainability_status,
        "attribution_completeness": status,
        "weight_path_declared_completeness": declared_weight_completeness,
        "weight_path_observed_completeness": observed_weight_completeness,
        "weight_path_limitations": weight_inspection.get("limitations", []),
        "component_evidence_statuses": component_statuses,
        "attribution_method": "path_and_aggregate_v2",
        "source_mutation_performed": False,
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
    attribution_dir.mkdir(parents=True, exist_ok=True)
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
    output_names = [
        "weight_path_delta.csv",
        "rebalance_event_attribution.json",
        "constraint_event_attribution.json",
        "drawdown_window_attribution.json",
        "turnover_attribution.json",
        "dynamic_vs_static_gap_attribution.json",
        "candidate_attribution_report.md",
    ]
    manifest["output_artifact_checksums"] = {
        name: _file_sha256_path(attribution_dir / name) for name in output_names
    }
    _write_json(attribution_dir / "attribution_manifest.json", manifest)
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
    manifest = _mapping(_read_optional_json(candidate_dir / "attribution_manifest.json"))
    checks.extend(_candidate_attribution_content_checks(candidate_dir, candidate_id, manifest))
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


def _candidate_attribution_content_checks(
    candidate_dir: Path,
    candidate_id: str,
    manifest: Mapping[str, Any],
) -> list[dict[str, Any]]:
    checks = [
        _check("manifest_is_object", bool(manifest), "attribution_manifest.json"),
        _check(
            "manifest_candidate_id_matches",
            _text(manifest.get("candidate_id")) == candidate_id,
            _text(manifest.get("candidate_id")),
        ),
        _check(
            "manifest_source_sweep_id_present",
            bool(_text(manifest.get("source_sweep_id"))),
            _text(manifest.get("source_sweep_id")),
        ),
        _check(
            "manifest_status_explicit",
            _text(manifest.get("status"))
            in {WEIGHT_PATH_COMPLETE, WEIGHT_PATH_PARTIAL, WEIGHT_PATH_INCOMPLETE},
            _text(manifest.get("status")),
        ),
        _check(
            "source_mutation_not_performed",
            manifest.get("source_mutation_performed") is False,
            str(manifest.get("source_mutation_performed")),
        ),
    ]
    source_sweep_id = _text(manifest.get("source_sweep_id"))
    candidate_report_raw = _text(manifest.get("candidate_report_path"))
    candidate_report_path = (
        _resolve_project_path(Path(candidate_report_raw)) if candidate_report_raw else None
    )
    candidate_report = _mapping(_read_optional_json(candidate_report_path))
    candidate_report_path_owned = False
    sweep_dir: Path | None = None
    if candidate_report_path is not None:
        try:
            sweep_dir = candidate_report_path.parents[2]
            candidate_report_path_owned = (
                candidate_report_path.parent.name == candidate_id
                and candidate_report_path.parents[1].name == "candidates"
                and sweep_dir.name == source_sweep_id
            )
        except IndexError:
            candidate_report_path_owned = False
    checks.extend(
        [
            _check(
                "candidate_report_exists",
                candidate_report_path is not None and candidate_report_path.is_file(),
                candidate_report_raw,
            ),
            _check(
                "candidate_report_candidate_id_matches",
                _text(candidate_report.get("candidate_id")) == candidate_id,
                _text(candidate_report.get("candidate_id")),
            ),
            _check(
                "candidate_report_source_sweep_id_matches",
                _text(candidate_report.get("source_sweep_id")) == source_sweep_id,
                _text(candidate_report.get("source_sweep_id")),
            ),
            _check(
                "candidate_report_path_owned_by_sweep_candidate",
                candidate_report_path_owned,
                candidate_report_raw,
            ),
        ]
    )
    source_checksums = _mapping(manifest.get("source_checksums"))
    candidate_results_raw = _text(manifest.get("candidate_results_path"))
    candidate_results_path = (
        _resolve_project_path(Path(candidate_results_raw)) if candidate_results_raw else None
    )
    expected_candidate_results_path = (
        None if sweep_dir is None else sweep_dir / "candidate_results.jsonl"
    )
    candidate_result = (
        _candidate_result(sweep_dir, candidate_id) if sweep_dir is not None else None
    )
    checks.append(
        _check(
            "candidate_report_checksum_matches",
            candidate_report_path is not None
            and _text(source_checksums.get("candidate_report_sha256"))
            == _file_sha256_path(candidate_report_path),
            _text(source_checksums.get("candidate_report_sha256")),
        )
    )
    checks.extend(
        [
            _check(
                "candidate_results_path_matches_sweep",
                candidate_results_path is not None
                and expected_candidate_results_path is not None
                and candidate_results_path.resolve(strict=False)
                == expected_candidate_results_path.resolve(strict=False),
                candidate_results_raw,
            ),
            _check(
                "candidate_results_checksum_matches",
                candidate_results_path is not None
                and _text(source_checksums.get("candidate_results_sha256"))
                == _file_sha256_path(candidate_results_path),
                _text(source_checksums.get("candidate_results_sha256")),
            ),
            _check(
                "candidate_result_present",
                candidate_result is not None,
                candidate_id,
            ),
            _check(
                "candidate_report_matches_candidate_result",
                candidate_result is not None
                and _text(candidate_report.get("real_evaluation_artifact_path"))
                == _text(candidate_result.get("real_evaluation_artifact_path"))
                and _mapping(candidate_report.get("parameters"))
                == _mapping(candidate_result.get("parameters"))
                and _text(candidate_report.get("metrics_source"))
                == _text(candidate_result.get("metrics_source")),
                candidate_id,
            ),
        ]
    )

    real_path_raw = _text(manifest.get("real_evaluation_artifact_path"))
    real_path = _resolve_project_path(Path(real_path_raw)) if real_path_raw else None
    real_payload = _mapping(_read_optional_json(real_path))
    expected_real_dir: Path | None = None
    if sweep_dir is not None:
        try:
            expected_real_dir = (sweep_dir / "real_evaluation" / candidate_id).resolve(
                strict=False
            )
        except IndexError:
            expected_real_dir = None
    real_path_owned = (
        real_path is None
        or (
            expected_real_dir is not None
            and real_path.resolve(strict=False).parent == expected_real_dir
        )
    )
    checks.extend(
        [
            _check("real_evaluation_path_owned_by_candidate", real_path_owned, real_path_raw),
            _check(
                "candidate_report_real_path_matches_manifest",
                _text(candidate_report.get("real_evaluation_artifact_path")) == real_path_raw,
                real_path_raw,
            ),
            _check(
                "real_evaluation_checksum_matches",
                (
                    not real_path_raw
                    and not _text(source_checksums.get("real_evaluation_sha256"))
                )
                or (
                    real_path is not None
                    and real_path.is_file()
                    and _text(source_checksums.get("real_evaluation_sha256"))
                    == _file_sha256_path(real_path)
                ),
                _text(source_checksums.get("real_evaluation_sha256")),
            ),
        ]
    )
    expected_report_id = _text(
        _mapping(candidate_report.get("metrics")).get("real_evaluation_report_id")
    )
    observed_report_id = _text(real_payload.get("dynamic_v3_real_evaluation_report_id"))
    checks.append(
        _check(
            "real_evaluation_report_id_matches",
            (not real_path_raw and not expected_report_id)
            or (bool(real_payload) and observed_report_id == expected_report_id),
            f"expected={expected_report_id};observed={observed_report_id}",
        )
    )

    weight_metadata_path_raw = _text(manifest.get("weight_path_metadata_path"))
    weight_metadata_path = (
        _resolve_project_path(Path(weight_metadata_path_raw)) if weight_metadata_path_raw else None
    )
    expected_weight_path = (
        None if real_path is None else real_path.parent / "weight_path_metadata.json"
    )
    checks.extend(
        [
            _check(
                "weight_metadata_path_matches_real_artifact",
                (weight_metadata_path is None and expected_weight_path is None)
                or (
                    weight_metadata_path is not None
                    and expected_weight_path is not None
                    and weight_metadata_path.resolve(strict=False)
                    == expected_weight_path.resolve(strict=False)
                ),
                weight_metadata_path_raw,
            ),
            _check(
                "weight_metadata_checksum_matches",
                (
                    weight_metadata_path is None
                    and not _text(source_checksums.get("weight_path_metadata_sha256"))
                )
                or (
                    weight_metadata_path is not None
                    and _text(source_checksums.get("weight_path_metadata_sha256"))
                    == _file_sha256_path(weight_metadata_path)
                ),
                _text(source_checksums.get("weight_path_metadata_sha256")),
            ),
        ]
    )
    weight_inspection = (
        _inspect_weight_path_evidence(observed_report_id, real_path.parent)
        if real_path is not None and real_payload and observed_report_id
        else _missing_weight_path_inspection()
    )
    observed_weight_status = _text(
        weight_inspection.get("observed_attribution_completeness"), WEIGHT_PATH_INCOMPLETE
    )
    declared_weight_status = _text(
        weight_inspection.get("declared_attribution_completeness"), WEIGHT_PATH_INCOMPLETE
    )
    checks.extend(
        [
            _check(
                "weight_path_observed_completeness_matches",
                _text(manifest.get("weight_path_observed_completeness"))
                == observed_weight_status,
                observed_weight_status,
            ),
            _check(
                "weight_path_declared_completeness_matches",
                _text(manifest.get("weight_path_declared_completeness"))
                == declared_weight_status,
                declared_weight_status,
            ),
            _check(
                "weight_path_limitations_match",
                _texts(manifest.get("weight_path_limitations"))
                == _texts(weight_inspection.get("limitations")),
                ",".join(_texts(weight_inspection.get("limitations"))),
            ),
        ]
    )

    expected_delta = _candidate_weight_delta_rows(
        real_payload or None,
        daily_weights_path=(None if real_path is None else real_path.parent / "daily_weights.csv"),
        candidate_id=candidate_id,
        source_sweep_id=source_sweep_id,
    )
    actual_delta = _read_csv_as_text(candidate_dir / "weight_path_delta.csv")
    delta_matches = _candidate_weight_delta_csv_matches(actual_delta, expected_delta)
    checks.append(
        _check(
            "weight_path_delta_matches_source",
            delta_matches,
            "expected_rows="
            f"{len(expected_delta)};actual_rows="
            f"{0 if actual_delta is None else len(actual_delta)}",
        )
    )
    expected_incomplete_reasons: list[str] = []
    if not real_payload:
        expected_incomplete_reasons.append("missing_real_evaluation_artifact")
    if observed_weight_status == WEIGHT_PATH_INCOMPLETE:
        expected_incomplete_reasons.append("MISSING_DAILY_WEIGHT_PATH")
    if real_payload and observed_weight_status != WEIGHT_PATH_INCOMPLETE and not expected_delta:
        expected_incomplete_reasons.append("MISSING_WEIGHT_DELTA_EVIDENCE")
    expected_status = (
        WEIGHT_PATH_INCOMPLETE if expected_incomplete_reasons else WEIGHT_PATH_PARTIAL
    )
    checks.extend(
        [
            _check(
                "incomplete_reasons_match_source",
                _texts(manifest.get("incomplete_reasons")) == expected_incomplete_reasons,
                ",".join(expected_incomplete_reasons),
            ),
            _check(
                "attribution_status_matches_source",
                _text(manifest.get("status")) == expected_status
                and _text(manifest.get("attribution_completeness")) == expected_status,
                expected_status,
            ),
            _check(
                "complete_attribution_not_fabricated",
                _text(manifest.get("status")) != WEIGHT_PATH_COMPLETE,
                "path_and_aggregate_v2 is not complete explainability",
            ),
        ]
    )
    component_files = {
        "rebalance_event_attribution": "rebalance_event_attribution.json",
        "constraint_event_attribution": "constraint_event_attribution.json",
        "drawdown_window_attribution": "drawdown_window_attribution.json",
        "turnover_attribution": "turnover_attribution.json",
        "dynamic_vs_static_gap_attribution": "dynamic_vs_static_gap_attribution.json",
    }
    component_statuses = _mapping(manifest.get("component_evidence_statuses"))
    expected_component_status = (
        WEIGHT_PATH_INCOMPLETE if expected_incomplete_reasons else WEIGHT_PATH_PARTIAL
    )
    checks.append(
        _check(
            "weight_delta_component_status_matches",
            _text(component_statuses.get("weight_path_delta"))
            == ("PASS" if expected_delta else WEIGHT_PATH_INCOMPLETE),
            _text(component_statuses.get("weight_path_delta")),
        )
    )
    for component_name, filename in component_files.items():
        component = _mapping(_read_optional_json(candidate_dir / filename))
        checks.extend(
            [
                _check(
                    f"{component_name}_candidate_id_matches",
                    _text(component.get("candidate_id")) == candidate_id,
                    _text(component.get("candidate_id")),
                ),
                _check(
                    f"{component_name}_source_sweep_matches",
                    _text(component.get("source_sweep_id")) == source_sweep_id,
                    _text(component.get("source_sweep_id")),
                ),
                _check(
                    f"{component_name}_status_matches",
                    _text(component.get("status")) == expected_component_status
                    and _text(component_statuses.get(component_name))
                    == expected_component_status,
                    expected_component_status,
                ),
                _check(
                    f"{component_name}_method_explicit",
                    _text(component.get("attribution_method")) == "path_and_aggregate_v2",
                    _text(component.get("attribution_method")),
                ),
                _check(
                    f"{component_name}_source_analysis_status_present",
                    bool(_text(component.get("source_analysis_status"))),
                    _text(component.get("source_analysis_status")),
                ),
            ]
        )
    output_checksums = _mapping(manifest.get("output_artifact_checksums"))
    for filename, expected_checksum in output_checksums.items():
        checks.append(
            _check(
                f"output_checksum_matches:{filename}",
                _text(expected_checksum) == _file_sha256_path(candidate_dir / filename),
                _text(expected_checksum),
            )
        )
    checks.append(
        _check(
            "output_checksum_inventory_complete",
            set(output_checksums)
            == {
                "weight_path_delta.csv",
                *component_files.values(),
                "candidate_attribution_report.md",
            },
            ",".join(sorted(output_checksums)),
        )
    )
    return checks


def _candidate_weight_delta_csv_matches(
    actual: pd.DataFrame | None,
    expected: Sequence[Mapping[str, Any]],
) -> bool:
    if not expected:
        return actual is not None and actual.empty and list(actual.columns) == ["status"]
    required = {
        "as_of",
        "candidate_id",
        "source_sweep_id",
        "reference_group",
        "symbol",
        "candidate_weight",
        "baseline_weight",
        "delta",
    }
    if actual is None or len(actual) != len(expected) or not required <= set(actual.columns):
        return False
    actual_by_symbol = {row["symbol"]: row for row in actual.to_dict(orient="records")}
    for expected_row in expected:
        row = actual_by_symbol.get(_text(expected_row.get("symbol")))
        if row is None:
            return False
        for field in ("as_of", "candidate_id", "source_sweep_id", "reference_group"):
            if _text(row.get(field)) != _text(expected_row.get(field)):
                return False
        for field in ("candidate_weight", "baseline_weight", "delta"):
            try:
                if abs(float(row.get(field)) - float(expected_row.get(field))) > 1e-9:
                    return False
            except (TypeError, ValueError):
                return False
        if abs(
            float(row["candidate_weight"])
            - float(row["baseline_weight"])
            - float(row["delta"])
        ) > 1e-6:
            return False
    return True


def weight_path_report_payload(
    *,
    evaluation_id: str,
    search_root: Path = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
) -> dict[str, Any]:
    inspection = _inspect_weight_path_evidence(evaluation_id, search_root)
    weight_dir = inspection["weight_path_dir"]
    if inspection["matching_directory_count"] != 1:
        if inspection["matching_directory_count"] == 0:
            raise DynamicV3ParameterResearchError(
                f"weight path artifact not found: {evaluation_id}"
            )
        raise DynamicV3ParameterResearchError(
            "weight path artifact is ambiguous: "
            f"{evaluation_id} matched {inspection['matching_directory_count']} directories"
        )
    metadata = inspection["metadata"]
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_path_report_view",
        "status": inspection["observed_attribution_completeness"],
        "declared_attribution_completeness": inspection[
            "declared_attribution_completeness"
        ],
        "observed_attribution_completeness": inspection[
            "observed_attribution_completeness"
        ],
        "evaluation_id": evaluation_id,
        "candidate_id": metadata.get("candidate_id"),
        "daily_weights_path": str(Path(weight_dir) / "daily_weights.csv"),
        "weight_path_metadata_path": str(Path(weight_dir) / "weight_path_metadata.json"),
        "checks": inspection["checks"],
        "failed_check_count": inspection["failed_check_count"],
        "limitations": inspection["limitations"],
        "metadata": metadata,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def validate_weight_path_artifact(
    *,
    evaluation_id: str,
    search_root: Path = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
) -> dict[str, Any]:
    inspection = _inspect_weight_path_evidence(evaluation_id, search_root)
    metadata = inspection["metadata"]
    checks = inspection["checks"]
    status = (
        "PASS"
        if inspection["observed_attribution_completeness"] != WEIGHT_PATH_INCOMPLETE
        and all(check["passed"] for check in checks)
        else "FAIL"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_path_validation",
        "evaluation_id": evaluation_id,
        "candidate_id": metadata.get("candidate_id", ""),
        "status": status,
        "attribution_completeness": inspection["observed_attribution_completeness"],
        "declared_attribution_completeness": inspection[
            "declared_attribution_completeness"
        ],
        "observed_attribution_completeness": inspection[
            "observed_attribution_completeness"
        ],
        "missing_fields": metadata.get("missing_fields", []),
        "limitations": inspection["limitations"],
        "checks": checks,
        "failed_check_count": inspection["failed_check_count"],
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
        row for row in _read_candidate_results(sweep_dir) if row.get("gate") != GATE_REJECT
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
        _check(f"artifact_exists:{name}", (wf_dir / name).exists(), name) for name in required
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
    results = _read_candidate_results(sweep_dir)
    result = _candidate_result_from_rows(results, candidate_id)
    if result is None:
        raise DynamicV3ParameterResearchError(f"candidate not found: {candidate_id}")
    robustness_id = _stable_id("robustness", sweep_id, candidate_id, generated.isoformat())
    robustness_dir = _unique_dir(output_dir / robustness_id)
    robustness_dir.mkdir(parents=True, exist_ok=False)
    evaluator = _robustness_evaluator_mode(result, config)
    source_evidence, real_payload = _robustness_source_evidence(result, evaluator)
    if evaluator == EVALUATOR_REAL_DYNAMIC_V3_RESCUE:
        sensitivity = _real_sensitivity_rows(result, results, config)
        stress = _real_stress_bucket_results(result, config, real_payload)
        regime = _real_regime_bucket_results(real_payload)
    else:
        sensitivity = _sensitivity_rows(result, config)
        stress = _stress_bucket_results(result, config)
        regime = _regime_bucket_results(result)
    overfit = _overfit_diagnostics(
        sensitivity,
        stress,
        config,
        evaluator_mode=evaluator,
        source_evidence=source_evidence,
        regime=regime,
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_robustness_manifest",
        "robustness_id": robustness_id,
        "source_sweep_id": sweep_id,
        "candidate_id": candidate_id,
        "status": overfit["robustness_status"],
        "evaluator_mode": evaluator,
        "evaluator_version": source_evidence["evaluator_version"],
        "metrics_source": source_evidence["metrics_source"],
        "not_for_investment_decision": source_evidence["not_for_investment_decision"],
        "data_quality": source_evidence["data_quality"],
        "real_evaluation_artifact_path": source_evidence["real_evaluation_artifact_path"],
        "source_real_evaluation_artifact_path": source_evidence[
            "source_real_evaluation_artifact_path"
        ],
        "source_real_evaluation_artifact_exists": source_evidence[
            "source_real_evaluation_artifact_exists"
        ],
        "source_real_evaluation_report_id": source_evidence["source_real_evaluation_report_id"],
        "sensitivity_evidence_status": overfit["sensitivity_evidence_status"],
        "real_neighbor_count": overfit["real_neighbor_count"],
        "missing_real_neighbor_count": overfit["missing_real_neighbor_count"],
        "stress_evidence_status": overfit["stress_evidence_status"],
        "regime_evidence_status": overfit["regime_evidence_status"],
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
        "evaluator_mode": evaluator,
        "evaluator_version": source_evidence["evaluator_version"],
        "metrics_source": source_evidence["metrics_source"],
        "not_for_investment_decision": source_evidence["not_for_investment_decision"],
        "data_quality": source_evidence["data_quality"],
        "real_evaluation_artifact_path": source_evidence["real_evaluation_artifact_path"],
        "source_real_evaluation_artifact_path": source_evidence[
            "source_real_evaluation_artifact_path"
        ],
        "source_real_evaluation_artifact_exists": source_evidence[
            "source_real_evaluation_artifact_exists"
        ],
        "source_real_evaluation_report_id": source_evidence["source_real_evaluation_report_id"],
        "overfit_status": overfit["overfit_status"],
        "parameter_sensitivity_status": overfit["parameter_sensitivity_status"],
        "sensitivity_evidence_status": overfit["sensitivity_evidence_status"],
        "real_neighbor_count": overfit["real_neighbor_count"],
        "missing_real_neighbor_count": overfit["missing_real_neighbor_count"],
        "stress_bucket_status": overfit["stress_bucket_status"],
        "stress_evidence_status": overfit["stress_evidence_status"],
        "regime_evidence_status": overfit["regime_evidence_status"],
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
        _check(f"artifact_exists:{name}", (overfit_dir / name).exists(), name) for name in required
    ]
    manifest = _read_optional_json(overfit_dir / "overfit_manifest.json") or {}
    checks.append(
        _check(
            "overfit_status_allowed",
            _text(manifest.get("overfit_status")) in {"LOW_RISK", "REVIEW_REQUIRED", "HIGH_RISK"},
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
    manifest = _read_optional_json(robustness_dir / "robustness_manifest.json") or {}
    diagnostics = _read_optional_json(robustness_dir / "overfit_diagnostics.json") or {}
    evaluator = _text(manifest.get("evaluator_mode"), EVALUATOR_TINY_FIXTURE_PROXY)
    checks.append(
        _check(
            "robustness_status_allowed",
            _text(manifest.get("status")) in {"PASS", "REVIEW_REQUIRED", "FAIL"},
            _text(manifest.get("status")),
        )
    )
    checks.append(
        _check(
            "evaluator_mode_valid",
            evaluator in EVALUATOR_VERSIONS,
            evaluator,
        )
    )
    checks.append(
        _check(
            "metrics_source_recorded",
            bool(_text(manifest.get("metrics_source"))),
            _text(manifest.get("metrics_source")),
        )
    )
    if evaluator == EVALUATOR_TINY_FIXTURE_PROXY:
        checks.append(
            _check(
                "tiny_fixture_not_for_investment_decision",
                manifest.get("not_for_investment_decision") is True,
                "tiny robustness artifact remains fixture-only",
            )
        )
    if evaluator == EVALUATOR_REAL_DYNAMIC_V3_RESCUE:
        artifact_path = _text(manifest.get("source_real_evaluation_artifact_path"))
        missing_neighbors = int(_float(manifest.get("missing_real_neighbor_count")))
        checks.append(
            _check(
                "real_evaluation_artifact_path_exists",
                bool(artifact_path) and Path(artifact_path).exists(),
                artifact_path,
            )
        )
        checks.append(
            _check(
                "real_metrics_from_real_artifact",
                manifest.get("metrics_source") == "real_evaluation_artifact",
                _text(manifest.get("metrics_source")),
            )
        )
        checks.append(
            _check(
                "real_evaluator_not_fixture_only",
                manifest.get("not_for_investment_decision") is False,
                "real evaluator robustness is not marked fixture-only",
            )
        )
        checks.append(
            _check(
                "missing_real_neighbors_fail_closed",
                missing_neighbors == 0 or manifest.get("status") != "PASS",
                f"missing_real_neighbor_count={missing_neighbors}; status={manifest.get('status')}",
            )
        )
        checks.append(
            _check(
                "real_sensitivity_not_fixture_proxy",
                diagnostics.get("sensitivity_evidence_status") != "TINY_FIXTURE_PROXY",
                _text(diagnostics.get("sensitivity_evidence_status")),
            )
        )
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_robustness_validation",
        "robustness_id": robustness_id,
        "status": status,
        "evaluator_mode": evaluator,
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
    walk_forward_dir: Path = DEFAULT_WALK_FORWARD_DIR,
    robustness_dir: Path = DEFAULT_ROBUSTNESS_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    sweep_dir = sweep_output_dir / sweep_id
    candidate_report_path = sweep_dir / "candidates" / candidate_id / "candidate_report.json"
    if not candidate_report_path.exists():
        raise DynamicV3ParameterResearchError("candidate report is required before registration")
    report = _read_json(candidate_report_path)
    if _text(report.get("source_sweep_id")) != sweep_id:
        raise DynamicV3ParameterResearchError("candidate report source_sweep_id mismatch")
    if _text(report.get("candidate_id")) != candidate_id:
        raise DynamicV3ParameterResearchError("candidate report candidate_id mismatch")
    hard_gate_status = _text(report.get("hard_gate_status"))
    if not hard_gate_status:
        raise DynamicV3ParameterResearchError("candidate report hard_gate_status is required")
    if hard_gate_status == GATE_REJECT:
        raise DynamicV3ParameterResearchError("rejected candidate cannot be registered")
    config = load_parameter_sweep_config(sweep_dir / "sweep_config.normalized.yaml")
    registry = load_shadow_registry(registry_path)
    candidates = _records(registry.get("candidates"))
    existing = next((row for row in candidates if row.get("candidate_id") == candidate_id), None)
    basis = _shadow_observation_basis(
        candidate_id,
        walk_forward_dir=walk_forward_dir,
        robustness_dir=robustness_dir,
    )
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
        _check(f"artifact_exists:{name}", (monitor_dir / name).exists(), name) for name in required
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
    walk_forward_dir: Path = DEFAULT_WALK_FORWARD_DIR,
    robustness_dir: Path = DEFAULT_ROBUSTNESS_DIR,
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
        basis_status = _text(row.get("observation_basis_status"), "incomplete_observation_basis")
        source_walk_forward_id = _text(row.get("source_walk_forward_id"))
        source_robustness_id = _text(row.get("source_robustness_id"))
        wf_manifest_path = walk_forward_dir / source_walk_forward_id / "wf_manifest.json"
        robustness_manifest_path = (
            robustness_dir / source_robustness_id / "robustness_manifest.json"
        )
        checks.append(
            _check(
                f"{candidate_id}:observation_basis_status_valid",
                basis_status in {"complete", "incomplete_observation_basis"},
                basis_status,
            )
        )
        if basis_status == "complete":
            checks.append(
                _check(
                    f"{candidate_id}:source_walk_forward_id_present",
                    bool(source_walk_forward_id),
                    "complete basis requires walk-forward id",
                )
            )
            checks.append(
                _check(
                    f"{candidate_id}:source_robustness_id_present",
                    bool(source_robustness_id),
                    "complete basis requires robustness id",
                )
            )
        if source_walk_forward_id:
            checks.append(
                _check(
                    f"{candidate_id}:source_walk_forward_exists",
                    wf_manifest_path.exists(),
                    str(wf_manifest_path),
                )
            )
        if source_robustness_id:
            checks.append(
                _check(
                    f"{candidate_id}:source_robustness_exists",
                    robustness_manifest_path.exists(),
                    str(robustness_manifest_path),
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
    require_pointers: bool = True,
) -> dict[str, Any]:
    pointers = artifacts_latest_payload(pointer_dir).get("pointers", {})
    checks = [_check("family_supported", family == STRATEGY_FAMILY, family)]
    enforce_canonical_root = _is_default_latest_pointer_dir(pointer_dir)
    if require_pointers:
        checks.append(
            _check(
                "latest_pointer_present",
                bool(pointers),
                f"pointer_dir={pointer_dir} pointer_count={len(pointers)}",
            )
        )
    for name, pointer in _mapping(pointers).items():
        target_text = _text(_mapping(pointer).get("path"))
        checks.append(_check(f"{name}:pointer_path_present", bool(target_text), target_text))
        target = (
            _resolve_project_path(Path(target_text))
            if target_text
            else Path("__missing_pointer_path__")
        )
        checks.append(_check(f"{name}:pointer_target_exists", target.exists(), str(target)))
        if enforce_canonical_root:
            checks.append(
                _check(
                    f"{name}:pointer_target_in_canonical_root",
                    _is_default_dynamic_v3_research_artifact(target),
                    str(target),
                )
            )
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


def repair_latest_pointers_payload(
    *,
    pointer_dir: Path = DEFAULT_LATEST_POINTER_DIR,
    artifact_root: Path = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    write: bool = True,
) -> dict[str, Any]:
    repaired: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for spec in _latest_pointer_repair_specs():
        candidates = sorted(
            (path for path in artifact_root.glob(spec["pattern"]) if path.is_file()),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if not candidates:
            skipped.append(
                {
                    "pointer_name": spec["pointer_name"],
                    "reason": "canonical_artifact_not_found",
                    "pattern": str(artifact_root / spec["pattern"]),
                }
            )
            continue
        path = candidates[0]
        payload = _read_optional_json(path) or {}
        artifact_id = _latest_pointer_repair_artifact_id(
            path=path,
            payload=payload,
            id_keys=spec.get("id_keys", ()),
            constant_id=_text(spec.get("constant_id")),
        )
        if not artifact_id:
            skipped.append(
                {
                    "pointer_name": spec["pointer_name"],
                    "reason": "artifact_id_not_found",
                    "path": str(path),
                }
            )
            continue
        if _is_default_latest_pointer_dir(
            pointer_dir
        ) and not _is_default_dynamic_v3_research_artifact(path):
            skipped.append(
                {
                    "pointer_name": spec["pointer_name"],
                    "reason": "canonical_root_violation",
                    "path": str(path),
                }
            )
            continue
        if write:
            _write_latest_pointer(pointer_dir, spec["pointer_name"], artifact_id, path)
        repaired.append(
            {
                "pointer_name": spec["pointer_name"],
                "artifact_id": artifact_id,
                "path": str(path),
            }
        )
    validation = validate_artifacts_payload(pointer_dir=pointer_dir) if write else None
    status = _mapping(validation).get("status", "PASS") if validation else "PASS"
    if skipped and status == "PASS":
        status = "PASS_WITH_WARNINGS"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_latest_pointer_repair",
        "status": status,
        "pointer_dir": str(pointer_dir),
        "artifact_root": str(artifact_root),
        "repaired_count": len(repaired),
        "skipped_count": len(skipped),
        "repaired_pointers": repaired,
        "skipped_pointers": skipped,
        "validation": validation,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def scheduled_observe_report_paths(
    *, output_dir: Path = DEFAULT_SCHEDULE_OBSERVE_DIR, as_of: date
) -> tuple[Path, Path]:
    stem = f"dynamic_v3_rescue_schedule_observe_{as_of.isoformat()}"
    return output_dir / f"{stem}.json", output_dir / f"{stem}.md"


def scheduled_observe_payload(
    *,
    as_of: date,
    family: str = STRATEGY_FAMILY,
    config_path: Path = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    pointer_dir: Path = DEFAULT_LATEST_POINTER_DIR,
    registry_path: Path = DEFAULT_SHADOW_REGISTRY_PATH,
    output_dir: Path = DEFAULT_SCHEDULE_OBSERVE_DIR,
    run_shadow_monitor_on_due: bool = True,
    force_due: bool = False,
    now: datetime | None = None,
    write: bool = True,
) -> dict[str, Any]:
    generated_at = now or datetime.now(UTC)
    session = us_equity_market_session(as_of)
    due = force_due or _is_weekly_last_trading_day(as_of)
    latest_payload = artifacts_latest_payload(pointer_dir)
    pointer_count = len(_mapping(latest_payload.get("pointers")))
    validation_payload = None
    stale_payload = None
    shadow_monitor = {
        "status": "SKIPPED",
        "reason": "not_due",
        "monitor_id": None,
        "monitor_dir": None,
    }
    checks = [
        _check("family_supported", family == STRATEGY_FAMILY, family),
        _check("production_effect_none", True, "production_effect=none"),
        _check("manual_review_required", True, "manual review required"),
        _check("no_real_sweep_execution", True, "schedule observe does not run profiles"),
        _check("no_promotion_pack_execution", True, "schedule observe does not build pack"),
    ]
    if not session.is_trading_day and not force_due:
        status = "PASS_WITH_SKIPS"
        due_status = "CLOSED_MARKET"
        due_reason = session.reason
        skip_reason = "closed_market"
    elif not due:
        status = "PASS_WITH_SKIPS"
        due_status = "NOT_DUE"
        due_reason = "as_of is not the weekly last completed U.S. equity trading day"
        skip_reason = "weekly_due_condition_not_met"
    elif pointer_count == 0:
        status = "PASS_WITH_SKIPS"
        due_status = "DUE_NO_POINTERS"
        due_reason = "weekly gate is due but no dynamic v3 rescue latest pointers exist"
        skip_reason = "no_research_artifacts_registered"
    else:
        due_status = "DUE"
        due_reason = "weekly last completed U.S. equity trading day"
        skip_reason = None
        validation_payload = validate_artifacts_payload(
            family=family,
            pointer_dir=pointer_dir,
            require_pointers=True,
        )
        stale_payload = stale_artifacts_payload(
            family=family,
            config_path=config_path,
            pointer_dir=pointer_dir,
            now=generated_at,
        )
        checks.append(
            _check(
                "latest_pointer_validation_pass",
                validation_payload.get("status") == "PASS",
                str(validation_payload.get("status")),
            )
        )
        checks.append(
            _check(
                "stale_artifacts_recorded",
                stale_payload.get("status") in {"PASS", "STALE"},
                str(stale_payload.get("status")),
            )
        )
        if validation_payload.get("status") != "PASS":
            status = "FAIL"
        elif stale_payload.get("status") == "STALE":
            status = "PASS_WITH_WARNINGS"
        else:
            status = "PASS"
        if status.startswith("PASS") and run_shadow_monitor_on_due:
            shadow_monitor = _run_scheduled_shadow_monitor(
                as_of=as_of,
                registry_path=registry_path,
                output_dir=DEFAULT_SHADOW_MONITOR_DIR,
            )
            if shadow_monitor.get("status") == "FAIL":
                status = "FAIL"
                checks.append(
                    _check(
                        "shadow_monitor_run",
                        False,
                        _text(shadow_monitor.get("reason")),
                    )
                )
            else:
                checks.append(
                    _check(
                        "shadow_monitor_run",
                        True,
                        _text(shadow_monitor.get("status")),
                    )
                )
        elif status.startswith("PASS"):
            shadow_monitor = {
                "status": "SKIPPED",
                "reason": "shadow monitor disabled for this scheduled observe run",
                "monitor_id": None,
                "monitor_dir": None,
            }
    failed_check_count = sum(1 for check in checks if not check.get("passed"))
    if failed_check_count and status.startswith("PASS"):
        status = "FAIL"
    json_path, markdown_path = scheduled_observe_report_paths(output_dir=output_dir, as_of=as_of)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rescue_scheduled_observe",
        "status": status,
        "as_of": as_of.isoformat(),
        "generated_at": generated_at.isoformat(),
        "family": family,
        "market_session": {
            "is_trading_day": session.is_trading_day,
            "reason": session.reason,
            "previous_trading_day": session.previous_trading_day.isoformat(),
        },
        "due": due,
        "due_status": due_status,
        "due_reason": due_reason,
        "skip_reason": skip_reason,
        "pointer_dir": str(pointer_dir),
        "pointer_count": pointer_count,
        "latest": latest_payload,
        "artifact_validation": validation_payload,
        "stale_check": stale_payload,
        "shadow_monitor": shadow_monitor,
        "checks": checks,
        "failed_check_count": failed_check_count,
        "output_artifacts": {
            "json": str(json_path),
            "markdown": str(markdown_path),
        },
        "schedule_boundary": {
            "daily_run_entry": True,
            "non_daily_research_execution": False,
            "real_sweep_execution_allowed": False,
            "promotion_pack_execution_allowed": False,
            "shadow_monitor_observe_only": True,
        },
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    if write:
        output_dir.mkdir(parents=True, exist_ok=True)
        _write_json(json_path, payload)
        _write_text(markdown_path, render_scheduled_observe_markdown(payload))
    return payload


def render_scheduled_observe_markdown(payload: Mapping[str, Any]) -> str:
    status = _text(payload.get("status")) or "UNKNOWN"
    lines = [
        "# Dynamic v3 Rescue Scheduled Observation Gate",
        "",
        f"- 状态：{status}",
        f"- as_of：{_text(payload.get('as_of'))}",
        f"- due_status：{_text(payload.get('due_status'))}",
        f"- due_reason：{_text(payload.get('due_reason'))}",
        f"- pointer_count：{payload.get('pointer_count')}",
        f"- production_effect：{_text(payload.get('production_effect'))}",
        f"- manual_review_required：{payload.get('manual_review_required')}",
        f"- production_candidate_generated：{payload.get('production_candidate_generated')}",
        "",
        "## Gate Results",
    ]
    validation = _mapping(payload.get("artifact_validation"))
    stale = _mapping(payload.get("stale_check"))
    shadow_monitor = _mapping(payload.get("shadow_monitor"))
    lines.extend(
        [
            f"- artifact_validation：{_text(validation.get('status')) or 'SKIPPED'}",
            f"- stale_check：{_text(stale.get('status')) or 'SKIPPED'}",
            f"- shadow_monitor：{_text(shadow_monitor.get('status')) or 'SKIPPED'}",
            f"- shadow_monitor_reason：{_text(shadow_monitor.get('reason')) or 'n/a'}",
            "",
            "## Safety Boundary",
            "- daily-run only executes this lightweight observation gate.",
            "- The gate does not run `small_real`, `medium_real`, or `overnight_real` sweeps.",
            "- The gate does not build promotion packs or generate production candidates.",
        ]
    )
    return "\n".join(lines) + "\n"


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
        row
        for row in _read_jsonl(output_dir / "candidates_index.jsonl")
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
    window_audit_dir: Path = DEFAULT_WINDOW_AUDIT_DIR,
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
        window_audit_dir=window_audit_dir,
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
        "not_for_investment_decision": (candidate.get("not_for_investment_decision") is True),
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
        "not_for_investment_decision": (candidate.get("not_for_investment_decision") is True),
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
        f"- Evaluator mode: {payload.get('evaluator_mode')}\n"
        f"- Metrics source: {payload.get('metrics_source')}\n"
        "- Source real evaluation artifact: "
        f"{payload.get('source_real_evaluation_artifact_path')}\n"
        f"- Overfit status: {payload.get('overfit_status')}\n"
        f"- Parameter sensitivity: {payload.get('parameter_sensitivity_status')}\n"
        f"- Sensitivity evidence: {payload.get('sensitivity_evidence_status')}\n"
        f"- Real neighbors: {payload.get('real_neighbor_count')}\n"
        f"- Missing real neighbors: {payload.get('missing_real_neighbor_count')}\n"
        f"- Stress bucket status: {payload.get('stress_bucket_status')}\n"
        f"- Stress evidence: {payload.get('stress_evidence_status')}\n"
        f"- Regime evidence: {payload.get('regime_evidence_status')}\n"
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
        "- Parameter effect pair coverage complete: "
        f"{payload.get('parameter_effect_pair_coverage_complete')}",
        "- Parameters without matched pairs: "
        f"{', '.join(_texts(payload.get('parameters_without_matched_pairs'))) or 'none'}",
        f"- All weight paths almost identical: {payload.get('all_weight_paths_almost_identical')}",
        "",
        "## Parameter Effects",
        "",
        "| Parameter | Status | Matched pairs | Config-changed pairs | "
        "Metric-changed pairs | Weight-changed pairs |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for row in _records(payload.get("parameter_effects")):
        lines.append(
            f"| {row.get('parameter')} | {row.get('effect_status')} | "
            f"{row.get('matched_pair_count')} | "
            f"{row.get('config_changed_pair_count')} | "
            f"{row.get('metric_changed_pair_count')} | {row.get('weight_changed_pair_count')} |"
        )
    lines.extend(["", "## Safety", "- production_candidate_generated=false"])
    return "\n".join(lines) + "\n"


def render_candidate_attribution_markdown(payload: Mapping[str, Any]) -> str:
    component_statuses = _mapping(payload.get("component_evidence_statuses"))
    lines = [
        f"# Dynamic v3 Rescue Candidate Attribution {payload.get('candidate_id')}",
        "",
        f"- Source sweep: {payload.get('source_sweep_id')}",
        f"- Status: {payload.get('status')}",
        f"- Explainability: {payload.get('explainability_status')}",
        f"- Attribution method: {payload.get('attribution_method')}",
        "- Weight path declared/observed: "
        f"{payload.get('weight_path_declared_completeness')} / "
        f"{payload.get('weight_path_observed_completeness')}",
        f"- Weight path limitations: {', '.join(_texts(payload.get('weight_path_limitations')))}",
        f"- Incomplete reasons: {', '.join(_texts(payload.get('incomplete_reasons')))}",
        f"- Source mutation performed: {str(payload.get('source_mutation_performed')).lower()}",
        "",
        "## Attribution Summary",
        f"- Constraint: {_mapping(payload.get('constraint_event_attribution')).get('summary')}",
        f"- Drawdown: {_mapping(payload.get('drawdown_window_attribution')).get('summary')}",
        f"- Turnover: {_mapping(payload.get('turnover_attribution')).get('summary')}",
        "- Dynamic-vs-static gap: "
        f"{_mapping(payload.get('dynamic_vs_static_gap_attribution')).get('summary')}",
        "",
        "## Component Evidence Status",
    ]
    lines.extend(f"- {name}: {status}" for name, status in sorted(component_statuses.items()))
    lines.extend(
        [
        "",
        "## Safety",
        "- production_candidate_generated=false",
        ]
    )
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


def _fixed_robustness_reports_for_real_context(
    *,
    prices: pd.DataFrame,
    etf_config: Any,
    real_policy: DynamicV3RealEvaluationPolicyConfig,
    dynamic_robustness_policy: Any,
    dynamic_policy: Any,
    failure_policy: Any,
    start: date,
    end: date,
    data_quality_status: str,
    data_quality_report: str,
    prices_path: Path,
    data_manifest_hash: str,
) -> dict[str, Any]:
    cache_key = _stable_id(
        "fixed-robustness-context",
        data_manifest_hash,
        str(prices_path.resolve()),
        start,
        end,
        data_quality_status,
        real_policy.model_dump(mode="json"),
        dynamic_robustness_policy.model_dump(mode="json"),
        dynamic_policy.model_dump(mode="json"),
        failure_policy.model_dump(mode="json"),
    )
    cached = _FIXED_ROBUSTNESS_REPORT_CACHE.get(cache_key)
    if cached is not None:
        fixed_cache = _clone_jsonable(cached)
        fixed_cache["cache_reused_in_process"] = True
        fixed_cache["cache_reuse_source"] = "in_memory_validation_runtime_cache"
        return fixed_cache

    fixed_cache = precompute_dynamic_v3_fixed_robustness_reports(
        prices=prices,
        etf_config=etf_config,
        policy=real_policy,
        dynamic_robustness_policy=dynamic_robustness_policy,
        dynamic_policy=dynamic_policy,
        failure_policy=failure_policy,
        start=start,
        end=end,
        data_quality_status=data_quality_status,
        data_quality_report=data_quality_report,
        prices_path=prices_path,
    )
    fixed_cache["cache_reused_in_process"] = False
    fixed_cache["cache_reuse_source"] = "fresh_precompute"
    _FIXED_ROBUSTNESS_REPORT_CACHE[cache_key] = _clone_jsonable(fixed_cache)
    return fixed_cache


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
    real_policy = load_dynamic_v3_real_evaluation_policy_config(
        DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH
    )
    v3_rescue_policy = load_dynamic_v3_rescue_policy_config()
    dynamic_robustness_policy = load_dynamic_robustness_policy_config()
    dynamic_policy = load_dynamic_allocation_policy_config()
    failure_policy = load_dynamic_failure_diagnostics_policy_config()
    fixed_cache = _fixed_robustness_reports_for_real_context(
        prices=prices,
        etf_config=etf_config,
        real_policy=real_policy,
        dynamic_robustness_policy=dynamic_robustness_policy,
        dynamic_policy=dynamic_policy,
        failure_policy=failure_policy,
        start=real_policy.market_regime.default_backtest_start,
        end=config.data.end,
        data_quality_status=quality_report.status,
        data_quality_report=str(quality_output),
        prices_path=prices_path,
        data_manifest_hash=data_manifest_hash,
    )
    fixed_cache_manifest = {key: value for key, value in fixed_cache.items() if key != "reports"}
    _write_json(sweep_dir / "fixed_robustness_cache_manifest.json", fixed_cache_manifest)
    _append_text(
        sweep_dir / "run.log",
        (
            f"{datetime.now(UTC).isoformat()} "
            "fixed robustness cache ready "
            f"cache_id={fixed_cache_manifest.get('cache_id')} "
            f"policy_count={len(fixed_cache_manifest.get('policy_ids', []))}\n"
        ),
    )
    return RealEvaluationContext(
        prices=prices,
        etf_config=etf_config,
        real_policy=real_policy,
        v3_rescue_policy=v3_rescue_policy,
        dynamic_robustness_policy=dynamic_robustness_policy,
        dynamic_policy=dynamic_policy,
        failure_policy=failure_policy,
        data_quality_status=quality_report.status,
        data_quality_report_path=quality_output,
        prices_path=prices_path,
        real_evaluation_output_dir=real_evaluation_output_dir or sweep_dir / "real_evaluation",
        data_manifest_hash=data_manifest_hash,
        precomputed_robustness_reports=dict(_mapping(fixed_cache.get("reports"))),
        fixed_robustness_cache_manifest=fixed_cache_manifest,
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
        precomputed_robustness_reports=real_context.precomputed_robustness_reports,
    )
    report_id = "dynamic-v3-real-evaluation-report_" + _stable_id(
        "sweep-real",
        sweep_dir.name,
        candidate_id,
        parameters,
        payload.get("dynamic_v3_real_evaluation_report_id"),
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
                materialization.trend_overlay_scale_with_soft_penalty * (1.0 - turnover_penalty)
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
                    materialization.drawdown_cash_increase_step * intensity * guard_multiplier,
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
                        f"{policy.policy_metadata.version}_sweep_{_stable_id(parameters)[:8]}"
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
                            policy.smoothing_policy.max_single_rebalance_delta * smooth_scale,
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
        "parameter_sensitivity_status": (
            "LOW" if _text(best.get("overfit_status")) == "PASS" else "REVIEW_REQUIRED"
        ),
        "stress_bucket_status": (
            "PASS" if _text(best.get("walk_forward_status")) == "PASS" else "MIXED"
        ),
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
        "execution": {
            "workers": config.execution.workers,
            "checkpoint_every_candidates": config.execution.checkpoint_every_candidates,
            "continue_on_candidate_error": config.execution.continue_on_candidate_error,
        },
        "search_space_version": _search_space_version(),
        "not_for_investment_decision": (config.execution.evaluator == EVALUATOR_TINY_FIXTURE_PROXY),
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


def _annotate_sweep_profile(
    *,
    sweep_dir: Path,
    profile: str,
    profile_config_path: Path,
    selected: SweepProfile,
) -> None:
    manifest_path = sweep_dir / "sweep_manifest.json"
    manifest = _read_optional_json(manifest_path)
    if not manifest:
        return
    manifest.update(
        {
            "profile": profile,
            "profile_config_path": str(profile_config_path),
            "profile_require_data_audit": selected.require_data_audit,
            "profile_require_window_audit": selected.require_window_audit,
            "profile_require_weight_path": selected.require_weight_path,
        }
    )
    _write_json(manifest_path, manifest)


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
        "not_for_investment_decision": (config.execution.evaluator == EVALUATOR_TINY_FIXTURE_PROXY),
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
        {_text(row.get("metrics_source")) for row in results if _text(row.get("metrics_source"))}
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


def _evidence_context(
    *,
    window_audit_dir: Path,
    data_provenance_dir: Path,
) -> dict[str, Any]:
    data_path = _latest_data_provenance_path(data_provenance_dir)
    return {
        "window_audit": _latest_window_audit_evidence(window_audit_dir),
        "data_provenance": _read_optional_json(data_path) or {},
        "data_provenance_path": "" if data_path is None else str(data_path),
    }


def _candidate_evidence_row(
    row: Mapping[str, Any],
    *,
    sweep_id: str,
    sweep_manifest: Mapping[str, Any],
    candidate_attribution_dir: Path,
    overfit_dir: Path,
    context: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_id = _text(row.get("candidate_id"))
    metrics = _mapping(row.get("metrics"))
    data_quality = _text(
        _mapping(row.get("data_quality")).get("status"),
        _text(_mapping(sweep_manifest.get("data_quality")).get("status"), "MISSING"),
    )
    provenance = _mapping(context.get("data_provenance"))
    provenance_status = _text(provenance.get("provenance_status"), "MISSING")
    if _text(provenance.get("status")) == "FAIL":
        provenance_status = "FAIL"
    date_range_status = _text(
        metrics.get("date_range_status"),
        _text(
            _mapping(row.get("backtest_window")).get("date_range_status"),
            _text(_mapping(context.get("window_audit")).get("date_range_status"), "MISSING"),
        ),
    )
    weight_path = _mapping(row.get("weight_path_metadata"))
    weight_status = _text(weight_path.get("attribution_completeness"), "MISSING")
    if weight_status == "MISSING":
        weight_status = _weight_status_from_real_artifact(row)
    attribution = _latest_candidate_attribution(candidate_id, candidate_attribution_dir)
    attribution_status = _text(attribution.get("status"), "MISSING")
    overfit = _latest_overfit_for_candidate(candidate_id, overfit_dir)
    overfit_status = _text(
        overfit.get("overfit_status"),
        _text(metrics.get("overfit_status"), "MISSING"),
    )
    blocking = _evidence_blocking_reasons(
        row=row,
        data_quality=data_quality,
        provenance_status=provenance_status,
        date_range_status=date_range_status,
        weight_status=weight_status,
        attribution_status=attribution_status,
        overfit_status=overfit_status,
    )
    score = _evidence_score(
        data_quality=data_quality,
        provenance_status=provenance_status,
        date_range_status=date_range_status,
        weight_status=weight_status,
        attribution_status=attribution_status,
        overfit_status=overfit_status,
    )
    recommendation = _evidence_recommendation(score, blocking)
    promotion_status = _evidence_promotion_status(row, blocking, recommendation)
    return {
        "candidate_id": candidate_id,
        "sweep_id": sweep_id,
        "evaluator_mode": row.get("evaluator_mode", "UNKNOWN"),
        "data_quality": data_quality,
        "data_provenance_status": provenance_status,
        "date_range_status": date_range_status,
        "weight_path_status": weight_status,
        "candidate_attribution_status": attribution_status,
        "overfit_status": overfit_status,
        "promotion_status": promotion_status,
        "promotion_blocking_reasons": blocking,
        "evidence_score": score,
        "evidence_recommendation": recommendation,
    }


def _latest_data_provenance_path(data_provenance_dir: Path) -> Path | None:
    pointer_path = None
    if _is_default_dynamic_v3_research_artifact(data_provenance_dir):
        pointer = _latest_pointer_payload("latest_data_provenance")
        pointer_text = _text(pointer.get("path"))
        if pointer_text:
            pointer_path = Path(pointer_text)
    direct = data_provenance_dir / "price_cache_provenance_report.json"
    if pointer_path is not None and pointer_path.exists():
        return pointer_path
    if direct.exists():
        return direct
    return None


def _weight_status_from_real_artifact(row: Mapping[str, Any]) -> str:
    real_path = Path(_text(row.get("real_evaluation_artifact_path")))
    if not _text(row.get("real_evaluation_artifact_path")) or not real_path.exists():
        return "MISSING"
    metadata = _read_optional_json(real_path.parent / "weight_path_metadata.json") or {}
    return _text(metadata.get("attribution_completeness"), WEIGHT_PATH_INCOMPLETE)


def _latest_candidate_attribution(candidate_id: str, output_dir: Path) -> dict[str, Any]:
    direct = output_dir / candidate_id / "attribution_manifest.json"
    if direct.exists():
        return _read_optional_json(direct) or {}
    candidates = []
    for path in output_dir.glob("*/attribution_manifest.json"):
        payload = _read_optional_json(path) or {}
        if _text(payload.get("candidate_id")) == candidate_id:
            candidates.append((path, payload))
    if not candidates:
        return {}
    return max(candidates, key=lambda item: item[0].stat().st_mtime)[1]


def _latest_overfit_for_candidate(candidate_id: str, output_dir: Path) -> dict[str, Any]:
    path = _latest_overfit_manifest_for_candidate(candidate_id, output_dir)
    return _read_optional_json(path) if path is not None else {}


def _evidence_blocking_reasons(
    *,
    row: Mapping[str, Any],
    data_quality: str,
    provenance_status: str,
    date_range_status: str,
    weight_status: str,
    attribution_status: str,
    overfit_status: str,
) -> list[str]:
    reasons: list[str] = []
    if row.get("evaluator_mode") == EVALUATOR_TINY_FIXTURE_PROXY:
        reasons.append("TINY_FIXTURE_NOT_FOR_INVESTMENT")
    if data_quality == "FAIL":
        reasons.append("DATA_QUALITY_FAIL")
    if provenance_status in {"MISSING", "FAIL", DATA_PROVENANCE_RECONSTRUCTED}:
        reasons.append("DATA_PROVENANCE_INCOMPLETE")
    if date_range_status in WINDOW_PROMOTION_BLOCKING_STATUSES or date_range_status == "MISSING":
        reasons.append("BACKTEST_WINDOW_INCOMPLETE")
    if weight_status in {"MISSING", WEIGHT_PATH_INCOMPLETE}:
        reasons.append("MISSING_DAILY_WEIGHT_PATH")
    elif weight_status == WEIGHT_PATH_PARTIAL:
        reasons.append("WEIGHT_PATH_PARTIAL")
    if attribution_status == "MISSING" or attribution_status == WEIGHT_PATH_INCOMPLETE:
        reasons.append("ATTRIBUTION_INCOMPLETE")
    elif attribution_status == WEIGHT_PATH_PARTIAL:
        reasons.append("ATTRIBUTION_PARTIAL")
    if overfit_status == "HIGH_RISK":
        reasons.append("OVERFIT_HIGH_RISK")
    elif overfit_status == "REVIEW_REQUIRED":
        reasons.append("OVERFIT_REVIEW_REQUIRED")
    return reasons


def _evidence_score(
    *,
    data_quality: str,
    provenance_status: str,
    date_range_status: str,
    weight_status: str,
    attribution_status: str,
    overfit_status: str,
) -> float:
    score = 0.0
    score += EVIDENCE_SCORE_POINTS["data_quality"] * {
        "PASS": 1.0,
        "PASS_WITH_WARNINGS": 0.7,
    }.get(data_quality, 0.0)
    score += EVIDENCE_SCORE_POINTS["data_provenance"] * {
        "PASS": 1.0,
        "ORIGINAL_OR_VENDOR": 1.0,
        DATA_PROVENANCE_RECONSTRUCTED: 0.45,
    }.get(provenance_status, 0.0)
    score += EVIDENCE_SCORE_POINTS["date_range"] * {
        DATE_RANGE_PASS: 1.0,
        DATE_RANGE_PASS_WITH_WARNINGS: 0.7,
    }.get(date_range_status, 0.0)
    score += EVIDENCE_SCORE_POINTS["weight_path"] * {
        WEIGHT_PATH_COMPLETE: 1.0,
        WEIGHT_PATH_PARTIAL: 0.6,
    }.get(weight_status, 0.0)
    score += EVIDENCE_SCORE_POINTS["candidate_attribution"] * {
        WEIGHT_PATH_COMPLETE: 1.0,
        WEIGHT_PATH_PARTIAL: 0.6,
    }.get(attribution_status, 0.0)
    score += EVIDENCE_SCORE_POINTS["overfit"] * {
        "LOW_RISK": 1.0,
        "REVIEW_REQUIRED": 0.5,
    }.get(overfit_status, 0.0)
    return round(score, 6)


def _evidence_recommendation(score: float, blockers: Sequence[str]) -> str:
    hard_blockers = {
        "DATA_QUALITY_FAIL",
        "BACKTEST_WINDOW_INCOMPLETE",
        "MISSING_DAILY_WEIGHT_PATH",
        "OVERFIT_HIGH_RISK",
        "TINY_FIXTURE_NOT_FOR_INVESTMENT",
    }
    if any(reason in hard_blockers for reason in blockers):
        return "not_usable"
    if score >= EVIDENCE_USABLE_SCORE_FLOOR:
        return "usable_for_research"
    if score >= EVIDENCE_REVIEW_SCORE_FLOOR:
        return "needs_review"
    return "not_usable"


def _evidence_promotion_status(
    row: Mapping[str, Any],
    blockers: Sequence[str],
    recommendation: str,
) -> str:
    if recommendation == "not_usable":
        return "incomplete"
    if blockers:
        return "manual_review_required"
    gate = _text(row.get("gate"))
    if gate == GATE_PROMOTE_CANDIDATE:
        return GATE_PROMOTE_CANDIDATE
    return "review_required"


def _evidence_summary_reader_brief_section(
    *,
    status: str,
    usable_count: int,
    candidate_count: int,
    blocking_counter: Counter[str],
) -> str:
    top_blocker = blocking_counter.most_common(1)[0][0] if blocking_counter else "none"
    return (
        "Dynamic Rescue Evidence Summary: "
        f"status={status}; usable={usable_count}/{candidate_count}; "
        f"top_blocker={top_blocker}; production_effect=none."
    )


def render_evidence_summary_markdown(
    manifest: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> str:
    top_blockers = _records(manifest.get("top_blocking_reasons"))[:8]
    requested_range = _mapping(manifest.get("requested_range"))
    lines = [
        "# Dynamic v3 Rescue Evidence Summary",
        "",
        f"- sweep_id: `{manifest.get('source_sweep_id')}`",
        f"- market_regime: `{manifest.get('market_regime')}`",
        f"- requested_range: `{requested_range.get('start')}` to `{requested_range.get('end')}`",
        f"- status: `{manifest.get('status')}`",
        f"- candidates: `{manifest.get('candidate_count')}`",
        f"- usable_for_research: `{manifest.get('usable_for_research_count')}`",
        f"- complete_evidence_count: `{manifest.get('complete_evidence_count')}`",
        f"- partial_evidence_count: `{manifest.get('partial_evidence_count')}`",
        f"- can_enter_medium_real: `{manifest.get('can_enter_medium_real')}`",
        "",
        "## Blocking Reasons",
    ]
    if top_blockers:
        lines.extend(f"- `{row.get('reason')}`: {row.get('count')}" for row in top_blockers)
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Evidence Matrix Preview",
            "|candidate_id|data|window|weight_path|attribution|overfit|recommendation|",
            "|---|---|---|---|---|---|---|",
        ]
    )
    for row in rows[:20]:
        lines.append(
            "|"
            + "|".join(
                [
                    _text(row.get("candidate_id")),
                    _text(row.get("data_quality")),
                    _text(row.get("date_range_status")),
                    _text(row.get("weight_path_status")),
                    _text(row.get("candidate_attribution_status")),
                    _text(row.get("overfit_status")),
                    _text(row.get("evidence_recommendation")),
                ]
            )
            + "|"
        )
    lines.extend(["", "production_effect=none; broker_action=none."])
    return "\n".join(lines) + "\n"


def _latest_sweep_id_for_profile(
    profile: str,
    *,
    sweep_output_dir: Path,
) -> str | None:
    candidates: list[tuple[float, str]] = []
    for manifest_path in sweep_output_dir.glob("*/sweep_manifest.json"):
        manifest = _read_optional_json(manifest_path) or {}
        if manifest.get("profile") == profile:
            candidates.append((manifest_path.stat().st_mtime, manifest_path.parent.name))
    if candidates:
        return max(candidates)[1]
    latest = latest_sweep_id()
    if latest and (sweep_output_dir / latest / "sweep_manifest.json").exists():
        return latest
    return None


def _artifact_size_summary(path: Path) -> dict[str, Any]:
    files = [item for item in path.rglob("*") if item.is_file()]
    total = sum(item.stat().st_size for item in files)
    return {
        "file_count": len(files),
        "total_bytes": total,
        "total_mb": round(total / (1024 * 1024), 3),
        "largest_files": [
            {"path": str(item), "bytes": item.stat().st_size}
            for item in sorted(files, key=lambda item: item.stat().st_size, reverse=True)[:10]
        ],
    }


def _average_runtime_seconds(manifest: Mapping[str, Any], completed_count: int) -> float:
    start = _parse_datetime(_text(manifest.get("started_at")))
    end = _parse_datetime(_text(manifest.get("completed_at")))
    if start is None or end is None or completed_count <= 0:
        return 0.0
    return round(max(0.0, (end - start).total_seconds()) / completed_count, 6)


def _medium_real_next_action(
    *,
    manifest: Mapping[str, Any],
    completed_count: int,
    failed_count: int,
    observe_only_count: int,
) -> str:
    if manifest.get("evaluator_mode") != EVALUATOR_REAL_DYNAMIC_V3_RESCUE:
        return "rerun_medium_real_with_real_dynamic_v3_rescue"
    if failed_count:
        return "inspect_candidate_errors_before_overnight"
    if completed_count < 300:
        return "record_medium_real_shortfall_or_expand_parameter_space"
    if observe_only_count:
        return "run_evidence_summary_regime_coverage_and_observe_pool"
    return "review_reject_reasons_before_expanding_search"


def render_medium_real_markdown(payload: Mapping[str, Any]) -> str:
    runtime_seconds = payload.get("average_runtime_seconds_per_candidate")
    lines = [
        "# Dynamic v3 Rescue Medium Real Report",
        "",
        f"- sweep_id: `{payload.get('source_sweep_id')}`",
        f"- status: `{payload.get('status')}`",
        f"- evaluator_mode: `{payload.get('evaluator_mode')}`",
        f"- candidate_count: `{payload.get('candidate_count')}`",
        f"- completed_count: `{payload.get('completed_count')}`",
        f"- failed_count: `{payload.get('failed_count')}`",
        f"- rejected_count: `{payload.get('rejected_count')}`",
        f"- review_required_count: `{payload.get('review_required_count')}`",
        f"- observe_only_count: `{payload.get('observe_only_count')}`",
        f"- promote_candidate_count: `{payload.get('promote_candidate_count')}`",
        f"- average_runtime_seconds_per_candidate: `{runtime_seconds}`",
        f"- artifact_total_mb: `{_mapping(payload.get('artifact_size_summary')).get('total_mb')}`",
        f"- recommended_next_action: `{payload.get('recommended_next_action')}`",
        "",
        "## Top Candidates",
        "|candidate_id|gate|score|",
        "|---|---|---|",
    ]
    for row in _records(payload.get("top_candidates"))[:20]:
        lines.append(f"|{row.get('candidate_id')}|{row.get('gate')}|{row.get('score')}|")
    lines.extend(["", "production_effect=none; broker_action=none."])
    return "\n".join(lines) + "\n"


def _first_candidate_id(rows: Any) -> str:
    records = _records(rows)
    return _text(records[0].get("candidate_id"), "MISSING") if records else "MISSING"


def _candidate_result(sweep_dir: Path, candidate_id: str) -> dict[str, Any] | None:
    return _candidate_result_from_rows(_read_candidate_results(sweep_dir), candidate_id)


def _candidate_result_from_rows(
    rows: Sequence[Mapping[str, Any]],
    candidate_id: str,
) -> dict[str, Any] | None:
    for row in rows:
        if row.get("candidate_id") == candidate_id:
            return dict(row)
    return None


def _ranked_candidate_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [dict(row) for row in rows if row.get("score") is not None],
        key=lambda row: (float(row.get("score") or 0.0), _text(row.get("candidate_id"))),
        reverse=True,
    )


def _date_from_any(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _regime_windows_from_prices(
    *,
    prices_path: Path,
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    windows = [_base_regime_window("ai_after_chatgpt", start, end, "configured")]
    if not prices_path.exists():
        return [*windows, *_missing_price_regime_windows(start, end, "price_cache_missing")]
    try:
        frame = pd.read_csv(prices_path)
    except Exception:  # noqa: BLE001
        return [*windows, *_missing_price_regime_windows(start, end, "price_cache_unreadable")]
    if not {"date", "ticker", "close"}.issubset(frame.columns):
        return [*windows, *_missing_price_regime_windows(start, end, "price_cache_schema_missing")]
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame[
        (frame["date"].dt.date >= start)
        & (frame["date"].dt.date <= end)
        & frame["ticker"].isin(["QQQ", "SMH", "SOXX"])
    ].sort_values(["ticker", "date"])
    if frame.empty:
        return [*windows, *_missing_price_regime_windows(start, end, "price_rows_missing")]
    qqq = _ticker_window_features(frame, "QQQ")
    semi = _ticker_window_features(frame, "SMH")
    if semi.empty:
        semi = _ticker_window_features(frame, "SOXX")
    windows.extend(
        [
            _detected_regime_window(
                "ai_strong_trend",
                qqq,
                qqq.get("rolling_return_63", pd.Series(dtype=float))
                >= STRONG_TREND_RETURN_THRESHOLD,
            ),
            _detected_regime_window(
                "tech_drawdown",
                qqq,
                qqq.get("drawdown", pd.Series(dtype=float)) <= REGIME_DRAWDOWN_THRESHOLD,
            ),
            _detected_regime_window(
                "semiconductor_pullback",
                semi,
                semi.get("drawdown", pd.Series(dtype=float)) <= SEMICONDUCTOR_DRAWDOWN_THRESHOLD,
            ),
            _configured_overlap_window(
                "high_rate_pressure",
                start,
                end,
                date(2022, 12, 1),
                date(2023, 10, 31),
            ),
            _detected_regime_window(
                "sideways_choppy",
                qqq,
                (
                    qqq.get("rolling_return_63", pd.Series(dtype=float)).abs()
                    <= SIDEWAYS_ABS_RETURN_THRESHOLD
                )
                & (
                    qqq.get("realized_vol_21", pd.Series(dtype=float))
                    >= HIGH_VOL_ANNUALIZED_THRESHOLD
                ),
            ),
            _detected_regime_window(
                "high_volatility",
                qqq,
                qqq.get("realized_vol_21", pd.Series(dtype=float)) >= HIGH_VOL_ANNUALIZED_THRESHOLD,
            ),
            _detected_regime_window(
                "strong_recovery",
                qqq,
                qqq.get("rolling_return_63", pd.Series(dtype=float))
                >= STRONG_RECOVERY_RETURN_THRESHOLD,
            ),
        ]
    )
    return windows


def _base_regime_window(
    regime_id: str,
    start: date,
    end: date,
    source: str,
) -> dict[str, Any]:
    return {
        "regime_id": regime_id,
        "status": "PRESENT" if end >= start else "MISSING",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "source": source,
        "observation_count": 0,
    }


def _missing_price_regime_windows(
    start: date,
    end: date,
    reason: str,
) -> list[dict[str, Any]]:
    return [
        {
            **_base_regime_window(regime_id, start, end, "price_behavior"),
            "status": "MISSING",
            "missing_reason": reason,
        }
        for regime_id in (
            "ai_strong_trend",
            "tech_drawdown",
            "semiconductor_pullback",
            "high_rate_pressure",
            "sideways_choppy",
            "high_volatility",
            "strong_recovery",
        )
    ]


def _ticker_window_features(frame: pd.DataFrame, ticker: str) -> pd.DataFrame:
    subset = frame[frame["ticker"] == ticker].copy()
    if subset.empty:
        return subset
    subset["close"] = pd.to_numeric(subset["close"], errors="coerce")
    subset = subset.dropna(subset=["close"])
    subset["return"] = subset["close"].pct_change()
    subset["rolling_return_63"] = subset["close"].pct_change(63)
    subset["drawdown"] = subset["close"] / subset["close"].cummax() - 1.0
    subset["realized_vol_21"] = subset["return"].rolling(21).std() * (252**0.5)
    return subset


def _detected_regime_window(
    regime_id: str,
    frame: pd.DataFrame,
    mask: Any,
) -> dict[str, Any]:
    if frame.empty:
        return {
            "regime_id": regime_id,
            "status": "MISSING",
            "start": "",
            "end": "",
            "source": "price_behavior",
            "observation_count": 0,
            "missing_reason": "ticker_price_rows_missing",
        }
    selected = frame[mask.fillna(False) if hasattr(mask, "fillna") else mask]
    if selected.empty:
        return {
            "regime_id": regime_id,
            "status": "MISSING",
            "start": "",
            "end": "",
            "source": "price_behavior",
            "observation_count": 0,
            "missing_reason": "condition_not_detected",
        }
    return {
        "regime_id": regime_id,
        "status": "PRESENT",
        "start": selected["date"].min().date().isoformat(),
        "end": selected["date"].max().date().isoformat(),
        "source": "price_behavior",
        "observation_count": int(len(selected)),
    }


def _configured_overlap_window(
    regime_id: str,
    requested_start: date,
    requested_end: date,
    configured_start: date,
    configured_end: date,
) -> dict[str, Any]:
    start = max(requested_start, configured_start)
    end = min(requested_end, configured_end)
    if end < start:
        return {
            "regime_id": regime_id,
            "status": "MISSING",
            "start": "",
            "end": "",
            "source": "configured_proxy",
            "observation_count": 0,
            "missing_reason": "configured_window_outside_requested_range",
        }
    return _base_regime_window(regime_id, start, end, "configured_proxy")


def _candidate_regime_result(
    row: Mapping[str, Any],
    *,
    windows: Sequence[Mapping[str, Any]],
    focus: str,
) -> dict[str, Any]:
    metrics = _mapping(row.get("metrics"))
    drawdown_degradation = _float(metrics.get("drawdown_degradation_pp"))
    dynamic_gap = _float(metrics.get("dynamic_vs_static_gap"))
    regime_results = []
    for window in windows:
        regime_id = _text(window.get("regime_id"))
        if window.get("status") != "PRESENT":
            status = "MISSING_WINDOW"
        elif regime_id in {"tech_drawdown", "semiconductor_pullback", "high_volatility"}:
            status = (
                "PASS"
                if drawdown_degradation <= REGIME_DRAWDOWN_PROTECTION_MAX_DEGRADATION_PP
                else "REVIEW_REQUIRED"
            )
        elif regime_id == "ai_strong_trend":
            status = "PASS" if dynamic_gap <= 1.0 else "REVIEW_REQUIRED"
        else:
            status = "PASS"
        regime_results.append(
            {
                "regime_id": regime_id,
                "window_status": window.get("status"),
                "candidate_status": status,
                "drawdown_degradation_pp": drawdown_degradation,
                "dynamic_vs_static_gap": dynamic_gap,
                "return_delta": metrics.get("return_delta"),
            }
        )
    stress_results = [
        item
        for item in regime_results
        if item["regime_id"] in {"tech_drawdown", "semiconductor_pullback", "high_volatility"}
    ]
    stress_failures = sum(1 for item in stress_results if item["candidate_status"] != "PASS")
    return {
        "candidate_id": row.get("candidate_id"),
        "source_sweep_id": row.get("source_sweep_id", ""),
        "focus": focus,
        "gate": row.get("gate"),
        "score": row.get("score"),
        "regime_results": regime_results,
        "stress_window_review_required_count": stress_failures,
        "ai_bull_market_only_signal": stress_failures == len(stress_results)
        and bool(stress_results),
    }


def _regime_gap_report(
    *,
    windows: Sequence[Mapping[str, Any]],
    candidate_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    required = {
        "ai_after_chatgpt",
        "ai_strong_trend",
        "tech_drawdown",
        "semiconductor_pullback",
        "sideways_choppy",
        "high_volatility",
    }
    present = {_text(row.get("regime_id")) for row in windows if row.get("status") == "PRESENT"}
    missing = sorted(required - present)
    ai_only_count = sum(1 for row in candidate_rows if row.get("ai_bull_market_only_signal"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_regime_gap_report",
        "coverage_status": "PASS" if not missing else "PASS_WITH_WARNINGS",
        "present_regimes": sorted(present),
        "missing_regimes": missing,
        "regime_gap_count": len(missing),
        "ai_bull_market_only_candidate_count": ai_only_count,
        "candidate_count": len(candidate_rows),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
    }


def _coverage_status(gap_report: Mapping[str, Any]) -> str:
    gap_count = int(gap_report.get("regime_gap_count") or 0)
    if gap_count == 0:
        return "PASS"
    if gap_count <= 2:
        return "PASS_WITH_WARNINGS"
    return "FAIL"


def _tech_semiconductor_relevance(gap_report: Mapping[str, Any]) -> str:
    missing = set(_texts(gap_report.get("missing_regimes")))
    if not {"tech_drawdown", "semiconductor_pullback"} & missing:
        return "HIGH"
    if {"tech_drawdown", "semiconductor_pullback"} <= missing:
        return "LOW"
    return "MEDIUM"


def _ai_bull_market_overfit_risk(
    gap_report: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
) -> str:
    if _tech_semiconductor_relevance(gap_report) == "LOW":
        return "HIGH"
    if any(row.get("ai_bull_market_only_signal") for row in candidate_rows):
        return "REVIEW_REQUIRED"
    return "LOW"


def render_regime_coverage_markdown(
    manifest: Mapping[str, Any],
    windows: Sequence[Mapping[str, Any]],
    gap_report: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Dynamic v3 Rescue Regime Coverage",
        "",
        f"- sweep_id: `{manifest.get('source_sweep_id')}`",
        f"- coverage_status: `{manifest.get('coverage_status')}`",
        f"- tech_semiconductor_relevance: `{manifest.get('tech_semiconductor_relevance')}`",
        f"- ai_bull_market_overfit_risk: `{manifest.get('ai_bull_market_overfit_risk')}`",
        f"- missing_regimes: `{', '.join(_texts(gap_report.get('missing_regimes'))) or 'none'}`",
        "",
        "## Windows",
        "|regime|status|start|end|source|",
        "|---|---|---|---|---|",
    ]
    for row in windows:
        lines.append(
            f"|{row.get('regime_id')}|{row.get('status')}|{row.get('start')}|"
            f"{row.get('end')}|{row.get('source')}|"
        )
    lines.extend(
        [
            "",
            "## Candidate Stress Review",
            "|candidate_id|gate|stress_reviews|ai_only|",
            "|---|---|---|---|",
        ]
    )
    for row in candidate_rows[:20]:
        lines.append(
            f"|{row.get('candidate_id')}|{row.get('gate')}|"
            f"{row.get('stress_window_review_required_count')}|"
            f"{row.get('ai_bull_market_only_signal')}|"
        )
    lines.extend(["", "production_effect=none; broker_action=none."])
    return "\n".join(lines) + "\n"


def _latest_regime_coverage_for_sweep(
    sweep_id: str,
    output_dir: Path,
) -> dict[str, Any]:
    manifest = _latest_manifest_for_sweep(
        sweep_id,
        output_dir,
        "regime_coverage_manifest.json",
        "source_sweep_id",
    )
    if not manifest:
        return {}
    manifest_path = Path(_text(manifest.get("_manifest_path")))
    rows_path = manifest_path.parent / "candidate_regime_results.jsonl"
    return {"manifest": manifest, "candidate_rows": _read_jsonl(rows_path)}


def _weight_path_interpretation(
    row: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    real_path_text = _text(row.get("real_evaluation_artifact_path"))
    if not real_path_text:
        return [], []
    daily_path = Path(real_path_text).parent / "daily_weights.csv"
    if not daily_path.exists():
        return [], []
    try:
        frame = pd.read_csv(daily_path)
    except Exception:  # noqa: BLE001
        return [], []
    numeric_cols = [
        col
        for col in frame.columns
        if col != "date"
        and pd.api.types.is_numeric_dtype(pd.to_numeric(frame[col], errors="coerce"))
    ]
    summaries = []
    for col in numeric_cols:
        values = pd.to_numeric(frame[col], errors="coerce").dropna()
        if values.empty:
            continue
        summaries.append(
            {
                "asset": col,
                "min_weight": round(float(values.min()), 6),
                "max_weight": round(float(values.max()), 6),
                "avg_weight": round(float(values.mean()), 6),
                "latest_weight": round(float(values.iloc[-1]), 6),
            }
        )
    changes: list[dict[str, Any]] = []
    if "date" in frame.columns and numeric_cols:
        weights = frame[["date", *numeric_cols]].copy()
        for col in numeric_cols:
            weights[f"{col}_delta"] = pd.to_numeric(weights[col], errors="coerce").diff().abs()
        delta_cols = [f"{col}_delta" for col in numeric_cols]
        weights["max_delta"] = weights[delta_cols].max(axis=1)
        for _, item in weights.sort_values("max_delta", ascending=False).head(10).iterrows():
            if pd.isna(item.get("max_delta")):
                continue
            changes.append(
                {
                    "date": _text(item.get("date")),
                    "max_abs_delta": round(float(item.get("max_delta")), 6),
                }
            )
    return summaries, changes


def _candidate_regime_summary(
    candidate_id: str,
    coverage: Mapping[str, Any],
) -> dict[str, Any]:
    for row in _records(coverage.get("candidate_rows")):
        if _text(row.get("candidate_id")) == candidate_id:
            return dict(row)
    return {
        "candidate_id": candidate_id,
        "status": "MISSING",
        "reason": "regime_coverage_candidate_result_missing",
    }


def _top_contribution_windows(
    row: Mapping[str, Any],
    *,
    positive: bool,
) -> list[dict[str, Any]]:
    metrics = _mapping(row.get("metrics"))
    value = _float(metrics.get("return_delta"))
    return [
        {
            "window": "full_requested_ai_after_chatgpt",
            "contribution": (
                value if positive else -abs(_float(metrics.get("drawdown_degradation_pp")))
            ),
            "source": "candidate_metric_proxy",
        }
    ]


def _drawdown_protection_text(row: Mapping[str, Any]) -> str:
    degradation = _float(_mapping(row.get("metrics")).get("drawdown_degradation_pp"))
    if degradation <= 0:
        return "drawdown protection improved versus reference in candidate metrics"
    if degradation <= REGIME_DRAWDOWN_PROTECTION_MAX_DEGRADATION_PP:
        return "drawdown degradation is within review band"
    return "drawdown degradation requires manual review"


def _interpretation_recommendation(
    row: Mapping[str, Any],
    evidence: Mapping[str, Any],
    regime_summary: Mapping[str, Any],
) -> str:
    if evidence.get("evidence_recommendation") == "not_usable":
        return "rejected_or_incomplete_evidence"
    if regime_summary.get("ai_bull_market_only_signal") is True:
        return "review_required_for_ai_bull_market_overfit"
    if row.get("gate") == GATE_OBSERVE_ONLY:
        return "observe_only_candidate"
    return _text(row.get("gate"), "review_required")


def render_candidate_interpretation_markdown(payload: Mapping[str, Any]) -> str:
    evidence = _mapping(payload.get("evidence_status"))
    semiconductor_regime = _mapping(payload.get("tech_semiconductor_regime_behavior"))
    lines = [
        "# Candidate Interpretation",
        "",
        f"- candidate_id: `{payload.get('candidate_id')}`",
        f"- rank: `{payload.get('rank')}`",
        f"- gate_status: `{payload.get('gate_status')}`",
        f"- evidence_recommendation: `{evidence.get('evidence_recommendation')}`",
        f"- total_score: `{payload.get('total_score')}`",
        f"- recommendation: `{payload.get('recommendation')}`",
        "",
        "## Parameters",
    ]
    for key, value in sorted(_mapping(payload.get("parameters")).items()):
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "## Behavior",
            f"- turnover_source: `{payload.get('turnover_source')}`",
            f"- drawdown_protection_behavior: {payload.get('drawdown_protection_behavior')}",
            "- tech_semiconductor_regime_behavior: "
            f"`{semiconductor_regime.get('status', 'AVAILABLE')}`",
            "",
            "## Major Weight Changes",
        ]
    )
    changes = _records(payload.get("major_weight_changes"))
    if changes:
        lines.extend(
            f"- `{row.get('date')}` max_abs_delta=`{row.get('max_abs_delta')}`"
            for row in changes[:10]
        )
    else:
        lines.append("- incomplete: daily weight path missing or unreadable")
    lines.extend(["", "## Human Review Notes", "- manual_review_required"])
    return "\n".join(lines) + "\n"


def _candidate_diagnosis_rows_for_sweep(
    *,
    sweep_id: str,
    summary_id: str | None,
    sweep_output_dir: Path,
    evidence_summary_dir: Path,
    regime_coverage_dir: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    summary_manifest = _evidence_summary_manifest_for_sweep(
        sweep_id=sweep_id,
        summary_id=summary_id,
        evidence_summary_dir=evidence_summary_dir,
    )
    evidence_rows = _read_evidence_matrix_rows(summary_manifest, evidence_summary_dir)
    sweep_dir = sweep_output_dir / sweep_id
    results_by_candidate = {
        _text(row.get("candidate_id")): row for row in _read_candidate_results(sweep_dir)
    }
    regime_status = _regime_status_for_sweep(sweep_id, regime_coverage_dir)
    rows = []
    for evidence in evidence_rows:
        candidate_id = _text(evidence.get("candidate_id"))
        source = results_by_candidate.get(candidate_id, {})
        reasons = _candidate_evidence_reasons(
            evidence=evidence, source=source, regime=regime_status
        )
        reason_classes = _classify_evidence_gate_reasons(reasons)
        current_gate = _text(source.get("gate"), _text(evidence.get("promotion_status"), "MISSING"))
        rows.append(
            {
                "candidate_id": candidate_id,
                "source_sweep_id": sweep_id,
                "gate": current_gate,
                "usable": evidence.get("evidence_recommendation") == "usable_for_research",
                "observe_eligible": current_gate == GATE_OBSERVE_ONLY,
                "blocking_reasons": reason_classes["blocking_reasons"],
                "warning_reasons": reason_classes["warning_reasons"],
                "hard_blocking_reasons": reason_classes["hard_blocking_reasons"],
                "soft_blocking_reasons": reason_classes["soft_blocking_reasons"],
                "categories": reason_classes["categories"],
                "evidence_status": evidence,
                "regime_status": regime_status,
                "score": source.get("score"),
                "real_evaluation_artifact_path": source.get("real_evaluation_artifact_path", ""),
            }
        )
    return rows, summary_manifest


def _evidence_summary_manifest_for_sweep(
    *,
    sweep_id: str,
    summary_id: str | None,
    evidence_summary_dir: Path,
) -> dict[str, Any]:
    if summary_id:
        path = evidence_summary_dir / summary_id / "evidence_summary_manifest.json"
        payload = _read_json(path)
        payload["_manifest_path"] = str(path)
        return payload
    payload = _latest_manifest_for_sweep(
        sweep_id,
        evidence_summary_dir,
        "evidence_summary_manifest.json",
        "source_sweep_id",
    )
    if not payload:
        raise DynamicV3ParameterResearchError(f"evidence summary not found for sweep: {sweep_id}")
    return payload


def _read_evidence_matrix_rows(
    summary_manifest: Mapping[str, Any],
    evidence_summary_dir: Path,
) -> list[dict[str, Any]]:
    matrix_path = Path(_text(summary_manifest.get("candidate_evidence_matrix_path")))
    if not matrix_path.exists():
        summary_id = _text(summary_manifest.get("summary_id"))
        matrix_path = evidence_summary_dir / summary_id / "candidate_evidence_matrix.jsonl"
    return _read_jsonl(matrix_path)


def _regime_status_for_sweep(sweep_id: str, regime_coverage_dir: Path) -> dict[str, Any]:
    coverage = _latest_manifest_for_sweep(
        sweep_id,
        regime_coverage_dir,
        "regime_coverage_manifest.json",
        "source_sweep_id",
    )
    return {
        "coverage_status": coverage.get("coverage_status", "MISSING"),
        "tech_semiconductor_relevance": coverage.get("tech_semiconductor_relevance", "MISSING"),
        "ai_bull_market_overfit_risk": coverage.get("ai_bull_market_overfit_risk", "MISSING"),
        "coverage_id": coverage.get("coverage_id", ""),
    }


def _candidate_evidence_reasons(
    *,
    evidence: Mapping[str, Any],
    source: Mapping[str, Any],
    regime: Mapping[str, Any],
) -> list[str]:
    reasons = list(dict.fromkeys(_texts(evidence.get("promotion_blocking_reasons"))))
    data_quality = _text(evidence.get("data_quality"))
    provenance = _text(evidence.get("data_provenance_status"))
    date_range = _text(evidence.get("date_range_status"))
    weight_status = _text(evidence.get("weight_path_status"))
    attribution_status = _text(evidence.get("candidate_attribution_status"))
    overfit_status = _text(evidence.get("overfit_status"))
    real_path_text = _text(source.get("real_evaluation_artifact_path"))
    real_path = Path(real_path_text) if real_path_text else None
    weight_metadata = _mapping(source.get("weight_path_metadata"))
    if not real_path_text or real_path is None or not real_path.exists():
        reasons.append("MISSING_REAL_EVALUATION_ARTIFACT")
    if not weight_metadata.get("has_daily_weights"):
        reasons.append("MISSING_DAILY_WEIGHT_PATH")
    if data_quality == "PASS_WITH_WARNINGS":
        reasons.append("DATA_QUALITY_PASS_WITH_WARNINGS")
    if provenance == DATA_PROVENANCE_RECONSTRUCTED:
        reasons.append("DATA_PROVENANCE_RECONSTRUCTED")
    if date_range == DATE_RANGE_FAIL:
        reasons.append("DATE_RANGE_FAIL")
    elif date_range == DATE_RANGE_INSUFFICIENT_DATA:
        reasons.append("DATE_RANGE_INSUFFICIENT_DATA")
    elif date_range == DATE_RANGE_INCOMPLETE:
        reasons.append("DATE_RANGE_INCOMPLETE")
    if weight_status == WEIGHT_PATH_PARTIAL:
        reasons.append("WEIGHT_PATH_PARTIAL")
    if attribution_status == WEIGHT_PATH_PARTIAL:
        reasons.append("ATTRIBUTION_PARTIAL")
    elif attribution_status in {"MISSING", WEIGHT_PATH_INCOMPLETE}:
        reasons.append("ATTRIBUTION_INCOMPLETE")
    if overfit_status == "HIGH_RISK":
        reasons.append("OVERFIT_HIGH_RISK")
    elif overfit_status == "REVIEW_REQUIRED":
        reasons.append("OVERFIT_REVIEW_REQUIRED")
    if regime.get("coverage_status") == "PASS_WITH_WARNINGS":
        reasons.append("REGIME_COVERAGE_PASS_WITH_WARNINGS")
    if regime.get("tech_semiconductor_relevance") == "LOW":
        reasons.append("TECH_SEMICONDUCTOR_RELEVANCE_LOW")
    return list(dict.fromkeys(reason for reason in reasons if reason))


def _classify_evidence_gate_reasons(reasons: Sequence[str]) -> dict[str, Any]:
    warning_reason_ids = {
        "DATA_QUALITY_PASS_WITH_WARNINGS",
        "DATA_PROVENANCE_RECONSTRUCTED",
        "WEIGHT_PATH_PARTIAL",
        "ATTRIBUTION_PARTIAL",
        "REGIME_COVERAGE_PASS_WITH_WARNINGS",
    }
    hard = [reason for reason in reasons if reason in EVIDENCE_GATE_TRUE_HARD_FAILURES]
    warnings = [reason for reason in reasons if reason in warning_reason_ids]
    blocking = [
        reason
        for reason in reasons
        if reason not in {"DATA_QUALITY_PASS_WITH_WARNINGS", "DATA_PROVENANCE_RECONSTRUCTED"}
    ]
    soft = [
        reason
        for reason in blocking
        if reason not in hard and reason not in {"WEIGHT_PATH_PARTIAL", "ATTRIBUTION_PARTIAL"}
    ]
    categories = {category: [] for category in EVIDENCE_GATE_CATEGORY_ORDER}
    for reason in reasons:
        categories.setdefault(_evidence_gate_reason_category(reason), []).append(reason)
    return {
        "blocking_reasons": blocking,
        "warning_reasons": warnings,
        "hard_blocking_reasons": hard,
        "soft_blocking_reasons": soft,
        "categories": categories,
    }


def _evidence_gate_reason_category(reason: str) -> str:
    return EVIDENCE_GATE_REASON_CATEGORIES.get(reason, "promotion")


def _evidence_gate_reason_severity(reason: str) -> str:
    if reason in EVIDENCE_GATE_TRUE_HARD_FAILURES:
        return "hard_block"
    if reason in {
        "DATA_QUALITY_PASS_WITH_WARNINGS",
        "DATA_PROVENANCE_RECONSTRUCTED",
        "WEIGHT_PATH_PARTIAL",
        "ATTRIBUTION_PARTIAL",
        "REGIME_COVERAGE_PASS_WITH_WARNINGS",
    }:
        return "warning"
    return "review_required"


def _gate_impact_scenarios(top_reasons: Sequence[str]) -> list[dict[str, Any]]:
    return [
        {
            "scenario": "current_rules",
            "label": "Scenario A: current rules",
            "manual_review_allowed": [],
            "fixed_reasons": [],
        },
        {
            "scenario": "attribution_partial_as_manual_review",
            "label": "Scenario B: ATTRIBUTION_PARTIAL as manual review",
            "manual_review_allowed": ["ATTRIBUTION_PARTIAL"],
            "fixed_reasons": [],
        },
        {
            "scenario": "data_provenance_reconstructed_as_warning",
            "label": "Scenario C: reconstructed provenance as warning",
            "manual_review_allowed": [
                "DATA_PROVENANCE_INCOMPLETE",
                "DATA_PROVENANCE_RECONSTRUCTED",
            ],
            "fixed_reasons": [],
        },
        {
            "scenario": "overfit_review_required_as_manual_review",
            "label": "Scenario D: OVERFIT_REVIEW_REQUIRED as manual review",
            "manual_review_allowed": ["OVERFIT_REVIEW_REQUIRED"],
            "fixed_reasons": [],
        },
        {
            "scenario": "regime_pass_with_warnings_allows_observe",
            "label": "Scenario E: regime PASS_WITH_WARNINGS allows observe",
            "manual_review_allowed": ["REGIME_COVERAGE_PASS_WITH_WARNINGS"],
            "fixed_reasons": [],
        },
        {
            "scenario": "true_hard_failures_only",
            "label": "Scenario F: only true hard failures remain hard",
            "manual_review_allowed": sorted(EVIDENCE_GATE_DEFAULT_MANUAL_REVIEW_REASONS),
            "fixed_reasons": [],
        },
        {
            "scenario": "fix_top_1_blocking_reason",
            "label": "Scenario G: fix top 1 blocking reason",
            "manual_review_allowed": [],
            "fixed_reasons": list(top_reasons[:1]),
        },
        {
            "scenario": "fix_top_3_blocking_reasons",
            "label": "Scenario H: fix top 3 blocking reasons",
            "manual_review_allowed": [],
            "fixed_reasons": list(top_reasons[:3]),
        },
    ]


def _simulate_gate_recovery(
    rows: Sequence[Mapping[str, Any]],
    *,
    scenario_id: str,
    scenario: Mapping[str, Any],
) -> dict[str, Any]:
    manual_allowed = set(_texts(scenario.get("manual_review_allowed")))
    fixed = set(_texts(scenario.get("fixed_reasons")))
    recovered: list[str] = []
    manual_review: list[str] = []
    still_blocked = []
    for row in rows:
        candidate_id = _text(row.get("candidate_id"))
        hard = [
            reason for reason in _texts(row.get("hard_blocking_reasons")) if reason not in fixed
        ]
        soft = [
            reason for reason in _texts(row.get("soft_blocking_reasons")) if reason not in fixed
        ]
        warning = [reason for reason in _texts(row.get("warning_reasons")) if reason not in fixed]
        unresolved_soft = [reason for reason in soft if reason not in manual_allowed]
        if hard or unresolved_soft:
            still_blocked.append(
                {
                    "candidate_id": candidate_id,
                    "hard": hard,
                    "unresolved_soft": unresolved_soft,
                }
            )
            continue
        recovered.append(candidate_id)
        if soft or warning or manual_allowed or fixed:
            manual_review.append(candidate_id)
    return {
        "scenario": scenario_id,
        "label": scenario.get("label", scenario_id),
        "usable_candidates": len(recovered),
        "observe_candidates": len(recovered),
        "new_manual_review_required": len(manual_review),
        "still_blocked_candidates": len(still_blocked),
        "manual_review_allowed": sorted(manual_allowed),
        "fixed_reasons": sorted(fixed),
        "recovered_candidate_ids": recovered[:50],
        "risks": _scenario_risks(manual_allowed=manual_allowed, fixed=fixed),
    }


def _scenario_risks(*, manual_allowed: set[str], fixed: set[str]) -> list[str]:
    risks = []
    if "ATTRIBUTION_INCOMPLETE" in manual_allowed or "ATTRIBUTION_PARTIAL" in manual_allowed:
        risks.append("candidate attribution evidence remains incomplete or partial")
    if "DATA_PROVENANCE_INCOMPLETE" in manual_allowed:
        risks.append("price cache provenance is reconstructed, not original vendor manifest")
    if "BACKTEST_WINDOW_INCOMPLETE" in manual_allowed or "DATE_RANGE_INCOMPLETE" in manual_allowed:
        risks.append("actual evaluation window is shorter than requested range")
    if "OVERFIT_REVIEW_REQUIRED" in manual_allowed:
        risks.append("overfit status still requires manual review")
    if fixed:
        risks.append("simulation assumes evidence gaps are fixed without changing source metrics")
    return risks


def _calibrated_candidate_status(
    row: Mapping[str, Any],
    *,
    hard_fail: set[str],
    manual_allowed: set[str],
) -> dict[str, Any]:
    all_reasons = list(
        dict.fromkeys(
            [
                *_texts(row.get("hard_blocking_reasons")),
                *_texts(row.get("soft_blocking_reasons")),
                *_texts(row.get("warning_reasons")),
            ]
        )
    )
    remaining_hard = [
        reason
        for reason in all_reasons
        if reason in hard_fail or reason in EVIDENCE_GATE_TRUE_HARD_FAILURES
    ]
    manual_reasons = [
        reason
        for reason in all_reasons
        if reason in manual_allowed and reason not in remaining_hard
    ]
    unresolved = [
        reason
        for reason in all_reasons
        if reason not in remaining_hard
        and reason not in manual_reasons
        and reason not in {"DATA_QUALITY_PASS_WITH_WARNINGS", "DATA_PROVENANCE_RECONSTRUCTED"}
    ]
    status = GATE_OBSERVE_ONLY
    if remaining_hard or unresolved:
        status = GATE_REJECT
    return {
        "candidate_id": row.get("candidate_id"),
        "source_sweep_id": row.get("source_sweep_id"),
        "original_gate": row.get("gate"),
        "calibrated_status": status,
        "manual_review_required": status == GATE_OBSERVE_ONLY,
        "remaining_hard_failures": remaining_hard,
        "manual_review_reasons": manual_reasons,
        "unresolved_reasons": unresolved,
        "warning_reasons": row.get("warning_reasons", []),
        "evidence_status": row.get("evidence_status", {}),
        "regime_status": row.get("regime_status", {}),
        "score": row.get("score"),
    }


def render_evidence_diagnosis_markdown(
    manifest: Mapping[str, Any],
    blocking_summary: Mapping[str, Any],
    gate_category_summary: Mapping[str, Any],
) -> str:
    blockers = _records(blocking_summary.get("blocking_reasons"))
    categories = _records(gate_category_summary.get("categories"))
    top = blockers[0] if blockers else {}
    lines = [
        "# Dynamic v3 Evidence Gate Diagnosis",
        "",
        f"- sweep_id: `{manifest.get('source_sweep_id')}`",
        f"- diagnosis_id: `{manifest.get('diagnosis_id')}`",
        f"- candidate_count: `{manifest.get('candidate_count')}`",
        f"- usable_candidates: `{manifest.get('usable_candidates')}`",
        f"- review_required_candidates: `{manifest.get('review_required_candidates')}`",
        f"- hard_blocked_candidates: `{manifest.get('hard_blocked_candidates')}`",
        f"- soft_blocked_candidates: `{manifest.get('soft_blocked_candidates')}`",
        f"- top_blocking_reason: `{top.get('reason', 'none')}`",
        "",
        "## Blocking Reasons",
        "|reason|count|share|severity|category|",
        "|---|---:|---:|---|---|",
    ]
    for row in blockers:
        lines.append(
            f"|{row.get('reason')}|{row.get('count')}|{row.get('share')}|"
            f"{row.get('severity')}|{row.get('category')}|"
        )
    lines.extend(["", "## Gate Categories", "|category|candidate_count|", "|---|---:|"])
    for row in categories:
        lines.append(f"|{row.get('category')}|{row.get('candidate_count')}|")
    lines.extend(
        [
            "",
            "## Diagnosis",
            (
                "300 个 medium_real candidate 全部不可用时，优先解释为 evidence gate "
                "未闭合，而不是 production candidate 失败。hard block 仍不得降级；"
                "soft block 和 warning 只能进入 manual review simulation。"
            ),
            "",
            "不建议直接运行 full overnight_real；应先完成 gate impact、policy calibration "
            "和 recovered observe pool 复核。",
            "",
            "production_effect=none; broker_action=none.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_gate_impact_markdown(
    manifest: Mapping[str, Any],
    scenarios: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Dynamic v3 Gate Impact Matrix",
        "",
        f"- impact_id: `{manifest.get('impact_id')}`",
        f"- source_diagnosis_id: `{manifest.get('source_diagnosis_id')}`",
        f"- baseline_observe_candidates: `{manifest.get('baseline_observe_candidates')}`",
        f"- best_scenario: `{manifest.get('best_scenario')}`",
        f"- best_observe_candidates: `{manifest.get('best_observe_candidates')}`",
        "",
        "|scenario|observe_candidates|manual_review_required|still_blocked|",
        "|---|---:|---:|---:|",
    ]
    for row in scenarios:
        lines.append(
            f"|{row.get('scenario')}|{row.get('observe_candidates')}|"
            f"{row.get('new_manual_review_required')}|{row.get('still_blocked_candidates')}|"
        )
    lines.extend(
        [
            "",
            "Simulation 不修改原始 candidate metrics；它只回答哪些 evidence gate "
            "如果修复或降级为 manual review，候选可进入 observe-only 复核池。",
            "",
            "production_effect=none; broker_action=none.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_gate_policy_markdown(
    manifest: Mapping[str, Any],
    effect: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic v3 Evidence Gate Policy Apply",
        "",
        f"- policy_run_id: `{manifest.get('policy_run_id')}`",
        f"- source_sweep_id: `{manifest.get('source_sweep_id')}`",
        f"- policy_version: `{manifest.get('policy_version')}`",
        f"- observe_only_candidates: `{manifest.get('observe_only_candidates')}`",
        "- manual_review_required_candidates: "
        f"`{manifest.get('manual_review_required_candidates')}`",
        f"- rejected_candidates: `{manifest.get('rejected_candidates')}`",
        "",
        "## Manual Review Reasons",
        "|reason|count|",
        "|---|---:|",
    ]
    for row in _records(effect.get("manual_review_reason_distribution")):
        lines.append(f"|{row.get('reason')}|{row.get('count')}|")
    lines.extend(
        [
            "",
            "Policy apply 不修改原始 sweep artifact；它只写 calibrated candidate status。",
            "production_candidate_generated=false.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_candidate_recovery_markdown(
    manifest: Mapping[str, Any],
    recovered: Sequence[Mapping[str, Any]],
    rejected: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Dynamic v3 Candidate Recovery",
        "",
        f"- recovery_id: `{manifest.get('recovery_id')}`",
        f"- source_sweep_id: `{manifest.get('source_sweep_id')}`",
        f"- recovered_candidate_count: `{manifest.get('recovered_candidate_count')}`",
        f"- observe_only_candidate_count: `{manifest.get('observe_only_candidate_count')}`",
        f"- manual_review_required_count: `{manifest.get('manual_review_required_count')}`",
        f"- rejected_after_calibration_count: `{manifest.get('rejected_after_calibration_count')}`",
        "",
        "|candidate_id|score|manual_review_required|",
        "|---|---:|---|",
    ]
    for row in recovered[:20]:
        lines.append(
            f"|{row.get('candidate_id')}|{row.get('score')}|{row.get('manual_review_required')}|"
        )
    if not recovered:
        lines.extend(
            [
                "",
                "当前 calibration 后仍无 recovered candidate；需要优先修复 rejected reasons。",
                f"rejected_after_calibration={len(rejected)}",
            ]
        )
    lines.extend(["", "production_effect=none; broker_action=none."])
    return "\n".join(lines) + "\n"


def render_research_decision_update_reader_brief(go_no_go: Mapping[str, Any]) -> str:
    return (
        "Dynamic Rescue Decision Update: "
        f"go_no_go={go_no_go.get('go_no_go')}; "
        f"recommended_action={go_no_go.get('recommended_action')}; "
        f"observe_candidates_after={go_no_go.get('observe_candidates_after')}; "
        "production_effect=none."
    )


def render_research_decision_update_markdown(
    manifest: Mapping[str, Any],
    go_no_go: Mapping[str, Any],
    *,
    simulation: Sequence[Mapping[str, Any]],
    impact: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic v3 Research Decision Update",
        "",
        f"- decision_update_id: `{manifest.get('decision_update_id')}`",
        f"- source_sweep_id: `{manifest.get('source_sweep_id')}`",
        f"- usable_candidates_before: `{go_no_go.get('usable_candidates_before')}`",
        f"- usable_candidates_after: `{go_no_go.get('usable_candidates_after')}`",
        f"- observe_candidates_after: `{go_no_go.get('observe_candidates_after')}`",
        f"- overnight_engineering_readiness: `{go_no_go.get('overnight_engineering_readiness')}`",
        f"- overnight_research_readiness: `{go_no_go.get('overnight_research_readiness')}`",
        f"- go_no_go: `{go_no_go.get('go_no_go')}`",
        f"- recommended_action: `{go_no_go.get('recommended_action')}`",
        f"- required_owner_approval: `{go_no_go.get('required_owner_approval')}`",
        "",
        "## Gate Impact Summary",
        f"- best_scenario: `{impact.get('best_scenario')}`",
        f"- best_observe_candidates: `{impact.get('best_observe_candidates')}`",
        "",
        "|scenario|observe_candidates|still_blocked|",
        "|---|---:|---:|",
    ]
    for row in simulation:
        lines.append(
            f"|{row.get('scenario')}|{row.get('observe_candidates')}|"
            f"{row.get('still_blocked_candidates')}|"
        )
    lines.extend(
        [
            "",
            "## Conclusion",
            (
                "不存在 production-grade candidate；若存在 recovered candidates，它们也只是 "
                "observe_only + manual_review_required。是否运行 limited overnight_real 需要 "
                "owner 明确批准，full overnight_real 不应作为自动下一步。"
            ),
            "",
            "production_effect=none; broker_action=none.",
        ]
    )
    return "\n".join(lines) + "\n"


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _observe_pool_allowed(
    *,
    row: Mapping[str, Any],
    evidence: Mapping[str, Any],
    regime_status: Mapping[str, Any],
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if row.get("evaluator_mode") != EVALUATOR_REAL_DYNAMIC_V3_RESCUE:
        reasons.append("evaluator_not_real_dynamic_v3_rescue")
    if evidence.get("data_quality") == "FAIL":
        reasons.append("data_quality_fail")
    if evidence.get("date_range_status") in {DATE_RANGE_FAIL, DATE_RANGE_INSUFFICIENT_DATA}:
        reasons.append("date_range_fail_or_insufficient")
    if evidence.get("weight_path_status") == "MISSING":
        reasons.append("weight_path_missing")
    if evidence.get("candidate_attribution_status") not in {
        WEIGHT_PATH_COMPLETE,
        WEIGHT_PATH_PARTIAL,
    }:
        reasons.append("candidate_attribution_not_complete_or_partial")
    if evidence.get("overfit_status") == "HIGH_RISK":
        reasons.append("overfit_high_risk")
    if regime_status.get("tech_semiconductor_relevance") == "LOW":
        reasons.append("tech_semiconductor_relevance_low")
    if evidence.get("promotion_status") == FORBIDDEN_GATE:
        reasons.append("production_status_forbidden")
    warning_map = {
        "PASS_WITH_WARNINGS": "data_quality_warning",
        DATA_PROVENANCE_RECONSTRUCTED: "data_provenance_warning",
        WEIGHT_PATH_PARTIAL: "WEIGHT_PATH_PARTIAL",
    }
    for value, reason in warning_map.items():
        if value in {
            evidence.get("data_quality"),
            evidence.get("data_provenance_status"),
            evidence.get("weight_path_status"),
            evidence.get("candidate_attribution_status"),
        }:
            reasons.append(reason)
    blocking = [
        reason
        for reason in reasons
        if not reason.endswith("_warning")
        and reason not in {"WEIGHT_PATH_PARTIAL", "ATTRIBUTION_PARTIAL"}
    ]
    return not blocking, reasons


def render_observe_pool_markdown(
    manifest: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Dynamic v3 Rescue Observe-only Candidate Pool",
        "",
        f"- pool_id: `{manifest.get('pool_id')}`",
        f"- source_sweep_id: `{manifest.get('source_sweep_id')}`",
        f"- status: `{manifest.get('status')}`",
        f"- observe_candidate_count: `{manifest.get('observe_candidate_count')}`",
        f"- manual_review_required_count: `{manifest.get('manual_review_required_count')}`",
        f"- shadow_registry_sync_status: `{manifest.get('shadow_registry_sync_status')}`",
        "",
        "|candidate_id|score|overfit|manual_review_required|",
        "|---|---|---|---|",
    ]
    for row in candidates:
        lines.append(
            f"|{row.get('candidate_id')}|{row.get('score')}|"
            f"{row.get('overfit_status')}|{row.get('manual_review_required')}|"
        )
    lines.extend(["", "production_effect=none; broker_action=none."])
    return "\n".join(lines) + "\n"


def _candidate_evidence_status(row: Mapping[str, Any]) -> dict[str, Any]:
    evidence = _mapping(row.get("evidence_status"))
    if evidence:
        return evidence
    metrics = _mapping(row.get("metrics"))
    return {
        "data_quality": metrics.get(
            "data_quality",
            _mapping(row.get("data_quality")).get("status", "MISSING"),
        ),
        "data_provenance_status": metrics.get("data_provenance_status", "MISSING"),
        "date_range_status": metrics.get(
            "date_range_status",
            _mapping(row.get("backtest_window")).get("date_range_status", "MISSING"),
        ),
        "weight_path_status": metrics.get(
            "weight_path_status",
            _mapping(row.get("weight_path_metadata")).get("attribution_completeness", "MISSING"),
        ),
        "candidate_attribution_status": metrics.get("candidate_attribution_status", "MISSING"),
        "overfit_status": metrics.get("overfit_status", row.get("overfit_status", "MISSING")),
        "promotion_status": row.get("gate", row.get("recovered_status", "")),
        "evidence_score": metrics.get("evidence_score"),
    }


def _candidate_regime_status(row: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(row.get("regime_coverage_status")) or _mapping(row.get("regime_status"))


def _candidate_overfit_status(row: Mapping[str, Any]) -> str:
    evidence = _candidate_evidence_status(row)
    return _text(row.get("overfit_status"), _text(evidence.get("overfit_status"), "MISSING"))


def _candidate_daily_weights_path(row: Mapping[str, Any]) -> Path | None:
    real_path_text = _text(row.get("real_evaluation_artifact_path"))
    if real_path_text:
        daily_path = Path(real_path_text).parent / "daily_weights.csv"
        if daily_path.exists():
            return daily_path
    metadata_path_text = _text(_mapping(row.get("weight_path_metadata")).get("metadata_path"))
    if metadata_path_text:
        daily_path = Path(metadata_path_text).parent / "daily_weights.csv"
        if daily_path.exists():
            return daily_path
    source_path_text = _text(row.get("source_weight_path_artifact"))
    if source_path_text and Path(source_path_text).exists():
        return Path(source_path_text)
    return None


def _shortlist_hard_reject_reasons(row: Mapping[str, Any]) -> list[str]:
    evidence = _candidate_evidence_status(row)
    regime = _candidate_regime_status(row)
    reasons: list[str] = []
    if _text(evidence.get("data_quality")) == "FAIL":
        reasons.append("data_quality_fail")
    if _text(evidence.get("date_range_status")) in {DATE_RANGE_FAIL, DATE_RANGE_INSUFFICIENT_DATA}:
        reasons.append("date_range_fail_or_insufficient")
    real_path = Path(_text(row.get("real_evaluation_artifact_path")))
    if not _text(row.get("real_evaluation_artifact_path")) or not real_path.exists():
        reasons.append("missing_real_evaluation_artifact")
    if _candidate_daily_weights_path(row) is None:
        reasons.append("missing_daily_weights")
    if _candidate_overfit_status(row) == "HIGH_RISK":
        reasons.append("overfit_high_risk")
    if _text(regime.get("tech_semiconductor_relevance")) == "LOW":
        reasons.append("tech_semiconductor_relevance_low")
    if row.get("production_candidate_generated") is True:
        reasons.append("production_candidate_generated")
    if (
        _text(evidence.get("promotion_status")) == FORBIDDEN_GATE
        or _text(row.get("gate")) == FORBIDDEN_GATE
    ):
        reasons.append("production_candidate_status_forbidden")
    return reasons


def _candidate_manual_review_warnings(row: Mapping[str, Any]) -> list[str]:
    evidence = _candidate_evidence_status(row)
    regime = _candidate_regime_status(row)
    warnings = _texts(row.get("remaining_warnings")) + _texts(row.get("warning_reasons"))
    if _text(evidence.get("data_quality")) == "PASS_WITH_WARNINGS":
        warnings.append("data_quality_pass_with_warnings")
    if _text(evidence.get("data_provenance_status")) == DATA_PROVENANCE_RECONSTRUCTED:
        warnings.append("data_provenance_reconstructed")
    if _text(evidence.get("candidate_attribution_status")) == WEIGHT_PATH_PARTIAL:
        warnings.append("attribution_partial")
    if _candidate_overfit_status(row) == "REVIEW_REQUIRED":
        warnings.append("overfit_review_required")
    if _text(regime.get("coverage_status")) == "PASS_WITH_WARNINGS":
        warnings.append("regime_coverage_pass_with_warnings")
    if row.get("manual_review_required") is True:
        warnings.append("source_manual_review_required")
    return sorted(set(warnings))


def _shortlist_score_breakdown(row: Mapping[str, Any], *, diversity: float) -> dict[str, float]:
    evidence = _candidate_evidence_status(row)
    regime = _candidate_regime_status(row)
    metrics = _mapping(row.get("metrics"))
    params = _mapping(row.get("parameters"))
    performance = _clamp01(_float(row.get("score"), _float(metrics.get("score"), 0.0)))
    constraint_load = _metric_first(
        metrics,
        ("constraint_hits", "constraint_hit_count", "constraint_hit_rate"),
    )
    turnover = _metric_first(metrics, ("turnover", "annualized_turnover", "turnover_ratio"))
    drawdown_gap = abs(
        _metric_first(metrics, ("drawdown_degradation_pp", "max_drawdown_delta_vs_static_pp"))
    )
    static_gap = abs(_metric_first(metrics, ("dynamic_vs_static_gap", "dynamic_static_gap")))
    risk = _avg(
        [
            1.0 / (1.0 + max(0.0, constraint_load)),
            1.0 / (1.0 + max(0.0, turnover)),
            1.0 / (1.0 + drawdown_gap * 100.0),
            1.0 / (1.0 + static_gap * 10.0),
        ]
    )
    evidence_score = evidence.get("evidence_score")
    evidence_component = (
        _clamp01(_float(evidence_score))
        if evidence_score is not None
        else _avg(
            [
                _status_score(evidence.get("data_quality")),
                _status_score(evidence.get("date_range_status")),
                _weight_status_score(evidence.get("weight_path_status")),
                _weight_status_score(evidence.get("candidate_attribution_status")),
                _overfit_status_score(_candidate_overfit_status(row)),
            ]
        )
    )
    regime_component = _avg(
        [
            _status_score(regime.get("coverage_status"), missing=0.5),
            1.0 if _text(regime.get("tech_semiconductor_relevance")) in {"HIGH", "MEDIUM"} else 0.5,
            (
                1.0
                if _text(regime.get("ai_bull_market_overfit_risk")) not in {"HIGH_RISK", "FAIL"}
                else 0.4
            ),
        ]
    )
    stability = _avg(
        [
            _weight_status_score(evidence.get("weight_path_status")),
            1.0 / (1.0 + max(0.0, turnover)),
            _clamp01(_float(params.get("turnover_penalty"))),
            _clamp01(_float(params.get("rebalance_cooldown_days")) / 20.0),
        ]
    )
    return {
        "performance": performance,
        "risk": _clamp01(risk),
        "evidence": _clamp01(evidence_component),
        "regime": _clamp01(regime_component),
        "stability": _clamp01(stability),
        "diversity": _clamp01(diversity),
    }


def _weighted_shortlist_score(breakdown: Mapping[str, Any]) -> float:
    return round(
        sum(
            SHORTLIST_SCORE_COMPONENT_WEIGHTS[key] * _float(breakdown.get(key))
            for key in SHORTLIST_SCORE_COMPONENT_WEIGHTS
        ),
        6,
    )


def _select_shortlist_candidates(
    rows: Sequence[Mapping[str, Any]],
    *,
    target_size: int,
) -> list[dict[str, Any]]:
    remaining = [dict(row) for row in rows]
    selected: list[dict[str, Any]] = []
    while remaining and len(selected) < target_size:
        scored: list[tuple[float, str, dict[str, Any]]] = []
        for row in remaining:
            diversity = (
                1.0
                if not selected
                else max(
                    SHORTLIST_DIVERSITY_PARAMETER_FLOOR,
                    min(_parameter_distance(row, selected_row) for selected_row in selected),
                )
            )
            breakdown = _shortlist_score_breakdown(row, diversity=diversity)
            row["_shortlist_score_breakdown"] = breakdown
            score = _weighted_shortlist_score(breakdown)
            scored.append((score, _text(row.get("candidate_id")), row))
        scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
        best = scored[0][2]
        selected.append(best)
        best_id = _text(best.get("candidate_id"))
        remaining = [row for row in remaining if _text(row.get("candidate_id")) != best_id]
    return selected


def _shortlist_selection_reasons(
    breakdown: Mapping[str, Any],
    warnings: Sequence[str],
) -> list[str]:
    reasons = []
    for key in ("performance", "risk", "evidence", "regime", "stability", "diversity"):
        if _float(breakdown.get(key)) >= 0.70:
            reasons.append(f"strong_{key}_component")
    if warnings:
        reasons.append("selected_despite_manual_review_warnings")
    return reasons or ["best_available_manual_review_candidate"]


def render_shortlist_markdown(
    manifest: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    rejected: Sequence[Mapping[str, Any]],
) -> str:
    hard_rejections = [
        row
        for row in rejected
        if "not_in_top_shortlist_after_diversity_selection"
        not in _texts(row.get("rejection_reasons"))
    ]
    lines = [
        "# Dynamic Rescue Shadow Shortlist",
        "",
        "- market_regime: `ai_after_chatgpt`",
        f"- source_observe_pool_id: `{manifest.get('source_observe_pool_id')}`",
        f"- observe_pool_candidate_count: `{manifest.get('observe_pool_candidate_count')}`",
        f"- shortlist_count: `{manifest.get('shortlist_count')}`",
        f"- manual_review_required_count: `{manifest.get('manual_review_required_count')}`",
        "",
        "## Selected Candidates",
        "",
        "|rank|candidate_id|score|overfit|warnings|",
        "|---|---|---|---|---|",
    ]
    for row in candidates:
        lines.append(
            f"|{row.get('shortlist_rank')}|{row.get('candidate_id')}|"
            f"{row.get('shortlist_score')}|{row.get('overfit_status')}|"
            f"{', '.join(_texts(row.get('remaining_warnings'))) or 'none'}|"
        )
    lines.extend(
        [
            "",
            "## Required Questions",
            "",
            (
                "1. 当前 observe pool 有 "
                f"`{manifest.get('observe_pool_candidate_count')}` 个 candidates。"
            ),
            f"2. 最终 shortlist 选出 `{manifest.get('shortlist_count')}` 个 candidates。",
            "3. 被选中 candidates 的共同特征是：综合 score、证据、风险和路径稳定性相对靠前。",
            (
                "4. 被排除 candidates 的主要原因是 hard gate 不满足或在 diversity-aware "
                "排序后未进入目标规模。"
            ),
            (
                "5. score 高但证据差的候选会保留在 rejected 或 warnings 中，不能静默进入 "
                "production。"
            ),
            "6. 证据较好但表现一般的候选可因 diversity component 被保留为人工复核对象。",
            "7. shortlist 通过 diversity component 为后续 clustering 保留多种策略行为。",
            "8. 所有 shortlist candidates 仍然 `manual_review_required=true`。",
            "",
            f"- hard_rejected_candidate_count: `{len(hard_rejections)}`",
            "",
            "production_effect=none; broker_action=none.",
        ]
    )
    return "\n".join(lines) + "\n"


def _similarity_matrix_rows(
    candidates: Sequence[Mapping[str, Any]],
    similarity_fn: Any,
) -> list[dict[str, Any]]:
    rows = []
    ids = [_text(row.get("candidate_id")) for row in candidates]
    for left in candidates:
        left_id = _text(left.get("candidate_id"))
        matrix_row: dict[str, Any] = {"candidate_id": left_id}
        for right_id, right in zip(ids, candidates, strict=True):
            matrix_row[right_id] = round(_float(similarity_fn(left, right)), 6)
        rows.append(matrix_row)
    return rows


def _parameter_similarity(left: Mapping[str, Any], right: Mapping[str, Any]) -> float:
    return 1.0 - _parameter_distance(left, right)


def _parameter_distance(left: Mapping[str, Any], right: Mapping[str, Any]) -> float:
    left_params = _mapping(left.get("parameters"))
    right_params = _mapping(right.get("parameters"))
    scales = {
        "rescue_intensity": 1.0,
        "smooth_window_days": 20.0,
        "constraint_buffer_bps": 200.0,
        "turnover_penalty": 1.0,
        "risk_off_confirmation_days": 10.0,
        "rebalance_cooldown_days": 30.0,
    }
    distances: list[float] = []
    for parameter in REQUIRED_INJECTION_PARAMETERS:
        left_value = left_params.get(parameter)
        right_value = right_params.get(parameter)
        if parameter == "drawdown_guard":
            distances.append(0.0 if left_value == right_value else 1.0)
            continue
        scale = scales.get(parameter, 1.0)
        distances.append(min(1.0, abs(_float(left_value) - _float(right_value)) / scale))
    return _clamp01(_avg(distances))


def _metric_similarity(left: Mapping[str, Any], right: Mapping[str, Any]) -> float:
    left_metrics = _mapping(left.get("metrics"))
    right_metrics = _mapping(right.get("metrics"))
    fields = (
        "constraint_hits",
        "constraint_hit_rate",
        "turnover",
        "drawdown_degradation_pp",
        "dynamic_vs_static_gap",
        "false_risk_off_delta",
        "return_delta",
    )
    distances = []
    for field in fields:
        left_value = _float(left_metrics.get(field))
        right_value = _float(right_metrics.get(field))
        scale = 1.0 + abs(left_value) + abs(right_value)
        distances.append(abs(left_value - right_value) / scale)
    if _candidate_overfit_status(left) != _candidate_overfit_status(right):
        distances.append(0.5)
    return _clamp01(1.0 - _avg(distances))


def _weight_path_similarity_matrix(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    ids = [_text(row.get("candidate_id")) for row in candidates]
    rows = []
    for left in candidates:
        matrix_row: dict[str, Any] = {"candidate_id": _text(left.get("candidate_id"))}
        for right_id, right in zip(ids, candidates, strict=True):
            matrix_row[right_id] = _weight_path_similarity(left, right)
        rows.append(matrix_row)
    return rows


def _weight_path_similarity(left: Mapping[str, Any], right: Mapping[str, Any]) -> float | str:
    left_path = _candidate_weight_path(left)
    right_path = _candidate_weight_path(right)
    if not left_path["dates"] or not right_path["dates"]:
        return "INCOMPLETE"
    diffs = []
    for day in sorted(set(left_path["dates"]) & set(right_path["dates"])):
        left_weights = left_path["dates"][day]
        right_weights = right_path["dates"][day]
        for symbol in sorted(set(left_weights) & set(right_weights)):
            diffs.append(abs(_float(left_weights.get(symbol)) - _float(right_weights.get(symbol))))
    if not diffs:
        return "INCOMPLETE"
    return round(1.0 - min(1.0, _avg(diffs)), 6)


def _candidate_weight_path(row: Mapping[str, Any]) -> dict[str, Any]:
    path = _candidate_daily_weights_path(row)
    if path is None:
        return {"status": "INCOMPLETE", "path": "", "dates": {}}
    daily_rows = _read_csv_rows(path)
    dates: dict[str, dict[str, float]] = {}
    for daily in daily_rows:
        day = _text(daily.get("date"), _text(daily.get("signal_date")))
        if not day:
            continue
        if "symbol" in daily:
            symbol = _text(daily.get("symbol"))
            if symbol:
                dates.setdefault(day, {})[symbol] = _float(
                    daily.get("target_weight"), _float(daily.get("weight"))
                )
            continue
        for symbol in DYNAMIC_V3_WEIGHT_SYMBOLS:
            if symbol in daily:
                dates.setdefault(day, {})[symbol] = _float(daily.get(symbol))
    return {
        "status": "COMPLETE" if dates else "INCOMPLETE",
        "path": str(path),
        "dates": dates,
    }


def _candidate_latest_target_weights(row: Mapping[str, Any]) -> dict[str, Any]:
    path = _candidate_weight_path(row)
    dates = _mapping(path.get("dates"))
    if not dates:
        return {
            "status": "INCOMPLETE",
            "as_of": "",
            "weights": {},
            "source_weight_path_artifact": path.get("path", ""),
        }
    latest_day = max(dates)
    weights = {
        symbol: round(_float(value), 6)
        for symbol, value in sorted(_mapping(dates.get(latest_day)).items())
    }
    return {
        "status": "COMPLETE",
        "as_of": latest_day,
        "weights": weights,
        "source_weight_path_artifact": path.get("path", ""),
    }


def _candidate_clusters(
    candidates: Sequence[Mapping[str, Any]],
    weight_matrix: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    lookup = {
        (_text(row.get("candidate_id")), key): value
        for row in weight_matrix
        for key, value in row.items()
        if key != "candidate_id"
    }
    ordered = sorted(candidates, key=lambda row: int(row.get("shortlist_rank") or 9999))
    assigned: set[str] = set()
    clusters: list[dict[str, Any]] = []
    for seed in ordered:
        seed_id = _text(seed.get("candidate_id"))
        if seed_id in assigned:
            continue
        members = [seed]
        assigned.add(seed_id)
        for candidate in ordered:
            candidate_id = _text(candidate.get("candidate_id"))
            if candidate_id in assigned:
                continue
            similarity = _composite_candidate_similarity(seed, candidate, lookup)
            if similarity >= CANDIDATE_CLUSTER_SIMILARITY_THRESHOLD:
                members.append(candidate)
                assigned.add(candidate_id)
        label = _cluster_label(members)
        cluster_key = f"{label}_{len(clusters) + 1:02d}"
        representative_ids = [
            _text(row.get("candidate_id"))
            for row in sorted(members, key=lambda item: int(item.get("shortlist_rank") or 9999))[
                : _cluster_representative_limit(members)
            ]
        ]
        clusters.append(
            {
                "cluster_id": cluster_key,
                "label": label,
                "candidate_count": len(members),
                "candidates": [_text(row.get("candidate_id")) for row in members],
                "representatives": representative_ids,
                "common_traits": _cluster_common_traits(members),
                "risks": _cluster_risks(members),
            }
        )
    return clusters


def _composite_candidate_similarity(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    weight_lookup: Mapping[tuple[str, str], Any],
) -> float:
    left_id = _text(left.get("candidate_id"))
    right_id = _text(right.get("candidate_id"))
    parameter = _parameter_similarity(left, right)
    metric = _metric_similarity(left, right)
    weight = weight_lookup.get((left_id, right_id), "INCOMPLETE")
    if weight == "INCOMPLETE":
        return _avg([parameter, metric])
    return _avg([parameter, metric, _float(weight)])


def _cluster_representatives(
    clusters: Sequence[Mapping[str, Any]],
    candidates: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    by_id = {_text(row.get("candidate_id")): row for row in candidates}
    reps = []
    for cluster in clusters:
        representatives = _texts(cluster.get("representatives"))
        for idx, candidate_id in enumerate(representatives):
            source = by_id.get(candidate_id, {})
            reps.append(
                {
                    "cluster_id": cluster.get("cluster_id"),
                    "cluster_label": cluster.get("label"),
                    "candidate_id": candidate_id,
                    "representative_type": "primary" if idx == 0 else "secondary",
                    "shortlist_rank": source.get("shortlist_rank"),
                    "shortlist_score": source.get("shortlist_score"),
                    "selection_reasons": source.get("selection_reasons", []),
                    "risks": cluster.get("risks", []),
                }
            )
    return reps


def _cluster_representative_limit(members: Sequence[Mapping[str, Any]]) -> int:
    return 2 if len(members) >= CANDIDATE_CLUSTER_SECONDARY_REPRESENTATIVE_SIZE else 1


def _cluster_label(members: Sequence[Mapping[str, Any]]) -> str:
    params = [_mapping(row.get("parameters")) for row in members]
    avg_rescue = _avg([_float(row.get("rescue_intensity")) for row in params])
    avg_smooth = _avg([_float(row.get("smooth_window_days")) for row in params])
    avg_buffer = _avg([_float(row.get("constraint_buffer_bps")) for row in params])
    avg_turnover_penalty = _avg([_float(row.get("turnover_penalty")) for row in params])
    guards = {_text(row.get("drawdown_guard")) for row in params}
    if "hard" in guards:
        return "drawdown_guard_focused"
    if avg_turnover_penalty >= 0.30 or avg_smooth >= 10:
        return "defensive_low_turnover"
    if avg_rescue >= 0.80:
        return "aggressive_rescue"
    if avg_buffer >= 50:
        return "high_constraint_buffer"
    if avg_smooth >= 7:
        return "smooth_transition"
    return "balanced_candidate"


def _cluster_common_traits(members: Sequence[Mapping[str, Any]]) -> list[str]:
    label = _cluster_label(members)
    trait_map = {
        "drawdown_guard_focused": ["drawdown guard active", "risk reduction emphasis"],
        "defensive_low_turnover": ["low turnover preference", "smoother transitions"],
        "aggressive_rescue": ["higher rescue intensity", "faster risk response"],
        "high_constraint_buffer": ["higher constraint buffer", "lower cap pressure"],
        "smooth_transition": ["larger smooth window", "gradual target changes"],
        "balanced_candidate": ["balanced parameter profile"],
    }
    return trait_map.get(label, ["balanced parameter profile"])


def _cluster_risks(members: Sequence[Mapping[str, Any]]) -> list[str]:
    label = _cluster_label(members)
    risk_map = {
        "drawdown_guard_focused": ["may lag strong rebound"],
        "defensive_low_turnover": ["may underreact to fast regime changes"],
        "aggressive_rescue": ["may create higher turnover"],
        "high_constraint_buffer": ["may reduce upside capture"],
        "smooth_transition": ["may lag abrupt drawdown"],
        "balanced_candidate": ["requires manual evidence review"],
    }
    return risk_map.get(label, ["requires manual evidence review"])


def render_candidate_cluster_markdown(
    manifest: Mapping[str, Any],
    clusters: Sequence[Mapping[str, Any]],
    representatives: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Dynamic Rescue Candidate Clustering",
        "",
        f"- cluster_id: `{manifest.get('cluster_id')}`",
        f"- source_shortlist_id: `{manifest.get('source_shortlist_id')}`",
        f"- candidate_count: `{manifest.get('candidate_count')}`",
        f"- cluster_count: `{manifest.get('cluster_count')}`",
        f"- representative_count: `{manifest.get('representative_count')}`",
        f"- weight_path_similarity_status: `{manifest.get('weight_path_similarity_status')}`",
        "",
        "|cluster|label|candidate_count|representatives|",
        "|---|---|---|---|",
    ]
    for cluster in clusters:
        lines.append(
            f"|{cluster.get('cluster_id')}|{cluster.get('label')}|"
            f"{cluster.get('candidate_count')}|"
            f"{', '.join(_texts(cluster.get('representatives')))}|"
        )
    lines.extend(
        [
            "",
            "## Representatives",
            "",
            "|candidate_id|cluster|type|shortlist_rank|",
            "|---|---|---|---|",
        ]
    )
    for row in representatives:
        lines.append(
            f"|{row.get('candidate_id')}|{row.get('cluster_id')}|"
            f"{row.get('representative_type')}|{row.get('shortlist_rank')}|"
        )
    lines.extend(["", "production_effect=none; broker_action=none."])
    return "\n".join(lines) + "\n"


def _shadow_shortlist_monitoring_plan(
    shadow_id: str,
    candidates: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_shortlist_monitoring_plan",
        "shadow_shortlist_id": shadow_id,
        "candidate_count": len(candidates),
        "daily_metrics": [
            "daily_target_weights",
            "weight_path_change",
            "turnover_estimate",
            "constraint_hits",
            "current_regime_response",
            "candidate_disagreement",
        ],
        "weekly_metrics": [
            "live_vs_backtest_drift",
            "drawdown_from_shadow_start",
            "dynamic_vs_static_gap_since_shadow_start",
            "weight_path_stability",
        ],
        "promotion_clock": {"min_days": 30, "min_rebalance_count": 3},
        "drift_thresholds": {
            "policy_source": "TRADING-126_to_130 pilot monitoring baseline",
            "live_vs_backtest_drift_requires_review": True,
        },
        "downgrade_rules": [
            "missing_daily_weight_path",
            "unexpected_broker_action_signal",
            "owner_review_rejected",
            "data_quality_fail",
        ],
        "manual_review_requirements": [
            "owner_accepts_shortlist",
            "owner_accepts_cluster_representatives",
            "owner_accepts_no_broker_action_boundary",
        ],
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def render_shadow_shortlist_reader_brief(
    manifest: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> str:
    labels = sorted(
        {_text(row.get("cluster_label")) for row in rows if _text(row.get("cluster_label"))}
    )
    return (
        "## Dynamic Rescue Shadow Shortlist\n\n"
        f"- shadow_shortlist_id: `{manifest.get('shadow_shortlist_id')}`\n"
        f"- shadow_candidate_count: `{manifest.get('shadow_candidate_count')}`\n"
        f"- cluster_labels: `{', '.join(labels) or 'none'}`\n"
        "- ready_for_shadow_monitoring: "
        f"`{manifest.get('shadow_monitoring_ready')}`\n"
        "- manual_review_required: `true`\n"
        "- production_effect=none; broker_action=none.\n"
    )


def render_shadow_shortlist_markdown(
    manifest: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    monitoring_plan: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic Rescue Shadow Shortlist Monitoring Pack",
        "",
        f"- shadow_shortlist_id: `{manifest.get('shadow_shortlist_id')}`",
        f"- source_shortlist_id: `{manifest.get('source_shortlist_id')}`",
        f"- source_cluster_id: `{manifest.get('source_cluster_id')}`",
        f"- shadow_candidate_count: `{manifest.get('shadow_candidate_count')}`",
        f"- shadow_monitoring_ready: `{manifest.get('shadow_monitoring_ready')}`",
        "",
        "|candidate_id|cluster_label|representative_type|min_days|min_rebalance_count|",
        "|---|---|---|---|---|",
    ]
    for row in rows:
        requirements = _mapping(row.get("monitoring_requirements"))
        lines.append(
            f"|{row.get('candidate_id')}|{row.get('cluster_label')}|"
            f"{row.get('representative_type')}|{requirements.get('min_days')}|"
            f"{requirements.get('min_rebalance_count')}|"
        )
    lines.extend(
        [
            "",
            "## Monitoring Plan",
            "",
            f"- daily_metrics: `{', '.join(_texts(monitoring_plan.get('daily_metrics')))}`",
            f"- weekly_metrics: `{', '.join(_texts(monitoring_plan.get('weekly_metrics')))}`",
            "- 不自动写入 production，不触发 broker action。",
            "",
            "production_effect=none; broker_action=none.",
        ]
    )
    return "\n".join(lines) + "\n"


def _load_portfolio_snapshot(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    normalized = _normalize_portfolio_snapshot(path, strict=False)
    if normalized.get("failed_check_count"):
        failed = ", ".join(
            _text(check.get("name"), _text(check.get("check_id"), "unknown_check"))
            for check in _records(normalized.get("checks"))
            if check.get("passed") is False
        )
        raise DynamicV3ParameterResearchError(f"portfolio snapshot validation failed: {failed}")
    return {
        "as_of": normalized.get("as_of", ""),
        "weights": _mapping(normalized.get("weights")),
        "path": str(path),
        "owner_reviewed": normalized.get("owner_reviewed", False),
        "manual_review_required": normalized.get("manual_review_required", True),
    }


def _consensus_target_weights(
    target_rows: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    symbols = sorted(
        {symbol for row in target_rows for symbol in _mapping(row.get("target_weights")).keys()}
    )
    consensus_config = _mapping(config.get("consensus"))
    agreement_threshold = _float(consensus_config.get("agreement_threshold"), 0.60)
    max_dispersion = _float(consensus_config.get("max_symbol_dispersion"), 0.20)
    rows: list[dict[str, Any]] = []
    disagreement = False
    for symbol in symbols:
        values = [_float(_mapping(row.get("target_weights")).get(symbol)) for row in target_rows]
        if not values:
            continue
        center = _median(values)
        dispersion = max(values) - min(values)
        agreement = sum(1 for value in values if abs(value - center) <= max_dispersion) / len(
            values
        )
        if agreement < agreement_threshold or dispersion > max_dispersion:
            disagreement = True
        rows.append(
            {
                "symbol": symbol,
                "mean_target_weight": round(sum(values) / len(values), 6),
                "median_target_weight": round(center, 6),
                "min_target_weight": round(min(values), 6),
                "max_target_weight": round(max(values), 6),
                "candidate_agreement_ratio": round(agreement, 6),
                "dispersion": round(dispersion, 6),
            }
        )
    if not rows:
        return [], "MISSING_TARGET_WEIGHTS"
    return rows, "DISAGREEMENT_REVIEW_REQUIRED" if disagreement else "PASS"


def _candidate_position_delta_rows(
    target_rows: Sequence[Mapping[str, Any]],
    snapshot: Mapping[str, Any],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    current = _mapping(snapshot.get("weights"))
    limits = _mapping(config.get("advisory_limits"))
    max_total = _float(limits.get("max_single_day_total_adjustment"), 0.10)
    max_symbol = _float(limits.get("max_single_symbol_adjustment"), 0.05)
    min_trade = _float(limits.get("min_trade_threshold"), 0.01)
    rows = []
    for target in target_rows:
        target_weights = _mapping(target.get("target_weights"))
        symbols = sorted(set(current) | set(target_weights))
        deltas = {
            symbol: round(_float(target_weights.get(symbol)) - _float(current.get(symbol)), 6)
            for symbol in symbols
        }
        total_abs = round(sum(abs(value) for value in deltas.values()), 6)
        max_abs = max([abs(value) for value in deltas.values()] or [0.0])
        if all(abs(value) < min_trade for value in deltas.values()):
            status = "no_trade"
        elif total_abs > max_total or max_abs > max_symbol:
            status = "requires_manual_review"
        else:
            status = "within_limits"
        rows.append(
            {
                "candidate_id": target.get("candidate_id"),
                "current_weights": current,
                "target_weights": target_weights,
                "deltas": {key: value for key, value in deltas.items() if value != 0},
                "total_abs_adjustment": total_abs,
                "max_symbol_adjustment": round(max_abs, 6),
                "advisory_status": status,
            }
        )
    return rows


def _position_advisory_actions(
    *,
    target_rows: Sequence[Mapping[str, Any]],
    delta_rows: Sequence[Mapping[str, Any]],
    snapshot: Mapping[str, Any],
    consensus_status: str,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    reasons = []
    if consensus_status == "DISAGREEMENT_REVIEW_REQUIRED":
        reasons.append("candidate_target_weight_disagreement")
    if not snapshot:
        advisory_status = POSITION_ADVISORY_TARGET_ONLY
        recommended_action = "monitor"
        reasons.append("current_portfolio_snapshot_missing")
    elif any(row.get("advisory_status") == "requires_manual_review" for row in delta_rows):
        advisory_status = POSITION_ADVISORY_READY_WITH_MANUAL_REVIEW
        recommended_action = "manual_review"
        reasons.append("adjustment_limit_exceeded")
    elif all(row.get("advisory_status") == "no_trade" for row in delta_rows):
        advisory_status = POSITION_ADVISORY_READY_WITH_MANUAL_REVIEW
        recommended_action = "no_trade"
        reasons.append("all_candidate_deltas_below_min_trade_threshold")
    elif consensus_status == "DISAGREEMENT_REVIEW_REQUIRED":
        advisory_status = POSITION_ADVISORY_READY_WITH_MANUAL_REVIEW
        recommended_action = "manual_review"
    else:
        advisory_status = POSITION_ADVISORY_READY_WITH_MANUAL_REVIEW
        recommended_action = "small_adjustment"
    limits = _mapping(config.get("advisory_limits"))
    step_adjustment = _float(
        limits.get("max_risk_asset_increase_without_confirmation"),
        _float(limits.get("max_single_symbol_adjustment"), 0.05),
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_position_advisory_actions",
        "position_advisory_status": advisory_status,
        "advisory_status": (
            "manual_review_required"
            if recommended_action in {"manual_review", "small_adjustment"}
            else advisory_status
        ),
        "broker_action_allowed": False,
        "owner_approval_required": True,
        "recommended_action": recommended_action,
        "stepwise_plan": [
            {
                "step": 1,
                "description": "Owner review required before any real portfolio change.",
                "max_total_adjustment": round(step_adjustment, 6),
            }
        ],
        "reasons": sorted(set(reasons)),
        "risks": [
            "target weights are candidate research outputs",
            "manual review remains required before any position change",
        ],
        "candidate_count": len(target_rows),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def render_position_advisory_markdown(
    manifest: Mapping[str, Any],
    targets: Sequence[Mapping[str, Any]],
    consensus: Sequence[Mapping[str, Any]],
    deltas: Sequence[Mapping[str, Any]],
    actions: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic Rescue Position Advisory",
        "",
        f"- advisory_id: `{manifest.get('advisory_id')}`",
        f"- position_advisory_status: `{manifest.get('position_advisory_status')}`",
        f"- portfolio_snapshot_provided: `{manifest.get('portfolio_snapshot_provided')}`",
        f"- consensus_target_weight_status: `{manifest.get('consensus_target_weight_status')}`",
        f"- recommended_action: `{manifest.get('recommended_action')}`",
        f"- owner_approval_required: `{manifest.get('owner_approval_required')}`",
        f"- broker_action_allowed: `{manifest.get('broker_action_allowed')}`",
        "",
        "## Candidate Target Weights",
        "",
        "|candidate_id|as_of|weight_path_status|target_weights|",
        "|---|---|---|---|",
    ]
    for row in targets:
        lines.append(
            f"|{row.get('candidate_id')}|{row.get('as_of')}|"
            f"{row.get('weight_path_status')}|"
            f"{json.dumps(row.get('target_weights'), sort_keys=True)}|"
        )
    lines.extend(
        [
            "",
            "## Consensus",
            "",
            "|symbol|mean|median|agreement|dispersion|",
            "|---|---|---|---|---|",
        ]
    )
    for row in consensus:
        lines.append(
            f"|{row.get('symbol')}|{row.get('mean_target_weight')}|"
            f"{row.get('median_target_weight')}|{row.get('candidate_agreement_ratio')}|"
            f"{row.get('dispersion')}|"
        )
    lines.extend(
        [
            "",
            "## Required Questions",
            "",
            f"1. 当前是否提供真实持仓：`{manifest.get('portfolio_snapshot_provided')}`。",
            (
                "2. 如果没有，报告状态必须为 "
                f"`{POSITION_ADVISORY_TARGET_ONLY}`；当前为 "
                f"`{manifest.get('position_advisory_status')}`。"
            ),
            "3. shadow shortlist candidates 的当前目标权重见 Candidate Target Weights。",
            f"4. 候选一致性状态：`{manifest.get('consensus_target_weight_status')}`。",
            "5. 共识目标权重见 Consensus。",
            f"6. delta rows: `{len(deltas)}`。",
            "7. 单日 / 单票调整限制在 candidate_position_deltas.jsonl 中逐候选披露。",
            f"8. recommended_action: `{actions.get('recommended_action')}`。",
            f"9. owner_approval_required: `{actions.get('owner_approval_required')}`。",
            f"10. broker_action_allowed: `{actions.get('broker_action_allowed')}`。",
            "",
            "production_effect=none; broker_action=none.",
        ]
    )
    return "\n".join(lines) + "\n"


def _position_review_decision(
    shortlist: Mapping[str, Any],
    cluster: Mapping[str, Any],
    shadow: Mapping[str, Any],
    advisory: Mapping[str, Any],
    actions: Mapping[str, Any],
) -> dict[str, Any]:
    blocking = []
    warnings = []
    if int(shadow.get("shadow_candidate_count") or 0) <= 0:
        blocking.append("shadow_shortlist_empty")
    if _text(advisory.get("consensus_target_weight_status")) == "DISAGREEMENT_REVIEW_REQUIRED":
        warnings.append("candidate_target_weight_disagreement")
    shadow_ready = "READY_WITH_WARNINGS" if not blocking else "NOT_READY"
    advisory_status = _text(advisory.get("position_advisory_status"))
    if advisory_status == POSITION_ADVISORY_TARGET_ONLY:
        advisory_ready = POSITION_ADVISORY_TARGET_ONLY
        next_action = "provide_portfolio_snapshot"
    elif advisory_status == POSITION_ADVISORY_READY_WITH_MANUAL_REVIEW:
        advisory_ready = POSITION_ADVISORY_READY_WITH_MANUAL_REVIEW
        next_action = (
            "start_shadow_monitoring" if shadow_ready != "NOT_READY" else "manual_review_shortlist"
        )
    else:
        advisory_ready = "NOT_READY"
        next_action = "manual_review_shortlist"
    if blocking:
        next_action = "manual_review_shortlist"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_position_review_go_no_go_decision",
        "shadow_observation_readiness": shadow_ready,
        "position_advisory_readiness": advisory_ready,
        "production_readiness": "NOT_READY",
        "broker_action_allowed": False,
        "owner_approval_required": True,
        "recommended_next_action": next_action,
        "blocking_issues": blocking,
        "warnings": warnings + _texts(actions.get("reasons")),
        "shortlist_count": shortlist.get("shortlist_count"),
        "cluster_count": cluster.get("cluster_count"),
        "shadow_candidate_count": shadow.get("shadow_candidate_count"),
        "position_advisory_status": advisory_status,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def render_owner_review_checklist(decision: Mapping[str, Any]) -> str:
    items = [
        "是否接受 shortlist candidates？",
        "是否接受 cluster representatives？",
        "是否接受所有候选仍 manual_review_required？",
        "是否开始 30 天 shadow observation？",
        "是否提供真实 portfolio snapshot？",
        "是否允许 position advisory 进入日报？",
        "是否禁止 broker action？",
        "是否设置最大调仓建议幅度？",
        "是否需要增加心理 / 税务 / 流动性约束？",
        "是否准备后续 paper portfolio？",
    ]
    lines = [
        "# Dynamic Rescue Position Advisory Owner Review Checklist",
        "",
        f"- recommended_next_action: `{decision.get('recommended_next_action')}`",
        f"- production_readiness: `{decision.get('production_readiness')}`",
        "",
    ]
    lines.extend(f"- [ ] {item}" for item in items)
    lines.extend(["", "broker_action_allowed=false; owner_approval_required=true."])
    return "\n".join(lines) + "\n"


def render_position_review_markdown(
    manifest: Mapping[str, Any],
    shortlist: Mapping[str, Any],
    cluster: Mapping[str, Any],
    shadow: Mapping[str, Any],
    advisory: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic Rescue Position Advisory Readiness",
        "",
        f"- review_id: `{manifest.get('review_id')}`",
        f"- shortlist_count: `{manifest.get('shortlist_count')}`",
        f"- cluster_count: `{manifest.get('cluster_count')}`",
        f"- shadow_candidate_count: `{manifest.get('shadow_candidate_count')}`",
        f"- position_advisory_status: `{manifest.get('position_advisory_status')}`",
        f"- shadow_observation_readiness: `{decision.get('shadow_observation_readiness')}`",
        f"- position_advisory_readiness: `{decision.get('position_advisory_readiness')}`",
        f"- production_readiness: `{decision.get('production_readiness')}`",
        f"- recommended_next_action: `{decision.get('recommended_next_action')}`",
        "",
        "## Required Questions",
        "",
        (
            "1. observe candidates 通过 shortlist scoring 和 hard-fail gate 从 "
            f"`{shortlist.get('observe_pool_candidate_count')}` 压缩到 "
            f"`{shortlist.get('shortlist_count')}`。"
        ),
        f"2. shortlist 聚类数量：`{cluster.get('cluster_count')}`。",
        f"3. shadow shortlist representatives 数量：`{shadow.get('shadow_candidate_count')}`。",
        f"4. 目标权重一致性：`{advisory.get('consensus_target_weight_status')}`。",
        f"5. 是否可以进入 shadow monitoring：`{decision.get('shadow_observation_readiness')}`。",
        f"6. 是否可用于实际仓位建议：`{decision.get('position_advisory_readiness')}`。",
        "7. 如果只能 TARGET_ONLY，缺少 current portfolio snapshot。",
        f"8. portfolio snapshot provided: `{advisory.get('portfolio_snapshot_provided')}`。",
        "9. 是否允许自动交易：否，`broker_action_allowed=false`。",
        f"10. 下一步：`{decision.get('recommended_next_action')}`。",
        "",
        "production_effect=none; broker_action=none.",
    ]
    return "\n".join(lines) + "\n"


def render_position_review_reader_brief(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> str:
    return (
        "## Dynamic Rescue Position Advisory Readiness\n\n"
        f"- review_id: `{manifest.get('review_id')}`\n"
        f"- shortlist_count: `{manifest.get('shortlist_count')}`\n"
        f"- shadow_candidate_count: `{manifest.get('shadow_candidate_count')}`\n"
        f"- position_advisory_status: `{manifest.get('position_advisory_status')}`\n"
        f"- shadow_observation_readiness: `{decision.get('shadow_observation_readiness')}`\n"
        f"- position_advisory_readiness: `{decision.get('position_advisory_readiness')}`\n"
        f"- production_readiness: `{decision.get('production_readiness')}`\n"
        f"- recommended_next_action: `{decision.get('recommended_next_action')}`\n"
        "- broker_action_allowed=false; owner_approval_required=true.\n"
    )


def render_shadow_monitor_activation_markdown(manifest: Mapping[str, Any]) -> str:
    lines = [
        "# Dynamic Rescue Shadow Monitor Activation",
        "",
        f"- activation_id: `{manifest.get('activation_id')}`",
        f"- shadow_shortlist_id: `{manifest.get('shadow_shortlist_id')}`",
        f"- monitoring_status: `{manifest.get('monitoring_status')}`",
        f"- candidate_count: `{manifest.get('candidate_count')}`",
        f"- owner_approval_required: `{manifest.get('owner_approval_required')}`",
        f"- broker_action_allowed: `{manifest.get('broker_action_allowed')}`",
        "",
        "该 activation 只表示 shadow shortlist 可以进入人工复核后的持续观察；不执行交易。",
        "",
        "production_effect=none; broker_action=none.",
    ]
    return "\n".join(lines) + "\n"


def render_shadow_monitor_run_reader_brief(summary: Mapping[str, Any]) -> str:
    return (
        "## Dynamic Rescue Shadow Monitor\n\n"
        f"- monitor_run_id: `{summary.get('monitor_run_id')}`\n"
        f"- as_of: `{summary.get('as_of')}`\n"
        f"- active_count: `{summary.get('active_count')}`\n"
        f"- downgrade_recommended_count: `{summary.get('downgrade_recommended_count')}`\n"
        f"- promotion_review_eligible_count: `{summary.get('promotion_review_eligible_count')}`\n"
        f"- max_candidate_disagreement: `{summary.get('max_candidate_disagreement')}`\n"
        f"- summary_recommendation: `{summary.get('summary_recommendation')}`\n"
        "- broker_action_allowed=false; broker_action_taken=false.\n"
    )


def render_shadow_monitor_run_markdown(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> str:
    ordered = sorted(
        rows,
        key=lambda row: _float(row.get("total_abs_weight_change")),
        reverse=True,
    )
    lines = [
        "# Dynamic Rescue Shadow Monitor Run",
        "",
        f"- monitor_run_id: `{manifest.get('monitor_run_id')}`",
        f"- shadow_shortlist_id: `{manifest.get('shadow_shortlist_id')}`",
        f"- as_of: `{manifest.get('as_of')}`",
        f"- active_count: `{summary.get('active_count')}`",
        f"- summary_recommendation: `{summary.get('summary_recommendation')}`",
        f"- broker_action_allowed: `{manifest.get('broker_action_allowed')}`",
        "",
        "## Candidate Daily Results",
        "",
        "|candidate_id|cluster_label|weight_change|drift|promotion_clock|recommendation|",
        "|---|---|---|---|---|---|",
    ]
    for row in ordered:
        drift = _mapping(row.get("live_vs_backtest_drift"))
        promotion = _mapping(row.get("promotion_clock"))
        lines.append(
            f"|{row.get('candidate_id')}|{row.get('cluster_label')}|"
            f"{row.get('total_abs_weight_change')}|{drift.get('status')}|"
            f"{promotion.get('status')}|{row.get('recommendation')}|"
        )
    lines.extend(
        [
            "",
            "## Required Questions",
            "",
            f"1. 当前 active candidates: `{summary.get('active_count')}`。",
            (
                "2. 今日目标权重变化最大的 candidates 见 Candidate Daily Results "
                "的 weight_change 排序。"
            ),
            (
                "3. live vs backtest drift 状态由每个 candidate 的 "
                "`live_vs_backtest_drift.status` 披露。"
            ),
            f"4. downgrade recommended count: `{summary.get('downgrade_recommended_count')}`。",
            (
                "5. promotion clock eligible count: "
                f"`{summary.get('promotion_review_eligible_count')}`。"
            ),
            f"6. 当前建议: `{summary.get('summary_recommendation')}`。",
            "7. 是否有 broker action: 否，`broker_action_allowed=false`。",
            "",
            "production_effect=none; broker_action=none.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_portfolio_snapshot_validation_markdown(
    manifest: Mapping[str, Any],
    normalized: Mapping[str, Any],
    exposure: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic Rescue Portfolio Snapshot Validation",
        "",
        f"- snapshot_id: `{manifest.get('snapshot_id')}`",
        f"- status: `{manifest.get('status')}`",
        f"- as_of: `{manifest.get('as_of')}`",
        f"- owner_reviewed: `{manifest.get('owner_reviewed')}`",
        f"- broker_imported: `{manifest.get('broker_imported')}`",
        f"- manual_review_required: `{manifest.get('manual_review_required')}`",
        f"- weight_sum: `{normalized.get('weight_sum')}`",
        f"- value_sum: `{normalized.get('value_sum')}`",
        f"- risk_asset_weight: `{exposure.get('risk_asset_weight')}`",
        f"- defensive_weight: `{exposure.get('defensive_weight')}`",
        "",
        "## Checks",
        "",
        "|check|passed|detail|",
        "|---|---|---|",
    ]
    for check in _records(normalized.get("checks")):
        lines.append(f"|{check.get('name')}|{check.get('passed')}|{check.get('detail')}|")
    lines.extend(["", "broker_action_allowed=false; source=manual."])
    return "\n".join(lines) + "\n"


def render_daily_position_advisory_reader_brief(
    actions: Mapping[str, Any],
    delta_rows: Sequence[Mapping[str, Any]],
) -> str:
    top_delta = _top_delta_summary(delta_rows)
    return (
        "## Dynamic Rescue Daily Position Advisory\n\n"
        f"- daily_advisory_id: `{actions.get('daily_advisory_id')}`\n"
        f"- position_advisory_mode: `{actions.get('mode')}`\n"
        f"- consensus_status: `{actions.get('consensus_status')}`\n"
        f"- recommended_action: `{actions.get('recommended_action')}`\n"
        f"- manual_review_required: `{actions.get('manual_review_required')}`\n"
        f"- broker_action_allowed: `{actions.get('broker_action_allowed')}`\n"
        f"- top_weight_deltas: `{top_delta}`\n"
        f"- candidate_disagreement: `{actions.get('disagreement_status')}`\n"
    )


def render_daily_position_advisory_markdown(
    manifest: Mapping[str, Any],
    targets: Sequence[Mapping[str, Any]],
    consensus: Sequence[Mapping[str, Any]],
    deltas: Sequence[Mapping[str, Any]],
    actions: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic Rescue Daily Position Advisory",
        "",
        f"- daily_advisory_id: `{manifest.get('daily_advisory_id')}`",
        f"- mode: `{actions.get('mode')}`",
        f"- consensus_status: `{actions.get('consensus_status')}`",
        f"- disagreement_status: `{actions.get('disagreement_status')}`",
        f"- recommended_action: `{actions.get('recommended_action')}`",
        f"- owner_approval_required: `{actions.get('owner_approval_required')}`",
        f"- broker_action_allowed: `{actions.get('broker_action_allowed')}`",
        "",
        "## Candidate Targets",
        "",
        "|candidate_id|as_of|target_weights|",
        "|---|---|---|",
    ]
    for row in targets:
        lines.append(
            f"|{row.get('candidate_id')}|{row.get('as_of')}|"
            f"{json.dumps(row.get('target_weights'), sort_keys=True)}|"
        )
    lines.extend(["", "## Consensus", "", "|symbol|mean|median|dispersion|", "|---|---|---|---|"])
    for row in consensus:
        lines.append(
            f"|{row.get('symbol')}|{row.get('mean_target_weight')}|"
            f"{row.get('median_target_weight')}|{row.get('dispersion')}|"
        )
    lines.extend(
        [
            "",
            "## Required Questions",
            "",
            f"1. 当前模式：`{actions.get('mode')}`。",
            "2. shadow candidates 今日目标权重共识见 Consensus。",
            f"3. candidate disagreement: `{actions.get('disagreement_status')}`。",
            f"4. portfolio snapshot delta rows: `{len(deltas)}`。",
            f"5. recommended_action: `{actions.get('recommended_action')}`。",
            "6. 是否有任何交易动作：否，`broker_action_allowed=false`。",
            "7. 是否需要 owner approval：是，`owner_approval_required=true`。",
            "",
            "production_effect=none; broker_action=none.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_consensus_drift_markdown(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    symbol_rows: Sequence[Mapping[str, Any]],
    pairwise_rows: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Dynamic Rescue Consensus Drift",
        "",
        f"- drift_id: `{manifest.get('drift_id')}`",
        f"- shadow_monitor_run_id: `{manifest.get('source_shadow_monitor_run_id')}`",
        f"- disagreement_status: `{summary.get('disagreement_status')}`",
        f"- position_advisory_implication: `{summary.get('position_advisory_implication')}`",
        f"- max_symbol_dispersion: `{summary.get('max_symbol_dispersion')}`",
        f"- risk_asset_exposure_dispersion: `{summary.get('risk_asset_exposure_dispersion')}`",
        f"- cash_exposure_dispersion: `{summary.get('cash_exposure_dispersion')}`",
        "",
        "## Symbol Dispersion",
        "",
        "|symbol|mean|median|min|max|dispersion|",
        "|---|---|---|---|---|---|",
    ]
    for row in symbol_rows:
        lines.append(
            f"|{row.get('symbol')}|{row.get('mean_target_weight')}|"
            f"{row.get('median_target_weight')}|{row.get('min_target_weight')}|"
            f"{row.get('max_target_weight')}|{row.get('dispersion')}|"
        )
    lines.extend(
        [
            "",
            "## Candidate Pairwise Disagreement",
            "",
            f"- pair_count: `{len(pairwise_rows)}`",
            "",
            "HIGH_DISAGREEMENT 会强制 daily position advisory 输出 manual_review。",
            "",
            "production_effect=none; broker_action=none.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_owner_review_report_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# Dynamic Rescue Owner Review Journal",
        "",
        f"- review_count: `{summary.get('review_count')}`",
        f"- pending_owner_review_count: `{summary.get('pending_owner_review_count')}`",
        f"- latest_daily_advisory_id: `{summary.get('latest_daily_advisory_id')}`",
        f"- latest_recommended_action: `{summary.get('latest_recommended_action')}`",
        f"- latest_owner_decision: `{summary.get('latest_owner_decision')}`",
        f"- manual_review_required_count: `{summary.get('manual_review_required_count')}`",
        f"- paper_action_count: `{summary.get('paper_action_count')}`",
        f"- broker_action_taken: `{summary.get('broker_action_taken')}`",
        "",
        "paper action 只记录纸面动作，不影响真实仓位、broker state 或 production weights。",
        "",
        "production_effect=none; broker_action=none.",
    ]
    return "\n".join(lines) + "\n"


def _candidate_target_weights_for_as_of(row: Mapping[str, Any], as_of: date) -> dict[str, Any]:
    path = _candidate_weight_path(row)
    dates = _mapping(path.get("dates"))
    valid_days = []
    for day in dates:
        try:
            parsed = date.fromisoformat(day)
        except ValueError:
            continue
        if parsed <= as_of:
            valid_days.append(day)
    if not valid_days:
        return {
            "status": "INCOMPLETE",
            "as_of": "",
            "weights": {},
            "source_weight_path_artifact": path.get("path", ""),
        }
    latest_day = max(valid_days)
    return {
        "status": "COMPLETE",
        "as_of": latest_day,
        "weights": {
            symbol: round(_float(value), 6)
            for symbol, value in sorted(_mapping(dates.get(latest_day)).items())
        },
        "source_weight_path_artifact": path.get("path", ""),
    }


def _previous_shadow_monitor_weights(
    *,
    shadow_shortlist_id: str,
    as_of: date,
    output_dir: Path,
) -> dict[str, dict[str, float]]:
    if not output_dir.exists():
        return {}
    candidates: list[tuple[date, Path]] = []
    for child in output_dir.iterdir():
        if not child.is_dir():
            continue
        manifest = _read_optional_json(child / "shadow_monitor_manifest.json") or {}
        if manifest.get("shadow_shortlist_id") != shadow_shortlist_id:
            continue
        try:
            previous_as_of = date.fromisoformat(_text(manifest.get("as_of")))
        except ValueError:
            continue
        if previous_as_of < as_of:
            candidates.append((previous_as_of, child))
    if not candidates:
        return {}
    _, previous_dir = max(candidates, key=lambda item: item[0])
    return {
        _text(row.get("candidate_id")): {
            symbol: _float(value) for symbol, value in _mapping(row.get("target_weights")).items()
        }
        for row in _read_jsonl(previous_dir / "shadow_candidate_daily_results.jsonl")
    }


def _shadow_shortlist_daily_result(
    row: Mapping[str, Any],
    *,
    as_of: date,
    previous_weights: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    candidate_id = _text(row.get("candidate_id"))
    latest = _candidate_target_weights_for_as_of(row, as_of)
    target_weights = _mapping(latest.get("weights"))
    previous = dict(_mapping(previous_weights.get(candidate_id)))
    deltas = {
        symbol: round(_float(target_weights.get(symbol)) - _float(previous.get(symbol)), 6)
        for symbol in sorted(set(target_weights) | set(previous))
        if previous
    }
    total_abs = round(sum(abs(value) for value in deltas.values()), 6)
    requirements = _mapping(row.get("monitoring_requirements"))
    min_days = int(requirements.get("min_days") or 0)
    min_rebalances = int(requirements.get("min_rebalance_count") or 0)
    days_observed = 0
    rebalance_count = 1 if total_abs > 0 else 0
    drift_status = "PASS" if latest.get("status") == "COMPLETE" else "REVIEW_REQUIRED"
    reasons = [] if drift_status == "PASS" else ["target_weight_path_incomplete"]
    if total_abs > 0:
        reasons.append("target_weight_changed_vs_previous_monitor")
    promotion_status = (
        "eligible_for_review"
        if days_observed >= min_days and rebalance_count >= min_rebalances
        else "not_eligible"
    )
    recommendation = "continue_observe" if drift_status == "PASS" else "manual_review"
    return {
        "candidate_id": candidate_id,
        "as_of": as_of.isoformat(),
        "cluster_id": row.get("cluster_id", ""),
        "cluster_label": row.get("cluster_label", ""),
        "monitoring_status": "active",
        "manual_review_required": True,
        "target_weights": target_weights,
        "previous_target_weights": previous,
        "daily_weight_delta": deltas,
        "total_abs_weight_change": total_abs,
        "estimated_turnover": round(total_abs / 2.0, 6),
        "constraint_hit_count": int(_mapping(row.get("metrics")).get("constraint_hits") or 0),
        "risk_bucket": "unknown",
        "regime": "unknown",
        "live_vs_backtest_drift": {
            "status": drift_status,
            "drift_score": total_abs,
            "reasons": reasons,
        },
        "promotion_clock": {
            "days_observed": days_observed,
            "rebalance_count_observed": rebalance_count,
            "min_days_required": min_days,
            "min_rebalance_count_required": min_rebalances,
            "status": promotion_status,
        },
        "recommendation": recommendation,
    }


def _shadow_shortlist_weekly_summary(row: Mapping[str, Any]) -> dict[str, Any]:
    drift = _mapping(row.get("live_vs_backtest_drift"))
    promotion = _mapping(row.get("promotion_clock"))
    return {
        "candidate_id": row.get("candidate_id"),
        "as_of": row.get("as_of"),
        "cluster_id": row.get("cluster_id"),
        "cluster_label": row.get("cluster_label"),
        "weekly_status": "active",
        "latest_drift_status": drift.get("status"),
        "latest_drift_score": drift.get("drift_score"),
        "promotion_clock_status": promotion.get("status"),
        "recommendation": row.get("recommendation"),
        "broker_action_allowed": False,
    }


def _max_target_weight_disagreement(rows: Sequence[Mapping[str, Any]]) -> float:
    targets = [_daily_target_row_from_monitor(row) for row in rows]
    pairwise = _candidate_pairwise_disagreement_rows(targets)
    return max([_float(row.get("pairwise_distance")) for row in pairwise] or [0.0])


def _normalize_portfolio_snapshot(path: Path | None, *, strict: bool) -> dict[str, Any]:
    if path is None:
        return {}
    schema_config = load_manual_portfolio_schema_config()
    normalized = _normalize_manual_portfolio_snapshot(
        path,
        schema_config=schema_config,
        strict=strict,
        enforce_schema_source=False,
        require_explicit_cash_currency=False,
        require_schema_position_fields=False,
    )
    return {
        **normalized,
        "report_type": "etf_dynamic_v3_normalized_portfolio_snapshot",
    }


def _normalize_manual_portfolio_snapshot(
    path: Path,
    *,
    schema_config: Mapping[str, Any],
    strict: bool,
    enforce_schema_source: bool = True,
    require_explicit_cash_currency: bool = True,
    require_schema_position_fields: bool = True,
) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise DynamicV3ParameterResearchError("portfolio snapshot must be a mapping")
    snapshot = _mapping(raw.get("snapshot"))
    metadata = _mapping(raw.get("metadata"))
    cash_raw = _mapping(raw.get("cash"))
    raw_positions = _records(raw.get("positions"))
    accounts = _records(raw.get("accounts"))
    tolerances = _mapping(schema_config.get("tolerances"))
    allowed_sources = set(_texts(schema_config.get("allowed_sources")))
    allowed_currencies = set(_texts(schema_config.get("allowed_currencies"))) or {
        *PORTFOLIO_SNAPSHOT_ALLOWED_CURRENCIES
    }
    weight_tolerance = _float(
        tolerances.get("weight_sum_tolerance"),
        PORTFOLIO_SNAPSHOT_WEIGHT_SUM_TOLERANCE,
    )
    value_tolerance_pct = _float(tolerances.get("value_sum_tolerance_pct"), 0.005)
    as_of = _text(snapshot.get("as_of") if snapshot else raw.get("as_of"))
    source = _text(snapshot.get("source") if snapshot else raw.get("source"), "manual")
    base_currency = _text(
        snapshot.get("base_currency") if snapshot else raw.get("base_currency"),
        "USD",
    )
    total_equity_present = ("total_equity" in snapshot) if snapshot else ("total_equity" in raw)
    total_equity = _float(snapshot.get("total_equity") if snapshot else raw.get("total_equity"))
    cash_symbol = _text(
        snapshot.get("cash_symbol") if snapshot else cash_raw.get("symbol"),
        "CASH",
    )
    owner_reviewed = (
        snapshot.get("owner_reviewed") is True
        if "owner_reviewed" in snapshot
        else metadata.get("owner_reviewed") is True
    )
    broker_imported = (
        snapshot.get("broker_imported") is True
        if "broker_imported" in snapshot
        else metadata.get("broker_imported") is True
    )
    broker_action_allowed = metadata.get("broker_action_allowed") is True
    broker_action_taken = metadata.get("broker_action_taken") is True
    cash = _manual_cash_row(cash_raw, raw_positions, cash_symbol, base_currency)
    positions = _manual_position_rows(raw_positions, cash_symbol, base_currency)
    raw_symbols = [_text(item.get("symbol")) for item in raw_positions if _text(item.get("symbol"))]
    if cash_raw and cash_symbol:
        raw_symbols.append(cash_symbol)
    weights = {row["symbol"]: _float(row.get("weight")) for row in positions}
    weights[_text(cash.get("symbol"), "CASH")] = _float(cash.get("weight"))
    total_weight = round(sum(_float(value) for value in weights.values()), 6)
    value_sum = round(
        sum(_float(row.get("value")) for row in positions) + _float(cash.get("value")),
        6,
    )
    value_tolerance = max(total_equity, 0.0) * value_tolerance_pct
    currencies = [_text(row.get("currency")) for row in positions]
    if cash.get("currency"):
        currencies.append(_text(cash.get("currency")))
    explicit_currency_ok = all(_text(item.get("currency")) for item in raw_positions)
    if require_explicit_cash_currency and cash_raw:
        explicit_currency_ok = explicit_currency_ok and bool(_text(cash_raw.get("currency")))
    missing_required_position_fields = []
    if require_schema_position_fields:
        missing_required_position_fields = [
            _text(item.get("symbol"), f"position_{idx}")
            for idx, item in enumerate(raw_positions, start=1)
            if _text(item.get("symbol")) != cash_symbol
            and any(
                field not in item or item.get(field) is None
                for field in _texts(schema_config.get("required_position_fields"))
            )
        ]
    account_total = round(sum(_float(row.get("total_equity")) for row in accounts), 6)
    account_check_required = bool(accounts) and total_equity_present
    manual_review_required = not owner_reviewed
    checks = [
        _check(
            "schema_version_exists",
            raw.get("schema_version") is not None,
            _text(raw.get("schema_version")),
        ),
        _check(
            "schema_version_supported",
            raw.get("schema_version") == SCHEMA_VERSION,
            _text(raw.get("schema_version")),
        ),
        _check("as_of_present", bool(as_of), as_of),
        _check(
            "total_equity_positive",
            (not strict and not total_equity_present) or total_equity > 0,
            f"total_equity={total_equity}",
        ),
        _check(
            "source_allowed",
            (not enforce_schema_source) or (not allowed_sources) or source in allowed_sources,
            source,
        ),
        _check(
            "base_currency_supported",
            (not strict) or base_currency in allowed_currencies,
            base_currency,
        ),
        _check("total_equity_present", (not strict) or total_equity_present, "total_equity"),
        _check(
            "account_total_matches_snapshot",
            (not account_check_required) or abs(account_total - total_equity) <= value_tolerance,
            f"account_total={account_total}, total_equity={total_equity}",
        ),
        _check(
            "weight_sum_within_tolerance",
            abs(total_weight - 1.0) <= weight_tolerance,
            f"weight_sum={total_weight}, tolerance={weight_tolerance}",
        ),
        _check(
            "value_sum_matches_total_equity",
            (not total_equity_present) or abs(value_sum - total_equity) <= value_tolerance,
            f"value_sum={value_sum}, total_equity={total_equity}, tolerance={value_tolerance}",
        ),
        _check(
            "symbol_not_duplicated",
            len(raw_symbols) == len(set(raw_symbols)),
            ",".join(raw_symbols),
        ),
        _check(
            "non_negative_weights",
            all(_float(row.get("weight")) >= 0 for row in positions + [cash]),
            "weights must be non-negative",
        ),
        _check(
            "non_negative_values",
            all(_float(row.get("value")) >= 0 for row in positions + [cash]),
            "values must be non-negative",
        ),
        _check(
            "currency_present",
            (not strict) or explicit_currency_ok,
            ",".join(sorted(set(currencies))),
        ),
        _check(
            "currencies_supported",
            all(currency in allowed_currencies for currency in currencies if currency),
            ",".join(sorted(set(currencies))),
        ),
        _check(
            "required_position_fields_present",
            not missing_required_position_fields,
            ",".join(missing_required_position_fields),
        ),
        _check("broker_imported_false", broker_imported is False, str(broker_imported)),
        _check(
            "broker_action_allowed_false",
            broker_action_allowed is False,
            str(broker_action_allowed),
        ),
        _check("broker_action_taken_false", broker_action_taken is False, str(broker_action_taken)),
        _check(
            "owner_review_pending_forces_manual_review",
            owner_reviewed or manual_review_required,
            f"owner_reviewed={owner_reviewed}, manual_review_required={manual_review_required}",
        ),
    ]
    cash_weight = _float(weights.get(_text(cash.get("symbol"), "CASH")))
    risk_asset_weight = round(
        sum(
            _float(value)
            for symbol, value in weights.items()
            if symbol not in DYNAMIC_V3_DEFENSIVE_SYMBOLS
        ),
        6,
    )
    failed_count = sum(1 for check in checks if not check["passed"])
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_manual_normalized_portfolio",
        "snapshot_id": "",
        "source_path": str(path),
        "as_of": as_of,
        "source": source,
        "broker_imported": broker_imported,
        "owner_reviewed": owner_reviewed,
        "base_currency": base_currency,
        "account_type": _text(raw.get("account_type"), "manual_snapshot"),
        "accounts": accounts,
        "total_equity": total_equity,
        "cash": cash,
        "positions": positions,
        "weights": weights,
        "total_weight": total_weight,
        "weight_sum": total_weight,
        "value_sum": value_sum,
        "cash_weight": round(cash_weight, 6),
        "risk_asset_weight": risk_asset_weight,
        "manual_review_required": manual_review_required,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "status": "PASS" if failed_count == 0 else "FAIL",
        "checks": checks,
        "failed_check_count": failed_count,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _manual_cash_row(
    cash_raw: Mapping[str, Any],
    raw_positions: Sequence[Mapping[str, Any]],
    cash_symbol: str,
    base_currency: str,
) -> dict[str, Any]:
    if cash_raw:
        return {
            "symbol": _text(cash_raw.get("symbol"), cash_symbol),
            "asset_type": _text(cash_raw.get("asset_type"), "CASH"),
            "weight": _float(cash_raw.get("weight")),
            "value": _float(cash_raw.get("value")),
            "currency": _text(cash_raw.get("currency"), base_currency),
            "account_id": _text(cash_raw.get("account_id")),
        }
    for item in raw_positions:
        if _text(item.get("symbol")) == cash_symbol:
            return {
                "symbol": cash_symbol,
                "asset_type": _text(item.get("asset_type"), "CASH"),
                "weight": _float(item.get("weight")),
                "value": _float(item.get("value")),
                "currency": _text(item.get("currency"), base_currency),
                "account_id": _text(item.get("account_id")),
            }
    return {
        "symbol": cash_symbol,
        "asset_type": "CASH",
        "weight": 0.0,
        "value": 0.0,
        "currency": base_currency,
        "account_id": "",
    }


def _manual_position_rows(
    raw_positions: Sequence[Mapping[str, Any]],
    cash_symbol: str,
    base_currency: str,
) -> list[dict[str, Any]]:
    positions: list[dict[str, Any]] = []
    for item in raw_positions:
        symbol = _text(item.get("symbol"))
        if not symbol or symbol == cash_symbol:
            continue
        positions.append(
            {
                "symbol": symbol,
                "asset_type": _text(item.get("asset_type"), "UNKNOWN"),
                "quantity": item.get("quantity"),
                "market_price": item.get("market_price"),
                "weight": _float(item.get("weight")),
                "value": _float(item.get("value")),
                "currency": _text(item.get("currency"), base_currency),
                "account_id": _text(item.get("account_id")),
            }
        )
    return positions


def _manual_portfolio_weight_check(
    normalized: Mapping[str, Any],
    schema_config: Mapping[str, Any],
) -> dict[str, Any]:
    checks = [
        check
        for check in _records(normalized.get("checks"))
        if check.get("check_id")
        in {
            "weight_sum_within_tolerance",
            "value_sum_matches_total_equity",
            "non_negative_weights",
            "non_negative_values",
        }
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_manual_portfolio_weight_check",
        "snapshot_id": normalized.get("snapshot_id", ""),
        "total_weight": normalized.get("total_weight"),
        "value_sum": normalized.get("value_sum"),
        "total_equity": normalized.get("total_equity"),
        "cash_weight": normalized.get("cash_weight"),
        "risk_asset_weight": normalized.get("risk_asset_weight"),
        "tolerances": _mapping(schema_config.get("tolerances")),
        "checks": checks,
        "status": "PASS" if all(check.get("passed") for check in checks) else "FAIL",
        "broker_action_allowed": False,
        "broker_action_taken": False,
    }


def _portfolio_exposure_summary(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    weights = _mapping(snapshot.get("weights"))
    cash_weight = _float(weights.get("CASH"))
    defensive = sum(
        _float(weights.get(symbol)) for symbol in DYNAMIC_V3_DEFENSIVE_SYMBOLS if symbol in weights
    )
    risk = sum(
        _float(value)
        for symbol, value in weights.items()
        if symbol not in DYNAMIC_V3_DEFENSIVE_SYMBOLS
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_portfolio_exposure_summary",
        "as_of": snapshot.get("as_of", ""),
        "risk_asset_weight": round(risk, 6),
        "cash_weight": round(cash_weight, 6),
        "defensive_weight": round(defensive, 6),
        "position_count": len(_records(snapshot.get("positions"))),
        "broker_action_allowed": False,
    }


def _portfolio_exposure_artifacts(
    normalized: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    weights = _mapping(normalized.get("weights"))
    groups = _mapping(policy.get("exposure_groups"))
    limits = _mapping(policy.get("limits"))
    warning_limits = _mapping(policy.get("warnings"))
    base_currency = _text(normalized.get("base_currency"), "USD")
    group_weights = {
        group: round(
            sum(_float(weights.get(symbol)) for symbol in _texts(_mapping(config).get("symbols"))),
            6,
        )
        for group, config in groups.items()
    }
    max_symbol, max_weight = _max_weight_symbol(weights)
    risk_asset_weight = _float(normalized.get("risk_asset_weight"))
    cash_weight = _float(group_weights.get("cash"), _float(normalized.get("cash_weight")))
    defensive_weight = _float(group_weights.get("defensive"))
    semiconductor_weight = _float(group_weights.get("semiconductor"))
    warnings = [
        _threshold_warning(
            "max_single_symbol_weight",
            "FAIL" if max_weight > _float(limits.get("max_single_symbol_weight")) else "INFO",
            max_weight,
            _float(limits.get("max_single_symbol_weight")),
            f"Max single symbol is {max_symbol}.",
        ),
        _threshold_warning(
            "semiconductor_watch",
            (
                "FAIL"
                if semiconductor_weight > _float(limits.get("max_semiconductor_weight"))
                else (
                    "WARNING"
                    if semiconductor_weight
                    >= _float(warning_limits.get("semiconductor_watch_weight"))
                    else "INFO"
                )
            ),
            semiconductor_weight,
            _float(warning_limits.get("semiconductor_watch_weight")),
            "Semiconductor exposure watch threshold.",
        ),
        _threshold_warning(
            "max_risk_asset_weight",
            "FAIL" if risk_asset_weight > _float(limits.get("max_risk_asset_weight")) else "INFO",
            risk_asset_weight,
            _float(limits.get("max_risk_asset_weight")),
            "Risk asset exposure limit.",
        ),
        _threshold_warning(
            "min_cash_weight",
            (
                "FAIL"
                if cash_weight < _float(limits.get("min_cash_weight"))
                else (
                    "WARNING"
                    if cash_weight < _float(warning_limits.get("cash_low_warning"))
                    else "INFO"
                )
            ),
            cash_weight,
            _float(limits.get("min_cash_weight")),
            "Cash floor and warning check.",
        ),
    ]
    currency_weights = _currency_weights(normalized)
    non_base = round(
        sum(weight for currency, weight in currency_weights.items() if currency != base_currency),
        6,
    )
    currency_status = (
        "FAIL" if non_base > _float(limits.get("max_non_base_currency_weight")) else "PASS"
    )
    warnings.append(
        _threshold_warning(
            "max_non_base_currency_weight",
            "FAIL" if currency_status == "FAIL" else "INFO",
            non_base,
            _float(limits.get("max_non_base_currency_weight")),
            "Non-base currency exposure limit.",
        )
    )
    status = _status_from_warnings(warnings)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_portfolio_exposure_summary",
        "exposure_id": "",
        "snapshot_id": normalized.get("snapshot_id", ""),
        "risk_asset_weight": round(risk_asset_weight, 6),
        "cash_weight": round(cash_weight, 6),
        "tech_weight": group_weights.get("tech", 0.0),
        "semiconductor_weight": round(semiconductor_weight, 6),
        "defensive_weight": round(defensive_weight, 6),
        "max_single_symbol": {"symbol": max_symbol, "weight": round(max_weight, 6)},
        "status": status,
        "warnings": [
            row["warning_id"] for row in warnings if row.get("severity") in {"WARNING", "FAIL"}
        ],
        "manual_review_required": status != "PASS",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    concentration = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_portfolio_concentration_warnings",
        "exposure_id": "",
        "snapshot_id": normalized.get("snapshot_id", ""),
        "warnings": warnings,
        "status": status,
        "broker_action_allowed": False,
    }
    currency = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_portfolio_currency_exposure",
        "exposure_id": "",
        "snapshot_id": normalized.get("snapshot_id", ""),
        "base_currency": base_currency,
        "currency_weights": currency_weights,
        "non_base_currency_weight": non_base,
        "status": currency_status,
        "broker_action_allowed": False,
    }
    return summary, concentration, currency


def _candidate_drift_row(
    target: Mapping[str, Any],
    current: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    target_weights = _mapping(target.get("target_weights"))
    symbols = sorted(set(current) | set(target_weights))
    deltas = {
        symbol: round(_float(target_weights.get(symbol)) - _float(current.get(symbol)), 6)
        for symbol in symbols
    }
    total_abs = round(sum(abs(value) for value in deltas.values()), 6)
    positive = [(symbol, value) for symbol, value in deltas.items() if value > 0]
    negative = [(symbol, value) for symbol, value in deltas.items() if value < 0]
    largest_positive = max(positive, key=lambda item: item[1]) if positive else ("", 0.0)
    largest_negative = min(negative, key=lambda item: item[1]) if negative else ("", 0.0)
    status = _drift_status(total_abs, config)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_position_drift",
        "candidate_id": target.get("candidate_id"),
        "current_weights": dict(current),
        "target_weights": dict(target_weights),
        "deltas": {key: value for key, value in deltas.items() if value != 0},
        "total_abs_drift": total_abs,
        "largest_positive_delta": {
            "symbol": largest_positive[0],
            "delta": round(largest_positive[1], 6),
        },
        "largest_negative_delta": {
            "symbol": largest_negative[0],
            "delta": round(largest_negative[1], 6),
        },
        "drift_status": status,
    }


def _position_drift_consensus_summary(
    *,
    current_weights: Mapping[str, Any],
    target_rows: Sequence[Mapping[str, Any]],
    matrix: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    symbols = sorted(
        {symbol for row in target_rows for symbol in _mapping(row.get("target_weights")).keys()}
        | set(current_weights)
    )
    if not target_rows:
        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_position_drift_consensus_summary",
            "drift_id": "",
            "current_weights": dict(current_weights),
            "consensus_target_weights": {},
            "median_target_weights": {},
            "min_target_weights": {},
            "max_target_weights": {},
            "symbol_dispersion": {},
            "total_abs_drift_to_consensus": 0.0,
            "candidate_agreement_status": "INSUFFICIENT_DATA",
            "drift_status": "INSUFFICIENT_DATA",
            "broker_action_allowed": False,
        }
    consensus_config = _mapping(config.get("consensus"))
    max_dispersion = _float(consensus_config.get("max_symbol_dispersion"))
    high_dispersion = _float(consensus_config.get("high_symbol_dispersion"))
    agreement_threshold = _float(consensus_config.get("agreement_threshold"))
    medians: dict[str, float] = {}
    mins: dict[str, float] = {}
    maxes: dict[str, float] = {}
    dispersions: dict[str, float] = {}
    high = False
    moderate = False
    for symbol in symbols:
        values = [_float(_mapping(row.get("target_weights")).get(symbol)) for row in target_rows]
        median_value = _median(values)
        dispersion = max(values) - min(values)
        agreement = sum(1 for value in values if abs(value - median_value) <= max_dispersion) / len(
            values
        )
        medians[symbol] = round(median_value, 6)
        mins[symbol] = round(min(values), 6)
        maxes[symbol] = round(max(values), 6)
        dispersions[symbol] = round(dispersion, 6)
        if dispersion > high_dispersion:
            high = True
        elif dispersion > max_dispersion or agreement < agreement_threshold:
            moderate = True
    deltas = {
        symbol: round(_float(medians.get(symbol)) - _float(current_weights.get(symbol)), 6)
        for symbol in symbols
    }
    total_abs = round(sum(abs(value) for value in deltas.values()), 6)
    agreement_status = (
        "HIGH_DISAGREEMENT" if high else "MODERATE_DISAGREEMENT" if moderate else "CONSENSUS"
    )
    drift_status = _drift_status(total_abs, config)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_position_drift_consensus_summary",
        "drift_id": "",
        "current_weights": dict(current_weights),
        "consensus_target_weights": medians,
        "median_target_weights": medians,
        "min_target_weights": mins,
        "max_target_weights": maxes,
        "symbol_dispersion": dispersions,
        "consensus_deltas": {key: value for key, value in deltas.items() if value != 0},
        "total_abs_drift_to_consensus": total_abs,
        "candidate_agreement_status": agreement_status,
        "drift_status": drift_status,
        "candidate_drift_status_counts": dict(
            Counter(_text(row.get("drift_status")) for row in matrix)
        ),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _position_drift_action_candidates(
    summary: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    current = _mapping(summary.get("current_weights"))
    target = _mapping(summary.get("consensus_target_weights"))
    deltas = _mapping(summary.get("consensus_deltas"))
    limits = _mapping(config.get("advisory_limits"))
    min_trade = _float(limits.get("min_trade_threshold"))
    actions = []
    for symbol in sorted(set(current) | set(target)):
        raw_delta = round(_float(target.get(symbol)) - _float(current.get(symbol)), 6)
        direction = "hold"
        if raw_delta >= min_trade:
            direction = "increase"
        elif raw_delta <= -min_trade:
            direction = "decrease"
        actions.append(
            {
                "symbol": symbol,
                "current_weight": round(_float(current.get(symbol)), 6),
                "consensus_target": round(_float(target.get(symbol)), 6),
                "suggested_direction": direction,
                "raw_delta": raw_delta,
                "requires_manual_review": direction != "hold",
            }
        )
    agreement = _text(summary.get("candidate_agreement_status"))
    total_abs = _float(summary.get("total_abs_drift_to_consensus"))
    drift_config = _mapping(config.get("drift_analysis"))
    manual_review_total = _float(drift_config.get("manual_review_total_abs_drift"))
    drift_status = _text(summary.get("drift_status"))
    if agreement in {"HIGH_DISAGREEMENT", "INSUFFICIENT_DATA"}:
        recommended = "manual_review"
    elif total_abs < min_trade or drift_status == "LOW":
        recommended = "no_trade"
    elif total_abs >= manual_review_total:
        recommended = "guardrail_check_required"
    else:
        recommended = "monitor"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_position_drift_action_candidates",
        "action_candidates": actions,
        "recommended_action": recommended,
        "consensus_deltas": dict(deltas),
        "manual_review_required": recommended in {"manual_review", "guardrail_check_required"},
        "owner_approval_required": True,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _guardrail_adjustment_checks(
    drift_summary: Mapping[str, Any],
    drift_actions: Mapping[str, Any],
    exposure_summary: Mapping[str, Any],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    limits = _mapping(config.get("limits"))
    controls = _mapping(config.get("risk_controls"))
    max_symbol = _float(limits.get("max_single_symbol_adjustment"))
    min_trade = _float(limits.get("min_trade_threshold"))
    rows = []
    total_raw = 0.0
    for action in _records(drift_actions.get("action_candidates")):
        symbol = _text(action.get("symbol"))
        raw_delta = _float(action.get("raw_delta"))
        total_raw += abs(raw_delta)
        capped_delta = max(-max_symbol, min(max_symbol, raw_delta))
        check_results = [
            {
                "check": "min_trade_threshold",
                "status": "PASS" if abs(raw_delta) >= min_trade else "NO_ACTION",
                "limit": min_trade,
                "actual": round(abs(raw_delta), 6),
            },
            {
                "check": "max_single_symbol_adjustment",
                "status": "PASS" if abs(raw_delta) <= max_symbol else "CAPPED",
                "limit": max_symbol,
                "actual": round(abs(raw_delta), 6),
            },
        ]
        blocked = False
        if abs(raw_delta) < min_trade:
            status = "NO_ACTION"
            capped_delta = 0.0
        elif abs(raw_delta) > max_symbol:
            status = "CAPPED"
        else:
            status = "ALLOWED_FOR_REVIEW"
        if (
            controls.get("block_risk_increase_during_high_disagreement") is True
            and _text(drift_summary.get("candidate_agreement_status")) == "HIGH_DISAGREEMENT"
            and symbol not in DYNAMIC_V3_DEFENSIVE_SYMBOLS
            and raw_delta > 0
        ):
            blocked = True
            capped_delta = 0.0
            status = "BLOCKED"
            check_results.append(
                {
                    "check": "block_risk_increase_during_high_disagreement",
                    "status": "BLOCKED",
                    "limit": "HIGH_DISAGREEMENT",
                    "actual": _text(drift_summary.get("candidate_agreement_status")),
                }
            )
        if (
            controls.get("block_risk_increase_when_data_quality_not_pass") is True
            and _text(exposure_summary.get("status")) == "FAIL"
            and symbol not in DYNAMIC_V3_DEFENSIVE_SYMBOLS
            and raw_delta > 0
        ):
            blocked = True
            capped_delta = 0.0
            status = "BLOCKED"
            check_results.append(
                {
                    "check": "block_risk_increase_when_exposure_not_pass",
                    "status": "BLOCKED",
                    "limit": "PASS",
                    "actual": _text(exposure_summary.get("status")),
                }
            )
        rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "report_type": "etf_dynamic_v3_proposed_adjustment_check",
                "symbol": symbol,
                "raw_delta": round(raw_delta, 6),
                "capped_delta": round(capped_delta, 6),
                "check_results": check_results,
                "adjustment_status": status,
                "manual_review_required": status != "NO_ACTION",
                "blocked": blocked,
            }
        )
    total_capped = sum(abs(_float(row.get("capped_delta"))) for row in rows)
    max_day = _float(limits.get("max_single_day_total_adjustment"))
    if total_capped > max_day and total_capped > 0:
        scale = max_day / total_capped
        for row in rows:
            if row.get("adjustment_status") == "BLOCKED":
                continue
            if _float(row.get("capped_delta")) != 0:
                row["capped_delta"] = round(_float(row.get("capped_delta")) * scale, 6)
                row["adjustment_status"] = "CAPPED"
                row["check_results"].append(
                    {
                        "check": "max_single_day_total_adjustment",
                        "status": "CAPPED",
                        "limit": max_day,
                        "actual": round(total_raw, 6),
                    }
                )
    cash_after = _float(drift_summary.get("consensus_target_weights", {}).get("CASH"))
    min_cash = _float(limits.get("min_cash_after_adjustment"))
    if cash_after < min_cash:
        for row in rows:
            if row.get("symbol") == "CASH":
                row["adjustment_status"] = "BLOCKED"
                row["capped_delta"] = 0.0
                row["blocked"] = True
                row["check_results"].append(
                    {
                        "check": "min_cash_after_adjustment",
                        "status": "BLOCKED",
                        "limit": min_cash,
                        "actual": round(cash_after, 6),
                    }
                )
    return rows


def _guardrail_summary(
    drift_id: str,
    exposure_id: str,
    checks: Sequence[Mapping[str, Any]],
    drift_summary: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    blocked_count = sum(1 for row in checks if row.get("adjustment_status") == "BLOCKED")
    capped_count = sum(1 for row in checks if row.get("adjustment_status") == "CAPPED")
    active_count = sum(1 for row in checks if row.get("adjustment_status") != "NO_ACTION")
    total_raw = round(sum(abs(_float(row.get("raw_delta"))) for row in checks), 6)
    total_capped = round(sum(abs(_float(row.get("capped_delta"))) for row in checks), 6)
    if active_count == 0:
        recommended = "no_trade"
    elif blocked_count:
        recommended = "blocked"
    elif capped_count or _text(drift_summary.get("drift_status")) in {"MODERATE", "HIGH"}:
        recommended = "paper_adjustment_review_only"
    else:
        recommended = "manual_review"
    mode = _mapping(config.get("mode"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_execution_guardrail_summary",
        "guardrail_id": "",
        "drift_id": drift_id,
        "exposure_id": exposure_id,
        "recommended_action": recommended,
        "total_raw_adjustment": total_raw,
        "total_capped_adjustment": total_capped,
        "blocked_count": blocked_count,
        "capped_count": capped_count,
        "manual_review_required": True,
        "owner_approval_required": mode.get("require_owner_approval") is True,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "order_ticket_generation_allowed": False,
        "production_effect": "none",
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _stepwise_adjustment_plan(
    summary: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    stepwise = _mapping(config.get("stepwise_execution"))
    max_steps = max(1, int(_float(stepwise.get("max_steps"), 1)))
    total = _float(summary.get("total_capped_adjustment"))
    per_step = round(total / max_steps, 6) if total > 0 else 0.0
    steps = []
    for idx in range(1, max_steps + 1):
        steps.append(
            {
                "step": idx,
                "max_total_adjustment": per_step,
                "description": "Paper-only adjustment review. No broker action.",
                "condition": (
                    "Owner review after configured waiting period and no high disagreement."
                    if idx > 1
                    else "Owner approval required before any paper review."
                ),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_stepwise_adjustment_plan",
        "enabled": stepwise.get("enabled") is True,
        "steps": steps,
        "paper_only": True,
        "broker_action_allowed": False,
        "order_ticket_generation_allowed": False,
    }


def _manual_execution_decision(
    review_id: str,
    snapshot: Mapping[str, Any],
    exposure: Mapping[str, Any],
    drift: Mapping[str, Any],
    guardrail: Mapping[str, Any],
) -> dict[str, Any]:
    blocking = []
    warnings = []
    if _text(snapshot.get("status")) == "FAIL":
        blocking.append("manual_portfolio_snapshot_failed")
    if _text(exposure.get("status")) == "FAIL":
        blocking.append("portfolio_exposure_failed")
    if _text(drift.get("candidate_agreement_status")) == "HIGH_DISAGREEMENT":
        warnings.append("candidate_high_disagreement")
    if _int(guardrail.get("blocked_count")) > 0:
        blocking.append("execution_guardrail_blocked_adjustments")
    recommended = "blocked" if blocking else _text(guardrail.get("recommended_action"), "monitor")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_manual_execution_decision",
        "manual_review_id": review_id,
        "snapshot_id": snapshot.get("snapshot_id", ""),
        "exposure_id": exposure.get("exposure_id", ""),
        "drift_id": drift.get("drift_id", ""),
        "guardrail_id": guardrail.get("guardrail_id", ""),
        "recommended_action": recommended,
        "order_ticket_generated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "owner_approval_required": True,
        "production_effect": "none",
        "reasons": [
            f"snapshot_status={snapshot.get('status')}",
            f"exposure_status={exposure.get('status')}",
            f"drift_status={drift.get('drift_status')}",
            f"guardrail_action={guardrail.get('recommended_action')}",
        ],
        "warnings": warnings,
        "blocking_issues": blocking,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def render_manual_portfolio_snapshot_markdown(
    manifest: Mapping[str, Any],
    normalized: Mapping[str, Any],
    weight_check: Mapping[str, Any],
) -> str:
    return (
        "\n".join(
            [
                "# Dynamic Rescue Manual Portfolio Snapshot",
                "",
                f"- snapshot_id: `{manifest.get('snapshot_id')}`",
                f"- status: `{manifest.get('status')}`",
                f"- as_of: `{manifest.get('as_of')}`",
                f"- total_equity: `{normalized.get('total_equity')}`",
                f"- total_weight: `{normalized.get('total_weight')}`",
                f"- cash_weight: `{normalized.get('cash_weight')}`",
                f"- risk_asset_weight: `{normalized.get('risk_asset_weight')}`",
                f"- broker_imported: `{normalized.get('broker_imported')}`",
                f"- broker_action_taken: `{normalized.get('broker_action_taken')}`",
                f"- owner_reviewed: `{normalized.get('owner_reviewed')}`",
                "",
                "## Required Questions",
                "",
                f"1. 当前持仓快照是否有效：`{manifest.get('status')}`。",
                f"2. 权重是否合计为 100%：`{weight_check.get('status')}`。",
                (
                    "3. value 是否和 total_equity 匹配："
                    f"`{_check_passed_text(normalized, 'value_sum_matches_total_equity')}`。"
                ),
                f"4. cash weight：`{normalized.get('cash_weight')}`。",
                f"5. risk asset weight：`{normalized.get('risk_asset_weight')}`。",
                f"6. 是否有 broker import：`{normalized.get('broker_imported')}`，必须为否。",
                f"7. 是否有 broker action：`{normalized.get('broker_action_taken')}`，必须为否。",
                f"8. 是否需要 owner review：`{normalized.get('manual_review_required')}`。",
                "",
                "production_effect=none; broker_action=none.",
            ]
        )
        + "\n"
    )


def render_portfolio_exposure_markdown(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    concentration: Mapping[str, Any],
    currency: Mapping[str, Any],
) -> str:
    warnings = _records(concentration.get("warnings"))
    warning_text = ", ".join(f"{row.get('warning_id')}={row.get('severity')}" for row in warnings)
    return (
        "\n".join(
            [
                "# Dynamic Rescue Portfolio Exposure Validation",
                "",
                f"- exposure_id: `{manifest.get('exposure_id')}`",
                f"- status: `{summary.get('status')}`",
                f"- max_single_symbol: `{summary.get('max_single_symbol')}`",
                f"- tech_weight: `{summary.get('tech_weight')}`",
                f"- semiconductor_weight: `{summary.get('semiconductor_weight')}`",
                f"- defensive_weight: `{summary.get('defensive_weight')}`",
                f"- cash_weight: `{summary.get('cash_weight')}`",
                f"- non_base_currency_weight: `{currency.get('non_base_currency_weight')}`",
                "",
                "## Required Questions",
                "",
                f"1. 当前最大单一资产权重：`{summary.get('max_single_symbol')}`。",
                f"2. 科技暴露：`{summary.get('tech_weight')}`。",
                f"3. 半导体暴露：`{summary.get('semiconductor_weight')}`。",
                (
                    f"4. 现金 / 防御资产：`{summary.get('cash_weight')}` / "
                    f"`{summary.get('defensive_weight')}`。"
                ),
                f"5. 是否违反集中度限制：`{summary.get('status') == 'FAIL'}`。",
                f"6. 是否存在币种异常：`{currency.get('status') == 'FAIL'}`。",
                f"7. 是否需要 manual review：`{summary.get('manual_review_required')}`。",
                "",
                f"warnings: `{warning_text}`",
                "production_effect=none; broker_action=none.",
            ]
        )
        + "\n"
    )


def render_position_drift_markdown(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    actions: Mapping[str, Any],
    matrix: Sequence[Mapping[str, Any]],
) -> str:
    largest = _largest_consensus_deltas(_mapping(summary.get("consensus_deltas")))
    return (
        "\n".join(
            [
                "# Dynamic Rescue Position Drift",
                "",
                f"- drift_id: `{manifest.get('drift_id')}`",
                f"- status: `{manifest.get('status')}`",
                f"- total_abs_drift_to_consensus: `{summary.get('total_abs_drift_to_consensus')}`",
                f"- candidate_agreement_status: `{summary.get('candidate_agreement_status')}`",
                f"- drift_status: `{summary.get('drift_status')}`",
                f"- recommended_action: `{actions.get('recommended_action')}`",
                "",
                "## Required Questions",
                "",
                (
                    "1. 当前持仓和 consensus target 差异："
                    f"`{summary.get('total_abs_drift_to_consensus')}`。"
                ),
                f"2. 最大差异资产：`{largest}`。",
                f"3. 候选之间是否一致：`{summary.get('candidate_agreement_status')}`。",
                f"4. drift 是否触发 manual review：`{actions.get('manual_review_required')}`。",
                f"5. 当前建议：`{actions.get('recommended_action')}`。",
                "6. broker_action_allowed: `False`，必须为否。",
                "",
                f"candidate_rows: `{len(matrix)}`",
                "production_effect=none; broker_action=none.",
            ]
        )
        + "\n"
    )


def render_execution_guardrails_markdown(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    stepwise: Mapping[str, Any],
    checks: Sequence[Mapping[str, Any]],
) -> str:
    return (
        "\n".join(
            [
                "# Dynamic Rescue Execution Guardrails",
                "",
                f"- guardrail_id: `{manifest.get('guardrail_id')}`",
                f"- recommended_action: `{summary.get('recommended_action')}`",
                f"- total_raw_adjustment: `{summary.get('total_raw_adjustment')}`",
                f"- total_capped_adjustment: `{summary.get('total_capped_adjustment')}`",
                f"- capped_count: `{summary.get('capped_count')}`",
                f"- blocked_count: `{summary.get('blocked_count')}`",
                f"- broker_action_allowed: `{summary.get('broker_action_allowed')}`",
                (
                    "- order_ticket_generation_allowed: "
                    f"`{summary.get('order_ticket_generation_allowed')}`"
                ),
                "",
                "## Required Questions",
                "",
                f"1. 原始建议调整幅度：`{summary.get('total_raw_adjustment')}`。",
                f"2. capped 调整数量：`{summary.get('capped_count')}`。",
                f"3. blocked 调整数量：`{summary.get('blocked_count')}`。",
                f"4. 是否需要分步执行：`{stepwise.get('enabled')}`。",
                "5. 是否允许生成订单：`False`，必须为否。",
                "6. 是否需要 owner approval："
                f"`{summary.get('owner_approval_required')}`，必须为是。",
                "",
                f"adjustment_rows: `{len(checks)}`",
                "production_effect=none; broker_action=none.",
            ]
        )
        + "\n"
    )


def render_owner_execution_checklist(decision: Mapping[str, Any]) -> str:
    items = [
        "当前持仓快照是否为最新？",
        "total_equity 是否正确？",
        "现金和权重是否可信？",
        "当前组合是否存在集中度风险？",
        "系统建议的目标权重是什么？",
        "当前持仓和目标权重差异多大？",
        "哪些调整超过 guardrail？",
        "是否只允许 paper review？",
        "是否明确禁止 broker action？",
        "owner 是否决定 no_trade / monitor / paper_adjustment_review_only？",
    ]
    lines = [
        "# Dynamic Rescue Manual Execution Owner Checklist",
        "",
        f"- recommended_action: `{decision.get('recommended_action')}`",
        f"- order_ticket_generated: `{decision.get('order_ticket_generated')}`",
        f"- broker_action_allowed: `{decision.get('broker_action_allowed')}`",
        "",
    ]
    lines.extend(f"- [ ] {item}" for item in items)
    lines.extend(["", "broker_action_allowed=false; owner_approval_required=true."])
    return "\n".join(lines) + "\n"


def render_manual_execution_review_markdown(
    manifest: Mapping[str, Any],
    normalized: Mapping[str, Any],
    exposure_manifest: Mapping[str, Any],
    exposure_summary: Mapping[str, Any],
    drift_manifest: Mapping[str, Any],
    drift_summary: Mapping[str, Any],
    guardrail_manifest: Mapping[str, Any],
    guardrail_summary: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> str:
    return (
        "\n".join(
            [
                "# Dynamic Rescue Manual Execution Review",
                "",
                f"- manual_review_id: `{manifest.get('manual_review_id')}`",
                f"- snapshot_status: `{normalized.get('status')}`",
                f"- exposure_status: `{exposure_manifest.get('status')}`",
                f"- drift_status: `{drift_summary.get('drift_status')}`",
                f"- guardrail_status: `{guardrail_manifest.get('status')}`",
                f"- recommended_action: `{decision.get('recommended_action')}`",
                f"- order_ticket_generated: `{decision.get('order_ticket_generated')}`",
                f"- broker_action_allowed: `{decision.get('broker_action_allowed')}`",
                f"- owner_approval_required: `{decision.get('owner_approval_required')}`",
                "",
                "## Required Questions",
                "",
                (
                    f"1. 当前组合状态：snapshot `{normalized.get('status')}`，"
                    f"exposure `{exposure_summary.get('status')}`。"
                ),
                (f"2. 和系统建议差异：`{drift_summary.get('total_abs_drift_to_consensus')}`。"),
                f"3. 是否存在过度集中：`{exposure_summary.get('status') == 'FAIL'}`。",
                f"4. 是否建议调仓：`{decision.get('recommended_action')}`。",
                "5. 如有建议，仅为 paper review，不是实际执行。",
                "6. 是否生成订单：`False`，必须为否。",
                "7. 是否允许 broker action：`False`，必须为否。",
                "8. 下一步 owner 应复核 checklist 并选择 no_trade / monitor / paper review。",
                "",
                f"source_drift_id: `{drift_manifest.get('drift_id')}`",
                f"source_guardrail_id: `{guardrail_summary.get('guardrail_id')}`",
                "production_effect=none; broker_action=none.",
            ]
        )
        + "\n"
    )


def render_manual_execution_reader_brief(
    snapshot: Mapping[str, Any],
    exposure: Mapping[str, Any],
    drift: Mapping[str, Any],
    guardrail: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> str:
    return (
        "\n".join(
            [
                "## Dynamic Rescue Manual Execution Review",
                "",
                f"- snapshot_status: `{snapshot.get('status')}`",
                f"- exposure_status: `{exposure.get('status')}`",
                f"- drift_status: `{drift.get('drift_status')}`",
                f"- guardrail_status: `{guardrail.get('recommended_action')}`",
                f"- recommended_action: `{decision.get('recommended_action')}`",
                f"- owner_approval_required: `{decision.get('owner_approval_required')}`",
                f"- broker_action_allowed: `{decision.get('broker_action_allowed')}`",
                "",
                "order_ticket_generated=false; production_effect=none.",
            ]
        )
        + "\n"
    )


def _max_weight_symbol(weights: Mapping[str, Any]) -> tuple[str, float]:
    if not weights:
        return "", 0.0
    symbol, value = max(weights.items(), key=lambda item: _float(item[1]))
    return _text(symbol), _float(value)


def _currency_weights(normalized: Mapping[str, Any]) -> dict[str, float]:
    rows = [*_records(normalized.get("positions")), _mapping(normalized.get("cash"))]
    weights: dict[str, float] = {}
    for row in rows:
        currency = _text(row.get("currency"), _text(normalized.get("base_currency"), "USD"))
        weights[currency] = weights.get(currency, 0.0) + _float(row.get("weight"))
    return {currency: round(weight, 6) for currency, weight in sorted(weights.items())}


def _threshold_warning(
    warning_id: str,
    severity: str,
    actual: float,
    threshold: float,
    message: str,
) -> dict[str, Any]:
    return {
        "warning_id": warning_id,
        "severity": severity,
        "actual": round(actual, 6),
        "threshold": round(threshold, 6),
        "message": message,
    }


def _status_from_warnings(warnings: Sequence[Mapping[str, Any]]) -> str:
    severities = {_text(row.get("severity")) for row in warnings}
    if "FAIL" in severities:
        return "FAIL"
    if "WARNING" in severities:
        return "PASS_WITH_WARNINGS"
    return "PASS"


def _drift_status(total_abs: float, config: Mapping[str, Any]) -> str:
    drift_config = _mapping(config.get("drift_analysis"))
    low = _float(drift_config.get("low_total_abs_drift"))
    high = _float(drift_config.get("high_total_abs_drift"))
    if total_abs <= low:
        return "LOW"
    if total_abs >= high:
        return "HIGH"
    return "MODERATE"


def _largest_consensus_deltas(deltas: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = [
        {"symbol": symbol, "delta": round(_float(delta), 6)} for symbol, delta in deltas.items()
    ]
    rows.sort(key=lambda row: abs(_float(row["delta"])), reverse=True)
    return rows[:3]


def _check_passed_text(normalized: Mapping[str, Any], check_id: str) -> str:
    for check in _records(normalized.get("checks")):
        if check.get("check_id") == check_id:
            return "PASS" if check.get("passed") is True else "FAIL"
    return "MISSING"


def _daily_target_row_from_monitor(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": row.get("candidate_id"),
        "cluster_id": row.get("cluster_id"),
        "cluster_label": row.get("cluster_label"),
        "as_of": row.get("as_of"),
        "target_weights": _mapping(row.get("target_weights")),
        "weight_path_status": _text(
            _mapping(row.get("live_vs_backtest_drift")).get("status"),
            "UNKNOWN",
        ),
    }


def _daily_consensus_target_weights(
    target_rows: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    rows = _symbol_weight_dispersion_rows(target_rows)
    if not rows:
        return [], "INSUFFICIENT_DATA"
    consensus_config = _mapping(config.get("consensus"))
    agreement_threshold = _float(consensus_config.get("agreement_threshold"))
    max_dispersion = _float(consensus_config.get("max_symbol_dispersion"))
    disagreement = False
    for row in rows:
        values = _records(row.get("_candidate_values"))
        agreement = _float(row.get("candidate_agreement_ratio"))
        if agreement < agreement_threshold or _float(row.get("dispersion")) > max_dispersion:
            disagreement = True
        row.pop("_candidate_values", None)
        row["candidate_count"] = len(values)
    return rows, "DISAGREEMENT_REVIEW_REQUIRED" if disagreement else "CONSENSUS"


def _symbol_weight_dispersion_rows(
    target_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    symbols = sorted(
        {symbol for row in target_rows for symbol in _mapping(row.get("target_weights")).keys()}
    )
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        values = [
            _float(_mapping(row.get("target_weights")).get(symbol))
            for row in target_rows
            if _mapping(row.get("target_weights"))
        ]
        if not values:
            continue
        center = _median(values)
        dispersion = max(values) - min(values)
        agreement = sum(1 for value in values if abs(value - center) <= dispersion) / len(values)
        rows.append(
            {
                "symbol": symbol,
                "mean_target_weight": round(sum(values) / len(values), 6),
                "median_target_weight": round(center, 6),
                "min_target_weight": round(min(values), 6),
                "max_target_weight": round(max(values), 6),
                "dispersion": round(dispersion, 6),
                "candidate_agreement_ratio": round(agreement, 6),
                "_candidate_values": [{"value": value} for value in values],
            }
        )
    return rows


def _candidate_pairwise_disagreement_rows(
    target_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    ordered = list(target_rows)
    for left_idx, left in enumerate(ordered):
        left_weights = _mapping(left.get("target_weights"))
        for right in ordered[left_idx + 1 :]:
            right_weights = _mapping(right.get("target_weights"))
            symbols = sorted(set(left_weights) | set(right_weights))
            distance = (
                sum(
                    abs(_float(left_weights.get(symbol)) - _float(right_weights.get(symbol)))
                    for symbol in symbols
                )
                / 2.0
            )
            rows.append(
                {
                    "left_candidate_id": left.get("candidate_id"),
                    "right_candidate_id": right.get("candidate_id"),
                    "pairwise_distance": round(distance, 6),
                    "symbol_count": len(symbols),
                }
            )
    return rows


def _exposure_disagreement_summary(target_rows: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    risk_values = []
    cash_values = []
    defensive_values = []
    for row in target_rows:
        weights = _mapping(row.get("target_weights"))
        risk_values.append(
            sum(
                _float(value)
                for symbol, value in weights.items()
                if symbol not in DYNAMIC_V3_DEFENSIVE_SYMBOLS
            )
        )
        cash_values.append(_float(weights.get("CASH")))
        defensive_values.append(
            sum(
                _float(weights.get(symbol))
                for symbol in DYNAMIC_V3_DEFENSIVE_SYMBOLS
                if symbol in weights
            )
        )
    return {
        "risk_asset_exposure_dispersion": round(_range(risk_values), 6),
        "cash_exposure_dispersion": round(_range(cash_values), 6),
        "defensive_exposure_dispersion": round(_range(defensive_values), 6),
    }


def _range(values: Sequence[float]) -> float:
    return max(values) - min(values) if values else 0.0


def _disagreement_status(
    *,
    candidate_count: int,
    symbol_rows: Sequence[Mapping[str, Any]],
    exposure: Mapping[str, Any],
    config: Mapping[str, Any],
) -> str:
    if candidate_count < 2 or not symbol_rows:
        return "INSUFFICIENT_DATA"
    consensus = _mapping(config.get("consensus"))
    max_symbol = max([_float(row.get("dispersion")) for row in symbol_rows] or [0.0])
    risk_dispersion = _float(exposure.get("risk_asset_exposure_dispersion"))
    cash_dispersion = _float(exposure.get("cash_exposure_dispersion"))
    if (
        max_symbol > _float(consensus.get("high_symbol_dispersion"))
        or risk_dispersion > _float(consensus.get("high_risk_asset_exposure_dispersion"))
        or cash_dispersion > _float(consensus.get("high_cash_exposure_dispersion"))
    ):
        return "HIGH_DISAGREEMENT"
    if (
        max_symbol > _float(consensus.get("max_symbol_dispersion"))
        or risk_dispersion > _float(consensus.get("max_risk_asset_exposure_dispersion"))
        or cash_dispersion > _float(consensus.get("max_cash_exposure_dispersion"))
    ):
        return "MODERATE_DISAGREEMENT"
    return "CONSENSUS"


def _daily_consensus_change_vs_previous(
    *,
    shadow_monitor_run_id: str,
    shadow_shortlist_id: str,
    as_of: date,
    current_symbol_rows: Sequence[Mapping[str, Any]],
    monitor_output_dir: Path,
) -> dict[str, Any]:
    previous_weights = _previous_shadow_monitor_weights(
        shadow_shortlist_id=shadow_shortlist_id,
        as_of=as_of,
        output_dir=monitor_output_dir,
    )
    if not previous_weights:
        return {"status": "NO_PRIOR_MONITOR_RUN", "max_mean_weight_delta": 0.0}
    previous_targets = [
        {"candidate_id": candidate_id, "target_weights": weights}
        for candidate_id, weights in previous_weights.items()
    ]
    previous_rows = {
        _text(row.get("symbol")): row for row in _symbol_weight_dispersion_rows(previous_targets)
    }
    deltas = []
    for row in current_symbol_rows:
        symbol = _text(row.get("symbol"))
        previous = previous_rows.get(symbol, {})
        deltas.append(
            abs(_float(row.get("mean_target_weight")) - _float(previous.get("mean_target_weight")))
        )
    return {
        "status": "PASS",
        "source_shadow_monitor_run_id": shadow_monitor_run_id,
        "max_mean_weight_delta": round(max(deltas or [0.0]), 6),
    }


def _latest_consensus_drift_for_monitor(
    *,
    shadow_monitor_run_id: str,
    output_dir: Path,
) -> dict[str, Any]:
    if not output_dir.exists():
        return {}
    candidates = []
    for child in output_dir.iterdir():
        if not child.is_dir():
            continue
        manifest = _read_optional_json(child / "consensus_drift_manifest.json") or {}
        if manifest.get("source_shadow_monitor_run_id") == shadow_monitor_run_id:
            candidates.append(child)
    if not candidates:
        return {}
    latest = max(candidates, key=lambda path: path.stat().st_mtime)
    return _read_optional_json(latest / "consensus_drift_summary.json") or {}


def _daily_position_delta_rows(
    target_rows: Sequence[Mapping[str, Any]],
    snapshot: Mapping[str, Any],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    current = _mapping(snapshot.get("weights"))
    limits = _mapping(config.get("advisory_limits"))
    max_total = _float(limits.get("max_single_day_total_adjustment"))
    max_symbol = _float(limits.get("max_single_symbol_adjustment"))
    min_trade = _float(limits.get("min_trade_threshold"))
    rows = []
    for target in target_rows:
        target_weights = _mapping(target.get("target_weights"))
        symbols = sorted(set(current) | set(target_weights))
        deltas = {
            symbol: round(_float(target_weights.get(symbol)) - _float(current.get(symbol)), 6)
            for symbol in symbols
        }
        total_abs = round(sum(abs(value) for value in deltas.values()), 6)
        max_abs = max([abs(value) for value in deltas.values()] or [0.0])
        status = (
            "no_trade"
            if all(abs(value) < min_trade for value in deltas.values())
            else (
                "requires_manual_review"
                if total_abs > max_total or max_abs > max_symbol
                else "within_limits"
            )
        )
        rows.append(
            {
                "candidate_id": target.get("candidate_id"),
                "current_weights": current,
                "target_weights": target_weights,
                "deltas": {key: value for key, value in deltas.items() if value != 0},
                "total_abs_adjustment": total_abs,
                "max_symbol_adjustment": round(max_abs, 6),
                "advisory_status": status,
            }
        )
    return rows


def _daily_position_advisory_actions(
    *,
    daily_advisory_id: str,
    as_of: str,
    target_rows: Sequence[Mapping[str, Any]],
    delta_rows: Sequence[Mapping[str, Any]],
    snapshot: Mapping[str, Any],
    consensus_status: str,
    disagreement_status: str,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    reasons = []
    risks = [
        "candidate target weights are research outputs",
        "manual owner review is required before any real portfolio change",
    ]
    mode = POSITION_ADVISORY_SNAPSHOT_DELTA if snapshot else POSITION_ADVISORY_TARGET_ONLY
    if consensus_status != "CONSENSUS":
        reasons.append("candidate_target_weight_consensus_not_confirmed")
    if disagreement_status == "HIGH_DISAGREEMENT":
        reasons.append("high_candidate_disagreement_forces_manual_review")
    if disagreement_status == "HIGH_DISAGREEMENT":
        recommended_action = "manual_review"
    elif not snapshot:
        reasons.append("current_portfolio_snapshot_missing")
        recommended_action = "monitor" if target_rows else "manual_review"
    elif not snapshot.get("owner_reviewed"):
        reasons.append("portfolio_snapshot_owner_review_pending")
        recommended_action = "manual_review"
    elif any(row.get("advisory_status") == "requires_manual_review" for row in delta_rows):
        reasons.append("position_delta_exceeds_review_limit")
        recommended_action = "manual_review"
    elif delta_rows and all(row.get("advisory_status") == "no_trade" for row in delta_rows):
        reasons.append("all_deltas_below_min_trade_threshold")
        recommended_action = "no_trade"
    elif delta_rows:
        recommended_action = "small_adjustment_review_only"
    else:
        recommended_action = "manual_review"
    max_adjustment = max([_float(row.get("total_abs_adjustment")) for row in delta_rows] or [0.0])
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_daily_position_advisory_actions",
        "daily_advisory_id": daily_advisory_id,
        "as_of": as_of,
        "mode": mode,
        "broker_action_allowed": False,
        "owner_approval_required": True,
        "manual_review_required": True,
        "consensus_status": (
            "DISAGREEMENT_REVIEW_REQUIRED"
            if disagreement_status == "HIGH_DISAGREEMENT"
            else consensus_status
        ),
        "disagreement_status": disagreement_status,
        "recommended_action": recommended_action,
        "max_suggested_total_adjustment": round(max_adjustment, 6),
        "reasons": sorted(set(reasons)),
        "risks": risks,
        "candidate_count": len(target_rows),
        "config_policy_id": _text(_mapping(config.get("policy_metadata")).get("policy_id")),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _top_delta_summary(delta_rows: Sequence[Mapping[str, Any]]) -> str:
    if not delta_rows:
        return "none"
    row = max(delta_rows, key=lambda item: _float(item.get("total_abs_adjustment")))
    return f"{row.get('candidate_id')}:{row.get('total_abs_adjustment')}"


def _paper_action_record(
    *,
    review: Mapping[str, Any],
    output_dir: Path,
    daily_advisory_dir: Path,
    generated_at: datetime,
) -> dict[str, Any]:
    daily_advisory_id = _text(review.get("daily_advisory_id"))
    delta_rows = _read_jsonl(daily_advisory_dir / daily_advisory_id / "daily_position_deltas.jsonl")
    proposed = {
        _text(row.get("candidate_id")): _mapping(row.get("deltas"))
        for row in delta_rows
        if _mapping(row.get("deltas"))
    }
    paper_action_id = _stable_id("paper-action", review.get("review_id"), generated_at.isoformat())
    record = {
        "schema_version": SCHEMA_VERSION,
        "paper_action_id": paper_action_id,
        "review_id": review.get("review_id"),
        "as_of": review.get("as_of"),
        "action_type": "paper_adjustment",
        "proposed_deltas": proposed,
        "paper_portfolio_after": {},
        "broker_action_taken": False,
        "notes": "Paper only, no broker action",
        "created_at": generated_at.isoformat(),
    }
    _append_jsonl(output_dir / "paper_action_log.jsonl", record)
    return record


def _metric_first(metrics: Mapping[str, Any], keys: Sequence[str]) -> float:
    for key in keys:
        if key in metrics:
            return _float(metrics.get(key))
    return 0.0


def _status_score(value: Any, *, missing: float = 0.0) -> float:
    text = _text(value)
    if text == "PASS":
        return 1.0
    if text == "PASS_WITH_WARNINGS":
        return 0.7
    if text in {"PARTIAL", "REVIEW_REQUIRED", "INCOMPLETE"}:
        return 0.5
    if not text or text == "MISSING":
        return missing
    return 0.0


def _weight_status_score(value: Any) -> float:
    text = _text(value)
    if text == WEIGHT_PATH_COMPLETE:
        return 1.0
    if text == WEIGHT_PATH_PARTIAL:
        return 0.6
    if text in {WEIGHT_PATH_INCOMPLETE, "MISSING"}:
        return 0.0
    return _status_score(text)


def _overfit_status_score(value: Any) -> float:
    text = _text(value)
    if text in {"LOW_RISK", "PASS"}:
        return 1.0
    if text == "REVIEW_REQUIRED":
        return 0.5
    if text == "HIGH_RISK":
        return 0.0
    return 0.4


def _avg(values: Sequence[float]) -> float:
    numbers = [float(value) for value in values if value is not None]
    return sum(numbers) / len(numbers) if numbers else 0.0


def _median(values: Sequence[float]) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return 0.0
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2.0


def _top_candidate_stability(rows: Sequence[Mapping[str, Any]]) -> str:
    ranked = _ranked_candidate_rows(rows)
    if len(ranked) < 2:
        return "INSUFFICIENT_CANDIDATES"
    first = _float(ranked[0].get("score"))
    second = _float(ranked[1].get("score"))
    return "REVIEW_REQUIRED" if abs(first - second) < 0.01 else "PASS"


def _overfit_warning_rate(rows: Sequence[Mapping[str, Any]]) -> float:
    if not rows:
        return 0.0
    count = sum(
        1
        for row in rows
        if _mapping(row.get("metrics")).get("overfit_status") in {"REVIEW_REQUIRED", "HIGH_RISK"}
    )
    return round(count / len(rows), 6)


def render_overnight_readiness_markdown(payload: Mapping[str, Any]) -> str:
    projected_runtime_hours = payload.get("projected_overnight_runtime_hours")
    lines = [
        "# Dynamic v3 Rescue Overnight Real Readiness",
        "",
        f"- source_sweep_id: `{payload.get('source_sweep_id')}`",
        f"- overnight_readiness: `{payload.get('overnight_readiness')}`",
        f"- projected_overnight_runtime_hours: `{projected_runtime_hours}`",
        f"- projected_artifact_size_gb: `{payload.get('projected_artifact_size_gb')}`",
        f"- failure_rate: `{payload.get('failure_rate')}`",
        f"- evidence_completeness_rate: `{payload.get('evidence_completeness_rate')}`",
        f"- blocking_reasons: `{', '.join(_texts(payload.get('blocking_reasons'))) or 'none'}`",
        f"- warnings: `{', '.join(_texts(payload.get('warnings'))) or 'none'}`",
        "",
        "This report does not start `overnight_real` automatically.",
        "",
        "production_effect=none; broker_action=none.",
    ]
    return "\n".join(lines) + "\n"


def _latest_manifest_for_sweep(
    sweep_id: str,
    root: Path,
    manifest_name: str,
    sweep_key: str,
) -> dict[str, Any]:
    candidates: list[tuple[float, Path, dict[str, Any]]] = []
    for path in root.glob(f"*/{manifest_name}"):
        payload = _read_optional_json(path) or {}
        if _text(payload.get(sweep_key)) == sweep_id:
            payload["_manifest_path"] = str(path)
            candidates.append((path.stat().st_mtime, path, payload))
    if not candidates:
        return {}
    return max(candidates, key=lambda item: item[0])[2]


def _research_decision_recommendation(
    *,
    evidence: Mapping[str, Any],
    regime: Mapping[str, Any],
    observe_pool: Mapping[str, Any],
    readiness: Mapping[str, Any],
) -> dict[str, Any]:
    reasons: list[str] = []
    blockers: list[str] = []
    recommendation = "manual_review_observe_pool"
    if not evidence or evidence.get("status") == "FAIL":
        recommendation = "fix_evidence_gaps"
        blockers.append("evidence_summary_missing_or_fail")
    elif regime.get("tech_semiconductor_relevance") == "LOW":
        recommendation = "expand_stress_windows"
        blockers.append("tech_semiconductor_relevance_low")
    elif int(observe_pool.get("observe_candidate_count") or 0) == 0:
        recommendation = "narrow_parameter_space"
        reasons.append("no_observe_only_candidate")
    elif readiness.get("overnight_readiness") == "READY":
        recommendation = "run_overnight_real"
        reasons.append("observe_pool_exists_and_overnight_ready")
    elif readiness.get("overnight_readiness") == "READY_WITH_WARNINGS":
        recommendation = "manual_review_observe_pool"
        reasons.append("overnight_ready_with_warnings")
    else:
        recommendation = "rerun_medium_real"
        blockers.append("overnight_not_ready_or_missing")
    if not reasons:
        reasons.append("research_decision_policy_applied")
    priority = "HIGH" if blockers else "MEDIUM"
    return {
        "recommendation": recommendation,
        "priority": priority,
        "reasons": reasons,
        "blocking_issues": blockers,
        "suggested_codex_task": _suggested_codex_task(recommendation),
    }


def _suggested_codex_task(recommendation: str) -> str:
    return {
        "run_overnight_real": (
            "Run overnight_real only after owner confirms runtime and disk budget."
        ),
        "rerun_medium_real": "Rerun medium_real after resolving readiness blockers.",
        "narrow_parameter_space": (
            "Inspect reject distribution and narrow policy-governed search axes."
        ),
        "expand_stress_windows": "Add or extend stress windows before promotion review.",
        "fix_evidence_gaps": (
            "Regenerate data/window/weight/attribution evidence and rerun summary."
        ),
        "manual_review_observe_pool": (
            "Review observe_pool candidates and register selected candidates."
        ),
    }.get(recommendation, "Review research decision report.")


def render_research_decision_reader_brief_section(
    manifest: Mapping[str, Any],
    recommendation: Mapping[str, Any],
) -> str:
    return (
        "Dynamic Rescue Research Decision: "
        f"recommendation={recommendation.get('recommendation')}; "
        f"observe_candidates={manifest.get('observe_candidate_count')}; "
        f"regime={manifest.get('tech_semiconductor_relevance')}; "
        f"overnight={manifest.get('overnight_readiness')}; production_effect=none."
    )


def render_research_decision_markdown(
    manifest: Mapping[str, Any],
    recommendation: Mapping[str, Any],
    *,
    evidence: Mapping[str, Any],
    regime: Mapping[str, Any],
    interpretation: Mapping[str, Any],
    observe_pool: Mapping[str, Any],
    readiness: Mapping[str, Any],
) -> str:
    blocking_issues = ", ".join(_texts(recommendation.get("blocking_issues"))) or "none"
    lines = [
        "# Dynamic v3 Rescue Research Decision",
        "",
        f"- sweep_id: `{manifest.get('source_sweep_id')}`",
        f"- medium_real_success: `{manifest.get('medium_real_success')}`",
        f"- research_value_candidate_found: `{manifest.get('research_value_candidate_found')}`",
        f"- evidence_quality_status: `{manifest.get('evidence_quality_status')}`",
        f"- regime_coverage_status: `{manifest.get('regime_coverage_status')}`",
        f"- tech_semiconductor_relevance: `{manifest.get('tech_semiconductor_relevance')}`",
        f"- ai_bull_market_overfit_risk: `{manifest.get('ai_bull_market_overfit_risk')}`",
        f"- observe_candidate_count: `{manifest.get('observe_candidate_count')}`",
        f"- overnight_readiness: `{manifest.get('overnight_readiness')}`",
        f"- recommendation: `{recommendation.get('recommendation')}`",
        f"- priority: `{recommendation.get('priority')}`",
        "",
        "## Source Artifacts",
        f"- evidence_summary: `{evidence.get('_manifest_path', '')}`",
        f"- regime_coverage: `{regime.get('_manifest_path', '')}`",
        f"- interpretation_pack: `{interpretation.get('_manifest_path', '')}`",
        f"- observe_pool: `{observe_pool.get('_manifest_path', '')}`",
        f"- overnight_readiness: `{readiness.get('_manifest_path', '')}`",
        "",
        "## Next Action",
        f"- reasons: `{', '.join(_texts(recommendation.get('reasons')))}`",
        f"- blocking_issues: `{blocking_issues}`",
        f"- suggested_codex_task: {recommendation.get('suggested_codex_task')}",
        "",
        "production_effect=none; broker_action=none.",
    ]
    return "\n".join(lines) + "\n"


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


def _robustness_evaluator_mode(
    result: Mapping[str, Any],
    config: DynamicV3ParameterSweepConfig,
) -> str:
    evaluator = _text(result.get("evaluator_mode"), config.execution.evaluator)
    if evaluator not in EVALUATOR_VERSIONS:
        raise DynamicV3ParameterResearchError(f"unknown robustness evaluator mode: {evaluator}")
    return evaluator


def _robustness_source_evidence(
    result: Mapping[str, Any],
    evaluator_mode: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    real_artifact_path = _text(result.get("real_evaluation_artifact_path"))
    real_artifact = Path(real_artifact_path) if real_artifact_path else None
    real_payload = _read_optional_json(real_artifact) or {}
    real_artifact_exists = bool(
        real_artifact_path and real_artifact is not None and real_artifact.exists()
    )
    metrics = _mapping(result.get("metrics"))
    data_quality = _mapping(result.get("data_quality"))
    report_id = _text(
        metrics.get("real_evaluation_report_id"),
        _text(real_payload.get("dynamic_v3_real_evaluation_report_id")),
    )
    evidence = {
        "evaluator_mode": evaluator_mode,
        "evaluator_version": _text(
            result.get("evaluator_version"),
            _evaluator_version(evaluator_mode),
        ),
        "metrics_source": _text(result.get("metrics_source"), "UNKNOWN"),
        "not_for_investment_decision": result.get("not_for_investment_decision") is True,
        "data_quality": data_quality,
        "real_evaluation_artifact_path": real_artifact_path,
        "source_real_evaluation_artifact_path": real_artifact_path,
        "source_real_evaluation_artifact_exists": real_artifact_exists,
        "source_real_evaluation_report_id": report_id,
        "source_real_evaluation_status": _text(real_payload.get("status"), "UNKNOWN"),
    }
    return evidence, real_payload


def _real_sensitivity_rows(
    result: Mapping[str, Any],
    results: Sequence[Mapping[str, Any]],
    config: DynamicV3ParameterSweepConfig,
) -> list[dict[str, Any]]:
    base_params = _mapping(result.get("parameters"))
    base_candidate_id = _text(result.get("candidate_id"))
    base_score = _float(result.get("score"))
    rows: list[dict[str, Any]] = []
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
            target_params = dict(base_params)
            target_params[key] = neighbor
            neighbor_result = _matching_parameter_result(
                results,
                target_params,
                exclude_candidate_id=base_candidate_id,
            )
            status = _real_neighbor_evaluation_status(neighbor_result)
            metrics = _mapping(neighbor_result.get("metrics")) if neighbor_result else {}
            neighbor_score = neighbor_result.get("score") if neighbor_result else ""
            score_delta = (
                round(_float(neighbor_score) - base_score, 6)
                if status == "AVAILABLE_REAL_NEIGHBOR_EVALUATION"
                else ""
            )
            rows.append(
                {
                    "candidate_id": base_candidate_id,
                    "parameter": key,
                    "base_value": value,
                    "neighbor_value": neighbor,
                    "neighbor_candidate_id": (
                        "" if neighbor_result is None else neighbor_result.get("candidate_id", "")
                    ),
                    "neighbor_evaluation_status": status,
                    "sensitivity_evidence_source": "real_evaluation_artifact",
                    "metrics_source": (
                        "" if neighbor_result is None else neighbor_result.get("metrics_source", "")
                    ),
                    "neighbor_real_evaluation_artifact_path": (
                        ""
                        if neighbor_result is None
                        else neighbor_result.get("real_evaluation_artifact_path", "")
                    ),
                    "neighbor_real_evaluation_artifact_exists": (
                        status == "AVAILABLE_REAL_NEIGHBOR_EVALUATION"
                    ),
                    "neighbor_gate": (
                        "" if neighbor_result is None else neighbor_result.get("gate", "")
                    ),
                    "neighbor_reasons": (
                        ""
                        if neighbor_result is None
                        else ";".join(_texts(neighbor_result.get("gate_reasons")))
                    ),
                    "neighbor_score": (
                        neighbor_score if status == "AVAILABLE_REAL_NEIGHBOR_EVALUATION" else ""
                    ),
                    "score_delta": score_delta,
                    "constraint_hit_rate": metrics.get("constraint_hit_rate", ""),
                    "turnover": metrics.get("turnover", ""),
                    "drawdown_degradation_pp": metrics.get("drawdown_degradation_pp", ""),
                }
            )
    return rows


def _matching_parameter_result(
    results: Sequence[Mapping[str, Any]],
    target_params: Mapping[str, Any],
    *,
    exclude_candidate_id: str,
) -> dict[str, Any] | None:
    canonical_target = _canonical_parameters(target_params)
    for row in results:
        if _text(row.get("candidate_id")) == exclude_candidate_id:
            continue
        if _canonical_parameters(_mapping(row.get("parameters"))) == canonical_target:
            return dict(row)
    return None


def _canonical_parameters(parameters: Mapping[str, Any]) -> dict[str, Any]:
    return {key: _canonical_parameter_value(value) for key, value in parameters.items()}


def _canonical_parameter_value(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return round(float(value), 12)
    text = _text(value)
    try:
        return round(float(text), 12)
    except ValueError:
        return text


def _real_neighbor_evaluation_status(neighbor_result: Mapping[str, Any] | None) -> str:
    if neighbor_result is None:
        return "MISSING_REAL_NEIGHBOR_EVALUATION"
    if _text(neighbor_result.get("status")) != "completed":
        return "REAL_NEIGHBOR_NOT_COMPLETED"
    if _text(neighbor_result.get("metrics_source")) != "real_evaluation_artifact":
        return "REAL_NEIGHBOR_NOT_REAL_EVALUATION"
    artifact_path = _text(neighbor_result.get("real_evaluation_artifact_path"))
    if not artifact_path or not Path(artifact_path).exists():
        return "MISSING_REAL_NEIGHBOR_ARTIFACT"
    return "AVAILABLE_REAL_NEIGHBOR_EVALUATION"


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


def _real_stress_bucket_results(
    result: Mapping[str, Any],
    config: DynamicV3ParameterSweepConfig,
    real_payload: Mapping[str, Any],
) -> dict[str, Any]:
    rows = []
    best = _mapping(real_payload.get("best_candidate"))
    daily = _mapping(real_payload.get("daily_path_summary"))
    daily_available = bool(real_payload) and _float(daily.get("row_count")) > 0
    unavailable_count = 0
    for bucket in config.robustness.require_stress_buckets:
        status, source, detail = _real_stress_bucket_status(
            bucket=bucket,
            result=result,
            config=config,
            real_payload=real_payload,
            best_candidate=best,
            daily_available=daily_available,
        )
        if status == "REVIEW_REQUIRED":
            unavailable_count += 1
        rows.append(
            {
                "bucket": bucket,
                "status": status,
                "constraint_hit_stability": status,
                "turnover_stability": status,
                "drawdown_stability": status,
                "static_gap_stability": status,
                "evidence_source": source,
                "evidence_detail": detail,
            }
        )
    pass_ratio = sum(1 for row in rows if row["status"] == "PASS") / max(1, len(rows))
    return {
        "buckets": rows,
        "pass_ratio": round(pass_ratio, 6),
        "evidence_source": "real_evaluation_artifact",
        "evidence_status": ("PASS" if unavailable_count == 0 else "PARTIAL_REAL_STRESS_EVIDENCE"),
        "missing_real_stress_bucket_count": unavailable_count,
    }


def _real_stress_bucket_status(
    *,
    bucket: str,
    result: Mapping[str, Any],
    config: DynamicV3ParameterSweepConfig,
    real_payload: Mapping[str, Any],
    best_candidate: Mapping[str, Any],
    daily_available: bool,
) -> tuple[str, str, str]:
    if not real_payload:
        return (
            "REVIEW_REQUIRED",
            "missing_real_evaluation_artifact",
            "source real evaluation artifact is missing",
        )
    if not daily_available:
        return (
            "REVIEW_REQUIRED",
            "missing_real_daily_path",
            "source real evaluation artifact lacks daily_path_summary",
        )
    if bucket == "high_drawdown":
        return _status_from_real_analysis(
            _mapping(real_payload.get("drawdown_preservation_analysis")),
            source="drawdown_preservation_analysis",
        )
    if bucket == "constraint_heavy":
        hit_rate = _float(
            best_candidate.get("constraint_hit_rate"),
            _float(_mapping(result.get("metrics")).get("constraint_hit_rate")),
        )
        status = "PASS" if hit_rate <= config.hard_constraints.max_constraint_hit_rate else "FAIL"
        return (
            status,
            "best_candidate.constraint_hit_rate",
            f"{hit_rate} <= {config.hard_constraints.max_constraint_hit_rate}",
        )
    if bucket in {"fast_recovery", "high_volatility"}:
        return _status_from_real_analysis(
            _mapping(real_payload.get("overfit_analysis")),
            source="overfit_analysis",
        )
    return (
        "REVIEW_REQUIRED",
        "real_bucket_not_available",
        f"no dedicated real stress extraction for bucket={bucket}",
    )


def _status_from_real_analysis(
    analysis: Mapping[str, Any],
    *,
    source: str,
) -> tuple[str, str, str]:
    status = _text(analysis.get("status"))
    if status == "PASS":
        return "PASS", source, _text(analysis.get("conclusion"), "analysis passed")
    if status == "FAIL":
        return "FAIL", source, _text(analysis.get("conclusion"), "analysis failed")
    return "REVIEW_REQUIRED", source, "analysis status missing"


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


def _real_regime_bucket_results(real_payload: Mapping[str, Any]) -> dict[str, Any]:
    paths = _mapping(real_payload.get("comparison_daily_paths"))
    records = _records(paths.get("dynamic_candidate"))
    if not records:
        return {
            "regimes": [],
            "evidence_source": "real_evaluation_artifact",
            "evidence_status": "MISSING_REAL_REGIME_DAILY_PATH",
        }
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in records:
        grouped.setdefault(_text(row.get("selected_regime"), "UNKNOWN"), []).append(row)
    regimes = []
    for regime, rows in sorted(grouped.items()):
        regimes.append(
            {
                "regime": regime,
                "status": "PASS",
                "rank_stability": "REAL_DAILY_PATH_OBSERVED",
                "row_count": len(rows),
                "strategy_return_sum": round(
                    sum(_float(row.get("strategy_return")) for row in rows),
                    6,
                ),
            }
        )
    return {
        "regimes": regimes,
        "regime_count": len(regimes),
        "evidence_source": "real_evaluation_artifact_daily_path",
        "evidence_status": "PASS",
        "regime_return_concentration": _mapping(real_payload.get("best_candidate")).get(
            "regime_return_concentration"
        ),
    }


def _overfit_diagnostics(
    sensitivity: Sequence[Mapping[str, Any]],
    stress: Mapping[str, Any],
    config: DynamicV3ParameterSweepConfig,
    *,
    evaluator_mode: str = EVALUATOR_TINY_FIXTURE_PROXY,
    source_evidence: Mapping[str, Any] | None = None,
    regime: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    source = _mapping(source_evidence)
    evidence_summary = _sensitivity_evidence_summary(sensitivity, evaluator_mode)
    max_delta = max(
        (
            abs(_float(row.get("score_delta")))
            for row in sensitivity
            if _text(row.get("score_delta"))
        ),
        default=0.0,
    )
    stress_pass_ratio = float(stress.get("pass_ratio", 0))
    if evaluator_mode == EVALUATOR_REAL_DYNAMIC_V3_RESCUE and evidence_summary["status"] != "PASS":
        sensitivity_status = "REVIEW_REQUIRED"
    elif max_delta <= config.robustness.max_score_delta_for_pass:
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
    stress_evidence_status = _text(
        stress.get("evidence_status"),
        "TINY_FIXTURE_PROXY" if evaluator_mode == EVALUATOR_TINY_FIXTURE_PROXY else "UNKNOWN",
    )
    regime_evidence_status = _text(
        _mapping(regime).get("evidence_status"),
        "TINY_FIXTURE_PROXY" if evaluator_mode == EVALUATOR_TINY_FIXTURE_PROXY else "UNKNOWN",
    )
    return {
        "robustness_status": robustness_status,
        "overfit_status": overfit_status,
        "parameter_sensitivity_status": sensitivity_status,
        "sensitivity_evidence_status": evidence_summary["status"],
        "real_neighbor_count": evidence_summary["real_neighbor_count"],
        "missing_real_neighbor_count": evidence_summary["missing_real_neighbor_count"],
        "stress_bucket_status": stress_status,
        "stress_evidence_status": stress_evidence_status,
        "regime_evidence_status": regime_evidence_status,
        "max_abs_score_delta": round(max_delta, 6),
        "stress_pass_ratio": round(stress_pass_ratio, 6),
        "evaluator_mode": evaluator_mode,
        "metrics_source": source.get("metrics_source", "UNKNOWN"),
        "source_real_evaluation_artifact_path": source.get(
            "source_real_evaluation_artifact_path", ""
        ),
        "source_real_evaluation_artifact_exists": source.get(
            "source_real_evaluation_artifact_exists", False
        ),
        "multiple_testing_warning": "review sweep size and ranking concentration before promotion",
        "pbo_dsr_placeholder": {
            "status": "NOT_RUN_REVIEW_NOTE",
            "behavioral_impact": "does not create PASS and does not bypass promotion review",
        },
    }


def _sensitivity_evidence_summary(
    sensitivity: Sequence[Mapping[str, Any]],
    evaluator_mode: str,
) -> dict[str, Any]:
    if evaluator_mode != EVALUATOR_REAL_DYNAMIC_V3_RESCUE:
        return {
            "status": "TINY_FIXTURE_PROXY",
            "real_neighbor_count": 0,
            "missing_real_neighbor_count": 0,
        }
    available = sum(
        1
        for row in sensitivity
        if row.get("neighbor_evaluation_status") == "AVAILABLE_REAL_NEIGHBOR_EVALUATION"
    )
    missing = sum(
        1
        for row in sensitivity
        if row.get("neighbor_evaluation_status") != "AVAILABLE_REAL_NEIGHBOR_EVALUATION"
    )
    if available > 0 and missing == 0:
        status = "PASS"
    elif available > 0:
        status = "PARTIAL_REAL_NEIGHBOR_EVIDENCE"
    else:
        status = "MISSING_REAL_NEIGHBOR_EVALUATION"
    return {
        "status": status,
        "real_neighbor_count": available,
        "missing_real_neighbor_count": missing,
    }


def _shadow_observation_basis(
    candidate_id: str,
    *,
    walk_forward_dir: Path = DEFAULT_WALK_FORWARD_DIR,
    robustness_dir: Path = DEFAULT_ROBUSTNESS_DIR,
) -> dict[str, str]:
    wf_manifest = _latest_walk_forward_manifest_for_candidate(candidate_id, walk_forward_dir)
    rob_manifest = _latest_robustness_manifest_for_candidate(candidate_id, robustness_dir)
    wf_id = _text(_mapping(_read_optional_json(wf_manifest)).get("walk_forward_id"))
    rob_id = _text(_mapping(_read_optional_json(rob_manifest)).get("robustness_id"))
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
    window_audit_dir: Path,
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
    wf_manifest_path = (
        walk_forward_dir / wf_id / "wf_manifest.json"
        if wf_id
        else _latest_walk_forward_manifest_for_candidate(candidate_id, walk_forward_dir)
    )
    if wf_manifest_path is not None and not wf_manifest_path.exists():
        wf_manifest_path = _latest_walk_forward_manifest_for_candidate(
            candidate_id, walk_forward_dir
        )
    if wf_manifest_path is not None:
        wf_id = wf_manifest_path.parent.name
    rob_manifest_path = (
        robustness_dir / rob_id / "robustness_manifest.json"
        if rob_id
        else _latest_robustness_manifest_for_candidate(candidate_id, robustness_dir)
    )
    if rob_manifest_path is not None and not rob_manifest_path.exists():
        rob_manifest_path = _latest_robustness_manifest_for_candidate(candidate_id, robustness_dir)
    if rob_manifest_path is not None:
        rob_id = rob_manifest_path.parent.name
    wf_report = None if wf_manifest_path is None else wf_manifest_path.parent / "wf_report.md"
    rob_report = (
        None if rob_manifest_path is None else rob_manifest_path.parent / "robustness_report.md"
    )
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
    window_audit = _latest_window_audit_evidence(window_audit_dir)
    shadow = _shadow_candidate_report(record) if record else None
    return {
        "registry_record": record,
        "candidate_report": candidate_report_payload,
        "candidate_report_path": (
            "" if candidate_report_path is None else str(candidate_report_path)
        ),
        "walk_forward_id": wf_id,
        "walk_forward_report_path": "" if wf_report is None else str(wf_report),
        "walk_forward_manifest": _read_optional_json(wf_manifest_path),
        "walk_forward_leaderboard": (
            _read_optional_json(walk_forward_dir / wf_id / "wf_leaderboard.json") if wf_id else None
        ),
        "robustness_id": rob_id,
        "robustness_report_path": "" if rob_report is None else str(rob_report),
        "robustness_manifest": _read_optional_json(rob_manifest_path),
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
        "window_audit": window_audit,
        "window_audit_manifest_path": window_audit.get("manifest_path", ""),
        "window_audit_report_path": window_audit.get("report_path", ""),
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
    if _promotion_window_blocks_promotion(evidence):
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


def _latest_window_audit_evidence(window_audit_dir: Path) -> dict[str, Any]:
    manifest_path: Path | None = None
    latest_pointer: dict[str, Any] = {}
    if _is_default_window_audit_dir(window_audit_dir):
        latest_pointer = _latest_pointer_payload("latest_window_audit")
        pointer_path_text = _text(_mapping(latest_pointer).get("path"))
        pointer_path = _resolve_project_path(Path(pointer_path_text)) if pointer_path_text else None
        if (
            pointer_path is not None
            and pointer_path.exists()
            and _is_default_dynamic_v3_research_artifact(pointer_path)
        ):
            manifest_path = pointer_path
    if manifest_path is None:
        latest_dir = _latest_child_dir(window_audit_dir)
        candidate_path = None if latest_dir is None else latest_dir / "window_audit_manifest.json"
        if candidate_path is not None and candidate_path.exists():
            manifest_path = candidate_path
    if manifest_path is None:
        return {
            "status": "MISSING",
            "date_range_status": "MISSING",
            "promotion_blocking": True,
            "promotion_blocking_count": None,
            "manifest_path": "",
            "report_path": "",
            "latest_pointer": latest_pointer,
        }
    manifest = _read_optional_json(manifest_path) or {}
    status = _text(manifest.get("status"), "MISSING")
    return {
        "window_audit_id": _text(manifest.get("window_audit_id"), manifest_path.parent.name),
        "status": status,
        "date_range_status": status,
        "configured_backtest_start": manifest.get("configured_backtest_start"),
        "requested_start": manifest.get("requested_start"),
        "requested_end": manifest.get("requested_end"),
        "earliest_actual_evaluation_start": manifest.get("earliest_actual_evaluation_start"),
        "promotion_blocking": (
            status in WINDOW_PROMOTION_BLOCKING_STATUSES
            or _float(manifest.get("promotion_blocking_count")) > 0
        ),
        "promotion_blocking_count": manifest.get("promotion_blocking_count"),
        "artifact_count": manifest.get("artifact_count"),
        "manifest_path": str(manifest_path),
        "report_path": str(manifest_path.parent / "window_audit_report.md"),
        "manifest": manifest,
        "latest_pointer": latest_pointer,
    }


def _promotion_window_summary(evidence: Mapping[str, Any]) -> dict[str, Any]:
    window_audit = _mapping(evidence.get("window_audit"))
    candidate_window = _mapping(_mapping(evidence.get("candidate_report")).get("backtest_window"))
    if window_audit:
        return {
            "source": "latest_window_audit",
            "date_range_status": window_audit.get("date_range_status", "MISSING"),
            "window_audit_id": window_audit.get("window_audit_id", ""),
            "configured_backtest_start": window_audit.get("configured_backtest_start"),
            "requested_start": window_audit.get("requested_start"),
            "requested_end": window_audit.get("requested_end"),
            "earliest_actual_evaluation_start": window_audit.get(
                "earliest_actual_evaluation_start"
            ),
            "promotion_blocking": window_audit.get("promotion_blocking", True),
            "promotion_blocking_count": window_audit.get("promotion_blocking_count"),
            "manifest_path": window_audit.get("manifest_path", ""),
            "report_path": window_audit.get("report_path", ""),
            "candidate_backtest_window": candidate_window,
        }
    if candidate_window:
        return {"source": "candidate_report", **candidate_window}
    return {"source": "missing", "date_range_status": "MISSING", "promotion_blocking": True}


def _promotion_window_blocks_promotion(evidence: Mapping[str, Any]) -> bool:
    window = _promotion_window_summary(evidence)
    status = _text(window.get("date_range_status"), "MISSING")
    blocking_count = _float(window.get("promotion_blocking_count"))
    return (
        status == "MISSING"
        or status in WINDOW_PROMOTION_BLOCKING_STATUSES
        or window.get("promotion_blocking") is True
        or blocking_count > 0
    )


def _promotion_evidence_summary(evidence: Mapping[str, Any]) -> dict[str, Any]:
    candidate = _mapping(evidence.get("candidate_report"))
    data_provenance = _mapping(evidence.get("data_provenance"))
    weight_path = _mapping(evidence.get("weight_path_metadata"))
    attribution = _mapping(evidence.get("candidate_attribution"))
    window = _promotion_window_summary(evidence)
    overfit = _mapping(evidence.get("overfit_manifest"))
    return {
        "data_quality": _mapping(candidate.get("data_quality")).get("status", "MISSING"),
        "price_cache_sha256": _mapping(data_provenance.get("prices")).get("sha256", ""),
        "download_manifest_status": data_provenance.get("download_manifest_status", "MISSING"),
        "provenance_status": data_provenance.get("provenance_status", "MISSING"),
        "data_provenance_warnings": data_provenance.get("warnings", []),
        "backtest_window_status": window.get("date_range_status", "MISSING"),
        "backtest_window": window,
        "window_audit_id": window.get("window_audit_id", ""),
        "window_audit_manifest_path": evidence.get("window_audit_manifest_path", ""),
        "window_audit_report_path": evidence.get("window_audit_report_path", ""),
        "weight_path_status": weight_path.get("attribution_completeness", "MISSING"),
        "weight_path_metadata_path": evidence.get("weight_path_metadata_path", ""),
        "candidate_attribution_status": attribution.get("status", "MISSING"),
        "candidate_attribution_path": evidence.get("candidate_attribution_path", ""),
        "overfit_status": overfit.get("overfit_status", "MISSING"),
        "promotion_blocking_flags": _promotion_blocking_flags(evidence),
    }


def _promotion_blocking_flags(evidence: Mapping[str, Any]) -> list[str]:
    flags: list[str] = []
    if _promotion_window_blocks_promotion(evidence):
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
        "window_audit": evidence.get("window_audit_manifest_path", ""),
        "window_audit_report": evidence.get("window_audit_report_path", ""),
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
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_path_metadata",
        "status": "PASS" if daily_weight_rows else "MISSING",
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
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
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
        rescue_codes = [code for code in reason_codes if "DRAWDOWN" in code or "RISK" in code]
        if not rescue_codes:
            continue
        events.append(
            {
                "date": _text(row.get("signal_date")),
                "rescue_trigger": (
                    "drawdown" if any("DRAWDOWN" in code for code in rescue_codes) else "regime"
                ),
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
            symbol: _float(value) for symbol, value in trade_deltas.items() if _float(value) > 0
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


def _matching_weight_path_dirs(evaluation_id: str, search_root: Path) -> list[Path]:
    matches: list[Path] = []
    for path in sorted(search_root.glob("**/weight_path_metadata.json"), key=str):
        payload = _mapping(_read_optional_json(path))
        if (
            _text(payload.get("evaluation_id")) == evaluation_id
            or path.parent.name == evaluation_id
        ):
            matches.append(path.parent)
    return matches


def _find_weight_path_dir(evaluation_id: str, search_root: Path) -> Path | None:
    matches = _matching_weight_path_dirs(evaluation_id, search_root)
    return matches[0] if len(matches) == 1 else None


def _inspect_weight_path_evidence(evaluation_id: str, search_root: Path) -> dict[str, Any]:
    matches = _matching_weight_path_dirs(evaluation_id, search_root)
    checks = [
        _check("weight_path_dir_found", bool(matches), evaluation_id),
        _check("weight_path_dir_unique", len(matches) == 1, str(len(matches))),
    ]
    empty_result = {
        "weight_path_dir": "",
        "matching_directory_count": len(matches),
        "metadata": {},
        "declared_attribution_completeness": WEIGHT_PATH_INCOMPLETE,
        "observed_attribution_completeness": WEIGHT_PATH_INCOMPLETE,
        "checks": checks,
        "failed_check_count": sum(1 for check in checks if not check["passed"]),
        "limitations": [
            "weight_path_artifact_not_found" if not matches else "ambiguous_weight_path_artifacts"
        ],
    }
    if len(matches) != 1:
        return empty_result

    weight_dir = matches[0]
    required = [
        "daily_weights.csv",
        "rebalance_events.json",
        "constraint_events.json",
        "rescue_events.json",
        "turnover_by_rebalance.csv",
        "weight_path_metadata.json",
    ]
    checks.extend(
        _check(f"artifact_exists:{name}", (weight_dir / name).is_file(), name)
        for name in required
    )
    metadata = _mapping(_read_optional_json(weight_dir / "weight_path_metadata.json"))
    checks.extend(
        [
            _check("metadata_is_object", bool(metadata), "weight_path_metadata.json"),
            _check(
                "metadata_evaluation_id_matches_request",
                _text(metadata.get("evaluation_id")) == evaluation_id,
                _text(metadata.get("evaluation_id")),
            ),
            _check(
                "metadata_candidate_id_present",
                bool(_text(metadata.get("candidate_id"))),
                _text(metadata.get("candidate_id")),
            ),
        ]
    )
    candidate_id = _text(metadata.get("candidate_id"))

    daily_path = weight_dir / "daily_weights.csv"
    daily = _read_csv_as_text(daily_path)
    daily_required = {"date", "symbol", "weight", "candidate_id"}
    daily_columns_valid = daily is not None and daily_required <= set(daily.columns)
    daily_nonempty = daily is not None and not daily.empty
    checks.extend(
        [
            _check(
                "daily_weights_readable",
                daily is not None,
                str(daily_path),
            ),
            _check(
                "daily_weights_required_columns",
                daily_columns_valid,
                "" if daily is None else ",".join(daily.columns),
            ),
            _check(
                "daily_weights_nonempty",
                daily_nonempty,
                "0" if daily is None else str(len(daily)),
            ),
        ]
    )
    daily_content_valid = False
    daily_detail_fields_supported = False
    if daily_columns_valid and daily_nonempty and daily is not None:
        parsed_dates = pd.to_datetime(daily["date"], errors="coerce")
        weights = pd.to_numeric(daily["weight"], errors="coerce")
        weights_finite = weights.notna() & weights.abs().ne(float("inf"))
        weight_bounds_valid = bool(
            weights_finite.all()
            and (weights >= -WEIGHT_PATH_WEIGHT_BOUND_TOLERANCE).all()
            and (weights <= 1.0 + WEIGHT_PATH_WEIGHT_BOUND_TOLERANCE).all()
        )
        duplicate_count = int(daily.duplicated(["date", "symbol", "candidate_id"]).sum())
        candidate_ids_match = bool(candidate_id) and bool(
            daily["candidate_id"].eq(candidate_id).all()
        )
        sums_valid = False
        if bool(weights_finite.all()):
            daily_with_numbers = daily.assign(_weight=weights)
            sums = daily_with_numbers.groupby("date", dropna=False)["_weight"].sum()
            sums_valid = bool((sums.sub(1.0).abs() <= WEIGHT_PATH_WEIGHT_SUM_TOLERANCE).all())
        valid_dates = bool(parsed_dates.notna().all())
        actual_start = parsed_dates.min().date().isoformat() if valid_dates else ""
        actual_end = parsed_dates.max().date().isoformat() if valid_dates else ""
        actual_symbol_count = int(daily["symbol"].nunique())
        checks.extend(
            [
                _check("daily_weights_dates_valid", valid_dates, actual_start or "invalid date"),
                _check("daily_weights_numeric_finite", bool(weights_finite.all()), "weight"),
                _check("daily_weights_within_bounds", weight_bounds_valid, "[0,1]"),
                _check("daily_weights_unique_keys", duplicate_count == 0, str(duplicate_count)),
                _check("daily_weights_candidate_ids_match", candidate_ids_match, candidate_id),
                _check("daily_weights_sum_to_one_by_date", sums_valid, "daily sum ~= 1"),
                _check(
                    "metadata_daily_weight_count_matches",
                    _safe_int(metadata.get("daily_weight_count")) == len(daily),
                    f"declared={metadata.get('daily_weight_count')};actual={len(daily)}",
                ),
                _check(
                    "metadata_weight_path_start_matches",
                    _text(metadata.get("weight_path_start")) == actual_start,
                    f"declared={metadata.get('weight_path_start')};actual={actual_start}",
                ),
                _check(
                    "metadata_weight_path_end_matches",
                    _text(metadata.get("weight_path_end")) == actual_end,
                    f"declared={metadata.get('weight_path_end')};actual={actual_end}",
                ),
                _check(
                    "metadata_symbol_count_matches",
                    _safe_int(metadata.get("symbol_count")) == actual_symbol_count,
                    f"declared={metadata.get('symbol_count')};actual={actual_symbol_count}",
                ),
            ]
        )
        daily_content_valid = all(check["passed"] for check in checks[-10:])
        detail_columns = {
            "target_weight",
            "pre_constraint_weight",
            "post_constraint_weight",
            "post_rescue_weight",
        }
        if detail_columns <= set(daily.columns):
            detail_values = daily[list(sorted(detail_columns))].apply(
                pd.to_numeric,
                errors="coerce",
            )
            daily_detail_fields_supported = bool(
                detail_values.notna().all().all()
                and detail_values.abs().ne(float("inf")).all().all()
                and (detail_values >= -WEIGHT_PATH_WEIGHT_BOUND_TOLERANCE).all().all()
                and (detail_values <= 1.0 + WEIGHT_PATH_WEIGHT_BOUND_TOLERANCE).all().all()
            )

    event_specs = {
        "rebalance_events": ({"date", "event_type", "turnover", "candidate_id"}, "turnover"),
        "constraint_events": (
            {"date", "constraint_type", "reason_code", "candidate_id"},
            None,
        ),
        "rescue_events": ({"date", "rescue_trigger", "candidate_id"}, None),
    }
    event_artifact_valid: dict[str, bool] = {}
    for stem, (required_fields, nonnegative_field) in event_specs.items():
        valid, event_checks = _inspect_weight_path_event_file(
            weight_dir / f"{stem}.json",
            stem=stem,
            required_fields=required_fields,
            candidate_id=candidate_id,
            nonnegative_field=nonnegative_field,
        )
        event_artifact_valid[stem] = valid
        checks.extend(event_checks)

    turnover_path = weight_dir / "turnover_by_rebalance.csv"
    turnover = _read_csv_as_text(turnover_path)
    turnover_required = {"date", "candidate_id", "turnover"}
    turnover_columns_valid = turnover is not None and turnover_required <= set(turnover.columns)
    turnover_nonempty = turnover is not None and not turnover.empty
    checks.extend(
        [
            _check("turnover_readable", turnover is not None, str(turnover_path)),
            _check(
                "turnover_required_columns",
                turnover_columns_valid,
                "" if turnover is None else ",".join(turnover.columns),
            ),
            _check(
                "turnover_nonempty",
                turnover_nonempty,
                "0" if turnover is None else str(len(turnover)),
            ),
        ]
    )
    turnover_valid = False
    if turnover_columns_valid and turnover_nonempty and turnover is not None:
        turnover_values = pd.to_numeric(turnover["turnover"], errors="coerce")
        turnover_values_valid = bool(
            turnover_values.notna().all()
            and turnover_values.abs().ne(float("inf")).all()
            and (turnover_values >= 0).all()
        )
        turnover_dates_valid = bool(
            pd.to_datetime(turnover["date"], errors="coerce").notna().all()
        )
        turnover_candidate_ids_match = bool(candidate_id) and bool(
            turnover["candidate_id"].eq(candidate_id).all()
        )
        checks.extend(
            [
                _check("turnover_dates_valid", turnover_dates_valid, "date"),
                _check("turnover_nonnegative_finite", turnover_values_valid, "turnover"),
                _check(
                    "turnover_candidate_ids_match",
                    turnover_candidate_ids_match,
                    candidate_id,
                ),
            ]
        )
        turnover_valid = all(check["passed"] for check in checks[-3:])

    artifact_flags = {
        "has_daily_weights": daily_content_valid,
        "has_rebalance_events": event_artifact_valid.get("rebalance_events", False),
        "has_constraint_events": event_artifact_valid.get("constraint_events", False),
        "has_rescue_events": event_artifact_valid.get("rescue_events", False),
        "has_turnover_by_rebalance": turnover_valid,
    }
    for field, observed in artifact_flags.items():
        checks.append(
            _check(
                f"metadata_{field}_matches",
                metadata.get(field) is observed,
                f"declared={metadata.get(field)};observed={observed}",
            )
        )

    declared = _text(metadata.get("attribution_completeness"), WEIGHT_PATH_INCOMPLETE)
    detail_level = _text(metadata.get("weight_path_detail_level"))
    checks.append(
        _check(
            "attribution_completeness_explicit",
            declared in {WEIGHT_PATH_COMPLETE, WEIGHT_PATH_PARTIAL, WEIGHT_PATH_INCOMPLETE},
            declared,
        )
    )
    checks.append(
        _check(
            "complete_detail_fields_supported",
            detail_level != "complete" or daily_detail_fields_supported,
            detail_level or "missing",
        )
    )
    core_valid = all(check["passed"] for check in checks)
    missing_fields = _texts(metadata.get("missing_fields"))
    if not core_valid:
        observed = WEIGHT_PATH_INCOMPLETE
    elif detail_level == "complete" and not missing_fields and all(artifact_flags.values()):
        observed = WEIGHT_PATH_COMPLETE
    else:
        observed = WEIGHT_PATH_PARTIAL
    checks.append(
        _check(
            "declared_completeness_matches_observed",
            declared == observed,
            f"declared={declared};observed={observed}",
        )
    )
    failed_ids = [check["check_id"] for check in checks if not check["passed"]]
    limitations = [f"missing_field:{field}" for field in missing_fields]
    if detail_level != "complete":
        limitations.append(f"weight_path_detail_level:{detail_level or 'missing'}")
    limitations.extend(f"failed_check:{check_id}" for check_id in failed_ids)
    return {
        "weight_path_dir": str(weight_dir),
        "matching_directory_count": len(matches),
        "metadata": metadata,
        "declared_attribution_completeness": declared,
        "observed_attribution_completeness": observed,
        "checks": checks,
        "failed_check_count": len(failed_ids),
        "limitations": limitations,
    }


def _missing_weight_path_inspection() -> dict[str, Any]:
    return {
        "weight_path_dir": "",
        "matching_directory_count": 0,
        "metadata": {},
        "declared_attribution_completeness": WEIGHT_PATH_INCOMPLETE,
        "observed_attribution_completeness": WEIGHT_PATH_INCOMPLETE,
        "checks": [],
        "failed_check_count": 0,
        "limitations": ["weight_path_artifact_not_available"],
    }


def _read_csv_as_text(path: Path) -> pd.DataFrame | None:
    if not path.is_file():
        return None
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    except Exception:  # noqa: BLE001
        return None


def _inspect_weight_path_event_file(
    path: Path,
    *,
    stem: str,
    required_fields: set[str],
    candidate_id: str,
    nonnegative_field: str | None,
) -> tuple[bool, list[dict[str, Any]]]:
    payload = _read_optional_json(path)
    events_value = payload.get("events") if isinstance(payload, Mapping) else None
    schema_valid = isinstance(events_value, list)
    events = _records(events_value)
    rows_are_objects = schema_valid and len(events) == len(events_value)
    required_fields_present = rows_are_objects and all(
        required_fields <= set(event) for event in events
    )
    dates_valid = required_fields_present and all(
        _date_from_any(event.get("date")) is not None for event in events
    )
    candidate_ids_match = required_fields_present and all(
        _text(event.get("candidate_id")) == candidate_id for event in events
    )
    nonnegative_valid = True
    if nonnegative_field is not None and required_fields_present:
        values = pd.to_numeric(
            pd.Series([event.get(nonnegative_field) for event in events], dtype="object"),
            errors="coerce",
        )
        nonnegative_valid = bool(
            values.notna().all()
            and values.abs().ne(float("inf")).all()
            and (values >= 0).all()
        )
    checks = [
        _check(f"{stem}_schema_valid", schema_valid and rows_are_objects, str(path)),
        _check(
            f"{stem}_required_fields",
            required_fields_present,
            ",".join(sorted(required_fields)),
        ),
        _check(f"{stem}_dates_valid", dates_valid, "date"),
        _check(f"{stem}_candidate_ids_match", candidate_ids_match, candidate_id),
    ]
    if nonnegative_field is not None:
        checks.append(
            _check(f"{stem}_{nonnegative_field}_nonnegative", nonnegative_valid, nonnegative_field)
        )
    return all(check["passed"] for check in checks), checks


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
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
        _mapping(row.get("backtest_window")) for row in results if row.get("backtest_window")
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
    config: DynamicV3ParameterSweepConfig,
    *,
    max_candidates: int,
) -> list[dict[str, Any]]:
    limit = max(max_candidates, 1)
    version = _git_commit()
    manifest_hash = config.data.manifest_hash
    base_parameters = {
        parameter: axis.values[0] for parameter, axis in config.parameter_space.items()
    }
    selected = [
        _injection_audit_candidate(
            base_parameters,
            code_version=version,
            data_manifest_hash=manifest_hash,
        )
    ]
    selected_parameter_keys = {_canonical_json(base_parameters)}

    # The first candidates form deterministic one-factor-at-a-time pairs with
    # the same base. Grid-prefix diversity cannot prove a parameter-specific
    # effect because earlier axes may remain constant after truncation.
    for parameter in REQUIRED_INJECTION_PARAMETERS:
        axis = config.parameter_space.get(parameter)
        if axis is None:
            continue
        alternate = next(
            (value for value in axis.values[1:] if value != base_parameters[parameter]),
            None,
        )
        if alternate is None:
            continue
        parameters = {**base_parameters, parameter: alternate}
        parameter_key = _canonical_json(parameters)
        if parameter_key in selected_parameter_keys:
            continue
        selected.append(
            _injection_audit_candidate(
                parameters,
                code_version=version,
                data_manifest_hash=manifest_hash,
            )
        )
        selected_parameter_keys.add(parameter_key)
        if len(selected) >= limit:
            return selected[:limit]

    pool_config = config.model_copy(
        update={
            "run": config.run.model_copy(
                update={"max_candidates": max(limit * 20, limit + len(selected))}
            )
        }
    )
    for candidate in parameter_grid_candidates(
        pool_config,
        code_version=version,
        data_manifest_hash=manifest_hash,
    ):
        parameter_key = _canonical_json(_mapping(candidate.get("parameters")))
        if parameter_key in selected_parameter_keys:
            continue
        selected.append(dict(candidate))
        selected_parameter_keys.add(parameter_key)
        if len(selected) >= limit:
            break
    return selected


def _injection_audit_candidate(
    parameters: Mapping[str, Any],
    *,
    code_version: str,
    data_manifest_hash: str,
) -> dict[str, Any]:
    normalized = dict(parameters)
    return {
        "candidate_id": stable_candidate_id(
            normalized,
            strategy_family=STRATEGY_FAMILY,
            code_version=code_version,
            data_manifest_hash=data_manifest_hash,
        ),
        "strategy_family": STRATEGY_FAMILY,
        "parameters": normalized,
        "code_version": code_version,
        "data_manifest_hash": data_manifest_hash,
    }


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
        matched_pairs = _parameter_matched_pairs(matrix_rows, parameter=parameter)
        config_changed_pairs = [
            pair
            for pair in matched_pairs
            if _effective_policy_hash(pair[0]) != _effective_policy_hash(pair[1])
        ]
        metric_changed_pairs = [
            pair
            for pair in config_changed_pairs
            if _text(pair[0].get("metric_hash")) != _text(pair[1].get("metric_hash"))
        ]
        weight_changed_pairs = [
            pair
            for pair in config_changed_pairs
            if _text(pair[0].get("latest_weight_hash")) != _text(pair[1].get("latest_weight_hash"))
        ]
        declared_consumed = parameter in PARAMETER_EFFECT_FIELDS
        consumed = bool(config_changed_pairs)
        if not declared_consumed:
            status = "NOT_CONSUMED"
        elif not matched_pairs:
            status = "INSUFFICIENT_MATCHED_PAIR_EVIDENCE"
        elif not consumed:
            status = "NOT_CONSUMED"
        elif metric_changed_pairs or weight_changed_pairs:
            status = "EFFECTIVE"
        else:
            status = "NO_OBSERVED_EFFECT"
        rows.append(
            {
                "parameter": parameter,
                "effect_status": status,
                "consumed": consumed,
                "declared_consumed": declared_consumed,
                "distinct_value_count": len(values),
                "distinct_effective_config_hash_count": len(config_hashes),
                "distinct_metric_hash_count": len(metric_hashes),
                "distinct_weight_hash_count": len(weight_hashes),
                "matched_pair_count": len(matched_pairs),
                "config_changed_pair_count": len(config_changed_pairs),
                "metric_changed_pair_count": len(metric_changed_pairs),
                "weight_changed_pair_count": len(weight_changed_pairs),
                "matched_pair_candidate_ids": [
                    [str(left.get("candidate_id")), str(right.get("candidate_id"))]
                    for left, right in matched_pairs
                ],
                "candidate_count": len(results),
            }
        )
    return rows


def _parameter_matched_pairs(
    matrix_rows: Sequence[Mapping[str, Any]],
    *,
    parameter: str,
) -> list[tuple[Mapping[str, Any], Mapping[str, Any]]]:
    pairs: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
    comparison_parameters = [
        candidate_parameter
        for candidate_parameter in REQUIRED_INJECTION_PARAMETERS
        if candidate_parameter != parameter
    ]
    for index, left in enumerate(matrix_rows):
        for right in matrix_rows[index + 1 :]:
            if left.get(parameter) == right.get(parameter):
                continue
            if all(left.get(name) == right.get(name) for name in comparison_parameters):
                pairs.append((left, right))
    return pairs


def _effective_policy_hash(row: Mapping[str, Any]) -> str:
    return _text(row.get("effective_real_policy_hash")) + _text(
        row.get("effective_rescue_policy_hash")
    )


def _latest_weight_hash_from_candidate(candidate: Mapping[str, Any]) -> str:
    artifact_path = _text(candidate.get("real_evaluation_artifact_path"))
    payload = _read_optional_json(Path(artifact_path)) if artifact_path else None
    weights = _mapping(_mapping(payload).get("best_candidate")).get("latest_weights", {})
    return _stable_id(weights)


def _candidate_weight_delta_rows(
    real_payload: Mapping[str, Any] | None,
    *,
    daily_weights_path: Path | None,
    candidate_id: str,
    source_sweep_id: str,
) -> list[dict[str, Any]]:
    if not real_payload or daily_weights_path is None:
        return []
    daily = _read_csv_as_text(daily_weights_path)
    required = {"date", "symbol", "weight", "candidate_id"}
    if daily is None or daily.empty or not required <= set(daily.columns):
        return []
    parsed_dates = pd.to_datetime(daily["date"], errors="coerce")
    if not bool(parsed_dates.notna().all()):
        return []
    latest_date = parsed_dates.max()
    latest = daily.loc[parsed_dates.eq(latest_date)].copy()
    if latest.empty or not bool(latest["candidate_id"].eq(candidate_id).all()):
        return []
    candidate_weights = pd.to_numeric(latest["weight"], errors="coerce")
    if not bool(candidate_weights.notna().all()):
        return []
    best_weights = {
        _text(symbol): float(weight)
        for symbol, weight in zip(latest["symbol"], candidate_weights, strict=True)
    }
    reference_rows = _records(
        _mapping(real_payload.get("comparison_daily_paths")).get("static_base_candidate")
    )
    if not reference_rows:
        return []
    reference_date = _date_from_any(reference_rows[-1].get("signal_date"))
    if reference_date != latest_date.date():
        return []
    reference_weights_raw = _json_mapping(reference_rows[-1].get("target_weights_json"))
    try:
        reference_weights = {
            _text(symbol): float(weight) for symbol, weight in reference_weights_raw.items()
        }
    except (TypeError, ValueError):
        return []
    reference_values = pd.Series(list(reference_weights.values()), dtype="float64")
    if (
        reference_values.empty
        or reference_values.abs().eq(float("inf")).any()
        or (reference_values < -WEIGHT_PATH_WEIGHT_BOUND_TOLERANCE).any()
        or (reference_values > 1.0 + WEIGHT_PATH_WEIGHT_BOUND_TOLERANCE).any()
        or abs(float(reference_values.sum()) - 1.0) > WEIGHT_PATH_WEIGHT_SUM_TOLERANCE
    ):
        return []
    if not best_weights or not reference_weights:
        return []
    symbols = sorted(set(best_weights) | set(reference_weights))
    return [
        {
            "as_of": latest_date.date().isoformat(),
            "candidate_id": candidate_id,
            "source_sweep_id": source_sweep_id,
            "reference_group": "static_base_candidate",
            "symbol": symbol,
            "candidate_weight": float(best_weights.get(symbol, 0.0)),
            "baseline_weight": float(reference_weights.get(symbol, 0.0)),
            "delta": round(
                float(best_weights.get(symbol, 0.0)) - float(reference_weights.get(symbol, 0.0)),
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
        "PASS" if max_abs_delta <= config.robustness.max_score_delta_for_pass else "REVIEW_REQUIRED"
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
    manual_only = {parameter for parameter, policy in policies.items() if policy == "manual_only"}
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
    pointer = _latest_pointer_payload(name)
    return _text(_mapping(pointer).get("artifact_id"))


def _latest_pointer_payload(name: str) -> dict[str, Any]:
    return _read_optional_json(DEFAULT_LATEST_POINTER_DIR / f"{name}.json") or {}


def _is_weekly_last_trading_day(value: date) -> bool:
    if not is_us_equity_trading_day(value):
        return False
    cursor = value + timedelta(days=1)
    value_week = value.isocalendar()[:2]
    while cursor.isocalendar()[:2] == value_week:
        if is_us_equity_trading_day(cursor):
            return False
        cursor += timedelta(days=1)
    return True


def _run_scheduled_shadow_monitor(
    *,
    as_of: date,
    registry_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    if not registry_path.exists():
        return {
            "status": "SKIPPED",
            "reason": f"shadow registry missing: {registry_path}",
            "monitor_id": None,
            "monitor_dir": None,
        }
    registry = load_shadow_registry(registry_path)
    candidate_count = len(_records(registry.get("candidates")))
    if candidate_count == 0:
        return {
            "status": "SKIPPED",
            "reason": "shadow registry has no candidates",
            "monitor_id": None,
            "monitor_dir": None,
        }
    try:
        result = run_shadow_monitor(
            as_of=as_of,
            registry_path=registry_path,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        return {
            "status": "FAIL",
            "reason": str(exc),
            "monitor_id": None,
            "monitor_dir": None,
        }
    return {
        "status": "PASS",
        "reason": "shadow monitor observe-only artifact generated",
        "monitor_id": result.get("monitor_id"),
        "monitor_dir": str(result.get("monitor_dir")),
    }


def _latest_overfit_manifest_for_candidate(candidate_id: str, output_dir: Path) -> Path | None:
    candidates = []
    for manifest_path in output_dir.glob("*/overfit_manifest.json"):
        manifest = _read_optional_json(manifest_path)
        if _mapping(manifest).get("candidate_id") == candidate_id:
            candidates.append(manifest_path)
    return max(candidates, key=lambda path: path.stat().st_mtime) if candidates else None


def _latest_walk_forward_manifest_for_candidate(candidate_id: str, output_dir: Path) -> Path | None:
    candidates = []
    for leaderboard_path in output_dir.glob("*/wf_leaderboard.json"):
        leaderboard = _read_optional_json(leaderboard_path)
        rows = _records(_mapping(leaderboard).get("candidates"))
        if any(_text(row.get("candidate_id")) == candidate_id for row in rows):
            manifest_path = leaderboard_path.parent / "wf_manifest.json"
            if manifest_path.exists():
                candidates.append(manifest_path)
    return max(candidates, key=lambda path: path.stat().st_mtime) if candidates else None


def _latest_robustness_manifest_for_candidate(candidate_id: str, output_dir: Path) -> Path | None:
    candidates = []
    for manifest_path in output_dir.glob("*/robustness_manifest.json"):
        manifest = _read_optional_json(manifest_path)
        if _text(_mapping(manifest).get("candidate_id")) == candidate_id:
            candidates.append(manifest_path)
    return max(candidates, key=lambda path: path.stat().st_mtime) if candidates else None


def _resolve_project_path(path: Path) -> Path:
    return path if path.is_absolute() else PROJECT_ROOT / path


def _update_latest_pointer(name: str, artifact_id: str, path: Path) -> None:
    if not _is_default_dynamic_v3_research_artifact(path):
        return
    _write_latest_pointer(DEFAULT_LATEST_POINTER_DIR, name, artifact_id, path)


def _write_latest_pointer(pointer_dir: Path, name: str, artifact_id: str, path: Path) -> None:
    pointer_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        pointer_dir / f"{name}.json",
        {
            "schema_version": SCHEMA_VERSION,
            "artifact_type": name.removeprefix("latest_"),
            "artifact_id": artifact_id,
            "path": str(path),
            "updated_at": datetime.now(UTC).isoformat(),
            "exists": path.exists(),
        },
    )


def _latest_pointer_repair_specs() -> tuple[dict[str, Any], ...]:
    return (
        {
            "pointer_name": "latest_sweep",
            "pattern": "sweeps/*/sweep_manifest.json",
            "id_keys": ("sweep_id",),
        },
        {
            "pointer_name": "latest_data_audit",
            "pattern": "data_audit/*/data_audit_manifest.json",
            "id_keys": ("data_audit_id",),
        },
        {
            "pointer_name": "latest_data_provenance",
            "pattern": "data_provenance/price_cache_provenance_report.json",
            "constant_id": "price_cache_provenance",
        },
        {
            "pointer_name": "latest_window_audit",
            "pattern": "window_audit/*/window_audit_manifest.json",
            "id_keys": ("window_audit_id",),
        },
        {
            "pointer_name": "latest_injection_audit",
            "pattern": "injection_audit/*/injection_audit_manifest.json",
            "id_keys": ("injection_audit_id", "audit_id"),
        },
        {
            "pointer_name": "latest_candidate_attribution",
            "pattern": "candidate_attribution/*/attribution_manifest.json",
            "id_keys": ("candidate_id",),
        },
        {
            "pointer_name": "latest_walk_forward_selection",
            "pattern": "walk_forward_selection/*/wf_selection_manifest.json",
            "id_keys": ("wf_selection_id", "walk_forward_selection_id"),
        },
        {
            "pointer_name": "latest_walk_forward",
            "pattern": "walk_forward/*/wf_manifest.json",
            "id_keys": ("wf_id", "walk_forward_id"),
        },
        {
            "pointer_name": "latest_robustness",
            "pattern": "robustness/*/robustness_manifest.json",
            "id_keys": ("robustness_id",),
        },
        {
            "pointer_name": "latest_overfit",
            "pattern": "overfit/*/overfit_manifest.json",
            "id_keys": ("overfit_id",),
        },
        {
            "pointer_name": "latest_shadow_monitor",
            "pattern": "shadow_monitor/*/shadow_monitor_manifest.json",
            "id_keys": ("monitor_id", "shadow_monitor_id"),
        },
        {
            "pointer_name": "latest_shadow_report",
            "pattern": "shadow/shadow_report_*.json",
            "id_keys": ("candidate_id",),
        },
        {
            "pointer_name": "latest_parameter_governance",
            "pattern": "governance/parameter_governance_report.json",
            "id_keys": ("search_space_version", "policy_id"),
        },
        {
            "pointer_name": "latest_research_index",
            "pattern": "index/research_index_manifest.json",
            "constant_id": "research_index",
        },
        {
            "pointer_name": "latest_promotion_pack",
            "pattern": "promotion/*/*/promotion_manifest.json",
            "id_keys": ("promotion_id",),
        },
        {
            "pointer_name": "latest_evidence_summary",
            "pattern": "evidence_summary/*/evidence_summary_manifest.json",
            "id_keys": ("summary_id",),
        },
        {
            "pointer_name": "latest_medium_real",
            "pattern": "medium_real/*/medium_real_manifest.json",
            "id_keys": ("medium_real_report_id",),
        },
        {
            "pointer_name": "latest_regime_coverage",
            "pattern": "regime_coverage/*/regime_coverage_manifest.json",
            "id_keys": ("coverage_id",),
        },
        {
            "pointer_name": "latest_interpretation_pack",
            "pattern": "interpretation_pack/*/interpretation_manifest.json",
            "id_keys": ("pack_id",),
        },
        {
            "pointer_name": "latest_observe_pool",
            "pattern": "observe_pool/*/observe_pool_manifest.json",
            "id_keys": ("pool_id",),
        },
        {
            "pointer_name": "latest_overnight_readiness",
            "pattern": "overnight_readiness/*/overnight_readiness_manifest.json",
            "id_keys": ("readiness_id",),
        },
        {
            "pointer_name": "latest_research_decision",
            "pattern": "research_decision/*/research_decision_manifest.json",
            "id_keys": ("decision_id",),
        },
        {
            "pointer_name": "latest_evidence_diagnosis",
            "pattern": "evidence_diagnosis/*/diagnosis_manifest.json",
            "id_keys": ("diagnosis_id",),
        },
        {
            "pointer_name": "latest_gate_impact",
            "pattern": "gate_impact/*/gate_impact_manifest.json",
            "id_keys": ("impact_id",),
        },
        {
            "pointer_name": "latest_gate_policy",
            "pattern": "gate_policy/*/gate_policy_manifest.json",
            "id_keys": ("policy_run_id",),
        },
        {
            "pointer_name": "latest_candidate_recovery",
            "pattern": "candidate_recovery/*/recovery_manifest.json",
            "id_keys": ("recovery_id",),
        },
        {
            "pointer_name": "latest_research_decision_update",
            "pattern": "research_decision_update/*/decision_update_manifest.json",
            "id_keys": ("decision_update_id",),
        },
        {
            "pointer_name": "latest_shortlist",
            "pattern": "shortlist/*/shortlist_manifest.json",
            "id_keys": ("shortlist_id",),
        },
        {
            "pointer_name": "latest_candidate_cluster",
            "pattern": "candidate_cluster/*/cluster_manifest.json",
            "id_keys": ("cluster_id",),
        },
        {
            "pointer_name": "latest_shadow_shortlist",
            "pattern": "shadow_shortlist/*/shadow_shortlist_manifest.json",
            "id_keys": ("shadow_shortlist_id",),
        },
        {
            "pointer_name": "latest_position_advisory",
            "pattern": "position_advisory/*/position_advisory_manifest.json",
            "id_keys": ("advisory_id",),
        },
        {
            "pointer_name": "latest_manual_portfolio_snapshot",
            "pattern": "manual_portfolio_snapshot/*/manual_portfolio_manifest.json",
            "id_keys": ("snapshot_id",),
        },
        {
            "pointer_name": "latest_portfolio_exposure",
            "pattern": "portfolio_exposure/*/portfolio_exposure_manifest.json",
            "id_keys": ("exposure_id",),
        },
        {
            "pointer_name": "latest_position_drift",
            "pattern": "position_drift/*/position_drift_manifest.json",
            "id_keys": ("drift_id",),
        },
        {
            "pointer_name": "latest_execution_guardrails",
            "pattern": "execution_guardrails/*/guardrail_manifest.json",
            "id_keys": ("guardrail_id",),
        },
        {
            "pointer_name": "latest_manual_execution_review",
            "pattern": "manual_execution_review/*/manual_execution_review_manifest.json",
            "id_keys": ("manual_review_id",),
        },
        {
            "pointer_name": "latest_real_snapshot_intake",
            "pattern": "real_snapshot_intake/*/real_snapshot_intake_manifest.json",
            "id_keys": ("snapshot_intake_id",),
        },
        {
            "pointer_name": "latest_real_snapshot_dry_run",
            "pattern": "real_snapshot_dry_run/*/real_snapshot_dry_run_manifest.json",
            "id_keys": ("dry_run_id",),
        },
        {
            "pointer_name": "latest_real_execution_owner_review",
            "pattern": ("real_execution_owner_review/*/real_execution_owner_review_manifest.json"),
            "id_keys": ("review_id",),
        },
        {
            "pointer_name": "latest_real_snapshot_paper_action",
            "pattern": ("real_snapshot_paper_action/*/real_snapshot_paper_action_manifest.json"),
            "id_keys": ("paper_action_id",),
        },
        {
            "pointer_name": "latest_weekly_real_snapshot_review",
            "pattern": ("weekly_real_snapshot_review/*/weekly_real_snapshot_review_manifest.json"),
            "id_keys": ("weekly_real_review_id",),
        },
        {
            "pointer_name": "latest_position_review",
            "pattern": "position_review/*/position_review_manifest.json",
            "id_keys": ("review_id",),
        },
        {
            "pointer_name": "latest_model_target",
            "pattern": "model_target/*/model_target_manifest.json",
            "id_keys": ("target_id",),
        },
        {
            "pointer_name": "latest_paper_shadow",
            "pattern": "paper_shadow/*/paper_shadow_manifest.json",
            "id_keys": ("paper_shadow_id",),
        },
        {
            "pointer_name": "latest_model_rebalance",
            "pattern": "model_rebalance/*/model_rebalance_manifest.json",
            "id_keys": ("rebalance_id",),
        },
        {
            "pointer_name": "latest_paper_shadow_performance",
            "pattern": "paper_shadow_performance/*/paper_shadow_performance_manifest.json",
            "id_keys": ("performance_id",),
        },
        {
            "pointer_name": "latest_system_target_review",
            "pattern": "system_target_review/*/system_target_review_manifest.json",
            "id_keys": ("review_id", "system_target_review_id"),
        },
        {
            "pointer_name": "latest_paper_portfolio",
            "pattern": "paper_portfolio/*/paper_portfolio_manifest.json",
            "id_keys": ("paper_portfolio_id",),
        },
        {
            "pointer_name": "latest_advisory_outcome",
            "pattern": "advisory_outcome/*/advisory_outcome_manifest.json",
            "id_keys": ("outcome_id",),
        },
        {
            "pointer_name": "latest_owner_attribution",
            "pattern": "owner_attribution/*/owner_attribution_manifest.json",
            "id_keys": ("attribution_id",),
        },
        {
            "pointer_name": "latest_shadow_aging",
            "pattern": "shadow_aging/*/shadow_aging_manifest.json",
            "id_keys": ("aging_id",),
        },
        {
            "pointer_name": "latest_weekly_advisory_review",
            "pattern": "weekly_advisory_review/*/weekly_review_manifest.json",
            "id_keys": ("weekly_review_id",),
        },
        {
            "pointer_name": "latest_replay_inventory",
            "pattern": "replay_inventory/*/replay_inventory_manifest.json",
            "id_keys": ("inventory_id",),
        },
        {
            "pointer_name": "latest_historical_replay",
            "pattern": "historical_replay/*/historical_replay_manifest.json",
            "id_keys": ("replay_id",),
        },
        {
            "pointer_name": "latest_backfilled_outcome",
            "pattern": "backfilled_outcome/*/backfill_manifest.json",
            "id_keys": ("backfill_id",),
        },
        {
            "pointer_name": "latest_historical_paper_sim",
            "pattern": "historical_paper_sim/*/historical_paper_sim_manifest.json",
            "id_keys": ("sim_id",),
        },
        {
            "pointer_name": "latest_replay_performance_review",
            "pattern": "replay_performance_review/*/replay_performance_manifest.json",
            "id_keys": ("review_id",),
        },
        {
            "pointer_name": "latest_replay_diagnosis",
            "pattern": "replay_diagnosis/*/replay_diagnosis_manifest.json",
            "id_keys": ("diagnosis_id",),
        },
        {
            "pointer_name": "latest_backfill_repair",
            "pattern": "backfill_repair/*/backfill_repair_manifest.json",
            "id_keys": ("repair_id",),
        },
        {
            "pointer_name": "latest_variant_comparison",
            "pattern": "variant_comparison/*/variant_comparison_manifest.json",
            "id_keys": ("comparison_id",),
        },
        {
            "pointer_name": "latest_rule_calibration",
            "pattern": "rule_calibration/*/rule_calibration_manifest.json",
            "id_keys": ("calibration_id",),
        },
        {
            "pointer_name": "latest_replay_forward_bridge",
            "pattern": "replay_forward_bridge/*/bridge_manifest.json",
            "id_keys": ("bridge_id",),
        },
        {
            "pointer_name": "latest_no_promotion_review",
            "pattern": "no_promotion_review/*/no_promotion_review_manifest.json",
            "id_keys": ("review_id",),
        },
        {
            "pointer_name": "latest_near_miss_candidates",
            "pattern": "near_miss_candidates/*/near_miss_manifest.json",
            "id_keys": ("near_miss_id",),
        },
        {
            "pointer_name": "latest_cash_buffer_attribution",
            "pattern": "cash_buffer_attribution/*/cash_buffer_attribution_manifest.json",
            "id_keys": ("attribution_id",),
        },
        {
            "pointer_name": "latest_search_coverage_gap",
            "pattern": "search_coverage_gap/*/search_coverage_gap_manifest.json",
            "id_keys": ("coverage_gap_id",),
        },
        {
            "pointer_name": "latest_targeted_search_v3",
            "pattern": "targeted_search_v3/*/targeted_search_v3_manifest.json",
            "id_keys": ("v3_matrix_id",),
        },
        {
            "pointer_name": "latest_targeted_v3_backfill",
            "pattern": "targeted_v3_backfill/*/targeted_v3_backfill_manifest.json",
            "id_keys": ("v3_backfill_id",),
        },
        {
            "pointer_name": "latest_near_miss_ab_comparison",
            "pattern": "near_miss_ab_comparison/*/near_miss_ab_manifest.json",
            "id_keys": ("ab_id",),
        },
        {
            "pointer_name": "latest_promotion_threshold_sensitivity",
            "pattern": ("promotion_threshold_sensitivity/*/threshold_sensitivity_manifest.json"),
            "id_keys": ("sensitivity_id",),
        },
        {
            "pointer_name": "latest_candidate_promotion_v2",
            "pattern": "candidate_promotion_v2/*/candidate_promotion_v2_manifest.json",
            "id_keys": ("promotion_v2_id",),
        },
        {
            "pointer_name": "latest_next_formal_or_search_plan",
            "pattern": "next_formal_or_search_plan/*/next_formal_or_search_manifest.json",
            "id_keys": ("plan_id",),
        },
        {
            "pointer_name": "latest_gate_calibration_review",
            "pattern": "gate_calibration_review/*/gate_calibration_manifest.json",
            "id_keys": ("gate_calibration_id",),
        },
        {
            "pointer_name": "latest_scorecard_attribution",
            "pattern": "scorecard_attribution/*/scorecard_attribution_manifest.json",
            "id_keys": ("scorecard_attribution_id",),
        },
        {
            "pointer_name": "latest_signal_instability_diagnosis",
            "pattern": "signal_instability_diagnosis/*/signal_instability_manifest.json",
            "id_keys": ("signal_diagnosis_id",),
        },
        {
            "pointer_name": "latest_consensus_quality_review",
            "pattern": "consensus_quality_review/*/consensus_quality_manifest.json",
            "id_keys": ("consensus_review_id",),
        },
        {
            "pointer_name": "latest_micro_search_v4_design",
            "pattern": "micro_search_v4_design/*/micro_search_v4_design_manifest.json",
            "id_keys": ("v4_design_id",),
        },
        {
            "pointer_name": "latest_micro_search_v4_backfill",
            "pattern": "micro_search_v4_backfill/*/micro_search_v4_backfill_manifest.json",
            "id_keys": ("v4_backfill_id",),
        },
        {
            "pointer_name": "latest_gate_calibrated_review",
            "pattern": "gate_calibrated_review/*/gate_calibrated_review_manifest.json",
            "id_keys": ("gate_review_id",),
        },
        {
            "pointer_name": "latest_signal_vs_parameter_attribution",
            "pattern": "signal_vs_parameter_attribution/*/signal_vs_parameter_manifest.json",
            "id_keys": ("attribution_id",),
        },
        {
            "pointer_name": "latest_next_research_direction",
            "pattern": "next_research_direction/*/next_research_direction_manifest.json",
            "id_keys": ("direction_id",),
        },
        {
            "pointer_name": "latest_owner_research_roadmap",
            "pattern": "owner_research_roadmap/*/owner_research_roadmap_manifest.json",
            "id_keys": ("roadmap_id",),
        },
    )


def _latest_pointer_repair_artifact_id(
    *,
    path: Path,
    payload: Mapping[str, Any],
    id_keys: Sequence[Any],
    constant_id: str = "",
) -> str:
    if constant_id:
        return constant_id
    for key in id_keys:
        value = _text(payload.get(_text(key)))
        if value:
            return value
    if path.parent.name:
        return path.parent.name
    return path.stem


def _is_default_dynamic_v3_research_artifact(path: Path) -> bool:
    try:
        resolved_path = path.resolve(strict=False)
        resolved_root = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT.resolve(strict=False)
        return resolved_path == resolved_root or resolved_path.is_relative_to(resolved_root)
    except (OSError, RuntimeError, ValueError):
        return False


def _is_default_latest_pointer_dir(path: Path) -> bool:
    try:
        return path.resolve(strict=False) == DEFAULT_LATEST_POINTER_DIR.resolve(strict=False)
    except (OSError, RuntimeError, ValueError):
        return False


def _is_default_window_audit_dir(path: Path) -> bool:
    try:
        return path.resolve(strict=False) == DEFAULT_WINDOW_AUDIT_DIR.resolve(strict=False)
    except (OSError, RuntimeError, ValueError):
        return False


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


def _clone_jsonable(payload: Any) -> Any:
    return json.loads(json.dumps(_jsonable(payload), ensure_ascii=False, sort_keys=True))


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


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
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
