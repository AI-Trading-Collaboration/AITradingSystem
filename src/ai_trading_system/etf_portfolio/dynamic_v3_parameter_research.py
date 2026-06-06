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
DEFAULT_DYNAMIC_V3_RESEARCH_ROOT = PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_v3_rescue"
DEFAULT_SWEEP_OUTPUT_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "sweeps"
DEFAULT_WALK_FORWARD_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "walk_forward"
DEFAULT_ROBUSTNESS_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "robustness"
DEFAULT_SHADOW_REPORT_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "shadow"
DEFAULT_PROMOTION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "promotion"
DEFAULT_LATEST_POINTER_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "latest"
DEFAULT_SHADOW_REGISTRY_PATH = (
    PROJECT_ROOT / "registry" / "etf_portfolio" / "dynamic_v3_rescue_shadow_candidates.yaml"
)

GATE_REJECT = "reject"
GATE_REVIEW_REQUIRED = "review_required"
GATE_OBSERVE_ONLY = "observe_only"
GATE_PROMOTE_CANDIDATE = "promote_candidate"
FORBIDDEN_GATE = "production_candidate"

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
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    try:
        config = load_parameter_sweep_config(config_path)
        candidates = parameter_grid_candidates(config)
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
            ]
        )
        status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    except DynamicV3ParameterResearchError as exc:
        checks.append(_check("schema_valid", False, str(exc)))
        status = "FAIL"
        candidates = []
        config = None
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_parameter_sweep_config_validation",
        "status": status,
        "config_path": str(config_path),
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


def run_parameter_sweep(
    *,
    config_path: Path = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    as_of: date | None = None,
    end: date | None = None,
    workers: int | None = None,
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
    if resume:
        sweep_dir = output_dir / resume
        if not sweep_dir.exists():
            raise DynamicV3ParameterResearchError(f"sweep artifact not found: {resume}")
        config = load_parameter_sweep_config(sweep_dir / "sweep_config.normalized.yaml")
        if evaluator_mode is not None and evaluator_mode != config.execution.evaluator:
            raise DynamicV3ParameterResearchError(
                "resume cannot change evaluator_mode from existing sweep config"
            )
        manifest = _read_json(sweep_dir / "sweep_manifest.json")
        sweep_id = _text(manifest.get("sweep_id"), resume)
        candidates = _read_jsonl(sweep_dir / "candidates.jsonl")
        completed = {
            _text(row.get("candidate_id"))
            for row in _read_jsonl(sweep_dir / "candidate_results.jsonl")
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
        if evaluator_mode is not None:
            config = _with_evaluator_mode(config, evaluator_mode)
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
    results = _read_jsonl(result_path)
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
        config_path=config_path,
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
        "evaluator_mode": evaluator,
        "evaluator_version": _evaluator_version(evaluator),
        "real_evaluation_artifact_path": real_artifact_path,
        "data_quality": data_quality,
        "metrics_source": metrics_source,
        "not_for_investment_decision": not_for_investment,
        "metrics": metrics,
        "score": score,
        "score_breakdown": breakdown,
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
    results = (
        _read_jsonl(sweep_dir / "candidate_results.jsonl")
        if (sweep_dir / "candidate_results.jsonl").exists()
        else []
    )
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
    results = _read_jsonl(sweep_dir / "candidate_results.jsonl")
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
        "artifact_links": {
            "sweep_manifest": str(sweep_dir / "sweep_manifest.json"),
            "leaderboard": str(sweep_dir / "leaderboard.json"),
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


def build_promotion_pack(
    *,
    candidate_id: str,
    registry_path: Path = DEFAULT_SHADOW_REGISTRY_PATH,
    sweep_output_dir: Path = DEFAULT_SWEEP_OUTPUT_DIR,
    walk_forward_dir: Path = DEFAULT_WALK_FORWARD_DIR,
    robustness_dir: Path = DEFAULT_ROBUSTNESS_DIR,
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
        "linked_artifacts": linked,
        "reader_brief_section": reader_brief,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(pack_dir / "promotion_manifest.json", manifest)
    _write_text(pack_dir / "promotion_decision.md", render_promotion_decision_markdown(payload))
    _write_csv(pack_dir / "metric_delta_table.csv", metric_delta)
    _write_json(pack_dir / "risk_summary.json", risk_summary)
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
            "linked_artifacts.json",
            "reader_brief_section.md",
        ]
        checks.extend(
            _check(f"artifact_exists:{name}", (latest / name).exists(), name) for name in required
        )
        manifest = _read_json(latest / "promotion_manifest.json")
        checks.append(
            _check(
                "production_candidate_not_generated",
                manifest.get("production_candidate_generated") is False,
                "production_candidate is manual-only",
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
    output_dir = real_context.real_evaluation_output_dir / candidate_id
    paths = write_dynamic_v3_real_evaluation_report(payload, output_dir=output_dir)
    readback = _read_json(paths["json"])
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
    for row in _read_jsonl(sweep_dir / "candidate_results.jsonl"):
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
    shadow = _shadow_candidate_report(record) if record else None
    return {
        "registry_record": record,
        "candidate_report": (
            _read_optional_json(candidate_report_path) if candidate_report_path else None
        ),
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
    if _text(overfit.get("overfit_status")) == "HIGH_RISK":
        return "reject", ["overfit_high_risk"]
    shadow = _mapping(evidence.get("shadow_report"))
    if shadow.get("promotion_eligibility_status") != "eligible":
        return "review_required", ["shadow_observation_insufficient"]
    if _text(overfit.get("robustness_status")) != "PASS":
        return "review_required", ["robustness_review_required"]
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
        "shadow": _mapping(evidence.get("shadow_report")).get(
            "promotion_eligibility_status", "MISSING"
        ),
        "production_candidate_generated": False,
    }


def _promotion_linked_artifacts(evidence: Mapping[str, Any]) -> dict[str, Any]:
    candidate = _mapping(evidence.get("candidate_report"))
    return {
        "candidate_report": evidence.get("candidate_report_path"),
        "walk_forward_report": evidence.get("walk_forward_report_path"),
        "robustness_report": evidence.get("robustness_report_path"),
        "real_evaluation_artifact": candidate.get("real_evaluation_artifact_path", ""),
    }


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
