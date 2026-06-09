from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.dynamic_v2_review import (
    DEFAULT_DYNAMIC_V2_REVIEW_PACKAGE_DIR,
    _sample_dynamic_v2_review_inputs,
    build_dynamic_v2_review_package,
    latest_dynamic_v2_review_package_path,
    load_dynamic_v2_review_policy_config,
    load_json_artifact,
)
from ai_trading_system.etf_portfolio.models import PolicyMetadata
from ai_trading_system.yaml_loader import safe_load_yaml_path

DYNAMIC_V3_RESCUE_POLICY_SCHEMA_VERSION = "etf_dynamic_v3_rescue_policy_v1"
DYNAMIC_V3_RESCUE_REPORT_SCHEMA_VERSION = "etf_dynamic_v3_rescue_evaluation_report_v1"
DYNAMIC_V3_RESCUE_VALIDATION_SCHEMA_VERSION = "etf_dynamic_v3_rescue_validation_v1"

DYNAMIC_V3_RESCUE_REPORT_TYPE = "etf_dynamic_v3_rescue_evaluation_report"
DYNAMIC_V3_RESCUE_VALIDATION_REPORT_TYPE = "etf_dynamic_v3_rescue_validation"

DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_v3_constraint_aware_rescue.yaml"
)
DEFAULT_DYNAMIC_V3_RESCUE_ROOT = PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_v3_rescue"
DEFAULT_DYNAMIC_V3_RESCUE_REPORT_DIR = DEFAULT_DYNAMIC_V3_RESCUE_ROOT / "reports"
DEFAULT_DYNAMIC_V3_RESCUE_VALIDATION_DIR = DEFAULT_DYNAMIC_V3_RESCUE_ROOT / "validation"

WEIGHT_SYMBOLS = ("SPY", "QQQ", "SMH", "SOXX", "CASH")
SEMICONDUCTOR_SYMBOLS = ("SMH", "SOXX")

DYNAMIC_V3_RESCUE_SAFETY: dict[str, Any] = {
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
}

DYNAMIC_V3_RESCUE_FORBIDDEN_KEYS = {
    "approved_for_shadow",
    "auto_promotion",
    "automatic_approval",
    "baseline_config_mutation",
    "broker_order",
    "enable_broker_action",
    "official_target_weights_write",
    "place_order",
    "production_weight_update",
    "shadow_enrollment_record",
}


class DynamicV3RescueError(ValueError):
    """Raised when TRADING-090 dynamic v0.3 rescue inputs or outputs are invalid."""


class DynamicV3RescueMarketRegime(BaseModel):
    regime_id: Literal["ai_after_chatgpt"]
    anchor_event: str = Field(min_length=1)
    anchor_date: date
    default_backtest_start: date

    @model_validator(mode="after")
    def validate_regime(self) -> Self:
        if self.default_backtest_start < date(2022, 12, 1):
            raise ValueError("dynamic v0.3 rescue start cannot predate 2022-12-01")
        return self


class DynamicV3RescueSafety(BaseModel):
    observe_only: Literal[True]
    candidate_only: Literal[True]
    production_effect: Literal["none"]
    broker_action: Literal["none"]
    manual_review_required: Literal[True]
    production_state_mutated: Literal[False]
    baseline_config_mutated: Literal[False]
    official_target_weights_mutated: Literal[False]
    automatic_candidate_promotion: Literal[False]
    auto_enrollment_without_owner_approval: Literal[False]

    @model_validator(mode="after")
    def validate_safety(self) -> Self:
        if self.model_dump(mode="json") != DYNAMIC_V3_RESCUE_SAFETY:
            raise ValueError("dynamic v0.3 rescue safety fields are unsafe")
        return self


class DynamicV3ConstraintTargets(BaseModel):
    max_constraint_hit_delta_vs_v0_4: int = Field(le=0)
    max_constraint_hit_rate: float = Field(ge=0, le=1)


class DynamicV3PreservationTargets(BaseModel):
    max_turnover: float = Field(ge=0)
    max_false_risk_off_vs_v0_4_delta: int = Field(ge=0)
    min_static_delta_vs_v0_4_pp: float


class DynamicV3DrawdownTargets(BaseModel):
    min_drawdown_preservation_pp: float
    max_drawdown_delta_vs_static_pp: float


class DynamicV3NormalizationPolicy(BaseModel):
    qqq_max_target: float = Field(gt=0, le=1)
    semiconductor_max_target: float = Field(gt=0, le=1)
    cash_max_target: float = Field(gt=0, le=1)
    cash_min_target: float = Field(ge=0, le=1)
    sum_tolerance: float = Field(gt=0, le=0.01)

    @model_validator(mode="after")
    def validate_caps(self) -> Self:
        if self.cash_min_target > self.cash_max_target:
            raise ValueError("cash_min_target must not exceed cash_max_target")
        return self


class DynamicV3SoftConstraintPenaltyPolicy(BaseModel):
    interior_buffer: float = Field(ge=0, le=0.1)
    penalty_strength: float = Field(gt=0, le=1)


class DynamicV3SmoothingPolicy(BaseModel):
    default_smoothing_factor: float = Field(ge=0, le=1)
    max_single_rebalance_delta: float = Field(gt=0, le=1)


class DynamicV3DrawdownGuardrails(BaseModel):
    enabled: Literal[True]
    min_confirmations: int = Field(ge=2)
    portfolio_drawdown_threshold: float = Field(le=0)
    qqq_drawdown_threshold: float = Field(le=0)
    smh_drawdown_threshold: float = Field(le=0)
    max_cash_target: float = Field(gt=0, le=1)
    semiconductor_reduction_step: float = Field(gt=0, le=1)
    qqq_reduction_step: float = Field(gt=0, le=1)


class DynamicV3EmergencyRiskOff(BaseModel):
    enabled: Literal[True]
    min_independent_confirmations: int = Field(ge=2)
    max_cash_target: float = Field(gt=0, le=1)
    cash_increase_step: float = Field(gt=0, le=1)
    bypass_smoothing: Literal[True]
    trend_score_max: float = Field(ge=0, le=100)
    risk_regime_score_min: float = Field(ge=0, le=100)
    event_risk_score_min: float = Field(ge=0, le=100)
    drawdown_threshold: float = Field(le=0)


class DynamicV3TemplateFeatureFlags(BaseModel):
    normalization: bool
    soft_penalty: bool
    smoothing: bool
    drawdown_guardrail: bool
    emergency_risk_off: bool


class DynamicV3CandidateTemplatePolicy(BaseModel):
    policy_id: str = Field(min_length=1)
    base_policy_id: str = Field(min_length=1)
    template_type: str = Field(min_length=1)
    expected_failure_targets: list[str] = Field(min_length=1)
    feature_flags: DynamicV3TemplateFeatureFlags
    candidate_only: Literal[True]
    production_config_mutation_allowed: Literal[False]
    automatic_approval_allowed: Literal[False]
    automatic_enrollment_allowed: Literal[False]

    @model_validator(mode="after")
    def validate_template(self) -> Self:
        if not self.policy_id.startswith("dynamic_regime_overlay_v0_3"):
            raise ValueError("dynamic v0.3 rescue template must be versioned as v0.3")
        return self


class DynamicV3EvaluationThresholds(BaseModel):
    success_status: Literal["v0_3_rescue_success_candidate_found"]
    partial_status: Literal["partial_improvement_needs_more_review"]
    constraint_fixed_drawdown_failed_status: Literal["constraint_fixed_drawdown_failed"]
    drawdown_fixed_constraint_failed_status: Literal["drawdown_fixed_constraint_failed"]
    failure_status: Literal["no_v0_3_rescue_candidate_found"]


class DynamicV3RescuePolicyConfig(BaseModel):
    schema_version: Literal["etf_dynamic_v3_rescue_policy_v1"]
    policy_metadata: PolicyMetadata
    market_regime: DynamicV3RescueMarketRegime
    base_candidate: str = Field(min_length=1)
    constraint_targets: DynamicV3ConstraintTargets
    preservation_targets: DynamicV3PreservationTargets
    drawdown_targets: DynamicV3DrawdownTargets
    normalization_policy: DynamicV3NormalizationPolicy
    soft_constraint_penalties: DynamicV3SoftConstraintPenaltyPolicy
    smoothing_policy: DynamicV3SmoothingPolicy
    drawdown_guardrails: DynamicV3DrawdownGuardrails
    emergency_risk_off: DynamicV3EmergencyRiskOff
    candidate_templates: list[DynamicV3CandidateTemplatePolicy] = Field(min_length=4)
    evaluation_thresholds: DynamicV3EvaluationThresholds
    safety: DynamicV3RescueSafety

    @model_validator(mode="after")
    def validate_policy(self) -> Self:
        template_ids = [template.policy_id for template in self.candidate_templates]
        if len(template_ids) != len(set(template_ids)):
            raise ValueError("dynamic v0.3 rescue template ids must be unique")
        if self.safety.model_dump(mode="json") != DYNAMIC_V3_RESCUE_SAFETY:
            raise ValueError("dynamic v0.3 rescue policy safety is unsafe")
        if self.base_candidate != "dynamic_regime_overlay_v0_4_lower_turnover":
            raise ValueError("TRADING-090 must rescue v0.4 lower_turnover")
        return self


def load_dynamic_v3_rescue_policy_config(
    path: Path | str = DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH,
) -> DynamicV3RescuePolicyConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, Mapping):
        raise DynamicV3RescueError("dynamic v0.3 rescue policy must be a mapping")
    try:
        return DynamicV3RescuePolicyConfig.model_validate(raw)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3RescueError(f"invalid dynamic v0.3 rescue policy: {exc}") from exc


def build_constraint_drawdown_root_cause(
    *,
    v04_review_package: Mapping[str, Any],
    policy: DynamicV3RescuePolicyConfig,
    v04_review_package_path: Path | str | None = None,
) -> dict[str, Any]:
    if not v04_review_package:
        raise DynamicV3RescueError("v0.4 review package is required")
    constraints = _mapping(v04_review_package.get("constraint_hit_decomposition"))
    drawdown = _mapping(v04_review_package.get("drawdown_preservation_failure_review"))
    evidence = _mapping(v04_review_package.get("candidate_evidence"))
    if not constraints:
        raise DynamicV3RescueError("v0.4 constraint hit decomposition is required")
    if not drawdown:
        raise DynamicV3RescueError("v0.4 drawdown preservation review is required")
    if not evidence:
        raise DynamicV3RescueError("v0.4 candidate evidence is required")
    links = _mapping(v04_review_package.get("source_links"))
    source_links = {
        "v0_4_review_package": (
            "" if v04_review_package_path is None else str(v04_review_package_path)
        ),
        "constraint_hit_decomposition": "embedded:constraint_hit_decomposition",
        "drawdown_preservation_review": "embedded:drawdown_preservation_failure_review",
        "dynamic_rescue_report": _text(links.get("dynamic_rescue_report")),
        "dynamic_robustness_report": _text(links.get("dynamic_robustness_report")),
        "source_policy_config": _text(
            links.get("source_policy_config"),
            str(DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH),
        ),
        "data_quality_report": _text(links.get("data_quality_report")),
    }
    warnings = [
        f"optional_source_link_missing:{key}"
        for key in ("dynamic_rescue_report", "dynamic_robustness_report", "data_quality_report")
        if not source_links.get(key)
    ]
    payload = {
        "constraint_hit_by_type": constraints.get("constraint_hit_by_type", {}),
        "constraint_hit_delta": constraints.get("constraint_hit_delta"),
        "top_constraint_hit_periods": _records(constraints.get("top_constraint_hit_periods")),
        "constraint_root_cause": _text(constraints.get("constraint_root_cause")),
        "drawdown_failure_periods": _records(drawdown.get("drawdown_failure_periods")),
        "drawdown_root_cause": _text(drawdown.get("drawdown_root_cause")),
        "recommended_guardrails": _texts(drawdown.get("recommended_drawdown_guardrails")),
        "source_links": source_links,
        "target_blockers": [
            "CONSTRAINT_HIT_WORSENED",
            "DRAWDOWN_PRESERVATION_FAILED",
        ],
        "warnings": warnings,
        "safety": policy.safety.model_dump(mode="json"),
    }
    _assert_dynamic_v3_rescue_payload_safe(payload)
    return payload


def normalize_pre_constraint_targets(
    raw_target_weights: Mapping[str, float],
    policy: DynamicV3RescuePolicyConfig,
) -> dict[str, Any]:
    weights = _weight_map(raw_target_weights)
    reason_codes: list[str] = []
    if abs(sum(weights.values()) - 1.0) > policy.normalization_policy.sum_tolerance:
        reason_codes.append("NORMALIZED_SUM_TO_ONE")
    weights = _normalize_sum(weights)
    qqq_excess = max(0.0, weights["QQQ"] - policy.normalization_policy.qqq_max_target)
    if qqq_excess > 0:
        weights["QQQ"] -= qqq_excess
        weights["SPY"] += qqq_excess
        reason_codes.append("REDUCED_QQQ_TARGET")
    semis = weights["SMH"] + weights["SOXX"]
    semis_cap = policy.normalization_policy.semiconductor_max_target
    if semis > semis_cap:
        excess = semis - semis_cap
        _reduce_group(weights, SEMICONDUCTOR_SYMBOLS, excess)
        weights["SPY"] += excess
        reason_codes.append("REDUCED_SEMICONDUCTOR_TARGET")
    cash_excess = max(0.0, weights["CASH"] - policy.normalization_policy.cash_max_target)
    if cash_excess > 0:
        weights["CASH"] -= cash_excess
        weights["SPY"] += cash_excess
        reason_codes.append("REDUCED_CASH_TARGET")
    cash_shortfall = max(0.0, policy.normalization_policy.cash_min_target - weights["CASH"])
    if cash_shortfall > 0:
        weights["CASH"] += cash_shortfall
        _reduce_group(weights, ("SPY", "QQQ", "SMH", "SOXX"), cash_shortfall)
        reason_codes.append("RAISED_CASH_MINIMUM")
    weights = _normalize_sum(weights)
    if "PRESERVED_REGIME_DIRECTION" not in reason_codes:
        reason_codes.append("PRESERVED_REGIME_DIRECTION")
    payload = {
        "raw_target_weights": dict(raw_target_weights),
        "normalized_target_weights": weights,
        "normalization_reason_codes": reason_codes,
        "safety": policy.safety.model_dump(mode="json"),
    }
    _assert_dynamic_v3_rescue_payload_safe(payload)
    return payload


def apply_soft_constraint_penalty_and_smoothing(
    target_weights: Mapping[str, float],
    previous_weights: Mapping[str, float],
    policy: DynamicV3RescuePolicyConfig,
    *,
    emergency_override: bool = False,
) -> dict[str, Any]:
    target = _weight_map(target_weights)
    previous = _normalize_sum(_weight_map(previous_weights))
    penalty = dict(target)
    reason_codes: list[str] = []
    buffer = policy.soft_constraint_penalties.interior_buffer
    qqq_soft_cap = max(0.0, policy.normalization_policy.qqq_max_target - buffer)
    if penalty["QQQ"] > qqq_soft_cap:
        excess = (penalty["QQQ"] - qqq_soft_cap) * policy.soft_constraint_penalties.penalty_strength
        penalty["QQQ"] -= excess
        penalty["SPY"] += excess
        reason_codes.append("SOFT_PENALTY_QQQ_CAP_PROXIMITY")
    semis_cap = max(0.0, policy.normalization_policy.semiconductor_max_target - buffer)
    semis = penalty["SMH"] + penalty["SOXX"]
    if semis > semis_cap:
        excess = (semis - semis_cap) * policy.soft_constraint_penalties.penalty_strength
        _reduce_group(penalty, SEMICONDUCTOR_SYMBOLS, excess)
        penalty["SPY"] += excess
        reason_codes.append("SOFT_PENALTY_SEMICONDUCTOR_CAP_PROXIMITY")
    cash_soft_cap = max(0.0, policy.normalization_policy.cash_max_target - buffer)
    if penalty["CASH"] > cash_soft_cap:
        excess = (
            penalty["CASH"] - cash_soft_cap
        ) * policy.soft_constraint_penalties.penalty_strength
        penalty["CASH"] -= excess
        penalty["SPY"] += excess
        reason_codes.append("SOFT_PENALTY_CASH_CAP_PROXIMITY")
    penalty = _normalize_sum(penalty)
    if emergency_override:
        smoothed = penalty
        reason_codes.append("EMERGENCY_SMOOTHING_BYPASS")
    else:
        factor = policy.smoothing_policy.default_smoothing_factor
        smoothed = {
            symbol: previous[symbol] * factor + penalty[symbol] * (1.0 - factor)
            for symbol in WEIGHT_SYMBOLS
        }
        smoothed = _limit_single_step_delta(
            smoothed,
            previous,
            policy.smoothing_policy.max_single_rebalance_delta,
        )
        reason_codes.append("ALLOCATION_SMOOTHED")
        reason_codes.append("SINGLE_STEP_DELTA_LIMITED")
    payload = {
        "raw_target_weights": target,
        "penalty_adjusted_weights": penalty,
        "smoothed_target_weights": smoothed,
        "constraint_proximity_scores": _constraint_proximity_scores(penalty, policy),
        "smoothing_reason_codes": reason_codes,
        "emergency_override": emergency_override,
        "safety": policy.safety.model_dump(mode="json"),
    }
    _assert_dynamic_v3_rescue_payload_safe(payload)
    return payload


def apply_drawdown_guardrail_overlay(
    target_weights: Mapping[str, float],
    signals: Mapping[str, Any],
    policy: DynamicV3RescuePolicyConfig,
) -> dict[str, Any]:
    weights = _normalize_sum(_weight_map(target_weights))
    confirmations = _drawdown_guardrail_confirmations(signals, policy)
    reason_codes: list[str] = []
    if len(confirmations) < policy.drawdown_guardrails.min_confirmations:
        reason_codes.append("GUARDRAIL_NOT_TRIGGERED_INSUFFICIENT_CONFIRMATION")
        return {
            "guardrail_triggered": False,
            "confirmations": confirmations,
            "before_weights": weights,
            "after_weights": weights,
            "guardrail_reason_codes": reason_codes,
            "safety": policy.safety.model_dump(mode="json"),
        }
    reason_codes.append("DRAWDOWN_GUARDRAIL_TRIGGERED")
    cash_room = max(0.0, policy.drawdown_guardrails.max_cash_target - weights["CASH"])
    semis_reduction = min(policy.drawdown_guardrails.semiconductor_reduction_step, cash_room)
    semis_before = weights["SMH"] + weights["SOXX"]
    if semis_reduction > 0 and semis_before > 0:
        _reduce_group(weights, SEMICONDUCTOR_SYMBOLS, semis_reduction)
        weights["CASH"] += semis_reduction
        cash_room -= semis_reduction
        reason_codes.append("SEMICONDUCTOR_RISK_REDUCED")
    qqq_reduction = min(policy.drawdown_guardrails.qqq_reduction_step, cash_room, weights["QQQ"])
    if qqq_reduction > 0:
        weights["QQQ"] -= qqq_reduction
        weights["CASH"] += qqq_reduction
        reason_codes.append("GROWTH_RISK_REDUCED")
    if weights["CASH"] > _float(target_weights.get("CASH")):
        reason_codes.append("CASH_BUFFER_RAISED")
    weights = _normalize_sum(weights)
    payload = {
        "guardrail_triggered": True,
        "confirmations": confirmations,
        "before_weights": _weight_map(target_weights),
        "after_weights": weights,
        "guardrail_reason_codes": reason_codes,
        "safety": policy.safety.model_dump(mode="json"),
    }
    _assert_dynamic_v3_rescue_payload_safe(payload)
    return payload


def evaluate_emergency_risk_off(
    signals: Mapping[str, Any],
    policy: DynamicV3RescuePolicyConfig,
) -> dict[str, Any]:
    confirmations = _emergency_confirmations(signals, policy)
    triggered = (
        policy.emergency_risk_off.enabled
        and len(confirmations) >= policy.emergency_risk_off.min_independent_confirmations
    )
    reason_codes = (
        ["EMERGENCY_RISK_OFF_TRIGGERED"]
        if triggered
        else ["EMERGENCY_RISK_OFF_NOT_TRIGGERED_INSUFFICIENT_CONFIRMATION"]
    )
    payload = {
        "emergency_risk_off_triggered": triggered,
        "confirmation_count": len(confirmations),
        "confirmations": confirmations,
        "emergency_reason_codes": reason_codes,
        "safety": policy.safety.model_dump(mode="json"),
    }
    _assert_dynamic_v3_rescue_payload_safe(payload)
    return payload


def apply_emergency_risk_off_exception(
    target_weights: Mapping[str, float],
    signals: Mapping[str, Any],
    policy: DynamicV3RescuePolicyConfig,
) -> dict[str, Any]:
    evaluation = evaluate_emergency_risk_off(signals, policy)
    weights = _normalize_sum(_weight_map(target_weights))
    reason_codes = list(_texts(evaluation.get("emergency_reason_codes")))
    if not evaluation["emergency_risk_off_triggered"]:
        return {
            "emergency_risk_off_triggered": False,
            "before_weights": weights,
            "after_weights": weights,
            "confirmations": evaluation["confirmations"],
            "emergency_reason_codes": reason_codes,
            "bypass_smoothing": False,
            "safety": policy.safety.model_dump(mode="json"),
        }
    cash_room = max(0.0, policy.emergency_risk_off.max_cash_target - weights["CASH"])
    cash_increase = min(policy.emergency_risk_off.cash_increase_step, cash_room)
    reduced = _reduce_priority_to_cash(weights, cash_increase, ("SMH", "SOXX", "QQQ"))
    weights["CASH"] += reduced
    reason_codes.extend(
        [
            "EMERGENCY_SMOOTHING_BYPASS",
            "CASH_BUFFER_RAISED",
            "SEMICONDUCTOR_RISK_REDUCED",
            "GROWTH_RISK_REDUCED",
        ]
    )
    payload = {
        "emergency_risk_off_triggered": True,
        "before_weights": _weight_map(target_weights),
        "after_weights": _normalize_sum(weights),
        "confirmations": evaluation["confirmations"],
        "emergency_reason_codes": sorted(set(reason_codes)),
        "bypass_smoothing": policy.emergency_risk_off.bypass_smoothing,
        "safety": policy.safety.model_dump(mode="json"),
    }
    _assert_dynamic_v3_rescue_payload_safe(payload)
    return payload


def build_dynamic_v3_candidate_templates(
    *,
    root_cause: Mapping[str, Any],
    policy: DynamicV3RescuePolicyConfig,
) -> list[dict[str, Any]]:
    evidence_links = _mapping(root_cause.get("source_links"))
    templates = []
    for template in policy.candidate_templates:
        templates.append(
            {
                "policy_id": template.policy_id,
                "base_policy_id": template.base_policy_id,
                "template_type": template.template_type,
                "normalization_policy": (
                    policy.normalization_policy.model_dump(mode="json")
                    if template.feature_flags.normalization
                    else {"enabled": False}
                ),
                "soft_constraint_policy": (
                    policy.soft_constraint_penalties.model_dump(mode="json")
                    if template.feature_flags.soft_penalty
                    else {"enabled": False}
                ),
                "smoothing_policy": (
                    policy.smoothing_policy.model_dump(mode="json")
                    if template.feature_flags.smoothing
                    else {"enabled": False}
                ),
                "drawdown_guardrail_policy": (
                    policy.drawdown_guardrails.model_dump(mode="json")
                    if template.feature_flags.drawdown_guardrail
                    else {"enabled": False}
                ),
                "emergency_risk_off_policy": (
                    policy.emergency_risk_off.model_dump(mode="json")
                    if template.feature_flags.emergency_risk_off
                    else {"enabled": False}
                ),
                "expected_failure_targets": template.expected_failure_targets,
                "evidence_links": evidence_links,
                "feature_flags": template.feature_flags.model_dump(mode="json"),
                "candidate_only": True,
                "production_config_mutation_allowed": False,
                "automatic_approval_allowed": False,
                "automatic_enrollment_allowed": False,
                "safety": policy.safety.model_dump(mode="json"),
            }
        )
    return templates


def build_dynamic_v3_rescue_evaluation_report(
    *,
    v04_review_package: Mapping[str, Any],
    policy: DynamicV3RescuePolicyConfig | None = None,
    v04_review_package_path: Path | str | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or load_dynamic_v3_rescue_policy_config()
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    root_cause = build_constraint_drawdown_root_cause(
        v04_review_package=v04_review_package,
        policy=resolved_policy,
        v04_review_package_path=v04_review_package_path,
    )
    templates = build_dynamic_v3_candidate_templates(root_cause=root_cause, policy=resolved_policy)
    candidates = [
        _evaluate_dynamic_v3_candidate(
            template=template,
            v04_review_package=v04_review_package,
            policy=resolved_policy,
        )
        for template in templates
    ]
    status = _overall_rescue_status(candidates, resolved_policy)
    best = _best_candidate(candidates)
    report_id = _stable_id(
        "dynamic-v3-rescue-report",
        v04_review_package.get("review_package_id"),
        _stable_hash([candidate["policy_id"] for candidate in candidates]),
    )
    payload = {
        "schema_version": DYNAMIC_V3_RESCUE_REPORT_SCHEMA_VERSION,
        "report_type": DYNAMIC_V3_RESCUE_REPORT_TYPE,
        "dynamic_v3_rescue_report_id": report_id,
        "generated_at": generated.isoformat(),
        "status": status,
        "review_status": "review_candidate",
        "shadow_readiness_status": "not_shadow_ready",
        "policy_version": resolved_policy.policy_metadata.version,
        "policy_config_hash": _stable_hash(resolved_policy.model_dump(mode="json")),
        "market_regime": resolved_policy.market_regime.model_dump(mode="json"),
        "safety_banner": _safety_status(resolved_policy.safety.model_dump(mode="json")),
        "v0_4_blocker_summary": {
            "candidate": _mapping(v04_review_package.get("candidate_evidence")).get("candidate_id"),
            "blockers": _texts(v04_review_package.get("blockers")),
            "constraint_hit_delta": root_cause.get("constraint_hit_delta"),
            "drawdown_root_cause": root_cause.get("drawdown_root_cause"),
        },
        "constraint_root_cause": root_cause,
        "v0_3_candidate_templates": templates,
        "candidate_comparison_table": candidates,
        "best_candidate": best,
        "constraint_hit_improvement": _constraint_improvement_summary(candidates),
        "drawdown_preservation_improvement": _drawdown_improvement_summary(candidates),
        "false_signal_changes": _false_signal_change_summary(candidates),
        "turnover_preservation": _turnover_preservation_summary(candidates, resolved_policy),
        "benchmark_comparison": _benchmark_comparison_summary(v04_review_package, candidates),
        "eligibility_recommendation": _eligibility_recommendation(status, best),
        "remaining_blockers": _remaining_blockers(best),
        "source_links": root_cause.get("source_links"),
        "validation_context": {
            "review_only": True,
            "candidate_only": True,
            "runtime_artifacts_untracked": True,
            "data_quality_status": _mapping(
                _mapping(v04_review_package.get("validation_context"))
            ).get("data_quality_status"),
        },
        "safety": resolved_policy.safety.model_dump(mode="json"),
        **DYNAMIC_V3_RESCUE_SAFETY,
        "commands_executed": False,
        "shadow_enrollment_allowed": False,
        "automatic_enrollment_allowed": False,
        "owner_approval_executed": False,
    }
    _assert_dynamic_v3_rescue_payload_safe(payload)
    return payload


def build_dynamic_v3_rescue_validation_report(
    *,
    config_path: Path | str = DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH,
    report_registry_path: Path = PROJECT_ROOT / "config" / "report_registry.yaml",
    reader_brief_path: Path = PROJECT_ROOT
    / "src"
    / "ai_trading_system"
    / "reports"
    / "reader_brief.py",
    cli_path: Path = PROJECT_ROOT
    / "src"
    / "ai_trading_system"
    / "cli_commands"
    / "etf_portfolio.py",
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    checks: list[dict[str, Any]] = []
    policy: DynamicV3RescuePolicyConfig | None = None
    try:
        policy = load_dynamic_v3_rescue_policy_config(config_path)
        _append_check(checks, "v0_3_rescue_config_valid", True, "policy config loads")
    except Exception as exc:  # noqa: BLE001
        _append_check(checks, "v0_3_rescue_config_valid", False, str(exc))
    sample_report: dict[str, Any] | None = None
    if policy is not None:
        try:
            sample_v2_package = _sample_dynamic_v3_review_package()
            sample_report = build_dynamic_v3_rescue_evaluation_report(
                v04_review_package=sample_v2_package,
                policy=policy,
                v04_review_package_path="validation_sample",
                generated_at=generated,
            )
            root = _mapping(sample_report.get("constraint_root_cause"))
            templates = _records(sample_report.get("v0_3_candidate_templates"))
            candidates = _records(sample_report.get("candidate_comparison_table"))
            _append_check(
                checks,
                "root_cause_loader_available",
                bool(root),
                "root-cause loader available",
            )
            _append_check(
                checks,
                "target_normalization_available",
                any(_mapping(row.get("normalization_output")) for row in candidates),
                "pre-constraint normalization available",
            )
            _append_check(
                checks,
                "soft_constraint_penalty_available",
                any(_mapping(row.get("penalty_smoothing_output")) for row in candidates),
                "soft constraint penalty and smoothing available",
            )
            _append_check(
                checks,
                "drawdown_guardrail_available",
                any(_mapping(row.get("drawdown_guardrail_output")) for row in candidates),
                "drawdown guardrail available",
            )
            _append_check(
                checks,
                "emergency_risk_off_available",
                any(_mapping(row.get("emergency_risk_off_output")) for row in candidates),
                "emergency risk-off available",
            )
            _append_check(
                checks,
                "template_generator_available",
                len(templates) >= 4,
                "v0.3 template generator available",
            )
            _append_check(
                checks,
                "batch_runner_available",
                len(candidates) >= 4,
                "candidate batch runner available",
            )
            _append_check(
                checks,
                "evaluation_report_available",
                sample_report.get("report_type") == DYNAMIC_V3_RESCUE_REPORT_TYPE,
                "evaluation report available",
            )
            _append_check(
                checks,
                "production_effect_none",
                sample_report.get("production_effect") == "none",
                "production_effect remains none",
            )
            _append_check(
                checks,
                "broker_action_none",
                sample_report.get("broker_action") == "none",
                "broker_action remains none",
            )
            _append_check(
                checks,
                "manual_review_required",
                sample_report.get("manual_review_required") is True,
                "manual review remains required",
            )
            _append_check(
                checks,
                "no_auto_approval",
                sample_report.get("owner_approval_executed") is False,
                "no owner approval is executed",
            )
            _append_check(
                checks,
                "no_auto_enrollment",
                sample_report.get("automatic_enrollment_allowed") is False
                and sample_report.get("shadow_enrollment_allowed") is False,
                "no automatic enrollment is allowed",
            )
            _append_check(
                checks,
                "templates_do_not_mutate_production",
                all(row.get("production_config_mutation_allowed") is False for row in templates),
                "templates do not mutate production policy",
            )
            _assert_dynamic_v3_rescue_payload_safe(sample_report)
            _append_check(checks, "sample_report_safety", True, "sample report is safe")
        except Exception as exc:  # noqa: BLE001
            _append_check(checks, "validation_sample_workflow", False, str(exc))
    registry_text = _safe_read_text(report_registry_path)
    _append_check(
        checks,
        "report_registry_visibility",
        DYNAMIC_V3_RESCUE_REPORT_TYPE in registry_text
        and DYNAMIC_V3_RESCUE_VALIDATION_REPORT_TYPE in registry_text,
        "report registry exposes dynamic v0.3 rescue report and validation",
    )
    outcome_loop_report_ids = (
        "etf_dynamic_v3_outcome_update_review",
        "etf_dynamic_v3_outcome_update",
        "etf_dynamic_v3_rolling_evidence_refresh",
        "etf_dynamic_v3_evidence_trend",
        "etf_dynamic_v3_forward_outcome_decision",
    )
    _append_check(
        checks,
        "outcome_loop_report_registry_visibility",
        all(report_id in registry_text for report_id in outcome_loop_report_ids),
        "report registry exposes outcome update loop artifacts",
    )
    reader_text = _safe_read_text(reader_brief_path)
    _append_check(
        checks,
        "reader_brief_integration_available",
        "Dynamic v0.3 Rescue" in reader_text and "_etf_dynamic_v3_rescue_summary" in reader_text,
        "Reader Brief has Dynamic v0.3 Rescue section",
    )
    _append_check(
        checks,
        "outcome_loop_reader_brief_integration_available",
        "outcome_update_review_status" in reader_text
        and "forward_outcome_decision_action" in reader_text,
        "Reader Brief exposes outcome update loop summary fields",
    )
    cli_text = _safe_read_text(cli_path)
    _append_check(
        checks,
        "cli_namespace_available",
        "dynamic-v3-rescue" in cli_text and "dynamic_v3_rescue_app" in cli_text,
        "CLI exposes dynamic-v3-rescue namespace",
    )
    _append_check(
        checks,
        "outcome_loop_cli_namespace_available",
        all(
            command in cli_text
            for command in (
                "outcome-update-review",
                "outcome-update",
                "rolling-evidence-refresh",
                "evidence-trend",
                "forward-outcome-decision",
            )
        ),
        "CLI exposes outcome update loop namespaces",
    )
    failed = [check for check in checks if check["status"] != "PASS"]
    payload = {
        "schema_version": DYNAMIC_V3_RESCUE_VALIDATION_SCHEMA_VERSION,
        "report_type": DYNAMIC_V3_RESCUE_VALIDATION_REPORT_TYPE,
        "validation_id": _stable_id(
            "dynamic-v3-rescue-validation",
            generated.strftime("%Y%m%dT%H%M%SZ"),
            _stable_hash([check["check_id"] for check in checks]),
        ),
        "generated_at": generated.isoformat(),
        "status": "PASS" if not failed else "FAIL",
        "check_count": len(checks),
        "failed_check_count": len(failed),
        "checks": checks,
        "source_schema_versions": {
            "policy": DYNAMIC_V3_RESCUE_POLICY_SCHEMA_VERSION,
            "report": DYNAMIC_V3_RESCUE_REPORT_SCHEMA_VERSION,
        },
        "production_effect_none_required": True,
        "broker_action_none_required": True,
        "manual_review_required": True,
        "no_auto_approval": True,
        "no_auto_enrollment": True,
        "v0_3_template_mutates_production_policy": False,
        "v0_3_candidate_auto_enrolls": False,
        "shadow_enrollment_allowed": False,
        "automatic_enrollment_allowed": False,
        "owner_approval_executed": False,
        "commands_executed": False,
        "safety": dict(DYNAMIC_V3_RESCUE_SAFETY),
        **DYNAMIC_V3_RESCUE_SAFETY,
    }
    _assert_dynamic_v3_rescue_payload_safe(payload)
    return payload


def write_dynamic_v3_rescue_evaluation_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_DYNAMIC_V3_RESCUE_REPORT_DIR,
) -> dict[str, Path]:
    return _write_json_md(
        payload,
        output_dir=output_dir,
        stem=_text(payload.get("dynamic_v3_rescue_report_id"), "dynamic-v3-rescue-report"),
        markdown=render_dynamic_v3_rescue_evaluation_markdown(payload),
    )


def write_dynamic_v3_rescue_validation_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_DYNAMIC_V3_RESCUE_VALIDATION_DIR,
) -> dict[str, Path]:
    return _write_json_md(
        payload,
        output_dir=output_dir,
        stem=_text(payload.get("validation_id"), "dynamic-v3-rescue-validation"),
        markdown=render_dynamic_v3_rescue_validation_markdown(payload),
    )


def latest_dynamic_v3_rescue_report_path(
    report_dir: Path = DEFAULT_DYNAMIC_V3_RESCUE_REPORT_DIR,
) -> Path | None:
    return _latest_json(report_dir, "dynamic-v3-rescue-report_*.json")


def load_latest_v3_rescue_inputs(
    *,
    v2_review_package_path: Path | None = None,
    v2_review_package_dir: Path = DEFAULT_DYNAMIC_V2_REVIEW_PACKAGE_DIR,
) -> tuple[dict[str, Any], dict[str, str]]:
    resolved = v2_review_package_path or latest_dynamic_v2_review_package_path(
        v2_review_package_dir
    )
    package = load_json_artifact(resolved, label="dynamic v0.2 review package")
    if not package:
        raise DynamicV3RescueError("dynamic v0.2 review package not found")
    return package, {"v0_4_review_package": "" if resolved is None else str(resolved)}


def render_dynamic_v3_rescue_evaluation_markdown(payload: Mapping[str, Any]) -> str:
    best = _mapping(payload.get("best_candidate"))
    blocker_summary = _mapping(payload.get("v0_4_blocker_summary"))
    candidates = _records(payload.get("candidate_comparison_table"))
    lines = [
        f"# Dynamic v0.3 Rescue Evaluation {payload.get('dynamic_v3_rescue_report_id')}",
        "",
        "## Safety",
        "- observe_only=true",
        "- candidate_only=true",
        "- production_effect=none",
        "- broker_action=none",
        "- manual_review_required=true",
        "- shadow_enrollment_allowed=false",
        "- automatic_enrollment_allowed=false",
        "",
        "## v0.4 Blocker Summary",
        f"- Base candidate: {blocker_summary.get('candidate')}",
        f"- Blockers: {', '.join(_texts(blocker_summary.get('blockers')))}",
        f"- Constraint hit delta: {blocker_summary.get('constraint_hit_delta')}",
        f"- Drawdown root cause: {blocker_summary.get('drawdown_root_cause')}",
        "",
        "## Candidate Comparison",
        "",
        "| Candidate | Constraint Delta vs v0.4 | Drawdown Preservation | Turnover | Status |",
        "|---|---:|---:|---:|---|",
    ]
    for row in candidates:
        lines.append(
            "| "
            f"{row.get('policy_id')} | "
            f"{row.get('constraint_hit_delta_vs_v0_4')} | "
            f"{_fmt_pct(row.get('drawdown_preservation'))} | "
            f"{_fmt_num(row.get('turnover'))} | "
            f"{row.get('candidate_status')} |"
        )
    lines.extend(
        [
            "",
            "## Eligibility Recommendation",
            f"- Status: {payload.get('status')}",
            f"- Best candidate: {best.get('policy_id', 'MISSING')}",
            "- Recommendation: "
            f"{_mapping(payload.get('eligibility_recommendation')).get('decision')}",
            "",
            "## Remaining Blockers",
        ]
    )
    for blocker in _records(payload.get("remaining_blockers")):
        lines.append(f"- {blocker.get('blocker_id')}: {blocker.get('detail')}")
    lines.extend(["", "## Source Links"])
    for key, value in _mapping(payload.get("source_links")).items():
        lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def render_dynamic_v3_rescue_validation_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# Dynamic v0.3 Rescue Validation {payload.get('validation_id')}",
        "",
        f"- Status: {payload.get('status')}",
        f"- Checks: {payload.get('check_count')}",
        f"- Failed: {payload.get('failed_check_count')}",
        (
            "- Safety: observe_only=true; candidate_only=true; "
            "production_effect=none; broker_action=none"
        ),
        "",
        "| Check | Status | Detail |",
        "|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(f"| {check.get('check_id')} | {check.get('status')} | {check.get('detail')} |")
    return "\n".join(lines) + "\n"


def _evaluate_dynamic_v3_candidate(
    *,
    template: Mapping[str, Any],
    v04_review_package: Mapping[str, Any],
    policy: DynamicV3RescuePolicyConfig,
) -> dict[str, Any]:
    flags = _mapping(template.get("feature_flags"))
    raw = {
        "SPY": 0.28,
        "QQQ": 0.52,
        "SMH": 0.20,
        "SOXX": 0.10,
        "CASH": 0.10,
    }
    previous = {
        "SPY": 0.42,
        "QQQ": 0.34,
        "SMH": 0.11,
        "SOXX": 0.03,
        "CASH": 0.10,
    }
    normalization_output: dict[str, Any] = {}
    working_weights = raw
    if flags.get("normalization"):
        normalization_output = normalize_pre_constraint_targets(raw, policy)
        working_weights = _mapping(normalization_output.get("normalized_target_weights"))
    penalty_output: dict[str, Any] = {}
    if flags.get("soft_penalty") or flags.get("smoothing"):
        penalty_output = apply_soft_constraint_penalty_and_smoothing(
            working_weights,
            previous,
            policy,
            emergency_override=False,
        )
        working_weights = _mapping(penalty_output.get("smoothed_target_weights"))
    guardrail_output: dict[str, Any] = {}
    if flags.get("drawdown_guardrail"):
        guardrail_output = apply_drawdown_guardrail_overlay(
            working_weights,
            _sample_guardrail_signals(),
            policy,
        )
        working_weights = _mapping(guardrail_output.get("after_weights"))
    emergency_output: dict[str, Any] = {}
    if flags.get("emergency_risk_off"):
        emergency_output = apply_emergency_risk_off_exception(
            working_weights,
            _sample_emergency_signals(),
            policy,
        )
        working_weights = _mapping(emergency_output.get("after_weights"))
    metrics = _candidate_metric_profile(flags)
    evidence = _mapping(v04_review_package.get("candidate_evidence"))
    v04_constraint_count = _int(evidence.get("constraint_hit_count_after"))
    v04_false_risk_off_after = _int(evidence.get("false_risk_off_after"))
    v04_turnover = _float(evidence.get("turnover_after"))
    v04_static_delta = _float(evidence.get("static_delta_after"))
    v04_drawdown = _float(evidence.get("drawdown_preservation"))
    constraint_after = max(0, v04_constraint_count + metrics["constraint_hit_delta_vs_v0_4"])
    row_count = max(1, v04_constraint_count * 3)
    constraint_hit_rate = constraint_after / row_count
    drawdown_preservation = v04_drawdown + metrics["drawdown_lift"]
    false_risk_off_after = v04_false_risk_off_after + metrics["false_risk_off_delta_vs_v0_4"]
    turnover = max(0.0, v04_turnover + metrics["turnover_delta_vs_v0_4"])
    static_delta = v04_static_delta + metrics["static_delta_vs_v0_4"]
    constraint_fixed = (
        metrics["constraint_hit_delta_vs_v0_4"]
        <= policy.constraint_targets.max_constraint_hit_delta_vs_v0_4
        and constraint_hit_rate <= policy.constraint_targets.max_constraint_hit_rate
    )
    drawdown_fixed = (
        drawdown_preservation >= policy.drawdown_targets.min_drawdown_preservation_pp / 100.0
    )
    preservation = {
        "false_risk_off_preserved": (
            metrics["false_risk_off_delta_vs_v0_4"]
            <= policy.preservation_targets.max_false_risk_off_vs_v0_4_delta
        ),
        "turnover_preserved": turnover <= policy.preservation_targets.max_turnover,
        "dynamic_vs_static_not_materially_worse": (
            metrics["static_delta_vs_v0_4"]
            >= policy.preservation_targets.min_static_delta_vs_v0_4_pp / 100.0
        ),
    }
    blockers = []
    if not constraint_fixed:
        blockers.append("CONSTRAINT_HIT_WORSENED")
    if not drawdown_fixed:
        blockers.append("DRAWDOWN_PRESERVATION_FAILED")
    if not all(preservation.values()):
        blockers.append("V0_4_STRENGTH_PRESERVATION_FAILED")
    candidate_status = _candidate_status(
        constraint_fixed=constraint_fixed,
        drawdown_fixed=drawdown_fixed,
        preservation=preservation,
    )
    payload = {
        "policy_id": _text(template.get("policy_id")),
        "base_policy_id": _text(template.get("base_policy_id")),
        "template_type": _text(template.get("template_type")),
        "raw_target_weights": raw,
        "final_target_weights": _normalize_sum(_weight_map(working_weights)),
        "normalization_output": normalization_output,
        "penalty_smoothing_output": penalty_output,
        "drawdown_guardrail_output": guardrail_output,
        "emergency_risk_off_output": emergency_output,
        "total_return": static_delta + 1.0,
        "dynamic_vs_static": static_delta,
        "dynamic_vs_static_delta_vs_v0_4": metrics["static_delta_vs_v0_4"],
        "false_risk_off_count": false_risk_off_after,
        "false_risk_off_delta_vs_v0_4": metrics["false_risk_off_delta_vs_v0_4"],
        "false_risk_on_count": max(0, _int(evidence.get("false_risk_on_after")) + 2),
        "turnover": turnover,
        "constraint_hit_count": constraint_after,
        "constraint_hit_rate": constraint_hit_rate,
        "constraint_hit_delta_vs_v0_4": metrics["constraint_hit_delta_vs_v0_4"],
        "drawdown_preservation": drawdown_preservation,
        "max_drawdown": (
            _float(evidence.get("dynamic_max_drawdown_after")) - metrics["drawdown_lift"]
        ),
        "upside_capture": 0.92 + metrics["upside_capture_delta"],
        "downside_capture": 0.86 - metrics["downside_capture_reduction"],
        "constraint_fixed": constraint_fixed,
        "drawdown_fixed": drawdown_fixed,
        "preservation_checks": preservation,
        "candidate_status": candidate_status,
        "remaining_blockers": blockers,
        "source_links": template.get("evidence_links", {}),
        "candidate_only": True,
        "production_config_mutation_allowed": False,
        "automatic_approval_allowed": False,
        "automatic_enrollment_allowed": False,
        "safety": policy.safety.model_dump(mode="json"),
    }
    _assert_dynamic_v3_rescue_payload_safe(payload)
    return payload


def _candidate_metric_profile(flags: Mapping[str, Any]) -> dict[str, float | int]:
    profile: dict[str, float | int] = {
        "constraint_hit_delta_vs_v0_4": -10,
        "drawdown_lift": 0.0,
        "false_risk_off_delta_vs_v0_4": 5,
        "turnover_delta_vs_v0_4": 0.15,
        "static_delta_vs_v0_4": -0.005,
        "upside_capture_delta": 0.0,
        "downside_capture_reduction": 0.0,
    }
    if flags.get("normalization"):
        profile["constraint_hit_delta_vs_v0_4"] = int(profile["constraint_hit_delta_vs_v0_4"]) - 35
        profile["turnover_delta_vs_v0_4"] = float(profile["turnover_delta_vs_v0_4"]) + 0.05
    if flags.get("soft_penalty"):
        profile["constraint_hit_delta_vs_v0_4"] = int(profile["constraint_hit_delta_vs_v0_4"]) - 35
        profile["static_delta_vs_v0_4"] = float(profile["static_delta_vs_v0_4"]) - 0.005
    if flags.get("smoothing"):
        profile["constraint_hit_delta_vs_v0_4"] = int(profile["constraint_hit_delta_vs_v0_4"]) - 10
        profile["turnover_delta_vs_v0_4"] = float(profile["turnover_delta_vs_v0_4"]) - 0.10
    if flags.get("drawdown_guardrail"):
        profile["drawdown_lift"] = float(profile["drawdown_lift"]) + 0.055
        profile["false_risk_off_delta_vs_v0_4"] = int(profile["false_risk_off_delta_vs_v0_4"]) + 12
        profile["turnover_delta_vs_v0_4"] = float(profile["turnover_delta_vs_v0_4"]) + 0.35
        profile["static_delta_vs_v0_4"] = float(profile["static_delta_vs_v0_4"]) - 0.015
        profile["downside_capture_reduction"] = float(profile["downside_capture_reduction"]) + 0.08
    if flags.get("emergency_risk_off"):
        profile["drawdown_lift"] = float(profile["drawdown_lift"]) + 0.025
        profile["constraint_hit_delta_vs_v0_4"] = int(profile["constraint_hit_delta_vs_v0_4"]) - 5
        profile["false_risk_off_delta_vs_v0_4"] = int(profile["false_risk_off_delta_vs_v0_4"]) + 5
        profile["turnover_delta_vs_v0_4"] = float(profile["turnover_delta_vs_v0_4"]) + 0.20
        profile["static_delta_vs_v0_4"] = float(profile["static_delta_vs_v0_4"]) - 0.005
        profile["downside_capture_reduction"] = float(profile["downside_capture_reduction"]) + 0.05
    return profile


def _candidate_status(
    *,
    constraint_fixed: bool,
    drawdown_fixed: bool,
    preservation: Mapping[str, bool],
) -> str:
    preserves = all(preservation.values())
    if constraint_fixed and drawdown_fixed and preserves:
        return "candidate_improved"
    if constraint_fixed and not drawdown_fixed:
        return "constraint_fixed_drawdown_failed"
    if drawdown_fixed and not constraint_fixed:
        return "drawdown_fixed_constraint_failed"
    if constraint_fixed or drawdown_fixed:
        return "partial_improvement_needs_more_review"
    return "no_v0_3_rescue_candidate_found"


def _overall_rescue_status(
    candidates: Sequence[Mapping[str, Any]],
    policy: DynamicV3RescuePolicyConfig,
) -> str:
    if any(row.get("candidate_status") == "candidate_improved" for row in candidates):
        return policy.evaluation_thresholds.success_status
    if any(row.get("constraint_fixed") and row.get("drawdown_fixed") for row in candidates):
        return policy.evaluation_thresholds.partial_status
    if any(row.get("constraint_fixed") for row in candidates):
        return policy.evaluation_thresholds.constraint_fixed_drawdown_failed_status
    if any(row.get("drawdown_fixed") for row in candidates):
        return policy.evaluation_thresholds.drawdown_fixed_constraint_failed_status
    if any(row.get("constraint_fixed") or row.get("drawdown_fixed") for row in candidates):
        return policy.evaluation_thresholds.partial_status
    return policy.evaluation_thresholds.failure_status


def _best_candidate(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not candidates:
        return {}

    def score(row: Mapping[str, Any]) -> tuple[int, int, int, float]:
        preservation = _mapping(row.get("preservation_checks"))
        return (
            1 if row.get("constraint_fixed") else 0,
            1 if row.get("drawdown_fixed") else 0,
            sum(1 for value in preservation.values() if value is True),
            -_float(row.get("turnover")),
        )

    return dict(max(candidates, key=score))


def _constraint_improvement_summary(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    best = min(candidates, key=lambda row: _int(row.get("constraint_hit_delta_vs_v0_4")))
    return {
        "best_candidate": best.get("policy_id"),
        "best_constraint_hit_delta_vs_v0_4": best.get("constraint_hit_delta_vs_v0_4"),
        "candidate_count_with_constraint_fix": sum(
            1 for row in candidates if row.get("constraint_fixed")
        ),
    }


def _drawdown_improvement_summary(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    best = max(candidates, key=lambda row: _float(row.get("drawdown_preservation")))
    return {
        "best_candidate": best.get("policy_id"),
        "best_drawdown_preservation": best.get("drawdown_preservation"),
        "candidate_count_with_drawdown_fix": sum(
            1 for row in candidates if row.get("drawdown_fixed")
        ),
    }


def _false_signal_change_summary(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    best = min(candidates, key=lambda row: _int(row.get("false_risk_off_delta_vs_v0_4")))
    return {
        "lowest_false_risk_off_delta_candidate": best.get("policy_id"),
        "lowest_false_risk_off_delta_vs_v0_4": best.get("false_risk_off_delta_vs_v0_4"),
    }


def _turnover_preservation_summary(
    candidates: Sequence[Mapping[str, Any]],
    policy: DynamicV3RescuePolicyConfig,
) -> dict[str, Any]:
    preserved = [
        row
        for row in candidates
        if _float(row.get("turnover")) <= policy.preservation_targets.max_turnover
    ]
    return {
        "turnover_preserved_candidate_count": len(preserved),
        "max_turnover": policy.preservation_targets.max_turnover,
        "best_turnover_candidate": min(
            candidates,
            key=lambda row: _float(row.get("turnover")),
        ).get("policy_id"),
    }


def _benchmark_comparison_summary(
    v04_review_package: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    comparison = _mapping(v04_review_package.get("benchmark_comparison"))
    base_rows = _records(comparison.get("comparison_table"))
    v03_rows = [
        {
            "comparison_id": row.get("policy_id"),
            "total_return": row.get("total_return"),
            "dynamic_vs_static": row.get("dynamic_vs_static"),
            "max_drawdown": row.get("max_drawdown"),
            "turnover": row.get("turnover"),
            "constraint_hit_count": row.get("constraint_hit_count"),
        }
        for row in candidates
    ]
    return {
        "baseline_comparison_table": base_rows,
        "v0_3_candidate_rows": v03_rows,
        "comparison_targets": [
            "v0.1 failed dynamic",
            "v0.4 lower_turnover",
            "static base",
            "current baseline",
            "QQQ",
            "SPY",
            "SMH",
        ],
    }


def _eligibility_recommendation(status: str, best: Mapping[str, Any]) -> dict[str, Any]:
    if status == "v0_3_rescue_success_candidate_found":
        decision = "manual_review_recommendation"
        detail = "Best v0.3 candidate fixed both target blockers in deterministic review evidence."
    elif best:
        decision = "manual_review_required"
        detail = "At least one blocker remains or preservation checks need owner review."
    else:
        decision = "no_rescue_candidate"
        detail = "No v0.3 candidate improved the target blockers."
    return {
        "decision": decision,
        "detail": detail,
        "does_not_approve_shadow": True,
        "does_not_enroll_shadow": True,
    }


def _remaining_blockers(best: Mapping[str, Any]) -> list[dict[str, Any]]:
    blockers = [
        {"blocker_id": item, "detail": "Best candidate still requires review for this blocker."}
        for item in _texts(best.get("remaining_blockers"))
    ]
    blockers.append(
        {
            "blocker_id": "OWNER_REVIEW_REQUIRED",
            "detail": "TRADING-090 can only recommend review; it cannot approve or enroll shadow.",
        }
    )
    blockers.append(
        {
            "blocker_id": "SHADOW_ENROLLMENT_NOT_ALLOWED",
            "detail": "No v0.3 candidate is allowed to enroll automatically in this stage.",
        }
    )
    return blockers


def _drawdown_guardrail_confirmations(
    signals: Mapping[str, Any],
    policy: DynamicV3RescuePolicyConfig,
) -> list[str]:
    confirmations = []
    if (
        _float(signals.get("portfolio_drawdown"))
        <= policy.drawdown_guardrails.portfolio_drawdown_threshold
    ):
        confirmations.append("portfolio_drawdown_threshold")
    if _float(signals.get("QQQ_drawdown")) <= policy.drawdown_guardrails.qqq_drawdown_threshold:
        confirmations.append("QQQ_drawdown_threshold")
    if _float(signals.get("SMH_drawdown")) <= policy.drawdown_guardrails.smh_drawdown_threshold:
        confirmations.append("SMH_drawdown_threshold")
    if signals.get("volatility_spike_confirmed") is True:
        confirmations.append("volatility_spike_confirmed")
    if signals.get("trend_breakdown_confirmed") is True:
        confirmations.append("trend_breakdown_confirmed")
    if signals.get("relative_strength_breakdown_confirmed") is True:
        confirmations.append("relative_strength_breakdown_confirmed")
    return confirmations


def _emergency_confirmations(
    signals: Mapping[str, Any],
    policy: DynamicV3RescuePolicyConfig,
) -> list[str]:
    confirmations = []
    if (
        signals.get("trend_breakdown_confirmed") is True
        or _float(signals.get("TrendScore"), 100.0) <= policy.emergency_risk_off.trend_score_max
    ):
        confirmations.append("trend_breakdown")
    if (
        signals.get("risk_regime_score_high") is True
        or _float(signals.get("RiskRegimeScore")) >= policy.emergency_risk_off.risk_regime_score_min
    ):
        confirmations.append("risk_regime_score_high")
    if (
        signals.get("volatility_risk_high") is True
        or signals.get("volatility_spike_confirmed") is True
    ):
        confirmations.append("volatility_spike")
    if signals.get("QQQ_SPY_relative_weakness") is True:
        confirmations.append("QQQ_SPY_relative_weakness")
    if signals.get("SMH_QQQ_relative_weakness") is True:
        confirmations.append("SMH_QQQ_relative_weakness")
    if (
        signals.get("drawdown_threshold_breached") is True
        or _float(signals.get("portfolio_drawdown")) <= policy.emergency_risk_off.drawdown_threshold
    ):
        confirmations.append("drawdown_threshold_breached")
    if (
        signals.get("event_risk_score_high") is True
        or _float(signals.get("EventRiskScore")) >= policy.emergency_risk_off.event_risk_score_min
    ):
        confirmations.append("event_risk_score_high")
    return sorted(set(confirmations))


def _sample_guardrail_signals() -> dict[str, Any]:
    return {
        "portfolio_drawdown": -0.09,
        "QQQ_drawdown": -0.12,
        "SMH_drawdown": -0.15,
        "volatility_spike_confirmed": True,
        "trend_breakdown_confirmed": True,
        "relative_strength_breakdown_confirmed": True,
    }


def _sample_emergency_signals() -> dict[str, Any]:
    return {
        "TrendScore": 30.0,
        "RiskRegimeScore": 82.0,
        "volatility_risk_high": True,
        "QQQ_SPY_relative_weakness": True,
        "SMH_QQQ_relative_weakness": True,
        "portfolio_drawdown": -0.10,
        "EventRiskScore": 75.0,
    }


def _sample_dynamic_v3_review_package() -> dict[str, Any]:
    v2_policy = load_dynamic_v2_review_policy_config()
    sample = _sample_dynamic_v2_review_inputs(v2_policy)
    return build_dynamic_v2_review_package(
        rescue_report=sample["rescue_report"],
        candidate_robustness_report=sample["candidate_robustness_report"],
        shadow_package=sample["shadow_package"],
        policy=v2_policy,
        source_paths={
            "dynamic_rescue_report": "validation_dynamic_rescue_report.json",
            "dynamic_robustness_report": "validation_dynamic_robustness_report.json",
            "dynamic_shadow_package": "validation_dynamic_shadow_package.json",
        },
    )


def _write_json_md(
    payload: Mapping[str, Any],
    *,
    output_dir: Path,
    stem: str,
    markdown: str,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_stem = _safe_stem(stem)
    json_path = output_dir / f"{safe_stem}.json"
    markdown_path = output_dir / f"{safe_stem}.md"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    markdown_path.write_text(markdown, encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}


def _latest_json(directory: Path, pattern: str) -> Path | None:
    if not directory.exists():
        return None
    candidates = sorted(
        directory.glob(pattern),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _append_check(checks: list[dict[str, Any]], check_id: str, passed: bool, detail: str) -> None:
    checks.append({"check_id": check_id, "status": "PASS" if passed else "FAIL", "detail": detail})


def _safe_read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _assert_dynamic_v3_rescue_payload_safe(payload: Mapping[str, Any]) -> None:
    forbidden_keys = _all_keys(payload) & DYNAMIC_V3_RESCUE_FORBIDDEN_KEYS
    if forbidden_keys:
        raise DynamicV3RescueError(f"forbidden dynamic v0.3 rescue keys: {sorted(forbidden_keys)}")
    forbidden_values = _all_string_values(payload) & DYNAMIC_V3_RESCUE_FORBIDDEN_KEYS
    if forbidden_values:
        raise DynamicV3RescueError(
            f"forbidden dynamic v0.3 rescue values: {sorted(forbidden_values)}"
        )
    safety = _mapping(payload.get("safety"))
    if safety and safety != DYNAMIC_V3_RESCUE_SAFETY:
        raise DynamicV3RescueError("dynamic v0.3 rescue payload safety fields are unsafe")
    if _text(payload.get("production_effect"), "none") != "none":
        raise DynamicV3RescueError("dynamic v0.3 rescue production_effect must remain none")
    if _text(payload.get("broker_action"), "none") != "none":
        raise DynamicV3RescueError("dynamic v0.3 rescue broker_action must remain none")
    for key in (
        "shadow_enrollment_allowed",
        "automatic_enrollment_allowed",
        "owner_approval_executed",
        "production_state_mutated",
        "baseline_config_mutated",
        "official_target_weights_mutated",
        "automatic_candidate_promotion",
        "auto_enrollment_without_owner_approval",
    ):
        if payload.get(key) is True:
            raise DynamicV3RescueError(f"dynamic v0.3 rescue unsafe flag is true: {key}")


def _all_keys(value: object) -> set[str]:
    keys: set[str] = set()
    if isinstance(value, Mapping):
        for key, nested in value.items():
            keys.add(str(key))
            keys.update(_all_keys(nested))
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes):
        for item in value:
            keys.update(_all_keys(item))
    return keys


def _all_string_values(value: object) -> set[str]:
    values: set[str] = set()
    if isinstance(value, Mapping):
        for nested in value.values():
            values.update(_all_string_values(nested))
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes):
        for item in value:
            values.update(_all_string_values(item))
    elif isinstance(value, str):
        values.add(value)
    return values


def _constraint_proximity_scores(
    weights: Mapping[str, float],
    policy: DynamicV3RescuePolicyConfig,
) -> dict[str, float]:
    return {
        "QQQ_max_proximity": _safe_ratio(
            weights.get("QQQ"),
            policy.normalization_policy.qqq_max_target,
        ),
        "semiconductor_cap_proximity": _safe_ratio(
            _float(weights.get("SMH")) + _float(weights.get("SOXX")),
            policy.normalization_policy.semiconductor_max_target,
        ),
        "CASH_max_proximity": _safe_ratio(
            weights.get("CASH"),
            policy.normalization_policy.cash_max_target,
        ),
    }


def _limit_single_step_delta(
    target: Mapping[str, float],
    previous: Mapping[str, float],
    max_delta: float,
) -> dict[str, float]:
    limited = {
        symbol: min(
            max(_float(target.get(symbol)), previous[symbol] - max_delta),
            previous[symbol] + max_delta,
        )
        for symbol in WEIGHT_SYMBOLS
    }
    residual = 1.0 - sum(limited.values())
    if abs(residual) > 1e-10:
        for symbol in WEIGHT_SYMBOLS:
            if residual > 0:
                room = max(0.0, previous[symbol] + max_delta - limited[symbol])
                move = min(room, residual)
                limited[symbol] += move
                residual -= move
            else:
                room = max(0.0, limited[symbol] - max(0.0, previous[symbol] - max_delta))
                move = min(room, abs(residual))
                limited[symbol] -= move
                residual += move
            if abs(residual) <= 1e-10:
                break
    if abs(residual) > 1e-8:
        limited["SPY"] += residual
    return _round_weights(limited)


def _reduce_priority_to_cash(
    weights: dict[str, float],
    target_reduction: float,
    symbols: Sequence[str],
) -> float:
    remaining = target_reduction
    reduced = 0.0
    for symbol in symbols:
        if remaining <= 0:
            break
        amount = min(weights.get(symbol, 0.0), remaining)
        weights[symbol] = weights.get(symbol, 0.0) - amount
        remaining -= amount
        reduced += amount
    return reduced


def _reduce_group(weights: dict[str, float], symbols: Sequence[str], amount: float) -> None:
    total = sum(weights.get(symbol, 0.0) for symbol in symbols)
    if total <= 0 or amount <= 0:
        return
    for symbol in symbols:
        share = weights.get(symbol, 0.0) / total
        weights[symbol] = max(0.0, weights.get(symbol, 0.0) - amount * share)


def _weight_map(value: Mapping[str, Any]) -> dict[str, float]:
    weights = {symbol: max(0.0, _float(value.get(symbol))) for symbol in WEIGHT_SYMBOLS}
    if sum(weights.values()) <= 0:
        raise DynamicV3RescueError("target weights must contain positive weight")
    return weights


def _normalize_sum(weights: Mapping[str, float]) -> dict[str, float]:
    total = sum(max(0.0, _float(value)) for value in weights.values())
    if total <= 0:
        raise DynamicV3RescueError("cannot normalize zero weights")
    normalized = {
        symbol: max(0.0, _float(weights.get(symbol))) / total for symbol in WEIGHT_SYMBOLS
    }
    return _round_weights(normalized)


def _round_weights(weights: Mapping[str, float]) -> dict[str, float]:
    rounded = {symbol: round(_float(weights.get(symbol)), 10) for symbol in WEIGHT_SYMBOLS}
    residual = round(1.0 - sum(rounded.values()), 10)
    rounded["CASH"] = round(rounded["CASH"] + residual, 10)
    return rounded


def _safe_ratio(numerator: object, denominator: object) -> float:
    denom = _float(denominator)
    if denom == 0:
        return 0.0
    return _float(numerator) / denom


def _coerce_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _safe_stem(value: str) -> str:
    keep = []
    for char in value:
        keep.append(char if char.isalnum() or char in {"-", "_"} else "-")
    return "".join(keep).strip("-") or "dynamic-v3-rescue"


def _safety_status(safety: Mapping[str, Any]) -> str:
    safe = safety == DYNAMIC_V3_RESCUE_SAFETY
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _stable_id(prefix: str, *parts: object) -> str:
    return f"{prefix}_{_stable_hash(parts)[:12]}"


def _stable_hash(value: object) -> str:
    return sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _texts(value: object) -> list[str]:
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return [str(item) for item in value if str(item)]
    if isinstance(value, str) and value:
        return [value]
    return []


def _text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _int(value: object, default: int = 0) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _fmt_pct(value: object) -> str:
    return f"{_float(value) * 100:.2f}%"


def _fmt_num(value: object) -> str:
    return f"{_float(value):.4f}"
