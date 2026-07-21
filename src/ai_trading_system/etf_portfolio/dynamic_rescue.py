from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from functools import lru_cache
from hashlib import sha256
from math import prod
from pathlib import Path
from statistics import mean
from typing import Any, Literal, Self

import pandas as pd
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import PRIMARY_RESEARCH_START_DATE
from ai_trading_system.etf_portfolio.dynamic_allocation import (
    DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    DynamicAllocationPolicyConfig,
    load_dynamic_allocation_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_robustness import (
    DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
    DynamicRobustnessPolicyConfig,
    _synthetic_validation_prices,
    build_dynamic_robustness_report,
    latest_dynamic_robustness_report_path,
    load_dynamic_robustness_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_shadow import (
    DEFAULT_DYNAMIC_SHADOW_PACKAGE_DIR,
    latest_dynamic_shadow_review_package_path,
)
from ai_trading_system.etf_portfolio.models import ETFConfigBundle, PolicyMetadata
from ai_trading_system.yaml_loader import safe_load_yaml_path

DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_SCHEMA_VERSION = "etf_dynamic_failure_diagnostics_policy_v1"
DYNAMIC_FAILURE_DATASET_SCHEMA_VERSION = "etf_dynamic_failure_dataset_v1"
DYNAMIC_RESCUE_REPORT_SCHEMA_VERSION = "etf_dynamic_rescue_evaluation_report_v1"
DYNAMIC_RESCUE_VALIDATION_SCHEMA_VERSION = "etf_dynamic_rescue_validation_v1"

DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_failure_diagnostics.yaml"
)
DEFAULT_DYNAMIC_RESCUE_ROOT = PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_rescue"
DEFAULT_DYNAMIC_RESCUE_DATASET_DIR = DEFAULT_DYNAMIC_RESCUE_ROOT / "datasets"
DEFAULT_DYNAMIC_RESCUE_REPORT_DIR = DEFAULT_DYNAMIC_RESCUE_ROOT / "reports"
DEFAULT_DYNAMIC_RESCUE_VALIDATION_DIR = DEFAULT_DYNAMIC_RESCUE_ROOT / "validation"

DYNAMIC_RESCUE_SAFETY: dict[str, Any] = {
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
FORBIDDEN_DYNAMIC_RESCUE_KEYS = {
    "broker_order",
    "production_weight_update",
    "baseline_config_mutation",
    "official_target_weights_write",
    "apply_to_production",
    "promote_to_baseline",
    "place_order",
    "enable_broker_action",
    "automatic_approval",
    "auto_approval",
    "auto_enrollment",
}
WEIGHT_SYMBOLS = ("SPY", "QQQ", "SMH", "SOXX", "CASH")


class DynamicRescueError(RuntimeError):
    """Raised when TRADING-088 dynamic rescue inputs or outputs are invalid."""


class DynamicRescueMarketRegime(BaseModel):
    regime_id: Literal["unified_primary_2021"]
    anchor_event: str = Field(min_length=1)
    anchor_date: date
    default_backtest_start: date

    @model_validator(mode="after")
    def validate_ai_regime_start(self) -> Self:
        if self.default_backtest_start < PRIMARY_RESEARCH_START_DATE:
            raise ValueError("dynamic rescue default backtest start cannot predate 2021-02-22")
        return self


class DynamicRescueSafety(BaseModel):
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
        if self.model_dump(mode="json") != DYNAMIC_RESCUE_SAFETY:
            raise ValueError("dynamic rescue safety fields are unsafe")
        return self


class DynamicFailureThresholds(BaseModel):
    max_underperformance_vs_static_pp: float
    max_false_risk_off_count: int = Field(ge=0)
    max_false_on_count: int = Field(ge=0)
    max_turnover: float = Field(ge=0)
    max_constraint_hit_rate: float = Field(ge=0, le=1)
    min_trend_signal_quality_score: float = Field(ge=0, le=1)
    max_signal_redundancy_count: int = Field(ge=0)


class DynamicFalseSignalDefinitions(BaseModel):
    false_risk_off_forward_window_days: int = Field(gt=1)
    false_risk_off_min_market_return: float
    false_risk_on_forward_drawdown_threshold: float = Field(le=0)
    cash_overweight_threshold: float = Field(ge=0, le=1)
    growth_underweight_threshold: float = Field(le=0)
    growth_overweight_threshold: float = Field(ge=0, le=1)
    event_risk_trigger_score: float = Field(ge=0, le=100)


class DynamicSignalBucketThresholds(BaseModel):
    low_score_max: float = Field(ge=0, le=100)
    high_score_min: float = Field(ge=0, le=100)
    weak_score_max: float = Field(ge=0, le=100)
    strong_score_min: float = Field(ge=0, le=100)
    event_risk_high_min: float = Field(ge=0, le=100)
    redundancy_warning_count: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_order(self) -> Self:
        if self.low_score_max >= self.high_score_min:
            raise ValueError("low_score_max must be below high_score_min")
        if self.weak_score_max >= self.strong_score_min:
            raise ValueError("weak_score_max must be below strong_score_min")
        return self


class DynamicTurnoverThresholds(BaseModel):
    max_turnover: float = Field(ge=0)
    high_regime_switch_count: int = Field(ge=0)
    high_single_row_turnover: float = Field(ge=0, le=1)


class DynamicConstraintHitThresholds(BaseModel):
    max_constraint_hit_rate: float = Field(ge=0, le=1)
    high_constraint_hit_count: int = Field(ge=0)
    top_period_count: int = Field(gt=0)


class DynamicAllocationTradeoffConfig(BaseModel):
    drawdown_improvement_weight: float = Field(ge=0)


class DynamicImprovementRequirements(BaseModel):
    min_return_vs_static_delta_improvement: float
    min_false_risk_off_reduction_count: int = Field(ge=0)
    min_turnover_reduction: float = Field(ge=0)
    min_constraint_hit_reduction_count: int = Field(ge=0)
    max_drawdown_degradation: float = Field(ge=0)


class DynamicRescueTemplateConstraints(BaseModel):
    candidate_only: Literal[True]
    production_config_mutation_allowed: Literal[False]
    automatic_approval_allowed: Literal[False]
    automatic_enrollment_allowed: Literal[False]


class DynamicRescueTemplateChanges(BaseModel):
    description: str = Field(min_length=1)
    target_weight_overrides: dict[str, dict[str, float]] = Field(default_factory=dict)
    event_risk_overlay_overrides: dict[str, Any] = Field(default_factory=dict)
    rebalance_policy_overrides: dict[str, float | int] = Field(default_factory=dict)
    trend_overlay_scale: float | None = Field(default=None, ge=0, le=1)
    regime_rule_overrides: dict[str, dict[str, float]] = Field(default_factory=dict)


class DynamicRescuePolicyTemplate(BaseModel):
    policy_id: str = Field(min_length=1)
    base_policy_id: str = Field(min_length=1)
    template_type: str = Field(min_length=1)
    expected_failure_target: list[str] = Field(min_length=1)
    changes_from_v0_1: DynamicRescueTemplateChanges
    constraints: DynamicRescueTemplateConstraints

    @model_validator(mode="after")
    def validate_template(self) -> Self:
        if not self.policy_id.startswith("dynamic_regime_overlay_v0_"):
            raise ValueError("dynamic rescue template policy_id must be versioned")
        return self


class DynamicFailureDiagnosticsPolicyConfig(BaseModel):
    schema_version: Literal["etf_dynamic_failure_diagnostics_policy_v1"]
    policy_metadata: PolicyMetadata
    market_regime: DynamicRescueMarketRegime
    failure_thresholds: DynamicFailureThresholds
    false_signal_definitions: DynamicFalseSignalDefinitions
    signal_bucket_thresholds: DynamicSignalBucketThresholds
    turnover_thresholds: DynamicTurnoverThresholds
    constraint_hit_thresholds: DynamicConstraintHitThresholds
    allocation_tradeoff: DynamicAllocationTradeoffConfig
    improvement_requirements: DynamicImprovementRequirements
    rescue_policy_templates: list[DynamicRescuePolicyTemplate] = Field(min_length=1)
    safety: DynamicRescueSafety

    @model_validator(mode="after")
    def validate_policy(self) -> Self:
        template_ids = [template.policy_id for template in self.rescue_policy_templates]
        if len(template_ids) != len(set(template_ids)):
            raise ValueError("dynamic rescue template ids must be unique")
        return self


def load_dynamic_failure_diagnostics_policy_config(
    path: Path = DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH,
) -> DynamicFailureDiagnosticsPolicyConfig:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise DynamicRescueError("dynamic failure diagnostics policy must be a mapping")
    try:
        return DynamicFailureDiagnosticsPolicyConfig.model_validate(raw)
    except Exception as exc:  # noqa: BLE001
        raise DynamicRescueError(f"invalid dynamic failure diagnostics policy: {exc}") from exc


def build_dynamic_failure_dataset(
    *,
    robustness_report: Mapping[str, Any],
    policy: DynamicFailureDiagnosticsPolicyConfig,
    shadow_review_package: Mapping[str, Any] | None = None,
    trend_report: Mapping[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    rows = _failure_dataset_rows(robustness_report=robustness_report, policy=policy)
    summary = _mapping(robustness_report.get("summary"))
    candidate_context = _mapping(robustness_report.get("candidate_context"))
    dataset_id = _stable_id(
        "dynamic-failure-dataset",
        _text(summary.get("dynamic_candidate_id"), "unknown_candidate"),
        _stable_hash([row["date"] for row in rows]),
    )
    payload = {
        "schema_version": DYNAMIC_FAILURE_DATASET_SCHEMA_VERSION,
        "report_type": "etf_dynamic_failure_dataset",
        "dataset_id": dataset_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if rows else "EMPTY",
        "market_regime": policy.market_regime.model_dump(mode="json"),
        "candidate_id": _text(summary.get("dynamic_candidate_id"), "MISSING"),
        "trend_signal_config_id": _text(candidate_context.get("trend_signal_config_id")),
        "dynamic_policy_id": _text(robustness_report.get("dynamic_allocation_policy_id")),
        "row_count": len(rows),
        "false_risk_off_count": sum(1 for row in rows if row["false_risk_off"]),
        "false_risk_on_count": sum(1 for row in rows if row["false_risk_on"]),
        "data_quality_status": _text(summary.get("data_quality_status"), "UNKNOWN"),
        "source_artifacts": {
            "dynamic_robustness_report": _text(
                robustness_report.get("dynamic_robustness_report_id")
            ),
            "dynamic_shadow_package": _text(
                _mapping(shadow_review_package).get("review_package_id")
            ),
            "trend_report": _text(_mapping(trend_report).get("trend_calibration_report_id")),
        },
        "rows": rows,
        "safety": policy.safety.model_dump(mode="json"),
        **DYNAMIC_RESCUE_SAFETY,
        "commands_executed": False,
        "shadow_enrollment_allowed": False,
        "automatic_enrollment_allowed": False,
        "owner_approval_executed": False,
    }
    _assert_dynamic_rescue_payload_safe(payload)
    return payload


def build_layer1_signal_failure_attribution(
    *,
    dataset: Mapping[str, Any],
    robustness_report: Mapping[str, Any],
    policy: DynamicFailureDiagnosticsPolicyConfig,
    trend_report: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    rows = _records(dataset.get("rows"))
    buckets = _trend_score_buckets(rows, policy)
    low_returns = [row["forward_return_dynamic"] for row in buckets["low"]]
    high_returns = [row["forward_return_dynamic"] for row in buckets["high"]]
    low_drawdowns = [row["forward_drawdown_dynamic"] for row in buckets["low"]]
    high_drawdowns = [row["forward_drawdown_dynamic"] for row in buckets["high"]]
    quality_score = _first_numeric_by_key(
        [trend_report or {}, robustness_report],
        ("trend_signal_quality_score", "quality_score"),
    )
    return_lift = _avg(high_returns) - _avg(low_returns)
    drawdown_lift = _avg(high_drawdowns) - _avg(low_drawdowns)
    false_off_contributors = _signal_contributors(rows, policy, flag="false_risk_off")
    false_on_contributors = _signal_contributors(rows, policy, flag="false_risk_on")
    redundancy_count = int(
        _first_numeric_by_key(
            [trend_report or {}, robustness_report],
            ("high_redundancy_count", "redundancy_count"),
            default=0.0,
        )
    )
    weak_signals = [
        item["signal_id"]
        for item in false_off_contributors + false_on_contributors
        if item["event_count"] > 0
    ]
    useful_signals = [
        (
            "CompositeTrendScore"
            if return_lift > 0
            else "RiskRegimeScore" if drawdown_lift >= 0 else ""
        )
    ]
    useful_signals = [item for item in useful_signals if item]
    recommendations = _layer1_recommendations(
        quality_score=quality_score,
        return_lift=return_lift,
        redundancy_count=redundancy_count,
        policy=policy,
    )
    return {
        "trend_signal_config_id": _text(dataset.get("trend_signal_config_id"), "MISSING"),
        "quality_score": quality_score,
        "return_lift": return_lift,
        "drawdown_lift": drawdown_lift,
        "false_risk_off_contributors": false_off_contributors,
        "false_risk_on_contributors": false_on_contributors,
        "redundancy_count": redundancy_count,
        "weak_signals": weak_signals,
        "useful_signals": useful_signals,
        "recommended_signal_weight_adjustments": recommendations,
        "candidate_only": True,
        "safety": policy.safety.model_dump(mode="json"),
    }


def build_false_signal_attribution(
    *,
    dataset: Mapping[str, Any],
    policy: DynamicFailureDiagnosticsPolicyConfig,
) -> dict[str, Any]:
    rows = _records(dataset.get("rows"))
    false_off = [row for row in rows if row.get("false_risk_off") is True]
    false_on = [row for row in rows if row.get("false_risk_on") is True]
    by_regime = _false_signal_group(rows, "regime_state")
    by_bucket = _false_signal_score_buckets(rows, policy)
    by_trigger = _false_signal_by_trigger(rows, policy)
    top_periods = sorted(
        [
            {
                "date": row.get("date"),
                "regime_state": row.get("regime_state"),
                "false_risk_off": row.get("false_risk_off"),
                "false_risk_on": row.get("false_risk_on"),
                "opportunity_cost": row.get("false_risk_off_opportunity_cost"),
                "drawdown_cost": row.get("false_risk_on_drawdown_cost"),
                "triggers": _false_signal_triggers(row, policy),
            }
            for row in false_off + false_on
        ],
        key=lambda item: _float(item.get("opportunity_cost")) + _float(item.get("drawdown_cost")),
        reverse=True,
    )
    return {
        "false_risk_off_count": len(false_off),
        "false_risk_on_count": len(false_on),
        "false_risk_off_opportunity_cost": sum(
            _float(row.get("false_risk_off_opportunity_cost")) for row in false_off
        ),
        "false_risk_on_drawdown_cost": sum(
            _float(row.get("false_risk_on_drawdown_cost")) for row in false_on
        ),
        "false_signal_by_regime": by_regime,
        "false_signal_by_score_bucket": by_bucket,
        "false_signal_by_trigger": by_trigger,
        "top_false_signal_periods": top_periods[
            : policy.constraint_hit_thresholds.top_period_count
        ],
        "safety": policy.safety.model_dump(mode="json"),
    }


def build_layer2_allocation_underperformance_attribution(
    *,
    dataset: Mapping[str, Any],
    robustness_report: Mapping[str, Any],
    policy: DynamicFailureDiagnosticsPolicyConfig,
) -> dict[str, Any]:
    rows = _records(dataset.get("rows"))
    dynamic_row = _comparison_row(robustness_report, "dynamic_candidate")
    static_row = _comparison_row(robustness_report, "static_base_candidate")
    total_underperformance = _float(dynamic_row.get("total_return")) - _float(
        static_row.get("total_return")
    )
    cash_drag = sum(_cash_drag(row) for row in rows)
    growth_underweight_cost = sum(_growth_underweight_cost(row) for row in rows)
    semiconductor_underweight_cost = sum(_semiconductor_underweight_cost(row) for row in rows)
    spy_overweight_effect = sum(_spy_overweight_effect(row) for row in rows)
    drawdown_improvement = abs(_float(static_row.get("max_drawdown"))) - abs(
        _float(dynamic_row.get("max_drawdown"))
    )
    net_tradeoff = (
        total_underperformance
        + drawdown_improvement * policy.allocation_tradeoff.drawdown_improvement_weight
    )
    reasons = _allocation_failure_reasons(
        rows=rows,
        total_underperformance=total_underperformance,
        robustness_report=robustness_report,
        policy=policy,
    )
    return {
        "candidate_id": _text(dataset.get("candidate_id"), "MISSING"),
        "total_underperformance_vs_static": total_underperformance,
        "cash_drag": cash_drag,
        "growth_underweight_cost": growth_underweight_cost,
        "semiconductor_underweight_cost": semiconductor_underweight_cost,
        "SPY_overweight_effect": spy_overweight_effect,
        "drawdown_improvement_value": drawdown_improvement,
        "net_risk_adjusted_tradeoff": net_tradeoff,
        "allocation_failure_reasons": reasons,
        "safety": policy.safety.model_dump(mode="json"),
    }


def build_turnover_constraint_breakdown(
    *,
    dataset: Mapping[str, Any],
    policy: DynamicFailureDiagnosticsPolicyConfig,
) -> dict[str, Any]:
    rows = _records(dataset.get("rows"))
    turnover_by_source: dict[str, float] = {
        "turnover_from_regime_switch": 0.0,
        "turnover_from_trend_overlay": 0.0,
        "turnover_from_event_risk_overlay": 0.0,
        "turnover_from_constraint_correction": 0.0,
        "turnover_from_rebalance_threshold": 0.0,
        "turnover_from_other": 0.0,
    }
    previous_regime = ""
    constraint_hits_by_type: dict[str, int] = {}
    top_periods: list[dict[str, Any]] = []
    constraint_hit_count = 0
    for row in rows:
        turnover = _float(row.get("turnover"))
        sources = _turnover_sources(row, previous_regime)
        previous_regime = _text(row.get("regime_state"))
        share = turnover / len(sources)
        for source in sources:
            turnover_by_source[source] = turnover_by_source.get(source, 0.0) + share
        constraints = [str(item) for item in row.get("constraint_hits", [])]
        if constraints:
            constraint_hit_count += 1
        for constraint in constraints:
            key = _constraint_type(constraint)
            constraint_hits_by_type[key] = constraint_hits_by_type.get(key, 0) + 1
        top_periods.append(
            {
                "date": row.get("date"),
                "turnover": turnover,
                "constraint_hits": constraints,
                "regime_state": row.get("regime_state"),
            }
        )
    row_count = len(rows)
    constraint_hit_rate = 0.0 if row_count == 0 else constraint_hit_count / row_count
    warnings = []
    if sum(_float(row.get("turnover")) for row in rows) > policy.turnover_thresholds.max_turnover:
        warnings.append("HIGH_TURNOVER")
    if constraint_hit_rate > policy.constraint_hit_thresholds.max_constraint_hit_rate:
        warnings.append("HIGH_CONSTRAINT_HIT_RATE")
    recommendations = _turnover_constraint_recommendations(
        turnover_by_source=turnover_by_source,
        constraint_hit_rate=constraint_hit_rate,
        warnings=warnings,
    )
    return {
        "turnover_total": sum(_float(row.get("turnover")) for row in rows),
        "turnover_by_source": turnover_by_source,
        "constraint_hit_count": constraint_hit_count,
        "constraint_hit_rate": constraint_hit_rate,
        "constraint_hits_by_type": constraint_hits_by_type,
        "top_constraint_hit_periods": sorted(
            top_periods,
            key=lambda item: len(item["constraint_hits"]) + _float(item["turnover"]),
            reverse=True,
        )[: policy.constraint_hit_thresholds.top_period_count],
        "warnings": warnings,
        "recommended_constraint_adjustments": recommendations,
        "safety": policy.safety.model_dump(mode="json"),
    }


def build_rescue_policy_templates(
    *,
    policy: DynamicFailureDiagnosticsPolicyConfig,
    evidence: Mapping[str, Any],
) -> list[dict[str, Any]]:
    templates: list[dict[str, Any]] = []
    evidence_links = _evidence_links(evidence)
    for template in policy.rescue_policy_templates:
        templates.append(
            {
                "policy_id": template.policy_id,
                "base_policy_id": template.base_policy_id,
                "template_type": template.template_type,
                "changes_from_v0_1": template.changes_from_v0_1.model_dump(mode="json"),
                "expected_failure_target": template.expected_failure_target,
                "evidence_links": [
                    link
                    for link in evidence_links
                    if link["reason_code"] in set(template.expected_failure_target)
                ],
                "constraints": template.constraints.model_dump(mode="json"),
                "safety": policy.safety.model_dump(mode="json"),
            }
        )
    return templates


def apply_dynamic_rescue_template(
    dynamic_policy: DynamicAllocationPolicyConfig,
    template: DynamicRescuePolicyTemplate | Mapping[str, Any],
) -> DynamicAllocationPolicyConfig:
    if isinstance(template, DynamicRescuePolicyTemplate):
        resolved_template = template
    else:
        resolved_template = DynamicRescuePolicyTemplate.model_validate(template)
    candidate = dynamic_policy.model_copy(deep=True)
    candidate.default_policy_id = resolved_template.policy_id
    candidate.policy_metadata.version = resolved_template.policy_id
    candidate.policy_metadata.status = "candidate_only_rescue_template"
    candidate.policy_metadata.rationale = resolved_template.changes_from_v0_1.description
    changes = resolved_template.changes_from_v0_1
    for regime_id, weights in changes.target_weight_overrides.items():
        if regime_id not in candidate.regime_weight_targets:
            raise DynamicRescueError(f"unknown regime override: {regime_id}")
        candidate.regime_weight_targets[regime_id].weights = _normalise_weights(weights)
    for field_name, value in changes.rebalance_policy_overrides.items():
        if not hasattr(candidate.rebalance_policy, field_name):
            raise DynamicRescueError(f"unknown rebalance override: {field_name}")
        setattr(candidate.rebalance_policy, field_name, value)
    if changes.event_risk_overlay_overrides:
        for field_name, value in changes.event_risk_overlay_overrides.items():
            if field_name == "reductions":
                candidate.event_risk_overlay.reductions = {
                    str(symbol): float(reduction) for symbol, reduction in _mapping(value).items()
                }
            elif hasattr(candidate.event_risk_overlay, field_name):
                setattr(candidate.event_risk_overlay, field_name, value)
            else:
                raise DynamicRescueError(f"unknown event risk override: {field_name}")
    if changes.trend_overlay_scale is not None:
        for rule in candidate.trend_overlay_rules:
            rule.adjustments = {
                symbol: round(delta * changes.trend_overlay_scale, 10)
                for symbol, delta in rule.adjustments.items()
            }
    for regime_id, overrides in changes.regime_rule_overrides.items():
        if regime_id not in candidate.regime_selection_rules:
            raise DynamicRescueError(f"unknown regime rule override: {regime_id}")
        rule = candidate.regime_selection_rules[regime_id]
        for field_name, value in overrides.items():
            if not hasattr(rule, field_name):
                raise DynamicRescueError(f"unknown regime rule override: {field_name}")
            setattr(rule, field_name, value)
    try:
        return DynamicAllocationPolicyConfig.model_validate(candidate.model_dump(mode="json"))
    except Exception as exc:  # noqa: BLE001
        raise DynamicRescueError(
            f"rescue template produced invalid dynamic allocation policy: {exc}"
        ) from exc


def build_dynamic_rescue_batch_report(
    *,
    prices: pd.DataFrame,
    etf_config: ETFConfigBundle,
    policy: DynamicFailureDiagnosticsPolicyConfig,
    dynamic_robustness_policy: DynamicRobustnessPolicyConfig,
    dynamic_policy: DynamicAllocationPolicyConfig,
    failed_robustness_report: Mapping[str, Any] | None = None,
    trend_report: Mapping[str, Any] | None = None,
    shadow_review_package: Mapping[str, Any] | None = None,
    candidate_id: str | None = None,
    start: date | None = None,
    end: date | None = None,
    data_quality_status: str = "UNKNOWN",
    data_quality_report: str = "",
    prices_path: Path | None = None,
) -> dict[str, Any]:
    failed_report = dict(failed_robustness_report or {})
    if not failed_report:
        failed_report = build_dynamic_robustness_report(
            prices=prices,
            etf_config=etf_config,
            policy=dynamic_robustness_policy,
            dynamic_policy=dynamic_policy,
            candidate_id=candidate_id,
            start=start,
            end=end,
            data_quality_status=data_quality_status,
            data_quality_report=data_quality_report,
            prices_path=prices_path,
        )
    dataset = build_dynamic_failure_dataset(
        robustness_report=failed_report,
        policy=policy,
        shadow_review_package=shadow_review_package,
        trend_report=trend_report,
    )
    layer1 = build_layer1_signal_failure_attribution(
        dataset=dataset,
        robustness_report=failed_report,
        policy=policy,
        trend_report=trend_report,
    )
    false_signal = build_false_signal_attribution(dataset=dataset, policy=policy)
    layer2 = build_layer2_allocation_underperformance_attribution(
        dataset=dataset,
        robustness_report=failed_report,
        policy=policy,
    )
    turnover_constraint = build_turnover_constraint_breakdown(dataset=dataset, policy=policy)
    evidence = {
        "layer1_signal_failure": layer1,
        "false_signal_attribution": false_signal,
        "layer2_allocation_failure": layer2,
        "turnover_constraint_breakdown": turnover_constraint,
    }
    templates = build_rescue_policy_templates(policy=policy, evidence=evidence)
    rescue_results: list[dict[str, Any]] = []
    base_summary = _mapping(failed_report.get("summary"))
    base_candidate = _text(
        candidate_id or base_summary.get("dynamic_candidate_id"), "dynamic_candidate"
    )
    for template_config in policy.rescue_policy_templates:
        template_policy = apply_dynamic_rescue_template(dynamic_policy, template_config)
        rescue_report = build_dynamic_robustness_report(
            prices=prices,
            etf_config=etf_config,
            policy=dynamic_robustness_policy,
            dynamic_policy=template_policy,
            candidate_id=f"{base_candidate}:{template_config.policy_id}",
            start=start,
            end=end,
            data_quality_status=data_quality_status,
            data_quality_report=data_quality_report,
            prices_path=prices_path,
        )
        rescue_results.append(
            _rescue_candidate_comparison(
                template=template_config,
                failed_report=failed_report,
                rescue_report=rescue_report,
                policy=policy,
            )
        )
    best = _best_rescue_candidate(rescue_results)
    status = _rescue_status(best, rescue_results, layer1, policy)
    generated = datetime.now(UTC)
    report_id = _stable_id(
        "dynamic-rescue-report",
        base_candidate,
        _text(failed_report.get("dynamic_robustness_report_id")),
        _stable_hash([row["policy_id"] for row in rescue_results]),
    )
    payload = {
        "schema_version": DYNAMIC_RESCUE_REPORT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_rescue_evaluation_report",
        "dynamic_rescue_report_id": report_id,
        "generated_at": generated.isoformat(),
        "status": status,
        "policy_version": policy.policy_metadata.version,
        "policy_config_hash": _stable_hash(policy.model_dump(mode="json")),
        "market_regime": policy.market_regime.model_dump(mode="json"),
        "safety_banner": _safety_status(policy.safety.model_dump(mode="json")),
        "failed_v0_1_summary": _failed_v0_1_summary(failed_report, policy),
        "failure_dataset": dataset,
        "layer1_signal_failure_attribution": layer1,
        "false_signal_attribution": false_signal,
        "layer2_allocation_underperformance_attribution": layer2,
        "turnover_constraint_breakdown": turnover_constraint,
        "rescue_policy_templates": templates,
        "rescue_candidate_comparison": rescue_results,
        "improvement_summary": _improvement_summary(best, rescue_results),
        "best_rescue_candidate": best,
        "remaining_blockers": _remaining_blockers(status, layer1, layer2, turnover_constraint),
        "recommended_next_action": _recommended_next_action(status),
        "source_links": _source_links(
            failed_report=failed_report,
            prices_path=prices_path,
            data_quality_report=data_quality_report,
        ),
        "validation_context": {
            "data_quality_status": data_quality_status,
            "data_quality_report": data_quality_report,
            "evaluation_only": True,
            "candidate_only": True,
        },
        "safety": policy.safety.model_dump(mode="json"),
        **DYNAMIC_RESCUE_SAFETY,
        "commands_executed": False,
        "shadow_enrollment_allowed": False,
        "automatic_enrollment_allowed": False,
        "owner_approval_executed": False,
    }
    _assert_dynamic_rescue_payload_safe(payload)
    return payload


def build_dynamic_rescue_validation_sample_report(
    *,
    policy_config_path: Path | str = DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH,
    dynamic_robustness_policy_path: Path
    | str = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    dynamic_policy_path: Path | str = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
) -> dict[str, Any]:
    cache_keys = [
        _path_cache_key(Path(item))
        for item in (
            policy_config_path,
            dynamic_robustness_policy_path,
            dynamic_policy_path,
        )
    ]
    payload_text = _cached_dynamic_rescue_validation_sample_report(
        *[part for key in cache_keys for part in key]
    )
    return json.loads(payload_text)


@lru_cache(maxsize=8)
def _cached_dynamic_rescue_validation_sample_report(
    policy_config_path_text: str,
    _policy_config_hash: str,
    dynamic_robustness_policy_path_text: str,
    _dynamic_robustness_policy_hash: str,
    dynamic_policy_path_text: str,
    _dynamic_policy_hash: str,
) -> str:
    policy = load_dynamic_failure_diagnostics_policy_config(Path(policy_config_path_text))
    robustness_policy = load_dynamic_robustness_policy_config(
        Path(dynamic_robustness_policy_path_text)
    )
    dynamic_policy = load_dynamic_allocation_policy_config(Path(dynamic_policy_path_text))
    from ai_trading_system.etf_portfolio.models import load_etf_config_bundle

    sample_report = build_dynamic_rescue_batch_report(
        prices=_synthetic_validation_prices(robustness_policy),
        etf_config=load_etf_config_bundle(),
        policy=policy,
        dynamic_robustness_policy=robustness_policy,
        dynamic_policy=dynamic_policy,
        candidate_id="validation_dynamic_candidate",
        data_quality_status="VALIDATION_SAMPLE",
        data_quality_report="validation_sample",
    )
    _assert_dynamic_rescue_payload_safe(sample_report)
    return json.dumps(sample_report, ensure_ascii=False, sort_keys=True, default=str)


def build_dynamic_rescue_validation_report(
    *,
    policy_config_path: Path = DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH,
    dynamic_robustness_policy_path: Path = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    dynamic_policy_path: Path = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    policy: DynamicFailureDiagnosticsPolicyConfig | None = None
    try:
        policy = load_dynamic_failure_diagnostics_policy_config(policy_config_path)
        _append_check(checks, "failure_diagnostics_policy_valid", True, "policy config loads")
    except Exception as exc:  # noqa: BLE001
        _append_check(checks, "failure_diagnostics_policy_valid", False, str(exc))
    robustness_policy: DynamicRobustnessPolicyConfig | None = None
    try:
        robustness_policy = load_dynamic_robustness_policy_config(dynamic_robustness_policy_path)
        _append_check(checks, "dynamic_robustness_policy_valid", True, "robustness policy loads")
    except Exception as exc:  # noqa: BLE001
        _append_check(checks, "dynamic_robustness_policy_valid", False, str(exc))
    dynamic_policy: DynamicAllocationPolicyConfig | None = None
    try:
        dynamic_policy = load_dynamic_allocation_policy_config(dynamic_policy_path)
        _append_check(checks, "dynamic_allocation_policy_valid", True, "allocation policy loads")
    except Exception as exc:  # noqa: BLE001
        _append_check(checks, "dynamic_allocation_policy_valid", False, str(exc))
    sample_report: dict[str, Any] | None = None
    if policy is not None and robustness_policy is not None and dynamic_policy is not None:
        try:
            sample_report = build_dynamic_rescue_validation_sample_report(
                policy_config_path=policy_config_path,
                dynamic_robustness_policy_path=dynamic_robustness_policy_path,
                dynamic_policy_path=dynamic_policy_path,
            )
            dataset = _mapping(sample_report.get("failure_dataset"))
            _append_check(
                checks,
                "failure_dataset_builder_available",
                _int(dataset.get("row_count")) > 0,
                "failure dataset built from validation sample",
            )
            _append_check(
                checks,
                "layer1_attribution_available",
                bool(sample_report.get("layer1_signal_failure_attribution")),
                "Layer 1 attribution generated",
            )
            _append_check(
                checks,
                "false_signal_attribution_available",
                bool(sample_report.get("false_signal_attribution")),
                "false risk-off/risk-on attribution generated",
            )
            _append_check(
                checks,
                "layer2_attribution_available",
                bool(sample_report.get("layer2_allocation_underperformance_attribution")),
                "Layer 2 allocation attribution generated",
            )
            _append_check(
                checks,
                "turnover_constraint_breakdown_available",
                bool(sample_report.get("turnover_constraint_breakdown")),
                "turnover and constraint breakdown generated",
            )
            _append_check(
                checks,
                "rescue_templates_available",
                len(_records(sample_report.get("rescue_policy_templates"))) >= 4,
                "v0.2-v0.5 rescue templates generated",
            )
            _append_check(
                checks,
                "rescue_batch_runner_available",
                len(_records(sample_report.get("rescue_candidate_comparison"))) >= 4,
                "rescue candidates evaluated with robustness pipeline",
            )
            _append_check(
                checks,
                "rescue_report_available",
                sample_report.get("report_type") == "etf_dynamic_rescue_evaluation_report",
                "rescue evaluation report generated",
            )
            _assert_dynamic_rescue_payload_safe(sample_report)
            _append_check(checks, "sample_report_safety", True, "sample report is safe")
        except Exception as exc:  # noqa: BLE001
            _append_check(checks, "validation_sample_workflow", False, str(exc))
    registry_text = (PROJECT_ROOT / "config" / "report_registry.yaml").read_text(encoding="utf-8")
    _append_check(
        checks,
        "report_registry_visibility",
        "etf_dynamic_rescue_evaluation_report" in registry_text
        and "etf_dynamic_rescue_validation" in registry_text
        and "etf_dynamic_failure_dataset" in registry_text,
        "report registry includes dynamic rescue dataset, report, and validation",
    )
    reader_brief_text = (
        PROJECT_ROOT / "src" / "ai_trading_system" / "reports" / "reader_brief.py"
    ).read_text(encoding="utf-8")
    _append_check(
        checks,
        "reader_brief_visibility",
        "Dynamic Strategy Rescue" in reader_brief_text
        and "_etf_dynamic_rescue_summary" in reader_brief_text,
        "Reader Brief has Dynamic Strategy Rescue section",
    )
    registration_text = (
        PROJECT_ROOT
        / "src"
        / "ai_trading_system"
        / "interfaces"
        / "cli"
        / "etf_portfolio"
        / "registration.py"
    ).read_text(encoding="utf-8")
    command_owner_text = (
        PROJECT_ROOT
        / "src"
        / "ai_trading_system"
        / "interfaces"
        / "cli"
        / "etf_portfolio"
        / "dynamic_rescue.py"
    ).read_text(encoding="utf-8")
    _append_check(
        checks,
        "cli_visibility",
        "dynamic-rescue" in registration_text
        and "dynamic_rescue_app" in command_owner_text,
        "CLI exposes dynamic-rescue namespace",
    )
    failed = [check for check in checks if check["status"] != "PASS"]
    payload = {
        "schema_version": DYNAMIC_RESCUE_VALIDATION_SCHEMA_VERSION,
        "report_type": "etf_dynamic_rescue_validation",
        "validation_id": _stable_id(
            "dynamic-rescue-validation",
            datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
            _stable_hash([check["check_id"] for check in checks]),
        ),
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "PASS" if not failed else "FAIL",
        "check_count": len(checks),
        "failed_check_count": len(failed),
        "checks": checks,
        "source_schema_versions": {
            "policy": DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_SCHEMA_VERSION,
            "dataset": DYNAMIC_FAILURE_DATASET_SCHEMA_VERSION,
            "report": DYNAMIC_RESCUE_REPORT_SCHEMA_VERSION,
        },
        "production_effect_none_required": True,
        "broker_action_none_required": True,
        "manual_review_required": True,
        "no_auto_approval": True,
        "no_auto_enrollment": True,
        "rescue_template_production_mutation_blocked": True,
        "safety": DYNAMIC_RESCUE_SAFETY,
        **DYNAMIC_RESCUE_SAFETY,
        "commands_executed": False,
        "shadow_enrollment_allowed": False,
        "automatic_enrollment_allowed": False,
        "owner_approval_executed": False,
    }
    _assert_dynamic_rescue_payload_safe(payload)
    return payload


def write_dynamic_failure_dataset(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_DYNAMIC_RESCUE_DATASET_DIR,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    dataset_id = _text(payload.get("dataset_id"), "dynamic-failure-dataset")
    json_path = output_dir / f"{dataset_id}.json"
    markdown_path = output_dir / f"{dataset_id}.md"
    _write_json(payload, json_path)
    _write_text(render_dynamic_failure_dataset_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def write_dynamic_rescue_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_DYNAMIC_RESCUE_REPORT_DIR,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_id = _text(payload.get("dynamic_rescue_report_id"), "dynamic-rescue-report")
    json_path = output_dir / f"{report_id}.json"
    markdown_path = output_dir / f"{report_id}.md"
    _write_json(payload, json_path)
    _write_text(render_dynamic_rescue_report_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def write_dynamic_rescue_validation_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_DYNAMIC_RESCUE_VALIDATION_DIR,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    validation_id = _text(payload.get("validation_id"), "dynamic-rescue-validation")
    json_path = output_dir / f"{validation_id}.json"
    markdown_path = output_dir / f"{validation_id}.md"
    _write_json(payload, json_path)
    _write_text(render_dynamic_rescue_validation_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def latest_dynamic_rescue_report_path(
    report_dir: Path = DEFAULT_DYNAMIC_RESCUE_REPORT_DIR,
) -> Path | None:
    return _latest_json(report_dir, "dynamic-rescue-report_*.json")


def load_latest_failed_dynamic_package(
    package_path: Path | None = None,
) -> tuple[Path | None, dict[str, Any]]:
    resolved = package_path or latest_dynamic_shadow_review_package_path(
        DEFAULT_DYNAMIC_SHADOW_PACKAGE_DIR
    )
    if resolved is None or not resolved.exists():
        return None, {}
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise DynamicRescueError("dynamic shadow package must be a mapping")
    return resolved, payload


def load_dynamic_robustness_report(
    report_path: Path | None = None,
    *,
    report_dir: Path = DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
) -> tuple[Path | None, dict[str, Any]]:
    resolved = report_path or latest_dynamic_robustness_report_path(report_dir)
    if resolved is None or not resolved.exists():
        return None, {}
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise DynamicRescueError("dynamic robustness report must be a mapping")
    return resolved, payload


def render_dynamic_failure_dataset_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# Dynamic Failure Dataset {payload.get('dataset_id')}",
        "",
        f"- Status: {payload.get('status')}",
        f"- Candidate: {payload.get('candidate_id')}",
        f"- Rows: {payload.get('row_count')}",
        f"- False Risk-Off Count: {payload.get('false_risk_off_count')}",
        f"- False Risk-On Count: {payload.get('false_risk_on_count')}",
        f"- Data Quality: {payload.get('data_quality_status')}",
        (
            "- Safety: observe_only=true; candidate_only=true; production_effect=none; "
            "broker_action=none; manual_review_required=true"
        ),
        "",
        "## Sample Rows",
        "",
        "| Date | Regime | Turnover | False Off | False On | Underperf Static |",
        "|---|---|---:|---|---|---:|",
    ]
    for row in _records(payload.get("rows"))[:10]:
        lines.append(
            "| "
            f"{row.get('date')} | {row.get('regime_state')} | {_fmt_num(row.get('turnover'))} | "
            f"{row.get('false_risk_off')} | {row.get('false_risk_on')} | "
            f"{_fmt_pct(row.get('underperformance_vs_static'))} |"
        )
    return "\n".join(lines) + "\n"


def render_dynamic_rescue_report_markdown(payload: Mapping[str, Any]) -> str:
    failed = _mapping(payload.get("failed_v0_1_summary"))
    best = _mapping(payload.get("best_rescue_candidate"))
    lines = [
        f"# Dynamic Strategy Rescue {payload.get('dynamic_rescue_report_id')}",
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
        "## Failed v0.1 Summary",
        f"- Status: {failed.get('status')}",
        f"- Candidate: {failed.get('candidate_id')}",
        f"- Reason Codes: {', '.join(str(item) for item in failed.get('reason_codes', []))}",
        (
            "- Positive Evidence: "
            f"{', '.join(str(item) for item in failed.get('positive_evidence', []))}"
        ),
        "",
        "## Rescue Candidate Comparison",
        "",
        (
            "| Policy | Status | Return Improvement | False-Off Reduction | "
            "Turnover Reduction | Constraint Reduction | Underperf Static |"
        ),
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for row in _records(payload.get("rescue_candidate_comparison")):
        lines.append(
            "| "
            f"{row.get('policy_id')} | {row.get('evaluation_status')} | "
            f"{_fmt_pct(row.get('return_vs_static_delta_improvement'))} | "
            f"{row.get('false_risk_off_reduction')} | "
            f"{_fmt_num(row.get('turnover_reduction'))} | "
            f"{row.get('constraint_hit_reduction')} | "
            f"{_fmt_pct(row.get('underperformance_vs_static'))} |"
        )
    lines.extend(
        [
            "",
            "## Improvement Summary",
            f"- Status: {payload.get('status')}",
            f"- Best Rescue Candidate: {best.get('policy_id', 'MISSING')}",
            f"- Recommended Next Action: {payload.get('recommended_next_action')}",
            "",
            "## Remaining Blockers",
        ]
    )
    for blocker in _records(payload.get("remaining_blockers")):
        lines.append(f"- {blocker.get('blocker_id')}: {blocker.get('detail')}")
    return "\n".join(lines) + "\n"


def render_dynamic_rescue_validation_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# Dynamic Rescue Validation {payload.get('validation_id')}",
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


def _failure_dataset_rows(
    *,
    robustness_report: Mapping[str, Any],
    policy: DynamicFailureDiagnosticsPolicyConfig,
) -> list[dict[str, Any]]:
    summary = _mapping(robustness_report.get("summary"))
    candidate_context = _mapping(robustness_report.get("candidate_context"))
    dynamic_records = _records(_mapping(robustness_report.get("daily_path_summary")).get("records"))
    if not dynamic_records:
        dynamic_records = _records(
            _mapping(robustness_report.get("daily_path_summary")).get("sample_rows")
        )
    comparison_paths = {
        key: _by_return_date(_records(value))
        for key, value in _mapping(robustness_report.get("comparison_daily_paths")).items()
    }
    static_weights = _static_base_weights(robustness_report)
    rows: list[dict[str, Any]] = []
    for record in dynamic_records:
        return_date = _text(record.get("return_date"))
        target_weights = _json_mapping(record.get("target_weights_json"))
        previous_weights = _json_mapping(record.get("previous_weights_json")) or dict(
            static_weights
        )
        input_scores = _json_mapping(record.get("input_scores_json"))
        asset_returns = _json_mapping(record.get("asset_returns_json"))
        constraints = _json_list(record.get("constraints_applied_json"))
        if not constraints:
            constraints = [
                code for code in _json_list(record.get("reason_codes_json")) if _is_constraint(code)
            ]
        comparison = {
            key: _mapping(path.get(return_date)) for key, path in comparison_paths.items()
        }
        dynamic_return = _float(record.get("strategy_return"))
        static_return = _float(
            _mapping(comparison.get("static_base_candidate")).get("strategy_return")
        )
        baseline_return = _float(
            _mapping(comparison.get("current_etf_baseline")).get("strategy_return")
        )
        qqq_return = _float(_mapping(comparison.get("QQQ_buy_and_hold")).get("strategy_return"))
        spy_return = _float(_mapping(comparison.get("SPY_buy_and_hold")).get("strategy_return"))
        smh_return = _float(_mapping(comparison.get("SMH_buy_and_hold")).get("strategy_return"))
        false_off, opportunity_cost = _false_risk_off(
            target_weights=target_weights,
            static_weights=static_weights,
            dynamic_return=dynamic_return,
            market_return=qqq_return,
            policy=policy,
        )
        false_on, drawdown_cost = _false_risk_on(
            target_weights=target_weights,
            static_weights=static_weights,
            dynamic_return=dynamic_return,
            static_return=static_return,
            market_return=qqq_return,
            policy=policy,
        )
        rows.append(
            {
                "date": _text(record.get("signal_date")),
                "candidate_id": _text(
                    record.get("candidate_id"), _text(summary.get("dynamic_candidate_id"))
                ),
                "trend_signal_config_id": _text(candidate_context.get("trend_signal_config_id")),
                "dynamic_policy_id": _text(
                    record.get("dynamic_policy_id"),
                    _text(robustness_report.get("dynamic_allocation_policy_id")),
                ),
                "regime_state": _text(record.get("selected_regime")),
                "input_scores": input_scores,
                "previous_weights": previous_weights,
                "target_weights": target_weights,
                "actual_rebalanced_weights": target_weights,
                "constraint_hits": constraints,
                "turnover": _float(record.get("turnover")),
                "forward_return_dynamic": dynamic_return,
                "forward_return_static": static_return,
                "forward_return_baseline": baseline_return,
                "forward_return_QQQ": qqq_return,
                "forward_return_SPY": spy_return,
                "forward_return_SMH": smh_return,
                "asset_returns": asset_returns,
                "forward_drawdown_dynamic": min(0.0, dynamic_return),
                "false_risk_off": false_off,
                "false_risk_on": false_on,
                "false_risk_off_opportunity_cost": opportunity_cost,
                "false_risk_on_drawdown_cost": drawdown_cost,
                "underperformance_vs_static": dynamic_return - static_return,
                "underperformance_vs_baseline": dynamic_return - baseline_return,
                "event_risk_flag": _float(input_scores.get("EventRiskScore"))
                >= policy.false_signal_definitions.event_risk_trigger_score,
                "reason_codes": _json_list(record.get("reason_codes_json")),
                "data_quality_status": _text(
                    record.get("data_quality_status"), _text(summary.get("data_quality_status"))
                ),
                "evaluation_only": True,
                "safety": policy.safety.model_dump(mode="json"),
            }
        )
    return rows


def _rescue_candidate_comparison(
    *,
    template: DynamicRescuePolicyTemplate,
    failed_report: Mapping[str, Any],
    rescue_report: Mapping[str, Any],
    policy: DynamicFailureDiagnosticsPolicyConfig,
) -> dict[str, Any]:
    failed_summary = _mapping(failed_report.get("summary"))
    rescue_summary = _mapping(rescue_report.get("summary"))
    failed_dynamic = _comparison_row(failed_report, "dynamic_candidate")
    rescue_dynamic = _comparison_row(rescue_report, "dynamic_candidate")
    return_improvement = _float(rescue_summary.get("excess_vs_static_base")) - _float(
        failed_summary.get("excess_vs_static_base")
    )
    false_off_reduction = _int(failed_summary.get("false_risk_off_count")) - _int(
        rescue_summary.get("false_risk_off_count")
    )
    false_on_reduction = _int(failed_summary.get("false_risk_on_count")) - _int(
        rescue_summary.get("false_risk_on_count")
    )
    turnover_reduction = _float(failed_dynamic.get("turnover")) - _float(
        rescue_dynamic.get("turnover")
    )
    constraint_reduction = _int(
        _mapping(failed_report.get("daily_path_summary")).get("constraint_hit_count")
    ) - _int(_mapping(rescue_report.get("daily_path_summary")).get("constraint_hit_count"))
    drawdown_preservation = _float(rescue_summary.get("dynamic_max_drawdown")) - _float(
        failed_summary.get("dynamic_max_drawdown")
    )
    evaluation_status = _candidate_evaluation_status(
        return_improvement=return_improvement,
        false_off_reduction=false_off_reduction,
        turnover_reduction=turnover_reduction,
        constraint_reduction=constraint_reduction,
        drawdown_preservation=drawdown_preservation,
        policy=policy,
    )
    return {
        "policy_id": template.policy_id,
        "template_type": template.template_type,
        "evaluation_status": evaluation_status,
        "return_vs_static_delta_improvement": return_improvement,
        "false_risk_off_reduction": false_off_reduction,
        "false_risk_on_reduction": false_on_reduction,
        "turnover_reduction": turnover_reduction,
        "constraint_hit_reduction": constraint_reduction,
        "drawdown_preservation": drawdown_preservation,
        "underperformance_vs_static": _float(rescue_summary.get("excess_vs_static_base")),
        "rescue_report_id": rescue_report.get("dynamic_robustness_report_id"),
        "evidence_linked_failure_targets": template.expected_failure_target,
        "safety": policy.safety.model_dump(mode="json"),
    }


def _candidate_evaluation_status(
    *,
    return_improvement: float,
    false_off_reduction: int,
    turnover_reduction: float,
    constraint_reduction: int,
    drawdown_preservation: float,
    policy: DynamicFailureDiagnosticsPolicyConfig,
) -> str:
    req = policy.improvement_requirements
    if (
        return_improvement >= req.min_return_vs_static_delta_improvement
        and false_off_reduction >= req.min_false_risk_off_reduction_count
        and turnover_reduction >= req.min_turnover_reduction
        and constraint_reduction >= req.min_constraint_hit_reduction_count
        and drawdown_preservation >= -req.max_drawdown_degradation
    ):
        return "rescue_success_candidate_found"
    if (
        return_improvement > 0
        or false_off_reduction > 0
        or turnover_reduction > 0
        or constraint_reduction > 0
    ):
        return "partial_improvement_needs_more_review"
    return "no_rescue_candidate_found"


def _rescue_status(
    best: Mapping[str, Any],
    results: list[Mapping[str, Any]],
    layer1: Mapping[str, Any],
    policy: DynamicFailureDiagnosticsPolicyConfig,
) -> str:
    statuses = {_text(row.get("evaluation_status")) for row in results}
    if "rescue_success_candidate_found" in statuses:
        return "rescue_success_candidate_found"
    if (
        _float(layer1.get("quality_score"))
        < policy.failure_thresholds.min_trend_signal_quality_score
    ):
        return "needs_signal_recalibration_first"
    if _text(best.get("evaluation_status")) == "partial_improvement_needs_more_review":
        return "partial_improvement_needs_more_review"
    return "no_rescue_candidate_found"


def _failed_v0_1_summary(
    failed_report: Mapping[str, Any],
    policy: DynamicFailureDiagnosticsPolicyConfig,
) -> dict[str, Any]:
    summary = _mapping(failed_report.get("summary"))
    daily_summary = _mapping(failed_report.get("daily_path_summary"))
    dynamic = _comparison_row(failed_report, "dynamic_candidate")
    reason_codes: list[str] = []
    if _float(summary.get("excess_vs_static_base")) <= (
        policy.failure_thresholds.max_underperformance_vs_static_pp / 100.0
    ):
        reason_codes.append("DYNAMIC_UNDERPERFORMED_STATIC_BASE")
    if (
        _int(summary.get("false_risk_off_count"))
        > policy.failure_thresholds.max_false_risk_off_count
    ):
        reason_codes.append("HIGH_FALSE_RISK_OFF_COUNT")
    if _float(dynamic.get("turnover")) > policy.failure_thresholds.max_turnover:
        reason_codes.append("HIGH_TURNOVER")
    row_count = max(1, _int(daily_summary.get("row_count")))
    hit_rate = _int(daily_summary.get("constraint_hit_count")) / row_count
    if hit_rate > policy.failure_thresholds.max_constraint_hit_rate:
        reason_codes.append("HIGH_CONSTRAINT_HIT_RATE")
    if not reason_codes:
        reason_codes.append("REVIEW_REQUIRED_DYNAMIC_FAILURE")
    positive_evidence = []
    static = _comparison_row(failed_report, "static_base_candidate")
    if abs(_float(dynamic.get("max_drawdown"))) < abs(_float(static.get("max_drawdown"))):
        positive_evidence.append("drawdown_improved_vs_static")
        positive_evidence.append("risk_control_has_some_value")
    return {
        "status": "not_approved_for_shadow",
        "candidate_id": _text(summary.get("dynamic_candidate_id")),
        "reason_codes": reason_codes,
        "positive_evidence": positive_evidence,
        "dynamic_total_return": summary.get("dynamic_total_return"),
        "excess_vs_static_base": summary.get("excess_vs_static_base"),
        "false_risk_off_count": summary.get("false_risk_off_count"),
        "false_risk_on_count": summary.get("false_risk_on_count"),
        "turnover": dynamic.get("turnover"),
        "constraint_hit_rate": hit_rate,
    }


def _trend_score_buckets(
    rows: list[Mapping[str, Any]],
    policy: DynamicFailureDiagnosticsPolicyConfig,
) -> dict[str, list[Mapping[str, Any]]]:
    low: list[Mapping[str, Any]] = []
    mid: list[Mapping[str, Any]] = []
    high: list[Mapping[str, Any]] = []
    for row in rows:
        score = _float(_mapping(row.get("input_scores")).get("CompositeTrendScore"))
        if score <= policy.signal_bucket_thresholds.low_score_max:
            low.append(row)
        elif score >= policy.signal_bucket_thresholds.high_score_min:
            high.append(row)
        else:
            mid.append(row)
    return {"low": low, "mid": mid, "high": high}


def _signal_contributors(
    rows: list[Mapping[str, Any]],
    policy: DynamicFailureDiagnosticsPolicyConfig,
    *,
    flag: str,
) -> list[dict[str, Any]]:
    thresholds = policy.signal_bucket_thresholds
    contributors = {
        "trend_breakdown": lambda scores: _float(scores.get("CompositeTrendScore"))
        <= thresholds.weak_score_max,
        "volatility_risk": lambda scores: _float(scores.get("RiskRegimeScore"))
        <= thresholds.weak_score_max,
        "event_risk": lambda scores: _float(scores.get("EventRiskScore"))
        >= thresholds.event_risk_high_min,
        "relative_strength_weak": lambda scores: _float(scores.get("GrowthLeadershipScore"))
        <= thresholds.weak_score_max,
        "AI_confirmation_weak": lambda scores: _float(scores.get("SemiconductorLeadershipScore"))
        <= thresholds.weak_score_max,
    }
    output: list[dict[str, Any]] = []
    flagged = [row for row in rows if row.get(flag) is True]
    for signal_id, predicate in contributors.items():
        count = sum(1 for row in flagged if predicate(_mapping(row.get("input_scores"))))
        output.append({"signal_id": signal_id, "event_count": count})
    return output


def _layer1_recommendations(
    *,
    quality_score: float,
    return_lift: float,
    redundancy_count: int,
    policy: DynamicFailureDiagnosticsPolicyConfig,
) -> list[str]:
    recommendations: list[str] = []
    if quality_score < policy.failure_thresholds.min_trend_signal_quality_score:
        recommendations.append("recalibrate_trend_signal_weights_before_shadow_approval")
    if return_lift <= 0:
        recommendations.append("reduce_weight_on_trend_bucket_that_fails_forward_return")
    if redundancy_count > policy.failure_thresholds.max_signal_redundancy_count:
        recommendations.append("deduplicate_highly_redundant_trend_signals")
    if not recommendations:
        recommendations.append("retain_signal_weights_for_candidate_only_rescue_review")
    return recommendations


def _false_signal_group(rows: list[Mapping[str, Any]], field: str) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, int]] = {}
    for row in rows:
        key = _text(row.get(field), "UNKNOWN")
        current = groups.setdefault(key, {"false_risk_off_count": 0, "false_risk_on_count": 0})
        if row.get("false_risk_off") is True:
            current["false_risk_off_count"] += 1
        if row.get("false_risk_on") is True:
            current["false_risk_on_count"] += 1
    return [{"group": key, **value} for key, value in sorted(groups.items())]


def _false_signal_score_buckets(
    rows: list[Mapping[str, Any]],
    policy: DynamicFailureDiagnosticsPolicyConfig,
) -> list[dict[str, Any]]:
    buckets = _trend_score_buckets(rows, policy)
    output = []
    for bucket, bucket_rows in buckets.items():
        output.append(
            {
                "bucket": bucket,
                "row_count": len(bucket_rows),
                "false_risk_off_count": sum(
                    1 for row in bucket_rows if row.get("false_risk_off") is True
                ),
                "false_risk_on_count": sum(
                    1 for row in bucket_rows if row.get("false_risk_on") is True
                ),
            }
        )
    return output


def _false_signal_by_trigger(
    rows: list[Mapping[str, Any]],
    policy: DynamicFailureDiagnosticsPolicyConfig,
) -> list[dict[str, Any]]:
    counts: dict[str, dict[str, int]] = {}
    for row in rows:
        for trigger in _false_signal_triggers(row, policy):
            current = counts.setdefault(
                trigger,
                {"false_risk_off_count": 0, "false_risk_on_count": 0},
            )
            if row.get("false_risk_off") is True:
                current["false_risk_off_count"] += 1
            if row.get("false_risk_on") is True:
                current["false_risk_on_count"] += 1
    return [{"trigger": key, **value} for key, value in sorted(counts.items())]


def _false_signal_triggers(
    row: Mapping[str, Any],
    policy: DynamicFailureDiagnosticsPolicyConfig,
) -> list[str]:
    scores = _mapping(row.get("input_scores"))
    reasons = [str(item) for item in row.get("reason_codes", [])]
    thresholds = policy.signal_bucket_thresholds
    triggers: list[str] = []
    if _float(scores.get("CompositeTrendScore")) <= thresholds.weak_score_max:
        triggers.append("trend_breakdown")
    if _float(scores.get("RiskRegimeScore")) <= thresholds.weak_score_max:
        triggers.append("volatility_risk")
    if _float(scores.get("EventRiskScore")) >= thresholds.event_risk_high_min:
        triggers.append("event_risk")
    if _float(scores.get("GrowthLeadershipScore")) <= thresholds.weak_score_max:
        triggers.append("relative_strength_weak")
    if _float(scores.get("SemiconductorLeadershipScore")) <= thresholds.weak_score_max:
        triggers.append("AI_confirmation_weak")
    if any("REGIME" in reason for reason in reasons):
        triggers.append("regime_switch")
    return sorted(set(triggers)) or ["unclassified"]


def _allocation_failure_reasons(
    *,
    rows: list[Mapping[str, Any]],
    total_underperformance: float,
    robustness_report: Mapping[str, Any],
    policy: DynamicFailureDiagnosticsPolicyConfig,
) -> list[str]:
    reasons: list[str] = []
    summary = _mapping(robustness_report.get("summary"))
    dynamic = _comparison_row(robustness_report, "dynamic_candidate")
    daily_summary = _mapping(robustness_report.get("daily_path_summary"))
    if (
        total_underperformance
        <= policy.failure_thresholds.max_underperformance_vs_static_pp / 100.0
    ):
        reasons.append("DYNAMIC_UNDERPERFORMED_STATIC_BASE")
    if (
        _int(summary.get("false_risk_off_count"))
        > policy.failure_thresholds.max_false_risk_off_count
    ):
        reasons.append("HIGH_FALSE_RISK_OFF_COUNT")
    if _float(dynamic.get("turnover")) > policy.failure_thresholds.max_turnover:
        reasons.append("HIGH_TURNOVER")
    row_count = max(1, len(rows))
    if _int(daily_summary.get("constraint_hit_count")) / row_count > (
        policy.failure_thresholds.max_constraint_hit_rate
    ):
        reasons.append("HIGH_CONSTRAINT_HIT_RATE")
    if not reasons:
        reasons.append("ALLOCATION_REVIEW_REQUIRED")
    return reasons


def _turnover_sources(row: Mapping[str, Any], previous_regime: str) -> list[str]:
    reasons = [str(item) for item in row.get("reason_codes", [])]
    sources: list[str] = []
    if previous_regime and _text(row.get("regime_state")) != previous_regime:
        sources.append("turnover_from_regime_switch")
    if any("EVENT_RISK" in reason for reason in reasons):
        sources.append("turnover_from_event_risk_overlay")
    if any(
        ("GROWTH" in reason or "SEMICONDUCTOR" in reason or "WEAK_" in reason) for reason in reasons
    ):
        sources.append("turnover_from_trend_overlay")
    if any(_is_constraint(reason) for reason in reasons):
        sources.append("turnover_from_constraint_correction")
    if any(
        marker in reason
        for reason in reasons
        for marker in ("SCORE_CHANGE", "MINIMUM_HOLDING", "WEIGHT_DELTA", "REGIME_CONFIRMATION")
    ):
        sources.append("turnover_from_rebalance_threshold")
    return sorted(set(sources)) or ["turnover_from_other"]


def _constraint_type(reason_code: str) -> str:
    if "QQQ" in reason_code and "MAX" in reason_code:
        return "QQQ_max_hit"
    if "SEMICONDUCTOR" in reason_code:
        return "semiconductor_cap_hit"
    if "CASH" in reason_code and "MAX" in reason_code:
        return "CASH_max_hit"
    if "CASH" in reason_code and "MIN" in reason_code:
        return "CASH_min_hit"
    if "SINGLE_REBALANCE" in reason_code or "MAX_SINGLE" in reason_code:
        return "single_rebalance_delta_hit"
    if "WEEKLY_TURNOVER_CAP" in reason_code:
        return "weekly_turnover_cap_hit"
    if "MINIMUM_HOLDING" in reason_code:
        return "minimum_holding_period_block"
    return reason_code


def _turnover_constraint_recommendations(
    *,
    turnover_by_source: Mapping[str, float],
    constraint_hit_rate: float,
    warnings: list[str],
) -> list[str]:
    recommendations = []
    if turnover_by_source.get("turnover_from_regime_switch", 0.0) > turnover_by_source.get(
        "turnover_from_trend_overlay", 0.0
    ):
        recommendations.append("evaluate_slow_switch_rescue_template")
    if turnover_by_source.get("turnover_from_trend_overlay", 0.0) > 0:
        recommendations.append("evaluate_lower_turnover_overlay_template")
    if constraint_hit_rate > 0:
        recommendations.append("review_constraint_hits_before_widening_caps")
    if not recommendations and not warnings:
        recommendations.append("retain_current_constraints_for_candidate_only_review")
    return recommendations


def _evidence_links(evidence: Mapping[str, Any]) -> list[dict[str, Any]]:
    false_signal = _mapping(evidence.get("false_signal_attribution"))
    layer2 = _mapping(evidence.get("layer2_allocation_failure"))
    turnover = _mapping(evidence.get("turnover_constraint_breakdown"))
    layer1 = _mapping(evidence.get("layer1_signal_failure"))
    links = [
        {
            "reason_code": "HIGH_FALSE_RISK_OFF_COUNT",
            "metric": "false_risk_off_count",
            "value": false_signal.get("false_risk_off_count"),
        },
        {
            "reason_code": "DYNAMIC_UNDERPERFORMED_STATIC_BASE",
            "metric": "total_underperformance_vs_static",
            "value": layer2.get("total_underperformance_vs_static"),
        },
        {
            "reason_code": "HIGH_TURNOVER",
            "metric": "turnover_total",
            "value": turnover.get("turnover_total"),
        },
        {
            "reason_code": "HIGH_CONSTRAINT_HIT_RATE",
            "metric": "constraint_hit_rate",
            "value": turnover.get("constraint_hit_rate"),
        },
        {
            "reason_code": "TREND_SIGNAL_QUALITY_ZERO",
            "metric": "quality_score",
            "value": layer1.get("quality_score"),
        },
        {
            "reason_code": "HIGH_SIGNAL_REDUNDANCY",
            "metric": "redundancy_count",
            "value": layer1.get("redundancy_count"),
        },
    ]
    return links


def _best_rescue_candidate(results: list[Mapping[str, Any]]) -> dict[str, Any]:
    if not results:
        return {}
    return dict(
        max(
            results,
            key=lambda row: (
                _float(row.get("return_vs_static_delta_improvement")),
                _int(row.get("false_risk_off_reduction")),
                _float(row.get("turnover_reduction")),
                _int(row.get("constraint_hit_reduction")),
            ),
        )
    )


def _improvement_summary(
    best: Mapping[str, Any],
    results: list[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "candidate_count": len(results),
        "best_candidate": best.get("policy_id", "MISSING"),
        "best_status": best.get("evaluation_status", "MISSING"),
        "best_return_vs_static_delta_improvement": best.get("return_vs_static_delta_improvement"),
        "best_false_risk_off_reduction": best.get("false_risk_off_reduction"),
        "best_turnover_reduction": best.get("turnover_reduction"),
        "best_constraint_hit_reduction": best.get("constraint_hit_reduction"),
    }


def _remaining_blockers(
    status: str,
    layer1: Mapping[str, Any],
    layer2: Mapping[str, Any],
    turnover: Mapping[str, Any],
) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    if status == "needs_signal_recalibration_first":
        blockers.append(
            {
                "blocker_id": "NEEDS_SIGNAL_RECALIBRATION_FIRST",
                "detail": "Layer 1 quality score is below policy threshold.",
            }
        )
    if _float(layer2.get("total_underperformance_vs_static")) < 0:
        blockers.append(
            {
                "blocker_id": "STILL_UNDERPERFORMS_STATIC_BASE",
                "detail": "Dynamic rescue evidence still must beat or justify static base.",
            }
        )
    if _mapping(turnover).get("warnings"):
        blockers.append(
            {
                "blocker_id": "TURNOVER_OR_CONSTRAINT_REVIEW_REQUIRED",
                "detail": ", ".join(str(item) for item in _mapping(turnover).get("warnings", [])),
            }
        )
    return blockers


def _recommended_next_action(status: str) -> str:
    if status == "rescue_success_candidate_found":
        return "prepare_TRADING_089_dynamic_v0_2_robustness_and_shadow_review_package"
    if status == "needs_signal_recalibration_first":
        return "prepare_TRADING_089_trend_signal_recalibration_v0_2"
    if status == "partial_improvement_needs_more_review":
        return "owner_review_partial_rescue_before_any_shadow_package"
    return "do_not_approve_dynamic_rescue_without_signal_recalibration"


def _source_links(
    *,
    failed_report: Mapping[str, Any],
    prices_path: Path | None,
    data_quality_report: str,
) -> dict[str, Any]:
    source = _mapping(failed_report.get("source_artifacts"))
    return {
        "failed_dynamic_robustness_report": failed_report.get("dynamic_robustness_report_id"),
        "dynamic_calibration_report": source.get("dynamic_calibration_report"),
        "prices_path": str(prices_path or source.get("prices_path", "")),
        "data_quality_report": data_quality_report or source.get("data_quality_report"),
    }


def _cash_drag(row: Mapping[str, Any]) -> float:
    static_cash = _float(_mapping(row.get("previous_weights")).get("CASH"))
    target_cash = _float(_mapping(row.get("target_weights")).get("CASH"))
    return max(0.0, target_cash - static_cash) * max(
        0.0,
        _float(row.get("forward_return_QQQ")) - _float(row.get("forward_return_dynamic")),
    )


def _growth_underweight_cost(row: Mapping[str, Any]) -> float:
    static_growth = _growth_weight(_mapping(row.get("previous_weights")))
    target_growth = _growth_weight(_mapping(row.get("target_weights")))
    return max(0.0, static_growth - target_growth) * max(
        0.0,
        _float(row.get("forward_return_QQQ")) - _float(row.get("forward_return_dynamic")),
    )


def _semiconductor_underweight_cost(row: Mapping[str, Any]) -> float:
    previous = _mapping(row.get("previous_weights"))
    target = _mapping(row.get("target_weights"))
    static_semi = _float(previous.get("SMH")) + _float(previous.get("SOXX"))
    target_semi = _float(target.get("SMH")) + _float(target.get("SOXX"))
    semi_return = _float(row.get("forward_return_SMH"))
    return max(0.0, static_semi - target_semi) * max(
        0.0, semi_return - _float(row.get("forward_return_dynamic"))
    )


def _spy_overweight_effect(row: Mapping[str, Any]) -> float:
    previous = _mapping(row.get("previous_weights"))
    target = _mapping(row.get("target_weights"))
    return max(0.0, _float(target.get("SPY")) - _float(previous.get("SPY"))) * (
        _float(row.get("forward_return_SPY")) - _float(row.get("forward_return_dynamic"))
    )


def _false_risk_off(
    *,
    target_weights: Mapping[str, Any],
    static_weights: Mapping[str, Any],
    dynamic_return: float,
    market_return: float,
    policy: DynamicFailureDiagnosticsPolicyConfig,
) -> tuple[bool, float]:
    cfg = policy.false_signal_definitions
    cash_diff = _float(target_weights.get("CASH")) - _float(static_weights.get("CASH"))
    growth_diff = _growth_weight(target_weights) - _growth_weight(static_weights)
    false_off = (
        cash_diff >= cfg.cash_overweight_threshold
        or growth_diff <= cfg.growth_underweight_threshold
    ) and market_return >= cfg.false_risk_off_min_market_return
    return false_off, max(0.0, market_return - dynamic_return) if false_off else 0.0


def _false_risk_on(
    *,
    target_weights: Mapping[str, Any],
    static_weights: Mapping[str, Any],
    dynamic_return: float,
    static_return: float,
    market_return: float,
    policy: DynamicFailureDiagnosticsPolicyConfig,
) -> tuple[bool, float]:
    cfg = policy.false_signal_definitions
    growth_diff = _growth_weight(target_weights) - _growth_weight(static_weights)
    drawdown = min(0.0, market_return)
    false_on = (
        growth_diff >= cfg.growth_overweight_threshold
        and drawdown <= cfg.false_risk_on_forward_drawdown_threshold
    )
    return false_on, max(0.0, abs(min(0.0, dynamic_return)) - abs(min(0.0, static_return)))


def _static_base_weights(robustness_report: Mapping[str, Any]) -> dict[str, float]:
    static_row = _comparison_row(robustness_report, "static_base_candidate")
    weights = _mapping(static_row.get("latest_weights"))
    if not weights:
        return {symbol: 0.0 for symbol in WEIGHT_SYMBOLS}
    return _normalise_weights(weights)


def _comparison_row(robustness_report: Mapping[str, Any], comparison_id: str) -> dict[str, Any]:
    for row in _records(robustness_report.get("comparison_table")):
        if _text(row.get("comparison_id")) == comparison_id:
            return dict(row)
    return {}


def _by_return_date(records: list[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    return {_text(record.get("return_date")): record for record in records}


def _is_constraint(reason_code: str) -> bool:
    return reason_code.startswith(("MAX_", "MIN_", "WEEKLY_TURNOVER_CAP", "REGIME_CONFIRMATION"))


def _normalise_weights(weights: Mapping[str, Any]) -> dict[str, float]:
    cleaned = {symbol: max(0.0, _float(weights.get(symbol))) for symbol in WEIGHT_SYMBOLS}
    total = sum(cleaned.values())
    if total <= 0:
        return {symbol: 1.0 / len(WEIGHT_SYMBOLS) for symbol in WEIGHT_SYMBOLS}
    return {symbol: round(value / total, 10) for symbol, value in cleaned.items()}


def _growth_weight(weights: Mapping[str, Any]) -> float:
    return _float(weights.get("QQQ")) + _float(weights.get("SMH")) + _float(weights.get("SOXX"))


def _first_numeric_by_key(
    payloads: list[Mapping[str, Any]],
    keys: tuple[str, ...],
    default: float = 0.0,
) -> float:
    for payload in payloads:
        found = _find_numeric(payload, set(keys))
        if found is not None:
            return found
    return default


def _find_numeric(value: Any, keys: set[str]) -> float | None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key) in keys:
                try:
                    return float(item)
                except (TypeError, ValueError):
                    continue
            found = _find_numeric(item, keys)
            if found is not None:
                return found
    if isinstance(value, list):
        for item in value:
            found = _find_numeric(item, keys)
            if found is not None:
                return found
    return None


def _avg(values: list[float]) -> float:
    return mean(values) if values else 0.0


def _compound_return(returns: list[float]) -> float:
    if not returns:
        return 0.0
    return prod(1.0 + value for value in returns) - 1.0


def _append_check(checks: list[dict[str, Any]], check_id: str, passed: bool, detail: str) -> None:
    checks.append(
        {
            "check_id": check_id,
            "status": "PASS" if passed else "FAIL",
            "passed": passed,
            "detail": detail,
        }
    )


def _assert_dynamic_rescue_payload_safe(payload: Mapping[str, Any]) -> None:
    safety = _mapping(payload.get("safety"))
    if safety != DYNAMIC_RESCUE_SAFETY:
        raise DynamicRescueError("dynamic rescue payload safety fields are unsafe")
    for key, expected in DYNAMIC_RESCUE_SAFETY.items():
        if payload.get(key) != expected:
            raise DynamicRescueError(f"dynamic rescue top-level safety mismatch: {key}")
    if payload.get("shadow_enrollment_allowed") is True:
        raise DynamicRescueError("dynamic rescue cannot allow shadow enrollment")
    if payload.get("automatic_enrollment_allowed") is True:
        raise DynamicRescueError("dynamic rescue cannot allow automatic enrollment")
    if payload.get("owner_approval_executed") is True:
        raise DynamicRescueError("dynamic rescue cannot execute owner approval")
    for key in _all_keys(payload):
        if key in FORBIDDEN_DYNAMIC_RESCUE_KEYS:
            raise DynamicRescueError(f"forbidden dynamic rescue output key: {key}")


def _all_keys(value: Any) -> set[str]:
    keys: set[str] = set()
    if isinstance(value, Mapping):
        for key, item in value.items():
            keys.add(str(key))
            keys.update(_all_keys(item))
    elif isinstance(value, list):
        for item in value:
            keys.update(_all_keys(item))
    return keys


def _safety_status(safety: Mapping[str, Any]) -> str:
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true"
        if _mapping(safety) == DYNAMIC_RESCUE_SAFETY
        else "SAFETY_REVIEW_REQUIRED"
    )


def _latest_json(directory: Path, pattern: str) -> Path | None:
    if not directory.exists():
        return None
    candidates = sorted(directory.glob(pattern), key=lambda path: path.stat().st_mtime)
    return candidates[-1] if candidates else None


def _write_json(payload: Mapping[str, Any], path: Path) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=_json_default),
        encoding="utf-8",
    )


def _write_text(text: str, path: Path) -> None:
    path.write_text(text, encoding="utf-8")


def _json_default(value: Any) -> str:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, date | datetime):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _stable_id(prefix: str, *parts: Any) -> str:
    return f"{prefix}_{_stable_hash(parts)[:12]}"


def _stable_hash(value: Any) -> str:
    return sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default).encode("utf-8")
    ).hexdigest()


def _path_cache_key(path: Path) -> tuple[str, str]:
    resolved = path.resolve()
    if not resolved.exists():
        return str(resolved), "missing"
    return str(resolved), sha256(resolved.read_bytes()).hexdigest()


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str) or not value:
        return {}
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return dict(loaded) if isinstance(loaded, Mapping) else {}


def _json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if not isinstance(value, str) or not value:
        return []
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(loaded, list):
        return []
    return [str(item) for item in loaded]


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _fmt_pct(value: Any) -> str:
    if value is None:
        return "N/A"
    return f"{_float(value) * 100:.2f}%"


def _fmt_num(value: Any) -> str:
    if value is None:
        return "N/A"
    return f"{_float(value):.4f}"
