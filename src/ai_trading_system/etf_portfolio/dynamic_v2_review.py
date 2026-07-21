from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from hashlib import sha256
from math import prod, sqrt
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Literal, Self

from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import PRIMARY_RESEARCH_START_DATE
from ai_trading_system.etf_portfolio.dynamic_rescue import (
    DEFAULT_DYNAMIC_RESCUE_REPORT_DIR,
    latest_dynamic_rescue_report_path,
)
from ai_trading_system.etf_portfolio.dynamic_robustness import (
    DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
    latest_dynamic_robustness_report_path,
)
from ai_trading_system.etf_portfolio.dynamic_shadow import (
    DEFAULT_DYNAMIC_SHADOW_PACKAGE_DIR,
    latest_dynamic_shadow_review_package_path,
)
from ai_trading_system.etf_portfolio.models import PolicyMetadata
from ai_trading_system.yaml_loader import safe_load_yaml_path

DYNAMIC_V2_REVIEW_POLICY_SCHEMA_VERSION = "etf_dynamic_v2_review_policy_v1"
DYNAMIC_V2_REVIEW_PACKAGE_SCHEMA_VERSION = "etf_dynamic_v2_review_package_v1"
DYNAMIC_V2_REVIEW_VALIDATION_SCHEMA_VERSION = "etf_dynamic_v2_review_validation_v1"

DYNAMIC_V2_REVIEW_PACKAGE_REPORT_TYPE = "etf_dynamic_v2_review_package"
DYNAMIC_V2_REVIEW_VALIDATION_REPORT_TYPE = "etf_dynamic_v2_review_validation"

DEFAULT_DYNAMIC_V2_REVIEW_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_v2_review.yaml"
)
DEFAULT_DYNAMIC_V2_REVIEW_ROOT = PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_v2_review"
DEFAULT_DYNAMIC_V2_REVIEW_PACKAGE_DIR = DEFAULT_DYNAMIC_V2_REVIEW_ROOT / "packages"
DEFAULT_DYNAMIC_V2_REVIEW_VALIDATION_DIR = DEFAULT_DYNAMIC_V2_REVIEW_ROOT / "validation"

DYNAMIC_V2_REVIEW_SAFETY: dict[str, Any] = {
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

DYNAMIC_V2_REVIEW_FORBIDDEN_KEYS = {
    "broker_order",
    "production_weight_update",
    "baseline_config_mutation",
    "official_target_weights_write",
    "apply_to_production",
    "promote_to_baseline",
    "place_order",
    "enable_broker_action",
    "approved_for_shadow",
    "shadow_enrollment_record",
    "auto_promotion",
}

REQUIRED_CONSTRAINT_TYPES = (
    "QQQ_max_hit",
    "semiconductor_cap_hit",
    "CASH_max_hit",
    "CASH_min_hit",
    "single_rebalance_delta_hit",
    "weekly_turnover_cap_hit",
    "minimum_holding_period_block",
    "risk_overlay_conflict",
    "trend_overlay_conflict",
)


class DynamicV2ReviewError(ValueError):
    """Raised when TRADING-089 dynamic v0.2 review inputs or outputs are invalid."""


class DynamicV2ReviewMarketRegime(BaseModel):
    regime_id: Literal["unified_primary_2021"]
    anchor_event: str = Field(min_length=1)
    anchor_date: date
    default_backtest_start: date

    @model_validator(mode="after")
    def validate_regime(self) -> Self:
        if self.default_backtest_start < PRIMARY_RESEARCH_START_DATE:
            raise ValueError("dynamic v0.2 review start cannot predate 2021-02-22")
        return self


class DynamicV2ReviewSafety(BaseModel):
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
        if self.model_dump(mode="json") != DYNAMIC_V2_REVIEW_SAFETY:
            raise ValueError("dynamic v0.2 review safety fields are unsafe")
        return self


class DynamicV2ReviewThresholds(BaseModel):
    max_improvement_concentration_share: float = Field(ge=0, le=1)
    top_period_count: int = Field(gt=0)
    min_regime_trading_days: int = Field(gt=0)


class DynamicV2PositiveEvidenceThresholds(BaseModel):
    min_false_risk_off_reduction: int = Field(ge=0)
    max_turnover: float = Field(ge=0)
    min_static_delta_improvement_pp: float
    min_static_delta_after_pp: float


class DynamicV2BlockingConditions(BaseModel):
    max_constraint_hit_worsening: int = Field(ge=0)
    min_drawdown_preservation_pp: float
    max_dynamic_drawdown_delta_vs_static_pp: float
    max_false_risk_on_worsening: int = Field(ge=0)
    block_shadow_enrollment: Literal[True]


class DynamicV2ReviewPackagePolicy(BaseModel):
    default_candidate_policy_id: str = Field(min_length=1)
    review_status: Literal["review_candidate"]
    blocked_status: Literal["not_shadow_ready"]
    owner_review_required: Literal[True]
    constraint_review_required: Literal[True]
    drawdown_review_required: Literal[True]
    output_formats: list[Literal["json", "markdown"]] = Field(min_length=2)

    @model_validator(mode="after")
    def validate_formats(self) -> Self:
        if set(self.output_formats) != {"json", "markdown"}:
            raise ValueError("dynamic v0.2 review package must support json and markdown")
        return self


class DynamicV2ReviewPolicyConfig(BaseModel):
    schema_version: Literal["etf_dynamic_v2_review_policy_v1"]
    policy_metadata: PolicyMetadata
    market_regime: DynamicV2ReviewMarketRegime
    review_thresholds: DynamicV2ReviewThresholds
    positive_evidence_thresholds: DynamicV2PositiveEvidenceThresholds
    blocking_conditions: DynamicV2BlockingConditions
    comparison_targets: list[str] = Field(min_length=1)
    required_regimes: list[str] = Field(min_length=1)
    review_package_policy: DynamicV2ReviewPackagePolicy
    safety: DynamicV2ReviewSafety

    @model_validator(mode="after")
    def validate_policy(self) -> Self:
        if self.safety.model_dump(mode="json") != DYNAMIC_V2_REVIEW_SAFETY:
            raise ValueError("dynamic v0.2 review policy safety is unsafe")
        if len(self.comparison_targets) != len(set(self.comparison_targets)):
            raise ValueError("dynamic v0.2 comparison targets must be unique")
        if len(self.required_regimes) != len(set(self.required_regimes)):
            raise ValueError("dynamic v0.2 required regimes must be unique")
        return self


def load_dynamic_v2_review_policy_config(
    path: Path | str = DEFAULT_DYNAMIC_V2_REVIEW_POLICY_CONFIG_PATH,
) -> DynamicV2ReviewPolicyConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, Mapping):
        raise DynamicV2ReviewError("dynamic v0.2 review policy must be a mapping")
    try:
        return DynamicV2ReviewPolicyConfig.model_validate(raw)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV2ReviewError(f"invalid dynamic v0.2 review policy: {exc}") from exc


def build_v04_candidate_evidence(
    *,
    rescue_report: Mapping[str, Any],
    candidate_robustness_report: Mapping[str, Any],
    policy: DynamicV2ReviewPolicyConfig,
    shadow_package: Mapping[str, Any] | None = None,
    source_paths: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    if not rescue_report:
        raise DynamicV2ReviewError("dynamic rescue report is required")
    if not candidate_robustness_report:
        raise DynamicV2ReviewError("candidate dynamic robustness report is required")
    candidate_row = _candidate_rescue_row(rescue_report, policy)
    failed = _mapping(rescue_report.get("failed_v0_1_summary"))
    rescue_summary = _mapping(candidate_robustness_report.get("summary"))
    before_turnover = _float(failed.get("turnover"))
    after_turnover = _float(
        _comparison_row(candidate_robustness_report, "dynamic_candidate").get("turnover"),
        before_turnover - _float(candidate_row.get("turnover_reduction")),
    )
    before_false_off = _int(failed.get("false_risk_off_count"))
    after_false_off = _int(
        rescue_summary.get("false_risk_off_count"),
        before_false_off - _int(candidate_row.get("false_risk_off_reduction")),
    )
    before_false_on = _int(failed.get("false_risk_on_count"))
    after_false_on = _int(
        rescue_summary.get("false_risk_on_count"),
        before_false_on - _int(candidate_row.get("false_risk_on_reduction")),
    )
    before_constraint_count = _constraint_count_before(rescue_report)
    after_constraint_count = _int(
        _mapping(candidate_robustness_report.get("daily_path_summary")).get("constraint_hit_count"),
        before_constraint_count - _int(candidate_row.get("constraint_hit_reduction")),
    )
    source_report_paths = _source_report_paths(
        rescue_report=rescue_report,
        candidate_robustness_report=candidate_robustness_report,
        shadow_package=shadow_package or {},
        source_paths=source_paths or {},
        policy_path=DEFAULT_DYNAMIC_V2_REVIEW_POLICY_CONFIG_PATH,
    )
    warnings = []
    if not shadow_package:
        warnings.append("optional_dynamic_shadow_package_missing")
    evidence = {
        "candidate_id": "v0_4_lower_turnover",
        "rescue_policy_id": _text(candidate_row.get("policy_id")),
        "base_policy_id": _base_policy_id(rescue_report, candidate_row),
        "source_report_paths": source_report_paths,
        "static_delta_before": _float(failed.get("excess_vs_static_base")),
        "static_delta_after": _float(
            rescue_summary.get("excess_vs_static_base"),
            _float(candidate_row.get("underperformance_vs_static")),
        ),
        "static_delta_improvement": _float(candidate_row.get("return_vs_static_delta_improvement")),
        "false_risk_off_before": before_false_off,
        "false_risk_off_after": after_false_off,
        "false_risk_off_reduction": before_false_off - after_false_off,
        "false_risk_on_before": before_false_on,
        "false_risk_on_after": after_false_on,
        "false_risk_on_worsening": max(0, after_false_on - before_false_on),
        "turnover_before": before_turnover,
        "turnover_after": after_turnover,
        "turnover_reduction": before_turnover - after_turnover,
        "constraint_hit_count_before": before_constraint_count,
        "constraint_hit_count_after": after_constraint_count,
        "constraint_hit_delta": after_constraint_count - before_constraint_count,
        "drawdown_preservation": _float(candidate_row.get("drawdown_preservation")),
        "dynamic_max_drawdown_after": rescue_summary.get("dynamic_max_drawdown"),
        "data_quality_status": _text(
            rescue_summary.get("data_quality_status"),
            _text(_mapping(rescue_report.get("validation_context")).get("data_quality_status")),
        ),
        "warnings": warnings,
        "safety": policy.safety.model_dump(mode="json"),
        **DYNAMIC_V2_REVIEW_SAFETY,
    }
    _assert_dynamic_v2_review_payload_safe(evidence)
    return evidence


def build_rescue_improvement_attribution(
    *,
    evidence: Mapping[str, Any],
    rescue_report: Mapping[str, Any],
    candidate_robustness_report: Mapping[str, Any],
    policy: DynamicV2ReviewPolicyConfig,
) -> dict[str, Any]:
    positive = _positive_reason_codes(evidence, policy)
    negative = _negative_reason_codes(evidence, candidate_robustness_report, policy)
    regimes = _records(
        _mapping(candidate_robustness_report.get("regime_attribution")).get("regimes")
    )
    windows = _records(candidate_robustness_report.get("walk_forward", {}))
    if not windows:
        windows = _records(_mapping(candidate_robustness_report.get("walk_forward")).get("windows"))
    concentration = _improvement_concentration(regimes or windows)
    rows = _candidate_daily_records(candidate_robustness_report)
    failed_rows = _records(_mapping(rescue_report.get("failure_dataset")).get("rows"))
    attribution = {
        "candidate_id": _text(evidence.get("candidate_id")),
        "improvement_summary": {
            "static_delta_improvement": evidence.get("static_delta_improvement"),
            "false_risk_off_reduction": evidence.get("false_risk_off_reduction"),
            "turnover_reduction": evidence.get("turnover_reduction"),
            "constraint_hit_delta": evidence.get("constraint_hit_delta"),
            "drawdown_preservation": evidence.get("drawdown_preservation"),
        },
        "false_risk_off_reduction_contribution": evidence.get("false_risk_off_reduction"),
        "turnover_reduction_contribution": evidence.get("turnover_reduction"),
        "static_delta_improvement": evidence.get("static_delta_improvement"),
        "regime_switch_reduction_contribution": _regime_switch_reduction_proxy(
            failed_rows,
            rows,
        ),
        "allocation_path_change_contribution": _allocation_path_change(rows),
        "cash_drag_change": _cash_drag_change(failed_rows, rows),
        "QQQ_SMH_exposure_restoration": _growth_exposure_restoration(failed_rows, rows),
        "main_positive_drivers": positive,
        "main_negative_drivers": negative,
        "improvement_by_regime": _improvement_by_regime(candidate_robustness_report),
        "improvement_by_period": _improvement_by_period(candidate_robustness_report, policy),
        "improvement_concentration_share": concentration,
        "improvement_concentration_warning": (
            concentration > policy.review_thresholds.max_improvement_concentration_share
        ),
        "safety": policy.safety.model_dump(mode="json"),
    }
    return attribution


def build_constraint_hit_decomposition(
    *,
    evidence: Mapping[str, Any],
    rescue_report: Mapping[str, Any],
    candidate_robustness_report: Mapping[str, Any],
    policy: DynamicV2ReviewPolicyConfig,
) -> dict[str, Any]:
    before_rows = _records(_mapping(rescue_report.get("failure_dataset")).get("rows"))
    after_rows = _candidate_daily_records(candidate_robustness_report)
    before_by_type = _constraint_hits_by_type(before_rows)
    after_by_type = _constraint_hits_by_type(after_rows)
    for key in REQUIRED_CONSTRAINT_TYPES:
        before_by_type.setdefault(key, 0)
        after_by_type.setdefault(key, 0)
    after_count = _int(evidence.get("constraint_hit_count_after"))
    before_count = _int(evidence.get("constraint_hit_count_before"))
    row_count_after = max(
        1,
        len(after_rows)
        or _int(_mapping(candidate_robustness_report.get("daily_path_summary")).get("row_count")),
    )
    row_count_before = max(1, len(before_rows))
    delta = _int(evidence.get("constraint_hit_delta"))
    decomposition = {
        "candidate_id": _text(evidence.get("candidate_id")),
        "constraint_hit_count_before": before_count,
        "constraint_hit_count_after": after_count,
        "constraint_hit_delta": delta,
        "constraint_hit_rate_before": before_count / row_count_before,
        "constraint_hit_rate_after": after_count / row_count_after,
        "constraint_hit_by_type": {
            key: {
                "before": before_by_type.get(key, 0),
                "after": after_by_type.get(key, 0),
                "delta": after_by_type.get(key, 0) - before_by_type.get(key, 0),
            }
            for key in REQUIRED_CONSTRAINT_TYPES
        },
        "top_constraint_hit_periods": _top_constraint_periods(after_rows, policy),
        "constraint_root_cause": _constraint_root_cause(after_by_type),
        "recommended_constraint_review": _constraint_review_actions(delta, after_by_type),
        "blocker": delta > policy.blocking_conditions.max_constraint_hit_worsening,
        "blocker_reason_code": (
            "CONSTRAINT_HIT_WORSENED"
            if delta > policy.blocking_conditions.max_constraint_hit_worsening
            else ""
        ),
        "safety": policy.safety.model_dump(mode="json"),
    }
    return decomposition


def build_drawdown_preservation_failure_review(
    *,
    evidence: Mapping[str, Any],
    candidate_robustness_report: Mapping[str, Any],
    policy: DynamicV2ReviewPolicyConfig,
) -> dict[str, Any]:
    dynamic = _comparison_row(candidate_robustness_report, "dynamic_candidate")
    static = _comparison_row(candidate_robustness_report, "static_base_candidate")
    rows = _candidate_daily_records(candidate_robustness_report)
    preservation = _float(evidence.get("drawdown_preservation"))
    failed = preservation < policy.blocking_conditions.min_drawdown_preservation_pp / 100.0
    review = {
        "candidate_id": _text(evidence.get("candidate_id")),
        "max_drawdown_before": _max_drawdown_before(evidence, candidate_robustness_report),
        "max_drawdown_after": dynamic.get("max_drawdown"),
        "static_max_drawdown": static.get("max_drawdown"),
        "drawdown_preservation_delta": preservation,
        "drawdown_failure_status": "FAILED" if failed else "PASS",
        "drawdown_failure_periods": _worst_drawdown_periods(rows, policy),
        "weights_during_drawdown": _weights_during_worst_drawdown(rows),
        "regime_state_during_drawdown": _regime_during_worst_drawdown(rows),
        "risk_off_state_during_drawdown": _risk_off_state_during_drawdown(rows),
        "did_lower_turnover_delay_defense": failed
        and _float(evidence.get("turnover_after")) < _float(evidence.get("turnover_before")),
        "did_false_risk_off_fix_overcorrect": failed
        and _int(evidence.get("false_risk_off_reduction"))
        >= policy.positive_evidence_thresholds.min_false_risk_off_reduction,
        "drawdown_root_cause": _drawdown_root_cause(failed, evidence, rows),
        "risk_control_tradeoff": _drawdown_tradeoff(evidence),
        "recommended_drawdown_guardrails": _drawdown_guardrails(failed),
        "blocker": failed,
        "blocker_reason_code": "DRAWDOWN_PRESERVATION_FAILED" if failed else "",
        "safety": policy.safety.model_dump(mode="json"),
    }
    return review


def build_regime_robustness_review(
    *,
    evidence: Mapping[str, Any],
    candidate_robustness_report: Mapping[str, Any],
    policy: DynamicV2ReviewPolicyConfig,
) -> dict[str, Any]:
    rows_by_regime = {
        _text(row.get("regime")): row
        for row in _records(
            _mapping(candidate_robustness_report.get("regime_attribution")).get("regimes")
        )
    }
    daily = _candidate_daily_records(candidate_robustness_report)
    false_by_regime = _false_signal_counts_by_regime(daily)
    constraint_by_regime = _constraint_counts_by_regime(daily)
    regime_rows = []
    for regime in policy.required_regimes:
        source = rows_by_regime.get(regime, {})
        trading_days = _int(source.get("trading_days"))
        missing = not source
        constraint_count = constraint_by_regime.get(regime, 0)
        regime_rows.append(
            {
                "regime": regime,
                "status": "MISSING" if missing else _regime_status(source, policy),
                "return": source.get("total_return"),
                "return_vs_static": source.get("excess_vs_static_base"),
                "drawdown": source.get("max_drawdown"),
                "turnover": source.get("turnover"),
                "constraint_hit_rate": (
                    0.0 if trading_days <= 0 else constraint_count / trading_days
                ),
                "false_risk_off_count": false_by_regime.get(regime, {}).get(
                    "false_risk_off_count",
                    0,
                ),
                "false_risk_on_count": false_by_regime.get(regime, {}).get(
                    "false_risk_on_count",
                    0,
                ),
                "allocation_path_stability": _allocation_path_stability(source),
                "trading_days": trading_days,
            }
        )
    return {
        "candidate_id": _text(evidence.get("candidate_id")),
        "required_regime_count": len(policy.required_regimes),
        "available_regime_count": len([row for row in regime_rows if row["status"] != "MISSING"]),
        "regime_metrics": regime_rows,
        "weak_regimes": [
            row["regime"]
            for row in regime_rows
            if row["status"] in {"MISSING", "WEAK", "INSUFFICIENT_DAYS"}
        ],
        "improvement_broadness_status": _broadness_status(regime_rows),
        "safety": policy.safety.model_dump(mode="json"),
    }


def build_benchmark_comparison(
    *,
    evidence: Mapping[str, Any],
    rescue_report: Mapping[str, Any],
    candidate_robustness_report: Mapping[str, Any],
    policy: DynamicV2ReviewPolicyConfig,
) -> dict[str, Any]:
    rows = [_failed_v01_comparison_row(rescue_report)]
    dynamic = _comparison_row(candidate_robustness_report, "dynamic_candidate")
    rows.append(_comparison_payload_row("v0_4_lower_turnover", dynamic, candidate=True))
    for target in policy.comparison_targets:
        if target == "failed_v0_1":
            continue
        rows.append(
            _comparison_payload_row(
                target,
                _comparison_row(candidate_robustness_report, target),
            )
        )
    return {
        "candidate_id": _text(evidence.get("candidate_id")),
        "comparison_targets": policy.comparison_targets,
        "comparison_table": rows,
        "comparison_status": "AVAILABLE" if len(rows) > 1 else "MISSING",
        "does_not_imply_approval": True,
        "safety": policy.safety.model_dump(mode="json"),
    }


def build_shadow_review_eligibility_gate(
    *,
    evidence: Mapping[str, Any],
    improvement_attribution: Mapping[str, Any],
    constraint_decomposition: Mapping[str, Any],
    drawdown_review: Mapping[str, Any],
    benchmark_comparison: Mapping[str, Any],
    policy: DynamicV2ReviewPolicyConfig,
) -> dict[str, Any]:
    positive_codes = _positive_reason_codes(evidence, policy)
    blockers = _eligibility_blockers(
        evidence=evidence,
        improvement_attribution=improvement_attribution,
        drawdown_review=drawdown_review,
        benchmark_comparison=benchmark_comparison,
        policy=policy,
    )
    if (
        constraint_decomposition.get("blocker") is True
        and "CONSTRAINT_HIT_WORSENED" not in blockers
    ):
        blockers.append("CONSTRAINT_HIT_WORSENED")
    status = policy.review_package_policy.blocked_status if blockers else "owner_review_required"
    return {
        "candidate_id": _text(evidence.get("candidate_id")),
        "status": status,
        "review_status": policy.review_package_policy.review_status,
        "owner_review_required": True,
        "constraint_review_required": "CONSTRAINT_HIT_WORSENED" in blockers,
        "drawdown_review_required": "DRAWDOWN_PRESERVATION_FAILED" in blockers,
        "positive_reason_codes": positive_codes,
        "blocking_reason_codes": blockers,
        "positive_evidence_thresholds_met": len(positive_codes) >= 3,
        "constraint_hit_worsened_materially": "CONSTRAINT_HIT_WORSENED" in blockers,
        "drawdown_preservation_negative": "DRAWDOWN_PRESERVATION_FAILED" in blockers,
        "dynamic_drawdown_worse_than_static_by_threshold": (
            "DYNAMIC_DRAWDOWN_WORSE_THAN_STATIC" in blockers
        ),
        "improvement_concentrated_in_few_slices": (
            "IMPROVEMENT_CONCENTRATED_IN_FEW_SLICES" in blockers
        ),
        "false_risk_on_worsened_materially": "FALSE_RISK_ON_WORSENED" in blockers,
        "data_quality_status": evidence.get("data_quality_status"),
        "production_effect": "none",
        "broker_action": "none",
        "shadow_enrollment_allowed": False,
        "automatic_enrollment_allowed": False,
        "owner_approval_executed": False,
        "safety": policy.safety.model_dump(mode="json"),
    }


def build_dynamic_v2_review_package(
    *,
    rescue_report: Mapping[str, Any],
    candidate_robustness_report: Mapping[str, Any],
    policy: DynamicV2ReviewPolicyConfig | None = None,
    shadow_package: Mapping[str, Any] | None = None,
    source_paths: Mapping[str, str] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or load_dynamic_v2_review_policy_config()
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    evidence = build_v04_candidate_evidence(
        rescue_report=rescue_report,
        candidate_robustness_report=candidate_robustness_report,
        policy=resolved_policy,
        shadow_package=shadow_package,
        source_paths=source_paths,
    )
    improvement = build_rescue_improvement_attribution(
        evidence=evidence,
        rescue_report=rescue_report,
        candidate_robustness_report=candidate_robustness_report,
        policy=resolved_policy,
    )
    constraints = build_constraint_hit_decomposition(
        evidence=evidence,
        rescue_report=rescue_report,
        candidate_robustness_report=candidate_robustness_report,
        policy=resolved_policy,
    )
    drawdown = build_drawdown_preservation_failure_review(
        evidence=evidence,
        candidate_robustness_report=candidate_robustness_report,
        policy=resolved_policy,
    )
    regime = build_regime_robustness_review(
        evidence=evidence,
        candidate_robustness_report=candidate_robustness_report,
        policy=resolved_policy,
    )
    comparison = build_benchmark_comparison(
        evidence=evidence,
        rescue_report=rescue_report,
        candidate_robustness_report=candidate_robustness_report,
        policy=resolved_policy,
    )
    eligibility = build_shadow_review_eligibility_gate(
        evidence=evidence,
        improvement_attribution=improvement,
        constraint_decomposition=constraints,
        drawdown_review=drawdown,
        benchmark_comparison=comparison,
        policy=resolved_policy,
    )
    package_id = _stable_id(
        "dynamic-v2-review-package",
        evidence.get("rescue_policy_id"),
        rescue_report.get("dynamic_rescue_report_id"),
        candidate_robustness_report.get("dynamic_robustness_report_id"),
    )
    payload = {
        "schema_version": DYNAMIC_V2_REVIEW_PACKAGE_SCHEMA_VERSION,
        "report_type": DYNAMIC_V2_REVIEW_PACKAGE_REPORT_TYPE,
        "review_package_id": package_id,
        "generated_at": generated.isoformat(),
        "status": eligibility["status"],
        "review_status": eligibility["review_status"],
        "policy_version": resolved_policy.policy_metadata.version,
        "policy_config_hash": _stable_hash(resolved_policy.model_dump(mode="json")),
        "market_regime": resolved_policy.market_regime.model_dump(mode="json"),
        "safety_banner": _safety_status(resolved_policy.safety.model_dump(mode="json")),
        "review_metadata": {
            "stage": "TRADING-089",
            "candidate": evidence["candidate_id"],
            "rescue_policy_id": evidence["rescue_policy_id"],
            "review_only": True,
            "owner_review_required": True,
        },
        "candidate_evidence": evidence,
        "rescue_improvement_attribution": improvement,
        "constraint_hit_decomposition": constraints,
        "drawdown_preservation_failure_review": drawdown,
        "regime_robustness_review": regime,
        "benchmark_comparison": comparison,
        "shadow_review_eligibility_gate": eligibility,
        "blockers": eligibility["blocking_reason_codes"],
        "warnings": sorted(set(_texts(evidence.get("warnings")) + _package_warnings(regime))),
        "recommended_next_actions": _recommended_next_actions(eligibility),
        "source_links": evidence["source_report_paths"],
        "validation_context": {
            "data_quality_status": evidence.get("data_quality_status"),
            "review_only": True,
            "runtime_artifacts_untracked": True,
        },
        "safety": resolved_policy.safety.model_dump(mode="json"),
        **DYNAMIC_V2_REVIEW_SAFETY,
        "commands_executed": False,
        "shadow_enrollment_allowed": False,
        "automatic_enrollment_allowed": False,
        "owner_approval_executed": False,
    }
    _assert_dynamic_v2_review_payload_safe(payload)
    return payload


def build_dynamic_v2_review_validation_report(
    *,
    config_path: Path | str = DEFAULT_DYNAMIC_V2_REVIEW_POLICY_CONFIG_PATH,
    report_registry_path: Path = PROJECT_ROOT / "config" / "report_registry.yaml",
    reader_brief_path: Path = PROJECT_ROOT
    / "src"
    / "ai_trading_system"
    / "reports"
    / "reader_brief.py",
    registration_path: Path = PROJECT_ROOT
    / "src"
    / "ai_trading_system"
    / "interfaces"
    / "cli"
    / "etf_portfolio"
    / "registration.py",
    command_owner_path: Path = PROJECT_ROOT
    / "src"
    / "ai_trading_system"
    / "interfaces"
    / "cli"
    / "etf_portfolio"
    / "dynamic_v2_review.py",
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    checks: list[dict[str, Any]] = []
    policy: DynamicV2ReviewPolicyConfig | None = None
    try:
        policy = load_dynamic_v2_review_policy_config(config_path)
        _append_check(checks, "review_policy_valid", True, "dynamic v0.2 review policy loads")
    except Exception as exc:  # noqa: BLE001
        _append_check(checks, "review_policy_valid", False, str(exc))
    if policy is not None:
        try:
            sample_inputs = _sample_dynamic_v2_review_inputs(policy)
            package = build_dynamic_v2_review_package(
                rescue_report=sample_inputs["rescue_report"],
                candidate_robustness_report=sample_inputs["candidate_robustness_report"],
                shadow_package=sample_inputs["shadow_package"],
                policy=policy,
                source_paths={"dynamic_rescue_report": "validation_sample"},
                generated_at=generated,
            )
            _append_check(
                checks,
                "v04_evidence_loader_available",
                bool(package.get("candidate_evidence")),
                "v0.4 evidence loader available",
            )
            _append_check(
                checks,
                "improvement_attribution_available",
                bool(package.get("rescue_improvement_attribution")),
                "rescue improvement attribution available",
            )
            _append_check(
                checks,
                "constraint_decomposition_available",
                bool(package.get("constraint_hit_decomposition")),
                "constraint hit decomposition available",
            )
            _append_check(
                checks,
                "drawdown_failure_review_available",
                bool(package.get("drawdown_preservation_failure_review")),
                "drawdown failure review available",
            )
            _append_check(
                checks,
                "regime_robustness_available",
                bool(package.get("regime_robustness_review")),
                "regime robustness review available",
            )
            _append_check(
                checks,
                "benchmark_comparison_available",
                bool(package.get("benchmark_comparison")),
                "benchmark comparison available",
            )
            gate = _mapping(package.get("shadow_review_eligibility_gate"))
            _append_check(
                checks,
                "eligibility_gate_blocks_v04",
                gate.get("status") == "not_shadow_ready"
                and "CONSTRAINT_HIT_WORSENED" in _texts(gate.get("blocking_reason_codes"))
                and "DRAWDOWN_PRESERVATION_FAILED" in _texts(gate.get("blocking_reason_codes")),
                "v0.4 improvements are recognized but blockers prevent shadow-ready status",
            )
            _append_check(
                checks,
                "review_package_generator_available",
                package.get("report_type") == DYNAMIC_V2_REVIEW_PACKAGE_REPORT_TYPE,
                "review package generator available",
            )
            _assert_dynamic_v2_review_payload_safe(package)
            _append_check(checks, "sample_package_safety", True, "sample package is safe")
        except Exception as exc:  # noqa: BLE001
            _append_check(checks, "validation_sample_workflow", False, str(exc))
        _append_check(
            checks,
            "safety_boundary_safe",
            policy.safety.model_dump(mode="json") == DYNAMIC_V2_REVIEW_SAFETY,
            "production_effect=none; broker_action=none; no enrollment",
        )
    registry_text = _safe_read_text(report_registry_path)
    _append_check(
        checks,
        "report_registry_visibility",
        DYNAMIC_V2_REVIEW_PACKAGE_REPORT_TYPE in registry_text
        and DYNAMIC_V2_REVIEW_VALIDATION_REPORT_TYPE in registry_text,
        "report registry exposes dynamic v0.2 review package and validation",
    )
    reader_text = _safe_read_text(reader_brief_path)
    _append_check(
        checks,
        "reader_brief_integration_available",
        "Dynamic v0.2 Review" in reader_text and "_etf_dynamic_v2_review_summary" in reader_text,
        "Reader Brief has Dynamic v0.2 Review section",
    )
    registration_text = _safe_read_text(registration_path)
    command_owner_text = _safe_read_text(command_owner_path)
    _append_check(
        checks,
        "cli_namespace_available",
        "dynamic-v2-review" in registration_text
        and "dynamic_v2_review_app" in command_owner_text,
        "CLI exposes dynamic-v2-review namespace",
    )
    failed = [check for check in checks if check["status"] != "PASS"]
    payload = {
        "schema_version": DYNAMIC_V2_REVIEW_VALIDATION_SCHEMA_VERSION,
        "report_type": DYNAMIC_V2_REVIEW_VALIDATION_REPORT_TYPE,
        "validation_id": _stable_id(
            "dynamic-v2-review-validation",
            generated.strftime("%Y%m%dT%H%M%SZ"),
            _stable_hash([check["check_id"] for check in checks]),
        ),
        "generated_at": generated.isoformat(),
        "status": "PASS" if not failed else "FAIL",
        "check_count": len(checks),
        "failed_check_count": len(failed),
        "checks": checks,
        "source_schema_versions": {
            "policy": DYNAMIC_V2_REVIEW_POLICY_SCHEMA_VERSION,
            "package": DYNAMIC_V2_REVIEW_PACKAGE_SCHEMA_VERSION,
        },
        "production_effect_none_required": True,
        "broker_action_none_required": True,
        "manual_review_required": True,
        "no_auto_approval": True,
        "no_auto_enrollment": True,
        "review_package_mutates_production_state": False,
        "shadow_enrollment_allowed": False,
        "automatic_enrollment_allowed": False,
        "owner_approval_executed": False,
        "commands_executed": False,
        "safety": dict(DYNAMIC_V2_REVIEW_SAFETY),
        **DYNAMIC_V2_REVIEW_SAFETY,
    }
    _assert_dynamic_v2_review_payload_safe(payload)
    return payload


def write_dynamic_v2_review_package(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_DYNAMIC_V2_REVIEW_PACKAGE_DIR,
) -> dict[str, Path]:
    return _write_json_md(
        payload,
        output_dir=output_dir,
        stem=_text(payload.get("review_package_id"), "dynamic-v2-review-package"),
        markdown=render_dynamic_v2_review_package_markdown(payload),
    )


def write_dynamic_v2_review_validation_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_DYNAMIC_V2_REVIEW_VALIDATION_DIR,
) -> dict[str, Path]:
    return _write_json_md(
        payload,
        output_dir=output_dir,
        stem=_text(payload.get("validation_id"), "dynamic-v2-review-validation"),
        markdown=render_dynamic_v2_review_validation_markdown(payload),
    )


def latest_dynamic_v2_review_package_path(
    package_dir: Path = DEFAULT_DYNAMIC_V2_REVIEW_PACKAGE_DIR,
) -> Path | None:
    return _latest_json(package_dir, "dynamic-v2-review-package_*.json")


def load_json_artifact(path: Path | None, *, label: str) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DynamicV2ReviewError(f"{label} JSON is invalid: {path}") from exc
    if not isinstance(payload, Mapping):
        raise DynamicV2ReviewError(f"{label} root must be a JSON object: {path}")
    return dict(payload)


def load_latest_review_inputs(
    *,
    rescue_report_path: Path | None = None,
    candidate_robustness_report_path: Path | None = None,
    shadow_package_path: Path | None = None,
    rescue_report_dir: Path = DEFAULT_DYNAMIC_RESCUE_REPORT_DIR,
    candidate_robustness_report_dir: Path = DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
    shadow_package_dir: Path = DEFAULT_DYNAMIC_SHADOW_PACKAGE_DIR,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, str]]:
    resolved_rescue = rescue_report_path or latest_dynamic_rescue_report_path(rescue_report_dir)
    resolved_robustness = candidate_robustness_report_path or latest_dynamic_robustness_report_path(
        candidate_robustness_report_dir
    )
    resolved_shadow = shadow_package_path or latest_dynamic_shadow_review_package_path(
        shadow_package_dir
    )
    rescue = load_json_artifact(resolved_rescue, label="dynamic rescue report")
    robustness = load_json_artifact(resolved_robustness, label="candidate robustness report")
    shadow = load_json_artifact(resolved_shadow, label="dynamic shadow package")
    source_paths = {
        "dynamic_rescue_report": "" if resolved_rescue is None else str(resolved_rescue),
        "dynamic_robustness_report": (
            "" if resolved_robustness is None else str(resolved_robustness)
        ),
        "dynamic_shadow_package": "" if resolved_shadow is None else str(resolved_shadow),
    }
    return rescue, robustness, shadow, source_paths


def render_dynamic_v2_review_package_markdown(payload: Mapping[str, Any]) -> str:
    evidence = _mapping(payload.get("candidate_evidence"))
    gate = _mapping(payload.get("shadow_review_eligibility_gate"))
    constraints = _mapping(payload.get("constraint_hit_decomposition"))
    drawdown = _mapping(payload.get("drawdown_preservation_failure_review"))
    comparison = _records(_mapping(payload.get("benchmark_comparison")).get("comparison_table"))
    lines = [
        f"# Dynamic v0.2 Review Package {payload.get('review_package_id')}",
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
        "## Review Metadata",
        f"- Candidate: {evidence.get('candidate_id')}",
        f"- Rescue Policy: {evidence.get('rescue_policy_id')}",
        f"- Status: {payload.get('review_status')} / {payload.get('status')}",
        f"- Data Quality: {evidence.get('data_quality_status')}",
        "",
        "## Positive Improvement Evidence",
        f"- false_risk_off_reduction: {evidence.get('false_risk_off_reduction')}",
        f"- turnover: {_fmt_num(evidence.get('turnover_before'))} -> "
        f"{_fmt_num(evidence.get('turnover_after'))}",
        f"- static_delta: {_fmt_pct(evidence.get('static_delta_before'))} -> "
        f"{_fmt_pct(evidence.get('static_delta_after'))}",
        "",
        "## Blockers",
    ]
    for blocker in _texts(gate.get("blocking_reason_codes")):
        lines.append(f"- {blocker}")
    lines.extend(
        [
            "",
            "## Constraint Hit Decomposition",
            f"- before: {constraints.get('constraint_hit_count_before')}",
            f"- after: {constraints.get('constraint_hit_count_after')}",
            f"- delta: {constraints.get('constraint_hit_delta')}",
            f"- root_cause: {constraints.get('constraint_root_cause')}",
            "",
            "## Drawdown Preservation Failure Review",
            f"- status: {drawdown.get('drawdown_failure_status')}",
            f"- preservation_delta: {_fmt_pct(drawdown.get('drawdown_preservation_delta'))}",
            f"- root_cause: {drawdown.get('drawdown_root_cause')}",
            "",
            "## Benchmark Comparison",
            "",
            "| Target | Total Return | Max Drawdown | Turnover | Constraint Hits |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in comparison:
        lines.append(
            "| "
            f"{row.get('comparison_id')} | "
            f"{_fmt_pct(row.get('total_return'))} | "
            f"{_fmt_pct(row.get('max_drawdown'))} | "
            f"{_fmt_num(row.get('turnover'))} | "
            f"{row.get('constraint_hit_count')} |"
        )
    lines.extend(
        [
            "",
            "## Recommended Next Actions",
        ]
    )
    for action in _texts(payload.get("recommended_next_actions")):
        lines.append(f"- {action}")
    lines.extend(
        [
            "",
            "## Source Links",
        ]
    )
    for key, value in _mapping(payload.get("source_links")).items():
        lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def render_dynamic_v2_review_validation_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# Dynamic v0.2 Review Validation {payload.get('validation_id')}",
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


def _candidate_rescue_row(
    rescue_report: Mapping[str, Any],
    policy: DynamicV2ReviewPolicyConfig,
) -> dict[str, Any]:
    target = policy.review_package_policy.default_candidate_policy_id
    for row in _records(rescue_report.get("rescue_candidate_comparison")):
        if (
            _text(row.get("policy_id")) == target
            or _text(row.get("template_type")) == "lower_turnover"
        ):
            return row
    raise DynamicV2ReviewError(f"v0.4 lower_turnover rescue row not found: {target}")


def _source_report_paths(
    *,
    rescue_report: Mapping[str, Any],
    candidate_robustness_report: Mapping[str, Any],
    shadow_package: Mapping[str, Any],
    source_paths: Mapping[str, str],
    policy_path: Path,
) -> dict[str, str]:
    rescue_links = _mapping(rescue_report.get("source_links"))
    robustness_sources = _mapping(candidate_robustness_report.get("source_artifacts"))
    shadow_sources = _mapping(shadow_package.get("source_artifacts"))
    return {
        "dynamic_rescue_report": _text(source_paths.get("dynamic_rescue_report")),
        "dynamic_robustness_report": _text(source_paths.get("dynamic_robustness_report")),
        "dynamic_shadow_package": _text(source_paths.get("dynamic_shadow_package")),
        "dynamic_allocation_decision_records": _text(
            robustness_sources.get("dynamic_allocation_decision_records")
            or rescue_links.get("dynamic_allocation_decision_records")
        ),
        "data_quality_report": _text(
            robustness_sources.get("data_quality_report")
            or rescue_links.get("data_quality_report")
            or shadow_sources.get("data_quality_report")
        ),
        "operations_report": _text(
            source_paths.get("operations_report") or shadow_sources.get("operations_validation")
        ),
        "source_candidate_pack": _text(
            robustness_sources.get("dynamic_calibration_report")
            or rescue_links.get("dynamic_calibration_report")
        ),
        "source_policy_config": str(policy_path),
    }


def _base_policy_id(
    rescue_report: Mapping[str, Any],
    candidate_row: Mapping[str, Any],
) -> str:
    for row in _records(rescue_report.get("rescue_policy_templates")):
        if _text(row.get("policy_id")) == _text(candidate_row.get("policy_id")):
            return _text(row.get("base_policy_id"), "dynamic_regime_overlay_v0_1")
    return "dynamic_regime_overlay_v0_1"


def _constraint_count_before(rescue_report: Mapping[str, Any]) -> int:
    turnover = _mapping(rescue_report.get("turnover_constraint_breakdown"))
    return _int(turnover.get("constraint_hit_count"))


def _positive_reason_codes(
    evidence: Mapping[str, Any],
    policy: DynamicV2ReviewPolicyConfig,
) -> list[str]:
    thresholds = policy.positive_evidence_thresholds
    codes: list[str] = []
    if _int(evidence.get("false_risk_off_reduction")) >= thresholds.min_false_risk_off_reduction:
        codes.append("FALSE_RISK_OFF_REDUCED")
    if _float(evidence.get("turnover_after")) <= thresholds.max_turnover:
        codes.append("TURNOVER_REDUCED")
    if _float(evidence.get("static_delta_after")) >= thresholds.min_static_delta_after_pp / 100.0:
        codes.append("STATIC_UNDERPERFORMANCE_REVERSED")
    if (
        _float(evidence.get("static_delta_improvement"))
        >= thresholds.min_static_delta_improvement_pp / 100.0
    ):
        codes.append("RESCUE_IMPROVEMENT_MATERIAL")
    return codes


def _negative_reason_codes(
    evidence: Mapping[str, Any],
    candidate_robustness_report: Mapping[str, Any],
    policy: DynamicV2ReviewPolicyConfig,
) -> list[str]:
    codes = []
    if (
        _int(evidence.get("constraint_hit_delta"))
        > policy.blocking_conditions.max_constraint_hit_worsening
    ):
        codes.append("CONSTRAINT_HIT_WORSENED")
    if (
        _float(evidence.get("drawdown_preservation"))
        < policy.blocking_conditions.min_drawdown_preservation_pp / 100.0
    ):
        codes.append("DRAWDOWN_PRESERVATION_FAILED")
        codes.append("DRAWDOWN_REVIEW_REQUIRED")
    if _dynamic_drawdown_worse_than_static(candidate_robustness_report, policy):
        codes.append("DYNAMIC_DRAWDOWN_WORSE_THAN_STATIC")
    codes.append("SHADOW_ENROLLMENT_NOT_ALLOWED")
    return sorted(set(codes))


def _eligibility_blockers(
    *,
    evidence: Mapping[str, Any],
    improvement_attribution: Mapping[str, Any],
    drawdown_review: Mapping[str, Any],
    benchmark_comparison: Mapping[str, Any],
    policy: DynamicV2ReviewPolicyConfig,
) -> list[str]:
    blockers = []
    if (
        _int(evidence.get("constraint_hit_delta"))
        > policy.blocking_conditions.max_constraint_hit_worsening
    ):
        blockers.append("CONSTRAINT_HIT_WORSENED")
    if drawdown_review.get("blocker") is True:
        blockers.append("DRAWDOWN_PRESERVATION_FAILED")
        blockers.append("DRAWDOWN_REVIEW_REQUIRED")
    dynamic_row = next(
        (
            row
            for row in _records(benchmark_comparison.get("comparison_table"))
            if row.get("comparison_id") == "v0_4_lower_turnover"
        ),
        {},
    )
    static_row = next(
        (
            row
            for row in _records(benchmark_comparison.get("comparison_table"))
            if row.get("comparison_id") == "static_base_candidate"
        ),
        {},
    )
    if _drawdown_delta_worse(dynamic_row, static_row, policy):
        blockers.append("DYNAMIC_DRAWDOWN_WORSE_THAN_STATIC")
    if improvement_attribution.get("improvement_concentration_warning") is True:
        blockers.append("IMPROVEMENT_CONCENTRATED_IN_FEW_SLICES")
    if (
        _int(evidence.get("false_risk_on_worsening"))
        > policy.blocking_conditions.max_false_risk_on_worsening
    ):
        blockers.append("FALSE_RISK_ON_WORSENED")
    blockers.append("SHADOW_ENROLLMENT_NOT_ALLOWED")
    return sorted(set(blockers))


def _dynamic_drawdown_worse_than_static(
    report: Mapping[str, Any],
    policy: DynamicV2ReviewPolicyConfig,
) -> bool:
    return _drawdown_delta_worse(
        _comparison_row(report, "dynamic_candidate"),
        _comparison_row(report, "static_base_candidate"),
        policy,
    )


def _drawdown_delta_worse(
    dynamic_row: Mapping[str, Any],
    static_row: Mapping[str, Any],
    policy: DynamicV2ReviewPolicyConfig,
) -> bool:
    if not dynamic_row or not static_row:
        return False
    delta = _float(dynamic_row.get("max_drawdown")) - _float(static_row.get("max_drawdown"))
    return delta < policy.blocking_conditions.max_dynamic_drawdown_delta_vs_static_pp / 100.0


def _candidate_daily_records(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    daily = _mapping(report.get("daily_path_summary"))
    records = _records(daily.get("records"))
    if records:
        return records
    paths = _mapping(report.get("comparison_daily_paths"))
    return _records(paths.get("dynamic_candidate"))


def _constraint_hits_by_type(rows: list[Mapping[str, Any]]) -> dict[str, int]:
    counts = {key: 0 for key in REQUIRED_CONSTRAINT_TYPES}
    for row in rows:
        for constraint in _constraints_from_row(row):
            key = _constraint_type(constraint)
            counts[key] = counts.get(key, 0) + 1
    return counts


def _constraints_from_row(row: Mapping[str, Any]) -> list[str]:
    explicit = row.get("constraint_hits")
    if isinstance(explicit, Sequence) and not isinstance(explicit, (str, bytes)):
        return [str(item) for item in explicit if str(item)]
    reason_codes = _reason_codes(row)
    return [code for code in reason_codes if _is_constraint_reason(code)]


def _reason_codes(row: Mapping[str, Any]) -> list[str]:
    value = row.get("reason_codes_json", row.get("reason_codes"))
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [str(item) for item in value]
    return _json_list(value)


def _is_constraint_reason(reason: str) -> bool:
    return reason.startswith(
        (
            "MAX_",
            "MIN_",
            "WEEKLY_TURNOVER_CAP",
            "REGIME_CONFIRMATION",
            "SINGLE_REBALANCE",
        )
    )


def _constraint_type(reason_code: str) -> str:
    upper = reason_code.upper()
    if "QQQ" in upper and "MAX" in upper:
        return "QQQ_max_hit"
    if "SEMICONDUCTOR" in upper or "SMH" in upper or "SOXX" in upper:
        return "semiconductor_cap_hit"
    if "CASH" in upper and "MAX" in upper:
        return "CASH_max_hit"
    if "CASH" in upper and "MIN" in upper:
        return "CASH_min_hit"
    if "SINGLE_REBALANCE" in upper or "MAX_SINGLE" in upper:
        return "single_rebalance_delta_hit"
    if "WEEKLY_TURNOVER_CAP" in upper:
        return "weekly_turnover_cap_hit"
    if "MINIMUM_HOLDING" in upper:
        return "minimum_holding_period_block"
    if "RISK" in upper:
        return "risk_overlay_conflict"
    if "TREND" in upper or "GROWTH" in upper:
        return "trend_overlay_conflict"
    return "trend_overlay_conflict"


def _top_constraint_periods(
    rows: list[Mapping[str, Any]],
    policy: DynamicV2ReviewPolicyConfig,
) -> list[dict[str, Any]]:
    periods = []
    for row in rows:
        constraints = _constraints_from_row(row)
        if not constraints:
            continue
        periods.append(
            {
                "date": _text(row.get("signal_date") or row.get("date")),
                "return_date": _text(row.get("return_date")),
                "regime_state": _text(row.get("selected_regime") or row.get("regime_state")),
                "constraint_hits": constraints,
                "turnover": _float(row.get("turnover")),
            }
        )
    return sorted(
        periods,
        key=lambda item: (len(item["constraint_hits"]), _float(item.get("turnover"))),
        reverse=True,
    )[: policy.review_thresholds.top_period_count]


def _constraint_root_cause(after_by_type: Mapping[str, int]) -> str:
    if not after_by_type:
        return "constraint_layer_review_required"
    top_type = max(after_by_type, key=lambda key: after_by_type.get(key, 0))
    if top_type in {"single_rebalance_delta_hit", "weekly_turnover_cap_hit"}:
        return "lower_turnover_rebalance_constraints_binding"
    if top_type in {"QQQ_max_hit", "semiconductor_cap_hit", "CASH_max_hit", "CASH_min_hit"}:
        return "target_allocation_conflicts_with_exposure_caps"
    return "overlay_constraint_interaction_review_required"


def _constraint_review_actions(delta: int, after_by_type: Mapping[str, int]) -> list[str]:
    actions = ["keep_shadow_enrollment_blocked_until_constraint_review_passes"]
    if delta > 0:
        actions.append("decompose_constraint_hits_before_relaxing_caps")
    if after_by_type.get("weekly_turnover_cap_hit", 0) or after_by_type.get(
        "single_rebalance_delta_hit",
        0,
    ):
        actions.append("review_lower_turnover_rebalance_gate_interaction")
    if after_by_type.get("QQQ_max_hit", 0) or after_by_type.get("semiconductor_cap_hit", 0):
        actions.append("review_growth_and_semiconductor_target_vs_caps")
    return actions


def _max_drawdown_before(
    evidence: Mapping[str, Any],
    candidate_robustness_report: Mapping[str, Any],
) -> float | None:
    after = _float(
        _comparison_row(candidate_robustness_report, "dynamic_candidate").get("max_drawdown")
    )
    preservation = _float(evidence.get("drawdown_preservation"))
    if after == 0 and preservation == 0:
        return None
    return after - preservation


def _worst_drawdown_periods(
    rows: list[Mapping[str, Any]],
    policy: DynamicV2ReviewPolicyConfig,
) -> list[dict[str, Any]]:
    curve = 1.0
    peak = 1.0
    periods = []
    for row in rows:
        curve *= 1.0 + _float(row.get("strategy_return"))
        peak = max(peak, curve)
        drawdown = curve / peak - 1.0 if peak > 0 else 0.0
        periods.append(
            {
                "date": _text(row.get("return_date") or row.get("date")),
                "drawdown": drawdown,
                "strategy_return": _float(row.get("strategy_return")),
                "regime_state": _text(row.get("selected_regime") or row.get("regime_state")),
                "target_weights": _target_weights(row),
            }
        )
    return sorted(periods, key=lambda item: _float(item.get("drawdown")))[
        : policy.review_thresholds.top_period_count
    ]


def _weights_during_worst_drawdown(rows: list[Mapping[str, Any]]) -> dict[str, float]:
    worst = _single_worst_drawdown_row(rows)
    return _target_weights(worst)


def _regime_during_worst_drawdown(rows: list[Mapping[str, Any]]) -> str:
    return _text(_single_worst_drawdown_row(rows).get("selected_regime"))


def _risk_off_state_during_drawdown(rows: list[Mapping[str, Any]]) -> str:
    regime = _regime_during_worst_drawdown(rows)
    return "risk_off" if regime == "risk_off" else "not_risk_off"


def _single_worst_drawdown_row(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    curve = 1.0
    peak = 1.0
    worst_row: Mapping[str, Any] = rows[0]
    worst_dd = 0.0
    for row in rows:
        curve *= 1.0 + _float(row.get("strategy_return"))
        peak = max(peak, curve)
        drawdown = curve / peak - 1.0 if peak > 0 else 0.0
        if drawdown < worst_dd:
            worst_dd = drawdown
            worst_row = row
    return dict(worst_row)


def _drawdown_root_cause(
    failed: bool,
    evidence: Mapping[str, Any],
    rows: list[Mapping[str, Any]],
) -> str:
    if not failed:
        return "drawdown_preservation_passed"
    worst_weights = _weights_during_worst_drawdown(rows)
    cash = _float(worst_weights.get("CASH"))
    if _float(evidence.get("turnover_after")) < _float(evidence.get("turnover_before")):
        return "lower_turnover_may_have_delayed_defensive_rebalance"
    if cash < 0.10:
        return "risk_off_defense_not_restored_during_drawdown"
    return "drawdown_guardrail_review_required"


def _drawdown_tradeoff(evidence: Mapping[str, Any]) -> str:
    if _float(evidence.get("drawdown_preservation")) < 0:
        return "false_risk_off_and_turnover_improvements_lost_drawdown_protection"
    return "drawdown_protection_preserved"


def _drawdown_guardrails(failed: bool) -> list[str]:
    if not failed:
        return ["retain_current_drawdown_guardrails_for_review"]
    return [
        "keep_shadow_enrollment_blocked_until_drawdown_guardrails_are_reviewed",
        "test_lower_turnover_with_explicit_drawdown_stop_or_risk_off_override",
        "review whether false risk-off reduction overcorrected defensive behavior",
    ]


def _improvement_by_regime(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "regime": row.get("regime"),
            "total_return": row.get("total_return"),
            "return_vs_static": row.get("excess_vs_static_base"),
            "turnover": row.get("turnover"),
            "max_drawdown": row.get("max_drawdown"),
            "trading_days": row.get("trading_days"),
        }
        for row in _records(_mapping(report.get("regime_attribution")).get("regimes"))
    ]


def _improvement_by_period(
    report: Mapping[str, Any],
    policy: DynamicV2ReviewPolicyConfig,
) -> list[dict[str, Any]]:
    windows = _records(_mapping(report.get("walk_forward")).get("windows"))
    if windows:
        return [
            {
                "start": row.get("start"),
                "end": row.get("end"),
                "total_return": row.get("total_return"),
                "max_drawdown": row.get("max_drawdown"),
                "status": row.get("status"),
            }
            for row in windows[: policy.review_thresholds.top_period_count]
        ]
    rows = _candidate_daily_records(report)
    periods = [
        {
            "date": _text(row.get("return_date")),
            "strategy_return": row.get("strategy_return"),
            "regime_state": _text(row.get("selected_regime")),
        }
        for row in rows
    ]
    return sorted(periods, key=lambda item: _float(item.get("strategy_return")), reverse=True)[
        : policy.review_thresholds.top_period_count
    ]


def _improvement_concentration(rows: list[Mapping[str, Any]]) -> float:
    values = [max(0.0, _float(row.get("total_return"))) for row in rows]
    total = sum(values)
    if total <= 0:
        return 0.0
    return max(values) / total


def _regime_switch_reduction_proxy(
    failed_rows: list[Mapping[str, Any]],
    candidate_rows: list[Mapping[str, Any]],
) -> int:
    return max(0, _regime_switch_count(failed_rows) - _regime_switch_count(candidate_rows))


def _regime_switch_count(rows: list[Mapping[str, Any]]) -> int:
    regimes = [_text(row.get("selected_regime") or row.get("regime_state")) for row in rows]
    return sum(1 for before, after in zip(regimes, regimes[1:], strict=False) if before != after)


def _allocation_path_change(rows: list[Mapping[str, Any]]) -> dict[str, float]:
    return {
        "average_turnover": _avg([_float(row.get("turnover")) for row in rows]),
        "average_cash_weight": _avg_weight(rows, "CASH"),
        "average_QQQ_weight": _avg_weight(rows, "QQQ"),
        "average_SMH_weight": _avg_weight(rows, "SMH"),
    }


def _cash_drag_change(
    failed_rows: list[Mapping[str, Any]],
    candidate_rows: list[Mapping[str, Any]],
) -> float:
    return _avg_weight(candidate_rows, "CASH") - _avg_weight(failed_rows, "CASH")


def _growth_exposure_restoration(
    failed_rows: list[Mapping[str, Any]],
    candidate_rows: list[Mapping[str, Any]],
) -> float:
    failed_growth = _avg_weight(failed_rows, "QQQ") + _avg_weight(failed_rows, "SMH")
    candidate_growth = _avg_weight(candidate_rows, "QQQ") + _avg_weight(candidate_rows, "SMH")
    return candidate_growth - failed_growth


def _avg_weight(rows: list[Mapping[str, Any]], symbol: str) -> float:
    weights = [_target_weights(row).get(symbol, 0.0) for row in rows]
    return _avg(weights)


def _target_weights(row: Mapping[str, Any]) -> dict[str, float]:
    raw = row.get("target_weights") or row.get("target_weights_json")
    if isinstance(raw, Mapping):
        return {str(key): _float(value) for key, value in raw.items()}
    return {key: _float(value) for key, value in _json_mapping(raw).items()}


def _false_signal_counts_by_regime(
    rows: list[Mapping[str, Any]],
) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for row in rows:
        regime = _text(row.get("selected_regime") or row.get("regime_state"), "unknown")
        item = result.setdefault(regime, {"false_risk_off_count": 0, "false_risk_on_count": 0})
        if row.get("false_risk_off") is True:
            item["false_risk_off_count"] += 1
        if row.get("false_risk_on") is True:
            item["false_risk_on_count"] += 1
    return result


def _constraint_counts_by_regime(rows: list[Mapping[str, Any]]) -> dict[str, int]:
    result: dict[str, int] = {}
    for row in rows:
        regime = _text(row.get("selected_regime") or row.get("regime_state"), "unknown")
        if _constraints_from_row(row):
            result[regime] = result.get(regime, 0) + 1
    return result


def _regime_status(row: Mapping[str, Any], policy: DynamicV2ReviewPolicyConfig) -> str:
    if _int(row.get("trading_days")) < policy.review_thresholds.min_regime_trading_days:
        return "INSUFFICIENT_DAYS"
    if _float(row.get("excess_vs_static_base")) < 0:
        return "WEAK"
    return "PASS"


def _allocation_path_stability(row: Mapping[str, Any]) -> float:
    return max(0.0, 1.0 - _float(row.get("turnover")))


def _broadness_status(rows: list[Mapping[str, Any]]) -> str:
    available = [row for row in rows if row.get("status") != "MISSING"]
    weak = [row for row in available if row.get("status") in {"WEAK", "INSUFFICIENT_DAYS"}]
    if not available:
        return "MISSING"
    if len(weak) > len(available) / 2:
        return "REGIME_SPECIFIC_WEAKNESS_VISIBLE"
    return "BROAD_REVIEW_AVAILABLE"


def _failed_v01_comparison_row(rescue_report: Mapping[str, Any]) -> dict[str, Any]:
    failed = _mapping(rescue_report.get("failed_v0_1_summary"))
    return {
        "comparison_id": "failed_v0_1",
        "row_type": "failed_dynamic_candidate",
        "status": failed.get("status", "MISSING"),
        "total_return": failed.get("dynamic_total_return"),
        "CAGR": None,
        "max_drawdown": None,
        "volatility": None,
        "Sharpe": None,
        "Sortino": None,
        "Calmar": None,
        "turnover": failed.get("turnover"),
        "false_risk_off": failed.get("false_risk_off_count"),
        "false_risk_on": failed.get("false_risk_on_count"),
        "constraint_hit_count": _constraint_count_before(rescue_report),
        "upside_capture": None,
        "downside_capture": None,
        "production_effect": "none",
        "broker_action": "none",
    }


def _comparison_payload_row(
    comparison_id: str,
    row: Mapping[str, Any],
    *,
    candidate: bool = False,
) -> dict[str, Any]:
    return {
        "comparison_id": comparison_id,
        "row_type": "dynamic_candidate" if candidate else _text(row.get("row_type")),
        "status": _text(row.get("status"), "MISSING" if not row else "AVAILABLE"),
        "total_return": row.get("total_return"),
        "CAGR": row.get("CAGR"),
        "max_drawdown": row.get("max_drawdown"),
        "volatility": row.get("volatility"),
        "Sharpe": row.get("Sharpe"),
        "Sortino": row.get("Sortino"),
        "Calmar": row.get("Calmar"),
        "turnover": row.get("turnover"),
        "false_risk_off": row.get("false_risk_off"),
        "false_risk_on": row.get("false_risk_on"),
        "constraint_hit_count": row.get("constraint_hit_count"),
        "upside_capture": row.get("upside_capture"),
        "downside_capture": row.get("downside_capture"),
        "production_effect": "none",
        "broker_action": "none",
    }


def _comparison_row(report: Mapping[str, Any], comparison_id: str) -> dict[str, Any]:
    for row in _records(report.get("comparison_table")):
        if _text(row.get("comparison_id")) == comparison_id:
            return row
    return {}


def _package_warnings(regime: Mapping[str, Any]) -> list[str]:
    warnings = []
    if _text(regime.get("improvement_broadness_status")) != "BROAD_REVIEW_AVAILABLE":
        warnings.append("regime_robustness_has_missing_or_weak_slices")
    return warnings


def _recommended_next_actions(eligibility: Mapping[str, Any]) -> list[str]:
    blockers = set(_texts(eligibility.get("blocking_reason_codes")))
    actions = ["do_not_enroll_shadow_without_owner_review"]
    if "CONSTRAINT_HIT_WORSENED" in blockers:
        actions.append("run_constraint_guardrail_review_before_TRADING_090")
    if "DRAWDOWN_PRESERVATION_FAILED" in blockers:
        actions.append("run_drawdown_guardrail_review_before_TRADING_090")
    actions.append("prepare_TRADING_090_constraint_aware_rescue_or_signal_recalibration_decision")
    return actions


def _safety_status(safety: Mapping[str, Any]) -> str:
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true"
        if _mapping(safety) == DYNAMIC_V2_REVIEW_SAFETY
        else "SAFETY_REVIEW_REQUIRED"
    )


def _assert_dynamic_v2_review_payload_safe(payload: Mapping[str, Any]) -> None:
    safety = _mapping(payload.get("safety"))
    if safety != DYNAMIC_V2_REVIEW_SAFETY:
        raise DynamicV2ReviewError("dynamic v0.2 review payload safety fields are unsafe")
    for key, expected in DYNAMIC_V2_REVIEW_SAFETY.items():
        if payload.get(key) != expected:
            raise DynamicV2ReviewError(f"dynamic v0.2 review top-level safety mismatch: {key}")
    if payload.get("shadow_enrollment_allowed") is True:
        raise DynamicV2ReviewError("dynamic v0.2 review cannot allow shadow enrollment")
    if payload.get("automatic_enrollment_allowed") is True:
        raise DynamicV2ReviewError("dynamic v0.2 review cannot allow automatic enrollment")
    if payload.get("owner_approval_executed") is True:
        raise DynamicV2ReviewError("dynamic v0.2 review cannot execute owner approval")
    for key in _all_keys(payload):
        if key in DYNAMIC_V2_REVIEW_FORBIDDEN_KEYS:
            raise DynamicV2ReviewError(f"forbidden dynamic v0.2 review output key: {key}")
    for value in _all_string_values(payload):
        if value in DYNAMIC_V2_REVIEW_FORBIDDEN_KEYS:
            raise DynamicV2ReviewError(f"forbidden dynamic v0.2 review output value: {value}")


def _sample_dynamic_v2_review_inputs(
    policy: DynamicV2ReviewPolicyConfig,
) -> dict[str, dict[str, Any]]:
    rescue_report = {
        "schema_version": "etf_dynamic_rescue_evaluation_report_v1",
        "report_type": "etf_dynamic_rescue_evaluation_report",
        "dynamic_rescue_report_id": "dynamic-rescue-report_validation",
        "failed_v0_1_summary": {
            "status": "not_approved_for_shadow",
            "candidate_id": "failed_v0_1",
            "dynamic_total_return": -0.12,
            "excess_vs_static_base": -0.4256,
            "false_risk_off_count": 240,
            "false_risk_on_count": 20,
            "turnover": 13.06,
        },
        "turnover_constraint_breakdown": {
            "constraint_hit_count": 100,
            "constraint_hits_by_type": {"QQQ_max_hit": 30},
        },
        "failure_dataset": {
            "rows": [
                _sample_daily_row("2023-01-03", "risk_off", -0.01, 0.20, ["MAX_QQQ"]),
                _sample_daily_row("2023-01-04", "neutral", 0.02, 0.15, []),
            ]
        },
        "rescue_policy_templates": [
            {
                "policy_id": policy.review_package_policy.default_candidate_policy_id,
                "base_policy_id": "dynamic_regime_overlay_v0_1",
            }
        ],
        "rescue_candidate_comparison": [
            {
                "policy_id": policy.review_package_policy.default_candidate_policy_id,
                "template_type": "lower_turnover",
                "evaluation_status": "partial_improvement_needs_more_review",
                "return_vs_static_delta_improvement": 0.611,
                "false_risk_off_reduction": 217,
                "false_risk_on_reduction": 0,
                "turnover_reduction": 11.91,
                "constraint_hit_reduction": -143,
                "drawdown_preservation": -0.0595,
                "underperformance_vs_static": 0.1854,
            }
        ],
        "validation_context": {"data_quality_status": "PASS"},
        "source_links": {"data_quality_report": "validation_data_quality.md"},
        "safety": dict(DYNAMIC_V2_REVIEW_SAFETY),
        **DYNAMIC_V2_REVIEW_SAFETY,
        "commands_executed": False,
        "shadow_enrollment_allowed": False,
        "automatic_enrollment_allowed": False,
        "owner_approval_executed": False,
    }
    candidate_robustness_report = {
        "schema_version": "etf_dynamic_robustness_report_v1",
        "report_type": "etf_dynamic_robustness_report",
        "dynamic_robustness_report_id": "dynamic-robustness-report_v04_validation",
        "summary": {
            "dynamic_candidate_id": "v0_4_lower_turnover",
            "market_regime": "unified_primary_2021",
            "data_quality_status": "PASS",
            "dynamic_total_return": 0.23,
            "dynamic_max_drawdown": -0.16,
            "excess_vs_static_base": 0.1854,
            "false_risk_off_count": 23,
            "false_risk_on_count": 20,
        },
        "comparison_table": [
            _sample_comparison_row("dynamic_candidate", 0.23, -0.16, 1.15, 243),
            _sample_comparison_row("static_base_candidate", 0.04, -0.10, 0.00, 0),
            _sample_comparison_row("current_etf_baseline", 0.10, -0.12, 0.20, 0),
            _sample_comparison_row("QQQ_buy_and_hold", 0.20, -0.18, 0.00, 0),
            _sample_comparison_row("SPY_buy_and_hold", 0.08, -0.09, 0.00, 0),
            _sample_comparison_row("SMH_buy_and_hold", 0.27, -0.22, 0.00, 0),
        ],
        "regime_attribution": {
            "regimes": [
                {
                    "regime": "risk_on",
                    "trading_days": 20,
                    "total_return": 0.12,
                    "excess_vs_static_base": 0.04,
                    "max_drawdown": -0.04,
                    "turnover": 0.20,
                },
                {
                    "regime": "risk_off",
                    "trading_days": 8,
                    "total_return": -0.03,
                    "excess_vs_static_base": -0.05,
                    "max_drawdown": -0.12,
                    "turnover": 0.18,
                },
            ]
        },
        "walk_forward": {
            "windows": [
                {"start": "2023-01-01", "end": "2023-03-31", "total_return": 0.11},
                {"start": "2023-04-01", "end": "2023-06-30", "total_return": 0.04},
            ]
        },
        "daily_path_summary": {
            "row_count": 4,
            "constraint_hit_count": 243,
            "records": [
                _sample_daily_row("2023-01-03", "risk_on", 0.02, 0.10, ["MAX_QQQ"]),
                _sample_daily_row(
                    "2023-01-04",
                    "risk_off",
                    -0.08,
                    0.08,
                    ["WEEKLY_TURNOVER_CAP"],
                ),
                _sample_daily_row("2023-01-05", "risk_off", -0.04, 0.05, ["MAX_CASH"]),
                _sample_daily_row("2023-01-06", "risk_on", 0.03, 0.04, []),
            ],
        },
        "source_artifacts": {
            "data_quality_report": "validation_data_quality.md",
            "dynamic_calibration_report": "validation_candidate_pack.json",
        },
        "safety": dict(DYNAMIC_V2_REVIEW_SAFETY),
        **DYNAMIC_V2_REVIEW_SAFETY,
        "commands_executed": False,
        "shadow_enrollment_allowed": False,
    }
    shadow_package = {
        "report_type": "etf_dynamic_shadow_review_package",
        "review_summary": {"status": "BLOCKED"},
        "source_artifacts": {"operations_validation": "validation_operations.json"},
        "safety": dict(DYNAMIC_V2_REVIEW_SAFETY),
        **DYNAMIC_V2_REVIEW_SAFETY,
        "commands_executed": False,
    }
    return {
        "rescue_report": rescue_report,
        "candidate_robustness_report": candidate_robustness_report,
        "shadow_package": shadow_package,
    }


def _sample_daily_row(
    day: str,
    regime: str,
    strategy_return: float,
    turnover: float,
    constraints: list[str],
) -> dict[str, Any]:
    return {
        "date": day,
        "signal_date": day,
        "return_date": day,
        "selected_regime": regime,
        "regime_state": regime,
        "strategy_return": strategy_return,
        "turnover": turnover,
        "reason_codes_json": json.dumps(constraints),
        "constraint_hits": constraints,
        "target_weights_json": json.dumps(
            {"SPY": 0.35, "QQQ": 0.35, "SMH": 0.15, "SOXX": 0.05, "CASH": 0.10}
        ),
    }


def _sample_comparison_row(
    comparison_id: str,
    total_return: float,
    max_drawdown: float,
    turnover: float,
    constraint_hit_count: int,
) -> dict[str, Any]:
    returns = [total_return / 10.0 for _ in range(10)]
    volatility = pstdev(returns) * sqrt(252.0) if len(returns) > 1 else 0.0
    return {
        "comparison_id": comparison_id,
        "status": "AVAILABLE",
        "total_return": total_return,
        "CAGR": total_return,
        "max_drawdown": max_drawdown,
        "volatility": volatility,
        "Sharpe": None,
        "Sortino": None,
        "Calmar": None,
        "turnover": turnover,
        "constraint_hit_count": constraint_hit_count,
        "upside_capture": None,
        "downside_capture": None,
        "production_effect": "none",
        "broker_action": "none",
    }


def _write_json_md(
    payload: Mapping[str, Any],
    *,
    output_dir: Path,
    stem: str,
    markdown: str,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{_safe_stem(stem)}.json"
    markdown_path = output_dir / f"{_safe_stem(stem)}.md"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    markdown_path.write_text(markdown, encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}


def _latest_json(directory: Path, pattern: str) -> Path | None:
    if not directory.exists():
        return None
    candidates = [path for path in directory.glob(pattern) if path.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.stat().st_mtime)


def _append_check(
    checks: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    detail: str,
) -> None:
    checks.append(
        {
            "check_id": check_id,
            "status": "PASS" if passed else "FAIL",
            "passed": passed,
            "detail": detail,
        }
    )


def _safe_read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _all_keys(value: object) -> set[str]:
    keys: set[str] = set()
    if isinstance(value, Mapping):
        for key, item in value.items():
            keys.add(str(key))
            keys.update(_all_keys(item))
    elif isinstance(value, list):
        for item in value:
            keys.update(_all_keys(item))
    return keys


def _all_string_values(value: object) -> set[str]:
    values: set[str] = set()
    if isinstance(value, Mapping):
        for item in value.values():
            values.update(_all_string_values(item))
    elif isinstance(value, list):
        for item in value:
            values.update(_all_string_values(item))
    elif isinstance(value, str):
        values.add(value)
    return values


def _coerce_datetime(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def _safe_stem(value: str) -> str:
    return "".join(
        character if character.isalnum() or character in "._-" else "_" for character in value
    )


def _compound_return(returns: list[float]) -> float:
    if not returns:
        return 0.0
    return prod(1.0 + value for value in returns) - 1.0


def _avg(values: list[float]) -> float:
    return mean(values) if values else 0.0


def _json_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str) or not value:
        return {}
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return dict(loaded) if isinstance(loaded, Mapping) else {}


def _json_list(value: object) -> list[str]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [str(item) for item in value]
    if not isinstance(value, str) or not value:
        return []
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return [value]
    if not isinstance(loaded, Sequence) or isinstance(loaded, (str, bytes)):
        return [str(loaded)]
    return [str(item) for item in loaded]


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    return (
        [dict(item) for item in value if isinstance(item, Mapping)]
        if isinstance(value, list)
        else []
    )


def _texts(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [str(item) for item in value if str(item)]


def _text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _fmt_pct(value: object) -> str:
    if value is None:
        return "n/a"
    return f"{_float(value):.2%}"


def _fmt_num(value: object) -> str:
    if value is None:
        return "n/a"
    return f"{_float(value):.3f}"


def _stable_id(prefix: str, *parts: object) -> str:
    return f"{prefix}_{_stable_hash([prefix, *parts])[:12]}"


def _stable_hash(value: object) -> str:
    return sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
