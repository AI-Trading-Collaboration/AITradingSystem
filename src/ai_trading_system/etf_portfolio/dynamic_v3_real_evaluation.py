from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, Self

import pandas as pd
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.dynamic_allocation import (
    DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    DynamicAllocationPolicyConfig,
    load_dynamic_allocation_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_rescue import (
    DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH,
    DynamicFailureDiagnosticsPolicyConfig,
    apply_dynamic_rescue_template,
    load_dynamic_failure_diagnostics_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_robustness import (
    DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    DynamicRobustnessPolicyConfig,
    _synthetic_validation_prices,
    build_dynamic_robustness_report,
    load_dynamic_robustness_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_v3_rescue import (
    DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH,
    DynamicV3CandidateTemplatePolicy,
    DynamicV3RescuePolicyConfig,
    load_dynamic_v3_rescue_policy_config,
)
from ai_trading_system.etf_portfolio.models import ETFConfigBundle, PolicyMetadata
from ai_trading_system.yaml_loader import safe_load_yaml_path

DYNAMIC_V3_REAL_EVALUATION_POLICY_SCHEMA_VERSION = (
    "etf_dynamic_v3_real_evaluation_policy_v1"
)
DYNAMIC_V3_REAL_EVALUATION_REPORT_SCHEMA_VERSION = (
    "etf_dynamic_v3_real_evaluation_report_v1"
)
DYNAMIC_V3_REAL_EVALUATION_VALIDATION_SCHEMA_VERSION = (
    "etf_dynamic_v3_real_evaluation_validation_v1"
)

DYNAMIC_V3_REAL_EVALUATION_REPORT_TYPE = "etf_dynamic_v3_real_evaluation_report"
DYNAMIC_V3_REAL_EVALUATION_VALIDATION_REPORT_TYPE = (
    "etf_dynamic_v3_real_evaluation_validation"
)

DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_v3_real_evaluation.yaml"
)
DEFAULT_DYNAMIC_V3_REAL_EVALUATION_ROOT = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_v3_rescue"
)
DEFAULT_DYNAMIC_V3_REAL_EVALUATION_REPORT_DIR = (
    DEFAULT_DYNAMIC_V3_REAL_EVALUATION_ROOT / "real_evaluation"
)
DEFAULT_DYNAMIC_V3_REAL_EVALUATION_VALIDATION_DIR = (
    DEFAULT_DYNAMIC_V3_REAL_EVALUATION_ROOT / "real_validation"
)

WEIGHT_SYMBOLS = ("SPY", "QQQ", "SMH", "SOXX", "CASH")
SEMICONDUCTOR_SYMBOLS = ("SMH", "SOXX")

DYNAMIC_V3_REAL_EVALUATION_SAFETY: dict[str, Any] = {
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
}

DYNAMIC_V3_REAL_EVALUATION_FORBIDDEN_KEYS = {
    "approved_for_shadow",
    "auto_promotion",
    "baseline_config_mutation",
    "broker_order",
    "enable_broker_action",
    "official_target_weights_write",
    "place_order",
    "production_weight_update",
    "shadow_enrollment_record",
}


class DynamicV3RealEvaluationError(ValueError):
    """Raised when TRADING-091 real evaluation inputs or outputs are invalid."""


class DynamicV3RealEvaluationMarketRegime(BaseModel):
    regime_id: Literal["ai_after_chatgpt"]
    anchor_event: str = Field(min_length=1)
    anchor_date: date
    default_backtest_start: date

    @model_validator(mode="after")
    def validate_regime(self) -> Self:
        if self.default_backtest_start < date(2022, 12, 1):
            raise ValueError("dynamic v0.3 real evaluation start cannot predate 2022-12-01")
        return self


class DynamicV3RealEvaluationComparisonConfig(BaseModel):
    baseline_policy_id: str = Field(min_length=1)
    v0_2_policy_id: str = Field(min_length=1)
    v0_4_policy_id: str = Field(min_length=1)
    static_comparison_ids: list[str] = Field(min_length=1)
    required_v0_3_candidate_count: int = Field(ge=1)


class DynamicV3RealEvaluationMaterializationConfig(BaseModel):
    base_policy_id: str = Field(min_length=1)
    qqq_target_buffer: float = Field(ge=0, le=0.2)
    semiconductor_target_buffer: float = Field(ge=0, le=0.2)
    cash_target_buffer: float = Field(ge=0, le=0.2)
    soft_penalty_strength: float = Field(gt=0, le=1)
    trend_overlay_scale_with_soft_penalty: float = Field(ge=0, le=1)
    smoothing_max_single_rebalance_delta: float = Field(gt=0, le=1)
    smoothing_weekly_turnover_cap: float = Field(gt=0, le=1)
    smoothing_min_rebalance_weight_delta: float = Field(ge=0, le=1)
    drawdown_guardrail_regimes: list[str] = Field(min_length=1)
    drawdown_cash_increase_step: float = Field(gt=0, le=1)
    drawdown_semiconductor_reduction_step: float = Field(gt=0, le=1)
    drawdown_qqq_reduction_step: float = Field(gt=0, le=1)
    emergency_event_risk_cash_increase_step: float = Field(gt=0, le=1)
    emergency_event_risk_high_threshold: float = Field(ge=0, le=100)
    emergency_reduction_order: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_symbols(self) -> Self:
        unknown = set(self.emergency_reduction_order) - set(WEIGHT_SYMBOLS)
        if unknown:
            raise ValueError(
                "emergency_reduction_order references unknown symbols: "
                + ", ".join(sorted(unknown))
            )
        return self


class DynamicV3RealEvaluationPromotionGateConfig(BaseModel):
    promote_candidate_status: Literal["promote_candidate"]
    observe_only_status: Literal["observe_only"]
    reject_status: Literal["reject"]
    min_constraint_hit_reduction_vs_v0_4: int = Field(ge=0)
    max_constraint_hit_rate: float = Field(ge=0, le=1)
    max_turnover: float = Field(ge=0)
    max_false_risk_off_delta_vs_v0_4: int = Field(ge=0)
    max_drawdown_degradation_vs_v0_4: float = Field(ge=0)
    max_dynamic_drawdown_delta_vs_static: float = Field(ge=0)
    min_static_gap_delta_vs_v0_4: float
    min_dynamic_vs_static_gap: float
    min_walk_forward_pass_ratio: float = Field(ge=0, le=1)
    max_single_window_return_share: float = Field(ge=0, le=1)
    max_regime_return_concentration: float = Field(ge=0, le=1)
    reject_if_constraint_not_improved: bool
    reject_if_static_gap_worse_than_v0_4: bool
    reject_if_drawdown_materially_worse: bool
    max_promote_blocker_count: int = Field(ge=0)


class DynamicV3RealEvaluationReaderBriefConfig(BaseModel):
    section_title: str = Field(min_length=1)
    summary_prefix: str = Field(min_length=1)


class DynamicV3RealEvaluationSafety(BaseModel):
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
    shadow_enrollment_allowed: Literal[False]
    automatic_enrollment_allowed: Literal[False]
    owner_approval_executed: Literal[False]

    @model_validator(mode="after")
    def validate_safety(self) -> Self:
        if self.model_dump(mode="json") != DYNAMIC_V3_REAL_EVALUATION_SAFETY:
            raise ValueError("dynamic v0.3 real evaluation safety fields are unsafe")
        return self


class DynamicV3RealEvaluationPolicyConfig(BaseModel):
    schema_version: Literal["etf_dynamic_v3_real_evaluation_policy_v1"]
    policy_metadata: PolicyMetadata
    market_regime: DynamicV3RealEvaluationMarketRegime
    comparison: DynamicV3RealEvaluationComparisonConfig
    materialization: DynamicV3RealEvaluationMaterializationConfig
    promotion_gate: DynamicV3RealEvaluationPromotionGateConfig
    reader_brief: DynamicV3RealEvaluationReaderBriefConfig
    safety: DynamicV3RealEvaluationSafety

    @model_validator(mode="after")
    def validate_policy(self) -> Self:
        if self.materialization.base_policy_id != self.comparison.v0_4_policy_id:
            raise ValueError("v0.3 real evaluation must materialize from v0.4 lower_turnover")
        if self.safety.model_dump(mode="json") != DYNAMIC_V3_REAL_EVALUATION_SAFETY:
            raise ValueError("dynamic v0.3 real evaluation policy safety is unsafe")
        return self


def load_dynamic_v3_real_evaluation_policy_config(
    path: Path | str = DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH,
) -> DynamicV3RealEvaluationPolicyConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, Mapping):
        raise DynamicV3RealEvaluationError("dynamic v0.3 real evaluation policy must be a mapping")
    try:
        return DynamicV3RealEvaluationPolicyConfig.model_validate(raw)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3RealEvaluationError(
            f"invalid dynamic v0.3 real evaluation policy: {exc}"
        ) from exc


def materialize_dynamic_v3_real_candidate_policy(
    *,
    base_v0_4_policy: DynamicAllocationPolicyConfig,
    template: DynamicV3CandidateTemplatePolicy,
    v3_rescue_policy: DynamicV3RescuePolicyConfig,
    real_policy: DynamicV3RealEvaluationPolicyConfig,
) -> DynamicAllocationPolicyConfig:
    """Convert a TRADING-090 v0.3 template into an in-memory backtest policy."""

    candidate = base_v0_4_policy.model_copy(deep=True)
    candidate.default_policy_id = template.policy_id
    candidate.policy_metadata.version = template.policy_id
    candidate.policy_metadata.status = "candidate_only_real_evaluation"
    candidate.policy_metadata.rationale = (
        "TRADING-091 in-memory real historical evaluation materialization of "
        f"{template.template_type}; source production config is not mutated."
    )
    flags = template.feature_flags
    if flags.normalization or flags.soft_penalty:
        for target in candidate.regime_weight_targets.values():
            weights = _normalise_weights(target.weights)
            if flags.normalization:
                weights = _apply_normalization_caps(weights, v3_rescue_policy, real_policy)
            if flags.soft_penalty:
                weights = _apply_soft_interior_penalty(weights, v3_rescue_policy, real_policy)
            target.weights = _normalise_weights(weights)
    if flags.soft_penalty:
        _scale_trend_overlays(
            candidate,
            real_policy.materialization.trend_overlay_scale_with_soft_penalty,
        )
    if flags.smoothing:
        candidate.rebalance_policy.max_single_rebalance_delta = min(
            candidate.rebalance_policy.max_single_rebalance_delta,
            real_policy.materialization.smoothing_max_single_rebalance_delta,
        )
        candidate.rebalance_policy.weekly_turnover_cap = min(
            candidate.rebalance_policy.weekly_turnover_cap,
            real_policy.materialization.smoothing_weekly_turnover_cap,
        )
        candidate.rebalance_policy.min_rebalance_weight_delta = max(
            candidate.rebalance_policy.min_rebalance_weight_delta,
            real_policy.materialization.smoothing_min_rebalance_weight_delta,
        )
    if flags.drawdown_guardrail:
        for regime_id in real_policy.materialization.drawdown_guardrail_regimes:
            if regime_id not in candidate.regime_weight_targets:
                raise DynamicV3RealEvaluationError(
                    f"unknown drawdown guardrail regime: {regime_id}"
                )
            target = candidate.regime_weight_targets[regime_id]
            target.weights = _apply_drawdown_guardrail_target(target.weights, real_policy)
    if flags.emergency_risk_off:
        candidate.event_risk_overlay.high_threshold = (
            real_policy.materialization.emergency_event_risk_high_threshold
        )
        cash_increase = min(
            v3_rescue_policy.emergency_risk_off.max_cash_target,
            real_policy.materialization.emergency_event_risk_cash_increase_step,
        )
        candidate.event_risk_overlay.cash_increase = cash_increase
        candidate.event_risk_overlay.reductions = _reduction_plan(
            cash_increase,
            candidate.event_risk_overlay.reductions,
            real_policy.materialization.emergency_reduction_order,
        )
    try:
        return DynamicAllocationPolicyConfig.model_validate(candidate.model_dump(mode="json"))
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3RealEvaluationError(
            f"v0.3 real evaluation materialization produced invalid policy: {exc}"
        ) from exc


def build_dynamic_v3_real_evaluation_report(
    *,
    prices: pd.DataFrame,
    etf_config: ETFConfigBundle,
    policy: DynamicV3RealEvaluationPolicyConfig,
    v3_rescue_policy: DynamicV3RescuePolicyConfig,
    dynamic_robustness_policy: DynamicRobustnessPolicyConfig,
    dynamic_policy: DynamicAllocationPolicyConfig,
    failure_policy: DynamicFailureDiagnosticsPolicyConfig,
    start: date | None = None,
    end: date | None = None,
    data_quality_status: str = "UNKNOWN",
    data_quality_report: str = "",
    prices_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    materialized = _materialized_policy_set(
        dynamic_policy=dynamic_policy,
        failure_policy=failure_policy,
        v3_rescue_policy=v3_rescue_policy,
        real_policy=policy,
    )
    robustness_reports: dict[str, dict[str, Any]] = {}
    dynamic_rows: list[dict[str, Any]] = []
    for label, allocation_policy in materialized["policies"].items():
        report = build_dynamic_robustness_report(
            prices=prices,
            etf_config=etf_config,
            policy=dynamic_robustness_policy,
            dynamic_policy=allocation_policy,
            candidate_id=label,
            start=start or policy.market_regime.default_backtest_start,
            end=end,
            data_quality_status=data_quality_status,
            data_quality_report=data_quality_report,
            prices_path=prices_path,
        )
        robustness_reports[label] = report
        dynamic_rows.append(
            _dynamic_comparison_row(
                label=label,
                group=materialized["groups"][label],
                robustness_report=report,
            )
        )
    baseline_report = robustness_reports[policy.comparison.baseline_policy_id]
    static_rows = _static_comparison_rows(
        baseline_report,
        policy.comparison.static_comparison_ids,
    )
    v0_4_row = _row_by_policy(dynamic_rows, policy.comparison.v0_4_policy_id)
    comparison_rows = [
        _with_v0_4_deltas(row, v0_4_row) if row.get("row_type") == "dynamic_policy" else row
        for row in [*dynamic_rows, *static_rows]
    ]
    best = _best_v0_3_candidate(comparison_rows, policy)
    analyses = _comprehensive_analyses(best, v0_4_row, static_rows, policy)
    promotion_gate = _promotion_gate(best, v0_4_row, analyses, policy)
    report_id = _stable_id(
        "dynamic-v3-real-evaluation-report",
        best.get("policy_id"),
        start or policy.market_regime.default_backtest_start,
        end or _mapping(baseline_report.get("summary")).get("requested_end"),
        _stable_hash([row.get("policy_id") for row in comparison_rows]),
    )
    payload = {
        "schema_version": DYNAMIC_V3_REAL_EVALUATION_REPORT_SCHEMA_VERSION,
        "report_type": DYNAMIC_V3_REAL_EVALUATION_REPORT_TYPE,
        "dynamic_v3_real_evaluation_report_id": report_id,
        "generated_at": generated.isoformat(),
        "status": promotion_gate["decision"],
        "promotion_gate_decision": promotion_gate["decision"],
        "review_status": "real_evaluation_review_required",
        "shadow_readiness_status": "not_shadow_ready",
        "policy_version": policy.policy_metadata.version,
        "policy_config_hash": _stable_hash(policy.model_dump(mode="json")),
        "v3_rescue_policy_version": v3_rescue_policy.policy_metadata.version,
        "dynamic_robustness_policy_version": dynamic_robustness_policy.policy_metadata.version,
        "market_regime": policy.market_regime.model_dump(mode="json"),
        "requested_range": {
            "start": (start or policy.market_regime.default_backtest_start).isoformat(),
            "end": "" if end is None else end.isoformat(),
        },
        "summary": {
            "best_v0_3_candidate": best.get("policy_id"),
            "promotion_gate_decision": promotion_gate["decision"],
            "data_quality_status": data_quality_status,
            "constraint_hit_reduction_vs_v0_4": best.get(
                "constraint_hit_reduction_vs_v0_4"
            ),
            "false_risk_off_delta_vs_v0_4": best.get("false_risk_off_delta_vs_v0_4"),
            "max_drawdown_degradation_vs_v0_4": best.get(
                "max_drawdown_degradation_vs_v0_4"
            ),
            "turnover": best.get("turnover"),
            "dynamic_vs_static_gap": best.get("dynamic_vs_static_gap"),
            "static_gap_delta_vs_v0_4": best.get("static_gap_delta_vs_v0_4"),
            "overfit_status": best.get("overfit_status"),
        },
        "materialization_summary": materialized["summary"],
        "comparison_table": comparison_rows,
        "v0_3_vs_baseline_v0_2_v0_4_table": [
            row
            for row in comparison_rows
            if row.get("group")
            in {"baseline", "dynamic_v0_2", "dynamic_v0_4", "dynamic_v0_3_rescue"}
        ],
        "best_candidate": best,
        "constraint_hit_analysis": analyses["constraint_hit_analysis"],
        "false_risk_off_analysis": analyses["false_risk_off_analysis"],
        "drawdown_preservation_analysis": analyses["drawdown_preservation_analysis"],
        "turnover_analysis": analyses["turnover_analysis"],
        "static_gap_analysis": analyses["static_gap_analysis"],
        "overfit_analysis": analyses["overfit_analysis"],
        "promotion_gate": promotion_gate,
        "reader_brief_summary": _reader_brief_summary(best, promotion_gate, analyses),
        "source_artifacts": {
            "prices_path": "" if prices_path is None else str(prices_path),
            "data_quality_report": data_quality_report,
            "real_evaluation_policy_config": str(
                DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH
            ),
            "v3_rescue_policy_config": str(DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH),
            "dynamic_robustness_policy_config": str(
                DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH
            ),
            "dynamic_allocation_policy_config": str(
                DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH
            ),
            "failure_diagnostics_policy_config": str(
                DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH
            ),
        },
        "validation_context": {
            "validate_data_status": data_quality_status,
            "validate_data_report": data_quality_report,
            "price_driven_real_historical_evaluation": True,
            "candidate_policies_in_memory_only": True,
            "does_not_mutate_source_policy_config": True,
            "does_not_execute_approval_or_enrollment": True,
        },
        "safety": policy.safety.model_dump(mode="json"),
        **DYNAMIC_V3_REAL_EVALUATION_SAFETY,
        "commands_executed": False,
    }
    _assert_dynamic_v3_real_evaluation_payload_safe(payload)
    return payload


def build_dynamic_v3_real_evaluation_validation_report(
    *,
    config_path: Path | str = DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH,
    v3_rescue_config_path: Path | str = DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH,
    dynamic_robustness_config_path: Path | str = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Path | str = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    failure_diagnostics_config_path: Path
    | str = DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH,
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
    generated = generated_at or datetime.now(UTC)
    checks: list[dict[str, Any]] = []
    sample_report: dict[str, Any] | None = None
    policy: DynamicV3RealEvaluationPolicyConfig | None = None
    try:
        policy = load_dynamic_v3_real_evaluation_policy_config(config_path)
        _append_check(checks, "real_evaluation_config_valid", True, "policy config loads")
    except Exception as exc:  # noqa: BLE001
        _append_check(checks, "real_evaluation_config_valid", False, str(exc))
    if policy is not None:
        try:
            robustness_policy = load_dynamic_robustness_policy_config(
                Path(dynamic_robustness_config_path)
            )
            prices = _synthetic_validation_prices(robustness_policy)
            from ai_trading_system.etf_portfolio.models import load_etf_config_bundle

            sample_report = build_dynamic_v3_real_evaluation_report(
                prices=prices,
                etf_config=load_etf_config_bundle(),
                policy=policy,
                v3_rescue_policy=load_dynamic_v3_rescue_policy_config(
                    Path(v3_rescue_config_path)
                ),
                dynamic_robustness_policy=robustness_policy,
                dynamic_policy=load_dynamic_allocation_policy_config(
                    Path(dynamic_allocation_config_path)
                ),
                failure_policy=load_dynamic_failure_diagnostics_policy_config(
                    Path(failure_diagnostics_config_path)
                ),
                start=policy.market_regime.default_backtest_start,
                data_quality_status="SYNTHETIC_VALIDATION_PASS",
                data_quality_report="validation_sample",
                prices_path=Path("validation_sample_prices"),
                generated_at=generated,
            )
            rows = _records(sample_report.get("comparison_table"))
            _append_check(
                checks,
                "price_driven_report_builds",
                sample_report.get("report_type") == DYNAMIC_V3_REAL_EVALUATION_REPORT_TYPE,
                "synthetic price-driven real evaluation report builds",
            )
            _append_check(
                checks,
                "v0_3_candidates_materialized",
                len(
                    [
                        row
                        for row in rows
                        if row.get("group") == "dynamic_v0_3_rescue"
                    ]
                )
                >= policy.comparison.required_v0_3_candidate_count,
                "all v0.3 candidate templates are materialized for robustness evaluation",
            )
            required = {
                policy.comparison.baseline_policy_id,
                policy.comparison.v0_2_policy_id,
                policy.comparison.v0_4_policy_id,
            }
            present = {str(row.get("policy_id")) for row in rows}
            _append_check(
                checks,
                "baseline_v02_v04_comparison_present",
                required.issubset(present),
                "baseline, v0.2, and v0.4 comparison rows are present",
            )
            _append_check(
                checks,
                "promotion_gate_decision_present",
                sample_report.get("promotion_gate_decision")
                in {
                    policy.promotion_gate.promote_candidate_status,
                    policy.promotion_gate.observe_only_status,
                    policy.promotion_gate.reject_status,
                },
                "promotion gate decision is one of the approved labels",
            )
            _append_check(
                checks,
                "required_analyses_visible",
                all(
                    sample_report.get(key)
                    for key in (
                        "constraint_hit_analysis",
                        "false_risk_off_analysis",
                        "drawdown_preservation_analysis",
                        "turnover_analysis",
                        "static_gap_analysis",
                        "overfit_analysis",
                    )
                ),
                "all required diagnostic analyses are visible",
            )
            _assert_dynamic_v3_real_evaluation_payload_safe(sample_report)
            _append_check(
                checks,
                "safety_boundary_enforced",
                True,
                "report keeps no approval, no enrollment, and no production mutation",
            )
        except Exception as exc:  # noqa: BLE001
            _append_check(checks, "price_driven_report_builds", False, str(exc))
    registry_text = _read_text(report_registry_path)
    _append_check(
        checks,
        "report_registry_visible",
        DYNAMIC_V3_REAL_EVALUATION_REPORT_TYPE in registry_text
        and DYNAMIC_V3_REAL_EVALUATION_VALIDATION_REPORT_TYPE in registry_text,
        "report registry includes real evaluation report and validation gate",
    )
    reader_text = _read_text(reader_brief_path)
    _append_check(
        checks,
        "reader_brief_visible",
        "_etf_dynamic_v3_real_evaluation_summary" in reader_text
        and "Dynamic v0.3 Real Evaluation" in reader_text,
        "Reader Brief exposes Dynamic v0.3 Real Evaluation section",
    )
    cli_text = _read_text(cli_path)
    _append_check(
        checks,
        "cli_commands_visible",
        "real-evaluate" in cli_text
        and "real-report" in cli_text
        and "validate-real" in cli_text,
        "CLI exposes real-evaluate, real-report, and validate-real",
    )
    failed = [item for item in checks if item["status"] != "PASS"]
    payload = {
        "schema_version": DYNAMIC_V3_REAL_EVALUATION_VALIDATION_SCHEMA_VERSION,
        "report_type": DYNAMIC_V3_REAL_EVALUATION_VALIDATION_REPORT_TYPE,
        "validation_id": _stable_id(
            "dynamic-v3-real-evaluation-validation",
            _stable_hash([item["status"] for item in checks]),
        ),
        "generated_at": generated.isoformat(),
        "status": "PASS" if not failed else "FAIL",
        "failed_check_count": len(failed),
        "checks": checks,
        "sample_report_id": (
            "" if sample_report is None else sample_report["dynamic_v3_real_evaluation_report_id"]
        ),
        "production_effect_none_required": True,
        "broker_action_none_required": True,
        "no_auto_approval": True,
        "no_auto_enrollment": True,
        "real_evaluation_mutates_production_policy": False,
        "real_evaluation_auto_promotes_candidate": False,
        "safety": (
            DYNAMIC_V3_REAL_EVALUATION_SAFETY
            if policy is None
            else policy.safety.model_dump(mode="json")
        ),
        **DYNAMIC_V3_REAL_EVALUATION_SAFETY,
        "commands_executed": False,
    }
    _assert_dynamic_v3_real_evaluation_payload_safe(payload)
    return payload


def write_dynamic_v3_real_evaluation_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_DYNAMIC_V3_REAL_EVALUATION_REPORT_DIR,
) -> dict[str, Path]:
    return _write_report_pair(
        payload=payload,
        output_dir=output_dir,
        stem=_text(
            payload.get("dynamic_v3_real_evaluation_report_id"),
            "dynamic-v3-real-evaluation-report",
        ),
        markdown=render_dynamic_v3_real_evaluation_markdown(payload),
    )


def write_dynamic_v3_real_evaluation_validation_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_DYNAMIC_V3_REAL_EVALUATION_VALIDATION_DIR,
) -> dict[str, Path]:
    return _write_report_pair(
        payload=payload,
        output_dir=output_dir,
        stem=_text(payload.get("validation_id"), "dynamic-v3-real-evaluation-validation"),
        markdown=render_dynamic_v3_real_evaluation_validation_markdown(payload),
    )


def latest_dynamic_v3_real_evaluation_report_path(
    report_dir: Path = DEFAULT_DYNAMIC_V3_REAL_EVALUATION_REPORT_DIR,
) -> Path | None:
    return _latest_json(report_dir, "dynamic-v3-real-evaluation-report_*.json")


def render_dynamic_v3_real_evaluation_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    gate = _mapping(payload.get("promotion_gate"))
    best = _mapping(payload.get("best_candidate"))
    lines = [
        f"# Dynamic v0.3 Real Evaluation {payload.get('dynamic_v3_real_evaluation_report_id')}",
        "",
        "## Summary",
        "",
        f"- Promotion Gate: {payload.get('promotion_gate_decision')}",
        f"- Best v0.3 Candidate: {summary.get('best_v0_3_candidate')}",
        f"- Data Quality: {summary.get('data_quality_status')}",
        f"- Constraint Hit Reduction vs v0.4: {summary.get('constraint_hit_reduction_vs_v0_4')}",
        f"- False Risk-Off Delta vs v0.4: {summary.get('false_risk_off_delta_vs_v0_4')}",
        "- Max Drawdown Degradation vs v0.4: "
        f"{_fmt_pct(summary.get('max_drawdown_degradation_vs_v0_4'))}",
        f"- Dynamic vs Static Gap: {_fmt_pct(summary.get('dynamic_vs_static_gap'))}",
        "",
        "## v0.3 vs baseline / v0.2 / v0.4",
        "",
        (
            "| Policy | Group | Return | Max DD | Turnover | Constraint Hits | "
            "False Risk-Off | Static Gap | Overfit |"
        ),
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in _records(payload.get("v0_3_vs_baseline_v0_2_v0_4_table")):
        lines.append(
            f"| {row.get('policy_id')} | {row.get('group')} | "
            f"{_fmt_pct(row.get('total_return'))} | {_fmt_pct(row.get('max_drawdown'))} | "
            f"{_fmt_num(row.get('turnover'))} | {row.get('constraint_hit_count')} | "
            f"{row.get('false_risk_off_count')} | {_fmt_pct(row.get('dynamic_vs_static_gap'))} | "
            f"{row.get('overfit_status')} |"
        )
    lines.extend(
        [
            "",
            "## Diagnostic Analysis",
            "",
            "- Constraint Hit: "
            f"{_mapping(payload.get('constraint_hit_analysis')).get('conclusion')}",
            "- False Risk-Off: "
            f"{_mapping(payload.get('false_risk_off_analysis')).get('conclusion')}",
            "- Drawdown Preservation: "
            f"{_mapping(payload.get('drawdown_preservation_analysis')).get('conclusion')}",
            f"- Turnover: {_mapping(payload.get('turnover_analysis')).get('conclusion')}",
            f"- Static Gap: {_mapping(payload.get('static_gap_analysis')).get('conclusion')}",
            f"- Overfit: {_mapping(payload.get('overfit_analysis')).get('conclusion')}",
            "",
            "## Promotion Gate",
            "",
            f"- Decision: {gate.get('decision')}",
            f"- Best Candidate: {best.get('policy_id')}",
            f"- Manual Review Required: {payload.get('manual_review_required')}",
            f"- Production Effect: {payload.get('production_effect')}",
            f"- Broker Action: {payload.get('broker_action')}",
            "",
            "| Check | Status | Observed | Threshold | Critical |",
            "|---|---|---:|---:|---|",
        ]
    )
    for check in _records(gate.get("checks")):
        lines.append(
            f"| {check.get('check_id')} | {check.get('status')} | "
            f"{_fmt_value(check.get('observed'))} | {_fmt_value(check.get('threshold'))} | "
            f"{check.get('critical')} |"
        )
    lines.extend(
        [
            "",
            "## Reader Brief Summary",
            "",
            _text(payload.get("reader_brief_summary")),
            "",
            "## Safety",
            "",
            "- observe_only=true",
            "- candidate_only=true",
            "- production_effect=none",
            "- broker_action=none",
            "- automatic_candidate_promotion=false",
            "- shadow_enrollment_allowed=false",
            "- owner_approval_executed=false",
            "- commands_executed=false",
            "",
        ]
    )
    return "\n".join(lines)


def render_dynamic_v3_real_evaluation_validation_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# Dynamic v0.3 Real Evaluation Validation {payload.get('validation_id')}",
        "",
        f"- Status: {payload.get('status')}",
        f"- Failed Check Count: {payload.get('failed_check_count')}",
        f"- Production Effect: {payload.get('production_effect')}",
        f"- Broker Action: {payload.get('broker_action')}",
        "",
        "| Check | Status | Detail |",
        "|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(f"| {check.get('check_id')} | {check.get('status')} | {check.get('detail')} |")
    return "\n".join(lines) + "\n"


def _materialized_policy_set(
    *,
    dynamic_policy: DynamicAllocationPolicyConfig,
    failure_policy: DynamicFailureDiagnosticsPolicyConfig,
    v3_rescue_policy: DynamicV3RescuePolicyConfig,
    real_policy: DynamicV3RealEvaluationPolicyConfig,
) -> dict[str, Any]:
    v0_2_template = _rescue_template_by_id(failure_policy, real_policy.comparison.v0_2_policy_id)
    v0_4_template = _rescue_template_by_id(failure_policy, real_policy.comparison.v0_4_policy_id)
    v0_2_policy = apply_dynamic_rescue_template(dynamic_policy, v0_2_template)
    v0_4_policy = apply_dynamic_rescue_template(dynamic_policy, v0_4_template)
    policies: dict[str, DynamicAllocationPolicyConfig] = {
        real_policy.comparison.baseline_policy_id: dynamic_policy.model_copy(deep=True),
        real_policy.comparison.v0_2_policy_id: v0_2_policy,
        real_policy.comparison.v0_4_policy_id: v0_4_policy,
    }
    groups = {
        real_policy.comparison.baseline_policy_id: "baseline",
        real_policy.comparison.v0_2_policy_id: "dynamic_v0_2",
        real_policy.comparison.v0_4_policy_id: "dynamic_v0_4",
    }
    materialization_rows: list[dict[str, Any]] = []
    for template in v3_rescue_policy.candidate_templates:
        candidate_policy = materialize_dynamic_v3_real_candidate_policy(
            base_v0_4_policy=v0_4_policy,
            template=template,
            v3_rescue_policy=v3_rescue_policy,
            real_policy=real_policy,
        )
        policies[template.policy_id] = candidate_policy
        groups[template.policy_id] = "dynamic_v0_3_rescue"
        materialization_rows.append(
            {
                "policy_id": template.policy_id,
                "base_policy_id": template.base_policy_id,
                "template_type": template.template_type,
                "feature_flags": template.feature_flags.model_dump(mode="json"),
                "candidate_only": True,
                "production_config_mutation_allowed": False,
                "automatic_approval_allowed": False,
                "automatic_enrollment_allowed": False,
            }
        )
    return {
        "policies": policies,
        "groups": groups,
        "summary": {
            "base_policy_id": real_policy.materialization.base_policy_id,
            "candidate_count": len(materialization_rows),
            "candidate_policies_in_memory_only": True,
            "source_policy_config_mutated": False,
            "materialization_rows": materialization_rows,
        },
    }


def _dynamic_comparison_row(
    *,
    label: str,
    group: str,
    robustness_report: Mapping[str, Any],
) -> dict[str, Any]:
    comparison = _records(robustness_report.get("comparison_table"))
    dynamic_row = next(
        row for row in comparison if row.get("comparison_id") == "dynamic_candidate"
    )
    static_row = next(
        row for row in comparison if row.get("comparison_id") == "static_base_candidate"
    )
    daily = _mapping(robustness_report.get("daily_path_summary"))
    summary = _mapping(robustness_report.get("summary"))
    walk_forward = _mapping(robustness_report.get("walk_forward"))
    overfit = _mapping(robustness_report.get("overfit_diagnostics"))
    regime = _mapping(robustness_report.get("regime_attribution"))
    single_window_share = _overfit_finding_value(overfit, "single_period_dependency")
    constraint_hits = _int(daily.get("constraint_hit_count"))
    row_count = max(1, _int(daily.get("row_count")))
    max_drawdown = _float(dynamic_row.get("max_drawdown"))
    static_max_drawdown = _float(static_row.get("max_drawdown"))
    return {
        "policy_id": label,
        "group": group,
        "row_type": "dynamic_policy",
        "status": _text(robustness_report.get("status"), "UNKNOWN"),
        "total_return": _float(dynamic_row.get("total_return")),
        "CAGR": _float(dynamic_row.get("CAGR")),
        "max_drawdown": max_drawdown,
        "turnover": _float(dynamic_row.get("turnover")),
        "Sharpe": dynamic_row.get("Sharpe"),
        "upside_capture": dynamic_row.get("upside_capture"),
        "downside_capture": dynamic_row.get("downside_capture"),
        "average_cash_weight": dynamic_row.get("average_cash_weight"),
        "average_semiconductor_weight": dynamic_row.get("average_semiconductor_weight"),
        "dynamic_vs_static_gap": _float(summary.get("excess_vs_static_base")),
        "drawdown_preservation_vs_static": abs(static_max_drawdown) - abs(max_drawdown),
        "constraint_hit_count": constraint_hits,
        "constraint_hit_rate": constraint_hits / row_count,
        "false_risk_off_count": _int(summary.get("false_risk_off_count")),
        "false_risk_on_count": _int(summary.get("false_risk_on_count")),
        "walk_forward_status": walk_forward.get("status"),
        "walk_forward_pass_ratio": _float(walk_forward.get("pass_ratio")),
        "walk_forward_window_count": _int(walk_forward.get("window_count")),
        "overfit_status": overfit.get("status"),
        "overfit_failed_finding_count": _int(overfit.get("failed_finding_count")),
        "single_window_return_share": single_window_share,
        "regime_return_concentration": _float(regime.get("max_positive_return_share")),
        "robustness_report_id": robustness_report.get("dynamic_robustness_report_id"),
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _static_comparison_rows(
    robustness_report: Mapping[str, Any],
    comparison_ids: list[str],
) -> list[dict[str, Any]]:
    rows = []
    source_rows = {
        str(row.get("comparison_id")): row
        for row in _records(robustness_report.get("comparison_table"))
    }
    for comparison_id in comparison_ids:
        row = source_rows.get(comparison_id)
        if not row:
            rows.append(
                {
                    "policy_id": comparison_id,
                    "group": "static_or_benchmark",
                    "row_type": "static_or_benchmark",
                    "status": "MISSING",
                    "observe_only": True,
                    "candidate_only": True,
                    "production_effect": "none",
                    "broker_action": "none",
                    "manual_review_required": True,
                }
            )
            continue
        rows.append(
            {
                "policy_id": comparison_id,
                "group": "static_or_benchmark",
                "row_type": row.get("row_type"),
                "status": row.get("status"),
                "total_return": row.get("total_return"),
                "CAGR": row.get("CAGR"),
                "max_drawdown": row.get("max_drawdown"),
                "turnover": row.get("turnover"),
                "Sharpe": row.get("Sharpe"),
                "upside_capture": row.get("upside_capture"),
                "downside_capture": row.get("downside_capture"),
                "average_cash_weight": row.get("average_cash_weight"),
                "average_semiconductor_weight": row.get("average_semiconductor_weight"),
                "latest_weights": row.get("latest_weights"),
                "observe_only": True,
                "candidate_only": True,
                "production_effect": "none",
                "broker_action": "none",
                "manual_review_required": True,
            }
        )
    return rows


def _with_v0_4_deltas(row: Mapping[str, Any], v0_4_row: Mapping[str, Any]) -> dict[str, Any]:
    enriched = dict(row)
    enriched["constraint_hit_delta_vs_v0_4"] = _int(row.get("constraint_hit_count")) - _int(
        v0_4_row.get("constraint_hit_count")
    )
    enriched["constraint_hit_reduction_vs_v0_4"] = _int(
        v0_4_row.get("constraint_hit_count")
    ) - _int(row.get("constraint_hit_count"))
    enriched["turnover_delta_vs_v0_4"] = _float(row.get("turnover")) - _float(
        v0_4_row.get("turnover")
    )
    enriched["false_risk_off_delta_vs_v0_4"] = _int(row.get("false_risk_off_count")) - _int(
        v0_4_row.get("false_risk_off_count")
    )
    enriched["static_gap_delta_vs_v0_4"] = _float(row.get("dynamic_vs_static_gap")) - _float(
        v0_4_row.get("dynamic_vs_static_gap")
    )
    enriched["max_drawdown_degradation_vs_v0_4"] = abs(_float(row.get("max_drawdown"))) - abs(
        _float(v0_4_row.get("max_drawdown"))
    )
    return enriched


def _best_v0_3_candidate(
    rows: list[Mapping[str, Any]],
    policy: DynamicV3RealEvaluationPolicyConfig,
) -> dict[str, Any]:
    candidates = [dict(row) for row in rows if row.get("group") == "dynamic_v0_3_rescue"]
    if not candidates:
        raise DynamicV3RealEvaluationError("no v0.3 candidates were evaluated")

    def key(row: Mapping[str, Any]) -> tuple[Any, ...]:
        checks = _candidate_gate_checks(row, policy)
        passed = sum(1 for item in checks if item["status"] == "PASS")
        return (
            passed,
            _int(row.get("constraint_hit_reduction_vs_v0_4")),
            _float(row.get("static_gap_delta_vs_v0_4")),
            -max(0.0, _float(row.get("max_drawdown_degradation_vs_v0_4"))),
            -max(0, _int(row.get("false_risk_off_delta_vs_v0_4"))),
            -_float(row.get("turnover")),
        )

    best = max(candidates, key=key)
    best["gate_pass_count"] = sum(
        1 for item in _candidate_gate_checks(best, policy) if item["status"] == "PASS"
    )
    return best


def _comprehensive_analyses(
    best: Mapping[str, Any],
    v0_4_row: Mapping[str, Any],
    static_rows: list[Mapping[str, Any]],
    policy: DynamicV3RealEvaluationPolicyConfig,
) -> dict[str, dict[str, Any]]:
    static_base = next(
        (row for row in static_rows if row.get("policy_id") == "static_base_candidate"),
        {},
    )
    constraint_reduction = _int(best.get("constraint_hit_reduction_vs_v0_4"))
    turnover_delta = _float(best.get("turnover_delta_vs_v0_4"))
    static_gap_delta = _float(best.get("static_gap_delta_vs_v0_4"))
    drawdown_degradation = _float(best.get("max_drawdown_degradation_vs_v0_4"))
    not_turnover_only = (
        constraint_reduction > 0
        and (
            turnover_delta >= 0
            or static_gap_delta >= policy.promotion_gate.min_static_gap_delta_vs_v0_4
            or drawdown_degradation <= policy.promotion_gate.max_drawdown_degradation_vs_v0_4
        )
    )
    false_delta = _int(best.get("false_risk_off_delta_vs_v0_4"))
    dynamic_drawdown_delta_vs_static = abs(_float(best.get("max_drawdown"))) - abs(
        _float(static_base.get("max_drawdown"))
    )
    return {
        "constraint_hit_analysis": {
            "status": "PASS" if not_turnover_only else "FAIL",
            "v0_4_constraint_hit_count": v0_4_row.get("constraint_hit_count"),
            "best_v0_3_constraint_hit_count": best.get("constraint_hit_count"),
            "constraint_hit_reduction_vs_v0_4": constraint_reduction,
            "best_v0_3_turnover_delta_vs_v0_4": turnover_delta,
            "not_turnover_only": not_turnover_only,
            "conclusion": (
                "v0.3 reduces constraint hits with supporting static-gap or "
                "drawdown preservation evidence."
                if not_turnover_only
                else "v0.3 does not prove constraint-hit improvement beyond lower turnover."
            ),
        },
        "false_risk_off_analysis": {
            "status": (
                "PASS"
                if false_delta <= policy.promotion_gate.max_false_risk_off_delta_vs_v0_4
                else "FAIL"
            ),
            "v0_4_false_risk_off_count": v0_4_row.get("false_risk_off_count"),
            "best_v0_3_false_risk_off_count": best.get("false_risk_off_count"),
            "false_risk_off_delta_vs_v0_4": false_delta,
            "threshold": policy.promotion_gate.max_false_risk_off_delta_vs_v0_4,
            "conclusion": (
                "false risk-off remains inside the configured gate."
                if false_delta <= policy.promotion_gate.max_false_risk_off_delta_vs_v0_4
                else "false risk-off worsens beyond the configured gate."
            ),
        },
        "drawdown_preservation_analysis": {
            "status": (
                "PASS"
                if drawdown_degradation
                <= policy.promotion_gate.max_drawdown_degradation_vs_v0_4
                and dynamic_drawdown_delta_vs_static
                <= policy.promotion_gate.max_dynamic_drawdown_delta_vs_static
                else "FAIL"
            ),
            "best_v0_3_max_drawdown": best.get("max_drawdown"),
            "v0_4_max_drawdown": v0_4_row.get("max_drawdown"),
            "static_base_max_drawdown": static_base.get("max_drawdown"),
            "max_drawdown_degradation_vs_v0_4": drawdown_degradation,
            "dynamic_drawdown_delta_vs_static": dynamic_drawdown_delta_vs_static,
            "conclusion": (
                "drawdown protection is preserved within configured tolerances."
                if drawdown_degradation
                <= policy.promotion_gate.max_drawdown_degradation_vs_v0_4
                and dynamic_drawdown_delta_vs_static
                <= policy.promotion_gate.max_dynamic_drawdown_delta_vs_static
                else "drawdown protection is materially weaker than the configured tolerance."
            ),
        },
        "turnover_analysis": {
            "status": (
                "PASS"
                if _float(best.get("turnover")) <= policy.promotion_gate.max_turnover
                else "FAIL"
            ),
            "best_v0_3_turnover": best.get("turnover"),
            "v0_4_turnover": v0_4_row.get("turnover"),
            "turnover_delta_vs_v0_4": turnover_delta,
            "threshold": policy.promotion_gate.max_turnover,
            "conclusion": (
                "turnover remains inside the configured gate."
                if _float(best.get("turnover")) <= policy.promotion_gate.max_turnover
                else "turnover exceeds the configured gate."
            ),
        },
        "static_gap_analysis": {
            "status": (
                "PASS"
                if static_gap_delta >= policy.promotion_gate.min_static_gap_delta_vs_v0_4
                and _float(best.get("dynamic_vs_static_gap"))
                >= policy.promotion_gate.min_dynamic_vs_static_gap
                else "FAIL"
            ),
            "best_v0_3_dynamic_vs_static_gap": best.get("dynamic_vs_static_gap"),
            "v0_4_dynamic_vs_static_gap": v0_4_row.get("dynamic_vs_static_gap"),
            "static_gap_delta_vs_v0_4": static_gap_delta,
            "threshold_delta_vs_v0_4": policy.promotion_gate.min_static_gap_delta_vs_v0_4,
            "conclusion": (
                "dynamic vs static gap is not materially worse than v0.4."
                if static_gap_delta >= policy.promotion_gate.min_static_gap_delta_vs_v0_4
                and _float(best.get("dynamic_vs_static_gap"))
                >= policy.promotion_gate.min_dynamic_vs_static_gap
                else "dynamic vs static gap remains a blocker."
            ),
        },
        "overfit_analysis": {
            "status": (
                "PASS"
                if best.get("overfit_status") == "PASS"
                and _float(best.get("walk_forward_pass_ratio"))
                >= policy.promotion_gate.min_walk_forward_pass_ratio
                and _float(best.get("single_window_return_share"))
                <= policy.promotion_gate.max_single_window_return_share
                and _float(best.get("regime_return_concentration"))
                <= policy.promotion_gate.max_regime_return_concentration
                else "FAIL"
            ),
            "robustness_overfit_status": best.get("overfit_status"),
            "walk_forward_pass_ratio": best.get("walk_forward_pass_ratio"),
            "single_window_return_share": best.get("single_window_return_share"),
            "regime_return_concentration": best.get("regime_return_concentration"),
            "conclusion": (
                "walk-forward and concentration checks do not show single-window dependence."
                if best.get("overfit_status") == "PASS"
                and _float(best.get("walk_forward_pass_ratio"))
                >= policy.promotion_gate.min_walk_forward_pass_ratio
                and _float(best.get("single_window_return_share"))
                <= policy.promotion_gate.max_single_window_return_share
                and _float(best.get("regime_return_concentration"))
                <= policy.promotion_gate.max_regime_return_concentration
                else (
                    "full overfit diagnostics, walk-forward, or market-window "
                    "concentration remains a review blocker."
                )
            ),
        },
    }


def _promotion_gate(
    best: Mapping[str, Any],
    v0_4_row: Mapping[str, Any],
    analyses: Mapping[str, Mapping[str, Any]],
    policy: DynamicV3RealEvaluationPolicyConfig,
) -> dict[str, Any]:
    checks = _candidate_gate_checks(best, policy)
    constraint_analysis = _mapping(analyses.get("constraint_hit_analysis"))
    checks.append(
        _gate_check(
            "constraint_not_turnover_only",
            constraint_analysis.get("not_turnover_only") is True,
            constraint_analysis.get("not_turnover_only"),
            True,
            True,
        )
    )
    failed = [item for item in checks if item["status"] != "PASS"]
    critical_failed = [item for item in failed if item.get("critical") is True]
    blocker_ids = [str(item["check_id"]) for item in failed]
    if not failed:
        decision = policy.promotion_gate.promote_candidate_status
        rationale = "all configured promotion gate checks passed"
    elif critical_failed:
        decision = policy.promotion_gate.reject_status
        rationale = "one or more critical promotion gate checks failed"
    else:
        decision = policy.promotion_gate.observe_only_status
        rationale = "non-critical checks require more observation before candidate promotion"
    return {
        "decision": decision,
        "rationale": rationale,
        "best_candidate": best.get("policy_id"),
        "v0_4_reference_policy": v0_4_row.get("policy_id"),
        "blocker_ids": blocker_ids,
        "critical_blocker_ids": [str(item["check_id"]) for item in critical_failed],
        "checks": checks,
        "manual_review_required": True,
        "promotion_is_recommendation_only": True,
        "automatic_candidate_promotion": False,
        "shadow_enrollment_allowed": False,
        "production_state_mutated": False,
    }


def _candidate_gate_checks(
    row: Mapping[str, Any],
    policy: DynamicV3RealEvaluationPolicyConfig,
) -> list[dict[str, Any]]:
    gate = policy.promotion_gate
    return [
        _gate_check(
            "constraint_hit_reduction_vs_v0_4",
            _int(row.get("constraint_hit_reduction_vs_v0_4"))
            >= gate.min_constraint_hit_reduction_vs_v0_4,
            row.get("constraint_hit_reduction_vs_v0_4"),
            gate.min_constraint_hit_reduction_vs_v0_4,
            gate.reject_if_constraint_not_improved,
        ),
        _gate_check(
            "constraint_hit_rate",
            _float(row.get("constraint_hit_rate")) <= gate.max_constraint_hit_rate,
            row.get("constraint_hit_rate"),
            gate.max_constraint_hit_rate,
            gate.reject_if_constraint_not_improved,
        ),
        _gate_check(
            "false_risk_off_delta_vs_v0_4",
            _int(row.get("false_risk_off_delta_vs_v0_4"))
            <= gate.max_false_risk_off_delta_vs_v0_4,
            row.get("false_risk_off_delta_vs_v0_4"),
            gate.max_false_risk_off_delta_vs_v0_4,
            False,
        ),
        _gate_check(
            "turnover",
            _float(row.get("turnover")) <= gate.max_turnover,
            row.get("turnover"),
            gate.max_turnover,
            False,
        ),
        _gate_check(
            "max_drawdown_degradation_vs_v0_4",
            _float(row.get("max_drawdown_degradation_vs_v0_4"))
            <= gate.max_drawdown_degradation_vs_v0_4,
            row.get("max_drawdown_degradation_vs_v0_4"),
            gate.max_drawdown_degradation_vs_v0_4,
            gate.reject_if_drawdown_materially_worse,
        ),
        _gate_check(
            "static_gap_delta_vs_v0_4",
            _float(row.get("static_gap_delta_vs_v0_4")) >= gate.min_static_gap_delta_vs_v0_4,
            row.get("static_gap_delta_vs_v0_4"),
            gate.min_static_gap_delta_vs_v0_4,
            gate.reject_if_static_gap_worse_than_v0_4,
        ),
        _gate_check(
            "dynamic_vs_static_gap",
            _float(row.get("dynamic_vs_static_gap")) >= gate.min_dynamic_vs_static_gap,
            row.get("dynamic_vs_static_gap"),
            gate.min_dynamic_vs_static_gap,
            gate.reject_if_static_gap_worse_than_v0_4,
        ),
        _gate_check(
            "walk_forward_pass_ratio",
            _float(row.get("walk_forward_pass_ratio")) >= gate.min_walk_forward_pass_ratio,
            row.get("walk_forward_pass_ratio"),
            gate.min_walk_forward_pass_ratio,
            False,
        ),
        _gate_check(
            "robustness_overfit_status",
            row.get("overfit_status") == "PASS",
            row.get("overfit_status"),
            "PASS",
            False,
        ),
        _gate_check(
            "single_window_return_share",
            _float(row.get("single_window_return_share")) <= gate.max_single_window_return_share,
            row.get("single_window_return_share"),
            gate.max_single_window_return_share,
            False,
        ),
        _gate_check(
            "regime_return_concentration",
            _float(row.get("regime_return_concentration"))
            <= gate.max_regime_return_concentration,
            row.get("regime_return_concentration"),
            gate.max_regime_return_concentration,
            False,
        ),
    ]


def _gate_check(
    check_id: str,
    passed: bool,
    observed: Any,
    threshold: Any,
    critical: bool,
) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": "PASS" if passed else "FAIL",
        "observed": observed,
        "threshold": threshold,
        "critical": critical,
    }


def _reader_brief_summary(
    best: Mapping[str, Any],
    promotion_gate: Mapping[str, Any],
    analyses: Mapping[str, Mapping[str, Any]],
) -> str:
    return (
        "Dynamic v0.3 Real Evaluation: "
        f"gate={promotion_gate.get('decision')}; best={best.get('policy_id')}; "
        f"constraint={_mapping(analyses.get('constraint_hit_analysis')).get('status')}; "
        f"false_risk_off={_mapping(analyses.get('false_risk_off_analysis')).get('status')}; "
        f"drawdown={_mapping(analyses.get('drawdown_preservation_analysis')).get('status')}; "
        f"static_gap={_mapping(analyses.get('static_gap_analysis')).get('status')}; "
        "result is a manual-review recommendation only, with production_effect=none."
    )


def _apply_normalization_caps(
    weights: Mapping[str, float],
    v3_policy: DynamicV3RescuePolicyConfig,
    real_policy: DynamicV3RealEvaluationPolicyConfig,
) -> dict[str, float]:
    adjusted = dict(_normalise_weights(weights))
    qqq_cap = max(0.0, v3_policy.normalization_policy.qqq_max_target)
    semiconductor_cap = max(0.0, v3_policy.normalization_policy.semiconductor_max_target)
    cash_cap = max(0.0, v3_policy.normalization_policy.cash_max_target)
    cash_min = v3_policy.normalization_policy.cash_min_target
    _cap_symbol_to_spy(adjusted, "QQQ", qqq_cap)
    _cap_group_to_spy(adjusted, SEMICONDUCTOR_SYMBOLS, semiconductor_cap)
    _cap_symbol_to_spy(adjusted, "CASH", cash_cap)
    shortfall = max(0.0, cash_min - adjusted.get("CASH", 0.0))
    if shortfall > 0:
        adjusted["CASH"] += _reduce_group(adjusted, ("SPY", "QQQ", "SMH", "SOXX"), shortfall)
    return _normalise_weights(adjusted)


def _apply_soft_interior_penalty(
    weights: Mapping[str, float],
    v3_policy: DynamicV3RescuePolicyConfig,
    real_policy: DynamicV3RealEvaluationPolicyConfig,
) -> dict[str, float]:
    adjusted = dict(_normalise_weights(weights))
    strength = real_policy.materialization.soft_penalty_strength
    qqq_soft_cap = max(
        0.0,
        v3_policy.normalization_policy.qqq_max_target
        - real_policy.materialization.qqq_target_buffer,
    )
    semi_soft_cap = max(
        0.0,
        v3_policy.normalization_policy.semiconductor_max_target
        - real_policy.materialization.semiconductor_target_buffer,
    )
    cash_soft_cap = max(
        0.0,
        v3_policy.normalization_policy.cash_max_target
        - real_policy.materialization.cash_target_buffer,
    )
    _soft_cap_symbol_to_spy(adjusted, "QQQ", qqq_soft_cap, strength)
    _soft_cap_group_to_spy(adjusted, SEMICONDUCTOR_SYMBOLS, semi_soft_cap, strength)
    _soft_cap_symbol_to_spy(adjusted, "CASH", cash_soft_cap, strength)
    return _normalise_weights(adjusted)


def _apply_drawdown_guardrail_target(
    weights: Mapping[str, float],
    policy: DynamicV3RealEvaluationPolicyConfig,
) -> dict[str, float]:
    adjusted = dict(_normalise_weights(weights))
    cash_room = max(0.0, 1.0 - adjusted["CASH"])
    cash_raise = min(policy.materialization.drawdown_cash_increase_step, cash_room)
    semis = min(policy.materialization.drawdown_semiconductor_reduction_step, cash_raise)
    reduced = _reduce_group(adjusted, SEMICONDUCTOR_SYMBOLS, semis)
    remaining = max(0.0, cash_raise - reduced)
    reduced += _reduce_group(
        adjusted,
        ("QQQ",),
        min(policy.materialization.drawdown_qqq_reduction_step, remaining),
    )
    if reduced > 0:
        adjusted["CASH"] += reduced
    return _normalise_weights(adjusted)


def _reduction_plan(
    cash_increase: float,
    base_reductions: Mapping[str, float],
    order: list[str],
) -> dict[str, float]:
    remaining = cash_increase
    plan = {symbol: 0.0 for symbol in WEIGHT_SYMBOLS if symbol != "CASH"}
    base_total = sum(max(0.0, _float(value)) for value in base_reductions.values())
    for symbol in order:
        if symbol == "CASH" or remaining <= 0:
            continue
        share = (
            max(0.0, _float(base_reductions.get(symbol))) / base_total
            if base_total > 0
            else 1.0 / max(1, len(order))
        )
        reduction = min(remaining, cash_increase * share)
        plan[symbol] = plan.get(symbol, 0.0) + reduction
        remaining -= reduction
    if remaining > 1e-10:
        for symbol in order:
            if symbol != "CASH":
                plan[symbol] = plan.get(symbol, 0.0) + remaining
                break
    return {symbol: round(value, 10) for symbol, value in plan.items() if value > 1e-10}


def _scale_trend_overlays(policy: DynamicAllocationPolicyConfig, scale: float) -> None:
    for rule in policy.trend_overlay_rules:
        rule.adjustments = {
            symbol: round(_float(delta) * scale, 10)
            for symbol, delta in rule.adjustments.items()
        }


def _cap_symbol_to_spy(weights: dict[str, float], symbol: str, cap: float) -> None:
    excess = max(0.0, weights.get(symbol, 0.0) - cap)
    if excess > 0:
        weights[symbol] -= excess
        weights["SPY"] += excess


def _cap_group_to_spy(weights: dict[str, float], symbols: tuple[str, ...], cap: float) -> None:
    total = sum(weights.get(symbol, 0.0) for symbol in symbols)
    excess = max(0.0, total - cap)
    if excess > 0:
        weights["SPY"] += _reduce_group(weights, symbols, excess)


def _soft_cap_symbol_to_spy(
    weights: dict[str, float],
    symbol: str,
    cap: float,
    strength: float,
) -> None:
    excess = max(0.0, weights.get(symbol, 0.0) - cap)
    if excess > 0:
        reduction = excess * strength
        weights[symbol] -= reduction
        weights["SPY"] += reduction


def _soft_cap_group_to_spy(
    weights: dict[str, float],
    symbols: tuple[str, ...],
    cap: float,
    strength: float,
) -> None:
    total = sum(weights.get(symbol, 0.0) for symbol in symbols)
    excess = max(0.0, total - cap)
    if excess > 0:
        weights["SPY"] += _reduce_group(weights, symbols, excess * strength)


def _reduce_group(
    weights: dict[str, float],
    symbols: tuple[str, ...],
    amount: float,
) -> float:
    remaining = max(0.0, amount)
    reduced = 0.0
    total = sum(max(0.0, weights.get(symbol, 0.0)) for symbol in symbols)
    if total <= 0:
        return 0.0
    for symbol in symbols:
        if remaining <= 0:
            break
        current = max(0.0, weights.get(symbol, 0.0))
        reduction = min(current, amount * current / total)
        weights[symbol] = current - reduction
        remaining -= reduction
        reduced += reduction
    return reduced


def _rescue_template_by_id(
    policy: DynamicFailureDiagnosticsPolicyConfig,
    policy_id: str,
) -> Any:
    for template in policy.rescue_policy_templates:
        if template.policy_id == policy_id:
            return template
    raise DynamicV3RealEvaluationError(f"dynamic rescue template not found: {policy_id}")


def _row_by_policy(rows: list[Mapping[str, Any]], policy_id: str) -> Mapping[str, Any]:
    for row in rows:
        if row.get("policy_id") == policy_id:
            return row
    raise DynamicV3RealEvaluationError(f"comparison row not found: {policy_id}")


def _overfit_finding_value(overfit: Mapping[str, Any], finding_id: str) -> float:
    for finding in _records(overfit.get("findings")):
        if finding.get("finding_id") == finding_id:
            return _float(finding.get("observed"))
    return 0.0


def _append_check(checks: list[dict[str, Any]], check_id: str, passed: bool, detail: str) -> None:
    checks.append(
        {
            "check_id": check_id,
            "status": "PASS" if passed else "FAIL",
            "detail": detail,
        }
    )


def _write_report_pair(
    *,
    payload: Mapping[str, Any],
    output_dir: Path,
    stem: str,
    markdown: str,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_stem = _slug(stem)
    json_path = output_dir / f"{safe_stem}.json"
    markdown_path = output_dir / f"{safe_stem}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(markdown, encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}


def _latest_json(directory: Path, pattern: str) -> Path | None:
    if not directory.exists():
        return None
    candidates = [path for path in directory.glob(pattern) if path.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.stat().st_mtime)


def _assert_dynamic_v3_real_evaluation_payload_safe(payload: Mapping[str, Any]) -> None:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True).lower()
    for key in DYNAMIC_V3_REAL_EVALUATION_FORBIDDEN_KEYS:
        if key in text:
            raise DynamicV3RealEvaluationError(f"forbidden real evaluation key present: {key}")
    safety = _mapping(payload.get("safety"))
    if safety and safety != DYNAMIC_V3_REAL_EVALUATION_SAFETY:
        raise DynamicV3RealEvaluationError("unsafe real evaluation safety payload")
    for key, expected in DYNAMIC_V3_REAL_EVALUATION_SAFETY.items():
        if payload.get(key, expected) != expected:
            raise DynamicV3RealEvaluationError(f"unsafe real evaluation field: {key}")
    if payload.get("commands_executed", False) is not False:
        raise DynamicV3RealEvaluationError("real evaluation report must not execute commands")


def _normalise_weights(weights: Mapping[str, float]) -> dict[str, float]:
    parsed = {symbol: max(0.0, _float(weights.get(symbol))) for symbol in WEIGHT_SYMBOLS}
    total = sum(parsed.values())
    if total <= 0:
        raise DynamicV3RealEvaluationError("weight map total must be positive")
    return {symbol: round(value / total, 10) for symbol, value in parsed.items()}


def _records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _fmt_pct(value: Any) -> str:
    return f"{_float(value):.2%}"


def _fmt_num(value: Any) -> str:
    return f"{_float(value):.4f}"


def _fmt_value(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.6f}"
    return _text(value)


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _stable_hash(value: object) -> str:
    material = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode()
    return sha256(material).hexdigest()[:16]


def _stable_id(prefix: str, *parts: object) -> str:
    return f"{prefix}_{_stable_hash([prefix, *parts])}"


def _slug(value: str) -> str:
    keep = []
    for char in value:
        if char.isalnum() or char in {"-", "_"}:
            keep.append(char)
        else:
            keep.append("-")
    return "".join(keep).strip("-") or "dynamic-v3-real-evaluation"
