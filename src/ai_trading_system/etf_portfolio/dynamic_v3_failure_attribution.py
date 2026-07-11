from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from functools import lru_cache
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
    load_dynamic_failure_diagnostics_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_robustness import (
    DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    DynamicRobustnessPolicyConfig,
    _synthetic_validation_prices,
    build_dynamic_robustness_report,
    load_dynamic_robustness_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_v3_real_evaluation import (
    DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH,
    DYNAMIC_V3_REAL_EVALUATION_REPORT_TYPE,
    DynamicV3RealEvaluationPolicyConfig,
    _materialized_policy_set,
    build_dynamic_v3_real_evaluation_report,
    load_dynamic_v3_real_evaluation_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_v3_rescue import (
    DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH,
    DynamicV3RescuePolicyConfig,
    load_dynamic_v3_rescue_policy_config,
)
from ai_trading_system.etf_portfolio.models import ETFConfigBundle, PolicyMetadata
from ai_trading_system.yaml_loader import safe_load_yaml_path

DYNAMIC_V3_FAILURE_ATTRIBUTION_POLICY_SCHEMA_VERSION = (
    "etf_dynamic_v3_failure_attribution_policy_v1"
)
DYNAMIC_V3_FAILURE_ATTRIBUTION_REPORT_SCHEMA_VERSION = (
    "etf_dynamic_v3_failure_attribution_report_v1"
)
DYNAMIC_V3_FAILURE_ATTRIBUTION_VALIDATION_SCHEMA_VERSION = (
    "etf_dynamic_v3_failure_attribution_validation_v1"
)

DYNAMIC_V3_FAILURE_ATTRIBUTION_REPORT_TYPE = "etf_dynamic_v3_failure_attribution_report"
DYNAMIC_V3_FAILURE_ATTRIBUTION_VALIDATION_REPORT_TYPE = (
    "etf_dynamic_v3_failure_attribution_validation"
)

DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_v3_failure_attribution.yaml"
)
DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_ROOT = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_v3_rescue"
)
DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_REPORT_DIR = (
    DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_ROOT / "failure_attribution"
)
DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_VALIDATION_DIR = (
    DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_ROOT / "failure_attribution_validation"
)

WEIGHT_SYMBOLS = ("SPY", "QQQ", "SMH", "SOXX", "CASH")
SEMICONDUCTOR_SYMBOLS = ("SMH", "SOXX")

CONSTRAINT_REASON_PREFIXES = (
    "MAX_",
    "MIN_",
    "WEEKLY_TURNOVER_CAP",
    "REGIME_CONFIRMATION_WINDOW",
)
GUARD_FIXABLE_REASONS = {
    "MAX_SINGLE_REBALANCE_DELTA",
    "WEEKLY_TURNOVER_CAP",
}

DYNAMIC_V3_FAILURE_ATTRIBUTION_SAFETY: dict[str, Any] = {
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

DYNAMIC_V3_FAILURE_ATTRIBUTION_FORBIDDEN_KEYS = {
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


class DynamicV3FailureAttributionError(ValueError):
    """Raised when TRADING-092 attribution inputs or outputs are invalid."""


class DynamicV3FailureAttributionMarketRegime(BaseModel):
    regime_id: Literal["ai_after_chatgpt"]
    anchor_event: str = Field(min_length=1)
    anchor_date: date
    default_backtest_start: date

    @model_validator(mode="after")
    def validate_regime(self) -> Self:
        if self.default_backtest_start < date(2022, 12, 1):
            raise ValueError("failure attribution start cannot predate 2022-12-01")
        return self


class DynamicV3FailureAttributionInputs(BaseModel):
    real_evaluation_report_glob: str = Field(min_length=1)
    required_real_evaluation_report_type: Literal["etf_dynamic_v3_real_evaluation_report"]
    require_real_evaluation_reject: bool
    best_v0_3_source: Literal["latest_real_evaluation_best_candidate"]
    v0_4_policy_id: str = Field(min_length=1)


class DynamicV3ConstraintAttributionConfig(BaseModel):
    high_bucket_share: float = Field(gt=0, le=1)
    guard_fixable_bucket_share: float = Field(gt=0, le=1)
    cap_proximity_tolerance: float = Field(ge=0, le=0.05)
    top_bucket_limit: int = Field(ge=1, le=50)
    top_event_limit: int = Field(ge=1, le=100)
    broad_failure_bucket_limit: int = Field(ge=1)
    material_hit_count_delta: int = Field(ge=0)


class DynamicV3V04PromotionReviewConfig(BaseModel):
    promote_v0_4_status: Literal["promote_v0_4"]
    observe_v0_4_with_constraint_guard_status: Literal["observe_v0_4_with_constraint_guard"]
    do_not_promote_v0_4_status: Literal["do_not_promote_v0_4"]
    max_promotable_constraint_hit_rate: float = Field(ge=0, le=1)
    max_promotable_overfit_failed_findings: int = Field(ge=0)
    max_drawdown_degradation_vs_v0_3: float = Field(ge=0)
    max_false_risk_off_delta_vs_v0_3: int = Field(ge=0)
    max_turnover: float = Field(ge=0)
    min_dynamic_vs_static_gap: float
    max_dynamic_drawdown_delta_vs_static: float = Field(ge=0)
    max_static_gap_loss_vs_v0_3_for_path_edge: float = Field(ge=0)
    require_overfit_pass_for_promote: bool


class DynamicV3V05DesignConfig(BaseModel):
    not_required_status: Literal["not_required"]
    constraint_guard_status: Literal["recommend_v0_5_constraint_guard"]
    exposure_redesign_status: Literal["recommend_v0_5_exposure_redesign"]
    guard_design_summary: str = Field(min_length=1)
    exposure_redesign_summary: str = Field(min_length=1)


class DynamicV3FailureAttributionReaderBriefConfig(BaseModel):
    section_title: str = Field(min_length=1)
    summary_prefix: str = Field(min_length=1)


class DynamicV3FailureAttributionSafety(BaseModel):
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
        if self.model_dump(mode="json") != DYNAMIC_V3_FAILURE_ATTRIBUTION_SAFETY:
            raise ValueError("dynamic v0.3 failure attribution safety fields are unsafe")
        return self


class DynamicV3FailureAttributionPolicyConfig(BaseModel):
    schema_version: Literal["etf_dynamic_v3_failure_attribution_policy_v1"]
    policy_metadata: PolicyMetadata
    market_regime: DynamicV3FailureAttributionMarketRegime
    inputs: DynamicV3FailureAttributionInputs
    constraint_attribution: DynamicV3ConstraintAttributionConfig
    v0_4_promotion_review: DynamicV3V04PromotionReviewConfig
    v0_5_design: DynamicV3V05DesignConfig
    reader_brief: DynamicV3FailureAttributionReaderBriefConfig
    safety: DynamicV3FailureAttributionSafety

    @model_validator(mode="after")
    def validate_policy(self) -> Self:
        if self.safety.model_dump(mode="json") != DYNAMIC_V3_FAILURE_ATTRIBUTION_SAFETY:
            raise ValueError("dynamic v0.3 failure attribution policy safety is unsafe")
        return self


def load_dynamic_v3_failure_attribution_policy_config(
    path: Path | str = DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_POLICY_CONFIG_PATH,
) -> DynamicV3FailureAttributionPolicyConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, Mapping):
        raise DynamicV3FailureAttributionError(
            "dynamic v0.3 failure attribution policy must be a mapping"
        )
    try:
        return DynamicV3FailureAttributionPolicyConfig.model_validate(raw)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3FailureAttributionError(
            f"invalid dynamic v0.3 failure attribution policy: {exc}"
        ) from exc


def load_json_artifact(path: Path | str) -> dict[str, Any]:
    resolved = Path(path)
    try:
        raw = json.loads(resolved.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise DynamicV3FailureAttributionError(f"artifact not found: {resolved}") from exc
    except json.JSONDecodeError as exc:
        raise DynamicV3FailureAttributionError(f"invalid JSON artifact: {resolved}") from exc
    if not isinstance(raw, dict):
        raise DynamicV3FailureAttributionError(f"artifact must be a JSON object: {resolved}")
    return raw


def latest_dynamic_v3_failure_attribution_report_path(
    report_dir: Path = DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_REPORT_DIR,
) -> Path | None:
    return _latest_json(report_dir, "dynamic-v3-failure-attribution-report_*.json")


def latest_dynamic_v3_failure_attribution_real_evaluation_path(
    policy: DynamicV3FailureAttributionPolicyConfig,
) -> Path | None:
    pattern = PROJECT_ROOT / policy.inputs.real_evaluation_report_glob
    matches = sorted(
        (path for path in pattern.parent.glob(pattern.name) if path.is_file()),
        key=lambda item: item.stat().st_mtime,
    )
    return matches[-1] if matches else None


def build_dynamic_v3_failure_attribution_validation_sample_report(
    *,
    config_path: Path | str = DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_POLICY_CONFIG_PATH,
    real_evaluation_config_path: Path
    | str = DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH,
    v3_rescue_config_path: Path | str = DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH,
    dynamic_robustness_config_path: Path
    | str = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Path
    | str = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    failure_diagnostics_config_path: Path
    | str = DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH,
) -> dict[str, Any]:
    """Build the deterministic synthetic validation sample once per process."""

    cache_keys = [
        _path_cache_key(Path(item))
        for item in (
            config_path,
            real_evaluation_config_path,
            v3_rescue_config_path,
            dynamic_robustness_config_path,
            dynamic_allocation_config_path,
            failure_diagnostics_config_path,
        )
    ]
    payload_text = _cached_dynamic_v3_failure_attribution_validation_sample_report(
        *[part for key in cache_keys for part in key]
    )
    return json.loads(payload_text)


@lru_cache(maxsize=8)
def _cached_dynamic_v3_failure_attribution_validation_sample_report(
    config_path_text: str,
    _config_hash: str,
    real_evaluation_config_path_text: str,
    _real_evaluation_config_hash: str,
    v3_rescue_config_path_text: str,
    _v3_rescue_config_hash: str,
    dynamic_robustness_config_path_text: str,
    _dynamic_robustness_config_hash: str,
    dynamic_allocation_config_path_text: str,
    _dynamic_allocation_config_hash: str,
    failure_diagnostics_config_path_text: str,
    _failure_diagnostics_config_hash: str,
) -> str:
    policy = load_dynamic_v3_failure_attribution_policy_config(Path(config_path_text))
    robustness_policy = load_dynamic_robustness_policy_config(
        Path(dynamic_robustness_config_path_text)
    )
    prices = _synthetic_validation_prices(robustness_policy)
    from ai_trading_system.etf_portfolio.models import load_etf_config_bundle

    etf_config = load_etf_config_bundle()
    real_policy = load_dynamic_v3_real_evaluation_policy_config(
        Path(real_evaluation_config_path_text)
    )
    v3_policy = load_dynamic_v3_rescue_policy_config(Path(v3_rescue_config_path_text))
    dynamic_allocation = load_dynamic_allocation_policy_config(
        Path(dynamic_allocation_config_path_text)
    )
    failure_policy = load_dynamic_failure_diagnostics_policy_config(
        Path(failure_diagnostics_config_path_text)
    )
    sample_real_report = build_dynamic_v3_real_evaluation_report(
        prices=prices,
        etf_config=etf_config,
        policy=real_policy,
        v3_rescue_policy=v3_policy,
        dynamic_robustness_policy=robustness_policy,
        dynamic_policy=dynamic_allocation,
        failure_policy=failure_policy,
        start=policy.market_regime.default_backtest_start,
        data_quality_status="SYNTHETIC_VALIDATION_PASS",
        data_quality_report="validation_sample",
        prices_path=Path("validation_sample_prices"),
    )
    sample_report = build_dynamic_v3_failure_attribution_report(
        prices=prices,
        etf_config=etf_config,
        policy=policy,
        real_evaluation_report=sample_real_report,
        real_evaluation_report_path=Path("validation_sample_real_evaluation"),
        real_policy=real_policy,
        v3_rescue_policy=v3_policy,
        dynamic_robustness_policy=robustness_policy,
        dynamic_policy=dynamic_allocation,
        failure_policy=failure_policy,
        start=policy.market_regime.default_backtest_start,
        data_quality_status="SYNTHETIC_VALIDATION_PASS",
        data_quality_report="validation_sample",
        prices_path=Path("validation_sample_prices"),
        allow_non_reject_for_validation=True,
    )
    _assert_dynamic_v3_failure_attribution_payload_safe(sample_report)
    return json.dumps(sample_report, ensure_ascii=False, sort_keys=True, default=str)


def build_dynamic_v3_failure_attribution_report(
    *,
    prices: pd.DataFrame,
    etf_config: ETFConfigBundle,
    policy: DynamicV3FailureAttributionPolicyConfig,
    real_evaluation_report: Mapping[str, Any],
    real_evaluation_report_path: Path | None = None,
    real_policy: DynamicV3RealEvaluationPolicyConfig,
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
    allow_non_reject_for_validation: bool = False,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    _validate_real_evaluation_report(
        real_evaluation_report,
        policy,
        allow_non_reject_for_validation=allow_non_reject_for_validation,
    )
    requested_start, requested_end = _requested_range_from_real_report(
        real_evaluation_report,
        fallback_start=start or policy.market_regime.default_backtest_start,
        fallback_end=end,
    )
    if start is not None:
        requested_start = start
    if end is not None:
        requested_end = end
    best_v0_3_policy_id = _best_v0_3_policy_id(real_evaluation_report)
    v0_4_policy_id = policy.inputs.v0_4_policy_id
    materialized = _materialized_policy_set(
        dynamic_policy=dynamic_policy,
        failure_policy=failure_policy,
        v3_rescue_policy=v3_rescue_policy,
        real_policy=real_policy,
    )
    policies = _mapping(materialized.get("policies"))
    groups = _mapping(materialized.get("groups"))
    missing = [item for item in (best_v0_3_policy_id, v0_4_policy_id) if item not in policies]
    if missing:
        raise DynamicV3FailureAttributionError(
            "attribution policy set missing required policies: " + ", ".join(missing)
        )

    robustness_reports: dict[str, dict[str, Any]] = {}
    for label in (best_v0_3_policy_id, v0_4_policy_id):
        robustness_reports[label] = build_dynamic_robustness_report(
            prices=prices,
            etf_config=etf_config,
            policy=dynamic_robustness_policy,
            dynamic_policy=policies[label],
            candidate_id=label,
            start=requested_start,
            end=requested_end,
            data_quality_status=data_quality_status,
            data_quality_report=data_quality_report,
            prices_path=prices_path,
        )

    v0_3_report = robustness_reports[best_v0_3_policy_id]
    v0_4_report = robustness_reports[v0_4_policy_id]
    v0_3_row = _dynamic_row_from_robustness(
        label=best_v0_3_policy_id,
        group=_text(groups.get(best_v0_3_policy_id), "dynamic_v0_3_rescue"),
        robustness_report=v0_3_report,
    )
    v0_4_row = _dynamic_row_from_robustness(
        label=v0_4_policy_id,
        group=_text(groups.get(v0_4_policy_id), "dynamic_v0_4"),
        robustness_report=v0_4_report,
    )
    static_v0_3_row = _static_row_from_robustness(v0_3_report)
    static_v0_4_row = _static_row_from_robustness(v0_4_report)
    v0_3_daily = _records(
        _mapping(v0_3_report.get("comparison_daily_paths")).get("dynamic_candidate")
    )
    v0_4_daily = _records(
        _mapping(v0_4_report.get("comparison_daily_paths")).get("dynamic_candidate")
    )
    v0_3_constraint = _constraint_hit_breakdown(
        v0_3_daily,
        policy=policy,
        allocation_policy=policies[best_v0_3_policy_id],
    )
    v0_4_constraint = _constraint_hit_breakdown(
        v0_4_daily,
        policy=policy,
        allocation_policy=policies[v0_4_policy_id],
    )
    real_gate = _mapping(real_evaluation_report.get("promotion_gate"))
    metric_delta_table = _metric_delta_table(v0_3_row, v0_4_row)
    constraint_bucket_breakdown = {
        "best_v0_3_policy": best_v0_3_policy_id,
        "v0_4_policy": v0_4_policy_id,
        "v0_3": v0_3_constraint,
        "v0_4": v0_4_constraint,
        "hit_count_delta_v0_3_minus_v0_4": (
            _int(v0_3_constraint.get("constraint_hit_count"))
            - _int(v0_4_constraint.get("constraint_hit_count"))
        ),
        "hit_reduction_v0_3_vs_v0_4": (
            _int(v0_4_constraint.get("constraint_hit_count"))
            - _int(v0_3_constraint.get("constraint_hit_count"))
        ),
        "concentration_assessment": _constraint_concentration_assessment(
            v0_3_constraint,
            v0_4_constraint,
            policy,
        ),
    }
    smooth_assessment = _constraint_smooth_assessment(
        v0_3_row=v0_3_row,
        v0_4_row=v0_4_row,
        v0_3_constraint=v0_3_constraint,
        v0_4_constraint=v0_4_constraint,
        policy=policy,
    )
    drawdown_attribution = _drawdown_attribution(
        v0_3_row=v0_3_row,
        v0_4_row=v0_4_row,
        static_v0_3_row=static_v0_3_row,
        static_v0_4_row=static_v0_4_row,
        v0_3_daily=v0_3_daily,
        v0_4_daily=v0_4_daily,
    )
    overfit_explanation = _overfit_review_required_explanation(
        v0_3_report=v0_3_report,
        v0_4_report=v0_4_report,
        best_v0_3_policy_id=best_v0_3_policy_id,
        v0_4_policy_id=v0_4_policy_id,
    )
    advantage_source = _v0_4_advantage_source(
        real_evaluation_report=real_evaluation_report,
        v0_3_row=v0_3_row,
        v0_4_row=v0_4_row,
        policy=policy,
    )
    v0_4_review = _v0_4_promotion_review(
        v0_3_row=v0_3_row,
        v0_4_row=v0_4_row,
        static_v0_4_row=static_v0_4_row,
        v0_4_constraint=v0_4_constraint,
        overfit_explanation=overfit_explanation,
        policy=policy,
    )
    v0_5_recommendation = _v0_5_design_recommendation(
        v0_4_review=v0_4_review,
        v0_4_constraint=v0_4_constraint,
        policy=policy,
    )
    rejection_attribution = _rejection_attribution(
        real_gate=real_gate,
        real_evaluation_report=real_evaluation_report,
        smooth_assessment=smooth_assessment,
        overfit_explanation=overfit_explanation,
        drawdown_attribution=drawdown_attribution,
    )
    report_id = _stable_id(
        "dynamic-v3-failure-attribution-report",
        best_v0_3_policy_id,
        v0_4_policy_id,
        requested_start.isoformat(),
        "" if requested_end is None else requested_end.isoformat(),
        _stable_hash(metric_delta_table),
    )
    payload = {
        "schema_version": DYNAMIC_V3_FAILURE_ATTRIBUTION_REPORT_SCHEMA_VERSION,
        "report_type": DYNAMIC_V3_FAILURE_ATTRIBUTION_REPORT_TYPE,
        "dynamic_v3_failure_attribution_report_id": report_id,
        "generated_at": generated.isoformat(),
        "status": v0_4_review["review_status"],
        "policy_version": policy.policy_metadata.version,
        "policy_config_hash": _stable_hash(policy.model_dump(mode="json")),
        "real_evaluation_report_id": real_evaluation_report.get(
            "dynamic_v3_real_evaluation_report_id"
        ),
        "real_evaluation_decision": real_evaluation_report.get("promotion_gate_decision"),
        "market_regime": policy.market_regime.model_dump(mode="json"),
        "requested_range": {
            "start": requested_start.isoformat(),
            "end": "" if requested_end is None else requested_end.isoformat(),
        },
        "summary": {
            "best_v0_3_candidate": best_v0_3_policy_id,
            "v0_4_policy": v0_4_policy_id,
            "v0_3_rejection_primary_reason": rejection_attribution["primary_reason"],
            "v0_4_promotion_review": v0_4_review["review_status"],
            "v0_5_design_recommendation": v0_5_recommendation["recommendation_status"],
            "data_quality_status": data_quality_status,
            "constraint_hit_reduction_vs_v0_4": constraint_bucket_breakdown[
                "hit_reduction_v0_3_vs_v0_4"
            ],
            "v0_3_constraint_hit_rate": v0_3_constraint["constraint_hit_rate"],
            "v0_4_constraint_hit_rate": v0_4_constraint["constraint_hit_rate"],
            "v0_3_overfit_status": overfit_explanation["v0_3"]["status"],
            "v0_4_overfit_status": overfit_explanation["v0_4"]["status"],
        },
        "v0_3_rejection_attribution": rejection_attribution,
        "v0_3_vs_v0_4_metric_delta_table": metric_delta_table,
        "constraint_hit_failure_bucket_breakdown": constraint_bucket_breakdown,
        "constraint_smooth_mechanism_assessment": smooth_assessment,
        "drawdown_degradation_attribution": drawdown_attribution,
        "robustness_overfit_review_required_explanation": overfit_explanation,
        "v0_4_advantage_source_analysis": advantage_source,
        "v0_4_promotion_review": v0_4_review,
        "v0_5_design_recommendation": v0_5_recommendation,
        "reader_brief_summary": _reader_brief_summary(
            rejection_attribution,
            v0_4_review,
            v0_5_recommendation,
        ),
        "source_artifacts": {
            "real_evaluation_report": (
                "" if real_evaluation_report_path is None else str(real_evaluation_report_path)
            ),
            "prices_path": "" if prices_path is None else str(prices_path),
            "data_quality_report": data_quality_report,
            "failure_attribution_policy_config": str(
                DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_POLICY_CONFIG_PATH
            ),
            "real_evaluation_policy_config": str(
                DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH
            ),
            "v3_rescue_policy_config": str(DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH),
            "dynamic_robustness_policy_config": str(DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH),
            "dynamic_allocation_policy_config": str(DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH),
            "failure_diagnostics_policy_config": str(
                DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH
            ),
        },
        "validation_context": {
            "validate_data_status": data_quality_status,
            "validate_data_report": data_quality_report,
            "price_driven_daily_paths_rebuilt_from_source_configs": True,
            "daily_paths_persisted_in_report": False,
            "real_evaluation_reject_required": policy.inputs.require_real_evaluation_reject,
            "does_not_execute_approval_or_enrollment": True,
            "does_not_mutate_source_policy_config": True,
        },
        "safety": policy.safety.model_dump(mode="json"),
        **DYNAMIC_V3_FAILURE_ATTRIBUTION_SAFETY,
        "commands_executed": False,
    }
    _assert_dynamic_v3_failure_attribution_payload_safe(payload)
    return payload


def build_dynamic_v3_failure_attribution_validation_report(
    *,
    config_path: Path | str = DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_POLICY_CONFIG_PATH,
    real_evaluation_config_path: Path | str = DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH,
    v3_rescue_config_path: Path | str = DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH,
    dynamic_robustness_config_path: Path | str = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Path | str = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    failure_diagnostics_config_path: (
        Path | str
    ) = DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH,
    report_registry_path: Path = PROJECT_ROOT / "config" / "report_registry.yaml",
    reader_brief_path: Path = PROJECT_ROOT
    / "src"
    / "ai_trading_system"
    / "reports"
    / "reader_brief.py",
    command_owner_path: Path = PROJECT_ROOT
    / "src"
    / "ai_trading_system"
    / "interfaces"
    / "cli"
    / "etf_portfolio"
    / "dynamic_v3_failure_attribution.py",
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    checks: list[dict[str, Any]] = []
    policy: DynamicV3FailureAttributionPolicyConfig | None = None
    sample_report: dict[str, Any] | None = None
    try:
        policy = load_dynamic_v3_failure_attribution_policy_config(config_path)
        _append_check(checks, "failure_attribution_config_valid", True, "policy config loads")
    except Exception as exc:  # noqa: BLE001
        _append_check(checks, "failure_attribution_config_valid", False, str(exc))
    if policy is not None:
        try:
            sample_report = build_dynamic_v3_failure_attribution_validation_sample_report(
                config_path=config_path,
                real_evaluation_config_path=real_evaluation_config_path,
                v3_rescue_config_path=v3_rescue_config_path,
                dynamic_robustness_config_path=dynamic_robustness_config_path,
                dynamic_allocation_config_path=dynamic_allocation_config_path,
                failure_diagnostics_config_path=failure_diagnostics_config_path,
            )
            _append_check(
                checks,
                "attribution_report_builds",
                sample_report.get("report_type") == DYNAMIC_V3_FAILURE_ATTRIBUTION_REPORT_TYPE,
                "synthetic price-driven failure attribution report builds",
            )
            _append_check(
                checks,
                "required_sections_visible",
                all(
                    sample_report.get(key)
                    for key in (
                        "v0_3_rejection_attribution",
                        "v0_3_vs_v0_4_metric_delta_table",
                        "constraint_hit_failure_bucket_breakdown",
                        "drawdown_degradation_attribution",
                        "robustness_overfit_review_required_explanation",
                        "v0_4_promotion_review",
                        "v0_5_design_recommendation",
                    )
                ),
                "all TRADING-092 required report sections are visible",
            )
            _append_check(
                checks,
                "daily_path_rebuild_declared",
                _mapping(sample_report.get("validation_context")).get(
                    "price_driven_daily_paths_rebuilt_from_source_configs"
                )
                is True,
                "daily path details are rebuilt from source configs for attribution",
            )
            _append_check(
                checks,
                "v0_4_review_label_valid",
                _mapping(sample_report.get("v0_4_promotion_review")).get("review_status")
                in {
                    policy.v0_4_promotion_review.promote_v0_4_status,
                    policy.v0_4_promotion_review.observe_v0_4_with_constraint_guard_status,
                    policy.v0_4_promotion_review.do_not_promote_v0_4_status,
                },
                "v0.4 review status is one of the approved labels",
            )
            _append_check(
                checks,
                "v0_5_recommendation_label_valid",
                _mapping(sample_report.get("v0_5_design_recommendation")).get(
                    "recommendation_status"
                )
                in {
                    policy.v0_5_design.not_required_status,
                    policy.v0_5_design.constraint_guard_status,
                    policy.v0_5_design.exposure_redesign_status,
                },
                "v0.5 recommendation status is one of the approved labels",
            )
            _assert_dynamic_v3_failure_attribution_payload_safe(sample_report)
            _append_check(
                checks,
                "safety_boundary_enforced",
                True,
                "report keeps no approval, no enrollment, and no production mutation",
            )
        except Exception as exc:  # noqa: BLE001
            _append_check(checks, "attribution_report_builds", False, str(exc))
    registry_text = _read_text(report_registry_path)
    _append_check(
        checks,
        "report_registry_visible",
        DYNAMIC_V3_FAILURE_ATTRIBUTION_REPORT_TYPE in registry_text
        and DYNAMIC_V3_FAILURE_ATTRIBUTION_VALIDATION_REPORT_TYPE in registry_text,
        "report registry includes failure attribution report and validation gate",
    )
    reader_text = _read_text(reader_brief_path)
    _append_check(
        checks,
        "reader_brief_visible",
        "_etf_dynamic_v3_failure_attribution_summary" in reader_text
        and "Dynamic v0.3 Failure Attribution" in reader_text,
        "Reader Brief exposes Dynamic v0.3 Failure Attribution section",
    )
    cli_text = _read_text(command_owner_path)
    _append_check(
        checks,
        "cli_commands_visible",
        "failure-attribution" in cli_text
        and "failure-attribution-report" in cli_text
        and "validate-attribution" in cli_text,
        "CLI exposes failure-attribution, failure-attribution-report, and validate-attribution",
    )
    failed = [item for item in checks if item["status"] != "PASS"]
    payload = {
        "schema_version": DYNAMIC_V3_FAILURE_ATTRIBUTION_VALIDATION_SCHEMA_VERSION,
        "report_type": DYNAMIC_V3_FAILURE_ATTRIBUTION_VALIDATION_REPORT_TYPE,
        "validation_id": _stable_id(
            "dynamic-v3-failure-attribution-validation",
            _stable_hash([item["status"] for item in checks]),
        ),
        "generated_at": generated.isoformat(),
        "status": "PASS" if not failed else "FAIL",
        "failed_check_count": len(failed),
        "checks": checks,
        "sample_report_id": (
            ""
            if sample_report is None
            else sample_report["dynamic_v3_failure_attribution_report_id"]
        ),
        "production_effect_none_required": True,
        "broker_action_none_required": True,
        "no_auto_approval": True,
        "no_auto_enrollment": True,
        "failure_attribution_mutates_production_policy": False,
        "failure_attribution_auto_promotes_candidate": False,
        "safety": (
            DYNAMIC_V3_FAILURE_ATTRIBUTION_SAFETY
            if policy is None
            else policy.safety.model_dump(mode="json")
        ),
        **DYNAMIC_V3_FAILURE_ATTRIBUTION_SAFETY,
        "commands_executed": False,
    }
    _assert_dynamic_v3_failure_attribution_payload_safe(payload)
    return payload


def write_dynamic_v3_failure_attribution_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_REPORT_DIR,
) -> dict[str, Path]:
    return _write_report_pair(
        payload=payload,
        output_dir=output_dir,
        stem=_text(
            payload.get("dynamic_v3_failure_attribution_report_id"),
            "dynamic-v3-failure-attribution-report",
        ),
        markdown=render_dynamic_v3_failure_attribution_markdown(payload),
    )


def write_dynamic_v3_failure_attribution_validation_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_VALIDATION_DIR,
) -> dict[str, Path]:
    return _write_report_pair(
        payload=payload,
        output_dir=output_dir,
        stem=_text(payload.get("validation_id"), "dynamic-v3-failure-attribution-validation"),
        markdown=render_dynamic_v3_failure_attribution_validation_markdown(payload),
    )


def render_dynamic_v3_failure_attribution_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    rejection = _mapping(payload.get("v0_3_rejection_attribution"))
    smooth = _mapping(payload.get("constraint_smooth_mechanism_assessment"))
    drawdown = _mapping(payload.get("drawdown_degradation_attribution"))
    overfit = _mapping(payload.get("robustness_overfit_review_required_explanation"))
    review = _mapping(payload.get("v0_4_promotion_review"))
    v05 = _mapping(payload.get("v0_5_design_recommendation"))
    lines = [
        "# Dynamic v0.3 Failure Attribution "
        f"{payload.get('dynamic_v3_failure_attribution_report_id')}",
        "",
        "## Summary",
        "",
        f"- v0.3 Reject Primary Reason: {summary.get('v0_3_rejection_primary_reason')}",
        f"- Best v0.3 Candidate: {summary.get('best_v0_3_candidate')}",
        f"- v0.4 Review: {summary.get('v0_4_promotion_review')}",
        f"- v0.5 Recommendation: {summary.get('v0_5_design_recommendation')}",
        f"- Data Quality: {summary.get('data_quality_status')}",
        f"- Production Effect: {payload.get('production_effect')}",
        "",
        "## v0.3 Rejection Attribution",
        "",
        f"- Conclusion: {rejection.get('conclusion')}",
        f"- Primary Failure Bucket: {rejection.get('primary_failure_bucket')}",
        f"- TRADING-091 Blockers: {', '.join(rejection.get('promotion_gate_blockers', []))}",
        "",
        "## v0.3 vs v0.4 Metric Delta",
        "",
        "| Metric | v0.3 | v0.4 | Delta v0.3-v0.4 | Interpretation |",
        "|---|---:|---:|---:|---|",
    ]
    for row in _records(payload.get("v0_3_vs_v0_4_metric_delta_table")):
        lines.append(
            f"| {row.get('metric')} | {_fmt_value(row.get('v0_3'))} | "
            f"{_fmt_value(row.get('v0_4'))} | {_fmt_value(row.get('delta_v0_3_minus_v0_4'))} | "
            f"{row.get('interpretation')} |"
        )
    lines.extend(
        [
            "",
            "## Constraint Hit Failure Buckets",
            "",
        ]
    )
    buckets = _mapping(payload.get("constraint_hit_failure_bucket_breakdown"))
    for label in ("v0_3", "v0_4"):
        side = _mapping(buckets.get(label))
        lines.extend(
            [
                f"### {label}",
                "",
                f"- Constraint Hits: {side.get('constraint_hit_count')}",
                f"- Constraint Hit Rate: {_fmt_pct(side.get('constraint_hit_rate'))}",
                f"- Guard-Fixable Share: {_fmt_pct(side.get('guard_fixable_share'))}",
                "",
                "| Reason | Count | Share |",
                "|---|---:|---:|",
            ]
        )
        for item in _records(side.get("reason_bucket_breakdown"))[:8]:
            lines.append(
                f"| {item.get('bucket')} | {item.get('count')} | {_fmt_pct(item.get('share'))} |"
            )
        lines.append("")
    lines.extend(
        [
            "## Mechanism Assessment",
            "",
            f"- Constraint Smooth: {smooth.get('conclusion')}",
            "- Exposure Path: "
            f"{_mapping(payload.get('v0_4_advantage_source_analysis')).get('conclusion')}",
            "",
            "## Drawdown Attribution",
            "",
            f"- Conclusion: {drawdown.get('conclusion')}",
            f"- v0.3 Max Drawdown: {_fmt_pct(drawdown.get('v0_3_max_drawdown'))}",
            f"- v0.4 Max Drawdown: {_fmt_pct(drawdown.get('v0_4_max_drawdown'))}",
            f"- Degradation vs v0.4: {_fmt_pct(drawdown.get('v0_3_degradation_vs_v0_4'))}",
            "",
            "## Overfit Review",
            "",
            f"- v0.3: {_mapping(overfit.get('v0_3')).get('status')} "
            f"failed={_mapping(overfit.get('v0_3')).get('failed_finding_count')}",
            f"- v0.4: {_mapping(overfit.get('v0_4')).get('status')} "
            f"failed={_mapping(overfit.get('v0_4')).get('failed_finding_count')}",
            f"- Explanation: {overfit.get('conclusion')}",
            "",
            "## v0.4 Promotion Review",
            "",
            f"- Review Status: {review.get('review_status')}",
            f"- Conclusion: {review.get('conclusion')}",
            "",
            "| Check | Status | Observed | Threshold |",
            "|---|---|---:|---:|",
        ]
    )
    for check in _records(review.get("checks")):
        lines.append(
            f"| {check.get('check_id')} | {check.get('status')} | "
            f"{_fmt_value(check.get('observed'))} | {_fmt_value(check.get('threshold'))} |"
        )
    lines.extend(
        [
            "",
            "## v0.5 Design Recommendation",
            "",
            f"- Recommendation: {v05.get('recommendation_status')}",
            f"- Priority: {v05.get('priority')}",
            f"- Rationale: {v05.get('rationale')}",
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


def render_dynamic_v3_failure_attribution_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    lines = [
        f"# Dynamic v0.3 Failure Attribution Validation {payload.get('validation_id')}",
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


def _validate_real_evaluation_report(
    payload: Mapping[str, Any],
    policy: DynamicV3FailureAttributionPolicyConfig,
    *,
    allow_non_reject_for_validation: bool,
) -> None:
    if payload.get("report_type") != policy.inputs.required_real_evaluation_report_type:
        raise DynamicV3FailureAttributionError(
            "failure attribution requires a dynamic v0.3 real evaluation report"
        )
    if payload.get("report_type") != DYNAMIC_V3_REAL_EVALUATION_REPORT_TYPE:
        raise DynamicV3FailureAttributionError("real evaluation report type is not recognized")
    decision = _text(payload.get("promotion_gate_decision"))
    if (
        policy.inputs.require_real_evaluation_reject
        and not allow_non_reject_for_validation
        and decision != "reject"
    ):
        raise DynamicV3FailureAttributionError(
            "failure attribution requires latest TRADING-091 decision=reject"
        )
    if not _best_v0_3_policy_id(payload):
        raise DynamicV3FailureAttributionError("real evaluation report missing best v0.3 candidate")


def _requested_range_from_real_report(
    payload: Mapping[str, Any],
    *,
    fallback_start: date,
    fallback_end: date | None,
) -> tuple[date, date | None]:
    requested = _mapping(payload.get("requested_range"))
    start = _parse_date(_text(requested.get("start"))) or fallback_start
    end = _parse_date(_text(requested.get("end"))) or fallback_end
    return start, end


def _best_v0_3_policy_id(payload: Mapping[str, Any]) -> str:
    best = _mapping(payload.get("best_candidate"))
    summary = _mapping(payload.get("summary"))
    return _text(best.get("policy_id")) or _text(summary.get("best_v0_3_candidate"))


def _dynamic_row_from_robustness(
    *,
    label: str,
    group: str,
    robustness_report: Mapping[str, Any],
) -> dict[str, Any]:
    comparison = _records(robustness_report.get("comparison_table"))
    dynamic_row = next(row for row in comparison if row.get("comparison_id") == "dynamic_candidate")
    static_row = next(
        row for row in comparison if row.get("comparison_id") == "static_base_candidate"
    )
    daily = _mapping(robustness_report.get("daily_path_summary"))
    summary = _mapping(robustness_report.get("summary"))
    walk_forward = _mapping(robustness_report.get("walk_forward"))
    overfit = _mapping(robustness_report.get("overfit_diagnostics"))
    regime = _mapping(robustness_report.get("regime_attribution"))
    constraint_hits = _int(daily.get("constraint_hit_count"))
    row_count = max(1, _int(daily.get("row_count")))
    max_drawdown = _float(dynamic_row.get("max_drawdown"))
    static_max_drawdown = _float(static_row.get("max_drawdown"))
    return {
        "policy_id": label,
        "group": group,
        "status": robustness_report.get("status"),
        "total_return": _float(dynamic_row.get("total_return")),
        "CAGR": _float(dynamic_row.get("CAGR")),
        "max_drawdown": max_drawdown,
        "turnover": _float(dynamic_row.get("turnover")),
        "Sharpe": dynamic_row.get("Sharpe"),
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
        "overfit_status": overfit.get("status"),
        "overfit_failed_finding_count": _int(overfit.get("failed_finding_count")),
        "single_window_return_share": _overfit_finding_value(overfit, "single_period_dependency"),
        "regime_return_concentration": _float(regime.get("max_positive_return_share")),
    }


def _static_row_from_robustness(robustness_report: Mapping[str, Any]) -> dict[str, Any]:
    comparison = _records(robustness_report.get("comparison_table"))
    row = next(item for item in comparison if item.get("comparison_id") == "static_base_candidate")
    return {
        "policy_id": "static_base_candidate",
        "total_return": _float(row.get("total_return")),
        "max_drawdown": _float(row.get("max_drawdown")),
        "turnover": _float(row.get("turnover")),
    }


def _metric_delta_table(
    v0_3_row: Mapping[str, Any],
    v0_4_row: Mapping[str, Any],
) -> list[dict[str, Any]]:
    specs = [
        ("total_return", "higher_is_better"),
        ("max_drawdown", "less_negative_is_better"),
        ("turnover", "lower_is_better"),
        ("constraint_hit_count", "lower_is_better"),
        ("constraint_hit_rate", "lower_is_better"),
        ("false_risk_off_count", "lower_is_better"),
        ("drawdown_preservation_vs_static", "higher_is_better"),
        ("dynamic_vs_static_gap", "higher_is_better"),
        ("walk_forward_pass_ratio", "higher_is_better"),
        ("overfit_failed_finding_count", "lower_is_better"),
        ("regime_return_concentration", "lower_is_better"),
    ]
    rows: list[dict[str, Any]] = []
    for metric, direction in specs:
        v03 = _float(v0_3_row.get(metric))
        v04 = _float(v0_4_row.get(metric))
        delta = v03 - v04
        rows.append(
            {
                "metric": metric,
                "v0_3": v03,
                "v0_4": v04,
                "delta_v0_3_minus_v0_4": delta,
                "direction": direction,
                "interpretation": _metric_interpretation(metric, delta, direction),
            }
        )
    rows.append(
        {
            "metric": "max_drawdown_abs_degradation",
            "v0_3": abs(_float(v0_3_row.get("max_drawdown"))),
            "v0_4": abs(_float(v0_4_row.get("max_drawdown"))),
            "delta_v0_3_minus_v0_4": abs(_float(v0_3_row.get("max_drawdown")))
            - abs(_float(v0_4_row.get("max_drawdown"))),
            "direction": "lower_is_better",
            "interpretation": (
                "v0.3 drawdown is less severe than v0.4"
                if abs(_float(v0_3_row.get("max_drawdown")))
                <= abs(_float(v0_4_row.get("max_drawdown")))
                else "v0.3 drawdown is more severe than v0.4"
            ),
        }
    )
    return rows


def _metric_interpretation(metric: str, delta: float, direction: str) -> str:
    if abs(delta) <= 1e-12:
        return f"{metric} is unchanged"
    if direction == "higher_is_better":
        return "v0.3 is better than v0.4" if delta > 0 else "v0.3 is worse than v0.4"
    if direction == "lower_is_better":
        return "v0.3 is better than v0.4" if delta < 0 else "v0.3 is worse than v0.4"
    if direction == "less_negative_is_better":
        return "v0.3 is less severe than v0.4" if delta > 0 else "v0.3 is more severe than v0.4"
    return "delta requires manual review"


def _constraint_hit_breakdown(
    records: Sequence[Mapping[str, Any]],
    *,
    policy: DynamicV3FailureAttributionPolicyConfig,
    allocation_policy: DynamicAllocationPolicyConfig,
) -> dict[str, Any]:
    reason_counts: Counter[str] = Counter()
    regime_counts: Counter[str] = Counter()
    ticker_counts: Counter[str] = Counter()
    rebalance_counts: Counter[str] = Counter()
    hit_events: list[dict[str, Any]] = []
    guard_fixable_hits = 0
    for row in records:
        reasons = _constraint_reason_codes(row)
        if not reasons:
            continue
        reason_counts.update(reasons)
        regime = _text(row.get("selected_regime"), "UNKNOWN")
        regime_counts[regime] += 1
        ticker_bucket = _ticker_bucket(row, reasons, allocation_policy, policy)
        ticker_counts[ticker_bucket] += 1
        rebalance_bucket = _rebalance_window_bucket(row, reasons)
        rebalance_counts[rebalance_bucket] += 1
        if any(reason in GUARD_FIXABLE_REASONS for reason in reasons):
            guard_fixable_hits += 1
        if len(hit_events) < policy.constraint_attribution.top_event_limit:
            hit_events.append(
                {
                    "signal_date": row.get("signal_date"),
                    "selected_regime": regime,
                    "rebalance_decision": row.get("rebalance_decision"),
                    "reason_codes": reasons,
                    "ticker_bucket": ticker_bucket,
                    "rebalance_window_bucket": rebalance_bucket,
                    "turnover": _float(row.get("turnover")),
                    "target_weights": _parse_json_mapping(row.get("target_weights_json")),
                    "pre_rebalance_candidate_weights": _parse_json_mapping(
                        row.get("pre_rebalance_candidate_weights_json")
                    ),
                    "trade_deltas": _parse_json_mapping(row.get("trade_deltas_json")),
                }
            )
    hit_count = sum(1 for row in records if _constraint_reason_codes(row))
    row_count = max(1, len(records))
    reason_breakdown = _bucket_rows(reason_counts, hit_count, policy)
    regime_breakdown = _bucket_rows(regime_counts, hit_count, policy)
    ticker_breakdown = _bucket_rows(ticker_counts, hit_count, policy)
    rebalance_breakdown = _bucket_rows(rebalance_counts, hit_count, policy)
    high_reason = reason_breakdown[0] if reason_breakdown else {}
    high_regime = regime_breakdown[0] if regime_breakdown else {}
    high_ticker = ticker_breakdown[0] if ticker_breakdown else {}
    high_rebalance = rebalance_breakdown[0] if rebalance_breakdown else {}
    return {
        "row_count": len(records),
        "constraint_hit_count": hit_count,
        "constraint_hit_rate": hit_count / row_count,
        "guard_fixable_hit_count": guard_fixable_hits,
        "guard_fixable_share": guard_fixable_hits / max(1, hit_count),
        "reason_bucket_breakdown": reason_breakdown,
        "regime_bucket_breakdown": regime_breakdown,
        "ticker_bucket_breakdown": ticker_breakdown,
        "rebalance_window_breakdown": rebalance_breakdown,
        "dominant_reason_bucket": high_reason,
        "dominant_regime_bucket": high_regime,
        "dominant_ticker_bucket": high_ticker,
        "dominant_rebalance_window_bucket": high_rebalance,
        "top_hit_events": hit_events,
    }


def _constraint_reason_codes(row: Mapping[str, Any]) -> list[str]:
    reasons = [str(item) for item in _parse_json_list(row.get("reason_codes_json"))]
    codes = sorted(set(reasons))
    return [code for code in codes if code.startswith(CONSTRAINT_REASON_PREFIXES)]


def _ticker_bucket(
    row: Mapping[str, Any],
    reasons: Sequence[str],
    allocation_policy: DynamicAllocationPolicyConfig,
    policy: DynamicV3FailureAttributionPolicyConfig,
) -> str:
    if any("SEMICONDUCTOR" in reason for reason in reasons):
        return "SMH_SOXX"
    if any("CASH" in reason for reason in reasons):
        return "CASH"
    diagnostics = _parse_json_list(row.get("constraint_diagnostics_json"))
    diagnostic_symbols = [
        _text(_mapping(item).get("symbol"))
        for item in diagnostics
        if _text(_mapping(item).get("symbol"))
    ]
    for symbol in diagnostic_symbols:
        if symbol in WEIGHT_SYMBOLS:
            return symbol
        if symbol == "SMH+SOXX":
            return "SMH_SOXX"
    trade_deltas = _parse_json_mapping(row.get("trade_deltas_json"))
    if trade_deltas:
        return max(trade_deltas, key=lambda symbol: abs(_float(trade_deltas.get(symbol))))
    weights = _parse_json_mapping(row.get("target_weights_json"))
    proximate = _cap_proximate_symbol(weights, allocation_policy, policy)
    return proximate or "portfolio"


def _cap_proximate_symbol(
    weights: Mapping[str, Any],
    allocation_policy: DynamicAllocationPolicyConfig,
    policy: DynamicV3FailureAttributionPolicyConfig,
) -> str:
    tolerance = policy.constraint_attribution.cap_proximity_tolerance
    constraints = allocation_policy.exposure_constraints
    for symbol in WEIGHT_SYMBOLS:
        weight = _float(weights.get(symbol))
        cap = constraints.asset_caps.get(symbol)
        floor = constraints.asset_floors.get(symbol)
        if cap is not None and abs(weight - cap) <= tolerance:
            return symbol
        if floor is not None and abs(weight - floor) <= tolerance:
            return symbol
    semi_weight = sum(_float(weights.get(symbol)) for symbol in SEMICONDUCTOR_SYMBOLS)
    if abs(semi_weight - constraints.semiconductor_sleeve_max) <= tolerance:
        return "SMH_SOXX"
    return ""


def _rebalance_window_bucket(row: Mapping[str, Any], reasons: Sequence[str]) -> str:
    if any(reason.startswith("REGIME_CONFIRMATION_WINDOW") for reason in reasons):
        return "regime_confirmation_window"
    if "WEEKLY_TURNOVER_CAP" in reasons:
        return "weekly_turnover_cap_window"
    if "MAX_SINGLE_REBALANCE_DELTA" in reasons:
        return "single_delta_cap_window"
    decision = _text(row.get("rebalance_decision"), "unknown")
    if decision == "rebalance_candidate":
        return "rebalance_candidate_window"
    return "hold_window"


def _bucket_rows(
    counts: Counter[str],
    total_hits: int,
    policy: DynamicV3FailureAttributionPolicyConfig,
) -> list[dict[str, Any]]:
    rows = [
        {
            "bucket": bucket,
            "count": count,
            "share": count / max(1, total_hits),
            "high_concentration": count / max(1, total_hits)
            >= policy.constraint_attribution.high_bucket_share,
        }
        for bucket, count in counts.most_common(policy.constraint_attribution.top_bucket_limit)
    ]
    return rows


def _constraint_concentration_assessment(
    v0_3_constraint: Mapping[str, Any],
    v0_4_constraint: Mapping[str, Any],
    policy: DynamicV3FailureAttributionPolicyConfig,
) -> dict[str, Any]:
    v03_reason = _mapping(v0_3_constraint.get("dominant_reason_bucket"))
    v04_reason = _mapping(v0_4_constraint.get("dominant_reason_bucket"))
    v03_regime = _mapping(v0_3_constraint.get("dominant_regime_bucket"))
    v04_regime = _mapping(v0_4_constraint.get("dominant_regime_bucket"))
    concentrated = any(
        _float(bucket.get("share")) >= policy.constraint_attribution.high_bucket_share
        for bucket in (v03_reason, v04_reason, v03_regime, v04_regime)
    )
    return {
        "is_concentrated": concentrated,
        "v0_3_dominant_reason": v03_reason,
        "v0_4_dominant_reason": v04_reason,
        "v0_3_dominant_regime": v03_regime,
        "v0_4_dominant_regime": v04_regime,
        "conclusion": (
            "constraint hit 集中在少数 reason/regime bucket，可优先评估 targeted guard。"
            if concentrated
            else "constraint hit 未集中在单一 reason/regime bucket，需要检查 exposure path。"
        ),
    }


def _constraint_smooth_assessment(
    *,
    v0_3_row: Mapping[str, Any],
    v0_4_row: Mapping[str, Any],
    v0_3_constraint: Mapping[str, Any],
    v0_4_constraint: Mapping[str, Any],
    policy: DynamicV3FailureAttributionPolicyConfig,
) -> dict[str, Any]:
    hit_reduction = _int(v0_4_constraint.get("constraint_hit_count")) - _int(
        v0_3_constraint.get("constraint_hit_count")
    )
    turnover_reduction = _float(v0_4_row.get("turnover")) - _float(v0_3_row.get("turnover"))
    material_hit_reduction = hit_reduction >= policy.constraint_attribution.material_hit_count_delta
    smooth_only = turnover_reduction > 0 and not material_hit_reduction
    return {
        "hit_reduction_vs_v0_4": hit_reduction,
        "turnover_reduction_vs_v0_4": turnover_reduction,
        "material_hit_reduction_threshold": policy.constraint_attribution.material_hit_count_delta,
        "smooth_only_noise_reduction": smooth_only,
        "conclusion": (
            "v0.3 smoothing 主要降低 turnover / 切换噪音，但没有实质改变触发 "
            "constraint hit 的权重结构。"
            if smooth_only
            else (
                "v0.3 smoothing 对 constraint hit 有可见改善，但仍需结合 bucket "
                "分布复核是否足够稳定。"
            )
        ),
    }


def _drawdown_attribution(
    *,
    v0_3_row: Mapping[str, Any],
    v0_4_row: Mapping[str, Any],
    static_v0_3_row: Mapping[str, Any],
    static_v0_4_row: Mapping[str, Any],
    v0_3_daily: Sequence[Mapping[str, Any]],
    v0_4_daily: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    degradation = abs(_float(v0_3_row.get("max_drawdown"))) - abs(
        _float(v0_4_row.get("max_drawdown"))
    )
    v03_event = _worst_drawdown_event(v0_3_daily)
    v04_event = _worst_drawdown_event(v0_4_daily)
    return {
        "v0_3_max_drawdown": v0_3_row.get("max_drawdown"),
        "v0_4_max_drawdown": v0_4_row.get("max_drawdown"),
        "v0_3_degradation_vs_v0_4": degradation,
        "v0_3_drawdown_delta_vs_static": abs(_float(v0_3_row.get("max_drawdown")))
        - abs(_float(static_v0_3_row.get("max_drawdown"))),
        "v0_4_drawdown_delta_vs_static": abs(_float(v0_4_row.get("max_drawdown")))
        - abs(_float(static_v0_4_row.get("max_drawdown"))),
        "v0_3_worst_drawdown_window": v03_event,
        "v0_4_worst_drawdown_window": v04_event,
        "conclusion": (
            "未发现 v0.3 相对 v0.4 的 drawdown degradation；reject 主要不是 "
            "drawdown protection 被牺牲。"
            if degradation <= 0
            else (
                "v0.3 相对 v0.4 的 max drawdown 更深，需要复核 smoothing " "是否延迟风险暴露调整。"
            )
        ),
    }


def _worst_drawdown_event(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    peak = 0.0
    peak_date = ""
    worst = 0.0
    worst_row: Mapping[str, Any] = {}
    for row in records:
        equity = _float(row.get("portfolio_equity"))
        if equity > peak:
            peak = equity
            peak_date = _text(row.get("signal_date"))
        if peak <= 0:
            continue
        drawdown = equity / peak - 1.0
        if drawdown < worst:
            worst = drawdown
            worst_row = row
    return {
        "peak_signal_date": peak_date,
        "trough_signal_date": worst_row.get("signal_date") if worst_row else "",
        "max_drawdown": worst,
        "selected_regime": worst_row.get("selected_regime") if worst_row else "",
        "target_weights": (
            _parse_json_mapping(worst_row.get("target_weights_json")) if worst_row else {}
        ),
        "reason_codes": _parse_json_list(worst_row.get("reason_codes_json")) if worst_row else [],
    }


def _overfit_review_required_explanation(
    *,
    v0_3_report: Mapping[str, Any],
    v0_4_report: Mapping[str, Any],
    best_v0_3_policy_id: str,
    v0_4_policy_id: str,
) -> dict[str, Any]:
    v03 = _overfit_side(v0_3_report, best_v0_3_policy_id)
    v04 = _overfit_side(v0_4_report, v0_4_policy_id)
    failed_ids = sorted(
        {
            str(item.get("finding_id"))
            for item in [
                *_records(v03.get("failed_findings")),
                *_records(v04.get("failed_findings")),
            ]
        }
    )
    return {
        "v0_3": v03,
        "v0_4": v04,
        "shared_failed_finding_ids": failed_ids,
        "conclusion": (
            "overfit REVIEW_REQUIRED 来自配置化 robustness diagnostics 的失败 "
            "finding；该状态阻止 promotion，但不等同于 production 失败或自动 reject "
            "owner review。"
        ),
    }


def _overfit_side(report: Mapping[str, Any], policy_id: str) -> dict[str, Any]:
    overfit = _mapping(report.get("overfit_diagnostics"))
    findings = _records(overfit.get("findings"))
    failed = [item for item in findings if item.get("status") != "PASS"]
    return {
        "policy_id": policy_id,
        "status": overfit.get("status"),
        "risk_level": overfit.get("risk_level"),
        "failed_finding_count": _int(overfit.get("failed_finding_count")),
        "failed_findings": failed,
        "all_findings": findings,
    }


def _v0_4_advantage_source(
    *,
    real_evaluation_report: Mapping[str, Any],
    v0_3_row: Mapping[str, Any],
    v0_4_row: Mapping[str, Any],
    policy: DynamicV3FailureAttributionPolicyConfig,
) -> dict[str, Any]:
    comparison = _records(real_evaluation_report.get("comparison_table"))
    v02 = next((row for row in comparison if row.get("group") == "dynamic_v0_2"), {})
    v04_vs_v02_turnover_delta = _float(v0_4_row.get("turnover")) - _float(v02.get("turnover"))
    v04_vs_v02_static_gap_delta = _float(v0_4_row.get("dynamic_vs_static_gap")) - _float(
        v02.get("dynamic_vs_static_gap")
    )
    v04_vs_v03_static_gap_delta = _float(v0_4_row.get("dynamic_vs_static_gap")) - _float(
        v0_3_row.get("dynamic_vs_static_gap")
    )
    path_gap_tolerance = policy.v0_4_promotion_review.max_static_gap_loss_vs_v0_3_for_path_edge
    drawdown_tolerance = policy.v0_4_promotion_review.max_drawdown_degradation_vs_v0_3
    v04_has_path_edge = (
        v04_vs_v02_turnover_delta < 0
        and v04_vs_v02_static_gap_delta > 0
        and v04_vs_v03_static_gap_delta >= -path_gap_tolerance
        and abs(_float(v0_4_row.get("max_drawdown")))
        <= abs(_float(v0_3_row.get("max_drawdown"))) + drawdown_tolerance
    )
    return {
        "v0_4_vs_v0_2_turnover_delta": v04_vs_v02_turnover_delta,
        "v0_4_vs_v0_2_static_gap_delta": v04_vs_v02_static_gap_delta,
        "v0_4_vs_v0_3_static_gap_delta": v04_vs_v03_static_gap_delta,
        "path_gap_tolerance_vs_v0_3": path_gap_tolerance,
        "v0_4_exposure_path_edge_vs_v0_3": v04_has_path_edge,
        "conclusion": (
            "v0.4 的优势不是 lower_turnover 单项；相对 v0.2 它同时改善 turnover "
            "和 dynamic-vs-static gap，相对 v0.3 的 static-gap 损失仍在配置容忍内，"
            "因此可保留 v0.4 exposure path 并优先设计 constraint guard。"
            if v04_has_path_edge
            else ("v0.4 优势更多依赖 turnover 下降，exposure path 仍需 v0.5 " "重新设计验证。")
        ),
    }


def _v0_4_promotion_review(
    *,
    v0_3_row: Mapping[str, Any],
    v0_4_row: Mapping[str, Any],
    static_v0_4_row: Mapping[str, Any],
    v0_4_constraint: Mapping[str, Any],
    overfit_explanation: Mapping[str, Any],
    policy: DynamicV3FailureAttributionPolicyConfig,
) -> dict[str, Any]:
    cfg = policy.v0_4_promotion_review
    v04_overfit = _mapping(overfit_explanation.get("v0_4"))
    drawdown_degradation_vs_v03 = abs(_float(v0_4_row.get("max_drawdown"))) - abs(
        _float(v0_3_row.get("max_drawdown"))
    )
    false_delta_vs_v03 = _int(v0_4_row.get("false_risk_off_count")) - _int(
        v0_3_row.get("false_risk_off_count")
    )
    drawdown_delta_vs_static = abs(_float(v0_4_row.get("max_drawdown"))) - abs(
        _float(static_v0_4_row.get("max_drawdown"))
    )
    checks = [
        _review_check(
            "constraint_hit_rate",
            _float(v0_4_constraint.get("constraint_hit_rate"))
            <= cfg.max_promotable_constraint_hit_rate,
            v0_4_constraint.get("constraint_hit_rate"),
            cfg.max_promotable_constraint_hit_rate,
        ),
        _review_check(
            "overfit_failed_finding_count",
            _int(v04_overfit.get("failed_finding_count"))
            <= cfg.max_promotable_overfit_failed_findings,
            v04_overfit.get("failed_finding_count"),
            cfg.max_promotable_overfit_failed_findings,
        ),
        _review_check(
            "overfit_status",
            (not cfg.require_overfit_pass_for_promote) or v04_overfit.get("status") == "PASS",
            v04_overfit.get("status"),
            "PASS",
        ),
        _review_check(
            "drawdown_degradation_vs_v0_3",
            drawdown_degradation_vs_v03 <= cfg.max_drawdown_degradation_vs_v0_3,
            drawdown_degradation_vs_v03,
            cfg.max_drawdown_degradation_vs_v0_3,
        ),
        _review_check(
            "false_risk_off_delta_vs_v0_3",
            false_delta_vs_v03 <= cfg.max_false_risk_off_delta_vs_v0_3,
            false_delta_vs_v03,
            cfg.max_false_risk_off_delta_vs_v0_3,
        ),
        _review_check(
            "turnover",
            _float(v0_4_row.get("turnover")) <= cfg.max_turnover,
            v0_4_row.get("turnover"),
            cfg.max_turnover,
        ),
        _review_check(
            "dynamic_vs_static_gap",
            _float(v0_4_row.get("dynamic_vs_static_gap")) >= cfg.min_dynamic_vs_static_gap,
            v0_4_row.get("dynamic_vs_static_gap"),
            cfg.min_dynamic_vs_static_gap,
        ),
        _review_check(
            "dynamic_drawdown_delta_vs_static",
            drawdown_delta_vs_static <= cfg.max_dynamic_drawdown_delta_vs_static,
            drawdown_delta_vs_static,
            cfg.max_dynamic_drawdown_delta_vs_static,
        ),
    ]
    failed = [item for item in checks if item["status"] != "PASS"]
    guard_fixable = _float(v0_4_constraint.get("guard_fixable_share")) >= (
        policy.constraint_attribution.guard_fixable_bucket_share
    )
    hard_failed = [
        item
        for item in failed
        if item["check_id"]
        not in {"constraint_hit_rate", "overfit_failed_finding_count", "overfit_status"}
    ]
    if not failed:
        status = cfg.promote_v0_4_status
        conclusion = "v0.4 passes configured promotion review checks; still requires owner review."
    elif guard_fixable and not hard_failed:
        status = cfg.observe_v0_4_with_constraint_guard_status
        conclusion = (
            "v0.4 cannot promote as-is because constraint/overfit review remains open, "
            "but failure buckets are sufficiently guard-fixable to keep v0.4 as the lead path."
        )
    else:
        status = cfg.do_not_promote_v0_4_status
        conclusion = (
            "v0.4 cannot promote as-is and failures are not isolated enough for "
            "a simple guard-only fix."
        )
    return {
        "review_status": status,
        "checks": checks,
        "failed_check_ids": [str(item["check_id"]) for item in failed],
        "guard_fixable": guard_fixable,
        "guard_fixable_share": v0_4_constraint.get("guard_fixable_share"),
        "drawdown_degradation_vs_v0_3": drawdown_degradation_vs_v03,
        "false_risk_off_delta_vs_v0_3": false_delta_vs_v03,
        "conclusion": conclusion,
        "manual_review_required": True,
        "promotion_is_recommendation_only": True,
        "automatic_candidate_promotion": False,
        "shadow_enrollment_allowed": False,
        "production_state_mutated": False,
    }


def _review_check(check_id: str, passed: bool, observed: Any, threshold: Any) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": "PASS" if passed else "FAIL",
        "observed": observed,
        "threshold": threshold,
    }


def _v0_5_design_recommendation(
    *,
    v0_4_review: Mapping[str, Any],
    v0_4_constraint: Mapping[str, Any],
    policy: DynamicV3FailureAttributionPolicyConfig,
) -> dict[str, Any]:
    review_status = _text(v0_4_review.get("review_status"))
    if review_status == policy.v0_4_promotion_review.promote_v0_4_status:
        return {
            "recommendation_status": policy.v0_5_design.not_required_status,
            "priority": "promote_v0_4_after_owner_review",
            "rationale": "v0.4 promotion review passed; v0.5 design is not required by this gate.",
            "design_summary": "",
        }
    if review_status == policy.v0_4_promotion_review.observe_v0_4_with_constraint_guard_status:
        return {
            "recommendation_status": policy.v0_5_design.constraint_guard_status,
            "priority": "prioritize_v0_5_constraint_guard_on_v0_4_path",
            "rationale": (
                "v0.4 has usable false-risk-off / turnover / static-gap evidence, "
                "but direct promotion is blocked by concentrated constraint hits and "
                "overfit review."
            ),
            "design_summary": policy.v0_5_design.guard_design_summary,
            "guard_fixable_share": v0_4_constraint.get("guard_fixable_share"),
        }
    return {
        "recommendation_status": policy.v0_5_design.exposure_redesign_status,
        "priority": "enter_v0_5_exposure_redesign",
        "rationale": (
            "v0.4 failures are not sufficiently isolated for a guard-only fix; "
            "redesign exposure path."
        ),
        "design_summary": policy.v0_5_design.exposure_redesign_summary,
        "guard_fixable_share": v0_4_constraint.get("guard_fixable_share"),
    }


def _rejection_attribution(
    *,
    real_gate: Mapping[str, Any],
    real_evaluation_report: Mapping[str, Any],
    smooth_assessment: Mapping[str, Any],
    overfit_explanation: Mapping[str, Any],
    drawdown_attribution: Mapping[str, Any],
) -> dict[str, Any]:
    blockers = [str(item) for item in _list_values(real_gate.get("blocker_ids"))]
    critical = [str(item) for item in _list_values(real_gate.get("critical_blocker_ids"))]
    primary = "constraint_structure_not_fixed"
    if "robustness_overfit_status" in blockers and "constraint_hit_rate" not in blockers:
        primary = "robustness_overfit_review_required"
    elif "constraint_hit_rate" in blockers and "robustness_overfit_status" in blockers:
        primary = "constraint_structure_and_overfit_review"
    drawdown_not_primary = _float(drawdown_attribution.get("v0_3_degradation_vs_v0_4")) <= 0
    return {
        "primary_reason": primary,
        "primary_failure_bucket": primary,
        "promotion_gate_decision": real_evaluation_report.get("promotion_gate_decision"),
        "promotion_gate_blockers": blockers,
        "critical_blockers": critical,
        "smooth_only_noise_reduction": smooth_assessment.get("smooth_only_noise_reduction"),
        "drawdown_not_primary_failure": drawdown_not_primary,
        "overfit_v0_3_status": _mapping(overfit_explanation.get("v0_3")).get("status"),
        "conclusion": (
            "v0.3 reject 的主因是 constraint hit rate 未被结构性修复，并叠加 robustness overfit "
            "REVIEW_REQUIRED；turnover 降低没有转化为足够的 constraint-risk 改善。"
        ),
    }


def _reader_brief_summary(
    rejection: Mapping[str, Any],
    review: Mapping[str, Any],
    v05: Mapping[str, Any],
) -> str:
    return (
        "Dynamic v0.3 Failure Attribution: "
        f"reject_reason={rejection.get('primary_reason')}; "
        f"v0_4_review={review.get('review_status')}; "
        f"v0_5={v05.get('recommendation_status')}; "
        "结果仅用于人工复核，不代表 approval、shadow enrollment 或 production mutation。"
    )


def _append_check(
    checks: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    detail: str,
) -> None:
    checks.append({"check_id": check_id, "status": "PASS" if passed else "FAIL", "detail": detail})


def _assert_dynamic_v3_failure_attribution_payload_safe(payload: Mapping[str, Any]) -> None:
    for key, expected in DYNAMIC_V3_FAILURE_ATTRIBUTION_SAFETY.items():
        if payload.get(key) != expected:
            raise DynamicV3FailureAttributionError(
                f"unsafe failure attribution payload field {key}={payload.get(key)!r}"
            )
    if payload.get("commands_executed") is not False:
        raise DynamicV3FailureAttributionError(
            "failure attribution payload must not execute commands"
        )
    forbidden = _find_forbidden_key(payload, DYNAMIC_V3_FAILURE_ATTRIBUTION_FORBIDDEN_KEYS)
    if forbidden:
        raise DynamicV3FailureAttributionError(
            f"failure attribution payload contains forbidden key: {forbidden}"
        )


def _find_forbidden_key(value: Any, forbidden: set[str]) -> str:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key) in forbidden:
                return str(key)
            found = _find_forbidden_key(child, forbidden)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_forbidden_key(child, forbidden)
            if found:
                return found
    return ""


def _write_report_pair(
    *,
    payload: Mapping[str, Any],
    output_dir: Path,
    stem: str,
    markdown: str,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{stem}.json"
    md_path = output_dir / f"{stem}.md"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    md_path.write_text(markdown, encoding="utf-8")
    return {"json": json_path, "markdown": md_path}


def _latest_json(report_dir: Path, pattern: str) -> Path | None:
    if not report_dir.exists():
        return None
    matches = sorted(
        (path for path in report_dir.glob(pattern) if path.is_file()),
        key=lambda item: item.stat().st_mtime,
    )
    return matches[-1] if matches else None


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


def _path_cache_key(path: Path) -> tuple[str, str]:
    resolved = path.resolve()
    if not resolved.exists():
        return str(resolved), "missing"
    return str(resolved), sha256(resolved.read_bytes()).hexdigest()


def _stable_id(prefix: str, *parts: Any) -> str:
    digest = _stable_hash(parts)[:16]
    return f"{prefix}_{digest}"


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return sha256(encoded).hexdigest()


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _list_values(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _parse_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _parse_json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        raw = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return dict(raw) if isinstance(raw, Mapping) else {}


def _parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    try:
        raw = json.loads(str(value))
    except (TypeError, ValueError):
        return []
    return list(raw) if isinstance(raw, list) else []


def _overfit_finding_value(overfit: Mapping[str, Any], finding_id: str) -> float:
    for item in _records(overfit.get("findings")):
        if item.get("finding_id") == finding_id:
            return _float(item.get("observed"))
    return 0.0


def _fmt_pct(value: Any) -> str:
    return f"{_float(value) * 100:.2f}%"


def _fmt_value(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return str(value)
    if value is None:
        return ""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{numeric:.6g}"
