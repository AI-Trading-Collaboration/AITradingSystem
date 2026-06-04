from __future__ import annotations

import csv
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.models import PolicyMetadata
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_SHADOW_READY_REVIEW_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "shadow_ready_review.yaml"
)
DEFAULT_SHADOW_READY_REVIEW_REPORT_DIR = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "shadow_ready_review"
)
DEFAULT_SHADOW_READY_REVIEW_PACKAGE_DIR = (
    DEFAULT_SHADOW_READY_REVIEW_REPORT_DIR / "packages"
)
DEFAULT_SHADOW_READY_REVIEW_APPROVAL_DIR = (
    DEFAULT_SHADOW_READY_REVIEW_REPORT_DIR / "approvals"
)
DEFAULT_SHADOW_READY_REVIEW_ENROLLMENT_DIR = (
    DEFAULT_SHADOW_READY_REVIEW_REPORT_DIR / "enrollments"
)
DEFAULT_SHADOW_READY_REVIEW_VALIDATION_DIR = (
    DEFAULT_SHADOW_READY_REVIEW_REPORT_DIR / "validation"
)
DEFAULT_WEIGHT_SEARCH_DIAGNOSTICS_DIR = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "weight_calibration" / "search_diagnostics"
)

SHADOW_READY_REVIEW_POLICY_SCHEMA_VERSION = "etf_shadow_ready_review_policy_v1"
SHADOW_READY_REVIEW_ARTIFACTS_SCHEMA_VERSION = "etf_shadow_ready_review_artifacts_v1"
SHADOW_READY_REVIEW_AGGREGATION_SCHEMA_VERSION = (
    "etf_shadow_ready_candidate_aggregation_v1"
)
SHADOW_READY_REVIEW_RANKING_SCHEMA_VERSION = "etf_shadow_ready_review_ranking_v1"
SHADOW_READY_REVIEW_NEAR_SHADOW_SCHEMA_VERSION = "etf_near_shadow_review_summary_v1"
SHADOW_READY_REVIEW_PACKAGE_SCHEMA_VERSION = "etf_shadow_candidate_review_package_v1"
SHADOW_READY_REVIEW_APPROVAL_SCHEMA_VERSION = "etf_shadow_candidate_owner_approval_v1"
SHADOW_READY_REVIEW_ENROLLMENT_SCHEMA_VERSION = (
    "etf_shadow_candidate_approved_enrollment_v1"
)
SHADOW_READY_REVIEW_VALIDATION_SCHEMA_VERSION = "etf_shadow_candidate_review_validation_v1"

SHADOW_READY_REVIEW_PACKAGE_REPORT_TYPE = "etf_shadow_candidate_review_package"
SHADOW_READY_REVIEW_APPROVAL_REPORT_TYPE = "etf_shadow_candidate_owner_approval"
SHADOW_READY_REVIEW_ENROLLMENT_REPORT_TYPE = "etf_shadow_candidate_enrollment"
SHADOW_READY_REVIEW_VALIDATION_REPORT_TYPE = "etf_shadow_candidate_review_validation"

SHADOW_READY_REVIEW_PACKAGE_REGISTRY_ID = "etf_shadow_candidate_review_package"
SHADOW_READY_REVIEW_APPROVAL_REGISTRY_ID = "etf_shadow_candidate_owner_approval"
SHADOW_READY_REVIEW_ENROLLMENT_REGISTRY_ID = "etf_shadow_candidate_enrollment"
SHADOW_READY_REVIEW_VALIDATION_REGISTRY_ID = "etf_shadow_candidate_review_validation"

SHADOW_READY_REVIEW_SAFETY = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
}

AllowedOwnerDecision = Literal[
    "approved_for_shadow",
    "continue_review",
    "needs_more_data",
    "reject_candidate",
    "defer_decision",
]

ALLOWED_OWNER_DECISIONS = {
    "approved_for_shadow",
    "continue_review",
    "needs_more_data",
    "reject_candidate",
    "defer_decision",
}
DISALLOWED_OWNER_DECISIONS = {
    "apply_to_production",
    "promote_to_baseline",
    "place_order",
    "enable_broker_action",
}
REQUIRED_HARD_BLOCKERS = {
    "NO_SHADOW_READY_APPEARANCE",
    "EVIDENCE_DASHBOARD_BLOCKED",
    "DATA_QUALITY_CRITICAL",
    "OPS_VALIDATION_FAILED",
    "UNSAFE_PRODUCTION_EFFECT",
    "BROKER_ACTION_NOT_NONE",
    "MISSING_DIAGNOSTICS_SOURCE",
    "MISSING_WEIGHT_SET_ID",
}


class ShadowReadyReviewError(ValueError):
    """Raised when shadow-ready review inputs or outputs are unsafe."""


class ShadowReadyReviewSafety(BaseModel):
    observe_only: Literal[True]
    candidate_only: Literal[True]
    production_effect: Literal["none"]
    broker_action: Literal["none"]
    manual_review_required: Literal[True]


class ShadowReadyReviewThresholds(BaseModel):
    min_shadow_ready_appearances: int = Field(ge=1)
    min_cross_preset_stability_score: float = Field(ge=0, le=1)
    max_regime_failure_count_warning: int = Field(ge=0)
    max_regime_failure_count_block: int = Field(ge=0)
    min_review_priority_score: float = Field(ge=0, le=1)
    caution_review_priority_score: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_threshold_order(self) -> Self:
        if self.max_regime_failure_count_warning > self.max_regime_failure_count_block:
            raise ValueError("warning regime failure threshold cannot exceed block threshold")
        if self.caution_review_priority_score > self.min_review_priority_score:
            raise ValueError("caution review score cannot exceed recommended score")
        return self


class ShadowReadyReviewRankingWeights(BaseModel):
    shadow_ready_appearance_score: float = Field(ge=0, le=1)
    cross_preset_stability_score: float = Field(ge=0, le=1)
    rank_consistency_score: float = Field(ge=0, le=1)
    weight_shape_similarity_score: float = Field(ge=0, le=1)
    low_regime_failure_score: float = Field(ge=0, le=1)
    overfit_medium_ratio_score: float = Field(ge=0, le=1)
    balanced_exposure_score: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_weight_sum(self) -> Self:
        total = sum(float(value) for value in self.model_dump().values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError("shadow-ready review ranking weights must sum to 1.0")
        return self


class ShadowReadyOwnerApprovalPolicy(BaseModel):
    require_owner_approval: Literal[True]
    require_decision_journal_link: Literal[True]
    allowed_decisions: list[AllowedOwnerDecision] = Field(min_length=1)
    rationale_required_for: list[AllowedOwnerDecision] = Field(default_factory=list)
    disallowed_decisions: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_decisions(self) -> Self:
        if set(self.allowed_decisions) != ALLOWED_OWNER_DECISIONS:
            raise ValueError("owner approval decisions must match the reviewed policy set")
        if set(self.disallowed_decisions) != DISALLOWED_OWNER_DECISIONS:
            raise ValueError("disallowed owner approval decisions must be explicit")
        return self


class ShadowReadyEnrollmentLimits(BaseModel):
    max_enroll_per_review: int = Field(gt=0)
    default_next_review_days: int = Field(gt=0)
    require_review_package: Literal[True]
    required_owner_decision: Literal["approved_for_shadow"]
    production_config_mutation_allowed: Literal[False]
    target_weight_mutation_allowed: Literal[False]
    broker_action_allowed: Literal[False]


class ShadowReadyReviewChecklistItem(BaseModel):
    checklist_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    required: bool
    prompt: str = Field(min_length=1)


class ShadowReadyReviewPolicyConfig(BaseModel):
    schema_version: Literal["etf_shadow_ready_review_policy_v1"]
    policy_metadata: PolicyMetadata
    safety: ShadowReadyReviewSafety
    review_thresholds: ShadowReadyReviewThresholds
    ranking_weights: ShadowReadyReviewRankingWeights
    hard_blockers: list[str] = Field(min_length=1)
    owner_approval_policy: ShadowReadyOwnerApprovalPolicy
    enrollment_limits: ShadowReadyEnrollmentLimits
    review_checklist: list[ShadowReadyReviewChecklistItem] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_policy(self) -> Self:
        missing = REQUIRED_HARD_BLOCKERS - set(self.hard_blockers)
        if missing:
            raise ValueError(
                "shadow-ready review hard blockers missing: "
                + ", ".join(sorted(missing))
            )
        if self.safety.model_dump(mode="json") != SHADOW_READY_REVIEW_SAFETY:
            raise ValueError("shadow-ready review safety fields are unsafe")
        return self


def load_shadow_ready_review_policy_config(
    path: Path | str = DEFAULT_SHADOW_READY_REVIEW_POLICY_CONFIG_PATH,
) -> ShadowReadyReviewPolicyConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, Mapping):
        raise ShadowReadyReviewError("shadow-ready review policy must be a mapping")
    try:
        return ShadowReadyReviewPolicyConfig.model_validate(raw)
    except Exception as exc:
        raise ShadowReadyReviewError(str(exc)) from exc


def load_shadow_review_diagnostics_artifacts(
    *,
    diagnostics_json_path: Path | None = None,
    stable_shapes_csv_path: Path | None = None,
    near_shadow_csv_path: Path | None = None,
    diagnostics_dir: Path = DEFAULT_WEIGHT_SEARCH_DIAGNOSTICS_DIR,
    latest: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    resolved_diagnostics = diagnostics_json_path
    if resolved_diagnostics is None and latest:
        resolved_diagnostics = _latest_file(
            diagnostics_dir,
            "historical_weight_search_diagnostics_*.json",
        )
    if resolved_diagnostics is not None and stable_shapes_csv_path is None:
        stable_shapes_csv_path = resolved_diagnostics.with_name(
            f"{resolved_diagnostics.stem}_stable_shapes.csv"
        )
    if resolved_diagnostics is not None and near_shadow_csv_path is None:
        near_shadow_csv_path = resolved_diagnostics.with_name(
            f"{resolved_diagnostics.stem}_near_shadow.csv"
        )

    loaded_artifacts: list[dict[str, Any]] = []
    missing_artifacts: list[dict[str, Any]] = []
    diagnostics_payload = _read_json_object(resolved_diagnostics)
    if diagnostics_payload:
        loaded_artifacts.append(_artifact_record("diagnostics_json", resolved_diagnostics))
    else:
        missing_artifacts.append(
            _missing_artifact_record("diagnostics_json", resolved_diagnostics, required=True)
        )

    stable_shapes = _records(diagnostics_payload.get("cross_preset_stable_shapes"))
    stable_csv_records = _read_csv_records(stable_shapes_csv_path)
    if stable_csv_records:
        loaded_artifacts.append(_artifact_record("stable_shapes_csv", stable_shapes_csv_path))
        if not stable_shapes:
            stable_shapes = stable_csv_records
    elif stable_shapes:
        missing_artifacts.append(
            _missing_artifact_record("stable_shapes_csv", stable_shapes_csv_path, required=False)
        )
    else:
        missing_artifacts.append(
            _missing_artifact_record("stable_shapes_csv", stable_shapes_csv_path, required=True)
        )

    near_shadow = _records(diagnostics_payload.get("near_shadow_candidates"))
    near_shadow_csv_records = _read_csv_records(near_shadow_csv_path)
    if near_shadow_csv_records:
        loaded_artifacts.append(_artifact_record("near_shadow_csv", near_shadow_csv_path))
        if not near_shadow:
            near_shadow = near_shadow_csv_records
    elif near_shadow:
        missing_artifacts.append(
            _missing_artifact_record("near_shadow_csv", near_shadow_csv_path, required=False)
        )
    else:
        missing_artifacts.append(
            _missing_artifact_record("near_shadow_csv", near_shadow_csv_path, required=False)
        )

    required_missing = [item for item in missing_artifacts if item["required"] is True]
    unsafe = _unsafe_safety_fields(diagnostics_payload) if diagnostics_payload else False
    if required_missing or unsafe:
        status = "FAIL"
    elif missing_artifacts:
        status = "PASS_WITH_WARNINGS"
    else:
        status = "PASS"
    run_manifest = _mapping(diagnostics_payload.get("run_manifest"))
    source_links = {
        "diagnostics_json": "" if resolved_diagnostics is None else str(resolved_diagnostics),
        "stable_shapes_csv": "" if stable_shapes_csv_path is None else str(stable_shapes_csv_path),
        "near_shadow_csv": "" if near_shadow_csv_path is None else str(near_shadow_csv_path),
    }
    payload = {
        "schema_version": SHADOW_READY_REVIEW_ARTIFACTS_SCHEMA_VERSION,
        "report_type": "etf_shadow_ready_review_artifacts",
        "diagnostics_run_id": _text(run_manifest.get("run_id"), "MISSING"),
        "generated_at": generated.isoformat(),
        "artifact_status": status,
        "loaded_artifacts": loaded_artifacts,
        "missing_artifacts": missing_artifacts,
        "data_quality_status": _text(diagnostics_payload.get("data_quality_status"), "MISSING"),
        "market_regime": _text(diagnostics_payload.get("market_regime"), "UNKNOWN"),
        "source_links": source_links,
        "diagnostics_payload": diagnostics_payload,
        "stable_shapes": stable_shapes,
        "near_shadow_candidates": near_shadow,
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(SHADOW_READY_REVIEW_SAFETY),
        **SHADOW_READY_REVIEW_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def aggregate_shadow_ready_review_candidates(
    artifacts: Mapping[str, Any],
    *,
    policy: ShadowReadyReviewPolicyConfig | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or load_shadow_ready_review_policy_config()
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    shapes: list[dict[str, Any]] = []
    diagnostics_source_present = bool(_mapping(artifacts.get("diagnostics_payload")))
    for shape in _records(artifacts.get("stable_shapes")):
        row = _shape_aggregation_row(shape, diagnostics_source_present=diagnostics_source_present)
        shapes.append(row)
    shapes = sorted(
        _dedupe_shapes(shapes),
        key=lambda item: (
            -_float(item.get("cross_preset_stability_score")),
            -_int(item.get("shadow_ready_appearance_count")),
            _float(item.get("average_rank"), default=999999.0),
            _text(item.get("shape_id")),
        ),
    )
    payload = {
        "schema_version": SHADOW_READY_REVIEW_AGGREGATION_SCHEMA_VERSION,
        "report_type": "etf_shadow_ready_candidate_aggregation",
        "generated_at": generated.isoformat(),
        "diagnostics_run_id": _text(artifacts.get("diagnostics_run_id"), "MISSING"),
        "candidate_count": len(shapes),
        "shape_candidates": shapes,
        "aggregation_level": "weight_shape",
        "policy_version": resolved_policy.policy_metadata.version,
        "source_links": _mapping(artifacts.get("source_links")),
        "artifact_status": _text(artifacts.get("artifact_status"), "UNKNOWN"),
        "data_quality_status": _text(artifacts.get("data_quality_status"), "MISSING"),
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(SHADOW_READY_REVIEW_SAFETY),
        **SHADOW_READY_REVIEW_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def rank_shadow_ready_review_candidates(
    aggregation: Mapping[str, Any],
    *,
    policy: ShadowReadyReviewPolicyConfig | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or load_shadow_ready_review_policy_config()
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    ranked: list[dict[str, Any]] = []
    for shape in _records(aggregation.get("shape_candidates")):
        scores = _review_component_scores(shape, resolved_policy)
        blockers = _shape_blocking_evidence(shape, aggregation, resolved_policy)
        warnings = _shape_warning_evidence(shape, resolved_policy)
        priority = _weighted_review_score(scores, resolved_policy.ranking_weights)
        status = _review_status(priority, blockers, warnings, resolved_policy)
        ranked.append(
            {
                "review_rank": 0,
                "shape_id": shape["shape_id"],
                "review_priority_score": round(priority, 6),
                "component_scores": scores,
                "reason_summary": _review_reason_summary(shape, priority, status),
                "supporting_evidence": _shape_supporting_evidence(shape),
                "blocking_evidence": blockers,
                "warning_evidence": warnings,
                "review_status": status,
                "representative_weights": shape.get("representative_weights", {}),
                "source_weight_set_ids": _texts(shape.get("source_weight_set_ids")),
                "preset_ids": _texts(shape.get("preset_ids")),
                "search_ids": _texts(shape.get("search_ids")),
                "safety": dict(SHADOW_READY_REVIEW_SAFETY),
                **SHADOW_READY_REVIEW_SAFETY,
            }
        )
    ranked = sorted(
        ranked,
        key=lambda item: (
            _review_status_rank(_text(item.get("review_status"))),
            -_float(item.get("review_priority_score")),
            _text(item.get("shape_id")),
        ),
    )
    for index, row in enumerate(ranked, start=1):
        row["review_rank"] = index
    payload = {
        "schema_version": SHADOW_READY_REVIEW_RANKING_SCHEMA_VERSION,
        "report_type": "etf_shadow_ready_review_ranking",
        "generated_at": generated.isoformat(),
        "diagnostics_run_id": _text(aggregation.get("diagnostics_run_id"), "MISSING"),
        "ranked_candidate_count": len(ranked),
        "recommended_count": len(
            [row for row in ranked if row["review_status"] == "review_recommended"]
        ),
        "blocked_count": len([row for row in ranked if row["review_status"] == "blocked"]),
        "ranking_weights": resolved_policy.ranking_weights.model_dump(mode="json"),
        "review_thresholds": resolved_policy.review_thresholds.model_dump(mode="json"),
        "ranked_candidates": ranked,
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(SHADOW_READY_REVIEW_SAFETY),
        **SHADOW_READY_REVIEW_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def build_near_shadow_review_summary(
    artifacts: Mapping[str, Any],
    *,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    rows = _records(artifacts.get("near_shadow_candidates"))
    gap_counts = Counter()
    suggestion_counts = Counter()
    by_shape: dict[str, list[dict[str, Any]]] = {}
    by_preset: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        gaps = _json_list(row.get("main_gaps"))
        suggestions = _json_list(row.get("rescue_suggestions"))
        gap_counts.update(gaps)
        suggestion_counts.update(suggestions)
        shape_key = _text(row.get("shape_key"), "UNKNOWN")
        preset_id = _text(row.get("preset_id"), "UNKNOWN")
        summary_row = {
            "weight_set_id": _text(row.get("weight_set_id"), "MISSING"),
            "rank": _int(row.get("rank"), default=999999),
            "overfit_risk": _text(row.get("overfit_risk"), "UNKNOWN"),
            "distance_to_shadow_ready": _float_or_none(row.get("distance_to_shadow_ready")),
            "main_gaps": gaps,
            "rescue_suggestions": suggestions,
        }
        by_shape.setdefault(shape_key, []).append(summary_row)
        by_preset.setdefault(preset_id, []).append(summary_row)
    should_rescue = "review_with_caution" if rows and gap_counts else "no_near_shadow_context"
    payload = {
        "schema_version": SHADOW_READY_REVIEW_NEAR_SHADOW_SCHEMA_VERSION,
        "report_type": "etf_near_shadow_review_summary",
        "generated_at": generated.isoformat(),
        "diagnostics_run_id": _text(artifacts.get("diagnostics_run_id"), "MISSING"),
        "near_shadow_count": len(rows),
        "common_gaps": _counter_rows(gap_counts),
        "common_rescue_suggestions": _counter_rows(suggestion_counts),
        "near_shadow_by_shape": _group_rows(by_shape),
        "near_shadow_by_preset": _group_rows(by_preset),
        "should_rescue": should_rescue,
        "rescue_review_notes": _near_shadow_review_notes(gap_counts),
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(SHADOW_READY_REVIEW_SAFETY),
        **SHADOW_READY_REVIEW_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def build_shadow_candidate_review_package(
    *,
    artifacts: Mapping[str, Any],
    aggregation: Mapping[str, Any] | None = None,
    ranking: Mapping[str, Any] | None = None,
    near_shadow_summary: Mapping[str, Any] | None = None,
    policy: ShadowReadyReviewPolicyConfig | None = None,
    top: int = 3,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    if top <= 0:
        raise ShadowReadyReviewError("top must be positive")
    resolved_policy = policy or load_shadow_ready_review_policy_config()
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    resolved_aggregation = aggregation or aggregate_shadow_ready_review_candidates(
        artifacts,
        policy=resolved_policy,
        generated_at=generated,
    )
    resolved_ranking = ranking or rank_shadow_ready_review_candidates(
        resolved_aggregation,
        policy=resolved_policy,
        generated_at=generated,
    )
    resolved_near_shadow = near_shadow_summary or build_near_shadow_review_summary(
        artifacts,
        generated_at=generated,
    )
    ranked = _records(resolved_ranking.get("ranked_candidates"))
    top_candidates = ranked[:top]
    package_id = _stable_id(
        "shadow-review-package",
        _text(artifacts.get("diagnostics_run_id"), "MISSING"),
        generated.date().isoformat(),
        top,
    )
    summary = {
        "review_candidate_count": len(ranked),
        "pending_review_count": len(
            [row for row in ranked if row.get("review_status") != "blocked"]
        ),
        "blocked_count": len([row for row in ranked if row.get("review_status") == "blocked"]),
        "top_candidate": _text(top_candidates[0].get("shape_id"), "MISSING")
        if top_candidates
        else "MISSING",
        "approved_enrollment_count": 0,
        "owner_approval_status": "owner_approval_required",
    }
    payload = {
        "schema_version": SHADOW_READY_REVIEW_PACKAGE_SCHEMA_VERSION,
        "report_type": SHADOW_READY_REVIEW_PACKAGE_REPORT_TYPE,
        "review_package_id": package_id,
        "generated_at": generated.isoformat(),
        "diagnostics_run_id": _text(artifacts.get("diagnostics_run_id"), "MISSING"),
        "market_regime": _text(artifacts.get("market_regime"), "UNKNOWN"),
        "data_quality_status": _text(artifacts.get("data_quality_status"), "MISSING"),
        "policy_version": resolved_policy.policy_metadata.version,
        "safety_banner": dict(SHADOW_READY_REVIEW_SAFETY),
        "review_metadata": {
            "top_n": top,
            "candidate_review_level": "weight_shape",
            "owner_approval_required": True,
            "production_effect": "none",
            "broker_action": "none",
        },
        "diagnostics_source_summary": {
            "artifact_status": _text(artifacts.get("artifact_status"), "UNKNOWN"),
            "loaded_artifacts": _records(artifacts.get("loaded_artifacts")),
            "missing_artifacts": _records(artifacts.get("missing_artifacts")),
            "source_links": _mapping(artifacts.get("source_links")),
        },
        "ranked_stable_shapes": ranked,
        "top_review_candidates": top_candidates,
        "near_shadow_summary": dict(near_shadow_summary or resolved_near_shadow),
        "owner_review_checklist": [
            {**item.model_dump(mode="json"), "status": "pending_owner_review"}
            for item in resolved_policy.review_checklist
        ],
        "recommended_decision_options": list(
            resolved_policy.owner_approval_policy.allowed_decisions
        ),
        "source_artifact_links": _source_artifact_links(artifacts),
        "review_summary": summary,
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(SHADOW_READY_REVIEW_SAFETY),
        **SHADOW_READY_REVIEW_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def build_shadow_candidate_owner_approval(
    *,
    review_package: Mapping[str, Any],
    shape_id: str,
    owner_decision: str,
    rationale: str,
    confidence: float,
    selected_weight_set_id: str | None = None,
    conditions: Sequence[str] | None = None,
    decision_journal_link: str | None = None,
    policy: ShadowReadyReviewPolicyConfig | None = None,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or load_shadow_ready_review_policy_config()
    created = _coerce_datetime(created_at or datetime.now(UTC))
    if (
        owner_decision in DISALLOWED_OWNER_DECISIONS
        or owner_decision not in ALLOWED_OWNER_DECISIONS
    ):
        raise ShadowReadyReviewError("owner decision is not allowed for shadow review")
    if (
        owner_decision in set(resolved_policy.owner_approval_policy.rationale_required_for)
        and not rationale.strip()
    ):
        raise ShadowReadyReviewError("rationale is required for this owner decision")
    if confidence < 0 or confidence > 1:
        raise ShadowReadyReviewError("confidence must be between 0 and 1")
    candidate = _candidate_from_package(review_package, shape_id)
    if not candidate:
        raise ShadowReadyReviewError(f"shape_id not found in review package: {shape_id}")
    resolved_weight_set_id = selected_weight_set_id or _first_text(
        candidate.get("source_weight_set_ids")
    )
    if not resolved_weight_set_id:
        raise ShadowReadyReviewError("selected_weight_set_id is required")
    approval_id = _stable_id(
        "shadow-approval",
        review_package.get("review_package_id"),
        shape_id,
        resolved_weight_set_id,
        owner_decision,
    )
    payload = {
        "schema_version": SHADOW_READY_REVIEW_APPROVAL_SCHEMA_VERSION,
        "report_type": SHADOW_READY_REVIEW_APPROVAL_REPORT_TYPE,
        "approval_id": approval_id,
        "review_package_id": _text(review_package.get("review_package_id"), "MISSING"),
        "shape_id": shape_id,
        "selected_weight_set_id": resolved_weight_set_id,
        "owner_decision": owner_decision,
        "owner_approval_status": owner_decision,
        "rationale": rationale,
        "confidence": round(float(confidence), 6),
        "conditions": [str(item) for item in conditions or []],
        "decision_journal_link": _text(decision_journal_link),
        "created_at": created.isoformat(),
        "manual_review_required": True,
        "review_status_at_approval": _text(candidate.get("review_status"), "UNKNOWN"),
        "production_state_mutated": False,
        "commands_executed": False,
        "safety": dict(SHADOW_READY_REVIEW_SAFETY),
        **SHADOW_READY_REVIEW_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def build_shadow_candidate_approved_enrollment(
    *,
    approval: Mapping[str, Any],
    review_package: Mapping[str, Any],
    policy: ShadowReadyReviewPolicyConfig | None = None,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or load_shadow_ready_review_policy_config()
    created = _coerce_datetime(created_at or datetime.now(UTC))
    if _text(approval.get("owner_decision")) != "approved_for_shadow":
        raise ShadowReadyReviewError("only approved_for_shadow decisions can enroll")
    shape_id = _text(approval.get("shape_id"))
    if not _candidate_from_package(review_package, shape_id):
        raise ShadowReadyReviewError("approval shape_id is not present in review package")
    weight_set_id = _text(approval.get("selected_weight_set_id"))
    if not weight_set_id:
        raise ShadowReadyReviewError("approval selected_weight_set_id is required")
    enrollment_id = _stable_id(
        "shadow-enrollment",
        approval.get("approval_id"),
        review_package.get("review_package_id"),
        shape_id,
        weight_set_id,
    )
    shadow_candidate_id = _stable_id("shadow-candidate", shape_id, weight_set_id)
    next_review_due = (
        created.date() + timedelta(days=resolved_policy.enrollment_limits.default_next_review_days)
    ).isoformat()
    payload = {
        "schema_version": SHADOW_READY_REVIEW_ENROLLMENT_SCHEMA_VERSION,
        "report_type": SHADOW_READY_REVIEW_ENROLLMENT_REPORT_TYPE,
        "enrollment_id": enrollment_id,
        "approval_id": _text(approval.get("approval_id"), "MISSING"),
        "shape_id": shape_id,
        "selected_weight_set_id": weight_set_id,
        "shadow_candidate_id": shadow_candidate_id,
        "review_package_id": _text(review_package.get("review_package_id"), "MISSING"),
        "owner_approval_status": _text(approval.get("owner_decision")),
        "forward_tracking_status": "forward_shadow_tracking_pending_first_update",
        "forward_tracking_link": {
            "shadow_candidate_id": shadow_candidate_id,
            "weight_set_id": weight_set_id,
            "shape_id": shape_id,
            "approval_id": _text(approval.get("approval_id")),
            "review_package_id": _text(review_package.get("review_package_id")),
            "forward_dashboard_link": "etf_forward_dashboard",
            "weekly_review_link": "etf_weekly_review",
            "decision_journal_link": _text(approval.get("decision_journal_link")),
            "next_review_due": next_review_due,
        },
        "source_links": {
            "review_package": _text(review_package.get("review_package_id")),
            "approval": _text(approval.get("approval_id")),
        },
        "created_at": created.isoformat(),
        "production_state_mutated": False,
        "commands_executed": False,
        "safety": dict(SHADOW_READY_REVIEW_SAFETY),
        **SHADOW_READY_REVIEW_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def build_shadow_candidate_review_validation_report(
    *,
    config_path: Path | str = DEFAULT_SHADOW_READY_REVIEW_POLICY_CONFIG_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    checks: list[dict[str, Any]] = []
    policy: ShadowReadyReviewPolicyConfig | None = None
    try:
        policy = load_shadow_ready_review_policy_config(config_path)
        _append_check(checks, "review_policy_valid", True, "shadow-ready review policy loads")
    except Exception as exc:
        _append_check(
            checks,
            "review_policy_valid",
            False,
            "shadow-ready review policy failed validation",
            {"error": str(exc), "error_type": type(exc).__name__},
        )
    if policy is not None:
        probe = _workflow_probe(policy, generated)
        for check_id, passed in probe.items():
            _append_check(checks, check_id, passed, _validation_summary(check_id, passed))
        _append_check(
            checks,
            "reader_brief_integration_available",
            _registry_has_shadow_review(report_registry_path),
            "Reader Brief can discover shadow candidate review registry entries",
        )
        _append_check(
            checks,
            "safety_boundary_safe",
            policy.safety.model_dump(mode="json") == SHADOW_READY_REVIEW_SAFETY,
            "production_effect=none; broker_action=none; manual_review_required=true",
        )
    failed = [check for check in checks if check["status"] == "FAIL"]
    payload = {
        "schema_version": SHADOW_READY_REVIEW_VALIDATION_SCHEMA_VERSION,
        "report_type": SHADOW_READY_REVIEW_VALIDATION_REPORT_TYPE,
        "validation_id": _stable_id("shadow-review-validation", generated.date()),
        "generated_at": generated.isoformat(),
        "status": "FAIL" if failed else "PASS",
        "checks": checks,
        "failed_check_count": len(failed),
        "warning_check_count": len([check for check in checks if check["status"] == "WARNING"]),
        "production_weight_update_blocked": True,
        "broker_order_blocked": True,
        "automatic_candidate_promotion_blocked": True,
        "auto_enrollment_without_approval_blocked": True,
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(SHADOW_READY_REVIEW_SAFETY),
        **SHADOW_READY_REVIEW_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def render_shadow_candidate_review_package_markdown(payload: Mapping[str, Any]) -> str:
    lines = _markdown_header(
        "ETF Shadow Candidate Review Package",
        payload.get("review_package_id"),
        payload.get("generated_at"),
    )
    summary = _mapping(payload.get("review_summary"))
    lines.extend(
        [
            "## Review Summary / 复核摘要",
            "",
            f"- Diagnostics Run: `{_escape_md(payload.get('diagnostics_run_id'))}`",
            f"- Market Regime: `{_escape_md(payload.get('market_regime'))}`",
            f"- Data Quality: `{_escape_md(payload.get('data_quality_status'))}`",
            f"- Top Candidate: `{_escape_md(summary.get('top_candidate'))}`",
            f"- Pending Review Candidates: `{summary.get('pending_review_count', 0)}`",
            f"- Approved Enrollments: `{summary.get('approved_enrollment_count', 0)}`",
            f"- Owner Approval Status: `{_escape_md(summary.get('owner_approval_status'))}`",
            "",
            "## Ranked Stable Shapes / 稳定权重形状排序",
            "",
            "| Rank | Shape | Score | Status | Weights | Evidence | Blockers |",
            "|---:|---|---:|---|---|---|---|",
        ]
    )
    for row in _records(payload.get("ranked_stable_shapes"))[:12]:
        lines.append(
            f"| {row.get('review_rank')} | `{_escape_md(row.get('shape_id'))}` | "
            f"{row.get('review_priority_score')} | `{_escape_md(row.get('review_status'))}` | "
            f"{_weights_text(row.get('representative_weights'))} | "
            f"{_escape_md(row.get('reason_summary'))} | "
            f"{_evidence_ids(row.get('blocking_evidence'))} |"
        )
    lines.extend(
        [
            "",
            "## Near-Shadow Context / Near-Shadow 背景",
            "",
        ]
    )
    near = _mapping(payload.get("near_shadow_summary"))
    lines.append(f"- Near-shadow count: `{near.get('near_shadow_count', 0)}`")
    lines.append(f"- Rescue posture: `{_escape_md(near.get('should_rescue'))}`")
    lines.extend(["", "## Owner Review Checklist / Owner 人工复核清单", ""])
    for item in _records(payload.get("owner_review_checklist")):
        lines.append(
            f"- `{_escape_md(item.get('checklist_id'))}` "
            f"required={str(item.get('required')).lower()} status={item.get('status')}"
        )
    lines.extend(
        [
            "",
            "## Decision Options / 决策选项",
            "",
            ", ".join(f"`{item}`" for item in _texts(payload.get("recommended_decision_options"))),
            "",
            "## Source Artifact Links / Source Artifact Links",
            "",
        ]
    )
    for link in _texts(payload.get("source_artifact_links")):
        lines.append(f"- `{_escape_md(link)}`")
    return "\n".join(lines) + "\n"


def render_shadow_candidate_owner_approval_markdown(payload: Mapping[str, Any]) -> str:
    lines = _markdown_header(
        "ETF Shadow Candidate Owner Approval",
        payload.get("approval_id"),
        payload.get("created_at"),
    )
    lines.extend(
        [
            "## Approval / 审批记录",
            "",
            f"- Review Package: `{_escape_md(payload.get('review_package_id'))}`",
            f"- Shape ID: `{_escape_md(payload.get('shape_id'))}`",
            f"- Selected Weight Set: `{_escape_md(payload.get('selected_weight_set_id'))}`",
            f"- Owner Decision: `{_escape_md(payload.get('owner_decision'))}`",
            f"- Confidence: `{payload.get('confidence')}`",
            f"- Rationale: {_escape_md(payload.get('rationale'))}",
            f"- Decision Journal Link: `{_escape_md(payload.get('decision_journal_link'))}`",
            "",
        ]
    )
    return "\n".join(lines)


def render_shadow_candidate_enrollment_markdown(payload: Mapping[str, Any]) -> str:
    lines = _markdown_header(
        "ETF Shadow Candidate Approved Enrollment",
        payload.get("enrollment_id"),
        payload.get("created_at"),
    )
    tracking = _mapping(payload.get("forward_tracking_link"))
    lines.extend(
        [
            "## Enrollment / Forward Shadow 观察登记",
            "",
            f"- Approval: `{_escape_md(payload.get('approval_id'))}`",
            f"- Shape ID: `{_escape_md(payload.get('shape_id'))}`",
            f"- Selected Weight Set: `{_escape_md(payload.get('selected_weight_set_id'))}`",
            f"- Shadow Candidate ID: `{_escape_md(payload.get('shadow_candidate_id'))}`",
            f"- Forward Tracking Status: `{_escape_md(payload.get('forward_tracking_status'))}`",
            f"- Next Review Due: `{_escape_md(tracking.get('next_review_due'))}`",
            "- Production state mutated: `false`",
            "",
        ]
    )
    return "\n".join(lines)


def render_shadow_candidate_review_validation_markdown(payload: Mapping[str, Any]) -> str:
    lines = _markdown_header(
        "ETF Shadow Candidate Review Validation Gate",
        payload.get("validation_id"),
        payload.get("generated_at"),
    )
    lines.extend(
        [
            "## Status / 状态",
            "",
            f"- Status: `{_escape_md(payload.get('status'))}`",
            f"- Failed checks: `{payload.get('failed_check_count', 0)}`",
            "",
            "## Checks / 校验项",
            "",
            "| Check | Status | Summary |",
            "|---|---|---|",
        ]
    )
    for check in _records(payload.get("checks")):
        lines.append(
            f"| `{_escape_md(check.get('check_id'))}` | `{_escape_md(check.get('status'))}` | "
            f"{_escape_md(check.get('summary'))} |"
        )
    return "\n".join(lines) + "\n"


def write_shadow_candidate_review_package(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> dict[str, Path]:
    _write_json(payload, json_path)
    _write_text(render_shadow_candidate_review_package_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def write_shadow_candidate_owner_approval(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> dict[str, Path]:
    _write_json(payload, json_path)
    _write_text(render_shadow_candidate_owner_approval_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def write_shadow_candidate_approved_enrollment(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> dict[str, Path]:
    _write_json(payload, json_path)
    _write_text(render_shadow_candidate_enrollment_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def write_shadow_candidate_review_validation_report(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> dict[str, Path]:
    _write_json(payload, json_path)
    _write_text(render_shadow_candidate_review_validation_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def _shape_aggregation_row(
    shape: Mapping[str, Any],
    *,
    diagnostics_source_present: bool,
) -> dict[str, Any]:
    readiness_counts = _json_mapping(shape.get("readiness_counts"))
    overfit_counts = _json_mapping(shape.get("overfit_risk_counts"))
    examples = _records(shape.get("appearance_examples"))
    source_weight_set_ids = sorted(
        {
            _text(example.get("weight_set_id"))
            for example in examples
            if _text(example.get("weight_set_id"))
        }
    )
    if not source_weight_set_ids:
        source_weight_set_ids = _texts(shape.get("source_weight_set_ids"))
    shape_id = _text(
        shape.get("stable_shape_id"),
        _text(shape.get("shape_id"), _stable_id("weight-shape", shape.get("shape_key"))),
    )
    representative_weights = _json_mapping(shape.get("representative_weights"))
    return {
        "shape_id": shape_id,
        "stable_shape_id": shape_id,
        "shape_key": _text(shape.get("shape_key")),
        "representative_weights": representative_weights,
        "appearance_count": _int(shape.get("appearance_count")),
        "shadow_ready_appearance_count": _int(
            shape.get("shadow_ready_appearance_count"),
            default=_int(readiness_counts.get("shadow_ready")),
        ),
        "blocked_by_overfit_count": _int(
            shape.get("blocked_by_overfit_count"),
            default=_int(readiness_counts.get("blocked_by_overfit_risk")),
        ),
        "preset_count": _int(shape.get("preset_count")),
        "search_count": _int(shape.get("search_count")),
        "preset_ids": _texts(shape.get("preset_ids")),
        "search_ids": _texts(shape.get("search_ids")),
        "average_rank": _float_or_none(shape.get("average_rank")),
        "best_rank": _int(shape.get("best_rank"), default=999999),
        "cross_preset_stability_score": _float(shape.get("cross_preset_stability_score")),
        "rank_consistency": _float(shape.get("rank_consistency")),
        "weight_shape_similarity": _float(shape.get("weight_shape_similarity")),
        "regime_failure_count": _int(shape.get("regime_failure_count")),
        "overfit_risk_counts": overfit_counts,
        "readiness_counts": readiness_counts,
        "source_weight_set_ids": source_weight_set_ids,
        "appearance_examples": examples,
        "diagnostics_source_present": diagnostics_source_present,
        "safety": dict(SHADOW_READY_REVIEW_SAFETY),
        **SHADOW_READY_REVIEW_SAFETY,
    }


def _dedupe_shapes(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        shape_id = _text(row.get("shape_id"))
        if shape_id not in by_id:
            by_id[shape_id] = dict(row)
            continue
        current = by_id[shape_id]
        current["appearance_count"] = max(
            _int(current.get("appearance_count")),
            _int(row.get("appearance_count")),
        )
        current["shadow_ready_appearance_count"] = max(
            _int(current.get("shadow_ready_appearance_count")),
            _int(row.get("shadow_ready_appearance_count")),
        )
        current["source_weight_set_ids"] = sorted(
            set(_texts(current.get("source_weight_set_ids")))
            | set(_texts(row.get("source_weight_set_ids")))
        )
    return list(by_id.values())


def _review_component_scores(
    shape: Mapping[str, Any],
    policy: ShadowReadyReviewPolicyConfig,
) -> dict[str, float]:
    thresholds = policy.review_thresholds
    shadow_count = _int(shape.get("shadow_ready_appearance_count"))
    appearance_score = min(1.0, shadow_count / thresholds.min_shadow_ready_appearances)
    failure_count = _int(shape.get("regime_failure_count"))
    if thresholds.max_regime_failure_count_block <= 0:
        regime_score = 1.0
    else:
        regime_score = 1.0 - min(1.0, failure_count / thresholds.max_regime_failure_count_block)
    overfit_score = _acceptable_overfit_ratio(_json_mapping(shape.get("overfit_risk_counts")))
    return {
        "shadow_ready_appearance_score": round(appearance_score, 6),
        "cross_preset_stability_score": round(
            _float(shape.get("cross_preset_stability_score")),
            6,
        ),
        "rank_consistency_score": round(_float(shape.get("rank_consistency")), 6),
        "weight_shape_similarity_score": round(_float(shape.get("weight_shape_similarity")), 6),
        "low_regime_failure_score": round(max(0.0, regime_score), 6),
        "overfit_medium_ratio_score": round(overfit_score, 6),
        "balanced_exposure_score": round(
            _balanced_exposure_score(_json_mapping(shape.get("representative_weights"))),
            6,
        ),
    }


def _weighted_review_score(
    scores: Mapping[str, float],
    weights: ShadowReadyReviewRankingWeights,
) -> float:
    weight_map = weights.model_dump(mode="json")
    return sum(float(scores.get(key, 0.0)) * float(weight) for key, weight in weight_map.items())


def _shape_blocking_evidence(
    shape: Mapping[str, Any],
    aggregation: Mapping[str, Any],
    policy: ShadowReadyReviewPolicyConfig,
) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    if _int(shape.get("shadow_ready_appearance_count")) <= 0:
        blockers.append(
            _evidence(
                "NO_SHADOW_READY_APPEARANCE",
                "blocking",
                "shape has no shadow_ready appearances",
            )
        )
    if not shape.get("diagnostics_source_present"):
        blockers.append(
            _evidence(
                "MISSING_DIAGNOSTICS_SOURCE",
                "blocking",
                "diagnostics JSON source is missing",
            )
        )
    if not _texts(shape.get("source_weight_set_ids")):
        blockers.append(
            _evidence(
                "MISSING_WEIGHT_SET_ID",
                "blocking",
                "shape has no source weight_set_id",
            )
        )
    if _text(aggregation.get("data_quality_status")).upper().startswith("FAIL"):
        blockers.append(
            _evidence(
                "DATA_QUALITY_CRITICAL",
                "critical",
                "diagnostics data quality failed",
            )
        )
    if _text(aggregation.get("artifact_status")) == "FAIL":
        blockers.append(
            _evidence(
                "MISSING_DIAGNOSTICS_SOURCE",
                "critical",
                "required diagnostics artifacts are missing",
            )
        )
    if (
        _int(shape.get("regime_failure_count"))
        >= policy.review_thresholds.max_regime_failure_count_block
    ):
        blockers.append(
            _evidence(
                "REGIME_FAILURE_BLOCK",
                "blocking",
                "regime failure count exceeds block threshold",
            )
        )
    if _unsafe_safety_fields(shape):
        blockers.append(
            _evidence(
                "UNSAFE_PRODUCTION_EFFECT",
                "critical",
                "shape safety fields are unsafe",
            )
        )
    return blockers


def _shape_warning_evidence(
    shape: Mapping[str, Any],
    policy: ShadowReadyReviewPolicyConfig,
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    if (
        _float(shape.get("cross_preset_stability_score"))
        < policy.review_thresholds.min_cross_preset_stability_score
    ):
        warnings.append(
            _evidence(
                "LOW_CROSS_PRESET_STABILITY",
                "warning",
                "stability score is below review threshold",
            )
        )
    if (
        _int(shape.get("regime_failure_count"))
        >= policy.review_thresholds.max_regime_failure_count_warning
    ):
        warnings.append(
            _evidence(
                "REGIME_FAILURE_WARNING",
                "warning",
                "regime failure count exceeds warning threshold",
            )
        )
    if _balanced_exposure_score(_json_mapping(shape.get("representative_weights"))) < 1.0:
        warnings.append(
            _evidence(
                "EXPOSURE_BALANCE_REVIEW",
                "warning",
                "representative weights need exposure balance review",
            )
        )
    return warnings


def _review_status(
    score: float,
    blockers: Sequence[Mapping[str, Any]],
    warnings: Sequence[Mapping[str, Any]],
    policy: ShadowReadyReviewPolicyConfig,
) -> str:
    if blockers:
        return "blocked"
    if score >= policy.review_thresholds.min_review_priority_score and not warnings:
        return "review_recommended"
    if score >= policy.review_thresholds.caution_review_priority_score:
        return "review_with_caution"
    return "needs_more_data"


def _review_status_rank(status: str) -> int:
    return {
        "review_recommended": 0,
        "review_with_caution": 1,
        "needs_more_data": 2,
        "not_recommended": 3,
        "blocked": 4,
    }.get(status, 5)


def _shape_supporting_evidence(shape: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        _evidence(
            "SHADOW_READY_APPEARANCES",
            "support",
            f"{_int(shape.get('shadow_ready_appearance_count'))} shadow_ready appearances",
        ),
        _evidence(
            "CROSS_PRESET_STABILITY",
            "support",
            f"stability={_float(shape.get('cross_preset_stability_score'))}",
        ),
        _evidence(
            "SOURCE_WEIGHT_SETS",
            "support",
            ", ".join(_texts(shape.get("source_weight_set_ids"))) or "MISSING",
        ),
    ]


def _review_reason_summary(shape: Mapping[str, Any], priority: float, status: str) -> str:
    return (
        f"status={status}; score={priority:.3f}; "
        f"shadow_ready={_int(shape.get('shadow_ready_appearance_count'))}; "
        f"stability={_float(shape.get('cross_preset_stability_score')):.3f}; "
        f"regime_failures={_int(shape.get('regime_failure_count'))}"
    )


def _acceptable_overfit_ratio(counts: Mapping[str, Any]) -> float:
    total = sum(_int(value) for value in counts.values())
    if total <= 0:
        return 0.0
    acceptable = _int(counts.get("low")) + _int(counts.get("medium"))
    return min(1.0, acceptable / total)


def _balanced_exposure_score(weights: Mapping[str, Any]) -> float:
    if not weights:
        return 0.0
    spy = _float(weights.get("SPY"))
    qqq = _float(weights.get("QQQ"))
    smh = _float(weights.get("SMH"))
    soxx = _float(weights.get("SOXX"))
    cash = _float(weights.get("CASH"))
    score = 1.0
    semiconductor = smh + soxx
    if semiconductor > 0.30:
        score -= min(0.4, semiconductor - 0.30)
    if cash < 0.05:
        score -= 0.2
    if spy + qqq < 0.50:
        score -= 0.2
    return max(0.0, min(1.0, score))


def _candidate_from_package(package: Mapping[str, Any], shape_id: str) -> dict[str, Any]:
    for row in _records(package.get("ranked_stable_shapes")):
        if _text(row.get("shape_id")) == shape_id:
            return row
    for row in _records(package.get("top_review_candidates")):
        if _text(row.get("shape_id")) == shape_id:
            return row
    return {}


def _workflow_probe(
    policy: ShadowReadyReviewPolicyConfig,
    generated: datetime,
) -> dict[str, bool]:
    artifacts = _sample_artifacts(generated)
    aggregation = aggregate_shadow_ready_review_candidates(
        artifacts,
        policy=policy,
        generated_at=generated,
    )
    ranking = rank_shadow_ready_review_candidates(
        aggregation,
        policy=policy,
        generated_at=generated,
    )
    near_shadow = build_near_shadow_review_summary(artifacts, generated_at=generated)
    package = build_shadow_candidate_review_package(
        artifacts=artifacts,
        aggregation=aggregation,
        ranking=ranking,
        near_shadow_summary=near_shadow,
        policy=policy,
        generated_at=generated,
    )
    approval = build_shadow_candidate_owner_approval(
        review_package=package,
        shape_id="weight_shape_010_8ce67406f0",
        selected_weight_set_id="weight_set_010",
        owner_decision="approved_for_shadow",
        rationale="Owner approves forward shadow observation only.",
        confidence=0.8,
        decision_journal_link="decision_journal:test",
        policy=policy,
        created_at=generated,
    )
    enrollment = build_shadow_candidate_approved_enrollment(
        approval=approval,
        review_package=package,
        policy=policy,
        created_at=generated,
    )
    unsafe_rejected = False
    unapproved_enrollment_blocked = False
    try:
        build_shadow_candidate_owner_approval(
            review_package=package,
            shape_id="weight_shape_010_8ce67406f0",
            owner_decision="place_order",
            rationale="unsafe",
            confidence=0.8,
            policy=policy,
            created_at=generated,
        )
    except ShadowReadyReviewError:
        unsafe_rejected = True
    continue_decision = build_shadow_candidate_owner_approval(
        review_package=package,
        shape_id="weight_shape_010_8ce67406f0",
        selected_weight_set_id="weight_set_010",
        owner_decision="continue_review",
        rationale="Continue review.",
        confidence=0.5,
        policy=policy,
        created_at=generated,
    )
    try:
        build_shadow_candidate_approved_enrollment(
            approval=continue_decision,
            review_package=package,
            policy=policy,
            created_at=generated,
        )
    except ShadowReadyReviewError:
        unapproved_enrollment_blocked = True
    top_candidate = _records(package.get("top_review_candidates"))[0]
    return {
        "diagnostics_artifact_loader_available": artifacts["artifact_status"] == "PASS",
        "candidate_aggregator_available": aggregation["candidate_count"] >= 1,
        "stable_shape_ranking_available": ranking["ranked_candidate_count"] >= 1,
        "near_shadow_summary_available": near_shadow["near_shadow_count"] >= 1,
        "review_package_generator_available": bool(package["source_artifact_links"]),
        "owner_approval_capture_available": approval["owner_decision"] == "approved_for_shadow",
        "approved_enrollment_available": enrollment["approval_id"] == approval["approval_id"],
        "forward_tracking_linkage_available": bool(enrollment["forward_tracking_link"]),
        "auto_enrollment_without_approval_blocked": unapproved_enrollment_blocked,
        "unsafe_owner_decision_rejected": unsafe_rejected,
        "source_evidence_links_present": bool(top_candidate.get("source_weight_set_ids")),
    }


def _sample_artifacts(generated: datetime) -> dict[str, Any]:
    diagnostics = {
        "schema_version": "etf_weight_search_diagnostics_v1",
        "report_type": "etf_weight_search_diagnostics",
        "generated_at": generated.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "data_quality_status": "PASS",
        "run_manifest": {"run_id": "shadow-review-validation-probe"},
        "cross_preset_stable_shapes": [
            {
                "stable_shape_id": "weight_shape_010_8ce67406f0",
                "shape_key": "SPY=0.35|QQQ=0.35|SMH=0.15|SOXX=0.05|CASH=0.10",
                "representative_weights": {
                    "SPY": 0.35,
                    "QQQ": 0.35,
                    "SMH": 0.15,
                    "SOXX": 0.05,
                    "CASH": 0.10,
                },
                "appearance_count": 4,
                "shadow_ready_appearance_count": 3,
                "preset_count": 3,
                "search_count": 2,
                "preset_ids": ["ai_cycle_recent", "last_3y", "last_5y"],
                "search_ids": [
                    "etf_initial_weight_search_v1",
                    "etf_initial_weight_balanced_lower_semiconductor_v1",
                ],
                "average_rank": 2.25,
                "best_rank": 1,
                "cross_preset_stability_score": 0.82,
                "rank_consistency": 0.78,
                "weight_shape_similarity": 0.95,
                "regime_failure_count": 8,
                "readiness_counts": {"shadow_ready": 3, "needs_manual_review": 1},
                "overfit_risk_counts": {"low": 2, "medium": 2},
                "appearance_examples": [
                    {
                        "search_id": "etf_initial_weight_search_v1",
                        "preset_id": "ai_cycle_recent",
                        "rank": 1,
                        "weight_set_id": "weight_set_010",
                        "forward_readiness_status": "shadow_ready",
                        "overfit_risk": "medium",
                    }
                ],
                **SHADOW_READY_REVIEW_SAFETY,
            }
        ],
        "near_shadow_candidates": [
            {
                "search_id": "etf_initial_weight_search_v1",
                "preset_id": "last_2y",
                "rank": 4,
                "weight_set_id": "weight_set_011",
                "shape_key": "SPY=0.30|QQQ=0.35|SMH=0.20|SOXX=0.05|CASH=0.10",
                "overfit_risk": "medium",
                "distance_to_shadow_ready": 0.2,
                "main_gaps": ["FORWARD_EVIDENCE_MISSING"],
                "rescue_suggestions": ["continue_forward_observation"],
                **SHADOW_READY_REVIEW_SAFETY,
            }
        ],
        **SHADOW_READY_REVIEW_SAFETY,
    }
    return {
        "schema_version": SHADOW_READY_REVIEW_ARTIFACTS_SCHEMA_VERSION,
        "report_type": "etf_shadow_ready_review_artifacts",
        "diagnostics_run_id": "shadow-review-validation-probe",
        "generated_at": generated.isoformat(),
        "artifact_status": "PASS",
        "loaded_artifacts": [
            {"artifact_id": "diagnostics_json", "status": "loaded", "path": "probe.json"}
        ],
        "missing_artifacts": [],
        "data_quality_status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "source_links": {"diagnostics_json": "probe.json"},
        "diagnostics_payload": diagnostics,
        "stable_shapes": diagnostics["cross_preset_stable_shapes"],
        "near_shadow_candidates": diagnostics["near_shadow_candidates"],
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(SHADOW_READY_REVIEW_SAFETY),
        **SHADOW_READY_REVIEW_SAFETY,
    }


def _registry_has_shadow_review(path: Path) -> bool:
    try:
        registry = load_report_registry(path)
    except Exception:
        return False
    reports = {_text(item.get("report_id")): item for item in _records(registry.get("reports"))}
    return all(
        reports.get(report_id, {}).get("include_in_reader_brief") is True
        for report_id in (
            SHADOW_READY_REVIEW_PACKAGE_REGISTRY_ID,
            SHADOW_READY_REVIEW_VALIDATION_REGISTRY_ID,
        )
    )


def _validation_summary(check_id: str, passed: bool) -> str:
    messages = {
        "diagnostics_artifact_loader_available": (
            "diagnostics artifact loader preserves source paths"
        ),
        "candidate_aggregator_available": "shape-level candidate aggregation is available",
        "stable_shape_ranking_available": "stable shape review ranking is available",
        "near_shadow_summary_available": "near-shadow diagnostic summary is available",
        "review_package_generator_available": "review package generator preserves source links",
        "owner_approval_capture_available": "owner approval capture is available",
        "approved_enrollment_available": "approved candidate enrollment is available",
        "forward_tracking_linkage_available": "forward tracking link is created",
        "auto_enrollment_without_approval_blocked": "non-approved enrollment is blocked",
        "unsafe_owner_decision_rejected": "unsafe owner decision is rejected",
        "source_evidence_links_present": "source evidence links are present",
    }
    suffix = "available" if passed else "failed"
    return f"{messages.get(check_id, check_id)}: {suffix}"


def _latest_file(directory: Path, pattern: str) -> Path | None:
    candidates = [path for path in directory.glob(pattern) if path.is_file()]
    if not candidates:
        return None
    return sorted(candidates, key=lambda path: (path.stat().st_mtime, path.name))[-1]


def _artifact_record(artifact_id: str, path: Path | None) -> dict[str, Any]:
    return {
        "artifact_id": artifact_id,
        "status": "loaded",
        "path": "" if path is None else str(path),
        "exists": path is not None and path.exists(),
        "required": artifact_id in {"diagnostics_json", "stable_shapes_csv"},
    }


def _missing_artifact_record(
    artifact_id: str,
    path: Path | None,
    *,
    required: bool,
) -> dict[str, Any]:
    return {
        "artifact_id": artifact_id,
        "status": "missing",
        "path": "" if path is None else str(path),
        "exists": False,
        "required": required,
    }


def _read_json_object(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists() or path.suffix.lower() != ".json":
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _read_csv_records(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists() or path.suffix.lower() != ".csv":
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except OSError:
        return []


def _append_check(
    checks: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    summary: str,
    evidence: Mapping[str, Any] | None = None,
) -> None:
    checks.append(
        {
            "check_id": check_id,
            "status": "PASS" if passed else "FAIL",
            "summary": summary,
            "evidence": dict(evidence or {}),
            "production_effect": "none",
        }
    )


def _write_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_text(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _markdown_header(title: str, subject: object, as_of: object) -> list[str]:
    return [
        f"# {title}",
        "",
        "## Safety Banner / 安全边界",
        "",
        "| Field | Value |",
        "|---|---|",
        "| observe_only | true |",
        "| candidate_only | true |",
        "| production_effect | none |",
        "| broker_action | none |",
        "| manual_review_required | true |",
        "",
        "## Metadata / 元数据",
        "",
        f"- Subject: `{_escape_md(subject)}`",
        f"- As Of / Created At: `{_escape_md(as_of)}`",
        "",
    ]


def _source_artifact_links(artifacts: Mapping[str, Any]) -> list[str]:
    links = []
    for value in _mapping(artifacts.get("source_links")).values():
        if _text(value):
            links.append(_text(value))
    return sorted(dict.fromkeys(links))


def _counter_rows(counter: Counter[str]) -> list[dict[str, Any]]:
    return [
        {"id": key, "count": value}
        for key, value in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    ]


def _group_rows(groups: Mapping[str, Sequence[Mapping[str, Any]]]) -> list[dict[str, Any]]:
    return [
        {"group_id": key, "count": len(rows), "rows": [dict(row) for row in rows[:5]]}
        for key, rows in sorted(groups.items())
    ]


def _near_shadow_review_notes(gap_counts: Counter[str]) -> list[str]:
    if not gap_counts:
        return ["No near-shadow rows available; do not infer rescue candidates."]
    notes = ["Near-shadow rows are diagnostic only and cannot bypass owner approval."]
    if gap_counts.get("OVERFIT_RISK_HIGH"):
        notes.append("High overfit near-shadow candidates require caution.")
    if gap_counts.get("FORWARD_EVIDENCE_MISSING"):
        notes.append("Forward evidence gaps should be resolved before enrollment.")
    return notes


def _evidence(evidence_id: str, severity: str, reason: str) -> dict[str, Any]:
    return {
        "evidence_id": evidence_id,
        "blocker_id": evidence_id,
        "severity": severity,
        "reason": reason,
        "production_effect": "none",
    }


def _evidence_ids(value: object) -> str:
    records = _records(value)
    if not records:
        return "none"
    return ", ".join(_text(row.get("evidence_id"), _text(row.get("blocker_id"))) for row in records)


def _weights_text(value: object) -> str:
    weights = _json_mapping(value)
    if not weights:
        return "MISSING"
    return " / ".join(f"{key} {round(_float(val) * 100)}" for key, val in weights.items())


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _json_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _json_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return [value]
        if isinstance(parsed, list):
            return [str(item) for item in parsed if str(item)]
    return []


def _texts(value: object) -> list[str]:
    if isinstance(value, list):
        return [_text(item) for item in value if _text(item)]
    if isinstance(value, tuple):
        return [_text(item) for item in value if _text(item)]
    if isinstance(value, set):
        return sorted(_text(item) for item in value if _text(item))
    if isinstance(value, str) and value.strip().startswith("["):
        return _json_list(value)
    text = _text(value)
    return [text] if text else []


def _first_text(value: object) -> str:
    values = _texts(value)
    return values[0] if values else ""


def _text(value: object, default: str = "") -> str:
    if value is None or value == "":
        return default
    text = str(value).strip()
    return text or default


def _int(value: object, default: int = 0) -> int:
    parsed = _float_or_none(value)
    return default if parsed is None else int(parsed)


def _float(value: object, default: float = 0.0) -> float:
    parsed = _float_or_none(value)
    return default if parsed is None else float(parsed)


def _float_or_none(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _coerce_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _stable_id(prefix: str, *parts: object) -> str:
    digest = sha256(
        "|".join(
            part.isoformat() if isinstance(part, (date, datetime)) else str(part)
            for part in parts
        ).encode("utf-8")
    ).hexdigest()[:12]
    return f"{prefix}:{digest}"


def _escape_md(value: object) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _unsafe_safety_fields(payload: Mapping[str, Any]) -> bool:
    safety = _mapping(payload.get("safety"))
    production_effect = _text(
        payload.get("production_effect"),
        _text(safety.get("production_effect")),
    )
    broker_action = _text(payload.get("broker_action"), _text(safety.get("broker_action")))
    return (
        production_effect not in {"", "none"}
        or broker_action not in {"", "none"}
        or payload.get("manual_review_required") is False
    )


def _assert_safe_output(payload: Mapping[str, Any]) -> None:
    if _unsafe_safety_fields(payload):
        raise ShadowReadyReviewError("shadow-ready review output safety boundary is unsafe")
    if _contains_disallowed_output(payload):
        raise ShadowReadyReviewError("shadow-ready review output contains disallowed action")


def _contains_disallowed_output(value: object) -> bool:
    disallowed_keys = {
        "production_weight_update",
        "baseline_config_mutation",
        "broker_order",
        "auto_candidate_promotion",
        "auto_enroll_without_owner_approval",
        "apply_to_production",
        "place_order",
        "enable_broker_action",
    }
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key) in disallowed_keys:
                return True
            if _contains_disallowed_output(child):
                return True
    elif isinstance(value, list):
        return any(_contains_disallowed_output(item) for item in value)
    return False
