from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, Self

import pandas as pd
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.dynamic_allocation import DynamicAllocationPolicyConfig
from ai_trading_system.etf_portfolio.dynamic_robustness import (
    DynamicRobustnessPolicyConfig,
    build_dynamic_robustness_report,
)
from ai_trading_system.etf_portfolio.models import ETFConfigBundle, PolicyMetadata
from ai_trading_system.reports.report_index import DEFAULT_REPORT_REGISTRY_PATH
from ai_trading_system.yaml_loader import safe_load_yaml_path

DYNAMIC_SHADOW_POLICY_SCHEMA_VERSION = "etf_dynamic_shadow_policy_v1"
DYNAMIC_SHADOW_PACKAGE_SCHEMA_VERSION = "etf_dynamic_shadow_review_package_v1"
DYNAMIC_SHADOW_APPROVAL_SCHEMA_VERSION = "etf_dynamic_shadow_owner_approval_v1"
DYNAMIC_SHADOW_ENROLLMENT_SCHEMA_VERSION = "etf_dynamic_shadow_enrollment_v1"
DYNAMIC_SHADOW_REGISTRY_SCHEMA_VERSION = "etf_dynamic_shadow_candidate_registry_v1"
DYNAMIC_SHADOW_FORWARD_UPDATE_SCHEMA_VERSION = "etf_dynamic_shadow_forward_update_v1"
DYNAMIC_SHADOW_WEEKLY_REVIEW_SCHEMA_VERSION = "etf_dynamic_shadow_weekly_review_v1"
DYNAMIC_SHADOW_VALIDATION_SCHEMA_VERSION = "etf_dynamic_shadow_validation_v1"

DYNAMIC_SHADOW_PACKAGE_REPORT_TYPE = "etf_dynamic_shadow_review_package"
DYNAMIC_SHADOW_APPROVAL_REPORT_TYPE = "etf_dynamic_shadow_owner_approval"
DYNAMIC_SHADOW_ENROLLMENT_REPORT_TYPE = "etf_dynamic_shadow_enrollment"
DYNAMIC_SHADOW_FORWARD_UPDATE_REPORT_TYPE = "etf_dynamic_shadow_forward_update"
DYNAMIC_SHADOW_WEEKLY_REVIEW_REPORT_TYPE = "etf_dynamic_shadow_weekly_review"
DYNAMIC_SHADOW_VALIDATION_REPORT_TYPE = "etf_dynamic_shadow_validation"

DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_shadow.yaml"
)
DEFAULT_DYNAMIC_SHADOW_ROOT = PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_shadow"
DEFAULT_DYNAMIC_SHADOW_PACKAGE_DIR = DEFAULT_DYNAMIC_SHADOW_ROOT / "packages"
DEFAULT_DYNAMIC_SHADOW_APPROVAL_DIR = DEFAULT_DYNAMIC_SHADOW_ROOT / "approvals"
DEFAULT_DYNAMIC_SHADOW_ENROLLMENT_DIR = DEFAULT_DYNAMIC_SHADOW_ROOT / "enrollments"
DEFAULT_DYNAMIC_SHADOW_FORWARD_UPDATE_DIR = DEFAULT_DYNAMIC_SHADOW_ROOT / "forward_updates"
DEFAULT_DYNAMIC_SHADOW_WEEKLY_REVIEW_DIR = DEFAULT_DYNAMIC_SHADOW_ROOT / "weekly_reviews"
DEFAULT_DYNAMIC_SHADOW_VALIDATION_DIR = DEFAULT_DYNAMIC_SHADOW_ROOT / "validation"
DEFAULT_DYNAMIC_SHADOW_REGISTRY_PATH = (
    PROJECT_ROOT / "data" / "simulation" / "etf_dynamic_shadow_candidates.json"
)

DYNAMIC_SHADOW_SAFETY: dict[str, Any] = {
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
FORBIDDEN_DYNAMIC_SHADOW_KEYS = {
    "broker_order",
    "production_weight_update",
    "baseline_config_mutation",
    "official_target_weights_write",
    "automatic_candidate_promotion",
    "auto_enrollment_without_owner_approval",
    "apply_to_production",
    "promote_to_baseline",
    "place_order",
    "enable_broker_action",
}

AllowedDynamicShadowOwnerDecision = Literal[
    "approved_for_dynamic_shadow",
    "continue_review",
    "needs_more_data",
    "reject_candidate",
    "defer_decision",
]
ALLOWED_DYNAMIC_SHADOW_OWNER_DECISIONS = {
    "approved_for_dynamic_shadow",
    "continue_review",
    "needs_more_data",
    "reject_candidate",
    "defer_decision",
}
DISALLOWED_DYNAMIC_SHADOW_OWNER_DECISIONS = {
    "apply_to_production",
    "promote_to_baseline",
    "place_order",
    "enable_broker_action",
}
REQUIRED_FORWARD_METRICS = {
    "dynamic_candidate_return",
    "static_base_return",
    "current_baseline_return",
    "QQQ_return",
    "SPY_return",
    "SMH_return",
    "excess_vs_static",
    "excess_vs_baseline",
    "drawdown",
    "turnover",
    "regime_switch_count",
    "false_signal_count",
    "constraint_hit_count",
}
REVIEW_STATUSES = {
    "active_shadow",
    "needs_more_data",
    "watch",
    "reject_pending_review",
    "rejected",
    "archived",
}


class DynamicShadowError(ValueError):
    """Raised when dynamic shadow workflow inputs or outputs are unsafe."""


class DynamicShadowSafety(BaseModel):
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


class DynamicShadowRequiredGates(BaseModel):
    dynamic_calibration_validation_statuses: list[str] = Field(min_length=1)
    dynamic_robustness_validation_statuses: list[str] = Field(min_length=1)
    data_quality_statuses: list[str] = Field(min_length=1)
    operations_validation_statuses: list[str] = Field(min_length=1)
    allowed_robustness_statuses_for_shadow: list[str] = Field(min_length=1)
    require_dynamic_robustness_report: Literal[True]
    require_dynamic_calibration_report: Literal[True]
    require_validation_artifacts: Literal[True]
    require_operations_validation: Literal[True]


class DynamicShadowOwnerApprovalPolicy(BaseModel):
    require_owner_approval: Literal[True]
    require_decision_journal_link_for_approval: Literal[True]
    allowed_decisions: list[AllowedDynamicShadowOwnerDecision] = Field(min_length=1)
    rationale_required_for: list[AllowedDynamicShadowOwnerDecision] = Field(
        default_factory=list
    )
    disallowed_decisions: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_decisions(self) -> Self:
        if set(self.allowed_decisions) != ALLOWED_DYNAMIC_SHADOW_OWNER_DECISIONS:
            raise ValueError("dynamic shadow owner decisions must match policy set")
        if set(self.disallowed_decisions) != DISALLOWED_DYNAMIC_SHADOW_OWNER_DECISIONS:
            raise ValueError("dynamic shadow disallowed decisions must be explicit")
        return self


class DynamicShadowEnrollmentLimits(BaseModel):
    max_enroll_per_package: int = Field(gt=0)
    default_next_review_days: int = Field(gt=0)
    required_owner_decision: Literal["approved_for_dynamic_shadow"]
    production_config_mutation_allowed: Literal[False]
    target_weight_mutation_allowed: Literal[False]
    broker_action_allowed: Literal[False]


class DynamicShadowForwardTracking(BaseModel):
    minimum_tracking_days_for_watch: int = Field(ge=1)
    minimum_tracking_days_for_review: int = Field(ge=1)
    metrics: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_metrics(self) -> Self:
        missing = REQUIRED_FORWARD_METRICS - set(self.metrics)
        if missing:
            raise ValueError(
                "dynamic shadow forward metrics missing: " + ", ".join(sorted(missing))
            )
        return self


class DynamicShadowWeeklyReview(BaseModel):
    active_statuses: list[str] = Field(min_length=1)
    watch_drawdown_threshold: float = Field(le=0)
    watch_excess_vs_static_threshold: float
    reject_pending_drawdown_threshold: float = Field(le=0)
    reject_pending_false_signal_count: int = Field(ge=0)
    max_constraint_hit_ratio: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_review_statuses(self) -> Self:
        if set(self.active_statuses) != REVIEW_STATUSES:
            raise ValueError("dynamic shadow weekly review statuses must match policy set")
        if self.reject_pending_drawdown_threshold > self.watch_drawdown_threshold:
            raise ValueError("reject drawdown threshold must be at least as strict as watch")
        return self


class DynamicShadowPolicyConfig(BaseModel):
    schema_version: Literal["etf_dynamic_shadow_policy_v1"]
    policy_metadata: PolicyMetadata
    safety: DynamicShadowSafety
    required_gates: DynamicShadowRequiredGates
    owner_approval_policy: DynamicShadowOwnerApprovalPolicy
    enrollment_limits: DynamicShadowEnrollmentLimits
    forward_tracking: DynamicShadowForwardTracking
    weekly_review: DynamicShadowWeeklyReview

    @model_validator(mode="after")
    def validate_policy(self) -> Self:
        if self.safety.model_dump(mode="json") != DYNAMIC_SHADOW_SAFETY:
            raise ValueError("dynamic shadow safety fields are unsafe")
        return self


def load_dynamic_shadow_policy_config(
    path: Path | str = DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH,
) -> DynamicShadowPolicyConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, Mapping):
        raise DynamicShadowError("dynamic shadow policy must be a mapping")
    try:
        return DynamicShadowPolicyConfig.model_validate(raw)
    except Exception as exc:  # noqa: BLE001
        raise DynamicShadowError(f"invalid dynamic shadow policy: {exc}") from exc


def build_dynamic_shadow_review_package(
    *,
    dynamic_robustness_report: Mapping[str, Any],
    policy: DynamicShadowPolicyConfig | None = None,
    dynamic_calibration_report: Mapping[str, Any] | None = None,
    dynamic_calibration_validation: Mapping[str, Any] | None = None,
    dynamic_robustness_validation: Mapping[str, Any] | None = None,
    operations_validation: Mapping[str, Any] | None = None,
    source_paths: Mapping[str, str] | None = None,
    top: int = 3,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    if top <= 0:
        raise DynamicShadowError("top must be positive")
    resolved_policy = policy or load_dynamic_shadow_policy_config()
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    paths = dict(source_paths or {})
    summary = _mapping(dynamic_robustness_report.get("summary"))
    candidate_id = _text(summary.get("dynamic_candidate_id"))
    if not candidate_id:
        raise DynamicShadowError("dynamic robustness report missing dynamic candidate id")

    candidate = _dynamic_candidate_review_row(
        candidate_id=candidate_id,
        robustness_report=dynamic_robustness_report,
        dynamic_calibration_report=dynamic_calibration_report or {},
        dynamic_calibration_validation=dynamic_calibration_validation or {},
        dynamic_robustness_validation=dynamic_robustness_validation or {},
        operations_validation=operations_validation or {},
        policy=resolved_policy,
        source_paths=paths,
    )
    top_candidates = [candidate][:top]
    package_id = _stable_id(
        "dynamic-shadow-package",
        candidate_id,
        _text(dynamic_robustness_report.get("dynamic_robustness_report_id")),
        generated.date().isoformat(),
        top,
    )
    blocked_count = len([row for row in top_candidates if row["hard_gate_status"] != "PASS"])
    ready_count = len(
        [row for row in top_candidates if row["enrollment_allowed_after_owner_approval"]]
    )
    payload = {
        "schema_version": DYNAMIC_SHADOW_PACKAGE_SCHEMA_VERSION,
        "report_type": DYNAMIC_SHADOW_PACKAGE_REPORT_TYPE,
        "review_package_id": package_id,
        "generated_at": generated.isoformat(),
        "policy_version": resolved_policy.policy_metadata.version,
        "market_regime": _text(summary.get("market_regime"), "ai_after_chatgpt"),
        "candidate_count": len(top_candidates),
        "top": top,
        "review_summary": {
            "status": "BLOCKED" if blocked_count else "OWNER_REVIEW_REQUIRED",
            "candidate_count": len(top_candidates),
            "ready_after_owner_approval_count": ready_count,
            "blocked_count": blocked_count,
            "top_candidate": candidate_id,
            "owner_approval_status": "owner_approval_required",
            "automatic_enrollment_allowed": False,
        },
        "top_review_candidates": top_candidates,
        "recommended_decision_options": list(
            resolved_policy.owner_approval_policy.allowed_decisions
        ),
        "required_preconditions": _required_precondition_statuses(candidate),
        "source_artifacts": {
            "dynamic_robustness_report": paths.get("dynamic_robustness_report", ""),
            "dynamic_calibration_report": paths.get("dynamic_calibration_report", ""),
            "dynamic_calibration_validation": paths.get(
                "dynamic_calibration_validation", ""
            ),
            "dynamic_robustness_validation": paths.get("dynamic_robustness_validation", ""),
            "operations_validation": paths.get("operations_validation", ""),
            "data_quality_report": _text(
                _mapping(dynamic_robustness_report.get("source_artifacts")).get(
                    "data_quality_report"
                )
            ),
        },
        "decision_journal_integration": {
            "required_for_approved_enrollment": True,
            "commands_executed": False,
            "proposed_entry_type": "dynamic_shadow_owner_approval",
        },
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(DYNAMIC_SHADOW_SAFETY),
        **DYNAMIC_SHADOW_SAFETY,
    }
    _assert_dynamic_shadow_payload_safe(payload)
    return payload


def build_dynamic_shadow_owner_approval(
    *,
    review_package: Mapping[str, Any],
    candidate_id: str,
    owner_decision: str,
    rationale: str,
    confidence: float,
    decision_journal_link: str | None = None,
    conditions: Sequence[str] | None = None,
    reviewer: str = "project_owner",
    policy: DynamicShadowPolicyConfig | None = None,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or load_dynamic_shadow_policy_config()
    created = _coerce_datetime(created_at or datetime.now(UTC))
    if (
        owner_decision in DISALLOWED_DYNAMIC_SHADOW_OWNER_DECISIONS
        or owner_decision not in ALLOWED_DYNAMIC_SHADOW_OWNER_DECISIONS
    ):
        raise DynamicShadowError("owner decision is not allowed for dynamic shadow review")
    if (
        owner_decision in set(resolved_policy.owner_approval_policy.rationale_required_for)
        and not rationale.strip()
    ):
        raise DynamicShadowError("rationale is required for this owner decision")
    if confidence < 0 or confidence > 1:
        raise DynamicShadowError("confidence must be between 0 and 1")
    candidate = _candidate_from_package(review_package, candidate_id)
    if not candidate:
        raise DynamicShadowError(f"candidate_id not found in review package: {candidate_id}")
    hard_blockers = _texts(candidate.get("hard_blockers"))
    if owner_decision == resolved_policy.enrollment_limits.required_owner_decision:
        if hard_blockers:
            raise DynamicShadowError(
                "approved dynamic shadow enrollment is blocked by hard preconditions: "
                + ", ".join(hard_blockers)
            )
        if (
            resolved_policy.owner_approval_policy.require_decision_journal_link_for_approval
            and not _text(decision_journal_link)
        ):
            raise DynamicShadowError(
                "decision_journal_link is required for approved dynamic shadow enrollment"
            )
    approved_for_enrollment = (
        owner_decision == resolved_policy.enrollment_limits.required_owner_decision
        and not hard_blockers
    )
    approval_id = _stable_id(
        "dynamic-shadow-approval",
        review_package.get("review_package_id"),
        candidate_id,
        owner_decision,
        reviewer,
    )
    payload = {
        "schema_version": DYNAMIC_SHADOW_APPROVAL_SCHEMA_VERSION,
        "report_type": DYNAMIC_SHADOW_APPROVAL_REPORT_TYPE,
        "approval_id": approval_id,
        "review_package_id": _text(review_package.get("review_package_id"), "MISSING"),
        "candidate_id": candidate_id,
        "owner_decision": owner_decision,
        "owner_approval_status": owner_decision,
        "approved_for_enrollment": approved_for_enrollment,
        "reviewer": reviewer,
        "rationale": rationale,
        "confidence": round(float(confidence), 6),
        "conditions": [str(item) for item in conditions or []],
        "decision_journal_link": _text(decision_journal_link),
        "decision_journal_integration": {
            "status": "linked" if _text(decision_journal_link) else "missing",
            "link": _text(decision_journal_link),
            "commands_executed": False,
        },
        "candidate_hard_gate_status": _text(candidate.get("hard_gate_status"), "UNKNOWN"),
        "candidate_review_status": _text(candidate.get("review_status"), "UNKNOWN"),
        "created_at": created.isoformat(),
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(DYNAMIC_SHADOW_SAFETY),
        **DYNAMIC_SHADOW_SAFETY,
    }
    _assert_dynamic_shadow_payload_safe(payload)
    return payload


def build_dynamic_shadow_approved_enrollment(
    *,
    approval: Mapping[str, Any],
    review_package: Mapping[str, Any],
    policy: DynamicShadowPolicyConfig | None = None,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or load_dynamic_shadow_policy_config()
    created = _coerce_datetime(created_at or datetime.now(UTC))
    required_decision = resolved_policy.enrollment_limits.required_owner_decision
    if _text(approval.get("owner_decision")) != required_decision:
        raise DynamicShadowError("only approved_for_dynamic_shadow decisions can enroll")
    if approval.get("approved_for_enrollment") is not True:
        raise DynamicShadowError("approval is not marked approved_for_enrollment")
    candidate_id = _text(approval.get("candidate_id"))
    candidate = _candidate_from_package(review_package, candidate_id)
    if not candidate:
        raise DynamicShadowError("approval candidate_id is not present in review package")
    if _texts(candidate.get("hard_blockers")):
        raise DynamicShadowError("candidate has hard blockers and cannot enroll")

    enrollment_id = _stable_id(
        "dynamic-shadow-enrollment",
        approval.get("approval_id"),
        review_package.get("review_package_id"),
        candidate_id,
    )
    shadow_candidate_id = _stable_id("dynamic-shadow-candidate", candidate_id)
    next_review_due = (
        created.date() + timedelta(days=resolved_policy.enrollment_limits.default_next_review_days)
    ).isoformat()
    payload = {
        "schema_version": DYNAMIC_SHADOW_ENROLLMENT_SCHEMA_VERSION,
        "report_type": DYNAMIC_SHADOW_ENROLLMENT_REPORT_TYPE,
        "enrollment_id": enrollment_id,
        "dynamic_shadow_candidate_id": shadow_candidate_id,
        "candidate_id": candidate_id,
        "approval_id": _text(approval.get("approval_id"), "MISSING"),
        "review_package_id": _text(review_package.get("review_package_id"), "MISSING"),
        "owner_approval_status": _text(approval.get("owner_decision")),
        "tracking_status": "active_shadow",
        "tracking_start_date": created.date().isoformat(),
        "next_review_due": next_review_due,
        "forward_tracking_metrics": list(resolved_policy.forward_tracking.metrics),
        "source_links": {
            "review_package": _text(review_package.get("review_package_id")),
            "approval": _text(approval.get("approval_id")),
            **_mapping(review_package.get("source_artifacts")),
        },
        "decision_journal_link": _text(approval.get("decision_journal_link")),
        "created_at": created.isoformat(),
        "registry_entry": {
            "dynamic_shadow_candidate_id": shadow_candidate_id,
            "candidate_id": candidate_id,
            "tracking_status": "active_shadow",
            "tracking_start_date": created.date().isoformat(),
            "next_review_due": next_review_due,
            "approval_id": _text(approval.get("approval_id")),
            "review_package_id": _text(review_package.get("review_package_id")),
            "source_dynamic_robustness_report": _mapping(
                review_package.get("source_artifacts")
            ).get("dynamic_robustness_report", ""),
            "decision_journal_link": _text(approval.get("decision_journal_link")),
        },
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(DYNAMIC_SHADOW_SAFETY),
        **DYNAMIC_SHADOW_SAFETY,
    }
    _assert_dynamic_shadow_payload_safe(payload)
    return payload


def load_dynamic_shadow_candidate_registry(
    registry_path: Path | str = DEFAULT_DYNAMIC_SHADOW_REGISTRY_PATH,
) -> dict[str, Any]:
    path = Path(registry_path)
    if not path.exists():
        return _empty_registry()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DynamicShadowError(f"invalid dynamic shadow registry JSON: {path}") from exc
    if not isinstance(payload, Mapping):
        raise DynamicShadowError("dynamic shadow registry must be a mapping")
    return dict(payload)


def upsert_dynamic_shadow_candidate_registry(
    registry: Mapping[str, Any],
    enrollment: Mapping[str, Any],
    *,
    updated_at: datetime | None = None,
) -> dict[str, Any]:
    updated = _coerce_datetime(updated_at or datetime.now(UTC))
    entry = dict(_mapping(enrollment.get("registry_entry")))
    if not entry:
        raise DynamicShadowError("enrollment missing registry_entry")
    existing = [
        row
        for row in _records(registry.get("active_candidates"))
        if row.get("dynamic_shadow_candidate_id") != entry.get("dynamic_shadow_candidate_id")
    ]
    active_candidates = [*existing, entry]
    payload = {
        "schema_version": DYNAMIC_SHADOW_REGISTRY_SCHEMA_VERSION,
        "report_type": "etf_dynamic_shadow_candidate_registry",
        "updated_at": updated.isoformat(),
        "candidate_count": len(active_candidates),
        "active_candidates": active_candidates,
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(DYNAMIC_SHADOW_SAFETY),
        **DYNAMIC_SHADOW_SAFETY,
    }
    _assert_dynamic_shadow_payload_safe(payload)
    return payload


def build_dynamic_shadow_forward_update(
    *,
    registry: Mapping[str, Any],
    policy: DynamicShadowPolicyConfig | None = None,
    as_of: date | str,
    data_quality_status: str,
    data_quality_report: str = "",
    prices: pd.DataFrame | None = None,
    etf_config: ETFConfigBundle | None = None,
    dynamic_shadow_policy: DynamicShadowPolicyConfig | None = None,
    dynamic_robustness_policy: DynamicRobustnessPolicyConfig | None = None,
    dynamic_allocation_policy: DynamicAllocationPolicyConfig | None = None,
    dynamic_calibration_report: Mapping[str, Any] | None = None,
    dynamic_calibration_report_path: Path | None = None,
    prices_path: Path | None = None,
    robustness_reports_by_candidate: Mapping[str, Mapping[str, Any]] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or dynamic_shadow_policy or load_dynamic_shadow_policy_config()
    run_date = _parse_date(as_of)
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    records: list[dict[str, Any]] = []
    for candidate in _records(registry.get("active_candidates")):
        if _text(candidate.get("tracking_status"), "active_shadow") not in {
            "active_shadow",
            "needs_more_data",
            "watch",
        }:
            continue
        records.append(
            _forward_record_for_candidate(
                candidate,
                policy=resolved_policy,
                as_of=run_date,
                data_quality_status=data_quality_status,
                data_quality_report=data_quality_report,
                prices=prices,
                etf_config=etf_config,
                dynamic_robustness_policy=dynamic_robustness_policy,
                dynamic_allocation_policy=dynamic_allocation_policy,
                dynamic_calibration_report=dynamic_calibration_report,
                dynamic_calibration_report_path=dynamic_calibration_report_path,
                prices_path=prices_path,
                robustness_reports_by_candidate=robustness_reports_by_candidate or {},
            )
        )
    status = "PASS" if records else "NEEDS_MORE_DATA"
    if any(row["tracking_status"] == "blocked" for row in records):
        status = "REVIEW_REQUIRED"
    payload = {
        "schema_version": DYNAMIC_SHADOW_FORWARD_UPDATE_SCHEMA_VERSION,
        "report_type": DYNAMIC_SHADOW_FORWARD_UPDATE_REPORT_TYPE,
        "forward_update_id": _stable_id("dynamic-shadow-forward-update", run_date.isoformat()),
        "as_of": run_date.isoformat(),
        "generated_at": generated.isoformat(),
        "status": status,
        "data_quality_status": data_quality_status,
        "data_quality_report": data_quality_report,
        "active_candidate_count": len(records),
        "tracking_records": records,
        "summary": {
            "active_candidate_count": len(records),
            "needs_more_data_count": len(
                [row for row in records if row["tracking_status"] == "needs_more_data"]
            ),
            "watch_count": len([row for row in records if row["tracking_status"] == "watch"]),
            "blocked_count": len([row for row in records if row["tracking_status"] == "blocked"]),
        },
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(DYNAMIC_SHADOW_SAFETY),
        **DYNAMIC_SHADOW_SAFETY,
    }
    _assert_dynamic_shadow_payload_safe(payload)
    return payload


def build_dynamic_shadow_weekly_review(
    *,
    forward_update: Mapping[str, Any],
    policy: DynamicShadowPolicyConfig | None = None,
    as_of: date | str | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or load_dynamic_shadow_policy_config()
    review_date = _parse_date(as_of or forward_update.get("as_of") or date.today())
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    reviews = [
        _weekly_candidate_review(row, policy=resolved_policy)
        for row in _records(forward_update.get("tracking_records"))
    ]
    status = "PASS"
    if any(row["review_status"] == "reject_pending_review" for row in reviews):
        status = "REVIEW_REQUIRED"
    elif any(row["review_status"] == "watch" for row in reviews):
        status = "WATCH"
    elif any(row["review_status"] == "needs_more_data" for row in reviews):
        status = "NEEDS_MORE_DATA"
    payload = {
        "schema_version": DYNAMIC_SHADOW_WEEKLY_REVIEW_SCHEMA_VERSION,
        "report_type": DYNAMIC_SHADOW_WEEKLY_REVIEW_REPORT_TYPE,
        "weekly_review_id": _stable_id("dynamic-shadow-weekly-review", review_date.isoformat()),
        "as_of": review_date.isoformat(),
        "generated_at": generated.isoformat(),
        "status": status,
        "review_statuses": list(resolved_policy.weekly_review.active_statuses),
        "candidate_reviews": reviews,
        "summary": {
            "candidate_count": len(reviews),
            "active_shadow_count": len(
                [row for row in reviews if row["review_status"] == "active_shadow"]
            ),
            "needs_more_data_count": len(
                [row for row in reviews if row["review_status"] == "needs_more_data"]
            ),
            "watch_count": len([row for row in reviews if row["review_status"] == "watch"]),
            "reject_pending_review_count": len(
                [row for row in reviews if row["review_status"] == "reject_pending_review"]
            ),
        },
        "manual_review_actions": [
            row["manual_review_action"]
            for row in reviews
            if row["review_status"] in {"watch", "reject_pending_review"}
        ],
        "decision_journal_integration": {
            "proposed_entries": [
                _weekly_decision_journal_entry(row)
                for row in reviews
                if row["review_status"] in {"watch", "reject_pending_review"}
            ],
            "commands_executed": False,
        },
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(DYNAMIC_SHADOW_SAFETY),
        **DYNAMIC_SHADOW_SAFETY,
    }
    _assert_dynamic_shadow_payload_safe(payload)
    return payload


def build_dynamic_shadow_validation_report(
    *,
    config_path: Path | str = DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    evidence_dashboard_config_path: Path = PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "evidence_dashboard.yaml",
    reader_brief_path: Path = PROJECT_ROOT
    / "src"
    / "ai_trading_system"
    / "reports"
    / "reader_brief.py",
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    checks: list[dict[str, Any]] = []
    policy: DynamicShadowPolicyConfig | None = None
    try:
        policy = load_dynamic_shadow_policy_config(config_path)
        _append_check(checks, "policy_config_valid", True, "dynamic shadow policy loads")
    except Exception as exc:  # noqa: BLE001
        _append_check(checks, "policy_config_valid", False, str(exc))
    if policy is not None:
        try:
            sample = _sample_dynamic_shadow_workflow(policy, generated)
            _append_check(
                checks,
                "approved_only_workflow_available",
                sample["approved_enrollment_built"],
                "approved owner decision can create enrollment",
            )
            _append_check(
                checks,
                "unapproved_enrollment_blocked",
                sample["unapproved_enrollment_blocked"],
                "non-approved owner decision cannot enroll",
            )
            _append_check(
                checks,
                "forward_records_created",
                sample["forward_records_created"],
                "forward tracking records include required metrics",
            )
            _append_check(
                checks,
                "weekly_review_available",
                sample["weekly_review_available"],
                "weekly review produces dynamic shadow candidate statuses",
            )
        except Exception as exc:  # noqa: BLE001
            _append_check(checks, "workflow_probe", False, str(exc))
        _append_check(
            checks,
            "safety_boundary_safe",
            policy.safety.model_dump(mode="json") == DYNAMIC_SHADOW_SAFETY,
            "production_effect=none; broker_action=none; no auto enrollment",
        )
    registry_text = _safe_read_text(report_registry_path)
    _append_check(
        checks,
        "report_registry_visibility",
        all(
            marker in registry_text
            for marker in (
                DYNAMIC_SHADOW_PACKAGE_REPORT_TYPE,
                DYNAMIC_SHADOW_APPROVAL_REPORT_TYPE,
                DYNAMIC_SHADOW_ENROLLMENT_REPORT_TYPE,
                DYNAMIC_SHADOW_FORWARD_UPDATE_REPORT_TYPE,
                DYNAMIC_SHADOW_WEEKLY_REVIEW_REPORT_TYPE,
                DYNAMIC_SHADOW_VALIDATION_REPORT_TYPE,
            )
        ),
        "report registry exposes dynamic shadow artifacts",
    )
    evidence_config_text = _safe_read_text(evidence_dashboard_config_path)
    _append_check(
        checks,
        "evidence_dashboard_visibility",
        "dynamic_shadow" in evidence_config_text
        and DYNAMIC_SHADOW_WEEKLY_REVIEW_REPORT_TYPE in evidence_config_text,
        "strategy evidence dashboard config includes dynamic shadow source",
    )
    reader_brief_text = _safe_read_text(reader_brief_path)
    _append_check(
        checks,
        "reader_brief_visibility",
        "Dynamic Shadow Review" in reader_brief_text
        and "_etf_dynamic_shadow_summary" in reader_brief_text,
        "Reader Brief has Dynamic Shadow Review section",
    )
    failed = [check for check in checks if check["status"] != "PASS"]
    payload = {
        "schema_version": DYNAMIC_SHADOW_VALIDATION_SCHEMA_VERSION,
        "report_type": DYNAMIC_SHADOW_VALIDATION_REPORT_TYPE,
        "validation_id": _stable_id(
            "dynamic-shadow-validation",
            generated.strftime("%Y%m%dT%H%M%SZ"),
            _stable_hash([check["check_id"] for check in checks]),
        ),
        "generated_at": generated.isoformat(),
        "status": "PASS" if not failed else "FAIL",
        "check_count": len(checks),
        "failed_check_count": len(failed),
        "checks": checks,
        "source_schema_versions": {
            "policy": DYNAMIC_SHADOW_POLICY_SCHEMA_VERSION,
            "package": DYNAMIC_SHADOW_PACKAGE_SCHEMA_VERSION,
            "approval": DYNAMIC_SHADOW_APPROVAL_SCHEMA_VERSION,
            "enrollment": DYNAMIC_SHADOW_ENROLLMENT_SCHEMA_VERSION,
            "forward_update": DYNAMIC_SHADOW_FORWARD_UPDATE_SCHEMA_VERSION,
            "weekly_review": DYNAMIC_SHADOW_WEEKLY_REVIEW_SCHEMA_VERSION,
        },
        "approved_only_enrollment_required": True,
        "official_target_weights_write_blocked": True,
        "automatic_candidate_promotion_blocked": True,
        "auto_enrollment_without_owner_approval_blocked": True,
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(DYNAMIC_SHADOW_SAFETY),
        **DYNAMIC_SHADOW_SAFETY,
    }
    _assert_dynamic_shadow_payload_safe(payload)
    return payload


def write_dynamic_shadow_review_package(
    payload: Mapping[str, Any],
    output_dir: Path = DEFAULT_DYNAMIC_SHADOW_PACKAGE_DIR,
) -> dict[str, Path]:
    package_id = _text(payload.get("review_package_id"), "dynamic-shadow-package")
    return _write_json_md(
        payload,
        output_dir=output_dir,
        stem=package_id,
        markdown=render_dynamic_shadow_review_package_markdown(payload),
    )


def write_dynamic_shadow_owner_approval(
    payload: Mapping[str, Any],
    output_dir: Path = DEFAULT_DYNAMIC_SHADOW_APPROVAL_DIR,
) -> dict[str, Path]:
    approval_id = _text(payload.get("approval_id"), "dynamic-shadow-approval")
    return _write_json_md(
        payload,
        output_dir=output_dir,
        stem=approval_id,
        markdown=render_dynamic_shadow_owner_approval_markdown(payload),
    )


def write_dynamic_shadow_approved_enrollment(
    payload: Mapping[str, Any],
    output_dir: Path = DEFAULT_DYNAMIC_SHADOW_ENROLLMENT_DIR,
) -> dict[str, Path]:
    enrollment_id = _text(payload.get("enrollment_id"), "dynamic-shadow-enrollment")
    return _write_json_md(
        payload,
        output_dir=output_dir,
        stem=enrollment_id,
        markdown=render_dynamic_shadow_enrollment_markdown(payload),
    )


def write_dynamic_shadow_candidate_registry(
    payload: Mapping[str, Any],
    registry_path: Path = DEFAULT_DYNAMIC_SHADOW_REGISTRY_PATH,
) -> Path:
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return registry_path


def write_dynamic_shadow_forward_update(
    payload: Mapping[str, Any],
    output_dir: Path = DEFAULT_DYNAMIC_SHADOW_FORWARD_UPDATE_DIR,
) -> dict[str, Path]:
    update_id = _text(payload.get("forward_update_id"), "dynamic-shadow-forward-update")
    return _write_json_md(
        payload,
        output_dir=output_dir,
        stem=update_id,
        markdown=render_dynamic_shadow_forward_update_markdown(payload),
    )


def write_dynamic_shadow_weekly_review(
    payload: Mapping[str, Any],
    output_dir: Path = DEFAULT_DYNAMIC_SHADOW_WEEKLY_REVIEW_DIR,
) -> dict[str, Path]:
    weekly_id = _text(payload.get("weekly_review_id"), "dynamic-shadow-weekly-review")
    return _write_json_md(
        payload,
        output_dir=output_dir,
        stem=weekly_id,
        markdown=render_dynamic_shadow_weekly_review_markdown(payload),
    )


def write_dynamic_shadow_validation_report(
    payload: Mapping[str, Any],
    output_dir: Path = DEFAULT_DYNAMIC_SHADOW_VALIDATION_DIR,
) -> dict[str, Path]:
    validation_id = _text(payload.get("validation_id"), "dynamic-shadow-validation")
    return _write_json_md(
        payload,
        output_dir=output_dir,
        stem=validation_id,
        markdown=render_dynamic_shadow_validation_markdown(payload),
    )


def latest_dynamic_shadow_review_package_path(
    package_dir: Path = DEFAULT_DYNAMIC_SHADOW_PACKAGE_DIR,
) -> Path | None:
    return _latest_json(package_dir, "dynamic-shadow-package_*.json")


def latest_dynamic_shadow_owner_approval_path(
    approval_dir: Path = DEFAULT_DYNAMIC_SHADOW_APPROVAL_DIR,
) -> Path | None:
    return _latest_json(approval_dir, "dynamic-shadow-approval_*.json")


def latest_dynamic_shadow_forward_update_path(
    update_dir: Path = DEFAULT_DYNAMIC_SHADOW_FORWARD_UPDATE_DIR,
) -> Path | None:
    return _latest_json(update_dir, "dynamic-shadow-forward-update_*.json")


def latest_dynamic_shadow_weekly_review_path(
    weekly_dir: Path = DEFAULT_DYNAMIC_SHADOW_WEEKLY_REVIEW_DIR,
) -> Path | None:
    return _latest_json(weekly_dir, "dynamic-shadow-weekly-review_*.json")


def render_dynamic_shadow_review_package_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("review_summary"))
    lines = [
        f"# Dynamic Shadow Review Package {payload.get('review_package_id')}",
        "",
        f"- Status: {summary.get('status')}",
        f"- Top Candidate: {summary.get('top_candidate')}",
        f"- Ready After Owner Approval: {summary.get('ready_after_owner_approval_count')}",
        f"- Blocked Count: {summary.get('blocked_count')}",
        (
            "- Safety: observe_only=true; candidate_only=true; production_effect=none; "
            "broker_action=none; manual_review_required=true"
        ),
        "",
        "## Candidates",
        "",
        (
            "| Candidate | Hard Gate | Review Status | Robustness | Data Quality | "
            "Warnings | Blockers |"
        ),
        "|---|---:|---|---|---|---:|---:|",
    ]
    for row in _records(payload.get("top_review_candidates")):
        lines.append(
            (
                "| {candidate} | {gate} | {review} | {robustness} | {quality} | "
                "{warnings} | {blockers} |"
            ).format(
                candidate=row.get("candidate_id"),
                gate=row.get("hard_gate_status"),
                review=row.get("review_status"),
                robustness=row.get("robustness_status"),
                quality=row.get("data_quality_status"),
                warnings=len(_texts(row.get("warnings"))),
                blockers=len(_texts(row.get("hard_blockers"))),
            )
        )
    lines.extend(["", "## Source Artifacts", ""])
    for key, value in _mapping(payload.get("source_artifacts")).items():
        lines.append(f"- {key}: {value or 'MISSING'}")
    return "\n".join(lines) + "\n"


def render_dynamic_shadow_owner_approval_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Dynamic Shadow Owner Approval {payload.get('approval_id')}",
            "",
            f"- Candidate: {payload.get('candidate_id')}",
            f"- Owner Decision: {payload.get('owner_decision')}",
            f"- Approved For Enrollment: {payload.get('approved_for_enrollment')}",
            f"- Reviewer: {payload.get('reviewer')}",
            f"- Decision Journal Link: {payload.get('decision_journal_link') or 'MISSING'}",
            (
                "- Safety: observe_only=true; candidate_only=true; production_effect=none; "
                "broker_action=none"
            ),
            "",
        ]
    )


def render_dynamic_shadow_enrollment_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Dynamic Shadow Enrollment {payload.get('enrollment_id')}",
            "",
            f"- Candidate: {payload.get('candidate_id')}",
            f"- Dynamic Shadow Candidate: {payload.get('dynamic_shadow_candidate_id')}",
            f"- Tracking Status: {payload.get('tracking_status')}",
            f"- Tracking Start: {payload.get('tracking_start_date')}",
            f"- Next Review Due: {payload.get('next_review_due')}",
            "- Safety: production_effect=none; broker_action=none; production_state_mutated=false",
            "",
        ]
    )


def render_dynamic_shadow_forward_update_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# Dynamic Shadow Forward Update {payload.get('as_of')}",
        "",
        f"- Status: {payload.get('status')}",
        f"- Data Quality: {payload.get('data_quality_status')}",
        f"- Active Candidates: {payload.get('active_candidate_count')}",
        "",
        (
            "| Candidate | Status | Days | Dynamic Return | Excess vs Static | Drawdown | "
            "Turnover | False Signals |"
        ),
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in _records(payload.get("tracking_records")):
        metrics = _mapping(row.get("metrics"))
        lines.append(
            (
                "| {candidate} | {status} | {days} | {dynamic} | {excess} | "
                "{drawdown} | {turnover} | {false} |"
            ).format(
                candidate=row.get("candidate_id"),
                status=row.get("tracking_status"),
                days=row.get("tracking_day_count"),
                dynamic=_fmt_pct(metrics.get("dynamic_candidate_return")),
                excess=_fmt_pct(metrics.get("excess_vs_static")),
                drawdown=_fmt_pct(metrics.get("drawdown")),
                turnover=_fmt_num(metrics.get("turnover")),
                false=metrics.get("false_signal_count"),
            )
        )
    return "\n".join(lines) + "\n"


def render_dynamic_shadow_weekly_review_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# Dynamic Shadow Weekly Review {payload.get('as_of')}",
        "",
        f"- Status: {payload.get('status')}",
        "",
        "| Candidate | Review Status | Action | Reasons |",
        "|---|---|---|---|",
    ]
    for row in _records(payload.get("candidate_reviews")):
        lines.append(
            "| {candidate} | {status} | {action} | {reasons} |".format(
                candidate=row.get("candidate_id"),
                status=row.get("review_status"),
                action=_mapping(row.get("manual_review_action")).get("action_type", ""),
                reasons=", ".join(_texts(row.get("review_reasons"))),
            )
        )
    return "\n".join(lines) + "\n"


def render_dynamic_shadow_validation_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# Dynamic Shadow Validation {payload.get('validation_id')}",
        "",
        f"- Status: {payload.get('status')}",
        f"- Failed Checks: {payload.get('failed_check_count')}",
        "",
        "| Check | Status | Summary |",
        "|---|---:|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"| {check.get('check_id')} | {check.get('status')} | {check.get('summary')} |"
        )
    return "\n".join(lines) + "\n"


def _dynamic_candidate_review_row(
    *,
    candidate_id: str,
    robustness_report: Mapping[str, Any],
    dynamic_calibration_report: Mapping[str, Any],
    dynamic_calibration_validation: Mapping[str, Any],
    dynamic_robustness_validation: Mapping[str, Any],
    operations_validation: Mapping[str, Any],
    policy: DynamicShadowPolicyConfig,
    source_paths: Mapping[str, str],
) -> dict[str, Any]:
    summary = _mapping(robustness_report.get("summary"))
    dynamic_row = _comparison_row(robustness_report, "dynamic_candidate")
    static_row = _comparison_row(robustness_report, "static_base_candidate")
    current_row = _comparison_row(robustness_report, "current_etf_baseline")
    gates = {
        "dynamic_calibration_validation": _gate_status(
            dynamic_calibration_validation,
            policy.required_gates.dynamic_calibration_validation_statuses,
            "DYNAMIC_CALIBRATION_VALIDATION_NOT_PASS",
        ),
        "dynamic_robustness_validation": _gate_status(
            dynamic_robustness_validation,
            policy.required_gates.dynamic_robustness_validation_statuses,
            "DYNAMIC_ROBUSTNESS_VALIDATION_NOT_PASS",
        ),
        "operations_validation": _gate_status(
            operations_validation,
            policy.required_gates.operations_validation_statuses,
            "OPS_VALIDATION_NOT_PASS",
        ),
        "data_quality": _direct_gate_status(
            _text(summary.get("data_quality_status"), "MISSING"),
            policy.required_gates.data_quality_statuses,
            "DATA_QUALITY_NOT_PASS",
        ),
        "safety": _safety_gate_status(robustness_report),
    }
    if policy.required_gates.require_dynamic_calibration_report and not dynamic_calibration_report:
        gates["dynamic_calibration_report"] = {
            "status": "FAIL",
            "actual_status": "MISSING",
            "blocker": "MISSING_DYNAMIC_CALIBRATION_REPORT",
        }
    hard_blockers = [
        _text(gate.get("blocker"))
        for gate in gates.values()
        if _text(gate.get("status")) != "PASS" and _text(gate.get("blocker"))
    ]
    warnings = _candidate_warnings(robustness_report, policy)
    robustness_status = _text(robustness_report.get("status"), "MISSING")
    if robustness_status not in policy.required_gates.allowed_robustness_statuses_for_shadow:
        hard_blockers.append("DYNAMIC_ROBUSTNESS_STATUS_NOT_ALLOWED_FOR_SHADOW")
    hard_gate_status = "PASS" if not hard_blockers else "FAIL"
    review_status = "owner_approval_required" if hard_gate_status == "PASS" else "blocked"
    return {
        "candidate_id": candidate_id,
        "review_status": review_status,
        "hard_gate_status": hard_gate_status,
        "hard_blockers": hard_blockers,
        "warnings": warnings,
        "enrollment_allowed_after_owner_approval": hard_gate_status == "PASS",
        "owner_approval_required": True,
        "robustness_status": robustness_status,
        "overfit_status": _text(summary.get("overfit_status"), "UNKNOWN"),
        "data_quality_status": _text(summary.get("data_quality_status"), "MISSING"),
        "dynamic_metrics": {
            "dynamic_candidate_return": dynamic_row.get("total_return"),
            "dynamic_cagr": dynamic_row.get("CAGR"),
            "dynamic_max_drawdown": dynamic_row.get("max_drawdown"),
            "dynamic_turnover": dynamic_row.get("turnover"),
            "static_base_return": static_row.get("total_return"),
            "current_baseline_return": current_row.get("total_return"),
            "excess_vs_static": _float(dynamic_row.get("total_return"))
            - _float(static_row.get("total_return")),
            "excess_vs_baseline": _float(dynamic_row.get("total_return"))
            - _float(current_row.get("total_return")),
            "false_risk_off_count": summary.get("false_risk_off_count"),
            "false_risk_on_count": summary.get("false_risk_on_count"),
        },
        "required_gates": gates,
        "source_links": {
            "dynamic_robustness_report": source_paths.get("dynamic_robustness_report", ""),
            "dynamic_calibration_report": source_paths.get("dynamic_calibration_report", ""),
            "dynamic_calibration_validation": source_paths.get(
                "dynamic_calibration_validation", ""
            ),
            "dynamic_robustness_validation": source_paths.get(
                "dynamic_robustness_validation", ""
            ),
            "operations_validation": source_paths.get("operations_validation", ""),
        },
    }


def _forward_record_for_candidate(
    candidate: Mapping[str, Any],
    *,
    policy: DynamicShadowPolicyConfig,
    as_of: date,
    data_quality_status: str,
    data_quality_report: str,
    prices: pd.DataFrame | None,
    etf_config: ETFConfigBundle | None,
    dynamic_robustness_policy: DynamicRobustnessPolicyConfig | None,
    dynamic_allocation_policy: DynamicAllocationPolicyConfig | None,
    dynamic_calibration_report: Mapping[str, Any] | None,
    dynamic_calibration_report_path: Path | None,
    prices_path: Path | None,
    robustness_reports_by_candidate: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    candidate_id = _text(candidate.get("candidate_id"))
    tracking_start = _parse_date(candidate.get("tracking_start_date") or as_of)
    if as_of <= tracking_start:
        return _needs_more_data_forward_record(candidate, as_of, "as_of_not_after_tracking_start")
    report = robustness_reports_by_candidate.get(candidate_id)
    if report is None:
        if (
            prices is None
            or etf_config is None
            or dynamic_robustness_policy is None
            or dynamic_allocation_policy is None
        ):
            return _needs_more_data_forward_record(
                candidate,
                as_of,
                "missing_price_or_policy_inputs_for_forward_update",
            )
        try:
            report = build_dynamic_robustness_report(
                prices=prices,
                etf_config=etf_config,
                policy=dynamic_robustness_policy,
                dynamic_policy=dynamic_allocation_policy,
                candidate_id=candidate_id,
                dynamic_calibration_report=dynamic_calibration_report,
                dynamic_calibration_report_path=dynamic_calibration_report_path,
                start=tracking_start,
                end=as_of,
                data_quality_status=data_quality_status,
                data_quality_report=data_quality_report,
                prices_path=prices_path,
            )
        except Exception as exc:  # noqa: BLE001
            return _needs_more_data_forward_record(candidate, as_of, str(exc))
    metrics = _forward_metrics(report)
    tracking_day_count = int(
        _mapping(report.get("daily_path_summary")).get("row_count")
        or _float(_comparison_row(report, "dynamic_candidate").get("trading_days"))
    )
    status = (
        "needs_more_data"
        if tracking_day_count < policy.forward_tracking.minimum_tracking_days_for_watch
        else "active_shadow"
    )
    return {
        "dynamic_shadow_candidate_id": _text(candidate.get("dynamic_shadow_candidate_id")),
        "candidate_id": candidate_id,
        "as_of": as_of.isoformat(),
        "tracking_start_date": tracking_start.isoformat(),
        "tracking_day_count": tracking_day_count,
        "tracking_status": status,
        "metrics": metrics,
        "source_dynamic_robustness_report": _text(
            report.get("dynamic_robustness_report_id"),
            _text(candidate.get("source_dynamic_robustness_report")),
        ),
        "data_quality_status": data_quality_status,
        "data_quality_report": data_quality_report,
        "production_effect": "none",
        "broker_action": "none",
    }


def _forward_metrics(report: Mapping[str, Any]) -> dict[str, Any]:
    dynamic = _comparison_row(report, "dynamic_candidate")
    static = _comparison_row(report, "static_base_candidate")
    current = _comparison_row(report, "current_etf_baseline")
    qqq = _comparison_row(report, "QQQ_buy_and_hold")
    spy = _comparison_row(report, "SPY_buy_and_hold")
    smh = _comparison_row(report, "SMH_buy_and_hold")
    false_signal = _mapping(report.get("false_signal_diagnostics"))
    false_off = _mapping(false_signal.get("false_risk_off"))
    false_on = _mapping(false_signal.get("false_risk_on"))
    daily = _mapping(report.get("daily_path_summary"))
    return {
        "dynamic_candidate_return": dynamic.get("total_return"),
        "static_base_return": static.get("total_return"),
        "current_baseline_return": current.get("total_return"),
        "QQQ_return": qqq.get("total_return"),
        "SPY_return": spy.get("total_return"),
        "SMH_return": smh.get("total_return"),
        "excess_vs_static": _float(dynamic.get("total_return"))
        - _float(static.get("total_return")),
        "excess_vs_baseline": _float(dynamic.get("total_return"))
        - _float(current.get("total_return")),
        "drawdown": dynamic.get("max_drawdown"),
        "turnover": dynamic.get("turnover"),
        "regime_switch_count": int(_float(daily.get("regime_switch_count"))),
        "false_signal_count": int(_float(false_off.get("event_count"))) + int(
            _float(false_on.get("event_count"))
        ),
        "constraint_hit_count": int(_float(daily.get("constraint_hit_count"))),
    }


def _weekly_candidate_review(
    row: Mapping[str, Any],
    *,
    policy: DynamicShadowPolicyConfig,
) -> dict[str, Any]:
    metrics = _mapping(row.get("metrics"))
    tracking_days = int(_float(row.get("tracking_day_count")))
    review_reasons: list[str] = []
    status = "active_shadow"
    if tracking_days < policy.forward_tracking.minimum_tracking_days_for_review:
        status = "needs_more_data"
        review_reasons.append("minimum_tracking_days_not_met")
    drawdown = _float(metrics.get("drawdown"))
    false_signals = int(_float(metrics.get("false_signal_count")))
    if (
        drawdown <= policy.weekly_review.reject_pending_drawdown_threshold
        or false_signals >= policy.weekly_review.reject_pending_false_signal_count
    ):
        status = "reject_pending_review"
        review_reasons.append("reject_pending_threshold_triggered")
    elif (
        drawdown <= policy.weekly_review.watch_drawdown_threshold
        or _float(metrics.get("excess_vs_static"))
        <= policy.weekly_review.watch_excess_vs_static_threshold
        or _constraint_hit_ratio(metrics, tracking_days)
        > policy.weekly_review.max_constraint_hit_ratio
    ):
        status = "watch"
        review_reasons.append("watch_threshold_triggered")
    if not review_reasons:
        review_reasons.append("continue_forward_shadow_observation")
    action = {
        "action_type": _manual_review_action_for_status(status),
        "candidate_id": _text(row.get("candidate_id")),
        "manual_review_required": status in {"watch", "reject_pending_review"},
        "production_effect": "none",
        "broker_action": "none",
    }
    return {
        "dynamic_shadow_candidate_id": _text(row.get("dynamic_shadow_candidate_id")),
        "candidate_id": _text(row.get("candidate_id")),
        "review_status": status,
        "tracking_day_count": tracking_days,
        "metrics": metrics,
        "review_reasons": review_reasons,
        "manual_review_action": action,
        "production_effect": "none",
        "broker_action": "none",
    }


def _sample_dynamic_shadow_workflow(
    policy: DynamicShadowPolicyConfig,
    generated: datetime,
) -> dict[str, bool]:
    report = _sample_robustness_report()
    validation = {"status": "PASS", "report_type": "validation_sample"}
    package = build_dynamic_shadow_review_package(
        dynamic_robustness_report=report,
        dynamic_calibration_report={"status": "PASS"},
        dynamic_calibration_validation=validation,
        dynamic_robustness_validation=validation,
        operations_validation=validation,
        policy=policy,
        source_paths={"dynamic_robustness_report": "validation_sample"},
        generated_at=generated,
    )
    blocked = False
    try:
        rejected = build_dynamic_shadow_owner_approval(
            review_package=package,
            candidate_id="validation_dynamic_candidate",
            owner_decision="continue_review",
            rationale="validation sample",
            confidence=0.5,
            policy=policy,
            created_at=generated,
        )
        build_dynamic_shadow_approved_enrollment(
            approval=rejected,
            review_package=package,
            policy=policy,
            created_at=generated,
        )
    except DynamicShadowError:
        blocked = True
    approval = build_dynamic_shadow_owner_approval(
        review_package=package,
        candidate_id="validation_dynamic_candidate",
        owner_decision="approved_for_dynamic_shadow",
        rationale="validation sample approval",
        confidence=0.8,
        decision_journal_link="validation_journal_entry",
        policy=policy,
        created_at=generated,
    )
    enrollment = build_dynamic_shadow_approved_enrollment(
        approval=approval,
        review_package=package,
        policy=policy,
        created_at=generated,
    )
    registry = upsert_dynamic_shadow_candidate_registry(_empty_registry(), enrollment)
    update = build_dynamic_shadow_forward_update(
        registry=registry,
        policy=policy,
        as_of=generated.date() + timedelta(days=30),
        data_quality_status="PASS",
        data_quality_report="validation_sample",
        robustness_reports_by_candidate={"validation_dynamic_candidate": report},
        generated_at=generated,
    )
    weekly = build_dynamic_shadow_weekly_review(
        forward_update=update,
        policy=policy,
        as_of=generated.date() + timedelta(days=30),
        generated_at=generated,
    )
    metrics = _mapping(_records(update.get("tracking_records"))[0].get("metrics"))
    return {
        "approved_enrollment_built": enrollment["tracking_status"] == "active_shadow",
        "unapproved_enrollment_blocked": blocked,
        "forward_records_created": REQUIRED_FORWARD_METRICS.issubset(metrics),
        "weekly_review_available": bool(weekly.get("candidate_reviews")),
    }


def _sample_robustness_report() -> dict[str, Any]:
    comparison = [
        _sample_comparison_row("dynamic_candidate", 0.08, -0.08, 0.9),
        _sample_comparison_row("static_base_candidate", 0.05, -0.09, 0.0),
        _sample_comparison_row("current_etf_baseline", 0.04, -0.10, 0.0),
        _sample_comparison_row("QQQ_buy_and_hold", 0.06, -0.11, 0.0),
        _sample_comparison_row("SPY_buy_and_hold", 0.03, -0.07, 0.0),
        _sample_comparison_row("SMH_buy_and_hold", 0.07, -0.12, 0.0),
    ]
    return {
        "schema_version": "etf_dynamic_robustness_report_v1",
        "report_type": "etf_dynamic_robustness_report",
        "dynamic_robustness_report_id": "dynamic-robustness-report_validation",
        "status": "PASS",
        "summary": {
            "dynamic_candidate_id": "validation_dynamic_candidate",
            "market_regime": "ai_after_chatgpt",
            "data_quality_status": "PASS",
            "dynamic_total_return": 0.08,
            "dynamic_max_drawdown": -0.08,
            "excess_vs_static_base": 0.03,
            "false_risk_off_count": 1,
            "false_risk_on_count": 1,
            "overfit_status": "PASS",
        },
        "comparison_table": comparison,
        "false_signal_diagnostics": {
            "false_risk_off": {"event_count": 1},
            "false_risk_on": {"event_count": 1},
        },
        "daily_path_summary": {
            "row_count": 30,
            "regime_switch_count": 2,
            "constraint_hit_count": 1,
        },
        "source_artifacts": {"data_quality_report": "validation_sample"},
        "safety": dict(DYNAMIC_SHADOW_SAFETY),
        **DYNAMIC_SHADOW_SAFETY,
        "commands_executed": False,
    }


def _sample_comparison_row(
    comparison_id: str,
    total_return: float,
    max_drawdown: float,
    turnover: float,
) -> dict[str, Any]:
    return {
        "comparison_id": comparison_id,
        "status": "AVAILABLE",
        "total_return": total_return,
        "CAGR": total_return,
        "max_drawdown": max_drawdown,
        "turnover": turnover,
        "trading_days": 30,
        "production_effect": "none",
        "broker_action": "none",
    }


def _required_precondition_statuses(candidate: Mapping[str, Any]) -> dict[str, Any]:
    gates = _mapping(candidate.get("required_gates"))
    return {
        key: {
            "status": _text(_mapping(value).get("status")),
            "actual_status": _text(_mapping(value).get("actual_status")),
        }
        for key, value in gates.items()
    }


def _candidate_warnings(
    robustness_report: Mapping[str, Any],
    policy: DynamicShadowPolicyConfig,
) -> list[str]:
    summary = _mapping(robustness_report.get("summary"))
    warnings: list[str] = []
    if _text(robustness_report.get("status")) == "REVIEW_REQUIRED":
        warnings.append("dynamic_robustness_report_requires_owner_review")
    if _text(summary.get("overfit_status")) != "PASS":
        warnings.append("dynamic_overfit_requires_owner_review")
    if _float(summary.get("excess_vs_static_base")) < 0:
        warnings.append("dynamic_candidate_underperformed_static_base")
    false_count = int(_float(summary.get("false_risk_off_count"))) + int(
        _float(summary.get("false_risk_on_count"))
    )
    if false_count >= policy.weekly_review.reject_pending_false_signal_count:
        warnings.append("high_historical_false_signal_count")
    return warnings


def _gate_status(
    payload: Mapping[str, Any],
    allowed_statuses: Sequence[str],
    blocker: str,
) -> dict[str, str]:
    status = _text(payload.get("status"), "MISSING")
    return {
        "status": "PASS" if status in set(allowed_statuses) else "FAIL",
        "actual_status": status,
        "blocker": "" if status in set(allowed_statuses) else blocker,
    }


def _direct_gate_status(
    status: str,
    allowed_statuses: Sequence[str],
    blocker: str,
) -> dict[str, str]:
    return {
        "status": "PASS" if status in set(allowed_statuses) else "FAIL",
        "actual_status": status,
        "blocker": "" if status in set(allowed_statuses) else blocker,
    }


def _safety_gate_status(payload: Mapping[str, Any]) -> dict[str, str]:
    safety = _mapping(payload.get("safety")) or {
        key: payload.get(key) for key in DYNAMIC_SHADOW_SAFETY
    }
    safe = (
        safety.get("production_effect") == "none"
        and safety.get("broker_action") == "none"
        and payload.get("production_effect") == "none"
        and payload.get("broker_action") == "none"
    )
    return {
        "status": "PASS" if safe else "FAIL",
        "actual_status": "PASS" if safe else "FAIL",
        "blocker": "" if safe else "UNSAFE_PRODUCTION_OR_BROKER_EFFECT",
    }


def _candidate_from_package(
    review_package: Mapping[str, Any],
    candidate_id: str,
) -> dict[str, Any]:
    for candidate in _records(review_package.get("top_review_candidates")):
        if _text(candidate.get("candidate_id")) == candidate_id:
            return candidate
    return {}


def _comparison_row(report: Mapping[str, Any], comparison_id: str) -> dict[str, Any]:
    for row in _records(report.get("comparison_table")):
        if _text(row.get("comparison_id")) == comparison_id:
            return row
    return {}


def _needs_more_data_forward_record(
    candidate: Mapping[str, Any],
    as_of: date,
    reason: str,
) -> dict[str, Any]:
    return {
        "dynamic_shadow_candidate_id": _text(candidate.get("dynamic_shadow_candidate_id")),
        "candidate_id": _text(candidate.get("candidate_id")),
        "as_of": as_of.isoformat(),
        "tracking_start_date": _text(candidate.get("tracking_start_date")),
        "tracking_day_count": 0,
        "tracking_status": "needs_more_data",
        "metrics": {metric: None for metric in REQUIRED_FORWARD_METRICS},
        "insufficient_data_reason": reason,
        "production_effect": "none",
        "broker_action": "none",
    }


def _constraint_hit_ratio(metrics: Mapping[str, Any], tracking_days: int) -> float:
    if tracking_days <= 0:
        return 0.0
    return _float(metrics.get("constraint_hit_count")) / tracking_days


def _manual_review_action_for_status(status: str) -> str:
    if status == "reject_pending_review":
        return "review_rejection"
    if status == "watch":
        return "review_dynamic_shadow_watch"
    if status == "needs_more_data":
        return "continue_observation"
    return "continue_dynamic_shadow"


def _weekly_decision_journal_entry(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "entry_type": "dynamic_shadow_weekly_review",
        "candidate_id": _text(row.get("candidate_id")),
        "review_status": _text(row.get("review_status")),
        "recommended_action": _mapping(row.get("manual_review_action")).get("action_type"),
        "production_effect": "none",
        "broker_action": "none",
    }


def _empty_registry() -> dict[str, Any]:
    return {
        "schema_version": DYNAMIC_SHADOW_REGISTRY_SCHEMA_VERSION,
        "report_type": "etf_dynamic_shadow_candidate_registry",
        "updated_at": "",
        "candidate_count": 0,
        "active_candidates": [],
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(DYNAMIC_SHADOW_SAFETY),
        **DYNAMIC_SHADOW_SAFETY,
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
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    markdown_path.write_text(markdown, encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}


def _latest_json(directory: Path, pattern: str) -> Path | None:
    if not directory.exists():
        return None
    files = [path for path in directory.glob(pattern) if path.is_file()]
    if not files:
        return None
    return max(files, key=lambda path: path.stat().st_mtime)


def _append_check(
    checks: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    summary: str,
    details: Mapping[str, Any] | None = None,
) -> None:
    checks.append(
        {
            "check_id": check_id,
            "status": "PASS" if passed else "FAIL",
            "summary": summary,
            "details": dict(details or {}),
        }
    )


def _assert_dynamic_shadow_payload_safe(payload: Mapping[str, Any]) -> None:
    for key, expected in DYNAMIC_SHADOW_SAFETY.items():
        if payload.get(key) != expected:
            raise DynamicShadowError(f"dynamic shadow safety violation: {key}")
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    for key in FORBIDDEN_DYNAMIC_SHADOW_KEYS:
        if key in text and key not in {
            "automatic_candidate_promotion",
            "auto_enrollment_without_owner_approval",
            "official_target_weights_write",
        }:
            raise DynamicShadowError(f"dynamic shadow payload contains forbidden key: {key}")


def _safe_read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _safe_stem(value: str) -> str:
    return value.replace("/", "_").replace("\\", "_").replace(":", "_")


def _coerce_datetime(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def _parse_date(value: date | str | object) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


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
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if pd.isna(parsed):
        return default
    return parsed


def _stable_id(prefix: str, *parts: object) -> str:
    digest = _stable_hash([prefix, *parts])[:12]
    return f"{prefix}_{digest}"


def _stable_hash(value: object) -> str:
    return sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _fmt_pct(value: object) -> str:
    if value is None:
        return "n/a"
    return f"{_float(value):.2%}"


def _fmt_num(value: object) -> str:
    if value is None:
        return "n/a"
    return f"{_float(value):.3f}"
