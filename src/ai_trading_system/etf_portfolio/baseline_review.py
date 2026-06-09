from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal, Self

from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.decision_journal import (
    DEFAULT_DECISION_JOURNAL_PATH,
    DecisionJournalError,
    add_decision_entry,
    build_decision_entry,
    load_decision_journal,
    write_decision_journal,
)
from ai_trading_system.etf_portfolio.models import PolicyMetadata, load_etf_config_bundle
from ai_trading_system.etf_portfolio.strategy_evidence_dashboard import (
    DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH,
    STRATEGY_EVIDENCE_SAFETY,
    StrategyEvidenceDashboard,
    StrategyEvidenceSourceReport,
    build_strategy_evidence_dashboard,
    load_strategy_evidence_dashboard_config,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    build_report_index_payload,
    load_report_registry,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "baseline_review.yaml"
)
DEFAULT_BASELINE_REVIEW_REPORT_DIR = PROJECT_ROOT / "reports" / "etf_portfolio" / "baseline_review"
DEFAULT_BASELINE_REVIEW_PACKAGE_DIR = DEFAULT_BASELINE_REVIEW_REPORT_DIR / "packages"
DEFAULT_BASELINE_REVIEW_DECISION_DIR = DEFAULT_BASELINE_REVIEW_REPORT_DIR / "decisions"
DEFAULT_BASELINE_REVIEW_PROPOSAL_DIR = DEFAULT_BASELINE_REVIEW_REPORT_DIR / "proposals"
DEFAULT_BASELINE_REVIEW_OUTCOME_DIR = DEFAULT_BASELINE_REVIEW_REPORT_DIR / "outcomes"
DEFAULT_BASELINE_REVIEW_VALIDATION_DIR = DEFAULT_BASELINE_REVIEW_REPORT_DIR / "validation"
JSON_SIDECAR_SUFFIXES = {".md", ".markdown", ".html", ".htm"}

BASELINE_REVIEW_POLICY_SCHEMA_VERSION = "etf_baseline_review_policy_v1"
BASELINE_REVIEW_MATRIX_SCHEMA_VERSION = "etf_baseline_review_evidence_matrix_v1"
BASELINE_REVIEW_ELIGIBILITY_SCHEMA_VERSION = "etf_baseline_review_eligibility_v1"
BASELINE_REVIEW_PACKAGE_SCHEMA_VERSION = "etf_baseline_review_package_v1"
BASELINE_REVIEW_DECISION_SCHEMA_VERSION = "etf_baseline_review_owner_decision_v1"
BASELINE_REVIEW_PROPOSAL_SCHEMA_VERSION = "etf_baseline_change_proposal_draft_v1"
BASELINE_REVIEW_OUTCOME_SCHEMA_VERSION = "etf_baseline_review_outcome_v1"
BASELINE_REVIEW_VALIDATION_SCHEMA_VERSION = "etf_baseline_review_validation_v1"

BASELINE_REVIEW_PACKAGE_REPORT_TYPE = "etf_baseline_review_package"
BASELINE_REVIEW_DECISION_REPORT_TYPE = "etf_baseline_review_owner_decision"
BASELINE_REVIEW_PROPOSAL_REPORT_TYPE = "etf_baseline_change_proposal_draft"
BASELINE_REVIEW_OUTCOME_REPORT_TYPE = "etf_baseline_review_outcome"
BASELINE_REVIEW_VALIDATION_REPORT_TYPE = "etf_baseline_review_validation"

BASELINE_REVIEW_PACKAGE_REGISTRY_ID = "etf_baseline_review_package"
BASELINE_REVIEW_DECISION_REGISTRY_ID = "etf_baseline_review_decision"
BASELINE_REVIEW_PROPOSAL_REGISTRY_ID = "etf_baseline_change_proposal_draft"
BASELINE_REVIEW_OUTCOME_REGISTRY_ID = "etf_baseline_review_outcome"
BASELINE_REVIEW_VALIDATION_REGISTRY_ID = "etf_baseline_review_validation"

BASELINE_REVIEW_SAFETY = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
}

EvidenceRequirementId = Literal[
    "historical_backtest",
    "walk_forward_robustness",
    "forward_performance",
    "drawdown_control",
    "turnover_control",
    "AI_attribution",
    "satellite_attribution",
    "parameter_review",
    "weekly_review",
    "decision_journal",
    "data_quality",
    "ops_health",
    "validation_gates",
]
CandidateType = Literal[
    "weight_calibration_candidate",
    "forward_shadow_candidate",
    "parameter_review_candidate",
    "AI_overlay_candidate",
    "satellite_adjusted_candidate",
]
EligibilityStatus = Literal[
    "eligible_for_owner_review",
    "needs_more_data",
    "defer_review",
    "blocked",
    "rejected_by_policy",
    "ready_for_proposal_draft",
]
EvidenceRowStatus = Literal[
    "satisfied",
    "missing",
    "optional_missing",
    "stale",
    "blocked",
    "needs_more_data",
    "warning",
]
OwnerDecision = Literal[
    "approve_for_proposal_draft",
    "continue_shadow",
    "needs_more_data",
    "reject_candidate",
    "defer_review",
    "request_new_experiment",
]
OutcomeStatus = Literal[
    "review_pending",
    "proposal_drafted",
    "continue_shadow",
    "needs_more_data",
    "rejected",
    "deferred",
    "archived",
]

REQUIRED_EVIDENCE_IDS: tuple[EvidenceRequirementId, ...] = (
    "historical_backtest",
    "walk_forward_robustness",
    "forward_performance",
    "drawdown_control",
    "turnover_control",
    "AI_attribution",
    "satellite_attribution",
    "parameter_review",
    "weekly_review",
    "decision_journal",
    "data_quality",
    "ops_health",
    "validation_gates",
)
ALLOWED_OWNER_DECISIONS = {
    "approve_for_proposal_draft",
    "continue_shadow",
    "needs_more_data",
    "reject_candidate",
    "defer_review",
    "request_new_experiment",
}
DISALLOWED_OWNER_DECISIONS = {
    "apply_to_production",
    "place_order",
    "enable_broker_action",
}
CRITICAL_BLOCKERS = {
    "EVIDENCE_DASHBOARD_BLOCKED",
    "DATA_QUALITY_CRITICAL",
    "OPS_VALIDATION_FAILED",
    "UNSAFE_PRODUCTION_EFFECT",
    "BROKER_ACTION_NOT_NONE",
}


class BaselineReviewError(ValueError):
    """Raised when baseline review inputs or outputs are unsafe."""


class BaselineReviewSafety(BaseModel):
    observe_only: Literal[True]
    candidate_only: Literal[True]
    production_effect: Literal["none"]
    broker_action: Literal["none"]
    manual_review_required: Literal[True]


class BaselineReviewEligibilityThresholds(BaseModel):
    minimum_forward_days: int = Field(ge=0)
    minimum_weekly_reviews: int = Field(ge=0)
    minimum_decision_journal_entries: int = Field(ge=0)
    max_allowed_turnover_delta: float = Field(ge=0)
    max_allowed_drawdown_delta: float = Field(ge=0)
    max_validation_gate_age_days: int = Field(ge=0)
    require_data_quality_pass: bool
    require_ops_validation_pass: bool
    require_evidence_dashboard_not_blocked: bool
    require_decision_journal_link: bool
    require_parameter_review_not_blocked: bool


class BaselineReviewEvidenceRequirement(BaseModel):
    title: str = Field(min_length=1)
    source_category: str = Field(min_length=1)
    source_report_id: str = Field(min_length=1)
    required: bool
    max_age_days: int = Field(ge=0)
    minimum_sample_count: int = Field(ge=0)
    blocking_if_missing: bool


class BaselineReviewBlockingCondition(BaseModel):
    severity: Literal["critical", "blocking", "warning"]
    blocks_review: bool
    description: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_blocking_policy(self) -> Self:
        if self.severity == "critical" and not self.blocks_review:
            raise ValueError("critical blocking condition must block review")
        return self


class BaselineReviewChecklistItem(BaseModel):
    checklist_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    required: bool
    prompt: str = Field(min_length=1)


class BaselineReviewDecisionCapturePolicy(BaseModel):
    allowed_decisions: list[OwnerDecision] = Field(min_length=1)
    rationale_required_for: list[OwnerDecision] = Field(default_factory=list)
    disallowed_decisions: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_decisions(self) -> Self:
        if set(self.allowed_decisions) - ALLOWED_OWNER_DECISIONS:
            raise ValueError("unknown baseline review owner decision")
        if set(self.disallowed_decisions) != DISALLOWED_OWNER_DECISIONS:
            raise ValueError("disallowed baseline review decisions must be explicit")
        return self


class BaselineReviewProposalDraftPolicy(BaseModel):
    require_owner_decision: Literal[True]
    required_owner_decision: Literal["approve_for_proposal_draft"]
    require_review_package: Literal[True]
    require_eligibility_pass: Literal[True]
    require_decision_journal_link: Literal[True]
    proposal_is_draft_only: Literal[True]
    production_config_mutation_allowed: Literal[False]
    target_weight_mutation_allowed: Literal[False]
    broker_action_allowed: Literal[False]


class BaselineReviewOutcomeTrackingPolicy(BaseModel):
    allowed_statuses: list[OutcomeStatus] = Field(min_length=1)
    default_next_review_days: int = Field(ge=0)


class BaselineReviewPolicyConfig(BaseModel):
    schema_version: Literal["etf_baseline_review_policy_v1"]
    policy_metadata: PolicyMetadata
    safety: BaselineReviewSafety
    candidate_types: list[CandidateType] = Field(min_length=1)
    eligibility_thresholds: BaselineReviewEligibilityThresholds
    required_evidence: dict[EvidenceRequirementId, BaselineReviewEvidenceRequirement]
    blocking_conditions: dict[str, BaselineReviewBlockingCondition]
    review_checklist: list[BaselineReviewChecklistItem] = Field(min_length=1)
    decision_capture_policy: BaselineReviewDecisionCapturePolicy
    proposal_draft_policy: BaselineReviewProposalDraftPolicy
    outcome_tracking_policy: BaselineReviewOutcomeTrackingPolicy

    @model_validator(mode="after")
    def validate_policy(self) -> Self:
        if self.safety.model_dump(mode="json") != BASELINE_REVIEW_SAFETY:
            raise ValueError("baseline review safety boundary is unsafe")
        missing_evidence = sorted(set(REQUIRED_EVIDENCE_IDS) - set(self.required_evidence))
        if missing_evidence:
            raise ValueError(
                "baseline review required_evidence missing: " + ", ".join(missing_evidence)
            )
        missing_blockers = sorted(
            {
                "EVIDENCE_DASHBOARD_BLOCKED",
                "DATA_QUALITY_CRITICAL",
                "OPS_VALIDATION_FAILED",
                "FORWARD_SAMPLE_TOO_SMALL",
                "HIGH_DRAWDOWN",
                "HIGH_TURNOVER",
                "CONFIG_DRIFT",
                "VALIDATION_GATE_STALE",
                "NO_DECISION_JOURNAL_LINK",
                "PARAMETER_REVIEW_BLOCKED",
                "UNSAFE_PRODUCTION_EFFECT",
                "BROKER_ACTION_NOT_NONE",
            }
            - set(self.blocking_conditions)
        )
        if missing_blockers:
            raise ValueError(
                "baseline review blocking_conditions missing: " + ", ".join(missing_blockers)
            )
        for condition_id, condition in self.blocking_conditions.items():
            if not condition_id.strip():
                raise ValueError("baseline review blocking condition ID cannot be empty")
            if condition_id != condition_id.upper():
                raise ValueError(f"{condition_id}: blocking condition IDs must be uppercase")
            if condition.severity in {"critical", "blocking"} and not condition.blocks_review:
                raise ValueError(f"{condition_id}: blocking condition must block review")
        return self


class BaselineReviewEvidenceMatrixRow(BaseModel):
    evidence_id: EvidenceRequirementId
    required: bool
    status: EvidenceRowStatus
    source_report: str = Field(min_length=1)
    latest_as_of_date: str = Field(min_length=1)
    freshness: str = Field(min_length=1)
    sample_count: int = Field(ge=0)
    blocking: bool
    notes: str = Field(min_length=1)


class BaselineReviewEvidenceMatrix(BaseModel):
    schema_version: Literal["etf_baseline_review_evidence_matrix_v1"] = (
        BASELINE_REVIEW_MATRIX_SCHEMA_VERSION
    )
    candidate_id: str = Field(min_length=1)
    candidate_type: CandidateType
    as_of_date: date
    rows: list[BaselineReviewEvidenceMatrixRow] = Field(min_length=1)
    source_report_links: list[str] = Field(default_factory=list)
    safety: BaselineReviewSafety
    observe_only: Literal[True] = True
    candidate_only: Literal[True] = True
    production_effect: Literal["none"] = "none"
    broker_action: Literal["none"] = "none"
    manual_review_required: Literal[True] = True


def load_baseline_review_policy_config(
    path: Path | str = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
) -> BaselineReviewPolicyConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, dict):
        raise BaselineReviewError("baseline review policy config must be a mapping")
    try:
        return BaselineReviewPolicyConfig.model_validate(raw)
    except ValueError as exc:
        raise BaselineReviewError(str(exc)) from exc


def build_baseline_review_evidence_matrix(
    *,
    candidate_id: str,
    as_of: date | str,
    config: BaselineReviewPolicyConfig | None = None,
    config_path: Path | str = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    strategy_dashboard: Mapping[str, Any] | StrategyEvidenceDashboard | None = None,
    report_index: Mapping[str, Any] | None = None,
    report_index_path: Path | None = None,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    root_path: Path = PROJECT_ROOT,
) -> BaselineReviewEvidenceMatrix:
    run_date = _parse_date(as_of)
    policy = config or load_baseline_review_policy_config(config_path)
    dashboard = _resolve_strategy_dashboard(
        as_of=run_date,
        strategy_dashboard=strategy_dashboard,
        report_index=report_index,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        root_path=root_path,
    )
    source_reports = _source_reports(dashboard)
    cards = _cards(dashboard)
    payloads = _source_payloads(source_reports)
    context = _candidate_context(candidate_id, dashboard, payloads)
    candidate_type = _supported_candidate_type(context.candidate_type, policy)
    rows = [
        _matrix_row(
            evidence_id=evidence_id,
            requirement=policy.required_evidence[evidence_id],
            policy=policy,
            dashboard=dashboard,
            cards=cards,
            source_reports=source_reports,
            context=context,
            as_of=run_date,
        )
        for evidence_id in REQUIRED_EVIDENCE_IDS
    ]
    source_links = _unique_strings(row.source_report for row in rows)
    return BaselineReviewEvidenceMatrix(
        candidate_id=candidate_id,
        candidate_type=candidate_type,
        as_of_date=run_date,
        rows=rows,
        source_report_links=source_links,
        safety=policy.safety,
    )


def build_baseline_review_eligibility(
    *,
    candidate_id: str,
    as_of: date | str,
    config: BaselineReviewPolicyConfig | None = None,
    config_path: Path | str = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    strategy_dashboard: Mapping[str, Any] | StrategyEvidenceDashboard | None = None,
    report_index: Mapping[str, Any] | None = None,
    report_index_path: Path | None = None,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    root_path: Path = PROJECT_ROOT,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    run_date = _parse_date(as_of)
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    policy = config or load_baseline_review_policy_config(config_path)
    dashboard = _resolve_strategy_dashboard(
        as_of=run_date,
        strategy_dashboard=strategy_dashboard,
        report_index=report_index,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        root_path=root_path,
    )
    source_reports = _source_reports(dashboard)
    payloads = _source_payloads(source_reports)
    context = _candidate_context(candidate_id, dashboard, payloads)
    matrix = build_baseline_review_evidence_matrix(
        candidate_id=candidate_id,
        as_of=run_date,
        config=policy,
        strategy_dashboard=dashboard,
        report_index=report_index,
        report_registry_path=report_registry_path,
        root_path=root_path,
    )
    blockers, warnings = _eligibility_findings(
        candidate_id=candidate_id,
        dashboard=dashboard,
        matrix=matrix,
        context=context,
        source_payloads=payloads,
        policy=policy,
    )
    status = _eligibility_status(blockers)
    supporting = [
        {
            "evidence_id": row.evidence_id,
            "status": row.status,
            "source_report": row.source_report,
            "freshness": row.freshness,
            "sample_count": row.sample_count,
        }
        for row in matrix.rows
        if row.status == "satisfied"
    ]
    missing = [
        {
            "evidence_id": row.evidence_id,
            "status": row.status,
            "source_report": row.source_report,
            "blocking": row.blocking,
        }
        for row in matrix.rows
        if row.status in {"missing", "optional_missing", "stale", "blocked", "needs_more_data"}
    ]
    payload = {
        "schema_version": BASELINE_REVIEW_ELIGIBILITY_SCHEMA_VERSION,
        "report_type": "etf_baseline_review_eligibility",
        "eligibility_id": _stable_id("baseline-review-eligibility", candidate_id, run_date),
        "candidate_id": candidate_id,
        "candidate_type": matrix.candidate_type,
        "as_of_date": run_date.isoformat(),
        "generated_at": generated.isoformat(),
        "eligibility_status": status,
        "blockers": blockers,
        "warnings": warnings,
        "supporting_evidence": supporting,
        "required_missing_evidence": missing,
        "manual_review_required": True,
        "evidence_requirement_matrix": matrix.model_dump(mode="json"),
        "source_report_links": matrix.source_report_links,
        "candidate_context": context.to_payload(),
        "safety": policy.safety.model_dump(mode="json"),
        "commands_executed": False,
        "production_state_mutated": False,
        **BASELINE_REVIEW_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def build_baseline_review_package(
    *,
    candidate_id: str,
    as_of: date | str,
    config: BaselineReviewPolicyConfig | None = None,
    config_path: Path | str = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    strategy_dashboard: Mapping[str, Any] | StrategyEvidenceDashboard | None = None,
    report_index: Mapping[str, Any] | None = None,
    report_index_path: Path | None = None,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    root_path: Path = PROJECT_ROOT,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    run_date = _parse_date(as_of)
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    policy = config or load_baseline_review_policy_config(config_path)
    dashboard = _resolve_strategy_dashboard(
        as_of=run_date,
        strategy_dashboard=strategy_dashboard,
        report_index=report_index,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        root_path=root_path,
    )
    source_payloads = _source_payloads(_source_reports(dashboard))
    context = _candidate_context(candidate_id, dashboard, source_payloads)
    eligibility = build_baseline_review_eligibility(
        candidate_id=candidate_id,
        as_of=run_date,
        config=policy,
        strategy_dashboard=dashboard,
        report_index=report_index,
        report_registry_path=report_registry_path,
        root_path=root_path,
        generated_at=generated,
    )
    matrix = _mapping(eligibility.get("evidence_requirement_matrix"))
    package_id = _stable_id("baseline-review-package", candidate_id, run_date)
    payload = {
        "schema_version": BASELINE_REVIEW_PACKAGE_SCHEMA_VERSION,
        "report_type": BASELINE_REVIEW_PACKAGE_REPORT_TYPE,
        "review_package_id": package_id,
        "candidate_id": candidate_id,
        "candidate_type": eligibility["candidate_type"],
        "as_of_date": run_date.isoformat(),
        "generated_at": generated.isoformat(),
        "safety_banner": dict(BASELINE_REVIEW_SAFETY),
        "candidate_metadata": {
            "candidate_id": candidate_id,
            "candidate_type": eligibility["candidate_type"],
            "candidate_exists": context.exists,
            "source_record_count": len(context.records),
        },
        "current_baseline_summary": _current_baseline_summary(),
        "candidate_allocation_summary": _candidate_allocation_summary(context),
        "historical_evidence": _section_from_matrix(matrix, "historical_backtest"),
        "forward_evidence": _section_from_matrix(matrix, "forward_performance"),
        "risk_drawdown_turnover_evidence": {
            "drawdown_control": _section_from_matrix(matrix, "drawdown_control"),
            "turnover_control": _section_from_matrix(matrix, "turnover_control"),
            "drawdown_delta": context.metric(
                "drawdown_delta",
                "max_drawdown_delta",
                "candidate_drawdown_delta",
            ),
            "turnover_delta": context.metric(
                "turnover_delta",
                "turnover_since_enrollment",
                "candidate_turnover_delta",
            ),
        },
        "data_quality_status": _section_from_matrix(matrix, "data_quality"),
        "ops_health_status": _section_from_matrix(matrix, "ops_health"),
        "AI_attribution_context": _section_from_matrix(matrix, "AI_attribution"),
        "satellite_attribution_context": _section_from_matrix(matrix, "satellite_attribution"),
        "decision_journal_summary": _section_from_matrix(matrix, "decision_journal"),
        "parameter_review_summary": _section_from_matrix(matrix, "parameter_review"),
        "evidence_requirement_matrix": matrix,
        "eligibility": eligibility,
        "blockers": eligibility["blockers"],
        "warnings": eligibility["warnings"],
        "owner_review_checklist": [
            {
                "checklist_id": item.checklist_id,
                "title": item.title,
                "required": item.required,
                "prompt": item.prompt,
                "status": "pending_owner_review",
            }
            for item in policy.review_checklist
        ],
        "recommended_decision_options": list(policy.decision_capture_policy.allowed_decisions),
        "source_report_links": eligibility["source_report_links"],
        "review_summary": {
            "eligible_count": (
                1 if eligibility["eligibility_status"] == "eligible_for_owner_review" else 0
            ),
            "needs_more_data_count": (
                1 if eligibility["eligibility_status"] == "needs_more_data" else 0
            ),
            "blocked_count": 1 if eligibility["eligibility_status"] == "blocked" else 0,
            "proposal_draft_count": 0,
            "manual_review_required": True,
        },
        "commands_executed": False,
        "production_state_mutated": False,
        "proposal_draft_generated": False,
        "baseline_config_mutated": False,
        "target_weights_mutated": False,
        "broker_order_submitted": False,
        "safety": policy.safety.model_dump(mode="json"),
        **BASELINE_REVIEW_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def build_owner_review_decision(
    *,
    review_package: Mapping[str, Any],
    owner_decision: str,
    rationale: str,
    confidence: float,
    conditions: Sequence[str] | None = None,
    follow_up_tasks: Sequence[str] | None = None,
    config: BaselineReviewPolicyConfig | None = None,
    config_path: Path | str = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    created_at: datetime | None = None,
    decision_id: str | None = None,
) -> dict[str, Any]:
    policy = config or load_baseline_review_policy_config(config_path)
    decision = _text(owner_decision)
    if decision in DISALLOWED_OWNER_DECISIONS:
        raise BaselineReviewError(f"disallowed owner decision: {decision}")
    if decision not in set(policy.decision_capture_policy.allowed_decisions):
        raise BaselineReviewError(f"unsupported owner decision: {decision}")
    if decision in set(policy.decision_capture_policy.rationale_required_for) and not _text(
        rationale
    ):
        raise BaselineReviewError(f"rationale is required for {decision}")
    if confidence < 0.0 or confidence > 1.0:
        raise BaselineReviewError("confidence must be between 0.0 and 1.0")
    created = _coerce_datetime(created_at or datetime.now(UTC))
    candidate_id = _text(review_package.get("candidate_id"))
    package_id = _text(review_package.get("review_package_id"))
    if not candidate_id or not package_id:
        raise BaselineReviewError("review package must include candidate_id and review_package_id")
    payload = {
        "schema_version": BASELINE_REVIEW_DECISION_SCHEMA_VERSION,
        "report_type": BASELINE_REVIEW_DECISION_REPORT_TYPE,
        "decision_id": decision_id
        or _stable_id("baseline-review-decision", candidate_id, package_id, created),
        "candidate_id": candidate_id,
        "review_package_id": package_id,
        "owner_decision": decision,
        "rationale": _text(rationale),
        "confidence": float(confidence),
        "conditions": _unique_strings([str(item) for item in conditions or []]),
        "follow_up_tasks": _unique_strings([str(item) for item in follow_up_tasks or []]),
        "created_at": created.isoformat(),
        "decision_journal_linkage": {
            "status": "not_linked",
            "journal_path": "",
            "journal_decision_id": "",
        },
        "manual_review_required": True,
        "safety": policy.safety.model_dump(mode="json"),
        "commands_executed": False,
        "production_state_mutated": False,
        **BASELINE_REVIEW_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def link_baseline_review_decision_to_journal(
    decision: Mapping[str, Any],
    *,
    review_package_path: Path,
    journal_path: Path = DEFAULT_DECISION_JOURNAL_PATH,
    updated_at: datetime | None = None,
) -> dict[str, Any]:
    if not review_package_path.exists():
        raise BaselineReviewError(f"review package path missing: {review_package_path}")
    review_package = _read_json_object(review_package_path)
    if _text(review_package.get("report_type")) != BASELINE_REVIEW_PACKAGE_REPORT_TYPE:
        raise BaselineReviewError("review_package_path must point to a baseline review package")
    timestamp = _coerce_datetime(updated_at or datetime.now(UTC))
    candidate_id = _text(decision.get("candidate_id"))
    journal_decision_id = "journal-" + _text(decision.get("decision_id")).replace(":", "-")
    entry = build_decision_entry(
        review_id=_text(decision.get("review_package_id")),
        review_date=timestamp.date(),
        source_weekly_review=review_package_path,
        action_item_id="baseline_review_owner_decision",
        human_decision=_text(decision.get("owner_decision")),
        decision_status=_journal_status_for_owner_decision(_text(decision.get("owner_decision"))),
        rationale=_text(decision.get("rationale")),
        confidence=float(decision.get("confidence") or 0.0),
        follow_up_task="; ".join(_texts(decision.get("follow_up_tasks"))) or "none",
        linked_candidate=candidate_id,
        linked_report=review_package_path,
        created_at=timestamp,
        decision_id=journal_decision_id,
        extra_fields={
            "source_section": "baseline_review",
            "source_baseline_review_package": str(review_package_path),
            "review_package_id": decision.get("review_package_id"),
            "owner_decision": decision.get("owner_decision"),
            "evidence_matrix": review_package.get("evidence_requirement_matrix"),
            "baseline_review_conditions": _texts(decision.get("conditions")),
            "baseline_review_follow_up_tasks": _texts(decision.get("follow_up_tasks")),
        },
    )
    try:
        journal = load_decision_journal(journal_path)
        updated = add_decision_entry(journal, entry, updated_at=timestamp)
        write_decision_journal(updated, journal_path)
    except DecisionJournalError as exc:
        raise BaselineReviewError(str(exc)) from exc
    result = dict(decision)
    result["decision_journal_linkage"] = {
        "status": "linked",
        "journal_path": str(journal_path),
        "journal_decision_id": journal_decision_id,
        "linked_at": timestamp.isoformat(),
    }
    _assert_safe_output(result)
    return result


def build_baseline_change_proposal_draft(
    *,
    review_package: Mapping[str, Any],
    owner_decision: Mapping[str, Any],
    config: BaselineReviewPolicyConfig | None = None,
    config_path: Path | str = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    policy = config or load_baseline_review_policy_config(config_path)
    required_decision = policy.proposal_draft_policy.required_owner_decision
    if _text(owner_decision.get("owner_decision")) != required_decision:
        raise BaselineReviewError("proposal draft requires approve_for_proposal_draft decision")
    linkage = _mapping(owner_decision.get("decision_journal_linkage"))
    if linkage.get("status") != "linked":
        raise BaselineReviewError("proposal draft requires linked decision journal entry")
    eligibility = _mapping(review_package.get("eligibility"))
    if _text(eligibility.get("eligibility_status")) != "eligible_for_owner_review":
        raise BaselineReviewError("proposal draft requires eligible_for_owner_review status")
    if _unsafe_safety_fields(owner_decision) or _unsafe_safety_fields(review_package):
        raise BaselineReviewError("proposal draft input safety boundary is unsafe")
    created = _coerce_datetime(created_at or datetime.now(UTC))
    candidate_id = _text(review_package.get("candidate_id"))
    candidate_summary = _mapping(review_package.get("candidate_allocation_summary"))
    payload = {
        "schema_version": BASELINE_REVIEW_PROPOSAL_SCHEMA_VERSION,
        "report_type": BASELINE_REVIEW_PROPOSAL_REPORT_TYPE,
        "proposal_id": _stable_id(
            "baseline-change-proposal-draft",
            candidate_id,
            _text(owner_decision.get("decision_id")),
            created,
        ),
        "candidate_id": candidate_id,
        "review_package_id": review_package.get("review_package_id"),
        "owner_decision_id": owner_decision.get("decision_id"),
        "current_baseline_config_hash": _current_baseline_summary()["config_hash"],
        "candidate_config_hash": _hash_json(candidate_summary),
        "proposed_changes": {
            "change_type": "baseline_allocation_review_draft",
            "candidate_allocation_summary": candidate_summary,
            "application_allowed": False,
            "production_config_mutation_allowed": False,
            "target_weight_mutation_allowed": False,
        },
        "supporting_evidence": eligibility.get("supporting_evidence", []),
        "remaining_risks": {
            "blockers": review_package.get("blockers", []),
            "warnings": review_package.get("warnings", []),
        },
        "approval_conditions": _texts(owner_decision.get("conditions")),
        "manual_review_required": True,
        "production_effect": "none",
        "broker_action": "none",
        "created_at": created.isoformat(),
        "proposal_is_draft_only": True,
        "baseline_config_mutated": False,
        "target_weights_mutated": False,
        "broker_order_submitted": False,
        "safety": policy.safety.model_dump(mode="json"),
        "commands_executed": False,
        "production_state_mutated": False,
        **BASELINE_REVIEW_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def build_candidate_review_outcome(
    *,
    candidate_id: str,
    decision: Mapping[str, Any] | None = None,
    proposal: Mapping[str, Any] | None = None,
    previous_outcome: Mapping[str, Any] | None = None,
    config: BaselineReviewPolicyConfig | None = None,
    config_path: Path | str = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    policy = config or load_baseline_review_policy_config(config_path)
    created = _coerce_datetime(created_at or datetime.now(UTC))
    latest_status = _outcome_status_for(decision=decision, proposal=proposal)
    if latest_status not in set(policy.outcome_tracking_policy.allowed_statuses):
        raise BaselineReviewError(f"unsafe baseline review outcome status: {latest_status}")
    prior_history = _records(_mapping(previous_outcome).get("review_history"))
    latest_decision_id = "" if decision is None else _text(decision.get("decision_id"))
    latest_proposal_id = "" if proposal is None else _text(proposal.get("proposal_id"))
    history_event = {
        "event": latest_status,
        "timestamp": created.isoformat(),
        "decision_id": latest_decision_id,
        "proposal_id": latest_proposal_id,
    }
    next_review_due = (
        created.date() + timedelta(days=policy.outcome_tracking_policy.default_next_review_days)
    ).isoformat()
    payload = {
        "schema_version": BASELINE_REVIEW_OUTCOME_SCHEMA_VERSION,
        "report_type": BASELINE_REVIEW_OUTCOME_REPORT_TYPE,
        "candidate_id": candidate_id,
        "latest_review_status": latest_status,
        "latest_decision_id": latest_decision_id,
        "latest_proposal_id": latest_proposal_id,
        "review_history": [*prior_history, history_event],
        "next_review_due": next_review_due,
        "follow_up_tasks": [] if decision is None else _texts(decision.get("follow_up_tasks")),
        "created_at": created.isoformat(),
        "safety": policy.safety.model_dump(mode="json"),
        "commands_executed": False,
        "production_state_mutated": False,
        **BASELINE_REVIEW_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def build_baseline_review_validation_report(
    *,
    as_of: date | str | None = None,
    config_path: Path | str = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    run_date = _parse_date(as_of or date.today())
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    checks: list[dict[str, Any]] = []
    policy: BaselineReviewPolicyConfig | None = None
    try:
        policy = load_baseline_review_policy_config(config_path)
        _append_check(checks, "policy_config_valid", True, "baseline review policy loads")
    except Exception as exc:  # noqa: BLE001 - validation report captures failure.
        _append_check(
            checks,
            "policy_config_valid",
            False,
            "baseline review policy failed validation",
            {"error": str(exc), "error_type": type(exc).__name__},
        )
    if policy is not None:
        probe = _workflow_probe(policy, run_date, generated)
        for check_id, passed in probe.items():
            _append_check(
                checks,
                check_id,
                bool(passed),
                _validation_check_summary(check_id, bool(passed)),
            )
        _append_check(
            checks,
            "reader_brief_integration_available",
            _registry_has_baseline_review(report_registry_path),
            "Reader Brief can discover baseline review registry entries",
        )
        _append_check(
            checks,
            "safety_boundary_safe",
            policy.safety.model_dump(mode="json") == BASELINE_REVIEW_SAFETY,
            "production_effect=none; broker_action=none; manual_review_required=true",
        )
    failed = [check for check in checks if check["status"] == "FAIL"]
    payload = {
        "schema_version": BASELINE_REVIEW_VALIDATION_SCHEMA_VERSION,
        "report_type": BASELINE_REVIEW_VALIDATION_REPORT_TYPE,
        "validation_id": _stable_id("baseline-review-validation", run_date),
        "as_of_date": run_date.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "FAIL" if failed else "PASS",
        "checks": checks,
        "failed_check_count": len(failed),
        "warning_check_count": len([check for check in checks if check["status"] == "WARNING"]),
        "safety": dict(BASELINE_REVIEW_SAFETY),
        "commands_executed": False,
        "production_state_mutated": False,
        **BASELINE_REVIEW_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def render_baseline_review_package_markdown(payload: Mapping[str, Any]) -> str:
    lines = _markdown_header(
        "ETF Baseline Candidate Review Package",
        payload.get("candidate_id"),
        payload.get("as_of_date"),
    )
    eligibility = _mapping(payload.get("eligibility"))
    summary = _mapping(payload.get("review_summary"))
    lines.extend(
        [
            "## Review Summary / 复核摘要",
            "",
            f"- Eligibility Status: `{_text(eligibility.get('eligibility_status'), 'UNKNOWN')}`",
            f"- Candidate Type: `{_text(payload.get('candidate_type'), 'UNKNOWN')}`",
            f"- Eligible Count: `{summary.get('eligible_count', 0)}`",
            f"- Needs More Data Count: `{summary.get('needs_more_data_count', 0)}`",
            f"- Blocked Count: `{summary.get('blocked_count', 0)}`",
            "",
            "## Evidence Requirement Matrix / 证据要求矩阵",
            "",
            "| Evidence | Required | Status | Freshness | Sample | Blocking | Source | Notes |",
            "|---|---|---|---|---:|---|---|---|",
        ]
    )
    for row in _records(_mapping(payload.get("evidence_requirement_matrix")).get("rows")):
        lines.append(
            f"| `{row.get('evidence_id')}` | `{str(row.get('required')).lower()}` | "
            f"`{row.get('status')}` | `{_escape_md(row.get('freshness'))}` | "
            f"{row.get('sample_count')} | `{str(row.get('blocking')).lower()}` | "
            f"`{_escape_md(row.get('source_report'))}` | {_escape_md(row.get('notes'))} |"
        )
    lines.extend(_findings_markdown("Blockers / 阻断项", _records(payload.get("blockers"))))
    lines.extend(_findings_markdown("Warnings / 警告", _records(payload.get("warnings"))))
    lines.extend(
        [
            "## Owner Review Checklist / Owner 人工复核清单",
            "",
            "| Checklist | Required | Status | Prompt |",
            "|---|---|---|---|",
        ]
    )
    for item in _records(payload.get("owner_review_checklist")):
        lines.append(
            f"| `{item.get('checklist_id')}` | `{str(item.get('required')).lower()}` | "
            f"`{item.get('status')}` | {_escape_md(item.get('prompt'))} |"
        )
    lines.extend(
        [
            "",
            "## Decision Options / 决策选项",
            "",
            ", ".join(f"`{item}`" for item in _texts(payload.get("recommended_decision_options"))),
            "",
            "## Source Report Links / Source Report Links",
            "",
        ]
    )
    for link in _texts(payload.get("source_report_links")):
        lines.append(f"- `{_escape_md(link)}`")
    lines.append("")
    return "\n".join(lines)


def render_owner_review_decision_markdown(payload: Mapping[str, Any]) -> str:
    lines = _markdown_header(
        "ETF Baseline Review Owner Decision",
        payload.get("candidate_id"),
        payload.get("created_at"),
    )
    lines.extend(
        [
            "## Decision / 决策",
            "",
            f"- Decision ID: `{_text(payload.get('decision_id'))}`",
            f"- Review Package: `{_text(payload.get('review_package_id'))}`",
            f"- Owner Decision: `{_text(payload.get('owner_decision'))}`",
            f"- Confidence: `{payload.get('confidence')}`",
            f"- Rationale: {_escape_md(payload.get('rationale'))}",
            f"- Conditions: {', '.join(_texts(payload.get('conditions'))) or 'none'}",
            f"- Follow-up Tasks: {', '.join(_texts(payload.get('follow_up_tasks'))) or 'none'}",
            "",
        ]
    )
    return "\n".join(lines)


def render_baseline_change_proposal_markdown(payload: Mapping[str, Any]) -> str:
    lines = _markdown_header(
        "ETF Baseline Change Proposal Draft",
        payload.get("candidate_id"),
        payload.get("created_at"),
    )
    lines.extend(
        [
            "## Proposal Draft / 草案",
            "",
            f"- Proposal ID: `{_text(payload.get('proposal_id'))}`",
            f"- Review Package: `{_text(payload.get('review_package_id'))}`",
            f"- Owner Decision: `{_text(payload.get('owner_decision_id'))}`",
            "- Current Baseline Config Hash: "
            f"`{_text(payload.get('current_baseline_config_hash'))}`",
            f"- Candidate Config Hash: `{_text(payload.get('candidate_config_hash'))}`",
            "- Proposal is draft only: `true`",
            "- Baseline config mutated: `false`",
            "- Target weights mutated: `false`",
            "",
        ]
    )
    return "\n".join(lines)


def render_baseline_review_outcome_markdown(payload: Mapping[str, Any]) -> str:
    lines = _markdown_header(
        "ETF Baseline Review Outcome",
        payload.get("candidate_id"),
        payload.get("created_at"),
    )
    lines.extend(
        [
            "## Outcome / 结果追踪",
            "",
            f"- Latest Review Status: `{_text(payload.get('latest_review_status'))}`",
            f"- Latest Decision ID: `{_text(payload.get('latest_decision_id'))}`",
            f"- Latest Proposal ID: `{_text(payload.get('latest_proposal_id'))}`",
            f"- Next Review Due: `{_text(payload.get('next_review_due'))}`",
            "",
        ]
    )
    return "\n".join(lines)


def render_baseline_review_validation_markdown(payload: Mapping[str, Any]) -> str:
    lines = _markdown_header(
        "ETF Baseline Review Playbook Validation Gate",
        payload.get("validation_id"),
        payload.get("as_of_date"),
    )
    lines.extend(
        [
            "## Status / 状态",
            "",
            f"- Status: `{_text(payload.get('status'), 'UNKNOWN')}`",
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
    lines.append("")
    return "\n".join(lines)


def write_baseline_review_package(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> dict[str, Path]:
    _write_json(payload, json_path)
    _write_text(render_baseline_review_package_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def write_owner_review_decision(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> dict[str, Path]:
    _write_json(payload, json_path)
    _write_text(render_owner_review_decision_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def write_baseline_change_proposal_draft(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> dict[str, Path]:
    _write_json(payload, json_path)
    _write_text(render_baseline_change_proposal_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def write_baseline_review_outcome(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> dict[str, Path]:
    _write_json(payload, json_path)
    _write_text(render_baseline_review_outcome_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def write_baseline_review_validation_report(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> dict[str, Path]:
    _write_json(payload, json_path)
    _write_text(render_baseline_review_validation_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


class _CandidateContext:
    def __init__(
        self,
        *,
        candidate_id: str,
        candidate_type: str,
        records: list[dict[str, Any]],
        source_paths: list[str],
    ) -> None:
        self.candidate_id = candidate_id
        self.candidate_type = candidate_type
        self.records = records
        self.source_paths = source_paths

    @property
    def exists(self) -> bool:
        return bool(self.records)

    def metric(self, *keys: str) -> float | None:
        for record in self.records:
            for key in keys:
                value = _first_nested_value(record, key)
                parsed = _float_or_none(value)
                if parsed is not None:
                    return parsed
        return None

    def metric_from_source(self, source_hint: str, *keys: str) -> float | None:
        for path, record in zip(self.source_paths, self.records, strict=False):
            if source_hint not in path:
                continue
            for key in keys:
                value = _first_nested_value(record, key)
                parsed = _float_or_none(value)
                if parsed is not None:
                    return parsed
        return self.metric(*keys)

    def count_matching_source(self, source_hint: str) -> int:
        return sum(
            1
            for path, record in zip(self.source_paths, self.records, strict=False)
            if source_hint in path or source_hint in json.dumps(record, sort_keys=True)
        )

    def weights(self) -> dict[str, float]:
        for record in self.records:
            weights = _mapping(_first_nested_value(record, "weights"))
            if weights:
                return {
                    str(symbol): float(value)
                    for symbol, value in weights.items()
                    if _float_or_none(value) is not None
                }
        return {}

    def to_payload(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "candidate_type": self.candidate_type,
            "candidate_exists": self.exists,
            "record_count": len(self.records),
            "source_paths": self.source_paths,
            "forward_days": self.metric("forward_days", "observation_days", "sample_count"),
            "turnover_delta": self.metric("turnover_delta", "turnover_since_enrollment"),
            "drawdown_delta": self.metric("drawdown_delta", "max_drawdown_delta"),
        }


def _resolve_strategy_dashboard(
    *,
    as_of: date,
    strategy_dashboard: Mapping[str, Any] | StrategyEvidenceDashboard | None,
    report_index: Mapping[str, Any] | None,
    report_index_path: Path | None,
    report_registry_path: Path,
    root_path: Path,
) -> StrategyEvidenceDashboard:
    if isinstance(strategy_dashboard, StrategyEvidenceDashboard):
        return strategy_dashboard
    if isinstance(strategy_dashboard, Mapping):
        return StrategyEvidenceDashboard.model_validate(strategy_dashboard)
    index_payload = _resolve_report_index(
        as_of=as_of,
        report_index=report_index,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        root_path=root_path,
    )
    dashboard_path = _report_index_artifact_path(
        index_payload,
        "etf_strategy_evidence_dashboard",
    )
    dashboard_payload = _read_json_object(dashboard_path)
    if dashboard_payload:
        return StrategyEvidenceDashboard.model_validate(dashboard_payload)
    return build_strategy_evidence_dashboard(
        as_of=as_of,
        report_index=index_payload,
        report_registry_path=report_registry_path,
        root_path=root_path,
    )


def _matrix_row(
    *,
    evidence_id: EvidenceRequirementId,
    requirement: BaselineReviewEvidenceRequirement,
    policy: BaselineReviewPolicyConfig,
    dashboard: StrategyEvidenceDashboard,
    cards: Mapping[str, Mapping[str, Any]],
    source_reports: Sequence[StrategyEvidenceSourceReport],
    context: _CandidateContext,
    as_of: date,
) -> BaselineReviewEvidenceMatrixRow:
    source = _source_for_requirement(requirement, source_reports)
    card = cards.get(requirement.source_category, {})
    source_path = (
        source.source_report_path
        if source is not None
        else f"config/report_registry.yaml#{requirement.source_report_id}"
    )
    sample_count = max(
        0 if source is None else source.sample_count,
        _int_or_default(card.get("sample_count"), 0),
    )
    status = _base_row_status(source=source, card=card, requirement=requirement)
    notes = _row_note(status=status, source=source, card=card, requirement=requirement)
    if evidence_id == "forward_performance":
        forward_days = context.metric_from_source(
            requirement.source_report_id,
            "forward_days",
            "observation_days",
            "available_forward_days",
            "sample_count",
        )
        sample_count = max(sample_count, int(forward_days or 0))
        if sample_count < policy.eligibility_thresholds.minimum_forward_days:
            status = "needs_more_data"
            notes = (
                "forward_days below policy minimum: "
                f"{sample_count}/{policy.eligibility_thresholds.minimum_forward_days}"
            )
    elif evidence_id == "drawdown_control":
        drawdown_delta = context.metric_from_source(
            requirement.source_report_id,
            "drawdown_delta",
            "max_drawdown_delta",
            "candidate_drawdown_delta",
        )
        if (
            drawdown_delta is not None
            and drawdown_delta > policy.eligibility_thresholds.max_allowed_drawdown_delta
        ):
            status = "blocked"
            notes = (
                "drawdown delta exceeds policy: "
                f"{drawdown_delta}>{policy.eligibility_thresholds.max_allowed_drawdown_delta}"
            )
    elif evidence_id == "turnover_control":
        turnover_delta = context.metric_from_source(
            requirement.source_report_id,
            "turnover_delta",
            "turnover_since_enrollment",
            "candidate_turnover_delta",
        )
        if (
            turnover_delta is not None
            and turnover_delta > policy.eligibility_thresholds.max_allowed_turnover_delta
        ):
            status = "blocked"
            notes = (
                "turnover delta exceeds policy: "
                f"{turnover_delta}>{policy.eligibility_thresholds.max_allowed_turnover_delta}"
            )
    if requirement.required and sample_count < requirement.minimum_sample_count:
        status = "needs_more_data"
        notes = (
            "sample_count below evidence requirement: "
            f"{sample_count}/{requirement.minimum_sample_count}"
        )
    blocking = requirement.required and status in {"missing", "stale", "blocked", "needs_more_data"}
    if status == "optional_missing":
        blocking = False
    return BaselineReviewEvidenceMatrixRow(
        evidence_id=evidence_id,
        required=requirement.required,
        status=status,
        source_report=source_path,
        latest_as_of_date=as_of.isoformat(),
        freshness="MISSING" if source is None else source.freshness_status,
        sample_count=sample_count,
        blocking=blocking,
        notes=notes,
    )


def _eligibility_findings(
    *,
    candidate_id: str,
    dashboard: StrategyEvidenceDashboard,
    matrix: BaselineReviewEvidenceMatrix,
    context: _CandidateContext,
    source_payloads: Sequence[tuple[str, dict[str, Any]]],
    policy: BaselineReviewPolicyConfig,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    blockers: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    if not context.exists:
        blockers.append(_blocker("CANDIDATE_NOT_FOUND", policy, f"{candidate_id} not found"))
    normalized_candidate_type = _normalize_candidate_type(context.candidate_type)
    if context.exists and normalized_candidate_type not in set(policy.candidate_types):
        blockers.append(_blocker("UNSUPPORTED_CANDIDATE_TYPE", policy, normalized_candidate_type))
    elif matrix.candidate_type not in set(policy.candidate_types):
        blockers.append(_blocker("UNSUPPORTED_CANDIDATE_TYPE", policy, matrix.candidate_type))
    if (
        policy.eligibility_thresholds.require_evidence_dashboard_not_blocked
        and dashboard.overall_status in {"blocked", "invalid"}
    ):
        blockers.append(
            _blocker(
                "EVIDENCE_DASHBOARD_BLOCKED",
                policy,
                f"overall_status={dashboard.overall_status}",
            )
        )
    for row in matrix.rows:
        if row.blocking:
            blockers.append(
                _blocker(
                    _blocker_id_for_row(row),
                    policy,
                    row.notes,
                    evidence_id=row.evidence_id,
                    source_report=row.source_report,
                )
            )
        elif row.status in {"warning", "optional_missing"}:
            warnings.append(
                _warning(
                    f"{row.evidence_id.upper()}_{row.status.upper()}",
                    row.notes,
                    evidence_id=row.evidence_id,
                    source_report=row.source_report,
                )
            )
    forward_days = context.metric_from_source(
        "etf_forward_dashboard",
        "forward_days",
        "observation_days",
        "available_forward_days",
        "sample_count",
    )
    if (
        forward_days is not None
        and forward_days < policy.eligibility_thresholds.minimum_forward_days
    ):
        blockers.append(
            _blocker(
                "FORWARD_SAMPLE_TOO_SMALL",
                policy,
                f"forward_days={forward_days}",
            )
        )
    drawdown_delta = context.metric(
        "drawdown_delta",
        "max_drawdown_delta",
        "candidate_drawdown_delta",
    )
    if (
        drawdown_delta is not None
        and drawdown_delta > policy.eligibility_thresholds.max_allowed_drawdown_delta
    ):
        blockers.append(_blocker("HIGH_DRAWDOWN", policy, f"drawdown_delta={drawdown_delta}"))
    turnover_delta = context.metric(
        "turnover_delta",
        "turnover_since_enrollment",
        "candidate_turnover_delta",
    )
    if (
        turnover_delta is not None
        and turnover_delta > policy.eligibility_thresholds.max_allowed_turnover_delta
    ):
        blockers.append(_blocker("HIGH_TURNOVER", policy, f"turnover_delta={turnover_delta}"))
    if policy.eligibility_thresholds.require_decision_journal_link and not _has_journal_link(
        candidate_id,
        source_payloads,
    ):
        blockers.append(_blocker("NO_DECISION_JOURNAL_LINK", policy, candidate_id))
    for source_path, payload in source_payloads:
        blockers.extend(_safety_blockers(payload, policy, source_path=source_path))
    return _dedupe_findings(blockers), _dedupe_findings(warnings)


def _eligibility_status(blockers: Sequence[Mapping[str, Any]]) -> EligibilityStatus:
    if not blockers:
        return "eligible_for_owner_review"
    blocker_ids = {_text(item.get("blocker_id")) for item in blockers}
    if blocker_ids <= {"FORWARD_SAMPLE_TOO_SMALL", "REQUIRED_EVIDENCE_MISSING"}:
        return "needs_more_data"
    if blocker_ids & {"HIGH_DRAWDOWN", "HIGH_TURNOVER"} and not blocker_ids & CRITICAL_BLOCKERS:
        return "rejected_by_policy"
    return "blocked"


def _source_reports(dashboard: StrategyEvidenceDashboard) -> list[StrategyEvidenceSourceReport]:
    return [
        StrategyEvidenceSourceReport.model_validate(item)
        for item in dashboard.model_dump(mode="json").get("source_reports", [])
    ]


def _cards(dashboard: StrategyEvidenceDashboard) -> dict[str, dict[str, Any]]:
    return {
        _text(card.get("category")): dict(card)
        for card in dashboard.model_dump(mode="json").get("evidence_cards", [])
        if isinstance(card, Mapping)
    }


def _source_payloads(
    source_reports: Sequence[StrategyEvidenceSourceReport],
) -> list[tuple[str, dict[str, Any]]]:
    payloads = []
    for source in source_reports:
        path = Path(source.source_report_path)
        payload = _read_json_object(path)
        if payload:
            payloads.append((source.source_report_path, payload))
    return payloads


def _candidate_context(
    candidate_id: str,
    dashboard: StrategyEvidenceDashboard,
    payloads: Sequence[tuple[str, dict[str, Any]]],
) -> _CandidateContext:
    records: list[dict[str, Any]] = []
    source_paths: list[str] = []
    candidate_type = ""
    for ranking in dashboard.model_dump(mode="json").get("candidate_rankings", []):
        if not isinstance(ranking, Mapping):
            continue
        if _text(ranking.get("candidate_id")) == candidate_id:
            records.append(dict(ranking))
            source_paths.append("strategy_evidence_dashboard:candidate_rankings")
            candidate_type = _normalize_candidate_type(_text(ranking.get("candidate_type")))
    for source_path, payload in payloads:
        for record in _find_candidate_records(payload, candidate_id):
            records.append(record)
            source_paths.append(source_path)
            if not candidate_type:
                candidate_type = _normalize_candidate_type(
                    _text(record.get("candidate_type"), _text(record.get("proposal_type")))
                )
    if not candidate_type:
        candidate_type = _infer_candidate_type(candidate_id, records)
    return _CandidateContext(
        candidate_id=candidate_id,
        candidate_type=candidate_type,
        records=records,
        source_paths=source_paths,
    )


def _find_candidate_records(value: Any, candidate_id: str) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    if isinstance(value, Mapping):
        candidate_keys = (
            "candidate_id",
            "weight_set_id",
            "source_candidate_id",
            "linked_candidate",
            "proposal_candidate_id",
        )
        if any(_text(value.get(key)) == candidate_id for key in candidate_keys):
            found.append(dict(value))
        for child in value.values():
            found.extend(_find_candidate_records(child, candidate_id))
    elif isinstance(value, list):
        for child in value:
            found.extend(_find_candidate_records(child, candidate_id))
    return found


def _supported_candidate_type(
    candidate_type: str,
    policy: BaselineReviewPolicyConfig,
) -> CandidateType:
    normalized = _normalize_candidate_type(candidate_type)
    if normalized in set(policy.candidate_types):
        return normalized  # type: ignore[return-value]
    return policy.candidate_types[0]


def _normalize_candidate_type(value: str) -> str:
    mapping = {
        "parameter_review_proposal": "parameter_review_candidate",
        "satellite_replacement_candidate": "satellite_adjusted_candidate",
        "satellite_candidate": "satellite_adjusted_candidate",
        "ai_overlay_candidate": "AI_overlay_candidate",
        "continue_forward_observation": "weight_calibration_candidate",
        "defer_until_more_forward_data": "weight_calibration_candidate",
        "propose_extended_shadow": "weight_calibration_candidate",
        "propose_manual_baseline_review": "weight_calibration_candidate",
        "reject_weight_set": "weight_calibration_candidate",
    }
    text = _text(value)
    return mapping.get(text, text)


def _infer_candidate_type(candidate_id: str, records: Sequence[Mapping[str, Any]]) -> str:
    text = candidate_id.lower()
    if "parameter" in text:
        return "parameter_review_candidate"
    if "forward" in text or any("forward" in json.dumps(record).lower() for record in records):
        return "forward_shadow_candidate"
    if "ai" in text:
        return "AI_overlay_candidate"
    if "satellite" in text:
        return "satellite_adjusted_candidate"
    return "weight_calibration_candidate"


def _source_for_requirement(
    requirement: BaselineReviewEvidenceRequirement,
    source_reports: Sequence[StrategyEvidenceSourceReport],
) -> StrategyEvidenceSourceReport | None:
    for source in source_reports:
        if (
            source.category == requirement.source_category
            or source.report_id == requirement.source_report_id
        ):
            return source
    return None


def _base_row_status(
    *,
    source: StrategyEvidenceSourceReport | None,
    card: Mapping[str, Any],
    requirement: BaselineReviewEvidenceRequirement,
) -> EvidenceRowStatus:
    if source is None:
        return "missing" if requirement.required else "optional_missing"
    if source.load_status == "missing":
        return "missing" if source.required else "optional_missing"
    if source.load_status == "optional_missing":
        return "optional_missing"
    if source.load_status == "stale":
        return "stale"
    if source.load_status == "blocked" or _is_blocked_status(source.artifact_status):
        return "blocked"
    card_status = _text(card.get("status"))
    if card_status in {"blocked", "invalid"}:
        return "blocked"
    if card_status == "stale":
        return "stale"
    if card_status == "needs_more_data":
        return "needs_more_data"
    if card_status in {"mixed", "weak"}:
        return "warning"
    return "satisfied"


def _row_note(
    *,
    status: EvidenceRowStatus,
    source: StrategyEvidenceSourceReport | None,
    card: Mapping[str, Any],
    requirement: BaselineReviewEvidenceRequirement,
) -> str:
    if source is None:
        return f"{requirement.title}: source report not available"
    card_status = _text(card.get("status"), "unknown")
    return (
        f"{requirement.title}: status={status}; card_status={card_status}; "
        f"load_status={source.load_status}; artifact_status={source.artifact_status}; "
        f"sample_count={source.sample_count}"
    )


def _blocker_id_for_row(row: BaselineReviewEvidenceMatrixRow) -> str:
    if row.status == "needs_more_data":
        if row.evidence_id in {"forward_performance", "drawdown_control"}:
            return "FORWARD_SAMPLE_TOO_SMALL"
        return "REQUIRED_EVIDENCE_MISSING"
    if row.status in {"missing", "stale"}:
        if row.evidence_id == "validation_gates":
            return "VALIDATION_GATE_STALE"
        if row.evidence_id == "decision_journal":
            return "NO_DECISION_JOURNAL_LINK"
        return "REQUIRED_EVIDENCE_MISSING"
    if row.evidence_id == "data_quality":
        return "DATA_QUALITY_CRITICAL"
    if row.evidence_id == "ops_health":
        return "OPS_VALIDATION_FAILED"
    if row.evidence_id == "validation_gates":
        return "VALIDATION_GATE_STALE"
    if row.evidence_id == "decision_journal":
        return "NO_DECISION_JOURNAL_LINK"
    if row.evidence_id == "parameter_review":
        return "PARAMETER_REVIEW_BLOCKED"
    if row.evidence_id == "forward_performance":
        return "FORWARD_SAMPLE_TOO_SMALL"
    if row.evidence_id == "drawdown_control":
        return "HIGH_DRAWDOWN"
    if row.evidence_id == "turnover_control":
        return "HIGH_TURNOVER"
    return "REQUIRED_EVIDENCE_MISSING"


def _blocker(
    blocker_id: str,
    policy: BaselineReviewPolicyConfig,
    reason: str,
    **evidence: Any,
) -> dict[str, Any]:
    condition = policy.blocking_conditions.get(blocker_id)
    severity = "blocking" if condition is None else condition.severity
    return {
        "blocker_id": blocker_id,
        "severity": severity,
        "blocking": True,
        "reason": reason,
        "description": "" if condition is None else condition.description,
        "evidence": evidence,
    }


def _warning(warning_id: str, reason: str, **evidence: Any) -> dict[str, Any]:
    return {
        "warning_id": warning_id,
        "severity": "warning",
        "blocking": False,
        "reason": reason,
        "evidence": evidence,
    }


def _dedupe_findings(findings: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    result = []
    for finding in findings:
        key = (
            _text(finding.get("blocker_id"), _text(finding.get("warning_id"))),
            _text(finding.get("reason")),
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(dict(finding))
    return result


def _has_journal_link(
    candidate_id: str,
    source_payloads: Sequence[tuple[str, dict[str, Any]]],
) -> bool:
    for path, payload in source_payloads:
        if "decision_journal" not in path:
            continue
        if _find_candidate_records(payload, candidate_id):
            return True
    return False


def _safety_blockers(
    payload: Mapping[str, Any],
    policy: BaselineReviewPolicyConfig,
    *,
    source_path: str,
) -> list[dict[str, Any]]:
    blockers = []
    production_effect = _text(
        payload.get("production_effect"),
        _text(_mapping(payload.get("safety")).get("production_effect")),
    )
    broker_action = _text(
        payload.get("broker_action"),
        _text(_mapping(payload.get("safety")).get("broker_action")),
    )
    if production_effect and production_effect != "none":
        blockers.append(
            _blocker("UNSAFE_PRODUCTION_EFFECT", policy, production_effect, source_path=source_path)
        )
    if broker_action and broker_action != "none":
        blockers.append(
            _blocker("BROKER_ACTION_NOT_NONE", policy, broker_action, source_path=source_path)
        )
    return blockers


def _current_baseline_summary() -> dict[str, Any]:
    try:
        config = load_etf_config_bundle()
        weights = {
            symbol: float(asset.default_weight)
            for symbol, asset in config.assets.assets.items()
            if asset.default_weight > 0
        }
        return {
            "model_version": config.strategy.model.version,
            "config_hash": config.config_hash,
            "default_weights": weights,
            "source": "config/etf_portfolio/assets.yaml",
        }
    except Exception:  # noqa: BLE001 - report remains auditable without config bundle.
        return {
            "model_version": "unknown",
            "config_hash": "unknown",
            "default_weights": {},
            "source": "config/etf_portfolio/assets.yaml",
        }


def _candidate_allocation_summary(context: _CandidateContext) -> dict[str, Any]:
    weights = context.weights()
    return {
        "candidate_id": context.candidate_id,
        "candidate_type": context.candidate_type,
        "weights": weights,
        "weight_sum": round(sum(weights.values()), 10) if weights else None,
        "source_record_count": len(context.records),
        "source_paths": context.source_paths,
    }


def _section_from_matrix(matrix: Mapping[str, Any], evidence_id: str) -> dict[str, Any]:
    for row in _records(matrix.get("rows")):
        if _text(row.get("evidence_id")) == evidence_id:
            return dict(row)
    return {
        "evidence_id": evidence_id,
        "status": "missing",
        "source_report": "",
        "notes": "missing from evidence matrix",
    }


def _journal_status_for_owner_decision(owner_decision: str) -> str:
    return {
        "approve_for_proposal_draft": "accept_recommendation",
        "continue_shadow": "continue_observation",
        "needs_more_data": "request_more_data",
        "reject_candidate": "reject_recommendation",
        "defer_review": "defer_decision",
        "request_new_experiment": "start_new_experiment",
    }[owner_decision]


def _outcome_status_for(
    *,
    decision: Mapping[str, Any] | None,
    proposal: Mapping[str, Any] | None,
) -> OutcomeStatus:
    if proposal is not None:
        return "proposal_drafted"
    if decision is None:
        return "review_pending"
    return {
        "approve_for_proposal_draft": "review_pending",
        "continue_shadow": "continue_shadow",
        "needs_more_data": "needs_more_data",
        "reject_candidate": "rejected",
        "defer_review": "deferred",
        "request_new_experiment": "deferred",
    }.get(
        _text(decision.get("owner_decision")), "review_pending"
    )  # type: ignore[return-value]


def _workflow_probe(
    policy: BaselineReviewPolicyConfig,
    run_date: date,
    generated: datetime,
) -> dict[str, bool]:
    try:
        with TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            report_index = _sample_report_index(temp, run_date)
            candidate_id = "candidate_weight_set_003"
            eligibility = build_baseline_review_eligibility(
                candidate_id=candidate_id,
                as_of=run_date,
                config=policy,
                report_index=report_index,
                root_path=temp,
                generated_at=generated,
            )
            package = build_baseline_review_package(
                candidate_id=candidate_id,
                as_of=run_date,
                config=policy,
                report_index=report_index,
                root_path=temp,
                generated_at=generated,
            )
            package_path = temp / "baseline_review_package.json"
            _write_json(package, package_path)
            decision = build_owner_review_decision(
                review_package=package,
                owner_decision="approve_for_proposal_draft",
                rationale="Probe approval for proposal draft only.",
                confidence=0.8,
                config=policy,
                created_at=generated,
            )
            linked = link_baseline_review_decision_to_journal(
                decision,
                review_package_path=package_path,
                journal_path=temp / "journal.json",
                updated_at=generated,
            )
            proposal = build_baseline_change_proposal_draft(
                review_package=package,
                owner_decision=linked,
                config=policy,
                created_at=generated,
            )
            outcome = build_candidate_review_outcome(
                candidate_id=candidate_id,
                decision=linked,
                proposal=proposal,
                config=policy,
                created_at=generated,
            )
            unsafe_decision_blocked = False
            try:
                build_owner_review_decision(
                    review_package=package,
                    owner_decision="apply_to_production",
                    rationale="unsafe",
                    confidence=0.5,
                    config=policy,
                    created_at=generated,
                )
            except BaselineReviewError:
                unsafe_decision_blocked = True
            return {
                "eligibility_gate_available": eligibility["eligibility_status"]
                == "eligible_for_owner_review",
                "evidence_matrix_available": bool(
                    _records(_mapping(eligibility.get("evidence_requirement_matrix")).get("rows"))
                ),
                "review_package_generator_available": bool(package.get("source_report_links")),
                "owner_decision_capture_available": linked["decision_journal_linkage"]["status"]
                == "linked",
                "decision_journal_integration_available": (temp / "journal.json").exists(),
                "proposal_draft_generator_available": proposal["proposal_is_draft_only"] is True,
                "outcome_tracker_available": outcome["latest_review_status"] == "proposal_drafted",
                "automatic_production_promotion_blocked": unsafe_decision_blocked,
                "proposal_does_not_mutate_config": proposal["baseline_config_mutated"] is False
                and proposal["target_weights_mutated"] is False,
                "evidence_links_required": bool(package["source_report_links"]),
            }
    except Exception:
        return {
            "eligibility_gate_available": False,
            "evidence_matrix_available": False,
            "review_package_generator_available": False,
            "owner_decision_capture_available": False,
            "decision_journal_integration_available": False,
            "proposal_draft_generator_available": False,
            "outcome_tracker_available": False,
            "automatic_production_promotion_blocked": False,
            "proposal_does_not_mutate_config": False,
            "evidence_links_required": False,
        }


def _sample_report_index(temp: Path, run_date: date) -> dict[str, Any]:
    temp.mkdir(parents=True, exist_ok=True)
    strategy_config = load_strategy_evidence_dashboard_config(DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH)
    records: dict[str, dict[str, Any]] = {}
    for source_id, source in sorted(strategy_config.sources.items()):
        path = temp / f"{source.report_id}_{run_date.isoformat()}.json"
        path.write_text(
            json.dumps(
                _sample_source_payload(source_id, source.category),
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        records[source.report_id] = _sample_report_record(source.report_id, path, run_date)
        if source.validation_report_id:
            validation_path = temp / f"{source.validation_report_id}_{run_date.isoformat()}.json"
            validation_path.write_text(
                json.dumps(
                    {
                        "report_type": source.validation_report_id,
                        "status": "PASS",
                        "data_quality_status": "PASS",
                        "sample_count": 1,
                        "production_effect": "none",
                        "broker_action": "none",
                        "manual_review_required": True,
                        "safety": dict(STRATEGY_EVIDENCE_SAFETY),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            records[source.validation_report_id] = _sample_report_record(
                source.validation_report_id,
                validation_path,
                run_date,
            )
    return {"reports": list(records.values())}


def _sample_source_payload(source_id: str, category: str) -> dict[str, Any]:
    candidate_id = "candidate_weight_set_003"
    payload = {
        "report_type": source_id,
        "status": "supportive",
        "overall_status": "supportive",
        "evidence_status": "supportive",
        "sample_count": 25,
        "candidate_count": 1,
        "entry_count": 1,
        "manual_review_action_count": 2,
        "data_quality_status": "PASS",
        "validation_status": "PASS",
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "safety": dict(STRATEGY_EVIDENCE_SAFETY),
        "candidates": [
            {
                "candidate_id": candidate_id,
                "weight_set_id": candidate_id,
                "candidate_type": "weight_calibration_candidate",
                "status": "supportive",
                "forward_days": 25,
                "sample_count": 25,
                "turnover_delta": 0.08,
                "drawdown_delta": 0.01,
                "weights": {"SPY": 0.25, "QQQ": 0.35, "SMH": 0.25, "CASH": 0.15},
            }
        ],
        "entries": [
            {
                "decision_id": "decision-probe",
                "linked_candidate": candidate_id,
                "decision_status": "continue_observation",
                "confidence": 0.8,
            }
        ],
        "summary": f"{source_id} probe",
    }
    if category == "weight_calibration":
        payload["status"] = "strong_support"
        payload["proposal_summary"] = {"status": "strong_support", "candidate_count": 1}
    if category == "forward_simulation":
        payload["summary"] = {"status": "supportive", "sample_count": 25}
    if category == "data_quality":
        payload["status"] = "PASS"
    if category == "operations_health":
        payload["status"] = "PASS"
        payload["pipeline_status"] = "PASS"
    if category == "validation_gates":
        payload["status"] = "PASS"
        payload["failed_check_count"] = 0
    return payload


def _sample_report_record(report_id: str, path: Path, run_date: date) -> dict[str, Any]:
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "latest_artifact_name": path.name,
        "artifact_date": run_date.isoformat(),
        "freshness_status": "FRESH",
        "artifact_status": "PASS",
        "exists": True,
        "age_days": 0,
    }


def _registry_has_baseline_review(path: Path) -> bool:
    try:
        registry = load_report_registry(path)
    except Exception:
        return False
    reports = {_text(item.get("report_id")): item for item in _records(registry.get("reports"))}
    return all(
        reports.get(report_id, {}).get("include_in_reader_brief") is True
        for report_id in (
            BASELINE_REVIEW_PACKAGE_REGISTRY_ID,
            BASELINE_REVIEW_VALIDATION_REGISTRY_ID,
        )
    )


def _validation_check_summary(check_id: str, passed: bool) -> str:
    messages = {
        "eligibility_gate_available": "eligibility gate evaluates an eligible probe candidate",
        "evidence_matrix_available": "evidence matrix rows are generated",
        "review_package_generator_available": "review package generator preserves source links",
        "owner_decision_capture_available": "owner decision is captured and journal-linked",
        "decision_journal_integration_available": "decision journal linkage writes audit record",
        "proposal_draft_generator_available": "approved decision generates draft-only proposal",
        "outcome_tracker_available": "candidate review outcome tracker updates status",
        "automatic_production_promotion_blocked": "unsafe production decision is rejected",
        "proposal_does_not_mutate_config": "proposal draft does not mutate baseline config",
        "evidence_links_required": "review outputs retain source evidence links",
    }
    suffix = "available" if passed else "failed"
    return f"{messages.get(check_id, check_id)}: {suffix}"


def _resolve_report_index(
    *,
    as_of: date,
    report_index: Mapping[str, Any] | None,
    report_index_path: Path | None,
    report_registry_path: Path,
    root_path: Path,
) -> Mapping[str, Any]:
    if report_index is not None:
        return report_index
    if report_index_path is not None and report_index_path.exists():
        payload = _read_json_object(report_index_path)
        if payload:
            return payload
    return build_report_index_payload(
        as_of=as_of,
        project_root=root_path,
        registry_path=report_registry_path,
    )


def _report_index_artifact_path(
    report_index: Mapping[str, Any],
    report_id: str,
) -> Path | None:
    for report in _records(report_index.get("reports")):
        if _text(report.get("report_id")) == report_id:
            path = _text(report.get("latest_artifact_path"))
            return Path(path) if path else None
    return None


def _parse_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        raise BaselineReviewError("date is required")
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as exc:
        raise BaselineReviewError("date must use YYYY-MM-DD") from exc


def _coerce_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _is_blocked_status(value: object) -> bool:
    text = _text(value).upper()
    return text in {"FAIL", "FAILED", "BLOCKED", "CRITICAL", "INVALID"} or text.startswith("FAIL")


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
        raise BaselineReviewError("baseline review output safety boundary is unsafe")
    if _contains_disallowed_output(payload):
        raise BaselineReviewError("baseline review output contains disallowed production action")


def _contains_disallowed_output(value: object) -> bool:
    disallowed_keys = {
        "apply_baseline_change",
        "overwrite_config",
        "promote_candidate_to_production",
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


def _hash_json(value: Mapping[str, Any]) -> str:
    return sha256(json.dumps(value, sort_keys=True).encode("utf-8")).hexdigest()


def _stable_id(prefix: str, *parts: object) -> str:
    digest = sha256(
        "|".join(
            part.isoformat() if isinstance(part, (date, datetime)) else str(part) for part in parts
        ).encode("utf-8")
    ).hexdigest()[:12]
    return f"{prefix}:{digest}"


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


def _findings_markdown(title: str, findings: Sequence[Mapping[str, Any]]) -> list[str]:
    lines = ["", f"## {title}", "", "| ID | Severity | Reason |", "|---|---|---|"]
    if not findings:
        lines.append("| none | none | none |")
    for finding in findings:
        finding_id = _text(finding.get("blocker_id"), _text(finding.get("warning_id")))
        lines.append(
            f"| `{_escape_md(finding_id)}` | `{_escape_md(finding.get('severity'))}` | "
            f"{_escape_md(finding.get('reason'))} |"
        )
    lines.append("")
    return lines


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


def _json_candidate_paths(path: Path | None) -> list[Path]:
    if path is None:
        return []
    suffix = path.suffix.lower()
    if suffix == ".json":
        return [path]
    if suffix in JSON_SIDECAR_SUFFIXES:
        return [path.with_suffix(".json")]
    return []


def _read_json_object(path: Path | None) -> dict[str, Any]:
    candidate_paths = _json_candidate_paths(path)
    if not candidate_paths:
        return {}
    for candidate_path in candidate_paths:
        if not candidate_path.exists():
            continue
        try:
            payload = json.loads(candidate_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, Mapping):
            return dict(payload)
    return {}


def _write_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_text(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _texts(value: object) -> list[str]:
    if isinstance(value, list):
        return [_text(item) for item in value if _text(item)]
    if isinstance(value, tuple):
        return [_text(item) for item in value if _text(item)]
    text = _text(value)
    return [text] if text else []


def _text(value: object, default: str = "") -> str:
    if value is None or value == "":
        return default
    text = str(value).strip()
    return text or default


def _int_or_default(value: object, default: int) -> int:
    parsed = _float_or_none(value)
    return default if parsed is None else int(parsed)


def _float_or_none(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _first_nested_value(value: object, key: str) -> object:
    if isinstance(value, Mapping):
        if key in value:
            return value[key]
        for child in value.values():
            found = _first_nested_value(child, key)
            if found not in (None, ""):
                return found
    elif isinstance(value, list):
        for child in value:
            found = _first_nested_value(child, key)
            if found not in (None, ""):
                return found
    return None


def _unique_strings(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = _text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _escape_md(value: object) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")
