from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime
from glob import glob
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal, Self, cast

import yaml
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.models import PolicyMetadata
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_ETF_OPERATIONS_SCHEDULE_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "operations_schedule.yaml"
)

OPERATIONS_SCHEDULE_SCHEMA_VERSION = "etf_operations_schedule_v1"
OPERATIONS_SCHEDULE_SAFETY = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
}

OperationsCadence = Literal["daily", "weekly", "biweekly", "monthly", "incident"]
OperationsFailurePolicy = Literal[
    "continue",
    "continue_with_warning",
    "skip_optional_step",
    "block_dependent_steps",
    "fail_pipeline",
    "manual_review_required",
]
OperationsFailureSeverity = Literal["info", "warning", "error", "critical"]
OperationsFailureEventType = Literal[
    "artifact_missing",
    "artifact_stale",
    "artifact_unknown",
    "dependency_blocked",
]
OperationsPipelineStatus = Literal[
    "pass",
    "warning",
    "manual_review_required",
    "blocked",
]
OperationsOwnerReviewCadence = Literal["daily", "weekly", "monthly", "incident"]
OperationsOwnerChecklistStatus = Literal[
    "ready",
    "manual_review_required",
    "blocked",
]
OperationsOwnerChecklistItemCategory = Literal[
    "safety_boundary",
    "cadence_gate",
    "summary_review",
    "blocking_event",
    "warning_event",
    "manual_review_event",
    "owner_signoff",
]
OperationsSchedulerDryRunStatus = Literal[
    "ready",
    "warning",
    "manual_review_required",
    "blocked",
]
OperationsHealthReportStatus = Literal[
    "pass",
    "warning",
    "manual_review_required",
    "blocked",
]
OperationsValidationStatus = Literal["PASS", "FAIL"]
OperationsValidationCheckStatus = Literal["PASS", "FAIL", "WARNING"]
OperationsDryRunStepStatus = Literal[
    "planned",
    "warning",
    "manual_review_required",
    "blocked",
]
OperationsRuntimeClass = Literal["fast", "medium", "slow"]
OperationsGraphCadence = Literal["daily", "weekly", "biweekly", "monthly"]
OperationsArtifactFreshnessStatus = Literal[
    "fresh",
    "stale",
    "missing",
    "not_applicable",
    "unknown",
]
OperationsArtifactDependencyStatus = Literal["blocking", "warning", "optional"]

_PIPELINE_FIELDS = (
    "daily_pipeline",
    "weekly_pipeline",
    "biweekly_pipeline",
    "monthly_pipeline",
    "manual_review_steps",
)
_PIPELINE_CADENCE = {
    "daily_pipeline": "daily",
    "weekly_pipeline": "weekly",
    "biweekly_pipeline": "biweekly",
    "monthly_pipeline": "monthly",
}
_GRAPH_PIPELINE_FIELD_BY_CADENCE: dict[OperationsGraphCadence, str] = {
    "daily": "daily_pipeline",
    "weekly": "weekly_pipeline",
    "biweekly": "biweekly_pipeline",
    "monthly": "monthly_pipeline",
}
OPERATIONS_COMMAND_GRAPH_SCHEMA_VERSION = "etf_operations_command_graph_v1"
OPERATIONS_ARTIFACT_FRESHNESS_SCHEMA_VERSION = "etf_operations_artifact_freshness_v1"
OPERATIONS_FAILURE_POLICY_SCHEMA_VERSION = "etf_operations_failure_policy_v1"
OPERATIONS_OWNER_REVIEW_CHECKLIST_SCHEMA_VERSION = "etf_operations_owner_review_checklist_v1"
OPERATIONS_SCHEDULER_DRY_RUN_SCHEMA_VERSION = "etf_operations_scheduler_dry_run_v1"
OPERATIONS_HEALTH_REPORT_SCHEMA_VERSION = "etf_operations_health_report_v1"
OPERATIONS_VALIDATION_SCHEMA_VERSION = "etf_operations_validation_v1"
OPERATIONS_VALIDATION_REPORT_TYPE = "etf_operations_validation"
OPERATIONS_VALIDATION_REPORT_ID = "etf_operations_validation"
OPERATIONS_HEALTH_REPORT_REGISTRY_ID = "etf_operations_health_report"
_ARTIFACT_PLACEHOLDER_RE = re.compile(r"\{[^}]+\}")
_ARTIFACT_DATE_RE = re.compile(r"(?P<date>\d{4}[-_]\d{2}[-_]\d{2})")
_TEXT_GENERATED_AT_RE = re.compile(
    r"(?:generated_at|generated at)\s*[:=]\s*(?P<value>[^\r\n]+)",
    re.IGNORECASE,
)
_TEXT_AS_OF_RE = re.compile(
    r"(?:as_of_date|as_of|as of|date)\s*[:=]\s*(?P<value>\d{4}[-_]\d{2}[-_]\d{2})",
    re.IGNORECASE,
)
REQUIRED_DAILY_OPERATION_NODE_IDS = frozenset(
    {
        "data_freshness_check",
        "etf_daily_run",
        "forward_update",
        "ai_confirmation_run",
        "satellite_replacement_run",
        "reader_brief_generate",
        "report_registry_update",
        "data_quality_governance_report",
        "operations_health_check",
    }
)
OPTIONAL_DAILY_OPERATION_NODE_IDS = frozenset(
    {
        "ai_attribution_update",
        "satellite_attribution_update",
    }
)
REQUIRED_WEEKLY_OPERATION_NODE_IDS = frozenset(
    {
        "weekly_review_aggregate",
        "weekly_review_generate",
        "forward_weekly_review",
        "decision_journal_review_prompt",
        "parameter_review_aggregate",
        "parameter_review_report",
        "watchlist_review",
        "operations_report",
        "reader_brief_weekly_navigation",
    }
)
OPTIONAL_WEEKLY_OPERATION_NODE_IDS = frozenset(
    {
        "parameter_review_aggregate",
        "parameter_review_report",
    }
)
REQUIRED_BIWEEKLY_OPERATION_NODE_IDS = frozenset(
    {
        "ai_attribution_scorecard_review",
        "satellite_attribution_scorecard_review",
        "weight_calibration_evidence_update",
        "operations_report_biweekly",
    }
)
REQUIRED_MONTHLY_OPERATION_NODE_IDS = frozenset(
    {
        "data_quality_audit",
        "weight_calibration_search",
        "weight_calibration_report",
        "parameter_review_governance",
        "strategy_evidence_dashboard_update",
        "operations_report_monthly",
    }
)

_DAILY_RUNTIME_CLASS_BY_STEP_ID: dict[str, OperationsRuntimeClass] = {
    "data_freshness_check": "medium",
    "etf_daily_run": "medium",
    "forward_update": "medium",
    "ai_confirmation_run": "medium",
    "satellite_replacement_run": "medium",
    "ai_attribution_update": "medium",
    "satellite_attribution_update": "medium",
    "report_registry_update": "fast",
    "data_quality_governance_report": "fast",
    "reader_brief_generate": "fast",
    "operations_health_check": "fast",
}
_WEEKLY_RUNTIME_CLASS_BY_STEP_ID: dict[str, OperationsRuntimeClass] = {
    "weekly_review_aggregate": "fast",
    "weekly_review_generate": "medium",
    "forward_weekly_review": "medium",
    "decision_journal_review_prompt": "fast",
    "parameter_review_aggregate": "medium",
    "parameter_review_report": "medium",
    "watchlist_review": "fast",
    "operations_report": "fast",
    "reader_brief_weekly_navigation": "fast",
}
_BIWEEKLY_RUNTIME_CLASS_BY_STEP_ID: dict[str, OperationsRuntimeClass] = {
    "ai_attribution_scorecard_review": "medium",
    "satellite_attribution_scorecard_review": "medium",
    "weight_calibration_evidence_update": "medium",
    "operations_report_biweekly": "fast",
}
_MONTHLY_RUNTIME_CLASS_BY_STEP_ID: dict[str, OperationsRuntimeClass] = {
    "data_quality_audit": "medium",
    "weight_calibration_search": "slow",
    "weight_calibration_report": "medium",
    "parameter_review_governance": "fast",
    "strategy_evidence_dashboard_update": "medium",
    "operations_report_monthly": "fast",
}
_REQUIRED_OPERATION_NODE_IDS_BY_CADENCE: dict[OperationsGraphCadence, frozenset[str]] = {
    "daily": REQUIRED_DAILY_OPERATION_NODE_IDS,
    "weekly": REQUIRED_WEEKLY_OPERATION_NODE_IDS,
    "biweekly": REQUIRED_BIWEEKLY_OPERATION_NODE_IDS,
    "monthly": REQUIRED_MONTHLY_OPERATION_NODE_IDS,
}
_OPTIONAL_OPERATION_NODE_IDS_BY_CADENCE: dict[OperationsGraphCadence, frozenset[str]] = {
    "daily": OPTIONAL_DAILY_OPERATION_NODE_IDS,
    "weekly": OPTIONAL_WEEKLY_OPERATION_NODE_IDS,
    "biweekly": frozenset(),
    "monthly": frozenset(),
}
_RUNTIME_CLASS_BY_CADENCE: dict[OperationsGraphCadence, dict[str, OperationsRuntimeClass]] = {
    "daily": _DAILY_RUNTIME_CLASS_BY_STEP_ID,
    "weekly": _WEEKLY_RUNTIME_CLASS_BY_STEP_ID,
    "biweekly": _BIWEEKLY_RUNTIME_CLASS_BY_STEP_ID,
    "monthly": _MONTHLY_RUNTIME_CLASS_BY_STEP_ID,
}


class OperationsScheduleError(ValueError):
    """Raised when the ETF operations schedule violates workflow requirements."""


class OperationsCommandGraphError(ValueError):
    """Raised when an operations command graph cannot be built safely."""


class ETFOperationsScheduleSafety(BaseModel):
    observe_only: bool
    candidate_only: bool
    production_effect: str = Field(min_length=1)
    broker_action: str = Field(min_length=1)
    manual_review_required: bool

    @model_validator(mode="after")
    def validate_safety_boundary(self) -> Self:
        for field, expected in OPERATIONS_SCHEDULE_SAFETY.items():
            if getattr(self, field) != expected:
                raise ValueError(f"ETF operations schedule safety {field} must be {expected!r}")
        return self


class ETFOperationsScheduleStep(BaseModel):
    step_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    description: str = Field(min_length=1)
    command: str = Field(min_length=1)
    cadence: OperationsCadence
    required: bool
    dependencies: list[str] = Field(default_factory=list)
    expected_outputs: list[str] = Field(default_factory=list)
    max_allowed_age: int = Field(ge=0)
    failure_policy: OperationsFailurePolicy
    owner_review_required: bool

    @model_validator(mode="after")
    def validate_step_fields(self) -> Self:
        self.dependencies = _normalized_unique_values(
            self.dependencies,
            field_name=f"{self.step_id}.dependencies",
        )
        self.expected_outputs = _normalized_unique_values(
            self.expected_outputs,
            field_name=f"{self.step_id}.expected_outputs",
        )
        if self.required and not self.expected_outputs:
            raise ValueError(f"{self.step_id}: required step must declare expected_outputs")
        if self.failure_policy == "manual_review_required" and not self.owner_review_required:
            raise ValueError(
                f"{self.step_id}: manual_review_required policy requires owner_review_required"
            )
        return self


class ETFOperationsScheduleConfig(BaseModel):
    schema_version: Literal["etf_operations_schedule_v1"]
    policy_metadata: PolicyMetadata
    safety: ETFOperationsScheduleSafety
    daily_pipeline: list[ETFOperationsScheduleStep] = Field(min_length=1)
    weekly_pipeline: list[ETFOperationsScheduleStep] = Field(min_length=1)
    biweekly_pipeline: list[ETFOperationsScheduleStep] = Field(min_length=1)
    monthly_pipeline: list[ETFOperationsScheduleStep] = Field(min_length=1)
    manual_review_steps: list[ETFOperationsScheduleStep] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_schedule(self) -> Self:
        step_ids: set[str] = set()
        duplicate_ids: set[str] = set()
        for step in self.steps():
            if step.step_id in step_ids:
                duplicate_ids.add(step.step_id)
            step_ids.add(step.step_id)
        if duplicate_ids:
            duplicates = ", ".join(sorted(duplicate_ids))
            raise ValueError(f"ETF operations schedule step IDs must be unique: {duplicates}")

        for field_name, cadence in _PIPELINE_CADENCE.items():
            for step in getattr(self, field_name):
                if step.cadence != cadence:
                    raise ValueError(
                        f"{step.step_id}: step cadence must be {cadence} in {field_name}"
                    )

        missing_dependencies: list[str] = []
        for step in self.steps():
            for dependency in step.dependencies:
                if dependency not in step_ids:
                    missing_dependencies.append(f"{step.step_id}->{dependency}")
        if missing_dependencies:
            missing = ", ".join(sorted(missing_dependencies))
            raise ValueError(
                "ETF operations schedule dependencies reference unknown steps: " f"{missing}"
            )

        return self

    def pipelines(self) -> dict[str, tuple[ETFOperationsScheduleStep, ...]]:
        return {field_name: tuple(getattr(self, field_name)) for field_name in _PIPELINE_FIELDS}

    def steps(self) -> tuple[ETFOperationsScheduleStep, ...]:
        return tuple(step for field_name in _PIPELINE_FIELDS for step in getattr(self, field_name))

    def step_by_id(self) -> dict[str, ETFOperationsScheduleStep]:
        return {step.step_id: step for step in self.steps()}

    def steps_for_cadence(
        self,
        cadence: OperationsCadence,
    ) -> tuple[ETFOperationsScheduleStep, ...]:
        return tuple(step for step in self.steps() if step.cadence == cadence)


class ETFOperationsCommandGraphNode(BaseModel):
    node_id: str = Field(min_length=1)
    command: str = Field(min_length=1)
    inputs: list[str] = Field(default_factory=list)
    outputs: list[str] = Field(default_factory=list)
    dependencies: list[str] = Field(default_factory=list)
    external_dependencies: list[str] = Field(default_factory=list)
    required: bool
    failure_policy: OperationsFailurePolicy
    estimated_runtime_class: OperationsRuntimeClass
    owner_review_required: bool
    safety: ETFOperationsScheduleSafety


class ETFOperationsCommandGraph(BaseModel):
    schema_version: Literal["etf_operations_command_graph_v1"] = (
        OPERATIONS_COMMAND_GRAPH_SCHEMA_VERSION
    )
    cadence: OperationsGraphCadence
    dry_run_only: bool = True
    commands_executed: bool = False
    execution_order: list[str] = Field(default_factory=list)
    skipped_optional_steps: list[str] = Field(default_factory=list)
    external_dependencies: list[str] = Field(default_factory=list)
    safety: ETFOperationsScheduleSafety
    nodes: list[ETFOperationsCommandGraphNode] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_graph_order(self) -> Self:
        node_ids = [node.node_id for node in self.nodes]
        if node_ids != self.execution_order:
            raise ValueError("ETF operations command graph nodes must match execution_order")
        seen: set[str] = set()
        for node in self.nodes:
            missing_dependencies = sorted(set(node.dependencies) - seen)
            if missing_dependencies:
                missing = ", ".join(missing_dependencies)
                raise ValueError(
                    f"ETF operations command graph node {node.node_id} is not "
                    f"topologically sorted; missing prior dependencies: {missing}"
                )
            seen.add(node.node_id)
        if self.commands_executed:
            raise ValueError("ETF operations command graph must not execute commands")
        for field, expected in OPERATIONS_SCHEDULE_SAFETY.items():
            if getattr(self.safety, field) != expected:
                raise ValueError(
                    f"ETF operations command graph safety {field} must be {expected!r}"
                )
        return self


class ETFOperationsArtifactStatus(BaseModel):
    artifact_id: str = Field(min_length=1)
    path: str = Field(min_length=1)
    artifact_type: str = Field(min_length=1)
    source_step: str = Field(min_length=1)
    as_of_date: date | None = None
    generated_at: datetime | None = None
    max_allowed_age: int = Field(ge=0)
    required: bool
    freshness_status: OperationsArtifactFreshnessStatus
    dependency_status: OperationsArtifactDependencyStatus
    dependency_steps: list[str] = Field(default_factory=list)
    blocking_dependencies: list[str] = Field(default_factory=list)
    age_days: int | None = None


class ETFOperationsArtifactFreshnessReport(BaseModel):
    schema_version: Literal["etf_operations_artifact_freshness_v1"] = (
        OPERATIONS_ARTIFACT_FRESHNESS_SCHEMA_VERSION
    )
    cadence: OperationsGraphCadence
    as_of_date: date
    checked_at: datetime
    read_only: bool = True
    commands_executed: bool = False
    safety: ETFOperationsScheduleSafety
    artifacts: list[ETFOperationsArtifactStatus] = Field(default_factory=list)
    blocking_artifacts: list[str] = Field(default_factory=list)
    warning_artifacts: list[str] = Field(default_factory=list)
    optional_artifacts: list[str] = Field(default_factory=list)
    freshness_summary: dict[str, int] = Field(default_factory=dict)
    dependency_summary: dict[str, int] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_report_safety(self) -> Self:
        if self.commands_executed:
            raise ValueError("ETF operations artifact freshness report must not execute commands")
        for field, expected in OPERATIONS_SCHEDULE_SAFETY.items():
            if getattr(self.safety, field) != expected:
                raise ValueError(
                    "ETF operations artifact freshness report safety "
                    f"{field} must be {expected!r}"
                )
        return self


class ETFOperationsFailurePolicyEvent(BaseModel):
    event_id: str = Field(min_length=1)
    event_type: OperationsFailureEventType
    source_step: str = Field(min_length=1)
    artifact_id: str = Field(min_length=1)
    path: str = Field(min_length=1)
    freshness_status: OperationsArtifactFreshnessStatus
    dependency_status: OperationsArtifactDependencyStatus
    required: bool
    failure_policy: OperationsFailurePolicy
    severity: OperationsFailureSeverity
    blocks_pipeline: bool
    blocks_dependent_steps: bool
    requires_manual_review: bool
    blocking_dependencies: list[str] = Field(default_factory=list)
    recommended_action: str = Field(min_length=1)


class ETFOperationsFailurePolicyReport(BaseModel):
    schema_version: Literal["etf_operations_failure_policy_v1"] = (
        OPERATIONS_FAILURE_POLICY_SCHEMA_VERSION
    )
    cadence: OperationsGraphCadence
    as_of_date: date
    evaluated_at: datetime
    read_only: bool = True
    commands_executed: bool = False
    safety: ETFOperationsScheduleSafety
    pipeline_status: OperationsPipelineStatus
    events: list[ETFOperationsFailurePolicyEvent] = Field(default_factory=list)
    blocking_events: list[str] = Field(default_factory=list)
    warning_events: list[str] = Field(default_factory=list)
    manual_review_events: list[str] = Field(default_factory=list)
    severity_summary: dict[str, int] = Field(default_factory=dict)
    failure_policy_summary: dict[str, int] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_report_safety(self) -> Self:
        if self.commands_executed:
            raise ValueError("ETF operations failure policy report must not execute commands")
        for field, expected in OPERATIONS_SCHEDULE_SAFETY.items():
            if getattr(self.safety, field) != expected:
                raise ValueError(
                    "ETF operations failure policy report safety " f"{field} must be {expected!r}"
                )
        return self


class ETFOperationsOwnerReviewChecklistItem(BaseModel):
    item_id: str = Field(min_length=1)
    category: OperationsOwnerChecklistItemCategory
    title: str = Field(min_length=1)
    description: str = Field(min_length=1)
    required: bool
    blocking: bool
    owner_action: str = Field(min_length=1)
    source_step: str | None = None
    related_event_ids: list[str] = Field(default_factory=list)
    related_artifact_ids: list[str] = Field(default_factory=list)
    evidence_paths: list[str] = Field(default_factory=list)


class ETFOperationsOwnerReviewChecklist(BaseModel):
    schema_version: Literal["etf_operations_owner_review_checklist_v1"] = (
        OPERATIONS_OWNER_REVIEW_CHECKLIST_SCHEMA_VERSION
    )
    cadence: OperationsOwnerReviewCadence
    as_of_date: date
    generated_at: datetime
    read_only: bool = True
    commands_executed: bool = False
    safety: ETFOperationsScheduleSafety
    checklist_step_id: str = Field(min_length=1)
    checklist_command: str = Field(min_length=1)
    checklist_dependencies: list[str] = Field(default_factory=list)
    checklist_expected_outputs: list[str] = Field(default_factory=list)
    checklist_status: OperationsOwnerChecklistStatus
    signoff_required: bool = True
    source_failure_policy_schema_version: str | None = None
    source_pipeline_status: OperationsPipelineStatus | None = None
    source_event_count: int = Field(ge=0)
    items: list[ETFOperationsOwnerReviewChecklistItem] = Field(min_length=1)
    required_items: list[str] = Field(default_factory=list)
    blocking_items: list[str] = Field(default_factory=list)
    warning_items: list[str] = Field(default_factory=list)
    manual_review_items: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_checklist_safety(self) -> Self:
        if self.commands_executed:
            raise ValueError("ETF operations owner review checklist must not execute commands")
        if not self.signoff_required:
            raise ValueError("ETF operations owner review checklist requires owner signoff")
        for field, expected in OPERATIONS_SCHEDULE_SAFETY.items():
            if getattr(self.safety, field) != expected:
                raise ValueError(
                    "ETF operations owner review checklist safety " f"{field} must be {expected!r}"
                )
        item_ids = [item.item_id for item in self.items]
        duplicate_item_ids = sorted(
            {item_id for item_id in item_ids if item_ids.count(item_id) > 1}
        )
        if duplicate_item_ids:
            duplicates = ", ".join(duplicate_item_ids)
            raise ValueError(
                "ETF operations owner review checklist item IDs must be unique: " f"{duplicates}"
            )
        return self


class ETFOperationsDryRunStep(BaseModel):
    step_id: str = Field(min_length=1)
    command: str = Field(min_length=1)
    status: OperationsDryRunStepStatus
    dependencies: list[str] = Field(default_factory=list)
    external_dependencies: list[str] = Field(default_factory=list)
    required: bool
    failure_policy: OperationsFailurePolicy
    estimated_runtime_class: OperationsRuntimeClass
    owner_review_required: bool
    expected_outputs: list[str] = Field(default_factory=list)
    related_artifact_ids: list[str] = Field(default_factory=list)
    blocking_event_ids: list[str] = Field(default_factory=list)
    warning_event_ids: list[str] = Field(default_factory=list)
    manual_review_event_ids: list[str] = Field(default_factory=list)
    command_would_execute: bool = True
    command_executed: bool = False


class ETFOperationsSchedulerDryRunReport(BaseModel):
    schema_version: Literal["etf_operations_scheduler_dry_run_v1"] = (
        OPERATIONS_SCHEDULER_DRY_RUN_SCHEMA_VERSION
    )
    dry_run_id: str = Field(min_length=1)
    cadence: OperationsGraphCadence
    as_of_date: date
    generated_at: datetime
    read_only: bool = True
    dry_run_only: bool = True
    commands_executed: bool = False
    production_state_mutated: bool = False
    safety: ETFOperationsScheduleSafety
    status: OperationsSchedulerDryRunStatus
    planned_steps: list[ETFOperationsDryRunStep] = Field(min_length=1)
    execution_order: list[str] = Field(default_factory=list)
    skipped_optional_steps: list[str] = Field(default_factory=list)
    blocking_failures: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    expected_outputs: list[str] = Field(default_factory=list)
    source_graph_schema_version: str
    source_freshness_schema_version: str
    source_failure_policy_schema_version: str
    owner_checklist_schema_version: str | None = None
    owner_checklist_status: OperationsOwnerChecklistStatus | None = None
    owner_checklist_item_count: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_dry_run_safety(self) -> Self:
        if not self.dry_run_only:
            raise ValueError("ETF operations scheduler dry-run must remain dry_run_only")
        if self.commands_executed:
            raise ValueError("ETF operations scheduler dry-run must not execute commands")
        if self.production_state_mutated:
            raise ValueError("ETF operations scheduler dry-run must not mutate production state")
        if [step.step_id for step in self.planned_steps] != self.execution_order:
            raise ValueError(
                "ETF operations scheduler dry-run planned steps must match execution_order"
            )
        for step in self.planned_steps:
            if step.command_executed:
                raise ValueError(
                    "ETF operations scheduler dry-run planned steps must not execute commands"
                )
        for field, expected in OPERATIONS_SCHEDULE_SAFETY.items():
            if getattr(self.safety, field) != expected:
                raise ValueError(
                    "ETF operations scheduler dry-run safety " f"{field} must be {expected!r}"
                )
        return self


class ETFOperationsHealthSourceArtifact(BaseModel):
    artifact_id: str = Field(min_length=1)
    path: str = Field(min_length=1)
    artifact_type: str = Field(min_length=1)
    source_step: str = Field(min_length=1)
    required: bool
    freshness_status: OperationsArtifactFreshnessStatus
    dependency_status: OperationsArtifactDependencyStatus
    generated_at: datetime | None = None
    as_of_date: date | None = None
    age_days: int | None = None


class ETFOperationsHealthReport(BaseModel):
    schema_version: Literal["etf_operations_health_report_v1"] = (
        OPERATIONS_HEALTH_REPORT_SCHEMA_VERSION
    )
    report_id: str = Field(min_length=1)
    cadence: OperationsGraphCadence
    as_of_date: date
    generated_at: datetime
    read_only: bool = True
    commands_executed: bool = False
    production_state_mutated: bool = False
    safety: ETFOperationsScheduleSafety
    status: OperationsHealthReportStatus
    safety_banner: dict[str, Any]
    run_metadata: dict[str, Any]
    pipeline_schedule: list[dict[str, Any]] = Field(default_factory=list)
    command_graph_summary: dict[str, Any]
    artifact_freshness_summary: dict[str, Any]
    dependency_status: dict[str, Any]
    failures: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[dict[str, Any]] = Field(default_factory=list)
    owner_review_checklist: dict[str, Any] | None = None
    expected_next_run: dict[str, Any]
    source_artifacts: list[ETFOperationsHealthSourceArtifact] = Field(default_factory=list)
    source_schema_versions: dict[str, str]
    source_dry_run_id: str = Field(min_length=1)
    source_dry_run_status: OperationsSchedulerDryRunStatus

    @model_validator(mode="after")
    def validate_report_safety(self) -> Self:
        if self.commands_executed:
            raise ValueError("ETF operations health report must not execute commands")
        if self.production_state_mutated:
            raise ValueError("ETF operations health report must not mutate production state")
        for field, expected in OPERATIONS_SCHEDULE_SAFETY.items():
            if getattr(self.safety, field) != expected:
                raise ValueError(
                    "ETF operations health report safety " f"{field} must be {expected!r}"
                )
            if self.safety_banner.get(field) != expected:
                raise ValueError(
                    "ETF operations health report safety banner " f"{field} must be {expected!r}"
                )
        if self.source_schema_versions.get("dry_run") != (
            OPERATIONS_SCHEDULER_DRY_RUN_SCHEMA_VERSION
        ):
            raise ValueError("ETF operations health report must link dry-run schema")
        return self


class ETFOperationsValidationCheck(BaseModel):
    check_id: str = Field(min_length=1)
    status: OperationsValidationCheckStatus
    message: str = Field(min_length=1)
    evidence: dict[str, Any] = Field(default_factory=dict)


class ETFOperationsValidationReport(BaseModel):
    schema_version: Literal["etf_operations_validation_v1"] = OPERATIONS_VALIDATION_SCHEMA_VERSION
    report_type: Literal["etf_operations_validation"] = OPERATIONS_VALIDATION_REPORT_TYPE
    report_id: str = Field(min_length=1)
    as_of_date: date
    generated_at: datetime
    status: OperationsValidationStatus
    checks: list[ETFOperationsValidationCheck] = Field(min_length=1)
    failed_check_count: int = Field(ge=0)
    warning_check_count: int = Field(ge=0)
    source_schema_versions: dict[str, str] = Field(default_factory=dict)
    safety_banner: dict[str, Any]
    read_only: bool = True
    commands_executed: bool = False
    production_state_mutated: bool = False
    production_effect: str = "none"
    broker_action: str = "none"
    manual_review_required: bool = True

    @model_validator(mode="after")
    def validate_report_safety(self) -> Self:
        if not self.read_only:
            raise ValueError("ETF operations validation report must remain read_only")
        if self.commands_executed:
            raise ValueError("ETF operations validation report must not execute commands")
        if self.production_state_mutated:
            raise ValueError("ETF operations validation report must not mutate production state")
        if self.production_effect != "none":
            raise ValueError("ETF operations validation report production_effect must be none")
        if self.broker_action != "none":
            raise ValueError("ETF operations validation report broker_action must be none")
        if not self.manual_review_required:
            raise ValueError("ETF operations validation report must require manual review")
        for field, expected in OPERATIONS_SCHEDULE_SAFETY.items():
            if self.safety_banner.get(field) != expected:
                raise ValueError(
                    "ETF operations validation report safety banner "
                    f"{field} must be {expected!r}"
                )

        check_ids = [check.check_id for check in self.checks]
        duplicates = sorted({check_id for check_id in check_ids if check_ids.count(check_id) > 1})
        if duplicates:
            raise ValueError(
                "ETF operations validation check IDs must be unique: " f"{', '.join(duplicates)}"
            )
        actual_failed = len([check for check in self.checks if check.status == "FAIL"])
        actual_warnings = len([check for check in self.checks if check.status == "WARNING"])
        if self.failed_check_count != actual_failed:
            raise ValueError("ETF operations validation failed_check_count mismatch")
        if self.warning_check_count != actual_warnings:
            raise ValueError("ETF operations validation warning_check_count mismatch")
        expected_status: OperationsValidationStatus = "FAIL" if actual_failed else "PASS"
        if self.status != expected_status:
            raise ValueError("ETF operations validation status must match failed checks")
        return self


def load_operations_schedule_config(
    path: Path | str = DEFAULT_ETF_OPERATIONS_SCHEDULE_CONFIG_PATH,
) -> ETFOperationsScheduleConfig:
    config_path = Path(path)
    try:
        raw = safe_load_yaml_path(config_path)
    except (OSError, yaml.YAMLError) as exc:
        raise OperationsScheduleError(
            f"ETF operations schedule config could not be loaded: {config_path}"
        ) from exc
    if not isinstance(raw, dict):
        raise OperationsScheduleError("ETF operations schedule config must be a mapping")
    return ETFOperationsScheduleConfig.model_validate(raw)


def operations_schedule_steps(
    config: ETFOperationsScheduleConfig,
) -> tuple[ETFOperationsScheduleStep, ...]:
    return config.steps()


def operations_schedule_step_ids(config: ETFOperationsScheduleConfig) -> tuple[str, ...]:
    return tuple(step.step_id for step in config.steps())


def operations_schedule_required_step_ids(
    config: ETFOperationsScheduleConfig,
    *,
    cadence: OperationsCadence | None = None,
) -> tuple[str, ...]:
    steps = config.steps() if cadence is None else config.steps_for_cadence(cadence)
    return tuple(step.step_id for step in steps if step.required)


def build_daily_operations_command_graph(
    config: ETFOperationsScheduleConfig | None = None,
    *,
    include_optional: bool = True,
    skipped_optional_step_ids: set[str] | None = None,
) -> ETFOperationsCommandGraph:
    return _build_operations_command_graph(
        config,
        cadence="daily",
        include_optional=include_optional,
        skipped_optional_step_ids=skipped_optional_step_ids,
    )


def build_weekly_operations_command_graph(
    config: ETFOperationsScheduleConfig | None = None,
    *,
    include_optional: bool = True,
    skipped_optional_step_ids: set[str] | None = None,
) -> ETFOperationsCommandGraph:
    return _build_operations_command_graph(
        config,
        cadence="weekly",
        include_optional=include_optional,
        skipped_optional_step_ids=skipped_optional_step_ids,
    )


def build_biweekly_operations_command_graph(
    config: ETFOperationsScheduleConfig | None = None,
    *,
    include_optional: bool = True,
    skipped_optional_step_ids: set[str] | None = None,
) -> ETFOperationsCommandGraph:
    return _build_operations_command_graph(
        config,
        cadence="biweekly",
        include_optional=include_optional,
        skipped_optional_step_ids=skipped_optional_step_ids,
    )


def build_monthly_operations_command_graph(
    config: ETFOperationsScheduleConfig | None = None,
    *,
    include_optional: bool = True,
    skipped_optional_step_ids: set[str] | None = None,
) -> ETFOperationsCommandGraph:
    return _build_operations_command_graph(
        config,
        cadence="monthly",
        include_optional=include_optional,
        skipped_optional_step_ids=skipped_optional_step_ids,
    )


def check_operations_artifact_freshness(
    graph: ETFOperationsCommandGraph,
    *,
    as_of: date | datetime | str,
    root_path: Path | str = PROJECT_ROOT,
    checked_at: datetime | None = None,
    config: ETFOperationsScheduleConfig | None = None,
) -> ETFOperationsArtifactFreshnessReport:
    requested_as_of = _coerce_date(as_of)
    checked = _coerce_datetime(checked_at or datetime.now(tz=UTC))
    schedule = config or load_operations_schedule_config()
    all_step_by_id = schedule.step_by_id()
    source_step_ids = _artifact_source_step_order(graph)
    dependency_steps_by_id = _artifact_dependency_steps_by_id(graph)

    artifacts: list[ETFOperationsArtifactStatus] = []
    artifacts_by_step: dict[str, list[ETFOperationsArtifactStatus]] = {}
    for step_id in source_step_ids:
        step = all_step_by_id.get(step_id)
        if step is None:
            raise OperationsCommandGraphError(
                f"{graph.cadence} operations artifact freshness references unknown "
                f"step: {step_id}"
            )
        dependency_steps = dependency_steps_by_id.get(step_id, ())
        step_artifacts = [
            _artifact_status_for_output(
                step,
                output_template=output_template,
                output_index=output_index,
                root_path=Path(root_path),
                requested_as_of=requested_as_of,
                dependency_steps=dependency_steps,
            )
            for output_index, output_template in enumerate(step.expected_outputs, start=1)
        ]
        artifacts.extend(step_artifacts)
        artifacts_by_step[step_id] = step_artifacts

    blocking_steps = {
        step_id
        for step_id, step_artifacts in artifacts_by_step.items()
        if any(_artifact_has_own_blocking_status(artifact) for artifact in step_artifacts)
    }
    for node in graph.nodes:
        blocking_dependencies = sorted(
            dependency_id
            for dependency_id in [*node.dependencies, *node.external_dependencies]
            if dependency_id in blocking_steps
        )
        if not blocking_dependencies:
            continue
        node_artifacts = artifacts_by_step.get(node.node_id, [])
        for artifact in node_artifacts:
            artifact.blocking_dependencies = blocking_dependencies
            artifact.dependency_status = "blocking" if artifact.required else "warning"
        blocking_steps.add(node.node_id)

    return ETFOperationsArtifactFreshnessReport(
        cadence=graph.cadence,
        as_of_date=requested_as_of,
        checked_at=checked,
        read_only=True,
        commands_executed=False,
        safety=graph.safety,
        artifacts=artifacts,
        blocking_artifacts=[
            artifact.artifact_id
            for artifact in artifacts
            if artifact.dependency_status == "blocking"
        ],
        warning_artifacts=[
            artifact.artifact_id
            for artifact in artifacts
            if artifact.dependency_status == "warning"
        ],
        optional_artifacts=[
            artifact.artifact_id
            for artifact in artifacts
            if artifact.dependency_status == "optional"
        ],
        freshness_summary=_status_counts(artifact.freshness_status for artifact in artifacts),
        dependency_summary=_status_counts(artifact.dependency_status for artifact in artifacts),
    )


def evaluate_operations_failure_policy(
    freshness_report: ETFOperationsArtifactFreshnessReport,
    *,
    config: ETFOperationsScheduleConfig | None = None,
    evaluated_at: datetime | None = None,
) -> ETFOperationsFailurePolicyReport:
    schedule = config or load_operations_schedule_config()
    step_by_id = schedule.step_by_id()
    evaluated = _coerce_datetime(evaluated_at or datetime.now(tz=UTC))

    events: list[ETFOperationsFailurePolicyEvent] = []
    for artifact in freshness_report.artifacts:
        event = _failure_policy_event_for_artifact(
            artifact,
            step_by_id=step_by_id,
        )
        if event is not None:
            events.append(event)
    events.sort(key=lambda event: (event.source_step, event.artifact_id, event.event_type))

    return ETFOperationsFailurePolicyReport(
        cadence=freshness_report.cadence,
        as_of_date=freshness_report.as_of_date,
        evaluated_at=evaluated,
        read_only=True,
        commands_executed=False,
        safety=freshness_report.safety,
        pipeline_status=_failure_policy_pipeline_status(events),
        events=events,
        blocking_events=[
            event.event_id
            for event in events
            if event.blocks_pipeline or event.blocks_dependent_steps
        ],
        warning_events=[event.event_id for event in events if event.severity == "warning"],
        manual_review_events=[event.event_id for event in events if event.requires_manual_review],
        severity_summary=_status_counts_with_defaults(
            (event.severity for event in events),
            allowed=("info", "warning", "error", "critical"),
        ),
        failure_policy_summary=_status_counts_with_defaults(
            (event.failure_policy for event in events),
            allowed=(
                "continue",
                "continue_with_warning",
                "skip_optional_step",
                "block_dependent_steps",
                "fail_pipeline",
                "manual_review_required",
            ),
        ),
    )


def build_operations_owner_review_checklist(
    *,
    cadence: OperationsOwnerReviewCadence,
    as_of: date | datetime | str,
    failure_report: ETFOperationsFailurePolicyReport | None = None,
    config: ETFOperationsScheduleConfig | None = None,
    generated_at: datetime | None = None,
) -> ETFOperationsOwnerReviewChecklist:
    schedule = config or load_operations_schedule_config()
    requested_as_of = _coerce_date(as_of)
    generated = _coerce_datetime(generated_at or datetime.now(tz=UTC))
    _validate_owner_checklist_failure_report(
        cadence=cadence,
        as_of_date=requested_as_of,
        failure_report=failure_report,
    )
    checklist_step = _owner_review_step_for_cadence(schedule, cadence)
    safety = failure_report.safety if failure_report is not None else schedule.safety

    items = [
        _owner_review_safety_item(),
        _owner_review_cadence_item(cadence),
        _owner_review_summary_item(failure_report),
    ]
    if failure_report is not None:
        items.extend(_owner_review_failure_event_item(event) for event in failure_report.events)
    items.append(_owner_review_signoff_item(cadence, failure_report))

    return ETFOperationsOwnerReviewChecklist(
        cadence=cadence,
        as_of_date=requested_as_of,
        generated_at=generated,
        read_only=True,
        commands_executed=False,
        safety=safety,
        checklist_step_id=checklist_step.step_id,
        checklist_command=checklist_step.command,
        checklist_dependencies=list(checklist_step.dependencies),
        checklist_expected_outputs=list(checklist_step.expected_outputs),
        checklist_status=_owner_review_checklist_status(cadence, failure_report),
        signoff_required=True,
        source_failure_policy_schema_version=(
            failure_report.schema_version if failure_report is not None else None
        ),
        source_pipeline_status=(
            failure_report.pipeline_status if failure_report is not None else None
        ),
        source_event_count=len(failure_report.events) if failure_report is not None else 0,
        items=items,
        required_items=[item.item_id for item in items if item.required],
        blocking_items=[item.item_id for item in items if item.blocking],
        warning_items=[item.item_id for item in items if item.category == "warning_event"],
        manual_review_items=[
            item.item_id for item in items if item.category == "manual_review_event"
        ],
    )


def build_operations_scheduler_dry_run(
    *,
    cadence: OperationsGraphCadence,
    as_of: date | datetime | str,
    root_path: Path | str = PROJECT_ROOT,
    config: ETFOperationsScheduleConfig | None = None,
    include_optional: bool = True,
    skipped_optional_step_ids: set[str] | None = None,
    generated_at: datetime | None = None,
) -> ETFOperationsSchedulerDryRunReport:
    schedule = config or load_operations_schedule_config()
    requested_cadence = _coerce_graph_cadence(cadence)
    requested_as_of = _coerce_date(as_of)
    generated = _coerce_datetime(generated_at or datetime.now(tz=UTC))
    graph = _build_operations_command_graph(
        schedule,
        cadence=requested_cadence,
        include_optional=include_optional,
        skipped_optional_step_ids=skipped_optional_step_ids,
    )
    freshness = check_operations_artifact_freshness(
        graph,
        as_of=requested_as_of,
        root_path=root_path,
        checked_at=generated,
        config=schedule,
    )
    failure_report = evaluate_operations_failure_policy(
        freshness,
        config=schedule,
        evaluated_at=generated,
    )
    owner_checklist = _dry_run_owner_checklist(
        cadence=requested_cadence,
        as_of_date=requested_as_of,
        failure_report=failure_report,
        config=schedule,
        generated_at=generated,
    )
    return _operations_dry_run_report_from_components(
        cadence=requested_cadence,
        as_of_date=requested_as_of,
        generated_at=generated,
        graph=graph,
        freshness=freshness,
        failure_report=failure_report,
        owner_checklist=owner_checklist,
    )


def write_operations_scheduler_dry_run(
    report: ETFOperationsSchedulerDryRunReport,
    path: Path | str,
) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    return output_path


def build_operations_health_report(
    *,
    cadence: OperationsGraphCadence,
    as_of: date | datetime | str,
    root_path: Path | str = PROJECT_ROOT,
    config: ETFOperationsScheduleConfig | None = None,
    include_optional: bool = True,
    skipped_optional_step_ids: set[str] | None = None,
    generated_at: datetime | None = None,
) -> ETFOperationsHealthReport:
    schedule = config or load_operations_schedule_config()
    requested_cadence = _coerce_graph_cadence(cadence)
    requested_as_of = _coerce_date(as_of)
    generated = _coerce_datetime(generated_at or datetime.now(tz=UTC))
    graph = _build_operations_command_graph(
        schedule,
        cadence=requested_cadence,
        include_optional=include_optional,
        skipped_optional_step_ids=skipped_optional_step_ids,
    )
    freshness = check_operations_artifact_freshness(
        graph,
        as_of=requested_as_of,
        root_path=root_path,
        checked_at=generated,
        config=schedule,
    )
    failure_report = evaluate_operations_failure_policy(
        freshness,
        config=schedule,
        evaluated_at=generated,
    )
    owner_checklist = _dry_run_owner_checklist(
        cadence=requested_cadence,
        as_of_date=requested_as_of,
        failure_report=failure_report,
        config=schedule,
        generated_at=generated,
    )
    dry_run = _operations_dry_run_report_from_components(
        cadence=requested_cadence,
        as_of_date=requested_as_of,
        generated_at=generated,
        graph=graph,
        freshness=freshness,
        failure_report=failure_report,
        owner_checklist=owner_checklist,
    )

    return ETFOperationsHealthReport(
        report_id=_operations_health_report_id(
            cadence=requested_cadence,
            as_of_date=requested_as_of,
            generated_at=generated,
        ),
        cadence=requested_cadence,
        as_of_date=requested_as_of,
        generated_at=generated,
        read_only=True,
        commands_executed=False,
        production_state_mutated=False,
        safety=graph.safety,
        status=_operations_health_status(dry_run.status),
        safety_banner=_operations_safety_banner(graph.safety),
        run_metadata=_operations_report_run_metadata(
            graph=graph,
            dry_run=dry_run,
            root_path=root_path,
        ),
        pipeline_schedule=_operations_report_pipeline_schedule(graph, dry_run),
        command_graph_summary=_operations_report_command_graph_summary(graph),
        artifact_freshness_summary=_operations_report_artifact_summary(freshness),
        dependency_status=_operations_report_dependency_status(freshness, graph),
        failures=_operations_report_failure_rows(
            failure_report,
            include_warning=False,
        ),
        warnings=_operations_report_failure_rows(
            failure_report,
            include_warning=True,
        ),
        owner_review_checklist=_operations_report_owner_checklist(owner_checklist),
        expected_next_run=_operations_expected_next_run(requested_cadence),
        source_artifacts=_operations_report_source_artifacts(freshness),
        source_schema_versions={
            "schedule": schedule.schema_version,
            "graph": graph.schema_version,
            "freshness": freshness.schema_version,
            "failure_policy": failure_report.schema_version,
            "owner_checklist": (
                owner_checklist.schema_version if owner_checklist is not None else "none"
            ),
            "dry_run": dry_run.schema_version,
        },
        source_dry_run_id=dry_run.dry_run_id,
        source_dry_run_status=dry_run.status,
    )


def render_operations_health_report_markdown(
    report: ETFOperationsHealthReport,
) -> str:
    lines: list[str] = [
        "# ETF Operations Health Report",
        "",
        "## Safety Banner / 安全边界",
        "",
        "| Field | Value |",
        "|---|---|",
    ]
    for field in OPERATIONS_SCHEDULE_SAFETY:
        value = report.safety_banner[field]
        lines.append(f"| {field} | {_markdown_value(value)} |")

    lines.extend(
        [
            "",
            "## Run Metadata / 运行元数据",
            "",
            "| Field | Value |",
            "|---|---|",
            f"| report_id | `{report.report_id}` |",
            f"| cadence | `{report.cadence}` |",
            f"| as_of_date | `{report.as_of_date.isoformat()}` |",
            f"| generated_at | `{report.generated_at.isoformat()}` |",
            f"| status | `{report.status}` |",
            f"| source_dry_run_id | `{report.source_dry_run_id}` |",
            f"| source_dry_run_status | `{report.source_dry_run_status}` |",
            "",
            "## Pipeline Schedule / Pipeline 计划",
            "",
            "| Step | Required | Status | Runtime | Dependencies | Command |",
            "|---|---:|---|---|---|---|",
        ]
    )
    for step in report.pipeline_schedule:
        dependencies = _markdown_list(step["dependencies"])
        lines.append(
            f"| {step['step_id']} | {_markdown_value(step['required'])} | "
            f"{step['status']} | {step['estimated_runtime_class']} | "
            f"{dependencies} | `{step['command']}` |"
        )

    lines.extend(
        [
            "",
            "## Command Graph Summary / Command Graph 摘要",
            "",
            "| Field | Value |",
            "|---|---|",
        ]
    )
    for field, value in report.command_graph_summary.items():
        lines.append(f"| {field} | {_markdown_value(value)} |")

    lines.extend(
        [
            "",
            "## Artifact Freshness Summary / Artifact Freshness 摘要",
            "",
            "| Field | Value |",
            "|---|---|",
        ]
    )
    for field, value in report.artifact_freshness_summary.items():
        lines.append(f"| {field} | {_markdown_value(value)} |")

    lines.extend(
        [
            "",
            "## Dependency Status / Dependency 状态",
            "",
            "| Field | Value |",
            "|---|---|",
        ]
    )
    for field, value in report.dependency_status.items():
        lines.append(f"| {field} | {_markdown_value(value)} |")

    lines.extend(
        [
            "",
            "## Failures And Warnings / 失败与警告",
            "",
            "| Type | Event | Severity | Step | Artifact | Action |",
            "|---|---|---|---|---|---|",
        ]
    )
    rows = [("failure", row) for row in report.failures] + [
        ("warning", row) for row in report.warnings
    ]
    if rows:
        for row_type, row in rows:
            lines.append(
                f"| {row_type} | `{row['event_id']}` | {row['severity']} | "
                f"{row['source_step']} | `{row['artifact_id']}` | "
                f"{row['recommended_action']} |"
            )
    else:
        lines.append("| none | none | none | none | none | no_action_required |")

    lines.extend(
        [
            "",
            "## Owner Review Checklist / Owner Review Checklist",
            "",
        ]
    )
    if report.owner_review_checklist is None:
        lines.append("No owner review checklist is defined for this cadence.")
    else:
        checklist = report.owner_review_checklist
        lines.extend(
            [
                f"- checklist_step_id: `{checklist['checklist_step_id']}`",
                f"- checklist_status: `{checklist['checklist_status']}`",
                f"- signoff_required: `{_markdown_value(checklist['signoff_required'])}`",
                "",
                "| Item | Category | Required | Blocking | Owner Action |",
                "|---|---|---:|---:|---|",
            ]
        )
        for item in checklist["items"]:
            lines.append(
                "| {item_id} | {category} | {required} | {blocking} | {owner_action} |".format(
                    item_id=item["item_id"],
                    category=item["category"],
                    required=_markdown_value(item["required"]),
                    blocking=_markdown_value(item["blocking"]),
                    owner_action=item["owner_action"],
                )
            )

    lines.extend(
        [
            "",
            "## Expected Next Run / 预计下一次运行",
            "",
            "| Field | Value |",
            "|---|---|",
        ]
    )
    for field, value in report.expected_next_run.items():
        lines.append(f"| {field} | {_markdown_value(value)} |")

    lines.extend(
        [
            "",
            "## Source Artifacts / Source Artifacts",
            "",
            "| Artifact | Step | Required | Freshness | Dependency | Path |",
            "|---|---|---:|---|---|---|",
        ]
    )
    for artifact in report.source_artifacts:
        lines.append(
            f"| {artifact.artifact_id} | {artifact.source_step} | "
            f"{_markdown_value(artifact.required)} | {artifact.freshness_status} | "
            f"{artifact.dependency_status} | `{artifact.path}` |"
        )

    lines.extend(
        [
            "",
            "## Source Schema Versions / Source Schema Versions",
            "",
            "| Source | Schema Version |",
            "|---|---|",
        ]
    )
    for source, schema_version in report.source_schema_versions.items():
        lines.append(f"| {source} | `{schema_version}` |")
    return "\n".join(lines) + "\n"


def write_operations_health_report(
    report: ETFOperationsHealthReport,
    *,
    json_path: Path | str,
    markdown_path: Path | str,
) -> dict[str, Path]:
    json_output = Path(json_path)
    markdown_output = Path(markdown_path)
    json_output.parent.mkdir(parents=True, exist_ok=True)
    markdown_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    markdown_output.write_text(
        render_operations_health_report_markdown(report),
        encoding="utf-8",
    )
    return {"json": json_output, "markdown": markdown_output}


def build_operations_validation_report(
    *,
    as_of: date | datetime | str,
    root_path: Path | str = PROJECT_ROOT,
    config_path: Path | str = DEFAULT_ETF_OPERATIONS_SCHEDULE_CONFIG_PATH,
    report_registry_path: Path | str = DEFAULT_REPORT_REGISTRY_PATH,
    config: ETFOperationsScheduleConfig | None = None,
    generated_at: datetime | None = None,
) -> ETFOperationsValidationReport:
    requested_as_of = _coerce_date(as_of)
    generated = _coerce_datetime(generated_at or datetime.now(tz=UTC))
    checks: list[ETFOperationsValidationCheck] = []
    source_schema_versions = _operations_validation_source_schema_versions()

    try:
        schedule = config or load_operations_schedule_config(config_path)
    except Exception as exc:  # noqa: BLE001 - validation gates must fail closed.
        checks.append(
            _operations_validation_check(
                "schedule_spec_valid",
                "FAIL",
                "operations schedule spec failed validation",
                evidence={
                    "config_path": str(config_path),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            )
        )
        return _operations_validation_report(
            as_of_date=requested_as_of,
            generated_at=generated,
            checks=checks,
            source_schema_versions=source_schema_versions,
        )

    source_schema_versions["schedule"] = schedule.schema_version
    checks.append(
        _operations_validation_check(
            "schedule_spec_valid",
            "PASS",
            "operations schedule spec loaded and validated",
            evidence={
                "config_path": str(config_path),
                "schema_version": schedule.schema_version,
                "step_count": len(schedule.steps()),
            },
        )
    )
    checks.append(
        _operations_validation_check(
            "schedule_safety_boundary",
            "PASS" if _operations_safety_matches(schedule.safety) else "FAIL",
            (
                "schedule safety boundary matches observe-only policy"
                if _operations_safety_matches(schedule.safety)
                else "schedule safety boundary is unsafe"
            ),
            evidence=_operations_safety_banner(schedule.safety),
        )
    )

    missing_required_by_cadence = _operations_validation_missing_required_nodes(schedule)
    checks.append(
        _operations_validation_check(
            "required_steps_present",
            "PASS" if not missing_required_by_cadence else "FAIL",
            (
                "required operations steps are present for every cadence"
                if not missing_required_by_cadence
                else "one or more required operations steps are missing"
            ),
            evidence={"missing_required_by_cadence": missing_required_by_cadence},
        )
    )

    graphs: dict[OperationsGraphCadence, ETFOperationsCommandGraph] = {}
    for cadence in _operations_validation_cadences():
        check_id = f"{cadence}_graph_valid"
        try:
            graph = _build_operations_command_graph(
                schedule,
                cadence=cadence,
                include_optional=True,
                skipped_optional_step_ids=None,
            )
        except Exception as exc:  # noqa: BLE001 - collect all gate failures.
            checks.append(
                _operations_validation_check(
                    check_id,
                    "FAIL",
                    f"{cadence} operations command graph failed validation",
                    evidence={
                        "cadence": cadence,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    },
                )
            )
            continue

        graphs[cadence] = graph
        source_schema_versions[f"{cadence}_graph"] = graph.schema_version
        missing_graph_nodes = sorted(
            _REQUIRED_OPERATION_NODE_IDS_BY_CADENCE[cadence] - set(graph.execution_order)
        )
        graph_safe = (
            graph.schema_version == OPERATIONS_COMMAND_GRAPH_SCHEMA_VERSION
            and graph.dry_run_only
            and not graph.commands_executed
            and _operations_safety_matches(graph.safety)
            and not missing_graph_nodes
        )
        checks.append(
            _operations_validation_check(
                check_id,
                "PASS" if graph_safe else "FAIL",
                (
                    f"{cadence} operations command graph is valid"
                    if graph_safe
                    else f"{cadence} operations command graph is unsafe or incomplete"
                ),
                evidence={
                    "cadence": cadence,
                    "schema_version": graph.schema_version,
                    "dry_run_only": graph.dry_run_only,
                    "commands_executed": graph.commands_executed,
                    "node_count": len(graph.nodes),
                    "missing_required_nodes": missing_graph_nodes,
                },
            )
        )

    daily_graph = graphs.get("daily")
    missing_freshness: ETFOperationsArtifactFreshnessReport | None = None
    optional_freshness: ETFOperationsArtifactFreshnessReport | None = None
    failure_report: ETFOperationsFailurePolicyReport | None = None
    owner_checklist: ETFOperationsOwnerReviewChecklist | None = None
    dry_run: ETFOperationsSchedulerDryRunReport | None = None
    health_report: ETFOperationsHealthReport | None = None

    if daily_graph is None:
        for check_id in (
            "freshness_checker_available",
            "required_missing_blocks",
            "optional_missing_warns",
            "failure_policy_available",
            "owner_checklist_available",
            "dry_run_available",
            "ops_report_generator_available",
        ):
            checks.append(
                _operations_validation_check(
                    check_id,
                    "FAIL",
                    "daily graph is unavailable; dependent operations validation skipped",
                    evidence={"required_graph": "daily"},
                )
            )
    else:
        with TemporaryDirectory(prefix="aits_ops_validate_missing_") as missing_root:
            try:
                missing_freshness = check_operations_artifact_freshness(
                    daily_graph,
                    as_of=requested_as_of,
                    root_path=missing_root,
                    checked_at=generated,
                    config=schedule,
                )
                source_schema_versions["freshness"] = missing_freshness.schema_version
                freshness_safe = (
                    missing_freshness.schema_version == OPERATIONS_ARTIFACT_FRESHNESS_SCHEMA_VERSION
                    and missing_freshness.read_only
                    and not missing_freshness.commands_executed
                    and _operations_safety_matches(missing_freshness.safety)
                )
                checks.append(
                    _operations_validation_check(
                        "freshness_checker_available",
                        "PASS" if freshness_safe else "FAIL",
                        (
                            "freshness checker is available and read-only"
                            if freshness_safe
                            else "freshness checker produced unsafe output"
                        ),
                        evidence={
                            "schema_version": missing_freshness.schema_version,
                            "artifact_count": len(missing_freshness.artifacts),
                            "blocking_artifact_count": len(missing_freshness.blocking_artifacts),
                        },
                    )
                )
                required_missing_blocks = [
                    artifact.artifact_id
                    for artifact in missing_freshness.artifacts
                    if artifact.required
                    and artifact.freshness_status == "missing"
                    and artifact.dependency_status == "blocking"
                ]
                checks.append(
                    _operations_validation_check(
                        "required_missing_blocks",
                        "PASS" if required_missing_blocks else "FAIL",
                        (
                            "required missing artifacts block dependent operations"
                            if required_missing_blocks
                            else "required missing artifacts did not block dependent operations"
                        ),
                        evidence={"required_missing_blocking_artifacts": required_missing_blocks},
                    )
                )
            except Exception as exc:  # noqa: BLE001 - collect gate failure.
                checks.append(
                    _operations_validation_check(
                        "freshness_checker_available",
                        "FAIL",
                        "freshness checker failed validation",
                        evidence={
                            "error_type": type(exc).__name__,
                            "error": str(exc),
                        },
                    )
                )
                checks.append(
                    _operations_validation_check(
                        "required_missing_blocks",
                        "FAIL",
                        "required missing artifact blocking could not be evaluated",
                        evidence={"error_type": type(exc).__name__, "error": str(exc)},
                    )
                )

        with TemporaryDirectory(prefix="aits_ops_validate_optional_") as optional_root:
            try:
                _write_operations_validation_required_artifacts(
                    root_path=Path(optional_root),
                    graph=daily_graph,
                    config=schedule,
                    as_of_date=requested_as_of,
                    generated_at=generated,
                )
                optional_freshness = check_operations_artifact_freshness(
                    daily_graph,
                    as_of=requested_as_of,
                    root_path=optional_root,
                    checked_at=generated,
                    config=schedule,
                )
                optional_warning_artifacts = [
                    artifact.artifact_id
                    for artifact in optional_freshness.artifacts
                    if not artifact.required
                    and artifact.freshness_status == "missing"
                    and artifact.dependency_status == "warning"
                ]
                required_blockers = [
                    artifact.artifact_id
                    for artifact in optional_freshness.artifacts
                    if artifact.required and artifact.dependency_status == "blocking"
                ]
                optional_warning_ok = bool(optional_warning_artifacts) and not required_blockers
                checks.append(
                    _operations_validation_check(
                        "optional_missing_warns",
                        "PASS" if optional_warning_ok else "FAIL",
                        (
                            "optional missing artifacts warn without blocking required flow"
                            if optional_warning_ok
                            else "optional missing artifacts did not warn cleanly"
                        ),
                        evidence={
                            "optional_warning_artifacts": optional_warning_artifacts,
                            "required_blockers": required_blockers,
                        },
                    )
                )
            except Exception as exc:  # noqa: BLE001 - collect gate failure.
                checks.append(
                    _operations_validation_check(
                        "optional_missing_warns",
                        "FAIL",
                        "optional missing artifact warning behavior failed validation",
                        evidence={"error_type": type(exc).__name__, "error": str(exc)},
                    )
                )

        if missing_freshness is None:
            checks.append(
                _operations_validation_check(
                    "failure_policy_available",
                    "FAIL",
                    "failure policy could not be evaluated without freshness report",
                    evidence={"required_source": "freshness"},
                )
            )
        else:
            try:
                failure_report = evaluate_operations_failure_policy(
                    missing_freshness,
                    config=schedule,
                    evaluated_at=generated,
                )
                source_schema_versions["failure_policy"] = failure_report.schema_version
                failure_policy_available = (
                    failure_report.schema_version == OPERATIONS_FAILURE_POLICY_SCHEMA_VERSION
                    and failure_report.read_only
                    and not failure_report.commands_executed
                    and failure_report.pipeline_status == "blocked"
                    and bool(failure_report.blocking_events)
                    and _operations_safety_matches(failure_report.safety)
                )
                checks.append(
                    _operations_validation_check(
                        "failure_policy_available",
                        "PASS" if failure_policy_available else "FAIL",
                        (
                            "failure policy is available and blocks unsafe missing inputs"
                            if failure_policy_available
                            else "failure policy did not block unsafe missing inputs"
                        ),
                        evidence={
                            "schema_version": failure_report.schema_version,
                            "pipeline_status": failure_report.pipeline_status,
                            "blocking_event_count": len(failure_report.blocking_events),
                            "warning_event_count": len(failure_report.warning_events),
                        },
                    )
                )
            except Exception as exc:  # noqa: BLE001 - collect gate failure.
                checks.append(
                    _operations_validation_check(
                        "failure_policy_available",
                        "FAIL",
                        "failure policy evaluator failed validation",
                        evidence={"error_type": type(exc).__name__, "error": str(exc)},
                    )
                )

        if failure_report is None:
            checks.append(
                _operations_validation_check(
                    "owner_checklist_available",
                    "FAIL",
                    "owner checklist could not be evaluated without failure policy",
                    evidence={"required_source": "failure_policy"},
                )
            )
        else:
            try:
                owner_checklist = build_operations_owner_review_checklist(
                    cadence="daily",
                    as_of=requested_as_of,
                    failure_report=failure_report,
                    config=schedule,
                    generated_at=generated,
                )
                source_schema_versions["owner_checklist"] = owner_checklist.schema_version
                checklist_available = (
                    owner_checklist.schema_version
                    == OPERATIONS_OWNER_REVIEW_CHECKLIST_SCHEMA_VERSION
                    and owner_checklist.read_only
                    and not owner_checklist.commands_executed
                    and owner_checklist.signoff_required
                    and owner_checklist.checklist_status == "blocked"
                    and _operations_safety_matches(owner_checklist.safety)
                )
                checks.append(
                    _operations_validation_check(
                        "owner_checklist_available",
                        "PASS" if checklist_available else "FAIL",
                        (
                            "owner checklist is available and requires signoff"
                            if checklist_available
                            else "owner checklist did not enforce signoff"
                        ),
                        evidence={
                            "schema_version": owner_checklist.schema_version,
                            "checklist_status": owner_checklist.checklist_status,
                            "signoff_required": owner_checklist.signoff_required,
                            "item_count": len(owner_checklist.items),
                        },
                    )
                )
            except Exception as exc:  # noqa: BLE001 - collect gate failure.
                checks.append(
                    _operations_validation_check(
                        "owner_checklist_available",
                        "FAIL",
                        "owner checklist builder failed validation",
                        evidence={"error_type": type(exc).__name__, "error": str(exc)},
                    )
                )

        try:
            dry_run = build_operations_scheduler_dry_run(
                cadence="daily",
                as_of=requested_as_of,
                root_path=root_path,
                config=schedule,
                generated_at=generated,
            )
            source_schema_versions["dry_run"] = dry_run.schema_version
            dry_run_available = (
                dry_run.schema_version == OPERATIONS_SCHEDULER_DRY_RUN_SCHEMA_VERSION
                and dry_run.read_only
                and dry_run.dry_run_only
                and not dry_run.commands_executed
                and not dry_run.production_state_mutated
                and _operations_safety_matches(dry_run.safety)
            )
            checks.append(
                _operations_validation_check(
                    "dry_run_available",
                    "PASS" if dry_run_available else "FAIL",
                    (
                        "scheduler dry-run is available and non-executing"
                        if dry_run_available
                        else "scheduler dry-run output is unsafe or incomplete"
                    ),
                    evidence={
                        "root_path": str(Path(root_path)),
                        "schema_version": dry_run.schema_version,
                        "status": dry_run.status,
                        "planned_step_count": len(dry_run.planned_steps),
                        "commands_executed": dry_run.commands_executed,
                        "production_state_mutated": dry_run.production_state_mutated,
                    },
                )
            )
        except Exception as exc:  # noqa: BLE001 - collect gate failure.
            checks.append(
                _operations_validation_check(
                    "dry_run_available",
                    "FAIL",
                    "scheduler dry-run builder failed validation",
                    evidence={"error_type": type(exc).__name__, "error": str(exc)},
                )
            )

        try:
            health_report = build_operations_health_report(
                cadence="daily",
                as_of=requested_as_of,
                root_path=root_path,
                config=schedule,
                generated_at=generated,
            )
            source_schema_versions["health_report"] = health_report.schema_version
            report_available = (
                health_report.schema_version == OPERATIONS_HEALTH_REPORT_SCHEMA_VERSION
                and health_report.read_only
                and not health_report.commands_executed
                and not health_report.production_state_mutated
                and _operations_safety_matches(health_report.safety)
            )
            checks.append(
                _operations_validation_check(
                    "ops_report_generator_available",
                    "PASS" if report_available else "FAIL",
                    (
                        "operations health report generator is available"
                        if report_available
                        else "operations health report output is unsafe or incomplete"
                    ),
                    evidence={
                        "root_path": str(Path(root_path)),
                        "schema_version": health_report.schema_version,
                        "status": health_report.status,
                        "source_dry_run_status": health_report.source_dry_run_status,
                        "commands_executed": health_report.commands_executed,
                        "production_state_mutated": health_report.production_state_mutated,
                    },
                )
            )
        except Exception as exc:  # noqa: BLE001 - collect gate failure.
            checks.append(
                _operations_validation_check(
                    "ops_report_generator_available",
                    "FAIL",
                    "operations health report builder failed validation",
                    evidence={"error_type": type(exc).__name__, "error": str(exc)},
                )
            )

    try:
        registry = load_report_registry(Path(report_registry_path))
        source_schema_versions["report_registry"] = str(registry.get("policy_version"))
        entry = next(
            (
                item
                for item in registry["reports"]
                if item.get("report_id") == OPERATIONS_HEALTH_REPORT_REGISTRY_ID
            ),
            None,
        )
        artifact_globs = list(entry.get("artifact_globs", [])) if entry else []
        command = str(entry.get("command", "")) if entry else ""
        reader_brief_integration_ok = (
            entry is not None
            and entry.get("include_in_reader_brief") is True
            and command.startswith("aits etf ops report")
            and any("operations_health_" in artifact_glob for artifact_glob in artifact_globs)
        )
        checks.append(
            _operations_validation_check(
                "reader_brief_integration_available",
                "PASS" if reader_brief_integration_ok else "FAIL",
                (
                    "Reader Brief operations health registry integration is available"
                    if reader_brief_integration_ok
                    else "Reader Brief operations health registry integration is missing"
                ),
                evidence={
                    "report_registry_path": str(report_registry_path),
                    "report_id": OPERATIONS_HEALTH_REPORT_REGISTRY_ID,
                    "include_in_reader_brief": (
                        entry.get("include_in_reader_brief") if entry else None
                    ),
                    "command": command,
                    "artifact_globs": artifact_globs,
                },
            )
        )
    except Exception as exc:  # noqa: BLE001 - collect gate failure.
        checks.append(
            _operations_validation_check(
                "reader_brief_integration_available",
                "FAIL",
                "Reader Brief operations health registry integration failed validation",
                evidence={"error_type": type(exc).__name__, "error": str(exc)},
            )
        )

    safety_components: dict[str, Any] = {"schedule": schedule}
    safety_components.update({f"{cadence}_graph": graph for cadence, graph in graphs.items()})
    for name, component in (
        ("freshness", missing_freshness),
        ("optional_freshness", optional_freshness),
        ("failure_policy", failure_report),
        ("owner_checklist", owner_checklist),
        ("dry_run", dry_run),
        ("health_report", health_report),
    ):
        if component is not None:
            safety_components[name] = component
    unsafe_components = _operations_validation_unsafe_components(safety_components)
    checks.append(
        _operations_validation_check(
            "safety_fields_intact",
            "PASS" if not unsafe_components else "FAIL",
            (
                "all operations validation components preserve safety boundary"
                if not unsafe_components
                else "one or more operations validation components are unsafe"
            ),
            evidence={
                "unsafe_components": unsafe_components,
                "required_safety": dict(OPERATIONS_SCHEDULE_SAFETY),
            },
        )
    )

    return _operations_validation_report(
        as_of_date=requested_as_of,
        generated_at=generated,
        checks=checks,
        source_schema_versions=source_schema_versions,
    )


def render_operations_validation_report_markdown(
    report: ETFOperationsValidationReport,
) -> str:
    lines: list[str] = [
        "# ETF Operations Validation Gate",
        "",
        "## Summary / 摘要",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| report_id | `{report.report_id}` |",
        f"| as_of_date | `{report.as_of_date.isoformat()}` |",
        f"| generated_at | `{report.generated_at.isoformat()}` |",
        f"| status | `{report.status}` |",
        f"| failed_check_count | {report.failed_check_count} |",
        f"| warning_check_count | {report.warning_check_count} |",
        f"| production_effect | `{report.production_effect}` |",
        f"| broker_action | `{report.broker_action}` |",
        f"| manual_review_required | {_markdown_value(report.manual_review_required)} |",
        "",
        "## Safety Banner / 安全边界",
        "",
        "| Field | Value |",
        "|---|---|",
    ]
    for field in OPERATIONS_SCHEDULE_SAFETY:
        lines.append(f"| {field} | {_markdown_value(report.safety_banner[field])} |")

    lines.extend(
        [
            "",
            "## Checks / 校验项",
            "",
            "| Check | Status | Message | Evidence |",
            "|---|---|---|---|",
        ]
    )
    for check in report.checks:
        lines.append(
            f"| `{check.check_id}` | `{check.status}` | "
            f"{check.message} | `{_json_for_markdown(check.evidence)}` |"
        )

    lines.extend(
        [
            "",
            "## Source Schema Versions / Source Schema Versions",
            "",
            "| Source | Schema Version |",
            "|---|---|",
        ]
    )
    for source, schema_version in report.source_schema_versions.items():
        lines.append(f"| {source} | `{schema_version}` |")
    return "\n".join(lines) + "\n"


def write_operations_validation_report(
    report: ETFOperationsValidationReport,
    *,
    json_path: Path | str,
    markdown_path: Path | str,
) -> dict[str, Path]:
    json_output = Path(json_path)
    markdown_output = Path(markdown_path)
    json_output.parent.mkdir(parents=True, exist_ok=True)
    markdown_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    markdown_output.write_text(
        render_operations_validation_report_markdown(report),
        encoding="utf-8",
    )
    return {"json": json_output, "markdown": markdown_output}


def _operations_validation_cadences() -> tuple[OperationsGraphCadence, ...]:
    return ("daily", "weekly", "biweekly", "monthly")


def _operations_validation_source_schema_versions() -> dict[str, str]:
    source_schema_versions = {
        "schedule": "not_checked",
        "freshness": "not_checked",
        "failure_policy": "not_checked",
        "owner_checklist": "not_checked",
        "dry_run": "not_checked",
        "health_report": "not_checked",
        "report_registry": "not_checked",
    }
    for cadence in _operations_validation_cadences():
        source_schema_versions[f"{cadence}_graph"] = "not_checked"
    return source_schema_versions


def _operations_validation_report_id(
    *,
    as_of_date: date,
    generated_at: datetime,
) -> str:
    timestamp = generated_at.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"operations_validation:{as_of_date.isoformat()}:{timestamp}"


def _operations_validation_report(
    *,
    as_of_date: date,
    generated_at: datetime,
    checks: list[ETFOperationsValidationCheck],
    source_schema_versions: dict[str, str],
) -> ETFOperationsValidationReport:
    failed_check_count = len([check for check in checks if check.status == "FAIL"])
    warning_check_count = len([check for check in checks if check.status == "WARNING"])
    return ETFOperationsValidationReport(
        report_id=_operations_validation_report_id(
            as_of_date=as_of_date,
            generated_at=generated_at,
        ),
        as_of_date=as_of_date,
        generated_at=generated_at,
        status="FAIL" if failed_check_count else "PASS",
        checks=checks,
        failed_check_count=failed_check_count,
        warning_check_count=warning_check_count,
        source_schema_versions=source_schema_versions,
        safety_banner=dict(OPERATIONS_SCHEDULE_SAFETY),
        read_only=True,
        commands_executed=False,
        production_state_mutated=False,
        production_effect="none",
        broker_action="none",
        manual_review_required=True,
    )


def _operations_validation_check(
    check_id: str,
    status: OperationsValidationCheckStatus,
    message: str,
    *,
    evidence: dict[str, Any] | None = None,
) -> ETFOperationsValidationCheck:
    return ETFOperationsValidationCheck(
        check_id=check_id,
        status=status,
        message=message,
        evidence=evidence or {},
    )


def _operations_validation_missing_required_nodes(
    schedule: ETFOperationsScheduleConfig,
) -> dict[str, list[str]]:
    missing_by_cadence: dict[str, list[str]] = {}
    for cadence, required_node_ids in _REQUIRED_OPERATION_NODE_IDS_BY_CADENCE.items():
        pipeline_field = _GRAPH_PIPELINE_FIELD_BY_CADENCE[cadence]
        configured_node_ids = {step.step_id for step in getattr(schedule, pipeline_field)}
        missing = sorted(required_node_ids - configured_node_ids)
        if missing:
            missing_by_cadence[cadence] = missing
    return missing_by_cadence


def _operations_validation_unsafe_components(
    components: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    unsafe_components: dict[str, dict[str, Any]] = {}
    for name, component in components.items():
        safety = getattr(component, "safety", None)
        if safety is None:
            continue
        if _operations_safety_matches(safety):
            continue
        unsafe_components[name] = _operations_safety_banner(safety)

    for name, component in components.items():
        executed = getattr(component, "commands_executed", False)
        production_mutated = getattr(component, "production_state_mutated", False)
        if executed or production_mutated:
            unsafe_components.setdefault(name, {})
            unsafe_components[name].update(
                {
                    "commands_executed": executed,
                    "production_state_mutated": production_mutated,
                }
            )
    return unsafe_components


def _operations_safety_matches(safety: ETFOperationsScheduleSafety) -> bool:
    return all(
        getattr(safety, field) == expected for field, expected in OPERATIONS_SCHEDULE_SAFETY.items()
    )


def _write_operations_validation_required_artifacts(
    *,
    root_path: Path,
    graph: ETFOperationsCommandGraph,
    config: ETFOperationsScheduleConfig,
    as_of_date: date,
    generated_at: datetime,
) -> None:
    step_by_id = config.step_by_id()
    for node in graph.nodes:
        step = step_by_id[node.node_id]
        if not step.required:
            continue
        for output_template in step.expected_outputs:
            if output_template.startswith("manual_review_checklist:"):
                continue
            output_path = _operations_validation_artifact_path(
                root_path=root_path,
                output_template=output_template,
                as_of_date=as_of_date,
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)
            if output_path.suffix.lower() == ".json":
                output_path.write_text(
                    json.dumps(
                        {
                            "generated_at": generated_at.isoformat(),
                            "as_of_date": as_of_date.isoformat(),
                            "status": "PASS",
                            "production_effect": "none",
                        },
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )
            else:
                output_path.write_text(
                    "\n".join(
                        [
                            f"generated_at: {generated_at.isoformat()}",
                            f"as_of_date: {as_of_date.isoformat()}",
                            "status: PASS",
                            "production_effect: none",
                            "",
                        ]
                    ),
                    encoding="utf-8",
                )


def _operations_validation_artifact_path(
    *,
    root_path: Path,
    output_template: str,
    as_of_date: date,
) -> Path:
    rendered = output_template.replace("{as_of}", as_of_date.isoformat())
    rendered = _ARTIFACT_PLACEHOLDER_RE.sub("validation", rendered)
    rendered_path = Path(rendered)
    if rendered_path.is_absolute():
        return rendered_path
    return root_path / rendered_path


def _build_operations_command_graph(
    config: ETFOperationsScheduleConfig | None,
    *,
    cadence: OperationsGraphCadence,
    include_optional: bool,
    skipped_optional_step_ids: set[str] | None,
) -> ETFOperationsCommandGraph:
    schedule = config or load_operations_schedule_config()
    pipeline_field = _GRAPH_PIPELINE_FIELD_BY_CADENCE[cadence]
    cadence_steps = tuple(getattr(schedule, pipeline_field))
    cadence_step_by_id = {step.step_id: step for step in cadence_steps}
    cadence_ids = set(cadence_step_by_id)
    all_step_by_id = schedule.step_by_id()

    required_node_ids = _REQUIRED_OPERATION_NODE_IDS_BY_CADENCE[cadence]
    missing_required = sorted(required_node_ids - cadence_ids)
    if missing_required:
        missing = ", ".join(missing_required)
        raise OperationsCommandGraphError(
            f"{cadence} operations graph missing required nodes: {missing}"
        )

    optional_node_ids = _OPTIONAL_OPERATION_NODE_IDS_BY_CADENCE[cadence]
    must_be_required_node_ids = required_node_ids - optional_node_ids
    unexpected_required = sorted(
        step_id for step_id in must_be_required_node_ids if not cadence_step_by_id[step_id].required
    )
    if unexpected_required:
        unexpected = ", ".join(unexpected_required)
        raise OperationsCommandGraphError(
            f"{cadence} operations graph required nodes must be marked required: " f"{unexpected}"
        )

    skipped = set(skipped_optional_step_ids or set())
    if not include_optional:
        skipped.update(step.step_id for step in cadence_steps if not step.required)
    _validate_optional_skips(
        skipped=skipped,
        step_by_id=cadence_step_by_id,
        cadence=cadence,
    )

    selected_steps = tuple(step for step in cadence_steps if step.step_id not in skipped)
    selected_ids = {step.step_id for step in selected_steps}
    dependencies_by_id: dict[str, tuple[str, ...]] = {}
    external_dependencies_by_id: dict[str, tuple[str, ...]] = {}
    for step in selected_steps:
        dependencies, external_dependencies = _graph_dependencies(
            step,
            selected_ids=selected_ids,
            skipped_ids=skipped,
            selected_step_by_id=cadence_step_by_id,
            all_step_by_id=all_step_by_id,
            cadence=cadence,
        )
        dependencies_by_id[step.step_id] = dependencies
        external_dependencies_by_id[step.step_id] = external_dependencies

    execution_order = _topological_order(selected_steps, dependencies_by_id, cadence=cadence)
    ordered_steps = [cadence_step_by_id[step_id] for step_id in execution_order]
    runtime_class_by_step_id = _RUNTIME_CLASS_BY_CADENCE[cadence]
    nodes = [
        _graph_node(
            step,
            dependencies=dependencies_by_id[step.step_id],
            external_dependencies=external_dependencies_by_id[step.step_id],
            all_step_by_id=all_step_by_id,
            runtime_class_by_step_id=runtime_class_by_step_id,
            safety=schedule.safety,
        )
        for step in ordered_steps
    ]
    external_dependencies = sorted(
        {
            dependency_id
            for dependencies in external_dependencies_by_id.values()
            for dependency_id in dependencies
        }
    )
    return ETFOperationsCommandGraph(
        cadence=cadence,
        dry_run_only=True,
        commands_executed=False,
        execution_order=execution_order,
        skipped_optional_steps=sorted(skipped),
        external_dependencies=external_dependencies,
        safety=schedule.safety,
        nodes=nodes,
    )


def _normalized_unique_values(values: list[str], *, field_name: str) -> list[str]:
    normalized = [str(value).strip() for value in values]
    if any(not value for value in normalized):
        raise ValueError(f"{field_name} must not contain blank values")
    duplicate_values = sorted({value for value in normalized if normalized.count(value) > 1})
    if duplicate_values:
        duplicates = ", ".join(duplicate_values)
        raise ValueError(f"{field_name} values must be unique: {duplicates}")
    return normalized


def _validate_optional_skips(
    *,
    skipped: set[str],
    step_by_id: dict[str, ETFOperationsScheduleStep],
    cadence: OperationsGraphCadence,
) -> None:
    unknown = sorted(skipped - set(step_by_id))
    if unknown:
        raise OperationsCommandGraphError(
            f"{cadence} operations graph cannot skip unknown optional nodes: "
            f"{', '.join(unknown)}"
        )
    required_skips = sorted(step_id for step_id in skipped if step_by_id[step_id].required)
    if required_skips:
        raise OperationsCommandGraphError(
            f"{cadence} operations graph cannot skip required nodes: "
            f"{', '.join(required_skips)}"
        )


def _graph_dependencies(
    step: ETFOperationsScheduleStep,
    *,
    selected_ids: set[str],
    skipped_ids: set[str],
    selected_step_by_id: dict[str, ETFOperationsScheduleStep],
    all_step_by_id: dict[str, ETFOperationsScheduleStep],
    cadence: OperationsGraphCadence,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    dependencies: list[str] = []
    external_dependencies: list[str] = []
    for dependency_id in step.dependencies:
        dependency = all_step_by_id.get(dependency_id)
        if dependency is None:
            raise OperationsCommandGraphError(
                f"{cadence} operations graph node {step.step_id} references unknown "
                f"dependency: {dependency_id}"
            )
        if dependency_id in selected_ids:
            dependencies.append(dependency_id)
            continue
        if dependency_id in skipped_ids and dependency_id in selected_step_by_id:
            if not dependency.required:
                continue
        if dependency_id not in selected_step_by_id:
            external_dependencies.append(dependency_id)
            continue
        raise OperationsCommandGraphError(
            f"{cadence} operations graph node {step.step_id} has unavailable dependency: "
            f"{dependency_id}"
        )
    return tuple(dependencies), tuple(external_dependencies)


def _topological_order(
    steps: tuple[ETFOperationsScheduleStep, ...],
    dependencies_by_id: dict[str, tuple[str, ...]],
    *,
    cadence: OperationsGraphCadence,
) -> list[str]:
    remaining = {step.step_id for step in steps}
    ordered: list[str] = []
    while remaining:
        ready = [
            step.step_id
            for step in steps
            if step.step_id in remaining
            and all(dependency not in remaining for dependency in dependencies_by_id[step.step_id])
        ]
        if not ready:
            cycle_nodes = ", ".join(sorted(remaining))
            raise OperationsCommandGraphError(
                f"{cadence} operations graph dependency cycle detected: {cycle_nodes}"
            )
        ordered.extend(ready)
        remaining.difference_update(ready)
    return ordered


def _graph_node(
    step: ETFOperationsScheduleStep,
    *,
    dependencies: tuple[str, ...],
    external_dependencies: tuple[str, ...],
    all_step_by_id: dict[str, ETFOperationsScheduleStep],
    runtime_class_by_step_id: dict[str, OperationsRuntimeClass],
    safety: ETFOperationsScheduleSafety,
) -> ETFOperationsCommandGraphNode:
    inputs: list[str] = []
    for dependency_id in external_dependencies:
        inputs.extend(all_step_by_id[dependency_id].expected_outputs)
    for dependency_id in dependencies:
        inputs.extend(all_step_by_id[dependency_id].expected_outputs)
    return ETFOperationsCommandGraphNode(
        node_id=step.step_id,
        command=step.command,
        inputs=_normalized_unique_values(inputs, field_name=f"{step.step_id}.inputs"),
        outputs=list(step.expected_outputs),
        dependencies=list(dependencies),
        external_dependencies=list(external_dependencies),
        required=step.required,
        failure_policy=step.failure_policy,
        estimated_runtime_class=_runtime_class(
            step,
            runtime_class_by_step_id=runtime_class_by_step_id,
        ),
        owner_review_required=step.owner_review_required,
        safety=safety,
    )


def _runtime_class(
    step: ETFOperationsScheduleStep,
    *,
    runtime_class_by_step_id: dict[str, OperationsRuntimeClass],
) -> OperationsRuntimeClass:
    configured = runtime_class_by_step_id.get(step.step_id)
    if configured is not None:
        return configured
    if "weight-calibration search" in step.command:
        return "slow"
    if step.command.startswith("manual_review:"):
        return "fast"
    return "medium"


def _failure_policy_event_for_artifact(
    artifact: ETFOperationsArtifactStatus,
    *,
    step_by_id: dict[str, ETFOperationsScheduleStep],
) -> ETFOperationsFailurePolicyEvent | None:
    event_type = _failure_event_type(artifact)
    if event_type is None:
        return None

    step = step_by_id.get(artifact.source_step)
    if step is None:
        raise OperationsCommandGraphError(
            "ETF operations failure policy references unknown source step: "
            f"{artifact.source_step}"
        )
    severity = _failure_event_severity(
        artifact=artifact,
        failure_policy=step.failure_policy,
    )
    return ETFOperationsFailurePolicyEvent(
        event_id=f"{artifact.artifact_id}:{event_type}",
        event_type=event_type,
        source_step=artifact.source_step,
        artifact_id=artifact.artifact_id,
        path=artifact.path,
        freshness_status=artifact.freshness_status,
        dependency_status=artifact.dependency_status,
        required=artifact.required,
        failure_policy=step.failure_policy,
        severity=severity,
        blocks_pipeline=_failure_event_blocks_pipeline(step.failure_policy),
        blocks_dependent_steps=_failure_event_blocks_dependent_steps(step.failure_policy),
        requires_manual_review=step.failure_policy == "manual_review_required",
        blocking_dependencies=list(artifact.blocking_dependencies),
        recommended_action=_failure_event_recommended_action(step.failure_policy),
    )


def _failure_event_type(
    artifact: ETFOperationsArtifactStatus,
) -> OperationsFailureEventType | None:
    if artifact.blocking_dependencies:
        return "dependency_blocked"
    if artifact.freshness_status == "missing":
        return "artifact_missing"
    if artifact.freshness_status == "stale":
        return "artifact_stale"
    if artifact.freshness_status == "unknown":
        return "artifact_unknown"
    return None


def _failure_event_severity(
    *,
    artifact: ETFOperationsArtifactStatus,
    failure_policy: OperationsFailurePolicy,
) -> OperationsFailureSeverity:
    if failure_policy == "fail_pipeline":
        return "critical"
    if failure_policy in {"block_dependent_steps", "manual_review_required"}:
        return "error"
    if failure_policy in {"continue_with_warning", "skip_optional_step"}:
        return "warning"
    if not artifact.required:
        return "warning"
    return "info"


def _failure_event_blocks_pipeline(
    failure_policy: OperationsFailurePolicy,
) -> bool:
    return failure_policy == "fail_pipeline"


def _failure_event_blocks_dependent_steps(
    failure_policy: OperationsFailurePolicy,
) -> bool:
    return failure_policy in {"fail_pipeline", "block_dependent_steps"}


def _failure_event_recommended_action(
    failure_policy: OperationsFailurePolicy,
) -> str:
    return {
        "continue": "continue",
        "continue_with_warning": "continue_with_warning",
        "skip_optional_step": "skip_optional_step_and_warn",
        "block_dependent_steps": "block_dependent_steps_until_artifact_recovers",
        "fail_pipeline": "stop_pipeline_until_artifact_recovers",
        "manual_review_required": "request_owner_manual_review",
    }[failure_policy]


def _failure_policy_pipeline_status(
    events: list[ETFOperationsFailurePolicyEvent],
) -> OperationsPipelineStatus:
    if any(event.blocks_pipeline or event.blocks_dependent_steps for event in events):
        return "blocked"
    if any(event.requires_manual_review for event in events):
        return "manual_review_required"
    if any(event.severity in {"warning", "error"} for event in events):
        return "warning"
    return "pass"


def _validate_owner_checklist_failure_report(
    *,
    cadence: OperationsOwnerReviewCadence,
    as_of_date: date,
    failure_report: ETFOperationsFailurePolicyReport | None,
) -> None:
    if failure_report is None:
        return
    if failure_report.as_of_date != as_of_date:
        raise OperationsCommandGraphError(
            "ETF operations owner review checklist as_of date must match "
            "failure report as_of date"
        )
    if cadence != "incident" and failure_report.cadence != cadence:
        raise OperationsCommandGraphError(
            "ETF operations owner review checklist cadence must match " "failure report cadence"
        )


def _owner_review_step_for_cadence(
    schedule: ETFOperationsScheduleConfig,
    cadence: OperationsOwnerReviewCadence,
) -> ETFOperationsScheduleStep:
    matching_steps = [step for step in schedule.manual_review_steps if step.cadence == cadence]
    if len(matching_steps) != 1:
        raise OperationsCommandGraphError(
            "ETF operations schedule must define exactly one owner review step "
            f"for cadence: {cadence}"
        )
    return matching_steps[0]


def _owner_review_safety_item() -> ETFOperationsOwnerReviewChecklistItem:
    return ETFOperationsOwnerReviewChecklistItem(
        item_id="safety:operations_boundary",
        category="safety_boundary",
        title="Confirm observe-only operations boundary",
        description=(
            "Confirm observe_only=true, candidate_only=true, production_effect=none, "
            "broker_action=none, and manual_review_required=true before using outputs."
        ),
        required=True,
        blocking=False,
        owner_action="confirm_no_production_or_broker_action",
    )


def _owner_review_cadence_item(
    cadence: OperationsOwnerReviewCadence,
) -> ETFOperationsOwnerReviewChecklistItem:
    guidance = {
        "daily": (
            "Confirm daily data quality and report readiness",
            "Review daily data quality, report index, Reader Brief, and operations health "
            "before treating daily outputs as reviewable.",
            "confirm_daily_quality_gate_and_report_readiness",
        ),
        "weekly": (
            "Confirm weekly review prerequisites",
            "Review daily upstream artifacts, weekly review package, decision journal, "
            "parameter review, watchlist, and weekly operations report.",
            "confirm_weekly_prerequisites_and_owner_review_scope",
        ),
        "monthly": (
            "Confirm monthly governance prerequisites",
            "Review monthly data quality audit, bounded weight calibration, parameter "
            "governance, strategy evidence dashboard, and monthly operations report.",
            "confirm_monthly_governance_and_slow_cadence_scope",
        ),
        "incident": (
            "Confirm incident review scope",
            "Review the critical blocker, impacted dependent steps, operator notes, "
            "and recovery evidence without applying production changes.",
            "confirm_incident_scope_and_recovery_boundary",
        ),
    }[cadence]
    return ETFOperationsOwnerReviewChecklistItem(
        item_id=f"cadence:{cadence}:entry_gate",
        category="cadence_gate",
        title=guidance[0],
        description=guidance[1],
        required=True,
        blocking=False,
        owner_action=guidance[2],
    )


def _owner_review_summary_item(
    failure_report: ETFOperationsFailurePolicyReport | None,
) -> ETFOperationsOwnerReviewChecklistItem:
    if failure_report is None:
        description = (
            "No failure policy report was attached; review latest operations artifacts "
            "and record the source evidence before signoff."
        )
        owner_action = "review_latest_operations_artifacts_before_signoff"
    else:
        description = (
            "Review failure policy summary with pipeline_status="
            f"{failure_report.pipeline_status}, blocking_events="
            f"{len(failure_report.blocking_events)}, warning_events="
            f"{len(failure_report.warning_events)}, manual_review_events="
            f"{len(failure_report.manual_review_events)}."
        )
        owner_action = "review_failure_policy_summary"
    return ETFOperationsOwnerReviewChecklistItem(
        item_id="summary:failure_policy",
        category="summary_review",
        title="Review operations failure summary",
        description=description,
        required=True,
        blocking=False,
        owner_action=owner_action,
    )


def _owner_review_failure_event_item(
    event: ETFOperationsFailurePolicyEvent,
) -> ETFOperationsOwnerReviewChecklistItem:
    category = _owner_review_event_category(event)
    return ETFOperationsOwnerReviewChecklistItem(
        item_id=f"event:{event.event_id}",
        category=category,
        title=f"Review {event.source_step} {event.event_type}",
        description=(
            f"Artifact {event.artifact_id} has freshness_status="
            f"{event.freshness_status}, severity={event.severity}, "
            f"failure_policy={event.failure_policy}."
        ),
        required=True,
        blocking=event.blocks_pipeline or event.blocks_dependent_steps,
        owner_action=event.recommended_action,
        source_step=event.source_step,
        related_event_ids=[event.event_id],
        related_artifact_ids=[event.artifact_id],
        evidence_paths=[event.path],
    )


def _owner_review_event_category(
    event: ETFOperationsFailurePolicyEvent,
) -> OperationsOwnerChecklistItemCategory:
    if event.blocks_pipeline or event.blocks_dependent_steps:
        return "blocking_event"
    if event.requires_manual_review:
        return "manual_review_event"
    if event.severity == "warning":
        return "warning_event"
    return "summary_review"


def _owner_review_signoff_item(
    cadence: OperationsOwnerReviewCadence,
    failure_report: ETFOperationsFailurePolicyReport | None,
) -> ETFOperationsOwnerReviewChecklistItem:
    blocking = failure_report is not None and failure_report.pipeline_status == "blocked"
    if blocking:
        owner_action = "record_blocker_and_do_not_approve_dependent_outputs"
        description = (
            "Pipeline or dependent-step blockers remain open; owner must record the "
            "blocker and recovery condition before any downstream interpretation."
        )
    elif failure_report is not None and failure_report.pipeline_status == "manual_review_required":
        owner_action = "complete_manual_review_before_downstream_interpretation"
        description = "Manual review is required before downstream interpretation or report use."
    else:
        owner_action = "record_owner_signoff_or_limitations"
        description = (
            "Record owner signoff, warning acknowledgements, limitations, and next "
            "responsible party for this cadence."
        )
    return ETFOperationsOwnerReviewChecklistItem(
        item_id=f"signoff:{cadence}:owner",
        category="owner_signoff",
        title="Record owner signoff",
        description=description,
        required=True,
        blocking=blocking,
        owner_action=owner_action,
    )


def _owner_review_checklist_status(
    cadence: OperationsOwnerReviewCadence,
    failure_report: ETFOperationsFailurePolicyReport | None,
) -> OperationsOwnerChecklistStatus:
    if failure_report is None:
        return "manual_review_required" if cadence == "incident" else "ready"
    if failure_report.pipeline_status == "blocked":
        return "blocked"
    if failure_report.pipeline_status == "manual_review_required":
        return "manual_review_required"
    return "ready"


def _coerce_graph_cadence(cadence: str) -> OperationsGraphCadence:
    if cadence not in _GRAPH_PIPELINE_FIELD_BY_CADENCE:
        valid = ", ".join(sorted(_GRAPH_PIPELINE_FIELD_BY_CADENCE))
        raise OperationsCommandGraphError(
            f"invalid operations graph cadence: {cadence!r}; expected one of {valid}"
        )
    return cast(OperationsGraphCadence, cadence)


def _operations_dry_run_id(
    *,
    cadence: OperationsGraphCadence,
    as_of_date: date,
    generated_at: datetime,
) -> str:
    timestamp = generated_at.strftime("%Y%m%dT%H%M%SZ")
    return f"{cadence}:{as_of_date.isoformat()}:{timestamp}"


def _dry_run_owner_checklist(
    *,
    cadence: OperationsGraphCadence,
    as_of_date: date,
    failure_report: ETFOperationsFailurePolicyReport,
    config: ETFOperationsScheduleConfig,
    generated_at: datetime,
) -> ETFOperationsOwnerReviewChecklist | None:
    if cadence not in {"daily", "weekly", "monthly"}:
        return None
    return build_operations_owner_review_checklist(
        cadence=cadence,
        as_of=as_of_date,
        failure_report=failure_report,
        config=config,
        generated_at=generated_at,
    )


def _operations_dry_run_report_from_components(
    *,
    cadence: OperationsGraphCadence,
    as_of_date: date,
    generated_at: datetime,
    graph: ETFOperationsCommandGraph,
    freshness: ETFOperationsArtifactFreshnessReport,
    failure_report: ETFOperationsFailurePolicyReport,
    owner_checklist: ETFOperationsOwnerReviewChecklist | None,
) -> ETFOperationsSchedulerDryRunReport:
    planned_steps = _dry_run_planned_steps(
        graph=graph,
        freshness_report=freshness,
        failure_report=failure_report,
    )
    return ETFOperationsSchedulerDryRunReport(
        dry_run_id=_operations_dry_run_id(
            cadence=cadence,
            as_of_date=as_of_date,
            generated_at=generated_at,
        ),
        cadence=cadence,
        as_of_date=as_of_date,
        generated_at=generated_at,
        read_only=True,
        dry_run_only=True,
        commands_executed=False,
        production_state_mutated=False,
        safety=graph.safety,
        status=_operations_dry_run_status(
            failure_report=failure_report,
            owner_checklist=owner_checklist,
        ),
        planned_steps=planned_steps,
        execution_order=list(graph.execution_order),
        skipped_optional_steps=list(graph.skipped_optional_steps),
        blocking_failures=list(failure_report.blocking_events),
        warnings=list(failure_report.warning_events),
        expected_outputs=_normalized_unique_values(
            [output for node in graph.nodes for output in node.outputs],
            field_name="operations_scheduler_dry_run.expected_outputs",
        ),
        source_graph_schema_version=graph.schema_version,
        source_freshness_schema_version=freshness.schema_version,
        source_failure_policy_schema_version=failure_report.schema_version,
        owner_checklist_schema_version=(
            owner_checklist.schema_version if owner_checklist is not None else None
        ),
        owner_checklist_status=(
            owner_checklist.checklist_status if owner_checklist is not None else None
        ),
        owner_checklist_item_count=(
            len(owner_checklist.items) if owner_checklist is not None else 0
        ),
    )


def _operations_dry_run_status(
    *,
    failure_report: ETFOperationsFailurePolicyReport,
    owner_checklist: ETFOperationsOwnerReviewChecklist | None,
) -> OperationsSchedulerDryRunStatus:
    if failure_report.pipeline_status == "blocked":
        return "blocked"
    if owner_checklist is not None and owner_checklist.checklist_status == "blocked":
        return "blocked"
    if failure_report.pipeline_status == "manual_review_required":
        return "manual_review_required"
    if owner_checklist is not None and owner_checklist.checklist_status == "manual_review_required":
        return "manual_review_required"
    if failure_report.pipeline_status == "warning":
        return "warning"
    return "ready"


def _operations_health_report_id(
    *,
    cadence: OperationsGraphCadence,
    as_of_date: date,
    generated_at: datetime,
) -> str:
    timestamp = generated_at.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"operations_health:{cadence}:{as_of_date.isoformat()}:{timestamp}"


def _operations_health_status(
    dry_run_status: OperationsSchedulerDryRunStatus,
) -> OperationsHealthReportStatus:
    if dry_run_status == "ready":
        return "pass"
    return dry_run_status


def _operations_safety_banner(
    safety: ETFOperationsScheduleSafety,
) -> dict[str, Any]:
    return {field: getattr(safety, field) for field in OPERATIONS_SCHEDULE_SAFETY}


def _operations_report_run_metadata(
    *,
    graph: ETFOperationsCommandGraph,
    dry_run: ETFOperationsSchedulerDryRunReport,
    root_path: Path | str,
) -> dict[str, Any]:
    return {
        "root_path": str(Path(root_path)),
        "dry_run_id": dry_run.dry_run_id,
        "dry_run_status": dry_run.status,
        "planned_step_count": len(dry_run.planned_steps),
        "blocking_failure_count": len(dry_run.blocking_failures),
        "warning_count": len(dry_run.warnings),
        "owner_checklist_status": dry_run.owner_checklist_status,
        "dry_run_only": dry_run.dry_run_only,
        "commands_executed": dry_run.commands_executed,
        "production_state_mutated": dry_run.production_state_mutated,
        "external_dependency_count": len(graph.external_dependencies),
    }


def _operations_report_pipeline_schedule(
    graph: ETFOperationsCommandGraph,
    dry_run: ETFOperationsSchedulerDryRunReport,
) -> list[dict[str, Any]]:
    step_status_by_id = {step.step_id: step.status for step in dry_run.planned_steps}
    return [
        {
            "step_id": node.node_id,
            "command": node.command,
            "required": node.required,
            "status": step_status_by_id.get(node.node_id, "planned"),
            "dependencies": list(node.dependencies),
            "external_dependencies": list(node.external_dependencies),
            "expected_outputs": list(node.outputs),
            "failure_policy": node.failure_policy,
            "estimated_runtime_class": node.estimated_runtime_class,
            "owner_review_required": node.owner_review_required,
        }
        for node in graph.nodes
    ]


def _operations_report_command_graph_summary(
    graph: ETFOperationsCommandGraph,
) -> dict[str, Any]:
    return {
        "schema_version": graph.schema_version,
        "cadence": graph.cadence,
        "node_count": len(graph.nodes),
        "required_step_count": sum(1 for node in graph.nodes if node.required),
        "optional_step_count": sum(1 for node in graph.nodes if not node.required),
        "owner_review_required_step_count": sum(
            1 for node in graph.nodes if node.owner_review_required
        ),
        "execution_order": list(graph.execution_order),
        "skipped_optional_steps": list(graph.skipped_optional_steps),
        "external_dependencies": list(graph.external_dependencies),
        "dry_run_only": graph.dry_run_only,
        "commands_executed": graph.commands_executed,
    }


def _operations_report_artifact_summary(
    freshness: ETFOperationsArtifactFreshnessReport,
) -> dict[str, Any]:
    return {
        "schema_version": freshness.schema_version,
        "artifact_count": len(freshness.artifacts),
        "blocking_artifact_count": len(freshness.blocking_artifacts),
        "warning_artifact_count": len(freshness.warning_artifacts),
        "optional_artifact_count": len(freshness.optional_artifacts),
        "freshness_summary": dict(freshness.freshness_summary),
    }


def _operations_report_dependency_status(
    freshness: ETFOperationsArtifactFreshnessReport,
    graph: ETFOperationsCommandGraph,
) -> dict[str, Any]:
    return {
        "dependency_summary": dict(freshness.dependency_summary),
        "blocking_artifacts": list(freshness.blocking_artifacts),
        "warning_artifacts": list(freshness.warning_artifacts),
        "external_dependencies": list(graph.external_dependencies),
    }


def _operations_report_failure_rows(
    failure_report: ETFOperationsFailurePolicyReport,
    *,
    include_warning: bool,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in failure_report.events:
        if include_warning and event.severity != "warning":
            continue
        if not include_warning and event.severity == "warning":
            continue
        rows.append(
            {
                "event_id": event.event_id,
                "event_type": event.event_type,
                "severity": event.severity,
                "source_step": event.source_step,
                "artifact_id": event.artifact_id,
                "path": event.path,
                "freshness_status": event.freshness_status,
                "dependency_status": event.dependency_status,
                "required": event.required,
                "failure_policy": event.failure_policy,
                "blocks_pipeline": event.blocks_pipeline,
                "blocks_dependent_steps": event.blocks_dependent_steps,
                "requires_manual_review": event.requires_manual_review,
                "recommended_action": event.recommended_action,
            }
        )
    return rows


def _operations_report_owner_checklist(
    owner_checklist: ETFOperationsOwnerReviewChecklist | None,
) -> dict[str, Any] | None:
    if owner_checklist is None:
        return None
    return {
        "schema_version": owner_checklist.schema_version,
        "checklist_step_id": owner_checklist.checklist_step_id,
        "checklist_command": owner_checklist.checklist_command,
        "checklist_status": owner_checklist.checklist_status,
        "signoff_required": owner_checklist.signoff_required,
        "required_items": list(owner_checklist.required_items),
        "blocking_items": list(owner_checklist.blocking_items),
        "warning_items": list(owner_checklist.warning_items),
        "manual_review_items": list(owner_checklist.manual_review_items),
        "items": [
            {
                "item_id": item.item_id,
                "category": item.category,
                "title": item.title,
                "required": item.required,
                "blocking": item.blocking,
                "owner_action": item.owner_action,
                "source_step": item.source_step,
                "related_event_ids": list(item.related_event_ids),
                "related_artifact_ids": list(item.related_artifact_ids),
                "evidence_paths": list(item.evidence_paths),
            }
            for item in owner_checklist.items
        ],
    }


def _operations_expected_next_run(
    cadence: OperationsGraphCadence,
) -> dict[str, Any]:
    rules = {
        "daily": "next unified daily scheduler trigger after the next completed U.S. trading day",
        "weekly": "weekly operator review after documented due-cadence condition is met",
        "biweekly": "biweekly owner review through the documented operations runbook path",
        "monthly": "monthly governance review after month-end due-cadence condition is met",
    }
    return {
        "cadence": cadence,
        "rule": rules[cadence],
        "source": "docs/operations/operations_runbook.md",
        "production_scheduler_entry": "aits ops daily-run",
        "separate_external_scheduler_entry": False,
    }


def _operations_report_source_artifacts(
    freshness: ETFOperationsArtifactFreshnessReport,
) -> list[ETFOperationsHealthSourceArtifact]:
    return [
        ETFOperationsHealthSourceArtifact(
            artifact_id=artifact.artifact_id,
            path=artifact.path,
            artifact_type=artifact.artifact_type,
            source_step=artifact.source_step,
            required=artifact.required,
            freshness_status=artifact.freshness_status,
            dependency_status=artifact.dependency_status,
            generated_at=artifact.generated_at,
            as_of_date=artifact.as_of_date,
            age_days=artifact.age_days,
        )
        for artifact in freshness.artifacts
    ]


def _markdown_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "none"
    if isinstance(value, list):
        return _markdown_list(value)
    if isinstance(value, dict):
        return f"`{json.dumps(value, sort_keys=True, ensure_ascii=False)}`"
    return f"`{value}`"


def _json_for_markdown(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False).replace("`", "'")


def _markdown_list(values: list[Any]) -> str:
    if not values:
        return "none"
    return ", ".join(f"`{value}`" for value in values)


def _dry_run_planned_steps(
    *,
    graph: ETFOperationsCommandGraph,
    freshness_report: ETFOperationsArtifactFreshnessReport,
    failure_report: ETFOperationsFailurePolicyReport,
) -> list[ETFOperationsDryRunStep]:
    artifacts_by_step: dict[str, list[ETFOperationsArtifactStatus]] = {}
    for artifact in freshness_report.artifacts:
        artifacts_by_step.setdefault(artifact.source_step, []).append(artifact)
    events_by_step: dict[str, list[ETFOperationsFailurePolicyEvent]] = {}
    for event in failure_report.events:
        events_by_step.setdefault(event.source_step, []).append(event)

    return [
        _dry_run_planned_step(
            node,
            artifacts=artifacts_by_step.get(node.node_id, []),
            events=events_by_step.get(node.node_id, []),
        )
        for node in graph.nodes
    ]


def _dry_run_planned_step(
    node: ETFOperationsCommandGraphNode,
    *,
    artifacts: list[ETFOperationsArtifactStatus],
    events: list[ETFOperationsFailurePolicyEvent],
) -> ETFOperationsDryRunStep:
    return ETFOperationsDryRunStep(
        step_id=node.node_id,
        command=node.command,
        status=_dry_run_step_status(events),
        dependencies=list(node.dependencies),
        external_dependencies=list(node.external_dependencies),
        required=node.required,
        failure_policy=node.failure_policy,
        estimated_runtime_class=node.estimated_runtime_class,
        owner_review_required=node.owner_review_required,
        expected_outputs=list(node.outputs),
        related_artifact_ids=[artifact.artifact_id for artifact in artifacts],
        blocking_event_ids=[
            event.event_id
            for event in events
            if event.blocks_pipeline or event.blocks_dependent_steps
        ],
        warning_event_ids=[event.event_id for event in events if event.severity == "warning"],
        manual_review_event_ids=[
            event.event_id for event in events if event.requires_manual_review
        ],
        command_would_execute=True,
        command_executed=False,
    )


def _dry_run_step_status(
    events: list[ETFOperationsFailurePolicyEvent],
) -> OperationsDryRunStepStatus:
    if any(event.blocks_pipeline or event.blocks_dependent_steps for event in events):
        return "blocked"
    if any(event.requires_manual_review for event in events):
        return "manual_review_required"
    if any(event.severity == "warning" for event in events):
        return "warning"
    return "planned"


def _artifact_source_step_order(graph: ETFOperationsCommandGraph) -> tuple[str, ...]:
    ordered: list[str] = []
    for step_id in [*graph.external_dependencies, *graph.execution_order]:
        if step_id not in ordered:
            ordered.append(step_id)
    return tuple(ordered)


def _artifact_dependency_steps_by_id(
    graph: ETFOperationsCommandGraph,
) -> dict[str, tuple[str, ...]]:
    return {
        node.node_id: tuple([*node.dependencies, *node.external_dependencies])
        for node in graph.nodes
    }


def _artifact_status_for_output(
    step: ETFOperationsScheduleStep,
    *,
    output_template: str,
    output_index: int,
    root_path: Path,
    requested_as_of: date,
    dependency_steps: tuple[str, ...],
) -> ETFOperationsArtifactStatus:
    artifact_id = f"{step.step_id}:{output_index}"
    if output_template.startswith("manual_review_checklist:"):
        return ETFOperationsArtifactStatus(
            artifact_id=artifact_id,
            path=output_template,
            artifact_type="manual_review_checklist",
            source_step=step.step_id,
            as_of_date=None,
            generated_at=None,
            max_allowed_age=step.max_allowed_age,
            required=step.required,
            freshness_status="not_applicable",
            dependency_status="optional",
            dependency_steps=list(dependency_steps),
            blocking_dependencies=[],
            age_days=None,
        )

    artifact_path = _resolve_artifact_path(
        output_template,
        requested_as_of=requested_as_of,
        root_path=root_path,
    )
    exists = artifact_path.exists()
    generated_at = _artifact_generated_at(artifact_path) if exists else None
    artifact_as_of = _artifact_as_of_date(artifact_path) if exists else None
    freshness_status, age_days = _artifact_freshness_status(
        exists=exists,
        requested_as_of=requested_as_of,
        artifact_as_of=artifact_as_of,
        generated_at=generated_at,
        max_allowed_age=step.max_allowed_age,
    )
    return ETFOperationsArtifactStatus(
        artifact_id=artifact_id,
        path=str(artifact_path),
        artifact_type=_artifact_type(artifact_path),
        source_step=step.step_id,
        as_of_date=artifact_as_of,
        generated_at=generated_at,
        max_allowed_age=step.max_allowed_age,
        required=step.required,
        freshness_status=freshness_status,
        dependency_status=_artifact_dependency_status(
            required=step.required,
            freshness_status=freshness_status,
        ),
        dependency_steps=list(dependency_steps),
        blocking_dependencies=[],
        age_days=age_days,
    )


def _resolve_artifact_path(
    output_template: str,
    *,
    requested_as_of: date,
    root_path: Path,
) -> Path:
    rendered = output_template.replace("{as_of}", requested_as_of.isoformat())
    pattern = _ARTIFACT_PLACEHOLDER_RE.sub("*", rendered)
    pattern_path = Path(pattern)
    if not pattern_path.is_absolute():
        pattern_path = root_path / pattern_path
    if "*" not in str(pattern_path):
        return pattern_path
    matches = [Path(match) for match in glob(str(pattern_path))]
    if not matches:
        return pattern_path
    return max(matches, key=lambda candidate: candidate.stat().st_mtime)


def _artifact_generated_at(path: Path) -> datetime | None:
    payload = _artifact_json_payload(path)
    if payload is not None:
        for field_name in ("generated_at", "created_at", "updated_at"):
            parsed = _parse_datetime_value(payload.get(field_name))
            if parsed is not None:
                return parsed

    text = _artifact_text(path)
    if text is not None:
        match = _TEXT_GENERATED_AT_RE.search(text)
        if match is not None:
            parsed = _parse_datetime_value(match.group("value"))
            if parsed is not None:
                return parsed

    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except OSError:
        return None


def _artifact_as_of_date(path: Path) -> date | None:
    payload = _artifact_json_payload(path)
    if payload is not None:
        for field_name in ("as_of_date", "as_of", "date", "requested_as_of"):
            parsed = _parse_date_value(payload.get(field_name))
            if parsed is not None:
                return parsed

    text = _artifact_text(path)
    if text is not None:
        match = _TEXT_AS_OF_RE.search(text)
        if match is not None:
            parsed = _parse_date_value(match.group("value"))
            if parsed is not None:
                return parsed

    match = _ARTIFACT_DATE_RE.search(path.name)
    if match is not None:
        return _parse_date_value(match.group("date"))
    return None


def _artifact_json_payload(path: Path) -> dict[str, Any] | None:
    if path.suffix.lower() != ".json":
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _artifact_text(path: Path) -> str | None:
    if path.suffix.lower() == ".json":
        return None
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None


def _artifact_freshness_status(
    *,
    exists: bool,
    requested_as_of: date,
    artifact_as_of: date | None,
    generated_at: datetime | None,
    max_allowed_age: int,
) -> tuple[OperationsArtifactFreshnessStatus, int | None]:
    if not exists:
        return "missing", None
    reference_date = artifact_as_of or (generated_at.date() if generated_at else None)
    if reference_date is None:
        return "unknown", None
    age_days = (requested_as_of - reference_date).days
    if age_days < 0:
        return "unknown", age_days
    if age_days <= max_allowed_age:
        return "fresh", age_days
    return "stale", age_days


def _artifact_dependency_status(
    *,
    required: bool,
    freshness_status: OperationsArtifactFreshnessStatus,
) -> OperationsArtifactDependencyStatus:
    if freshness_status in {"fresh", "not_applicable"}:
        return "optional"
    if required:
        return "blocking"
    return "warning"


def _artifact_has_own_blocking_status(artifact: ETFOperationsArtifactStatus) -> bool:
    return artifact.required and artifact.freshness_status in {
        "missing",
        "stale",
        "unknown",
    }


def _artifact_type(path: Path) -> str:
    suffix = path.suffix.lower().lstrip(".")
    return suffix or "unknown"


def _coerce_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    parsed = _parse_date_value(value)
    if parsed is None:
        raise OperationsCommandGraphError(f"invalid operations as_of date: {value!r}")
    return parsed


def _coerce_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_date_value(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        return None
    clean_value = value.strip().replace("_", "-")
    if not clean_value:
        return None
    try:
        return datetime.fromisoformat(clean_value).date()
    except ValueError:
        try:
            return date.fromisoformat(clean_value[:10])
        except ValueError:
            return None


def _parse_datetime_value(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _coerce_datetime(value)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time(), tzinfo=UTC)
    if not isinstance(value, str):
        return None
    clean_value = value.strip().replace("Z", "+00:00")
    if not clean_value:
        return None
    try:
        parsed = datetime.fromisoformat(clean_value)
    except ValueError:
        return None
    return _coerce_datetime(parsed)


def _status_counts(statuses: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for status in statuses:
        counts[str(status)] = counts.get(str(status), 0) + 1
    return counts


def _status_counts_with_defaults(statuses: Any, *, allowed: tuple[str, ...]) -> dict[str, int]:
    counts = {status: 0 for status in allowed}
    for status in statuses:
        status_key = str(status)
        counts[status_key] = counts.get(status_key, 0) + 1
    return counts
