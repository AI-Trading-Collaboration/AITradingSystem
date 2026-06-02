from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime
from glob import glob
from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.models import PolicyMetadata
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
OPERATIONS_OWNER_REVIEW_CHECKLIST_SCHEMA_VERSION = (
    "etf_operations_owner_review_checklist_v1"
)
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
                "ETF operations schedule dependencies reference unknown steps: "
                f"{missing}"
            )

        return self

    def pipelines(self) -> dict[str, tuple[ETFOperationsScheduleStep, ...]]:
        return {
            field_name: tuple(getattr(self, field_name))
            for field_name in _PIPELINE_FIELDS
        }

    def steps(self) -> tuple[ETFOperationsScheduleStep, ...]:
        return tuple(
            step
            for field_name in _PIPELINE_FIELDS
            for step in getattr(self, field_name)
        )

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
                    "ETF operations failure policy report safety "
                    f"{field} must be {expected!r}"
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
                    "ETF operations owner review checklist safety "
                    f"{field} must be {expected!r}"
                )
        item_ids = [item.item_id for item in self.items]
        duplicate_item_ids = sorted(
            {item_id for item_id in item_ids if item_ids.count(item_id) > 1}
        )
        if duplicate_item_ids:
            duplicates = ", ".join(duplicate_item_ids)
            raise ValueError(
                "ETF operations owner review checklist item IDs must be unique: "
                f"{duplicates}"
            )
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
        freshness_summary=_status_counts(
            artifact.freshness_status for artifact in artifacts
        ),
        dependency_summary=_status_counts(
            artifact.dependency_status for artifact in artifacts
        ),
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
        warning_events=[
            event.event_id for event in events if event.severity == "warning"
        ],
        manual_review_events=[
            event.event_id for event in events if event.requires_manual_review
        ],
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
        warning_items=[
            item.item_id for item in items if item.category == "warning_event"
        ],
        manual_review_items=[
            item.item_id for item in items if item.category == "manual_review_event"
        ],
    )


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
        step_id
        for step_id in must_be_required_node_ids
        if not cadence_step_by_id[step_id].required
    )
    if unexpected_required:
        unexpected = ", ".join(unexpected_required)
        raise OperationsCommandGraphError(
            f"{cadence} operations graph required nodes must be marked required: "
            f"{unexpected}"
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
            "ETF operations owner review checklist cadence must match "
            "failure report cadence"
        )


def _owner_review_step_for_cadence(
    schedule: ETFOperationsScheduleConfig,
    cadence: OperationsOwnerReviewCadence,
) -> ETFOperationsScheduleStep:
    matching_steps = [
        step for step in schedule.manual_review_steps if step.cadence == cadence
    ]
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
    blocking = (
        failure_report is not None
        and failure_report.pipeline_status == "blocked"
    )
    if blocking:
        owner_action = "record_blocker_and_do_not_approve_dependent_outputs"
        description = (
            "Pipeline or dependent-step blockers remain open; owner must record the "
            "blocker and recovery condition before any downstream interpretation."
        )
    elif (
        failure_report is not None
        and failure_report.pipeline_status == "manual_review_required"
    ):
        owner_action = "complete_manual_review_before_downstream_interpretation"
        description = (
            "Manual review is required before downstream interpretation or report use."
        )
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
