from __future__ import annotations

from pathlib import Path
from typing import Literal, Self

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
OperationsRuntimeClass = Literal["fast", "medium", "slow"]
OperationsGraphCadence = Literal["daily", "weekly", "biweekly", "monthly"]

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
