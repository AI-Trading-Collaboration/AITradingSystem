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
    required: bool
    failure_policy: OperationsFailurePolicy
    estimated_runtime_class: OperationsRuntimeClass
    owner_review_required: bool
    safety: ETFOperationsScheduleSafety


class ETFOperationsCommandGraph(BaseModel):
    schema_version: Literal["etf_operations_command_graph_v1"] = (
        OPERATIONS_COMMAND_GRAPH_SCHEMA_VERSION
    )
    cadence: Literal["daily"]
    dry_run_only: bool = True
    commands_executed: bool = False
    execution_order: list[str] = Field(default_factory=list)
    skipped_optional_steps: list[str] = Field(default_factory=list)
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
    schedule = config or load_operations_schedule_config()
    daily_steps = tuple(schedule.daily_pipeline)
    daily_step_by_id = {step.step_id: step for step in daily_steps}
    daily_ids = set(daily_step_by_id)

    missing_required = sorted(REQUIRED_DAILY_OPERATION_NODE_IDS - daily_ids)
    if missing_required:
        missing = ", ".join(missing_required)
        raise OperationsCommandGraphError(
            f"daily operations graph missing required nodes: {missing}"
        )

    unexpected_required = sorted(
        step_id
        for step_id in REQUIRED_DAILY_OPERATION_NODE_IDS
        if not daily_step_by_id[step_id].required
    )
    if unexpected_required:
        unexpected = ", ".join(unexpected_required)
        raise OperationsCommandGraphError(
            f"daily operations graph required nodes must be marked required: {unexpected}"
        )

    skipped = set(skipped_optional_step_ids or set())
    if not include_optional:
        skipped.update(step.step_id for step in daily_steps if not step.required)
    _validate_optional_skips(skipped=skipped, daily_step_by_id=daily_step_by_id)

    selected_steps = tuple(step for step in daily_steps if step.step_id not in skipped)
    selected_ids = {step.step_id for step in selected_steps}
    dependencies_by_id: dict[str, tuple[str, ...]] = {}
    for step in selected_steps:
        dependencies_by_id[step.step_id] = _daily_graph_dependencies(
            step,
            selected_ids=selected_ids,
            skipped_ids=skipped,
            daily_step_by_id=daily_step_by_id,
        )
    execution_order = _topological_order(selected_steps, dependencies_by_id)
    ordered_steps = [daily_step_by_id[step_id] for step_id in execution_order]
    nodes = [
        _daily_graph_node(
            step,
            dependencies=dependencies_by_id[step.step_id],
            daily_step_by_id=daily_step_by_id,
            safety=schedule.safety,
        )
        for step in ordered_steps
    ]
    return ETFOperationsCommandGraph(
        cadence="daily",
        dry_run_only=True,
        commands_executed=False,
        execution_order=execution_order,
        skipped_optional_steps=sorted(skipped),
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
    daily_step_by_id: dict[str, ETFOperationsScheduleStep],
) -> None:
    unknown = sorted(skipped - set(daily_step_by_id))
    if unknown:
        raise OperationsCommandGraphError(
            "daily operations graph cannot skip unknown optional nodes: "
            f"{', '.join(unknown)}"
        )
    required_skips = sorted(step_id for step_id in skipped if daily_step_by_id[step_id].required)
    if required_skips:
        raise OperationsCommandGraphError(
            "daily operations graph cannot skip required nodes: "
            f"{', '.join(required_skips)}"
        )


def _daily_graph_dependencies(
    step: ETFOperationsScheduleStep,
    *,
    selected_ids: set[str],
    skipped_ids: set[str],
    daily_step_by_id: dict[str, ETFOperationsScheduleStep],
) -> tuple[str, ...]:
    dependencies: list[str] = []
    for dependency_id in step.dependencies:
        dependency = daily_step_by_id.get(dependency_id)
        if dependency is None:
            raise OperationsCommandGraphError(
                f"daily operations graph node {step.step_id} references non-daily "
                f"dependency: {dependency_id}"
            )
        if dependency_id in selected_ids:
            dependencies.append(dependency_id)
            continue
        if dependency_id in skipped_ids and not dependency.required:
            continue
        raise OperationsCommandGraphError(
            f"daily operations graph node {step.step_id} has unavailable dependency: "
            f"{dependency_id}"
        )
    return tuple(dependencies)


def _topological_order(
    steps: tuple[ETFOperationsScheduleStep, ...],
    dependencies_by_id: dict[str, tuple[str, ...]],
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
                f"daily operations graph dependency cycle detected: {cycle_nodes}"
            )
        ordered.extend(ready)
        remaining.difference_update(ready)
    return ordered


def _daily_graph_node(
    step: ETFOperationsScheduleStep,
    *,
    dependencies: tuple[str, ...],
    daily_step_by_id: dict[str, ETFOperationsScheduleStep],
    safety: ETFOperationsScheduleSafety,
) -> ETFOperationsCommandGraphNode:
    inputs: list[str] = []
    for dependency_id in dependencies:
        inputs.extend(daily_step_by_id[dependency_id].expected_outputs)
    return ETFOperationsCommandGraphNode(
        node_id=step.step_id,
        command=step.command,
        inputs=_normalized_unique_values(inputs, field_name=f"{step.step_id}.inputs"),
        outputs=list(step.expected_outputs),
        dependencies=list(dependencies),
        required=step.required,
        failure_policy=step.failure_policy,
        estimated_runtime_class=_daily_runtime_class(step),
        owner_review_required=step.owner_review_required,
        safety=safety,
    )


def _daily_runtime_class(step: ETFOperationsScheduleStep) -> OperationsRuntimeClass:
    configured = _DAILY_RUNTIME_CLASS_BY_STEP_ID.get(step.step_id)
    if configured is not None:
        return configured
    if "weight-calibration search" in step.command:
        return "slow"
    if step.command.startswith("manual_review:"):
        return "fast"
    return "medium"
