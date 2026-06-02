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


class OperationsScheduleError(ValueError):
    """Raised when the ETF operations schedule violates workflow requirements."""


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


def _normalized_unique_values(values: list[str], *, field_name: str) -> list[str]:
    normalized = [str(value).strip() for value in values]
    if any(not value for value in normalized):
        raise ValueError(f"{field_name} must not contain blank values")
    duplicate_values = sorted({value for value in normalized if normalized.count(value) > 1})
    if duplicate_values:
        duplicates = ", ".join(duplicate_values)
        raise ValueError(f"{field_name} values must be unique: {duplicates}")
    return normalized
