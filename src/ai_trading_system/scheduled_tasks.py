from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_SCHEDULED_TASKS_CONFIG_PATH = PROJECT_ROOT / "config" / "scheduled_tasks.yaml"
DAILY_CADENCE_ID = "daily_trading_day"
NON_DAILY_CADENCE_IDS = ("weekly", "biweekly", "monthly", "ad_hoc_research")


class TaskActivationCondition(StrEnum):
    ALWAYS = "always"
    TRADING_DAY_ONLY = "trading_day_only"
    CLOSED_MARKET_ONLY = "closed_market_only"


@dataclass(frozen=True)
class ScheduledTask:
    task_id: str
    title: str
    command: str
    cadence: str
    production_effect: str
    daily_plan_step_id: str | None = None
    command_contains: tuple[str, ...] = ()
    closed_market_behavior: str | None = None
    date_gate: str | None = None
    trigger_condition: str | None = None
    data_quality_gate: str | None = None
    manual_review_required: bool = False
    production_weight_write: bool = False
    active_shadow_weight_write: bool = False
    broker_action: bool = False
    trading_action: bool = False
    max_attempts: int = 1
    activation_condition: TaskActivationCondition = TaskActivationCondition.ALWAYS

    def active_for_session(self, *, is_trading_day: bool) -> bool:
        return (
            self.activation_condition is TaskActivationCondition.ALWAYS
            or (
                self.activation_condition is TaskActivationCondition.TRADING_DAY_ONLY
                and is_trading_day
            )
            or (
                self.activation_condition is TaskActivationCondition.CLOSED_MARKET_ONLY
                and not is_trading_day
            )
        )

    @property
    def is_safety_scoped(self) -> bool:
        haystack = " ".join(
            (
                self.task_id,
                self.title,
                self.command,
            )
        ).lower()
        return any(
            token in haystack
            for token in (
                "reader",
                "governance",
                "shadow-monitor",
                "shadow monitor",
                "shadow-observe",
                "shadow observe",
                "report-contract",
                "report index",
                "score-change-attribution",
                "market-panel",
                "weight candidate",
                "weight promotion",
                "dynamic-v3-rescue",
                "dynamic v3 rescue",
                "dynamic_v3_rescue",
                "parameter research",
            )
        )


@dataclass(frozen=True)
class ScheduledCadence:
    cadence_id: str
    description: str
    tasks: tuple[ScheduledTask, ...]


@dataclass(frozen=True)
class ScheduledTasksConfig:
    schema_version: int
    policy_version: str
    cadences: tuple[ScheduledCadence, ...]
    path: Path

    def cadence(self, cadence_id: str) -> ScheduledCadence:
        for cadence in self.cadences:
            if cadence.cadence_id == cadence_id:
                return cadence
        raise KeyError(f"unknown scheduled cadence: {cadence_id}")

    def tasks_by_id(self) -> dict[str, ScheduledTask]:
        return {task.task_id: task for task in self.tasks()}

    def tasks(self) -> tuple[ScheduledTask, ...]:
        return tuple(task for cadence in self.cadences for task in cadence.tasks)

    def daily_tasks(self, *, is_trading_day: bool | None = None) -> tuple[ScheduledTask, ...]:
        tasks = self.cadence(DAILY_CADENCE_ID).tasks
        if is_trading_day is None:
            return tasks
        return tuple(
            task for task in tasks if task.active_for_session(is_trading_day=is_trading_day)
        )

    def non_daily_tasks(self) -> tuple[ScheduledTask, ...]:
        return tuple(
            task
            for cadence in self.cadences
            if cadence.cadence_id != DAILY_CADENCE_ID
            for task in cadence.tasks
        )


def load_scheduled_tasks_config(
    path: Path | str = DEFAULT_SCHEDULED_TASKS_CONFIG_PATH,
) -> ScheduledTasksConfig:
    config_path = Path(path)
    try:
        raw = safe_load_yaml_path(config_path)
    except (OSError, yaml.YAMLError) as exc:
        raise ValueError(f"scheduled tasks config could not be loaded: {config_path}") from exc
    if not isinstance(raw, dict):
        raise ValueError("scheduled tasks config must be a mapping")
    schema_version = int(raw.get("schema_version") or 0)
    if schema_version != 1:
        raise ValueError("scheduled tasks schema_version must be 1")
    policy_version = str(raw.get("policy_version") or "").strip()
    if not policy_version:
        raise ValueError("scheduled tasks policy_version is required")
    raw_cadences = raw.get("cadences")
    if not isinstance(raw_cadences, dict):
        raise ValueError("scheduled tasks cadences must be a mapping")

    cadences = tuple(
        _load_cadence(cadence_id, raw_cadence) for cadence_id, raw_cadence in raw_cadences.items()
    )
    config = ScheduledTasksConfig(
        schema_version=schema_version,
        policy_version=policy_version,
        cadences=cadences,
        path=config_path,
    )
    _validate_config(config)
    return config


def scheduled_daily_step_ids(
    config: ScheduledTasksConfig, *, is_trading_day: bool | None = None
) -> tuple[str, ...]:
    return tuple(
        task.daily_plan_step_id or ""
        for task in config.daily_tasks(is_trading_day=is_trading_day)
        if task.daily_plan_step_id
    )


def scheduled_non_daily_task_ids(config: ScheduledTasksConfig) -> tuple[str, ...]:
    return tuple(task.task_id for task in config.non_daily_tasks())


def scheduled_safety_issues(config: ScheduledTasksConfig) -> tuple[str, ...]:
    issues: list[str] = []
    for task in config.tasks():
        if not task.is_safety_scoped:
            continue
        if task.production_effect != "none":
            issues.append(f"{task.task_id}: production_effect must be none")
        if task.production_weight_write:
            issues.append(f"{task.task_id}: production_weight_write must be false")
        if task.active_shadow_weight_write:
            issues.append(f"{task.task_id}: active_shadow_weight_write must be false")
        if task.broker_action or task.trading_action:
            issues.append(f"{task.task_id}: broker/trading action must be false")
    return tuple(issues)


def _load_cadence(cadence_id: str, raw: Any) -> ScheduledCadence:
    if not isinstance(raw, dict):
        raise ValueError(f"scheduled cadence {cadence_id} must be a mapping")
    raw_tasks = raw.get("tasks")
    if not isinstance(raw_tasks, list) or not raw_tasks:
        raise ValueError(f"scheduled cadence {cadence_id} must include tasks")
    return ScheduledCadence(
        cadence_id=cadence_id,
        description=str(raw.get("description") or ""),
        tasks=tuple(_load_task(cadence_id, item) for item in raw_tasks),
    )


def _load_task(cadence_id: str, raw: Any) -> ScheduledTask:
    if not isinstance(raw, dict):
        raise ValueError(f"scheduled task in {cadence_id} must be a mapping")
    task_id = str(raw.get("task_id") or "").strip()
    title = str(raw.get("title") or "").strip()
    command = str(raw.get("command") or "").strip()
    if not task_id or not title or not command:
        raise ValueError(f"scheduled task in {cadence_id} is missing task_id/title/command")
    contains = raw.get("command_contains") or ()
    if not isinstance(contains, list | tuple):
        raise ValueError(f"{task_id}: command_contains must be a list")
    return ScheduledTask(
        task_id=task_id,
        title=title,
        command=command,
        cadence=cadence_id,
        production_effect=str(raw.get("production_effect") or "none"),
        daily_plan_step_id=(
            str(raw["daily_plan_step_id"]).strip()
            if raw.get("daily_plan_step_id") is not None
            else None
        ),
        command_contains=tuple(str(token).strip() for token in contains if str(token).strip()),
        closed_market_behavior=(
            str(raw["closed_market_behavior"]).strip()
            if raw.get("closed_market_behavior") is not None
            else None
        ),
        date_gate=_optional_task_text(raw.get("date_gate")),
        trigger_condition=_optional_task_text(raw.get("trigger_condition")),
        data_quality_gate=_optional_task_text(raw.get("data_quality_gate")),
        manual_review_required=bool(raw.get("manual_review_required", False)),
        production_weight_write=bool(raw.get("production_weight_write", False)),
        active_shadow_weight_write=bool(raw.get("active_shadow_weight_write", False)),
        broker_action=bool(raw.get("broker_action", False)),
        trading_action=bool(raw.get("trading_action", False)),
        max_attempts=_positive_task_int(raw.get("max_attempts"), field=f"{task_id}.max_attempts"),
        activation_condition=TaskActivationCondition(
            str(raw.get("activation_condition") or TaskActivationCondition.ALWAYS.value)
        ),
    )


def _optional_task_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _positive_task_int(value: Any, *, field: str) -> int:
    if value is None:
        return 1
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _validate_config(config: ScheduledTasksConfig) -> None:
    cadence_ids = {cadence.cadence_id for cadence in config.cadences}
    missing = {DAILY_CADENCE_ID, *NON_DAILY_CADENCE_IDS} - cadence_ids
    if missing:
        raise ValueError(f"scheduled tasks missing cadences: {', '.join(sorted(missing))}")

    task_ids: set[str] = set()
    duplicate_task_ids: set[str] = set()
    daily_step_ids: set[str] = set()
    duplicate_daily_step_ids: set[str] = set()
    for task in config.tasks():
        if task.task_id in task_ids:
            duplicate_task_ids.add(task.task_id)
        task_ids.add(task.task_id)
        if task.cadence == DAILY_CADENCE_ID:
            if not task.daily_plan_step_id:
                raise ValueError(f"{task.task_id}: daily task requires daily_plan_step_id")
            if task.daily_plan_step_id in daily_step_ids:
                duplicate_daily_step_ids.add(task.daily_plan_step_id)
            daily_step_ids.add(task.daily_plan_step_id)
            if not task.closed_market_behavior:
                raise ValueError(f"{task.task_id}: daily task requires closed_market_behavior")
            if not task.command_contains:
                raise ValueError(f"{task.task_id}: daily task requires command_contains")
        if "dynamic-v3-rescue" in task.command:
            is_daily_schedule_observe = (
                task.cadence == DAILY_CADENCE_ID
                and "dynamic-v3-rescue schedule observe" in task.command
            )
            if task.cadence == DAILY_CADENCE_ID and not is_daily_schedule_observe:
                raise ValueError(
                    f"{task.task_id}: only dynamic-v3-rescue schedule observe may be daily"
                )
            if not task.date_gate:
                raise ValueError(f"{task.task_id}: dynamic-v3-rescue requires date_gate")
            if not task.trigger_condition:
                raise ValueError(f"{task.task_id}: dynamic-v3-rescue requires trigger_condition")
            if not task.data_quality_gate:
                raise ValueError(f"{task.task_id}: dynamic-v3-rescue requires data_quality_gate")
            if not task.manual_review_required:
                raise ValueError(
                    f"{task.task_id}: dynamic-v3-rescue requires manual_review_required"
                )
    if duplicate_task_ids:
        raise ValueError(f"scheduled task ids must be unique: {sorted(duplicate_task_ids)}")
    if duplicate_daily_step_ids:
        raise ValueError(
            "scheduled daily_plan_step_id values must be unique: "
            f"{sorted(duplicate_daily_step_ids)}"
        )

    safety_issues = scheduled_safety_issues(config)
    if safety_issues:
        raise ValueError("scheduled task safety issues: " + "; ".join(safety_issues))
