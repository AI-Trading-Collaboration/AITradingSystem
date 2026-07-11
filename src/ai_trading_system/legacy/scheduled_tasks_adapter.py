from __future__ import annotations

from dataclasses import dataclass

from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.workflow import (
    EntrypointRef,
    WorkflowCadence,
    WorkflowSpec,
    WorkflowStepSpec,
)
from ai_trading_system.core.production_effect import ProductionEffect
from ai_trading_system.scheduled_tasks import ScheduledCadence, ScheduledTask

_CADENCE_MAP = {
    "daily_trading_day": WorkflowCadence.DAILY,
    "weekly": WorkflowCadence.WEEKLY,
    "biweekly": WorkflowCadence.BIWEEKLY,
    "monthly": WorkflowCadence.MONTHLY,
    "ad_hoc_research": WorkflowCadence.MANUAL,
}

_LEGACY_EFFECT_MAP = {
    "none": ProductionEffect.NONE,
    "local_cache_write": ProductionEffect.NONE,
    "local_report_write": ProductionEffect.NONE,
}


class LegacyScheduledTaskDispatchBlocked(RuntimeError):
    pass


@dataclass(frozen=True)
class LegacyScheduledWorkflowBinding:
    owner: str
    timezone: str
    due_policy_id: str
    trading_calendar: str | None
    preserve_sequential_order: bool
    is_trading_day: bool | None = None


@dataclass(frozen=True)
class ScheduledWorkflowCompatibilityAssessment:
    cadence_id: str
    status: CanonicalStatus
    blocker_codes: tuple[str, ...]
    workflow_spec: WorkflowSpec | None
    legacy_production_effects: tuple[tuple[str, str, str], ...]


@dataclass(frozen=True)
class DailyShadowParityAssessment:
    status: CanonicalStatus
    blocker_codes: tuple[str, ...]
    expected_step_ids: tuple[str, ...]
    observed_step_ids: tuple[str, ...]
    legacy_only_step_ids: tuple[str, ...]


def assess_scheduled_cadence(
    cadence: ScheduledCadence,
    *,
    binding: LegacyScheduledWorkflowBinding | None,
) -> ScheduledWorkflowCompatibilityAssessment:
    blockers: list[str] = []
    canonical_cadence = _CADENCE_MAP.get(cadence.cadence_id)
    if canonical_cadence is None:
        blockers.append("UNKNOWN_LEGACY_CADENCE")
    if binding is None:
        blockers.extend(
            (
                "DUE_POLICY_BINDING_MISSING",
                "OPERATIONS_OWNER_BINDING_MISSING",
                "TIMEZONE_BINDING_MISSING",
            )
        )

    effect_rows: list[tuple[str, str, str]] = []
    for task in cadence.tasks:
        effect = _LEGACY_EFFECT_MAP.get(task.production_effect)
        if effect is None:
            blockers.append(f"UNKNOWN_LEGACY_PRODUCTION_EFFECT:{task.task_id}")
            canonical = "BLOCKED"
        else:
            canonical = effect.value
        effect_rows.append((task.task_id, task.production_effect, canonical))

    if blockers or binding is None or canonical_cadence is None:
        return ScheduledWorkflowCompatibilityAssessment(
            cadence_id=cadence.cadence_id,
            status=CanonicalStatus.BLOCKED,
            blocker_codes=tuple(sorted(set(blockers))),
            workflow_spec=None,
            legacy_production_effects=tuple(effect_rows),
        )

    selected_tasks = tuple(
        task
        for task in cadence.tasks
        if binding.is_trading_day is None
        or task.active_for_session(is_trading_day=binding.is_trading_day)
    )
    steps: list[WorkflowStepSpec] = []
    previous_step_id: str | None = None
    for task in selected_tasks:
        dependencies = (
            (previous_step_id,)
            if binding.preserve_sequential_order and previous_step_id is not None
            else ()
        )
        steps.append(_workflow_step(task, dependencies=dependencies))
        previous_step_id = task.daily_plan_step_id or task.task_id
    workflow = WorkflowSpec(
        workflow_id=f"scheduled_{cadence.cadence_id}",
        owner=binding.owner,
        cadence=canonical_cadence,
        timezone=binding.timezone,
        steps=tuple(steps),
        due_policy_id=binding.due_policy_id,
        trading_calendar=binding.trading_calendar,
    )
    return ScheduledWorkflowCompatibilityAssessment(
        cadence_id=cadence.cadence_id,
        status=CanonicalStatus.PASS,
        blocker_codes=(),
        workflow_spec=workflow,
        legacy_production_effects=tuple(effect_rows),
    )


def dispatch_scheduled_task(task_id: str) -> None:
    raise LegacyScheduledTaskDispatchBlocked(
        f"{task_id}: canonical operations executor is not enabled before ARCH-004F1 cut-in"
    )


def assess_daily_shadow_parity(
    *,
    cadence: ScheduledCadence,
    workflow_spec: WorkflowSpec,
    observed_step_ids: tuple[str, ...],
    observed_commands: tuple[tuple[str, ...], ...],
    observed_enabled: tuple[bool, ...],
    is_trading_day: bool,
) -> DailyShadowParityAssessment:
    if not (len(observed_step_ids) == len(observed_commands) == len(observed_enabled)):
        return DailyShadowParityAssessment(
            status=CanonicalStatus.BLOCKED,
            blocker_codes=("OBSERVED_DAILY_PLAN_LENGTH_MISMATCH",),
            expected_step_ids=tuple(step.step_id for step in workflow_spec.steps),
            observed_step_ids=observed_step_ids,
            legacy_only_step_ids=(),
        )
    expected_step_ids = tuple(step.step_id for step in workflow_spec.steps)
    blockers: list[str] = []
    expected_set = set(expected_step_ids)
    observed_scheduled_order = tuple(
        step_id for step_id in observed_step_ids if step_id in expected_set
    )
    legacy_only_step_ids = tuple(
        step_id for step_id in observed_step_ids if step_id not in expected_set
    )
    if observed_scheduled_order != expected_step_ids:
        blockers.append("DAILY_STEP_ORDER_MISMATCH")
    task_by_step_id = {(task.daily_plan_step_id or task.task_id): task for task in cadence.tasks}
    for step_id, command, enabled in zip(
        observed_step_ids, observed_commands, observed_enabled, strict=True
    ):
        task = task_by_step_id.get(step_id)
        if task is None:
            continue
        if not is_trading_day and task.closed_market_behavior == "skip_score_artifacts" and enabled:
            blockers.append(f"CLOSED_MARKET_STEP_ENABLED:{step_id}")
        if not enabled:
            continue
        command_text = " ".join(command)
        if any(token not in command_text for token in task.command_contains):
            blockers.append(f"DAILY_COMMAND_MISMATCH:{step_id}")
    status = CanonicalStatus.BLOCKED if blockers else CanonicalStatus.PASS
    if status is CanonicalStatus.PASS and legacy_only_step_ids:
        status = CanonicalStatus.LIMITED
    return DailyShadowParityAssessment(
        status=status,
        blocker_codes=tuple(sorted(set(blockers))),
        expected_step_ids=expected_step_ids,
        observed_step_ids=observed_step_ids,
        legacy_only_step_ids=legacy_only_step_ids,
    )


def _workflow_step(task: ScheduledTask, *, dependencies: tuple[str, ...]) -> WorkflowStepSpec:
    effect = _LEGACY_EFFECT_MAP[task.production_effect]
    return WorkflowStepSpec(
        step_id=task.daily_plan_step_id or task.task_id,
        entrypoint=EntrypointRef(
            module="ai_trading_system.legacy.scheduled_tasks_adapter",
            callable_name="dispatch_scheduled_task",
        ),
        dependencies=dependencies,
        quality_gate_required=False,
        idempotent=True,
        max_attempts=1,
        production_effect=effect,
        legacy_command=tuple(task.command.split()),
    )
