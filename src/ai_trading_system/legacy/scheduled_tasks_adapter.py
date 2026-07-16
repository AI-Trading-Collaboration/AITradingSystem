from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from ai_trading_system.contracts.operations import (
    DueRule,
    OperationsDueContext,
    OperationsDuePolicy,
    build_operations_shadow_plan,
    resolve_operations_due,
)
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

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status.value,
            "blocker_codes": list(self.blocker_codes),
            "expected_step_ids": list(self.expected_step_ids),
            "observed_step_ids": list(self.observed_step_ids),
            "legacy_only_step_ids": list(self.legacy_only_step_ids),
        }


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


def build_daily_schedule_shadow_payload(
    *,
    cadence: ScheduledCadence,
    as_of: date,
    generated_at: datetime,
    is_trading_day: bool,
    observed_step_ids: tuple[str, ...],
    observed_commands: tuple[tuple[str, ...], ...],
    observed_enabled: tuple[bool, ...],
    source_config_path: Path,
    source_config_sha256: str,
) -> dict[str, object]:
    workflow_spec = build_daily_schedule_workflow_spec(
        cadence=cadence,
        is_trading_day=is_trading_day,
    )
    binding = LegacyScheduledWorkflowBinding(
        owner="system_operations",
        timezone="America/New_York",
        due_policy_id="daily_unified_trigger_v1",
        trading_calendar="XNYS",
        preserve_sequential_order=True,
        is_trading_day=is_trading_day,
    )
    policy = OperationsDuePolicy(
        policy_id=binding.due_policy_id,
        owner=binding.owner,
        version="1.0.0",
        cadence=WorkflowCadence.DAILY,
        rule=DueRule.DAILY_TRIGGER,
        requires_trading_day=False,
        requires_completed_daily=False,
        requires_data_quality=False,
        requires_artifacts=False,
        requires_owner_gate=False,
    )
    resolution = resolve_operations_due(
        workflow_id=workflow_spec.workflow_id,
        policy=policy,
        context=OperationsDueContext(as_of=as_of, is_trading_day=is_trading_day),
    )
    run_id = f"daily_shadow_{as_of.isoformat()}_{generated_at.strftime('%Y%m%dT%H%M%S%f%z')}"
    shadow_plan = build_operations_shadow_plan(
        spec=workflow_spec,
        due_resolution=resolution,
        run_id=run_id,
        created_at=generated_at,
    )
    parity = assess_daily_shadow_parity(
        cadence=cadence,
        workflow_spec=workflow_spec,
        observed_step_ids=observed_step_ids,
        observed_commands=observed_commands,
        observed_enabled=observed_enabled,
        is_trading_day=is_trading_day,
    )
    if parity.status is not CanonicalStatus.PASS:
        raise LegacyScheduledTaskDispatchBlocked(
            "daily legacy/canonical shadow parity failed: " + ",".join(parity.blocker_codes)
        )
    return {
        "schema_version": "daily_operations_shadow.v1",
        "source_config": {
            "path": str(source_config_path),
            "sha256": source_config_sha256,
        },
        "workflow_spec": workflow_spec.to_dict(),
        "shadow_plan": shadow_plan.to_dict(),
        "parity": parity.to_dict(),
        "commands_executed": False,
        "non_daily_dispatch_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def build_daily_schedule_workflow_spec(
    *, cadence: ScheduledCadence, is_trading_day: bool
) -> WorkflowSpec:
    assessment = assess_scheduled_cadence(
        cadence,
        binding=LegacyScheduledWorkflowBinding(
            owner="system_operations",
            timezone="America/New_York",
            due_policy_id="daily_unified_trigger_v1",
            trading_calendar="XNYS",
            preserve_sequential_order=True,
            is_trading_day=is_trading_day,
        ),
    )
    if assessment.status is not CanonicalStatus.PASS or assessment.workflow_spec is None:
        raise LegacyScheduledTaskDispatchBlocked(
            "daily schedule compatibility assessment did not produce a canonical workflow"
        )
    return assessment.workflow_spec


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
        max_attempts=task.max_attempts,
        production_effect=effect,
        legacy_command=tuple(task.command.split()),
    )
