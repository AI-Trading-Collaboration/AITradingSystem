from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path

from ai_trading_system.contracts.operations import (
    OperationsDueContext,
    PeriodicOperationsPlan,
    PeriodicOperationsPlanEntry,
    build_operations_shadow_plan,
    resolve_operations_due,
)
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.workflow import WorkflowSpec
from ai_trading_system.legacy.scheduled_tasks_adapter import (
    LegacyScheduledWorkflowBinding,
    assess_scheduled_cadence,
)
from ai_trading_system.platform.operations.periodic_control import (
    PeriodicCadenceControl,
    PeriodicOperationsControlError,
    PeriodicOperationsControlPolicy,
    load_periodic_operations_control_policy,
)
from ai_trading_system.scheduled_tasks import (
    ScheduledCadence,
    ScheduledTask,
    ScheduledTasksConfig,
    load_scheduled_tasks_config,
)


def build_periodic_operations_plan(
    *,
    as_of: date,
    generated_at: datetime,
    contexts: Mapping[str, OperationsDueContext],
    scheduled: ScheduledTasksConfig | None = None,
    policy: PeriodicOperationsControlPolicy | None = None,
) -> PeriodicOperationsPlan:
    resolved_scheduled = scheduled or load_scheduled_tasks_config()
    resolved_policy = policy or load_periodic_operations_control_policy()
    entries: list[PeriodicOperationsPlanEntry] = []
    for cadence in resolved_scheduled.cadences:
        if cadence.cadence_id == "daily_trading_day":
            continue
        control = resolved_policy.cadence(cadence.cadence_id)
        context = contexts.get(cadence.cadence_id)
        if context is None:
            raise PeriodicOperationsControlError("PERIODIC_CONTEXT_MISSING", cadence.cadence_id)
        if context.as_of != as_of:
            raise PeriodicOperationsControlError(
                "PERIODIC_CONTEXT_AS_OF_MISMATCH", cadence.cadence_id
            )
        for task in cadence.tasks:
            spec = _task_workflow_spec(task=task, cadence=cadence, control=control)
            resolution = resolve_operations_due(
                workflow_id=spec.workflow_id,
                policy=control.due_policy,
                context=context,
            )
            shadow = build_operations_shadow_plan(
                spec=spec,
                due_resolution=resolution,
                run_id=(
                    f"periodic_shadow:{task.task_id}:{as_of.isoformat()}:"
                    f"{generated_at.strftime('%Y%m%dT%H%M%S%f%z')}"
                ),
                created_at=generated_at,
                unified_external_trigger=resolved_policy.unified_external_trigger,
            )
            entries.append(
                PeriodicOperationsPlanEntry(
                    task_id=task.task_id,
                    cadence_id=cadence.cadence_id,
                    command_template=task.command,
                    dispatch_mode=control.dispatch_mode,
                    workflow_spec=spec,
                    shadow_plan=shadow,
                )
            )
    scheduled_path = Path(resolved_scheduled.path)
    return PeriodicOperationsPlan(
        policy_id=resolved_policy.policy_id,
        as_of=as_of,
        generated_at=generated_at,
        unified_external_trigger=resolved_policy.unified_external_trigger,
        scheduled_config_path=str(scheduled_path),
        scheduled_config_sha256=hashlib.sha256(scheduled_path.read_bytes()).hexdigest(),
        policy_path=str(resolved_policy.path),
        policy_sha256=resolved_policy.sha256,
        entries=tuple(entries),
        automatic_command_dispatch_enabled=(resolved_policy.automatic_command_dispatch_enabled),
    )


def _task_workflow_spec(
    *,
    task: ScheduledTask,
    cadence: ScheduledCadence,
    control: PeriodicCadenceControl,
) -> WorkflowSpec:
    assessment = assess_scheduled_cadence(
        ScheduledCadence(
            cadence_id=cadence.cadence_id,
            description=cadence.description,
            tasks=(task,),
        ),
        binding=LegacyScheduledWorkflowBinding(
            owner=control.due_policy.owner,
            timezone="America/New_York",
            due_policy_id=control.due_policy.policy_id,
            trading_calendar="XNYS",
            preserve_sequential_order=False,
        ),
    )
    if assessment.status is not CanonicalStatus.PASS or assessment.workflow_spec is None:
        raise PeriodicOperationsControlError(
            "PERIODIC_TASK_WORKFLOW_BLOCKED",
            f"{task.task_id}:{','.join(assessment.blocker_codes)}",
        )
    return replace(
        assessment.workflow_spec,
        workflow_id=f"scheduled_task_{task.task_id}",
    )
