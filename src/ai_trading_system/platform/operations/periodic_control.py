from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Protocol

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.operations import (
    DueRule,
    OperationsDispatchMode,
    OperationsDueContext,
    OperationsDuePolicy,
    PeriodicOperationsPlan,
)
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.workflow import WorkflowCadence
from ai_trading_system.platform.artifacts import write_json_atomic
from ai_trading_system.platform.operations.runtime_control import OperationsRunControl
from ai_trading_system.trading_calendar import is_us_equity_trading_day
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_PERIODIC_OPERATIONS_CONTROL_POLICY_PATH = (
    PROJECT_ROOT / "config" / "operations" / "periodic_control.yaml"
)


class PeriodicOperationsControlError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class PeriodicCadenceControl:
    cadence_id: str
    due_policy: OperationsDuePolicy
    dispatch_mode: OperationsDispatchMode
    rationale: str

    def __post_init__(self) -> None:
        if not self.cadence_id.strip() or not self.rationale.strip():
            raise PeriodicOperationsControlError("PERIODIC_CADENCE_FIELD_MISSING", self.cadence_id)


@dataclass(frozen=True)
class PeriodicOperationsControlPolicy:
    policy_id: str
    owner: str
    version: str
    unified_external_trigger: str
    automatic_command_dispatch_enabled: bool
    allowed_command_prefixes: tuple[str, ...]
    cadence_controls: tuple[PeriodicCadenceControl, ...]
    path: Path
    sha256: str

    def __post_init__(self) -> None:
        for value, field in (
            (self.policy_id, "policy_id"),
            (self.owner, "owner"),
            (self.version, "version"),
            (self.unified_external_trigger, "unified_external_trigger"),
            (self.sha256, "sha256"),
        ):
            if not value.strip():
                raise PeriodicOperationsControlError("PERIODIC_POLICY_FIELD_MISSING", field)
        cadence_ids = [item.cadence_id for item in self.cadence_controls]
        if len(cadence_ids) != len(set(cadence_ids)):
            raise PeriodicOperationsControlError(
                "PERIODIC_POLICY_DUPLICATE_CADENCE", self.policy_id
            )
        expected = {"weekly", "biweekly", "monthly", "ad_hoc_research"}
        if set(cadence_ids) != expected:
            raise PeriodicOperationsControlError(
                "PERIODIC_POLICY_CADENCE_COVERAGE",
                ",".join(sorted(expected - set(cadence_ids))),
            )
        if self.automatic_command_dispatch_enabled:
            raise PeriodicOperationsControlError(
                "PERIODIC_AUTOMATIC_DISPATCH_NOT_ENABLED", self.policy_id
            )
        if not self.allowed_command_prefixes or any(
            not item.strip() for item in self.allowed_command_prefixes
        ):
            raise PeriodicOperationsControlError(
                "PERIODIC_COMMAND_PREFIXES_INVALID", self.policy_id
            )

    def cadence(self, cadence_id: str) -> PeriodicCadenceControl:
        for item in self.cadence_controls:
            if item.cadence_id == cadence_id:
                return item
        raise PeriodicOperationsControlError("PERIODIC_CADENCE_UNKNOWN", cadence_id)


def load_periodic_operations_control_policy(
    path: Path = DEFAULT_PERIODIC_OPERATIONS_CONTROL_POLICY_PATH,
) -> PeriodicOperationsControlPolicy:
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise PeriodicOperationsControlError("PERIODIC_POLICY_INVALID", str(path))
    if payload.get("schema_version") != "periodic_operations_control_policy.v1":
        raise PeriodicOperationsControlError("PERIODIC_POLICY_SCHEMA_INVALID", str(path))
    raw_cadences = payload.get("cadence_policies")
    if not isinstance(raw_cadences, dict):
        raise PeriodicOperationsControlError("PERIODIC_POLICY_CADENCES_INVALID", str(path))
    owner = str(payload.get("owner", ""))
    version = str(payload.get("version", ""))
    controls = tuple(
        _load_cadence_control(
            cadence_id=str(cadence_id),
            payload=raw,
            owner=owner,
            version=version,
        )
        for cadence_id, raw in raw_cadences.items()
    )
    return PeriodicOperationsControlPolicy(
        policy_id=str(payload.get("policy_id", "")),
        owner=owner,
        version=version,
        unified_external_trigger=str(payload.get("unified_external_trigger", "")),
        automatic_command_dispatch_enabled=(
            payload.get("automatic_command_dispatch_enabled") is True
        ),
        allowed_command_prefixes=tuple(
            str(item) for item in _required_list(payload, "allowed_command_prefixes")
        ),
        cadence_controls=controls,
        path=path,
        sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
    )


def build_periodic_due_contexts_from_daily(
    *,
    as_of: date,
    daily_status: CanonicalStatus | None,
    data_quality_status: CanonicalStatus | None,
    data_quality_evidence_id: str | None = None,
    required_artifacts_ready: bool | None = None,
    source_artifact_ids: tuple[str, ...] = (),
    owner_gate_approved: bool | None = None,
    owner_decision_id: str | None = None,
    explicit_trigger: bool = False,
) -> dict[str, OperationsDueContext]:
    is_trading_day = is_us_equity_trading_day(as_of)

    def context(
        *, is_period_end: bool | None = None, event_trigger: bool | None = None
    ) -> OperationsDueContext:
        return OperationsDueContext(
            as_of=as_of,
            is_trading_day=is_trading_day,
            is_period_end=is_period_end,
            explicit_trigger=event_trigger,
            daily_status=daily_status,
            data_quality_status=data_quality_status,
            data_quality_evidence_id=data_quality_evidence_id,
            required_artifacts_ready=required_artifacts_ready,
            source_artifact_ids=source_artifact_ids,
            owner_gate_approved=owner_gate_approved,
            owner_decision_id=owner_decision_id,
        )

    return {
        "weekly": context(
            is_period_end=_is_period_end(as_of, cadence=WorkflowCadence.WEEKLY),
        ),
        "biweekly": context(
            is_period_end=_is_period_end(as_of, cadence=WorkflowCadence.BIWEEKLY),
        ),
        "monthly": context(
            is_period_end=_is_period_end(as_of, cadence=WorkflowCadence.MONTHLY),
        ),
        "ad_hoc_research": context(event_trigger=explicit_trigger),
    }


def default_periodic_operations_plan_path(root: Path, as_of: date) -> Path:
    return root / f"periodic_operations_plan_{as_of.isoformat()}.json"


def write_periodic_operations_plan(plan: PeriodicOperationsPlan, path: Path) -> Path:
    return write_json_atomic(path, plan.to_dict()).path


class PeriodicCommandRunner(Protocol):
    def __call__(self, command: tuple[str, ...], *, cwd: Path) -> object: ...


@dataclass(frozen=True)
class PeriodicDispatchResult:
    task_id: str
    status: CanonicalStatus
    command: tuple[str, ...]
    return_code: int | None
    blocker_codes: tuple[str, ...] = ()


def dispatch_periodic_operations_plan(
    plan: PeriodicOperationsPlan,
    *,
    selected_task_ids: tuple[str, ...],
    control: OperationsRunControl,
    policy: PeriodicOperationsControlPolicy,
    runner: PeriodicCommandRunner,
    project_root: Path = PROJECT_ROOT,
    manual_invocation: bool = False,
) -> tuple[PeriodicDispatchResult, ...]:
    if plan.policy_id != policy.policy_id or plan.policy_sha256 != policy.sha256:
        raise PeriodicOperationsControlError("PERIODIC_PLAN_POLICY_MISMATCH", plan.policy_id)
    if not control.policy.non_daily_dispatch_enabled:
        raise PeriodicOperationsControlError(
            "PERIODIC_RUNTIME_DISPATCH_DISABLED", control.policy.policy_id
        )
    if not policy.automatic_command_dispatch_enabled and not manual_invocation:
        raise PeriodicOperationsControlError("PERIODIC_MANUAL_INVOCATION_REQUIRED", plan.policy_id)
    selected = tuple(dict.fromkeys(selected_task_ids))
    if not selected:
        raise PeriodicOperationsControlError("PERIODIC_TASK_SELECTION_EMPTY", plan.policy_id)
    entry_by_id = {entry.task_id: entry for entry in plan.entries}
    unknown = set(selected) - set(entry_by_id)
    if unknown:
        raise PeriodicOperationsControlError(
            "PERIODIC_TASK_SELECTION_UNKNOWN", ",".join(sorted(unknown))
        )

    results: list[PeriodicDispatchResult] = []
    for task_id in selected:
        entry = entry_by_id[task_id]
        resolution = entry.shadow_plan.due_resolution
        if resolution.status is not CanonicalStatus.DUE:
            results.append(
                PeriodicDispatchResult(
                    task_id=task_id,
                    status=(
                        CanonicalStatus.SKIPPED
                        if resolution.status is CanonicalStatus.NOT_DUE
                        else CanonicalStatus.BLOCKED
                    ),
                    command=(),
                    return_code=None,
                    blocker_codes=resolution.reason_codes,
                )
            )
            continue
        command, blockers = _resolve_periodic_command(
            entry.command_template,
            as_of=plan.as_of,
            allowed_prefixes=policy.allowed_command_prefixes,
        )
        if blockers:
            results.append(
                PeriodicDispatchResult(
                    task_id=task_id,
                    status=CanonicalStatus.BLOCKED,
                    command=command,
                    return_code=None,
                    blocker_codes=blockers,
                )
            )
            continue
        acquisition = control.acquire(
            spec=entry.workflow_spec,
            as_of=plan.as_of,
            run_id=f"periodic:{task_id}:{plan.as_of.isoformat()}:{plan.plan_id}",
        )
        if acquisition.lease is None:
            results.append(
                PeriodicDispatchResult(
                    task_id=task_id,
                    status=(
                        CanonicalStatus.PASS
                        if acquisition.resolution.decision.value == "ALREADY_COMPLETE"
                        else CanonicalStatus.BLOCKED
                    ),
                    command=command,
                    return_code=None,
                    blocker_codes=acquisition.resolution.blocker_codes,
                )
            )
            continue
        lease = acquisition.lease
        lease.start_step(task_id)
        try:
            completed = runner(command, cwd=project_root)
            return_code = getattr(completed, "returncode", None)
            if not isinstance(return_code, int):
                raise PeriodicOperationsControlError("PERIODIC_RUNNER_RESULT_INVALID", task_id)
            if return_code == 0:
                lease.pass_step(task_id)
                lease.finish(CanonicalStatus.PASS)
                results.append(
                    PeriodicDispatchResult(
                        task_id=task_id,
                        status=CanonicalStatus.PASS,
                        command=command,
                        return_code=return_code,
                    )
                )
            else:
                blocker = f"PERIODIC_STEP_FAILED:{task_id}"
                lease.fail_step(
                    task_id,
                    retryable=False,
                    blocker_code=blocker,
                )
                results.append(
                    PeriodicDispatchResult(
                        task_id=task_id,
                        status=CanonicalStatus.FAILED,
                        command=command,
                        return_code=return_code,
                        blocker_codes=(blocker,),
                    )
                )
        except Exception:
            if not lease.released:
                lease.finish(
                    CanonicalStatus.FAILED,
                    blocker_codes=(f"PERIODIC_RUNNER_EXCEPTION:{task_id}",),
                )
            raise
    return tuple(results)


def _load_cadence_control(
    *, cadence_id: str, payload: object, owner: str, version: str
) -> PeriodicCadenceControl:
    if not isinstance(payload, dict):
        raise PeriodicOperationsControlError("PERIODIC_CADENCE_POLICY_INVALID", cadence_id)
    anchor = payload.get("anchor_date")
    interval = payload.get("interval_weeks")
    due_policy = OperationsDuePolicy(
        policy_id=str(payload.get("due_policy_id", "")),
        owner=owner,
        version=version,
        cadence=WorkflowCadence(str(payload.get("workflow_cadence", ""))),
        rule=DueRule(str(payload.get("rule", ""))),
        requires_trading_day=_required_bool(payload, "requires_trading_day"),
        requires_completed_daily=_required_bool(payload, "requires_completed_daily"),
        requires_data_quality=_required_bool(payload, "requires_data_quality"),
        requires_artifacts=_required_bool(payload, "requires_artifacts"),
        requires_owner_gate=_required_bool(payload, "requires_owner_gate"),
        anchor_date=None if anchor is None else date.fromisoformat(str(anchor)),
        interval_weeks=None if interval is None else _required_int(payload, "interval_weeks"),
    )
    return PeriodicCadenceControl(
        cadence_id=cadence_id,
        due_policy=due_policy,
        dispatch_mode=OperationsDispatchMode(str(payload.get("dispatch_mode", ""))),
        rationale=str(payload.get("rationale", "")),
    )


def _required_bool(payload: dict[object, object], field: str) -> bool:
    value = payload.get(field)
    if not isinstance(value, bool):
        raise PeriodicOperationsControlError("PERIODIC_POLICY_BOOL_INVALID", field)
    return value


def _required_int(payload: dict[object, object], field: str) -> int:
    value = payload.get(field)
    if not isinstance(value, int) or isinstance(value, bool):
        raise PeriodicOperationsControlError("PERIODIC_POLICY_INT_INVALID", field)
    return value


def _required_list(payload: dict[object, object], field: str) -> list[object]:
    value = payload.get(field)
    if not isinstance(value, list):
        raise PeriodicOperationsControlError("PERIODIC_POLICY_LIST_INVALID", field)
    return value


def _resolve_periodic_command(
    template: str, *, as_of: date, allowed_prefixes: tuple[str, ...]
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    resolved = template.replace("{as_of}", as_of.isoformat()).strip()
    blockers: list[str] = []
    if re.search(r"\{[^{}]+\}", resolved):
        blockers.append("UNRESOLVED_BRACE_PLACEHOLDER")
    if re.search(r"<[^<>]+>", resolved):
        blockers.append("UNRESOLVED_ANGLE_PLACEHOLDER")
    if not any(resolved.startswith(prefix) for prefix in allowed_prefixes):
        blockers.append("COMMAND_PREFIX_NOT_ALLOWED")
    return tuple(resolved.split()), tuple(blockers)


def _is_period_end(as_of: date, *, cadence: WorkflowCadence) -> bool:
    if not is_us_equity_trading_day(as_of):
        return False
    next_trading_day = as_of + timedelta(days=1)
    while not is_us_equity_trading_day(next_trading_day):
        next_trading_day += timedelta(days=1)
    if cadence in {WorkflowCadence.WEEKLY, WorkflowCadence.BIWEEKLY}:
        return next_trading_day.isocalendar()[:2] != as_of.isocalendar()[:2]
    if cadence is WorkflowCadence.MONTHLY:
        return (next_trading_day.year, next_trading_day.month) != (
            as_of.year,
            as_of.month,
        )
    raise PeriodicOperationsControlError("PERIODIC_PERIOD_END_CADENCE_INVALID", cadence.value)
