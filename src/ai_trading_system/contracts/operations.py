from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime
from enum import StrEnum
from typing import ClassVar

from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.workflow import RunLedger, WorkflowCadence, WorkflowSpec


class OperationsContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class DueRule(StrEnum):
    DAILY_TRIGGER = "daily_trigger"
    PERIOD_END = "period_end"
    INTERVAL_WEEKS = "interval_weeks"
    EXPLICIT_TRIGGER = "explicit_trigger"


class OperationsRunDecision(StrEnum):
    START_NEW = "START_NEW"
    RESUME = "RESUME"
    ALREADY_COMPLETE = "ALREADY_COMPLETE"
    BLOCKED_CONCURRENT = "BLOCKED_CONCURRENT"
    BLOCKED_RETRY_EXHAUSTED = "BLOCKED_RETRY_EXHAUSTED"
    BLOCKED_UNSAFE_RESUME = "BLOCKED_UNSAFE_RESUME"


@dataclass(frozen=True)
class OperationsDuePolicy:
    schema_version: ClassVar[str] = "operations_due_policy.v1"

    policy_id: str
    owner: str
    version: str
    cadence: WorkflowCadence
    rule: DueRule
    requires_trading_day: bool
    requires_completed_daily: bool
    requires_data_quality: bool
    requires_artifacts: bool
    requires_owner_gate: bool
    anchor_date: date | None = None
    interval_weeks: int | None = None

    def __post_init__(self) -> None:
        for value, field in (
            (self.policy_id, "policy_id"),
            (self.owner, "owner"),
            (self.version, "version"),
        ):
            if not value.strip():
                raise OperationsContractError("REQUIRED_DUE_POLICY_FIELD_EMPTY", field)
        if self.rule is DueRule.INTERVAL_WEEKS:
            if self.anchor_date is None:
                raise OperationsContractError("DUE_POLICY_ANCHOR_REQUIRED", self.policy_id)
            if (
                self.interval_weeks is None
                or isinstance(self.interval_weeks, bool)
                or self.interval_weeks < 1
            ):
                raise OperationsContractError("DUE_POLICY_INTERVAL_INVALID", self.policy_id)
        elif self.anchor_date is not None or self.interval_weeks is not None:
            raise OperationsContractError("DUE_POLICY_INTERVAL_FIELDS_UNEXPECTED", self.policy_id)
        if self.rule is DueRule.DAILY_TRIGGER and self.cadence is not WorkflowCadence.DAILY:
            raise OperationsContractError("DUE_POLICY_CADENCE_RULE_MISMATCH", self.policy_id)
        if self.rule is DueRule.INTERVAL_WEEKS and self.cadence is not WorkflowCadence.BIWEEKLY:
            raise OperationsContractError("DUE_POLICY_CADENCE_RULE_MISMATCH", self.policy_id)
        if self.rule is DueRule.EXPLICIT_TRIGGER and self.cadence not in {
            WorkflowCadence.MANUAL,
            WorkflowCadence.EVENT_DRIVEN,
        }:
            raise OperationsContractError("DUE_POLICY_CADENCE_RULE_MISMATCH", self.policy_id)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "policy_id": self.policy_id,
            "owner": self.owner,
            "version": self.version,
            "cadence": self.cadence.value,
            "rule": self.rule.value,
            "requires_trading_day": self.requires_trading_day,
            "requires_completed_daily": self.requires_completed_daily,
            "requires_data_quality": self.requires_data_quality,
            "requires_artifacts": self.requires_artifacts,
            "requires_owner_gate": self.requires_owner_gate,
            "anchor_date": None if self.anchor_date is None else self.anchor_date.isoformat(),
            "interval_weeks": self.interval_weeks,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> OperationsDuePolicy:
        anchor = payload.get("anchor_date")
        interval = payload.get("interval_weeks")
        return cls(
            policy_id=str(payload.get("policy_id", "")),
            owner=str(payload.get("owner", "")),
            version=str(payload.get("version", "")),
            cadence=WorkflowCadence(str(payload.get("cadence", ""))),
            rule=DueRule(str(payload.get("rule", ""))),
            requires_trading_day=payload.get("requires_trading_day") is True,
            requires_completed_daily=payload.get("requires_completed_daily") is True,
            requires_data_quality=payload.get("requires_data_quality") is True,
            requires_artifacts=payload.get("requires_artifacts") is True,
            requires_owner_gate=payload.get("requires_owner_gate") is True,
            anchor_date=None if anchor is None else _date_value(anchor, "anchor_date"),
            interval_weeks=None if interval is None else _int_value(interval, "interval_weeks"),
        )


@dataclass(frozen=True)
class OperationsDueContext:
    as_of: date
    is_trading_day: bool | None = None
    is_period_end: bool | None = None
    explicit_trigger: bool | None = None
    daily_status: CanonicalStatus | None = None
    data_quality_status: CanonicalStatus | None = None
    data_quality_evidence_id: str | None = None
    required_artifacts_ready: bool | None = None
    source_artifact_ids: tuple[str, ...] = ()
    owner_gate_approved: bool | None = None
    owner_decision_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "source_artifact_ids",
            tuple(
                sorted(
                    set(_nonempty(item, "source_artifact_id") for item in self.source_artifact_ids)
                )
            ),
        )
        if self.data_quality_evidence_id is not None:
            _nonempty(self.data_quality_evidence_id, "data_quality_evidence_id")
        if self.owner_decision_id is not None:
            _nonempty(self.owner_decision_id, "owner_decision_id")


@dataclass(frozen=True)
class OperationsDueResolution:
    schema_version: ClassVar[str] = "operations_due_resolution.v1"

    workflow_id: str
    due_policy_id: str
    cadence: WorkflowCadence
    as_of: date
    status: CanonicalStatus
    reason_codes: tuple[str, ...]
    data_quality_evidence_id: str | None = None
    source_artifact_ids: tuple[str, ...] = ()
    owner_decision_id: str | None = None

    def __post_init__(self) -> None:
        _nonempty(self.workflow_id, "workflow_id")
        _nonempty(self.due_policy_id, "due_policy_id")
        if self.status not in {
            CanonicalStatus.DUE,
            CanonicalStatus.NOT_DUE,
            CanonicalStatus.BLOCKED,
        }:
            raise OperationsContractError("INVALID_DUE_RESOLUTION_STATUS", self.status.value)
        reasons = tuple(sorted(set(_nonempty(item, "reason_code") for item in self.reason_codes)))
        if not reasons:
            raise OperationsContractError("DUE_RESOLUTION_REASON_REQUIRED", self.workflow_id)
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(
            self,
            "source_artifact_ids",
            tuple(
                sorted(
                    set(_nonempty(item, "source_artifact_id") for item in self.source_artifact_ids)
                )
            ),
        )

    @property
    def resolution_id(self) -> str:
        material = json.dumps(
            self.to_dict(include_id=False),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return f"operations_due_{hashlib.sha256(material).hexdigest()[:20]}"

    def to_dict(self, *, include_id: bool = True) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": self.schema_version,
            "workflow_id": self.workflow_id,
            "due_policy_id": self.due_policy_id,
            "cadence": self.cadence.value,
            "as_of": self.as_of.isoformat(),
            "status": self.status.value,
            "reason_codes": list(self.reason_codes),
            "data_quality_evidence_id": self.data_quality_evidence_id,
            "source_artifact_ids": list(self.source_artifact_ids),
            "owner_decision_id": self.owner_decision_id,
        }
        return {"resolution_id": self.resolution_id, **payload} if include_id else payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> OperationsDueResolution:
        resolution = cls(
            workflow_id=str(payload.get("workflow_id", "")),
            due_policy_id=str(payload.get("due_policy_id", "")),
            cadence=WorkflowCadence(str(payload.get("cadence", ""))),
            as_of=_date_value(payload.get("as_of"), "as_of"),
            status=CanonicalStatus(str(payload.get("status", ""))),
            reason_codes=tuple(str(item) for item in _list(payload, "reason_codes")),
            data_quality_evidence_id=_optional_text(
                payload.get("data_quality_evidence_id"), "data_quality_evidence_id"
            ),
            source_artifact_ids=tuple(str(item) for item in _list(payload, "source_artifact_ids")),
            owner_decision_id=_optional_text(payload.get("owner_decision_id"), "owner_decision_id"),
        )
        supplied = payload.get("resolution_id")
        if supplied is not None and str(supplied) != resolution.resolution_id:
            raise OperationsContractError("DUE_RESOLUTION_ID_MISMATCH", resolution.workflow_id)
        return resolution


@dataclass(frozen=True)
class OperationsShadowPlan:
    schema_version: ClassVar[str] = "operations_shadow_plan.v1"

    workflow_spec_id: str
    due_resolution: OperationsDueResolution
    run_ledger: RunLedger
    unified_external_trigger: str
    execution_enabled: bool = False

    def __post_init__(self) -> None:
        _nonempty(self.workflow_spec_id, "workflow_spec_id")
        _nonempty(self.unified_external_trigger, "unified_external_trigger")
        if self.execution_enabled:
            raise OperationsContractError(
                "SHADOW_PLAN_EXECUTION_FORBIDDEN", self.due_resolution.workflow_id
            )
        if self.workflow_spec_id != self.run_ledger.workflow_spec_id:
            raise OperationsContractError(
                "SHADOW_PLAN_SPEC_MISMATCH", self.due_resolution.workflow_id
            )
        if self.due_resolution.workflow_id != self.run_ledger.workflow_id:
            raise OperationsContractError(
                "SHADOW_PLAN_WORKFLOW_MISMATCH", self.due_resolution.workflow_id
            )
        states = {entry.status for entry in self.run_ledger.entries}
        expected = {
            CanonicalStatus.DUE: {CanonicalStatus.DUE},
            CanonicalStatus.NOT_DUE: {CanonicalStatus.NOT_DUE},
            CanonicalStatus.BLOCKED: {CanonicalStatus.BLOCKED},
        }[self.due_resolution.status]
        if states != expected:
            raise OperationsContractError(
                "SHADOW_PLAN_LEDGER_STATUS_MISMATCH", self.due_resolution.workflow_id
            )

    @property
    def shadow_plan_id(self) -> str:
        material = json.dumps(
            self.to_dict(include_id=False),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return f"operations_shadow_{hashlib.sha256(material).hexdigest()[:20]}"

    def to_dict(self, *, include_id: bool = True) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": self.schema_version,
            "workflow_spec_id": self.workflow_spec_id,
            "due_resolution": self.due_resolution.to_dict(),
            "run_ledger": self.run_ledger.to_dict(),
            "unified_external_trigger": self.unified_external_trigger,
            "execution_enabled": self.execution_enabled,
        }
        return {"shadow_plan_id": self.shadow_plan_id, **payload} if include_id else payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> OperationsShadowPlan:
        resolution_payload = payload.get("due_resolution")
        ledger_payload = payload.get("run_ledger")
        if not isinstance(resolution_payload, Mapping) or not isinstance(ledger_payload, Mapping):
            raise OperationsContractError(
                "INVALID_OPERATIONS_PAYLOAD", "shadow plan nested payloads must be mappings"
            )
        plan = cls(
            workflow_spec_id=str(payload.get("workflow_spec_id", "")),
            due_resolution=OperationsDueResolution.from_dict(resolution_payload),
            run_ledger=RunLedger.from_dict(ledger_payload),
            unified_external_trigger=str(payload.get("unified_external_trigger", "")),
            execution_enabled=payload.get("execution_enabled") is True,
        )
        supplied = payload.get("shadow_plan_id")
        if supplied is not None and str(supplied) != plan.shadow_plan_id:
            raise OperationsContractError(
                "SHADOW_PLAN_ID_MISMATCH", plan.due_resolution.workflow_id
            )
        return plan


@dataclass(frozen=True)
class OperationsExecutionState:
    schema_version: ClassVar[str] = "operations_execution_state.v1"

    idempotency_key: str
    workflow_id: str
    workflow_spec_id: str
    as_of: date
    run_id: str
    status: CanonicalStatus
    attempt: int
    started_at: datetime
    updated_at: datetime
    finished_at: datetime | None = None
    completed_step_ids: tuple[str, ...] = ()
    skipped_step_ids: tuple[str, ...] = ()
    current_step_id: str | None = None
    step_attempts: tuple[tuple[str, int], ...] = ()
    blocker_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for value, field in (
            (self.idempotency_key, "idempotency_key"),
            (self.workflow_id, "workflow_id"),
            (self.workflow_spec_id, "workflow_spec_id"),
            (self.run_id, "run_id"),
        ):
            _nonempty(value, field)
        if isinstance(self.attempt, bool) or self.attempt < 1:
            raise OperationsContractError("INVALID_RUN_ATTEMPT", self.run_id)
        _aware_datetime(self.started_at, "started_at")
        _aware_datetime(self.updated_at, "updated_at")
        if self.finished_at is not None:
            _aware_datetime(self.finished_at, "finished_at")
        if self.updated_at < self.started_at:
            raise OperationsContractError("RUN_STATE_TIME_REVERSED", self.run_id)
        if self.finished_at is not None and self.finished_at < self.started_at:
            raise OperationsContractError("RUN_STATE_TIME_REVERSED", self.run_id)
        completed = tuple(
            sorted(set(_nonempty(item, "completed_step_id") for item in self.completed_step_ids))
        )
        object.__setattr__(self, "completed_step_ids", completed)
        skipped = tuple(
            sorted(set(_nonempty(item, "skipped_step_id") for item in self.skipped_step_ids))
        )
        object.__setattr__(self, "skipped_step_ids", skipped)
        overlap = set(completed) & set(skipped)
        if overlap:
            raise OperationsContractError(
                "RUN_STEP_COMPLETE_AND_SKIPPED", ",".join(sorted(overlap))
            )
        blockers = tuple(
            sorted(set(_nonempty(item, "blocker_code") for item in self.blocker_codes))
        )
        object.__setattr__(self, "blocker_codes", blockers)
        if self.current_step_id is not None:
            _nonempty(self.current_step_id, "current_step_id")
            if self.current_step_id in completed:
                raise OperationsContractError("RUN_CURRENT_STEP_ALREADY_COMPLETE", self.run_id)
            if self.current_step_id in skipped:
                raise OperationsContractError("RUN_CURRENT_STEP_ALREADY_SKIPPED", self.run_id)
        attempts: dict[str, int] = {}
        for step_id, count in self.step_attempts:
            _nonempty(step_id, "step_attempt_id")
            if isinstance(count, bool) or count < 1:
                raise OperationsContractError("INVALID_STEP_ATTEMPT", step_id)
            if step_id in attempts:
                raise OperationsContractError("DUPLICATE_STEP_ATTEMPT", step_id)
            attempts[step_id] = count
        object.__setattr__(self, "step_attempts", tuple(sorted(attempts.items())))
        terminal = self.status in {
            CanonicalStatus.PASS,
            CanonicalStatus.BLOCKED,
            CanonicalStatus.FAILED,
        }
        if terminal != (self.finished_at is not None):
            raise OperationsContractError("RUN_TERMINAL_TIME_MISMATCH", self.run_id)
        if self.status in {CanonicalStatus.BLOCKED, CanonicalStatus.FAILED} and not blockers:
            raise OperationsContractError("RUN_TERMINAL_BLOCKER_REQUIRED", self.run_id)

    def with_step_started(self, *, step_id: str, at: datetime) -> OperationsExecutionState:
        if self.status is not CanonicalStatus.RUNNING:
            raise OperationsContractError("RUN_NOT_RUNNING", self.run_id)
        if self.current_step_id is not None:
            raise OperationsContractError("RUN_STEP_ALREADY_ACTIVE", self.current_step_id)
        if step_id in self.completed_step_ids:
            raise OperationsContractError("RUN_STEP_ALREADY_COMPLETE", step_id)
        if step_id in self.skipped_step_ids:
            raise OperationsContractError("RUN_STEP_ALREADY_SKIPPED", step_id)
        _aware_datetime(at, "step_started_at")
        attempts = dict(self.step_attempts)
        attempts[step_id] = attempts.get(step_id, 0) + 1
        return replace(
            self,
            current_step_id=_nonempty(step_id, "step_id"),
            step_attempts=tuple(attempts.items()),
            updated_at=at,
        )

    def with_step_passed(self, *, step_id: str, at: datetime) -> OperationsExecutionState:
        if self.current_step_id != step_id:
            raise OperationsContractError("RUN_ACTIVE_STEP_MISMATCH", step_id)
        _aware_datetime(at, "step_finished_at")
        return replace(
            self,
            completed_step_ids=(*self.completed_step_ids, step_id),
            current_step_id=None,
            updated_at=at,
        )

    def terminal(
        self,
        *,
        status: CanonicalStatus,
        at: datetime,
        blocker_codes: tuple[str, ...] = (),
    ) -> OperationsExecutionState:
        if status not in {CanonicalStatus.PASS, CanonicalStatus.BLOCKED, CanonicalStatus.FAILED}:
            raise OperationsContractError("INVALID_RUN_TERMINAL_STATUS", status.value)
        _aware_datetime(at, "finished_at")
        return replace(
            self,
            status=status,
            updated_at=at,
            finished_at=at,
            current_step_id=None,
            blocker_codes=blocker_codes,
        )

    def with_step_retry_ready(self, *, step_id: str, at: datetime) -> OperationsExecutionState:
        if self.current_step_id != step_id:
            raise OperationsContractError("RUN_ACTIVE_STEP_MISMATCH", step_id)
        _aware_datetime(at, "step_retry_at")
        return replace(self, current_step_id=None, updated_at=at)

    def with_step_skipped(self, *, step_id: str, at: datetime) -> OperationsExecutionState:
        if self.status is not CanonicalStatus.RUNNING:
            raise OperationsContractError("RUN_NOT_RUNNING", self.run_id)
        if self.current_step_id is not None:
            raise OperationsContractError("RUN_STEP_ALREADY_ACTIVE", self.current_step_id)
        if step_id in self.completed_step_ids:
            raise OperationsContractError("RUN_STEP_ALREADY_COMPLETE", step_id)
        if step_id in self.skipped_step_ids:
            return self
        _aware_datetime(at, "step_skipped_at")
        return replace(
            self,
            skipped_step_ids=(*self.skipped_step_ids, _nonempty(step_id, "step_id")),
            updated_at=at,
        )

    def step_attempt_count(self, step_id: str) -> int:
        return dict(self.step_attempts).get(step_id, 0)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "idempotency_key": self.idempotency_key,
            "workflow_id": self.workflow_id,
            "workflow_spec_id": self.workflow_spec_id,
            "as_of": self.as_of.isoformat(),
            "run_id": self.run_id,
            "status": self.status.value,
            "attempt": self.attempt,
            "started_at": self.started_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "finished_at": None if self.finished_at is None else self.finished_at.isoformat(),
            "completed_step_ids": list(self.completed_step_ids),
            "skipped_step_ids": list(self.skipped_step_ids),
            "current_step_id": self.current_step_id,
            "step_attempts": [
                {"step_id": step_id, "attempts": attempts}
                for step_id, attempts in self.step_attempts
            ],
            "blocker_codes": list(self.blocker_codes),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> OperationsExecutionState:
        return cls(
            idempotency_key=str(payload.get("idempotency_key", "")),
            workflow_id=str(payload.get("workflow_id", "")),
            workflow_spec_id=str(payload.get("workflow_spec_id", "")),
            as_of=_date_value(payload.get("as_of"), "as_of"),
            run_id=str(payload.get("run_id", "")),
            status=CanonicalStatus(str(payload.get("status", ""))),
            attempt=_int_value(payload.get("attempt"), "attempt"),
            started_at=_datetime_value(payload.get("started_at"), "started_at"),
            updated_at=_datetime_value(payload.get("updated_at"), "updated_at"),
            finished_at=_optional_datetime_value(payload.get("finished_at"), "finished_at"),
            completed_step_ids=tuple(str(item) for item in _list(payload, "completed_step_ids")),
            skipped_step_ids=tuple(str(item) for item in _list(payload, "skipped_step_ids")),
            current_step_id=_optional_text(payload.get("current_step_id"), "current_step_id"),
            step_attempts=_step_attempts_value(payload.get("step_attempts", [])),
            blocker_codes=tuple(str(item) for item in _list(payload, "blocker_codes")),
        )


@dataclass(frozen=True)
class OperationsRunControlResolution:
    schema_version: ClassVar[str] = "operations_run_control_resolution.v1"

    decision: OperationsRunDecision
    idempotency_key: str
    workflow_id: str
    workflow_spec_id: str
    as_of: date
    attempt: int
    resume_completed_step_ids: tuple[str, ...] = ()
    blocker_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _nonempty(self.idempotency_key, "idempotency_key")
        _nonempty(self.workflow_id, "workflow_id")
        _nonempty(self.workflow_spec_id, "workflow_spec_id")
        if isinstance(self.attempt, bool) or self.attempt < 0:
            raise OperationsContractError("INVALID_RUN_ATTEMPT", self.workflow_id)
        object.__setattr__(
            self,
            "resume_completed_step_ids",
            tuple(
                sorted(
                    set(
                        _nonempty(item, "resume_completed_step_id")
                        for item in self.resume_completed_step_ids
                    )
                )
            ),
        )
        blockers = tuple(
            sorted(set(_nonempty(item, "blocker_code") for item in self.blocker_codes))
        )
        object.__setattr__(self, "blocker_codes", blockers)
        if self.decision.value.startswith("BLOCKED_") and not blockers:
            raise OperationsContractError("RUN_CONTROL_BLOCKER_REQUIRED", self.workflow_id)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "decision": self.decision.value,
            "idempotency_key": self.idempotency_key,
            "workflow_id": self.workflow_id,
            "workflow_spec_id": self.workflow_spec_id,
            "as_of": self.as_of.isoformat(),
            "attempt": self.attempt,
            "resume_completed_step_ids": list(self.resume_completed_step_ids),
            "blocker_codes": list(self.blocker_codes),
        }


def build_operations_shadow_plan(
    *,
    spec: WorkflowSpec,
    due_resolution: OperationsDueResolution,
    run_id: str,
    created_at: datetime,
    unified_external_trigger: str = "aits ops daily-run",
) -> OperationsShadowPlan:
    if spec.workflow_id != due_resolution.workflow_id:
        raise OperationsContractError("SHADOW_PLAN_WORKFLOW_MISMATCH", spec.workflow_id)
    if spec.due_policy_id != due_resolution.due_policy_id:
        raise OperationsContractError("SHADOW_PLAN_DUE_POLICY_MISMATCH", spec.workflow_id)
    if spec.cadence is not due_resolution.cadence:
        raise OperationsContractError("SHADOW_PLAN_CADENCE_MISMATCH", spec.workflow_id)

    ledger = RunLedger.initialize(
        spec,
        run_id=run_id,
        as_of=due_resolution.as_of,
        created_at=created_at,
    )
    if due_resolution.status is CanonicalStatus.DUE:
        for step in spec.steps:
            entry = ledger.entry(step.step_id).transition(CanonicalStatus.DUE, at=created_at)
            ledger = ledger.with_entry(spec, entry)
    elif due_resolution.status is CanonicalStatus.BLOCKED:
        for step in spec.steps:
            due_entry = ledger.entry(step.step_id).transition(CanonicalStatus.DUE, at=created_at)
            ledger = ledger.with_entry(spec, due_entry)
            blocked_entry = due_entry.transition(
                CanonicalStatus.BLOCKED,
                at=created_at,
                blocker_codes=due_resolution.reason_codes,
            )
            ledger = ledger.with_entry(spec, blocked_entry)

    return OperationsShadowPlan(
        workflow_spec_id=spec.spec_id,
        due_resolution=due_resolution,
        run_ledger=ledger,
        unified_external_trigger=unified_external_trigger,
    )


def resolve_operations_due(
    *, workflow_id: str, policy: OperationsDuePolicy, context: OperationsDueContext
) -> OperationsDueResolution:
    _nonempty(workflow_id, "workflow_id")
    not_due: list[str] = []
    blocked: list[str] = []

    if policy.requires_trading_day:
        if context.is_trading_day is None:
            blocked.append("TRADING_DAY_STATUS_MISSING")
        elif not context.is_trading_day:
            not_due.append("NOT_A_TRADING_DAY")

    if policy.rule in {DueRule.PERIOD_END, DueRule.INTERVAL_WEEKS}:
        if context.is_period_end is None:
            blocked.append("PERIOD_END_STATUS_MISSING")
        elif not context.is_period_end:
            not_due.append("NOT_PERIOD_END")
    if policy.rule is DueRule.INTERVAL_WEEKS and context.is_period_end:
        assert policy.anchor_date is not None
        assert policy.interval_weeks is not None
        weeks = (context.as_of - policy.anchor_date).days // 7
        if weeks < 0 or weeks % policy.interval_weeks != 0:
            not_due.append("INTERVAL_NOT_ELAPSED")
    if policy.rule is DueRule.EXPLICIT_TRIGGER:
        if context.explicit_trigger is None:
            blocked.append("EXPLICIT_TRIGGER_STATUS_MISSING")
        elif not context.explicit_trigger:
            not_due.append("EXPLICIT_TRIGGER_NOT_SET")

    base_due = not not_due and not blocked
    if base_due and policy.requires_completed_daily:
        if context.daily_status is None:
            blocked.append("DAILY_STATUS_MISSING")
        elif context.daily_status is not CanonicalStatus.PASS:
            blocked.append("DAILY_NOT_PASSED")
    if base_due and policy.requires_data_quality:
        if context.data_quality_status is None:
            blocked.append("DATA_QUALITY_STATUS_MISSING")
        elif context.data_quality_status is not CanonicalStatus.PASS:
            blocked.append("DATA_QUALITY_NOT_PASSED")
        elif not context.data_quality_evidence_id:
            blocked.append("DATA_QUALITY_EVIDENCE_MISSING")
    if base_due and policy.requires_artifacts:
        if context.required_artifacts_ready is None:
            blocked.append("REQUIRED_ARTIFACT_STATUS_MISSING")
        elif not context.required_artifacts_ready:
            blocked.append("REQUIRED_ARTIFACTS_NOT_READY")
        elif not context.source_artifact_ids:
            blocked.append("SOURCE_ARTIFACT_IDS_MISSING")
    if base_due and policy.requires_owner_gate:
        if context.owner_gate_approved is None:
            blocked.append("OWNER_GATE_STATUS_MISSING")
        elif not context.owner_gate_approved:
            blocked.append("OWNER_GATE_NOT_APPROVED")
        elif not context.owner_decision_id:
            blocked.append("OWNER_DECISION_ID_MISSING")

    if blocked:
        status = CanonicalStatus.BLOCKED
        reasons = blocked
    elif not_due:
        status = CanonicalStatus.NOT_DUE
        reasons = not_due
    else:
        status = CanonicalStatus.DUE
        reasons = ["DUE_POLICY_SATISFIED"]
    return OperationsDueResolution(
        workflow_id=workflow_id,
        due_policy_id=policy.policy_id,
        cadence=policy.cadence,
        as_of=context.as_of,
        status=status,
        reason_codes=tuple(reasons),
        data_quality_evidence_id=context.data_quality_evidence_id,
        source_artifact_ids=context.source_artifact_ids,
        owner_decision_id=context.owner_decision_id,
    )


def _nonempty(value: str, field: str) -> str:
    if not value.strip():
        raise OperationsContractError("REQUIRED_OPERATIONS_FIELD_EMPTY", field)
    return value


def _date_value(value: object, field: str) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise OperationsContractError("INVALID_OPERATIONS_DATE", field) from exc
    raise OperationsContractError("INVALID_OPERATIONS_DATE", field)


def _int_value(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise OperationsContractError("INVALID_OPERATIONS_INTEGER", field)
    return value


def _list(payload: Mapping[str, object], field: str) -> list[object]:
    value = payload.get(field, [])
    if not isinstance(value, list):
        raise OperationsContractError("INVALID_OPERATIONS_PAYLOAD", f"{field} must be list")
    return value


def _optional_text(value: object, field: str) -> str | None:
    if value is None:
        return None
    return _nonempty(str(value), field)


def _aware_datetime(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise OperationsContractError("OPERATIONS_DATETIME_TZ_REQUIRED", field)


def _datetime_value(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise OperationsContractError("INVALID_OPERATIONS_DATETIME", field) from exc
    else:
        raise OperationsContractError("INVALID_OPERATIONS_DATETIME", field)
    _aware_datetime(parsed, field)
    return parsed


def _optional_datetime_value(value: object, field: str) -> datetime | None:
    return None if value is None else _datetime_value(value, field)


def _step_attempts_value(value: object) -> tuple[tuple[str, int], ...]:
    if not isinstance(value, list):
        raise OperationsContractError("INVALID_OPERATIONS_PAYLOAD", "step_attempts must be list")
    parsed: list[tuple[str, int]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise OperationsContractError(
                "INVALID_OPERATIONS_PAYLOAD", "step_attempt must be mapping"
            )
        parsed.append(
            (
                str(item.get("step_id", "")),
                _int_value(item.get("attempts"), "step_attempts.attempts"),
            )
        )
    return tuple(parsed)
