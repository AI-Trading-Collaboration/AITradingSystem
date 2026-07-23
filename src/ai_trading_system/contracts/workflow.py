from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime
from enum import StrEnum
from typing import ClassVar, Self

from ai_trading_system.contracts.artifact_envelope import ArtifactPointer
from ai_trading_system.contracts.data_quality import DataQualityEvidence
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.core.production_effect import ProductionEffect


class WorkflowContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class WorkflowCadence(StrEnum):
    DAILY = "daily"
    WEEKLY = "weekly"
    BIWEEKLY = "biweekly"
    MONTHLY = "monthly"
    EVENT_DRIVEN = "event_driven"
    MANUAL = "manual"


class FailurePropagation(StrEnum):
    BLOCK_DEPENDENTS = "BLOCK_DEPENDENTS"
    CONTINUE_LIMITED = "CONTINUE_LIMITED"


_ALLOWED_TRANSITIONS: dict[CanonicalStatus, frozenset[CanonicalStatus]] = {
    CanonicalStatus.NOT_DUE: frozenset({CanonicalStatus.DUE, CanonicalStatus.SKIPPED}),
    CanonicalStatus.DUE: frozenset(
        {CanonicalStatus.RUNNING, CanonicalStatus.SKIPPED, CanonicalStatus.BLOCKED}
    ),
    CanonicalStatus.RUNNING: frozenset(
        {
            CanonicalStatus.PASS,
            CanonicalStatus.LIMITED,
            CanonicalStatus.BLOCKED,
            CanonicalStatus.FAILED,
        }
    ),
    CanonicalStatus.PASS: frozenset(),
    CanonicalStatus.LIMITED: frozenset(),
    CanonicalStatus.SKIPPED: frozenset(),
    CanonicalStatus.BLOCKED: frozenset(),
    CanonicalStatus.FAILED: frozenset(),
}


def _require_text(value: str, field: str) -> None:
    if not value.strip():
        raise WorkflowContractError("REQUIRED_WORKFLOW_FIELD_EMPTY", f"{field} is required")


def _aware(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise WorkflowContractError("WORKFLOW_DATETIME_TZ_REQUIRED", field)


def _datetime_value(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise WorkflowContractError("INVALID_WORKFLOW_DATETIME", f"{field}={value!r}") from exc
    else:
        raise WorkflowContractError("INVALID_WORKFLOW_DATETIME", f"{field}={value!r}")
    _aware(parsed, field)
    return parsed


def _optional_datetime(value: object, field: str) -> datetime | None:
    return None if value is None else _datetime_value(value, field)


def _date_value(value: object, field: str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise WorkflowContractError("INVALID_WORKFLOW_DATE", f"{field}={value!r}") from exc
    raise WorkflowContractError("INVALID_WORKFLOW_DATE", f"{field}={value!r}")


def _int_value(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise WorkflowContractError("INVALID_WORKFLOW_PAYLOAD", f"{field} must be integer")
    return value


@dataclass(frozen=True)
class EntrypointRef:
    module: str
    callable_name: str

    def __post_init__(self) -> None:
        _require_text(self.module, "entrypoint.module")
        _require_text(self.callable_name, "entrypoint.callable_name")
        if " " in self.module or " " in self.callable_name:
            raise WorkflowContractError("INVALID_WORKFLOW_ENTRYPOINT", self.display)

    @property
    def display(self) -> str:
        return f"{self.module}:{self.callable_name}"

    def to_dict(self) -> dict[str, str]:
        return {"module": self.module, "callable_name": self.callable_name}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> EntrypointRef:
        return cls(
            module=str(payload.get("module", "")),
            callable_name=str(payload.get("callable_name", "")),
        )


@dataclass(frozen=True)
class WorkflowStepSpec:
    step_id: str
    entrypoint: EntrypointRef
    dependencies: tuple[str, ...] = ()
    expected_artifact_types: tuple[str, ...] = ()
    quality_gate_required: bool = False
    idempotent: bool = True
    lock_key: str | None = None
    timeout_seconds: int = 900
    max_attempts: int = 1
    failure_propagation: FailurePropagation = FailurePropagation.BLOCK_DEPENDENTS
    production_effect: ProductionEffect = ProductionEffect.NONE
    legacy_command: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_text(self.step_id, "step_id")
        dependencies = tuple(sorted(set(self.dependencies)))
        if self.step_id in dependencies:
            raise WorkflowContractError("WORKFLOW_SELF_DEPENDENCY", self.step_id)
        object.__setattr__(self, "dependencies", dependencies)
        object.__setattr__(
            self, "expected_artifact_types", tuple(sorted(set(self.expected_artifact_types)))
        )
        if isinstance(self.timeout_seconds, bool) or self.timeout_seconds <= 0:
            raise WorkflowContractError("INVALID_WORKFLOW_TIMEOUT", self.step_id)
        if isinstance(self.max_attempts, bool) or self.max_attempts < 1:
            raise WorkflowContractError("INVALID_WORKFLOW_ATTEMPTS", self.step_id)
        if not self.idempotent and self.max_attempts != 1:
            raise WorkflowContractError(
                "NON_IDEMPOTENT_RETRY_FORBIDDEN", f"{self.step_id} max_attempts={self.max_attempts}"
            )
        if not self.idempotent and not self.lock_key:
            raise WorkflowContractError("NON_IDEMPOTENT_LOCK_REQUIRED", self.step_id)

    def to_dict(self) -> dict[str, object]:
        return {
            "step_id": self.step_id,
            "entrypoint": self.entrypoint.to_dict(),
            "dependencies": list(self.dependencies),
            "expected_artifact_types": list(self.expected_artifact_types),
            "quality_gate_required": self.quality_gate_required,
            "idempotent": self.idempotent,
            "lock_key": self.lock_key,
            "timeout_seconds": self.timeout_seconds,
            "max_attempts": self.max_attempts,
            "failure_propagation": self.failure_propagation.value,
            "production_effect": self.production_effect.value,
            "legacy_command": list(self.legacy_command),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> WorkflowStepSpec:
        entrypoint = payload.get("entrypoint")
        if not isinstance(entrypoint, Mapping):
            raise WorkflowContractError("INVALID_WORKFLOW_PAYLOAD", "entrypoint must be mapping")
        return cls(
            step_id=str(payload.get("step_id", "")),
            entrypoint=EntrypointRef.from_dict(entrypoint),
            dependencies=tuple(str(item) for item in _list(payload, "dependencies")),
            expected_artifact_types=tuple(
                str(item) for item in _list(payload, "expected_artifact_types")
            ),
            quality_gate_required=payload.get("quality_gate_required") is True,
            idempotent=payload.get("idempotent") is not False,
            lock_key=None if payload.get("lock_key") is None else str(payload.get("lock_key")),
            timeout_seconds=_int_value(payload.get("timeout_seconds", 900), "timeout_seconds"),
            max_attempts=_int_value(payload.get("max_attempts", 1), "max_attempts"),
            failure_propagation=FailurePropagation(
                str(payload.get("failure_propagation", FailurePropagation.BLOCK_DEPENDENTS.value))
            ),
            production_effect=ProductionEffect.parse(
                str(payload.get("production_effect", ProductionEffect.NONE.value))
            ),
            legacy_command=tuple(str(item) for item in _list(payload, "legacy_command")),
        )


@dataclass(frozen=True)
class WorkflowSpec:
    schema_version: ClassVar[str] = "workflow_spec.v1"

    workflow_id: str
    owner: str
    cadence: WorkflowCadence
    timezone: str
    steps: tuple[WorkflowStepSpec, ...]
    due_policy_id: str
    trading_calendar: str | None = None

    def __post_init__(self) -> None:
        for value, field in (
            (self.workflow_id, "workflow_id"),
            (self.owner, "owner"),
            (self.timezone, "timezone"),
            (self.due_policy_id, "due_policy_id"),
        ):
            _require_text(value, field)
        if not self.steps:
            raise WorkflowContractError("WORKFLOW_STEPS_EMPTY", self.workflow_id)
        step_ids = [step.step_id for step in self.steps]
        if len(step_ids) != len(set(step_ids)):
            raise WorkflowContractError("DUPLICATE_WORKFLOW_STEP", self.workflow_id)
        known = set(step_ids)
        for step in self.steps:
            unknown = sorted(set(step.dependencies) - known)
            if unknown:
                raise WorkflowContractError(
                    "UNKNOWN_WORKFLOW_DEPENDENCY", f"{step.step_id}: {','.join(unknown)}"
                )
        self._assert_acyclic()

    def _assert_acyclic(self) -> None:
        dependencies = {step.step_id: set(step.dependencies) for step in self.steps}
        pending = set(dependencies)
        while pending:
            ready = {step_id for step_id in pending if not (dependencies[step_id] & pending)}
            if not ready:
                raise WorkflowContractError("WORKFLOW_DEPENDENCY_CYCLE", ",".join(sorted(pending)))
            pending -= ready

    @property
    def spec_id(self) -> str:
        material = json.dumps(
            self._semantic_payload(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return f"workflow_spec_{hashlib.sha256(material).hexdigest()[:20]}"

    def _semantic_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "workflow_id": self.workflow_id,
            "owner": self.owner,
            "cadence": self.cadence.value,
            "timezone": self.timezone,
            "due_policy_id": self.due_policy_id,
            "trading_calendar": self.trading_calendar,
            "steps": [step.to_dict() for step in self.steps],
        }

    def to_dict(self) -> dict[str, object]:
        return {"spec_id": self.spec_id, **self._semantic_payload()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> WorkflowSpec:
        spec = cls(
            workflow_id=str(payload.get("workflow_id", "")),
            owner=str(payload.get("owner", "")),
            cadence=WorkflowCadence(str(payload.get("cadence", ""))),
            timezone=str(payload.get("timezone", "")),
            due_policy_id=str(payload.get("due_policy_id", "")),
            trading_calendar=(
                None
                if payload.get("trading_calendar") is None
                else str(payload.get("trading_calendar"))
            ),
            steps=tuple(
                WorkflowStepSpec.from_dict(_mapping(item, "step"))
                for item in _list(payload, "steps")
            ),
        )
        supplied_id = payload.get("spec_id")
        if supplied_id is not None and str(supplied_id) != spec.spec_id:
            raise WorkflowContractError("WORKFLOW_SPEC_ID_MISMATCH", spec.workflow_id)
        return spec


@dataclass(frozen=True)
class RunLedgerEntry:
    step_id: str
    status: CanonicalStatus
    attempt: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    artifacts: tuple[ArtifactPointer, ...] = ()
    data_quality: DataQualityEvidence | None = None
    blocker_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_text(self.step_id, "ledger.step_id")
        if isinstance(self.attempt, bool) or self.attempt < 0:
            raise WorkflowContractError("INVALID_LEDGER_ATTEMPT", self.step_id)
        if self.started_at is not None:
            _aware(self.started_at, "ledger.started_at")
        if self.finished_at is not None:
            _aware(self.finished_at, "ledger.finished_at")
        if self.started_at and self.finished_at and self.finished_at < self.started_at:
            raise WorkflowContractError("LEDGER_TIME_REVERSED", self.step_id)
        if self.status is CanonicalStatus.RUNNING and self.started_at is None:
            raise WorkflowContractError("RUNNING_START_REQUIRED", self.step_id)
        if (
            self.status
            in {
                CanonicalStatus.PASS,
                CanonicalStatus.LIMITED,
                CanonicalStatus.BLOCKED,
                CanonicalStatus.FAILED,
            }
            and self.finished_at is None
        ):
            raise WorkflowContractError("TERMINAL_FINISH_REQUIRED", self.step_id)
        blockers = tuple(sorted(set(self.blocker_codes)))
        object.__setattr__(self, "blocker_codes", blockers)
        if self.status in {CanonicalStatus.BLOCKED, CanonicalStatus.FAILED} and not blockers:
            raise WorkflowContractError("LEDGER_BLOCKER_REQUIRED", self.step_id)

    def transition(
        self,
        status: CanonicalStatus,
        *,
        at: datetime,
        artifacts: tuple[ArtifactPointer, ...] = (),
        data_quality: DataQualityEvidence | None = None,
        blocker_codes: tuple[str, ...] = (),
    ) -> RunLedgerEntry:
        _aware(at, "transition.at")
        if status not in _ALLOWED_TRANSITIONS[self.status]:
            raise WorkflowContractError(
                "INVALID_LEDGER_TRANSITION", f"{self.step_id}: {self.status}->{status}"
            )
        return replace(
            self,
            status=status,
            attempt=self.attempt + (1 if status is CanonicalStatus.RUNNING else 0),
            started_at=at if status is CanonicalStatus.RUNNING else self.started_at,
            finished_at=(
                at
                if status
                in {
                    CanonicalStatus.PASS,
                    CanonicalStatus.LIMITED,
                    CanonicalStatus.BLOCKED,
                    CanonicalStatus.FAILED,
                }
                else None
            ),
            artifacts=artifacts,
            data_quality=data_quality,
            blocker_codes=blocker_codes,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "step_id": self.step_id,
            "status": self.status.value,
            "attempt": self.attempt,
            "started_at": None if self.started_at is None else self.started_at.isoformat(),
            "finished_at": None if self.finished_at is None else self.finished_at.isoformat(),
            "artifacts": [item.to_dict() for item in self.artifacts],
            "data_quality": None if self.data_quality is None else self.data_quality.to_dict(),
            "blocker_codes": list(self.blocker_codes),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> RunLedgerEntry:
        quality = payload.get("data_quality")
        return cls(
            step_id=str(payload.get("step_id", "")),
            status=CanonicalStatus(str(payload.get("status", ""))),
            attempt=_int_value(payload.get("attempt", 0), "attempt"),
            started_at=_optional_datetime(payload.get("started_at"), "started_at"),
            finished_at=_optional_datetime(payload.get("finished_at"), "finished_at"),
            artifacts=tuple(
                ArtifactPointer.from_dict(_mapping(item, "artifact"))
                for item in _list(payload, "artifacts")
            ),
            data_quality=(
                None
                if quality is None
                else DataQualityEvidence.from_dict(_mapping(quality, "data_quality"))
            ),
            blocker_codes=tuple(str(item) for item in _list(payload, "blocker_codes")),
        )


@dataclass(frozen=True)
class RunLedger:
    schema_version: ClassVar[str] = "run_ledger.v1"

    workflow_id: str
    workflow_spec_id: str
    run_id: str
    as_of: date
    created_at: datetime
    entries: tuple[RunLedgerEntry, ...]
    run_status: CanonicalStatus | None = None
    run_blocker_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for value, field in (
            (self.workflow_id, "workflow_id"),
            (self.workflow_spec_id, "workflow_spec_id"),
            (self.run_id, "run_id"),
        ):
            _require_text(value, field)
        _aware(self.created_at, "created_at")
        step_ids = [entry.step_id for entry in self.entries]
        if len(step_ids) != len(set(step_ids)):
            raise WorkflowContractError("DUPLICATE_LEDGER_STEP", self.run_id)
        blockers = tuple(sorted(set(self.run_blocker_codes)))
        object.__setattr__(self, "run_blocker_codes", blockers)
        if self.run_status in {CanonicalStatus.BLOCKED, CanonicalStatus.FAILED} and not blockers:
            raise WorkflowContractError("RUN_LEDGER_BLOCKER_REQUIRED", self.run_id)
        if self.run_status not in {CanonicalStatus.BLOCKED, CanonicalStatus.FAILED} and blockers:
            raise WorkflowContractError("RUN_LEDGER_BLOCKER_STATUS_INVALID", self.run_id)

    @classmethod
    def initialize(
        cls, spec: WorkflowSpec, *, run_id: str, as_of: date, created_at: datetime
    ) -> RunLedger:
        return cls(
            workflow_id=spec.workflow_id,
            workflow_spec_id=spec.spec_id,
            run_id=run_id,
            as_of=as_of,
            created_at=created_at,
            entries=tuple(
                RunLedgerEntry(step_id=step.step_id, status=CanonicalStatus.NOT_DUE)
                for step in spec.steps
            ),
        )

    def entry(self, step_id: str) -> RunLedgerEntry:
        for item in self.entries:
            if item.step_id == step_id:
                return item
        raise WorkflowContractError("LEDGER_STEP_UNKNOWN", step_id)

    def with_entry(self, spec: WorkflowSpec, entry: RunLedgerEntry) -> Self:
        step_by_id = {step.step_id: step for step in spec.steps}
        if spec.spec_id != self.workflow_spec_id or entry.step_id not in step_by_id:
            raise WorkflowContractError("LEDGER_SPEC_MISMATCH", entry.step_id)
        step = step_by_id[entry.step_id]
        states = {item.step_id: item.status for item in self.entries}
        if entry.status in {CanonicalStatus.RUNNING, CanonicalStatus.PASS}:
            unmet = [
                dependency
                for dependency in step.dependencies
                if states.get(dependency) not in {CanonicalStatus.PASS, CanonicalStatus.SKIPPED}
            ]
            if unmet:
                raise WorkflowContractError(
                    "WORKFLOW_DEPENDENCY_NOT_PASSED", f"{entry.step_id}: {','.join(unmet)}"
                )
        if entry.status is CanonicalStatus.PASS and step.quality_gate_required:
            if entry.data_quality is None:
                raise WorkflowContractError("WORKFLOW_DQ_EVIDENCE_REQUIRED", entry.step_id)
            try:
                entry.data_quality.assert_ready()
            except ValueError as exc:
                raise WorkflowContractError("WORKFLOW_DQ_NOT_READY", str(exc)) from exc
        updated = tuple(entry if item.step_id == entry.step_id else item for item in self.entries)
        return replace(self, entries=updated)

    @property
    def ledger_id(self) -> str:
        material = json.dumps(
            self.to_dict(include_id=False),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return f"run_ledger_{hashlib.sha256(material).hexdigest()[:20]}"

    def to_dict(self, *, include_id: bool = True) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": self.schema_version,
            "workflow_id": self.workflow_id,
            "workflow_spec_id": self.workflow_spec_id,
            "run_id": self.run_id,
            "as_of": self.as_of.isoformat(),
            "created_at": self.created_at.isoformat(),
            "entries": [entry.to_dict() for entry in self.entries],
        }
        if self.run_status is not None:
            payload["run_status"] = self.run_status.value
            payload["run_blocker_codes"] = list(self.run_blocker_codes)
        return {"ledger_id": self.ledger_id, **payload} if include_id else payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> RunLedger:
        ledger = cls(
            workflow_id=str(payload.get("workflow_id", "")),
            workflow_spec_id=str(payload.get("workflow_spec_id", "")),
            run_id=str(payload.get("run_id", "")),
            as_of=_date_value(payload.get("as_of"), "as_of"),
            created_at=_datetime_value(payload.get("created_at"), "created_at"),
            entries=tuple(
                RunLedgerEntry.from_dict(_mapping(item, "entry"))
                for item in _list(payload, "entries")
            ),
            run_status=(
                None
                if payload.get("run_status") is None
                else CanonicalStatus(str(payload.get("run_status")))
            ),
            run_blocker_codes=tuple(str(item) for item in _list(payload, "run_blocker_codes")),
        )
        supplied_id = payload.get("ledger_id")
        if supplied_id is not None and str(supplied_id) != ledger.ledger_id:
            raise WorkflowContractError("RUN_LEDGER_ID_MISMATCH", ledger.run_id)
        return ledger


def _list(payload: Mapping[str, object], field: str) -> list[object]:
    value = payload.get(field, [])
    if not isinstance(value, list):
        raise WorkflowContractError("INVALID_WORKFLOW_PAYLOAD", f"{field} must be list")
    return value


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise WorkflowContractError("INVALID_WORKFLOW_PAYLOAD", f"{field} must be mapping")
    return value
