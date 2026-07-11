from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.operations import (
    OperationsExecutionState,
    OperationsRunControlResolution,
    OperationsRunDecision,
)
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.workflow import WorkflowSpec, WorkflowStepSpec
from ai_trading_system.platform.artifacts import write_json_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_OPERATIONS_RUNTIME_CONTROL_POLICY_PATH = (
    PROJECT_ROOT / "config" / "operations" / "runtime_control.yaml"
)


class OperationsRuntimeControlError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class OperationsRuntimeControlPolicy:
    policy_id: str
    owner: str
    version: str
    lock_ttl_seconds: int
    max_run_attempts: int
    resume_idempotent_steps: bool
    legacy_daily_executor_cut_in_enabled: bool
    non_daily_dispatch_enabled: bool

    def __post_init__(self) -> None:
        if not self.policy_id.strip() or not self.owner.strip() or not self.version.strip():
            raise OperationsRuntimeControlError("RUNTIME_POLICY_FIELD_MISSING", self.policy_id)
        if isinstance(self.lock_ttl_seconds, bool) or self.lock_ttl_seconds < 1:
            raise OperationsRuntimeControlError("RUNTIME_POLICY_TTL_INVALID", self.policy_id)
        if isinstance(self.max_run_attempts, bool) or self.max_run_attempts < 1:
            raise OperationsRuntimeControlError("RUNTIME_POLICY_ATTEMPTS_INVALID", self.policy_id)


@dataclass(frozen=True)
class _LockRecord:
    lock_key: str
    owner_run_id: str
    workflow_id: str
    acquired_at: datetime
    heartbeat_at: datetime
    expires_at: datetime

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "operations_lock_record.v1",
            "lock_key": self.lock_key,
            "owner_run_id": self.owner_run_id,
            "workflow_id": self.workflow_id,
            "acquired_at": self.acquired_at.isoformat(),
            "heartbeat_at": self.heartbeat_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, payload: object) -> _LockRecord:
        if not isinstance(payload, dict):
            raise OperationsRuntimeControlError("LOCK_RECORD_INVALID", "payload is not mapping")
        try:
            return cls(
                lock_key=str(payload["lock_key"]),
                owner_run_id=str(payload["owner_run_id"]),
                workflow_id=str(payload["workflow_id"]),
                acquired_at=_parse_datetime(payload["acquired_at"]),
                heartbeat_at=_parse_datetime(payload["heartbeat_at"]),
                expires_at=_parse_datetime(payload["expires_at"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise OperationsRuntimeControlError("LOCK_RECORD_INVALID", str(exc)) from exc


@dataclass(frozen=True)
class OperationsRunControlAcquisition:
    resolution: OperationsRunControlResolution
    lease: OperationsRunControlLease | None


class OperationsRunControlLease:
    def __init__(
        self,
        *,
        control: OperationsRunControl,
        spec: WorkflowSpec,
        lock_record: _LockRecord,
        state: OperationsExecutionState,
    ) -> None:
        self._control = control
        self.spec = spec
        self.lock_record = lock_record
        self.state = state
        self._released = False

    def heartbeat(self, *, at: datetime | None = None) -> None:
        timestamp = at or datetime.now(tz=UTC)
        self.lock_record = replace(
            self.lock_record,
            heartbeat_at=timestamp,
            expires_at=timestamp + timedelta(seconds=self._control.policy.lock_ttl_seconds),
        )
        self._control._write_lock_record(self.lock_record)

    def start_step(self, step_id: str, *, at: datetime | None = None) -> None:
        step = self._step(step_id)
        if self.state.step_attempt_count(step_id) >= step.max_attempts:
            raise OperationsRuntimeControlError("STEP_ATTEMPT_BUDGET_EXHAUSTED", step_id)
        timestamp = at or datetime.now(tz=UTC)
        self.heartbeat(at=timestamp)
        self.state = self.state.with_step_started(step_id=step_id, at=timestamp)
        self._control._write_state(self.state)

    def fail_step(
        self,
        step_id: str,
        *,
        retryable: bool,
        blocker_code: str,
        at: datetime | None = None,
    ) -> bool:
        step = self._step(step_id)
        timestamp = at or datetime.now(tz=UTC)
        can_retry = (
            retryable
            and step.idempotent
            and self.state.step_attempt_count(step_id) < step.max_attempts
        )
        if can_retry:
            self.state = self.state.with_step_retry_ready(step_id=step_id, at=timestamp)
            self._control._write_state(self.state)
            return True
        self.finish(
            CanonicalStatus.FAILED,
            blocker_codes=(blocker_code,),
            at=timestamp,
        )
        return False

    def pass_step(self, step_id: str, *, at: datetime | None = None) -> None:
        timestamp = at or datetime.now(tz=UTC)
        self.state = self.state.with_step_passed(step_id=step_id, at=timestamp)
        self._control._write_state(self.state)

    def finish(
        self,
        status: CanonicalStatus,
        *,
        blocker_codes: tuple[str, ...] = (),
        at: datetime | None = None,
    ) -> None:
        timestamp = at or datetime.now(tz=UTC)
        if status is CanonicalStatus.PASS:
            expected = {step.step_id for step in self.spec.steps}
            if set(self.state.completed_step_ids) != expected:
                raise OperationsRuntimeControlError(
                    "RUN_COMPLETE_STEPS_MISSING",
                    ",".join(sorted(expected - set(self.state.completed_step_ids))),
                )
        self.state = self.state.terminal(
            status=status,
            at=timestamp,
            blocker_codes=blocker_codes,
        )
        self._control._write_state(self.state)
        self.release()

    def release(self) -> None:
        if not self._released:
            self._control._release_lock(self.lock_record)
            self._released = True

    def _step(self, step_id: str) -> WorkflowStepSpec:
        for step in self.spec.steps:
            if step.step_id == step_id:
                return step
        raise OperationsRuntimeControlError("RUN_STEP_UNKNOWN", step_id)

    def __enter__(self) -> OperationsRunControlLease:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.release()


class OperationsRunControl:
    def __init__(
        self,
        *,
        root: Path,
        policy: OperationsRuntimeControlPolicy,
    ) -> None:
        self.root = root
        self.policy = policy
        self.lock_root = root / "locks"
        self.state_root = root / "states"

    def acquire(
        self,
        *,
        spec: WorkflowSpec,
        as_of: date,
        run_id: str,
        now: datetime | None = None,
    ) -> OperationsRunControlAcquisition:
        timestamp = now or datetime.now(tz=UTC)
        key = operations_idempotency_key(spec=spec, as_of=as_of)
        lock_record = self._acquire_lock(
            lock_key=f"{spec.workflow_id}:{as_of.isoformat()}",
            owner_run_id=run_id,
            workflow_id=spec.workflow_id,
            now=timestamp,
        )
        if lock_record is None:
            return self._without_lease(
                decision=OperationsRunDecision.BLOCKED_CONCURRENT,
                spec=spec,
                as_of=as_of,
                key=key,
                blocker_codes=("ACTIVE_WORKFLOW_DATE_LOCK",),
            )
        try:
            prior = self._read_state(key)
            if prior is not None and prior.status is CanonicalStatus.PASS:
                self._release_lock(lock_record)
                return self._without_lease(
                    decision=OperationsRunDecision.ALREADY_COMPLETE,
                    spec=spec,
                    as_of=as_of,
                    key=key,
                    attempt=prior.attempt,
                    completed=prior.completed_step_ids,
                )
            if prior is not None and prior.attempt >= self.policy.max_run_attempts:
                self._release_lock(lock_record)
                return self._without_lease(
                    decision=OperationsRunDecision.BLOCKED_RETRY_EXHAUSTED,
                    spec=spec,
                    as_of=as_of,
                    key=key,
                    attempt=prior.attempt,
                    completed=prior.completed_step_ids,
                    blocker_codes=("RUN_ATTEMPT_BUDGET_EXHAUSTED",),
                )
            exhausted_steps = self._exhausted_resume_steps(spec=spec, prior=prior)
            if exhausted_steps:
                self._release_lock(lock_record)
                return self._without_lease(
                    decision=OperationsRunDecision.BLOCKED_RETRY_EXHAUSTED,
                    spec=spec,
                    as_of=as_of,
                    key=key,
                    attempt=0 if prior is None else prior.attempt,
                    completed=() if prior is None else prior.completed_step_ids,
                    blocker_codes=tuple(
                        f"STEP_ATTEMPT_BUDGET_EXHAUSTED:{item}" for item in exhausted_steps
                    ),
                )
            unsafe = self._unsafe_resume_steps(spec=spec, prior=prior)
            if unsafe:
                self._release_lock(lock_record)
                return self._without_lease(
                    decision=OperationsRunDecision.BLOCKED_UNSAFE_RESUME,
                    spec=spec,
                    as_of=as_of,
                    key=key,
                    attempt=0 if prior is None else prior.attempt,
                    completed=() if prior is None else prior.completed_step_ids,
                    blocker_codes=tuple(f"NON_IDEMPOTENT_RESUME:{item}" for item in unsafe),
                )
            if prior is None:
                decision = OperationsRunDecision.START_NEW
                state = OperationsExecutionState(
                    idempotency_key=key,
                    workflow_id=spec.workflow_id,
                    workflow_spec_id=spec.spec_id,
                    as_of=as_of,
                    run_id=run_id,
                    status=CanonicalStatus.RUNNING,
                    attempt=1,
                    started_at=timestamp,
                    updated_at=timestamp,
                )
            else:
                decision = OperationsRunDecision.RESUME
                state = replace(
                    prior,
                    run_id=run_id,
                    status=CanonicalStatus.RUNNING,
                    attempt=prior.attempt + 1,
                    updated_at=timestamp,
                    finished_at=None,
                    current_step_id=None,
                    blocker_codes=(),
                )
            self._write_state(state)
            resolution = OperationsRunControlResolution(
                decision=decision,
                idempotency_key=key,
                workflow_id=spec.workflow_id,
                workflow_spec_id=spec.spec_id,
                as_of=as_of,
                attempt=state.attempt,
                resume_completed_step_ids=state.completed_step_ids,
            )
            return OperationsRunControlAcquisition(
                resolution=resolution,
                lease=OperationsRunControlLease(
                    control=self,
                    spec=spec,
                    lock_record=lock_record,
                    state=state,
                ),
            )
        except Exception:
            self._release_lock(lock_record)
            raise

    def _unsafe_resume_steps(
        self, *, spec: WorkflowSpec, prior: OperationsExecutionState | None
    ) -> tuple[str, ...]:
        if prior is None:
            return ()
        if prior.workflow_spec_id != spec.spec_id:
            raise OperationsRuntimeControlError("RUN_STATE_SPEC_MISMATCH", prior.workflow_spec_id)
        step_by_id = {step.step_id: step for step in spec.steps}
        affected = set(prior.completed_step_ids)
        if prior.current_step_id is not None:
            affected.add(prior.current_step_id)
        unknown = affected - set(step_by_id)
        if unknown:
            raise OperationsRuntimeControlError("RUN_STATE_STEP_UNKNOWN", ",".join(sorted(unknown)))
        if not self.policy.resume_idempotent_steps and affected:
            return tuple(sorted(affected))
        return tuple(sorted(step_id for step_id in affected if not step_by_id[step_id].idempotent))

    def _exhausted_resume_steps(
        self, *, spec: WorkflowSpec, prior: OperationsExecutionState | None
    ) -> tuple[str, ...]:
        if prior is None or prior.current_step_id is None:
            return ()
        step_by_id = {step.step_id: step for step in spec.steps}
        step = step_by_id.get(prior.current_step_id)
        if step is None:
            raise OperationsRuntimeControlError("RUN_STATE_STEP_UNKNOWN", prior.current_step_id)
        if prior.step_attempt_count(step.step_id) >= step.max_attempts:
            return (step.step_id,)
        return ()

    def _without_lease(
        self,
        *,
        decision: OperationsRunDecision,
        spec: WorkflowSpec,
        as_of: date,
        key: str,
        attempt: int = 0,
        completed: tuple[str, ...] = (),
        blocker_codes: tuple[str, ...] = (),
    ) -> OperationsRunControlAcquisition:
        return OperationsRunControlAcquisition(
            resolution=OperationsRunControlResolution(
                decision=decision,
                idempotency_key=key,
                workflow_id=spec.workflow_id,
                workflow_spec_id=spec.spec_id,
                as_of=as_of,
                attempt=attempt,
                resume_completed_step_ids=completed,
                blocker_codes=blocker_codes,
            ),
            lease=None,
        )

    def _acquire_lock(
        self,
        *,
        lock_key: str,
        owner_run_id: str,
        workflow_id: str,
        now: datetime,
    ) -> _LockRecord | None:
        lock_dir = self._lock_dir(lock_key)
        lock_dir.parent.mkdir(parents=True, exist_ok=True)
        try:
            lock_dir.mkdir()
        except FileExistsError:
            current = self._read_lock_record(lock_dir)
            if current is None or current.expires_at > now:
                return None
            stale_dir = lock_dir.with_name(f"{lock_dir.name}.stale.{owner_run_id}")
            try:
                os.replace(lock_dir, stale_dir)
            except OSError:
                return None
            self._remove_lock_dir(stale_dir)
            try:
                lock_dir.mkdir()
            except FileExistsError:
                return None
        record = _LockRecord(
            lock_key=lock_key,
            owner_run_id=owner_run_id,
            workflow_id=workflow_id,
            acquired_at=now,
            heartbeat_at=now,
            expires_at=now + timedelta(seconds=self.policy.lock_ttl_seconds),
        )
        try:
            self._write_lock_record(record)
        except Exception:
            self._remove_lock_dir(lock_dir)
            raise
        return record

    def _release_lock(self, expected: _LockRecord) -> None:
        lock_dir = self._lock_dir(expected.lock_key)
        current = self._read_lock_record(lock_dir)
        if current is None:
            raise OperationsRuntimeControlError("LOCK_OWNER_RECORD_MISSING", expected.lock_key)
        if current.owner_run_id != expected.owner_run_id:
            raise OperationsRuntimeControlError("LOCK_OWNER_MISMATCH", expected.lock_key)
        self._remove_lock_dir(lock_dir)

    def _write_lock_record(self, record: _LockRecord) -> None:
        write_json_atomic(self._lock_dir(record.lock_key) / "owner.json", record.to_dict())

    def _read_lock_record(self, lock_dir: Path) -> _LockRecord | None:
        path = lock_dir / "owner.json"
        if not path.exists():
            return None
        try:
            return _LockRecord.from_dict(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError, OperationsRuntimeControlError):
            return None

    def _remove_lock_dir(self, lock_dir: Path) -> None:
        owner_path = lock_dir / "owner.json"
        owner_path.unlink(missing_ok=True)
        try:
            lock_dir.rmdir()
        except FileNotFoundError:
            pass

    def _lock_dir(self, lock_key: str) -> Path:
        digest = hashlib.sha256(lock_key.encode("utf-8")).hexdigest()
        return self.lock_root / digest

    def _state_path(self, key: str) -> Path:
        return self.state_root / f"{key}.json"

    def _read_state(self, key: str) -> OperationsExecutionState | None:
        path = self._state_path(key)
        if not path.exists():
            return None
        try:
            return OperationsExecutionState.from_dict(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            raise OperationsRuntimeControlError("RUN_STATE_INVALID", str(exc)) from exc

    def _write_state(self, state: OperationsExecutionState) -> None:
        write_json_atomic(self._state_path(state.idempotency_key), state.to_dict())


def operations_idempotency_key(*, spec: WorkflowSpec, as_of: date) -> str:
    material = f"{spec.workflow_id}|{spec.spec_id}|{as_of.isoformat()}".encode()
    return f"operations_run_{hashlib.sha256(material).hexdigest()[:24]}"


def load_operations_runtime_control_policy(
    path: Path = DEFAULT_OPERATIONS_RUNTIME_CONTROL_POLICY_PATH,
) -> OperationsRuntimeControlPolicy:
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise OperationsRuntimeControlError("RUNTIME_POLICY_INVALID", str(path))
    if payload.get("schema_version") != "operations_runtime_control_policy.v1":
        raise OperationsRuntimeControlError("RUNTIME_POLICY_SCHEMA_INVALID", str(path))
    return OperationsRuntimeControlPolicy(
        policy_id=str(payload.get("policy_id", "")),
        owner=str(payload.get("owner", "")),
        version=str(payload.get("version", "")),
        lock_ttl_seconds=_required_int(payload, "lock_ttl_seconds"),
        max_run_attempts=_required_int(payload, "max_run_attempts"),
        resume_idempotent_steps=payload.get("resume_idempotent_steps") is True,
        legacy_daily_executor_cut_in_enabled=(
            payload.get("legacy_daily_executor_cut_in_enabled") is True
        ),
        non_daily_dispatch_enabled=payload.get("non_daily_dispatch_enabled") is True,
    )


def _required_int(payload: dict[object, object], field: str) -> int:
    value = payload.get(field)
    if not isinstance(value, int) or isinstance(value, bool):
        raise OperationsRuntimeControlError("RUNTIME_POLICY_INTEGER_INVALID", field)
    return value


def _parse_datetime(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("datetime must be string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return parsed
