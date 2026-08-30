from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path, PurePosixPath
from threading import Event, Thread
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.architecture.parallel_control import (
    ChangeManifest,
    ContractAccess,
    ContractClaim,
    LaneRole,
    ParallelControlError,
)
from ai_trading_system.platform.architecture.parallel_control_kernel import (
    ExecutionLease,
    FileExecutionLeaseStore,
    LeaseReplay,
    ParallelControlPolicy,
    ReadinessDecision,
    TaskControlRecord,
    load_parallel_control_policy,
)
from ai_trading_system.platform.artifacts import write_json_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

CHECKOUT_GUARD_POLICY_SCHEMA_VERSION = "arch_005_s4d_checkout_guard_policy.v2"
CHECKOUT_INTENT_SCHEMA_VERSION = "checkout_operation_intent.v1"
CHECKOUT_DECISION_SCHEMA_VERSION = "checkout_guard_decision.v1"
CHECKOUT_WORKTREE_AUDIT_SCHEMA_VERSION = "checkout_worktree_audit.v2"
DEFAULT_CHECKOUT_GUARD_POLICY_PATH = (
    PROJECT_ROOT / "config" / "architecture" / "arch_005_s4d_checkout_guard.yaml"
)
DEFAULT_PARALLEL_CONTROL_POLICY_PATH = (
    PROJECT_ROOT / "config" / "architecture" / "arch_005_parallel_control_policy.yaml"
)


class CheckoutGuardError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class CheckoutOperationClass(StrEnum):
    DOMAIN_MUTATION = "domain_mutation"
    SHARED_MUTATION = "shared_mutation"
    DAILY_OPERATION = "daily_operation"
    READ_ONLY_AUDIT = "read_only_audit"


@dataclass(frozen=True)
class KnownUnrelatedExclusion:
    path: str
    rationale: str
    owner_ref: str

    def to_dict(self) -> dict[str, str]:
        return {
            "path": self.path,
            "rationale": self.rationale,
            "owner_ref": self.owner_ref,
        }


@dataclass(frozen=True)
class CheckoutGuardPolicy:
    policy_id: str
    version: str
    status: str
    owner: str
    approval_ref: str
    runtime_root: str
    identity_method: str
    require_exact_head: bool
    require_git_checkout: bool
    protected_branches: tuple[str, ...]
    protected_branch_domain_mutation_allowed: bool
    protected_branch_shared_mutation_actors: tuple[str, ...]
    operation_gate_access: tuple[tuple[CheckoutOperationClass, ContractAccess], ...]
    lease_ttl_seconds: int
    heartbeat_interval_seconds: int
    max_reassignments: int
    arbiter_ttl_seconds: int
    max_total_active_leases: int
    authority_task_id: str
    allowlisted_actors: tuple[str, ...]
    known_unrelated_exclusions: tuple[KnownUnrelatedExclusion, ...]

    @property
    def policy_version(self) -> str:
        return f"{self.policy_id}@{self.version}"

    def gate_access(self, operation_class: CheckoutOperationClass) -> ContractAccess:
        return dict(self.operation_gate_access)[operation_class]


@dataclass(frozen=True)
class CheckoutIdentity:
    workspace_id: str
    checkout_root: str
    git_common_dir: str
    head_commit: str
    branch_name: str | None
    upstream_ref: str | None
    upstream_commit: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "workspace_id": self.workspace_id,
            "checkout_root": self.checkout_root,
            "git_common_dir": self.git_common_dir,
            "head_commit": self.head_commit,
            "branch_name": self.branch_name,
            "upstream_ref": self.upstream_ref,
            "upstream_commit": self.upstream_commit,
        }


@dataclass(frozen=True)
class RegisteredWorktree:
    toplevel: str
    head_commit: str
    branch_ref: str | None
    detached: bool
    locked_reason: str | None
    prunable_reason: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "toplevel": self.toplevel,
            "head_commit": self.head_commit,
            "branch_ref": self.branch_ref,
            "detached": self.detached,
            "locked_reason": self.locked_reason,
            "prunable_reason": self.prunable_reason,
        }


@dataclass(frozen=True)
class WorktreeAuditBinding:
    policy_identity: CheckoutIdentity
    audited_identity: CheckoutIdentity
    registration: RegisteredWorktree


@dataclass(frozen=True)
class CheckoutOperationIntent:
    intent_id: str
    task_id: str
    thread_id: str
    actor: str
    operation_class: CheckoutOperationClass
    base_commit: str
    owned_paths: tuple[str, ...]
    shared_paths: tuple[str, ...]
    workspace_identity: CheckoutIdentity
    observed_dirty_paths: tuple[str, ...]
    known_unrelated_exclusions: tuple[KnownUnrelatedExclusion, ...]
    created_at: datetime

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": CHECKOUT_INTENT_SCHEMA_VERSION,
            "intent_id": self.intent_id,
            "task_id": self.task_id,
            "thread_id": self.thread_id,
            "actor": self.actor,
            "operation_class": self.operation_class.value,
            "base_commit": self.base_commit,
            "owned_paths": list(self.owned_paths),
            "shared_paths": list(self.shared_paths),
            "workspace_identity": self.workspace_identity.to_dict(),
            "observed_dirty_paths": list(self.observed_dirty_paths),
            "known_unrelated_exclusions": [
                exclusion.to_dict() for exclusion in self.known_unrelated_exclusions
            ],
            "task_source_cutover": False,
            "production_effect": "none",
            "broker_action": "none",
            "created_at": self.created_at.isoformat(),
        }


@dataclass(frozen=True)
class CheckoutGuardDecision:
    status: str
    reason_codes: tuple[str, ...]
    intent: CheckoutOperationIntent
    intent_path: Path
    lease_id: str | None
    lease_state: str | None

    def to_dict(self) -> dict[str, object]:
        daily_allowed = (
            self.status == "PASS"
            and self.intent.operation_class is CheckoutOperationClass.DAILY_OPERATION
        )
        return {
            "schema_version": CHECKOUT_DECISION_SCHEMA_VERSION,
            "status": self.status,
            "reason_codes": list(self.reason_codes),
            "intent": self.intent.to_dict(),
            "intent_path": self.intent_path.as_posix(),
            "lease_id": self.lease_id,
            "lease_state": self.lease_state,
            "provider_request_allowed": daily_allowed,
            "cache_mutation_allowed": daily_allowed,
            "report_mutation_allowed": daily_allowed,
            "task_source_cutover": False,
            "production_effect": "none",
            "broker_action": "none",
        }


@dataclass(frozen=True)
class CheckoutWorktreeAudit:
    policy_identity: CheckoutIdentity
    audited_identity: CheckoutIdentity
    registration: RegisteredWorktree
    dirty_paths: tuple[str, ...]
    known_unrelated_exclusions: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": CHECKOUT_WORKTREE_AUDIT_SCHEMA_VERSION,
            "status": "PASS",
            "policy_repository": {
                "toplevel": self.policy_identity.checkout_root,
                "git_common_dir": self.policy_identity.git_common_dir,
                "workspace_id": self.policy_identity.workspace_id,
                "head_commit": self.policy_identity.head_commit,
                "branch_name": self.policy_identity.branch_name,
            },
            "audited_repository": {
                "toplevel": self.audited_identity.checkout_root,
                "git_common_dir": self.audited_identity.git_common_dir,
                "workspace_id": self.audited_identity.workspace_id,
                "head_commit": self.audited_identity.head_commit,
                "branch_name": self.audited_identity.branch_name,
            },
            "worktree_registration": self.registration.to_dict(),
            "same_git_common_dir": True,
            "dirty_paths": list(self.dirty_paths),
            "known_unrelated_exclusions": list(self.known_unrelated_exclusions),
            "unstaged_diff_check": "PASS",
            "staged_diff_check": "PASS",
            "task_governance_status_mutated": False,
            "production_effect": "none",
            "broker_action": "none",
        }


class CheckoutLeaseHandle:
    def __init__(
        self,
        *,
        guard: CheckoutLeaseGuard,
        decision: CheckoutGuardDecision,
    ) -> None:
        if decision.status != "PASS" or decision.lease_id is None:
            raise CheckoutGuardError("CHECKOUT_LEASE_NOT_ACTIVE", decision.status)
        self.guard = guard
        self.decision = decision
        self.lease_id = decision.lease_id
        self.actor = decision.intent.actor
        self._released = False

    @property
    def released(self) -> bool:
        return self._released

    def heartbeat(self, *, at: datetime | None = None) -> None:
        if self._released:
            raise CheckoutGuardError("CHECKOUT_LEASE_RELEASED", self.lease_id)
        self.guard.store.heartbeat(
            self.lease_id,
            actor=self.actor,
            now=at or datetime.now(tz=UTC),
        )

    def release(
        self,
        *,
        outcome: str,
        evidence_refs: Sequence[str] = (),
        at: datetime | None = None,
    ) -> None:
        if self._released:
            return
        reason = _identifier(outcome, "outcome").upper()
        try:
            self.guard.release(
                self.lease_id,
                actor=self.actor,
                outcome=reason,
                evidence_refs=evidence_refs,
                now=at,
            )
        except CheckoutGuardError as exc:
            if exc.code == "CHECKOUT_RELEASE_DIRTY_UNATTRIBUTED":
                self._released = True
            raise
        else:
            self._released = True


class CheckoutLeaseGuard:
    def __init__(
        self,
        *,
        project_root: Path,
        runtime_root: Path | None = None,
        policy_path: Path = DEFAULT_CHECKOUT_GUARD_POLICY_PATH,
        parallel_policy_path: Path = DEFAULT_PARALLEL_CONTROL_POLICY_PATH,
    ) -> None:
        self.project_root = project_root.resolve()
        self.policy = load_checkout_guard_policy(policy_path)
        parallel_policy = load_parallel_control_policy(parallel_policy_path)
        self.lease_policy = _checkout_lease_policy(parallel_policy, self.policy)
        self.runtime_root = (
            runtime_root.resolve()
            if runtime_root is not None
            else (self.project_root / self.policy.runtime_root).resolve()
        )
        if not self.runtime_root.is_relative_to(self.project_root):
            raise CheckoutGuardError("CHECKOUT_RUNTIME_ROOT_OUTSIDE", str(self.runtime_root))
        self.store = FileExecutionLeaseStore(
            self.runtime_root / "leases",
            policy=self.lease_policy,
        )

    def replay(self) -> LeaseReplay:
        return self.store.replay()

    def audit_worktree(
        self,
        *,
        policy_project_root: Path | None = None,
    ) -> CheckoutWorktreeAudit:
        policy_root = (
            self.project_root if policy_project_root is None else policy_project_root.resolve()
        )
        binding = resolve_worktree_audit_binding(
            policy_project_root=policy_root,
            audited_project_root=self.project_root,
        )
        exclusions = tuple(row.path for row in self.policy.known_unrelated_exclusions)
        dirty_paths = collect_checkout_dirty_paths(
            self.project_root,
            exclusions=exclusions,
        )
        _run_git_diff_check(
            self.project_root,
            exclusions=exclusions,
            cached=False,
        )
        _run_git_diff_check(
            self.project_root,
            exclusions=exclusions,
            cached=True,
        )
        verified_binding = resolve_worktree_audit_binding(
            policy_project_root=policy_root,
            audited_project_root=self.project_root,
        )
        if verified_binding != binding:
            raise CheckoutGuardError(
                "CHECKOUT_AUDIT_IDENTITY_DRIFT",
                (
                    f"before={_worktree_binding_summary(binding)};"
                    f"after={_worktree_binding_summary(verified_binding)}"
                ),
            )
        return CheckoutWorktreeAudit(
            policy_identity=binding.policy_identity,
            audited_identity=binding.audited_identity,
            registration=binding.registration,
            dirty_paths=dirty_paths,
            known_unrelated_exclusions=exclusions,
        )

    def release(
        self,
        lease_id: str,
        *,
        actor: str,
        outcome: str,
        evidence_refs: Sequence[str] = (),
        now: datetime | None = None,
    ) -> ExecutionLease:
        instant = _aware_utc(now or datetime.now(tz=UTC))
        replay = self.store.replay()
        head = {lease.lease_id: lease for lease in replay.lease_heads}.get(lease_id)
        if head is None:
            raise CheckoutGuardError("CHECKOUT_LEASE_UNKNOWN", lease_id)
        intent_id = head.change_id.removeprefix("checkout:")
        if f"checkout:{intent_id}" != head.change_id:
            raise CheckoutGuardError(
                "CHECKOUT_RELEASE_CHANGE_ID",
                head.change_id,
            )
        intent_path = self.runtime_root / "intents" / f"{intent_id}.json"
        declared_paths, operation_class = _load_release_scope(
            intent_path,
            expected_intent_id=intent_id,
        )
        status_exclusions = (
            *(row.path for row in self.policy.known_unrelated_exclusions),
            self.runtime_root.relative_to(self.project_root).as_posix(),
        )
        dirty_paths = collect_checkout_dirty_paths(
            self.project_root,
            exclusions=status_exclusions,
        )
        unattributed = _unattributed_dirty_paths(
            dirty_paths,
            operation_class=operation_class,
            declared_paths=declared_paths,
        )
        reason = _identifier(outcome, "outcome").upper()
        reason_codes = (
            f"CHECKOUT_OPERATION_{reason}",
            *(f"CHECKOUT_RELEASE_DIRTY_UNATTRIBUTED:{path}" for path in unattributed),
        )
        released = self.store.release(
            lease_id,
            actor=actor,
            now=instant,
            evidence_refs=(intent_path.as_posix(), *evidence_refs),
            reason_codes=reason_codes,
        )
        if unattributed:
            raise CheckoutGuardError(
                "CHECKOUT_RELEASE_DIRTY_UNATTRIBUTED",
                ",".join(unattributed),
            )
        return released

    def acquire(
        self,
        *,
        intent_id: str,
        task_id: str,
        thread_id: str,
        actor: str,
        operation_class: CheckoutOperationClass,
        owned_paths: Sequence[str] = (),
        shared_paths: Sequence[str] = (),
        base_commit: str | None = None,
        now: datetime | None = None,
    ) -> tuple[CheckoutGuardDecision, CheckoutLeaseHandle | None]:
        instant = _aware_utc(now or datetime.now(tz=UTC))
        if actor not in self.policy.allowlisted_actors:
            raise CheckoutGuardError("CHECKOUT_ACTOR_NOT_ALLOWLISTED", actor)
        identity = resolve_checkout_identity(self.project_root)
        if identity.branch_name in self.policy.protected_branches:
            if (
                operation_class is CheckoutOperationClass.DOMAIN_MUTATION
                and not self.policy.protected_branch_domain_mutation_allowed
            ):
                raise CheckoutGuardError(
                    "CHECKOUT_PROTECTED_BRANCH_DOMAIN_MUTATION",
                    identity.branch_name,
                )
            if (
                operation_class is CheckoutOperationClass.SHARED_MUTATION
                and actor not in self.policy.protected_branch_shared_mutation_actors
            ):
                raise CheckoutGuardError(
                    "CHECKOUT_PROTECTED_BRANCH_COORDINATOR_REQUIRED",
                    f"{identity.branch_name}:{actor}",
                )
        checked_base = base_commit or identity.head_commit
        if self.policy.require_exact_head and checked_base != identity.head_commit:
            raise CheckoutGuardError(
                "CHECKOUT_BASE_HEAD_DRIFT",
                f"declared={checked_base};head={identity.head_commit}",
            )
        checked_owned = _checked_paths(self.project_root, owned_paths, "owned_paths")
        checked_shared = _checked_paths(self.project_root, shared_paths, "shared_paths")
        if set(checked_owned) & set(checked_shared):
            raise CheckoutGuardError(
                "CHECKOUT_INTRA_INTENT_PATH_OVERLAP",
                ",".join(sorted(set(checked_owned) & set(checked_shared))),
            )
        if operation_class in {
            CheckoutOperationClass.DOMAIN_MUTATION,
            CheckoutOperationClass.SHARED_MUTATION,
        }:
            if not checked_owned and not checked_shared:
                raise CheckoutGuardError("CHECKOUT_MUTATION_SCOPE_EMPTY", intent_id)
        elif checked_owned or checked_shared:
            raise CheckoutGuardError(
                "CHECKOUT_NONMUTATION_PATH_SCOPE",
                operation_class.value,
            )
        status_exclusions = (
            *(row.path for row in self.policy.known_unrelated_exclusions),
            self.runtime_root.relative_to(self.project_root).as_posix(),
        )
        dirty_paths = collect_checkout_dirty_paths(
            self.project_root,
            exclusions=status_exclusions,
        )
        unattributed = _unattributed_dirty_paths(
            dirty_paths,
            operation_class=operation_class,
            declared_paths=(*checked_owned, *checked_shared),
        )
        intent = CheckoutOperationIntent(
            intent_id=_identifier(intent_id, "intent_id"),
            task_id=_required_text(task_id, "task_id"),
            thread_id=_required_text(thread_id, "thread_id"),
            actor=actor,
            operation_class=operation_class,
            base_commit=checked_base,
            owned_paths=checked_owned,
            shared_paths=checked_shared,
            workspace_identity=identity,
            observed_dirty_paths=dirty_paths,
            known_unrelated_exclusions=self.policy.known_unrelated_exclusions,
            created_at=instant,
        )
        intent_path = self.runtime_root / "intents" / f"{intent.intent_id}.json"
        intent = _persist_or_replay_intent(intent_path, intent)
        if unattributed:
            return (
                CheckoutGuardDecision(
                    status="BLOCKED",
                    reason_codes=tuple(
                        f"CHECKOUT_DIRTY_UNATTRIBUTED:{path}" for path in unattributed
                    ),
                    intent=intent,
                    intent_path=intent_path,
                    lease_id=None,
                    lease_state=None,
                ),
                None,
            )
        if operation_class is CheckoutOperationClass.READ_ONLY_AUDIT:
            return (
                CheckoutGuardDecision(
                    status="PASS",
                    reason_codes=("CHECKOUT_READ_ONLY_AUDIT",),
                    intent=intent,
                    intent_path=intent_path,
                    lease_id=None,
                    lease_state=None,
                ),
                None,
            )
        task, readiness = self._lease_task(intent)
        try:
            acquisition = self.store.acquire(
                task=task,
                readiness=readiness,
                lane_id=_lane_id(operation_class),
                actor=actor,
                current_base_commit=checked_base,
                now=instant,
            )
        except ParallelControlError as exc:
            if exc.code == "LEASE_ARBITER_BUSY":
                return (
                    CheckoutGuardDecision(
                        status="BLOCKED",
                        reason_codes=(f"CHECKOUT_LEASE_ARBITER_BUSY:{exc.message}",),
                        intent=intent,
                        intent_path=intent_path,
                        lease_id=None,
                        lease_state=None,
                    ),
                    None,
                )
            raise CheckoutGuardError(exc.code, exc.message) from exc
        decision = CheckoutGuardDecision(
            status="PASS" if acquisition.status == "ACTIVE" else "BLOCKED",
            reason_codes=acquisition.reason_codes,
            intent=intent,
            intent_path=intent_path,
            lease_id=acquisition.lease.lease_id,
            lease_state=acquisition.lease.state,
        )
        if decision.status != "PASS":
            return decision, None
        after_acquire_dirty = collect_checkout_dirty_paths(
            self.project_root,
            exclusions=status_exclusions,
        )
        after_unattributed = _unattributed_dirty_paths(
            after_acquire_dirty,
            operation_class=operation_class,
            declared_paths=(*checked_owned, *checked_shared),
        )
        if after_unattributed:
            self.store.release(
                acquisition.lease.lease_id,
                actor=actor,
                now=instant,
                evidence_refs=(intent_path.as_posix(),),
                reason_codes=("CHECKOUT_POST_ACQUIRE_DIRTY_BLOCKED",),
            )
            return (
                replace(
                    decision,
                    status="BLOCKED",
                    reason_codes=tuple(
                        f"CHECKOUT_DIRTY_UNATTRIBUTED:{path}" for path in after_unattributed
                    ),
                    lease_state="RELEASED",
                ),
                None,
            )
        return decision, CheckoutLeaseHandle(guard=self, decision=decision)

    def _lease_task(
        self,
        intent: CheckoutOperationIntent,
    ) -> tuple[TaskControlRecord, ReadinessDecision]:
        gate_contract = ContractClaim(
            contract_id=f"checkout-gate:{intent.workspace_identity.workspace_id}",
            version=self.policy.version,
            access=self.policy.gate_access(intent.operation_class),
        )
        lane_role = (
            LaneRole.COORDINATOR
            if intent.operation_class is CheckoutOperationClass.SHARED_MUTATION
            else LaneRole.DOMAIN
        )
        manifest = ChangeManifest(
            change_id=f"checkout:{intent.intent_id}",
            task_id=self.policy.authority_task_id,
            lane_role=lane_role,
            base_commit=intent.base_commit,
            owner=f"{intent.task_id}:{intent.thread_id}",
            production_effect="none",
            owned_paths=intent.owned_paths,
            shared_paths=intent.shared_paths,
            module_ids=(),
            contract_claims=(gate_contract,),
            required_validation_tiers=("focused",),
        )
        task = TaskControlRecord(
            task_id=self.policy.authority_task_id,
            title="ARCH-005S4D checkout operation",
            governance_status="IN_PROGRESS",
            priority="P0",
            requirement_refs=(
                "docs/requirements/ARCH-005S4D_Shared_Checkout_Write_Lease_Guard.md",
            ),
            acceptance_criteria=("checkout guard preflight passes",),
            manifest=manifest,
        )
        readiness = ReadinessDecision(
            task_id=task.task_id,
            change_id=manifest.change_id,
            status="READY",
            reason_codes=("OWNER_APPROVED_S0_S1",),
            dependency_checks=(),
            manifest_sha256=manifest.sha256,
            policy_version=self.lease_policy.policy_version,
        )
        return task, readiness


@contextmanager
def hold_daily_checkout_guard(
    *,
    project_root: Path,
    task_id: str,
    thread_id: str,
    actor: str = "operations-automation",
    runtime_root: Path | None = None,
    policy_path: Path = DEFAULT_CHECKOUT_GUARD_POLICY_PATH,
    parallel_policy_path: Path = DEFAULT_PARALLEL_CONTROL_POLICY_PATH,
    now: datetime | None = None,
) -> Iterator[CheckoutGuardDecision]:
    guard = CheckoutLeaseGuard(
        project_root=project_root,
        runtime_root=runtime_root,
        policy_path=policy_path,
        parallel_policy_path=parallel_policy_path,
    )
    instant = _aware_utc(now or datetime.now(tz=UTC))
    decision, handle = guard.acquire(
        intent_id=f"daily-{instant.strftime('%Y%m%dT%H%M%S%fZ')}-{uuid.uuid4().hex[:12]}",
        task_id=task_id,
        thread_id=thread_id,
        actor=actor,
        operation_class=CheckoutOperationClass.DAILY_OPERATION,
        now=instant,
    )
    if decision.status != "PASS" or handle is None:
        raise CheckoutGuardError(
            "CHECKOUT_DAILY_PREFLIGHT_BLOCKED",
            ",".join(decision.reason_codes),
        )
    stop_heartbeat = Event()
    heartbeat_errors: list[BaseException] = []

    def heartbeat_until_stopped() -> None:
        interval = guard.policy.heartbeat_interval_seconds
        while not stop_heartbeat.wait(interval):
            try:
                handle.heartbeat()
            except BaseException as exc:
                heartbeat_errors.append(exc)
                return

    heartbeat_thread = Thread(
        target=heartbeat_until_stopped,
        name=f"checkout-lease-heartbeat-{handle.lease_id}",
        daemon=True,
    )
    heartbeat_thread.start()
    try:
        yield decision
    except BaseException:
        stop_heartbeat.set()
        heartbeat_thread.join()
        handle.release(outcome="failed")
        raise
    else:
        stop_heartbeat.set()
        heartbeat_thread.join()
        if heartbeat_errors:
            handle.release(outcome="failed")
            raise CheckoutGuardError(
                "CHECKOUT_DAILY_HEARTBEAT_FAILED",
                str(heartbeat_errors[0]),
            )
        handle.release(outcome="completed")


def load_checkout_guard_policy(path: Path) -> CheckoutGuardPolicy:
    payload = _mapping(safe_load_yaml_path(path), "policy")
    if payload.get("schema_version") != CHECKOUT_GUARD_POLICY_SCHEMA_VERSION:
        raise CheckoutGuardError(
            "CHECKOUT_POLICY_SCHEMA",
            str(payload.get("schema_version")),
        )
    if payload.get("status") != "OWNER_APPROVED_S0_S1_S2_READ_ONLY":
        raise CheckoutGuardError("CHECKOUT_POLICY_STATUS", str(payload.get("status")))
    authority = _mapping(payload.get("authority"), "authority")
    workspace = _mapping(payload.get("workspace"), "workspace")
    protected = _mapping(
        payload.get("protected_branch_mutation"),
        "protected_branch_mutation",
    )
    operations = _mapping(payload.get("operation_classes"), "operation_classes")
    lease = _mapping(payload.get("lease"), "lease")
    safety = _mapping(payload.get("safety"), "safety")
    if authority.get("lease_schema") != "execution_lease.v1":
        raise CheckoutGuardError(
            "CHECKOUT_POLICY_LEASE_AUTHORITY",
            str(authority.get("lease_schema")),
        )
    if authority.get("implementation") != "arch_005_file_execution_lease_store":
        raise CheckoutGuardError(
            "CHECKOUT_POLICY_LEASE_IMPLEMENTATION",
            str(authority.get("implementation")),
        )
    operation_gate_access: list[tuple[CheckoutOperationClass, ContractAccess]] = []
    for operation_class in CheckoutOperationClass:
        row = _mapping(operations.get(operation_class.value), operation_class.value)
        try:
            access = ContractAccess(str(row.get("workspace_gate_access")))
        except ValueError as exc:
            raise CheckoutGuardError(
                "CHECKOUT_POLICY_GATE_ACCESS",
                operation_class.value,
            ) from exc
        operation_gate_access.append((operation_class, access))
    if dict(operation_gate_access) != {
        CheckoutOperationClass.DOMAIN_MUTATION: ContractAccess.READ,
        CheckoutOperationClass.SHARED_MUTATION: ContractAccess.READ,
        CheckoutOperationClass.DAILY_OPERATION: ContractAccess.WRITE,
        CheckoutOperationClass.READ_ONLY_AUDIT: ContractAccess.READ,
    }:
        raise CheckoutGuardError(
            "CHECKOUT_POLICY_CONFLICT_MATRIX",
            "operation workspace gate access does not match reviewed S0 matrix",
        )
    if any(
        safety.get(field) is not False
        for field in (
            "task_source_cutover",
            "automatic_task_mutation",
            "wave15_assignment",
            "strategy_logic_change",
            "strategy_threshold_change",
            "s5_cutover_authorized",
        )
    ):
        raise CheckoutGuardError(
            "CHECKOUT_POLICY_UNSAFE_PERMISSION",
            "S0/S1 safety flags must remain false",
        )
    if safety.get("production_effect") != "none" or safety.get("broker_action") != "none":
        raise CheckoutGuardError(
            "CHECKOUT_POLICY_UNSAFE_EFFECT",
            f"{safety.get('production_effect')}/{safety.get('broker_action')}",
        )
    exclusions: list[KnownUnrelatedExclusion] = []
    raw_exclusions = payload.get("known_unrelated_exclusions")
    if not isinstance(raw_exclusions, list):
        raise CheckoutGuardError(
            "CHECKOUT_POLICY_EXCLUSIONS",
            "known_unrelated_exclusions must be a list",
        )
    for raw in raw_exclusions:
        row = _mapping(raw, "known_unrelated_exclusion")
        exclusions.append(
            KnownUnrelatedExclusion(
                path=_portable_path(row.get("path"), "exclusion.path"),
                rationale=_required_text(row.get("rationale"), "exclusion.rationale"),
                owner_ref=_required_text(row.get("owner_ref"), "exclusion.owner_ref"),
            )
        )
    paths = [row.path.casefold() for row in exclusions]
    if len(paths) != len(set(paths)):
        raise CheckoutGuardError(
            "CHECKOUT_POLICY_EXCLUSION_DUPLICATE",
            ",".join(paths),
        )
    policy = CheckoutGuardPolicy(
        policy_id=_identifier(payload.get("policy_id"), "policy_id"),
        version=_identifier(payload.get("version"), "version"),
        status=str(payload.get("status")),
        owner=_required_text(payload.get("owner"), "owner"),
        approval_ref=_required_text(payload.get("approval_ref"), "approval_ref"),
        runtime_root=_portable_path(authority.get("runtime_root"), "runtime_root"),
        identity_method=_required_text(workspace.get("identity_method"), "identity_method"),
        require_exact_head=_boolean(workspace.get("require_exact_head"), "require_exact_head"),
        require_git_checkout=_boolean(
            workspace.get("require_git_checkout"),
            "require_git_checkout",
        ),
        protected_branches=_strings(
            protected.get("branches"),
            "protected_branches",
        ),
        protected_branch_domain_mutation_allowed=_boolean(
            protected.get("domain_mutation_allowed"),
            "domain_mutation_allowed",
        ),
        protected_branch_shared_mutation_actors=_strings(
            protected.get("shared_mutation_actors"),
            "shared_mutation_actors",
        ),
        operation_gate_access=tuple(operation_gate_access),
        lease_ttl_seconds=_positive_int(lease.get("ttl_seconds"), "ttl_seconds"),
        heartbeat_interval_seconds=_positive_int(
            lease.get("heartbeat_interval_seconds"),
            "heartbeat_interval_seconds",
        ),
        max_reassignments=_non_negative_int(
            lease.get("max_reassignments"),
            "max_reassignments",
        ),
        arbiter_ttl_seconds=_positive_int(
            lease.get("arbiter_ttl_seconds"),
            "arbiter_ttl_seconds",
        ),
        max_total_active_leases=_positive_int(
            lease.get("max_total_active_leases"),
            "max_total_active_leases",
        ),
        authority_task_id=_identifier(
            lease.get("authority_task_id"),
            "authority_task_id",
        ),
        allowlisted_actors=_strings(
            lease.get("allowlisted_actors"),
            "allowlisted_actors",
        ),
        known_unrelated_exclusions=tuple(exclusions),
    )
    if policy.heartbeat_interval_seconds >= policy.lease_ttl_seconds:
        raise CheckoutGuardError(
            "CHECKOUT_POLICY_HEARTBEAT_INTERVAL",
            "heartbeat interval must be shorter than lease TTL",
        )
    if policy.protected_branch_domain_mutation_allowed:
        raise CheckoutGuardError(
            "CHECKOUT_POLICY_PROTECTED_BRANCH_DOMAIN_MUTATION",
            "protected branch domain mutation must remain false",
        )
    if any(
        actor not in policy.allowlisted_actors
        for actor in policy.protected_branch_shared_mutation_actors
    ):
        raise CheckoutGuardError(
            "CHECKOUT_POLICY_PROTECTED_BRANCH_ACTOR",
            "shared mutation actor must be allowlisted",
        )
    return policy


def resolve_checkout_identity(project_root: Path) -> CheckoutIdentity:
    root = project_root.resolve()
    checkout_text = _git_output(root, ("rev-parse", "--show-toplevel"), required=True)
    if checkout_text is None:
        raise CheckoutGuardError("CHECKOUT_GIT_EMPTY", "--show-toplevel")
    checkout_root = Path(checkout_text).resolve()
    if checkout_root != root:
        raise CheckoutGuardError(
            "CHECKOUT_ROOT_MISMATCH",
            f"declared={root};git={checkout_root}",
        )
    common_text = _git_output(
        root,
        ("rev-parse", "--path-format=absolute", "--git-common-dir"),
        required=True,
    )
    if common_text is None:
        raise CheckoutGuardError("CHECKOUT_GIT_EMPTY", "--git-common-dir")
    common_dir = Path(common_text).resolve()
    head = _git_output(root, ("rev-parse", "--verify", "HEAD"), required=True)
    if head is None:
        raise CheckoutGuardError("CHECKOUT_GIT_EMPTY", "HEAD")
    branch_name = _git_output(
        root,
        ("symbolic-ref", "--quiet", "--short", "HEAD"),
        required=False,
    )
    upstream_ref = _git_output(
        root,
        ("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"),
        required=False,
    )
    upstream_commit = (
        None
        if upstream_ref is None
        else _git_output(root, ("rev-parse", "--verify", upstream_ref), required=True)
    )
    identity_payload = {
        "checkout_root": os.path.normcase(str(checkout_root)),
        "git_common_dir": os.path.normcase(str(common_dir)),
    }
    workspace_id = hashlib.sha256(
        json.dumps(identity_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:24]
    return CheckoutIdentity(
        workspace_id=f"checkout-{workspace_id}",
        checkout_root=str(checkout_root),
        git_common_dir=str(common_dir),
        head_commit=head,
        branch_name=branch_name,
        upstream_ref=upstream_ref,
        upstream_commit=upstream_commit,
    )


def resolve_worktree_audit_binding(
    *,
    policy_project_root: Path,
    audited_project_root: Path,
) -> WorktreeAuditBinding:
    policy_root = _checked_audit_root(policy_project_root, role="POLICY")
    audited_root = _checked_audit_root(audited_project_root, role="TARGET")
    try:
        policy_identity = resolve_checkout_identity(policy_root)
    except CheckoutGuardError as exc:
        raise CheckoutGuardError(
            f"CHECKOUT_AUDIT_POLICY_{exc.code.removeprefix('CHECKOUT_')}",
            exc.message,
        ) from exc
    try:
        audited_identity = resolve_checkout_identity(audited_root)
    except CheckoutGuardError as exc:
        raise CheckoutGuardError(
            f"CHECKOUT_AUDIT_TARGET_{exc.code.removeprefix('CHECKOUT_')}",
            exc.message,
        ) from exc
    if _path_identity_key(Path(policy_identity.git_common_dir)) != _path_identity_key(
        Path(audited_identity.git_common_dir)
    ):
        raise CheckoutGuardError(
            "CHECKOUT_AUDIT_GIT_COMMON_DIR_MISMATCH",
            (
                f"policy={policy_identity.git_common_dir};"
                f"target={audited_identity.git_common_dir}"
            ),
        )
    registrations = _registered_worktrees(policy_root)
    if (
        _matching_worktree_registration(
            registrations,
            Path(policy_identity.checkout_root),
        )
        is None
    ):
        raise CheckoutGuardError(
            "CHECKOUT_AUDIT_POLICY_UNREGISTERED",
            policy_identity.checkout_root,
        )
    registration = _matching_worktree_registration(
        registrations,
        Path(audited_identity.checkout_root),
    )
    if registration is None:
        raise CheckoutGuardError(
            "CHECKOUT_AUDIT_TARGET_UNREGISTERED",
            audited_identity.checkout_root,
        )
    _assert_registration_matches_identity(registration, audited_identity)
    return WorktreeAuditBinding(
        policy_identity=policy_identity,
        audited_identity=audited_identity,
        registration=registration,
    )


def _checked_audit_root(path: Path, *, role: str) -> Path:
    candidate = path.expanduser()
    try:
        resolved = candidate.resolve(strict=True)
    except FileNotFoundError as exc:
        raise CheckoutGuardError(
            f"CHECKOUT_AUDIT_{role}_NOT_FOUND",
            str(candidate),
        ) from exc
    except OSError as exc:
        raise CheckoutGuardError(
            f"CHECKOUT_AUDIT_{role}_RESOLUTION_FAILED",
            str(exc),
        ) from exc
    if not resolved.is_dir():
        raise CheckoutGuardError(
            f"CHECKOUT_AUDIT_{role}_NOT_DIRECTORY",
            str(resolved),
        )
    return resolved


def _registered_worktrees(policy_root: Path) -> tuple[RegisteredWorktree, ...]:
    try:
        result = subprocess.run(
            [
                "git",
                "-c",
                "core.quotepath=false",
                "worktree",
                "list",
                "--porcelain",
                "-z",
            ],
            cwd=policy_root,
            check=False,
            capture_output=True,
        )
    except OSError as exc:
        raise CheckoutGuardError(
            "CHECKOUT_AUDIT_WORKTREE_LIST_EXECUTION",
            str(exc),
        ) from exc
    if result.returncode != 0:
        message = result.stderr.decode("utf-8", errors="replace").strip()
        raise CheckoutGuardError(
            "CHECKOUT_AUDIT_WORKTREE_LIST_FAILED",
            message,
        )
    records: list[RegisteredWorktree] = []
    fields: dict[str, str] = {}
    flags: set[str] = set()
    for raw_token in (*result.stdout.split(b"\0"), b""):
        if not raw_token:
            if fields or flags:
                records.append(_registered_worktree_from_fields(fields, flags))
                fields = {}
                flags = set()
            continue
        try:
            token = raw_token.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise CheckoutGuardError(
                "CHECKOUT_AUDIT_WORKTREE_LIST_ENCODING",
                str(exc),
            ) from exc
        key, separator, value = token.partition(" ")
        if separator:
            if key in fields:
                raise CheckoutGuardError(
                    "CHECKOUT_AUDIT_WORKTREE_LIST_DUPLICATE_FIELD",
                    key,
                )
            fields[key] = value
        else:
            flags.add(key)
    if not records:
        raise CheckoutGuardError(
            "CHECKOUT_AUDIT_WORKTREE_LIST_EMPTY",
            str(policy_root),
        )
    return tuple(records)


def _registered_worktree_from_fields(
    fields: Mapping[str, str],
    flags: set[str],
) -> RegisteredWorktree:
    toplevel = fields.get("worktree")
    head = fields.get("HEAD")
    if not toplevel or not head:
        raise CheckoutGuardError(
            "CHECKOUT_AUDIT_WORKTREE_LIST_INVALID",
            json.dumps(dict(fields), ensure_ascii=False, sort_keys=True),
        )
    if "branch" in fields and "detached" in flags:
        raise CheckoutGuardError(
            "CHECKOUT_AUDIT_WORKTREE_LIST_INVALID",
            f"branch_and_detached:{toplevel}",
        )
    return RegisteredWorktree(
        toplevel=str(Path(toplevel).resolve(strict=False)),
        head_commit=head,
        branch_ref=fields.get("branch"),
        detached="detached" in flags,
        locked_reason=(
            fields.get("locked") if "locked" in fields else ("" if "locked" in flags else None)
        ),
        prunable_reason=(
            fields.get("prunable")
            if "prunable" in fields
            else ("" if "prunable" in flags else None)
        ),
    )


def _matching_worktree_registration(
    registrations: Sequence[RegisteredWorktree],
    target: Path,
) -> RegisteredWorktree | None:
    target_key = _path_identity_key(target)
    matches = [row for row in registrations if _path_identity_key(Path(row.toplevel)) == target_key]
    if len(matches) > 1:
        raise CheckoutGuardError(
            "CHECKOUT_AUDIT_WORKTREE_REGISTRATION_DUPLICATE",
            str(target),
        )
    return matches[0] if matches else None


def _assert_registration_matches_identity(
    registration: RegisteredWorktree,
    identity: CheckoutIdentity,
) -> None:
    expected_branch_ref = (
        None if identity.branch_name is None else f"refs/heads/{identity.branch_name}"
    )
    if (
        registration.head_commit != identity.head_commit
        or registration.branch_ref != expected_branch_ref
        or registration.detached != (identity.branch_name is None)
    ):
        raise CheckoutGuardError(
            "CHECKOUT_AUDIT_REGISTRATION_IDENTITY_MISMATCH",
            (
                f"registration={json.dumps(registration.to_dict(), sort_keys=True)};"
                f"identity={json.dumps(identity.to_dict(), sort_keys=True)}"
            ),
        )


def _path_identity_key(path: Path) -> str:
    return os.path.normcase(str(path.resolve(strict=False)))


def _worktree_binding_summary(binding: WorktreeAuditBinding) -> str:
    return json.dumps(
        {
            "policy": binding.policy_identity.to_dict(),
            "audited": binding.audited_identity.to_dict(),
            "registration": binding.registration.to_dict(),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def collect_checkout_dirty_paths(
    project_root: Path,
    *,
    exclusions: Sequence[str],
) -> tuple[str, ...]:
    root = project_root.resolve()
    git_environment = os.environ.copy()
    # A dirty-path audit is read-only. Prevent `git status` from refreshing the
    # index, which can transiently deny concurrent readers on Windows.
    git_environment["GIT_OPTIONAL_LOCKS"] = "0"
    args = [
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
        "--ignore-submodules=none",
        "--",
        ".",
    ]
    args.extend(f":(exclude,literal){path}" for path in exclusions)
    try:
        result = subprocess.run(
            ["git", "-c", "core.quotepath=false", *args],
            cwd=root,
            check=False,
            capture_output=True,
            env=git_environment,
        )
    except OSError as exc:
        raise CheckoutGuardError("CHECKOUT_GIT_STATUS_EXECUTION", str(exc)) from exc
    if result.returncode != 0:
        message = result.stderr.decode("utf-8", errors="replace").strip()
        raise CheckoutGuardError("CHECKOUT_GIT_STATUS_FAILED", message)
    tokens = result.stdout.split(b"\0")
    paths: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        index += 1
        if not token:
            continue
        text = token.decode("utf-8", errors="strict")
        if len(text) < 4:
            raise CheckoutGuardError("CHECKOUT_GIT_STATUS_INVALID", text)
        status_code = text[:2]
        paths.append(_portable_path(text[3:], "git_status.path"))
        if "R" in status_code or "C" in status_code:
            if index >= len(tokens) or not tokens[index]:
                raise CheckoutGuardError("CHECKOUT_GIT_STATUS_RENAME_INVALID", text)
            paths.append(
                _portable_path(
                    tokens[index].decode("utf-8", errors="strict"),
                    "git_status.original_path",
                )
            )
            index += 1
    return tuple(sorted(set(paths), key=lambda value: value.casefold()))


def _run_git_diff_check(
    project_root: Path,
    *,
    exclusions: Sequence[str],
    cached: bool,
) -> None:
    root = project_root.resolve()
    args = ["diff"]
    if cached:
        args.append("--cached")
    args.extend(("--check", "--", "."))
    args.extend(f":(exclude,literal){path}" for path in exclusions)
    try:
        result = subprocess.run(
            ["git", "-c", "core.quotepath=false", *args],
            cwd=root,
            check=False,
            capture_output=True,
        )
    except OSError as exc:
        raise CheckoutGuardError(
            "CHECKOUT_GIT_DIFF_CHECK_EXECUTION",
            str(exc),
        ) from exc
    if result.returncode != 0:
        output = (
            (result.stdout + result.stderr)
            .decode(
                "utf-8",
                errors="replace",
            )
            .strip()
        )
        scope = "staged" if cached else "unstaged"
        raise CheckoutGuardError(
            "CHECKOUT_GIT_DIFF_CHECK_FAILED",
            f"{scope}:{output}",
        )


def _checkout_lease_policy(
    base: ParallelControlPolicy,
    guard: CheckoutGuardPolicy,
) -> ParallelControlPolicy:
    return replace(
        base,
        policy_id=guard.policy_id,
        version=guard.version,
        lease_ttl_seconds=guard.lease_ttl_seconds,
        max_reassignments=guard.max_reassignments,
        arbiter_ttl_seconds=guard.arbiter_ttl_seconds,
        max_total_active_leases=guard.max_total_active_leases,
        allowlisted_task_ids=(guard.authority_task_id,),
        allowlisted_actors=guard.allowlisted_actors,
    )


def _checked_paths(
    project_root: Path,
    values: Sequence[str],
    field: str,
) -> tuple[str, ...]:
    checked = tuple(sorted({_portable_path(value, field) for value in values}))
    if len(checked) != len(values):
        raise CheckoutGuardError("CHECKOUT_PATH_DUPLICATE", field)
    for value in checked:
        _assert_no_reparse_components(project_root, value)
    return checked


def _assert_no_reparse_components(project_root: Path, relative_path: str) -> None:
    root = project_root.resolve()
    current = root
    for part in PurePosixPath(relative_path).parts:
        current = current / part
        try:
            metadata = os.lstat(current)
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(metadata.st_mode) or _is_reparse_point(metadata):
            raise CheckoutGuardError(
                "CHECKOUT_PATH_REPARSE_POINT",
                relative_path,
            )
    resolved = (root / Path(*PurePosixPath(relative_path).parts)).resolve(strict=False)
    if not resolved.is_relative_to(root):
        raise CheckoutGuardError("CHECKOUT_PATH_ROOT_ESCAPE", relative_path)


def _is_reparse_point(metadata: os.stat_result) -> bool:
    attributes = getattr(metadata, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(reparse_flag and attributes & reparse_flag)


def _unattributed_dirty_paths(
    dirty_paths: Sequence[str],
    *,
    operation_class: CheckoutOperationClass,
    declared_paths: Sequence[str],
) -> tuple[str, ...]:
    if operation_class not in {
        CheckoutOperationClass.DOMAIN_MUTATION,
        CheckoutOperationClass.SHARED_MUTATION,
    }:
        return tuple(dirty_paths)
    return tuple(
        path
        for path in dirty_paths
        if not any(_paths_overlap(path, declared) for declared in declared_paths)
    )


def _paths_overlap(first: str, second: str) -> bool:
    left = tuple(part.casefold() for part in PurePosixPath(first).parts)
    right = tuple(part.casefold() for part in PurePosixPath(second).parts)
    common = min(len(left), len(right))
    return left[:common] == right[:common]


def _lane_id(operation_class: CheckoutOperationClass) -> str:
    if operation_class is CheckoutOperationClass.SHARED_MUTATION:
        return "checkout-shared-coordinator"
    if operation_class is CheckoutOperationClass.DAILY_OPERATION:
        return "checkout-daily-operation"
    return "checkout-domain-mutation"


def _persist_or_replay_intent(
    path: Path,
    intent: CheckoutOperationIntent,
) -> CheckoutOperationIntent:
    payload = intent.to_dict()
    if path.exists():
        try:
            current = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", str(path)) from exc
        existing_created_at = current.get("created_at")
        if not isinstance(existing_created_at, str):
            raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", str(path))
        comparable = {**payload, "created_at": existing_created_at}
        if current != comparable:
            raise CheckoutGuardError("CHECKOUT_INTENT_IMMUTABILITY", str(path))
        try:
            created_at = _aware_utc(datetime.fromisoformat(existing_created_at))
        except ValueError as exc:
            raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", str(path)) from exc
        return replace(intent, created_at=created_at)
    write_json_atomic(path, payload)
    return intent


def _load_release_scope(
    path: Path,
    *,
    expected_intent_id: str,
) -> tuple[tuple[str, ...], CheckoutOperationClass]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", str(path)) from exc
    if not isinstance(payload, Mapping):
        raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", str(path))
    if payload.get("schema_version") != CHECKOUT_INTENT_SCHEMA_VERSION:
        raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", str(path))
    if payload.get("intent_id") != expected_intent_id:
        raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", str(path))
    try:
        operation_class = CheckoutOperationClass(str(payload.get("operation_class")))
    except ValueError as exc:
        raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", str(path)) from exc
    owned_paths = _release_paths(payload.get("owned_paths"), "owned_paths")
    shared_paths = _release_paths(payload.get("shared_paths"), "shared_paths")
    return (*owned_paths, *shared_paths), operation_class


def _release_paths(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", field)
    return tuple(_portable_path(item, field) for item in value)


def _git_output(
    root: Path,
    args: Sequence[str],
    *,
    required: bool,
) -> str | None:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except OSError as exc:
        raise CheckoutGuardError("CHECKOUT_GIT_EXECUTION", str(exc)) from exc
    if result.returncode != 0:
        if not required:
            return None
        raise CheckoutGuardError(
            "CHECKOUT_GIT_FAILED",
            result.stderr.strip() or " ".join(args),
        )
    value = result.stdout.strip()
    if not value and required:
        raise CheckoutGuardError("CHECKOUT_GIT_EMPTY", " ".join(args))
    return value or None


def _portable_path(value: object, field: str) -> str:
    text = _required_text(value, field)
    if "\\" in text or text.endswith("/"):
        raise CheckoutGuardError(
            "CHECKOUT_PATH_NON_CANONICAL",
            f"{field} must use canonical POSIX syntax",
        )
    path = PurePosixPath(text)
    if (
        path.is_absolute()
        or ":" in path.parts[0]
        or any(part in {"", ".", ".."} for part in path.parts)
        or str(path) != text
    ):
        raise CheckoutGuardError(
            "CHECKOUT_PATH_UNSAFE",
            f"{field} must be repository relative",
        )
    return text


def _identifier(value: object, field: str) -> str:
    text = _required_text(value, field)
    if len(text) > 96 or not all(character.isalnum() or character in "._:-" for character in text):
        raise CheckoutGuardError("CHECKOUT_IDENTIFIER_INVALID", f"{field}={text}")
    return text


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise CheckoutGuardError("CHECKOUT_TEXT_REQUIRED", field)
    return value


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CheckoutGuardError("CHECKOUT_MAPPING_REQUIRED", field)
    return value


def _strings(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise CheckoutGuardError("CHECKOUT_LIST_REQUIRED", field)
    rows = tuple(_identifier(item, field) for item in value)
    if len(rows) != len(set(rows)):
        raise CheckoutGuardError("CHECKOUT_LIST_DUPLICATE", field)
    return rows


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise CheckoutGuardError("CHECKOUT_POSITIVE_INT_REQUIRED", field)
    return value


def _non_negative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CheckoutGuardError("CHECKOUT_NON_NEGATIVE_INT_REQUIRED", field)
    return value


def _boolean(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise CheckoutGuardError("CHECKOUT_BOOL_REQUIRED", field)
    return value


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise CheckoutGuardError("CHECKOUT_TIMEZONE_REQUIRED", value.isoformat())
    return value.astimezone(UTC)
