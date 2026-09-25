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
    LeaseReplay,
    ParallelControlPolicy,
    ReadinessDecision,
    TaskControlRecord,
    load_parallel_control_policy,
    manifest_resource_claims,
)
from ai_trading_system.platform.artifacts import write_json_atomic
from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text
from ai_trading_system.yaml_loader import safe_load_yaml_path

CHECKOUT_GUARD_POLICY_SCHEMA_VERSION = "arch_005_s4d_checkout_guard_policy.v2"
CHECKOUT_INTENT_SCHEMA_VERSION = "checkout_operation_intent.v1"
CHECKOUT_SOURCE_ONLY_INTENT_SCHEMA_VERSION = "checkout_operation_intent.v2"
CHECKOUT_FULL_WORKTREE_PROFILE = "FULL_WORKTREE"
CHECKOUT_SOURCE_ONLY_PROFILE = "SOURCE_ONLY_EXPLICIT_PATHS"
CHECKOUT_SOURCE_ONLY_CAPABILITY_PREFIX = "checkout-source-only-capability:"
CHECKOUT_SOURCE_ONLY_CAPABILITY_VERSION = "SOURCE_ONLY_EXPLICIT_PATHS@2"
CHECKOUT_SOURCE_ONLY_RUNTIME = "outputs/architecture/arch_005_task_checkpoints"
# DEVX-015 reviewed engineering ceilings. A checkpoint policy may be stricter;
# these hard ceilings never authorize publication or a source-file mutation.
_CHECKPOINT_MAX_FILES = 2048
_CHECKPOINT_MAX_TOTAL_BYTES = 64 * 1024 * 1024
_INTENT_MAX_BYTES = 16 * 1024 * 1024
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
    inspection_profile: str = CHECKOUT_FULL_WORKTREE_PROFILE

    def to_dict(self) -> dict[str, object]:
        if self.inspection_profile not in {
            CHECKOUT_FULL_WORKTREE_PROFILE,
            CHECKOUT_SOURCE_ONLY_PROFILE,
        }:
            raise CheckoutGuardError("CHECKOUT_INSPECTION_PROFILE_INVALID", self.inspection_profile)
        result: dict[str, object] = {
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
        if self.inspection_profile == CHECKOUT_SOURCE_ONLY_PROFILE:
            result.update(
                schema_version=CHECKOUT_SOURCE_ONLY_INTENT_SCHEMA_VERSION,
                inspection_profile=CHECKOUT_SOURCE_ONLY_PROFILE,
                source_mutation_allowed=False,
                unscoped_worktree_status="NOT_INSPECTED",
                clean_integration_status="NOT_EVALUATED",
            )
        return result


def parse_checkout_operation_intent(payload: Mapping[str, object]) -> CheckoutOperationIntent:
    """Parse exact v1/v2 authority without upgrading a historical capability."""
    try:
        schema = payload.get("schema_version")
        if schema == CHECKOUT_INTENT_SCHEMA_VERSION:
            profile = CHECKOUT_FULL_WORKTREE_PROFILE
        elif schema == CHECKOUT_SOURCE_ONLY_INTENT_SCHEMA_VERSION:
            profile = CHECKOUT_SOURCE_ONLY_PROFILE
        else:
            raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", "unsupported intent schema")
        raw_identity = _mapping(payload.get("workspace_identity"), "workspace_identity")
        identity = CheckoutIdentity(**raw_identity)
        for name in ("workspace_id", "checkout_root", "git_common_dir", "head_commit"):
            _required_text(getattr(identity, name), name)
        for name in ("branch_name", "upstream_ref", "upstream_commit"):
            if getattr(identity, name) is not None:
                _required_text(getattr(identity, name), name)
        raw_exclusions = payload.get("known_unrelated_exclusions")
        if not isinstance(raw_exclusions, list):
            raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", "known_unrelated_exclusions")
        exclusions = []
        for raw in raw_exclusions:
            item = _mapping(raw, "known_unrelated_exclusion")
            exclusions.append(
                KnownUnrelatedExclusion(
                    path=_portable_path(item.get("path"), "excluded path"),
                    rationale=_required_text(item.get("rationale"), "rationale"),
                    owner_ref=_required_text(item.get("owner_ref"), "owner_ref"),
                )
            )
        intent = CheckoutOperationIntent(
            intent_id=_identifier(payload.get("intent_id"), "intent_id"),
            task_id=_required_text(payload.get("task_id"), "task_id"),
            thread_id=_required_text(payload.get("thread_id"), "thread_id"),
            actor=_required_text(payload.get("actor"), "actor"),
            operation_class=CheckoutOperationClass(
                _required_text(payload.get("operation_class"), "operation_class")
            ),
            base_commit=_required_text(payload.get("base_commit"), "base_commit"),
            owned_paths=_release_paths(payload.get("owned_paths"), "owned_paths"),
            shared_paths=_release_paths(payload.get("shared_paths"), "shared_paths"),
            workspace_identity=identity,
            observed_dirty_paths=_release_paths(
                payload.get("observed_dirty_paths"), "observed_dirty_paths"
            ),
            known_unrelated_exclusions=tuple(exclusions),
            created_at=datetime.fromisoformat(
                _required_text(payload.get("created_at"), "created_at")
            ),
            inspection_profile=profile,
        )
        _aware_utc(intent.created_at)
        if (
            intent.to_dict() != dict(payload)
            or type(payload.get("task_source_cutover")) is not bool
            or profile == CHECKOUT_SOURCE_ONLY_PROFILE
            and type(payload.get("source_mutation_allowed")) is not bool
        ):
            raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", "exact intent schema required")
        if profile == CHECKOUT_SOURCE_ONLY_PROFILE:
            _source_only_scope(intent.operation_class, intent.owned_paths, intent.shared_paths)
            if intent.observed_dirty_paths:
                raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", "unscoped status not inspected")
        return intent
    except (TypeError, ValueError, KeyError, AttributeError) as exc:
        raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", type(exc).__name__) from exc


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
        from ai_trading_system.platform.architecture.workflow_coordination import (
            coordinated_lease_store,
        )

        self.store = coordinated_lease_store(
            self.project_root,
            self.runtime_root / "leases",
            policy=self.lease_policy,
            entrypoint="checkout-guard",
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

    def cancel_request(
        self, lease_id: str, *, actor: str, now: datetime | None = None,
    ) -> ExecutionLease:
        """Cancel only the exact bound, never-executed checkout request."""
        instant = _aware_utc(now or datetime.now(tz=UTC))
        with self.store.atomic(actor=actor, now=instant, operation="terminal"):
            replay = self.store.replay()
            if replay.status != "PASS":
                raise CheckoutGuardError("CHECKOUT_LEASE_REPLAY_INVALID", lease_id)
            head = next((row for row in replay.lease_heads if row.lease_id == lease_id), None)
            if head is None:
                raise CheckoutGuardError("CHECKOUT_LEASE_UNKNOWN", lease_id)
            _intent, intent_path = self._bound_lease_intent(head)
            return self.store.cancel_request(
                lease_id, actor=actor, now=instant, evidence_refs=(intent_path.as_posix(),),
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
        intent, intent_path = self._bound_lease_intent(head)
        declared_paths = (*intent.owned_paths, *intent.shared_paths)
        operation_class = intent.operation_class
        status_exclusions = (
            *(row.path for row in self.policy.known_unrelated_exclusions),
            self.runtime_root.relative_to(self.project_root).as_posix(),
        )
        if intent.inspection_profile == CHECKOUT_SOURCE_ONLY_PROFILE:
            self._inspect_source_only(operation_class, intent.owned_paths, intent.shared_paths)
            dirty_paths: tuple[str, ...] = ()
        else:
            dirty_paths = collect_checkout_dirty_paths(
                self.project_root,
                exclusions=status_exclusions,
            )
        unattributed = _unattributed_dirty_paths(
            dirty_paths,
            operation_class=operation_class,
            declared_paths=self._live_attributed_paths(
                dirty_paths, operation_class=operation_class, declared_paths=declared_paths,
                actor=actor, now=instant,
            ),
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

    def _live_attributed_paths(
        self, dirty_paths: Sequence[str], *, operation_class: CheckoutOperationClass,
        declared_paths: Sequence[str], actor: str, now: datetime,
    ) -> tuple[str, ...]:
        """Recognize current ordinary owners; never turn their claims into ours.

        Resource-conflict admission still uses only the caller's declared paths.
        Snapshot the existing authority under its arbiter, and independently bind
        each same-checkout intent before accepting its dirty-path attribution.
        """
        paths = set(declared_paths)
        if not dirty_paths or operation_class not in {
            CheckoutOperationClass.DOMAIN_MUTATION, CheckoutOperationClass.SHARED_MUTATION,
        }:
            return tuple(sorted(paths))
        with self.store.atomic(actor=actor, now=now):
            replay = self.store.replay()
            if replay.status != "PASS":
                raise CheckoutGuardError("CHECKOUT_ATTRIBUTION_REPLAY_INVALID", str(replay.issues))
            identity = resolve_checkout_identity(self.project_root)
            gate = "checkout-gate:" + identity.workspace_id
            for lease in replay.active_leases:
                if (
                    lease.task_id != self.policy.authority_task_id
                    or lease.actor not in self.policy.allowlisted_actors
                    or lease.expires_at is None
                    or _aware_utc(datetime.fromisoformat(lease.expires_at)) <= now
                    or not any(row.kind == "contract" and row.resource_id == gate
                               for row in lease.resources)
                ):
                    continue
                intent, _ = self._bound_lease_intent(lease)
                if intent.inspection_profile == CHECKOUT_FULL_WORKTREE_PROFILE and (
                    intent.operation_class in {
                        CheckoutOperationClass.DOMAIN_MUTATION,
                        CheckoutOperationClass.SHARED_MUTATION,
                    }
                ):
                    paths.update((*intent.owned_paths, *intent.shared_paths))
        return tuple(sorted(paths))

    def _bound_lease_intent(self, lease: ExecutionLease) -> tuple[CheckoutOperationIntent, Path]:
        intent_id = lease.change_id.removeprefix("checkout:")
        if f"checkout:{intent_id}" != lease.change_id:
            raise CheckoutGuardError("CHECKOUT_RELEASE_CHANGE_ID", lease.change_id)
        _identifier(intent_id, "intent_id")
        path = self.runtime_root / "intents" / f"{intent_id}.json"
        intent = _read_checkout_intent(self.project_root, path)
        task, _ = self._lease_task(intent)
        if (
            lease.state == "RELEASED"
            and self.store.coordination_binding is not None
            and lease.change_manifest_sha256 != task.manifest.sha256
        ):
            # A retired lease keeps its original unscoped path namespace. Prove
            # the exact immutable terminal origin; never infer it from a caller
            # flag or use this branch for an ACTIVE lease/new writer admission.
            from ai_trading_system.platform.architecture.workflow_coordination import (
                registered_legacy_terminal_lease,
            )

            try:
                origin = registered_legacy_terminal_lease(
                    self.project_root, self.runtime_root / "leases",
                    policy=self.lease_policy, lease_id=lease.lease_id,
                )
            except (ParallelControlError, OSError, ValueError) as exc:
                raise CheckoutGuardError("CHECKOUT_TERMINAL_ORIGIN_INVALID", str(exc)) from exc
            if origin == lease:
                task = replace(task, manifest=replace(
                    task.manifest, owned_paths=intent.owned_paths, shared_paths=intent.shared_paths,
                ))
        current = resolve_checkout_identity(self.project_root)
        if (
            intent.intent_id != intent_id
            or lease.task_id != self.policy.authority_task_id
            or lease.change_id != task.manifest.change_id
            or lease.actor != intent.actor
            or lease.base_commit != intent.base_commit
            or lease.change_manifest_sha256 != task.manifest.sha256
            or lease.lane_id != _lane_id(intent.operation_class)
            or lease.resources != manifest_resource_claims(task.manifest)
            or Path(intent.workspace_identity.checkout_root).resolve() != self.project_root
            or intent.workspace_identity.workspace_id != current.workspace_id
            or Path(intent.workspace_identity.git_common_dir).resolve()
            != Path(current.git_common_dir).resolve()
            or intent.workspace_identity.head_commit != intent.base_commit
            or intent.known_unrelated_exclusions != self.policy.known_unrelated_exclusions
        ):
            raise CheckoutGuardError("CHECKOUT_LEASE_INTENT_BINDING", lease.lease_id)
        return intent, path

    def require_mutation_lease(
        self,
        lease: ExecutionLease,
        *,
        expected_intent_path: Path | None = None,
        task_id: str | None = None,
        actor: str | None = None,
    ) -> CheckoutOperationIntent:
        """Require ordinary mutation capability, including historical release proof.

        State/expiry and a publication transaction's remaining identity are the
        consumer's responsibility; a source-only lease is never a mutation grant.
        """
        intent, path = self._bound_lease_intent(lease)
        if (
            intent.inspection_profile != CHECKOUT_FULL_WORKTREE_PROFILE
            or intent.operation_class
            not in {
                CheckoutOperationClass.DOMAIN_MUTATION,
                CheckoutOperationClass.SHARED_MUTATION,
            }
        ):
            raise CheckoutGuardError("CHECKOUT_MUTATION_CAPABILITY_REQUIRED", lease.lease_id)
        if (
            expected_intent_path is not None
            and expected_intent_path.absolute() != path.absolute()
            or task_id is not None
            and intent.task_id != task_id
            or actor is not None
            and intent.actor != actor
        ):
            raise CheckoutGuardError("CHECKOUT_LEASE_INTENT_BINDING", lease.lease_id)
        return intent

    def _inspect_source_only(
        self,
        operation_class: CheckoutOperationClass,
        owned_paths: Sequence[str],
        shared_paths: Sequence[str],
    ) -> None:
        paths = _source_only_scope(operation_class, owned_paths, shared_paths)
        # No Git status, attributes, ignore-file or source-content read here.
        # These are conservative metadata gates, not a clean-worktree claim.
        for path in paths:
            if any(
                _paths_overlap(path, row.path) for row in self.policy.known_unrelated_exclusions
            ):
                raise CheckoutGuardError("CHECKOUT_SOURCE_ONLY_SCOPE", "known unrelated path")
        total = 0
        for path in paths:
            _assert_no_reparse_components(self.project_root, path)
            try:
                metadata = os.lstat(self.project_root / path)
            except FileNotFoundError:
                continue  # The checkpoint layer proves DELETE against source HEAD.
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise CheckoutGuardError("CHECKOUT_SOURCE_ONLY_PATH", "regular single-link source")
            total += metadata.st_size
        if total > _CHECKPOINT_MAX_TOTAL_BYTES:
            raise CheckoutGuardError("CHECKOUT_SOURCE_ONLY_BUDGET", "aggregate source size")

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
        inspection_profile: str = CHECKOUT_FULL_WORKTREE_PROFILE,
    ) -> tuple[CheckoutGuardDecision, CheckoutLeaseHandle | None]:
        instant = _aware_utc(now or datetime.now(tz=UTC))
        if inspection_profile not in {CHECKOUT_FULL_WORKTREE_PROFILE, CHECKOUT_SOURCE_ONLY_PROFILE}:
            raise CheckoutGuardError("CHECKOUT_INSPECTION_PROFILE_INVALID", inspection_profile)
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
        if inspection_profile == CHECKOUT_SOURCE_ONLY_PROFILE:
            self._inspect_source_only(operation_class, checked_owned, checked_shared)
            dirty_paths: tuple[str, ...] = ()
        else:
            dirty_paths = collect_checkout_dirty_paths(
                self.project_root,
                exclusions=status_exclusions,
            )
        unattributed = _unattributed_dirty_paths(
            dirty_paths,
            operation_class=operation_class,
            declared_paths=self._live_attributed_paths(
                dirty_paths, operation_class=operation_class,
                declared_paths=(*checked_owned, *checked_shared), actor=actor, now=instant,
            ),
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
            inspection_profile=inspection_profile,
        )
        intent_path = self.runtime_root / "intents" / f"{intent.intent_id}.json"
        # Intent persistence is a writer side effect too. Migration phase/epoch
        # checks must precede it under the same short store arbiter as leases.
        try:
            with self.store.atomic(actor=actor, now=instant, operation="acquire"):
                intent = _persist_or_replay_intent(intent_path, intent)
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
        if inspection_profile == CHECKOUT_SOURCE_ONLY_PROFILE:
            try:
                self._inspect_source_only(operation_class, checked_owned, checked_shared)
            except (CheckoutGuardError, OSError):
                self.store.release(
                    acquisition.lease.lease_id,
                    actor=actor,
                    now=instant,
                    evidence_refs=(intent_path.as_posix(),),
                    reason_codes=("CHECKOUT_POST_ACQUIRE_SOURCE_ONLY_BLOCKED",),
                )
                raise
            after_acquire_dirty: tuple[str, ...] = ()
        else:
            after_acquire_dirty = collect_checkout_dirty_paths(
                self.project_root,
                exclusions=status_exclusions,
            )
        after_unattributed = _unattributed_dirty_paths(
            after_acquire_dirty,
            operation_class=operation_class,
            declared_paths=self._live_attributed_paths(
                after_acquire_dirty, operation_class=operation_class,
                declared_paths=(*checked_owned, *checked_shared), actor=actor, now=instant,
            ),
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
        contracts: tuple[ContractClaim, ...] = (gate_contract,)
        if intent.inspection_profile == CHECKOUT_SOURCE_ONLY_PROFILE:
            _source_only_scope(intent.operation_class, intent.owned_paths, intent.shared_paths)
            contracts += (
                ContractClaim(
                    contract_id=CHECKOUT_SOURCE_ONLY_CAPABILITY_PREFIX
                    + intent.workspace_identity.workspace_id,
                    version=CHECKOUT_SOURCE_ONLY_CAPABILITY_VERSION,
                    access=ContractAccess.READ,
                ),
            )
        elif intent.inspection_profile != CHECKOUT_FULL_WORKTREE_PROFILE:
            raise CheckoutGuardError(
                "CHECKOUT_INSPECTION_PROFILE_INVALID", intent.inspection_profile
            )
        lane_role = (
            LaneRole.COORDINATOR
            if intent.operation_class is CheckoutOperationClass.SHARED_MUTATION
            else LaneRole.DOMAIN
        )
        binding = self.store.coordination_binding
        owned_paths = intent.owned_paths
        shared_paths = intent.shared_paths
        if binding is not None:
            shared_scopes = {scope for path in shared_paths for scope in binding.scoped_paths(path)}
            owned_scopes = {scope for path in owned_paths for scope in binding.scoped_paths(path)}
            shared_paths = tuple(sorted(shared_scopes))
            owned_paths = tuple(sorted(owned_scopes - shared_scopes))
        manifest = ChangeManifest(
            change_id=f"checkout:{intent.intent_id}",
            task_id=self.policy.authority_task_id,
            lane_role=lane_role,
            base_commit=intent.base_commit,
            owner=f"{intent.task_id}:{intent.thread_id}",
            production_effect="none",
            owned_paths=owned_paths,
            shared_paths=shared_paths,
            module_ids=(),
            contract_claims=contracts,
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
            (f"policy={policy_identity.git_common_dir};target={audited_identity.git_common_dir}"),
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
        result = _checkout_git_result(
            policy_root, ("worktree", "list", "--porcelain", "-z"),
            configuration=("-c", "core.quotepath=false"),
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
        result = _checkout_git_result(
            # A repository-configured fsmonitor hook executes code even for
            # status. Read-only audit must not invoke that callback.
            root, args,
            configuration=("-c", "core.quotepath=false", "-c", "core.fsmonitor=false"),
            environment=git_environment,
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
        result = _checkout_git_result(
            # Porcelain diff otherwise refreshes stat-only index entries even
            # with GIT_OPTIONAL_LOCKS=0. An audit must preserve exact bytes and
            # file identity, including an installed zero-stat index.
            root, args,
            configuration=("-c", "core.quotepath=false", "-c", "core.fsmonitor=false",
                           "-c", "diff.autoRefreshIndex=false"),
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


def _source_only_scope(
    operation_class: CheckoutOperationClass,
    owned_paths: Sequence[str],
    shared_paths: Sequence[str],
) -> tuple[str, ...]:
    if (
        operation_class is not CheckoutOperationClass.SHARED_MUTATION
        or owned_paths
        or list(shared_paths) != sorted(set(shared_paths))
        or CHECKOUT_SOURCE_ONLY_RUNTIME not in shared_paths
    ):
        raise CheckoutGuardError(
            "CHECKOUT_SOURCE_ONLY_SCOPE", "exact shared checkpoint scope required"
        )
    paths = tuple(path for path in shared_paths if path != CHECKOUT_SOURCE_ONLY_RUNTIME)
    if not paths or len(paths) > _CHECKPOINT_MAX_FILES:
        raise CheckoutGuardError("CHECKOUT_SOURCE_ONLY_BUDGET", "source path count")
    if len({path.casefold() for path in paths}) != len(paths):
        raise CheckoutGuardError("CHECKOUT_SOURCE_ONLY_SCOPE", "casefold duplicate paths")
    for path in paths:
        _portable_path(path, "source path")
        parts = PurePosixPath(path).parts
        name = parts[-1].casefold()
        if (
            ":" in path
            or any(
                part.casefold() in {".git", ".ssh", ".aws", ".azure", ".gnupg"} for part in parts
            )
            or name in {".env", "credentials", "credentials.json", "secrets.yaml", "secrets.yml"}
            or name.startswith((".env.", "id_rsa", "id_ed25519"))
            or name.endswith((".pem", ".key", ".p12", ".pfx"))
            or _paths_overlap(path, CHECKOUT_SOURCE_ONLY_RUNTIME)
        ):
            raise CheckoutGuardError("CHECKOUT_SOURCE_ONLY_SCOPE", "private or runtime source path")
    return paths


def _read_checkout_intent(project_root: Path, path: Path) -> CheckoutOperationIntent:
    """Bounded authority read; reject links before opening or reading their bytes."""
    try:
        relative = path.absolute().relative_to(project_root).as_posix()
        _assert_no_reparse_components(project_root, relative)
        before = path.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_size > _INTENT_MAX_BYTES
        ):
            raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", "regular bounded intent required")

        def identity(value: os.stat_result) -> tuple[int, ...]:
            return (
                value.st_dev,
                value.st_ino,
                value.st_size,
                value.st_mtime_ns,
                value.st_ctime_ns,
                value.st_mode,
                value.st_nlink,
            )

        with path.open("rb") as stream:
            if identity(os.fstat(stream.fileno())) != identity(before):
                raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", "intent changed before read")
            content = stream.read(_INTENT_MAX_BYTES + 1)
            if identity(os.fstat(stream.fileno())) != identity(before):
                raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", "intent changed during read")
        if len(content) != before.st_size or identity(path.lstat()) != identity(before):
            raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", "intent changed after read")
        payload = load_strict_json_text(content.decode("utf-8"))
        return parse_checkout_operation_intent(_mapping(payload, "intent"))
    except (OSError, ValueError) as exc:
        raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", type(exc).__name__) from exc


def _release_paths(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise CheckoutGuardError("CHECKOUT_INTENT_INVALID", field)
    return tuple(_portable_path(item, field) for item in value)


def _checkout_git_result(
    root: Path, arguments: Sequence[str], *, configuration: Sequence[str] = (),
    environment: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[bytes]:
    from ai_trading_system.platform.architecture.source_preservation import inspection_git_result

    protected = inspection_git_result(root, *arguments)
    if protected is not None:
        return protected
    return subprocess.run(
        ["git", *configuration, *arguments], cwd=root, check=False,
        capture_output=True, env=environment,
    )


def _git_output(
    root: Path,
    args: Sequence[str],
    *,
    required: bool,
) -> str | None:
    try:
        result = _checkout_git_result(root, args)
    except OSError as exc:
        raise CheckoutGuardError("CHECKOUT_GIT_EXECUTION", str(exc)) from exc
    if result.returncode != 0:
        if not required:
            return None
        raise CheckoutGuardError(
            "CHECKOUT_GIT_FAILED",
            result.stderr.decode("utf-8").strip() or " ".join(args),
        )
    value = result.stdout.decode("utf-8").strip()
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
