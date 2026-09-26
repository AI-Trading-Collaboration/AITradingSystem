from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import wraps
from pathlib import Path, PurePosixPath
from typing import Any, TypeVar, cast

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutGuardError,
    CheckoutLeaseGuard,
    CheckoutOperationClass,
)
from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
from ai_trading_system.platform.architecture.parallel_control_kernel import ExecutionLease
from ai_trading_system.platform.artifacts import canonical_json_bytes, write_json_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_SCHEMA_VERSION = "arch_005_integration_publication_fence_policy.v1"
TRANSACTION_SCHEMA_VERSION = "integration_publication_fence.v1"
EVENT_SCHEMA_VERSION = "integration_publication_fence_event.v1"
REPLAY_SCHEMA_VERSION = "integration_publication_fence_replay.v1"
RECEIPT_SCHEMA_VERSION = "integration_publication_closeout_receipt.v1"
# Bounded combined runtime/code/custody/seven-readiness inspection, not a Full
# execution or lease timeout. DEVX-015 V3 v83 measured 50.773s before custody and
# startup: allow the existing 120s readiness envelope plus 60s identity/custody.
# Timeout still refuses publication; the later locked identity rechecks remain.
# 2026-09-26: raised to the protected envelope (360s) after formal Full load (16
# workers plus nested Full children) exceeded 180s; unloaded runs take 59-75s.
FULL_PROFILE_INSPECTION_TIMEOUT_SECONDS = 360
# Installed child additionally reconstructs complete native runtime custody.
# V345 measured 132.216s with ACL/elevation seams; reserve a separate bounded
# 180s admission envelope, preserving the original 180s profile envelope.
# This is a subprocess limit, not permission to skip any check or retry Full.
FULL_PROFILE_PROTECTED_INSPECTION_TIMEOUT_SECONDS = 360


def _full_profile_inspector_command(
    candidate_root: Path, transaction: Path, task_id: str,
) -> list[str]:
    # The candidate is data. Never run its copy of the inspector under the
    # publication caller's identity. Deployment must protect this implementation.
    script = Path(__file__).resolve().parents[4] / "scripts/run_validation_tier.py"
    if not script.is_file():
        raise PublicationFenceError("PUBLICATION_INSPECTOR_UNAVAILABLE", str(script))
    installed = script.is_relative_to(Path(sys.executable).absolute().parent)
    return [
        sys.executable, "-I", *(["-S", "-B"] if installed else []),
        str(script), "full", "--inspect-full-publication-profile",
        *(["--protected-inspector"] if installed else []),
        "--inspection-candidate-root", str(candidate_root),
        "--publication-transaction", str(transaction), "--task-id", task_id,
    ]
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "architecture" / "arch_005_integration_publication_fence.yaml"
)

_F = TypeVar("_F", bound=Callable[..., Any])
_RemotePreparation = tuple[str, str, str, bytes]


def _serialized_transition(function: _F) -> _F:
    """Serialize replay/check/lease/event/receipt through the one existing store."""

    @wraps(function)
    def serialized(self: Any, *args: Any, **kwargs: Any) -> Any:
        actor = str(kwargs.get("actor", ""))
        try:
            if function.__name__ == "acquire":
                # Checkout inspection may spawn Git and must not hold the host
                # arbiter. acquire serializes only its final transaction write;
                # CheckoutLeaseGuard serializes the exclusive lease admission.
                self.guard.store._assert_writer("compound")
                return function(self, *args, **kwargs)
            if function.__name__ == "checkpoint":
                if "_remote_preparation" in kwargs or "_profile_preparation" in kwargs:
                    raise PublicationFenceError(
                        "PUBLICATION_CALLER_OBSERVATION_FORBIDDEN", "internal observation only"
                    )
                kwargs["_remote_preparation"] = self._prepare_remote_checkpoint(
                    args[0] if args else kwargs.get("transaction"),
                    phase=str(kwargs.get("phase", "")),
                    actor=actor,
                    now=kwargs.get("now"),
                )
                kwargs["_profile_preparation"] = self._prepare_profile_checkpoint(
                    args[0] if args else kwargs.get("transaction"),
                    phase=str(kwargs.get("phase", "")),
                    actor=actor,
                    now=kwargs.get("now"),
                )
            now = _aware_utc(kwargs.get("now") or datetime.now(tz=UTC))
            with self.guard.store.atomic(
                actor=actor,
                now=now,
                operation="terminal" if function.__name__ == "release" else "compound",
            ):
                if function.__name__ in {"checkpoint", "release"}:
                    transaction = args[0] if args else kwargs.get("transaction")
                    with self._hold_transaction(transaction):
                        if function.__name__ == "checkpoint":
                            with self._hold_checkpoint_main():
                                return function(self, *args, **kwargs)
                        return function(self, *args, **kwargs)
                return function(self, *args, **kwargs)
        except ParallelControlError as exc:
            code = exc.code
            if code == "LEASE_ARBITER_BUSY":
                code = (
                    "PUBLICATION_LEASE_CONFLICT"
                    if function.__name__ == "acquire"
                    else "PUBLICATION_BUSY"
                )
            raise PublicationFenceError(code, str(exc)) from exc

    return cast(_F, serialized)


class PublicationFenceError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class PublicationFencePolicy:
    policy_id: str
    version: str
    status: str
    owner: str
    approval_ref: str
    checkout_guard_policy: str
    parallel_control_policy: str
    transaction_root: str
    exclusive_publication_resource: str
    exclusive_validation_resource: str
    phase_order: tuple[str, ...]
    allowed_generator_ids: tuple[str, ...]
    require_exact_declared_order: bool
    required_formal_tiers: tuple[str, ...]
    heavyweight_tier: str
    failure_fix_trigger: str

    @property
    def policy_version(self) -> str:
        return f"{self.policy_id}@{self.version}"


@dataclass(frozen=True)
class PublicationReplay:
    status: str
    transaction: Mapping[str, Any]
    events: tuple[Mapping[str, Any], ...]
    phase: str
    candidate_sha: str | None
    issues: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": REPLAY_SCHEMA_VERSION,
            "status": self.status,
            "transaction_id": self.transaction.get("transaction_id"),
            "transaction_sha256": self.transaction.get("transaction_sha256"),
            "lease_id": self.transaction.get("lease_id"),
            "phase": self.phase,
            "candidate_sha": self.candidate_sha,
            "event_count": len(self.events),
            "head_event_id": self.events[-1].get("event_id") if self.events else None,
            "issues": list(self.issues),
            "production_effect": "none",
            "broker_action": "none",
        }


class IntegrationPublicationFence:
    """Coordinator transaction layered on the existing S4D checkout lease authority."""

    def __init__(
        self,
        *,
        project_root: Path,
        policy_path: Path = DEFAULT_POLICY_PATH,
        runtime_root: Path | None = None,
        checkout_runtime_root: Path | None = None,
        checkout_guard_policy_path: Path | None = None,
        parallel_control_policy_path: Path | None = None,
    ) -> None:
        self.project_root = project_root.resolve()
        self.policy_path = policy_path.resolve()
        self.policy = load_publication_fence_policy(self.policy_path)
        self.policy_sha256 = _sha256_file(self.policy_path)
        self.runtime_root = (
            runtime_root.resolve()
            if runtime_root is not None
            else (self.project_root / self.policy.transaction_root).resolve()
        )
        if not self.runtime_root.is_relative_to(self.project_root):
            raise PublicationFenceError(
                "PUBLICATION_RUNTIME_OUTSIDE_REPOSITORY",
                str(self.runtime_root),
            )
        guard_policy = checkout_guard_policy_path or (
            self.project_root / self.policy.checkout_guard_policy
        )
        parallel_policy = parallel_control_policy_path or (
            self.project_root / self.policy.parallel_control_policy
        )
        self.guard = CheckoutLeaseGuard(
            project_root=self.project_root,
            runtime_root=checkout_runtime_root,
            policy_path=guard_policy,
            parallel_policy_path=parallel_policy,
        )

    @_serialized_transition
    def acquire(
        self,
        *,
        transaction_id: str,
        task_id: str,
        change_id: str,
        thread_id: str,
        actor: str,
        frozen_base_sha: str,
        lane_head_sha: str,
        expected_main_sha: str,
        owned_paths: Sequence[str],
        shared_paths: Sequence[str],
        generator_ids: Sequence[str],
        required_validation_tiers: Sequence[str] | None = None,
        integration_plan_path: Path | None = None,
        full_parent_path: Path | None = None,
        now: datetime | None = None,
    ) -> dict[str, object]:
        instant = _aware_utc(now or datetime.now(tz=UTC))
        checked_id = _identifier(transaction_id, "transaction_id")
        transaction_dir = self.runtime_root / "transactions" / checked_id
        transaction_path = transaction_dir / "transaction.json"
        if transaction_path.exists():
            with self.guard.store.atomic(actor=actor, now=instant, operation="compound"):
                existing = self._load_transaction(transaction_path)
                self._validate_immutable_acquire_replay(
                    existing,
                    task_id=task_id,
                    change_id=change_id,
                    thread_id=thread_id,
                    actor=actor,
                    frozen_base_sha=frozen_base_sha,
                    expected_main_sha=expected_main_sha,
                    lane_head_sha=lane_head_sha,
                    owned_paths=owned_paths,
                    shared_paths=shared_paths,
                    generator_ids=generator_ids,
                    required_validation_tiers=required_validation_tiers,
                    integration_plan_path=integration_plan_path,
                    full_parent_path=full_parent_path,
                )
                replay = self.replay(transaction_path)
                if replay.status != "PASS":
                    raise PublicationFenceError(
                        "PUBLICATION_REPLAY_INVALID",
                        ",".join(replay.issues),
                    )
                return self._binding(replay)
        current_head = _git(self.project_root, "rev-parse", "HEAD")
        current_main = _git(self.project_root, "rev-parse", "main")
        if current_head != lane_head_sha:
            raise PublicationFenceError(
                "PUBLICATION_LANE_HEAD_DRIFT",
                f"declared={lane_head_sha};observed={current_head}",
            )
        if current_main != expected_main_sha:
            raise PublicationFenceError(
                "PUBLICATION_EXPECTED_MAIN_STALE",
                f"declared={expected_main_sha};observed={current_main}",
            )
        _require_ancestor(self.project_root, frozen_base_sha, lane_head_sha)
        _require_ancestor(self.project_root, expected_main_sha, lane_head_sha)

        checked_owned = _checked_paths(owned_paths)
        checked_shared = _checked_paths(
            (
                *shared_paths,
                self.policy.exclusive_publication_resource,
                self.policy.exclusive_validation_resource,
            )
        )
        overlap = sorted(set(checked_owned) & set(checked_shared))
        if overlap:
            raise PublicationFenceError(
                "PUBLICATION_PATH_SCOPE_OVERLAP",
                ",".join(overlap),
            )
        checked_generators = tuple(_identifier(row, "generator_id") for row in generator_ids)
        unknown_generators = sorted(
            set(checked_generators) - set(self.policy.allowed_generator_ids)
        )
        if unknown_generators:
            raise PublicationFenceError(
                "PUBLICATION_GENERATOR_NOT_ALLOWED",
                ",".join(unknown_generators),
            )
        if len(set(checked_generators)) != len(checked_generators):
            raise PublicationFenceError(
                "PUBLICATION_GENERATOR_DUPLICATE",
                ",".join(checked_generators),
            )
        if self.policy.require_exact_declared_order:
            expected_order = tuple(
                row for row in self.policy.allowed_generator_ids if row in checked_generators
            )
            if checked_generators != expected_order:
                raise PublicationFenceError(
                    "PUBLICATION_GENERATOR_ORDER_INVALID",
                    f"declared={checked_generators};expected={expected_order}",
                )
        tiers = tuple(required_validation_tiers or self.policy.required_formal_tiers)
        missing_tiers = sorted(set(self.policy.required_formal_tiers) - set(tiers))
        if missing_tiers:
            raise PublicationFenceError(
                "PUBLICATION_REQUIRED_VALIDATION_MISSING",
                ",".join(missing_tiers),
            )

        plan_binding = _optional_json_binding(
            self.project_root,
            integration_plan_path,
            id_field="plan_id",
        )
        parent_binding = _optional_file_binding(self.project_root, full_parent_path)
        guard_intent_id = f"publication-{checked_id}"
        try:
            decision, handle = self.guard.acquire(
                intent_id=guard_intent_id,
                task_id=_required_text(task_id, "task_id"),
                thread_id=_required_text(thread_id, "thread_id"),
                actor=_required_text(actor, "actor"),
                operation_class=CheckoutOperationClass.SHARED_MUTATION,
                owned_paths=checked_owned,
                shared_paths=checked_shared,
                base_commit=lane_head_sha,
                now=instant,
            )
        except CheckoutGuardError as exc:
            raise PublicationFenceError(exc.code, exc.message) from exc
        if decision.status != "PASS" or handle is None:
            raise PublicationFenceError(
                "PUBLICATION_LEASE_CONFLICT",
                ",".join(decision.reason_codes),
            )

        body: dict[str, object] = {
            "schema_version": TRANSACTION_SCHEMA_VERSION,
            "transaction_id": checked_id,
            "task_id": _required_text(task_id, "task_id"),
            "change_id": _identifier(change_id, "change_id"),
            "thread_id": _required_text(thread_id, "thread_id"),
            "actor": _required_text(actor, "actor"),
            "repository_root": self.project_root.as_posix(),
            "workspace_identity": decision.intent.workspace_identity.to_dict(),
            "frozen_base_sha": _sha(frozen_base_sha, "frozen_base_sha"),
            "lane_head_sha": _sha(lane_head_sha, "lane_head_sha"),
            "expected_main_sha": _sha(expected_main_sha, "expected_main_sha"),
            "candidate_sha": None,
            "integration_revalidation_plan": plan_binding,
            "owned_paths": list(checked_owned),
            "shared_paths": list(checked_shared),
            "generator_ids": list(checked_generators),
            "required_validation_tiers": list(tiers),
            "full_parent": parent_binding,
            "policy_version": self.policy.policy_version,
            "policy_sha256": self.policy_sha256,
            "lease_authority": "execution_lease.v1/FileExecutionLeaseStore",
            "lease_id": handle.lease_id,
            "checkout_intent_path": decision.intent_path.as_posix(),
            "created_at": instant.isoformat(),
            "production_effect": "none",
            "broker_action": "none",
        }
        transaction_sha = _json_sha256(body)
        transaction = {**body, "transaction_sha256": transaction_sha}
        release_on_failure = True
        try:
            with self.guard.store.atomic(actor=actor, now=instant, operation="compound"):
                if transaction_path.exists():
                    existing = self._load_transaction(transaction_path)
                    # Never release a lease already attached to another caller's
                    # successfully persisted transaction, including replay errors.
                    release_on_failure = existing.get("lease_id") != handle.lease_id
                    self._validate_immutable_acquire_replay(
                        existing, task_id=task_id, change_id=change_id, thread_id=thread_id,
                        actor=actor, frozen_base_sha=frozen_base_sha,
                        expected_main_sha=expected_main_sha, lane_head_sha=lane_head_sha,
                        owned_paths=owned_paths, shared_paths=shared_paths,
                        generator_ids=generator_ids,
                        required_validation_tiers=required_validation_tiers,
                        integration_plan_path=integration_plan_path,
                        full_parent_path=full_parent_path,
                    )
                    replay = self.replay(transaction_path)
                    if replay.status != "PASS":
                        raise PublicationFenceError(
                            "PUBLICATION_REPLAY_INVALID", ",".join(replay.issues),
                        )
                    return self._binding(replay)
                # The acquired exclusive lease protects the publication scope.
                # Recheck Git identities before materializing the transaction.
                if (_git(self.project_root, "rev-parse", "HEAD") != current_head
                        or _git(self.project_root, "rev-parse", "main") != current_main):
                    raise PublicationFenceError(
                        "PUBLICATION_ACQUIRE_IDENTITY_CHANGED",
                        "Git identity changed during admission",
                    )
                transaction_dir.mkdir(parents=True, exist_ok=True)
                _write_json_exclusive(transaction_path, transaction)
                self._append_event(
                    transaction_path,
                    phase="ACQUIRED",
                    actor=actor,
                    payload={
                        "lease_id": handle.lease_id,
                        "observed_head": current_head,
                        "observed_main": current_main,
                    },
                    occurred_at=instant,
                )
                return self._binding(self.replay(transaction_path))
        except BaseException:
            if release_on_failure:
                handle.release(outcome="failed", at=instant)
            raise

    def replay(self, transaction: Path | str) -> PublicationReplay:
        transaction_path = self._transaction_path(transaction)
        payload = self._load_transaction(transaction_path)
        issues: list[str] = []
        transaction_body = dict(payload)
        observed_transaction_sha = str(transaction_body.pop("transaction_sha256", ""))
        if _json_sha256(transaction_body) != observed_transaction_sha:
            issues.append("PUBLICATION_TRANSACTION_HASH_MISMATCH")
        if payload.get("policy_sha256") != self.policy_sha256:
            issues.append("PUBLICATION_POLICY_HASH_MISMATCH")
        event_paths = sorted((transaction_path.parent / "events").glob("*.json"))
        events: list[Mapping[str, Any]] = []
        prior_id: str | None = None
        candidate_sha: str | None = None
        for expected_sequence, path in enumerate(event_paths, start=1):
            try:
                event = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                issues.append(f"PUBLICATION_EVENT_UNREADABLE:{path.name}")
                continue
            if not isinstance(event, dict):
                issues.append(f"PUBLICATION_EVENT_NOT_OBJECT:{path.name}")
                continue
            event_body = dict(event)
            observed_event_id = str(event_body.pop("event_id", ""))
            if _json_sha256(event_body) != observed_event_id:
                issues.append(f"PUBLICATION_EVENT_HASH_MISMATCH:{path.name}")
            if event.get("schema_version") != EVENT_SCHEMA_VERSION:
                issues.append(f"PUBLICATION_EVENT_SCHEMA:{path.name}")
            if event.get("sequence") != expected_sequence:
                issues.append(f"PUBLICATION_EVENT_SEQUENCE:{path.name}")
            if event.get("previous_event_id") != prior_id:
                issues.append(f"PUBLICATION_EVENT_CHAIN:{path.name}")
            if event.get("transaction_sha256") != observed_transaction_sha:
                issues.append(f"PUBLICATION_EVENT_TRANSACTION_BINDING:{path.name}")
            event_payload = event.get("payload")
            if isinstance(event_payload, dict) and event_payload.get("candidate_sha"):
                candidate_sha = str(event_payload["candidate_sha"])
            prior_id = observed_event_id
            events.append(event)
        if not events:
            issues.append("PUBLICATION_EVENT_MISSING")
            phase = "MISSING"
        else:
            phase = str(events[-1].get("phase"))
            self._validate_phase_chain(events, issues)
        return PublicationReplay(
            status="PASS" if not issues else "FAIL",
            transaction=payload,
            events=tuple(events),
            phase=phase,
            candidate_sha=candidate_sha,
            issues=tuple(issues),
        )

    def observe_remote(self, transaction: Path | str) -> dict[str, object]:
        """Read the actual push endpoint; this observation never grants a push."""
        replay = self.replay(transaction)
        if replay.status != "PASS" or replay.candidate_sha is None:
            raise PublicationFenceError("PUBLICATION_REMOTE_BINDING_MISSING", "candidate")
        prior = next(
            (event for event in replay.events if event["phase"] == "REMOTE_PUSH_PRE"), None
        )
        endpoint_sha = None
        if prior is not None:
            frozen = prior.get("payload", {}).get("remote_observation")
            if not isinstance(frozen, dict) or not isinstance(frozen.get("endpoint_sha256"), str):
                raise PublicationFenceError("PUBLICATION_REMOTE_BINDING_MISSING", "endpoint")
            endpoint_sha = frozen["endpoint_sha256"]
        elif replay.phase != "LOCAL_MAIN_FF_PRE":
            raise PublicationFenceError("PUBLICATION_REMOTE_BINDING_MISSING", replay.phase)
        common: dict[str, object] = {
            "candidate_sha": replay.candidate_sha,
            "push_allowed": False,
            "mutation_performed": False,
            "allowed_actions": ["remote-observe"],
        }
        try:
            observed = _observe_push_remote(self.project_root, expected_endpoint_sha=endpoint_sha)
        except PublicationFenceError as exc:
            if exc.code != "PUBLICATION_REMOTE_UNKNOWN":
                raise
            return {**common, "status": "REMOTE_UNKNOWN", "reason_code": exc.code}
        return {
            **common,
            **observed,
            "candidate_is_remote_tip": observed["tip_sha"] == replay.candidate_sha,
        }

    def inspect_local_publication(self, transaction: Path | str) -> dict[str, Any]:
        """Observe actual candidate/main checkout identities without publication."""
        from ai_trading_system.platform.architecture.workflow_integration import (
            inspect_local_publication_topology,
        )

        before = self.replay(transaction)
        binding = self.validate(
            transaction, exact_phase="LOCAL_MAIN_FF_PRE", require_candidate=True,
        )
        self._require_clean_candidate()
        topology = inspect_local_publication_topology(
            self.project_root, candidate=str(binding["candidate_sha"]),
            expected_main=str(binding["expected_main_sha"]),
        )
        intent = before.events[-1].get("payload", {}).get("local_publication_intent")
        expected_intent = self._local_publication_intent(before, topology)
        if intent != expected_intent:
            raise PublicationFenceError(
                "PUBLICATION_LOCAL_INTENT_CHANGED", "original checkpoint topology differs"
            )
        self._require_clean_candidate()
        self.validate(transaction, exact_phase="LOCAL_MAIN_FF_PRE", require_candidate=True)
        if self.replay(transaction) != before:
            raise PublicationFenceError("PUBLICATION_OBSERVATION_CHANGED", "local topology")
        return {
            "status": "OBSERVED", "transaction_sha256": binding["transaction_sha256"],
            "head_event_id": before.events[-1]["event_id"], "topology": topology,
            "publication_allowed": False, "dispatch_allowed": False, "mutation_performed": False,
        }

    @staticmethod
    def _local_publication_intent(
        replay: PublicationReplay, topology: Mapping[str, Any],
    ) -> dict[str, object]:
        """Original pre-write observation, not an executor or recovery capability.

        The existing phase event owns this record. A future execution request
        must independently bind that event and its original lease custody before
        any Git write; a caller-supplied or rehashed copy is never authority.
        """
        return {
            "schema_version": "integration_publication_local_intent.v1",
            "transaction_sha256": replay.transaction["transaction_sha256"],
            "lease_id": replay.transaction["lease_id"],
            "candidate_sha": replay.candidate_sha,
            "expected_main_sha": replay.transaction["expected_main_sha"],
            "topology": dict(topology),
            "dispatch_allowed": False,
            "publication_allowed": False,
        }

    def validate_publication_head_handoff(self, transaction: Path | str) -> dict[str, object]:
        """Original pre-resume HEAD seam; ordinary validate remains strict C-HEAD.

        The only source of the effect record is this fence's active lease event
        history. No caller-supplied observation or candidate-check bypass flag
        admits a switched checkout. This method itself grants no resume rights.
        """
        return self._validate_publication_window(transaction, merge=False)

    def validate_publication_merge_window(
        self, transaction: Path | str, *, auto_merge_cleanup: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        """Original resumed Git effects only; not ordinary publication admission."""
        return self._validate_publication_window(
            transaction, merge=True, auto_merge_cleanup=auto_merge_cleanup,
        )

    def validate_publication_recovery_window(self, transaction: Path | str) -> dict[str, object]:
        """Only the original failed terminal Job may restore its own M-side effects."""
        return self._validate_publication_window(transaction, merge=False, recovery=True)

    def validate_publication_main_advanced_recovery_window(
        self, transaction: Path | str,
    ) -> dict[str, object]:
        """Original terminal attempt only; preserve N and its handed-off peer at M."""
        return self._validate_publication_window(
            transaction, merge=False, recovery=True, main_advanced_recovery=True,
        )

    def _validate_publication_window(
        self, transaction: Path | str, *, merge: bool, recovery: bool = False,
        auto_merge_cleanup: Mapping[str, object] | None = None,
        main_advanced_recovery: bool = False,
    ) -> dict[str, object]:
        from ai_trading_system.platform.architecture.workflow_coordination import validate_execution
        from ai_trading_system.platform.architecture.workflow_integration import (
            inspect_publication_main_advanced_recovery_window,
            inspect_publication_merge_window,
            inspect_publication_recovery_window,
            inspect_publication_switched_heads,
        )

        if main_advanced_recovery and (not recovery or merge or auto_merge_cleanup is not None):
            raise PublicationFenceError("PUBLICATION_RECOVERY_WINDOW_KIND", "N recovery only")
        replay = self.replay(transaction)
        if replay.status != "PASS":
            raise PublicationFenceError("PUBLICATION_REPLAY_INVALID", ",".join(replay.issues))
        self._require_phase(replay.phase, exact_phase="LOCAL_MAIN_FF_PRE", minimum_phase=None)
        self._validate_plan_binding(replay.transaction)
        lease = self._active_lease(replay)
        validate_execution(lease)
        execution = lease.execution
        if (not isinstance(execution, Mapping) or not execution.get("publication_attempts")
                or execution["schema_version"] != "lease_execution.v5"):
            raise PublicationFenceError("PUBLICATION_HEAD_HANDOFF_REQUIRED", "original attempt")
        attempt = execution["publication_attempts"][-1]
        request, effect = attempt["request"], attempt.get("checkout_effect")
        valid_window = (
            attempt["state"] == "RESULT_RECORDED" and attempt["result"]["status"] != "PASS"
            and isinstance(effect, Mapping)
            and effect.get("state") in {"HEAD_HANDOFF_INTENT", "HEADS_SWITCHED"}
        ) if recovery else (attempt["state"] in (
            {"RUNNING", "EXIT_CONFIRMED", "RESULT_RECORDED"} if merge else {"RUNNING"}
        ) and isinstance(effect, Mapping) and effect.get("state") == "HEADS_SWITCHED"
            and ("git_merge" in attempt) is merge and "head_recovery" not in attempt)
        if (not valid_window or request.get("publication_transaction_sha256")
                != replay.transaction["transaction_sha256"]
                or request.get("lease_id") != replay.transaction["lease_id"]
                or request.get("candidate_sha") != replay.candidate_sha
                or request.get("expected_main_sha") != replay.transaction["expected_main_sha"]
                or request.get("local_publication_event_id") != replay.events[-1]["event_id"]
                or request.get("local_publication_intent_sha256") != _json_sha256(
                    replay.events[-1]["payload"].get("local_publication_intent")
                )):
            raise PublicationFenceError(
                "PUBLICATION_HEAD_HANDOFF_REQUIRED", "exact pre-resume state",
            )
        if recovery:
            from ai_trading_system.platform.architecture.workflow_execution import (
                observe_job,
                observe_process,
            )

            if (observe_job(request["job_name"])["state"] not in {"EMPTY", "ABSENT"}
                    or observe_process(**attempt["git_launch"]["process"])["state"]
                    not in {"EXITED", "REUSED"}):
                raise PublicationFenceError("PUBLICATION_RECOVERY_NOT_TERMINAL", "original Job/Git")
            if main_advanced_recovery and (
                "main_preparation" in attempt
                or (attempt.get("head_recovery") is not None and attempt["head_recovery"].get(
                    "schema_version"
                ) != "workflow_publication_head_recovery.v2")
            ):
                raise PublicationFenceError(
                    "PUBLICATION_RECOVERY_WINDOW_KIND", "original N attempt",
                )
        if auto_merge_cleanup is not None:
            from ai_trading_system.platform.architecture.workflow_execution import (
                hold_contained_ancestor,
                observe_process,
            )

            if (not merge or recovery or attempt["state"] != "RUNNING"
                    or attempt["git_merge"]["exit"] is not None):
                raise PublicationFenceError("PUBLICATION_AUTO_MERGE_LOCK_PHASE", "live hook only")
            with hold_contained_ancestor(request["job_name"], attempt["git_launch"]["process"]):
                if observe_process(**attempt["git_launch"]["worker_process"])["state"] != "RUNNING":
                    raise PublicationFenceError("PUBLICATION_AUTO_MERGE_LOCK_PHASE", "worker dead")
                observation = inspect_publication_merge_window(
                    self.project_root, request, attempt["checkout_plan"]["plan"],
                    attempt["git_merge"], auto_merge_cleanup=auto_merge_cleanup,
                )
        else:
            observation = (inspect_publication_main_advanced_recovery_window(
                self.project_root, request, attempt["checkout_plan"]["plan"],
                attempt.get("git_merge"), attempt.get("head_recovery"),
            ) if main_advanced_recovery else inspect_publication_recovery_window(
                self.project_root, request, attempt["checkout_plan"]["plan"],
                attempt.get("git_merge"), attempt.get("head_recovery"),
            ) if recovery else
                inspect_publication_merge_window(
                    self.project_root, request, attempt["checkout_plan"]["plan"],
                    attempt["git_merge"],
                ) if merge else inspect_publication_switched_heads(
                    self.project_root, request, attempt["checkout_plan"]["plan"],
                )
            )
        return {**self._binding(replay),
                ("recovery_observation" if recovery else
                 "merge_observation" if merge else "head_handoff_observation"): observation,
                "dispatch_allowed": False, "resume_allowed": False, "publication_allowed": False}

    def validate(
        self,
        transaction: Path | str,
        *,
        minimum_phase: str | None = None,
        exact_phase: str | None = None,
        task_id: str | None = None,
        validation_tier: str | None = None,
        parent_path: Path | None = None,
        require_candidate: bool = False,
        now: datetime | None = None,
    ) -> dict[str, object]:
        replay = self.replay(transaction)
        if replay.status != "PASS":
            raise PublicationFenceError(
                "PUBLICATION_REPLAY_INVALID",
                ",".join(replay.issues),
            )
        if replay.phase in {"FAILED", "RELEASED"}:
            raise PublicationFenceError("PUBLICATION_TRANSACTION_TERMINAL", replay.phase)
        if task_id is not None and replay.transaction.get("task_id") != task_id:
            raise PublicationFenceError(
                "PUBLICATION_TASK_MISMATCH",
                f"transaction={replay.transaction.get('task_id')};requested={task_id}",
            )
        self._require_phase(replay.phase, minimum_phase=minimum_phase, exact_phase=exact_phase)
        self._validate_plan_binding(replay.transaction)
        lease = self._active_lease(replay, now=now)
        current_main = _git(self.project_root, "rev-parse", "main")
        expected_main = str(replay.transaction["expected_main_sha"])
        # A declared validation tier checks V(C), not permission to publish C.
        # Only already-bound validation phases may outlive an independent main
        # advance. Ordinary validation/publication callers retain the main gate.
        fixed_validation = validation_tier is not None and replay.phase in {
            "FORMAL_VALIDATION_PRE",
            "FULL_DISPATCHED",
            "FORMAL_VALIDATION_RESULT",
            "LOCAL_MAIN_FF_PRE",
        }
        if (
            not fixed_validation
            and replay.phase not in {"REMOTE_PUSH_PRE", "CLEANUP_PRE"}
            and current_main != expected_main
        ):
            raise PublicationFenceError(
                "PUBLICATION_EXPECTED_MAIN_STALE",
                f"declared={expected_main};observed={current_main}",
            )
        if require_candidate or validation_tier is not None:
            current_head = _git(self.project_root, "rev-parse", "HEAD")
            if replay.candidate_sha is None or current_head != replay.candidate_sha:
                raise PublicationFenceError(
                    "PUBLICATION_CANDIDATE_DRIFT",
                    f"bound={replay.candidate_sha};observed={current_head}",
                )
        if validation_tier is not None:
            if validation_tier not in replay.transaction["required_validation_tiers"]:
                raise PublicationFenceError(
                    "PUBLICATION_VALIDATION_TIER_UNDECLARED",
                    validation_tier,
                )
            if validation_tier == self.policy.heavyweight_tier:
                validation_resources = {self.policy.exclusive_validation_resource.casefold()}
                coordination = self.guard.store.coordination_binding
                if coordination is not None:
                    validation_resources = {
                        path.casefold()
                        for path in coordination.scoped_paths(
                            self.policy.exclusive_validation_resource
                        )
                    }
                    if "host/" + coordination.host_id + "/full" not in validation_resources:
                        raise PublicationFenceError(
                            "PUBLICATION_FULL_HOST_SCOPE_MISSING",
                            self.policy.exclusive_validation_resource,
                        )
                resources = {
                    str(row.get("resource_id", "")).casefold()
                    for row in lease.to_dict()["resources"]
                    if isinstance(row, dict)
                    and row.get("kind") == "path"
                    and row.get("access") == "WRITE"
                }
                if not validation_resources.issubset(resources):
                    raise PublicationFenceError(
                        "PUBLICATION_FULL_RESOURCE_MISSING",
                        self.policy.exclusive_validation_resource,
                    )
        if validation_tier == self.policy.heavyweight_tier or parent_path is not None:
            self._validate_parent_binding(replay.transaction, parent_path)
        binding = self._binding(replay)
        if fixed_validation:
            binding.update(
                validation_only=True,
                publication_allowed=False,
                observed_main_sha=current_main,
                expected_main_matches_observation=current_main == expected_main,
            )
        return binding

    def _prepare_remote_checkpoint(
        self, transaction: Path | str, *, phase: str, actor: str, now: datetime | None
    ) -> _RemotePreparation | None:
        if phase not in {"REMOTE_PUSH_PRE", "CLEANUP_PRE"}:
            return None
        replay = self.replay(transaction)
        if replay.status != "PASS" or replay.candidate_sha is None:
            raise PublicationFenceError("PUBLICATION_REMOTE_BINDING_MISSING", "candidate")
        if replay.transaction.get("actor") != actor:
            raise PublicationFenceError("PUBLICATION_ACTOR_MISMATCH", actor)
        if phase != self._next_phase(replay.phase):
            raise PublicationFenceError("PUBLICATION_PHASE_TRANSITION_INVALID", phase)
        self._validate_plan_binding(replay.transaction)
        self._active_lease(replay, now=now)
        # Network I/O is outside the short store arbiter. The locked transition
        # below rechecks the exact event head and endpoint before consuming it.
        observation = self.observe_remote(transaction)
        if observation["status"] == "REMOTE_UNKNOWN":
            raise PublicationFenceError(
                "PUBLICATION_REMOTE_UNKNOWN", "retry read-only remote-observe; no push granted"
            )
        return (
            str(replay.transaction["transaction_sha256"]),
            str(replay.events[-1]["event_id"]),
            phase,
            canonical_json_bytes(observation),
        )

    def _prepare_profile_checkpoint(
        self,
        transaction: Path | str,
        *,
        phase: str,
        actor: str,
        now: datetime | None,
    ) -> _RemotePreparation | None:
        if phase not in {"LOCAL_MAIN_FF_PRE", "REMOTE_PUSH_PRE"}:
            return None
        replay = self.replay(transaction)
        if replay.transaction.get("actor") != actor:
            raise PublicationFenceError("PUBLICATION_ACTOR_MISMATCH", actor)
        if phase != self._next_phase(replay.phase):
            raise PublicationFenceError("PUBLICATION_PHASE_TRANSITION_INVALID", phase)
        prepared = self._prepare_current_full_profile(transaction, actor=actor, now=now)
        return (*prepared[:2], phase, prepared[3])

    def _prepare_current_full_profile(
        self, transaction: Path | str, *, actor: str, now: datetime | None = None,
    ) -> _RemotePreparation:
        """Observe current V(C) without advancing or impersonating a checkpoint.

        Called outside the original arbiter. Both checkpoint and the contained
        publication worker must recheck its captures inside that same arbiter.
        """
        replay = self.replay(transaction)
        if replay.transaction.get("actor") != actor:
            raise PublicationFenceError("PUBLICATION_ACTOR_MISMATCH", actor)
        if replay.phase not in {"FORMAL_VALIDATION_RESULT", "LOCAL_MAIN_FF_PRE"}:
            raise PublicationFenceError("PUBLICATION_PHASE_TRANSITION_INVALID", replay.phase)
        self._active_lease(replay, now=now)
        self.validate(
            transaction,
            exact_phase=replay.phase,
            # The remote phase follows the actual local FF. This probe validates
            # original V(C), not old-main freshness or a new Full dispatch. The
            # locked REMOTE_PUSH_PRE still requires main=HEAD=C and remote ancestry.
            validation_tier=(self.policy.heavyweight_tier
                             if replay.phase == "LOCAL_MAIN_FF_PRE" else None),
            task_id=str(replay.transaction["task_id"]),
            require_candidate=True,
            now=now,
        )
        result_events = [row for row in replay.events if row["phase"] == "FORMAL_VALIDATION_RESULT"]
        if (
            len(result_events) != 1
            or result_events[0]["payload"].get("validation_status") != "PASS"
        ):
            raise PublicationFenceError("PUBLICATION_FORMAL_VALIDATION_NOT_PASS", "Full result")
        self._require_clean_candidate()
        # The isolated (-I) inspector never loads caller startup code; the
        # profile-consuming CLI entrypoints attest their own loaded code first.
        # Reuse the runner's strict validator through its read-only public entry
        # point, outside the shared arbiter. No src -> scripts import or second
        # validator, proof store, execution queue, or lease is introduced.
        command = _full_profile_inspector_command(
            self.project_root, self._transaction_path(transaction),
            str(replay.transaction["task_id"]),
        )
        try:
            inspected = subprocess.run(
                command,
                cwd=(Path(sys.executable).absolute().parent
                     if "--protected-inspector" in command else self.project_root),
                capture_output=True,
                timeout=(FULL_PROFILE_PROTECTED_INSPECTION_TIMEOUT_SECONDS
                         if "--protected-inspector" in command
                         else FULL_PROFILE_INSPECTION_TIMEOUT_SECONDS),
                check=False,
            )
            if inspected.returncode:
                raise ValueError(inspected.stderr.decode("utf-8", errors="replace")[-4000:])
            observation = json.loads(inspected.stdout)
            if (
                observation.get("schema_version") != "full_publication_profile_inspection.v1"
                or observation.get("status") != "PASS"
                or observation.get("transaction_sha256") != replay.transaction["transaction_sha256"]
                or observation.get("head_event_id") != replay.events[-1]["event_id"]
                or observation.get("candidate_sha") != replay.candidate_sha
            ):
                raise ValueError("profile inspection identity differs")
        except (OSError, ValueError, TypeError, subprocess.TimeoutExpired) as exc:
            raise PublicationFenceError("PUBLICATION_FULL_CLOSURE_INVALID", str(exc)) from exc
        return (
            str(replay.transaction["transaction_sha256"]),
            str(replay.events[-1]["event_id"]),
            replay.phase,
            canonical_json_bytes(observation),
        )

    def _recheck_full_profile(
        self, replay: PublicationReplay, execution: Mapping[str, Any] | None,
        preparation: _RemotePreparation | None, *, phase: str,
    ) -> dict[str, Any]:
        """Recheck original observation under the caller's original store atomic."""
        from ai_trading_system.platform.architecture.workflow_contract import (
            WorkflowContractError,
            bounded_regular_bytes,
            canonical_digest,
        )

        expected = (str(replay.transaction["transaction_sha256"]),
                    str(replay.events[-1]["event_id"]), phase)
        if preparation is None or preparation[:3] != expected:
            raise PublicationFenceError("PUBLICATION_FULL_CLOSURE_INVALID", "stale inspection")
        observation: dict[str, Any] = json.loads(preparation[3])
        if observation["execution_sha256"] != canonical_digest(execution):
            raise PublicationFenceError("PUBLICATION_FULL_CLOSURE_INVALID", "execution changed")
        implementation_root = Path(__file__).resolve().parents[4]
        for row in observation["captures"]:
            path = Path(row["path"])
            inspector_source = path.suffix == ".py" and any(
                path.is_relative_to(implementation_root / part) for part in ("src", "scripts")
            )
            if (not path.is_relative_to(self.project_root) and not inspector_source
                    or ".." in path.parts):
                raise PublicationFenceError("PUBLICATION_FULL_CLOSURE_INVALID", "capture scope")
            try:
                raw = bounded_regular_bytes(path)
            except (OSError, WorkflowContractError) as exc:
                raise PublicationFenceError(
                    "PUBLICATION_FULL_CLOSURE_INVALID", "capture unavailable",
                ) from exc
            if len(raw) != row["size_bytes"] or hashlib.sha256(raw).hexdigest() != row["sha256"]:
                raise PublicationFenceError("PUBLICATION_FULL_CLOSURE_INVALID", "evidence changed")
        return observation

    @_serialized_transition
    def checkpoint(
        self,
        transaction: Path | str,
        *,
        phase: str,
        actor: str,
        evidence_paths: Sequence[Path] = (),
        generator_ids: Sequence[str] = (),
        full_run_id: str | None = None,
        validation_status: str | None = None,
        now: datetime | None = None,
        _remote_preparation: _RemotePreparation | None = None,
        _profile_preparation: _RemotePreparation | None = None,
    ) -> dict[str, object]:
        instant = _aware_utc(now or datetime.now(tz=UTC))
        transaction_path = self._transaction_path(transaction)
        replay = self.replay(transaction_path)
        if replay.status != "PASS":
            raise PublicationFenceError(
                "PUBLICATION_REPLAY_INVALID",
                ",".join(replay.issues),
            )
        remote_observation = None
        if phase in {"REMOTE_PUSH_PRE", "CLEANUP_PRE"}:
            expected = (
                str(replay.transaction["transaction_sha256"]),
                str(replay.events[-1]["event_id"]),
                phase,
            )
            if _remote_preparation is None or _remote_preparation[:3] != expected:
                raise PublicationFenceError(
                    "PUBLICATION_REMOTE_OBSERVATION_STALE", "transaction changed during probe"
                )
            remote_observation = json.loads(_remote_preparation[3])
            endpoint_sha = hashlib.sha256(_push_endpoint(self.project_root).encode()).hexdigest()
            if remote_observation["endpoint_sha256"] != endpoint_sha:
                raise PublicationFenceError(
                    "PUBLICATION_REMOTE_ENDPOINT_CHANGED", "endpoint changed before transition"
                )
        if replay.transaction.get("actor") != actor:
            raise PublicationFenceError("PUBLICATION_ACTOR_MISMATCH", actor)
        expected_next = self._next_phase(replay.phase)
        if phase != expected_next:
            raise PublicationFenceError(
                "PUBLICATION_PHASE_TRANSITION_INVALID",
                f"{replay.phase}->{phase};expected={expected_next}",
            )
        self._validate_plan_binding(replay.transaction)
        lease = self._active_lease(replay, now=instant)
        profile_observation = None
        if phase in {"LOCAL_MAIN_FF_PRE", "REMOTE_PUSH_PRE"}:
            profile_observation = self._recheck_full_profile(
                replay, lease.execution, _profile_preparation, phase=phase,
            )
        payload = self._checkpoint_payload(
            replay,
            phase=phase,
            evidence_paths=evidence_paths,
            generator_ids=generator_ids,
            full_run_id=full_run_id,
            validation_status=validation_status,
            remote_observation=remote_observation,
            lease_execution=lease.execution,
        )
        if profile_observation is not None:
            payload["formal_profile_inspection"] = profile_observation
        # Admission failures must not renew or mutate the original execution lease.
        self._require_unchanged_replay(transaction_path, replay)
        for ref, field in (("HEAD", "observed_head"), ("refs/heads/main", "observed_main")):
            if _git(self.project_root, "rev-parse", ref) != payload[field]:
                raise PublicationFenceError("PUBLICATION_CHECKOUT_CHANGED", ref)
        self.guard.store.heartbeat(
            str(replay.transaction["lease_id"]),
            actor=actor,
            now=instant,
        )
        self._append_event(
            transaction_path,
            phase=phase,
            actor=actor,
            payload=payload,
            occurred_at=instant,
        )
        if phase == "FULL_DISPATCHED":
            self._persist_full_claim(self.replay(transaction_path))
        return self._binding(self.replay(transaction_path))

    def _full_claim(self, replay: PublicationReplay) -> dict[str, Any]:
        event = next((row for row in replay.events if row["phase"] == "FULL_DISPATCHED"), None)
        if event is None:
            raise PublicationFenceError("PUBLICATION_FULL_CLAIM_MISSING", replay.phase)
        payload = event["payload"]
        claim = payload.get("dispatch_request")
        if not isinstance(claim, dict):
            raise PublicationFenceError("PUBLICATION_FULL_LAUNCHER_UNPROVEN", "legacy claim")
        expected = {
            "schema_version": "integration_publication_full_dispatch.v2",
            "transaction_id": replay.transaction["transaction_id"],
            "transaction_sha256": replay.transaction["transaction_sha256"],
            "candidate_sha": replay.candidate_sha,
            "full_run_id": payload["full_run_id"],
            "production_effect": "none",
            "broker_action": "none",
        }
        launcher = claim.get("launcher")
        if (
            set(claim) != {*expected, "launcher", "claim_sha256"}
            or event.get("actor") != replay.transaction["actor"]
            or any(claim.get(key) != value for key, value in expected.items())
            or not isinstance(launcher, dict)
            or set(launcher) != {"pid", "creation_time"}
            or any(type(item) is not int or item <= 0 for item in launcher.values())
            or claim["claim_sha256"]
            != _json_sha256({key: value for key, value in claim.items() if key != "claim_sha256"})
        ):
            raise PublicationFenceError("PUBLICATION_FULL_CLAIM_BINDING", "event claim")
        path = self.runtime_root / "transactions" / str(replay.transaction["transaction_id"])
        expected_ref = {
            "path": (path / "full_dispatch_claim.json")
            .relative_to(
                self.project_root,
            )
            .as_posix(),
            "sha256": hashlib.sha256(canonical_json_bytes(claim)).hexdigest(),
        }
        if payload.get("dispatch_claim") != expected_ref:
            raise PublicationFenceError("PUBLICATION_FULL_CLAIM_BINDING", "projection binding")
        return dict(claim)

    def _persist_full_claim(self, replay: PublicationReplay) -> Path:
        from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes

        claim = self._full_claim(replay)
        path = (
            self.runtime_root
            / "transactions"
            / str(replay.transaction["transaction_id"])
            / "full_dispatch_claim.json"
        )
        if path.exists():
            if bounded_regular_bytes(path) != canonical_json_bytes(claim):
                raise PublicationFenceError("PUBLICATION_FULL_CLAIM_CHANGED", "projection")
        else:
            _write_json_exclusive(path, claim)
        return path

    def require_full_launcher(self, transaction: Path | str) -> None:
        from ai_trading_system.platform.architecture.workflow_execution import (
            current_process_identity,
        )

        replay = self.replay(transaction)
        if replay.status != "PASS" or replay.phase != "FULL_DISPATCHED":
            raise PublicationFenceError("PUBLICATION_FULL_LAUNCHER_PHASE", replay.phase)
        claim = self._full_claim(replay)
        if claim["launcher"] != current_process_identity():
            raise PublicationFenceError(
                "PUBLICATION_FULL_LAUNCHER_MISMATCH",
                "original process only",
            )

    def recover_unstarted_full(self, transaction: Path | str, *, actor: str) -> dict[str, object]:
        """Close a dead original dispatcher with no reserved execution; never redispatch."""
        from ai_trading_system.platform.architecture.workflow_execution import observe_process

        before = self.replay(transaction)
        if before.status != "PASS" or before.transaction["actor"] != actor:
            raise PublicationFenceError("PUBLICATION_FULL_RECOVERY_BINDING", "transaction/actor")
        claim = self._full_claim(before)
        if before.phase not in {"FULL_DISPATCHED", "FAILED"}:
            raise PublicationFenceError("PUBLICATION_FULL_RECOVERY_PHASE", before.phase)
        observed = observe_process(**claim["launcher"])
        if observed["state"] not in {"EXITED", "REUSED"}:
            return {
                "status": "OBSERVE_ONLY",
                "launcher": observed,
                "dispatch_performed": False,
                "publication_allowed": False,
                "allowed_actions": ["observe", "launcher_owned_complete"],
            }
        with self.guard.store.atomic(actor=actor, now=datetime.now(UTC), operation="terminal"):
            replay = self.replay(transaction)
            if replay != before:
                raise PublicationFenceError("PUBLICATION_FULL_RECOVERY_CHANGED", "replay again")
            leases = self.guard.store.replay()
            lease = next(
                (
                    row
                    for row in leases.lease_heads
                    if row.lease_id == replay.transaction["lease_id"]
                ),
                None,
            )
            if leases.status != "PASS" or lease is None or lease.execution is not None:
                raise PublicationFenceError("PUBLICATION_FULL_EXECUTION_PRESENT", "not unstarted")
            if lease.state not in {"ACTIVE", "RELEASED"}:
                raise PublicationFenceError("PUBLICATION_FULL_RECOVERY_OWNER", lease.state)
            self._require_publication_lease_intent(replay, lease)
            path = self._persist_full_claim(replay)
            receipt = self.release(
                transaction,
                actor=actor,
                outcome="FAILED",
                evidence_paths=(path,),
            )
        return {
            "status": "RECOVERED_FAILED_ATTEMPT",
            "technical_status": "NOT_EXECUTED",
            "reason": "ORIGINAL_FULL_DISPATCHER_EXITED_BEFORE_RESERVATION",
            "dispatch_performed": False,
            "publication_allowed": False,
            "receipt": receipt,
        }

    @_serialized_transition
    def release(
        self,
        transaction: Path | str,
        *,
        actor: str,
        outcome: str,
        evidence_paths: Sequence[Path] = (),
        now: datetime | None = None,
    ) -> dict[str, object]:
        instant = _aware_utc(now or datetime.now(tz=UTC))
        transaction_path = self._transaction_path(transaction)
        replay = self.replay(transaction_path)
        if replay.status != "PASS":
            raise PublicationFenceError("PUBLICATION_REPLAY_INVALID", ",".join(replay.issues))
        if replay.transaction.get("actor") != actor:
            raise PublicationFenceError("PUBLICATION_ACTOR_MISMATCH", actor)
        normalized_outcome = _identifier(outcome, "outcome").upper()
        if normalized_outcome not in {"COMPLETED", "FAILED"}:
            raise PublicationFenceError("PUBLICATION_OUTCOME_INVALID", normalized_outcome)
        lease_id = str(replay.transaction["lease_id"])
        lease = (
            self._terminal_replay_lease(replay)
            if replay.phase in {"RELEASED", "FAILED"}
            else next(
                (row for row in self.guard.replay().lease_heads if row.lease_id == lease_id), None
            )
        )
        if lease is None:
            raise PublicationFenceError("PUBLICATION_LEASE_UNKNOWN", lease_id)
        self._require_publication_lease_intent(replay, lease)
        terminal_phase = "RELEASED" if normalized_outcome == "COMPLETED" else "FAILED"
        if replay.phase in {"RELEASED", "FAILED"}:
            if replay.phase != terminal_phase:
                raise PublicationFenceError("PUBLICATION_TERMINAL_REPLAY_INVALID", replay.phase)
            receipt = self._terminal_receipt(replay)
            if (
                evidence_paths
                and _artifact_bindings(self.project_root, evidence_paths) != receipt["evidence"]
            ):
                raise PublicationFenceError("PUBLICATION_TERMINAL_REPLAY_INVALID", "evidence")
            return self._persist_terminal_receipt(transaction_path, receipt)
        if normalized_outcome == "COMPLETED" and replay.phase != "CLEANUP_PRE":
            raise PublicationFenceError(
                "PUBLICATION_CLOSEOUT_PHASE_REQUIRED",
                replay.phase,
            )
        terminal_details: dict[str, object] = {}
        if normalized_outcome == "COMPLETED":
            cleanup = replay.events[-1]
            observation = cleanup.get("payload", {}).get("remote_observation")
            if "remote_observation" in cleanup.get("payload", {}):
                _require_remote_confirmation(observation, replay.candidate_sha)
                terminal_details["remote_confirmation"] = {
                    "candidate_sha": replay.candidate_sha,
                    "source_event_id": cleanup["event_id"],
                    "point_in_time": True,
                    "observation": observation,
                }
        evidence = _artifact_bindings(self.project_root, evidence_paths)
        self._require_unchanged_replay(transaction_path, replay)
        if lease.state == "ACTIVE":
            try:
                self.guard.release(
                    lease_id,
                    actor=actor,
                    outcome=normalized_outcome,
                    evidence_refs=tuple(str(row["path"]) for row in evidence),
                    now=instant,
                )
            except CheckoutGuardError as exc:
                raise PublicationFenceError(exc.code, exc.message) from exc
        elif lease.state != "RELEASED":
            raise PublicationFenceError(
                "PUBLICATION_LEASE_NOT_RELEASABLE",
                f"{lease_id}:{lease.state}",
            )
        self._append_event(
            transaction_path,
            phase=terminal_phase,
            actor=actor,
            payload={
                **terminal_details,
                "outcome": normalized_outcome,
                "evidence": evidence,
                "observed_head": _git(self.project_root, "rev-parse", "HEAD"),
                "observed_main": _git(self.project_root, "rev-parse", "main"),
                "observed_origin_main": _git_optional(
                    self.project_root,
                    "rev-parse",
                    "origin/main",
                ),
            },
            occurred_at=instant,
            allow_terminal=True,
        )
        terminal = self.replay(transaction_path)
        return self._persist_terminal_receipt(transaction_path, self._terminal_receipt(terminal))

    def _terminal_replay_lease(self, replay: PublicationReplay) -> ExecutionLease | None:
        """Only a terminal publication can read its registered retired origin."""
        if replay.status != "PASS" or replay.phase not in {"RELEASED", "FAILED"}:
            raise PublicationFenceError("PUBLICATION_TERMINAL_REPLAY_INVALID", replay.phase)
        current = self.guard.replay()
        if current.status != "PASS":
            raise PublicationFenceError(
                "PUBLICATION_TERMINAL_REPLAY_INVALID", "current_lease_replay"
            )
        lease_id = str(replay.transaction["lease_id"])
        lease = next((row for row in current.lease_heads if row.lease_id == lease_id), None)
        if lease is not None or self.guard.store.coordination_binding is None:
            return lease
        from ai_trading_system.platform.architecture.workflow_coordination import (
            registered_legacy_terminal_lease,
        )

        try:
            return registered_legacy_terminal_lease(
                self.project_root,
                self.guard.runtime_root / "leases",
                policy=self.guard.lease_policy,
                lease_id=lease_id,
            )
        except (ParallelControlError, OSError, ValueError) as exc:
            raise PublicationFenceError("PUBLICATION_TERMINAL_ORIGIN_INVALID", str(exc)) from exc

    def _terminal_receipt(self, replay: PublicationReplay) -> dict[str, object]:
        """Recover from durable terminal facts, never from the retrier's clock or status."""
        if replay.status != "PASS" or replay.phase not in {"RELEASED", "FAILED"}:
            raise PublicationFenceError("PUBLICATION_TERMINAL_REPLAY_INVALID", replay.phase)
        lease_id = str(replay.transaction["lease_id"])
        lease = self._terminal_replay_lease(replay)
        if lease is None or lease.state != "RELEASED":
            raise PublicationFenceError("PUBLICATION_TERMINAL_REPLAY_INVALID", "lease_not_released")
        self._require_publication_lease_intent(replay, lease)
        event = replay.events[-1]
        payload = event.get("payload")
        outcome = "COMPLETED" if replay.phase == "RELEASED" else "FAILED"
        if not isinstance(payload, Mapping) or payload.get("outcome") != outcome:
            raise PublicationFenceError("PUBLICATION_TERMINAL_REPLAY_INVALID", "outcome")
        evidence = payload.get("evidence")
        if not isinstance(evidence, list) or not all(isinstance(row, dict) for row in evidence):
            raise PublicationFenceError("PUBLICATION_TERMINAL_REPLAY_INVALID", "evidence")
        receipt: dict[str, object] = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "status": "PASS" if outcome == "COMPLETED" else "FAIL",
            "outcome": outcome,
            "transaction_id": replay.transaction["transaction_id"],
            "transaction_sha256": replay.transaction["transaction_sha256"],
            "lease_id": lease_id,
            "lease_state": "RELEASED",
            "candidate_sha": replay.candidate_sha,
            "final_phase": replay.phase,
            "head_event_id": event["event_id"],
            "evidence": evidence,
            "completed_at": event["occurred_at"],
            "production_effect": "none",
            "broker_action": "none",
        }
        # Only project a fact saved by this terminal event. Older receipts are
        # replayed unchanged; a new reader must not retrofit confirmation into them.
        confirmation = payload.get("remote_confirmation")
        if confirmation is not None:
            source = next((row for row in replay.events if row["phase"] == "CLEANUP_PRE"), None)
            expected = (
                None
                if source is None
                else {
                    "candidate_sha": replay.candidate_sha,
                    "source_event_id": source["event_id"],
                    "point_in_time": True,
                    "observation": source.get("payload", {}).get("remote_observation"),
                }
            )
            _require_remote_confirmation(
                None if expected is None else expected["observation"], replay.candidate_sha
            )
            if outcome != "COMPLETED" or confirmation != expected:
                raise PublicationFenceError("PUBLICATION_TERMINAL_REPLAY_INVALID", "remote_fact")
            receipt["remote_confirmation"] = confirmation
        return receipt

    def _persist_terminal_receipt(
        self, transaction_path: Path, receipt: dict[str, object]
    ) -> dict[str, object]:
        receipt_path = transaction_path.parent / "closeout_receipt.json"
        if receipt_path.exists():
            try:
                existing = json.loads(receipt_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise PublicationFenceError(
                    "PUBLICATION_TERMINAL_RECEIPT_MISMATCH", str(receipt_path)
                ) from exc
            if existing != receipt:
                raise PublicationFenceError(
                    "PUBLICATION_TERMINAL_RECEIPT_MISMATCH", str(receipt_path)
                )
            return receipt
        write_json_atomic(receipt_path, receipt)
        return receipt

    def _checkpoint_payload(
        self,
        replay: PublicationReplay,
        *,
        phase: str,
        evidence_paths: Sequence[Path],
        generator_ids: Sequence[str],
        full_run_id: str | None,
        validation_status: str | None,
        lease_execution: Mapping[str, Any] | None,
        remote_observation: Mapping[str, Any] | None = None,
    ) -> dict[str, object]:
        current_head = _git(self.project_root, "rev-parse", "HEAD")
        current_main = _git(self.project_root, "rev-parse", "main")
        expected_main = str(replay.transaction["expected_main_sha"])
        payload: dict[str, object] = {
            "observed_head": current_head,
            "observed_main": current_main,
            "evidence": _artifact_bindings(self.project_root, evidence_paths),
        }
        if (
            phase
            in {
                "TASK_SOURCE_PRE_WRITE",
                "GENERATED_REBUILD_PRE",
                "GENERATED_REBUILD_POST",
                "CANDIDATE_COMMIT_PRE",
                "FORMAL_VALIDATION_PRE",
                "LOCAL_MAIN_FF_PRE",
            }
            and current_main != expected_main
        ):
            raise PublicationFenceError(
                "PUBLICATION_EXPECTED_MAIN_STALE",
                f"declared={expected_main};observed={current_main}",
            )
        if phase in {
            "TASK_SOURCE_PRE_WRITE",
            "GENERATED_REBUILD_PRE",
            "GENERATED_REBUILD_POST",
            "CANDIDATE_COMMIT_PRE",
        }:
            if current_head != replay.transaction["lane_head_sha"]:
                raise PublicationFenceError(
                    "PUBLICATION_LANE_HEAD_DRIFT",
                    f"declared={replay.transaction['lane_head_sha']};observed={current_head}",
                )
            self._require_dirty_attributed(replay)
        if phase in {"GENERATED_REBUILD_PRE", "GENERATED_REBUILD_POST"}:
            checked = tuple(_identifier(row, "generator_id") for row in generator_ids)
            declared = tuple(str(row) for row in replay.transaction["generator_ids"])
            if checked != declared:
                raise PublicationFenceError(
                    "PUBLICATION_GENERATOR_ORDER_MISMATCH",
                    f"checkpoint={checked};declared={declared}",
                )
            payload["generator_ids"] = list(checked)
        if phase == "FORMAL_VALIDATION_PRE":
            self._require_clean_candidate()
            _require_ancestor(
                self.project_root,
                str(replay.transaction["lane_head_sha"]),
                current_head,
            )
            _require_ancestor(self.project_root, expected_main, current_head)
            payload["candidate_sha"] = current_head
        if phase == "FULL_DISPATCHED":
            from ai_trading_system.platform.architecture.workflow_execution import (
                current_process_identity,
            )

            if replay.candidate_sha is None or current_head != replay.candidate_sha:
                raise PublicationFenceError(
                    "PUBLICATION_CANDIDATE_DRIFT",
                    f"bound={replay.candidate_sha};observed={current_head}",
                )
            checked_run_id = _identifier(full_run_id or "", "full_run_id")
            claim_body: dict[str, object] = {
                "schema_version": "integration_publication_full_dispatch.v2",
                "transaction_id": replay.transaction["transaction_id"],
                "transaction_sha256": replay.transaction["transaction_sha256"],
                "candidate_sha": replay.candidate_sha,
                "full_run_id": checked_run_id,
                "launcher": current_process_identity(),
                "production_effect": "none",
                "broker_action": "none",
            }
            claim_sha = _json_sha256(claim_body)
            claim = {**claim_body, "claim_sha256": claim_sha}
            claim_path = (
                self.runtime_root
                / "transactions"
                / str(replay.transaction["transaction_id"])
                / "full_dispatch_claim.json"
            )
            if claim_path.exists():
                raise PublicationFenceError("PUBLICATION_FULL_ALREADY_DISPATCHED", "existing claim")
            payload["candidate_sha"] = replay.candidate_sha
            payload["full_run_id"] = checked_run_id
            payload["dispatch_claim"] = {
                "path": claim_path.relative_to(self.project_root).as_posix(),
                "sha256": hashlib.sha256(canonical_json_bytes(claim)).hexdigest(),
            }
            payload["dispatch_request"] = claim
        if phase == "FORMAL_VALIDATION_RESULT":
            if replay.candidate_sha is None or current_head != replay.candidate_sha:
                raise PublicationFenceError(
                    "PUBLICATION_CANDIDATE_DRIFT",
                    f"bound={replay.candidate_sha};observed={current_head}",
                )
            normalized_status = _identifier(validation_status or "", "validation_status").upper()
            if normalized_status not in {"PASS", "FAIL"}:
                raise PublicationFenceError(
                    "PUBLICATION_VALIDATION_STATUS_INVALID",
                    normalized_status,
                )
            payload["candidate_sha"] = replay.candidate_sha
            payload["validation_status"] = normalized_status
            if lease_execution is None:
                raise PublicationFenceError("PUBLICATION_FULL_EXECUTION_REQUIRED", "result")
            payload["execution_result"] = self._full_result_custody(
                replay,
                lease_execution,
                normalized_status,
                payload["evidence"],
            )
            payload["publication_preflight_required"] = True
            payload["expected_main_matches_observation"] = current_main == expected_main
        if phase == "LOCAL_MAIN_FF_PRE":
            from ai_trading_system.platform.architecture.workflow_integration import (
                inspect_local_publication_topology,
            )

            self._require_clean_candidate()
            if replay.candidate_sha is None or current_head != replay.candidate_sha:
                raise PublicationFenceError(
                    "PUBLICATION_CANDIDATE_DRIFT",
                    f"bound={replay.candidate_sha};observed={current_head}",
                )
            if replay.events[-1].get("payload", {}).get("validation_status") != "PASS":
                raise PublicationFenceError(
                    "PUBLICATION_FORMAL_VALIDATION_NOT_PASS",
                    str(replay.events[-1].get("payload", {}).get("validation_status")),
                )
            payload["candidate_sha"] = replay.candidate_sha
            # Persist in the original ordered event, under the existing lease
            # arbiter. No auxiliary journal, phase or caller-provided snapshot.
            topology = inspect_local_publication_topology(
                self.project_root, candidate=replay.candidate_sha, expected_main=expected_main,
            )
            payload["local_publication_intent"] = self._local_publication_intent(replay, topology)
        if phase == "REMOTE_PUSH_PRE":
            self._require_clean_candidate()
            candidate = replay.candidate_sha
            if _git(self.project_root, "branch", "--show-current") != "main":
                raise PublicationFenceError("PUBLICATION_REMOTE_PUSH_REQUIRES_MAIN", current_head)
            if candidate is None or current_main != candidate or current_head != candidate:
                raise PublicationFenceError(
                    "PUBLICATION_LOCAL_MAIN_CANDIDATE_MISMATCH",
                    f"candidate={candidate};head={current_head};main={current_main}",
                )
            if remote_observation is None:
                raise PublicationFenceError("PUBLICATION_REMOTE_OBSERVATION_REQUIRED", phase)
            observation = remote_observation
            remote_main = observation["tip_sha"]
            if not isinstance(remote_main, str):
                raise PublicationFenceError("PUBLICATION_REMOTE_REF_MISSING", "refs/heads/main")
            _require_ancestor(self.project_root, remote_main, candidate)
            payload["candidate_sha"] = candidate
            payload["remote_observation"] = observation
        if phase == "CLEANUP_PRE":
            candidate = replay.candidate_sha
            if candidate is None or current_head != candidate or current_main != candidate:
                raise PublicationFenceError(
                    "PUBLICATION_CLEANUP_CANDIDATE_MISMATCH",
                    f"candidate={candidate};head={current_head};main={current_main}",
                )
            if remote_observation is None:
                raise PublicationFenceError("PUBLICATION_REMOTE_OBSERVATION_REQUIRED", phase)
            observation = remote_observation
            if observation["status"] == "REMOTE_UNKNOWN":
                raise PublicationFenceError(
                    "PUBLICATION_REMOTE_UNKNOWN", "retry read-only remote-observe; no push granted"
                )
            remote_main = observation["tip_sha"]
            if remote_main != candidate:
                raise PublicationFenceError(
                    "PUBLICATION_REMOTE_SHA_MISMATCH",
                    f"candidate={candidate};actual_remote={remote_main}",
                )
            self._require_clean_candidate()
            payload["candidate_sha"] = candidate
            payload["remote_observation"] = observation
        return payload

    def _active_lease(
        self,
        replay: PublicationReplay,
        *,
        now: datetime | None = None,
    ) -> Any:
        lease_id = str(replay.transaction["lease_id"])
        lease_replay = self.guard.replay()
        if lease_replay.status != "PASS":
            raise PublicationFenceError("PUBLICATION_LEASE_REPLAY_INVALID", lease_id)
        lease = {row.lease_id: row for row in lease_replay.lease_heads}.get(lease_id)
        if lease is None or lease.state != "ACTIVE":
            raise PublicationFenceError(
                "PUBLICATION_ACTIVE_LEASE_REQUIRED",
                f"{lease_id}:{getattr(lease, 'state', 'missing')}",
            )
        instant = _aware_utc(now or datetime.now(tz=UTC))
        if lease.expires_at is None or datetime.fromisoformat(lease.expires_at) <= instant:
            raise PublicationFenceError("PUBLICATION_LEASE_EXPIRED", lease_id)
        self._require_publication_lease_intent(replay, lease)
        return lease

    def _require_publication_lease_intent(self, replay: PublicationReplay, lease: Any) -> None:
        """Recover S1's ordinary-capability check without replacing the V3 recovery code."""
        transaction = replay.transaction
        try:
            intent = self.guard.require_mutation_lease(
                lease,
                expected_intent_path=Path(str(transaction["checkout_intent_path"])),
                task_id=str(transaction["task_id"]),
                actor=str(transaction["actor"]),
            )
        except CheckoutGuardError as exc:
            raise PublicationFenceError(exc.code, exc.message) from exc
        if not isinstance(transaction.get("owned_paths"), list) or not isinstance(
            transaction.get("shared_paths"), list
        ):
            raise PublicationFenceError("PUBLICATION_LEASE_INTENT_BINDING", lease.lease_id)
        owned = tuple(sorted(_checked_paths(transaction["owned_paths"])))
        shared = tuple(sorted(_checked_paths(transaction["shared_paths"])))
        if (
            intent.intent_id != "publication-" + str(transaction["transaction_id"])
            or intent.operation_class is not CheckoutOperationClass.SHARED_MUTATION
            or intent.thread_id != transaction["thread_id"]
            or intent.base_commit != transaction["lane_head_sha"]
            or intent.owned_paths != owned
            or intent.shared_paths != shared
            or intent.workspace_identity.to_dict() != transaction["workspace_identity"]
        ):
            raise PublicationFenceError("PUBLICATION_LEASE_INTENT_BINDING", lease.lease_id)

    def _require_dirty_attributed(self, replay: PublicationReplay) -> None:
        audit = self.guard.audit_worktree()
        declared = tuple(
            str(row)
            for row in (
                *replay.transaction["owned_paths"],
                *replay.transaction["shared_paths"],
            )
        )
        unattributed = [
            path
            for path in audit.dirty_paths
            if not any(_paths_overlap(path, allowed) for allowed in declared)
        ]
        if unattributed:
            raise PublicationFenceError(
                "PUBLICATION_DIRTY_UNATTRIBUTED",
                ",".join(unattributed),
            )

    def _require_clean_candidate(self) -> None:
        audit = self.guard.audit_worktree()
        if audit.dirty_paths:
            raise PublicationFenceError(
                "PUBLICATION_CANDIDATE_DIRTY",
                ",".join(audit.dirty_paths),
            )

    def _validate_plan_binding(self, transaction: Mapping[str, Any]) -> None:
        binding = transaction.get("integration_revalidation_plan")
        if binding is None:
            return
        if not isinstance(binding, dict):
            raise PublicationFenceError("PUBLICATION_PLAN_BINDING_INVALID", "not-object")
        path = self.project_root / str(binding.get("path"))
        if not path.is_file() or _sha256_file(path) != binding.get("sha256"):
            raise PublicationFenceError("PUBLICATION_PLAN_TAMPERED", str(path))
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or payload.get("plan_id") != binding.get("id"):
            raise PublicationFenceError("PUBLICATION_PLAN_ID_MISMATCH", str(path))

    def _validate_parent_binding(
        self,
        transaction: Mapping[str, Any],
        parent_path: Path | None,
    ) -> None:
        binding = transaction.get("full_parent")
        if binding is None and parent_path is None:
            return
        if binding is None or parent_path is None:
            raise PublicationFenceError("PUBLICATION_FULL_PARENT_MISMATCH", "missing-side")
        observed = _optional_file_binding(self.project_root, parent_path)
        if observed != binding:
            raise PublicationFenceError("PUBLICATION_FULL_PARENT_MISMATCH", str(parent_path))

    def _require_phase(
        self,
        phase: str,
        *,
        minimum_phase: str | None,
        exact_phase: str | None,
    ) -> None:
        if exact_phase is not None and phase != exact_phase:
            raise PublicationFenceError(
                "PUBLICATION_PHASE_MISMATCH",
                f"observed={phase};required={exact_phase}",
            )
        if minimum_phase is not None:
            if phase not in self.policy.phase_order or minimum_phase not in self.policy.phase_order:
                raise PublicationFenceError(
                    "PUBLICATION_PHASE_UNKNOWN",
                    f"observed={phase};required={minimum_phase}",
                )
            if self.policy.phase_order.index(phase) < self.policy.phase_order.index(minimum_phase):
                raise PublicationFenceError(
                    "PUBLICATION_PHASE_TOO_EARLY",
                    f"observed={phase};minimum={minimum_phase}",
                )

    def _next_phase(self, phase: str) -> str:
        if phase not in self.policy.phase_order:
            raise PublicationFenceError("PUBLICATION_PHASE_UNKNOWN", phase)
        index = self.policy.phase_order.index(phase)
        if index + 1 >= len(self.policy.phase_order):
            raise PublicationFenceError("PUBLICATION_TRANSACTION_TERMINAL", phase)
        return self.policy.phase_order[index + 1]

    def _validate_phase_chain(
        self,
        events: Sequence[Mapping[str, Any]],
        issues: list[str],
    ) -> None:
        expected = "ACQUIRED"
        for index, event in enumerate(events):
            phase = str(event.get("phase"))
            if index == 0:
                if phase != expected:
                    issues.append("PUBLICATION_PHASE_INITIAL_INVALID")
                continue
            prior = str(events[index - 1].get("phase"))
            if phase == "FAILED":
                if index != len(events) - 1:
                    issues.append("PUBLICATION_PHASE_AFTER_TERMINAL")
                continue
            try:
                expected = self._next_phase(prior)
            except PublicationFenceError:
                issues.append("PUBLICATION_PHASE_AFTER_TERMINAL")
                continue
            if phase != expected:
                issues.append(f"PUBLICATION_PHASE_TRANSITION:{prior}->{phase}")

    def _append_event(
        self,
        transaction_path: Path,
        *,
        phase: str,
        actor: str,
        payload: Mapping[str, object],
        occurred_at: datetime,
        allow_terminal: bool = False,
    ) -> None:
        transaction = self._load_transaction(transaction_path)
        event_root = transaction_path.parent / "events"
        event_root.mkdir(parents=True, exist_ok=True)
        existing = sorted(event_root.glob("*.json"))
        previous_id: str | None = None
        if existing:
            previous = json.loads(existing[-1].read_text(encoding="utf-8"))
            previous_id = str(previous["event_id"])
        sequence = len(existing) + 1
        body: dict[str, object] = {
            "schema_version": EVENT_SCHEMA_VERSION,
            "transaction_id": transaction["transaction_id"],
            "transaction_sha256": transaction["transaction_sha256"],
            "sequence": sequence,
            "previous_event_id": previous_id,
            "phase": phase,
            "actor": actor,
            "occurred_at": occurred_at.isoformat(),
            "payload": dict(payload),
            "terminal": allow_terminal,
            "production_effect": "none",
            "broker_action": "none",
        }
        event_id = _json_sha256(body)
        event = {**body, "event_id": event_id}
        _write_json_exclusive(
            event_root / f"{sequence:04d}_{phase.lower()}_{event_id[:12]}.json",
            event,
        )

    @contextmanager
    def _hold_checkpoint_main(self) -> Iterator[None]:
        """Keep the observed main ref stable through the original checkpoint.

        Existing Windows read/directory custody also covers packed refs and
        symbolic-ref targets. No Git lock, journal or second arbiter is created.
        POSIX retains store arbitration and the explicit pre-write value check.
        """
        if os.name != "nt":
            yield
            return
        from ai_trading_system.platform.architecture.workflow_contract import (
            WorkflowContractError,
            bounded_regular_bytes,
            hold_bound_directory,
            hold_bound_read_file,
            portable_path,
        )

        with ExitStack() as custody:
            try:
                common = Path(_git(
                    self.project_root, "rev-parse", "--path-format=absolute", "--git-common-dir"
                ))
                root_info = common.lstat()
                root_identity = (root_info.st_dev, root_info.st_ino)
                held_directories: set[str] = set()

                def retain(relative: str, *, directory: bool = False) -> bytes:
                    relative = portable_path(relative)
                    path = common / relative
                    info = path.lstat()
                    parents = {}
                    for parent in Path(relative).parents:
                        if parent == Path("."):
                            continue
                        metadata = (common / parent).lstat()
                        parents[parent.as_posix()] = (metadata.st_dev, metadata.st_ino)
                    if directory:
                        if relative not in held_directories:
                            custody.enter_context(hold_bound_directory(
                                common, relative, expected_identity=(info.st_dev, info.st_ino),
                                expected_root_identity=root_identity,
                                expected_parent_identities=parents,
                            ))
                            held_directories.add(relative)
                        return b""
                    raw = bounded_regular_bytes(path, expected_identity=(info.st_dev, info.st_ino))
                    custody.enter_context(hold_bound_read_file(
                        common, relative, expected=raw,
                        expected_identity=(info.st_dev, info.st_ino),
                        expected_root_identity=root_identity, expected_parent_identities=parents,
                    ))
                    return raw

                reference = "refs/heads/main"
                seen: set[str] = set()
                while reference not in seen and len(seen) < 16:
                    seen.add(reference)
                    retain(Path(reference).parent.as_posix(), directory=True)
                    log = "logs/" + reference
                    if (common / log).exists():
                        retain(log)
                    if not (common / reference).exists():
                        # A held packed-refs file prevents pack/prune replacement;
                        # the retained ref directory rejects loose-ref installation.
                        retain("packed-refs")
                        break
                    raw = retain(reference)
                    if not raw.startswith(b"ref: "):
                        break
                    reference = portable_path(raw[5:].decode("ascii").strip())
                    if not reference.startswith("refs/"):
                        raise WorkflowContractError("PUBLICATION_MAIN_REF_TARGET")
                else:
                    raise WorkflowContractError("PUBLICATION_MAIN_REF_CYCLE")
            except (WorkflowContractError, OSError, UnicodeError) as exc:
                raise PublicationFenceError("PUBLICATION_MAIN_CUSTODY_INVALID", str(exc)) from exc
            yield

    @contextmanager
    def _hold_transaction(self, transaction: Path | str) -> Iterator[None]:
        """Retain native Windows transaction custody through the serialized transition.

        POSIX retains the existing cooperative store arbitration and the explicit
        pre-write replay check; this is not a POSIX mandatory file-lock claim.
        """
        if os.name != "nt":
            yield
            return
        from ai_trading_system.platform.architecture.workflow_contract import (
            WorkflowContractError,
            bounded_regular_bytes,
            hold_bound_read_file,
        )

        try:
            path = self._transaction_path(transaction)
            relative = path.relative_to(self.project_root)
            info = path.lstat()
            root_info = self.project_root.lstat()
            parents: dict[str, tuple[int, int]] = {}
            for position in range(1, len(relative.parts)):
                parent = Path(*relative.parts[:position])
                parent_info = (self.project_root / parent).lstat()
                parents[parent.as_posix()] = (parent_info.st_dev, parent_info.st_ino)
            raw = bounded_regular_bytes(path, expected_identity=(info.st_dev, info.st_ino))
            with hold_bound_read_file(
                self.project_root, relative.as_posix(), expected=raw,
                expected_identity=(info.st_dev, info.st_ino),
                expected_root_identity=(root_info.st_dev, root_info.st_ino),
                expected_parent_identities=parents,
                allow_parent_updates=True,
            ):
                yield
        except (WorkflowContractError, OSError) as exc:
            raise PublicationFenceError(
                "PUBLICATION_TRANSACTION_CUSTODY_INVALID", str(exc)
            ) from exc

    def _require_unchanged_replay(self, path: Path, admitted: PublicationReplay) -> None:
        current = self.replay(path)
        if current.status != "PASS" or current != admitted:
            raise PublicationFenceError(
                "PUBLICATION_TRANSACTION_CHANGED", "replay changed after admission"
            )

    def _transaction_path(self, transaction: Path | str) -> Path:
        path = Path(transaction)
        resolved = path.resolve() if path.is_absolute() else (self.project_root / path).resolve()
        if resolved.is_dir():
            resolved = resolved / "transaction.json"
        if not resolved.is_relative_to(self.project_root) or not resolved.is_file():
            raise PublicationFenceError("PUBLICATION_TRANSACTION_MISSING", str(resolved))
        return resolved

    def _load_transaction(self, path: Path) -> Mapping[str, Any]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise PublicationFenceError("PUBLICATION_TRANSACTION_UNREADABLE", str(path)) from exc
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != TRANSACTION_SCHEMA_VERSION
        ):
            raise PublicationFenceError("PUBLICATION_TRANSACTION_SCHEMA", str(path))
        if Path(str(payload.get("repository_root"))).resolve() != self.project_root:
            raise PublicationFenceError("PUBLICATION_REPOSITORY_MISMATCH", str(path))
        return payload

    def _validate_immutable_acquire_replay(
        self,
        transaction: Mapping[str, Any],
        *,
        task_id: str,
        change_id: str,
        thread_id: str,
        actor: str,
        frozen_base_sha: str,
        expected_main_sha: str,
        lane_head_sha: str,
        owned_paths: Sequence[str],
        shared_paths: Sequence[str],
        generator_ids: Sequence[str],
        required_validation_tiers: Sequence[str] | None,
        integration_plan_path: Path | None,
        full_parent_path: Path | None,
    ) -> None:
        expected: dict[str, object] = {
            "task_id": task_id,
            "change_id": change_id,
            "thread_id": thread_id,
            "actor": actor,
            "frozen_base_sha": frozen_base_sha,
            "expected_main_sha": expected_main_sha,
            "lane_head_sha": lane_head_sha,
            "owned_paths": sorted(_checked_paths(owned_paths)),
            "shared_paths": sorted(
                _checked_paths(
                    (
                        *shared_paths,
                        self.policy.exclusive_publication_resource,
                        self.policy.exclusive_validation_resource,
                    )
                )
            ),
            "generator_ids": list(generator_ids),
            "required_validation_tiers": sorted(
                required_validation_tiers or self.policy.required_formal_tiers
            ),
            "policy_version": self.policy.policy_version,
            "policy_sha256": self.policy_sha256,
        }
        observed = dict(transaction)
        for set_field in ("owned_paths", "shared_paths", "required_validation_tiers"):
            values = observed.get(set_field)
            if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
                raise PublicationFenceError("PUBLICATION_TRANSACTION_IDENTITY_CONFLICT", set_field)
            observed[set_field] = sorted(values)
        mismatches = [
            f"{key}:{observed.get(key)!r}!={value!r}"
            for key, value in expected.items()
            if observed.get(key) != value
        ]
        # Reject changed locators before opening their bytes. A duplicate request
        # cannot use a new plan/parent path to read content outside its old scope.
        for key, requested_path in (
            ("integration_revalidation_plan", integration_plan_path),
            ("full_parent", full_parent_path),
        ):
            old_binding = transaction.get(key)
            old_path = old_binding.get("path") if isinstance(old_binding, Mapping) else None
            requested_locator = None
            if requested_path is not None:
                resolved = (
                    requested_path.resolve()
                    if requested_path.is_absolute()
                    else (self.project_root / requested_path).resolve()
                )
                if not resolved.is_relative_to(self.project_root):
                    mismatches.append(f"{key}:outside_repository")
                    continue
                requested_locator = resolved.relative_to(self.project_root).as_posix()
            if requested_locator != old_path:
                mismatches.append(f"{key}:locator")
        if mismatches:
            raise PublicationFenceError(
                "PUBLICATION_TRANSACTION_IDENTITY_CONFLICT",
                ";".join(mismatches),
            )
        bindings = {
            "integration_revalidation_plan": _optional_json_binding(
                self.project_root, integration_plan_path, id_field="plan_id"
            ),
            "full_parent": _optional_file_binding(self.project_root, full_parent_path),
        }
        for key, binding in bindings.items():
            if binding != transaction.get(key):
                raise PublicationFenceError("PUBLICATION_TRANSACTION_IDENTITY_CONFLICT", key)

    def _full_result_custody(
        self,
        replay: PublicationReplay,
        execution: Mapping[str, Any],
        status: str,
        evidence: Any,
    ) -> dict[str, object]:
        from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes
        from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

        if execution["state"] != "RESULT_RECORDED" or execution["result"]["artifact"] is None:
            raise PublicationFenceError("PUBLICATION_FULL_CUSTODY_NOT_RECORDED", "execution")
        request, result = execution["request"], execution["result"]
        commitment = execution.get("full_result_commitment")
        if not isinstance(commitment, Mapping):
            raise PublicationFenceError("PUBLICATION_FULL_COMMITMENT_REQUIRED", "execution")
        claim = self._full_claim(replay)
        request_id = _json_sha256(
            {
                "transaction": replay.transaction["transaction_sha256"],
                "full_run_id": claim["full_run_id"],
            }
        )
        if (
            request.get("schema_version") != "workflow_execution_request.v1"
            or request.get("candidate_sha") != replay.candidate_sha
            or request.get("request_id") != request_id
            or request.get("job_name") != "Local\\AITS-DEVX015-full-" + request_id
            or execution.get("launcher") != claim["launcher"]
        ):
            raise PublicationFenceError("PUBLICATION_FULL_EXECUTION_BINDING", "claim")
        artifact = result["artifact"]
        raw = bounded_regular_bytes(Path(artifact["path"]))
        if hashlib.sha256(raw).hexdigest() != artifact["sha256"]:
            raise PublicationFenceError("PUBLICATION_FULL_CUSTODY_CHANGED", "artifact")
        record = load_strict_json_text(raw.decode("utf-8"))
        if commitment.get("sha256") != artifact["sha256"] or commitment.get("record") != record:
            raise PublicationFenceError("PUBLICATION_FULL_COMMITMENT_CHANGED", "artifact")
        expected = {
            "schema_version": "full_execution_result.v1",
            "candidate_sha": replay.candidate_sha,
            "request_id": request.get("request_id"),
            "validation_identity_sha256": request.get("validation_identity_sha256"),
            "status": status,
        }
        if (
            result["status"] != status
            or not isinstance(record, dict)
            or any(record.get(key) != value for key, value in expected.items())
        ):
            raise PublicationFenceError("PUBLICATION_FULL_CUSTODY_BINDING", "result")
        summary = record.get("summary")
        if not isinstance(summary, dict) or not isinstance(summary.get("path"), str):
            raise PublicationFenceError("PUBLICATION_FULL_CUSTODY_BINDING", "summary")
        path = Path(summary["path"])
        if not path.is_relative_to(self.project_root):
            raise PublicationFenceError("PUBLICATION_FULL_CUSTODY_BINDING", "summary scope")
        relative = path.relative_to(self.project_root).as_posix()
        if not any(
            row["path"] == relative and row["sha256"] == summary.get("sha256") for row in evidence
        ):
            raise PublicationFenceError("PUBLICATION_FULL_SUMMARY_CUSTODY_CHANGED", "evidence")
        return dict(artifact)

    def _binding(self, replay: PublicationReplay) -> dict[str, object]:
        if replay.status != "PASS":
            raise PublicationFenceError("PUBLICATION_REPLAY_INVALID", ",".join(replay.issues))
        return {
            "schema_version": TRANSACTION_SCHEMA_VERSION,
            "status": "PASS",
            "transaction_id": replay.transaction["transaction_id"],
            "transaction_path": (
                self.runtime_root
                / "transactions"
                / str(replay.transaction["transaction_id"])
                / "transaction.json"
            )
            .relative_to(self.project_root)
            .as_posix(),
            "transaction_sha256": replay.transaction["transaction_sha256"],
            "task_id": replay.transaction["task_id"],
            "lease_id": replay.transaction["lease_id"],
            "phase": replay.phase,
            "candidate_sha": replay.candidate_sha,
            "expected_main_sha": replay.transaction["expected_main_sha"],
            "policy_version": replay.transaction["policy_version"],
            "production_effect": "none",
            "broker_action": "none",
        }


def load_publication_fence_policy(path: Path) -> PublicationFencePolicy:
    payload = _mapping(safe_load_yaml_path(path), "policy")
    if payload.get("schema_version") != POLICY_SCHEMA_VERSION:
        raise PublicationFenceError("PUBLICATION_POLICY_SCHEMA", str(payload.get("schema_version")))
    if payload.get("status") != "OWNER_APPROVED_ENFORCED":
        raise PublicationFenceError("PUBLICATION_POLICY_STATUS", str(payload.get("status")))
    authority = _mapping(payload.get("authority"), "authority")
    runtime = _mapping(payload.get("runtime"), "runtime")
    generated = _mapping(payload.get("generated_rebuild"), "generated_rebuild")
    validation = _mapping(payload.get("validation"), "validation")
    safety = _mapping(payload.get("safety"), "safety")
    if (
        authority.get("lease_schema") != "execution_lease.v1"
        or authority.get("lease_implementation") != "arch_005_file_execution_lease_store"
    ):
        raise PublicationFenceError("PUBLICATION_LEASE_AUTHORITY_INVALID", str(authority))
    if safety.get("production_effect") != "none" or safety.get("broker_action") != "none":
        raise PublicationFenceError("PUBLICATION_SAFETY_BOUNDARY_INVALID", str(safety))
    for field in (
        "automatic_rebase_allowed",
        "automatic_merge_allowed",
        "automatic_cherry_pick_allowed",
        "force_push_allowed",
        "remote_divergence_repair_allowed",
    ):
        if safety.get(field) is not False:
            raise PublicationFenceError("PUBLICATION_UNSAFE_ACTION_ENABLED", field)
    phase_order = _string_tuple(payload.get("phase_order"), "phase_order")
    if (
        phase_order[0] != "ACQUIRED"
        or phase_order[-1] != "RELEASED"
        or len(set(phase_order)) != len(phase_order)
    ):
        raise PublicationFenceError("PUBLICATION_PHASE_ORDER_INVALID", str(phase_order))
    required_tiers = _string_tuple(validation.get("required_formal_tiers"), "required_formal_tiers")
    heavyweight = _required_text(validation.get("heavyweight_tier"), "heavyweight_tier")
    if heavyweight not in required_tiers:
        raise PublicationFenceError("PUBLICATION_HEAVYWEIGHT_TIER_MISSING", heavyweight)
    return PublicationFencePolicy(
        policy_id=_identifier(payload.get("policy_id"), "policy_id"),
        version=_required_text(payload.get("version"), "version"),
        status="OWNER_APPROVED_ENFORCED",
        owner=_required_text(payload.get("owner"), "owner"),
        approval_ref=_required_text(payload.get("approval_ref"), "approval_ref"),
        checkout_guard_policy=_repo_path(authority.get("checkout_guard_policy")),
        parallel_control_policy=_repo_path(authority.get("parallel_control_policy")),
        transaction_root=_repo_path(runtime.get("transaction_root")),
        exclusive_publication_resource=_repo_path(runtime.get("exclusive_publication_resource")),
        exclusive_validation_resource=_repo_path(runtime.get("exclusive_validation_resource")),
        phase_order=phase_order,
        allowed_generator_ids=_string_tuple(
            generated.get("allowed_generator_ids"), "allowed_generator_ids"
        ),
        require_exact_declared_order=generated.get("require_exact_declared_order") is True,
        required_formal_tiers=required_tiers,
        heavyweight_tier=heavyweight,
        failure_fix_trigger=_identifier(
            validation.get("failure_fix_trigger"), "failure_fix_trigger"
        ),
    )


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise PublicationFenceError("PUBLICATION_POLICY_FIELD", field)
    return value


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(row, str) and row for row in value)
    ):
        raise PublicationFenceError("PUBLICATION_POLICY_FIELD", field)
    return tuple(value)


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PublicationFenceError("PUBLICATION_REQUIRED_FIELD", field)
    return value.strip()


def _identifier(value: object, field: str) -> str:
    text = _required_text(value, field)
    if any(not (char.isalnum() or char in "-_.:@") for char in text):
        raise PublicationFenceError("PUBLICATION_IDENTIFIER_INVALID", f"{field}:{text}")
    return text


def _sha(value: object, field: str) -> str:
    text = _required_text(value, field).lower()
    if len(text) != 40 or any(char not in "0123456789abcdef" for char in text):
        raise PublicationFenceError("PUBLICATION_SHA_INVALID", f"{field}:{text}")
    return text


def _repo_path(value: object) -> str:
    text = _required_text(value, "repo_path").replace("\\", "/")
    candidate = PurePosixPath(text)
    if candidate.is_absolute() or ".." in candidate.parts or text.startswith("./"):
        raise PublicationFenceError("PUBLICATION_REPO_PATH_INVALID", text)
    return candidate.as_posix()


def _checked_paths(paths: Sequence[str]) -> tuple[str, ...]:
    checked = tuple(_repo_path(path) for path in paths)
    if len(set(row.casefold() for row in checked)) != len(checked):
        raise PublicationFenceError("PUBLICATION_PATH_DUPLICATE", ",".join(checked))
    return checked


def _paths_overlap(left: str, right: str) -> bool:
    a = left.casefold().rstrip("/")
    b = right.casefold().rstrip("/")
    return a == b or a.startswith(f"{b}/") or b.startswith(f"{a}/")


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise PublicationFenceError("PUBLICATION_TIMEZONE_REQUIRED", value.isoformat())
    return value.astimezone(UTC)


def _require_remote_confirmation(value: Any, candidate: str | None) -> None:
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != "publication_remote_observation.v1"
        or value.get("status") != "OBSERVED"
        or value.get("source") != "git-ls-remote"
        or value.get("ref") != "refs/heads/main"
        or candidate is None
        or value.get("tip_sha") != candidate
        or value.get("mutation_performed") is not False
    ):
        raise PublicationFenceError("PUBLICATION_TERMINAL_REPLAY_INVALID", "remote_confirmation")


def _push_endpoint(root: Path) -> str:
    endpoints = _git(root, "remote", "get-url", "--push", "--all", "origin").splitlines()
    if len(endpoints) != 1 or not endpoints[0] or endpoints[0].startswith("-"):
        raise PublicationFenceError(
            "PUBLICATION_REMOTE_ENDPOINT_AMBIGUOUS", "one push URL required"
        )
    return endpoints[0]


def _observe_push_remote(
    root: Path, *, expected_endpoint_sha: str | None = None
) -> dict[str, object]:
    endpoint = _push_endpoint(root)
    endpoint_sha = hashlib.sha256(endpoint.encode("utf-8")).hexdigest()
    if expected_endpoint_sha is not None and endpoint_sha != expected_endpoint_sha:
        raise PublicationFenceError("PUBLICATION_REMOTE_ENDPOINT_CHANGED", "push endpoint binding")
    try:
        result = subprocess.run(
            ["git", "ls-remote", "--exit-code", "--refs", endpoint, "refs/heads/main"],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=30,
            env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise PublicationFenceError(
            "PUBLICATION_REMOTE_UNKNOWN", "read-only remote probe unavailable"
        ) from exc
    if result.returncode not in {0, 2}:
        # Do not copy transport stderr/URLs, which may contain credential material.
        raise PublicationFenceError("PUBLICATION_REMOTE_UNKNOWN", "read-only remote probe failed")
    if _push_endpoint(root) != endpoint:
        raise PublicationFenceError("PUBLICATION_REMOTE_ENDPOINT_CHANGED", "probe endpoint drift")
    rows = result.stdout.splitlines()
    tip: str | None = None
    if result.returncode == 0:
        if len(rows) != 1 or len(fields := rows[0].split("\t")) != 2:
            raise PublicationFenceError("PUBLICATION_REMOTE_UNKNOWN", "ambiguous remote response")
        if fields[1] != "refs/heads/main":
            raise PublicationFenceError("PUBLICATION_REMOTE_UNKNOWN", "unexpected remote ref")
        tip = _sha(fields[0], "actual_remote_main")
    elif rows:
        raise PublicationFenceError("PUBLICATION_REMOTE_UNKNOWN", "unexpected missing-ref response")
    return {
        "schema_version": "publication_remote_observation.v1",
        "status": "OBSERVED",
        "remote_name": "origin",
        "endpoint_sha256": endpoint_sha,
        "ref": "refs/heads/main",
        "tip_sha": tip,
        "source": "git-ls-remote",
        "observed_at": datetime.now(tz=UTC).isoformat(),
        "mutation_performed": False,
    }


def _git(root: Path, *args: str) -> str:
    completed = _local_git_result(root, *args)
    if completed.returncode != 0:
        raise PublicationFenceError(
            "PUBLICATION_GIT_COMMAND_FAILED",
            f"git {' '.join(args)}:{(completed.stderr or completed.stdout).strip()}",
        )
    return completed.stdout.strip()


def _local_git_result(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    from ai_trading_system.platform.architecture.source_preservation import inspection_git_result

    protected = inspection_git_result(root, *args)
    if protected is not None:
        return subprocess.CompletedProcess(
            protected.args, protected.returncode,
            protected.stdout.decode("utf-8"), protected.stderr.decode("utf-8"),
        )
    return subprocess.run(
        ["git", *args],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )


def _git_optional(root: Path, *args: str) -> str | None:
    try:
        return _git(root, *args)
    except PublicationFenceError:
        return None


def _require_ancestor(root: Path, ancestor: str, descendant: str) -> None:
    completed = _local_git_result(root, "merge-base", "--is-ancestor", ancestor, descendant)
    if completed.returncode != 0:
        raise PublicationFenceError(
            "PUBLICATION_ANCESTRY_INVALID",
            f"{ancestor}!<={descendant}",
        )


def _json_sha256(payload: Mapping[str, object]) -> str:
    raw = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json_exclusive(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = canonical_json_bytes(payload)
    # Exclusive creation is the append-only CAS. Atomic replace is intentionally
    # inapplicable because an existing transaction/event must never be replaced.
    with path.open("xb") as stream:
        stream.write(encoded)


def _optional_json_binding(
    project_root: Path,
    path: Path | None,
    *,
    id_field: str,
) -> dict[str, object] | None:
    binding = _optional_file_binding(project_root, path)
    if binding is None or path is None:
        return None
    resolved = path.resolve() if path.is_absolute() else (project_root / path).resolve()
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get(id_field), str):
        raise PublicationFenceError("PUBLICATION_PLAN_ID_MISSING", str(resolved))
    return {**binding, "id": payload[id_field]}


def _optional_file_binding(project_root: Path, path: Path | None) -> dict[str, object] | None:
    if path is None:
        return None
    resolved = path.resolve() if path.is_absolute() else (project_root / path).resolve()
    if not resolved.is_relative_to(project_root) or not resolved.is_file():
        raise PublicationFenceError("PUBLICATION_EVIDENCE_FILE_MISSING", str(resolved))
    return {
        "path": resolved.relative_to(project_root).as_posix(),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _artifact_bindings(project_root: Path, paths: Sequence[Path]) -> list[dict[str, object]]:
    return [
        binding
        for path in paths
        if (binding := _optional_file_binding(project_root, path)) is not None
    ]
