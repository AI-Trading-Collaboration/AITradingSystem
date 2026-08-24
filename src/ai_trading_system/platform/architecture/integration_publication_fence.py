from __future__ import annotations

import hashlib
import json
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutGuardError,
    CheckoutLeaseGuard,
    CheckoutOperationClass,
)
from ai_trading_system.platform.artifacts import canonical_json_bytes, write_json_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_SCHEMA_VERSION = "arch_005_integration_publication_fence_policy.v1"
TRANSACTION_SCHEMA_VERSION = "integration_publication_fence.v1"
EVENT_SCHEMA_VERSION = "integration_publication_fence_event.v1"
REPLAY_SCHEMA_VERSION = "integration_publication_fence_replay.v1"
RECEIPT_SCHEMA_VERSION = "integration_publication_closeout_receipt.v1"
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "architecture" / "arch_005_integration_publication_fence.yaml"
)


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
            existing = self._load_transaction(transaction_path)
            self._validate_immutable_acquire_replay(
                existing,
                task_id=task_id,
                change_id=change_id,
                expected_main_sha=expected_main_sha,
                lane_head_sha=lane_head_sha,
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
        try:
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
        except BaseException:
            handle.release(outcome="failed", at=instant)
            raise
        return self._binding(self.replay(transaction_path))

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
        if replay.phase not in {"REMOTE_PUSH_PRE", "CLEANUP_PRE"} and current_main != expected_main:
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
                validation_resource = self.policy.exclusive_validation_resource.casefold()
                resources = {
                    str(row.get("resource_id", "")).casefold()
                    for row in lease.to_dict()["resources"]
                    if isinstance(row, dict)
                }
                if validation_resource not in resources:
                    raise PublicationFenceError(
                        "PUBLICATION_FULL_RESOURCE_MISSING",
                        self.policy.exclusive_validation_resource,
                    )
        if validation_tier == self.policy.heavyweight_tier or parent_path is not None:
            self._validate_parent_binding(replay.transaction, parent_path)
        return self._binding(replay)

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
    ) -> dict[str, object]:
        instant = _aware_utc(now or datetime.now(tz=UTC))
        transaction_path = self._transaction_path(transaction)
        replay = self.replay(transaction_path)
        if replay.status != "PASS":
            raise PublicationFenceError(
                "PUBLICATION_REPLAY_INVALID",
                ",".join(replay.issues),
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
        self._active_lease(replay, now=instant)
        self.guard.store.heartbeat(
            str(replay.transaction["lease_id"]),
            actor=actor,
            now=instant,
        )
        payload = self._checkpoint_payload(
            replay,
            phase=phase,
            evidence_paths=evidence_paths,
            generator_ids=generator_ids,
            full_run_id=full_run_id,
            validation_status=validation_status,
        )
        self._append_event(
            transaction_path,
            phase=phase,
            actor=actor,
            payload=payload,
            occurred_at=instant,
        )
        return self._binding(self.replay(transaction_path))

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
        normalized_outcome = _identifier(outcome, "outcome").upper()
        if normalized_outcome not in {"COMPLETED", "FAILED"}:
            raise PublicationFenceError("PUBLICATION_OUTCOME_INVALID", normalized_outcome)
        terminal_phase = "RELEASED" if normalized_outcome == "COMPLETED" else "FAILED"
        receipt_path = transaction_path.parent / "closeout_receipt.json"
        if replay.phase in {"RELEASED", "FAILED"}:
            if replay.phase != terminal_phase or not receipt_path.is_file():
                raise PublicationFenceError("PUBLICATION_TERMINAL_REPLAY_INVALID", replay.phase)
            return json.loads(receipt_path.read_text(encoding="utf-8"))
        if replay.transaction.get("actor") != actor:
            raise PublicationFenceError("PUBLICATION_ACTOR_MISMATCH", actor)
        if normalized_outcome == "COMPLETED" and replay.phase != "CLEANUP_PRE":
            raise PublicationFenceError(
                "PUBLICATION_CLOSEOUT_PHASE_REQUIRED",
                replay.phase,
            )
        evidence = _artifact_bindings(self.project_root, evidence_paths)
        lease_id = str(replay.transaction["lease_id"])
        lease_heads = {row.lease_id: row for row in self.guard.replay().lease_heads}
        lease = lease_heads.get(lease_id)
        if lease is None:
            raise PublicationFenceError("PUBLICATION_LEASE_UNKNOWN", lease_id)
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
        receipt: dict[str, object] = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "status": "PASS" if normalized_outcome == "COMPLETED" else "FAIL",
            "outcome": normalized_outcome,
            "transaction_id": replay.transaction["transaction_id"],
            "transaction_sha256": replay.transaction["transaction_sha256"],
            "lease_id": lease_id,
            "lease_state": "RELEASED",
            "candidate_sha": terminal.candidate_sha,
            "final_phase": terminal.phase,
            "head_event_id": terminal.events[-1]["event_id"],
            "evidence": evidence,
            "completed_at": instant.isoformat(),
            "production_effect": "none",
            "broker_action": "none",
        }
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
                "FULL_DISPATCHED",
                "FORMAL_VALIDATION_RESULT",
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
            if replay.candidate_sha is None or current_head != replay.candidate_sha:
                raise PublicationFenceError(
                    "PUBLICATION_CANDIDATE_DRIFT",
                    f"bound={replay.candidate_sha};observed={current_head}",
                )
            checked_run_id = _identifier(full_run_id or "", "full_run_id")
            claim_body: dict[str, object] = {
                "schema_version": "integration_publication_full_dispatch.v1",
                "transaction_id": replay.transaction["transaction_id"],
                "transaction_sha256": replay.transaction["transaction_sha256"],
                "candidate_sha": replay.candidate_sha,
                "full_run_id": checked_run_id,
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
            try:
                _write_json_exclusive(claim_path, claim)
            except FileExistsError:
                existing = json.loads(claim_path.read_text(encoding="utf-8"))
                if existing != claim:
                    raise PublicationFenceError(
                        "PUBLICATION_FULL_ALREADY_DISPATCHED",
                        str(existing.get("full_run_id")),
                    ) from None
            payload["candidate_sha"] = replay.candidate_sha
            payload["full_run_id"] = checked_run_id
            payload["dispatch_claim"] = {
                "path": claim_path.relative_to(self.project_root).as_posix(),
                "sha256": _sha256_file(claim_path),
            }
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
        if phase == "LOCAL_MAIN_FF_PRE":
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
        if phase == "REMOTE_PUSH_PRE":
            candidate = replay.candidate_sha
            if _git(self.project_root, "branch", "--show-current") != "main":
                raise PublicationFenceError("PUBLICATION_REMOTE_PUSH_REQUIRES_MAIN", current_head)
            if candidate is None or current_main != candidate or current_head != candidate:
                raise PublicationFenceError(
                    "PUBLICATION_LOCAL_MAIN_CANDIDATE_MISMATCH",
                    f"candidate={candidate};head={current_head};main={current_main}",
                )
            origin_main = _git(self.project_root, "rev-parse", "origin/main")
            _require_ancestor(self.project_root, origin_main, candidate)
            payload["candidate_sha"] = candidate
            payload["observed_origin_main"] = origin_main
        if phase == "CLEANUP_PRE":
            candidate = replay.candidate_sha
            origin_main = _git(self.project_root, "rev-parse", "origin/main")
            if candidate is None or current_head != candidate or current_main != candidate:
                raise PublicationFenceError(
                    "PUBLICATION_CLEANUP_CANDIDATE_MISMATCH",
                    f"candidate={candidate};head={current_head};main={current_main}",
                )
            if origin_main != candidate:
                raise PublicationFenceError(
                    "PUBLICATION_REMOTE_SHA_MISMATCH",
                    f"candidate={candidate};origin={origin_main}",
                )
            self._require_clean_candidate()
            payload["candidate_sha"] = candidate
            payload["observed_origin_main"] = origin_main
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
        return lease

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
        expected_main_sha: str,
        lane_head_sha: str,
    ) -> None:
        expected = {
            "task_id": task_id,
            "change_id": change_id,
            "expected_main_sha": expected_main_sha,
            "lane_head_sha": lane_head_sha,
        }
        mismatches = [
            f"{key}:{transaction.get(key)!r}!={value!r}"
            for key, value in expected.items()
            if transaction.get(key) != value
        ]
        if mismatches:
            raise PublicationFenceError(
                "PUBLICATION_TRANSACTION_IDENTITY_CONFLICT",
                ";".join(mismatches),
            )

    def _binding(self, replay: PublicationReplay) -> dict[str, object]:
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


def _git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode != 0:
        raise PublicationFenceError(
            "PUBLICATION_GIT_COMMAND_FAILED",
            f"git {' '.join(args)}:{(completed.stderr or completed.stdout).strip()}",
        )
    return completed.stdout.strip()


def _git_optional(root: Path, *args: str) -> str | None:
    try:
        return _git(root, *args)
    except PublicationFenceError:
        return None


def _require_ancestor(root: Path, ancestor: str, descendant: str) -> None:
    completed = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
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
