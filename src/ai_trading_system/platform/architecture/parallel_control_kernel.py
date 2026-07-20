from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any

from ai_trading_system.platform.architecture.parallel_control import (
    ChangeManifest,
    ContractAccess,
    ControlIssue,
    ParallelControlError,
)
from ai_trading_system.platform.artifacts import write_json_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_SCHEMA_VERSION = "arch_005_parallel_control_policy.v1"
DEPENDENCY_SCHEMA_VERSION = "task_dependency.v1"
LEASE_SCHEMA_VERSION = "execution_lease.v1"
LEASE_EVENT_SCHEMA_VERSION = "execution_lease_event.v1"
READINESS_SCHEMA_VERSION = "task_readiness_decision.v1"
LEASE_REPLAY_SCHEMA_VERSION = "execution_lease_replay.v1"

_HARD_DEPENDENCY_TYPES = frozenset({"blocks_start", "blocks_completion"})
_DEPENDENCY_TYPES = _HARD_DEPENDENCY_TYPES | frozenset({"parent_child", "informational"})
_LEASE_TRANSITIONS: dict[str | None, frozenset[str]] = {
    None: frozenset({"REQUESTED"}),
    "REQUESTED": frozenset({"ACTIVE", "BLOCKED"}),
    "ACTIVE": frozenset({"RELEASED", "EXPIRED"}),
    "EXPIRED": frozenset({"REASSIGNED"}),
    "RELEASED": frozenset(),
    "REASSIGNED": frozenset(),
    "BLOCKED": frozenset(),
}


class ResourceAccess(StrEnum):
    READ = "READ"
    WRITE = "WRITE"


@dataclass(frozen=True)
class ParallelControlPolicy:
    policy_id: str
    version: str
    status: str
    eligible_governance_statuses: tuple[str, ...]
    hard_dependency_types: tuple[str, ...]
    require_requirement_refs: bool
    require_acceptance_criteria: bool
    max_parallel_domain_lanes: int
    max_total_active_leases: int
    priority_order: tuple[str, ...]
    tie_breakers: tuple[str, ...]
    fairness_mode: str
    lease_ttl_seconds: int
    max_reassignments: int
    arbiter_ttl_seconds: int
    require_exact_base_commit: bool
    require_manifest_hash_binding: bool
    allowlisted_task_ids: tuple[str, ...]
    allowlisted_actors: tuple[str, ...]
    failure_injection_change_id: str
    source_of_truth: str

    @property
    def policy_version(self) -> str:
        return f"{self.policy_id}@{self.version}"


@dataclass(frozen=True, order=True)
class TaskDependency:
    dependency_id: str
    task_id: str
    depends_on_task_id: str
    edge_type: str
    required_statuses: tuple[str, ...]
    rationale: str
    owner: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": DEPENDENCY_SCHEMA_VERSION,
            "dependency_id": self.dependency_id,
            "task_id": self.task_id,
            "depends_on_task_id": self.depends_on_task_id,
            "edge_type": self.edge_type,
            "required_statuses": list(self.required_statuses),
            "rationale": self.rationale,
            "owner": self.owner,
        }


@dataclass(frozen=True)
class TaskControlRecord:
    task_id: str
    title: str
    governance_status: str
    priority: str
    requirement_refs: tuple[str, ...]
    acceptance_criteria: tuple[str, ...]
    manifest: ChangeManifest

    def to_dict(self) -> dict[str, object]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "governance_status": self.governance_status,
            "priority": self.priority,
            "requirement_refs": list(self.requirement_refs),
            "acceptance_criteria": list(self.acceptance_criteria),
            "change_manifest": self.manifest.to_dict(),
        }


@dataclass(frozen=True)
class DependencyGraphReport:
    status: str
    task_ids: tuple[str, ...]
    dependency_ids: tuple[str, ...]
    topological_order: tuple[str, ...]
    issues: tuple[ControlIssue, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "task_dependency_graph_validation.v1",
            "status": self.status,
            "task_ids": list(self.task_ids),
            "dependency_ids": list(self.dependency_ids),
            "topological_order": list(self.topological_order),
            "issues": [issue.to_dict() for issue in self.issues],
            "production_effect": "none",
        }


@dataclass(frozen=True)
class ReadinessDecision:
    task_id: str
    change_id: str
    status: str
    reason_codes: tuple[str, ...]
    dependency_checks: tuple[dict[str, object], ...]
    manifest_sha256: str
    policy_version: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": READINESS_SCHEMA_VERSION,
            "task_id": self.task_id,
            "change_id": self.change_id,
            "status": self.status,
            "reason_codes": list(self.reason_codes),
            "dependency_checks": list(self.dependency_checks),
            "manifest_sha256": self.manifest_sha256,
            "policy_version": self.policy_version,
            "dispatch_allowed": False,
            "lease_acquisition_allowed": False,
            "task_governance_status_mutated": False,
            "production_effect": "none",
        }


@dataclass(frozen=True, order=True)
class ResourceClaim:
    kind: str
    resource_id: str
    access: ResourceAccess

    def to_dict(self) -> dict[str, str]:
        return {
            "kind": self.kind,
            "resource_id": self.resource_id,
            "access": self.access.value,
        }


@dataclass(frozen=True)
class ExecutionLease:
    lease_id: str
    task_id: str
    change_id: str
    lane_id: str
    actor: str
    base_commit: str
    change_manifest_sha256: str
    policy_version: str
    generation: int
    previous_lease_id: str | None
    state: str
    requested_at: str
    acquired_at: str | None
    expires_at: str | None
    resources: tuple[ResourceClaim, ...]
    evidence_refs: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": LEASE_SCHEMA_VERSION,
            "lease_id": self.lease_id,
            "task_id": self.task_id,
            "change_id": self.change_id,
            "lane_id": self.lane_id,
            "actor": self.actor,
            "base_commit": self.base_commit,
            "change_manifest_sha256": self.change_manifest_sha256,
            "policy_version": self.policy_version,
            "generation": self.generation,
            "previous_lease_id": self.previous_lease_id,
            "state": self.state,
            "requested_at": self.requested_at,
            "acquired_at": self.acquired_at,
            "expires_at": self.expires_at,
            "resources": [claim.to_dict() for claim in self.resources],
            "evidence_refs": list(self.evidence_refs),
            "production_effect": "none",
            "broker_action": "none",
        }


@dataclass(frozen=True)
class LeaseEvent:
    event_id: str
    lease: ExecutionLease
    previous_event_id: str | None
    from_state: str | None
    to_state: str
    occurred_at: str
    actor: str
    reason_codes: tuple[str, ...]

    def _body(self) -> dict[str, object]:
        return {
            "schema_version": LEASE_EVENT_SCHEMA_VERSION,
            "lease": self.lease.to_dict(),
            "previous_event_id": self.previous_event_id,
            "from_state": self.from_state,
            "to_state": self.to_state,
            "occurred_at": self.occurred_at,
            "actor": self.actor,
            "reason_codes": list(self.reason_codes),
            "task_governance_status_mutated": False,
            "production_effect": "none",
        }

    def to_dict(self) -> dict[str, object]:
        return {"event_id": self.event_id, **self._body()}


@dataclass(frozen=True)
class LeaseReplay:
    status: str
    lease_heads: tuple[ExecutionLease, ...]
    active_leases: tuple[ExecutionLease, ...]
    head_event_ids: tuple[tuple[str, str], ...]
    event_count: int
    issues: tuple[ControlIssue, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": LEASE_REPLAY_SCHEMA_VERSION,
            "status": self.status,
            "lease_heads": [lease.to_dict() for lease in self.lease_heads],
            "active_leases": [lease.to_dict() for lease in self.active_leases],
            "head_event_ids": [
                {"lease_id": lease_id, "event_id": event_id}
                for lease_id, event_id in self.head_event_ids
            ],
            "event_count": self.event_count,
            "issues": [issue.to_dict() for issue in self.issues],
            "task_governance_status_mutated": False,
            "production_effect": "none",
        }


@dataclass(frozen=True)
class LeaseAcquisition:
    status: str
    lease: ExecutionLease
    reason_codes: tuple[str, ...]
    idempotent_replay: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "execution_lease_acquisition.v1",
            "status": self.status,
            "lease": self.lease.to_dict(),
            "reason_codes": list(self.reason_codes),
            "idempotent_replay": self.idempotent_replay,
            "task_governance_status_mutated": False,
            "production_effect": "none",
        }


def load_parallel_control_policy(path: Path) -> ParallelControlPolicy:
    payload = _mapping(safe_load_yaml_path(path), "policy")
    if payload.get("schema_version") != POLICY_SCHEMA_VERSION:
        raise ParallelControlError("CONTROL_POLICY_SCHEMA", str(payload.get("schema_version")))
    readiness = _mapping(payload.get("readiness"), "readiness")
    scheduler = _mapping(payload.get("scheduler"), "scheduler")
    lease = _mapping(payload.get("lease"), "lease")
    s4 = _mapping(payload.get("s4"), "s4")
    safety = _mapping(payload.get("safety"), "safety")
    if payload.get("status") != "REVIEWED_PILOT_BASELINE":
        raise ParallelControlError("CONTROL_POLICY_STATUS", str(payload.get("status")))
    if safety.get("production_effect") != "none" or safety.get("broker_action") != "none":
        raise ParallelControlError("CONTROL_POLICY_SAFETY", "production and broker must be none")
    if safety.get("source_of_truth") != "LEGACY_MARKDOWN_ONLY":
        raise ParallelControlError(
            "CONTROL_POLICY_SOURCE_OF_TRUTH", str(safety.get("source_of_truth"))
        )
    if any(
        safety.get(field) is not False
        for field in (
            "task_governance_status_mutation_allowed",
            "generated_task_view_write_allowed",
            "strategy_logic_change_allowed",
            "strategy_threshold_change_allowed",
            "paper_shadow_change_allowed",
        )
    ):
        raise ParallelControlError(
            "CONTROL_POLICY_UNSAFE_PERMISSION", "pilot safety flags must be false"
        )
    hard_types = _strings(readiness.get("hard_dependency_types"), "hard_dependency_types")
    if not set(hard_types).issubset(_HARD_DEPENDENCY_TYPES):
        raise ParallelControlError("CONTROL_POLICY_DEPENDENCY_TYPE", ",".join(hard_types))
    priority_order = _strings(scheduler.get("priority_order"), "priority_order")
    if set(priority_order) != {"P0", "P1", "P2", "P3"}:
        raise ParallelControlError("CONTROL_POLICY_PRIORITY_ORDER", ",".join(priority_order))
    return ParallelControlPolicy(
        policy_id=_text(payload.get("policy_id"), "policy_id"),
        version=_text(payload.get("version"), "version"),
        status=str(payload.get("status")),
        eligible_governance_statuses=_strings(
            readiness.get("eligible_governance_statuses"),
            "eligible_governance_statuses",
        ),
        hard_dependency_types=hard_types,
        require_requirement_refs=_boolean(
            readiness.get("require_requirement_refs"), "require_requirement_refs"
        ),
        require_acceptance_criteria=_boolean(
            readiness.get("require_acceptance_criteria"), "require_acceptance_criteria"
        ),
        max_parallel_domain_lanes=_positive_int(
            scheduler.get("max_parallel_domain_lanes"), "max_parallel_domain_lanes"
        ),
        max_total_active_leases=_positive_int(
            scheduler.get("max_total_active_leases"), "max_total_active_leases"
        ),
        priority_order=priority_order,
        tie_breakers=_strings(scheduler.get("tie_breakers"), "tie_breakers"),
        fairness_mode=_text(scheduler.get("fairness_mode"), "fairness_mode"),
        lease_ttl_seconds=_positive_int(lease.get("ttl_seconds"), "ttl_seconds"),
        max_reassignments=_non_negative_int(lease.get("max_reassignments"), "max_reassignments"),
        arbiter_ttl_seconds=_positive_int(lease.get("arbiter_ttl_seconds"), "arbiter_ttl_seconds"),
        require_exact_base_commit=_boolean(
            lease.get("require_exact_base_commit"), "require_exact_base_commit"
        ),
        require_manifest_hash_binding=_boolean(
            lease.get("require_manifest_hash_binding"), "require_manifest_hash_binding"
        ),
        allowlisted_task_ids=_strings(s4.get("allowlisted_task_ids"), "allowlisted_task_ids"),
        allowlisted_actors=_strings(s4.get("allowlisted_actors"), "allowlisted_actors"),
        failure_injection_change_id=_text(
            s4.get("failure_injection_change_id"), "failure_injection_change_id"
        ),
        source_of_truth=str(safety.get("source_of_truth")),
    )


def parse_task_dependency(payload: Mapping[str, Any]) -> TaskDependency:
    expected = {
        "dependency_id",
        "task_id",
        "depends_on_task_id",
        "edge_type",
        "required_statuses",
        "rationale",
        "owner",
    }
    if set(payload) != expected:
        raise ParallelControlError(
            "DEPENDENCY_FIELDS",
            f"expected={sorted(expected)} actual={sorted(payload)}",
        )
    edge_type = _text(payload.get("edge_type"), "edge_type")
    if edge_type not in _DEPENDENCY_TYPES:
        raise ParallelControlError("DEPENDENCY_EDGE_TYPE", edge_type)
    required = _strings(payload.get("required_statuses"), "required_statuses")
    if edge_type in _HARD_DEPENDENCY_TYPES and not required:
        raise ParallelControlError("DEPENDENCY_REQUIRED_STATUSES", edge_type)
    return TaskDependency(
        dependency_id=_text(payload.get("dependency_id"), "dependency_id"),
        task_id=_text(payload.get("task_id"), "task_id"),
        depends_on_task_id=_text(payload.get("depends_on_task_id"), "depends_on_task_id"),
        edge_type=edge_type,
        required_statuses=tuple(sorted(required)),
        rationale=_text(payload.get("rationale"), "rationale"),
        owner=_text(payload.get("owner"), "owner"),
    )


def validate_dependency_graph(
    task_ids: Sequence[str], dependencies: Sequence[TaskDependency]
) -> DependencyGraphReport:
    ordered_tasks = tuple(sorted(task_ids))
    task_set = set(ordered_tasks)
    issues: set[ControlIssue] = set()
    if len(ordered_tasks) != len(task_set):
        issues.add(_issue("DUPLICATE_TASK_ID", (), "task_ids", "task ids must be unique"))
    dependency_ids = [item.dependency_id for item in dependencies]
    if len(dependency_ids) != len(set(dependency_ids)):
        issues.add(
            _issue("DUPLICATE_DEPENDENCY_ID", (), "dependencies", "dependency ids must be unique")
        )
    graph: dict[str, set[str]] = {task_id: set() for task_id in ordered_tasks}
    indegree = {task_id: 0 for task_id in ordered_tasks}
    for edge in sorted(dependencies):
        if edge.task_id not in task_set:
            issues.add(
                _issue(
                    "DEPENDENCY_TASK_UNKNOWN", (edge.task_id,), edge.dependency_id, "unknown task"
                )
            )
            continue
        if edge.depends_on_task_id not in task_set:
            issues.add(
                _issue(
                    "DEPENDENCY_TARGET_UNKNOWN",
                    (edge.task_id, edge.depends_on_task_id),
                    edge.dependency_id,
                    "unknown dependency target",
                )
            )
            continue
        if edge.task_id == edge.depends_on_task_id:
            issues.add(
                _issue(
                    "DEPENDENCY_SELF_EDGE", (edge.task_id,), edge.dependency_id, "self dependency"
                )
            )
            continue
        if (
            edge.edge_type in _HARD_DEPENDENCY_TYPES
            and edge.task_id not in graph[edge.depends_on_task_id]
        ):
            graph[edge.depends_on_task_id].add(edge.task_id)
            indegree[edge.task_id] += 1
    ready = sorted(task_id for task_id, count in indegree.items() if count == 0)
    topological: list[str] = []
    while ready:
        current = ready.pop(0)
        topological.append(current)
        for successor in sorted(graph[current]):
            indegree[successor] -= 1
            if indegree[successor] == 0:
                ready.append(successor)
                ready.sort()
    cyclic = sorted(task_id for task_id, count in indegree.items() if count > 0)
    if cyclic:
        issues.add(
            _issue(
                "DEPENDENCY_HARD_CYCLE",
                tuple(cyclic),
                ",".join(cyclic),
                "hard dependency graph contains a cycle",
            )
        )
    ordered_issues = tuple(sorted(issues))
    return DependencyGraphReport(
        status="PASS" if not ordered_issues else "FAIL",
        task_ids=ordered_tasks,
        dependency_ids=tuple(sorted(dependency_ids)),
        topological_order=tuple(topological) if not ordered_issues else (),
        issues=ordered_issues,
    )


def evaluate_task_readiness(
    task: TaskControlRecord,
    *,
    dependencies: Sequence[TaskDependency],
    observed_statuses: Mapping[str, str],
    graph_report: DependencyGraphReport,
    current_base_commit: str,
    policy: ParallelControlPolicy,
) -> ReadinessDecision:
    reasons: list[str] = []
    checks: list[dict[str, object]] = []
    if graph_report.status != "PASS":
        reasons.append("DEPENDENCY_GRAPH_INVALID")
    if task.governance_status not in policy.eligible_governance_statuses:
        reasons.append("GOVERNANCE_STATUS_NOT_ELIGIBLE")
    if policy.require_requirement_refs and not task.requirement_refs:
        reasons.append("REQUIREMENT_REFS_MISSING")
    if policy.require_acceptance_criteria and not task.acceptance_criteria:
        reasons.append("ACCEPTANCE_CRITERIA_MISSING")
    if task.manifest.production_effect != "none":
        reasons.append("UNSAFE_PRODUCTION_EFFECT")
    if policy.require_exact_base_commit and task.manifest.base_commit != current_base_commit:
        reasons.append("BASE_DRIFT")
    for edge in sorted(item for item in dependencies if item.task_id == task.task_id):
        observed = observed_statuses.get(edge.depends_on_task_id)
        satisfied = (
            edge.edge_type not in policy.hard_dependency_types or observed in edge.required_statuses
        )
        checks.append(
            {
                "dependency_id": edge.dependency_id,
                "depends_on_task_id": edge.depends_on_task_id,
                "edge_type": edge.edge_type,
                "required_statuses": list(edge.required_statuses),
                "observed_status": observed,
                "satisfied": satisfied,
            }
        )
        if not satisfied:
            reasons.append(f"DEPENDENCY_UNSATISFIED:{edge.dependency_id}")
    if not reasons:
        reasons.append("READY_ALL_GATES_PASS")
    return ReadinessDecision(
        task_id=task.task_id,
        change_id=task.manifest.change_id,
        status="READY" if reasons == ["READY_ALL_GATES_PASS"] else "BLOCKED",
        reason_codes=tuple(sorted(reasons)),
        dependency_checks=tuple(checks),
        manifest_sha256=task.manifest.sha256,
        policy_version=policy.policy_version,
    )


def manifest_resource_claims(manifest: ChangeManifest) -> tuple[ResourceClaim, ...]:
    claims = {
        *(ResourceClaim("path", path, ResourceAccess.WRITE) for path in manifest.owned_paths),
        *(ResourceClaim("path", path, ResourceAccess.WRITE) for path in manifest.shared_paths),
        *(ResourceClaim("module", item, ResourceAccess.WRITE) for item in manifest.module_ids),
        *(
            ResourceClaim(
                "contract",
                claim.contract_id,
                (
                    ResourceAccess.WRITE
                    if claim.access is ContractAccess.WRITE
                    else ResourceAccess.READ
                ),
            )
            for claim in manifest.contract_claims
        ),
    }
    return tuple(sorted(claims))


def leases_conflict(first: ExecutionLease, second: ExecutionLease) -> bool:
    left = {(claim.kind, claim.resource_id): claim.access for claim in first.resources}
    right = {(claim.kind, claim.resource_id): claim.access for claim in second.resources}
    for key in set(left) & set(right):
        if ResourceAccess.WRITE in {left[key], right[key]}:
            return True
    return False


class FileExecutionLeaseStore:
    def __init__(self, root: Path, *, policy: ParallelControlPolicy) -> None:
        self.root = root.resolve()
        self.policy = policy
        self.events_root = self.root / "events"
        self.arbiter_root = self.root / "arbiter.lock"

    def replay(self) -> LeaseReplay:
        events: list[LeaseEvent] = []
        issues: set[ControlIssue] = set()
        if self.events_root.exists():
            for path in sorted(self.events_root.glob("*/*.json")):
                try:
                    events.append(_parse_lease_event(json.loads(path.read_text(encoding="utf-8"))))
                except (OSError, json.JSONDecodeError, ParallelControlError) as exc:
                    issues.add(_issue("LEASE_EVENT_INVALID", (), path.as_posix(), str(exc)))
        by_lease: dict[str, list[LeaseEvent]] = {}
        for event in events:
            by_lease.setdefault(event.lease.lease_id, []).append(event)
        heads: list[ExecutionLease] = []
        head_ids: list[tuple[str, str]] = []
        for lease_id, records in sorted(by_lease.items()):
            event_by_id = {record.event_id: record for record in records}
            if len(event_by_id) != len(records):
                issues.add(_issue("LEASE_EVENT_ID_DUPLICATE", (), lease_id, "duplicate event id"))
                continue
            children = {record.previous_event_id for record in records if record.previous_event_id}
            candidates = sorted(set(event_by_id) - children)
            if len(candidates) != 1:
                issues.add(_issue("LEASE_CAUSAL_HEAD_COUNT", (), lease_id, str(len(candidates))))
                continue
            head = event_by_id[candidates[0]]
            chain: list[LeaseEvent] = []
            seen: set[str] = set()
            current: LeaseEvent | None = head
            while current is not None:
                if current.event_id in seen:
                    issues.add(_issue("LEASE_CAUSAL_CYCLE", (), lease_id, current.event_id))
                    break
                seen.add(current.event_id)
                chain.append(current)
                current = (
                    event_by_id.get(current.previous_event_id)
                    if current.previous_event_id is not None
                    else None
                )
            if len(seen) != len(records):
                issues.add(
                    _issue("LEASE_CAUSAL_DISCONNECTED", (), lease_id, "event chain incomplete")
                )
                continue
            prior_state: str | None = None
            valid = True
            for record in reversed(chain):
                if (
                    record.from_state != prior_state
                    or record.to_state not in _LEASE_TRANSITIONS[prior_state]
                ):
                    issues.add(
                        _issue(
                            "LEASE_TRANSITION_INVALID",
                            (record.lease.task_id,),
                            lease_id,
                            f"{record.from_state}->{record.to_state}",
                        )
                    )
                    valid = False
                    break
                if record.lease.state != record.to_state:
                    issues.add(_issue("LEASE_EVENT_STATE_MISMATCH", (), lease_id, record.event_id))
                    valid = False
                    break
                prior_state = record.to_state
            if valid:
                heads.append(head.lease)
                head_ids.append((lease_id, head.event_id))
        active = sorted(
            (lease for lease in heads if lease.state == "ACTIVE"), key=lambda x: x.lease_id
        )
        for index, first in enumerate(active):
            for second in active[index + 1 :]:
                if leases_conflict(first, second):
                    issues.add(
                        _issue(
                            "ACTIVE_LEASE_RESOURCE_CONFLICT",
                            (first.task_id, second.task_id),
                            f"{first.lease_id},{second.lease_id}",
                            "active leases overlap",
                        )
                    )
        ordered_issues = tuple(sorted(issues))
        return LeaseReplay(
            status="PASS" if not ordered_issues else "FAIL",
            lease_heads=tuple(sorted(heads, key=lambda item: item.lease_id)),
            active_leases=tuple(active),
            head_event_ids=tuple(sorted(head_ids)),
            event_count=len(events),
            issues=ordered_issues,
        )

    def acquire(
        self,
        *,
        task: TaskControlRecord,
        readiness: ReadinessDecision,
        lane_id: str,
        actor: str,
        current_base_commit: str,
        now: datetime,
        generation: int = 1,
        previous_lease_id: str | None = None,
    ) -> LeaseAcquisition:
        instant = _utc(now)
        if readiness.status != "READY" or readiness.task_id != task.task_id:
            raise ParallelControlError("LEASE_READINESS_REQUIRED", task.task_id)
        if task.task_id not in self.policy.allowlisted_task_ids:
            raise ParallelControlError("LEASE_TASK_NOT_ALLOWLISTED", task.task_id)
        if actor not in self.policy.allowlisted_actors:
            raise ParallelControlError("LEASE_ACTOR_NOT_ALLOWLISTED", actor)
        if task.manifest.base_commit != current_base_commit:
            raise ParallelControlError("LEASE_BASE_DRIFT", task.manifest.base_commit)
        lease_id = _lease_id(task.manifest, lane_id=lane_id, generation=generation)
        with self._arbiter(actor=actor, now=instant):
            replay = self.replay()
            if replay.status != "PASS":
                raise ParallelControlError("LEASE_REPLAY_INVALID", replay.issues[0].code)
            existing = {lease.lease_id: lease for lease in replay.lease_heads}.get(lease_id)
            head_event_id = dict(replay.head_event_ids).get(lease_id)
            if existing is not None and existing.state == "ACTIVE":
                if (
                    existing.actor != actor
                    or existing.change_manifest_sha256 != task.manifest.sha256
                ):
                    raise ParallelControlError("LEASE_IDENTITY_CONFLICT", lease_id)
                return LeaseAcquisition("ACTIVE", existing, ("IDEMPOTENT_REPLAY",), True)
            requested = existing
            if requested is None:
                requested = ExecutionLease(
                    lease_id=lease_id,
                    task_id=task.task_id,
                    change_id=task.manifest.change_id,
                    lane_id=lane_id,
                    actor=actor,
                    base_commit=current_base_commit,
                    change_manifest_sha256=task.manifest.sha256,
                    policy_version=self.policy.policy_version,
                    generation=generation,
                    previous_lease_id=previous_lease_id,
                    state="REQUESTED",
                    requested_at=instant.isoformat(),
                    acquired_at=None,
                    expires_at=None,
                    resources=manifest_resource_claims(task.manifest),
                )
                request_event = _lease_event(
                    lease=requested,
                    previous_event_id=None,
                    from_state=None,
                    to_state="REQUESTED",
                    occurred_at=instant,
                    actor=actor,
                    reason_codes=("READINESS_PASS",),
                )
                self._append_event(request_event)
                head_event_id = request_event.event_id
            elif requested.state != "REQUESTED":
                raise ParallelControlError("LEASE_ALREADY_TERMINAL", lease_id)
            blockers: list[str] = []
            if len(replay.active_leases) >= self.policy.max_total_active_leases:
                blockers.append("LEASE_CAPACITY_EXHAUSTED")
            candidate_active = replace(requested, state="ACTIVE")
            for active in replay.active_leases:
                if leases_conflict(candidate_active, active):
                    blockers.append(f"LEASE_RESOURCE_CONFLICT:{active.lease_id}")
            if blockers:
                blocked = replace(requested, state="BLOCKED")
                self._append_event(
                    _lease_event(
                        lease=blocked,
                        previous_event_id=head_event_id,
                        from_state="REQUESTED",
                        to_state="BLOCKED",
                        occurred_at=instant,
                        actor=actor,
                        reason_codes=tuple(sorted(blockers)),
                    )
                )
                return LeaseAcquisition("BLOCKED", blocked, tuple(sorted(blockers)), False)
            active = replace(
                requested,
                state="ACTIVE",
                acquired_at=instant.isoformat(),
                expires_at=(instant + timedelta(seconds=self.policy.lease_ttl_seconds)).isoformat(),
            )
            self._append_event(
                _lease_event(
                    lease=active,
                    previous_event_id=head_event_id,
                    from_state="REQUESTED",
                    to_state="ACTIVE",
                    occurred_at=instant,
                    actor=actor,
                    reason_codes=("LEASE_ACQUIRED",),
                )
            )
            return LeaseAcquisition("ACTIVE", active, ("LEASE_ACQUIRED",), False)

    def release(
        self,
        lease_id: str,
        *,
        actor: str,
        now: datetime,
        evidence_refs: Sequence[str],
    ) -> ExecutionLease:
        return self._terminal_transition(
            lease_id,
            actor=actor,
            now=now,
            to_state="RELEASED",
            reason_codes=("EXECUTION_AND_EVIDENCE_PASS",),
            evidence_refs=tuple(sorted(evidence_refs)),
        )

    def expire(
        self,
        lease_id: str,
        *,
        actor: str,
        now: datetime,
        reason_code: str,
    ) -> ExecutionLease:
        return self._terminal_transition(
            lease_id,
            actor=actor,
            now=now,
            to_state="EXPIRED",
            reason_codes=(reason_code,),
            evidence_refs=(),
        )

    def reassign(
        self,
        lease_id: str,
        *,
        task: TaskControlRecord,
        readiness: ReadinessDecision,
        lane_id: str,
        actor: str,
        current_base_commit: str,
        now: datetime,
    ) -> LeaseAcquisition:
        instant = _utc(now)
        with self._arbiter(actor=actor, now=instant):
            replay = self.replay()
            head = {lease.lease_id: lease for lease in replay.lease_heads}.get(lease_id)
            previous_event = dict(replay.head_event_ids).get(lease_id)
            if head is None or head.state != "EXPIRED":
                raise ParallelControlError("LEASE_REASSIGN_REQUIRES_EXPIRED", lease_id)
            if head.generation > self.policy.max_reassignments:
                raise ParallelControlError("LEASE_REASSIGNMENT_LIMIT", lease_id)
            reassigned = replace(head, state="REASSIGNED")
            self._append_event(
                _lease_event(
                    lease=reassigned,
                    previous_event_id=previous_event,
                    from_state="EXPIRED",
                    to_state="REASSIGNED",
                    occurred_at=instant,
                    actor=actor,
                    reason_codes=("LEASE_REASSIGNED",),
                )
            )
        return self.acquire(
            task=task,
            readiness=readiness,
            lane_id=lane_id,
            actor=actor,
            current_base_commit=current_base_commit,
            now=instant,
            generation=head.generation + 1,
            previous_lease_id=lease_id,
        )

    def _terminal_transition(
        self,
        lease_id: str,
        *,
        actor: str,
        now: datetime,
        to_state: str,
        reason_codes: tuple[str, ...],
        evidence_refs: tuple[str, ...],
    ) -> ExecutionLease:
        instant = _utc(now)
        with self._arbiter(actor=actor, now=instant):
            replay = self.replay()
            head = {lease.lease_id: lease for lease in replay.lease_heads}.get(lease_id)
            previous_event = dict(replay.head_event_ids).get(lease_id)
            if head is None or head.state != "ACTIVE":
                raise ParallelControlError("LEASE_ACTIVE_REQUIRED", lease_id)
            if actor != head.actor and actor != "integration-coordinator":
                raise ParallelControlError("LEASE_ACTOR_MISMATCH", lease_id)
            terminal = replace(
                head,
                state=to_state,
                evidence_refs=evidence_refs,
            )
            self._append_event(
                _lease_event(
                    lease=terminal,
                    previous_event_id=previous_event,
                    from_state="ACTIVE",
                    to_state=to_state,
                    occurred_at=instant,
                    actor=actor,
                    reason_codes=reason_codes,
                )
            )
            return terminal

    def _append_event(self, event: LeaseEvent) -> None:
        path = self.events_root / event.lease.lease_id / f"{event.event_id}.json"
        if path.exists():
            existing = json.loads(path.read_text(encoding="utf-8"))
            if existing != event.to_dict():
                raise ParallelControlError("LEASE_EVENT_IMMUTABILITY", event.event_id)
            return
        write_json_atomic(path, event.to_dict())

    @contextmanager
    def _arbiter(self, *, actor: str, now: datetime) -> Iterator[None]:
        self.root.mkdir(parents=True, exist_ok=True)
        acquired = False
        try:
            self.arbiter_root.mkdir()
            acquired = True
        except FileExistsError:
            owner_path = self.arbiter_root / "owner.json"
            try:
                owner = json.loads(owner_path.read_text(encoding="utf-8"))
                expires = datetime.fromisoformat(str(owner["expires_at"]))
            except (OSError, KeyError, ValueError, json.JSONDecodeError) as exc:
                raise ParallelControlError("LEASE_ARBITER_STATE_INVALID", str(exc)) from exc
            if expires > now:
                raise ParallelControlError("LEASE_ARBITER_BUSY", str(owner.get("actor"))) from None
            stale = self.root / f"arbiter.stale.{hashlib.sha256(actor.encode()).hexdigest()[:12]}"
            try:
                os.replace(self.arbiter_root, stale)
            except OSError as exc:
                raise ParallelControlError("LEASE_ARBITER_CAS_FAILED", str(exc)) from exc
            (stale / "owner.json").unlink(missing_ok=True)
            stale.rmdir()
            try:
                self.arbiter_root.mkdir()
                acquired = True
            except FileExistsError as exc:
                raise ParallelControlError("LEASE_ARBITER_CAS_FAILED", actor) from exc
        try:
            write_json_atomic(
                self.arbiter_root / "owner.json",
                {
                    "schema_version": "execution_lease_arbiter.v1",
                    "actor": actor,
                    "acquired_at": now.isoformat(),
                    "expires_at": (
                        now + timedelta(seconds=self.policy.arbiter_ttl_seconds)
                    ).isoformat(),
                    "production_effect": "none",
                },
            )
            yield
        finally:
            if acquired:
                (self.arbiter_root / "owner.json").unlink(missing_ok=True)
                try:
                    self.arbiter_root.rmdir()
                except FileNotFoundError:
                    pass


def _lease_event(
    *,
    lease: ExecutionLease,
    previous_event_id: str | None,
    from_state: str | None,
    to_state: str,
    occurred_at: datetime,
    actor: str,
    reason_codes: tuple[str, ...],
) -> LeaseEvent:
    prototype = LeaseEvent(
        event_id="",
        lease=lease,
        previous_event_id=previous_event_id,
        from_state=from_state,
        to_state=to_state,
        occurred_at=occurred_at.isoformat(),
        actor=actor,
        reason_codes=reason_codes,
    )
    return replace(prototype, event_id=f"lease-event-{_canonical_sha256(prototype._body())[:20]}")


def _parse_lease_event(payload: Mapping[str, Any]) -> LeaseEvent:
    event_id = _text(payload.get("event_id"), "event_id")
    lease_payload = _mapping(payload.get("lease"), "lease")
    resource_payloads = lease_payload.get("resources")
    if not isinstance(resource_payloads, list):
        raise ParallelControlError("LEASE_RESOURCES", "resources must be a list")
    resources = tuple(
        sorted(
            ResourceClaim(
                kind=_text(_mapping(item, "resource").get("kind"), "kind"),
                resource_id=_text(_mapping(item, "resource").get("resource_id"), "resource_id"),
                access=ResourceAccess(_mapping(item, "resource").get("access")),
            )
            for item in resource_payloads
        )
    )
    lease = ExecutionLease(
        lease_id=_text(lease_payload.get("lease_id"), "lease_id"),
        task_id=_text(lease_payload.get("task_id"), "task_id"),
        change_id=_text(lease_payload.get("change_id"), "change_id"),
        lane_id=_text(lease_payload.get("lane_id"), "lane_id"),
        actor=_text(lease_payload.get("actor"), "actor"),
        base_commit=_text(lease_payload.get("base_commit"), "base_commit"),
        change_manifest_sha256=_text(
            lease_payload.get("change_manifest_sha256"), "change_manifest_sha256"
        ),
        policy_version=_text(lease_payload.get("policy_version"), "policy_version"),
        generation=_positive_int(lease_payload.get("generation"), "generation"),
        previous_lease_id=(
            None
            if lease_payload.get("previous_lease_id") is None
            else _text(lease_payload.get("previous_lease_id"), "previous_lease_id")
        ),
        state=_text(lease_payload.get("state"), "state"),
        requested_at=_text(lease_payload.get("requested_at"), "requested_at"),
        acquired_at=(
            None
            if lease_payload.get("acquired_at") is None
            else _text(lease_payload.get("acquired_at"), "acquired_at")
        ),
        expires_at=(
            None
            if lease_payload.get("expires_at") is None
            else _text(lease_payload.get("expires_at"), "expires_at")
        ),
        resources=resources,
        evidence_refs=_strings(lease_payload.get("evidence_refs"), "evidence_refs"),
    )
    prototype = LeaseEvent(
        event_id=event_id,
        lease=lease,
        previous_event_id=(
            None
            if payload.get("previous_event_id") is None
            else _text(payload.get("previous_event_id"), "previous_event_id")
        ),
        from_state=(
            None
            if payload.get("from_state") is None
            else _text(payload.get("from_state"), "from_state")
        ),
        to_state=_text(payload.get("to_state"), "to_state"),
        occurred_at=_text(payload.get("occurred_at"), "occurred_at"),
        actor=_text(payload.get("actor"), "actor"),
        reason_codes=_strings(payload.get("reason_codes"), "reason_codes"),
    )
    expected = f"lease-event-{_canonical_sha256(prototype._body())[:20]}"
    if event_id != expected:
        raise ParallelControlError("LEASE_EVENT_HASH", event_id)
    return prototype


def _lease_id(manifest: ChangeManifest, *, lane_id: str, generation: int) -> str:
    identity = {
        "change_id": manifest.change_id,
        "manifest_sha256": manifest.sha256,
        "lane_id": lane_id,
        "generation": generation,
    }
    return f"lease-{_canonical_sha256(identity)[:20]}"


def _canonical_sha256(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _issue(
    code: str,
    task_ids: tuple[str, ...],
    resource: str,
    message: str,
) -> ControlIssue:
    return ControlIssue(code, task_ids, resource, message)


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ParallelControlError("CONTROL_MAPPING_REQUIRED", field)
    return value


def _strings(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ParallelControlError("CONTROL_LIST_REQUIRED", field)
    rows = tuple(_text(item, field) for item in value)
    if len(rows) != len(set(rows)):
        raise ParallelControlError("CONTROL_LIST_DUPLICATE", field)
    return rows


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ParallelControlError("CONTROL_TEXT_REQUIRED", field)
    return value.strip()


def _boolean(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise ParallelControlError("CONTROL_BOOL_REQUIRED", field)
    return value


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ParallelControlError("CONTROL_POSITIVE_INT_REQUIRED", field)
    return value


def _non_negative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ParallelControlError("CONTROL_NON_NEGATIVE_INT_REQUIRED", field)
    return value


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ParallelControlError("CONTROL_TIMEZONE_REQUIRED", value.isoformat())
    return value.astimezone(UTC)
