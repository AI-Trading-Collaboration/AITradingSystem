from __future__ import annotations

import copy
import hashlib
import json
import stat
import threading
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, cast

from ai_trading_system.platform.architecture.lease_arbiter import hold_lease_arbiter
from ai_trading_system.platform.architecture.parallel_control import (
    ChangeManifest,
    ContractAccess,
    ControlIssue,
    ParallelControlError,
)
from ai_trading_system.platform.artifacts import write_bytes_atomic, write_json_atomic
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
    "ACTIVE": frozenset({"ACTIVE", "RELEASED", "EXPIRED"}),
    "EXPIRED": frozenset({"REASSIGNED"}),
    "RELEASED": frozenset(),
    "REASSIGNED": frozenset(),
    "BLOCKED": frozenset({"RELEASED"}),
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
    execution: Mapping[str, Any] | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": LEASE_SCHEMA_VERSION
            if self.execution is None
            else "execution_lease.v2",
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
            **({"execution": dict(self.execution)} if self.execution is not None else {}),
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
    # DEVX-022 S3b: how this event is STORED. False = every historical event (the custody table is
    # embedded, original id formula); True = qualifying tables are externalized (schema v3).
    rows_externalized: bool = False

    def _body(self) -> dict[str, object]:
        """The stored form: the id is the hash of exactly this canonical body."""
        lease = self.lease.to_dict()
        schema = (
            LEASE_EVENT_SCHEMA_VERSION
            if self.lease.execution is None
            else "execution_lease_event.v2"
        )
        if self.rows_externalized and isinstance(lease.get("execution"), Mapping):
            execution, changed = _rewrite_rows(
                cast(Mapping[str, Any], lease["execution"]), _compact_rows,
            )
            if changed:
                lease["execution"] = dict(execution)
                lease["schema_version"] = LEASE_V3_SCHEMA_VERSION
                schema = LEASE_EVENT_V3_SCHEMA_VERSION
        return {
            "schema_version": schema,
            "lease": lease,
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
    if safety.get("source_of_truth") not in {
        "LEGACY_MARKDOWN_ONLY",
        "ARCH_005_TASK_REGISTRY",
    }:
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
    for left in first.resources:
        for right in second.resources:
            if left.kind != right.kind:
                continue
            if left.kind == "path":
                same_resource = _repository_paths_overlap(left.resource_id, right.resource_id)
            else:
                same_resource = left.resource_id == right.resource_id
            if same_resource and ResourceAccess.WRITE in {left.access, right.access}:
                return True
    return False


def replay_lease_events(
    events: Sequence[LeaseEvent],
    *,
    initial_issues: Sequence[ControlIssue] = (),
) -> LeaseReplay:
    from ai_trading_system.platform.architecture.workflow_coordination import (
        validate_execution_transition,
    )

    # Public callers may construct LeaseEvent objects without parsing them.
    # Preserve the complete execution validation at this public boundary.
    return _replay_lease_events(
        events, initial_issues=initial_issues,
        execution_transition=validate_execution_transition,
    )


def _replay_lease_events(
    events: Sequence[LeaseEvent],
    *,
    initial_issues: Sequence[ControlIssue],
    execution_transition: Callable[[ExecutionLease | None, ExecutionLease], None],
) -> LeaseReplay:
    issues = set(initial_issues)
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
            issues.add(_issue("LEASE_CAUSAL_DISCONNECTED", (), lease_id, "event chain incomplete"))
            continue
        prior_state: str | None = None
        prior_lease: ExecutionLease | None = None
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
            if prior_state == "BLOCKED" and record.to_state == "RELEASED":
                if (
                    prior_lease is None or prior_lease.execution is not None
                    or prior_lease.acquired_at is not None
                    or record.actor not in {prior_lease.actor, "integration-coordinator"}
                    or record.reason_codes != ("REQUEST_CANCELLED",)
                    or record.lease != replace(
                        prior_lease, state="RELEASED", evidence_refs=record.lease.evidence_refs,
                    )
                ):
                    issues.add(_issue("LEASE_CANCEL_INVALID", (), lease_id, record.event_id))
                    valid = False
                    break
            if record.lease.execution is not None or (
                prior_lease is not None and prior_lease.execution is not None
            ):
                from ai_trading_system.platform.architecture.workflow_coordination import (
                    validate_execution_transition,
                )

                try:
                    # An absent execution was not checked by the file parser;
                    # removing a previous execution still takes the full gate.
                    if record.lease.execution is None:
                        validate_execution_transition(prior_lease, record.lease)
                    else:
                        execution_transition(prior_lease, record.lease)
                except ParallelControlError as exc:
                    issues.add(_issue(exc.code, (), lease_id, str(exc)))
                    valid = False
                    break
            prior_state = record.to_state
            prior_lease = record.lease
        if valid:
            heads.append(head.lease)
            head_ids.append((lease_id, head.event_id))
    active = sorted(
        (lease for lease in heads if lease.state == "ACTIVE"),
        key=lambda item: item.lease_id,
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


class FileExecutionLeaseStore:
    def __init__(self, root: Path, *, policy: ParallelControlPolicy) -> None:
        self.requested_root = root.absolute()
        self.root = root.resolve()
        self.policy = policy
        self.events_root = self.root / "events"
        self.arbiter_root = self.root / "arbiter.lock"
        # DEVX-022 S3b: content-addressed row tables named by externalized (v3) events.
        self.blobs = ExternalizedRowsReader(self.root / "blobs")
        self._atomic_context = threading.local()
        self.coordination_binding: Any = None

    def replay(self) -> LeaseReplay:
        from ai_trading_system.platform.architecture.workflow_coordination import (
            _validate_checked_execution_transition,
            replay_validation_scope,
        )

        events: list[LeaseEvent] = []
        issues: set[ControlIssue] = set()
        if self.events_root.exists():
            # DEVX-022 S3a: the scope memoizes equal large sub-structures for this call only.
            # DEVX-022 S3b: a table blob is read, hashed and decoded once per call and shared.
            with replay_validation_scope(), _rows_sharing_scope():
                for path in sorted(self.events_root.glob("*/*.json")):
                    try:
                        events.append(parse_lease_event(
                            json.loads(path.read_text(encoding="utf-8")), blobs=self.blobs,
                        ))
                    except (OSError, json.JSONDecodeError, ParallelControlError) as exc:
                        issues.add(_issue("LEASE_EVENT_INVALID", (), path.as_posix(), str(exc)))
        # Each local event was fully validated by parse_lease_event above in
        # this call. Check every causal/transition rule without validating the
        # same current tree twice. No validation survives this invocation and
        # no caller-owned event or on-disk "already checked" flag is admitted.
        return _replay_lease_events(
            events, initial_issues=tuple(issues),
            execution_transition=_validate_checked_execution_transition,
        )

    def _require_ready_request(
        self, task: TaskControlRecord, readiness: ReadinessDecision, *,
        actor: str, current_base_commit: str,
    ) -> None:
        # A READY decision belongs to one exact request and policy. Validate
        # before either admission or consuming an expired lease's recovery slot.
        if (
            readiness.status != "READY" or readiness.task_id != task.task_id
            or readiness.change_id != task.manifest.change_id
            or readiness.manifest_sha256 != task.manifest.sha256
            or readiness.policy_version != self.policy.policy_version
        ):
            raise ParallelControlError("LEASE_READINESS_REQUIRED", task.task_id)
        if task.task_id not in self.policy.allowlisted_task_ids:
            raise ParallelControlError("LEASE_TASK_NOT_ALLOWLISTED", task.task_id)
        if actor not in self.policy.allowlisted_actors:
            raise ParallelControlError("LEASE_ACTOR_NOT_ALLOWLISTED", actor)
        if task.manifest.base_commit != current_base_commit:
            raise ParallelControlError("LEASE_BASE_DRIFT", task.manifest.base_commit)

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
        self._require_ready_request(task, readiness, actor=actor,
                                    current_base_commit=current_base_commit)
        lease_id = _lease_id(task.manifest, lane_id=lane_id, generation=generation)
        with self._arbiter(actor=actor, now=instant, operation="acquire"):
            replay = self.replay()
            if replay.status != "PASS":
                raise ParallelControlError("LEASE_REPLAY_INVALID", replay.issues[0].code)
            if self._expire_stale_heads(replay, actor=actor, now=instant):
                replay = self.replay()
                if replay.status != "PASS":
                    raise ParallelControlError("LEASE_REPLAY_INVALID", replay.issues[0].code)
            heads = {lease.lease_id: lease for lease in replay.lease_heads}
            existing = heads.get(lease_id)
            # A resource refusal is immutable evidence, not a permanently lost
            # request. Recheck the SAME complete identity and current readiness;
            # only after contention clears create a linked fresh attempt. Never
            # rewrite BLOCKED to ACTIVE or consume an execution-recovery slot.
            while existing is not None and existing.state == "BLOCKED":
                if (
                    existing.actor != actor or existing.task_id != task.task_id
                    or existing.change_id != task.manifest.change_id
                    or existing.change_manifest_sha256 != task.manifest.sha256
                    or existing.base_commit != current_base_commit
                    or existing.policy_version != self.policy.policy_version
                    or existing.lane_id != lane_id
                    or existing.resources != manifest_resource_claims(task.manifest)
                    or existing.execution is not None
                ):
                    raise ParallelControlError("LEASE_IDENTITY_CONFLICT", existing.lease_id)
                next_id = _lease_id(task.manifest, lane_id=lane_id, generation=generation + 1)
                if next_id not in heads:
                    retry_blockers = []
                    if len(replay.active_leases) >= self.policy.max_total_active_leases:
                        retry_blockers.append("LEASE_CAPACITY_EXHAUSTED")
                    for active in replay.active_leases:
                        if leases_conflict(replace(existing, state="ACTIVE"), active):
                            retry_blockers.append(f"LEASE_RESOURCE_CONFLICT:{active.lease_id}")
                    if retry_blockers:
                        return LeaseAcquisition(
                            "BLOCKED", existing, tuple(sorted(retry_blockers)), True,
                        )
                previous_lease_id = existing.lease_id
                generation += 1
                lease_id = next_id
                existing = heads.get(lease_id)
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

    def heartbeat(
        self,
        lease_id: str,
        *,
        actor: str,
        now: datetime,
    ) -> ExecutionLease:
        instant = _utc(now)
        with self._arbiter(actor=actor, now=instant, operation="heartbeat"):
            replay = self.replay()
            if replay.status != "PASS":
                raise ParallelControlError("LEASE_REPLAY_INVALID", replay.issues[0].code)
            head = {lease.lease_id: lease for lease in replay.lease_heads}.get(lease_id)
            previous_event = dict(replay.head_event_ids).get(lease_id)
            if head is None or head.state != "ACTIVE":
                raise ParallelControlError("LEASE_ACTIVE_REQUIRED", lease_id)
            if actor != head.actor:
                raise ParallelControlError("LEASE_ACTOR_MISMATCH", lease_id)
            expiry = _lease_expiry(head)
            if expiry <= instant and head.execution is None:
                raise ParallelControlError("LEASE_HEARTBEAT_EXPIRED", lease_id)
            refreshed = replace(
                head,
                expires_at=(instant + timedelta(seconds=self.policy.lease_ttl_seconds)).isoformat(),
            )
            self._append_event(
                _lease_event(
                    lease=refreshed,
                    previous_event_id=previous_event,
                    from_state="ACTIVE",
                    to_state="ACTIVE",
                    occurred_at=instant,
                    actor=actor,
                    reason_codes=("LEASE_HEARTBEAT",),
                )
            )
            return refreshed

    def cancel_request(
        self, lease_id: str, *, actor: str, now: datetime,
        evidence_refs: Sequence[str] = (),
    ) -> ExecutionLease:
        """Retire a nonexecuting refusal under the original arbiter, without granting work."""
        instant = _utc(now)
        with self._arbiter(actor=actor, now=instant, operation="terminal"):
            replay = self.replay()
            if replay.status != "PASS":
                raise ParallelControlError("LEASE_REPLAY_INVALID", lease_id)
            head = next((row for row in replay.lease_heads if row.lease_id == lease_id), None)
            if head is None:
                raise ParallelControlError("LEASE_CANCEL_UNKNOWN", lease_id)
            if actor not in {head.actor, "integration-coordinator"}:
                raise ParallelControlError("LEASE_ACTOR_MISMATCH", lease_id)
            if any(row.previous_lease_id == lease_id for row in replay.lease_heads):
                raise ParallelControlError("LEASE_CANCEL_SUPERSEDED", lease_id)
            previous_event = dict(replay.head_event_ids)[lease_id]
            if head.state == "RELEASED":
                event = parse_lease_event(json.loads(
                    (self.events_root / lease_id / f"{previous_event}.json").read_text(
                        encoding="utf-8",
                    )
                ), blobs=self.blobs)
                if event.from_state == "BLOCKED" and event.reason_codes == ("REQUEST_CANCELLED",):
                    return head
            if (
                head.state != "BLOCKED" or head.execution is not None
                or head.acquired_at is not None
            ):
                raise ParallelControlError("LEASE_CANCEL_REQUIRES_BLOCKED", lease_id)
            terminal = replace(head, state="RELEASED", evidence_refs=tuple(sorted(evidence_refs)))
            self._append_event(_lease_event(
                lease=terminal, previous_event_id=previous_event, from_state="BLOCKED",
                to_state="RELEASED", occurred_at=instant, actor=actor,
                reason_codes=("REQUEST_CANCELLED",),
            ))
            return terminal

    def release(
        self,
        lease_id: str,
        *,
        actor: str,
        now: datetime,
        evidence_refs: Sequence[str],
        reason_codes: Sequence[str] = ("EXECUTION_AND_EVIDENCE_PASS",),
    ) -> ExecutionLease:
        return self._terminal_transition(
            lease_id,
            actor=actor,
            now=now,
            to_state="RELEASED",
            reason_codes=tuple(sorted(reason_codes)),
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
        self._require_ready_request(task, readiness, actor=actor,
                                    current_base_commit=current_base_commit)
        with self._arbiter(actor=actor, now=instant, operation="reassign"):
            replay = self.replay()
            head = {lease.lease_id: lease for lease in replay.lease_heads}.get(lease_id)
            previous_event = dict(replay.head_event_ids).get(lease_id)
            if head is None or head.state != "EXPIRED":
                raise ParallelControlError("LEASE_REASSIGN_REQUIRES_EXPIRED", lease_id)
            self._require_execution_terminal(head)
            # Admission-only BLOCKED attempts never ran an executor. Count
            # actual prior reassignments, not their generation numbers.
            ancestors = {lease.lease_id: lease for lease in replay.lease_heads}
            cursor = head
            reassignments = 0
            seen = {head.lease_id}
            while cursor.previous_lease_id is not None:
                prior = ancestors.get(cursor.previous_lease_id)
                if (
                    prior is None or prior.lease_id in seen
                    or prior.generation + 1 != cursor.generation
                    or prior.state not in {"BLOCKED", "REASSIGNED"}
                ):
                    raise ParallelControlError("LEASE_REASSIGNMENT_HISTORY", lease_id)
                reassignments += int(prior.state == "REASSIGNED")
                seen.add(prior.lease_id)
                cursor = prior
            if cursor.generation != 1:
                raise ParallelControlError("LEASE_REASSIGNMENT_HISTORY", lease_id)
            if reassignments >= self.policy.max_reassignments:
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
        with self._arbiter(actor=actor, now=instant, operation="terminal"):
            replay = self.replay()
            head = {lease.lease_id: lease for lease in replay.lease_heads}.get(lease_id)
            previous_event = dict(replay.head_event_ids).get(lease_id)
            if head is None or head.state != "ACTIVE":
                raise ParallelControlError("LEASE_ACTIVE_REQUIRED", lease_id)
            if actor != head.actor and actor != "integration-coordinator":
                raise ParallelControlError("LEASE_ACTOR_MISMATCH", lease_id)
            self._require_execution_terminal(head)
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

    def _expire_stale_heads(
        self,
        replay: LeaseReplay,
        *,
        actor: str,
        now: datetime,
    ) -> bool:
        from ai_trading_system.platform.architecture.workflow_coordination import (
            execution_is_terminal,
        )

        expired_any = False
        head_event_ids = dict(replay.head_event_ids)
        for head in replay.active_leases:
            if not execution_is_terminal(head.execution):
                continue  # Diagnostic TTL never steals live or uncertain execution.
            if _lease_expiry(head) > now:
                continue
            expired = replace(head, state="EXPIRED")
            self._append_event(
                _lease_event(
                    lease=expired,
                    previous_event_id=head_event_ids[head.lease_id],
                    from_state="ACTIVE",
                    to_state="EXPIRED",
                    occurred_at=now,
                    actor=actor,
                    reason_codes=("STALE_HEARTBEAT_EXPIRED",),
                )
            )
            expired_any = True
        return expired_any

    @staticmethod
    def _require_execution_terminal(head: ExecutionLease) -> None:
        from ai_trading_system.platform.architecture.workflow_coordination import (
            execution_is_terminal,
        )

        if not execution_is_terminal(head.execution):
            raise ParallelControlError("LEASE_EXECUTION_NOT_TERMINAL", head.lease_id)

    def execution_lifecycle(self) -> Any:
        from ai_trading_system.platform.architecture.workflow_coordination import ExecutionLifecycle

        return ExecutionLifecycle(self)

    def _append_event(self, event: LeaseEvent) -> None:
        path = self.events_root / event.lease.lease_id / f"{event.event_id}.json"
        if event.rows_externalized:
            # DEVX-022 S3b: every named blob exists (and matches) before the event naming it.
            for rows in _qualifying_rows(event.lease.execution):
                self.blobs.write(rows)
        if path.exists():
            existing = json.loads(path.read_text(encoding="utf-8"))
            if existing != event.to_dict():
                raise ParallelControlError("LEASE_EVENT_IMMUTABILITY", event.event_id)
            return
        write_json_atomic(path, event.to_dict())

    @contextmanager
    def atomic(self, *, actor: str, now: datetime, operation: str = "compound") -> Iterator[None]:
        """One short compound operation under this store's existing OS arbiter.

        The capability is a live process/thread-bound handle, never an input
        boolean or on-disk receipt. Nested store methods reuse it only on the
        same store instance and for the same actor. It is not an execution-
        lifetime lease and must not enclose a test run or a child-process wait.
        """
        if getattr(self._atomic_context, "binding", None) is not None:
            with self._arbiter(actor=actor, now=now, operation=operation):
                yield
            return
        # Reject an unregistered root before even creating the arbiter anchor.
        # This is admission only; the same checks repeat after lock acquisition
        # to fence a concurrent registration/phase change.
        self._assert_writer(operation)
        with hold_lease_arbiter(
            self.root, actor=actor, now=now, arbiter_ttl_seconds=self.policy.arbiter_ttl_seconds
        ) as held:
            self._assert_writer(operation)
            self._atomic_context.binding = (actor, held)
            try:
                yield
            finally:
                del self._atomic_context.binding

    def _assert_writer(self, operation: str) -> None:
        from ai_trading_system.platform.architecture.workflow_coordination import (
            assert_store_writer,
        )

        assert_store_writer(self, operation=operation)

    @contextmanager
    def _arbiter(self, *, actor: str, now: datetime, operation: str = "compound") -> Iterator[None]:
        binding = getattr(self._atomic_context, "binding", None)
        if binding is not None:
            owner, held = binding
            if actor != owner or held.path.parent != self.root:
                raise ParallelControlError("LEASE_ATOMIC_OWNER_MISMATCH", actor)
            held.assert_owner(owner_token=held.token)
            held.assert_anchor()
            self._assert_writer(operation)
            try:
                yield
            finally:
                held.assert_owner(owner_token=held.token)
                held.assert_anchor()
            return
        # DEVX-014/S1a: the same unique arbiter uses a stable OS-owned handle.
        # Policy TTL remains diagnostic; it never permits stealing a live lock.
        self._assert_writer(operation)
        with hold_lease_arbiter(
            self.root, actor=actor, now=now, arbiter_ttl_seconds=self.policy.arbiter_ttl_seconds
        ):
            self._assert_writer(operation)
            yield


# DEVX-022 S3b: externalized row tables in lease events -----------------------------------------
#
# A publication execution carries the hook-ready custody list (`read_file_custodies`, ~17.9k rows,
# 94% of a 22 MB event) and every later event repeats the whole execution snapshot. Replaying one
# such event cost about 100 ms (read, decode, canonical serialize, hash), almost all of it for that
# list. The stored form of an event now replaces the list, at two fixed positions only, with a
# marker naming the SHA-256 of its canonical bytes; the bytes live in a content-addressed blob next
# to the events. The event id is always the hash of the *stored* canonical body, so a stored form
# that embeds the list (every historical event) keeps its original id formula, and a compact one is
# verified without serializing the list. In memory the list is expanded back to exactly the value
# the old code held, so every validator sees unchanged data.
#
# EXTERNALIZED_ROWS_MINIMUM is an engineering invariant, not a tunable heuristic: below it embedding
# is cheaper than a blob and the small fixtures stay byte-for-byte as they were.
EXTERNALIZED_ROWS_MINIMUM = 1024
EXTERNALIZED_ROWS_MARKER = "externalized_rows.v1"
EXTERNALIZED_ROWS_FIELD = "read_file_custodies"
EXTERNALIZED_ROWS_MAX_BYTES = 64 * 1024 * 1024
LEASE_V3_SCHEMA_VERSION = "execution_lease.v3"
LEASE_EVENT_V3_SCHEMA_VERSION = "execution_lease_event.v3"
_HEX_DIGITS = frozenset("0123456789abcdef")


class ExternalizedRows(list[Any]):
    """A verified row table, shared by every event of one replay call and never mutated.

    It carries the SHA-256 of its canonical bytes so that the stored form of an event can be
    rebuilt without serializing the rows again. Every mutator raises: a changed row would
    silently invalidate that digest. (A deep copy is a plain list and is simply hashed again.)
    """

    __slots__ = ("sha256",)
    sha256: str

    def _immutable(self, *args: Any, **kwargs: Any) -> Any:
        raise TypeError("externalized rows are immutable")

    __setitem__ = __delitem__ = __iadd__ = __imul__ = _immutable
    append = extend = insert = pop = remove = clear = sort = reverse = _immutable

    def __copy__(self) -> list[Any]:
        return list(self)

    def __deepcopy__(self, memo: dict[int, Any]) -> list[Any]:
        return copy.deepcopy(list(self), memo)


_ROWS_SHARING: ContextVar[dict[str, ExternalizedRows] | None] = ContextVar(
    "lease_replay_externalized_rows", default=None,
)


@contextmanager
def _rows_sharing_scope() -> Iterator[None]:
    """Share decoded row tables inside ONE replay call; nothing survives the call."""
    token = _ROWS_SHARING.set({})
    try:
        yield
    finally:
        _ROWS_SHARING.reset(token)


def _rows_canonical_bytes(rows: Sequence[Any]) -> bytes:
    return json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _is_sha256_hex(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and set(value) <= _HEX_DIGITS


def _adopt_rows(rows: Any) -> Any:
    """Give a qualifying table its digest once; any other value passes through unchanged."""
    if not isinstance(rows, list) or len(rows) < EXTERNALIZED_ROWS_MINIMUM:
        return rows
    if type(rows) is ExternalizedRows and hasattr(rows, "sha256"):
        return rows
    adopted = ExternalizedRows(rows)
    adopted.sha256 = hashlib.sha256(_rows_canonical_bytes(rows)).hexdigest()
    return adopted


def _compact_rows(rows: Any) -> Any:
    """Stored form of a qualifying table: the marker (digest from the shared table)."""
    adopted = _adopt_rows(rows)
    if type(adopted) is not ExternalizedRows:
        return rows
    return {
        EXTERNALIZED_ROWS_MARKER: {"sha256": adopted.sha256, "row_count": len(adopted)},
    }


def _rewrite_capsule(capsule: Any, convert: Callable[[Any], Any]) -> tuple[Any, bool]:
    if not isinstance(capsule, Mapping):
        return capsule, False
    ready = capsule.get("ready")
    if not isinstance(ready, Mapping):
        return capsule, False
    inputs = ready.get("inputs")
    if not isinstance(inputs, Mapping) or EXTERNALIZED_ROWS_FIELD not in inputs:
        return capsule, False
    current = inputs[EXTERNALIZED_ROWS_FIELD]
    converted = convert(current)
    if converted is current:
        return capsule, False
    return {
        **capsule,
        "ready": {**ready, "inputs": {**inputs, EXTERNALIZED_ROWS_FIELD: converted}},
    }, True


def _rewrite_rows(
    execution: Mapping[str, Any], convert: Callable[[Any], Any],
) -> tuple[Mapping[str, Any], bool]:
    """Apply `convert` to the custody table at the two fixed positions; copy on write."""
    result = dict(execution)
    changed = False
    capsule, hit = _rewrite_capsule(result.get("hook_capsule"), convert)
    if hit:
        result["hook_capsule"] = capsule
        changed = True
    attempts = result.get("publication_attempts")
    if isinstance(attempts, list):
        rewritten = list(attempts)
        touched = False
        for index, attempt in enumerate(attempts):
            if not isinstance(attempt, Mapping):
                continue
            capsule, hit = _rewrite_capsule(attempt.get("hook_capsule"), convert)
            if hit:
                rewritten[index] = {**attempt, "hook_capsule": capsule}
                touched = True
        if touched:
            result["publication_attempts"] = rewritten
            changed = True
    return (result if changed else execution), changed


def _qualifying_rows(execution: Mapping[str, Any] | None) -> list[ExternalizedRows]:
    """The tables of this execution that the stored form externalizes, each with its digest."""
    if not isinstance(execution, Mapping):
        return []
    found: list[ExternalizedRows] = []

    def collect(rows: Any) -> Any:
        adopted = _adopt_rows(rows)
        if type(adopted) is ExternalizedRows:
            found.append(adopted)
        return rows

    _rewrite_rows(execution, collect)
    return found


def _marker_payload(value: object) -> tuple[str, int] | None:
    if not isinstance(value, Mapping) or set(value) != {EXTERNALIZED_ROWS_MARKER}:
        return None
    inner = value[EXTERNALIZED_ROWS_MARKER]
    if (
        not isinstance(inner, Mapping) or set(inner) != {"sha256", "row_count"}
        or not _is_sha256_hex(inner["sha256"])
        or type(inner["row_count"]) is not int
        or inner["row_count"] < EXTERNALIZED_ROWS_MINIMUM
    ):
        raise ParallelControlError("LEASE_EXTERNALIZED_ROWS_INVALID", "marker")
    return cast(str, inner["sha256"]), inner["row_count"]


def _assert_no_stray_marker(node: object) -> None:
    """A marker is legitimate only as the value of a custody-table field."""
    if isinstance(node, Mapping):
        if set(node) == {EXTERNALIZED_ROWS_MARKER}:
            raise ParallelControlError("LEASE_EXTERNALIZED_ROWS_INVALID", "marker position")
        for key, value in node.items():
            if key != EXTERNALIZED_ROWS_FIELD:
                _assert_no_stray_marker(value)
    elif isinstance(node, list):
        for item in node:
            _assert_no_stray_marker(item)


@dataclass(frozen=True)
class ExternalizedRowsReader:
    """Content-addressed row-table blobs next to the events of one lease store."""

    root: Path

    def path_for(self, sha256: str) -> Path:
        if not _is_sha256_hex(sha256):
            raise ParallelControlError("LEASE_EXTERNALIZED_ROWS_INVALID", "digest")
        return self.root / sha256[:2] / f"{sha256}.json"

    def read(self, sha256: str, row_count: int) -> ExternalizedRows:
        shared = _ROWS_SHARING.get()
        if shared is not None and sha256 in shared:
            known = shared[sha256]
            if len(known) != row_count:
                raise ParallelControlError("LEASE_EXTERNALIZED_ROWS_INVALID", "row count")
            return known
        path = self.path_for(sha256)
        try:
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_size > EXTERNALIZED_ROWS_MAX_BYTES:
                raise ParallelControlError("LEASE_EXTERNALIZED_ROWS_INVALID", "blob file")
            raw = path.read_bytes()
        except OSError as exc:
            raise ParallelControlError("LEASE_EXTERNALIZED_ROWS_UNAVAILABLE", sha256) from exc
        if hashlib.sha256(raw).hexdigest() != sha256:
            raise ParallelControlError("LEASE_EXTERNALIZED_ROWS_DIGEST", sha256)
        try:
            decoded = json.loads(raw)
        except ValueError as exc:
            raise ParallelControlError("LEASE_EXTERNALIZED_ROWS_INVALID", "blob json") from exc
        if (
            not isinstance(decoded, list) or len(decoded) != row_count
            or any(not isinstance(row, dict) for row in decoded)
        ):
            raise ParallelControlError("LEASE_EXTERNALIZED_ROWS_INVALID", "blob rows")
        rows = ExternalizedRows(decoded)
        rows.sha256 = sha256
        if shared is not None:
            shared[sha256] = rows
        return rows

    def write(self, rows: ExternalizedRows) -> None:
        """Create the blob before the event that names it; an existing one must still match."""
        path = self.path_for(rows.sha256)
        if path.exists():
            try:
                existing = path.read_bytes()
            except OSError as exc:
                raise ParallelControlError(
                    "LEASE_EXTERNALIZED_ROWS_UNAVAILABLE", rows.sha256,
                ) from exc
            if hashlib.sha256(existing).hexdigest() != rows.sha256:
                raise ParallelControlError("LEASE_EXTERNALIZED_ROWS_DIGEST", rows.sha256)
            return
        content = _rows_canonical_bytes(rows)
        if hashlib.sha256(content).hexdigest() != rows.sha256:
            raise ParallelControlError("LEASE_EXTERNALIZED_ROWS_DIGEST", rows.sha256)
        write_bytes_atomic(path, content)

    def expand(
        self, execution: Mapping[str, Any] | None,
    ) -> tuple[Mapping[str, Any] | None, bool]:
        """Stored form -> the in-memory execution; also whether any table was externalized."""
        if not isinstance(execution, Mapping):
            return execution, False
        _assert_no_stray_marker(execution)
        used = False

        def restore(value: Any) -> Any:
            nonlocal used
            marker = _marker_payload(value)
            if marker is None:
                return value
            used = True
            return self.read(*marker)

        expanded, _changed = _rewrite_rows(execution, restore)
        return expanded, used


def _expand_stored_execution(
    execution: Any, blobs: ExternalizedRowsReader | None,
) -> tuple[Any, bool]:
    if not isinstance(execution, Mapping):
        return execution, False
    if blobs is None:
        _assert_no_stray_marker(execution)

        def probe(value: Any) -> Any:
            if _marker_payload(value) is not None:
                raise ParallelControlError("LEASE_EXTERNALIZED_ROWS_UNAVAILABLE", "no reader")
            return value
        _rewrite_rows(execution, probe)
        return execution, False
    return blobs.expand(execution)


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
    externalized = False
    if lease.execution is not None:
        # DEVX-022 S3b: every new event is written in the externalized stored form. Qualifying
        # tables get their digest once here and are shared by the stored form and the blob write.
        # A table that is already an ExternalizedRows (e.g. a head taken from replay) is kept as
        # is, so the decision is "does a qualifying table exist", not "did adoption replace one".
        adopted, replaced = _rewrite_rows(lease.execution, _adopt_rows)
        if replaced:
            lease = replace(lease, execution=adopted)
        externalized = bool(_qualifying_rows(lease.execution))
    prototype = LeaseEvent(
        event_id="",
        lease=lease,
        previous_event_id=previous_event_id,
        from_state=from_state,
        to_state=to_state,
        occurred_at=occurred_at.isoformat(),
        actor=actor,
        reason_codes=reason_codes,
        rows_externalized=externalized,
    )
    return replace(prototype, event_id=f"lease-event-{_canonical_sha256(prototype._body())[:20]}")


def parse_lease_event(
    payload: Mapping[str, Any], *, blobs: ExternalizedRowsReader | None = None,
) -> LeaseEvent:
    """Parse one STORED event. Externalized tables need the blob reader of the store
    (DEVX-022 S3b); without one, an event that names a blob fails closed instead of being misread.
    """
    event_id = _text(payload.get("event_id"), "event_id")
    lease_payload = _mapping(payload.get("lease"), "lease")
    execution_payload, externalized = _expand_stored_execution(
        lease_payload.get("execution"), blobs,
    )
    resource_payloads = lease_payload.get("resources")
    if not isinstance(resource_payloads, list):
        raise ParallelControlError("LEASE_RESOURCES", "resources must be a list")
    resources = tuple(
        sorted(
            ResourceClaim(
                kind=_text(_mapping(item, "resource").get("kind"), "kind"),
                resource_id=_text(_mapping(item, "resource").get("resource_id"), "resource_id"),
                access=ResourceAccess(cast(str, _mapping(item, "resource").get("access"))),
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
        execution=execution_payload,
    )
    if lease.execution is not None:
        from ai_trading_system.platform.architecture.workflow_coordination import validate_execution

        validate_execution(lease)
    if lease.execution is None:
        event_schema, lease_schema = LEASE_EVENT_SCHEMA_VERSION, LEASE_SCHEMA_VERSION
    elif externalized:
        event_schema, lease_schema = LEASE_EVENT_V3_SCHEMA_VERSION, LEASE_V3_SCHEMA_VERSION
    else:
        event_schema, lease_schema = "execution_lease_event.v2", "execution_lease.v2"
    if (
        payload.get("schema_version") != event_schema
        or lease_payload.get("schema_version") != lease_schema
    ):
        raise ParallelControlError("LEASE_EVENT_SCHEMA", event_id)
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
        rows_externalized=externalized,
    )
    # The id is the hash of the stored body: the externalized form re-compacts from the shared,
    # digest-carrying tables, so an embedded (historical) event is hashed exactly as before.
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


def _repository_paths_overlap(first: str, second: str) -> bool:
    left = tuple(part.casefold() for part in Path(first).parts)
    right = tuple(part.casefold() for part in Path(second).parts)
    common = min(len(left), len(right))
    return left[:common] == right[:common]


def _lease_expiry(lease: ExecutionLease) -> datetime:
    if lease.expires_at is None:
        raise ParallelControlError("LEASE_EXPIRY_MISSING", lease.lease_id)
    try:
        parsed = datetime.fromisoformat(lease.expires_at)
    except ValueError as exc:
        raise ParallelControlError("LEASE_EXPIRY_INVALID", lease.lease_id) from exc
    return _utc(parsed)


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
