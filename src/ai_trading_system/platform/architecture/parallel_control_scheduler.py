from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ai_trading_system.platform.architecture.parallel_control import (
    LaneRole,
    ParallelControlError,
    detect_change_conflicts,
    parse_change_manifest,
)
from ai_trading_system.platform.architecture.parallel_control_kernel import (
    ExecutionLease,
    ParallelControlPolicy,
    ReadinessDecision,
    TaskControlRecord,
    TaskDependency,
    evaluate_task_readiness,
    leases_conflict,
    manifest_resource_claims,
    parse_task_dependency,
    validate_dependency_graph,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PILOT_SCHEMA_VERSION = "arch_005_s2_s4_pilot.v1"
SCHEDULER_SCHEMA_VERSION = "scheduler_decision.v1"
GOVERNANCE_AUDIT_SCHEMA_VERSION = "shadow_scheduler_governance_audit.v1"


@dataclass(frozen=True)
class PilotSpec:
    pilot_id: str
    policy_id: str
    tasks: tuple[TaskControlRecord, ...]
    dependencies: tuple[TaskDependency, ...]
    governance_cycles: tuple[str, ...]
    failure_recovery: Mapping[str, object]
    safety: Mapping[str, object]

    def task_by_id(self) -> dict[str, TaskControlRecord]:
        return {task.task_id: task for task in self.tasks}

    def task_by_change_id(self) -> dict[str, TaskControlRecord]:
        return {task.manifest.change_id: task for task in self.tasks}


@dataclass(frozen=True)
class SchedulerDecision:
    status: str
    snapshot_sha256: str
    policy_version: str
    current_base_commit: str
    capacity: int
    selected: tuple[dict[str, object], ...]
    not_selected: tuple[dict[str, object], ...]
    alternatives: tuple[str, ...]
    readiness: tuple[ReadinessDecision, ...]
    active_lease_ids: tuple[str, ...]

    def _body(self) -> dict[str, object]:
        return {
            "schema_version": SCHEDULER_SCHEMA_VERSION,
            "status": self.status,
            "snapshot_sha256": self.snapshot_sha256,
            "policy_version": self.policy_version,
            "current_base_commit": self.current_base_commit,
            "capacity": self.capacity,
            "selected": list(self.selected),
            "not_selected": list(self.not_selected),
            "reason_codes": sorted(
                {
                    str(code)
                    for row in (*self.selected, *self.not_selected)
                    for code in _reason_codes(row)
                }
            ),
            "alternatives": list(self.alternatives),
            "readiness": [item.to_dict() for item in self.readiness],
            "active_lease_ids": list(self.active_lease_ids),
            "dispatch_allowed": False,
            "lease_acquisition_allowed": False,
            "task_governance_status_mutated": False,
            "source_of_truth": "LEGACY_MARKDOWN_ONLY",
            "production_effect": "none",
            "broker_action": "none",
        }

    @property
    def decision_id(self) -> str:
        return f"scheduler-decision-{_canonical_sha256(self._body())[:20]}"

    def to_dict(self) -> dict[str, object]:
        return {"decision_id": self.decision_id, **self._body()}


@dataclass(frozen=True)
class ShadowGovernanceAudit:
    status: str
    pilot_id: str
    policy_version: str
    cycle_ids: tuple[str, ...]
    decision_ids: tuple[str, ...]
    decision_byte_sha256: tuple[str, ...]
    differences: tuple[dict[str, object], ...]
    decision: SchedulerDecision

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": GOVERNANCE_AUDIT_SCHEMA_VERSION,
            "status": self.status,
            "pilot_id": self.pilot_id,
            "policy_version": self.policy_version,
            "cycle_ids": list(self.cycle_ids),
            "decision_ids": list(self.decision_ids),
            "decision_byte_sha256": list(self.decision_byte_sha256),
            "differences": list(self.differences),
            "decision": self.decision.to_dict(),
            "dispatch_allowed": False,
            "lease_acquisition_allowed": False,
            "task_governance_status_mutated": False,
            "source_of_truth": "LEGACY_MARKDOWN_ONLY",
            "production_effect": "none",
        }


def load_pilot_spec(
    path: Path,
    *,
    current_base_commit: str,
    policy: ParallelControlPolicy,
) -> PilotSpec:
    payload = _mapping(safe_load_yaml_path(path), "pilot")
    if payload.get("schema_version") != PILOT_SCHEMA_VERSION:
        raise ParallelControlError("PILOT_SCHEMA", str(payload.get("schema_version")))
    if payload.get("base_commit_source") != "runtime_head":
        raise ParallelControlError("PILOT_BASE_SOURCE", str(payload.get("base_commit_source")))
    if payload.get("policy_id") != policy.policy_id:
        raise ParallelControlError("PILOT_POLICY_BINDING", str(payload.get("policy_id")))
    raw_tasks = payload.get("tasks")
    if not isinstance(raw_tasks, list) or not raw_tasks:
        raise ParallelControlError("PILOT_TASKS", "tasks must be a non-empty list")
    tasks = tuple(
        sorted(
            (
                parse_task_control_record(
                    _mapping(item, "task"), current_base_commit=current_base_commit
                )
                for item in raw_tasks
            ),
            key=lambda item: item.task_id,
        )
    )
    if len(tasks) != len({task.task_id for task in tasks}):
        raise ParallelControlError("PILOT_DUPLICATE_TASK_ID", "task ids must be unique")
    if len(tasks) != len({task.manifest.change_id for task in tasks}):
        raise ParallelControlError("PILOT_DUPLICATE_CHANGE_ID", "change ids must be unique")
    if set(task.task_id for task in tasks) != set(policy.allowlisted_task_ids):
        raise ParallelControlError("PILOT_ALLOWLIST_BINDING", "pilot task set differs from policy")
    raw_dependencies = payload.get("dependencies")
    if not isinstance(raw_dependencies, list):
        raise ParallelControlError("PILOT_DEPENDENCIES", "dependencies must be a list")
    dependencies = tuple(
        sorted(parse_task_dependency(_mapping(item, "dependency")) for item in raw_dependencies)
    )
    graph = validate_dependency_graph([task.task_id for task in tasks], dependencies)
    if graph.status != "PASS":
        raise ParallelControlError("PILOT_DEPENDENCY_GRAPH", graph.issues[0].code)
    cycles = _strings(payload.get("governance_cycles"), "governance_cycles")
    if len(cycles) < 2:
        raise ParallelControlError("PILOT_GOVERNANCE_CYCLES", "at least two cycles are required")
    failure = _mapping(payload.get("failure_recovery"), "failure_recovery")
    if failure.get("injected_change_id") != policy.failure_injection_change_id:
        raise ParallelControlError("PILOT_FAILURE_BINDING", str(failure.get("injected_change_id")))
    if failure.get("fail_first_attempt") is not True:
        raise ParallelControlError(
            "PILOT_FAILURE_INJECTION_REQUIRED", "fail_first_attempt must be true"
        )
    safety = _mapping(payload.get("safety"), "safety")
    if safety.get("production_effect") != "none" or safety.get("broker_action") != "none":
        raise ParallelControlError("PILOT_SAFETY", "production and broker must be none")
    if any(
        safety.get(field) is not False
        for field in (
            "task_governance_status_mutation_allowed",
            "generated_task_view_write_allowed",
            "strategy_logic_change_allowed",
        )
    ):
        raise ParallelControlError("PILOT_UNSAFE_PERMISSION", "pilot safety flags must be false")
    return PilotSpec(
        pilot_id=_text(payload.get("pilot_id"), "pilot_id"),
        policy_id=policy.policy_id,
        tasks=tasks,
        dependencies=dependencies,
        governance_cycles=cycles,
        failure_recovery=failure,
        safety=safety,
    )


def build_shadow_scheduler_decision(
    spec: PilotSpec,
    *,
    policy: ParallelControlPolicy,
    current_base_commit: str,
    observed_statuses: Mapping[str, str],
    active_leases: Sequence[ExecutionLease] = (),
) -> SchedulerDecision:
    graph = validate_dependency_graph([task.task_id for task in spec.tasks], spec.dependencies)
    readiness = tuple(
        evaluate_task_readiness(
            task,
            dependencies=spec.dependencies,
            observed_statuses=observed_statuses,
            graph_report=graph,
            current_base_commit=current_base_commit,
            policy=policy,
        )
        for task in spec.tasks
    )
    readiness_by_change = {item.change_id: item for item in readiness}
    priority_rank = {priority: index for index, priority in enumerate(policy.priority_order)}
    ordered = sorted(
        spec.tasks,
        key=lambda task: (
            priority_rank.get(task.priority, len(priority_rank)),
            task.task_id,
            task.manifest.change_id,
        ),
    )
    selected_tasks: list[TaskControlRecord] = []
    selected: list[dict[str, object]] = []
    not_selected: list[dict[str, object]] = []
    not_selected_change_ids: list[str] = []
    for task in ordered:
        decision = readiness_by_change[task.manifest.change_id]
        reasons: list[str] = []
        if decision.status != "READY":
            reasons.extend(decision.reason_codes)
        if task.manifest.lane_role is not LaneRole.DOMAIN:
            reasons.append("S3_DOMAIN_SELECTION_ONLY")
        if len(selected_tasks) >= policy.max_parallel_domain_lanes:
            reasons.append("DOMAIN_CAPACITY_REACHED")
        for existing in selected_tasks:
            if detect_change_conflicts([task.manifest, existing.manifest]):
                reasons.append(f"CONFLICT_WITH_SELECTED:{existing.manifest.change_id}")
        candidate_lease = _candidate_lease(task, policy=policy)
        for active in active_leases:
            if leases_conflict(candidate_lease, active):
                reasons.append(f"ACTIVE_LEASE_CONFLICT:{active.lease_id}")
        if reasons:
            not_selected.append(
                {
                    "task_id": task.task_id,
                    "change_id": task.manifest.change_id,
                    "reason_codes": sorted(set(reasons)),
                    "manifest_sha256": task.manifest.sha256,
                }
            )
            not_selected_change_ids.append(task.manifest.change_id)
            continue
        selected_tasks.append(task)
        selected.append(
            {
                "task_id": task.task_id,
                "change_id": task.manifest.change_id,
                "lane_id": f"domain-{len(selected_tasks):02d}",
                "reason_codes": ["SELECTED_READY_NON_CONFLICTING"],
                "manifest_sha256": task.manifest.sha256,
            }
        )
    snapshot = {
        "pilot_id": spec.pilot_id,
        "policy_version": policy.policy_version,
        "current_base_commit": current_base_commit,
        "tasks": [task.to_dict() for task in spec.tasks],
        "dependencies": [item.to_dict() for item in spec.dependencies],
        "observed_statuses": dict(sorted(observed_statuses.items())),
        "active_lease_ids": sorted(lease.lease_id for lease in active_leases),
    }
    status = "PASS" if selected else "NO_READY_TASK"
    return SchedulerDecision(
        status=status,
        snapshot_sha256=_canonical_sha256(snapshot),
        policy_version=policy.policy_version,
        current_base_commit=current_base_commit,
        capacity=policy.max_parallel_domain_lanes,
        selected=tuple(selected),
        not_selected=tuple(not_selected),
        alternatives=tuple(not_selected_change_ids),
        readiness=readiness,
        active_lease_ids=tuple(sorted(lease.lease_id for lease in active_leases)),
    )


def run_shadow_governance_cycles(
    spec: PilotSpec,
    *,
    policy: ParallelControlPolicy,
    current_base_commit: str,
    observed_statuses: Mapping[str, str],
    active_leases: Sequence[ExecutionLease] = (),
) -> ShadowGovernanceAudit:
    decisions = tuple(
        build_shadow_scheduler_decision(
            spec,
            policy=policy,
            current_base_commit=current_base_commit,
            observed_statuses=observed_statuses,
            active_leases=active_leases,
        )
        for _ in spec.governance_cycles
    )
    encoded = tuple(_canonical_bytes(decision.to_dict()) for decision in decisions)
    hashes = tuple(hashlib.sha256(value).hexdigest() for value in encoded)
    differences: list[dict[str, object]] = []
    baseline = encoded[0]
    for cycle_id, candidate in zip(spec.governance_cycles[1:], encoded[1:], strict=True):
        if candidate != baseline:
            differences.append(
                {
                    "cycle_id": cycle_id,
                    "code": "SCHEDULER_DECISION_DRIFT",
                    "owner_disposition_required": True,
                }
            )
    status = (
        "PASS"
        if not differences and all(decision.status == "PASS" for decision in decisions)
        else "FAIL"
    )
    return ShadowGovernanceAudit(
        status=status,
        pilot_id=spec.pilot_id,
        policy_version=policy.policy_version,
        cycle_ids=spec.governance_cycles,
        decision_ids=tuple(decision.decision_id for decision in decisions),
        decision_byte_sha256=hashes,
        differences=tuple(differences),
        decision=decisions[0],
    )


def parse_task_control_record(
    payload: Mapping[str, Any], *, current_base_commit: str
) -> TaskControlRecord:
    """Parse one task row while binding its change manifest to the runtime Git base."""
    manifest_payload = dict(_mapping(payload.get("change_manifest"), "change_manifest"))
    manifest_payload.update(
        {
            "schema_version": "change_manifest.v1",
            "task_id": _text(payload.get("task_id"), "task_id"),
            "base_commit": current_base_commit,
            "production_effect": "none",
        }
    )
    manifest = parse_change_manifest(manifest_payload)
    return TaskControlRecord(
        task_id=manifest.task_id,
        title=_text(payload.get("title"), "title"),
        governance_status=_text(payload.get("governance_status"), "governance_status"),
        priority=_text(payload.get("priority"), "priority"),
        requirement_refs=_strings(payload.get("requirement_refs"), "requirement_refs"),
        acceptance_criteria=_strings(payload.get("acceptance_criteria"), "acceptance_criteria"),
        manifest=manifest,
    )


def _candidate_lease(task: TaskControlRecord, *, policy: ParallelControlPolicy) -> ExecutionLease:
    return ExecutionLease(
        lease_id=f"candidate:{task.manifest.change_id}",
        task_id=task.task_id,
        change_id=task.manifest.change_id,
        lane_id="shadow",
        actor="shadow-scheduler",
        base_commit=task.manifest.base_commit,
        change_manifest_sha256=task.manifest.sha256,
        policy_version=policy.policy_version,
        generation=1,
        previous_lease_id=None,
        state="REQUESTED",
        requested_at="shadow",
        acquired_at=None,
        expires_at=None,
        resources=manifest_resource_claims(task.manifest),
    )


def _reason_codes(row: Mapping[str, object]) -> tuple[str, ...]:
    value = row.get("reason_codes")
    if not isinstance(value, list):
        return ()
    return tuple(str(item) for item in value)


def _canonical_bytes(payload: Mapping[str, object]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _canonical_sha256(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ParallelControlError("PILOT_MAPPING_REQUIRED", field)
    return value


def _strings(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ParallelControlError("PILOT_LIST_REQUIRED", field)
    rows = tuple(_text(item, field) for item in value)
    if len(rows) != len(set(rows)):
        raise ParallelControlError("PILOT_LIST_DUPLICATE", field)
    return rows


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ParallelControlError("PILOT_TEXT_REQUIRED", field)
    return value.strip()
