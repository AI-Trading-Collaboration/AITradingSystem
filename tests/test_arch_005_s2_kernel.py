from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from ai_trading_system.platform.architecture.parallel_control import (
    ParallelControlError,
    parse_change_manifest,
)
from ai_trading_system.platform.architecture.parallel_control_kernel import (
    FileExecutionLeaseStore,
    TaskControlRecord,
    evaluate_task_readiness,
    load_parallel_control_policy,
    parse_task_dependency,
    validate_dependency_graph,
)
from ai_trading_system.platform.artifacts import write_json_atomic

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = PROJECT_ROOT / "config/architecture/arch_005_parallel_control_policy.yaml"
BASE_COMMIT = "a" * 40


def _task(
    task_id: str,
    *,
    change_id: str | None = None,
    module_ids: list[str] | None = None,
) -> TaskControlRecord:
    manifest = parse_change_manifest(
        {
            "schema_version": "change_manifest.v1",
            "change_id": change_id or f"change:{task_id}",
            "task_id": task_id,
            "lane_role": "DOMAIN",
            "base_commit": BASE_COMMIT,
            "owner": "test-owner",
            "production_effect": "none",
            "owned_paths": [f"outputs/test/{task_id}.json"],
            "shared_paths": [],
            "module_ids": module_ids or [f"module.{task_id}"],
            "contract_claims": [],
            "required_validation_tiers": ["focused"],
        }
    )
    return TaskControlRecord(
        task_id=task_id,
        title=task_id,
        governance_status="READY",
        priority="P0",
        requirement_refs=("docs/requirements/test.md",),
        acceptance_criteria=("pass",),
        manifest=manifest,
    )


def _dependency(
    dependency_id: str,
    *,
    task_id: str,
    depends_on_task_id: str,
) -> object:
    return parse_task_dependency(
        {
            "dependency_id": dependency_id,
            "task_id": task_id,
            "depends_on_task_id": depends_on_task_id,
            "edge_type": "blocks_start",
            "required_statuses": ["EXECUTION_PASS"],
            "rationale": "test dependency",
            "owner": "test-owner",
        }
    )


def test_policy_is_reviewed_and_keeps_s5_and_investment_state_closed() -> None:
    policy = load_parallel_control_policy(POLICY_PATH)

    assert policy.status == "REVIEWED_PILOT_BASELINE"
    assert policy.max_parallel_domain_lanes == 2
    assert policy.max_total_active_leases == 3
    assert policy.max_reassignments == 1
    assert policy.source_of_truth == "LEGACY_MARKDOWN_ONLY"


def test_dependency_graph_rejects_unknown_self_and_hard_cycle() -> None:
    unknown = validate_dependency_graph(
        ["task-a"],
        [_dependency("dep-unknown", task_id="task-a", depends_on_task_id="missing")],
    )
    self_edge = validate_dependency_graph(
        ["task-a"],
        [_dependency("dep-self", task_id="task-a", depends_on_task_id="task-a")],
    )
    cycle = validate_dependency_graph(
        ["task-a", "task-b"],
        [
            _dependency("dep-a", task_id="task-a", depends_on_task_id="task-b"),
            _dependency("dep-b", task_id="task-b", depends_on_task_id="task-a"),
        ],
    )

    assert unknown.status == "FAIL"
    assert unknown.issues[0].code == "DEPENDENCY_TARGET_UNKNOWN"
    assert self_edge.issues[0].code == "DEPENDENCY_SELF_EDGE"
    assert cycle.issues[0].code == "DEPENDENCY_HARD_CYCLE"


def test_readiness_requires_dependency_status_and_exact_base() -> None:
    policy = load_parallel_control_policy(POLICY_PATH)
    first = _task("task-a")
    second = _task("task-b")
    dependency = _dependency("dep-b", task_id="task-b", depends_on_task_id="task-a")
    graph = validate_dependency_graph([first.task_id, second.task_id], [dependency])

    blocked = evaluate_task_readiness(
        second,
        dependencies=[dependency],
        observed_statuses={},
        graph_report=graph,
        current_base_commit=BASE_COMMIT,
        policy=policy,
    )
    ready = evaluate_task_readiness(
        second,
        dependencies=[dependency],
        observed_statuses={"task-a": "EXECUTION_PASS"},
        graph_report=graph,
        current_base_commit=BASE_COMMIT,
        policy=policy,
    )

    assert blocked.status == "BLOCKED"
    assert blocked.reason_codes == ("DEPENDENCY_UNSATISFIED:dep-b",)
    assert ready.status == "READY"
    assert ready.to_dict()["dispatch_allowed"] is False


def test_lease_store_acquire_release_replay_is_idempotent(tmp_path: Path) -> None:
    policy = load_parallel_control_policy(POLICY_PATH)
    task = _task("ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE")
    graph = validate_dependency_graph([task.task_id], [])
    readiness = evaluate_task_readiness(
        task,
        dependencies=[],
        observed_statuses={},
        graph_report=graph,
        current_base_commit=BASE_COMMIT,
        policy=policy,
    )
    store = FileExecutionLeaseStore(tmp_path / "leases", policy=policy)
    now = datetime(2026, 7, 20, tzinfo=UTC)

    first = store.acquire(
        task=task,
        readiness=readiness,
        lane_id="domain-01",
        actor="engineering-agent",
        current_base_commit=BASE_COMMIT,
        now=now,
    )
    replayed = store.acquire(
        task=task,
        readiness=readiness,
        lane_id="domain-01",
        actor="engineering-agent",
        current_base_commit=BASE_COMMIT,
        now=now,
    )
    released = store.release(
        first.lease.lease_id,
        actor="engineering-agent",
        now=now + timedelta(seconds=1),
        evidence_refs=("outputs/test/evidence.json",),
    )
    replay = store.replay()

    assert first.status == "ACTIVE"
    assert replayed.idempotent_replay is True
    assert released.state == "RELEASED"
    assert replay.status == "PASS"
    assert replay.active_leases == ()
    assert replay.event_count == 3


def test_conflicting_lease_is_blocked_before_execution(tmp_path: Path) -> None:
    policy = load_parallel_control_policy(POLICY_PATH)
    first = _task(
        "ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE",
        change_id="change-first",
        module_ids=["module.shared"],
    )
    second = _task(
        "TRADING-2446_to_2448_RESEARCH_RESTART_R0_R2",
        change_id="change-second",
        module_ids=["module.shared"],
    )
    graph = validate_dependency_graph([first.task_id, second.task_id], [])
    store = FileExecutionLeaseStore(tmp_path / "leases", policy=policy)
    now = datetime(2026, 7, 20, tzinfo=UTC)

    first_result = store.acquire(
        task=first,
        readiness=evaluate_task_readiness(
            first,
            dependencies=[],
            observed_statuses={},
            graph_report=graph,
            current_base_commit=BASE_COMMIT,
            policy=policy,
        ),
        lane_id="domain-01",
        actor="engineering-agent",
        current_base_commit=BASE_COMMIT,
        now=now,
    )
    second_result = store.acquire(
        task=second,
        readiness=evaluate_task_readiness(
            second,
            dependencies=[],
            observed_statuses={},
            graph_report=graph,
            current_base_commit=BASE_COMMIT,
            policy=policy,
        ),
        lane_id="domain-02",
        actor="research-agent",
        current_base_commit=BASE_COMMIT,
        now=now,
    )

    assert first_result.status == "ACTIVE"
    assert second_result.status == "BLOCKED"
    assert second_result.reason_codes[0].startswith("LEASE_RESOURCE_CONFLICT:")
    assert len(store.replay().active_leases) == 1


def test_expired_lease_can_be_reassigned_once_and_replayed(tmp_path: Path) -> None:
    policy = load_parallel_control_policy(POLICY_PATH)
    task = _task("ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE")
    graph = validate_dependency_graph([task.task_id], [])
    readiness = evaluate_task_readiness(
        task,
        dependencies=[],
        observed_statuses={},
        graph_report=graph,
        current_base_commit=BASE_COMMIT,
        policy=policy,
    )
    store = FileExecutionLeaseStore(tmp_path / "leases", policy=policy)
    now = datetime(2026, 7, 20, tzinfo=UTC)
    acquired = store.acquire(
        task=task,
        readiness=readiness,
        lane_id="domain-01",
        actor="engineering-agent",
        current_base_commit=BASE_COMMIT,
        now=now,
    )
    store.expire(
        acquired.lease.lease_id,
        actor="engineering-agent",
        now=now + timedelta(seconds=1),
        reason_code="EXECUTION_FAILED",
    )
    reassigned = store.reassign(
        acquired.lease.lease_id,
        task=task,
        readiness=readiness,
        lane_id="domain-01",
        actor="recovery-agent",
        current_base_commit=BASE_COMMIT,
        now=now + timedelta(seconds=2),
    )
    store.release(
        reassigned.lease.lease_id,
        actor="recovery-agent",
        now=now + timedelta(seconds=3),
        evidence_refs=("outputs/test/recovered.json",),
    )

    replay = store.replay()
    assert replay.status == "PASS"
    assert replay.active_leases == ()
    assert {lease.state for lease in replay.lease_heads} == {"REASSIGNED", "RELEASED"}
    assert reassigned.lease.generation == 2


def test_lease_event_tamper_fails_replay(tmp_path: Path) -> None:
    policy = load_parallel_control_policy(POLICY_PATH)
    task = _task("ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE")
    graph = validate_dependency_graph([task.task_id], [])
    readiness = evaluate_task_readiness(
        task,
        dependencies=[],
        observed_statuses={},
        graph_report=graph,
        current_base_commit=BASE_COMMIT,
        policy=policy,
    )
    store = FileExecutionLeaseStore(tmp_path / "leases", policy=policy)
    acquired = store.acquire(
        task=task,
        readiness=readiness,
        lane_id="domain-01",
        actor="engineering-agent",
        current_base_commit=BASE_COMMIT,
        now=datetime(2026, 7, 20, tzinfo=UTC),
    )
    event_path = next((tmp_path / "leases/events" / acquired.lease.lease_id).glob("*.json"))
    payload = json.loads(event_path.read_text(encoding="utf-8"))
    payload["reason_codes"] = ["TAMPERED"]
    write_json_atomic(event_path, payload)

    replay = store.replay()
    assert replay.status == "FAIL"
    assert replay.issues[0].code == "LEASE_EVENT_INVALID"


def test_arbiter_rejects_unreviewed_actor(tmp_path: Path) -> None:
    policy = load_parallel_control_policy(POLICY_PATH)
    task = _task("ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE")
    graph = validate_dependency_graph([task.task_id], [])
    readiness = evaluate_task_readiness(
        task,
        dependencies=[],
        observed_statuses={},
        graph_report=graph,
        current_base_commit=BASE_COMMIT,
        policy=policy,
    )
    store = FileExecutionLeaseStore(tmp_path / "leases", policy=policy)

    with pytest.raises(ParallelControlError, match="LEASE_ACTOR_NOT_ALLOWLISTED"):
        store.acquire(
            task=task,
            readiness=readiness,
            lane_id="domain-01",
            actor="unknown-agent",
            current_base_commit=BASE_COMMIT,
            now=datetime(2026, 7, 20, tzinfo=UTC),
        )


def test_non_conflicting_lease_requests_can_both_become_active(tmp_path: Path) -> None:
    policy = load_parallel_control_policy(POLICY_PATH)
    tasks = [
        _task("ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE", change_id="change-a"),
        _task("TRADING-2446_to_2448_RESEARCH_RESTART_R0_R2", change_id="change-b"),
    ]
    graph = validate_dependency_graph([task.task_id for task in tasks], [])
    store = FileExecutionLeaseStore(tmp_path / "leases", policy=policy)
    now = datetime(2026, 7, 20, tzinfo=UTC)

    results = []
    for index, task in enumerate(tasks, start=1):
        results.append(
            store.acquire(
                task=task,
                readiness=evaluate_task_readiness(
                    task,
                    dependencies=[],
                    observed_statuses={},
                    graph_report=graph,
                    current_base_commit=BASE_COMMIT,
                    policy=policy,
                ),
                lane_id=f"domain-{index:02d}",
                actor="engineering-agent" if index == 1 else "research-agent",
                current_base_commit=BASE_COMMIT,
                now=now,
            )
        )

    assert [result.status for result in results] == ["ACTIVE", "ACTIVE"]
    assert len(store.replay().active_leases) == 2
