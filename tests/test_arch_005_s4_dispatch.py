from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from ai_trading_system.platform.architecture.parallel_control_dispatch import (
    ControlledThreeLaneDispatcher,
    validate_controlled_dispatch_report,
)
from ai_trading_system.platform.architecture.parallel_control_kernel import (
    TaskControlRecord,
    load_parallel_control_policy,
)
from ai_trading_system.platform.architecture.parallel_control_scheduler import (
    load_pilot_spec,
    run_shadow_governance_cycles,
)
from ai_trading_system.platform.artifacts import write_json_atomic

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = PROJECT_ROOT / "config/architecture/arch_005_parallel_control_policy.yaml"
PILOT_PATH = PROJECT_ROOT / "inputs/architecture/arch_005_s2_s4_pilot.yaml"
BASE_COMMIT = "a" * 40


def _domain_adapter(task: TaskControlRecord) -> dict[str, object]:
    return {
        "status": "PASS",
        "validated_task": task.task_id,
        "production_effect": "none",
        "broker_action": "none",
    }


def _coordinator_adapter(results: object) -> dict[str, object]:
    rows = list(results)
    return {
        "status": "PASS",
        "integrated_change_ids": sorted(row.change_id for row in rows),
        "all_evidence_bound": all(row.evidence_binding_status == "PASS" for row in rows),
        "production_effect": "none",
        "broker_action": "none",
    }


def _run(tmp_path: Path) -> tuple[object, object, Path]:
    policy = load_parallel_control_policy(POLICY_PATH)
    spec = load_pilot_spec(PILOT_PATH, current_base_commit=BASE_COMMIT, policy=policy)
    audit = run_shadow_governance_cycles(
        spec,
        policy=policy,
        current_base_commit=BASE_COMMIT,
        observed_statuses={},
    )
    dispatcher = ControlledThreeLaneDispatcher(
        project_root=tmp_path,
        runtime_root=tmp_path / "outputs/architecture/arch_005_s4",
        policy=policy,
    )
    actors = {
        "arch-005-s4-engineering-validation": "engineering-agent",
        "arch-005-s4-research-evidence-validation": "research-agent",
        "arch-005-s4-integration-coordinator": "integration-coordinator",
    }
    adapters = {
        "arch-005-s4-engineering-validation": _domain_adapter,
        "arch-005-s4-research-evidence-validation": _domain_adapter,
    }
    report = dispatcher.run(
        spec,
        audit=audit,
        current_base_commit=BASE_COMMIT,
        domain_adapters=adapters,
        coordinator_adapter=_coordinator_adapter,
        actors=actors,
        started_at=datetime(2026, 7, 20, 4, 0, tzinfo=UTC),
    )
    return (
        report,
        policy,
        tmp_path / "outputs/architecture/arch_005_s4/controlled_dispatch_report.json",
    )


def test_controlled_three_lane_dispatch_isolates_failure_and_recovers(tmp_path: Path) -> None:
    report, _, _ = _run(tmp_path)

    assert report.status == "PASS"
    assert len(report.final_domain_results) == 2
    assert report.coordinator_result.status == "PASS"
    assert report.failure_isolation == {
        "status": "PASS",
        "injected_failure_change_id": "arch-005-s4-engineering-validation",
        "first_attempt_status": "FAIL",
        "unaffected_change_id": "arch-005-s4-research-evidence-validation",
        "unaffected_first_attempt_status": "PASS",
        "recovered_attempt": 2,
        "recovered_status": "PASS",
        "reassigned_lease_id": report.failure_isolation["reassigned_lease_id"],
        "other_lane_blocked": False,
    }
    assert report.lease_replay["active_leases"] == []
    assert report.to_dict()["task_governance_status_mutated"] is False
    assert report.to_dict()["source_of_truth"] == "LEGACY_MARKDOWN_ONLY"


def test_controlled_dispatch_report_validates_artifact_bytes(tmp_path: Path) -> None:
    _, policy, report_path = _run(tmp_path)

    validation = validate_controlled_dispatch_report(
        report_path,
        project_root=tmp_path,
        policy=policy,
    )

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0


def test_controlled_dispatch_validation_rejects_tampered_lane_artifact(tmp_path: Path) -> None:
    report, policy, report_path = _run(tmp_path)
    artifact = tmp_path / report.final_domain_results[0].artifact_path
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    payload["status"] = "TAMPERED"
    write_json_atomic(artifact, payload)

    validation = validate_controlled_dispatch_report(
        report_path,
        project_root=tmp_path,
        policy=policy,
    )

    assert validation["status"] == "FAIL"
    assert (
        next(row for row in validation["checks"] if row["check_id"] == "artifact_checksums")[
            "passed"
        ]
        is False
    )


def test_controlled_dispatch_validation_rejects_tampered_report(tmp_path: Path) -> None:
    _, policy, report_path = _run(tmp_path)
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["status"] = "TAMPERED"
    write_json_atomic(report_path, payload)

    validation = validate_controlled_dispatch_report(
        report_path,
        project_root=tmp_path,
        policy=policy,
    )

    assert validation["status"] == "FAIL"
    failed = {row["check_id"] for row in validation["checks"] if row["passed"] is False}
    assert {"dispatch_id", "status"}.issubset(failed)
