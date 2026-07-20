from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path

from ai_trading_system.platform.architecture.devex import build_architecture_fitness
from ai_trading_system.platform.architecture.parallel_control_dispatch import (
    ControlledThreeLaneDispatcher,
    LaneExecutionResult,
    validate_controlled_dispatch_report,
)
from ai_trading_system.platform.architecture.parallel_control_kernel import (
    FileExecutionLeaseStore,
    TaskControlRecord,
    load_parallel_control_policy,
)
from ai_trading_system.platform.architecture.parallel_control_scheduler import (
    load_pilot_spec,
    run_shadow_governance_cycles,
)
from ai_trading_system.platform.artifacts import write_json_atomic
from ai_trading_system.research_restart import (
    DEFAULT_RESTART_OUTPUT_ROOT,
    validate_research_restart_preflight,
)
from ai_trading_system.research_restart_decision import (
    validate_strategy_research_restart_decision,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = PROJECT_ROOT / "config/architecture/arch_005_parallel_control_policy.yaml"
PILOT_PATH = PROJECT_ROOT / "inputs/architecture/arch_005_s2_s4_pilot.yaml"
RUNTIME_ROOT = PROJECT_ROOT / "outputs/architecture/arch_005_s4"
SHADOW_AUDIT_PATH = RUNTIME_ROOT / "shadow_governance_audit.json"
DISPATCH_REPORT_PATH = RUNTIME_ROOT / "controlled_dispatch_report.json"
DISPATCH_VALIDATION_PATH = RUNTIME_ROOT / "controlled_dispatch_validation.json"
DEVEX_POLICY_PATH = PROJECT_ROOT / "config/architecture/devex_ownership_policy.yaml"
MODULE_MANIFEST_PATH = PROJECT_ROOT / "inputs/architecture/arch_004e_module_manifest.yaml"
TEST_MANIFEST_PATH = PROJECT_ROOT / "inputs/architecture/arch_004e_test_manifest.yaml"
AGGREGATE_INDEX_PATH = PROJECT_ROOT / "inputs/architecture/arch_004e_aggregate_shadow_index.yaml"
DEPENDENCY_POLICY_PATH = PROJECT_ROOT / "config/architecture/arch_004c_dependency_policy.yaml"
DIRECT_WRITER_BASELINE_PATH = (
    PROJECT_ROOT / "inputs/architecture/arch_004c_direct_writer_baseline.yaml"
)


def main() -> int:
    parser = argparse.ArgumentParser(description="ARCH-005 S2-S4 parallel control plane")
    subparsers = parser.add_subparsers(dest="command", required=True)
    shadow = subparsers.add_parser("shadow-audit")
    shadow.add_argument("--base-commit")
    run = subparsers.add_parser("run-pilot")
    run.add_argument("--base-commit")
    run.add_argument("--started-at", required=True)
    validate = subparsers.add_parser("validate-pilot")
    validate.add_argument("--base-commit")
    args = parser.parse_args()
    base_commit = args.base_commit or _git_head()
    if args.command == "shadow-audit":
        payload = _shadow_audit(base_commit)
        write_json_atomic(SHADOW_AUDIT_PATH, payload)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0 if payload["status"] == "PASS" else 1
    if args.command == "run-pilot":
        report = _run_pilot(
            base_commit=base_commit, started_at=datetime.fromisoformat(args.started_at)
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["status"] == "PASS" else 1
    validation = _validate_pilot(base_commit=base_commit)
    write_json_atomic(DISPATCH_VALIDATION_PATH, validation)
    print(json.dumps(validation, ensure_ascii=False, indent=2))
    return 0 if validation["status"] == "PASS" else 1


def _shadow_audit(base_commit: str) -> dict[str, object]:
    policy = load_parallel_control_policy(POLICY_PATH)
    spec = load_pilot_spec(PILOT_PATH, current_base_commit=base_commit, policy=policy)
    return run_shadow_governance_cycles(
        spec,
        policy=policy,
        current_base_commit=base_commit,
        observed_statuses={},
    ).to_dict()


def _run_pilot(*, base_commit: str, started_at: datetime) -> dict[str, object]:
    policy = load_parallel_control_policy(POLICY_PATH)
    spec = load_pilot_spec(PILOT_PATH, current_base_commit=base_commit, policy=policy)
    audit = run_shadow_governance_cycles(
        spec,
        policy=policy,
        current_base_commit=base_commit,
        observed_statuses={},
    )
    write_json_atomic(SHADOW_AUDIT_PATH, audit.to_dict())
    run_id = _run_id(base_commit=base_commit, started_at=started_at)
    dispatcher = ControlledThreeLaneDispatcher(
        project_root=PROJECT_ROOT,
        runtime_root=RUNTIME_ROOT / "runs" / run_id,
        policy=policy,
    )
    report = dispatcher.run(
        spec,
        audit=audit,
        current_base_commit=base_commit,
        domain_adapters={
            "arch-005-s4-engineering-validation": _engineering_adapter,
            "arch-005-s4-research-evidence-validation": _research_adapter,
        },
        coordinator_adapter=_coordinator_adapter,
        actors={
            "arch-005-s4-engineering-validation": "engineering-agent",
            "arch-005-s4-research-evidence-validation": "research-agent",
            "arch-005-s4-integration-coordinator": "integration-coordinator",
        },
        started_at=started_at,
    )
    return report.to_dict()


def _validate_pilot(*, base_commit: str) -> dict[str, object]:
    policy = load_parallel_control_policy(POLICY_PATH)
    spec = load_pilot_spec(PILOT_PATH, current_base_commit=base_commit, policy=policy)
    static = validate_controlled_dispatch_report(
        DISPATCH_REPORT_PATH,
        project_root=PROJECT_ROOT,
        policy=policy,
    )
    report = _mapping(json.loads(DISPATCH_REPORT_PATH.read_text(encoding="utf-8")))
    checks = list(static["checks"])

    def check(check_id: str, passed: bool) -> None:
        checks.append({"check_id": check_id, "passed": passed})

    audit = run_shadow_governance_cycles(
        spec,
        policy=policy,
        current_base_commit=base_commit,
        observed_statuses={},
    ).to_dict()
    check("shadow_audit_recomputed", report.get("shadow_governance_audit") == audit)
    check("shadow_audit_file", json.loads(SHADOW_AUDIT_PATH.read_text(encoding="utf-8")) == audit)
    lease_store_path = (PROJECT_ROOT / str(report.get("lease_store_path"))).resolve()
    check("lease_store_contained", lease_store_path.is_relative_to(PROJECT_ROOT))
    lease_replay = (
        FileExecutionLeaseStore(
            lease_store_path,
            policy=policy,
        )
        .replay()
        .to_dict()
    )
    check("lease_replay_recomputed", report.get("lease_replay") == lease_replay)
    final_rows = report.get("final_domain_results")
    by_change = (
        {str(row.get("change_id")): _mapping(row) for row in final_rows if isinstance(row, Mapping)}
        if isinstance(final_rows, list)
        else {}
    )
    engineering = _engineering_adapter(
        spec.task_by_change_id()["arch-005-s4-engineering-validation"]
    )
    research = _research_adapter(
        spec.task_by_change_id()["arch-005-s4-research-evidence-validation"]
    )
    check(
        "engineering_adapter_recomputed",
        _mapping(by_change.get("arch-005-s4-engineering-validation", {})).get("payload")
        == engineering,
    )
    check(
        "research_adapter_recomputed",
        _mapping(by_change.get("arch-005-s4-research-evidence-validation", {})).get("payload")
        == research,
    )
    check("base_commit", report.get("current_base_commit") == base_commit)
    passed = all(bool(row["passed"]) for row in checks)
    return {
        "schema_version": "arch_005_s2_s4_pilot_validation.v1",
        "status": "PASS" if passed else "FAIL",
        "dispatch_id": report.get("dispatch_id"),
        "policy_id": policy.policy_id,
        "policy_version": policy.policy_version,
        "pilot_id": spec.pilot_id,
        "checks": checks,
        "failed_check_count": sum(1 for row in checks if not row["passed"]),
        "source_commit": base_commit,
        "policy_sha256": _sha256(POLICY_PATH),
        "pilot_spec_sha256": _sha256(PILOT_PATH),
        "task_governance_status_mutated": False,
        "canonical_source_cutover": False,
        "source_of_truth": "LEGACY_MARKDOWN_ONLY",
        "production_effect": "none",
        "broker_action": "none",
    }


def _engineering_adapter(task: TaskControlRecord) -> dict[str, object]:
    fitness = build_architecture_fitness(
        project_root=PROJECT_ROOT,
        policy_path=DEVEX_POLICY_PATH,
        module_manifest_path=MODULE_MANIFEST_PATH,
        test_manifest_path=TEST_MANIFEST_PATH,
        aggregate_index_path=AGGREGATE_INDEX_PATH,
        dependency_policy_path=DEPENDENCY_POLICY_PATH,
        direct_writer_baseline_path=DIRECT_WRITER_BASELINE_PATH,
    )
    return {
        "status": fitness["status"],
        "validated_task": task.task_id,
        "module_count": fitness["module_count"],
        "test_file_count": fitness["test_file_count"],
        "violation_count": fitness["violation_count"],
        "direct_writer_violation_count": fitness["dependency_gate"]["violation_count"],
        "generated_manifests_mutated": False,
        "task_governance_status_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _research_adapter(task: TaskControlRecord) -> dict[str, object]:
    r0 = validate_research_restart_preflight(
        artifact_path=DEFAULT_RESTART_OUTPUT_ROOT / "strategy_research_restart_preflight.json"
    )
    r2 = validate_strategy_research_restart_decision()
    passed = r0["status"] == "PASS" and r2["status"] == "PASS"
    return {
        "status": "PASS" if passed else "FAIL",
        "validated_task": task.task_id,
        "r0_status": r0["status"],
        "r0_failed_check_count": r0["failed_check_count"],
        "r2_status": r2["status"],
        "r2_failed_check_count": r2["failed_check_count"],
        "r2_decision": r2["decision"],
        "candidate_expansion_allowed": False,
        "parameter_search_allowed": False,
        "task_governance_status_mutated": False,
        "strategy_logic_changed": False,
        "strategy_threshold_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _coordinator_adapter(results: Sequence[LaneExecutionResult]) -> dict[str, object]:
    return {
        "status": (
            "PASS"
            if len(results) == 2
            and all(result.status == "PASS" for result in results)
            and all(result.evidence_binding_status == "PASS" for result in results)
            else "FAIL"
        ),
        "integrated_change_ids": sorted(result.change_id for result in results),
        "evidence_binding_statuses": {
            result.change_id: result.evidence_binding_status for result in results
        },
        "merge_order": [
            "arch-005-s4-engineering-validation",
            "arch-005-s4-research-evidence-validation",
            "arch-005-s4-integration-coordinator",
        ],
        "task_governance_status_mutated": False,
        "generated_task_view_written": False,
        "canonical_source_cutover": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _git_head() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    value = result.stdout.strip()
    if len(value) != 40 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("git HEAD must be a full lowercase commit")
    return value


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _run_id(*, base_commit: str, started_at: datetime) -> str:
    identity = f"{base_commit}|{started_at.isoformat()}|arch-005-s4"
    return f"run-{hashlib.sha256(identity.encode()).hexdigest()[:16]}"


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError("expected mapping")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
