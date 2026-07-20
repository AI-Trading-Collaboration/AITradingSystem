from __future__ import annotations

from pathlib import Path

import pytest

from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
from ai_trading_system.platform.architecture.parallel_control_kernel import (
    load_parallel_control_policy,
)
from ai_trading_system.platform.architecture.parallel_control_scheduler import (
    build_shadow_scheduler_decision,
    load_pilot_spec,
    run_shadow_governance_cycles,
)
from ai_trading_system.platform.artifacts import write_text_atomic

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = PROJECT_ROOT / "config/architecture/arch_005_parallel_control_policy.yaml"
PILOT_PATH = PROJECT_ROOT / "inputs/architecture/arch_005_s2_s4_pilot.yaml"
BASE_COMMIT = "a" * 40


def test_pilot_loads_with_reviewed_policy_and_exact_runtime_base() -> None:
    policy = load_parallel_control_policy(POLICY_PATH)
    spec = load_pilot_spec(PILOT_PATH, current_base_commit=BASE_COMMIT, policy=policy)

    assert len(spec.tasks) == 3
    assert len(spec.dependencies) == 2
    assert len(spec.governance_cycles) == 2
    assert all(task.manifest.base_commit == BASE_COMMIT for task in spec.tasks)


def test_shadow_scheduler_selects_engineering_and_research_not_coordinator() -> None:
    policy = load_parallel_control_policy(POLICY_PATH)
    spec = load_pilot_spec(PILOT_PATH, current_base_commit=BASE_COMMIT, policy=policy)

    decision = build_shadow_scheduler_decision(
        spec,
        policy=policy,
        current_base_commit=BASE_COMMIT,
        observed_statuses={},
    )

    assert decision.status == "PASS"
    assert {row["change_id"] for row in decision.selected} == {
        "arch-005-s4-engineering-validation",
        "arch-005-s4-research-evidence-validation",
    }
    coordinator = next(
        row
        for row in decision.not_selected
        if row["change_id"] == "arch-005-s4-integration-coordinator"
    )
    assert "S3_DOMAIN_SELECTION_ONLY" in coordinator["reason_codes"]
    assert any(code.startswith("DEPENDENCY_UNSATISFIED:") for code in coordinator["reason_codes"])
    assert decision.to_dict()["dispatch_allowed"] is False


def test_two_governance_cycles_are_byte_identical_and_explain_all_tasks() -> None:
    policy = load_parallel_control_policy(POLICY_PATH)
    spec = load_pilot_spec(PILOT_PATH, current_base_commit=BASE_COMMIT, policy=policy)

    audit = run_shadow_governance_cycles(
        spec,
        policy=policy,
        current_base_commit=BASE_COMMIT,
        observed_statuses={},
    )

    assert audit.status == "PASS"
    assert len(set(audit.decision_ids)) == 1
    assert len(set(audit.decision_byte_sha256)) == 1
    assert audit.differences == ()
    assert len(audit.decision.selected) + len(audit.decision.not_selected) == 3
    assert audit.to_dict()["lease_acquisition_allowed"] is False


def test_pilot_loader_rejects_non_runtime_base_source(tmp_path: Path) -> None:
    policy = load_parallel_control_policy(POLICY_PATH)
    text = PILOT_PATH.read_text(encoding="utf-8").replace(
        "base_commit_source: runtime_head",
        "base_commit_source: hardcoded",
    )
    path = tmp_path / "pilot.yaml"
    write_text_atomic(path, text)

    with pytest.raises(ParallelControlError, match="PILOT_BASE_SOURCE"):
        load_pilot_spec(path, current_base_commit=BASE_COMMIT, policy=policy)
