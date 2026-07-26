from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
PREFLIGHT_PATH = (
    ROOT / "tools" / "codex_skills" / "run-governed-development" / "scripts" / "preflight.py"
)
SKILL_PATH = ROOT / "tools" / "codex_skills" / "run-governed-development" / "SKILL.md"
WORKFLOW_REFERENCE_PATH = (
    ROOT
    / "tools"
    / "codex_skills"
    / "run-governed-development"
    / "references"
    / "workflow-modes.md"
)


def _load_preflight() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "governed_development_skill_preflight",
        PREFLIGHT_PATH,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


PREFLIGHT = _load_preflight()


def test_read_only_mode_rejects_write_claims() -> None:
    blockers, serial = PREFLIGHT.evaluate_claims(
        mode="READ_ONLY",
        role="reader",
        claims={"task": ["src/example.py"]},
        coordinator_paths=[],
        contract_change=False,
    )
    assert serial == []
    assert {row["code"] for row in blockers} == {"READ_ONLY_WRITE_CLAIMS_FORBIDDEN"}


def test_single_lane_accepts_task_and_coordinator_scopes() -> None:
    blockers, serial = PREFLIGHT.evaluate_claims(
        mode="SINGLE_LANE",
        role="coordinator",
        claims={"task": ["tools/codex_skills/run-governed-development"]},
        coordinator_paths=["AGENTS.md", "docs/task_register.md"],
        contract_change=False,
    )
    assert blockers == []
    assert serial == []


def test_dual_lane_accepts_disjoint_owned_paths() -> None:
    blockers, serial = PREFLIGHT.evaluate_claims(
        mode="DUAL_LANE",
        role="coordinator",
        claims={
            "engineering": [
                "src/ai_trading_system/data/quality_capability.py",
            ],
            "strategy-evidence": [
                "src/ai_trading_system/research_framework/plugins/"
                "decision_target_capability_audit_label_foundation.py",
            ],
        },
        coordinator_paths=["docs/task_register.md"],
        contract_change=False,
    )
    assert blockers == []
    assert serial == []


@pytest.mark.parametrize(
    ("engineering_path", "strategy_path"),
    [
        ("src/shared.py", "src/shared.py"),
        ("src/shared", "src/shared/consumer.py"),
    ],
)
def test_dual_lane_rejects_exact_and_ancestor_conflicts(
    engineering_path: str,
    strategy_path: str,
) -> None:
    blockers, _ = PREFLIGHT.evaluate_claims(
        mode="DUAL_LANE",
        role="coordinator",
        claims={
            "engineering": [engineering_path],
            "strategy-evidence": [strategy_path],
        },
        coordinator_paths=[],
        contract_change=False,
    )
    assert "LANE_PATH_CONFLICT" in {row["code"] for row in blockers}


def test_dual_lane_requires_serial_contract_wave() -> None:
    blockers, serial = PREFLIGHT.evaluate_claims(
        mode="DUAL_LANE",
        role="coordinator",
        claims={
            "engineering": ["src/engineering.py"],
            "strategy-evidence": ["src/research.py"],
        },
        coordinator_paths=[],
        contract_change=True,
    )
    assert blockers == []
    assert [row["code"] for row in serial] == ["SERIAL_CONTRACT_WAVE_REQUIRED"]


def test_lane_cannot_claim_coordinator_only_path() -> None:
    blockers, _ = PREFLIGHT.evaluate_claims(
        mode="SINGLE_LANE",
        role="worker",
        claims={"task": ["docs/task_register.md"]},
        coordinator_paths=[],
        contract_change=False,
    )
    assert "COORDINATOR_ONLY_PATH_CLAIMED_BY_LANE" in {row["code"] for row in blockers}


@pytest.mark.parametrize(
    "path",
    ["../escape.py", "/absolute.py", r"C:\absolute.py", ""],
)
def test_unsafe_repository_paths_are_rejected(path: str) -> None:
    with pytest.raises(ValueError):
        PREFLIGHT.normalize_repo_path(path)


def _checkout_gate(
    **overrides: object,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    arguments: dict[str, object] = {
        "mode": "SINGLE_LANE",
        "role": "coordinator",
        "stage": "CLOSEOUT",
        "remote_action": True,
        "current_branch": "main",
        "audit_status": "PASS",
        "dirty_paths": [],
        "origin_main": "a" * 40,
        "origin_main_vs_local_main": {
            "origin_only": 0,
            "local_only": 1,
        },
    }
    arguments.update(overrides)
    return PREFLIGHT.evaluate_checkout_remote_gate(**arguments)


@pytest.mark.parametrize("local_only", [0, 1, 4])
def test_main_closeout_remote_gate_accepts_equal_or_ancestor_remote(
    local_only: int,
) -> None:
    blockers, warnings = _checkout_gate(
        origin_main_vs_local_main={
            "origin_only": 0,
            "local_only": local_only,
        }
    )
    assert blockers == []
    assert warnings == []


@pytest.mark.parametrize(
    ("overrides", "expected_code"),
    [
        ({"stage": "LANE", "remote_action": False}, "MUTATION_STAGE_ON_MAIN"),
        ({"remote_action": False}, "MAIN_CLOSEOUT_REQUIRES_REMOTE_ACTION"),
        (
            {"current_branch": "codex/task"},
            "REMOTE_ACTION_REQUIRES_MAIN",
        ),
        ({"role": "worker"}, "REMOTE_ACTION_REQUIRES_COORDINATOR"),
        (
            {"dirty_paths": ["docs/task_register.md"]},
            "REMOTE_ACTION_DIRTY_WORKTREE",
        ),
        ({"origin_main": None}, "REMOTE_MAIN_UNAVAILABLE"),
        (
            {
                "origin_main_vs_local_main": {
                    "origin_only": 1,
                    "local_only": 0,
                }
            },
            "REMOTE_MAIN_NOT_CANDIDATE_ANCESTOR",
        ),
        (
            {"mode": "READ_ONLY"},
            "REMOTE_ACTION_REQUIRES_GOVERNED_MODE",
        ),
        (
            {"stage": "START"},
            "REMOTE_ACTION_REQUIRES_CLOSEOUT_STAGE",
        ),
    ],
)
def test_closeout_remote_gate_fails_closed_with_typed_blockers(
    overrides: dict[str, object],
    expected_code: str,
) -> None:
    blockers, _ = _checkout_gate(**overrides)
    assert expected_code in {row["code"] for row in blockers}


def test_non_remote_preflight_preserves_divergence_visibility_warning() -> None:
    blockers, warnings = _checkout_gate(
        stage="START",
        remote_action=False,
        origin_main_vs_local_main={
            "origin_only": 0,
            "local_only": 2,
        },
    )
    assert blockers == []
    assert warnings == [
        {
            "code": "REMOTE_DIVERGENCE_DISCLOSED_LOCAL_ONLY",
            "detail": '{"local_only": 2, "origin_only": 0}',
        }
    ]


@pytest.mark.parametrize(
    ("stage", "expected_registered", "expected_source"),
    [
        ("START", False, "NONE"),
        ("LANE", False, "NONE"),
        ("INTEGRATION", False, "NONE"),
        ("CLOSEOUT", True, "COMPLETED_CLOSEOUT_ONLY"),
    ],
)
def test_completed_task_registration_is_closeout_only(
    stage: str,
    expected_registered: bool,
    expected_source: str,
) -> None:
    registered, source = PREFLIGHT.evaluate_task_registration(
        mode="SINGLE_LANE",
        stage=stage,
        task_id="DEVX-ARCHIVED",
        active_task_register="|DEVX-ACTIVE|IN_PROGRESS|",
        completed_task_register="|DEVX-ARCHIVED|DONE|",
    )
    assert registered is expected_registered
    assert source == expected_source


def test_active_and_read_only_task_registration_behavior_is_preserved() -> None:
    active = PREFLIGHT.evaluate_task_registration(
        mode="SINGLE_LANE",
        stage="LANE",
        task_id="DEVX-ACTIVE",
        active_task_register="|DEVX-ACTIVE|IN_PROGRESS|",
        completed_task_register="",
    )
    read_only = PREFLIGHT.evaluate_task_registration(
        mode="READ_ONLY",
        stage="START",
        task_id=None,
        active_task_register="",
        completed_task_register="",
    )
    assert active == (True, "ACTIVE")
    assert read_only == (True, "READ_ONLY")


def _base_drift(
    **overrides: object,
) -> tuple[
    list[dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
]:
    arguments: dict[str, object] = {
        "stage": "INTEGRATION",
        "current_branch": "codex/task",
        "expected_base": "a" * 40,
        "local_main": "b" * 40,
        "head": "c" * 40,
        "expected_base_is_head_ancestor": True,
        "integration_plan": None,
        "reviewed_reconciliation_plan_id": None,
    }
    arguments.update(overrides)
    return PREFLIGHT.evaluate_base_drift(**arguments)


def test_lane_continues_on_frozen_base_until_integration_boundary() -> None:
    blockers, serial, warnings = _base_drift(stage="LANE")
    assert blockers == []
    assert serial == []
    assert warnings == [
        {
            "code": "BASE_DRIFT_DEFERRED_TO_INTEGRATION_PLAN",
            "detail": f"{'a' * 40}!={'b' * 40}",
        }
    ]


def test_integration_base_drift_still_blocks_without_validated_plan() -> None:
    blockers, serial, warnings = _base_drift()
    assert serial == []
    assert warnings == []
    assert blockers == [
        {
            "code": "EXPECTED_BASE_MISMATCH",
            "detail": f"{'a' * 40}!={'b' * 40}",
        }
    ]


def test_ready_plan_unlocks_exactly_one_integration_candidate() -> None:
    plan = {
        "plan_id": "integration-revalidation-ready",
        "plan_sha256": "d" * 64,
        "frozen_base": "a" * 40,
        "lane_head": "c" * 40,
        "latest_main": "b" * 40,
        "decision": "READY_FOR_SINGLE_INTEGRATION_CANDIDATE",
        "candidate_creation_allowed": True,
    }
    blockers, serial, warnings = _base_drift(integration_plan=plan)
    assert blockers == []
    assert serial == []
    assert warnings == []


@pytest.mark.parametrize(
    ("decision", "expected_kind", "expected_code"),
    [
        (
            "RECONCILIATION_REQUIRED",
            "blocker",
            "BASE_DRIFT_RECONCILIATION_REQUIRED",
        ),
        (
            "SERIAL_CONTRACT_WAVE_REQUIRED",
            "serial",
            "SERIAL_CONTRACT_WAVE_REQUIRED",
        ),
        ("BLOCKED", "blocker", "INTEGRATION_REVALIDATION_NOT_READY"),
    ],
)
def test_non_ready_drift_plans_remain_typed_stop_conditions(
    decision: str,
    expected_kind: str,
    expected_code: str,
) -> None:
    plan = {
        "plan_id": "integration-revalidation-stop",
        "plan_sha256": "d" * 64,
        "frozen_base": "a" * 40,
        "lane_head": "c" * 40,
        "latest_main": "b" * 40,
        "decision": decision,
        "candidate_creation_allowed": False,
    }
    blockers, serial, warnings = _base_drift(integration_plan=plan)
    assert warnings == []
    selected = serial if expected_kind == "serial" else blockers
    assert expected_code in {row["code"] for row in selected}


def test_exact_reviewed_reconciliation_id_keeps_lane_without_rebuild() -> None:
    plan = {
        "plan_id": "integration-revalidation-reconcile",
        "plan_sha256": "d" * 64,
        "frozen_base": "a" * 40,
        "lane_head": "c" * 40,
        "latest_main": "b" * 40,
        "decision": "RECONCILIATION_REQUIRED",
        "candidate_creation_allowed": False,
        "reviewed_reconciliation_required": True,
    }
    blockers, serial, warnings = _base_drift(
        integration_plan=plan,
        reviewed_reconciliation_plan_id=plan["plan_id"],
    )
    assert blockers == []
    assert serial == []
    assert warnings == [
        {
            "code": "REVIEWED_BASE_DRIFT_RECONCILIATION",
            "detail": plan["plan_id"],
        }
    ]


def test_drift_plan_must_bind_exact_lane_and_latest_main() -> None:
    plan = {
        "plan_id": "integration-revalidation-wrong",
        "plan_sha256": "d" * 64,
        "frozen_base": "a" * 40,
        "lane_head": "f" * 40,
        "latest_main": "e" * 40,
        "decision": "READY_FOR_SINGLE_INTEGRATION_CANDIDATE",
        "candidate_creation_allowed": True,
    }
    blockers, _, _ = _base_drift(integration_plan=plan)
    mismatches = [
        row for row in blockers if row["code"] == "INTEGRATION_REVALIDATION_BINDING_MISMATCH"
    ]
    assert len(mismatches) == 2


def test_default_remote_push_contract_is_consistent_and_fail_closed() -> None:
    agents = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    skill = SKILL_PATH.read_text(encoding="utf-8")
    workflow = WORKFLOW_REFERENCE_PATH.read_text(encoding="utf-8")
    normalized_workflow = " ".join(workflow.split())

    assert "default closeout boundary includes local `main` and a normal push" in agents
    assert "ordinary non-force push" in agents
    assert "remote has diverged" in agents
    assert "`--stage CLOSEOUT --remote-action`" in skill
    assert "force-push" in skill
    assert (
        "repository default is an ordinary push after local-main integration"
        in normalized_workflow.lower()
    )
    assert "clean local `main`" in workflow
    assert "`origin_only=0`" in workflow
    assert "missing remote/upstream, remote divergence, or non-fast-forward push" in workflow
    assert "completed.md` is eligible only" in skill
    assert "integration_revalidation_plan.v1" in skill
    assert "--integration-revalidation-plan" in workflow
