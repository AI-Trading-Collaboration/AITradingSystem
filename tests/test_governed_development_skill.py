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


def test_default_remote_push_contract_is_consistent_and_fail_closed() -> None:
    agents = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    skill = SKILL_PATH.read_text(encoding="utf-8")
    workflow = WORKFLOW_REFERENCE_PATH.read_text(encoding="utf-8")
    normalized_workflow = " ".join(workflow.split())

    assert "default closeout boundary includes local `main` and a normal push" in agents
    assert "ordinary non-force push" in agents
    assert "remote has diverged" in agents
    assert "run the closeout preflight with\n  `--remote-action`" in skill
    assert "force-push" in skill
    assert (
        "repository default is an ordinary push after local-main integration"
        in normalized_workflow.lower()
    )
    assert "missing remote/upstream, remote divergence, or non-fast-forward push" in workflow
