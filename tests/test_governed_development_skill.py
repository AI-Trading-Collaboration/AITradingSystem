from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
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


def _admission_git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout.strip()


def _admission_registry(repo: Path, tasks: dict[str, str], base: str) -> None:
    from ai_trading_system.platform.architecture import task_registry_canonical as canonical

    records = []
    previous = canonical._chain_genesis()
    for order, (task_id, status) in enumerate(tasks.items(), 1):
        cells = [task_id, "fixture", "P1", status, "coordinator", "none", "checks", "note"]
        event = {
            "schema_version": "task_event.v1",
            "task_id": task_id,
            "event_type": "TASK_REGISTERED",
            "occurred_at": "2026-09-10T01:00:00+00:00",
            "actor": "fixture",
            "change_id": "fixture-register",
            "base_commit": base,
            "previous_state_event_id": None,
            "from_status": None,
            "to_status": status,
            "payload": {"legacy_projection": cells},
            "evidence_refs": ["fixture"],
        }
        event["event_id"] = canonical._canonical_event_id(event)
        fragment = {
            "schema_version": canonical.CANONICAL_FRAGMENT_SCHEMA,
            "source_of_truth": canonical.CANONICAL_SOURCE,
            "stable_task_identity": {
                "task_id": task_id,
                "task_id_sha256": hashlib.sha256(task_id.encode()).hexdigest(),
            },
            "task_record": canonical._task_record_from_cells(cells, prior={}),
            "events": [event],
            "projection": canonical._projection_from_cells(cells),
            "last_event_id": event["event_id"],
        }
        fragment["fragment_checksum"] = canonical._payload_checksum(fragment, "fragment_checksum")
        canonical.validate_canonical_fragment(fragment)
        path = canonical._canonical_fragment_path(task_id)
        target = repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(canonical._yaml_bytes(fragment))
        core = {
            "task_id": task_id,
            "path": path,
            "file_sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
            "fragment_checksum": fragment["fragment_checksum"],
            "partition": "completed" if status == "DONE" else "active",
            "order": order,
        }
        chain = canonical._entry_chain(previous, core)
        records.append({**core, "previous_entry_sha256": previous, "entry_sha256": chain})
        previous = chain
    completed = sum(status == "DONE" for status in tasks.values())
    index = {
        "schema_version": canonical.CANONICAL_INDEX_SCHEMA,
        "status": "PASS",
        "source_of_truth": canonical.CANONICAL_SOURCE,
        "cutover_performed": True,
        "legacy_markdown_writable": False,
        "fragment_root": canonical.CANONICAL_FRAGMENT_ROOT,
        "policy_sha256": hashlib.sha256((repo / canonical.POLICY_PATH).read_bytes()).hexdigest(),
        "task_count": len(tasks),
        "fragment_count": len(tasks),
        "active_task_count": len(tasks) - completed,
        "completed_task_count": completed,
        "missing_task_count": 0,
        "duplicate_task_count": 0,
        "chain_genesis_sha256": canonical._chain_genesis(),
        "final_chain_sha256": previous,
        "fragments": records,
    }
    index["index_checksum"] = canonical._payload_checksum(index, "index_checksum")
    target = repo / canonical.CANONICAL_INDEX_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(canonical._yaml_bytes(index))


@pytest.fixture
def real_frozen_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
):
    """Real isolated Git, canonical bytes, official plan, audit and S4D fence."""
    from ai_trading_system.platform.architecture import task_registry_canonical as canonical
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )
    from ai_trading_system.platform.architecture.integration_revalidation import (
        build_integration_revalidation_plan,
    )

    variant = getattr(request, "param", "active")
    repo = tmp_path / "frozen-task-admission"
    repo.mkdir()
    for relative in (
        "AGENTS.md",
        "docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md",
        "scripts/architecture_arch005_checkout_guard.py",
        "scripts/architecture_arch005_publication_fence.py",
        "scripts/architecture_arch005_integration_revalidation.py",
        "config/architecture/arch_005_s4d_checkout_guard.yaml",
        "config/architecture/arch_005_parallel_control_policy.yaml",
        "config/architecture/arch_005_integration_publication_fence.yaml",
        "config/architecture/arch_005_s5_task_source_cutover.yaml",
    ):
        target = repo / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    (repo / "docs/task_register.md").write_text("# Empty current task view\n", encoding="utf-8")
    (repo / ".gitignore").write_text("outputs/\n", encoding="utf-8")
    _admission_git(repo, "init", "-b", "main")
    _admission_git(repo, "config", "user.name", "Admission fixture")
    _admission_git(repo, "config", "user.email", "fixture@example.invalid")
    _admission_git(repo, "config", "core.autocrlf", "true")
    _admission_git(
        repo,
        "remote",
        "add",
        "origin",
        "https://github.com/AI-Trading-Collaboration/AITradingSystem.git",
    )
    _admission_registry(repo, {"TASK-OTHER": "IN_PROGRESS"}, "a" * 40)
    _admission_git(repo, "add", ".")
    _admission_git(repo, "commit", "-m", "base")
    base = _admission_git(repo, "rev-parse", "HEAD")
    _admission_git(repo, "switch", "-c", "frozen-lane")
    _admission_registry(
        repo,
        {
            "TASK-OTHER": "IN_PROGRESS",
            "TASK-FROZEN": "DONE" if variant == "lane-terminal" else "IN_PROGRESS",
        },
        base,
    )
    if variant == "fragment-missing":
        (repo / canonical._canonical_fragment_path("TASK-FROZEN")).unlink()
    elif variant == "fragment-corrupt":
        (repo / canonical._canonical_fragment_path("TASK-FROZEN")).write_text("corrupt\n")
    _admission_git(repo, "add", ".")
    _admission_git(repo, "commit", "-m", "registered lane")
    lane = _admission_git(repo, "rev-parse", "HEAD")
    _admission_git(repo, "switch", "main")
    if variant == "current-terminal":
        _admission_registry(repo, {"TASK-OTHER": "IN_PROGRESS", "TASK-FROZEN": "DONE"}, base)
    (repo / "docs/unrelated.md").write_text("main advance\n", encoding="utf-8")
    _admission_git(repo, "add", ".")
    _admission_git(repo, "commit", "-m", "main advance")
    main = _admission_git(repo, "rev-parse", "HEAD")
    _admission_git(repo, "switch", "-c", "candidate")
    manifest = {
        "schema_version": "change_manifest.v1",
        "change_id": "admission-test",
        "task_id": "TASK-OTHER" if variant == "manifest-other-task" else "TASK-FROZEN",
        "lane_role": "COORDINATOR",
        "base_commit": base,
        "owner": "fixture",
        "production_effect": "none",
        "owned_paths": [],
        "shared_paths": ["registry/development_tasks", "inputs/architecture"],
        "module_ids": ["admission"],
        "contract_claims": [],
        "required_validation_tiers": ["focused"],
    }
    plan = build_integration_revalidation_plan(
        repository=repo,
        frozen_base=base,
        lane_head=lane,
        latest_main=main,
        manifest=manifest,
    )
    expected_decision = (
        "RECONCILIATION_REQUIRED"
        if variant == "current-terminal"
        else "READY_FOR_SINGLE_INTEGRATION_CANDIDATE"
    )
    assert plan["decision"] == expected_decision, plan
    output = repo / "outputs/admission"
    output.mkdir(parents=True)
    plan_path = output / "plan.json"
    manifest_path = output / "manifest.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    fence = IntegrationPublicationFence(project_root=repo)
    binding = fence.acquire(
        transaction_id="frozen-admission",
        task_id="TASK-FROZEN",
        change_id="admission-test",
        thread_id="fixture",
        actor="integration-coordinator",
        frozen_base_sha=base,
        lane_head_sha=main,
        expected_main_sha=main,
        owned_paths=("tests/fixture.py",),
        shared_paths=("registry/development_tasks", "inputs/architecture", "docs/task_register.md"),
        generator_ids=("canonical-task-source",),
        integration_plan_path=plan_path,
    )
    transaction = Path(binding["transaction_path"])
    monkeypatch.setenv("PYTHONPATH", str(ROOT / "src"))
    args = argparse.Namespace(
        repo=str(repo),
        mode="SINGLE_LANE",
        role="coordinator",
        stage="INTEGRATION",
        task_id="TASK-FROZEN",
        expected_base=base,
        integration_revalidation_plan=str(plan_path),
        change_manifest=str(manifest_path),
        publication_transaction=str(transaction),
        reviewed_reconciliation_plan_id=(
            plan["plan_id"] if variant == "current-terminal" else None
        ),
        claim=["task=tests/fixture.py"],
        coordinator_path=["registry/development_tasks", "inputs/architecture"],
        allow_active_lease=[],
        contract_change=False,
        remote_action=False,
    )
    yield repo, args, lane, fence, transaction
    fence.release(transaction, actor="integration-coordinator", outcome="failed")


def test_frozen_admission_real_build_result_and_cli(real_frozen_admission) -> None:
    repo, args, lane, _, _ = real_frozen_admission
    before = _admission_git(repo, "rev-parse", "HEAD")
    result = PREFLIGHT.build_result(args)
    assert result["status"] == "PASS", result
    assert result["task_registration_source"] == "FROZEN_LANE_CANONICAL_PRE_WRITE"
    assert result["frozen_task_registration_proof"]["source_commit"] == lane
    command = [sys.executable, str(PREFLIGHT_PATH)]
    for key, value in vars(args).items():
        if value is None or value is False:
            continue
        for item in value if isinstance(value, list) else [value]:
            command.extend(("--" + key.replace("_", "-"), str(item)))
    completed = subprocess.run(command, cwd=repo, capture_output=True, text=True, env=os.environ)
    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert (
        json.loads(completed.stdout)["task_registration_source"]
        == result["task_registration_source"]
    )
    assert _admission_git(repo, "rev-parse", "HEAD") == before
    assert not result["worktree_audit"]["dirty_paths"]


@pytest.mark.parametrize(
    "real_frozen_admission",
    [
        "lane-terminal",
        "fragment-missing",
        "fragment-corrupt",
        "current-terminal",
        "manifest-other-task",
    ],
    indirect=True,
)
def test_frozen_admission_real_canonical_refusals(real_frozen_admission) -> None:
    _, args, _, _, _ = real_frozen_admission
    result = PREFLIGHT.build_result(args)
    assert result["status"] == "BLOCKED", result
    assert result["task_registered"] is False
    assert result["frozen_task_registration_proof"] is None
    assert "TASK_NOT_REGISTERED" in {item["code"] for item in result["blockers"]}


@pytest.mark.parametrize("mutation", ["wrong-base", "dirty", "checkpoint", "plan-tamper", "phase"])
def test_frozen_admission_real_refusals(real_frozen_admission, mutation: str) -> None:
    repo, args, lane, fence, transaction = real_frozen_admission
    if mutation == "wrong-base":
        args.expected_base = _admission_git(repo, "rev-parse", "main")
    elif mutation == "dirty":
        (repo / "docs/task_register.md").write_text("dirty\n", encoding="utf-8")
    elif mutation == "checkpoint":
        _admission_git(repo, "update-ref", "refs/aits/task-checkpoints/fixture", lane)
    elif mutation == "plan-tamper":
        with Path(args.integration_revalidation_plan).open("a", encoding="utf-8") as stream:
            stream.write("\n ")
    else:
        fence.checkpoint(
            transaction, phase="TASK_SOURCE_PRE_WRITE", actor="integration-coordinator"
        )
    result = PREFLIGHT.build_result(args)
    assert result["status"] == "BLOCKED", result
    assert result["task_registered"] is False
    assert result["frozen_task_registration_proof"] is None
    assert "TASK_NOT_REGISTERED" in {item["code"] for item in result["blockers"]}


@pytest.mark.parametrize(
    "mutation",
    [
        "worker",
        "START",
        "LANE",
        "CLOSEOUT",
        "READ_ONLY",
        "dirty",
        "phase",
        "missing-plan",
        "missing-transaction",
        "wrong-task",
        "main-branch",
        "wrong-candidate",
        "wrong-main",
        "wrong-frozen-base",
        "wrong-plan-id",
        "wrong-plan-hash",
        "current-terminal",
        "current-corrupt",
        "lane-terminal",
        "lane-unknown",
        "wrong-proof-task",
        "wrong-proof-commit",
        "source-only-message",
    ],
)
def test_frozen_admission_additional_identity_matrix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    base, head, lane = "a" * 40, "b" * 40, "c" * 40
    args = argparse.Namespace(
        mode="SINGLE_LANE",
        role="coordinator",
        stage="INTEGRATION",
        task_id="TASK-FROZEN",
        expected_base=base,
    )
    state = {
        "head": head,
        "local_main": head,
        "current_branch": "candidate",
        "worktree_audit": {"status": "PASS", "dirty_paths": []},
    }
    plan = {
        "frozen_base": base,
        "latest_main": head,
        "lane_head": lane,
        "plan_id": "plan-exact",
        "_binding_file_sha256": "d" * 64,
        "_binding_manifest_task_id": "TASK-FROZEN",
    }
    transaction = {
        "phase": "ACQUIRED",
        "task_id": args.task_id,
        "lane_head_sha": head,
        "expected_main_sha": head,
        "integration_revalidation_plan": {"id": "plan-exact", "sha256": "d" * 64},
    }
    current = {"reader_error": "CANONICAL_TASK_NOT_FOUND"}
    proof = {"task_id": args.task_id, "source_commit": lane, "is_terminal": False}
    if mutation == "worker":
        args.role = "worker"
    elif mutation in {"START", "LANE", "CLOSEOUT"}:
        args.stage = mutation
    elif mutation == "READ_ONLY":
        args.mode = mutation
    elif mutation == "dirty":
        state["worktree_audit"]["dirty_paths"] = ["tests/fixture.py"]
    elif mutation == "phase":
        transaction["phase"] = "FORMAL_VALIDATION_PRE"
    elif mutation == "missing-plan":
        plan = None
    elif mutation == "missing-transaction":
        transaction = None
    elif mutation == "wrong-task":
        transaction["task_id"] = "TASK-OTHER"
    elif mutation == "main-branch":
        state["current_branch"] = "main"
    elif mutation == "wrong-candidate":
        transaction["lane_head_sha"] = lane
    elif mutation == "wrong-main":
        transaction["expected_main_sha"] = base
    elif mutation == "wrong-frozen-base":
        args.expected_base = head
    elif mutation == "wrong-plan-id":
        transaction["integration_revalidation_plan"]["id"] = "another-plan"
    elif mutation == "wrong-plan-hash":
        transaction["integration_revalidation_plan"]["sha256"] = "e" * 64
    elif mutation == "current-terminal":
        current = {"task_id": args.task_id, "is_terminal": True}
    elif mutation == "current-corrupt":
        current = {"reader_error": "INDEX_CHECKSUM"}
    elif mutation == "lane-terminal":
        proof["is_terminal"] = True
    elif mutation == "lane-unknown":
        proof = {"reader_error": "CANONICAL_TASK_NOT_FOUND"}
    elif mutation == "wrong-proof-task":
        proof["task_id"] = "TASK-OTHER"
    elif mutation == "wrong-proof-commit":
        proof["source_commit"] = base
    monkeypatch.setattr(
        PREFLIGHT,
        "read_exact_canonical_task",
        lambda repo, commit, task_id: current if commit == head else proof,
    )
    monkeypatch.setattr(
        PREFLIGHT,
        "_run",
        lambda command, repo: (
            "profile=RAW_BYTES_TASK_CHECKPOINT_UNVALIDATED"
            if mutation == "source-only-message" and command[1] == "show"
            else ""
        ),
    )
    try:
        result = PREFLIGHT.frozen_lane_task_registration(
            repo=tmp_path,
            args=args,
            state=state,
            plan=plan,
            transaction=transaction,
        )
    except PREFLIGHT.PreflightError:
        return
    assert result is None


ADMISSION_TASK = "DEVX-015-SYNTHETIC-ADMISSION"


@pytest.fixture
def admission_checkout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    # Child guard/fence CLIs must share the synthetic Git context, never a
    # caller's alternate index, object directory, config, or worktree binding.
    for name in tuple(os.environ):
        if name.startswith("GIT_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("PYTHONPATH", str(ROOT / "src"))
    repository = tmp_path / "admission"
    repository.mkdir()
    for relative in (
        "scripts/architecture_arch005_checkout_guard.py",
        "scripts/architecture_arch005_publication_fence.py",
        "config/architecture/arch_005_s4d_checkout_guard.yaml",
        "config/architecture/arch_005_parallel_control_policy.yaml",
        "config/architecture/arch_005_integration_publication_fence.yaml",
        "docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md",
    ):
        destination = repository / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, destination)
    for relative, content in {
        "AGENTS.md": "Synthetic admission fixture\n",
        ".gitignore": "outputs/\n",
        "src/a.py": "VALUE = 1\n",
        "docs/task_register.md": f"|{ADMISSION_TASK}|fixture|P0|IN_PROGRESS|\n",
        "docs/task_register_completed.md": "Completed tasks\n",
    }.items():
        destination = repository / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(content, encoding="utf-8")
    _admission_git(repository, "init", "-b", "main")
    _admission_git(repository, "config", "user.name", "Synthetic Admission")
    _admission_git(repository, "config", "user.email", "admission@example.invalid")
    _admission_git(repository, "config", "core.autocrlf", "true")
    _admission_git(repository, "add", ".")
    _admission_git(repository, "commit", "-m", "synthetic admission seed")
    # Scope validation reads this URL; these tests never contact the network.
    _admission_git(
        repository,
        "remote",
        "add",
        "origin",
        "https://github.com/AI-Trading-Collaboration/AITradingSystem.git",
    )
    _admission_git(repository, "update-ref", "refs/remotes/origin/main", "HEAD")
    _admission_git(repository, "switch", "-c", "codex/admission")
    yield repository
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
        PublicationFenceError,
    )

    fence = IntegrationPublicationFence(
        project_root=repository,
        policy_path=repository / "config/architecture/arch_005_integration_publication_fence.yaml",
    )
    for transaction in fence.runtime_root.glob("transactions/*/transaction.json"):
        try:
            fence.release(transaction, actor="integration-coordinator", outcome="failed")
        except PublicationFenceError:
            # Deliberately tampered evidence cannot produce a publication
            # receipt. Release its actual lease through the guard, retaining
            # the rejected transaction bytes inside pytest's temporary tree.
            assert fence.replay(transaction).status != "PASS"
    for lease in fence.guard.replay().active_leases:
        fence.guard.release(lease.lease_id, actor="integration-coordinator", outcome="failed")
    assert fence.guard.replay().active_leases == ()


def _admission_complete_projection(repository: Path) -> None:
    # Synthetic compatibility projections: canonical task-writer validation is
    # covered separately; here the consumer must read the final committed view.
    (repository / "docs/task_register.md").write_text(
        "|Other-task|fixture|P0|IN_PROGRESS|\n", encoding="utf-8"
    )
    (repository / "docs/task_register_completed.md").write_text(
        f"|{ADMISSION_TASK}|fixture|P0|DONE|\n", encoding="utf-8"
    )


def _admission_transaction(
    repository: Path, *, phase: str = "LOCAL_MAIN_FF_PRE", expired: bool = False
) -> tuple[object, Path]:
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )

    fence = IntegrationPublicationFence(
        project_root=repository,
        policy_path=repository / "config/architecture/arch_005_integration_publication_fence.yaml",
    )
    instant = datetime.now(UTC) - timedelta(days=2) if expired else datetime.now(UTC)
    base = _admission_git(repository, "rev-parse", "HEAD")
    binding = fence.acquire(
        transaction_id="synthetic-admission",
        task_id=ADMISSION_TASK,
        change_id="synthetic-admission-change",
        thread_id="synthetic-admission-thread",
        actor="integration-coordinator",
        frozen_base_sha=base,
        lane_head_sha=base,
        expected_main_sha=base,
        owned_paths=("src/a.py",),
        shared_paths=("docs/task_register.md", "docs/task_register_completed.md"),
        generator_ids=("canonical-task-source",),
        now=instant,
    )
    transaction = repository / str(binding["transaction_path"])
    for next_phase in (
        "TASK_SOURCE_PRE_WRITE",
        "GENERATED_REBUILD_PRE",
        "GENERATED_REBUILD_POST",
        "CANDIDATE_COMMIT_PRE",
        "FORMAL_VALIDATION_PRE",
        "FULL_DISPATCHED",
        "FORMAL_VALIDATION_RESULT",
        "LOCAL_MAIN_FF_PRE",
    ):
        if fence.replay(transaction).phase == phase:
            break
        options = {}
        if next_phase in {"GENERATED_REBUILD_PRE", "GENERATED_REBUILD_POST"}:
            options = {
                "generator_ids": ("canonical-task-source",),
                "evidence_paths": (repository / "docs/task_register_completed.md",),
            }
        elif next_phase == "FORMAL_VALIDATION_PRE":
            (repository / "src/a.py").write_text("VALUE = 2\n", encoding="utf-8")
            _admission_git(
                repository,
                "add",
                "src/a.py",
                "docs/task_register.md",
                "docs/task_register_completed.md",
            )
            _admission_git(repository, "commit", "-m", "synthetic final candidate")
        elif next_phase == "FULL_DISPATCHED":
            options = {"full_run_id": "synthetic-full-admission"}
        elif next_phase == "FORMAL_VALIDATION_RESULT":
            # A synthetic runner receipt tests the real fence consumer chain;
            # this is not evidence that project Full executed in this fixture.
            summary = repository / "outputs/validation_runtime/full/test_runtime_summary.json"
            summary.parent.mkdir(parents=True)
            summary.write_text('{"status":"PASS"}\n', encoding="utf-8")
            options = {"evidence_paths": (summary,), "validation_status": "PASS"}
        fence.checkpoint(
            transaction, phase=next_phase, actor="integration-coordinator", now=instant, **options
        )
        if next_phase == "TASK_SOURCE_PRE_WRITE":
            _admission_complete_projection(repository)
    return fence, transaction


def _admission_result(
    repository: Path,
    transaction: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    stage: str = "INTEGRATION",
    role: str = "coordinator",
    task_id: str = ADMISSION_TASK,
    allowed_lease: str | None = None,
) -> dict:
    argv = [
        "preflight.py",
        "--repo",
        str(repository),
        "--mode",
        "SINGLE_LANE",
        "--role",
        role,
        "--stage",
        stage,
        "--task-id",
        task_id,
        "--claim",
        "task=src/a.py",
        "--publication-transaction",
        str(transaction),
    ]
    if allowed_lease:
        argv.extend(("--allow-active-lease", allowed_lease))
    monkeypatch.setattr(sys, "argv", argv)
    # No state, validator, registration, or Git helpers are mocked here.
    return PREFLIGHT.build_result(PREFLIGHT.parse_args())


def test_completed_admission_full_entry_real_publication_pass(
    admission_checkout: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fence, transaction = _admission_transaction(admission_checkout)
    validated = fence.validate(
        transaction, exact_phase="LOCAL_MAIN_FF_PRE", task_id=ADMISSION_TASK, require_candidate=True
    )
    result = _admission_result(admission_checkout, transaction, monkeypatch)
    assert result["status"] == "PASS", result["blockers"]
    assert result["task_registration_source"] == "COMPLETED_VALIDATED_CANDIDATE_INTEGRATION"
    assert result["publication_transaction"]["candidate_sha"] == result["git"]["head"]
    assert result["leases"]["active_lease_ids"] == [validated["lease_id"]]
    assert result["worktree_audit"]["dirty_paths"] == []
    assert _admission_git(admission_checkout, "rev-parse", "main") != result["git"]["head"]


@pytest.mark.parametrize(
    "case",
    [
        "phase",
        "task",
        "candidate",
        "role",
        "dirty",
        "audit",
        "START",
        "LANE",
        "tampered",
        "terminal",
        "expired",
    ],
)
def test_completed_admission_full_entry_rejects_real_invalid_context(
    admission_checkout: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    fence, transaction = _admission_transaction(
        admission_checkout,
        phase="FORMAL_VALIDATION_RESULT" if case == "phase" else "LOCAL_MAIN_FF_PRE",
        expired=case == "expired",
    )
    if case == "candidate":
        _admission_git(admission_checkout, "commit", "--allow-empty", "-m", "candidate drift")
    elif case in {"dirty", "audit"}:
        (admission_checkout / "src/a.py").write_text(
            "VALUE = 3  \n" if case == "audit" else "VALUE = 3\n", encoding="utf-8"
        )
    elif case == "tampered":
        payload = json.loads(transaction.read_text(encoding="utf-8"))
        payload["task_id"] = "forged-task"
        transaction.write_text(json.dumps(payload), encoding="utf-8")
    elif case == "terminal":
        fence.release(transaction, actor="integration-coordinator", outcome="failed")
    if case == "audit":
        # A real failed audit CLI exits nonzero; the result builder must abort
        # before considering completed-task admission, rather than consume PASS.
        with pytest.raises(PREFLIGHT.PreflightError, match="worktree-audit.*failed"):
            _admission_result(admission_checkout, transaction, monkeypatch)
        return
    result = _admission_result(
        admission_checkout,
        transaction,
        monkeypatch,
        stage=case if case in {"START", "LANE"} else "INTEGRATION",
        role="worker" if case == "role" else "coordinator",
        task_id="Other-task" if case == "task" else ADMISSION_TASK,
    )
    assert result["status"] == "BLOCKED", result
    codes = {row["code"] for row in result["blockers"]}
    if case in {"task", "tampered", "terminal", "expired"}:
        assert "PUBLICATION_TRANSACTION_INVALID" in codes
        assert result["publication_transaction"] is None
        native_code = {
            "task": "PUBLICATION_TASK_MISMATCH",
            "tampered": "PUBLICATION_REPLAY_INVALID",
            "terminal": "PUBLICATION_TRANSACTION_TERMINAL",
            "expired": "PUBLICATION_LEASE_EXPIRED",
        }[case]
        assert any(native_code in row["detail"] for row in result["blockers"])
    else:
        assert not result["task_registered"]
        assert "TASK_NOT_REGISTERED" in codes


@pytest.mark.parametrize(
    ("mode", "source_only", "expected"),
    [
        ("READ_ONLY", True, "PASS"),
        ("SINGLE_LANE", False, "PASS"),
        ("SINGLE_LANE", True, "BLOCKED"),
        ("DUAL_LANE", True, "BLOCKED"),
    ],
)
def test_allowed_snapshot_lease_never_becomes_workflow_mutation_permission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str, source_only: bool, expected: str
) -> None:
    # Exercise the real result builder, not a source-text assertion. The replay
    # here is a deterministic input; real immutable lease binding is separately
    # covered by test_arch_005_checkpoint_capability.
    (tmp_path / "docs").mkdir()
    (tmp_path / "AGENTS.md").write_text("Synthetic governance fixture\n")
    (tmp_path / "docs/task_register.md").write_text("|DEVX-015|fixture|P0|IN_PROGRESS|\n")
    resources = (
        [
            {
                "kind": "contract",
                "resource_id": "checkout-source-only-capability:fixture",
                "access": "READ",
            }
        ]
        if source_only
        else []
    )
    state = {
        "current_branch": "codex/fixture",
        "head": "a" * 40,
        "local_main": "a" * 40,
        "origin_main": "a" * 40,
        "origin_main_vs_local_main": {"local_only": 0, "origin_only": 0},
        "worktrees": [str(tmp_path)],
        "worktree_audit": {"status": "PASS", "dirty_paths": [], "known_unrelated_exclusions": []},
        "lease_replay": {
            "status": "PASS",
            "active_leases": [{"lease_id": "lease-fixture", "resources": resources}],
        },
    }
    monkeypatch.setattr(PREFLIGHT, "validate_repository_scope", lambda _root: tmp_path)
    monkeypatch.setattr(PREFLIGHT, "collect_repo_state", lambda _root: state)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "preflight.py",
            "--repo",
            str(tmp_path),
            "--mode",
            mode,
            "--role",
            "reader" if mode == "READ_ONLY" else "coordinator",
            "--stage",
            "LANE",
            "--task-id",
            "DEVX-015",
            "--allow-active-lease",
            "lease-fixture",
        ],
    )
    args = PREFLIGHT.parse_args()
    if mode == "SINGLE_LANE":
        args.claim = ["task=src/fixture.py"]
    result = PREFLIGHT.build_result(args)
    if mode == "DUAL_LANE":
        # An empty dual-lane claim is independently invalid; capability refusal
        # must still be present and cannot be hidden by another admission failure.
        assert result["status"] == "BLOCKED"
    else:
        assert result["status"] == expected
    codes = {row["code"] for row in result["blockers"]}
    assert ("SOURCE_ONLY_LEASE_NOT_MUTATION_AUTHORITY" in codes) == (
        mode != "READ_ONLY" and source_only
    )


def test_repository_scope_gate_rejects_similarly_named_wrong_origin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(command: list[str], cwd: Path, *, timeout: int = 90) -> str:
        del cwd, timeout
        if command[-1] == "--show-toplevel":
            return str(tmp_path)
        if command[-3:] == ["remote", "get-url", "origin"]:
            return "https://github.com/example/AITradingSystem.git"
        raise AssertionError(command)

    monkeypatch.setattr(PREFLIGHT, "_run", fake_run)
    with pytest.raises(PREFLIGHT.PreflightError, match="repository origin is not"):
        PREFLIGHT.validate_repository_scope(tmp_path)


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


def test_integration_and_closeout_require_publication_transaction() -> None:
    for stage in ("INTEGRATION", "CLOSEOUT"):
        transaction, blockers = PREFLIGHT.load_publication_transaction(
            repo=ROOT,
            transaction_argument=None,
            mode="SINGLE_LANE",
            role="coordinator",
            stage=stage,
            task_id="DEVX-009",
        )
        assert transaction is None
        assert [row["code"] for row in blockers] == ["PUBLICATION_TRANSACTION_REQUIRED"]


def test_lane_stage_does_not_acquire_or_require_publication_transaction() -> None:
    transaction, blockers = PREFLIGHT.load_publication_transaction(
        repo=ROOT,
        transaction_argument=None,
        mode="SINGLE_LANE",
        role="coordinator",
        stage="LANE",
        task_id="DEVX-009",
    )
    assert transaction is None
    assert blockers == []


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
        "publication_transaction": None,
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


def test_reviewed_reconciliation_binds_latest_main_publication_candidate() -> None:
    plan = {
        "plan_id": "integration-revalidation-reconcile",
        "plan_sha256": "d" * 64,
        "_binding_file_sha256": "e" * 64,
        "frozen_base": "a" * 40,
        "lane_head": "c" * 40,
        "latest_main": "b" * 40,
        "decision": "RECONCILIATION_REQUIRED",
        "candidate_creation_allowed": False,
        "reviewed_reconciliation_required": True,
    }
    transaction = {
        "expected_main_sha": "b" * 40,
        "lane_head_sha": "b" * 40,
        "integration_revalidation_plan": {
            "id": plan["plan_id"],
            "sha256": plan["_binding_file_sha256"],
        },
    }
    blockers, serial, warnings = _base_drift(
        head="b" * 40,
        integration_plan=plan,
        publication_transaction=transaction,
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


@pytest.mark.parametrize(
    ("transaction_override", "expected_code"),
    [
        (
            {"expected_main_sha": "f" * 40},
            "PUBLICATION_CANDIDATE_BASE_BINDING_MISMATCH",
        ),
        (
            {"lane_head_sha": "f" * 40},
            "PUBLICATION_CANDIDATE_BASE_BINDING_MISMATCH",
        ),
        (
            {"integration_revalidation_plan": None},
            "PUBLICATION_INTEGRATION_PLAN_BINDING_MISSING",
        ),
        (
            {
                "integration_revalidation_plan": {
                    "id": "wrong-plan",
                    "sha256": "e" * 64,
                }
            },
            "PUBLICATION_INTEGRATION_PLAN_BINDING_MISMATCH",
        ),
    ],
)
def test_publication_candidate_base_binding_drift_fails_closed(
    transaction_override: dict[str, object],
    expected_code: str,
) -> None:
    plan = {
        "plan_id": "integration-revalidation-reconcile",
        "plan_sha256": "d" * 64,
        "_binding_file_sha256": "e" * 64,
        "frozen_base": "a" * 40,
        "lane_head": "c" * 40,
        "latest_main": "b" * 40,
        "decision": "RECONCILIATION_REQUIRED",
        "candidate_creation_allowed": False,
        "reviewed_reconciliation_required": True,
    }
    transaction: dict[str, object] = {
        "expected_main_sha": "b" * 40,
        "lane_head_sha": "b" * 40,
        "integration_revalidation_plan": {
            "id": plan["plan_id"],
            "sha256": plan["_binding_file_sha256"],
        },
    }
    transaction.update(transaction_override)
    blockers, _, _ = _base_drift(
        head="b" * 40,
        integration_plan=plan,
        publication_transaction=transaction,
        reviewed_reconciliation_plan_id=plan["plan_id"],
    )
    assert expected_code in {row["code"] for row in blockers}


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
    assert "completed.md` is normally eligible only" in skill
    assert "`START` and `LANE` still require the active register" in skill
    assert "`LOCAL_MAIN_FF_PRE`" in skill
    assert "integration_revalidation_plan.v1" in skill
    assert "--integration-revalidation-plan" in workflow
    assert "--publication-transaction" in skill
    assert "integration_publication_fence.v1" in workflow
