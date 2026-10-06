"""DEVX-016 S2: one transaction may cover several tasks, and a task-source-only one completes.

Before S2 a transaction named exactly one task id, so registering or moving N tasks took N
transactions that were all closed as FAILED, and a failed transaction was how a normal operation
ended. These tests pin the contract: the task set only widens which task rows a transaction may
write (the lease, the Full profile and every publication phase stay bound to the primary task),
and a TASK_SOURCE_ONLY transaction can end COMPLETED right after its task-source phase but can
never reach a candidate, validation or publication phase.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from ai_trading_system.platform.architecture.integration_publication_fence import (
    DEFAULT_POLICY_PATH,
    IntegrationPublicationFence,
    PublicationFenceError,
)

ROOT = Path(__file__).resolve().parents[1]
CHECKOUT_POLICY = ROOT / "config/architecture/arch_005_s4d_checkout_guard.yaml"
PARALLEL_POLICY = ROOT / "config/architecture/arch_005_parallel_control_policy.yaml"
PRIMARY = "DEVX-016-PRIMARY"
ACTOR = "integration-coordinator"


def _git(repository: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args], cwd=repository, check=True, capture_output=True, text=True,
    )
    return completed.stdout.strip()


@pytest.fixture
def checkout(tmp_path: Path) -> Path:
    repository = tmp_path / "repository"
    repository.mkdir()
    (repository / "src").mkdir()
    (repository / "src/a.py").write_text("VALUE = 1\n", encoding="utf-8")
    (repository / "docs").mkdir()
    (repository / "docs/shared_note.md").write_text("shared v1\n", encoding="utf-8")
    (repository / ".gitignore").write_text("outputs/\n", encoding="utf-8")
    for policy in (ROOT / DEFAULT_POLICY_PATH, CHECKOUT_POLICY, PARALLEL_POLICY):
        target = repository / "config/architecture" / policy.name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(policy.read_bytes())
    _git(repository, "init", "-b", "main")
    _git(repository, "config", "user.email", "s2@example.com")
    _git(repository, "config", "user.name", "S2 Test")
    _git(repository, "config", "core.autocrlf", "true")
    _git(repository, "add", ".")
    _git(repository, "commit", "-m", "fixture")
    _git(repository, "switch", "-c", "codex/s2-test")
    return repository


def _fence(repository: Path) -> IntegrationPublicationFence:
    return IntegrationPublicationFence(
        project_root=repository, policy_path=DEFAULT_POLICY_PATH,
        checkout_guard_policy_path=CHECKOUT_POLICY, parallel_control_policy_path=PARALLEL_POLICY,
    )


def _acquire(fence: IntegrationPublicationFence, repository: Path, transaction_id: str,
             **extra: object) -> dict[str, object]:
    head = _git(repository, "rev-parse", "HEAD")
    main = _git(repository, "rev-parse", "main")
    return fence.acquire(
        transaction_id=transaction_id, task_id=PRIMARY, change_id=f"{transaction_id}-change",
        thread_id=f"{transaction_id}-thread", actor=ACTOR, frozen_base_sha=main,
        lane_head_sha=head, expected_main_sha=main, owned_paths=("src/a.py",),
        shared_paths=("docs/shared_note.md",), generator_ids=("canonical-task-source",),
        **extra,  # type: ignore[arg-type]
    )


def _transaction(repository: Path, binding: dict[str, object]) -> Path:
    return repository / str(binding["transaction_path"])


def test_a_multi_task_transaction_admits_every_member_and_nobody_else(checkout: Path) -> None:
    fence = _fence(checkout)
    binding = _acquire(fence, checkout, "multi", task_ids=("TASK-C", "TASK-B"))
    transaction = _transaction(checkout, binding)
    body = json.loads(transaction.read_text(encoding="utf-8"))
    assert body["task_id"] == PRIMARY
    assert body["task_ids"] == sorted([PRIMARY, "TASK-B", "TASK-C"])  # sorted, primary included
    fence.checkpoint(transaction, phase="TASK_SOURCE_PRE_WRITE", actor=ACTOR)
    for member in (PRIMARY, "TASK-B", "TASK-C"):
        fence.validate(transaction, exact_phase="TASK_SOURCE_PRE_WRITE", task_id=member)
    with pytest.raises(PublicationFenceError, match="PUBLICATION_TASK_MISMATCH"):
        fence.validate(transaction, exact_phase="TASK_SOURCE_PRE_WRITE", task_id="TASK-OTHER")


def test_a_single_task_transaction_keeps_its_original_form(checkout: Path) -> None:
    fence = _fence(checkout)
    binding = _acquire(fence, checkout, "single")
    transaction = _transaction(checkout, binding)
    body = json.loads(transaction.read_text(encoding="utf-8"))
    assert "task_ids" not in body and "kind" not in body  # historical transactions hash unchanged
    fence.checkpoint(transaction, phase="TASK_SOURCE_PRE_WRITE", actor=ACTOR)
    fence.validate(transaction, exact_phase="TASK_SOURCE_PRE_WRITE", task_id=PRIMARY)
    with pytest.raises(PublicationFenceError, match="PUBLICATION_TASK_MISMATCH"):
        fence.validate(transaction, exact_phase="TASK_SOURCE_PRE_WRITE", task_id="TASK-B")


def test_acquire_is_idempotent_only_for_the_same_task_set_and_kind(checkout: Path) -> None:
    fence = _fence(checkout)
    first = _acquire(fence, checkout, "again", task_ids=("TASK-B",))
    assert _acquire(fence, checkout, "again", task_ids=("TASK-B",))["transaction_sha256"] == (
        first["transaction_sha256"]
    )
    conflicting: tuple[dict[str, object], ...] = (
        {"task_ids": ("TASK-C",)},
        {"task_ids": ()},
        {"task_ids": ("TASK-B",), "kind": "TASK_SOURCE_ONLY"},
    )
    for different in conflicting:
        with pytest.raises(
            PublicationFenceError, match="PUBLICATION_TRANSACTION_IDENTITY_CONFLICT",
        ):
            _acquire(fence, checkout, "again", **different)


def test_a_task_source_only_transaction_completes_after_its_task_phase(checkout: Path) -> None:
    fence = _fence(checkout)
    binding = _acquire(
        fence, checkout, "rows", task_ids=("TASK-B", "TASK-C"), kind="TASK_SOURCE_ONLY",
    )
    transaction = _transaction(checkout, binding)
    assert json.loads(transaction.read_text(encoding="utf-8"))["kind"] == "TASK_SOURCE_ONLY"
    fence.checkpoint(transaction, phase="TASK_SOURCE_PRE_WRITE", actor=ACTOR)
    for member in (PRIMARY, "TASK-B", "TASK-C"):
        fence.validate(transaction, exact_phase="TASK_SOURCE_PRE_WRITE", task_id=member)

    receipt = fence.release(transaction, actor=ACTOR, outcome="completed")
    assert receipt["outcome"] == "COMPLETED" and receipt["final_phase"] == "RELEASED"
    replay = fence.replay(transaction)
    assert replay.status == "PASS" and replay.phase == "RELEASED"
    assert [event["phase"] for event in replay.events] == [
        "ACQUIRED", "TASK_SOURCE_PRE_WRITE", "RELEASED",
    ]
    assert fence.release(transaction, actor=ACTOR, outcome="completed") == receipt  # idempotent
    with pytest.raises(PublicationFenceError):  # a released transaction admits no more writes
        fence.validate(transaction, exact_phase="TASK_SOURCE_PRE_WRITE", task_id="TASK-B")


def test_a_task_source_only_transaction_can_never_reach_a_candidate_phase(checkout: Path) -> None:
    fence = _fence(checkout)
    binding = _acquire(fence, checkout, "rows2", kind="TASK_SOURCE_ONLY")
    transaction = _transaction(checkout, binding)
    fence.checkpoint(transaction, phase="TASK_SOURCE_PRE_WRITE", actor=ACTOR)
    for phase in ("GENERATED_REBUILD_PRE", "CANDIDATE_COMMIT_PRE", "FORMAL_VALIDATION_PRE",
                  "LOCAL_MAIN_FF_PRE", "REMOTE_PUSH_PRE", "CLEANUP_PRE", "RELEASED"):
        with pytest.raises(
            PublicationFenceError, match="PUBLICATION_PHASE_TRANSITION_INVALID|TERMINAL",
        ):
            fence.checkpoint(transaction, phase=phase, actor=ACTOR)
    assert fence.replay(transaction).status == "PASS"
    assert fence.replay(transaction).phase == "TASK_SOURCE_PRE_WRITE"


def test_task_source_only_declares_no_plan_or_parent_and_ordinary_ones_still_need_cleanup(
    checkout: Path,
) -> None:
    fence = _fence(checkout)
    with pytest.raises(PublicationFenceError, match="PUBLICATION_TASK_SOURCE_ONLY_SCOPE"):
        _acquire(
            fence, checkout, "scoped", kind="TASK_SOURCE_ONLY", full_parent_path=Path("x.json"),
        )

    binding = _acquire(fence, checkout, "ordinary")  # an ordinary transaction
    transaction = _transaction(checkout, binding)
    fence.checkpoint(transaction, phase="TASK_SOURCE_PRE_WRITE", actor=ACTOR)
    with pytest.raises(PublicationFenceError, match="PUBLICATION_CLOSEOUT_PHASE_REQUIRED"):
        fence.release(transaction, actor=ACTOR, outcome="completed")  # still only at CLEANUP_PRE
    fence.release(transaction, actor=ACTOR, outcome="failed")


def test_the_kind_and_task_set_are_part_of_the_transaction_hash(checkout: Path) -> None:
    fence = _fence(checkout)
    binding = _acquire(fence, checkout, "hashed", task_ids=("TASK-B",), kind="TASK_SOURCE_ONLY")
    transaction = _transaction(checkout, binding)
    body = json.loads(transaction.read_text(encoding="utf-8"))
    body["kind"] = "PUBLICATION"  # try to turn the transaction into an ordinary one
    transaction.write_text(json.dumps(body), encoding="utf-8")
    assert fence.replay(transaction).status == "FAIL"
    with pytest.raises(PublicationFenceError, match="PUBLICATION_REPLAY_INVALID"):
        fence.checkpoint(transaction, phase="TASK_SOURCE_PRE_WRITE", actor=ACTOR)


def test_an_unknown_kind_is_refused(checkout: Path) -> None:
    fence = _fence(checkout)
    with pytest.raises(PublicationFenceError, match="PUBLICATION_TRANSACTION_KIND_INVALID"):
        _acquire(fence, checkout, "kind", kind="PUBLISH_EVERYTHING")


def test_the_cli_exposes_the_task_set_and_the_kind() -> None:
    completed = subprocess.run(
        [sys.executable, str(ROOT / "scripts/architecture_arch005_publication_fence.py"),
         "acquire", "--help"],
        capture_output=True, text=True, cwd=ROOT,
        env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
    )
    assert completed.returncode == 0, completed.stderr
    assert "--task-id-extra" in completed.stdout and "--task-source-only" in completed.stdout
