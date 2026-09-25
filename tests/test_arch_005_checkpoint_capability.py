from __future__ import annotations

import json
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.platform.architecture.checkout_guard import (
    CHECKOUT_FULL_WORKTREE_PROFILE,
    CHECKOUT_SOURCE_ONLY_CAPABILITY_PREFIX,
    CHECKOUT_SOURCE_ONLY_PROFILE,
    CHECKOUT_SOURCE_ONLY_RUNTIME,
    CheckoutGuardError,
    CheckoutLeaseGuard,
    CheckoutOperationClass,
    parse_checkout_operation_intent,
)
from ai_trading_system.platform.architecture.integration_publication_fence import (
    IntegrationPublicationFence,
    PublicationFenceError,
    PublicationReplay,
)

ROOT = Path(__file__).resolve().parents[1]
ACTOR = "integration-coordinator"
TASK = "DEVX-015-SYNTHETIC-CAPABILITY"
CHECKOUT_POLICY = ROOT / "config/architecture/arch_005_s4d_checkout_guard.yaml"
PARALLEL_POLICY = ROOT / "config/architecture/arch_005_parallel_control_policy.yaml"
FENCE_POLICY = ROOT / "config/architecture/arch_005_integration_publication_fence.yaml"


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=root, capture_output=True, text=True, check=True
    ).stdout.strip()


@pytest.fixture
def repository(tmp_path: Path) -> Path:
    (tmp_path / "src").mkdir()
    (tmp_path / "src/a.py").write_text("A = 1\n", encoding="utf-8")
    (tmp_path / "src/b.py").write_text("B = 1\n", encoding="utf-8")
    (tmp_path / ".gitignore").write_text("outputs/\n", encoding="utf-8")
    _git(tmp_path, "init", "-b", "main")
    _git(tmp_path, "config", "user.name", "Synthetic Capability")
    _git(tmp_path, "config", "user.email", "capability@example.invalid")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-m", "synthetic capability seed")
    _git(tmp_path, "switch", "-c", "codex/capability")
    return tmp_path


def _guard(root: Path) -> CheckoutLeaseGuard:
    return CheckoutLeaseGuard(
        project_root=root, policy_path=CHECKOUT_POLICY, parallel_policy_path=PARALLEL_POLICY
    )


def _acquire(guard: CheckoutLeaseGuard, *, profile: str = CHECKOUT_SOURCE_ONLY_PROFILE) -> Any:
    decision, handle = guard.acquire(
        intent_id="synthetic-source-only",
        task_id=TASK,
        thread_id="synthetic-thread",
        actor=ACTOR,
        operation_class=CheckoutOperationClass.SHARED_MUTATION,
        shared_paths=("src/a.py", CHECKOUT_SOURCE_ONLY_RUNTIME),
        inspection_profile=profile,
    )
    assert decision.status == "PASS" and handle is not None
    return decision, handle


def test_v1_roundtrip_and_manifest_bytes_remain_unchanged(repository: Path) -> None:
    guard = _guard(repository)
    decision, handle = _acquire(guard, profile=CHECKOUT_FULL_WORKTREE_PROFILE)
    try:
        payload = decision.intent.to_dict()
        assert payload["schema_version"] == "checkout_operation_intent.v1"
        assert "inspection_profile" not in payload
        assert "source_mutation_allowed" not in payload
        assert parse_checkout_operation_intent(payload).to_dict() == payload
        task, _ = guard._lease_task(decision.intent)
        assert len(task.manifest.contract_claims) == 1
        lease = guard.replay().active_leases[0]
        assert lease.change_manifest_sha256 == task.manifest.sha256
        assert guard.require_mutation_lease(lease).to_dict() == payload
    finally:
        handle.release(outcome="completed")


def test_source_only_never_reads_unrequested_source_or_runs_status(
    repository: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = repository / "src/credentials.json"
    secret.write_bytes(b"synthetic untouched secret")
    (repository / "src/b.py").write_bytes(b"unrequested dirty synthetic bytes")
    (repository / "src/a.py").write_bytes(b"requested synthetic bytes")
    guard = _guard(repository)
    real_open = Path.open
    real_run = subprocess.run

    def guarded_open(path: Path, *args: Any, **kwargs: Any) -> Any:
        assert not path.is_relative_to(repository / "src"), "source bytes must not be opened"
        return real_open(path, *args, **kwargs)

    def guarded_run(command: Any, *args: Any, **kwargs: Any) -> Any:
        assert not ({"status", "check-attr"} & set(command)), "implicit worktree-content read"
        return real_run(command, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    monkeypatch.setattr(subprocess, "run", guarded_run)
    decision, handle = _acquire(guard)
    payload = decision.intent.to_dict()
    assert payload["schema_version"] == "checkout_operation_intent.v2"
    assert payload["source_mutation_allowed"] is False
    assert payload["unscoped_worktree_status"] == "NOT_INSPECTED"
    assert payload["clean_integration_status"] == "NOT_EVALUATED"
    assert payload["observed_dirty_paths"] == []
    assert parse_checkout_operation_intent(payload) == decision.intent
    lease = guard.replay().active_leases[0]
    assert any(
        claim.resource_id.startswith(CHECKOUT_SOURCE_ONLY_CAPABILITY_PREFIX)
        for claim in lease.resources
    )
    handle.release(outcome="completed")
    assert guard.replay().active_leases == ()


def test_source_only_cannot_be_laundered_as_v1_mutation(repository: Path) -> None:
    guard = _guard(repository)
    decision, handle = _acquire(guard)
    original = decision.intent_path.read_bytes()
    lease = guard.replay().active_leases[0]
    with pytest.raises(CheckoutGuardError, match="CHECKOUT_MUTATION_CAPABILITY_REQUIRED"):
        guard.require_mutation_lease(lease)
    legacy = replace(decision.intent, inspection_profile=CHECKOUT_FULL_WORKTREE_PROFILE)
    assert guard._lease_task(legacy)[0].manifest.sha256 != lease.change_manifest_sha256
    decision.intent_path.write_text(json.dumps(legacy.to_dict()), encoding="utf-8")
    try:
        with pytest.raises(CheckoutGuardError, match="CHECKOUT_LEASE_INTENT_BINDING"):
            guard.require_mutation_lease(lease)
        with pytest.raises(CheckoutGuardError, match="CHECKOUT_LEASE_INTENT_BINDING"):
            handle.release(outcome="completed")
        assert guard.replay().active_leases
    finally:
        decision.intent_path.write_bytes(original)
        handle.release(outcome="failed")


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_mutation_allowed", True),
        ("source_mutation_allowed", 0),
        ("unscoped_worktree_status", "CLEAN"),
        ("clean_integration_status", "PASS"),
        ("inspection_profile", CHECKOUT_FULL_WORKTREE_PROFILE),
        ("observed_dirty_paths", ["src/a.py"]),
    ],
)
def test_v2_parser_rejects_capability_field_tamper(
    repository: Path, field: str, value: object
) -> None:
    guard = _guard(repository)
    decision, handle = _acquire(guard)
    try:
        payload = decision.intent.to_dict()
        payload[field] = value
        with pytest.raises(CheckoutGuardError):
            parse_checkout_operation_intent(payload)
    finally:
        handle.release(outcome="completed")


@pytest.mark.parametrize("path", ["src/credentials.json", "src", "outputs", ".git/config"])
def test_source_only_rejects_private_or_directory_scope_before_acquisition(
    repository: Path, path: str
) -> None:
    guard = _guard(repository)
    with pytest.raises(CheckoutGuardError):
        guard.acquire(
            intent_id="bad-source-only",
            task_id=TASK,
            thread_id="synthetic-thread",
            actor=ACTOR,
            operation_class=CheckoutOperationClass.SHARED_MUTATION,
            shared_paths=(path, CHECKOUT_SOURCE_ONLY_RUNTIME),
            inspection_profile=CHECKOUT_SOURCE_ONLY_PROFILE,
        )
    assert not guard.replay().lease_heads


def test_fence_rejects_source_only_lease_in_active_and_release_paths(repository: Path) -> None:
    guard = _guard(repository)
    decision, handle = _acquire(guard)
    fence = IntegrationPublicationFence(
        project_root=repository,
        policy_path=FENCE_POLICY,
        checkout_guard_policy_path=CHECKOUT_POLICY,
        parallel_control_policy_path=PARALLEL_POLICY,
    )
    lease = guard.replay().active_leases[0]
    transaction = {
        "lease_id": lease.lease_id,
        "checkout_intent_path": decision.intent_path.as_posix(),
        "task_id": TASK,
        "actor": ACTOR,
    }
    replay = PublicationReplay(
        status="PASS",
        transaction=transaction,
        events=(),
        phase="TASK_SOURCE_PRE_WRITE",
        candidate_sha=None,
        issues=(),
    )
    try:
        with pytest.raises(PublicationFenceError, match="CHECKOUT_MUTATION_CAPABILITY_REQUIRED"):
            fence._active_lease(replay)
        handle.release(outcome="completed")
        released = guard.replay().lease_heads[0]
        with pytest.raises(PublicationFenceError, match="CHECKOUT_MUTATION_CAPABILITY_REQUIRED"):
            fence._require_publication_lease_intent(replay, released)
    finally:
        handle.release(outcome="failed")


def test_publication_keeps_unsorted_scope_bytes_but_checks_exact_members(repository: Path) -> None:
    fence = IntegrationPublicationFence(
        project_root=repository,
        policy_path=FENCE_POLICY,
        checkout_guard_policy_path=CHECKOUT_POLICY,
        parallel_control_policy_path=PARALLEL_POLICY,
    )
    head = _git(repository, "rev-parse", "HEAD")
    binding = fence.acquire(
        transaction_id="synthetic-unsorted-scope",
        task_id=TASK,
        change_id="synthetic-unsorted-change",
        thread_id="synthetic-unsorted-thread",
        actor=ACTOR,
        frozen_base_sha=head,
        lane_head_sha=head,
        expected_main_sha=head,
        owned_paths=("src/b.py", "src/a.py"),
        shared_paths=("docs/z.md", "docs/a.md"),
        generator_ids=("canonical-task-source",),
    )
    path = repository / str(binding["transaction_path"])
    before = path.read_bytes()
    transaction = json.loads(before)
    assert transaction["owned_paths"] == ["src/b.py", "src/a.py"]
    try:
        assert fence.validate(path, exact_phase="ACQUIRED")["status"] == "PASS"
        replay = fence.replay(path)
        lease = fence.guard.replay().active_leases[0]
        duplicate = {**transaction, "owned_paths": ["src/b.py", "src/a.py", "src/a.py"]}
        with pytest.raises(PublicationFenceError, match="PUBLICATION_PATH_DUPLICATE"):
            fence._require_publication_lease_intent(replace(replay, transaction=duplicate), lease)
        missing = {**transaction, "owned_paths": ["src/a.py"]}
        with pytest.raises(PublicationFenceError, match="PUBLICATION_LEASE_INTENT_BINDING"):
            fence._require_publication_lease_intent(replace(replay, transaction=missing), lease)
    finally:
        receipt = fence.release(path, actor=ACTOR, outcome="failed")
    assert path.read_bytes() == before
    assert fence.release(path, actor=ACTOR, outcome="failed") == receipt
