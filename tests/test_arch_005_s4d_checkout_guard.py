from __future__ import annotations

import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier

import pytest
from typer.testing import CliRunner

import ai_trading_system.cli_commands.ops as ops_cli
from ai_trading_system.cli import app
from ai_trading_system.platform.architecture.checkout_guard import (
    DEFAULT_CHECKOUT_GUARD_POLICY_PATH,
    CheckoutGuardError,
    CheckoutLeaseGuard,
    CheckoutOperationClass,
    collect_checkout_dirty_paths,
    load_checkout_guard_policy,
    resolve_checkout_identity,
)

NOW = datetime(2026, 7, 24, 3, 0, tzinfo=UTC)


@pytest.fixture
def git_checkout(tmp_path: Path) -> Path:
    (tmp_path / "src").mkdir()
    (tmp_path / "src/a.py").write_text("A = 1\n", encoding="utf-8")
    (tmp_path / "src/b.py").write_text("B = 1\n", encoding="utf-8")
    unrelated = tmp_path / "docs/research/growth_tilt_owner_diagnosis_pack.md"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_text("owner bytes v1\n", encoding="utf-8")
    _git(tmp_path, "init", "-b", "fixture")
    _git(tmp_path, "config", "user.email", "checkout-guard@example.com")
    _git(tmp_path, "config", "user.name", "Checkout Guard Test")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-m", "fixture")
    return tmp_path


def test_policy_freezes_owner_approved_s0_s1_s2_matrix_and_safety() -> None:
    policy = load_checkout_guard_policy(DEFAULT_CHECKOUT_GUARD_POLICY_PATH)

    assert policy.status == "OWNER_APPROVED_S0_S1_S2_READ_ONLY"
    assert policy.approval_ref == "owner_decision:ARCH-005S4D:2026-07-24:approve_narrow_s0_s1_v1"
    assert dict(policy.operation_gate_access) == {
        CheckoutOperationClass.DOMAIN_MUTATION: "READ",
        CheckoutOperationClass.SHARED_MUTATION: "READ",
        CheckoutOperationClass.DAILY_OPERATION: "WRITE",
        CheckoutOperationClass.READ_ONLY_AUDIT: "READ",
    }
    assert policy.authority_task_id == "ARCH-005S4D_SHARED_CHECKOUT_WRITE_LEASE_GUARD"
    assert policy.known_unrelated_exclusions[0].path == (
        "docs/research/growth_tilt_owner_diagnosis_pack.md"
    )


def test_workspace_identity_is_checkout_scoped_and_records_lineage(
    git_checkout: Path,
) -> None:
    identity = resolve_checkout_identity(git_checkout)

    assert identity.workspace_id.startswith("checkout-")
    assert Path(identity.checkout_root) == git_checkout.resolve()
    assert len(identity.head_commit) == 40
    assert identity.branch_name == "fixture"
    assert identity.upstream_ref is None
    assert identity.upstream_commit is None


def test_main_branch_requires_integration_coordinator_for_mutation(
    git_checkout: Path,
) -> None:
    _git(git_checkout, "branch", "-m", "main")
    guard = _guard(git_checkout)

    with pytest.raises(
        CheckoutGuardError,
        match="CHECKOUT_PROTECTED_BRANCH_DOMAIN_MUTATION",
    ):
        _acquire_mutation(
            guard,
            intent_id="main-domain",
            task_id="TASK-A",
            owned_paths=("src/a.py",),
        )

    with pytest.raises(
        CheckoutGuardError,
        match="CHECKOUT_PROTECTED_BRANCH_COORDINATOR_REQUIRED",
    ):
        guard.acquire(
            intent_id="main-shared-wrong-actor",
            task_id="TASK-A",
            thread_id="thread-task-a",
            actor="architecture-control-plane",
            operation_class=CheckoutOperationClass.SHARED_MUTATION,
            shared_paths=("src/a.py",),
            now=NOW,
        )

    decision, handle = guard.acquire(
        intent_id="main-shared-coordinator",
        task_id="TASK-A",
        thread_id="thread-task-a",
        actor="integration-coordinator",
        operation_class=CheckoutOperationClass.SHARED_MUTATION,
        shared_paths=("src/a.py",),
        now=NOW,
    )
    assert decision.status == "PASS"
    assert handle is not None
    handle.release(outcome="completed", at=NOW + timedelta(seconds=1))


def test_disjoint_domain_mutations_remain_parallel_but_daily_is_exclusive(
    git_checkout: Path,
) -> None:
    guard = _guard(git_checkout)
    first, first_handle = _acquire_mutation(
        guard,
        intent_id="domain-a",
        task_id="TASK-A",
        owned_paths=("src/a.py",),
    )
    second, second_handle = _acquire_mutation(
        guard,
        intent_id="domain-b",
        task_id="TASK-B",
        owned_paths=("src/b.py",),
    )
    daily, daily_handle = guard.acquire(
        intent_id="daily-conflict",
        task_id="OPS-DAILY-UNIFIED-TRIGGER",
        thread_id="daily",
        actor="operations-automation",
        operation_class=CheckoutOperationClass.DAILY_OPERATION,
        now=NOW,
    )

    assert first.status == "PASS"
    assert second.status == "PASS"
    assert first_handle is not None
    assert second_handle is not None
    assert daily.status == "BLOCKED"
    assert daily_handle is None
    assert daily.reason_codes[0].startswith("LEASE_RESOURCE_CONFLICT:")
    assert len(guard.replay().active_leases) == 2

    first_handle.release(outcome="completed", at=NOW + timedelta(seconds=1))
    second_handle.release(outcome="completed", at=NOW + timedelta(seconds=1))
    allowed, allowed_handle = guard.acquire(
        intent_id="daily-after-release",
        task_id="OPS-DAILY-UNIFIED-TRIGGER",
        thread_id="daily",
        actor="operations-automation",
        operation_class=CheckoutOperationClass.DAILY_OPERATION,
        now=NOW + timedelta(seconds=2),
    )
    assert allowed.status == "PASS"
    assert allowed_handle is not None
    allowed_handle.release(outcome="completed", at=NOW + timedelta(seconds=3))


def test_concurrent_overlapping_writers_produce_exactly_one_active_decision(
    git_checkout: Path,
) -> None:
    guard = _guard(git_checkout)
    barrier = Barrier(2)

    def acquire(intent_id: str):
        barrier.wait()
        return _acquire_mutation(
            guard,
            intent_id=intent_id,
            task_id=intent_id.upper(),
            owned_paths=("src/a.py",),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(
            executor.map(
                acquire,
                ("concurrent-a", "concurrent-b"),
            )
        )

    decisions = tuple(result[0] for result in results)
    handles = tuple(result[1] for result in results)
    assert sorted(decision.status for decision in decisions) == ["BLOCKED", "PASS"]
    assert sum(handle is not None for handle in handles) == 1
    assert len(guard.replay().active_leases) == 1
    active_handle = next(handle for handle in handles if handle is not None)
    active_handle.release(outcome="completed", at=NOW + timedelta(seconds=1))


def test_duplicate_trigger_replays_same_intent_and_lease_without_pid_authority(
    git_checkout: Path,
) -> None:
    guard = _guard(git_checkout)
    first, first_handle = _acquire_mutation(
        guard,
        intent_id="repeatable-trigger",
        task_id="TASK-A",
        owned_paths=("src/a.py",),
    )
    repeated, repeated_handle = _acquire_mutation(
        guard,
        intent_id="repeatable-trigger",
        task_id="TASK-A",
        owned_paths=("src/a.py",),
    )

    assert first.status == "PASS"
    assert repeated.status == "PASS"
    assert repeated.reason_codes == ("IDEMPOTENT_REPLAY",)
    assert first.lease_id == repeated.lease_id
    assert first.intent.created_at == repeated.intent.created_at
    assert "pid" not in first.intent.to_dict()
    assert first_handle is not None
    assert repeated_handle is not None
    first_handle.release(outcome="completed", at=NOW + timedelta(seconds=1))


@pytest.mark.parametrize(
    ("first_path", "second_path"),
    [
        ("src", "src/a.py"),
        ("src/a.py", "SRC/A.PY"),
    ],
)
def test_ancestor_and_casefold_path_conflicts_are_blocked(
    git_checkout: Path,
    first_path: str,
    second_path: str,
) -> None:
    guard = _guard(git_checkout)
    first, first_handle = _acquire_mutation(
        guard,
        intent_id="overlap-a",
        task_id="TASK-A",
        owned_paths=(first_path,),
    )
    second, second_handle = _acquire_mutation(
        guard,
        intent_id="overlap-b",
        task_id="TASK-B",
        owned_paths=(second_path,),
    )

    assert first.status == "PASS"
    assert first_handle is not None
    assert second.status == "BLOCKED"
    assert second_handle is None
    first_handle.release(outcome="completed", at=NOW + timedelta(seconds=1))


def test_unattributed_dirty_state_blocks_daily_before_lease_or_business_output(
    git_checkout: Path,
) -> None:
    (git_checkout / "src/a.py").write_text("A = 2\n", encoding="utf-8")
    guard = _guard(git_checkout)

    decision, handle = guard.acquire(
        intent_id="daily-dirty",
        task_id="OPS-DAILY-UNIFIED-TRIGGER",
        thread_id="daily",
        actor="operations-automation",
        operation_class=CheckoutOperationClass.DAILY_OPERATION,
        now=NOW,
    )

    assert decision.status == "BLOCKED"
    assert handle is None
    assert decision.reason_codes == ("CHECKOUT_DIRTY_UNATTRIBUTED:src/a.py",)
    assert guard.replay().event_count == 0
    assert not (git_checkout / "data").exists()
    assert not (git_checkout / "outputs/reports").exists()


def test_declared_dirty_mutation_is_attributed_and_unrelated_exact_path_is_excluded(
    git_checkout: Path,
) -> None:
    (git_checkout / "src/a.py").write_text("A = 2\n", encoding="utf-8")
    unrelated = git_checkout / "docs/research/growth_tilt_owner_diagnosis_pack.md"
    unrelated.write_text("owner bytes v2\n", encoding="utf-8")
    policy = load_checkout_guard_policy(DEFAULT_CHECKOUT_GUARD_POLICY_PATH)

    assert collect_checkout_dirty_paths(
        git_checkout,
        exclusions=tuple(row.path for row in policy.known_unrelated_exclusions),
    ) == ("src/a.py",)

    guard = _guard(git_checkout)
    decision, handle = _acquire_mutation(
        guard,
        intent_id="declared-dirty",
        task_id="TASK-A",
        owned_paths=("src/a.py",),
    )
    assert decision.status == "PASS"
    assert handle is not None
    serialized = decision.to_dict()
    exclusion = serialized["intent"]["known_unrelated_exclusions"][0]
    assert set(exclusion) == {"path", "rationale", "owner_ref"}
    assert "sha256" not in exclusion
    handle.release(outcome="completed", at=NOW + timedelta(seconds=1))


def test_release_scope_drift_fails_after_safely_releasing_lease(
    git_checkout: Path,
) -> None:
    guard = _guard(git_checkout)
    decision, handle = _acquire_mutation(
        guard,
        intent_id="release-scope-drift",
        task_id="TASK-A",
        owned_paths=("src/a.py",),
    )
    assert decision.status == "PASS"
    assert handle is not None
    (git_checkout / "src/late.py").write_text("LATE = 1\n", encoding="utf-8")

    with pytest.raises(
        CheckoutGuardError,
        match="CHECKOUT_RELEASE_DIRTY_UNATTRIBUTED.*src/late.py",
    ):
        handle.release(outcome="completed", at=NOW + timedelta(seconds=1))

    assert handle.released is True
    replay = guard.replay()
    assert replay.active_leases == ()
    head = next(
        lease for lease in replay.lease_heads if lease.lease_id == decision.lease_id
    )
    assert head.state == "RELEASED"
    event_path = next(
        (guard.store.events_root / head.lease_id).glob("*.json")
    )
    events = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (guard.store.events_root / head.lease_id).glob("*.json")
    ]
    assert event_path.is_file()
    assert any(
        "CHECKOUT_RELEASE_DIRTY_UNATTRIBUTED:src/late.py"
        in event["reason_codes"]
        for event in events
    )


def test_heartbeat_extends_lease_and_stale_owner_is_expired_before_daily(
    git_checkout: Path,
) -> None:
    guard = _guard(git_checkout)
    decision, handle = _acquire_mutation(
        guard,
        intent_id="heartbeat-owner",
        task_id="TASK-A",
        owned_paths=("src/a.py",),
    )
    assert handle is not None
    original = next(
        lease for lease in guard.replay().active_leases if lease.lease_id == handle.lease_id
    )

    handle.heartbeat(at=NOW + timedelta(minutes=5))
    refreshed = next(
        lease for lease in guard.replay().active_leases if lease.lease_id == handle.lease_id
    )
    assert refreshed.expires_at > original.expires_at

    daily, daily_handle = guard.acquire(
        intent_id="daily-after-expiry",
        task_id="OPS-DAILY-UNIFIED-TRIGGER",
        thread_id="daily",
        actor="operations-automation",
        operation_class=CheckoutOperationClass.DAILY_OPERATION,
        now=NOW + timedelta(hours=7),
    )
    heads = {lease.lease_id: lease for lease in guard.replay().lease_heads}
    assert decision.lease_id is not None
    assert heads[decision.lease_id].state == "EXPIRED"
    assert daily.status == "PASS"
    assert daily_handle is not None
    daily_handle.release(outcome="completed", at=NOW + timedelta(hours=7, seconds=1))


def test_symlink_or_reparse_component_is_rejected(git_checkout: Path) -> None:
    outside = git_checkout.parent / "outside"
    outside.mkdir()
    link = git_checkout / "src/link"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation is unavailable on this platform")

    guard = _guard(git_checkout)
    with pytest.raises(CheckoutGuardError, match="CHECKOUT_PATH_REPARSE_POINT"):
        _acquire_mutation(
            guard,
            intent_id="symlink-path",
            task_id="TASK-A",
            owned_paths=("src/link/file.py",),
        )


def test_daily_cli_guard_blocks_before_run_bundle_creation(
    git_checkout: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (git_checkout / "src/a.py").write_text("A = 2\n", encoding="utf-8")
    run_root = git_checkout / "daily-runs"
    monkeypatch.setattr(ops_cli, "PROJECT_ROOT", git_checkout)

    result = CliRunner().invoke(
        app,
        [
            "ops",
            "daily-run",
            "--as-of",
            "2026-07-23",
            "--run-output-root",
            str(run_root),
            "--run-id",
            "checkout-guard-blocked",
        ],
    )

    assert result.exit_code == 1
    assert "Checkout guard：BLOCKED" in result.output
    assert "provider_request=false" in result.output
    assert not run_root.exists()


def _guard(project_root: Path) -> CheckoutLeaseGuard:
    return CheckoutLeaseGuard(
        project_root=project_root,
        runtime_root=project_root / "outputs/architecture/checkout-guard-test",
    )


def _acquire_mutation(
    guard: CheckoutLeaseGuard,
    *,
    intent_id: str,
    task_id: str,
    owned_paths: tuple[str, ...],
):
    return guard.acquire(
        intent_id=intent_id,
        task_id=task_id,
        thread_id=f"thread-{task_id.lower()}",
        actor="architecture-control-plane",
        operation_class=CheckoutOperationClass.DOMAIN_MUTATION,
        owned_paths=owned_paths,
        now=NOW,
    )


def _git(root: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
