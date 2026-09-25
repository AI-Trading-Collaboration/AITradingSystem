from __future__ import annotations

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier

import pytest
from typer.testing import CliRunner

import ai_trading_system.cli_commands.ops as ops_cli
import ai_trading_system.platform.architecture.checkout_guard as checkout_guard_module
import scripts.architecture_arch005_checkout_guard as checkout_guard_cli
from ai_trading_system.cli import app
from ai_trading_system.platform.architecture.checkout_guard import (
    CHECKOUT_WORKTREE_AUDIT_SCHEMA_VERSION,
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
    # Synthetic text has a repository-local EOL contract even when the Git
    # system/global configuration is isolated; do not inherit host defaults.
    _git(tmp_path, "config", "core.autocrlf", "true")
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


def test_migration_blocks_intent_persistence_before_lease_acquire(
    git_checkout: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from test_devx015_workflow_coordination import _anchor_control, _registered_control

    from ai_trading_system.platform.architecture.workflow_coordination import (
        CONTROL_STATE_NAME,
        RETIREMENT_NAME,
        directory_identity,
    )

    guard = _guard(git_checkout)
    guard.store.root.mkdir(parents=True)
    fixture = guard.runtime_root / "control-fixture"
    fixture.mkdir()
    _repo, control, _policy, state = _registered_control(fixture, monkeypatch)
    state["phase"] = "DRAINING"
    retired = {"root_identity": directory_identity(guard.store.root), "epoch": state["epoch"]}
    state["legacy_roots"] = [retired]
    (control / CONTROL_STATE_NAME).write_text(json.dumps(state), encoding="utf-8")
    _anchor_control(_repo, control, monkeypatch)
    (guard.store.root / RETIREMENT_NAME).write_text(
        json.dumps(
            {
                "schema_version": "workflow_legacy_retirement.v1",
                "control_root": control.as_posix(),
                **retired,
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(CheckoutGuardError, match="LEGACY_WRITER_RETIRED"):
        _acquire_mutation(
            guard, intent_id="blocked-before-intent", task_id="TASK-A", owned_paths=("src/a.py",)
        )
    assert not (guard.runtime_root / "intents" / "blocked-before-intent.json").exists()
    assert not guard.store.replay().active_leases


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


def test_git_audits_do_not_execute_repository_fsmonitor(tmp_path: Path) -> None:
    def git(*args: str) -> None:
        subprocess.run(["git", *args], cwd=tmp_path, check=True, capture_output=True)

    git("init", "-q")
    source = tmp_path / "source.txt"
    source.write_text("initial\n", encoding="utf-8")
    git("add", "--", "source.txt")
    source.write_text("changed\n", encoding="utf-8")
    hook = tmp_path / ".git" / "fsmonitor-probe.sh"
    hook.write_text(
        "#!/bin/sh\nprintf 'invoked\\n' >> .git/fsmonitor-witness\nprintf 'probe\\000'\n",
        encoding="utf-8", newline="\n",
    )
    git("config", "core.fsmonitor", hook.as_posix())
    index = tmp_path / ".git" / "index"
    before = index.read_bytes()
    identity = (index.stat().st_dev, index.stat().st_ino)
    assert collect_checkout_dirty_paths(tmp_path, exclusions=()) == ("source.txt",)
    for cached in (False, True):
        checkout_guard_module._run_git_diff_check(tmp_path, exclusions=(), cached=cached)
    assert not (tmp_path / ".git" / "fsmonitor-witness").exists()
    assert index.read_bytes() == before
    assert (index.stat().st_dev, index.stat().st_ino) == identity
    # Positive control: prove this fixture's hook really executes under Git.
    git("status", "--porcelain=v1")
    assert (tmp_path / ".git" / "fsmonitor-witness").exists()


def test_dirty_path_audit_disables_git_optional_locks(
    git_checkout: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_environment: dict[str, str] = {}

    def fake_run(
        args: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[bytes]:
        environment = kwargs.get("env")
        assert isinstance(environment, dict)
        observed_environment.update(environment)
        return subprocess.CompletedProcess(args, 0, stdout=b"", stderr=b"")

    monkeypatch.setattr(checkout_guard_module.subprocess, "run", fake_run)

    assert collect_checkout_dirty_paths(git_checkout, exclusions=()) == ()
    assert observed_environment["GIT_OPTIONAL_LOCKS"] == "0"


def test_worktree_audit_excludes_known_unrelated_from_all_git_checks(
    git_checkout: Path,
) -> None:
    unrelated = git_checkout / "docs/research/growth_tilt_owner_diagnosis_pack.md"
    unrelated.write_text("owner bytes with trailing whitespace   \n", encoding="utf-8")
    guard = _guard(git_checkout)

    audit = guard.audit_worktree().to_dict()
    identity = resolve_checkout_identity(git_checkout)

    assert audit == {
        "schema_version": CHECKOUT_WORKTREE_AUDIT_SCHEMA_VERSION,
        "status": "PASS",
        "policy_repository": {
            "toplevel": identity.checkout_root,
            "git_common_dir": identity.git_common_dir,
            "workspace_id": identity.workspace_id,
            "head_commit": identity.head_commit,
            "branch_name": identity.branch_name,
        },
        "audited_repository": {
            "toplevel": identity.checkout_root,
            "git_common_dir": identity.git_common_dir,
            "workspace_id": identity.workspace_id,
            "head_commit": identity.head_commit,
            "branch_name": identity.branch_name,
        },
        "worktree_registration": {
            "toplevel": identity.checkout_root,
            "head_commit": identity.head_commit,
            "branch_ref": "refs/heads/fixture",
            "detached": False,
            "locked_reason": None,
            "prunable_reason": None,
        },
        "same_git_common_dir": True,
        "dirty_paths": [],
        "known_unrelated_exclusions": [
            "docs/research/growth_tilt_owner_diagnosis_pack.md",
        ],
        "unstaged_diff_check": "PASS",
        "staged_diff_check": "PASS",
        "task_governance_status_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }

    (git_checkout / "src/a.py").write_text("A = 2   \n", encoding="utf-8")
    with pytest.raises(
        CheckoutGuardError,
        match="CHECKOUT_GIT_DIFF_CHECK_FAILED.*unstaged.*trailing whitespace",
    ):
        guard.audit_worktree()

    (git_checkout / "src/a.py").write_text("A = 3   \n", encoding="utf-8")
    _git(git_checkout, "add", "src/a.py")
    with pytest.raises(
        CheckoutGuardError,
        match="CHECKOUT_GIT_DIFF_CHECK_FAILED.*staged.*trailing whitespace",
    ):
        guard.audit_worktree()


@pytest.mark.parametrize("staged", [False, True], ids=["unstaged", "staged"])
@pytest.mark.parametrize("trailing", [b"", b" ", b"\t"], ids=["clean", "space", "tab"])
def test_isolated_git_crlf_fixture_preserves_whitespace_gate(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    staged: bool,
    trailing: bytes,
) -> None:
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    git_checkout: Path = request.getfixturevalue("git_checkout")
    (git_checkout / "src/a.py").write_bytes(b"A = 2" + trailing + b"\r\n")
    if staged:
        _git(git_checkout, "add", "src/a.py")
    guard = _guard(git_checkout)

    if trailing:
        diff_kind = "staged" if staged else "unstaged"
        with pytest.raises(
            CheckoutGuardError,
            match=rf"CHECKOUT_GIT_DIFF_CHECK_FAILED.*\b{diff_kind}:.*trailing whitespace",
        ):
            guard.audit_worktree()
    else:
        audit = guard.audit_worktree().to_dict()
        assert audit["status"] == "PASS"
        assert audit["dirty_paths"] == ["src/a.py"]
        assert audit["unstaged_diff_check"] == "PASS"
        assert audit["staged_diff_check"] == "PASS"


def test_worktree_audit_injects_exact_literal_exclusion_into_every_git_call(
    git_checkout: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    guard = _guard(git_checkout)
    calls: list[list[str]] = []
    real_run = subprocess.run

    def fake_run(args: list[str], **_: object) -> subprocess.CompletedProcess[bytes]:
        if "status" in args or "diff" in args:
            calls.append(args)
            return subprocess.CompletedProcess(args, 0, stdout=b"", stderr=b"")
        return real_run(args, **_)

    monkeypatch.setattr(checkout_guard_module.subprocess, "run", fake_run)

    guard.audit_worktree()

    exclusion = ":(exclude,literal)docs/research/growth_tilt_owner_diagnosis_pack.md"
    assert len(calls) == 3
    assert all(call[-1] == exclusion for call in calls)
    assert all(call[:5] == [
        "git", "-c", "core.quotepath=false", "-c", "core.fsmonitor=false",
    ] for call in calls)
    assert calls[0][5:10] == [
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
        "--ignore-submodules=none",
    ]
    assert all(call[5:7] == ["-c", "diff.autoRefreshIndex=false"] for call in calls[1:])
    assert calls[1][7:10] == ["diff", "--check", "--"]
    assert calls[2][7:11] == ["diff", "--cached", "--check", "--"]


def test_target_bound_worktree_audit_isolates_target_state(
    git_checkout: Path,
) -> None:
    target = git_checkout.with_name(f"{git_checkout.name}-target")
    _git(git_checkout, "worktree", "add", "-b", "target-audit", str(target))
    (target / "src/a.py").write_text("A = 2\n", encoding="utf-8")
    (target / "src/b.py").write_text("B = 2\n", encoding="utf-8")
    _git(target, "add", "src/b.py")
    (target / "src/new.py").write_text("NEW = 1\n", encoding="utf-8")
    unrelated = target / "docs/research/growth_tilt_owner_diagnosis_pack.md"
    unrelated.write_text("target owner bytes   \n", encoding="utf-8")

    policy_audit = _guard(git_checkout).audit_worktree().to_dict()
    target_audit = (
        _guard(target)
        .audit_worktree(
            policy_project_root=git_checkout,
        )
        .to_dict()
    )

    assert policy_audit["dirty_paths"] == []
    assert target_audit["dirty_paths"] == [
        "src/a.py",
        "src/b.py",
        "src/new.py",
    ]
    assert target_audit["policy_repository"]["toplevel"] == str(git_checkout.resolve())
    assert target_audit["audited_repository"]["toplevel"] == str(target.resolve())
    assert target_audit["worktree_registration"]["branch_ref"] == ("refs/heads/target-audit")
    assert target_audit["same_git_common_dir"] is True


def test_target_bound_worktree_audit_cli_uses_explicit_target_repository(
    git_checkout: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    target = git_checkout.with_name(f"{git_checkout.name}-cli-target")
    _git(git_checkout, "worktree", "add", "-b", "cli-target-audit", str(target))
    (target / "src/new.py").write_text("NEW = 1\n", encoding="utf-8")
    monkeypatch.setattr(checkout_guard_cli, "PROJECT_ROOT", git_checkout)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "architecture_arch005_checkout_guard.py",
            "worktree-audit",
            "--target-repository",
            str(target),
        ],
    )

    assert checkout_guard_cli.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["audited_repository"]["toplevel"] == str(target.resolve())
    assert payload["dirty_paths"] == ["src/new.py"]


def test_target_bound_worktree_audit_rejects_invalid_targets(
    git_checkout: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(CheckoutGuardError, match="CHECKOUT_AUDIT_TARGET_NOT_FOUND"):
        checkout_guard_module.resolve_worktree_audit_binding(
            policy_project_root=git_checkout,
            audited_project_root=git_checkout / "missing",
        )

    with pytest.raises(
        CheckoutGuardError,
        match="CHECKOUT_AUDIT_TARGET_ROOT_MISMATCH",
    ):
        _guard(git_checkout / "src").audit_worktree(
            policy_project_root=git_checkout,
        )

    independent = git_checkout.with_name(f"{git_checkout.name}-independent")
    _git(tmp_path, "clone", str(git_checkout), str(independent))
    with pytest.raises(
        CheckoutGuardError,
        match="CHECKOUT_AUDIT_GIT_COMMON_DIR_MISMATCH",
    ):
        _guard(independent).audit_worktree(
            policy_project_root=git_checkout,
        )

    target = git_checkout.with_name(f"{git_checkout.name}-unregistered-target")
    _git(git_checkout, "worktree", "add", "-b", "unregistered-target", str(target))
    registrations = checkout_guard_module._registered_worktrees(git_checkout)
    monkeypatch.setattr(
        checkout_guard_module,
        "_registered_worktrees",
        lambda _: tuple(row for row in registrations if Path(row.toplevel) != target.resolve()),
    )
    with pytest.raises(
        CheckoutGuardError,
        match="CHECKOUT_AUDIT_TARGET_UNREGISTERED",
    ):
        _guard(target).audit_worktree(
            policy_project_root=git_checkout,
        )


def test_target_bound_worktree_audit_fails_on_identity_drift(
    git_checkout: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding = checkout_guard_module.resolve_worktree_audit_binding(
        policy_project_root=git_checkout,
        audited_project_root=git_checkout,
    )
    drifted = replace(
        binding,
        audited_identity=replace(
            binding.audited_identity,
            head_commit="0" * 40,
        ),
    )
    bindings = iter((binding, drifted))
    monkeypatch.setattr(
        checkout_guard_module,
        "resolve_worktree_audit_binding",
        lambda **_: next(bindings),
    )

    with pytest.raises(
        CheckoutGuardError,
        match="CHECKOUT_AUDIT_IDENTITY_DRIFT",
    ):
        _guard(git_checkout).audit_worktree(
            policy_project_root=git_checkout,
        )


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
    head = next(lease for lease in replay.lease_heads if lease.lease_id == decision.lease_id)
    assert head.state == "RELEASED"
    event_path = next((guard.store.events_root / head.lease_id).glob("*.json"))
    events = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (guard.store.events_root / head.lease_id).glob("*.json")
    ]
    assert event_path.is_file()
    assert any(
        "CHECKOUT_RELEASE_DIRTY_UNATTRIBUTED:src/late.py" in event["reason_codes"]
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


def test_disjoint_live_writers_acquire_after_dirty_and_release_interleaved(
    git_checkout: Path,
) -> None:
    """L02: two real writers retain both edits through the original guard/store."""
    driver = r'''
import json, os, subprocess, sys
from datetime import UTC, datetime
from pathlib import Path
from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutLeaseGuard, CheckoutOperationClass,
)
root = Path.cwd()
name = sys.argv[1]
relative = 'src/' + name + '.py'
guard = CheckoutLeaseGuard(
    project_root=root, runtime_root=root/'outputs/architecture/checkout-guard-test',
)
decision, handle = guard.acquire(
    intent_id='l02-'+name, task_id='L02-'+name.upper(), thread_id='writer-'+name,
    actor='architecture-control-plane', operation_class=CheckoutOperationClass.DOMAIN_MUTATION,
    owned_paths=(relative,), now=datetime.now(UTC),
)
if handle is not None:
    (root/relative).write_text(name.upper()+' = 2\n', encoding='utf-8')
print(json.dumps({'status':decision.status, 'reasons':decision.reason_codes,
                  'lease_id':decision.lease_id, 'pid':os.getpid()}), flush=True)
if handle is None:
    raise SystemExit(0)
assert sys.stdin.readline().strip() == 'commit-release'
with guard.store.atomic(actor='architecture-control-plane', now=datetime.now(UTC)):
    subprocess.run(['git','add','--',relative], check=True, capture_output=True)
    subprocess.run(['git','commit','-m','L02 writer '+name,'--',relative],
                   check=True, capture_output=True)
handle.release(outcome='completed', at=datetime.now(UTC))
released = next(row for row in guard.replay().lease_heads if row.lease_id == handle.lease_id)
print(json.dumps({'released':released.state, 'pid':os.getpid()}), flush=True)
'''
    # Genuine child imports, API, Git and store; no admission/identity patch.
    environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"),
                       PYTHONDONTWRITEBYTECODE="1")
    children: list[subprocess.Popen[str]] = []
    observations = []
    try:
        for name in ("a", "b"):
            child = subprocess.Popen(
                [sys.executable, "-c", driver, name], cwd=git_checkout, env=environment,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            children.append(child)
            assert child.stdout is not None
            row = json.loads(child.stdout.readline())
            observations.append(row)
            assert row["status"] == "PASS", row
            assert child.poll() is None
            assert (git_checkout / f"src/{name}.py").read_text() == f"{name.upper()} = 2\n"
        assert observations[0]["pid"] != observations[1]["pid"]
        assert len(_guard(git_checkout).replay().active_leases) == 2
        for child in children:
            output, error = child.communicate("commit-release\n", timeout=60)
            assert child.returncode == 0, output + error
            assert json.loads(output)["released"] == "RELEASED"
        assert not _guard(git_checkout).replay().active_leases
        for name in ("a", "b"):
            committed = subprocess.check_output(
                ["git", "show", f"HEAD:src/{name}.py"], cwd=git_checkout,
            )
            assert committed == f"{name.upper()} = 2\n".encode()
    finally:
        for child in children:
            if child.poll() is None:
                child.communicate("commit-release\n", timeout=60)
        (git_checkout.parent / "l02-writer-observations.json").write_text(
            json.dumps(observations), encoding="utf-8",
        )
        (git_checkout.parent / "l02-original-driver.py").write_text(driver, encoding="utf-8")


def test_l02_two_process_index_operations_serialize(git_checkout: Path) -> None:
    """Actual competing arbiter entrants, Git trace and committed bytes are the oracle."""
    driver = r'''
import json, os, subprocess, sys
from datetime import UTC, datetime
from pathlib import Path
from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutLeaseGuard, CheckoutOperationClass,
)
from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
root = Path.cwd()
name = sys.argv[1]
relative = 'src/' + name + '.py'
guard = CheckoutLeaseGuard(
    project_root=root, runtime_root=root/'outputs/architecture/checkout-guard-test',
)
def report(state, **fields):
    print(json.dumps({'state': state, 'pid': os.getpid(), **fields}), flush=True)
def commit():
    subprocess.run(['git', 'add', '--', relative], check=True, capture_output=True)
    subprocess.run(['git', 'commit', '-m', 'index writer '+name, '--', relative],
                   check=True, capture_output=True)
decision, handle = guard.acquire(
    intent_id='index-'+name, task_id='L02-'+name.upper(), thread_id='index-'+name,
    actor='architecture-control-plane', operation_class=CheckoutOperationClass.DOMAIN_MUTATION,
    owned_paths=(relative,), now=datetime.now(UTC),
)
assert decision.status == 'PASS' and handle is not None, decision
(root/relative).write_text(name.upper()+' = 3\n', encoding='utf-8')
report('READY')
assert sys.stdin.readline().strip() == 'ATTEMPT'
if name == 'a':
    with guard.store.atomic(actor='architecture-control-plane', now=datetime.now(UTC)):
        report('HELD')
        assert sys.stdin.readline().strip() == 'COMMIT'
        commit()
else:
    try:
        with guard.store.atomic(actor='architecture-control-plane', now=datetime.now(UTC)):
            commit()
    except ParallelControlError as error:
        report('DENIED', reason=str(error))
    else:
        report('WRONGLY_ENTERED')
    assert sys.stdin.readline().strip() == 'RETRY'
    with guard.store.atomic(actor='architecture-control-plane', now=datetime.now(UTC)):
        commit()
handle.release(outcome='completed', at=datetime.now(UTC))
report('DONE')
'''
    children: list[subprocess.Popen[str]] = []
    observations: list[dict[str, object]] = []
    traces = [git_checkout.parent / f"index-{name}-trace.jsonl" for name in ("a", "b")]
    reader = ThreadPoolExecutor(max_workers=2)

    def receive(child: subprocess.Popen[str]) -> dict[str, object]:
        assert child.stdout is not None
        row = json.loads(reader.submit(child.stdout.readline).result(timeout=30))
        observations.append(row)
        return row

    def send(child: subprocess.Popen[str], command: str) -> None:
        assert child.stdin is not None
        child.stdin.write(command + "\n")
        child.stdin.flush()

    def git_writes(trace: Path) -> list[str]:
        return [row["name"] for line in trace.read_text(encoding="utf-8").splitlines()
                if (row := json.loads(line)).get("event") == "cmd_name"
                and row.get("name") in {"add", "commit"}]

    try:
        for name, trace in zip(("a", "b"), traces, strict=True):
            environment = dict(
                os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"),
                PYTHONDONTWRITEBYTECODE="1", GIT_TRACE2_EVENT=str(trace),
            )
            child = subprocess.Popen(
                [sys.executable, "-c", driver, name], cwd=git_checkout, env=environment,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            children.append(child)
            assert receive(child)["state"] == "READY"
        assert observations[0]["pid"] != observations[1]["pid"]
        index = (git_checkout / ".git/index").read_bytes()
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=git_checkout)
        send(children[0], "ATTEMPT")
        assert receive(children[0])["state"] == "HELD"
        send(children[1], "ATTEMPT")
        denied = receive(children[1])
        assert denied["state"] == "DENIED", denied
        assert "LEASE_ARBITER_BUSY" in str(denied["reason"])
        assert (git_checkout / ".git/index").read_bytes() == index
        assert subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=git_checkout) == head
        assert all(git_writes(trace) == [] for trace in traces)
        send(children[0], "COMMIT")
        assert receive(children[0])["state"] == "DONE"
        send(children[1], "RETRY")
        assert receive(children[1])["state"] == "DONE"
        for child in children:
            output, error = child.communicate(timeout=30)
            assert child.returncode == 0, output + error
        assert [git_writes(trace) for trace in traces] == [["add", "commit"], ["add", "commit"]]
        for name in ("a", "b"):
            assert subprocess.check_output(
                ["git", "show", f"HEAD:src/{name}.py"], cwd=git_checkout,
            ) == f"{name.upper()} = 3\n".encode()
        assert not _guard(git_checkout).replay().active_leases
        assert not (git_checkout / ".git/index.lock").exists()
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=30)
        reader.shutdown(wait=True)
        (git_checkout.parent / "index-driver.py").write_text(driver, encoding="utf-8")
        (git_checkout.parent / "index-observations.json").write_text(
            json.dumps(observations), encoding="utf-8",
        )


@pytest.mark.parametrize(
    "case", ["released", "expired", "source-only", "corrupt-intent", "overlap"],
)
def test_live_dirty_attribution_does_not_grant_invalid_or_overlapping_claims(
    git_checkout: Path, case: str,
) -> None:
    guard = _guard(git_checkout)
    instant = datetime.now(UTC)
    first, owner = guard.acquire(
        intent_id="l02-owner", task_id="L02-OWNER", thread_id="owner",
        actor="architecture-control-plane",
        operation_class=(CheckoutOperationClass.SHARED_MUTATION if case == "source-only"
                         else CheckoutOperationClass.DOMAIN_MUTATION),
        owned_paths=() if case == "source-only" else ("src/a.py",), now=instant,
        shared_paths=(
            tuple(sorted((checkout_guard_module.CHECKOUT_SOURCE_ONLY_RUNTIME, "src/a.py")))
            if case == "source-only" else ()
        ),
        inspection_profile=(checkout_guard_module.CHECKOUT_SOURCE_ONLY_PROFILE
                            if case == "source-only" else "FULL_WORKTREE"),
    )
    assert first.status == "PASS" and owner is not None
    original_intent = first.intent_path.read_bytes()
    (git_checkout / "src/a.py").write_text("A = 2\n", encoding="utf-8")
    attempt_time = instant + timedelta(seconds=1)
    if case == "released":
        owner.release(outcome="completed", at=attempt_time)
    elif case == "expired":
        attempt_time = instant + timedelta(seconds=guard.lease_policy.lease_ttl_seconds + 1)
    elif case == "corrupt-intent":
        payload = json.loads(original_intent)
        payload["actor"] = "operations-automation"
        first.intent_path.write_text(json.dumps(payload), encoding="utf-8")
    second_owner = None
    try:
        kwargs = dict(
            intent_id="l02-second", task_id="L02-SECOND", thread_id="second",
            actor="architecture-control-plane",
            operation_class=CheckoutOperationClass.DOMAIN_MUTATION,
            owned_paths=("src/a.py" if case == "overlap" else "src/b.py",), now=attempt_time,
        )
        if case == "corrupt-intent":
            with pytest.raises(CheckoutGuardError, match="CHECKOUT_LEASE_INTENT_BINDING"):
                guard.acquire(**kwargs)
        else:
            second, second_owner = guard.acquire(**kwargs)
            assert second.status == "BLOCKED" and second_owner is None
            expected = "LEASE_RESOURCE_CONFLICT:" if case == "overlap" else (
                "CHECKOUT_DIRTY_UNATTRIBUTED:src/a.py"
            )
            assert any(reason.startswith(expected) for reason in second.reason_codes)
        assert (git_checkout / "src/b.py").read_text() == "B = 1\n"
    finally:
        first.intent_path.write_bytes(original_intent)
        if second_owner is not None:
            second_owner.release(outcome="failed", at=attempt_time + timedelta(seconds=1))
        if not owner.released:
            owner.release(outcome="completed", at=attempt_time + timedelta(seconds=2))


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
