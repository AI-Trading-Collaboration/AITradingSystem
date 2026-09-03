from __future__ import annotations

import json
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import typer

import ai_trading_system.cli_commands.ops as ops_cli
import ai_trading_system.ops_scheduler_checkout as scheduler_checkout
from ai_trading_system.ops_scheduler_checkout import (
    DEFAULT_OPS_SCHEDULER_CHECKOUT_POLICY_PATH,
    OpsSchedulerTerminalDisposition,
    inspect_ops_scheduler_checkout,
    load_ops_scheduler_checkout_policy,
    resolve_ops_scheduler_terminal_disposition,
)

EXPECTED_REMOTE = "git@github.com:AI-Trading-Collaboration/AITradingSystem.git"


@pytest.fixture
def independent_checkouts(tmp_path: Path) -> tuple[Path, Path, str]:
    development = tmp_path / "development"
    checkout = tmp_path / "ops-checkout"
    _init_repo(development, "development")
    checkout_commit = _init_repo(checkout, "ops")
    return development, checkout, checkout_commit


def test_policy_requires_receipt_gated_independent_clone() -> None:
    policy = load_ops_scheduler_checkout_policy(DEFAULT_OPS_SCHEDULER_CHECKOUT_POLICY_PATH)

    assert policy.status == "REVIEWED_RECEIPT_GATED_DEPLOYMENT"
    assert policy.unified_external_trigger == ("aits", "ops", "daily-run")
    assert policy.manual_execution_option == "--manual-execution"
    assert policy.separate_periodic_scheduler_entries_allowed is False
    assert policy.independent_git_common_dir_required is True
    assert policy.activation_mode == "ACTIVE_OWNER_ACCEPTED_RECEIPT_REQUIRED"
    assert policy.release_identity_authority == "active_deployment_receipt"
    assert policy.legacy_release_assertion_mode == "exact_match_if_present"
    assert policy.acceptance_schema == "ops_deployment_acceptance.v2"
    assert policy.legacy_acceptance_schema == "ops_deployment_acceptance.v1"
    assert policy.scheduler_provider == "codex_automation"
    assert policy.scheduler_entry_count == 1
    assert policy.windows_task_scheduler_entries_allowed is False
    assert policy.terminal_dispositions == tuple(
        item.value for item in OpsSchedulerTerminalDisposition
    )
    assert policy.same_as_of_ordinary_allowed is False
    assert policy.ordinary_requires_as_of_strictly_after_parent is True
    assert policy.ordinary_requires_fresh_idempotency_key is True
    assert policy.ordinary_requires_no_active_lock is True
    assert policy.nonrecoverable_parent_recovery_allowed is False
    assert policy.parent_bytes_must_remain_immutable is True
    assert policy.new_business_scheduler_entry_allowed is False


def test_terminal_disposition_waits_when_same_as_of_is_not_recoverable() -> None:
    decision = resolve_ops_scheduler_terminal_disposition(
        parent_status="FAILED",
        parent_as_of=date(2026, 7, 31),
        resolved_as_of=date(2026, 7, 31),
        recovery_eligible=False,
        fresh_state_exists=True,
        fresh_lock_exists=False,
        active_deployment_accepted=True,
    )

    assert decision["disposition"] == ("WAIT_FOR_NEXT_PROVIDER_READY_AS_OF_ORDINARY")
    assert decision["trigger_allowed"] is False
    assert decision["trigger_mode"] == "NONE"
    assert decision["same_as_of_ordinary_allowed"] is False
    assert decision["parent_bytes_must_remain_immutable"] is True


def test_terminal_disposition_allows_reviewed_same_as_of_tail_recovery() -> None:
    decision = resolve_ops_scheduler_terminal_disposition(
        parent_status="BLOCKED",
        parent_as_of=date(2026, 7, 31),
        resolved_as_of=date(2026, 7, 31),
        recovery_eligible=True,
        fresh_state_exists=True,
        fresh_lock_exists=False,
        active_deployment_accepted=True,
    )

    assert decision["disposition"] == "RECOVERABLE_SAME_AS_OF_TAIL"
    assert decision["trigger_allowed"] is True
    assert decision["trigger_mode"] == "RECOVERY"
    assert decision["recovery_arguments_required"] is True


def test_terminal_disposition_allows_new_as_of_ordinary_on_fresh_key() -> None:
    decision = resolve_ops_scheduler_terminal_disposition(
        parent_status="FAILED",
        parent_as_of=date(2026, 7, 31),
        resolved_as_of=date(2026, 8, 3),
        recovery_eligible=False,
        fresh_state_exists=False,
        fresh_lock_exists=False,
        active_deployment_accepted=True,
    )

    assert decision["disposition"] == "READY_FOR_NEW_AS_OF_ORDINARY"
    assert decision["trigger_allowed"] is True
    assert decision["trigger_mode"] == "ORDINARY"
    assert decision["recovery_arguments_required"] is False
    assert decision["scheduler_entry_count"] == 1
    assert decision["unified_external_trigger"] == ["aits", "ops", "daily-run"]


@pytest.mark.parametrize(
    ("fresh_state_exists", "fresh_lock_exists", "active_deployment_accepted", "reason"),
    [
        (True, False, True, "FRESH_IDEMPOTENCY_KEY_STATE_ALREADY_EXISTS"),
        (False, True, True, "FRESH_IDEMPOTENCY_KEY_ACTIVE_LOCK_EXISTS"),
        (False, False, False, "ACTIVE_DEPLOYMENT_NOT_ACCEPTED"),
    ],
)
def test_terminal_disposition_blocks_unsafe_new_as_of_ordinary(
    fresh_state_exists: bool,
    fresh_lock_exists: bool,
    active_deployment_accepted: bool,
    reason: str,
) -> None:
    decision = resolve_ops_scheduler_terminal_disposition(
        parent_status="FAILED",
        parent_as_of=date(2026, 7, 31),
        resolved_as_of=date(2026, 8, 3),
        recovery_eligible=False,
        fresh_state_exists=fresh_state_exists,
        fresh_lock_exists=fresh_lock_exists,
        active_deployment_accepted=active_deployment_accepted,
    )

    assert decision["disposition"] == "BLOCKED_EXTERNAL_OR_OWNER"
    assert decision["trigger_allowed"] is False
    assert reason in decision["reason_codes"]


def test_terminal_disposition_blocks_as_of_regression_and_external_boundary() -> None:
    regressed = resolve_ops_scheduler_terminal_disposition(
        parent_status="FAILED",
        parent_as_of=date(2026, 7, 31),
        resolved_as_of=date(2026, 7, 30),
        recovery_eligible=False,
        fresh_state_exists=False,
        fresh_lock_exists=False,
        active_deployment_accepted=True,
    )
    external = resolve_ops_scheduler_terminal_disposition(
        parent_status="FAILED",
        parent_as_of=date(2026, 7, 31),
        resolved_as_of=date(2026, 8, 3),
        recovery_eligible=False,
        fresh_state_exists=False,
        fresh_lock_exists=False,
        active_deployment_accepted=True,
        external_or_owner_blocked=True,
    )

    assert regressed["reason_codes"] == ["RESOLVED_AS_OF_PRECEDES_PARENT"]
    assert external["reason_codes"] == ["EXTERNAL_OR_OWNER_BLOCKED"]
    assert regressed["trigger_allowed"] is False
    assert external["trigger_allowed"] is False


def test_clean_exact_independent_clone_passes_candidate_only(
    independent_checkouts: tuple[Path, Path, str],
) -> None:
    development, checkout, commit = independent_checkouts
    payload = inspect_ops_scheduler_checkout(
        project_root=development,
        env=_scheduler_env(development, checkout, commit),
    )

    assert payload["status"] == "PASS"
    assert payload["scheduler_execution_ready"] is True
    assert payload["activation_authorized"] is False
    assert payload["owner_deployment_required"] is True
    assert payload["git_common_dir"] != payload["development_git_common_dir"]
    assert payload["production_effect"] == "none"


def test_linked_worktree_fails_independent_git_common_dir(tmp_path: Path) -> None:
    development = tmp_path / "development"
    commit = _init_repo(development, "shared")
    checkout = tmp_path / "linked-ops"
    _git(development, "worktree", "add", "--detach", str(checkout), commit)

    payload = inspect_ops_scheduler_checkout(
        project_root=development,
        env=_scheduler_env(development, checkout, commit),
    )

    assert payload["status"] == "BLOCKED"
    assert "OPS_CHECKOUT_INDEPENDENT_GIT_COMMON_DIR" in payload["blocker_codes"]
    assert payload["git_common_dir"] == payload["development_git_common_dir"]


def test_runtime_preflight_requires_current_process_to_use_isolated_checkout(
    independent_checkouts: tuple[Path, Path, str],
) -> None:
    development, checkout, commit = independent_checkouts
    env = _scheduler_env(development, checkout, commit)

    isolated = inspect_ops_scheduler_checkout(
        project_root=checkout,
        env=env,
        require_current_process_checkout=True,
    )
    development_process = inspect_ops_scheduler_checkout(
        project_root=development,
        env=env,
        require_current_process_checkout=True,
    )

    assert isolated["status"] == "PASS"
    assert development_process["status"] == "BLOCKED"
    assert "OPS_CHECKOUT_CURRENT_PROCESS_CHECKOUT" in development_process["blocker_codes"]


def test_dirty_or_unpinned_checkout_fails_closed(
    independent_checkouts: tuple[Path, Path, str],
) -> None:
    development, checkout, commit = independent_checkouts
    (checkout / "untracked.txt").write_text("dirty\n", encoding="utf-8")

    payload = inspect_ops_scheduler_checkout(
        project_root=development,
        env=_scheduler_env(development, checkout, "0" * 40),
    )

    assert payload["status"] == "BLOCKED"
    assert "OPS_CHECKOUT_EXACT_RELEASE_COMMIT" in payload["blocker_codes"]
    assert "OPS_CHECKOUT_REVIEWED_REMOTE_REF" in payload["blocker_codes"]
    assert "OPS_CHECKOUT_CLEAN_CHECKOUT" in payload["blocker_codes"]
    assert payload["head_commit"] == commit


def test_active_scheduler_mode_requires_receipt_and_runtime_python(
    independent_checkouts: tuple[Path, Path, str],
) -> None:
    development, checkout, commit = independent_checkouts

    payload = inspect_ops_scheduler_checkout(
        project_root=checkout,
        env=_scheduler_env(development, checkout, commit),
        require_current_process_checkout=True,
        require_active_deployment=True,
    )

    assert payload["status"] == "BLOCKED"
    assert "OPS_CHECKOUT_ACTIVE_DEPLOYMENT_RECEIPT" in payload["blocker_codes"]
    assert payload["activation_authorized"] is False


def test_valid_active_receipt_authorizes_scheduler_mode(
    independent_checkouts: tuple[Path, Path, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    development, checkout, commit = independent_checkouts
    receipt = checkout / "outputs" / "operations" / "deployment" / "active.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text(
        json.dumps(
            {
                "deployment_id": "ops_deployment_fixture",
                "release": {"candidate_commit": commit},
            }
        ),
        encoding="utf-8",
    )
    runtime_python = checkout / ".venv" / "Scripts" / "python.exe"
    runtime_python.parent.mkdir(parents=True)
    runtime_python.write_bytes(b"fixture")
    monkeypatch.setattr(
        scheduler_checkout,
        "load_ops_release_promotion_policy",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        scheduler_checkout,
        "validate_ops_deployment_acceptance",
        lambda *_args, **_kwargs: None,
    )

    env = _scheduler_env(development, checkout, None)
    env["AITS_OPS_DEPLOYMENT_RECEIPT"] = str(receipt)
    env["AITS_OPS_PYTHON"] = str(runtime_python)
    payload = inspect_ops_scheduler_checkout(
        project_root=checkout,
        env=env,
        require_current_process_checkout=True,
        require_active_deployment=True,
    )

    assert payload["status"] == "PASS"
    assert payload["activation_authorized"] is True
    assert payload["scheduler_installed"] is True
    assert payload["scheduler_enabled"] is True
    assert payload["owner_deployment_required"] is False
    assert payload["release_commit"] == commit
    assert payload["release_commit_source"] == "active_deployment_receipt"
    assert payload["legacy_release_assertion_provided"] is False


def test_active_receipt_rejects_mismatched_legacy_release_assertion(
    independent_checkouts: tuple[Path, Path, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    development, checkout, commit = independent_checkouts
    receipt = checkout / "outputs" / "operations" / "deployment" / "active.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text(
        json.dumps(
            {
                "deployment_id": "ops_deployment_fixture",
                "release": {"candidate_commit": commit},
            }
        ),
        encoding="utf-8",
    )
    runtime_python = checkout / ".venv" / "Scripts" / "python.exe"
    runtime_python.parent.mkdir(parents=True)
    runtime_python.write_bytes(b"fixture")
    monkeypatch.setattr(
        scheduler_checkout,
        "load_ops_release_promotion_policy",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        scheduler_checkout,
        "validate_ops_deployment_acceptance",
        lambda *_args, **_kwargs: None,
    )
    env = _scheduler_env(development, checkout, "0" * 40)
    env["AITS_OPS_DEPLOYMENT_RECEIPT"] = str(receipt)
    env["AITS_OPS_PYTHON"] = str(runtime_python)

    payload = inspect_ops_scheduler_checkout(
        project_root=checkout,
        env=env,
        require_current_process_checkout=True,
        require_active_deployment=True,
    )

    assert payload["status"] == "BLOCKED"
    assert "OPS_CHECKOUT_LEGACY_RELEASE_ASSERTION" in payload["blocker_codes"]
    assert payload["release_commit"] == commit
    assert payload["legacy_release_assertion_matches_receipt"] is False


def test_terminal_recovery_derives_current_release_from_active_receipt(
    tmp_path: Path,
) -> None:
    commit = "a" * 40
    parent_commit = "b" * 40
    run_root = tmp_path / "runs"
    manifest_path = run_root / "daily" / "2026-08-31" / "parent" / "manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        json.dumps(
            {
                "run_id": "parent-run",
                "as_of": "2026-08-31",
                "git_commit": parent_commit,
            }
        ),
        encoding="utf-8",
    )
    receipt = tmp_path / "active.json"
    receipt.write_text(
        json.dumps({"release": {"candidate_commit": commit}}),
        encoding="utf-8",
    )

    request = ops_cli._build_daily_terminal_recovery_request(
        as_of=date(2026, 8, 31),
        run_output_root=run_root,
        parent_run_id="parent-run",
        recovery_from_step="artifact_lineage",
        recovery_reason_code="FIXED_CODE_DEFECT",
        requested_at=datetime.now(tz=UTC),
        env={"AITS_OPS_DEPLOYMENT_RECEIPT": str(receipt)},
    )

    assert request is not None
    assert request.current_release_commit == commit
    assert request.parent_release_commit == parent_commit


def test_terminal_recovery_rejects_mismatched_legacy_release_assertion(
    tmp_path: Path,
) -> None:
    run_root = tmp_path / "runs"
    manifest_path = run_root / "daily" / "2026-08-31" / "parent" / "manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        json.dumps(
            {
                "run_id": "parent-run",
                "as_of": "2026-08-31",
                "git_commit": "b" * 40,
            }
        ),
        encoding="utf-8",
    )
    receipt = tmp_path / "active.json"
    receipt.write_text(
        json.dumps({"release": {"candidate_commit": "a" * 40}}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must exactly match active deployment receipt"):
        ops_cli._build_daily_terminal_recovery_request(
            as_of=date(2026, 8, 31),
            run_output_root=run_root,
            parent_run_id="parent-run",
            recovery_from_step="artifact_lineage",
            recovery_reason_code="FIXED_CODE_DEFECT",
            requested_at=datetime.now(tz=UTC),
            env={
                "AITS_OPS_DEPLOYMENT_RECEIPT": str(receipt),
                "AITS_OPS_RELEASE_COMMIT": "c" * 40,
            },
        )


def test_scheduler_preflight_writes_only_inside_checkout_write_guard(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    monkeypatch.setenv("AITS_EXTERNAL_SCHEDULER", "1")
    monkeypatch.setattr(ops_cli, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        ops_cli,
        "load_ops_scheduler_checkout_policy",
        lambda: SimpleNamespace(
            scheduler_marker_name="AITS_EXTERNAL_SCHEDULER",
            active_receipt_relative_path="outputs/operations/deployment/active.json",
        ),
    )

    @contextmanager
    def fake_guard(**_kwargs: object) -> Iterator[None]:
        calls.append("guard_enter")
        try:
            yield
        finally:
            calls.append("guard_exit")

    monkeypatch.setattr(ops_cli, "hold_daily_checkout_guard", fake_guard)
    monkeypatch.setattr(
        ops_cli,
        "inspect_ops_scheduler_checkout",
        lambda **_: (
            calls.append("preflight_inspected")
            or {
                "status": "BLOCKED",
                "blocker_codes": ["OPS_CHECKOUT_ACTIVE_DEPLOYMENT_RECEIPT"],
            }
        ),
    )
    monkeypatch.setattr(
        ops_cli,
        "write_ops_scheduler_checkout_preflight",
        lambda *_args, **_kwargs: calls.append("preflight_written"),
    )

    with pytest.raises(typer.Exit):
        ops_cli.daily_ops_run_command()

    assert calls == [
        "guard_enter",
        "preflight_inspected",
        "preflight_written",
        "guard_exit",
    ]


def test_active_receipt_detects_missing_scheduler_marker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    active_receipt = tmp_path / "outputs" / "operations" / "deployment" / "active.json"
    active_receipt.parent.mkdir(parents=True)
    active_receipt.write_text("{}\n", encoding="utf-8")
    monkeypatch.delenv("AITS_EXTERNAL_SCHEDULER", raising=False)
    monkeypatch.setattr(ops_cli, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        ops_cli,
        "load_ops_scheduler_checkout_policy",
        lambda: SimpleNamespace(
            scheduler_marker_name="AITS_EXTERNAL_SCHEDULER",
            active_receipt_relative_path="outputs/operations/deployment/active.json",
        ),
    )

    @contextmanager
    def fake_guard(**_kwargs: object) -> Iterator[None]:
        calls.append("guard")
        yield

    monkeypatch.setattr(ops_cli, "hold_daily_checkout_guard", fake_guard)
    monkeypatch.setattr(
        ops_cli,
        "inspect_ops_scheduler_checkout",
        lambda **_: {
            "status": "BLOCKED",
            "blocker_codes": ["OPS_CHECKOUT_SCHEDULER_MARKER"],
        },
    )
    monkeypatch.setattr(
        ops_cli,
        "write_ops_scheduler_checkout_preflight",
        lambda *_args, **_kwargs: calls.append("written"),
    )

    with pytest.raises(typer.Exit):
        ops_cli.daily_ops_run_command()

    assert calls == ["guard", "written"]


def test_daily_run_without_scheduler_or_manual_mode_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    monkeypatch.delenv("AITS_EXTERNAL_SCHEDULER", raising=False)
    monkeypatch.setattr(ops_cli, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        ops_cli,
        "load_ops_scheduler_checkout_policy",
        lambda: SimpleNamespace(
            scheduler_marker_name="AITS_EXTERNAL_SCHEDULER",
            active_receipt_relative_path="outputs/operations/deployment/active.json",
        ),
    )

    @contextmanager
    def fake_guard(**_kwargs: object) -> Iterator[None]:
        calls.append("guard")
        yield

    monkeypatch.setattr(ops_cli, "hold_daily_checkout_guard", fake_guard)

    with pytest.raises(typer.Exit):
        ops_cli.daily_ops_run_command()

    assert calls == ["guard"]


def _scheduler_env(
    development: Path,
    checkout: Path,
    commit: str | None,
) -> dict[str, str]:
    payload = {
        "AITS_EXTERNAL_SCHEDULER": "1",
        "AITS_OPS_CHECKOUT_ROOT": str(checkout),
        "AITS_DEVELOPMENT_CHECKOUT_ROOT": str(development),
    }
    if commit is not None:
        payload["AITS_OPS_RELEASE_COMMIT"] = commit
    return payload


def _init_repo(root: Path, content: str) -> str:
    root.mkdir()
    checkout_policy = (
        Path(__file__).resolve().parents[1]
        / "config"
        / "architecture"
        / "arch_005_s4d_checkout_guard.yaml"
    )
    target_policy = root / "config" / "architecture" / checkout_policy.name
    target_policy.parent.mkdir(parents=True)
    target_policy.write_text(checkout_policy.read_text(encoding="utf-8"), encoding="utf-8")
    (root / ".gitignore").write_text("outputs/\n.venv/\n", encoding="utf-8")
    (root / "tracked.txt").write_text(f"{content}\n", encoding="utf-8")
    _git(root, "init")
    _git(root, "config", "user.email", "ops-checkout@example.com")
    _git(root, "config", "user.name", "Ops Checkout")
    _git(root, "remote", "add", "origin", EXPECTED_REMOTE)
    _git(root, "add", ".")
    _git(root, "commit", "-m", "fixture")
    commit = _git(root, "rev-parse", "HEAD")
    _git(root, "update-ref", "refs/remotes/origin/main", commit)
    return commit


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ("git", "-C", str(root), *args),
        check=True,
        text=True,
        capture_output=True,
    )
    return result.stdout.strip()
