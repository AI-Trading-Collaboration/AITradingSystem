from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import typer

import ai_trading_system.cli_commands.ops as ops_cli
from ai_trading_system.ops_scheduler_checkout import (
    DEFAULT_OPS_SCHEDULER_CHECKOUT_POLICY_PATH,
    inspect_ops_scheduler_checkout,
    load_ops_scheduler_checkout_policy,
)


@pytest.fixture
def isolated_checkout(tmp_path: Path) -> tuple[Path, str]:
    root = tmp_path / "ops-checkout"
    root.mkdir()
    (root / "tracked.txt").write_text("ops\n", encoding="utf-8")
    _git(root, "init")
    _git(root, "config", "user.email", "ops-checkout@example.com")
    _git(root, "config", "user.name", "Ops Checkout")
    _git(
        root,
        "remote",
        "add",
        "origin",
        "git@github.com:AI-Trading-Collaboration/AITradingSystem.git",
    )
    _git(root, "add", ".")
    _git(root, "commit", "-m", "fixture")
    return root, _git(root, "rev-parse", "HEAD")


def test_policy_keeps_scheduler_deployment_owner_gated() -> None:
    policy = load_ops_scheduler_checkout_policy(
        DEFAULT_OPS_SCHEDULER_CHECKOUT_POLICY_PATH
    )

    assert policy.status == "REVIEWED_ENGINEERING_READY_OWNER_DEPLOYMENT_REQUIRED"
    assert policy.unified_external_trigger == ("aits", "ops", "daily-run")
    assert policy.separate_periodic_scheduler_entries_allowed is False
    assert policy.activation_authorized is False
    assert policy.scheduler_installed is False
    assert policy.scheduler_enabled is False


def test_clean_exact_independent_checkout_passes_without_authorizing_activation(
    isolated_checkout: tuple[Path, str],
    tmp_path: Path,
) -> None:
    checkout, commit = isolated_checkout
    development_root = tmp_path / "development"
    development_root.mkdir()
    payload = inspect_ops_scheduler_checkout(
        project_root=development_root,
        env={
            "AITS_EXTERNAL_SCHEDULER": "1",
            "AITS_OPS_CHECKOUT_ROOT": str(checkout),
            "AITS_OPS_RELEASE_COMMIT": commit,
            "AITS_DEVELOPMENT_CHECKOUT_ROOT": str(development_root),
        },
    )

    assert payload["status"] == "PASS"
    assert payload["scheduler_execution_ready"] is True
    assert payload["activation_authorized"] is False
    assert payload["scheduler_installed"] is False
    assert payload["scheduler_enabled"] is False
    assert payload["production_effect"] == "none"


def test_runtime_preflight_requires_current_process_to_use_isolated_checkout(
    isolated_checkout: tuple[Path, str],
    tmp_path: Path,
) -> None:
    checkout, commit = isolated_checkout
    development_root = tmp_path / "development"
    development_root.mkdir()
    env = {
        "AITS_EXTERNAL_SCHEDULER": "1",
        "AITS_OPS_CHECKOUT_ROOT": str(checkout),
        "AITS_OPS_RELEASE_COMMIT": commit,
        "AITS_DEVELOPMENT_CHECKOUT_ROOT": str(development_root),
    }

    isolated = inspect_ops_scheduler_checkout(
        project_root=checkout,
        env=env,
        require_current_process_checkout=True,
    )
    development = inspect_ops_scheduler_checkout(
        project_root=development_root,
        env=env,
        require_current_process_checkout=True,
    )

    assert isolated["status"] == "PASS"
    assert development["status"] == "BLOCKED"
    assert "OPS_CHECKOUT_CURRENT_PROCESS_CHECKOUT" in development["blocker_codes"]


def test_dirty_or_unpinned_checkout_fails_closed(
    isolated_checkout: tuple[Path, str],
    tmp_path: Path,
) -> None:
    checkout, commit = isolated_checkout
    development_root = tmp_path / "development"
    development_root.mkdir()
    (checkout / "untracked.txt").write_text("dirty\n", encoding="utf-8")

    payload = inspect_ops_scheduler_checkout(
        project_root=development_root,
        env={
            "AITS_EXTERNAL_SCHEDULER": "1",
            "AITS_OPS_CHECKOUT_ROOT": str(checkout),
            "AITS_OPS_RELEASE_COMMIT": "0" * 40,
            "AITS_DEVELOPMENT_CHECKOUT_ROOT": str(development_root),
        },
    )

    assert payload["status"] == "BLOCKED"
    assert "OPS_CHECKOUT_EXACT_RELEASE_COMMIT" in payload["blocker_codes"]
    assert "OPS_CHECKOUT_CLEAN_CHECKOUT" in payload["blocker_codes"]
    assert payload["head_commit"] == commit
    assert payload["scheduler_execution_ready"] is False


def test_external_scheduler_marker_blocks_daily_before_checkout_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    monkeypatch.setenv("AITS_EXTERNAL_SCHEDULER", "1")
    monkeypatch.setattr(
        ops_cli,
        "inspect_ops_scheduler_checkout",
        lambda **_: {
            "status": "BLOCKED",
            "blocker_codes": ["OPS_CHECKOUT_CLEAN_CHECKOUT"],
        },
    )
    monkeypatch.setattr(
        ops_cli,
        "write_ops_scheduler_checkout_preflight",
        lambda *_args, **_kwargs: calls.append("preflight_written"),
    )
    monkeypatch.setattr(
        ops_cli,
        "hold_daily_checkout_guard",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("checkout guard must not run after scheduler preflight block")
        ),
    )

    with pytest.raises(typer.Exit):
        ops_cli.daily_ops_run_command()

    assert calls == ["preflight_written"]


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ("git", "-C", str(root), *args),
        check=True,
        text=True,
        capture_output=True,
    )
    return result.stdout.strip()
