from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ai_trading_system.platform.architecture.checkout_guard import CheckoutGuardError
from ai_trading_system.platform.architecture.checkout_reconciliation import (
    CHECKOUT_HANDOFF_SCHEMA_VERSION,
    CHECKOUT_RECONCILIATION_REPORT_SCHEMA_VERSION,
    CheckoutHandoffMode,
    ReconciliationClassification,
    build_checkout_handoff,
    build_checkout_reconciliation_report,
    load_checkout_reconciliation_policy,
    validate_checkout_handoff,
    validate_checkout_reconciliation_report,
)

NOW = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)


@pytest.fixture
def handoff_checkouts(tmp_path: Path) -> tuple[Path, Path]:
    source = tmp_path / "source"
    target = tmp_path / "target"
    source.mkdir()
    (source / "src").mkdir()
    (source / "generated").mkdir()
    (source / "docs/research").mkdir(parents=True)
    (source / "src/a.py").write_text("A = 1\n", encoding="utf-8")
    (source / "generated/view.yaml").write_text("version: 1\n", encoding="utf-8")
    (source / "AGENTS.md").write_text("rules v1\n", encoding="utf-8")
    unrelated = source / "docs/research/growth_tilt_owner_diagnosis_pack.md"
    unrelated.write_text("owner bytes v1\n", encoding="utf-8")
    _git(source, "init", "-b", "main")
    _git(source, "config", "user.email", "s4e@example.com")
    _git(source, "config", "user.name", "S4E Test")
    _git(source, "add", ".")
    _git(source, "commit", "-m", "base")
    _git(source, "worktree", "add", "-b", "task-branch", str(target), "HEAD")
    return source, target


def test_policy_freezes_cleanup_and_protected_main_boundaries() -> None:
    policy = load_checkout_reconciliation_policy()

    assert policy.status == "OWNER_APPROVED_S0_S1"
    assert policy.approval_ref == (
        "owner_decision:ARCH-005S4E:2026-07-25:"
        "approve_checkout_handoff_reconciliation_v1"
    )
    assert policy.protected_branches == ("main",)
    assert policy.domain_mutation_allowed is False
    assert policy.shared_mutation_actors == ("integration-coordinator",)
    assert policy.cleanup_eligible_classifications == (
        ReconciliationClassification.EXACT_TARGET,
        ReconciliationClassification.SUPERSEDED_IN_TARGET_HISTORY,
    )


def test_prepared_copy_is_bound_and_later_target_commit_can_supersede_it(
    handoff_checkouts: tuple[Path, Path],
) -> None:
    source, target = handoff_checkouts
    _write_both(source, target, "src/a.py", "A = 2\n")
    handoff = build_checkout_handoff(
        source_root=source,
        target_root=target,
        task_id="TASK-A",
        target_ref="task-branch",
        owned_paths=("src/a.py",),
        created_at=NOW,
    )

    assert handoff["schema_version"] == CHECKOUT_HANDOFF_SCHEMA_VERSION
    assert handoff["mode"] == "PREPARED_COPY"
    assert handoff["entries"][0]["copy_equal"] is True
    validate_checkout_handoff(handoff)

    _git(target, "add", "src/a.py")
    _git(target, "commit", "-m", "copy source bytes")
    (target / "src/a.py").write_text("A = 3\n", encoding="utf-8")
    _git(target, "add", "src/a.py")
    _git(target, "commit", "-m", "reviewed follow-up")

    report = build_checkout_reconciliation_report(
        handoff=handoff,
        source_root=source,
        target_root=target,
        target_ref="task-branch",
        created_at=NOW,
    )

    result = _result(report, "src/a.py")
    assert report["schema_version"] == CHECKOUT_RECONCILIATION_REPORT_SCHEMA_VERSION
    assert report["status"] == "PASS"
    assert report["decision"] == "READY_FOR_COORDINATOR_RECONCILIATION"
    assert result["classification"] == "SUPERSEDED_IN_TARGET_HISTORY"
    assert result["cleanup_eligible"] is True
    assert len(result["matching_history_commit"]) == 40
    assert report["cleanup_allowlist"] == ["src/a.py"]
    assert report["automatic_cleanup_allowed"] is False
    validate_checkout_reconciliation_report(report)


def test_source_change_after_handoff_is_mixed_and_never_cleanup_eligible(
    handoff_checkouts: tuple[Path, Path],
) -> None:
    source, target = handoff_checkouts
    _write_both(source, target, "src/a.py", "A = 2\n")
    handoff = build_checkout_handoff(
        source_root=source,
        target_root=target,
        task_id="TASK-A",
        target_ref="task-branch",
        owned_paths=("src/a.py",),
        created_at=NOW,
    )
    _git(target, "add", "src/a.py")
    _git(target, "commit", "-m", "target copy")
    (source / "src/a.py").write_text("A = 99\n", encoding="utf-8")

    report = build_checkout_reconciliation_report(
        handoff=handoff,
        source_root=source,
        target_root=target,
        created_at=NOW,
    )

    result = _result(report, "src/a.py")
    assert report["status"] == "BLOCKED"
    assert report["decision"] == "BLOCKED"
    assert result["classification"] == "MIXED_SPLIT_REQUIRED"
    assert result["cleanup_eligible"] is False
    assert report["cleanup_allowlist"] == []
    assert report["blocking_paths"] == ["src/a.py"]


def test_recovery_audit_classifies_generated_retained_and_unattributed_paths(
    handoff_checkouts: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, target = handoff_checkouts
    (source / "src/a.py").write_text("A = 2\n", encoding="utf-8")
    (target / "src/a.py").write_text("A = 2\n", encoding="utf-8")
    _git(target, "add", "src/a.py")
    _git(target, "commit", "-m", "target source bytes")
    (source / "generated/view.yaml").write_text("version: 2\n", encoding="utf-8")
    (source / "AGENTS.md").write_text("rules v2\n", encoding="utf-8")
    unrelated_path = (
        source / "docs/research/growth_tilt_owner_diagnosis_pack.md"
    ).resolve()
    original_read_bytes = Path.read_bytes

    def _reject_unrelated_read(path: Path) -> bytes:
        if path.resolve() == unrelated_path:
            raise AssertionError("known-unrelated bytes must not be read")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", _reject_unrelated_read)
    handoff = build_checkout_handoff(
        source_root=source,
        target_root=target,
        task_id="TASK-A",
        target_ref="task-branch",
        owned_paths=("src/a.py",),
        generated_paths=("generated/view.yaml",),
        retained_paths=("AGENTS.md",),
        mode=CheckoutHandoffMode.RECOVERY_AUDIT,
        created_at=NOW,
    )

    report = build_checkout_reconciliation_report(
        handoff=handoff,
        source_root=source,
        target_root=target,
        created_at=NOW,
    )

    assert report["status"] == "PASS"
    assert _result(report, "src/a.py")["classification"] == "EXACT_TARGET"
    assert _result(report, "generated/view.yaml")["classification"] == (
        "GENERATED_INVALIDATED"
    )
    assert _result(report, "AGENTS.md")["classification"] == "RETAIN_UNIQUE"
    unrelated = _result(
        report,
        "docs/research/growth_tilt_owner_diagnosis_pack.md",
    )
    assert unrelated["classification"] == "KNOWN_UNRELATED_NOT_READ"
    assert unrelated["bytes_observed"] is False
    assert unrelated["prepared_snapshot"] is None
    assert unrelated["current_snapshot"] is None
    assert report["generated_to_rebuild"] == ["generated/view.yaml"]
    assert report["retained_paths"] == ["AGENTS.md"]

    (source / "src/unexpected.py").write_text("X = 1\n", encoding="utf-8")
    blocked = build_checkout_reconciliation_report(
        handoff=handoff,
        source_root=source,
        target_root=target,
        created_at=NOW,
    )
    unexpected = _result(blocked, "src/unexpected.py")
    assert blocked["status"] == "BLOCKED"
    assert unexpected["classification"] == "UNATTRIBUTED_DIRTY"
    assert unexpected["bytes_observed"] is False


def test_copy_tamper_and_manifest_tamper_fail_closed(
    handoff_checkouts: tuple[Path, Path],
) -> None:
    source, target = handoff_checkouts
    (source / "src/a.py").write_text("A = 2\n", encoding="utf-8")
    (target / "src/a.py").write_text("A = 3\n", encoding="utf-8")

    with pytest.raises(CheckoutGuardError, match="CHECKOUT_HANDOFF_COPY_MISMATCH"):
        build_checkout_handoff(
            source_root=source,
            target_root=target,
            task_id="TASK-A",
            target_ref="task-branch",
            owned_paths=("src/a.py",),
            created_at=NOW,
        )

    (target / "src/a.py").write_text("A = 2\n", encoding="utf-8")
    handoff = build_checkout_handoff(
        source_root=source,
        target_root=target,
        task_id="TASK-A",
        target_ref="task-branch",
        owned_paths=("src/a.py",),
        created_at=NOW,
    )
    tampered = json.loads(json.dumps(handoff))
    tampered["task_id"] = "TASK-B"
    with pytest.raises(CheckoutGuardError, match="CHECKOUT_HANDOFF_CHECKSUM"):
        validate_checkout_handoff(tampered)

    _git(target, "add", "src/a.py")
    _git(target, "commit", "-m", "target copy")
    report = build_checkout_reconciliation_report(
        handoff=handoff,
        source_root=source,
        target_root=target,
        created_at=NOW,
    )
    report_tampered = json.loads(json.dumps(report))
    report_tampered["cleanup_allowlist"] = []
    with pytest.raises(
        CheckoutGuardError,
        match="CHECKOUT_RECONCILIATION_REPORT_ALLOWLIST",
    ):
        validate_checkout_reconciliation_report(report_tampered)


def test_non_descendant_recovery_target_fails_closed(
    handoff_checkouts: tuple[Path, Path],
) -> None:
    source, target = handoff_checkouts
    _git(target, "checkout", "--orphan", "unrelated")
    _git(target, "rm", "-rf", ".")
    (target / "other.txt").write_text("other\n", encoding="utf-8")
    _git(target, "add", "other.txt")
    _git(target, "commit", "-m", "unrelated root")

    with pytest.raises(
        CheckoutGuardError,
        match="CHECKOUT_HANDOFF_TARGET_NOT_DESCENDANT",
    ):
        build_checkout_handoff(
            source_root=source,
            target_root=target,
            task_id="TASK-A",
            target_ref="unrelated",
            owned_paths=("src/a.py",),
            mode=CheckoutHandoffMode.RECOVERY_AUDIT,
            created_at=NOW,
        )


def test_second_parent_only_recovery_target_fails_closed(
    handoff_checkouts: tuple[Path, Path],
) -> None:
    source, target = handoff_checkouts
    _git(target, "checkout", "--orphan", "second-parent-only")
    _git(target, "rm", "-rf", ".")
    (target / "other.txt").write_text("other\n", encoding="utf-8")
    _git(target, "add", "other.txt")
    _git(target, "commit", "-m", "alternate first parent")
    _git(
        target,
        "merge",
        "--allow-unrelated-histories",
        "-s",
        "ours",
        "main",
        "-m",
        "base only through second parent",
    )

    with pytest.raises(
        CheckoutGuardError,
        match="CHECKOUT_HANDOFF_TARGET_NOT_DESCENDANT",
    ):
        build_checkout_handoff(
            source_root=source,
            target_root=target,
            task_id="TASK-A",
            target_ref="second-parent-only",
            owned_paths=("src/a.py",),
            mode=CheckoutHandoffMode.RECOVERY_AUDIT,
            created_at=NOW,
        )


def _result(report: dict[str, object], path: str) -> dict[str, object]:
    rows = report["results"]
    assert isinstance(rows, list)
    return next(row for row in rows if row["path"] == path)


def _write_both(source: Path, target: Path, path: str, value: str) -> None:
    (source / path).write_text(value, encoding="utf-8")
    (target / path).write_text(value, encoding="utf-8")


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.stdout.strip()
