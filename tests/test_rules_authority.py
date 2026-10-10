"""Pinned AGENTS.md authority: current bytes, or the bytes in force when the record was written."""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

import pytest

from ai_trading_system.core.rules_authority import rules_authority_matches

RULES_V1 = b"# rules v1\ndefault research and backtest start: 2021-02-22;\n"
RULES_V2 = b"# rules v2, reworded\ndefault research and backtest start: 2021-02-22;\n"


def _git(root: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-c", "user.email=t@example.com", "-c", "user.name=t", "-C", str(root), *args],
        capture_output=True,
        check=True,
    )


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path.resolve()
    _git(root, "init", "-q")
    (root / "AGENTS.md").write_bytes(RULES_V1)
    (root / "record.yaml").write_text(f"rules_sha256: {_sha(RULES_V1)}\n", encoding="utf-8")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "record written under rules v1")
    return root


def test_current_rules_with_pinned_bytes_match(repo: Path) -> None:
    assert rules_authority_matches(
        project_root=repo, pinned_sha256=_sha(RULES_V1), record_path=repo / "record.yaml"
    )


def test_rewritten_rules_still_match_the_pin_of_an_earlier_record(repo: Path) -> None:
    (repo / "AGENTS.md").write_bytes(RULES_V2)
    _git(repo, "commit", "-q", "-am", "rewrite rules")
    assert rules_authority_matches(
        project_root=repo, pinned_sha256=_sha(RULES_V1), record_path=repo / "record.yaml"
    )


def test_a_pin_that_was_never_true_when_the_record_was_written_fails(repo: Path) -> None:
    (repo / "AGENTS.md").write_bytes(RULES_V2)
    _git(repo, "commit", "-q", "-am", "rewrite rules")
    for pin in (_sha(RULES_V2 + b"x"), "0" * 64):
        assert not rules_authority_matches(
            project_root=repo, pinned_sha256=pin, record_path=repo / "record.yaml"
        )


def test_record_rewritten_after_the_rules_change_must_pin_the_new_rules(repo: Path) -> None:
    (repo / "AGENTS.md").write_bytes(RULES_V2)
    (repo / "record.yaml").write_text("rules_sha256: still-v1\nnote: edited\n", encoding="utf-8")
    _git(repo, "commit", "-q", "-am", "rules and record both change")
    assert not rules_authority_matches(
        project_root=repo, pinned_sha256=_sha(RULES_V1), record_path=repo / "record.yaml"
    )


def test_untracked_record_or_outside_path_has_no_history(repo: Path, tmp_path: Path) -> None:
    (repo / "AGENTS.md").write_bytes(RULES_V2)
    (repo / "new_record.yaml").write_text("x\n", encoding="utf-8")
    assert not rules_authority_matches(
        project_root=repo, pinned_sha256=_sha(RULES_V1), record_path=repo / "new_record.yaml"
    )
    assert not rules_authority_matches(
        project_root=repo, pinned_sha256=_sha(RULES_V1), record_path=tmp_path.parent / "x.yaml"
    )
