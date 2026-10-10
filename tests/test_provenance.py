"""GOV-008 P2 spike: run provenance block (commit, code_modified, config content hashes)."""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

import pytest

from ai_trading_system.core import provenance


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def _repo(tmp_path: Path) -> Path:
    _git(tmp_path, "init", "-b", "main")
    _git(tmp_path, "config", "user.email", "t@example.invalid")
    _git(tmp_path, "config", "user.name", "t")
    (tmp_path / "config").mkdir()
    # bytes, not write_text: on Windows text mode would turn "\n" into "\r\n" and change the hash
    (tmp_path / "config/rules.yaml").write_bytes(b"a: 1\n")
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs/note.md").write_text("n\n", encoding="utf-8")
    _git(tmp_path, "add", "-A")
    _git(tmp_path, "commit", "-m", "init")
    return tmp_path


def test_config_hash_is_the_content_hash(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    block = provenance.provenance_block({"rules": repo / "config/rules.yaml"})
    assert block["config_sha256"]["rules"] == hashlib.sha256(b"a: 1\n").hexdigest()


def test_clean_repo_reports_commit_and_unmodified_code(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    git = provenance.provenance_block({"rules": repo / "config/rules.yaml"})["git"]
    assert len(git["commit"]) == 40 and git["branch"] == "main" and git["code_modified"] is False


def test_modified_config_marks_code_modified_but_notes_do_not(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    (repo / "docs/note.md").write_text("changed\n", encoding="utf-8")
    assert provenance.git_provenance(repo)["code_modified"] is False
    (repo / "config/rules.yaml").write_text("a: 2\n", encoding="utf-8")
    assert provenance.git_provenance(repo)["code_modified"] is True


def test_missing_repository_and_file_are_explicit_not_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path.parent))
    block = provenance.provenance_block({"x": tmp_path / "nope.yaml"})
    assert block["config_sha256"]["x"] == provenance.UNAVAILABLE
    assert block["git"] == {
        "commit": provenance.UNAVAILABLE,
        "branch": provenance.UNAVAILABLE,
        "code_modified": provenance.UNAVAILABLE,
    }
