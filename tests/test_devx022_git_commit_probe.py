"""DEVX-022 17.12: the Full runner's `git rev-parse HEAD` probe has a calibrated hang guard.

A hard-coded 5 s used to time out on a saturated host (100% CPU, ~600 processes during a Full),
which made the written summary carry git_commit "unknown" and failed FULL_RESULT_SUMMARY_BINDING.
The probe keeps its fail-closed meaning (a timeout or a failure is None) but waits long enough.
"""

from __future__ import annotations

import inspect
import re
import subprocess
from pathlib import Path
from typing import Any

import pytest

from scripts import run_validation_tier as runner

SHA = "a" * 40


def _completed(stdout: str, code: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(("git", "rev-parse", "HEAD"), code, stdout, "")


def test_the_probe_waits_with_the_named_hang_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: dict[str, Any] = {}

    def fake_run(argv: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        seen.update(kwargs)
        seen["argv"] = tuple(argv)
        return _completed(f"{SHA}\n")

    monkeypatch.setattr(subprocess, "run", fake_run)
    assert runner._git_commit(tmp_path) == SHA
    assert seen["argv"] == ("git", "rev-parse", "HEAD")
    assert seen["timeout"] == runner.GIT_COMMIT_PROBE_TIMEOUT_SECONDS


def test_the_hang_guard_is_far_above_what_a_saturated_host_needs() -> None:
    # 5 s timed out in the formal Full of p-20261009-v1; the guard must stay well above that and
    # well below the Full's own budgets (the leased Full command has hours).
    assert 30 <= runner.GIT_COMMIT_PROBE_TIMEOUT_SECONDS <= 600


def test_no_single_digit_literal_timeout_remains_in_the_probe() -> None:
    source = inspect.getsource(runner._git_commit)
    assert re.search(r"timeout\s*=\s*\d\b", source) is None
    assert "GIT_COMMIT_PROBE_TIMEOUT_SECONDS" in source


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        (_completed(f"{SHA}\n"), SHA),
        (_completed(f"  {SHA}  \n"), SHA),
        (_completed("", 0), None),
        (_completed("fatal: not a git repository\n", 128), None),
        (subprocess.TimeoutExpired(("git",), 120), None),
        (FileNotFoundError("git"), None),
    ],
)
def test_a_timeout_a_failure_or_empty_output_is_none_and_never_a_made_up_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: Any, expected: str | None
) -> None:
    def fake_run(argv: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome  # type: ignore[no-any-return]

    monkeypatch.setattr(subprocess, "run", fake_run)
    assert runner._git_commit(tmp_path) == expected


def test_the_probe_reads_the_real_head_of_a_git_repository(tmp_path: Path) -> None:
    def git(*args: str) -> str:
        return subprocess.run(
            ["git", *args], cwd=tmp_path, check=True, capture_output=True, text=True
        ).stdout.strip()

    git("init", "-b", "main")
    git("config", "user.email", "probe@example.com")
    git("config", "user.name", "Probe")
    (tmp_path / "a.txt").write_text("a\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-m", "first")
    assert runner._git_commit(tmp_path) == git("rev-parse", "HEAD")
