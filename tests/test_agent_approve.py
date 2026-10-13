"""DEVX-029: scripts/agent/approve.ps1 writes owner decision records into the review root."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
from test_agent_scripts import powershell  # noqa: E402, I001

APPROVE = ROOT / "scripts/agent/approve.ps1"
DECISION = "owner_decision:T:2026-10-13:x"
RECORD_NAME = "T.2026-10-13.x.json"


def run_git(repo: Path, *args: str) -> str:
    done = subprocess.run(
        ["git", "-C", str(repo), *args],
        text=True,
        capture_output=True,
        encoding="utf-8",
        check=True,
    )
    return done.stdout.strip()


def setup_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    run_git(tmp_path, "init", "-b", "main", str(repo))
    run_git(repo, "config", "user.email", "t@example.invalid")
    run_git(repo, "config", "user.name", "t")
    (repo / "tools/gov008").mkdir(parents=True)
    (repo / "scripts/agent").mkdir(parents=True)
    shutil.copy2(ROOT / "tools/gov008/ship.py", repo / "tools/gov008/ship.py")
    config = {
        "runs_root": str(tmp_path / "runs"),
        "review_root": str(tmp_path / "reviews"),
        "decision_drafts_root": str(tmp_path / "drafts"),
        "agent_clone": str(tmp_path / "agent"),
    }
    (repo / "scripts/agent/agent_config.json").write_text(json.dumps(config), encoding="utf-8")
    run_git(repo, "add", "-A")
    run_git(repo, "commit", "-m", "init")
    return repo


def write_draft(path: Path, **overrides: object) -> str:
    today = dt.datetime.now(dt.UTC).date()
    record = {
        "schema": "aits.owner_decision/1",
        "decision": DECISION,
        "approved_at": f"{today.isoformat()}T00:10:00Z",
        "owner_words": "approve decision T and write it into the review root",
        "c_paths": ["secret/app/**"],
        "expected_result": "secret/app only",
        "expires": (today + dt.timedelta(days=7)).isoformat(),
    }
    record.update(overrides)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record), encoding="utf-8")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_approve(
    repo: Path, *args: str, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    if env is None:
        env = {k: v for k, v in os.environ.items() if not k.startswith("AITS_PI_")}
    return subprocess.run(
        [
            powershell(),
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(APPROVE),
            "-Repo",
            str(repo),
            "-Python",
            sys.executable,
            *args,
        ],
        text=True,
        capture_output=True,
        timeout=90,
        env=env,
    )


def test_approve_writes_the_checked_draft_once(tmp_path: Path) -> None:
    repo = setup_repo(tmp_path)
    draft = tmp_path / "drafts" / "d.json"
    digest = write_draft(draft)
    done = run_approve(repo, "-Draft", str(draft), "-ExpectSha256", digest, "-Yes")
    assert done.returncode == 0, done.stdout + done.stderr
    target = tmp_path / "reviews" / "decisions" / RECORD_NAME
    assert target.read_bytes() == draft.read_bytes()
    assert DECISION in done.stdout and "secret/app/**" in done.stdout
    again = run_approve(repo, "-Draft", str(draft), "-ExpectSha256", digest, "-Yes")
    assert again.returncode == 0
    assert "identical record already exists" in again.stdout


def test_approve_never_overwrites_a_different_record(tmp_path: Path) -> None:
    repo = setup_repo(tmp_path)
    first = tmp_path / "drafts" / "a.json"
    assert (
        run_approve(
            repo, "-Draft", str(first), "-ExpectSha256", write_draft(first), "-Yes"
        ).returncode
        == 0
    )
    second = tmp_path / "drafts" / "b.json"
    digest = write_draft(second, c_paths=["secret/**"])
    done = run_approve(repo, "-Draft", str(second), "-ExpectSha256", digest, "-Yes")
    assert done.returncode == 2
    assert "not overwritten" in done.stdout
    target = tmp_path / "reviews" / "decisions" / RECORD_NAME
    assert target.read_bytes() == first.read_bytes()


@pytest.mark.parametrize(
    ("where", "args", "problem"),
    [
        ("drafts", ["-Yes"], "-Yes needs -ExpectSha256"),
        ("drafts", ["-ExpectSha256", "0" * 64, "-Yes"], "does not match ExpectSha256"),
        ("runs", ["-ExpectSha256", "HASH", "-Yes"], "must be inside decision_drafts_root"),
        ("reviews", ["-ExpectSha256", "HASH", "-Yes"], "must be inside decision_drafts_root"),
    ],
)
def test_approve_refusals_write_nothing(
    tmp_path: Path, where: str, args: list[str], problem: str
) -> None:
    repo = setup_repo(tmp_path)
    draft = tmp_path / where / "d.json"
    digest = write_draft(draft)
    done = run_approve(repo, "-Draft", str(draft), *[digest if a == "HASH" else a for a in args])
    assert done.returncode == 2
    assert problem in done.stdout
    assert not (tmp_path / "reviews" / "decisions" / RECORD_NAME).exists()


def test_approve_rejects_an_invalid_draft(tmp_path: Path) -> None:
    repo = setup_repo(tmp_path)
    draft = tmp_path / "drafts" / "d.json"
    digest = write_draft(draft, expires="2026-01-01", approved_at="2025-12-30T00:00:00Z")
    done = run_approve(repo, "-Draft", str(draft), "-ExpectSha256", digest, "-Yes")
    assert done.returncode == 2
    assert "draft rejected" in done.stdout and "expired" in done.stdout
    assert not (tmp_path / "reviews" / "decisions").exists()


def test_approve_refuses_under_pi_environment(tmp_path: Path) -> None:
    repo = setup_repo(tmp_path)
    draft = tmp_path / "drafts" / "d.json"
    digest = write_draft(draft)
    env = {**os.environ, "AITS_PI_RUN": "X-01"}
    done = run_approve(repo, "-Draft", str(draft), "-ExpectSha256", digest, "-Yes", env=env)
    assert done.returncode == 2
    assert "AITS_PI environment variable is set" in done.stdout


def test_approve_without_yes_needs_a_typed_y(tmp_path: Path) -> None:
    repo = setup_repo(tmp_path)
    draft = tmp_path / "drafts" / "d.json"
    write_draft(draft)
    done = run_approve(repo, "-Draft", str(draft))
    assert done.returncode != 0
    assert not (tmp_path / "reviews" / "decisions" / RECORD_NAME).exists()


def test_approve_reads_config_from_base_not_the_branch(tmp_path: Path) -> None:
    repo = setup_repo(tmp_path)
    run_git(repo, "switch", "-c", "task/x")
    config = json.loads((repo / "scripts/agent/agent_config.json").read_text(encoding="utf-8"))
    config["decision_drafts_root"] = str(tmp_path / "runs")
    (repo / "scripts/agent/agent_config.json").write_text(json.dumps(config), encoding="utf-8")
    run_git(repo, "commit", "-am", "move drafts root on the branch")
    draft = tmp_path / "runs" / "d.json"
    digest = write_draft(draft)
    done = run_approve(repo, "-Draft", str(draft), "-ExpectSha256", digest, "-Yes")
    assert done.returncode == 2
    assert "must be inside decision_drafts_root" in done.stdout
