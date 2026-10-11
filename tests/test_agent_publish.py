from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
from test_agent_scripts import powershell  # noqa: E402, I001

PUBLISH = ROOT / "scripts/agent/publish.ps1"
MINI_POLICY = {
    "schema_version": "gov008_ship_policy.v1",
    "tree_clean_exclusions": [],
    "zones": {"C": ["secret/**"], "A": ["docs/**", "*.md"]},
    "gates": {"A": [], "B": [], "C": []},
}


def run_git(repo: Path, *args: str) -> str:
    done = subprocess.run(
        ["git", "-C", str(repo), *args],
        text=True,
        capture_output=True,
        encoding="utf-8",
        check=True,
    )
    return done.stdout.strip()


def commit_file(repo: Path, rel: str, text: str, message: str) -> None:
    path = repo / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    run_git(repo, "add", rel)
    run_git(repo, "commit", "-m", message)


def write_acceptance(run_dir: Path, verdict: str = "accepted") -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "acceptance.json").write_text(
        json.dumps({"reviews": [{"verdict": verdict}]}), encoding="utf-8"
    )


def setup_repo(tmp_path: Path) -> tuple[Path, Path, Path]:
    repo = tmp_path / "repo"
    origin = tmp_path / "origin.git"
    run_git(tmp_path, "init", "--bare", "-b", "main", str(origin))
    run_git(tmp_path, "init", "-b", "main", str(repo))
    run_git(repo, "config", "user.email", "t@example.invalid")
    run_git(repo, "config", "user.name", "t")
    (repo / "config").mkdir()
    (repo / "tools/gov008").mkdir(parents=True)
    (repo / "config/gov008_ship.yaml").write_text(yaml.safe_dump(MINI_POLICY), encoding="utf-8")
    shutil.copy2(ROOT / "tools/gov008/ship.py", repo / "tools/gov008/ship.py")
    (repo / "README.md").write_text("start\n", encoding="utf-8")
    run_git(repo, "add", "-A")
    run_git(repo, "commit", "-m", "init")
    run_git(repo, "remote", "add", "origin", str(origin))
    run_git(repo, "push", "-u", "origin", "main")
    run_git(repo, "switch", "-c", "task/x")
    return repo, origin, tmp_path / "run"


def run_ship(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-I", str(repo / "tools/gov008/ship.py"), "--repo", str(repo), *args],
        text=True,
        capture_output=True,
        timeout=60,
    )


def show_main_ship(repo: Path, path: Path) -> None:
    data = subprocess.run(
        ["git", "-C", str(repo), "show", "main:tools/gov008/ship.py"],
        capture_output=True,
        check=True,
    ).stdout
    path.write_bytes(data)


def run_publish(
    repo: Path, run_dir: Path, *extra: str, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    ps = powershell()
    if env is None:
        env = {k: v for k, v in os.environ.items() if not k.startswith("AITS_PI_")}
    return subprocess.run(
        [
            ps,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(PUBLISH),
            "-Branch",
            "task/x",
            "-RunDir",
            str(run_dir),
            "-Repo",
            str(repo),
            "-Python",
            sys.executable,
            *extra,
        ],
        text=True,
        capture_output=True,
        timeout=90,
        env=env,
    )


def test_publish_success_fast_forwards_pushes_deletes_branch(tmp_path: Path) -> None:
    repo, origin, run_dir = setup_repo(tmp_path)
    write_acceptance(run_dir)
    commit_file(repo, "src/app.py", "x\n", "code")
    assert run_ship(repo, "--base", "main", "--gate").returncode == 0
    done = run_publish(repo, run_dir)
    assert done.returncode == 0, done.stdout + done.stderr
    data = json.loads((run_dir / "publish.json").read_bytes().decode("utf-8"))
    assert not (run_dir / "publish.json").read_bytes().startswith(b"\xef\xbb\xbf")
    expected_fields = {
        "run",
        "mode",
        "ok",
        "started_at",
        "ended_at",
        "main_sha",
        "gate_trailer",
        "pushed",
        "branch_deleted",
        "summary",
    }
    assert expected_fields <= set(data)
    assert data["ok"] is True and data["branch_deleted"] is True
    assert (
        run_git(repo, "rev-parse", "main")
        == run_git(origin, "rev-parse", "main")
        == data["main_sha"]
    )
    assert run_git(repo, "branch", "--show-current") == "main"
    assert "task/x" not in run_git(repo, "branch", "--list", "task/x")


def test_publish_uses_ship_py_from_main_not_task_branch(tmp_path: Path) -> None:
    repo, _origin, run_dir = setup_repo(tmp_path)
    write_acceptance(run_dir)
    commit_file(repo, "src/app.py", "x\n", "code")
    main_ship = tmp_path / "main-ship.py"
    show_main_ship(repo, main_ship)
    run_git(repo, "add", "src/app.py")
    (repo / "tools/gov008/ship.py").write_text("raise SystemExit(99)\n", encoding="utf-8")
    run_git(repo, "add", "tools/gov008/ship.py")
    run_git(repo, "commit", "-m", "break branch ship")
    gate = subprocess.run(
        [sys.executable, "-I", str(main_ship), "--repo", str(repo), "--base", "main", "--gate"],
        text=True,
        capture_output=True,
        timeout=60,
    )
    assert gate.returncode == 0, gate.stdout + gate.stderr
    done = run_publish(repo, run_dir)
    assert done.returncode == 0, done.stdout + done.stderr
    assert json.loads((run_dir / "publish.json").read_text(encoding="utf-8"))["ok"] is True


def test_publish_fails_without_gate_trailer_and_restores_branch(tmp_path: Path) -> None:
    repo, _origin, run_dir = setup_repo(tmp_path)
    write_acceptance(run_dir)
    before = run_git(repo, "rev-parse", "main")
    commit_file(repo, "src/app.py", "x\n", "code")
    done = run_publish(repo, run_dir)
    assert done.returncode == 1
    assert json.loads((run_dir / "publish.json").read_text(encoding="utf-8"))["ok"] is False
    assert run_git(repo, "rev-parse", "main") == before
    assert run_git(repo, "branch", "--show-current") == "task/x"
    assert "task/x" in run_git(repo, "branch", "--list", "task/x")


def test_publish_fails_after_extra_commit_and_restores_branch(tmp_path: Path) -> None:
    repo, _origin, run_dir = setup_repo(tmp_path)
    write_acceptance(run_dir)
    before = run_git(repo, "rev-parse", "main")
    commit_file(repo, "src/app.py", "x\n", "code")
    assert run_ship(repo, "--base", "main", "--gate").returncode == 0
    gate = run_git(repo, "log", "-1", "--format=%(trailers:key=Gate,valueonly)")
    commit_file(repo, "src/next.py", "y\n", f"next\n\nGate: {gate}")
    done = run_publish(repo, run_dir)
    assert done.returncode == 1
    assert json.loads((run_dir / "publish.json").read_text(encoding="utf-8"))["ok"] is False
    assert run_git(repo, "rev-parse", "main") == before
    assert run_git(repo, "branch", "--show-current") == "task/x"


def test_publish_validation_rejects_unaccepted_review_without_json(tmp_path: Path) -> None:
    repo, _origin, run_dir = setup_repo(tmp_path)
    write_acceptance(run_dir, verdict="rejected")
    before = run_git(repo, "rev-parse", "main")
    done = run_publish(repo, run_dir)
    assert done.returncode == 2
    assert not (run_dir / "publish.json").exists()
    assert run_git(repo, "rev-parse", "main") == before


def test_publish_validation_rejects_aits_pi_environment(tmp_path: Path) -> None:
    repo, _origin, run_dir = setup_repo(tmp_path)
    write_acceptance(run_dir)
    env = os.environ.copy()
    env["AITS_PI_RUN"] = "leak"
    done = run_publish(repo, run_dir, env=env)
    assert done.returncode == 2
    assert not (run_dir / "publish.json").exists()


def test_publish_no_push_moves_main_but_not_origin(tmp_path: Path) -> None:
    repo, origin, run_dir = setup_repo(tmp_path)
    write_acceptance(run_dir)
    before_origin = run_git(origin, "rev-parse", "main")
    commit_file(repo, "src/app.py", "x\n", "code")
    assert run_ship(repo, "--base", "main", "--gate").returncode == 0
    done = run_publish(repo, run_dir, "-NoPush")
    assert done.returncode == 0, done.stdout + done.stderr
    data = json.loads((run_dir / "publish.json").read_text(encoding="utf-8"))
    assert data["pushed"] is False
    assert run_git(repo, "rev-parse", "main") == data["main_sha"]
    assert run_git(origin, "rev-parse", "main") == before_origin
