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


def current_tree(repo: Path, ref: str = "HEAD") -> str:
    return run_git(repo, "rev-parse", f"{ref}^{{tree}}")


def review_dir(run_dir: Path) -> Path:
    # setup_repo commits agent_config.json with review_root = <tmp>/reviews on main.
    return run_dir.parent / "reviews" / run_dir.name


def write_acceptance(
    run_dir: Path, verdict: str = "accepted", tree: str | None = None, target: Path | None = None
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    review = {"verdict": verdict}
    if tree is not None:
        review["tree"] = tree
    path = target or (review_dir(run_dir) / "acceptance.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"reviews": [review]}), encoding="utf-8")


def setup_repo(tmp_path: Path) -> tuple[Path, Path, Path]:
    repo = tmp_path / "repo"
    origin = tmp_path / "origin.git"
    run_git(tmp_path, "init", "--bare", "-b", "main", str(origin))
    run_git(tmp_path, "init", "-b", "main", str(repo))
    run_git(repo, "config", "user.email", "t@example.invalid")
    run_git(repo, "config", "user.name", "t")
    (repo / "config").mkdir()
    (repo / "tools/gov008").mkdir(parents=True)
    (repo / "scripts/agent").mkdir(parents=True)
    (repo / "config/gov008_ship.yaml").write_text(yaml.safe_dump(MINI_POLICY), encoding="utf-8")
    (repo / "scripts/agent/agent_config.json").write_text(
        json.dumps({"review_root": str(tmp_path / "reviews")}), encoding="utf-8"
    )
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
    repo: Path,
    run_dir: Path,
    *extra: str,
    env: dict[str, str] | None = None,
    branch: str = "task/x",
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
            branch,
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
    commit_file(repo, "src/app.py", "x\n", "code")
    write_acceptance(run_dir, tree=current_tree(repo))
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
    commit_file(repo, "src/app.py", "x\n", "code")
    main_ship = tmp_path / "main-ship.py"
    show_main_ship(repo, main_ship)
    run_git(repo, "add", "src/app.py")
    (repo / "tools/gov008/ship.py").write_text("raise SystemExit(99)\n", encoding="utf-8")
    run_git(repo, "add", "tools/gov008/ship.py")
    run_git(repo, "commit", "-m", "break branch ship")
    write_acceptance(run_dir, tree=current_tree(repo))
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
    before = run_git(repo, "rev-parse", "main")
    commit_file(repo, "src/app.py", "x\n", "code")
    write_acceptance(run_dir, tree=current_tree(repo))
    done = run_publish(repo, run_dir)
    assert done.returncode == 1
    assert json.loads((run_dir / "publish.json").read_text(encoding="utf-8"))["ok"] is False
    assert run_git(repo, "rev-parse", "main") == before
    assert run_git(repo, "branch", "--show-current") == "task/x"
    assert "task/x" in run_git(repo, "branch", "--list", "task/x")


def test_publish_fails_after_extra_commit_and_restores_branch(tmp_path: Path) -> None:
    repo, _origin, run_dir = setup_repo(tmp_path)
    before = run_git(repo, "rev-parse", "main")
    commit_file(repo, "src/app.py", "x\n", "code")
    assert run_ship(repo, "--base", "main", "--gate").returncode == 0
    gate = run_git(repo, "log", "-1", "--format=%(trailers:key=Gate,valueonly)")
    commit_file(repo, "src/next.py", "y\n", f"next\n\nGate: {gate}")
    write_acceptance(run_dir, tree=current_tree(repo))
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
    write_acceptance(run_dir, tree=current_tree(repo))
    env = os.environ.copy()
    env["AITS_PI_RUN"] = "leak"
    done = run_publish(repo, run_dir, env=env)
    assert done.returncode == 2
    assert not (run_dir / "publish.json").exists()


def test_publish_validation_rejects_missing_acceptance_tree_without_json(tmp_path: Path) -> None:
    repo, _origin, run_dir = setup_repo(tmp_path)
    write_acceptance(run_dir)
    before = run_git(repo, "rev-parse", "main")
    done = run_publish(repo, run_dir)
    assert done.returncode == 2
    assert "tree" in done.stdout
    assert not (run_dir / "publish.json").exists()
    assert run_git(repo, "rev-parse", "main") == before


def test_publish_validation_rejects_branch_tree_after_acceptance_without_json(
    tmp_path: Path,
) -> None:
    repo, _origin, run_dir = setup_repo(tmp_path)
    commit_file(repo, "src/app.py", "x\n", "code")
    write_acceptance(run_dir, tree=current_tree(repo))
    before = run_git(repo, "rev-parse", "main")
    commit_file(repo, "src/after.py", "y\n", "after acceptance")
    done = run_publish(repo, run_dir)
    assert done.returncode == 2
    assert "accepted review tree" in done.stdout
    assert not (run_dir / "publish.json").exists()
    assert run_git(repo, "rev-parse", "main") == before


def test_publish_validation_rejects_worktree_checkout_without_json(tmp_path: Path) -> None:
    repo, _origin, run_dir = setup_repo(tmp_path)
    write_acceptance(run_dir, tree=current_tree(repo))
    worktree = tmp_path / "worktree"
    run_git(repo, "worktree", "add", "-b", "task/worktree", str(worktree), "HEAD")
    done = run_publish(worktree, run_dir, branch="task/worktree")
    assert done.returncode == 2
    assert "main checkout" in done.stdout
    assert not (run_dir / "publish.json").exists()


def test_publish_validation_rejects_wrong_current_branch_without_json(tmp_path: Path) -> None:
    repo, _origin, run_dir = setup_repo(tmp_path)
    write_acceptance(run_dir, tree=current_tree(repo, "task/x"))
    before = run_git(repo, "rev-parse", "main")
    run_git(repo, "switch", "-c", "other")
    done = run_publish(repo, run_dir)
    assert done.returncode == 2
    assert "current branch" in done.stdout
    assert not (run_dir / "publish.json").exists()
    assert run_git(repo, "rev-parse", "main") == before


def test_publish_no_push_moves_main_but_not_origin(tmp_path: Path) -> None:
    repo, origin, run_dir = setup_repo(tmp_path)
    before_origin = run_git(origin, "rev-parse", "main")
    commit_file(repo, "src/app.py", "x\n", "code")
    write_acceptance(run_dir, tree=current_tree(repo))
    assert run_ship(repo, "--base", "main", "--gate").returncode == 0
    done = run_publish(repo, run_dir, "-NoPush")
    assert done.returncode == 0, done.stdout + done.stderr
    data = json.loads((run_dir / "publish.json").read_text(encoding="utf-8"))
    assert data["pushed"] is False
    assert run_git(repo, "rev-parse", "main") == data["main_sha"]
    assert run_git(origin, "rev-parse", "main") == before_origin


def test_publish_validation_rejects_uppercase_accepted_tree_without_json(tmp_path: Path) -> None:
    repo, _origin, run_dir = setup_repo(tmp_path)
    commit_file(repo, "src/app.py", "x\n", "code")
    write_acceptance(run_dir, tree=current_tree(repo).upper())
    before = run_git(repo, "rev-parse", "main")
    done = run_publish(repo, run_dir)
    assert done.returncode == 2
    assert (
        "last accepted review tree is missing or is not a 40-character lowercase hex tree"
        in done.stdout
    )
    assert not (run_dir / "publish.json").exists()
    assert run_git(repo, "rev-parse", "main") == before


def test_publish_ignores_acceptance_in_run_dir(tmp_path: Path) -> None:
    repo, _origin, run_dir = setup_repo(tmp_path)
    commit_file(repo, "src/app.py", "x\n", "code")
    assert run_ship(repo, "--base", "main", "--gate").returncode == 0
    write_acceptance(run_dir, tree=current_tree(repo), target=run_dir / "acceptance.json")
    before = run_git(repo, "rev-parse", "main")
    done = run_publish(repo, run_dir)
    assert done.returncode == 2
    expected = str(review_dir(run_dir) / "acceptance.json")
    assert "acceptance.json is missing: " + expected in done.stdout
    assert not (run_dir / "publish.json").exists()
    assert run_git(repo, "rev-parse", "main") == before


def test_publish_reads_review_root_from_base_not_task_branch(tmp_path: Path) -> None:
    repo, _origin, run_dir = setup_repo(tmp_path)
    elsewhere = tmp_path / "elsewhere"
    commit_file(
        repo,
        "scripts/agent/agent_config.json",
        json.dumps({"review_root": str(elsewhere)}),
        "move review root on the branch",
    )
    assert run_ship(repo, "--base", "main", "--gate").returncode == 0
    target = elsewhere / run_dir.name / "acceptance.json"
    write_acceptance(run_dir, tree=current_tree(repo), target=target)
    before = run_git(repo, "rev-parse", "main")
    done = run_publish(repo, run_dir)
    assert done.returncode == 2
    expected = str(review_dir(run_dir) / "acceptance.json")
    assert "acceptance.json is missing: " + expected in done.stdout
    assert run_git(repo, "rev-parse", "main") == before


def test_publish_rejects_acceptance_file_inside_run_dir(tmp_path: Path) -> None:
    repo, _origin, run_dir = setup_repo(tmp_path)
    inside = run_dir / "acceptance.json"
    write_acceptance(run_dir, tree=current_tree(repo), target=inside)
    done = run_publish(repo, run_dir, "-AcceptanceFile", str(inside))
    assert done.returncode == 2
    assert "must not be inside RunDir" in done.stdout
    assert not (run_dir / "publish.json").exists()


def test_publish_acceptance_file_override_publishes(tmp_path: Path) -> None:
    repo, origin, run_dir = setup_repo(tmp_path)
    commit_file(repo, "src/app.py", "x\n", "code")
    other = tmp_path / "other" / "acceptance.json"
    write_acceptance(run_dir, tree=current_tree(repo), target=other)
    assert run_ship(repo, "--base", "main", "--gate").returncode == 0
    done = run_publish(repo, run_dir, "-AcceptanceFile", str(other))
    assert done.returncode == 0, done.stdout + done.stderr
    assert run_git(repo, "rev-parse", "main") == run_git(origin, "rev-parse", "main")
