"""DEVX-031: a reviewed one-commit change moves onto a newer main only through ship.py --replay.

Owner decision owner_decision:DEVX-031:2026-10-14:parallel_pi_lanes. Throw-away git repositories;
the lanes publish one change at a time, and every change that waited is replayed onto the main the
previous one left, keeps its change fingerprint and message, and is gated again.
"""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest
import yaml

from tools.gov008 import ship

REPO = Path(__file__).resolve().parents[1]
REAL_POLICY = yaml.safe_load((REPO / ship.POLICY_PATH).read_text(encoding="utf-8"))
POLICY = {
    "schema_version": "gov008_ship_policy.v1",
    "tree_clean_exclusions": ["notes/unrelated.md"],
    "zones": {"C": ["secret/**", "config/*.yaml"], "A": ["docs/**", "research/**", "*.md"]},
    "research_records": REAL_POLICY["research_records"],
    "gates": {"A": [], "B": [], "C": [], "records": []},
}
LINES = "".join(f"line {n}\n" for n in range(1, 21))
# Stand-in for tools/tasks.py render: the view lists the task files in name order.
RENDER = """import pathlib
root = pathlib.Path(__file__).resolve().parents[1]
names = sorted(p.name for p in (root / "tasks").glob("*.yaml"))
(root / "docs").mkdir(exist_ok=True)
(root / "docs/task_register.md").write_text("".join(n + "\\n" for n in names), encoding="utf-8")
"""


def run_git(repo: Path, *args: str) -> str:
    done = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )
    return done.stdout.strip()


def write(repo: Path, rel: str, text: str) -> None:
    path = repo / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def commit(repo: Path, files: dict[str, str], message: str) -> str:
    for rel, text in files.items():
        write(repo, rel, text)
    run_git(repo, "add", "-A")
    run_git(repo, "commit", "-q", "-m", message)
    return run_git(repo, "rev-parse", "HEAD")


def render(repo: Path) -> str:
    subprocess.run([sys.executable, "tools/tasks.py", "render"], cwd=repo, check=True)
    return (repo / "docs/task_register.md").read_text(encoding="utf-8")


def build_repo(path: Path) -> None:
    run_git(path, "init", "-q", "-b", "main")
    run_git(path, "config", "user.email", "t@example.invalid")
    run_git(path, "config", "user.name", "t")
    run_git(path, "config", "core.autocrlf", "false")
    write(path, ship.POLICY_PATH, yaml.safe_dump(POLICY))
    write(path, "tools/tasks.py", RENDER)
    write(path, "src/app.py", LINES)
    write(path, "tasks/A0.yaml", "id: A0\n")
    render(path)
    run_git(path, "add", "-A")
    run_git(path, "commit", "-q", "-m", "init")


@pytest.fixture()
def repo(tmp_path: Path, git_repo_template: Callable[..., Path]) -> Path:
    return git_repo_template(tmp_path, __file__, build_repo)


def branch_change(repo: Path, files: dict[str, str], message: str = "code: lane change") -> str:
    run_git(repo, "switch", "-q", "-c", "pi/lane", "main")
    return commit(repo, files, message)


def main_moves(repo: Path, files: dict[str, str], message: str = "main moved") -> str:
    run_git(repo, "switch", "-q", "main")
    sha = commit(repo, files, message)
    run_git(repo, "switch", "-q", "pi/lane")
    return sha


def edited(line: int, text: str, base: str = LINES) -> str:
    lines = base.splitlines(keepends=True)
    lines[line - 1] = text + "\n"
    return "".join(lines)


def test_change_fingerprint_ignores_context_but_patch_fingerprint_does_not(repo: Path) -> None:
    branch_change(repo, {"src/app.py": edited(10, "lane ten")})
    patch = ship.patch_fingerprint(repo, "main", POLICY)
    change = ship.change_fingerprint(repo, "main", POLICY)
    main_moves(repo, {"src/app.py": edited(12, "main twelve")})
    ship.replay(repo, "main")
    assert ship.change_fingerprint(repo, "main", POLICY) == change
    assert ship.patch_fingerprint(repo, "main", POLICY) != patch


def test_change_fingerprint_sees_every_added_and_removed_line(repo: Path) -> None:
    branch_change(repo, {"src/app.py": edited(10, "lane ten")})
    change = ship.change_fingerprint(repo, "main", POLICY)
    write(repo, "src/app.py", edited(10, "lane ten!"))
    run_git(repo, "commit", "-q", "-a", "--amend", "-m", "code: lane change")
    assert ship.change_fingerprint(repo, "main", POLICY) != change


def test_replay_moves_the_commit_onto_main_and_keeps_change_and_message(repo: Path) -> None:
    old = branch_change(repo, {"src/app.py": edited(10, "lane ten")}, "code: lane\n\nbody")
    ship.gate(repo, "main")
    gated = run_git(repo, "rev-parse", "HEAD")
    change = ship.change_fingerprint(repo, "main", POLICY)
    message = ship.message_fingerprint(repo)
    moved = main_moves(repo, {"docs/other.md": "m\n"})
    done = ship.replay(repo, "main")
    new = run_git(repo, "rev-parse", "HEAD")
    assert done["replayed_from"] == gated and done["replayed_sha"] == new != old
    assert run_git(repo, "rev-parse", "HEAD^") == moved
    assert run_git(repo, "branch", "--show-current") == "pi/lane"
    assert done["change"] == change and done["message"] == message
    assert ship.message_fingerprint(repo) == message
    assert run_git(repo, "log", "-1", "--format=%(trailers:key=Gate,valueonly)") == ""
    assert run_git(repo, "log", "-1", "--format=%an %ae", new) == "t t@example.invalid"
    assert run_git(repo, "status", "--porcelain") == ""
    assert (repo / "docs/other.md").read_text(encoding="utf-8") == "m\n"
    assert (repo / "src/app.py").read_text(encoding="utf-8") == edited(10, "lane ten")


def test_replayed_change_publishes_by_change_after_a_new_gate(repo: Path) -> None:
    branch_change(repo, {"src/app.py": edited(10, "lane ten")})
    change = ship.change_fingerprint(repo, "main", POLICY)
    patch = ship.patch_fingerprint(repo, "main", POLICY)
    message = ship.message_fingerprint(repo)
    main_moves(repo, {"src/app.py": edited(12, "main twelve")})
    ship.replay(repo, "main")
    with pytest.raises(ship.ShipError, match="missing Gate trailer"):
        ship.publish(repo, "main", push=False, expected_change=change, expected_message=message)
    ship.gate(repo, "main")
    with pytest.raises(ship.ShipError, match="reviewed patch"):
        ship.publish(repo, "main", push=False, expected_patch=patch, expected_message=message)
    done = ship.publish(repo, "main", push=False, expected_change=change, expected_message=message)
    assert done["reviewed_patch"] == {"change": change, "message": message}
    assert run_git(repo, "rev-parse", "main") == done["shipped_sha"]
    assert (repo / "src/app.py").read_text(encoding="utf-8") == edited(
        12, "main twelve", edited(10, "lane ten")
    )


def test_replay_refuses_a_conflict_and_leaves_the_branch(repo: Path) -> None:
    old = branch_change(repo, {"src/app.py": edited(10, "lane ten")})
    main_moves(repo, {"src/app.py": edited(10, "main ten")})
    with pytest.raises(ship.ShipError, match=r"conflicts with main in \['src/app.py'\]"):
        ship.replay(repo, "main")
    assert run_git(repo, "rev-parse", "HEAD") == old
    assert run_git(repo, "status", "--porcelain") == ""


def test_replay_renders_task_views_again_instead_of_conflicting(repo: Path) -> None:
    write(repo, "tasks/B1.yaml", "id: B1\n")
    run_git(repo, "switch", "-q", "-c", "pi/lane", "main")
    write(repo, "tasks/B1.yaml", "id: B1\n")
    render(repo)
    commit(repo, {"src/app.py": edited(3, "lane three")}, "code: lane with task B1")
    change = ship.change_fingerprint(repo, "main", POLICY)
    run_git(repo, "switch", "-q", "main")
    write(repo, "tasks/A2.yaml", "id: A2\n")
    render(repo)
    commit(repo, {}, "main: task A2")
    run_git(repo, "switch", "-q", "pi/lane")
    done = ship.replay(repo, "main")
    assert done["rendered_views"] == ["docs/task_register.md"]
    assert done["change"] == change
    assert run_git(repo, "show", "HEAD:docs/task_register.md") == "A0.yaml\nA2.yaml\nB1.yaml"
    assert run_git(repo, "status", "--porcelain") == ""
    assert run_git(repo, "rev-parse", "HEAD^{tree}") == done["tree"]
    assert run_git(repo, "rev-list", "--count", "main..HEAD") == "1"


@pytest.mark.parametrize("case", ["on_main", "detached", "dirty", "nothing", "two_commits"])
def test_replay_refusals(repo: Path, case: str) -> None:
    branch_change(repo, {"src/app.py": edited(10, "lane ten")})
    problem = {
        "on_main": "task branch",
        "detached": "task branch",
        "dirty": "not clean",
        "nothing": "already an ancestor",
        "two_commits": "exactly one commit",
    }[case]
    if case == "on_main":
        run_git(repo, "switch", "-q", "main")
    elif case == "detached":
        run_git(repo, "switch", "-q", "--detach")
    elif case == "dirty":
        main_moves(repo, {"docs/other.md": "m\n"})
        write(repo, "src/app.py", "dirty\n")
    elif case == "two_commits":
        commit(repo, {"src/more.py": "x\n"}, "second")
        main_moves(repo, {"docs/other.md": "m\n"})
    before = run_git(repo, "rev-parse", "HEAD")
    with pytest.raises(ship.ShipError, match=problem):
        ship.replay(repo, "main")
    assert run_git(repo, "rev-parse", "HEAD") == before


def test_replay_leaves_research_records_to_replay_records(repo: Path) -> None:
    branch_change(repo, {"research/Q1/runs/0001.json": "{}\n"}, "records")
    main_moves(repo, {"docs/other.md": "m\n"})
    with pytest.raises(ship.ShipError, match="--replay-records"):
        ship.replay(repo, "main")


def test_plan_prints_fingerprints_for_one_commit_only(repo: Path) -> None:
    branch_change(repo, {"src/app.py": edited(10, "lane ten")})
    fingerprints = ship.plan(repo)["fingerprints"]
    assert fingerprints == {
        "patch": ship.patch_fingerprint(repo, "main", POLICY),
        "change": ship.change_fingerprint(repo, "main", POLICY),
        "message": ship.message_fingerprint(repo),
    }
    commit(repo, {"src/more.py": "x\n"}, "second")
    assert "fingerprints" not in ship.plan(repo)


def test_publish_by_change_refuses_other_change_and_both_names(repo: Path) -> None:
    branch_change(repo, {"src/app.py": edited(10, "lane ten")})
    change = ship.change_fingerprint(repo, "main", POLICY)
    patch = ship.patch_fingerprint(repo, "main", POLICY)
    message = ship.message_fingerprint(repo)
    write(repo, "src/app.py", edited(10, "lane ten!"))
    run_git(repo, "commit", "-q", "-a", "--amend", "-m", "code: lane change")
    ship.gate(repo, "main")
    before = run_git(repo, "rev-parse", "main")
    with pytest.raises(ship.ShipError, match="reviewed change"):
        ship.publish(repo, "main", push=False, expected_change=change, expected_message=message)
    with pytest.raises(ship.ShipError, match="exactly one"):
        ship.publish(
            repo,
            "main",
            push=False,
            expected_patch=patch,
            expected_change=change,
            expected_message=message,
        )
    with pytest.raises(ship.ShipError, match="40-character"):
        ship.publish(repo, "main", push=False, expected_change="A" * 40, expected_message=message)
    assert run_git(repo, "rev-parse", "main") == before


def test_publish_by_change_refuses_the_same_change_in_two_commits(repo: Path) -> None:
    branch_change(repo, {"src/app.py": edited(10, "lane ten")})
    change = ship.change_fingerprint(repo, "main", POLICY)
    message = ship.message_fingerprint(repo)
    run_git(repo, "reset", "-q", "--hard", "main")
    commit(repo, {"src/app.py": edited(10, "lane")}, "code: first half")
    commit(repo, {"src/app.py": edited(10, "lane ten")}, "code: lane change")
    assert ship.change_fingerprint(repo, "main", POLICY) == change
    assert ship.message_fingerprint(repo) == message
    ship.gate(repo, "main")
    before = run_git(repo, "rev-parse", "main")
    with pytest.raises(ship.ShipError, match="exactly one commit on main, found 2"):
        ship.publish(repo, "main", push=False, expected_change=change, expected_message=message)
    assert run_git(repo, "rev-parse", "main") == before


def cli(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(REPO / "tools/gov008/ship.py"), "--repo", str(repo), *args],
        text=True,
        capture_output=True,
        check=False,
    )


def test_cli_replay_then_gate_then_publish_by_change(repo: Path) -> None:
    branch_change(repo, {"src/app.py": edited(10, "lane ten")})
    dry = json.loads(cli(repo, "--dry-run").stdout.strip().splitlines()[-1])
    main_moves(repo, {"src/app.py": edited(12, "main twelve")})
    replayed = cli(repo, "--replay")
    assert replayed.returncode == 0, replayed.stderr
    assert (
        json.loads(replayed.stdout.strip().splitlines()[-1])["change"]
        == dry["fingerprints"]["change"]
    )
    assert cli(repo, "--gate").returncode == 0
    done = cli(
        repo,
        "--publish",
        "--expected-change",
        dry["fingerprints"]["change"],
        "--expected-message",
        dry["fingerprints"]["message"],
    )
    assert done.returncode == 0, done.stderr
    assert run_git(repo, "rev-parse", "main") == run_git(repo, "rev-parse", "HEAD")


@pytest.mark.parametrize(
    ("args", "problem"),
    [
        (["--dry-run", "--expected-change", "a" * 40], "need --publish"),
        (["--publish", "--expected-change", "a" * 40], "go together"),
        (
            ["--publish", "--expected-patch", "a" * 40, "--expected-change", "a" * 40],
            "not both",
        ),
    ],
)
def test_cli_expected_change_option_rules(repo: Path, args: list[str], problem: str) -> None:
    done = cli(repo, *args)
    assert done.returncode == 2
    assert problem in done.stderr
