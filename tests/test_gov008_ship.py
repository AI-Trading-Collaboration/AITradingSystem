"""GOV-008 P2 spike: unit tests for tools/gov008/ship.py using throw-away git repositories."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

from tools.gov008 import ship

REPO = Path(__file__).resolve().parents[1]
# Zone C globs intentionally allowed to match nothing yet (created later in GOV-008).
PLANNED_ZONE_C = {"tests/invariants/**"}

MINI_POLICY = {
    "schema_version": "gov008_ship_policy.v1",
    "tree_clean_exclusions": ["notes/unrelated.md"],
    "zones": {"C": ["secret/**", "config/*.yaml"], "A": ["docs/**", "*.md"]},
    "gates": {"A": [], "B": [], "C": []},
}


def run_git(repo: Path, *args: str) -> str:
    done = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )
    return done.stdout.strip()


@pytest.fixture()
def repo(tmp_path: Path) -> Path:
    run_git(tmp_path, "init", "-b", "main")
    run_git(tmp_path, "config", "user.email", "t@example.invalid")
    run_git(tmp_path, "config", "user.name", "t")
    (tmp_path / "config").mkdir()
    (tmp_path / "config/gov008_ship.yaml").write_text(yaml.safe_dump(MINI_POLICY), encoding="utf-8")
    (tmp_path / "README.md").write_text("start\n", encoding="utf-8")
    run_git(tmp_path, "add", "-A")
    run_git(tmp_path, "commit", "-m", "init")
    run_git(tmp_path, "switch", "-c", "task/x")
    return tmp_path


def commit_file(repo: Path, rel: str, text: str, message: str) -> None:
    path = repo / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    run_git(repo, "add", rel)
    run_git(repo, "commit", "-m", message)


def test_glob_semantics() -> None:
    rx = ship.glob_to_regex("src/x/**")
    assert rx.match("src/x/a/b/c.py") and not rx.match("src/y/a.py")
    assert ship.glob_to_regex("*.md").match("AGENTS.md")
    assert not ship.glob_to_regex("*.md").match("docs/a.md")
    assert ship.glob_to_regex("config/ibkr_paper_*.yaml").match("config/ibkr_paper_order.yaml")
    assert ship.glob_to_regex("a/**/b.py").match("a/b.py") and ship.glob_to_regex(
        "a/**/b.py"
    ).match("a/x/y/b.py")


def test_real_policy_zone_c_globs_match_tracked_files() -> None:
    policy = yaml.safe_load((REPO / ship.POLICY_PATH).read_text(encoding="utf-8"))
    tracked = ship.git(REPO, "ls-files", "--cached", "--others", "--exclude-standard").splitlines()
    stale = [
        g
        for g in policy["zones"]["C"]
        if g not in PLANNED_ZONE_C and not any(ship.glob_to_regex(g).match(p) for p in tracked)
    ]
    assert stale == [], f"zone C globs that match no tracked file: {stale}"


@pytest.mark.parametrize(
    ("path", "zone"),
    [
        ("AGENTS.md", "C"),
        ("src/ai_trading_system/scoring/daily.py", "C"),
        ("src/ai_trading_system/trading_engine/brokers/ibkr.py", "C"),
        ("config/scoring_rules.yaml", "C"),
        ("docs/requirements/x.md", "A"),
        ("registry/development_tasks/aa/x.yaml", "A"),
        ("src/ai_trading_system/reports/reader_brief.py", "B"),
        ("tests/test_x.py", "B"),
    ],
)
def test_real_policy_classification(path: str, zone: str) -> None:
    policy = yaml.safe_load((REPO / ship.POLICY_PATH).read_text(encoding="utf-8"))
    assert [z for z, v in ship.classify([path], policy).items() if v] == [zone]


def test_plan_ok_for_zone_a_change(repo: Path) -> None:
    commit_file(repo, "docs/a.md", "x\n", "docs")
    p = ship.plan(repo)
    assert p["ok"] and p["zone"] == "A" and p["gate"] == []


def test_zone_c_requires_owner_decision_trailer(repo: Path) -> None:
    commit_file(repo, "secret/rule.txt", "x\n", "change a guarded file")
    p = ship.plan(repo)
    assert not p["ok"] and "Owner-Decision" in p["problems"][0]
    commit_file(
        repo, "secret/rule2.txt", "y\n", "again\n\nOwner-Decision: owner_decision:T:2026-10-10:x"
    )
    p = ship.plan(repo)
    assert (
        p["ok"] and p["zone"] == "C" and p["owner_decisions"] == ["owner_decision:T:2026-10-10:x"]
    )


def test_dirty_tree_blocks_but_registered_exclusion_does_not(repo: Path) -> None:
    commit_file(repo, "docs/a.md", "x\n", "docs")
    (repo / "notes").mkdir()
    (repo / "notes/unrelated.md").write_text("owner scratch\n", encoding="utf-8")
    assert ship.plan(repo)["ok"]
    (repo / "docs/b.md").write_text("dirty\n", encoding="utf-8")
    p = ship.plan(repo)
    assert not p["ok"] and "not clean" in p["problems"][0]


def test_refuses_when_main_is_not_an_ancestor(repo: Path) -> None:
    commit_file(repo, "docs/a.md", "x\n", "docs")
    run_git(repo, "switch", "main")
    commit_file(repo, "docs/other.md", "m\n", "main moved")
    run_git(repo, "switch", "task/x")
    p = ship.plan(repo)
    assert not p["ok"] and "not an ancestor" in p["problems"][0]


def test_refuses_to_run_on_main(repo: Path) -> None:
    run_git(repo, "switch", "main")
    assert not ship.plan(repo)["ok"]


def test_ship_moves_main_to_same_tree_with_gate_trailer(repo: Path) -> None:
    commit_file(repo, "docs/a.md", "x\n", "docs: add a")
    tree = run_git(repo, "rev-parse", "HEAD^{tree}")
    done = ship.ship(repo, "main", push=False)
    assert (
        run_git(repo, "rev-parse", "main")
        == done["shipped_sha"]
        == run_git(repo, "rev-parse", "HEAD")
    )
    assert run_git(repo, "rev-parse", "main^{tree}") == tree
    trailers = run_git(repo, "log", "-1", "--format=%(trailers:key=Gate,valueonly)", "main")
    assert "PASS" in trailers and f"tree={tree[:12]}" in trailers


def test_ship_compare_and_swap_fails_if_main_moves_during_the_gate(
    repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    commit_file(repo, "docs/a.md", "x\n", "docs: add a")
    real_run_gate = ship.run_gate

    def gate_then_main_moves(repo_: Path, commands: list[list[str]]) -> dict:
        result = real_run_gate(repo_, commands)
        run_git(
            repo_,
            "update-ref",
            "refs/heads/main",
            run_git(repo_, "commit-tree", "-p", "main", "-m", "race", "main^{tree}"),
        )
        return result

    monkeypatch.setattr(ship, "run_gate", gate_then_main_moves)
    with pytest.raises(ship.ShipError):
        ship.ship(repo, "main", push=False)


def _with_origin(repo: Path, tmp_path_factory: pytest.TempPathFactory) -> Path:
    origin = tmp_path_factory.mktemp("origin") / "origin.git"
    run_git(origin.parent, "init", "--bare", "-b", "main", str(origin))
    run_git(repo, "remote", "add", "origin", str(origin))
    run_git(repo, "push", "origin", "main")
    return origin


def test_ship_push_fast_forwards_origin_and_verifies_it(
    repo: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    origin = _with_origin(repo, tmp_path_factory)
    commit_file(repo, "docs/a.md", "x\n", "docs: add a")
    done = ship.ship(repo, "main", push=True)
    assert done["pushed"] is True
    assert (
        run_git(origin, "rev-parse", "main")
        == done["shipped_sha"]
        == run_git(repo, "rev-parse", "main")
    )


NOISY_SUCCESS = (
    "print(*[f'progress dot line {i}' for i in range(300)], sep='\\n'); "
    "print('7930 passed, 614 warnings in 600.00s')"
)
NOISY_FAILURE = (
    "import sys; "
    "print(*[f'chatty output line {i}' for i in range(120)], sep='\\n'); "
    "print('FAILED tests/test_x.py::test_broken - assert False'); sys.exit(1)"
)


def test_run_gate_logs_full_output_and_prints_only_one_summary_line(
    repo: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    result = ship.run_gate(repo, [["python", "-c", NOISY_SUCCESS]])
    captured = capsys.readouterr()
    # the terminal gets exactly one line per command: command, exit code, seconds, last output line
    assert captured.out.count("\n") == 1
    assert "exit=0" in captured.out and "7930 passed, 614 warnings in 600.00s" in captured.out
    assert "progress dot line 299" not in captured.out
    assert captured.err == ""
    # the full output goes to a log file under the git directory, never the working tree
    log_path = Path(result["log"])
    git_dir = Path(ship.git_dir(repo))
    assert log_path.parent == git_dir / ship.GATE_LOG_DIRNAME
    log_text = log_path.read_text(encoding="utf-8")
    assert "progress dot line 299" in log_text
    assert "7930 passed, 614 warnings in 600.00s" in log_text
    assert "exit=0" in log_text
    assert ship.dirty_paths(repo, []) == []


def test_run_gate_failure_prints_tail_and_error_names_the_log(
    repo: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(ship.ShipError) as excinfo:
        ship.run_gate(repo, [["python", "-c", NOISY_FAILURE]])
    message = str(excinfo.value)
    assert "exit 1" in message
    log_path = message.rsplit("full gate output: ", 1)[1].strip()
    assert Path(log_path).is_file()
    captured = capsys.readouterr()
    # the failure tail on stderr shows the short-test-summary-style line but is truncated
    assert "FAILED tests/test_x.py::test_broken" in captured.err
    tail = captured.err.splitlines()
    assert "chatty output line 119" in captured.err
    assert "chatty output line 0" not in captured.err
    printed_body = [ln for ln in tail if ln.startswith("chatty output line")]
    assert len(printed_body) == ship.GATE_FAILURE_TAIL_LINES - 1  # one line is the FAILED line
    # the log keeps everything, including the lines the terminal never showed
    log_text = Path(log_path).read_text(encoding="utf-8")
    assert "chatty output line 0" in log_text and "chatty output line 119" in log_text
    assert ship.dirty_paths(repo, []) == []


def test_run_gate_log_lives_in_git_dir_so_working_tree_stays_clean(repo: Path) -> None:
    result = ship.run_gate(repo, [["python", "-c", NOISY_SUCCESS]])
    assert Path(result["log"]).is_relative_to(Path(ship.git_dir(repo)))
    assert run_git(repo, "status", "--porcelain", "--untracked-files=all") == ""


def test_ship_push_refuses_a_diverged_origin_without_moving_local_main(
    repo: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    origin = _with_origin(repo, tmp_path_factory)
    other = tmp_path_factory.mktemp("other") / "clone"
    run_git(other.parent, "clone", str(origin), str(other))
    run_git(other, "config", "user.email", "o@example.invalid")
    run_git(other, "config", "user.name", "o")
    commit_file(other, "docs/elsewhere.md", "y\n", "someone else")
    run_git(other, "push", "origin", "main")
    before = run_git(repo, "rev-parse", "main")
    commit_file(repo, "docs/a.md", "x\n", "docs: add a")
    with pytest.raises(ship.ShipError, match="not an ancestor"):
        ship.ship(repo, "main", push=True)
    assert run_git(repo, "rev-parse", "main") == before
