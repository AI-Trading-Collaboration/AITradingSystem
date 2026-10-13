"""GOV-008 P2 spike: unit tests for tools/gov008/ship.py using throw-away git repositories."""

from __future__ import annotations

import datetime as dt
import json
import subprocess
import sys
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
        ("src/ai_trading_system/research_bench/simulate.py", "C"),
        ("config/research/research_bench.yaml", "C"),
        ("research/QQQ-Q1/prereg.yaml", "C"),
        ("research/QQQ-Q1/strategy.py", "A"),
        ("research/QQQ-Q1/runs/0001.json", "A"),
        (".pi/extensions/aits-guard/index.ts", "C"),
        ("scripts/agent/publish.ps1", "C"),
        ("scripts/agent/approve.ps1", "C"),
        ("scripts/agent/run-brief.ps1", "B"),
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


def _gate_values(repo: Path) -> list[str]:
    out = run_git(repo, "log", "-1", "--format=%(trailers:key=Gate,valueonly)")
    return [line.strip() for line in out.splitlines() if line.strip()]


def _write_policy(repo: Path, policy: dict) -> None:
    (repo / "config/gov008_ship.yaml").write_text(yaml.safe_dump(policy), encoding="utf-8")


def test_gate_records_trailer_without_moving_main_or_origin(
    repo: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    origin = _with_origin(repo, tmp_path_factory)
    before_main = run_git(repo, "rev-parse", "main")
    before_origin = run_git(origin, "rev-parse", "main")
    commit_file(repo, "docs/a.md", "x\n", "docs: add a")
    tree = run_git(repo, "rev-parse", "HEAD^{tree}")
    done = ship.gate(repo, "main")
    assert done["tree"] == tree
    assert run_git(repo, "rev-parse", "HEAD^{tree}") == tree
    assert run_git(repo, "rev-parse", "main") == before_main
    assert run_git(origin, "rev-parse", "main") == before_origin
    assert f"tree={tree[:12]}" in _gate_values(repo)[-1]


def test_gate_refuses_plan_problems(repo: Path) -> None:
    commit_file(repo, "secret/rule.txt", "x\n", "change guarded file")
    with pytest.raises(ship.ShipError, match="Owner-Decision"):
        ship.gate(repo, "main")


def test_gate_then_publish_fast_forwards_main(repo: Path) -> None:
    commit_file(repo, "docs/a.md", "x\n", "docs: add a")
    gated = ship.gate(repo, "main")
    done = ship.publish(repo, "main", push=False)
    assert done["shipped_sha"] == gated["gated_sha"] == run_git(repo, "rev-parse", "main")
    assert done["gate_trailer"] == _gate_values(repo)[-1]


def test_publish_does_not_run_gate_commands_but_gate_and_execute_do(repo: Path) -> None:
    failing = [["python", "-c", "raise SystemExit(1)"]]
    policy = MINI_POLICY | {"gates": {"A": [], "B": failing, "C": failing}}
    run_git(repo, "switch", "main")
    _write_policy(repo, policy)
    run_git(repo, "add", "config/gov008_ship.yaml")
    run_git(repo, "commit", "-m", "policy\n\nOwner-Decision: owner_decision:T:2026-10-10:x")
    before_publish = run_git(repo, "rev-parse", "main")
    run_git(repo, "switch", "task/x")
    run_git(repo, "merge", "--ff-only", "main")
    commit_file(repo, "src/app.py", "x\n", "code")
    tree = run_git(repo, "rev-parse", "HEAD^{tree}")
    ship.append_gate_trailer(repo, f"zone=B tree={tree[:12]} commands=1 seconds=0 PASS")
    head = run_git(repo, "rev-parse", "HEAD")
    assert ship.publish(repo, "main", push=False)["shipped_sha"] == head
    assert run_git(repo, "rev-parse", "main") == head

    run_git(repo, "update-ref", "refs/heads/main", before_publish)
    with pytest.raises(ship.ShipError, match="exit 1"):
        ship.gate(repo, "main")
    with pytest.raises(ship.ShipError, match="exit 1"):
        ship.ship(repo, "main", push=False)


def test_publish_refuses_missing_gate_trailer(repo: Path) -> None:
    before = run_git(repo, "rev-parse", "main")
    commit_file(repo, "docs/a.md", "x\n", "docs: add a")
    with pytest.raises(ship.ShipError, match="missing Gate"):
        ship.publish(repo, "main", push=False)
    assert run_git(repo, "rev-parse", "main") == before


def test_publish_refuses_tree_mismatch_after_extra_commit(repo: Path) -> None:
    before = run_git(repo, "rev-parse", "main")
    commit_file(repo, "docs/a.md", "x\n", "docs: add a")
    ship.gate(repo, "main")
    old_gate = _gate_values(repo)[-1]
    commit_file(repo, "docs/b.md", "y\n", f"docs: add b\n\nGate: {old_gate}")
    with pytest.raises(ship.ShipError, match="does not match HEAD tree"):
        ship.publish(repo, "main", push=False)
    assert run_git(repo, "rev-parse", "main") == before


@pytest.mark.parametrize(
    "bad",
    [
        "nonsense",
        "zone=A tree=123 commands=0 seconds=0",
        "zone=A tree=123456789abc commands=0 seconds=0 FAIL",
    ],
)
def test_publish_refuses_bad_trailer_format(repo: Path, bad: str) -> None:
    before = run_git(repo, "rev-parse", "main")
    commit_file(repo, "docs/a.md", "x\n", f"docs: add a\n\nGate: {bad}")
    with pytest.raises(ship.ShipError, match="format is invalid"):
        ship.publish(repo, "main", push=False)
    assert run_git(repo, "rev-parse", "main") == before


def test_publish_refuses_zone_lower_than_actual(repo: Path) -> None:
    before = run_git(repo, "rev-parse", "main")
    commit_file(repo, "src/app.py", "x\n", "code")
    tree = run_git(repo, "rev-parse", "HEAD^{tree}")
    ship.append_gate_trailer(repo, f"zone=A tree={tree[:12]} commands=0 seconds=0 PASS")
    with pytest.raises(ship.ShipError, match="does not match planned zone"):
        ship.publish(repo, "main", push=False)
    assert run_git(repo, "rev-parse", "main") == before


def test_publish_refuses_command_count_mismatch(repo: Path) -> None:
    before = run_git(repo, "rev-parse", "main")
    commit_file(repo, "docs/a.md", "x\n", "docs: add a")
    tree = run_git(repo, "rev-parse", "HEAD^{tree}")
    ship.append_gate_trailer(repo, f"zone=A tree={tree[:12]} commands=1 seconds=0 PASS")
    with pytest.raises(ship.ShipError, match="does not match policy commands"):
        ship.publish(repo, "main", push=False)
    assert run_git(repo, "rev-parse", "main") == before


def test_publish_refuses_when_main_not_ancestor(repo: Path) -> None:
    commit_file(repo, "docs/a.md", "x\n", "docs: add a")
    ship.gate(repo, "main")
    run_git(repo, "switch", "main")
    commit_file(repo, "docs/other.md", "m\n", "main moved")
    run_git(repo, "switch", "task/x")
    before = run_git(repo, "rev-parse", "main")
    with pytest.raises(ship.ShipError, match="not an ancestor"):
        ship.publish(repo, "main", push=False)
    assert run_git(repo, "rev-parse", "main") == before


def test_publish_reads_base_policy_while_dry_run_reads_worktree_policy(repo: Path) -> None:
    before = run_git(repo, "rev-parse", "main")
    policy = MINI_POLICY | {"zones": {"C": [], "A": ["docs/**", "*.md", "secret/**", "config/**"]}}
    _write_policy(repo, policy)
    run_git(repo, "add", "config/gov008_ship.yaml")
    run_git(repo, "commit", "-m", "loosen policy")
    commit_file(repo, "secret/rule.txt", "x\n", "secret")
    p = ship.plan(repo)
    assert p["ok"] and p["zone"] == "A"
    with pytest.raises(ship.ShipError, match="Owner-Decision"):
        ship.publish(repo, "main", push=False)
    assert run_git(repo, "rev-parse", "main") == before


def test_publish_push_fast_forwards_origin_and_verifies(
    repo: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    origin = _with_origin(repo, tmp_path_factory)
    commit_file(repo, "docs/a.md", "x\n", "docs: add a")
    ship.gate(repo, "main")
    done = ship.publish(repo, "main", push=True)
    assert done["pushed"] is True
    assert (
        run_git(origin, "rev-parse", "main")
        == done["shipped_sha"]
        == run_git(repo, "rev-parse", "main")
    )


def test_publish_push_refuses_diverged_origin_without_moving_local_main(
    repo: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    origin = _with_origin(repo, tmp_path_factory)
    other = tmp_path_factory.mktemp("other-publish") / "clone"
    run_git(other.parent, "clone", str(origin), str(other))
    run_git(other, "config", "user.email", "o@example.invalid")
    run_git(other, "config", "user.name", "o")
    commit_file(other, "docs/elsewhere.md", "y\n", "someone else")
    run_git(other, "push", "origin", "main")
    before = run_git(repo, "rev-parse", "main")
    commit_file(repo, "docs/a.md", "x\n", "docs: add a")
    ship.gate(repo, "main")
    with pytest.raises(ship.ShipError, match="not an ancestor"):
        ship.publish(repo, "main", push=True)
    assert run_git(repo, "rev-parse", "main") == before


@pytest.mark.parametrize("args", [["--dry-run", "--push"], ["--gate", "--push"]])
def test_cli_rejects_push_with_dry_run_or_gate(repo: Path, args: list[str]) -> None:
    done = subprocess.run(
        [sys.executable, str(REPO / "tools/gov008/ship.py"), "--repo", str(repo), *args],
        text=True,
        capture_output=True,
        check=False,
    )
    assert done.returncode == 2
    assert "--push can only be used" in done.stderr


# --- DEVX-029: owner decision records and the reviewed patch --------------------------------------

DECISION = "owner_decision:T:2026-10-13:x"
DECISION_POLICY = MINI_POLICY | {"decision_record_excluded": ["secret/rules/**"]}


def _today() -> dt.date:
    return dt.datetime.now(dt.UTC).date()


def _decision_record(**overrides: object) -> dict:
    approved = _today()
    record = {
        "schema": ship.DECISION_SCHEMA,
        "decision": DECISION,
        "approved_at": f"{approved.isoformat()}T00:10:00Z",
        "owner_words": "批准决定 T，按草稿写进验收目录",
        "c_paths": ["secret/app/**"],
        "expected_result": "only secret/app changes",
        "expires": (approved + dt.timedelta(days=7)).isoformat(),
    }
    record.update(overrides)
    return record


def _outside(repo: Path, name: str) -> Path:
    """A directory next to the throw-away repository (the repo fixture is tmp_path itself)."""
    return repo.parent / f"{repo.name}-{name}"


def _write_record(root: Path, record: dict, decision: str = DECISION) -> Path:
    path = root / ship.DECISION_RECORD_DIR / ship.decision_record_name(decision)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")
    return path


def _decision_repo(repo: Path, policy: dict | None = None) -> None:
    """Commit a base policy on main (with decision_record_excluded) and bring task/x up to it."""
    run_git(repo, "switch", "main")
    _write_policy(repo, DECISION_POLICY if policy is None else policy)
    run_git(repo, "add", "config/gov008_ship.yaml")
    run_git(repo, "commit", "--allow-empty", "-m", f"policy\n\nOwner-Decision: {DECISION}")
    run_git(repo, "switch", "task/x")
    run_git(repo, "merge", "--ff-only", "main")


def _gated_zone_c_change(repo: Path, rel: str = "secret/app/rule.txt") -> None:
    commit_file(repo, rel, "x\n", f"guarded change\n\nOwner-Decision: {DECISION}")
    ship.gate(repo, "main")


def test_real_policy_decision_record_excluded_is_inside_zone_c() -> None:
    policy = yaml.safe_load((REPO / ship.POLICY_PATH).read_text(encoding="utf-8"))
    excluded = policy["decision_record_excluded"]
    assert [g for g in excluded if g not in policy["zones"]["C"]] == []
    for rules_or_gate in (
        "AGENTS.md",
        "config/gov008_ship.yaml",
        "tools/gov008/ship.py",
        ".pi/extensions/aits-guard/**",
        "scripts/agent/publish.ps1",
        "scripts/agent/approve.ps1",
        ".github/**",
        "src/ai_trading_system/core/production_effect.py",
        "src/ai_trading_system/trading_engine/brokers/**",
    ):
        assert rules_or_gate in excluded


def test_decision_record_name_and_malformed_id() -> None:
    assert ship.decision_record_name(DECISION) == "T.2026-10-13.x.json"
    with pytest.raises(ship.ShipError, match="malformed"):
        ship.decision_record_name("owner_decision:T:missing")


@pytest.mark.parametrize(
    ("overrides", "problem"),
    [
        ({"schema": "other"}, "schema must be"),
        ({"decision": "owner_decision:T"}, "decision must be"),
        ({"owner_words": " "}, "owner_words must be"),
        ({"expected_result": None}, "expected_result must be"),
        ({"c_paths": []}, "c_paths must be"),
        ({"c_paths": ["/abs/**"]}, "c_paths must be"),
        ({"approved_at": "2026-10-13 00:10"}, "approved_at must be"),
        ({"expires": "soon"}, "expires must be"),
    ],
)
def test_decision_record_validation_names_the_problem(overrides: dict, problem: str) -> None:
    with pytest.raises(ship.ShipError, match=problem):
        ship.validate_decision_record(_decision_record(**overrides), _today())


def test_decision_record_validity_window() -> None:
    today = _today()
    ship.validate_decision_record(_decision_record(), today)
    with pytest.raises(ship.ShipError, match="more than 14 days"):
        ship.validate_decision_record(
            _decision_record(expires=(today + dt.timedelta(days=15)).isoformat()), today
        )
    with pytest.raises(ship.ShipError, match="expired on"):
        ship.validate_decision_record(_decision_record(), today + dt.timedelta(days=8))
    with pytest.raises(ship.ShipError, match="before approved_at"):
        ship.validate_decision_record(
            _decision_record(expires=(today - dt.timedelta(days=1)).isoformat()),
            today - dt.timedelta(days=2),
        )


def test_publish_zone_c_with_decision_record_covering_its_paths(repo: Path, tmp_path: Path) -> None:
    _decision_repo(repo)
    _gated_zone_c_change(repo)
    root = _outside(tmp_path, "reviews")
    _write_record(root, _decision_record())
    done = ship.publish(repo, "main", push=False, review_root=root)
    assert done["decision_records"] == {"decisions": [DECISION], "zone_c_paths": 1}
    assert run_git(repo, "rev-parse", "main") == done["shipped_sha"]


@pytest.mark.parametrize(
    ("record", "rel", "problem"),
    [
        (None, "secret/app/rule.txt", "no decision record"),
        ({"c_paths": ["secret/other/**"]}, "secret/app/rule.txt", "outside the decision records"),
        ({"c_paths": ["secret/**"]}, "secret/rules/gate.txt", "cannot approve"),
        ({"decision": "owner_decision:T:2026-10-13:other"}, "secret/app/rule.txt", "is for"),
        (
            {"expires": "2026-01-01", "approved_at": "2025-12-30T00:00:00Z"},
            "secret/app/rule.txt",
            "expired",
        ),
    ],
)
def test_publish_zone_c_decision_record_refusals_keep_main(
    repo: Path, tmp_path: Path, record: dict | None, rel: str, problem: str
) -> None:
    _decision_repo(repo)
    before = run_git(repo, "rev-parse", "main")
    _gated_zone_c_change(repo, rel)
    root = _outside(tmp_path, "reviews")
    if record is not None:
        _write_record(root, _decision_record(**record))
    with pytest.raises(ship.ShipError, match=problem):
        ship.publish(repo, "main", push=False, review_root=root)
    assert run_git(repo, "rev-parse", "main") == before


def test_publish_decision_records_need_the_base_policy_list(repo: Path, tmp_path: Path) -> None:
    _decision_repo(repo, policy=MINI_POLICY)
    _gated_zone_c_change(repo)
    root = _outside(tmp_path, "reviews")
    _write_record(root, _decision_record())
    with pytest.raises(ship.ShipError, match="decision records are off"):
        ship.publish(repo, "main", push=False, review_root=root)


def test_publish_review_root_does_not_change_zone_a_or_b(repo: Path, tmp_path: Path) -> None:
    _decision_repo(repo)
    commit_file(repo, "src/app.py", "x\n", "code")
    ship.gate(repo, "main")
    done = ship.publish(repo, "main", push=False, review_root=_outside(tmp_path, "no-reviews"))
    assert "decision_records" not in done


def test_message_fingerprint_ignores_gate_trailer(repo: Path) -> None:
    commit_file(repo, "docs/a.md", "x\n", "docs: add a\n\nbody line")
    before = ship.message_fingerprint(repo)
    ship.gate(repo, "main")
    assert ship.message_fingerprint(repo) == before


def test_patch_fingerprint_leaves_out_rendered_task_views(repo: Path) -> None:
    commit_file(repo, "src/app.py", "x\n", "code")
    plain = ship.patch_fingerprint(repo, "main", MINI_POLICY)
    commit_file(repo, "docs/task_register.md", "rendered\n", "views")
    assert ship.patch_fingerprint(repo, "main", MINI_POLICY) == plain


def test_reviewed_patch_publishes_after_main_moves(repo: Path) -> None:
    message = "code: add app\n\nreviewed body"
    commit_file(repo, "src/app.py", "x\n", message)
    patch = ship.patch_fingerprint(repo, "main", MINI_POLICY)
    reviewed = ship.message_fingerprint(repo)
    reviewed_tree = run_git(repo, "rev-parse", "HEAD^{tree}")
    run_git(repo, "switch", "main")
    commit_file(repo, "docs/other.md", "m\n", "main moved")
    run_git(repo, "switch", "-c", "task/again")
    commit_file(repo, "src/app.py", "x\n", message)
    ship.gate(repo, "main")
    assert run_git(repo, "rev-parse", "HEAD^{tree}") != reviewed_tree
    done = ship.publish(repo, "main", push=False, expected_patch=patch, expected_message=reviewed)
    assert done["reviewed_patch"] == {"patch": patch, "message": reviewed}
    assert run_git(repo, "rev-parse", "main") == done["shipped_sha"]


@pytest.mark.parametrize("change", ["extra_hunk", "message", "two_commits"])
def test_reviewed_patch_refuses_any_difference(repo: Path, change: str) -> None:
    message = "code: add app"
    commit_file(repo, "src/app.py", "x\n", message)
    patch = ship.patch_fingerprint(repo, "main", MINI_POLICY)
    reviewed = ship.message_fingerprint(repo)
    before = run_git(repo, "rev-parse", "main")
    if change == "extra_hunk":
        (repo / "src/app.py").write_text("x\ny\n", encoding="utf-8")
        run_git(repo, "commit", "-a", "--amend", "-m", message)
    elif change == "message":
        run_git(repo, "commit", "--amend", "-m", message + "\n\nOwner-Decision: " + DECISION)
    else:
        run_git(repo, "reset", "--soft", "HEAD~1")
        run_git(repo, "commit", "-m", "half")
        commit_file(repo, "docs/empty.md", "", "second")
    ship.gate(repo, "main")
    problem = {
        "extra_hunk": "reviewed patch",
        "message": "reviewed message",
        "two_commits": "exactly one",
    }
    with pytest.raises(ship.ShipError, match=problem[change]):
        ship.publish(repo, "main", push=False, expected_patch=patch, expected_message=reviewed)
    assert run_git(repo, "rev-parse", "main") == before


@pytest.mark.parametrize(
    ("patch", "message", "problem"),
    [("A" * 40, "a" * 64, "40-character"), ("a" * 40, "a" * 63, "64-character")],
)
def test_reviewed_patch_formats(repo: Path, patch: str, message: str, problem: str) -> None:
    commit_file(repo, "src/app.py", "x\n", "code")
    ship.gate(repo, "main")
    with pytest.raises(ship.ShipError, match=problem):
        ship.publish(repo, "main", push=False, expected_patch=patch, expected_message=message)


def test_cli_check_decision_record(tmp_path: Path) -> None:
    good = _write_record(tmp_path, _decision_record())
    done = subprocess.run(
        [sys.executable, str(REPO / "tools/gov008/ship.py"), "--check-decision-record", str(good)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    summary = json.loads(done.stdout.strip().splitlines()[-1])
    assert summary["record_name"] == "T.2026-10-13.x.json"
    assert summary["c_paths"] == ["secret/app/**"]
    assert len(summary["sha256"]) == 64
    bad = _write_record(tmp_path / "bad", _decision_record(c_paths=[]))
    refused = subprocess.run(
        [sys.executable, str(REPO / "tools/gov008/ship.py"), "--check-decision-record", str(bad)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert refused.returncode == 2
    assert "c_paths must be" in refused.stderr


@pytest.mark.parametrize(
    "args",
    [
        ["--dry-run", "--review-root", "x"],
        ["--publish", "--expected-patch", "a" * 40],
        ["--publish", "--expected-message", "a" * 64],
    ],
)
def test_cli_rejects_publish_only_options_elsewhere(repo: Path, args: list[str]) -> None:
    done = subprocess.run(
        [sys.executable, str(REPO / "tools/gov008/ship.py"), "--repo", str(repo), *args],
        text=True,
        capture_output=True,
        check=False,
    )
    assert done.returncode == 2
    assert "need --publish" in done.stderr or "go together" in done.stderr
