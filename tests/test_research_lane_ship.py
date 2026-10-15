"""DEVX-030: research records ship on their own and are checked mechanically by ship.py.

Owner decision owner_decision:TRADING-2565:2026-10-13:research_throughput. Throw-away git
repositories carry the real policy's research_records rules, so the tests use them as configured.
"""

from __future__ import annotations

import hashlib
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
RECORDS_GATE = [["python", "-c", "pass"]]
LANE_POLICY = {
    "schema_version": "gov008_ship_policy.v1",
    "tree_clean_exclusions": [],
    "zones": {
        "C": ["research/*/prereg.yaml", "research/*/*.py", "config/*.yaml"],
        "A": ["docs/**", "research/**", "*.md"],
    },
    "research_records": REAL_POLICY["research_records"],
    "gates": {"A": [], "B": [], "C": [], "records": RECORDS_GATE},
}
QUESTION = "research/Q1"
RUNS = f"{QUESTION}/runs"
READS = f"{QUESTION}/window_reads.jsonl"
SIGNALS = "src/ai_trading_system/research_screens/signals"
SCREENS = "research/_screens/ledger"


def run_git(repo: Path, *args: str) -> str:
    done = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )
    return done.stdout.strip()


def raw_message(repo: Path, rev: str = "HEAD") -> bytes:
    commit = subprocess.run(
        ["git", "-C", str(repo), "cat-file", "commit", rev], capture_output=True, check=True
    ).stdout
    return commit.split(b"\n\n", 1)[1]


def write(repo: Path, rel: str, data: bytes) -> None:
    path = repo / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def commit_all(repo: Path, message: str) -> str:
    run_git(repo, "add", "-A")
    run_git(repo, "commit", "-q", "-m", message)
    return run_git(repo, "rev-parse", "HEAD")


def sha(repo: Path, rel: str) -> str:
    return hashlib.sha256((repo / rel).read_bytes()).hexdigest()


def build_repo(path: Path) -> None:
    run_git(path, "init", "-q", "-b", "main")
    run_git(path, "config", "user.email", "t@example.invalid")
    run_git(path, "config", "user.name", "t")
    run_git(path, "config", "core.autocrlf", "false")
    write(path, ship.POLICY_PATH, yaml.safe_dump(LANE_POLICY).encode())
    write(path, "README.md", b"start\n")
    write(path, f"{QUESTION}/prereg.yaml", b"question_id: Q1\n")
    write(path, f"{QUESTION}/strategy.py", b"def s1():\n    return 1\n")
    write(path, f"{SIGNALS}/mom.py", b"def signal():\n    return 0\n")
    write(path, f"{SIGNALS}/mom.yaml", b"signal_id: mom\n")
    commit_all(path, "init")
    run_git(path, "switch", "-q", "-c", "lane/q1")


@pytest.fixture()
def repo(tmp_path: Path, git_repo_template: Callable[..., Path]) -> Path:
    return git_repo_template(tmp_path, __file__, build_repo)


def run_record(repo: Path, number: int, prev: bytes | None, **overrides: object) -> bytes:
    commit = run_git(repo, "rev-parse", "main")
    record: dict = {
        "schema_version": "research_run.v1",
        "seq": number,
        "prev_sha256": None if prev is None else ship.chain_digest(prev),
        "strategy": {
            "id": "s1",
            "module": "strategy.py",
            "module_sha256": sha(repo, f"{QUESTION}/strategy.py"),
        },
        "prereg_sha256": sha(repo, f"{QUESTION}/prereg.yaml"),
        "provenance": {"commit": commit, "branch": "lane/q1", "code_modified": False},
    }
    for dotted, value in overrides.items():
        target = record
        keys = dotted.split("__")
        for key in keys[:-1]:
            target = target[key]
        target[keys[-1]] = value
    return (json.dumps(record, indent=1, sort_keys=True) + "\n").encode()


def add_runs(repo: Path, count: int, start: int = 1, **overrides: object) -> list[bytes]:
    """Write count chained run records after record start - 1 (read from the working tree)."""
    previous = (repo / RUNS / f"{start - 1:04d}.json").read_bytes() if start > 1 else None
    written = []
    for number in range(start, start + count):
        data = run_record(repo, number, previous, **overrides)
        write(repo, f"{RUNS}/{number:04d}.json", data)
        written.append(data)
        previous = data
    return written


def read_line(repo: Path, seq: int, prev: bytes | None, **overrides: object) -> bytes:
    entry = {
        "schema_version": "research_window_read.v1",
        "seq": seq,
        "prev_sha256": None if prev is None else ship.chain_digest(prev),
        "approval_commit": run_git(repo, "rev-parse", "main"),
        "prereg_sha256": sha(repo, f"{QUESTION}/prereg.yaml"),
    } | overrides
    return json.dumps(entry, sort_keys=True, separators=(",", ":")).encode() + b"\n"


def screen_record(repo: Path, number: int, prev: bytes | None, **overrides: object) -> bytes:
    record = {
        "schema_version": "research_screen_record.v1",
        "number": number,
        "prev_sha256": None if prev is None else ship.chain_digest(prev),
        "signal_id": "mom",
        "module_sha256": sha(repo, f"{SIGNALS}/mom.py"),
        "spec_sha256": sha(repo, f"{SIGNALS}/mom.yaml"),
        "code": {"commit": run_git(repo, "rev-parse", "main"), "modified": False},
    } | overrides
    return (json.dumps(record, indent=1, sort_keys=True) + "\n").encode()


def problems_of(repo: Path) -> list[str]:
    return ship.plan(repo, "main", policy=LANE_POLICY)["problems"]


# The real policy -------------------------------------------------------------------------------


def test_real_policy_records_are_zone_a_and_have_their_own_gate() -> None:
    records = REAL_POLICY["research_records"]
    samples = {
        "research/*/runs/*.json": "research/QQQ-Q1/runs/0001.json",
        "research/_screens/ledger/*.json": "research/_screens/ledger/0001.json",
        "research/*/window_reads.jsonl": "research/QQQ-Q1/window_reads.jsonl",
    }
    rules = [*records["numbered"], *records["appended"]]
    assert sorted(rule["glob"] for rule in rules) == sorted(samples)
    for rule in rules:
        sample = samples[rule["glob"]]
        assert ship.classify([sample], REAL_POLICY)["A"] == [sample]
        assert ship.record_rule(sample, REAL_POLICY) is not None
        assert set(rule) <= {"glob", "commit_field", "modified_field", "files_at_commit"}
    assert REAL_POLICY["gates"]["records"] == [
        ["python", "-m", "pytest", "tests/invariants", "-q", "-k", "repository_ledgers"]
    ]
    for path in ("research/QQQ-Q1/strategy.py", "research/QQQ-Q1/prereg.yaml", "docs/x.md"):
        assert ship.record_rule(path, REAL_POLICY) is None


def test_record_rules_match_the_bench_record_layout() -> None:
    """The fields ship.py reads are the ones the bench writes (research_bench.ledger)."""
    source = (REPO / "src/ai_trading_system/research_bench/ledger.py").read_text(encoding="utf-8")
    for needle in (
        '"provenance": git_provenance(',
        '"module_sha256": question.strategy_sha256',
        '"module": question.strategy_path.name',
        '"prereg_sha256": question.prereg_sha256',
        '"approval_commit": git_output(',
        '"prev_sha256": chain_digest(',
    ):
        assert needle in source
    provenance = (REPO / "src/ai_trading_system/core/provenance.py").read_text(encoding="utf-8")
    assert '"code_modified": UNAVAILABLE if status is None else bool(status)' in provenance


# Plans of records-only changes -----------------------------------------------------------------


def test_records_only_change_plans_the_records_gate_and_its_fingerprints(repo: Path) -> None:
    add_runs(repo, 2)
    commit_all(repo, "Q1: development runs 0001-0002")
    p = ship.plan(repo, "main", policy=LANE_POLICY)
    assert p["ok"], p["problems"]
    assert p["zone"] == "A" and p["gate"] == RECORDS_GATE
    research = p["research_records"]
    assert research["paths"] == 2
    assert research["patch"] == ship.patch_fingerprint(repo, "main", LANE_POLICY)
    assert research["message"] == ship.message_fingerprint(repo)


def test_several_records_commits_plan_without_fingerprints(repo: Path) -> None:
    add_runs(repo, 1)
    commit_all(repo, "run 0001")
    add_runs(repo, 1, start=2)
    commit_all(repo, "run 0002")
    p = ship.plan(repo, "main", policy=LANE_POLICY)
    assert p["ok"] and p["gate"] == RECORDS_GATE
    assert p["research_records"] == {"paths": 2}


def test_records_numbered_on_from_the_base_and_chained_to_its_last_record(repo: Path) -> None:
    add_runs(repo, 1)
    commit_all(repo, "run 0001")
    run_git(repo, "switch", "-q", "main")
    run_git(repo, "merge", "-q", "--ff-only", "lane/q1")
    run_git(repo, "switch", "-q", "-c", "lane/q1-more")
    add_runs(repo, 2, start=2)
    commit_all(repo, "runs 0002-0003")
    assert problems_of(repo) == []


def test_records_and_other_files_do_not_ship_together(repo: Path) -> None:
    add_runs(repo, 1)
    write(repo, "docs/note.md", b"note\n")
    commit_all(repo, "run and a note")
    p = ship.plan(repo, "main", policy=LANE_POLICY)
    assert not p["ok"] and "research_records" not in p
    assert any(
        "ship on their own" in problem and "docs/note.md" in problem for problem in p["problems"]
    )


def test_a_change_without_records_keeps_its_zone_gate(repo: Path) -> None:
    write(repo, "docs/note.md", b"note\n")
    commit_all(repo, "a note")
    p = ship.plan(repo, "main", policy=LANE_POLICY)
    assert p["ok"] and p["gate"] == [] and "research_records" not in p


def _side_commit(repo: Path) -> str:
    tree = run_git(repo, "rev-parse", "main^{tree}")
    return run_git(repo, "commit-tree", tree, "-p", "main", "-m", "never published")


@pytest.mark.parametrize(
    ("case", "problem"),
    [
        ("gap", "expected record 0001 next"),
        ("bad_name", "named NNNN.json"),
        ("prev", "prev_sha256 does not match"),
        ("not_json", "is not a JSON object"),
        ("short_commit", "provenance.commit is not a full commit id"),
        ("unpublished", "is not a published commit"),
        ("modified", "provenance.code_modified is not false"),
        ("unavailable", "provenance.code_modified is not false"),
        ("strategy_hash", "strategy.module_sha256 is not the hash of research/Q1/strategy.py"),
        ("prereg_hash", "prereg_sha256 is not the hash of research/Q1/prereg.yaml"),
        ("missing_module", "research/Q1/other.py is not in commit"),
        ("escaping_module", "cannot name the file for strategy.module_sha256"),
        ("executable", "regular file"),
    ],
)
def test_numbered_record_refusals(repo: Path, case: str, problem: str) -> None:
    overrides: dict[str, object] = {
        "short_commit": {"provenance__commit": "abc123"},
        "unpublished": {"provenance__commit": _side_commit(repo)},
        "modified": {"provenance__code_modified": True},
        "unavailable": {"provenance__code_modified": "unavailable"},
        "strategy_hash": {"strategy__module_sha256": "0" * 64},
        "prereg_hash": {"prereg_sha256": "0" * 64},
        "missing_module": {"strategy__module": "other.py"},
        "escaping_module": {"strategy__module": "../strategy.py"},
    }.get(case, {})
    if case == "gap":
        write(repo, f"{RUNS}/0002.json", run_record(repo, 2, None))
    elif case == "bad_name":
        write(repo, f"{RUNS}/run.json", run_record(repo, 1, None))
    elif case == "prev":
        write(repo, f"{RUNS}/0001.json", run_record(repo, 1, b"something else"))
    elif case == "not_json":
        write(repo, f"{RUNS}/0001.json", b"not json\n")
    else:
        add_runs(repo, 1, **overrides)
    if case == "executable":
        run_git(repo, "add", "-A")
        run_git(repo, "update-index", "--chmod=+x", f"{RUNS}/0001.json")
        run_git(repo, "commit", "-q", "-m", "records")
    else:
        commit_all(repo, "records")
    problems = problems_of(repo)
    assert any(problem in item for item in problems), problems


@pytest.mark.parametrize("change", ["modify", "delete"])
def test_a_published_numbered_record_is_never_changed_or_removed(repo: Path, change: str) -> None:
    add_runs(repo, 1)
    commit_all(repo, "run 0001")
    run_git(repo, "switch", "-q", "main")
    run_git(repo, "merge", "-q", "--ff-only", "lane/q1")
    run_git(repo, "switch", "-q", "-c", "lane/q1-edit")
    if change == "modify":
        write(repo, f"{RUNS}/0001.json", run_record(repo, 1, None, seq=9))
    else:
        (repo / RUNS / "0001.json").unlink()
    commit_all(repo, "rewrite")
    letter = "M" if change == "modify" else "D"
    assert problems_of(repo) == [
        f"{RUNS}/0001.json: a numbered record is only ever added (status {letter})"
    ]


# Appended JSON-lines records -------------------------------------------------------------------


def _published_reads(repo: Path, count: int) -> bytes:
    data = b""
    previous = None
    for seq in range(1, count + 1):
        line = read_line(repo, seq, previous)
        data += line
        previous = line.rstrip(b"\n")
    write(repo, READS, data)
    commit_all(repo, "reads")
    run_git(repo, "switch", "-q", "main")
    run_git(repo, "merge", "-q", "--ff-only", "lane/q1")
    run_git(repo, "switch", "-q", "-c", "lane/q1-reads")
    return data


def test_appended_lines_chained_to_the_last_published_line_pass(repo: Path) -> None:
    before = _published_reads(repo, 1)
    write(repo, READS, before + read_line(repo, 2, before.rstrip(b"\n")))
    commit_all(repo, "read 2")
    assert problems_of(repo) == []


def test_a_new_lines_file_starts_with_no_previous_line(repo: Path) -> None:
    first = read_line(repo, 1, None)
    write(repo, READS, first + read_line(repo, 2, first.rstrip(b"\n")))
    commit_all(repo, "reads")
    assert problems_of(repo) == []


def test_a_published_lines_file_is_never_removed(repo: Path) -> None:
    _published_reads(repo, 1)
    (repo / READS).unlink()
    commit_all(repo, "remove reads")
    assert problems_of(repo) == [f"{READS}: a record file is only ever appended (status D)"]


@pytest.mark.parametrize(
    ("case", "problem"),
    [
        ("rewrite", "existing lines changed"),
        ("no_newline", "do not end with a newline"),
        ("prev", "line 2: prev_sha256 does not match"),
        ("not_object", "line 2 is not a JSON object"),
        ("unpublished", "approval_commit"),
        ("prereg_hash", "prereg_sha256 is not the hash of research/Q1/prereg.yaml"),
    ],
)
def test_appended_line_refusals(repo: Path, case: str, problem: str) -> None:
    before = _published_reads(repo, 1)
    last = before.rstrip(b"\n")
    line = read_line(repo, 2, last)
    if case == "rewrite":
        data = read_line(repo, 1, None, role="changed") + line
    elif case == "no_newline":
        data = before + line.rstrip(b"\n")
    elif case == "prev":
        data = before + read_line(repo, 2, b"other")
    elif case == "not_object":
        data = before + b"[1, 2]\n"
    elif case == "unpublished":
        data = before + read_line(repo, 2, last, approval_commit=_side_commit(repo))
    else:
        data = before + read_line(repo, 2, last, prereg_sha256="0" * 64)
    write(repo, READS, data)
    commit_all(repo, "read 2")
    problems = problems_of(repo)
    assert any(problem in item for item in problems), problems


# The screening ledger --------------------------------------------------------------------------


def test_screen_records_name_their_signal_files_at_the_commit(repo: Path) -> None:
    first = screen_record(repo, 1, None)
    write(repo, f"{SCREENS}/0001.json", first)
    write(repo, f"{SCREENS}/0002.json", screen_record(repo, 2, first))
    commit_all(repo, "screens")
    assert problems_of(repo) == []


@pytest.mark.parametrize(
    ("overrides", "problem"),
    [
        ({"signal_id": "new_signal"}, f"{SIGNALS}/new_signal.py is not in commit"),
        ({"spec_sha256": "0" * 64}, "spec_sha256 is not the hash of"),
        ({"code": {"commit": "x", "modified": False}}, "code.commit is not a full commit id"),
    ],
)
def test_screen_record_refusals(repo: Path, overrides: dict, problem: str) -> None:
    write(repo, f"{SCREENS}/0001.json", screen_record(repo, 1, None, **overrides))
    commit_all(repo, "screen")
    problems = problems_of(repo)
    assert any(problem in item for item in problems), problems


# Gate and publish ------------------------------------------------------------------------------


def test_records_only_commit_gates_with_the_records_gate_and_publishes_by_patch(
    repo: Path,
) -> None:
    add_runs(repo, 2)
    commit_all(repo, "Q1: development runs 0001-0002")
    research = ship.plan(repo, "main")["research_records"]
    gated = ship.gate(repo, "main")
    assert gated["gate"]["commands"] == len(RECORDS_GATE)
    assert f"commands={len(RECORDS_GATE)} " in run_git(repo, "log", "-1", "--format=%B")
    done = ship.publish(
        repo,
        "main",
        push=False,
        expected_patch=research["patch"],
        expected_message=research["message"],
    )
    assert run_git(repo, "rev-parse", "main") == done["shipped_sha"] == gated["gated_sha"]


def test_publish_refuses_a_broken_record_whatever_the_patch(repo: Path) -> None:
    add_runs(repo, 1, provenance__code_modified=True)
    commit_all(repo, "run made on changed code")
    patch = ship.patch_fingerprint(repo, "main", LANE_POLICY)
    message = ship.message_fingerprint(repo)
    before = run_git(repo, "rev-parse", "main")
    with pytest.raises(ship.ShipError, match="code_modified is not false"):
        ship.gate(repo, "main")
    with pytest.raises(ship.ShipError, match="code_modified is not false"):
        ship.publish(repo, "main", push=False, expected_patch=patch, expected_message=message)
    assert run_git(repo, "rev-parse", "main") == before


# Replay onto a moved base ----------------------------------------------------------------------


def _move_main(repo: Path, rel: str = "docs/other.md", data: bytes = b"main moved\n") -> str:
    branch = run_git(repo, "branch", "--show-current")
    run_git(repo, "switch", "-q", "main")
    write(repo, rel, data)
    moved = commit_all(repo, "main moved")
    run_git(repo, "switch", "-q", branch)
    return moved


def test_replay_puts_the_same_records_on_the_moved_base(repo: Path) -> None:
    add_runs(repo, 2)
    commit_all(repo, "Q1: development runs 0001-0002\n\nbody")
    research = "--author=Research Session <research@example.invalid>"
    run_git(repo, "commit", "-q", "--amend", "--no-edit", research, "--date=1760000000 +0900")
    before = ship.plan(repo, "main", policy=LANE_POLICY)["research_records"]
    author = run_git(repo, "log", "-1", "--format=%an|%ae|%ad", "--date=raw")
    assert author == "Research Session|research@example.invalid|1760000000 +0900"
    old = ship.gate(repo, "main")["gated_sha"]
    moved = _move_main(repo)
    stale = ship.plan(repo, "main", policy=LANE_POLICY)
    assert not stale["ok"] and any("--replay-records" in item for item in stale["problems"])

    done = ship.replay_records(repo, "main")
    new = run_git(repo, "rev-parse", "HEAD")
    assert done["replayed_from"] == old and done["replayed_sha"] == new != old
    assert done["base_sha"] == moved == run_git(repo, "rev-parse", "HEAD^")
    assert done["patch"] == before["patch"] and done["message"] == before["message"]
    assert done["research_records"] == 2
    assert run_git(repo, "log", "-1", "--format=%an|%ae|%ad", "--date=raw") == author
    assert raw_message(repo) == b"Q1: development runs 0001-0002\n\nbody\n"
    assert run_git(repo, "status", "--porcelain") == ""
    assert (repo / "docs/other.md").is_file() and (repo / RUNS / "0002.json").is_file()
    after = ship.plan(repo, "main", policy=LANE_POLICY)
    assert after["ok"], after["problems"]
    assert after["research_records"] == before


def test_replay_keeps_appended_lines_when_the_base_did_not_touch_the_file(repo: Path) -> None:
    before = _published_reads(repo, 1)
    write(repo, READS, before + read_line(repo, 2, before.rstrip(b"\n")))
    commit_all(repo, "read 2")
    _move_main(repo, f"{QUESTION}/strategy.py", b"def s1():\n    return 2\n")
    ship.replay_records(repo, "main")
    assert problems_of(repo) == []
    assert (repo / f"{QUESTION}/strategy.py").read_bytes() == b"def s1():\n    return 2\n"


@pytest.mark.parametrize(
    ("case", "problem"),
    [
        ("up_to_date", "nothing to replay"),
        ("two_commits", "exactly one commit"),
        ("not_records", "research records and nothing else"),
        ("base_changed_file", "since the records were made"),
        ("broken", "code_modified is not false"),
        ("dirty", "working tree is not clean"),
        ("on_main", "task branch"),
    ],
)
def test_replay_refusals_leave_the_branch_alone(repo: Path, case: str, problem: str) -> None:
    if case == "base_changed_file":
        before = _published_reads(repo, 1)
        write(repo, READS, before + read_line(repo, 2, before.rstrip(b"\n")))
        commit_all(repo, "read 2")
        _move_main(repo, READS, before + read_line(repo, 2, before.rstrip(b"\n"), role="other"))
    else:
        add_runs(repo, 1, **({"provenance__code_modified": True} if case == "broken" else {}))
        commit_all(repo, "run 0001")
        if case == "two_commits":
            add_runs(repo, 1, start=2)
            commit_all(repo, "run 0002")
        if case == "not_records":
            write(repo, "docs/note.md", b"note\n")
            run_git(repo, "add", "-A")
            run_git(repo, "commit", "-q", "--amend", "-m", "run and note")
        if case != "up_to_date":
            _move_main(repo)
        if case == "dirty":
            write(repo, "docs/scratch.md", b"x\n")
        if case == "on_main":
            run_git(repo, "switch", "-q", "main")
    head = run_git(repo, "rev-parse", "HEAD")
    with pytest.raises(ship.ShipError, match=problem):
        ship.replay_records(repo, "main")
    assert run_git(repo, "rev-parse", "HEAD") == head


def test_cli_replay_records_prints_the_result(repo: Path) -> None:
    add_runs(repo, 1)
    commit_all(repo, "run 0001")
    _move_main(repo)
    done = subprocess.run(
        [
            sys.executable,
            str(REPO / "tools/gov008/ship.py"),
            "--repo",
            str(repo),
            "--replay-records",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    result = json.loads(done.stdout.strip().splitlines()[-1])
    assert result["replayed_sha"] == run_git(repo, "rev-parse", "HEAD")
    again = subprocess.run(
        [
            sys.executable,
            str(REPO / "tools/gov008/ship.py"),
            "--repo",
            str(repo),
            "--replay-records",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert again.returncode == 2 and "nothing to replay" in again.stderr
