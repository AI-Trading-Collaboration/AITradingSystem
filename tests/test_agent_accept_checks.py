# ruff: noqa: E501
from __future__ import annotations

import json
import subprocess
import sys
from importlib import util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "agent" / "accept_checks.py"

POLICY = """schema_version: gov008_ship_policy.v1
tree_clean_exclusions:
  - docs/research/dirty.md
zones:
  C:
    - config/critical.yaml
    - tests/invariants/**
  A:
    - docs/**
    - "*.md"
"""

BRIEF = """---
task: DEVX-1
run: DEVX-1-01
template: mechanical-change
zone: B
model: local
thinking: default
tools: default
owner_decision: none
allow_done: false
ship: publish
allowed_paths:
  - src/**
  - tests/**
  - tasks/*.yaml
  - docs/task_register.md
  - docs/task_register_completed.md
read_paths: []
---
# DEVX-1

## 等价检查

不适用：测试用例。
"""


def git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=repo, check=check, text=True, capture_output=True)


def init_repo(tmp_path: Path, brief_text: str = BRIEF) -> tuple[Path, Path]:
    repo = tmp_path / "repo"
    run_dir = tmp_path / "run"
    repo.mkdir(parents=True)
    run_dir.mkdir(parents=True)
    git(repo, "init", "-b", "main")
    git(repo, "config", "user.email", "t@example.com")
    git(repo, "config", "user.name", "Tester")
    (repo / "config").mkdir()
    (repo / "config/gov008_ship.yaml").write_text(POLICY, encoding="utf-8")
    (repo / "src").mkdir()
    (repo / "src/app.py").write_text("VALUE = 1\n", encoding="utf-8")
    (repo / "tests").mkdir()
    (repo / "tests/test_app.py").write_text("def test_ok():\n    assert True\n", encoding="utf-8")
    (repo / "tasks").mkdir()
    (repo / "tasks/DEVX.yaml").write_text("status: IN_PROGRESS\nnotes: old\n", encoding="utf-8")
    (repo / "docs").mkdir()
    (repo / "docs/requirements").mkdir()
    (repo / "docs/requirements/REQ.md").write_text("owner_decision:DEVX-1:2026-10-11:ok\n", encoding="utf-8")
    (repo / "docs/task_register.md").write_text("old\n", encoding="utf-8")
    (repo / "docs/task_register_completed.md").write_text("old\n", encoding="utf-8")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "base")
    git(repo, "checkout", "-b", "work")
    (run_dir / "brief.md").write_text(brief_text, encoding="utf-8")
    (run_dir / "report.json").write_text('{"blocked": [], "out_of_scope_files": []}\n', encoding="utf-8")
    return repo, run_dir


def commit_change(repo: Path, path: str, content: str, message: str = "change") -> str:
    full = repo / path
    full.parent.mkdir(parents=True, exist_ok=True)
    full.write_text(content, encoding="utf-8")
    git(repo, "add", path)
    git(repo, "commit", "-m", message)
    return git(repo, "rev-parse", "HEAD").stdout.strip()


def run_checks(repo: Path, run_dir: Path, head: str = "HEAD", *extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--repo", str(repo), "--run-dir", str(run_dir), "--head", head, *extra],
        text=True,
        capture_output=True,
        timeout=60,
    )


def check_map(run_dir: Path) -> dict[str, dict]:
    data = json.loads((run_dir / "checks.json").read_text(encoding="utf-8"))
    return {c["name"]: c for c in data["checks"]}


def test_all_checks_pass_for_simple_allowed_change(tmp_path: Path) -> None:
    repo, run_dir = init_repo(tmp_path)
    commit_change(repo, "src/app.py", "VALUE = 2\n")
    res = run_checks(repo, run_dir)
    assert res.returncode == 0, res.stdout + res.stderr
    checks = check_map(run_dir)
    assert all(c["ok"] for c in checks.values())


def test_allowed_paths_failure(tmp_path: Path) -> None:
    repo, run_dir = init_repo(tmp_path)
    commit_change(repo, "other.txt", "x\n")
    res = run_checks(repo, run_dir)
    assert res.returncode == 1
    assert not check_map(run_dir)["changes within allowed_paths"]["ok"]


def test_c_zone_requires_c_brief_and_owner_decision(tmp_path: Path) -> None:
    repo, run_dir = init_repo(tmp_path)
    commit_change(repo, "config/critical.yaml", "x: 1\n")
    res = run_checks(repo, run_dir)
    assert res.returncode == 1
    assert not check_map(run_dir)["file zones match brief"]["ok"]


def test_exclusion_and_orphan_task_views_fail(tmp_path: Path) -> None:
    repo, run_dir = init_repo(tmp_path, BRIEF.replace("  - docs/task_register_completed.md\n", "  - docs/task_register_completed.md\n  - docs/research/dirty.md\n"))
    commit_change(repo, "docs/research/dirty.md", "dirty\n")
    res = run_checks(repo, run_dir)
    assert res.returncode == 1
    assert not check_map(run_dir)["exclusions and task views"]["ok"]


def test_test_weakening_detects_removed_assert(tmp_path: Path) -> None:
    repo, run_dir = init_repo(tmp_path)
    commit_change(repo, "tests/test_app.py", "def test_ok():\n    pass\n")
    res = run_checks(repo, run_dir)
    assert res.returncode == 1
    assert not check_map(run_dir)["tests not weakened"]["ok"]


def test_skip_and_xfail_additions_are_listed_for_review_but_do_not_fail(tmp_path: Path) -> None:
    repo, run_dir = init_repo(tmp_path)
    body = """import pytest
import unittest
from unittest import SkipTest

pytestmark = pytest.mark.skipif(True, reason='x')
CASES = [pytest.param(1, marks=pytest.mark.xfail(reason='x'))]

@pytest.mark.skip(reason='x')
def test_a():
    assert True

def test_b():
    raise pytest.skip.Exception('x')

def test_c():
    pytest.importorskip('missing_pkg')

def test_d():
    pytest.xfail('x')

def test_e():
    raise unittest.SkipTest('x')

def test_f():
    raise SkipTest('x')
"""
    commit_change(repo, "tests/test_app.py", body)
    res = run_checks(repo, run_dir)
    assert res.returncode == 0, res.stdout + res.stderr
    check = check_map(run_dir)["tests not weakened"]
    assert check["ok"]
    detail = check["detail"]
    for needle in ["pytest.mark.skipif", "pytest.mark.xfail", "pytest.mark.skip", "pytest.skip.Exception", "pytest.importorskip", "pytest.xfail", "unittest.SkipTest", "SkipTest("]:
        assert needle in detail
    assert "tests/test_app.py:" in detail


def test_tolerance_change_is_reported_but_not_failed(tmp_path: Path) -> None:
    repo, run_dir = init_repo(tmp_path)
    commit_change(repo, "tests/test_app.py", "import pytest\n\ndef test_ok():\n    assert 1 == pytest.approx(1, rel=0.2)\n")
    res = run_checks(repo, run_dir)
    assert res.returncode == 0, res.stdout + res.stderr
    c = check_map(run_dir)["tests not weakened"]
    assert c["ok"] and "tolerance" in c["detail"]


def test_task_notes_must_be_append_only_and_done_forbidden(tmp_path: Path) -> None:
    repo, run_dir = init_repo(tmp_path)
    commit_change(repo, "tasks/DEVX.yaml", "status: DONE\nnotes: changed\n")
    res = run_checks(repo, run_dir)
    assert res.returncode == 1
    assert not check_map(run_dir)["task notes append-only and allow_done"]["ok"]


def test_report_missing_fails(tmp_path: Path) -> None:
    repo, run_dir = init_repo(tmp_path)
    (run_dir / "report.json").unlink()
    commit_change(repo, "src/app.py", "VALUE = 30\n")
    res = run_checks(repo, run_dir)
    assert res.returncode == 1
    assert "缺失" in check_map(run_dir)["run report"]["detail"]


def test_report_out_of_scope_fails_and_blocked_rules_are_listed(tmp_path: Path) -> None:
    repo, run_dir = init_repo(tmp_path)
    (run_dir / "report.json").write_text(json.dumps({"blocked": [{"rule": "git-whole-repo"}], "out_of_scope_files": ["x"]}), encoding="utf-8")
    commit_change(repo, "src/app.py", "VALUE = 3\n")
    res = run_checks(repo, run_dir)
    assert res.returncode == 1
    c = check_map(run_dir)["run report"]
    assert not c["ok"] and "git-whole-repo" in c["detail"]


def test_equivalence_required_when_section_is_not_na(tmp_path: Path) -> None:
    brief_text = BRIEF.replace("不适用：测试用例。", "需要对比。")
    repo, run_dir = init_repo(tmp_path, brief_text)
    commit_change(repo, "src/app.py", "VALUE = 4\n")
    res = run_checks(repo, run_dir)
    assert res.returncode == 1
    assert not check_map(run_dir)["equivalence artifact"]["ok"]
    (run_dir / "equivalence.json").write_text('{"ok": true}\n', encoding="utf-8")
    res = run_checks(repo, run_dir)
    assert res.returncode == 0, res.stdout + res.stderr


def amend_gate(repo: Path, trailer: str) -> None:
    git(repo, "commit", "--amend", "-m", "change\n\nGate: " + trailer)


def test_gate_trailer_real_format_passes_and_failures_are_detected(tmp_path: Path) -> None:
    repo, run_dir = init_repo(tmp_path)
    commit_change(repo, "src/app.py", "VALUE = 5\n")
    tree = git(repo, "rev-parse", "HEAD^{tree}").stdout.strip()
    amend_gate(repo, f"zone=B tree={tree[:12]} commands=1 seconds=626.2 PASS")
    res = run_checks(repo, run_dir, "HEAD", "--gate")
    assert res.returncode == 0, res.stdout + res.stderr

    commit_change(repo, "src/app.py", "VALUE = 6\n")
    assert run_checks(repo, run_dir, "HEAD", "--gate").returncode == 1
    tree = git(repo, "rev-parse", "HEAD^{tree}").stdout.strip()
    amend_gate(repo, "zone=B tree=deadbeef commands=1 seconds=1.0 PASS")
    assert run_checks(repo, run_dir, "HEAD", "--gate").returncode == 1
    amend_gate(repo, f"zone=B tree={tree[:12]} commands=1 seconds=1.0 FAIL")
    assert run_checks(repo, run_dir, "HEAD", "--gate").returncode == 1


def test_c_zone_with_owner_decision_on_main_passes(tmp_path: Path) -> None:
    c_brief = BRIEF.replace("zone: B", "zone: C").replace("owner_decision: none", "owner_decision: owner_decision:DEVX-1:2026-10-11:ok").replace("  - docs/task_register_completed.md\n", "  - docs/task_register_completed.md\n  - config/critical.yaml\n")
    repo, run_dir = init_repo(tmp_path, c_brief)
    commit_change(repo, "config/critical.yaml", "x: 2\n")
    assert run_checks(repo, run_dir).returncode == 0


def test_task_views_pass_when_task_yaml_changes_and_fail_when_alone(tmp_path: Path) -> None:
    repo, run_dir = init_repo(tmp_path)
    (repo / "docs/task_register.md").write_text("new\n", encoding="utf-8")
    (repo / "tasks/DEVX.yaml").write_text("status: IN_PROGRESS\nnotes: old; more\n", encoding="utf-8")
    git(repo, "add", "docs/task_register.md", "tasks/DEVX.yaml")
    git(repo, "commit", "-m", "task view")
    assert run_checks(repo, run_dir).returncode == 0

    repo2, run_dir2 = init_repo(tmp_path / "alone")
    commit_change(repo2, "docs/task_register.md", "new\n")
    assert run_checks(repo2, run_dir2).returncode == 1


def test_zone_classification_matches_ship_for_real_policy() -> None:
    spec = util.spec_from_file_location("ship", ROOT / "tools/gov008/ship.py")
    assert spec and spec.loader
    ship = util.module_from_spec(spec)
    spec.loader.exec_module(ship)
    sys.path.insert(0, str(ROOT / "scripts/agent"))
    import accept_checks  # noqa: PLC0415

    policy_text = subprocess.run(["git", "show", "main:config/gov008_ship.yaml"], cwd=ROOT, text=True, capture_output=True, check=True).stdout
    policy = accept_checks.parse_policy(policy_text)
    paths = subprocess.run(["git", "ls-files"], cwd=ROOT, text=True, capture_output=True, check=True).stdout.splitlines()
    paths += [".agents/skills/ship-change/SKILL.md", ".pi/prompts/x.md", "README.md", "docs/x.md", "tasks/x.yaml", "src/ai_trading_system/scoring/a.py", "config/ibkr_paper_x.yaml"]
    ours = {p: accept_checks.zone_for(p, policy) for p in paths}
    theirs_by_zone = ship.classify(paths, ship.load_policy(ROOT))
    theirs = {p: z for z, vals in theirs_by_zone.items() for p in vals}
    assert ours == theirs


def test_excluded_worktree_file_and_non_head_worktree_are_ignored(tmp_path: Path) -> None:
    repo, run_dir = init_repo(tmp_path)
    commit_change(repo, "src/app.py", "VALUE = 7\n")
    dirty = repo / "docs/research/dirty.md"
    dirty.parent.mkdir(parents=True, exist_ok=True)
    dirty.write_text("SECRET\n", encoding="utf-8")
    res = run_checks(repo, run_dir)
    assert "docs/research/dirty.md" not in res.stdout
    assert "SECRET" not in res.stdout

    head = git(repo, "rev-parse", "HEAD").stdout.strip()
    commit_change(repo, "src/other.py", "x=1\n")
    (repo / "other.txt").write_text("dirty\n", encoding="utf-8")
    res = run_checks(repo, run_dir, head)
    assert "working tree ignored" in check_map(run_dir)["changes within allowed_paths"]["detail"]
    assert "other.txt" not in res.stdout


def test_test_function_rename_passes_but_delete_fails(tmp_path: Path) -> None:
    repo, run_dir = init_repo(tmp_path)
    commit_change(repo, "tests/test_app.py", "def test_renamed():\n    assert True\n")
    assert run_checks(repo, run_dir).returncode == 0
    commit_change(repo, "tests/test_app.py", "VALUE = 1\n")
    assert run_checks(repo, run_dir).returncode == 1



def test_gate_c_zone_owner_decision_in_first_commit_passes(tmp_path: Path) -> None:
    c_brief = BRIEF.replace("zone: B", "zone: C").replace("owner_decision: none", "owner_decision: owner_decision:DEVX-1:2026-10-11:ok").replace("  - docs/task_register_completed.md\n", "  - docs/task_register_completed.md\n  - config/critical.yaml\n")
    repo, run_dir = init_repo(tmp_path, c_brief)
    commit_change(repo, "config/critical.yaml", "x: 3\n", "critical\n\nOwner-Decision: owner_decision:DEVX-1:2026-10-11:ok")
    commit_change(repo, "src/app.py", "VALUE = 8\n")
    tree = git(repo, "rev-parse", "HEAD^{tree}").stdout.strip()
    amend_gate(repo, f"zone=C tree={tree[:12]} commands=1 seconds=1.0 PASS")
    res = run_checks(repo, run_dir, "HEAD", "--gate")
    assert res.returncode == 0, res.stdout + res.stderr


def test_gate_c_zone_owner_decision_mismatch_fails(tmp_path: Path) -> None:
    c_brief = BRIEF.replace("zone: B", "zone: C").replace("owner_decision: none", "owner_decision: owner_decision:DEVX-1:2026-10-11:other").replace("  - docs/task_register_completed.md\n", "  - docs/task_register_completed.md\n  - config/critical.yaml\n")
    repo, run_dir = init_repo(tmp_path, c_brief)
    commit_change(repo, "config/critical.yaml", "x: 4\n", "critical\n\nOwner-Decision: owner_decision:DEVX-1:2026-10-11:ok")
    tree = git(repo, "rev-parse", "HEAD^{tree}").stdout.strip()
    amend_message(repo, f"critical\n\nOwner-Decision: owner_decision:DEVX-1:2026-10-11:ok\nGate: zone=C tree={tree[:12]} commands=1 seconds=1.0 PASS")
    res = run_checks(repo, run_dir, "HEAD", "--gate")
    assert res.returncode == 1
    assert "brief owner_decision is not among Owner-Decision trailers in base..head" in check_map(run_dir)["gate trailers"]["detail"]


def amend_message(repo: Path, message: str) -> None:
    git(repo, "commit", "--amend", "-m", message)


def test_gate_uses_last_gate_trailer(tmp_path: Path) -> None:
    repo, run_dir = init_repo(tmp_path)
    commit_change(repo, "src/app.py", "VALUE = 9\n")
    tree = git(repo, "rev-parse", "HEAD^{tree}").stdout.strip()
    amend_message(repo, f"change\n\nGate: zone=B tree={tree[:12]} commands=1 seconds=1.0 PASS\nGate: zone=B tree=deadbeefdead commands=1 seconds=1.0 PASS")
    res = run_checks(repo, run_dir, "HEAD", "--gate")
    assert res.returncode == 1
    assert "does not match head tree" in check_map(run_dir)["gate trailers"]["detail"]
    amend_message(repo, f"change\n\nGate: zone=B tree=deadbeefdead commands=1 seconds=1.0 PASS\nGate: zone=B tree={tree[:12]} commands=1 seconds=1.0 PASS")
    res = run_checks(repo, run_dir, "HEAD", "--gate")
    assert res.returncode == 0, res.stdout + res.stderr


def test_gate_short_tree_and_wrong_zone_fail(tmp_path: Path) -> None:
    repo, run_dir = init_repo(tmp_path)
    commit_change(repo, "src/app.py", "VALUE = 10\n")
    tree = git(repo, "rev-parse", "HEAD^{tree}").stdout.strip()
    amend_gate(repo, f"zone=B tree={tree[:7]} commands=1 seconds=1.0 PASS")
    res = run_checks(repo, run_dir, "HEAD", "--gate")
    assert res.returncode == 1
    assert "format is invalid" in check_map(run_dir)["gate trailers"]["detail"]
    amend_gate(repo, f"zone=A tree={tree[:12]} commands=1 seconds=1.0 PASS")
    res = run_checks(repo, run_dir, "HEAD", "--gate")
    assert res.returncode == 1
    assert "does not match planned zone" in check_map(run_dir)["gate trailers"]["detail"]


def test_gate_regex_matches_ship() -> None:
    spec = util.spec_from_file_location("ship", ROOT / "tools/gov008/ship.py")
    assert spec and spec.loader
    ship = util.module_from_spec(spec)
    spec.loader.exec_module(ship)
    sys.path.insert(0, str(ROOT / "scripts/agent"))
    import accept_checks  # noqa: PLC0415
    assert accept_checks.GATE_RE.pattern == ship.GATE_RE.pattern


@pytest.mark.parametrize(
    "line",
    [
        "mark.skip(reason='x')",
        "mark.skipif(True, reason='x')",
        "mark.xfail(reason='x')",
        "skip('x')",
        "xfail('x')",
        "importorskip('x')",
        "unittest.skip('x')",
        "unittest.skipIf(True, 'x')",
        "unittest.skipUnless(False, 'x')",
        "unittest.expectedFailure(test_ok)",
        "@skip('x')",
        "@skipIf(True, 'x')",
        "@skipUnless(False, 'x')",
        "@expectedFailure",
        "from pytest import skip, xfail, importorskip, mark",
        "test.skip('x', () => {})",
        "it.skip('x', () => {})",
        "describe.skip('x', () => {})",
        "test.todo('x')",
        "it.todo('x')",
        "describe.todo('x')",
        "t.skip('x')",
        "t.todo('x')",
        "test('x', { skip: true }, () => {})",
        "test('x', { todo: true }, () => {})",
    ],
)
def test_new_skip_forms_are_listed_not_failed(tmp_path: Path, line: str) -> None:
    repo, run_dir = init_repo(tmp_path)
    suffix = ".test.ts" if any(token in line for token in ["test.", "it.", "describe.", "t.", "{ skip", "{ todo"]) else ".py"
    path = "tests/test_added" + suffix
    body = line + "\n" if suffix == ".test.ts" else line + "\ndef test_ok():\n    assert True\n"
    commit_change(repo, path, body)
    res = run_checks(repo, run_dir)
    assert res.returncode == 0, res.stdout + res.stderr
    detail = check_map(run_dir)["tests not weakened"]["detail"]
    assert line in detail


def test_ts_test_delete_fails_and_rename_passes(tmp_path: Path) -> None:
    repo, run_dir = init_repo(tmp_path)
    git(repo, "switch", "main")
    commit_change(repo, "tests/example.test.ts", "test('a', () => {});\ntest('b', () => {});\n", "base ts tests")
    git(repo, "switch", "-C", "work", "main")
    commit_change(repo, "tests/example.test.ts", "test('renamed', () => {});\n", "delete ts test")
    assert run_checks(repo, run_dir).returncode == 1
    repo2, run_dir2 = init_repo(tmp_path / "rename")
    git(repo2, "switch", "main")
    commit_change(repo2, "tests/example.test.ts", "test('a', () => {});\n", "base ts test")
    git(repo2, "switch", "-C", "work", "main")
    commit_change(repo2, "tests/example.test.ts", "test('renamed', () => {});\n", "rename ts test")
    assert run_checks(repo2, run_dir2).returncode == 0


def test_skip_review_limit_reports_more(tmp_path: Path) -> None:
    repo, run_dir = init_repo(tmp_path)
    body = "\n".join(f"pytest.skip('x{i}')" for i in range(15)) + "\ndef test_ok():\n    assert True\n"
    commit_change(repo, "tests/test_app.py", body)
    res = run_checks(repo, run_dir)
    assert res.returncode == 0, res.stdout + res.stderr
    assert "+3 more" in check_map(run_dir)["tests not weakened"]["detail"]
