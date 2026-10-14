"""OPS-086-02 acceptance: code identity from RELEASE.json when the root is a deployed release.

Written by Claude from docs/requirements/OPS-086_Daily_Release_Bundle.md section 4; pi implements
the code and does not edit this file. Every release here is a temporary directory built by the
helpers below, never the real repository. production_effect=none; broker_action=none.
"""

from __future__ import annotations

import ast
import functools
import hashlib
import importlib.util
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

import ai_trading_system.release_identity as release_identity
import ai_trading_system.run_artifacts as run_artifacts
import ai_trading_system.shadow.lineage as lineage
import ai_trading_system.trading_engine.portfolio_candidate_review as candidate_review
from ai_trading_system import ops_daily
from ai_trading_system.core import provenance
from ai_trading_system.platform.operations.runtime_checkout import (
    RuntimeCheckoutError,
    inspect_runtime_checkout,
)
from ai_trading_system.release_identity import (
    ReleaseManifestError,
    find_release_root,
    is_release_root,
    read_release_manifest,
    release_mismatches,
)

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SCRIPTS = ROOT / "scripts" / "ops"

COMMIT = "62502d10c" + "1" * 31
TREE = "51798e4f" + "2" * 32
ORIGIN_MAIN = "3" * 40
RELEASE_ID = "20261014-62502d10c"
FILES = {
    "config/a.yaml": b"a: 1\n",
    "docs/note.md": b"# note\n",
    "pyproject.toml": b"[project]\nname = 'x'\n",
    "scripts/ops/run.ps1": b"Write-Output 'x'\r\n",
    "src/pkg/__init__.py": b"",
    "src/pkg/mod.py": b"VALUE = 1\n",
}
CHANGED_NOTE = b"# changed\n"
CHANGED_MOD = b"VALUE = 2\n"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _manifest(content: dict[str, bytes], **overrides: object) -> dict[str, object]:
    entries = [
        {"path": path, "sha256": _sha256(data), "size": len(data)}
        for path, data in sorted(content.items())
    ]
    lines = "".join(f"{item['sha256']}  {item['path']}\n" for item in entries)
    payload: dict[str, object] = {
        "schema_version": "aits_release.v1",
        "release_id": RELEASE_ID,
        "source_commit": COMMIT,
        "source_tree": TREE,
        "origin_main_commit": ORIGIN_MAIN,
        "built_at": "2026-10-14T00:00:00Z",
        "manifest_rules_sha256": "4" * 64,
        "dependencies_sha256": _sha256(content.get("pyproject.toml", b"")),
        "files": entries,
        "files_sha256": _sha256(lines.encode("utf-8")),
    }
    payload.update(overrides)
    return payload


def _release(root: Path, content: dict[str, bytes] = FILES, **overrides: object) -> Path:
    for path, data in content.items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    (root / "RELEASE.json").write_text(
        json.dumps(_manifest(content, **overrides), indent=2), encoding="utf-8"
    )
    return root


def _git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@example.invalid",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@example.invalid",
        },
    )
    return completed.stdout.strip()


def _git_checkout_with_a_release_file(tmp_path: Path) -> tuple[Path, str]:
    """A git checkout that also carries a valid RELEASE.json naming another commit."""

    root = tmp_path / "checkout"
    root.mkdir()
    _git(root, "init", "-q", "-b", "main")
    _git(root, "config", "core.autocrlf", "false")
    _release(root)
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "init")
    head = _git(root, "rev-parse", "HEAD")
    _git(root, "update-ref", "refs/remotes/origin/main", head)
    return root, head


# --- release_identity -------------------------------------------------------------------------


def test_a_valid_release_is_read_back_field_by_field(tmp_path: Path) -> None:
    root = _release(tmp_path / "release")

    manifest = read_release_manifest(root)

    assert issubclass(ReleaseManifestError, ValueError)
    assert is_release_root(root)
    assert manifest.release_id == RELEASE_ID
    assert manifest.source_commit == COMMIT
    assert manifest.source_tree == TREE
    assert manifest.origin_main_commit == ORIGIN_MAIN
    assert [(item.path, item.sha256, item.size) for item in manifest.files] == [
        (path, _sha256(data), len(data)) for path, data in sorted(FILES.items())
    ]
    assert find_release_root(root / "src" / "pkg" / "mod.py") == root
    assert find_release_root(root / "src" / "pkg") == root
    assert find_release_root(root) == root
    assert find_release_root(tmp_path) is None


@pytest.mark.parametrize("git_kind", ["directory", "file"])
def test_a_root_with_git_is_a_checkout_even_with_a_release_file(
    tmp_path: Path, git_kind: str
) -> None:
    root = _release(tmp_path / "release")
    if git_kind == "directory":
        (root / ".git").mkdir()
    else:
        (root / ".git").write_text("gitdir: elsewhere\n", encoding="utf-8")

    assert not is_release_root(root)
    assert find_release_root(root / "src" / "pkg" / "mod.py") is None


def _bad(payload: dict[str, object], key: str, value: object) -> dict[str, object]:
    return {**payload, key: value}


def _entries(**change: object) -> list[dict[str, object]]:
    entries = list(_manifest(FILES)["files"])  # type: ignore[arg-type]
    first = {**entries[0], **change}
    return [first, *entries[1:]]


def _relisted(entries: list[dict[str, object]]) -> dict[str, object]:
    lines = "".join(f"{item['sha256']}  {item['path']}\n" for item in entries)
    return {"files": entries, "files_sha256": _sha256(lines.encode("utf-8"))}


INVALID = {
    "schema": {"schema_version": "aits_release.v0"},
    "commit_upper": {"source_commit": COMMIT.upper()},
    "commit_short": {"source_commit": COMMIT[:39]},
    "tree_bad": {"source_tree": "x" * 40},
    "origin_bad": {"origin_main_commit": None},
    "release_id_format": {"release_id": "2026-10-14-62502d10c"},
    "release_id_other_commit": {"release_id": "20261014-62502d10d"},
    "files_empty": _relisted([]),
    "files_digest": {"files_sha256": "0" * 64},
    "absolute": _relisted(_entries(path="/etc/passwd")),
    "drive": _relisted(_entries(path="C:/Windows/x.py")),
    "dotdot": _relisted(_entries(path="config/../../x.py")),
    "backslash": _relisted(_entries(path="config\\a.yaml")),
    "dot_segment": _relisted(_entries(path="./config/a.yaml")),
    "self": _relisted(_entries(path="RELEASE.json")),
    "state_data": _relisted(_entries(path="data/a.yaml")),
    "state_outputs": _relisted(_entries(path="outputs/a.yaml")),
    "sha_short": _relisted(_entries(sha256="ab" * 31)),
    "size_negative": _relisted(_entries(size=-1)),
    "size_bool": _relisted(_entries(size=True)),
    "size_text": _relisted(_entries(size="5")),
    "unsorted": _relisted(list(reversed(_manifest(FILES)["files"]))),  # type: ignore[arg-type]
    "duplicate": _relisted([_entries()[0], *_entries()]),
    "missing_files": {"files": None},
}


@pytest.mark.parametrize("case", sorted(INVALID))
def test_an_invalid_manifest_is_refused(tmp_path: Path, case: str) -> None:
    root = _release(tmp_path / "release", **INVALID[case])

    with pytest.raises(ReleaseManifestError):
        read_release_manifest(root)


@pytest.mark.parametrize("text", ["", "{", "[]", '"aits_release.v1"'])
def test_an_unreadable_manifest_is_refused(tmp_path: Path, text: str) -> None:
    root = _release(tmp_path / "release")
    (root / "RELEASE.json").write_text(text, encoding="utf-8")

    with pytest.raises(ReleaseManifestError):
        read_release_manifest(root)


def test_a_missing_manifest_is_refused(tmp_path: Path) -> None:
    root = tmp_path / "empty"
    root.mkdir()

    assert not is_release_root(root)
    with pytest.raises(ReleaseManifestError):
        read_release_manifest(root)


def test_mismatches_list_missing_and_changed_files_sorted_and_ignore_extra_files(
    tmp_path: Path,
) -> None:
    root = _release(tmp_path / "release")
    assert release_mismatches(root) == ()

    (root / "src" / "pkg" / "mod.py").write_bytes(CHANGED_MOD)
    (root / "config" / "a.yaml").unlink()
    (root / "docs" / "note.md").write_bytes(CHANGED_NOTE)
    (root / "src" / "pkg" / "extra.py").write_bytes(b"EXTRA = 1\n")

    note = f"changed docs/note.md {_sha256(CHANGED_NOTE)}"
    mod = f"changed src/pkg/mod.py {_sha256(CHANGED_MOD)}"
    assert release_mismatches(root) == (note, mod, "missing config/a.yaml")
    assert release_mismatches(root, prefixes=("src", "config")) == (mod, "missing config/a.yaml")
    assert release_mismatches(root, read_release_manifest(root), prefixes=("docs",)) == (note,)


def _verify(root: Path, capsys: pytest.CaptureFixture[str]) -> tuple[int, list[str]]:
    code = release_identity.main(["verify", "--root", str(root)])
    return code, capsys.readouterr().out.splitlines()


def test_verify_prints_the_identity_and_exits_0_when_the_release_matches(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = _release(tmp_path / "release")

    code, lines = _verify(root, capsys)

    assert code == 0
    assert lines == [f"release_id={RELEASE_ID}", f"source_commit={COMMIT}", "mismatches=0"]


def test_verify_exits_1_and_lists_at_most_20_mismatches(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    files = {f"src/pkg/m{index:02d}.py": b"x\n" for index in range(25)}
    root = _release(tmp_path / "release", files)
    for path in files:
        (root / path).unlink()

    code, lines = _verify(root, capsys)

    assert code == 1
    assert lines[:3] == [f"release_id={RELEASE_ID}", f"source_commit={COMMIT}", "mismatches=25"]
    assert lines[3:] == [f"missing src/pkg/m{index:02d}.py" for index in range(20)]


def test_verify_exits_2_without_a_valid_release(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    with_git = _release(tmp_path / "with_git")
    (with_git / ".git").mkdir()
    invalid = _release(tmp_path / "invalid", files_sha256="0" * 64)

    for root in (empty, with_git, invalid):
        code, lines = _verify(root, capsys)
        assert code == 2, root
        assert not any(line.startswith("release_id=") for line in lines)


def test_verify_runs_as_a_module_with_only_the_standard_library_and_paths(
    tmp_path: Path,
) -> None:
    root = _release(tmp_path / "release")
    env = {**os.environ, "PYTHONPATH": str(SRC)}

    completed = subprocess.run(
        [sys.executable, "-m", "ai_trading_system.release_identity", "verify", "--root", str(root)],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    show = "import sys; print(chr(10).join(sorted(sys.modules)))"
    baseline, probe = (
        subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            env=env,
            check=True,
        )
        for code in (show, "import ai_trading_system.release_identity; " + show)
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.splitlines()[0] == f"release_id={RELEASE_ID}"
    added = set(probe.stdout.split()) - set(baseline.stdout.split())
    roots = {name.split(".")[0] for name in added}
    assert roots - set(sys.stdlib_module_names) - {"ai_trading_system"} == set()
    assert {name for name in added if name.startswith("ai_trading_system")} <= {
        "ai_trading_system",
        "ai_trading_system.paths",
        "ai_trading_system.release_identity",
    }


# --- the 7 sites in a release -----------------------------------------------------------------


def test_provenance_in_a_release_uses_the_manifest_and_code_paths_only(tmp_path: Path) -> None:
    root = _release(tmp_path / "release")
    anchor = root / "src" / "pkg" / "mod.py"

    assert provenance.git_provenance(anchor) == {
        "commit": COMMIT,
        "branch": f"release:{RELEASE_ID}",
        "code_modified": False,
    }
    (root / "docs" / "note.md").write_bytes(b"# edited\n")
    assert provenance.git_provenance(anchor)["code_modified"] is False
    (root / "config" / "a.yaml").write_bytes(b"a: 2\n")
    assert provenance.git_provenance(root / "src")["code_modified"] is True


def test_run_artifacts_commit_in_a_release_is_the_source_commit(tmp_path: Path) -> None:
    root = _release(tmp_path / "release")

    assert run_artifacts._git_commit(root) == COMMIT


def test_daily_run_metadata_in_a_release(tmp_path: Path) -> None:
    root = _release(tmp_path / "release")

    clean = dict(ops_daily._git_metadata(root))
    assert clean == {
        "available": True,
        "commit": COMMIT,
        "dirty": False,
        "status_sha256": _sha256(b""),
        "dirty_diff_sha256": _sha256(b""),
        "release_id": RELEASE_ID,
    }

    (root / "src" / "pkg" / "mod.py").write_bytes(b"VALUE = 3\n")
    (root / "docs" / "note.md").unlink()
    joined = "\n".join(release_mismatches(root)).encode("utf-8")
    dirty = dict(ops_daily._git_metadata(root))
    assert dirty["dirty"] is True
    assert dirty["commit"] == COMMIT
    assert dirty["status_sha256"] == dirty["dirty_diff_sha256"] == _sha256(joined)


def test_shadow_lineage_in_a_release(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _release(tmp_path / "release")
    monkeypatch.setattr(lineage, "PROJECT_ROOT", root)

    assert lineage.git_commit_sha() == COMMIT
    assert lineage.git_worktree_dirty() is False
    (root / "pyproject.toml").write_bytes(b"[project]\nname = 'y'\n")
    assert lineage.git_worktree_dirty() is True


def test_candidate_review_path_check_in_a_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _release(tmp_path / "release")
    monkeypatch.setattr(candidate_review, "PROJECT_ROOT", root)
    tracked = root / "config" / "a.yaml"
    unlisted = root / "config" / "local.yaml"
    unlisted.write_bytes(b"local: true\n")

    assert candidate_review._git_path_modified(tracked) is False
    assert candidate_review._git_path_modified(unlisted) is True
    assert candidate_review._git_path_modified(root / "config" / "absent.yaml") is False
    tracked.write_bytes(b"a: 9\n")
    assert candidate_review._git_path_modified(tracked) is True


def test_runtime_checkout_state_in_a_release(tmp_path: Path) -> None:
    root = _release(tmp_path / "release")

    state = inspect_runtime_checkout(root)
    assert state.root == root.resolve()
    assert state.head_commit == COMMIT
    assert state.origin_main_commit == ORIGIN_MAIN
    assert state.dirty_entries == ()
    assert state.head_on_origin_main is True
    assert state.release_id == RELEASE_ID
    assert state.clean

    (root / "scripts" / "ops" / "run.ps1").write_bytes(b"Write-Output 'y'\r\n")
    changed = inspect_runtime_checkout(root)
    assert changed.dirty_entries == release_mismatches(root)
    assert len(changed.dirty_entries) == 1
    assert not changed.clean


def test_runtime_checkout_refuses_an_invalid_release(tmp_path: Path) -> None:
    root = _release(tmp_path / "release", release_id="20261014-000000000")

    with pytest.raises(RuntimeCheckoutError) as raised:
        inspect_runtime_checkout(root)

    assert raised.value.code == "RUNTIME_RELEASE_INVALID"


def test_invalid_release_never_reports_the_manifest_commit(tmp_path: Path) -> None:
    root = _release(tmp_path / "release", files_sha256="0" * 64)

    assert provenance.git_provenance(root)["commit"] != COMMIT
    assert run_artifacts._git_commit(root) is None
    metadata = ops_daily._git_metadata(root)
    assert metadata["available"] is False
    assert metadata["commit"] is None


# --- a git checkout keeps its git answers ------------------------------------------------------


def test_a_git_checkout_reports_git_even_with_a_release_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, head = _git_checkout_with_a_release_file(tmp_path)

    assert provenance.git_provenance(root / "src" / "pkg")["commit"] == head
    assert provenance.git_provenance(root)["branch"] == "main"
    assert provenance.git_provenance(root)["code_modified"] is False
    assert run_artifacts._git_commit(root) == head

    metadata = dict(ops_daily._git_metadata(root))
    assert set(metadata) == {"available", "commit", "dirty", "status_sha256", "dirty_diff_sha256"}
    assert metadata["commit"] == head
    assert metadata["dirty"] is False

    state = inspect_runtime_checkout(root)
    assert state.head_commit == head
    assert state.origin_main_commit == head
    assert state.release_id is None

    monkeypatch.setattr(lineage, "PROJECT_ROOT", root)
    monkeypatch.setattr(candidate_review, "PROJECT_ROOT", root)
    assert lineage.git_commit_sha() == head
    assert lineage.git_worktree_dirty() is False

    (root / "config" / "a.yaml").write_bytes(b"a: 5\n")
    assert provenance.git_provenance(root)["code_modified"] is True
    assert ops_daily._git_metadata(root)["dirty"] is True
    assert inspect_runtime_checkout(root).dirty_entries == (" M config/a.yaml",)
    assert lineage.git_worktree_dirty() is True
    assert candidate_review._git_path_modified(root / "config" / "a.yaml") is True
    assert candidate_review._git_path_modified(root / "src" / "pkg" / "mod.py") is False


# --- scheduler summary reads the state root ----------------------------------------------------

AS_OF = "2026-10-09"
RUN_ID = "daily_ops_run:2026-10-09:20261011T003000Z"
RECEIPT = "outputs/data_quality/executions/receipts/dq_receipt_x.json"


@functools.cache
def _summary_module() -> ModuleType:
    path = SCRIPTS / "daily_scheduler_summary.py"
    spec = importlib.util.spec_from_file_location("ops086_daily_scheduler_summary", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_json(path: Path, payload: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _daily_state(state_root: Path, *, status: str, receipt_status: str) -> None:
    module = _summary_module()
    states = state_root / "outputs" / "run_control" / "daily" / "states"
    key = "operations_run_0123456789abcdef01234567"
    _write_json(
        states / f"{key}.json",
        {
            "as_of": AS_OF,
            "attempt": 1,
            "blocker_codes": [],
            "idempotency_key": key,
            "run_id": RUN_ID,
            "status": status,
            "updated_at": "2026-10-11T00:40:00+00:00",
        },
    )
    _write_json(states / f"{key}.run_ledger.json", {"as_of": AS_OF, "entries": []})
    _write_json(
        state_root
        / "outputs"
        / "runs"
        / "daily"
        / "20261011T003000Z"
        / f"as_of_{AS_OF}__{module.safe_run_id(RUN_ID)}"
        / "metadata"
        / f"daily_ops_run_metadata_{AS_OF}.json",
        {"status": status, "step_results": [], "input_visibility_issues": []},
    )
    _write_json(
        state_root
        / "outputs"
        / "daily_input_capture"
        / AS_OF
        / f"daily_input_capture_manifest_{AS_OF}.json",
        {"status": "COMPLETE", "component_results": []},
    )
    _write_json(
        state_root
        / "outputs"
        / "data_quality"
        / "executions"
        / "discovery"
        / "daily_default"
        / AS_OF
        / "current.json",
        {"receipt_id": f"receipt-{status}", "receipt_path": RECEIPT},
    )
    _write_json(
        state_root / RECEIPT,
        {"data_quality_evidence": {"status": receipt_status, "passed": receipt_status == "PASS"}},
    )


def _run_summary(tmp_path: Path, extra: list[str]) -> dict[str, object]:
    runtime = tmp_path / "release"
    log = tmp_path / "run.log"
    log.write_text(f"每日运行执行：PASS\nRun ID：{RUN_ID}\n", encoding="utf-8")
    stem = tmp_path / "scheduler" / "2026-10-11_PRIMARY"
    code = _summary_module().main(
        [
            "--runtime-root",
            str(runtime),
            "--window",
            "PRIMARY",
            "--output-stem",
            str(stem),
            "--log",
            str(log),
            "--exit-code",
            "0",
            *extra,
        ]
    )
    assert code == 0
    return json.loads((stem.parent / (stem.name + ".summary.json")).read_text(encoding="utf-8"))


def test_summary_reads_every_output_from_the_state_root(tmp_path: Path) -> None:
    runtime = tmp_path / "release"
    state_root = tmp_path / "state"
    # A decoy under the release root: none of it may be read.
    _daily_state(runtime, status="BLOCKED", receipt_status="FAIL")
    _daily_state(state_root, status="PASS", receipt_status="PASS")

    result = _run_summary(tmp_path, ["--state-root", str(state_root), "--release-id", RELEASE_ID])

    assert result["state_root"] == str(state_root)
    assert result["release_id"] == RELEASE_ID
    assert result["runtime_root"] == str(runtime)
    run_state = result["run_state"]
    assert isinstance(run_state, dict) and run_state["status"] == "PASS"
    for key in ("run_ledger_path", "run_metadata_path", "capture_manifest_path"):
        assert Path(str(result[key])).is_relative_to(state_root), key
    assert Path(str(run_state["path"])).is_relative_to(state_root)
    quality = result["data_quality"]
    assert isinstance(quality, dict)
    assert Path(str(quality["discovery_pointer"])).is_relative_to(state_root)
    assert quality["receipt_id"] == "receipt-PASS"
    assert quality["receipt_status"] == "PASS"
    assert quality["receipt_passed"] is True


def test_summary_without_a_state_root_reads_the_runtime_root(tmp_path: Path) -> None:
    runtime = tmp_path / "release"
    _daily_state(runtime, status="PASS", receipt_status="PASS")

    result = _run_summary(tmp_path, [])

    assert result["state_root"] == str(runtime)
    assert result["release_id"] is None
    run_state = result["run_state"]
    assert isinstance(run_state, dict) and run_state["status"] == "PASS"
    quality = result["data_quality"]
    assert isinstance(quality, dict) and quality["receipt_status"] == "PASS"


def test_summary_resolves_a_receipt_outside_the_state_directories_from_the_runtime_root(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "release"
    state_root = tmp_path / "state"
    _daily_state(state_root, status="PASS", receipt_status="PASS")
    pointer = (
        state_root
        / "outputs"
        / "data_quality"
        / "executions"
        / "discovery"
        / "daily_default"
        / AS_OF
        / "current.json"
    )
    _write_json(pointer, {"receipt_id": "fixture", "receipt_path": "inputs/dq/receipt.json"})
    _write_json(
        runtime / "inputs" / "dq" / "receipt.json",
        {"data_quality_evidence": {"status": "FIXTURE", "passed": False}},
    )

    result = _run_summary(tmp_path, ["--state-root", str(state_root)])

    quality = result["data_quality"]
    assert isinstance(quality, dict) and quality["receipt_status"] == "FIXTURE"


def test_summary_script_imports_only_the_standard_library() -> None:
    tree = ast.parse((SCRIPTS / "daily_scheduler_summary.py").read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            imported.add(node.module.split(".")[0])

    assert imported - set(sys.stdlib_module_names) == set()


# --- wrapper script text -------------------------------------------------------------------------


def _code_lines(name: str) -> str:
    raw = (SCRIPTS / name).read_bytes()
    assert all(byte < 128 for byte in raw), f"{name} must stay ASCII for Windows PowerShell 5.1"
    text = re.sub(r"<#.*?#>", "", raw.decode("ascii"), flags=re.DOTALL)
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def test_wrapper_takes_its_state_root_from_the_environment_and_logs_under_it() -> None:
    code = _code_lines("daily_scheduler_run.ps1")

    assert "[Environment]::GetEnvironmentVariable('AITS_STATE_ROOT', 'Process')" in code
    assert "$StateRoot = $RuntimeRoot" in code
    assert "$OutputDir = Join-Path $StateRoot 'outputs\\run_control\\scheduler'" in code
    assert "Join-Path $RuntimeRoot 'outputs" not in code
    assert "'STATE_ROOT_INVALID'" in code


def test_wrapper_checks_git_only_in_a_checkout_and_the_release_otherwise() -> None:
    code = _code_lines("daily_scheduler_run.ps1")

    git_test = code.index("Test-Path -LiteralPath (Join-Path $RuntimeRoot '.git')")
    git_status = code.index("Invoke-Git @('status', '--porcelain=v1', '--untracked-files=normal')")
    release_test = code.index("Test-Path -LiteralPath $ReleaseManifest")
    missing = code.index("'RUNTIME_IDENTITY_MISSING'")
    assert git_test < git_status < release_test < missing
    assert "$ReleaseManifest = Join-Path $RuntimeRoot 'RELEASE.json'" in code
    git_calls = [match.start() for match in re.finditer(r"Invoke-Git @\(", code)]
    assert len(git_calls) == 4
    assert all(git_test < position < release_test for position in git_calls)

    release_block = code[release_test:missing]
    assert "'STATE_ROOT_NOT_SET'" in release_block
    assert "$StateRoot -eq $RuntimeRoot" in release_block
    assert "& $Python -I -m ai_trading_system.release_identity verify --root $RuntimeRoot" in code
    assert re.search(r"-eq 0\)\s*\{[^}]*'source_commit'", release_block)
    assert re.search(r"-eq 1\)\s*\{[^}]*'RUNTIME_RELEASE_MODIFIED'", release_block)
    assert re.search(
        r"\}\s*else\s*\{\s*\$precheckBlocker = 'RUNTIME_RELEASE_INVALID'", release_block
    )


def test_wrapper_passes_state_root_and_release_id_to_the_summary() -> None:
    code = _code_lines("daily_scheduler_run.ps1")

    summary_args = code[code.index("$summaryArgs = @(") :]
    assert "'--state-root', $StateRoot" in summary_args
    assert re.search(
        r"if \(\$releaseId\) \{ \$summaryArgs \+= @\('--release-id', \$releaseId\) \}", code
    )
