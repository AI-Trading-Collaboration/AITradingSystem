"""OPS-086-03 acceptance: build, deploy and run the daily release bundle.

Written by Claude from docs/requirements/OPS-086_Daily_Release_Bundle.md section 5; pi implements
the code and does not edit this file. Builds use temporary git repositories; deploys use a fake
command runner and never create a real venv or touch a scheduled task. The slow case builds this
commit's release into a temporary directory and plans a daily run from it.
production_effect=none; broker_action=none.
"""

from __future__ import annotations

import ast
import glob
import hashlib
import io
import json
import os
import re
import stat
import subprocess
import sys
import tarfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ai_trading_system.release_identity import (
    is_release_root,
    read_release_manifest,
    release_mismatches,
)
from tools.release import build_daily_release as build
from tools.release import deploy_daily_release as deploy

CODE_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = CODE_ROOT / "scripts" / "ops"
RULES = "config/release/daily_release.yaml"
NOW = datetime(2026, 10, 14, 3, 0, 0, tzinfo=UTC)
STAMP = "20261014T030000Z"

# --- build in a temporary git repository ---------------------------------------------------------

REGISTRY = b"""schema_version: 1
reports:
  - report_id: note
    scope: daily
    artifact_globs:
      - docs/research/*.md
      - outputs/x/*.json
  - report_id: lab
    scope: research
    artifact_globs:
      - research/_results/r/*.csv
      - docs/research/sub/*.md
  - report_id: unscoped
    artifact_globs:
      - docs/other.md
"""
TREE = {
    ".gitattributes": b"* text=auto eol=lf\n*.ps1 text eol=crlf\n",
    ".python-version": b"3.11\n",
    "README.md": b"# readme\n",
    "pyproject.toml": b"[project]\nname = 'ai-trading-system'\n",
    "config/a.yaml": b"a: 1\n",
    "config/report_registry.yaml": REGISTRY,
    "src/ai_trading_system/__init__.py": b"",
    "src/ai_trading_system/mod.py": b"VALUE = 1\r\n",
    "src/ai_trading_system/__pycache__/mod.cpython-311.pyc": b"\x00\x01",
    "scripts/ops/run_daily.ps1": b"Write-Output 'x'\n",
    "scripts/other/tool.py": b"",
    "tools/x.py": b"",
    "tests/test_x.py": b"",
    "inputs/trade_theses/a.yaml": b"x: 1\n",
    "docs/artifact_catalog.md": b"# catalog\n",
    "docs/schema/historical_gap_recovery.v1.schema.json": b"{}\n",
    "docs/other.md": b"# other\n",
    "docs/research/note_1.md": b"# note\n",
    "docs/research/.note_hidden.md": b"# hidden\n",
    "docs/research/sub/note_2.md": b"# deeper\n",
    "research/_results/r/a.csv": b"a\n",
    "research/q1/prereg.yaml": b"q: 1\n",
    "data/.gitkeep": b"",
    "outputs/.gitkeep": b"",
    "outputs/x/a.json": b"{}\n",
}
EXPECTED = [
    ".python-version",
    "README.md",
    "config/a.yaml",
    RULES,
    "config/report_registry.yaml",
    "docs/artifact_catalog.md",
    "docs/research/note_1.md",
    "docs/schema/historical_gap_recovery.v1.schema.json",
    "inputs/trade_theses/a.yaml",
    "pyproject.toml",
    "scripts/ops/run_daily.ps1",
    "src/ai_trading_system/__init__.py",
    "src/ai_trading_system/mod.py",
]


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _git(root: Path, *args: str, date: str = "2026-10-14T08:30:00+09:00") -> str:
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
            "GIT_AUTHOR_DATE": date,
            "GIT_COMMITTER_DATE": date,
        },
    )
    return completed.stdout.strip()


def _dev_repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "dev"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "core.autocrlf", "false")
    files = {**TREE, RULES: (CODE_ROOT / RULES).read_bytes()}
    for path, data in files.items():
        target = repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    _git(repo, "add", "-A", "-f")
    _git(repo, "commit", "-q", "-m", "release source")
    head = _git(repo, "rev-parse", "HEAD")
    _git(repo, "update-ref", "refs/remotes/origin/main", head)
    return repo, head


def test_release_file_list_follows_the_rules_and_the_registry(tmp_path: Path) -> None:
    repo, head = _dev_repo(tmp_path)

    assert build.release_file_list(repo, head) == EXPECTED
    assert build.release_file_list(repo, "HEAD") == EXPECTED


def test_build_writes_a_bundle_of_the_commit_with_its_release_manifest(
    tmp_path: Path,
) -> None:
    repo, head = _dev_repo(tmp_path)
    (repo / "config" / "a.yaml").write_bytes(b"a: uncommitted\n")
    out = tmp_path / "bundles"

    assert build.main(["--commit", head, "--out-dir", str(out), "--repo", str(repo)]) == 0

    release_id = f"20261013-{head[:9]}"
    bundle = out / f"{release_id}.zip"
    sidecar = out / f"{release_id}.zip.sha256"
    assert sorted(item.name for item in out.iterdir()) == [bundle.name, sidecar.name]
    assert sidecar.read_bytes() == f"{_sha256(bundle.read_bytes())}  {bundle.name}\n".encode()
    with zipfile.ZipFile(bundle) as archive:
        names = sorted(info.filename for info in archive.infolist() if not info.is_dir())
        contents = {name: archive.read(name) for name in names}
    assert names == sorted([*EXPECTED, "RELEASE.json"])
    assert contents["config/a.yaml"] == b"a: 1\n"
    assert contents["src/ai_trading_system/mod.py"] == b"VALUE = 1\n"
    assert contents["scripts/ops/run_daily.ps1"] == b"Write-Output 'x'\r\n"

    manifest = json.loads(contents["RELEASE.json"])
    assert manifest["schema_version"] == "aits_release.v1"
    assert manifest["release_id"] == release_id
    assert manifest["source_commit"] == head
    assert manifest["source_tree"] == _git(repo, "rev-parse", "HEAD^{tree}")
    assert manifest["origin_main_commit"] == head
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", manifest["built_at"])
    assert manifest["manifest_rules_sha256"] == _sha256((CODE_ROOT / RULES).read_bytes())
    assert manifest["dependencies_sha256"] == _sha256(TREE["pyproject.toml"])
    assert manifest["files"] == [
        {"path": path, "sha256": _sha256(contents[path]), "size": len(contents[path])}
        for path in EXPECTED
    ]

    extracted = tmp_path / "extracted"
    with zipfile.ZipFile(bundle) as archive:
        archive.extractall(extracted)
    assert is_release_root(extracted)
    assert read_release_manifest(extracted).release_id == release_id
    assert release_mismatches(extracted) == ()


def test_build_refuses_a_commit_not_on_origin_main(tmp_path: Path) -> None:
    repo, _ = _dev_repo(tmp_path)
    (repo / "config" / "a.yaml").write_bytes(b"a: 2\n")
    _git(repo, "commit", "-q", "-am", "local only")
    out = tmp_path / "bundles"

    assert build.main(["--commit", "HEAD", "--out-dir", str(out), "--repo", str(repo)]) == 2
    assert not out.exists() or list(out.iterdir()) == []


def test_build_never_overwrites_a_bundle(tmp_path: Path) -> None:
    repo, head = _dev_repo(tmp_path)
    out = tmp_path / "bundles"
    assert build.main(["--commit", head, "--out-dir", str(out), "--repo", str(repo)]) == 0
    before = {item.name: item.read_bytes() for item in out.iterdir()}

    assert build.main(["--commit", head, "--out-dir", str(out), "--repo", str(repo)]) == 2
    assert {item.name: item.read_bytes() for item in out.iterdir()} == before


# --- the real release -----------------------------------------------------------------------------


def _tracked(*paths: str) -> list[str]:
    completed = subprocess.run(
        [
            "git",
            "-C",
            str(CODE_ROOT),
            "ls-tree",
            "-r",
            "--name-only",
            "HEAD",
            "--",
            *paths,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.split()


def test_the_real_release_holds_the_daily_code_and_nothing_from_development() -> None:
    files = build.release_file_list(CODE_ROOT, "HEAD")
    selected = set(files)

    assert files == sorted(selected)
    for required in (
        "pyproject.toml",
        ".python-version",
        "src/ai_trading_system/cli.py",
        "src/ai_trading_system/paths.py",
        "src/ai_trading_system/release_identity.py",
        RULES,
        "config/report_registry.yaml",
        "config/data_sources.yaml",
        "scripts/ops/daily_scheduler_run.ps1",
        "scripts/ops/daily_scheduler_summary.py",
        "scripts/ops/run_daily.ps1",
        "docs/artifact_catalog.md",
    ):
        assert required in selected, required
    for prefix in ("src/", "config/", "scripts/ops/", "inputs/"):
        tracked = [path for path in _tracked(prefix) if "__pycache__" not in path.split("/")]
        assert tracked and set(tracked) <= selected, prefix
    assert len([path for path in files if path.startswith("inputs/trade_theses/")]) == 6
    for path in files:
        top = path.split("/", 1)[0]
        assert top not in {
            "tests",
            "tools",
            "data",
            "outputs",
            "artifacts",
            ".github",
            ".pi",
        }, path
        assert not path.startswith("scripts/agent/"), path
        assert "__pycache__" not in path.split("/") and not path.endswith(".pyc"), path
    assert "AGENTS.md" not in selected


def test_the_real_release_holds_the_tracked_files_of_daily_registry_entries_only() -> None:
    import yaml

    registry = yaml.safe_load((CODE_ROOT / "config" / "report_registry.yaml").read_text("utf-8"))
    tracked = set(_tracked())
    rules = yaml.safe_load((CODE_ROOT / RULES).read_text("utf-8"))
    included = [build._glob_regex(pattern) for pattern in rules["include"]]

    def named(scope: str) -> set[str]:
        found = set()
        for entry in registry["reports"]:
            if entry.get("scope") != scope:
                continue
            for pattern in entry.get("artifact_globs", []):
                for match in glob.glob(str(CODE_ROOT / pattern)):
                    rel = Path(match).relative_to(CODE_ROOT).as_posix()
                    if rel in tracked and rel.split("/", 1)[0] not in {
                        "data",
                        "outputs",
                        "artifacts",
                    }:
                        found.add(rel)
        return found

    release = set(build.release_file_list(CODE_ROOT, "HEAD"))
    only_research = {
        path
        for path in named("research") - named("daily")
        if not any(pattern.fullmatch(path) for pattern in included)
    }

    assert named("daily") <= release
    assert len(only_research) >= 20
    assert not only_research & release


# --- deploy with a fake command runner ------------------------------------------------------------

OLD_FREEZE = (
    "# Editable install with no version control (ai-trading-system==0.1.0)\n"
    "-e d:\\work\\aitradingsystem_ops_runtime\n"
    "numpy==2.1.0\n"
    "pandas==2.2.3\n"
    "ai-trading-system @ file:///d:/work/x\n"
)
NEW_FREEZE = "-e c:\\daily\\releases\\x\nnumpy==2.1.0\npandas==2.2.3\npyyaml==6.0.2\n"


def _manifest(commit: str, files: dict[str, bytes]) -> dict[str, object]:
    entries = [
        {"path": path, "sha256": _sha256(data), "size": len(data)}
        for path, data in sorted(files.items())
    ]
    lines = "".join(f"{item['sha256']}  {item['path']}\n" for item in entries)
    return {
        "schema_version": "aits_release.v1",
        "release_id": f"20261014-{commit[:9]}",
        "source_commit": commit,
        "source_tree": "e" * 40,
        "origin_main_commit": commit,
        "built_at": "2026-10-14T00:00:00Z",
        "manifest_rules_sha256": "d" * 64,
        "dependencies_sha256": _sha256(files["pyproject.toml"]),
        "files": entries,
        "files_sha256": _sha256(lines.encode("utf-8")),
    }


def _bundle(
    out: Path,
    letter: str,
    *,
    content: dict[str, bytes] | None = None,
    extra: dict[str, bytes] | None = None,
) -> tuple[Path, str]:
    """A bundle whose manifest lists content; the zip holds content plus extra."""

    commit = letter * 40
    files = content or {
        "pyproject.toml": b"[project]\nname = 'ai-trading-system'\n",
        "scripts/ops/run_daily.ps1": f"Write-Output '{letter}'\r\n".encode(),
        "scripts/ops/daily_scheduler_run.ps1": b"exit 0\r\n",
        "src/ai_trading_system/__init__.py": b"",
        "src/ai_trading_system/release_identity.py": b"# fake\n",
    }
    manifest = _manifest(commit, files)
    out.mkdir(parents=True, exist_ok=True)
    bundle = out / f"{manifest['release_id']}.zip"
    with zipfile.ZipFile(bundle, "w") as archive:
        for path, data in {**files, **(extra or {})}.items():
            archive.writestr(path, data)
        archive.writestr("RELEASE.json", json.dumps(manifest))
    sidecar = bundle.with_name(bundle.name + ".sha256")
    sidecar.write_text(f"{_sha256(bundle.read_bytes())}  {bundle.name}\n", encoding="utf-8")
    return bundle, str(manifest["release_id"])


class FakeRunner:
    def __init__(self, tmp_path: Path) -> None:
        self.old_python = tmp_path / "old_runtime" / "python.exe"
        self.base_python = tmp_path / "base" / "python.exe"
        self.calls: list[dict[str, object]] = []
        self.task_status = "Ready"
        self.fail: str | None = None
        self.read_only_at_plan: bool | None = None

    @staticmethod
    def kind(argv: list[str]) -> str:
        if argv[0] == "schtasks":
            return "schtasks"
        if argv[1:3] == ["-m", "venv"]:
            return "venv"
        if argv[1:4] == ["-m", "pip", "install"]:
            return "install"
        if argv[1:4] == ["-m", "pip", "freeze"]:
            return "freeze"
        if "ai_trading_system.release_identity" in argv:
            return "verify"
        if argv[1:3] == ["ops", "daily-plan"]:
            return "plan"
        raise AssertionError(f"unexpected command {argv}")

    def __call__(
        self,
        argv: list[str],
        *,
        env: dict[str, str] | None = None,
        cwd: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        argv = [str(item) for item in argv]
        kind = self.kind(argv)
        self.calls.append({"kind": kind, "argv": argv, "env": env, "cwd": cwd})
        if kind == self.fail:
            return subprocess.CompletedProcess(argv, 1, "", f"{kind} failed")
        if kind == "schtasks":
            text = f"TaskName:      \\{argv[3]}\nStatus:        {self.task_status}\n"
            return subprocess.CompletedProcess(argv, 0, text, "")
        if kind == "venv":
            python = deploy.venv_python(Path(argv[-1]).parent)
            python.parent.mkdir(parents=True, exist_ok=True)
            python.write_bytes(b"")
        if kind == "freeze":
            text = OLD_FREEZE if argv[0] == str(self.old_python) else NEW_FREEZE
            return subprocess.CompletedProcess(argv, 0, text, "")
        if kind == "plan":
            release = Path(argv[0]).parents[2]
            mode = stat.S_IMODE((release / "RELEASE.json").stat().st_mode)
            self.read_only_at_plan = mode & 0o222 == 0
        return subprocess.CompletedProcess(argv, 0, "", "")

    def kinds(self) -> list[str]:
        return [str(call["kind"]) for call in self.calls]

    def one(self, kind: str) -> dict[str, object]:
        found = [call for call in self.calls if call["kind"] == kind]
        assert len(found) == 1, (kind, self.kinds())
        return found[0]


def _deploy(runner: FakeRunner, *args: object) -> int:
    return deploy.main([str(item) for item in args], run=runner, now=lambda: NOW)


def _later(runner: FakeRunner, bundle: Path, daily: Path) -> int:
    """A deploy after the first: state root and constraints come from current.json."""

    return _deploy(
        runner, "--bundle", bundle, "--daily-root", daily, "--python", runner.base_python
    )


def _first(tmp_path: Path, runner: FakeRunner, bundle: Path, *extra: str) -> tuple[int, Path, Path]:
    daily = tmp_path / "daily"
    state = tmp_path / "state"
    code = _deploy(
        runner,
        "--bundle",
        str(bundle),
        "--daily-root",
        str(daily),
        "--state-root",
        str(state),
        "--constraints-from",
        str(runner.old_python),
        "--python",
        str(runner.base_python),
        *extra,
    )
    return code, daily, state


def _current(daily: Path) -> dict[str, object]:
    return json.loads((daily / "current.json").read_text(encoding="utf-8"))


def _log(daily: Path) -> list[dict[str, object]]:
    return [
        json.loads(line) for line in (daily / "deploy_log.jsonl").read_text("utf-8").splitlines()
    ]


def _constraints(runner: FakeRunner) -> str:
    argv = runner.one("install")["argv"]
    assert isinstance(argv, list) and "-c" in argv
    return Path(argv[argv.index("-c") + 1]).read_text(encoding="utf-8")


def _read_only(path: Path) -> bool:
    return stat.S_IMODE(path.stat().st_mode) & 0o222 == 0


def _inside(path: Path, root: Path) -> bool:
    return path.resolve().is_relative_to(root.resolve())


def test_first_deploy_installs_checks_plans_then_activates(tmp_path: Path) -> None:
    runner = FakeRunner(tmp_path)
    bundle, release_id = _bundle(tmp_path / "bundles", "a")

    code, daily, state = _first(tmp_path, runner, bundle)

    assert code == 0
    release = daily / "releases" / release_id
    with zipfile.ZipFile(bundle) as archive:
        for info in archive.infolist():
            assert (release / info.filename).read_bytes() == archive.read(info.filename)
            assert _read_only(release / info.filename), info.filename
    assert not _read_only(deploy.venv_python(release))

    kinds = runner.kinds()
    assert kinds[0] == "schtasks"
    assert runner.calls[0]["argv"] == [
        "schtasks",
        "/query",
        "/tn",
        "AITradingSystem Daily Run",
        "/fo",
        "LIST",
    ]
    order = [kinds.index(kind) for kind in ("venv", "install", "verify", "plan")]
    assert order == sorted(order)
    old_freeze = [call for call in runner.calls if call["argv"][0] == str(runner.old_python)]
    assert len(old_freeze) == 1 and kinds.index("freeze") < kinds.index("install")
    assert runner.one("venv")["argv"] == [
        str(runner.base_python),
        "-m",
        "venv",
        str(release / ".venv"),
    ]
    python = str(deploy.venv_python(release))
    install = runner.one("install")["argv"]
    assert install[:6] == [python, "-m", "pip", "install", "-e", str(release)]
    assert _constraints(runner) == "numpy==2.1.0\npandas==2.2.3\n"
    assert runner.one("verify")["argv"] == [
        python,
        "-I",
        "-m",
        "ai_trading_system.release_identity",
        "verify",
        "--root",
        str(release),
    ]
    plan = runner.one("plan")
    argv = plan["argv"]
    assert isinstance(argv, list)
    assert argv[0] == str(deploy.venv_aits(release))
    assert argv[1:5] == ["ops", "daily-plan", "--fail-on-missing-env", "--output-path"]
    report = Path(argv[5])
    assert not _inside(report, state) and not _inside(report, release)
    env = plan["env"]
    assert isinstance(env, dict)
    assert env["AITS_STATE_ROOT"] == str(state)
    assert env["PYTHONDONTWRITEBYTECODE"] == "1"
    assert runner.read_only_at_plan is True
    assert (release / ".venv_freeze.txt").read_text(encoding="utf-8") == NEW_FREEZE

    assert _current(daily) == {
        "schema_version": "aits_daily_current.v1",
        "release_id": release_id,
        "previous_release_id": None,
        "state_root": str(state),
        "source_commit": "a" * 40,
        "activated_at": "2026-10-14T03:00:00Z",
    }
    assert (daily / "run_daily.ps1").read_bytes() == b"Write-Output 'a'\r\n"
    log = _log(daily)
    assert len(log) == 1
    assert log[0]["outcome"] == "ACTIVATED" and log[0]["release_id"] == release_id


def test_a_later_deploy_keeps_the_state_root_and_pins_to_the_current_release(
    tmp_path: Path,
) -> None:
    runner = FakeRunner(tmp_path)
    first, first_id = _bundle(tmp_path / "bundles", "a")
    second, second_id = _bundle(tmp_path / "bundles", "b")
    _, daily, state = _first(tmp_path, runner, first)
    runner.calls.clear()

    code = _later(runner, second, daily)

    assert code == 0
    assert _constraints(runner) == "numpy==2.1.0\npandas==2.2.3\npyyaml==6.0.2\n"
    assert all(call["argv"][0] != str(runner.old_python) for call in runner.calls)
    current = _current(daily)
    assert current["release_id"] == second_id
    assert current["previous_release_id"] == first_id
    assert current["state_root"] == str(state)
    assert (daily / "releases" / first_id / "RELEASE.json").is_file()
    assert (daily / "run_daily.ps1").read_bytes() == b"Write-Output 'b'\r\n"
    assert [entry["outcome"] for entry in _log(daily)] == ["ACTIVATED", "ACTIVATED"]


def test_an_unpinned_first_deploy_installs_without_constraints(tmp_path: Path) -> None:
    runner = FakeRunner(tmp_path)
    bundle, _ = _bundle(tmp_path / "bundles", "a")
    daily = tmp_path / "daily"

    code = _deploy(
        runner,
        "--bundle",
        bundle,
        "--daily-root",
        daily,
        "--state-root",
        tmp_path / "state",
        "--unpinned",
        "--python",
        runner.base_python,
    )

    assert code == 0
    assert "-c" not in runner.one("install")["argv"]
    assert "freeze" in runner.kinds()


def test_activate_switches_back_after_checking_the_release(tmp_path: Path) -> None:
    runner = FakeRunner(tmp_path)
    first, first_id = _bundle(tmp_path / "bundles", "a")
    second, second_id = _bundle(tmp_path / "bundles", "b")
    _, daily, _ = _first(tmp_path, runner, first)
    _later(runner, second, daily)
    runner.calls.clear()

    assert _deploy(runner, "--activate", first_id, "--daily-root", daily) == 0

    assert runner.kinds() == ["schtasks", "verify"]
    assert runner.one("verify")["argv"][-1] == str(daily / "releases" / first_id)
    current = _current(daily)
    assert current["release_id"] == first_id
    assert current["previous_release_id"] == second_id
    assert current["source_commit"] == "a" * 40
    assert (daily / "run_daily.ps1").read_bytes() == b"Write-Output 'a'\r\n"
    assert (daily / "releases" / second_id / "RELEASE.json").is_file()


@pytest.mark.parametrize(
    ("case", "code"),
    [
        ("unknown", "RELEASE_MISSING"),
        ("verify", "VERIFY_FAILED"),
        ("running", "TASK_RUNNING"),
    ],
)
def test_activate_refusals_leave_current_unchanged(tmp_path: Path, case: str, code: str) -> None:
    runner = FakeRunner(tmp_path)
    first, first_id = _bundle(tmp_path / "bundles", "a")
    second, _ = _bundle(tmp_path / "bundles", "b")
    _, daily, _ = _first(tmp_path, runner, first)
    _later(runner, second, daily)
    before = (daily / "current.json").read_bytes()
    target = "20261014-ccccccccc" if case == "unknown" else first_id
    runner.fail = "verify" if case == "verify" else None
    runner.task_status = "Running" if case == "running" else "Ready"

    assert _deploy(runner, "--activate", target, "--daily-root", daily) == 2

    assert (daily / "current.json").read_bytes() == before
    assert _log(daily)[-1]["outcome"] == "REFUSED"
    assert _log(daily)[-1]["code"] == code


def _refusal_setup(tmp_path: Path, case: str, runner: FakeRunner) -> tuple[list[str], Path]:
    """Arguments of a deploy that must be refused before anything is unpacked."""

    bundles = tmp_path / "bundles"
    bundle, _ = _bundle(bundles, "a")
    daily = tmp_path / "daily"
    state = tmp_path / "state"
    args = [
        "--bundle",
        str(bundle),
        "--daily-root",
        str(daily),
        "--state-root",
        str(state),
        "--constraints-from",
        str(runner.old_python),
        "--python",
        str(runner.base_python),
    ]
    if case == "TASK_RUNNING":
        runner.task_status = "Running"
    elif case == "RUN_LOCK_PRESENT":
        lock = state / "outputs" / "run_control" / "daily" / "locks" / "daily.lock"
        lock.parent.mkdir(parents=True)
        lock.write_text("{}", encoding="utf-8")
    elif case == "STATE_ROOT_REQUIRED":
        args[4:6] = []
    elif case == "STATE_ROOT_INVALID":
        args[5] = "relative/state"
    elif case == "CONSTRAINTS_REQUIRED":
        args[6:8] = []
    elif case == "BUNDLE_SHA256_MISMATCH":
        bundle.with_name(bundle.name + ".sha256").write_text(
            f"{'0' * 64}  {bundle.name}\n", encoding="utf-8"
        )
    elif case == "BUNDLE_SHA256_MISSING_SIDECAR":
        bundle.with_name(bundle.name + ".sha256").unlink()
    elif case == "BUNDLE_INVALID_EXTRA":
        bundle, _ = _bundle(bundles / "extra", "a", extra={"tools/x.py": b""})
    elif case == "BUNDLE_INVALID_CHANGED":
        bundle, _ = _bundle(
            bundles / "changed",
            "a",
            extra={"src/ai_trading_system/__init__.py": b"X = 1\n"},
        )
    elif case == "BUNDLE_INVALID_UNSAFE":
        content = {"pyproject.toml": b"x", "../escape.py": b""}
        bundle, _ = _bundle(bundles / "unsafe", "a", content=content)
    elif case == "BUNDLE_INVALID_STATE":
        content = {"pyproject.toml": b"x", "outputs/x.json": b"{}"}
        bundle, _ = _bundle(bundles / "state", "a", content=content)
    elif case == "BUNDLE_INVALID_NOT_ZIP":
        bundle.write_bytes(b"not a zip")
        bundle.with_name(bundle.name + ".sha256").write_text(
            f"{_sha256(b'not a zip')}  {bundle.name}\n", encoding="utf-8"
        )
    else:
        raise AssertionError(case)
    if case.startswith("BUNDLE_INVALID_") and case != "BUNDLE_INVALID_NOT_ZIP":
        args[1] = str(bundle)
    return args, daily


REFUSALS = {
    "TASK_RUNNING": "TASK_RUNNING",
    "RUN_LOCK_PRESENT": "RUN_LOCK_PRESENT",
    "STATE_ROOT_REQUIRED": "STATE_ROOT_REQUIRED",
    "STATE_ROOT_INVALID": "STATE_ROOT_INVALID",
    "CONSTRAINTS_REQUIRED": "CONSTRAINTS_REQUIRED",
    "BUNDLE_SHA256_MISMATCH": "BUNDLE_SHA256_MISMATCH",
    "BUNDLE_SHA256_MISSING_SIDECAR": "BUNDLE_SHA256_MISMATCH",
    "BUNDLE_INVALID_EXTRA": "BUNDLE_INVALID",
    "BUNDLE_INVALID_CHANGED": "BUNDLE_INVALID",
    "BUNDLE_INVALID_UNSAFE": "BUNDLE_INVALID",
    "BUNDLE_INVALID_STATE": "BUNDLE_INVALID",
    "BUNDLE_INVALID_NOT_ZIP": "BUNDLE_INVALID",
}


@pytest.mark.parametrize("case", sorted(REFUSALS))
def test_a_refused_first_deploy_unpacks_nothing(tmp_path: Path, case: str) -> None:
    runner = FakeRunner(tmp_path)
    args, daily = _refusal_setup(tmp_path, case, runner)

    assert _deploy(runner, *args) == 2

    assert not (daily / "current.json").exists()
    assert not (daily / "run_daily.ps1").exists()
    releases = daily / "releases"
    assert not releases.exists() or list(releases.iterdir()) == []
    assert not {"venv", "install", "verify", "plan"} & set(runner.kinds())
    log = _log(daily)
    assert len(log) == 1
    assert log[0]["outcome"] == "REFUSED"
    assert log[0]["code"] == REFUSALS[case]


@pytest.mark.parametrize(
    ("case", "code"),
    [("exists", "RELEASE_EXISTS"), ("changed_root", "STATE_ROOT_CHANGED")],
)
def test_a_refused_later_deploy_keeps_the_active_release(
    tmp_path: Path, case: str, code: str
) -> None:
    runner = FakeRunner(tmp_path)
    first, first_id = _bundle(tmp_path / "bundles", "a")
    second, _ = _bundle(tmp_path / "bundles", "b")
    _, daily, _ = _first(tmp_path, runner, first)
    before = (daily / "current.json").read_bytes()
    files_before = sorted(path for path in (daily / "releases").rglob("*"))
    args = [
        "--bundle",
        str(first if case == "exists" else second),
        "--daily-root",
        str(daily),
    ]
    if case == "changed_root":
        args += ["--state-root", str(tmp_path / "other_state")]
    runner.calls.clear()

    assert _deploy(runner, *args, "--python", runner.base_python) == 2

    assert (daily / "current.json").read_bytes() == before
    assert sorted(path for path in (daily / "releases").rglob("*")) == files_before
    assert (daily / "run_daily.ps1").read_bytes() == b"Write-Output 'a'\r\n"
    assert not {"venv", "install", "verify", "plan"} & set(runner.kinds())
    assert _log(daily)[-1]["code"] == code
    assert first_id in _current(daily).values()


@pytest.mark.parametrize(
    ("fail", "code"),
    [
        ("venv", "VENV_FAILED"),
        ("install", "PIP_INSTALL_FAILED"),
        ("verify", "VERIFY_FAILED"),
        ("plan", "DAILY_PLAN_FAILED"),
    ],
)
def test_a_failed_install_or_check_keeps_the_directory_under_a_failed_name(
    tmp_path: Path, fail: str, code: str
) -> None:
    runner = FakeRunner(tmp_path)
    first, first_id = _bundle(tmp_path / "bundles", "a")
    second, second_id = _bundle(tmp_path / "bundles", "b")
    _, daily, _ = _first(tmp_path, runner, first)
    before = (daily / "current.json").read_bytes()
    runner.fail = fail

    assert _later(runner, second, daily) == 2

    releases = daily / "releases"
    assert sorted(item.name for item in releases.iterdir()) == [
        first_id,
        f"{second_id}.failed-{STAMP}",
    ]
    assert (releases / f"{second_id}.failed-{STAMP}" / "RELEASE.json").is_file()
    assert (daily / "current.json").read_bytes() == before
    assert (daily / "run_daily.ps1").read_bytes() == b"Write-Output 'a'\r\n"
    assert _log(daily)[-1]["outcome"] == "REFUSED"
    assert _log(daily)[-1]["code"] == code
    assert _current(daily)["release_id"] == first_id


def test_deploys_never_delete_a_release_and_name_cleanup_candidates(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runner = FakeRunner(tmp_path)
    ids = []
    for index, letter in enumerate("abcd"):
        bundle, release_id = _bundle(tmp_path / "bundles", letter)
        ids.append(release_id)
        if index == 0:
            _, daily, _ = _first(tmp_path, runner, bundle)
        else:
            capsys.readouterr()
            assert _later(runner, bundle, daily) == 0

    assert sorted(item.name for item in (daily / "releases").iterdir()) == ids
    assert f"cleanup_candidate={ids[0]}" in capsys.readouterr().out.splitlines()


def test_deploy_uses_only_the_standard_library() -> None:
    path = CODE_ROOT / "tools" / "release" / "deploy_daily_release.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            imported.add(node.module.split(".")[0])

    assert imported - set(sys.stdlib_module_names) == set()


# --- entry point, task registration, runbook -----------------------------------------------------


def _code_lines(name: str) -> str:
    raw = (SCRIPTS / name).read_bytes()
    assert all(byte < 128 for byte in raw), f"{name} must stay ASCII for Windows PowerShell 5.1"
    text = re.sub(r"<#.*?#>", "", raw.decode("ascii"), flags=re.DOTALL)
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def test_run_daily_reads_current_json_and_runs_the_release_wrapper() -> None:
    code = _code_lines("run_daily.ps1")

    assert "[ValidateSet('AUTO', 'PRIMARY', 'RESCUE')]" in code
    assert "Join-Path $DailyRoot 'current.json'" in code
    assert "'aits_daily_current.v1'" in code
    assert "'run_daily_error.log'" in code
    assert "'scripts\\ops\\daily_scheduler_run.ps1'" in code
    assert "'releases'" in code
    set_state = code.index("$env:AITS_STATE_ROOT = ")
    set_bytecode = code.index("$env:PYTHONDONTWRITEBYTECODE = '1'")
    call = code.index("& $wrapper -Window $Window")
    assert set_state < call and set_bytecode < call
    assert code.rstrip().endswith("exit $LASTEXITCODE")
    assert "exit 2" in code
    assert re.search(r"\bgit\b", code, flags=re.IGNORECASE) is None
    assert "Remove-Item" not in code


def test_register_can_point_the_single_task_at_the_daily_root() -> None:
    code = _code_lines("register_daily_scheduler_task.ps1")

    assert "[string]$DailyRoot = ''" in code
    assert "Join-Path $root 'run_daily.ps1'" in code
    assert "Join-Path $root 'current.json'" in code
    assert "'daily_scheduler_run\\.ps1|run_daily\\.ps1|ops daily-run'" in code
    assert code.count("Register-ScheduledTask") == 1


def test_runbook_explains_build_deploy_rollback_and_the_switch() -> None:
    text = (CODE_ROOT / "docs" / "operations" / "operations_runbook.md").read_text("utf-8")
    section = text.split("### 9.3", 1)[1].split("\n## ", 1)[0].replace("\\", "/")

    for needle in (
        "tools/release/build_daily_release.py --commit",
        "tools/release/deploy_daily_release.py --bundle",
        "--state-root",
        "--constraints-from",
        "--activate",
        "register_daily_scheduler_task.ps1 -DailyRoot",
        "current.json",
        "deploy_log.jsonl",
    ):
        assert needle in section, needle


# --- slow: this commit's release plans a daily run without the development repository -------------

_PLAN_SCRIPT = r"""
import json, os, sys
release, repo, out = sys.argv[1:4]
norm = lambda value: os.path.normcase(os.path.abspath(value))
repo = norm(os.path.realpath(repo))
allowed = {norm(os.path.realpath(p)) for p in (sys.prefix, sys.base_prefix, sys.exec_prefix)}
inside = lambda path, root: path == root or path.startswith(root + os.sep)
touched = []

def hook(event, args):
    if event in ("open", "os.listdir", "os.scandir") and args:
        target = args[0]
        if isinstance(target, (str, bytes, os.PathLike)):
            path = norm(os.fsdecode(target))
            if inside(path, repo) and not any(inside(path, root) for root in allowed):
                touched.append(event + " " + path)
    elif event == "subprocess.Popen":
        command = args[1]
        parts = [command] if isinstance(command, str) else [os.fsdecode(x) for x in command]
        text = " ".join(parts)
        # Windows Python 3.11 asks cmd.exe for the OS version when anything calls platform.machine()
        # (pandas does at import): the interpreter's own probe, not the release reading anything.
        if not (sys.platform == "win32" and text.lower().endswith('cmd.exe /c "ver"')):
            touched.append("subprocess " + repr(command))

# The development venv reaches its own checkout through an editable-install path entry. A release's
# venv has none, so drop those entries (the venv's own site-packages may sit inside the checkout).
def _kept(entry):
    path = norm(os.path.realpath(entry))
    return not inside(path, repo) or any(inside(path, root) for root in allowed)

sys.path[:] = [entry for entry in sys.path if _kept(entry)]
sys.path.insert(0, os.path.join(release, "src"))
sys.addaudithook(hook)
import ai_trading_system
from ai_trading_system.cli import app
try:
    app(["ops", "daily-plan", "--output-path", out], standalone_mode=False)
    code = 0
except SystemExit as exc:
    code = exc.code
print(json.dumps({"code": code, "module": ai_trading_system.__file__, "touched": touched}))
"""


@pytest.mark.slow
def test_a_release_of_this_commit_plans_the_daily_run_without_the_development_repo(
    tmp_path: Path,
) -> None:
    files = build.release_file_list(CODE_ROOT, "HEAD")
    archive = subprocess.run(
        ["git", "-C", str(CODE_ROOT), "archive", "--format=tar", "HEAD"],
        check=True,
        capture_output=True,
    ).stdout
    release = tmp_path / "release"
    wanted = set(files)
    contents: dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as tar:
        for member in tar:
            if member.name in wanted:
                handle = tar.extractfile(member)
                assert handle is not None
                contents[member.name] = handle.read()
    assert sorted(contents) == files
    for path, data in contents.items():
        (release / path).parent.mkdir(parents=True, exist_ok=True)
        (release / path).write_bytes(data)
    commit = subprocess.run(
        ["git", "-C", str(CODE_ROOT), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    manifest = _manifest(commit, contents)
    (release / "RELEASE.json").write_text(json.dumps(manifest), encoding="utf-8")
    assert release_mismatches(release) == ()
    before = sorted(path.relative_to(release).as_posix() for path in release.rglob("*"))
    state = tmp_path / "state"
    out = tmp_path / "plan" / "daily_ops_plan.md"
    env = {
        **os.environ,
        "AITS_STATE_ROOT": str(state),
        "PYTHONDONTWRITEBYTECODE": "1",
    }

    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-c",
            _PLAN_SCRIPT,
            str(release),
            str(CODE_ROOT),
            str(out),
        ],
        cwd=release,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=600,
    )

    assert completed.returncode == 0, completed.stderr[-4000:]
    result = json.loads(completed.stdout.strip().splitlines()[-1])
    assert result["code"] == 0, completed.stdout[-4000:]
    assert Path(result["module"]).resolve().is_relative_to(release.resolve())
    assert result["touched"] == []
    assert out.is_file()
    after = sorted(path.relative_to(release).as_posix() for path in release.rglob("*"))
    assert after == before
