"""DEVX-031: parallel pi lanes (scripts/agent/lane.py) and the daily-run window (daily_window.py).

Owner decision owner_decision:DEVX-031:2026-10-14:parallel_pi_lanes. Throw-away repositories: a main
checkout with an origin, lane worktrees of it, and a runs root. pi, the brief parser and the
mechanical checks are stand-ins; ship.py is the real one, and publish.ps1 is the real one where
PowerShell is installed.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
LANE = ROOT / "scripts/agent/lane.py"
WINDOW = ROOT / "scripts/agent/daily_window.py"
POLICY = {
    "schema_version": "gov008_ship_policy.v1",
    "tree_clean_exclusions": [],
    "zones": {"C": ["secret/**"], "A": ["docs/**", "*.md"]},
    "gates": {"A": [], "B": [], "C": []},
}
RENDER = """import pathlib
root = pathlib.Path(__file__).resolve().parents[1]
names = sorted(p.name for p in (root / "tasks").glob("*.yaml"))
(root / "docs").mkdir(exist_ok=True)
(root / "docs/task_register.md").write_text("".join(n + "\\n" for n in names), encoding="utf-8")
"""
# brief.py show: the test briefs are JSON, printed back as the parsed front matter.
FAKE_BRIEF = """import json, pathlib, sys
print(json.dumps(json.loads(pathlib.Path(sys.argv[2]).read_text(encoding="utf-8"))))
"""
# accept_checks.py: records its arguments; fails when the brief asks it to.
FAKE_ACCEPT = """import json, pathlib, sys
args = sys.argv[1:]
run_dir = pathlib.Path(args[args.index("--run-dir") + 1])
with (run_dir / "accept-calls.jsonl").open("a", encoding="utf-8") as h:
    h.write(json.dumps(args) + "\\n")
fail = json.loads((run_dir / "brief.md").read_text(encoding="utf-8")).get("accept_fail", False)
print(json.dumps({"checks": [{"name": "fake", "ok": not fail}]}))
sys.exit(1 if fail else 0)
"""
# Stand-in for powershell: run-brief.ps1 plays pi in the lane; publish.ps1 goes to the real
# script under PowerShell when there is one. pi["outside"] (absolute path -> text) is written while
# pi runs, before its commit: the config or a publish lock that changes during a run (DEVX-031-06).
FAKE_POWERSHELL = """import json, pathlib, shutil, subprocess, sys
args = sys.argv[1:]
script = pathlib.Path(args[args.index("-File") + 1])
rest = args[args.index("-File") + 2:]
def git(repo, *a):
    subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True)
if script.name == "run-brief.ps1":
    brief = json.loads(pathlib.Path(rest[0]).read_text(encoding="utf-8"))
    repo = pathlib.Path(rest[rest.index("-Repo") + 1])
    with (pathlib.Path(rest[0]).parent / "pi-calls.jsonl").open("a", encoding="utf-8") as h:
        h.write(json.dumps(rest) + "\\n")
    pi = brief["pi"]
    if not pi.get("before_pi"):
        (pathlib.Path(rest[0]).parent / "attempts" / "01").mkdir(parents=True, exist_ok=True)
    if pi.get("exit"):
        sys.exit(pi["exit"])
    if "-Fix" not in rest:
        git(repo, "switch", "-c", brief["branch"])
    for rel, text in pi["files"].items():
        path = repo / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    for name, text in pi.get("outside", {}).items():
        pathlib.Path(name).write_text(text, encoding="utf-8")
    git(repo, "add", "-A")
    git(repo, "commit", "-m", pi["message"])
    print("stopReason=stop")
    sys.exit(0)
if script.name == "publish.ps1":
    ps = shutil.which("powershell")
    if ps is None:
        sys.exit(97)
    done = subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script),
                           *rest, "-Python", sys.executable], capture_output=True, text=True)
    sys.stdout.write(done.stdout)
    sys.stderr.write(done.stderr)
    sys.exit(done.returncode)
sys.exit(99)
"""
LINES = "".join(f"line {n}\n" for n in range(1, 21))


def run_git(repo: Path, *args: str) -> str:
    done = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=True
    )
    return done.stdout.strip()


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def needs_powershell() -> None:
    if shutil.which("powershell") is None:
        pytest.skip("PowerShell is not installed; publish.ps1 runs only where it is")


class Setup:
    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        self.origin = tmp / "origin.git"
        self.main = tmp / "main"
        self.runs = tmp / "runs"
        self.config = tmp / "agent_config.json"
        self.lanes = {name: tmp / name for name in ("lane1", "lane2")}
        run_git(tmp, "init", "-q", "--bare", "-b", "main", str(self.origin))
        run_git(tmp, "init", "-q", "-b", "main", str(self.main))
        for key, value in (("user.email", "t@example.invalid"), ("user.name", "t")):
            run_git(self.main, "config", key, value)
        run_git(self.main, "config", "core.autocrlf", "false")
        write(self.main / "config/gov008_ship.yaml", yaml.safe_dump(POLICY))
        for rel in ("tools/gov008/ship.py", "scripts/agent/publish.ps1"):
            (self.main / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / rel, self.main / rel)
        write(self.main / "tools/tasks.py", RENDER)
        write(self.main / "scripts/agent/brief.py", FAKE_BRIEF)
        write(self.main / "scripts/agent/accept_checks.py", FAKE_ACCEPT)
        write(
            self.main / "scripts/agent/agent_config.json",
            json.dumps({"review_root": str(tmp / "reviews")}),
        )
        write(self.main / "src/app.py", LINES)
        write(self.main / "tasks/A0.yaml", "id: A0\n")
        subprocess.run([sys.executable, "tools/tasks.py", "render"], cwd=self.main, check=True)
        run_git(self.main, "add", "-A")
        run_git(self.main, "commit", "-q", "-m", "init")
        run_git(self.main, "remote", "add", "origin", str(self.origin))
        run_git(self.main, "push", "-q", "-u", "origin", "main")
        for path in self.lanes.values():
            run_git(self.main, "worktree", "add", "-q", "--detach", str(path), "main")
        fake_ps = tmp / "fake_powershell.py"
        write(fake_ps, FAKE_POWERSHELL)
        self.settings = {
            "runs_root": str(self.runs),
            "review_root": str(tmp / "reviews"),
            "main_checkout": str(self.main),
            "lanes": {k: str(v) for k, v in self.lanes.items()},
            "powershell": [sys.executable, str(fake_ps)],
            "daily_windows_jst": [],
        }
        self.save()

    def save(self) -> None:
        self.config.write_text(json.dumps(self.settings), encoding="utf-8")

    def batch(
        self,
        run: str,
        files: dict[str, str],
        allowed: list[str],
        model: str = "cloud",
        **extra: object,
    ) -> Path:
        run_dir = self.runs / run
        brief = {
            "task": run.rsplit("-", 1)[0],
            "run": run,
            "zone": "B",
            "model": model,
            "allowed_paths": allowed,
            "branch": f"pi/{run}",
            "pi": {"files": files, "message": f"code: {run}"},
            **extra,
        }
        write(run_dir / "brief.md", json.dumps(brief))
        digest = hashlib.sha256((run_dir / "brief.md").read_bytes()).hexdigest()
        write(run_dir / "SHA256SUMS", f"{digest}  brief.md\n")
        return run_dir

    def lane(self, *args: str) -> tuple[int, dict]:
        # lane.py runs from the PC session, never inside a pi run: publish.ps1 refuses AITS_PI_*.
        env = {k: v for k, v in os.environ.items() if not k.startswith("AITS_PI_")}
        done = subprocess.run(
            [sys.executable, "-I", str(LANE), *args, "--config", str(self.config)],
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
        )
        lines = [line for line in done.stdout.splitlines() if line.startswith("{")]
        assert lines, done.stdout + done.stderr
        return done.returncode, json.loads(lines[-1])

    def lane_in_background(self, *args: str) -> subprocess.Popen[str]:
        env = {k: v for k, v in os.environ.items() if not k.startswith("AITS_PI_")}
        return subprocess.Popen(
            [sys.executable, "-I", str(LANE), *args, "--config", str(self.config)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )

    def state(self, lane: str = "lane1") -> dict:
        return json.loads((self.runs / f"lanes/{lane}.json").read_text(encoding="utf-8"))

    def main_moves(self, files: dict[str, str], message: str = "main moved") -> None:
        for rel, text in files.items():
            write(self.main / rel, text)
        run_git(self.main, "add", "-A")
        run_git(self.main, "commit", "-q", "-m", message)
        run_git(self.main, "push", "-q", "origin", "main")


def edited(line: int, text: str, base: str = LINES) -> str:
    lines = base.splitlines(keepends=True)
    lines[line - 1] = text + "\n"
    return "".join(lines)


@pytest.fixture()
def lanes(tmp_path: Path) -> Setup:
    return Setup(tmp_path)


# ----- glob overlap and the daily window -------------------------------------------------------
def load(name: str, path: Path):  # type: ignore[no-untyped-def]
    import importlib.util

    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("a", "b", "overlap"),
    [
        ("src/a/**", "src/a/x.py", True),
        ("src/a/**", "src/b/**", False),
        ("src/a/**", "src/ab/**", False),
        ("src/a*", "src/ab/**", True),
        ("tasks/DEVX-031*.yaml", "tasks/TRADING-2565*.yaml", False),
        ("tasks/DEVX-031*.yaml", "tasks/DEVX-031_X.yaml", True),
        ("docs/**", "docs/system_flow.md", True),
        ("docs/system_flow.md", "docs/system_flow.md", True),
        ("docs/system_flow.md", "docs/other.md", False),
        ("docs/task_register.md", "docs/task_register.md", False),
        ("docs/task_register.md", "docs/**", False),
    ],
)
def test_globs_overlap(a: str, b: str, overlap: bool) -> None:
    lane = load("lane_mod", LANE)
    assert lane.globs_overlap(a, b) is overlap
    assert lane.globs_overlap(b, a) is overlap


WINDOWS = [
    {"start": "11:50", "end": "13:00", "label": "PRIMARY"},
    {"start": "17:20", "end": "18:30", "label": "RESCUE"},
]


def summary(directory: Path, name: str, written: str) -> Path:
    path = directory / name
    write(path, "{}")
    stamp = dt.datetime.fromisoformat(written).timestamp()
    os.utime(path, (stamp, stamp))
    return path


@pytest.mark.parametrize(
    ("now", "files", "blocked"),
    [
        ("2026-10-14T11:49:00+09:00", [], False),
        ("2026-10-14T11:55:00+09:00", [], True),
        ("2026-10-14T12:16:00+09:00", [("2026-10-14_PRIMARY.summary.json", "12:15")], False),
        ("2026-10-14T12:16:00+09:00", [("2026-10-14_PRIMARY_2.summary.json", "12:15")], False),
        ("2026-10-14T12:16:00+09:00", [("2026-10-14_PRIMARY.summary.json", "11:40")], True),
        ("2026-10-14T12:16:00+09:00", [("2026-10-13_PRIMARY.summary.json", "12:15")], True),
        ("2026-10-14T12:16:00+09:00", [("2026-10-14_RESCUE.summary.json", "12:15")], True),
        ("2026-10-14T17:40:00+09:00", [("2026-10-14_PRIMARY.summary.json", "12:15")], True),
        ("2026-10-14T13:00:00+09:00", [], True),
        ("2026-10-14T13:00:01+09:00", [], False),
        ("2026-10-14T03:16:00+00:00", [("2026-10-14_PRIMARY.summary.json", "12:15")], False),
    ],
)
def test_daily_window_opens_early_on_that_windows_summary(
    tmp_path: Path, now: str, files: list[tuple[str, str]], blocked: bool
) -> None:
    window = load("window_mod", WINDOW)
    directory = tmp_path / "scheduler"
    directory.mkdir()
    for name, at in files:
        summary(
            directory,
            name,
            f"2026-10-14T{at}:00+09:00" if "2026-10-14" in name else f"2026-10-13T{at}:00+09:00",
        )
    config = {"daily_windows_jst": WINDOWS, "daily_scheduler_dir": str(directory)}
    state = window.window_state(config, dt.datetime.fromisoformat(now))
    assert state["blocked"] is blocked


def test_daily_window_without_label_or_directory_stays_closed(tmp_path: Path) -> None:
    window = load("window_mod", WINDOW)
    at = dt.datetime.fromisoformat("2026-10-14T12:16:00+09:00")
    summary(tmp_path, "2026-10-14_PRIMARY.summary.json", "2026-10-14T12:15:00+09:00")
    unlabeled = [{"start": "11:50", "end": "13:00"}]
    assert window.window_state(
        {"daily_windows_jst": unlabeled, "daily_scheduler_dir": str(tmp_path)}, at
    )["blocked"]
    assert window.window_state({"daily_windows_jst": WINDOWS}, at)["blocked"]
    missing = {"daily_windows_jst": WINDOWS, "daily_scheduler_dir": str(tmp_path / "none")}
    assert window.window_state(missing, at)["blocked"]


def test_daily_window_cli_exit_codes(tmp_path: Path) -> None:
    config = tmp_path / "c.json"
    config.write_text(json.dumps({"daily_windows_jst": WINDOWS}), encoding="utf-8")

    def cli(now: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-I", str(WINDOW), "--config", str(config), "--now", now],
            capture_output=True,
            text=True,
        )

    assert cli("2026-10-14T10:00:00+09:00").returncode == 0
    closed = cli("2026-10-14T12:00:00+09:00")
    assert closed.returncode == 3
    assert json.loads(closed.stdout)["window"] == "11:50-13:00 JST"
    assert cli("2026-10-14T12:00:00").returncode == 2


# ----- the rescue window after a passed primary run (DEVX-031 3.7, 2026-10-19) -----------------
# Baker chose 「两项都改」: once the 12:00 run passed, the 17:30 run only finds its as_of complete,
# so the RESCUE window does not close; and pi may start inside a window, only the gate and the
# publish wait for it.
OPEN_IF_PASSED = [
    {"start": "11:50", "end": "13:00", "label": "PRIMARY"},
    {"start": "17:20", "end": "18:30", "label": "RESCUE", "open_if_passed": "PRIMARY"},
]
PASSED = {"blocker_category": "NONE", "daily_run_exit_code": 0, "cli_status": "PASS"}
COMPLETE = {
    "blocker_category": "NONE",
    "daily_run_exit_code": 0,
    "cli_status": "RUN_CONTROL_ALREADY_COMPLETE",
}
SOURCE_FAILED = {"blocker_category": "DATA_SOURCE_UNAVAILABLE", "daily_run_exit_code": 1}
TODAY = "2026-10-14"
P1, P2, R1 = (
    f"{TODAY}_PRIMARY.summary.json",
    f"{TODAY}_PRIMARY_2.summary.json",
    f"{TODAY}_RESCUE.summary.json",
)


def scheduler_file(directory: Path, name: str, written: str, content: object) -> Path:
    """A summary file; content is written as JSON unless it is already text or bytes."""
    path = directory / name
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        write(path, content if isinstance(content, str) else json.dumps(content))
    stamp = dt.datetime.fromisoformat(written).timestamp()
    os.utime(path, (stamp, stamp))
    return path


def jst(day: str, hhmm: str) -> str:
    return f"{day}T{hhmm}:00+09:00"


@pytest.mark.parametrize(
    ("now", "files", "released_by"),
    [
        ("17:25", [(P1, "12:07", PASSED)], P1),
        ("17:20", [(P1, "12:00", COMPLETE)], P1),
        ("18:30", [(P1, "12:07", PASSED)], P1),
        ("17:25", [(P2, "14:10", PASSED)], P2),
        ("17:25", [(P1, "12:07", SOURCE_FAILED), (P2, "14:10", PASSED)], P2),
        ("17:25", [(P1, "12:07", PASSED), (P2, "14:10", SOURCE_FAILED)], P1),
        ("17:25", [(P1, "12:07", PASSED), (R1, "17:24", SOURCE_FAILED)], None),
        ("17:25", [(P1, "12:07", SOURCE_FAILED), (R1, "17:24", "{}")], R1),
        ("17:25", [(R1, "17:24", SOURCE_FAILED)], R1),
        ("12:16", [(P1, "12:07", SOURCE_FAILED)], P1),
    ],
)
def test_rescue_window_opens_on_a_passed_primary_run_of_today(
    tmp_path: Path, now: str, files: list[tuple[str, str, object]], released_by: str | None
) -> None:
    window = load("window_mod", WINDOW)
    for name, at, content in files:
        scheduler_file(tmp_path, name, jst(TODAY, at), content)
    config = {"daily_windows_jst": OPEN_IF_PASSED, "daily_scheduler_dir": str(tmp_path)}
    state = window.window_state(config, dt.datetime.fromisoformat(jst(TODAY, now)))
    assert state["blocked"] is False, state
    assert state["window"] in ("17:20-18:30 JST", "11:50-13:00 JST")
    if released_by is not None:
        assert Path(state["released_by"]).name == released_by
    else:
        assert Path(state["released_by"]).name in (P1, R1)


@pytest.mark.parametrize(
    "files",
    [
        [],
        [(P1, jst(TODAY, "12:07"), SOURCE_FAILED)],
        [(P1, jst(TODAY, "12:07"), {"blocker_category": "NONE", "daily_run_exit_code": 1})],
        [
            (
                P1,
                jst(TODAY, "12:07"),
                {"blocker_category": "CONTROL_PLANE", "daily_run_exit_code": 0},
            )
        ],
        [(P1, jst(TODAY, "12:07"), {"blocker_category": "NONE", "daily_run_exit_code": None})],
        [(P1, jst(TODAY, "12:07"), {"blocker_category": "NONE", "daily_run_exit_code": "0"})],
        [(P1, jst(TODAY, "12:07"), {"blocker_category": "NONE"})],
        [(P1, jst(TODAY, "12:07"), {"daily_run_exit_code": 0})],
        [(P1, jst(TODAY, "12:07"), '{"blocker_category": "NONE", "daily_run_exit')],
        [(P1, jst(TODAY, "12:07"), "[]")],
        [(P1, jst(TODAY, "12:07"), b"\xff\xfe not utf-8")],
        [(P1, jst(TODAY, "11:40"), PASSED)],
        [("2026-10-13_PRIMARY.summary.json", jst("2026-10-13", "12:07"), PASSED)],
        [(f"{TODAY}_PRIMARYX.summary.json", jst(TODAY, "12:07"), PASSED)],
        [(f"{TODAY}_PRIMARY.summary.md", jst(TODAY, "12:07"), PASSED)],
    ],
)
def test_rescue_window_stays_closed_without_a_passed_primary_run_of_today(
    tmp_path: Path, files: list[tuple[str, str, object]]
) -> None:
    window = load("window_mod", WINDOW)
    for name, written, content in files:
        scheduler_file(tmp_path, name, written, content)
    config = {"daily_windows_jst": OPEN_IF_PASSED, "daily_scheduler_dir": str(tmp_path)}
    state = window.window_state(config, dt.datetime.fromisoformat(jst(TODAY, "17:25")))
    assert state["blocked"] is True, state
    assert state["window"] == "17:20-18:30 JST"
    assert state["reason"] == (
        "no RESCUE summary of today written since 17:20 JST and no passed PRIMARY summary of today"
    )
    assert "released_by" not in state


def test_open_if_passed_needs_its_own_key_and_a_configured_window(tmp_path: Path) -> None:
    window = load("window_mod", WINDOW)
    scheduler_file(tmp_path, P1, jst(TODAY, "12:07"), PASSED)
    scheduler_file(tmp_path, f"{TODAY}_NOON.summary.json", jst(TODAY, "12:07"), PASSED)
    at = dt.datetime.fromisoformat(jst(TODAY, "17:25"))
    plain = window.window_state(
        {"daily_windows_jst": WINDOWS, "daily_scheduler_dir": str(tmp_path)}, at
    )
    assert plain["blocked"] is True
    assert plain["reason"] == "no RESCUE summary of today written since 17:20 JST"
    unknown = [OPEN_IF_PASSED[0], {**OPEN_IF_PASSED[1], "open_if_passed": "NOON"}]
    state = window.window_state(
        {"daily_windows_jst": unknown, "daily_scheduler_dir": str(tmp_path)}, at
    )
    assert state["blocked"] is True
    missing = {"daily_windows_jst": OPEN_IF_PASSED, "daily_scheduler_dir": str(tmp_path / "none")}
    assert window.window_state(missing, at)["blocked"] is True
    # The PRIMARY window has no open_if_passed: it still waits for its own summary.
    assert window.window_state(
        {"daily_windows_jst": OPEN_IF_PASSED, "daily_scheduler_dir": str(tmp_path / "none")},
        dt.datetime.fromisoformat(jst(TODAY, "11:55")),
    )["blocked"]
    assert window.window_state(
        {"daily_windows_jst": OPEN_IF_PASSED, "daily_scheduler_dir": str(tmp_path)},
        dt.datetime.fromisoformat(jst(TODAY, "17:19")),
    ) == {"blocked": False, "now": f"{TODAY} 17:19 +0900"}


@pytest.mark.parametrize(
    ("log", "exit_code", "precheck", "blocked"),
    [
        ("Canonical run control：RUN_CONTROL_ALREADY_COMPLETE\n", 0, None, False),
        ("每日运行执行：PASS\n", 0, None, False),
        ("每日运行执行：FAIL\n", 1, None, True),
        ("Traceback (most recent call last)\n", 1, None, True),
        (None, None, "STATE_ROOT_INVALID", True),
    ],
)
def test_rescue_window_reads_what_the_real_scheduler_summary_writes(
    tmp_path: Path, log: str | None, exit_code: int | None, precheck: str | None, blocked: bool
) -> None:
    scheduler = tmp_path / "state/outputs/run_control/scheduler"
    args = [
        sys.executable,
        "-I",
        str(ROOT / "scripts/ops/daily_scheduler_summary.py"),
        "--runtime-root",
        str(tmp_path / "release"),
        "--state-root",
        str(tmp_path / "state"),
        "--window",
        "PRIMARY",
        "--output-stem",
        str(scheduler / f"{TODAY}_PRIMARY"),
    ]
    if precheck is not None:
        args += ["--precheck-blocker", precheck]
    else:
        assert log is not None
        log_path = tmp_path / "daily.log"
        log_path.write_bytes(("Run ID：daily_ops_run:2026-10-13:x1\n" + log).encode("utf-8"))
        args += ["--log", str(log_path), "--exit-code", str(exit_code)]
    subprocess.run(args, check=True, capture_output=True, timeout=60)
    written = scheduler / P1
    assert written.is_file()
    stamp = dt.datetime.fromisoformat(jst(TODAY, "12:07")).timestamp()
    os.utime(written, (stamp, stamp))
    window = load("window_mod", WINDOW)
    config = {"daily_windows_jst": OPEN_IF_PASSED, "daily_scheduler_dir": str(scheduler)}
    state = window.window_state(config, dt.datetime.fromisoformat(jst(TODAY, "17:25")))
    assert state["blocked"] is blocked, (state, json.loads(written.read_text(encoding="utf-8")))


def test_daily_window_cli_with_open_if_passed(tmp_path: Path) -> None:
    config = tmp_path / "c.json"
    scheduler = tmp_path / "scheduler"
    scheduler.mkdir()
    config.write_text(
        json.dumps({"daily_windows_jst": OPEN_IF_PASSED, "daily_scheduler_dir": str(scheduler)}),
        encoding="utf-8",
    )

    def cli() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                "-I",
                str(WINDOW),
                "--config",
                str(config),
                "--now",
                jst(TODAY, "17:25"),
            ],
            capture_output=True,
            text=True,
        )

    assert cli().returncode == 3
    summary_file = scheduler_file(scheduler, P1, jst(TODAY, "12:07"), '{"blocker_category": ')
    assert cli().returncode == 3
    scheduler_file(scheduler, P1, jst(TODAY, "12:07"), PASSED)
    opened = cli()
    assert opened.returncode == 0, opened.stdout + opened.stderr
    assert json.loads(opened.stdout)["released_by"] == str(summary_file)


# ----- lanes ------------------------------------------------------------------------------------
def test_start_runs_pi_in_the_lane_and_holds_it(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    code, out = lanes.lane("start", "--lane", "lane1", "--run", "T-01")
    assert code == 0, out
    assert out["branch"] == "pi/T-01" and out["commits"] == "1"
    call = json.loads((lanes.runs / "T-01/pi-calls.jsonl").read_text().splitlines()[0])
    assert call[call.index("-Repo") + 1] == str(lanes.lanes["lane1"])
    assert "-StateRoot" not in call
    assert "-Direct" in call
    state = json.loads((lanes.runs / "lanes/lane1.json").read_text())
    assert state["run"] == "T-01" and state["phase"] == "stopped" and state["branch"] == "pi/T-01"
    assert run_git(lanes.main, "rev-parse", "main") == state["base_at_start"]


def test_start_passes_the_state_root_only_when_asked(lanes: Setup) -> None:
    # DEVX-031-05: AITS_STATE_ROOT made pi's own tests in a lane read the main checkout's data.
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    code, out = lanes.lane("start", "--lane", "lane1", "--run", "T-01", "--with-state-root")
    assert code == 0, out
    call = json.loads((lanes.runs / "T-01/pi-calls.jsonl").read_text().splitlines()[0])
    assert call[call.index("-StateRoot") + 1] == str(lanes.main)


def test_start_frees_the_lane_when_run_brief_stops_before_pi_starts(lanes: Setup) -> None:
    # DEVX-031-03: run-brief.ps1 failed on its prompt copy; nothing had run, so nothing to resume.
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"], pi={"exit": 1, "before_pi": True})
    for _ in range(2):
        code, out = lanes.lane("start", "--lane", "lane1", "--run", "T-01")
        assert code == 1 and out["run_brief_exit"] == 1 and "before pi started" in out["error"]
        assert not (lanes.runs / "lanes/lane1.json").exists()
        path = lanes.lanes["lane1"]
        assert run_git(path, "branch", "--show-current") == ""
        assert run_git(path, "rev-parse", "HEAD") == run_git(lanes.main, "rev-parse", "main")


def test_start_keeps_the_lane_when_pi_ran_and_failed(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"], pi={"exit": 1})
    code, out = lanes.lane("start", "--lane", "lane1", "--run", "T-01")
    assert code == 1 and out["run_brief_exit"] == 1 and "did not finish cleanly" in out["error"]
    state = json.loads((lanes.runs / "lanes/lane1.json").read_text())
    assert state["run"] == "T-01" and state["phase"] == "stopped"


def test_two_lanes_run_side_by_side_unless_their_paths_overlap(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**", "docs/task_register.md"])
    lanes.batch("T-02", {"src/b/y.py": "y\n"}, ["src/b/**", "docs/task_register.md"])
    lanes.batch("T-03", {"src/a/z.py": "z\n"}, ["src/a/z.py"])
    assert lanes.lane("start", "--lane", "lane1", "--run", "T-01")[0] == 0
    code, out = lanes.lane("start", "--lane", "lane2", "--run", "T-03")
    assert code == 2 and out["overlaps"][0]["run"] == "T-01"
    assert not (lanes.runs / "T-03/pi-calls.jsonl").exists()
    assert lanes.lane("start", "--lane", "lane2", "--run", "T-02")[0] == 0


def test_shared_paths_do_not_count_as_overlap_unless_a_glob_covers_them(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**", "docs/flow.md"])
    lanes.batch("T-02", {"src/b/y.py": "y\n"}, ["src/b/**", "docs/flow.md"])
    lanes.batch("T-03", {"src/c/z.py": "z\n"}, ["src/c/**", "docs/**"])
    assert lanes.lane("start", "--lane", "lane1", "--run", "T-01")[0] == 0
    code, out = lanes.lane("start", "--lane", "lane2", "--run", "T-02")
    assert code == 2 and out["overlaps"][0]["paths"] == [["docs/flow.md", "docs/flow.md"]]
    lanes.settings["shared_paths"] = ["docs/flow.md"]
    lanes.save()
    assert lanes.lane("start", "--lane", "lane2", "--run", "T-02")[0] == 0
    assert lanes.lane("release", "--lane", "lane2", "--run", "T-02")[0] == 0
    code, out = lanes.lane("start", "--lane", "lane2", "--run", "T-03")
    assert code == 2 and out["overlaps"][0]["run"] == "T-01"


@pytest.mark.parametrize("case", ["sums", "run_name", "held", "on_branch", "dirty"])
def test_start_refusals_change_nothing(lanes: Setup, case: str) -> None:
    run_dir = lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    lane1 = lanes.lanes["lane1"]
    problem = {
        "sums": "SHA256SUMS",
        "run_name": "is for run",
        "held": "holds run",
        "on_branch": "on a branch",
        "dirty": "not clean",
    }[case]
    if case == "sums":
        (run_dir / "brief.md").write_text((run_dir / "brief.md").read_text() + " ")
    elif case == "run_name":
        lanes.batch("T-02", {}, ["src/**"])
        (run_dir / "brief.md").write_bytes((lanes.runs / "T-02/brief.md").read_bytes())
        (run_dir / "SHA256SUMS").write_bytes((lanes.runs / "T-02/SHA256SUMS").read_bytes())
    elif case == "held":
        write(lanes.runs / "lanes/lane1.json", json.dumps({"run": "OTHER-01"}))
    elif case == "on_branch":
        run_git(lane1, "switch", "-q", "-c", "stray")
    elif case == "dirty":
        write(lane1 / "src/app.py", "dirty\n")
    code, out = lanes.lane("start", "--lane", "lane1", "--run", "T-01")
    assert code == 2 and problem in out["error"], out
    assert not (run_dir / "pi-calls.jsonl").exists()


def test_start_runs_pi_inside_a_closed_window_but_report_and_refresh_wait(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    lanes.settings["daily_windows_jst"] = [{"start": "00:00", "end": "23:59"}]
    lanes.save()
    code, out = lanes.lane("start", "--lane", "lane1", "--run", "T-01")
    assert code == 0, out
    assert out["branch"] == "pi/T-01" and out["commits"] == "1"
    assert (lanes.runs / "T-01/pi-calls.jsonl").exists()
    for action in ("report", "refresh"):
        code, out = lanes.lane(action, "--lane", "lane1", "--run", "T-01")
        assert code == 2 and "daily-run window" in out["error"], (action, out)
    assert not (lanes.runs / "T-01/accept-calls.jsonl").exists()
    assert run_git(lanes.origin, "branch", "--list", "pi/T-01") == ""
    assert lanes.state()["phase"] == "stopped"
    lanes.settings["daily_windows_jst"] = []
    lanes.save()
    code, out = lanes.lane("report", "--lane", "lane1", "--run", "T-01")
    assert code == 0, out


def test_start_then_report_starts_inside_a_closed_window_and_waits_for_it(lanes: Setup) -> None:
    lanes.settings.update(
        {"daily_windows_jst": [{"start": "00:00", "end": "23:59"}], "report_wait_seconds": 0}
    )
    lanes.save()
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    code, out = lanes.lane("start", "--lane", "lane1", "--run", "T-01", "--then-report")
    assert code == 1, out
    assert out["run_brief_exit"] == 0 and out["commits"] == "1"
    assert out["error"] == REPORT_FAILED + out["report_error"]["error"]
    assert "daily-run window" in out["report_error"]["error"]
    assert out["report_error"]["tries"] == 1
    assert not (lanes.runs / "T-01/accept-calls.jsonl").exists()
    assert run_git(lanes.origin, "branch", "--list", "pi/T-01") == ""


def test_publish_waits_for_a_closed_window(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    lanes.lane("start", "--lane", "lane1", "--run", "T-01")
    code, report = lanes.lane("report", "--lane", "lane1", "--run", "T-01")
    assert code == 0, report
    lanes.settings["daily_windows_jst"] = [{"start": "00:00", "end": "23:59"}]
    lanes.save()
    before = run_git(lanes.main, "rev-parse", "main")
    fp = report["fingerprints"]
    code, out = lanes.lane(
        "publish",
        "--lane",
        "lane1",
        "--run",
        "T-01",
        "--expected-change",
        fp["change"],
        "--expected-message",
        fp["message"],
    )
    assert code == 2 and "daily-run window" in out["error"], out
    assert run_git(lanes.main, "rev-parse", "main") == before
    assert run_git(lanes.origin, "rev-parse", "refs/heads/pi/T-01") == report["commit"]
    assert not (lanes.runs / "lanes/publish.lock").exists()
    assert lanes.state()["phase"] == "reported"


def test_one_local_model_run_at_a_time(lanes: Setup) -> None:
    write(
        lanes.runs / "lanes/lane1.json",
        json.dumps(
            {
                "run": "L-01",
                "model": "local",
                "phase": "running",
                "pid": os.getpid(),
                "allowed_paths": ["src/a/**"],
            }
        ),
    )
    lanes.batch("L-02", {"src/b/y.py": "y\n"}, ["src/b/**"], model="local")
    code, out = lanes.lane("start", "--lane", "lane2", "--run", "L-02")
    assert code == 2 and out["busy"] == ["lane1:L-01"]
    lanes.batch("C-02", {"src/b/y.py": "y\n"}, ["src/b/**"])
    assert lanes.lane("start", "--lane", "lane2", "--run", "C-02")[0] == 0


def test_report_checks_gates_and_pushes_the_branch(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    lanes.lane("start", "--lane", "lane1", "--run", "T-01")
    code, out = lanes.lane("report", "--lane", "lane1", "--run", "T-01")
    assert code == 0, out
    lane1 = lanes.lanes["lane1"]
    assert run_git(lane1, "log", "-1", "--format=%(trailers:key=Gate,valueonly)").startswith(
        "zone=B"
    )
    assert out["commit"] == run_git(lane1, "rev-parse", "HEAD")
    assert run_git(lanes.origin, "rev-parse", "refs/heads/pi/T-01") == out["commit"]
    assert set(out["fingerprints"]) == {"patch", "change", "message"}
    calls = [
        json.loads(x) for x in (lanes.runs / "T-01/accept-calls.jsonl").read_text().splitlines()
    ]
    assert ["--gate" in c for c in calls] == [False, True]
    assert json.loads((lanes.runs / "T-01/lane-report.json").read_text())["tree"] == out["tree"]


def test_report_stops_on_failed_checks_before_the_gate(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"], accept_fail=True)
    lanes.lane("start", "--lane", "lane1", "--run", "T-01")
    code, out = lanes.lane("report", "--lane", "lane1", "--run", "T-01")
    assert code == 1 and out["failed"] == ["fake"]
    assert (
        run_git(lanes.lanes["lane1"], "log", "-1", "--format=%(trailers:key=Gate,valueonly)") == ""
    )
    assert run_git(lanes.origin, "branch", "--list", "pi/T-01") == ""


# ----- start --then-report (DEVX-031-06) --------------------------------------------------------
# 2026-10-18: pi finished in a lane at 19:19 JST, but the PC session that ran start in the
# background had gone idle and been stopped, so nobody ran report for two hours.
REPORT_FAILED = "pi finished; report did not run or failed: "
LOCK_TEXT = "lane2 X-01 pid 1"


def pi_that_also_writes(outside: dict[Path, str]) -> dict[str, object]:
    return {
        "files": {"src/a/x.py": "x\n"},
        "message": "code: T-01",
        "outside": {str(path): text for path, text in outside.items()},
    }


def finished(proc: subprocess.Popen[str]) -> tuple[int, dict]:
    stdout, stderr = proc.communicate(timeout=180)
    lines = [line for line in stdout.splitlines() if line.startswith("{")]
    assert lines, stdout + stderr
    return proc.returncode, json.loads(lines[-1])


def remove(path: Path) -> None:
    # Windows refuses to delete a file another process has open for that moment: try again.
    for _ in range(100):
        try:
            path.unlink()
            return
        except PermissionError:
            time.sleep(0.05)
    path.unlink()


def test_start_then_report_reports_when_pi_finishes(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    code, out = lanes.lane("start", "--lane", "lane1", "--run", "T-01", "--then-report")
    assert code == 0, out
    lane1 = lanes.lanes["lane1"]
    assert out["action"] == "start" and out["run_brief_exit"] == 0
    assert out["branch"] == "pi/T-01" and out["commits"] == "1"
    assert out["tail"] == ["stopReason=stop"] and "report_error" not in out
    assert out["report"]["commit"] == run_git(lane1, "rev-parse", "HEAD")
    assert run_git(lanes.origin, "rev-parse", "refs/heads/pi/T-01") == out["report"]["commit"]
    assert run_git(lane1, "log", "-1", "--format=%(trailers:key=Gate,valueonly)").startswith(
        "zone=B"
    )
    calls = [
        json.loads(x) for x in (lanes.runs / "T-01/accept-calls.jsonl").read_text().splitlines()
    ]
    assert ["--gate" in c for c in calls] == [False, True]
    state = lanes.state()
    assert state["phase"] == "reported" and state["commit"] == out["report"]["commit"]
    assert "report_error" not in state
    report = json.loads((lanes.runs / "T-01/lane-report.json").read_text())
    assert report["tree"] == out["report"]["tree"]
    assert report["commit"] == out["report"]["commit"]
    code, status = lanes.lane("status")
    assert code == 0 and "report_error" not in status["lanes"]["lane1"]["state"]


def test_start_then_report_keeps_the_pi_result_when_the_report_fails(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"], accept_fail=True)
    code, out = lanes.lane("start", "--lane", "lane1", "--run", "T-01", "--then-report")
    assert code == 1 and out["ok"] is False, out
    assert out["error"] == REPORT_FAILED + "accept_checks failed"
    assert out["report_error"]["error"] == "accept_checks failed"
    assert out["report_error"]["failed"] == ["fake"]
    assert out["run_brief_exit"] == 0 and out["branch"] == "pi/T-01" and out["commits"] == "1"
    assert out["tail"] == ["stopReason=stop"] and "report" not in out
    state = lanes.state()
    assert state["run"] == "T-01" and state["phase"] == "stopped"
    assert state["report_error"] == "accept_checks failed"
    assert run_git(lanes.origin, "branch", "--list", "pi/T-01") == ""
    assert run_git(lanes.lanes["lane1"], "branch", "--show-current") == "pi/T-01"
    code, status = lanes.lane("status")
    assert code == 0 and status["lanes"]["lane1"]["state"]["phase"] == "stopped"
    assert status["lanes"]["lane1"]["state"]["report_error"] == "accept_checks failed"


def test_start_then_report_stops_on_a_closed_window_and_report_works_later(lanes: Setup) -> None:
    # The config changes while pi runs: the report reads it as it is after pi stopped.
    closed = {
        **lanes.settings,
        "daily_windows_jst": [{"start": "00:00", "end": "23:59"}],
        "report_wait_seconds": 0,
    }
    lanes.batch(
        "T-01", {}, ["src/a/**"], pi=pi_that_also_writes({lanes.config: json.dumps(closed)})
    )
    code, out = lanes.lane("start", "--lane", "lane1", "--run", "T-01", "--then-report")
    assert code == 1, out
    assert "daily-run window" in out["report_error"]["error"]
    assert out["error"] == REPORT_FAILED + out["report_error"]["error"]
    assert out["report_error"]["tries"] == 1
    assert out["run_brief_exit"] == 0 and out["branch"] == "pi/T-01"
    assert not (lanes.runs / "T-01/accept-calls.jsonl").exists()
    assert run_git(lanes.origin, "branch", "--list", "pi/T-01") == ""
    state = lanes.state()
    assert state["phase"] == "stopped"
    assert "daily-run window" in state["report_error"]
    lanes.save()
    code, out = lanes.lane("report", "--lane", "lane1", "--run", "T-01")
    assert code == 0, out
    assert run_git(lanes.origin, "rev-parse", "refs/heads/pi/T-01") == out["commit"]
    state = lanes.state()
    assert state["phase"] == "reported" and "report_error" not in state


@pytest.mark.parametrize("main_moves", [False, True])
def test_start_then_report_waits_for_a_publish_to_end(lanes: Setup, main_moves: bool) -> None:
    lock = lanes.runs / "lanes/publish.lock"
    lanes.settings.update({"report_poll_seconds": 1, "report_wait_seconds": 60})
    lanes.save()
    lanes.batch("T-01", {}, ["src/a/**"], pi=pi_that_also_writes({lock: LOCK_TEXT}))
    proc = lanes.lane_in_background("start", "--lane", "lane1", "--run", "T-01", "--then-report")
    try:
        deadline = time.monotonic() + 120
        while not lock.exists() and proc.poll() is None and time.monotonic() < deadline:
            time.sleep(0.05)
        time.sleep(2.5)
        assert proc.poll() is None, finished(proc)
        if main_moves:
            lanes.main_moves({"docs/other.md": "m\n"})
        remove(lock)
        code, out = finished(proc)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    if main_moves:
        # No refresh on its own: report's own refusal is the outcome.
        assert code == 1 and "use refresh" in out["report_error"]["error"], out
        assert run_git(lanes.origin, "branch", "--list", "pi/T-01") == ""
        assert lanes.state()["phase"] == "stopped"
        assert "use refresh" in lanes.state()["report_error"]
    else:
        assert code == 0, out
        assert run_git(lanes.origin, "rev-parse", "refs/heads/pi/T-01") == out["report"]["commit"]
        assert lanes.state()["phase"] == "reported"


def test_start_then_report_gives_up_after_report_wait_seconds_without_spinning(
    lanes: Setup,
) -> None:
    lock = lanes.runs / "lanes/publish.lock"
    lanes.settings.update({"report_poll_seconds": 0, "report_wait_seconds": 2.5})
    lanes.save()
    lanes.batch("T-01", {}, ["src/a/**"], pi=pi_that_also_writes({lock: LOCK_TEXT}))
    began = time.monotonic()
    code, out = lanes.lane("start", "--lane", "lane1", "--run", "T-01", "--then-report")
    assert code == 1, out
    assert f"a publish is running ({LOCK_TEXT})" in out["report_error"]["error"]
    assert out["error"] == REPORT_FAILED + out["report_error"]["error"]
    # report_poll_seconds 0 still sleeps 1 s between tries: tries at about 0, 1 and 2 s.
    assert 2 <= out["report_error"]["tries"] <= 3
    assert time.monotonic() - began >= 1
    assert not (lanes.runs / "T-01/accept-calls.jsonl").exists()
    state = lanes.state()
    assert state["phase"] == "stopped" and LOCK_TEXT in state["report_error"]
    remove(lock)
    code, out = lanes.lane("report", "--lane", "lane1", "--run", "T-01")
    assert code == 0, out
    assert run_git(lanes.origin, "rev-parse", "refs/heads/pi/T-01") == out["commit"]


@pytest.mark.parametrize(
    ("key", "value"),
    [("report_wait_seconds", "soon"), ("report_poll_seconds", -1), ("report_poll_seconds", True)],
)
def test_start_then_report_refuses_bad_wait_settings(lanes: Setup, key: str, value: object) -> None:
    lanes.settings[key] = value
    lanes.save()
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    code, out = lanes.lane("start", "--lane", "lane1", "--run", "T-01", "--then-report")
    assert code == 1 and key in out["report_error"]["error"], out
    assert out["error"] == REPORT_FAILED + out["report_error"]["error"]
    assert out["branch"] == "pi/T-01"
    assert not (lanes.runs / "T-01/accept-calls.jsonl").exists()
    state = lanes.state()
    assert state["phase"] == "stopped" and key in state["report_error"]


@pytest.mark.parametrize("case", ["refused", "before_pi", "pi_failed"])
def test_start_then_report_does_not_report_when_start_fails(lanes: Setup, case: str) -> None:
    pi = {
        "refused": {"files": {"src/a/x.py": "x\n"}, "message": "code: T-01"},
        "before_pi": {"exit": 1, "before_pi": True},
        "pi_failed": {"exit": 1},
    }[case]
    run_dir = lanes.batch("T-01", {}, ["src/a/**"], pi=pi)
    if case == "refused":
        write(lanes.runs / "lanes/publish.lock", LOCK_TEXT)
    code, out = lanes.lane("start", "--lane", "lane1", "--run", "T-01", "--then-report")
    want_code, problem = {
        "refused": (2, "publish is running"),
        "before_pi": (1, "before pi started"),
        "pi_failed": (1, "did not finish cleanly"),
    }[case]
    assert code == want_code and problem in out["error"], out
    assert not out["error"].startswith(REPORT_FAILED)
    assert "report_error" not in out and "report" not in out
    assert not (run_dir / "accept-calls.jsonl").exists()
    assert run_git(lanes.origin, "branch", "--list", "pi/T-01") == ""
    if case == "pi_failed":
        assert out["run_brief_exit"] == 1
        state = lanes.state()
        assert state["phase"] == "stopped" and "report_error" not in state
    else:
        assert not (lanes.runs / "lanes/lane1.json").exists()


def test_the_actions_in_the_docstring_name_then_report() -> None:
    lane = load("lane_mod", LANE)
    actions = lane.__doc__.split("Actions", 1)[1].split("Rules it enforces", 1)[0]
    start = [line for line in actions.splitlines() if line.lstrip().startswith("start ")]
    assert len(start) == 1 and start[0].rstrip().endswith("[--then-report]")


def test_the_docstring_rules_let_start_run_inside_a_window() -> None:
    lane = load("lane_mod", LANE)
    rules = " ".join(lane.__doc__.split("Rules it enforces", 1)[1].split())
    assert "no gate or publish inside a closed daily-run window" in rules
    assert "no start" not in rules


def test_refresh_replays_onto_moved_main_and_keeps_the_change(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/app.py": edited(10, "lane ten")}, ["src/app.py"])
    lanes.lane("start", "--lane", "lane1", "--run", "T-01")
    _, first = lanes.lane("report", "--lane", "lane1", "--run", "T-01")
    lanes.main_moves({"src/app.py": edited(12, "main twelve")})
    code, out = lanes.lane("report", "--lane", "lane1", "--run", "T-01")
    assert code == 2 and "refresh" in out["error"]
    code, out = lanes.lane("refresh", "--lane", "lane1", "--run", "T-01")
    assert code == 0, out
    assert out["replayed"]["change"] == first["fingerprints"]["change"]
    assert out["fingerprints"]["change"] == first["fingerprints"]["change"]
    assert out["fingerprints"]["patch"] != first["fingerprints"]["patch"]
    assert out["tree"] != first["tree"]
    assert run_git(lanes.origin, "rev-parse", "refs/heads/pi/T-01") == out["commit"]


def test_publish_by_change_publishes_frees_the_lane_and_deletes_the_remote_branch(
    lanes: Setup,
) -> None:
    needs_powershell()
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    lanes.lane("start", "--lane", "lane1", "--run", "T-01")
    _, report = lanes.lane("report", "--lane", "lane1", "--run", "T-01")
    fp = report["fingerprints"]
    code, out = lanes.lane(
        "publish",
        "--lane",
        "lane1",
        "--run",
        "T-01",
        "--expected-change",
        fp["change"],
        "--expected-message",
        fp["message"],
    )
    assert code == 0, out
    assert run_git(lanes.origin, "rev-parse", "main") == report["commit"]
    assert run_git(lanes.main, "rev-parse", "main") == report["commit"]
    assert (
        out["remote_branch_deleted"] and run_git(lanes.origin, "branch", "--list", "pi/T-01") == ""
    )
    assert run_git(lanes.lanes["lane1"], "rev-parse", "HEAD") == report["commit"]
    assert run_git(lanes.lanes["lane1"], "branch", "--show-current") == ""
    assert not (lanes.runs / "lanes/lane1.json").exists()
    assert json.loads((lanes.runs / "T-01/lane-state.json").read_text())["phase"] == "published"
    assert not (lanes.runs / "lanes/publish.lock").exists()


def test_second_lane_publishes_after_the_first_by_replay(lanes: Setup) -> None:
    needs_powershell()
    lanes.batch("T-01", {"src/app.py": edited(10, "lane ten")}, ["src/app.py"])
    lanes.batch(
        "T-02",
        {"src/b/y.py": "y\n", "tasks/B1.yaml": "id: B1\n"},
        ["src/b/**", "tasks/B1.yaml", "docs/task_register.md"],
    )
    for lane, run in (("lane1", "T-01"), ("lane2", "T-02")):
        assert lanes.lane("start", "--lane", lane, "--run", run)[0] == 0
    reports = {
        run: lanes.lane("report", "--lane", lane, "--run", run)[1]
        for lane, run in (("lane1", "T-01"), ("lane2", "T-02"))
    }
    fp1, fp2 = reports["T-01"]["fingerprints"], reports["T-02"]["fingerprints"]
    assert (
        lanes.lane(
            "publish",
            "--lane",
            "lane1",
            "--run",
            "T-01",
            "--expected-patch",
            fp1["patch"],
            "--expected-message",
            fp1["message"],
        )[0]
        == 0
    )
    code, out = lanes.lane(
        "publish",
        "--lane",
        "lane2",
        "--run",
        "T-02",
        "--expected-change",
        fp2["change"],
        "--expected-message",
        fp2["message"],
    )
    assert code == 0, out
    assert out["refreshed"] is True
    main_files = run_git(lanes.main, "ls-tree", "-r", "--name-only", "main").splitlines()
    assert {"src/b/y.py", "tasks/B1.yaml"} <= set(main_files)
    assert run_git(lanes.main, "show", "main:src/app.py") == edited(10, "lane ten").strip()
    assert run_git(lanes.main, "rev-parse", "main") == run_git(lanes.origin, "rev-parse", "main")


def test_publish_by_tree_refuses_after_main_moved(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    lanes.lane("start", "--lane", "lane1", "--run", "T-01")
    _, report = lanes.lane("report", "--lane", "lane1", "--run", "T-01")
    lanes.main_moves({"docs/other.md": "m\n"})
    before = run_git(lanes.main, "rev-parse", "main")
    code, out = lanes.lane(
        "publish", "--lane", "lane1", "--run", "T-01", "--expected-tree", report["tree"]
    )
    assert code == 2 and "new tree approved" in out["error"]
    assert run_git(lanes.main, "rev-parse", "main") == before
    assert run_git(lanes.lanes["lane1"], "branch", "--show-current") == "pi/T-01"
    assert not (lanes.runs / "lanes/publish.lock").exists()


def test_publish_refuses_other_fingerprints_before_touching_anything(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    lanes.lane("start", "--lane", "lane1", "--run", "T-01")
    _, report = lanes.lane("report", "--lane", "lane1", "--run", "T-01")
    before = run_git(lanes.main, "rev-parse", "main")
    code, out = lanes.lane(
        "publish",
        "--lane",
        "lane1",
        "--run",
        "T-01",
        "--expected-change",
        "0" * 40,
        "--expected-message",
        report["fingerprints"]["message"],
    )
    assert code == 2 and "not the reviewed ones" in out["error"]
    assert run_git(lanes.main, "rev-parse", "main") == before
    assert run_git(lanes.lanes["lane1"], "branch", "--show-current") == "pi/T-01"


def test_publish_lock_blocks_other_actions(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    write(lanes.runs / "lanes/publish.lock", "lane2 X-01 pid 1")
    code, out = lanes.lane("start", "--lane", "lane1", "--run", "T-01")
    assert code == 2 and "publish is running" in out["error"]


def test_publish_refuses_while_another_publish_holds_the_lock(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    lanes.lane("start", "--lane", "lane1", "--run", "T-01")
    _, report = lanes.lane("report", "--lane", "lane1", "--run", "T-01")
    fp = report["fingerprints"]
    write(lanes.runs / "lanes/publish.lock", "lane2 X-01 pid 1")
    before = run_git(lanes.main, "rev-parse", "main")
    code, out = lanes.lane(
        "publish",
        "--lane",
        "lane1",
        "--run",
        "T-01",
        "--expected-change",
        fp["change"],
        "--expected-message",
        fp["message"],
    )
    assert code == 2 and "publish is running" in out["error"]
    assert (lanes.runs / "lanes/publish.lock").read_text(encoding="utf-8") == "lane2 X-01 pid 1"
    assert run_git(lanes.main, "rev-parse", "main") == before


@pytest.mark.parametrize("case", ["message", "no_gate", "main_dirty", "main_on_branch"])
def test_publish_refusals_before_publish_ps1(lanes: Setup, case: str) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    lanes.lane("start", "--lane", "lane1", "--run", "T-01")
    if case == "no_gate":
        fp = {"change": "a" * 40, "message": "b" * 64}
    else:
        fp = lanes.lane("report", "--lane", "lane1", "--run", "T-01")[1]["fingerprints"]
    if case == "message":
        fp = {**fp, "message": "0" * 64}
    elif case == "main_dirty":
        write(lanes.main / "src/app.py", "dirty\n")
    elif case == "main_on_branch":
        run_git(lanes.main, "switch", "-q", "-c", "elsewhere")
    problem = {
        "message": "not the reviewed ones",
        "no_gate": "no Gate trailer",
        "main_dirty": "must be on main and clean",
        "main_on_branch": "must be on main and clean",
    }[case]
    before = run_git(lanes.main, "rev-parse", "main")
    code, out = lanes.lane(
        "publish",
        "--lane",
        "lane1",
        "--run",
        "T-01",
        "--expected-change",
        fp["change"],
        "--expected-message",
        fp["message"],
    )
    assert code == 2 and problem in out["error"], out
    assert run_git(lanes.main, "rev-parse", "main") == before
    assert run_git(lanes.lanes["lane1"], "branch", "--show-current") == "pi/T-01"
    assert not (lanes.runs / "lanes/publish.lock").exists()


def test_release_frees_the_lane_and_keeps_the_branch(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    lanes.lane("start", "--lane", "lane1", "--run", "T-01")
    code, out = lanes.lane("release", "--lane", "lane1", "--run", "T-01")
    assert code == 0 and out["branch_kept"] == "pi/T-01"
    assert not (lanes.runs / "lanes/lane1.json").exists()
    assert run_git(lanes.main, "branch", "--list", "pi/T-01") != ""
    assert run_git(lanes.lanes["lane1"], "branch", "--show-current") == ""


def test_status_lists_lanes_and_their_runs(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    lanes.lane("start", "--lane", "lane1", "--run", "T-01")
    code, out = lanes.lane("status")
    assert code == 0
    assert out["lanes"]["lane1"]["state"]["run"] == "T-01"
    assert out["lanes"]["lane2"]["branch"] is None and "state" not in out["lanes"]["lane2"]
    assert out["main"] == run_git(lanes.main, "rev-parse", "main")


@pytest.mark.parametrize(
    "args",
    [
        ["start", "--lane", "lane1"],
        ["publish", "--lane", "lane1", "--run", "T-01"],
        ["publish", "--lane", "lane1", "--run", "T-01", "--expected-change", "a" * 40],
        [
            "publish",
            "--lane",
            "lane1",
            "--run",
            "T-01",
            "--expected-tree",
            "a" * 40,
            "--expected-message",
            "b" * 64,
        ],
        ["report", "--lane", "lane1", "--run", "T-01", "--expected-tree", "a" * 40],
        ["report", "--lane", "lane1", "--run", "T-01", "--fix", "x.md"],
        ["report", "--lane", "lane1", "--run", "T-01", "--with-state-root"],
        ["report", "--lane", "lane1", "--run", "T-01", "--then-report"],
    ],
)
def test_cli_argument_rules(lanes: Setup, args: list[str]) -> None:
    done = subprocess.run(
        [sys.executable, "-I", str(LANE), *args, "--config", str(lanes.config)],
        capture_output=True,
        text=True,
    )
    assert done.returncode == 2 and "error:" in done.stderr


def test_real_config_labels_windows_by_scheduler_window_and_names_its_summary_directory() -> None:
    config = json.loads((ROOT / "scripts/agent/agent_config.json").read_text(encoding="utf-8"))
    wrapper = (ROOT / "scripts/ops/daily_scheduler_run.ps1").read_text(encoding="utf-8")
    labels = [w.get("label") for w in config["daily_windows_jst"]]
    assert labels == ["PRIMARY", "RESCUE"]
    assert "$OutputDir = Join-Path $StateRoot 'outputs\\run_control\\scheduler'" in wrapper
    assert config["daily_scheduler_dir"].endswith("\\outputs\\run_control\\scheduler")
    assert "'{0}_{1}' -f $tokyoNow.ToString('yyyy-MM-dd'), $Window" in wrapper
    paths = [config["main_checkout"], *config["lanes"].values()]
    assert len(set(paths)) == len(paths) and config["main_checkout"] not in config["lanes"].values()


def test_real_config_keeps_the_rescue_window_open_after_a_passed_primary_run() -> None:
    config = json.loads((ROOT / "scripts/agent/agent_config.json").read_text(encoding="utf-8"))
    windows = {w["label"]: w for w in config["daily_windows_jst"]}
    assert windows["RESCUE"]["open_if_passed"] == "PRIMARY"
    assert "open_if_passed" not in windows["PRIMARY"]
    assert [(w["start"], w["end"]) for w in config["daily_windows_jst"]] == [
        ("11:50", "13:00"),
        ("17:20", "18:30"),
    ]
