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
# script under PowerShell when there is one.
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
    if pi.get("exit"):
        sys.exit(pi["exit"])
    if "-Fix" not in rest:
        git(repo, "switch", "-c", brief["branch"])
    for rel, text in pi["files"].items():
        path = repo / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
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


# ----- lanes ------------------------------------------------------------------------------------
def test_start_runs_pi_in_the_lane_and_holds_it(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    code, out = lanes.lane("start", "--lane", "lane1", "--run", "T-01")
    assert code == 0, out
    assert out["branch"] == "pi/T-01" and out["commits"] == "1"
    call = json.loads((lanes.runs / "T-01/pi-calls.jsonl").read_text().splitlines()[0])
    assert call[call.index("-Repo") + 1] == str(lanes.lanes["lane1"])
    assert call[call.index("-StateRoot") + 1] == str(lanes.main)
    assert "-Direct" in call
    state = json.loads((lanes.runs / "lanes/lane1.json").read_text())
    assert state["run"] == "T-01" and state["phase"] == "stopped" and state["branch"] == "pi/T-01"
    assert run_git(lanes.main, "rev-parse", "main") == state["base_at_start"]


def test_two_lanes_run_side_by_side_unless_their_paths_overlap(lanes: Setup) -> None:
    lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**", "docs/task_register.md"])
    lanes.batch("T-02", {"src/b/y.py": "y\n"}, ["src/b/**", "docs/task_register.md"])
    lanes.batch("T-03", {"src/a/z.py": "z\n"}, ["src/a/z.py"])
    assert lanes.lane("start", "--lane", "lane1", "--run", "T-01")[0] == 0
    code, out = lanes.lane("start", "--lane", "lane2", "--run", "T-03")
    assert code == 2 and out["overlaps"][0]["run"] == "T-01"
    assert not (lanes.runs / "T-03/pi-calls.jsonl").exists()
    assert lanes.lane("start", "--lane", "lane2", "--run", "T-02")[0] == 0


@pytest.mark.parametrize("case", ["sums", "run_name", "held", "on_branch", "dirty", "window"])
def test_start_refusals_change_nothing(lanes: Setup, case: str) -> None:
    run_dir = lanes.batch("T-01", {"src/a/x.py": "x\n"}, ["src/a/**"])
    lane1 = lanes.lanes["lane1"]
    problem = {
        "sums": "SHA256SUMS",
        "run_name": "is for run",
        "held": "holds run",
        "on_branch": "on a branch",
        "dirty": "not clean",
        "window": "daily-run window",
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
    else:
        lanes.settings["daily_windows_jst"] = [{"start": "00:00", "end": "23:59"}]
        lanes.save()
    code, out = lanes.lane("start", "--lane", "lane1", "--run", "T-01")
    assert code == 2 and problem in out["error"], out
    assert not (run_dir / "pi-calls.jsonl").exists()


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
    assert "$OutputDir = Join-Path $RuntimeRoot 'outputs\\run_control\\scheduler'" in wrapper
    assert config["daily_scheduler_dir"].endswith("\\outputs\\run_control\\scheduler")
    assert "'{0}_{1}' -f $tokyoNow.ToString('yyyy-MM-dd'), $Window" in wrapper
    paths = [config["main_checkout"], *config["lanes"].values()]
    assert len(set(paths)) == len(paths) and config["main_checkout"] not in config["lanes"].values()
