"""Parallel pi lanes: run several pi batches at once, publish them one at a time (DEVX-031).

Owner decision owner_decision:DEVX-031:2026-10-14:parallel_pi_lanes. Each lane is a git worktree
of the main checkout (``lanes`` in agent_config.json) with its own .venv. A batch runs pi, its gate
and its checks in a lane; only the publish uses the main checkout. A lane is taken from ``start``
until its batch publishes (or ``release``), so the lane's state file under ``<runs_root>/lanes``
says which run, branch and allowed paths it holds.

Actions (each prints one JSON line; exit 0 ok, 1 failed step, 2 refused before doing anything):
  setup   --lane L               create the lane worktree (detached at main) and its .venv
  start   --lane L --run R [--fix FILE] [--with-state-root] [--then-report]
                                 check the run dir against SHA256SUMS, put the lane on main, run
                                 run-brief.ps1 -Direct in the lane (blocks until pi stops);
                                 --with-state-root adds -StateRoot <main> (AITS_STATE_ROOT)
                                 --then-report waits after pi finishes for the daily window and
                                 any publish (report_poll_seconds, report_wait_seconds), then
                                 reports; failures are saved in the lane state's report_error
  report  --lane L --run R       mechanical checks (main's accept_checks), gate in the lane, checks
                                 again with --gate, main's ship.py --dry-run for the fingerprints,
                                 push the branch to origin for review
  refresh --lane L --run R       main moved: ship.py --replay, gate, checks, push again
  publish --lane L --run R (--expected-tree T | --expected-patch P --expected-message M |
                            --expected-change C --expected-message M)
                                 refresh if needed (never for a tree approval), then publish.ps1 in
                                 the main checkout, delete the remote branch, free the lane
  release --lane L --run R       give the lane up without publishing (branches are kept)
  status                         lanes, their runs, main, the daily window

Rules it enforces: no two lanes hold overlapping allowed_paths (the rendered task views excepted:
``ship.py --replay`` renders them again); one local-model run at a time; no start, gate or publish
inside a closed daily-run window (daily_window.py); one publish at a time; a lane only ever moves
to main or to its own branch. It does not review anything: Claude reviews the pushed branch, and
publish.ps1 checks the reviewed tree, patch or change exactly as before.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib.util
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from types import ModuleType
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
RENDERED_VIEWS = ("docs/task_register.md", "docs/task_register_completed.md")
WILDCARDS = "*?["
BASE = "main"
# Operational polling/timeout defaults, not investment interpretation thresholds.
REPORT_POLL_SECONDS = 60
REPORT_WAIT_SECONDS = 7200


class LaneError(RuntimeError):
    """A step failed; ``code`` is the exit code (2: refused before changing anything)."""

    def __init__(self, message: str, code: int = 1, **details: Any) -> None:
        super().__init__(message)
        self.code = code
        self.details = details


def _load(name: str, path: Path) -> ModuleType:
    # python -I leaves the script directory off sys.path, so siblings load by path.
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise LaneError(f"cannot load {path}", 2)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def iso(moment: dt.datetime | None = None) -> str:
    return (moment or now_utc()).strftime("%Y-%m-%dT%H:%M:%SZ")


def run(
    argv: list[str], cwd: Path | None = None, check: bool = True, log: Path | None = None
) -> subprocess.CompletedProcess[str]:
    done = subprocess.run(
        argv,
        cwd=cwd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if log is not None:
        with log.open("a", encoding="utf-8") as handle:
            handle.write(f"\n===== {iso()} {' '.join(argv)} (exit {done.returncode}) =====\n")
            handle.write(done.stdout)
            handle.write(done.stderr)
    if check and done.returncode != 0:
        raise LaneError(
            f"{' '.join(argv[:4])}... failed with exit {done.returncode}",
            stdout_tail=done.stdout[-2000:],
            stderr_tail=done.stderr[-2000:],
        )
    return done


def git(repo: Path, *args: str, check: bool = True) -> str:
    return run(["git", "-C", str(repo), *args], check=check).stdout.strip()


def git_ok(repo: Path, *args: str) -> bool:
    return run(["git", "-C", str(repo), *args], check=False).returncode == 0


def python_of(checkout: Path) -> str:
    for rel in (".venv/Scripts/python.exe", ".venv/bin/python"):
        if (checkout / rel).exists():
            return str(checkout / rel)
    return sys.executable


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class Lanes:
    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path.resolve()
        self.config: dict[str, Any] = json.loads(config_path.read_text(encoding="utf-8-sig"))
        for key in ("runs_root", "main_checkout", "lanes"):
            if not self.config.get(key):
                raise LaneError(f"agent_config.json has no {key}", 2)
        self.main = Path(self.config["main_checkout"])
        self.runs_root = Path(self.config["runs_root"])
        self.state_dir = self.runs_root / "lanes"
        self.remote = str(self.config.get("lane_remote") or "origin")
        command = self.config.get("powershell") or ["powershell", "-NoProfile"]
        self.powershell = [command] if isinstance(command, str) else list(command)
        self.window = _load("aits_daily_window", SCRIPT_DIR / "daily_window.py")

    # ----- lanes and their state -------------------------------------------------------------
    def lane_path(self, lane: str) -> Path:
        lanes = self.config["lanes"]
        if lane not in lanes:
            raise LaneError(f"unknown lane {lane!r}; configured: {sorted(lanes)}", 2)
        return Path(lanes[lane])

    def state_path(self, lane: str) -> Path:
        return self.state_dir / f"{lane}.json"

    def read_state(self, lane: str) -> dict[str, Any] | None:
        path = self.state_path(lane)
        if not path.exists():
            return None
        data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        return data

    def write_state(self, lane: str, state: dict[str, Any]) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        state["updated_at"] = iso()
        tmp = self.state_path(lane).with_suffix(".tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        os.replace(tmp, self.state_path(lane))

    def held_state(self, lane: str, run_id: str) -> dict[str, Any]:
        state = self.read_state(lane)
        if state is None or state.get("run") != run_id:
            held = state.get("run") if state else None
            raise LaneError(f"lane {lane} does not hold run {run_id} (holds {held})", 2)
        return state

    def run_dir(self, run_id: str) -> Path:
        if not run_id or any(c in run_id for c in "/\\:") or run_id.startswith("."):
            raise LaneError(f"bad run name {run_id!r}", 2)
        return self.runs_root / run_id

    # ----- guards ----------------------------------------------------------------------------
    def window_open(self) -> dict[str, Any]:
        state: dict[str, Any] = self.window.window_state(self.config, now_utc())
        if state["blocked"]:
            raise LaneError(
                f"daily-run window {state.get('window')}: {state.get('reason')}", 2, window=state
            )
        return state

    def publish_lock(self) -> Path:
        return self.state_dir / "publish.lock"

    def no_publish_running(self) -> None:
        lock = self.publish_lock()
        if lock.exists():
            raise LaneError(f"a publish is running ({lock.read_text(encoding='utf-8')})", 2)

    def check_lane_checkout(self, lane: str, path: Path) -> None:
        if not (path / ".git").exists():
            raise LaneError(f"lane {lane} has no worktree at {path}: run setup", 2)
        common = Path(git(path, "rev-parse", "--path-format=absolute", "--git-common-dir"))
        main_git = Path(git(self.main, "rev-parse", "--path-format=absolute", "--git-common-dir"))
        if common.resolve() != main_git.resolve():
            raise LaneError(f"{path} is not a worktree of {self.main}", 2)

    def dirty(self, checkout: Path) -> list[str]:
        exclusions = self.exclusions()
        spec = [".", *[f":(exclude,literal){p}" for p in exclusions]]
        out = git(checkout, "status", "--porcelain=v1", "--untracked-files=normal", "--", *spec)
        return [line for line in out.splitlines() if line.strip()]

    def exclusions(self) -> list[str]:
        policy = self.main_module("ship").load_policy_from_ref(self.main, BASE)
        return list(policy.get("tree_clean_exclusions", []))

    # ----- main's own tools, never the branch's ------------------------------------------------
    def main_tools(self) -> Path:
        if not hasattr(self, "_tools"):
            self._tools = Path(tempfile.mkdtemp(prefix="aits-lane-"))
            for rel in (
                "tools/gov008/ship.py",
                "scripts/agent/accept_checks.py",
                "scripts/agent/brief.py",
            ):
                blob = subprocess.run(
                    ["git", "-C", str(self.main), "show", f"{BASE}:{rel}"],
                    capture_output=True,
                    check=False,
                )
                if blob.returncode != 0:
                    raise LaneError(f"git show {BASE}:{rel} failed", 2)
                (self._tools / Path(rel).name).write_bytes(blob.stdout)
        return self._tools

    def main_module(self, name: str) -> ModuleType:
        cache = f"_module_{name}"
        if not hasattr(self, cache):
            path = self.main_tools() / {"ship": "ship.py"}[name]
            setattr(self, cache, _load(f"aits_main_{name}", path))
        module: ModuleType = getattr(self, cache)
        return module

    def cleanup(self) -> None:
        if hasattr(self, "_tools"):
            shutil.rmtree(self._tools, ignore_errors=True)

    def ship(self, lane_path: Path, *args: str, log: Path | None = None) -> dict[str, Any]:
        """main's ship.py run in the lane with the lane's python."""
        done = run(
            [
                python_of(lane_path),
                "-I",
                str(self.main_tools() / "ship.py"),
                "--repo",
                str(lane_path),
                "--base",
                BASE,
                *args,
            ],
            check=False,
            log=log,
        )
        return self._json_result(done, f"ship.py {' '.join(args)}")

    def branch_gate(self, lane_path: Path, log: Path) -> dict[str, Any]:
        """The gate as the main checkout ran it: the branch's own ship.py --gate."""
        done = run(
            [
                python_of(lane_path),
                str(lane_path / "tools/gov008/ship.py"),
                "--repo",
                str(lane_path),
                "--base",
                BASE,
                "--gate",
            ],
            check=False,
            log=log,
        )
        return self._json_result(done, "gate")

    @staticmethod
    def _json_result(done: subprocess.CompletedProcess[str], what: str) -> dict[str, Any]:
        lines = [line for line in done.stdout.splitlines() if line.strip().startswith("{")]
        if done.returncode != 0 or not lines:
            raise LaneError(
                f"{what} failed with exit {done.returncode}",
                stdout_tail=done.stdout[-3000:],
                stderr_tail=done.stderr[-3000:],
            )
        result: dict[str, Any] = json.loads(lines[-1])
        return result

    def accept_checks(
        self, lane_path: Path, branch: str, run_dir: Path, gate: bool
    ) -> dict[str, Any]:
        argv = [
            python_of(lane_path),
            "-I",
            str(self.main_tools() / "accept_checks.py"),
            "--repo",
            str(lane_path),
            "--head",
            branch,
            "--run-dir",
            str(run_dir),
        ]
        if gate:
            argv.append("--gate")
        done = run(argv, check=False, log=run_dir / "lane.log")
        try:
            result: dict[str, Any] = json.loads(done.stdout)
        except json.JSONDecodeError:
            result = {"checks": [], "stdout_tail": done.stdout[-2000:]}
        failed = [c.get("name") for c in result.get("checks", []) if not c.get("ok")]
        if done.returncode != 0 or failed:
            raise LaneError(
                "accept_checks failed" + (" (--gate)" if gate else ""),
                failed=failed,
                stderr_tail=done.stderr[-2000:],
                checks=result.get("checks"),
            )
        return {"ok": True, "gate": gate, "checks": len(result.get("checks", []))}

    def brief(self, brief_path: Path, lane_path: Path) -> dict[str, Any]:
        done = run(
            [
                python_of(lane_path),
                "-I",
                str(self.main_tools() / "brief.py"),
                "show",
                str(brief_path),
                "--repo",
                str(self.main),
            ],
            check=False,
        )
        if done.returncode != 0:
            raise LaneError("the brief is invalid", 2, errors=done.stderr.strip().splitlines())
        data: dict[str, Any] = json.loads(done.stdout)
        return data

    # ----- actions ---------------------------------------------------------------------------
    def setup(self, lane: str) -> dict[str, Any]:
        path = self.lane_path(lane)
        if path.exists():
            raise LaneError(f"{path} already exists", 2)
        cfg = self.main / ".venv" / "pyvenv.cfg"
        home = next(
            (
                line.split("=", 1)[1].strip()
                for line in cfg.read_text(encoding="utf-8").splitlines()
                if line.split("=", 1)[0].strip() == "home"
            ),
            None,
        )
        if not home:
            raise LaneError(f"no home line in {cfg}", 2)
        base_python = next(
            (
                str(Path(home) / n)
                for n in ("python.exe", "python3", "python")
                if (Path(home) / n).exists()
            ),
            None,
        )
        if base_python is None:
            raise LaneError(f"no base python in {home}", 2)
        main_python = python_of(self.main)
        main_sha = git(self.main, "rev-parse", BASE)
        git(self.main, "worktree", "add", "--detach", str(path), BASE)
        frozen = run([main_python, "-I", "-m", "pip", "freeze", "--exclude-editable"]).stdout
        venv = path / ".venv"
        run([base_python, "-I", "-m", "venv", str(venv)])
        python = python_of(path)
        with tempfile.TemporaryDirectory(prefix="aits-lane-venv-") as tmp:
            requirements = Path(tmp) / "requirements.txt"
            requirements.write_text(frozen, encoding="utf-8")
            run(
                [
                    python,
                    "-I",
                    "-m",
                    "pip",
                    "install",
                    "--disable-pip-version-check",
                    "-r",
                    str(requirements),
                ]
            )
        after = run([python, "-I", "-m", "pip", "freeze", "--exclude-editable"]).stdout
        purelib = run(
            [python, "-I", "-c", "import sysconfig; print(sysconfig.get_paths()['purelib'])"]
        ).stdout.strip()
        (Path(purelib) / f"aits_{lane}_src.pth").write_text(f"{path / 'src'}\n", encoding="utf-8")
        imported = run(
            [python, "-I", "-c", "import ai_trading_system; print(ai_trading_system.__file__)"]
        ).stdout.strip()
        differences = sorted(set(frozen.splitlines()) ^ set(after.splitlines()))
        result: dict[str, Any] = {
            "lane": lane,
            "path": str(path),
            "head": git(path, "rev-parse", "HEAD"),
            "main": main_sha,
            "packages": len(after.splitlines()),
            "package_differences": differences,
            "imports_from": imported,
            "dirty": self.dirty(path),
        }
        ok = (
            result["head"] == main_sha
            and not differences
            and Path(imported).resolve().is_relative_to((path / "src").resolve())
            and not result["dirty"]
        )
        if not ok:
            raise LaneError("the new lane does not check out", **result)
        return result

    def check_run_dir(self, run_dir: Path) -> list[str]:
        sums = run_dir / "SHA256SUMS"
        if not (run_dir / "brief.md").exists() or not sums.exists():
            raise LaneError(f"{run_dir} needs brief.md and SHA256SUMS", 2)
        listed: dict[str, str] = {}
        for line in sums.read_text(encoding="utf-8-sig").splitlines():
            if not line.strip():
                continue
            digest, _, name = line.strip().partition(" ")
            listed[name.strip().lstrip("*")] = digest.lower()
        if "brief.md" not in listed:
            raise LaneError("SHA256SUMS does not list brief.md", 2)
        wrong = [
            name
            for name, digest in listed.items()
            if not (run_dir / name).is_file() or sha256(run_dir / name) != digest
        ]
        if wrong:
            raise LaneError(f"files do not match SHA256SUMS: {wrong}", 2)
        return sorted(listed)

    def overlaps(self, lane: str, allowed: list[str]) -> list[dict[str, Any]]:
        found = []
        # shared_paths: files every batch adds a line to (docs/system_flow.md ...). Two batches may
        # both name such a file exactly; ship.py --replay merges them or refuses a real conflict.
        shared = {str(p) for p in self.config.get("shared_paths") or []}
        for other in self.config["lanes"]:
            state = self.read_state(other) if other != lane else None
            if state is None:
                continue
            pairs = [
                [mine, theirs]
                for mine in allowed
                for theirs in state.get("allowed_paths", [])
                if globs_overlap(mine, theirs) and not (mine == theirs and mine in shared)
            ]
            if pairs:
                found.append({"lane": other, "run": state.get("run"), "paths": pairs[:5]})
        return found

    def local_model_busy(self, lane: str) -> list[str]:
        busy = []
        for other in self.config["lanes"]:
            state = self.read_state(other) if other != lane else None
            if (
                state
                and state.get("model") == "local"
                and state.get("phase") == "running"
                and pid_alive(int(state.get("pid") or 0))
            ):
                busy.append(f"{other}:{state.get('run')}")
        return busy

    def start(
        self, lane: str, run_id: str, fix: Path | None, state_root: bool = False
    ) -> dict[str, Any]:
        path = self.lane_path(lane)
        run_dir = self.run_dir(run_id)
        self.check_lane_checkout(lane, path)
        self.no_publish_running()
        self.window_open()
        files = self.check_run_dir(run_dir)
        brief = self.brief(run_dir / "brief.md", path)
        if brief.get("run") != run_id:
            raise LaneError(f"the brief is for run {brief.get('run')}, not {run_id}", 2)
        state = self.read_state(lane)
        if fix is None:
            if state is not None:
                raise LaneError(f"lane {lane} holds run {state.get('run')}", 2)
            if self.dirty(path):
                raise LaneError(f"lane {lane} is not clean: {self.dirty(path)[:5]}", 2)
            if git(path, "branch", "--show-current"):
                raise LaneError(f"lane {lane} is on a branch; it must be detached", 2)
        else:
            if state is None or state.get("run") != run_id:
                raise LaneError(f"--fix needs lane {lane} to hold run {run_id}", 2)
            if state.get("phase") == "running" and pid_alive(int(state.get("pid") or 0)):
                raise LaneError(f"run {run_id} is still running in lane {lane}", 2)
            if not fix.is_file():
                raise LaneError(f"no fix prompt at {fix}", 2)
        allowed = [str(p) for p in brief.get("allowed_paths") or []]
        clashes = self.overlaps(lane, allowed)
        if clashes:
            raise LaneError("allowed_paths overlap a batch in another lane", 2, overlaps=clashes)
        model = str(brief.get("model"))
        if model == "local" and self.local_model_busy(lane):
            raise LaneError("the local model is busy", 2, busy=self.local_model_busy(lane))
        if fix is None:
            git(path, "switch", "--detach", BASE)
            state = {
                "lane": lane,
                "run": run_id,
                "task": brief.get("task"),
                "zone": brief.get("zone"),
                "model": model,
                "allowed_paths": allowed,
                "files": files,
                "started_at": iso(),
                "base_at_start": git(path, "rev-parse", "HEAD"),
            }
        assert state is not None
        state.update({"phase": "running", "pid": os.getpid(), "attempt_started_at": iso()})
        self.write_state(lane, state)
        argv = [
            *self.powershell,
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(self.main / "scripts/agent/run-brief.ps1"),
            str(run_dir / "brief.md"),
            "-Direct",
            "-Repo",
            str(path),
        ]
        if state_root:
            # AITS_STATE_ROOT sends the pi process's own tests to the main checkout's data; only a
            # batch that must read research data from main asks for it (--with-state-root).
            argv += ["-StateRoot", str(self.main)]
        if fix is not None:
            argv += ["-Fix", str(fix)]
        done = run(argv, check=False, log=run_dir / "lane.log")
        if done.returncode != 0 and not (run_dir / "attempts").is_dir() and fix is None:
            # run-brief.ps1 stopped before pi started (no attempt to resume): free the lane so the
            # same start line can be run again once the cause is fixed.
            if not self.dirty(path):
                git(path, "switch", "--detach", BASE)
            self.state_path(lane).unlink(missing_ok=True)
            raise LaneError(
                "run-brief.ps1 stopped before pi started; the lane is free again",
                run_brief_exit=done.returncode,
                tail=(done.stdout + done.stderr).strip().splitlines()[-6:],
            )
        branch = git(path, "branch", "--show-current")
        state.update(
            {
                "phase": "stopped",
                "pid": None,
                "run_brief_exit": done.returncode,
                "branch": branch or state.get("branch"),
                "stopped_at": iso(),
            }
        )
        self.write_state(lane, state)
        result: dict[str, Any] = {
            "lane": lane,
            "run": run_id,
            "run_brief_exit": done.returncode,
            "branch": branch,
            "commits": git(path, "rev-list", "--count", f"{BASE}..HEAD") if branch else "0",
            "tail": done.stdout.strip().splitlines()[-6:],
        }
        if done.returncode != 0:
            raise LaneError("run-brief.ps1 did not finish cleanly", **result)
        return result

    def start_then_report(
        self, lane: str, run_id: str, fix: Path | None, state_root: bool = False
    ) -> dict[str, Any]:
        started = self.start(lane, run_id, fix, state_root)
        try:
            self.config = json.loads(self.config_path.read_text(encoding="utf-8-sig"))
            self._wait_for_report()
            reported = self.report(lane, run_id)
        except LaneError as exc:
            state = self.read_state(lane)
            if state is not None and state.get("run") == run_id:
                state["report_error"] = str(exc)
                self.write_state(lane, state)
            raise LaneError(
                "pi finished; report did not run or failed: " + str(exc),
                **started,
                report_error={"error": str(exc), **exc.details},
            ) from exc
        return {**started, "report": reported}

    def _report_seconds(self, key: str, default: int) -> int | float:
        value = self.config.get(key, default)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or (isinstance(value, float) and not math.isfinite(value))
            or value < 0
        ):
            raise LaneError(f"agent_config.json {key} must be a number >= 0, not {value!r}")
        return value

    def _wait_for_report(self) -> None:
        interval = max(1, self._report_seconds("report_poll_seconds", REPORT_POLL_SECONDS))
        wait = self._report_seconds("report_wait_seconds", REPORT_WAIT_SECONDS)
        began = time.monotonic()
        tries = 0
        while True:
            tries += 1
            try:
                self.no_publish_running()
                self.window_open()
                return
            except LaneError as exc:
                if time.monotonic() - began + interval > wait:
                    raise LaneError(
                        f"report not allowed after waiting {wait:g} s: {exc}",
                        **{**exc.details, "tries": tries},
                    ) from exc
            time.sleep(interval)

    def _branch_of(self, lane: str, path: Path, state: dict[str, Any]) -> str:
        branch = git(path, "branch", "--show-current")
        if not branch or branch == BASE or branch != (state.get("branch") or branch):
            raise LaneError(
                f"lane {lane} is not on its run's branch (on {branch or 'detached'})", 2
            )
        if self.dirty(path):
            raise LaneError(f"lane {lane} is not clean: {self.dirty(path)[:5]}", 2)
        return branch

    def _checks_gate_push(
        self, lane: str, path: Path, branch: str, run_dir: Path, state: dict[str, Any]
    ) -> dict[str, Any]:
        log = run_dir / "lane.log"
        if git(path, "rev-list", "--count", f"{BASE}..HEAD") != "1":
            raise LaneError("a lane branch publishes exactly one commit on main", 2)
        first = self.accept_checks(path, branch, run_dir, gate=False)
        gate = self.branch_gate(path, log)
        second = self.accept_checks(path, branch, run_dir, gate=True)
        plan = self.ship(path, "--dry-run", log=log)
        if not plan.get("ok"):
            raise LaneError("ship.py --dry-run reports problems", problems=plan.get("problems"))
        commit = git(path, "rev-parse", "HEAD")
        run(
            [
                "git",
                "-C",
                str(path),
                "push",
                "--force-with-lease",
                self.remote,
                f"{branch}:refs/heads/{branch}",
            ],
            log=log,
        )
        result = {
            "lane": lane,
            "run": run_dir.name,
            "branch": branch,
            "base": git(path, "rev-parse", BASE),
            "commit": commit,
            "tree": plan["tree"],
            "zone": plan["zone"],
            "paths_by_zone": plan["paths_by_zone"],
            "owner_decisions": plan["owner_decisions"],
            "fingerprints": plan.get("fingerprints"),
            "gate_seconds": gate["gate"]["seconds"],
            "accept_checks": [first, second],
            "pushed": f"{self.remote}/{branch} = {commit}",
        }
        state.update(
            {
                "phase": "reported",
                "branch": branch,
                "commit": commit,
                "tree": plan["tree"],
                "fingerprints": plan.get("fingerprints"),
                "reported_at": iso(),
            }
        )
        state.pop("report_error", None)
        self.write_state(lane, state)
        (run_dir / "lane-report.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        return result

    def report(self, lane: str, run_id: str) -> dict[str, Any]:
        path = self.lane_path(lane)
        run_dir = self.run_dir(run_id)
        state = self.held_state(lane, run_id)
        if state.get("phase") == "running" and pid_alive(int(state.get("pid") or 0)):
            raise LaneError(f"run {run_id} is still running", 2)
        self.no_publish_running()
        branch = self._branch_of(lane, path, state)
        if not git_ok(path, "merge-base", "--is-ancestor", BASE, "HEAD"):
            raise LaneError("main moved since the run started: use refresh", 2)
        self.window_open()
        return self._checks_gate_push(lane, path, branch, run_dir, state)

    def refresh(self, lane: str, run_id: str, publishing: bool = False) -> dict[str, Any]:
        path = self.lane_path(lane)
        run_dir = self.run_dir(run_id)
        state = self.held_state(lane, run_id)
        if not publishing:
            self.no_publish_running()
        branch = self._branch_of(lane, path, state)
        self.window_open()
        replayed = None
        if not git_ok(path, "merge-base", "--is-ancestor", BASE, "HEAD"):
            replayed = self.ship(path, "--replay", log=run_dir / "lane.log")
        result = self._checks_gate_push(lane, path, branch, run_dir, state)
        result["replayed"] = replayed
        return result

    def publish(self, lane: str, run_id: str, expected: dict[str, str]) -> dict[str, Any]:
        path = self.lane_path(lane)
        run_dir = self.run_dir(run_id)
        state = self.held_state(lane, run_id)
        branch = self._branch_of(lane, path, state)
        self.window_open()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        lock = self.publish_lock()
        try:
            fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise LaneError(
                f"a publish is running ({lock.read_text(encoding='utf-8')})", 2
            ) from exc
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(f"{lane} {run_id} pid {os.getpid()} since {iso()}")
        try:
            return self._publish_locked(lane, path, run_dir, state, branch, expected)
        finally:
            lock.unlink(missing_ok=True)

    def _publish_locked(
        self,
        lane: str,
        path: Path,
        run_dir: Path,
        state: dict[str, Any],
        branch: str,
        expected: dict[str, str],
    ) -> dict[str, Any]:
        log = run_dir / "lane.log"
        refreshed = None
        if not git_ok(path, "merge-base", "--is-ancestor", BASE, "HEAD"):
            if "tree" in expected:
                raise LaneError(
                    "main moved since this tree was approved: run refresh and have the new tree "
                    "approved",
                    2,
                )
            refreshed = self.refresh(lane, run_dir.name, publishing=True)
        if not git(path, "log", "-1", "--format=%(trailers:key=Gate,valueonly)"):
            raise LaneError("the branch has no Gate trailer: run report first", 2)
        if git(self.main, "branch", "--show-current") != BASE or self.dirty(self.main):
            raise LaneError("the main checkout must be on main and clean", 2)
        if "tree" not in expected:
            plan = self.ship(path, "--dry-run", log=log)
            kind = "change" if "change" in expected else "patch"
            got = (plan.get("fingerprints") or {}).get(kind)
            message = (plan.get("fingerprints") or {}).get("message")
            if got != expected[kind] or message != expected["message"]:
                raise LaneError(
                    f"the branch's {kind}/message are not the reviewed ones",
                    2,
                    branch={kind: got, "message": message},
                    reviewed=expected,
                )
        git(path, "switch", "--detach")
        argv = [
            *self.powershell,
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(self.main / "scripts/agent/publish.ps1"),
            "-Branch",
            branch,
            "-RunDir",
            str(run_dir),
        ]
        names = {
            "tree": "-ExpectedTree",
            "patch": "-ExpectedPatch",
            "change": "-ExpectedChange",
            "message": "-ExpectedMessage",
        }
        for key in ("tree", "patch", "change", "message"):
            if key in expected:
                argv += [names[key], expected[key]]
        argv += ["-Repo", str(self.main)]
        done = run(argv, check=False, log=log)
        publish_json = run_dir / "publish.json"
        published = (
            json.loads(publish_json.read_text(encoding="utf-8")) if publish_json.exists() else None
        )
        if done.returncode != 0:
            if git_ok(path, "rev-parse", "--verify", "--quiet", f"refs/heads/{branch}"):
                git(path, "switch", branch)
            raise LaneError(
                f"publish.ps1 exit {done.returncode}",
                publish=published,
                stdout_tail=done.stdout[-2000:],
            )
        remote_deleted = git_ok(self.main, "push", self.remote, "--delete", branch)
        git(path, "switch", "--detach", BASE)
        state.update({"phase": "published", "published": published, "published_at": iso()})
        (run_dir / "lane-state.json").write_text(
            json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        self.state_path(lane).unlink()
        return {
            "lane": lane,
            "run": run_dir.name,
            "published": published,
            "main": git(self.main, "rev-parse", BASE),
            "remote_branch_deleted": remote_deleted,
            "refreshed": refreshed is not None,
            "lane_head": git(path, "rev-parse", "HEAD"),
        }

    def release(self, lane: str, run_id: str) -> dict[str, Any]:
        path = self.lane_path(lane)
        state = self.held_state(lane, run_id)
        if state.get("phase") == "running" and pid_alive(int(state.get("pid") or 0)):
            raise LaneError(f"run {run_id} is still running", 2)
        branch = git(path, "branch", "--show-current")
        moved = False
        if not self.dirty(path):
            git(path, "switch", "--detach", BASE)
            moved = True
        state.update({"phase": "released", "released_at": iso()})
        run_dir = self.run_dir(run_id)
        if run_dir.is_dir():
            (run_dir / "lane-state.json").write_text(
                json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
            )
        self.state_path(lane).unlink()
        return {"lane": lane, "run": run_id, "branch_kept": branch, "lane_on_main": moved}

    def status(self) -> dict[str, Any]:
        lanes = {}
        for lane, raw in self.config["lanes"].items():
            path = Path(raw)
            entry: dict[str, Any] = {"path": str(path), "exists": (path / ".git").exists()}
            if entry["exists"]:
                entry["head"] = git(path, "rev-parse", "HEAD")
                entry["branch"] = git(path, "branch", "--show-current") or None
                entry["dirty"] = len(self.dirty(path))
            state = self.read_state(lane)
            if state is not None:
                entry["state"] = {
                    k: state.get(k)
                    for k in ("run", "phase", "model", "branch", "commit", "tree", "fingerprints")
                }
                entry["state"]["pid_alive"] = pid_alive(int(state.get("pid") or 0))
                if "report_error" in state:
                    entry["state"]["report_error"] = state["report_error"]
            lanes[lane] = entry
        lock = self.publish_lock()
        return {
            "main": git(self.main, "rev-parse", BASE),
            "main_checkout_branch": git(self.main, "branch", "--show-current"),
            "publish_running": lock.read_text(encoding="utf-8") if lock.exists() else None,
            "window": self.window.window_state(self.config, now_utc()),
            "lanes": lanes,
        }


def literal_prefix(glob: str) -> str:
    cut = min((glob.index(c) for c in WILDCARDS if c in glob), default=len(glob))
    return glob[:cut]


def globs_overlap(a: str, b: str) -> bool:
    """Conservative: two allowed_paths globs may name a common file. Rendered views never count."""
    if a in RENDERED_VIEWS or b in RENDERED_VIEWS:
        return False
    pa, pb = literal_prefix(a), literal_prefix(b)
    exact_a, exact_b = pa == a, pb == b
    if exact_a and exact_b:
        return a == b
    if exact_a:
        return a.startswith(pb)
    if exact_b:
        return b.startswith(pa)
    return pa.startswith(pb) or pb.startswith(pa)


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined,unused-ignore]
        handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            return False
        try:
            code = ctypes.c_ulong()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                return False
            return code.value == 259  # STILL_ACTIVE
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "action", choices=["setup", "start", "report", "refresh", "publish", "release", "status"]
    )
    parser.add_argument("--config", type=Path, default=SCRIPT_DIR / "agent_config.json")
    parser.add_argument("--lane")
    parser.add_argument("--run")
    parser.add_argument("--fix", type=Path)
    parser.add_argument("--with-state-root", action="store_true")
    parser.add_argument("--then-report", action="store_true")
    parser.add_argument("--expected-tree")
    parser.add_argument("--expected-patch")
    parser.add_argument("--expected-change")
    parser.add_argument("--expected-message")
    args = parser.parse_args(argv)
    needs = {"setup": ("lane",), "status": ()}.get(args.action, ("lane", "run"))
    missing = [f"--{n}" for n in needs if not getattr(args, n)]
    if missing:
        parser.error(f"{args.action} needs {' '.join(missing)}")
    if args.fix is not None and args.action != "start":
        parser.error("--fix goes with start")
    if args.with_state_root and args.action != "start":
        parser.error("--with-state-root goes with start")
    if args.then_report and args.action != "start":
        parser.error("--then-report goes with start")
    expected = {
        k: v
        for k, v in {
            "tree": args.expected_tree,
            "patch": args.expected_patch,
            "change": args.expected_change,
            "message": args.expected_message,
        }.items()
        if v
    }
    if args.action == "publish":
        names = [k for k in ("tree", "patch", "change") if k in expected]
        if len(names) != 1 or (names != ["tree"]) != ("message" in expected):
            parser.error(
                "publish needs --expected-tree, or --expected-patch or --expected-change with "
                "--expected-message"
            )
    elif expected:
        parser.error("--expected-* go with publish")
    lanes = None
    try:
        lanes = Lanes(args.config)
        if args.action == "setup":
            result = lanes.setup(args.lane)
        elif args.action == "start":
            if args.then_report:
                result = lanes.start_then_report(
                    args.lane, args.run, args.fix, args.with_state_root
                )
            else:
                result = lanes.start(args.lane, args.run, args.fix, args.with_state_root)
        elif args.action == "report":
            result = lanes.report(args.lane, args.run)
        elif args.action == "refresh":
            result = lanes.refresh(args.lane, args.run)
        elif args.action == "publish":
            result = lanes.publish(args.lane, args.run, expected)
        elif args.action == "release":
            result = lanes.release(args.lane, args.run)
        else:
            result = lanes.status()
        print(json.dumps({"ok": True, "action": args.action, **result}, ensure_ascii=False))
        return 0
    except LaneError as exc:
        print(
            json.dumps(
                {"ok": False, "action": args.action, "error": str(exc), **exc.details},
                ensure_ascii=False,
            )
        )
        return exc.code
    finally:
        if lanes is not None:
            lanes.cleanup()


if __name__ == "__main__":
    sys.exit(main())
