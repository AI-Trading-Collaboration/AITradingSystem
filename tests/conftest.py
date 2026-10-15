"""Scheduling, temporary-directory cleanup and a timing summary for parallel test runs (DEVX-033).

Active only under pytest-xdist (``-n``); a plain ``pytest`` run is unchanged.

- With ``--dist loadfile`` files are queued longest first. xdist's own order is by test count, so
  slow files with few tests started last and kept one worker busy after the others were idle.
  Durations come from runs of 1000 or more tests in this checkout (pytest cache
  ``aits/file_durations``, updated per file), then ``tests/file_durations.json``, then 0.3 s
  per test.
- A file expected to run longer than half of one worker's share of the run is cut into parts
  of consecutive tests, each its own work unit, so several workers share it. Otherwise one
  long file sets the length of the run (Windows, 2026-10-15: tests/test_agent_lane.py 194 s
  while 9 of 16 workers were done at 120 s).
- Each worker deletes its own temporary directory when its tests passed. Otherwise pytest's main
  process deletes the run from three runs ago at exit, one file at a time, outside the reported
  test time (tens of thousands of files from throw-away git repositories).
- The main process prints a short timing summary before pytest's last line and stores it in the
  pytest cache as ``aits/last_gate_timing``.
"""

from __future__ import annotations

import json
import math
import os
import shutil
import stat
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pytest

SEED_PATH = Path(__file__).with_name("file_durations.json")
CACHE_DURATIONS = "aits/file_durations"
CACHE_TIMING = "aits/last_gate_timing"
SECONDS_PER_TEST = 0.3
# Files expected to take longer than this share of one worker's part of the run are split.
SPLIT_SHARE = 0.5
# Smaller runs (targeted test runs) do not replace the recorded durations.
MIN_TESTS_TO_RECORD = 1000
MIN_TESTS_TO_SUMMARIZE = 200
DURATIONS_KEY = "aits_file_durations"
CLEAN_KEY = "aits_clean_basetemp"


def file_of(nodeid: str) -> str:
    return nodeid.split("::", 1)[0]


def load_seed(path: Path = SEED_PATH) -> dict[str, float]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    files = data.get("files") if isinstance(data, dict) else None
    if not isinstance(files, dict):
        return {}
    return {str(k): float(v) for k, v in files.items() if isinstance(v, (int, float))}


def merge_durations(seed: dict[str, float], recorded: object) -> dict[str, float]:
    """``seed`` overridden by ``recorded`` (the cache value: a dict, or nothing usable)."""
    durations = dict(seed)
    if isinstance(recorded, dict):
        durations.update(
            {str(k): float(v) for k, v in recorded.items() if isinstance(v, (int, float))}
        )
    return durations


def split_plan(nodeids: Sequence[str], durations: dict[str, float], workers: int) -> dict[str, int]:
    """``{file: parts}`` for the files to split; the same answer in every process of a run."""
    counts = Counter(file_of(n) for n in nodeids)
    sizes = {f: durations.get(f, c * SECONDS_PER_TEST) for f, c in counts.items()}
    # fsum does not depend on the order of the files, which differs between processes.
    target = SPLIT_SHARE * math.fsum(sizes.values()) / max(workers, 1)
    plan: dict[str, int] = {}
    if workers < 2 or target <= 0:
        return plan
    for name, size in sizes.items():
        parts = min(math.ceil(size / target), counts[name], workers)
        if parts > 1:
            plan[name] = parts
    return plan


def part_scopes(nodeids: Sequence[str], plan: dict[str, int]) -> dict[str, str]:
    """Work unit of each test of a split file: near-equal runs of consecutive tests."""
    tests: dict[str, list[str]] = defaultdict(list)
    for nodeid in nodeids:
        if file_of(nodeid) in plan:
            tests[file_of(nodeid)].append(nodeid)
    scopes: dict[str, str] = {}
    for name, ids in tests.items():
        for index, nodeid in enumerate(ids):
            scopes[nodeid] = f"{name}::aits-part-{index * plan[name] // len(ids) + 1}"
    return scopes


def order_by_duration(
    items: list[Any], durations: dict[str, float], plan: dict[str, int] | None = None
) -> list[Any]:
    """Items grouped by file, longest file (or part) first; tests keep their order in a file."""
    first: dict[str, int] = {}
    count: dict[str, int] = defaultdict(int)
    for index, item in enumerate(items):
        name = file_of(item.nodeid)
        first.setdefault(name, index)
        count[name] += 1
    parts = plan or {}

    def key(item: Any) -> tuple[float, int]:
        name = file_of(item.nodeid)
        size = durations.get(name, count[name] * SECONDS_PER_TEST)
        return (-size / parts.get(name, 1), first[name])

    return sorted(items, key=key)


def split_scheduler(config: pytest.Config, log: Any, durations: dict[str, float]) -> Any:
    """xdist's ``loadfile`` scheduler, with the files of ``split_plan`` cut into parts."""
    from xdist.scheduler import LoadFileScheduling

    class SplitFileScheduling(LoadFileScheduling):  # type: ignore[misc]
        def __init__(self, config: pytest.Config, log: Any) -> None:
            super().__init__(config, log)
            self.plan: dict[str, int] | None = None
            self.scopes: dict[str, str] = {}

        def schedule(self) -> None:
            if self.plan is None:
                # Called once every worker has collected; the collections are identical.
                collection = next(iter(self.registered_collections.values()))
                self.plan = split_plan(collection, durations, self.numnodes)
                self.scopes = part_scopes(collection, self.plan)
            super().schedule()

        def _split_scope(self, nodeid: str) -> str:
            scope = self.scopes.get(nodeid)
            return scope if scope is not None else str(super()._split_scope(nodeid))

    return SplitFileScheduling(config, log)


def remove_tree(path: Path) -> None:
    """Best-effort delete that also removes read-only files (git objects on Windows)."""

    def retry(func: Callable[..., Any], target: str, _exc: object) -> None:
        try:
            os.chmod(target, stat.S_IRWXU)
            func(target)
        except OSError:
            pass

    if sys.version_info >= (3, 12):
        shutil.rmtree(path, onexc=retry)
    else:
        shutil.rmtree(path, onerror=retry)


class ControllerTiming:
    """Main process of an xdist run: queue order for the workers, timing summary, durations."""

    def __init__(self, config: pytest.Config) -> None:
        self.config = config
        self.started = time.monotonic()
        self.collected: float | None = None
        self.finished: float | None = None
        self.last_report: dict[str, float] = {}
        self.node_down: dict[str, float] = {}
        self.file_seconds: dict[str, float] = defaultdict(float)
        self.file_tests: dict[str, int] = defaultdict(int)
        self.durations: dict[str, float] | None = None
        self.scheduler: Any = None
        if config.getoption("dist") == "loadfile":
            cache = getattr(config, "cache", None)
            recorded = cache.get(CACHE_DURATIONS, None) if cache is not None else None
            self.durations = merge_durations(load_seed(), recorded)
            # Keep the order the workers collected (longest file first) instead of test count.
            config.option.loadscopereorder = False

    def pytest_xdist_make_scheduler(self, config: pytest.Config, log: Any) -> Any:
        if self.durations is None:
            return None
        self.scheduler = split_scheduler(config, log, self.durations)
        return self.scheduler

    def pytest_configure_node(self, node: Any) -> None:
        if self.durations is not None:
            node.workerinput[DURATIONS_KEY] = self.durations
        node.workerinput[CLEAN_KEY] = self.config.option.basetemp is None

    def pytest_xdist_node_collection_finished(self, node: Any, ids: Any) -> None:
        self.collected = time.monotonic()

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        node = getattr(report, "node", None)
        worker = node.gateway.id if node is not None else "main"
        self.last_report[worker] = time.monotonic()
        name = file_of(report.nodeid)
        self.file_seconds[name] += report.duration
        if report.when == "teardown":
            self.file_tests[name] += 1

    def pytest_testnodedown(self, node: Any, error: object) -> None:
        self.node_down[node.gateway.id] = time.monotonic()

    def tests(self) -> int:
        return sum(self.file_tests.values())

    def summary(self) -> dict[str, Any]:
        finished = self.finished if self.finished is not None else time.monotonic()
        collected = self.collected if self.collected is not None else self.started
        last = max(self.last_report.values(), default=collected)
        slowest = sorted(self.file_seconds.items(), key=lambda kv: -kv[1])[:10]
        plan = getattr(self.scheduler, "plan", None) or {}
        return {
            "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "workers": len(self.last_report),
            "tests": self.tests(),
            "start_to_collected_s": round(collected - self.started, 1),
            "tests_s": round(last - collected, 1),
            "after_tests_s": round(finished - last, 1),
            "total_s": round(finished - self.started, 1),
            "worker_done_s": {
                w: round(t - collected, 1) for w, t in sorted(self.last_report.items())
            },
            "worker_cleanup_s": round(
                max(
                    (
                        t - self.last_report[w]
                        for w, t in self.node_down.items()
                        if w in self.last_report
                    ),
                    default=0.0,
                ),
                1,
            ),
            "slowest_files": [
                {"file": f, "seconds": round(s, 1), "tests": self.file_tests[f]} for f, s in slowest
            ],
            "split_files": dict(sorted(plan.items())),
        }

    @pytest.hookimpl(trylast=True)
    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int) -> None:
        self.finished = time.monotonic()
        cache = getattr(self.config, "cache", None)
        if cache is None:
            return
        if self.tests() >= MIN_TESTS_TO_RECORD and exitstatus in (0, 1):
            # Files this run did not touch keep their earlier durations.
            measured = {f: round(s, 2) for f, s in self.file_seconds.items()}
            merged = merge_durations({}, cache.get(CACHE_DURATIONS, None)) | measured
            cache.set(CACHE_DURATIONS, dict(sorted(merged.items())))
        if self.tests() >= MIN_TESTS_TO_SUMMARIZE:
            cache.set(CACHE_TIMING, self.summary())

    def pytest_terminal_summary(self, terminalreporter: Any) -> None:
        if self.tests() < MIN_TESTS_TO_SUMMARIZE:
            return
        s = self.summary()
        write = terminalreporter.write_line
        write(
            f"gate timing: {s['workers']} workers, {s['tests']} tests; "
            f"start to collected {s['start_to_collected_s']} s, tests {s['tests_s']} s, "
            f"after tests {s['after_tests_s']} s (worker cleanup up to "
            f"{s['worker_cleanup_s']} s), total {s['total_s']} s"
        )
        done = sorted(s["worker_done_s"].values())
        write("  workers done after collection (s): " + " ".join(f"{t:.0f}" for t in done))
        write(
            "  slowest files (s): "
            + "; ".join(f"{f['file']} {f['seconds']}" for f in s["slowest_files"][:5])
        )
        if s["split_files"]:
            write(
                "  split files (parts): "
                + "; ".join(f"{f} {n}" for f, n in s["split_files"].items())
            )


class WorkerTiming:
    """xdist worker: collect in the controller's file order, delete the temporary directory."""

    def __init__(self, config: pytest.Config) -> None:
        self.config = config

    @pytest.hookimpl(trylast=True)
    def pytest_collection_modifyitems(self, items: list[pytest.Item]) -> None:
        workerinput = getattr(self.config, "workerinput", {})
        durations = workerinput.get(DURATIONS_KEY)
        if durations is not None:
            workers = int(workerinput.get("workercount", 1))
            plan = split_plan([i.nodeid for i in items], durations, workers)
            items[:] = order_by_duration(items, durations, plan)

    @pytest.hookimpl(trylast=True)
    def pytest_sessionfinish(self, exitstatus: int) -> None:
        basetemp = self.config.option.basetemp
        clean = getattr(self.config, "workerinput", {}).get(CLEAN_KEY)
        if exitstatus == 0 and clean and basetemp and Path(basetemp).name.startswith("popen-"):
            remove_tree(Path(basetemp))


def pytest_configure(config: pytest.Config) -> None:
    if not config.pluginmanager.hasplugin("xdist"):
        return
    if hasattr(config, "workerinput"):
        config.pluginmanager.register(WorkerTiming(config), "aits-worker-timing")
    elif getattr(config.option, "numprocesses", None):
        config.pluginmanager.register(ControllerTiming(config), "aits-controller-timing")


@pytest.fixture(scope="session")
def git_repo_template(
    tmp_path_factory: pytest.TempPathFactory,
) -> Callable[[Path, str, Callable[[Path], None]], Path]:
    """``make(dest, key, build)``: copy into ``dest`` the repository ``build`` made for ``key``.

    The repository is built once per process. Starting git costs far more than copying a few
    files, most of all on Windows, and the fixtures of the git-heavy tests start git 6-8 times.
    """
    built: dict[str, Path] = {}

    def make(dest: Path, key: str, build: Callable[[Path], None]) -> Path:
        template = built.get(key)
        if template is None:
            template = tmp_path_factory.mktemp("git-template")
            build(template)
            for sample in (template / ".git" / "hooks").glob("*.sample"):
                sample.unlink()
            built[key] = template
        shutil.copytree(template, dest, symlinks=True, dirs_exist_ok=True)
        return dest

    return make
