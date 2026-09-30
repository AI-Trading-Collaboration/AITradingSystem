from __future__ import annotations

import dataclasses
import heapq
import json
import os
import random
import subprocess
import sys
import types
from pathlib import Path

import pytest
import yaml

from ai_trading_system.platform.validation_scheduling import (
    EXCLUSIVE_GROUP_SCOPE_PREFIX,
    REAL_FULL_CHAIN_MARKER,
    SCHEDULING_MANIFEST_RELATIVE_PATH,
    SchedulingManifestError,
    load_scheduling_manifest,
    missing_real_full_chain_functions,
    parse_scheduling_manifest,
    split_scope_evidence_error,
)
from scripts.pytest_runtime_profile import (
    DurationProfile,
    build_runtime_profile,
    make_governed_split_scheduler,
)
from scripts.run_validation_tier import (
    TIER_SPECS,
    _runtime_profile_contract_error_from_inputs,
    build_command,
)

ROOT = Path(__file__).resolve().parents[1]


def _manifest_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": "devx_018_validation_scheduling.v1",
        "policy_id": "test_policy",
        "owner": "validation_operations",
        "version": 1,
        "status": "ACTIVE_PILOT",
        "requirement": "docs/requirements/DEVX-018_Validation_Runtime_Throughput_V1.md",
        "rationale": "test",
        "intended_effect": "test",
        "validation_evidence": "test",
        "review_condition": "test",
        "heavy_concurrency_cap": 1,
        "split_scope_files": ["tests/test_split.py"],
        "real_full_chain": {
            "marker": REAL_FULL_CHAIN_MARKER,
            "functions": ["tests/test_split.py::test_chain"],
        },
        "production_effect": "none",
    }
    payload.update(overrides)
    return payload


def _raw(payload: dict[str, object]) -> bytes:
    return yaml.safe_dump(payload, sort_keys=False).encode("utf-8")


def test_tracked_manifest_is_valid_and_every_chain_lives_in_a_split_file() -> None:
    manifest = load_scheduling_manifest(ROOT)

    assert manifest is not None
    assert manifest.heavy_concurrency_cap == 4
    groups = dict(manifest.exclusive_groups)
    assert set(groups) == {"fixed_publication_binding_job", "host_registry_view"}
    assert manifest.heavy_start_interval_seconds == 600
    assert (
        manifest.exclusive_group_of(
            "tests/test_devx015_workflow_coordination.py::"
            "test_native_registry_fixture_preserves_existing_view_and_leaves_no_new_root"
        )
        == "host_registry_view"
    )
    other = "tests/test_devx015_workflow_coordination.py::test_x"
    assert manifest.exclusive_group_of(other) is None
    for name in groups:
        assert manifest.is_heavy_scope(EXCLUSIVE_GROUP_SCOPE_PREFIX + name)
    assert not manifest.is_heavy_scope("tests/test_plain.py")
    assert manifest.real_full_chain_functions
    for key in manifest.real_full_chain_functions:
        assert manifest.is_split_file(key.split("::", 1)[0])
    assert manifest.is_real_full_chain(
        "tests/test_devx015_workflow_execution.py::test_mandatory_acceptance_actual_runner_chain[xfail]"
    )
    assert not manifest.is_real_full_chain(
        "tests/test_devx015_workflow_execution.py::test_unrelated_fast_helper"
    )


@pytest.mark.parametrize(
    "overrides",
    [
        {"unexpected": True},
        {"schema_version": "devx_018_validation_scheduling.v0"},
        {"heavy_concurrency_cap": 0},
        {"heavy_concurrency_cap": True},
        {"heavy_start_interval_seconds": -1},
        {"heavy_start_interval_seconds": True},
        {"heavy_start_interval_seconds": "600"},
        {"version": 0},
        {"status": "RETIRED"},
        {"rationale": " "},
        {"production_effect": "live"},
        {"split_scope_files": ["tests/test_z.py", "tests/test_a.py"]},
        {"split_scope_files": ["tests/test_split.py", "tests/test_split.py"]},
        {"split_scope_files": ["../outside/test_x.py"]},
        {"real_full_chain": {"marker": "other", "functions": ["tests/test_split.py::test_chain"]}},
        {"real_full_chain": {"marker": REAL_FULL_CHAIN_MARKER,
                             "functions": ["tests/test_other.py::test_chain"]}},
        {"real_full_chain": {"marker": REAL_FULL_CHAIN_MARKER,
                             "functions": ["tests/test_split.py::test_chain[param]"]}},
        {"exclusive_groups": []},
        {"exclusive_groups": {"Bad-Name": ["tests/test_split.py::test_chain"]}},
        {"exclusive_groups": {"group_a": ["tests/test_other.py::test_x"]}},
        {"exclusive_groups": {"group_a": ["tests/test_split.py::test_b",
                                          "tests/test_split.py::test_a"]}},
        {"exclusive_groups": {"group_a": ["tests/test_split.py::test_a"],
                              "group_b": ["tests/test_split.py::test_a"]}},
    ],
)
def test_manifest_rejects_unreviewed_shapes(overrides: dict[str, object]) -> None:
    with pytest.raises(SchedulingManifestError):
        parse_scheduling_manifest(_raw(_manifest_payload(**overrides)))


def test_manifest_rejects_duplicate_yaml_keys() -> None:
    raw = _raw(_manifest_payload()) + b"version: 2\n"
    with pytest.raises(SchedulingManifestError):
        parse_scheduling_manifest(raw)


def test_absent_manifest_disables_policy_and_missing_functions_are_reported(
    tmp_path: Path,
) -> None:
    assert load_scheduling_manifest(tmp_path) is None
    manifest = parse_scheduling_manifest(_raw(_manifest_payload()))
    assert missing_real_full_chain_functions(manifest, ["tests/test_split.py::test_other"]) == [
        "tests/test_split.py::test_chain"
    ]
    # A collection that does not include the file cannot prove anything missing.
    assert missing_real_full_chain_functions(manifest, ["tests/test_else.py::test_x"]) == []


def _two_worker_profile(split_scope: dict[str, object] | None) -> dict[str, object]:
    nodeids = ["tests/test_split.py::test_a", "tests/test_split.py::test_b"]
    phases = []
    for index, nodeid in enumerate(nodeids):
        for phase, start, stop in (("setup", 1.0, 1.1), ("call", 1.1, 1.2), ("teardown", 1.2, 1.3)):
            phases.append({
                "nodeid": nodeid, "phase": phase, "worker_id": f"gw{index}", "start": start,
                "stop": stop, "duration": stop - start, "outcome": "passed",
            })
    duration = DurationProfile(
        configured_path=str(ROOT / "inputs/architecture/arch_004g2_full_duration_profile.yaml"),
        manifest_sha256="0" * 64, schema_version="arch_004g2_full_duration_profile.v1",
        profile_id="test", owner="validation_operations", version=1,
        manifest_status="PARTIAL_SEED", partial_seed=True, complete_profile=False,
        source_tier="full", source_artifact_path="x", source_artifact_sha256="1" * 64,
        source_workers=16, source_dist="loadfile",
        observed_seconds={"tests/test_split.py": 1.0}, file_node_counts={},
        source_node_count=None, source_file_count=None,
        source_collection_ordered_sha256=None, source_collection_set_sha256=None,
        source_file_set_sha256=None, source_file_rows_sha256=None,
        expected_scheduled_ordered_sha256=None, source_file_duration_total_seconds=None,
        valid=True, fallback_reason=None,
    )
    return build_runtime_profile(
        collections={"gw0": nodeids, "gw1": nodeids}, phase_reports=phases,
        duration_profile=duration, expected_worker_count=2, xdist_dist="loadfile",
        loadscope_reorder=False, formal_full_selection_eligible=False, pytest_exitstatus=0,
        started_at=0.0, ended_at=2.0, split_scope=split_scope,
    )


def test_profile_contract_exempts_only_manifest_bound_split_files() -> None:
    manifest = parse_scheduling_manifest(_raw(_manifest_payload()))
    evidence = manifest.split_scope_evidence()

    legacy = _two_worker_profile(None)
    assert "spans workers" in str(
        _runtime_profile_contract_error_from_inputs(legacy, pytest_exitstatus=0)
    )

    split = _two_worker_profile(evidence)
    assert _runtime_profile_contract_error_from_inputs(split, pytest_exitstatus=0) is None
    assert _runtime_profile_contract_error_from_inputs(
        split, pytest_exitstatus=0, scheduling_manifest=manifest
    ) is None

    other = parse_scheduling_manifest(_raw(_manifest_payload(heavy_concurrency_cap=2)))
    assert "differs from the scheduling manifest" in str(
        _runtime_profile_contract_error_from_inputs(
            split, pytest_exitstatus=0, scheduling_manifest=other
        )
    )
    # A present manifest with loadfile workers requires the evidence.
    assert "missing although the manifest is present" in str(
        _runtime_profile_contract_error_from_inputs(
            legacy, pytest_exitstatus=0, scheduling_manifest=manifest
        )
    )
    tampered = _two_worker_profile({**evidence, "split_scope_files": []})
    assert "split_scope evidence is invalid" in str(
        _runtime_profile_contract_error_from_inputs(tampered, pytest_exitstatus=0)
    )
    assert split_scope_evidence_error(None, None) is None
    assert split_scope_evidence_error(evidence, None) == "split scope evidence without a manifest"


def test_only_pre_full_architecture_tier_excludes_real_full_chain() -> None:
    assert TIER_SPECS["full"].excluded_markers == ()
    assert TIER_SPECS["architecture-fitness"].excluded_markers == (REAL_FULL_CHAIN_MARKER,)
    command = build_command("architecture-fitness", python_executable="python", repo_root=ROOT)
    marker_index = command.index("-m", 3)
    assert command[marker_index + 1] == f"not {REAL_FULL_CHAIN_MARKER}"
    assert command[marker_index - 2:marker_index] == ["-p", "scripts.pytest_runtime_profile"]
    full = build_command("full", python_executable="python", repo_root=ROOT)
    assert f"not {REAL_FULL_CHAIN_MARKER}" not in full


_RECORDING_TEST = """
import json, os, time
from pathlib import Path

def _record(name, seconds):
    start = time.time()
    time.sleep(seconds)
    line = {"test": name, "worker": os.environ.get("PYTEST_XDIST_WORKER"),
            "start": start, "stop": time.time()}
    # One file per test: concurrent appends from several workers are not atomic on Windows.
    Path(os.environ["DEVX018_EVENTS"], name + ".json").write_text(json.dumps(line))
"""


def _write_fixture_project(root: Path, *, cap: int, grouped: bool | str = False) -> None:
    (root / "pytest.ini").write_text("[pytest]\n", encoding="utf-8")
    manifest = _manifest_payload(
        heavy_concurrency_cap=cap,
        split_scope_files=["tests/test_split.py"],
        real_full_chain={"marker": REAL_FULL_CHAIN_MARKER,
                         "functions": ["tests/test_split.py::test_chain"]},
    )
    if grouped == "heavy":
        manifest["exclusive_groups"] = {"shared_host_resource": [
            "tests/test_split.py::test_chain", "tests/test_split.py::test_light",
        ]}
    elif grouped:
        manifest["exclusive_groups"] = {"shared_host_resource": [
            "tests/test_split.py::test_light", "tests/test_split.py::test_other_light",
        ]}
    path = root / SCHEDULING_MANIFEST_RELATIVE_PATH
    path.parent.mkdir(parents=True)
    path.write_bytes(_raw(manifest))
    tests = root / "tests"
    tests.mkdir()
    (tests / "test_split.py").write_text(
        _RECORDING_TEST
        + "import pytest\n"
        + "@pytest.mark.parametrize('index', range(3))\n"
        + "def test_chain(index):\n    _record(f'chain{index}', 1.5)\n"
        + "@pytest.mark.parametrize('index', range(6))\n"
        + "def test_light(index):\n    _record(f'light{index}', 0.3)\n"
        + "def test_other_light():\n    _record('otherlight', 0.3)\n",
        encoding="utf-8",
    )
    (tests / "test_whole.py").write_text(
        _RECORDING_TEST
        + "import pytest\n"
        + "@pytest.mark.parametrize('index', range(3))\n"
        + "def test_whole(index):\n    _record(f'whole{index}', 0.2)\n",
        encoding="utf-8",
    )


def _run_fixture(root: Path, *extra: str) -> tuple[subprocess.CompletedProcess[str], list[dict]]:
    events = root / "events"
    events.mkdir(exist_ok=True)
    environment = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([str(ROOT), str(ROOT / "src")]),
        "DEVX018_EVENTS": str(events),
    }
    environment.pop("PYTEST_ADDOPTS", None)
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "-p", "scripts.pytest_runtime_profile",
         "-p", "no:cacheprovider", "-q", *extra, "tests"],
        cwd=root, env=environment, capture_output=True, text=True, timeout=120,
    )
    rows = [json.loads(path.read_text(encoding="utf-8")) for path in sorted(events.glob("*.json"))]
    return completed, rows


def test_real_xdist_split_scope_caps_heavy_nodes_and_keeps_loadfile_elsewhere(
    tmp_path: Path,
) -> None:
    _write_fixture_project(tmp_path, cap=1)
    completed, rows = _run_fixture(tmp_path, "-n", "3", "--dist", "loadfile")
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert len(rows) == 13

    whole_workers = {row["worker"] for row in rows if row["test"].startswith("whole")}
    assert len(whole_workers) == 1  # non-listed file keeps loadfile semantics
    split_workers = {
        row["worker"] for row in rows if row["test"].startswith(("chain", "light"))
    }
    assert len(split_workers) > 1  # listed file is distributed per node

    chain = sorted(
        (row["start"], row["stop"]) for row in rows if row["test"].startswith("chain")
    )
    assert len(chain) == 3
    for (_, previous_stop), (next_start, _) in zip(chain, chain[1:], strict=False):
        assert next_start >= previous_stop  # cap=1: never two real chains at once


def test_real_marker_exclusion_deselects_exactly_listed_chain_nodes(tmp_path: Path) -> None:
    _write_fixture_project(tmp_path, cap=1)
    completed, rows = _run_fixture(tmp_path, "-m", f"not {REAL_FULL_CHAIN_MARKER}")
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "3 deselected" in completed.stdout
    assert sorted(row["test"] for row in rows) == sorted(
        [f"light{index}" for index in range(6)] + [f"whole{index}" for index in range(3)]
        + ["otherlight"]
    )


def test_real_xdist_exclusive_group_runs_as_one_sequential_unit(tmp_path: Path) -> None:
    _write_fixture_project(tmp_path, cap=3, grouped=True)
    completed, rows = _run_fixture(tmp_path, "-n", "4", "--dist", "loadfile")
    assert completed.returncode == 0, completed.stdout + completed.stderr
    grouped = sorted(
        (row["start"], row["stop"], row["worker"])
        for row in rows
        if row["test"].startswith(("light", "otherlight"))
    )
    assert len(grouped) == 7
    assert len({worker for _, _, worker in grouped}) == 1  # one composite unit, one worker
    for (_, previous_stop, _), (next_start, _, _) in zip(grouped, grouped[1:], strict=False):
        assert next_start >= previous_stop  # one shared host resource, never two holders


def test_real_xdist_heavy_group_unit_counts_once_against_the_cap(tmp_path: Path) -> None:
    _write_fixture_project(tmp_path, cap=1, grouped="heavy")
    completed, rows = _run_fixture(tmp_path, "-n", "3", "--dist", "loadfile")
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert len(rows) == 13
    grouped = sorted(
        (row["start"], row["stop"], row["worker"])
        for row in rows
        if row["test"].startswith(("chain", "light"))
    )
    assert len(grouped) == 9
    assert len({worker for _, _, worker in grouped}) == 1
    for (_, previous_stop, _), (next_start, _, _) in zip(grouped, grouped[1:], strict=False):
        assert next_start >= previous_stop


class _FakeConfig:
    def __init__(self, workers: int) -> None:
        self.workers = workers
        self.option = types.SimpleNamespace(loadscopereorder=False)

    def getvalue(self, name: str) -> list[str]:
        assert name == "tx"
        return [f"{self.workers}*popen"]


class _NullLog:
    def __getattr__(self, name: str) -> object:
        return lambda *args, **kwargs: None

    def __call__(self, *args: object, **kwargs: object) -> None:
        return None


class _FakeNode:
    def __init__(self, name: str) -> None:
        self.name = name
        self.shutting_down = False
        self.torun: list[int] = []
        self.head: int | None = None
        self.current: int | None = None
        self.exited = False
        self.gateway = types.SimpleNamespace(id=name)

    def send_runtest_some(self, indices: list[int]) -> None:
        self.torun.extend(indices)

    def shutdown(self) -> None:
        self.shutting_down = True

    def __repr__(self) -> str:
        return self.name


class _EventSimulation:
    """xdist worker model: a worker runs its head item only once it knows the next item."""

    def __init__(
        self,
        manifest: object,
        collection: list[str],
        durations: dict[str, float],
        workers: int,
    ) -> None:
        self.now = 0.0
        self.events: list[tuple[float, int, _FakeNode, int]] = []
        self.sequence = 0
        self.collection = collection
        self.durations = durations
        self.manifest = manifest
        self.scheduler = make_governed_split_scheduler(
            _FakeConfig(workers), _NullLog(), manifest, clock=lambda: self.now
        )
        self.nodes = [_FakeNode(f"gw{index}") for index in range(workers)]
        self.running: dict[_FakeNode, str] = {}
        self.workers_of: dict[str, set[str]] = {}
        self.done = 0
        self.max_heavy_running = 0
        self.heavy_assignments: list[tuple[float, str, str, bool, bool]] = []
        original = self.scheduler._assign_work_unit

        def spy(node: _FakeNode) -> None:
            was_holder = self.scheduler._holds_heavy(node)
            before = set(self.scheduler.workqueue)
            original(node)
            for scope in before - set(self.scheduler.workqueue):
                if self.scheduler._is_heavy(scope):
                    light_left = any(
                        not self.scheduler._is_heavy(other) for other in self.scheduler.workqueue
                    )
                    self.heavy_assignments.append(
                        (self.now, node.name, scope, was_holder, light_left)
                    )

        self.scheduler._assign_work_unit = spy

    def _try_start(self, node: _FakeNode) -> None:
        while not node.exited and node.current is None:
            if node.head is None:
                if node.torun:
                    node.head = node.torun.pop(0)
                elif node.shutting_down:
                    node.exited = True
                    return
                else:
                    return
            if not (node.torun or node.shutting_down):
                return
            index = node.head
            node.head = node.torun.pop(0) if node.torun else None
            node.current = index
            nodeid = self.collection[index]
            self.running[node] = nodeid
            self.workers_of.setdefault(self.scheduler._split_scope(nodeid), set()).add(node.name)
            heavy_running = sum(
                1 for running in self.running.values() if self.manifest.is_real_full_chain(running)
            )
            self.max_heavy_running = max(self.max_heavy_running, heavy_running)
            self.sequence += 1
            heapq.heappush(
                self.events, (self.now + self.durations[nodeid], self.sequence, node, index)
            )
            return

    def run(self) -> bool:
        scheduler = self.scheduler
        for node in self.nodes:
            scheduler.add_node(node)
            scheduler.add_node_collection(node, self.collection)
        scheduler.schedule()
        for node in self.nodes:
            self._try_start(node)
        while self.events:
            self.now, _, node, index = heapq.heappop(self.events)
            node.current = None
            self.running.pop(node, None)
            self.done += 1
            scheduler.mark_test_complete(node, index, 0.0)
            for other in self.nodes:
                self._try_start(other)
        return self.done == len(self.collection)


def _stuck_report(sim: _EventSimulation) -> str:
    return f"done {sim.done}/{len(sim.collection)} queue {list(sim.scheduler.workqueue)[:8]}"


def _realistic_collection(manifest: object, seed: int) -> tuple[list[str], dict[str, float]]:
    rng = random.Random(seed)
    nodeids: list[str] = []
    durations: dict[str, float] = {}

    def add(nodeid: str, low: float, high: float) -> None:
        nodeids.append(nodeid)
        durations[nodeid] = rng.uniform(low, high)

    group_members = {key for _, members in manifest.exclusive_groups for key in members}
    for key in manifest.real_full_chain_functions:
        for param in range(rng.randint(1, 3)):
            add(f"{key}[p{param}]", 900.0, 4000.0)
    for key in sorted(group_members - set(manifest.real_full_chain_functions)):
        add(key, 30.0, 900.0)
    for file_path in manifest.split_scope_files:
        for index in range(rng.randint(4, 12)):
            add(f"{file_path}::test_light_{index}", 1.0, 60.0)
    for file_index in range(rng.randint(25, 40)):
        for index in range(rng.randint(3, 20)):
            add(f"tests/test_plain_{file_index}.py::test_case_{index}", 0.5, 120.0)
    # Collection (audited duration) order is fixed across workers; shuffle to vary the shape.
    rng.shuffle(nodeids)
    return nodeids, durations


@pytest.mark.parametrize("interval", [0, 600])
@pytest.mark.parametrize("seed", range(12))
def test_simulated_split_scheduler_never_deadlocks_and_respects_policy(
    seed: int, interval: int
) -> None:
    manifest = dataclasses.replace(
        load_scheduling_manifest(ROOT), heavy_start_interval_seconds=interval
    )
    nodeids, durations = _realistic_collection(manifest, seed)
    sim = _EventSimulation(manifest, nodeids, durations, workers=16)

    assert sim.run(), _stuck_report(sim)
    assert sim.max_heavy_running <= manifest.heavy_concurrency_cap
    for name, _ in manifest.exclusive_groups:
        # Composite unit: a group is one sequential run, so no two workers ever overlap in it.
        assert len(sim.workers_of[EXCLUSIVE_GROUP_SCOPE_PREFIX + name]) == 1, name
    holder_starts = [
        (at, scope, light_left)
        for at, _, scope, was_holder, light_left in sim.heavy_assignments
        if not was_holder
    ]
    assert holder_starts[0][1].startswith(EXCLUSIVE_GROUP_SCOPE_PREFIX)  # composites lead
    if interval:
        for (previous_at, _, _), (next_at, _, light_left) in zip(
            holder_starts, holder_starts[1:], strict=False
        ):
            if light_left:
                assert next_at - previous_at >= interval


def test_simulated_scheduler_survives_group_members_behind_capped_heavy_units() -> None:
    # v14 stall shape: many heavy single nodes plus a group whose members are heavy.
    functions = sorted(
        [f"tests/test_split.py::test_chain_{name}" for name in ("a", "b", "c")]
        + ["tests/test_split.py::test_grouped_chain"]
    )
    payload = _manifest_payload(
        heavy_concurrency_cap=2,
        real_full_chain={"marker": REAL_FULL_CHAIN_MARKER, "functions": functions},
        exclusive_groups={
            "shared_host_resource": [
                "tests/test_split.py::test_grouped_chain",
                "tests/test_split.py::test_grouped_light",
            ]
        },
    )
    manifest = parse_scheduling_manifest(_raw(payload))
    nodeids = (
        [f"tests/test_split.py::test_chain_{name}[{i}]" for name in "abc" for i in range(2)]
        + [f"tests/test_split.py::test_grouped_chain[{i}]" for i in range(2)]
        + ["tests/test_split.py::test_grouped_light"]
        + [f"tests/test_plain_{i}.py::test_x" for i in range(30)]
    )
    sim = _EventSimulation(manifest, nodeids, dict.fromkeys(nodeids, 50.0), workers=8)

    assert sim.run(), _stuck_report(sim)
    assert sim.max_heavy_running <= 2


def test_simulated_ramp_spaces_heavy_starts_with_the_injected_clock() -> None:
    functions = sorted(f"tests/test_split.py::test_chain_{index}" for index in range(6))
    payload = _manifest_payload(
        heavy_concurrency_cap=4,
        heavy_start_interval_seconds=300,
        real_full_chain={"marker": REAL_FULL_CHAIN_MARKER, "functions": functions},
    )
    manifest = parse_scheduling_manifest(_raw(payload))
    nodeids = [f"{key}[0]" for key in functions] + [
        f"tests/test_plain_{index}.py::test_x" for index in range(60)
    ]
    durations = {nodeid: 1000.0 if "chain" in nodeid else 100.0 for nodeid in nodeids}
    sim = _EventSimulation(manifest, nodeids, durations, workers=8)

    assert sim.run(), _stuck_report(sim)
    starts = [
        (at, light_left)
        for at, _, _, was_holder, light_left in sim.heavy_assignments
        if not was_holder
    ]
    assert starts[0][0] == 0.0
    assert len(starts) >= 4
    for (previous, _), (following, light_left) in zip(starts, starts[1:], strict=False):
        if light_left:
            assert following - previous >= 300
    assert sim.max_heavy_running <= 4
