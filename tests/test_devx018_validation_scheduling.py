from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from ai_trading_system.platform.validation_scheduling import (
    REAL_FULL_CHAIN_MARKER,
    SCHEDULING_MANIFEST_RELATIVE_PATH,
    SchedulingManifestError,
    load_scheduling_manifest,
    missing_real_full_chain_functions,
    parse_scheduling_manifest,
    split_scope_evidence_error,
)
from scripts.pytest_runtime_profile import DurationProfile, build_runtime_profile
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
    assert manifest.heavy_concurrency_cap == 6
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


def _write_fixture_project(root: Path, *, cap: int) -> None:
    (root / "pytest.ini").write_text("[pytest]\n", encoding="utf-8")
    manifest = _manifest_payload(
        heavy_concurrency_cap=cap,
        split_scope_files=["tests/test_split.py"],
        real_full_chain={"marker": REAL_FULL_CHAIN_MARKER,
                         "functions": ["tests/test_split.py::test_chain"]},
    )
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
        + "def test_light(index):\n    _record(f'light{index}', 0.3)\n",
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
    assert len(rows) == 12

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
    )
