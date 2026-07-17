from __future__ import annotations

import json
import os
import subprocess
import sys
import tomllib
from dataclasses import replace
from pathlib import Path

from scripts.pytest_runtime_profile import (
    RUNTIME_PROFILE_FORMAL_SELECTION_ENV,
    RUNTIME_PROFILE_OUTPUT_ENV,
    build_runtime_profile,
    collection_identity,
    load_duration_profile,
    resolve_scheduler_decision,
    stable_reorder_nodeids,
    verify_duration_order,
)

PROFILE_PATH = Path("inputs/architecture/arch_004g2_full_duration_profile.yaml")


def _phase(
    nodeid: str,
    phase: str,
    worker_id: str,
    start: float,
    stop: float,
    *,
    outcome: str = "passed",
) -> dict[str, object]:
    return {
        "nodeid": nodeid,
        "phase": phase,
        "worker_id": worker_id,
        "start": start,
        "stop": stop,
        "duration": stop - start,
        "outcome": outcome,
    }


def test_tracked_partial_seed_profile_is_valid_and_source_bound() -> None:
    profile = load_duration_profile(PROFILE_PATH)

    assert profile.valid is True
    assert profile.partial_seed is True
    assert profile.owner == "validation_operations"
    assert profile.version == 1
    assert profile.source_workers == 16
    assert profile.source_dist == "loadfile"
    assert profile.source_artifact_path == (
        "outputs/validation_runtime/full_20260717T161557Z/test_runtime_summary.json"
    )
    assert profile.source_artifact_sha256 == (
        "93d1713f78bc1bec3a9792bbea99c02da6a0966123d096a9c233004d2b47c756"
    )
    assert len(profile.observed_seconds) == 44
    assert profile.observed_seconds["tests/test_layer1_meta_policy_readiness.py"] == 1198.77


def test_duration_order_is_stable_and_preserves_file_internal_order() -> None:
    nodeids = [
        "tests/test_b.py::test_b",
        "tests/test_a.py::test_a_first",
        "tests/test_a.py::test_a_second",
        "tests/test_c.py::test_c",
        "tests/test_untracked_first.py::test_untracked",
        "tests/test_untracked_second.py::test_untracked",
    ]
    observed = {
        "tests/test_a.py": 10.0,
        "tests/test_b.py": 10.0,
        "tests/test_c.py": 20.0,
    }

    reordered = stable_reorder_nodeids(nodeids, observed)

    assert reordered == [
        "tests/test_c.py::test_c",
        "tests/test_b.py::test_b",
        "tests/test_a.py::test_a_first",
        "tests/test_a.py::test_a_second",
        "tests/test_untracked_first.py::test_untracked",
        "tests/test_untracked_second.py::test_untracked",
    ]
    verification = verify_duration_order(reordered, observed)

    assert verification.verified is True
    assert verification.matched_tracked_file_count == 3
    assert verification.matched_tracked_node_count == 4
    assert verification.expected_ordered_sha256 == collection_identity(reordered)[
        "ordered_sha256"
    ]
    assert verify_duration_order(nodeids, observed).verified is False


def test_stock_fallback_matches_loadfile_test_count_order() -> None:
    nodeids = [
        "tests/test_single_first.py::test_one",
        "tests/test_double.py::test_first",
        "tests/test_double.py::test_second",
        "tests/test_single_last.py::test_one",
    ]

    reordered = stable_reorder_nodeids(nodeids, {}, fallback_by_count=True)

    assert reordered == [
        "tests/test_double.py::test_first",
        "tests/test_double.py::test_second",
        "tests/test_single_first.py::test_one",
        "tests/test_single_last.py::test_one",
    ]


def test_scheduler_requires_matching_parallel_loadfile_execution_contract() -> None:
    profile = load_duration_profile(PROFILE_PATH)

    eligible = resolve_scheduler_decision(
        profile,
        expected_worker_count=16,
        xdist_dist="loadfile",
        loadscope_reorder=False,
    )
    wrong_workers = resolve_scheduler_decision(
        profile,
        expected_worker_count=8,
        xdist_dist="loadfile",
        loadscope_reorder=False,
    )
    worksteal = resolve_scheduler_decision(
        profile,
        expected_worker_count=16,
        xdist_dist="worksteal",
        loadscope_reorder=False,
    )
    serial = resolve_scheduler_decision(
        profile,
        expected_worker_count=1,
        xdist_dist="no",
        loadscope_reorder=False,
    )
    xdist_reorder_enabled = resolve_scheduler_decision(
        profile,
        expected_worker_count=16,
        xdist_dist="loadfile",
        loadscope_reorder=True,
    )

    assert eligible.applied is True
    assert eligible.fallback is False
    assert wrong_workers.applied is False
    assert wrong_workers.fallback is True
    assert wrong_workers.plugin_fallback_by_count is True
    assert worksteal.applied is False
    assert worksteal.fallback is False
    assert serial.applied is False
    assert serial.fallback is False
    assert xdist_reorder_enabled.applied is False
    assert xdist_reorder_enabled.fallback is True
    assert xdist_reorder_enabled.plugin_fallback_by_count is False


def test_invalid_profile_is_explicit_and_can_only_use_stock_fallback(tmp_path: Path) -> None:
    invalid_path = tmp_path / "invalid_profile.yaml"
    invalid_path.write_text(
        "schema_version: arch_004g2_full_duration_profile.v1\n"
        "profile_id: incomplete\n",
        encoding="utf-8",
    )

    profile = load_duration_profile(invalid_path)

    assert profile.valid is False
    assert profile.fallback_reason
    assert profile.observed_seconds == {}

    malformed_path = tmp_path / "malformed_profile.yaml"
    malformed_path.write_text("files: [", encoding="utf-8")
    malformed_profile = load_duration_profile(malformed_path)
    assert malformed_profile.valid is False
    assert "could not be read" in str(malformed_profile.fallback_reason)

    traversal_path = tmp_path / "traversal_profile.yaml"
    traversal_path.write_text(
        PROFILE_PATH.read_text(encoding="utf-8").replace(
            "tests/test_layer1_meta_policy_readiness.py",
            "tests/../outside.py",
            1,
        ),
        encoding="utf-8",
    )
    traversal_profile = load_duration_profile(traversal_path)
    assert traversal_profile.valid is False
    assert "out of scope" in str(traversal_profile.fallback_reason)


def test_collection_identity_is_order_and_duplicate_auditable() -> None:
    first = collection_identity(["tests/test_a.py::test_a", "tests/test_b.py::test_b"])
    reversed_order = collection_identity(
        ["tests/test_b.py::test_b", "tests/test_a.py::test_a"]
    )
    duplicated = collection_identity(["tests/test_a.py::test_a", "tests/test_a.py::test_a"])

    assert first["count"] == 2
    assert first["set_sha256"] == reversed_order["set_sha256"]
    assert first["ordered_sha256"] != reversed_order["ordered_sha256"]
    assert duplicated["duplicate_nodeids"] == ["tests/test_a.py::test_a"]


def test_runtime_profile_aggregates_nodes_files_workers_and_tail_idle() -> None:
    profile = replace(
        load_duration_profile(PROFILE_PATH),
        source_workers=2,
        observed_seconds={"tests/test_a.py": 10.0, "tests/test_b.py": 5.0},
    )
    nodeids = [
        "tests/test_a.py::test_first",
        "tests/test_a.py::test_second",
        "tests/test_b.py::test_last",
    ]
    reports = [
        _phase(nodeids[0], "setup", "gw0", 10.0, 11.0),
        _phase(nodeids[0], "call", "gw0", 11.0, 13.0),
        _phase(nodeids[0], "teardown", "gw0", 13.0, 13.5),
        _phase(nodeids[1], "setup", "gw0", 14.0, 14.5),
        _phase(nodeids[1], "call", "gw0", 14.5, 15.5),
        _phase(nodeids[1], "teardown", "gw0", 15.5, 16.0),
        _phase(nodeids[2], "setup", "gw1", 12.0, 12.5),
        _phase(nodeids[2], "call", "gw1", 12.5, 20.0),
        _phase(nodeids[2], "teardown", "gw1", 20.0, 20.5),
    ]

    payload = build_runtime_profile(
        collections={"gw0": nodeids, "gw1": nodeids},
        phase_reports=reports,
        duration_profile=profile,
        expected_worker_count=2,
        xdist_dist="loadfile",
        loadscope_reorder=False,
        formal_full_selection_eligible=True,
        pytest_exitstatus=0,
        started_at=9.0,
        ended_at=21.0,
    )

    assert payload["profile_status"] == "PASS"
    assert payload["performance_evidence_status"] == "PASS"
    assert payload["scheduler"]["duration_order_verified"] is True
    assert payload["scheduler"]["matched_tracked_file_count"] == 2
    assert payload["scheduler"]["matched_tracked_node_count"] == 3
    assert payload["collection"]["count"] == 3
    assert payload["node_count"] == 3
    assert payload["file_count"] == 2
    assert payload["worker_count"] == 2
    assert payload["outcome_counts"] == {"passed": 3}
    assert payload["files"][0]["path"] == "tests/test_a.py"
    assert payload["files"][0]["node_count"] == 2
    workers = {row["worker_id"]: row for row in payload["workers"]}
    assert workers["gw0"]["tail_idle_seconds"] == 4.5
    assert workers["gw1"]["tail_idle_seconds"] == 0.0
    assert payload["tail_idle_total_seconds"] == 4.5


def test_applied_scheduler_with_wrong_final_order_cannot_form_performance_evidence() -> None:
    profile = replace(
        load_duration_profile(PROFILE_PATH),
        source_workers=2,
        observed_seconds={"tests/test_a.py": 5.0, "tests/test_b.py": 10.0},
    )
    nodeids = ["tests/test_a.py::test_a", "tests/test_b.py::test_b"]
    reports = [
        _phase(nodeids[0], "setup", "gw0", 1.0, 1.1),
        _phase(nodeids[0], "call", "gw0", 1.1, 1.2),
        _phase(nodeids[0], "teardown", "gw0", 1.2, 1.3),
        _phase(nodeids[1], "setup", "gw1", 1.0, 1.1),
        _phase(nodeids[1], "call", "gw1", 1.1, 1.2),
        _phase(nodeids[1], "teardown", "gw1", 1.2, 1.3),
    ]

    payload = build_runtime_profile(
        collections={"gw0": nodeids, "gw1": nodeids},
        phase_reports=reports,
        duration_profile=profile,
        expected_worker_count=2,
        xdist_dist="loadfile",
        loadscope_reorder=False,
        formal_full_selection_eligible=True,
        pytest_exitstatus=0,
        started_at=0.0,
        ended_at=2.0,
    )

    assert payload["profile_status"] == "PASS"
    assert payload["scheduler"]["applied"] is True
    assert payload["scheduler"]["duration_order_verified"] is False
    assert payload["scheduler"]["matched_tracked_file_count"] == 2
    assert payload["scheduler"]["matched_tracked_node_count"] == 2
    assert payload["performance_evidence_status"] == "FAIL"
    assert any(
        "final collection order verification failed" in warning
        for warning in payload["warnings"]
    )


def test_incomplete_telemetry_fails_performance_evidence_not_pytest_outcome() -> None:
    profile = replace(load_duration_profile(PROFILE_PATH), source_workers=2)
    nodeids = ["tests/test_a.py::test_a", "tests/test_b.py::test_b"]

    payload = build_runtime_profile(
        collections={"gw0": nodeids, "gw1": nodeids[:1]},
        phase_reports=[_phase(nodeids[0], "call", "gw0", 1.0, 2.0)],
        duration_profile=profile,
        expected_worker_count=2,
        xdist_dist="loadfile",
        loadscope_reorder=False,
        formal_full_selection_eligible=True,
        pytest_exitstatus=0,
        started_at=0.0,
        ended_at=3.0,
    )

    assert payload["profile_status"] == "FAIL"
    assert payload["telemetry_status"] == "FAIL"
    assert payload["performance_evidence_status"] == "FAIL"
    assert payload["pytest_exitstatus"] == 0
    assert payload["pytest_outcome_authoritative"] is True
    assert payload["pytest_outcome_overridden"] is False
    assert payload["telemetry"]["missing_nodeids"] == ["tests/test_b.py::test_b"]


def test_missing_required_phase_and_inactive_worker_fail_telemetry() -> None:
    profile = replace(load_duration_profile(PROFILE_PATH), source_workers=2)
    nodeids = ["tests/test_a.py::test_a", "tests/test_b.py::test_b"]
    reports = [
        _phase(nodeids[0], "setup", "gw0", 1.0, 1.1),
        _phase(nodeids[0], "call", "gw0", 1.1, 1.2),
        _phase(nodeids[0], "teardown", "gw0", 1.2, 1.3),
        _phase(nodeids[1], "setup", "gw0", 1.3, 1.4),
        _phase(nodeids[1], "call", "gw0", 1.4, 1.5),
    ]

    payload = build_runtime_profile(
        collections={"gw0": nodeids, "gw1": nodeids},
        phase_reports=reports,
        duration_profile=profile,
        expected_worker_count=2,
        xdist_dist="loadfile",
        loadscope_reorder=False,
        formal_full_selection_eligible=True,
        pytest_exitstatus=0,
        started_at=0.0,
        ended_at=2.0,
    )

    assert payload["telemetry_status"] == "FAIL"
    assert payload["performance_evidence_status"] == "FAIL"
    assert payload["telemetry"]["missing_required_phase_nodeids"] == [nodeids[1]]
    assert payload["telemetry"]["inactive_worker_ids"] == ["gw1"]


def test_filtered_selection_cannot_be_performance_evidence() -> None:
    profile = replace(load_duration_profile(PROFILE_PATH), source_workers=2)
    nodeids = ["tests/test_a.py::test_a", "tests/test_b.py::test_b"]

    payload = build_runtime_profile(
        collections={"gw0": nodeids, "gw1": nodeids},
        phase_reports=[
            _phase(nodeids[0], "setup", "gw0", 1.0, 1.1),
            _phase(nodeids[0], "call", "gw0", 1.1, 1.2),
            _phase(nodeids[0], "teardown", "gw0", 1.2, 1.3),
            _phase(nodeids[1], "setup", "gw1", 1.0, 1.1),
            _phase(nodeids[1], "call", "gw1", 1.1, 1.2),
            _phase(nodeids[1], "teardown", "gw1", 1.2, 1.3),
        ],
        duration_profile=profile,
        expected_worker_count=2,
        xdist_dist="loadfile",
        loadscope_reorder=False,
        formal_full_selection_eligible=False,
        pytest_exitstatus=0,
        started_at=0.0,
        ended_at=2.0,
    )

    assert payload["telemetry_status"] == "PASS"
    assert payload["performance_evidence_status"] == "FAIL"
    assert payload["scheduler"]["applied"] is True
    assert payload["scheduler"]["formal_full_selection_eligible"] is False


def test_invalid_scheduler_profile_marks_fallback_without_overriding_pytest(
    tmp_path: Path,
) -> None:
    invalid_profile = load_duration_profile(tmp_path / "missing.yaml")
    nodeid = "tests/test_a.py::test_a"

    payload = build_runtime_profile(
        collections={"gw0": [nodeid]},
        phase_reports=[
            _phase(nodeid, "setup", "gw0", 1.0, 1.1),
            _phase(nodeid, "call", "gw0", 1.1, 2.0),
            _phase(nodeid, "teardown", "gw0", 2.0, 2.1),
        ],
        duration_profile=invalid_profile,
        expected_worker_count=1,
        xdist_dist="no",
        loadscope_reorder=False,
        formal_full_selection_eligible=True,
        pytest_exitstatus=0,
        started_at=0.0,
        ended_at=3.0,
    )

    assert payload["profile_status"] == "PASS"
    assert payload["telemetry_status"] == "PASS"
    assert payload["performance_evidence_status"] == "FAIL"
    assert payload["scheduler"]["policy"] == "non_loadfile_collection_order_preserved"
    assert payload["scheduler"]["applied"] is False
    assert payload["scheduler"]["fallback"] is False
    assert payload["pytest_exitstatus"] == 0
    assert payload["pytest_outcome_overridden"] is False


def test_failed_pytest_outcome_cannot_be_promoted_as_performance_evidence() -> None:
    profile = load_duration_profile(PROFILE_PATH)
    nodeid = "tests/test_a.py::test_a"

    payload = build_runtime_profile(
        collections={"gw0": [nodeid]},
        phase_reports=[
            _phase(nodeid, "setup", "gw0", 1.0, 1.1),
            _phase(nodeid, "call", "gw0", 1.1, 2.0, outcome="failed"),
            _phase(nodeid, "teardown", "gw0", 2.0, 2.1),
        ],
        duration_profile=profile,
        expected_worker_count=1,
        xdist_dist="no",
        loadscope_reorder=False,
        formal_full_selection_eligible=True,
        pytest_exitstatus=1,
        started_at=0.0,
        ended_at=3.0,
    )

    assert payload["profile_status"] == "PASS"
    assert payload["telemetry_status"] == "PASS"
    assert payload["performance_evidence_status"] == "FAIL"
    assert payload["pytest_exitstatus"] == 1
    assert payload["outcome_counts"] == {"failed": 1}


def test_real_xdist_plugin_writes_complete_noncomparable_profile(tmp_path: Path) -> None:
    suite_dir = tmp_path / "mini_suite"
    suite_dir.mkdir()
    (suite_dir / "test_alpha.py").write_text(
        "def test_alpha():\n    assert True\n",
        encoding="utf-8",
    )
    (suite_dir / "test_beta.py").write_text(
        "def test_beta_first():\n    assert True\n\n"
        "def test_beta_second():\n    assert True\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "test_runtime_profile.json"
    env = dict(os.environ)
    env[RUNTIME_PROFILE_OUTPUT_ENV] = str(output_path)
    env[RUNTIME_PROFILE_FORMAL_SELECTION_ENV] = "1"

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-n",
            "2",
            "--dist",
            "loadfile",
            "-p",
            "scripts.pytest_runtime_profile",
            "--aits-duration-profile",
            str(PROFILE_PATH),
            str(suite_dir),
            "-q",
            "--no-loadscope-reorder",
        ],
        cwd=Path.cwd(),
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["profile_status"] == "PASS"
    assert payload["telemetry_status"] == "PASS"
    assert payload["performance_evidence_status"] == "FAIL"
    assert payload["collection"]["count"] == 3
    assert payload["collection"]["nodeids"] == [
        "test_beta.py::test_beta_first",
        "test_beta.py::test_beta_second",
        "test_alpha.py::test_alpha",
    ]
    assert payload["node_count"] == 3
    assert payload["file_count"] == 2
    assert payload["worker_count"] == 2
    assert payload["scheduler"]["applied"] is False
    assert payload["scheduler"]["fallback"] is True
    assert "worker count mismatch" in payload["scheduler"]["fallback_reason"]


def test_real_xdist_plugin_applies_and_verifies_duration_order(tmp_path: Path) -> None:
    repo_root = Path.cwd().resolve()
    suite_dir = tmp_path / "tests"
    suite_dir.mkdir()
    tracked_files = [
        "test_layer1_meta_policy_readiness.py",
        "test_refined_method_proposal.py",
    ]
    untracked_files = [f"test_{index:02d}_untracked.py" for index in range(14)]
    for index, file_name in enumerate([*tracked_files, *untracked_files]):
        (suite_dir / file_name).write_text(
            f"def test_runtime_profile_smoke_{index}():\n    assert True\n",
            encoding="utf-8",
        )

    output_path = tmp_path / "test_runtime_profile.json"
    env = dict(os.environ)
    env[RUNTIME_PROFILE_OUTPUT_ENV] = str(output_path)
    env[RUNTIME_PROFILE_FORMAL_SELECTION_ENV] = "1"
    python_path_parts = [str(repo_root), str(repo_root / "src")]
    if env.get("PYTHONPATH"):
        python_path_parts.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(python_path_parts)

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-n",
            "16",
            "--dist",
            "loadfile",
            "-p",
            "scripts.pytest_runtime_profile",
            "--aits-duration-profile",
            str((repo_root / PROFILE_PATH).resolve()),
            "--rootdir",
            str(tmp_path),
            "tests",
            "-q",
            "--no-loadscope-reorder",
        ],
        cwd=tmp_path,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["profile_status"] == "PASS"
    assert payload["telemetry_status"] == "PASS"
    assert payload["performance_evidence_status"] == "PASS"
    assert payload["scheduler"]["applied"] is True
    assert payload["scheduler"]["duration_order_verified"] is True
    assert payload["scheduler"]["matched_tracked_file_count"] == 2
    assert payload["scheduler"]["matched_tracked_node_count"] == 2
    assert payload["collection"]["nodeids"][:2] == [
        "tests/test_layer1_meta_policy_readiness.py::test_runtime_profile_smoke_0",
        "tests/test_refined_method_proposal.py::test_runtime_profile_smoke_1",
    ]
    assert payload["collection"]["nodeids"][2:] == [
        f"tests/{file_name}::test_runtime_profile_smoke_{index}"
        for index, file_name in enumerate(untracked_files, start=2)
    ]
    assert payload["worker_count"] == 16


def test_runtime_sidecar_write_failure_preserves_pytest_exit(tmp_path: Path) -> None:
    suite_dir = tmp_path / "write_failure_suite"
    suite_dir.mkdir()
    (suite_dir / "test_ok.py").write_text(
        "def test_ok():\n    assert True\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "profile_destination_is_a_directory"
    output_path.mkdir()
    env = dict(os.environ)
    env[RUNTIME_PROFILE_OUTPUT_ENV] = str(output_path)
    env[RUNTIME_PROFILE_FORMAL_SELECTION_ENV] = "1"

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "scripts.pytest_runtime_profile",
            "--aits-duration-profile",
            str(PROFILE_PATH),
            str(suite_dir),
            "-q",
        ],
        cwd=Path.cwd(),
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "1 passed" in completed.stdout
    assert "runtime profile sidecar write failed" in completed.stderr
    assert output_path.is_dir()


def test_xdist_dependency_floor_supports_loadscope_reorder_contract() -> None:
    pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    dev_dependencies = pyproject["project"]["optional-dependencies"]["dev"]

    assert "pytest-xdist>=3.8" in dev_dependencies
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "--help"],
        cwd=Path.cwd(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "--no-loadscope-reorder" in completed.stdout
