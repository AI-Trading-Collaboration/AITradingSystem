from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tomllib
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from scripts.pytest_runtime_profile import (
    RUNTIME_PROFILE_FORMAL_SELECTION_ENV,
    RUNTIME_PROFILE_OUTPUT_ENV,
    RUNTIME_PROFILE_VALIDATION_PROVENANCE_ENV,
    DurationProfile,
    _validation_provenance_from_environment,
    build_runtime_profile,
    collection_identity,
    load_duration_profile,
    resolve_scheduler_decision,
    stable_reorder_nodeids,
    verify_complete_profile_collection,
    verify_duration_order,
)
from scripts.refresh_partial_duration_profile import build_partial_duration_manifest

PROFILE_PATH = Path("inputs/architecture/arch_004g2_full_duration_profile.yaml")


def _full_validation_provenance(
    *,
    boundary_id: str = "runtime-profile-test",
) -> dict[str, object]:
    return {
        "schema_version": "validation_trigger_provenance.v1",
        "status": "PASS",
        "required_for_tier": True,
        "trigger_reason": "formal_performance_profile",
        "task_id": "ARCH-004G2",
        "boundary_id": boundary_id,
        "parent_run": None,
        "envelope_source": "environment",
        "field_sources": {
            "trigger_reason": "environment",
            "task_id": "environment",
            "boundary_id": "environment",
            "parent_run": "unset",
        },
        "cli_over_environment_precedence": "whole_envelope",
        "validation_errors": [],
    }


def _full_validation_provenance_json(
    *,
    boundary_id: str = "runtime-profile-test",
) -> str:
    return json.dumps(
        _full_validation_provenance(boundary_id=boundary_id),
        separators=(",", ":"),
        sort_keys=True,
    )


def _file_set_sha256(paths: list[str]) -> str:
    return hashlib.sha256("\n".join(sorted(paths)).encode("utf-8")).hexdigest()


def _file_rows_sha256(rows: list[dict[str, object]]) -> str:
    canonical = sorted(rows, key=lambda row: str(row["path"]))
    return hashlib.sha256(
        json.dumps(
            canonical,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _write_legacy_partial_profile(path: Path) -> Path:
    payload = {
        "schema_version": "arch_004g2_full_duration_profile.v1",
        "profile_id": "legacy_partial_seed_test",
        "status": "PARTIAL_SEED",
        "owner": "validation_operations",
        "version": 1,
        "source": {
            "artifact_path": "outputs/validation_runtime/legacy/test_runtime_summary.json",
            "artifact_sha256": "1" * 64,
            "tier": "full",
            "workers": 16,
            "dist": "loadfile",
        },
        "partial_seed": {
            "enabled": True,
            "source_duration_row_count": 2,
            "aggregated_file_count": 2,
        },
        "review": {
            "stable_improvement_claimed": False,
            "conditions": ["legacy v1 compatibility"],
        },
        "files": [
            {"path": "tests/test_a.py", "observed_seconds": 2.0},
            {"path": "tests/test_b.py", "observed_seconds": 1.0},
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _write_complete_profile(
    path: Path,
    *,
    nodeids: list[str],
    observed_seconds: dict[str, float],
) -> Path:
    file_node_counts = {
        file_path: sum(
            nodeid.split("::", 1)[0].replace("\\", "/") == file_path for nodeid in nodeids
        )
        for file_path in observed_seconds
    }
    file_rows = [
        {
            "path": file_path,
            "node_count": file_node_counts[file_path],
            "observed_seconds": observed_seconds[file_path],
        }
        for file_path in observed_seconds
    ]
    expected_nodeids = stable_reorder_nodeids(nodeids, observed_seconds)
    payload = {
        "schema_version": "arch_004g2_full_duration_profile.v1",
        "profile_id": "complete_duration_profile_test",
        "status": "COMPLETE",
        "owner": "validation_operations",
        "version": 2,
        "source": {
            "artifact_path": "outputs/validation_runtime/test/test_runtime_profile.json",
            "artifact_sha256": "2" * 64,
            "tier": "full",
            "workers": 16,
            "dist": "loadfile",
            "elapsed_seconds": 1.0,
            "git_commit": "3" * 40,
            "profile_status": "PASS",
            "telemetry_status": "PASS",
            "performance_evidence_status": "PASS",
            "pytest_exitstatus": 0,
        },
        "complete_profile": {
            "enabled": True,
            "source_node_count": len(nodeids),
            "source_file_count": len(file_rows),
            "source_collection_ordered_sha256": collection_identity(nodeids)["ordered_sha256"],
            "source_collection_set_sha256": collection_identity(nodeids)["set_sha256"],
            "source_file_set_sha256": _file_set_sha256(list(observed_seconds)),
            "source_file_rows_sha256": _file_rows_sha256(file_rows),
            "expected_scheduled_ordered_sha256": collection_identity(expected_nodeids)[
                "ordered_sha256"
            ],
            "source_file_duration_total_seconds": sum(observed_seconds.values()),
        },
        "review": {
            "stable_improvement_claimed": False,
            "conditions": ["complete v1 collection binding"],
        },
        "files": file_rows,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _legacy_runtime_profile(
    *,
    observed_seconds: dict[str, float] | None = None,
    source_workers: int = 2,
) -> DurationProfile:
    return replace(
        load_duration_profile(PROFILE_PATH),
        manifest_status="PARTIAL_SEED",
        partial_seed=True,
        complete_profile=False,
        source_workers=source_workers,
        observed_seconds=observed_seconds or {},
        file_node_counts={},
        source_node_count=None,
        source_file_count=None,
        source_collection_ordered_sha256=None,
        source_collection_set_sha256=None,
        source_file_set_sha256=None,
        source_file_rows_sha256=None,
        expected_scheduled_ordered_sha256=None,
        source_file_duration_total_seconds=None,
    )


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


def _build_comparable_runtime_payload(
    *,
    validation_provenance: dict[str, object] | None,
    validation_provenance_errors: tuple[str, ...] = (),
) -> dict[str, object]:
    nodeids = [f"tests/test_{index:02d}.py::test_{index:02d}" for index in range(16)]
    phase_reports = [
        _phase(nodeid, phase, f"gw{index}", start, stop)
        for index, nodeid in enumerate(nodeids)
        for phase, start, stop in (
            ("setup", 1.0, 1.1),
            ("call", 1.1, 1.2),
            ("teardown", 1.2, 1.3),
        )
    ]
    return build_runtime_profile(
        collections={f"gw{index}": nodeids for index in range(16)},
        phase_reports=phase_reports,
        duration_profile=_legacy_runtime_profile(
            observed_seconds={
                f"tests/test_{index:02d}.py": float(16 - index) for index in range(16)
            },
            source_workers=16,
        ),
        expected_worker_count=16,
        xdist_dist="loadfile",
        loadscope_reorder=False,
        formal_full_selection_eligible=True,
        pytest_exitstatus=0,
        started_at=0.0,
        ended_at=2.0,
        validation_provenance=validation_provenance,
        validation_provenance_errors=validation_provenance_errors,
    )


def test_tracked_partial_profile_is_valid_and_source_bound(tmp_path: Path) -> None:
    profile = load_duration_profile(PROFILE_PATH)

    assert profile.valid is True
    assert profile.manifest_status == "PARTIAL_SEED"
    assert profile.partial_seed is True
    assert profile.complete_profile is False
    assert profile.owner == "validation_operations"
    assert profile.version == 18
    assert profile.source_workers == 16
    assert profile.source_dist == "loadfile"
    assert profile.source_artifact_path == (
        "outputs/validation_runtime/full_20260720T163446Z/test_runtime_profile.json"
    )
    assert profile.source_artifact_sha256 == (
        "61ccfaf0b85b0f65abedfa22b009567a109cb511037dc16a65636dca7c007227"
    )
    assert len(profile.observed_seconds) == 1084
    assert profile.source_node_count is None
    assert profile.source_file_count is None
    assert profile.source_collection_ordered_sha256 is None
    assert profile.source_collection_set_sha256 is None
    assert profile.source_file_set_sha256 is None
    assert profile.source_file_rows_sha256 is None
    assert profile.expected_scheduled_ordered_sha256 is None
    assert profile.source_file_duration_total_seconds is None
    assert profile.observed_seconds["tests/test_layer1_meta_policy_readiness.py"] == (507.7504568)
    assert (
        profile.observed_seconds["tests/test_filtered_candidate_readiness_pipeline_foundation.py"]
        == 117.1397316
    )

    legacy = load_duration_profile(_write_legacy_partial_profile(tmp_path / "legacy_partial.yaml"))
    assert legacy.valid is True
    assert legacy.manifest_status == "PARTIAL_SEED"
    assert legacy.partial_seed is True
    assert legacy.complete_profile is False
    legacy_decision = resolve_scheduler_decision(
        legacy,
        expected_worker_count=16,
        xdist_dist="loadfile",
        loadscope_reorder=False,
    )
    assert legacy_decision.policy == "tracked_partial_seed_duration_descending_stable"
    assert legacy_decision.applied is True


def test_partial_profile_refresh_uses_duration_rows_and_exact_summary_binding(
    tmp_path: Path,
) -> None:
    source_profile_path = tmp_path / "outputs/full/test_runtime_profile.json"
    source_summary_path = tmp_path / "outputs/full/test_runtime_summary.json"
    source_profile_path.parent.mkdir(parents=True)
    source_profile = {
        "schema_version": "test_runtime_profile.v1",
        "profile_status": "PASS",
        "telemetry_status": "PASS",
        "performance_evidence_status": "PASS",
        "validation_provenance_binding_status": "PASS",
        "pytest_exitstatus": 0,
        "worker_count": 16,
        "node_count": 3,
        "file_count": 2,
        "elapsed_seconds": 5.0,
        "production_effect": "none",
        "strategy_logic_changed": False,
        "broker_action_taken": False,
        "scheduler": {
            "applied": True,
            "fallback": False,
            "expected_worker_count": 16,
            "xdist_dist": "loadfile",
            "formal_full_selection_eligible": True,
        },
        "collection": {
            "complete": True,
            "count": 3,
            "observed_worker_count": 16,
            "duplicate_nodeids": [],
        },
        "files": [
            {
                "path": "tests/test_fast.py",
                "node_count": 1,
                "duration_seconds": 1.0,
                "elapsed_envelope_seconds": 99.0,
            },
            {
                "path": "tests/test_slow.py",
                "node_count": 2,
                "duration_seconds": 3.0,
                "elapsed_envelope_seconds": 0.1,
            },
        ],
    }
    source_profile_path.write_text(json.dumps(source_profile), encoding="utf-8")
    profile_bytes = source_profile_path.read_bytes()
    source_sha256 = hashlib.sha256(profile_bytes).hexdigest()
    source_summary = {
        "status": "PASS",
        "exit_code": 0,
        "runtime_profile_status": "PASS",
        "validation_provenance_status": "PASS",
        "dist": "loadfile",
        "workers": "16",
        "formal_full_selection_eligible": True,
        "production_effect": "none",
        "strategy_logic_changed": False,
        "broker_action_taken": False,
        "runtime_profile_path": str(source_profile_path.resolve()),
        "git_commit": "1" * 40,
        "runtime_profile_summary": {
            "collection_count": 3,
            "node_count": 3,
            "file_count": 2,
            "worker_count": 16,
            "performance_evidence_status": "PASS",
            "telemetry_status": "PASS",
            "validation_provenance_binding_status": "PASS",
            "scheduler_applied": True,
            "scheduler_fallback": False,
            "formal_full_selection_eligible": True,
        },
        "output_artifacts": [
            {
                "path": str(source_profile_path.resolve()),
                "exists": True,
                "sha256": source_sha256,
                "size_bytes": len(profile_bytes),
            }
        ],
    }
    source_summary_path.write_text(json.dumps(source_summary), encoding="utf-8")

    with patch("scripts.refresh_partial_duration_profile.PROJECT_ROOT", tmp_path):
        manifest = build_partial_duration_manifest(
            source_profile_path=source_profile_path,
            source_summary_path=source_summary_path,
            profile_id="eb4_test_seed",
            version=10,
            expected_nodes=3,
            expected_files=2,
        )

    assert manifest["status"] == "PARTIAL_SEED"
    assert "complete_profile" not in manifest
    assert manifest["source"]["artifact_sha256"] == source_sha256
    assert manifest["source"]["artifact_path"] == ("outputs/full/test_runtime_profile.json")
    assert [row["path"] for row in manifest["files"]] == [
        "tests/test_slow.py",
        "tests/test_fast.py",
    ]
    assert manifest["files"][0]["observed_seconds"] == 3.0

    source_summary["output_artifacts"][0]["sha256"] = "0" * 64
    source_summary_path.write_text(json.dumps(source_summary), encoding="utf-8")
    try:
        with patch("scripts.refresh_partial_duration_profile.PROJECT_ROOT", tmp_path):
            build_partial_duration_manifest(
                source_profile_path=source_profile_path,
                source_summary_path=source_summary_path,
                profile_id="eb4_test_seed",
                version=10,
                expected_nodes=3,
                expected_files=2,
            )
    except ValueError as exc:
        assert "inventory sha256" in str(exc)
    else:
        raise AssertionError("tampered runtime profile inventory must fail closed")


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
    assert verification.expected_ordered_sha256 == collection_identity(reordered)["ordered_sha256"]
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


def test_scheduler_requires_matching_parallel_loadfile_execution_contract(
    tmp_path: Path,
) -> None:
    nodeids = ["tests/test_a.py::test_a", "tests/test_b.py::test_b"]
    observed = {"tests/test_a.py": 2.0, "tests/test_b.py": 1.0}
    profile = load_duration_profile(
        _write_complete_profile(
            tmp_path / "complete.yaml",
            nodeids=nodeids,
            observed_seconds=observed,
        )
    )

    eligible = resolve_scheduler_decision(
        profile,
        expected_worker_count=16,
        xdist_dist="loadfile",
        loadscope_reorder=False,
        nodeids=nodeids,
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
    assert eligible.policy == "complete_full_duration_descending_stable"
    assert eligible.fallback is False
    assert wrong_workers.applied is False
    assert wrong_workers.fallback is True
    assert wrong_workers.plugin_fallback_by_count is True
    assert worksteal.applied is False
    assert worksteal.fallback is True
    assert worksteal.policy == "stock_loadfile_test_count_order"
    assert serial.applied is False
    assert serial.fallback is True
    assert xdist_reorder_enabled.applied is False
    assert xdist_reorder_enabled.fallback is True
    assert xdist_reorder_enabled.plugin_fallback_by_count is False

    exact = verify_complete_profile_collection(nodeids, profile)
    stale = verify_complete_profile_collection(
        ["tests/test_a.py::test_changed", nodeids[1]],
        profile,
    )
    duplicated = verify_complete_profile_collection([nodeids[0], nodeids[0]], profile)
    assert exact.verified is True
    assert stale.verified is False
    assert "set_sha256 mismatch" in str(stale.fallback_reason)
    assert duplicated.verified is False
    assert "duplicate nodeids" in str(duplicated.fallback_reason)
    stale_decision = resolve_scheduler_decision(
        profile,
        expected_worker_count=16,
        xdist_dist="loadfile",
        loadscope_reorder=False,
        nodeids=["tests/test_a.py::test_changed", nodeids[1]],
    )
    assert stale_decision.policy == "stock_loadfile_test_count_order"
    assert stale_decision.fallback is True


def test_invalid_profile_is_explicit_and_can_only_use_stock_fallback(tmp_path: Path) -> None:
    invalid_path = tmp_path / "invalid_profile.yaml"
    invalid_path.write_text(
        "schema_version: arch_004g2_full_duration_profile.v1\n" "profile_id: incomplete\n",
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

    complete_path = _write_complete_profile(
        tmp_path / "complete_profile.yaml",
        nodeids=["tests/test_a.py::test_a", "tests/test_b.py::test_b"],
        observed_seconds={"tests/test_a.py": 2.0, "tests/test_b.py": 1.0},
    )
    duplicate_payload = json.loads(complete_path.read_text(encoding="utf-8"))
    duplicate_payload["files"].append(dict(duplicate_payload["files"][0]))
    duplicate_path = tmp_path / "duplicate_complete_profile.yaml"
    duplicate_path.write_text(json.dumps(duplicate_payload), encoding="utf-8")
    duplicate_profile = load_duration_profile(duplicate_path)
    assert duplicate_profile.valid is False
    assert "duplicate file" in str(duplicate_profile.fallback_reason)

    stale_payload = json.loads(complete_path.read_text(encoding="utf-8"))
    stale_payload["complete_profile"]["source_node_count"] = 999
    stale_path = tmp_path / "stale_complete_profile.yaml"
    stale_path.write_text(json.dumps(stale_payload), encoding="utf-8")
    stale_profile = load_duration_profile(stale_path)
    assert stale_profile.valid is False
    assert "source_node_count is stale" in str(stale_profile.fallback_reason)


def test_collection_identity_is_order_and_duplicate_auditable() -> None:
    first = collection_identity(["tests/test_a.py::test_a", "tests/test_b.py::test_b"])
    reversed_order = collection_identity(["tests/test_b.py::test_b", "tests/test_a.py::test_a"])
    duplicated = collection_identity(["tests/test_a.py::test_a", "tests/test_a.py::test_a"])

    assert first["count"] == 2
    assert first["set_sha256"] == reversed_order["set_sha256"]
    assert first["ordered_sha256"] != reversed_order["ordered_sha256"]
    assert duplicated["duplicate_nodeids"] == ["tests/test_a.py::test_a"]


def test_runtime_profile_aggregates_nodes_files_workers_and_tail_idle() -> None:
    profile = _legacy_runtime_profile(
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

    provenance = _full_validation_provenance(boundary_id="runtime-profile-unit")
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
        validation_provenance=provenance,
    )

    assert payload["profile_status"] == "PASS"
    assert payload["validation_provenance_binding_status"] == "PASS"
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
    assert payload["validation_provenance"] == provenance


def test_missing_validation_provenance_fails_only_performance_binding() -> None:
    payload = _build_comparable_runtime_payload(validation_provenance=None)

    assert payload["profile_status"] == "PASS"
    assert payload["telemetry_status"] == "PASS"
    assert payload["scheduler"]["applied"] is True
    assert payload["scheduler"]["duration_order_verified"] is True
    assert payload["validation_provenance_binding_status"] == "FAIL"
    assert payload["performance_evidence_status"] == "FAIL"
    assert payload["pytest_exitstatus"] == 0
    assert payload["pytest_outcome_authoritative"] is True
    assert payload["pytest_outcome_overridden"] is False
    assert payload["validation_provenance"] is None
    assert any(
        "validation provenance must be a mapping" in warning for warning in payload["warnings"]
    )


def test_invalid_validation_provenance_fails_only_performance_binding() -> None:
    invalid_provenance = _full_validation_provenance()
    invalid_provenance["status"] = "FAIL"

    payload = _build_comparable_runtime_payload(
        validation_provenance=invalid_provenance,
    )

    assert payload["profile_status"] == "PASS"
    assert payload["telemetry_status"] == "PASS"
    assert payload["scheduler"]["applied"] is True
    assert payload["scheduler"]["duration_order_verified"] is True
    assert payload["validation_provenance_binding_status"] == "FAIL"
    assert payload["performance_evidence_status"] == "FAIL"
    assert payload["pytest_exitstatus"] == 0
    assert payload["pytest_outcome_authoritative"] is True
    assert payload["pytest_outcome_overridden"] is False
    assert payload["validation_provenance"] == invalid_provenance
    assert any(
        "validation provenance status must be PASS" in warning for warning in payload["warnings"]
    )


def test_validation_provenance_environment_rejects_duplicate_and_non_finite_json() -> None:
    invalid_cases = [
        (
            '{"schema_version":"first","schema_version":"second"}',
            "duplicate JSON key: schema_version",
        ),
        ('{"schema_version":NaN}', "non-finite JSON constant: NaN"),
    ]

    for raw_provenance, expected_error in invalid_cases:
        with patch.dict(
            os.environ,
            {RUNTIME_PROFILE_VALIDATION_PROVENANCE_ENV: raw_provenance},
        ):
            payload, errors = _validation_provenance_from_environment()

        assert payload is None
        assert any(expected_error in error for error in errors)


def test_validation_provenance_validator_is_total_for_legal_json_types() -> None:
    invalid_payloads = []
    invalid_source = _full_validation_provenance()
    invalid_source["envelope_source"] = []
    invalid_payloads.append(invalid_source)
    invalid_field_source = _full_validation_provenance()
    field_sources = invalid_field_source["field_sources"]
    assert isinstance(field_sources, dict)
    field_sources["task_id"] = {}
    invalid_payloads.append(invalid_field_source)

    for invalid_payload in invalid_payloads:
        with patch.dict(
            os.environ,
            {
                RUNTIME_PROFILE_VALIDATION_PROVENANCE_ENV: json.dumps(invalid_payload),
            },
        ):
            payload, errors = _validation_provenance_from_environment()

        assert payload == invalid_payload
        assert errors


def test_validation_provenance_validator_exception_fails_closed() -> None:
    provenance = _full_validation_provenance()
    with (
        patch.dict(
            os.environ,
            {
                RUNTIME_PROFILE_VALIDATION_PROVENANCE_ENV: json.dumps(provenance),
            },
        ),
        patch(
            "scripts.pytest_runtime_profile.validate_full_provenance",
            side_effect=RuntimeError("validator boom"),
        ),
    ):
        parsed, errors = _validation_provenance_from_environment()
        payload = _build_comparable_runtime_payload(
            validation_provenance=parsed,
            validation_provenance_errors=tuple(errors),
        )

    assert parsed == provenance
    assert any("RuntimeError: validator boom" in error for error in errors)
    assert payload["profile_status"] == "PASS"
    assert payload["telemetry_status"] == "PASS"
    assert payload["validation_provenance_binding_status"] == "FAIL"
    assert payload["performance_evidence_status"] == "FAIL"
    assert payload["pytest_exitstatus"] == 0


def test_applied_scheduler_with_wrong_final_order_cannot_form_performance_evidence() -> None:
    profile = _legacy_runtime_profile(
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
        "final collection order verification failed" in warning for warning in payload["warnings"]
    )


def test_incomplete_telemetry_fails_performance_evidence_not_pytest_outcome() -> None:
    profile = _legacy_runtime_profile()
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
    profile = _legacy_runtime_profile()
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
    profile = _legacy_runtime_profile()
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
    repo_root = Path.cwd().resolve()
    suite_dir = tmp_path / "mini_suite"
    suite_dir.mkdir()
    (suite_dir / "test_alpha.py").write_text(
        "def test_alpha():\n    assert True\n",
        encoding="utf-8",
    )
    (suite_dir / "test_beta.py").write_text(
        "def test_beta_first():\n    assert True\n\n" "def test_beta_second():\n    assert True\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "test_runtime_profile.json"
    env = dict(os.environ)
    env[RUNTIME_PROFILE_OUTPUT_ENV] = str(output_path)
    env[RUNTIME_PROFILE_FORMAL_SELECTION_ENV] = "1"
    env[RUNTIME_PROFILE_VALIDATION_PROVENANCE_ENV] = _full_validation_provenance_json(
        boundary_id="real-xdist-noncomparable",
    )
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
            "2",
            "--dist",
            "loadfile",
            "-p",
            "scripts.pytest_runtime_profile",
            "--aits-duration-profile",
            str((repo_root / PROFILE_PATH).resolve()),
            "--rootdir",
            str(tmp_path),
            "--confcutdir",
            str(tmp_path),
            "mini_suite",
            "-q",
            "--no-loadscope-reorder",
        ],
        # Keep the nested session inside one hermetic root.  Collecting an
        # external temp suite from the repository root makes Windows pytest
        # traverse sibling temp paths that another xdist worker may delete.
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
    assert payload["validation_provenance_binding_status"] == "PASS"
    assert payload["performance_evidence_status"] == "FAIL"
    assert payload["collection"]["count"] == 3
    assert payload["collection"]["nodeids"] == [
        "mini_suite/test_beta.py::test_beta_first",
        "mini_suite/test_beta.py::test_beta_second",
        "mini_suite/test_alpha.py::test_alpha",
    ]
    assert payload["node_count"] == 3
    assert payload["file_count"] == 2
    assert payload["worker_count"] == 2
    assert payload["scheduler"]["applied"] is False
    assert payload["scheduler"]["fallback"] is True
    assert payload["scheduler"]["policy"] == "stock_loadfile_test_count_order"
    assert payload["scheduler"]["manifest_status"] == "PARTIAL_SEED"
    assert payload["scheduler"]["complete_collection_verified"] is False
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
    all_files = [*tracked_files, *untracked_files]
    for index, file_name in enumerate(all_files):
        (suite_dir / file_name).write_text(
            f"def test_runtime_profile_smoke_{index}():\n    assert True\n",
            encoding="utf-8",
        )
    expected_nodeids = [
        f"tests/{file_name}::test_runtime_profile_smoke_{index}"
        for index, file_name in enumerate(all_files)
    ]
    observed_seconds = {
        f"tests/{file_name}": float(len(all_files) - index)
        for index, file_name in enumerate(all_files)
    }
    complete_profile_path = _write_complete_profile(
        tmp_path / "complete_duration_profile.yaml",
        nodeids=expected_nodeids,
        observed_seconds=observed_seconds,
    )

    output_path = tmp_path / "test_runtime_profile.json"
    env = dict(os.environ)
    env[RUNTIME_PROFILE_OUTPUT_ENV] = str(output_path)
    env[RUNTIME_PROFILE_FORMAL_SELECTION_ENV] = "1"
    provenance = _full_validation_provenance(boundary_id="real-xdist-comparable")
    env[RUNTIME_PROFILE_VALIDATION_PROVENANCE_ENV] = json.dumps(
        provenance,
        separators=(",", ":"),
        sort_keys=True,
    )
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
            str(complete_profile_path.resolve()),
            "--rootdir",
            str(tmp_path),
            "--confcutdir",
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
    assert payload["validation_provenance_binding_status"] == "PASS"
    assert payload["performance_evidence_status"] == "PASS"
    assert payload["validation_provenance"] == provenance
    assert payload["scheduler"]["applied"] is True
    assert payload["scheduler"]["policy"] == "complete_full_duration_descending_stable"
    assert payload["scheduler"]["manifest_status"] == "COMPLETE"
    assert payload["scheduler"]["complete_collection_verified"] is True
    assert payload["scheduler"]["duration_order_verified"] is True
    assert payload["scheduler"]["matched_tracked_file_count"] == 16
    assert payload["scheduler"]["matched_tracked_node_count"] == 16
    assert payload["collection"]["nodeids"][:2] == [
        "tests/test_layer1_meta_policy_readiness.py::test_runtime_profile_smoke_0",
        "tests/test_refined_method_proposal.py::test_runtime_profile_smoke_1",
    ]
    assert payload["collection"]["nodeids"][2:] == [
        f"tests/{file_name}::test_runtime_profile_smoke_{index}"
        for index, file_name in enumerate(untracked_files, start=2)
    ]
    assert payload["worker_count"] == 16

    stale_file = suite_dir / untracked_files[-1]
    stale_file.write_text(
        "def test_runtime_profile_smoke_changed():\n    assert True\n",
        encoding="utf-8",
    )
    stale_output_path = tmp_path / "stale_test_runtime_profile.json"
    env[RUNTIME_PROFILE_OUTPUT_ENV] = str(stale_output_path)
    stale_completed = subprocess.run(
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
            str(complete_profile_path.resolve()),
            "--rootdir",
            str(tmp_path),
            "--confcutdir",
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
    assert stale_completed.returncode == 0, stale_completed.stdout + stale_completed.stderr
    stale_payload = json.loads(stale_output_path.read_text(encoding="utf-8"))
    assert stale_payload["scheduler"]["policy"] == "stock_loadfile_test_count_order"
    assert stale_payload["scheduler"]["applied"] is False
    assert stale_payload["scheduler"]["fallback"] is True
    assert stale_payload["scheduler"]["complete_collection_verified"] is False
    assert stale_payload["validation_provenance_binding_status"] == "PASS"
    assert stale_payload["performance_evidence_status"] == "FAIL"


def test_real_xdist_plugin_fails_closed_for_missing_or_malformed_provenance(
    tmp_path: Path,
) -> None:
    repo_root = Path.cwd().resolve()
    suite_dir = tmp_path / "tests"
    suite_dir.mkdir()
    nodeids: list[str] = []
    observed_seconds: dict[str, float] = {}
    for index in range(16):
        file_name = f"test_provenance_{index:02d}.py"
        (suite_dir / file_name).write_text(
            f"def test_provenance_{index:02d}():\n    assert True\n",
            encoding="utf-8",
        )
        nodeid = f"tests/{file_name}::test_provenance_{index:02d}"
        nodeids.append(nodeid)
        observed_seconds[f"tests/{file_name}"] = float(16 - index)
    complete_profile_path = _write_complete_profile(
        tmp_path / "complete_duration_profile.yaml",
        nodeids=nodeids,
        observed_seconds=observed_seconds,
    )

    base_env = dict(os.environ)
    base_env[RUNTIME_PROFILE_FORMAL_SELECTION_ENV] = "1"
    python_path_parts = [str(repo_root), str(repo_root / "src")]
    if base_env.get("PYTHONPATH"):
        python_path_parts.append(base_env["PYTHONPATH"])
    base_env["PYTHONPATH"] = os.pathsep.join(python_path_parts)
    invalid_cases = [
        ("missing", None, "validation provenance environment is missing"),
        ("malformed", "{", "validation provenance environment is invalid"),
    ]

    for case_name, raw_provenance, expected_warning in invalid_cases:
        output_path = tmp_path / f"{case_name}_test_runtime_profile.json"
        env = dict(base_env)
        env[RUNTIME_PROFILE_OUTPUT_ENV] = str(output_path)
        if raw_provenance is None:
            env.pop(RUNTIME_PROFILE_VALIDATION_PROVENANCE_ENV, None)
        else:
            env[RUNTIME_PROFILE_VALIDATION_PROVENANCE_ENV] = raw_provenance

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
                str(complete_profile_path.resolve()),
                "--rootdir",
                str(tmp_path),
                "--confcutdir",
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
        assert payload["scheduler"]["applied"] is True
        assert payload["scheduler"]["duration_order_verified"] is True
        assert payload["validation_provenance_binding_status"] == "FAIL"
        assert payload["performance_evidence_status"] == "FAIL"
        assert payload["pytest_exitstatus"] == 0
        assert payload["pytest_outcome_authoritative"] is True
        assert payload["pytest_outcome_overridden"] is False
        assert payload["validation_provenance"] is None
        assert any(expected_warning in warning for warning in payload["warnings"])


def test_runtime_sidecar_write_failure_preserves_pytest_exit(tmp_path: Path) -> None:
    repo_root = Path.cwd().resolve()
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
    env[RUNTIME_PROFILE_VALIDATION_PROVENANCE_ENV] = _full_validation_provenance_json(
        boundary_id="real-xdist-write-failure",
    )
    python_path_parts = [str(repo_root), str(repo_root / "src")]
    if env.get("PYTHONPATH"):
        python_path_parts.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(python_path_parts)

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "scripts.pytest_runtime_profile",
            "--aits-duration-profile",
            str((repo_root / PROFILE_PATH).resolve()),
            "--rootdir",
            str(tmp_path),
            "--confcutdir",
            str(tmp_path),
            "write_failure_suite",
            "-q",
        ],
        cwd=tmp_path,
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
