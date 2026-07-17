from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

import scripts.run_validation_tier as validation_tier
from scripts.run_validation_tier import (
    FULL_DURATION_PROFILE_MANIFEST,
    FULL_RUNTIME_PROFILE_PLUGIN,
    RUNTIME_PROFILE_OUTPUT_ENV,
    RUNTIME_PROFILE_OUTPUT_NAME,
    TIER_SPECS,
    _parse_pytest_slow_durations,
    _read_runtime_profile_payload,
    _summarize_runtime_profile,
    build_command,
    resolve_tier,
)


def test_validation_tier_print_only_writes_command_summary(tmp_path: Path) -> None:
    report_path = tmp_path / "validation_tier.json"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_validation_tier.py",
            "fast",
            "--print-only",
            "--json-output",
            str(report_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "Validation tier: fast" in completed.stdout
    assert "Resolved tier: fast-unit" in completed.stdout
    assert "Promotion blocking: True" in completed.stdout
    assert "Workers: 16" in completed.stdout
    assert "-n 16 --dist loadfile" in completed.stdout
    assert "tests/test_documentation_contract.py" in completed.stdout.replace("\\", "/")

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["tier"] == "fast"
    assert payload["resolved_tier"] == "fast-unit"
    assert payload["status"] == "PRINT_ONLY"
    assert payload["promotion_blocking"] is True
    assert payload["slow_suite_allowed"] is False
    assert payload["production_effect"] == "none"
    assert payload["strategy_logic_changed"] is False
    assert payload["broker_action_allowed"] is False
    assert payload["can_support_promotion_evidence"] is False
    assert payload["workers"] == "16"
    assert payload["dist"] == "loadfile"
    assert "tests/test_report_index.py" in " ".join(payload["command"]).replace("\\", "/")
    assert "tests/test_clean_clone_release_acceptance.py" in " ".join(
        payload["command"]
    ).replace("\\", "/")
    assert "tests/test_engineering_release_candidate.py" in " ".join(
        payload["command"]
    ).replace("\\", "/")
    assert "-n 16 --dist loadfile" in " ".join(payload["command"])


def test_validation_tier_can_render_serial_command(tmp_path: Path) -> None:
    report_path = tmp_path / "validation_tier_serial.json"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_validation_tier.py",
            "fast",
            "--print-only",
            "--workers",
            "1",
            "--json-output",
            str(report_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    command = " ".join(payload["command"])

    assert payload["workers"] == "1"
    assert "-n" not in command
    assert "--dist" not in command


def test_full_command_preserves_loadfile_and_enables_duration_profile() -> None:
    command = build_command(
        "full",
        python_executable="python",
        repo_root=Path.cwd(),
        workers="16",
        dist="loadfile",
        extra_pytest_args=("--loadscope-reorder",),
    )
    rendered = " ".join(command).replace("\\", "/")

    assert "-n 16 --dist loadfile" in rendered
    assert f"-p {FULL_RUNTIME_PROFILE_PLUGIN}" in rendered
    assert f"--aits-duration-profile {FULL_DURATION_PROFILE_MANIFEST}" in rendered
    assert command[-1] == "--no-loadscope-reorder"
    assert rendered.count("tests") >= 1


def test_runtime_profile_summary_does_not_embed_node_rows(tmp_path: Path) -> None:
    profile_payload = {
        "schema_version": "test_runtime_profile.v1",
        "profile_status": "PASS",
        "telemetry_status": "PASS",
        "performance_evidence_status": "PASS",
        "stable_full_improvement_claimed": False,
        "collection": {"count": 2, "set_sha256": "abc"},
        "scheduler": {
            "policy": "tracked_partial_seed_duration_descending_stable",
            "applied": True,
            "fallback": False,
            "fallback_reason": None,
        },
        "node_count": 2,
        "file_count": 2,
        "worker_count": 2,
        "tail_idle_total_seconds": 0.5,
        "tail_idle_max_seconds": 0.5,
        "nodes": [{"nodeid": "tests/test_a.py::test_a"}],
        "warnings": [],
    }

    summary = _summarize_runtime_profile(
        profile_payload,
        final_path=tmp_path / "test_runtime_profile.json",
    )

    assert summary["runtime_profile_status"] == "PASS"
    assert summary["runtime_profile_summary"]["collection_count"] == 2
    assert summary["runtime_profile_summary"]["scheduler_applied"] is True
    assert "nodes" not in summary["runtime_profile_summary"]


def _runtime_profile_payload(*, pytest_exitstatus: object) -> dict[str, object]:
    nodeid = "tests/test_layer1_meta_policy_readiness.py::test_runtime_profile_stub"
    file_path = nodeid.split("::", 1)[0]
    ordered_sha256 = hashlib.sha256(nodeid.encode("utf-8")).hexdigest()
    duration_profile_path = Path(FULL_DURATION_PROFILE_MANIFEST).resolve()
    duration_profile = validation_tier.safe_load_yaml_path(duration_profile_path)
    duration_source = duration_profile["source"]
    phases = [
        {
            "phase": "setup",
            "start_utc": "1970-01-01T00:00:01Z",
            "stop_utc": "1970-01-01T00:00:01.100000Z",
            "start_epoch_seconds": 1.0,
            "stop_epoch_seconds": 1.1,
            "duration_seconds": 0.1,
            "outcome": "passed",
            "worker_id": "gw0",
        },
        {
            "phase": "call",
            "start_utc": "1970-01-01T00:00:01.100000Z",
            "stop_utc": "1970-01-01T00:00:01.200000Z",
            "start_epoch_seconds": 1.1,
            "stop_epoch_seconds": 1.2,
            "duration_seconds": 0.1,
            "outcome": "passed",
            "worker_id": "gw0",
        },
        {
            "phase": "teardown",
            "start_utc": "1970-01-01T00:00:01.200000Z",
            "stop_utc": "1970-01-01T00:00:01.300000Z",
            "start_epoch_seconds": 1.2,
            "stop_epoch_seconds": 1.3,
            "duration_seconds": 0.1,
            "outcome": "passed",
            "worker_id": "gw0",
        },
    ]
    identity = {
        "count": 1,
        "ordered_sha256": ordered_sha256,
        "set_sha256": ordered_sha256,
        "duplicate_nodeids": [],
    }
    return {
        "schema_version": "test_runtime_profile.v1",
        "report_type": "test_runtime_profile",
        "profile_status": "PASS",
        "telemetry_status": "PASS",
        "performance_evidence_status": "FAIL",
        "stable_full_improvement_claimed": False,
        "pytest_exitstatus": pytest_exitstatus,
        "pytest_outcome_authoritative": True,
        "pytest_outcome_overridden": False,
        "started_at_utc": "1970-01-01T00:00:00Z",
        "ended_at_utc": "1970-01-01T00:00:02Z",
        "elapsed_seconds": 2.0,
        "collection": {
            **identity,
            "complete": True,
            "expected_worker_count": 1,
            "observed_worker_count": 1,
            "worker_identities": {"gw0": identity},
            "nodeids": [nodeid],
        },
        "scheduler": {
            "policy": "non_loadfile_collection_order_preserved",
            "applied": False,
            "fallback": False,
            "fallback_reason": "duration-aware scheduling requires pytest-xdist loadfile",
            "configured_manifest_path": str(duration_profile_path),
            "manifest_sha256": hashlib.sha256(duration_profile_path.read_bytes()).hexdigest(),
            "manifest_schema_version": duration_profile["schema_version"],
            "profile_id": duration_profile["profile_id"],
            "owner": duration_profile["owner"],
            "version": duration_profile["version"],
            "partial_seed": duration_profile["partial_seed"]["enabled"],
            "tracked_file_count": len(duration_profile["files"]),
            "source_artifact_path": duration_source["artifact_path"],
            "source_artifact_sha256": duration_source["artifact_sha256"],
            "file_internal_node_order_preserved": True,
            "duration_order_verified": True,
            "matched_tracked_file_count": 1,
            "matched_tracked_node_count": 1,
            "expected_ordered_sha256": ordered_sha256,
            "equal_duration_tie_policy": "stable_first_seen_file_order",
            "untracked_file_weight_seconds": 0.0,
            "expected_worker_count": 1,
            "xdist_dist": "no",
            "loadscope_reorder_disabled": True,
            "formal_full_selection_eligible": True,
        },
        "telemetry": {
            "complete": True,
            "phase_report_count": 3,
            "invalid_phase_report_count": 0,
            "reported_node_count": 1,
            "missing_nodeids": [],
            "extra_nodeids": [],
            "duplicate_phase_nodeids": [],
            "missing_required_phase_nodeids": [],
            "inconsistent_worker_nodeids": [],
            "inactive_worker_ids": [],
            "unexpected_runtime_worker_ids": [],
        },
        "outcome_counts": {"passed": 1},
        "node_count": 1,
        "file_count": 1,
        "worker_count": 1,
        "observed_test_window_seconds": 0.3,
        "tail_idle_total_seconds": 0.0,
        "tail_idle_max_seconds": 0.0,
        "nodes": [
            {
                "nodeid": nodeid,
                "file": file_path,
                "worker_id": "gw0",
                "outcome": "passed",
                "start_utc": "1970-01-01T00:00:01Z",
                "stop_utc": "1970-01-01T00:00:01.300000Z",
                "start_epoch_seconds": 1.0,
                "stop_epoch_seconds": 1.3,
                "duration_seconds": 0.3,
                "phases": phases,
            }
        ],
        "files": [
            {
                "path": file_path,
                "node_count": 1,
                "worker_ids": ["gw0"],
                "duration_seconds": 0.3,
                "start_utc": "1970-01-01T00:00:01Z",
                "stop_utc": "1970-01-01T00:00:01.300000Z",
                "elapsed_envelope_seconds": 0.3,
            }
        ],
        "workers": [
            {
                "worker_id": "gw0",
                "node_count": 1,
                "first_start_utc": "1970-01-01T00:00:01Z",
                "last_stop_utc": "1970-01-01T00:00:01.300000Z",
                "busy_seconds": 0.3,
                "span_seconds": 0.3,
                "internal_idle_seconds": 0.0,
                "tail_idle_seconds": 0.0,
            }
        ],
        "warnings": ["duration-aware scheduling was not eligible"],
        "strategy_logic_changed": False,
        "production_effect": "none",
        "cached_data_mutated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
    }


@pytest.mark.parametrize(
    ("sidecar_exitstatus", "subprocess_exitstatus"),
    [(True, 1), ("7", 7), (None, 7), (0, 7)],
)
def test_runtime_profile_exitstatus_must_be_matching_non_bool_int(
    tmp_path: Path,
    sidecar_exitstatus: object,
    subprocess_exitstatus: int,
) -> None:
    profile_path = tmp_path / RUNTIME_PROFILE_OUTPUT_NAME
    profile_path.write_text(
        json.dumps(_runtime_profile_payload(pytest_exitstatus=sidecar_exitstatus)),
        encoding="utf-8",
    )

    payload = _read_runtime_profile_payload(
        profile_path,
        pytest_exitstatus=subprocess_exitstatus,
    )

    assert payload["profile_status"] == "FAIL"
    assert payload["performance_evidence_status"] == "FAIL"
    assert payload["pytest_exitstatus"] == subprocess_exitstatus
    reason = payload["warnings"][0]
    assert f"sidecar={sidecar_exitstatus!r}" in reason
    assert f"subprocess={subprocess_exitstatus!r}" in reason
    assert payload["pytest_outcome_overridden"] is False


def test_runtime_profile_matching_integer_exitstatus_is_preserved(tmp_path: Path) -> None:
    profile_path = tmp_path / RUNTIME_PROFILE_OUTPUT_NAME
    expected = _runtime_profile_payload(pytest_exitstatus=7)
    profile_path.write_text(json.dumps(expected), encoding="utf-8")

    assert _read_runtime_profile_payload(profile_path, pytest_exitstatus=7) == expected


def test_runtime_profile_rejects_incomplete_flag_only_pass(tmp_path: Path) -> None:
    profile_path = tmp_path / RUNTIME_PROFILE_OUTPUT_NAME
    profile_path.write_text(
        json.dumps(
            {
                "schema_version": "test_runtime_profile.v1",
                "report_type": "test_runtime_profile",
                "profile_status": "PASS",
                "telemetry_status": "PASS",
                "performance_evidence_status": "PASS",
                "pytest_exitstatus": 0,
            }
        ),
        encoding="utf-8",
    )

    payload = _read_runtime_profile_payload(profile_path, pytest_exitstatus=0)

    assert payload["profile_status"] == "FAIL"
    assert payload["performance_evidence_status"] == "FAIL"
    assert "runtime profile contract is invalid" in payload["warnings"][0]


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (lambda payload: payload.__setitem__("performance_evidence_status", "PASS"), "performance"),
        (
            lambda payload: payload["collection"].__setitem__("set_sha256", "0" * 64),
            "collection identity",
        ),
        (
            lambda payload: payload["telemetry"].pop("missing_nodeids"),
            "telemetry.missing_nodeids",
        ),
        (lambda payload: payload.__setitem__("cached_data_mutated", True), "cached_data_mutated"),
    ],
)
def test_runtime_profile_cross_field_drift_fails_closed(
    tmp_path: Path,
    mutation: Callable[[dict[str, object]], None],
    reason: str,
) -> None:
    profile_path = tmp_path / RUNTIME_PROFILE_OUTPUT_NAME
    payload = _runtime_profile_payload(pytest_exitstatus=0)
    mutation(payload)
    profile_path.write_text(json.dumps(payload), encoding="utf-8")

    observed = _read_runtime_profile_payload(profile_path, pytest_exitstatus=0)

    assert observed["profile_status"] == "FAIL"
    assert observed["performance_evidence_status"] == "FAIL"
    assert reason in observed["warnings"][0]


@pytest.mark.parametrize(
    "invalid_json",
    [
        '{"schema_version":"test_runtime_profile.v1","schema_version":"duplicate"}',
        '{"schema_version":"test_runtime_profile.v1","pytest_exitstatus":NaN}',
    ],
)
def test_runtime_profile_rejects_duplicate_keys_and_non_finite_json(
    tmp_path: Path,
    invalid_json: str,
) -> None:
    profile_path = tmp_path / RUNTIME_PROFILE_OUTPUT_NAME
    profile_path.write_text(invalid_json, encoding="utf-8")

    payload = _read_runtime_profile_payload(profile_path, pytest_exitstatus=0)

    assert payload["profile_status"] == "FAIL"
    assert payload["performance_evidence_status"] == "FAIL"


def test_runtime_profile_binds_runner_manifest_and_full_file_contract(tmp_path: Path) -> None:
    profile_path = tmp_path / RUNTIME_PROFILE_OUTPUT_NAME
    expected = _runtime_profile_payload(pytest_exitstatus=0)
    profile_path.write_text(json.dumps(expected), encoding="utf-8")
    expected_files = {"tests/test_layer1_meta_policy_readiness.py"}

    accepted = _read_runtime_profile_payload(
        profile_path,
        pytest_exitstatus=0,
        expected_worker_count=1,
        expected_dist="no",
        formal_selection_eligible=True,
        duration_profile_path=Path(FULL_DURATION_PROFILE_MANIFEST),
        expected_test_files=expected_files,
    )
    rejected = _read_runtime_profile_payload(
        profile_path,
        pytest_exitstatus=0,
        expected_worker_count=1,
        expected_dist="no",
        formal_selection_eligible=True,
        duration_profile_path=Path(FULL_DURATION_PROFILE_MANIFEST),
        expected_test_files={"tests/test_different.py"},
    )

    assert accepted == expected
    assert rejected["profile_status"] == "FAIL"
    assert "full test manifest" in rejected["warnings"][0]


def test_runtime_profile_rejects_empty_required_phase_telemetry(tmp_path: Path) -> None:
    profile_path = tmp_path / RUNTIME_PROFILE_OUTPUT_NAME
    payload = _runtime_profile_payload(pytest_exitstatus=0)
    payload["nodes"][0]["phases"] = []  # type: ignore[index]
    payload["nodes"][0]["outcome"] = "unknown"  # type: ignore[index]
    payload["outcome_counts"] = {"unknown": 1}
    payload["telemetry"]["phase_report_count"] = 0  # type: ignore[index]
    profile_path.write_text(json.dumps(payload), encoding="utf-8")

    observed = _read_runtime_profile_payload(profile_path, pytest_exitstatus=0)

    assert observed["profile_status"] == "FAIL"
    assert observed["performance_evidence_status"] == "FAIL"
    assert "required phases are missing" in observed["warnings"][0]


def test_runtime_profile_rejects_failed_node_with_passing_exit(tmp_path: Path) -> None:
    profile_path = tmp_path / RUNTIME_PROFILE_OUTPUT_NAME
    payload = _runtime_profile_payload(pytest_exitstatus=0)
    phases = payload["nodes"][0]["phases"]  # type: ignore[index]
    next(phase for phase in phases if phase["phase"] == "call")["outcome"] = "failed"
    payload["nodes"][0]["outcome"] = "failed"  # type: ignore[index]
    payload["outcome_counts"] = {"failed": 1}
    profile_path.write_text(json.dumps(payload), encoding="utf-8")

    observed = _read_runtime_profile_payload(profile_path, pytest_exitstatus=0)

    assert observed["profile_status"] == "FAIL"
    assert observed["performance_evidence_status"] == "FAIL"
    assert "passing pytest exit contains failed node outcomes" in observed["warnings"][0]


def test_runtime_profile_rejects_node_timing_aggregate_drift(tmp_path: Path) -> None:
    profile_path = tmp_path / RUNTIME_PROFILE_OUTPUT_NAME
    payload = _runtime_profile_payload(pytest_exitstatus=0)
    payload["nodes"][0]["duration_seconds"] = 99.0  # type: ignore[index]
    profile_path.write_text(json.dumps(payload), encoding="utf-8")

    observed = _read_runtime_profile_payload(profile_path, pytest_exitstatus=0)

    assert observed["profile_status"] == "FAIL"
    assert observed["performance_evidence_status"] == "FAIL"
    assert "node timing is not phase-derived" in observed["warnings"][0]


@pytest.mark.parametrize(
    ("section", "identity_key", "replacement"),
    [
        ("files", "path", "tests/not_collected.py"),
        ("workers", "worker_id", "gw999"),
    ],
)
def test_runtime_profile_rejects_unmatched_aggregate_identity_without_raising(
    tmp_path: Path,
    section: str,
    identity_key: str,
    replacement: str,
) -> None:
    profile_path = tmp_path / RUNTIME_PROFILE_OUTPUT_NAME
    payload = _runtime_profile_payload(pytest_exitstatus=0)
    payload[section][0][identity_key] = replacement  # type: ignore[index]
    profile_path.write_text(json.dumps(payload), encoding="utf-8")

    observed = _read_runtime_profile_payload(profile_path, pytest_exitstatus=0)

    assert observed["profile_status"] == "FAIL"
    assert observed["pytest_exitstatus"] == 0
    assert "aggregate is not node-derived" in observed["warnings"][0]


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (lambda payload: payload.pop("started_at_utc"), "session UTC window"),
        (lambda payload: payload.__setitem__("elapsed_seconds", 0.0), "elapsed_seconds"),
    ],
)
def test_runtime_profile_rejects_session_timing_drift(
    tmp_path: Path,
    mutation: Callable[[dict[str, object]], object],
    reason: str,
) -> None:
    profile_path = tmp_path / RUNTIME_PROFILE_OUTPUT_NAME
    payload = _runtime_profile_payload(pytest_exitstatus=0)
    mutation(payload)
    profile_path.write_text(json.dumps(payload), encoding="utf-8")

    observed = _read_runtime_profile_payload(profile_path, pytest_exitstatus=0)

    assert observed["profile_status"] == "FAIL"
    assert observed["pytest_exitstatus"] == 0
    assert reason in observed["warnings"][0]


def test_runtime_profile_rejects_phase_utc_drift(tmp_path: Path) -> None:
    profile_path = tmp_path / RUNTIME_PROFILE_OUTPUT_NAME
    payload = _runtime_profile_payload(pytest_exitstatus=0)
    payload["nodes"][0]["phases"][0]["start_utc"] = "2099-01-01T00:00:00Z"  # type: ignore[index]
    profile_path.write_text(json.dumps(payload), encoding="utf-8")

    observed = _read_runtime_profile_payload(profile_path, pytest_exitstatus=0)

    assert observed["profile_status"] == "FAIL"
    assert "phase UTC timing is not epoch-derived" in observed["warnings"][0]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("policy", "non_loadfile_collection_order_preserved"),
        ("equal_duration_tie_policy", "random"),
        ("untracked_file_weight_seconds", 999.0),
        ("fallback_reason", "fake"),
    ],
)
def test_runtime_profile_rejects_formal_scheduler_semantic_drift(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    profile_path = tmp_path / RUNTIME_PROFILE_OUTPUT_NAME
    payload = _runtime_profile_payload(pytest_exitstatus=0)
    scheduler = payload["scheduler"]  # type: ignore[assignment]
    scheduler.update(
        {
            "policy": "tracked_partial_seed_duration_descending_stable",
            "applied": True,
            "fallback": False,
            "fallback_reason": None,
            "xdist_dist": "loadfile",
        }
    )
    scheduler[field] = value
    payload["performance_evidence_status"] = "PASS"
    payload["warnings"] = []
    profile_path.write_text(json.dumps(payload), encoding="utf-8")

    observed = _read_runtime_profile_payload(profile_path, pytest_exitstatus=0)

    assert observed["profile_status"] == "FAIL"
    assert observed["performance_evidence_status"] == "FAIL"
    assert "formal scheduler contract" in observed["warnings"][0]


def test_runtime_profile_contract_exception_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_path = tmp_path / RUNTIME_PROFILE_OUTPUT_NAME
    profile_path.write_text(
        json.dumps(_runtime_profile_payload(pytest_exitstatus=0)), encoding="utf-8"
    )

    def raise_contract_error(*args: object, **kwargs: object) -> str | None:
        del args, kwargs
        raise OverflowError("malformed epoch")

    monkeypatch.setattr(
        validation_tier, "_runtime_profile_contract_error", raise_contract_error
    )

    observed = _read_runtime_profile_payload(profile_path, pytest_exitstatus=0)

    assert observed["profile_status"] == "FAIL"
    assert observed["pytest_exitstatus"] == 0
    assert "contract evaluation failed closed" in observed["warnings"][0]


def test_full_runtime_summary_hashes_final_sidecar_before_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_dir = tmp_path / "runtime_artifact"

    def fake_run_command(
        command: list[str],
        *,
        cwd: Path,
        env_overrides: dict[str, str] | None = None,
    ) -> dict[str, object]:
        del cwd
        assert env_overrides is not None
        profile_path = Path(env_overrides[RUNTIME_PROFILE_OUTPUT_ENV])
        profile_path.write_text(
            json.dumps(_runtime_profile_payload(pytest_exitstatus=0)),
            encoding="utf-8",
        )
        return {
            "command": command,
            "exit_code": 0,
            "elapsed_seconds": 0.01,
            "pytest_output": "stub pytest output\n",
        }

    monkeypatch.setattr(validation_tier, "_run_command", fake_run_command)
    monkeypatch.setattr(
        validation_tier,
        "_load_expected_full_test_files",
        lambda path: ({"tests/test_layer1_meta_policy_readiness.py"}, None),
    )

    exit_code = validation_tier.main(
        [
            "full",
            "--workers",
            "1",
            "--write-runtime-artifact",
            "--artifact-dir",
            str(artifact_dir),
        ]
    )

    assert exit_code == 0
    sidecar_path = artifact_dir / RUNTIME_PROFILE_OUTPUT_NAME
    summary_path = artifact_dir / "test_runtime_summary.json"
    reader_path = artifact_dir / "test_runtime_reader_brief.md"
    log_path = artifact_dir / "pytest_output.log"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    records = {row["path"]: row for row in summary["output_artifacts"]}
    summary_record = records[str(summary_path)]
    assert summary_record["exists"] is True
    assert summary_record["sha256"] is None
    assert summary_record["size_bytes"] is None
    assert summary_record["integrity_status"] == "SELF_REFERENCE_NOT_EMBEDDED"
    for path in (reader_path, log_path, sidecar_path):
        artifact_bytes = path.read_bytes()
        assert records[str(path)] == {
            "path": str(path),
            "exists": True,
            "artifact_type": path.suffix.lstrip("."),
            "sha256": hashlib.sha256(artifact_bytes).hexdigest(),
            "size_bytes": len(artifact_bytes),
            "file_count": None,
        }
    sidecar_record = records[str(sidecar_path)]
    sidecar_bytes = sidecar_path.read_bytes()
    assert sidecar_record["sha256"] == hashlib.sha256(sidecar_bytes).hexdigest()


def test_sidecar_exit_mismatch_fails_evidence_without_overriding_subprocess_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_dir = tmp_path / "runtime_artifact"

    def fake_run_command(
        command: list[str],
        *,
        cwd: Path,
        env_overrides: dict[str, str] | None = None,
    ) -> dict[str, object]:
        del cwd
        assert env_overrides is not None
        profile_path = Path(env_overrides[RUNTIME_PROFILE_OUTPUT_ENV])
        profile_path.write_text(
            json.dumps(_runtime_profile_payload(pytest_exitstatus=0)),
            encoding="utf-8",
        )
        return {
            "command": command,
            "exit_code": 7,
            "elapsed_seconds": 0.01,
            "pytest_output": "",
        }

    monkeypatch.setattr(validation_tier, "_run_command", fake_run_command)
    monkeypatch.setattr(
        validation_tier,
        "_load_expected_full_test_files",
        lambda path: ({"tests/test_layer1_meta_policy_readiness.py"}, None),
    )

    exit_code = validation_tier.main(
        [
            "full",
            "--workers",
            "1",
            "--write-runtime-artifact",
            "--artifact-dir",
            str(artifact_dir),
        ]
    )

    assert exit_code == 7
    sidecar = json.loads(
        (artifact_dir / RUNTIME_PROFILE_OUTPUT_NAME).read_text(encoding="utf-8")
    )
    summary = json.loads(
        (artifact_dir / "test_runtime_summary.json").read_text(encoding="utf-8")
    )
    assert sidecar["profile_status"] == "FAIL"
    assert sidecar["pytest_exitstatus"] == 7
    assert "sidecar=0 subprocess=7" in sidecar["warnings"][0]
    assert summary["exit_code"] == 7
    assert summary["status"] == "FAIL"
    assert summary["runtime_profile_summary"]["performance_evidence_status"] == "FAIL"


def test_sidecar_write_failure_does_not_override_subprocess_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_dir = tmp_path / "runtime_artifact"
    original_write_report = validation_tier._write_report

    def fake_run_command(
        command: list[str],
        *,
        cwd: Path,
        env_overrides: dict[str, str] | None = None,
    ) -> dict[str, object]:
        del cwd
        assert env_overrides is not None
        profile_path = Path(env_overrides[RUNTIME_PROFILE_OUTPUT_ENV])
        profile_path.write_text(
            json.dumps(_runtime_profile_payload(pytest_exitstatus=0)),
            encoding="utf-8",
        )
        return {
            "command": command,
            "exit_code": 0,
            "elapsed_seconds": 0.01,
            "pytest_output": "",
        }

    def fail_sidecar_write(path: Path, payload: dict[str, object]) -> None:
        if RUNTIME_PROFILE_OUTPUT_NAME in path.name:
            raise OSError("simulated sidecar write failure")
        original_write_report(path, payload)

    monkeypatch.setattr(validation_tier, "_run_command", fake_run_command)
    monkeypatch.setattr(validation_tier, "_write_report", fail_sidecar_write)
    monkeypatch.setattr(
        validation_tier,
        "_load_expected_full_test_files",
        lambda path: ({"tests/test_layer1_meta_policy_readiness.py"}, None),
    )

    exit_code = validation_tier.main(
        [
            "full",
            "--workers",
            "1",
            "--write-runtime-artifact",
            "--artifact-dir",
            str(artifact_dir),
        ]
    )

    assert exit_code == 0
    summary = json.loads(
        (artifact_dir / "test_runtime_summary.json").read_text(encoding="utf-8")
    )
    sidecar_record = next(
        row
        for row in summary["output_artifacts"]
        if row["path"] == str(artifact_dir / RUNTIME_PROFILE_OUTPUT_NAME)
    )
    assert sidecar_record["exists"] is False
    assert summary["exit_code"] == 0
    assert summary["status"] == "PASS"
    assert summary["runtime_profile_status"] == "FAIL"
    assert summary["runtime_profile_summary"]["performance_evidence_status"] == "FAIL"


def test_runtime_artifacts_are_written_for_print_only(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "runtime_artifact"
    artifact_dir.mkdir()
    output_log_path = artifact_dir / "pytest_output.log"
    output_log_path.write_text("stale output", encoding="utf-8")
    json_output_path = tmp_path / "runtime_artifact_copy.json"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_validation_tier.py",
            "contract-validation",
            "--print-only",
            "--write-runtime-artifact",
            "--artifact-dir",
            str(artifact_dir),
            "--json-output",
            str(json_output_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    summary_path = artifact_dir / "test_runtime_summary.json"
    reader_brief_path = artifact_dir / "test_runtime_reader_brief.md"
    assert summary_path.exists()
    assert reader_brief_path.exists()

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    json_output_payload = json.loads(json_output_path.read_text(encoding="utf-8"))
    reader_brief = reader_brief_path.read_text(encoding="utf-8")
    assert payload["report_type"] == "test_runtime_summary"
    assert payload["git_commit"]
    assert payload["resolved_config"] == {"validation_tier": "contract-validation"}
    assert payload["as_of"]
    assert payload["random_seed"] == "not_applicable"
    assert payload["environment_summary"]["python_version"]
    assert payload["schema_versions"]["test_runtime_summary"] == "1"
    assert payload["input_artifacts"]
    assert payload["input_checksums"]
    assert payload["output_artifacts"]
    assert json_output_payload["output_artifacts"] == payload["output_artifacts"]
    assert output_log_path.read_bytes() == b""
    output_records = {row["path"]: row for row in payload["output_artifacts"]}
    assert output_records[str(output_log_path)]["exists"] is True
    assert output_records[str(output_log_path)]["size_bytes"] == 0
    assert output_records[str(output_log_path)]["sha256"] == hashlib.sha256(b"").hexdigest()
    assert payload["warnings"] == ["validation_status=PRINT_ONLY"]
    assert payload["resolved_tier"] == "contract-validation"
    assert payload["suite_family"] == "contract_validation"
    assert payload["promotion_blocking"] is True
    assert payload["status"] == "PRINT_ONLY"
    assert "test_runtime_reader_brief.md" in payload["reader_brief_path"]
    assert "Can support promotion evidence: `False`" in reader_brief
    assert "Production effect: `none`" in reader_brief


def test_json_output_cannot_overwrite_managed_runtime_artifact(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "runtime_artifact"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_validation_tier.py",
            "contract-validation",
            "--print-only",
            "--write-runtime-artifact",
            "--artifact-dir",
            str(artifact_dir),
            "--json-output",
            str(artifact_dir / "pytest_output.log"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    assert "must not overwrite a managed runtime artifact path" in completed.stderr
    assert not artifact_dir.exists()


def test_pytest_slow_duration_parser_extracts_duration_rows() -> None:
    durations = _parse_pytest_slow_durations(
        """
        ============================= slowest 3 durations =============================
        12.34s call     tests/test_example.py::test_slow_case
        2.50s setup    tests/test_other.py::test_setup
        0.99s teardown tests/test_other.py::test_teardown
        """
    )

    assert durations == [
        {
            "seconds": 12.34,
            "phase": "call",
            "nodeid": "tests/test_example.py::test_slow_case",
        },
        {
            "seconds": 2.5,
            "phase": "setup",
            "nodeid": "tests/test_other.py::test_setup",
        },
        {
            "seconds": 0.99,
            "phase": "teardown",
            "nodeid": "tests/test_other.py::test_teardown",
        },
    ]


def test_validation_tier_print_only_can_render_benchmark_variants(tmp_path: Path) -> None:
    report_path = tmp_path / "validation_benchmark_plan.json"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_validation_tier.py",
            "fast",
            "--print-only",
            "--benchmark-dist",
            "loadfile,worksteal",
            "--json-output",
            str(report_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "Benchmark variants: 2" in completed.stdout
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    variants = payload["benchmark_runs"]

    assert payload["benchmark_mode"] is True
    assert payload["benchmark_variant_count"] == 2
    assert [variant["dist"] for variant in variants] == ["loadfile", "worksteal"]
    assert all(variant["status"] == "PRINT_ONLY" for variant in variants)
    assert "--dist worksteal" in " ".join(variants[1]["command"])


def test_runtime_artifact_records_pytest_output_and_slow_durations(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "runtime_artifact"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_validation_tier.py",
            "fast",
            "--write-runtime-artifact",
            "--artifact-dir",
            str(artifact_dir),
            "--workers",
            "1",
            "--pytest-arg=-k",
            "--pytest-arg",
            "test_pytest_slow_duration_parser_extracts_duration_rows",
            "--pytest-arg=-s",
            "--pytest-arg=--durations=1",
            "--pytest-arg=--durations-min=0",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    summary_path = artifact_dir / "test_runtime_summary.json"
    output_log_path = artifact_dir / "pytest_output.log"
    reader_brief_path = artifact_dir / "test_runtime_reader_brief.md"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    reader_brief = reader_brief_path.read_text(encoding="utf-8")

    assert output_log_path.exists()
    assert payload["pytest_output_captured"] is True
    assert payload["pytest_output_log_path"].endswith("pytest_output.log")
    assert payload["pytest_slow_duration_count"] >= 1
    assert payload["pytest_slow_durations"][0]["nodeid"].endswith(
        "test_pytest_slow_duration_parser_extracts_duration_rows"
    )
    assert "## Slow Durations" in reader_brief
    assert "## Pytest Output" in reader_brief


def test_formal_suite_contracts_are_registered() -> None:
    expected = {
        "fast-unit": ("fast_unit", True, False),
        "contract-validation": ("contract_validation", True, False),
        "report-validation": ("report_validation", True, False),
        "integration": ("integration", False, True),
        "reproducibility": ("reproducibility", True, False),
        "slow-research-regression": ("slow_research_regression", False, True),
        "full": ("full_pytest", True, True),
    }

    for tier, (suite_family, promotion_blocking, slow_allowed) in expected.items():
        spec = TIER_SPECS[tier]
        assert spec.suite_family == suite_family
        assert spec.promotion_blocking is promotion_blocking
        assert spec.slow_suite_allowed is slow_allowed

    assert resolve_tier("fast") == "fast-unit"
    assert resolve_tier("reader-brief") == "report-validation"
    assert resolve_tier("dynamic-v3") == "slow-research-regression"
    assert resolve_tier("trading-engine") == "integration"
    assert resolve_tier("artifact-reproduce") == "reproducibility"


def test_reader_brief_alias_preserves_report_validation_coverage() -> None:
    command = build_command(
        "reader-brief",
        python_executable="python",
        repo_root=Path.cwd(),
        workers="1",
    )
    normalized = " ".join(command).replace("\\", "/")

    assert "tests/test_report_index.py" in normalized
    assert "tests/test_reader_brief.py" in normalized
    assert "tests/trading_engine" in normalized
    assert "report_index or reader_brief" in normalized


def test_slow_research_tier_discovers_related_test_files() -> None:
    command = build_command(
        "dynamic-v3",
        python_executable="python",
        repo_root=Path.cwd(),
        workers="1",
    )
    normalized = " ".join(command).replace("\\", "/")

    assert "tests/test_etf_dynamic_v3_parameter_research.py" in normalized
    assert "tests/test_backtest_sim_outcome.py" in normalized
    assert "tests/test_etf_dynamic_rescue.py" in normalized
    assert "tests/test_sim_defensive_validation.py" in normalized


def test_reproducibility_tier_covers_lineage_and_manifest_contracts() -> None:
    command = build_command(
        "artifact-reproduce",
        python_executable="python",
        repo_root=Path.cwd(),
        workers="1",
    )
    normalized = " ".join(command).replace("\\", "/")

    assert "tests/test_artifact_lineage.py" in normalized
    assert "tests/test_engineering_stage_b_readiness.py" in normalized
    assert "tests/test_pit_source_manifest.py" in normalized
    assert "tests/trading_engine/test_backtest_snapshot_manifest.py" in normalized
