from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import pytest

import scripts.run_validation_tier as validation_tier
from scripts.pytest_runtime_profile import (
    build_runtime_profile,
    collection_identity,
    load_duration_profile,
)
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

FULL_PROVENANCE_ARGS = [
    "--trigger-reason",
    "formal_performance_profile",
    "--task-id",
    "ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE",
    "--boundary-id",
    "pytest-runtime-contract",
]


def _full_provenance_payload(*, boundary_id: str = "pytest-runtime-contract") -> dict[str, object]:
    return {
        "schema_version": "validation_trigger_provenance.v1",
        "status": "PASS",
        "required_for_tier": True,
        "trigger_reason": "formal_performance_profile",
        "task_id": "ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE",
        "boundary_id": boundary_id,
        "parent_run": None,
        "envelope_source": "cli",
        "field_sources": {
            "trigger_reason": "cli",
            "task_id": "cli",
            "boundary_id": "cli",
            "parent_run": "unset",
        },
        "cli_over_environment_precedence": "whole_envelope",
        "validation_errors": [],
    }


def _write_formal_failed_full_parent(
    parent_path: Path,
    *,
    exit_code: int = 1,
    status: str | None = None,
    benchmark_mode: bool = False,
    print_only: bool = False,
) -> bytes:
    parent_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path = parent_path.parent / RUNTIME_PROFILE_OUTPUT_NAME
    profile_payload = _runtime_profile_payload(pytest_exitstatus=exit_code)
    profile_path.write_text(json.dumps(profile_payload), encoding="utf-8")
    profile_bytes = profile_path.read_bytes()
    parent_payload = {
        "schema_version": 1,
        "report_type": "test_runtime_summary",
        "resolved_tier": "full",
        "status": status or ("PASS" if exit_code == 0 else "FAIL"),
        "exit_code": exit_code,
        "print_only": print_only,
        "benchmark_mode": benchmark_mode,
        "production_effect": "none",
        "validation_provenance_status": "PASS",
        "validation_provenance": _full_provenance_payload(),
        "output_artifacts": [
            {
                "path": str(profile_path),
                "exists": True,
                "sha256": hashlib.sha256(profile_bytes).hexdigest(),
                "size_bytes": len(profile_bytes),
            }
        ],
        **_summarize_runtime_profile(profile_payload, final_path=profile_path),
    }
    raw_parent = json.dumps(parent_payload, sort_keys=True).encode()
    parent_path.write_bytes(raw_parent)
    return raw_parent


def _write_formal_runtime_profile_failed_parent(parent_path: Path) -> bytes:
    parent_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path = parent_path.parent / RUNTIME_PROFILE_OUTPUT_NAME
    provenance = _full_provenance_payload()
    reason = (
        "runtime profile contract is invalid: runtime complete duration file set "
        "differs from full test manifest"
    )
    profile_payload = validation_tier._attach_validation_provenance(
        validation_tier._runtime_profile_failure_payload(
            reason=reason,
            pytest_exitstatus=0,
        ),
        provenance,
    )
    profile_path.write_text(json.dumps(profile_payload), encoding="utf-8")
    profile_bytes = profile_path.read_bytes()
    parent_payload = {
        "schema_version": 1,
        "report_type": "test_runtime_summary",
        "resolved_tier": "full",
        "status": "PASS",
        "exit_code": 0,
        "print_only": False,
        "benchmark_mode": False,
        "production_effect": "none",
        "validation_provenance_status": "PASS",
        "validation_provenance": provenance,
        "output_artifacts": [
            {
                "path": str(profile_path),
                "exists": True,
                "sha256": hashlib.sha256(profile_bytes).hexdigest(),
                "size_bytes": len(profile_bytes),
            }
        ],
        **_summarize_runtime_profile(profile_payload, final_path=profile_path),
    }
    raw_parent = json.dumps(parent_payload, sort_keys=True).encode()
    parent_path.write_bytes(raw_parent)
    return raw_parent


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
    assert "tests/test_clean_clone_release_acceptance.py" in " ".join(payload["command"]).replace(
        "\\", "/"
    )
    assert "tests/test_engineering_release_candidate.py" in " ".join(payload["command"]).replace(
        "\\", "/"
    )
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


def test_full_junit_instrumentation_preserves_formal_selection_eligibility() -> None:
    assert validation_tier._formal_full_selection_eligible(
        ["--junitxml=pytest-results.xml"],
        pytest_addopts="",
    )
    assert validation_tier._formal_full_selection_eligible(
        ["--junitxml", "pytest-results.xml"],
        pytest_addopts="",
    )
    assert not validation_tier._formal_full_selection_eligible(
        ["-k", "focused"],
        pytest_addopts="",
    )
    assert not validation_tier._formal_full_selection_eligible(
        ["--junitxml=pytest-results.xml"],
        pytest_addopts="-k focused",
    )


def test_full_fails_before_pytest_without_trigger_provenance(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "should_not_exist.json"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_validation_tier.py",
            "full",
            "--print-only",
            "--json-output",
            str(report_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    assert "Validation trigger provenance failed" in completed.stderr
    assert "full requires trigger_reason" in completed.stderr
    assert not report_path.exists()


def test_full_requires_runtime_artifacts_before_pytest() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_validation_tier.py",
            "full",
            "--print-only",
            *FULL_PROVENANCE_ARGS,
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    assert "Full validation requires --write-runtime-artifact" in completed.stderr


def test_full_print_only_records_canonical_trigger_provenance(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "runtime_artifact"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_validation_tier.py",
            "full",
            "--print-only",
            "--write-runtime-artifact",
            "--artifact-dir",
            str(artifact_dir),
            *FULL_PROVENANCE_ARGS,
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    summary = json.loads(
        (artifact_dir / "test_runtime_summary.json").read_text(encoding="utf-8")
    )
    provenance = summary["validation_provenance"]
    assert provenance == {
        "schema_version": "validation_trigger_provenance.v1",
        "status": "PASS",
        "required_for_tier": True,
        "trigger_reason": "formal_performance_profile",
        "task_id": "ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE",
        "boundary_id": "pytest-runtime-contract",
        "parent_run": None,
        "envelope_source": "cli",
        "field_sources": {
            "trigger_reason": "cli",
            "task_id": "cli",
            "boundary_id": "cli",
            "parent_run": "unset",
        },
        "cli_over_environment_precedence": "whole_envelope",
        "validation_errors": [],
    }
    reader_brief = (artifact_dir / "test_runtime_reader_brief.md").read_text(
        encoding="utf-8"
    )
    assert "## Validation Trigger Provenance" in reader_brief
    assert "Trigger reason: `formal_performance_profile`" in reader_brief


def test_failure_fix_rerun_requires_parent_run_before_pytest() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_validation_tier.py",
            "full",
            "--print-only",
            "--trigger-reason",
            "failure_fix_rerun",
            "--task-id",
            "ARCH-004G2",
            "--boundary-id",
            "rerun-boundary",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    assert "failure_fix_rerun requires non-empty parent_run" in completed.stderr


def test_failure_fix_rerun_binds_failed_full_summary_and_sha256(tmp_path: Path) -> None:
    parent_path = (
        tmp_path
        / "outputs"
        / "validation_runtime"
        / "full_parent_failure"
        / "test_runtime_summary.json"
    )
    raw_parent = _write_formal_failed_full_parent(parent_path)
    args = validation_tier.parse_args(
        [
            "full",
            "--trigger-reason",
            "failure_fix_rerun",
            "--task-id",
            "ARCH-004G2",
            "--boundary-id",
            "rerun-boundary",
            "--parent-run",
            parent_path.relative_to(tmp_path).as_posix(),
        ]
    )

    provenance = validation_tier._validation_trigger_provenance(
        args,
        resolved_tier="full",
        repo_root=tmp_path,
    )

    assert provenance["status"] == "PASS"
    assert provenance["parent_run"] == {
        "run_id": "full_parent_failure",
        "summary_path": (
            "outputs/validation_runtime/full_parent_failure/test_runtime_summary.json"
        ),
        "summary_sha256": hashlib.sha256(raw_parent).hexdigest(),
        "runtime_profile_sha256": hashlib.sha256(
            (parent_path.parent / RUNTIME_PROFILE_OUTPUT_NAME).read_bytes()
        ).hexdigest(),
        "report_type": "test_runtime_summary",
        "resolved_tier": "full",
        "status": "FAIL",
        "failure_basis": "PYTEST_FAIL",
        "production_effect": "none",
    }


def test_failure_fix_rerun_binds_canonical_persisted_runtime_profile_failure(
    tmp_path: Path,
) -> None:
    parent_path = (
        tmp_path
        / "outputs"
        / "validation_runtime"
        / "full_parent_runtime_profile_failure"
        / "test_runtime_summary.json"
    )
    raw_parent = _write_formal_runtime_profile_failed_parent(parent_path)
    args = validation_tier.parse_args(
        [
            "full",
            "--trigger-reason",
            "failure_fix_rerun",
            "--task-id",
            "ARCH-004G2",
            "--boundary-id",
            "rerun-boundary",
            "--parent-run",
            parent_path.relative_to(tmp_path).as_posix(),
        ]
    )

    provenance = validation_tier._validation_trigger_provenance(
        args,
        resolved_tier="full",
        repo_root=tmp_path,
    )

    profile_path = parent_path.parent / RUNTIME_PROFILE_OUTPUT_NAME
    assert provenance["status"] == "PASS"
    assert provenance["parent_run"] == {
        "run_id": "full_parent_runtime_profile_failure",
        "summary_path": (
            "outputs/validation_runtime/full_parent_runtime_profile_failure/"
            "test_runtime_summary.json"
        ),
        "summary_sha256": hashlib.sha256(raw_parent).hexdigest(),
        "runtime_profile_sha256": hashlib.sha256(profile_path.read_bytes()).hexdigest(),
        "report_type": "test_runtime_summary",
        "resolved_tier": "full",
        "status": "PASS",
        "failure_basis": "RUNTIME_PROFILE_FAIL",
        "production_effect": "none",
    }


def test_failure_fix_rerun_rejects_noncanonical_persisted_failure_shape(
    tmp_path: Path,
) -> None:
    parent_path = (
        tmp_path
        / "outputs"
        / "validation_runtime"
        / "full_parent_noncanonical_profile_failure"
        / "test_runtime_summary.json"
    )
    _write_formal_runtime_profile_failed_parent(parent_path)
    profile_path = parent_path.parent / RUNTIME_PROFILE_OUTPUT_NAME
    profile_payload = json.loads(profile_path.read_text(encoding="utf-8"))
    profile_payload["unknown_field"] = "must fail closed"
    profile_path.write_text(json.dumps(profile_payload), encoding="utf-8")
    profile_bytes = profile_path.read_bytes()
    parent_payload = json.loads(parent_path.read_text(encoding="utf-8"))
    parent_payload["output_artifacts"][0].update(
        {
            "sha256": hashlib.sha256(profile_bytes).hexdigest(),
            "size_bytes": len(profile_bytes),
        }
    )
    parent_payload.update(_summarize_runtime_profile(profile_payload, final_path=profile_path))
    parent_path.write_text(json.dumps(parent_payload), encoding="utf-8")
    args = validation_tier.parse_args(
        [
            "full",
            "--trigger-reason",
            "failure_fix_rerun",
            "--task-id",
            "ARCH-004G2",
            "--boundary-id",
            "rerun-boundary",
            "--parent-run",
            parent_path.relative_to(tmp_path).as_posix(),
        ]
    )

    provenance = validation_tier._validation_trigger_provenance(
        args,
        resolved_tier="full",
        repo_root=tmp_path,
    )

    assert provenance["status"] == "FAIL"
    assert "runtime profile is missing or invalid" in "; ".join(
        provenance["validation_errors"]
    )


def test_failure_fix_rerun_validates_the_same_profile_bytes_it_hashes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent_path = (
        tmp_path
        / "outputs"
        / "validation_runtime"
        / "full_parent_profile_replace"
        / "test_runtime_summary.json"
    )
    _write_formal_failed_full_parent(parent_path)
    profile_path = parent_path.parent / RUNTIME_PROFILE_OUTPUT_NAME
    valid_profile_bytes = profile_path.read_bytes()
    captured_invalid_bytes = b"{}"
    profile_path.write_bytes(captured_invalid_bytes)
    parent_payload = json.loads(parent_path.read_text(encoding="utf-8"))
    profile_record = parent_payload["output_artifacts"][0]
    profile_record["sha256"] = hashlib.sha256(captured_invalid_bytes).hexdigest()
    profile_record["size_bytes"] = len(captured_invalid_bytes)
    parent_path.write_text(json.dumps(parent_payload), encoding="utf-8")

    original_reader = validation_tier._read_runtime_profile_payload

    def replace_path_then_read(path: Path, **kwargs: object) -> dict[str, object]:
        profile_path.write_bytes(valid_profile_bytes)
        return original_reader(path, **kwargs)

    monkeypatch.setattr(
        validation_tier,
        "_read_runtime_profile_payload",
        replace_path_then_read,
    )
    args = validation_tier.parse_args(
        [
            "full",
            "--trigger-reason",
            "failure_fix_rerun",
            "--task-id",
            "ARCH-004G2",
            "--boundary-id",
            "rerun-boundary",
            "--parent-run",
            parent_path.relative_to(tmp_path).as_posix(),
        ]
    )

    provenance = validation_tier._validation_trigger_provenance(
        args,
        resolved_tier="full",
        repo_root=tmp_path,
    )

    assert provenance["status"] == "FAIL"
    assert "runtime profile is missing or invalid" in "; ".join(
        provenance["validation_errors"]
    )


def test_failure_fix_rerun_rejects_profile_resolving_outside_parent_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent_path = (
        tmp_path
        / "outputs"
        / "validation_runtime"
        / "full_parent_profile_symlink"
        / "test_runtime_summary.json"
    )
    _write_formal_failed_full_parent(parent_path)
    profile_path = parent_path.parent / RUNTIME_PROFILE_OUTPUT_NAME
    external_profile_path = tmp_path / "outside" / RUNTIME_PROFILE_OUTPUT_NAME
    original_resolve = Path.resolve

    def resolve_with_external_profile(self: Path, strict: bool = False) -> Path:
        if self == profile_path and strict:
            return external_profile_path
        return original_resolve(self, strict=strict)

    monkeypatch.setattr(Path, "resolve", resolve_with_external_profile)
    args = validation_tier.parse_args(
        [
            "full",
            "--trigger-reason",
            "failure_fix_rerun",
            "--task-id",
            "ARCH-004G2",
            "--boundary-id",
            "rerun-boundary",
            "--parent-run",
            parent_path.relative_to(tmp_path).as_posix(),
        ]
    )

    provenance = validation_tier._validation_trigger_provenance(
        args,
        resolved_tier="full",
        repo_root=tmp_path,
    )

    assert provenance["status"] == "FAIL"
    assert "fixed sibling in its parent run directory" in "; ".join(
        provenance["validation_errors"]
    )


@pytest.mark.parametrize(
    ("resolution_target", "expected_error"),
    [
        (
            "runtime_profile",
            "runtime profile does not exist or could not be resolved",
        ),
        ("allowed_root", "validation artifact root could not be resolved"),
        (
            "repo_root",
            "repository root could not be resolved for parent_run summary",
        ),
        (
            "summary_containment",
            "parent_run summary must be under outputs/validation_runtime",
        ),
    ],
)
def test_failure_fix_rerun_resolve_loop_is_a_canonical_cli_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    resolution_target: str,
    expected_error: str,
) -> None:
    parent_path = (
        tmp_path
        / "outputs"
        / "validation_runtime"
        / "full_parent_profile_loop"
        / "test_runtime_summary.json"
    )
    _write_formal_failed_full_parent(parent_path)
    profile_path = parent_path.parent / RUNTIME_PROFILE_OUTPUT_NAME
    original_resolve = Path.resolve
    pytest_started = False
    allowed_root = tmp_path / validation_tier.DEFAULT_ARTIFACT_ROOT

    def resolve_with_profile_loop(self: Path, strict: bool = False) -> Path:
        if resolution_target == "runtime_profile" and self == profile_path and strict:
            raise RuntimeError("simulated runtime_profile symlink loop")
        if resolution_target == "allowed_root" and self == allowed_root and not strict:
            raise RuntimeError("simulated allowed_root symlink loop")
        if resolution_target == "repo_root" and self == tmp_path and not strict:
            raise RuntimeError("simulated repo_root symlink loop")
        if (
            resolution_target == "summary_containment"
            and self == parent_path
            and not strict
        ):
            raise RuntimeError("simulated summary_containment symlink loop")
        return original_resolve(self, strict=strict)

    def fail_if_pytest_starts(*args: object, **kwargs: object) -> dict[str, object]:
        nonlocal pytest_started
        del args, kwargs
        pytest_started = True
        raise AssertionError("pytest subprocess must not start after provenance failure")

    monkeypatch.setattr(Path, "resolve", resolve_with_profile_loop)
    monkeypatch.setattr(validation_tier, "_repo_root", lambda: tmp_path)
    monkeypatch.setattr(validation_tier, "_run_command", fail_if_pytest_starts)

    exit_code = validation_tier.main(
        [
            "full",
            "--write-runtime-artifact",
            "--trigger-reason",
            "failure_fix_rerun",
            "--task-id",
            "ARCH-004G2",
            "--boundary-id",
            "rerun-boundary",
            "--parent-run",
            parent_path.relative_to(tmp_path).as_posix(),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert not pytest_started
    assert "Validation trigger provenance failed" in captured.err
    assert expected_error in captured.err
    if resolution_target != "summary_containment":
        assert f"simulated {resolution_target} symlink loop" in captured.err
    assert "Traceback" not in captured.err


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    [
        ({"schema_version": None}, "schema_version must be 1"),
        ({"benchmark_mode": True}, "non-benchmark Full"),
        ({"print_only": True, "status": "PRINT_ONLY"}, "non-print-only Full"),
        ({"status": "FAIL", "exit_code": 0}, "status and exit_code are inconsistent"),
        ({"runtime_profile_path": "invalid\x00path"}, "runtime_profile_path"),
        ({"output_artifacts": []}, "inventory exactly one runtime profile sidecar"),
    ],
)
def test_failure_fix_rerun_rejects_non_formal_or_inconsistent_parent(
    tmp_path: Path,
    mutation: dict[str, object],
    expected_error: str,
) -> None:
    parent_path = (
        tmp_path
        / "outputs"
        / "validation_runtime"
        / "full_parent_not_failed"
        / "test_runtime_summary.json"
    )
    _write_formal_failed_full_parent(parent_path)
    parent_payload = json.loads(parent_path.read_text(encoding="utf-8"))
    parent_payload.update(mutation)
    parent_path.write_text(json.dumps(parent_payload), encoding="utf-8")
    args = validation_tier.parse_args(
        [
            "full",
            "--trigger-reason",
            "failure_fix_rerun",
            "--task-id",
            "ARCH-004G2",
            "--boundary-id",
            "rerun-boundary",
            "--parent-run",
            parent_path.relative_to(tmp_path).as_posix(),
        ]
    )

    provenance = validation_tier._validation_trigger_provenance(
        args,
        resolved_tier="full",
        repo_root=tmp_path,
    )

    assert provenance["status"] == "FAIL"
    assert expected_error in "; ".join(provenance["validation_errors"])


def test_failure_fix_rerun_rejects_minimal_forged_summary(tmp_path: Path) -> None:
    parent_path = (
        tmp_path
        / "outputs"
        / "validation_runtime"
        / "full_parent_minimal_forgery"
        / "test_runtime_summary.json"
    )
    parent_path.parent.mkdir(parents=True)
    parent_path.write_text(
        json.dumps(
            {
                "report_type": "test_runtime_summary",
                "resolved_tier": "full",
                "status": "FAIL",
                "production_effect": "none",
            }
        ),
        encoding="utf-8",
    )
    args = validation_tier.parse_args(
        [
            "full",
            "--trigger-reason",
            "failure_fix_rerun",
            "--task-id",
            "ARCH-004G2",
            "--boundary-id",
            "rerun-boundary",
            "--parent-run",
            parent_path.relative_to(tmp_path).as_posix(),
        ]
    )

    provenance = validation_tier._validation_trigger_provenance(
        args,
        resolved_tier="full",
        repo_root=tmp_path,
    )

    assert provenance["status"] == "FAIL"
    assert "schema_version must be 1" in "; ".join(provenance["validation_errors"])


def test_failure_fix_rerun_rejects_unbound_identifier(tmp_path: Path) -> None:
    args = validation_tier.parse_args(
        [
            "full",
            "--trigger-reason",
            "failure_fix_rerun",
            "--task-id",
            "ARCH-004G2",
            "--boundary-id",
            "rerun-boundary",
            "--parent-run",
            "fake-run-id",
        ]
    )

    provenance = validation_tier._validation_trigger_provenance(
        args,
        resolved_tier="full",
        repo_root=tmp_path,
    )

    assert provenance["status"] == "FAIL"
    assert "does not exist" in "; ".join(provenance["validation_errors"])


def test_trigger_provenance_uses_one_complete_cli_or_environment_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(validation_tier.VALIDATION_TRIGGER_REASON_ENV, "scheduled_ci")
    monkeypatch.setenv(validation_tier.VALIDATION_TASK_ID_ENV, "ENV_TASK")
    monkeypatch.setenv(validation_tier.VALIDATION_BOUNDARY_ID_ENV, "env-boundary")
    env_args = validation_tier.parse_args(["full"])
    env_provenance = validation_tier._validation_trigger_provenance(
        env_args,
        resolved_tier="full",
        repo_root=Path.cwd(),
    )

    assert env_provenance["status"] == "PASS"
    assert env_provenance["envelope_source"] == "environment"
    assert env_provenance["task_id"] == "ENV_TASK"

    cli_args = validation_tier.parse_args(
        [
            "full",
            "--trigger-reason",
            "formal_performance_profile",
            "--task-id",
            "CLI_TASK",
            "--boundary-id",
            "cli-boundary",
        ]
    )

    provenance = validation_tier._validation_trigger_provenance(
        cli_args,
        resolved_tier="full",
        repo_root=Path.cwd(),
    )

    assert provenance["status"] == "PASS"
    assert provenance["envelope_source"] == "cli"
    assert provenance["trigger_reason"] == "formal_performance_profile"
    assert provenance["task_id"] == "CLI_TASK"
    assert provenance["boundary_id"] == "cli-boundary"
    assert provenance["field_sources"] == {
        "trigger_reason": "cli",
        "task_id": "cli",
        "boundary_id": "cli",
        "parent_run": "unset",
    }


def test_runtime_profile_summary_does_not_embed_node_rows(tmp_path: Path) -> None:
    profile_payload = {
        "schema_version": "test_runtime_profile.v1",
        "profile_status": "PASS",
        "telemetry_status": "PASS",
        "performance_evidence_status": "PASS",
        "stable_full_improvement_claimed": False,
        "collection": {"count": 2, "set_sha256": "abc"},
        "scheduler": {
            "policy": "complete_full_duration_descending_stable",
            "applied": True,
            "fallback": False,
            "fallback_reason": None,
            "manifest_status": "COMPLETE",
            "complete_profile": True,
            "complete_collection_verified": True,
            "tracked_file_count": 2,
            "tracked_node_count": 2,
            "matched_tracked_file_count": 2,
            "matched_tracked_node_count": 2,
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
    assert summary["runtime_profile_summary"]["duration_manifest_status"] == "COMPLETE"
    assert summary["runtime_profile_summary"]["duration_collection_coverage_verified"] is True
    assert summary["runtime_profile_summary"]["duration_tracked_node_count"] == 2
    assert "nodes" not in summary["runtime_profile_summary"]


def _runtime_profile_payload(
    *,
    pytest_exitstatus: object,
    duration_profile_path: Path | None = None,
) -> dict[str, object]:
    nodeid = "tests/test_layer1_meta_policy_readiness.py::test_runtime_profile_stub"
    file_path = nodeid.split("::", 1)[0]
    ordered_sha256 = hashlib.sha256(nodeid.encode("utf-8")).hexdigest()
    duration_profile_path = (
        duration_profile_path or Path(FULL_DURATION_PROFILE_MANIFEST)
    ).resolve()
    duration_profile = validation_tier.safe_load_yaml_path(duration_profile_path)
    duration_source = duration_profile["source"]
    manifest_status = duration_profile["status"]
    partial_seed = manifest_status == "PARTIAL_SEED"
    complete_profile = manifest_status == "COMPLETE"
    complete_evidence = duration_profile.get("complete_profile", {})
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
    complete_collection_verified = bool(
        complete_profile
        and complete_evidence.get("source_node_count") == 1
        and complete_evidence.get("source_file_count") == 1
        and complete_evidence.get("source_collection_set_sha256") == ordered_sha256
        and complete_evidence.get("source_file_set_sha256")
        == hashlib.sha256(file_path.encode("utf-8")).hexdigest()
        and complete_evidence.get("expected_scheduled_ordered_sha256") == ordered_sha256
    )
    return {
        "schema_version": "test_runtime_profile.v1",
        "report_type": "test_runtime_profile",
        "profile_status": "PASS",
        "telemetry_status": "PASS",
        "performance_evidence_status": "FAIL",
        "validation_provenance_binding_status": "PASS",
        "validation_provenance": _full_provenance_payload(),
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
            "policy": (
                "stock_loadfile_test_count_order"
                if complete_profile
                else "non_loadfile_collection_order_preserved"
            ),
            "applied": False,
            "fallback": complete_profile,
            "fallback_reason": (
                "complete duration profile requires pytest-xdist --dist loadfile; observed=no"
                if complete_profile
                else "duration-aware scheduling requires pytest-xdist loadfile"
            ),
            "configured_manifest_path": str(duration_profile_path),
            "manifest_sha256": hashlib.sha256(duration_profile_path.read_bytes()).hexdigest(),
            "manifest_schema_version": duration_profile["schema_version"],
            "profile_id": duration_profile["profile_id"],
            "owner": duration_profile["owner"],
            "version": duration_profile["version"],
            "manifest_status": manifest_status,
            "partial_seed": partial_seed,
            "complete_profile": complete_profile,
            "tracked_file_count": len(duration_profile["files"]),
            "tracked_node_count": complete_evidence.get("source_node_count"),
            "source_tier": duration_source["tier"],
            "source_workers": duration_source["workers"],
            "source_dist": duration_source["dist"],
            "source_artifact_path": duration_source["artifact_path"],
            "source_artifact_sha256": duration_source["artifact_sha256"],
            "source_collection_ordered_sha256": complete_evidence.get(
                "source_collection_ordered_sha256"
            ),
            "source_collection_set_sha256": complete_evidence.get("source_collection_set_sha256"),
            "source_file_set_sha256": complete_evidence.get("source_file_set_sha256"),
            "source_file_rows_sha256": complete_evidence.get("source_file_rows_sha256"),
            "complete_expected_ordered_sha256": complete_evidence.get(
                "expected_scheduled_ordered_sha256"
            ),
            "complete_collection_verified": complete_collection_verified,
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


def _write_complete_duration_manifest(path: Path) -> Path:
    nodeid = "tests/test_layer1_meta_policy_readiness.py::test_runtime_profile_stub"
    file_path = nodeid.split("::", 1)[0]
    node_identity = validation_tier._nodeid_identity([nodeid])
    file_rows = [{"path": file_path, "node_count": 1, "observed_seconds": 1.0}]
    payload = {
        "schema_version": "arch_004g2_full_duration_profile.v1",
        "profile_id": "strict_reader_complete_profile_test",
        "status": "COMPLETE",
        "owner": "validation_operations",
        "version": 2,
        "source": {
            "artifact_path": "outputs/validation_runtime/test/test_runtime_profile.json",
            "artifact_sha256": "5" * 64,
            "tier": "full",
            "workers": 16,
            "dist": "loadfile",
            "elapsed_seconds": 1.0,
            "git_commit": "6" * 40,
            "profile_status": "PASS",
            "telemetry_status": "PASS",
            "performance_evidence_status": "PASS",
            "pytest_exitstatus": 0,
        },
        "complete_profile": {
            "enabled": True,
            "source_node_count": 1,
            "source_file_count": 1,
            "source_collection_ordered_sha256": node_identity["ordered_sha256"],
            "source_collection_set_sha256": node_identity["set_sha256"],
            "source_file_set_sha256": hashlib.sha256(file_path.encode("utf-8")).hexdigest(),
            "source_file_rows_sha256": validation_tier._duration_file_rows_sha256(
                {file_path: 1.0},
                {file_path: 1},
            ),
            "expected_scheduled_ordered_sha256": node_identity["ordered_sha256"],
            "source_file_duration_total_seconds": 1.0,
        },
        "review": {
            "stable_improvement_claimed": False,
            "conditions": ["strict complete reader fixture"],
        },
        "files": file_rows,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


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
    duration_manifest_path = _write_complete_duration_manifest(
        tmp_path / "complete_duration_profile.yaml"
    )
    expected = _runtime_profile_payload(
        pytest_exitstatus=0,
        duration_profile_path=duration_manifest_path,
    )
    profile_path.write_text(json.dumps(expected), encoding="utf-8")
    expected_files = {"tests/test_layer1_meta_policy_readiness.py"}

    accepted = _read_runtime_profile_payload(
        profile_path,
        pytest_exitstatus=0,
        expected_worker_count=1,
        expected_dist="no",
        formal_selection_eligible=True,
        duration_profile_path=duration_manifest_path,
        expected_test_files=expected_files,
    )
    rejected = _read_runtime_profile_payload(
        profile_path,
        pytest_exitstatus=0,
        expected_worker_count=1,
        expected_dist="no",
        formal_selection_eligible=True,
        duration_profile_path=duration_manifest_path,
        expected_test_files={"tests/test_different.py"},
    )

    assert accepted == expected
    assert rejected["profile_status"] == "FAIL"
    assert "full test manifest" in rejected["warnings"][0]

    drifted = json.loads(json.dumps(expected))
    drifted["scheduler"]["source_file_rows_sha256"] = "0" * 64
    profile_path.write_text(json.dumps(drifted), encoding="utf-8")
    drift_rejected = _read_runtime_profile_payload(
        profile_path,
        pytest_exitstatus=0,
        expected_worker_count=1,
        expected_dist="no",
        formal_selection_eligible=True,
        duration_profile_path=duration_manifest_path,
        expected_test_files=expected_files,
    )
    assert drift_rejected["profile_status"] == "FAIL"
    assert "hash evidence mismatch" in drift_rejected["warnings"][0]


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


def _formal_scheduler_profile_payload() -> dict[str, object]:
    nodeids = [f"tests/runtime_semantic_{index:02d}.py::test_case" for index in range(16)]
    files = [nodeid.split("::", 1)[0] for nodeid in nodeids]
    identity = collection_identity(nodeids)
    observed_seconds = {
        file_path: float(len(files) - index) for index, file_path in enumerate(files)
    }
    file_node_counts = {file_path: 1 for file_path in files}
    file_set_sha256 = hashlib.sha256(
        "\n".join(sorted(files)).encode("utf-8")
    ).hexdigest()
    duration_profile = replace(
        load_duration_profile(Path(FULL_DURATION_PROFILE_MANIFEST)),
        manifest_status="COMPLETE",
        partial_seed=False,
        complete_profile=True,
        observed_seconds=observed_seconds,
        file_node_counts=file_node_counts,
        source_node_count=len(nodeids),
        source_file_count=len(files),
        source_collection_ordered_sha256=str(identity["ordered_sha256"]),
        source_collection_set_sha256=str(identity["set_sha256"]),
        source_file_set_sha256=file_set_sha256,
        source_file_rows_sha256=validation_tier._duration_file_rows_sha256(
            observed_seconds,
            file_node_counts,
        ),
        expected_scheduled_ordered_sha256=str(identity["ordered_sha256"]),
        source_file_duration_total_seconds=sum(observed_seconds.values()),
    )
    phase_reports: list[dict[str, object]] = []
    for index, nodeid in enumerate(nodeids):
        worker_id = f"gw{index}"
        for phase, start, stop in (
            ("setup", 1.0, 1.1),
            ("call", 1.1, 1.2),
            ("teardown", 1.2, 1.3),
        ):
            phase_reports.append(
                {
                    "nodeid": nodeid,
                    "phase": phase,
                    "worker_id": worker_id,
                    "start": start,
                    "stop": stop,
                    "duration": stop - start,
                    "outcome": "passed",
                }
            )
    return build_runtime_profile(
        collections={f"gw{index}": nodeids for index in range(16)},
        phase_reports=phase_reports,
        duration_profile=duration_profile,
        expected_worker_count=16,
        xdist_dist="loadfile",
        loadscope_reorder=False,
        formal_full_selection_eligible=True,
        pytest_exitstatus=0,
        started_at=0.0,
        ended_at=2.0,
    )


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
    payload = _formal_scheduler_profile_payload()
    assert validation_tier._runtime_profile_contract_error(
        payload,
        pytest_exitstatus=0,
    ) is None
    if field == "policy":
        exact_fallback = _formal_scheduler_profile_payload()
        exact_fallback_scheduler = exact_fallback["scheduler"]  # type: ignore[assignment]
        exact_fallback_scheduler.update(
            {
                "policy": "stock_loadfile_test_count_order",
                "applied": False,
                "fallback": True,
                "fallback_reason": "fabricated exact-match fallback",
            }
        )
        exact_fallback["performance_evidence_status"] = "FAIL"
        assert validation_tier._runtime_profile_contract_error(
            exact_fallback,
            pytest_exitstatus=0,
        ) == "runtime complete scheduler fell back despite exact eligibility"

        ineligible_policy_drift = _formal_scheduler_profile_payload()
        ineligible_scheduler = ineligible_policy_drift["scheduler"]  # type: ignore[assignment]
        ineligible_scheduler["formal_full_selection_eligible"] = False
        ineligible_scheduler["policy"] = "bogus"
        ineligible_policy_drift["performance_evidence_status"] = "FAIL"
        ineligible_policy_drift["warnings"] = [
            "runtime profile invocation is not the formal unfiltered full-tier selection"
        ]
        assert validation_tier._runtime_profile_contract_error(
            ineligible_policy_drift,
            pytest_exitstatus=0,
        ) == (
            "runtime profile applied scheduler does not satisfy the formal scheduler "
            "contract"
        )
    scheduler = payload["scheduler"]  # type: ignore[assignment]
    scheduler[field] = value
    error = validation_tier._runtime_profile_contract_error(
        payload,
        pytest_exitstatus=0,
    )
    expected_error = (
        "runtime profile applied scheduler contains fallback evidence"
        if field == "fallback_reason"
        else (
            "runtime profile applied scheduler does not satisfy the formal scheduler "
            "contract"
        )
    )
    assert error == expected_error
    profile_path.write_text(json.dumps(payload), encoding="utf-8")

    observed = _read_runtime_profile_payload(profile_path, pytest_exitstatus=0)

    assert observed["profile_status"] == "FAIL"
    assert observed["performance_evidence_status"] == "FAIL"
    assert "scheduler" in observed["warnings"][0]


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

    monkeypatch.setattr(validation_tier, "_runtime_profile_contract_error", raise_contract_error)

    observed = _read_runtime_profile_payload(profile_path, pytest_exitstatus=0)

    assert observed["profile_status"] == "FAIL"
    assert observed["pytest_exitstatus"] == 0
    assert "contract evaluation failed closed" in observed["warnings"][0]


def test_runtime_profile_provenance_must_match_runner_envelope(tmp_path: Path) -> None:
    profile_path = tmp_path / "profile.json"
    expected_provenance = _full_provenance_payload(boundary_id="expected-boundary")
    payload = _runtime_profile_payload(pytest_exitstatus=0)
    payload["validation_provenance"] = {
        **expected_provenance,
        "boundary_id": "different-boundary",
    }
    profile_path.write_text(json.dumps(payload), encoding="utf-8")

    observed = _read_runtime_profile_payload(
        profile_path,
        pytest_exitstatus=0,
        expected_validation_provenance=expected_provenance,
    )

    assert observed["profile_status"] == "FAIL"
    assert observed["performance_evidence_status"] == "FAIL"
    assert "does not match runner envelope" in observed["warnings"][0]


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
        profile_payload = _runtime_profile_payload(pytest_exitstatus=0)
        profile_payload["validation_provenance"] = json.loads(
            env_overrides[validation_tier.RUNTIME_PROFILE_VALIDATION_PROVENANCE_ENV]
        )
        profile_path.write_text(
            json.dumps(profile_payload),
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
            *FULL_PROVENANCE_ARGS,
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
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    assert sidecar["validation_provenance"] == summary["validation_provenance"]
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
        profile_payload = _runtime_profile_payload(pytest_exitstatus=0)
        profile_payload["validation_provenance"] = json.loads(
            env_overrides[validation_tier.RUNTIME_PROFILE_VALIDATION_PROVENANCE_ENV]
        )
        profile_path.write_text(
            json.dumps(profile_payload),
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
            *FULL_PROVENANCE_ARGS,
            "--workers",
            "1",
            "--write-runtime-artifact",
            "--artifact-dir",
            str(artifact_dir),
        ]
    )

    assert exit_code == 7
    sidecar = json.loads((artifact_dir / RUNTIME_PROFILE_OUTPUT_NAME).read_text(encoding="utf-8"))
    summary = json.loads((artifact_dir / "test_runtime_summary.json").read_text(encoding="utf-8"))
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
        profile_payload = _runtime_profile_payload(pytest_exitstatus=0)
        profile_payload["validation_provenance"] = json.loads(
            env_overrides[validation_tier.RUNTIME_PROFILE_VALIDATION_PROVENANCE_ENV]
        )
        profile_path.write_text(
            json.dumps(profile_payload),
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
            *FULL_PROVENANCE_ARGS,
            "--workers",
            "1",
            "--write-runtime-artifact",
            "--artifact-dir",
            str(artifact_dir),
        ]
    )

    assert exit_code == 0
    summary = json.loads((artifact_dir / "test_runtime_summary.json").read_text(encoding="utf-8"))
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
    durations = _parse_pytest_slow_durations("""
        ============================= slowest 3 durations =============================
        12.34s call     tests/test_example.py::test_slow_case
        2.50s setup    tests/test_other.py::test_setup
        0.99s teardown tests/test_other.py::test_teardown
        """)

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


def test_full_benchmark_print_only_marks_runtime_profile_not_applicable(
    tmp_path: Path,
) -> None:
    artifact_dir = tmp_path / "full_benchmark"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_validation_tier.py",
            "full",
            "--print-only",
            "--write-runtime-artifact",
            "--artifact-dir",
            str(artifact_dir),
            "--benchmark-worker",
            "8,16",
            *FULL_PROVENANCE_ARGS,
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    payload = json.loads(
        (artifact_dir / "test_runtime_summary.json").read_text(encoding="utf-8")
    )
    assert payload["runtime_profile_status"] == "NOT_APPLICABLE"
    assert payload["runtime_profile_not_applicable_reason"] == (
        "benchmark_variants_are_non_formal"
    )
    assert "test_runtime_profile" not in payload["schema_versions"]
    assert not (artifact_dir / "test_runtime_profile.json").exists()
    assert all(
        not str(row["path"]).endswith("test_runtime_profile.json")
        for row in payload["output_artifacts"]
    )


def test_full_benchmark_removes_inherited_validation_profile_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_dir = tmp_path / "full_benchmark"
    inherited_variables = validation_tier.BENCHMARK_REMOVED_VALIDATION_ENV_VARS
    for variable_name in inherited_variables:
        monkeypatch.setenv(variable_name, f"inherited-{variable_name}")
    observed_removals: list[tuple[str, ...]] = []

    def fake_run_command(
        command: list[str],
        *,
        cwd: Path,
        env_overrides: dict[str, str] | None = None,
        env_removals: tuple[str, ...] = (),
    ) -> dict[str, object]:
        del command, cwd, env_overrides
        observed_removals.append(env_removals)
        return {"exit_code": 0, "elapsed_seconds": 0.01, "pytest_output": ""}

    monkeypatch.setattr(validation_tier, "_run_command", fake_run_command)

    exit_code = validation_tier.main(
        [
            "full",
            "--write-runtime-artifact",
            "--artifact-dir",
            str(artifact_dir),
            "--benchmark-worker",
            "16",
            *FULL_PROVENANCE_ARGS,
        ]
    )

    assert exit_code == 0
    assert observed_removals == [inherited_variables]
    assert not (artifact_dir / RUNTIME_PROFILE_OUTPUT_NAME).exists()


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
