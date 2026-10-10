"""Production parent correlation for one canonical DQ child, never research authority.

TRADING-2564 S3b: an existing capture bootstrap and capture hold are prerequisites.
Attempts are immutable and cannot be resumed or dispatched twice. This module
does not acquire holds, generate previews, or adopt temporal evidence.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, NoReturn

from ai_trading_system.contracts.named_data_quality_execution import (
    NamedArtifactBinding,
    NamedDQExecutionReceipt,
    NamedDQExecutionRequest,
    NamedDQSuccessfulDispatchBinding,
)
from ai_trading_system.contracts.named_execution_context import require_named_execution_context
from ai_trading_system.contracts.prospective_event_time_evidence import (
    EventBinding,
    canonical_json_bytes,
    parse_utc_datetime,
    strict_json_loads,
)
from ai_trading_system.data.capture_hold import CaptureHold, recheck_capture_hold
from ai_trading_system.data.immutable_publish import (
    _bound_directory,
    _root_authority,
    exclusive_store_maintenance,
    read_contained_artifact_bytes,
    write_contained_artifact_bytes,
)
from ai_trading_system.data.named_quality_execution import NamedBootstrapAuthority

# Process resource limit, not an investment threshold. No timeout retry is allowed.
CHILD_TIMEOUT_SECONDS = 120
BOOTSTRAP_PATH = "scripts/run_named_data_quality.py"


@dataclass(frozen=True)
class NamedQualityTerminalObservation:
    """Original call-stack observation when terminal evidence publication fails.

    The counter remains an observation of the actual child output. A missing
    parent binding means no successfully published parent artifact is attested;
    this DTO cannot itself supply a retained DQ proof or authorize another run.
    """

    canonical_dq_call_count: int | None
    counter_observation_state: str
    returncode: int | None
    terminal_state: str
    parent_receipt: NamedArtifactBinding | None
    status: str = "BLOCKED"

    def __post_init__(self) -> None:
        count = self.canonical_dq_call_count
        if (
            self.status != "BLOCKED"
            or (count is not None and (type(count) is not int or count < 0))
            or self.counter_observation_state != ("UNKNOWN" if count is None else "KNOWN")
            or (self.returncode is not None and type(self.returncode) is not int)
            or self.terminal_state
            not in {
                "NOT_STARTED",
                "EXITED",
                "TIMED_OUT_AND_CHILD_REAPED",
                "PARENT_ERROR_AND_CHILD_REAPED",
            }
            or (
                self.parent_receipt is not None
                and type(self.parent_receipt) is not NamedArtifactBinding
            )
        ):
            raise ValueError("invalid original child terminal observation")


class NamedQualityDispatchError(ValueError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        terminal_observation: NamedQualityTerminalObservation | None = None,
    ) -> None:
        self.code = code
        self.terminal_observation = terminal_observation
        super().__init__(f"{code}: {message}")


def _fail(code: str, message: str) -> NoReturn:
    raise NamedQualityDispatchError(code, message)


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _now() -> datetime:
    return datetime.now(UTC)


def _object(value: object) -> dict[str, Any]:
    if type(value) is not dict or any(type(key) is not str for key in value):
        _fail("NAMED_PARENT_OBJECT_REQUIRED", "strict JSON object required")
    return value


def _text(value: object) -> str:
    if type(value) is not str or not value or value != value.strip():
        _fail("NAMED_PARENT_TEXT_INVALID", "nonempty exact string required")
    return value


def _path(value: object) -> str:
    result = _text(value)
    EventBinding(result, "0" * 64, 0)
    return result


@dataclass(frozen=True)
class NamedQualityDispatchResult:
    status: str
    receipt_path: str | None
    receipt_sha256: str | None
    run_dispatch_path: str | None
    run_dispatch_sha256: str | None
    parent_receipt: NamedArtifactBinding
    canonical_dq_call_count: int | None
    counter_observation_state: str
    returncode: int | None
    terminal_state: str


@dataclass(frozen=True)
class _ChildPythonLaunch:
    executable: str
    environment: dict[str, str]
    audit: dict[str, Any]


def _child_python_launch(
    *,
    platform_name: str,
    implementation: str,
    logical_executable: str,
    base_executable: str | None,
    prefix: str,
    base_prefix: str,
    environment: Mapping[str, str],
) -> _ChildPythonLaunch:
    """Use CPython's direct Windows venv spawn, retaining the real child PID.

    Mirrors CPython 3.11 multiprocessing/popen_spawn_win32.py's launch selection.
    This selection grants no execution authority and does not mutate os.environ.
    """
    if implementation != "cpython" or platform_name not in {"win32", "linux", "darwin"}:
        _fail("NAMED_PARENT_PYTHON_RUNTIME_UNSUPPORTED", implementation)
    path_type = PureWindowsPath if platform_name == "win32" else PurePosixPath
    for value in (logical_executable, base_executable, prefix, base_prefix):
        if (
            type(value) is not str
            or not value
            or "\0" in value
            or not path_type(value).is_absolute()
        ):
            _fail("NAMED_PARENT_PYTHON_PATH_INVALID", "absolute interpreter identity required")
    assert base_executable is not None
    in_venv = path_type(prefix) != path_type(base_prefix)
    windows_venv = platform_name == "win32" and in_venv
    if platform_name == "win32" and (
        any(
            path_type(value).name.lower() != "python.exe"
            for value in (logical_executable, base_executable)
        )
        or (path_type(logical_executable) != path_type(base_executable)) != in_venv
    ):
        _fail("NAMED_PARENT_PYTHON_VENV_IDENTITY_INCONSISTENT", logical_executable)
    child_environment = dict(environment)
    removed = sorted(
        key
        for key in child_environment
        if key.upper() in {"PYTHONEXECUTABLE", "__PYVENV_LAUNCHER__"}
    )
    for key in removed:
        del child_environment[key]
    overrides = {"__PYVENV_LAUNCHER__": logical_executable} if windows_venv else {}
    child_environment.update(overrides)
    executable = base_executable if windows_venv else logical_executable
    return _ChildPythonLaunch(
        executable,
        child_environment,
        {
            "schema_version": "named_data_quality_python_launch.v1",
            "profile": "CPYTHON_WINDOWS_DIRECT_VENV" if windows_venv else "CPYTHON_DIRECT",
            "implementation": implementation,
            "platform": platform_name,
            "logical_executable": logical_executable,
            "os_executable": executable,
            "base_executable": base_executable,
            "logical_prefix": prefix,
            "base_prefix": base_prefix,
            "venv_detected": in_venv,
            "windows_redirector_bypassed": windows_venv,
            "child_environment_removed_keys": removed,
            "child_environment_overrides": overrides,
            "parent_environment_mutated": False,
            "dispatch_authority_granted": False,
        },
    )


def _current_child_python_launch() -> _ChildPythonLaunch:
    launch = _child_python_launch(
        platform_name=sys.platform,
        implementation=sys.implementation.name,
        logical_executable=sys.executable,
        base_executable=getattr(sys, "_base_executable", None),
        prefix=sys.prefix,
        base_prefix=sys.base_prefix,
        environment=os.environ,
    )
    for key in ("logical_executable", "base_executable"):
        executable = launch.audit[key]
        if not Path(executable).is_file() or not os.access(executable, os.X_OK):
            _fail("NAMED_PARENT_PYTHON_NOT_EXECUTABLE", executable)
    return launch


def _parent_proof(
    request: NamedDQExecutionRequest,
    bootstrap: NamedBootstrapAuthority,
    hold: CaptureHold,
    required_paths: tuple[str, ...],
    *,
    stage: str,
) -> dict[str, Any]:
    # Import at call time so the coordinator can freeze the new exact profile
    # without widening any previous consumer or bootstrap contract.
    from ai_trading_system.contracts.named_data_quality_execution import (
        COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_PATH,
        COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_SHA256,
        PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
        PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
    )

    context = require_named_execution_context()
    profile = (request.source_manifest_path, request.source_manifest_sha256)
    operation_profile_allowed = (
        getattr(bootstrap, "operation", None) == "capture"
        and profile
        == (
            PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
            PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
        )
    ) or (
        getattr(bootstrap, "operation", None) in {"composer-readiness", "composer-capture"}
        and profile
        == (
            COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_PATH,
            COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_SHA256,
        )
    )
    if (
        bootstrap.context is not context
        or context.process_id != os.getpid()
        or not operation_profile_allowed
        or type(bootstrap.canonical_dq_call_count) is not int
        or bootstrap.canonical_dq_call_count != 0
        or bootstrap.source_hold_id != hold.hold_id
        or context.identity.execution_root != request.roots.execution_root
        or context.identity.candidate_commit != request.candidate_commit
        or context.identity.source_manifest_path != request.source_manifest_path
        or context.identity.source_manifest_sha256 != request.source_manifest_sha256
        or Path(request.roots.execution_root) != hold.root
    ):
        _fail("NAMED_PARENT_CAPTURE_CONTEXT_REQUIRED", request.request_id)
    source_checked = parse_utc_datetime(bootstrap.assert_execution_unchanged(stage=stage))
    proof = recheck_capture_hold(
        hold, candidate_commit=request.candidate_commit, required_paths=required_paths
    )
    if source_checked > parse_utc_datetime(proof["checked_at"]):
        _fail("NAMED_PARENT_CLOCK_ROLLBACK", stage)
    return {
        **proof,
        "schema_version": "named_dq_existing_parent_proof.v2",
        "request_sha256": request.canonical_sha256,
        "parent_pid": os.getpid(),
        "parent_execution_identity_sha256": context.stable_identity_sha256,
        "parent_canonical_dq_call_count": 0,
        "source_checked_at": source_checked.isoformat(),
    }


def _write(root: Path, relative: str, content: bytes) -> dict[str, Any]:
    result = write_contained_artifact_bytes(
        root=root, relative_path=relative, content=content, immutable=True
    )
    return {
        "path": result.path.as_posix(),
        "sha256": result.sha256,
        "size_bytes": result.size_bytes,
    }


def _matching_receipt(
    request: NamedDQExecutionRequest,
    child: dict[str, Any],
    *,
    child_pid: int | None,
    hold_id: str,
    identity_sha256: str,
    spawned: datetime,
    terminal: datetime,
) -> NamedDQExecutionReceipt:
    path = _path(child.get("receipt_path"))
    content = read_contained_artifact_bytes(
        root=Path(request.roots.evidence_root), relative_path=path
    )
    receipt = NamedDQExecutionReceipt.from_json_bytes(content)
    expected = {
        "schema_version": "named_data_quality_bootstrap_result.v2",
        "status": "PASS",
        "request_id": request.request_id,
        "process_id": child_pid,
        "source_hold_id": hold_id,
        "receipt_id": receipt.receipt_id,
        "receipt_sha256": _sha(content),
        "canonical_dq_call_count": 1,
        "verified_input_seal_exported": False,
        "dispatch_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    if (
        any(
            type(child.get(key)) is not type(value) or child.get(key) != value
            for key, value in expected.items()
        )
        or receipt.request != request
        or receipt.report.status != "PASS"
        or receipt.execution.stable_identity_sha256 != identity_sha256
        or receipt.execution_observation.execution_pid != child_pid
        or receipt.execution_observation.source_hold_id != hold_id
        or not spawned
        <= parse_utc_datetime(child.get("child_started_at"))
        <= receipt.started_at
        <= receipt.ended_at
        <= receipt.execution_observation.terminal_checked_at
        <= parse_utc_datetime(child.get("child_terminal_checked_at"))
        <= terminal
    ):
        _fail("NAMED_PARENT_RUN_RECEIPT_ASSOCIATION_MISMATCH", request.request_id)
    return receipt


def dispatch_named_quality_child(
    request: NamedDQExecutionRequest,
    *,
    bootstrap: NamedBootstrapAuthority,
    hold: CaptureHold,
    output_relative_path: str,
) -> NamedQualityDispatchResult:
    """Dispatch once; return retained BLOCKED evidence for any terminal failure.

    Invalid preconditions raise before attempt creation. Once an attempt exists,
    the same slot can never dispatch again, including after process interruption.
    """
    if type(request) is not NamedDQExecutionRequest or type(hold) is not CaptureHold:
        _fail("NAMED_PARENT_TYPED_INPUT_REQUIRED", "request and capture hold handle required")
    output = _path(output_relative_path)
    root = Path(request.roots.execution_root)
    evidence = Path(request.roots.evidence_root)
    if evidence == root or not evidence.is_relative_to(root):
        _fail("NAMED_PARENT_EVIDENCE_SCOPE_INVALID", str(evidence))
    required = tuple(sorted(set((output, evidence.relative_to(root).as_posix()))))
    launch = _current_child_python_launch()
    before = _parent_proof(request, bootstrap, hold, required, stage="PARENT_PRE_DISPATCH")
    store = root / output
    # Reuse the writer's descriptor-bound namespace creation. No shared file
    # may be written before acquiring this store's existing maintenance lock:
    # concurrent Windows replacements can race even when the bytes agree.
    with _root_authority(root), _bound_directory(root, store, "named dispatch store", create=True):
        pass
    attempt = {
        "schema_version": "named_quality_dispatch_attempt.v2",
        "request_sha256": request.canonical_sha256,
        "parent_pid": os.getpid(),
        "created_at": _now().isoformat(),
        "source_hold_id": hold.hold_id,
    }
    with exclusive_store_maintenance(store_root=store):
        # This directory is dedicated to the business key supplied by the parent.
        # lstat observes existence only; contained reads/writes reject links.
        try:
            (store / "attempt.json").lstat()
        except FileNotFoundError:
            pass
        else:
            _fail("NAMED_PARENT_ATTEMPT_ALREADY_EXISTS", output)
        _write(
            store,
            "store.json",
            canonical_json_bytes(
                {
                    "schema_version": "named_quality_dispatch_store.v1",
                    "request_sha256": request.canonical_sha256,
                }
            ),
        )
        _write(store, "attempt.json", canonical_json_bytes(attempt))
    request_binding = _write(store, "request.json", request.canonical_bytes)
    pre_binding = _write(store, "pre_dispatch_proof.json", canonical_json_bytes(before))
    command = [
        launch.executable,
        "-I",
        "-B",
        "-X",
        "utf8",
        str(root / BOOTSTRAP_PATH),
        "--request",
        str(store / "request.json"),
        "--request-sha256",
        request.canonical_sha256,
        "--source-hold-id",
        hold.hold_id,
        "--operation",
        "run",
    ]
    spawned = _now()
    process: subprocess.Popen[bytes] | None = None
    child: dict[str, Any] = {}
    stdout = stderr = b""
    child_pid: int | None = None
    spawn_observed: str | None = None
    terminal_state = "NOT_STARTED"
    failure: str | None = None
    try:
        if (
            not parse_utc_datetime(before["checked_at"])
            <= spawned
            < parse_utc_datetime(before["active_hold"]["expires_at"])
        ):
            _fail("NAMED_PARENT_HOLD_EXPIRED_BEFORE_SPAWN", hold.hold_id)
        process = subprocess.Popen(
            command,
            executable=launch.executable,
            env=launch.environment,
            cwd=root,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
        )
        child_pid, spawn_observed = process.pid, _now().isoformat()
        try:
            stdout, stderr = process.communicate(timeout=CHILD_TIMEOUT_SECONDS)
            terminal_state = "EXITED"
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            terminal_state = "TIMED_OUT_AND_CHILD_REAPED"
        if terminal_state == "EXITED":
            child = _object(strict_json_loads(stdout))
    except (OSError, RuntimeError, ValueError, TypeError) as exc:
        failure = str(exc)
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            stdout, stderr = process.communicate()
            terminal_state = "PARENT_ERROR_AND_CHILD_REAPED"
    terminal = _now()
    try:
        after = _parent_proof(request, bootstrap, hold, required, stage="PARENT_POST_DISPATCH")
    except (OSError, RuntimeError, ValueError, TypeError) as exc:
        after = {"status": "BLOCKED", "checked_at": _now().isoformat(), "detail": str(exc)}
        failure = f"NAMED_PARENT_POST_DISPATCH_BLOCKED: {exc}"
    observed = parse_utc_datetime(spawn_observed) if spawn_observed is not None else terminal
    if not (
        parse_utc_datetime(before["checked_at"])
        <= spawned
        <= observed
        <= terminal
        <= parse_utc_datetime(after["checked_at"])
    ):
        failure = "NAMED_PARENT_PROOF_TIME_ORDER_INVALID"
    receipt = None
    returncode = None if process is None else process.returncode
    if failure is None and terminal_state == "EXITED" and returncode == 0:
        try:
            receipt = _matching_receipt(
                request,
                child,
                child_pid=child_pid,
                hold_id=hold.hold_id,
                identity_sha256=before["parent_execution_identity_sha256"],
                spawned=spawned,
                terminal=terminal,
            )
        except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
            failure = f"NAMED_PARENT_RECEIPT_REJECTED: {exc}"
    else:
        failure = failure or f"NAMED_PARENT_CHILD_UNSUCCESSFUL:{terminal_state}:{returncode}"
    count = child.get("canonical_dq_call_count")
    # Preserve an observed excess count as a known violation, never turn it
    # into the permitted maximum. Missing/malformed counters remain unknown.
    if type(count) is not int or count < 0:
        count = None
    parent_binding: NamedArtifactBinding | None = None
    publication_stage = "post_dispatch_proof.json"
    try:
        post_binding = _write(store, publication_stage, canonical_json_bytes(after))
        publication_stage = "child_stdout.json"
        stdout_binding = _write(store, publication_stage, stdout)
        publication_stage = "child_stderr.txt"
        stderr_binding = _write(store, publication_stage, stderr)
        parent: dict[str, Any] = {
            "schema_version": "named_data_quality_parent_dispatch.v2",
            "profile": (
                "COMPOSER_PROSPECTIVE_PRODUCTION_PARENT"
                if bootstrap.operation.startswith("composer-")
                else "PROSPECTIVE_FIVE_CANDIDATE_PRODUCTION_PARENT"
            ),
            "status_semantics": "PARENT_ASSOCIATION_AND_PROCESS_OBSERVATION_ONLY",
            "status": "PASS" if receipt is not None and failure is None else "BLOCKED",
            "candidate_commit": request.candidate_commit,
            "execution_root": root.as_posix(),
            "request": request_binding,
            "request_id": request.request_id,
            "source_hold_id": hold.hold_id,
            "operation": "run",
            "parent_operation": (
                bootstrap.operation if bootstrap.operation.startswith("composer-") else "capture"
            ),
            "command": command,
            "launch_audit": launch.audit,
            "parent_pid": os.getpid(),
            "child_pid": child_pid,
            "spawn_requested_at": spawned.isoformat(),
            "spawn_observed_at": spawn_observed,
            "terminal_observed_at": terminal.isoformat(),
            "terminal_state": terminal_state,
            "returncode": returncode,
            "pre_dispatch_proof": pre_binding,
            "post_dispatch_proof": post_binding,
            "child_stdout": stdout_binding,
            "child_stderr": stderr_binding,
            "child_result": child,
            "observed_canonical_dq_call_count": count,
            "counter_observation_state": "KNOWN" if count is not None else "UNKNOWN",
            "parent_canonical_dq_call_count": bootstrap.canonical_dq_call_count,
            "failure": failure,
            "hold_acquired_or_mutated": False,
            "verified_input_seal_exported": False,
            "dispatch_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        }
        parent_bytes = canonical_json_bytes(parent)
        publication_stage = "parent_receipt.json"
        _write(store, publication_stage, parent_bytes)
        # Assign only after the original writer has completed its verification.
        # An exception, even with same-named bytes on disk, supplies no binding.
        parent_binding = NamedArtifactBinding(
            "EXECUTION", f"{output}/parent_receipt.json", _sha(parent_bytes), len(parent_bytes)
        )
        proof_path = proof_sha = receipt_path = receipt_sha = None
        if receipt is not None and failure is None:
            publication_stage = "successful_run_dispatch.json"
            receipt_path, receipt_sha = _path(child["receipt_path"]), receipt.canonical_sha256
            proof = NamedDQSuccessfulDispatchBinding(
                receipt=NamedArtifactBinding(
                    "EVIDENCE", receipt_path, receipt_sha, len(receipt.canonical_bytes)
                ),
                receipt_id=receipt.receipt_id,
                request_id=request.request_id,
                request_sha256=request.canonical_sha256,
                execution_identity_sha256=receipt.execution.stable_identity_sha256,
                candidate_commit=request.candidate_commit,
                execution_root=root.as_posix(),
                execution_pid=receipt.execution_observation.execution_pid,
                source_hold_id=hold.hold_id,
                child_started_at=parse_utc_datetime(child["child_started_at"]),
                child_terminal_checked_at=parse_utc_datetime(child["child_terminal_checked_at"]),
                parent_postchecked_at=parse_utc_datetime(after["checked_at"]),
                parent_receipt=parent_binding,
            )
            proof.assert_matches_receipt(receipt, receipt_path=receipt_path)
            _write(store, publication_stage, proof.canonical_bytes)
            proof_path, proof_sha = f"{output}/successful_run_dispatch.json", proof.canonical_sha256
    except (OSError, RuntimeError, ValueError, TypeError, KeyError) as exc:
        # Preserve the observed child count independently from whether terminal
        # artifacts could be published. Never reconstruct authority by looking
        # for a same-named parent or retry a successful-dispatch publication.
        observation = NamedQualityTerminalObservation(
            canonical_dq_call_count=count,
            counter_observation_state="KNOWN" if count is not None else "UNKNOWN",
            returncode=returncode,
            terminal_state=terminal_state,
            parent_receipt=parent_binding,
        )
        raise NamedQualityDispatchError(
            "NAMED_PARENT_TERMINAL_PUBLICATION_FAILED",
            f"{publication_stage}: {exc}",
            terminal_observation=observation,
        ) from exc
    assert parent_binding is not None
    return NamedQualityDispatchResult(
        parent["status"],
        receipt_path,
        receipt_sha,
        proof_path,
        proof_sha,
        parent_binding,
        count,
        parent["counter_observation_state"],
        returncode,
        terminal_state,
    )
