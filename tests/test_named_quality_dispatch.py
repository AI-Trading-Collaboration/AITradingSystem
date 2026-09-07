"""Synthetic parent tests with real S4D replay and contained immutable writers.

The process/context doubles do not prove Git-byte execution or canonical DQ.
The coordinator's fresh-candidate E2E covers that separate runtime boundary.
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace
from typing import Any, cast

import pytest
from test_named_data_quality_execution_contract import _receipt as synthetic_receipt

import ai_trading_system.data.named_quality_dispatch as dispatch
from ai_trading_system.contracts.named_data_quality_execution import (
    PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
    PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
    NamedDQExecutionReceipt,
    NamedDQExecutionRequest,
    NamedDQRoots,
    NamedDQSuccessfulDispatchBinding,
    NamedExecutionObservation,
)
from ai_trading_system.contracts.named_execution_context import NamedExecutionIdentity
from ai_trading_system.contracts.prospective_event_time_evidence import (
    canonical_json_bytes,
    parse_utc_datetime,
)
from ai_trading_system.data.named_quality_execution import (
    NamedBootstrapAuthority,
    _verify_successful_run_dispatch,
)
from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutLeaseGuard,
    CheckoutLeaseHandle,
    CheckoutOperationClass,
)

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = "outputs/capture/dq-attempt"


def _git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()


@dataclass
class _LeaseFixture:
    root: Path
    commit: str
    handle: CheckoutLeaseHandle


@pytest.fixture
def leased(tmp_path: Path) -> Iterator[_LeaseFixture]:
    root = tmp_path / "synthetic-dispatch"
    root.mkdir()
    (root / ".gitignore").write_text("outputs/\n", encoding="utf-8")
    for relative in (
        "config/architecture/arch_005_s4d_checkout_guard.yaml",
        "config/architecture/arch_005_parallel_control_policy.yaml",
    ):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((ROOT / relative).read_bytes())
    _git(root, "init", "-b", "synthetic-dispatch")
    _git(root, "config", "user.email", "dispatch@example.invalid")
    _git(root, "config", "user.name", "Synthetic Dispatch")
    _git(root, "config", "core.autocrlf", "false")
    _git(root, "add", ".gitignore", "config")
    _git(root, "commit", "-m", "synthetic dispatch fixture")
    commit = _git(root, "rev-parse", "HEAD")
    guard = CheckoutLeaseGuard(
        project_root=root,
        policy_path=root / "config/architecture/arch_005_s4d_checkout_guard.yaml",
        parallel_policy_path=root / "config/architecture/arch_005_parallel_control_policy.yaml",
    )
    decision, handle = guard.acquire(
        intent_id="synthetic-named-dispatch",
        task_id="TRADING-2564-SYNTHETIC",
        thread_id="synthetic-dispatch",
        actor="integration-coordinator",
        operation_class=CheckoutOperationClass.SHARED_MUTATION,
        shared_paths=("outputs/capture",),
        base_commit=commit,
    )
    assert decision.status == "PASS" and handle is not None
    try:
        yield _LeaseFixture(root, commit, handle)
    finally:
        if not handle.released:
            handle.release(outcome="synthetic_test_complete")


def test_restore_and_recheck_replay_real_authority_without_new_events(
    leased: _LeaseFixture,
) -> None:
    before = leased.handle.guard.replay().to_dict()
    restored = dispatch.restore_named_capture_lease(
        execution_root=leased.root,
        source_lease_id=leased.handle.lease_id,
        candidate_commit=leased.commit,
        required_paths=(OUTPUT,),
    )
    assert type(restored) is CheckoutLeaseHandle
    proof = dispatch.recheck_named_capture_lease(
        restored,
        candidate_commit=leased.commit,
        required_paths=(OUTPUT,),
    )
    assert proof["status"] == "PASS"
    assert proof["active_lease"]["lease_id"] == leased.handle.lease_id
    assert not proof["lease_acquired_or_mutated"]
    assert leased.handle.guard.replay().to_dict() == before


def _retained_proof(leased: _LeaseFixture) -> dict[str, Any]:
    return dispatch.recheck_named_capture_lease(
        leased.handle, candidate_commit=leased.commit, required_paths=(OUTPUT,)
    )


def _verify_retained(leased: _LeaseFixture, proof: dict[str, Any], **changes: Any) -> None:
    arguments = {
        "execution_root": leased.root,
        "candidate_commit": leased.commit,
        "required_paths": (OUTPUT,),
        "source_lease_id": leased.handle.lease_id,
        "checked_at": parse_utc_datetime(proof["checked_at"]),
        **changes,
    }
    dispatch.verify_retained_named_capture_proof(proof, **arguments)


def test_retained_proof_survives_heartbeat_release_expiry_and_checkout_drift(
    leased: _LeaseFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = _retained_proof(leased)
    leased.handle.heartbeat()
    after_heartbeat = _retained_proof(leased)
    assert original["lease_event_sha256"] != after_heartbeat["lease_event_sha256"]
    leased.handle.release(outcome="synthetic_retained_proof_complete")
    _git(leased.root, "commit", "--allow-empty", "-m", "later unrelated head")

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("retained proof verification must not require current live authority")

    monkeypatch.setattr(CheckoutLeaseGuard, "replay", forbidden)
    monkeypatch.setattr(CheckoutLeaseGuard, "audit_worktree", forbidden)
    monkeypatch.setattr(dispatch, "_now", lambda: datetime.now(UTC) + timedelta(days=3))
    _verify_retained(leased, original)
    _verify_retained(leased, after_heartbeat)
    _verify_retained(leased, {**original, "schema_version": "named_dq_existing_parent_proof.v1"})
    _verify_retained(leased, {**original, "schema_version": "prospective_capture_parent_proof.v1"})


@pytest.mark.parametrize("member", ["lease_intent", "lease_event"])
def test_retained_proof_rejects_structural_tamper_with_recomputed_raw_hash(
    leased: _LeaseFixture, member: str
) -> None:
    proof = _retained_proof(leased)
    if member == "lease_intent":
        proof[member]["shared_paths"] = ["outputs"]
    else:
        proof[member]["actor"] = "unreviewed-actor"
    content = canonical_json_bytes(proof[member])
    proof[f"{member}_bytes_hex"] = content.hex()
    proof[f"{member}_sha256"] = dispatch._sha(content)
    with pytest.raises(ValueError):
        _verify_retained(leased, proof)


@pytest.mark.parametrize(
    "mutation",
    [
        "intent_bytes",
        "event_bytes",
        "intent_hash",
        "event_hash",
        "intent_dto",
        "event_dto",
        "active_lease",
        "replay_head",
        "replay_event",
        "replay_count",
        "audit_head",
        "audit_dirty",
        "scope",
        "checked_time",
        "outer_root",
        "safety",
    ],
)
def test_retained_proof_rejects_tampered_original_authority(
    leased: _LeaseFixture, mutation: str
) -> None:
    proof = _retained_proof(leased)
    if mutation in {"intent_bytes", "event_bytes"}:
        proof[f"lease_{mutation}_hex"] += "20"
    elif mutation in {"intent_hash", "event_hash"}:
        proof[f"lease_{mutation.replace('_hash', '')}_sha256"] = "0" * 64
    elif mutation == "intent_dto":
        proof["lease_intent"]["actor"] = "unreviewed"
    elif mutation == "event_dto":
        proof["lease_event"]["actor"] = "unreviewed"
    elif mutation == "active_lease":
        proof["active_lease"]["lease_id"] = "lease-forged"
    elif mutation == "replay_head":
        proof["lease_replay"]["lease_heads"] = []
    elif mutation == "replay_event":
        proof["lease_replay"]["head_event_ids"][0]["event_id"] = "lease-event-forged"
    elif mutation == "replay_count":
        proof["lease_replay"]["event_count"] = True
    elif mutation == "audit_head":
        proof["checkout_audit"]["audited_repository"]["head_commit"] = "a" * 40
    elif mutation == "audit_dirty":
        proof["checkout_audit"]["dirty_paths"] = ["config/unowned.yaml"]
    elif mutation == "scope":
        proof["required_paths"] = ["config/unowned.yaml"]
    elif mutation == "checked_time":
        proof["checked_at"] = proof["active_lease"]["expires_at"]
    elif mutation == "outer_root":
        proof["execution_root"] = leased.root.parent.as_posix()
    elif mutation == "safety":
        proof["lease_acquired_or_mutated"] = True
    with pytest.raises(ValueError):
        _verify_retained(leased, proof)


@pytest.mark.parametrize("change", ["root", "candidate", "lease", "scope", "time", "naive"])
def test_retained_proof_rejects_wrong_expected_identity_or_scope(
    leased: _LeaseFixture, change: str
) -> None:
    proof = _retained_proof(leased)
    changes: dict[str, dict[str, Any]] = {
        "root": {"execution_root": leased.root.parent},
        "candidate": {"candidate_commit": "f" * 40},
        "lease": {"source_lease_id": "lease-wrong"},
        "scope": {"required_paths": ("outputs/unclaimed",)},
        "time": {"checked_at": parse_utc_datetime(proof["checked_at"]) + timedelta(microseconds=1)},
        "naive": {"checked_at": datetime(2026, 1, 1)},
    }
    with pytest.raises(ValueError):
        _verify_retained(leased, proof, **changes[change])


@pytest.mark.parametrize(
    "scope", ["outputs/capture-other", "config", "../outputs/capture", "outputs/capture/../x"]
)
def test_restore_rejects_unclaimed_or_noncanonical_scope(leased: _LeaseFixture, scope: str) -> None:
    with pytest.raises(ValueError):
        dispatch.restore_named_capture_lease(
            execution_root=leased.root,
            source_lease_id=leased.handle.lease_id,
            candidate_commit=leased.commit,
            required_paths=(scope,),
        )


@pytest.mark.parametrize(
    "mutation", ["actor", "scope", "path", "released", "head", "expired", "extra_intent"]
)
def test_recheck_rejects_forged_stale_or_tampered_handle(
    leased: _LeaseFixture,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    handle = leased.handle
    if mutation == "actor":
        monkeypatch.setattr(handle, "actor", "unreviewed-actor")
    elif mutation == "scope":
        handle.decision = replace(
            handle.decision, intent=replace(handle.decision.intent, shared_paths=("outputs",))
        )
    elif mutation == "path":
        handle.decision = replace(handle.decision, intent_path=leased.root / "other.json")
    elif mutation == "released":
        handle.release(outcome="synthetic_test_complete")
    elif mutation == "head":
        _git(leased.root, "commit", "--allow-empty", "-m", "synthetic drift")
    elif mutation == "expired":
        monkeypatch.setattr(dispatch, "_now", lambda: datetime.now(UTC) + timedelta(days=1))
    elif mutation == "extra_intent":
        path = handle.decision.intent_path
        raw = json.loads(path.read_bytes())
        path.write_bytes(canonical_json_bytes({**raw, "caller_permission": True}))
    with pytest.raises((ValueError, RuntimeError)):
        dispatch.recheck_named_capture_lease(
            handle, candidate_commit=leased.commit, required_paths=(OUTPUT,)
        )


@pytest.mark.parametrize(
    "platform_name,in_venv", [("win32", True), ("win32", False), ("linux", True), ("darwin", False)]
)
def test_launch_selects_direct_runtime_without_mutating_parent_environment(
    platform_name: str,
    in_venv: bool,
) -> None:
    base = "C:/synthetic/base" if platform_name == "win32" else "/synthetic/base"
    prefix = base + "/venv" if in_venv else base
    suffix = "/python.exe" if platform_name == "win32" else "/bin/python"
    environment = {"KEEP": "value", "PYTHONEXECUTABLE": "secret", "__PYVENV_LAUNCHER__": "stale"}
    original = dict(environment)
    result = dispatch._child_python_launch(
        platform_name=platform_name,
        implementation="cpython",
        logical_executable=prefix + suffix,
        base_executable=base + suffix,
        prefix=prefix,
        base_prefix=base,
        environment=environment,
    )
    mapped = platform_name == "win32" and in_venv
    assert environment == original
    assert result.executable == (base + suffix if mapped else prefix + suffix)
    assert result.environment == {
        "KEEP": "value",
        **({"__PYVENV_LAUNCHER__": prefix + suffix} if mapped else {}),
    }
    assert "secret" not in json.dumps(result.audit)


def test_actual_stdlib_child_preserves_pid_and_venv() -> None:
    launch = dispatch._current_child_python_launch()
    process = subprocess.Popen(
        [
            launch.executable,
            "-I",
            "-B",
            "-X",
            "utf8",
            "-c",
            "import json,os,sys; print(json.dumps([os.getpid(),sys.executable,"
            "sys.prefix,sys.flags.isolated,sys.dont_write_bytecode]))",
        ],
        executable=launch.executable,
        env=launch.environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
    )
    stdout, stderr = process.communicate(timeout=30)
    assert process.returncode == 0, stderr
    pid, executable, prefix, isolated, no_pyc = json.loads(stdout)
    assert pid == process.pid
    assert Path(executable) == Path(sys.executable) and Path(prefix) == Path(sys.prefix)
    assert isolated == 1 and no_pyc is True


@dataclass
class _Harness:
    leased: _LeaseFixture
    request: NamedDQExecutionRequest
    identity: NamedExecutionIdentity
    bootstrap: Any
    child_calls: int = 0
    mode: str = "pass"
    receipt: NamedDQExecutionReceipt | None = None

    def run(self) -> dispatch.NamedQualityDispatchResult:
        return dispatch.dispatch_named_quality_child(
            self.request,
            bootstrap=cast(NamedBootstrapAuthority, self.bootstrap),
            lease=self.leased.handle,
            output_relative_path=OUTPUT,
        )

    def parent(self, result: dispatch.NamedQualityDispatchResult) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            json.loads((self.leased.root / result.parent_receipt.relative_path).read_bytes()),
        )


@pytest.fixture
def harness(leased: _LeaseFixture, monkeypatch: pytest.MonkeyPatch) -> _Harness:
    original = synthetic_receipt()
    evidence = leased.root / "outputs/capture/evidence"
    evidence.mkdir(parents=True)
    request = replace(
        original.request,
        roots=NamedDQRoots(
            original.request.roots.source_root,
            original.request.roots.publication_root,
            leased.root.as_posix(),
            evidence.as_posix(),
        ),
        candidate_commit=leased.commit,
        source_manifest_path=PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
        source_manifest_sha256=PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
    )
    identity = replace(
        original.execution,
        execution_root=leased.root.as_posix(),
        candidate_commit=leased.commit,
        source_manifest_path=request.source_manifest_path,
        source_manifest_sha256=request.source_manifest_sha256,
    )
    context = SimpleNamespace(
        identity=identity,
        process_id=os.getpid(),
        stable_identity_sha256=identity.stable_identity_sha256,
    )
    bootstrap = SimpleNamespace(
        context=context,
        operation="capture",
        canonical_dq_call_count=0,
        source_lease_id=leased.handle.lease_id,
        assert_execution_unchanged=lambda **_: datetime.now(UTC).isoformat(),
    )
    result = _Harness(leased, request, identity, bootstrap)
    monkeypatch.setattr(dispatch, "require_named_execution_context", lambda: context)

    class Child:
        def __init__(self, command: list[str], **kwargs: Any) -> None:
            result.child_calls += 1
            assert command[-2:] == ["--operation", "run"]
            assert command[1:5] == ["-I", "-B", "-X", "utf8"]
            assert kwargs["executable"] == command[0]
            if result.mode == "spawn_error":
                raise OSError("synthetic spawn failure")
            self.pid = 7654
            self.returncode: int | None = None
            self.killed = False

        def communicate(self, timeout: int | None = None) -> tuple[bytes, bytes]:
            if result.mode == "timeout" and not self.killed:
                raise subprocess.TimeoutExpired("synthetic child", 120)
            self.returncode = -9 if self.killed else 2 if result.mode == "exit" else 0
            if result.mode in {"invalid_json", "timeout"}:
                return b"retained partial child bytes", b"retained stderr"
            started = datetime.now(UTC)
            receipt = replace(
                original,
                request=request,
                execution=identity,
                started_at=started,
                checked_at=started,
                ended_at=started,
                execution_observation=NamedExecutionObservation(
                    self.pid, leased.handle.lease_id, started, started, started
                ),
                data_quality_evidence=replace(original.data_quality_evidence, checked_at=started),
            )
            receipt_path = "synthetic/receipt.json"
            path = evidence / receipt_path
            path.parent.mkdir(parents=True)
            path.write_bytes(receipt.canonical_bytes)
            result.receipt = receipt
            child: dict[str, Any] = {
                "schema_version": "named_data_quality_bootstrap_result.v1",
                "status": "PASS",
                "request_id": request.request_id,
                "process_id": self.pid,
                "source_lease_id": leased.handle.lease_id,
                "receipt_id": receipt.receipt_id,
                "receipt_path": receipt_path,
                "receipt_sha256": receipt.canonical_sha256,
                "canonical_dq_call_count": 1,
                "verified_input_seal_exported": False,
                "dispatch_allowed": False,
                "production_effect": "none",
                "broker_action": "none",
                "child_started_at": started.isoformat(),
                "child_terminal_checked_at": datetime.now(UTC).isoformat(),
            }
            edits: dict[str, tuple[str, object]] = {
                "warn": ("status", "WARN"),
                "fail": ("status", "FAIL"),
                "count_zero": ("canonical_dq_call_count", 0),
                "count_excess": ("canonical_dq_call_count", 2),
                "count_bool": ("canonical_dq_call_count", True),
                "count_unknown": ("canonical_dq_call_count", None),
                "wrong_pid": ("process_id", self.pid + 1),
                "wrong_lease": ("source_lease_id", "other"),
                "wrong_sha": ("receipt_sha256", "0" * 64),
                "wrong_request": ("request_id", "other"),
                "future_terminal": (
                    "child_terminal_checked_at",
                    (datetime.now(UTC) + timedelta(days=1)).isoformat(),
                ),
            }
            if result.mode in edits:
                key, value = edits[result.mode]
                child[key] = value
            if result.mode == "postguard":
                bootstrap.operation = "run"
            if result.mode == "duplicate_json":
                return b'{"canonical_dq_call_count":1,"canonical_dq_call_count":0}', b"stderr"
            return canonical_json_bytes(child), b"synthetic stderr"

        def poll(self) -> int | None:
            return self.returncode

        def kill(self) -> None:
            self.killed = True

    monkeypatch.setattr(
        dispatch,
        "subprocess",
        SimpleNamespace(
            Popen=Child,
            TimeoutExpired=subprocess.TimeoutExpired,
            DEVNULL=subprocess.DEVNULL,
            PIPE=subprocess.PIPE,
            CREATE_NO_WINDOW=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        ),
    )
    return result


def test_strict_pass_creates_existing_v1_binding_and_is_consumable(harness: _Harness) -> None:
    result = harness.run()
    assert result.status == "PASS"
    assert result.canonical_dq_call_count == 1 and result.counter_observation_state == "KNOWN"
    assert harness.child_calls == 1 and harness.bootstrap.canonical_dq_call_count == 0
    assert result.run_dispatch_path is not None and harness.receipt is not None
    proof = NamedDQSuccessfulDispatchBinding.from_json_bytes(
        (harness.leased.root / result.run_dispatch_path).read_bytes()
    )
    assert proof.parent_receipt == result.parent_receipt
    verified = _verify_successful_run_dispatch(
        harness.receipt,
        receipt_path=cast(str, result.receipt_path),
        dispatch_path=result.run_dispatch_path,
        dispatch_sha256=result.run_dispatch_sha256,
    )
    assert verified == proof
    parent = harness.parent(result)
    assert all(
        set(parent[key]) == {"path", "sha256", "size_bytes"}
        for key in (
            "request",
            "child_stdout",
            "child_stderr",
            "pre_dispatch_proof",
            "post_dispatch_proof",
        )
    )
    with pytest.raises(dispatch.NamedQualityDispatchError, match="ATTEMPT_ALREADY_EXISTS"):
        harness.run()
    assert harness.child_calls == 1


@pytest.mark.parametrize(
    "filename",
    [
        "post_dispatch_proof.json",
        "child_stdout.json",
        "parent_receipt.json",
        "successful_run_dispatch.json",
    ],
)
def test_terminal_writer_failure_retains_original_counter_and_only_completed_parent_binding(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, filename: str
) -> None:
    original_write = dispatch._write
    failures: list[str] = []

    def fail_write(root: Path, relative: str, content: bytes) -> dict[str, Any]:
        if relative == filename:
            failures.append(relative)
            raise OSError("synthetic terminal publication failure")
        return original_write(root, relative, content)

    monkeypatch.setattr(dispatch, "_write", fail_write)
    with pytest.raises(dispatch.NamedQualityDispatchError) as caught:
        harness.run()
    error = caught.value
    assert error.code == "NAMED_PARENT_TERMINAL_PUBLICATION_FAILED"
    assert filename in str(error) and isinstance(error.__cause__, OSError)
    observation = error.terminal_observation
    assert type(observation) is dispatch.NamedQualityTerminalObservation
    assert observation.status == "BLOCKED"
    assert observation.canonical_dq_call_count == 1
    assert observation.counter_observation_state == "KNOWN"
    assert observation.returncode == 0 and observation.terminal_state == "EXITED"
    dispatch_store = harness.leased.root / OUTPUT
    if filename == "successful_run_dispatch.json":
        assert observation.parent_receipt is not None
        raw = (dispatch_store / "parent_receipt.json").read_bytes()
        assert observation.parent_receipt.sha256 == dispatch._sha(raw)
        assert observation.parent_receipt.size_bytes == len(raw)
        parent = json.loads(raw)
        assert parent["status"] == "PASS" and parent["observed_canonical_dq_call_count"] == 1
    else:
        assert observation.parent_receipt is None
        assert not (dispatch_store / "parent_receipt.json").exists()
    assert not (dispatch_store / "successful_run_dispatch.json").exists()
    assert failures == [filename] and harness.child_calls == 1
    monkeypatch.setattr(dispatch, "_write", original_write)
    with pytest.raises(dispatch.NamedQualityDispatchError, match="ATTEMPT_ALREADY_EXISTS"):
        harness.run()
    assert harness.child_calls == 1


def test_parent_write_exception_never_adopts_same_named_disk_bytes(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_write = dispatch._write

    def lost_write_ack(root: Path, relative: str, content: bytes) -> dict[str, Any]:
        result = original_write(root, relative, content)
        if relative == "parent_receipt.json":
            raise OSError("synthetic writer lost its completion acknowledgement")
        return result

    monkeypatch.setattr(dispatch, "_write", lost_write_ack)
    with pytest.raises(dispatch.NamedQualityDispatchError) as caught:
        harness.run()
    observation = caught.value.terminal_observation
    assert observation is not None and observation.canonical_dq_call_count == 1
    assert observation.parent_receipt is None
    assert (harness.leased.root / OUTPUT / "parent_receipt.json").is_file()
    assert not (harness.leased.root / OUTPUT / "successful_run_dispatch.json").exists()
    assert harness.child_calls == 1


@pytest.mark.parametrize(
    "mode",
    [
        "warn",
        "fail",
        "exit",
        "spawn_error",
        "timeout",
        "invalid_json",
        "duplicate_json",
        "count_zero",
        "count_excess",
        "count_bool",
        "count_unknown",
        "wrong_pid",
        "wrong_lease",
        "wrong_sha",
        "wrong_request",
        "future_terminal",
        "postguard",
    ],
)
def test_failure_preserves_bytes_and_never_mints_success_or_retries(
    harness: _Harness, mode: str
) -> None:
    harness.mode = mode
    result = harness.run()
    assert result.status == "BLOCKED" and result.run_dispatch_path is None
    assert result.receipt_path is None and result.receipt_sha256 is None
    parent = harness.parent(result)
    assert parent["failure"]
    assert Path(parent["child_stdout"]["path"]).is_file()
    assert Path(parent["child_stderr"]["path"]).is_file()
    if mode in {
        "timeout",
        "spawn_error",
        "invalid_json",
        "duplicate_json",
        "count_bool",
        "count_unknown",
    }:
        assert (
            result.canonical_dq_call_count is None and result.counter_observation_state == "UNKNOWN"
        )
    if mode == "timeout":
        assert result.terminal_state == "TIMED_OUT_AND_CHILD_REAPED"
    if mode == "count_excess":
        assert result.canonical_dq_call_count == 2 and result.counter_observation_state == "KNOWN"
    harness.bootstrap.operation = "capture"
    with pytest.raises(dispatch.NamedQualityDispatchError, match="ATTEMPT_ALREADY_EXISTS"):
        harness.run()
    assert harness.child_calls == 1


@pytest.mark.parametrize(
    "mutation", ["operation", "count", "context", "pid", "profile", "source", "lease"]
)
def test_invalid_parent_preconditions_do_not_create_attempt(
    harness: _Harness, mutation: str
) -> None:
    if mutation == "operation":
        harness.bootstrap.operation = "verify"
    elif mutation == "count":
        harness.bootstrap.canonical_dq_call_count = 1
    elif mutation == "context":
        harness.bootstrap.context = None
    elif mutation == "pid":
        harness.bootstrap.context.process_id += 1
    elif mutation == "profile":
        harness.request = replace(harness.request, source_manifest_sha256="0" * 64)
    elif mutation == "source":

        def changed(**_: Any) -> str:
            raise ValueError("synthetic source drift")

        harness.bootstrap.assert_execution_unchanged = changed
    else:
        harness.bootstrap.source_lease_id = "other"
    with pytest.raises(ValueError):
        harness.run()
    assert harness.child_calls == 0
    assert not (harness.leased.root / OUTPUT / "attempt.json").exists()


def test_incomplete_attempt_cannot_be_resumed(harness: _Harness) -> None:
    store = harness.leased.root / OUTPUT
    store.mkdir(parents=True)
    (store / "attempt.json").write_bytes(b"retained incomplete original attempt")
    with pytest.raises(dispatch.NamedQualityDispatchError, match="ATTEMPT_ALREADY_EXISTS"):
        harness.run()
    assert harness.child_calls == 0
    assert (store / "attempt.json").read_bytes() == b"retained incomplete original attempt"


def test_concurrent_same_key_has_exactly_one_dispatch(
    harness: _Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = dispatch._parent_proof
    barrier = Barrier(2)

    def synchronized(*args: Any, **kwargs: Any) -> dict[str, Any]:
        result = original(*args, **kwargs)
        if kwargs["stage"] == "PARENT_PRE_DISPATCH":
            barrier.wait(timeout=30)
        return result

    monkeypatch.setattr(dispatch, "_parent_proof", synchronized)

    def attempt() -> str:
        try:
            return harness.run().status
        except dispatch.NamedQualityDispatchError as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: attempt(), range(2)))
    assert sorted(results) == ["NAMED_PARENT_ATTEMPT_ALREADY_EXISTS", "PASS"]
    assert harness.child_calls == 1


@pytest.mark.parametrize("lease_id", ["../lease-one", "lease-../one", "", "lease-missing"])
def test_restore_never_searches_for_a_replacement_lease(
    leased: _LeaseFixture, lease_id: str
) -> None:
    before = leased.handle.guard.replay().to_dict()
    with pytest.raises(ValueError):
        dispatch.restore_named_capture_lease(
            execution_root=leased.root,
            source_lease_id=lease_id,
            candidate_commit=leased.commit,
            required_paths=(OUTPUT,),
        )
    assert leased.handle.guard.replay().to_dict() == before


def test_production_module_never_imports_test_authority() -> None:
    tree = ast.parse(Path(dispatch.__file__).read_text(encoding="utf-8"))
    imports = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    imports += [
        name.name for node in ast.walk(tree) if isinstance(node, ast.Import) for name in node.names
    ]
    assert not any(
        name.startswith(("test", "pytest", "named_data_quality_support")) for name in imports
    )
