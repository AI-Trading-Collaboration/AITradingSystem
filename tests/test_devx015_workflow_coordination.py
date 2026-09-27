"""Actual execution lifecycle tests; new functionality, not baseline-red claims."""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import re
import subprocess
import sys
import threading
import time
import uuid
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

# Subprocess hang guards below are sized for formal Full load (16 xdist workers plus
# nested actual Full/pytest children); they bound hangs only, never pass/fail meaning.
from test_arch_005_integration_publication_fence import publication_checkout as publication_checkout
from test_arch_005_s2_kernel import BASE_COMMIT, POLICY_PATH, _task
from test_devx015_workflow_execution import NativeOracle, _read_json, _until
from test_devx015_workflow_integration import (
    canonical_merge_repository as canonical_merge_repository,
)
from test_devx015_workflow_integration import (
    small_repository as small_repository,
)

from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
from ai_trading_system.platform.architecture.parallel_control_kernel import (
    FileExecutionLeaseStore,
    evaluate_task_readiness,
    load_parallel_control_policy,
    validate_dependency_graph,
)
from ai_trading_system.platform.architecture.workflow_execution import (
    ExecutionContainmentError,
    WindowsJobProcess,
    execution_environment_sha256,
)

ACTOR = "engineering-agent"


@pytest.mark.parametrize(
    "fault", ["before", "state", "complete", "reverse", "missing", "body",
              "publish", "publish-failure", "publish-tamper",
              "registry", "registry-flush", "registry-unknown", "registry-missing"]
)
def test_recovery_custody_locks_original_roots_across_split_publication(
    tmp_path,
    monkeypatch,
    fault,
):
    from ai_trading_system.platform.architecture import workflow_coordination as coordination

    root, old = tmp_path / "control", tmp_path / "legacy"
    root.mkdir()
    old.mkdir()
    policy = load_parallel_control_policy(POLICY_PATH)
    identity, old_identity = (
        coordination.directory_identity(root),
        coordination.directory_identity(old),
    )
    state = {
        "schema_version": "workflow_host_control.v1",
        "phase": "DRAINING",
        "host_id": coordination.machine_host_id(),
        "root_identity": identity,
        "epoch": "recovery-fixture",
        "registrations": [],
        "legacy_roots": [{"root_identity": old_identity, "epoch": "recovery-fixture"}],
        "resource_markers": {"full": "full.resource", "publication": "publish.resource"},
        "policy_sha256": coordination.control_policy_sha256(policy),
    }

    def encode(value):
        return (json.dumps(value, sort_keys=True) + "\n").encode()

    before, after = encode(state), encode({**state, "phase": "LEGACY_WRITERS_DISABLED"})
    registration = {
        "schema_version": "workflow_machine_registration.v1",
        "host_id": state["host_id"],
        "control_root": root.as_posix(),
        "root_identity": identity,
        "state_sha256": hashlib.sha256(before).hexdigest(),
        "repositories": [
            {
                "common_identity": identity,
                "checkout_identities": [identity],
                "locator_sha256": "b" * 64,
            }
        ],
    }
    previous = encode(registration)
    following = encode({**registration, "state_sha256": hashlib.sha256(after).hexdigest()})
    documents = {
        "before_state": before,
        "after_state": after,
        "before_registration": previous,
        "after_registration": following,
    }
    raw = encode(
        {
            "schema_version": "workflow_cutover_journal.v1",
            **{key: value.hex() for key, value in documents.items()},
        }
    )
    digest = hashlib.sha256(raw).hexdigest()
    (root / ("cutover-" + digest + ".json")).write_bytes(raw)
    (root / coordination.CONTROL_STATE_NAME).write_bytes(
        before if fault in {"before", "reverse"} or fault.startswith("publish") else after
    )
    current_registration = following if fault in {"complete", "reverse"} else previous
    if fault == "missing":
        old.rmdir()  # Empty test-owned directory; identity must not be recreated.
    else:
        (old / coordination.RETIREMENT_NAME).write_bytes(
            encode(
                {
                    "schema_version": "workflow_legacy_retirement.v1",
                    "root_identity": old_identity,
                    "control_root": root.as_posix(),
                    "epoch": state["epoch"],
                }
            )
        )
    journal_held = []

    @contextmanager
    def protected(_files, *, protected_directories):
        assert protected_directories == (root,)
        journal_held.append(True)
        try:
            yield
        finally:
            journal_held.pop()

    @contextmanager
    def pinned(_roots):
        yield  # Explicit native directory-pin seam; arbiter handles below are real.

    admin = object.__new__(coordination._WindowsEnrollmentAdministrator)
    import ctypes
    from ctypes import wintypes

    admin.c, admin.w = ctypes, wintypes
    admin.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    admin._bind(admin.kernel, "CloseHandle", [wintypes.HANDLE], wintypes.BOOL)

    @contextmanager
    def security():
        # Explicit ACL seam: native files remain ordinary disposable test files.
        class Attributes(ctypes.Structure):
            _fields_ = [("size", wintypes.DWORD), ("descriptor", ctypes.c_void_p),
                        ("inherit", wintypes.BOOL)]
        yield Attributes(ctypes.sizeof(Attributes), None, False)

    monkeypatch.setattr(admin, "_security", security)
    monkeypatch.setattr(admin, "assert_protected", lambda _path, **_kwargs: None)
    monkeypatch.setattr(admin, "hold_protected_files", protected)
    monkeypatch.setattr(admin, "pin_directories", pinned)
    monkeypatch.setattr(coordination, "_WindowsEnrollmentAdministrator", lambda: admin)
    monkeypatch.setattr(
        coordination, "_trusted_host_registration_bytes", lambda: current_registration
    )
    custody = None
    if fault in {"reverse", "missing"}:
        with pytest.raises((ParallelControlError, FileNotFoundError)):
            with coordination.host_cutover_recovery_custody(
                root,
                journal_sha256=digest,
                policy=policy,
                actor=ACTOR,
            ):
                pytest.fail("invalid recovery admitted")
        assert not (root / "arbiter.lock").exists()
        if fault == "missing":
            assert not old.exists()
    else:
        try:
            with coordination.host_cutover_recovery_custody(
                root,
                journal_sha256=digest,
                policy=policy,
                actor=ACTOR,
            ) as custody:
                assert journal_held and len(custody.lease_snapshots) == 2
                assert (
                    custody.assert_current()
                    == {
                        "before": "BEFORE_PUBLICATION",
                        "state": "STATE_PUBLISHED",
                        "complete": "REGISTRATION_PUBLISHED",
                        "body": "STATE_PUBLISHED",
                        "publish": "BEFORE_PUBLICATION",
                        "publish-failure": "BEFORE_PUBLICATION",
                        "publish-tamper": "BEFORE_PUBLICATION",
                        "registry": "STATE_PUBLISHED",
                        "registry-flush": "STATE_PUBLISHED",
                        "registry-unknown": "STATE_PUBLISHED",
                        "registry-missing": "STATE_PUBLISHED",
                    }[fault]
                )
                for index, target in enumerate((root, old)):
                    assert (
                        _cutover_lock_probe(target, tmp_path, f"recovery-{index}")["status"]
                        == "LEASE_ARBITER_BUSY"
                    )
                if fault == "body":
                    raise RuntimeError("consumer crash")
                if fault == "before":
                    with pytest.raises(ParallelControlError, match="REGISTRATION_ORDER"):
                        admin._publish_cutover_registration(custody)
                if fault.startswith("registry"):
                    import winreg

                    calls = []
                    original_open = winreg.OpenKey
                    original_query = winreg.QueryValueEx

                    class Key:
                        def __enter__(self):
                            return self

                        def __exit__(self, *_args):
                            calls.append("close")

                    key = Key()

                    def open_key(hive, name, reserved=0, access=winreg.KEY_READ):
                        if name != coordination.HOST_REGISTRY_KEY:
                            return original_open(hive, name, reserved, access)
                        assert hive == winreg.HKEY_LOCAL_MACHINE and reserved == 0
                        assert access == (winreg.KEY_READ | winreg.KEY_SET_VALUE
                                          | winreg.KEY_WOW64_64KEY)
                        calls.append("open")
                        if fault == "registry-missing":
                            raise FileNotFoundError("injected missing registration")
                        return key

                    def query(handle, name):
                        if handle is not key:
                            return original_query(handle, name)
                        assert name == coordination.HOST_REGISTRY_VALUE
                        return ("unknown" if fault == "registry-unknown"
                                else current_registration.decode("utf-8")), winreg.REG_SZ

                    def write(handle, name, reserved, kind, value):
                        nonlocal current_registration
                        assert handle is key and name == coordination.HOST_REGISTRY_VALUE
                        assert reserved == 0 and kind == winreg.REG_SZ
                        assert value.encode("utf-8") == following
                        calls.append("write")
                        current_registration = value.encode("utf-8")

                    def flush(handle):
                        assert handle is key
                        calls.append("flush")
                        if fault == "registry-flush" and calls.count("flush") == 1:
                            raise OSError("injected flush failure")

                    monkeypatch.setattr(winreg, "OpenKey", open_key)
                    monkeypatch.setattr(winreg, "QueryValueEx", query)
                    monkeypatch.setattr(winreg, "SetValueEx", write)
                    monkeypatch.setattr(winreg, "FlushKey", flush)
                    if fault == "registry-missing":
                        with pytest.raises(FileNotFoundError):
                            admin._publish_cutover_registration(custody)
                        assert "write" not in calls
                    elif fault == "registry-unknown":
                        with pytest.raises(ParallelControlError, match="REGISTRATION_CHANGED"):
                            admin._publish_cutover_registration(custody)
                        assert "write" not in calls and calls[-1] == "close"
                    else:
                        if fault == "registry-flush":
                            with pytest.raises(OSError, match="injected flush failure"):
                                admin._publish_cutover_registration(custody)
                            assert custody.assert_current() == "REGISTRATION_PUBLISHED"
                        assert admin._publish_cutover_registration(custody) == (
                            "REGISTRATION_PUBLISHED"
                        )
                        assert admin._publish_cutover_registration(custody) == (
                            "REGISTRATION_PUBLISHED"
                        )
                        assert calls.count("write") == 1
                        assert calls.count("flush") == (3 if fault == "registry-flush" else 2)
                if fault in {"publish", "state", "complete"}:
                    expected = ("REGISTRATION_PUBLISHED" if fault == "complete"
                                else "STATE_PUBLISHED")
                    assert admin._publish_cutover_state(custody) == expected
                    assert admin._publish_cutover_state(custody) == expected
                    assert (root / coordination.CONTROL_STATE_NAME).read_bytes() == after
                    assert not list(root.glob(".*.pending-*"))
                elif fault == "publish-failure":
                    original_bind = admin._bind

                    def bind(library, name, arguments, result):
                        if name == "MoveFileExW":
                            def fail(*_args):
                                raise OSError("injected publication failure")
                            return fail
                        return original_bind(library, name, arguments, result)

                    monkeypatch.setattr(admin, "_bind", bind)
                    with pytest.raises(OSError, match="injected publication failure"):
                        admin._publish_cutover_state(custody)
                    assert (root / coordination.CONTROL_STATE_NAME).read_bytes() == before
                    retained = list(root.glob(".*.pending-*"))
                    assert len(retained) == 1 and retained[0].read_bytes() == after
                elif fault == "publish-tamper":
                    (root / coordination.CONTROL_STATE_NAME).write_bytes(b"unknown")
                    with pytest.raises(ParallelControlError, match="OBSERVATION_UNKNOWN"):
                        admin._publish_cutover_state(custody)
                    assert (root / coordination.CONTROL_STATE_NAME).read_bytes() == b"unknown"
                    assert not list(root.glob(".*.pending-*"))
        except RuntimeError as exc:
            assert fault == "body" and str(exc) == "consumer crash"
        assert custody is not None
        with pytest.raises(ParallelControlError, match="CUTOVER_RECOVERY_CLOSED"):
            custody.assert_current()
        for index, target in enumerate((root, old)):
            assert (
                _cutover_lock_probe(target, tmp_path, f"released-{index}")["status"] == "ACQUIRED"
            )
    assert not journal_held
    assert (root / ("cutover-" + digest + ".json")).read_bytes() == raw


@pytest.mark.parametrize(
    "fault",
    ["none", "missing-key", "missing-value", "legacy-key", "denied", "kind", "schema"],
)
def test_registration_bytes_preserve_text_and_fail_closed(tmp_path, monkeypatch, fault):
    import winreg

    from ai_trading_system.platform.architecture import workflow_coordination as coordination

    identity = coordination.directory_identity(tmp_path)
    value = {
        "schema_version": "workflow_machine_registration.v1",
        "host_id": coordination.machine_host_id(),
        "control_root": tmp_path.as_posix(),
        "root_identity": identity,
        "state_sha256": "a" * 64,
        "repositories": [
            {
                "common_identity": identity,
                "checkout_identities": [identity],
                "locator_sha256": "b" * 64,
            }
        ],
    }
    if fault == "schema":
        value["extra"] = True
    text = " \n" + json.dumps(value, indent=3) + "\r\n"

    class Key:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    key = Key()
    original_open, original_query = winreg.OpenKey, winreg.QueryValueEx

    def opened(hive, path, reserved=0, access=winreg.KEY_READ):
        if path == coordination.LEGACY_HOST_REGISTRY_KEY and fault == "legacy-key":
            assert hive == winreg.HKEY_LOCAL_MACHINE
            return Key()
        if path != coordination.HOST_REGISTRY_KEY:
            return original_open(hive, path, reserved, access)
        assert hive == winreg.HKEY_LOCAL_MACHINE
        assert access == winreg.KEY_READ | winreg.KEY_WOW64_64KEY
        if fault == "missing-key":
            raise FileNotFoundError()
        if fault == "denied":
            raise PermissionError()
        return key

    def queried(handle, name):
        if handle is not key:
            return original_query(handle, name)
        assert name == coordination.HOST_REGISTRY_VALUE
        if fault == "missing-value":
            raise FileNotFoundError()
        return text, winreg.REG_BINARY if fault == "kind" else winreg.REG_SZ

    monkeypatch.setattr(winreg, "OpenKey", opened)
    monkeypatch.setattr(winreg, "QueryValueEx", queried)
    for reader in (
        coordination._trusted_host_registration_bytes,
        coordination._trusted_host_registration,
    ):
        # DEVX-015A: an absent anchor value on the parent key is not enrolled.
        if fault in {"missing-key", "missing-value"}:
            assert reader() is None
        elif fault == "legacy-key":
            with pytest.raises(ParallelControlError, match="HOST_REGISTRATION_LEGACY_KEY"):
                reader()
        elif fault == "denied":
            with pytest.raises(PermissionError):
                reader()
        elif fault in {"kind", "schema"}:
            with pytest.raises(ParallelControlError, match="HOST_REGISTRATION_"):
                reader()
        else:
            expected = (
                text.encode("utf-8")
                if reader is (coordination._trusted_host_registration_bytes)
                else value
            )
            assert reader() == expected


@pytest.mark.parametrize("fault", ["none", "body", "epoch", "digest", "extra"])
def test_cutover_journal_parses_only_while_transport_is_held(tmp_path, monkeypatch, fault):
    from ai_trading_system.platform.architecture import workflow_coordination as coordination

    root = tmp_path.absolute()
    identity = coordination.directory_identity(root)
    state = {
        "schema_version": "workflow_host_control.v1",
        "phase": "DRAINING",
        "host_id": coordination.machine_host_id(),
        "root_identity": identity,
        "epoch": "fixture",
        "registrations": [],
        "legacy_roots": [],
        "resource_markers": {"full": "full.resource", "publication": "publish.resource"},
        "policy_sha256": "a" * 64,
    }
    changed = {**state, "phase": "LEGACY_WRITERS_DISABLED"}
    if fault == "epoch":
        changed["epoch"] = "other"

    def encode(value):
        return (json.dumps(value, sort_keys=True) + "\n").encode()

    before, after = encode(state), encode(changed)
    registration = {
        "schema_version": "workflow_machine_registration.v1",
        "host_id": state["host_id"],
        "control_root": root.as_posix(),
        "root_identity": identity,
        "state_sha256": hashlib.sha256(before).hexdigest(),
        "repositories": [
            {
                "common_identity": identity,
                "checkout_identities": [identity],
                "locator_sha256": "b" * 64,
            }
        ],
    }
    documents = {
        "before_state": before,
        "after_state": after,
        "before_registration": encode(registration),
        "after_registration": encode(
            {**registration, "state_sha256": hashlib.sha256(after).hexdigest()}
        ),
    }
    payload = {
        "schema_version": "workflow_cutover_journal.v1",
        **{key: raw.hex() for key, raw in documents.items()},
    }
    if fault == "extra":
        payload["activation_allowed"] = True
    raw = encode(payload)
    digest = hashlib.sha256(raw).hexdigest() if fault != "digest" else "c" * 64
    path = root / ("cutover-" + digest + ".json")
    path.write_bytes(raw)
    active = []

    @contextmanager
    def held(files, *, protected_directories):
        assert files == {path: digest} and protected_directories == (root,)
        active.append(True)
        try:
            yield
        finally:
            active.pop()

    # Explicit transport seam; parsing, identity/schema and byte checks are real.
    admin = object.__new__(coordination._WindowsEnrollmentAdministrator)
    monkeypatch.setattr(admin, "hold_protected_files", held)
    expected = {
        "epoch": "CUTOVER_TRANSITION_BINDING",
        "digest": "CUTOVER_JOURNAL_CHANGED",
        "extra": "CUTOVER_JOURNAL_SCHEMA",
    }
    if fault in expected:
        with pytest.raises(ParallelControlError, match=expected[fault]):
            with admin.hold_cutover_journal(root, digest):
                pytest.fail("invalid journal admitted")
    elif fault == "body":
        with pytest.raises(RuntimeError, match="consumer interrupted"):
            with admin.hold_cutover_journal(root, digest) as observed:
                assert active and observed == documents
                raise RuntimeError("consumer interrupted")
    else:
        with admin.hold_cutover_journal(root, digest) as observed:
            assert active and observed == documents
    assert active == [] and path.read_bytes() == raw


@pytest.mark.parametrize("stage", ["before", "state", "registration", "reverse", "unknown"])
@pytest.mark.parametrize("phase", ["DRAINING", "LEGACY_WRITERS_DISABLED"])
def test_cutover_publication_position_requires_exact_ordered_bytes(stage, phase):
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _cutover_publication_position,
    )

    def encode(value):
        return (json.dumps(value, sort_keys=True) + "\n").encode()

    old = {"schema_version": "workflow_host_control.v1", "phase": phase, "epoch": "same"}
    new = {**old, "phase": "LEGACY_WRITERS_DISABLED" if phase == "DRAINING" else "ACTIVE"}
    before, after = encode(old), encode(new)
    registration = {"state_sha256": hashlib.sha256(before).hexdigest(), "host_id": "same"}
    previous = encode(registration)
    following = encode({**registration, "state_sha256": hashlib.sha256(after).hexdigest()})
    observed = {
        "before": (before, previous),
        "state": (after, previous),
        "registration": (after, following),
        "reverse": (before, following),
        "unknown": (after + b" ", following),
    }[stage]
    arguments = dict(
        before_state=before,
        after_state=after,
        before_registration=previous,
        after_registration=following,
        observed_state=observed[0],
        observed_registration=observed[1],
    )
    if stage in {"reverse", "unknown"}:
        with pytest.raises(ParallelControlError, match="CUTOVER_PUBLICATION_OBSERVATION_UNKNOWN"):
            _cutover_publication_position(**arguments)
    else:
        assert (
            _cutover_publication_position(**arguments)
            == {
                "before": "BEFORE_PUBLICATION",
                "state": "STATE_PUBLISHED",
                "registration": "REGISTRATION_PUBLISHED",
            }[stage]
        )
    # Rebinding the registry hash cannot hide a changed epoch or skipped phase.
    altered = encode({**new, "epoch": "other"})
    arguments.update(
        after_state=altered,
        after_registration=encode(
            {
                **registration,
                "state_sha256": hashlib.sha256(altered).hexdigest(),
            }
        ),
    )
    with pytest.raises(ParallelControlError, match="CUTOVER_TRANSITION_BINDING"):
        _cutover_publication_position(**arguments)


@pytest.mark.parametrize("fault", [
    "none", "running", "orig-head", "candidate-reflog", "main-reflog", "unknown-lock",
    "merge-state",
])
def test_failed_git_launch_requires_native_exit_and_unchanged_auxiliary_files(tmp_path, fault):
    """Actual native process/files; no fabricated Full, lease or publication permit."""
    from ai_trading_system.platform.architecture import workflow_coordination as coordination
    from ai_trading_system.platform.architecture import workflow_integration as integration
    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_execution import (
        current_process_identity,
    )

    watched = [tmp_path / name for name in ("ORIG_HEAD", "candidate-reflog", "main-reflog")]
    for path in watched:
        path.write_bytes(b"original auxiliary state\n")
    absent = [tmp_path / "main.lock", tmp_path / "MERGE_HEAD"]
    plan = {
        "orig_head": integration._local_publication_metadata(watched[0], contents=True),
        "reflogs": {
            "candidate": integration._local_publication_metadata(watched[1]),
            "main": integration._local_publication_metadata(watched[2]),
        },
        "absent_paths": [path.as_posix() for path in absent],
    }
    completed = subprocess.run(
        [sys.executable, "-c", "import json; from ai_trading_system.platform.architecture."
         "workflow_execution import current_process_identity; "
         "print(json.dumps(current_process_identity()))"],
        capture_output=True, text=True, check=True,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
    )
    process = (current_process_identity() if fault == "running"
               else json.loads(completed.stdout))
    attempt = {"git_launch": {"process": process}, "checkout_plan": {"plan": plan}}
    mutations = {"orig-head": watched[0], "candidate-reflog": watched[1],
                 "main-reflog": watched[2], "unknown-lock": absent[0], "merge-state": absent[1]}
    if fault in mutations:
        mutations[fault].write_bytes(b"unrelated owner residue - preserve\n")
    before = {path: path.read_bytes() if path.exists() else None for path in watched + absent}
    if fault == "none":
        result = coordination._observe_unchanged_git_launch(attempt)
        assert result["git_process"]["state"] in {"EXITED", "REUSED"}
        assert result["auxiliary_state"] == "ORIGINAL_UNCHANGED"
    else:
        code = ("NOT_TERMINAL" if fault == "running" else "STATE_EXISTS"
                if fault in {"unknown-lock", "merge-state"} else "EFFECTS_REMAIN")
        with pytest.raises((ParallelControlError, WorkflowContractError), match=code):
            coordination._observe_unchanged_git_launch(attempt)
    assert {path: path.read_bytes() if path.exists() else None
            for path in watched + absent} == before
    assert coordination._observe_unchanged_git_launch({}) is None


@pytest.mark.parametrize("schema", [
    "lease_execution.v1", "lease_execution.v4", "lease_execution.v5",
])
def test_full_projection_copies_only_retained_fields(schema, monkeypatch):
    """Pure projection/copy behavior, not an execution or publication capability."""
    from ai_trading_system.platform.architecture import workflow_coordination as coordination

    value = {
        "schema_version": schema, "request": {"nested": [1]},
        "publication_attempts": [{"readset": [2, 3]}],
        "publication_stable_observation": {"nested": [4]},
    }
    expected = json.loads(json.dumps(value))
    if schema == "lease_execution.v5":
        expected.pop("publication_attempts")
        expected.pop("publication_stable_observation")
        expected["schema_version"] = "lease_execution.v4"
    copied_keys = []
    original_copy = coordination._copy

    def observe_copy(payload):
        copied_keys.append(set(payload))
        return original_copy(payload)

    monkeypatch.setattr(coordination, "_copy", observe_copy)
    actual = coordination.full_execution_projection(value)
    assert actual == expected and copied_keys == [set(expected)]
    actual["request"]["nested"].append(5)
    assert value["request"]["nested"] == [1]
    assert value["schema_version"] == schema
    if schema != "lease_execution.v5":
        actual["publication_attempts"][0]["readset"].append(6)
        assert value["publication_attempts"][0]["readset"] == [2, 3]


@pytest.mark.parametrize("fault", [
    "none", "scope", "candidate", "transaction", "event", "execution", "dispatch",
    "empty-captures", "duplicate-capture", "capture-outside", "capture-size", "capture-sha",
    "original-topology", "stable-state", "self-pass",
])
def test_failed_index_stable_observation_binding_contract(tmp_path, fault):
    """Shape/binding only: synthetic envelope cannot admit Full or append any event."""
    from ai_trading_system.platform.architecture.workflow_contract import canonical_digest
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _validate_unchanged_publication_observation,
    )
    from ai_trading_system.platform.architecture.workflow_execution import current_process_identity

    original = {
        "candidate_checkout": {"index": {
            "path": (tmp_path / ".git/index").as_posix(), "identity": [1, 2],
            "sha256": "a" * 64, "size": 128,
        }},
        "candidate_index_matches_tree": True,
    }
    original["topology_sha256"] = canonical_digest(original)
    observed = json.loads(json.dumps(original))
    observed["candidate_checkout"]["index"]["identity"] = [1, 3]
    observed["topology_sha256"] = canonical_digest(
        {key: value for key, value in observed.items() if key != "topology_sha256"}
    )
    request = {
        "cwd": tmp_path.as_posix(), "candidate_sha": "b" * 40, "expected_main_sha": "c" * 40,
        "lease_id": "binding-only", "publication_transaction_sha256": "d" * 64,
        "local_publication_event_id": "local-event",
    }
    request["local_publication_intent_sha256"] = canonical_digest({
        "schema_version": "integration_publication_local_intent.v1",
        "transaction_sha256": request["publication_transaction_sha256"],
        "lease_id": request["lease_id"], "candidate_sha": request["candidate_sha"],
        "expected_main_sha": request["expected_main_sha"], "topology": original,
        "dispatch_allowed": False, "publication_allowed": False,
    })
    attempt = {"request": request, "request_sha256": canonical_digest(request),
               "state": "RESULT_RECORDED", "result": {"status": "FAIL"}}
    execution = {"publication_attempts": [attempt]}
    profile = {
        "schema_version": "full_publication_profile_inspection.v1", "status": "PASS",
        "scope": "PROFILE_MANDATORY_AND_READINESS_IDENTITY",
        "candidate_sha": request["candidate_sha"],
        "transaction_sha256": request["publication_transaction_sha256"],
        "head_event_id": request["local_publication_event_id"],
        "execution_sha256": canonical_digest(execution),
        "dispatch_performed": False, "publication_performed": False,
        "captures": [{"path": (tmp_path / "outputs/contract-only.json").as_posix(),
                      "size_bytes": 2, "sha256": "e" * 64}],
    }
    observation = {
        "schema_version": "workflow_publication_stable_observation.v2",
        "stable_state": "CANDIDATE_STABLE_INDEX_REPLACED",
        "request_sha256": attempt["request_sha256"], "execution_sha256": canonical_digest(attempt),
        "topology": observed, "original_topology": original, "profile_inspection": profile,
        "observer": current_process_identity(), "observed_at": datetime.now(UTC).isoformat(),
    }
    execution["publication_stable_observation"] = observation
    profile_fields = {"scope": "scope", "candidate": "candidate_sha",
                      "transaction": "transaction_sha256", "event": "head_event_id",
                      "execution": "execution_sha256", "dispatch": "dispatch_performed"}
    if fault in profile_fields:
        profile[profile_fields[fault]] = "incorrect"
    elif fault == "empty-captures":
        profile["captures"] = []
    elif fault == "duplicate-capture":
        profile["captures"] *= 2
    elif fault == "capture-outside":
        profile["captures"][0]["path"] = (tmp_path.parent / "outside.json").as_posix()
    elif fault == "capture-size":
        profile["captures"][0]["size_bytes"] = True
    elif fault == "capture-sha":
        profile["captures"][0]["sha256"] = "invalid"
    elif fault == "original-topology":
        original["topology_sha256"] = "0" * 64
    elif fault == "stable-state":
        observation["stable_state"] = "ORIGINAL_UNCHANGED"
    elif fault == "self-pass":
        attempt["result"]["status"] = "PASS"
    if fault == "none":
        _validate_unchanged_publication_observation(execution)
    else:
        with pytest.raises(ParallelControlError, match="LEASE_EXECUTION_"):
            _validate_unchanged_publication_observation(execution)
    assert list(tmp_path.iterdir()) == []  # No profile/artifact/lease was manufactured.


def _remove_native_registry_fixture(registry, registry_root: str) -> None:
    """Remove only a fresh owned fixture tree, including OS-created descendants.

    DEVX-015 v220: inventory the entire bounded tree before the first deletion.
    This helper is not an entry point for cleaning retained historical fixtures.
    """
    assert re.fullmatch(r"Software\\AITS-DEVX015-Test-[0-9a-f]{32}", registry_root)
    pending = [(registry_root, 0)]
    inventory = []
    while pending:
        path, depth = pending.pop()
        assert depth <= 16, "native fixture registry depth exceeds cleanup budget"
        assert len(inventory) < 256, "native fixture registry inventory exceeds cleanup budget"
        assert path == registry_root or path.startswith(registry_root + "\\")
        with registry.OpenKey(
            registry.HKEY_CURRENT_USER, path, 0,
            registry.KEY_READ | registry.KEY_WOW64_64KEY,
        ) as key:
            count = registry.QueryInfoKey(key)[0]
            assert len(inventory) + len(pending) + count < 256, (
                "native fixture registry inventory exceeds cleanup budget"
            )
            for index in range(count):
                name = registry.EnumKey(key, index)
                assert name and "\\" not in name and name not in {".", ".."}
                pending.append((path + "\\" + name, depth + 1))
        inventory.append(path)
    # Every descendant occurs after its parent in the inventory; reverse it.
    for path in reversed(inventory):
        registry.DeleteKeyEx(registry.HKEY_CURRENT_USER, path, registry.KEY_WOW64_64KEY)
    with pytest.raises(FileNotFoundError):
        registry.OpenKey(
            registry.HKEY_CURRENT_USER, registry_root, 0,
            registry.KEY_READ | registry.KEY_WOW64_64KEY,
        )


@pytest.mark.parametrize("fault", [
    "none", "root-only", "invalid-root", "enumeration-error", "wide", "deep", "invalid-child",
])
def test_native_registry_fixture_cleanup_inventory_contract(fault: str) -> None:
    """In-memory transport only; this does not exercise or authorize native deletion."""
    root = "Software\\AITS-DEVX015-Test-" + "a" * 32
    other = "Software\\AITS-DEVX015-Test-" + "b" * 32
    tree = {
        root: ["Software", "System"],
        root + "\\Software": [],
        root + "\\System": ["CurrentControlSet"],
        root + "\\System\\CurrentControlSet": ["Services"],
        root + "\\System\\CurrentControlSet\\Services": [],
        other: ["Keep"], other + "\\Keep": [],
    }
    if fault == "root-only":
        tree = {root: [], other: ["Keep"], other + "\\Keep": []}
    elif fault == "wide":
        tree[root] = [f"child-{index}" for index in range(256)]
    elif fault == "deep":
        tree = {other: ["Keep"], other + "\\Keep": []}
        for depth in range(18):
            tree[root + "\\child" * depth] = ["child"] if depth < 17 else []
    elif fault == "invalid-child":
        tree[root] = ["..\\outside"]
    original = {key: list(value) for key, value in tree.items()}

    class RegistryModel:
        HKEY_CURRENT_USER, KEY_READ, KEY_WOW64_64KEY = 1, 2, 4

        def __init__(self):
            self.deleted = []
            self.opened = []
            self.handles = set()

        def OpenKey(self, hive, path, reserved, access):
            assert (hive, reserved, access) == (1, 0, 6)
            self.opened.append(path)
            if path not in tree:
                raise FileNotFoundError(path)
            self.handles.add(path)

            return self.handle(path)

        @contextmanager
        def handle(self, path):
            try:
                yield path
            finally:
                self.handles.remove(path)

        def QueryInfoKey(self, key):
            return len(tree[key]), 0, 0

        def EnumKey(self, key, index):
            if fault == "enumeration-error" and key.endswith("\\System"):
                raise PermissionError("injected enumeration access failure")
            return tree[key][index]

        def DeleteKeyEx(self, hive, path, access):
            assert (hive, access) == (1, 4)
            assert not self.handles, "inventory handles must close before deletion"
            assert not tree[path], "parent must not be deleted before its children"
            self.deleted.append(path)
            del tree[path]
            parent, _, name = path.rpartition("\\")
            if parent in tree:
                tree[parent].remove(name)

    registry = RegistryModel()
    if fault in {"none", "root-only"}:
        _remove_native_registry_fixture(registry, root)
        assert tree == {other: ["Keep"], other + "\\Keep": []}
        expected = {key for key in original if key == root or key.startswith(root + "\\")}
        assert set(registry.deleted) == expected
        assert len(registry.deleted) == len(expected)
        assert registry.deleted[-1] == root
    else:
        expected_error = PermissionError if fault == "enumeration-error" else AssertionError
        with pytest.raises(expected_error):
            _remove_native_registry_fixture(
                registry, root + "\\System" if fault == "invalid-root" else root
            )
        assert registry.deleted == [] and tree == original
        if fault == "invalid-root":
            assert registry.opened == []
    assert not registry.handles
    assert all(path == root or path.startswith(root + "\\") for path in registry.opened)


@contextmanager
def native_full_host_registration(
    root: Path, monkeypatch: pytest.MonkeyPatch, *, linked_contender: bool = False,
    independent_contender: bool = False,
):
    """Register a disposable existing checkout before its first lease, using native APIs."""
    import ctypes
    import winreg

    from ai_trading_system.platform.architecture.checkout_guard import CheckoutLeaseGuard
    from ai_trading_system.platform.architecture.workflow_coordination import (
        CONTROL_BINDING_NAME,
        CONTROL_STATE_NAME,
        HOST_REGISTRY_KEY,
        HOST_REGISTRY_VALUE,
        control_policy_sha256,
        directory_identity,
        machine_host_id,
    )

    guard = CheckoutLeaseGuard(
        project_root=root,
        policy_path=root / "config/architecture/arch_005_s4d_checkout_guard.yaml",
        parallel_policy_path=root / "config/architecture/arch_005_parallel_control_policy.yaml",
    )
    assert not guard.replay().active_leases
    assert not guard.store.events_root.exists(), "native fixture enrolment must precede first lease"
    common = Path(
        subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "--path-format=absolute", "--git-common-dir"],
            text=True,
        ).strip()
    )
    control = root.parent / "native-host-control"
    bootstrap = root.parent / "native-host-bootstrap"
    control.mkdir()
    bootstrap.mkdir()
    identity = directory_identity(control)
    host = machine_host_id()
    row = {
        "registration_id": "native-full-repository",
        "common_root": common.as_posix(),
        "common_identity": directory_identity(common),
        "entrypoints": ["checkout-guard"],
    }
    shared = {
        "host_id": host,
        "epoch": "native-full-v1",
        "root_identity": identity,
        "policy_sha256": control_policy_sha256(guard.lease_policy),
        "resource_markers": {
            "publication": "outputs/architecture/arch_005_integration_publication_fence/"
            "publication.resource",
            "full": "outputs/validation_runtime",
        },
    }
    state = {
        "schema_version": "workflow_host_control.v1",
        "phase": "ACTIVE",
        **shared,
        "registrations": [row],
        "legacy_roots": [],
    }
    locator = {
        "schema_version": "workflow_host_binding.v1",
        "control_root": control.as_posix(),
        **shared,
        **{key: value for key, value in row.items() if key != "entrypoints"},
    }
    (control / CONTROL_STATE_NAME).write_text(json.dumps(state), encoding="utf-8")
    assert not (common / CONTROL_BINDING_NAME).exists()
    (common / CONTROL_BINDING_NAME).write_text(json.dumps(locator), encoding="utf-8")
    enrolled = [root]
    if linked_contender:
        peer = root.parent / "native-full-contender"
        subprocess.run(
            ["git", "-C", str(root), "worktree", "add", "--detach", str(peer), "HEAD"],
            check=True, capture_output=True,
        )
        enrolled.append(peer)
    registration = {
        "schema_version": "workflow_machine_registration.v1",
        "host_id": host,
        "control_root": control.as_posix(),
        "root_identity": identity,
        "state_sha256": hashlib.sha256((control / CONTROL_STATE_NAME).read_bytes()).hexdigest(),
        "repositories": [
            {
                "common_identity": directory_identity(common),
                "checkout_identities": [directory_identity(checkout) for checkout in enrolled],
                "locator_sha256": hashlib.sha256(
                    (common / CONTROL_BINDING_NAME).read_bytes(),
                ).hexdigest(),
            }
        ],
    }
    if independent_contender:
        assert not linked_contender
        peer = root.parent / "native-full-contender"
        subprocess.run(
            ["git", "clone", "--no-hardlinks", "--config", "core.longpaths=true",
             str(root), str(peer)],
            check=True, capture_output=True,
        )
        original_main = subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "refs/heads/main"], text=True,
        ).strip()
        peer_main = subprocess.run(
            ["git", "-C", str(peer), "show-ref", "--verify", "--quiet", "refs/heads/main"],
            capture_output=True,
        )
        assert peer_main.returncode in {0, 1}
        if peer_main.returncode == 1:
            subprocess.run(["git", "-C", str(peer), "branch", "main", original_main],
                           check=True, capture_output=True)
        assert subprocess.check_output(
            ["git", "-C", str(peer), "rev-parse", "refs/heads/main"], text=True,
        ).strip() == original_main
        origin = subprocess.check_output(
            ["git", "-C", str(root), "remote", "get-url", "origin"], text=True,
        ).strip()
        subprocess.run(["git", "-C", str(peer), "remote", "set-url", "origin", origin],
                       check=True, capture_output=True)
        peer_common = peer / ".git"
        peer_row = {**row, "registration_id": "native-full-independent",
                    "common_root": peer_common.as_posix(),
                    "common_identity": directory_identity(peer_common)}
        state["registrations"].append(peer_row)
        (control / CONTROL_STATE_NAME).write_text(json.dumps(state), encoding="utf-8")
        peer_locator = {**locator, **{key: value for key, value in peer_row.items()
                                     if key != "entrypoints"}}
        (peer_common / CONTROL_BINDING_NAME).write_text(json.dumps(peer_locator), encoding="utf-8")
        registration["state_sha256"] = hashlib.sha256(
            (control / CONTROL_STATE_NAME).read_bytes(),
        ).hexdigest()
        registration["repositories"].append({
            "common_identity": directory_identity(peer_common),
            "checkout_identities": [directory_identity(peer)],
            "locator_sha256": hashlib.sha256(
                (peer_common / CONTROL_BINDING_NAME).read_bytes(),
            ).hexdigest(),
        })
    registry_root = r"Software\AITS-DEVX015-Test-" + uuid.uuid4().hex
    with pytest.raises(FileNotFoundError):
        winreg.OpenKey(
            winreg.HKEY_CURRENT_USER, registry_root, 0,
            winreg.KEY_READ | winreg.KEY_WOW64_64KEY,
        )
    crypto = r"SOFTWARE\Microsoft\Cryptography"
    created = []
    override = ctypes.WinDLL("advapi32", use_last_error=True).RegOverridePredefKey
    override.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    override.restype = ctypes.c_long
    hklm = ctypes.c_void_p(ctypes.c_int32(int(winreg.HKEY_LOCAL_MACHINE)).value)
    activated = False
    try:
        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE, crypto, 0, winreg.KEY_READ | winreg.KEY_WOW64_64KEY
        ) as original:
            guid, kind = winreg.QueryValueEx(original, "MachineGuid")
        for suffix in (
            "",
            "SOFTWARE",
            r"SOFTWARE\AITradingSystem",
            HOST_REGISTRY_KEY,
            r"SOFTWARE\Microsoft",
            crypto,
        ):
            key_path = registry_root + ("\\" + suffix if suffix else "")
            with winreg.CreateKeyEx(
                winreg.HKEY_CURRENT_USER,
                key_path,
                0,
                winreg.KEY_ALL_ACCESS | winreg.KEY_WOW64_64KEY,
            ):
                created.append(key_path)
        for suffix, name, value, value_type in (
            (crypto, "MachineGuid", guid, kind),
            (HOST_REGISTRY_KEY, HOST_REGISTRY_VALUE, json.dumps(registration), winreg.REG_SZ),
        ):
            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                registry_root + "\\" + suffix,
                0,
                winreg.KEY_SET_VALUE | winreg.KEY_WOW64_64KEY,
            ) as key:
                winreg.SetValueEx(key, name, 0, value_type, value)
        # Test-only native mapping must be applied explicitly in EVERY Python
        # descendant. Failure exits before SUT imports; no fallback to real HKLM.
        startup = (
            "import ctypes, json, os, uuid, winreg\nfrom pathlib import Path\n"
            "try:\n"
            "    import asyncio, socket, ssl\n"
            "    socket.getaddrinfo('localhost',0)\n"
            "    override=ctypes.WinDLL('advapi32').RegOverridePredefKey\n"
            "    override.argtypes=[ctypes.c_void_p,ctypes.c_void_p]\n"
            "    override.restype=ctypes.c_long\n"
            "    hklm=ctypes.c_void_p(ctypes.c_int32(int(winreg.HKEY_LOCAL_MACHINE)).value)\n"
            f"    with winreg.OpenKey(winreg.HKEY_CURRENT_USER,{registry_root!r},0,"
            "winreg.KEY_READ|winreg.KEY_WOW64_64KEY) as key:\n"
            "        assert override(hklm,ctypes.c_void_p(int(key))) == 0\n"
            f"    witness=Path({str(bootstrap)!r})/(\n"
            "        'pid-'+str(os.getpid())+'-'+uuid.uuid4().hex+'.json')\n"
            "    with witness.open('x',encoding='utf-8') as output:\n"
            "        json.dump({'pid':os.getpid(),'native_registry_override':True},output)\n"
            "except BaseException:\n"
            "    os._exit(91)\n"
        )
        # The development profile inspector runs with -I, so it never imports this
        # startup from PYTHONPATH (by design). Pass the same mapping to that child
        # explicitly by executing this file before its original script; the
        # inspector command, script and arguments are otherwise unchanged.
        boot = str(bootstrap / "sitecustomize.py")
        inspector_entry = (
            "import os,runpy,sys\n"
            f"exec(compile(open({boot!r},encoding='utf-8').read(),{boot!r},'exec'),"
            "{'__name__':'aits_native_bootstrap'})\n"
            "sys.argv=sys.argv[1:]\n"
            "sys.path.insert(0,os.path.dirname(sys.argv[0]))\n"
            "runpy.run_path(sys.argv[0],run_name='__main__')\n"
        )
        startup += (
            "import subprocess\n"
            "_aits_popen_init=subprocess.Popen.__init__\n"
            "def _aits_native_popen(self,args,*rest,**kwargs):\n"
            "    if (isinstance(args,list) and len(args)>2 and args[1]=='-I'\n"
            "            and '--inspect-full-publication-profile' in args\n"
            "            and '--protected-inspector' not in args):\n"
            f"        args=[args[0],'-I','-c',{inspector_entry!r},*args[2:]]\n"
            "    return _aits_popen_init(self,args,*rest,**kwargs)\n"
            "subprocess.Popen.__init__=_aits_native_popen\n"
        )
        (bootstrap / "sitecustomize.py").write_text(startup, encoding="utf-8")
        # The real publication launcher deliberately freezes PYTHONPATH to
        # candidate/src. Freeze this fixture-only bootstrap in that candidate,
        # rather than weakening the launcher's production environment binding.
        candidate_startup = root / "src/sitecustomize.py"
        assert not candidate_startup.exists()
        candidate_startup.parent.mkdir(parents=True, exist_ok=True)
        candidate_startup.write_bytes((bootstrap / "sitecustomize.py").read_bytes())
        with winreg.OpenKey(
            winreg.HKEY_CURRENT_USER, registry_root, 0, winreg.KEY_READ | winreg.KEY_WOW64_64KEY
        ) as isolated:
            assert override(hklm, ctypes.c_void_p(int(isolated))) == 0
        activated = True
        monkeypatch.setenv("PYTHONPATH", str(bootstrap) + os.pathsep + os.environ["PYTHONPATH"])
        # This pytest process never runs the startup file, so apply the same
        # explicit inspector mapping to profile inspections it launches itself.
        import ai_trading_system.platform.architecture.integration_publication_fence as fence_module

        original_inspector = fence_module._full_profile_inspector_command

        def native_inspector(*args):
            command = original_inspector(*args)
            if command[1] == "-I" and "--protected-inspector" not in command:
                return [command[0], "-I", "-c", inspector_entry, *command[2:]]
            return command

        monkeypatch.setattr(fence_module, "_full_profile_inspector_command", native_inspector)
        assert (
            CheckoutLeaseGuard(
                project_root=root,
                policy_path=root / "config/architecture/arch_005_s4d_checkout_guard.yaml",
                parallel_policy_path=root
                / "config/architecture/arch_005_parallel_control_policy.yaml",
            ).store.root
            == control
        )
        yield control
    finally:
        if activated:
            assert override(hklm, None) == 0
        if created:
            _remove_native_registry_fixture(winreg, registry_root)


@pytest.mark.parametrize(
    "canonical_merge_repository",
    ["native-full-profile-publish"],
    indirect=True,
)
def test_native_host_actual_full_and_publication(canonical_merge_repository) -> None:
    from test_arch_005_integration_publication_fence import _run_actual_publication_fixture

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )

    root, _scope = canonical_merge_repository
    _run_actual_publication_fixture(root, "normal")
    fence = IntegrationPublicationFence(project_root=root)
    assert fence.guard.store.root == root.parent / "native-host-control"
    leases = fence.guard.replay()
    assert leases.status == "PASS" and not leases.active_leases
    assert len([row for row in leases.lease_heads if row.execution is not None]) == 1
    assert len(list((root.parent / "native-host-bootstrap").glob("pid-*.json"))) >= 17


@pytest.mark.parametrize(
    "canonical_merge_repository",
    ["native-linked-full-profile-publish", "native-independent-full-profile-publish"],
    indirect=True,
)
def test_native_linked_contender_refused_during_actual_full(canonical_merge_repository):
    from ai_trading_system.platform.architecture.workflow_execution import observe_process

    root, _scope = canonical_merge_repository
    peer = root.parent / "native-full-contender"
    evidence = root.parent / "native-live-full-contention.json"
    from ai_trading_system.platform.architecture.workflow_coordination import (
        resolve_host_control_binding,
    )
    first = resolve_host_control_binding(root, entrypoint="checkout-guard")
    second = resolve_host_control_binding(peer, entrypoint="checkout-guard")
    scopes = [{kind: list(binding.scoped_paths(marker))
               for kind, marker in binding.resource_markers.items()}
              for binding in (first, second)]
    assert scopes[0]["full"] == scopes[1]["full"]
    if first.common_identity != second.common_identity:
        claimed = [set(row["full"] + row["publication"]) for row in scopes]
        assert claimed[0] & claimed[1] == {"host/" + first.host_id + "/full"}
    (root.parent / "native-contender-scopes.json").write_text(
        json.dumps({"scopes": scopes,
                    "independent": first.common_identity != second.common_identity}),
        encoding="utf-8",
    )

    def observer(process, fence, environment):
        started = time.monotonic()
        while True:
            assert process.poll() is None, "FULL_DRIVER_EXITED_BEFORE_LIVE_OBSERVATION"
            replay = fence.guard.replay()
            assert replay.status == "PASS"
            running = [row for row in replay.active_leases
                       if row.execution and row.execution["state"] == "RUNNING"]
            if running:
                break
            assert time.monotonic() - started < 180, "FULL_EXECUTOR_NOT_OBSERVED"
            time.sleep(0.25)
        assert len(running) == 1
        identity = running[0].execution["process"]
        assert observe_process(**identity)["state"] == "RUNNING"
        head = subprocess.check_output(
            ["git", "-C", str(peer), "rev-parse", "HEAD"], text=True,
        ).strip()
        before_refs = subprocess.check_output(["git", "-C", str(peer), "show-ref"])
        index = Path(subprocess.check_output(
            ["git", "-C", str(peer), "rev-parse", "--path-format=absolute", "--git-path", "index"],
            text=True,
        ).strip())
        before_index = index.read_bytes()
        cli = POLICY_PATH.parents[2] / "scripts/architecture_arch005_publication_fence.py"
        command = [sys.executable, str(cli),
                   "--repository", str(peer), "acquire", "--transaction-id", "live-full-loser",
                   "--task-id", "L01-live-full", "--change-id", "live-full-loser",
                   "--thread-id", "live-full-loser", "--frozen-base", head,
                   "--lane-head", head, "--expected-main", head]
        result = subprocess.run(command, cwd=peer, env=environment,
                                capture_output=True, text=True, timeout=30)
        after = observe_process(**identity)
        evidence.write_text(json.dumps({
            "executor": identity, "executor_after": after, "argv": command,
            "returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr,
            "observed_after_seconds": time.monotonic() - started,
        }), encoding="utf-8")
        assert after["state"] == "RUNNING"
        assert result.returncode == 2, result.stdout + result.stderr
        assert json.loads(result.stdout)["reason_code"] == "PUBLICATION_LEASE_CONFLICT"
        # The original guard records rejected requests. This is not a lease,
        # publication transaction, Full claim, or dispatched execution.
        files = {path.relative_to(peer).as_posix()
                 for path in (peer / "outputs").rglob("*") if path.is_file()}
        assert files == {
            "outputs/architecture/arch_005_s4d_checkout_guard/"
            "intents/publication-live-full-loser.json",
        }, files
        assert index.read_bytes() == before_index
        assert subprocess.check_output(["git", "-C", str(peer), "show-ref"]) == before_refs

    binding, directory, _script, _environment = _run_actual_profile_full(
        root, live_observer=observer,
    )
    assert evidence.is_file() and (directory / "test_runtime_summary.json").is_file()
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )
    fence = IntegrationPublicationFence(project_root=root)
    transaction = root / binding["transaction_path"]
    fence.release(transaction, actor="integration-coordinator", outcome="failed")
    assert not fence.guard.replay().active_leases


@pytest.mark.parametrize("linked_contender", [False, True, "independent"],
                         ids=["single", "linked", "independent"])
def test_native_bootstrap_preserves_python_windows_runtime(
    publication_checkout, monkeypatch, linked_contender,
):
    import shutil

    root = publication_checkout
    shutil.copytree(POLICY_PATH.parents[2] / "src/ai_trading_system",
                    root / "src/ai_trading_system")
    if linked_contender == "independent":
        subprocess.run(["git", "-C", str(root), "switch", "-c", "independent-source"],
                       check=True, capture_output=True)
    monkeypatch.setenv("PYTHONPATH", str(POLICY_PATH.parents[2] / "src"))
    with native_full_host_registration(
        root, monkeypatch, linked_contender=linked_contender is True,
        independent_contender=linked_contender == "independent",
    ) as control:
        command = (
            "import asyncio,json,pydantic,socket,ssl\nfrom pathlib import Path\n"
            "from ai_trading_system.platform.architecture.checkout_guard import (\n"
            "    CheckoutLeaseGuard)\n"
            "sock=socket.socket(); sock.close()\n"
            "assert socket.getaddrinfo('localhost',0)\n"
            "guard=CheckoutLeaseGuard(project_root=Path.cwd())\n"
            "print(json.dumps({'store':str(guard.store.root),'runtime':'PASS'}))\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", command], cwd=root, capture_output=True, text=True, timeout=30
        )
        (root.parent / "native-runtime-check.json").write_text(
            json.dumps(
                {
                    "returncode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                }
            ),
            encoding="utf-8",
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert json.loads(result.stdout) == {"store": str(control), "runtime": "PASS"}
        frozen = subprocess.run(
            [sys.executable, "-B", "-c",
             "import ai_trading_system\n"
             "from pathlib import Path\n"
             "assert Path(ai_trading_system.__file__).resolve().is_relative_to("
             "Path.cwd()/'src')\n" + command], cwd=root,
            env={**os.environ, "PYTHONPATH": str(root / "src")},
            capture_output=True, text=True, timeout=30,
        )
        (root.parent / "native-frozen-src-runtime-check.json").write_text(json.dumps({
            "returncode": frozen.returncode, "stdout": frozen.stdout, "stderr": frozen.stderr,
        }), encoding="utf-8")
        assert frozen.returncode == 0, frozen.stdout + frozen.stderr
        assert json.loads(frozen.stdout) == {"store": str(control), "runtime": "PASS"}
        if linked_contender:
            peer = root.parent / "native-full-contender"
            peer_result = subprocess.run(
                [sys.executable, "-c", command], cwd=peer,
                capture_output=True, text=True, timeout=30,
            )
            (root.parent / "native-linked-runtime-check.json").write_text(json.dumps({
                "returncode": peer_result.returncode, "stdout": peer_result.stdout,
                "stderr": peer_result.stderr, "checkout": str(peer),
            }), encoding="utf-8")
            assert peer_result.returncode == 0, peer_result.stdout + peer_result.stderr
            assert json.loads(peer_result.stdout) == {"store": str(control), "runtime": "PASS"}
            if linked_contender == "independent":
                from ai_trading_system.platform.architecture.workflow_coordination import (
                    resolve_host_control_binding,
                )
                first = resolve_host_control_binding(root, entrypoint="checkout-guard")
                second = resolve_host_control_binding(peer, entrypoint="checkout-guard")
                marker = first.resource_markers
                assert first.common_identity != second.common_identity
                assert first.scoped_paths(marker["full"]) == second.scoped_paths(marker["full"])
                assert set(first.scoped_paths(marker["publication"])).isdisjoint(
                    second.scoped_paths(marker["publication"]),
                )
        assert not (control / "events").exists()


@pytest.mark.parametrize(
    "canonical_merge_repository",
    ["source-job-created-directory-before-record", "source-job-created-directory-after-record"],
    indirect=True,
)
def test_public_recovery_restores_directory_creation_record_crashes(
    canonical_merge_repository, request
):
    from test_devx015_workflow_execution import (
        test_source_candidate_cli_uses_real_job_and_private_two_parent_commit,
    )

    mode = request.node.callspec.params["canonical_merge_repository"]
    test_source_candidate_cli_uses_real_job_and_private_two_parent_commit(
        canonical_merge_repository,
        installation_boundary=mode.removeprefix("source-job-"),
    )


def _anchor_control(repo: Path, control: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Mock only registry transport; real parser/Git/host/path checks still execute.

    This test seam does not claim Windows key ACL or administrator installation
    acceptance. Those require a separate privileged integration environment.
    """
    import winreg

    from ai_trading_system.platform.architecture.workflow_coordination import (
        CONTROL_BINDING_NAME,
        CONTROL_STATE_NAME,
        HOST_REGISTRY_KEY,
        HOST_REGISTRY_VALUE,
        directory_identity,
        machine_host_id,
    )

    common = repo / ".git"
    frozen = json.dumps(
        {
            "schema_version": "workflow_machine_registration.v1",
            "host_id": machine_host_id(),
            "control_root": control.as_posix(),
            "root_identity": directory_identity(control),
            "state_sha256": hashlib.sha256((control / CONTROL_STATE_NAME).read_bytes()).hexdigest(),
            "repositories": [
                {
                    "common_identity": directory_identity(common),
                    "checkout_identities": [directory_identity(repo)],
                    "locator_sha256": hashlib.sha256(
                        (common / CONTROL_BINDING_NAME).read_bytes()
                    ).hexdigest(),
                }
            ],
        }
    )
    real_open = winreg.OpenKey
    real_query = winreg.QueryValueEx

    class FixtureKey:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    key = FixtureKey()

    def open_key(hive, path, *args):
        if hive == winreg.HKEY_LOCAL_MACHINE and path == HOST_REGISTRY_KEY:
            return key
        return real_open(hive, path, *args)

    def query_value(handle, name):
        if handle is key and name == HOST_REGISTRY_VALUE:
            return frozen, winreg.REG_SZ
        return real_query(handle, name)

    monkeypatch.setattr(winreg, "OpenKey", open_key)
    monkeypatch.setattr(winreg, "QueryValueEx", query_value)


def _registered_control(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    repository=None,
    lease_policy=None,
):
    from ai_trading_system.platform.architecture.workflow_coordination import (
        CONTROL_BINDING_NAME,
        CONTROL_STATE_NAME,
        control_policy_sha256,
        directory_identity,
        machine_host_id,
    )

    repo = tmp_path / "repo" if repository is None else repository
    if repository is None:
        repo.mkdir()
        env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
        env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull, GIT_OPTIONAL_LOCKS="0")
        subprocess.run(["git", "init", str(repo)], check=True, capture_output=True, env=env)
    common = repo / ".git"
    control = tmp_path / "control"
    control.mkdir()
    policy = load_parallel_control_policy(POLICY_PATH) if lease_policy is None else lease_policy
    shared = {
        "host_id": machine_host_id(),
        "epoch": "epoch-1",
        "root_identity": directory_identity(control),
        "resource_markers": {"full": "validation/full", "publication": "publication/main"},
        "policy_sha256": control_policy_sha256(policy),
    }
    registration = {
        "registration_id": "repository-1",
        "common_root": common.as_posix(),
        "common_identity": directory_identity(common),
        "entrypoints": ["checkout-guard"],
    }
    state = {
        "schema_version": "workflow_host_control.v1",
        "phase": "ACTIVE",
        **shared,
        "registrations": [registration],
        "legacy_roots": [],
    }
    locator = {
        "schema_version": "workflow_host_binding.v1",
        **shared,
        "control_root": control.as_posix(),
        **{key: value for key, value in registration.items() if key != "entrypoints"},
    }
    (control / CONTROL_STATE_NAME).write_text(json.dumps(state), encoding="utf-8")
    (common / CONTROL_BINDING_NAME).write_text(json.dumps(locator), encoding="utf-8")
    _anchor_control(repo, control, monkeypatch)
    return repo, control, policy, state


def test_registered_control_pins_physical_scopes_and_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ai_trading_system.platform.architecture.workflow_coordination import (
        coordinated_lease_store,
    )

    repo, root, policy, _state = _registered_control(tmp_path, monkeypatch)
    store = coordinated_lease_store(
        repo, tmp_path / "unused", policy=policy, entrypoint="checkout-guard"
    )
    assert store.root == root
    with store.atomic(actor=ACTOR, now=datetime.now(UTC)):
        binding = store.coordination_binding
        assert binding.scoped_path("validation/full") == "host/" + binding.host_id + "/full"
        assert binding.scoped_path("publication/main").startswith("repository/")
        assert binding.scoped_path("src/example.py").startswith("checkout/")
    assert not (tmp_path / "unused").exists()


@pytest.mark.parametrize("mutation", ["markers", "policy", "epoch", "common_identity"])
def test_registered_writer_rejects_identity_drift_before_side_effect(
    tmp_path: Path,
    mutation: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from ai_trading_system.platform.architecture.workflow_coordination import (
        CONTROL_STATE_NAME,
        coordinated_lease_store,
    )

    repo, root, policy, state = _registered_control(tmp_path, monkeypatch)
    store = coordinated_lease_store(
        repo, tmp_path / "unused", policy=policy, entrypoint="checkout-guard"
    )
    if mutation == "markers":
        state["resource_markers"]["full"] = "validation/different"
    elif mutation == "policy":
        state["policy_sha256"] = "0" * 64
    elif mutation == "epoch":
        state["epoch"] = "epoch-2"
    else:
        state["registrations"][0]["common_identity"]["file_id"] += 1
    (root / CONTROL_STATE_NAME).write_text(json.dumps(state), encoding="utf-8")
    canary = tmp_path / "forbidden-write"
    with pytest.raises(ParallelControlError, match="HOST_REGISTRATION_CHANGED"):
        with store.atomic(actor=ACTOR, now=datetime.now(UTC)):
            canary.write_text("must not write")
    assert not canary.exists()


def test_registered_writer_rejects_caller_policy_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dataclasses import replace

    from ai_trading_system.platform.architecture.workflow_coordination import (
        coordinated_lease_store,
    )

    repo, _root, policy, _state = _registered_control(tmp_path, monkeypatch)
    changed = replace(policy, lease_ttl_seconds=policy.lease_ttl_seconds + 1)
    store = coordinated_lease_store(
        repo, tmp_path / "unused", policy=changed, entrypoint="checkout-guard"
    )
    with pytest.raises(ParallelControlError, match="STORE_POLICY_CHANGED"):
        with store.atomic(actor=ACTOR, now=datetime.now(UTC)):
            pytest.fail("changed policy entered writer body")


@pytest.mark.parametrize(
    "mutation", ["unknown_field", "bad_row", "duplicate", "entrypoint_type", "marker_type"]
)
def test_registered_state_is_strictly_typed(
    tmp_path: Path, mutation: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ai_trading_system.platform.architecture.workflow_coordination import (
        CONTROL_STATE_NAME,
        resolve_host_control_binding,
    )

    repo, root, _policy, state = _registered_control(tmp_path, monkeypatch)
    if mutation == "unknown_field":
        state["extra"] = True
    elif mutation == "bad_row":
        state["registrations"] = [42]
    elif mutation == "duplicate":
        state["registrations"] *= 2
    elif mutation == "entrypoint_type":
        state["registrations"][0]["entrypoints"] = "checkout-guard"
    else:
        state["resource_markers"] = []
    (root / CONTROL_STATE_NAME).write_text(json.dumps(state), encoding="utf-8")
    _anchor_control(repo, root, monkeypatch)
    with pytest.raises(ParallelControlError, match="STATE_|RESOURCE_MARKERS"):
        resolve_host_control_binding(repo, entrypoint="checkout-guard")


def test_registered_locator_rejects_duplicate_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ai_trading_system.platform.architecture.workflow_coordination import (
        CONTROL_BINDING_NAME,
        resolve_host_control_binding,
    )

    repo, _root, _policy, _state = _registered_control(tmp_path, monkeypatch)
    locator = repo / ".git" / CONTROL_BINDING_NAME
    value = locator.read_text(encoding="utf-8")
    locator.write_text('{"epoch":"epoch-1",' + value[1:], encoding="utf-8")
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        resolve_host_control_binding(repo, entrypoint="checkout-guard")


def test_unregistered_retirement_cannot_authorize_drain_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ai_trading_system.platform.architecture.workflow_coordination import (
        CONTROL_STATE_NAME,
        RETIREMENT_NAME,
        directory_identity,
    )

    _repo, root, policy, state = _registered_control(tmp_path, monkeypatch)
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    state["phase"] = "DRAINING"
    (root / CONTROL_STATE_NAME).write_text(json.dumps(state), encoding="utf-8")
    _anchor_control(_repo, root, monkeypatch)
    (legacy / RETIREMENT_NAME).write_text(
        json.dumps(
            {
                "schema_version": "workflow_legacy_retirement.v1",
                "root_identity": directory_identity(legacy),
                "control_root": root.as_posix(),
                "epoch": state["epoch"],
            }
        ),
        encoding="utf-8",
    )
    store = FileExecutionLeaseStore(legacy, policy=policy)
    with pytest.raises(ParallelControlError, match="RETIREMENT_NOT_REGISTERED"):
        with store.atomic(actor=ACTOR, now=datetime.now(UTC), operation="heartbeat"):
            pytest.fail("unregistered retirement allowed drain writer")


def test_retired_root_without_marker_cannot_reopen_legacy_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from ai_trading_system.platform.architecture.workflow_coordination import (
        CONTROL_STATE_NAME,
        directory_identity,
    )

    repo, root, policy, state = _registered_control(tmp_path, monkeypatch)
    legacy = tmp_path / "retired"
    legacy.mkdir()
    state["phase"] = "DRAINING"
    state["legacy_roots"] = [{"root_identity": directory_identity(legacy), "epoch": state["epoch"]}]
    (root / CONTROL_STATE_NAME).write_text(json.dumps(state), encoding="utf-8")
    _anchor_control(repo, root, monkeypatch)
    store = FileExecutionLeaseStore(legacy, policy=policy)
    with pytest.raises(ParallelControlError, match="LEGACY_FENCE_MISSING"):
        with store.atomic(actor=ACTOR, now=datetime.now(UTC)):
            pytest.fail("missing retirement marker reopened a registered legacy writer")
    assert not store.events_root.exists()


@pytest.mark.parametrize("entrypoint", ["atomic", "acquire"])
def test_registered_host_rejects_bare_alternate_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    entrypoint: str,
) -> None:
    _repo, _root, policy, _state = _registered_control(tmp_path, monkeypatch)
    store = FileExecutionLeaseStore(tmp_path / "second-authority", policy=policy)
    with pytest.raises(ParallelControlError, match="RETIREMENT_NOT_REGISTERED"):
        if entrypoint == "atomic":
            with store.atomic(actor=ACTOR, now=datetime.now(UTC)):
                pytest.fail("bare store bypassed registered host authority")
        else:
            task = _task("ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE")
            readiness = evaluate_task_readiness(
                task,
                dependencies=[],
                observed_statuses={},
                graph_report=validate_dependency_graph([task.task_id], []),
                current_base_commit=BASE_COMMIT,
                policy=policy,
            )
            store.acquire(
                task=task,
                readiness=readiness,
                lane_id="unregistered",
                actor=ACTOR,
                current_base_commit=BASE_COMMIT,
                now=datetime.now(UTC),
            )
    assert not store.root.exists(), "rejected request created an alternate arbiter root"


@pytest.mark.parametrize("declared", ["Validation/Full", "validation", "validation/full/child"])
def test_case_and_broad_reserved_claims_conflict_in_actual_lease_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    declared: str,
) -> None:
    from dataclasses import replace

    from ai_trading_system.platform.architecture.workflow_coordination import (
        coordinated_lease_store,
    )

    repo, _root, policy, _state = _registered_control(tmp_path, monkeypatch)
    store = coordinated_lease_store(
        repo, tmp_path / "unused", policy=policy, entrypoint="checkout-guard"
    )
    scopes = store.coordination_binding.scoped_paths(declared)
    expected_host = "host/" + store.coordination_binding.host_id + "/full"
    assert expected_host in scopes
    if declared != "Validation/Full":
        assert any(scope.startswith("checkout/") for scope in scopes)
    graph = validate_dependency_graph(
        [
            "ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE",
            "TRADING-2446_to_2448_RESEARCH_RESTART_R0_R2",
        ],
        [],
    )
    first = _task("ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE")
    second = _task("TRADING-2446_to_2448_RESEARCH_RESTART_R0_R2")
    first = replace(first, manifest=replace(first.manifest, owned_paths=(expected_host,)))
    second = replace(second, manifest=replace(second.manifest, owned_paths=scopes))
    results = []
    for task in (first, second):
        readiness = evaluate_task_readiness(
            task,
            dependencies=[],
            observed_statuses={},
            graph_report=graph,
            current_base_commit=BASE_COMMIT,
            policy=policy,
        )
        results.append(
            store.acquire(
                task=task,
                readiness=readiness,
                lane_id=task.task_id,
                actor=ACTOR,
                current_base_commit=BASE_COMMIT,
                now=datetime.now(UTC),
            )
        )
    assert [result.status for result in results] == ["ACTIVE", "BLOCKED"]
    assert len(store.replay().active_leases) == 1


def _cutover_lock_probe(root: Path, evidence: Path, label: str) -> dict:
    driver = evidence / "lock-probe.py"
    if not driver.exists():
        driver.write_text(
            "import json, os, sys\n"
            "from datetime import UTC, datetime\n"
            "from pathlib import Path\n"
            "from ai_trading_system.platform.architecture.lease_arbiter import hold_lease_arbiter\n"
            "from ai_trading_system.platform.architecture.parallel_control "
            "import ParallelControlError\n"
            "try:\n"
            " with hold_lease_arbiter(Path(sys.argv[1]), actor='engineering-agent', "
            "now=datetime.now(UTC), arbiter_ttl_seconds=60):\n"
            "  Path(sys.argv[2]).write_bytes(b'actual contender effect')\n"
            "  print(json.dumps({'pid':os.getpid(),'status':'ACQUIRED'}))\n"
            "except ParallelControlError as exc:\n"
            " print(json.dumps({'pid':os.getpid(),'status':exc.code}))\n"
            " sys.exit(2)\n",
            encoding="utf-8",
        )
    effect = evidence / (label + ".effect")
    command = [sys.executable, str(driver), str(root), str(effect)]
    result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    record = {
        "argv": command,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "effect": effect.exists(),
    }
    (evidence / (label + ".json")).write_text(json.dumps(record), encoding="utf-8")
    assert result.returncode in {0, 2}, record
    return {**record, **json.loads(result.stdout)}


@pytest.mark.parametrize(
    "fault", ["none", "body_exception", "old_active", "new_active", "busy_second"]
)
def test_host_cutover_holds_actual_old_new_arbiters_and_unwinds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    from ai_trading_system.platform.architecture.lease_arbiter import hold_lease_arbiter
    from ai_trading_system.platform.architecture.workflow_coordination import (
        CONTROL_STATE_NAME,
        RETIREMENT_NAME,
        coordinated_lease_store,
        directory_identity,
        host_cutover_custody,
    )

    old, lease, _request, _environment, then = _case(tmp_path / "legacy")
    if fault != "old_active":
        old.release(lease.lease_id, actor=ACTOR, now=then, evidence_refs=())
    repo, control, policy, state = _registered_control(tmp_path, monkeypatch)
    if fault == "new_active":
        new = coordinated_lease_store(
            repo, tmp_path / "unused", policy=policy, entrypoint="checkout-guard"
        )
        task = _task("ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE")
        readiness = evaluate_task_readiness(
            task,
            dependencies=[],
            observed_statuses={},
            graph_report=validate_dependency_graph([task.task_id], []),
            current_base_commit=BASE_COMMIT,
            policy=policy,
        )
        new.acquire(
            task=task,
            readiness=readiness,
            lane_id="new-lane",
            actor=ACTOR,
            current_base_commit=BASE_COMMIT,
            now=datetime.now(UTC),
        )
    state["phase"] = "DRAINING"
    state["legacy_roots"] = [
        {"root_identity": directory_identity(old.root), "epoch": state["epoch"]}
    ]
    (control / CONTROL_STATE_NAME).write_text(json.dumps(state), encoding="utf-8")
    (old.root / RETIREMENT_NAME).write_text(
        json.dumps(
            {
                "schema_version": "workflow_legacy_retirement.v1",
                "root_identity": directory_identity(old.root),
                "epoch": state["epoch"],
                "control_root": control.as_posix(),
            }
        ),
        encoding="utf-8",
    )
    _anchor_control(repo, control, monkeypatch)
    roots = sorted(
        [control, old.root],
        key=lambda root: (
            directory_identity(root)["device"],
            directory_identity(root)["file_id"],
        ),
    )
    frozen = {
        path: path.read_bytes()
        for root in roots
        for path in root.rglob("*")
        if path.is_file() and path.name not in {"arbiter.lock", "arbiter.owner.json"}
    }
    custody = None
    with ExitStack() as stack:
        if fault == "busy_second":
            stack.enter_context(
                hold_lease_arbiter(
                    roots[1],
                    actor=ACTOR,
                    now=datetime.now(UTC),
                    arbiter_ttl_seconds=60,
                )
            )
        if fault in {"old_active", "new_active", "busy_second"}:
            expected = "LEASE_ARBITER_BUSY" if fault == "busy_second" else "CUTOVER_DRAIN_REQUIRED"
            with pytest.raises(ParallelControlError, match=expected):
                with host_cutover_custody(repo, policy=policy, actor=ACTOR):
                    pytest.fail("undrained or contended cutover was admitted")
        else:
            try:
                with host_cutover_custody(repo, policy=policy, actor=ACTOR) as custody:
                    custody.assert_current()
                    assert [row["root_identity"]["path"] for row in custody.lease_snapshots] == [
                        root.as_posix() for root in roots
                    ]
                    for index, root in enumerate(roots):
                        observed = _cutover_lock_probe(root, tmp_path, f"held-{index}")
                        assert observed["status"] == "LEASE_ARBITER_BUSY"
                        assert observed["returncode"] == 2 and not observed["effect"]
                    if fault == "body_exception":
                        raise RuntimeError("test cutover interrupted")
            except RuntimeError as exc:
                assert fault == "body_exception" and str(exc) == "test cutover interrupted"
        if fault == "busy_second":
            assert _cutover_lock_probe(roots[0], tmp_path, "unwound-first")["status"] == "ACQUIRED"
            assert (
                _cutover_lock_probe(roots[1], tmp_path, "still-owned-second")["status"]
                == "LEASE_ARBITER_BUSY"
            )
    for index, root in enumerate(roots):
        observed = _cutover_lock_probe(root, tmp_path, f"released-{index}")
        assert observed["status"] == "ACQUIRED" and observed["effect"]
    if custody is not None:
        with pytest.raises(ParallelControlError):
            custody.assert_current()
    assert {
        path: path.read_bytes()
        for root in roots
        for path in root.rglob("*")
        if path.is_file() and path.name not in {"arbiter.lock", "arbiter.owner.json"}
    } == frozen
    assert old.replay().lease_heads[0].state == ("ACTIVE" if fault == "old_active" else "RELEASED")


@pytest.mark.parametrize("layout", ["direct", "installation", "publication", "nested"])
def test_cutover_os_observation_rejects_dead_parent_with_live_job_child(
    tmp_path: Path, layout: str,
) -> None:
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _cutover_execution_observations,
    )

    identity_code = (
        "import json,time; from pathlib import Path; "
        "from ai_trading_system.platform.architecture.workflow_execution "
        "import current_process_identity; "
    )
    child = (
        identity_code + "Path('child.ready').write_text(json.dumps(current_process_identity())); "
        "exec(\"while not Path('child.release').exists():\\n time.sleep(.02)\")"
    )
    parent = (
        identity_code + "import subprocess,sys; "
        "Path('parent.ready').write_text(json.dumps(current_process_identity())); "
        "subprocess.Popen([sys.executable,'-c'," + repr(child) + "]); "
        "exec(\"while not Path('parent.release').exists():\\n time.sleep(.02)\")"
    )
    job_name = "Local\\AITS-DEVX015-cutover-" + uuid.uuid4().hex
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    with WindowsJobProcess.create(
        argv=[sys.executable, "-c", parent],
        cwd=tmp_path,
        environment=environment,
        stdout_path=tmp_path / "job.log",
        job_name=job_name,
    ) as handle:
        handle.resume()
        _until(
            lambda: _read_json(tmp_path / "parent.ready") and _read_json(tmp_path / "child.ready"),
            description="actual Python parent and child identity witnesses",
        )
        parent_identity = json.loads((tmp_path / "parent.ready").read_text())
        child_identity = json.loads((tmp_path / "child.ready").read_text())
        execution = {
            "request": {"job_name": job_name, "request_id": "actual-parent-child"},
            "process": parent_identity,
            "installation_attempts": [],
        }
        layers = {
            "direct": (), "installation": ("installation_attempts",),
            "publication": ("publication_attempts",),
            "nested": ("publication_attempts", "installation_attempts"),
        }[layout]
        for number, collection in enumerate(reversed(layers)):
            execution = {
                "request": {"job_name": job_name + f"-empty-{number}",
                            "request_id": f"outer-{number}"},
                "process": None,
                collection: [execution],
            }
        oracle = NativeOracle()
        with (
            oracle.process(parent_identity["pid"]) as native,
            oracle.process(child_identity["pid"]) as child_native,
        ):
            oracle.assert_in_job(native, job_name)
            oracle.assert_in_job(child_native, job_name)
            (tmp_path / "parent.release").write_bytes(b"release")
            _until(
                lambda: oracle.exited(native),
                description="original parent exited while its contained child remains alive",
            )
            # Venv launchers may add intermediates. The independently held child
            # handle and native membership, not an assumed count, prove liveness.
            assert not oracle.exited(child_native)
            assert handle.active_process_count() >= 1
            with pytest.raises(ParallelControlError, match="CUTOVER_EXECUTOR_NOT_DRAINED"):
                _cutover_execution_observations(execution)
            (tmp_path / "child.release").write_bytes(b"release")
            assert handle.wait(timeout=20) == 0
            assert oracle.exited(child_native)
            observed = _cutover_execution_observations(execution)
            assert observed[-1]["job"]["state"] == "EMPTY"
            assert observed[-1]["process"]["state"] == "EXITED"
            (tmp_path / "os-observation.json").write_text(json.dumps(observed), encoding="utf-8")


@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "unregistered",
        "retirement_missing",
        "ledger_corrupt",
        "active_not_terminal",
        "root_replaced",
        "intent_modified",
    ],
)
def test_terminal_publication_replays_registered_legacy_origin_without_copying_leases(
    publication_checkout: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    from test_arch_005_integration_publication_fence import _acquire, _fence

    from ai_trading_system.platform.architecture.checkout_guard import CheckoutGuardError
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        PublicationFenceError,
    )
    from ai_trading_system.platform.architecture.workflow_coordination import (
        CONTROL_BINDING_NAME,
        CONTROL_STATE_NAME,
        RETIREMENT_NAME,
        control_policy_sha256,
        directory_identity,
    )

    fence = _fence(publication_checkout)
    acquired = _acquire(fence, publication_checkout, transaction_id="terminal-before-host-cutover")
    transaction = publication_checkout / str(acquired["transaction_path"])
    old = fence.guard.store
    receipt = None
    if fault != "active_not_terminal":
        receipt = fence.release(transaction, actor="integration-coordinator", outcome="failed")
        assert receipt["outcome"] == "FAILED" and receipt["lease_state"] == "RELEASED"
    template, control, _policy, state = _registered_control(tmp_path, monkeypatch)
    common = publication_checkout / ".git"
    common_identity = directory_identity(common)
    policy_digest = control_policy_sha256(fence.guard.lease_policy)
    state["policy_sha256"] = policy_digest
    state["registrations"][0]["common_root"] = common.as_posix()
    state["registrations"][0]["common_identity"] = common_identity
    state["legacy_roots"] = (
        []
        if fault == "unregistered"
        else [{"root_identity": directory_identity(old.root), "epoch": state["epoch"]}]
    )
    locator = json.loads((template / ".git" / CONTROL_BINDING_NAME).read_text())
    locator.update(
        common_root=common.as_posix(),
        common_identity=common_identity,
        policy_sha256=policy_digest,
    )
    (common / CONTROL_BINDING_NAME).write_text(json.dumps(locator), encoding="utf-8")
    (control / CONTROL_STATE_NAME).write_text(json.dumps(state), encoding="utf-8")
    if fault != "retirement_missing":
        (old.root / RETIREMENT_NAME).write_text(
            json.dumps(
                {
                    "schema_version": "workflow_legacy_retirement.v1",
                    "epoch": state["epoch"],
                    "control_root": control.as_posix(),
                    "root_identity": directory_identity(old.root),
                }
            ),
            encoding="utf-8",
        )
    _anchor_control(publication_checkout, control, monkeypatch)
    if fault == "ledger_corrupt":
        damaged = sorted(old.events_root.glob("*/*.json"))[0]
        (tmp_path / "original-event.json").write_bytes(damaged.read_bytes())
        damaged.write_bytes(b"{invalid event\n")
    elif fault == "root_replaced":
        old.root.rename(old.root.with_name("original-leases"))
        old.root.mkdir()
    elif fault == "intent_modified":
        intent_path = Path(json.loads(transaction.read_text())["checkout_intent_path"])
        original_intent = intent_path.read_bytes()
        (tmp_path / "original-intent.json").write_bytes(original_intent)
        intent_payload = json.loads(original_intent)
        intent_payload["owned_paths"] = ["src/other.py"]
        intent_path.write_text(json.dumps(intent_payload), encoding="utf-8")
    frozen = {
        path: path.read_bytes()
        for root in (old.root, transaction.parent)
        for path in root.rglob("*")
        if path.is_file()
    }
    current = _fence(publication_checkout)
    assert current.guard.store.root == control
    assert current.guard.replay().lease_heads == ()
    if fault == "active_not_terminal":
        with pytest.raises(CheckoutGuardError, match="CHECKOUT_LEASE_INTENT_BINDING"):
            current.guard.require_mutation_lease(old.replay().lease_heads[0])
    control_before = (control / CONTROL_STATE_NAME).read_bytes()
    locator_before = (common / CONTROL_BINDING_NAME).read_bytes()
    intent_path = Path(json.loads(transaction.read_text())["checkout_intent_path"])
    intent_before = intent_path.read_bytes()
    if fault == "none":
        replayed = current.release(transaction, actor="integration-coordinator", outcome="failed")
        assert replayed == receipt
        (tmp_path / "replayed-receipt.json").write_text(json.dumps(replayed), encoding="utf-8")
    else:
        with pytest.raises(PublicationFenceError) as rejected:
            current.release(transaction, actor="integration-coordinator", outcome="failed")
        (tmp_path / "refusal.json").write_text(
            json.dumps({"code": rejected.value.code}), encoding="utf-8"
        )
    assert current.guard.replay().lease_heads == ()
    assert {
        path: path.read_bytes()
        for root in (old.root, transaction.parent)
        for path in root.rglob("*")
        if path.is_file()
    } == frozen
    assert (control / CONTROL_STATE_NAME).read_bytes() == control_before
    assert (common / CONTROL_BINDING_NAME).read_bytes() == locator_before
    assert intent_path.read_bytes() == intent_before


@pytest.mark.parametrize(
    "profile", ["host_exact", "host_parent", "wrong_marker", "unmanaged_legacy"]
)
def test_public_full_admission_requires_the_actual_host_write_slot(
    publication_checkout: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
    profile: str,
) -> None:
    from test_arch_005_integration_publication_fence import TASK_ID, _acquire, _fence, _git

    from ai_trading_system.platform.architecture.workflow_coordination import (
        CONTROL_BINDING_NAME,
        CONTROL_STATE_NAME,
        machine_host_id,
    )
    from scripts import architecture_arch005_publication_fence as cli

    root = publication_checkout
    fence = _fence(root)
    if profile != "unmanaged_legacy":
        _repo, control, _policy, state = _registered_control(
            tmp_path,
            monkeypatch,
            repository=root,
            lease_policy=fence.guard.lease_policy,
        )
        state["resource_markers"] = {
            "full": {
                "host_exact": fence.policy.exclusive_validation_resource,
                "host_parent": "outputs",
                "wrong_marker": "validation/full",
            }[profile],
            "publication": fence.policy.exclusive_publication_resource,
        }
        locator_path = root / ".git" / CONTROL_BINDING_NAME
        locator = json.loads(locator_path.read_text())
        locator["resource_markers"] = state["resource_markers"]
        locator_path.write_text(json.dumps(locator), encoding="utf-8")
        (control / CONTROL_STATE_NAME).write_text(json.dumps(state), encoding="utf-8")
        _anchor_control(root, control, monkeypatch)
        fence = _fence(root)
    acquired = _acquire(fence, root, transaction_id="actual-host-full-admission")
    transaction = root / str(acquired["transaction_path"])
    for phase in (
        "TASK_SOURCE_PRE_WRITE",
        "GENERATED_REBUILD_PRE",
        "GENERATED_REBUILD_POST",
        "CANDIDATE_COMMIT_PRE",
        "FORMAL_VALIDATION_PRE",
    ):
        fence.checkpoint(
            transaction,
            phase=phase,
            actor="integration-coordinator",
            generator_ids=("canonical-task-source",) if phase.startswith("GENERATED_") else (),
        )
    lease = fence.guard.replay().active_leases[0]
    host_slot = "host/" + machine_host_id() + "/full"
    write_paths = {
        r.resource_id for r in lease.resources if r.kind == "path" and r.access == "WRITE"
    }
    assert (host_slot in write_paths) == (profile in {"host_exact", "host_parent"})
    snapshots = {
        path: path.read_bytes()
        for directory in (fence.guard.store.root, transaction.parent)
        for path in directory.rglob("*")
        if path.is_file()
    }
    index_before = (root / ".git/index").read_bytes()
    refs_before = _git(root, "for-each-ref", "--format=%(refname) %(objectname)")
    argv = [
        str(Path(cli.__file__)),
        "--repository",
        str(root),
        "validate",
        "--transaction",
        str(transaction),
        "--exact-phase",
        "FORMAL_VALIDATION_PRE",
        "--task-id",
        TASK_ID,
        "--validation-tier",
        "full",
        "--require-candidate",
    ]
    capsys.readouterr()
    with monkeypatch.context() as invocation:
        invocation.setattr(sys, "argv", argv)
        code = cli.main()
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    (tmp_path / "original-full-admission.json").write_text(
        json.dumps(
            {
                "argv": argv,
                "returncode": code,
                "stdout": captured.out,
                "stderr": captured.err,
                "write_paths": sorted(write_paths),
                "expected_host_slot": host_slot,
            }
        ),
        encoding="utf-8",
    )
    assert not captured.err
    if profile == "wrong_marker":
        assert code == 2 and payload["reason_code"] in {
            "PUBLICATION_FULL_RESOURCE_MISSING",
            "PUBLICATION_FULL_HOST_SCOPE_MISSING",
        }
    else:
        assert code == 0, payload
        assert payload["lease_id"] == lease.lease_id
        assert payload["candidate_sha"] == _git(root, "rev-parse", "HEAD")
        assert payload["validation_only"] is True and payload["publication_allowed"] is False
    assert {
        path: path.read_bytes()
        for directory in (fence.guard.store.root, transaction.parent)
        for path in directory.rglob("*")
        if path.is_file()
    } == snapshots
    assert (root / ".git/index").read_bytes() == index_before
    assert _git(root, "for-each-ref", "--format=%(refname) %(objectname)") == refs_before
    assert fence.replay(transaction).phase == "FORMAL_VALIDATION_PRE"


@pytest.mark.parametrize("command", ["control-enroll", "control-enroll-recover"])
def test_public_enrollment_requires_actual_administrator_before_target_access(tmp_path, command):
    source = Path(__file__).resolve().parents[1]
    target = tmp_path / "not-created"
    flag = "--plan" if command == "control-enroll" else "--control-root"
    result = subprocess.run(
        [
            sys.executable,
            str(source / "scripts/architecture_arch005_workflow.py"),
            command,
            "--lease-policy",
            str(POLICY_PATH),
            "--actor",
            ACTOR,
            flag,
            str(target),
        ],
        cwd=source,
        env={**os.environ, "PYTHONPATH": str(source / "src")},
        capture_output=True,
        text=True,
        timeout=30,
    )
    # This host's actual medium token must fail before touching the nonexistent
    # requested plan/root. Positive privileged installation requires its own host.
    assert result.returncode != 0 and "ADMINISTRATOR_REQUIRED" in result.stderr
    assert not list(tmp_path.iterdir())


@pytest.fixture
def confidential_native_reader():
    """Only elevation construction is bypassed; Win32 ACL/handle calls are real."""
    import ctypes as c
    from ctypes import wintypes as w

    from ai_trading_system.platform.architecture.workflow_coordination import (
        _WindowsEnrollmentAdministrator,
    )

    reader = object.__new__(_WindowsEnrollmentAdministrator)
    reader.c, reader.w = c, w
    reader.kernel = c.WinDLL("kernel32", use_last_error=True)
    reader.advapi = c.WinDLL("advapi32", use_last_error=True)
    reader._bind(reader.kernel, "CloseHandle", [w.HANDLE], w.BOOL)
    reader._bind(reader.kernel, "LocalFree", [c.c_void_p], c.c_void_p)
    return reader


def test_confidential_descriptor_excludes_ordinary_readers(confidential_native_reader):
    reader = confidential_native_reader
    c, w = reader.c, reader.w
    convert = reader._bind(
        reader.advapi, "ConvertSecurityDescriptorToStringSecurityDescriptorW",
        [c.c_void_p, w.DWORD, w.DWORD, c.POINTER(w.LPWSTR), c.c_void_p], w.BOOL,
    )
    with reader._security(confidential=True) as attributes:
        value = w.LPWSTR()
        assert convert(attributes.descriptor, 1, 7, c.byref(value), None)
        try:
            assert value.value == "O:BAG:BAD:P(A;;FA;;;SY)(A;;FA;;;BA)"
        finally:
            reader.kernel.LocalFree(c.cast(value, c.c_void_p))
    for arguments in ({"registry": True}, {"worker_sid": "S-1-5-21-1-2-3-1010"},
                      {"exchange_role": "result"}):
        with pytest.raises(ParallelControlError, match="CONFIDENTIAL_SECURITY_CONTEXT"):
            with reader._security(confidential=True, **arguments):
                pytest.fail("secret ACL must not combine with registry/exchange grants")


def test_confidential_reader_refuses_ordinary_acl_before_content(
    confidential_native_reader, tmp_path, monkeypatch,
):
    from ai_trading_system.platform.architecture import workflow_contract

    path = tmp_path / "not-secret-envelope"
    path.write_bytes(b"synthetic-only")
    monkeypatch.setattr(workflow_contract, "bounded_regular_bytes",
                        lambda *args, **kwargs: pytest.fail("content read before secret ACL"))
    with pytest.raises(ParallelControlError, match="EXCHANGE_SECURITY_CHANGED"):
        with confidential_native_reader.hold_confidential_file(path):
            pytest.fail("ordinary user file is not a secret store")


@pytest.mark.parametrize("caller_fails", [False, True])
def test_confidential_file_retains_native_hold_and_releases_on_exception(
    confidential_native_reader, tmp_path, caller_fails,
):
    reader = confidential_native_reader
    observed = []
    # Explicit ACL seams permit an ordinary test fixture; native custody remains real.
    reader._assert_exact_security = lambda path, descriptor: observed.append(path)
    reader.assert_protected = lambda *args, **kwargs: None
    path = tmp_path / "synthetic-envelope"
    raw = b"synthetic-encrypted-payload"
    path.write_bytes(raw)

    def consume():
        with reader.hold_confidential_file(path) as captured:
            assert captured == raw
            with pytest.raises(OSError):
                path.write_bytes(b"must-not-write")
            if caller_fails:
                raise RuntimeError("synthetic caller failure")

    if caller_fails:
        with pytest.raises(RuntimeError, match="synthetic caller failure"):
            consume()
    else:
        consume()
    assert observed == [path, path, path]
    path.write_bytes(b"released")
    assert path.read_bytes() == b"released"


@pytest.mark.parametrize("wrong_digest", [False, True])
@pytest.mark.parametrize("linked", [False, True])
@pytest.mark.parametrize("file_size", [8, 16 * 1024 * 1024 + 1])
def test_protected_file_transport_retains_native_denial_until_exit(
    tmp_path, wrong_digest, linked, file_size,
):
    import ctypes as c
    from ctypes import wintypes as w

    from ai_trading_system.platform.architecture.workflow_coordination import (
        _WindowsEnrollmentAdministrator,
    )

    # Ordinary synthetic files: only ACL admission is a unit seam. Actual file
    # and ancestor handles remain native; no administrative mutation occurs.
    reader = object.__new__(_WindowsEnrollmentAdministrator)
    reader.c, reader.w = c, w
    reader.kernel = c.WinDLL("kernel32", use_last_error=True)
    reader._bind(reader.kernel, "CloseHandle", [w.HANDLE], w.BOOL)
    observed = []
    reader.assert_protected = lambda path, **kwargs: observed.append(path)
    target = tmp_path / "frozen-config"
    original = b"original" + b"x" * (file_size - 8)
    target.write_bytes(original)
    sibling = tmp_path / "second-config"
    sibling.write_bytes(b"second")
    alias = tmp_path / "installed-alias"
    identities = {}
    if linked:
        os.link(target, alias)
        info = target.stat()
        identities[target] = (info.st_dev, info.st_ino, info.st_nlink)
        with pytest.raises(ParallelControlError, match="PROTECTED_FILES_IDENTITY_CHANGED"):
            with reader.hold_protected_files(
                {target: hashlib.sha256(original).hexdigest()},
                expected_file_identities={target: (info.st_dev, info.st_ino, 1)},
            ):
                pytest.fail("wrong link count must not admit caller")
    digest = "0" * 64 if wrong_digest else hashlib.sha256(original).hexdigest()
    if wrong_digest:
        with pytest.raises(ParallelControlError, match="PROTECTED_FILES_CHANGED"):
            with reader.hold_protected_files({target: digest}, expected_file_identities=identities):
                pytest.fail("wrong digest must never admit caller")
    else:
        with reader.hold_protected_files(
            {target: digest, sibling: hashlib.sha256(b"second").hexdigest()},
            expected_file_identities=identities,
        ):
            for action in (lambda: target.write_bytes(b"changed"), target.unlink,
                           lambda: target.rename(tmp_path / "replaced")):
                with pytest.raises(OSError):
                    action()
            assert target.read_bytes() == original
            with pytest.raises(OSError):
                sibling.write_bytes(b"changed")
            with pytest.raises(OSError):
                tmp_path.rename(tmp_path.with_name(tmp_path.name + "-moved"))
            if linked:
                with pytest.raises(OSError):
                    alias.write_bytes(b"alias must not bypass held inode")
        assert target in observed and tmp_path in observed
    target.write_bytes(b"released")
    assert target.read_bytes() == b"released"
    sibling.write_bytes(b"released sibling")
    # A new interval must inspect fresh bytes, never reuse a prior file result.
    with pytest.raises(ParallelControlError, match="PROTECTED_FILES_CHANGED"):
        with reader.hold_protected_files({sibling: hashlib.sha256(b"second").hexdigest()}):
            pytest.fail("stale bytes admitted in a later interval")


def test_protected_runtime_file_above_native_limit_rejected(tmp_path):
    import ctypes as c
    from ctypes import wintypes as w

    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _WindowsEnrollmentAdministrator,
    )

    reader = object.__new__(_WindowsEnrollmentAdministrator)
    reader.c, reader.w = c, w
    reader.kernel = c.WinDLL("kernel32", use_last_error=True)
    reader._bind(reader.kernel, "CloseHandle", [w.HANDLE], w.BOOL)
    reader.assert_protected = lambda path, **kwargs: None  # Only ACL is a test seam.
    target = tmp_path / "oversized-runtime"
    with target.open("wb") as stream:
        stream.seek(64 * 1024 * 1024)
        stream.write(b"x")
    with pytest.raises(WorkflowContractError, match="ARTIFACT_BUDGET"):
        with reader.hold_protected_files({target: "0" * 64}):
            pytest.fail("native byte limit must reject before entering caller")


def test_enrollment_native_security_descriptor_and_directory_pins_are_real(tmp_path):
    import ctypes as c
    from ctypes import wintypes as w

    from ai_trading_system.platform.architecture.workflow_coordination import (
        _WindowsEnrollmentAdministrator,
    )

    # Read-only descriptor conversion and directory handles need no elevation.
    # Do not invoke the adapter's privileged creation/registration methods.
    reader = object.__new__(_WindowsEnrollmentAdministrator)
    reader.c, reader.w = c, w
    reader.kernel = c.WinDLL("kernel32", use_last_error=True)
    reader.advapi = c.WinDLL("advapi32", use_last_error=True)
    reader._bind(reader.kernel, "LocalFree", [c.c_void_p], c.c_void_p)
    reader._bind(reader.kernel, "CloseHandle", [w.HANDLE], w.BOOL)
    get_control = reader._bind(
        reader.advapi,
        "GetSecurityDescriptorControl",
        [c.c_void_p, c.POINTER(c.c_ushort), c.POINTER(w.DWORD)],
        w.BOOL,
    )
    for registry in (False, True):
        with reader._security(registry=registry) as attributes:
            control, revision = c.c_ushort(), w.DWORD()
            assert get_control(attributes.descriptor, c.byref(control), c.byref(revision))
            assert control.value & 0x1000 and not attributes.inherit
    get_dacl = reader._bind(
        reader.advapi, "GetSecurityDescriptorDacl",
        [c.c_void_p, c.POINTER(w.BOOL), c.POINTER(c.c_void_p), c.POINTER(w.BOOL)], w.BOOL,
    )
    get_ace = reader._bind(
        reader.advapi, "GetAce", [c.c_void_p, w.DWORD, c.POINTER(c.c_void_p)], w.BOOL,
    )
    for role in ("root", "result", "profile"):
        with reader._security(worker_sid="S-1-5-21-1-2-3-1001", exchange_role=role) as attributes:
            control, revision = c.c_ushort(), w.DWORD()
            assert get_control(attributes.descriptor, c.byref(control), c.byref(revision))
            assert control.value & 0x1000 and not attributes.inherit
            present, defaulted, dacl = w.BOOL(), w.BOOL(), c.c_void_p()
            assert get_dacl(attributes.descriptor, c.byref(present), c.byref(dacl),
                            c.byref(defaulted))
            assert present.value and dacl.value
            ace = c.c_void_p()
            assert get_ace(dacl, 2, c.byref(ace))
            mask = c.c_uint32.from_address(ace.value + 4).value
            assert not mask & (0x10000 | 0x40000 | 0x80000)  # DELETE / DAC / owner
            if role == "root":
                assert not mask & (0x2 | 0x4 | 0x40 | 0x100)
            elif role == "result":
                assert mask & 0x2  # The reserved file can be filled in place.
            else:
                assert mask & 0x2 and not mask & (0x40 | 0x100)
                assert get_ace(dacl, 3, c.byref(ace))
                assert c.c_ubyte.from_address(ace.value + 1).value & 0x08  # inherit-only
                assert c.c_uint32.from_address(ace.value + 4).value & 0x10000
    for sid, role in (("S-1-5-18", "root"), ("S-1-5-21-1-2-3-1001", "unknown")):
        with pytest.raises(ParallelControlError, match="EXCHANGE"):
            with reader._security(worker_sid=sid, exchange_role=role):
                pytest.fail("invalid exchange descriptor accepted")
    with pytest.raises(ParallelControlError, match="ADMIN_OBJECT_NOT_PROTECTED"):
        reader.assert_protected(tmp_path)
    reader.assert_protected(Path(os.environ["ProgramData"]), parent=True)
    reader.assert_protected(r"MACHINE\SOFTWARE", registry=True, allow_inheritance=True)
    root = tmp_path / "pinned-directory"
    root.mkdir()
    moved = tmp_path / "moved-directory"
    with reader.pin_directories([root]):
        with pytest.raises(PermissionError):
            root.rename(moved)
        assert root.is_dir() and not moved.exists()
    root.rename(moved)
    moved.rename(root)


@pytest.mark.parametrize("phase", ["DRAINING", "LEGACY_WRITERS_DISABLED"])
@pytest.mark.parametrize(
    "fault", ["none", "corrupt", "after-journal", "after-state", "after-registration"]
)
def test_cutover_journal_preparation_binds_live_bytes(tmp_path, monkeypatch, phase, fault):
    import ctypes as c
    from ctypes import wintypes as w

    from ai_trading_system.platform.architecture import workflow_coordination as coordination

    repo, control, policy, state = _registered_control(tmp_path, monkeypatch)
    state["phase"] = phase
    old = tmp_path / "legacy"
    old.mkdir()
    state["legacy_roots"] = [
        {"root_identity": coordination.directory_identity(old), "epoch": state["epoch"]}
    ]
    (old / coordination.RETIREMENT_NAME).write_text(json.dumps({
        "schema_version": "workflow_legacy_retirement.v1",
        "root_identity": coordination.directory_identity(old), "epoch": state["epoch"],
        "control_root": control.as_posix(),
    }), encoding="utf-8")
    state_path = control / coordination.CONTROL_STATE_NAME
    # Preserve distinct original whitespace, rather than silently normalizing it.
    before = (json.dumps(state, indent=2) + "\n\n").encode("utf-8")
    state_path.write_bytes(before)
    _anchor_control(repo, control, monkeypatch)
    registration = coordination._trusted_host_registration_bytes()
    admin = object.__new__(coordination._WindowsEnrollmentAdministrator)
    admin.c, admin.w = c, w
    admin.kernel = c.WinDLL("kernel32", use_last_error=True)
    admin._bind(admin.kernel, "CloseHandle", [w.HANDLE], w.BOOL)

    @contextmanager
    def security():
        class Attributes(c.Structure):
            _fields_ = [("size", w.DWORD), ("descriptor", c.c_void_p), ("inherit", w.BOOL)]
        yield Attributes(c.sizeof(Attributes), None, False)

    @contextmanager
    def protected(_files, *, protected_directories):
        assert protected_directories == (control,)
        yield  # ACL/transport seam only; native file write and journal parser are real.

    monkeypatch.setattr(admin, "_security", security)
    monkeypatch.setattr(admin, "assert_protected", lambda _path, **_kwargs: None)
    monkeypatch.setattr(admin, "hold_protected_files", protected)
    with coordination.host_cutover_custody(repo, policy=policy, actor=ACTOR) as custody:
        receipt = admin._prepare_cutover_journal(custody)
        path = Path(receipt["path"])
        raw, identity = path.read_bytes(), path.stat().st_ino
        assert not receipt["activation_allowed"]
        assert hashlib.sha256(raw).hexdigest() == receipt["sha256"]
        payload = json.loads(raw)
        assert bytes.fromhex(payload["before_state"]) == before
        assert bytes.fromhex(payload["before_registration"]) == registration
        assert json.loads(bytes.fromhex(payload["after_state"]))["phase"] == {
            "DRAINING": "LEGACY_WRITERS_DISABLED", "LEGACY_WRITERS_DISABLED": "ACTIVE",
        }[phase]
        if fault == "corrupt":
            path.write_bytes(raw + b" ")
            with pytest.raises(ParallelControlError, match="CUTOVER_JOURNAL_CHANGED"):
                admin._prepare_cutover_journal(custody)
            assert path.read_bytes() == raw + b" "  # Never overwrite unknown bytes.
        else:
            assert admin._prepare_cutover_journal(custody) == receipt
            assert path.stat().st_ino == identity and path.read_bytes() == raw
        assert state_path.read_bytes() == before
        assert coordination._trusted_host_registration_bytes() == registration
    with pytest.raises(ParallelControlError):
        admin._prepare_cutover_journal(custody)
    assert len(list(control.glob("cutover-*.json"))) == 1
    if not fault.startswith("after-"):
        return

    import winreg

    key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, coordination.HOST_REGISTRY_KEY)
    original_query = winreg.QueryValueEx
    current_registration = registration
    writes = []

    def query(handle, name):
        if handle is key:
            assert name == coordination.HOST_REGISTRY_VALUE
            return current_registration.decode("utf-8"), winreg.REG_SZ
        return original_query(handle, name)

    def write(handle, name, reserved, kind, content):
        nonlocal current_registration
        assert handle is key and name == coordination.HOST_REGISTRY_VALUE
        assert reserved == 0 and kind == winreg.REG_SZ
        current_registration = content.encode("utf-8")
        writes.append(current_registration)

    def flush(handle):
        assert handle is key

    monkeypatch.setattr(winreg, "QueryValueEx", query)
    monkeypatch.setattr(winreg, "SetValueEx", write)
    monkeypatch.setattr(winreg, "FlushKey", flush)
    monkeypatch.setattr(coordination, "_WindowsEnrollmentAdministrator", lambda: admin)
    with pytest.raises(RuntimeError, match="cutover interruption"):
        with coordination.host_cutover_recovery_custody(
            control, journal_sha256=receipt["sha256"], policy=policy, actor=ACTOR,
        ) as interrupted:
            assert len(interrupted.lease_snapshots) == 2
            if fault != "after-journal":
                assert admin._publish_cutover_state(interrupted) == "STATE_PUBLISHED"
                with pytest.raises(ParallelControlError, match="HOST_REGISTRATION_CHANGED"):
                    coordination.resolve_host_control_binding(repo, entrypoint="checkout-guard")
            if fault == "after-registration":
                assert admin._publish_cutover_registration(interrupted) == "REGISTRATION_PUBLISHED"
            raise RuntimeError("cutover interruption")
    with pytest.raises(ParallelControlError, match="CUTOVER_RECOVERY_CLOSED"):
        interrupted.assert_current()
    for index, root in enumerate((control, old)):
        assert _cutover_lock_probe(root, tmp_path, f"interrupted-{index}")["status"] == "ACQUIRED"
    with coordination.host_cutover_recovery_custody(
        control, journal_sha256=receipt["sha256"], policy=policy, actor=ACTOR,
    ) as recovered:
        expected = {"after-journal": "BEFORE_PUBLICATION", "after-state": "STATE_PUBLISHED",
                    "after-registration": "REGISTRATION_PUBLISHED"}[fault]
        assert recovered.assert_current() == expected
        admin._publish_cutover_state(recovered)
        assert admin._publish_cutover_registration(recovered) == "REGISTRATION_PUBLISHED"
    assert len(writes) == 1
    assert state_path.read_bytes() == bytes.fromhex(payload["after_state"])
    assert current_registration == bytes.fromhex(payload["after_registration"])
    assert path.read_bytes() == raw and path.stat().st_ino == identity
    binding = coordination.resolve_host_control_binding(repo, entrypoint="checkout-guard")
    assert binding.assert_current(operation="observe")["phase"] == json.loads(
        bytes.fromhex(payload["after_state"])
    )["phase"]


@pytest.mark.parametrize("access", [0x2, 0x4, 0x10000])
def test_native_directory_barrier_rejects_existing_writer(tmp_path, access):
    import ctypes as c
    from ctypes import wintypes as w

    from ai_trading_system.platform.architecture.workflow_coordination import (
        _WindowsEnrollmentAdministrator,
    )

    reader = object.__new__(_WindowsEnrollmentAdministrator)
    reader.c, reader.w = c, w
    reader.kernel = c.WinDLL("kernel32", use_last_error=True)
    close = reader._bind(reader.kernel, "CloseHandle", [w.HANDLE], w.BOOL)
    create = reader._bind(
        reader.kernel, "CreateFileW",
        [w.LPCWSTR, w.DWORD, w.DWORD, c.c_void_p, w.DWORD, w.DWORD, w.HANDLE], w.HANDLE,
    )
    root = tmp_path / "old-root"
    root.mkdir()
    # FILE_ADD_FILE / FILE_ADD_SUBDIRECTORY / DELETE respectively. The default
    # test-root Modify ACL does not grant FILE_DELETE_CHILD (0x40).
    writer = create(str(root), access, 7, None, 3, 0x02200000, None)
    assert writer != c.c_void_p(-1).value
    try:
        if access != 0x10000:
            with reader.pin_directories([root]):
                pass  # Original pin allows writers, but already denies DELETE.
        else:
            with pytest.raises(OSError) as failure:
                with reader.pin_directories([root]):
                    pytest.fail("existing delete handle was admitted")
            assert failure.value.winerror == 32
        with pytest.raises(OSError) as failure:
            with reader.pin_directories([root], deny_target_writers=True):
                pytest.fail("existing writer was admitted")
        assert failure.value.winerror == 32  # ERROR_SHARING_VIOLATION.
    finally:
        assert close(writer)
    with reader.pin_directories([root], deny_target_writers=True):
        writer = create(str(root), access, 7, None, 3, 0x02200000, None)
        if writer != c.c_void_p(-1).value:
            close(writer)
            pytest.fail("new writer was admitted")
        assert c.get_last_error() == 32
    # A failed/successful barrier must not leave a leaked directory handle.
    writer = create(str(root), access, 7, None, 3, 0x02200000, None)
    assert writer != c.c_void_p(-1).value
    assert close(writer)
    root.rename(tmp_path / "released-root")


def test_enrollment_native_directory_publication_never_replaces(tmp_path):
    import ctypes as c
    from ctypes import wintypes as w

    from ai_trading_system.platform.architecture.workflow_coordination import (
        _WindowsEnrollmentAdministrator,
        directory_identity,
    )

    # Exercise the actual directory publication API on ordinary disposable
    # directories. No administrator construction, ACL or registry write occurs.
    publisher = object.__new__(_WindowsEnrollmentAdministrator)
    publisher.c, publisher.w = c, w
    publisher.kernel = c.WinDLL("kernel32", use_last_error=True)
    prepared, root, contender = (tmp_path / name for name in ("prepared", "root", "contender"))
    prepared.mkdir()
    (prepared / "journal.json").write_bytes(b"immutable-intent")
    identity = directory_identity(prepared)
    publisher.publish_root(prepared, root)
    assert directory_identity(root) == {**identity, "path": root.as_posix()}
    assert not prepared.exists() and (root / "journal.json").read_bytes() == b"immutable-intent"
    contender.mkdir()
    (contender / "journal.json").write_bytes(b"different-intent")
    other_identity = directory_identity(contender)
    with pytest.raises(OSError):
        publisher.publish_root(contender, root)
    assert directory_identity(root) == {**identity, "path": root.as_posix()}
    assert directory_identity(contender) == other_identity
    assert (root / "journal.json").read_bytes() == b"immutable-intent"
    assert (contender / "journal.json").read_bytes() == b"different-intent"


@pytest.mark.parametrize(
    "boundary",
    [
        "none",
        "value-written-crash",
        "existing-same",
        "existing-different",
        "legacy-subkey",
    ],
)
def test_enrollment_single_value_anchor_protocol_with_isolated_io(monkeypatch, boundary):
    """DEVX-015A single-value anchor algorithm; not native HKLM/ACL acceptance."""
    from ai_trading_system.platform.architecture import workflow_coordination as target

    # Payload parsing is covered by the real registration parser and the native
    # HKCU test below; this canary distinguishes complete and competing payloads.
    expected = {"protocol_canary": "complete-original-payload"}
    competitor = {"protocol_canary": "competing-payload"}
    values: dict[str, object] = {}
    legacy = boundary == "legacy-subkey"
    crashed = False

    def trusted():
        if legacy:
            raise ParallelControlError("WORKFLOW_CONTROL_HOST_REGISTRATION_LEGACY_KEY", "canary")
        return values.get(target.HOST_REGISTRY_VALUE)

    def reject_legacy():
        if legacy:
            raise ParallelControlError("WORKFLOW_CONTROL_HOST_REGISTRATION_LEGACY_KEY", "canary")

    class IsolatedRegistry(target._WindowsEnrollmentAdministrator):
        def __init__(self):
            pass  # No actual administrator transport or native mutation.

        def assert_protected(self, *_args, **_kwargs):
            pass

        @contextmanager
        def registry_key(self, name):
            assert name == target.HOST_REGISTRY_KEY
            yield name

        def registry_payload(self, handle):
            return values.get(target.HOST_REGISTRY_VALUE)

        def write_registry_payload(self, handle, value):
            nonlocal crashed
            values[target.HOST_REGISTRY_VALUE] = json.loads(json.dumps(value))
            if boundary == "value-written-crash" and not crashed:
                crashed = True
                raise RuntimeError("registry-crash-after-value-write")

    monkeypatch.setattr(target, "_trusted_host_registration", trusted)
    monkeypatch.setattr(target, "_reject_legacy_host_registry_key", reject_legacy)
    publisher = IsolatedRegistry()
    if boundary == "legacy-subkey":
        with pytest.raises(ParallelControlError, match="HOST_REGISTRATION_LEGACY_KEY"):
            publisher.register(expected)
        assert values == {}
        return
    if boundary == "existing-different":
        values[target.HOST_REGISTRY_VALUE] = competitor
        with pytest.raises(ParallelControlError, match="HOST_REGISTRATION_CHANGED"):
            publisher.register(expected)
        assert values == {target.HOST_REGISTRY_VALUE: competitor}
        return
    if boundary == "existing-same":
        values[target.HOST_REGISTRY_VALUE] = json.loads(json.dumps(expected))
    if boundary == "value-written-crash":
        with pytest.raises(RuntimeError, match="registry-crash"):
            publisher.register(expected)
        # A single value write is atomic: after the crash it is complete and visible.
        assert values == {target.HOST_REGISTRY_VALUE: expected}
    publisher.register(expected)
    assert values == {target.HOST_REGISTRY_VALUE: expected}
    frozen = json.dumps(values, sort_keys=True)
    publisher.register(expected)
    assert json.dumps(values, sort_keys=True) == frozen


def test_enrollment_native_single_value_anchor_write_readback_and_delete(tmp_path):
    """Native HKCU value write/read-back/delete only; never renames a registry key."""
    import winreg

    from ai_trading_system.platform.architecture import workflow_coordination as coordination

    publisher = object.__new__(coordination._WindowsEnrollmentAdministrator)
    identity = coordination.directory_identity(tmp_path)
    registration = {
        "schema_version": "workflow_machine_registration.v1",
        "host_id": coordination.machine_host_id(),
        "control_root": tmp_path.as_posix(),
        "root_identity": identity,
        "state_sha256": "a" * 64,
        "repositories": [
            {
                "common_identity": identity,
                "checkout_identities": [identity],
                "locator_sha256": "b" * 64,
            }
        ],
    }
    before = _native_registry_fixture_observation()
    scope = "Software\AITS-DEVX015-Test-" + uuid.uuid4().hex
    assert scope not in before["roots"]
    access = winreg.KEY_READ | winreg.KEY_WRITE | winreg.KEY_WOW64_64KEY
    try:
        with winreg.CreateKeyEx(winreg.HKEY_CURRENT_USER, scope, 0, access) as key:
            assert publisher.registry_payload(key) is None
            publisher.write_registry_payload(key, registration)
            assert publisher.registry_payload(key) == registration
            winreg.SetValueEx(key, "Foreign", 0, winreg.REG_SZ, "unknown")
            with pytest.raises(ParallelControlError, match="HOST_REGISTRATION_ANCHOR_CHANGED"):
                publisher.registry_payload(key)
            winreg.DeleteValue(key, "Foreign")
            with winreg.CreateKeyEx(key, "WorkflowControl", 0, access):
                pass
            with pytest.raises(ParallelControlError, match="HOST_REGISTRATION_ANCHOR_CHANGED"):
                publisher.registry_payload(key)
            winreg.DeleteKey(key, "WorkflowControl")
            assert publisher.registry_payload(key) == registration
    finally:
        _remove_native_registry_fixture(winreg, scope)
    after = _native_registry_fixture_observation()
    (tmp_path / "native-enrollment-single-value-observation.json").write_text(
        json.dumps(
            {
                "scope": "CURRENT_PROCESS_HKCU_FRESH_TEST_ROOT_ONLY",
                "fixture_root": scope,
                "before": before,
                "after": after,
                "registry_key_rename_performed": False,
                "production_host_acceptance": False,
            }
        ),
        encoding="utf-8",
    )
    assert before == after, "NATIVE_FIXTURE_CHANGED_RETAINED_ROOT_OR_LEFT_RESIDUE"


def test_no_registry_key_rename_api_is_bound_anywhere():
    """DEVX-015A guard: RegRenameKey/NtRenameKey crashed this host's kernel twice."""
    root = Path(__file__).resolve().parents[1]
    names = ("Reg" + "RenameKey", "Nt" + "RenameKey")
    pattern = re.compile(
        "|".join(rf"[\"']{name}[\"']|\.{name}|{name}\s*\(" for name in names)
    )
    offenders = []
    for folder in ("src", "scripts", "tests", "tools"):
        for path in sorted((root / folder).rglob("*.py")):
            for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                code = line.split("#", 1)[0]
                if pattern.search(code):
                    offenders.append(f"{path.relative_to(root).as_posix()}:{number}")
    assert offenders == []


@pytest.mark.parametrize(
    "boundary",
    [
        "none",
        "root-before-journal",
        "root-publish-before",
        "root-publish-after",
        "journal",
        "state",
        "locator",
        "retirement",
        "anchor",
        "journal-active",
        "journal-digest",
        "journal-host",
    ],
)
def test_enrollment_journal_resumes_same_draining_intent_with_isolated_admin_transport(
    small_repository,
    tmp_path,
    monkeypatch,
    boundary,
):
    """Protocol unit test: simulated admin transport, never native L03 evidence."""
    import shutil
    from contextlib import nullcontext

    from ai_trading_system.platform.architecture import workflow_coordination as target

    root = small_repository
    source = Path(__file__).resolve().parents[1]
    for name in (
        "scripts/architecture_arch005_checkout_guard.py",
        "docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md",
        "config/architecture/arch_005_integration_publication_fence.yaml",
    ):
        destination = root / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / name, destination)
    store, lease, _request, _env, _now = _case(tmp_path)
    original_events = {p: p.read_bytes() for p in store.events_root.rglob("*.json")}
    control = tmp_path / "enrolled"
    plan = target.plan_host_enrollment(
        root, policy=store.policy, control_root=control, legacy_roots=[store.root]
    )
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    crashed = False

    def crash(name):
        nonlocal crashed
        expected_boundary = (
            "root"
            if boundary == "root-before-journal"
            else "journal"
            if boundary.startswith("journal-")
            else boundary
        )
        if name == expected_boundary and not crashed:
            crashed = True
            raise RuntimeError("simulated-enrollment-crash-" + name)

    class IsolatedAdministrator:
        def assert_protected(self, *_args, **_kwargs):
            pass  # Deliberate security transport isolation, not an ACL oracle.

        def pin_directories(self, _paths):
            return nullcontext()

        def create_root(self, path):
            path.mkdir()
            crash("root")

        def publish_root(self, prepared, final):
            crash("root-publish-before")
            prepared.rename(final)
            crash("root-publish-after")

        def write_admin_json(self, path, value):
            if path.exists():
                assert json.loads(path.read_text(encoding="utf-8")) == value
            else:
                with path.open("x", encoding="utf-8", newline="\n") as stream:
                    stream.write(json.dumps(value, sort_keys=True, ensure_ascii=False) + "\n")
            name = {
                target.ENROLLMENT_JOURNAL_NAME: "journal",
                target.CONTROL_STATE_NAME: "state",
                target.CONTROL_BINDING_NAME: "locator",
                target.RETIREMENT_NAME: "retirement",
            }
            crash(name[path.name])

        def register(self, registration):
            # Reuse the existing narrow winreg transport seam; original registry
            # parser, host identity, repository identity and binding checks run.
            if not getattr(self, "registered", False):
                _anchor_control(root, control, monkeypatch)
            assert target._trusted_host_registration() == registration
            crash("anchor")

    monkeypatch.setattr(target, "_WindowsEnrollmentAdministrator", IsolatedAdministrator)
    if boundary == "none":
        result = target.enroll_host_draining(
            root, policy=store.policy, actor=ACTOR, plan_path=plan_path
        )
    else:
        with pytest.raises(RuntimeError, match="simulated-enrollment-crash"):
            target.enroll_host_draining(root, policy=store.policy, actor=ACTOR, plan_path=plan_path)
        if boundary == "root-before-journal":
            with pytest.raises(ParallelControlError, match="ENROLLMENT_PREPARE_INCOMPLETE"):
                target.enroll_host_draining(
                    root,
                    policy=store.policy,
                    actor=ACTOR,
                    recover_root=control,
                )
            assert not control.exists()
            assert list(target._enrollment_preparation_path(control).iterdir()) == []
            assert {p: p.read_bytes() for p in store.events_root.rglob("*.json")} == original_events
            # Before a journal exists, only the explicit original, still-current
            # plan can resume preparation. Recovery never invents an intent.
            target.enroll_host_draining(root, policy=store.policy, actor=ACTOR, plan_path=plan_path)
        if boundary.startswith("journal-"):
            preparation = target._enrollment_preparation_path(control)
            journal_path = preparation / target.ENROLLMENT_JOURNAL_NAME
            changed = json.loads(journal_path.read_text(encoding="utf-8"))
            if boundary == "journal-active":
                changed["documents"]["state"]["phase"] = "ACTIVE"
            elif boundary == "journal-digest":
                changed["plan"]["plan_sha256"] = "0" * 64
            else:
                from ai_trading_system.platform.architecture.workflow_contract import (
                    canonical_digest,
                )

                changed["plan"]["host_id"] = "0" * 64
                changed["plan"]["plan_sha256"] = canonical_digest(
                    {key: value for key, value in changed["plan"].items() if key != "plan_sha256"}
                )
            journal_path.write_text(json.dumps(changed), encoding="utf-8")
            before = {p: p.read_bytes() for p in preparation.rglob("*") if p.is_file()}
            with pytest.raises(ParallelControlError, match="ENROLLMENT_ARTIFACT_CHANGED"):
                target.enroll_host_draining(
                    root, policy=store.policy, actor=ACTOR, recover_root=control
                )
            assert {p: p.read_bytes() for p in preparation.rglob("*") if p.is_file()} == before
            assert {p: p.read_bytes() for p in store.events_root.rglob("*.json")} == original_events
            assert not (control / target.CONTROL_STATE_NAME).exists()
            return
        result = target.enroll_host_draining(
            root, policy=store.policy, actor=ACTOR, recover_root=control
        )
    assert result["status"] == "DRAINING" and not result["activation_allowed"]
    assert not result["old_binary_os_fence_installed"]
    binding = target.resolve_host_control_binding(root, entrypoint="checkout-guard")
    with pytest.raises(ParallelControlError, match="MIGRATION_DRAINING"):
        binding.assert_current(operation="acquire")
    assert {p: p.read_bytes() for p in store.events_root.rglob("*.json")} == original_events
    assert (
        next(head for head in store.replay().lease_heads if head.lease_id == lease.lease_id).state
        == "ACTIVE"
    )
    assert FileExecutionLeaseStore(control, policy=store.policy).replay().event_count == 0
    frozen = {
        p: p.read_bytes()
        for p in [
            control / target.ENROLLMENT_JOURNAL_NAME,
            control / target.CONTROL_STATE_NAME,
            root / ".git" / target.CONTROL_BINDING_NAME,
            store.root / target.RETIREMENT_NAME,
        ]
    }
    again = target.enroll_host_draining(
        root, policy=store.policy, actor=ACTOR, recover_root=control
    )
    assert again == result and all(p.read_bytes() == content for p, content in frozen.items())


@pytest.mark.parametrize(
    "case",
    [
        "empty",
        "active",
        "live-job",
        "invalid-ledger",
        "duplicate",
        "overlap",
        "target-not-empty",
        "preparation-overlap",
        "wrong-origin",
        "missing-sentinel",
        "relative-root",
    ],
)
def test_public_enrollment_plan_preserves_original_stores_and_repository(
    small_repository,
    tmp_path,
    case,
):
    """Real CLI/identity/ledger checks; no administrator installation claim."""
    import shutil

    from test_devx015_workflow_integration import _git

    from ai_trading_system.platform.architecture.workflow_contract import canonical_digest

    root = small_repository
    source = Path(__file__).resolve().parents[1]
    for name in (
        "scripts/architecture_arch005_workflow.py",
        "scripts/architecture_arch005_checkout_guard.py",
        "docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md",
    ):
        destination = root / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / name, destination)
    old = tmp_path / "leases"
    old.mkdir()
    target = tmp_path / "new-host-control"
    if case == "preparation-overlap":
        from ai_trading_system.platform.architecture.workflow_coordination import (
            _enrollment_preparation_path,
        )

        old = _enrollment_preparation_path(target)
        old.mkdir()
    if case in {"active", "live-job"}:
        store, lease, _request, _env, _now = _case(tmp_path)
        assert store.root == old
        if case == "live-job":
            from ai_trading_system.platform.architecture.workflow_coordination import (
                ExecutionLifecycle,
            )

            ExecutionLifecycle(store).reserve(_request, actor=ACTOR, now=_now)
    elif case == "invalid-ledger":
        bad = old / "events/invalid/broken.json"
        bad.parent.mkdir(parents=True)
        bad.write_text("{}", encoding="utf-8")
    elif case == "target-not-empty":
        target.mkdir()
        (target / "unrelated.txt").write_text("preserve", encoding="utf-8")
    elif case == "wrong-origin":
        _git(root, "remote", "set-url", "origin", "https://example.invalid/wrong.git")
    elif case == "missing-sentinel":
        (root / "docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md").unlink()
    command = [
        sys.executable,
        str(root / "scripts/architecture_arch005_workflow.py"),
        "control-enrollment-plan",
        "--lease-policy",
        str(POLICY_PATH),
        "--control-root",
        str(old / "nested" if case == "overlap" else target),
        "--legacy-root",
        "relative" if case == "relative-root" else str(old),
    ]
    if case == "duplicate":
        command += ["--legacy-root", str(old)]

    def snapshot():
        return {
            path.relative_to(tmp_path).as_posix(): path.read_bytes()
            for path in tmp_path.rglob("*")
            if path.is_file()
        }

    with ExitStack() as stack:
        if case == "live-job":
            handle = stack.enter_context(
                WindowsJobProcess.create(
                    argv=[sys.executable, "-c", "import time; time.sleep(60)"],
                    cwd=tmp_path,
                    environment=_env,
                    stdout_path=tmp_path / "live-job.log",
                    job_name=_request["job_name"],
                )
            )
            handle.resume()
            assert handle.active_process_count() >= 1
        before = snapshot()
        result = subprocess.run(
            command,
            cwd=root,
            env={**os.environ, "PYTHONPATH": str(source / "src"), "PYTHONDONTWRITEBYTECODE": "1"},
            capture_output=True,
            text=True,
            timeout=45,
        )
        assert snapshot() == before
        if case == "live-job":
            assert handle.active_process_count() >= 1
    if case in {"empty", "active", "live-job"}:
        assert result.returncode == 0, result.stdout + result.stderr
        plan = json.loads(result.stdout)
        digest = plan.pop("plan_sha256")
        assert digest == canonical_digest(plan)
        assert plan["status"] == (
            "ADMIN_INSTALLATION_REQUIRED" if case == "empty" else "DRAIN_REQUIRED"
        )
        assert not plan["activation_allowed"] and not plan["mutation_performed"]
        assert not plan["snapshot_atomic"] and not target.exists()
        assert plan["repositories"][0]["repository"]["checkout"] == root.as_posix()
        assert plan["legacy_stores"][0]["active_lease_ids"] == (
            [] if case == "empty" else [lease.lease_id]
        )
        if case == "live-job":
            assert plan["legacy_stores"][0]["executors"] == [
                {"lease_id": lease.lease_id, "drained": False}
            ]
    else:
        expected = {
            "invalid-ledger": "CUTOVER_REPLAY_INVALID",
            "duplicate": "CUTOVER_ROOT_ALIAS",
            "overlap": "CUTOVER_ROOT_OVERLAP",
            "target-not-empty": "ENROLLMENT_TARGET_NOT_EMPTY",
            "wrong-origin": "REPOSITORY_IDENTITY",
            "missing-sentinel": "REPOSITORY_SENTINEL",
            "relative-root": "ENROLLMENT_ABSOLUTE_PATH_REQUIRED",
            "preparation-overlap": "CUTOVER_ROOT_OVERLAP",
        }[case]
        assert result.returncode != 0 and expected in result.stderr, result.stdout + result.stderr


def _case(tmp_path: Path):
    task = _task("ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE")
    policy = load_parallel_control_policy(POLICY_PATH)
    store = FileExecutionLeaseStore(tmp_path / "leases", policy=policy)
    now = datetime.now(UTC)
    readiness = evaluate_task_readiness(
        task,
        dependencies=[],
        observed_statuses={},
        graph_report=validate_dependency_graph([task.task_id], []),
        current_base_commit=BASE_COMMIT,
        policy=policy,
    )
    lease = store.acquire(
        task=task,
        readiness=readiness,
        lane_id="test-lane",
        actor=ACTOR,
        current_base_commit=BASE_COMMIT,
        now=now,
    ).lease
    env = dict(os.environ)
    argv = [sys.executable, "-c", "print('contained lifecycle')"]
    request = {
        "schema_version": "workflow_execution_request.v1",
        "request_id": uuid.uuid4().hex,
        "lease_id": lease.lease_id,
        "manifest_sha256": lease.change_manifest_sha256,
        "candidate_sha": BASE_COMMIT,
        "validation_identity_sha256": "b" * 64,
        "argv": argv,
        "cwd": tmp_path.resolve().as_posix(),
        "environment_sha256": execution_environment_sha256(env),
        "stdout_path": (tmp_path / "stdout.log").as_posix(),
        "result_path": (tmp_path / "result.json").as_posix(),
        "job_name": "Local\\AITS-DEVX015-lifecycle-" + uuid.uuid4().hex,
        "host_id": "synthetic-host",
        "writer_epoch": "synthetic-v3",
        "subject_task_id": task.task_id,
        "task_authority_sha256": "c" * 64,
    }
    return store, lease, request, env, now


@pytest.mark.parametrize("entry", ["files", "public"])
@pytest.mark.parametrize("fault", ["none", "fields", "identity", "removed"])
def test_replay_preserves_full_event_and_transition_validation(tmp_path, entry, fault):
    from dataclasses import replace

    from ai_trading_system.platform.architecture.parallel_control_kernel import (
        _lease_event,
        parse_lease_event,
        replay_lease_events,
    )
    from ai_trading_system.platform.architecture.workflow_coordination import ExecutionLifecycle

    store, lease, request, _environment, now = _case(tmp_path)
    ExecutionLifecycle(store).reserve(request, actor=ACTOR, now=now)
    before = store.replay()
    assert before.status == "PASS"
    head = next(row for row in before.lease_heads if row.lease_id == lease.lease_id)
    current = replace(head, execution=json.loads(json.dumps(head.execution)))
    if fault == "fields":
        del current.execution["request_sha256"]
    elif fault == "identity":
        current = replace(current, lane_id="different-lane")
    elif fault == "removed":
        current = replace(current, execution=None)
    event = _lease_event(
        lease=current, previous_event_id=dict(before.head_event_ids)[lease.lease_id],
        from_state="ACTIVE", to_state="ACTIVE", occurred_at=now + timedelta(seconds=1),
        actor=ACTOR, reason_codes=("EXECUTION_RESERVED",),
    )
    # The bad event has its own correct content hash and a valid causal parent;
    # schema and transition validation, not a checksum mismatch, must reject it.
    if entry == "files":
        path = store.events_root / lease.lease_id / (event.event_id + ".json")
        path.write_text(json.dumps(event.to_dict()), encoding="utf-8")
        result = store.replay()
    else:
        events = [parse_lease_event(json.loads(path.read_text(encoding="utf-8")))
                  for path in store.events_root.glob("*/*.json")]
        result = replay_lease_events([*events, event])
    assert result.status == ("PASS" if fault == "none" else "FAIL")
    if fault != "none":
        expected = "LEASE_IDENTITY_DRIFT" if fault == "identity" else "FIELDS"
        assert any(expected in str(issue) for issue in result.issues), result.issues


@pytest.mark.parametrize("terminal", [False, True])
def test_drain_inspection_never_steals_expired_lease_or_authorizes_cutover(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    terminal: bool,
) -> None:
    from ai_trading_system.platform.architecture.workflow_coordination import (
        CONTROL_STATE_NAME,
        RETIREMENT_NAME,
        directory_identity,
        inspect_control_drain,
    )

    policy = load_parallel_control_policy(POLICY_PATH)
    old = FileExecutionLeaseStore(tmp_path / "old-store", policy=policy)
    task = _task("ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE")
    readiness = evaluate_task_readiness(
        task,
        dependencies=[],
        observed_statuses={},
        graph_report=validate_dependency_graph([task.task_id], []),
        current_base_commit=BASE_COMMIT,
        policy=policy,
    )
    then = datetime.now(UTC) - timedelta(days=1)
    lease = old.acquire(
        task=task,
        readiness=readiness,
        lane_id="old-lane",
        actor=ACTOR,
        current_base_commit=BASE_COMMIT,
        now=then,
    ).lease
    if terminal:
        old.release(lease.lease_id, actor=ACTOR, now=then + timedelta(seconds=1), evidence_refs=())
    repo, control, _policy, state = _registered_control(tmp_path, monkeypatch)
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "remote",
            "add",
            "origin",
            "https://github.com/AI-Trading-Collaboration/AITradingSystem.git",
        ],
        check=True,
        capture_output=True,
    )
    for relative in (
        "docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md",
        "scripts/architecture_arch005_checkout_guard.py",
    ):
        target = repo / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("synthetic sentinel", encoding="utf-8")
    state["phase"] = "DRAINING"
    state["legacy_roots"] = [
        {"root_identity": directory_identity(old.root), "epoch": state["epoch"]}
    ]
    (control / CONTROL_STATE_NAME).write_text(json.dumps(state), encoding="utf-8")
    (old.root / RETIREMENT_NAME).write_text(
        json.dumps(
            {
                "schema_version": "workflow_legacy_retirement.v1",
                "root_identity": directory_identity(old.root),
                "control_root": control.as_posix(),
                "epoch": state["epoch"],
            }
        ),
        encoding="utf-8",
    )
    _anchor_control(repo, control, monkeypatch)
    before = {path: path.read_bytes() for path in old.root.rglob("*") if path.is_file()}
    result = inspect_control_drain(repo, policy=policy)
    assert result["activation_allowed"] is False
    assert result["mutation_performed"] is False
    assert result["roots"][0]["active_lease_ids"] == ([] if terminal else [lease.lease_id])
    assert result["status"] == ("OS_FENCE_EVIDENCE_REQUIRED" if terminal else "DRAIN_REQUIRED")
    assert {path: path.read_bytes() for path in old.root.rglob("*") if path.is_file()} == before
    assert old.replay().lease_heads[0].state == ("RELEASED" if terminal else "ACTIVE")


@pytest.mark.parametrize(
    "damage", ["locator_removed", "git_removed", "locator_resealed", "state_resealed"]
)
def test_managed_host_never_falls_back_to_fresh_legacy_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    damage: str,
) -> None:
    from ai_trading_system.platform.architecture.workflow_coordination import (
        CONTROL_BINDING_NAME,
        CONTROL_STATE_NAME,
        coordinated_lease_store,
    )

    repo, root, policy, state = _registered_control(tmp_path, monkeypatch)
    locator = repo / ".git" / CONTROL_BINDING_NAME
    if damage == "locator_removed":
        locator.rename(locator.with_suffix(".retained"))
        expected = "REGISTERED_LOCATOR_MISSING"
    elif damage == "git_removed":
        (repo / ".git").rename(repo / "retained-git")
        expected = "GIT_IDENTITY_UNAVAILABLE"
    elif damage == "locator_resealed":
        value = json.loads(locator.read_text(encoding="utf-8"))
        value["resource_markers"]["full"] = "other/full"
        locator.write_text(json.dumps(value), encoding="utf-8")
        expected = "HOST_REGISTRATION_CHANGED"
    else:
        state["resource_markers"]["full"] = "other/full"
        (root / CONTROL_STATE_NAME).write_text(json.dumps(state), encoding="utf-8")
        expected = "HOST_REGISTRATION_CHANGED"
    bypass = tmp_path / "alternate-legacy"
    with pytest.raises(ParallelControlError, match=expected):
        coordinated_lease_store(repo, bypass, policy=policy, entrypoint="checkout-guard")
    assert not bypass.exists(), "failed closed only after creating an alternate lock authority"


def test_untrusted_locator_cannot_enrol_host(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import winreg

    from ai_trading_system.platform.architecture.workflow_coordination import (
        HOST_REGISTRY_KEY,
        resolve_host_control_binding,
    )

    repo, _root, _policy, _state = _registered_control(tmp_path, monkeypatch)
    real_open = winreg.OpenKey

    def unregistered(hive, path, *args):
        if hive == winreg.HKEY_LOCAL_MACHINE and path == HOST_REGISTRY_KEY:
            raise FileNotFoundError("synthetic pre-installation registry")
        return real_open(hive, path, *args)

    monkeypatch.setattr(winreg, "OpenKey", unregistered)
    with pytest.raises(ParallelControlError, match="HOST_REGISTRATION_REQUIRED"):
        resolve_host_control_binding(repo, entrypoint="checkout-guard")


def test_real_registry_host_rejects_unregistered_checkout(tmp_path: Path) -> None:
    _exercise_native_registry(tmp_path)


def _native_registry_fixture_observation() -> dict:
    import winreg
    roots = []
    with winreg.OpenKey(
        winreg.HKEY_CURRENT_USER, "Software", 0,
        winreg.KEY_READ | winreg.KEY_WOW64_64KEY,
    ) as software:
        for index in range(winreg.QueryInfoKey(software)[0]):
            name = winreg.EnumKey(software, index)
            if name.startswith("AITS-DEVX015-Test-"):
                roots.append("Software\\" + name)
    rows = []
    pending = sorted(roots)
    while pending:
        assert len(rows) < 256, "retained fixture inventory exceeds diagnostic bound"
        path = pending.pop()
        with winreg.OpenKey(
            winreg.HKEY_CURRENT_USER, path, 0,
            winreg.KEY_READ | winreg.KEY_WOW64_64KEY,
        ) as key:
            children, values, modified = winreg.QueryInfoKey(key)
            contents = [winreg.EnumValue(key, i) for i in range(values)]
            rows.append((path, modified, hashlib.sha256(repr(contents).encode()).hexdigest()))
            pending.extend(path + "\\" + winreg.EnumKey(key, i) for i in range(children))
    return {"roots": sorted(roots), "rows": sorted(rows)}


def test_native_registry_fixture_preserves_existing_view_and_leaves_no_new_root(
    tmp_path: Path,
) -> None:
    """Real Win32 fixture cleanup; never delete retained roots or assert host visibility."""
    before = _native_registry_fixture_observation()
    _exercise_native_registry(tmp_path)
    after = _native_registry_fixture_observation()
    (tmp_path / "native-view-cleanup-observation.json").write_text(json.dumps({
        "schema_version": "devx015_native_fixture_view_cleanup.v1",
        "before": before, "after": after,
        "registry_scope": "CURRENT_PROCESS_REGISTRY_VIEW_WITH_FIXTURE_HKLM_OVERRIDE",
        "production_host_acceptance": False,
    }), encoding="utf-8")
    assert after == before, "NATIVE_FIXTURE_CHANGED_RETAINED_ROOT_OR_LEFT_RESIDUE"


@pytest.mark.parametrize("competition", ["linked-publication", "linked-full", "repositories-full"])
def test_native_registered_guard_competition(tmp_path: Path, competition: str) -> None:
    _exercise_native_registry(tmp_path, competition=competition)


def test_native_linked_publication_cli_competition(tmp_path: Path) -> None:
    _exercise_native_registry(tmp_path, competition="linked-fence")


@pytest.mark.parametrize("mutation", [False, True], ids=["original", "M04"])
def test_m04_native_per_checkout_store_hits_original_shared_store_assertion(
    tmp_path: Path, mutation: bool,
) -> None:
    if not mutation:
        _exercise_native_registry(tmp_path, competition="linked-full")
        return
    with pytest.raises(AssertionError, match="NATIVE_REGISTERED_STORE_MUST_BE_SHARED"):
        _exercise_native_registry(tmp_path, competition="linked-full", per_checkout_mutant=True)
    rows = json.loads((tmp_path / "real-registry-results.json").read_text(encoding="utf-8"))
    registered = next(row for row in rows if row.get("status") == "REGISTERED")
    expected = Path(registered["checkout"]) / "forbidden-legacy-store"
    assert registered["store"] == expected.as_posix()
    proof = json.loads((tmp_path / "m04-method.json").read_text(encoding="utf-8"))
    assert proof["before_sha256"] != proof["after_sha256"]
    assert proof["replacement_count"] == 1


def _exercise_native_registry(
    tmp_path: Path, *, competition: str | None = None, per_checkout_mutant: bool = False,
) -> None:
    """Real child registry/Git identity; no production HKLM writes or gate mocks."""
    import winreg

    from ai_trading_system.platform.architecture.workflow_coordination import (
        CONTROL_BINDING_NAME,
        CONTROL_STATE_NAME,
        HOST_REGISTRY_KEY,
        HOST_REGISTRY_VALUE,
        control_policy_sha256,
        directory_identity,
        machine_host_id,
    )

    # Reuse only fixture construction. All transport patches are removed BEFORE
    # independent children import the SUT and exercise the admission boundary.
    with pytest.MonkeyPatch.context() as construction:
        repo, control, _policy, _state = _registered_control(tmp_path, construction)
    other = tmp_path / "unregistered-checkout"
    subprocess.run(["git", "init", str(other)], check=True, capture_output=True)
    common = repo / ".git"
    enrolled = [repo]
    repositories = [(repo, common)]
    if competition is not None:
        from ai_trading_system.platform.architecture.checkout_guard import (
            DEFAULT_CHECKOUT_GUARD_POLICY_PATH,
            _checkout_lease_policy,
            load_checkout_guard_policy,
        )

        def seed(checkout: Path) -> None:
            (checkout / ".gitignore").write_text("outputs/\n", encoding="utf-8")
            if competition == "linked-fence":
                for name in (
                    "arch_005_s4d_checkout_guard.yaml",
                    "arch_005_parallel_control_policy.yaml",
                    "arch_005_integration_publication_fence.yaml",
                ):
                    target = checkout / "config/architecture" / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes((POLICY_PATH.parent / name).read_bytes())
            for arguments in (
                ("config", "user.email", "native-host@example.com"),
                ("config", "user.name", "Native Host Test"),
                (
                    "add",
                    "--",
                    ".gitignore",
                    *(("config",) if competition == "linked-fence" else ()),
                ),
                ("commit", "-m", "seed"),
                ("branch", "-M", "main" if competition == "linked-fence" else "fixture"),
            ):
                subprocess.run(
                    ["git", "-C", str(checkout), *arguments], check=True, capture_output=True
                )

        seed(repo)
        second = tmp_path / "participant-b"
        if competition.startswith("linked-"):
            subprocess.run(
                ["git", "-C", str(repo), "worktree", "add", "--detach", str(second), "HEAD"],
                check=True,
                capture_output=True,
            )
        else:
            subprocess.run(["git", "init", str(second)], check=True, capture_output=True)
            seed(second)
            repositories.append((second, second / ".git"))
        enrolled.append(second)
        if competition == "linked-fence":
            _state["resource_markers"] = {
                "publication": (
                    "outputs/architecture/arch_005_integration_publication_fence/publication.resource"
                ),
                "full": "outputs/validation_runtime",
            }
        _state["policy_sha256"] = control_policy_sha256(
            _checkout_lease_policy(
                _policy,
                load_checkout_guard_policy(DEFAULT_CHECKOUT_GUARD_POLICY_PATH),
            )
        )
        _state["registrations"] = []
        for number, (_checkout, git_common) in enumerate(repositories):
            row = {
                "registration_id": f"native-{number}",
                "common_root": git_common.as_posix(),
                "common_identity": directory_identity(git_common),
                "entrypoints": ["checkout-guard"],
            }
            _state["registrations"].append(row)
            locator = {
                "schema_version": "workflow_host_binding.v1",
                "control_root": control.as_posix(),
                **{
                    key: _state[key]
                    for key in (
                        "host_id",
                        "epoch",
                        "root_identity",
                        "resource_markers",
                        "policy_sha256",
                    )
                },
                **{key: value for key, value in row.items() if key != "entrypoints"},
            }
            (git_common / CONTROL_BINDING_NAME).write_text(json.dumps(locator), encoding="utf-8")
        (control / CONTROL_STATE_NAME).write_text(json.dumps(_state), encoding="utf-8")
    registration = {
        "schema_version": "workflow_machine_registration.v1",
        "host_id": machine_host_id(),
        "control_root": control.as_posix(),
        "root_identity": directory_identity(control),
        "state_sha256": hashlib.sha256((control / CONTROL_STATE_NAME).read_bytes()).hexdigest(),
        "repositories": [
            {
                "common_identity": directory_identity(common),
                "checkout_identities": [directory_identity(repo)],
                "locator_sha256": hashlib.sha256(
                    (common / CONTROL_BINDING_NAME).read_bytes(),
                ).hexdigest(),
            }
        ],
    }
    if competition is not None:
        registration["repositories"] = [
            {
                "common_identity": directory_identity(git_common),
                "checkout_identities": [
                    directory_identity(checkout)
                    for checkout in enrolled
                    if git_common == common
                    and competition.startswith("linked-")
                    or checkout == owner
                ],
                "locator_sha256": hashlib.sha256(
                    (git_common / CONTROL_BINDING_NAME).read_bytes(),
                ).hexdigest(),
            }
            for owner, git_common in repositories
        ]
    driver = r"""
import ctypes, json, os, sys, winreg
from pathlib import Path
from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
from ai_trading_system.platform.architecture.parallel_control_kernel import (
    load_parallel_control_policy,
)
from ai_trading_system.platform.architecture.workflow_coordination import coordinated_lease_store
override = ctypes.WinDLL('advapi32', use_last_error=True).RegOverridePredefKey
override.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
override.restype = ctypes.c_long
hklm = ctypes.c_void_p(ctypes.c_int32(int(winreg.HKEY_LOCAL_MACHINE)).value)
with winreg.OpenKey(winreg.HKEY_CURRENT_USER, sys.argv[1], 0,
                    winreg.KEY_READ | winreg.KEY_WOW64_64KEY) as isolated:
    code = override(hklm, ctypes.c_void_p(int(isolated)))
    assert code == 0, ('registry override', code)
    try:
        root = Path(sys.argv[2])
        policy = load_parallel_control_policy(Path(sys.argv[3]))
        try:
            store = coordinated_lease_store(root, root/'forbidden-legacy-store',
                                            policy=policy, entrypoint='checkout-guard')
        except ParallelControlError as error:
            result = {'status': 'DENIED', 'reason': str(error)}
        else:
            assert store.coordination_binding is not None
            result = {'status': 'REGISTERED', 'store': store.root.as_posix(),
                      'checkout': store.coordination_binding.project_root.as_posix()}
        def report(**fields):
            print(json.dumps({'pid': os.getpid(), **fields}), flush=True)
        report(**result)
        if len(sys.argv) > 4:
            from datetime import UTC, datetime
            from ai_trading_system.platform.architecture.checkout_guard import (
                CheckoutLeaseGuard, CheckoutOperationClass,
            )
            from ai_trading_system.platform.architecture.workflow_execution import WindowsJobProcess
            guard = CheckoutLeaseGuard(project_root=root)
            marker = 'publication/main' if sys.argv[4] == 'publication' else 'validation/full'
            fence_cli = sys.argv[4] == 'fence'
            participant = sys.argv[5]
            transaction = (root/'outputs/architecture/arch_005_integration_publication_fence'
                           /'transactions'/('native-'+participant)/'transaction.json')
            def public_cli(arguments):
                import contextlib, io, runpy
                original_argv = sys.argv
                script = (Path(sys.argv[3]).parents[2]
                          /'scripts/architecture_arch005_publication_fence.py')
                capture = io.StringIO()
                try:
                    sys.argv = [str(script), '--repository', str(root), *arguments]
                    with contextlib.redirect_stdout(capture):
                        try:
                            runpy.run_path(str(script), run_name='__main__')
                        except SystemExit as exit_result:
                            code = exit_result.code
                        else:
                            code = 0
                finally:
                    sys.argv = original_argv
                raw = capture.getvalue()
                report(status='CLI_RESULT', argv=arguments, returncode=code, stdout=raw)
                return code, json.loads(raw)
            def acquire(suffix):
                global transaction
                if fence_cli:
                    import subprocess
                    attempt = 'native-'+participant+('-retry' if suffix == 'retry' else '')
                    transaction = transaction.parent.parent/attempt/'transaction.json'
                    head = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'],
                                                   text=True).strip()
                    code, result = public_cli(['acquire', '--transaction-id', attempt,
                        '--task-id', 'L01-'+participant, '--change-id', attempt,
                        '--thread-id', participant, '--actor', 'integration-coordinator',
                        '--frozen-base', head, '--lane-head', head, '--expected-main', head])
                    return code, result
                return guard.acquire(intent_id='native-'+sys.argv[5]+'-'+suffix,
                    task_id='L01-'+sys.argv[5], thread_id=sys.argv[5],
                    actor='architecture-control-plane',
                    operation_class=CheckoutOperationClass.DOMAIN_MUTATION,
                    owned_paths=(marker,), now=datetime.now(UTC))
            assert sys.stdin.readline().strip() == 'ACQUIRE'
            if fence_cli:
                code, result = acquire('first')
                report(status='PASS' if code == 0 else 'BLOCKED',
                       reasons=[result.get('reason_code', '')],
                       active=len(guard.replay().active_leases))
                if code != 0:
                    assert sys.stdin.readline().strip() == 'RETRY'
                    code, result = acquire('retry')
                    report(status='PASS' if code == 0 else 'BLOCKED',
                           active=len(guard.replay().active_leases))
                assert code == 0 and transaction.is_file()
                assert sys.stdin.readline().strip() == 'EXECUTE'
                # Exercise the real public terminal release for this admission-only attempt.
                code, result = public_cli(['release', '--transaction', str(transaction),
                    '--actor', 'integration-coordinator', '--outcome', 'failed'])
                assert code == 0
                report(status='DONE', active=len(guard.replay().active_leases))
                raise SystemExit(0)
            decision, handle = acquire('first')
            report(status=decision.status, reasons=decision.reason_codes,
                   active=len(guard.replay().active_leases))
            if handle is None:
                assert sys.stdin.readline().strip() == 'RETRY'
                decision, handle = acquire('retry')
                report(status=decision.status, reasons=decision.reason_codes,
                       active=len(guard.replay().active_leases))
            assert decision.status == 'PASS' and handle is not None
            assert sys.stdin.readline().strip() == 'EXECUTE'
            # Actual bounded native execution, not a claimed project Full run.
            output = root/'outputs/native-proof'
            output.mkdir(parents=True, exist_ok=True)
            source = "from pathlib import Path; Path('effect').write_text('once')"
            job = WindowsJobProcess.create(argv=[sys.executable, '-c', source], cwd=output,
                environment=dict(os.environ), stdout_path=output/'stdout.log',
                job_name='Local\\AITS-DEVX015-native-proof-'+str(os.getpid()))
            try:
                job.resume()
                assert job.wait(timeout=30) == 0
            finally:
                job.close()
            handle.release(outcome='completed', at=datetime.now(UTC))
            report(status='DONE', active=len(guard.replay().active_leases))
    finally:
        assert override(hklm, None) == 0
"""
    if per_checkout_mutant:
        # Execute the changed real method only inside the isolated child. Keep
        # registry resolution, binding, and the original parent oracle intact.
        injection = r'''
import hashlib, inspect, textwrap
import ai_trading_system.platform.architecture.workflow_coordination as coordination
before = textwrap.dedent(inspect.getsource(coordination.coordinated_lease_store))
target = 'binding.root if binding is not None else legacy_root'
assert before.count(target) == 1
after = before.replace(target, 'legacy_root')
exec(compile('from __future__ import annotations\n' + after,
             '<M04-per-checkout-store>', 'exec'), coordination.__dict__)
coordinated_lease_store = coordination.coordinated_lease_store
Path(sys.argv[2]).parent.joinpath('m04-method.json').write_text(json.dumps({
    'before_sha256': hashlib.sha256(before.encode()).hexdigest(),
    'after_sha256': hashlib.sha256(after.encode()).hexdigest(),
    'replacement_count': before.count(target), 'before': before, 'after': after,
    'module_sha256': hashlib.sha256(Path(coordination.__file__).read_bytes()).hexdigest(),
}), encoding='utf-8')
'''
        anchor = "override = ctypes.WinDLL('advapi32', use_last_error=True).RegOverridePredefKey"
        assert driver.count(anchor) == 1
        driver = driver.replace(anchor, injection + "\n" + anchor)
        compile(driver, "<native-M04-driver>", "exec")
    registry_root = r"Software\AITS-DEVX015-Test-" + uuid.uuid4().hex
    crypto_path = r"SOFTWARE\Microsoft\Cryptography"
    with pytest.raises(FileNotFoundError):
        winreg.OpenKey(
            winreg.HKEY_CURRENT_USER, registry_root, 0,
            winreg.KEY_READ | winreg.KEY_WOW64_64KEY,
        )
    created: list[str] = []
    outputs = []
    try:
        # Unique exact allowlist; no broad registry-tree cleanup or shared key.
        for suffix in (
            "",
            "SOFTWARE",
            r"SOFTWARE\AITradingSystem",
            HOST_REGISTRY_KEY,
            r"SOFTWARE\Microsoft",
            crypto_path,
        ):
            key_path = registry_root + ("\\" + suffix if suffix else "")
            with winreg.CreateKeyEx(
                winreg.HKEY_CURRENT_USER,
                key_path,
                0,
                winreg.KEY_ALL_ACCESS | winreg.KEY_WOW64_64KEY,
            ):
                created.append(key_path)
        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            crypto_path,
            0,
            winreg.KEY_READ | winreg.KEY_WOW64_64KEY,
        ) as original:
            machine_guid, kind = winreg.QueryValueEx(original, "MachineGuid")
        with winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            registry_root + "\\" + crypto_path,
            0,
            winreg.KEY_SET_VALUE | winreg.KEY_WOW64_64KEY,
        ) as isolated:
            winreg.SetValueEx(isolated, "MachineGuid", 0, kind, machine_guid)
        with winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            registry_root + "\\" + HOST_REGISTRY_KEY,
            0,
            winreg.KEY_SET_VALUE | winreg.KEY_WOW64_64KEY,
        ) as isolated:
            winreg.SetValueEx(
                isolated, HOST_REGISTRY_VALUE, 0, winreg.REG_SZ, json.dumps(registration)
            )
        environment = dict(
            os.environ, PYTHONPATH=str(POLICY_PATH.parents[2] / "src"), PYTHONDONTWRITEBYTECODE="1"
        )
        for checkout in (repo, other) if competition is None else ():
            before = {path: path.read_bytes() for path in control.rglob("*") if path.is_file()}
            result = subprocess.run(
                [sys.executable, "-c", driver, registry_root, str(checkout), str(POLICY_PATH)],
                env=environment,
                capture_output=True,
                text=True,
                timeout=30,
            )
            outputs.append(
                {"returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
            )
            assert result.returncode == 0, outputs[-1]
            assert {
                path: path.read_bytes() for path in control.rglob("*") if path.is_file()
            } == before
            assert not (checkout / "forbidden-legacy-store").exists()
        if competition is None:
            registered, denied = [json.loads(row["stdout"]) for row in outputs]
            assert registered["status"] == "REGISTERED"
            assert registered["store"] == control.as_posix()
            assert registered["checkout"] == repo.as_posix()
            assert denied["status"] == "DENIED"
            assert "HOST_REPOSITORY_NOT_REGISTERED" in denied["reason"]
            assert registered["pid"] != denied["pid"]
        else:
            from concurrent.futures import ThreadPoolExecutor

            children = []
            reader = ThreadPoolExecutor(max_workers=2)
            before_git = []
            for checkout in enrolled:
                index_path = subprocess.check_output(
                    ["git", "-C", str(checkout), "rev-parse", "--git-path", "index"],
                    text=True,
                ).strip()
                before_git.append(
                    (
                        subprocess.check_output(["git", "-C", str(checkout), "show-ref", "--head"]),
                        (checkout / index_path).read_bytes(),
                        checkout / index_path,
                    )
                )

            def receive(child):
                line = reader.submit(child.stdout.readline).result(timeout=30)
                assert line, f"child {child.pid} closed stdout; terminal stderr retained"
                row = json.loads(line)
                outputs.append(row)
                if row.get("status") == "CLI_RESULT":
                    return receive(child)
                return row

            def send(child, command):
                child.stdin.write(command + "\n")
                child.stdin.flush()

            try:
                for index, checkout in enumerate(enrolled):
                    child = subprocess.Popen(
                        [
                            sys.executable,
                            "-c",
                            driver,
                            registry_root,
                            str(checkout),
                            str(POLICY_PATH),
                            competition.rsplit("-", 1)[-1],
                            str(index),
                        ],
                        env=dict(
                            environment, GIT_TRACE2_EVENT=str(tmp_path / f"git-{index}.jsonl")
                        ),
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                    )
                    children.append(child)
                    row = receive(child)
                    assert row["status"] == "REGISTERED" and row["store"] == control.as_posix(), (
                        "NATIVE_REGISTERED_STORE_MUST_BE_SHARED", row
                    )
                assert outputs[0]["pid"] != outputs[1]["pid"]
                send(children[0], "ACQUIRE")
                assert receive(children[0])["status"] == "PASS"
                send(children[1], "ACQUIRE")
                denied = receive(children[1])
                assert denied["status"] == "BLOCKED" and denied["active"] == 1, denied
                if competition == "linked-fence":
                    assert denied["reasons"] == ["PUBLICATION_LEASE_CONFLICT"], denied
                    assert not (
                        enrolled[1]
                        / "outputs/architecture"
                        / "arch_005_integration_publication_fence/transactions/native-1"
                        / "transaction.json"
                    ).exists()
                else:
                    assert any(
                        reason.startswith("LEASE_RESOURCE_CONFLICT:")
                        for reason in denied["reasons"]
                    )
                assert all(
                    not (checkout / "outputs/native-proof").exists() for checkout in enrolled
                )
                send(children[0], "EXECUTE")
                assert receive(children[0])["status"] == "DONE"
                send(children[1], "RETRY")
                assert receive(children[1])["status"] == "PASS"
                send(children[1], "EXECUTE")
                assert receive(children[1]) == {
                    "pid": outputs[1]["pid"],
                    "status": "DONE",
                    "active": 0,
                }
                for child in children:
                    stdout, stderr = child.communicate(timeout=30)
                    assert child.returncode == 0, stdout + stderr
                for checkout in enrolled:
                    if competition != "linked-fence":
                        assert (checkout / "outputs/native-proof/effect").read_text() == "once"
                    assert not (checkout / "forbidden-legacy-store").exists()
                for number, checkout in enumerate(enrolled):
                    refs, index_bytes, index_path = before_git[number]
                    assert (
                        subprocess.check_output(
                            ["git", "-C", str(checkout), "show-ref", "--head"],
                        )
                        == refs
                    )
                    assert index_path.read_bytes() == index_bytes
                    trace = [
                        json.loads(line)
                        for line in (tmp_path / f"git-{number}.jsonl")
                        .read_text(encoding="utf-8")
                        .splitlines()
                    ]
                    assert not [
                        row
                        for row in trace
                        if row.get("event") == "cmd_name"
                        and row.get("name") in {"add", "commit", "update-ref", "push"}
                    ]
                if competition == "linked-fence":
                    commands = [row for row in outputs if row.get("status") == "CLI_RESULT"]
                    assert [row["returncode"] for row in commands] == [0, 2, 0, 0, 0]
                    assert [row["argv"][0] for row in commands] == [
                        "acquire",
                        "acquire",
                        "release",
                        "acquire",
                        "release",
                    ]
            finally:
                for child in children:
                    if child.poll() is None:
                        child.kill()
                    if child.stdout is not None and not child.stdout.closed:
                        stdout, stderr = child.communicate(timeout=30)
                        outputs.append(
                            {
                                "phase": "terminal",
                                "pid": child.pid,
                                "returncode": child.returncode,
                                "stdout": stdout,
                                "stderr": stderr,
                            }
                        )
                reader.shutdown(wait=True)
    finally:
        (tmp_path / "real-registry-driver.py").write_text(driver, encoding="utf-8")
        (tmp_path / "real-registry-results.json").write_text(json.dumps(outputs), encoding="utf-8")
        if created:
            _remove_native_registry_fixture(winreg, registry_root)


def test_managed_host_rejects_unregistered_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ai_trading_system.platform.architecture.workflow_coordination import (
        coordinated_lease_store,
    )

    _repo, _root, policy, _state = _registered_control(tmp_path, monkeypatch)
    other = tmp_path / "not-enrolled"
    subprocess.run(["git", "init", str(other)], check=True, capture_output=True)
    with pytest.raises(ParallelControlError, match="HOST_REPOSITORY_NOT_REGISTERED"):
        coordinated_lease_store(
            other, tmp_path / "bypass", policy=policy, entrypoint="checkout-guard"
        )
    assert not (tmp_path / "bypass").exists()


@pytest.mark.parametrize(
    "case",
    ["active", "done", "dropped", "revoked", "uncommitted", "scope-tamper"],
)
def test_full_task_admission_uses_current_committed_canonical_state(
    canonical_merge_repository,
    case: str,
) -> None:
    from test_devx015_workflow_integration import TASK, _git

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
        PublicationFenceError,
    )
    from ai_trading_system.platform.architecture.task_registry_canonical import (
        validate_canonical_registry,
    )
    from scripts.run_validation_tier import (
        _full_task_commitment,
        _validate_publication_transaction_for_full,
        parse_args,
    )

    root, _scope = canonical_merge_repository
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    if case == "uncommitted":
        _git(root, "add", ".")
        _git(root, "commit", "-m", "freeze pre-change task")
    if case not in {"active", "scope-tamper"}:
        options = ["--notes", "actual task gate mutation"]
        if case in {"done", "dropped"}:
            options = ["--status", case.upper()]
        elif case == "revoked":
            authority = dict(
                validate_canonical_registry(project_root=root).fragment(TASK)["task_record"][
                    "workflow_authority"
                ]
            )
            authority["status"] = "REVOKED"
            target = root / "outputs/revoked-authority.json"
            target.write_text(json.dumps(authority), encoding="utf-8")
            options = ["--workflow-authority", str(target)]
        changed = subprocess.run(
            [
                sys.executable,
                "scripts/architecture_arch005_task_source.py",
                "update",
                "--task-id",
                TASK,
                "--actor",
                "integration-coordinator",
                "--change-id",
                "full-task-gate-" + case,
                "--occurred-at",
                datetime.now(UTC).isoformat(),
                "--base-commit",
                _git(root, "rev-parse", "HEAD"),
                "--publication-transaction",
                str(transaction),
                *options,
            ],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert changed.returncode == 0, changed.stdout + changed.stderr
    if case in {"dropped", "revoked"}:
        for phase in ("GENERATED_REBUILD_PRE", "GENERATED_REBUILD_POST", "CANDIDATE_COMMIT_PRE"):
            fence.checkpoint(
                transaction,
                phase=phase,
                actor="integration-coordinator",
                generator_ids=("canonical-task-source",) if phase.startswith("GENERATED_") else (),
            )
    if case != "uncommitted":
        _git(root, "add", ".")
        _git(root, "commit", "-m", "freeze task gate candidate")
    candidate = _git(root, "rev-parse", "HEAD")
    if case == "scope-tamper":
        authority = validate_canonical_registry(project_root=root).fragment(TASK)["task_record"][
            "workflow_authority"
        ]
        scope_path = root / authority["scope_ref"]["path"]
        scope_path.write_bytes(scope_path.read_bytes() + b" ")
    before_refs = _git(root, "show-ref")
    index = root / ".git/index"
    before_index = index.read_bytes()
    expected = {
        "dropped": "PUBLICATION_FULL_TASK_DROPPED",
        "revoked": "PUBLICATION_FULL_TASK_AUTHORITY_REVOKED",
        "uncommitted": "PUBLICATION_FULL_TASK_NOT_CANDIDATE",
        "scope-tamper": "PUBLICATION_FULL_TASK_SCOPE_INVALID",
    }.get(case)
    if expected is None:
        commitment = _full_task_commitment(root, TASK, candidate)
        assert commitment["task_id"] == TASK
        assert commitment["status"] == ("DONE" if case == "done" else "IN_PROGRESS")
    else:
        with pytest.raises(PublicationFenceError, match=expected):
            _full_task_commitment(root, TASK, candidate)
    if case in {"dropped", "revoked"}:
        fence.checkpoint(
            transaction,
            phase="FORMAL_VALIDATION_PRE",
            actor="integration-coordinator",
        )
        before = fence.replay(transaction)
        args = parse_args(["full", "--publication-transaction", str(transaction)])
        with pytest.raises(PublicationFenceError, match=expected):
            _validate_publication_transaction_for_full(
                args,
                repo_root=root,
                validation_provenance={"task_id": TASK},
                full_run_id="must-not-dispatch",
            )
        assert fence.replay(transaction) == before
    assert not (transaction.parent / "full_dispatch_claim.json").exists()
    assert fence.guard.store.replay().active_leases[0].execution is None
    assert _git(root, "show-ref") == before_refs and index.read_bytes() == before_index


@pytest.mark.parametrize("technical_status", ["PASS", "FAIL"])
@pytest.mark.parametrize("execution_state", ["missing", "uncommitted"])
@pytest.mark.parametrize("canonical_merge_repository", ["full-recovery"], indirect=True)
def test_new_full_claim_rejects_direct_result_without_execution(
    canonical_merge_repository,
    technical_status: str,
    execution_state: str,
) -> None:
    """Existing public fence API must not accept a summary without actual custody."""
    from test_devx015_workflow_integration import _git

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
        PublicationFenceError,
    )

    root, _scope = canonical_merge_repository
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    actor = "integration-coordinator"
    for phase in ("GENERATED_REBUILD_PRE", "GENERATED_REBUILD_POST", "CANDIDATE_COMMIT_PRE"):
        fence.checkpoint(
            transaction,
            phase=phase,
            actor=actor,
            generator_ids=("canonical-task-source",) if phase.startswith("GENERATED_") else (),
        )
    _git(root, "add", ".")
    _git(root, "commit", "-m", "freeze actual direct Full result admission fixture")
    fence.checkpoint(transaction, phase="FORMAL_VALIDATION_PRE", actor=actor)
    fence.checkpoint(
        transaction,
        phase="FULL_DISPATCHED",
        actor=actor,
        full_run_id="no-execution",
    )
    dispatched = fence.replay(transaction)
    assert dispatched.events[-1]["payload"]["dispatch_request"]["schema_version"] == (
        "integration_publication_full_dispatch.v2"
    )
    lease = fence.guard.store.replay().active_leases[0]
    assert lease.execution is None
    evidence = root / "outputs/validation_runtime/untrusted/test_runtime_summary.json"
    evidence.parent.mkdir(parents=True)
    raw = json.dumps({"git_commit": dispatched.candidate_sha, "status": technical_status}).encode()
    evidence.write_bytes(raw)
    expected = "PUBLICATION_FULL_EXECUTION_REQUIRED"
    if execution_state == "uncommitted":
        from test_arch_005_integration_publication_fence import (
            _record_contained_full_fixture_result,
        )

        _record_contained_full_fixture_result(
            fence,
            transaction,
            evidence,
            status=technical_status,
            commit_result=False,
        )
        lease = fence.guard.store.replay().active_leases[0]
        assert lease.execution["state"] == "RESULT_RECORDED"
        assert lease.execution.get("full_result_commitment") is None
        expected = "PUBLICATION_FULL_COMMITMENT_REQUIRED"
    before = fence.replay(transaction)
    refs = _git(root, "show-ref")
    index = root / _git(root, "rev-parse", "--git-path", "index")
    index_raw = index.read_bytes()
    with pytest.raises(PublicationFenceError, match=expected):
        fence.checkpoint(
            transaction,
            phase="FORMAL_VALIDATION_RESULT",
            actor=actor,
            evidence_paths=(evidence,),
            validation_status=technical_status,
        )
    assert fence.replay(transaction) == before
    assert fence.guard.store.replay().active_leases[0] == lease
    assert evidence.read_bytes() == raw
    assert _git(root, "show-ref") == refs and index.read_bytes() == index_raw
    fence.release(transaction, actor=actor, outcome="failed")


@pytest.mark.parametrize("projection_written", [False, True], ids=["event", "projection"])
@pytest.mark.parametrize("canonical_merge_repository", ["full-recovery"], indirect=True)
def test_public_full_claim_recovery_after_actual_dispatcher_dies_before_reservation(
    canonical_merge_repository,
    projection_written: bool,
) -> None:
    import argparse

    from test_devx015_workflow_integration import TASK, _git

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
        PublicationFenceError,
    )
    from ai_trading_system.platform.artifacts import canonical_json_bytes
    from scripts.run_validation_tier import _FullCommandRunner

    root, _scope = canonical_merge_repository
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    actor = "integration-coordinator"
    for phase in ("GENERATED_REBUILD_PRE", "GENERATED_REBUILD_POST", "CANDIDATE_COMMIT_PRE"):
        fence.checkpoint(
            transaction,
            phase=phase,
            actor=actor,
            generator_ids=("canonical-task-source",) if phase.startswith("GENERATED_") else (),
        )
    _git(root, "add", ".")
    _git(root, "commit", "-m", "freeze actual Full claim recovery fixture")
    fence.checkpoint(transaction, phase="FORMAL_VALIDATION_PRE", actor=actor)
    refs = _git(root, "show-ref")
    index = root / _git(root, "rev-parse", "--git-path", "index")
    index_raw = index.read_bytes()
    script = (
        "import json,os,sys\nfrom pathlib import Path\n"
        "from ai_trading_system.platform.architecture.integration_publication_fence "
        "import IntegrationPublicationFence\n"
        "from ai_trading_system.platform.architecture.workflow_execution "
        "import current_process_identity\n"
        "original=IntegrationPublicationFence._persist_full_claim\n"
        "def fault(self,replay):\n"
        f" if {projection_written!r}: original(self,replay)\n"
        " print('FULL_LAUNCHER='+json.dumps(current_process_identity()),flush=True)\n"
        " assert sys.stdin.readline().strip()=='exit'\n"
        " os._exit(47)\n"
        "IntegrationPublicationFence._persist_full_claim=fault\n"
        f"IntegrationPublicationFence(project_root=Path.cwd()).checkpoint(Path({str(transaction)!r}),"
        "phase='FULL_DISPATCHED',actor='integration-coordinator',full_run_id='claim-recovery')\n"
    )
    cli = [
        sys.executable,
        "scripts/run_validation_tier.py",
        "full",
        "--recover-full",
        "--publication-transaction",
        str(transaction),
        "--task-id",
        TASK,
    ]
    oracle = NativeOracle()
    with subprocess.Popen(
        [sys.executable, "-c", script],
        cwd=root,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as launcher:
        assert launcher.stdout is not None
        line = launcher.stdout.readline()
        assert line.startswith("FULL_LAUNCHER="), line + launcher.stderr.read()
        announced = json.loads(line.removeprefix("FULL_LAUNCHER="))
        with oracle.process(announced["pid"]) as handle:
            assert announced["creation_time"] == oracle.creation_time(handle)
            assert not oracle.exited(handle)
            before = fence.replay(transaction)
            assert before.phase == "FULL_DISPATCHED"
            claim = before.events[-1]["payload"]["dispatch_request"]
            assert claim["launcher"] == announced
            with pytest.raises(PublicationFenceError, match="PUBLICATION_FULL_LAUNCHER_MISMATCH"):
                fence.require_full_launcher(transaction)
            binding = fence.validate(transaction, require_candidate=True)
            unauthorized_directory = root / "outputs/validation_runtime/wrong-launcher"
            runner = _FullCommandRunner(
                args=argparse.Namespace(publication_transaction=transaction),
                root=root,
                artifact_dir=unauthorized_directory,
                publication_binding=binding,
                provenance={"task_id": TASK},
            )
            with pytest.raises(PublicationFenceError, match="PUBLICATION_FULL_LAUNCHER_MISMATCH"):
                runner(
                    [sys.executable, "-c", "raise AssertionError('must not dispatch')"],
                    cwd=root,
                )
            assert not unauthorized_directory.exists()
            assert fence.guard.store.replay().active_leases[0].execution is None
            observed = subprocess.run(cli, cwd=root, capture_output=True, text=True, timeout=60)
            assert observed.returncode == 2, observed.stdout + observed.stderr
            assert json.loads(observed.stdout)["status"] == "OBSERVE_ONLY"
            assert fence.replay(transaction) == before
            stdout, stderr = launcher.communicate(input="exit\n", timeout=60)
            assert launcher.returncode == 47, stdout + stderr
            assert oracle.exited(handle)
    head = fence.guard.store.replay().active_leases[0]
    assert head.execution is None
    projection = transaction.parent / "full_dispatch_claim.json"
    assert projection.exists() is projection_written
    frozen_raw = canonical_json_bytes(claim)
    projection.write_bytes(frozen_raw + b" ")
    rejected = subprocess.run(cli, cwd=root, capture_output=True, text=True, timeout=60)
    assert rejected.returncode == 2 and "PUBLICATION_FULL_CLAIM_CHANGED" in rejected.stderr
    assert fence.replay(transaction) == before
    if projection_written:
        projection.write_bytes(frozen_raw)
    else:
        projection.unlink()  # Only the newly injected synthetic projection.
    recovered = subprocess.run(cli, cwd=root, capture_output=True, text=True, timeout=60)
    assert recovered.returncode == 0, recovered.stdout + recovered.stderr
    result = json.loads(recovered.stdout)
    assert result["status"] == "RECOVERED_FAILED_ATTEMPT"
    assert result["technical_status"] == "NOT_EXECUTED"
    assert not result["dispatch_performed"] and not result["publication_allowed"]
    after = fence.replay(transaction)
    assert after.phase == "FAILED" and after.events[-1]["payload"]["outcome"] == "FAILED"
    leases = fence.guard.store.replay()
    assert not leases.active_leases
    assert leases.lease_heads[0].execution is None and leases.lease_heads[0].state == "RELEASED"
    assert projection.read_bytes() == frozen_raw
    again = subprocess.run(cli, cwd=root, capture_output=True, text=True, timeout=60)
    assert again.returncode == 0, again.stdout + again.stderr
    assert fence.replay(transaction) == after and fence.guard.store.replay() == leases
    assert _git(root, "show-ref") == refs and index.read_bytes() == index_raw


@pytest.mark.parametrize(
    "boundary",
    [
        "reserved",
        "request",
        "created",
        "bound",
        "resume-intent",
        "running",
        "running-held-job",
        "exit-unrecorded",
        "exit-0",
        "exit-7",
        "summary-0",
        "summary-7",
        "custody-0",
        "custody-7",
    ],
)
@pytest.mark.parametrize("canonical_merge_repository", ["full-recovery"], indirect=True)
def test_public_full_uncommitted_crash_recovery_closes_without_adopting_loose_pass(
    canonical_merge_repository,
    boundary: str,
) -> None:
    from test_devx015_workflow_integration import TASK, _git

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )
    from scripts.run_validation_tier import _full_task_commitment

    root, _scope = canonical_merge_repository
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    actor = "integration-coordinator"
    for phase in ("GENERATED_REBUILD_PRE", "GENERATED_REBUILD_POST", "CANDIDATE_COMMIT_PRE"):
        fence.checkpoint(
            transaction,
            phase=phase,
            actor=actor,
            generator_ids=("canonical-task-source",) if phase.startswith("GENERATED_") else (),
        )
    _git(root, "add", ".")
    _git(root, "commit", "-m", "freeze actual uncommitted Full recovery fixture")
    fence.checkpoint(transaction, phase="FORMAL_VALIDATION_PRE", actor=actor)
    binding = fence.validate(transaction, require_candidate=True)
    binding["task_commitment"] = _full_task_commitment(root, TASK, str(binding["candidate_sha"]))
    binding["pre_dispatch_readiness"] = {
        "fixture_scope": "uncommitted-recovery-not-formal-readiness",
    }
    directory = root / "outputs/validation_runtime/uncommitted"
    exit_code = 7 if boundary.endswith("-7") else 0
    worker = (
        "import os,time\nfrom pathlib import Path\n"
        f"with Path({str(directory / 'child-started')!r}).open('a') as journal:\n"
        " journal.write(str(os.getpid())+'\\n')\n"
        + ("time.sleep(120)\n" if boundary.startswith("running") else "")
        + f"print('original pytest-like result'); raise SystemExit({exit_code})\n"
    )
    script = (
        "import argparse,json,os,sys,time\nfrom pathlib import Path\n"
        "from scripts.run_validation_tier import _FullCommandRunner\n"
        "from ai_trading_system.platform.architecture.integration_publication_fence "
        "import IntegrationPublicationFence\n"
        "from ai_trading_system.platform.architecture.workflow_coordination "
        "import ExecutionLifecycle\n"
        "from ai_trading_system.platform.architecture.workflow_execution "
        "import WindowsJobProcess,current_process_identity\n"
        f"boundary={boundary!r}\ndirectory=Path({str(directory)!r})\n"
        "def crash():\n"
        " print('FULL_LAUNCHER='+json.dumps(current_process_identity()),flush=True)\n"
        " assert sys.stdin.readline().strip()=='exit'\n"
        " os._exit(47)\n"
        "if boundary=='reserved':\n"
        " original=ExecutionLifecycle.reserve\n"
        " def fault(self,*args,**kwargs):\n"
        "  original(self,*args,**kwargs)\n"
        "  crash()\n"
        " ExecutionLifecycle.reserve=fault\n"
        "elif boundary in {'request','created'}:\n"
        " original=WindowsJobProcess.create\n"
        " def fault(*args,**kwargs):\n"
        "  if boundary=='created': process=original(*args,**kwargs)\n"
        "  crash()\n"
        " WindowsJobProcess.create=fault\n"
        "elif boundary=='bound':\n"
        " original=ExecutionLifecycle.bind\n"
        " def fault(self,*args,**kwargs):\n"
        "  original(self,*args,**kwargs)\n"
        "  crash()\n"
        " ExecutionLifecycle.bind=fault\n"
        "elif boundary=='resume-intent':\n"
        " original=ExecutionLifecycle._append\n"
        " def fault(self,head,value,now):\n"
        "  updated=original(self,head,value,now)\n"
        "  if value['state']=='RESUME_INTENT': crash()\n"
        "  return updated\n"
        " ExecutionLifecycle._append=fault\n"
        "elif boundary.startswith('running'):\n"
        " original=ExecutionLifecycle.resume\n"
        " def fault(self,*args,**kwargs):\n"
        "  original(self,*args,**kwargs)\n"
        "  deadline=time.monotonic()+30\n"
        "  while not (directory/'child-started').exists():\n"
        "   assert time.monotonic()<deadline\n"
        "   time.sleep(0.02)\n"
        "  crash()\n"
        " ExecutionLifecycle.resume=fault\n"
        "elif boundary.startswith('exit-'):\n"
        " original=ExecutionLifecycle.confirm_exit\n"
        " def fault(self,*args,**kwargs):\n"
        "  if boundary!='exit-unrecorded': original(self,*args,**kwargs)\n"
        "  crash()\n"
        " ExecutionLifecycle.confirm_exit=fault\n"
        f"transaction=Path({str(transaction)!r})\n"
        "IntegrationPublicationFence(project_root=Path.cwd()).checkpoint(transaction,"
        "phase='FULL_DISPATCHED',actor='integration-coordinator',full_run_id='uncommitted')\n"
        "runner=_FullCommandRunner(args=argparse.Namespace(publication_transaction=transaction),"
        "root=Path.cwd(),artifact_dir=directory,"
        f"publication_binding={binding!r},provenance={{'task_id':{TASK!r}}})\n"
        f"result=runner([sys.executable,'-c',{worker!r}],cwd=Path.cwd())\n"
        "assert boundary.startswith(('summary-','custody-'))\n"
        "uncommitted_status='FAIL' if boundary=='custody-7' else 'PASS'\n"
        f"summary={{**result,'git_commit':{binding['candidate_sha']!r},"
        "'status':uncommitted_status}\n"
        "(directory/'test_runtime_summary.json').write_text(json.dumps(summary),encoding='utf-8')\n"
        "request=runner.request\n"
        "loose={key:request[key] for key in "
        "('candidate_sha','validation_identity_sha256','request_id')}\n"
        "loose.update(schema_version='full_execution_result.v1',status=uncommitted_status)\n"
        "(directory/'execution_result.json').write_text(json.dumps(loose),encoding='utf-8')\n"
        "if boundary.startswith('custody-'):\n"
        " runner.fence.guard.store.execution_lifecycle().record_result(request['lease_id'],"
        "actor='integration-coordinator',result_path=directory/'execution_result.json')\n"
        "crash()\n"
    )
    cli = [
        sys.executable,
        "scripts/run_validation_tier.py",
        "full",
        "--recover-full",
        "--publication-transaction",
        str(transaction),
        "--task-id",
        TASK,
    ]
    oracle = NativeOracle()
    with subprocess.Popen(
        [sys.executable, "-c", script],
        cwd=root,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as launcher:
        assert launcher.stdout is not None
        for line in launcher.stdout:
            if line.startswith("FULL_LAUNCHER="):
                announced = json.loads(line.removeprefix("FULL_LAUNCHER="))
                break
        else:
            pytest.fail("uncommitted barrier missing: " + launcher.stderr.read())
        with oracle.process(announced["pid"]) as handle:
            assert oracle.creation_time(handle) == announced["creation_time"]
            assert not oracle.exited(handle)
            original_head = fence.guard.store.replay().active_leases[0]
            assert original_head.execution["launcher"] == announced
            with ExitStack() as stack:
                if boundary == "running-held-job":
                    job = oracle.api.OpenJobObjectW(
                        0x0004,
                        False,
                        original_head.execution["request"]["job_name"],
                    )
                    assert job
                    stack.callback(oracle.api.CloseHandle, job)
                if boundary.startswith("running"):
                    process = stack.enter_context(
                        oracle.process(
                            int((directory / "child-started").read_text()),
                        )
                    )
                    assert not oracle.exited(process)
                    oracle.assert_in_job(process, original_head.execution["request"]["job_name"])
                observed = subprocess.run(cli, cwd=root, capture_output=True, text=True, timeout=60)
                assert observed.returncode == 2, observed.stdout + observed.stderr
                assert json.loads(observed.stdout)["status"] == "OBSERVE_ONLY"
                assert fence.guard.store.replay().active_leases[0] == original_head
                stdout, stderr = launcher.communicate(input="exit\n", timeout=60)
                assert launcher.returncode == 47, stdout + stderr
                assert oracle.exited(handle)
                if boundary == "running-held-job":
                    assert not oracle.exited(process), "held Job control failed to retain worker"
                    before_termination = {
                        path.name: path.read_bytes()
                        for path in directory.iterdir()
                        if path.is_file()
                    }
                    waiting = subprocess.run(
                        cli,
                        cwd=root,
                        capture_output=True,
                        text=True,
                        timeout=60,
                    )
                    assert waiting.returncode == 2, waiting.stdout + waiting.stderr
                    assert json.loads(waiting.stdout)["status"] == "RECOVERY_REQUIRED"
                    assert "terminate_frozen_job" in json.loads(waiting.stdout)["allowed_actions"]
                    assert fence.guard.store.replay().active_leases[0] == original_head
                    terminated = subprocess.run(
                        [*cli, "--recover-full-action", "terminate_frozen_job"],
                        cwd=root,
                        capture_output=True,
                        text=True,
                        timeout=60,
                    )
                    assert terminated.returncode == 0, terminated.stdout + terminated.stderr
                    assert json.loads(terminated.stdout)["technical_status"] == "INSUFFICIENT"
                    assert {
                        path.name: path.read_bytes()
                        for path in directory.iterdir()
                        if path.is_file()
                    } == before_termination
                if boundary.startswith("running"):
                    assert oracle.exited(process, 10)
    oracle.assert_job_absent(original_head.execution["request"]["job_name"])
    retained = {path.name: path.read_bytes() for path in directory.iterdir() if path.is_file()}
    if "child-started" in retained:
        assert len(retained["child-started"].splitlines()) == 1
    refs = _git(root, "show-ref")
    index = root / _git(root, "rev-parse", "--git-path", "index")
    original_index = index.read_bytes()
    recovered = subprocess.run(cli, cwd=root, capture_output=True, text=True, timeout=60)
    assert recovered.returncode == 0, recovered.stdout + recovered.stderr
    report = json.loads(recovered.stdout)
    assert report["status"] == "RECOVERED_FAILED_ATTEMPT"
    assert report["technical_status"] == "INSUFFICIENT"
    assert not report["dispatch_performed"] and not report["publication_allowed"]
    expected_code = (
        exit_code
        if boundary
        in {
            "exit-0",
            "exit-7",
            "summary-0",
            "summary-7",
            "custody-0",
            "custody-7",
        }
        else None
    )
    assert report["original_exit"]["returncode"] == expected_code
    terminal = fence.guard.store.replay()
    assert not terminal.active_leases and terminal.lease_heads[0].state == "RELEASED"
    execution = terminal.lease_heads[0].execution
    assert execution["state"] == "RESULT_RECORDED"
    if boundary.startswith("custody-"):
        assert execution == original_head.execution  # Preserve unadopted custody, even loose PASS.
        assert execution["result"]["status"] == ("PASS" if exit_code == 0 else "FAIL")
        assert execution.get("full_result_commitment") is None
    else:
        assert execution["result"]["status"] == "INSUFFICIENT"
        assert execution["result"]["artifact"] is None
    after = fence.replay(transaction)
    assert after.phase == "FAILED"
    assert not any(row["phase"] == "FORMAL_VALIDATION_RESULT" for row in after.events)
    repeated = subprocess.run(cli, cwd=root, capture_output=True, text=True, timeout=60)
    assert repeated.returncode == 0, repeated.stdout + repeated.stderr
    assert fence.replay(transaction) == after and fence.guard.store.replay() == terminal
    assert {
        path.name: path.read_bytes() for path in directory.iterdir() if path.is_file()
    } == retained
    assert _git(root, "show-ref") == refs and index.read_bytes() == original_index


def _seed_readiness_atlas_inputs(root: Path) -> list[dict[str, object]]:
    """Frozen baseline samples for an isolated Atlas fixture, never new authority.

    Keep the reviewed policies and original sealed task events unchanged. Only
    their disposable fixture index/views and new-C render are rebuilt. The
    original repository is read-only; no research result is recomputed.
    """
    import re

    from ai_trading_system.platform.architecture import task_registry_canonical as canonical
    from ai_trading_system.platform.architecture import validation_readiness as readiness
    from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes
    from ai_trading_system.yaml_loader import load_strict_yaml_text

    source = Path(__file__).resolve().parents[1]
    baseline = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source).decode().strip()
    forbidden = "docs/research/growth_tilt_owner_diagnosis_pack.md"
    captures: dict[str, bytes] = {}

    def read(relative: str, expected_sha: str | None = None, *, install: bool = True) -> bytes:
        assert relative != forbidden and ".." not in relative.split("/")
        assert "\\" not in relative and ":" not in relative
        assert relative.startswith(
            ("config/", "inputs/", "docs/", "registry/", "scripts/", "outputs/", "tests/")
        ), relative
        if relative in captures:
            raw = captures[relative]
        else:
            result = subprocess.run(
                ["git", "show", baseline + ":" + relative],
                cwd=source,
                capture_output=True,
                timeout=30,
            )
            if result.returncode:
                # Only an explicit frozen binding may request retained untracked
                # bytes. No directory scan, inferred path, or download is allowed.
                assert expected_sha is not None, result.stderr.decode(errors="replace")
                raw = bounded_regular_bytes(source / relative)
            else:
                raw = result.stdout
            captures[relative] = raw
            assert len(captures) <= 512 and sum(map(len, captures.values())) <= 64 * 1024 * 1024
        if expected_sha is not None:
            assert hashlib.sha256(raw).hexdigest() == expected_sha, relative
        if install:
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(raw)
        return raw

    def yaml_file(relative: str):
        return load_strict_yaml_text(read(relative).decode(), label=relative)

    page = yaml_file("config/atlas/page_effectiveness.yaml")
    config_names = (
        subprocess.check_output(
            ["git", "ls-tree", "-r", "--name-only", baseline, "--", "config/atlas"],
            cwd=source,
        )
        .decode()
        .splitlines()
    )
    for relative in config_names:
        read(relative)
    for relative in page["relevant_source_paths"]:
        if relative.startswith(("src/", "registry/")) or relative == canonical.CANONICAL_INDEX_PATH:
            continue  # Current fixture runtime and canonical index, not old generated bytes.
        if relative.startswith("scripts/") and (root / relative).exists():
            continue
        read(relative)
    for row in page["task_sources"]:
        read(row["requirement_path"])
    # The frozen TRADING-2564 requirement enables the real S2a compatibility
    # section. Preserve its missing source inputs, not synthetic hash stand-ins.
    for relative in (
        "tests/test_named_immutable_publication.py",
        "tests/test_devx_006d_report_catalog_flow_authority.py",
    ):
        read(relative)
    # Use the frozen official source inventory for the whole transitive chain.
    # Metadata is captured but never installed as current generated authority.
    compatibility_index = json.loads(
        read(
            "inputs/architecture/devx_006c_compatibility_authority_index.json",
            install=False,
        )
    )
    entries = [
        row
        for row in compatibility_index["entries"]
        if row["section_id"].startswith("phase_trading_2564_")
        or row["section_id"] == "phase_trading_2560_composer_known_snapshot_capture_v1"
    ]
    assert len(entries) == 10
    source_paths = set()
    for entry in entries:
        section = json.loads(read(entry["fragment_path"], entry["fragment_sha256"], install=False))[
            "section"
        ]
        source_paths.update(section["superseded_live_source_paths"])
    for relative in sorted(source_paths):
        if relative.startswith(("src/", "registry/")) or relative == canonical.CANONICAL_INDEX_PATH:
            continue
        if not (root / relative).exists():
            read(relative)
    registry = yaml_file("config/atlas/source_registry.yaml")
    for row in registry["sources"]:
        read(row["source_path"])
    adapters = yaml_file("config/atlas/historical_source_adapters.yaml")
    for row in adapters["adapters"]:
        read(row["source_path"])
    read("config/research/evidence_first_research_portfolio_v1.yaml")
    read(
        "inputs/research/qqq_options/"
        "trading_2541_exact_date_subscription_recovery_execution_v3/export_safe_terminal_evidence.json"
    )
    admission = yaml_file(
        "config/research/frozen_signal_value_confirmation_result_admission_v1.yaml"
    )
    for row in [
        admission["authorization_binding"],
        admission["manifest_binding"],
        *admission["evidence_bindings"],
    ]:
        read(row["path"], row["file_sha256"])
    for relative in readiness.RESULT_ADMISSIONS:
        if relative in captures:
            policy = load_strict_yaml_text(captures[relative].decode(), label=relative)
            for row in policy["evidence_bindings"]:
                read(row["path"], row["file_sha256"])
    if readiness.O1_POLICY in captures:
        policy = load_strict_yaml_text(captures[readiness.O1_POLICY].decode(), label="frozen O1")
        gate = policy["isolated_dq_evidence"]["gate"]
        read(gate["path"], gate["sha256"])

    # Select exact sealed fragments from the frozen index, not Markdown rows.
    index_raw = subprocess.check_output(
        ["git", "show", baseline + ":" + canonical.CANONICAL_INDEX_PATH],
        cwd=source,
    )
    index = load_strict_yaml_text(index_raw.decode(), label="frozen Atlas task index")
    selected = {row["task_id"] for row in page["task_sources"]}
    records = [dict(row) for row in index["fragments"] if row["task_id"] in selected]
    assert {row["task_id"] for row in records} == selected
    commit_objects = {}
    for row in records:
        fragment = load_strict_yaml_text(
            read(row["path"], row["file_sha256"]).decode(),
            label=row["path"],
        )
        for event in fragment["events"]:
            if event.get("occurred_at") is not None:
                continue
            commit = event["base_commit"]
            assert re.fullmatch(r"[0-9a-f]{40}", commit)
            if commit in commit_objects:
                continue
            raw_commit = subprocess.check_output(
                ["git", "cat-file", "commit", commit], cwd=source, timeout=30
            )
            assert len(raw_commit) <= 1024 * 1024 and len(commit_objects) < 512
            stored = (
                subprocess.run(
                    ["git", "hash-object", "-t", "commit", "-w", "--stdin"],
                    input=raw_commit,
                    cwd=root,
                    capture_output=True,
                    check=True,
                    timeout=30,
                )
                .stdout.decode()
                .strip()
            )
            assert stored == commit
            # Commit metadata only: no tree/blob traversal or refs to old history.
            commit_objects[commit] = hashlib.sha256(raw_commit).hexdigest()
            # Git show --no-patch still parses immediate parents. Preserve those
            # exact metadata objects too; never traverse their trees or history.
            for line in raw_commit.split(b"\n\n", 1)[0].splitlines():
                if not line.startswith(b"parent "):
                    continue
                parent = line.removeprefix(b"parent ").decode("ascii")
                assert re.fullmatch(r"[0-9a-f]{40}", parent)
                if parent in commit_objects:
                    continue
                parent_raw = subprocess.check_output(
                    ["git", "cat-file", "commit", parent], cwd=source, timeout=30
                )
                assert len(parent_raw) <= 1024 * 1024 and len(commit_objects) < 512
                stored_parent = (
                    subprocess.run(
                        ["git", "hash-object", "-t", "commit", "-w", "--stdin"],
                        input=parent_raw,
                        cwd=root,
                        capture_output=True,
                        check=True,
                        timeout=30,
                    )
                    .stdout.decode()
                    .strip()
                )
                assert stored_parent == parent
                commit_objects[parent] = hashlib.sha256(parent_raw).hexdigest()
    tree_objects = {}
    for commit in commit_objects:
        tree = (
            subprocess.check_output(
                ["git", "rev-parse", commit + "^{tree}"], cwd=source, timeout=30
            )
            .decode()
            .strip()
        )
        # ls-tree reads directory metadata, never file blob contents. Git show -s
        # inspects these trees even though it emits only the commit timestamp.
        rows = (
            subprocess.check_output(
                ["git", "ls-tree", "-r", "-t", "--format=%(objecttype) %(objectname)", tree],
                cwd=source,
                timeout=30,
            )
            .decode()
            .splitlines()
        )
        trees = {tree, *(row.split()[1] for row in rows if row.startswith("tree "))}
        for object_id in sorted(trees):
            if object_id in tree_objects:
                continue
            raw_tree = subprocess.check_output(
                ["git", "cat-file", "tree", object_id], cwd=source, timeout=30
            )
            assert len(tree_objects) < 4096 and len(raw_tree) <= 1024 * 1024
            stored_tree = (
                subprocess.run(
                    ["git", "hash-object", "-t", "tree", "-w", "--stdin"],
                    input=raw_tree,
                    cwd=root,
                    capture_output=True,
                    check=True,
                    timeout=30,
                )
                .stdout.decode()
                .strip()
            )
            assert stored_tree == object_id
            tree_objects[object_id] = hashlib.sha256(raw_tree).hexdigest()
    capsule = {
        "schema_version": "devx015_atlas_fixture_inputs.v1",
        "source_commit": baseline,
        "source_index_sha256": hashlib.sha256(index_raw).hexdigest(),
        "original_commit_metadata_sha256": commit_objects,
        "original_tree_metadata_sha256": tree_objects,
        "scope": "ISOLATED_FROZEN_TEST_INPUTS_NOT_NEW_RESEARCH_OR_TASK_AUTHORITY",
        "files": [
            {"path": path, "sha256": hashlib.sha256(raw).hexdigest(), "size_bytes": len(raw)}
            for path, raw in sorted(captures.items())
        ],
        "forbidden_content_reads": 0,
    }
    (root.parent / "atlas-input-capsule.json").write_text(json.dumps(capsule), encoding="utf-8")
    return records


def _seed_readiness_retained_evidence(root: Path) -> None:
    """Construct real committed-policy/raw-byte inputs, not checker return values.

    These explicitly synthetic research payloads only exercise engineering hash
    admission in the disposable repository. They authorize no research or DQ.
    """
    from ai_trading_system.platform.architecture import validation_readiness as readiness

    def bind(relative: str, raw: bytes) -> dict[str, object]:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        return {"path": relative, "sha256": hashlib.sha256(raw).hexdigest(), "size_bytes": len(raw)}

    def policy(relative: str, value: dict[str, object]) -> None:
        bind(relative, (json.dumps(value, sort_keys=True) + "\n").encode())

    retained = bind("outputs/readiness-fixture/retained.json", b'{"engineering_fixture":true}\n')
    authority = {**retained, "file_sha256": retained["sha256"]}
    for relative in readiness.RESULT_ADMISSIONS:
        policy(relative, {"evidence_bindings": [authority]})
    policy(readiness.O1_POLICY, {"isolated_dq_evidence": {"gate": retained}})
    package_root = "outputs/readiness-fixture/signal-package"
    daily = bind(package_root + "/daily/one.json", b'{"engineering_fixture":true}\n')
    receipt = {
        "daily_signal_artifacts": [{**daily, "relative_path": "daily/one.json"}],
        "source_artifact": {**retained, "locator": retained["path"]},
    }
    package: dict[str, object] = {"root": package_root}
    for name in ("package_receipt", "signal_index", "run_manifest"):
        raw = (
            json.dumps(
                receipt
                if name == "package_receipt"
                else {
                    "engineering_fixture": True,
                },
                sort_keys=True,
            )
            + "\n"
        ).encode()
        binding = bind(package_root + "/" + name + ".json", raw)
        package[name + "_sha256"] = binding["sha256"]
    policy(readiness.SIGNAL_POLICY, {"authority_bindings": [authority], "signal_package": package})


@pytest.mark.parametrize("canonical_merge_repository", ["full-readiness"], indirect=True)
def test_whole_readiness_fixture_uses_actual_seven_checkers(canonical_merge_repository) -> None:
    """Real seven-checker positive fixture; not original-project Full/V3 acceptance."""
    from test_devx015_workflow_integration import _git

    root, _scope = canonical_merge_repository
    # Canonical task updates invalidate the compatibility projection; use its
    # actual official builder before C, never patch the readiness adapter.
    generated = subprocess.run(
        [
            sys.executable,
            "scripts/architecture_compatibility_authority.py",
            "build",
            "--repository-root",
            str(root),
        ],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert generated.returncode == 0, generated.stdout + generated.stderr
    _git(root, "add", ".")
    _git(root, "commit", "-m", "freeze real readiness fixture inputs")
    candidate = _git(root, "rev-parse", "HEAD")
    before_refs = _git(root, "show-ref")
    before_index = (root / ".git/index").read_bytes()
    rendered = subprocess.run(
        [
            sys.executable,
            "scripts/render_atlas_strategy_research_page.py",
            "--repository-root",
            str(root),
            "--exact-commit",
            candidate,
        ],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=180,
    )
    (root.parent / "atlas-render.stdout.log").write_text(rendered.stdout, encoding="utf-8")
    (root.parent / "atlas-render.stderr.log").write_text(rendered.stderr, encoding="utf-8")
    assert rendered.returncode == 0, rendered.stdout + rendered.stderr
    # This fresh process resolves the inspector from this committed fixture's
    # own src. The complete actual checker inventory runs without monkeypatches.
    checked = subprocess.run(
        [
            sys.executable,
            "scripts/validation_readiness.py",
            "--repository-root",
            str(root),
            "--candidate-sha",
            candidate,
        ],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=120,
    )
    (root.parent / "actual-readiness.json").write_text(checked.stdout, encoding="utf-8")
    (root.parent / "actual-readiness.stderr.log").write_text(checked.stderr, encoding="utf-8")
    assert checked.returncode == 0, checked.stdout + checked.stderr
    result = json.loads(checked.stdout)
    assert [row["checker_id"] for row in result["checks"]] == [
        "candidate_identity",
        "retained_evidence",
        "canonical_tasks",
        "atlas_final_binding",
        "architecture_generated",
        "report_flow_authority",
        "compatibility_authority",
    ], [(row["checker_id"], row["code"], row["detail"][:1600]) for row in result["blockers"]]
    assert _git(root, "show-ref") == before_refs
    assert (root / ".git/index").read_bytes() == before_index
    assert result["dispatch_performed"] is False and result["artifacts_written"] is False
    assert result["status"] == "PASS" and result["full_dispatch_ready"] is True, result
    assert all(row["status"] == "PASS" for row in result["checks"]), result
    assert result["blockers"] == []


def _run_actual_profile_full(
    root: Path,
    *,
    filtered: bool = False,
    whole_readiness: bool = True,
    readiness_fault: str = "none",
    # Hang guard only: the inner mandatory Full takes 170-520s serially and more
    # under formal-tier load (16 workers); it is not a semantic threshold.
    driver_timeout: int = 1200,
    live_observer=None,
) -> tuple[dict[str, object], Path, str, dict[str, str]]:
    """Real canonical/Job/profile chain; isolated probes, not original V3 oracles."""
    from test_devx015_workflow_integration import TASK, _git
    from test_validation_runtime_profile import (
        _full_validation_provenance,
        _write_complete_profile,
    )

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )
    from ai_trading_system.platform.architecture.workflow_contract import canonical_digest
    from scripts.run_validation_tier import _full_task_commitment

    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    duration = root / "inputs/architecture/arch_004g2_full_duration_profile.yaml"
    test_files = [
        "tests/test_full_job.py",
        *[f"tests/test_full_job_{index:02d}.py" for index in range(1, 16)],
    ]
    if whole_readiness:
        from ai_trading_system.yaml_loader import safe_load_yaml_path

        manifest = safe_load_yaml_path(root / "inputs/architecture/arch_004e_test_manifest.yaml")
        test_files = [row["path"] for row in manifest["tests"] if row["file_role"] == "test"]
        assert len(test_files) >= 16
    _write_complete_profile(
        duration,
        nodeids=[path + "::test_required" for path in test_files],
        observed_seconds=dict.fromkeys(test_files, 1.0),
    )
    if not whole_readiness:
        (root / "inputs/architecture/arch_004e_test_manifest.yaml").write_text(
            json.dumps(
                {
                    "status": "PASS",
                    "test_count": 16,
                    "tests": [{"path": path, "file_role": "test"} for path in test_files],
                }
            ),
            encoding="utf-8",
        )
    generators = tuple(fence.replay(transaction).transaction["generator_ids"])
    for phase in ("GENERATED_REBUILD_PRE", "GENERATED_REBUILD_POST", "CANDIDATE_COMMIT_PRE"):
        if phase == "GENERATED_REBUILD_POST" and whole_readiness:
            generated = subprocess.run(
                [
                    sys.executable,
                    "scripts/architecture_compatibility_authority.py",
                    "build",
                    "--repository-root",
                    str(root),
                ],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=60,
            )
            assert generated.returncode == 0, generated.stdout + generated.stderr
        fence.checkpoint(
            transaction,
            phase=phase,
            actor="integration-coordinator",
            generator_ids=generators if phase.startswith("GENERATED_") else (),
        )
    _git(root, "add", ".")
    _git(root, "commit", "-m", "freeze actual Full profile publication boundary fixture")
    fence.checkpoint(transaction, phase="FORMAL_VALIDATION_PRE", actor="integration-coordinator")
    binding = fence.validate(transaction, require_candidate=True)
    binding["task_commitment"] = _full_task_commitment(root, TASK, str(binding["candidate_sha"]))
    binding["pre_dispatch_readiness"] = {"fixture_scope": "profile-not-whole-readiness"}
    if whole_readiness:
        rendered = subprocess.run(
            [
                sys.executable,
                "scripts/render_atlas_strategy_research_page.py",
                "--repository-root",
                str(root),
                "--exact-commit",
                str(binding["candidate_sha"]),
            ],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=180,
        )
        (root.parent / "full-atlas-render.stdout.log").write_text(rendered.stdout, encoding="utf-8")
        (root.parent / "full-atlas-render.stderr.log").write_text(rendered.stderr, encoding="utf-8")
        assert rendered.returncode == 0, rendered.stdout + rendered.stderr
        checked = subprocess.run(
            [
                sys.executable,
                "scripts/validation_readiness.py",
                "--repository-root",
                str(root),
                "--candidate-sha",
                str(binding["candidate_sha"]),
            ],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=120,
        )
        (root.parent / "full-readiness.json").write_text(checked.stdout, encoding="utf-8")
        assert checked.returncode == 0, checked.stdout + checked.stderr
        binding["pre_dispatch_readiness"] = json.loads(checked.stdout)
        if readiness_fault == "missing-checker":
            binding["pre_dispatch_readiness"]["checks"] = [
                row
                for row in binding["pre_dispatch_readiness"]["checks"]
                if row["checker_id"] != "atlas_final_binding"
            ]
        else:
            assert readiness_fault == "none"
    provenance = {**_full_validation_provenance(), "task_id": TASK}
    run_id = "formal-profile"
    job_name = "Local\\AITS-DEVX015-full-" + canonical_digest(
        {
            "transaction": binding["transaction_sha256"],
            "full_run_id": run_id,
        }
    )
    directory = root / "outputs/validation_runtime/mandatory"
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-n",
        "16",
        "--dist",
        "loadfile",
        "--no-loadscope-reorder",
        "-p",
        "scripts.pytest_runtime_profile",
        "--aits-duration-profile",
        str(duration),
        *test_files,
        "-q",
        *(["-k", "test_required"] if filtered else []),
    ]
    script = (
        "import argparse,json,faulthandler\nfrom pathlib import Path\n"
        "from datetime import UTC,datetime\n"
        "faulthandler.enable()\n"
        "from scripts.run_validation_tier import (_FullCommandRunner,"
        "_run_mandatory_acceptance_command,"
        "_read_runtime_profile_payload,_summarize_runtime_profile,_runtime_payload,TIER_SPECS)\n"
        "from scripts.run_validation_tier import _write_runtime_artifacts\n"
        "from ai_trading_system.platform.architecture.integration_publication_fence "
        "import IntegrationPublicationFence\n"
        "from ai_trading_system.platform.architecture.workflow_execution "
        "import bind_mandatory_acceptance\n"
        f"transaction=Path({str(transaction)!r})\nbinding={binding!r}\n"
        f"provenance={provenance!r}\ndirectory=Path({str(directory)!r})\n"
        "fence=IntegrationPublicationFence(project_root=Path.cwd())\n"
        "fence.checkpoint(transaction,phase='FULL_DISPATCHED',actor='integration-coordinator',"
        f"full_run_id={run_id!r})\n"
        "mandatory=bind_mandatory_acceptance(Path.cwd(),binding['candidate_sha'])\n"
        "binding['mandatory_acceptance_binding']=mandatory\n"
        "runner=_FullCommandRunner(args=argparse.Namespace(publication_transaction=transaction),"
        "root=Path.cwd(),artifact_dir=directory,publication_binding=binding,provenance=provenance)\n"
        "started=datetime.now(UTC)\n"
        f"result=_run_mandatory_acceptance_command({command!r},cwd=Path.cwd(),binding=mandatory,"
        f"expected_collections=16,env_overrides={{'DEVX015_EXPECTED_JOB':{job_name!r},"
        "'DEVX015_FAIL':'0','AITS_PYTEST_RUNTIME_PROFILE_OUTPUT':"
        "str(directory/'test_runtime_profile.json'),"
        f"'AITS_PYTEST_RUNTIME_PROFILE_FORMAL_SELECTION':{str(int(not filtered))!r},"
        "'AITS_VALIDATION_PROVENANCE_JSON':json.dumps(provenance)},command_runner=runner)\n"
        "assert result['exit_code']==0,result\n"
        "profile=directory/'test_runtime_profile.json'\n"
        "checked=_read_runtime_profile_payload(profile,pytest_exitstatus=0,expected_worker_count=16,"
        f"expected_dist='loadfile',formal_selection_eligible={not filtered!r},"
        f"duration_profile_path=Path({str(duration)!r}),"
        f"expected_test_files=set({test_files!r}),expected_validation_provenance=provenance)\n"
        "result.update(_summarize_runtime_profile(checked,final_path=profile,repo_root=Path.cwd()))\n"
        "payload=_runtime_payload(repo_root=Path.cwd(),requested_tier='full',resolved_tier='full',"
        f"spec=TIER_SPECS['full'],command={command!r},workers='16',dist='loadfile',status='PASS',"
        "started_at=started,ended_at=datetime.now(UTC),result=result,artifact_dir=directory,"
        "validation_provenance=provenance)\n"
        "payload['publication_transaction']=binding\n"
        "summary=directory/'test_runtime_summary.json'\n"
        "_write_runtime_artifacts(directory,payload,repo_root=Path.cwd(),"
        "pytest_output=result['pytest_output'])\n"
        "runner.record_summary(summary,status='PASS')\n"
        "fence.checkpoint(transaction,phase='FORMAL_VALIDATION_RESULT',"
        "actor='integration-coordinator',evidence_paths=(summary,),validation_status='PASS')\n"
    )
    environment = dict(os.environ)
    for name in (
        "PYTEST_ADDOPTS",
        "PYTEST_CURRENT_TEST",
        "PYTEST_XDIST_WORKER",
        "PYTEST_XDIST_WORKER_COUNT",
        "AITS_MANDATORY_ACCEPTANCE_REQUEST",
    ):
        environment.pop(name, None)
    if live_observer is None:
        completed = subprocess.run(
            [sys.executable, "-c", script], cwd=root, env=environment,
            capture_output=True, text=True, timeout=driver_timeout,
        )
    else:
        with subprocess.Popen(
            [sys.executable, "-c", script], cwd=root, env=environment,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        ) as process:
            observation_error = None
            try:
                try:
                    live_observer(process, fence, environment)
                except Exception as error:
                    observation_error = error
                stdout, stderr = process.communicate(timeout=driver_timeout)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=30)
            completed = subprocess.CompletedProcess(
                process.args, process.returncode, stdout, stderr,
            )
            (root.parent / "observed-full-driver.stdout.log").write_text(stdout, encoding="utf-8")
            (root.parent / "observed-full-driver.stderr.log").write_text(stderr, encoding="utf-8")
            if observation_error is not None:
                raise observation_error
    assert completed.returncode == 0, completed.stdout + completed.stderr
    return binding, directory, script, environment


@pytest.mark.parametrize("canonical_merge_repository", ["full-readiness-profile"], indirect=True)
def test_original_full_cannot_authorize_changed_candidate(canonical_merge_repository) -> None:
    """One original Full, then actual new Git candidate and public admission refusal."""
    from test_devx015_workflow_integration import TASK, _git

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )

    root, _scope = canonical_merge_repository
    binding, directory, script, environment = _run_actual_profile_full(root)
    (root.parent / "full-driver.py").write_text(script, encoding="utf-8")
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    inspected = subprocess.run(
        [sys.executable, "scripts/run_validation_tier.py", "full",
         "--inspect-full-publication-profile", "--publication-transaction", str(transaction),
         "--task-id", TASK],
        cwd=root, env=environment, capture_output=True, text=True, timeout=720,
    )
    (root.parent / "original-full-inspection.stdout.log").write_text(
        inspected.stdout, encoding="utf-8"
    )
    (root.parent / "original-full-inspection.stderr.log").write_text(
        inspected.stderr, encoding="utf-8"
    )
    assert inspected.returncode == 0, inspected.stdout + inspected.stderr
    assert json.loads(inspected.stdout)["candidate_sha"] == binding["candidate_sha"]
    evidence_before = {
        str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in directory.rglob("*") if path.is_file()
    }
    before = fence.replay(transaction)
    assert before.phase == "FORMAL_VALIDATION_RESULT"
    original_main = _git(root, "rev-parse", "main")
    changed_path = root / "candidate-after-full.txt"
    changed_path.write_text(
        "A different candidate requires its own validation.\n", encoding="utf-8"
    )
    _git(root, "add", "candidate-after-full.txt")
    _git(root, "commit", "-m", "V02 fixture candidate changes after original Full")
    changed_candidate = _git(root, "rev-parse", "HEAD")
    assert changed_candidate != binding["candidate_sha"]
    admission = subprocess.run(
        [sys.executable, "scripts/architecture_arch005_publication_fence.py", "checkpoint",
         "--transaction", str(transaction), "--phase", "LOCAL_MAIN_FF_PRE",
         "--actor", "integration-coordinator"],
        cwd=root, env=environment, capture_output=True, text=True, timeout=720,
    )
    (root.parent / "changed-candidate-admission.json").write_text(
        json.dumps({"original_candidate": binding["candidate_sha"],
                    "changed_candidate": changed_candidate, "main": original_main,
                    "exit_code": admission.returncode, "stdout": admission.stdout,
                    "stderr": admission.stderr, "original_evidence": evidence_before}),
        encoding="utf-8",
    )
    assert admission.returncode == 2, admission.stdout + admission.stderr
    assert "PUBLICATION_CANDIDATE_DRIFT" in admission.stdout + admission.stderr
    assert fence.replay(transaction) == before
    assert _git(root, "rev-parse", "main") == original_main
    assert _git(root, "rev-parse", "HEAD") == changed_candidate
    assert evidence_before == {
        str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in directory.rglob("*") if path.is_file()
    }


@pytest.mark.parametrize("canonical_merge_repository", ["full-readiness-profile"], indirect=True)
def test_original_full_rejects_changed_publication_identities(canonical_merge_repository) -> None:
    """One real C/V, independent mutable faults, original CLI refusals and final admission."""
    from test_devx015_workflow_integration import TASK, _git

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )
    from ai_trading_system.yaml_loader import safe_load_yaml_path

    root, _scope = canonical_merge_repository
    binding, directory, script, environment = _run_actual_profile_full(root)
    (root.parent / "full-driver.py").write_text(script, encoding="utf-8")
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    before = fence.replay(transaction)
    assert before.phase == "FORMAL_VALIDATION_RESULT"
    refs = _git(root, "show-ref")
    index = (root / ".git/index").read_bytes()
    evidence = {
        str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in directory.rglob("*") if path.is_file()
    }
    command = [
        sys.executable, "scripts/architecture_arch005_publication_fence.py", "checkpoint",
        "--transaction", str(transaction), "--phase", "LOCAL_MAIN_FF_PRE",
        "--actor", "integration-coordinator",
    ]

    def run_probe(name: str, argv: list[str], env: dict[str, str]) -> subprocess.CompletedProcess:
        started = time.perf_counter()
        result = subprocess.run(
            argv, cwd=root, env=env, capture_output=True, text=True, timeout=720,
        )
        (root.parent / (name + ".json")).write_text(
            json.dumps({"argv": argv, "exit_code": result.returncode,
                        "stdout": result.stdout, "stderr": result.stderr,
                        "elapsed_seconds": time.perf_counter() - started,
                        "original_candidate": binding["candidate_sha"]}),
            encoding="utf-8",
        )
        return result

    def unchanged() -> None:
        assert fence.replay(transaction) == before
        assert _git(root, "show-ref") == refs
        assert (root / ".git/index").read_bytes() == index
        assert evidence == {
            str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in directory.rglob("*") if path.is_file()
        }

    inspected = run_probe("original-inspection", [
        sys.executable, "scripts/run_validation_tier.py", "full",
        "--inspect-full-publication-profile", "--publication-transaction", str(transaction),
        "--task-id", TASK,
    ], environment)
    assert inspected.returncode == 0, inspected.stdout + inspected.stderr
    unchanged()

    # Fault injection is startup code for these isolated publication processes,
    # not a patched identity gate or a write to installed dependency files.
    bootstrap = root.parent / "loaded-origin-fault"
    bootstrap.mkdir()
    (bootstrap / "sitecustomize.py").write_text(
        "import hashlib,json,marshal,os\nfrom pathlib import Path\n"
        "from yaml.reader import Reader\n"
        "original=Reader.peek.__code__\n"
        "Reader.peek.__code__=original.replace(co_filename='V02_CHANGED_LOADED_ORIGIN')\n"
        "Path(__file__).with_name(str(os.getpid())+'.json').write_text(json.dumps({"
        "'pid':os.getpid(),'old_filename':original.co_filename,"
        "'new_filename':Reader.peek.__code__.co_filename,"
        "'old_code_sha':hashlib.sha256(marshal.dumps(original)).hexdigest(),"
        "'new_code_sha':hashlib.sha256(marshal.dumps(Reader.peek.__code__)).hexdigest()}))\n",
        encoding="utf-8",
    )
    fault_environment = dict(environment)
    fault_environment["PYTHONPATH"] = str(bootstrap) + os.pathsep + environment["PYTHONPATH"]
    loaded = run_probe("loaded-origin-admission", command, fault_environment)
    assert loaded.returncode == 2, loaded.stdout + loaded.stderr
    assert "PUBLICATION_FULL_CLOSURE_INVALID" in loaded.stdout + loaded.stderr
    # The isolated (-I) inspector never loads caller startup code, so only the
    # original CLI is faulted and must reject itself via loaded-source custody.
    assert "ACCEPTANCE_LOADED_CODE_ORIGIN" in loaded.stdout + loaded.stderr
    witnesses = [json.loads(path.read_bytes()) for path in bootstrap.glob("*.json")]
    assert len(witnesses) == 1
    assert all(row["old_code_sha"] != row["new_code_sha"] for row in witnesses)
    assert all(row["new_filename"] == "V02_CHANGED_LOADED_ORIGIN" for row in witnesses)
    unchanged()

    duration = root / "inputs/architecture/arch_004g2_full_duration_profile.yaml"
    duration_value = safe_load_yaml_path(duration)
    duration_value["files"][0]["observed_seconds"] += 1.0
    policy = root / "config/architecture/arch_005_integration_publication_fence.yaml"
    policy_value = safe_load_yaml_path(policy)
    policy_value["version"] = "1.0.1"
    generator = root / "scripts/architecture_compatibility_authority.py"
    generator_raw = generator.read_bytes()
    assert b"raise SystemExit(main())" in generator_raw
    selection = root / "inputs/architecture/arch_004e_test_manifest.yaml"
    selection_value = safe_load_yaml_path(selection)
    removed = next(row for row in selection_value["tests"] if row["file_role"] == "test")
    selection_value["tests"].remove(removed)
    selection_value["test_count"] -= 1
    selection_value["test_file_count"] -= 1
    faults = [
        ("duration-input", duration, json.dumps(duration_value).encode(),
         "PUBLICATION_CANDIDATE_DIRTY"),
        ("publication-policy", policy, json.dumps(policy_value).encode(),
         "PUBLICATION_POLICY_HASH_MISMATCH"),
        ("generator-entry", generator,
         generator_raw.replace(b"raise SystemExit(main())", b"raise SystemExit(99)"),
         "PUBLICATION_CANDIDATE_DIRTY"),
        ("runner-selection", selection, json.dumps(selection_value).encode(),
         "PUBLICATION_CANDIDATE_DIRTY"),
    ]
    for name, path, changed, reason in faults:
        original = path.read_bytes()
        assert original != changed
        (root.parent / (name + "-fault.json")).write_text(json.dumps({
            "path": path.relative_to(root).as_posix(),
            "original_sha": hashlib.sha256(original).hexdigest(),
            "changed_sha": hashlib.sha256(changed).hexdigest(),
        }), encoding="utf-8")
        try:
            path.write_bytes(changed)
            rejected = run_probe(name + "-admission", command, environment)
            assert rejected.returncode == 2, rejected.stdout + rejected.stderr
            assert reason in rejected.stdout + rejected.stderr
        finally:
            path.write_bytes(original)
        unchanged()
    accepted = run_probe("restored-original-admission", command, environment)
    assert accepted.returncode == 0, accepted.stdout + accepted.stderr
    assert fence.replay(transaction).phase == "LOCAL_MAIN_FF_PRE"
    assert _git(root, "show-ref") == refs
    assert (root / ".git/index").read_bytes() == index
    assert evidence == {
        str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in directory.rglob("*") if path.is_file()
    }


@pytest.mark.parametrize("readiness_fault", ["none", "missing-checker"])
@pytest.mark.parametrize("canonical_merge_repository", ["full-readiness-profile"], indirect=True)
def test_actual_full_profile_preserves_real_whole_readiness(
    canonical_merge_repository,
    readiness_fault: str,
) -> None:
    """Complete real input/Job/profile chain, not original-project V3 acceptance."""
    from test_devx015_workflow_integration import TASK, _git

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )

    root, _scope = canonical_merge_repository
    binding, directory, script, environment = _run_actual_profile_full(
        root,
        whole_readiness=True,
        readiness_fault=readiness_fault,
    )
    (root.parent / "full-driver.py").write_text(script, encoding="utf-8")
    readiness = json.loads((root.parent / "full-readiness.json").read_bytes())
    identity = json.loads((directory / "execution_validation_identity.json").read_bytes())
    summary = json.loads((directory / "test_runtime_summary.json").read_bytes())
    assert readiness["status"] == "PASS" and readiness["full_dispatch_ready"] is True
    assert len(readiness["checks"]) == 7 and all(
        row["status"] == "PASS" for row in readiness["checks"]
    )
    recorded = json.loads(json.dumps(readiness))
    if readiness_fault == "missing-checker":
        recorded["checks"] = [
            row for row in recorded["checks"] if row["checker_id"] != "atlas_final_binding"
        ]
    assert identity["pre_dispatch_readiness"] == recorded
    assert identity["candidate_sha"] == readiness["candidate_sha"] == binding["candidate_sha"]
    assert summary["mandatory_acceptance"]["status"] == "PASS"
    assert summary["status"] == "PASS" and summary["exit_code"] == 0
    assert summary["can_support_promotion_evidence"] is True
    probes = json.loads((root.parent / "executed-engineering-probes.json").read_bytes())["tests"]
    witnesses = list(directory.glob("probe-*.json"))
    assert len(witnesses) == len(probes)
    assert {json.loads(path.read_bytes())["worker"] for path in witnesses} == {
        f"gw{index}" for index in range(16)
    }
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    refs = _git(root, "show-ref")
    inspected = subprocess.run(
        [
            sys.executable,
            "scripts/run_validation_tier.py",
            "full",
            "--inspect-full-publication-profile",
            "--publication-transaction",
            str(transaction),
            "--task-id",
            TASK,
        ],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=720,
    )
    (root.parent / "full-inspection.stdout.log").write_text(inspected.stdout, encoding="utf-8")
    (root.parent / "full-inspection.stderr.log").write_text(inspected.stderr, encoding="utf-8")
    if readiness_fault == "none":
        assert inspected.returncode == 0, inspected.stdout + inspected.stderr
        fence.checkpoint(transaction, phase="LOCAL_MAIN_FF_PRE", actor="integration-coordinator")
    else:
        before = fence.replay(transaction)
        admission = subprocess.run(
            [
                sys.executable,
                "scripts/architecture_arch005_publication_fence.py",
                "checkpoint",
                "--transaction",
                str(transaction),
                "--phase",
                "LOCAL_MAIN_FF_PRE",
                "--actor",
                "integration-coordinator",
            ],
            cwd=root,
            env=environment,
            capture_output=True,
            text=True,
            timeout=720,
        )
        (root.parent / "full-admission.stdout.log").write_text(admission.stdout, encoding="utf-8")
        (root.parent / "full-admission.stderr.log").write_text(admission.stderr, encoding="utf-8")
        assert admission.returncode == 2, admission.stdout + admission.stderr
        assert "PUBLICATION_FULL_CLOSURE_INVALID" in admission.stdout + admission.stderr
        assert inspected.returncode == 2, inspected.stdout + inspected.stderr
        assert fence.replay(transaction) == before
    assert _git(root, "show-ref") == refs


@pytest.mark.parametrize("filtered", [False, True], ids=["formal", "filtered"])
@pytest.mark.parametrize("canonical_merge_repository", ["full-profile"], indirect=True)
def test_actual_pytest_pass_with_invalid_formal_profile_cannot_publish(
    canonical_merge_repository,
    filtered: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Actual profile/Job evidence; other Full-readiness obligations remain separate."""
    from test_devx015_workflow_integration import TASK, _git

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        FULL_PROFILE_INSPECTION_TIMEOUT_SECONDS,
        IntegrationPublicationFence,
        PublicationFenceError,
    )

    root, _scope = canonical_merge_repository
    binding, directory, script, environment = _run_actual_profile_full(root, filtered=filtered)
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    summary = directory / "test_runtime_summary.json"
    raw = summary.read_bytes()
    payload = json.loads(raw)
    profile = json.loads((directory / "test_runtime_profile.json").read_bytes())
    assert payload["status"] == "PASS" and payload["exit_code"] == 0
    assert payload["mandatory_acceptance"]["status"] == "PASS"
    assert {
        json.loads(path.read_bytes())["worker"]
        for path in [*directory.glob("job-witness*.json"), *directory.glob("probe-*.json")]
    } == {f"gw{index}" for index in range(16)}
    assert profile["profile_status"] == profile["telemetry_status"] == "PASS", profile
    assert profile["performance_evidence_status"] == ("FAIL" if filtered else "PASS"), profile
    assert payload["can_support_promotion_evidence"] is (not filtered)
    # Retain exact source/test/driver alongside the original raw result; the
    # invocation uses a unique retained --basetemp, never pytest's rolling temp pool.
    evidence = directory / "source-evidence"
    evidence.mkdir()
    evidence_sources = {
        "fence.py": root
        / ("src/ai_trading_system/platform/architecture/integration_publication_fence.py"),
        "runner.py": root / "scripts/run_validation_tier.py",
        "test_harness.py": Path(__file__),
    }
    for name, path in evidence_sources.items():
        (evidence / name).write_bytes(path.read_bytes())
    (evidence / "driver.py").write_text(script, encoding="utf-8")
    (evidence / "identity.json").write_text(
        json.dumps(
            {
                "candidate_sha": binding["candidate_sha"],
                "filtered": filtered,
                "source_sha256": {
                    path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in evidence.iterdir()
                    if path.is_file()
                },
            }
        ),
        encoding="utf-8",
    )
    before = fence.replay(transaction)
    lease = fence.guard.store.replay().active_leases[0]
    refs = _git(root, "show-ref")
    inspected = subprocess.run(
        [
            sys.executable,
            "scripts/run_validation_tier.py",
            "full",
            "--inspect-full-publication-profile",
            "--publication-transaction",
            str(transaction),
            "--task-id",
            TASK,
        ],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        # Same budget the fence grants this inspection (observed ~59s serially).
        timeout=FULL_PROFILE_INSPECTION_TIMEOUT_SECONDS,
    )
    assert inspected.returncode == (2 if filtered else 0), inspected.stdout + inspected.stderr
    (evidence / "inspection.stdout.log").write_text(inspected.stdout, encoding="utf-8")
    (evidence / "inspection.stderr.log").write_text(inspected.stderr, encoding="utf-8")
    assert fence.replay(transaction) == before
    assert fence.guard.store.replay().active_leases[0] == lease
    if not filtered:
        for path in (
            directory / "test_runtime_profile.json",
            directory / "execution_validation_identity.json",
        ):
            original = path.read_bytes()
            try:
                # Still valid JSON with identical values: raw custody must win.
                path.write_bytes(original + b"\n")
                with pytest.raises(PublicationFenceError, match="PUBLICATION_FULL_CLOSURE_INVALID"):
                    fence.checkpoint(
                        transaction,
                        phase="LOCAL_MAIN_FF_PRE",
                        actor="integration-coordinator",
                    )
                assert fence.replay(transaction) == before
                assert fence.guard.store.replay().active_leases[0] == lease
            finally:
                path.write_bytes(original)
        profile_path = directory / "test_runtime_profile.json"
        original_profile = profile_path.read_bytes()
        prepare = fence._prepare_profile_checkpoint

        def change_after_real_inspection(*args, **kwargs):
            observation = prepare(*args, **kwargs)
            profile_path.write_bytes(original_profile + b"\n")
            return observation

        try:
            with monkeypatch.context() as race:
                race.setattr(fence, "_prepare_profile_checkpoint", change_after_real_inspection)
                with pytest.raises(PublicationFenceError, match="PUBLICATION_FULL_CLOSURE_INVALID"):
                    fence.checkpoint(
                        transaction,
                        phase="LOCAL_MAIN_FF_PRE",
                        actor="integration-coordinator",
                    )
            assert fence.replay(transaction) == before
            assert fence.guard.store.replay().active_leases[0] == lease
        finally:
            profile_path.write_bytes(original_profile)
    if filtered:
        with pytest.raises(PublicationFenceError, match="PUBLICATION_FULL_CLOSURE_INVALID"):
            fence.checkpoint(
                transaction,
                phase="LOCAL_MAIN_FF_PRE",
                actor="integration-coordinator",
            )
        assert fence.replay(transaction) == before
        assert fence.guard.store.replay().active_leases[0] == lease
    else:
        fence.checkpoint(transaction, phase="LOCAL_MAIN_FF_PRE", actor="integration-coordinator")
    assert summary.read_bytes() == raw and _git(root, "show-ref") == refs
    fence.release(transaction, actor="integration-coordinator", outcome="failed")


@pytest.mark.parametrize("input_kind", ["retained", "atlas"])
@pytest.mark.parametrize("canonical_merge_repository", ["full-profile"], indirect=True)
def test_readiness_input_replaced_after_real_inspection_cannot_publish(
    canonical_merge_repository,
    input_kind: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """V03/V02: actual ignored input drift in the inspection-to-atomic seam."""
    from test_devx015_workflow_integration import _git

    from ai_trading_system.atlas.page_effectiveness import load_page_effectiveness_policy
    from ai_trading_system.contracts.strategy_research_page_effectiveness import (
        StrategyResearchPageEffectivenessManifest,
    )
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
        PublicationFenceError,
    )
    from ai_trading_system.platform.architecture.validation_readiness import O1_POLICY
    from ai_trading_system.yaml_loader import safe_load_yaml_path

    root, _scope = canonical_merge_repository
    binding, directory, driver, _environment = _run_actual_profile_full(root)
    (root.parent / "readiness-race-driver.py").write_text(driver, encoding="utf-8")
    (root.parent / "readiness-race-harness.py").write_bytes(Path(__file__).read_bytes())
    if input_kind == "retained":
        relative = safe_load_yaml_path(root / O1_POLICY)["isolated_dq_evidence"]["gate"]["path"]
    else:
        policy = load_page_effectiveness_policy(repository_root=root)
        manifest = StrategyResearchPageEffectivenessManifest.from_json_bytes(
            (root / policy.manifest_path).read_bytes()
        )
        relative = next(
            row.locator for row in manifest.rendered_artifacts if row.locator.endswith("index.html")
        )
    assert relative.startswith("outputs/") and ".." not in Path(relative).parts
    target = root / relative
    original = target.read_bytes()
    changed = original + b"\n"
    assert _git(root, "check-ignore", "--", relative) == relative
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    before = fence.replay(transaction)
    lease = fence.guard.store.replay().active_leases[0]
    refs = _git(root, "show-ref")
    index = (root / ".git/index").read_bytes()
    full_records = {
        name: (directory / name).read_bytes()
        for name in (
            "test_runtime_summary.json",
            "execution_validation_identity.json",
            "execution_result.json",
        )
    }
    prepare = fence._prepare_profile_checkpoint
    observations = []

    def replace_after_actual_probe(*args, **kwargs):
        observation = prepare(*args, **kwargs)
        assert observation is not None
        parsed = json.loads(observation[3])
        assert parsed["status"] == "PASS" and parsed["candidate_sha"] == binding["candidate_sha"]
        observations.append(parsed)
        (root.parent / "readiness-race-inspection.json").write_bytes(observation[3])
        target.write_bytes(changed)
        return observation

    try:
        with monkeypatch.context() as race:
            race.setattr(fence, "_prepare_profile_checkpoint", replace_after_actual_probe)
            with pytest.raises(PublicationFenceError, match="PUBLICATION_FULL_CLOSURE_INVALID"):
                fence.checkpoint(
                    transaction,
                    phase="LOCAL_MAIN_FF_PRE",
                    actor="integration-coordinator",
                )
        assert len(observations) == 1
        assert fence.replay(transaction) == before
        assert fence.guard.store.replay().active_leases[0] == lease
        assert _git(root, "show-ref") == refs and (root / ".git/index").read_bytes() == index
        assert {name: (directory / name).read_bytes() for name in full_records} == full_records
    finally:
        (root.parent / "readiness-race-input.json").write_text(
            json.dumps(
                {
                    "path": relative,
                    "candidate_sha": binding["candidate_sha"],
                    "original_sha256": hashlib.sha256(original).hexdigest(),
                    "changed_sha256": hashlib.sha256(changed).hexdigest(),
                    "actual_probe_count": len(observations),
                    "before_event_count": len(before.events),
                    "after_event_count": len(fence.replay(transaction).events),
                    "phase_after_attempt": fence.replay(transaction).phase,
                }
            ),
            encoding="utf-8",
        )
        target.write_bytes(original)


@pytest.mark.parametrize("fault", ["none", "profile"], ids=["control", "changed-profile"])
@pytest.mark.parametrize("canonical_merge_repository", ["full-profile-publish"], indirect=True)
def test_remote_admission_rechecks_original_full_profile(
    canonical_merge_repository,
    fault: str,
) -> None:
    """Real local-FF to remote-admission boundary; no remote push is performed."""
    from test_arch_005_integration_publication_fence import _actual_push_invocations
    from test_devx015_workflow_integration import _git

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )

    root, _scope = canonical_merge_repository
    remote = root.parent / "profile-origin.git"
    _git(root, "init", "--bare", str(remote))
    # Preserve the real repository identity gate (P01 topology); isolate only the
    # push transport, and address fetches to this actual local bare remote.
    _git(root, "remote", "set-url", "--push", "origin", str(remote))
    _git(root, "push", "origin", "main")
    binding, directory, _driver, environment = _run_actual_profile_full(root)
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    fence.checkpoint(transaction, phase="LOCAL_MAIN_FF_PRE", actor="integration-coordinator")
    candidate = str(binding["candidate_sha"])
    _git(root, "switch", "main")
    _git(root, "merge", "--ff-only", candidate)
    _git(root, "fetch", str(remote), "refs/heads/main:refs/remotes/origin/main")
    profile_path = directory / "test_runtime_profile.json"
    original_profile = profile_path.read_bytes()
    summary_path = directory / "test_runtime_summary.json"
    summary = summary_path.read_bytes()
    assert json.loads(summary)["status"] == "PASS"
    if fault == "profile":
        profile_path.write_bytes(original_profile + b"\n")
    before = fence.replay(transaction)
    lease = fence.guard.store.replay().active_leases[0]
    before_refs = _git(root, "show-ref")
    before_index = (root / ".git/index").read_bytes()
    remote_tip = _git(remote, "rev-parse", "main")
    evidence = root.parent / "source-evidence"
    evidence.mkdir()
    for name, source in {
        "fence.py": root
        / ("src/ai_trading_system/platform/architecture/integration_publication_fence.py"),
        "runner.py": root / "scripts/run_validation_tier.py",
        "test_harness.py": Path(__file__),
    }.items():
        (evidence / name).write_bytes(source.read_bytes())
    trace = evidence / "actual-git-trace.jsonl"
    result = subprocess.run(
        [
            sys.executable,
            "scripts/architecture_arch005_publication_fence.py",
            "--repository",
            str(root),
            "checkpoint",
            "--transaction",
            str(transaction),
            "--phase",
            "REMOTE_PUSH_PRE",
            "--actor",
            "integration-coordinator",
        ],
        cwd=root,
        env={**environment, "GIT_TRACE2_EVENT": trace.as_posix()},
        capture_output=True,
        text=True,
        # Public CLI observation budget: 360s profile probe plus entry custody and
        # process overhead under Full load, as for the other CLI probes in this module.
        timeout=720,
    )
    (evidence / "remote-admission.stdout.log").write_text(result.stdout, encoding="utf-8")
    (evidence / "remote-admission.stderr.log").write_text(result.stderr, encoding="utf-8")
    assert not _actual_push_invocations(trace)
    assert _git(remote, "rev-parse", "main") == remote_tip
    assert _git(root, "show-ref") == before_refs
    assert (root / ".git/index").read_bytes() == before_index
    assert summary_path.read_bytes() == summary
    assert profile_path.read_bytes() == original_profile + (b"\n" if fault == "profile" else b"")
    if fault == "none":
        assert result.returncode == 0, result.stdout + result.stderr
        assert fence.replay(transaction).phase == "REMOTE_PUSH_PRE"
    else:
        assert result.returncode != 0, "changed Full evidence obtained remote publication admission"
        assert "PUBLICATION_FULL_CLOSURE_INVALID" in result.stdout + result.stderr
        assert fence.replay(transaction) == before
        assert fence.guard.store.replay().active_leases[0] == lease


@pytest.mark.parametrize("exit_code", [0, 7])
@pytest.mark.parametrize("crash_boundary", ["recorded", "committed", "artifact"])
@pytest.mark.parametrize("canonical_merge_repository", ["full-recovery"], indirect=True)
def test_public_full_recovery_after_real_launcher_dies_with_recorded_result(
    canonical_merge_repository,
    exit_code: int,
    crash_boundary: str,
) -> None:
    from test_devx015_workflow_integration import TASK, _git

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )
    from scripts.run_validation_tier import _full_task_commitment

    root, _scope = canonical_merge_repository
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    actor = "integration-coordinator"
    for phase in ("GENERATED_REBUILD_PRE", "GENERATED_REBUILD_POST", "CANDIDATE_COMMIT_PRE"):
        fence.checkpoint(
            transaction,
            phase=phase,
            actor=actor,
            generator_ids=("canonical-task-source",) if phase.startswith("GENERATED_") else (),
        )
    _git(root, "add", ".")
    _git(root, "commit", "-m", "freeze Full recovery CLI runtime fixture")
    fence.checkpoint(transaction, phase="FORMAL_VALIDATION_PRE", actor=actor)
    binding = fence.validate(transaction, require_candidate=True)
    binding["pre_dispatch_readiness"] = {"fixture_scope": "recovery-not-formal-readiness"}
    binding["task_commitment"] = _full_task_commitment(root, TASK, str(binding["candidate_sha"]))
    directory = root / "outputs/validation_runtime/recovery"
    status = "PASS" if exit_code == 0 else "FAIL"
    cli = [
        sys.executable,
        "scripts/run_validation_tier.py",
        "full",
        "--recover-full",
        "--publication-transaction",
        str(transaction),
        "--task-id",
        TASK,
    ]
    launcher = (
        "import argparse,json,os,sys\nfrom pathlib import Path\n"
        "from scripts.run_validation_tier import _FullCommandRunner\n"
        "from ai_trading_system.platform.architecture.integration_publication_fence "
        "import IntegrationPublicationFence\n"
        f"IntegrationPublicationFence(project_root=Path.cwd()).checkpoint(Path({str(transaction)!r}),"
        "phase='FULL_DISPATCHED',actor='integration-coordinator',full_run_id='recorded-recovery')\n"
        "from ai_trading_system.platform.architecture.workflow_coordination "
        "import ExecutionLifecycle\n"
        "from ai_trading_system.platform.architecture.workflow_execution "
        "import current_process_identity\n"
        "def crash():\n"
        " print('FULL_LAUNCHER='+json.dumps(current_process_identity()),flush=True)\n"
        " assert sys.stdin.readline().strip()=='exit'\n"
        " os._exit(47)\n"
        f"boundary={crash_boundary!r}\n"
        "if boundary=='committed':\n"
        " original=ExecutionLifecycle.commit_full_result\n"
        " def fault(self,*args,**kwargs):\n"
        "  original(self,*args,**kwargs)\n"
        "  crash()\n"
        " ExecutionLifecycle.commit_full_result=fault\n"
        "elif boundary=='artifact':\n"
        " def fault(self,*args,**kwargs):\n"
        "  crash()\n"
        " ExecutionLifecycle.record_result=fault\n"
        f"runner=_FullCommandRunner(args=argparse.Namespace(publication_transaction=Path({str(transaction)!r})),"
        f"root=Path.cwd(),artifact_dir=Path({str(directory)!r}),publication_binding={binding!r},"
        f"provenance={{'task_id':{TASK!r}}})\n"
        "result=runner([sys.executable,'-c',"
        f"\"print('single actual dispatch'); raise SystemExit({exit_code})\"],cwd=Path.cwd())\n"
        f"summary=Path({str(directory / 'test_runtime_summary.json')!r})\n"
        f"summary.write_text(json.dumps({{**result,'git_commit':{binding['candidate_sha']!r},'status':{status!r}}}),encoding='utf-8')\n"
        f"runner.record_summary(summary,status={status!r})\n"
        "crash()\n"
    )
    oracle = NativeOracle()
    with subprocess.Popen(
        [sys.executable, "-c", launcher],
        cwd=root,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as crashed:
        assert crashed.stdout is not None
        for line in crashed.stdout:
            if line.startswith("FULL_LAUNCHER="):
                announced = json.loads(line.removeprefix("FULL_LAUNCHER="))
                break
        else:
            pytest.fail("launcher identity barrier missing: " + crashed.stderr.read())
        with oracle.process(announced["pid"]) as handle:
            launcher_identity = {
                "pid": announced["pid"],
                "creation_time": oracle.creation_time(handle),
            }
            assert launcher_identity == announced and not oracle.exited(handle)
            if crash_boundary != "recorded":
                live_before = fence.guard.store.replay()
                observed = subprocess.run(cli, cwd=root, capture_output=True, text=True, timeout=60)
                assert observed.returncode == 2, observed.stdout + observed.stderr
                assert json.loads(observed.stdout)["status"] == "OBSERVE_ONLY"
                assert fence.guard.store.replay() == live_before
            stdout, stderr = crashed.communicate(input="exit\n", timeout=60)
            assert crashed.returncode == 47, stdout + stderr
            assert oracle.exited(handle)
    assert fence.replay(transaction).phase == "FULL_DISPATCHED"
    head = fence.guard.store.replay().active_leases[0]
    assert head.execution["state"] == (
        "RESULT_RECORDED" if crash_boundary == "recorded" else "EXIT_CONFIRMED"
    )
    assert head.execution["launcher"] == launcher_identity
    oracle.assert_job_absent(head.execution["request"]["job_name"])
    summary = directory / "test_runtime_summary.json"
    original = summary.read_bytes()
    log = (directory / "execution.stdout.log").read_bytes()
    before = fence.replay(transaction)
    result_path = directory / "execution_result.json"
    from ai_trading_system.platform.artifacts import canonical_json_bytes

    assert result_path.exists() is (crash_boundary != "committed")
    original_result = canonical_json_bytes(head.execution["full_result_commitment"]["record"])
    if crash_boundary == "committed":
        observed = fence.guard.store.execution_lifecycle().recover(head.lease_id, actor=actor)
        assert observed["status"] == "RECOVERY_REQUIRED"
        assert observed["reason"] == "FULL_COMMITTED_RESULT_REQUIRES_VERIFIED_RECOVERY"
        assert fence.guard.store.replay().active_leases[0].execution == head.execution
        assert not result_path.exists()
    result_path.write_bytes(original_result + b" ")
    rejected_result = subprocess.run(cli, cwd=root, capture_output=True, text=True, timeout=60)
    assert rejected_result.returncode == 2
    assert "FULL_RECOVERY_RESULT_CHANGED" in rejected_result.stderr
    assert fence.replay(transaction) == before
    result_path.write_bytes(original_result)
    if crash_boundary == "committed":
        result_path.unlink()  # Only the new fixture's injected artifact, not original evidence.
    wrong_task = subprocess.run(
        [*cli, "--task-id", TASK + "-OTHER"],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert wrong_task.returncode == 2 and "FULL_RECOVERY_TRANSACTION_BINDING" in wrong_task.stderr
    assert fence.replay(transaction) == before
    print_only = subprocess.run([*cli, "--print-only"], cwd=root, capture_output=True, text=True)
    assert print_only.returncode == 2
    assert fence.replay(transaction) == before
    summary.write_bytes(original + b" ")
    rejected = subprocess.run(cli, cwd=root, capture_output=True, text=True, timeout=60)
    assert rejected.returncode == 2 and "FULL_RECOVERY_SUMMARY_CHANGED" in rejected.stderr
    assert fence.replay(transaction) == before
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        PublicationFenceError,
    )

    with pytest.raises(
        PublicationFenceError,
        match=(
            "PUBLICATION_FULL_SUMMARY_CUSTODY_CHANGED"
            if crash_boundary == "recorded"
            else "PUBLICATION_FULL_CUSTODY_NOT_RECORDED"
        ),
    ):
        fence.checkpoint(
            transaction,
            phase="FORMAL_VALIDATION_RESULT",
            actor=actor,
            evidence_paths=(summary,),
            validation_status=status,
        )
    assert fence.replay(transaction) == before
    summary.write_bytes(original)
    recovered = subprocess.run(cli, cwd=root, capture_output=True, text=True, timeout=60)
    assert recovered.returncode == 0, recovered.stdout + recovered.stderr
    report = json.loads(recovered.stdout)
    assert report["technical_status"] == status and report["status"] == "RECOVERED_RESULT"
    assert report["dispatch_performed"] is False and report["publication_allowed"] is False
    after = fence.replay(transaction)
    assert after.phase == "FORMAL_VALIDATION_RESULT"
    terminal = fence.guard.store.replay().active_leases[0].execution
    assert terminal["state"] == "RESULT_RECORDED"
    assert terminal["request"] == head.execution["request"]
    assert terminal["full_result_commitment"] == head.execution["full_result_commitment"]
    assert result_path.read_bytes() == original_result
    assert after.events[-1]["payload"]["execution_result"] == terminal["result"]["artifact"]
    again = subprocess.run(cli, cwd=root, capture_output=True, text=True, timeout=60)
    assert again.returncode == 0, again.stdout + again.stderr
    assert fence.replay(transaction) == after
    assert fence.guard.store.replay().active_leases[0].execution == terminal
    assert summary.read_bytes() == original
    assert (directory / "execution.stdout.log").read_bytes() == log


@pytest.mark.parametrize("fails", [False, True], ids=["pass", "fail"])
@pytest.mark.parametrize("canonical_merge_repository", ["full-mandatory"], indirect=True)
def test_actual_mandatory_xdist_runs_inside_full_job_and_records_custody(
    canonical_merge_repository,
    fails: bool,
) -> None:
    """Actual mandatory/xdist/Job chain, not whole matrix or formal profile acceptance."""
    from test_devx015_workflow_integration import TASK, _git

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )
    from ai_trading_system.platform.architecture.workflow_contract import canonical_digest
    from scripts.run_validation_tier import _full_task_commitment

    root, _scope = canonical_merge_repository
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    for phase in ("GENERATED_REBUILD_PRE", "GENERATED_REBUILD_POST", "CANDIDATE_COMMIT_PRE"):
        fence.checkpoint(
            transaction,
            phase=phase,
            actor="integration-coordinator",
            generator_ids=("canonical-task-source",) if phase.startswith("GENERATED_") else (),
        )
    _git(root, "add", ".")
    _git(root, "commit", "-m", "freeze actual mandatory Job transport fixture")
    fence.checkpoint(transaction, phase="FORMAL_VALIDATION_PRE", actor="integration-coordinator")
    full_run_id = "actual-mandatory-job"
    binding = fence.validate(transaction, require_candidate=True)
    binding["pre_dispatch_readiness"] = {"fixture_scope": "mandatory-job-not-formal-readiness"}
    binding["task_commitment"] = _full_task_commitment(root, TASK, str(binding["candidate_sha"]))
    job_name = "Local\\AITS-DEVX015-full-" + canonical_digest(
        {
            "transaction": binding["transaction_sha256"],
            "full_run_id": full_run_id,
        }
    )
    directory = root / "outputs/validation_runtime/mandatory"
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-n2",
        "--dist",
        "loadfile",
        "tests/test_full_job.py",
        "-q",
    ]
    script = (
        "import argparse,json\nfrom pathlib import Path\n"
        "from ai_trading_system.platform.architecture.integration_publication_fence "
        "import IntegrationPublicationFence\n"
        f"IntegrationPublicationFence(project_root=Path.cwd()).checkpoint(Path({str(transaction)!r}),"
        f"phase='FULL_DISPATCHED',actor='integration-coordinator',full_run_id={full_run_id!r})\n"
        "from scripts.run_validation_tier import (_FullCommandRunner,"
        "_run_mandatory_acceptance_command,_record_publication_full_result)\n"
        "from ai_trading_system.platform.architecture.workflow_execution "
        "import bind_mandatory_acceptance\n"
        f"binding={binding!r}\n"
        "mandatory=bind_mandatory_acceptance(Path.cwd(),binding['candidate_sha'])\n"
        "binding['mandatory_acceptance_binding']=mandatory\n"
        f"args=argparse.Namespace(publication_transaction=Path({str(transaction)!r}))\n"
        f"directory=Path({str(directory)!r})\n"
        "runner=_FullCommandRunner(args=args,root=Path.cwd(),artifact_dir=directory,"
        f"publication_binding=binding,provenance={{'task_id':{TASK!r}}})\n"
        f"result=_run_mandatory_acceptance_command({command!r},cwd=Path.cwd(),binding=mandatory,"
        f"expected_collections=2,env_overrides={{'DEVX015_EXPECTED_JOB':{job_name!r},"
        f"'DEVX015_FAIL':{str(int(fails))!r}}},command_runner=runner)\n"
        "status='PASS' if result['exit_code']==0 else 'FAIL'\n"
        "summary=directory/'test_runtime_summary.json'\n"
        "summary.write_text(json.dumps({**result,'git_commit':binding['candidate_sha'],'status':status}),encoding='utf-8')\n"
        "runner.record_summary(summary,status=status)\n"
        "_record_publication_full_result(args,repo_root=Path.cwd(),status=status,summary_path=summary)\n"
    )
    environment = dict(os.environ)
    for name in (
        "PYTEST_ADDOPTS",
        "PYTEST_CURRENT_TEST",
        "PYTEST_XDIST_WORKER",
        "PYTEST_XDIST_WORKER_COUNT",
        "AITS_MANDATORY_ACCEPTANCE_REQUEST",
    ):
        environment.pop(name, None)
    # Hang guard only, not a performance assertion: mandatory acceptance source
    # identity hashing inside the Job measured above 120s on the 2026-09-25 host.
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=1200,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    summary = json.loads((directory / "test_runtime_summary.json").read_bytes())
    assert summary["status"] == ("FAIL" if fails else "PASS")
    assert summary["mandatory_acceptance"]["status"] == summary["status"]
    if fails:
        assert "actual mandatory target failure" in summary["pytest_output"]
    else:
        execution = summary["mandatory_acceptance"]["evidence"]["execution"]
        assert execution["status"] == "PASS" and execution["collection_count"] == 2
        assert execution["reports"]["tests/test_full_job.py::test_required"] == [
            ["setup", "passed", False],
            ["call", "passed", False],
            ["teardown", "passed", False],
        ]
    witness = json.loads((directory / "job-witness.json").read_bytes())
    assert witness["in_job"] is True and witness["worker"] in {"gw0", "gw1"}
    head = fence.guard.store.replay().active_leases[0]
    assert head.execution["request"]["job_name"] == job_name
    assert head.execution["result"]["status"] == summary["status"]
    actual_argv = head.execution["request"]["argv"]
    assert "-p" in actual_argv
    assert "ai_trading_system.platform.architecture.workflow_execution" in actual_argv
    identity = json.loads((directory / "execution_validation_identity.json").read_bytes())
    assert identity["mandatory_acceptance_binding"]["candidate_sha"] == binding["candidate_sha"]
    assert identity["mandatory_acceptance_binding"]["required_nodes"] == [
        "tests/test_full_job.py::test_required"
    ]
    NativeOracle().assert_job_absent(job_name)
    assert fence.replay(transaction).phase == "FORMAL_VALIDATION_RESULT"


@pytest.fixture(params=[False, True], ids=["inherited", "restricted-native-modeled-broker"])
def adapter_worker(request, monkeypatch):
    """Exercise real native token ABI, not actual distinct-account acceptance."""
    if not request.param:
        yield None
        return
    import ctypes
    from ctypes import wintypes as w

    from test_devx015_workflow_execution import _own_primary_token

    from ai_trading_system.platform.architecture import workflow_execution as execution

    with _own_primary_token(execution) as (api, security, token, sid):
        security.CreateRestrictedToken.argtypes = [
            w.HANDLE, w.DWORD, w.DWORD, ctypes.c_void_p, w.DWORD, ctypes.c_void_p,
            w.DWORD, ctypes.c_void_p, ctypes.POINTER(w.HANDLE),
        ]
        security.CreateRestrictedToken.restype = w.BOOL
        restricted = w.HANDLE()
        assert security.CreateRestrictedToken(token, 1, 0, None, 0, None, 0, None,
                                              ctypes.byref(restricted))
        try:
            worker = execution.WindowsWorkerToken.from_primary_handle(
                restricted.value, expected_sid=sid,
            )
        finally:
            assert api.CloseHandle(restricted)
    observe = execution._process_primary_token

    def modeled_launcher(kernel, process):
        actual = observe(kernel, process)
        if process == kernel.GetCurrentProcess():
            return {"sid": "S-1-5-21-9-9-9-9999", "elevated": False}
        return actual  # Suspended child's real primary token must still match.

    monkeypatch.setattr(execution, "_process_primary_token", modeled_launcher)
    try:
        yield worker
    finally:
        worker.close()


@pytest.mark.parametrize("exit_code", [0, 7])
def test_full_command_adapter_binds_real_fence_canonical_lease_and_summary(
    canonical_merge_repository,
    exit_code: int,
    monkeypatch,
    adapter_worker,
) -> None:
    """Real adapter/Job custody; synthetic readiness is not formal Full acceptance."""
    import argparse

    from test_devx015_workflow_integration import TASK, _git

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )
    from ai_trading_system.platform.architecture.task_registry_canonical import (
        validate_canonical_registry,
    )
    from ai_trading_system.platform.architecture.workflow_contract import canonical_digest
    from scripts.run_validation_tier import _full_task_commitment, _FullCommandRunner

    root, _scope = canonical_merge_repository
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    actor = "integration-coordinator"
    for phase in ("GENERATED_REBUILD_PRE", "GENERATED_REBUILD_POST", "CANDIDATE_COMMIT_PRE"):
        fence.checkpoint(
            transaction,
            phase=phase,
            actor=actor,
            generator_ids=("canonical-task-source",) if phase.startswith("GENERATED_") else (),
        )
    _git(root, "add", ".")  # Disposable synthetic fixture only.
    _git(root, "commit", "-m", "freeze actual Full adapter fixture")
    fence.checkpoint(transaction, phase="FORMAL_VALIDATION_PRE", actor=actor)
    binding = fence.checkpoint(
        transaction,
        phase="FULL_DISPATCHED",
        actor=actor,
        full_run_id="actual-adapter",
    )
    binding["pre_dispatch_readiness"] = {"fixture_scope": "adapter-not-formal-readiness"}
    binding["task_commitment"] = _full_task_commitment(root, TASK, str(binding["candidate_sha"]))
    directory = root / "outputs/validation_runtime/adapter"
    monkeypatch.setenv("DEVX015_BROKER_ONLY_WITNESS", "must-not-inherit")
    worker_environment = {"SystemRoot": os.environ["SystemRoot"]} if adapter_worker else None
    runner = _FullCommandRunner(
        args=argparse.Namespace(publication_transaction=transaction),
        root=root,
        artifact_dir=directory,
        publication_binding=binding,
        provenance={"task_id": TASK},
        worker_token=adapter_worker,
        worker_environment=worker_environment,
    )
    environment = {"DEVX015_FULL_ADAPTER_WITNESS": "actual-child"}
    # Bounded wiring observation: stop before dependency capture/launch. This
    # checks mandatory wrapper environment selection, not mandatory acceptance.
    from scripts import run_validation_tier as validation_runner

    captured_environments = []

    def observe_mandatory_environment(value, **kwargs):
        captured_environments.append(dict(value))
        raise ExecutionContainmentError("TEST_RUNTIME_CAPTURE_STOP")

    with monkeypatch.context() as capture:
        capture.setattr(validation_runner, "bind_acceptance_checkout", lambda *args: {})
        capture.setattr(validation_runner, "acceptance_runtime_identity",
                        observe_mandatory_environment)
        if adapter_worker:
            with pytest.raises(ExecutionContainmentError, match="FULL_WORKER_EXCHANGE_REQUIRED"):
                validation_runner._run_mandatory_acceptance_command(
                    [sys.executable, "-m", "pytest"], cwd=root, binding={},
                    expected_collections=1, env_overrides=environment, command_runner=runner,
                )
        else:
            stopped = validation_runner._run_mandatory_acceptance_command(
                [sys.executable, "-m", "pytest"], cwd=root, binding={},
                expected_collections=1, env_overrides=environment, command_runner=runner,
            )
            assert stopped["exit_code"] == 1
            assert "TEST_RUNTIME_CAPTURE_STOP" in stopped["mandatory_acceptance"]["reason"]
    expected_environment = {
        **(os.environ if worker_environment is None else worker_environment), **environment,
    }
    assert captured_environments == ([] if adapter_worker else [expected_environment])
    # The production runner is its own clean process. This in-process adapter test
    # may share a worker that already loaded unrelated compiled wrappers (numpy),
    # so measure runtime identity in a fresh interpreter with the same environment.
    monkeypatch.setattr(
        validation_runner, "acceptance_runtime_identity", _clean_process_runtime_identity,
    )
    changed = _FullCommandRunner(
        args=argparse.Namespace(publication_transaction=transaction),
        root=root,
        artifact_dir=directory,
        publication_binding={**binding, "task_commitment": {}},
        provenance={"task_id": TASK},
    )
    with pytest.raises(ExecutionContainmentError, match="FULL_EXECUTION_TASK_CHANGED"):
        changed([sys.executable, "-c", "raise AssertionError('must not launch')"], cwd=root)
    assert not directory.exists()
    assert fence.guard.store.replay().active_leases[0].execution is None
    result = runner(
        [
            sys.executable,
            "-c",
            "import os,sys; print(os.environ['DEVX015_FULL_ADAPTER_WITNESS']); "
            + ("assert 'DEVX015_BROKER_ONLY_WITNESS' not in os.environ; "
               if adapter_worker else "")
            +
            f"sys.exit({exit_code})",
        ],
        cwd=root,
        env_overrides=environment,
    )
    request = json.loads((directory / "execution_request.json").read_bytes())
    assert request == runner.request
    identity_raw = (directory / "execution_validation_identity.json").read_bytes()
    assert hashlib.sha256(identity_raw).hexdigest() == request["validation_identity_sha256"]
    assert request["task_authority_sha256"] == canonical_digest(
        validate_canonical_registry(project_root=root).fragment(TASK)
    )
    assert request["environment_sha256"] == execution_environment_sha256(
        {**(os.environ if worker_environment is None else worker_environment), **environment}
    )
    identity = json.loads(identity_raw)
    if adapter_worker:
        assert identity["worker_identity"] == adapter_worker.binding()
    else:
        assert "worker_identity" not in identity
    assert request["candidate_sha"] == _git(root, "rev-parse", "HEAD")
    assert result["exit_code"] == exit_code and "actual-child" in result["pytest_output"]
    head = fence.guard.store.replay().active_leases[0]
    assert head.execution["state"] == "EXIT_CONFIRMED"
    status = "PASS" if exit_code == 0 else "FAIL"
    summary = directory / "test_runtime_summary.json"
    payload = {
        **result,
        "git_commit": request["candidate_sha"],
        "status": status,
    }
    summary.write_text(json.dumps({**payload, "git_commit": "0" * 40}), encoding="utf-8")
    with pytest.raises(ExecutionContainmentError, match="FULL_RESULT_SUMMARY_BINDING"):
        runner.record_summary(summary, status=status)
    assert not (directory / "execution_result.json").exists()
    summary.write_text(json.dumps(payload), encoding="utf-8")
    runner.record_summary(summary, status=status)
    head = fence.guard.store.replay().active_leases[0]
    assert head.execution["state"] == "RESULT_RECORDED"
    assert head.execution["result"]["status"] == status
    if status == "PASS":
        _assert_publication_envelope_preserves_actual_full(head, monkeypatch)
    custody = json.loads((directory / "execution_result.json").read_bytes())
    assert custody["summary"]["sha256"] == hashlib.sha256(summary.read_bytes()).hexdigest()
    from dataclasses import replace

    from ai_trading_system.platform.architecture.workflow_coordination import (
        validate_execution,
        validate_execution_transition,
    )
    from ai_trading_system.platform.artifacts import canonical_json_bytes

    frozen = head.execution["full_result_commitment"]
    assert frozen["record"] == custody
    assert (
        frozen["sha256"]
        == hashlib.sha256((directory / "execution_result.json").read_bytes()).hexdigest()
    )
    for field, replacement in (("candidate_sha", "0" * 40), ("request_id", "different-request")):
        changed_execution = json.loads(json.dumps(head.execution))
        changed_commitment = changed_execution["full_result_commitment"]
        changed_commitment["record"][field] = replacement
        changed_commitment["sha256"] = hashlib.sha256(
            canonical_json_bytes(changed_commitment["record"])
        ).hexdigest()
        with pytest.raises(ParallelControlError, match="FULL_COMMITMENT_BINDING"):
            validate_execution(replace(head, execution=changed_execution))
    changed_execution = json.loads(json.dumps(head.execution))
    changed_commitment = changed_execution["full_result_commitment"]
    changed_commitment["record"]["summary"]["sha256"] = "0" * 64
    changed_commitment["sha256"] = hashlib.sha256(
        canonical_json_bytes(changed_commitment["record"])
    ).hexdigest()
    with pytest.raises(ParallelControlError, match="FULL_COMMITMENT_DRIFT"):
        validate_execution_transition(head, replace(head, execution=changed_execution))
    before = fence.guard.store.replay()
    runner.record_summary(summary, status=status)
    assert fence.guard.store.replay() == before
    summary.write_text(json.dumps({**payload, "extra": "changed"}), encoding="utf-8")
    with pytest.raises(ExecutionContainmentError, match="FULL_RESULT_REPLAY_CHANGED"):
        runner.record_summary(summary, status=status)
    assert fence.guard.store.replay() == before
    summary.write_text(json.dumps(payload), encoding="utf-8")
    assert fence.replay(transaction).phase == "FULL_DISPATCHED"


def _assert_publication_envelope_preserves_actual_full(head, monkeypatch):
    """Validate proposed event values only; never append a fake publication run."""
    from dataclasses import replace

    from ai_trading_system.platform.architecture.workflow_contract import canonical_digest
    from ai_trading_system.platform.architecture.workflow_coordination import (
        execution_is_terminal,
        full_execution_projection,
        validate_execution,
        validate_execution_transition,
    )
    from ai_trading_system.platform.architecture.workflow_execution import current_process_identity

    original = json.loads(json.dumps(head.execution))
    request = {key: value for key, value in original["request"].items()
               if key != "validation_identity_sha256"}
    request.update(
        schema_version="workflow_execution_request.v5", request_id="publication-envelope-unit",
        execution_kind="CONTROLLED_LOCAL_PUBLICATION",
        publication_transaction_path=(Path(request["cwd"]) / "outputs/transaction.json").as_posix(),
        publication_transaction_sha256="a" * 64, local_publication_event_id="b" * 64,
        local_publication_intent_sha256="c" * 64,
        full_execution_sha256=canonical_digest(original), expected_main_sha=head.base_commit,
        publication_action="PUBLISH", publication_attempt=1, previous_publication_sha256=None,
    )
    attempt = {
        "schema_version": "lease_execution.v1", "request": request,
        "request_sha256": canonical_digest(request), "launcher": current_process_identity(),
        "state": "RESERVED", "process": None, "exit": None, "result": None,
    }
    value = {**original, "schema_version": "lease_execution.v5", "publication_attempts": [attempt]}
    proposed = replace(head, execution=value)
    validate_execution(proposed)
    from ai_trading_system.platform.architecture import workflow_coordination as coordination

    validations = []
    original_validate = coordination.validate_execution

    def observe_validate(lease, **kwargs):
        validations.append(lease.execution["schema_version"])
        return original_validate(lease, **kwargs)

    with monkeypatch.context() as scoped:
        scoped.setattr(coordination, "validate_execution", observe_validate)
        validate_execution_transition(head, proposed)
    assert validations == ["lease_execution.v5", "lease_execution.v1"]
    assert full_execution_projection(value) == original == head.execution
    assert execution_is_terminal(original) is True
    assert execution_is_terminal(value) is False
    with pytest.raises(ParallelControlError, match="PUBLICATION_PARENT_REQUIRED"):
        validate_execution(replace(head, execution=attempt))
    with pytest.raises(ParallelControlError, match="PUBLICATION_HISTORY_CHANGED"):
        validate_execution_transition(proposed, replace(proposed, state="RELEASED"))
    with pytest.raises(ParallelControlError, match="PUBLICATION_HISTORY_CHANGED"):
        validate_execution_transition(proposed, head)
    changed = json.loads(json.dumps(value))
    changed["publication_attempts"][0]["request"]["candidate_sha"] = "f" * 40
    with pytest.raises(ParallelControlError, match="PUBLICATION_FULL_BINDING"):
        validate_execution(replace(head, execution=changed))
    changed = json.loads(json.dumps(value))
    changed["result"]["reason"] = "different-original-full"
    changed["publication_attempts"][0]["request"]["full_execution_sha256"] = canonical_digest(
        full_execution_projection(changed)
    )
    changed["publication_attempts"][0]["request_sha256"] = canonical_digest(
        changed["publication_attempts"][0]["request"]
    )
    validate_execution(replace(head, execution=changed))  # Internally consistent rehash.
    with pytest.raises(ParallelControlError, match="PUBLICATION_HISTORY_CHANGED"):
        validate_execution_transition(proposed, replace(head, execution=changed))
    changed = json.loads(json.dumps(value))
    child = changed["publication_attempts"][0]
    child.update(state="RESULT_RECORDED", process=original["process"], exit=original["exit"],
                 result={"status": "PASS", "reason": "SELF_REPORTED", "artifact": None})
    with pytest.raises(ParallelControlError, match="PUBLICATION_STABLE_VERIFICATION_REQUIRED"):
        validate_execution(replace(head, execution=changed))
    assert head.execution == original


@pytest.mark.parametrize("exit_code", [0, 7])
def test_validation_runner_owns_real_tree_until_exit_without_adopting_result(
    tmp_path: Path,
    exit_code: int,
) -> None:
    from scripts.run_validation_tier import _run_leased_command

    store, lease, request, env, _now = _case(tmp_path)
    # The candidate is independent of the source lease's base; never rewrite it.
    request["candidate_sha"] = "d" * 40
    request["argv"] = [
        sys.executable,
        "-c",
        "import subprocess,sys; "
        "subprocess.Popen([sys.executable, '-c', "
        "\"import time; time.sleep(0.6); print('descendant complete')\"]); "
        f"print('root complete'); sys.exit({exit_code})",
    ]
    lifecycle = store.execution_lifecycle()
    result = _run_leased_command(
        request=request,
        lifecycle=lifecycle,
        actor=ACTOR,
        environment=env,
    )
    assert result["exit_code"] == exit_code
    assert "root complete" in result["pytest_output"]
    assert "descendant complete" in result["pytest_output"]
    assert result["execution_exit_confirmed"] is True
    head = store.replay().active_leases[0]
    assert head.base_commit == BASE_COMMIT
    assert head.execution["request"]["candidate_sha"] == "d" * 40
    assert head.execution["state"] == "EXIT_CONFIRMED"
    assert head.execution["exit"]["returncode"] == exit_code
    assert head.execution["exit"]["job_state"] == "EMPTY"
    assert head.execution["result"] is None
    before = store.replay()
    with pytest.raises(ExecutionContainmentError, match="FULL_EXECUTION_REPLAY_ONLY"):
        _run_leased_command(
            request=request,
            lifecycle=lifecycle,
            actor=ACTOR,
            environment=env,
        )
    assert store.replay() == before
    lifecycle.record_incomplete_result(lease.lease_id, actor=ACTOR)
    store.release(lease.lease_id, actor=ACTOR, now=datetime.now(UTC), evidence_refs=())


@pytest.mark.parametrize("changed", ["environment", "identity"])
@pytest.mark.parametrize("persist_request", [False, True])
def test_validation_runner_rejects_changed_inputs_before_reservation(
    tmp_path: Path, changed: str, persist_request: bool,
) -> None:
    from scripts.run_validation_tier import _run_leased_command

    store, _lease, request, env, _now = _case(tmp_path)
    before = store.replay()
    identity = None
    if changed == "environment":
        env = {**env, "DEVX015_CHANGED_INPUT": "different"}
        reason = "FULL_EXECUTION_ENVIRONMENT_CHANGED"
    else:
        identity = {"candidate_sha": "e" * 40}
        reason = "FULL_VALIDATION_IDENTITY_CHANGED"
    request_path = tmp_path / "evidence" / "execution_request.json"
    with pytest.raises(ExecutionContainmentError, match=reason):
        _run_leased_command(
            request=request, lifecycle=store.execution_lifecycle(), actor=ACTOR,
            environment=env, validation_identity=identity,
            request_path=request_path if persist_request else None,
        )
    assert store.replay() == before
    assert not request_path.parent.exists()
    assert not Path(request["stdout_path"]).exists()
    assert not Path(request["result_path"]).exists()


@pytest.mark.parametrize("serialized", [False, True])
def test_validation_runner_requires_live_worker_before_reservation(
    tmp_path: Path, serialized: bool,
) -> None:
    from scripts.run_validation_tier import _run_leased_command

    store, _lease, request, env, _now = _case(tmp_path)
    before = store.replay()
    worker = {"sid": "S-1-5-21-1-2-3-1001", "elevated": False, "session_id": 1}
    with pytest.raises(ExecutionContainmentError, match="WORKER_TOKEN_CAPABILITY_REQUIRED"):
        _run_leased_command(
            request=request, lifecycle=store.execution_lifecycle(), actor=ACTOR,
            environment=env, validation_identity={"worker_identity": worker},
            worker_token=worker if serialized else None,
        )
    assert store.replay() == before
    assert not Path(request["stdout_path"]).exists()


@pytest.mark.parametrize("token,environment", [(object(), None), (None, {}), ({}, {})])
def test_full_adapter_rejects_incomplete_worker_configuration(
    tmp_path: Path, token: object, environment: object,
) -> None:
    from argparse import Namespace

    from scripts.run_validation_tier import _FullCommandRunner

    with pytest.raises(ExecutionContainmentError, match="WORKER"):
        _FullCommandRunner(
            args=Namespace(), root=tmp_path, artifact_dir=tmp_path / "outputs",
            publication_binding={}, provenance={}, worker_token=token,
            worker_environment=environment,
        )
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("drift", ["parent", "worker"])
def test_mandatory_wrapper_rechecks_actual_worker_environment(tmp_path, monkeypatch, drift):
    """Environment wiring unit proof; no claim of native mandatory acceptance."""
    from scripts import run_validation_tier as runner

    class EnvironmentProbe(runner._FullCommandRunner):
        def __init__(self):
            self.worker_environment = {"EXPLICIT_WORKER": "initial"}
            self.worker_token = None
            self.worker_exchange = None

        def __call__(self, command, **kwargs):
            if drift == "worker":
                self.worker_environment["EXPLICIT_WORKER"] = "changed"
            else:
                monkeypatch.setenv("PARENT_ONLY", "changed")
            return {"exit_code": 0}

    observed = []

    def runtime(environment, **kwargs):
        observed.append(dict(environment))
        return {"environment": dict(environment)}

    def stop_at_result(*args, **kwargs):
        raise ExecutionContainmentError("TEST_RESULT_VALIDATION_REACHED")

    binding = {"candidate_sha": "c" * 40}
    monkeypatch.setattr(runner, "bind_acceptance_checkout", lambda *args: {})
    monkeypatch.setattr(runner, "bind_acceptance_implementation", lambda *args, **kwargs: {})
    monkeypatch.setattr(runner, "bind_mandatory_acceptance", lambda *args: binding)
    monkeypatch.setattr(runner, "acceptance_runtime_identity", runtime)
    monkeypatch.setattr(runner, "validate_mandatory_acceptance_result", stop_at_result)
    result = runner._run_mandatory_acceptance_command(
        [sys.executable, "-m", "pytest"], cwd=tmp_path, binding=binding,
        expected_collections=1, env_overrides={"OVERRIDE": "bound"},
        command_runner=EnvironmentProbe(),
    )
    assert len(observed) == 2
    assert observed[0] == {"EXPLICIT_WORKER": "initial", "OVERRIDE": "bound"}
    reason = result["mandatory_acceptance"]["reason"]
    if drift == "worker":
        assert "ACCEPTANCE_RUNTIME_CHANGED" in reason
        assert observed[1]["EXPLICIT_WORKER"] == "changed"
    else:
        assert observed[1] == observed[0]
        assert "TEST_RESULT_VALIDATION_REACHED" in reason


@pytest.mark.parametrize("drift", ["none", "launcher", "candidate"])
def test_mandatory_exchange_separates_launcher_and_candidate(tmp_path, monkeypatch, drift):
    """Wrapper custody and identity wiring; exchange/token are explicit unit seams."""
    from contextlib import nullcontext

    from ai_trading_system.platform.architecture import workflow_coordination as coordination
    from scripts import run_validation_tier as runner

    result_path = tmp_path / "result.json"
    result_path.write_bytes(b"")
    info = result_path.stat()

    class ExchangeProbe:
        result_identity = (info.st_dev, info.st_ino)

        def directory(self):
            return nullcontext(tmp_path)

    dispatched = False
    sources = [{"path": "src/candidate.py", "sha256": "c" * 64}]
    launcher = [{"path": "fixed/scripts/run_validation_tier.py", "sha256": "a" * 64}]
    binding = {"candidate_sha": "c" * 40}

    class ExchangeRunner(runner._FullCommandRunner):
        def __init__(self):
            self.worker_token = object()
            self.worker_exchange = ExchangeProbe()
            self.worker_environment = {"EXPLICIT_WORKER": "bound"}

        def __call__(self, command, **kwargs):
            nonlocal dispatched
            request = json.loads((tmp_path / "request.json").read_text())
            assert request["implementation_identity"] == sources
            assert request["runtime_identity"] == {"runtime": "bound"}
            assert "ai_trading_system.platform.architecture.workflow_execution" in command
            dispatched = True
            return {"exit_code": 0}

    def candidate_capture(*args, **kwargs):
        if dispatched and drift == "candidate":
            return [{**sources[0], "sha256": "d" * 64}]
        return sources

    def launcher_capture(*args, **kwargs):
        if dispatched and drift == "launcher":
            return [{**launcher[0], "sha256": "b" * 64}]
        return launcher

    def reject_parent_candidate_execution(*args, **kwargs):
        pytest.fail("trusted parent must not bind its loaded modules to candidate source")

    worker_input = {"checkout_identity": {}, "origin_valid": True,
                    "runtime_identity": {"runtime": "bound"}, "implementation_identity": sources}
    monkeypatch.setattr(coordination, "WindowsWorkerExchange", ExchangeProbe)
    monkeypatch.setattr(runner, "bind_acceptance_checkout", lambda *args: {})
    monkeypatch.setattr(runner, "bind_mandatory_acceptance", lambda *args: binding)
    monkeypatch.setattr(runner, "bind_acceptance_implementation", reject_parent_candidate_execution)
    monkeypatch.setattr(runner, "capture_acceptance_implementation", candidate_capture)
    monkeypatch.setattr(runner, "bind_inspector_implementation", launcher_capture)
    monkeypatch.setattr(
        runner, "acceptance_runtime_identity", lambda *args, **kwargs: {"runtime": "bound"},
    )
    monkeypatch.setattr(runner, "validate_mandatory_acceptance_result", lambda *args, **kwargs: {
        **worker_input, "worker_inputs": [worker_input],
    })
    result = runner._run_mandatory_acceptance_command(
        [sys.executable, "-m", "pytest"], cwd=tmp_path, binding=binding,
        expected_collections=1, command_runner=ExchangeRunner(),
    )
    assert dispatched
    mandatory = result["mandatory_acceptance"]
    if drift == "none":
        assert result["exit_code"] == 0 and mandatory["status"] == "PASS"
        assert mandatory["launcher_identity"] == launcher
        assert mandatory["runner_identity"] == sources
        assert mandatory["result_custody"]["result_identity"] == [info.st_dev, info.st_ino]
    else:
        assert result["exit_code"] == 1 and mandatory["status"] == "FAIL"
        expected = "LAUNCHER_CHANGED" if drift == "launcher" else "IMPLEMENTATION_CHANGED"
        assert expected in mandatory["reason"]


def test_launcher_inventory_rechecks_fixed_source_bytes(tmp_path, monkeypatch):
    """Inventory verifier proof, not administrator deployment or Full admission."""
    from scripts import run_validation_tier as runner

    fixed = tmp_path / "fixed"
    entry = fixed / "scripts" / "run_validation_tier.py"
    entry.parent.mkdir(parents=True)
    entry.write_bytes(b"# fixed launcher\n")
    monkeypatch.setattr(runner, "_repo_root", lambda: fixed)
    row = {"path": entry.as_posix(), "sha256": hashlib.sha256(entry.read_bytes()).hexdigest(),
           "size_bytes": entry.stat().st_size}
    assert runner._recheck_launcher_identity([row]) == [row]
    for inventory in (None, [], [row, row], [{**row, "size_bytes": True}],
                      [{**row, "path": (tmp_path / "candidate.py").as_posix()}]):
        with pytest.raises(ValueError):
            runner._recheck_launcher_identity(inventory)
    entry.write_bytes(b"# mutated launcher\n")
    with pytest.raises(ValueError, match="source changed"):
        runner._recheck_launcher_identity([row])


def test_worker_exchange_factory_requires_real_admin_before_effects(tmp_path):
    from ai_trading_system.platform.architecture.workflow_coordination import WindowsWorkerExchange

    with pytest.raises(ParallelControlError, match="EXCHANGE_FACTORY_REQUIRED"):
        WindowsWorkerExchange()
    with pytest.raises(ParallelControlError, match="ADMINISTRATOR_REQUIRED"):
        WindowsWorkerExchange.create(tmp_path / "not-created", None)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("exchange", [None, {"root": "forged"}])
def test_mandatory_worker_never_falls_back_to_parent_temp(tmp_path, monkeypatch, exchange):
    from scripts import run_validation_tier as runner

    class InvalidContext(runner._FullCommandRunner):
        def __init__(self):
            self.worker_token = object()
            self.worker_exchange = exchange

    def unexpected_temp(*args, **kwargs):
        pytest.fail("worker path must reject before allocating a parent temp directory")

    monkeypatch.setattr(runner.tempfile, "TemporaryDirectory", unexpected_temp)
    with pytest.raises(ExecutionContainmentError, match="FULL_WORKER_EXCHANGE_REQUIRED"):
        runner._allocate_runtime_profile(InvalidContext())
    with pytest.raises(ExecutionContainmentError, match="FULL_WORKER_EXCHANGE_REQUIRED"):
        runner._run_mandatory_acceptance_command(
            [sys.executable, "-m", "pytest"], cwd=tmp_path, binding={},
            expected_collections=1, command_runner=InvalidContext(),
        )
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("mode", ["--list", "--print-only", "--recover-full"])
def test_inspection_candidate_override_cannot_retarget_mutating_entry(tmp_path, mode, capsys):
    from scripts.run_validation_tier import main

    assert main(["full", mode, "--inspection-candidate-root", str(tmp_path)]) == 2
    assert "restricted to read-only" in capsys.readouterr().err
    assert list(tmp_path.iterdir()) == []


def test_publication_inspector_never_executes_candidate_script(tmp_path):
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        _full_profile_inspector_command,
    )

    scripts = tmp_path / "scripts"
    scripts.mkdir()
    witness = tmp_path / "candidate-executed"
    candidate_script = scripts / "run_validation_tier.py"
    candidate_script.write_text(
        "from pathlib import Path\n" + f"Path({str(witness)!r}).write_text('unsafe')\n",
        encoding="utf-8",
    )
    command = _full_profile_inspector_command(tmp_path, tmp_path / "missing.json", "TEST")
    assert command[1] == "-I" and Path(command[2]) != candidate_script
    injected = tmp_path / "injected"
    injected.mkdir()
    injection_witness = tmp_path / "environment-executed"
    (injected / "sitecustomize.py").write_text(
        "from pathlib import Path\n"
        + f"Path({str(injection_witness)!r}).write_text('unsafe')\n", encoding="utf-8",
    )
    result = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True, timeout=30,
                            env={**os.environ, "PYTHONPATH": str(injected)})
    assert result.returncode == 2
    assert "Full publication profile rejected" in result.stderr
    assert not witness.exists()
    assert not injection_witness.exists()


def test_candidate_source_capture_reads_without_execution_and_rejects_drift(tmp_path):
    from test_devx015_workflow_integration import _git

    from ai_trading_system.platform.architecture.workflow_execution import (
        capture_acceptance_implementation,
    )

    _git(tmp_path, "init")
    source = tmp_path / "src/probe.py"
    source.parent.mkdir()
    witness = tmp_path / "executed"
    source.write_text("from pathlib import Path\n"
                      + f"Path({str(witness)!r}).write_text('unexpected')\n", encoding="utf-8")
    _git(tmp_path, "add", "src/probe.py")
    _git(tmp_path, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
         "commit", "-m", "freeze source capture fixture")
    candidate = _git(tmp_path, "rev-parse", "HEAD")
    rows = capture_acceptance_implementation(tmp_path, candidate)
    assert len(rows) == 1 and rows[0]["path"] == "src/probe.py"
    assert not witness.exists()
    source.write_text("raise AssertionError('changed')\n", encoding="utf-8")
    with pytest.raises(ExecutionContainmentError, match="IMPLEMENTATION_NOT_CANDIDATE"):
        capture_acceptance_implementation(tmp_path, candidate)
    assert not witness.exists()


@pytest.mark.parametrize("case", ["candidate", "inspector", "outside", "changed"])
def test_profile_capture_recheck_limits_independent_inspector_scope(tmp_path, case):
    """Isolate locked capture recheck; not a transaction/publication admission test."""
    from types import SimpleNamespace

    from ai_trading_system.platform.architecture import integration_publication_fence as fence_api
    from ai_trading_system.platform.architecture.workflow_contract import canonical_digest

    candidate = tmp_path / "candidate"
    candidate.mkdir()
    path = candidate / "evidence.json"
    path.write_bytes(b"candidate")
    if case == "inspector":
        path = Path(fence_api.__file__).absolute()
    elif case == "outside":
        path = tmp_path / "outside.py"
        path.write_bytes(b"outside")
    raw = path.read_bytes()
    observation = {"execution_sha256": canonical_digest(None), "captures": [{
        "path": path.as_posix(), "size_bytes": len(raw),
        "sha256": "0" * 64 if case == "changed" else hashlib.sha256(raw).hexdigest(),
    }]}
    replay = SimpleNamespace(transaction={"transaction_sha256": "a" * 64},
                             events=[{"event_id": "event"}])
    preparation = ("a" * 64, "event", "TEST_ONLY", json.dumps(observation).encode())
    fence = object.__new__(fence_api.IntegrationPublicationFence)
    fence.project_root = candidate
    if case in {"outside", "changed"}:
        with pytest.raises(fence_api.PublicationFenceError,
                           match="PUBLICATION_FULL_CLOSURE_INVALID"):
            fence._recheck_full_profile(replay, None, preparation, phase="TEST_ONLY")
    else:
        actual = fence._recheck_full_profile(replay, None, preparation, phase="TEST_ONLY")
        assert actual == observation


def test_validation_runner_renews_same_lease_during_real_execution(tmp_path: Path) -> None:
    from dataclasses import replace

    from scripts.run_validation_tier import _run_leased_command

    store, lease, request, env, _now = _case(tmp_path)
    # Bounded test-only policy; real wall-clock and native process, not a fake clock.
    store.policy = replace(store.policy, lease_ttl_seconds=1)
    request["argv"] = [sys.executable, "-c", "import time; time.sleep(2); print('completed')"]
    before = datetime.now(UTC)
    lifecycle = store.execution_lifecycle()
    result = _run_leased_command(
        request=request,
        lifecycle=lifecycle,
        actor=ACTOR,
        environment=env,
    )
    assert result["exit_code"] == 0 and "completed" in result["pytest_output"]
    replay = store.replay()
    assert len(replay.lease_heads) == 1
    head = replay.active_leases[0]
    assert head.lease_id == lease.lease_id
    assert datetime.fromisoformat(head.execution["exit"]["observed_at"]) > before + timedelta(
        seconds=1
    )
    assert datetime.fromisoformat(head.expires_at) > datetime.fromisoformat(
        head.execution["exit"]["observed_at"]
    )
    assert head.execution["state"] == "EXIT_CONFIRMED"
    lifecycle.record_incomplete_result(lease.lease_id, actor=ACTOR)
    store.release(lease.lease_id, actor=ACTOR, now=datetime.now(UTC), evidence_refs=())


def test_reserved_execution_cannot_be_expired_released_or_relaunched(tmp_path: Path) -> None:
    store, lease, request, _env, now = _case(tmp_path)
    lifecycle = store.execution_lifecycle()
    assert lifecycle.reserve(request, actor=ACTOR)["dispatch_allowed"] is True
    assert lifecycle.reserve(request, actor=ACTOR)["status"] == "REPLAY_ONLY"
    changed = {**request, "argv": [sys.executable, "-c", "print('different')"]}
    with pytest.raises(ParallelControlError, match="REQUEST_REUSE_MISMATCH"):
        lifecycle.reserve(changed, actor=ACTOR)
    future = now + timedelta(seconds=store.policy.lease_ttl_seconds + 10)
    with pytest.raises(ParallelControlError, match="LEASE_EXECUTION_NOT_TERMINAL"):
        store.release(lease.lease_id, actor=ACTOR, now=future, evidence_refs=())
    with pytest.raises(ParallelControlError, match="LEASE_EXECUTION_NOT_TERMINAL"):
        store.expire(lease.lease_id, actor=ACTOR, now=future, reason_code="SYNTHETIC")
    with store.atomic(actor=ACTOR, now=future):
        assert not store._expire_stale_heads(store.replay(), actor=ACTOR, now=future)
    assert store.replay().active_leases[0].execution["state"] == "RESERVED"
    assert lifecycle.recover(lease.lease_id, actor=ACTOR)["status"] == "OBSERVE_ONLY"


@pytest.mark.parametrize("fault", ["status", "manifest", "change", "policy"])
def test_x04_invalid_readiness_cannot_obstruct_later_waiter(tmp_path, fault):
    from dataclasses import replace

    policy = load_parallel_control_policy(POLICY_PATH)
    task_id = "ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE"
    holder, invalid, eligible = [_task(task_id, change_id=name)
                                 for name in ("invalid-holder", "invalid-first", "valid-later")]
    graph = validate_dependency_graph([task_id], [])
    store = FileExecutionLeaseStore(tmp_path / "leases", policy=policy)
    now = datetime.now(UTC)

    def readiness(task):
        return evaluate_task_readiness(task, dependencies=[], observed_statuses={},
                                       graph_report=graph, current_base_commit=BASE_COMMIT,
                                       policy=policy)

    def acquire(task, decision=None):
        return store.acquire(task=task, readiness=decision or readiness(task), lane_id="domain-01",
                             actor=ACTOR, current_base_commit=BASE_COMMIT, now=now)

    try:
        active = acquire(holder)
        refused = acquire(invalid)
        waiting = acquire(eligible)
        assert active.status == "ACTIVE"
        assert refused.status == waiting.status == "BLOCKED"
        store.release(active.lease.lease_id, actor=ACTOR, now=now, evidence_refs=())
        assert not store.replay().active_leases
        original = readiness(invalid)
        faults = {"status": {"status": "BLOCKED"}, "manifest": {"manifest_sha256": "0" * 64},
                  "change": {"change_id": "another-change"},
                  "policy": {"policy_version": original.policy_version + "-stale"}}
        stale = replace(original, **faults[fault])
        before = store.replay()
        old_paths = list(store.events_root.rglob("*.json"))
        old_bytes = {path: path.read_bytes() for path in old_paths}
        with pytest.raises(ParallelControlError, match="LEASE_READINESS_REQUIRED"):
            acquire(invalid, stale)
        assert store.replay() == before
        assert {path: path.read_bytes() for path in old_paths} == old_bytes
        admitted = acquire(eligible)
        assert admitted.status == "ACTIVE"
        assert admitted.lease.previous_lease_id == waiting.lease.lease_id
        assert [row.lease_id for row in store.replay().active_leases] == [admitted.lease.lease_id]
        store.release(admitted.lease.lease_id, actor=ACTOR, now=now, evidence_refs=())
        assert store.replay().status == "PASS" and not store.replay().active_leases
        # The invalid request may proceed only with its original exact valid
        # readiness. A later stale recovery request must not consume its slot.
        restored = acquire(invalid)
        assert restored.status == "ACTIVE"
        store.expire(restored.lease.lease_id, actor=ACTOR, now=now, reason_code="EXECUTION_FAILED")
        before_recovery = store.replay()
        with pytest.raises(ParallelControlError, match="LEASE_READINESS_REQUIRED"):
            store.reassign(restored.lease.lease_id, task=invalid, readiness=stale,
                           lane_id="domain-01", actor="recovery-agent",
                           current_base_commit=BASE_COMMIT, now=now)
        assert store.replay() == before_recovery
        recovered = store.reassign(
            restored.lease.lease_id, task=invalid, readiness=original,
            lane_id="domain-01", actor="recovery-agent", current_base_commit=BASE_COMMIT, now=now,
        )
        assert recovered.status == "ACTIVE"
        store.release(recovered.lease.lease_id, actor="recovery-agent", now=now, evidence_refs=())
        assert not store.replay().active_leases
    finally:
        for lease in store.replay().active_leases:
            store.release(lease.lease_id, actor=lease.actor, now=now, evidence_refs=())


def test_blocked_admission_retry_preserves_execution_reassignment_quota(tmp_path):
    policy = load_parallel_control_policy(POLICY_PATH)
    task_id = "ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE"
    holder = _task(task_id, change_id="finite-holder")
    waiter = _task(task_id, change_id="finite-waiter")
    graph = validate_dependency_graph([task_id], [])
    store = FileExecutionLeaseStore(tmp_path / "leases", policy=policy)
    now = datetime.now(UTC)

    def readiness(task):
        return evaluate_task_readiness(task, dependencies=[], observed_statuses={},
                                       graph_report=graph, current_base_commit=BASE_COMMIT,
                                       policy=policy)

    def acquire(task, actor=ACTOR):
        return store.acquire(task=task, readiness=readiness(task), lane_id="domain-01",
                             actor=actor, current_base_commit=BASE_COMMIT, now=now)

    active = acquire(holder)
    blocked = acquire(waiter)
    assert active.status == "ACTIVE" and blocked.status == "BLOCKED"
    store.release(active.lease.lease_id, actor=ACTOR, now=now, evidence_refs=())
    before = store.replay()
    with pytest.raises(ParallelControlError, match="LEASE_IDENTITY_CONFLICT"):
        acquire(waiter, actor="research-agent")
    assert store.replay() == before
    admitted = acquire(waiter)
    assert admitted.lease.generation == 2
    assert admitted.lease.previous_lease_id == blocked.lease.lease_id
    store.expire(admitted.lease.lease_id, actor=ACTOR, now=now, reason_code="EXECUTION_FAILED")
    recovered = store.reassign(
        admitted.lease.lease_id, task=waiter, readiness=readiness(waiter), lane_id="domain-01",
        actor="recovery-agent", current_base_commit=BASE_COMMIT, now=now,
    )
    assert recovered.lease.generation == 3
    store.expire(recovered.lease.lease_id, actor="recovery-agent", now=now,
                 reason_code="EXECUTION_FAILED")
    before = store.replay()
    with pytest.raises(ParallelControlError, match="LEASE_REASSIGNMENT_LIMIT"):
        store.reassign(recovered.lease.lease_id, task=waiter, readiness=readiness(waiter),
                       lane_id="domain-01", actor="recovery-agent",
                       current_base_commit=BASE_COMMIT, now=now)
    assert store.replay() == before
    assert store.replay().status == "PASS" and not store.replay().active_leases


@pytest.mark.parametrize("fault", [
    "none", "recovery", "missing_event", "wrong_kind", "boolean_attempt",
    "recovery_without_parent", "first_with_parent", "outside_transaction", "full_field",
])
def test_publication_request_contract_cannot_dispatch_as_generic_execution(tmp_path, fault):
    from ai_trading_system.platform.architecture.workflow_coordination import _request

    store, lease, source, _environment, now = _case(tmp_path)
    request = {key: value for key, value in source.items() if key != "validation_identity_sha256"}
    request.update(
        schema_version="workflow_execution_request.v5",
        execution_kind="CONTROLLED_LOCAL_PUBLICATION",
        publication_transaction_path=(tmp_path / "outputs/transaction.json").as_posix(),
        publication_transaction_sha256="a" * 64,
        local_publication_event_id="b" * 64,
        local_publication_intent_sha256="c" * 64,
        full_execution_sha256="d" * 64,
        expected_main_sha="e" * 40,
        publication_action="PUBLISH",
        publication_attempt=1,
        previous_publication_sha256=None,
    )
    expected = "PUBLICATION_PARENT_REQUIRED"
    if fault == "recovery":
        request.update(publication_action="RECOVER", publication_attempt=2,
                       previous_publication_sha256="f" * 64)
    elif fault == "missing_event":
        del request["local_publication_event_id"]
        expected = "REQUEST_FIELDS"
    elif fault == "wrong_kind":
        request["execution_kind"] = "CONTROLLED_SOURCE_INSTALLATION"
        expected = "REQUEST_KIND"
    elif fault == "boolean_attempt":
        request["publication_attempt"] = True
        expected = "PUBLICATION_ATTEMPT"
    elif fault == "recovery_without_parent":
        request.update(publication_action="RECOVER", publication_attempt=2)
        expected = "IDENTITY"
    elif fault == "first_with_parent":
        request["previous_publication_sha256"] = "f" * 64
        expected = "PUBLICATION_ATTEMPT"
    elif fault == "outside_transaction":
        request["publication_transaction_path"] = (tmp_path.parent / "foreign.json").as_posix()
        expected = "PUBLICATION_TRANSACTION_PATH"
    elif fault == "full_field":
        request["validation_identity_sha256"] = "f" * 64
        expected = "REQUEST_FIELDS"
    before = store.replay()
    try:
        if fault in {"none", "recovery"}:
            assert _request(request) == request  # Shape only, not Full/publication evidence.
        with pytest.raises(ParallelControlError, match="LEASE_EXECUTION_" + expected):
            store.execution_lifecycle().reserve(request, actor=ACTOR)
        assert store.replay() == before
        assert store.replay().active_leases[0].execution is None
    finally:
        store.release(lease.lease_id, actor=ACTOR, now=now, evidence_refs=())


def _installation_test_request(root: Path, source, *, status="PASS", stable="SOURCE_INSTALLED"):
    """Real small worker: proves lifecycle only, never checkout installation."""
    request = dict(source)
    token = uuid.uuid4().hex
    request_path = root / (token + ".request.json")
    program = """
import json, sys, time
from pathlib import Path
from ai_trading_system.platform.architecture.parallel_control_kernel import (
    FileExecutionLeaseStore, load_parallel_control_policy)
from ai_trading_system.platform.architecture.workflow_coordination import InstallationLifecycle
request=json.loads(Path(sys.argv[1]).read_text())
store=FileExecutionLeaseStore(Path(request['cwd'])/'leases',
    policy=load_parallel_control_policy(Path(sys.argv[2])))
installation=request['schema_version']=='workflow_execution_request.v4'
lifecycle=InstallationLifecycle(store) if installation else store.execution_lifecycle()
admit=(lifecycle.require_installation_worker if installation
       else lifecycle.require_source_candidate_worker)
witness=admit(request, actor=sys.argv[3])
Path(sys.argv[1]+'.witness').write_text(json.dumps(witness))
deadline=time.monotonic()+40
while not Path(sys.argv[1]+'.release').exists():
    if time.monotonic()>deadline: raise RuntimeError('worker release timeout')
    time.sleep(.02)
keys=['request_id','execution_kind','source_head_sha','source_request_sha256',
      'source_transaction_sha256','review_sha256']
if installation:
    keys+=['installed_candidate_sha','source_result_sha256','installation_plan_sha256',
           'installation_action','installation_attempt','previous_installation_sha256']
result={key:request[key] for key in keys}
result['schema_version']=('controlled_source_installation_worker_result.v1' if installation
                          else 'controlled_source_candidate_worker_result.v1')
result.update(status=sys.argv[4], stable_state=sys.argv[5])
Path(request['result_path']).write_text(json.dumps(result))
"""
    request.update(
        request_id=token,
        job_name="Local\\AITS-DEVX015-install-test-" + token,
        stdout_path=(root / (token + ".log")).as_posix(),
        result_path=(root / (token + ".result.json")).as_posix(),
        argv=[
            sys.executable,
            "-c",
            program,
            str(request_path),
            str(Path(POLICY_PATH).resolve()),
            ACTOR,
            status,
            stable,
        ],
    )
    request_path.write_text(json.dumps(request), encoding="utf-8")
    return request


def _finish_installation_test_job(lifecycle, request, environment):
    from ai_trading_system.platform.architecture.workflow_execution import ExecutionContainmentError

    request_path = Path(request["argv"][3])
    with WindowsJobProcess.create(
        argv=request["argv"],
        cwd=Path(request["cwd"]),
        environment=environment,
        stdout_path=Path(request["stdout_path"]),
        job_name=request["job_name"],
    ) as process:
        lifecycle.bind(request["lease_id"], process, actor=ACTOR)
        lifecycle.resume(request["lease_id"], process, actor=ACTOR)
        witness_path = Path(str(request_path) + ".witness")
        _until(
            lambda: witness_path.exists() or process.poll() is not None,
            description="real typed installation/source admission",
        )
        assert witness_path.exists(), Path(request["stdout_path"]).read_text()
        witness = json.loads(witness_path.read_text())
        oracle = NativeOracle()
        with oracle.process(witness["worker_process"]["pid"]) as native:
            assert oracle.creation_time(native) == witness["worker_process"]["creation_time"]
            oracle.assert_in_job(native, request["job_name"])
            if request["schema_version"] == "workflow_execution_request.v4":
                with pytest.raises(ExecutionContainmentError, match="JOB_MEMBERSHIP_MISMATCH"):
                    lifecycle.require_installation_worker(request, actor=ACTOR)
                with pytest.raises(ExecutionContainmentError, match="JOB_MEMBERSHIP_MISMATCH"):
                    lifecycle.record_created_object(request, -1, actor=ACTOR)
            Path(str(request_path) + ".release").write_text("release")
            assert process.wait(timeout=20) == 0
            assert oracle.exited(native, 10)
        lifecycle.confirm_exit(request["lease_id"], process, actor=ACTOR)
        if (
            request["schema_version"] == "workflow_execution_request.v4"
            and json.loads(Path(request["result_path"]).read_text())["status"] == "PASS"
        ):
            before = tuple(lifecycle.store.replay().head_event_ids)
            with pytest.raises(
                ParallelControlError, match="INSTALLATION_STABLE_VERIFICATION_REQUIRED"
            ):
                lifecycle.record_result(
                    request["lease_id"],
                    actor=ACTOR,
                    result_path=Path(request["result_path"]),
                )
            assert tuple(lifecycle.store.replay().head_event_ids) == before
            lifecycle.record_incomplete_result(request["lease_id"], actor=ACTOR)
        else:
            lifecycle.record_result(
                request["lease_id"],
                actor=ACTOR,
                result_path=Path(request["result_path"]),
            )
    oracle.assert_job_absent(request["job_name"])


def _installation_source_case(root: Path):
    store, lease, base, environment, now = _case(root)
    # The real Job runs in the fixture checkout, so a relative inherited src
    # would select the installed main package instead of this exact SUT.
    environment["PYTHONPATH"] = (Path(__file__).resolve().parents[1] / "src").as_posix()
    base["environment_sha256"] = execution_environment_sha256(environment)
    source = {
        k: v for k, v in base.items() if k not in {"candidate_sha", "validation_identity_sha256"}
    }
    source.update(
        schema_version="workflow_execution_request.v3",
        execution_kind="CONTROLLED_SOURCE_CANDIDATE",
        source_head_sha=lease.base_commit,
        source_request_sha256="d" * 64,
        source_transaction_sha256="e" * 64,
        review_sha256="f" * 64,
    )
    source = _installation_test_request(root, source)
    lifecycle = store.execution_lifecycle()
    assert lifecycle.reserve(source, actor=ACTOR)["dispatch_allowed"]
    _finish_installation_test_job(lifecycle, source, environment)
    execution = store.replay().active_leases[0].execution
    installation = {
        **source,
        "schema_version": "workflow_execution_request.v4",
        "execution_kind": "CONTROLLED_SOURCE_INSTALLATION",
        "installed_candidate_sha": "1" * 40,
        "source_result_sha256": execution["result"]["artifact"]["sha256"],
        "installation_plan_sha256": "2" * 64,
        "installation_action": "INSTALL",
        "installation_attempt": 1,
        "previous_installation_sha256": None,
    }
    return store, lease, installation, environment, now, execution


@pytest.mark.parametrize("recover_failed", [False, True], ids=["install", "failed-then-recover"])
def test_installation_attempts_real_job_rejects_self_reported_stability_and_keeps_custody(
    tmp_path: Path,
    recover_failed: bool,
) -> None:
    from ai_trading_system.platform.architecture.workflow_coordination import InstallationLifecycle

    store, lease, base, environment, now, original_source = _installation_source_case(tmp_path)
    lifecycle = InstallationLifecycle(store)
    request = _installation_test_request(
        tmp_path,
        base,
        status="FAIL" if recover_failed else "PASS",
    )
    future = now + timedelta(seconds=store.policy.lease_ttl_seconds + 10)
    before = tuple(store.replay().head_event_ids)
    with pytest.raises(ParallelControlError, match="LEASE_EXPIRED"):
        lifecycle.reserve(request, actor=ACTOR, now=future)
    with pytest.raises(ParallelControlError, match="INSTALLATION_SOURCE_BINDING"):
        lifecycle.reserve({**request, "source_result_sha256": "0" * 64}, actor=ACTOR)
    assert tuple(store.replay().head_event_ids) == before
    assert lifecycle.reserve(request, actor=ACTOR)["dispatch_allowed"]
    assert lifecycle.reserve(request, actor=ACTOR)["status"] == "REPLAY_ONLY"
    before = tuple(store.replay().head_event_ids)
    with pytest.raises(ParallelControlError, match="REQUEST_REUSE_MISMATCH"):
        lifecycle.reserve({**request, "installation_plan_sha256": "3" * 64}, actor=ACTOR)
    assert tuple(store.replay().head_event_ids) == before
    _finish_installation_test_job(lifecycle, request, environment)
    if recover_failed:
        with pytest.raises(ParallelControlError, match="LEASE_EXECUTION_NOT_TERMINAL"):
            store.release(lease.lease_id, actor=ACTOR, now=future, evidence_refs=())
        with pytest.raises(ParallelControlError, match="LEASE_EXECUTION_NOT_TERMINAL"):
            store.expire(lease.lease_id, actor=ACTOR, now=future, reason_code="SYNTHETIC")
        with store.atomic(actor=ACTOR, now=future):
            assert not store._expire_stale_heads(store.replay(), actor=ACTOR, now=future)
        task = _task("ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE")
        readiness = evaluate_task_readiness(
            task,
            dependencies=[],
            observed_statuses={},
            graph_report=validate_dependency_graph([task.task_id], []),
            current_base_commit=BASE_COMMIT,
            policy=store.policy,
        )
        competitor = store.acquire(
            task=task,
            readiness=readiness,
            lane_id="competing-installation",
            actor="research-agent",
            current_base_commit=BASE_COMMIT,
            now=future,
        )
        assert competitor.status == "BLOCKED"
        assert store.replay().active_leases[0].lease_id == lease.lease_id
        previous = store.replay().active_leases[0].execution["installation_attempts"][-1]
        digest = hashlib.sha256(
            json.dumps(previous, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        ).hexdigest()
        recovery = _installation_test_request(
            tmp_path,
            {
                **base,
                "installation_action": "RECOVER",
                "installation_attempt": 2,
                "previous_installation_sha256": digest,
                "environment_sha256": execution_environment_sha256(
                    {**environment, "DEVX015_RECOVERY_SESSION": "fresh-process"}
                ),
            },
            stable="SOURCE_RESTORED",
        )
        before = tuple(store.replay().head_event_ids)
        for key in ("installation_plan_sha256", "previous_installation_sha256"):
            with pytest.raises(ParallelControlError, match="INSTALLATION_RECOVERY_BINDING"):
                lifecycle.reserve({**recovery, key: "0" * 64}, actor=ACTOR, now=future)
            assert tuple(store.replay().head_event_ids) == before
        with pytest.raises(ParallelControlError, match="INSTALLATION_ATTEMPT"):
            lifecycle.reserve(
                {**recovery, "installation_action": "INSTALL"},
                actor=ACTOR,
                now=future,
            )
        assert tuple(store.replay().head_event_ids) == before
        assert lifecycle.reserve(recovery, actor=ACTOR, now=future)["dispatch_allowed"]
        assert recovery["environment_sha256"] != request["environment_sha256"]
        reserved_events = tuple(store.replay().head_event_ids)
        with WindowsJobProcess.create(
            argv=recovery["argv"],
            cwd=Path(recovery["cwd"]),
            environment=environment,
            stdout_path=Path(recovery["stdout_path"]),
            job_name=recovery["job_name"],
        ) as wrong_process:
            assert {
                key
                for key, value in wrong_process.launch_binding().items()
                if recovery.get(key) != value
            } == {"environment_sha256"}
            with pytest.raises(ParallelControlError, match="LAUNCH_BINDING_MISMATCH"):
                lifecycle.bind(recovery["lease_id"], wrong_process, actor=ACTOR)
        assert tuple(store.replay().head_event_ids) == reserved_events
        assert not Path(str(Path(recovery["argv"][3])) + ".witness").exists()
        wrong_log = Path(recovery["stdout_path"])
        assert wrong_log.read_bytes() == b"", "rejected suspended child must not execute"
        wrong_log.rename(wrong_log.with_suffix(".rejected-environment.log"))
        environment = {**environment, "DEVX015_RECOVERY_SESSION": "fresh-process"}
        _finish_installation_test_job(lifecycle, recovery, environment)
    outer = store.replay().active_leases[0].execution
    assert outer["schema_version"] == "lease_execution.v2"
    assert {
        **{k: v for k, v in outer.items() if k != "installation_attempts"},
        "schema_version": "lease_execution.v1",
    } == original_source
    assert len(outer["installation_attempts"]) == (2 if recover_failed else 1)
    assert outer["installation_attempts"][-1]["result"]["status"] == "INSUFFICIENT"
    with pytest.raises(ParallelControlError, match="LEASE_EXECUTION_NOT_TERMINAL"):
        store.release(lease.lease_id, actor=ACTOR, now=future, evidence_refs=())
    assert lifecycle.reserve(request, actor=ACTOR)["status"] == "REPLAY_ONLY"


def test_installation_creation_schema_preserves_history_and_old_events(tmp_path: Path) -> None:
    """Structural transition tests, not evidence that a file was created by a Job."""
    from dataclasses import replace

    from ai_trading_system.platform.architecture.workflow_coordination import (
        InstallationLifecycle,
        validate_execution,
        validate_execution_transition,
    )

    store, lease, base, _environment, _now, _source = _installation_source_case(tmp_path)
    lifecycle = InstallationLifecycle(store)
    request = _installation_test_request(tmp_path, base)
    reserved = lifecycle.reserve(request, actor=ACTOR)["execution"]
    assert reserved["schema_version"] == "lease_execution.v3"
    assert reserved["created_objects"] == []
    before_events = tuple(store.replay().head_event_ids)
    value = json.loads(json.dumps(reserved))
    value.update(state="RUNNING", process=dict(value["launcher"]))
    prior = replace(lease, execution=value)
    validate_execution(prior)
    old = {key: item for key, item in value.items() if key != "created_objects"}
    old["schema_version"] = "lease_execution.v1"
    validate_execution(replace(lease, execution=old))
    target = tmp_path / "schema-object.bin"
    target.write_bytes(b"")
    root_info, info = tmp_path.stat(), target.stat()
    record = {
        "plan_sha256": request["installation_plan_sha256"],
        "root": tmp_path.as_posix(),
        "root_identity": [root_info.st_dev, root_info.st_ino],
        "path": target.name,
        "file_identity": [info.st_dev, info.st_ino],
        "target_sha256": hashlib.sha256(b"planned").hexdigest(),
        "worker_process": dict(value["process"]),
    }
    added = {**value, "created_objects": [record]}
    validate_execution_transition(prior, replace(lease, execution=added))
    validate_execution_transition(replace(lease, execution=added), replace(lease, execution=added))
    directory_record = {key: item for key, item in record.items() if key != "target_sha256"}
    directory_record.update(kind="directory", path="created-directory")
    directory_added = {**value, "created_objects": [directory_record]}
    validate_execution_transition(prior, replace(lease, execution=directory_added))
    for malformed in (
        {**directory_record, "kind": "file"},
        {**directory_record, "target_sha256": "a" * 64},
    ):
        with pytest.raises(ParallelControlError, match="CREATION_RECORD"):
            validate_execution(replace(lease, execution={**value, "created_objects": [malformed]}))
    for mutation, code in (
        ("plan", "CREATION_PLAN"),
        ("boolean-id", "CREATION_IDENTITY"),
        ("volume", "CREATION_IDENTITY"),
        ("duplicate", "CREATION_DUPLICATE"),
        ("reserved", "CREATION_STATE"),
        ("drop", "CREATION_HISTORY_CHANGED"),
        ("replace", "CREATION_HISTORY_CHANGED"),
        ("append-two", "CREATION_HISTORY_CHANGED"),
        ("schema-downgrade", "CREATION_HISTORY_CHANGED"),
    ):
        changed = json.loads(json.dumps(added))
        comparison = prior
        if mutation == "plan":
            changed["created_objects"][0]["plan_sha256"] = "0" * 64
        elif mutation == "boolean-id":
            changed["created_objects"][0]["file_identity"][1] = True
        elif mutation == "volume":
            changed["created_objects"][0]["file_identity"][0] += 1
        elif mutation == "duplicate":
            changed["created_objects"].append({**record, "path": record["path"].upper()})
        elif mutation == "reserved":
            changed["state"] = "RESERVED"
        elif mutation == "append-two":
            changed["created_objects"].append({**record, "path": "other.bin"})
        else:
            comparison = replace(lease, execution=added)
            if mutation == "drop":
                changed["created_objects"] = []
            elif mutation == "replace":
                changed["created_objects"][0]["file_identity"][1] += 1
            else:
                changed = old
        with pytest.raises(ParallelControlError, match=code):
            validate_execution_transition(comparison, replace(lease, execution=changed))
    assert tuple(store.replay().head_event_ids) == before_events


def test_checkpoint_typed_execution_uses_real_source_lease_and_job_member(tmp_path: Path) -> None:
    from test_arch_005_source_preservation import POLICIES
    from test_arch_005_task_checkpoint import RUNTIME
    from test_arch_005_task_checkpoint import _case as checkpoint_case

    from ai_trading_system.platform.architecture.checkout_guard import (
        CHECKOUT_SOURCE_ONLY_PROFILE,
        CheckoutLeaseGuard,
        CheckoutOperationClass,
    )
    from ai_trading_system.platform.architecture.workflow_execution import ExecutionContainmentError

    source = checkpoint_case(tmp_path)
    actor = source.scope["actor"]
    guard = CheckoutLeaseGuard(
        project_root=source.root,
        policy_path=source.root / POLICIES[1],
        parallel_policy_path=source.root / POLICIES[2],
    )
    intent_id = "task-checkpoint-" + uuid.uuid4().hex
    decision, handle = guard.acquire(
        intent_id=intent_id,
        task_id=source.scope["task_id"],
        thread_id=source.scope["thread_id"],
        actor=actor,
        operation_class=CheckoutOperationClass.SHARED_MUTATION,
        shared_paths=[*source.scope["paths"], RUNTIME],
        inspection_profile=CHECKOUT_SOURCE_ONLY_PROFILE,
        base_commit=source.scope["source_head_sha"],
    )
    assert decision.status == "PASS" and handle is not None
    lease = next(
        item for item in guard.store.replay().active_leases if item.lease_id == handle.lease_id
    )
    root = source.root / RUNTIME / "typed-execution-test"
    root.mkdir(parents=True)
    request_path, admitted, release = (
        root / "execution.json",
        root / "admitted.json",
        root / "release",
    )
    environment = {
        **os.environ,
        "PYTHONPATH": str(source.main_root / "src"),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    program = """
import json, os, sys, time
from pathlib import Path
from ai_trading_system.platform.architecture.checkout_guard import CheckoutLeaseGuard
from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
request=json.loads(Path(sys.argv[1]).read_text())
root=Path(request['source_root'])
guard=CheckoutLeaseGuard(
    project_root=root, policy_path=root/sys.argv[4], parallel_policy_path=root/sys.argv[5])
bad={**request, 'checkpoint_request_sha256':'0'*64}
try:
    guard.store.execution_lifecycle().require_checkpoint_worker(bad, actor=sys.argv[6])
except ParallelControlError as error:
    assert error.code == 'LEASE_EXECUTION_CHECKPOINT_WORKER_STATE'
else:
    raise AssertionError('contained worker accepted changed checkpoint payload')
witness=guard.store.execution_lifecycle().require_checkpoint_worker(request, actor=sys.argv[6])
Path(sys.argv[2]).write_text(json.dumps(witness))
deadline=time.monotonic()+30
while not Path(sys.argv[3]).exists():
    if time.monotonic()>deadline: raise RuntimeError('synthetic release deadline')
    time.sleep(0.01)
keys=('request_id','execution_kind','source_head_sha','checkpoint_id',
      'checkpoint_task_id','checkpoint_request_sha256','checkpoint_intent_sha256')
result={key:request[key] for key in keys}
result.update(schema_version='task_checkpoint_worker_result.v1', status='PASS')
Path(request['result_path']).write_text(json.dumps(result))
"""
    request = {
        "schema_version": "workflow_execution_request.v2",
        "execution_kind": "TASK_SOURCE_CAPTURE",
        "request_id": uuid.uuid4().hex,
        "lease_id": lease.lease_id,
        "manifest_sha256": lease.change_manifest_sha256,
        "source_root": source.root.as_posix(),
        "source_head_sha": source.scope["source_head_sha"],
        "checkpoint_id": source.scope["checkpoint_id"],
        "checkpoint_task_id": source.scope["task_id"],
        "checkpoint_thread_id": source.scope["thread_id"],
        # This fixture freezes a real source scope, not a completed checkpoint.
        "checkpoint_request_sha256": hashlib.sha256(
            json.dumps(source.scope, sort_keys=True).encode()
        ).hexdigest(),
        "checkpoint_intent_id": intent_id,
        "checkpoint_intent_sha256": hashlib.sha256(decision.intent_path.read_bytes()).hexdigest(),
        "scope_intent_sha256": source.scope["scope_intent"]["sha256"],
        "argv": [
            sys.executable,
            "-c",
            program,
            str(request_path),
            str(admitted),
            str(release),
            POLICIES[1],
            POLICIES[2],
            actor,
        ],
        "cwd": source.main_root.as_posix(),
        "environment_sha256": execution_environment_sha256(environment),
        "stdout_path": (root / "stdout.log").as_posix(),
        "result_path": (root / "result.json").as_posix(),
        "job_name": "Local\\AITS-DEVX015-checkpoint-" + uuid.uuid4().hex,
        "host_id": "synthetic-host",
        "writer_epoch": "synthetic-v3",
        "subject_task_id": lease.task_id,
    }
    request_path.write_text(json.dumps(request), encoding="utf-8")
    lifecycle = guard.store.execution_lifecycle()
    original_events = tuple(guard.store.replay().head_event_ids)
    for bad in (
        {**request, "candidate_sha": source.scope["source_head_sha"]},
        {**request, "validation_identity_sha256": "b" * 64},
        {**request, "execution_kind": "VALIDATION"},
        {**request, "source_head_sha": "0" * 40},
        {**request, "checkpoint_intent_id": "task-checkpoint-" + "0" * 32},
        {**request, "subject_task_id": source.scope["task_id"]},
    ):
        with pytest.raises(ParallelControlError):
            lifecycle.reserve(bad, actor=actor)
        assert tuple(guard.store.replay().head_event_ids) == original_events
    assert lifecycle.reserve(request, actor=actor)["dispatch_allowed"] is True
    with pytest.raises(ParallelControlError, match="CHECKPOINT_WORKER_STATE"):
        lifecycle.require_checkpoint_worker(request, actor=actor)
    with WindowsJobProcess.create(
        argv=request["argv"],
        cwd=source.main_root,
        environment=environment,
        stdout_path=Path(request["stdout_path"]),
        job_name=request["job_name"],
    ) as process:
        lifecycle.bind(lease.lease_id, process, actor=actor)
        lifecycle.resume(lease.lease_id, process, actor=actor)
        _until(
            lambda: admitted.exists() or process.poll() is not None,
            description="typed checkpoint worker admission or terminal process",
        )
        assert admitted.exists(), Path(request["stdout_path"]).read_text(encoding="utf-8")
        witness = json.loads(admitted.read_text())
        assert witness["status"] == "PASS" and witness["worker_process"]["pid"] != os.getpid()
        oracle = NativeOracle()
        with oracle.process(witness["worker_process"]["pid"]) as native:
            assert oracle.creation_time(native) == witness["worker_process"]["creation_time"]
            oracle.assert_in_job(native, request["job_name"])
        with pytest.raises(ExecutionContainmentError, match="JOB_MEMBERSHIP_MISMATCH"):
            lifecycle.require_checkpoint_worker(request, actor=actor)
        release.write_text("release", encoding="utf-8")
        assert process.wait(timeout=20) == 0
        lifecycle.confirm_exit(lease.lease_id, process, actor=actor)
        result_path = Path(request["result_path"])
        original_result = result_path.read_bytes()
        value = json.loads(original_result)
        value["candidate_sha"] = source.scope["source_head_sha"]
        result_path.write_text(json.dumps(value))
        with pytest.raises(ParallelControlError, match="CHECKPOINT_RESULT_KIND"):
            lifecycle.record_result(lease.lease_id, actor=actor, result_path=result_path)
        result_path.write_bytes(original_result)
        lifecycle.record_result(lease.lease_id, actor=actor, result_path=result_path)
    handle.release(outcome="completed")
    final = next(
        item for item in guard.store.replay().lease_heads if item.lease_id == lease.lease_id
    )
    assert final.state == "RELEASED" and final.execution["result"]["status"] == "PASS"
    assert lifecycle.reserve(request, actor=actor)["status"] == "REPLAY_ONLY"


def test_source_candidate_typed_execution_admits_actual_job_worker(tmp_path: Path) -> None:
    from ai_trading_system.platform.architecture.workflow_execution import ExecutionContainmentError

    store, lease, base, environment, _now = _case(tmp_path)
    lifecycle = store.execution_lifecycle()
    request_path = tmp_path / "execution.json"
    admitted = tmp_path / "admitted.json"
    release = tmp_path / "release"
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    program = """
import json, sys, time
from pathlib import Path
from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
from ai_trading_system.platform.architecture.parallel_control_kernel import (
    FileExecutionLeaseStore, load_parallel_control_policy)
request=json.loads(Path(sys.argv[1]).read_text())
lifecycle=FileExecutionLeaseStore(
    Path(request['cwd'])/'leases',
    policy=load_parallel_control_policy(Path(sys.argv[4]))).execution_lifecycle()
for key, changed in (
    ('request_id', 'different-request'), ('source_request_sha256', '0'*64),
    ('source_transaction_sha256', '0'*64), ('task_authority_sha256', '0'*64),
    ('review_sha256', '0'*64), ('source_head_sha', '0'*40),
    ('job_name', 'Local\\\\AITS-DEVX015-wrong-job-12345678')):
    try:
        lifecycle.require_source_candidate_worker({**request, key:changed}, actor=sys.argv[5])
    except ParallelControlError as error:
        assert error.code == 'LEASE_EXECUTION_SOURCE_CANDIDATE_WORKER_STATE'
    else:
        raise AssertionError('changed request admitted: '+key)
try:
    lifecycle.require_checkpoint_worker(request, actor=sys.argv[5])
except ParallelControlError as error:
    assert error.code == 'LEASE_EXECUTION_CHECKPOINT_REQUEST_REQUIRED'
else:
    raise AssertionError('v3 admitted as checkpoint')
witness=lifecycle.require_source_candidate_worker(request, actor=sys.argv[5])
Path(sys.argv[2]).write_text(json.dumps(witness))
deadline=time.monotonic()+30
while not Path(sys.argv[3]).exists():
    if time.monotonic()>deadline: raise RuntimeError('synthetic release deadline')
    time.sleep(.01)
keys=('request_id','execution_kind','source_head_sha','source_request_sha256',
      'source_transaction_sha256','review_sha256')
result={key:request[key] for key in keys}
result.update(schema_version='controlled_source_candidate_worker_result.v1',status='PASS')
Path(request['result_path']).write_text(json.dumps(result))
"""
    request = {
        key: value
        for key, value in base.items()
        if key not in {"candidate_sha", "validation_identity_sha256"}
    }
    request.update(
        schema_version="workflow_execution_request.v3",
        execution_kind="CONTROLLED_SOURCE_CANDIDATE",
        source_head_sha=lease.base_commit,
        source_request_sha256="d" * 64,
        source_transaction_sha256="e" * 64,
        review_sha256="f" * 64,
        argv=[
            sys.executable,
            "-c",
            program,
            str(request_path),
            str(admitted),
            str(release),
            str(Path(POLICY_PATH).resolve()),
            ACTOR,
        ],
        environment_sha256=execution_environment_sha256(environment),
    )
    request_path.write_text(json.dumps(request), encoding="utf-8")
    before = tuple(store.replay().head_event_ids)
    malformed = [
        {**request, "candidate_sha": lease.base_commit},
        {**request, "validation_identity_sha256": "b" * 64},
        {**request, "execution_kind": "TASK_SOURCE_CAPTURE"},
        {**request, "lease_id": "other-lease"},
        {**request, "subject_task_id": "other-task"},
        {key: value for key, value in request.items() if key != "review_sha256"},
    ]
    malformed.extend(
        {**request, key: "invalid"}
        for key in (
            "source_head_sha",
            "source_request_sha256",
            "source_transaction_sha256",
            "task_authority_sha256",
            "review_sha256",
        )
    )
    for bad in malformed:
        with pytest.raises(ParallelControlError):
            lifecycle.reserve(bad, actor=ACTOR)
        assert tuple(store.replay().head_event_ids) == before
    with pytest.raises(ParallelControlError, match="SOURCE_CANDIDATE_LEASE_BINDING"):
        lifecycle.reserve({**request, "source_head_sha": "0" * 40}, actor=ACTOR)
    assert tuple(store.replay().head_event_ids) == before
    with pytest.raises(ParallelControlError, match="SOURCE_CANDIDATE_REQUEST_REQUIRED"):
        lifecycle.require_source_candidate_worker(base, actor=ACTOR)
    assert lifecycle.reserve(request, actor=ACTOR)["dispatch_allowed"] is True
    assert lifecycle.reserve(request, actor=ACTOR)["status"] == "REPLAY_ONLY"
    with pytest.raises(ParallelControlError, match="SOURCE_CANDIDATE_WORKER_STATE"):
        lifecycle.require_source_candidate_worker(request, actor=ACTOR)
    with WindowsJobProcess.create(
        argv=request["argv"],
        cwd=tmp_path,
        environment=environment,
        stdout_path=Path(request["stdout_path"]),
        job_name=request["job_name"],
    ) as process:
        lifecycle.bind(lease.lease_id, process, actor=ACTOR)
        lifecycle.resume(lease.lease_id, process, actor=ACTOR)
        _until(
            lambda: admitted.exists() or process.poll() is not None,
            description="source candidate worker admission or terminal process",
        )
        assert admitted.exists(), Path(request["stdout_path"]).read_text(encoding="utf-8")
        witness = json.loads(admitted.read_text())
        assert witness["status"] == "PASS"
        assert witness["effect_authority"] == "SAME_BOUND_LEASE"
        assert witness["worker_process"]["pid"] != os.getpid()
        oracle = NativeOracle()
        with oracle.process(witness["worker_process"]["pid"]) as native:
            assert oracle.creation_time(native) == witness["worker_process"]["creation_time"]
            oracle.assert_in_job(native, request["job_name"])
        # An observed witness cannot admit this live parent outside the Job.
        with pytest.raises(ExecutionContainmentError, match="JOB_MEMBERSHIP_MISMATCH"):
            lifecycle.require_source_candidate_worker(request, actor=ACTOR)
        with pytest.raises(ParallelControlError, match="ACTIVE_OWNER"):
            lifecycle.require_source_candidate_worker(request, actor="other-actor")
        release.write_text("release", encoding="utf-8")
        assert process.wait(timeout=20) == 0
        lifecycle.confirm_exit(lease.lease_id, process, actor=ACTOR)
        with pytest.raises(ParallelControlError, match="SOURCE_CANDIDATE_WORKER_STATE"):
            lifecycle.require_source_candidate_worker(request, actor=ACTOR)
        result_path = Path(request["result_path"])
        original = result_path.read_bytes()
        result = json.loads(original)
        for key in (
            "schema_version",
            "request_id",
            "execution_kind",
            "source_head_sha",
            "source_request_sha256",
            "source_transaction_sha256",
            "review_sha256",
        ):
            result_path.write_text(json.dumps({**result, key: "wrong"}))
            with pytest.raises(ParallelControlError, match="RESULT_BINDING"):
                lifecycle.record_result(lease.lease_id, actor=ACTOR, result_path=result_path)
        for key in ("candidate_sha", "validation_identity_sha256"):
            result_path.write_text(json.dumps({**result, key: "a" * 64}))
            with pytest.raises(ParallelControlError, match="SOURCE_CANDIDATE_RESULT_KIND"):
                lifecycle.record_result(lease.lease_id, actor=ACTOR, result_path=result_path)
        result_path.write_bytes(original)
        before_adoption = store.replay().lease_heads[0].execution
        with pytest.raises(ParallelControlError, match="RESULT_ADOPTION_CHANGED"):
            lifecycle.record_result(
                lease.lease_id,
                actor=ACTOR,
                result_path=result_path,
                expected_sha256="0" * 64,
            )
        assert store.replay().lease_heads[0].execution == before_adoption
        lifecycle.record_result(
            lease.lease_id,
            actor=ACTOR,
            result_path=result_path,
            expected_sha256=hashlib.sha256(original).hexdigest(),
        )
    store.release(lease.lease_id, actor=ACTOR, now=datetime.now(UTC), evidence_refs=())
    assert store.replay().status == "PASS"
    assert store.replay().lease_heads[0].execution["result"]["status"] == "PASS"
    assert lifecycle.reserve(request, actor=ACTOR)["status"] == "REPLAY_ONLY"


def test_source_candidate_execution_rejects_actual_source_only_lease(tmp_path: Path) -> None:
    from test_arch_005_source_preservation import POLICIES
    from test_arch_005_task_checkpoint import RUNTIME
    from test_arch_005_task_checkpoint import _case as checkpoint_case

    from ai_trading_system.platform.architecture.checkout_guard import (
        CHECKOUT_SOURCE_ONLY_PROFILE,
        CheckoutLeaseGuard,
        CheckoutOperationClass,
    )

    source = checkpoint_case(tmp_path)
    actor = source.scope["actor"]
    guard = CheckoutLeaseGuard(
        project_root=source.root,
        policy_path=source.root / POLICIES[1],
        parallel_policy_path=source.root / POLICIES[2],
    )
    decision, handle = guard.acquire(
        intent_id="task-checkpoint-" + uuid.uuid4().hex,
        task_id=source.scope["task_id"],
        thread_id=source.scope["thread_id"],
        actor=actor,
        operation_class=CheckoutOperationClass.SHARED_MUTATION,
        shared_paths=[*source.scope["paths"], RUNTIME],
        inspection_profile=CHECKOUT_SOURCE_ONLY_PROFILE,
        base_commit=source.scope["source_head_sha"],
    )
    assert decision.status == "PASS" and handle is not None
    lease = next(
        item for item in guard.store.replay().active_leases if item.lease_id == handle.lease_id
    )
    request = {
        "schema_version": "workflow_execution_request.v3",
        "execution_kind": "CONTROLLED_SOURCE_CANDIDATE",
        "request_id": uuid.uuid4().hex,
        "lease_id": lease.lease_id,
        "manifest_sha256": lease.change_manifest_sha256,
        "source_head_sha": lease.base_commit,
        "source_request_sha256": "a" * 64,
        "source_transaction_sha256": "b" * 64,
        "task_authority_sha256": "c" * 64,
        "review_sha256": "d" * 64,
        "argv": [sys.executable, "-c", "raise AssertionError('must not dispatch')"],
        "cwd": source.root.as_posix(),
        "environment_sha256": execution_environment_sha256(dict(os.environ)),
        "stdout_path": (source.root / RUNTIME / "stdout.log").as_posix(),
        "result_path": (source.root / RUNTIME / "result.json").as_posix(),
        "job_name": "Local\\AITS-DEVX015-source-reject-" + uuid.uuid4().hex,
        "host_id": "synthetic-host",
        "writer_epoch": "synthetic-v3",
        "subject_task_id": lease.task_id,
    }
    before = tuple(guard.store.replay().head_event_ids)
    with pytest.raises(ParallelControlError, match="SOURCE_CANDIDATE_LEASE_BINDING"):
        guard.store.execution_lifecycle().reserve(request, actor=actor)
    assert tuple(guard.store.replay().head_event_ids) == before
    assert (
        next(
            item for item in guard.store.replay().active_leases if item.lease_id == lease.lease_id
        ).execution
        is None
    )
    assert not Path(request["stdout_path"]).exists()
    assert not Path(request["result_path"]).exists()
    handle.release(outcome="completed")


def test_checkpoint_execution_rejects_ordinary_non_source_lease(tmp_path: Path) -> None:
    store, lease, base, _environment, _now = _case(tmp_path)
    request = {
        key: value
        for key, value in base.items()
        if key not in {"candidate_sha", "validation_identity_sha256", "task_authority_sha256"}
    }
    request.update(
        schema_version="workflow_execution_request.v2",
        execution_kind="TASK_SOURCE_CAPTURE",
        source_root=tmp_path.as_posix(),
        source_head_sha=lease.base_commit,
        checkpoint_id="synthetic-capture",
        checkpoint_task_id="synthetic-task",
        checkpoint_thread_id="synthetic-thread",
        checkpoint_request_sha256="b" * 64,
        checkpoint_intent_id="task-checkpoint-" + "a" * 32,
        checkpoint_intent_sha256="c" * 64,
        scope_intent_sha256="d" * 64,
    )
    before = tuple(store.replay().head_event_ids)
    with pytest.raises(ParallelControlError, match="CHECKPOINT_LEASE_BINDING"):
        store.execution_lifecycle().reserve(request, actor=ACTOR)
    assert tuple(store.replay().head_event_ids) == before


def test_real_contained_exit_and_result_precede_lease_release(tmp_path: Path) -> None:
    store, lease, request, env, _ = _case(tmp_path)
    lifecycle = store.execution_lifecycle()
    assert lifecycle.reserve(request, actor=ACTOR)["dispatch_allowed"] is True
    with WindowsJobProcess.create(
        argv=request["argv"],
        cwd=tmp_path,
        environment=env,
        stdout_path=Path(request["stdout_path"]),
        job_name=request["job_name"],
    ) as handle:
        lifecycle.bind(lease.lease_id, handle, actor=ACTOR)
        with pytest.raises(ParallelControlError, match="STILL_RUNNING"):
            lifecycle.confirm_exit(lease.lease_id, handle, actor=ACTOR)
        lifecycle.resume(lease.lease_id, handle, actor=ACTOR)
        assert lifecycle.resume(lease.lease_id, handle, actor=ACTOR)["status"] == "REPLAY_ONLY"
        assert handle.wait(timeout=20) == 0
        lifecycle.confirm_exit(lease.lease_id, handle, actor=ACTOR)
        with pytest.raises(ParallelControlError, match="LEASE_EXECUTION_NOT_TERMINAL"):
            store.release(lease.lease_id, actor=ACTOR, now=datetime.now(UTC), evidence_refs=())
        result = {
            key: request[key]
            for key in ("candidate_sha", "validation_identity_sha256", "request_id")
        }
        result["status"] = "PASS"
        Path(request["result_path"]).write_text(json.dumps(result), encoding="utf-8")
        lifecycle.record_result(
            lease.lease_id, actor=ACTOR, result_path=Path(request["result_path"])
        )
        lifecycle.record_result(
            lease.lease_id, actor=ACTOR, result_path=Path(request["result_path"])
        )
        recorded = store.replay().lease_heads[0].execution
        with pytest.raises(ParallelControlError):
            lifecycle.record_incomplete_result(lease.lease_id, actor=ACTOR)
        assert store.replay().lease_heads[0].execution == recorded
    assert (
        store.release(lease.lease_id, actor=ACTOR, now=datetime.now(UTC), evidence_refs=()).state
        == "RELEASED"
    )
    assert lifecycle.reserve(request, actor=ACTOR)["status"] == "REPLAY_ONLY"
    replay = store.replay()
    assert replay.status == "PASS" and not replay.active_leases
    assert replay.lease_heads[0].execution["state"] == "RESULT_RECORDED"
    assert replay.lease_heads[0].to_dict()["schema_version"] == "execution_lease.v2"


@pytest.mark.parametrize("corrupt", [False, True], ids=["missing_result", "corrupt_result"])
def test_incomplete_result_requires_real_exit_and_preserves_original_artifact(
    tmp_path: Path, corrupt: bool
) -> None:
    store, lease, request, env, _ = _case(tmp_path)
    request["argv"] = [
        sys.executable,
        "-c",
        "import time; from pathlib import Path; "
        "exec('while not Path(\"worker.release\").exists():\\n time.sleep(.02)')",
    ]
    result_path = Path(request["result_path"])
    original = b"{invalid runner result\n"
    if corrupt:
        result_path.write_bytes(original)
    lifecycle = store.execution_lifecycle()
    lifecycle.reserve(request, actor=ACTOR)
    with pytest.raises(ParallelControlError):
        lifecycle.record_incomplete_result(lease.lease_id, actor=ACTOR)
    oracle = NativeOracle()
    with WindowsJobProcess.create(
        argv=request["argv"],
        cwd=tmp_path,
        environment=env,
        stdout_path=Path(request["stdout_path"]),
        job_name=request["job_name"],
    ) as handle:
        with oracle.process(handle.identity()["pid"]) as native:
            lifecycle.bind(lease.lease_id, handle, actor=ACTOR)
            lifecycle.resume(lease.lease_id, handle, actor=ACTOR)
            oracle.assert_in_job(native, request["job_name"])
            assert not oracle.exited(native)
            with pytest.raises(ParallelControlError):
                lifecycle.record_incomplete_result(lease.lease_id, actor=ACTOR)
            assert store.replay().lease_heads[0].execution["state"] == "RUNNING"
            (tmp_path / "worker.release").write_bytes(b"release")
            assert handle.wait(timeout=20) == 0
            assert oracle.exited(native)
            # Physical exit alone is insufficient until the lifecycle records it.
            with pytest.raises(ParallelControlError):
                lifecycle.record_incomplete_result(lease.lease_id, actor=ACTOR)
            lifecycle.confirm_exit(lease.lease_id, handle, actor=ACTOR)
            with pytest.raises(ParallelControlError):
                lifecycle.record_incomplete_result(lease.lease_id, actor="foreign-actor")
            lifecycle.record_incomplete_result(lease.lease_id, actor=ACTOR)
            recorded = store.replay().lease_heads[0].execution
            assert recorded["state"] == "RESULT_RECORDED"
            assert recorded["exit"]["basis"] == "LIVE_CONTAINED_HANDLE"
            assert recorded["result"] == {
                "status": "INSUFFICIENT",
                "artifact": None,
                "reason": "RUNNER_RESULT_UNAVAILABLE_OR_INVALID",
            }
            lifecycle.record_incomplete_result(lease.lease_id, actor=ACTOR)
            assert store.replay().lease_heads[0].execution == recorded
    if corrupt:
        assert result_path.read_bytes() == original
    else:
        assert not result_path.exists()
    assert (
        store.release(lease.lease_id, actor=ACTOR, now=datetime.now(UTC), evidence_refs=()).state
        == "RELEASED"
    )
    assert lifecycle.reserve(request, actor=ACTOR)["status"] == "REPLAY_ONLY"
    with pytest.raises(ParallelControlError):
        lifecycle.record_incomplete_result(lease.lease_id, actor=ACTOR)
    replay = store.replay()
    assert replay.status == "PASS" and not replay.active_leases
    assert replay.lease_heads[0].execution == recorded


CRASH_EXIT = 37
CRASH_PHASES = (
    "RESERVED",
    "CONTAINED_SUSPENDED",
    "RESUME_INTENT",
    "RUNNING",
    "RUNNING_RESULT_FILE",
    "EXIT_CONFIRMED",
    "RESULT_FILE_WRITTEN",
)


def _crash_launcher(root: Path, phase: str, source_candidate: bool = False) -> None:
    """Subprocess-only fault injection; never called in the pytest controller."""
    # Bound even a stuck launcher independently of parent-side test cleanup.
    watchdog = threading.Timer(90, lambda: os._exit(98))
    watchdog.daemon = True
    watchdog.start()
    store, lease, request, env, _ = _case(root)
    request["argv"] = [
        sys.executable,
        "-u",
        "-c",
        """
import json, os, time
from pathlib import Path
with Path('dispatches.txt').open('a', encoding='utf-8') as stream:
    stream.write('executed\\n')
Path('worker.json').write_text(json.dumps({'pid': os.getpid()}), encoding='utf-8')
end = time.monotonic() + 60
while not Path('worker.release').exists() and time.monotonic() < end:
    time.sleep(.02)
""",
    ]
    if source_candidate:
        request.pop("candidate_sha")
        request.pop("validation_identity_sha256")
        request.update(
            schema_version="workflow_execution_request.v3",
            execution_kind="CONTROLLED_SOURCE_CANDIDATE",
            source_head_sha=lease.base_commit,
            source_request_sha256="d" * 64,
            source_transaction_sha256="e" * 64,
            review_sha256="f" * 64,
        )
        request["argv"][-1] += """
request=json.loads(Path('source-execution.json').read_text())
keys=('request_id','execution_kind','source_head_sha','source_request_sha256',
      'source_transaction_sha256','review_sha256')
result={key:request[key] for key in keys}
result.update(schema_version='controlled_source_candidate_worker_result.v1',status='PASS')
Path(request['result_path']).write_text(json.dumps(result), encoding='utf-8')
"""
        (root / "source-execution.json").write_text(json.dumps(request), encoding="utf-8")
    lifecycle = store.execution_lifecycle()
    identity = None

    def crash_barrier() -> None:
        (root / "crash.ready.json").write_text(
            json.dumps({"request": request, "identity": identity, "phase": phase}),
            encoding="utf-8",
        )
        deadline = time.monotonic() + 40
        while not (root / "crash.release").exists():
            if time.monotonic() >= deadline:
                os._exit(97)
            time.sleep(0.02)
        os._exit(CRASH_EXIT)  # Deliberately bypass Python context/finally cleanup.

    assert lifecycle.reserve(request, actor=ACTOR)["dispatch_allowed"]
    if phase == "RESERVED":
        crash_barrier()
    handle = WindowsJobProcess.create(
        argv=request["argv"],
        cwd=root,
        environment=env,
        stdout_path=Path(request["stdout_path"]),
        job_name=request["job_name"],
    )
    identity = handle.identity()
    lifecycle.bind(lease.lease_id, handle, actor=ACTOR)
    if phase == "CONTAINED_SUSPENDED":
        crash_barrier()
    if phase == "RESUME_INTENT":
        original_append = lifecycle._append

        def append_then_crash(head, value, now):
            updated = original_append(head, value, now)
            if value["state"] == "RESUME_INTENT":
                crash_barrier()  # Real durable append, before actual ResumeThread.
            return updated

        lifecycle._append = append_then_crash
    lifecycle.resume(lease.lease_id, handle, actor=ACTOR)
    _until(lambda: _read_json(root / "worker.json"), description="crash worker ready")
    if phase == "RUNNING_RESULT_FILE":
        result = {
            key: request[key]
            for key in ("candidate_sha", "validation_identity_sha256", "request_id")
        }
        result["status"] = "PASS"
        Path(request["result_path"]).write_text(json.dumps(result), encoding="utf-8")
    if phase in {"RUNNING", "RUNNING_RESULT_FILE"}:
        crash_barrier()
    (root / "worker.release").write_text("release", encoding="utf-8")
    assert handle.wait(timeout=20) == 0
    lifecycle.confirm_exit(lease.lease_id, handle, actor=ACTOR)
    if source_candidate:
        assert phase in {"RESULT_FILE_WRITTEN", "RESULT_RECORDED"}
        assert _read_json(Path(request["result_path"]))["status"] == "PASS"
        if phase == "RESULT_RECORDED":
            lifecycle.record_result(
                lease.lease_id, actor=ACTOR, result_path=Path(request["result_path"])
            )
        crash_barrier()
    if phase == "EXIT_CONFIRMED":
        crash_barrier()
    assert phase == "RESULT_FILE_WRITTEN"
    result = {
        key: request[key] for key in ("candidate_sha", "validation_identity_sha256", "request_id")
    }
    result["status"] = "PASS"
    Path(request["result_path"]).write_text(json.dumps(result), encoding="utf-8")
    crash_barrier()


@pytest.mark.parametrize(
    ("phase", "source_candidate"),
    [(phase, False) for phase in CRASH_PHASES]
    + [("RESULT_FILE_WRITTEN", True), ("RESULT_RECORDED", True)],
)
def test_real_launcher_crash_recovery_replays_without_second_dispatch(
    tmp_path: Path, phase: str, source_candidate: bool
) -> None:
    assert os.name == "nt" and sys.version_info[:2] == (3, 11), (
        "INSUFFICIENT: recovery acceptance requires actual Windows/Python 3.11"
    )
    root = Path(__file__).resolve().parents[1]
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join((str(root / "tests"), str(root / "src")))
    source = (
        "from pathlib import Path; "
        "from test_devx015_workflow_coordination import _crash_launcher; "
        f"_crash_launcher(Path({str(tmp_path)!r}), {phase!r}, {source_candidate!r})"
    )
    native = NativeOracle()
    with (tmp_path / "launcher.log").open("wb") as log:
        launcher = subprocess.Popen(
            [sys.executable, "-u", "-c", source], cwd=root, env=env, stdout=log, stderr=log
        )
        try:
            ready = _until(
                lambda: _read_json(tmp_path / "crash.ready.json"),
                description=f"launcher at durable {phase}",
            )
            request = ready["request"]
            policy = load_parallel_control_policy(POLICY_PATH)
            # Each reconstruction independently replays on-disk event authority.
            before = FileExecutionLeaseStore(tmp_path / "leases", policy=policy).replay()
            assert before.status == "PASS"
            expected_state = {
                "RESULT_FILE_WRITTEN": "EXIT_CONFIRMED",
                "RUNNING_RESULT_FILE": "RUNNING",
            }.get(phase, phase)
            execution = before.active_leases[0].execution
            assert execution["state"] == expected_state
            assert execution["request"] == request
            assert request["subject_task_id"] == before.active_leases[0].task_id
            result_path = Path(request["result_path"])
            original_result = result_path.read_bytes() if result_path.exists() else None
            if source_candidate:
                worker_result = json.loads(original_result)
                assert (
                    worker_result["schema_version"]
                    == "controlled_source_candidate_worker_result.v1"
                )
                assert worker_result["status"] == "PASS"
                for key in (
                    "request_id",
                    "execution_kind",
                    "source_head_sha",
                    "source_request_sha256",
                    "source_transaction_sha256",
                    "review_sha256",
                ):
                    assert worker_result[key] == request[key]
                assert execution["exit"]["returncode"] == 0
                assert execution["exit"]["basis"] == "LIVE_CONTAINED_HANDLE"
            dispatch_path = tmp_path / "dispatches.txt"
            original_dispatches = dispatch_path.read_bytes() if dispatch_path.exists() else b""
            was_resumed = phase in {
                "RUNNING",
                "RUNNING_RESULT_FILE",
                "EXIT_CONFIRMED",
                "RESULT_FILE_WRITTEN",
                "RESULT_RECORDED",
            }
            assert original_dispatches.splitlines() == ([b"executed"] if was_resumed else [])
            with ExitStack() as stack:
                launcher_handle = stack.enter_context(native.process(execution["launcher"]["pid"]))
                assert (
                    native.creation_time(launcher_handle) == execution["launcher"]["creation_time"]
                )
                assert not native.exited(launcher_handle)
                process_handles = []
                if ready["identity"] is not None:
                    identity = ready["identity"]
                    process = stack.enter_context(native.process(identity["pid"]))
                    process_handles.append(process)
                    assert native.creation_time(process) == identity["creation_time"]
                    if phase in {
                        "CONTAINED_SUSPENDED",
                        "RESUME_INTENT",
                        "RUNNING",
                        "RUNNING_RESULT_FILE",
                    }:
                        assert not native.exited(process)
                        native.assert_in_job(process, request["job_name"])
                    else:
                        assert native.exited(process)
                if phase in {"RUNNING", "RUNNING_RESULT_FILE"}:
                    worker = _read_json(tmp_path / "worker.json")
                    worker_handle = stack.enter_context(native.process(worker["pid"]))
                    process_handles.append(worker_handle)
                    assert not native.exited(worker_handle)
                    native.assert_in_job(worker_handle, request["job_name"])
                (tmp_path / "crash.release").write_text("crash", encoding="utf-8")
                assert launcher.wait(timeout=20) == CRASH_EXIT
                assert native.exited(launcher_handle, 10)
                for process in process_handles:
                    assert native.exited(process, 10), "launcher crash left a live executor"
            # Process references are closed before proving named Job disappearance.
            native.assert_job_absent(request["job_name"])
            restarted = FileExecutionLeaseStore(tmp_path / "leases", policy=policy)
            assert restarted.replay().status == "PASS"
            lifecycle = restarted.execution_lifecycle()
            with pytest.raises(ParallelControlError, match="ACTIVE_OWNER"):
                lifecycle.recover(request["lease_id"], actor="different-agent")
            recovered = lifecycle.recover(request["lease_id"], actor=ACTOR)
            assert recovered["status"] == (
                "REPLAY_ONLY" if phase == "RESULT_RECORDED" else "RECOVERED_TERMINAL"
            )
            if phase != "RESULT_RECORDED":
                assert recovered["dispatch_allowed"] is False
            else:
                assert tuple(restarted.replay().head_event_ids) == tuple(before.head_event_ids)
                assert recovered["execution"] == execution
            terminal = recovered["execution"]
            assert terminal["state"] == "RESULT_RECORDED"
            assert terminal["result"]["status"] == (
                "PASS" if phase == "RESULT_RECORDED" else "INSUFFICIENT"
            )
            if source_candidate and phase == "RESULT_FILE_WRITTEN":
                assert terminal["result"]["reason"] == "SOURCE_ADOPTION_INCOMPLETE"
            if not source_candidate and phase == "RESULT_FILE_WRITTEN":
                # Even an exact-looking PASS file is not the original validator's commitment.
                assert terminal["result"]["reason"] == "RECOVERED_WITHOUT_COMPLETE_RUNNER_RESULT"
            if phase in {"EXIT_CONFIRMED", "RESULT_FILE_WRITTEN", "RESULT_RECORDED"}:
                assert terminal["exit"]["returncode"] == 0
                assert terminal["exit"]["basis"] == "LIVE_CONTAINED_HANDLE"
            else:
                assert terminal["exit"]["returncode"] is None
                assert terminal["exit"]["basis"] == "DEAD_LAUNCHER_JOB_EMPTY"
                assert terminal["result"]["status"] != "PASS"
            if original_result is not None:
                assert result_path.read_bytes() == original_result
                if phase == "RESULT_RECORDED":
                    assert terminal["result"]["artifact"]["path"] == request["result_path"]
                else:
                    assert terminal["result"]["artifact"] is None
            assert lifecycle.recover(request["lease_id"], actor=ACTOR)["status"] == "REPLAY_ONLY"
            replayed = lifecycle.reserve(request, actor=ACTOR)
            assert replayed["status"] == "REPLAY_ONLY" and not replayed["dispatch_allowed"]
            assert (
                restarted.release(
                    request["lease_id"], actor=ACTOR, now=datetime.now(UTC), evidence_refs=()
                ).state
                == "RELEASED"
            )
            final_store = FileExecutionLeaseStore(tmp_path / "leases", policy=policy)
            final = final_store.replay()
            assert final.status == "PASS" and not final.active_leases
            assert final.lease_heads[0].execution == terminal
            assert not final_store.execution_lifecycle().reserve(request, actor=ACTOR)[
                "dispatch_allowed"
            ]
            assert (
                dispatch_path.read_bytes() if dispatch_path.exists() else b""
            ) == original_dispatches, "RECOVERY_MUST_NOT_DISPATCH_AGAIN"
        finally:
            if launcher.poll() is None:
                # Kill only this test's launcher. Its owned Job closes in the OS.
                launcher.kill()
                launcher.wait(timeout=20)


@pytest.mark.parametrize("mutation", [False, True], ids=["original", "M06"])
def test_m06_actual_redispatch_hits_original_recovery_assertion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: bool
) -> None:
    """Run the original crash oracle against a real duplicate contained execution."""
    import textwrap

    from ai_trading_system.platform.architecture import workflow_coordination as coordination

    original = coordination.ExecutionLifecycle.recover
    before = textwrap.dedent(inspect.getsource(original))
    target = '        # No observed exit code/result closure means no PASS and no redispatch.\n'
    assert before.count(target) == 1, "INVALID_MUTATION_TARGET"
    injection = '''        # M06: blindly execute the frozen request after the original Job exited.
        import os
        from ai_trading_system.platform.architecture.workflow_execution import WindowsJobProcess
        frozen = value["request"]
        duplicate = WindowsJobProcess.create(
            argv=frozen["argv"], cwd=Path(frozen["cwd"]), environment=dict(os.environ),
            stdout_path=Path(frozen["cwd"]) / "m06-duplicate.stdout.log",
            job_name=frozen["job_name"],
        )
        try:
            duplicate_identity = duplicate.identity()
            duplicate.resume()
            duplicate_exit = duplicate.wait(timeout=20)
            assert duplicate_exit == 0
            assert duplicate.active_process_count() == 0
            Path(frozen["cwd"], "m06-duplicate.json").write_text(
                json.dumps({"identity": duplicate_identity, "exit_code": duplicate_exit,
                            "original_process": value["process"], "argv": frozen["argv"]}),
                encoding="utf-8",
            )
        finally:
            duplicate.close()
'''
    after = before.replace(target, injection + target)
    code = compile(after, "<M06-blind-recovery-dispatch>", "exec")
    (tmp_path / "m06-method-before.py").write_text(before, encoding="utf-8")
    (tmp_path / "m06-method-after.py").write_text(after, encoding="utf-8")
    fixture = tmp_path / "original-crash-fixture"
    fixture.mkdir()
    with monkeypatch.context() as scoped:
        if mutation:
            namespace: dict = {}
            exec(code, original.__globals__, namespace)
            scoped.setattr(coordination.ExecutionLifecycle, "recover", namespace["recover"])
            with pytest.raises(AssertionError, match=r"^RECOVERY_MUST_NOT_DISPATCH_AGAIN(?:\n|$)"):
                test_real_launcher_crash_recovery_replays_without_second_dispatch(
                    fixture, "EXIT_CONFIRMED", False
                )
        else:
            test_real_launcher_crash_recovery_replays_without_second_dispatch(
                fixture, "EXIT_CONFIRMED", False
            )
    assert coordination.ExecutionLifecycle.recover is original
    dispatches = (fixture / "dispatches.txt").read_bytes().splitlines()
    assert dispatches == [b"executed"] * (2 if mutation else 1)
    replay = FileExecutionLeaseStore(
        fixture / "leases", policy=load_parallel_control_policy(POLICY_PATH)
    ).replay()
    assert replay.status == "PASS" and not replay.active_leases
    head = replay.lease_heads[0]
    assert head.state == "RELEASED" and head.execution["state"] == "RESULT_RECORDED"
    NativeOracle().assert_job_absent(head.execution["request"]["job_name"])
    if mutation:
        duplicate = _read_json(fixture / "m06-duplicate.json")
        assert duplicate["identity"] != duplicate["original_process"]
        assert duplicate["argv"] == head.execution["request"]["argv"]
        assert duplicate["exit_code"] == 0
    (tmp_path / "m06-counterfactual.json").write_text(json.dumps({
        "mutant_id": "M06", "mutation": mutation, "dispatch_count": len(dispatches),
        "target_assertion_killed": mutation, "target_assertion": "RECOVERY_MUST_NOT_DISPATCH_AGAIN",
        "module_sha256": hashlib.sha256(Path(coordination.__file__).read_bytes()).hexdigest(),
        "before_method_sha256": hashlib.sha256(before.encode()).hexdigest(),
        "after_method_sha256": hashlib.sha256(after.encode()).hexdigest(),
        "scope": "ORIGINAL_REAL_LAUNCHER_CRASH_LIFECYCLE", "formal_project_acceptance": False,
        "lease_id": head.lease_id, "lease_state": head.state,
    }), encoding="utf-8")


def test_reservation_rejects_wrong_subject_and_actor_without_consuming_request(
    tmp_path: Path,
) -> None:
    store, lease, request, _env, _now = _case(tmp_path)
    lifecycle = store.execution_lifecycle()
    with pytest.raises(ParallelControlError, match="ACTIVE_OWNER"):
        lifecycle.reserve(request, actor="different-agent")
    with pytest.raises(ParallelControlError, match="REQUEST_BINDING"):
        lifecycle.reserve({**request, "subject_task_id": "UNRELATED-TASK"}, actor=ACTOR)
    assert store.replay().active_leases[0].execution is None
    assert lifecycle.reserve(request, actor=ACTOR)["dispatch_allowed"] is True
    assert store.replay().active_leases[0].lease_id == lease.lease_id


@pytest.mark.parametrize(
    "launcher",
    ["cmd/git.exe", "bin/git.exe", "mingw64/bin/git.exe", "usr/bin/git.exe"],
)
def test_git_installation_root_accepts_every_git_for_windows_launcher(launcher) -> None:
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _git_installation_root,
    )

    root = Path("C:/Program Files/Git")
    assert _git_installation_root(root / launcher) == root

def _clean_process_runtime_identity(environment=None, **kwargs):
    """Loaded-source custody measured in a fresh interpreter (tests only)."""
    if kwargs:
        raise AssertionError("clean-process identity supports the environment argument only")
    probe = (
        "import json, sys\n"
        "from ai_trading_system.platform.architecture.workflow_execution import (\n"
        "    acceptance_runtime_identity)\n"
        "environment = json.loads(sys.stdin.read())\n"
        "sys.stdout.write(json.dumps(acceptance_runtime_identity(environment)))\n"
    )
    selected = dict(os.environ if environment is None else environment)
    completed = subprocess.run(
        [sys.executable, "-c", probe], input=json.dumps(selected), capture_output=True,
        text=True, timeout=300,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)
