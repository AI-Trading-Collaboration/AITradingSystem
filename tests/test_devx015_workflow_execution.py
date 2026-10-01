"""Actual Windows/Python 3.11 execution containment, never repository Full.

These are new-API acceptance cases, not evidence of baseline red. Independent
Win32 process handles and explicit file barriers observe the actual processes.
All launchers, jobs, files and cleanup targets belong to the current tmp_path.
"""

from __future__ import annotations

import ctypes
import gc
import hashlib
import importlib
import inspect
import json
import os
import shlex
import shutil
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

import pytest

# Subprocess hang guards below are sized for formal Full load (16 xdist workers plus
# nested actual Full/pytest children); they bound hangs only, never pass/fail meaning.
from test_devx015_workflow_integration import (
    _git,
)
from test_devx015_workflow_integration import (
    canonical_merge_repository as canonical_merge_repository,
)
from test_devx015_workflow_integration import (
    small_repository as small_repository,
)

ROOT = Path(__file__).resolve().parents[1]
# Test hang bounds (Full runs 16 loaded workers plus nested Full), not production lease or
# scheduling policy. DEVX-018 load calibration (provisional, owner review pending; exit
# condition in docs/requirements/DEVX-018_Validation_Runtime_Throughput_V1.md): the former
# 120s/60s/180s values were unloaded-host values; a loaded Windows host needs ~194s for one
# runtime-identity hash and Job termination confirmation. CLI bounds stay above the 900s
# production profile inspector bound.
DEADLINE = 600.0
LOADED_HOST_CLI_TIMEOUT_SECONDS = 1800
CRASH_EXIT = 23


@pytest.mark.parametrize("mode", ["dispatch", "print-only", "relative", "missing-transaction"])
def test_protected_full_cli_routes_or_refuses_before_installed_admission(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mode: str,
) -> None:
    monkeypatch.syspath_prepend(str(ROOT))
    runner = importlib.import_module("scripts.run_validation_tier")
    from ai_trading_system.platform.architecture import source_preservation

    args = ["full", "--write-runtime-artifact", "--task-id", "unit",
            "--protected-full-candidate-root", "relative" if mode == "relative" else str(tmp_path)]
    if mode != "missing-transaction":
        args += ["--publication-transaction", str(tmp_path / "transaction.json")]
    if mode == "print-only":
        args.append("--print-only")
    monkeypatch.setattr(source_preservation, "hold_installed_inspector",
                        lambda root: pytest.fail("invalid arguments reached native admission"))
    if mode == "dispatch":
        def dispatch(argv, *, candidate_root):
            assert list(argv) == args and candidate_root == tmp_path
            return 23
        monkeypatch.setattr(runner, "run_installed_protected_full", dispatch)
        assert runner.main(args) == 23
        with pytest.raises(runner.ExecutionContainmentError, match="PROTECTED_FULL_CAPABILITIES"):
            runner._main(runner.parse_args(args))
    else:
        with pytest.raises(runner.ExecutionContainmentError, match="PROTECTED_FULL_"):
            runner.main(args)


@pytest.mark.parametrize("fault", [None, "admission", "logon", "exchange", "full"])
def test_installed_full_composition_retains_custody_and_does_not_inherit_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fault: str | None,
) -> None:
    monkeypatch.syspath_prepend(str(ROOT))
    runner = importlib.import_module("scripts.run_validation_tier")
    from ai_trading_system.platform.architecture import (
        source_preservation as preservation,
    )
    from ai_trading_system.platform.architecture import (
        workflow_coordination as coordination,
    )
    from ai_trading_system.platform.architecture import (
        workflow_execution as execution,
    )

    events: list[str] = []
    token = object()

    class Context:
        @contextmanager
        def inspection(self, root):
            assert root == tmp_path
            yield

    context = Context()

    @contextmanager
    def held(root):
        assert root == tmp_path
        if fault == "admission":
            raise RuntimeError("admission")
        events.append("held")
        try:
            yield context
        finally:
            events.append("unheld")

    @contextmanager
    def logon(cls, **kwargs):
        assert kwargs == {"candidate_root": tmp_path, "candidate_sha": "a" * 40,
                          "git_context": context}
        if fault == "logon":
            raise RuntimeError("logon")
        events.append("token")
        try:
            yield token
        finally:
            events.append("token-close")

    class Exchange:
        profile_directory = tmp_path / "profile"
        used = False

        @classmethod
        def create(cls, root, worker):
            assert worker is token and root.parent == tmp_path
            assert root.name.startswith("full-exchange-")
            if fault == "exchange":
                raise RuntimeError("exchange")
            return cls()

        @contextmanager
        def directory(self):
            assert not self.used, "exchange consumed more than once"
            self.used = True
            events.append("exchange")
            try:
                yield tmp_path
            finally:
                events.append("exchange-close")

    def full(argv, **kwargs):
        assert events == ["held", "token"]
        env = kwargs["worker_environment"]
        assert "SECRET_SENTINEL" not in env and "PYTHONPATH" not in env
        assert env["TEMP"] == str(tmp_path / "profile")
        assert "untrusted-path" not in env["PATH"]
        consumer = object.__new__(runner._FullCommandRunner)
        consumer.worker_token = token
        consumer.worker_exchange = kwargs["worker_exchange"]
        # Exercise the real mandatory consumer up to its checkout admission.
        # Refuse there so this test cannot dispatch pytest or claim formal evidence.
        def stop_at_checkout(*args, **kwargs):
            raise runner.ExecutionContainmentError("COMPOSITION_TEST_BOUNDARY")
        monkeypatch.setattr(runner, "bind_acceptance_checkout", stop_at_checkout)
        outcome = runner._run_mandatory_acceptance_command(
            [sys.executable, "-m", "pytest"], cwd=tmp_path, binding={},
            expected_collections=1, command_runner=consumer,
        )
        assert "COMPOSITION_TEST_BOUNDARY" in outcome["mandatory_acceptance"]["reason"]
        assert events[-2:] == ["exchange", "exchange-close"]
        if fault == "full":
            raise RuntimeError("full")
        return 7

    monkeypatch.setenv("SECRET_SENTINEL", "must-not-inherit")
    monkeypatch.setenv("PATH", "untrusted-path")
    monkeypatch.setattr(preservation, "hold_installed_inspector", held)
    monkeypatch.setattr(execution.WindowsWorkerToken, "logon_registered", classmethod(logon))
    monkeypatch.setattr(coordination, "WindowsWorkerExchange", Exchange)
    monkeypatch.setattr(runner, "_git_commit", lambda root: "a" * 40)
    monkeypatch.setattr(runner, "run_protected_full", full)
    monkeypatch.setattr(runner.sys, "executable", str(tmp_path / "runtime/python.exe"))
    args = ["full", "--write-runtime-artifact", "--task-id", "unit",
            "--publication-transaction", str(tmp_path / "transaction.json")]
    if fault:
        with pytest.raises(RuntimeError, match=fault):
            runner.run_installed_protected_full(args, candidate_root=tmp_path)
    else:
        assert runner.run_installed_protected_full(args, candidate_root=tmp_path) == 7
    if "exchange" in events:
        assert events[-3:] == ["exchange-close", "token-close", "unheld"]
    elif "token" in events:
        assert events[-2:] == ["token-close", "unheld"]
    elif "held" in events:
        assert events[-1] == "unheld"


@pytest.mark.parametrize("fault", [None, "body", "binding", "unterminated", "interior"])
def test_dpapi_worker_password_native_lifetime(monkeypatch: Any, fault: str | None) -> None:
    from ctypes import wintypes as w

    from ai_trading_system.platform.architecture import workflow_execution as api

    crypt = ctypes.WinDLL("crypt32", use_last_error=True)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    crypt.CryptProtectData.argtypes = [
        ctypes.POINTER(api._CredentialBlob), ctypes.c_void_p,
        ctypes.POINTER(api._CredentialBlob), ctypes.c_void_p, ctypes.c_void_p,
        w.DWORD, ctypes.POINTER(api._CredentialBlob),
    ]
    crypt.CryptProtectData.restype = w.BOOL
    kernel.LocalFree.argtypes = [ctypes.c_void_p]
    kernel.LocalFree.restype = ctypes.c_void_p
    chars = ["A", "B", "C", "\0"]
    if fault == "unterminated":
        chars[-1] = "D"
    if fault == "interior":
        chars[1] = "\0"
    secret = (ctypes.c_wchar * len(chars))(*chars)
    binding = b"b" * 32
    salt_bytes = ctypes.create_string_buffer(binding)
    incoming = api._CredentialBlob(ctypes.sizeof(secret), ctypes.addressof(secret))
    salt = api._CredentialBlob(32, ctypes.addressof(salt_bytes))
    outgoing = api._CredentialBlob()
    assert crypt.CryptProtectData(
        ctypes.byref(incoming), None, ctypes.byref(salt), None, None, 1,
        ctypes.byref(outgoing),
    )
    try:
        encrypted = ctypes.string_at(outgoing.data, outgoing.length)
    finally:
        ctypes.memset(ctypes.addressof(secret), 0, ctypes.sizeof(secret))
        kernel.LocalFree(outgoing.data)
    # Observe the actual native allocation immediately before the real LocalFree.
    # No API success or decryption result is replaced by this observation seam.
    freed: list[bool] = []

    class ObservedFree:
        def __call__(self, pointer: Any) -> Any:
            freed.append(ctypes.string_at(pointer, 8) == bytes(8))
            return kernel.LocalFree(pointer)

    class ObservedKernel:
        LocalFree = ObservedFree()

    original = ctypes.WinDLL
    monkeypatch.setattr(ctypes, "WinDLL", lambda name, **kw:
                        ObservedKernel() if name == "kernel32" else original(name, **kw))
    try:
        with api.decrypted_worker_password(
            encrypted, b"x" * 32 if fault == "binding" else binding,
        ) as password:
            assert fault not in {"binding", "unterminated", "interior"}
            assert len(password) == 4 and password[0] == "A" and password[-1] == "\0"
            if fault == "body":
                raise RuntimeError("synthetic caller failure")
    except api.ExecutionContainmentError as exc:
        assert fault in {"binding", "unterminated", "interior"}
        assert "CREDENTIAL_" in str(exc)
    except RuntimeError:
        assert fault == "body"
    else:
        assert fault is None
    assert freed == ([] if fault == "binding" else [True])


@pytest.mark.parametrize("mode", ["valid", "wrong-creation", "sibling", "wrong-job", "exception"])
def test_git_reference_hook_requires_original_native_ancestor(
    small_repository: Path, execution_api: Any, native: NativeOracle, mode: str,
) -> None:
    """Actual prepared hook before a Git ref effect; not a publication-lease test."""
    root = small_repository
    name = _job_name()
    commit = _git(root, "rev-parse", "HEAD")
    git = shutil.which("git")
    assert git is not None
    probe = root / "hook-probe.py"
    probe.write_text(f"""
import ctypes,json,sys,time
from ctypes import wintypes as w
from pathlib import Path
from ai_trading_system.platform.architecture.workflow_execution import (
    hold_contained_ancestor,ExecutionContainmentError)
root=Path({str(root)!r})
binding=json.loads((root/'git-binding.json').read_text())
api=ctypes.WinDLL('kernel32',use_last_error=True)
api.GetCurrentProcess.argtypes=[];api.GetCurrentProcess.restype=w.HANDLE
api.GetProcessHandleCount.argtypes=[w.HANDLE,ctypes.POINTER(w.DWORD)]
api.GetProcessHandleCount.restype=w.BOOL
def count():
    value=w.DWORD();assert api.GetProcessHandleCount(api.GetCurrentProcess(),ctypes.byref(value))
    return value.value
before=count(); result={{'before':before}}; code=0
try:
    with hold_contained_ancestor(binding['job'],binding['expected']) as chain:
        result.update(chain=chain,held=count())
        assert result['held']==before+len(chain)  # No retained query Job/snapshot handle.
        (root/'hook-live.json').write_text(json.dumps(result))
        if {mode!r}=='exception': raise RuntimeError('owned body failure')
        end=time.monotonic()+20
        while not (root/'hook.release').exists() and time.monotonic()<end:time.sleep(.02)
        assert (root/'hook.release').exists()
    result['status']='ACCEPTED'
except ExecutionContainmentError as exc:
    result.update(status='DENIED',error=str(exc));code=97
except RuntimeError as exc:
    result.update(status='BODY_EXCEPTION',error=str(exc));code=97
result['after']=count();assert result['after']==before
(root/'hook-result.json').write_text(json.dumps(result))
raise SystemExit(code)
""", encoding="utf-8")
    hook = root / ".git/hooks/reference-transaction"
    hook.write_bytes(("#!/bin/sh\n[ \"$1\" = prepared ] || exit 0\nexec "
                      + shlex.join([sys._base_executable, "-B", str(probe)]) + "\n").encode())
    source = f"""
import json,os,subprocess,sys
from pathlib import Path
from ai_trading_system.platform.architecture.workflow_execution import (
    InheritedJobChild,contained_subprocess_identity)
root=Path({str(root)!r}); name={name!r}; sibling=None
environment=dict(os.environ)
environment.update(GIT_CONFIG_NOSYSTEM='1',GIT_CONFIG_GLOBAL=os.devnull,GIT_OPTIONAL_LOCKS='0')
try:
    if {mode!r}=='sibling':
        sibling=subprocess.Popen([sys._base_executable,'-c','import time;time.sleep(30)'])
    with InheritedJobChild.create(
        argv=[{git!r},'update-ref','refs/heads/ancestry-probe',{commit!r},'0'*40],
        cwd=root,environment=environment,stdout_path=root/'git.stdout',
        job_name=name,
    ) as child:
        original=child.pre_resume_binding()['process']; expected=dict(original)
        if {mode!r}=='wrong-creation':expected['creation_time']+=1
        if sibling is not None:expected=contained_subprocess_identity(sibling,name)
        (root/'git-binding.json').write_text(json.dumps({{
            'original':original,'expected':expected,
            'job':name+'-missing' if {mode!r}=='wrong-job' else name}}))
        child.resume(); code=child.wait_exit(timeout=180)
        (root/'git-result.json').write_text(json.dumps({{'returncode':code}}))
finally:
    if sibling is not None:
        sibling.terminate();sibling.wait(timeout=180)
"""
    with _create(execution_api, root, source, name=name) as process:
        process.resume()
        if mode == "valid":
            _until(lambda: _read_json(root / "hook-live.json") or process.poll() is not None,
                   description="actual Git prepared hook holds original native ancestry")
            live = _read_json(root / "hook-live.json")
            assert live is not None, (root / "stdout.log").read_text()
            original = _read_json(root / "git-binding.json")["original"]
            assert live["chain"][-1] == original
            assert len(live["chain"]) >= 2
            for row in live["chain"]:
                with native.process(row["pid"]) as handle:
                    assert native.creation_time(handle) == row["creation_time"]
                    native.assert_in_job(handle, name)
            (root / "hook.release").write_text("release", encoding="utf-8")
        assert process.wait(timeout=40) == 0, (root / "stdout.log").read_text()
    result = _read_json(root / "hook-result.json")
    assert result is not None, (root / "git.stdout").read_text()
    assert result["after"] == result["before"]
    git_result = _read_json(root / "git-result.json")
    refs = _git(root, "for-each-ref", "--format=%(objectname)", "refs/heads/ancestry-probe")
    assert _git(root, "rev-parse", "main") == commit
    if mode == "valid":
        assert result["status"] == "ACCEPTED" and git_result["returncode"] == 0
        assert refs == commit
    else:
        assert result["status"] == ("BODY_EXCEPTION" if mode == "exception" else "DENIED")
        assert git_result["returncode"] != 0 and refs == ""
        if mode == "wrong-creation":
            assert "ANCESTRY_ORIGINAL_MISMATCH" in result["error"]
        elif mode == "sibling":
            assert "ANCESTRY_NOT_LIVE_MEMBER" in result["error"]
        elif mode == "wrong-job":
            assert "JOB_MEMBERSHIP_UNPROVEN" in result["error"]
    native.assert_job_absent(name)


def test_installed_executable_hardlinks_keep_default_source_alias_denial(tmp_path: Path) -> None:
    from ai_trading_system.platform.architecture.workflow_contract import (
        WorkflowContractError,
        bounded_regular_bytes,
    )

    original = tmp_path / "installed.exe"
    original.write_bytes(b"frozen installed executable")
    alias = tmp_path / "installer-hardlink.exe"
    os.link(original, alias)
    metadata = original.stat()
    identity = (metadata.st_dev, metadata.st_ino)
    assert metadata.st_nlink == 2
    with pytest.raises(WorkflowContractError, match="NOT_REGULAR_FILE"):
        bounded_regular_bytes(original)
    with pytest.raises(WorkflowContractError, match="ARTIFACT_LINK_IDENTITY"):
        bounded_regular_bytes(original, expected_link_count=2)
    assert bounded_regular_bytes(
        original, expected_identity=identity, expected_link_count=2
    ) == b"frozen installed executable"
    with pytest.raises(WorkflowContractError, match="NOT_REGULAR_FILE"):
        bounded_regular_bytes(original, expected_identity=identity, expected_link_count=3)
    with pytest.raises(WorkflowContractError, match="EXPECTED_IDENTITY"):
        bounded_regular_bytes(original, expected_identity=(identity[0], identity[1] + 1),
                              expected_link_count=2)
    os.link(original, tmp_path / "later-alias.exe")
    with pytest.raises(WorkflowContractError, match="NOT_REGULAR_FILE"):
        bounded_regular_bytes(original, expected_identity=identity, expected_link_count=2)
    assert original.read_bytes() == alias.read_bytes() == b"frozen installed executable"


@pytest.mark.parametrize("case", ["default", "wrong-count", "wrong-identity", "valid", "exception"])
def test_exact_hardlink_read_custody_preserves_source_default(tmp_path: Path, case: str) -> None:
    from test_arch_005_integration_publication_fence import _publication_input_write_probe_source

    from ai_trading_system.platform.architecture.workflow_contract import (
        WorkflowContractError,
        hold_bound_read_file,
    )

    original, alias = tmp_path / "installed.exe", tmp_path / "installed-alias.exe"
    content = b"fixed installed code\n"
    original.write_bytes(content)
    os.link(original, alias)
    info, root_info = original.stat(), tmp_path.stat()
    options = ({} if case == "default"
               else {"expected_link_count": 3 if case == "wrong-count" else 2})
    expected_id = (info.st_dev, info.st_ino + (case == "wrong-identity"))
    context = hold_bound_read_file(
        tmp_path, original.name, expected=content, expected_identity=expected_id,
        expected_root_identity=(root_info.st_dev, root_info.st_ino),
        expected_parent_identities={}, **options,
    )
    if case in {"default", "wrong-count", "wrong-identity"}:
        with pytest.raises(WorkflowContractError):
            with context:
                pytest.fail("mismatched installed file was admitted")
    else:
        try:
            with context as custody:
                binding = custody.binding()
                assert binding["schema_version"] == "workflow_read_file_custody.v2"
                assert binding["link_count"] == 2 and binding["identity"] == list(expected_id)
                checked = subprocess.run(
                    [sys.executable, "-c", _publication_input_write_probe_source(), "denied",
                     str(tmp_path / "write-probes.json"), str(original), str(alias)],
                    capture_output=True, text=True, timeout=DEADLINE,
                )
                assert checked.returncode == 0, (checked.stdout, checked.stderr)
                probes = _read_json(tmp_path / "write-probes.json")
                assert all(row["winerror"] == 32 for row in probes)
                if case == "exception":
                    raise RuntimeError("owned test body")
        except RuntimeError as exc:
            assert case == "exception" and str(exc) == "owned test body"
        with pytest.raises(WorkflowContractError, match="READ_FILE_CUSTODY_OWNER"):
            custody.binding()
    assert original.read_bytes() == alias.read_bytes() == content
    alias.write_bytes(content)  # Both aliases are writable after original custody closes.
    original.write_bytes(content)


def test_original_installed_git_images_have_exact_read_custody(tmp_path: Path) -> None:
    from contextlib import ExitStack

    from ai_trading_system.platform.architecture.workflow_contract import (
        bounded_regular_bytes,
        hold_bound_read_file,
    )

    launcher = shutil.which("git")
    assert launcher is not None
    execution_path = Path(_git(tmp_path, "--exec-path"))
    root = execution_path.parents[2]
    paths = [Path(launcher), execution_path.parents[1] / "bin/git.exe", root / "usr/bin/sh.exe"]
    records = []
    with ExitStack() as stack:
        for path in paths:
            assert path.is_relative_to(root)
            relative = path.relative_to(root)
            info, root_info = path.stat(), root.stat()
            parents = {}
            for count in range(1, len(relative.parts)):
                parent = Path(*relative.parts[:count])
                metadata = (root / parent).stat()
                parents[parent.as_posix()] = (metadata.st_dev, metadata.st_ino)
            expected_id = (info.st_dev, info.st_ino)
            content = bounded_regular_bytes(path, expected_identity=expected_id,
                                            expected_link_count=info.st_nlink)
            custody = stack.enter_context(hold_bound_read_file(
                root, relative.as_posix(), expected=content, expected_identity=expected_id,
                expected_root_identity=(root_info.st_dev, root_info.st_ino),
                expected_parent_identities=parents, expected_link_count=info.st_nlink,
            ))
            record = custody.binding()
            assert record.get("link_count", 1) == info.st_nlink
            assert record["sha256"] == hashlib.sha256(content).hexdigest()
            records.append(record)
    (tmp_path / "installed-git-custody.json").write_text(json.dumps(records), encoding="utf-8")


@pytest.mark.parametrize("case", ["missing", "malformed", "error", "valid"])
def test_acceptance_worker_down_retains_invalid_identity(tmp_path: Path, case: str) -> None:
    """A node-down notification must not obscure the original worker failure."""
    from types import SimpleNamespace

    from ai_trading_system.platform.architecture.workflow_execution import _BoundAcceptancePlugin

    plugin = _BoundAcceptancePlugin(
        tmp_path, {"required_nodes": ["tests/test_probe.py::test_probe"]},
        tmp_path / "unused-result.json", [], {}, [], (1, 2), (1, 3),
    )
    identity = {"original": "worker-input"}
    node = SimpleNamespace()
    if case != "missing":
        node.workeroutput = (
            "not-a-report" if case == "malformed" else {"mandatory_input_identity": identity}
        )
    plugin.pytest_testnodedown(node, "worker exited" if case == "error" else None)
    assert plugin.worker_inputs == [identity if case == "valid" else None]


@pytest.mark.parametrize("fault", ["none", "callback-failure"])
def test_dependency_observer_covers_original_inventory_and_releases_custody(tmp_path, fault):
    from contextlib import ExitStack

    from ai_trading_system.platform.architecture.workflow_contract import hold_bound_read_file
    from ai_trading_system.platform.architecture.workflow_execution import (
        _acceptance_distribution_code,
    )

    metadata = tmp_path / "devx_observer-1.0.dist-info"
    metadata.mkdir()
    files = {
        "code.py": b"VALUE = 1\n", "cached.pyc": b"observed cache, not executed",
        "types.pyi": b"VALUE: int\n", "native.pyd": b"observed native fixture, not loaded",
        "binary.dll": b"observed dll, not loaded", "tool.exe": b"observed exe, not executed",
        "shared.so": b"observed shared library", "shared.dylib": b"observed library",
        "paths.pth": b"observed path configuration, not activated\n", "data.bin": b"excluded data",
        metadata.name + "/METADATA": b"Name: devx-observer\nVersion: 1.0\n",
    }
    record = metadata.name + "/RECORD"
    files[record] = "".join(name + ",,\n" for name in [*files, record]).encode()
    for name, raw in files.items():
        (tmp_path / name).write_bytes(raw)
    distribution = importlib.metadata.PathDistribution(metadata)
    baseline = _acceptance_distribution_code([distribution])
    owner_thread = threading.get_ident()
    previous_readers = {thread.ident for thread in threading.enumerate()
                        if thread.name.startswith("acceptance-read")}
    observed, cached = [], {}
    root_info = tmp_path.stat()

    class ObserverFailure(RuntimeError):
        pass

    with ExitStack() as stack:
        def capture(path, raw):
            assert threading.get_ident() == owner_thread
            relative = path.relative_to(tmp_path).as_posix()
            assert raw == files[relative]
            info = path.stat()
            parents = {}
            parts = Path(relative).parts
            for index in range(1, len(parts)):
                name = "/".join(parts[:index])
                parent = (tmp_path / name).stat()
                parents[name] = (parent.st_dev, parent.st_ino)
            custody = stack.enter_context(hold_bound_read_file(
                tmp_path, relative, expected=raw, expected_identity=(info.st_dev, info.st_ino),
                expected_root_identity=(root_info.st_dev, root_info.st_ino),
                expected_parent_identities=parents,
            ))
            observed.append({"path": str(path), "size_bytes": len(raw),
                             "sha256": hashlib.sha256(raw).hexdigest(),
                             "binding": custody.binding()})
            if fault == "callback-failure" and len(observed) == 3:
                raise ObserverFailure("stop after third actual file")

        if fault == "callback-failure":
            with pytest.raises(ObserverFailure, match="third actual file"):
                _acceptance_distribution_code([distribution], source_inputs=cached,
                                              observe_dependency=capture)
            assert len(observed) == 3
        else:
            actual = _acceptance_distribution_code([distribution], source_inputs=cached,
                                                   observe_dependency=capture)
            assert actual == baseline
            assert {Path(row["path"]).relative_to(tmp_path).as_posix() for row in observed} == (
                set(files) - {"data.bin"}
            )
            assert set(cached) == {tmp_path / "code.py", tmp_path / "cached.pyc"}
        assert {thread.ident for thread in threading.enumerate()
                if thread.name.startswith("acceptance-read")} == previous_readers
        first = Path(observed[0]["path"])
        assert "error" in _independent_file_custody_probe("write", first, tmp_path / "unused")
    assert _independent_file_custody_probe("write", first, tmp_path / "unused") == {"changed": True}
    (tmp_path / "dependency-observation.json").write_text(json.dumps({
        "fault": fault, "baseline": baseline, "observed": observed,
        "reader_threads_drained": True, "native_custody_released": True,
    }), encoding="utf-8")


def test_runtime_dependency_observer_preserves_original_identity(tmp_path):
    # Loaded-source custody is scoped to a clean process (acceptance workers and CLI
    # entries). A shared pytest worker may already hold unrelated compiled wrappers
    # (e.g. numpy dispatchers), so measure in a fresh interpreter.
    probe = (
        "import hashlib, json, sys, threading\n"
        "from ai_trading_system.platform.architecture.workflow_execution import (\n"
        "    acceptance_runtime_identity)\n"
        "owner = threading.get_ident(); rows = []\n"
        "def observe(path, raw):\n"
        "    assert threading.get_ident() == owner\n"
        "    rows.append((str(path), len(raw), hashlib.sha256(raw).hexdigest()))\n"
        "original = acceptance_runtime_identity()\n"
        "observed = acceptance_runtime_identity(observe_dependency=observe)\n"
        "sys.stdout.write(json.dumps({'original': original, 'observed': observed,"
        " 'rows': rows}))\n"
    )
    completed = subprocess.run(
        [sys.executable, "-c", probe], cwd=ROOT, env=_environment(), capture_output=True,
        text=True, timeout=300,
    )
    assert completed.returncode == 0, completed.stderr
    measured = json.loads(completed.stdout)
    original, observed = measured["original"], measured["observed"]
    rows = [tuple(row) for row in measured["rows"]]
    assert observed == original
    assert rows[0][0] == original["executable"]
    assert rows[0][2] == original["executable_sha256"]
    assert rows[1][0] == original["engine"]
    assert rows[1][2] == original["engine_sha256"]
    distribution_rows = rows[2:]
    assert len(distribution_rows) == original["distribution_code"]["file_count"]
    assert len({row[0] for row in distribution_rows}) == len(distribution_rows)
    assert sum(row[1] for row in distribution_rows) == original["distribution_code"]["size_bytes"]
    digest = hashlib.sha256()
    for row in distribution_rows:
        digest.update(json.dumps(list(row), separators=(",", ":")).encode())
        digest.update(b"\n")
    assert digest.hexdigest() == original["distribution_code"]["sha256"]
    (tmp_path / "original-runtime-observation.json").write_text(json.dumps({
        "identity": original, "rows": rows, "observer_grants_custody": False,
    }), encoding="utf-8")


@pytest.mark.parametrize("api", ["runtime", "distribution"])
def test_dependency_observer_requires_callable(api):
    from ai_trading_system.platform.architecture.workflow_execution import (
        ExecutionContainmentError,
        _acceptance_distribution_code,
        acceptance_runtime_identity,
    )

    with pytest.raises(ExecutionContainmentError, match="ACCEPTANCE_DEPENDENCY_OBSERVER"):
        if api == "runtime":
            acceptance_runtime_identity(observe_dependency=True)
        else:
            _acceptance_distribution_code([], observe_dependency=True)


@pytest.mark.parametrize("case", [
    "default-small", "default-large", "explicit-large", "too-small", "bool", "negative",
    "above-cap", "float", "empty-zero",
])
def test_bound_read_file_explicit_runtime_budget_preserves_default(tmp_path, case):
    from ai_trading_system.platform.architecture.workflow_contract import (
        WorkflowContractError,
        hold_bound_read_file,
    )

    raw = b"x" * (16 * 1024 * 1024 + 1) if case in {"default-large", "explicit-large"} else b"bytes"
    if case == "empty-zero":
        raw = b""
    target = tmp_path / "runtime.dll"
    target.write_bytes(raw)
    info, root_info = target.stat(), tmp_path.stat()
    options = {}
    if case not in {"default-small", "default-large"}:
        options["budget"] = {"explicit-large": 64 * 1024 * 1024, "too-small": len(raw) - 1,
                             "bool": True, "negative": -1, "above-cap": 64 * 1024 * 1024 + 1,
                             "float": 16.0, "empty-zero": 0}[case]
    def hold():
        return hold_bound_read_file(
            tmp_path, target.name, expected=raw, expected_identity=(info.st_dev, info.st_ino),
            expected_root_identity=(root_info.st_dev, root_info.st_ino),
            expected_parent_identities={}, **options,
        )
    if case in {"default-small", "explicit-large", "empty-zero"}:
        with hold() as custody:
            assert custody.binding()["size_bytes"] == len(raw)
            assert "error" in _independent_file_custody_probe("write", target, tmp_path / "unused")
    else:
        with pytest.raises(WorkflowContractError, match="READ_FILE_CUSTODY_EXPECTATION"), hold():
            raise AssertionError("invalid budget admitted")
    assert target.read_bytes() == raw
    assert _independent_file_custody_probe("write", target, tmp_path / "unused") == {
        "changed": True,
    }


@pytest.mark.parametrize("fault", ["none", "missing", "oversize"])
def test_dependency_capture_bounds_readers_and_drains_before_return(
    tmp_path: Path, fault: str,
) -> None:
    """Observe original reader threads with barriers, never replace an identity gate."""
    from ai_trading_system.platform.architecture.workflow_execution import (
        ExecutionContainmentError,
        _acceptance_distribution_code,
    )

    metadata = tmp_path / "devx_read_probe-1.0.dist-info"
    metadata.mkdir()
    files = {f"code{index:02d}.py": f"VALUE = {index}\n".encode() for index in range(8)}
    files[metadata.name + "/METADATA"] = b"Name: devx-read-probe\nVersion: 1.0\n"
    record = metadata.name + "/RECORD"
    files[record] = "".join(name + ",,\n" for name in [*files, record]).encode()
    for relative, raw in files.items():
        (tmp_path / relative).write_bytes(raw)
    first = tmp_path / "code00.py"
    if fault == "missing":
        first.unlink()
    elif fault == "oversize":
        with first.open("r+b") as stream:
            stream.truncate(64 * 1024 * 1024 + 1)

    capture = _acceptance_distribution_code
    read_code = next(item for item in capture.__code__.co_consts
                     if getattr(item, "co_name", None) == "read")
    barrier = threading.Barrier(4)
    release, ready = threading.Event(), threading.Event()
    lock = threading.Lock()
    active: set[int] = set()
    readers: dict[int, threading.Thread] = {}
    state = {"calls": 0, "peak": 0, "others_done": 0, "escaped": False}
    result: dict[str, Any] = {}
    captured: dict[Path, bytes] = {}

    def observe(frame: Any, event: str, _value: Any) -> None:
        if frame.f_code is read_code:
            identity = threading.get_ident()
            path = frame.f_locals["path"]
            if event == "call":
                with lock:
                    state["calls"] += 1
                    ordinal = state["calls"]
                    active.add(identity)
                    readers[identity] = threading.current_thread()
                    state["peak"] = max(state["peak"], len(active))
                if ordinal <= 4:
                    barrier.wait(timeout=DEADLINE)
                    if (fault == "none" and path == first) or (
                        fault != "none" and path != first
                    ):
                        assert release.wait(DEADLINE), "test barrier was not released"
            elif event == "return":
                with lock:
                    active.remove(identity)
                    if path != first:
                        state["others_done"] += 1
                        if fault == "none" and state["others_done"] == 3:
                            ready.set()
        elif frame.f_code is threading.Thread.join.__code__ and event == "call":
            # For failure, the caller must join a still-blocked owned reader;
            # observing this call avoids a timing guess about premature return.
            with lock:
                if fault != "none" and frame.f_locals["self"].ident in active:
                    ready.set()
        elif frame.f_code is capture.__code__ and event == "return":
            with lock:
                state["escaped"] = bool(active)
            ready.set()

    def collect() -> None:
        try:
            result["value"] = capture(
                [importlib.metadata.Distribution.at(metadata)], source_inputs=captured,
            )
        except BaseException as error:
            result["error"] = error

    previous = threading.getprofile()
    collector = threading.Thread(target=collect, name="dependency-capture-test")
    threading.setprofile(observe)
    try:
        collector.start()
        assert ready.wait(DEADLINE), "capture did not reach the actual reader barrier"
        with lock:
            assert state["calls"] == 4 and state["peak"] == 4
            assert not state["escaped"]
        release.set()
        collector.join(timeout=DEADLINE)
        assert not collector.is_alive()
    finally:
        release.set()
        barrier.abort()
        collector.join(timeout=DEADLINE)
        threading.setprofile(previous)
    assert not active and all(not thread.is_alive() for thread in readers.values())
    assert not state["escaped"]
    if fault != "none":
        assert isinstance(result.get("error"), ExecutionContainmentError), result
        assert "ACCEPTANCE_DEPENDENCY_CUSTODY" in str(result["error"])
        assert state["calls"] == 4 and captured == {}
        return
    assert "error" not in result, result
    expected = hashlib.sha256()
    for relative, raw in sorted(files.items(), key=lambda item: str(tmp_path / item[0])):
        expected.update(json.dumps([
            str(tmp_path / relative), len(raw), hashlib.sha256(raw).hexdigest(),
        ], separators=(",", ":")).encode() + b"\n")
    assert result["value"] == {
        "sha256": expected.hexdigest(), "file_count": len(files),
        "size_bytes": sum(map(len, files.values())),
    }
    assert captured == {tmp_path / name: raw for name, raw in files.items() if name.endswith(".py")}


# Runner-path modules imported lazily inside functions (worker exchange, source
# inspection, readiness checkers). Module-load discovery alone misses them, so
# the fixture would lack code the real runner reaches at execution time.
_RUNNER_LAZY_MODULES = (
    "ai_trading_system.atlas.page_effectiveness",
    "ai_trading_system.contracts.strategy_research_page_effectiveness",
    "ai_trading_system.platform.architecture.compatibility_authority",
    "ai_trading_system.platform.architecture.devex",
    "ai_trading_system.platform.architecture.integration_publication_fence",
    "ai_trading_system.platform.architecture.lease_arbiter",
    "ai_trading_system.platform.architecture.parallel_control_kernel",
    "ai_trading_system.platform.architecture.report_catalog_flow_authority",
    "ai_trading_system.platform.architecture.source_preservation",
    "ai_trading_system.platform.architecture.task_registry_canonical",
    "ai_trading_system.platform.architecture.workflow_contract",
    "ai_trading_system.platform.architecture.workflow_coordination",
    "ai_trading_system.platform.architecture.workflow_execution",
    "ai_trading_system.platform.architecture.workflow_integration",
    "ai_trading_system.platform.artifacts.json_contract",
    "ai_trading_system.yaml_loader",
)


@lru_cache(maxsize=1)
def _acceptance_fixture_sources() -> tuple[str, ...]:
    """Freeze only a clean runner import closure, not all modules collected by Full."""
    script = (
        "import importlib,json,sys\nfrom pathlib import Path\n"
        "import scripts.run_validation_tier\n"
        f"for name in {list(_RUNNER_LAZY_MODULES)!r}: importlib.import_module(name)\n"
        f"root=Path({str(ROOT)!r})\n"
        "paths=set()\n"
        "for module in tuple(sys.modules.values()):\n"
        " origin=getattr(module,'__file__',None)\n"
        " if not isinstance(origin,str): continue\n"
        " path=Path(origin).absolute()\n"
        " if path.is_relative_to(root) and path.suffix=='.py':\n"
        "  relative=path.relative_to(root)\n"
        "  if relative.parts[0] in {'src','scripts'}: paths.add(relative.as_posix())\n"
        "print(json.dumps(sorted(paths)))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env=dict(os.environ, PYTHONPATH=os.pathsep.join([str(ROOT), str(ROOT / "src")])),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    paths = tuple(json.loads(result.stdout))
    assert "scripts/run_validation_tier.py" in paths
    return paths


@pytest.mark.parametrize(
    "case",
    ["valid", "partial", "missing", "duplicate", "inventory", "path", "blob", "candidate",
     "other-candidate", "task"],
)
def test_mandatory_acceptance_binds_actual_committed_inventory(tmp_path: Path, case: str) -> None:
    """Real Git fixture proves binding, not the adequacy of its synthetic assertions."""
    from ai_trading_system.platform.architecture.workflow_execution import (
        ExecutionContainmentError,
        bind_mandatory_acceptance,
    )

    manifest_path = "config/architecture/devx_015_workflow_acceptance.v1.json"
    manifest = json.loads((ROOT / manifest_path).read_bytes())
    manifest["mapping_state"] = "COMPLETE_REVIEWED"
    manifest["variant_node_mapping"] = [
        {
            "case_id": row["id"],
            "variant": variant,
            "node_ids": ["tests/test_binding.py::test_required"],
        }
        for row in manifest["cases"]
        for variant in row["variants"]
    ]
    expected = "ACCEPTANCE_MAPPING_INVALID"
    if case == "task":
        manifest["task_id"] = "DEVX-015-UNRELATED-TASK"
    elif case == "partial":
        manifest["mapping_state"] = "PARTIAL_NOT_ACCEPTANCE_READY"
    elif case == "missing":
        manifest["variant_node_mapping"].pop()
    elif case == "duplicate":
        manifest["variant_node_mapping"].append(manifest["variant_node_mapping"][0])
    elif case == "inventory":
        manifest["cases"][0]["variants"][0] = "silently_replaced_variant"
    elif case == "path":
        manifest["variant_node_mapping"][0]["node_ids"] = ["tests/../private.py::test_required"]
    elif case == "blob":
        manifest["variant_node_mapping"][0]["node_ids"] = ["tests/test_absent.py::test_required"]
        expected = "ACCEPTANCE_BLOB_TYPE"
    elif case in {"candidate", "other-candidate"}:
        expected = "ACCEPTANCE_CANDIDATE_CHANGED"
    target = tmp_path / manifest_path
    target.parent.mkdir(parents=True)
    target.write_text(json.dumps(manifest), encoding="utf-8")
    test_path = tmp_path / "tests/test_binding.py"
    test_path.parent.mkdir()
    test_bytes = b"def test_required():\n    assert True\n"
    test_path.write_bytes(test_bytes)
    _git(tmp_path, "init", "-b", "main")
    _git(tmp_path, "config", "user.name", "Acceptance Fixture")
    _git(tmp_path, "config", "user.email", "acceptance@example.invalid")
    _git(tmp_path, "config", "core.autocrlf", "false")
    _git(tmp_path, "add", "config", "tests")
    _git(tmp_path, "commit", "-m", "synthetic mapping binding only")
    candidate = _git(tmp_path, "rev-parse", "HEAD")
    current = candidate
    if case == "other-candidate":
        _git(tmp_path, "commit", "--allow-empty", "-m", "same tree distinct real candidate")
        current = _git(tmp_path, "rev-parse", "HEAD")
        assert current != candidate
        assert _git(tmp_path, "rev-parse", candidate + "^{tree}") == _git(
            tmp_path, "rev-parse", current + "^{tree}"
        )
    if case in {"other-candidate", "task"}:
        (tmp_path.parent / f"{tmp_path.name}-candidate-identities.json").write_text(json.dumps({
            "requested": candidate, "current": current,
            "refs": _git(tmp_path, "show-ref"),
            "index_sha256": hashlib.sha256((tmp_path / ".git/index").read_bytes()).hexdigest(),
        }), encoding="utf-8")
    before_index = (tmp_path / ".git/index").read_bytes()
    before_refs = _git(tmp_path, "show-ref")
    if case == "valid":
        # The authority comes from C, never an uncommitted replacement manifest.
        target.write_bytes(b"uncommitted replacement is not candidate authority")
        binding = bind_mandatory_acceptance(tmp_path, candidate)
        assert binding["candidate_sha"] == candidate
        assert binding["variant_count"] == 106
        assert binding["execution_status"] == "NOT_EXECUTED"
        assert binding["required_nodes"] == ["tests/test_binding.py::test_required"]
        assert binding["test_blobs"] == [
            {
                "path": "tests/test_binding.py",
                "mode": "100644",
                "blob": _git(tmp_path, "rev-parse", candidate + ":tests/test_binding.py"),
                "sha256": hashlib.sha256(test_bytes).hexdigest(),
            }
        ]
        assert binding["manifest"]["blob"] == _git(
            tmp_path, "rev-parse", candidate + ":" + manifest_path
        )
    else:
        with pytest.raises(ExecutionContainmentError, match=expected):
            unexpected = bind_mandatory_acceptance(
                tmp_path, "0" * 40 if case == "candidate" else candidate
            )
            (tmp_path.parent / f"{tmp_path.name}-unexpected-binding.json").write_text(
                json.dumps(unexpected), encoding="utf-8"
            )
    assert _git(tmp_path, "show-ref") == before_refs
    assert (tmp_path / ".git/index").read_bytes() == before_index


@pytest.mark.parametrize("mutation", [False, True], ids=["original", "M03"])
@pytest.mark.parametrize("violation", ["candidate", "task"])
def test_m03_real_distinct_candidate_mutant_hits_original_binding_refusal(
    tmp_path: Path, execution_api: Any, mutation: bool, violation: str,
) -> None:
    original_function = execution_api.bind_mandatory_acceptance
    original = inspect.getsource(original_function)
    target = (
        '    if git("rev-parse", "HEAD").decode().strip() != candidate_sha:\n'
        '        raise ExecutionContainmentError("ACCEPTANCE_CANDIDATE_CHANGED")\n'
    )
    task_target = '            or manifest["task_id"] != DEVX015_ACCEPTANCE_TASK\n'
    assert original.count(target) == 2, "INVALID_MUTATION_TARGET"
    assert original.count(task_target) == 1, "INVALID_TASK_MUTATION_TARGET"
    changed = original.replace(target, "", 2).replace(task_target, "", 1) if mutation else original
    case = "other-candidate" if violation == "candidate" else "task"
    before_path = tmp_path.parent / f"{tmp_path.name}-m03-before.py"
    after_path = tmp_path.parent / f"{tmp_path.name}-m03-after.py"
    before_path.write_text(original, encoding="utf-8")
    after_path.write_text(changed, encoding="utf-8")
    with pytest.MonkeyPatch.context() as injected:
        if mutation:
            namespace: dict[str, Any] = {}
            exec(compile(changed, str(after_path), "exec"), execution_api.__dict__, namespace)
            injected.setattr(execution_api, "bind_mandatory_acceptance",
                             namespace["bind_mandatory_acceptance"])
            with pytest.raises(pytest.fail.Exception, match=r"^DID NOT RAISE"):
                test_mandatory_acceptance_binds_actual_committed_inventory(
                    tmp_path, case
                )
        else:
            test_mandatory_acceptance_binds_actual_committed_inventory(tmp_path, case)
        identities = _read_json(tmp_path.parent / f"{tmp_path.name}-candidate-identities.json")
        assert _git(tmp_path, "show-ref") == identities["refs"]
        assert hashlib.sha256((tmp_path / ".git/index").read_bytes()).hexdigest() == (
            identities["index_sha256"]
        )
        current = _git(tmp_path, "rev-parse", "HEAD")
        assert current == identities["current"]
        assert (current != identities["requested"]) == (violation == "candidate")
        if mutation:
            actual = _read_json(tmp_path.parent / f"{tmp_path.name}-unexpected-binding.json")
            assert actual["candidate_sha"] == identities["requested"]
            if violation == "candidate":
                assert actual["candidate_sha"] != current
            else:
                manifest = _read_json(
                    tmp_path / "config/architecture/devx_015_workflow_acceptance.v1.json"
                )
                assert manifest["task_id"] == "DEVX-015-UNRELATED-TASK"
                assert manifest["task_id"] != actual["task_id"]
                assert actual["task_id"] == execution_api.DEVX015_ACCEPTANCE_TASK
            assert actual["execution_status"] == "NOT_EXECUTED"
        if violation == "candidate":
            positive = execution_api.bind_mandatory_acceptance(tmp_path, current)
            assert positive["candidate_sha"] == current
    assert execution_api.bind_mandatory_acceptance is original_function
    (tmp_path.parent / f"{tmp_path.name}-m03-counterfactual.json").write_text(json.dumps({
        "schema_version": "devx015_m03_counterfactual.v1", "mutant_id": "M03",
        "mutation": mutation, "violation": violation, "target_assertion_killed": mutation,
        "target_assertion": "EXPECTED_BINDING_REFUSAL_NOT_RAISED",
        "scope": "REAL_GIT_MANDATORY_CANDIDATE_BINDING", "formal_acceptance": False,
        "module_path": execution_api.__file__,
        "module_sha256": hashlib.sha256(Path(execution_api.__file__).read_bytes()).hexdigest(),
        "before_method_sha256": hashlib.sha256(original.encode()).hexdigest(),
        "after_method_sha256": hashlib.sha256(changed.encode()).hexdigest(),
        "identities": identities,
    }), encoding="utf-8")


@pytest.mark.parametrize("timing", ["before-runner", "during-runner", "before-result"])
@pytest.mark.parametrize("fails", [False, True], ids=["pass", "fail"])
@pytest.mark.parametrize("canonical_merge_repository", ["full-recovery"], indirect=True)
def test_fixed_candidate_actual_runner_result_survives_main_advance(
    canonical_merge_repository, timing: str, fails: bool,
) -> None:
    """Real canonical/mandatory/Job chain; not repository Full readiness acceptance."""
    from test_devx015_workflow_integration import TASK

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
        PublicationFenceError,
    )
    from ai_trading_system.platform.architecture.workflow_execution import bind_mandatory_acceptance

    tmp_path, _scope = canonical_merge_repository
    manifest = "config/architecture/devx_015_workflow_acceptance.v1.json"
    inventory = json.loads((ROOT / manifest).read_bytes())
    inventory["mapping_state"] = "COMPLETE_REVIEWED"
    inventory["variant_node_mapping"] = [
        {"case_id": row["id"], "variant": variant,
         "node_ids": ["tests/test_binding.py::test_required"]}
        for row in inventory["cases"] for variant in row["variants"]
    ]
    (tmp_path / manifest).write_text(json.dumps(inventory), encoding="utf-8")
    (tmp_path / "tests").mkdir(exist_ok=True)
    (tmp_path / ".gitignore").write_bytes(b"outputs/\n__pycache__/\n")
    peer = tmp_path.parent / (tmp_path.name + "-main-writer")
    advance = ["git", "-C", str(peer), "commit", "--allow-empty", "-m", "main advance"]
    body = "import subprocess\n"
    body += "def test_required():\n"
    if timing == "during-runner":
        body += f"    subprocess.run({advance!r}, check=True, capture_output=True)\n"
    body += f"    assert {not fails!r}, 'actual technical failure'\n"
    (tmp_path / "tests/test_binding.py").write_bytes(body.encode("utf-8"))
    fence = IntegrationPublicationFence(project_root=tmp_path)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    for phase in ("GENERATED_REBUILD_PRE", "GENERATED_REBUILD_POST", "CANDIDATE_COMMIT_PRE"):
        fence.checkpoint(
            transaction, phase=phase, actor="integration-coordinator",
            generator_ids=("canonical-task-source",) if phase.startswith("GENERATED_") else (),
        )
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-m", "freeze actual fixed candidate canonical Full runner")
    fence.checkpoint(transaction, phase="FORMAL_VALIDATION_PRE", actor="integration-coordinator")
    candidate = _git(tmp_path, "rev-parse", "HEAD")
    _git(tmp_path, "worktree", "add", str(peer), "main")
    index_path = tmp_path / _git(tmp_path, "rev-parse", "--git-path", "index")
    original_index = index_path.read_bytes()
    original_test = (tmp_path / "tests/test_binding.py").read_bytes()
    binding = bind_mandatory_acceptance(tmp_path, candidate)
    command = [
        sys.executable, "-m", "pytest", "-n2", "--dist", "loadfile", "tests/test_binding.py", "-q",
    ]
    advance_command = f"subprocess.run({advance!r},check=True,capture_output=True)\n"
    validation_cli = [
        sys.executable, "scripts/architecture_arch005_publication_fence.py", "validate",
        "--transaction", str(transaction), "--exact-phase", "FORMAL_VALIDATION_PRE",
        "--validation-tier", "full", "--require-candidate",
    ]
    script = (
        "import argparse,json,subprocess\nfrom pathlib import Path\n"
        "from scripts.run_validation_tier import "
        "_FullCommandRunner, _full_task_commitment, "
        "_run_mandatory_acceptance_command, _record_publication_full_result\n"
        "from ai_trading_system.platform.architecture.integration_publication_fence "
        "import IntegrationPublicationFence\n"
        "from ai_trading_system.platform.architecture.workflow_contract "
        "import repository_identity\n"
        "identity=repository_identity(Path.cwd())\n"
        "assert identity['checkout']==Path.cwd().as_posix()\n"
        f"transaction=Path({str(transaction)!r})\n"
        "fence=IntegrationPublicationFence(project_root=Path.cwd())\n"
        + (advance_command if timing == "before-runner" else "")
        + f"entry=subprocess.run({validation_cli!r},capture_output=True,text=True)\n"
        "assert entry.returncode==0,entry.stdout+entry.stderr\n"
        "checked=json.loads(entry.stdout)\n"
        "assert checked['validation_only'] and not checked['publication_allowed']\n"
        "publication=fence.checkpoint(transaction,phase='FULL_DISPATCHED',"
        "actor='integration-coordinator',full_run_id='actual-fixed-candidate')\n"
        "publication['pre_dispatch_readiness']={'fixture_scope':'v01-not-formal-readiness'}\n"
        f"publication['task_commitment']=_full_task_commitment(Path.cwd(),{TASK!r},"
        "publication['candidate_sha'])\n"
        f"publication['mandatory_acceptance_binding']={binding!r}\n"
        "directory=Path.cwd()/'outputs/validation_runtime/actual-v01'\n"
        "runner=_FullCommandRunner(args=argparse.Namespace(publication_transaction=transaction),"
        "root=Path.cwd(),artifact_dir=directory,publication_binding=publication,"
        f"provenance={{'task_id':{TASK!r}}})\n"
        f"result=_run_mandatory_acceptance_command({command!r},cwd=Path.cwd(),binding={binding!r},"
        "expected_collections=2,command_runner=runner)\n"
        + (advance_command if timing == "before-result" else "")
        + "summary=directory/'test_runtime_summary.json'\n"
        "status='PASS' if result['exit_code']==0 else 'FAIL'\n"
        "summary.write_text(json.dumps({**result,'git_commit':publication['candidate_sha'],"
        "'status':status},sort_keys=True),encoding='utf-8')\n"
        "runner.record_summary(summary,status=status)\n"
        "recorded=_record_publication_full_result("
        "argparse.Namespace(publication_transaction=transaction),repo_root=Path.cwd(),"
        "status='PASS' if result['exit_code']==0 else 'FAIL',summary_path=summary)\n"
        "print('V01_RESULT='+json.dumps({'result':result,'recorded':recorded}))\n"
    )
    environment = dict(
        os.environ, PYTHONPATH=os.pathsep.join([str(tmp_path), str(tmp_path / "src")])
    )
    for name in (
        "PYTEST_ADDOPTS", "PYTEST_CURRENT_TEST", "PYTEST_XDIST_WORKER", "PYTEST_XDIST_WORKER_COUNT",
        "AITS_MANDATORY_ACCEPTANCE_REQUEST",
    ):
        environment.pop(name, None)
    # Hang guard only, not a performance assertion: mandatory acceptance source
    # identity hashing alone measured ~170s on the 2026-09-25 host, above 120s.
    completed = subprocess.run(
        [sys.executable, "-c", script], cwd=tmp_path, env=environment,
        capture_output=True, text=True, timeout=1200,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    lines = [
        line.removeprefix("V01_RESULT=") for line in completed.stdout.splitlines()
        if line.startswith("V01_RESULT=")
    ]
    assert len(lines) == 1, completed.stdout
    result = json.loads(lines[0])
    assert (result["result"]["exit_code"] != 0) is fails, result
    if fails:
        assert "AssertionError: actual technical failure" in result["result"]["pytest_output"]
        assert result["result"]["mandatory_acceptance"]["status"] == "FAIL"
    else:
        accepted = result["result"]["mandatory_acceptance"]
        assert accepted["status"] == "PASS"
        assert accepted["evidence"]["binding"] == binding
        assert accepted["evidence"]["execution"]["collection_count"] == 2
    assert result["recorded"]["candidate_sha"] == candidate
    replay = fence.replay(transaction)
    assert replay.phase == "FORMAL_VALIDATION_RESULT" and replay.status == "PASS"
    assert sum(event["phase"] == "FULL_DISPATCHED" for event in replay.events) == 1
    assert sum(event["phase"] == "FORMAL_VALIDATION_RESULT" for event in replay.events) == 1
    execution = fence.guard.store.replay().active_leases[0].execution
    assert execution["state"] == "RESULT_RECORDED"
    assert execution["full_result_commitment"]["sha256"] == (
        execution["result"]["artifact"]["sha256"]
    )
    payload = replay.events[-1]["payload"]
    assert payload["validation_status"] == ("FAIL" if fails else "PASS")
    assert payload["candidate_sha"] == candidate
    assert payload["observed_main"] == _git(tmp_path, "rev-parse", "main") != candidate
    assert payload["publication_preflight_required"] is True
    summary = tmp_path / "outputs/validation_runtime/actual-v01/test_runtime_summary.json"
    raw = summary.read_bytes()
    assert json.loads(raw) == {
        **result["result"], "git_commit": candidate, "status": "FAIL" if fails else "PASS",
    }
    with pytest.raises(PublicationFenceError, match="PUBLICATION_EXPECTED_MAIN_STALE"):
        fence.checkpoint(transaction, phase="LOCAL_MAIN_FF_PRE", actor="integration-coordinator")
    assert _git(tmp_path, "rev-parse", "HEAD") == candidate
    assert index_path.read_bytes() == original_index
    assert (tmp_path / "tests/test_binding.py").read_bytes() == original_test
    # Synthetic adversary changes only this fixture's candidate ref to the
    # independently advanced same-tree commit. Main tolerance must not allow C2.
    newer_main = _git(tmp_path, "rev-parse", "main")
    candidate_ref = _git(tmp_path, "symbolic-ref", "HEAD")
    _git(tmp_path, "update-ref", candidate_ref, newer_main, candidate)
    try:
        with pytest.raises(PublicationFenceError, match="PUBLICATION_CANDIDATE_DRIFT"):
            fence.validate(transaction, validation_tier="full", require_candidate=True)
    finally:
        _git(tmp_path, "update-ref", candidate_ref, candidate, newer_main)
    assert fence.replay(transaction).events == replay.events
    fence.release(transaction, actor="integration-coordinator", outcome="failed")
    assert not fence.guard.replay().active_leases
    assert summary.read_bytes() == raw


@pytest.mark.parametrize(
    "case",
    [
        "pass",
        "deselect",
        "skip",
        "xfail",
        "failure",
        "collect_only",
        "disabled",
        "working_drift",
        "same_bytes_replace",
        "wrong_origin",
        "before_drift",
        "serial",
        "environment_drift",
        "distribution_drift",
        "wrong_interpreter",
        "loaded_code_drift",
        "loaded_sut_clean",
        "loaded_sut_drift",
        "loaded_sut_descriptors_clean",
        "loaded_sut_property_drift",
        "loaded_sut_classmethod_drift",
        "loaded_sut_wrapper_drift",
        "loaded_sut_wrapper_binding_drift",
        "loaded_sut_dependency_clean",
        "loaded_sut_dependency_disk_drift",
        "loaded_sut_dependency_code_drift",
        "result_replace",
        "request_replace",
    ],
)
def test_mandatory_acceptance_actual_runner_chain(tmp_path: Path, case: str) -> None:
    """Actual existing runner helper + real Git + child pytest, no identity mocks."""
    from ai_trading_system.platform.architecture.workflow_execution import (
        ExecutionContainmentError,
        bind_mandatory_acceptance,
        validate_mandatory_acceptance_result,
    )

    test_mandatory_acceptance_binds_actual_committed_inventory(tmp_path, "valid")
    # Load the genuine runner import closure, then copy only those project Python
    # sources into this fixture. Child execution must use its own committed code,
    # never the original checkout's modules or a patched identity gate.
    for relative in _acceptance_fixture_sources():
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, destination)
    manifest_path = "config/architecture/devx_015_workflow_acceptance.v1.json"
    (tmp_path / manifest_path).write_bytes(
        subprocess.check_output(["git", "show", "HEAD:" + manifest_path], cwd=tmp_path)
    )
    body = {
        "skip": "pytest.skip('required')",
        "xfail": "pytest.xfail('required')",
        "failure": "assert False, 'required target assertion'",
        "working_drift": "Path('executed').touch(); p=Path(__file__); "
        "p.write_text(p.read_text()+'\\n# changed')",
        "same_bytes_replace": "Path('executed').touch(); p=Path(__file__); "
        "q=p.with_suffix('.swap'); "
        "q.write_bytes(p.read_bytes()); q.replace(p)",
        "environment_drift": "Path('executed').touch(); "
        "os.environ['DEVX_ACCEPTANCE_SYNTHETIC_DRIFT']='changed'",
        "distribution_drift": "Path('executed').touch(); "
        "d=Path('devx_synthetic_dependency-1.0.dist-info'); d.mkdir(); "
        "(d/'METADATA').write_text('Name: devx-synthetic-dependency\\nVersion: 1.0\\n'); "
        # A well-formed RECORD passes the inventory gate, so this exercises the
        # runtime-identity change rather than the earlier empty-RECORD refusal.
        "(d/'RECORD').write_text(d.name+'/METADATA,,\\n'+d.name+'/RECORD,,\\n'); "
        "sys.path.insert(0, str(Path.cwd()))",
        "loaded_code_drift": "Path('executed').touch(); "
        "from ai_trading_system.platform.architecture import workflow_execution as execution; "
        "execution._job_name.__code__=(lambda value: value).__code__",
        "loaded_sut_drift": "import json; Path('executed').touch(); "
        "from ai_trading_system.platform.architecture import "
        "integration_publication_fence as fence; "
        "assert Path(fence.__file__).resolve().is_relative_to(Path.cwd()); "
        "before=fence._identifier('identity-probe','field'); "
        "fence._identifier.__code__=(lambda value,field: 'incorrect-loaded-body').__code__; "
        "after=fence._identifier('identity-probe','field'); "
        "assert before == 'identity-probe' and after != before; "
        "Path('loaded-sut-fault.json').write_text(json.dumps("
        "{'module':fence.__file__,'before':before,'after':after}))",
    }.get(case, "assert True")
    descriptor_cases = {
        "loaded_sut_descriptors_clean": "",
        "loaded_sut_property_drift":
            "probe.Sample.value.fget.__code__=(lambda self: 99).__code__; "
            "assert sample.value == 99",
        "loaded_sut_classmethod_drift":
            "probe.Sample.__dict__['class_value'].__func__.__code__="
            "(lambda cls: 99).__code__; assert sample.class_value() == 99",
        "loaded_sut_wrapper_drift":
            "probe.decorated.__code__=probe.changed_wrapper(probe.decorated.__wrapped__).__code__; "
            "assert probe.decorated() == 99",
        "loaded_sut_wrapper_binding_drift":
            "probe.decorated.__wrapped__=lambda: 99; "
            "assert probe.decorated() == 7 and probe.decorated.__wrapped__() == 99",
    }
    if case in descriptor_cases:
        (tmp_path / "src/ai_trading_system/devx_binding_probe.py").write_text(
            '''from contextlib import contextmanager
from functools import cache, cached_property, wraps
def instrument(function):
    @wraps(function)
    def wrapped():
        return function()
    return wrapped
def changed_wrapper(function):
    def wrapped():
        return function() + 92
    return wrapped
@instrument
def decorated():
    return 7
@cache
def cached():
    return 7
@contextmanager
def managed():
    yield 7
class Sample:
    @property
    def value(self):
        return getattr(self, '_value', 7)
    @value.setter
    def value(self, value):
        self._value = value
    @classmethod
    def class_value(cls):
        return 7
    @staticmethod
    def static_value():
        return 7
    @cached_property
    def cached_value(self):
        return 7
''', encoding="utf-8",
        )
        body = (
            "from ai_trading_system import devx_binding_probe as probe; "
            "sample=probe.Sample(); assert sample.value == 7; sample.value=8; "
            "assert sample.value == 8; assert sample.class_value() == 7; "
            "assert sample.static_value() == 7; assert sample.cached_value == 7; "
            "assert probe.decorated() == 7 and probe.cached() == 7; "
            "context=probe.managed(); assert context.__enter__() == 7; "
            "context.__exit__(None,None,None); "
            + (descriptor_cases[case] + "; " if descriptor_cases[case] else "")
            + "Path('executed').touch()"
        )
    dependency_root: Path | None = None
    if case.startswith("loaded_sut_dependency_"):
        # Real, isolated installation metadata and code, never edit the shared venv.
        dependency_root = tmp_path.parent / (tmp_path.name + "-dependency")
        dependency_root.mkdir()
        dependency_source = "def value():\n    return 7\n"
        (dependency_root / "devx_runtime_dependency.py").write_text(
            dependency_source, encoding="utf-8"
        )
        metadata = dependency_root / "devx_runtime_dependency-1.0.dist-info"
        metadata.mkdir()
        (metadata / "METADATA").write_text(
            "Metadata-Version: 2.1\nName: devx-runtime-dependency\nVersion: 1.0\n",
            encoding="utf-8",
        )
        (metadata / "RECORD").write_text(
            "devx_runtime_dependency.py,,\n"
            "devx_runtime_dependency-1.0.dist-info/METADATA,,\n"
            "devx_runtime_dependency-1.0.dist-info/RECORD,,\n", encoding="utf-8",
        )
        mutation = {
            "loaded_sut_dependency_clean": "",
            "loaded_sut_dependency_disk_drift":
                "source.write_bytes(raw+b'\\n# changed dependency bytes\\n'); "
                "assert source.read_bytes() != raw; ",
            "loaded_sut_dependency_code_drift":
                "dependency.value.__code__=(lambda: 99).__code__; "
                "assert dependency.value() == 99 and source.read_bytes() == raw; ",
        }[case]
        body = (
            "import json, hashlib, importlib.metadata; "
            "import devx_runtime_dependency as dependency; "
            "source=Path(dependency.__file__); raw=source.read_bytes(); "
            "assert not source.resolve().is_relative_to(Path.cwd()); "
            "assert dependency.value() == 7; "
            "assert importlib.metadata.version('devx-runtime-dependency') == '1.0'; "
            + mutation
            + "assert importlib.metadata.version('devx-runtime-dependency') == '1.0'; "
            "Path('dependency-fault.json').write_text(json.dumps({'version':'1.0',"
            "'source':str(source),'before_sha256':hashlib.sha256(raw).hexdigest(),"
            "'after_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),"
            "'after_value':dependency.value()})); Path('executed').touch()"
        )
    (tmp_path / "tests/test_binding.py").write_text(
        "import os, sys, pytest\nfrom pathlib import Path\n"
        + (
            "__file__ = str(Path(__file__).with_name('wrong.py'))\n"
            if case == "wrong_origin"
            else ""
        )
        + "def test_required():\n    "
        + body
        + "\ndef test_optional():\n    assert True\n",
        encoding="utf-8",
    )
    if case in {"result_replace", "request_replace"}:
        hook = """
import json, os
from pathlib import Path
def replace_same_bytes(path):
    raw = path.read_bytes()
    original = path.stat().st_ino
    replacement = path.with_suffix('.replacement')
    replacement.write_bytes(raw)
    assert replacement.stat().st_ino != original
    replacement.replace(path)
    assert path.read_bytes() == raw
    Path('replacement-executed').write_text(str(original)+':'+str(path.stat().st_ino))
def pytest_unconfigure(config):
    if hasattr(config, 'workerinput'): return
    descriptor = json.loads(os.environ['AITS_MANDATORY_ACCEPTANCE_REQUEST'])
    request = json.loads(Path(descriptor['path']).read_bytes())
    output = Path(request['output'])
    assert json.loads(output.read_bytes())['execution']['status'] == 'PASS'
    replace_same_bytes(output)
"""
        if case == "request_replace":
            hook = hook[: hook.index("def pytest_unconfigure")]
            hook += (
                "if 'PYTEST_XDIST_WORKER' not in os.environ:\n"
                " descriptor=json.loads(os.environ['AITS_MANDATORY_ACCEPTANCE_REQUEST'])\n"
                " replace_same_bytes(Path(descriptor['path']))\n"
            )
        (tmp_path / "tests/conftest.py").write_text(hook, encoding="utf-8")
    _git(tmp_path, "add", "tests", "src", "scripts")
    _git(tmp_path, "commit", "-m", "actual runner case")
    binding = bind_mandatory_acceptance(tmp_path, _git(tmp_path, "rev-parse", "HEAD"))
    if case == "before_drift":
        (tmp_path / "tests/test_binding.py").write_text(
            "from pathlib import Path\nPath('forbidden-execution').touch()\n", encoding="utf-8"
        )
    selection = {
        "deselect": ["-k", "optional"],
        "collect_only": ["--collect-only"],
        "disabled": ["-p", "no:ai_trading_system.platform.architecture.workflow_execution"],
    }.get(case, [])
    worker_count = 16 if case.startswith("loaded_sut_") else 2
    expected_collections = 1 if case == "serial" else worker_count
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-n0" if case == "serial" else f"-n{worker_count}",
        "--dist",
        "loadfile",
        "tests/test_binding.py",
        "-q",
        *selection,
    ]
    if case == "wrong_interpreter":
        command[0] = str(tmp_path / "not-bound-python.exe")
    script = (
        "import json\nfrom pathlib import Path\n"
        "from scripts.run_validation_tier import _run_mandatory_acceptance_command\n"
        f"result=_run_mandatory_acceptance_command({command!r},cwd=Path.cwd(),"
        f"binding={binding!r},expected_collections={expected_collections})\n"
        "print('RUNNER_RESULT='+json.dumps(result))\n"
    )
    environment = dict(
        os.environ, PYTHONPATH=os.pathsep.join([str(tmp_path), str(tmp_path / "src")])
    )
    if dependency_root is not None:
        environment["PYTHONPATH"] += os.pathsep + str(dependency_root)
    for name in (
        "PYTEST_ADDOPTS",
        "PYTEST_CURRENT_TEST",
        "PYTEST_XDIST_WORKER",
        "PYTEST_XDIST_WORKER_COUNT",
        "AITS_MANDATORY_ACCEPTANCE_REQUEST",
    ):
        environment.pop(name, None)
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        # Hang guard only, not a performance assertion. v65 measured ~213s for
        # the sixteen-worker loaded-code controls; on the 2026-09-25 host the
        # two-worker cases measured ~120-126s once the fixture carried the
        # runner's lazily imported modules, so both groups share one bound.
        timeout=1200,
    )
    (tmp_path / "original-runner-command.py").write_text(script, encoding="utf-8")
    (tmp_path / "original-runner-output.json").write_text(json.dumps({
        "returncode": completed.returncode, "stdout": completed.stdout, "stderr": completed.stderr,
    }), encoding="utf-8")
    assert completed.returncode == 0, completed.stdout + completed.stderr
    records = [
        line.removeprefix("RUNNER_RESULT=")
        for line in completed.stdout.splitlines()
        if line.startswith("RUNNER_RESULT=")
    ]
    assert len(records) == 1, completed.stdout + completed.stderr
    result = json.loads(records[0])
    if case in {
        "pass", "serial", "loaded_sut_clean", "loaded_sut_descriptors_clean",
        "loaded_sut_dependency_clean",
    }:
        assert result["exit_code"] == 0, result
        evidence = result["mandatory_acceptance"]["evidence"]
        custody = result["mandatory_acceptance"]["result_custody"]
        captured = json.dumps(evidence, sort_keys=True).encode()
        assert custody["result_sha256"] == hashlib.sha256(captured).hexdigest()
        assert custody["result_size_bytes"] == len(captured)
        assert all(
            len(custody[key]) == 2
            for key in ("request_identity", "result_identity", "root_identity")
        )
        assert evidence["binding"] == binding
        runtime = evidence["runtime_identity"]
        assert Path(runtime["executable"]) == Path(sys.executable)
        assert (
            runtime["executable_sha256"]
            == hashlib.sha256(Path(sys.executable).read_bytes()).hexdigest()
        )
        assert Path(runtime["engine"]).name.lower() == "python311.dll"
        assert (
            runtime["engine_sha256"]
            == hashlib.sha256(Path(runtime["engine"]).read_bytes()).hexdigest()
        )
        assert runtime["python_version"] == sys.version
        assert "environment" not in runtime  # No raw environment or secret values in receipt.
        assert evidence["execution"]["collection_count"] == expected_collections
        # A re-labelled PASS without its actual target call is rejected by the parent.
        evidence["execution"]["reports"]["tests/test_binding.py::test_required"].pop(1)
        with pytest.raises(ExecutionContainmentError, match="ACCEPTANCE_RESULT_INVALID"):
            validate_mandatory_acceptance_result(
                json.dumps(evidence).encode(),
                binding,
                exit_code=0,
                expected_collections=expected_collections,
            )
    else:
        assert result["exit_code"] != 0, result
        assert result["mandatory_acceptance"]["status"] == "FAIL"
        if case == "before_drift":
            assert not (tmp_path / "forbidden-execution").exists()
            assert "ACCEPTANCE_INPUT_NOT_CANDIDATE" in result["mandatory_acceptance"]["reason"]
        elif case in {"working_drift", "same_bytes_replace"}:
            assert (tmp_path / "executed").is_file(), result
            reason = (
                "ACCEPTANCE_INPUT_NOT_CANDIDATE"
                if case == "working_drift"
                else "ACCEPTANCE_CHECKOUT_CHANGED"
            )
            assert reason in result["mandatory_acceptance"]["reason"], result
        elif case in {
            "environment_drift", "distribution_drift", "loaded_code_drift", "loaded_sut_drift",
            "loaded_sut_property_drift", "loaded_sut_classmethod_drift",
            "loaded_sut_wrapper_drift", "loaded_sut_wrapper_binding_drift",
            "loaded_sut_dependency_disk_drift", "loaded_sut_dependency_code_drift",
        }:
            assert (tmp_path / "executed").is_file(), result
            if case == "loaded_sut_drift":
                fault = json.loads((tmp_path / "loaded-sut-fault.json").read_text())
                assert fault["before"] == "identity-probe"
                assert fault["after"] == "incorrect-loaded-body"
            if case.startswith("loaded_sut_dependency_"):
                fault = json.loads((tmp_path / "dependency-fault.json").read_text())
                assert fault["version"] == "1.0"
                if case == "loaded_sut_dependency_disk_drift":
                    assert fault["before_sha256"] != fault["after_sha256"]
                else:
                    assert fault["before_sha256"] == fault["after_sha256"]
                    assert fault["after_value"] == 99
            assert any(
                code in result["mandatory_acceptance"]["reason"]
                for code in ("ACCEPTANCE_RUNTIME_CHANGED", "ACCEPTANCE_WORKER_INPUT_CHANGED")
            ), result
        elif case == "wrong_interpreter":
            assert "ACCEPTANCE_INTERPRETER_UNBOUND" in result["mandatory_acceptance"]["reason"], (
                result
            )
            assert result["elapsed_seconds"] == 0.0
        elif case in {"result_replace", "request_replace"}:
            assert (tmp_path / "replacement-executed").is_file(), result
            assert "HANDLE_EXPECTED_IDENTITY_CHANGED" in (
                result["mandatory_acceptance"]["reason"] + result["pytest_output"]
            ), result
        else:
            assert (
                "ACCEPTANCE_INPUT_NOT_CANDIDATE" not in result["mandatory_acceptance"]["reason"]
            ), result


@pytest.mark.parametrize(
    "case,body,selection,expected_reason",
    [
        ("pass", "assert True", [], None),
        ("missing", "assert True", [], "MANDATORY_NODE_MISSING"),
        ("deselect", "assert True", ["-k", "optional"], "MANDATORY_NODE_MISSING"),
        ("skip", "pytest.skip('required')", [], "MANDATORY_REPORT_NOT_PASS"),
        ("xfail", "pytest.xfail('required')", [], "MANDATORY_REPORT_NOT_PASS"),
        ("xpass", "assert True", [], "MANDATORY_REPORT_NOT_PASS"),
        ("failure", "assert False, 'target assertion'", [], "PYTEST_NOT_SUCCESSFUL"),
        (
            "collect_only",
            "assert True",
            ["--collect-only"],
            "MANDATORY_PHASES_INCOMPLETE_OR_DUPLICATE",
        ),
    ],
)
def test_mandatory_acceptance_real_xdist_result_guard(
    tmp_path: Path, case: str, body: str, selection: list[str], expected_reason: str | None,
    mutation: str | None = None,
) -> None:
    """Actual small pytest processes, not repository Full or 106-variant coverage."""
    marker = (
        "@pytest.mark.xfail(reason='must not count', strict=False)\n" if case == "xpass" else ""
    )
    (tmp_path / "test_required.py").write_text(
        "import pytest\n"
        + marker
        + "def test_required():\n    "
        + body
        + "\ndef test_optional():\n    pytest.skip('existing optional skip')\n",
        encoding="utf-8",
    )
    required = ["test_required.py::test_required"]
    if case == "missing":
        required.append("test_required.py::test_missing")
    assert mutation in {None, "M09", "M05"}
    script = (
        "import hashlib, inspect, json, os, sys, textwrap, pytest\nfrom pathlib import Path\n"
        "from ai_trading_system.platform.architecture.workflow_execution "
        "import MandatoryAcceptancePlugin\n"
        "method=MandatoryAcceptancePlugin.pytest_sessionfinish\n"
        "before=textwrap.dedent(inspect.getsource(method))\n"
        "after=before\n"
        f"if {mutation!r} == 'M09':\n"
        "    target='    reasons: set[str] = set()'\n"
        "    assert before.count(target)==1, 'INVALID_MUTATION_TARGET'\n"
        "    after=before.replace(target,"
        "'    self.required = self.required.intersection(self.collections[0])\\n'+target,1)\n"
        f"elif {mutation!r} == 'M05':\n"
        "    target='    self.result = {'\n"
        "    assert before.count(target)==1, 'INVALID_MUTATION_TARGET'\n"
        "    after=before.replace(target,"
        "'    reasons.clear()\\n    session.exitstatus = 0\\n'+target,1)\n"
        "Path('method-before.py').write_text(before,encoding='utf-8')\n"
        "Path('method-after.py').write_text(after,encoding='utf-8')\n"
        "identity={'module_path':inspect.getsourcefile(method),"
        "'module_sha256':hashlib.sha256(Path(inspect.getsourcefile(method)).read_bytes()).hexdigest(),"
        "'before_sha256':hashlib.sha256(before.encode()).hexdigest(),"
        "'after_sha256':hashlib.sha256(after.encode()).hexdigest(),"
        f"'mutation':{mutation!r},'pid':os.getpid(),'interpreter':sys.executable,"
        "'python_version':sys.version,'platform':sys.platform}\n"
        "Path('implementation-identity.json').write_text(json.dumps(identity),encoding='utf-8')\n"
        f"if {mutation!r} is not None:\n"
        "    namespace={}\n"
        "    exec(compile(after,str(Path('method-after.py').absolute()),'exec'),"
        "method.__globals__,namespace)\n"
        "    MandatoryAcceptancePlugin.pytest_sessionfinish=namespace['pytest_sessionfinish']\n"
        f"guard=MandatoryAcceptancePlugin({required!r})\n"
        f"args={['-n', '2', '--dist', 'loadfile', '-q', 'test_required.py', *selection]!r}\n"
        "code=pytest.main(args, plugins=[guard])\n"
        "print('ACCEPTANCE_RESULT='+json.dumps(guard.result))\n"
        "raise SystemExit(code)\n"
    )
    environment = dict(os.environ, PYTHONPATH=str(ROOT / "src"))
    for name in (
        "PYTEST_ADDOPTS",
        "PYTEST_CURRENT_TEST",
        "PYTEST_XDIST_WORKER",
        "PYTEST_XDIST_WORKER_COUNT",
    ):
        environment.pop(name, None)
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=LOADED_HOST_CLI_TIMEOUT_SECONDS,
    )
    (tmp_path / "driver.py").write_text(script, encoding="utf-8")
    (tmp_path / "driver-output.txt").write_text(completed.stdout, encoding="utf-8")
    records = [
        line.removeprefix("ACCEPTANCE_RESULT=")
        for line in completed.stdout.splitlines()
        if line.startswith("ACCEPTANCE_RESULT=")
    ]
    assert len(records) == 1, completed.stdout
    result = json.loads(records[0])
    assert result is not None, completed.stdout
    (tmp_path / "actual-result.json").write_text(
        json.dumps({"exit_code": completed.returncode, "result": result}), encoding="utf-8"
    )
    assert result["required_nodes"] == sorted(required), "MANDATORY_FROZEN_SELECTION_CHANGED"
    if expected_reason is None:
        assert completed.returncode == 0, completed.stdout
        assert result["status"] == "PASS"
        assert result["collection_count"] == 2
        assert result["reason_codes"] == []
    else:
        assert completed.returncode != 0, "PYTEST_FAILURE_MUST_REMAIN_FAILURE"
        assert result["status"] == "FAIL"
        assert expected_reason in result["reason_codes"], completed.stdout


@pytest.mark.parametrize("mutation", [None, "M09"], ids=["original", "mutant"])
@pytest.mark.parametrize("deselect", [False, True], ids=["complete", "deselect"])
def test_m09_actual_deselection_mutant_hits_frozen_selection_assertion(
    tmp_path: Path, mutation: str | None, deselect: bool,
) -> None:
    """Counterfactual on the original xdist test; no arbitrary-error mutant kill."""
    killed = mutation == "M09" and deselect
    args = (
        tmp_path, "deselect" if deselect else "pass", "assert True",
        ["-k", "optional"] if deselect else [],
        "MANDATORY_NODE_MISSING" if deselect else None,
        mutation,
    )
    if killed:
        with pytest.raises(
            AssertionError, match=r"^MANDATORY_FROZEN_SELECTION_CHANGED(?:\n|$)"
        ):
            test_mandatory_acceptance_real_xdist_result_guard(*args)
    else:
        test_mandatory_acceptance_real_xdist_result_guard(*args)
    actual = _read_json(tmp_path / "actual-result.json")
    identity = _read_json(tmp_path / "implementation-identity.json")
    assert identity["mutation"] == mutation
    assert (identity["before_sha256"] != identity["after_sha256"]) == (mutation == "M09")
    assert actual["result"]["collection_count"] == 2
    if killed:
        assert actual["exit_code"] == 0 and actual["result"]["status"] == "PASS"
        assert actual["result"]["required_nodes"] == []
        assert actual["result"]["reports"] == {}
    elif deselect:
        assert actual["exit_code"] != 0 and actual["result"]["status"] == "FAIL"
        assert "MANDATORY_NODE_MISSING" in actual["result"]["reason_codes"]
    else:
        assert actual["exit_code"] == 0 and actual["result"]["status"] == "PASS"
    (tmp_path / "counterfactual.json").write_text(json.dumps({
        "schema_version": "devx015_m09_counterfactual.v1", "mutant_id": "M09",
        "mutation": mutation, "deselect": deselect, "target_assertion_killed": killed,
        "target_assertion": "MANDATORY_FROZEN_SELECTION_CHANGED",
        "scope": "ORIGINAL_REAL_XDIST_RESULT_GUARD", "formal_acceptance": False,
    }), encoding="utf-8")


@pytest.mark.parametrize("mutation", [None, "M05"], ids=["original", "mutant"])
@pytest.mark.parametrize("fails", [False, True], ids=["pass", "failure"])
def test_m05_actual_pytest_failure_mutant_hits_original_exit_assertion(
    tmp_path: Path, mutation: str | None, fails: bool,
) -> None:
    """The child really fails; only its result guard deliberately lies about success."""
    killed = mutation == "M05" and fails
    args = (
        tmp_path, "failure" if fails else "pass",
        "assert False, 'target assertion'" if fails else "assert True", [],
        "PYTEST_NOT_SUCCESSFUL" if fails else None, mutation,
    )
    if killed:
        with pytest.raises(
            AssertionError, match=r"^PYTEST_FAILURE_MUST_REMAIN_FAILURE(?:\n|$)"
        ):
            test_mandatory_acceptance_real_xdist_result_guard(*args)
    else:
        test_mandatory_acceptance_real_xdist_result_guard(*args)
    actual = _read_json(tmp_path / "actual-result.json")
    identity = _read_json(tmp_path / "implementation-identity.json")
    assert identity["mutation"] == mutation
    assert (identity["before_sha256"] != identity["after_sha256"]) == (mutation == "M05")
    result = actual["result"]
    assert result["collection_count"] == 2
    assert result["required_nodes"] == ["test_required.py::test_required"]
    assert result["pytest_exitstatus"] == (1 if fails else 0)
    reports = result["reports"]["test_required.py::test_required"]
    assert ["call", "failed" if fails else "passed", False] in reports
    if killed:
        assert actual["exit_code"] == 0 and result["status"] == "PASS"
        assert result["reason_codes"] == []
    elif fails:
        assert actual["exit_code"] != 0 and result["status"] == "FAIL"
        assert "PYTEST_NOT_SUCCESSFUL" in result["reason_codes"]
    else:
        assert actual["exit_code"] == 0 and result["status"] == "PASS"
    (tmp_path / "counterfactual.json").write_text(json.dumps({
        "schema_version": "devx015_m05_counterfactual.v1", "mutant_id": "M05",
        "mutation": mutation, "actual_pytest_failed": fails, "target_assertion_killed": killed,
        "target_assertion": "PYTEST_FAILURE_MUST_REMAIN_FAILURE",
        "scope": "ORIGINAL_REAL_XDIST_RESULT_GUARD", "formal_acceptance": False,
    }), encoding="utf-8")


def _stage_inputs(root: Path, *, excluded: set[str] | None = None) -> dict[str, dict[str, Any]]:
    """Independent external raw Git objects, frozen before the renderer runs."""
    states = {}
    for row in _git(root, "ls-files", "--stage").splitlines():
        metadata, name = row.split("\t", 1)
        if name in (excluded or set()):
            continue
        mode, _old_oid, stage = metadata.split()
        assert stage == "0"
        if mode not in {"100644", "100755"}:
            continue
        content = (root / name).read_bytes()
        oid = _git(root, "hash-object", "--no-filters", "--stdin", content=content)
        states[name] = {"exists": True, "mode": mode, "type": "blob", "oid": oid}
    return states


def _stage_snapshot(
    root: Path, *, excluded: set[str] | None = None
) -> tuple[dict[str, bytes], str, bytes]:
    files = {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if ".git" not in path.relative_to(root).parts
        and path.relative_to(root).as_posix() not in (excluded or set())
        and path.is_file()
    }
    return files, _git(root, "rev-parse", "HEAD"), (root / ".git/index").read_bytes()


def _canonical_stage_outputs(root: Path) -> set[str]:
    from ai_trading_system.platform.architecture import task_registry_canonical as canonical

    policy = canonical.load_cutover_policy(root)
    return {
        canonical.CANONICAL_INDEX_PATH,
        policy["canonical"]["consumer_inventory_path"],
        policy["generated_views"]["active_path"],
        policy["generated_views"]["completed_path"],
    }


def _seed_source_generator_architecture(
    root: Path, *, capture_runtime_baseline: bool = False
) -> tuple[list[str], set[str]]:
    """Use the official builders before a fixture freezes its source namespace."""
    from ai_trading_system.platform.architecture import (
        build_aggregate_shadow_index,
        build_architecture_fitness,
        build_module_manifest,
        build_test_manifest,
        load_deprecation_policy,
        scan_deprecation_inventory,
        write_generated_architecture_artifact,
    )
    from ai_trading_system.yaml_loader import safe_load_yaml_path

    additions = [
        "config/architecture/devex_ownership_policy.yaml",
        "config/architecture/arch_004c_dependency_policy.yaml",
        "config/architecture/arch_004g_deprecation_policy.yaml",
        "inputs/architecture/arch_004c_direct_writer_baseline.yaml",
    ]
    for name in additions:
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, target)
    source_name = "src/ai_trading_system/contracts/synthetic.py"
    source = root / source_name
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(b"VALUE = 1\n")
    deprecation_path = root / "config/architecture/arch_004g_deprecation_policy.yaml"
    deprecation = safe_load_yaml_path(deprecation_path)
    target = dict(deprecation["targets"][1])
    target.update(surface_id="synthetic_module", path=source_name)
    deprecation["targets"] = [target]
    write_generated_architecture_artifact(deprecation_path, deprecation)
    if capture_runtime_baseline:
        from ai_trading_system.platform.architecture import capture_direct_writer_baseline

        # Test-only exact baseline for the complete copied runtime, before M.
        # Keep every real dependency/ownership rule and validate actual findings.
        _git(root, "add", "--", "src")
        _git(root, "commit", "-m", "freeze complete source-job runtime baseline input")
        capture_direct_writer_baseline(
            source_root=root / "src/ai_trading_system",
            output_path=root / additions[3],
            canonical_writer_path="src/ai_trading_system/platform/artifacts/writer.py",
            source_commit=_git(root, "rev-parse", "HEAD"),
        )
    policy = root / additions[0]
    outputs = {
        "module": "inputs/architecture/arch_004e_module_manifest.yaml",
        "test": "inputs/architecture/arch_004e_test_manifest.yaml",
        "aggregate": "inputs/architecture/arch_004e_aggregate_shadow_index.yaml",
        "fitness": "inputs/architecture/arch_004e_architecture_fitness.yaml",
        "deprecation": "inputs/architecture/arch_004g_deprecation_inventory.yaml",
    }
    for name, builder in (
        ("module", build_module_manifest),
        ("test", build_test_manifest),
        ("aggregate", build_aggregate_shadow_index),
    ):
        write_generated_architecture_artifact(
            root / outputs[name], builder(project_root=root, policy_path=policy)
        )
    fitness = build_architecture_fitness(
        project_root=root,
        policy_path=policy,
        module_manifest_path=root / outputs["module"],
        test_manifest_path=root / outputs["test"],
        aggregate_index_path=root / outputs["aggregate"],
        dependency_policy_path=root / additions[1],
        direct_writer_baseline_path=root / additions[3],
    )
    assert fitness["status"] == "PASS"
    write_generated_architecture_artifact(root / outputs["fitness"], fitness)
    inventory = scan_deprecation_inventory(
        load_deprecation_policy(deprecation_path),
        project_root=root,
        architecture_fitness_path=root / outputs["fitness"],
    ).to_dict()
    write_generated_architecture_artifact(root / outputs["deprecation"], inventory)
    return [*additions, source_name, *outputs.values()], set(outputs.values())


def _install_source_job_runtime(root: Path) -> None:
    """Install genuine project bytes before M so fresh CLI imports are local."""
    shutil.copytree(
        ROOT / "src/ai_trading_system",
        root / "src/ai_trading_system",
        dirs_exist_ok=True,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    scripts = {
        path.relative_to(root).as_posix()
        for path in (root / "scripts").glob("*.py")
        if (ROOT / path.relative_to(root)).is_file()
    } | {
        "scripts/architecture_arch005_workflow.py",
        "scripts/architecture_arch005_task_source.py",
        "scripts/architecture_arch005_checkout_guard.py",
        "scripts/architecture_arch005_source_preservation.py",
        "scripts/architecture_arch005_publication_fence.py",
        "scripts/architecture_compatibility_authority.py",
        "scripts/architecture_report_catalog_flow_authority.py",
        "scripts/run_validation_tier.py",
        "scripts/validation_readiness.py",
    }
    for name in sorted(scripts | {"config/architecture/arch_005_source_preservation.yaml"}):
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, target)


@pytest.mark.parametrize("canonical_merge_repository", ["source-job"], indirect=True)
def test_source_job_freezes_real_readiness_entrypoints(canonical_merge_repository) -> None:
    """Reject missing inspector inputs before paying for the source Job chain."""
    from ai_trading_system.platform.architecture.validation_readiness import (
        _inspection_code_identity,
    )

    root, scope = canonical_merge_repository
    candidate = _git(root, "rev-parse", "HEAD")
    assert candidate == scope["latest_main"]
    _inspection_code_identity(root, candidate)
    for name in ("scripts/run_validation_tier.py", "scripts/validation_readiness.py"):
        committed = subprocess.run(
            ["git", "cat-file", "blob", f"{candidate}:{name}"],
            cwd=root, capture_output=True, check=True, timeout=30,
        ).stdout
        assert committed == (ROOT / name).read_bytes() == (root / name).read_bytes()
    # A live modification must still be refused by the original identity gate.
    path = root / "scripts/validation_readiness.py"
    original = path.read_bytes()
    try:
        path.write_bytes(original + b"\n# uncommitted readiness input\n")
        with pytest.raises(ValueError, match="READINESS_INSPECTION_CODE_DIRTY"):
            _inspection_code_identity(root, candidate)
    finally:
        path.write_bytes(original)
    _inspection_code_identity(root, candidate)


@pytest.fixture
def four_generator_repository(canonical_merge_repository):
    """Real existing compatibility fixture plus independently frozen builder inputs."""
    root, _scope = canonical_merge_repository
    additions, outputs = _seed_source_generator_architecture(root)
    flow = root / "docs/system_flow.md"
    flow.write_bytes(flow.read_bytes() + b"DEVX015 private four-generator fixture block.\n\n")
    _git(root, "add", "--", *additions)
    return root, outputs, _stage_inputs(root)


@pytest.fixture
def source_candidate_render_repository(canonical_merge_repository):
    from test_devx015_workflow_integration import TASK, _freeze_candidate_fixture

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )
    from ai_trading_system.platform.architecture.workflow_integration import (
        prepare_source_generation,
    )

    root, scope = canonical_merge_repository
    raw = b"reviewed source candidate\0raw\r\n"
    added = b"reviewed candidate addition\r\n"
    (root / "mode.txt").write_bytes(raw)
    (root / "extra-source.txt").write_bytes(added)
    (root / "rename-old.txt").unlink()
    _git(root, "update-index", "--chmod=+x", "--", "mode.txt")
    flow = root / "docs/system_flow.md"
    flow.write_bytes(flow.read_bytes() + b"Source candidate renderer fixture block.\n\n")
    transaction = _freeze_candidate_fixture(root)
    fence = IntegrationPublicationFence(project_root=root)
    assert fence.replay(transaction).phase == "TASK_SOURCE_PRE_WRITE"
    prepared = prepare_source_generation(
        root, TASK, transaction_path=transaction, actor="integration-coordinator"
    )
    assert prepared["status"] == "PREPARED_SOURCE_GENERATION_INPUTS"
    assert prepared["generator_order"] == [
        "canonical-task-source",
        "architecture-manifests",
        "report-flow-authority",
        "compatibility-authority",
    ]
    # Phase advancement is the real fence event, not a handcrafted prepared identity.
    fence.checkpoint(
        transaction,
        phase="GENERATED_REBUILD_PRE",
        actor="integration-coordinator",
        generator_ids=tuple(scope["generator_order"]),
    )
    assert fence.replay(transaction).phase == "GENERATED_REBUILD_PRE"
    return root, scope, transaction, prepared, {"mode.txt": raw, "extra-source.txt": added}


@pytest.mark.parametrize("canonical_merge_repository", ["source-candidate"], indirect=True)
def test_render_source_candidate_delta_closes_actual_four_generator_view(
    source_candidate_render_repository,
) -> None:
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )
    from ai_trading_system.platform.architecture.workflow_integration import (
        render_source_candidate_delta,
    )

    root, scope, transaction, prepared, source_bytes = source_candidate_render_repository
    main_objects = {}
    for row in _git(root, "ls-tree", "-r", "--full-tree", scope["latest_main"]).splitlines():
        metadata, name = row.split("\t", 1)
        mode, kind, oid = metadata.split()
        main_objects[name] = {"exists": True, "mode": mode, "type": kind, "oid": oid}
    before = _stage_snapshot(root)
    result, captured = render_source_candidate_delta(
        root, prepared, transaction_path=transaction, actor="integration-coordinator"
    )
    assert result["schema_version"] == "rendered_source_candidate_delta.v1"
    assert result["status"] == "RENDERED_SOURCE_CANDIDATE_DELTA"
    assert result["materialization_allowed"] is False
    assert result["publication_allowed"] is False
    assert result["generation"]["generator_order"] == prepared["generator_order"]
    operations = result["operations"]
    by_path = {row["path"]: row for row in operations}
    absent = {"exists": False, "mode": None, "type": None, "oid": None}
    assert len(by_path) == len(operations)
    assert set(captured) == {row["path"] for row in operations if row["after"]["exists"]}
    for row in operations:
        name = row["path"]
        assert row["before"] == main_objects.get(name, absent)
        if row["after"]["exists"]:
            content = captured[name]
            raw_oid = hashlib.sha1(
                b"blob " + str(len(content)).encode() + b"\0" + content
            ).hexdigest()
            assert row["after"]["oid"] == raw_oid
            assert row["after"]["type"] == "blob"
        else:
            assert row["operation"] == "D" and row["after"] == absent
        if row["origin"] == "OFFICIAL_GENERATOR" and row["after"]["exists"]:
            expected_mode = row["before"]["mode"] if row["before"]["exists"] else "100644"
            assert row["after"]["mode"] == expected_mode
    for name, content in source_bytes.items():
        assert captured[name] == content
        assert by_path[name]["origin"] == "FROZEN_SOURCE"
        assert by_path[name]["generator_id"] is None
        assert by_path[name]["after"]["oid"] == _git(
            root, "hash-object", "--no-filters", "--stdin", content=content
        )
    assert by_path["mode.txt"]["after"]["mode"] == "100755"
    assert by_path["extra-source.txt"]["operation"] == "A"
    assert by_path["rename-old.txt"]["operation"] == "D"
    assert by_path["rename-old.txt"]["origin"] == "FROZEN_SOURCE"
    deleted_fragments = [
        row
        for row in operations
        if row["generator_id"] == "compatibility-authority" and row["operation"] == "D"
    ]
    assert deleted_fragments, "actual obsolete M fragments must be deleted only in the private view"
    assert all(
        (root / row["path"]).read_bytes() == before[0][row["path"]] for row in deleted_fragments
    )
    assert _stage_snapshot(root) == before
    assert IntegrationPublicationFence(project_root=root).replay(transaction).phase == (
        "GENERATED_REBUILD_PRE"
    )


@pytest.mark.parametrize("canonical_merge_repository", ["source-candidate"], indirect=True)
@pytest.mark.parametrize("damage", ["prepared-object", "source-mode"])
def test_render_source_candidate_delta_rejects_binding_and_source_mode_drift(
    source_candidate_render_repository, damage: str
) -> None:
    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_integration import (
        render_source_candidate_delta,
    )

    root, _scope, transaction, prepared, _source_bytes = source_candidate_render_repository
    if damage == "prepared-object":
        prepared = json.loads(json.dumps(prepared))
        prepared["expected_inputs"]["mode.txt"]["oid"] = "0" * 40
        code = "SOURCE_REQUEST_INPUTS_CHANGED"
    else:
        _git(root, "update-index", "--chmod=-x", "--", "mode.txt")
        code = "REVIEW_WORKING_RESULT_CHANGED"
    before = _stage_snapshot(root)
    with pytest.raises(WorkflowContractError, match=code):
        render_source_candidate_delta(
            root, prepared, transaction_path=transaction, actor="integration-coordinator"
        )
    assert _stage_snapshot(root) == before


@pytest.mark.parametrize("canonical_merge_repository", ["source-job"], indirect=True)
def test_x01_public_source_rejects_rehashed_canonical_history(canonical_merge_repository):
    from test_devx015_workflow_integration import (
        TASK,
        _freeze_candidate_fixture,
        _rewrite_registered_history_with_current_hashes,
    )

    from ai_trading_system.platform.architecture import task_registry_canonical as canonical
    from ai_trading_system.platform.architecture import workflow_contract as contract
    from ai_trading_system.platform.architecture import workflow_integration as integration
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )

    root, _scope = canonical_merge_repository
    transaction = _freeze_candidate_fixture(root)
    original_authority = contract.load_current_task_authority(root, TASK)
    prepared = integration.prepare_source_generation(
        root, TASK, transaction_path=transaction, actor="integration-coordinator",
    )
    assert prepared["task_id"] == TASK
    fence = IntegrationPublicationFence(project_root=root)
    lease_id = fence.replay(transaction).transaction["lease_id"]
    original_heads = tuple(fence.guard.replay().head_event_ids)
    original_refs = _git(root, "for-each-ref", "--format=%(refname) %(objectname)")
    original = _stage_snapshot(root)
    _rewrite_registered_history_with_current_hashes(root)
    canonical.validate_canonical_registry(project_root=root)
    current = contract.load_current_task_authority(root, TASK)
    assert current["authority"] == original_authority["authority"]
    assert current["scope"] == original_authority["scope"]
    damaged = _stage_snapshot(root)
    evidence = root.parent / "x01-rehashed-history"
    evidence.mkdir()
    changed = [name for name in original[0] if original[0][name] != damaged[0].get(name)]
    assert any(name.startswith("registry/development_tasks/") for name in changed)
    for version, snapshot in (("original", original), ("replacement", damaged)):
        for name in changed:
            target = evidence / version / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(snapshot[0][name])
    request_id = "source-history-" + uuid.uuid4().hex
    run = root / "outputs/architecture/workflow_integration/source_candidates" / request_id
    environment = {key: value for key, value in os.environ.items()
                   if not key.upper().startswith("GIT_")}
    environment.update(
        PYTHONPATH=str(root / "src"), PYTHONDONTWRITEBYTECODE="1",
        GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull, GIT_OPTIONAL_LOCKS="0",
    )
    result = subprocess.run([
        sys.executable, str(root / "scripts/architecture_arch005_workflow.py"),
        "source-candidate", "--task-id", TASK, "--publication-transaction", str(transaction),
        "--actor", "integration-coordinator", "--request-id", request_id,
    ], cwd=root, env=environment, capture_output=True, timeout=LOADED_HOST_CLI_TIMEOUT_SECONDS)
    (evidence / "cli.stdout").write_bytes(result.stdout)
    (evidence / "cli.stderr").write_bytes(result.stderr)
    (evidence / "observation.json").write_text(json.dumps({
        "exit_code": result.returncode, "expected_reason": "CANONICAL_EVENT_HISTORY_CHANGED",
        "request_id": request_id, "changed_paths": changed,
    }), encoding="utf-8")
    assert result.returncode == 1, result.stdout + result.stderr
    assert b"CANONICAL_EVENT_HISTORY_CHANGED" in result.stderr, result.stderr
    assert _stage_snapshot(root) == damaged
    assert tuple(fence.guard.replay().head_event_ids) == original_heads
    assert _git(root, "for-each-ref", "--format=%(refname) %(objectname)") == original_refs
    head = next(row for row in fence.guard.replay().lease_heads if row.lease_id == lease_id)
    assert head.execution is None
    assert not run.exists()
    NativeOracle().assert_job_absent("Local\\AITS-DEVX015-" + request_id)
    fence.release(transaction, actor="integration-coordinator", outcome="failed")
    assert fence.replay(transaction).phase == "FAILED"
    assert not fence.guard.replay().active_leases


@pytest.mark.parametrize("canonical_merge_repository", ["source-job"], indirect=True)
def test_source_candidate_cli_uses_real_job_and_private_two_parent_commit(
    canonical_merge_repository,
    installation_boundary="normal",
    handoff_boundary="normal",
    recovery_adversary=None,
    request_replays=False,
    generation_adversaries=False,
) -> None:
    from test_devx015_workflow_integration import TASK, _freeze_candidate_fixture

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )

    root, scope = canonical_merge_repository
    raw = b"reviewed CLI source\0raw\r\n"
    added = b"reviewed CLI addition\r\n"
    (root / "mode.txt").write_bytes(raw)
    (root / "extra-source.txt").write_bytes(added)
    (root / "rename-old.txt").unlink()
    _git(root, "update-index", "--chmod=+x", "--", "mode.txt")
    flow = root / "docs/system_flow.md"
    flow.write_bytes(flow.read_bytes() + b"Real source candidate Job fixture block.\n\n")
    transaction = _freeze_candidate_fixture(root)
    request_id = "source-job-" + uuid.uuid4().hex
    run = root / "outputs/architecture/workflow_integration/source_candidates" / request_id
    command = [
        sys.executable,
        str(root / "scripts/architecture_arch005_workflow.py"),
        "source-candidate",
        "--task-id",
        TASK,
        "--publication-transaction",
        str(transaction),
        "--actor",
        "integration-coordinator",
        "--request-id",
        request_id,
    ]
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment.update(
        PYTHONPATH=str(root / "src"),
        PYTHONDONTWRITEBYTECODE="1",
        GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_OPTIONAL_LOCKS="0",
    )
    before = _stage_snapshot(root)
    before_source = {
        name: content for name, content in before[0].items() if not name.startswith("outputs/")
    }
    before_refs = _git(root, "for-each-ref", "--format=%(refname) %(objectname)")
    fence = IntegrationPublicationFence(project_root=root)
    lease_id = fence.replay(transaction).transaction["lease_id"]
    cli_log = root.parent / "source-job-cli.stdout.log"

    def diagnostics() -> str:
        text = cli_log.read_text(encoding="utf-8", errors="replace")
        worker_log = run / "worker.stdout.log"
        if worker_log.exists():
            text += "\nworker:\n" + worker_log.read_text(encoding="utf-8", errors="replace")
        return text

    with cli_log.open("wb") as output:
        launcher = subprocess.Popen(
            command,
            cwd=root,
            env=environment,
            stdout=output,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        try:
            request = _until(
                lambda: (
                    _read_json(run / "execution_request.json")
                    or ({"launcher_exited": True} if launcher.poll() is not None else None)
                ),
                description="real source CLI request or actual failure",
                timeout=1800,
            )
            assert "launcher_exited" not in request, diagnostics()
            assert request["cwd"] == root.as_posix()
            assert request["argv"][1] == str(root / "scripts/architecture_arch005_workflow.py")

            def contained_execution():
                replay = fence.guard.store.replay()
                if replay.status == "PASS":
                    head = next(item for item in replay.lease_heads if item.lease_id == lease_id)
                    if head.execution is not None and head.execution["state"] in {
                        "RESUME_INTENT",
                        "RUNNING",
                    }:
                        return head.execution
                return {"launcher_exited": True} if launcher.poll() is not None else None

            execution = _until(
                contained_execution,
                description="actual contained source process",
                timeout=1800,
            )
            assert "launcher_exited" not in execution, diagnostics()
            oracle = NativeOracle()
            with oracle.process(execution["process"]["pid"]) as process:
                assert oracle.creation_time(process) == execution["process"]["creation_time"]
                oracle.assert_in_job(process, request["job_name"])
                if request_replays:
                    assert not oracle.exited(process), "R01 needs actual live contention"
                    duplicate = subprocess.run(
                        command, cwd=root, env=environment, capture_output=True,
                        timeout=LOADED_HOST_CLI_TIMEOUT_SECONDS,
                    )
                    (root.parent / "r01-live-replay.stdout").write_bytes(duplicate.stdout)
                    (root.parent / "r01-live-replay.stderr").write_bytes(duplicate.stderr)
                    assert duplicate.returncode == 0, duplicate.stdout + duplicate.stderr
                    observed = json.loads(duplicate.stdout)
                    assert observed["status"] == "REPLAY_ONLY"
                    assert observed["dispatch_allowed"] is False
                    assert observed["execution"]["status"] == "OBSERVE_ONLY"
                    assert not oracle.exited(process), "R01 observation did not overlap executor"
                    live = next(item for item in fence.guard.store.replay().lease_heads
                                if item.lease_id == lease_id)
                    assert live.execution["process"] == execution["process"]
                    assert live.execution["request"] == request
                assert launcher.wait(timeout=1800) == 0, diagnostics()
                assert oracle.exited(process)
            oracle.assert_job_absent(request["job_name"])
        finally:
            if launcher.poll() is None:
                launcher.kill()
                launcher.wait(timeout=DEADLINE)
    result = json.loads(cli_log.read_text(encoding="utf-8"))
    assert result["status"] == "PASS"
    assert result["schema_version"] == "controlled_source_candidate_worker_result.v1"
    assert result["installation_performed"] is False
    assert result["publication_performed"] is False
    assert result["worker_process"]["pid"] != launcher.pid
    snapshot = result["snapshot"]
    commit = snapshot["commit"]
    assert _git(root, "rev-list", "--parents", "-n", "1", commit).split() == [
        commit,
        scope["latest_main"],
        scope["lane_head"],
    ]
    assert snapshot["parents"] == [scope["latest_main"], scope["lane_head"]]
    assert _git(root, "rev-parse", commit + "^{tree}") == snapshot["tree"]

    def tree_entries(identity: str):
        entries = {}
        for row in _git(root, "ls-tree", "-r", "--full-tree", identity).splitlines():
            metadata, path = row.split("\t", 1)
            entries[path] = tuple(metadata.split())
        return entries

    expected_tree = tree_entries(scope["latest_main"])
    generation_bytes = (run / "generation.json").read_bytes()
    assert hashlib.sha256(generation_bytes).hexdigest() == result["generation_sha256"]
    delta = json.loads(generation_bytes)
    for operation in delta["operations"]:
        after = operation["after"]
        if after["exists"]:
            expected_tree[operation["path"]] = (after["mode"], after["type"], after["oid"])
        else:
            expected_tree.pop(operation["path"], None)
    actual_tree = tree_entries(commit)
    assert actual_tree == expected_tree
    assert actual_tree["mode.txt"][0] == "100755"
    assert "rename-old.txt" not in actual_tree
    for name, content in (("mode.txt", raw), ("extra-source.txt", added)):
        actual = subprocess.run(
            ["git", "-C", str(root), "cat-file", "blob", actual_tree[name][2]],
            capture_output=True,
            env=environment,
            timeout=30,
        )
        assert actual.returncode == 0
        assert actual.stdout == content
    final = next(
        item for item in fence.guard.store.replay().lease_heads if item.lease_id == lease_id
    )
    assert final.execution["state"] == "RESULT_RECORDED"
    assert final.execution["result"]["status"] == "PASS"
    assert final.execution["exit"]["returncode"] == 0
    assert final.execution["exit"]["job_state"] == "EMPTY"
    assert final.execution["request"] == request
    assert (
        final.execution["result"]["artifact"]["sha256"]
        == hashlib.sha256((run / "worker_result.json").read_bytes()).hexdigest()
    )
    after = _stage_snapshot(root)
    assert {
        name: content for name, content in after[0].items() if not name.startswith("outputs/")
    } == before_source
    assert after[1:] == before[1:]
    assert _git(root, "for-each-ref", "--format=%(refname) %(objectname)") == before_refs
    run_before_replay = {
        path.relative_to(run).as_posix(): path.read_bytes()
        for path in run.rglob("*")
        if path.is_file()
    }
    heads_before_replay = tuple(fence.guard.store.replay().head_event_ids)
    repeated = subprocess.run(command, cwd=root, env=environment, capture_output=True, timeout=1800)
    assert repeated.returncode == 0, repeated.stdout.decode(
        errors="replace"
    ) + repeated.stderr.decode(errors="replace")
    replay_result = json.loads(repeated.stdout)
    assert replay_result["status"] == "REPLAY_ONLY"
    assert replay_result["dispatch_allowed"] is False
    assert tuple(fence.guard.store.replay().head_event_ids) == heads_before_replay
    assert {
        path.relative_to(run).as_posix(): path.read_bytes()
        for path in run.rglob("*")
        if path.is_file()
    } == run_before_replay
    oracle.assert_job_absent(request["job_name"])

    if request_replays:
        execution_path = run / "execution_request.json"
        original_request = execution_path.read_bytes()
        alternatives = {
            "argv": [*request["argv"], "--unrequested-option"],
            "environment_sha256": "a" * 64,
            "review_sha256": "b" * 64,
            "source_head_sha": "c" * 40,
        }
        for field, value in alternatives.items():
            changed = {**request, field: value}
            assert changed != request
            try:
                execution_path.write_text(json.dumps(changed), encoding="utf-8")
                refused = subprocess.run(
                    command, cwd=root, env=environment, capture_output=True,
                    timeout=LOADED_HOST_CLI_TIMEOUT_SECONDS,
                )
                (root.parent / ("r01-payload-" + field + ".json")).write_text(
                    json.dumps({"request": changed, "exit_code": refused.returncode,
                                "stdout": refused.stdout.decode(errors="replace"),
                                "stderr": refused.stderr.decode(errors="replace")}),
                    encoding="utf-8",
                )
                # This workflow CLI propagates WorkflowContractError (exit 1),
                # unlike publication_fence's JSON error adapter (exit 2).
                assert refused.returncode == 1, refused.stdout + refused.stderr
                assert b"WORKFLOW_MERGE_SOURCE_REQUEST_BINDING" in refused.stderr
                assert tuple(fence.guard.store.replay().head_event_ids) == heads_before_replay
            finally:
                execution_path.write_bytes(original_request)
        assert {
            path.relative_to(run).as_posix(): path.read_bytes()
            for path in run.rglob("*") if path.is_file()
        } == run_before_replay
        assert _git(root, "for-each-ref", "--format=%(refname) %(objectname)") == before_refs
        assert _stage_snapshot(root)[1:] == before[1:]
        oracle.assert_job_absent(request["job_name"])

    # Actual adopted source, not a synthetic self-attested installation plan.
    from ai_trading_system.platform.architecture.workflow_integration import (
        _prepare_source_installation,
    )

    before_plan = _stage_snapshot(root)
    plan = _prepare_source_installation(
        root,
        TASK,
        transaction_path=transaction,
        actor="integration-coordinator",
        request_id=request_id,
    )
    assert plan["candidate"] == commit and plan["tree"] == snapshot["tree"]
    assert plan["source_result_sha256"] == final.execution["result"]["artifact"]["sha256"]
    assert plan["branch"] == _git(root, "symbolic-ref", "HEAD")
    index_path = Path(_git(root, "rev-parse", "--path-format=absolute", "--git-path", "index"))
    assert bytes.fromhex(plan["index"]["before_hex"]) == index_path.read_bytes()
    assert plan["index"]["identity"] == [index_path.stat().st_dev, index_path.stat().st_ino]
    assert Path(plan["gitdir_identity"]["path"]) == index_path.parent
    changed = {}
    for row in plan["files"]:
        path = root / row["path"]
        original = path.read_bytes() if path.exists() else None
        assert row["before_hex"] == (None if original is None else original.hex())
        if original is not None:
            assert row["identity"] == [path.stat().st_dev, path.stat().st_ino]
        if row["after_hex"] is None:
            assert row["path"] not in actual_tree
        else:
            content = bytes.fromhex(row["after_hex"])
            oid = hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest()
            assert oid == actual_tree[row["path"]][2]
        changed[row["path"]] = row
    assert changed and "mode.txt" not in changed and "extra-source.txt" not in changed
    after_plan = _stage_snapshot(root)
    assert after_plan == before_plan, {
        "changed_paths": [
            name
            for name in sorted(set(before_plan[0]) | set(after_plan[0]))
            if before_plan[0].get(name) != after_plan[0].get(name)
        ],
        "head_index_equal": before_plan[1:] == after_plan[1:],
    }
    assert tuple(fence.guard.store.replay().head_event_ids) == heads_before_replay
    assert _git(root, "for-each-ref", "--format=%(refname) %(objectname)") == before_refs

    expected_created_paths = {
        (root / name).as_posix() for name in actual_tree if not (root / name).exists()
    }
    actual_branch_ref = Path(
        _git(root, "rev-parse", "--path-format=absolute", "--git-path", plan["branch"])
    )
    if not actual_branch_ref.exists():
        expected_created_paths.add(actual_branch_ref.as_posix())
    expected_created_directories = set()
    for name in expected_created_paths:
        parent = Path(name).parent
        while not parent.exists():
            expected_created_directories.add(parent.as_posix())
            parent = parent.parent
    install_id = "install-job-" + uuid.uuid4().hex
    install_run = run / "installations" / install_id
    install_command = [
        sys.executable,
        str(root / "scripts/architecture_arch005_workflow.py"),
        "source-install",
        "--task-id",
        TASK,
        "--publication-transaction",
        str(transaction),
        "--actor",
        "integration-coordinator",
        "--source-request-id",
        request_id,
        "--request-id",
        install_id,
    ]
    if generation_adversaries:
        # Use the original successful four-generator Job and private S, not a
        # fabricated manifest or a substitute installation authority.
        position, operation = next(
            (position, row) for position, row in enumerate(delta["operations"])
            if row["generator_id"] == "canonical-task-source"
            and row["before"]["exists"] and row["after"]["exists"]
            and row["before"]["oid"] != row["after"]["oid"]
        )
        old_output = subprocess.run(
            ["git", "cat-file", "blob", operation["before"]["oid"]], cwd=root,
            env=environment, capture_output=True, timeout=30, check=True,
        ).stdout
        capture = run / "capture" / f"{position:06d}.bin"
        assert capture.read_bytes() != old_output
        altered_generation = json.loads(generation_bytes)
        assert len(altered_generation["generation"]["generator_order"]) == 4
        altered_generation["generation"]["generator_order"].reverse()
        faults = (
            ("stale-output", capture, old_output, b"SOURCE_CAPTURE_CHANGED"),
            ("manifest-only", run / "generation.json",
             json.dumps(altered_generation).encode(), b"REFERENCE_DRIFT"),
        )
        if generation_adversaries == "self-reference":
            from ai_trading_system.platform.architecture.workflow_contract import canonical_digest

            result_path = run / "worker_result.json"
            attested = json.loads(result_path.read_bytes())
            assert attested["status"] == "PASS" and attested["snapshot"]["commit"] == commit
            # The self-attestation is internally consistent, but cannot replace
            # the original lease's external result digest. The false commit is
            # a real Git object, not a malformed or absent-object shortcut.
            attested["snapshot"]["commit"] = scope["latest_main"]
            assert attested["snapshot"]["commit"] != commit
            attested["artifact"] = {
                "path": result_path.relative_to(root).as_posix(),
                "sha256": canonical_digest(attested),
            }
            assert attested["artifact"]["sha256"] == canonical_digest({
                key: value for key, value in attested.items() if key != "artifact"
            })
            replacement = json.dumps(attested).encode()
            assert hashlib.sha256(replacement).hexdigest() != (
                final.execution["result"]["artifact"]["sha256"]
            )
            faults = (("self-reference", result_path, replacement, b"REFERENCE_DRIFT"),)
        for label, target, replacement, reason in faults:
            original = target.read_bytes()
            assert original != replacement
            evidence = root.parent / ("x01-" + label)
            evidence.mkdir()
            (evidence / "original.bin").write_bytes(original)
            (evidence / "replacement.bin").write_bytes(replacement)
            target.write_bytes(replacement)
            damaged = _stage_snapshot(root)
            try:
                refused = subprocess.run(
                    install_command, cwd=root, env=environment, capture_output=True,
                    timeout=LOADED_HOST_CLI_TIMEOUT_SECONDS,
                )
                (evidence / "cli.stdout").write_bytes(refused.stdout)
                (evidence / "cli.stderr").write_bytes(refused.stderr)
                (evidence / "observation.json").write_text(json.dumps({
                    "exit_code": refused.returncode, "source_candidate": commit,
                    "target": str(target), "source_operation": operation["path"],
                    "expected_reason": reason.decode(),
                }), encoding="utf-8")
                assert refused.returncode == 1, refused.stdout + refused.stderr
                assert reason in refused.stderr, refused.stderr
                assert _stage_snapshot(root) == damaged
                assert tuple(fence.guard.store.replay().head_event_ids) == heads_before_replay
                assert not install_run.exists()
                assert (
                    _git(root, "for-each-ref", "--format=%(refname) %(objectname)") == before_refs
                )
                oracle.assert_job_absent("Local\\AITS-DEVX015-" + install_id)
            finally:
                # Restore only this test's mutation. Both byte versions and
                # original CLI evidence remain outside the candidate checkout.
                target.write_bytes(original)
            assert {
                path.relative_to(run).as_posix(): path.read_bytes()
                for path in run.rglob("*") if path.is_file()
            } == run_before_replay
        # Continue the same original public installation and final handoff below.
    if installation_boundary.startswith("created-"):
        from test_devx015_workflow_acceptance import recover_creation_crash

        recover_creation_crash(
            root,
            install_command,
            environment,
            fence,
            lease_id,
            installation_boundary.removeprefix("created-"),
            before,
            before_source,
            before_refs,
            expected_created_paths,
            expected_created_directories,
            adversary=recovery_adversary,
        )
        return
    if installation_boundary == "partial-plan":
        fault_program = """
import os, sys
from pathlib import Path
from ai_trading_system.platform.architecture import workflow_integration as integration
root, transaction = map(Path, sys.argv[1:3])
original = integration.write_bound_once
def interrupted(bound_root, relative, content, **kwargs):
    if relative.endswith('/installation_plan.json'):
        original(bound_root, relative, content[:17], **kwargs)
        os._exit(29)
    return original(bound_root, relative, content, **kwargs)
integration.write_bound_once = interrupted
integration.start_source_installation(root, sys.argv[3], transaction_path=transaction,
    actor='integration-coordinator', source_request_id=sys.argv[4], request_id=sys.argv[5])
"""
        crashed = subprocess.run(
            [
                sys.executable,
                "-c",
                fault_program,
                str(root),
                str(transaction),
                TASK,
                request_id,
                install_id,
            ],
            cwd=root,
            env=environment,
            capture_output=True,
            timeout=LOADED_HOST_CLI_TIMEOUT_SECONDS,
        )
        assert crashed.returncode == 29, crashed.stderr.decode(errors="replace")
        partial = (run / "installation_plan.json").read_bytes()
        assert len(partial) == 17
        interrupted_lease = next(
            item for item in fence.guard.store.replay().lease_heads if item.lease_id == lease_id
        )
        original_attempt = interrupted_lease.execution["installation_attempts"][0]
        assert original_attempt["state"] == "RESERVED" and original_attempt["process"] is None
        oracle.assert_job_absent(original_attempt["request"]["job_name"])
        install_command[2] = "source-install-recover"
        install_command[-1] = "recover-plan-" + uuid.uuid4().hex
        environment = {**environment, "DEVX015_RECOVERY_SESSION": "fresh-public-process"}
        recovery_fault = """
import os, sys
from pathlib import Path
from ai_trading_system.platform.architecture import workflow_integration as integration
root, transaction = map(Path, sys.argv[1:3])
original = integration.write_bound_once
def interrupted(bound_root, relative, content, **kwargs):
    selected = (relative.endswith('/installation_plan.recovered.json') if sys.argv[6] == 'plan'
                else '/installation_plan.recovered.partial-' in relative)
    if selected:
        original(bound_root, relative, content[:7 if sys.argv[6] == 'plan' else 3], **kwargs)
        os._exit(31)
    return original(bound_root, relative, content, **kwargs)
integration.write_bound_once = interrupted
integration.start_source_installation(root, sys.argv[3], transaction_path=transaction,
    actor='integration-coordinator', source_request_id=sys.argv[4], request_id=sys.argv[5],
    action='RECOVER')
"""
        for recovery_boundary in ("plan", "retention"):
            interrupted_recovery = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    recovery_fault,
                    str(root),
                    str(transaction),
                    TASK,
                    request_id,
                    install_command[-1],
                    recovery_boundary,
                ],
                cwd=root,
                env=environment,
                capture_output=True,
                timeout=LOADED_HOST_CLI_TIMEOUT_SECONDS,
            )
            assert interrupted_recovery.returncode == 31, interrupted_recovery.stderr.decode(
                errors="replace"
            )
            assert (run / "installation_plan.json").read_bytes() == partial
        recovery_partial = (run / "installation_plan.recovered.json").read_bytes()
        retained = run / (
            "installation_plan.recovered.partial-"
            + hashlib.sha256(recovery_partial).hexdigest()
            + ".json"
        )
        assert len(recovery_partial) == 7
        # Force a genuinely shorter retention prefix than its seven-byte source.
        # The injected native write below already captures this boundary.
        assert retained.read_bytes() == recovery_partial[:3]
        recovered = subprocess.run(
            install_command,
            cwd=root,
            env=environment,
            capture_output=True,
            timeout=1800,
        )
        recovery_log = run / "installations" / install_command[-1] / "worker.stdout.log"
        assert recovered.returncode == 0, recovered.stderr.decode(errors="replace") + (
            recovery_log.read_text(errors="replace") if recovery_log.exists() else ""
        )
        assert json.loads(recovered.stdout)["stable_state"] == "SOURCE_RESTORED"
        assert (run / "installation_plan.json").read_bytes() == partial
        assert retained.read_bytes() == recovery_partial
        restored_plan = (run / "installation_plan.recovered.json").read_bytes()
        assert (
            hashlib.sha256(restored_plan).hexdigest()
            == original_attempt["request"]["installation_plan_sha256"]
        )
        assert json.loads(restored_plan) == plan
        restored = _stage_snapshot(root)
        assert {
            name: content
            for name, content in restored[0].items()
            if not name.startswith("outputs/")
        } == before_source
        assert restored[1:] == before[1:]
        assert _git(root, "for-each-ref", "--format=%(refname) %(objectname)") == before_refs
        final_recovery = next(
            item for item in fence.guard.store.replay().lease_heads if item.lease_id == lease_id
        )
        history = final_recovery.execution["installation_attempts"]
        assert len(history) == 2
        assert history[0]["result"]["status"] == "INSUFFICIENT"
        assert history[1]["result"]["status"] == "PASS"
        assert history[1]["request"]["installation_action"] == "RECOVER"
        assert (
            history[1]["request"]["environment_sha256"]
            != history[0]["request"]["environment_sha256"]
        )
        assert (
            history[1]["request"]["environment_sha256"]
            == hashlib.sha256(
                json.dumps(
                    environment, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                ).encode()
            ).hexdigest()
        )
        before_replay = tuple(fence.guard.store.replay().head_event_ids)
        repeated = subprocess.run(
            install_command,
            cwd=root,
            env=environment,
            capture_output=True,
            timeout=LOADED_HOST_CLI_TIMEOUT_SECONDS,
        )
        assert repeated.returncode == 0, repeated.stderr.decode(errors="replace")
        assert json.loads(repeated.stdout)["dispatch_allowed"] is False
        assert tuple(fence.guard.store.replay().head_event_ids) == before_replay
        return
    installed = subprocess.run(
        install_command,
        cwd=root,
        env=environment,
        capture_output=True,
        timeout=1800,
    )
    worker_log = install_run / "worker.stdout.log"
    assert installed.returncode == 0, (
        installed.stdout.decode(errors="replace")
        + installed.stderr.decode(errors="replace")
        + (worker_log.read_text(encoding="utf-8", errors="replace") if worker_log.exists() else "")
    )
    installed_result = json.loads(installed.stdout)
    assert installed_result["stable_state"] == "SOURCE_INSTALLED"
    assert installed_result["publication_performed"] is False
    assert _git(root, "rev-parse", "HEAD") == commit
    assert _git(root, "rev-parse", "refs/heads/main") == scope["latest_main"]
    assert index_path.read_bytes() == bytes.fromhex(plan["index"]["after_hex"])
    actual_index = {}
    for entry in _git(root, "ls-files", "--stage").splitlines():
        metadata, name = entry.split("\t", 1)
        mode, oid, stage = metadata.split()
        assert stage == "0"
        actual_index[name] = (mode, "blob", oid)
    assert actual_index == actual_tree
    for row in plan["files"]:
        path = root / row["path"]
        if row["after_hex"] is None:
            assert not path.exists()
        else:
            assert path.read_bytes() == bytes.fromhex(row["after_hex"])
    installed_lease = next(
        item for item in fence.guard.store.replay().lease_heads if item.lease_id == lease_id
    )
    assert installed_lease.state == "ACTIVE"
    assert installed_lease.execution["request"] == request
    attempts = installed_lease.execution["installation_attempts"]
    assert len(attempts) == 1
    assert attempts[0]["state"] == "RESULT_RECORDED"
    assert attempts[0]["result"]["status"] == "PASS"
    assert attempts[0]["result"]["reason"] == "INDEPENDENT_SOURCE_INSTALLATION_VERIFIED"
    assert attempts[0]["schema_version"] == "lease_execution.v3"
    directory_creations = [
        item for item in attempts[0]["created_objects"] if item.get("kind") == "directory"
    ]
    assert {
        (Path(item["root"]) / item["path"]).as_posix() for item in directory_creations
    } == expected_created_directories
    assert directory_creations, "fixture must exercise actual missing parent creation"
    for created in directory_creations:
        actual = (Path(created["root"]) / created["path"]).stat()
        assert created["file_identity"] == [actual.st_dev, actual.st_ino]
        assert "target_sha256" not in created
        assert created["plan_sha256"] == attempts[0]["request"]["installation_plan_sha256"]
    creations = [item for item in attempts[0]["created_objects"] if "kind" not in item]
    assert (
        creations
        and {(Path(item["root"]) / item["path"]).as_posix() for item in creations}
        == expected_created_paths
    )
    for created in creations:
        created_path = Path(created["root"]) / created["path"]
        info, base_info = created_path.stat(), Path(created["root"]).stat()
        assert created["file_identity"] == [info.st_dev, info.st_ino]
        assert created["root_identity"] == [base_info.st_dev, base_info.st_ino]
        assert created["target_sha256"] == hashlib.sha256(created_path.read_bytes()).hexdigest()
        assert created["plan_sha256"] == attempts[0]["request"]["installation_plan_sha256"]
    oracle.assert_job_absent(attempts[0]["request"]["job_name"])
    gitdir = Path(plan["gitdir_identity"]["path"])
    common = Path(plan["common_identity"]["path"])
    assert all(
        not path.exists()
        for path in (
            gitdir / "HEAD.lock",
            gitdir / "index.lock",
            common / (plan["branch"] + ".lock"),
            common / "refs/heads/main.lock",
            common / "packed-refs.lock",
        )
    )
    installed_heads = tuple(fence.guard.store.replay().head_event_ids)
    repeated_install = subprocess.run(
        install_command,
        cwd=root,
        env=environment,
        capture_output=True,
        timeout=LOADED_HOST_CLI_TIMEOUT_SECONDS,
    )
    assert repeated_install.returncode == 0, repeated_install.stderr.decode(errors="replace")
    assert json.loads(repeated_install.stdout)["dispatch_allowed"] is False
    assert tuple(fence.guard.store.replay().head_event_ids) == installed_heads
    handoff_command = [
        "source-final-handoff" if part == "source-install" else part for part in install_command
    ]
    wrong_handoff = subprocess.run(
        ["install-wrong-request" if part == install_id else part for part in handoff_command],
        cwd=root,
        env=environment,
        capture_output=True,
        timeout=LOADED_HOST_CLI_TIMEOUT_SECONDS,
    )
    assert wrong_handoff.returncode != 0
    assert b"SOURCE_HANDOFF_INDEPENDENT_INSTALL_REQUIRED" in wrong_handoff.stderr
    assert tuple(fence.guard.store.replay().head_event_ids) == installed_heads
    if handoff_boundary in {"release", "later-phase"}:
        from datetime import UTC, datetime

        from ai_trading_system.platform.architecture import workflow_integration as integration

        arguments = {
            "transaction_path": transaction,
            "actor": "integration-coordinator",
            "source_request_id": request_id,
            "request_id": install_id,
        }

        def current_handoff_path():
            event_id = dict(fence.guard.store.replay().head_event_ids)[lease_id]
            return run / ("source_final_handoff." + event_id + ".json")

        handoff_path = current_handoff_path()
        real_audit = IntegrationPublicationFence._require_clean_candidate

        def heartbeat_after_observation(selected):
            real_audit(selected)
            selected.guard.store.heartbeat(
                lease_id,
                actor="integration-coordinator",
                now=datetime.now(UTC),
            )

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                IntegrationPublicationFence, "_require_clean_candidate", heartbeat_after_observation
            )
            with pytest.raises(
                integration.WorkflowContractError, match="SOURCE_HANDOFF_LEASE_CHANGED"
            ):
                integration.finish_source_installation(root, TASK, **arguments)
        assert not handoff_path.exists()
        assert index_path.read_bytes() == bytes.fromhex(plan["index"]["after_hex"])
        handoff_path = current_handoff_path()
        real_write = integration.write_bound_once

        def interrupt_after_evidence(base, relative, content, **kwargs):
            real_write(base, relative, content, **kwargs)
            if base / relative == handoff_path:
                raise RuntimeError("handoff evidence durable before release")

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(integration, "write_bound_once", interrupt_after_evidence)
            with pytest.raises(RuntimeError, match="handoff evidence durable before release"):
                integration.finish_source_installation(root, TASK, **arguments)
        preserved_handoff = handoff_path.read_bytes()
        assert fence.replay(transaction).phase == "CANDIDATE_COMMIT_PRE"
        if handoff_boundary == "later-phase":
            fence.checkpoint(
                transaction,
                phase="FORMAL_VALIDATION_PRE",
                actor="integration-coordinator",
            )
            fence.release(
                transaction,
                actor="integration-coordinator",
                outcome="failed",
                evidence_paths=[handoff_path],
            )
            terminal_before = fence.replay(transaction)
            lease_heads_before = tuple(fence.guard.store.replay().head_event_ids)
            denied = subprocess.run(
                handoff_command,
                cwd=root,
                env=environment,
                capture_output=True,
                timeout=LOADED_HOST_CLI_TIMEOUT_SECONDS,
            )
            assert denied.returncode != 0
            assert b"SOURCE_HANDOFF_TERMINAL_PREDECESSOR" in denied.stderr
            assert fence.replay(transaction).events == terminal_before.events
            assert tuple(fence.guard.store.replay().head_event_ids) == lease_heads_before
            assert handoff_path.read_bytes() == preserved_handoff
            return

        def release_after_observation(selected):
            real_audit(selected)
            selected.guard.release(
                lease_id,
                actor="integration-coordinator",
                outcome="failed",
                evidence_refs=[handoff_path.relative_to(root).as_posix()],
            )

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                IntegrationPublicationFence, "_require_clean_candidate", release_after_observation
            )
            with pytest.raises(
                integration.WorkflowContractError, match="SOURCE_HANDOFF_LEASE_CHANGED"
            ):
                integration.finish_source_installation(root, TASK, **arguments)
        assert fence.replay(transaction).phase == "CANDIDATE_COMMIT_PRE"
        assert handoff_path.read_bytes() == preserved_handoff
        assert (
            next(
                row for row in fence.guard.store.replay().lease_heads if row.lease_id == lease_id
            ).state
            == "RELEASED"
        )
        # Historical terminal recovery must not require or modify the old
        # checkout state after its writer boundary has ended.
        (root / "mode.txt").write_bytes(b"later checkout bytes preserved\n")
    elif handoff_boundary == "evidence":
        from datetime import UTC, datetime

        program = """
import os, sys
from pathlib import Path
from ai_trading_system.platform.architecture import workflow_integration as integration
root, transaction = Path(sys.argv[1]), Path(sys.argv[2])
mode = sys.argv[5]
original = integration.write_bound_once
def crash_write(base, relative, content, **kwargs):
    name = Path(relative).name
    if name.startswith('source_final_handoff.'):
        retained = '.partial-' in name
        if mode == 'complete' and not retained:
            original(base, relative, content, **kwargs)
            os._exit(41)
        if mode == 'marker' and not retained:
            original(base, relative, content[:17], **kwargs)
            os._exit(42)
        if mode == 'retained' and retained:
            original(base, relative, content[:3], **kwargs)
            os._exit(43)
    return original(base, relative, content, **kwargs)
integration.write_bound_once = crash_write
integration.finish_source_installation(root, 'DEVX-015-MERGE-FIXTURE',
    transaction_path=transaction, actor='integration-coordinator',
    source_request_id=sys.argv[3], request_id=sys.argv[4])
"""

        def evidence_crash(mode, exit_code):
            before_events = tuple(fence.guard.store.replay().head_event_ids)
            crashed = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    program,
                    str(root),
                    str(transaction),
                    request_id,
                    install_id,
                    mode,
                ],
                cwd=root,
                env=environment,
                capture_output=True,
                timeout=LOADED_HOST_CLI_TIMEOUT_SECONDS,
            )
            assert crashed.returncode == exit_code, crashed.stderr.decode(errors="replace")
            assert tuple(fence.guard.store.replay().head_event_ids) == before_events

        old_head = dict(fence.guard.store.replay().head_event_ids)[lease_id]
        old_marker = run / ("source_final_handoff." + old_head + ".json")
        evidence_crash("complete", 41)
        old_bytes = old_marker.read_bytes()
        assert json.loads(old_bytes)["observed_lease_head_event_id"] == old_head
        fence.guard.store.heartbeat(
            lease_id, actor="integration-coordinator", now=datetime.now(UTC)
        )
        new_head = dict(fence.guard.store.replay().head_event_ids)[lease_id]
        assert new_head != old_head
        new_marker = run / ("source_final_handoff." + new_head + ".json")
        evidence_crash("marker", 42)
        partial_marker = new_marker.read_bytes()
        assert len(partial_marker) == 17
        retained_marker = new_marker.with_name(
            new_marker.stem + ".partial-" + hashlib.sha256(partial_marker).hexdigest() + ".json"
        )
        evidence_crash("retained", 43)
        assert retained_marker.read_bytes() == partial_marker[:3]
        assert new_marker.read_bytes() == partial_marker
        assert old_marker.read_bytes() == old_bytes
    handoff = subprocess.run(
        handoff_command,
        cwd=root,
        env=environment,
        capture_output=True,
        timeout=LOADED_HOST_CLI_TIMEOUT_SECONDS,
    )
    assert handoff.returncode == 0, handoff.stderr.decode(errors="replace")
    handed = json.loads(handoff.stdout)
    assert handed["source_handoff_status"] == "PASS"
    assert handed["publication_outcome"] == "FAILED"
    assert handed["formal_validation_status"] == "NOT_EXECUTED"
    assert handed["publication_performed"] is False
    assert handed["lease_released"] is True
    assert handed["source_candidate_sha"] == commit
    assert handed["expected_main_sha"] == scope["latest_main"]
    assert fence.replay(transaction).phase == "FAILED"
    handoff_heads = tuple(fence.guard.store.replay().head_event_ids)
    replay_handoff = subprocess.run(
        handoff_command,
        cwd=root,
        env=environment,
        capture_output=True,
        timeout=LOADED_HOST_CLI_TIMEOUT_SECONDS,
    )
    assert replay_handoff.returncode == 0, replay_handoff.stderr.decode(errors="replace")
    assert json.loads(replay_handoff.stdout) == handed
    assert tuple(fence.guard.store.replay().head_event_ids) == handoff_heads
    assert _git(root, "rev-parse", "HEAD") == commit
    assert _git(root, "rev-parse", "refs/heads/main") == scope["latest_main"]
    assert index_path.read_bytes() == bytes.fromhex(plan["index"]["after_hex"])
    if handoff_boundary == "release":
        assert (root / "mode.txt").read_bytes() == b"later checkout bytes preserved\n"
    elif handoff_boundary == "evidence":
        assert old_marker.read_bytes() == old_bytes
        assert retained_marker.read_bytes() == partial_marker
        assert json.loads(new_marker.read_bytes())["observed_lease_head_event_id"] == new_head
        assert handed["publication_receipt"]["evidence"] == [
            {
                "path": new_marker.relative_to(root).as_posix(),
                "sha256": hashlib.sha256(new_marker.read_bytes()).hexdigest(),
                "size_bytes": new_marker.stat().st_size,
            }
        ]


@pytest.mark.parametrize("canonical_merge_repository", ["source-job"], indirect=True)
def test_r01_original_source_job_replays_and_rejects_changed_payload(canonical_merge_repository):
    test_source_candidate_cli_uses_real_job_and_private_two_parent_commit(
        canonical_merge_repository, request_replays=True,
    )


@pytest.mark.parametrize("canonical_merge_repository", ["source-job"], indirect=True)
def test_x01_original_source_installation_rejects_stale_output_and_manifest(
    canonical_merge_repository,
):
    test_source_candidate_cli_uses_real_job_and_private_two_parent_commit(
        canonical_merge_repository, generation_adversaries=True,
    )


@pytest.mark.parametrize("canonical_merge_repository", ["source-job"], indirect=True)
def test_x01_original_source_installation_rejects_self_attested_result(canonical_merge_repository):
    test_source_candidate_cli_uses_real_job_and_private_two_parent_commit(
        canonical_merge_repository, generation_adversaries="self-reference",
    )


@pytest.mark.parametrize("canonical_merge_repository", ["source-job"], indirect=True)
def test_source_final_handoff_rejects_actual_release_race_and_recovers_history(
    canonical_merge_repository,
):
    test_source_candidate_cli_uses_real_job_and_private_two_parent_commit(
        canonical_merge_repository,
        handoff_boundary="release",
    )


@pytest.mark.parametrize("canonical_merge_repository", ["source-job"], indirect=True)
def test_public_installation_recovers_actual_partial_plan_before_job(canonical_merge_repository):
    test_source_candidate_cli_uses_real_job_and_private_two_parent_commit(
        canonical_merge_repository,
        installation_boundary="partial-plan",
    )


@pytest.mark.parametrize(
    "canonical_merge_repository", ["source-job-created-file-after-create"], indirect=True
)
def test_public_installation_recovers_created_file_from_original_job(canonical_merge_repository):
    test_source_candidate_cli_uses_real_job_and_private_two_parent_commit(
        canonical_merge_repository,
        installation_boundary="created-file-after-create",
    )


@pytest.mark.parametrize(
    "canonical_merge_repository", ["source-job-created-file-after-create"], indirect=True
)
def test_public_recovery_preserves_same_bytes_replacement_before_original_restore(
    canonical_merge_repository,
):
    test_source_candidate_cli_uses_real_job_and_private_two_parent_commit(
        canonical_merge_repository,
        installation_boundary="created-file-after-create",
        recovery_adversary="same-bytes-replacement",
    )


def _install_source_boundary_probe(root: Path, boundary: str) -> None:
    """Freeze a worker observation barrier before fixture history and custody."""
    module = root / "src/ai_trading_system/platform/architecture/workflow_integration.py"
    source = module.read_text(encoding="utf-8")
    markers = {
        "generation-before": "    generation_content = _encode_source_generation(delta)\n",
        "generation-after": (
            "    for position, operation in enumerate(delta[\"operations\"]):\n"
            "        if operation[\"path\"] in captured:\n"
        ),
        "commit-before": "    commit = (\n        io_backend._git(\n",
        "commit-after": "    header = io_backend._git(root, \"cat-file\", \"commit\", commit)",
    }
    marker = markers[boundary]
    # Bound the insertion to the original source worker, not another renderer.
    start = source.index("def source_candidate_worker(")
    end = source.index("\ndef _source_runtime_binding(", start)
    worker = source[start:end]
    assert worker.count(marker) == 1
    hook = (
        "    import time as probe_time\n"
        "    from ai_trading_system.platform.architecture.workflow_execution "
        "import current_process_identity as probe_identity\n"
        f"    print(json.dumps({{'source_boundary': {boundary!r}, "
        "'process': probe_identity(), 'commit': locals().get('commit')}), flush=True)\n"
        "    probe_time.sleep(120)\n"
        "    os._exit(98)\n"
    )
    module.write_text(source[:start] + worker.replace(marker, hook + marker) + source[end:],
                      encoding="utf-8", newline="\n")


@pytest.mark.parametrize("canonical_merge_repository", ["source-job"], indirect=True)
@pytest.mark.parametrize("source_boundary", [
    "generation-before", "generation-after", "commit-before", "commit-after",
])
def test_source_generation_and_commit_crash_recovers_without_installation(
    canonical_merge_repository, source_boundary: str,
) -> None:
    from test_arch_005_task_checkpoint import _terminate_fixture_producer
    from test_devx015_workflow_integration import TASK, _freeze_candidate_fixture

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )

    root, _scope = canonical_merge_repository
    (root / "mode.txt").write_bytes(b"unique original generation crash source\n")
    transaction = _freeze_candidate_fixture(root)
    fence = IntegrationPublicationFence(project_root=root)
    lease_id = fence.replay(transaction).transaction["lease_id"]
    request_id = "generation-crash-" + uuid.uuid4().hex
    run = root / "outputs/architecture/workflow_integration/source_candidates" / request_id
    command = [sys.executable, str(root / "scripts/architecture_arch005_workflow.py"),
               "source-candidate", "--task-id", TASK, "--publication-transaction",
               str(transaction), "--actor", "integration-coordinator", "--request-id", request_id]
    environment = dict(os.environ, PYTHONPATH=str(root / "src"), PYTHONDONTWRITEBYTECODE="1")
    before = _stage_snapshot(root)
    refs = _git(root, "show-ref")
    log_path = root.parent / "generation-crash-launcher.log"

    def witness():
        path = run / "worker.stdout.log"
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if row.get("source_boundary") == source_boundary:
                    return row
        return {"launcher_exited": True} if launcher.poll() is not None else None

    oracle = NativeOracle()
    producer_identity = None
    with log_path.open("wb") as log:
        launcher = subprocess.Popen(
            command, cwd=root, env=environment, stdout=log,
            stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW,
        )
        try:
            observed = _until(witness, description="original source worker boundary", timeout=1800)
            assert "launcher_exited" not in observed, log_path.read_text(errors="replace")
            head = next(row for row in fence.guard.store.replay().lease_heads
                        if row.lease_id == lease_id)
            request = head.execution["request"]
            producer_identity = head.execution["launcher"]
            bound_identity = head.execution["process"]
            with (
                oracle.process(producer_identity["pid"]) as producer,
                oracle.process(bound_identity["pid"]) as bound,
                oracle.process(observed["process"]["pid"]) as worker,
            ):
                assert oracle.creation_time(producer) == producer_identity["creation_time"]
                assert oracle.creation_time(bound) == bound_identity["creation_time"]
                assert oracle.creation_time(worker) == observed["process"]["creation_time"]
                oracle.assert_in_job(bound, request["job_name"])
                oracle.assert_in_job(worker, request["job_name"])
                assert not oracle.exited(producer) and not oracle.exited(worker)
                _terminate_fixture_producer(producer_identity)
                launcher.wait(timeout=DEADLINE)
                assert launcher.returncode != 0
                assert oracle.exited(producer, 10)
                assert oracle.exited(bound, 10) and oracle.exited(worker, 10)
            oracle.assert_job_absent(request["job_name"])
        finally:
            if producer_identity is not None:
                _terminate_fixture_producer(producer_identity)
            if launcher.poll() is None:
                launcher.kill()
                launcher.wait(timeout=DEADLINE)
    preserved = {p.relative_to(run).as_posix(): p.read_bytes()
                 for p in run.rglob("*") if p.is_file()}
    assert ("generation.json" in preserved) == (source_boundary != "generation-before")
    assert ("object_intent.json" in preserved) == source_boundary.startswith("commit-")
    assert "worker_result.json" not in preserved
    private_commit = observed["commit"]
    if source_boundary == "commit-after":
        assert private_commit and _git(root, "cat-file", "-t", private_commit) == "commit"
        private_bytes = _git(root, "cat-file", "commit", private_commit)
    else:
        assert private_commit is None
    recovery = list(command)
    recovery[2] = "source-recover"
    completed = subprocess.run(
        recovery, cwd=root, env=environment, capture_output=True, timeout=1800,
    )
    (root.parent / "generation-crash-recovery.stdout").write_bytes(completed.stdout)
    (root.parent / "generation-crash-recovery.stderr").write_bytes(completed.stderr)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    result = json.loads(completed.stdout)
    assert result["status"] == "RECOVERED_FAILED"
    assert result["dispatch_allowed"] is False and result["installation_performed"] is False
    assert result["private_evidence_preserved"] is True
    assert fence.replay(transaction).phase == "FAILED"
    head = next(row for row in fence.guard.store.replay().lease_heads if row.lease_id == lease_id)
    assert head.state == "RELEASED" and head.execution["result"]["status"] == "INSUFFICIENT"
    terminal = tuple(fence.guard.store.replay().head_event_ids)
    repeated = subprocess.run(
        recovery, cwd=root, env=environment, capture_output=True, timeout=1800,
    )
    assert repeated.returncode == 0, repeated.stdout + repeated.stderr
    assert json.loads(repeated.stdout)["dispatch_allowed"] is False
    assert tuple(fence.guard.store.replay().head_event_ids) == terminal
    assert {p.relative_to(run).as_posix(): p.read_bytes()
            for p in run.rglob("*") if p.is_file()} == preserved
    if private_commit:
        assert _git(root, "cat-file", "commit", private_commit) == private_bytes
    after = _stage_snapshot(root)
    assert {k: v for k, v in after[0].items() if not k.startswith("outputs/")} == {
        k: v for k, v in before[0].items() if not k.startswith("outputs/")}
    assert after[1:] == before[1:] and _git(root, "show-ref") == refs


@pytest.mark.parametrize("canonical_merge_repository", ["source-job"], indirect=True)
@pytest.mark.parametrize("write_boundary", ["before", "after"])
def test_source_recover_cli_preserves_early_reserved_crash(
    canonical_merge_repository,
    write_boundary: str,
) -> None:
    from test_devx015_workflow_integration import TASK, _freeze_candidate_fixture

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )

    root, _scope = canonical_merge_repository
    (root / "mode.txt").write_bytes(b"reviewed early source crash\n")
    transaction = _freeze_candidate_fixture(root)
    request_id = "source-crash-" + uuid.uuid4().hex
    run = root / "outputs/architecture/workflow_integration/source_candidates" / request_id
    ready = root.parent / "source-crash.ready"
    release = root.parent / "source-crash.release"
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment.update(
        PYTHONPATH=str(root / "src"),
        PYTHONDONTWRITEBYTECODE="1",
        GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_OPTIONAL_LOCKS="0",
    )
    program = """
import os, sys, time
from pathlib import Path
from ai_trading_system.platform.architecture import workflow_integration as integration
from ai_trading_system.platform.architecture.workflow_integration import start_source_candidate
root, transaction, ready, release = map(Path, sys.argv[1:5])
original = integration.write_bound_once
target = root/'outputs/architecture/workflow_integration/source_candidates'/sys.argv[5]
def fault(bound_root, relative, content):
    path = bound_root/relative
    if path == target/'request.json':
        if sys.argv[6] == 'after':
            original(bound_root, relative, content)
        ready.write_text(str(os.getpid()))
        deadline = time.monotonic()+60
        while not release.exists():
            if time.monotonic()>deadline: os._exit(98)
            time.sleep(.02)
        os._exit(17)
    return original(bound_root, relative, content)
integration.write_bound_once = fault
start_source_candidate(root, sys.argv[7], transaction_path=transaction,
                       actor='integration-coordinator', request_id=sys.argv[5])
"""
    fence = IntegrationPublicationFence(project_root=root)
    lease_id = fence.replay(transaction).transaction["lease_id"]

    def lease_head():
        return next(
            row for row in fence.guard.store.replay().lease_heads if row.lease_id == lease_id
        )

    before = _stage_snapshot(root)
    refs = _git(root, "for-each-ref", "--format=%(refname) %(objectname)")
    log_path = root.parent / "source-crash.stdout.log"
    oracle = NativeOracle()
    with log_path.open("wb") as log:
        launcher = subprocess.Popen(
            [
                sys.executable,
                "-u",
                "-c",
                program,
                str(root),
                str(transaction),
                str(ready),
                str(release),
                request_id,
                write_boundary,
                TASK,
            ],
            cwd=root,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        try:
            _until(
                lambda: ready.exists() or launcher.poll() is not None,
                description="source reservation before request write crash",
                timeout=1800,
            )
            assert ready.exists(), log_path.read_text(encoding="utf-8", errors="replace")
            execution = lease_head().execution
            assert execution["state"] == "RESERVED" and execution["process"] is None
            request = execution["request"]
            assert request["schema_version"] == "workflow_execution_request.v3"
            assert request["execution_kind"] == "CONTROLLED_SOURCE_CANDIDATE"
            assert request["request_id"] == request_id and request["lease_id"] == lease_id
            assert request["cwd"] == root.as_posix()
            assert request["argv"][1] == str(root / "scripts/architecture_arch005_workflow.py")
            assert request["result_path"] == (run / "worker_result.json").as_posix()
            assert execution["launcher"]["pid"] == int(ready.read_text())
            with oracle.process(execution["launcher"]["pid"]) as process:
                assert oracle.creation_time(process) == execution["launcher"]["creation_time"]
                assert not oracle.exited(process)
                oracle.assert_job_absent(request["job_name"])
                release.write_text("exit", encoding="utf-8")
                assert launcher.wait(timeout=DEADLINE) == 17
                assert oracle.exited(process, 10)
        finally:
            if launcher.poll() is None:
                launcher.kill()
                launcher.wait(timeout=DEADLINE)
    preserved = {path.name: path.read_bytes() for path in run.glob("*") if path.is_file()}
    assert set(preserved) == ({"request.json"} if write_boundary == "after" else set())
    if write_boundary == "after":
        assert json.loads(preserved["request.json"])["request_id"] == request_id
    command = [
        sys.executable,
        str(root / "scripts/architecture_arch005_workflow.py"),
        "source-recover",
        "--task-id",
        TASK,
        "--publication-transaction",
        str(transaction),
        "--actor",
        "integration-coordinator",
        "--request-id",
        request_id,
    ]
    original_heads = tuple(fence.guard.store.replay().head_event_ids)
    for option, value, code in (
        ("--actor", "wrong-actor", "SOURCE_TRANSACTION_IDENTITY"),
        ("--request-id", request_id + "-wrong", "SOURCE_REQUEST_BINDING"),
    ):
        wrong = list(command)
        wrong[wrong.index(option) + 1] = value
        rejected = subprocess.run(
            wrong,
            cwd=root,
            env=environment,
            capture_output=True,
            timeout=1800,
        )
        assert rejected.returncode != 0
        assert code in (rejected.stdout + rejected.stderr).decode(errors="replace")
        assert tuple(fence.guard.store.replay().head_event_ids) == original_heads
    recovered = subprocess.run(
        command, cwd=root, env=environment, capture_output=True, timeout=1800,
    )
    assert recovered.returncode == 0, (recovered.stdout + recovered.stderr).decode(errors="replace")
    result = json.loads(recovered.stdout)
    assert result["status"] == "RECOVERED_FAILED" and result["dispatch_allowed"] is False
    assert result["private_evidence_preserved"] is True
    assert result["installation_performed"] is False
    assert fence.replay(transaction).phase == "FAILED"
    terminal = lease_head()
    assert terminal.state == "RELEASED"
    assert terminal.execution["state"] == "RESULT_RECORDED"
    assert terminal.execution["result"]["status"] == "INSUFFICIENT"
    terminal_heads = tuple(fence.guard.store.replay().head_event_ids)
    for verb in ("source-recover", "source-candidate"):
        replay_command = list(command)
        replay_command[2] = verb
        repeated = subprocess.run(
            replay_command,
            cwd=root,
            env=environment,
            capture_output=True,
            timeout=1800,
        )
        assert repeated.returncode == 0, (repeated.stdout + repeated.stderr).decode(
            errors="replace"
        )
        assert json.loads(repeated.stdout)["dispatch_allowed"] is False
        assert tuple(fence.guard.store.replay().head_event_ids) == terminal_heads
    assert {path.name: path.read_bytes() for path in run.glob("*") if path.is_file()} == preserved
    oracle.assert_job_absent(request["job_name"])
    after = _stage_snapshot(root)
    assert {k: v for k, v in after[0].items() if not k.startswith("outputs/")} == {
        k: v for k, v in before[0].items() if not k.startswith("outputs/")
    }
    assert after[1:] == before[1:]
    assert _git(root, "for-each-ref", "--format=%(refname) %(objectname)") == refs


@pytest.mark.parametrize("canonical_merge_repository", ["compatibility"], indirect=True)
@pytest.mark.parametrize("mutation", ["none", "changed", "unknown", "unknown-declared"])
def test_source_generators_preserve_main_history_outside_current_report_index(
    four_generator_repository, mutation
) -> None:
    from ai_trading_system.platform.architecture import report_catalog_flow_authority as report
    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_integration import (
        render_source_generators,
    )

    root, _outputs, _expected = four_generator_repository
    policy = report.load_policy(root)
    content = b'{"retained_historical_fragment":"synthetic prior generation"}\n'
    name = policy["fragment_root"] + "/historical/" + hashlib.sha256(content).hexdigest() + ".json"
    path = root / name
    path.parent.mkdir()
    path.write_bytes(content)
    _git(root, "add", "--", name)
    _git(root, "commit", "--only", "-m", "retain prior report generation", "--", name)
    main = _git(root, "rev-parse", "HEAD")
    _git(root, "update-ref", "refs/heads/main", main)
    index = json.loads(_git(root, "show", main + ":" + policy["index_path"]))
    assert name not in {
        row["fragment_path"] for target in index["targets"] for row in target["fragments"]
    }
    assert _git(root, "cat-file", "blob", main + ":" + name).encode() == content.rstrip(b"\n")
    if mutation == "changed":
        path.write_bytes(b"changed historical bytes\n")
    elif mutation in {"unknown", "unknown-declared"}:
        path.with_name("unknown.json").write_bytes(b"unowned bytes\n")
        if mutation == "unknown-declared":
            _git(root, "add", "--", path.with_name("unknown.json").relative_to(root).as_posix())
    expected = _stage_inputs(root)
    before = _stage_snapshot(root)
    refs = _git(root, "for-each-ref", "--format=%(refname) %(objectname)")
    if mutation == "none":
        summary, generated = render_source_generators(root, expected, main_commit=main)
        assert summary["status"] == "RENDERED_PRIVATE_SOURCE_GENERATION"
        assert name not in generated
        assert path.read_bytes() == content
    else:
        code = {
            "changed": "GENERATOR_MAIN_INPUT_CHANGED",
            "unknown": "GENERATOR_INPUT_UNDECLARED",
            "unknown-declared": "REPORT_OUTPUT_SET_CHANGED",
        }[mutation]
        with pytest.raises(WorkflowContractError, match=code):
            render_source_generators(root, expected, main_commit=main)
    assert _stage_snapshot(root) == before
    assert _git(root, "for-each-ref", "--format=%(refname) %(objectname)") == refs


@pytest.mark.parametrize("canonical_merge_repository", ["compatibility"], indirect=True)
def test_source_generators_render_real_four_step_chain_without_materialization(
    four_generator_repository,
) -> None:
    from ai_trading_system.platform.architecture import compatibility_authority as compatibility
    from ai_trading_system.platform.architecture import report_catalog_flow_authority as report
    from ai_trading_system.platform.architecture.workflow_integration import (
        render_source_generators,
    )
    from ai_trading_system.yaml_loader import safe_load_yaml_path, safe_load_yaml_text

    root, architecture_outputs, expected = four_generator_repository
    policy_before = report.load_policy(root)
    compatibility_policy = safe_load_yaml_path(root / compatibility.DEFAULT_POLICY_PATH)
    compatibility_index = compatibility_policy["index_path"]
    old_compatibility = json.loads((root / compatibility_index).read_text(encoding="utf-8"))
    old_fragments = {entry["fragment_path"] for entry in old_compatibility["entries"]}
    canonical_outputs = _canonical_stage_outputs(root)
    before = _stage_snapshot(root)
    progress_events = []
    summary, generated = render_source_generators(
        root,
        expected,
        main_commit=_git(root, "rev-parse", "refs/heads/main"),
        progress=lambda generator, phase, boundary: progress_events.append(
            (generator, phase, boundary)
        ),
    )
    order = [
        "canonical-task-source",
        "architecture-manifests",
        "report-flow-authority",
        "compatibility-authority",
    ]
    assert summary["generator_order"] == order
    assert summary["status"] == "RENDERED_PRIVATE_SOURCE_GENERATION"
    assert [step["generator_id"] for step in summary["steps"]] == order
    assert progress_events == [
        (step["generator_id"], phase["phase"], boundary)
        for step in summary["steps"]
        for phase in step["phases"]
        for boundary in ("START", "END")
    ]
    assert summary["materialization_allowed"] is False
    assert summary["publication_allowed"] is False
    for step in summary["steps"]:
        assert step["inputs"]
        assert all(observed["versions"] for observed in step["inputs"].values())
        for output in step["outputs"]:
            content = generated[output["path"]]
            assert isinstance(content, bytes)
            assert hashlib.sha256(content).hexdigest() == output["sha256"]
    for name in (
        canonical_outputs
        | architecture_outputs
        | {policy_before["index_path"], compatibility_index}
    ):
        assert isinstance(generated[name], bytes) and generated[name]
    assert generated[policy_before["index_path"]] != before[0][policy_before["index_path"]]
    assert generated[compatibility_index] != before[0][compatibility_index]
    new_compatibility = json.loads(generated[compatibility_index])
    stale = old_fragments - {entry["fragment_path"] for entry in new_compatibility["entries"]}
    assert stale, "fixture must actually supersede old compatibility fragments"
    assert all(generated[name] is None for name in stale)
    assert all((root / name).read_bytes() == before[0][name] for name in stale)
    policy_after = safe_load_yaml_text(generated[report.DEFAULT_POLICY_PATH.as_posix()].decode())
    assert {key: value for key, value in policy_after.items() if key != "targets"} == {
        key: value for key, value in policy_before.items() if key != "targets"
    }
    seal_fields = {"byte_count", "file_sha256", "lf_sha256", "git_blob", "entry_count"}
    assert len(policy_after["targets"]) == len(policy_before["targets"])
    for old, new in zip(policy_before["targets"], policy_after["targets"], strict=True):
        changed = {key for key in old.keys() | new.keys() if old.get(key) != new.get(key)}
        assert changed == (seal_fields if old["target_id"] == "system_flow" else set())
        if old["target_id"] == "system_flow":
            content = before[0][old["path"]]
            assert new["byte_count"] == len(content)
            assert new["file_sha256"] == hashlib.sha256(content).hexdigest()
            assert new["lf_sha256"] == hashlib.sha256(content.replace(b"\r\n", b"\n")).hexdigest()
            assert new["git_blob"] == _git(
                root, "hash-object", "--no-filters", "--stdin", content=content
            )
            assert new["entry_count"] == old["entry_count"] + 1
    assert _stage_snapshot(root) == before


@pytest.mark.parametrize("canonical_merge_repository", ["compatibility"], indirect=True)
@pytest.mark.parametrize("damage", ["reordered", "omitted"])
def test_source_generators_reject_incomplete_or_reordered_sequence(
    four_generator_repository, damage: str
) -> None:
    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_integration import (
        render_source_generators,
    )

    root, _outputs, expected = four_generator_repository
    order = [
        "canonical-task-source",
        "architecture-manifests",
        "report-flow-authority",
        "compatibility-authority",
    ]
    if damage == "reordered":
        order[0], order[1] = order[1], order[0]
    else:
        order.pop()
    before = _stage_snapshot(root)
    with pytest.raises(WorkflowContractError, match="GENERATOR_ORDER"):
        render_source_generators(
            root,
            expected,
            main_commit=_git(root, "rev-parse", "refs/heads/main"),
            generator_order=order,
        )
    assert _stage_snapshot(root) == before


@pytest.mark.parametrize("canonical_merge_repository", ["compatibility"], indirect=True)
def test_source_generators_reject_unknown_python_before_open(
    four_generator_repository, monkeypatch: pytest.MonkeyPatch
) -> None:
    import builtins
    import io

    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_integration import (
        render_source_generators,
    )

    root, _outputs, expected = four_generator_repository
    unknown = root / "scripts/unreviewed_canary.py"
    assert "scripts/unreviewed_canary.py" not in expected
    unknown.write_bytes(b"UNREVIEWED_CONSUMER = True\n")
    before = _stage_snapshot(root)
    main = _git(root, "rev-parse", "refs/heads/main")
    opened = []

    def watched(original):
        def call(path, *args, **kwargs):
            if not isinstance(path, int) and os.path.normcase(os.fspath(path)) == os.path.normcase(
                str(unknown)
            ):
                opened.append(str(path))
                raise AssertionError("unknown source reached actual open")
            return original(path, *args, **kwargs)

        return call

    real_dll = ctypes.WinDLL

    class WatchedKernel:
        def __init__(self, native):
            self.native = native
            self.CreateFileW = _ObservedNativeCall(
                native.CreateFileW, lambda original, *a, **k: watched(original)(*a, **k)
            )

        def __getattr__(self, name):
            return getattr(self.native, name)

    with monkeypatch.context() as hooks:
        hooks.setattr(builtins, "open", watched(builtins.open))
        hooks.setattr(io, "open", watched(io.open))
        hooks.setattr(os, "open", watched(os.open))
        hooks.setattr(ctypes, "WinDLL", lambda *a, **k: WatchedKernel(real_dll(*a, **k)))
        with pytest.raises(WorkflowContractError, match="GENERATOR_INPUT_UNDECLARED"):
            render_source_generators(root, expected, main_commit=main)
    assert opened == []
    assert _stage_snapshot(root) == before


@pytest.mark.parametrize("canonical_merge_repository", ["compatibility"], indirect=True)
def test_source_generators_recomputed_input_cannot_replace_main_report_contract(
    four_generator_repository,
) -> None:
    from ai_trading_system.platform.architecture import report_catalog_flow_authority as report
    from ai_trading_system.platform.architecture import task_registry_canonical as canonical
    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_integration import (
        render_source_generators,
    )

    root, _outputs, _expected = four_generator_repository
    policy = report.load_policy(root)
    policy["owner_decision"] = "unreviewed-policy-owner-decision"
    (root / report.DEFAULT_POLICY_PATH).write_bytes(canonical._yaml_bytes(policy))
    expected = _stage_inputs(root)
    before = _stage_snapshot(root)
    with pytest.raises(WorkflowContractError, match="REPORT_POLICY_CONTRACT_CHANGED"):
        render_source_generators(
            root, expected, main_commit=_git(root, "rev-parse", "refs/heads/main")
        )
    assert _stage_snapshot(root) == before


@pytest.mark.parametrize("canonical_merge_repository", ["compatibility"], indirect=True)
def test_source_generators_recomputed_input_cannot_replace_old_main_fragment(
    four_generator_repository,
) -> None:
    from ai_trading_system.platform.architecture import compatibility_authority as compatibility
    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_integration import (
        render_source_generators,
    )
    from ai_trading_system.yaml_loader import safe_load_yaml_path

    root, _outputs, _expected = four_generator_repository
    main = _git(root, "rev-parse", "refs/heads/main")
    policy = safe_load_yaml_path(root / compatibility.DEFAULT_POLICY_PATH)
    main_index = json.loads(_git(root, "show", main + ":" + policy["index_path"]))
    target = next(
        entry["fragment_path"]
        for entry in main_index["entries"]
        if entry["section_id"] == "phase_devx_006d_report_catalog_flow_lossless_fragmentation"
    )
    (root / target).write_bytes(b"tampered immutable main fragment\n")
    expected = _stage_inputs(root)
    before = _stage_snapshot(root)
    with pytest.raises(WorkflowContractError, match="GENERATOR_MAIN_INPUT_CHANGED"):
        render_source_generators(root, expected, main_commit=main)
    assert _stage_snapshot(root) == before


@pytest.mark.parametrize("canonical_merge_repository", ["compatibility"], indirect=True)
def test_source_generators_revalidate_earlier_inputs_after_real_final_validator(
    four_generator_repository, monkeypatch: pytest.MonkeyPatch
) -> None:
    import builtins

    from ai_trading_system.platform.architecture import compatibility_authority as compatibility
    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_integration import (
        render_source_generators,
    )

    root, _outputs, expected = four_generator_repository
    before = _stage_snapshot(root)
    main = _git(root, "rev-parse", "refs/heads/main")
    name = "docs/system_flow.md"
    changed = before[0][name] + b"EXTERNAL_POST_VALIDATION_DRIFT\n"
    real_validate = compatibility.validate_repository_authority
    original_open = builtins.open
    validated = []

    def drift_after_actual_validator(*args, **kwargs):
        result = real_validate(*args, **kwargs)
        validated.append(result)
        with original_open(root / name, "wb") as stream:
            stream.write(changed)
        return result

    with monkeypatch.context() as hooks:
        hooks.setattr(compatibility, "validate_repository_authority", drift_after_actual_validator)
        with pytest.raises(WorkflowContractError, match="GENERATOR_INPUT_DRIFT"):
            render_source_generators(root, expected, main_commit=main)
    assert len(validated) == 1, "fault must follow the real compatibility validation"
    after = _stage_snapshot(root)
    assert after == ({**before[0], name: changed}, before[1], before[2])
    assert (root / name).read_bytes() == changed, "external fault bytes must remain as evidence"


def test_generated_stage_historical_git_reads_use_exact_objects_and_exclusions(
    small_repository: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ai_trading_system.platform.architecture import compatibility_authority as compatibility
    from ai_trading_system.platform.architecture import task_registry_canonical as canonical
    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_integration import GeneratedArtifactStage
    from ai_trading_system.yaml_loader import safe_load_yaml_path

    root = small_repository
    literal = "DEVX015_GIT_LITERAL"
    source = "src/probe.py"
    excluded_name = "src/excluded_probe.py"
    (root / "src").mkdir()
    (root / source).write_bytes(b"VALUE = 'DEVX015_GIT_LITERAL'\n")
    excluded_content = b"EXCLUDED = 'DEVX015_GIT_LITERAL'\n"
    (root / excluded_name).write_bytes(excluded_content)
    excluded_oid = _git(root, "hash-object", "--stdin", content=excluded_content)
    _git(root, "add", "--", source, excluded_name)
    policy_path = root / "config/architecture/arch_005_s4d_checkout_guard.yaml"
    policy = safe_load_yaml_path(policy_path)
    policy["known_unrelated_exclusions"].append(
        {"path": excluded_name, "rationale": "Synthetic excluded input", "owner_ref": "fixture"}
    )
    policy_path.write_bytes(canonical._yaml_bytes(policy))
    _git(root, "add", "--", str(policy_path.relative_to(root)))
    _git(root, "commit", "-m", "exact historical read fixture")
    commit = _git(root, "rev-parse", "HEAD")
    tree = _git(root, "rev-parse", commit + "^{tree}")
    blob_content = (root / ".gitignore").read_bytes()
    blob_oid = _git(root, "rev-parse", commit + ":.gitignore")
    excluded = {row["path"] for row in policy["known_unrelated_exclusions"]}
    expected = _stage_inputs(root, excluded=excluded)
    assert excluded_name not in expected
    before = _stage_snapshot(root, excluded=excluded)
    original_run = subprocess.run
    grep_commands = []
    forbidden_reads = []

    def observe_command(command, *args, **kwargs):
        if isinstance(command, list) and "cat-file" in command and excluded_oid in command:
            forbidden_reads.append(command)
            raise AssertionError("excluded Git object reached cat-file")
        if isinstance(command, list) and "grep" in command:
            grep_commands.append(list(command))
        return original_run(command, *args, **kwargs)

    with monkeypatch.context() as hooks:
        hooks.setattr(subprocess, "run", observe_command)
        with GeneratedArtifactStage(root, expected, output_paths=set()) as stage:
            assert compatibility._git_bytes(root, commit, ".gitignore") == blob_content
            assert compatibility._git_text(root, commit, "absent.txt") == ""
            assert compatibility._git_lines(
                root,
                ["grep", "-l", "-F", literal, commit, "--", "src", "scripts", "tests"],
                allow_no_match=True,
            ) == [commit + ":" + source]
            assert (
                compatibility._git_lines(
                    root,
                    ["grep", "-l", "-F", "NO_MATCH_015", commit, "--", "src", "scripts", "tests"],
                    allow_no_match=True,
                )
                == []
            )
            with pytest.raises(WorkflowContractError, match="GENERATOR_GIT_EXCLUDED"):
                compatibility._git_bytes(root, commit, excluded_name)
            with pytest.raises(WorkflowContractError, match="GENERATOR_GIT_COMMAND"):
                compatibility._git_lines(
                    root,
                    ["grep", "-l", "-F", literal, commit, "--", "."],
                    allow_no_match=True,
                )
            for invalid in ("main", "f" * 40):
                with pytest.raises(WorkflowContractError):
                    compatibility._git_text(root, invalid, "absent.txt")
            evidence = stage.finish()["git_inputs"]
    assert forbidden_reads == []
    assert len(grep_commands) == 2
    full_pathspec = ["src", "scripts", "tests"] + [
        ":(exclude,literal)" + row["path"] for row in policy["known_unrelated_exclusions"]
    ]
    assert all(command[command.index("--") + 1 :] == full_pathspec for command in grep_commands)
    by_path = {row["path"]: row for row in evidence if "path" in row}
    assert by_path[".gitignore"] == {
        "commit": commit,
        "path": ".gitignore",
        "object": {"exists": True, "mode": "100644", "type": "blob", "oid": blob_oid},
        "sha256": hashlib.sha256(blob_content).hexdigest(),
    }
    assert by_path["absent.txt"] == {
        "commit": commit,
        "path": "absent.txt",
        "object": {"exists": False, "mode": None, "type": None, "oid": None},
    }
    searches = [row for row in evidence if "search_text" in row]
    assert len(searches) == 2
    assert all(row["commit"] == commit and row["tree"] == tree for row in searches)
    assert all(row["pathspec"] == full_pathspec for row in searches)
    assert _stage_snapshot(root, excluded=excluded) == before


class _ObservedNativeCall:
    """Keep actual ctypes signature/return conversion while inserting a timing hook."""

    def __init__(self, native, invoke):
        object.__setattr__(self, "native", native)
        object.__setattr__(self, "invoke", invoke)

    def __setattr__(self, name, value):
        setattr(self.native, name, value)

    def __getattr__(self, name):
        return getattr(self.native, name)

    def __call__(self, *args, **kwargs):
        return self.invoke(self.native, *args, **kwargs)


@pytest.mark.parametrize("canonical_merge_repository", ["candidate"], indirect=True)
def test_generated_stage_runs_real_canonical_renderer_without_materialization(
    canonical_merge_repository,
) -> None:
    from ai_trading_system.platform.architecture import task_registry_canonical as canonical
    from ai_trading_system.platform.architecture.workflow_integration import GeneratedArtifactStage

    root, _scope = canonical_merge_repository
    expected = _stage_inputs(root)
    outputs = _canonical_stage_outputs(root)
    before = _stage_snapshot(root)
    with GeneratedArtifactStage(root, expected, output_paths=outputs) as stage:
        canonical.refresh_consumer_inventory(project_root=root)
        summary = stage.finish()
    assert set(stage.outputs) == outputs
    assert all(isinstance(value, bytes) and value for value in stage.outputs.values())
    assert stage.reads
    for path, observed in stage.reads.items():
        if observed["origin"] == "WORKTREE_INPUT":
            assert observed["object"] == expected[path]
            assert observed["sha256"] == hashlib.sha256(before[0][path]).hexdigest()
            assert observed["read_count"] >= 1
    assert summary["status"] == "RENDERED_PRIVATE_ARTIFACTS"
    assert summary["materialization_allowed"] is False
    assert summary["publication_allowed"] is False
    assert summary["worktree_mutated"] is False
    assert _stage_snapshot(root) == before


@pytest.mark.parametrize("canonical_merge_repository", ["candidate"], indirect=True)
def test_generated_stage_unknown_python_is_rejected_before_any_open(
    canonical_merge_repository, monkeypatch: pytest.MonkeyPatch
) -> None:
    import builtins
    import io

    from ai_trading_system.platform.architecture import task_registry_canonical as canonical
    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_integration import GeneratedArtifactStage

    root, _scope = canonical_merge_repository
    expected = _stage_inputs(root)
    outputs = _canonical_stage_outputs(root)
    unknown = root / "scripts/unreviewed_canary.py"
    unknown.write_bytes(b"raise AssertionError('unreviewed consumer')\n")
    before = _stage_snapshot(root)
    opened = []

    def watched(original):
        def call(path, *args, **kwargs):
            if not isinstance(path, int) and os.path.normcase(os.fspath(path)) == os.path.normcase(
                str(unknown)
            ):
                opened.append(str(path))
                raise AssertionError("unknown input reached OS open")
            return original(path, *args, **kwargs)

        return call

    # Observe both ordinary Python opens and the actual Win32 frozen-reader open.
    real_dll = ctypes.WinDLL

    class WatchedKernel:
        def __init__(self, native):
            self.native = native
            self.CreateFileW = _ObservedNativeCall(
                native.CreateFileW, lambda original, *a, **k: watched(original)(*a, **k)
            )

        def __getattr__(self, name):
            return getattr(self.native, name)

    with monkeypatch.context() as hooks:
        hooks.setattr(builtins, "open", watched(builtins.open))
        hooks.setattr(io, "open", watched(io.open))
        hooks.setattr(os, "open", watched(os.open))
        hooks.setattr(ctypes, "WinDLL", lambda *a, **k: WatchedKernel(real_dll(*a, **k)))
        with pytest.raises(WorkflowContractError):
            with GeneratedArtifactStage(root, expected, output_paths=outputs):
                canonical.refresh_consumer_inventory(project_root=root)
    assert opened == []
    assert _stage_snapshot(root) == before


def test_generated_stage_denies_unlisted_unlink(small_repository: Path) -> None:
    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_integration import GeneratedArtifactStage

    root = small_repository
    expected = _stage_inputs(root)
    target = root / ".gitignore"
    before = _stage_snapshot(root)
    with pytest.raises(WorkflowContractError):
        with GeneratedArtifactStage(root, expected, output_paths=set()):
            target.unlink()
    assert _stage_snapshot(root) == before


def test_generated_stage_denies_direct_os_open_on_allowed_output(small_repository: Path) -> None:
    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_integration import GeneratedArtifactStage

    root = small_repository
    expected = _stage_inputs(root)
    before = _stage_snapshot(root)
    rejection = None
    try:
        with GeneratedArtifactStage(root, expected, output_paths={".gitignore"}):
            descriptor = os.open(root / ".gitignore", os.O_WRONLY | os.O_TRUNC | os.O_BINARY)
            os.close(descriptor)
    except WorkflowContractError as error:
        rejection = error
    assert _stage_snapshot(root) == before, "direct os.open changed physical allowed output"
    assert rejection is not None, "direct os.open was not rejected"
    assert "DIRECT_IO_DENIED" in rejection.code


def test_generated_stage_denies_direct_os_replace(small_repository: Path) -> None:
    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_integration import GeneratedArtifactStage

    root = small_repository
    source = root / "replacement.txt"
    source.write_bytes(b"unauthorized replacement\n")
    _git(root, "add", "--", "replacement.txt")
    expected = _stage_inputs(root)
    before = _stage_snapshot(root)
    rejection = None
    try:
        with GeneratedArtifactStage(root, expected, output_paths={".gitignore"}):
            os.replace(source, root / ".gitignore")
    except WorkflowContractError as error:
        rejection = error
    assert _stage_snapshot(root) == before, "direct os.replace changed physical worktree"
    assert rejection is not None, "direct os.replace was not rejected"
    assert "DIRECT_IO_DENIED" in rejection.code


def test_generated_stage_finish_rejects_post_scan_file_arrival(small_repository: Path) -> None:
    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_integration import GeneratedArtifactStage

    root = small_repository
    scripts = root / "scripts"
    scripts.mkdir()
    known = scripts / "known.py"
    known.write_bytes(b"KNOWN = True\n")
    _git(root, "add", "--", "scripts/known.py")
    expected = _stage_inputs(root)
    arrived = scripts / "arrived.py"
    original_open = os.open
    # Save only the exact OS operations needed to inject this one external arrival.
    original_write, original_close = os.write, os.close
    with pytest.raises(WorkflowContractError, match="SCAN"):
        with GeneratedArtifactStage(root, expected, output_paths=set()) as stage:
            assert list(scripts.glob("*.py")) == [known]
            assert list(scripts.rglob("*.py")) == [known]
            descriptor = original_open(arrived, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_BINARY)
            try:
                assert original_write(descriptor, b"EXTERNAL_ARRIVAL = True\n") == 24
            finally:
                original_close(descriptor)
            stage.finish()
    assert arrived.read_bytes() == b"EXTERNAL_ARRIVAL = True\n"
    assert known.read_bytes() == b"KNOWN = True\n"


def test_generated_stage_rejects_unknown_existing_metadata(small_repository: Path) -> None:
    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_integration import GeneratedArtifactStage

    root = small_repository
    expected = _stage_inputs(root)
    unknown = root / "unreviewed-condition.txt"
    unknown.write_bytes(b"unreviewed branch condition\n")
    before = _stage_snapshot(root)
    for method in ("exists", "is_file"):
        with pytest.raises(WorkflowContractError, match="GENERATOR_INPUT_UNDECLARED"):
            with GeneratedArtifactStage(root, expected, output_paths=set()) as stage:
                assert getattr(unknown, method)()
                stage.finish()
    assert _stage_snapshot(root) == before


@pytest.mark.parametrize("declaration", ["undeclared", "absent"])
@pytest.mark.parametrize("method", ["exists", "is_file"])
def test_generated_stage_prior_metadata_requires_external_object(
    small_repository: Path, declaration: str, method: str
) -> None:
    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_integration import GeneratedArtifactStage

    root = small_repository
    expected = _stage_inputs(root)
    name = "unreviewed-prior.txt"
    if declaration == "absent":
        expected[name] = {"exists": False, "mode": None, "type": None, "oid": None}
    before = _stage_snapshot(root)
    with pytest.raises(WorkflowContractError, match="GENERATOR_INPUT"):
        with GeneratedArtifactStage(
            root, expected, output_paths=set(), prior_outputs={name: b"unreviewed prior\n"}
        ) as stage:
            assert getattr(root / name, method)()
            stage.finish()
    assert _stage_snapshot(root) == before


@pytest.mark.parametrize(
    "module_name", ["report_catalog_flow_authority", "compatibility_authority"]
)
@pytest.mark.parametrize(
    "name",
    ["config/architecture/memory-fragment.yaml", "registry/new-memory-parent/fragment.yaml"],
)
def test_generated_stage_official_regular_reader_reads_memory_fragment(
    small_repository: Path, module_name: str, name: str
) -> None:
    from ai_trading_system.platform.architecture.workflow_integration import GeneratedArtifactStage
    from ai_trading_system.platform.artifacts import writer

    root = small_repository
    module = importlib.import_module("ai_trading_system.platform.architecture." + module_name)
    expected = _stage_inputs(root)
    parent = (root / name).parent
    missing_parent = name.startswith("registry/")
    if missing_parent:
        assert not parent.exists()
    content = b"schema_version: synthetic_memory_fragment.v1\n"
    before = _stage_snapshot(root)
    with GeneratedArtifactStage(root, expected, output_paths={name}) as stage:
        writer.write_bytes_atomic(root / name, content)
        actual = module._regular_path(root, name, "test memory fragment")
        assert actual.read_bytes() == content
        assert root / name in parent.glob("*.yaml")
        assert root / name in parent.rglob("*.yaml")
        stage.finish()
    assert stage.outputs == {name: content}
    assert _stage_snapshot(root) == before
    if missing_parent:
        assert not parent.exists()


@pytest.mark.parametrize("canonical_merge_repository", ["candidate"], indirect=True)
def test_generated_stage_retains_old_and_new_canonical_read_versions(
    canonical_merge_repository,
) -> None:
    from ai_trading_system.platform.architecture import task_registry_canonical as canonical
    from ai_trading_system.platform.architecture.workflow_integration import GeneratedArtifactStage

    root, _scope = canonical_merge_repository
    consumer = root / "scripts/unreviewed_canary.py"
    consumer.write_bytes(b"# Documentation reference: task_register.md\n")
    _git(root, "add", "--", "scripts/unreviewed_canary.py")
    expected = _stage_inputs(root)
    outputs = _canonical_stage_outputs(root)
    before = _stage_snapshot(root)
    with GeneratedArtifactStage(root, expected, output_paths=outputs) as stage:
        canonical.refresh_consumer_inventory(project_root=root)
        stage.finish()
    index = canonical.CANONICAL_INDEX_PATH
    new_oid = _git(root, "hash-object", "--no-filters", "--stdin", content=stage.outputs[index])
    old_oid = expected[index]["oid"]
    assert new_oid != old_oid, "fixture did not produce a new canonical index version"
    versions = stage.reads[index]["versions"]
    assert any(
        row["origin"] == "WORKTREE_INPUT" and row["object"]["oid"] == old_oid for row in versions
    )
    assert any(
        row["origin"] == "CURRENT_GENERATOR_OUTPUT" and row["object"]["oid"] == new_oid
        for row in versions
    )
    assert _stage_snapshot(root) == before


def test_generated_stage_projects_prior_addition_and_deletion_into_scans(
    small_repository: Path,
) -> None:
    from ai_trading_system.platform.architecture.workflow_integration import GeneratedArtifactStage
    from ai_trading_system.platform.artifacts import writer

    root = small_repository
    scripts = root / "scripts"
    scripts.mkdir()
    removed = "scripts/old.py"
    added = "scripts/new.py"
    (root / removed).write_bytes(b"OLD = True\n")
    _git(root, "add", "--", removed)
    expected = _stage_inputs(root)
    before = _stage_snapshot(root)
    generated = b"NEW = True\n"
    with GeneratedArtifactStage(
        root, expected, output_paths={added}, deletion_paths={removed}
    ) as first:
        writer.write_bytes_atomic(root / added, generated)
        (root / removed).unlink()
        first.finish()
    assert first.outputs == {added: generated, removed: None}
    assert _stage_snapshot(root) == before
    # Freeze next-stage object authority independently from prior result metadata.
    oid = _git(root, "hash-object", "--no-filters", "--stdin", content=generated)
    next_expected = {
        **expected,
        added: {"exists": True, "mode": "100644", "type": "blob", "oid": oid},
        removed: {"exists": False, "mode": None, "type": None, "oid": None},
    }
    with GeneratedArtifactStage(
        root, next_expected, output_paths=set(), prior_outputs=first.outputs
    ) as second:
        assert list(scripts.glob("*.py")) == [root / added]
        assert list(scripts.rglob("*.py")) == [root / added]
        assert (root / added).read_bytes() == generated
        with pytest.raises(FileNotFoundError):
            (root / removed).read_bytes()
        second.finish()
    assert second.reads[added]["origin"] == "PRIOR_GENERATOR_OUTPUT"
    assert second.reads[added]["object"] == next_expected[added]
    assert second.outputs == {}
    assert _stage_snapshot(root) == before


def test_generated_stage_reads_external_prior_output_identity(small_repository: Path) -> None:
    from ai_trading_system.platform.architecture.workflow_integration import GeneratedArtifactStage
    from ai_trading_system.platform.artifacts import writer

    root = small_repository
    path = ".gitignore"
    expected = _stage_inputs(root)
    before = _stage_snapshot(root)
    rendered = b"outputs/\n# prior generator version\n"
    with GeneratedArtifactStage(root, expected, output_paths={path}) as first:
        writer.write_bytes_atomic(root / path, rendered)
        first.finish()
    # The next stage's authority is frozen separately using real Git plumbing.
    # It does not accept the first stage's self-reported hash as external authority.
    prior_oid = _git(root, "hash-object", "--no-filters", "--stdin", content=rendered)
    next_expected = {**expected, path: {**expected[path], "oid": prior_oid}}
    with GeneratedArtifactStage(
        root, next_expected, output_paths=set(), prior_outputs=first.outputs
    ) as second:
        assert (root / path).read_bytes() == rendered
        summary = second.finish()
    observed = second.reads[path]
    assert observed["origin"] == "PRIOR_GENERATOR_OUTPUT"
    assert observed["object"] == next_expected[path]
    assert observed["sha256"] == hashlib.sha256(rendered).hexdigest()
    assert second.outputs == {}
    assert summary["materialization_allowed"] is False
    assert _stage_snapshot(root) == before


def test_generated_stage_junction_swap_rejected_before_handle_read(
    small_repository: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import msvcrt

    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_integration import GeneratedArtifactStage

    root = small_repository
    directory = root / "frozen-inputs"
    directory.mkdir()
    target = directory / "input.txt"
    target.write_bytes(b"reviewed input\n")
    _git(root, "add", "--", "frozen-inputs/input.txt")
    expected = _stage_inputs(root)
    outside = tmp_path / "outside-inputs"
    outside.mkdir()
    secret = outside / "input.txt"
    secret.write_bytes(b"unreviewed private content\n")
    preserved = root / "retained-frozen-inputs"
    real_dll = ctypes.WinDLL
    real_fd = msvcrt.open_osfhandle
    swapped = []
    escaped = []
    converted = []

    def open_after_swap(native, path, *args):
        if os.path.normcase(os.fspath(path)) == os.path.normcase(str(target)) and not swapped:
            # Exact fixture directories; move only after validating the resolved targets.
            assert directory.resolve().parent == root.resolve()
            assert preserved.absolute().parent == root.resolve()
            directory.rename(preserved)
            created = subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(directory), str(outside)],
                capture_output=True,
                timeout=10,
            )
            assert created.returncode == 0, created.stderr
            swapped.append(True)
            handle = native(path, *args)
            escaped.append(int(handle))
            return handle
        return native(path, *args)

    class RacingKernel:
        def __init__(self, native):
            self.native = native
            self.CreateFileW = _ObservedNativeCall(native.CreateFileW, open_after_swap)

        def __getattr__(self, name):
            return getattr(self.native, name)

    def observe_fd(handle, *args):
        if int(handle) in escaped:
            converted.append(handle)
            raise AssertionError("escaped junction handle reached content stream")
        return real_fd(handle, *args)

    try:
        with monkeypatch.context() as hooks:
            hooks.setattr(ctypes, "WinDLL", lambda *a, **k: RacingKernel(real_dll(*a, **k)))
            hooks.setattr(msvcrt, "open_osfhandle", observe_fd)
            with pytest.raises(WorkflowContractError, match="HANDLE_PATH_CHANGED"):
                with GeneratedArtifactStage(root, expected, output_paths=set()):
                    target.read_bytes()
        assert swapped == [True]
        assert len(escaped) == 1
        assert converted == []
        assert secret.read_bytes() == b"unreviewed private content\n"
        assert (preserved / "input.txt").read_bytes() == b"reviewed input\n"
    finally:
        if swapped:
            # Remove only the exact fixture junction, never traverse its external target.
            assert directory.parent.resolve() == root.resolve()
            assert directory.lstat().st_file_attributes & 0x400
            directory.rmdir()
            preserved.rename(directory)


@pytest.mark.parametrize("mutation", ["raw_bytes", "index_mode"])
def test_generated_stage_rejects_external_object_drift(
    small_repository: Path, mutation: str
) -> None:
    from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
    from ai_trading_system.platform.architecture.workflow_integration import GeneratedArtifactStage

    root = small_repository
    expected = _stage_inputs(root)
    if mutation == "raw_bytes":
        (root / ".gitignore").write_bytes(b"changed outside frozen request\n")
    else:
        _git(root, "update-index", "--chmod=+x", "--", ".gitignore")
    before = _stage_snapshot(root)
    with pytest.raises(WorkflowContractError):
        with GeneratedArtifactStage(root, expected, output_paths=set()):
            (root / ".gitignore").read_bytes()
    assert _stage_snapshot(root) == before


class NativeOracle:
    """Read actual kernel objects independently of workflow_execution helpers."""

    def __init__(self) -> None:
        from ctypes import wintypes

        self.api = ctypes.WinDLL("kernel32", use_last_error=True)
        self.api.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        self.api.OpenProcess.restype = wintypes.HANDLE
        self.api.GetProcessTimes.argtypes = [wintypes.HANDLE] + [
            ctypes.POINTER(wintypes.FILETIME)
        ] * 4
        self.api.GetProcessTimes.restype = wintypes.BOOL
        self.api.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        self.api.WaitForSingleObject.restype = wintypes.DWORD
        self.api.CloseHandle.argtypes = [wintypes.HANDLE]
        self.api.CloseHandle.restype = wintypes.BOOL
        self.api.OpenJobObjectW.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.LPCWSTR]
        self.api.OpenJobObjectW.restype = wintypes.HANDLE
        self.api.IsProcessInJob.argtypes = [
            wintypes.HANDLE,
            wintypes.HANDLE,
            ctypes.POINTER(wintypes.BOOL),
        ]
        self.api.IsProcessInJob.restype = wintypes.BOOL
        self.api.CreateEventW.argtypes = [
            ctypes.c_void_p,
            wintypes.BOOL,
            wintypes.BOOL,
            wintypes.LPCWSTR,
        ]
        self.api.CreateEventW.restype = wintypes.HANDLE
        self.api.SetHandleInformation.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.DWORD]
        self.api.SetHandleInformation.restype = wintypes.BOOL

    @contextmanager
    def process(self, pid: int) -> Iterator[int]:
        handle = self.api.OpenProcess(0x00100000 | 0x1000, False, pid)
        assert handle, (
            f"cannot independently observe synthetic PID {pid}: {ctypes.get_last_error()}"
        )
        try:
            yield handle
        finally:
            assert self.api.CloseHandle(handle)

    def creation_time(self, handle: int) -> int:
        from ctypes import wintypes

        values = [wintypes.FILETIME() for _ in range(4)]
        assert self.api.GetProcessTimes(handle, *(ctypes.byref(value) for value in values))
        return (values[0].dwHighDateTime << 32) | values[0].dwLowDateTime

    def exited(self, handle: int, timeout: float = 0) -> bool:
        result = self.api.WaitForSingleObject(handle, int(timeout * 1000))
        assert result in {0, 258}, f"unexpected native wait result {result}"
        return result == 0

    def assert_job_absent(self, name: str) -> None:
        handle = self.api.OpenJobObjectW(0x0004, False, name)
        if handle:
            self.api.CloseHandle(handle)
            pytest.fail("observer created or retained an unexpected Job object")
        assert ctypes.get_last_error() == 2, "absence must not be inferred from access failure"

    def assert_in_job(self, process: int, name: str) -> None:
        from ctypes import wintypes

        job = self.api.OpenJobObjectW(0x0004, False, name)
        assert job, f"cannot independently open live Job: {ctypes.get_last_error()}"
        try:
            member = wintypes.BOOL()
            assert self.api.IsProcessInJob(process, job, ctypes.byref(member))
            assert member.value, "live synthetic worker is outside its named Job"
        finally:
            assert self.api.CloseHandle(job)


@pytest.fixture
def execution_api() -> Any:
    assert os.name == "nt" and sys.version_info[:2] == (3, 11), (
        "INSUFFICIENT: mandatory execution acceptance requires actual Windows/Python 3.11"
    )
    try:
        module = importlib.import_module(
            "ai_trading_system.platform.architecture.workflow_execution"
        )
    except ImportError as exc:
        pytest.fail(f"INSUFFICIENT: execution API is not ready; this is not baseline red: {exc}")
    for name in (
        "WindowsJobProcess",
        "ExecutionContainmentError",
        "observe_process",
        "observe_job",
    ):
        assert hasattr(module, name), f"INSUFFICIENT: required API {name} is unavailable"
    return module


@pytest.fixture
def native(execution_api: Any) -> NativeOracle:
    return NativeOracle()


def _environment() -> dict[str, str]:
    result = {
        key: value for key, value in os.environ.items() if not key.startswith(("GIT_", "PYTEST_"))
    }
    result["PYTHONPATH"] = str(ROOT / "src")
    result["PYTHONUNBUFFERED"] = "1"
    return result


def _until(predicate: Callable[[], Any], *, description: str, timeout: float = DEADLINE) -> Any:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        result = predicate()
        if result:
            return result
        time.sleep(0.02)
    pytest.fail(f"synthetic process deadline: {description}")


def _read_json(path: Path) -> Any:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None  # The writer may still be completing its bounded record.


def _job_name() -> str:
    return "Local\\AITS-DEVX015-test-" + uuid.uuid4().hex


def _create(api: Any, root: Path, source: str, *, name: str | None = None) -> Any:
    return api.WindowsJobProcess.create(
        argv=[sys.executable, "-u", "-c", source],
        cwd=root,
        environment=_environment(),
        stdout_path=root / "stdout.log",
        job_name=name or _job_name(),
    )


# Every synthetic worker has its own hard deadline even if the test controller
# fails. Files define interleavings; sleep only backs off within a bounded wait.
CHILD = """
import json, os, time
from pathlib import Path
Path('child.json').write_text(json.dumps({'pid': os.getpid()}), encoding='utf-8')
end = time.monotonic() + 60
while not Path('child.release').exists() and time.monotonic() < end:
    time.sleep(.02)
Path('child.done').write_text('exited', encoding='utf-8')
"""


def _parent_source(*, stay_alive: bool) -> str:
    return f"""
import json, os, subprocess, sys, time
from pathlib import Path
child = subprocess.Popen([sys.executable, '-u', '-c', {CHILD!r}])
Path('parent.json').write_text(
    json.dumps({{'pid': os.getpid(), 'child': child.pid}}), encoding='utf-8')
end = time.monotonic() + 60
while not Path('child.json').exists() and time.monotonic() < end:
    time.sleep(.02)
print('PARENT_READY', flush=True)
if {stay_alive!r}:
    while not Path('parent.release').exists() and time.monotonic() < end:
        time.sleep(.02)
"""


@pytest.mark.parametrize("mode", [
    "exit", "close-suspended", "terminate", "forged-custody", "closed-custody",
    "exit-before-resume",
])
def test_inherited_child_preserves_parent_job_and_directory_custody(
    tmp_path: Path, execution_api: Any, native: NativeOracle, mode: str,
) -> None:
    name = _job_name()
    protected = tmp_path / "protected"
    protected.mkdir()
    source = f"""
import json,os,subprocess,sys,threading,time
from pathlib import Path
from ai_trading_system.platform.architecture.workflow_execution import (
    InheritedJobChild,WindowsJobProcess,ExecutionContainmentError)
from ai_trading_system.platform.architecture.workflow_contract import hold_bound_directory
root=Path.cwd(); mode={mode!r}; job={name!r}
def until(predicate):
    end=time.monotonic()+60
    while not predicate():
        if time.monotonic()>end: raise RuntimeError('owned worker deadline')
        time.sleep(.02)
sibling=subprocess.Popen([sys._base_executable,'-u','-c',{CHILD.replace('child.', 'sibling.')!r}])
until(lambda: Path('sibling.json').exists())
directory=root/'protected'; parent=root.stat(); leaf=directory.stat()
child=None; rejection=None; wrong_thread=[]; pre_resume=None
with hold_bound_directory(root,'protected',expected_identity=(leaf.st_dev,leaf.st_ino),
    expected_root_identity=(parent.st_dev,parent.st_ino),
    expected_parent_identities={{}}) as custody:
    offered=object() if mode=='forged-custody' else custody
    if mode=='closed-custody': custody.close()
    try:
        child=InheritedJobChild.create(argv=[sys._base_executable,'-u','-c',{CHILD!r}],
            cwd=root,environment=dict(os.environ),stdout_path=root/'inherited.stdout.log',
            job_name=job,directory_custody=offered)
    except (ExecutionContainmentError,ValueError) as exc:
        if mode not in ('forged-custody','closed-custody'): raise
        rejection=str(exc)
if child is not None:
    assert not isinstance(child,WindowsJobProcess)
    pre_resume=child.pre_resume_binding()
    assert child.pre_resume_binding()==pre_resume
    def other_thread():
        try: child.resume()
        except ExecutionContainmentError as exc: wrong_thread.append(exc.code)
        try: child.pre_resume_binding()
        except ExecutionContainmentError as exc: wrong_thread.append(exc.code)
    thread=threading.Thread(target=other_thread); thread.start(); thread.join()
    assert wrong_thread==['WORKFLOW_EXECUTION_HANDLE_OWNER']*2
Path('inherited-ready.json').write_text(json.dumps({{
    'worker_pid':os.getpid(),'sibling_pid':sibling.pid,'rejection':rejection,
    'identity':child.identity() if child else None,
    'launch_binding':child.launch_binding() if child else None,'wrong_thread':wrong_thread,
    'pre_resume':pre_resume}}))
until(lambda: Path('controller.release').exists())
if child is not None:
    if mode=='close-suspended': child.close()
    elif mode=='exit-before-resume':
        assert child.terminate_process()==1067
        try: child.pre_resume_binding()
        except ExecutionContainmentError as exc:
            assert exc.code=='WORKFLOW_EXECUTION_PRE_RESUME_NOT_LIVE'
        else: raise AssertionError('exited child pre-resume snapshot admitted')
        child.close()
    else:
        child.resume()
        try: child.pre_resume_binding()
        except ExecutionContainmentError as exc:
            assert exc.code=='WORKFLOW_EXECUTION_PRE_RESUME_REQUIRED'
        else: raise AssertionError('resumed child pre-resume snapshot admitted')
        try: child.resume()
        except ExecutionContainmentError as exc:
            assert exc.code=='WORKFLOW_EXECUTION_ALREADY_RESUMED'
        else: raise AssertionError('second resume admitted')
        until(lambda: Path('child.json').exists())
        if mode=='terminate': assert child.terminate_process()==1067
        else:
            Path('child.release').write_text('release')
            assert child.wait_exit(timeout=180)==0
        child.close()
    try: child.pre_resume_binding()
    except ExecutionContainmentError as exc:
        assert exc.code=='WORKFLOW_EXECUTION_HANDLE_OWNER'
    else: raise AssertionError('closed child pre-resume snapshot admitted')
assert sibling.poll() is None
Path('inherited-completed.json').write_text(json.dumps({{'worker_pid':os.getpid(),
    'sibling_pid':sibling.pid,'original_job_not_terminated':True}}))
until(lambda: Path('final.release').exists())
Path('sibling.release').write_text('release')
assert sibling.wait(timeout=180)==0
"""
    handle = _create(execution_api, tmp_path, source, name=name)
    try:
        handle.resume()
        _until(lambda: (tmp_path / "inherited-ready.json").exists() or handle.poll() is not None,
               description="original worker creates a bound suspended child")
        ready = _read_json(tmp_path / "inherited-ready.json")
        assert ready, (tmp_path / "stdout.log").read_text()
        assert not (tmp_path / "child.json").exists()
        with native.process(ready["worker_pid"]) as worker, native.process(
            ready["sibling_pid"],
        ) as sibling:
            native.assert_in_job(worker, name)
            native.assert_in_job(sibling, name)
            if mode in {"forged-custody", "closed-custody"}:
                assert ready["identity"] is None and ready["rejection"]
                assert not (tmp_path / "inherited.stdout.log").exists()
            else:
                identity = ready["identity"]
                assert identity["scope"] == "INHERITED_JOB_CHILD"
                pre_resume = ready["pre_resume"]
                assert pre_resume["schema_version"] == "workflow_inherited_child_pre_resume.v2"
                assert pre_resume["read_file_custodies"] == []
                assert pre_resume["owner_resume_state"] == "NOT_RESUMED"
                assert pre_resume["job_name"] == name
                assert pre_resume["worker_process"] == {
                    "pid": ready["worker_pid"], "creation_time": native.creation_time(worker),
                }
                assert pre_resume["process"] == {
                    "pid": identity["pid"], "creation_time": identity["creation_time"],
                }
                assert pre_resume["launch_binding"] == ready["launch_binding"]
                assert pre_resume["dispatch_allowed"] is False
                assert pre_resume["publication_allowed"] is False
                with native.process(identity["pid"]) as child:
                    assert native.creation_time(child) == identity["creation_time"]
                    native.assert_in_job(child, name)
                    assert not native.exited(child)
                # Parent custody has closed; suspended native child alone retains the directory.
                with pytest.raises(OSError) as denied:
                    protected.rename(tmp_path / "protected-moved")
                assert denied.value.winerror in {5, 32}
            (tmp_path / "controller.release").write_text("release")
            _until(lambda: (tmp_path / "inherited-completed.json").exists()
                   or handle.poll() is not None,
                   description="child close preserves worker and sibling")
            completed = _read_json(tmp_path / "inherited-completed.json")
            assert completed, (tmp_path / "stdout.log").read_text()
            assert completed["original_job_not_terminated"]
            assert not native.exited(worker) and not native.exited(sibling)
            if ready["identity"]:
                observed = execution_api.observe_process(
                    pid=ready["identity"]["pid"], creation_time=ready["identity"]["creation_time"],
                )
                assert observed["state"] in {"EXITED", "REUSED"}
            protected.rename(tmp_path / "protected-moved")
            (tmp_path / "protected-moved").rename(protected)
            (tmp_path / "final.release").write_text("release")
            assert handle.wait(timeout=DEADLINE) == 0
            assert native.exited(worker) and native.exited(sibling)
        if mode in {"close-suspended", "forged-custody", "closed-custody", "exit-before-resume"}:
            assert not (tmp_path / "child.json").exists()
    finally:
        handle.close()
    native.assert_job_absent(name)


def _independent_file_custody_probe(action, target, other):
    source = (
        "import json,sys\nfrom pathlib import Path\n"
        "action,target,other=sys.argv[1],Path(sys.argv[2]),Path(sys.argv[3])\n"
        "try:\n"
        " if action=='read': result={'bytes_hex':target.read_bytes().hex()}\n"
        " elif action=='write': target.write_bytes(b'forbidden write'); result={'changed':True}\n"
        " elif action=='replace': other.replace(target); result={'changed':True}\n"
        " else: target.rename(other); result={'changed':True}\n"
        "except OSError as exc: result={'error':type(exc).__name__,"
        "'errno':exc.errno,'winerror':getattr(exc,'winerror',None)}\n"
        "print(json.dumps(result))\n"
    )
    result = subprocess.run(
        [sys.executable, "-B", "-c", source, action, str(target), str(other)],
        cwd=target.anchor, capture_output=True, text=True, timeout=DEADLINE,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


@pytest.mark.parametrize("fault", [
    "none", "root", "parent", "file", "content", "hardlink", "missing", "body-exception",
])
@pytest.mark.parametrize("allow_parent_updates", [False, True])
def test_bound_read_file_custody_preserves_namespace_and_bytes(
    tmp_path, fault, allow_parent_updates,
):
    from ai_trading_system.platform.architecture.workflow_contract import (
        WorkflowContractError,
        hold_bound_read_file,
    )

    root = tmp_path / "owned-root"
    parent = root / "hooks"
    parent.mkdir(parents=True)
    target = parent / "reference-transaction"
    expected = b"#!/bin/sh\n# original hook\n"
    target.write_bytes(expected)
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"replacement canary")
    def identity(path):
        metadata = path.stat()
        return metadata.st_dev, metadata.st_ino
    identities = {"root": identity(root), "parent": identity(parent), "file": identity(target)}
    before = target.stat()
    if fault in identities:
        identities[fault] = (identities[fault][0], identities[fault][1] + 1)
    if fault == "hardlink":
        os.link(target, parent / "other-link")
    if fault == "missing":
        target.rename(parent / "retained-original")
    observations = []
    def exercise():
        with hold_bound_read_file(
            root, "hooks/reference-transaction",
            expected=b"wrong content" if fault == "content" else expected,
            expected_identity=identities["file"], expected_root_identity=identities["root"],
            expected_parent_identities={"hooks": identities["parent"]},
            allow_parent_updates=allow_parent_updates,
        ) as custody:
            assert fault in {"none", "body-exception"}
            binding = custody.binding()
            assert binding["identity"] == list(identities["file"])
            assert binding["sha256"] == hashlib.sha256(expected).hexdigest()
            assert binding["size_bytes"] == len(expected)
            assert custody.binding() == binding
            assert _independent_file_custody_probe("read", target, replacement) == {
                "bytes_hex": expected.hex(),
            }
            for action, path, other in (
                ("write", target, replacement), ("replace", target, replacement),
                ("rename", target, parent / "moved-hook"),
                ("rename", parent, root / "moved-hooks"),
                ("rename", root, tmp_path / "moved-root"),
            ):
                observed = _independent_file_custody_probe(action, path, other)
                observations.append({"action": action, "path": str(path), **observed})
                assert observed.get("errno") == 13 or observed.get("winerror") in {5, 32}, observed
            assert target.read_bytes() == expected
            assert target.stat().st_ino == before.st_ino
            assert target.stat().st_mtime_ns == before.st_mtime_ns
            if allow_parent_updates:
                # Sibling atomic receipt replacement is compatible with custody;
                # all leaf/ancestor mutation probes above must still be denied.
                sibling = parent / "receipt.tmp"
                sibling.write_bytes(b"receipt")
                sibling.replace(parent / "receipt.json")
                assert (parent / "receipt.json").read_bytes() == b"receipt"
            if fault == "body-exception":
                raise RuntimeError("owned read custody body failure")
        with pytest.raises(WorkflowContractError, match="READ_FILE_CUSTODY_OWNER"):
            custody.binding()
    if fault == "none":
        exercise()
    else:
        expected_error = (
            RuntimeError if fault == "body-exception" else (WorkflowContractError, OSError)
        )
        with pytest.raises(expected_error):
            exercise()
    (tmp_path / "file-custody-observations.json").write_text(json.dumps(observations))
    if fault != "missing":
        assert target.read_bytes() == expected
        target.write_bytes(expected)  # Confirms every rejection/body-exit released its handles.
        moved = parent / "released-hook"
        target.rename(moved)
        moved.rename(target)
    parent.rename(root / "released-hooks")
    (root / "released-hooks").rename(parent)
    root.rename(tmp_path / "released-root")


def test_inherited_child_native_handle_count_control(
    tmp_path: Path, execution_api: Any, native: NativeOracle,
) -> None:
    """Trace actual count changes without a runtime readset or replacing native calls."""
    name = _job_name()
    source = f"NAME={name!r}\n" + """
import ctypes,gc,json,os,sys,time
from pathlib import Path
from ctypes import wintypes as w
from ai_trading_system.platform.architecture.workflow_execution import (
    WindowsJobProcess,InheritedJobChild,current_job_member)
api=ctypes.WinDLL('kernel32',use_last_error=True)
api.GetCurrentProcess.restype=w.HANDLE
api.GetProcessHandleCount.argtypes=[w.HANDLE,ctypes.POINTER(w.DWORD)]
api.GetProcessHandleCount.restype=w.BOOL
def count():
    value=w.DWORD()
    assert api.GetProcessHandleCount(api.GetCurrentProcess(),ctypes.byref(value))
    return value.value
targets={WindowsJobProcess._create.__func__.__code__,WindowsJobProcess.close.__code__,
    InheritedJobChild.pre_resume_binding.__code__}
events=[]; last=count(); cycle=0
def trace(frame,event,arg):
    global last
    if frame.f_code in targets and event in ('line','return'):
        now=count()
        if now!=last:
            events.append({'cycle':cycle,'function':frame.f_code.co_name,
                'event':event,'line':frame.f_lineno,'before':last,'after':now})
            last=now
    return trace
rows=[]
stream_rows=[]
for buffering in (0,-1):
    before=count()
    stream=open('stream-control-'+str(buffering)+'.bin','xb',buffering=buffering)
    opened=count()
    stream.close(); closed=count()
    del stream
    gc.collect()
    stream_rows.append({'buffering':buffering,'before':before,'opened':opened,
        'closed':closed,'deleted':count()})
sys.settrace(trace)
try:
    for cycle in range(3):
        row={'cycle':cycle,'before':count()}
        current_job_member(NAME); row['after_member']=count()
        child=InheritedJobChild.create(argv=[sys._base_executable,'-c','pass'],cwd=Path.cwd(),
            environment=dict(os.environ),stdout_path=Path('control-'+str(cycle)+'.stdout').absolute(),
            job_name=NAME)
        row['after_create']=count()
        child.pre_resume_binding(); row['after_binding']=count()
        child.resume(); row['after_resume']=count()
        assert child.wait_exit(timeout=30)==0
        row['after_wait']=count()
        child.close(); row['after_close']=count()
        gc.collect(); time.sleep(.05); row['after_gc']=count()
        rows.append(row)
finally:
    sys.settrace(None)
retained=count()
del child
gc.collect()
released=count()
Path('native-handle-control.json').write_text(json.dumps({'rows':rows,'events':events,
    'streams':stream_rows,'retained_child':retained,'deleted_child':released}))
"""
    handle = _create(execution_api, tmp_path, source, name=name)
    try:
        handle.resume()
        assert handle.wait(timeout=DEADLINE) == 0, (tmp_path / "stdout.log").read_text()
        result = _read_json(tmp_path / "native-handle-control.json")
        assert len(result["rows"]) == 3 and result["events"]
        # Measure first-use initialization separately; subsequent complete
        # create/bind/resume/wait/close cycles must return to that exact baseline.
        stable = result["rows"][0]["after_gc"]
        for row in result["rows"][1:]:
            assert row["before"] == row["after_close"] == row["after_gc"] == stable
        assert all(row["deleted"] == row["before"] for row in result["streams"])
        assert result["retained_child"] == result["deleted_child"]
    finally:
        handle.close()
    native.assert_job_absent(name)


@pytest.mark.parametrize("mode", ["success", "body-exception"])
def test_complete_runtime_native_custody_inherited_and_released(
    tmp_path: Path, execution_api: Any, native: NativeOracle, mode: str,
) -> None:
    name = _job_name()
    counter = """
import ctypes,json,os,sys,time
from pathlib import Path
from ctypes import wintypes as w
api=ctypes.WinDLL('kernel32',use_last_error=True)
api.GetCurrentProcess.restype=w.HANDLE
api.GetProcessHandleCount.argtypes=[w.HANDLE,ctypes.POINTER(w.DWORD)]
api.GetProcessHandleCount.restype=w.BOOL
def handle_count():
    count=w.DWORD()
    assert api.GetProcessHandleCount(api.GetCurrentProcess(),ctypes.byref(count))
    return count.value
"""
    child_source = counter + """
Path('runtime-child.json').write_text(json.dumps({'pid':os.getpid(),'handles':handle_count()}))
"""
    source = f"NAME={name!r}\nMODE={mode!r}\nCHILD_SOURCE={child_source!r}\n" + counter + """
from ai_trading_system.platform.architecture.workflow_execution import (
    InheritedJobChild,acceptance_runtime_identity,hold_acceptance_runtime_identity)
from ai_trading_system.platform.architecture.workflow_contract import (
    WorkflowContractError,canonical_digest)
from ai_trading_system.platform.architecture.workflow_coordination import directory_identity
directory_identity(Path(sys.prefix))
original=acceptance_runtime_identity()
cold=handle_count(); controls=[]
for cycle in range(3):
    before=handle_count()
    control=InheritedJobChild.create(argv=[sys._base_executable,'-c','pass'],
        cwd=Path.cwd(),environment=dict(os.environ),
        stdout_path=Path('runtime-control-'+str(cycle)+'.stdout').absolute(),job_name=NAME)
    try:
        control.pre_resume_binding()
        control.resume()
        assert control.wait_exit(timeout=30)==0
    finally:
        control.close()
    controls.append({'before':before,'after':handle_count()})
for row in controls[1:]:
    assert row['before']==row['after']==controls[0]['after'],controls
baseline=handle_count(); child=None; binding=None; raised=False; files=()
class BodyFailure(RuntimeError): pass
try:
    with hold_acceptance_runtime_identity() as (identity,files):
        assert identity==original
        assert len(files)==original['distribution_code']['file_count']+2
        held=handle_count()
        assert held>=baseline+len(files)
        if MODE=='body-exception': raise BodyFailure('original body failed')
        child=InheritedJobChild.create(argv=[sys._base_executable,'-u','-c',CHILD_SOURCE],
            cwd=Path.cwd(),environment=dict(os.environ),stdout_path=Path('runtime-child.stdout').absolute(),
            job_name=NAME,file_custodies=files)
        binding=child.pre_resume_binding()
        assert len(binding['read_file_custodies'])==len(files)
        Path('complete-runtime-binding.json').write_text(json.dumps(binding))
except BodyFailure:
    raised=True
assert raised==(MODE=='body-exception')
for custody in files:
    try: custody.binding()
    except WorkflowContractError as exc: assert exc.code=='WORKFLOW_READ_FILE_CUSTODY_OWNER'
    else: raise AssertionError('custody remained live after context exit')
ready={'file_count':len(files),'runtime':original,'parent_baseline_handles':baseline,
    'parent_cold_handles':cold,'native_child_controls':controls,
    'parent_held_handles':held,'parent_after_context_handles':handle_count(),
    'body_exception':raised,'process':binding['process'] if binding else None,
    'binding_sha256':canonical_digest(binding) if binding else None}
Path('runtime-ready.json').write_text(json.dumps(ready))
end=time.monotonic()+60
while not Path('runtime.release').exists():
    if time.monotonic()>end: raise RuntimeError('test release deadline')
    time.sleep(.02)
if child:
    child.resume()
    assert child.wait_exit(timeout=30)==0
    child.close()
after=handle_count()
Path('runtime-complete.json').write_text(json.dumps({'baseline':baseline,'after':after}))
assert after==baseline,(baseline,after)
"""
    handle = _create(execution_api, tmp_path, source, name=name)
    try:
        handle.resume()
        ready_path = tmp_path / "runtime-ready.json"
        _until(lambda: ready_path.exists() or handle.poll() is not None,
               description="complete original runtime native retention and child inheritance",
               timeout=LOADED_HOST_CLI_TIMEOUT_SECONDS)
        assert ready_path.exists(), (tmp_path / "stdout.log").read_text()
        ready = _read_json(ready_path)
        controls = ready["native_child_controls"]
        assert len(controls) == 3 and controls[0]["before"] == ready["parent_cold_handles"]
        for row in controls[1:]:
            assert row["before"] == row["after"] == ready["parent_baseline_handles"]
        assert ready["file_count"] == ready["runtime"]["distribution_code"]["file_count"] + 2
        assert ready["parent_held_handles"] >= (
            ready["parent_baseline_handles"] + ready["file_count"]
        )
        assert not (tmp_path / "runtime-child.json").exists()
        if mode == "success":
            with native.process(ready["process"]["pid"]) as child:
                native.assert_in_job(child, name)
                assert native.creation_time(child) == ready["process"]["creation_time"]
                assert not native.exited(child)
            assert (tmp_path / "complete-runtime-binding.json").is_file()
        else:
            assert ready["process"] is None and ready["body_exception"] is True
            assert ready["parent_after_context_handles"] == ready["parent_baseline_handles"]
        (tmp_path / "runtime.release").write_text("release original native child")
        assert handle.wait(timeout=DEADLINE) == 0, (tmp_path / "stdout.log").read_text()
        completed = _read_json(tmp_path / "runtime-complete.json")
        assert completed["after"] == completed["baseline"]
        if mode == "success":
            actual_child = _read_json(tmp_path / "runtime-child.json")
            assert actual_child["pid"] == ready["process"]["pid"]
            assert actual_child["handles"] >= ready["file_count"]
    finally:
        handle.close()
    native.assert_job_absent(name)


@pytest.mark.parametrize("mode", ["success", "hardlink-success", "forged", "closed"])
def test_inherited_child_retains_exact_read_file_custodies(
    tmp_path: Path, execution_api: Any, native: NativeOracle, mode: str,
) -> None:
    name = _job_name()
    protected = tmp_path / "protected"
    protected.mkdir()
    first, second = protected / "reference-transaction", protected / "request.json"
    first.write_bytes(b"fixed hook\n")
    second.write_bytes(b'{"binding":"fixed"}\n')
    alias = protected / "installed-alias"
    if mode == "hardlink-success":
        os.link(first, alias)
    child_source = (
        "from pathlib import Path\n"
        "Path('read-inputs.hex').write_text((Path('protected/reference-transaction').read_bytes()"
        "+Path('protected/request.json').read_bytes()).hex())\n" + CHILD
    )
    source = f"""
import json,os,sys,time
from pathlib import Path
from contextlib import ExitStack
from ai_trading_system.platform.architecture.workflow_execution import (
    InheritedJobChild,ExecutionContainmentError)
from ai_trading_system.platform.architecture.workflow_contract import (
    hold_bound_read_file,WorkflowContractError)
root=Path.cwd(); mode={mode!r}; child=None; rejected=None; records=[]
def identity(path):
    item=path.stat(); return item.st_dev,item.st_ino
with ExitStack() as stack:
    files=[]
    for relative in ('protected/reference-transaction','protected/request.json'):
        path=root/relative
        custody=stack.enter_context(hold_bound_read_file(root,relative,expected=path.read_bytes(),
            expected_identity=identity(path),expected_root_identity=identity(root),
            expected_parent_identities={{'protected':identity(root/'protected')}},
            expected_link_count=path.stat().st_nlink if mode=='hardlink-success' else 1))
        records.append(custody.binding()); files.append(custody)
    if mode=='closed': files[-1].close()
    try:
        child=InheritedJobChild.create(argv=[sys._base_executable,'-u','-c',{child_source!r}],
            cwd=root,environment=dict(os.environ),stdout_path=root/'child.stdout',job_name={name!r},
            file_custodies=[object()] if mode=='forged' else files)
    except (ExecutionContainmentError,WorkflowContractError) as exc:
        if mode in ('success','hardlink-success'): raise
        rejected=exc.code
    binding=child.pre_resume_binding() if child else None
Path('file-child-ready.json').write_text(json.dumps({{'binding':binding,'files':records,
    'rejected':rejected,'worker_pid':os.getpid()}}))
end=time.monotonic()+60
while not Path('file-child.release').exists():
    if time.monotonic()>end: raise RuntimeError('file child release deadline')
    time.sleep(.02)
if child:
    child.resume()
    assert child.wait_exit(timeout=30)==0
    child.close()
"""
    handle = _create(execution_api, tmp_path, source, name=name)
    try:
        handle.resume()
        ready_path = tmp_path / "file-child-ready.json"
        _until(lambda: ready_path.exists() or handle.poll() is not None,
               description="original Job worker binds inherited read-only file handles")
        assert ready_path.exists(), (tmp_path / "stdout.log").read_text()
        ready = _read_json(ready_path)
        assert not (tmp_path / "child.json").exists()
        if mode in {"success", "hardlink-success"}:
            binding = ready["binding"]
            assert binding["schema_version"] == "workflow_inherited_child_pre_resume.v2"
            assert binding["read_file_custodies"] == ready["files"]
            if mode == "hardlink-success":
                assert ready["files"][0]["schema_version"] == "workflow_read_file_custody.v2"
                assert ready["files"][0]["link_count"] == 2
            with native.process(binding["process"]["pid"]) as child:
                native.assert_in_job(child, name)
                assert native.creation_time(child) == binding["process"]["creation_time"]
                assert not native.exited(child)
            for target in (first, second, alias) if mode == "hardlink-success" else (first, second):
                observed = _independent_file_custody_probe("write", target, target)
                assert observed.get("errno") == 13 or observed.get("winerror") in {5, 32}
            with pytest.raises(OSError):
                protected.rename(tmp_path / "unexpected-moved")
            # The worker closed both custodies; only this suspended child holds them.
            (tmp_path / "child.release").write_text("release after reading fixed inputs")
        else:
            assert ready["binding"] is None and ready["rejected"]
            assert not (tmp_path / "child.stdout").exists()
        (tmp_path / "file-child.release").write_text("resume original child")
        assert handle.wait(timeout=DEADLINE) == 0, (tmp_path / "stdout.log").read_text()
        if mode in {"success", "hardlink-success"}:
            assert (tmp_path / "read-inputs.hex").read_text() == (
                first.read_bytes() + second.read_bytes()
            ).hex()
        for target in (first, second):
            target.write_bytes(target.read_bytes())
        protected.rename(tmp_path / "released-protected")
    finally:
        handle.close()
    native.assert_job_absent(name)


@pytest.mark.parametrize("existing", [False, True])
def test_inherited_child_nonmember_cannot_create_output_or_process(
    tmp_path: Path, execution_api: Any, native: NativeOracle, existing: bool,
) -> None:
    name = _job_name()
    parent = (
        _create(execution_api, tmp_path, "raise SystemExit(0)", name=name) if existing else None
    )
    try:
        with pytest.raises(execution_api.ExecutionContainmentError):
            execution_api.InheritedJobChild.create(
                argv=[sys.executable, "-c", "raise SystemExit(0)"], cwd=tmp_path,
                environment=_environment(), stdout_path=tmp_path / "forbidden.stdout",
                job_name=name,
            )
        assert not (tmp_path / "forbidden.stdout").exists()
        if parent:
            assert parent.poll() is None and parent.active_process_count() == 1
    finally:
        if parent:
            parent.close()
    native.assert_job_absent(name)


@contextmanager
def _own_primary_token(execution_api: Any) -> Iterator[tuple[Any, Any, int, str]]:
    from ctypes import wintypes as w

    api, security = execution_api._api(), execution_api._token_api()
    token = w.HANDLE()
    assert security.OpenProcessToken(api.GetCurrentProcess(), 0x000B, ctypes.byref(token))
    try:
        sid = execution_api._process_primary_token(api, api.GetCurrentProcess())["sid"]
        yield api, security, token.value, sid
    finally:
        assert api.CloseHandle(token)


@pytest.mark.parametrize("failure", [None, "logon", "missing-token", "adopt", "identity", "body"])
def test_local_worker_logon_clears_credentials_and_closes_tokens(
    execution_api: Any, monkeypatch: pytest.MonkeyPatch, failure: str | None,
) -> None:
    """Explicit native API seam: no real account logon or launch acceptance."""
    from ctypes import wintypes as w

    password = ctypes.create_unicode_buffer("synthetic-test-password")
    events: list[str] = []
    sid = "S-1-5-21-1-2-3-1010"

    def cleared() -> bool:
        return ctypes.string_at(ctypes.addressof(password), ctypes.sizeof(password)) == (
            b"\0" * ctypes.sizeof(password)
        )

    class NativeSeam:
        def LogonUserW(self, account, domain, pointer, logon_type, provider, output):
            assert (account, domain, logon_type, provider) == ("AITSWorker", ".", 2, 0)
            assert pointer.value == ctypes.addressof(password) and not cleared()
            events.append("logon")
            if failure == "logon":
                ctypes.set_last_error(1326)
                return False
            if failure == "missing-token":
                return True
            ctypes.cast(output, ctypes.POINTER(w.HANDLE))[0] = 907
            return True

        def CloseHandle(self, handle):
            assert handle.value == 907 and cleared()
            events.append("close-original")
            return True

    class AdoptedSeam:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            events.append("close-duplicate")

        def validate_launcher(self):
            events.append("identity")
            if failure == "identity":
                raise execution_api.ExecutionContainmentError("WORKER_SESSION_MISMATCH")

    def adopt(cls, handle, *, expected_sid):
        assert handle == 907 and expected_sid == sid and cleared()
        events.append("adopt")
        if failure == "adopt":
            raise execution_api.ExecutionContainmentError("WORKER_TOKEN_IDENTITY")
        return AdoptedSeam()

    native = NativeSeam()
    monkeypatch.setattr(execution_api, "_api", lambda: native)
    monkeypatch.setattr(execution_api, "_token_api", lambda: native)
    monkeypatch.setattr(execution_api.WindowsWorkerToken, "from_primary_handle", classmethod(adopt))

    def run() -> None:
        with execution_api.WindowsWorkerToken.logon_local(
            account="AITSWorker", password_buffer=password, expected_sid=sid,
        ) as worker:
            assert isinstance(worker, AdoptedSeam) and cleared()
            events.append("body")
            if failure == "body":
                raise RuntimeError("synthetic caller failure")

    if failure:
        expected = {"logon": "WORKER_LOGON", "missing-token": "WORKER_LOGON_TOKEN_MISSING",
                    "adopt": "WORKER_TOKEN_IDENTITY",
                    "identity": "WORKER_SESSION_MISMATCH", "body": "synthetic caller failure"}
        with pytest.raises((execution_api.ExecutionContainmentError, RuntimeError),
                           match=expected[failure]):
            run()
    else:
        run()
    assert cleared()
    assert events.count("logon") == 1  # No retry/fallback, even on native logon failure.
    assert events.count("close-original") == (0 if failure in {"logon", "missing-token"} else 1)
    assert events.count("close-duplicate") == (
        0 if failure in {"logon", "missing-token", "adopt"} else 1
    )
    if failure not in {"logon", "missing-token", "adopt"}:
        assert events[-2:] == ["close-duplicate", "close-original"]


@pytest.mark.parametrize("stored,fault", [
    (stored, fault) for stored in (False, True)
    for fault in (None, "host", "extra", "duplicate", "runtime", "custody", "body")
] + [(True, "secret-acl"), (True, "decrypt")])
def test_registered_worker_identity_is_parsed_under_custody_and_secret_is_cleared(
    execution_api: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fault: str | None,
    stored: bool,
) -> None:
    """Composition seams for runtime/admin/token; bounded file reading remains real."""
    from ai_trading_system.platform.architecture import workflow_coordination as coordination

    identity = {"schema_version": "devx015_worker_identity.v1", "host_id": "unit-host",
                "account": "AITSWorker", "sid": "S-1-5-21-1-2-3-1010"}
    if fault == "host":
        identity["host_id"] = "different-host"
    if fault == "extra":
        identity["password"] = "must-not-be-configured"
    raw = json.dumps(identity).encode()
    if fault == "duplicate":
        raw = raw[:-1] + b',"account":"other"}'
    path = tmp_path / "worker-identity.json"
    path.write_bytes(raw)
    password = ctypes.create_unicode_buffer("synthetic-only")
    events: list[str] = []
    held = False

    class ContextSeam:
        def assert_current(self, root, files):
            assert root == tmp_path and files == {} and held
            events.append("context")

    class AdminSeam:
        @contextmanager
        def hold_confidential_file(self, target):
            assert held and target == tmp_path / "worker-credential.dpapi"
            if fault == "secret-acl":
                raise RuntimeError("synthetic confidential ACL rejection")
            events.append("secret-hold")
            try:
                yield b"synthetic ciphertext"
            finally:
                events.append("secret-release")

        def assert_protected(self, target):
            assert target in {tmp_path, path}
            events.append("acl")

        @contextmanager
        def hold_protected_files(self, files):
            nonlocal held
            assert files == {path: hashlib.sha256(raw).hexdigest()}
            if fault == "custody":
                raise RuntimeError("synthetic custody rejection")
            held = True
            events.append("hold")
            try:
                yield
            finally:
                held = False
                events.append("release")

    def bind(root, sha, *, git_context):
        assert root == tmp_path and sha == "a" * 40 and isinstance(git_context, ContextSeam)
        events.append("runtime")
        if fault == "runtime":
            raise RuntimeError("synthetic runtime rejection")

    @contextmanager
    def logon(cls, *, account, expected_sid, password_buffer):
        assert held and account == "AITSWorker" and expected_sid == identity["sid"]
        assert password_buffer is password
        events.append("logon")
        try:
            yield "unit-token"
        finally:
            events.append("close-token")

    @contextmanager
    def decrypt(encrypted, binding):
        assert encrypted == b"synthetic ciphertext" and binding == hashlib.sha256(raw).digest()
        assert held and events[-1] == "secret-hold"
        if fault == "decrypt":
            raise RuntimeError("synthetic decrypt rejection")
        events.append("decrypt")
        try:
            yield password
        finally:
            ctypes.memset(ctypes.addressof(password), 0, ctypes.sizeof(password))
            events.append("zero")

    monkeypatch.setattr(coordination, "_WindowsEnrollmentAdministrator", AdminSeam)
    monkeypatch.setattr(coordination, "machine_host_id", lambda: "unit-host")
    monkeypatch.setattr(execution_api, "bind_protected_inspector_runtime", bind)
    monkeypatch.setattr(execution_api.sys, "executable", str(tmp_path / "runtime" / "python.exe"))
    monkeypatch.setattr(execution_api.WindowsWorkerToken, "logon_local", classmethod(logon))
    monkeypatch.setattr(execution_api, "decrypted_worker_password", decrypt)

    def run():
        with execution_api.WindowsWorkerToken.logon_registered(
            candidate_root=tmp_path, candidate_sha="a" * 40,
            git_context=ContextSeam(), password_buffer=None if stored else password,
        ) as token:
            assert token == "unit-token" and held
            if fault == "body":
                raise RuntimeError("synthetic caller failure")

    if fault:
        with pytest.raises((RuntimeError, ValueError, execution_api.ExecutionContainmentError)):
            run()
    else:
        run()
    assert not held
    if not stored or "decrypt" in events:
        assert ctypes.string_at(ctypes.addressof(password), ctypes.sizeof(password)) == (
            b"\0" * ctypes.sizeof(password)
        )
    assert events.count("runtime") == 1
    if fault in {"host", "extra", "duplicate", "runtime", "custody", "secret-acl", "decrypt"}:
        assert "logon" not in events
    else:
        assert events.index("hold") < events.index("logon")
        assert events.index("close-token") < events.index("release")
        if stored:
            assert events.index("close-token") < events.index("zero")
            assert events.index("zero") < events.index("secret-release")
            assert events.index("secret-release") < events.index("release")


@pytest.mark.parametrize("invalid", ["account", "sid", "embedded-nul", "unterminated", "string"])
def test_local_worker_logon_rejects_invalid_inputs_before_native_api(
    execution_api: Any, monkeypatch: pytest.MonkeyPatch, invalid: str,
) -> None:
    password: Any = ctypes.create_unicode_buffer("synthetic")
    account, sid = "AITSWorker", "S-1-5-21-1-2-3-1010"
    if invalid == "account":
        account = "DOMAIN\\AITSWorker"
    elif invalid == "sid":
        sid = "S-1-5-18"
    elif invalid == "embedded-nul":
        password[1] = "\0"
    elif invalid == "unterminated":
        password[-1] = "x"
    else:
        password = "immutable-test-string"
    monkeypatch.setattr(execution_api, "_api", lambda: pytest.fail("native API must not open"))
    with pytest.raises(execution_api.ExecutionContainmentError, match="WORKER_"):
        with execution_api.WindowsWorkerToken.logon_local(
            account=account, password_buffer=password, expected_sid=sid,
        ):
            pytest.fail("invalid credential inputs must not yield")
    if invalid != "string":
        assert ctypes.string_at(ctypes.addressof(password), ctypes.sizeof(password)) == (
            b"\0" * ctypes.sizeof(password)
        )


def test_worker_token_owns_noninheritable_duplicate_and_binds_live_owner(
    execution_api: Any,
) -> None:
    from ctypes import wintypes as w

    with _own_primary_token(execution_api) as (api, security, token, sid):
        worker = execution_api.WindowsWorkerToken.from_primary_handle(token, expected_sid=sid)
    # The original handle is closed; the capability owns its independent token.
    try:
        duplicate = worker._checked_handle()
        binding = worker.binding()
        assert binding == {"sid": sid, "elevated": False,
                           "session_id": execution_api._token_dword(security, duplicate, 12)}
        binding["sid"] = "tampered"
        assert worker.binding()["sid"] == sid
        with pytest.raises(execution_api.ExecutionContainmentError,
                           match="WORKER_PRINCIPAL_NOT_SEPARATE"):
            worker.validate_launcher()
        assert execution_api._token_dword(security, duplicate, 8) == 1
        api.GetHandleInformation.argtypes = [w.HANDLE, ctypes.POINTER(w.DWORD)]
        api.GetHandleInformation.restype = w.BOOL
        flags = w.DWORD()
        assert api.GetHandleInformation(duplicate, ctypes.byref(flags))
        assert not flags.value & 1
        errors = []

        def other_thread() -> None:
            for action in (worker._checked_handle, worker.close):
                try:
                    action()
                except execution_api.ExecutionContainmentError as exc:
                    errors.append(exc.code)

        thread = threading.Thread(target=other_thread)
        thread.start()
        thread.join(timeout=DEADLINE)
        assert not thread.is_alive()
        assert errors == ["WORKFLOW_EXECUTION_WORKER_TOKEN_OWNER"] * 2
        assert worker._checked_handle() == duplicate
    finally:
        worker.close()
    worker.close()  # Owner close is idempotent.
    with pytest.raises(execution_api.ExecutionContainmentError, match="WORKER_TOKEN_OWNER"):
        worker._checked_handle()


def test_worker_token_rejects_wrong_sid_impersonation_and_does_not_leak(execution_api: Any) -> None:
    from ctypes import wintypes as w

    with _own_primary_token(execution_api) as (api, security, token, sid):
        api.GetProcessHandleCount.argtypes = [w.HANDLE, ctypes.POINTER(w.DWORD)]
        api.GetProcessHandleCount.restype = w.BOOL
        gc.collect()  # Unreferenced handles from earlier tests must not be freed mid-check.
        before, after = w.DWORD(), w.DWORD()
        assert api.GetProcessHandleCount(api.GetCurrentProcess(), ctypes.byref(before))
        for _ in range(16):
            with pytest.raises(
                execution_api.ExecutionContainmentError, match="WORKER_TOKEN_IDENTITY",
            ):
                execution_api.WindowsWorkerToken.from_primary_handle(
                    token, expected_sid="S-1-5-21-9-9-9-9999",
                )
        assert api.GetProcessHandleCount(api.GetCurrentProcess(), ctypes.byref(after))
        assert after.value == before.value
        impersonation = w.HANDLE()
        assert security.DuplicateTokenEx(token, 0x000B, None, 2, 2, ctypes.byref(impersonation))
        try:
            with pytest.raises(
                execution_api.ExecutionContainmentError, match="WORKER_PRIMARY_TOKEN_REQUIRED",
            ):
                execution_api.WindowsWorkerToken.from_primary_handle(
                    impersonation.value, expected_sid=sid,
                )
        finally:
            assert api.CloseHandle(impersonation)
        assert api.GetProcessHandleCount(api.GetCurrentProcess(), ctypes.byref(after))
        assert after.value == before.value


@pytest.mark.parametrize("case", ["none", "serialized", "same-principal", "closed"])
def test_worker_launch_rejects_missing_stale_or_same_principal_before_effects(
    tmp_path: Path, execution_api: Any, native: NativeOracle, case: str,
) -> None:
    with _own_primary_token(execution_api) as (_, _, token, sid):
        with execution_api.WindowsWorkerToken.from_primary_handle(
            token, expected_sid=sid,
        ) as worker:
            supplied = worker
            error = "WORKER_PRINCIPAL_NOT_SEPARATE"
            if case == "closed":
                worker.close()
                error = "WORKER_TOKEN_OWNER"
            elif case in {"none", "serialized"}:
                supplied = None if case == "none" else {"sid": sid, "handle": token}
                error = "WORKER_TOKEN_CAPABILITY_REQUIRED"
            name = _job_name()
            with pytest.raises(execution_api.ExecutionContainmentError, match=error):
                execution_api.WindowsJobProcess.create_as_worker(
                    worker_token=supplied, argv=[sys.executable, "-c", "raise SystemExit(0)"],
                    cwd=tmp_path, environment=_environment(), stdout_path=tmp_path / "stdout.log",
                    job_name=name,
                )
            native.assert_job_absent(name)
            assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("mode", ["success", "privilege-missing"])
def test_worker_backend_native_restricted_token_abi_with_modeled_broker_identity(
    tmp_path: Path, execution_api: Any, native: NativeOracle, monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    """Native AsUser/Job/stdio proof only; distinct-account broker identity is modeled."""
    from ctypes import wintypes as w

    with _own_primary_token(execution_api) as (api, security, token, sid):
        security.CreateRestrictedToken.argtypes = [
            w.HANDLE, w.DWORD, w.DWORD, ctypes.c_void_p, w.DWORD, ctypes.c_void_p,
            w.DWORD, ctypes.c_void_p, ctypes.POINTER(w.HANDLE),
        ]
        security.CreateRestrictedToken.restype = w.BOOL
        restricted = w.HANDLE()
        assert security.CreateRestrictedToken(token, 1, 0, None, 0, None, 0, None,
                                              ctypes.byref(restricted))
        try:
            worker = execution_api.WindowsWorkerToken.from_primary_handle(
                restricted.value, expected_sid=sid,
            )
        finally:
            assert api.CloseHandle(restricted)
    original = execution_api._process_primary_token
    observed = []

    def broker_observation(kernel: Any, process: int) -> dict[str, object]:
        value = original(kernel, process)
        observed.append(value)
        if len(observed) == 1:
            return {"sid": "S-1-5-21-9-9-9-9999", "elevated": False}
        return value

    monkeypatch.setattr(execution_api, "_process_primary_token", broker_observation)
    name = _job_name()
    launch = {
        "worker_token": worker,
        "argv": [sys._base_executable, "-c", "print('AS_USER_NATIVE')"],
        "cwd": tmp_path, "environment": _environment(),
        "stdout_path": tmp_path / "stdout.log", "job_name": name,
    }
    try:
        if mode == "privilege-missing":
            attempted = []

            def privilege_missing(*args: Any) -> int:
                attempted.append(args[0])
                ctypes.set_last_error(1314)
                return 0

            monkeypatch.setattr(worker._security, "CreateProcessAsUserW", privilege_missing)
            with pytest.raises(execution_api.ExecutionContainmentError, match="winerror=1314"):
                with execution_api.WindowsJobProcess.create_as_worker(**launch):
                    pytest.fail("must not fall back to inherited caller identity")
            assert attempted == [worker._checked_handle()]
            assert len(observed) == 1  # No process was returned to observe or resume.
            assert (tmp_path / "stdout.log").read_bytes() == b""
            native.assert_job_absent(name)
            return
        with execution_api.WindowsJobProcess.create_as_worker(**launch) as child:
            assert observed == [{"sid": sid, "elevated": False}] * 2
            with native.process(child.identity()["pid"]) as process:
                native.assert_in_job(process, name)
                assert not native.exited(process)
                assert (tmp_path / "stdout.log").read_bytes() == b""
                # No live token handle is retained by the suspended child/launcher result.
                worker.close()
                child.resume()
                assert child.wait(timeout=DEADLINE) == 0
                assert native.exited(process)
        native.assert_job_absent(name)
        assert (tmp_path / "stdout.log").read_text().strip() == "AS_USER_NATIVE"
    finally:
        worker.close()


def test_primary_token_observation_matches_independent_dotnet_and_closes_handles(
    execution_api: Any,
) -> None:
    from ctypes import wintypes as w

    api = execution_api._api()
    api.GetProcessHandleCount.argtypes = [w.HANDLE, ctypes.POINTER(w.DWORD)]
    api.GetProcessHandleCount.restype = w.BOOL
    process = api.GetCurrentProcess()
    expected = json.loads(subprocess.check_output([
        str(Path(os.environ["SystemRoot"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"),
        "-NoProfile", "-NonInteractive", "-Command",
        "$i=[Security.Principal.WindowsIdentity]::GetCurrent();"
        "@{sid=$i.User.Value;elevated=([Security.Principal.WindowsPrincipal]::new($i))."
        "IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)}"
        "|ConvertTo-Json -Compress",
    ], text=True, timeout=DEADLINE))
    execution_api._process_primary_token(api, process)  # Warm library loading before leak check.
    # Other tests on this worker may leave unreferenced handles that a collection frees mid-check.
    gc.collect()
    before, after = w.DWORD(), w.DWORD()
    assert api.GetProcessHandleCount(process, ctypes.byref(before))
    for _ in range(32):
        assert execution_api._process_primary_token(api, process) == expected
    assert api.GetProcessHandleCount(process, ctypes.byref(after))
    assert after.value == before.value
    with pytest.raises(execution_api.ExecutionContainmentError, match="OPEN_PROCESS_TOKEN"):
        execution_api._process_primary_token(api, 0)
    assert api.GetProcessHandleCount(process, ctypes.byref(after))
    assert after.value == before.value


@pytest.mark.parametrize("observed", [
    {"sid": "S-1-5-21-1-2-3-1001", "elevated": True},
    {"sid": "S-1-5-18", "elevated": False},
    {"sid": "S-1-5-19", "elevated": False},
    {"sid": "S-1-5-20", "elevated": False},
])
def test_privileged_token_observation_rejects_before_job_or_stdout(
    tmp_path: Path, execution_api: Any, native: NativeOracle,
    monkeypatch: pytest.MonkeyPatch, observed: dict[str, object],
) -> None:
    """Injected observer result covers the guard; not an administrator-run claim."""
    name = _job_name()
    monkeypatch.setattr(execution_api, "_process_primary_token", lambda *_: observed)
    with pytest.raises(
        execution_api.ExecutionContainmentError, match="PRIVILEGED_CANDIDATE_LAUNCH",
    ):
        _create(execution_api, tmp_path, "raise SystemExit(0)", name=name)
    assert list(tmp_path.iterdir()) == []
    native.assert_job_absent(name)


def test_changed_suspended_child_token_closes_real_process_without_resuming(
    tmp_path: Path, execution_api: Any, native: NativeOracle, monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = execution_api._process_primary_token
    observed = []
    held = None
    api = execution_api._api()

    def read_token(kernel: Any, process: int) -> dict[str, object]:
        nonlocal held
        value = original(kernel, process)
        observed.append(value)
        if len(observed) == 2:
            # Keep an independent process handle to verify actual failed-launch cleanup.
            pid = kernel.GetProcessId(process)
            held = native.api.OpenProcess(0x00100000 | 0x1000, False, pid)
            assert held
            return {**value, "sid": "S-1-5-21-9-9-9-9999"}
        return value

    monkeypatch.setattr(execution_api, "_process_primary_token", read_token)
    name = _job_name()
    try:
        with pytest.raises(
            execution_api.ExecutionContainmentError, match="CANDIDATE_TOKEN_CHANGED",
        ):
            _create(
                execution_api, tmp_path, "from pathlib import Path;Path('ran').touch()", name=name,
            )
        assert len(observed) == 2 and observed[0] == observed[1]
        assert held and native.exited(held)
        assert not (tmp_path / "ran").exists()
        assert (tmp_path / "stdout.log").read_bytes() == b""
        native.assert_job_absent(name)
    finally:
        if held:
            assert api.CloseHandle(held)


def test_suspended_launch_identity_resume_stdout_and_single_resume(
    tmp_path: Path, execution_api: Any, native: NativeOracle
) -> None:
    source = """
import json, os, time
from pathlib import Path
Path('canary.json').write_text(json.dumps({'cwd':os.getcwd(), 'pid':os.getpid()}), encoding='utf-8')
print('STDOUT_CANARY', flush=True)
end = time.monotonic() + 60
while not Path('canary.release').exists() and time.monotonic() < end:
    time.sleep(.02)
"""
    handle = _create(execution_api, tmp_path, source)
    try:
        identity = handle.identity()
        original_binding = handle.launch_binding()
        exported_binding = handle.launch_binding()
        exported_binding["argv"].append("caller-only-mutation")
        assert handle.launch_binding() == original_binding
        assert set(identity) >= {
            "pid",
            "creation_time",
            "job_name",
            "launcher_pid",
            "launcher_creation_time",
        }
        assert identity["launcher_pid"] == os.getpid()
        (tmp_path / "observed_identity.json").write_text(json.dumps(identity), encoding="utf-8")
        with native.process(identity["pid"]) as process, native.process(os.getpid()) as launcher:
            assert int(identity["creation_time"]) == native.creation_time(process)
            assert int(identity["launcher_creation_time"]) == native.creation_time(launcher)
            assert not native.exited(process)
            native.assert_in_job(process, identity["job_name"])
            # A bounded forbidden-effect observation before the explicit resume.
            end = time.monotonic() + 0.25
            while time.monotonic() < end:
                assert not (tmp_path / "canary.json").exists()
                time.sleep(0.01)
            assert handle.active_process_count() == 1
            handle.resume()
            with pytest.raises(execution_api.ExecutionContainmentError) as duplicate:
                handle.resume()
            assert duplicate.value.code == "WORKFLOW_EXECUTION_ALREADY_RESUMED"
            observed = _until(
                lambda: _read_json(tmp_path / "canary.json"), description="live canary ready"
            )
            # A venv redirector may launch a distinct real-interpreter PID.
            # Prove the actual worker's containment, not equality to the root PID.
            with native.process(observed["pid"]) as worker:
                assert not native.exited(worker)
                native.assert_in_job(worker, identity["job_name"])
                (tmp_path / "canary.release").write_text("release", encoding="utf-8")
                assert handle.wait(timeout=DEADLINE) == 0
                assert native.exited(process)
                assert native.exited(worker)
        assert handle.active_process_count() == 0
        assert handle.poll() == 0
        assert Path(observed["cwd"]).resolve() == tmp_path.resolve()
        assert "STDOUT_CANARY" in (tmp_path / "stdout.log").read_text(encoding="utf-8")
    finally:
        handle.close()


@pytest.mark.parametrize("active", [0, 1])
@pytest.mark.parametrize("query_fault", ["short-success", "more-data", "accounting-denied"])
def test_job_process_list_budget_retains_bounded_native_query_diagnostics(active, query_fault):
    from types import SimpleNamespace

    from ai_trading_system.platform.architecture import workflow_execution as execution

    capacities = []

    def query(job, kind, buffer, size, returned):
        assert job == 123
        if kind == 1:
            if query_fault == "accounting-denied":
                ctypes.set_last_error(5)
                return False
            buffer._obj.ActiveProcesses = active
            return True
        assert kind == 3
        listing = buffer._obj
        capacity = len(listing.pids)
        capacities.append(capacity)
        listing.assigned, listing.count = 2, 1
        ctypes.set_last_error(234 if query_fault == "more-data" else 0)
        return query_fault != "more-data"

    api = SimpleNamespace(QueryInformationJobObject=query)
    processes = execution._JobProcesses(api, 123)
    with pytest.raises(
        execution.ExecutionContainmentError, match="JOB_PROCESS_LIST_BUDGET",
    ) as error:
        processes.collect()
    detail = json.loads(str(error.value).split(": ", 1)[1])
    assert capacities == [2 ** power for power in range(4, 17)]
    assert detail["retained_process_count"] == 0
    assert len(detail["queries"]) == len(capacities)
    assert detail["queries"][-1] == {
        "capacity": 65536, "ok": query_fault != "more-data",
        "winerror": 234 if query_fault == "more-data" else 0, "assigned": 2, "count": 1,
    }
    if query_fault == "accounting-denied":
        assert detail["accounting_error_code"] == "WORKFLOW_EXECUTION_JOB_QUERY"
        assert "active_process_count" not in detail
    else:
        assert detail["active_process_count"] == active
    assert not processes.handles
    assert execution._job_process_list_diagnostics(error.value) == {
        **detail, "observation_only": True,
    }


@pytest.mark.parametrize("case", [
    "explained", "not-repeated", "active-mismatch", "unsignaled-retained", "accounting-denied",
])
def test_job_process_list_short_list_accepted_only_when_retained_exits_explain_it(case):
    """GOV-007 F2 (v13 native counters: assigned=65, listed=64=active, retained=65).

    A retained handle keeps one exited process object assigned to the job. Only a
    repeated successful list equal to ActiveProcesses whose gap is covered by our
    own signaled identities is complete; every other short list stays fail-closed.
    """
    from types import SimpleNamespace

    from ai_trading_system.platform.architecture import workflow_execution as execution

    listings = []

    def query(job, kind, buffer, size, returned):
        assert job == 123
        if kind == 1:
            if case == "accounting-denied":
                ctypes.set_last_error(5)
                return False
            buffer._obj.ActiveProcesses = 1 if case == "active-mismatch" else 0
            return True
        assert kind == 3
        listing = buffer._obj
        listings.append(len(listing.pids))
        assigned = 1 + (len(listings) % 2 if case == "not-repeated" else 0)
        listing.assigned, listing.count = assigned, 0
        return True

    retained = object()
    api = SimpleNamespace(
        QueryInformationJobObject=query,
        WaitForSingleObject=lambda handle, timeout: (
            execution._WAIT_TIMEOUT if case == "unsignaled-retained" else 0
        ),
    )
    processes = execution._JobProcesses(api, 123)
    processes.handles[(4242, 1)] = retained
    if case == "explained":
        processes.collect()
        assert listings == [16, 32]  # The second identical successful list is accepted.
        assert processes.handles == {(4242, 1): retained}
        return
    with pytest.raises(execution.ExecutionContainmentError, match="JOB_PROCESS_LIST_BUDGET"):
        processes.collect()
    assert listings == [2 ** power for power in range(4, 17)]


@pytest.mark.parametrize("fault", [
    "wrong-error", "too-many", "extra", "secret", "bool-count", "capacity", "accounting-type",
])
def test_job_process_list_diagnostics_excludes_unbounded_or_untyped_data(fault):
    from ai_trading_system.platform.architecture import workflow_execution as execution

    row = {"capacity": 16, "ok": True, "winerror": 0, "assigned": 2, "count": 1}
    value = {"queries": [row], "retained_process_count": 0, "active_process_count": 0}
    error = execution.ExecutionContainmentError(
        "JOB_QUERY" if fault == "wrong-error" else "JOB_PROCESS_LIST_BUDGET",
        "secret text must never be serialized",
    )
    if fault == "too-many":
        value["queries"] = [dict(row) for _ in range(14)]
    elif fault == "extra":
        value["secret"] = "secret text"
    elif fault == "secret":
        row["assigned"] = "secret text"
    elif fault == "bool-count":
        value["active_process_count"] = False
    elif fault == "capacity":
        row["capacity"] = 65537
    elif fault == "accounting-type":
        class ErrorCode(str):
            pass

        value.pop("active_process_count")
        value["accounting_error_code"] = ErrorCode("WORKFLOW_EXECUTION_JOB_QUERY")
    error._job_process_list_diagnostics = value
    assert execution._job_process_list_diagnostics(error) is None


@pytest.mark.parametrize("completion", ["wait", "close"])
def test_wait_does_not_release_when_direct_parent_exits_before_child(
    tmp_path: Path, execution_api: Any, native: NativeOracle, completion: str
) -> None:
    handle = _create(execution_api, tmp_path, _parent_source(stay_alive=False))
    try:
        with native.process(handle.identity()["pid"]) as parent:
            handle.resume()
            child = _until(lambda: _read_json(tmp_path / "child.json"), description="child ready")
            with native.process(child["pid"]) as child_handle:
                assert native.exited(parent, DEADLINE)
                assert handle.poll() == 0
                assert not native.exited(child_handle)
                assert handle.active_process_count() >= 1
                with pytest.raises(TimeoutError) as timed_out:
                    handle.wait(timeout=0.1)
                detail = json.loads(str(timed_out.value).split(": ", 1)[1])
                assert detail["primary_exit_code"] == 0
                assert detail["job_active_process_count"] >= 1
                assert detail["observation_only"] is True
                assert {
                    "pid": child["pid"],
                    "creation_time": native.creation_time(child_handle),
                    "wait_status": 258,
                } in detail["observed_processes"]
                assert not native.exited(child_handle)
                if completion == "wait":
                    (tmp_path / "child.release").write_text("release", encoding="utf-8")
                    assert handle.wait(timeout=DEADLINE) == 0
                    assert handle.active_process_count() == 0
                    assert (tmp_path / "child.done").is_file()
                else:
                    handle.close()
                    assert not (tmp_path / "child.done").exists()
                assert native.exited(child_handle)
    finally:
        handle.close()


@pytest.mark.parametrize("spawn_mode", ["popen", "inherited-child"])
def test_launcher_os_exit_kills_job_tree_without_inherited_job_handle(
    tmp_path: Path, execution_api: Any, native: NativeOracle, spawn_mode: str,
) -> None:
    name = _job_name()
    child_source = _parent_source(stay_alive=True)
    if spawn_mode == "inherited-child":
        child_source = f"""
import os,sys,time
from pathlib import Path
from ai_trading_system.platform.architecture.workflow_execution import InheritedJobChild
child=InheritedJobChild.create(argv=[sys.executable,'-u','-c',{CHILD!r}],
    cwd=Path.cwd(),environment=dict(os.environ),stdout_path=Path.cwd()/'inherited-child.log',
    job_name={name!r})
assert child.pre_resume_binding()['owner_resume_state']=='NOT_RESUMED'
child.resume()
end=time.monotonic()+60
while not Path('parent.release').exists() and time.monotonic()<end: time.sleep(.02)
child.close()
"""
    launcher_source = f"""
import json, os, sys, time
from pathlib import Path
from ai_trading_system.platform.architecture.workflow_execution import WindowsJobProcess
root = Path({str(tmp_path)!r})
child_source = {child_source!r}
handle = WindowsJobProcess.create(argv=[sys.executable, '-u', '-c', child_source],
    cwd=root, environment=dict(os.environ), stdout_path=root/'job.stdout.log', job_name={name!r})
(root/'launcher.identity.json').write_text(json.dumps(handle.identity()), encoding='utf-8')
handle.resume()
end = time.monotonic() + 60
while not (root/'launcher.crash').exists() and time.monotonic() < end:
    time.sleep(.02)
os._exit({CRASH_EXIT})
"""
    with (tmp_path / "launcher.log").open("wb") as output:
        launcher = subprocess.Popen(
            [sys.executable, "-u", "-c", launcher_source],
            cwd=tmp_path,
            env=_environment(),
            stdout=output,
            stderr=subprocess.STDOUT,
        )
        identity: dict[str, Any] | None = None
        try:
            identity = _until(
                lambda: _read_json(tmp_path / "launcher.identity.json"),
                description="launcher binding",
            )
            child = _until(
                lambda: _read_json(tmp_path / "child.json"), description="grandchild ready"
            )
            with (
                native.process(identity["pid"]) as parent,
                native.process(child["pid"]) as grandchild,
            ):
                assert not native.exited(parent) and not native.exited(grandchild)
                (tmp_path / "launcher.crash").write_text("crash", encoding="utf-8")
                assert launcher.wait(timeout=DEADLINE) == CRASH_EXIT
                assert native.exited(parent, 10), "parent survived last Job handle closure"
                assert native.exited(grandchild, 10), "grandchild survived or inherited Job handle"
            assert not (tmp_path / "child.done").exists(), (
                "child ended normally instead of containment kill"
            )
            observation = execution_api.observe_job(name)
            assert observation["state"] in {"EMPTY", "ABSENT"}
        finally:
            if launcher.poll() is None:
                launcher.kill()
                launcher.wait(timeout=DEADLINE)
            observation = execution_api.observe_job(name)
            if identity is not None and observation["state"] == "ACTIVE":
                execution_api.observe_job(
                    name, terminate=True, expected_process=identity
                )
            elif identity is None:
                # A Job name alone never authorizes adopting or terminating it.
                _until(
                    lambda: execution_api.observe_job(name)["state"] == "ABSENT",
                    description="failed launcher closed its unnamed process binding",
                    timeout=DEADLINE,
                )


def test_nonallowlisted_inheritable_event_handle_does_not_reach_child(
    tmp_path: Path, execution_api: Any, native: NativeOracle
) -> None:
    event = native.api.CreateEventW(None, True, False, None)
    assert event and native.api.SetHandleInformation(event, 1, 1)
    source = f"""
import ctypes, json
from pathlib import Path
api = ctypes.WinDLL('kernel32', use_last_error=True)
api.SetEvent.argtypes = [ctypes.c_void_p]
api.SetEvent.restype = ctypes.c_int
result = api.SetEvent({int(event)})
Path('handle_probe.json').write_text(
    json.dumps({{'result':result, 'error':ctypes.get_last_error()}}))
print('EXPLICIT_STDOUT_STILL_WORKS', flush=True)
"""
    try:
        with _create(execution_api, tmp_path, source) as handle:
            handle.resume()
            assert handle.wait(timeout=DEADLINE) == 0
        assert native.api.WaitForSingleObject(event, 0) == 258, "nonallowlist event handle leaked"
        probe = _read_json(tmp_path / "handle_probe.json")
        assert probe == {"result": 0, "error": 6}  # ERROR_INVALID_HANDLE, not an inherited event.
        assert "EXPLICIT_STDOUT_STILL_WORKS" in (tmp_path / "stdout.log").read_text()
    finally:
        assert native.api.CloseHandle(event)


def test_failed_launch_and_absent_observation_never_create_an_executor(
    tmp_path: Path, execution_api: Any, native: NativeOracle
) -> None:
    name = _job_name()
    native.assert_job_absent(name)
    assert execution_api.observe_job(name, terminate=True)["state"] == "ABSENT"
    native.assert_job_absent(name)
    executable = tmp_path / "invalid.exe"
    executable.write_bytes(b"Synthetic invalid executable: not a PE image.\n")
    with pytest.raises(execution_api.ExecutionContainmentError) as failure:
        execution_api.WindowsJobProcess.create(
            argv=[str(executable)],
            cwd=tmp_path,
            environment=_environment(),
            stdout_path=tmp_path / "failed.stdout.log",
            job_name=name,
        )
    assert failure.value.code == "WORKFLOW_EXECUTION_CREATE_CONTAINED_PROCESS"
    # Windows may report ERROR_BAD_EXE_FORMAT or ERROR_EXE_MACHINE_TYPE_MISMATCH.
    assert any(f"winerror={code}" in str(failure.value) for code in (193, 216))
    assert not (tmp_path / "failed.stdout.log").read_bytes()
    native.assert_job_absent(name)


def test_observer_requires_exact_process_identity_before_terminating_live_job(
    tmp_path: Path, execution_api: Any, native: NativeOracle
) -> None:
    handle = _create(execution_api, tmp_path, _parent_source(stay_alive=True))
    try:
        identity = handle.identity()
        with native.process(identity["pid"]) as parent:
            handle.resume()
            child = _until(
                lambda: _read_json(tmp_path / "child.json"), description="observed child"
            )
            with native.process(child["pid"]) as grandchild:
                with pytest.raises(execution_api.ExecutionContainmentError) as missing:
                    execution_api.observe_job(identity["job_name"], terminate=True)
                assert missing.value.code == "WORKFLOW_EXECUTION_JOB_IDENTITY_REQUIRED"
                assert not native.exited(parent) and not native.exited(grandchild)
                wrong = {"pid": identity["pid"], "creation_time": identity["creation_time"] + 1}
                with pytest.raises(execution_api.ExecutionContainmentError) as mismatch:
                    execution_api.observe_job(
                        identity["job_name"], terminate=True, expected_process=wrong
                    )
                assert mismatch.value.code == "WORKFLOW_EXECUTION_JOB_IDENTITY_MISMATCH"
                assert not native.exited(parent) and not native.exited(grandchild)
                result = execution_api.observe_job(
                    identity["job_name"],
                    terminate=True,
                    expected_process={
                        "pid": identity["pid"],
                        "creation_time": identity["creation_time"],
                    },
                )
                assert result["state"] == "EMPTY" and result["active_process_count"] == 0
                assert native.exited(parent) and native.exited(grandchild)
    finally:
        handle.close()


@pytest.mark.parametrize(
    "timeout", [float("nan"), float("inf"), -1.0], ids=["nan", "inf", "negative"]
)
@pytest.mark.parametrize("operation", ["wait", "terminate", "observe_job"])
def test_invalid_timeout_is_rejected_without_terminating_live_execution(
    tmp_path: Path, execution_api: Any, native: NativeOracle, timeout: float, operation: str
) -> None:
    handle = _create(execution_api, tmp_path, _parent_source(stay_alive=True))
    try:
        identity = handle.identity()
        with native.process(identity["pid"]) as parent:
            handle.resume()
            child = _until(lambda: _read_json(tmp_path / "child.json"), description="timeout child")
            with native.process(child["pid"]) as grandchild:
                assert not native.exited(parent) and not native.exited(grandchild)
                with pytest.raises(execution_api.ExecutionContainmentError) as rejected:
                    if operation == "observe_job":
                        execution_api.observe_job(
                            identity["job_name"],
                            terminate=True,
                            timeout=timeout,
                            expected_process=identity,
                        )
                    elif operation == "wait":
                        handle.wait(timeout=timeout)
                    else:
                        handle.terminate(timeout=timeout)
                assert rejected.value.code == "WORKFLOW_EXECUTION_TIMEOUT"
                assert not native.exited(parent) and not native.exited(grandchild)
                assert handle.active_process_count() >= 2
    finally:
        handle.close()


@pytest.mark.parametrize("action", ["terminate", "close", "context"])
def test_explicit_cleanup_waits_for_synthetic_descendants(
    tmp_path: Path, execution_api: Any, native: NativeOracle, action: str
) -> None:
    handle = _create(execution_api, tmp_path, _parent_source(stay_alive=True))
    try:
        with native.process(handle.identity()["pid"]) as parent:
            handle.resume()
            child = _until(lambda: _read_json(tmp_path / "child.json"), description="cleanup child")
            with native.process(child["pid"]) as grandchild:
                if action == "terminate":
                    assert isinstance(handle.terminate(), int)
                    assert handle.active_process_count() == 0
                elif action == "context":
                    with handle:
                        pass
                else:
                    handle.close()
                assert native.exited(parent), "cleanup returned with direct process alive"
                assert native.exited(grandchild), "cleanup returned with descendant alive"
    finally:
        handle.close()


def test_actual_pid_reuse_rejects_old_identity_and_preserves_current_job(
    tmp_path: Path, execution_api: Any, native: NativeOracle
) -> None:
    # DEVX-015 V3 v219: bounded real kernel reuse, never a manufactured FILETIME.
    max_attempts, window_seconds = 16_384, 480
    started = time.monotonic()
    seen: dict[int, dict[str, int]] = {}
    outcome: dict[str, Any] = {
        "status": "INSUFFICIENT_NO_REUSE_OBSERVED",
        "max_attempts": max_attempts,
        "window_seconds": window_seconds,
        "attempts": 0,
    }
    try:
        with (tmp_path / "observations.jsonl").open("x", encoding="utf-8") as observations:
            for index in range(max_attempts):
                if time.monotonic() - started >= window_seconds:
                    break
                name = _job_name()
                marker = tmp_path / f"current-{index}.json"
                source = (
                    "import json,os,time;from pathlib import Path;"
                    f"Path({str(marker)!r}).write_text(json.dumps({{'pid':os.getpid()}}));"
                    "time.sleep(30)"
                )
                handle = execution_api.WindowsJobProcess.create(
                    argv=[sys.executable, "-c", source],
                    cwd=tmp_path,
                    environment=_environment(),
                    stdout_path=tmp_path / f"attempt-{index}.stdout",
                    job_name=name,
                )
                record: dict[str, Any] = {"index": index, "job": name}
                try:
                    identity = handle.identity()
                    current = {key: identity[key] for key in ("pid", "creation_time")}
                    record["identity"] = current
                    with native.process(current["pid"]) as process:
                        assert native.creation_time(process) == current["creation_time"]
                        native.assert_in_job(process, name)
                        assert not native.exited(process)
                        previous = seen.get(current["pid"])
                        if previous is not None:
                            assert previous["creation_time"] != current["creation_time"]
                            record["previous"] = previous
                            stale = execution_api.observe_process(**previous)
                            assert stale["state"] == "REUSED", stale
                            record["stale_observation"] = stale
                            with pytest.raises(execution_api.ExecutionContainmentError) as error:
                                execution_api.observe_job(
                                    name, terminate=True, expected_process=previous
                                )
                            assert error.value.code == "WORKFLOW_EXECUTION_JOB_IDENTITY_MISMATCH"
                            record["stale_refusal"] = error.value.code
                            assert not native.exited(process)
                            assert execution_api.observe_process(**current)["state"] == "RUNNING"
                            handle.resume()
                            worker = _until(
                                lambda marker=marker: _read_json(marker),
                                description="actual reused PID worker",
                            )
                            with native.process(worker["pid"]) as child:
                                native.assert_in_job(child, name)
                                assert not native.exited(child)
                                record["worker"] = {
                                    "pid": worker["pid"],
                                    "creation_time": native.creation_time(child),
                                }
                                result = execution_api.observe_job(
                                    name, terminate=True, expected_process=current
                                )
                                assert result["state"] == "EMPTY"
                                assert result["active_process_count"] == 0
                                assert native.exited(process) and native.exited(child)
                                record["fresh_exit"] = result
                            outcome.update(
                                status="ACTUAL_REUSE_VERIFIED", old=previous, current=current
                            )
                        else:
                            handle.terminate()
                            assert native.exited(process)
                        record["exit_code"] = handle.poll()
                        assert record["exit_code"] is not None
                        assert handle.active_process_count() == 0
                finally:
                    handle.close()
                    native.assert_job_absent(name)
                    record["job_absent_after_close"] = True
                    observations.write(json.dumps(record) + "\n")
                    observations.flush()
                outcome["attempts"] = index + 1
                seen[current["pid"]] = current
                if outcome["status"] == "ACTUAL_REUSE_VERIFIED":
                    break
        assert outcome["status"] == "ACTUAL_REUSE_VERIFIED", outcome
    except BaseException:
        outcome["test_exception"] = True
        raise
    finally:
        outcome["elapsed_seconds"] = round(time.monotonic() - started, 3)
        (tmp_path / "result.json").write_text(json.dumps(outcome), encoding="utf-8")


def test_process_observation_binds_creation_time_and_actual_exit(
    tmp_path: Path, execution_api: Any, native: NativeOracle
) -> None:
    handle = _create(execution_api, tmp_path, "print('done', flush=True)")
    try:
        identity = handle.identity()
        with native.process(identity["pid"]) as process:
            created = native.creation_time(process)
            assert execution_api.observe_process(identity["pid"], created)["state"] == "RUNNING"
            assert execution_api.observe_process(identity["pid"], created + 1)["state"] == "REUSED"
            handle.resume()
            assert handle.wait(timeout=DEADLINE) == 0
            assert native.exited(process)
            assert execution_api.observe_process(identity["pid"], created)["state"] == "EXITED"
    finally:
        handle.close()


def test_actual_two_worker_xdist_is_contained_until_every_worker_exits(
    tmp_path: Path, execution_api: Any, native: NativeOracle
) -> None:
    from contextlib import ExitStack

    (tmp_path / "pytest.ini").write_text("[pytest]\n", encoding="utf-8")
    source = """
import json, os, time
from pathlib import Path
def test_worker(worker_id):
    root = Path(__file__).resolve().parent
    (root/(worker_id+'.json')).write_text(json.dumps({'pid':os.getpid(), 'worker':worker_id}))
    end = time.monotonic()+60
    while not (root/'workers.release').exists() and time.monotonic()<end:
        time.sleep(.02)
    assert (root/'workers.release').exists(), 'test controller did not release worker'
"""
    for name in ("test_small_a.py", "test_small_b.py"):
        (tmp_path / name).write_text(source, encoding="utf-8")
    env = _environment()
    env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    handle = execution_api.WindowsJobProcess.create(
        argv=[
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "xdist.plugin",
            "-n",
            "2",
            "--dist",
            "loadfile",
            "-c",
            "pytest.ini",
            "--confcutdir",
            str(tmp_path),
            "--junitxml=small.xml",
            "test_small_a.py",
            "test_small_b.py",
        ],
        cwd=tmp_path,
        environment=env,
        stdout_path=tmp_path / "xdist.stdout.log",
        job_name=_job_name(),
    )
    try:
        handle.resume()
        workers = [
            _until(
                lambda i=i: _read_json(tmp_path / f"gw{i}.json"),
                description=f"xdist worker {i} ready",
            )
            for i in range(2)
        ]
        assert {worker["worker"] for worker in workers} == {"gw0", "gw1"}
        assert len({worker["pid"] for worker in workers}) == 2
        with ExitStack() as stack:
            processes = [stack.enter_context(native.process(worker["pid"])) for worker in workers]
            assert all(not native.exited(process) for process in processes)
            for process in processes:
                native.assert_in_job(process, handle.identity()["job_name"])
            assert handle.active_process_count() >= 3
            (tmp_path / "workers.release").write_text("release", encoding="utf-8")
            assert handle.wait(timeout=DEADLINE) == 0
            assert all(native.exited(process) for process in processes)
        assert handle.active_process_count() == 0
        suites = ElementTree.parse(tmp_path / "small.xml").getroot().findall("testsuite")
        assert sum(int(suite.attrib["tests"]) for suite in suites) == 2
        assert all(
            int(suite.attrib[key]) == 0
            for suite in suites
            for key in ("failures", "errors", "skipped")
        )
    finally:
        handle.close()
