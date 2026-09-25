from __future__ import annotations

import ctypes
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from test_devx015_workflow_integration import (
    canonical_merge_repository as canonical_merge_repository,
)
from test_devx015_workflow_integration import (
    small_repository as small_repository,
)
from test_governed_development_skill import _admission_registry

from ai_trading_system.platform.architecture import task_registry_canonical as canonical
from ai_trading_system.platform.architecture import workflow_contract as workflow
from ai_trading_system.platform.architecture.integration_publication_fence import (
    IntegrationPublicationFence,
)

ROOT = Path(__file__).resolve().parents[1]
TASK = "DEVX-015-WORKFLOW-FIXTURE"


def install_creation_crash_fixture(root: Path, boundary: str) -> None:
    """Instrument only the disposable CLI, before fixture B/L/M are frozen.

    The real worker must reach native creation under its original Job and
    lease. No authority or identity check is replaced. The test controller
    independently checks the live process before permitting the injected exit.
    """
    kind, phase = boundary.split("-", 1)
    assert kind in {"file", "directory"}
    assert phase in {"before-record", "after-record", "after-create"}
    script = root / "scripts/architecture_arch005_workflow.py"
    original = script.read_text(encoding="utf-8")
    entry = 'if __name__ == "__main__":\n    raise SystemExit(main())'
    assert original.count(entry) == 1
    hook = r"""
# Disposable acceptance fixture: this hook is frozen into M before execution.
def _acceptance_creation_crash():
    import json
    import os
    import sys
    import time
    from pathlib import Path
    from ai_trading_system.platform.architecture import workflow_integration as target
    from ai_trading_system.platform.architecture.workflow_execution import (
        current_process_identity,
    )

    if len(sys.argv) < 2 or sys.argv[1] != "source-install-worker":
        return
    request_path = Path(sys.argv[sys.argv.index("--execution-request") + 1])
    request = json.loads(request_path.read_text(encoding="utf-8"))
    if request["installation_action"] != "INSTALL":
        return
    kind, phase = __BOUNDARY__.split("-", 1)
    name = "create_bound_recoverable_" + kind
    native_create = getattr(target, name)
    root = Path(__file__).resolve().parents[1]
    witness_path = root.parent / "installation-creation-fault.json"
    release_path = root.parent / "installation-creation-fault.release"

    def pause_then_exit(witness):
        with witness_path.open("x", encoding="utf-8") as stream:
            json.dump(witness, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        deadline = time.monotonic() + 300
        while not release_path.exists():
            if time.monotonic() >= deadline:
                raise TimeoutError("acceptance controller did not confirm actual Job")
            time.sleep(0.02)
        os._exit(47)

    def interrupted(base, relative, *args, **kwargs):
        original_recorder = kwargs["record_created"]
        witness = {}

        def record(descriptor):
            info = os.fstat(descriptor)
            witness.update(
                root=base.as_posix(), path=relative, kind=kind, phase=phase,
                file_identity=[info.st_dev, info.st_ino],
                worker_process=current_process_identity(),
                request_id=request["request_id"], job_name=request["job_name"],
            )
            if phase == "before-record":
                pause_then_exit(witness)
            original_recorder(descriptor)
            if phase == "after-record":
                pause_then_exit(witness)

        kwargs["record_created"] = record
        result = native_create(base, relative, *args, **kwargs)
        if phase == "after-create":
            pause_then_exit(witness)
        return result

    setattr(target, name, interrupted)

_acceptance_creation_crash()

""".replace("__BOUNDARY__", repr(boundary))
    script.write_text(original.replace(entry, hook + entry), encoding="utf-8", newline="\n")


@pytest.mark.parametrize("kind", ["file", "directory"])
@pytest.mark.parametrize("phase", ["before-record", "after-record", "after-create"])
def test_creation_crash_fixture_loads_actual_hook_imports(tmp_path, kind, phase):
    """Startup smoke only: this is not an execution/Job/creation acceptance oracle."""
    script = tmp_path / "scripts/architecture_arch005_workflow.py"
    script.parent.mkdir()
    script.write_bytes((ROOT / "scripts/architecture_arch005_workflow.py").read_bytes())
    install_creation_crash_fixture(tmp_path, kind + "-" + phase)
    request = tmp_path / "request.json"
    request.write_text('{"installation_action":"INSTALL"}', encoding="utf-8")
    # Load the actual CLI and its own globals, not a stub that accidentally
    # supplies missing imports. The non-main run name prevents real dispatch;
    # the unconditional fixture hook still exercises INSTALL wrapper setup.
    driver = (
        "import runpy, sys; sys.argv = sys.argv[1:]; "
        "runpy.run_path(sys.argv[0], run_name='acceptance_instrumentation_smoke')"
    )
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            driver,
            str(script),
            "source-install-worker",
            "--execution-request",
            str(request),
        ],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr.decode(errors="replace")
    assert not (tmp_path.parent / "installation-creation-fault.json").exists()


def observe_creation_crash(root, command, environment, fence, lease_id, boundary):
    """Release an actual contained worker at the requested native boundary."""
    from test_devx015_workflow_execution import NativeOracle, _read_json, _until

    kind, phase = boundary.split("-", 1)
    witness_path = root.parent / "installation-creation-fault.json"
    release_path = root.parent / "installation-creation-fault.release"
    log_path = root.parent / "installation-creation-fault.stdout.log"
    assert not witness_path.exists() and not release_path.exists()
    oracle = NativeOracle()
    with log_path.open("xb") as log:
        launcher = subprocess.Popen(
            command,
            cwd=root,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        try:
            witness = _until(
                lambda: (
                    _read_json(witness_path)
                    or ({"launcher_exited": True} if launcher.poll() is not None else None)
                ),
                description="real installation creation boundary or original launcher exit",
                timeout=600,
            )
            assert "launcher_exited" not in witness, log_path.read_text(errors="replace")
            assert (witness["kind"], witness["phase"]) == (kind, phase)
            head = next(
                item for item in fence.guard.store.replay().lease_heads if item.lease_id == lease_id
            )
            attempt = head.execution["installation_attempts"][-1]
            assert attempt["state"] == "RUNNING"
            assert attempt["request"]["request_id"] == witness["request_id"]
            assert attempt["request"]["job_name"] == witness["job_name"]
            with oracle.process(witness["worker_process"]["pid"]) as handle:
                assert oracle.creation_time(handle) == witness["worker_process"]["creation_time"]
                oracle.assert_in_job(handle, witness["job_name"])
                release_path.write_bytes(b"independent Job identity confirmed\n")
                assert launcher.wait(timeout=180) != 0
                assert oracle.exited(handle)
            oracle.assert_job_absent(witness["job_name"])
        finally:
            if launcher.poll() is None:
                # Let the already selected original worker exit even when a
                # test assertion fails; never leave a barrier silently held.
                if witness_path.exists() and not release_path.exists():
                    release_path.write_bytes(b"failed assertion: release original test barrier\n")
                launcher.wait(timeout=180)
    assert "INSTALLATION_WORKER_FAILED" in log_path.read_text(errors="replace")
    head = next(
        item for item in fence.guard.store.replay().lease_heads if item.lease_id == lease_id
    )
    attempt = head.execution["installation_attempts"][-1]
    assert attempt["state"] == "RESULT_RECORDED"
    assert attempt["exit"]["returncode"] == 47
    assert attempt["exit"]["job_state"] == "EMPTY"
    assert attempt["result"]["status"] == "INSUFFICIENT"
    records = [
        row
        for row in attempt["created_objects"]
        if (row["root"], row["path"]) == (witness["root"], witness["path"])
    ]
    if phase == "before-record":
        assert records == []
    else:
        assert len(records) == 1
        assert records[0]["file_identity"] == witness["file_identity"]
        assert records[0]["worker_process"] == witness["worker_process"]
        assert records[0]["plan_sha256"] == attempt["request"]["installation_plan_sha256"]
        assert records[0].get("kind", "file") == kind
    target = Path(witness["root"]) / witness["path"]
    if phase == "after-create":
        actual = target.stat()
        assert [actual.st_dev, actual.st_ino] == witness["file_identity"]
        if kind == "directory":
            assert target.is_dir() and list(target.iterdir()) == []
        else:
            assert hashlib.sha256(target.read_bytes()).hexdigest() == records[0]["target_sha256"]
    else:
        assert not target.exists(), "native delete-on-close must remove the uncommitted object"
    return attempt, witness


def reject_creation_recovery_adversary(
    root, command, environment, fence, lease_id, original_attempt, witness, adversary
):
    """Preserve a real foreign object on refusal, then retain it outside the target."""
    from test_devx015_workflow_execution import _git

    target = Path(witness["root"]) / witness["path"]
    assert target.is_absolute() and target.is_relative_to(root)
    retained_original = root.parent / "retained-original-created-file.bin"
    preserved = root.parent / "preserved-rejected-object.bin"
    assert not retained_original.exists() and not preserved.exists()
    assert witness["phase"] == "after-create"
    if adversary == "same-bytes-replacement":
        assert witness["kind"] == "file"
        content = target.read_bytes()
        target.rename(retained_original)
        assert [retained_original.stat().st_dev, retained_original.stat().st_ino] == witness[
            "file_identity"
        ]
        target.write_bytes(content)
        foreign = target
    else:
        assert adversary == "unknown-child" and witness["kind"] == "directory"
        foreign = target / "unowned-child.bin"
        content = b"Unowned child must survive refused recovery\0\r\n"
        assert not foreign.exists()
        foreign.write_bytes(content)
    foreign_identity = (foreign.stat().st_dev, foreign.stat().st_ino)
    if adversary == "same-bytes-replacement":
        assert list(foreign_identity) != witness["file_identity"]
    refs = _git(root, "for-each-ref", "--format=%(refname) %(objectname)")
    index_path = Path(_git(root, "rev-parse", "--path-format=absolute", "--git-path", "index"))
    index_bytes = index_path.read_bytes()
    rejected = subprocess.run(command, cwd=root, env=environment, capture_output=True, timeout=900)
    log_path = (
        Path(original_attempt["request"]["result_path"]).parent.parent
        / command[-1]
        / "worker.stdout.log"
    )
    detail = rejected.stdout.decode(errors="replace") + rejected.stderr.decode(errors="replace")
    if log_path.exists():
        detail += log_path.read_text(errors="replace")
    assert rejected.returncode != 0, detail
    expected = (
        "INSTALLATION_RECOVERY_IDENTITY_UNKNOWN"
        if adversary == "same-bytes-replacement"
        else "bound directory disposition rejected"
    )
    assert expected in detail, detail
    assert foreign.read_bytes() == content
    assert (foreign.stat().st_dev, foreign.stat().st_ino) == foreign_identity
    assert index_path.read_bytes() == index_bytes
    assert _git(root, "for-each-ref", "--format=%(refname) %(objectname)") == refs
    head = next(
        item for item in fence.guard.store.replay().lease_heads if item.lease_id == lease_id
    )
    history = head.execution["installation_attempts"]
    assert len(history) == 2 and history[0] == original_attempt
    failed = history[-1]
    assert failed["request"]["request_id"] == command[-1]
    assert failed["request"]["installation_action"] == "RECOVER"
    assert failed["state"] == "RESULT_RECORDED"
    assert failed["result"]["status"] == "INSUFFICIENT"
    assert failed["exit"]["job_state"] == "EMPTY"
    if adversary == "unknown-child":
        assert failed["exit"]["returncode"] == 0, "worker exit is not stable adoption"
        assert [target.stat().st_dev, target.stat().st_ino] == witness["file_identity"]
    else:
        assert failed["exit"]["returncode"] != 0
    # These are test-owned adversarial files, not a product repair or receipt
    # edit. Preserve the foreign object; put back the exact retained original
    # inode when applicable. Only a NEW public recovery request may proceed.
    foreign.rename(preserved)
    if adversary == "same-bytes-replacement":
        retained_original.rename(target)
        assert [target.stat().st_dev, target.stat().st_ino] == witness["file_identity"]
    assert preserved.read_bytes() == content
    assert (preserved.stat().st_dev, preserved.stat().st_ino) == foreign_identity
    return failed, preserved, content, foreign_identity


def recover_creation_crash(
    root,
    command,
    environment,
    fence,
    lease_id,
    boundary,
    before,
    before_source,
    before_refs,
    expected_created_paths,
    expected_created_directories,
    adversary=None,
):
    """Use only the public recovery entry, with independent original-state checks."""
    from test_devx015_workflow_execution import _git, _stage_snapshot

    attempt, witness = observe_creation_crash(root, command, environment, fence, lease_id, boundary)
    created_path = (Path(witness["root"]) / witness["path"]).as_posix()
    assert created_path in (
        expected_created_directories if witness["kind"] == "directory" else expected_created_paths
    )
    recovery_command = list(command)
    recovery_command[2] = "source-install-recover"
    recovery_command[-1] = "recover-created-" + attempt["request"]["request_id"]
    recovery_environment = {
        **environment,
        "DEVX015_RECOVERY_SESSION": "created-object-public-process",
    }
    previous = attempt
    preserved_evidence = None
    if adversary is not None:
        previous, preserved, preserved_content, preserved_identity = (
            reject_creation_recovery_adversary(
                root,
                recovery_command,
                recovery_environment,
                fence,
                lease_id,
                attempt,
                witness,
                adversary,
            )
        )
        preserved_evidence = (preserved, preserved_content, preserved_identity)
        recovery_command[-1] = "recover-created-retry-" + attempt["request"]["request_id"]
    recovered = subprocess.run(
        recovery_command,
        cwd=root,
        env=recovery_environment,
        capture_output=True,
        timeout=900,
    )
    recovery_log = (
        Path(attempt["request"]["result_path"]).parent.parent
        / recovery_command[-1]
        / "worker.stdout.log"
    )
    assert recovered.returncode == 0, (
        recovered.stdout.decode(errors="replace")
        + recovered.stderr.decode(errors="replace")
        + (recovery_log.read_text(errors="replace") if recovery_log.exists() else "")
    )
    result = json.loads(recovered.stdout)
    assert result["stable_state"] == "SOURCE_RESTORED"
    assert result["publication_performed"] is False
    restored = _stage_snapshot(root)
    assert {
        name: content for name, content in restored[0].items() if not name.startswith("outputs/")
    } == before_source
    assert restored[1:] == before[1:]
    assert _git(root, "for-each-ref", "--format=%(refname) %(objectname)") == before_refs
    assert all(not Path(name).exists() for name in expected_created_directories)
    final = next(
        item for item in fence.guard.store.replay().lease_heads if item.lease_id == lease_id
    )
    history = final.execution["installation_attempts"]
    assert len(history) == (2 if adversary is None else 3)
    assert history[0] == attempt, "original creation and terminal evidence must remain immutable"
    assert history[-2] == previous
    assert history[-1]["result"]["status"] == "PASS"
    assert history[-1]["result"]["reason"] == "INDEPENDENT_SOURCE_INSTALLATION_VERIFIED"
    assert history[-1]["request"]["installation_action"] == "RECOVER"
    assert (
        history[-1]["request"]["previous_installation_sha256"]
        == hashlib.sha256(
            json.dumps(previous, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )
    assert history[-1]["request"]["environment_sha256"] != attempt["request"]["environment_sha256"]
    assert (
        history[-1]["request"]["environment_sha256"]
        == hashlib.sha256(
            json.dumps(
                recovery_environment, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode()
        ).hexdigest()
    )
    before_replay = tuple(fence.guard.store.replay().head_event_ids)
    replayed = subprocess.run(
        recovery_command,
        cwd=root,
        env=recovery_environment,
        capture_output=True,
        timeout=60,
    )
    assert replayed.returncode == 0, replayed.stderr.decode(errors="replace")
    assert json.loads(replayed.stdout)["dispatch_allowed"] is False
    assert tuple(fence.guard.store.replay().head_event_ids) == before_replay
    if preserved_evidence is not None:
        preserved, content, identity = preserved_evidence
        assert preserved.read_bytes() == content
        assert (preserved.stat().st_dev, preserved.stat().st_ino) == identity


@pytest.mark.parametrize(
    "canonical_merge_repository", ["source-job-created-directory-after-create"], indirect=True
)
def test_public_installation_recovers_created_directory_from_original_job(
    canonical_merge_repository,
):
    from test_devx015_workflow_execution import (
        test_source_candidate_cli_uses_real_job_and_private_two_parent_commit,
    )

    test_source_candidate_cli_uses_real_job_and_private_two_parent_commit(
        canonical_merge_repository,
        installation_boundary="created-directory-after-create",
    )


@pytest.mark.parametrize("canonical_merge_repository", ["source-job"], indirect=True)
def test_source_final_handoff_recovers_real_marker_and_retention_crashes_after_heartbeat(
    canonical_merge_repository,
) -> None:
    from test_devx015_workflow_execution import (
        test_source_candidate_cli_uses_real_job_and_private_two_parent_commit,
    )

    test_source_candidate_cli_uses_real_job_and_private_two_parent_commit(
        canonical_merge_repository,
        handoff_boundary="evidence",
    )


@pytest.mark.parametrize(
    "canonical_merge_repository", ["source-job-created-directory-after-create"], indirect=True
)
def test_public_recovery_preserves_unknown_child_before_empty_directory_restore(
    canonical_merge_repository,
):
    from test_devx015_workflow_execution import (
        test_source_candidate_cli_uses_real_job_and_private_two_parent_commit,
    )

    test_source_candidate_cli_uses_real_job_and_private_two_parent_commit(
        canonical_merge_repository,
        installation_boundary="created-directory-after-create",
        recovery_adversary="unknown-child",
    )


@pytest.mark.parametrize(
    "boundary",
    [
        "missing",
        "complete",
        "empty",
        "partial",
        "retention-partial",
        "unknown",
        "retention-unknown",
    ],
)
def test_reproducible_evidence_native_completion_preserves_partial_and_unknown_bytes(
    tmp_path: Path,
    boundary: str,
) -> None:
    from ai_trading_system.platform.architecture.workflow_integration import (
        _complete_recoverable_evidence,
    )

    target = tmp_path / "marker.json"
    content = b'{"bound":"original","status":"PASS"}\n'
    partial = b"" if boundary == "empty" else content[:12]
    retained = target.with_name(
        target.stem + ".partial-" + hashlib.sha256(partial).hexdigest() + ".json"
    )
    if boundary != "missing":
        target.write_bytes(
            content
            if boundary == "complete"
            else b"unknown source"
            if boundary == "unknown"
            else partial
        )
    if boundary.startswith("retention-"):
        retained.write_bytes(
            partial[:3] if boundary == "retention-partial" else b"unknown retained"
        )
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    identity = target.stat().st_ino if target.exists() else None
    arguments = {
        "root_identity": (tmp_path.stat().st_dev, tmp_path.stat().st_ino),
        "error_prefix": "TEST_EVIDENCE",
    }
    if "unknown" in boundary:
        with pytest.raises(workflow.WorkflowContractError, match="TEST_EVIDENCE"):
            _complete_recoverable_evidence(tmp_path, target, content, **arguments)
        assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before
    else:
        _complete_recoverable_evidence(tmp_path, target, content, **arguments)
        assert target.read_bytes() == content
        if identity is not None:
            assert target.stat().st_ino == identity
        if boundary not in {"missing", "complete"}:
            assert retained.read_bytes() == partial
        completed = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
        _complete_recoverable_evidence(tmp_path, target, content, **arguments)
        assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == completed


@pytest.mark.parametrize("operation", ["create", "apply"])
def test_bound_installation_root_identity_rejects_replacement_before_call(
    tmp_path: Path,
    operation: str,
) -> None:
    root = tmp_path / "selected"
    root.mkdir()
    frozen_root = (root.stat().st_dev, root.stat().st_ino)
    leaf = root / "leaf.bin"
    leaf.write_bytes(b"before")
    frozen_file = (leaf.stat().st_dev, leaf.stat().st_ino)
    if operation == "create":
        workflow.write_bound_once(root, "first.bin", b"first", expected_root_identity=frozen_root)
    else:
        workflow.apply_bound_file(
            root,
            "leaf.bin",
            b"before",
            b"after",
            expected_identity=frozen_file,
            expected_root_identity=frozen_root,
        )
    original = tmp_path / "original"
    root.rename(original)
    root.mkdir()
    (root / "leaf.bin").write_bytes(b"replacement canary")
    with pytest.raises(workflow.WorkflowContractError, match="OUTPUT_ROOT_IDENTITY_CHANGED"):
        if operation == "create":
            workflow.write_bound_once(
                root,
                "forbidden.bin",
                b"forbidden",
                expected_root_identity=frozen_root,
            )
        else:
            workflow.apply_bound_file(
                root,
                "leaf.bin",
                b"after",
                b"forbidden",
                expected_identity=frozen_file,
                expected_root_identity=frozen_root,
            )
    assert list(path.name for path in root.iterdir()) == ["leaf.bin"]
    assert (root / "leaf.bin").read_bytes() == b"replacement canary"
    assert (original / "leaf.bin").read_bytes() == (
        b"before" if operation == "create" else b"after"
    )


def test_bound_output_create_preserves_raw_bytes_and_rejects_overwrite(tmp_path: Path) -> None:
    content = b"raw\0binary\r\n"
    workflow.write_bound_once(tmp_path, "nested/deeper/result.bin", content)
    result = tmp_path / "nested/deeper/result.bin"
    assert result.read_bytes() == content
    with pytest.raises(OSError):
        workflow.write_bound_once(tmp_path, "nested/deeper/result.bin", b"wrong")
    assert result.read_bytes() == content
    # No native directory handle leaked and no deferred write remains.
    (tmp_path / "nested").rename(tmp_path / "renamed")
    assert (tmp_path / "renamed/deeper/result.bin").read_bytes() == content


@pytest.mark.parametrize(
    "boundary",
    [
        "callback-error",
        "callback-crash",
        "recorded-crash",
        "after-clear-crash",
        "partial-payload-crash",
        "success",
        "recorder-seek",
    ],
)
def test_recoverable_creation_actual_process_boundaries(tmp_path: Path, boundary: str) -> None:
    """Native primitive oracle only; the journal here is not a lease authority."""
    program = r"""
import json, os, sys
from pathlib import Path
from ai_trading_system.platform.architecture import workflow_contract as workflow
root, boundary = Path(sys.argv[1]), sys.argv[2]
content = b"raw\0created\r\nbytes"
def recorded(fd):
    state = os.fstat(fd)
    assert state.st_size == 0
    if boundary == "callback-error":
        raise RuntimeError("recorder rejected")
    if boundary == "callback-crash":
        os._exit(41)
    with (root / "observed.json").open("xb") as journal:
        journal.write(json.dumps([state.st_dev, state.st_ino]).encode())
        journal.flush()
        os.fsync(journal.fileno())
    if boundary == "recorded-crash":
        os._exit(42)
    if boundary == "recorder-seek":
        os.lseek(fd, 9, os.SEEK_SET)
original_fdopen = os.fdopen
class Stream:
    def __init__(self, stream): self.stream = stream
    def __enter__(self): return self
    def __exit__(self, *args): return self.stream.__exit__(*args)
    def __getattr__(self, name): return getattr(self.stream, name)
    def write(self, data):
        if boundary == "after-clear-crash":
            os._exit(43)
        if boundary == "partial-payload-crash":
            self.stream.write(data[:7])
            self.stream.flush()
            os.fsync(self.stream.fileno())
            os._exit(44)
        return self.stream.write(data)
os.fdopen = lambda *args, **kwargs: Stream(original_fdopen(*args, **kwargs))
info = root.stat()
try:
    workflow.create_bound_recoverable_file(root, "leaf.bin", content,
        record_created=recorded, expected_root_identity=(info.st_dev, info.st_ino))
except RuntimeError as error:
    assert boundary == "callback-error" and str(error) == "recorder rejected"
    sys.exit(45)
"""
    completed = subprocess.run(
        [sys.executable, "-c", program, str(tmp_path), boundary],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert (
        completed.returncode
        == {
            "callback-error": 45,
            "callback-crash": 41,
            "recorded-crash": 42,
            "after-clear-crash": 43,
            "partial-payload-crash": 44,
            "success": 0,
            "recorder-seek": 0,
        }[boundary]
    ), completed.stdout + completed.stderr
    leaf = tmp_path / "leaf.bin"
    journal = tmp_path / "observed.json"
    assert journal.exists() == (boundary not in {"callback-error", "callback-crash"})
    if boundary in {"callback-error", "callback-crash", "recorded-crash"}:
        assert not leaf.exists(), "uncommitted creation survived actual process exit"
    else:
        info = leaf.stat()
        assert [info.st_dev, info.st_ino] == json.loads(journal.read_text())
        assert (
            leaf.read_bytes()
            == {
                "after-clear-crash": b"",
                "partial-payload-crash": b"raw\0cre",
                "success": b"raw\0created\r\nbytes",
                "recorder-seek": b"raw\0created\r\nbytes",
            }[boundary]
        )
    # No native handle may remain after confirmed child exit.
    moved = tmp_path.rename(tmp_path.with_name(tmp_path.name + "-" + boundary + "-closed"))
    moved.rename(tmp_path)


@pytest.mark.parametrize(
    "boundary", ["existing-leaf", "missing-parent", "wrong-root", "missing-root"]
)
def test_recoverable_creation_rejects_without_record_or_new_file(tmp_path: Path, boundary) -> None:
    info = tmp_path.stat()
    identity = (info.st_dev, info.st_ino)
    relative = "missing/leaf.bin" if boundary == "missing-parent" else "leaf.bin"
    if boundary == "existing-leaf":
        (tmp_path / relative).write_bytes(b"existing-canary")
    if boundary == "wrong-root":
        identity = (info.st_dev, info.st_ino + 1)
    if boundary == "missing-root":
        identity = None
    calls = []
    before = {path.name: path.read_bytes() for path in tmp_path.iterdir()}
    with pytest.raises((OSError, workflow.WorkflowContractError)):
        workflow.create_bound_recoverable_file(
            tmp_path,
            relative,
            b"new",
            record_created=calls.append,
            expected_root_identity=identity,
        )
    assert calls == []
    assert {path.name: path.read_bytes() for path in tmp_path.iterdir()} == before


@pytest.mark.parametrize(
    "boundary",
    ["callback-error", "callback-crash", "recorded-crash", "after-clear-crash", "success"],
)
def test_recoverable_directory_actual_process_boundaries(tmp_path: Path, boundary: str) -> None:
    """Actual native lifecycle only; observation journal is not lease authority."""
    program = r"""
import json, os, stat, sys
from pathlib import Path
from ai_trading_system.platform.architecture import workflow_contract as workflow
root, boundary = Path(sys.argv[1]), sys.argv[2]
def recorded(fd):
    state = os.fstat(fd)
    assert stat.S_ISDIR(state.st_mode)
    if boundary == "callback-error": raise RuntimeError("rejected")
    if boundary == "callback-crash": os._exit(41)
    with (root / "observed.json").open("xb") as journal:
        journal.write(json.dumps([state.st_dev, state.st_ino]).encode())
        journal.flush()
        os.fsync(journal.fileno())
    if boundary == "recorded-crash": os._exit(42)
state = root.stat()
try:
    workflow.create_bound_recoverable_directory(root, "created", record_created=recorded,
        expected_root_identity=(state.st_dev, state.st_ino))
except RuntimeError:
    assert boundary == "callback-error"
    sys.exit(45)
if boundary == "after-clear-crash": os._exit(43)
"""
    completed = subprocess.run(
        [sys.executable, "-c", program, str(tmp_path), boundary],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert (
        completed.returncode
        == {
            "callback-error": 45,
            "callback-crash": 41,
            "recorded-crash": 42,
            "after-clear-crash": 43,
            "success": 0,
        }[boundary]
    ), completed.stderr
    leaf = tmp_path / "created"
    if boundary in {"callback-error", "callback-crash", "recorded-crash"}:
        assert not leaf.exists()
    else:
        state = leaf.stat()
        assert [state.st_dev, state.st_ino] == json.loads((tmp_path / "observed.json").read_text())
        assert list(leaf.iterdir()) == []
        root_info = tmp_path.stat()
        workflow.remove_bound_empty_directory(
            tmp_path,
            "created",
            expected_identity=(state.st_dev, state.st_ino),
            expected_root_identity=(root_info.st_dev, root_info.st_ino),
        )
        assert not leaf.exists()


def test_legacy_installation_lock_cannot_create_unplanned_parent(tmp_path):
    root_info = tmp_path.stat()
    with pytest.raises(FileNotFoundError):
        workflow.write_bound_once(
            tmp_path,
            "missing/ref.lock",
            b"plan lock",
            expected_root_identity=(root_info.st_dev, root_info.st_ino),
            require_existing_parents=True,
        )
    assert list(tmp_path.iterdir()) == []
    (tmp_path / "existing").mkdir()
    workflow.write_bound_once(
        tmp_path,
        "existing/ref.lock",
        b"plan lock",
        expected_root_identity=(root_info.st_dev, root_info.st_ino),
        require_existing_parents=True,
    )
    assert (tmp_path / "existing/ref.lock").read_bytes() == b"plan lock"


def test_bound_directory_preserves_unknown_children_and_replaced_parent(tmp_path):
    root_info = tmp_path.stat()
    root_identity = (root_info.st_dev, root_info.st_ino)
    parent = tmp_path / "parent"
    parent.mkdir()
    original = parent.stat()
    original_identity = (original.st_dev, original.st_ino)
    child = parent / "unknown.bin"
    child.write_bytes(b"canary")
    with pytest.raises(OSError):
        workflow.remove_bound_empty_directory(
            tmp_path,
            "parent",
            expected_identity=original_identity,
            expected_root_identity=root_identity,
        )
    assert child.read_bytes() == b"canary"
    parent.rename(tmp_path / "preserved")
    parent.mkdir()
    assert parent.stat().st_ino != original.st_ino
    recorded = []
    with pytest.raises(workflow.WorkflowContractError, match="OUTPUT_PARENT_IDENTITY_CHANGED"):
        workflow.create_bound_recoverable_directory(
            tmp_path,
            "parent/new",
            record_created=recorded.append,
            expected_root_identity=root_identity,
            expected_parent_identities={"parent": original_identity},
        )
    assert recorded == [] and not (parent / "new").exists()
    assert (tmp_path / "preserved/unknown.bin").read_bytes() == b"canary"


def test_bound_output_rejects_junction_before_leaf_creation(tmp_path: Path) -> None:
    selected, protected = tmp_path / "selected", tmp_path / "protected"
    selected.mkdir()
    protected.mkdir()
    canary = protected / "canary.bin"
    canary.write_bytes(b"untouched")
    _junction(selected / "escape", protected)
    before = {path.name: path.read_bytes() for path in protected.iterdir()}
    with pytest.raises(workflow.WorkflowContractError, match="OUTPUT_DIRECTORY_REPARSE"):
        workflow.write_bound_once(selected, "escape/created.bin", b"forbidden")
    assert {path.name: path.read_bytes() for path in protected.iterdir()} == before


def test_bound_output_fdopen_failure_closes_actual_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_fdopen = workflow.os.fdopen
    descriptors = []

    def fail_constructor(descriptor, mode="r", *args, **kwargs):
        if mode == "wb":
            descriptors.append(descriptor)
            raise RuntimeError("injected fdopen constructor failure")
        return original_fdopen(descriptor, mode, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(workflow.os, "fdopen", fail_constructor)
        with pytest.raises(RuntimeError, match="injected fdopen constructor failure"):
            workflow.write_bound_once(tmp_path, "nested/result.bin", b"not written")
    assert len(descriptors) == 1
    leaf = tmp_path / "nested/result.bin"
    assert leaf.is_file() and leaf.read_bytes() == b""
    leaf.unlink()
    (tmp_path / "nested").rename(tmp_path / "renamed")
    assert list((tmp_path / "renamed").iterdir()) == []


def test_bound_output_root_swap_after_lstat_creates_no_leaf(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected, protected = tmp_path / "selected", tmp_path / "protected"
    selected.mkdir()
    protected.mkdir()
    (protected / "canary.bin").write_bytes(b"protected raw\0bytes")
    before = {path.name: path.read_bytes() for path in protected.iterdir()}
    original_lstat = Path.lstat
    swapped = []

    def swap(path, *args, **kwargs):
        info = original_lstat(path, *args, **kwargs)
        if path == selected and not swapped:
            swapped.append(True)
            selected.rename(tmp_path / "original")
            _junction(selected, protected)
        return info

    monkeypatch.setattr(Path, "lstat", swap)
    with pytest.raises(workflow.WorkflowContractError, match="OUTPUT_DIRECTORY_REPARSE"):
        workflow.write_bound_once(selected, "created.bin", b"forbidden")
    assert swapped == [True]
    assert {path.name: path.read_bytes() for path in protected.iterdir()} == before
    assert list((tmp_path / "original").iterdir()) == []


def test_bound_output_held_directory_blocks_real_process_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected = tmp_path / "selected"
    selected.mkdir()
    directory = selected / "nested"
    directory.mkdir()
    original_identity = directory.stat().st_ino
    program = """
import ctypes, json, sys
from ctypes import wintypes as w
from pathlib import Path
path=Path(sys.argv[1])
renamed=path.with_name('renamed')
result={}
try:
    path.rename(renamed)
except OSError as error:
    result['rename']=error.winerror
else:
    renamed.rename(path)
    result['rename']=0
api=ctypes.WinDLL('kernel32', use_last_error=True)
api.CreateFileW.argtypes=[w.LPCWSTR,w.DWORD,w.DWORD,ctypes.c_void_p,w.DWORD,w.DWORD,w.HANDLE]
api.CreateFileW.restype=w.HANDLE
api.CloseHandle.argtypes=[w.HANDLE]
api.CloseHandle.restype=w.BOOL
handle=api.CreateFileW(str(path),0x40000000,7,None,3,0x02000000,None)
if handle == ctypes.c_void_p(-1).value:
    result['write_open']=ctypes.get_last_error()
else:
    result['write_open']=0
    assert api.CloseHandle(handle)
print(json.dumps(result))
"""

    def attempt():
        result = subprocess.run(
            [sys.executable, "-c", program, str(directory)],
            capture_output=True,
            timeout=30,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        assert result.returncode == 0, result.stderr.decode(errors="replace")
        return json.loads(result.stdout)

    assert attempt() == {"rename": 0, "write_open": 0}
    real_windll = ctypes.WinDLL
    barriers = []

    class NativeCall:
        def __init__(self, function):
            object.__setattr__(self, "function", function)

        def __setattr__(self, name, value):
            setattr(self.function, name, value)

        def __getattr__(self, name):
            return getattr(self.function, name)

        def __call__(self, *args):
            code = self.function(*args)
            if code >= 0 and args[8] & 1:
                assert not barriers
                assert not (directory / "result.bin").exists()
                barriers.append(attempt())
            return code

    class NativeLibrary:
        def __init__(self, library):
            self.library = library
            self.NtCreateFile = NativeCall(library.NtCreateFile)

        def __getattr__(self, name):
            return getattr(self.library, name)

    def observed_library(name, *args, **kwargs):
        library = real_windll(name, *args, **kwargs)
        return NativeLibrary(library) if name == "ntdll" else library

    monkeypatch.setattr(ctypes, "WinDLL", observed_library)
    content = b"actual handle-relative write\0\r\n"
    workflow.write_bound_once(selected, "nested/result.bin", content)
    assert barriers == [{"rename": 32, "write_open": 32}]
    assert directory.stat().st_ino == original_identity
    assert (directory / "result.bin").read_bytes() == content
    assert not (selected / "renamed").exists()
    # The same independent process succeeds after every held handle is closed.
    assert attempt() == {"rename": 0, "write_open": 0}
    assert directory.stat().st_ino == original_identity


@pytest.mark.parametrize("relative", ["../escape", "NUL", "dir./leaf", "dir /leaf", "a:b"])
def test_bound_output_rejects_alias_names_without_creating_files(
    tmp_path: Path,
    relative: str,
) -> None:
    with pytest.raises(workflow.WorkflowContractError):
        workflow.write_bound_once(tmp_path, relative, b"wrong")
    assert list(tmp_path.iterdir()) == []


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


@pytest.mark.parametrize("after", [b"short\0\xff\r\n", None], ids=["raw-truncate", "delete"])
def test_bound_apply_existing_file_preserves_identity_or_deletes(tmp_path: Path, after) -> None:
    path = tmp_path / "leaf.bin"
    before = b"long original raw binary\0\xff\r\n" * 8
    path.write_bytes(before)
    identity = (path.lstat().st_dev, path.lstat().st_ino)
    workflow.apply_bound_file(tmp_path, "leaf.bin", before, after, expected_identity=identity)
    if after is None:
        assert not path.exists()
    else:
        assert path.read_bytes() == after
        assert (path.lstat().st_dev, path.lstat().st_ino) == identity
        path.unlink()
    renamed = tmp_path.rename(tmp_path.with_name(tmp_path.name + "-closed"))
    renamed.rename(tmp_path)


@pytest.mark.parametrize(
    ("mismatch", "code"),
    [("bytes", "OUTPUT_FILE_CONTENT_CHANGED"), ("identity", "OUTPUT_FILE_IDENTITY")],
)
def test_bound_apply_rejects_wrong_binding_without_write(tmp_path: Path, mismatch, code) -> None:
    path = tmp_path / "leaf.bin"
    before = b"original\0bytes"
    path.write_bytes(before)
    info = path.lstat()
    identity = (info.st_dev, info.st_ino + (mismatch == "identity"))
    with pytest.raises(workflow.WorkflowContractError, match=code):
        workflow.apply_bound_file(
            tmp_path,
            "leaf.bin",
            b"wrong" if mismatch == "bytes" else before,
            b"forbidden",
            expected_identity=identity,
        )
    assert path.read_bytes() == before
    assert path.lstat().st_ino == info.st_ino
    path.unlink()


@pytest.mark.parametrize("relative", ["missing/leaf.bin", "leaf.bin"])
def test_bound_apply_missing_path_never_creates(tmp_path: Path, relative: str) -> None:
    with pytest.raises(OSError) as failure:
        workflow.apply_bound_file(
            tmp_path,
            relative,
            b"",
            b"forbidden",
            expected_identity=(1, 1),
        )
    assert failure.value.errno in {2, 3} or failure.value.winerror in {2, 3}
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("link_kind", ["symlink", "hardlink"])
def test_bound_apply_rejects_leaf_link_without_canary_change(tmp_path: Path, link_kind) -> None:
    selected = tmp_path / "selected"
    selected.mkdir()
    canary = tmp_path / "canary.bin"
    before = b"protected canary\0\xff"
    canary.write_bytes(before)
    leaf = selected / "leaf.bin"
    if link_kind == "symlink":
        leaf.symlink_to(canary)
    else:
        os.link(canary, leaf)
    info = leaf.lstat()
    entries = sorted(path.name for path in tmp_path.iterdir())
    with pytest.raises(workflow.WorkflowContractError, match="OUTPUT_FILE_TYPE_OR_LINKS"):
        workflow.apply_bound_file(
            selected,
            "leaf.bin",
            before,
            None,
            expected_identity=(info.st_dev, info.st_ino),
        )
    assert canary.read_bytes() == before
    assert sorted(path.name for path in tmp_path.iterdir()) == entries
    assert leaf.lstat().st_ino == info.st_ino
    leaf.unlink()


def test_bound_apply_parent_swap_after_lstat_preserves_canary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected, protected = tmp_path / "selected", tmp_path / "protected"
    selected.mkdir()
    protected.mkdir()
    (selected / "leaf.bin").write_bytes(b"original")
    (protected / "leaf.bin").write_bytes(b"protected")
    info = (selected / "leaf.bin").lstat()
    before = {path.name: path.read_bytes() for path in protected.iterdir()}
    original = Path.lstat
    swapped = []

    def swap(path, *args, **kwargs):
        result = original(path, *args, **kwargs)
        if path == selected and not swapped:
            swapped.append(True)
            selected.rename(tmp_path / "original")
            _junction(selected, protected)
        return result

    monkeypatch.setattr(Path, "lstat", swap)
    with pytest.raises(workflow.WorkflowContractError, match="OUTPUT_DIRECTORY_REPARSE"):
        workflow.apply_bound_file(
            selected,
            "leaf.bin",
            b"original",
            b"changed",
            expected_identity=(info.st_dev, info.st_ino),
        )
    assert swapped == [True]
    assert {path.name: path.read_bytes() for path in protected.iterdir()} == before
    assert (tmp_path / "original/leaf.bin").read_bytes() == b"original"


def test_bound_apply_exclusive_leaf_blocks_real_process_writer_and_parent_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    directory = tmp_path / "nested"
    directory.mkdir()
    leaf = directory / "leaf.bin"
    leaf.write_bytes(b"original")
    info = leaf.lstat()
    program = """
import ctypes, json, sys
from ctypes import wintypes as w
from pathlib import Path
directory=Path(sys.argv[1]); renamed=directory.with_name('renamed')
result={}
try:
    directory.rename(renamed)
except OSError as error:
    result['rename']=error.winerror
else:
    renamed.rename(directory)
    result['rename']=0
api=ctypes.WinDLL('kernel32', use_last_error=True)
api.CreateFileW.argtypes=[w.LPCWSTR,w.DWORD,w.DWORD,ctypes.c_void_p,w.DWORD,w.DWORD,w.HANDLE]
api.CreateFileW.restype=w.HANDLE
api.CloseHandle.argtypes=[w.HANDLE]; api.CloseHandle.restype=w.BOOL
handle=api.CreateFileW(str(directory/'leaf.bin'),0x40000000,7,None,3,0x80,None)
if handle == ctypes.c_void_p(-1).value:
    result['writer']=ctypes.get_last_error()
else:
    result['writer']=0
    assert api.CloseHandle(handle)
print(json.dumps(result))
"""

    def attempt():
        result = subprocess.run(
            [sys.executable, "-c", program, str(directory)],
            capture_output=True,
            timeout=30,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        assert result.returncode == 0, result.stderr.decode(errors="replace")
        return json.loads(result.stdout)

    assert attempt() == {"rename": 0, "writer": 0}
    original = ctypes.WinDLL
    barriers = []

    class NativeCall:
        def __init__(self, function):
            object.__setattr__(self, "function", function)

        def __setattr__(self, name, value):
            setattr(self.function, name, value)

        def __getattr__(self, name):
            return getattr(self.function, name)

        def __call__(self, *args):
            result = self.function(*args)
            if result >= 0 and not args[8] & 1:
                barriers.append(attempt())
            return result

    class NativeLibrary:
        def __init__(self, library):
            self.library = library
            self.NtCreateFile = NativeCall(library.NtCreateFile)

        def __getattr__(self, name):
            return getattr(self.library, name)

    def observe(name, *args, **kwargs):
        library = original(name, *args, **kwargs)
        return NativeLibrary(library) if name == "ntdll" else library

    monkeypatch.setattr(ctypes, "WinDLL", observe)
    workflow.apply_bound_file(
        tmp_path,
        "nested/leaf.bin",
        b"original",
        b"changed\0",
        expected_identity=(info.st_dev, info.st_ino),
    )
    assert barriers == [{"rename": 32, "writer": 32}]
    assert leaf.read_bytes() == b"changed\0" and leaf.lstat().st_ino == info.st_ino
    assert attempt() == {"rename": 0, "writer": 0}


def _json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(json.dumps(value, sort_keys=True).encode())


def test_repository_identity_does_not_inherit_git_redirection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected = tmp_path / "selected"
    other = tmp_path / "other"
    for root in (selected, other):
        root.mkdir()
        _git(root, "init")
        _git(
            root,
            "remote",
            "add",
            "origin",
            "https://github.com/AI-Trading-Collaboration/AITradingSystem.git",
        )
        for relative in (
            "docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md",
            "scripts/architecture_arch005_checkout_guard.py",
        ):
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("synthetic sentinel", encoding="utf-8")
    monkeypatch.setenv("GIT_DIR", str(other / ".git"))
    monkeypatch.setenv("GIT_WORK_TREE", str(other))
    identity = workflow.repository_identity(selected)
    assert identity["checkout"] == selected.as_posix()
    assert identity["common"] == (selected / ".git").as_posix()


@pytest.mark.parametrize("content", [b"", b"a\r\nb\n", b"\x00\xff\x80"])
def test_regular_bytes_preserve_exact_bytes_at_budget(tmp_path: Path, content: bytes) -> None:
    assert os.name == "nt", "mandatory Windows evidence unavailable"
    path = tmp_path / "source.bin"
    path.write_bytes(content)
    assert workflow.bounded_regular_bytes(path, budget=len(content)) == content


def test_regular_bytes_reject_budget_overrun(tmp_path: Path) -> None:
    path = tmp_path / "source.bin"
    path.write_bytes(b"abcd")
    with pytest.raises(workflow.WorkflowContractError, match="ARTIFACT_BUDGET"):
        workflow.bounded_regular_bytes(path, budget=3)


def test_regular_bytes_reject_same_bytes_replacement_before_read(
    tmp_path: Path, monkeypatch
) -> None:
    path = tmp_path / "source.bin"
    content = b"same bytes are not original custody"
    path.write_bytes(content)
    original = path.stat()
    expected = (original.st_dev, original.st_ino)
    assert workflow.bounded_regular_bytes(path, expected_identity=expected) == content
    path.rename(tmp_path / "preserved.bin")
    path.write_bytes(content)
    assert (path.stat().st_dev, path.stat().st_ino) != expected
    reads = _observe_reads(monkeypatch)
    with pytest.raises(workflow.WorkflowContractError, match="HANDLE_EXPECTED_IDENTITY_CHANGED"):
        workflow.bounded_regular_bytes(path, expected_identity=expected)
    assert reads == []
    assert path.read_bytes() == (tmp_path / "preserved.bin").read_bytes() == content


def _observe_reads(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    reads: list[int] = []
    real_fdopen = os.fdopen

    class ObservedStream:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            self.stream.__enter__()
            return self

        def __exit__(self, *args):
            return self.stream.__exit__(*args)

        def fileno(self):
            return self.stream.fileno()

        def read(self, size=-1):
            reads.append(size)
            return self.stream.read(size)

    monkeypatch.setattr(os, "fdopen", lambda *a, **k: ObservedStream(real_fdopen(*a, **k)))
    return reads


def _junction(link: Path, target: Path) -> None:
    # Both exact paths are new pytest fixture directories; no deletion or shell composition.
    result = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(link), str(target)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_junction_canary_rejected_before_any_content_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "protected"
    target.mkdir()
    (target / "canary.bin").write_bytes(b"forbidden-canary")
    link = tmp_path / "alias"
    _junction(link, target)
    reads = _observe_reads(monkeypatch)
    with pytest.raises(workflow.WorkflowContractError, match="REPARSE_PATH"):
        workflow.bounded_regular_bytes(link / "canary.bin")
    assert reads == [], "canary content was read before rejection"


@pytest.mark.parametrize("replacement", ["leaf", "ancestor-junction"])
def test_path_swap_after_lstat_rejected_before_canary_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement: str,
) -> None:
    directory = tmp_path / "authorized"
    directory.mkdir()
    requested = directory / "source.bin"
    requested.write_bytes(b"authorized-original")
    protected = tmp_path / "protected"
    protected.mkdir()
    canary = protected / "source.bin"
    canary.write_bytes(b"forbidden-canary")
    real_lstat = Path.lstat
    swapped = []

    def swap_after_check(path: Path, *args, **kwargs):
        info = real_lstat(path, *args, **kwargs)
        if path == requested and not swapped:
            swapped.append(True)
            if replacement == "leaf":
                requested.rename(directory / "preserved-original.bin")
                canary.rename(requested)
            else:
                directory.rename(tmp_path / "preserved-original-directory")
                _junction(directory, protected)
        return info

    monkeypatch.setattr(Path, "lstat", swap_after_check)
    reads = _observe_reads(monkeypatch)
    rejection = None
    try:
        workflow.bounded_regular_bytes(requested)
    except workflow.WorkflowContractError as exc:
        rejection = exc
    assert swapped == [True]
    assert reads == [], "post-check replacement bytes were consumed"
    assert rejection is not None, "post-check replacement was accepted"


@pytest.fixture
def workflow_repository(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Two-task strict current registry, official index/views and real task CLI/fence."""
    for name in tuple(os.environ):
        if name.startswith("GIT_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("PYTHONPATH", str(ROOT / "src"))
    root = tmp_path / "workflow-repository"
    root.mkdir()
    for relative in (
        "AGENTS.md",
        "scripts/architecture_arch005_checkout_guard.py",
        "scripts/architecture_arch005_task_source.py",
        "docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md",
        "config/architecture/arch_005_s5_task_source_cutover.yaml",
        "config/architecture/arch_005_s4d_checkout_guard.yaml",
        "config/architecture/arch_005_parallel_control_policy.yaml",
        "config/architecture/arch_005_integration_publication_fence.yaml",
    ):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.email", "fixture@example.invalid")
    _git(root, "config", "user.name", "Workflow fixture")
    _git(root, "config", "core.autocrlf", "true")
    _git(
        root,
        "remote",
        "add",
        "origin",
        "https://github.com/AI-Trading-Collaboration/AITradingSystem.git",
    )
    (root / ".gitignore").write_bytes(b"outputs/\n")
    _admission_registry(root, {TASK: "IN_PROGRESS", TASK + "-OTHER": "IN_PROGRESS"}, "a" * 40)
    policy = canonical.load_cutover_policy(root)
    old_index = canonical._load_generated_mapping(root / canonical.CANONICAL_INDEX_PATH)
    records = old_index["fragments"]
    fragments = canonical._load_canonical_fragments(root, records)
    templates = []
    for partition in ("active", "completed"):
        relative = policy["generated_views"][partition + "_template_path"]
        target = root / relative
        target.write_bytes(b"Synthetic preserved template\n")
        templates.append(
            {
                "partition": partition,
                "path": relative,
                "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
                "byte_count": target.stat().st_size,
                "removed_task_row_count": 0,
            }
        )
    manifest = {
        "schema_version": canonical.CUTOVER_MANIFEST_SCHEMA,
        "status": "PASS",
        "task_id": policy["task_id"],
        "source_of_truth_after": canonical.CANONICAL_SOURCE,
    }
    manifest["manifest_checksum"] = canonical._payload_checksum(manifest, "manifest_checksum")
    manifest_path = root / policy["canonical"]["manifest_path"]
    manifest_path.write_bytes(canonical._yaml_bytes(manifest))
    inventory_path = root / policy["canonical"]["consumer_inventory_path"]
    inventory_path.write_bytes(canonical._yaml_bytes(canonical.build_consumer_inventory(root)))
    index = canonical._build_index(
        root=root,
        policy=policy,
        fragment_records=records,
        fragments=fragments,
        templates=templates,
        governance_cycles=[],
        manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        consumer_inventory_sha256=hashlib.sha256(inventory_path.read_bytes()).hexdigest(),
    )
    canonical._write_index_and_views(root, policy, index, fragments)
    scope = {"task_id": TASK, "decision_id": "owner_decision:DEVX-015:fixture"}
    scope_path = root / "config/architecture/workflow_fixture_scope.json"
    _json(scope_path, scope)
    authority = {
        "schema_version": "workflow_task_authority.v1",
        "task_id": TASK,
        "decision_id": scope["decision_id"],
        "status": "ACTIVE",
        "review_ref": None,
        "scope_ref": {
            "path": scope_path.relative_to(root).as_posix(),
            "sha256": hashlib.sha256(scope_path.read_bytes()).hexdigest(),
        },
    }
    _git(root, "add", ".")
    _git(root, "commit", "-m", "strict synthetic canonical registry")
    _git(root, "switch", "-c", "workflow-task")
    base = _git(root, "rev-parse", "HEAD")
    canonical.validate_canonical_registry(project_root=root)
    assert workflow.repository_identity(root)["checkout"] == root.as_posix()
    fence = IntegrationPublicationFence(project_root=root)
    binding = fence.acquire(
        transaction_id="workflow-authority",
        task_id=TASK,
        change_id="workflow-fixture",
        thread_id="fixture",
        actor="integration-coordinator",
        frozen_base_sha=base,
        lane_head_sha=base,
        expected_main_sha=base,
        owned_paths=("tests/fixture.py",),
        shared_paths=(
            "config/architecture/workflow_fixture_scope.json",
            "registry/development_tasks",
            "inputs/architecture",
            "docs/task_register.md",
            "docs/task_register_completed.md",
        ),
        generator_ids=("canonical-task-source",),
    )
    transaction = root / binding["transaction_path"]
    fence.checkpoint(transaction, phase="TASK_SOURCE_PRE_WRITE", actor="integration-coordinator")
    authority_path = root / "outputs/fixture-authority.json"
    _json(authority_path, authority)

    def update(change: str, *options: str):
        command = [
            sys.executable,
            str(root / "scripts/architecture_arch005_task_source.py"),
            "update",
            "--task-id",
            TASK,
            "--actor",
            "integration-coordinator",
            "--change-id",
            change,
            "--occurred-at",
            "2026-09-11T10:00:00+00:00",
            "--base-commit",
            base,
            "--publication-transaction",
            str(transaction),
            *options,
        ]
        result = subprocess.run(command, cwd=root, capture_output=True, text=True, encoding="utf-8")
        assert result.returncode == 0, result.stdout + result.stderr
        return json.loads(result.stdout)

    yield root, authority, authority_path, update
    fence.release(transaction, actor="integration-coordinator", outcome="failed")


def test_authority_persists_across_official_cli_ordinary_updates(workflow_repository) -> None:
    root, authority, path, update = workflow_repository
    update("bind-authority", "--workflow-authority", str(path))
    first = workflow.load_current_task_authority(root, TASK)
    update("ordinary-note", "--notes", "ordinary progress, no authority override")
    second = workflow.load_current_task_authority(root, TASK)
    assert first["authority"] == second["authority"] == authority
    assert first["authority_event_id"] == second["authority_event_id"]
    assert first["current_event_id"] != second["current_event_id"]
    fragment = canonical.validate_canonical_registry(project_root=root).fragment(TASK)
    assert "workflow_authority" not in fragment["events"][-1]["payload"]
    assert fragment["task_record"]["workflow_authority"] == authority


@pytest.mark.parametrize("entry", ["authority", "structure"])
@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ("revoked", "WORKFLOW_TASK_AUTHORITY_REVOKED"),
        ("terminal", "WORKFLOW_TASK_TERMINAL"),
        ("prefix", "TASK_NOT_FOUND"),
        ("malformed", "INDEX_FILE_HASH"),
        ("scope-drift", "WORKFLOW_REFERENCE_DRIFT"),
        ("stale-markdown", "GENERATED_VIEW_DRIFT"),
    ],
)
def test_current_authority_rejects_invalid_context(
    workflow_repository, mutation: str, expected_code: str, entry: str,
) -> None:
    root, authority, path, update = workflow_repository
    update("bind-authority", "--workflow-authority", str(path))
    reader = {
        "authority": workflow.load_current_task_authority,
        "structure": workflow.load_current_task_structure,
    }[entry]
    # First reach the real entry with a valid current authority. A fixture or
    # import failure is not evidence that the selected identity fault is denied.
    assert reader(root, TASK)["authority"] == authority
    task_id = TASK
    expected = workflow.WorkflowContractError
    if mutation == "revoked":
        authority["status"] = "REVOKED"
        _json(path, authority)
        update("revoke", "--workflow-authority", str(path))
    elif mutation == "terminal":
        update("finish", "--status", "DONE")
    elif mutation == "prefix":
        task_id = TASK[:-1]
        expected = canonical.CanonicalTaskRegistryError
    elif mutation == "malformed":
        fragment = root / canonical._canonical_fragment_path(TASK)
        fragment.write_bytes(b"corrupted current authority\n")
        expected = canonical.CanonicalTaskRegistryError
    elif mutation == "stale-markdown":
        view = root / "docs/task_register.md"
        stale_view = view.read_bytes()
        update("finish-with-stale-view", "--status", "DONE")
        assert view.read_bytes() != stale_view
        # Retain the old ACTIVE row while the actual canonical task is DONE.
        # No re-sealed index or synthetic replacement authority is supplied.
        view.write_bytes(stale_view)
        expected = canonical.CanonicalTaskRegistryError
    else:
        _json(root / authority["scope_ref"]["path"], {"task_id": "forged"})

    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/workflow-authority/transaction.json"
    observed_paths = [
        root / canonical.CANONICAL_INDEX_PATH,
        root / canonical._canonical_fragment_path(TASK),
        root / canonical._canonical_fragment_path(TASK + "-OTHER"),
        root / "config/architecture/arch_005_s5_task_source_cutover.yaml",
        root / authority["scope_ref"]["path"],
        root / "docs/task_register.md",
        root / "docs/task_register_completed.md",
        root / ".git/index",
    ]

    def snapshot():
        return {
            "head": _git(root, "rev-parse", "HEAD"),
            "refs": _git(root, "show-ref"),
            "inputs": {item.relative_to(root).as_posix(): item.read_bytes()
                       for item in observed_paths},
            "outputs": {item.relative_to(root).as_posix(): item.read_bytes()
                        for item in (root / "outputs").rglob("*") if item.is_file()},
            "publication": fence.replay(transaction),
            "leases": fence.guard.store.replay(),
        }

    before = snapshot()
    assert all(lease.execution is None for lease in before["leases"].active_leases)
    with pytest.raises(expected) as rejected:
        reader(root, task_id)
    assert rejected.value.code == expected_code
    assert snapshot() == before, "rejected task admission changed authoritative state"
