"""Real synthetic Git/guard source-only checkpoints; no publication authority.

Ordinary in-process fixtures substitute only loaded-module origin metadata.
Implementation files, historical Git blobs, linked worktrees, scope leases and
checkpoint evidence remain real; no implementation binding/verifier is mocked.
The separately named subprocess E2E does not substitute loaded origins either.
Selected fault cases commit finite instrumentation in their copied worker before
binding, then independently observe its real Job membership at durable barriers.
"""

from __future__ import annotations

import ast
import copy
import ctypes
import importlib.util
import json
import os
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
from test_arch_005_source_preservation import (
    ACTOR,
    BRANCH,
    EXCLUDED,
    EXCLUSION,
    POLICIES,
    ROOT,
    SOURCE_TASK,
    TASK_PATH,
    _commit,
    _fragment,
    _git,
    _json,
    _ref,
    _sha,
    _write,
)

from ai_trading_system.platform.architecture import source_preservation as safe
from ai_trading_system.platform.architecture import task_checkpoint as checkpoint
from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutLeaseGuard,
    CheckoutOperationClass,
)
from ai_trading_system.platform.architecture.parallel_control_kernel import FileExecutionLeaseStore
from ai_trading_system.platform.architecture.task_checkpoint import (
    POLICY_PATH,
    TaskCheckpoint,
    TaskCheckpointError,
)

RUNTIME = "outputs/architecture/arch_005_task_checkpoints"
UNREQUESTED_TRACKED_PATH = "src/unrequested-tracked.py"
UNREQUESTED_BASE_BYTES = b"unrequested committed base\n"
# Capture real paths at collection, before an individual fixture substitutes
# origin metadata. These are copied byte-for-byte into each synthetic Git repo.
MODULE_ORIGINS = {
    name: Path(sys.modules[name].__file__).resolve().relative_to(ROOT).as_posix()
    for name in (*safe._IMPLEMENTATION_MODULES, checkpoint.__name__, *checkpoint._EXECUTION_MODULES)
}
IMPLEMENTATION_PATHS = tuple(
    sorted(
        {
            *MODULE_ORIGINS.values(),
            safe.MODULE_PATH,
            safe.CLI_PATH,
            safe.POLICY_PATH,
            checkpoint.MODULE_PATH,
            checkpoint.CLI_PATH,
            checkpoint.POLICY_PATH,
        }
    )
)


@cache
def _fixture_import_paths() -> tuple[str, ...]:
    """Copy real import-only package dependencies without changing authority.

    The production verifier still requires its exact finite implementation set.
    Package initializers also import unrelated exported API definitions; a fresh
    CLI must load those real files, not stub their modules or rewrite __init__.
    Traverse only declared project imports, never repository-wide discovery.
    """
    paths = set(IMPLEMENTATION_PATHS)
    pending = [path for path in paths if path.endswith(".py")]
    visited: set[str] = set()

    def add_module(name: str) -> None:
        if name != "ai_trading_system" and not name.startswith("ai_trading_system."):
            return
        parts = name.split(".")
        for length in range(1, len(parts) + 1):
            stem = ROOT / "src" / Path(*parts[:length])
            candidates = (stem.with_suffix(".py"), stem / "__init__.py")
            for candidate in candidates:
                if candidate.is_file():
                    relative = candidate.relative_to(ROOT).as_posix()
                    if relative not in paths:
                        paths.add(relative)
                        pending.append(relative)

    while pending:
        relative = pending.pop()
        if relative in visited:
            continue
        visited.add(relative)
        path = Path(relative)
        if path.parts[0] != "src":
            continue
        module = ".".join(path.with_suffix("").parts[1:])
        package = (
            module.removesuffix(".__init__")
            if path.name == "__init__.py"
            else module.rpartition(".")[0]
        )
        for node in ast.walk(ast.parse((ROOT / path).read_text(encoding="utf-8-sig"))):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    add_module(alias.name)
            elif isinstance(node, ast.ImportFrom):
                name = node.module or ""
                if node.level:
                    name = importlib.util.resolve_name("." * node.level + name, package)
                add_module(name)
                for alias in node.names:
                    if alias.name != "*":
                        add_module(name + "." + alias.name)
    return tuple(sorted(paths))


@dataclass(frozen=True)
class CheckpointCase:
    root: Path
    main_root: Path
    scope: dict[str, Any]
    expected: dict[str, bytes | None]

    @property
    def destination(self) -> Path:
        return self.root / RUNTIME / self.scope["checkpoint_id"]


def _case(
    tmp_path: Path,
    *,
    large: bool = False,
    budget: dict[str, int] | None = None,
    worker_probe: tuple[str, str] | None = None,
    worker_mutation: str | None = None,
) -> CheckpointCase:
    main = tmp_path / "synthetic-checkpoint-main"
    main.mkdir()
    _git(main, "init", "-b", "main")
    _git(main, "config", "user.email", "checkpoint@example.invalid")
    _git(main, "config", "user.name", "Synthetic Checkpoint")
    fixture_files = tuple(sorted({*POLICIES, *_fixture_import_paths()}))
    for relative in fixture_files:
        _write(main, relative, (ROOT / relative).read_bytes())
    if worker_mutation is not None:
        _instrument_m07_worker(main, worker_mutation)
    if worker_probe is not None:
        _instrument_worker(main, worker_probe)
    if budget:
        policy = yaml.safe_load((main / POLICY_PATH).read_bytes())
        policy.update(budget)
        _write(main, POLICY_PATH, yaml.safe_dump(policy).encode())
    _write(main, ".gitignore", b"outputs/\n")
    mods = [f"src/modify-{i:03d}.py" for i in range(45 if large else 1)]
    deletes = [f"src/delete-{i:03d}.py" for i in range(23 if large else 1)]
    adds = [f"src/add-{i:03d}.bin" for i in range(24 if large else 1)]
    for path in (*mods, *deletes):
        _write(main, path, b"original\n")
    _write(main, UNREQUESTED_TRACKED_PATH, UNREQUESTED_BASE_BYTES)
    seed = _commit(
        main,
        (*fixture_files, ".gitignore", *mods, *deletes, UNREQUESTED_TRACKED_PATH),
        "synthetic seed",
    )
    if worker_mutation is not None:
        module = "src/ai_trading_system/platform/architecture/task_checkpoint.py"
        raw = (main / module).read_bytes()
        assert b"\r\n" not in raw
        assert _git(main, "cat-file", "blob", f"{seed}:{module}") == raw
    _write(main, TASK_PATH, _json(_fragment(seed)) + b"\n")
    frozen = _commit(main, (TASK_PATH,), "synthetic canonical task")
    source = tmp_path / "synthetic-checkpoint-source"
    _git(main, "worktree", "add", "-b", BRANCH, str(source), frozen)
    _git(main, "update-ref", "refs/remotes/origin/main", frozen)
    paths = sorted((*mods, *deletes, *adds), key=str.casefold)
    guard = CheckoutLeaseGuard(
        project_root=source,
        policy_path=source / POLICIES[1],
        parallel_policy_path=source / POLICIES[2],
    )
    decision, handle = guard.acquire(
        intent_id="synthetic-checkpoint-original-scope",
        task_id=SOURCE_TASK,
        thread_id="synthetic-checkpoint-thread",
        actor=ACTOR,
        operation_class=CheckoutOperationClass.DOMAIN_MUTATION,
        owned_paths=paths,
        base_commit=frozen,
    )
    assert decision.status == "PASS" and handle is not None
    expected: dict[str, bytes | None] = {}
    for path in mods:
        expected[path] = f"changed {path}\r\n".encode()
    for path in adds:
        expected[path] = b"new\x00binary\xff\r\n" + path.encode()
    for path in deletes:
        expected[path] = None
    for path, content in expected.items():
        if content is None:
            (source / path).unlink()
        else:
            _write(source, path, content)
    handle.release(outcome="completed")
    intent = decision.intent_path
    scope = {
        "schema_version": "task_checkpoint_scope.v1",
        "checkpoint_id": "synthetic-checkpoint-v1",
        "task_id": SOURCE_TASK,
        "actor": ACTOR,
        "thread_id": "synthetic-checkpoint-thread",
        "source_root": source.as_posix(),
        "source_common_git_dir": Path(
            _git(source, "rev-parse", "--path-format=absolute", "--git-common-dir").decode().strip()
        ).as_posix(),
        "source_branch": BRANCH,
        "source_head_sha": frozen,
        "frozen_base_sha": frozen,
        "scope_intent": {
            "path": intent.relative_to(source).as_posix(),
            "sha256": _sha(intent.read_bytes()),
            "lease_id": handle.lease_id,
        },
        "paths": paths,
    }
    return CheckpointCase(source, main, scope, expected)


def _instrument_m07_worker(main: Path, mutation: str) -> None:
    """Mutate only the copied capture worker before its real seed commit."""
    assert mutation in {"delete", "normalize"}
    relative = "src/ai_trading_system/platform/architecture/task_checkpoint.py"
    path = main / relative
    source = path.read_text(encoding="utf-8")
    owner = next(node for node in ast.parse(source).body
                 if isinstance(node, ast.ClassDef) and node.name == "TaskCheckpoint")
    method = next(node for node in owner.body
                  if isinstance(node, ast.FunctionDef) and node.name == "capture_worker")
    lines = source.splitlines(keepends=True)
    before = "".join(lines[method.lineno - 1:method.end_lineno])
    if mutation == "delete":
        target = " " * 16 + '''index_entries.append(f"0 {'0' * 40}\\t{row['path']}\\0".encode())'''
        replacement = "                pass  # M07: omit the actual deletion index entry."
    else:
        target = '''                        content=captures[row["path"]],'''
        replacement = " " * 24 + '''content=captures[row["path"]].replace(b"\\r\\n", b"\\n"),'''
    assert before.count(target) == 1, "INVALID_MUTATION_TARGET"
    after = before.replace(target, replacement)
    changed = "".join(lines[:method.lineno - 1]) + after + "".join(lines[method.end_lineno:])
    compile(changed, str(path), "exec")
    path.write_bytes(changed.encode("utf-8"))
    (main.parent / "m07-method-before.py").write_text(before, encoding="utf-8")
    (main.parent / "m07-method-after.py").write_text(after, encoding="utf-8")
    (main.parent / "m07-implementation.json").write_bytes(_json({
        "mutation": mutation, "module_sha256": _sha(path.read_bytes()),
        "before_method_sha256": _sha(before.encode()), "after_method_sha256": _sha(after.encode()),
    }))


def _instrument_worker(main: Path, probe: tuple[str, str]) -> None:
    """Commit finite fault instrumentation in the synthetic worker before binding.

    Real implementation, Git, lease and Job verification remain active. Nothing
    in production reads a test switch; only this copied fixture contains probes.
    """
    mode, phase = probe
    assert (
        (mode == "barrier" and phase in {"CAPTURED", "OBJECTS_WRITTEN", "REF_CREATED"})
        or (mode == "interrupt" and phase in {"CAPTURED", "OBJECTS_WRITTEN"})
        or (mode == "commit" and phase == "COMMIT_TREE")
        or (mode == "read-check" and phase == "SOURCE_READ_CHECKED")
        or (mode == "raw-write" and phase in {"BEFORE_RAW", "PARTIAL_RAW", "AFTER_RAW"})
    )
    directory = main.parent / "probes"
    directory.mkdir()
    instrumentation = f"""

# Synthetic fixture-only instrumentation; these bytes precede the seed commit.
if len(sys.argv) > 1 and sys.argv[1] == "capture-worker":
    import time as _probe_time
    _probe_directory = Path({str(directory)!r})
    _probe_mode = {mode!r}
    _probe_phase = {phase!r}

    def _probe_observe(run, facts):
        facts = {{**facts, "pid": os.getpid(), "phase": _probe_phase}}
        pending = _probe_directory / "reached.pending"
        pending.write_bytes(safe._json_bytes(facts))
        os.replace(pending, _probe_directory / "reached.json")
        deadline = _probe_time.monotonic() + 90
        while not (_probe_directory / "continue").exists():
            if _probe_time.monotonic() >= deadline:
                raise RuntimeError("synthetic worker probe timed out")
            _probe_time.sleep(.02)

    if _probe_mode == "read-check":
        _probe_target = _probe_directory.parent / "synthetic-checkpoint-source/src/add-000.bin"
        _probe_active = False
        _probe_done = False
        _probe_real_lstat = Path.lstat

        def _probe_lstat(path, *args, **kwargs):
            global _probe_done
            info = _probe_real_lstat(path, *args, **kwargs)
            if _probe_active and path == _probe_target and not _probe_done:
                _probe_done = True
                run = Path(sys.argv[sys.argv.index("--execution-request") + 1]).parent
                _probe_observe(run, {{"identity": [info.st_dev, info.st_ino]}})
            return info

        def _probe_wrap(function):
            def wrapped(path, *args, **kwargs):
                global _probe_active
                active = path == _probe_target
                _probe_active = active
                try:
                    result = function(path, *args, **kwargs)
                    if active:
                        (_probe_directory / "reader_returned.json").write_bytes(
                            safe._json_bytes({{"sha256": safe._sha(result), "size": len(result)}})
                        )
                    return result
                finally:
                    _probe_active = False
            return wrapped

        Path.lstat = _probe_lstat
        safe._regular = _probe_wrap(safe._regular)
        workflow_contract.bounded_regular_bytes = _probe_wrap(
            workflow_contract.bounded_regular_bytes
        )
    elif _probe_mode == "raw-write":
        _probe_real_write = safe._write_once
        _probe_raw_done = False

        def _probe_raw_write(path, content):
            global _probe_raw_done
            if _probe_raw_done or path.parent.name != "capture" or len(content) < 2:
                return _probe_real_write(path, content)
            _probe_raw_done = True
            run = path.parent.parent
            if _probe_phase == "AFTER_RAW":
                _probe_real_write(path, content)
            elif _probe_phase == "PARTIAL_RAW":
                # Fault injection only: leave an actually flushed partial file
                # at this exact disposable capture destination, then stop.
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("xb") as stream:
                    stream.write(content[:len(content) // 2])
                    stream.flush()
                    os.fsync(stream.fileno())
            _probe_observe(run, {{"storage_path": path.relative_to(run).as_posix()}})
            raise RuntimeError("raw write fault must terminate the original worker")

        safe._write_once = _probe_raw_write
    elif _probe_mode in {{"barrier", "interrupt"}}:
        _probe_real_event = TaskCheckpoint._event

        def _probe_event(self, run, events, phase, payload):
            result = _probe_real_event(self, run, events, phase, payload)
            if phase == _probe_phase:
                _probe_observe(run, {{"event_id": events[-1]["event_id"]}})
                if _probe_mode == "interrupt":
                    raise RuntimeError("synthetic capture interruption")
            return result

        TaskCheckpoint._event = _probe_event
    else:
        _probe_real_git = safe.SourcePreservation._git

        def _probe_git(self, root, *args, **kwargs):
            if args[0] != "commit-tree":
                return _probe_real_git(self, root, *args, **kwargs)
            run = Path(sys.argv[sys.argv.index("--execution-request") + 1]).parent
            intent = _json(run / "object_intent.json")
            request = _json(run / "request.json")
            assert intent["tree"] == args[1]
            assert intent["parent"] == request["source_head_sha"]
            assert intent["timestamp"] == kwargs["timestamp"]
            first = _probe_real_git(self, root, *args, **kwargs)
            second = _probe_real_git(self, root, *args, **kwargs)
            assert first == second
            _probe_observe(run, {{
                "intent_sha256": safe._sha(_bytes(run / "object_intent.json")),
                "first_oid": first.decode().strip(),
                "second_oid": second.decode().strip(),
            }})
            return first

        safe.SourcePreservation._git = _probe_git
"""
    path = main / checkpoint.MODULE_PATH
    path.write_bytes(path.read_bytes() + instrumentation.encode())


@contextmanager
def _worker_probe(
    case: CheckpointCase, action: Callable[[], Any] | None = None
) -> Iterator[list[dict[str, Any]]]:
    """Observe the actual contained worker before permitting its next action."""
    from test_devx015_workflow_execution import NativeOracle

    directory = case.main_root.parent / "probes"
    observations: list[dict[str, Any]] = []
    errors: list[BaseException] = []
    stop = threading.Event()

    def observe() -> None:
        try:
            deadline = time.monotonic() + 120
            marker = directory / "reached.json"
            while not marker.exists():
                if stop.wait(0.02) or time.monotonic() >= deadline:
                    raise AssertionError("real worker never reached requested probe")
            value = json.loads(marker.read_bytes())
            execution = json.loads((case.destination / "execution_request.json").read_bytes())
            witness = json.loads((case.destination / "worker_observation.json").read_bytes())
            if value["phase"] not in {
                "COMMIT_TREE", "SOURCE_READ_CHECKED", "BEFORE_RAW", "PARTIAL_RAW", "AFTER_RAW",
            }:
                events = list((case.destination / "events").glob(f"*-{value['phase']}.json"))
                assert len(events) == 1
                assert json.loads(events[0].read_bytes())["event_id"] == value["event_id"]
            oracle = NativeOracle()
            with oracle.process(value["pid"]) as native:
                assert not oracle.exited(native)
                oracle.assert_in_job(native, execution["job_name"])
                assert witness["worker_process"] == {
                    "pid": value["pid"],
                    "creation_time": oracle.creation_time(native),
                }
                if action is not None:
                    action()
                observations.append(value)
        except BaseException as exc:
            errors.append(exc)
        finally:
            (directory / "continue").write_bytes(b"continue")
            # Actions may arm overlapped native observations on this thread.
            # Keep their issuing thread alive until the observed operation ends;
            # Windows cancels its pending I/O when that thread exits.
            stop.wait()

    thread = threading.Thread(target=observe, name="synthetic-worker-probe", daemon=True)
    thread.start()
    try:
        yield observations
    finally:
        stop.set()
        (directory / "continue").write_bytes(b"continue")
        thread.join(timeout=10)
        assert not thread.is_alive(), "probe observer did not terminate"
        if errors:
            raise errors[0]
        assert len(observations) == 1, "requested real-worker probe was not observed once"


@pytest.fixture
def controlled_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in tuple(os.environ):
        if key.upper().startswith("GIT_"):
            monkeypatch.delenv(key)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_OPTIONAL_LOCKS", "0")


@pytest.fixture
def case(tmp_path: Path, controlled_environment: None) -> CheckpointCase:
    return _case(tmp_path)


def _engine(case: CheckpointCase, monkeypatch: pytest.MonkeyPatch) -> TaskCheckpoint:
    # Only the loaded-origin metadata boundary is substituted; binding and
    # historical validation execute their real production code against commits.
    for name, relative in MODULE_ORIGINS.items():
        monkeypatch.setattr(sys.modules[name], "__file__", str(case.main_root / relative))
    result = TaskCheckpoint(project_root=case.main_root)
    return result


@pytest.fixture
def engine(case: CheckpointCase, monkeypatch: pytest.MonkeyPatch) -> TaskCheckpoint:
    return _engine(case, monkeypatch)


def _state(case: CheckpointCase) -> dict[str, Any]:
    index = Path(
        _git(case.root, "rev-parse", "--path-format=absolute", "--git-path", "index")
        .decode()
        .strip()
    )
    configurations = {}
    for name in ("config", "config.worktree"):
        path = Path(
            _git(case.root, "rev-parse", "--path-format=absolute", "--git-path", name)
            .decode()
            .strip()
        )
        configurations[name] = path.read_bytes() if path.is_file() else None
    return {
        "head": _ref(case.root, "HEAD"),
        "branch": _git(case.root, "branch", "--show-current"),
        "index": index.read_bytes(),
        "git_configuration": configurations,
        "files": {
            path: (case.root / path).read_bytes() if (case.root / path).exists() else None
            for path in case.expected
        },
    }


def _advance_main(case: CheckpointCase) -> str:
    # B is the real main worktree, with its own index and HEAD. No direct shared
    # ref rewrite or mocked Git response stands in for another writer.
    _write(case.main_root, "src/main-only.py", b"independent main writer\n")
    head = _commit(case.main_root, ("src/main-only.py",), "synthetic B advances main")
    _git(case.main_root, "update-ref", "refs/remotes/origin/main", head)
    return head


def _receipt(case: CheckpointCase, result: dict[str, Any]) -> Path:
    path = Path(result["receipt_path"])
    return path if path.is_absolute() else case.root / path


def _assert_snapshot(case: CheckpointCase, engine: TaskCheckpoint, request: dict[str, Any]) -> Path:
    before = _state(case)
    result = engine.capture(request)
    path = _receipt(case, result)
    validated = engine.validate(path)
    assert validated["status"] == "PASS"
    assert _state(case) == before
    ref = f"refs/aits/task-checkpoints/{case.scope['checkpoint_id']}"
    snapshot = _ref(case.root, ref)
    assert _git(case.root, "rev-list", "--parents", "-n", "1", snapshot).decode().split() == [
        snapshot,
        case.scope["source_head_sha"],
    ]
    changed = (
        _git(
            case.root,
            "diff",
            "--name-only",
            "-z",
            case.scope["source_head_sha"],
            snapshot,
            "--",
            ".",
            EXCLUSION,
        )
        .decode()
        .strip("\0")
        .split("\0")
    )
    assert set(changed) == set(case.expected)
    for relative, content in case.expected.items():
        if content is None:
            assert _git(case.root, "ls-tree", snapshot, "--", relative) == b""
        else:
            assert _git(case.root, "cat-file", "blob", f"{snapshot}:{relative}") == content
    for key in (
        "full_allowed",
        "main_ff_allowed",
        "push_allowed",
        "generator_allowed",
        "research_allowed",
        "trading_allowed",
    ):
        assert result["safety"][key] is False
        assert validated["safety"][key] is False
    return path


def test_plan_is_read_only_and_records_explicit_add_modify_delete(
    case: CheckpointCase, engine: TaskCheckpoint
) -> None:
    before = _state(case)
    request = engine.plan(case.scope)
    assert request["schema_version"] == "task_checkpoint_request.v1"
    assert {row["operation"] for row in request["files"]} == {"ADD", "MODIFY", "DELETE"}
    for row in request["files"]:
        content = case.expected[row["path"]]
        assert row["sha256"] == (_sha(content) if content is not None else None)
        assert row["size_bytes"] == (len(content) if content is not None else 0)
    assert _state(case) == before
    assert not case.destination.exists()
    _assert_snapshot(case, engine, request)


@pytest.mark.parametrize("mutation", [None, "delete", "normalize"],
                         ids=["original", "M07-delete", "M07-normalize"])
def test_m07_real_worker_snapshot_refuses_delete_and_byte_loss(
    tmp_path: Path, controlled_environment: None, monkeypatch: pytest.MonkeyPatch,
    mutation: str | None,
) -> None:
    case = _case(tmp_path, worker_mutation=mutation)
    engine = _engine(case, monkeypatch)
    before = _state(case)
    if mutation is None:
        test_plan_is_read_only_and_records_explicit_add_modify_delete(case, engine)
    else:
        with pytest.raises(TaskCheckpointError, match="TASK_CHECKPOINT_EXECUTION"):
            test_plan_is_read_only_and_records_explicit_add_modify_delete(case, engine)
        run = case.destination
        output = (run / "worker.stdout.log").read_text(encoding="utf-8")
        assert "TASK_CHECKPOINT_SNAPSHOT" in output, output
        expected = (
            "delete not represented exactly" if mutation == "delete" else "blob size mismatch"
        )
        assert expected in output, output
        intent = json.loads((run / "object_intent.json").read_bytes())
        tree = intent["tree"]
        assert _git(case.root, "cat-file", "-t", tree).strip() == b"tree"
        if mutation == "delete":
            assert _git(case.root, "cat-file", "blob", f"{tree}:src/delete-000.py") == b"original\n"
            assert case.expected["src/delete-000.py"] is None
        else:
            original = case.expected["src/add-000.bin"]
            actual = _git(case.root, "cat-file", "blob", f"{tree}:src/add-000.bin")
            assert actual == original.replace(b"\r\n", b"\n") != original
        assert not (run / "receipt.json").exists()
        assert _git(case.root, "for-each-ref", "refs/aits/task-checkpoints/") == b""
        assert (run / "failure.json").is_file()
        (tmp_path / "m07-counterfactual.json").write_bytes(_json({
            "mutant_id": "M07", "mutation": mutation, "target_assertion_killed": True,
            "original_success_test": (
                "test_plan_is_read_only_and_records_explicit_add_modify_delete"
            ),
            "worker_reason": "TASK_CHECKPOINT_SNAPSHOT", "detail": expected, "actual_tree": tree,
            "scope": "ACTUAL_CONTAINED_CAPTURE_WORKER_AND_GIT_TREE",
            "formal_project_acceptance": False,
        }))
    assert _state(case) == before
    guard = CheckoutLeaseGuard(
        project_root=case.root, policy_path=case.root / POLICIES[1],
        parallel_policy_path=case.root / POLICIES[2],
    )
    replay = guard.store.replay()
    assert replay.status == "PASS" and not replay.active_leases


def test_92_changes_survive_real_linked_worktree_main_advance(
    tmp_path: Path, controlled_environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = _case(tmp_path, large=True)
    engine = _engine(case, monkeypatch)
    request = engine.plan(case.scope)
    assert len(request["files"]) == 92
    newer = _advance_main(case)
    _assert_snapshot(case, engine, request)
    assert _ref(case.root, "main") == _ref(case.root, "origin/main") == newer


@pytest.mark.parametrize("staging", ["requested", "unrequested", "mixed", "empty"])
def test_s01_staged_index_and_raw_worktree_bytes_are_preserved(
    case: CheckpointCase, engine: TaskCheckpoint, staging: str
) -> None:
    """V3 changes old staged rejection; the snapshot never substitutes index bytes."""
    if staging in {"requested", "mixed", "empty"}:
        _git(case.root, "add", "-A", "--", *(f":(literal){p}" for p in case.expected))
    if staging == "unrequested":
        _write(case.root, UNREQUESTED_TRACKED_PATH, b"unrequested staged source\n")
        _git(case.root, "add", "--", f":(literal){UNREQUESTED_TRACKED_PATH}")
    if staging in {"mixed", "empty"}:
        current = b"" if staging == "empty" else b"unstaged raw\x00\xff\r\n"
        _write(case.root, "src/add-000.bin", current)
        case.expected["src/add-000.bin"] = current
    before = _state(case)
    staged = _git(case.root, "ls-files", "--stage", "-z")
    request = engine.plan(case.scope)
    receipt = _assert_snapshot(case, engine, request)
    assert _state(case) == before
    assert _git(case.root, "ls-files", "--stage", "-z") == staged
    assert (receipt.parent / "source.index").read_bytes() == before["index"]
    # Snapshot's unrequested path comes from HEAD, never the caller's staged change.
    snapshot = json.loads(receipt.read_bytes())["snapshot"]["commit"]
    assert _git(case.root, "cat-file", "blob", f"{snapshot}:{UNREQUESTED_TRACKED_PATH}") == (
        UNREQUESTED_BASE_BYTES
    )


def test_old_validated_checkpoint_remains_true_after_source_and_main_change(
    case: CheckpointCase, engine: TaskCheckpoint
) -> None:
    request = engine.plan(case.scope)
    path = _assert_snapshot(case, engine, request)
    original = engine.validate(path)
    _write(case.root, "src/modify-000.py", b"later legitimate task work\n")
    _advance_main(case)
    assert engine.validate(path) == original
    assert original["safety"]["main_ff_allowed"] is False


@pytest.mark.parametrize("mutation", ["bytes", "head", "index", "config"])
def test_source_drift_fails_closed(
    case: CheckpointCase, engine: TaskCheckpoint, mutation: str
) -> None:
    request = engine.plan(case.scope)
    if mutation == "bytes":
        _write(case.root, "src/modify-000.py", b"different after plan\n")
    elif mutation == "head":
        _git(case.root, "commit", "--allow-empty", "-m", "new task HEAD")
    elif mutation == "index":
        _git(case.root, "add", "--", ":(literal)src/modify-000.py")
    else:
        _git(case.root, "config", "diff.algorithm", "patience")
    before = _state(case)
    with pytest.raises(TaskCheckpointError) as failure:
        engine.capture(request)
    assert failure.value.code.startswith("TASK_CHECKPOINT_")
    assert _state(case) == before
    assert not (case.destination / "receipt.json").exists()


@pytest.mark.parametrize(
    "mutation",
    [
        "unowned_request",
        "wrong_task",
        "wrong_actor",
        "wrong_lease",
        "intent_hash",
    ],
)
def test_scope_and_source_admission_are_not_publication_bypasses(
    case: CheckpointCase, engine: TaskCheckpoint, mutation: str
) -> None:
    scope = copy.deepcopy(case.scope)
    if mutation == "unowned_request":
        _write(case.root, "src/unattributed.py", b"not owned\n")
        scope["paths"] = sorted([*scope["paths"], "src/unattributed.py"], key=str.casefold)
    elif mutation == "wrong_task":
        scope["task_id"] = "SYNTHETIC_WRONG_TASK"
    elif mutation == "wrong_actor":
        scope["actor"] = "architecture-control-plane"
    elif mutation == "wrong_lease":
        scope["scope_intent"]["lease_id"] = "lease-does-not-exist"
    else:
        scope["scope_intent"]["sha256"] = "0" * 64
    with pytest.raises(TaskCheckpointError) as failure:
        engine.plan(scope)
    assert failure.value.code.startswith("TASK_CHECKPOINT_")
    assert not case.destination.exists()


def test_excluded_path_is_rejected_before_content_read(
    case: CheckpointCase, engine: TaskCheckpoint, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write(case.root, EXCLUDED, b"synthetic forbidden owner content\n")
    scope = copy.deepcopy(case.scope)
    scope["paths"] = sorted([*scope["paths"], EXCLUDED], key=str.casefold)
    actual = Path.open

    def forbidden(path: Path, *args: Any, **kwargs: Any) -> Any:
        assert path != case.root / EXCLUDED, "excluded bytes must never be read"
        return actual(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", forbidden)
    with pytest.raises(TaskCheckpointError):
        engine.plan(scope)


def test_existing_ref_never_overwritten(case: CheckpointCase, engine: TaskCheckpoint) -> None:
    request = engine.plan(case.scope)
    ref = f"refs/aits/task-checkpoints/{case.scope['checkpoint_id']}"
    _git(case.root, "update-ref", ref, case.scope["source_head_sha"])
    with pytest.raises(TaskCheckpointError):
        engine.capture(request)
    assert _ref(case.root, ref) == case.scope["source_head_sha"]


def test_identical_success_is_idempotent_but_different_request_cannot_reuse_id(
    case: CheckpointCase, engine: TaskCheckpoint
) -> None:
    request = engine.plan(case.scope)
    path = _assert_snapshot(case, engine, request)
    before = {
        p.relative_to(case.destination).as_posix(): p.read_bytes()
        for p in case.destination.rglob("*")
        if p.is_file()
    }
    assert _receipt(case, engine.capture(copy.deepcopy(request))) == path
    assert {
        p.relative_to(case.destination).as_posix(): p.read_bytes()
        for p in case.destination.rglob("*")
        if p.is_file()
    } == before
    different = copy.deepcopy(request)
    different["thread_id"] = "different-thread"
    with pytest.raises(TaskCheckpointError):
        engine.capture(different)
    assert path.read_bytes() == before["receipt.json"]


@pytest.mark.parametrize(
    "target", ["receipt", "request", "event", "source_index", "scope_lease", "checkpoint_lease"]
)
def test_evidence_tamper_invalidates_independent_validation(
    case: CheckpointCase, engine: TaskCheckpoint, target: str
) -> None:
    receipt = _assert_snapshot(case, engine, engine.plan(case.scope))
    path = receipt if target == "receipt" else case.destination / "request.json"
    if target == "event":
        path = sorted((case.destination / "events").glob("*.json"))[0]
    elif target in {"scope_lease", "checkpoint_lease"}:
        path = sorted((case.destination / "authority" / target).glob("*.json"))[0]
    elif target == "source_index":
        path = case.destination / "source.index"
        path.write_bytes(path.read_bytes() + b"synthetic corruption")
        with pytest.raises(TaskCheckpointError):
            engine.validate(receipt)
        return
    value = json.loads(path.read_bytes())
    value["synthetic_tamper"] = True
    path.write_bytes(_json(value))
    with pytest.raises(TaskCheckpointError):
        engine.validate(receipt)


def test_incomplete_capture_directory_never_reused(
    case: CheckpointCase, engine: TaskCheckpoint
) -> None:
    request = engine.plan(case.scope)
    case.destination.mkdir(parents=True)
    marker = case.destination / "request.json"
    marker.write_bytes(_json(request))
    before = marker.read_bytes()
    with pytest.raises(TaskCheckpointError):
        engine.capture(request)
    assert marker.read_bytes() == before
    assert not (case.destination / "receipt.json").exists()


@pytest.mark.parametrize("phase", ["ACQUIRED", "CAPTURED", "OBJECTS_WRITTEN", "REF_CREATED"])
def test_main_advance_during_capture_does_not_invalidate_source_identity(
    tmp_path: Path, controlled_environment: None, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    case = _case(tmp_path, worker_probe=None if phase == "ACQUIRED" else ("barrier", phase))
    engine = _engine(case, monkeypatch)
    request = engine.plan(case.scope)
    actual = engine._event
    moved: list[str] = []

    def advance(run: Path, events: list[Any], current: str, payload: dict[str, Any]) -> Any:
        result = actual(run, events, current, payload)
        if current == phase:
            moved.append(_advance_main(case))
        return result

    if phase == "ACQUIRED":
        monkeypatch.setattr(engine, "_event", advance)
        _assert_snapshot(case, engine, request)
    else:
        with _worker_probe(case, lambda: moved.append(_advance_main(case))):
            _assert_snapshot(case, engine, request)
    assert len(moved) == 1
    assert _ref(case.root, "main") == moved[0]


@pytest.mark.parametrize("phase", ["ACQUIRED", "CAPTURED", "OBJECTS_WRITTEN"])
def test_interrupted_capture_retains_source_and_refuses_automatic_retry(
    tmp_path: Path, controlled_environment: None, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    case = _case(tmp_path, worker_probe=None if phase == "ACQUIRED" else ("interrupt", phase))
    engine = _engine(case, monkeypatch)
    request = engine.plan(case.scope)
    actual = engine._event
    before = _state(case)

    def interrupted(run: Path, events: list[Any], current: str, payload: dict[str, Any]) -> Any:
        result = actual(run, events, current, payload)
        if current == phase:
            raise RuntimeError("synthetic capture interruption")
        return result

    if phase == "ACQUIRED":
        monkeypatch.setattr(engine, "_event", interrupted)
        with pytest.raises((TaskCheckpointError, RuntimeError)):
            engine.capture(request)
    else:
        with _worker_probe(case):
            with pytest.raises(TaskCheckpointError):
                engine.capture(request)
        fence, _ = engine._context(request)
        execution_request = json.loads((case.destination / "execution_request.json").read_bytes())
        head = next(
            row
            for row in fence.guard.store.replay().lease_heads
            if row.lease_id == execution_request["lease_id"]
        )
        assert head.state == "RELEASED"
        assert head.execution["state"] == "RESULT_RECORDED"
        assert head.execution["exit"]["basis"] == "LIVE_CONTAINED_HANDLE"
        assert head.execution["exit"]["returncode"] != 0
        assert head.execution["result"]["status"] == "INSUFFICIENT"
        assert head.execution["result"]["artifact"] is None
    assert _state(case) == before
    assert not (case.destination / "receipt.json").exists()
    if phase in {"CAPTURED", "OBJECTS_WRITTEN"}:
        bundle = json.loads((case.destination / "capture_manifest.json").read_bytes())
        restored = {
            row["file"]["path"]: (
                None
                if row["storage_path"] is None
                else (case.destination / row["storage_path"]).read_bytes()
            )
            for row in bundle["records"]
        }
        assert restored == case.expected, "CAPTURED event preceded durable raw byte custody"
    retained = {
        p.relative_to(case.destination).as_posix(): p.read_bytes()
        for p in case.destination.rglob("*")
        if p.is_file()
    }
    assert retained
    monkeypatch.setattr(engine, "_event", actual)
    with pytest.raises(TaskCheckpointError):
        engine.capture(request)
    assert {
        p.relative_to(case.destination).as_posix(): p.read_bytes()
        for p in case.destination.rglob("*")
        if p.is_file()
    } == retained


def test_durable_object_intent_precedes_commit_and_reproduces_exact_oid(
    tmp_path: Path,
    controlled_environment: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path, worker_probe=("commit", "COMMIT_TREE"))
    engine = _engine(case, monkeypatch)
    request = engine.plan(case.scope)
    with _worker_probe(case) as observed:
        receipt_path = _assert_snapshot(case, engine, request)
    receipt = json.loads(receipt_path.read_bytes())
    assert observed[0]["first_oid"] == observed[0]["second_oid"] == receipt["snapshot"]["commit"]
    assert observed[0]["intent_sha256"] == _sha(
        (case.destination / "object_intent.json").read_bytes()
    )


def test_capture_bundle_rejects_tamper_even_after_receipt_digest_refresh(
    case: CheckpointCase,
    engine: TaskCheckpoint,
) -> None:
    request = engine.plan(case.scope)
    receipt_path = _assert_snapshot(case, engine, request)
    manifest = json.loads((case.destination / "capture_manifest.json").read_bytes())
    row = next(row for row in manifest["records"] if row["storage_path"] is not None)
    path = case.destination / row["storage_path"]
    original = path.read_bytes()
    path.write_bytes(bytes([original[0] ^ 1]) + original[1:])
    receipt = json.loads(receipt_path.read_bytes())
    for evidence in receipt["evidence"]:
        if evidence["path"] == row["storage_path"]:
            evidence["sha256"] = _sha(path.read_bytes())
    receipt["receipt_sha256"] = _sha(
        _json({key: value for key, value in receipt.items() if key != "receipt_sha256"}) + b"\n"
    )
    receipt_path.write_bytes(_json(receipt))
    with pytest.raises(TaskCheckpointError, match="CAPTURE_BUNDLE"):
        engine.validate(receipt_path)


def test_competing_real_source_lease_blocks_checkpoint_without_touching_source(
    case: CheckpointCase, engine: TaskCheckpoint
) -> None:
    request = engine.plan(case.scope)
    guard = CheckoutLeaseGuard(
        project_root=case.root,
        policy_path=case.root / POLICIES[1],
        parallel_policy_path=case.root / POLICIES[2],
    )
    decision, handle = guard.acquire(
        intent_id="synthetic-competing-writer",
        task_id=SOURCE_TASK,
        thread_id="synthetic-competing-thread",
        actor=ACTOR,
        operation_class=CheckoutOperationClass.DOMAIN_MUTATION,
        owned_paths=case.scope["paths"],
        base_commit=case.scope["source_head_sha"],
    )
    assert decision.status == "PASS" and handle is not None
    before = _state(case)
    try:
        with pytest.raises(TaskCheckpointError):
            engine.capture(request)
        assert _state(case) == before
        assert not (case.destination / "receipt.json").exists()
    finally:
        handle.release(outcome="completed")


@pytest.mark.parametrize("limit", ["max_files", "max_total_bytes"])
def test_budget_is_rejected_before_source_content_read(
    tmp_path: Path,
    controlled_environment: None,
    monkeypatch: pytest.MonkeyPatch,
    limit: str,
) -> None:
    case = _case(tmp_path, budget={limit: 1})
    bounded = _engine(case, monkeypatch)
    actual = Path.open
    targets = {case.root / path for path in case.expected}

    def forbidden(path: Path, *args: Any, **kwargs: Any) -> Any:
        assert path not in targets, "metadata budget must reject before source bytes are opened"
        return actual(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", forbidden)
    with pytest.raises(TaskCheckpointError) as failure:
        bounded.plan(case.scope)
    assert failure.value.code == "TASK_CHECKPOINT_BUDGET"
    assert not case.destination.exists()


def test_reparse_source_is_rejected_before_content_read(
    case: CheckpointCase,
    engine: TaskCheckpoint,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    # Windows link creation requires host privileges; identify this synthetic
    # metadata probe explicitly, rather than claiming an actual OS-link E2E.
    request.node.user_properties.append(
        ("reparse_probe", "SYNTHETIC_LSTAT_ATTRIBUTE_NOT_OS_LINK_CREATION")
    )
    target = case.root / "src/add-000.bin"
    lstat = Path.lstat
    actual = Path.open

    def reparse(path: Path) -> Any:
        info = lstat(path)
        if path == target:
            return SimpleNamespace(st_mode=info.st_mode, st_file_attributes=0x400)
        return info

    def forbidden(path: Path, *args: Any, **kwargs: Any) -> Any:
        assert path != target, "reparse file bytes must not be opened"
        return actual(path, *args, **kwargs)

    monkeypatch.setattr(Path, "lstat", reparse)
    monkeypatch.setattr(Path, "open", forbidden)
    with pytest.raises(TaskCheckpointError):
        engine.plan(case.scope)


def test_filter_configuration_is_rejected_before_status_or_filter_dispatch(
    case: CheckpointCase, engine: TaskCheckpoint, monkeypatch: pytest.MonkeyPatch
) -> None:
    _git(case.root, "config", "filter.synthetic.clean", "synthetic-filter-must-not-run")
    actual = subprocess.run

    def checked(command: list[str], *args: Any, **kwargs: Any) -> Any:
        assert "status" not in command, "unsafe Git status could dispatch a filter"
        return actual(command, *args, **kwargs)

    monkeypatch.setattr(subprocess, "run", checked)
    with pytest.raises(TaskCheckpointError):
        engine.plan(case.scope)


@pytest.mark.parametrize("key", ["GIT_OPTIONAL_LOCKS", "GIT_CONFIG_NOSYSTEM", "GIT_CONFIG_GLOBAL"])
def test_missing_safe_git_environment_rejects_before_any_output(
    case: CheckpointCase, engine: TaskCheckpoint, monkeypatch: pytest.MonkeyPatch, key: str
) -> None:
    before = _state(case)
    monkeypatch.delenv(key)
    with pytest.raises(TaskCheckpointError) as failure:
        engine.plan(case.scope)
    assert failure.value.code.startswith("TASK_CHECKPOINT_")
    assert not case.destination.exists()
    assert _state(case) == before


@pytest.mark.parametrize("mutation", ["bytes", "head", "index"])
def test_source_drift_during_capture_keeps_actual_source_and_cannot_issue_receipt(
    tmp_path: Path,
    controlled_environment: None,
    mutation: str,
) -> None:
    case = _case(tmp_path, worker_probe=("barrier", "CAPTURED"))
    scope_path = case.main_root / "outputs/validation_runtime/drift-scope.json"
    request_path = case.main_root / "outputs/validation_runtime/drift-request.json"
    _write(case.main_root, scope_path.relative_to(case.main_root).as_posix(), _json(case.scope))
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(case.main_root / "src")
    environment["PYTHONDONTWRITEBYTECODE"] = "1"

    def cli(*arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(case.main_root / checkpoint.CLI_PATH), *arguments],
            cwd=case.main_root,
            env=environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=300,
            check=False,
        )

    planned = cli("plan", "--scope", str(scope_path), "--output", str(request_path))
    assert planned.returncode == 0, (planned.stdout, planned.stderr)
    assert json.loads(planned.stdout)["status"] == "PASS"
    refs_before = _git(case.root, "for-each-ref", checkpoint.REF_PREFIX)
    changed: list[dict[str, Any]] = []

    def mutate() -> None:
        if mutation == "bytes":
            _write(case.root, "src/modify-000.py", b"concurrent task edit\n")
        elif mutation == "head":
            _git(case.root, "commit", "--allow-empty", "-m", "concurrent task HEAD")
        else:
            _git(case.root, "add", "--", ":(literal)src/modify-000.py")
        changed.append(_state(case))

    with _worker_probe(case, mutate):
        captured = cli("capture", "--request", str(request_path))
    assert captured.returncode == 2, (captured.stdout, captured.stderr)
    result = json.loads(captured.stdout)
    assert result["status"] == "BLOCKED"
    assert result["reason_code"] == "TASK_CHECKPOINT_EXECUTION"
    worker = json.loads((case.destination / "worker.stdout.log").read_bytes())
    assert worker["status"] == "BLOCKED"
    assert worker["reason_code"] == (
        "TASK_CHECKPOINT_IDENTITY" if mutation == "head" else "TASK_CHECKPOINT_DRIFT"
    )
    failure = json.loads((case.destination / "execution_failure.json").read_bytes())
    assert failure["primary_error_code"] == "TASK_CHECKPOINT_EXECUTION"
    assert failure["cleanup_errors"] == []
    assert len(changed) == 1
    assert _state(case) == changed[0]
    assert not (case.destination / "receipt.json").exists()
    assert _git(case.root, "for-each-ref", checkpoint.REF_PREFIX) == refs_before


def test_independent_validation_retains_truth_after_task_head_and_index_continue(
    case: CheckpointCase, engine: TaskCheckpoint
) -> None:
    receipt = _assert_snapshot(case, engine, engine.plan(case.scope))
    before = engine.validate(receipt)
    _git(case.root, "commit", "--allow-empty", "-m", "later task HEAD")
    _git(case.root, "add", "--", ":(literal)src/modify-000.py")
    changed = _state(case)
    assert engine.validate(receipt) == before
    assert _state(case) == changed


def test_explicit_scope_does_not_read_hash_or_capture_unrequested_private_or_unowned_files(
    case: CheckpointCase, engine: TaskCheckpoint, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Create these only after the original ordinary-mutation scope is released.
    # It is not permission to inspect them during source-only checkpointing.
    forbidden_paths = {
        case.root / ".env": b"SYNTHETIC_PRIVATE_VALUE=never-read\n",
        case.root / "src/unrequested-private.key": b"synthetic private bytes\n",
        case.root / "src/unattributed.py": b"unowned work\n",
        case.root / EXCLUDED: b"excluded owner bytes\n",
        case.root / UNREQUESTED_TRACKED_PATH: b"unrequested tracked dirty bytes never read\n",
    }
    for path, content in forbidden_paths.items():
        _write(case.root, path.relative_to(case.root).as_posix(), content)
    actual_open, actual_run = Path.open, subprocess.run
    seen: list[list[str]] = []

    def no_private_open(path: Path, *args: Any, **kwargs: Any) -> Any:
        assert path not in forbidden_paths, "unrequested bytes must not be opened"
        return actual_open(path, *args, **kwargs)

    def no_implicit_git_reads(command: list[str], *args: Any, **kwargs: Any) -> Any:
        parts = [str(part) for part in command]
        seen.append(parts)
        assert "status" not in parts, "Git status would hash unrequested working-tree files"
        assert "check-attr" not in parts, "Git attributes may read unrequested attribute files"
        if "hash-object" in parts:
            assert "--stdin" in parts and "--no-filters" in parts
        return actual_run(command, *args, **kwargs)

    monkeypatch.setattr(Path, "open", no_private_open)
    monkeypatch.setattr(subprocess, "run", no_implicit_git_reads)
    receipt = _assert_snapshot(case, engine, engine.plan(case.scope))
    assert seen
    result = json.loads(receipt.read_bytes())
    assert result["safety"]["source_mutation_allowed"] is False
    assert result["safety"]["unscoped_worktree_status"] == "NOT_INSPECTED"
    assert result["safety"]["clean_integration_status"] == "NOT_EVALUATED"
    snapshot = result["snapshot"]["commit"]
    for path in forbidden_paths:
        relative = path.relative_to(case.root).as_posix()
        if relative == UNREQUESTED_TRACKED_PATH:
            assert (
                _git(case.root, "cat-file", "blob", f"{snapshot}:{relative}")
                == UNREQUESTED_BASE_BYTES
            )
        else:
            assert _git(case.root, "ls-tree", snapshot, "--", relative) == b""


@contextmanager
def _native_canary_accesses(paths: list[Path]) -> Iterator[list[Path]]:
    """Observe incompatible opens across processes, not just Python read wrappers.

    Level-1 oplocks break before another handle can read/write. A watcher records
    and releases the break so a faulty child cannot hang the test. Only newly
    created synthetic canaries are passed here; no project-private path is read.
    Microsoft contract: winioctl/ni-winioctl-fsctl_request_oplock_level_1.
    """
    from ctypes import wintypes as w

    class Overlapped(ctypes.Structure):
        _fields_ = [
            ("Internal", ctypes.c_size_t),
            ("InternalHigh", ctypes.c_size_t),
            ("Offset", w.DWORD),
            ("OffsetHigh", w.DWORD),
            ("hEvent", w.HANDLE),
        ]

    api = ctypes.WinDLL("kernel32", use_last_error=True)
    api.CreateFileW.argtypes = [w.LPCWSTR, w.DWORD, w.DWORD, w.LPVOID, w.DWORD, w.DWORD, w.HANDLE]
    api.CreateFileW.restype = w.HANDLE
    api.CreateEventW.argtypes = [w.LPVOID, w.BOOL, w.BOOL, w.LPCWSTR]
    api.CreateEventW.restype = w.HANDLE
    api.DeviceIoControl.argtypes = [
        w.HANDLE, w.DWORD, w.LPVOID, w.DWORD, w.LPVOID, w.DWORD,
        ctypes.POINTER(w.DWORD), ctypes.POINTER(Overlapped),
    ]
    api.DeviceIoControl.restype = w.BOOL
    api.WaitForSingleObject.argtypes = [w.HANDLE, w.DWORD]
    api.WaitForSingleObject.restype = w.DWORD
    api.CancelIoEx.argtypes = [w.HANDLE, ctypes.POINTER(Overlapped)]
    api.CancelIoEx.restype = w.BOOL
    api.GetOverlappedResult.argtypes = [
        w.HANDLE, ctypes.POINTER(Overlapped), ctypes.POINTER(w.DWORD), w.BOOL,
    ]
    api.GetOverlappedResult.restype = w.BOOL
    api.CloseHandle.argtypes = [w.HANDLE]
    api.CloseHandle.restype = w.BOOL
    held: list[dict[str, Any]] = []
    accesses: list[Path] = []
    errors: list[BaseException] = []
    stop = threading.Event()
    thread = None

    def observe() -> None:
        try:
            while not stop.wait(0.01):
                for row in held:
                    if row["handle"] is None:
                        continue
                    state = api.WaitForSingleObject(row["event"], 0)
                    assert state in {0, 258}, ctypes.get_last_error()
                    if state == 0:
                        returned = w.DWORD()
                        assert api.GetOverlappedResult(
                            row["handle"], ctypes.byref(row["overlap"]),
                            ctypes.byref(returned), False,
                        ), ("oplock observation cancelled, not an access", ctypes.get_last_error())
                        accesses.append(row["path"])
                        assert api.CloseHandle(row["handle"])
                        row["handle"] = None
        except BaseException as exc:
            errors.append(exc)

    try:
        for path in paths:
            event = api.CreateEventW(None, True, False, None)
            assert event, ctypes.get_last_error()
            row = {"path": path, "event": event, "handle": None, "pending": False}
            held.append(row)
            handle = api.CreateFileW(str(path), 0xC0000000, 7, None, 3, 0x40000080, None)
            assert handle not in {None, ctypes.c_void_p(-1).value}, ctypes.get_last_error()
            row["handle"] = handle
            overlap = Overlapped(hEvent=event)
            row["overlap"] = overlap
            returned = w.DWORD()
            accepted = api.DeviceIoControl(
                handle, 0x00090000, None, 0, None, 0,
                ctypes.byref(returned), ctypes.byref(overlap),
            )
            assert not accepted and ctypes.get_last_error() == 997
            row["pending"] = True
            assert api.WaitForSingleObject(event, 0) == 258
        thread = threading.Thread(target=observe, name="synthetic-oplock-observer", daemon=True)
        thread.start()
        yield accesses
    finally:
        stop.set()
        if thread is not None:
            thread.join(timeout=5)
            assert not thread.is_alive()
        for row in held:
            handle = row["handle"]
            if handle is not None:
                if row["pending"]:
                    # Record a late break before cancelling our still-pending request.
                    if api.WaitForSingleObject(row["event"], 0) == 0:
                        returned = w.DWORD()
                        assert api.GetOverlappedResult(
                            handle, ctypes.byref(row["overlap"]),
                            ctypes.byref(returned), False,
                        ), ("late oplock cancellation is not an access", ctypes.get_last_error())
                        accesses.append(row["path"])
                    api.CancelIoEx(handle, ctypes.byref(row["overlap"]))
                    returned = w.DWORD()
                    completed = api.GetOverlappedResult(
                        handle, ctypes.byref(row["overlap"]), ctypes.byref(returned), True
                    )
                    assert completed or ctypes.get_last_error() == 995
                assert api.CloseHandle(handle)
            assert api.CloseHandle(row["event"])
        assert not errors, errors


@pytest.mark.parametrize("operation", ["none", "python-read", "python-write", "git-read"])
def test_native_canary_observer_detects_real_child_access(tmp_path: Path, operation: str) -> None:
    path = tmp_path / "synthetic-canary.bin"
    original = b"synthetic canary raw\x00\xff\r\n"
    path.write_bytes(original)
    with _native_canary_accesses([path]) as accesses:
        if operation == "git-read":
            command = ["git", "hash-object", "--no-filters", str(path)]
        else:
            expression = {
                "none": "pass",
                "python-read": "assert p.read_bytes()",
                "python-write": "p.write_bytes(b'changed')",
            }[operation]
            command = [
                sys.executable, "-c",
                "import sys; from pathlib import Path; p=Path(sys.argv[1]); " + expression,
                str(path),
            ]
        result = subprocess.run(command, capture_output=True, timeout=20, check=False)
        assert result.returncode == 0, result.stderr
    assert accesses == ([] if operation == "none" else [path])
    assert path.read_bytes() == (b"changed" if operation == "python-write" else original)


@pytest.mark.parametrize(
    "step", ["open", "final-path", "fdopen", "fstat", "attributes", "resolve", "lstat", "reader"]
)
def test_native_canary_metadata_observation_is_not_content_access(
    tmp_path: Path, step: str
) -> None:
    path = tmp_path / "synthetic-metadata.bin"
    path.write_bytes(b"synthetic metadata canary")
    script = r'''
import ctypes, msvcrt, os, sys
from pathlib import Path
from ctypes import wintypes as w
api=ctypes.WinDLL("kernel32",use_last_error=True)
api.CreateFileW.argtypes=[w.LPCWSTR,w.DWORD,w.DWORD,w.LPVOID,w.DWORD,w.DWORD,w.HANDLE]
api.CreateFileW.restype=w.HANDLE
api.CloseHandle.argtypes=[w.HANDLE]
api.GetFinalPathNameByHandleW.argtypes=[w.HANDLE,w.LPWSTR,w.DWORD,w.DWORD]
handle=api.CreateFileW(sys.argv[1],0,7,None,3,0x200000,None)
assert handle not in (None,ctypes.c_void_p(-1).value)
if sys.argv[2]=="attributes":
    api.GetFileInformationByHandleEx.argtypes=[w.HANDLE,ctypes.c_int,w.LPVOID,w.DWORD]
    data=(w.DWORD*2)()
    assert api.GetFileInformationByHandleEx(handle,9,ctypes.byref(data),ctypes.sizeof(data))
if sys.argv[2]=="resolve":
    assert Path(sys.argv[1]).resolve().is_absolute()
if sys.argv[2]=="lstat":
    assert Path(sys.argv[1]).lstat().st_size>0
if sys.argv[2]=="reader":
    from ai_trading_system.platform.architecture import workflow_contract as workflow
    try:
        workflow.bounded_regular_bytes(Path(sys.argv[1]),expected_identity=(0,0))
    except workflow.WorkflowContractError as error:
        assert error.code=="WORKFLOW_HANDLE_EXPECTED_IDENTITY_CHANGED"
    else:
        raise AssertionError("wrong identity accepted")
if sys.argv[2]=="final-path":
    name=ctypes.create_unicode_buffer(32768)
    assert api.GetFinalPathNameByHandleW(handle,name,len(name),0)
if sys.argv[2] in ("fdopen","fstat"):
    fd=msvcrt.open_osfhandle(int(handle),os.O_RDONLY|os.O_BINARY)
    with os.fdopen(fd,"rb") as stream:
        if sys.argv[2]=="fstat":
            assert os.fstat(stream.fileno()).st_size>0
else:
    assert api.CloseHandle(handle)
'''
    with _native_canary_accesses([path]) as accesses:
        result = subprocess.run(
            [sys.executable, "-c", script, str(path), step],
            capture_output=True, timeout=20, check=False,
        )
        assert result.returncode == 0, result.stderr
    assert accesses == [], (step, accesses)


@pytest.mark.parametrize(
    "selection",
    [
        "ignored", "unowned", "excluded", "secret",
        "alias_case", "alias_parent", "alias_slash", "alias_dot",
    ],
)
def test_s02_actual_cli_protected_canaries_have_zero_native_access(
    tmp_path: Path, controlled_environment: None, selection: str
) -> None:
    case = _case(tmp_path)
    protected = {
        ".env": b"SYNTHETIC_PRIVATE_VALUE=never-read\n",
        "src/unrequested-private.key": b"synthetic private bytes\n",
        "src/unattributed.py": b"unowned work\n",
        EXCLUDED: b"synthetic excluded owner bytes\n",
        UNREQUESTED_TRACKED_PATH: b"unrequested tracked dirty bytes never read\n",
    }
    # Original scope is already frozen/released. Never add these to its authority.
    for relative, raw in protected.items():
        _write(case.root, relative, raw)
    scope = copy.deepcopy(case.scope)
    if selection != "ignored":
        requested = {
            "unowned": "src/unattributed.py",
            "excluded": EXCLUDED,
            "secret": ".env",
            "alias_case": EXCLUDED.upper(),
            "alias_parent": "docs/research/../research/" + Path(EXCLUDED).name,
            "alias_slash": EXCLUDED.replace("/", "\\"),
            "alias_dot": "./" + EXCLUDED,
        }[selection]
        if selection.startswith("alias_"):
            assert os.path.samefile(case.root / requested, case.root / EXCLUDED)
        scope["paths"] = sorted([*scope["paths"], requested], key=str.casefold)
    scope_path = case.main_root / "outputs/validation_runtime/protected-scope.json"
    request_path = case.main_root / "outputs/validation_runtime/protected-request.json"
    _write(case.main_root, scope_path.relative_to(case.main_root).as_posix(), _json(scope))
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(case.main_root / "src")
    environment["PYTHONDONTWRITEBYTECODE"] = "1"

    def cli(*arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(case.main_root / checkpoint.CLI_PATH), *arguments],
            cwd=case.main_root,
            env=environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=300,
            check=False,
        )

    before = _state(case)
    refs_before = _git(case.root, "for-each-ref", checkpoint.REF_PREFIX)
    with _native_canary_accesses([case.root / relative for relative in protected]) as accesses:
        planned = cli("plan", "--scope", str(scope_path), "--output", str(request_path))
        result = json.loads(planned.stdout)
        if selection == "ignored":
            assert planned.returncode == 0, (planned.stdout, planned.stderr)
            assert result["status"] == "PASS"
            captured = cli("capture", "--request", str(request_path))
            assert captured.returncode == 0, (captured.stdout, captured.stderr)
            result = json.loads(captured.stdout)
            assert result["status"] == "PASS"
            validated = cli("validate", "--receipt", str(_receipt(case, result)))
            assert validated.returncode == 0, (validated.stdout, validated.stderr)
            assert json.loads(validated.stdout)["status"] == "PASS"
        else:
            assert planned.returncode == 2, (planned.stdout, planned.stderr)
            assert result["status"] == "BLOCKED"
            assert result["reason_code"] == (
                "TASK_CHECKPOINT_PATH"
                if selection in {"alias_parent", "alias_slash", "alias_dot"}
                else "TASK_CHECKPOINT_SCOPE"
            )
            assert not request_path.exists()
            assert not case.destination.exists()
    assert accesses == [], "protected file opened by CLI, contained worker or child Git"
    assert _state(case) == before
    assert {relative: (case.root / relative).read_bytes() for relative in protected} == protected
    if selection == "ignored":
        snapshot = result["snapshot"]["commit"]
        for relative in protected:
            if relative == UNREQUESTED_TRACKED_PATH:
                assert _git(case.root, "cat-file", "blob", f"{snapshot}:{relative}") == (
                    UNREQUESTED_BASE_BYTES
                )
            else:
                assert _git(case.root, "ls-tree", snapshot, "--", relative) == b""
    else:
        assert _git(case.root, "for-each-ref", checkpoint.REF_PREFIX) == refs_before


@pytest.mark.parametrize("stage", ["plan", "capture"])
@pytest.mark.parametrize("kind", ["leaf-symlink", "ancestor-junction"])
def test_s02_actual_cli_reparse_rejected_before_native_canary_access(
    tmp_path: Path, controlled_environment: None, stage: str, kind: str
) -> None:
    from test_devx015_workflow_acceptance import _junction

    case = _case(tmp_path)
    metadata_case = CheckpointCase(case.root, case.main_root, case.scope, {})
    before = _state(metadata_case)
    refs_before = _git(case.root, "for-each-ref", checkpoint.REF_PREFIX)
    scope_path = case.main_root / "outputs/validation_runtime/reparse-scope.json"
    request_path = case.main_root / "outputs/validation_runtime/reparse-request.json"
    _write(case.main_root, scope_path.relative_to(case.main_root).as_posix(), _json(case.scope))
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(case.main_root / "src")
    environment["PYTHONDONTWRITEBYTECODE"] = "1"

    def cli(*arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(case.main_root / checkpoint.CLI_PATH), *arguments],
            cwd=case.main_root, env=environment, capture_output=True, text=True,
            encoding="utf-8", timeout=300, check=False,
        )

    if stage == "capture":
        planned = cli("plan", "--scope", str(scope_path), "--output", str(request_path))
        assert planned.returncode == 0, (planned.stdout, planned.stderr)
        assert json.loads(planned.stdout)["status"] == "PASS"
    protected = tmp_path / "synthetic-reparse-canaries"
    protected.mkdir()
    canaries = {
        protected / Path(relative).name: b"forbidden synthetic bytes\x00\xff\r\n"
        for relative, raw in case.expected.items() if raw is not None
    }
    for path, raw in canaries.items():
        path.write_bytes(raw)
    if kind == "leaf-symlink":
        link = case.root / "src/add-000.bin"
        preserved = case.root / "src/preserved-add.bin"
        link.rename(preserved)
        link.symlink_to(protected / "add-000.bin")
    else:
        link = case.root / "src"
        preserved = case.root / "preserved-src"
        link.rename(preserved)
        _junction(link, protected)
    assert link.lstat().st_file_attributes & 0x400
    link_target = os.readlink(link)
    with _native_canary_accesses(list(canaries)) as accesses:
        completed = (
            cli("plan", "--scope", str(scope_path), "--output", str(request_path))
            if stage == "plan" else cli("capture", "--request", str(request_path))
        )
    assert accesses == [], "reparse target was opened before rejection"
    assert completed.returncode == 2, (completed.stdout, completed.stderr)
    result = json.loads(completed.stdout)
    assert result["status"] == "BLOCKED"
    assert result["reason_code"] == "TASK_CHECKPOINT_PATH"
    assert _state(metadata_case) == before
    assert _git(case.root, "for-each-ref", checkpoint.REF_PREFIX) == refs_before
    assert not (case.destination / "receipt.json").exists()
    if stage == "plan":
        assert not request_path.exists()
    assert os.readlink(link) == link_target
    assert {path: path.read_bytes() for path in canaries} == canaries
    for relative, raw in case.expected.items():
        original = (
            preserved / Path(relative).name if kind == "ancestor-junction"
            else preserved if relative == "src/add-000.bin" else case.root / relative
        )
        assert (original.read_bytes() if original.exists() else None) == raw


def test_native_reader_swap_has_zero_content_opens(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ai_trading_system.platform.architecture import workflow_contract

    requested = tmp_path / "original.bin"
    requested.write_bytes(b"original")
    replacement = tmp_path / "replacement.bin"
    replacement.write_bytes(b"replaced")
    original_lstat = Path.lstat
    watched: list[list[Path]] = []
    with ExitStack() as stack:
        def swap(path: Path, *args: Any, **kwargs: Any) -> os.stat_result:
            info = original_lstat(path, *args, **kwargs)
            if path == requested and not watched:
                requested.rename(tmp_path / "preserved.bin")
                replacement.rename(requested)
                watched.append(stack.enter_context(_native_canary_accesses([requested])))
            return info

        monkeypatch.setattr(Path, "lstat", swap)
        with pytest.raises(workflow_contract.WorkflowContractError, match="IDENTITY_CHANGED"):
            workflow_contract.bounded_regular_bytes(requested)
    assert watched == [[]]


@pytest.mark.parametrize("kind", ["leaf", "ancestor-junction"])
def test_s02_actual_worker_check_open_swap_never_reads_replacement(
    tmp_path: Path, controlled_environment: None, kind: str
) -> None:
    from test_devx015_workflow_acceptance import _junction

    case = _case(tmp_path, worker_probe=("read-check", "SOURCE_READ_CHECKED"))
    requested = case.root / "src/add-000.bin"
    original = requested.read_bytes()
    original_identity = (requested.stat().st_dev, requested.stat().st_ino)
    replacement = tmp_path / "synthetic-replacement.bin"
    forbidden = b"X" * len(original)
    if kind == "leaf":
        replacement.write_bytes(forbidden)
        canary = replacement
    else:
        replacement.mkdir()
        for relative, raw in case.expected.items():
            if raw is not None:
                (replacement / Path(relative).name).write_bytes(b"X" * len(raw))
        canary = replacement / "add-000.bin"
    replacement_identity = (canary.stat().st_dev, canary.stat().st_ino)
    assert replacement_identity != original_identity
    preserved = case.root / (
        "src/preserved-original.bin" if kind == "leaf" else "preserved-original-src"
    )
    scope_path = case.main_root / "outputs/validation_runtime/swap-scope.json"
    request_path = case.main_root / "outputs/validation_runtime/swap-request.json"
    _write(case.main_root, scope_path.relative_to(case.main_root).as_posix(), _json(case.scope))
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(case.main_root / "src")
    environment["PYTHONDONTWRITEBYTECODE"] = "1"

    def cli(*arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(case.main_root / checkpoint.CLI_PATH), *arguments],
            cwd=case.main_root, env=environment, capture_output=True, text=True,
            encoding="utf-8", timeout=300, check=False,
        )

    planned = cli("plan", "--scope", str(scope_path), "--output", str(request_path))
    assert planned.returncode == 0, (planned.stdout, planned.stderr)
    assert json.loads(planned.stdout)["status"] == "PASS"
    changed: list[dict[str, Any]] = []
    observations: list[list[Path]] = []
    refs_before = _git(case.root, "for-each-ref", checkpoint.REF_PREFIX)
    with ExitStack() as stack:
        def swap() -> None:
            if kind == "leaf":
                requested.rename(preserved)
                replacement.rename(requested)
                watched = [requested]
            else:
                (case.root / "src").rename(preserved)
                _junction(case.root / "src", replacement)
                watched = list(replacement.iterdir())
            changed.append(_state(case))
            observations.append(stack.enter_context(_native_canary_accesses(watched)))

        with _worker_probe(case, swap) as witness:
            captured = cli("capture", "--request", str(request_path))
            stack.close()  # Close native I/O before its issuing probe thread exits.
    assert witness[0]["identity"] == list(original_identity)
    assert len(observations) == 1
    returned = case.main_root.parent / "probes/reader_returned.json"
    assert observations == [[]], (
        "replacement opened for content", observations,
        json.loads(returned.read_bytes()) if returned.exists() else None,
    )
    assert not returned.exists(), "protected replacement returned by source reader"
    assert captured.returncode == 2, (captured.stdout, captured.stderr)
    assert json.loads(captured.stdout)["reason_code"] == "TASK_CHECKPOINT_EXECUTION"
    worker = json.loads((case.destination / "worker.stdout.log").read_bytes())
    assert worker["reason_code"] == "TASK_CHECKPOINT_DRIFT"
    failure = json.loads((case.destination / "execution_failure.json").read_bytes())
    assert failure["cleanup_errors"] == []
    assert not (case.destination / "receipt.json").exists()
    assert _git(case.root, "for-each-ref", checkpoint.REF_PREFIX) == refs_before
    assert _state(case) == changed[0]
    assert requested.read_bytes() == forbidden
    for relative, raw in case.expected.items():
        original_path = (
            preserved / Path(relative).name if kind == "ancestor-junction"
            else preserved if relative == "src/add-000.bin" else case.root / relative
        )
        assert (original_path.read_bytes() if original_path.exists() else None) == raw
    retained = {
        path.relative_to(case.destination).as_posix(): path.read_bytes()
        for path in case.destination.rglob("*") if path.is_file()
    }
    if kind == "ancestor-junction":
        link = case.root / "src"
        assert link.lstat().st_file_attributes & 0x400
        # Remove only our synthetic junction, preserving its external canaries.
        link.rmdir()
        preserved.rename(link)
    watched = [requested] if kind == "leaf" else list(replacement.iterdir())
    before_recovery = _state(case)
    with _native_canary_accesses(watched) as recovery_accesses:
        recovered = cli("recover-interrupted", "--request", str(request_path), "--actor", ACTOR)
        assert recovered.returncode == 0, (recovered.stdout, recovered.stderr)
        recovered_result = json.loads(recovered.stdout)
        assert recovered_result["status"] == "INSUFFICIENT"
        assert recovered_result["action"] == "FAILED_ATTEMPT_TERMINAL_ONLY"
        assert recovered_result["dispatch_allowed"] is False
    assert recovery_accesses == []
    assert _state(case) == before_recovery
    assert not (case.destination / "receipt.json").exists()
    assert _git(case.root, "for-each-ref", checkpoint.REF_PREFIX) == refs_before
    for relative, raw in retained.items():
        assert (case.destination / relative).read_bytes() == raw
    guard = CheckoutLeaseGuard(
        project_root=case.root, policy_path=case.root / POLICIES[1],
        parallel_policy_path=case.root / POLICIES[2],
    )
    replay = guard.store.replay()
    assert replay.status == "PASS" and not replay.active_leases


def test_implementation_identity_is_real_committed_and_frozen_across_unrelated_main_advance(
    case: CheckpointCase, engine: TaskCheckpoint
) -> None:
    binding = engine._implementation_binding()
    assert binding["schema_version"] == "task_checkpoint_implementation.v2"
    assert binding["commit"] == case.scope["source_head_sha"]
    assert {row["path"] for row in binding["files"]} == {
        checkpoint.MODULE_PATH,
        checkpoint.CLI_PATH,
        checkpoint.POLICY_PATH,
        "src/ai_trading_system/platform/architecture/workflow_coordination.py",
        "src/ai_trading_system/platform/architecture/workflow_execution.py",
        "src/ai_trading_system/platform/architecture/workflow_contract.py",
    }
    inherited = binding["safe_git_implementation"]
    assert inherited["basis"] == "COMMITTED_SOURCE_GIT_EOL_LF"
    assert inherited["commit"] == binding["commit"]
    assert inherited["files"]
    for row in (*binding["files"], *inherited["files"]):
        assert (
            _sha(_git(case.main_root, "cat-file", "blob", f"{binding['commit']}:{row['path']}"))
            == row["git_blob_content_sha256"]
        )
    newer = _advance_main(case)
    assert newer != binding["commit"]
    assert engine._implementation_binding() == binding


@pytest.mark.parametrize("committed", [False, True], ids=["working_bytes", "new_committed_code"])
@pytest.mark.parametrize("relative", [checkpoint.MODULE_PATH, *checkpoint._EXECUTION_PATHS])
def test_frozen_implementation_rejects_changed_code_not_just_changed_head(
    case: CheckpointCase, engine: TaskCheckpoint, committed: bool, relative: str
) -> None:
    engine._implementation_binding()
    path = case.main_root / relative
    path.write_bytes(path.read_bytes() + b"\n# synthetic changed implementation\n")
    if committed:
        _commit(case.main_root, (relative,), "synthetic new implementation")
    with pytest.raises(TaskCheckpointError):
        engine._implementation_binding()


@pytest.mark.parametrize("name", checkpoint._EXECUTION_MODULES)
@pytest.mark.parametrize("origin", [None, "foreign-checkout"])
def test_execution_dependency_requires_actual_loaded_origin(
    case: CheckpointCase,
    engine: TaskCheckpoint,
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    origin: str | None,
) -> None:
    binding = engine._implementation_binding()
    monkeypatch.setattr(
        sys.modules[name], "__file__", None if origin is None else str(case.root / "foreign.py")
    )
    engine._verify_implementation(binding, current=False)
    with pytest.raises(TaskCheckpointError, match="loaded execution dependency"):
        engine._implementation_binding()
    fresh = TaskCheckpoint(case.main_root)
    with pytest.raises(TaskCheckpointError, match="loaded execution dependency"):
        fresh._implementation_binding()


def test_historical_v1_cannot_downgrade_current_execution_identity(
    case: CheckpointCase,
    engine: TaskCheckpoint,
) -> None:
    current = engine._implementation_binding()
    legacy = copy.deepcopy(current)
    legacy["schema_version"] = "task_checkpoint_implementation.v1"
    legacy["files"] = [
        row for row in legacy["files"] if row["path"] not in checkpoint._EXECUTION_PATHS
    ]
    engine._verify_implementation(legacy, current=False)
    with pytest.raises(TaskCheckpointError, match="historical implementation"):
        engine._verify_implementation(legacy, current=True)
    for relative in checkpoint._EXECUTION_PATHS:
        missing = copy.deepcopy(current)
        missing["files"] = [row for row in missing["files"] if row["path"] != relative]
        with pytest.raises(TaskCheckpointError):
            engine._verify_implementation(missing)
        forged = copy.deepcopy(current)
        next(row for row in forged["files"] if row["path"] == relative)[
            "git_blob_content_sha256"
        ] = "0" * 64
        with pytest.raises(TaskCheckpointError):
            engine._verify_implementation(forged)
        path = case.main_root / relative
        path.write_bytes(path.read_bytes() + b"\n# changed live dependency\n")
    engine._verify_implementation(legacy, current=False)
    engine._verify_implementation(current, current=False)
    with pytest.raises(TaskCheckpointError):
        engine._verify_implementation(current, current=True)
    extended_legacy = copy.deepcopy(current)
    extended_legacy["schema_version"] = "task_checkpoint_implementation.v1"
    with pytest.raises(TaskCheckpointError):
        engine._verify_implementation(extended_legacy)


@pytest.mark.parametrize("boundary", ["exact", "over_count", "over_bytes"])
def test_s01_actual_cli_aggregate_budget_boundary(
    tmp_path: Path, controlled_environment: None, boundary: str
) -> None:
    # Freeze both limits before seed/canonical commits and original scope acquisition.
    total = len(b"changed src/modify-000.py\r\n") + len(b"new\x00binary\xff\r\nsrc/add-000.bin")
    case = _case(
        tmp_path,
        budget={
            "max_files": 3 - (boundary == "over_count"),
            "max_total_bytes": total - (boundary == "over_bytes"),
        },
    )
    assert len(case.expected) == 3
    assert sum(len(value) for value in case.expected.values() if value is not None) == total
    scope_path = case.main_root / "outputs/validation_runtime/budget-scope.json"
    request_path = case.main_root / "outputs/validation_runtime/budget-request.json"
    _write(case.main_root, scope_path.relative_to(case.main_root).as_posix(), _json(case.scope))
    environment = dict(
        os.environ, PYTHONPATH=str(case.main_root / "src"), PYTHONDONTWRITEBYTECODE="1"
    )
    before = _state(case)
    refs_before = _git(case.root, "show-ref")

    def cli(*arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(case.main_root / checkpoint.CLI_PATH), *arguments],
            cwd=case.main_root,
            env=environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=300,
        )

    planned = cli("plan", "--scope", str(scope_path), "--output", str(request_path))
    if boundary != "exact":
        assert planned.returncode != 0
        assert "TASK_CHECKPOINT_BUDGET" in planned.stderr + planned.stdout
        assert not request_path.exists()
        assert not case.destination.exists()
        assert _git(case.root, "show-ref") == refs_before
    else:
        assert planned.returncode == 0, planned.stderr + planned.stdout
        request = json.loads(request_path.read_bytes())
        assert len(request["files"]) == 3
        assert sum(row["size_bytes"] for row in request["files"]) == total
        assert {row["path"]: row["operation"] for row in request["files"]} == {
            "src/add-000.bin": "ADD",
            "src/modify-000.py": "MODIFY",
            "src/delete-000.py": "DELETE",
        }
        captured = cli("capture", "--request", str(request_path))
        assert captured.returncode == 0, captured.stderr + captured.stdout
        result = json.loads(captured.stdout)
        receipt = _receipt(case, result)
        validated = cli("validate", "--receipt", str(receipt))
        assert validated.returncode == 0, validated.stderr + validated.stdout
        assert json.loads(validated.stdout)["status"] == "PASS"
        snapshot = result["snapshot"]["commit"]
        changed = (
            _git(
                case.root,
                "-c",
                "diff.autoRefreshIndex=false",
                "diff",
                "--name-only",
                "-z",
                case.scope["source_head_sha"],
                snapshot,
                "--",
                ".",
                EXCLUSION,
            )
            .decode()
            .strip("\0")
            .split("\0")
        )
        assert set(changed) == set(case.expected)
        for path, content in case.expected.items():
            if content is None:
                assert _git(case.root, "ls-tree", snapshot, "--", path) == b""
            else:
                assert _git(case.root, "cat-file", "blob", f"{snapshot}:{path}") == content
        assert result["safety"]["full_allowed"] is False
        assert result["safety"]["main_ff_allowed"] is False
    assert _state(case) == before


@pytest.mark.parametrize("staging", ["unstaged", "requested", "unrequested", "mixed", "empty"])
def test_committed_implementation_cli_subprocess_e2e_survives_main_advance(
    tmp_path: Path, controlled_environment: None, staging: str
) -> None:
    # No monkeypatch fixture and no in-process engine: each actual CLI process
    # imports the committed synthetic source files from its own physical root.
    case = _case(tmp_path)
    if staging in {"requested", "mixed", "empty"}:
        _git(case.root, "add", "-A", "--", *(f":(literal){path}" for path in case.expected))
    if staging in {"unrequested", "mixed"}:
        _write(case.root, UNREQUESTED_TRACKED_PATH, b"unrequested staged bytes\r\n")
        _git(case.root, "add", "--", f":(literal){UNREQUESTED_TRACKED_PATH}")
    if staging in {"mixed", "empty"}:
        raw = b"" if staging == "empty" else b"unstaged raw\x00\xff\r\n"
        _write(case.root, "src/add-000.bin", raw)
        case.expected["src/add-000.bin"] = raw
    unrequested_before = (case.root / UNREQUESTED_TRACKED_PATH).read_bytes()
    staged_before = _git(case.root, "ls-files", "--stage", "-z")
    scope_path = case.main_root / "outputs/validation_runtime/synthetic-scope.json"
    request_path = case.main_root / "outputs/validation_runtime/synthetic-request.json"
    _write(case.main_root, scope_path.relative_to(case.main_root).as_posix(), _json(case.scope))
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(case.main_root / "src")
    environment["PYTHONDONTWRITEBYTECODE"] = "1"

    def cli(*arguments: str) -> dict[str, Any]:
        completed = subprocess.run(
            [sys.executable, str(case.main_root / checkpoint.CLI_PATH), *arguments],
            cwd=case.main_root,
            env=environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=300,
            check=False,
        )
        assert completed.returncode == 0, (completed.stdout, completed.stderr)
        result = json.loads(completed.stdout)
        assert result["status"] == "PASS"
        return result

    before = _state(case)
    cli("plan", "--scope", str(scope_path), "--output", str(request_path))
    planned = json.loads(request_path.read_bytes())
    for row in planned["files"]:
        raw = case.expected[row["path"]]
        assert row["sha256"] == (_sha(raw) if raw is not None else None)
        assert row["size_bytes"] == (len(raw) if raw is not None else 0)
    newer = _advance_main(case)
    captured = cli("capture", "--request", str(request_path))
    receipt_path = _receipt(case, captured)
    validated = cli("validate", "--receipt", str(receipt_path))
    assert _state(case) == before
    assert _git(case.root, "ls-files", "--stage", "-z") == staged_before
    assert (receipt_path.parent / "source.index").read_bytes() == before["index"]
    assert (case.root / UNREQUESTED_TRACKED_PATH).read_bytes() == unrequested_before
    assert captured["implementation"]["commit"] == case.scope["source_head_sha"]
    assert _ref(case.root, "main") == newer
    assert validated["safety"]["main_ff_allowed"] is False
    assert validated["safety"]["full_allowed"] is False
    snapshot = captured["snapshot"]["commit"]
    assert (
        _git(case.root, "cat-file", "blob", f"{snapshot}:{UNREQUESTED_TRACKED_PATH}")
        == UNREQUESTED_BASE_BYTES
    )
    changed = (
        _git(
            case.root,
            "-c",
            "diff.autoRefreshIndex=false",
            "diff",
            "--name-only",
            "-z",
            case.scope["source_head_sha"],
            snapshot,
            "--",
            ".",
            EXCLUSION,
        )
        .decode()
        .strip("\0")
        .split("\0")
    )
    assert set(changed) == set(case.expected)
    for path, content in case.expected.items():
        if content is None:
            assert _git(case.root, "ls-tree", snapshot, "--", path) == b""
        else:
            assert _git(case.root, "cat-file", "blob", f"{snapshot}:{path}") == content


def test_capture_worker_uses_actual_job_and_the_original_source_lease(
    case: CheckpointCase,
    engine: TaskCheckpoint,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from test_devx015_workflow_execution import NativeOracle, _until

    from ai_trading_system.platform.architecture.workflow_execution import WindowsJobProcess

    request = engine.plan(case.scope)
    actual_wait = WindowsJobProcess.wait
    observed: list[dict[str, Any]] = []
    oracle = NativeOracle()

    def observe_wait(process: Any, *, timeout: float) -> int:
        marker = case.destination / "worker_observation.json"
        _until(
            lambda: marker.exists() or process.poll() is not None, description="checkpoint worker"
        )
        assert marker.exists(), (case.destination / "worker.stdout.log").read_text()
        worker = json.loads(marker.read_bytes())
        assert worker["worker_process"]["pid"] != os.getpid()
        identity = process.identity()
        with oracle.process(worker["worker_process"]["pid"]) as worker_handle:
            assert oracle.creation_time(worker_handle) == worker["worker_process"]["creation_time"]
            assert not oracle.exited(worker_handle)
            oracle.assert_in_job(worker_handle, identity["job_name"])
            fence, _ = engine._context(request)
            active = fence.guard.store.replay().active_leases
            assert len(active) == 1 and active[0].lease_id == worker["lease_id"]
            assert active[0].execution["state"] == "RUNNING"
            assert active[0].execution["launcher"]["pid"] == os.getpid()
            assert active[0].execution["request"]["lease_id"] == worker["lease_id"]
            with pytest.raises(TaskCheckpointError, match="JOB_MEMBERSHIP_MISMATCH"):
                engine.capture_worker(case.destination / "execution_request.json")
            observed.append(worker)
            result = actual_wait(process, timeout=timeout)
            assert oracle.exited(worker_handle)
            return result

    monkeypatch.setattr(WindowsJobProcess, "wait", observe_wait)
    receipt_path = _assert_snapshot(case, engine, request)
    assert len(observed) == 1
    receipt = json.loads(receipt_path.read_bytes())
    assert receipt["schema_version"] == "task_checkpoint_receipt.v2"
    assert receipt["lease_id"] == observed[0]["lease_id"]
    original = receipt_path.read_bytes()
    downgraded = copy.deepcopy(receipt)
    downgraded["schema_version"] = "task_checkpoint_receipt.v1"
    downgraded.pop("execution")
    downgraded["receipt_sha256"] = _sha(
        _json({key: value for key, value in downgraded.items() if key != "receipt_sha256"}) + b"\n"
    )
    try:
        receipt_path.write_bytes(_json(downgraded))
        with pytest.raises(TaskCheckpointError, match="cannot downgrade"):
            engine.validate(receipt_path)
    finally:
        receipt_path.write_bytes(original)
    assert engine.validate(receipt_path)["status"] == "PASS"


def test_capture_retains_primary_failure_when_real_exit_cleanup_reports_error(
    case: CheckpointCase,
    engine: TaskCheckpoint,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from ai_trading_system.platform.architecture.workflow_execution import WindowsJobProcess

    planned = engine.plan(case.scope)
    actual_wait, actual_terminate = WindowsJobProcess.wait, WindowsJobProcess.terminate
    probe, _ = engine._context(planned)

    def corrupt_after_exit(process: Any, *, timeout: float) -> int:
        code = actual_wait(process, timeout=timeout)
        assert code == 0 and process.active_process_count() == 0
        (case.destination / "worker_result.json").write_bytes(b'{"status":"FAIL"}')
        return code

    def cleanup_error(process: Any, *, timeout: float = 10) -> int:
        actual_terminate(process, timeout=timeout)
        raise RuntimeError("synthetic cleanup reporting failure")

    monkeypatch.setattr(WindowsJobProcess, "wait", corrupt_after_exit)
    monkeypatch.setattr(WindowsJobProcess, "terminate", cleanup_error)
    try:
        with pytest.raises(TaskCheckpointError, match="successful result"):
            engine.capture(planned)
        diagnostic = json.loads((case.destination / "execution_failure.json").read_bytes())
        assert diagnostic["primary_error_code"] == "TASK_CHECKPOINT_EXECUTION"
        assert diagnostic["cleanup_errors"] == [
            {"operation": "terminate", "error_code": "RuntimeError"}
        ]
        active = probe.guard.store.replay().active_leases
        assert len(active) == 1 and active[0].execution["state"] == "EXIT_CONFIRMED"
        assert active[0].execution["exit"]["returncode"] == 0
        assert not (case.destination / "receipt.json").exists()
        assert (case.destination / "worker_result.json").read_bytes() == b'{"status":"FAIL"}'
    finally:
        # Synthetic harness cleanup only after the real contained process exited.
        for lease in probe.guard.store.replay().active_leases:
            if lease.execution and lease.execution["state"] == "EXIT_CONFIRMED":
                probe.guard.store.execution_lifecycle().record_incomplete_result(
                    lease.lease_id, actor=ACTOR
                )
                probe.guard.release(lease.lease_id, actor=ACTOR, outcome="failed")


def test_capture_retains_bounded_job_query_diagnostics_before_public_error_wrap(
    case: CheckpointCase, engine: TaskCheckpoint, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real worker exit plus deterministic query seam; not a native budget reproduction."""
    from types import SimpleNamespace

    from ai_trading_system.platform.architecture import workflow_execution as execution

    planned = engine.plan(case.scope)
    actual_wait = execution.WindowsJobProcess.wait
    main_before = _git(case.main_root, "rev-parse", "HEAD")

    def query(job, kind, buffer, size, returned):
        if kind == 1:
            buffer._obj.ActiveProcesses = 0
        else:
            assert kind == 3
            buffer._obj.assigned, buffer._obj.count = 2, 1
        return True

    def fail_after_real_exit(process: Any, *, timeout: float) -> int:
        code = actual_wait(process, timeout=timeout)
        assert code == 0 and process.active_process_count() == 0
        execution._JobProcesses(SimpleNamespace(QueryInformationJobObject=query), 123).collect()
        pytest.fail("short query must exhaust the existing bounded guard")

    monkeypatch.setattr(execution.WindowsJobProcess, "wait", fail_after_real_exit)
    with pytest.raises(TaskCheckpointError, match="WORKFLOW_EXECUTION_JOB_PROCESS_LIST_BUDGET"):
        engine.capture(planned)
    failure = json.loads((case.destination / "execution_failure.json").read_bytes())
    assert failure["primary_error_code"] == "WORKFLOW_EXECUTION_JOB_PROCESS_LIST_BUDGET"
    assert failure["cleanup_errors"] == []
    diagnostic = failure["job_process_list_diagnostics"]
    assert diagnostic["observation_only"] is True
    assert diagnostic["active_process_count"] == 0
    assert diagnostic["retained_process_count"] == 0
    assert diagnostic["queries"] == [
        {"capacity": 2 ** power, "ok": True, "winerror": 0, "assigned": 2, "count": 1}
        for power in range(4, 17)
    ]
    assert not (case.destination / "receipt.json").exists()
    assert _git(case.main_root, "rev-parse", "HEAD") == main_before


def _interrupted_cli_setup(case: CheckpointCase) -> tuple[str, dict[str, str], Path]:
    environment = {
        **os.environ,
        "PYTHONPATH": str(case.main_root / "src"),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    entry = str(case.main_root / checkpoint.CLI_PATH)
    scope_path = case.main_root / "outputs/validation_runtime/interrupted-scope.json"
    request_path = scope_path.with_name("interrupted-request.json")
    _write(case.main_root, scope_path.relative_to(case.main_root).as_posix(), _json(case.scope))
    value = _fresh_checkpoint_cli(
        case, entry, environment, "plan", "--scope", str(scope_path), "--output", str(request_path)
    )
    assert value["status"] == "PASS"
    return entry, environment, request_path


def _fresh_checkpoint_cli(
    case: CheckpointCase,
    entry: str,
    environment: dict[str, str],
    *arguments: str,
    expected: int = 0,
) -> dict[str, Any]:
    # Each recovery/observation is an actual fresh interpreter, not an engine
    # object reused from the producer or a mocked process observation.
    with subprocess.Popen(
        [sys.executable, entry, *arguments],
        cwd=case.main_root,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    ) as child:
        try:
            stdout, stderr = child.communicate(timeout=180)
        except BaseException:
            child.kill()
            child.communicate(timeout=30)
            raise
        assert child.returncode == expected, (stdout, stderr)
        return json.loads(stdout)


def _assert_interrupted_recovery(
    case: CheckpointCase, entry: str, environment: dict[str, str], request_path: Path
) -> None:
    run = case.destination
    original = {
        path.relative_to(run).as_posix(): path.read_bytes()
        for path in run.rglob("*")
        if path.is_file()
    }
    source = _state(case)
    refs = _git(case.root, "for-each-ref", "refs/aits/task-checkpoints/")
    arguments = ("recover-interrupted", "--request", str(request_path), "--actor", ACTOR)
    result = _fresh_checkpoint_cli(case, entry, environment, *arguments)
    assert result["status"] == "INSUFFICIENT"
    assert result["action"] == "FAILED_ATTEMPT_TERMINAL_ONLY"
    assert result["dispatch_allowed"] is False
    recovery = run / "interrupted_recovery.json"
    assert recovery.is_file()
    first_recovery = recovery.read_bytes()
    replay = _fresh_checkpoint_cli(case, entry, environment, *arguments)
    assert replay["status"] == "INSUFFICIENT" and replay["dispatch_allowed"] is False
    assert recovery.read_bytes() == first_recovery
    rejected = _fresh_checkpoint_cli(
        case, entry, environment, "capture", "--request", str(request_path), expected=2
    )
    assert rejected["reason_code"] == "TASK_CHECKPOINT_PARTIAL"
    assert _state(case) == source
    assert _git(case.root, "for-each-ref", "refs/aits/task-checkpoints/") == refs
    assert not (run / "receipt.json").exists()
    assert not (run / "terminal_recovery.json").exists()
    for relative, content in original.items():
        assert (run / relative).read_bytes() == content
    guard = CheckoutLeaseGuard(
        project_root=case.root,
        policy_path=case.root / POLICIES[1],
        parallel_policy_path=case.root / POLICIES[2],
    )
    assert guard.store.replay().status == "PASS"
    assert not guard.store.replay().active_leases


_NATIVE_PRODUCER_STARTUP = """
import ctypes, json, os, runpy, sys
from ctypes import wintypes
from pathlib import Path
_times = [wintypes.FILETIME() for _ in range(4)]
_api = ctypes.WinDLL("kernel32", use_last_error=True)
assert _api.GetProcessTimes(ctypes.c_void_p(-1), *(ctypes.byref(item) for item in _times))
_identity = {
    "pid": os.getpid(),
    "creation_time": (_times[0].dwHighDateTime << 32) | _times[0].dwLowDateTime,
}
_startup = Path(sys.argv[-1])
_pending = _startup.with_suffix(".pending")
_pending.write_text(json.dumps(_identity), encoding="utf-8")
os.replace(_pending, _startup)
sys.argv.pop()
"""


def _terminate_fixture_producer(identity: dict[str, int]) -> None:
    """Terminate only the observed synthetic PID after native FILETIME recheck."""
    import ctypes
    from ctypes import wintypes

    from test_devx015_workflow_execution import NativeOracle

    oracle = NativeOracle()
    native = oracle.api.OpenProcess(0x00100000 | 0x1000 | 0x0001, False, identity["pid"])
    if not native:
        assert ctypes.get_last_error() == 87, "fixture producer absence is unproven"
        return
    try:
        assert oracle.creation_time(native) == identity["creation_time"], "fixture PID was reused"
        if not oracle.exited(native):
            oracle.api.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
            oracle.api.TerminateProcess.restype = wintypes.BOOL
            assert oracle.api.TerminateProcess(native, 31)
            assert oracle.exited(native, timeout=30)
    finally:
        assert oracle.api.CloseHandle(native)


def _interrupt_recovery_install(
    case: CheckpointCase, entry: str, environment: dict[str, str], request_path: Path
) -> None:
    from test_devx015_workflow_execution import NativeOracle, _until

    marker = case.main_root.parent / "recovery-startup.json"
    script = (
        _NATIVE_PRODUCER_STARTUP
        + """
import time
deadline = time.monotonic() + 120
while not _startup.with_suffix(".continue").exists():
    if time.monotonic() >= deadline:
        os._exit(98)
    time.sleep(.02)
from ai_trading_system.platform.architecture import source_preservation as safe
original = safe._write_once
def interrupted(path, content):
    if path.name.startswith("interrupted_recovery.staging-"):
        with path.open("xb") as stream:
            stream.write(content[:17])
            stream.flush()
            os.fsync(stream.fileno())
        os._exit(23)
    return original(path, content)
safe._write_once = interrupted
entry, request, actor = sys.argv[1:]
sys.argv = [entry, "recover-interrupted", "--request", request, "--actor", actor]
runpy.run_path(entry, run_name="__main__")
"""
    )
    child = subprocess.Popen(
        [sys.executable, "-c", script, entry, str(request_path), ACTOR, str(marker)],
        cwd=case.main_root,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _until(lambda: marker.exists() or child.poll() is not None, description="recovery producer")
        assert marker.exists(), child.communicate(timeout=30)
        identity = json.loads(marker.read_bytes())
        original_producer = json.loads((case.destination / "attempt.json").read_bytes())["producer"]
        assert identity != original_producer
        oracle = NativeOracle()
        with oracle.process(identity["pid"]) as native:
            assert oracle.creation_time(native) == identity["creation_time"]
            marker.with_suffix(".continue").write_bytes(b"continue")
            stdout, stderr = child.communicate(timeout=180)
            assert child.returncode == 23, (stdout, stderr)
            assert oracle.exited(native)
    finally:
        if marker.exists():
            _terminate_fixture_producer(json.loads(marker.read_bytes()))
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=30)
    assert not (case.destination / "interrupted_recovery.json").exists()
    staged = list(case.destination.glob("interrupted_recovery.staging-*.json"))
    assert len(staged) == 1 and len(staged[0].read_bytes()) == 17
    guard = CheckoutLeaseGuard(
        project_root=case.root,
        policy_path=case.root / POLICIES[1],
        parallel_policy_path=case.root / POLICIES[2],
    )
    attempt = json.loads((case.destination / "attempt.json").read_bytes())
    head = next(
        row
        for row in guard.store.replay().lease_heads
        if row.change_id == "checkout:" + attempt["intent_id"]
    )
    assert head.state == "RELEASED" and head.execution is None
    retained = {
        path.relative_to(case.destination).as_posix(): path.read_bytes()
        for path in case.destination.rglob("*")
        if path.is_file()
    }
    before_leases = guard.store.replay()
    wrong_actor = _fresh_checkpoint_cli(
        case,
        entry,
        environment,
        "recover-interrupted",
        "--request",
        str(request_path),
        "--actor",
        "wrong-actor",
        expected=2,
    )
    assert wrong_actor["reason_code"] == "TASK_CHECKPOINT_RECOVERY_ACTOR"
    changed = json.loads(request_path.read_bytes())
    changed["observed_main_sha"] = "0" * 40
    changed_path = request_path.with_name("interrupted-changed-request.json")
    changed_path.write_bytes(_json(changed))
    wrong_request = _fresh_checkpoint_cli(
        case,
        entry,
        environment,
        "recover-interrupted",
        "--request",
        str(changed_path),
        "--actor",
        ACTOR,
        expected=2,
    )
    assert wrong_request["reason_code"] == "TASK_CHECKPOINT_RECOVERY_REQUEST"
    assert guard.store.replay() == before_leases
    assert {
        path.relative_to(case.destination).as_posix(): path.read_bytes()
        for path in case.destination.rglob("*")
        if path.is_file()
    } == retained


def _cleanup_interrupted_producer(
    case: CheckpointCase, producer: subprocess.Popen, identity: dict[str, int] | None
) -> None:
    startup = case.main_root.parent / "producer-startup.json"
    if identity is None and startup.exists():
        identity = json.loads(startup.read_bytes())
    if identity is None and (case.destination / "attempt.json").exists():
        identity = json.loads((case.destination / "attempt.json").read_bytes())["producer"]
    try:
        if identity is not None:
            _terminate_fixture_producer(identity)
    finally:
        # Only after the actual interpreter is handled, reap its possible venv
        # redirector. Popen.pid itself is never treated as the execution owner.
        if producer.poll() is None:
            producer.kill()
        producer.communicate(timeout=30)


@pytest.mark.parametrize("phase", ["BEFORE_ACQUIRE", "ACTIVE_PERSISTED", "TRUNCATED_ACQUIRED"])
def test_interrupted_attempt_new_pid_recovers_real_pre_execution_producer_exit(
    tmp_path: Path, controlled_environment: None, phase: str
) -> None:
    from test_devx015_workflow_execution import NativeOracle, _until

    case = _case(tmp_path)
    entry, environment, request_path = _interrupted_cli_setup(case)
    marker, release = tmp_path / "producer-ready.json", tmp_path / "producer-exit"
    script = (
        _NATIVE_PRODUCER_STARTUP
        + """
import json, os, runpy, sys, time
from pathlib import Path
from ai_trading_system.platform.architecture.checkout_guard import CheckoutLeaseGuard
from ai_trading_system.platform.architecture.task_checkpoint import TaskCheckpoint
original = CheckoutLeaseGuard.acquire
entry, request, phase, marker, release = sys.argv[1:]
def stop_producer():
    Path(marker).write_text(json.dumps({"pid": os.getpid(), "phase": phase}), encoding="utf-8")
    deadline = time.monotonic() + 120
    while not Path(release).exists():
        if time.monotonic() >= deadline:
            os._exit(99)
        time.sleep(.02)
    os._exit(29)
def paused(self, **kwargs):
    result = original(self, **kwargs) if phase == "ACTIVE_PERSISTED" else None
    stop_producer()
original_event = TaskCheckpoint._event
def truncated(self, run, events, current, payload):
    if current != "ACQUIRED":
        return original_event(self, run, events, current, payload)
    path = run / "events" / "01-ACQUIRED.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(b'{"schema_version":'[:17])
        stream.flush()
        os.fsync(stream.fileno())
    stop_producer()
if phase == "TRUNCATED_ACQUIRED":
    TaskCheckpoint._event = truncated
else:
    CheckoutLeaseGuard.acquire = paused
sys.argv = [entry, "capture", "--request", request]
runpy.run_path(entry, run_name="__main__")
"""
    )
    producer = subprocess.Popen(
        [
            sys.executable,
            "-c",
            script,
            entry,
            str(request_path),
            phase,
            str(marker),
            str(release),
            str(tmp_path / "producer-startup.json"),
        ],
        cwd=case.main_root,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    oracle = NativeOracle()
    actual_producer: dict[str, int] | None = None
    try:
        _until(
            lambda: marker.exists() or producer.poll() is not None,
            description="producer boundary",
        )
        assert marker.exists(), producer.communicate(timeout=30)
        actual_pid = json.loads(marker.read_bytes())["pid"]
        with oracle.process(actual_pid) as native:
            actual_producer = {"pid": actual_pid, "creation_time": oracle.creation_time(native)}
            assert json.loads((tmp_path / "producer-startup.json").read_bytes()) == actual_producer
            attempt = json.loads((case.destination / "attempt.json").read_bytes())
            assert attempt["schema_version"] == "task_checkpoint_attempt.v2"
            assert attempt["producer"] == actual_producer
            assert (case.destination / "request.json").read_bytes() == request_path.read_bytes()
            assert not oracle.exited(native)
            guard = CheckoutLeaseGuard(
                project_root=case.root,
                policy_path=case.root / POLICIES[1],
                parallel_policy_path=case.root / POLICIES[2],
            )
            active = guard.store.replay().active_leases
            assert len(active) == (0 if phase == "BEFORE_ACQUIRE" else 1)
            if active:
                assert active[0].change_id == "checkout:" + attempt["intent_id"]
                assert active[0].execution is None
            if phase == "BEFORE_ACQUIRE":
                observed = _fresh_checkpoint_cli(
                    case,
                    entry,
                    environment,
                    "recover-interrupted",
                    "--request",
                    str(request_path),
                    "--actor",
                    ACTOR,
                )
                assert observed["status"] == "OBSERVE_ONLY"
                assert observed["action"] == "NONE" and observed["dispatch_allowed"] is False
                assert not (case.destination / "interrupted_recovery.json").exists()
                assert not oracle.exited(native)
            release.write_bytes(b"exit")
            stdout, stderr = producer.communicate(timeout=30)
            assert producer.returncode == 29, (stdout, stderr)
            assert oracle.exited(native)
        if phase == "ACTIVE_PERSISTED":
            _interrupt_recovery_install(case, entry, environment, request_path)
        _assert_interrupted_recovery(case, entry, environment, request_path)
        recovery_path = case.destination / "interrupted_recovery.json"
        if phase == "TRUNCATED_ACQUIRED":
            assert (
                case.destination / "events/01-ACQUIRED.json"
            ).read_bytes() == b'{"schema_version":'[:17]
            assert len((case.destination / "events/01-ACQUIRED.json").read_bytes()) == 17
            assert json.loads(recovery_path.read_bytes())["capture_events"]["status"] == "INVALID"
        elif phase == "BEFORE_ACQUIRE":
            from ai_trading_system.platform.architecture.checkout_guard import (
                CHECKOUT_SOURCE_ONLY_PROFILE,
            )

            frozen_recovery = recovery_path.read_bytes()
            historical = _fresh_checkpoint_cli(
                case, entry, environment, "validate", "--receipt", str(recovery_path)
            )
            # A separate lawful source-only lease B is live after no-lease A
            # was terminalized. Historical A validation must not adopt B.
            decision, later = guard.acquire(
                intent_id="b" * 64,
                task_id=case.scope["task_id"],
                thread_id="synthetic-later-checkpoint-thread",
                actor=ACTOR,
                operation_class=CheckoutOperationClass.SHARED_MUTATION,
                inspection_profile=CHECKOUT_SOURCE_ONLY_PROFILE,
                shared_paths=tuple(case.scope["paths"] + [RUNTIME]),
                base_commit=case.scope["source_head_sha"],
            )
            assert decision.status == "PASS" and later is not None
            try:
                before_later = guard.store.replay().active_leases
                assert len(before_later) == 1 and before_later[0].lease_id == later.lease_id
                assert (
                    _fresh_checkpoint_cli(
                        case, entry, environment, "validate", "--receipt", str(recovery_path)
                    )
                    == historical
                )
                assert (
                    _fresh_checkpoint_cli(
                        case,
                        entry,
                        environment,
                        "recover-interrupted",
                        "--request",
                        str(request_path),
                        "--actor",
                        ACTOR,
                    )
                    == historical
                )
                assert recovery_path.read_bytes() == frozen_recovery
                assert guard.store.replay().active_leases == before_later
            finally:
                later.release(outcome="completed")
            assert not guard.store.replay().active_leases
    finally:
        _cleanup_interrupted_producer(case, producer, actual_producer)


@pytest.mark.parametrize(
    "phase",
    [
        "RESERVED",
        "JOB_CREATED",
        "BOUND",
        "RESUME_INTENT",
        "BEFORE_EXIT_CONFIRMED",
        "EXIT_CONFIRMED",
        "BEFORE_RESULT_RECORDED",
        "RESULT_RECORDED",
        "BEFORE_SUCCESS_RELEASED",
        "SUCCESS_RELEASED",
    ],
)
def test_interrupted_checkpoint_execution_boundaries_recover_without_redispatch(
    tmp_path: Path, controlled_environment: None, phase: str
) -> None:
    from test_devx015_workflow_execution import NativeOracle, _until

    case = _case(tmp_path)
    entry, environment, request_path = _interrupted_cli_setup(case)
    marker = tmp_path / "execution-boundary.json"
    release = tmp_path / "producer-exit"
    script = (
        _NATIVE_PRODUCER_STARTUP
        + """
import time
from ai_trading_system.platform.architecture.checkout_guard import CheckoutLeaseGuard
from ai_trading_system.platform.architecture.workflow_coordination import ExecutionLifecycle
from ai_trading_system.platform.architecture.workflow_execution import WindowsJobProcess
entry, request, phase, marker, release = sys.argv[1:]
def boundary(handle=None):
    facts = {"pid": os.getpid(), "phase": phase}
    if handle is not None:
        facts["handle"] = handle.identity()
    pending = Path(marker).with_suffix(".pending")
    pending.write_text(json.dumps(facts), encoding="utf-8")
    os.replace(pending, marker)
    deadline = time.monotonic() + 120
    while not Path(release).exists():
        if time.monotonic() >= deadline:
            os._exit(99)
        time.sleep(.02)
    os._exit(29)
if phase == "JOB_CREATED":
    original = WindowsJobProcess.create
    def create(cls, **kwargs):
        handle = original(**kwargs)
        boundary(handle)
    WindowsJobProcess.create = classmethod(create)
elif phase == "RESUME_INTENT":
    def before_resume(self):
        boundary(self)
    WindowsJobProcess.resume = before_resume
elif phase in {"BEFORE_SUCCESS_RELEASED", "SUCCESS_RELEASED"}:
    original = CheckoutLeaseGuard.release
    def released(self, *args, **kwargs):
        assert kwargs["outcome"].upper() == "COMPLETED"
        if phase == "BEFORE_SUCCESS_RELEASED":
            boundary()
        result = original(self, *args, **kwargs)
        boundary()
    CheckoutLeaseGuard.release = released
else:
    method = {"RESERVED": "reserve", "BOUND": "bind",
              "BEFORE_EXIT_CONFIRMED": "confirm_exit", "EXIT_CONFIRMED": "confirm_exit",
              "BEFORE_RESULT_RECORDED": "record_result", "RESULT_RECORDED": "record_result"}[phase]
    original = getattr(ExecutionLifecycle, method)
    def completed(self, *args, **kwargs):
        if phase.startswith("BEFORE_"):
            boundary(args[1] if phase == "BEFORE_EXIT_CONFIRMED" else None)
        result = original(self, *args, **kwargs)
        boundary(args[1] if phase in {"BOUND", "EXIT_CONFIRMED"} else None)
    setattr(ExecutionLifecycle, method, completed)
sys.argv = [entry, "capture", "--request", request]
runpy.run_path(entry, run_name="__main__")
"""
    )
    producer = subprocess.Popen(
        [
            sys.executable,
            "-c",
            script,
            entry,
            str(request_path),
            phase,
            str(marker),
            str(release),
            str(tmp_path / "producer-startup.json"),
        ],
        cwd=case.main_root,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    oracle = NativeOracle()
    actual_producer: dict[str, int] | None = None
    try:
        _until(
            lambda: marker.exists() or producer.poll() is not None, description="execution boundary"
        )
        assert marker.exists(), producer.communicate(timeout=30)
        observed = json.loads(marker.read_bytes())
        assert observed["phase"] == phase
        actual_producer = json.loads((tmp_path / "producer-startup.json").read_bytes())
        assert observed["pid"] == actual_producer["pid"]
        guard = CheckoutLeaseGuard(
            project_root=case.root,
            policy_path=case.root / POLICIES[1],
            parallel_policy_path=case.root / POLICIES[2],
        )
        request = json.loads((case.destination / "execution_request.json").read_bytes())
        head = next(
            row for row in guard.store.replay().lease_heads if row.lease_id == request["lease_id"]
        )
        assert head.execution["launcher"] == actual_producer
        expected_state = {
            "RESERVED": "RESERVED",
            "JOB_CREATED": "RESERVED",
            "BOUND": "CONTAINED_SUSPENDED",
            "RESUME_INTENT": "RESUME_INTENT",
            "BEFORE_EXIT_CONFIRMED": "RUNNING",
            "EXIT_CONFIRMED": "EXIT_CONFIRMED",
            "BEFORE_RESULT_RECORDED": "EXIT_CONFIRMED",
            "RESULT_RECORDED": "RESULT_RECORDED",
            "BEFORE_SUCCESS_RELEASED": "RESULT_RECORDED",
            "SUCCESS_RELEASED": "RESULT_RECORDED",
        }[phase]
        assert head.execution["state"] == expected_state
        assert head.state == ("RELEASED" if phase == "SUCCESS_RELEASED" else "ACTIVE")
        lease_directory = guard.store.events_root / head.lease_id
        lease_before = {path.name: path.read_bytes() for path in lease_directory.glob("*.json")}
        with oracle.process(actual_producer["pid"]) as launcher:
            assert oracle.creation_time(launcher) == actual_producer["creation_time"]
            assert not oracle.exited(launcher)
            if phase in {"RESERVED", "BEFORE_SUCCESS_RELEASED", "SUCCESS_RELEASED"}:
                oracle.assert_job_absent(request["job_name"])
                release.write_bytes(b"exit")
                stdout, stderr = producer.communicate(timeout=30)
            else:
                identity = observed.get("handle", head.execution["process"])
                assert identity is not None
                with oracle.process(identity["pid"]) as worker:
                    assert oracle.creation_time(worker) == identity["creation_time"]
                    if phase in {"JOB_CREATED", "BOUND", "RESUME_INTENT"}:
                        assert not oracle.exited(worker)
                        oracle.assert_in_job(worker, request["job_name"])
                        assert not (case.destination / "worker_observation.json").exists()
                    else:
                        assert oracle.exited(worker)
                        if phase == "BEFORE_EXIT_CONFIRMED":
                            # Native exit is real while durable custody still says RUNNING.
                            assert head.execution["exit"] is None
                        else:
                            assert head.execution["exit"]["basis"] == "LIVE_CONTAINED_HANDLE"
                            assert head.execution["exit"]["returncode"] == 0
                    release.write_bytes(b"exit")
                    stdout, stderr = producer.communicate(timeout=30)
                    assert oracle.exited(worker, timeout=30)
            assert producer.returncode == 29, (stdout, stderr)
            assert oracle.exited(launcher)
        oracle.assert_job_absent(request["job_name"])
        assert not (case.destination / "events/06-RELEASED.json").exists()
        _assert_interrupted_recovery(case, entry, environment, request_path)
        recovered = json.loads((case.destination / "interrupted_recovery.json").read_bytes())
        assert recovered["checkpoint_status"] == "INSUFFICIENT"
        if phase in {
            "EXIT_CONFIRMED", "BEFORE_RESULT_RECORDED", "RESULT_RECORDED",
            "BEFORE_SUCCESS_RELEASED", "SUCCESS_RELEASED",
        }:
            assert recovered["execution_result"]["status"] == "PASS"
        else:
            assert recovered["execution_result"]["status"] == "INSUFFICIENT"
        if phase == "SUCCESS_RELEASED":
            assert any(
                "CHECKOUT_OPERATION_COMPLETED" in json.loads(content)["reason_codes"]
                for content in lease_before.values()
            )
            assert {
                path.name: path.read_bytes() for path in lease_directory.glob("*.json")
            } == lease_before
    finally:
        _cleanup_interrupted_producer(case, producer, actual_producer)


@pytest.mark.parametrize(
    "phase",
    ["BEFORE_RAW", "PARTIAL_RAW", "AFTER_RAW", "CAPTURED", "OBJECTS_WRITTEN", "REF_CREATED"],
)
def test_interrupted_captured_worker_requires_dead_producer_and_preserves_raw_evidence(
    tmp_path: Path, controlled_environment: None, phase: str
) -> None:
    from test_devx015_workflow_execution import NativeOracle

    raw_phase = phase in {"BEFORE_RAW", "PARTIAL_RAW", "AFTER_RAW"}
    case = _case(tmp_path, worker_probe=("raw-write" if raw_phase else "barrier", phase))
    entry, environment, request_path = _interrupted_cli_setup(case)
    source_before = _state(case)
    producer = subprocess.Popen(
        [
            sys.executable,
            "-c",
            _NATIVE_PRODUCER_STARTUP
            + """
entry, request = sys.argv[1:]
sys.argv = [entry, "capture", "--request", request]
runpy.run_path(entry, run_name="__main__")
""",
            entry,
            str(request_path),
            str(tmp_path / "producer-startup.json"),
        ],
        cwd=case.main_root,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    oracle = NativeOracle()
    killed: list[int] = []
    actual_producer: dict[str, int] | None = None
    try:

        def kill_launcher() -> None:
            nonlocal actual_producer
            waiting = _fresh_checkpoint_cli(
                case, entry, environment, "recover-interrupted", "--request",
                str(request_path), "--actor", ACTOR,
            )
            assert waiting["status"] == "OBSERVE_ONLY"
            assert waiting["dispatch_allowed"] is False and waiting["action"] == "NONE"
            assert not (case.destination / "interrupted_recovery.json").exists()
            witness = json.loads((case.destination / "worker_observation.json").read_bytes())
            guard = CheckoutLeaseGuard(
                project_root=case.root,
                policy_path=case.root / POLICIES[1],
                parallel_policy_path=case.root / POLICIES[2],
            )
            head = next(
                row
                for row in guard.store.replay().lease_heads
                if row.lease_id == witness["lease_id"]
            )
            actual_producer = head.execution["launcher"]
            assert json.loads((tmp_path / "producer-startup.json").read_bytes()) == actual_producer
            assert (
                json.loads((case.destination / "attempt.json").read_bytes())["producer"]
                == actual_producer
            )
            with oracle.process(actual_producer["pid"]) as launcher:
                assert oracle.creation_time(launcher) == actual_producer["creation_time"]
                with oracle.process(witness["worker_process"]["pid"]) as worker:
                    assert (
                        oracle.creation_time(worker) == witness["worker_process"]["creation_time"]
                    )
                    _terminate_fixture_producer(actual_producer)
                    assert oracle.exited(launcher, timeout=30)
                    assert oracle.exited(worker, timeout=30)
                    killed.append(witness["worker_process"]["pid"])

        with _worker_probe(case, kill_launcher) as observations:
            producer.communicate(timeout=180)
        assert producer.returncode != 0 and len(killed) == 1
        execution = json.loads((case.destination / "execution_request.json").read_bytes())
        oracle.assert_job_absent(execution["job_name"])
        reached = {json.loads(path.read_bytes())["phase"]
                   for path in (case.destination / "events").glob("*.json")}
        expected_events = {"ACQUIRED"} if raw_phase else {"ACQUIRED", "CAPTURED"}
        if phase in {"OBJECTS_WRITTEN", "REF_CREATED"}:
            expected_events.add("OBJECTS_WRITTEN")
        if phase == "REF_CREATED":
            expected_events.add("REF_CREATED")
        assert reached == expected_events
        if raw_phase:
            assert not (case.destination / "capture_manifest.json").exists()
            storage = observations[0]["storage_path"]
            request = json.loads(request_path.read_bytes())
            source_name = request["files"][int(Path(storage).stem)]["path"]
            expected = case.expected[source_name]
            assert expected is not None and len(expected) >= 2
            raw_path = case.destination / storage
            if phase == "BEFORE_RAW":
                assert not raw_path.exists()
            else:
                assert raw_path.read_bytes() == (
                    expected[:len(expected) // 2] if phase == "PARTIAL_RAW" else expected
                )
        else:
            bundle = json.loads((case.destination / "capture_manifest.json").read_bytes())
            assert {
                row["file"]["path"]: None
                if row["storage_path"] is None
                else (case.destination / row["storage_path"]).read_bytes()
                for row in bundle["records"]
            } == case.expected
        attempt_path = case.destination / "attempt.json"
        original = attempt_path.read_bytes()
        tampered = json.loads(original)
        tampered["producer"]["creation_time"] += 1
        attempt_path.write_bytes(_json(tampered))
        refused = _fresh_checkpoint_cli(
            case,
            entry,
            environment,
            "recover-interrupted",
            "--request",
            str(request_path),
            "--actor",
            ACTOR,
            expected=2,
        )
        assert refused["reason_code"] == "TASK_CHECKPOINT_RECOVERY_PRODUCER"
        assert not (case.destination / "interrupted_recovery.json").exists()
        attempt_path.write_bytes(original)
        _assert_interrupted_recovery(case, entry, environment, request_path)
        assert _state(case) == source_before
    finally:
        _cleanup_interrupted_producer(case, producer, actual_producer)


def test_terminal_recovery_public_cli_after_real_producer_exit(
    tmp_path: Path, controlled_environment: None
) -> None:
    case = _case(tmp_path)
    scope_path = case.main_root / "outputs/validation_runtime/recovery-scope.json"
    request_path = case.main_root / "outputs/validation_runtime/recovery-request.json"
    _write(case.main_root, scope_path.relative_to(case.main_root).as_posix(), _json(case.scope))
    environment = {
        **os.environ,
        "PYTHONPATH": str(case.main_root / "src"),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    entry = str(case.main_root / checkpoint.CLI_PATH)

    def cli(*arguments: str, expected: int = 0) -> dict[str, Any]:
        completed = subprocess.run(
            [sys.executable, entry, *arguments],
            cwd=case.main_root,
            env=environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=300,
            check=False,
        )
        assert completed.returncode == expected, (completed.stdout, completed.stderr)
        return json.loads(completed.stdout)

    before = _state(case)
    cli("plan", "--scope", str(scope_path), "--output", str(request_path))
    # Fault injection only: real committed implementation, guard, lease, Git and
    # public capture CLI run in another PID. No identity verifier is replaced.
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import os, runpy, sys
from ai_trading_system.platform.architecture.task_checkpoint import TaskCheckpoint
original = TaskCheckpoint._event
def crash(self, run, events, phase, payload):
    original(self, run, events, phase, payload)
    if phase == 'RELEASED':
        os._exit(17)
TaskCheckpoint._event = crash
sys.argv = [sys.argv[1], 'capture', '--request', sys.argv[2]]
runpy.run_path(sys.argv[0], run_name='__main__')
""",
            entry,
            str(request_path),
        ],
        cwd=case.main_root,
        env=environment,
        capture_output=True,
        timeout=300,
        check=False,
    )
    assert child.returncode == 17, (child.stdout, child.stderr)
    run = case.root / RUNTIME / case.scope["checkpoint_id"]
    assert not (run / "receipt.json").exists()
    assert not (run / "failure.json").exists()
    assert len(list((run / "events").glob("*.json"))) == 6
    originals = {
        p.relative_to(run).as_posix(): p.read_bytes() for p in run.rglob("*") if p.is_file()
    }
    refs_before = _git(case.root, "for-each-ref", "refs/aits/task-checkpoints/")
    # Ordinary capture remains explicitly non-retrying; only the new finite
    # receipt action is permitted. This refusal is not claimed as a new red.
    assert (
        cli("capture", "--request", str(request_path), expected=2)["reason_code"]
        == "TASK_CHECKPOINT_PARTIAL"
    )
    wrong = cli(
        "recover-terminal", "--request", str(request_path), "--actor", "wrong-actor", expected=2
    )
    assert wrong["reason_code"] == "TASK_CHECKPOINT_RECOVERY_ACTOR"
    assert not (run / "terminal_recovery.json").exists()
    contenders = [
        subprocess.Popen(
            [
                sys.executable,
                entry,
                "recover-terminal",
                "--request",
                str(request_path),
                "--actor",
                ACTOR,
            ],
            cwd=case.main_root,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        for _ in range(2)
    ]
    assert contenders[0].pid != contenders[1].pid
    outcomes = []
    try:
        for contender in contenders:
            stdout, stderr = contender.communicate(timeout=300)
            assert contender.returncode == 0, (stdout, stderr)
            outcomes.append(json.loads(stdout))
    finally:
        for contender in contenders:
            if contender.poll() is None:
                contender.kill()
                contender.communicate(timeout=30)
    assert all(value["status"] in {"PASS", "WAITING_FOR_ARBITER"} for value in outcomes)
    completed = [value for value in outcomes if value["status"] == "PASS"]
    assert completed, "at least one admitted recovery must reach its terminal result"
    recovered = completed[0]
    for value in outcomes:
        if value["status"] == "PASS":
            assert value == recovered
        else:
            assert value["action"] == "NONE"
            assert value["next_action"] == "RETRY_SAME_RECOVERY_REQUEST_AFTER_ARBITER_RELEASE"
    assert recovered["status"] == "PASS"
    assert recovered["action"] == "RECEIPT_RECONSTRUCTION_ONLY"
    recovery_path = run / "terminal_recovery.json"
    frozen = recovery_path.read_bytes(), recovery_path.stat().st_mtime_ns
    assert cli("recover-terminal", "--request", str(request_path), "--actor", ACTOR) == recovered
    assert (recovery_path.read_bytes(), recovery_path.stat().st_mtime_ns) == frozen
    assert cli("validate", "--receipt", str(recovery_path)) == recovered
    assert {
        p.relative_to(run).as_posix(): p.read_bytes()
        for p in run.rglob("*")
        if p.is_file() and p != recovery_path
    } == originals
    assert _state(case) == before
    assert _git(case.root, "for-each-ref", "refs/aits/task-checkpoints/") == refs_before
    envelope = json.loads(frozen[0])
    snapshot = envelope["recovered_receipt"]["snapshot"]["commit"]
    for path, content in case.expected.items():
        if content is None:
            assert _git(case.root, "ls-tree", snapshot, "--", path) == b""
        else:
            assert _git(case.root, "cat-file", "blob", f"{snapshot}:{path}") == content


@pytest.mark.parametrize("phase", ["CAPTURED", "RELEASED"])
def test_terminal_recovery_boundaries_preserve_original_failure(
    tmp_path: Path, controlled_environment: None, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    case = _case(tmp_path, worker_probe=("interrupt", phase) if phase == "CAPTURED" else None)
    engine = _engine(case, monkeypatch)
    request = engine.plan(case.scope)
    original = engine._event

    def fail_after(run: Path, events: list, stage: str, payload: dict) -> None:
        original(run, events, stage, payload)
        if stage == phase:
            raise RuntimeError("synthetic terminal recovery fault")

    if phase == "CAPTURED":
        with _worker_probe(case):
            with pytest.raises(TaskCheckpointError, match="TASK_CHECKPOINT_EXECUTION"):
                engine.capture(request)
        execution = json.loads((case.destination / "execution_request.json").read_bytes())
        fence, _ = engine._context(request)
        head = next(
            row
            for row in fence.guard.store.replay().lease_heads
            if row.lease_id == execution["lease_id"]
        )
        assert head.state == "RELEASED"
        assert head.execution["state"] == "RESULT_RECORDED"
        assert head.execution["exit"]["basis"] == "LIVE_CONTAINED_HANDLE"
        assert head.execution["exit"]["returncode"] != 0
        assert head.execution["result"]["status"] == "INSUFFICIENT"
    else:
        monkeypatch.setattr(engine, "_event", fail_after)
        with pytest.raises(RuntimeError, match="synthetic terminal recovery fault"):
            engine.capture(request)
    run = case.root / RUNTIME / request["checkpoint_id"]
    failure = (run / "failure.json").read_bytes()
    before = _state(case)
    if phase == "CAPTURED":
        with pytest.raises(TaskCheckpointError, match="RECOVERY_NOT_TERMINAL"):
            engine.recover_terminal(request, actor=ACTOR)
        assert not (run / "terminal_recovery.json").exists()
    else:
        changed = copy.deepcopy(request)
        changed["observed_main_sha"] = "0" * 40
        with pytest.raises(TaskCheckpointError, match="RECOVERY_REQUEST"):
            engine.recover_terminal(changed, actor=ACTOR)
        assert not (run / "terminal_recovery.json").exists()
        result = engine.recover_terminal(request, actor=ACTOR)
        assert result["status"] == "PASS"
        # The original failed attempt is still not an ordinary complete receipt.
        with pytest.raises(TaskCheckpointError, match="PARTIAL"):
            engine.validate(run / "receipt.json")
        record = json.loads((run / "terminal_recovery.json").read_bytes())
        record["actor"] = "forged-actor"
        body = {k: v for k, v in record.items() if k != "recovery_sha256"}
        record["recovery_sha256"] = _sha(_json(body) + b"\n")
        (run / "terminal_recovery.json").write_bytes(_json(record))
        with pytest.raises(TaskCheckpointError, match="RECOVERY_RECEIPT"):
            engine.validate(run / "terminal_recovery.json")
        tampered = (run / "terminal_recovery.json").read_bytes()
        with pytest.raises(TaskCheckpointError, match="RECOVERY_CONFLICT"):
            engine.recover_terminal(request, actor=ACTOR)
        assert (run / "terminal_recovery.json").read_bytes() == tampered
    assert (run / "failure.json").read_bytes() == failure
    assert _state(case) == before


@pytest.mark.parametrize("attack", ["live_lease", "raw_bundle", "terminal_event"])
def test_terminal_recovery_rejects_changed_original_authority(
    case: CheckpointCase, engine: TaskCheckpoint, monkeypatch: pytest.MonkeyPatch, attack: str
) -> None:
    request = engine.plan(case.scope)
    original = engine._event

    def fail_after(run: Path, events: list, stage: str, payload: dict) -> None:
        original(run, events, stage, payload)
        if stage == "RELEASED":
            raise RuntimeError("synthetic terminal boundary")

    monkeypatch.setattr(engine, "_event", fail_after)
    with pytest.raises(RuntimeError, match="synthetic terminal boundary"):
        engine.capture(request)
    run = case.root / RUNTIME / request["checkpoint_id"]
    before = _state(case)
    failure = (run / "failure.json").read_bytes()
    if attack == "live_lease":
        fence, _ = engine._context(request)
        lease_id = json.loads((run / "events/01-ACQUIRED.json").read_bytes())["payload"]["lease_id"]
        target = next((fence.guard.store.events_root / lease_id).glob("*.json"))
        target.write_bytes(target.read_bytes() + b"\n")
        expected = "RECOVERY_LEASE"
    elif attack == "raw_bundle":
        target = next((run / "capture").glob("*.bin"))
        target.write_bytes(target.read_bytes() + b"synthetic-tamper")
        expected = "CAPTURE_BUNDLE"
    else:
        target = run / "events/06-RELEASED.json"
        event = json.loads(target.read_bytes())
        event["payload"]["lease_state"] = "ACTIVE"
        body = {key: value for key, value in event.items() if key != "event_id"}
        event["event_id"] = _sha(_json(body) + b"\n")
        target.write_bytes(_json(event))
        expected = "RECEIPT"
    with pytest.raises(TaskCheckpointError, match=expected):
        engine.recover_terminal(request, actor=ACTOR)
    assert not (run / "terminal_recovery.json").exists()
    assert (run / "failure.json").read_bytes() == failure
    assert _state(case) == before


def test_terminal_recovery_own_partial_write_is_recoverable_in_new_pid(
    case: CheckpointCase, engine: TaskCheckpoint, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = engine.plan(case.scope)
    original = engine._event

    def fail_after(run: Path, events: list, stage: str, payload: dict) -> None:
        original(run, events, stage, payload)
        if stage == "RELEASED":
            raise RuntimeError("synthetic original terminal boundary")

    monkeypatch.setattr(engine, "_event", fail_after)
    with pytest.raises(RuntimeError, match="synthetic original terminal boundary"):
        engine.capture(request)
    run = case.root / RUNTIME / request["checkpoint_id"]
    before = _state(case)
    failure = (run / "failure.json").read_bytes()
    request_path = case.main_root / "outputs/validation_runtime/partial-recovery-request.json"
    _write(case.main_root, request_path.relative_to(case.main_root).as_posix(), _json(request))
    entry = str(case.main_root / checkpoint.CLI_PATH)
    environment = {
        **os.environ,
        "PYTHONPATH": str(case.main_root / "src"),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    crashed = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import os, runpy, sys
from ai_trading_system.platform.architecture import source_preservation as safe
original = safe._write_once
def crash(path, content):
    if path.name == 'terminal_recovery.json' or path.name.startswith('terminal_recovery.staging-'):
        with path.open('xb') as output:
            output.write(content[:17])
            output.flush()
            os.fsync(output.fileno())
        os._exit(23)
    original(path, content)
safe._write_once = crash
sys.argv = [sys.argv[1], 'recover-terminal', '--request', sys.argv[2], '--actor', sys.argv[3]]
runpy.run_path(sys.argv[0], run_name='__main__')
""",
            entry,
            str(request_path),
            ACTOR,
        ],
        cwd=case.main_root,
        env=environment,
        capture_output=True,
        timeout=300,
        check=False,
    )
    assert crashed.returncode == 23, (crashed.stdout, crashed.stderr)
    # The old direct-to-final writer fails HERE, not at import/API/fixture setup.
    assert not (run / "terminal_recovery.json").exists(), "partial recovery was exposed as final"
    interrupted = {
        path.name: path.read_bytes() for path in run.glob("terminal_recovery.staging-*.json")
    }
    assert len(interrupted) == 1 and all(len(content) == 17 for content in interrupted.values())
    resumed = subprocess.run(
        [
            sys.executable,
            entry,
            "recover-terminal",
            "--request",
            str(request_path),
            "--actor",
            ACTOR,
        ],
        cwd=case.main_root,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=300,
        check=False,
    )
    assert resumed.returncode == 0, (resumed.stdout, resumed.stderr)
    result = json.loads(resumed.stdout)
    assert result["status"] == "PASS" and result["action"] == "RECEIPT_RECONSTRUCTION_ONLY"
    assert {
        path.name: path.read_bytes() for path in run.glob("terminal_recovery.staging-*.json")
    } == interrupted
    assert (run / "failure.json").read_bytes() == failure
    assert _state(case) == before


def test_terminal_recovery_real_busy_observation_then_terminal_replay(
    case: CheckpointCase, engine: TaskCheckpoint
) -> None:
    from datetime import UTC, datetime

    request = engine.plan(case.scope)
    captured = engine.capture(request)
    run = case.root / RUNTIME / request["checkpoint_id"]
    fence, _ = engine._context(request)
    before = engine._lease_bytes(fence, captured["lease_id"])
    # A deliberately held real OS handle, not a mocked BUSY exception. Recovery
    # resolves another store instance and cannot borrow this handle/capability.
    with fence.guard.store.atomic(
        actor=ACTOR, now=datetime.now(UTC), operation="synthetic-held-arbiter"
    ):
        waiting = engine.recover_terminal(request, actor=ACTOR)
        assert waiting["status"] == "WAITING_FOR_ARBITER"
        assert waiting["action"] == "NONE"
        assert waiting["next_action"] == "RETRY_SAME_RECOVERY_REQUEST_AFTER_ARBITER_RELEASE"
        assert not (run / "terminal_recovery.json").exists()
        assert not list(run.glob("terminal_recovery.staging-*.json"))
    result = engine.recover_terminal(request, actor=ACTOR)
    assert result["status"] == "PASS"
    assert engine._lease_bytes(fence, captured["lease_id"]) == before
    assert engine.recover_terminal(request, actor=ACTOR) == result


def test_terminal_recovery_install_never_overwrites_existing_windows_file(tmp_path: Path) -> None:
    target = tmp_path / "terminal_recovery.json"
    target.write_bytes(b"synthetic-existing-canary")
    with pytest.raises(FileExistsError):
        TaskCheckpoint._install_terminal_recovery(target, b"synthetic-derived-receipt")
    assert target.read_bytes() == b"synthetic-existing-canary"
    staged = list(tmp_path.glob("terminal_recovery.staging-*.json"))
    assert len(staged) == 1 and staged[0].read_bytes() == b"synthetic-derived-receipt"


def test_historical_implementation_verifier_rejects_forged_or_incomplete_real_bindings(
    case: CheckpointCase, engine: TaskCheckpoint, binding_variant: str | None = None
) -> None:
    original = engine._implementation_binding()
    engine._verify_implementation(original)
    variants: dict[str, dict[str, Any]] = {}
    for name in (
        "schema",
        "empty_own",
        "missing_helper",
        "duplicate_own",
        "unknown_commit",
        "nested_commit",
        "own_blob_hash",
        "helper_blob_hash",
        "wrong_root",
    ):
        value = copy.deepcopy(original)
        if name == "schema":
            value["schema_version"] = "synthetic_invented_implementation.v1"
        elif name == "empty_own":
            value["files"] = []
        elif name == "missing_helper":
            value["safe_git_implementation"]["files"].pop()
        elif name == "duplicate_own":
            value["files"].append(copy.deepcopy(value["files"][0]))
        elif name == "unknown_commit":
            value["commit"] = value["safe_git_implementation"]["commit"] = "0" * 40
        elif name == "nested_commit":
            value["safe_git_implementation"]["commit"] = "0" * 40
        elif name == "own_blob_hash":
            value["files"][0]["git_blob_content_sha256"] = "0" * 64
            value["files"][0]["sha256"] = "0" * 64
        elif name == "helper_blob_hash":
            value["safe_git_implementation"]["files"][0]["git_blob_content_sha256"] = "0" * 64
            value["safe_git_implementation"]["files"][0]["sha256"] = "0" * 64
        else:
            value["project_root"] = case.root.as_posix()
        variants[name] = value
    if binding_variant is not None:
        assert binding_variant in variants
        variants = {binding_variant: variants[binding_variant]}
        (case.main_root.parent / "m08-binding.json").write_bytes(_json({
            "variant": binding_variant, "original": original,
            "forged": variants[binding_variant],
        }))
    for binding in variants.values():
        with pytest.raises((TaskCheckpointError, safe.SourcePreservationError)):
            # No hash/schema helper is replaced: even a structurally plausible
            # caller-supplied binding must independently resolve real Git blobs.
            engine._verify_implementation(binding)
    assert engine._implementation_binding() == original


@pytest.mark.parametrize("binding_variant", ["own_blob_hash", "helper_blob_hash"])
@pytest.mark.parametrize("mutation", [False, True], ids=["original", "M08"])
def test_m08_self_reported_hash_mutant_hits_original_historical_refusal(
    case: CheckpointCase, engine: TaskCheckpoint, monkeypatch: pytest.MonkeyPatch,
    binding_variant: str, mutation: bool,
) -> None:
    import inspect
    import textwrap

    original = TaskCheckpoint._verify_implementation
    before = textwrap.dedent(inspect.getsource(original))
    target = (
        '        if safe._sha(committed) != row["git_blob_content_sha256"]:\n'
        '            _fail("IDENTITY", "historical implementation blob checksum mismatch")\n'
    )
    assert before.count(target) == 1, "INVALID_MUTATION_TARGET"
    after = before.replace(target, "")
    code = compile(after, "<M08-trust-self-reported-hash>", "exec")
    directory = case.main_root.parent
    (directory / "m08-method-before.py").write_bytes(before.encode())
    (directory / "m08-method-after.py").write_bytes(after.encode())
    state = _state(case)
    refs = _git(case.root, "show-ref")
    with monkeypatch.context() as scoped:
        if mutation:
            namespace: dict = {}
            exec(code, original.__globals__, namespace)
            scoped.setattr(TaskCheckpoint, "_verify_implementation", namespace[original.__name__])
            with pytest.raises(pytest.fail.Exception, match=r"^DID NOT RAISE"):
                test_historical_implementation_verifier_rejects_forged_or_incomplete_real_bindings(
                    case, engine, binding_variant
                )
        else:
            test_historical_implementation_verifier_rejects_forged_or_incomplete_real_bindings(
                case, engine, binding_variant
            )
    assert TaskCheckpoint._verify_implementation is original
    evidence = json.loads((directory / "m08-binding.json").read_bytes())
    forged = evidence["forged"]
    row = (forged["files"][0] if binding_variant == "own_blob_hash"
           else forged["safe_git_implementation"]["files"][0])
    actual = _git(case.main_root, "cat-file", "blob", f"{forged['commit']}:{row['path']}")
    assert _sha(actual) != row["sha256"] == row["git_blob_content_sha256"]
    engine._verify_implementation(evidence["original"])
    with pytest.raises(
        TaskCheckpointError, match="historical implementation blob checksum mismatch"
    ):
        engine._verify_implementation(forged)
    assert _state(case) == state and _git(case.root, "show-ref") == refs
    (directory / "m08-counterfactual.json").write_bytes(_json({
        "mutant_id": "M08", "mutation": mutation, "variant": binding_variant,
        "target_assertion_killed": mutation, "actual_blob_sha256": _sha(actual),
        "claimed_blob_sha256": row["git_blob_content_sha256"], "commit": forged["commit"],
        "path": row["path"], "before_method_sha256": _sha(before.encode()),
        "after_method_sha256": _sha(after.encode()),
        "module_sha256": _sha((case.main_root / checkpoint.MODULE_PATH).read_bytes()),
        "scope": "ORIGINAL_HISTORICAL_IMPLEMENTATION_GIT_BLOB_VERIFIER",
        "formal_project_acceptance": False,
    }))


def test_historical_implementation_truth_is_distinct_from_current_execution_admission(
    case: CheckpointCase, engine: TaskCheckpoint
) -> None:
    original = engine._implementation_binding()
    path = case.main_root / checkpoint.MODULE_PATH
    path.write_bytes(path.read_bytes() + b"\n# later trusted implementation\n")
    _commit(case.main_root, (checkpoint.MODULE_PATH,), "synthetic later implementation")
    # Historical bytes still exist and are independently true; they cannot
    # authorize execution of the changed current implementation.
    engine._verify_implementation(original, current=False)
    with pytest.raises(TaskCheckpointError):
        engine._verify_implementation(original, current=True)


def test_acquisition_fault_after_real_active_persistence_retains_attempt_and_blocks_same_id_retry(
    case: CheckpointCase,
    engine: TaskCheckpoint,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    planned = engine.plan(case.scope)
    probe = CheckoutLeaseGuard(
        project_root=case.root,
        policy_path=case.root / POLICIES[1],
        parallel_policy_path=case.root / POLICIES[2],
    )
    actual_acquire = FileExecutionLeaseStore.acquire
    acquired: list[str] = []
    calls: list[str] = []

    def persist_active_then_raise(store: FileExecutionLeaseStore, **kwargs: Any) -> Any:
        calls.append("acquire")
        result = actual_acquire(store, **kwargs)
        assert result.status == "ACTIVE"
        acquired.append(result.lease.lease_id)
        replay = store.replay()
        assert replay.status == "PASS"
        assert any(row.lease_id == result.lease.lease_id for row in replay.active_leases)
        # Simulate an arbiter/finalization error after the real append-only ACTIVE
        # event exists, before CheckoutLeaseGuard can return a caller handle.
        raise RuntimeError("synthetic acquisition fault after actual ACTIVE persistence")

    monkeypatch.setattr(FileExecutionLeaseStore, "acquire", persist_active_then_raise)
    before = _state(case)
    try:
        with pytest.raises((TaskCheckpointError, RuntimeError)):
            engine.capture(planned)
        assert calls == ["acquire"] and len(acquired) == 1
        assert _state(case) == before
        assert not (case.destination / "receipt.json").exists()
        attempt = json.loads((case.destination / "attempt.json").read_bytes())
        failure = json.loads((case.destination / "failure.json").read_bytes())
        assert attempt["schema_version"] == "task_checkpoint_attempt.v2"
        assert attempt["checkpoint_id"] == case.scope["checkpoint_id"]
        assert attempt["request_sha256"] == _sha(_json(planned) + b"\n")
        assert attempt["request_sha256"] == _sha((case.destination / "request.json").read_bytes())
        assert failure["attempt_intent_id"] == attempt["intent_id"]
        assert failure["lease_state"] == "ACTIVE"
        assert failure["release_disposition"] == "DEFERRED_ACQUISITION_FAILED"
        replay = probe.replay()
        active = [row for row in replay.active_leases if row.lease_id == acquired[0]]
        assert len(active) == 1
        assert active[0].change_id == "checkout:" + attempt["intent_id"]
        retained = {
            path.relative_to(case.destination).as_posix(): path.read_bytes()
            for path in case.destination.rglob("*")
            if path.is_file()
        }
        with pytest.raises(TaskCheckpointError) as retry:
            engine.capture(copy.deepcopy(planned))
        assert retry.value.code == "TASK_CHECKPOINT_PARTIAL"
        assert calls == ["acquire"], "same id must not enter the lease kernel again"
        assert {
            path.relative_to(case.destination).as_posix(): path.read_bytes()
            for path in case.destination.rglob("*")
            if path.is_file()
        } == retained
        request.node.user_properties.append(
            ("acquisition_fault_observation", "REAL_ACTIVE_LEASE_RETAINED_BEFORE_TEST_CLEANUP")
        )
    finally:
        # Cleanup is explicit test-harness action, not automatic production
        # recovery. Retained attempt/failure evidence is never rewritten.
        for lease_id in acquired:
            if any(row.lease_id == lease_id for row in probe.replay().active_leases):
                probe.release(lease_id, actor=ACTOR, outcome="failed")


def test_attempt_semantic_binding_rejects_resealed_intent_request_checkpoint_or_time_tamper(
    case: CheckpointCase, engine: TaskCheckpoint
) -> None:
    receipt_path = _assert_snapshot(case, engine, engine.plan(case.scope))
    attempt_path = case.destination / "attempt.json"
    original_attempt = attempt_path.read_bytes()
    original_receipt = receipt_path.read_bytes()
    phases = [
        json.loads(path.read_bytes())["phase"]
        for path in sorted((case.destination / "events").glob("*.json"))
    ]
    assert phases == [
        "ACQUIRED",
        "CAPTURED",
        "OBJECTS_WRITTEN",
        "REF_CREATED",
        "VERIFIED",
        "RELEASED",
    ]
    replacements = {
        "intent_id": "task-checkpoint-" + "0" * 32,
        "request_sha256": "0" * 64,
        "checkpoint_id": "synthetic-wrong-checkpoint",
        "created_at": "9999-01-01T00:00:00+00:00",
    }
    try:
        for field, replacement in replacements.items():
            attempt = json.loads(original_attempt)
            assert attempt[field] != replacement
            attempt[field] = replacement
            tampered_bytes = _json(attempt)
            attempt_path.write_bytes(tampered_bytes)
            receipt = json.loads(original_receipt)
            row = next(row for row in receipt["evidence"] if row["path"] == "attempt.json")
            row["sha256"] = _sha(tampered_bytes)
            row["size_bytes"] = len(tampered_bytes)
            receipt["receipt_sha256"] = _sha(
                _json({key: value for key, value in receipt.items() if key != "receipt_sha256"})
                + b"\n"
            )
            receipt_path.write_bytes(_json(receipt))
            assert receipt["evidence"] == engine._evidence_manifest(case.destination)
            with pytest.raises(TaskCheckpointError) as rejected:
                engine.validate(receipt_path)
            assert rejected.value.code == (
                "TASK_CHECKPOINT_RECOVERY_PRODUCER"
                if field == "created_at"
                else "TASK_CHECKPOINT_RECEIPT"
            )
    finally:
        # Only restore this adversarial synthetic fixture, never real evidence.
        attempt_path.write_bytes(original_attempt)
        receipt_path.write_bytes(original_receipt)
    assert engine.validate(receipt_path)["status"] == "PASS"
