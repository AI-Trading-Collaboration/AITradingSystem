"""Real OS-backed arbitration on pytest-owned stores, never production authority.

Spawned workers execute a finite test protocol and import this checkout's real
implementation. No backend is replaced by an in-memory lock. Parent deadlines
only prevent hangs; explicit process handshakes define every tested interleaving.
Legacy migration fixtures represent coordinator correlation data, not a live
publication authorization. The coordinator CLI's live fence remains separate.
"""

from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import stat
import sys
import traceback
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.platform.architecture import lease_arbiter
from ai_trading_system.platform.architecture.lease_arbiter import (
    hold_lease_arbiter,
    migrate_legacy_arbiter,
)
from ai_trading_system.platform.architecture.parallel_control import (
    ParallelControlError,
    parse_change_manifest,
)
from ai_trading_system.platform.architecture.parallel_control_kernel import (
    FileExecutionLeaseStore,
    TaskControlRecord,
    evaluate_task_readiness,
    load_parallel_control_policy,
    validate_dependency_graph,
)

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "config/architecture/arch_005_parallel_control_policy.yaml"
MODULE = ROOT / "src/ai_trading_system/platform/architecture/lease_arbiter.py"
NOW = datetime(2026, 7, 20, tzinfo=UTC)
BASE = "a" * 40
TASK_A = "ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE"
TASK_B = "TRADING-2446_to_2448_RESEARCH_RESTART_R0_R2"
ACTOR = "engineering-agent"
CRASH_EXIT = 23  # Test-only marker, not an operating policy or retry count.
WAIT_SECONDS = 30  # A hang guard; never a mechanism for arranging a race.


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _json(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")) + "\n"
    ).encode()


def _identity(path: Path) -> tuple[int, int]:
    info = path.stat()
    assert stat.S_ISREG(info.st_mode)
    assert not path.is_symlink()
    return info.st_dev, info.st_ino


def _task(task_id: str, change_id: str) -> TaskControlRecord:
    return TaskControlRecord(
        task_id=task_id,
        title=task_id,
        governance_status="READY",
        priority="P0",
        requirement_refs=("docs/requirements/synthetic-arbiter-test.md",),
        acceptance_criteria=("single writer",),
        manifest=parse_change_manifest(
            {
                "schema_version": "change_manifest.v1",
                "change_id": change_id,
                "task_id": task_id,
                "lane_role": "DOMAIN",
                "base_commit": BASE,
                "owner": "synthetic-test-owner",
                "production_effect": "none",
                "owned_paths": [f"outputs/test/{task_id}.json"],
                "shared_paths": [],
                "module_ids": ["synthetic.shared.module"],
                "contract_claims": [],
                "required_validation_tiers": ["focused"],
            }
        ),
    )


def _ready(task: TaskControlRecord) -> Any:
    policy = load_parallel_control_policy(POLICY)
    return evaluate_task_readiness(
        task,
        dependencies=[],
        observed_statuses={},
        graph_report=validate_dependency_graph([task.task_id], []),
        current_base_commit=BASE,
        policy=policy,
    )


def _send(connection: Connection, state: str, **fields: object) -> None:
    connection.send(
        {
            "state": state,
            "pid": os.getpid(),
            "platform": sys.platform,
            "implementation": str(Path(lease_arbiter.__file__).resolve()),
            **fields,
        }
    )


def _worker(
    connection: Connection,
    root: str,
    mode: str,
    actor: str,
    now_text: str,
    barrier: Any = None,
) -> None:
    """Only fixed synthetic HOLD / ACQUIRE_FIRST / ACQUIRE_SECOND scenarios."""
    try:
        assert Path(lease_arbiter.__file__).resolve() == MODULE
        now = datetime.fromisoformat(now_text)
        policy = load_parallel_control_policy(POLICY)
        if barrier is not None:
            barrier.wait(timeout=WAIT_SECONDS)
        if mode == "HOLD":
            with hold_lease_arbiter(
                Path(root),
                actor=actor,
                now=now,
                arbiter_ttl_seconds=policy.arbiter_ttl_seconds,
            ):
                _send(connection, "ENTERED")
                command = connection.recv()
                if command == "CRASH":
                    os._exit(CRASH_EXIT)
                assert command == "RELEASE"
            _send(connection, "EXITED")
        else:
            assert mode in {"ACQUIRE_FIRST", "ACQUIRE_SECOND"}
            first = mode == "ACQUIRE_FIRST"
            task = _task(TASK_A if first else TASK_B, "synthetic-a" if first else "synthetic-b")
            store = FileExecutionLeaseStore(Path(root), policy=policy)
            if first:
                original_append = store._append_event
                reported = False

                def held_append(event: Any) -> None:
                    nonlocal reported
                    if not reported:
                        reported = True
                        _send(connection, "WRITER_ENTERED")
                        assert connection.recv() == "CONTINUE"
                    original_append(event)

                # This pauses a real write while its real OS arbiter is held.
                # No acquisition result, backend, or journal event is fabricated.
                store._append_event = held_append  # type: ignore[method-assign]
            result = store.acquire(
                task=task,
                readiness=_ready(task),
                lane_id="domain-01" if first else "domain-02",
                actor=actor,
                current_base_commit=BASE,
                now=now,
            )
            _send(
                connection,
                "LEASE_RESULT",
                status=result.status,
                lease_id=result.lease.lease_id,
                reasons=list(result.reason_codes),
            )
    except ParallelControlError as exc:
        _send(
            connection,
            "DENIED",
            code=exc.code,
            message=exc.message,
            traceback="".join(traceback.format_exception(exc)),
        )
    except BaseException as exc:
        _send(
            connection,
            "ERROR",
            exception_type=type(exc).__name__,
            traceback="".join(traceback.format_exception(exc)),
        )
        raise
    finally:
        connection.close()


@dataclass
class Child:
    process: BaseProcess
    connection: Connection

    def read(self) -> dict[str, Any]:
        assert self.connection.poll(WAIT_SECONDS), f"child PID {self.process.pid} did not report"
        result = self.connection.recv()
        assert isinstance(result, dict)
        assert result["pid"] == self.process.pid
        assert Path(result["implementation"]) == MODULE
        assert result["platform"] == sys.platform
        assert result["state"] != "ERROR", result
        return result

    def finish(self) -> None:
        self.process.join(WAIT_SECONDS)
        assert self.process.exitcode == 0


@pytest.fixture
def children(request: pytest.FixtureRequest) -> Iterator[Callable[..., Child]]:
    context = multiprocessing.get_context("spawn")
    created: list[Child] = []
    request.node.user_properties.extend(
        [
            ("arbiter_test_profile", "SYNTHETIC_REAL_OS_MULTIPROCESS"),
            ("actual_platform", sys.platform),
            ("start_method", "spawn"),
        ]
    )

    def launch(
        root: Path,
        *,
        mode: str = "HOLD",
        actor: str = ACTOR,
        now: datetime = NOW,
        barrier: Any = None,
    ) -> Child:
        parent, child = context.Pipe()
        process = context.Process(
            target=_worker,
            args=(child, str(root), mode, actor, now.isoformat(), barrier),
        )
        process.start()
        child.close()
        item = Child(process, parent)
        created.append(item)
        return item

    try:
        yield launch
    finally:
        for child in created:
            if child.process.is_alive():
                try:
                    child.connection.send("RELEASE")
                except (BrokenPipeError, EOFError, OSError):
                    pass
                child.process.join(5)
            if child.process.is_alive():
                # Only this fixture's own synthetic child can be terminated.
                child.process.terminate()
                child.process.join(5)
            child.connection.close()
            child.process.close()


def _release(child: Child) -> None:
    child.connection.send("RELEASE")
    assert child.read()["state"] == "EXITED"
    child.finish()


def test_anchor_is_a_stable_regular_file_across_acquisitions(tmp_path: Path) -> None:
    root = tmp_path / "store"
    policy = load_parallel_control_policy(POLICY)
    observed = []
    tokens = []
    for actor in ("engineering-agent", "research-agent"):
        with hold_lease_arbiter(
            root, actor=actor, now=NOW, arbiter_ttl_seconds=policy.arbiter_ttl_seconds
        ):
            observed.append(_identity(root / "arbiter.lock"))
            owner = json.loads((root / "arbiter.owner.json").read_bytes())
            assert owner["schema_version"] == "execution_lease_os_arbiter_owner.v2"
            assert owner["state"] == "ACTIVE"
            assert owner["pid"] == os.getpid()
            assert owner["actor"] == actor
            tokens.append(owner["acquisition_id"])
        assert json.loads((root / "arbiter.owner.json").read_bytes())["state"] == "RELEASED"
    assert observed[0] == observed[1]
    assert tokens[0] != tokens[1]


def test_two_spawned_contenders_have_exactly_one_real_entrant(
    tmp_path: Path,
    children: Callable[..., Child],
) -> None:
    barrier = multiprocessing.get_context("spawn").Barrier(3)
    candidates = [
        children(tmp_path / "store", actor=actor, barrier=barrier)
        for actor in ("engineering-agent", "research-agent")
    ]
    barrier.wait(timeout=WAIT_SECONDS)
    reports = [child.read() for child in candidates]
    assert sorted(report["state"] for report in reports) == ["DENIED", "ENTERED"]
    assert len({report["pid"] for report in reports}) == 2
    for child, report in zip(candidates, reports, strict=True):
        if report["state"] == "ENTERED":
            _release(child)
        else:
            assert report["code"] == "LEASE_ARBITER_BUSY"
            child.finish()


def test_live_holder_cannot_be_stolen_after_its_diagnostic_ttl(
    tmp_path: Path,
    children: Callable[..., Child],
) -> None:
    root = tmp_path / "store"
    first = children(root)
    assert first.read()["state"] == "ENTERED"
    anchor = _identity(root / "arbiter.lock")
    later = NOW + timedelta(seconds=load_parallel_control_policy(POLICY).arbiter_ttl_seconds + 1)
    contender = children(root, actor="research-agent", now=later)
    denied = contender.read()
    assert denied["state"] == "DENIED" and denied["code"] == "LEASE_ARBITER_BUSY"
    assert first.process.is_alive()
    contender.finish()
    _release(first)
    successor = children(root, actor="research-agent", now=later)
    assert successor.read()["state"] == "ENTERED"
    assert _identity(root / "arbiter.lock") == anchor
    _release(successor)


def test_stale_released_metadata_cannot_replace_a_live_os_owner(
    tmp_path: Path,
    children: Callable[..., Child],
) -> None:
    root = tmp_path / "store"
    initial = children(root)
    assert initial.read()["state"] == "ENTERED"
    _release(initial)
    metadata = root / "arbiter.owner.json"
    stale = metadata.read_bytes()
    holder = children(root)
    assert holder.read()["state"] == "ENTERED"
    active = metadata.read_bytes()
    # Deliberately replay a genuine old generation's diagnostic bytes. This is
    # synthetic metadata poisoning, never a mock of the real OS mutex.
    metadata.write_bytes(stale)
    try:
        contender = children(root, actor="research-agent")
        denied = contender.read()
        assert denied["state"] == "DENIED" and denied["code"] == "LEASE_ARBITER_BUSY"
        contender.finish()
        assert holder.process.is_alive()
    finally:
        metadata.write_bytes(active)
    _release(holder)


def test_crash_releases_os_mutex_without_releasing_business_lease(
    tmp_path: Path,
    children: Callable[..., Child],
) -> None:
    root = tmp_path / "store"
    policy = load_parallel_control_policy(POLICY)
    store = FileExecutionLeaseStore(root, policy=policy)
    task = _task(TASK_A, "synthetic-business-lease")
    acquired = store.acquire(
        task=task,
        readiness=_ready(task),
        lane_id="domain-01",
        actor=ACTOR,
        current_base_commit=BASE,
        now=NOW,
    )
    before = {
        path.relative_to(store.events_root): path.read_bytes()
        for path in store.events_root.glob("*/*.json")
    }
    holder = children(root)
    assert holder.read()["state"] == "ENTERED"
    anchor = _identity(root / "arbiter.lock")
    holder.connection.send("CRASH")
    holder.process.join(WAIT_SECONDS)
    assert holder.process.exitcode == CRASH_EXIT
    replacement = children(root, actor="research-agent")
    assert replacement.read()["state"] == "ENTERED"
    assert _identity(root / "arbiter.lock") == anchor
    _release(replacement)
    replay = store.replay()
    assert replay.status == "PASS"
    assert [row.lease_id for row in replay.active_leases] == [acquired.lease.lease_id]
    assert {
        path.relative_to(store.events_root): path.read_bytes()
        for path in store.events_root.glob("*/*.json")
    } == before


def test_actual_acquire_allows_one_writer_then_replays_the_conflict(
    tmp_path: Path,
    children: Callable[..., Child],
) -> None:
    root = tmp_path / "store"
    first = children(root, mode="ACQUIRE_FIRST")
    assert first.read()["state"] == "WRITER_ENTERED"
    second = children(root, mode="ACQUIRE_SECOND", actor="research-agent")
    denied = second.read()
    assert denied["state"] == "DENIED" and denied["code"] == "LEASE_ARBITER_BUSY"
    second.finish()
    assert list((root / "events").glob("*/*.json")) == []
    first.connection.send("CONTINUE")
    granted = first.read()
    assert granted["state"] == "LEASE_RESULT" and granted["status"] == "ACTIVE"
    first.finish()
    # One explicit post-critical-section request, not a retry-until-green loop.
    conflicting = children(root, mode="ACQUIRE_SECOND", actor="research-agent")
    rejected = conflicting.read()
    assert rejected["state"] == "LEASE_RESULT" and rejected["status"] == "BLOCKED"
    assert any(reason.startswith("LEASE_RESOURCE_CONFLICT:") for reason in rejected["reasons"])
    conflicting.finish()
    replay = FileExecutionLeaseStore(root, policy=load_parallel_control_policy(POLICY)).replay()
    assert replay.status == "PASS"
    assert replay.event_count == 4
    assert [row.lease_id for row in replay.active_leases] == [granted["lease_id"]]


@pytest.mark.parametrize("wrong_owner", ["token", "thread", "pid"])
def test_wrong_capability_owner_cannot_unlock_another_holder(
    tmp_path: Path,
    children: Callable[..., Child],
    monkeypatch: pytest.MonkeyPatch,
    wrong_owner: str,
) -> None:
    root = tmp_path / "store"
    root.mkdir()
    held = lease_arbiter._acquire_lock(root / "arbiter.lock")
    try:
        if wrong_owner == "thread":
            with ThreadPoolExecutor(max_workers=1) as pool:
                failed = pool.submit(held.release, owner_token=held.token)
                with pytest.raises(ParallelControlError, match="LEASE_ARBITER_OWNER_MISMATCH"):
                    failed.result()
        elif wrong_owner == "pid":
            with monkeypatch.context() as pid_patch:
                actual_pid = os.getpid()
                pid_patch.setattr(lease_arbiter.os, "getpid", lambda: actual_pid + 1)
                with pytest.raises(ParallelControlError, match="LEASE_ARBITER_OWNER_MISMATCH"):
                    held.release(owner_token=held.token)
        else:
            with pytest.raises(ParallelControlError, match="LEASE_ARBITER_OWNER_MISMATCH"):
                held.release(owner_token="wrong-synthetic-token")
        contender = children(root, actor="research-agent")
        denied = contender.read()
        assert denied["state"] == "DENIED" and denied["code"] == "LEASE_ARBITER_BUSY"
        contender.finish()
    finally:
        held.release(owner_token=held.token)
    successor = children(root)
    assert successor.read()["state"] == "ENTERED"
    _release(successor)


def test_business_lease_wrong_actor_is_rejected_but_coordinator_exception_is_preserved(
    tmp_path: Path,
) -> None:
    store = FileExecutionLeaseStore(tmp_path / "store", policy=load_parallel_control_policy(POLICY))
    task = _task(TASK_A, "synthetic-owner")
    result = store.acquire(
        task=task,
        readiness=_ready(task),
        lane_id="domain-01",
        actor=ACTOR,
        current_base_commit=BASE,
        now=NOW,
    )
    before = store.replay().event_count
    with pytest.raises(ParallelControlError, match="LEASE_ACTOR_MISMATCH"):
        store.release(result.lease.lease_id, actor="research-agent", now=NOW, evidence_refs=())
    assert store.replay().event_count == before
    released = store.release(
        result.lease.lease_id, actor="integration-coordinator", now=NOW, evidence_refs=()
    )
    assert released.state == "RELEASED"
    assert store.replay().status == "PASS"


def _legacy(root: Path) -> bytes:
    directory = root / "arbiter.lock"
    directory.mkdir(parents=True)
    content = _json(
        {
            "schema_version": "execution_lease_arbiter.v1",
            "state": "RELEASED",
            "actor": ACTOR,
            "acquired_at": NOW.isoformat(),
            "expires_at": NOW.isoformat(),
            "production_effect": "none",
        }
    )
    (directory / "owner.json").write_bytes(content)
    return content


def _migration_arguments(root: Path, owner: bytes) -> dict[str, Any]:
    owner_ref = "owner_instruction:SYNTHETIC_TEST_ONLY:no-live-publication-authority"
    quiescence = {
        "schema_version": "lease_arbiter_quiescence.v1",
        "store_root": root.resolve().as_posix(),
        "actor": "integration-coordinator",
        "owner_instruction_ref": owner_ref,
        "publication_transaction_id": "synthetic-correlation-only",
        "publication_transaction_sha256": "1" * 64,
        "observed_owner_sha256": _sha(owner),
        "checked_at": NOW.isoformat(),
        "assertion": "ALL_LEGACY_ARBITER_USERS_STOPPED_AND_NO_IN_FLIGHT_READERS",
        "production_effect": "none",
        "broker_action": "none",
    }
    path = root.parent / "synthetic-quiescence.json"
    path.write_bytes(_json(quiescence))
    return {
        "migration_id": "synthetic-migration-v1",
        "actor": "integration-coordinator",
        "now": NOW,
        "expected_owner_sha256": _sha(owner),
        "owner_instruction_ref": owner_ref,
        "quiescence_receipt_path": path,
        "quiescence_receipt_sha256": _sha(path.read_bytes()),
    }


def test_legacy_directory_requires_explicit_migration_without_changing_its_bytes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "store"
    owner = _legacy(root)
    with pytest.raises(ParallelControlError, match="LEASE_ARBITER_MIGRATION_REQUIRED"):
        with hold_lease_arbiter(root, actor=ACTOR, now=NOW, arbiter_ttl_seconds=30):
            pytest.fail("legacy directory must not be silently taken over")
    assert (root / "arbiter.lock/owner.json").read_bytes() == owner
    assert not (root / "arbiter-migrations").exists()


def test_explicit_synthetic_migration_preserves_owner_and_business_event_bytes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "store"
    owner = _legacy(root)
    event = root / "events/synthetic-retained.json"
    event.parent.mkdir()
    event.write_bytes(b"synthetic retained bytes; not a real lease event\r\n")
    original_event = event.read_bytes()
    result = migrate_legacy_arbiter(root, **_migration_arguments(root, owner))
    assert result["schema_version"] == "lease_arbiter_migration_receipt.v1"
    assert result["status"] == "PASS"
    assert result["safety"] == {
        "production_effect": "none",
        "broker_action": "none",
        "business_lease_events_mutated": False,
    }
    archive = root / "arbiter-migrations/synthetic-migration-v1/legacy/owner.json"
    assert archive.read_bytes() == owner
    assert event.read_bytes() == original_event
    anchor = _identity(root / "arbiter.lock")
    with hold_lease_arbiter(root, actor=ACTOR, now=NOW, arbiter_ttl_seconds=30):
        assert _identity(root / "arbiter.lock") == anchor
    with pytest.raises((NotADirectoryError, FileNotFoundError)):
        (root / "arbiter.lock/owner.json").read_bytes()


@pytest.mark.parametrize(
    "tamper", ["owner_sha", "quiescence_sha", "root", "actor", "assertion", "extra"]
)
def test_migration_requires_exact_quiescent_correlation_before_mutation(
    tmp_path: Path,
    tamper: str,
) -> None:
    root = tmp_path / "store"
    owner = _legacy(root)
    arguments = _migration_arguments(root, owner)
    if tamper == "owner_sha":
        arguments["expected_owner_sha256"] = "0" * 64
    elif tamper == "quiescence_sha":
        arguments["quiescence_receipt_sha256"] = "0" * 64
    else:
        path = arguments["quiescence_receipt_path"]
        value = json.loads(path.read_bytes())
        if tamper == "root":
            value["store_root"] = str(tmp_path / "wrong-root")
        elif tamper == "actor":
            value["actor"] = "research-agent"
        elif tamper == "assertion":
            value["assertion"] = "PARTIAL_OBSERVATION_ONLY"
        else:
            value["allow_live_migration"] = True
        path.write_bytes(_json(value))
        arguments["quiescence_receipt_sha256"] = _sha(path.read_bytes())
    with pytest.raises(ParallelControlError, match="LEASE_ARBITER_MIGRATION_BINDING_INVALID"):
        migrate_legacy_arbiter(root, **arguments)
    assert (root / "arbiter.lock/owner.json").read_bytes() == owner


@pytest.mark.parametrize("operation", ["set_inheritable", "fstat"])
def test_opened_handle_is_closed_after_setup_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    anchor = tmp_path / "arbiter.lock"
    opened: list[int] = []
    actual_open = os.open
    actual_fstat = os.fstat
    actual_operation = getattr(os, operation)

    def tracked_open(path: Any, flags: int, mode: int = 0o777, **kwargs: Any) -> int:
        descriptor = actual_open(path, flags, mode, **kwargs)
        if Path(path) == anchor:
            opened.append(descriptor)
        return descriptor

    def failed_setup(descriptor: int, *args: Any) -> Any:
        if descriptor in opened:
            raise OSError("synthetic descriptor setup failure")
        return actual_operation(descriptor, *args)

    with monkeypatch.context() as fault:
        fault.setattr(lease_arbiter.os, "open", tracked_open)
        fault.setattr(lease_arbiter.os, operation, failed_setup)
        with pytest.raises(ParallelControlError, match="LEASE_ARBITER_LOCK_IO_ERROR"):
            lease_arbiter._acquire_lock(anchor)
    assert len(opened) == 1
    with pytest.raises(OSError):
        actual_fstat(opened[0])
    # The fstat case fails after the real OS lock is acquired. A separate real
    # open must succeed after failure; the backend is not replaced in this test.
    successor = lease_arbiter._acquire_lock(anchor)
    successor.release(owner_token=successor.token)


def test_release_does_not_overwrite_another_metadata_generation(
    tmp_path: Path,
) -> None:
    root = tmp_path / "store"
    metadata = root / "arbiter.owner.json"
    foreign: bytes | None = None
    with pytest.raises(ParallelControlError, match="LEASE_ARBITER_OWNER_MISMATCH"):
        with hold_lease_arbiter(root, actor=ACTOR, now=NOW, arbiter_ttl_seconds=30):
            value = json.loads(metadata.read_bytes())
            value["acquisition_id"] = "f" * 32
            foreign = _json(value)
            metadata.write_bytes(foreign)
    assert metadata.read_bytes() == foreign
    # The failed release must close only its own handle. A valid ACTIVE sidecar
    # with a free OS lock is crash residue, not a live-lock takeover permission.
    with hold_lease_arbiter(root, actor=ACTOR, now=NOW, arbiter_ttl_seconds=30):
        assert json.loads(metadata.read_bytes())["acquisition_id"] != "f" * 32


def test_migration_parses_the_same_quiescence_bytes_that_were_hashed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "store"
    owner = _legacy(root)
    arguments = _migration_arguments(root, owner)
    locator = arguments["quiescence_receipt_path"]
    actual_read = lease_arbiter._read_regular
    captured_reads: list[bytes] = []

    def one_capture(path: Path) -> bytes:
        if path == locator:
            assert not captured_reads, "external quiescence must be parsed from captured bytes"
            captured_reads.append(actual_read(path))
            return captured_reads[0]
        return actual_read(path)

    monkeypatch.setattr(lease_arbiter, "_read_regular", one_capture)
    result = migrate_legacy_arbiter(root, **arguments)
    assert result["status"] == "PASS"
    assert len(captured_reads) == 1
    frozen = root / "arbiter-migrations/synthetic-migration-v1/quiescence.json"
    assert frozen.read_bytes() == captured_reads[0]
    # Normal arbitration replays its frozen evidence, never follows a changed
    # external locator or silently turns that locator into current authority.
    locator.write_bytes(b"external locator changed after the accepted capture\n")
    with hold_lease_arbiter(root, actor=ACTOR, now=NOW, arbiter_ttl_seconds=30):
        pass
    assert len(captured_reads) == 1


def test_migration_cannot_adopt_an_anchor_created_after_legacy_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "store"
    owner = _legacy(root)
    arguments = _migration_arguments(root, owner)
    anchor = root / "arbiter.lock"
    actual_rename = Path.rename
    competed: list[tuple[int, int]] = []

    def competing_creator(path: Path, target: Any) -> Path:
        result = actual_rename(path, target)
        if path == anchor:
            # A deterministic filesystem interleaving, not a fake OS lock.
            # Empty bytes are intentional: the old nonexclusive open adopted it.
            with anchor.open("xb"):
                pass
            competed.append(_identity(anchor))
        return result

    monkeypatch.setattr(Path, "rename", competing_creator)
    with pytest.raises(ParallelControlError, match="LEASE_ARBITER_MIGRATION_PARTIAL"):
        migrate_legacy_arbiter(root, **arguments)
    run = root / "arbiter-migrations/synthetic-migration-v1"
    assert competed == [_identity(anchor)]
    assert anchor.read_bytes() == b""
    assert not (root / "arbiter.owner.json").exists()
    assert (run / "legacy/owner.json").read_bytes() == owner
    assert (run / "failure.json").is_file()
    assert not (run / "receipt.json").exists()
    with pytest.raises(ParallelControlError, match="LEASE_ARBITER_MIGRATION_PARTIAL"):
        with hold_lease_arbiter(root, actor=ACTOR, now=NOW, arbiter_ttl_seconds=30):
            pytest.fail("a competing anchor cannot erase the partial-migration barrier")


@pytest.mark.parametrize("failure_stage", ["receipt_write", "postwrite_check"])
def test_partial_migration_is_retained_and_cannot_be_replayed_as_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str,
) -> None:
    root = tmp_path / "store"
    owner = _legacy(root)
    arguments = _migration_arguments(root, owner)
    run = root / "arbiter-migrations/synthetic-migration-v1"
    receipt = run / "receipt.json"
    actual_write = lease_arbiter._exclusive_json
    actual_check = lease_arbiter._completed_migrations

    def failed_write(path: Path, value: object) -> None:
        if path == receipt:
            raise PermissionError("synthetic receipt write failure")
        actual_write(path, value)

    def failed_check(path: Path) -> None:
        actual_check(path)
        if receipt.exists():
            raise ParallelControlError(
                "LEASE_ARBITER_MIGRATION_PARTIAL", "synthetic post-receipt check failure"
            )

    with monkeypatch.context() as fault:
        if failure_stage == "receipt_write":
            fault.setattr(lease_arbiter, "_exclusive_json", failed_write)
        else:
            fault.setattr(lease_arbiter, "_completed_migrations", failed_check)
        with pytest.raises(ParallelControlError, match="LEASE_ARBITER_MIGRATION_PARTIAL"):
            migrate_legacy_arbiter(root, **arguments)
    assert (run / "legacy/owner.json").read_bytes() == owner
    assert (run / "failure.json").is_file()
    assert receipt.exists() is (failure_stage == "postwrite_check")
    retained = {
        path.relative_to(run): path.read_bytes() for path in run.rglob("*") if path.is_file()
    }
    with pytest.raises(ParallelControlError, match="LEASE_ARBITER_MIGRATION_PARTIAL"):
        migrate_legacy_arbiter(root, **arguments)
    with pytest.raises(ParallelControlError, match="LEASE_ARBITER_MIGRATION_PARTIAL"):
        with hold_lease_arbiter(root, actor=ACTOR, now=NOW, arbiter_ttl_seconds=30):
            pytest.fail("retained PASS-shaped bytes must not erase failure terminal state")
    assert {
        path.relative_to(run): path.read_bytes() for path in run.rglob("*") if path.is_file()
    } == retained
    # Private handle cleanup check only: this does not bypass the normal gate.
    held = lease_arbiter._acquire_lock(root / "arbiter.lock")
    held.release(owner_token=held.token)


def test_successful_migration_is_idempotent_only_for_identical_request(
    tmp_path: Path,
) -> None:
    root = tmp_path / "store"
    owner = _legacy(root)
    arguments = _migration_arguments(root, owner)
    first = migrate_legacy_arbiter(root, **arguments)
    run = root / "arbiter-migrations/synthetic-migration-v1"
    retained = {
        path.relative_to(run): path.read_bytes() for path in run.rglob("*") if path.is_file()
    }
    anchor = _identity(root / "arbiter.lock")
    assert migrate_legacy_arbiter(root, **arguments) == first
    assert _identity(root / "arbiter.lock") == anchor
    assert {
        path.relative_to(run): path.read_bytes() for path in run.rglob("*") if path.is_file()
    } == retained
    locator = arguments["quiescence_receipt_path"]
    value = json.loads(locator.read_bytes())
    value["publication_transaction_id"] = "different-synthetic-correlation"
    locator.write_bytes(_json(value))
    arguments["quiescence_receipt_sha256"] = _sha(locator.read_bytes())
    with pytest.raises(ParallelControlError, match="LEASE_ARBITER_MIGRATION_BINDING_INVALID"):
        migrate_legacy_arbiter(root, **arguments)
    assert {
        path.relative_to(run): path.read_bytes() for path in run.rglob("*") if path.is_file()
    } == retained


@pytest.mark.parametrize("tamper", ["receipt", "archive_resealed", "request", "quiescence"])
def test_completed_migration_rebinds_frozen_evidence_before_any_new_owner_write(
    tmp_path: Path,
    tamper: str,
) -> None:
    root = tmp_path / "store"
    owner = _legacy(root)
    migrate_legacy_arbiter(root, **_migration_arguments(root, owner))
    run = root / "arbiter-migrations/synthetic-migration-v1"
    if tamper == "receipt":
        path = run / "receipt.json"
        value = json.loads(path.read_bytes())
        value["actor"] = "research-agent"
        path.write_bytes(_json(value))
    elif tamper == "archive_resealed":
        path = run / "legacy/owner.json"
        value = json.loads(path.read_bytes())
        value["actor"] = "research-agent"
        path.write_bytes(_json(value))
        receipt_path = run / "receipt.json"
        receipt = json.loads(receipt_path.read_bytes())
        receipt["legacy_owner"]["sha256"] = _sha(path.read_bytes())
        receipt.pop("receipt_sha256")
        receipt["receipt_sha256"] = _sha(_json(receipt))
        receipt_path.write_bytes(_json(receipt))
    elif tamper == "request":
        path = run / "request.json"
        value = json.loads(path.read_bytes())
        value["expected_owner_sha256"] = "0" * 64
        path.write_bytes(_json(value))
    else:
        path = run / "quiescence.json"
        value = json.loads(path.read_bytes())
        value["assertion"] = "SYNTHETIC_TAMPER"
        path.write_bytes(_json(value))
    metadata = root / "arbiter.owner.json"
    before = metadata.read_bytes()
    with pytest.raises(ParallelControlError, match="LEASE_ARBITER_MIGRATION_PARTIAL"):
        with hold_lease_arbiter(root, actor=ACTOR, now=NOW, arbiter_ttl_seconds=30):
            pytest.fail("self-consistent receipt hashes cannot replace the frozen source binding")
    assert metadata.read_bytes() == before
