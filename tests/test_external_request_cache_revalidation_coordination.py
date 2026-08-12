from __future__ import annotations

import errno
import json
import multiprocessing
import os
import socket
import time
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from queue import Empty
from typing import Any, Literal

import pytest

import ai_trading_system.external_request_cache_revalidation_coordination as coordination
from ai_trading_system.external_request_cache_revalidation_coordination import (
    ExternalRequestRevalidationCoordinator,
    RevalidationCoordinationError,
    RevalidationCoordinationIntegrityError,
    RevalidationCoordinationTimeout,
    RevalidationProbe,
    load_revalidation_coordination_policy,
)
from ai_trading_system.platform.artifacts import write_json_atomic

POLICY_PATH = (
    Path(__file__).parents[1]
    / "config/data/external_request_cache_revalidation_coordination_policy.yaml"
)
KEY_A = "a" * 64
KEY_B = "b" * 64
BODY_OLD = "1" * 64
BODY_NEW = "2" * 64
SPAWN_ORCHESTRATION_TIMEOUT_SECONDS = 30.0
SPAWN_QUEUE_POLL_SECONDS = 0.05
SPAWN_CLEANUP_TIMEOUT_SECONDS = 2.0


@dataclass(frozen=True)
class _SpawnProbeRecord:
    cache_key: str
    pid: int


@dataclass(frozen=True)
class _SpawnWorkerResult:
    pid: int
    outcome: Literal["PASS", "FAIL"]
    coordination_status: str | None
    value: str | None
    error_type: str | None
    error_message: str | None


@dataclass(frozen=True)
class _SpawnRun:
    worker_pids: tuple[int, ...]
    results: tuple[_SpawnWorkerResult, ...]
    call_dir: Path


def _read_probe(state_path: Path) -> RevalidationProbe[str]:
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    if payload["status"] == "REUSABLE":
        return RevalidationProbe(
            status="REUSABLE",
            generation_id=payload["generation_id"],
            body_sha256=payload["body_sha256"],
            reason_code="HIT",
            value="cached",
        )
    return RevalidationProbe(
        status="NEEDS_REVALIDATION",
        generation_id=payload["generation_id"],
        body_sha256=payload["body_sha256"],
        reason_code=payload["reason_code"],
    )


def _multiprocess_execute_worker(
    request_dir: str,
    cache_key: str,
    state_path: str,
    call_dir: str,
    first_probe_queue: Any,
    release_first_probe: Any,
    result_queue: Any,
    network_delay_seconds: float,
    published_status: str,
) -> None:
    path = Path(state_path)
    first_probe = True

    def probe() -> RevalidationProbe[str]:
        nonlocal first_probe
        observed = _read_probe(path)
        if first_probe:
            first_probe = False
            first_probe_queue.put(
                _SpawnProbeRecord(cache_key=cache_key, pid=os.getpid())
            )
            if not release_first_probe.wait(timeout=10):
                raise RuntimeError("test first-probe barrier timed out")
        return observed

    def fetch() -> str:
        started = time.time()
        time.sleep(network_delay_seconds)
        ended = time.time()
        write_json_atomic(
            Path(call_dir) / f"{cache_key[:8]}-{os.getpid()}.json",
            {"cache_key": cache_key, "started": started, "ended": ended},
        )
        return "network"

    def publish(_value: str) -> None:
        write_json_atomic(
            path,
            {
                "status": published_status,
                "generation_id": f"published-{cache_key[:8]}-{os.getpid()}",
                "body_sha256": BODY_NEW,
                "reason_code": ("HIT" if published_status == "REUSABLE" else "EXPIRED_REVALIDATE"),
            },
        )

    try:
        result = ExternalRequestRevalidationCoordinator(
            Path(request_dir), cache_key=cache_key
        ).execute(probe=probe, fetch=fetch, publish=publish)
        result_queue.put(
            _SpawnWorkerResult(
                pid=os.getpid(),
                outcome="PASS",
                coordination_status=result.status,
                value=result.value,
                error_type=None,
                error_message=None,
            )
        )
    except BaseException as exc:  # pragma: no cover - surfaced in parent process
        result_queue.put(
            _SpawnWorkerResult(
                pid=os.getpid(),
                outcome="FAIL",
                coordination_status=None,
                value=None,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
        )


def _multiprocess_early_exit_worker(*_args: Any) -> None:
    os._exit(23)


def _multiprocess_clean_exit_before_probe_worker(*_args: Any) -> None:
    return


def _multiprocess_exit_before_result_worker(
    _request_dir: str,
    cache_key: str,
    _state_path: str,
    _call_dir: str,
    first_probe_queue: Any,
    release_first_probe: Any,
    _result_queue: Any,
    _network_delay_seconds: float,
    _published_status: str,
) -> None:
    first_probe_queue.put(
        _SpawnProbeRecord(cache_key=cache_key, pid=os.getpid())
    )
    if not release_first_probe.wait(timeout=10):
        os._exit(32)
    os._exit(31)


def _multiprocess_stall_before_probe_worker(*_args: Any) -> None:
    time.sleep(60)


def _remaining_or_raise(deadline: float, *, phase: str) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise AssertionError(
            f"spawn harness orchestration deadline elapsed during {phase}"
        )
    return remaining


def _get_record_until_deadline(
    queue: Any,
    *,
    deadline: float,
    phase: str,
    workers: list[Any],
    terminal_pids: set[int] | None = None,
) -> Any:
    while True:
        remaining = _remaining_or_raise(deadline, phase=phase)
        try:
            return queue.get(timeout=min(SPAWN_QUEUE_POLL_SECONDS, remaining))
        except Empty:
            for worker in workers:
                pid = worker.pid
                if (
                    worker.exitcode is not None
                    and (terminal_pids is None or pid not in terminal_pids)
                ):
                    raise AssertionError(
                        f"spawn harness child PID {pid} exited with code "
                        f"{worker.exitcode} during {phase} before required record"
                    ) from None


def _join_workers_until_deadline(
    workers: list[Any],
    *,
    deadline: float,
) -> None:
    phase = "normal child join"
    while any(worker.is_alive() for worker in workers):
        remaining = _remaining_or_raise(deadline, phase=phase)
        for worker in workers:
            worker.join(timeout=0)
        time.sleep(min(SPAWN_QUEUE_POLL_SECONDS, remaining))
    nonzero = [
        (worker.pid, worker.exitcode)
        for worker in workers
        if worker.exitcode != 0
    ]
    if nonzero:
        raise AssertionError(f"spawn harness child exit failure during {phase}: {nonzero}")


def _cleanup_spawn_resources(
    *,
    workers: list[Any],
    release_first_probe: Any | None,
    queues: list[Any],
) -> list[str]:
    errors: list[str] = []
    if release_first_probe is not None:
        release_first_probe.set()

    for worker in workers:
        if worker.is_alive():
            worker.terminate()
    cleanup_deadline = time.monotonic() + SPAWN_CLEANUP_TIMEOUT_SECONDS
    for worker in workers:
        worker.join(timeout=max(0.0, cleanup_deadline - time.monotonic()))
    for worker in workers:
        if worker.is_alive():
            worker.kill()
    for worker in workers:
        worker.join(timeout=max(0.0, cleanup_deadline - time.monotonic()))
        if worker.is_alive():
            errors.append(f"child PID {worker.pid} remained alive after kill")

    for queue in queues:
        try:
            queue.close()
            queue.join_thread()
        except (OSError, ValueError) as exc:
            errors.append(f"queue cleanup failed: {type(exc).__name__}: {exc}")
    return errors


def _run_spawn_workers(
    tmp_path: Path,
    specifications: list[tuple[str, str]],
    *,
    network_delay_seconds: float,
    published_status: str = "REUSABLE",
    orchestration_timeout_seconds: float = SPAWN_ORCHESTRATION_TIMEOUT_SECONDS,
    worker_target: Callable[..., None] = _multiprocess_execute_worker,
) -> _SpawnRun:
    request_keys: dict[str, str] = {}
    for request_name, cache_key in specifications:
        existing_key = request_keys.setdefault(request_name, cache_key)
        if existing_key != cache_key:
            raise ValueError("spawn fixture request_name must map to exactly one cache_key")

    for request_name in request_keys:
        write_json_atomic(
            tmp_path / f"{request_name}.json",
            {
                "status": "NEEDS_REVALIDATION",
                "generation_id": "expired-generation",
                "body_sha256": BODY_OLD,
                "reason_code": "EXPIRED_REVALIDATE",
            },
        )

    call_dir = tmp_path / "calls"
    call_dir.mkdir()
    context = multiprocessing.get_context("spawn")
    first_probe_queue = context.Queue()
    result_queue = context.Queue()
    release_first_probe = context.Event()
    workers: list[Any] = []
    primary_error: BaseException | None = None
    output: _SpawnRun | None = None
    deadline = time.monotonic() + orchestration_timeout_seconds

    try:
        for request_name, cache_key in specifications:
            request_dir = tmp_path / request_name
            state_path = tmp_path / f"{request_name}.json"
            worker = context.Process(
                target=worker_target,
                args=(
                    str(request_dir),
                    cache_key,
                    str(state_path),
                    str(call_dir),
                    first_probe_queue,
                    release_first_probe,
                    result_queue,
                    network_delay_seconds,
                    published_status,
                ),
            )
            worker.start()
            workers.append(worker)

        worker_pids = tuple(worker.pid for worker in workers)
        if any(pid is None for pid in worker_pids):
            raise AssertionError("spawn harness worker PID missing after start")
        expected_pids = {int(pid) for pid in worker_pids}

        probe_records: dict[int, _SpawnProbeRecord] = {}
        for _ in workers:
            record = _get_record_until_deadline(
                first_probe_queue,
                deadline=deadline,
                phase="first-probe barrier",
                workers=workers,
            )
            if not isinstance(record, _SpawnProbeRecord):
                raise AssertionError(
                    f"spawn harness first-probe record has invalid type: {type(record).__name__}"
                )
            if record.pid not in expected_pids:
                raise AssertionError(
                    f"spawn harness first-probe record has unknown PID {record.pid}"
                )
            if record.pid in probe_records:
                raise AssertionError(
                    f"spawn harness first-probe record duplicated PID {record.pid}"
                )
            probe_records[record.pid] = record
        release_first_probe.set()

        results_by_pid: dict[int, _SpawnWorkerResult] = {}
        for _ in workers:
            result = _get_record_until_deadline(
                result_queue,
                deadline=deadline,
                phase="terminal result collection",
                workers=workers,
                terminal_pids=set(results_by_pid),
            )
            if not isinstance(result, _SpawnWorkerResult):
                raise AssertionError(
                    f"spawn harness terminal result has invalid type: {type(result).__name__}"
                )
            if result.pid not in expected_pids:
                raise AssertionError(
                    f"spawn harness terminal result has unknown PID {result.pid}"
                )
            if result.pid in results_by_pid:
                raise AssertionError(
                    f"spawn harness terminal result duplicated PID {result.pid}"
                )
            results_by_pid[result.pid] = result

        _join_workers_until_deadline(workers, deadline=deadline)
        ordered_pids = tuple(int(pid) for pid in worker_pids)
        output = _SpawnRun(
            worker_pids=ordered_pids,
            results=tuple(results_by_pid[pid] for pid in ordered_pids),
            call_dir=call_dir,
        )
    except BaseException as exc:
        primary_error = exc

    cleanup_errors = _cleanup_spawn_resources(
        workers=workers,
        release_first_probe=release_first_probe,
        queues=[first_probe_queue, result_queue],
    )
    if primary_error is not None:
        if cleanup_errors:
            primary_error.add_note("; ".join(cleanup_errors))
        raise primary_error
    if cleanup_errors:
        raise AssertionError("; ".join(cleanup_errors))
    if output is None:
        raise AssertionError("spawn harness completed without output")
    return output


def test_reviewed_policy_has_bounded_fail_closed_parameters() -> None:
    policy = load_revalidation_coordination_policy(POLICY_PATH)

    assert policy.status == "reviewed_pilot_baseline"
    assert policy.waiter_timeout_seconds > policy.lease_ttl_seconds
    assert policy.max_stale_takeovers_per_cause == 1
    assert policy.allow_waiter_retry_after_owner_failure is False
    assert policy.initial_poll_interval_milliseconds <= policy.maximum_poll_interval_milliseconds


def test_per_key_lock_smoke_acquire_complete_and_redacted_replay(tmp_path: Path) -> None:
    coordinator = ExternalRequestRevalidationCoordinator(tmp_path / "request", cache_key=KEY_A)
    probe = RevalidationProbe[str](
        status="NEEDS_REVALIDATION",
        generation_id="expired-generation",
        body_sha256=BODY_OLD,
        reason_code="EXPIRED_REVALIDATE",
    )

    acquired = coordinator.acquire(probe)
    assert acquired.status == "OWNER"
    assert acquired.lease is not None
    coordinator.complete(acquired.lease, outcome="OWNER_FAILURE")

    replay = coordinator.replay()
    serialized = json.dumps(replay.to_dict(), sort_keys=True)
    assert replay.status == "PASS"
    assert replay.event_count == 2
    assert coordinator.arbiter_path.read_bytes().startswith(b"\0")
    assert acquired.lease.owner_token not in serialized
    assert socket.gethostname() not in serialized
    assert '"production_effect": "none"' in serialized


def test_winner_double_checks_and_invalidation_change_gets_new_lease(tmp_path: Path) -> None:
    coordinator = ExternalRequestRevalidationCoordinator(tmp_path / "request", cache_key=KEY_A)
    probes: list[RevalidationProbe[str]] = [
        RevalidationProbe(
            status="NEEDS_REVALIDATION",
            generation_id="generation-1",
            body_sha256=BODY_OLD,
            reason_code="EXPIRED_REVALIDATE",
        ),
        RevalidationProbe(
            status="NEEDS_REVALIDATION",
            generation_id="generation-1",
            body_sha256=BODY_OLD,
            reason_code="INVALIDATED_REVALIDATE",
        ),
        RevalidationProbe(
            status="NEEDS_REVALIDATION",
            generation_id="generation-1",
            body_sha256=BODY_OLD,
            reason_code="INVALIDATED_REVALIDATE",
        ),
    ]
    published = False
    network_calls = 0

    def probe() -> RevalidationProbe[str]:
        if published:
            return RevalidationProbe(
                status="REUSABLE",
                generation_id="generation-2",
                body_sha256=BODY_NEW,
                reason_code="HIT",
                value="cached",
            )
        return probes.pop(0)

    def fetch() -> str:
        nonlocal network_calls
        network_calls += 1
        return "network"

    def publish(_value: str) -> None:
        nonlocal published
        published = True

    result = coordinator.execute(probe=probe, fetch=fetch, publish=publish)

    assert result.status == "WINNER_PUBLISHED"
    assert result.lease_generation == 2
    assert network_calls == 1
    replay = coordinator.replay()
    assert [event["outcome"] for event in replay.events if event["outcome"]] == [
        "SUPERSEDED",
        "PUBLISHED",
    ]


def test_winner_double_check_reuses_without_live_request(tmp_path: Path) -> None:
    coordinator = ExternalRequestRevalidationCoordinator(tmp_path / "request", cache_key=KEY_A)
    observations = iter(
        [
            RevalidationProbe[str](
                status="NEEDS_REVALIDATION",
                generation_id="generation-1",
                body_sha256=BODY_OLD,
                reason_code="EXPIRED_REVALIDATE",
            ),
            RevalidationProbe[str](
                status="REUSABLE",
                generation_id="generation-2",
                body_sha256=BODY_NEW,
                reason_code="HIT",
                value="cached",
            ),
        ]
    )

    result = coordinator.execute(
        probe=lambda: next(observations),
        fetch=lambda: pytest.fail("double-check reuse must not call live client"),
        publish=lambda _value: pytest.fail("double-check reuse must not publish"),
    )

    assert result.status == "WINNER_DOUBLE_CHECK_REUSE"
    assert result.value == "cached"


def test_waiter_reuses_after_active_owner_publishes_without_live_request(
    tmp_path: Path,
) -> None:
    coordinator = ExternalRequestRevalidationCoordinator(tmp_path / "request", cache_key=KEY_A)
    needs = RevalidationProbe[str](
        status="NEEDS_REVALIDATION",
        generation_id="generation-1",
        body_sha256=BODY_OLD,
        reason_code="EXPIRED_REVALIDATE",
    )
    reusable = RevalidationProbe(
        status="REUSABLE",
        generation_id="generation-2",
        body_sha256=BODY_NEW,
        reason_code="HIT",
        value="cached",
    )
    prior_owner = coordinator.acquire(needs, owner_id="owner-prior000")
    prior_lease = prior_owner.lease
    assert prior_lease is not None
    published = False

    def probe() -> RevalidationProbe[str]:
        return reusable if published else needs

    def publish_during_wait(_seconds: float) -> None:
        nonlocal published
        published = True
        coordinator.complete(
            prior_lease,
            outcome="PUBLISHED",
            published_probe=reusable,
        )

    result = coordinator.execute(
        probe=probe,
        fetch=lambda: pytest.fail("waiter reuse must not call live client"),
        publish=lambda _value: pytest.fail("waiter reuse must not publish"),
        sleep=publish_during_wait,
    )

    assert result.status == "WAITER_REUSE"
    assert result.value == "cached"
    assert coordinator.replay().current_state == "COMPLETED"


def test_waiter_probes_under_same_arbiter_as_winner_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator = ExternalRequestRevalidationCoordinator(tmp_path / "request", cache_key=KEY_A)
    needs = RevalidationProbe[str](
        status="NEEDS_REVALIDATION",
        generation_id="generation-1",
        body_sha256=BODY_OLD,
        reason_code="EXPIRED_REVALIDATE",
    )
    reusable = RevalidationProbe(
        status="REUSABLE",
        generation_id="generation-2",
        body_sha256=BODY_NEW,
        reason_code="HIT",
        value="cached",
    )
    prior_owner = coordinator.acquire(needs, owner_id="owner-prior000")
    prior_lease = prior_owner.lease
    assert prior_lease is not None

    original_lock = coordination._exclusive_file_lock
    lock_depth = 0

    @contextmanager
    def tracked_lock(*args: Any, **kwargs: Any) -> Any:
        nonlocal lock_depth
        with original_lock(*args, **kwargs):
            lock_depth += 1
            try:
                yield
            finally:
                lock_depth -= 1

    monkeypatch.setattr(coordination, "_exclusive_file_lock", tracked_lock)
    probe_calls = 0
    published = False

    def probe() -> RevalidationProbe[str]:
        nonlocal probe_calls
        probe_calls += 1
        if probe_calls > 1:
            assert lock_depth == 1
        return reusable if published else needs

    def publish_during_wait(_seconds: float) -> None:
        nonlocal published
        published = True
        coordinator.complete(
            prior_lease,
            outcome="PUBLISHED",
            published_probe=reusable,
        )

    result = coordinator.execute(
        probe=probe,
        fetch=lambda: pytest.fail("waiter reuse must not call live client"),
        publish=lambda _value: pytest.fail("waiter reuse must not publish"),
        sleep=publish_during_wait,
    )

    assert result.status == "WAITER_REUSE"
    assert result.value == "cached"
    assert probe_calls == 3
    assert lock_depth == 0


def test_late_contender_double_checks_completed_owner_without_live_request(
    tmp_path: Path,
) -> None:
    coordinator = ExternalRequestRevalidationCoordinator(tmp_path / "request", cache_key=KEY_A)
    needs = RevalidationProbe[str](
        status="NEEDS_REVALIDATION",
        generation_id="generation-1",
        body_sha256=BODY_OLD,
        reason_code="EXPIRED_REVALIDATE",
    )
    reusable = RevalidationProbe(
        status="REUSABLE",
        generation_id="generation-2",
        body_sha256=BODY_NEW,
        reason_code="HIT",
        value="cached",
    )
    prior_owner = coordinator.acquire(needs, owner_id="owner-prior000")
    assert prior_owner.lease is not None
    coordinator.complete(
        prior_owner.lease,
        outcome="PUBLISHED",
        published_probe=reusable,
    )
    observations = iter((needs, reusable))

    result = coordinator.execute(
        probe=lambda: next(observations),
        fetch=lambda: pytest.fail("late contender must not call live client"),
        publish=lambda _value: pytest.fail("late contender must not publish"),
    )

    assert result.status == "WINNER_DOUBLE_CHECK_REUSE"
    assert result.value == "cached"
    assert result.lease_generation == 2
    assert coordinator.replay().current_state == "COMPLETED"


def test_stale_owner_takeover_is_bounded_and_old_owner_cannot_publish(tmp_path: Path) -> None:
    policy = replace(
        load_revalidation_coordination_policy(POLICY_PATH),
        lease_ttl_seconds=1,
        waiter_timeout_seconds=2,
    )
    coordinator = ExternalRequestRevalidationCoordinator(
        tmp_path / "request", cache_key=KEY_A, policy=policy
    )
    probe = RevalidationProbe[str](
        status="NEEDS_REVALIDATION",
        generation_id="generation-1",
        body_sha256=BODY_OLD,
        reason_code="EXPIRED_REVALIDATE",
    )
    now = datetime(2026, 7, 20, tzinfo=UTC)
    first = coordinator.acquire(probe, owner_id="owner-first000", now=now)
    waiter = coordinator.acquire(
        probe, owner_id="owner-waiter00", now=now + timedelta(milliseconds=500)
    )
    takeover = coordinator.acquire(probe, owner_id="owner-takeover", now=now + timedelta(seconds=1))

    assert first.status == "OWNER"
    assert waiter.status == "WAITER"
    assert takeover.status == "OWNER"
    assert takeover.lease is not None and takeover.lease.generation == 2
    assert first.lease is not None
    with pytest.raises(RevalidationCoordinationError, match="STALE_LEASE_OWNER"):
        coordinator.complete(first.lease, outcome="OWNER_FAILURE", now=now + timedelta(seconds=1))

    blocked = coordinator.acquire(probe, owner_id="owner-third000", now=now + timedelta(seconds=2))
    assert blocked.status == "OWNER_FAILURE_BLOCKED"
    assert blocked.reason_code == "STALE_TAKEOVER_LIMIT_REACHED"


def test_stale_owner_is_fenced_before_cache_publish_callback(tmp_path: Path) -> None:
    policy = replace(
        load_revalidation_coordination_policy(POLICY_PATH),
        lease_ttl_seconds=1,
        waiter_timeout_seconds=2,
    )
    coordinator = ExternalRequestRevalidationCoordinator(
        tmp_path / "request", cache_key=KEY_A, policy=policy
    )
    expired = RevalidationProbe[str](
        status="NEEDS_REVALIDATION",
        generation_id="generation-1",
        body_sha256=BODY_OLD,
        reason_code="EXPIRED_REVALIDATE",
    )
    reusable = RevalidationProbe(
        status="REUSABLE",
        generation_id="generation-2",
        body_sha256=BODY_NEW,
        reason_code="HIT",
        value="cached",
    )
    now = datetime(2026, 7, 20, tzinfo=UTC)
    first = coordinator.acquire(expired, owner_id="owner-first000", now=now)
    takeover = coordinator.acquire(
        expired,
        owner_id="owner-takeover",
        now=now + timedelta(seconds=1),
    )
    assert first.lease is not None and takeover.lease is not None
    publish_calls = 0

    def publish(_value: str) -> None:
        nonlocal publish_calls
        publish_calls += 1

    with pytest.raises(RevalidationCoordinationError, match="STALE_LEASE_OWNER"):
        coordinator.publish_if_current_owner(
            first.lease,
            "stale-network-response",
            publish=publish,
            probe=lambda: reusable,
        )
    assert publish_calls == 0

    coordinator.publish_if_current_owner(
        takeover.lease,
        "current-network-response",
        publish=publish,
        probe=lambda: reusable,
    )
    assert publish_calls == 1
    assert coordinator.replay().current_state == "COMPLETED"


def test_explicit_owner_failure_disables_waiter_retry(tmp_path: Path) -> None:
    coordinator = ExternalRequestRevalidationCoordinator(tmp_path / "request", cache_key=KEY_A)
    probe = RevalidationProbe[str](
        status="NEEDS_REVALIDATION",
        generation_id="generation-1",
        body_sha256=BODY_OLD,
        reason_code="EXPIRED_REVALIDATE",
    )
    acquired = coordinator.acquire(probe)
    assert acquired.lease is not None
    coordinator.complete(acquired.lease, outcome="OWNER_FAILURE")

    blocked = coordinator.acquire(probe)
    assert blocked.status == "OWNER_FAILURE_BLOCKED"
    assert blocked.reason_code == "OWNER_FAILURE_RETRY_DISABLED"


def test_waiter_timeout_is_bounded_and_does_not_take_live_request(tmp_path: Path) -> None:
    policy = replace(
        load_revalidation_coordination_policy(POLICY_PATH),
        lease_ttl_seconds=10,
        waiter_timeout_seconds=1,
    )
    coordinator = ExternalRequestRevalidationCoordinator(
        tmp_path / "request", cache_key=KEY_A, policy=policy
    )
    needs = RevalidationProbe[str](
        status="NEEDS_REVALIDATION",
        generation_id="generation-1",
        body_sha256=BODY_OLD,
        reason_code="EXPIRED_REVALIDATE",
    )
    coordinator.acquire(needs)
    ticks = iter((0.0, 0.6, 1.2))

    with pytest.raises(RevalidationCoordinationTimeout, match="WAITER_TIMEOUT"):
        coordinator.execute(
            probe=lambda: needs,
            fetch=lambda: pytest.fail("waiter timeout must fail closed"),
            publish=lambda _value: pytest.fail("waiter timeout must not publish"),
            monotonic=lambda: next(ticks),
            sleep=lambda _seconds: None,
        )

    assert coordinator.replay().current_state == "ACTIVE"


@pytest.mark.parametrize("target", ["pointer", "event"])
def test_pointer_or_event_tamper_fails_closed(tmp_path: Path, target: str) -> None:
    coordinator = ExternalRequestRevalidationCoordinator(tmp_path / "request", cache_key=KEY_A)
    probe = RevalidationProbe[str](
        status="NEEDS_REVALIDATION",
        generation_id="generation-1",
        body_sha256=BODY_OLD,
        reason_code="EXPIRED_REVALIDATE",
    )
    coordinator.acquire(probe)
    path = (
        coordinator.pointer_path
        if target == "pointer"
        else next(coordinator.events_root.glob("*.json"))
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["generation"] = 999
    write_json_atomic(path, payload)

    assert coordinator.replay().status == "FAIL"
    with pytest.raises(RevalidationCoordinationIntegrityError):
        coordinator.acquire(probe)


def test_file_lock_retries_transient_open_permission_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_path = tmp_path / "open-contention.lock"
    original_open = Path.open
    attempts = 0

    def flaky_open(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal attempts
        if path == lock_path:
            attempts += 1
            if attempts == 1:
                raise PermissionError(errno.EACCES, "transient open contention", path)
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", flaky_open)

    with coordination._exclusive_file_lock(
        lock_path,
        timeout_seconds=1.0,
        poll_seconds=0.0,
    ):
        assert lock_path.is_file()

    assert attempts == 2


def test_file_lock_open_contention_deadline_uses_typed_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_path = tmp_path / "open-timeout.lock"
    original_open = Path.open

    def denied_open(path: Path, *args: Any, **kwargs: Any) -> Any:
        if path == lock_path:
            raise PermissionError(errno.EACCES, "persistent open contention", path)
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", denied_open)

    with pytest.raises(RevalidationCoordinationTimeout, match="ARBITER_TIMEOUT"):
        with coordination._exclusive_file_lock(
            lock_path,
            timeout_seconds=0.0,
            poll_seconds=0.0,
        ):
            pytest.fail("timed-out open contention must not enter the critical section")


@pytest.mark.parametrize("_iteration", range(20))
def test_two_spawned_processes_same_key_make_one_live_request(
    tmp_path: Path,
    _iteration: int,
) -> None:
    run = _run_spawn_workers(
        tmp_path,
        [("shared", KEY_A), ("shared", KEY_A)],
        network_delay_seconds=0.35,
    )
    results = run.results

    assert {result.pid for result in results} == set(run.worker_pids)
    assert all(result.outcome == "PASS" for result in results), results
    statuses = [result.coordination_status for result in results]
    assert statuses.count("WINNER_PUBLISHED") == 1
    reuse_statuses = [status for status in statuses if status != "WINNER_PUBLISHED"]
    # The first-probe barrier freezes the same stale observation in both processes, but
    # Windows may schedule the contender either before or after the winner completes.
    # Both paths reuse the published generation and must make no second live request.
    assert reuse_statuses in (["WAITER_REUSE"], ["WINNER_DOUBLE_CHECK_REUSE"])
    assert len(list(run.call_dir.glob("*.json"))) == 1
    replay = ExternalRequestRevalidationCoordinator(tmp_path / "shared", cache_key=KEY_A).replay()
    assert replay.status == "PASS"
    assert replay.current_state == "COMPLETED"
    terminal_outcomes = [event["outcome"] for event in replay.events if event["outcome"]]
    if reuse_statuses == ["WAITER_REUSE"]:
        assert terminal_outcomes == ["PUBLISHED"]
    else:
        assert terminal_outcomes == ["PUBLISHED", "DOUBLE_CHECK_REUSE"]
        assert [event["generation"] for event in replay.events] == [1, 1, 2, 2]


def test_spawn_fixture_rejects_same_request_name_with_different_keys(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="exactly one cache_key"):
        _run_spawn_workers(
            tmp_path,
            [("shared", KEY_A), ("shared", KEY_B)],
            network_delay_seconds=0.01,
        )

    assert not (tmp_path / "calls").exists()


def test_spawned_processes_different_keys_revalidate_in_parallel(tmp_path: Path) -> None:
    run = _run_spawn_workers(
        tmp_path,
        [("request-a", KEY_A), ("request-b", KEY_B)],
        network_delay_seconds=0.45,
    )
    results = run.results
    calls = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in run.call_dir.glob("*.json")
    ]

    assert {result.pid for result in results} == set(run.worker_pids)
    assert all(result.outcome == "PASS" for result in results), results
    assert [result.coordination_status for result in results].count(
        "WINNER_PUBLISHED"
    ) == 2
    assert len(calls) == 2
    assert max(call["started"] for call in calls) < min(call["ended"] for call in calls)


def test_non_reusable_winner_response_is_returned_and_waiter_revalidates_serially(
    tmp_path: Path,
) -> None:
    run = _run_spawn_workers(
        tmp_path,
        [("shared", KEY_A), ("shared", KEY_A)],
        network_delay_seconds=0.25,
        published_status="NEEDS_REVALIDATION",
    )
    results = run.results
    calls = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in run.call_dir.glob("*.json")
    ]

    assert {result.pid for result in results} == set(run.worker_pids)
    assert all(result.outcome == "PASS" for result in results), results
    assert [result.coordination_status for result in results].count(
        "WINNER_NON_REUSABLE_RESPONSE"
    ) == 2
    assert [result.value for result in results] == ["network", "network"]
    assert len(calls) == 2
    first, second = sorted(calls, key=lambda call: call["started"])
    assert first["ended"] <= second["started"]


@pytest.mark.parametrize(
    ("worker_target", "expected_exit_code"),
    [
        (_multiprocess_early_exit_worker, 23),
        (_multiprocess_clean_exit_before_probe_worker, 0),
    ],
)
def test_spawn_harness_detects_child_early_exit_and_cleans_resources(
    tmp_path: Path,
    worker_target: Callable[..., None],
    expected_exit_code: int,
) -> None:
    baseline_child_pids = {
        child.pid for child in multiprocessing.active_children()
    }
    started = time.monotonic()

    with pytest.raises(
        AssertionError,
        match=(
            rf"child PID \d+ exited with code {expected_exit_code} "
            "during first-probe barrier"
        ),
    ):
        _run_spawn_workers(
            tmp_path,
            [("shared", KEY_A)],
            network_delay_seconds=0.01,
            orchestration_timeout_seconds=5.0,
            worker_target=worker_target,
        )

    assert time.monotonic() - started < 5.0
    assert {
        child.pid for child in multiprocessing.active_children()
    } <= baseline_child_pids


def test_spawn_harness_deadline_terminates_stalled_child(tmp_path: Path) -> None:
    baseline_child_pids = {
        child.pid for child in multiprocessing.active_children()
    }
    started = time.monotonic()

    with pytest.raises(
        AssertionError,
        match="orchestration deadline elapsed during first-probe barrier",
    ):
        _run_spawn_workers(
            tmp_path,
            [("shared", KEY_A)],
            network_delay_seconds=0.01,
            orchestration_timeout_seconds=0.5,
            worker_target=_multiprocess_stall_before_probe_worker,
        )

    assert time.monotonic() - started < 3.0
    assert {
        child.pid for child in multiprocessing.active_children()
    } <= baseline_child_pids


def test_spawn_harness_detects_exit_before_terminal_result(tmp_path: Path) -> None:
    baseline_child_pids = {
        child.pid for child in multiprocessing.active_children()
    }
    started = time.monotonic()

    with pytest.raises(
        AssertionError,
        match=r"child PID \d+ exited with code 31 during terminal result collection",
    ):
        _run_spawn_workers(
            tmp_path,
            [("shared", KEY_A)],
            network_delay_seconds=0.01,
            orchestration_timeout_seconds=5.0,
            worker_target=_multiprocess_exit_before_result_worker,
        )

    assert time.monotonic() - started < 5.0
    assert {
        child.pid for child in multiprocessing.active_children()
    } <= baseline_child_pids
