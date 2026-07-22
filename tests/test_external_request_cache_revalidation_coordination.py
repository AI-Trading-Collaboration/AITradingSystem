from __future__ import annotations

import json
import multiprocessing
import os
import socket
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

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
            first_probe_queue.put((cache_key, os.getpid()))
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
        result_queue.put(("PASS", result.status, result.value))
    except BaseException as exc:  # pragma: no cover - surfaced in parent process
        result_queue.put(("FAIL", type(exc).__name__, str(exc)))


def _start_spawn_workers(
    tmp_path: Path,
    specifications: list[tuple[str, str]],
    *,
    network_delay_seconds: float,
    published_status: str = "REUSABLE",
) -> tuple[list[Any], Any, Any, Path]:
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

    context = multiprocessing.get_context("spawn")
    first_probe_queue = context.Queue()
    release_first_probe = context.Event()
    result_queue = context.Queue()
    call_dir = tmp_path / "calls"
    call_dir.mkdir()
    workers = []
    for request_name, cache_key in specifications:
        request_dir = tmp_path / request_name
        state_path = tmp_path / f"{request_name}.json"
        worker = context.Process(
            target=_multiprocess_execute_worker,
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
    for _ in workers:
        first_probe_queue.get(timeout=15)
    release_first_probe.set()
    return workers, result_queue, release_first_probe, call_dir


def _finish_workers(workers: list[Any], result_queue: Any) -> list[tuple[str, str, str]]:
    results = [result_queue.get(timeout=15) for _ in workers]
    for worker in workers:
        worker.join(timeout=15)
        assert worker.exitcode == 0
    return results


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
    assert prior_owner.lease is not None
    published = False

    def probe() -> RevalidationProbe[str]:
        return reusable if published else needs

    def publish_during_wait(_seconds: float) -> None:
        nonlocal published
        published = True
        coordinator.complete(
            prior_owner.lease,
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


def test_two_spawned_processes_same_key_make_one_live_request(tmp_path: Path) -> None:
    workers, result_queue, _release, call_dir = _start_spawn_workers(
        tmp_path,
        [("shared", KEY_A), ("shared", KEY_A)],
        network_delay_seconds=0.35,
    )
    results = _finish_workers(workers, result_queue)

    assert all(result[0] == "PASS" for result in results), results
    statuses = [result[1] for result in results]
    assert statuses.count("WINNER_PUBLISHED") == 1
    reuse_statuses = [status for status in statuses if status != "WINNER_PUBLISHED"]
    # The first-probe barrier freezes the same stale observation in both processes, but
    # Windows may schedule the contender either before or after the winner completes.
    # Both paths reuse the published generation and must make no second live request.
    assert reuse_statuses in (["WAITER_REUSE"], ["WINNER_DOUBLE_CHECK_REUSE"])
    assert len(list(call_dir.glob("*.json"))) == 1
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
        _start_spawn_workers(
            tmp_path,
            [("shared", KEY_A), ("shared", KEY_B)],
            network_delay_seconds=0.01,
        )

    assert not (tmp_path / "calls").exists()


def test_spawned_processes_different_keys_revalidate_in_parallel(tmp_path: Path) -> None:
    workers, result_queue, _release, call_dir = _start_spawn_workers(
        tmp_path,
        [("request-a", KEY_A), ("request-b", KEY_B)],
        network_delay_seconds=0.45,
    )
    results = _finish_workers(workers, result_queue)
    calls = [json.loads(path.read_text(encoding="utf-8")) for path in call_dir.glob("*.json")]

    assert all(result[0] == "PASS" for result in results), results
    assert [result[1] for result in results].count("WINNER_PUBLISHED") == 2
    assert len(calls) == 2
    assert max(call["started"] for call in calls) < min(call["ended"] for call in calls)


def test_non_reusable_winner_response_is_returned_and_waiter_revalidates_serially(
    tmp_path: Path,
) -> None:
    workers, result_queue, _release, call_dir = _start_spawn_workers(
        tmp_path,
        [("shared", KEY_A), ("shared", KEY_A)],
        network_delay_seconds=0.25,
        published_status="NEEDS_REVALIDATION",
    )
    results = _finish_workers(workers, result_queue)
    calls = [json.loads(path.read_text(encoding="utf-8")) for path in call_dir.glob("*.json")]

    assert all(result[0] == "PASS" for result in results), results
    assert [result[1] for result in results].count("WINNER_NON_REUSABLE_RESPONSE") == 2
    assert [result[2] for result in results] == ["network", "network"]
    assert len(calls) == 2
    first, second = sorted(calls, key=lambda call: call["started"])
    assert first["ended"] <= second["started"]
