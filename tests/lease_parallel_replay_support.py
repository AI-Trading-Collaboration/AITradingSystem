"""Fixtures for the DEVX-023 P parallel replay tests: synthetic multi-chain lease stores.

Chains carry no execution unless asked, so the real validators (which run in worker processes) never
see a synthetic execution. Execution-bearing chains are tested in-process with the validators
replaced (the same technique as tests/test_devx022_lease_event_externalization.py).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.platform.architecture import parallel_control_kernel as kernel

ROOT = Path(__file__).resolve().parents[1]
T0 = datetime(2026, 10, 5, tzinfo=UTC)
CHAIN_STATES = ("REQUESTED", "ACTIVE", "ACTIVE", "RELEASED")


def make_store(tmp_path: Path) -> kernel.FileExecutionLeaseStore:
    policy = kernel.load_parallel_control_policy(
        ROOT / "config/architecture/arch_005_parallel_control_policy.yaml"
    )
    return kernel.FileExecutionLeaseStore(tmp_path / "store", policy=policy)


def make_lease(
    lease_id: str,
    state: str,
    *,
    execution: Any = None,
    resources: tuple[kernel.ResourceClaim, ...] = (),
    task_id: str | None = None,
) -> kernel.ExecutionLease:
    return kernel.ExecutionLease(
        lease_id=lease_id,
        task_id=task_id or f"T-{lease_id}",
        change_id="C",
        lane_id="L",
        actor="a",
        base_commit="0" * 40,
        change_manifest_sha256="0" * 64,
        policy_version="p",
        generation=1,
        previous_lease_id=None,
        state=state,
        requested_at="2026-10-05T00:00:00+00:00",
        acquired_at=None,
        expires_at=None,
        resources=resources,
        execution=execution,
    )


def make_event(
    lease: kernel.ExecutionLease, *, previous: str | None, from_state: str | None, minute: int
) -> kernel.LeaseEvent:
    return kernel._lease_event(
        lease=lease,
        previous_event_id=previous,
        from_state=from_state,
        to_state=lease.state,
        occurred_at=T0 + timedelta(minutes=minute),
        actor="a",
        reason_codes=(),
    )


def append_chain(
    store: kernel.FileExecutionLeaseStore,
    lease_id: str,
    states: tuple[str, ...] = CHAIN_STATES,
    *,
    execution: Any = None,
    resources: tuple[kernel.ResourceClaim, ...] = (),
    task_id: str | None = None,
) -> list[kernel.LeaseEvent]:
    events: list[kernel.LeaseEvent] = []
    previous: str | None = None
    from_state: str | None = None
    for minute, state in enumerate(states):
        lease = make_lease(
            lease_id, state, execution=execution, resources=resources, task_id=task_id
        )
        event = make_event(lease, previous=previous, from_state=from_state, minute=minute)
        store._append_event(event)
        events.append(event)
        previous, from_state = event.event_id, state
    return events


def populate(
    store: kernel.FileExecutionLeaseStore, chains: int = 20
) -> dict[str, list[kernel.LeaseEvent]]:
    return {
        f"lease-p{index:03d}": append_chain(store, f"lease-p{index:03d}") for index in range(chains)
    }


def event_file(store: kernel.FileExecutionLeaseStore, event: kernel.LeaseEvent) -> Path:
    return store.events_root / event.lease.lease_id / f"{event.event_id}.json"


def write_claim(resource_id: str) -> tuple[kernel.ResourceClaim, ...]:
    return (
        kernel.ResourceClaim(
            kind="path", resource_id=resource_id, access=kernel.ResourceAccess.WRITE
        ),
    )
