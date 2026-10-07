"""DEVX-023 P: the real process pool against pristine and mutated on-disk lease stores.

Every case compares the opt-in parallel replay with the serial replay of the SAME directory tree
(status, heads, active leases, head event ids, event count and issues). A mutation that makes a
worker unable to produce an answer must fall back to the serial replay and give its answer.
"""

from __future__ import annotations

import json
import shutil
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from lease_parallel_replay_support import (
    T0,
    append_chain,
    event_file,
    make_store,
    populate,
    write_claim,
)

from ai_trading_system.platform.architecture import lease_parallel_replay as parallel
from ai_trading_system.platform.architecture import parallel_control_kernel as kernel

VICTIM = "lease-p003"


@pytest.fixture(autouse=True)
def _real_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(parallel.PARALLEL_REPLAY_ENV, "2")
    monkeypatch.delenv(parallel.VERIFY_ENV, raising=False)
    monkeypatch.setattr(parallel, "MIN_EVENTS", 1)


def _tamper(store: Any, chains: dict[str, list[kernel.LeaseEvent]]) -> None:
    path = event_file(store, chains[VICTIM][1])
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["actor"] = "tampered"
    path.write_text(json.dumps(payload), encoding="utf-8")


def _truncate(store: Any, chains: dict[str, list[kernel.LeaseEvent]]) -> None:
    path = event_file(store, chains[VICTIM][1])
    path.write_bytes(path.read_bytes()[:40])


def _delete_middle(store: Any, chains: dict[str, list[kernel.LeaseEvent]]) -> None:
    event_file(store, chains[VICTIM][1]).unlink()


def _duplicate_file(store: Any, chains: dict[str, list[kernel.LeaseEvent]]) -> None:
    source = event_file(store, chains[VICTIM][1])
    shutil.copyfile(source, source.with_name("copy-of-the-same-event.json"))


def _garbage_file(store: Any, chains: dict[str, list[kernel.LeaseEvent]]) -> None:
    (store.events_root / VICTIM / "garbage.json").write_text("not json at all", encoding="utf-8")


def _foreign_file(store: Any, chains: dict[str, list[kernel.LeaseEvent]]) -> None:
    (store.events_root / VICTIM / "notes.txt").write_text("not an event", encoding="utf-8")


def _empty_directory(store: Any, chains: dict[str, list[kernel.LeaseEvent]]) -> None:
    (store.events_root / "lease-empty").mkdir()


def _swap_contents(store: Any, chains: dict[str, list[kernel.LeaseEvent]]) -> None:
    first = event_file(store, chains[VICTIM][1])
    second = event_file(store, chains[VICTIM][2])
    left, right = first.read_bytes(), second.read_bytes()
    first.write_bytes(right)
    second.write_bytes(left)


def _active_conflict(store: Any, chains: dict[str, list[kernel.LeaseEvent]]) -> None:
    for name in ("lease-x1", "lease-x2"):
        append_chain(
            store,
            name,
            ("REQUESTED", "ACTIVE"),
            resources=write_claim("src/shared.py"),
            task_id=f"T-{name}",
        )


def _a_second_chain_head(store: Any, chains: dict[str, list[kernel.LeaseEvent]]) -> None:
    # A chain whose newest event is not the head of a single causal line: two children of one event.
    base = chains[VICTIM][0]
    sibling = kernel._lease_event(
        lease=chains[VICTIM][1].lease,
        previous_event_id=base.event_id,
        from_state="REQUESTED",
        to_state="ACTIVE",
        occurred_at=T0 + timedelta(hours=1),
        actor="a",
        reason_codes=("FORK",),
    )
    store._append_event(sibling)


CASES = {
    "pristine": lambda store, chains: None,
    "tampered_byte": _tamper,
    "truncated_file": _truncate,
    "deleted_middle_event": _delete_middle,
    "duplicated_event_file": _duplicate_file,
    "garbage_json_file": _garbage_file,
    "foreign_non_json_file": _foreign_file,
    "empty_chain_directory": _empty_directory,
    "swapped_event_contents": _swap_contents,
    "two_active_leases_overlap": _active_conflict,
    "forked_chain": _a_second_chain_head,
}


@pytest.mark.parametrize("name", sorted(CASES))
def test_the_parallel_replay_equals_the_serial_replay_after_each_mutation(
    tmp_path: Path, name: str
) -> None:
    store = make_store(tmp_path)
    chains = populate(store, 12)
    CASES[name](store, chains)
    serial = store._replay_serial()
    result = store.replay()
    assert result == serial
    assert parallel.LAST_FALLBACK_REASON is None  # the real pool answered, not the fallback
    if name == "pristine":
        assert serial.status == "PASS" and serial.event_count == 48 and not serial.issues
    elif name in {"foreign_non_json_file", "empty_chain_directory", "swapped_event_contents"}:
        assert serial.status == "PASS"
    else:
        assert serial.status == "FAIL" and serial.issues  # the mutation is really detected


def test_an_event_in_another_chains_directory_is_left_to_the_serial_replay(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    chains = populate(store, 8)
    stray = chains["lease-p000"][0]
    shutil.copyfile(
        event_file(store, stray), store.events_root / "lease-p001" / f"{stray.event_id}.json"
    )
    serial = store._replay_serial()
    assert store.replay() == serial
    assert (parallel.LAST_FALLBACK_REASON or "").startswith("lease_id_differs_from_directory:")


def test_more_workers_than_chains_and_a_single_chain_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = make_store(tmp_path)
    populate(store, 1)
    monkeypatch.setenv(parallel.PARALLEL_REPLAY_ENV, "8")
    assert store.replay() == store._replay_serial()
    assert parallel.LAST_FALLBACK_REASON is None


def test_the_verify_mode_with_the_real_pool_agrees(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = make_store(tmp_path)
    populate(store, 10)
    monkeypatch.setenv(parallel.VERIFY_ENV, "1")
    assert store.replay() == store._replay_serial()
    assert parallel.LAST_FALLBACK_REASON is None
