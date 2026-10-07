"""DEVX-023 P: the opt-in chain-level parallel replay (unit level, workers run in-process).

The serial replay is the authority: these tests pin the switch, every fallback, the shared assembly,
the pickle-able row tables and the verify mode. The real process pool is exercised against mutated
stores in test_arch_005_lease_parallel_replay_store.py.
"""

from __future__ import annotations

import copy
import pickle
import sys
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from lease_parallel_replay_support import (
    append_chain,
    event_file,
    make_store,
    populate,
    write_claim,
)

from ai_trading_system.platform.architecture import lease_parallel_replay as parallel
from ai_trading_system.platform.architecture import parallel_control_kernel as kernel
from ai_trading_system.platform.architecture import workflow_coordination as coordination
from ai_trading_system.platform.architecture.parallel_control import ParallelControlError


class InProcessPool:
    """A stand-in for ProcessPoolExecutor: same call shape, tasks run here (validators patched)."""

    created = 0

    def __init__(self, *, max_workers: int, mp_context: Any, initializer: Any, initargs: Any):
        type(self).created += 1
        self.max_workers = max_workers
        initializer(*initargs)

    def map(self, function: Any, items: Any, chunksize: int = 1, timeout: Any = None) -> Any:
        return [function(item) for item in items]

    def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
        if parallel._SCOPES is not None:
            parallel._SCOPES.close()


@pytest.fixture(autouse=True)
def _isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(parallel.PARALLEL_REPLAY_ENV, raising=False)
    monkeypatch.delenv(parallel.VERIFY_ENV, raising=False)
    InProcessPool.created = 0
    # Synthetic executions: business validation is covered elsewhere (see the S3b tests).
    monkeypatch.setattr(coordination, "validate_execution", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        coordination, "_validate_checked_execution_transition", lambda *args, **kwargs: None
    )


def _enable(monkeypatch: pytest.MonkeyPatch, pool: Any = InProcessPool, workers: str = "3") -> None:
    monkeypatch.setenv(parallel.PARALLEL_REPLAY_ENV, workers)
    monkeypatch.setattr(parallel, "MIN_EVENTS", 1)
    monkeypatch.setattr(parallel, "ProcessPoolExecutor", pool)


def _rows(count: int, tag: str = "a") -> list[dict[str, Any]]:
    return [
        {"relative": f"{tag}/{index}.py", "sha256": f"{index:064x}", "size_bytes": index}
        for index in range(count)
    ]


def _execution(rows: list[Any]) -> dict[str, Any]:
    capsule = {
        "definition": {"directory": ".git/hooks"},
        "ready": {
            "schema_version": "ready",
            "inputs": {kernel.EXTERNALIZED_ROWS_FIELD: rows, "runtime_file_count": 3},
        },
    }
    return {"state": "RUNNING", "hook_capsule": capsule}


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, 0),
        ("", 0),
        ("0", 0),
        ("1", 0),
        ("off", 0),
        ("False", 0),
        ("no", 0),
        ("garbage", 0),
        ("-3", 0),
        ("2", 2),
        ("4", 4),
        (" 8 ", 8),
        ("64", parallel.MAX_WORKERS),
        ("auto", parallel.DEFAULT_WORKERS),
        ("ON", parallel.DEFAULT_WORKERS),
        ("true", parallel.DEFAULT_WORKERS),
    ],
)
def test_the_switch_is_parsed_fail_safe(raw: str | None, expected: int) -> None:
    environ = {} if raw is None else {parallel.PARALLEL_REPLAY_ENV: raw}
    assert parallel.configured_workers(environ) == expected


def test_with_the_switch_off_the_helper_is_never_consulted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = make_store(tmp_path)
    populate(store, 5)

    def forbidden(**kwargs: Any) -> Any:
        raise AssertionError("the parallel helper must not run while the switch is unset")

    monkeypatch.setattr(parallel, "replay_if_enabled", forbidden)
    replay = store.replay()
    assert replay.status == "PASS" and replay.event_count == 20
    assert replay == store._replay_serial()


def test_the_replay_is_identical_with_and_without_the_pool_on_plain_chains(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = make_store(tmp_path)
    populate(store, 8)
    serial = store._replay_serial()
    _enable(monkeypatch)
    assert store.replay() == serial
    assert parallel.LAST_FALLBACK_REASON is None and InProcessPool.created == 1
    assert len(serial.lease_heads) == 8 and serial.status == "PASS"


def test_chains_with_externalized_tables_survive_the_trip_through_a_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = make_store(tmp_path)
    populate(store, 4)
    big = _rows(kernel.EXTERNALIZED_ROWS_MINIMUM + 100)
    append_chain(store, "lease-big-a", execution=_execution(big))
    append_chain(
        store, "lease-big-b", execution=_execution(_rows(kernel.EXTERNALIZED_ROWS_MINIMUM + 7, "b"))
    )
    serial = store._replay_serial()
    _enable(monkeypatch)
    parallel_replay = store.replay()
    assert parallel.LAST_FALLBACK_REASON is None
    assert parallel_replay == serial
    heads = {lease.lease_id: lease for lease in parallel_replay.lease_heads}
    rows = heads["lease-big-a"].execution["hook_capsule"]["ready"]["inputs"][
        kernel.EXTERNALIZED_ROWS_FIELD
    ]
    assert type(rows) is kernel.ExternalizedRows and rows.sha256 and rows == big
    with pytest.raises(TypeError, match="immutable"):
        rows.append({})  # still immutable after the trip


def test_externalized_rows_pickle_round_trip_keeps_the_digest_and_stays_immutable() -> None:
    rows = kernel._adopt_rows(_rows(kernel.EXTERNALIZED_ROWS_MINIMUM))
    assert type(rows) is kernel.ExternalizedRows
    restored = pickle.loads(pickle.dumps(rows, protocol=pickle.HIGHEST_PROTOCOL))
    assert type(restored) is kernel.ExternalizedRows
    assert restored == rows and restored.sha256 == rows.sha256
    with pytest.raises(TypeError, match="immutable"):
        restored.append({})
    # copy semantics are unchanged: a copy is a plain list
    assert type(copy.copy(restored)) is list and type(copy.deepcopy(restored)) is list


def test_a_small_store_stays_serial(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = make_store(tmp_path)
    populate(store, 3)
    _enable(monkeypatch)
    monkeypatch.setattr(parallel, "MIN_EVENTS", 600)
    assert store.replay() == store._replay_serial()
    assert parallel.LAST_FALLBACK_REASON == "small_store" and InProcessPool.created == 0


def test_a_missing_events_directory_is_left_to_the_serial_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = make_store(tmp_path)
    _enable(monkeypatch)
    replay = store.replay()
    assert replay.status == "PASS" and replay.event_count == 0
    assert parallel.LAST_FALLBACK_REASON == "no_events"


@pytest.mark.parametrize("failure", ["creation", "map", "timeout", "broken_pool"])
def test_every_pool_failure_falls_back_to_the_serial_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    store = make_store(tmp_path)
    populate(store, 6)
    serial = store._replay_serial()

    class FailingPool(InProcessPool):
        def __init__(self, **kwargs: Any) -> None:
            if failure == "creation":
                raise OSError("cannot create the pool")
            super().__init__(**kwargs)

        def map(self, function: Any, items: Any, chunksize: int = 1, timeout: Any = None) -> Any:
            if failure == "map":
                raise RuntimeError("worker error")
            if failure == "timeout":
                raise TimeoutError()
            raise BrokenProcessPool("a worker died")

    _enable(monkeypatch, pool=FailingPool)
    assert store.replay() == serial
    assert (parallel.LAST_FALLBACK_REASON or "").startswith("parallel_failed:")


def test_a_result_that_cannot_be_unpickled_falls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = make_store(tmp_path)
    populate(store, 4)
    serial = store._replay_serial()
    _enable(monkeypatch)

    def broken(blob: bytes) -> Any:
        raise pickle.UnpicklingError("corrupt")

    monkeypatch.setattr(parallel.pickle, "loads", broken)
    assert store.replay() == serial
    assert parallel.LAST_FALLBACK_REASON == "parallel_failed:UnpicklingError"


def test_an_event_in_the_wrong_directory_makes_the_replay_serial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = make_store(tmp_path)
    chains = populate(store, 4)
    victim = chains["lease-p000"][0]
    other = store.events_root / "lease-p001"
    (other / f"{victim.event_id}.json").write_bytes(event_file(store, victim).read_bytes())
    serial = store._replay_serial()
    _enable(monkeypatch)
    assert store.replay() == serial
    assert (parallel.LAST_FALLBACK_REASON or "").startswith("lease_id_differs_from_directory:")


def test_the_serial_and_the_parallel_replay_end_in_the_same_assembly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = make_store(tmp_path)
    populate(store, 4)
    calls: list[int] = []
    real = kernel._assemble_lease_replay

    def spy(heads: Any, head_ids: Any, issues: Any, *, event_count: int) -> Any:
        calls.append(event_count)
        return real(heads, head_ids, issues, event_count=event_count)

    monkeypatch.setattr(kernel, "_assemble_lease_replay", spy)
    store._replay_serial()
    serial_calls = len(calls)
    assert serial_calls == 1
    _enable(monkeypatch)
    store.replay()
    # one assembly per chain inside the workers plus the final merge in the parent
    assert len(calls) - serial_calls == 4 + 1 and calls[-1] == 16


def test_the_verify_mode_catches_a_difference_and_the_default_mode_does_not(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = make_store(tmp_path)
    populate(store, 4)
    serial = store._replay_serial()
    _enable(monkeypatch)
    real_loads = pickle.loads

    def lossy(blob: bytes) -> Any:
        # A defective worker result: one chain loses its head on the way back to the parent.
        part = real_loads(blob)
        if part.lease_heads and part.lease_heads[0].lease_id == "lease-p000":
            return kernel.LeaseReplay(
                status=part.status,
                lease_heads=(),
                active_leases=(),
                head_event_ids=(),
                event_count=part.event_count,
                issues=part.issues,
            )
        return part

    monkeypatch.setattr(parallel.pickle, "loads", lossy)
    unverified = store.replay()  # without the verify mode a lossy merge goes unnoticed
    assert unverified != serial and len(unverified.lease_heads) == 3
    monkeypatch.setenv(parallel.VERIFY_ENV, "1")
    with pytest.raises(ParallelControlError, match="LEASE_PARALLEL_REPLAY_MISMATCH"):
        store.replay()


def test_the_verify_mode_accepts_a_correct_parallel_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = make_store(tmp_path)
    populate(store, 5)
    _enable(monkeypatch)
    monkeypatch.setenv(parallel.VERIFY_ENV, "1")
    assert store.replay() == store._replay_serial()


def test_an_unsafe_main_module_keeps_the_replay_serial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = make_store(tmp_path)
    populate(store, 4)
    _enable(monkeypatch)
    monkeypatch.setattr(parallel, "_main_is_safe_for_spawn", lambda: False)
    assert store.replay() == store._replay_serial()
    assert parallel.LAST_FALLBACK_REASON == "unsafe_main" and InProcessPool.created == 0


def test_the_main_module_check_reads_the_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    guarded = tmp_path / "guarded.py"
    guarded.write_text(
        "def main():\n    pass\n\nif __name__ == '__main__':\n    main()\n", encoding="utf-8"
    )
    unguarded = tmp_path / "unguarded.py"
    unguarded.write_text("def main():\n    pass\n\nmain()\n", encoding="utf-8")

    def check(main: Any) -> bool:
        monkeypatch.setitem(sys.modules, "__main__", main)
        return parallel._main_is_safe_for_spawn()

    assert check(SimpleNamespace(__file__=str(guarded)))
    assert not check(SimpleNamespace(__file__=str(unguarded)))
    assert check(SimpleNamespace())  # no file: interactive, -c, execnet/xdist workers
    assert not check(
        SimpleNamespace(__file__=str(tmp_path / "missing" / "stub.exe" / "__main__.py"))
    )


def test_ties_between_chain_sizes_keep_every_chain_exactly_once(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    populate(store, 7)
    chains = parallel._chains_by_size(store.events_root)
    assert len(chains) == 7 and len({path for _size, _files, path in chains}) == 7
    assert [size for size, _files, _path in chains] == sorted(
        (size for size, _files, _path in chains), reverse=True
    )
    assert all(files == 4 for _size, files, _path in chains)


def test_a_conflict_between_two_active_chains_is_reported_by_the_final_merge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = make_store(tmp_path)
    populate(store, 3)
    append_chain(
        store, "lease-x1", ("REQUESTED", "ACTIVE"), resources=write_claim("src/a.py"), task_id="TX1"
    )
    append_chain(
        store, "lease-x2", ("REQUESTED", "ACTIVE"), resources=write_claim("src/a.py"), task_id="TX2"
    )
    serial = store._replay_serial()
    assert serial.status == "FAIL"
    assert any(issue.code == "ACTIVE_LEASE_RESOURCE_CONFLICT" for issue in serial.issues)
    _enable(monkeypatch)
    assert store.replay() == serial
