"""DEVX-022 S3b: lease events store large custody tables as content-addressed blobs.

These tests pin the STORED form only. Business validation of publication executions is covered by
the arch_005/devx015 suites; here it is replaced by no-ops so that synthetic executions can carry a
custody table at the two fixed positions.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.platform.architecture import parallel_control_kernel as kernel
from ai_trading_system.platform.architecture import workflow_coordination as coordination
from ai_trading_system.platform.architecture.parallel_control import ParallelControlError

ROOT = Path(__file__).resolve().parents[1]
MARKER = kernel.EXTERNALIZED_ROWS_MARKER
FIELD = kernel.EXTERNALIZED_ROWS_FIELD


@pytest.fixture(autouse=True)
def _synthetic_executions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(coordination, "validate_execution", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        coordination, "_validate_checked_execution_transition", lambda *args, **kwargs: None
    )


def _rows(count: int, tag: str = "a") -> list[dict[str, Any]]:
    return [
        {"relative": f"{tag}/{index}.py", "sha256": f"{index:064x}", "size_bytes": index}
        for index in range(count)
    ]


def _execution(rows: list[Any], *, attempts: bool = False) -> dict[str, Any]:
    capsule = {
        "definition": {"directory": ".git/hooks"},
        "ready": {"schema_version": "ready", "inputs": {FIELD: rows, "runtime_file_count": 3}},
    }
    if attempts:
        return {"state": "RESULT_RECORDED", "publication_attempts": [{"hook_capsule": capsule}]}
    return {"state": "RUNNING", "hook_capsule": capsule}


def _lease(execution: Any, *, state: str = "REQUESTED", lease_id: str = "lease-s3b") -> Any:
    return kernel.ExecutionLease(
        lease_id=lease_id, task_id="T", change_id="C", lane_id="L", actor="a",
        base_commit="0" * 40, change_manifest_sha256="0" * 64, policy_version="p", generation=1,
        previous_lease_id=None, state=state, requested_at="2026-10-05T00:00:00+00:00",
        acquired_at=None, expires_at=None, resources=(), execution=execution,
    )


def _event(lease: Any, *, previous: str | None = None, from_state: str | None = None) -> Any:
    return kernel._lease_event(
        lease=lease, previous_event_id=previous, from_state=from_state, to_state=lease.state,
        occurred_at=datetime(2026, 10, 5, tzinfo=UTC), actor="a", reason_codes=(),
    )


def _store(tmp_path: Path) -> Any:
    policy = kernel.load_parallel_control_policy(
        ROOT / "config/architecture/arch_005_parallel_control_policy.yaml"
    )
    return kernel.FileExecutionLeaseStore(tmp_path / "store", policy=policy)


def _event_path(store: Any, event: Any) -> Path:
    return store.events_root / event.lease.lease_id / f"{event.event_id}.json"


def _stored_inputs(payload: dict[str, Any], *, attempts: bool = False) -> dict[str, Any]:
    execution = payload["lease"]["execution"]
    capsule = (
        execution["publication_attempts"][0]["hook_capsule"] if attempts
        else execution["hook_capsule"]
    )
    return capsule["ready"]["inputs"]


def _legacy(event: Any) -> Any:
    """The same event in the historical stored form (embedded table, v2, original id)."""
    embedded = replace(event, rows_externalized=False, event_id="")
    digest = kernel._canonical_sha256(embedded._body())
    return replace(embedded, event_id=f"lease-event-{digest[:20]}")


@pytest.mark.parametrize("attempts", [False, True])
def test_new_event_stores_a_marker_and_a_blob_and_round_trips(
    tmp_path: Path, attempts: bool
) -> None:
    rows = _rows(2000)
    store = _store(tmp_path)
    event = _event(_lease(_execution(rows, attempts=attempts)))
    store._append_event(event)

    payload = json.loads(_event_path(store, event).read_text(encoding="utf-8"))
    digest = hashlib.sha256(kernel._rows_canonical_bytes(rows)).hexdigest()
    assert _stored_inputs(payload, attempts=attempts)[FIELD] == {
        MARKER: {"sha256": digest, "row_count": 2000}
    }
    assert payload["schema_version"] == kernel.LEASE_EVENT_V3_SCHEMA_VERSION
    assert payload["lease"]["schema_version"] == kernel.LEASE_V3_SCHEMA_VERSION
    blob = store.blobs.path_for(digest)
    assert hashlib.sha256(blob.read_bytes()).hexdigest() == digest
    assert _event_path(store, event).stat().st_size < 20_000  # the table is not in the event

    parsed = kernel.parse_lease_event(payload, blobs=store.blobs)
    assert parsed == event and parsed.rows_externalized
    restored = parsed.lease.execution
    capsule = (
        restored["publication_attempts"][0]["hook_capsule"] if attempts
        else restored["hook_capsule"]
    )
    assert capsule["ready"]["inputs"][FIELD] == rows  # the in-memory value is unchanged
    assert parsed.to_dict() == payload  # compact(expand(stored)) == stored


def test_the_event_id_binds_the_marker_and_the_marker_binds_the_blob(tmp_path: Path) -> None:
    store = _store(tmp_path)
    event = _event(_lease(_execution(_rows(2000))))
    store._append_event(event)
    other = _event(_lease(_execution(_rows(2000, tag="b")), lease_id="lease-other"))
    store._append_event(other)  # a second, valid blob exists
    payload = json.loads(_event_path(store, event).read_text(encoding="utf-8"))
    other_marker = _stored_inputs(other.to_dict())[FIELD]

    swapped = copy.deepcopy(payload)
    _stored_inputs(swapped)[FIELD] = other_marker  # a real, matching blob under another digest
    with pytest.raises(ParallelControlError, match="LEASE_EVENT_HASH"):
        kernel.parse_lease_event(swapped, blobs=store.blobs)

    for bad_count in (1999, 2001, kernel.EXTERNALIZED_ROWS_MINIMUM - 1):
        counted = copy.deepcopy(payload)
        _stored_inputs(counted)[FIELD][MARKER]["row_count"] = bad_count
        with pytest.raises(ParallelControlError, match="LEASE_EXTERNALIZED_ROWS_INVALID"):
            kernel.parse_lease_event(counted, blobs=store.blobs)


def test_a_changed_or_missing_blob_fails_closed(tmp_path: Path) -> None:
    store = _store(tmp_path)
    event = _event(_lease(_execution(_rows(2000))))
    store._append_event(event)
    payload = json.loads(_event_path(store, event).read_text(encoding="utf-8"))
    blob = store.blobs.path_for(_stored_inputs(payload)[FIELD][MARKER]["sha256"])
    original = blob.read_bytes()

    blob.write_bytes(original.replace(b'"a/1.py"', b'"a/9.py"'))
    with pytest.raises(ParallelControlError, match="LEASE_EXTERNALIZED_ROWS_DIGEST"):
        kernel.parse_lease_event(payload, blobs=store.blobs)
    blob.unlink()
    with pytest.raises(ParallelControlError, match="LEASE_EXTERNALIZED_ROWS_UNAVAILABLE"):
        kernel.parse_lease_event(payload, blobs=store.blobs)
    blob.write_bytes(original)
    replay = store.replay()
    assert replay.status == "PASS" and replay.event_count == 1


def test_a_marker_without_a_reader_or_out_of_place_fails_closed(tmp_path: Path) -> None:
    store = _store(tmp_path)
    event = _event(_lease(_execution(_rows(2000))))
    store._append_event(event)
    payload = json.loads(_event_path(store, event).read_text(encoding="utf-8"))
    with pytest.raises(ParallelControlError, match="LEASE_EXTERNALIZED_ROWS_UNAVAILABLE"):
        kernel.parse_lease_event(payload)

    stray = copy.deepcopy(payload)
    stray["lease"]["execution"]["elsewhere"] = _stored_inputs(payload)[FIELD]
    with pytest.raises(ParallelControlError, match="LEASE_EXTERNALIZED_ROWS_INVALID"):
        kernel.parse_lease_event(stray, blobs=store.blobs)

    malformed = copy.deepcopy(payload)
    _stored_inputs(malformed)[FIELD][MARKER]["sha256"] = "../../outside"
    with pytest.raises(ParallelControlError, match="LEASE_EXTERNALIZED_ROWS_INVALID"):
        kernel.parse_lease_event(malformed, blobs=store.blobs)


def test_schema_and_stored_form_are_exclusive(tmp_path: Path) -> None:
    store = _store(tmp_path)
    event = _event(_lease(_execution(_rows(2000))))
    store._append_event(event)
    compact = json.loads(_event_path(store, event).read_text(encoding="utf-8"))
    claimed_v2 = {**compact, "schema_version": "execution_lease_event.v2"}
    with pytest.raises(ParallelControlError, match="LEASE_EVENT_SCHEMA"):
        kernel.parse_lease_event(claimed_v2, blobs=store.blobs)

    embedded = _legacy(event).to_dict()
    assert embedded["schema_version"] == "execution_lease_event.v2"
    claimed_v3 = {**embedded, "schema_version": kernel.LEASE_EVENT_V3_SCHEMA_VERSION}
    with pytest.raises(ParallelControlError, match="LEASE_EVENT_SCHEMA"):
        kernel.parse_lease_event(claimed_v3, blobs=store.blobs)


def test_historical_embedded_events_keep_their_form_and_id() -> None:
    rows = _rows(2000)
    legacy = _legacy(_event(_lease(_execution(rows))))
    payload = json.loads(json.dumps(legacy.to_dict()))
    assert _stored_inputs(payload)[FIELD] == rows
    parsed = kernel.parse_lease_event(payload)  # no blob reader is needed
    assert parsed == legacy and not parsed.rows_externalized
    assert parsed.to_dict() == payload


def test_a_mixed_store_replays_and_shares_one_decoded_table_per_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    rows = _rows(3000)
    old_requested = _legacy(_event(_lease(_execution(rows), lease_id="lease-old")))
    old_active = _legacy(_event(
        _lease(_execution(rows), state="ACTIVE", lease_id="lease-old"),
        previous=old_requested.event_id, from_state="REQUESTED",
    ))
    new_requested = _event(_lease(_execution(rows)))
    new_active = _event(
        _lease(_execution(rows), state="ACTIVE"),
        previous=new_requested.event_id, from_state="REQUESTED",
    )
    for event in (old_requested, old_active, new_requested, new_active):
        store._append_event(event)
    assert len(list(store.blobs.root.rglob("*.json"))) == 1  # one table, one blob

    blob_reads = 0
    real_read_bytes = Path.read_bytes

    def counting(self: Path) -> bytes:
        nonlocal blob_reads
        if self.parent.parent == store.blobs.root:
            blob_reads += 1
        return real_read_bytes(self)

    monkeypatch.setattr(Path, "read_bytes", counting)
    replay = store.replay()
    assert replay.status == "PASS" and replay.event_count == 4 and not replay.issues
    assert blob_reads == 1  # two v3 events, one read inside this call
    heads = {lease.lease_id: lease for lease in replay.lease_heads}
    assert heads["lease-old"].execution == heads["lease-s3b"].execution
    store.replay()
    assert blob_reads == 2  # nothing survives the call


def test_small_tables_stay_embedded_and_write_no_blob(tmp_path: Path) -> None:
    store = _store(tmp_path)
    event = _event(_lease(_execution(_rows(kernel.EXTERNALIZED_ROWS_MINIMUM - 1))))
    assert not event.rows_externalized
    store._append_event(event)
    payload = json.loads(_event_path(store, event).read_text(encoding="utf-8"))
    assert payload["schema_version"] == "execution_lease_event.v2"
    assert isinstance(_stored_inputs(payload)[FIELD], list)
    assert not store.blobs.root.exists()


def test_append_writes_the_blob_first_and_keeps_existing_bytes_honest(tmp_path: Path) -> None:
    store = _store(tmp_path)
    rows = _rows(2000)
    event = _event(_lease(_execution(rows)))
    digest = hashlib.sha256(kernel._rows_canonical_bytes(rows)).hexdigest()
    blob = store.blobs.path_for(digest)
    blob.parent.mkdir(parents=True)
    blob.write_bytes(b"[]")  # a corrupted pre-existing blob is never trusted or overwritten
    with pytest.raises(ParallelControlError, match="LEASE_EXTERNALIZED_ROWS_DIGEST"):
        store._append_event(event)
    assert not _event_path(store, event).exists()
    blob.unlink()
    store._append_event(event)
    store._append_event(event)  # idempotent for the identical event
    tampered = replace(event, reason_codes=("CHANGED",))
    with pytest.raises(ParallelControlError, match="LEASE_EVENT_IMMUTABILITY"):
        store._append_event(tampered)


def test_shared_tables_are_immutable_but_copies_are_plain_lists() -> None:
    rows = kernel._adopt_rows(_rows(2000))
    assert type(rows) is kernel.ExternalizedRows
    for mutate in (
        lambda: rows.append({}), lambda: rows.extend([]), lambda: rows.pop(),
        lambda: rows.__setitem__(0, {}), lambda: rows.sort(), lambda: rows.clear(),
    ):
        with pytest.raises(TypeError, match="immutable"):
            mutate()
    assert type(copy.copy(rows)) is list and type(copy.deepcopy(rows)) is list
    assert copy.deepcopy(rows) == rows and copy.deepcopy(rows)[0] is not rows[0]


REAL_STORE = ROOT / "outputs/architecture/arch_005_s4d_checkout_guard/leases/events"


def _largest_real_event() -> Path | None:
    if not REAL_STORE.is_dir():
        return None
    candidates = [
        path for path in REAL_STORE.glob("*/*.json") if path.stat().st_size > 5_000_000
    ]
    return max(candidates, key=lambda path: path.stat().st_size) if candidates else None


@pytest.mark.skipif(_largest_real_event() is None, reason="no real lease store in this checkout")
def test_a_real_historical_event_converts_and_round_trips_value_for_value(tmp_path: Path) -> None:
    path = _largest_real_event()
    assert path is not None
    payload = json.loads(path.read_text(encoding="utf-8"))
    original = kernel.parse_lease_event(copy.deepcopy(payload))
    assert not original.rows_externalized and original.to_dict() == payload
    store = _store(tmp_path)
    converted = replace(original, rows_externalized=True, event_id="")
    converted = replace(
        converted, event_id=f"lease-event-{kernel._canonical_sha256(converted._body())[:20]}",
    )
    stored = converted.to_dict()
    assert len(json.dumps(stored)) * 10 < len(json.dumps(payload))
    for rows in kernel._qualifying_rows(converted.lease.execution):
        store.blobs.write(rows)
    parsed = kernel.parse_lease_event(json.loads(json.dumps(stored)), blobs=store.blobs)
    assert parsed.lease.execution == original.lease.execution
    assert parsed.to_dict() == stored
    assert os.path.getsize(path) > 20 * len(json.dumps(stored))
