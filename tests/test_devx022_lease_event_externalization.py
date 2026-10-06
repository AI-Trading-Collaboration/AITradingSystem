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
    assert payload["schema_version"] == kernel.LEASE_EVENT_V4_SCHEMA_VERSION
    assert payload["lease"]["schema_version"] == kernel.LEASE_V4_SCHEMA_VERSION
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


def test_an_event_derived_from_a_replayed_head_is_still_externalized(tmp_path: Path) -> None:
    """Regression: a head from replay already holds ExternalizedRows; the next event (e.g. a
    terminal transition built from that head) must still be stored compactly as v3."""
    store = _store(tmp_path)
    requested = _event(_lease(_execution(_rows(2000))))
    store._append_event(requested)
    head = next(lease for lease in store.replay().lease_heads if lease.lease_id == "lease-s3b")
    capsule_rows = head.execution["hook_capsule"]["ready"]["inputs"][FIELD]
    assert type(capsule_rows) is kernel.ExternalizedRows
    following = _event(
        replace(head, state="ACTIVE"), previous=requested.event_id, from_state="REQUESTED",
    )
    assert following.rows_externalized
    store._append_event(following)
    payload = json.loads(_event_path(store, following).read_text(encoding="utf-8"))
    assert payload["schema_version"] == kernel.LEASE_EVENT_V4_SCHEMA_VERSION
    assert MARKER in _stored_inputs(payload)[FIELD]
    assert _event_path(store, following).stat().st_size < 20_000
    replay = store.replay()
    assert replay.status == "PASS" and replay.event_count == 2


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


def _assert_exact_json(value: Any, where: str = "$") -> None:
    """What a strict canonical-JSON consumer accepts: no list/dict subclass anywhere."""
    kind = type(value)
    if value is None or kind in (bool, int, float, str):
        return
    if kind is list:
        for index, item in enumerate(value):
            _assert_exact_json(item, f"{where}[{index}]")
    elif kind is dict:
        for key, item in value.items():
            assert type(key) is str, where
            _assert_exact_json(item, f"{where}.{key}")
    else:
        raise AssertionError(f"{where}: {kind.__module__}.{kind.__qualname__}")


@pytest.mark.parametrize("attempts", [False, True])
def test_public_dicts_of_a_replay_hold_only_exact_json_types(
    tmp_path: Path, attempts: bool
) -> None:
    """Regression, found when the first real v3 events existed: a head taken from replay shares
    ExternalizedRows tables and to_dict() leaked that list subclass into strict canonical-JSON
    consumers, e.g. the named-DQ parent proof, which embeds the replay of the whole real store."""
    from ai_trading_system.contracts.prospective_event_time_evidence import canonical_json_bytes

    store = _store(tmp_path)
    requested = _event(_lease(_execution(_rows(2000), attempts=attempts)))
    store._append_event(requested)
    active = _event(
        _lease(_execution(_rows(2000), attempts=attempts), state="ACTIVE"),
        previous=requested.event_id, from_state="REQUESTED",
    )
    store._append_event(active)
    replay = store.replay()
    assert replay.status == "PASS" and len(replay.active_leases) == 1
    head = replay.lease_heads[0]
    execution = head.execution
    capsule = (
        execution["publication_attempts"][0]["hook_capsule"] if attempts
        else execution["hook_capsule"]
    )
    shared = capsule["ready"]["inputs"][FIELD]
    assert type(shared) is kernel.ExternalizedRows  # the state that produced the leak

    for payload in (replay.to_dict(), head.to_dict(), replay.active_leases[0].to_dict()):
        _assert_exact_json(payload)
        canonical_json_bytes(payload)
    # converting for the public dict never disturbs the shared table of the head itself
    assert type(capsule["ready"]["inputs"][FIELD]) is kernel.ExternalizedRows
    plain = head.to_dict()["execution"]
    plain_capsule = (
        plain["publication_attempts"][0]["hook_capsule"] if attempts else plain["hook_capsule"]
    )
    assert plain_capsule["ready"]["inputs"][FIELD] == list(shared)

    stored = json.loads(_event_path(store, active).read_text(encoding="utf-8"))
    parsed = kernel.parse_lease_event(stored, blobs=store.blobs)
    _assert_exact_json(parsed.to_dict())
    assert parsed.to_dict() == stored  # the stored form is still the compacted one


def test_a_derived_event_keeps_its_stored_form_after_the_public_conversion(
    tmp_path: Path,
) -> None:
    """The conversion is for PUBLIC dicts only: an event built from a replayed head must still
    compact from the shared table (digest reuse), i.e. be written as v3 with a marker."""
    store = _store(tmp_path)
    requested = _event(_lease(_execution(_rows(2000))))
    store._append_event(requested)
    head = store.replay().lease_heads[0]
    head.to_dict()  # a public dict was taken first, as the named-DQ proof does
    following = _event(
        replace(head, state="ACTIVE"), previous=requested.event_id, from_state="REQUESTED",
    )
    assert following.rows_externalized
    _assert_exact_json(following.to_dict())
    assert MARKER in _stored_inputs(following.to_dict())[FIELD]


def test_checkout_telemetry_reads_externalized_events_of_the_lease_store(tmp_path: Path) -> None:
    """Regression: the telemetry loader parsed stored events without the store's blob reader and
    failed closed on the first real v3 event."""
    from ai_trading_system.platform.architecture import checkout_telemetry as telemetry

    root = tmp_path.resolve()
    policy = kernel.load_parallel_control_policy(
        ROOT / "config/architecture/arch_005_parallel_control_policy.yaml"
    )
    store = kernel.FileExecutionLeaseStore(
        root / "outputs/architecture/checkout-guard-test/leases", policy=policy,
    )
    event = _event(_lease(_execution(_rows(2000))))
    store._append_event(event)
    path = _event_path(store, event)
    telemetry_policy = telemetry.load_checkout_telemetry_policy()

    records = telemetry._source_records(
        root, [("lease_event", path)], policy=telemetry_policy, batch_id="batch-s3b",
    )
    assert [row["source_id"] for row in records] == [event.event_id]
    assert records[0]["schema_version"] == kernel.LEASE_EVENT_V4_SCHEMA_VERSION
    loaded = telemetry._load_sources(
        root, records, policy=telemetry_policy, batch_id="batch-s3b",
    )
    assert [(kind, value) for kind, _, value in loaded] == [("lease_event", event)]

    # an event that names a blob which is gone still fails closed instead of being misread
    blob = next(store.blobs.root.rglob("*.json"))
    blob.unlink()
    with pytest.raises(ParallelControlError, match="LEASE_EXTERNALIZED_ROWS_UNAVAILABLE"):
        telemetry._load_sources(root, records, policy=telemetry_policy, batch_id="batch-s3b")


# Callers that parse a STORED event without the store's blob reader. Each one reads events that
# never carry a publication custody table (so they are embedded/v2 by construction). Every other
# call site must pass `blobs=`, because the real store now holds v3 events and a blob-less parse
# fails closed on them (DEVX-022 S3b regression: telemetry and the named-DQ proof).
BLOBLESS_PARSE_SITES = {
    ("src/ai_trading_system/platform/architecture/task_checkpoint.py", "_intent_lease"):
        "checkpoint lease: no hook capsule",
    ("src/ai_trading_system/platform/architecture/task_checkpoint.py",
     "_validate_capture_execution"): "checkpoint lease: no hook capsule",
    ("src/ai_trading_system/platform/architecture/task_checkpoint.py", "_interrupted_lease"):
        "checkpoint lease: no hook capsule",
    ("src/ai_trading_system/platform/architecture/workflow_coordination.py",
     "registered_legacy_terminal_lease"): "retired legacy control root, frozen before S3b",
    ("src/ai_trading_system/prospective_event_time_evidence.py", "_check_snapshot"):
        "captured ACTIVE source-lease event, taken before any hook capsule exists",
}


def _parse_sites() -> dict[tuple[str, str], bool]:
    """(file, enclosing functions) -> whether the call passes `blobs=`."""
    import ast

    sites: dict[tuple[str, str], bool] = {}
    for base in ("src", "scripts"):
        for path in sorted((ROOT / base).rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            parents = {
                child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)
            }
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                func = node.func
                name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
                if name != "parse_lease_event":
                    continue
                chain, current = [], node
                while current in parents:
                    current = parents[current]
                    if isinstance(current, ast.FunctionDef | ast.AsyncFunctionDef):
                        chain.append(current.name)
                key = (path.relative_to(ROOT).as_posix(), "->".join(reversed(chain)))
                sites[key] = any(item.arg == "blobs" for item in node.keywords)
    return sites


def test_every_stored_event_parse_names_a_blob_reader_or_is_justified() -> None:
    sites = _parse_sites()
    unjustified = sorted(
        site for site, has_reader in sites.items()
        if not has_reader and site not in BLOBLESS_PARSE_SITES
    )
    assert not unjustified, (
        "parse_lease_event without blobs= on a store that holds v3 events: "
        f"{unjustified}"
    )
    stale = sorted(site for site in BLOBLESS_PARSE_SITES if sites.get(site) is not False)
    assert not stale, f"allowlisted call site gone or now passes a reader: {stale}"
    for site, reason in BLOBLESS_PARSE_SITES.items():
        assert reason, site


# ---- DEVX-023 S3d: multi-slot, digest-keyed call-scoped memo of the hook-ready validation ------


def _hook_ready_rows(count: int, tag: str) -> list[dict[str, Any]]:
    return [
        {
            "schema_version": "workflow_read_file_custody.v1", "root": f"C:/memo-{tag}",
            "relative": f"pkg/module_{index}.py", "root_identity": [1, 2],
            "parent_identities": {"pkg": [3, 4]}, "identity": [5, index], "size_bytes": 10,
            "sha256": "a" * 64,
        }
        for index in range(count)
    ]


def test_hook_ready_row_memo_keeps_one_slot_per_distinct_table_inside_a_scope() -> None:
    """S3a kept ONE slot. A replay visits one lease chain after another, so chains whose tables
    differ evicted each other; with one slot per distinct digest every table is validated once."""
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _REPLAY_MEMO,
        _hook_ready_distribution_digest,
        _validated_hook_ready_paths,
        replay_validation_scope,
    )

    table_a = kernel._adopt_rows(_hook_ready_rows(1100, "a"))
    table_b = kernel._adopt_rows(_hook_ready_rows(1100, "b"))
    assert type(table_a) is kernel.ExternalizedRows and table_a.sha256 != table_b.sha256
    with replay_validation_scope():
        first_a = _validated_hook_ready_paths(table_a, 1100)
        first_b = _validated_hook_ready_paths(table_b, 1100)
        assert first_a != first_b
        assert _validated_hook_ready_paths(table_a, 1100) is first_a  # served from its own slot
        assert _validated_hook_ready_paths(table_b, 1100) is first_b
        # a later event's copy: equal content, distinct object, same digest -> same slot
        assert _validated_hook_ready_paths(
            kernel._adopt_rows(_hook_ready_rows(1100, "a")), 1100,
        ) is first_a
        # a different count is a different question (it changes the budget rule)
        assert _validated_hook_ready_paths(table_a, 1099) is not first_a

        names_a = [str(path) for path in first_a[2:1100]]
        names_b = [str(path) for path in first_b[2:1100]]
        digest_a = _hook_ready_distribution_digest(names_a, table_a, 1100)
        digest_b = _hook_ready_distribution_digest(names_b, table_b, 1100)
        assert digest_a != digest_b
        assert _hook_ready_distribution_digest(names_a, table_a, 1100) is digest_a
        assert _hook_ready_distribution_digest(names_b, table_b, 1100) is digest_b
    assert _REPLAY_MEMO.get() is None  # nothing survives the scope
    outside = _validated_hook_ready_paths(table_a, 1100)
    assert outside is not first_a and outside == first_a


def _adopted_publication_ready(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """The shape-only synthetic publication attempt (same builder as the ready-binding tests) whose
    two custody tables are digest-carrying, as a replayed v3 event delivers them."""
    import sys

    sys.path.insert(0, str(ROOT / "tests"))
    try:
        from test_devx015_workflow_integration import _synthetic_publication_ready
    finally:
        sys.path.remove(str(ROOT / "tests"))
    # the small synthetic tables qualify for adoption
    monkeypatch.setattr(kernel, "EXTERNALIZED_ROWS_MINIMUM", 1)
    _before, after = _synthetic_publication_ready(tmp_path)
    return _readopt(after["publication_attempts"][-1])


def _readopt(execution: dict[str, Any]) -> dict[str, Any]:
    """A distinct copy whose tables carry fresh digests (a deep copy makes them plain lists)."""
    value = copy.deepcopy(execution)
    inputs = value["hook_capsule"]["ready"]["inputs"]
    inputs["read_file_custodies"] = kernel._adopt_rows(list(inputs["read_file_custodies"]))
    profile = inputs["profile_inspection"]
    profile["captures"] = kernel._adopt_rows(list(profile["captures"]))
    return value


def test_hook_ready_validation_runs_once_per_distinct_input_inside_a_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ai_trading_system.platform.architecture import workflow_contract as contract
    from ai_trading_system.platform.architecture import workflow_coordination as wc

    execution = _adopted_publication_ready(tmp_path, monkeypatch)
    inputs = execution["hook_capsule"]["ready"]["inputs"]
    assert type(inputs["read_file_custodies"]) is kernel.ExternalizedRows
    assert type(inputs["profile_inspection"]["captures"]) is kernel.ExternalizedRows

    full_runs: list[int] = []
    real = wc._validate_publication_profile_binding

    def counting(*args: Any, **kwargs: Any) -> Any:
        full_runs.append(1)
        return real(*args, **kwargs)

    monkeypatch.setattr(wc, "_validate_publication_profile_binding", counting)
    actor = "integration-coordinator"
    wc._validate_hook_capsule(execution, actor=actor)
    wc._validate_hook_capsule(_readopt(execution), actor=actor)
    assert len(full_runs) == 2  # no scope: every call validates in full

    full_runs.clear()
    with wc.replay_validation_scope():
        wc._validate_hook_capsule(execution, actor=actor)
        assert len(full_runs) == 1
        for _ in range(3):  # later events of the same chain: equal inputs, distinct objects
            wc._validate_hook_capsule(_readopt(execution), actor=actor)
        assert len(full_runs) == 1
        # a changed table is validated in full and rejected, memo or not
        broken = _readopt(execution)
        broken["hook_capsule"]["ready"]["inputs"]["read_file_custodies"] = kernel._adopt_rows(
            [{**row, "sha256": "0" * 64} if index == 2 else row for index, row in enumerate(
                broken["hook_capsule"]["ready"]["inputs"]["read_file_custodies"]
            )]
        )
        with pytest.raises((ParallelControlError, contract.WorkflowContractError)):
            wc._validate_hook_capsule(broken, actor=actor)
        assert len(full_runs) == 2
        # the unchanged input is still served from the memo afterwards
        wc._validate_hook_capsule(_readopt(execution), actor=actor)
        assert len(full_runs) == 2
    full_runs.clear()
    wc._validate_hook_capsule(_readopt(execution), actor=actor)
    assert full_runs  # nothing survived the scope


def test_hook_ready_memo_key_changes_with_every_input_the_validator_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ai_trading_system.platform.architecture import workflow_coordination as wc

    execution = _adopted_publication_ready(tmp_path, monkeypatch)
    key = wc._hook_ready_memo_key(execution)
    assert isinstance(key, str) and len(key) == 64
    assert wc._hook_ready_memo_key(_readopt(execution)) == key

    def variant(mutate: Any) -> dict[str, Any]:
        value = copy.deepcopy(execution)
        mutate(value)
        inputs = value["hook_capsule"]["ready"]["inputs"]
        inputs["read_file_custodies"] = kernel._adopt_rows(list(inputs["read_file_custodies"]))
        captures = inputs["profile_inspection"]["captures"]
        inputs["profile_inspection"]["captures"] = kernel._adopt_rows(list(captures))
        return value

    mutations = {
        "a custody row": lambda v: v["hook_capsule"]["ready"]["inputs"]["read_file_custodies"][
            2].__setitem__("sha256", "0" * 64),
        "a capture row": lambda v: v["hook_capsule"]["ready"]["inputs"]["profile_inspection"][
            "captures"][0].__setitem__("sha256", "0" * 64),
        "the request": lambda v: v["request"].__setitem__("cwd", v["request"]["cwd"] + "x"),
        "the request digest": lambda v: v.__setitem__("request_sha256", "0" * 64),
        "the runtime identity": lambda v: v["hook_capsule"]["ready"]["inputs"][
            "runtime_identity"].__setitem__("executable", "C:/elsewhere/python.exe"),
        "the definition": lambda v: v["hook_capsule"]["definition"].__setitem__(
            "python_path", "C:/elsewhere/python.exe"),
        "the created objects": lambda v: v["hook_capsule"]["objects"].pop(),
        "the plan root identity": lambda v: v["checkout_plan"]["plan"]["topology"][
            "candidate_checkout"]["root"].__setitem__("identity", [9, 9]),
    }
    keys = {name: wc._hook_ready_memo_key(variant(mutate)) for name, mutate in mutations.items()}
    assert all(isinstance(value, str) for value in keys.values()), keys
    assert len({key, *keys.values()}) == len(mutations) + 1, keys  # all distinct from each other

    # a short embedded (plain list) table is keyed by its content: the same key as its adopted twin
    plain = copy.deepcopy(execution)
    assert type(plain["hook_capsule"]["ready"]["inputs"]["read_file_custodies"]) is list
    assert wc._hook_ready_memo_key(plain) == key
    # a long plain table is not keyed (hashing it would cost what the memo saves); a
    # digest-carrying one always is, and a malformed record is left to the real validator
    monkeypatch.setattr(wc, "HOOK_READY_KEY_MAX_PLAIN_ROWS", 1)
    assert wc._hook_ready_memo_key(plain) is None
    assert wc._hook_ready_memo_key(execution) == key
    assert wc._hook_ready_memo_key({}) is None
    assert wc._hook_ready_memo_key({"hook_capsule": {"ready": None}}) is None


def test_hook_ready_memo_key_covers_every_input_the_validator_reads_and_stays_pure() -> None:
    """The whole-function memo is sound only while _validate_hook_ready is a pure function of the
    inputs its key names. Pin both: the execution fields it reads and the absence of any clock or
    filesystem observation in it and its helpers."""
    import ast
    import inspect
    import textwrap

    from ai_trading_system.platform.architecture import workflow_coordination as wc

    def parse(function: Any) -> ast.AST:
        return ast.parse(textwrap.dedent(inspect.getsource(function)))

    reads: set[tuple[str, ...]] = set()
    tree = parse(wc._validate_hook_ready_full)
    inner = {id(node.value) for node in ast.walk(tree) if isinstance(node, ast.Subscript)}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Subscript) or id(node) in inner:
            continue  # only maximal chains: execution["a"]["b"] counts once, not as ["a"] too
        chain: list[str] = []
        current: ast.AST = node
        while isinstance(current, ast.Subscript):
            index = current.slice
            if isinstance(index, ast.Constant) and isinstance(index.value, str):
                chain.append(index.value)
            else:
                chain.append("*")
            current = current.value
        if isinstance(current, ast.Name) and current.id == "execution":
            reads.add(tuple(reversed(chain)))
    roots = {chain[0] for chain in reads}
    assert roots == {"hook_capsule", "request", "request_sha256", "checkout_plan"}, roots
    assert {chain for chain in reads if chain[0] == "checkout_plan"} == {
        ("checkout_plan", "plan", "topology", "candidate_checkout", "root", "identity"),
    }
    forbidden = {
        "now", "utcnow", "today", "time", "monotonic", "resolve", "exists", "stat", "lstat",
        "is_file", "is_dir", "read_text", "read_bytes", "open", "iterdir", "getenv", "environ",
    }
    for function in (
        wc._validate_hook_ready_full, wc._validated_hook_ready_paths,
        wc._hook_ready_distribution_digest, wc._validate_publication_profile_binding,
    ):
        names = {
            node.attr for node in ast.walk(parse(function)) if isinstance(node, ast.Attribute)
        } | {node.id for node in ast.walk(parse(function)) if isinstance(node, ast.Name)}
        assert not (names & forbidden), (function.__name__, names & forbidden)


# ---- DEVX-023 S3c: the profile_inspection.captures table is externalized too (schema v4) ---------

CAPTURES = "captures"
V3 = "execution_lease_event.v3"
V4 = "execution_lease_event.v4"


def _captures(count: int) -> list[dict[str, Any]]:
    return [
        {"path": f"D:/repo/outputs/evidence/{index}.json", "sha256": f"{index:064x}",
         "size_bytes": index}
        for index in range(count)
    ]


def _profile_execution(custody: list[Any], captures: list[Any]) -> dict[str, Any]:
    """A late publication execution: the captures table sits at three kinds of position."""
    def capsule() -> dict[str, Any]:
        return {
            "definition": {"directory": ".git/hooks"},
            "ready": {"schema_version": "ready", "inputs": {
                FIELD: custody, "runtime_file_count": 3,
                "profile_inspection": {"status": "PASS", CAPTURES: captures},
            }},
        }

    return {
        "state": "RESULT_RECORDED", "hook_capsule": capsule(),
        "publication_attempts": [{"hook_capsule": capsule()}, {"hook_capsule": capsule()}],
        "publication_stable_observation": {
            "profile_inspection": {"status": "PASS", CAPTURES: captures},
        },
    }


def _capture_tables(payload: dict[str, Any]) -> list[Any]:
    """Every captures value of a stored (or in-memory) execution, in a fixed order."""
    execution = payload["lease"]["execution"] if "lease" in payload else payload
    found = [execution["hook_capsule"]["ready"]["inputs"]["profile_inspection"][CAPTURES]]
    for attempt in execution["publication_attempts"]:
        found.append(attempt["hook_capsule"]["ready"]["inputs"]["profile_inspection"][CAPTURES])
    found.append(execution["publication_stable_observation"]["profile_inspection"][CAPTURES])
    return found


def _s3b_form(event: Any) -> Any:
    """The same event as S3b stored it: schema v3, only the custody table externalized."""
    stored = replace(event, externalized_schema=V3, event_id="")
    return replace(stored, event_id=f"lease-event-{kernel._canonical_sha256(stored._body())[:20]}")


def test_new_events_externalize_the_captures_table_at_every_position(tmp_path: Path) -> None:
    store = _store(tmp_path)
    custody, captures = _rows(2000), _captures(1400)
    event = _event(_lease(_profile_execution(custody, captures)))
    store._append_event(event)

    path = _event_path(store, event)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == V4
    assert payload["lease"]["schema_version"] == "execution_lease.v4"
    digest = hashlib.sha256(kernel._rows_canonical_bytes(captures)).hexdigest()
    markers = _capture_tables(payload)
    assert markers == [{MARKER: {"sha256": digest, "row_count": 1400}}] * 4
    assert path.stat().st_size < 30_000  # neither table is in the event any more
    blobs = sorted(store.blobs.root.rglob("*.json"))
    assert len(blobs) == 2  # the custody table and the (single, shared) captures table

    parsed = kernel.parse_lease_event(payload, blobs=store.blobs)
    assert parsed == event and parsed.externalized_schema == V4
    assert all(table == captures for table in _capture_tables(parsed.lease.execution))
    assert parsed.to_dict() == payload  # compact(expand(stored)) == stored
    replay = store.replay()
    assert replay.status == "PASS" and replay.event_count == 1
    _assert_exact_json(replay.to_dict())  # the public dict still holds exact JSON types


def test_s3b_era_v3_events_stay_valid_and_mix_with_v4_in_one_store(tmp_path: Path) -> None:
    """The real store holds v3 events whose captures table is EMBEDDED (written by S3b code). They
    keep their id and their stored form; only new events use the larger position set."""
    store = _store(tmp_path)
    custody, captures = _rows(2000), _captures(1400)
    old_requested = _s3b_form(_event(_lease(_profile_execution(custody, captures), lease_id="old")))
    old_active = _s3b_form(_event(
        _lease(_profile_execution(custody, captures), state="ACTIVE", lease_id="old"),
        previous=old_requested.event_id, from_state="REQUESTED",
    ))
    new_requested = _event(_lease(_profile_execution(custody, captures), lease_id="new"))
    for event in (old_requested, old_active, new_requested):
        store._append_event(event)

    stored = json.loads(_event_path(store, old_requested).read_text(encoding="utf-8"))
    assert stored["schema_version"] == V3
    assert all(table == captures for table in _capture_tables(stored))  # still embedded
    assert MARKER in _stored_inputs(stored)[FIELD]  # the custody table: a marker, as in S3b
    parsed_old = kernel.parse_lease_event(stored, blobs=store.blobs)
    assert parsed_old == old_requested and parsed_old.to_dict() == stored

    replay = store.replay()
    assert replay.status == "PASS" and replay.event_count == 3 and not replay.issues
    heads = {lease.lease_id: lease for lease in replay.lease_heads}
    assert heads["old"].execution == heads["new"].execution
    old_tables = _capture_tables(heads["old"].execution)
    new_tables = _capture_tables(heads["new"].execution)
    assert all(table == captures for table in old_tables + new_tables)
    # an event derived from an S3b-era head is stored in the new form
    follow = _event(
        replace(heads["old"], state="RELEASED"), previous=old_active.event_id, from_state="ACTIVE",
    )
    assert follow.externalized_schema == V4
    assert MARKER in _capture_tables(follow.to_dict())[0]


def test_captures_markers_fail_closed_outside_their_schema_and_positions(tmp_path: Path) -> None:
    store = _store(tmp_path)
    event = _event(_lease(_profile_execution(_rows(2000), _captures(1400))))
    store._append_event(event)
    payload = json.loads(_event_path(store, event).read_text(encoding="utf-8"))

    claimed_v3 = copy.deepcopy(payload)
    claimed_v3["schema_version"] = V3
    claimed_v3["lease"]["schema_version"] = "execution_lease.v3"
    with pytest.raises(ParallelControlError, match="LEASE_EXTERNALIZED_ROWS_INVALID"):
        kernel.parse_lease_event(claimed_v3, blobs=store.blobs)  # v3 knows no captures marker

    undeclared = copy.deepcopy(payload)
    undeclared["lease"]["execution"]["elsewhere"] = _capture_tables(payload)[0]
    with pytest.raises(ParallelControlError, match="LEASE_EXTERNALIZED_ROWS_INVALID"):
        kernel.parse_lease_event(undeclared, blobs=store.blobs)

    wrong_field = copy.deepcopy(payload)  # a marker under a field that is not a declared table
    inputs = wrong_field["lease"]["execution"]["hook_capsule"]["ready"]["inputs"]
    inputs["profile_inspection"]["other_rows"] = inputs["profile_inspection"].pop(CAPTURES)
    with pytest.raises(ParallelControlError, match="LEASE_EXTERNALIZED_ROWS_INVALID"):
        kernel.parse_lease_event(wrong_field, blobs=store.blobs)

    with pytest.raises(ParallelControlError, match="LEASE_EXTERNALIZED_ROWS_UNAVAILABLE"):
        kernel.parse_lease_event(payload)  # no reader

    digest = _capture_tables(payload)[0][MARKER]["sha256"]
    blob = store.blobs.path_for(digest)
    original = blob.read_bytes()
    blob.unlink()
    with pytest.raises(ParallelControlError, match="LEASE_EXTERNALIZED_ROWS_UNAVAILABLE"):
        kernel.parse_lease_event(payload, blobs=store.blobs)
    blob.write_bytes(original.replace(b'"size_bytes":1', b'"size_bytes":9'))
    with pytest.raises(ParallelControlError, match="LEASE_EXTERNALIZED_ROWS_DIGEST"):
        kernel.parse_lease_event(payload, blobs=store.blobs)
    blob.write_bytes(original)

    no_marker = copy.deepcopy(payload)
    no_marker["lease"]["execution"] = _profile_execution(_rows(10), _captures(10))
    with pytest.raises(ParallelControlError, match="LEASE_EVENT_SCHEMA"):
        kernel.parse_lease_event(no_marker, blobs=store.blobs)  # v4 means "a marker is present"


def test_small_captures_stay_embedded_and_the_custody_table_still_externalizes(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    captures = _captures(kernel.EXTERNALIZED_ROWS_MINIMUM - 1)
    event = _event(_lease(_profile_execution(_rows(2000), captures)))
    assert event.rows_externalized and event.externalized_schema == V4
    store._append_event(event)
    payload = json.loads(_event_path(store, event).read_text(encoding="utf-8"))
    assert payload["schema_version"] == V4
    assert _capture_tables(payload) == [captures] * 4  # embedded: below the minimum
    assert MARKER in _stored_inputs(payload)[FIELD]
    assert kernel.parse_lease_event(payload, blobs=store.blobs) == event


# ---- DEVX-023 W: one locked compound operation replays the lease store once -------------------


def _count_replays(monkeypatch: pytest.MonkeyPatch, store: Any) -> list[int]:
    # the counting tests assume plain reuse: verify mode (set for whole-suite runs) re-replays
    monkeypatch.delenv("AITS_LEASE_SECTION_REPLAY_VERIFY", raising=False)
    calls: list[int] = []
    real = type(store)._replay_uncached

    def counting(self: Any) -> Any:
        calls.append(1)
        return real(self)

    monkeypatch.setattr(type(store), "_replay_uncached", counting)
    return calls


def _active_chain(store: Any, lease_id: str) -> Any:
    """REQUESTED -> ACTIVE for one lease, written through `store`; returns the ACTIVE event."""
    requested = _event(_lease(None, lease_id=lease_id))
    active = _event(
        _lease(None, state="ACTIVE", lease_id=lease_id),
        previous=requested.event_id, from_state="REQUESTED",
    )
    store._append_event(requested)
    store._append_event(active)
    return active


NOW = datetime(2026, 10, 6, tzinfo=UTC)


def test_a_locked_section_replays_once_while_nothing_is_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    _active_chain(store, "lease-w")
    calls = _count_replays(monkeypatch, store)

    store.replay()
    store.replay()
    assert len(calls) == 2  # outside a section every call is a fresh replay
    calls.clear()
    with store.atomic(actor="a", now=NOW):
        a, b, c = store.replay(), store.replay(), store.replay()
        assert len(calls) == 1 and a is b is c  # the section's own replay, shared while valid
        with store.atomic(actor="a", now=NOW):  # a nested section is the same section
            assert store.replay() is a
        assert len(calls) == 1
    assert len(calls) == 1
    store.replay()
    assert len(calls) == 2  # nothing survives the section
    with store.atomic(actor="a", now=NOW):
        assert store.replay() is not a  # a new section starts empty
    assert len(calls) == 3


def test_a_write_ends_the_reuse_inside_the_section(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    _active_chain(store, "lease-w")
    calls = _count_replays(monkeypatch, store)
    with store.atomic(actor="a", now=NOW):
        before = store.replay()
        _active_chain(store, "lease-w2")  # a write through the section's own store
        after = store.replay()
        assert len(calls) == 2 and after is not before
        assert {lease_id for lease_id, _ in after.head_event_ids} == {"lease-w", "lease-w2"}
        assert store.replay() is after and len(calls) == 2

    # a write through ANOTHER instance of the same root ends the reuse too
    other = _store(tmp_path)
    with store.atomic(actor="a", now=NOW):
        held = store.replay()
        _active_chain(other, "lease-w3")
        fresh = store.replay()
        assert fresh is not held
        assert {lease_id for lease_id, _ in fresh.head_event_ids} == {
            "lease-w", "lease-w2", "lease-w3",
        }


def test_an_event_file_that_appears_or_changes_ends_the_reuse_too(tmp_path: Path) -> None:
    """Defense in depth: the arbiter keeps other writers out, but a file that shows up anyway
    (a writer that bypassed the generation counter) must never be answered from the old replay."""
    store = _store(tmp_path)
    _active_chain(store, "lease-w")
    with store.atomic(actor="a", now=NOW):
        before = store.replay()
        stray = store.events_root / "lease-w" / "lease-event-stray.json"
        stray.write_text("{}", encoding="utf-8")
        after = store.replay()
        assert after is not before
        assert after.status == "FAIL" and after.issues  # the stray file is reported, not hidden


def test_section_replays_are_per_thread_and_can_be_verified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import threading

    store = _store(tmp_path)
    _active_chain(store, "lease-w")
    calls = _count_replays(monkeypatch, store)
    seen: list[Any] = []
    with store.atomic(actor="a", now=NOW):
        mine = store.replay()
        worker = threading.Thread(target=lambda: seen.append(store.replay()))
        worker.start()
        worker.join()
        assert seen[0] is not mine  # a thread that does not hold the arbiter never reuses it
        assert len(calls) == 2

    monkeypatch.setenv("AITS_LEASE_SECTION_REPLAY_VERIFY", "1")
    with store.atomic(actor="a", now=NOW):
        first = store.replay()
        assert store.replay() is first  # verified against a fresh replay, equal
        head = first.lease_heads[0]
        object.__setattr__(head, "state", "RELEASED")  # a caller that mutated the shared head
        with pytest.raises(ParallelControlError, match="LEASE_SECTION_REPLAY_STALE"):
            store.replay()


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
