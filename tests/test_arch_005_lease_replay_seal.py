"""DEVX-023 S: the sealed replay of terminal lease chains.

The serial replay is the authority. These tests pin the trust model (DEVX-023 sections 10.3, 10.9):
a sealed replay equals the serial replay field by field (issues included) on stores of every shape;
every byte of a sealed chain (events AND the row-table blobs they name) is re-checked, so any change
sends that chain back to the full validation; every problem with the seal means "no seal"; the seal
is bound to a fingerprint of the code and policy; and the switch is off unless set.
"""

from __future__ import annotations

import builtins
import hashlib
import json
import random
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from lease_parallel_replay_support import (
    append_chain,
    make_event,
    make_lease,
    make_store,
    populate,
    write_claim,
)

from ai_trading_system.platform.architecture import lease_replay_seal as seal
from ai_trading_system.platform.architecture import parallel_control_kernel as kernel
from ai_trading_system.platform.architecture import workflow_coordination as coordination

NOW = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(seal.SEAL_ENV, raising=False)
    monkeypatch.delenv("AITS_LEASE_PARALLEL_REPLAY", raising=False)
    # Synthetic executions: the business validators are covered elsewhere (see the S3b tests).
    monkeypatch.setattr(coordination, "validate_execution", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        coordination, "_validate_checked_execution_transition", lambda *args, **kwargs: None
    )


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


def _store(tmp_path: Path, *, chains: int = 6, with_blobs: bool = True) -> Any:
    store = make_store(tmp_path)
    populate(store, chains)
    if with_blobs:
        big = kernel.EXTERNALIZED_ROWS_MINIMUM + 20
        append_chain(store, "lease-blob-a", execution=_execution(_rows(big, "a")))
        append_chain(store, "lease-blob-b", execution=_execution(_rows(big + 1, "b")))
    return store


def _build(store: Any) -> dict[str, Any]:
    document, _report = seal.build_seal_document(store, created_at=NOW, created_by="test")
    seal.write_seal(store.root / seal.SEAL_FILE_NAME, document)
    return document


def _loaded(store: Any) -> seal.LoadedSeal:
    loaded, reason = seal.load_seal(
        store.root / seal.SEAL_FILE_NAME, fingerprint=seal.kernel_fingerprint()
    )
    assert loaded is not None, reason
    return loaded


def _sealed(store: Any) -> tuple[kernel.LeaseReplay, seal.SealedReplayStats]:
    return seal.sealed_replay(events_root=store.events_root, blobs=store.blobs, seal=_loaded(store))


def _same(store: Any) -> seal.SealedReplayStats:
    """The sealed replay equals the serial replay field by field; returns how it was composed."""
    sealed, stats = _sealed(store)
    full = store._replay_serial()
    assert sealed.to_dict() == full.to_dict()
    assert sealed == full
    return stats


# ------------------------------------------------------------------------------- equivalence


def test_a_fully_sealed_store_replays_identically_and_takes_every_terminal_chain_from_the_seal(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    document = _build(store)
    assert set(document["chains"]) == {f"lease-p{i:03d}" for i in range(6)} | {
        "lease-blob-a",
        "lease-blob-b",
    }
    stats = _same(store)
    assert stats.full_chains == 0 and stats.sealed_chains == 8
    assert stats.sealed_events == store._replay_serial().event_count


def test_chains_that_are_not_terminal_are_never_sealed_and_are_validated_in_full(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path, with_blobs=False)
    append_chain(store, "lease-live", ("REQUESTED", "ACTIVE"), resources=write_claim("path:x"))
    append_chain(store, "lease-blocked", ("REQUESTED", "BLOCKED"))
    document = _build(store)
    assert "lease-live" not in document["chains"] and "lease-blocked" not in document["chains"]
    assert document["source"]["skipped_non_terminal"] == 2
    stats = _same(store)
    assert stats.full_reasons == {"lease-live": "unsealed", "lease-blocked": "unsealed"}


def test_the_terminal_states_come_from_the_kernels_own_transition_table() -> None:
    assert seal.TERMINAL_STATES == {"RELEASED", "REASSIGNED"}
    assert "BLOCKED" not in seal.TERMINAL_STATES  # BLOCKED -> RELEASED is still allowed


def test_a_chain_added_after_the_seal_is_validated_in_full_and_the_result_is_unchanged(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    _build(store)
    append_chain(store, "lease-new", resources=write_claim("path:new"))
    stats = _same(store)
    assert stats.full_reasons == {"lease-new": "unsealed"}


def test_an_empty_or_missing_events_directory_is_left_to_the_normal_replay(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    document, report = seal.build_seal_document(store, created_at=NOW, created_by="test")
    assert document["chains"] == {} and report.chains == 0
    seal.write_seal(store.root / seal.SEAL_FILE_NAME, document)
    assert _same(store).sealed_chains == 0


def test_a_conflict_between_two_active_chains_is_reported_with_the_seal_too(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path, with_blobs=False)
    _build(store)
    append_chain(store, "lease-act-a", ("REQUESTED", "ACTIVE"), resources=write_claim("path:same"))
    append_chain(store, "lease-act-b", ("REQUESTED", "ACTIVE"), resources=write_claim("path:same"))
    sealed, _stats = _sealed(store)
    assert sealed.status == "FAIL" and sealed.issues == store._replay_serial().issues
    assert any(issue.code == "ACTIVE_LEASE_RESOURCE_CONFLICT" for issue in sealed.issues)


def test_the_blob_expanded_execution_of_a_sealed_head_is_the_same_object_value(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    _build(store)
    sealed, _ = _sealed(store)
    heads = {lease.lease_id: lease for lease in sealed.lease_heads}
    rows = heads["lease-blob-a"].execution["hook_capsule"]["ready"]["inputs"][
        kernel.EXTERNALIZED_ROWS_FIELD
    ]
    assert (
        type(rows) is kernel.ExternalizedRows and len(rows) == kernel.EXTERNALIZED_ROWS_MINIMUM + 20
    )


# ------------------------------------------------------------------------------- mutations


def _flip_byte(path: Path, offset: int = 40) -> None:
    raw = bytearray(path.read_bytes())
    raw[offset] = (raw[offset] + 1) % 256 if raw[offset] != ord("0") else ord("1")
    path.write_bytes(bytes(raw))


def _chain_files(store: Any, name: str) -> list[Path]:
    return sorted((store.events_root / name).glob("*.json"))


def _mutations() -> dict[str, Any]:
    def flip_middle(store: Any, name: str) -> None:
        _flip_byte(_chain_files(store, name)[1])

    def flip_head(store: Any, name: str) -> None:
        _flip_byte(_chain_files(store, name)[-1])

    def truncate(store: Any, name: str) -> None:
        path = _chain_files(store, name)[1]
        path.write_bytes(path.read_bytes()[:-30])

    def delete_file(store: Any, name: str) -> None:
        _chain_files(store, name)[1].unlink()

    def add_valid_event(store: Any, name: str) -> None:
        events = [
            make_event(make_lease(name, "RELEASED"), previous=None, from_state=None, minute=99)
        ]
        store._append_event(events[0])

    def add_garbage_file(store: Any, name: str) -> None:
        (store.events_root / name / "extra-event.json").write_text("{}", encoding="utf-8")

    def rename_file(store: Any, name: str) -> None:
        path = _chain_files(store, name)[1]
        path.rename(path.with_name("lease-event-zzzzzzzzzzzzzzzzzzzz.json"))

    def swap_with_other_chain(store: Any, name: str) -> None:
        other = _chain_files(store, "lease-p001")[1]
        target = _chain_files(store, name)[1]
        target.write_bytes(other.read_bytes())

    def replace_with_whitespace_variant(store: Any, name: str) -> None:
        path = _chain_files(store, name)[1]
        path.write_bytes(path.read_bytes() + b"\n")

    def empty_a_file(store: Any, name: str) -> None:
        _chain_files(store, name)[1].write_bytes(b"")

    return {
        "flip_middle_byte": flip_middle,
        "flip_head_byte": flip_head,
        "truncate_file": truncate,
        "delete_file": delete_file,
        "add_valid_event": add_valid_event,
        "add_garbage_file": add_garbage_file,
        "rename_file": rename_file,
        "replace_with_other_chains_event": swap_with_other_chain,
        "append_whitespace": replace_with_whitespace_variant,
        "empty_file": empty_a_file,
    }


# A foreign event (another lease id inside this directory) is not a per-chain mutation: see the test
# of the bail-out below. Every other mutation keeps the directory-wise equivalence.
_PER_CHAIN_MUTATIONS = sorted(set(_mutations()) - {"replace_with_other_chains_event"})


@pytest.mark.parametrize("mutation", _PER_CHAIN_MUTATIONS)
def test_any_mutation_of_a_sealed_chain_sends_it_to_full_validation_with_the_serial_result(
    tmp_path: Path, mutation: str
) -> None:
    store = _store(tmp_path)
    _build(store)
    _mutations()[mutation](store, "lease-p003")
    stats = _same(store)  # the field-by-field equality (issues included) is the point of the test
    assert "lease-p003" in stats.full_reasons and stats.full_reasons["lease-p003"] != "unsealed"
    assert stats.sealed_chains == 7  # every other chain is still taken from the seal


@pytest.mark.parametrize("mutation", _PER_CHAIN_MUTATIONS)
def test_the_same_mutations_on_an_unsealed_chain_change_nothing_about_the_equivalence(
    tmp_path: Path, mutation: str
) -> None:
    store = _store(tmp_path, with_blobs=False)
    _build(store)
    append_chain(store, "lease-p050")  # added after the seal: unsealed
    _mutations()[mutation](store, "lease-p050")
    stats = _same(store)
    assert stats.full_reasons["lease-p050"] == "unsealed"


@pytest.mark.parametrize("sealed_target", [True, False])
def test_an_event_of_another_lease_inside_a_validated_directory_makes_the_sealed_route_decline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sealed_target: bool
) -> None:
    """The serial replay groups events by the lease id INSIDE them. A foreign event in a changed
    directory may belong to a sealed chain there; the directory-wise route cannot reproduce that,
    so it must not guess: the whole sealed route is abandoned and the normal replay answers."""
    store = _store(tmp_path, with_blobs=False)
    _build(store)
    victim = "lease-p003"
    if not sealed_target:
        append_chain(store, "lease-p050")
        victim = "lease-p050"
    _mutations()["replace_with_other_chains_event"](store, victim)
    with pytest.raises(seal.SealBailout) as bailout:
        _sealed(store)
    assert bailout.value.reason == f"lease_id_differs_from_directory:{victim}"
    monkeypatch.setenv(seal.SEAL_ENV, "1")
    serial = store._replay_serial()
    assert store.replay() == serial  # the production path declines and the normal replay answers
    assert serial.status == "FAIL"  # and that answer reports the duplicate / broken chains
    assert seal.LAST_SEAL_REASON == f"bailout:lease_id_differs_from_directory:{victim}"
    assert seal.LAST_SEAL_STATS is None
    verified = seal.verify(store)
    assert not verified.ok and (verified.reason or "").startswith("bailout:")


def test_build_counts_the_directories_that_hold_a_foreign_event(tmp_path: Path) -> None:
    store = _store(tmp_path, with_blobs=False)
    _mutations()["replace_with_other_chains_event"](store, "lease-p003")
    _document, report = seal.build_seal_document(store, created_at=NOW, created_by="test")
    assert report.foreign_event_directories == 1


def test_a_changed_chain_file_is_never_taken_from_the_seal_whatever_its_byte(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path, chains=3, with_blobs=False)
    _build(store)
    generator = random.Random(20261009)
    files = [
        path
        for name in ("lease-p000", "lease-p001", "lease-p002")
        for path in _chain_files(store, name)
    ]
    for path in generator.sample(files, 6):
        original = path.read_bytes()
        offset = generator.randrange(len(original))
        mutated = bytearray(original)
        mutated[offset] ^= 0x01
        path.write_bytes(bytes(mutated))
        try:
            stats = _same(store)
            assert path.parent.name in stats.full_reasons
        finally:
            path.write_bytes(original)
    assert _same(store).full_chains == 0  # restored: the seal applies again


def test_a_corrupted_or_missing_blob_sends_the_chain_to_full_validation(tmp_path: Path) -> None:
    store = _store(tmp_path)
    document = _build(store)
    blobs = document["chains"]["lease-blob-a"]["blobs"]
    assert len(blobs) == 1
    digest = blobs[0][0]
    path = store.blobs.path_for(digest)
    original = path.read_bytes()
    path.write_bytes(original[:-5] + b"xxxxx")
    stats = _same(store)
    assert stats.full_reasons["lease-blob-a"] == "blob_changed"
    path.unlink()
    stats = _same(store)
    assert stats.full_reasons["lease-blob-a"] == "blob_changed"
    path.write_bytes(original)
    assert _same(store).full_chains == 0


def test_the_blob_hash_is_checked_once_per_call_not_once_per_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = make_store(tmp_path)
    shared = _execution(_rows(kernel.EXTERNALIZED_ROWS_MINIMUM + 5))
    append_chain(store, "lease-s1", execution=shared)
    append_chain(store, "lease-s2", execution=shared)
    _build(store)
    reads: list[Path] = []
    original = Path.read_bytes

    def counting(self: Path) -> bytes:
        if "blobs" in self.parts:
            reads.append(self)
        return original(self)

    monkeypatch.setattr(Path, "read_bytes", counting)
    _same(store)
    sealed_reads = [path for path in reads if path.suffix == ".json"]
    assert len(set(sealed_reads)) == 1  # two chains name the same table


# --------------------------------------------------------------------- trust: the seal itself


def _write(store: Any, mutate: Any) -> None:
    path = store.root / seal.SEAL_FILE_NAME
    document = json.loads(path.read_text(encoding="utf-8"))
    mutate(document)
    path.write_text(json.dumps(document), encoding="utf-8")


def _reason(store: Any) -> str | None:
    return seal.load_seal(store.root / seal.SEAL_FILE_NAME, fingerprint=seal.kernel_fingerprint())[
        1
    ]


def test_a_valid_seal_loads_and_every_problem_with_the_seal_means_no_seal(tmp_path: Path) -> None:
    store = _store(tmp_path)
    assert _reason(store) == "seal_absent"
    _build(store)
    assert _reason(store) is None
    path = store.root / seal.SEAL_FILE_NAME

    path.write_bytes(b"\xff\xfe not json")
    assert _reason(store) == "seal_not_json"
    path.write_text("[1, 2]", encoding="utf-8")
    assert _reason(store) == "seal_schema"
    path.unlink()
    path.mkdir()  # a directory where the file should be: unreadable, not a crash
    assert _reason(store) == "seal_unreadable"
    path.rmdir()

    _build(store)
    _write(store, lambda d: d.update(schema_version="lease_replay_seal.v0"))
    assert _reason(store) == "seal_schema"

    _build(store)
    _write(store, lambda d: d["chains"]["lease-p000"].update(head_state="ACTIVE"))
    assert _reason(store) == "seal_digest"  # any edit of the body breaks the digest

    _build(store)
    _write(store, lambda d: d.update(seal_sha256="0" * 64))
    assert _reason(store) == "seal_digest"

    _build(store)
    _write(store, lambda d: d.update(kernel_fingerprint="0" * 64))
    assert _reason(store) == "seal_digest"  # the fingerprint is part of the digested body


def _forge(store: Any, mutate: Any) -> None:
    """Edit the body AND recompute the digest, as a writer that knows the format would."""
    path = store.root / seal.SEAL_FILE_NAME
    document = json.loads(path.read_text(encoding="utf-8"))
    document.pop("seal_sha256")
    mutate(document)
    document["seal_sha256"] = seal._canonical_digest(document)
    path.write_text(json.dumps(document), encoding="utf-8")


def test_a_forged_but_well_digested_seal_is_rejected_when_it_is_not_bound_to_this_code(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    _build(store)
    _forge(store, lambda d: d.update(kernel_fingerprint="0" * 64))
    assert _reason(store) == "seal_fingerprint"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d["chains"]["lease-p000"].update(head_state="BLOCKED"),
        lambda d: d["chains"]["lease-p000"].update(head_state="ACTIVE"),
        lambda d: d["chains"]["lease-p000"].update(event_count=99),
        lambda d: d["chains"]["lease-p000"]["files"].reverse(),
        lambda d: d["chains"]["lease-p000"].pop("blobs"),
        lambda d: d["chains"]["lease-p000"].update(extra=1),
        lambda d: d["chains"]["lease-p000"].update(chain_sha256="0" * 64),
        lambda d: d["chains"]["lease-p000"]["files"][0].__setitem__(1, "z" * 64),
        lambda d: d["chains"].update({"": d["chains"]["lease-p000"]}),
        lambda d: d.update(chains=[]),
        lambda d: d.update(source=3),
    ],
)
def test_a_structurally_wrong_seal_is_malformed_even_with_a_correct_digest(
    tmp_path: Path, mutate: Any
) -> None:
    store = _store(tmp_path)
    _build(store)
    _forge(store, mutate)
    assert _reason(store) == "seal_malformed"


def test_a_head_that_no_longer_matches_the_seal_is_not_trusted(tmp_path: Path) -> None:
    store = _store(tmp_path, with_blobs=False)
    _build(store)

    def point_to_the_first_event(document: dict[str, Any]) -> None:
        entry = document["chains"]["lease-p002"]
        first = entry["files"][0][0].removesuffix(".json")
        entry["head_event_id"] = first
        entry["chain_sha256"] = seal._chain_digest(
            first,
            entry["head_state"],
            tuple((n, h) for n, h in entry["files"]),
            tuple((b, r) for b, r in entry["blobs"]),
        )

    _forge(store, point_to_the_first_event)
    stats = _same(store)
    assert stats.full_reasons["lease-p002"] in {"head_mismatch", "head_invalid"}


# ------------------------------------------------------------------------- fingerprint


def _mini_repository(tmp_path: Path) -> Path:
    root = tmp_path / "mini"
    for relative in (*seal.FINGERPRINT_FILES,):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"{relative}\n", encoding="utf-8")
    for directory in seal.FINGERPRINT_DIRECTORIES:
        base = root / directory
        base.mkdir(parents=True, exist_ok=True)
        (base / "a.py").write_text("a = 1\n", encoding="utf-8")
        (base / "sub").mkdir(exist_ok=True)
        (base / "sub" / "b.py").write_text("b = 1\n", encoding="utf-8")
    return root


def test_the_fingerprint_covers_every_source_and_policy_and_ignores_caches(tmp_path: Path) -> None:
    root = _mini_repository(tmp_path)
    baseline = seal.kernel_fingerprint(root)
    assert seal.kernel_fingerprint(root) == baseline  # stable
    pycache = root / seal.FINGERPRINT_DIRECTORIES[0] / "__pycache__"
    pycache.mkdir()
    (pycache / "a.cpython-311.py").write_text("stale\n", encoding="utf-8")
    assert seal.kernel_fingerprint(root) == baseline  # caches are not code
    for relative in (
        f"{seal.FINGERPRINT_DIRECTORIES[0]}/a.py",
        f"{seal.FINGERPRINT_DIRECTORIES[0]}/sub/b.py",
        f"{seal.FINGERPRINT_DIRECTORIES[1]}/a.py",
        *seal.FINGERPRINT_FILES,
    ):
        target = root / relative
        original = target.read_bytes()
        target.write_bytes(original + b"# changed\n")
        assert seal.kernel_fingerprint(root) != baseline, relative
        target.write_bytes(original)
    assert seal.kernel_fingerprint(root) == baseline
    (root / seal.FINGERPRINT_DIRECTORIES[1] / "new.py").write_text("n = 1\n", encoding="utf-8")
    assert seal.kernel_fingerprint(root) != baseline  # a NEW source file changes it as well


def test_the_fingerprint_follows_the_interpreter_minor_version_and_the_seal_code_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _mini_repository(tmp_path)
    baseline = seal.kernel_fingerprint(root)
    monkeypatch.setattr(seal, "SEAL_CODE_VERSION", seal.SEAL_CODE_VERSION + 1)
    assert seal.kernel_fingerprint(root) != baseline
    monkeypatch.undo()
    fake = type("V", (), {"major": 3, "minor": 99})()
    monkeypatch.setattr(seal.sys, "version_info", fake)
    assert seal.kernel_fingerprint(root) != baseline


def test_a_missing_policy_is_part_of_the_fingerprint(tmp_path: Path) -> None:
    root = _mini_repository(tmp_path)
    baseline = seal.kernel_fingerprint(root)
    (root / seal.FINGERPRINT_FILES[1]).unlink()
    assert seal.kernel_fingerprint(root) != baseline


def test_the_real_fingerprint_covers_the_modules_a_replay_executes() -> None:
    covered = {
        path.relative_to(seal.REPOSITORY_ROOT).as_posix()
        for directory in seal.FINGERPRINT_DIRECTORIES
        for path in (seal.REPOSITORY_ROOT / directory).rglob("*.py")
    }
    for module in (
        "workflow_coordination",
        "parallel_control_kernel",
        "workflow_contract",
        "workflow_integration",
        "lease_replay_seal",
    ):
        assert f"src/ai_trading_system/platform/architecture/{module}.py" in covered
    assert "src/ai_trading_system/platform/artifacts/writer.py" in covered
    for relative in seal.FINGERPRINT_FILES:
        assert (seal.REPOSITORY_ROOT / relative).is_file(), relative


# ---------------------------------------------------------------------------- the switch


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, False),
        ("", False),
        ("0", False),
        ("off", False),
        ("False", False),
        ("no", False),
        ("garbage", False),
        ("2", False),
        ("1", True),
        ("on", True),
        ("TRUE", True),
        ("yes", True),
        (" auto ", True),
    ],
)
def test_the_switch_is_off_unless_explicitly_on(raw: str | None, expected: bool) -> None:
    environ = {} if raw is None else {seal.SEAL_ENV: raw}
    assert seal.seal_enabled(environ) is expected


def test_with_the_switch_unset_nothing_is_imported_or_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    _build(store)

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("the seal must not be consulted")

    monkeypatch.setattr(seal, "replay_if_enabled", forbidden)
    monkeypatch.setattr(seal, "load_seal", forbidden)
    assert store.replay() == store._replay_serial()


def test_with_the_switch_on_the_store_replay_uses_the_seal_and_equals_the_serial_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    serial = store._replay_serial()
    _build(store)
    monkeypatch.setenv(seal.SEAL_ENV, "1")
    assert store.replay() == serial
    assert seal.LAST_SEAL_REASON is None
    assert seal.LAST_SEAL_STATS is not None and seal.LAST_SEAL_STATS.sealed_chains == 8


def test_the_off_values_force_the_full_replay_even_when_a_valid_seal_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    _build(store)
    monkeypatch.setenv(seal.SEAL_ENV, "off")
    assert store.replay() == store._replay_serial()
    assert seal.LAST_SEAL_STATS is None


@pytest.mark.parametrize(
    "damage",
    ["absent", "not_json", "fingerprint"],
)
def test_without_a_usable_seal_the_switch_falls_back_to_the_normal_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    store = _store(tmp_path)
    serial = store._replay_serial()
    path = store.root / seal.SEAL_FILE_NAME
    if damage != "absent":
        _build(store)
    if damage == "not_json":
        path.write_text("{{{", encoding="utf-8")
    if damage == "fingerprint":
        monkeypatch.setattr(seal, "kernel_fingerprint", lambda *args, **kwargs: "f" * 64)
    monkeypatch.setenv(seal.SEAL_ENV, "1")
    assert store.replay() == serial
    assert seal.LAST_SEAL_STATS is None and seal.LAST_SEAL_REASON is not None


def test_an_unexpected_failure_of_the_sealed_route_falls_back_to_the_normal_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    serial = store._replay_serial()
    _build(store)

    def boom(**kwargs: Any) -> Any:
        raise RuntimeError("boom")

    monkeypatch.setattr(seal, "sealed_replay", boom)
    monkeypatch.setenv(seal.SEAL_ENV, "1")
    assert store.replay() == serial
    assert seal.LAST_SEAL_REASON == "sealed_failed:RuntimeError"


def test_a_process_that_refuses_the_import_stays_on_the_normal_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    serial = store._replay_serial()
    _build(store)
    real_import = builtins.__import__

    def refusing(name: str, *args: Any, **kwargs: Any) -> Any:
        if name.endswith("lease_replay_seal") or (
            args and len(args) >= 3 and args[2] and "lease_replay_seal" in args[2]
        ):
            raise ValueError("NAMED_BOOTSTRAP_UNREVIEWED_IMPORT: lease_replay_seal")
        return real_import(name, *args, **kwargs)

    monkeypatch.setenv(seal.SEAL_ENV, "1")
    monkeypatch.setattr(builtins, "__import__", refusing)
    assert store.replay() == serial


def test_the_sealed_route_takes_precedence_over_the_parallel_route_and_both_equal_serial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    serial = store._replay_serial()
    _build(store)
    monkeypatch.setenv(seal.SEAL_ENV, "1")
    monkeypatch.setenv("AITS_LEASE_PARALLEL_REPLAY", "4")
    assert store.replay() == serial
    assert seal.LAST_SEAL_STATS is not None  # the seal answered; the pool was not needed


def test_the_section_reuse_of_a_replay_still_works_with_the_seal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    _build(store)
    monkeypatch.setenv(seal.SEAL_ENV, "1")
    first = store.replay()
    second = store.replay()
    assert first == second == store._replay_serial()


# ------------------------------------------------------------- the validation of a sealed head


def test_only_the_sealed_head_skips_the_semantic_execution_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    _build(store)
    calls: list[str] = []

    def counting(lease: Any, *args: Any, **kwargs: Any) -> None:
        calls.append(lease.lease_id)

    monkeypatch.setattr(coordination, "validate_execution", counting)
    _sealed(store)
    assert calls == []  # sealed heads: structure, schema and event id only
    calls.clear()
    store._replay_serial()
    assert sorted(set(calls)) == ["lease-blob-a", "lease-blob-b"]  # the full replay validates all


def test_parse_lease_event_validates_the_execution_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    path = _chain_files(store, "lease-blob-a")[-1]
    payload = json.loads(path.read_text(encoding="utf-8"))

    class Rejected(Exception):
        pass

    def reject(lease: Any, *args: Any, **kwargs: Any) -> None:
        raise kernel.ParallelControlError("EXECUTION_INVALID", lease.lease_id)

    monkeypatch.setattr(coordination, "validate_execution", reject)
    with pytest.raises(kernel.ParallelControlError, match="EXECUTION_INVALID"):
        kernel.parse_lease_event(payload, blobs=store.blobs)
    event = kernel.parse_lease_event(payload, blobs=store.blobs, validate_execution_payload=False)
    assert event.lease.lease_id == "lease-blob-a"
    # structure and the event id are still checked without the semantic validation
    tampered = json.loads(path.read_text(encoding="utf-8"))
    tampered["actor"] = "someone-else"
    with pytest.raises(kernel.ParallelControlError, match="LEASE_EVENT_HASH"):
        kernel.parse_lease_event(tampered, blobs=store.blobs, validate_execution_payload=False)


# ------------------------------------------------------------------------- build and verify


def test_build_writes_one_atomic_seal_and_describes_what_it_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path, with_blobs=False)
    append_chain(store, "lease-live", ("REQUESTED", "ACTIVE"), resources=write_claim("path:x"))
    writes: list[Path] = []
    real = seal.write_bytes_atomic

    def spy(path: Path, content: bytes) -> Any:
        writes.append(path)
        return real(path, content)

    monkeypatch.setattr(seal, "write_bytes_atomic", spy)
    document, report = seal.build_seal_document(store, created_at=NOW, created_by="test")
    assert writes == []  # building does not write
    seal.write_seal(store.root / seal.SEAL_FILE_NAME, document)
    assert writes == [store.root / seal.SEAL_FILE_NAME]  # exactly one atomic replace
    assert report.sealed_chains == 6 and report.skipped_non_terminal == 1
    assert document["kernel_fingerprint"] == seal.kernel_fingerprint()
    assert document["seal_sha256"] == seal._canonical_digest(
        {k: v for k, v in document.items() if k != "seal_sha256"}
    )
    assert (store.root / seal.SEAL_FILE_NAME).parent == store.root  # next to events/, not in it
    assert not list(store.events_root.glob("*.json"))


def test_a_chain_with_an_issue_is_never_sealed(tmp_path: Path) -> None:
    store = _store(tmp_path, with_blobs=False)
    _chain_files(store, "lease-p002")[1].unlink()  # a hole in the chain: disconnected
    document, report = seal.build_seal_document(store, created_at=NOW, created_by="test")
    assert "lease-p002" not in document["chains"]
    assert report.skipped_with_issues == 1


def test_building_validates_in_full_even_when_a_seal_already_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    _build(store)
    monkeypatch.setenv(seal.SEAL_ENV, "1")
    seen: list[str] = []
    real = kernel._replay_lease_events

    def spy(events: Any, **kwargs: Any) -> Any:
        seen.append(events[0].lease.lease_id if events else "")
        return real(events, **kwargs)

    monkeypatch.setattr(kernel, "_replay_lease_events", spy)
    seal.build_seal_document(store, created_at=NOW, created_by="test")
    assert len(seen) == 8  # every chain went through the full per-chain validation again


def test_verify_reports_ok_and_catches_a_sealed_replay_that_differs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    assert seal.verify(store).reason == "seal_absent"
    _build(store)
    good = seal.verify(store)
    assert good.ok and good.differing_fields == [] and good.stats is not None
    assert good.to_dict()["sealed_chains"] == 8
    real = seal.sealed_replay

    def lossy(**kwargs: Any) -> Any:
        replay, stats = real(**kwargs)
        return (
            kernel.LeaseReplay(
                status=replay.status,
                lease_heads=replay.lease_heads[:-1],
                active_leases=replay.active_leases,
                head_event_ids=replay.head_event_ids,
                event_count=replay.event_count,
                issues=replay.issues,
            ),
            stats,
        )

    monkeypatch.setattr(seal, "sealed_replay", lossy)
    bad = seal.verify(store)
    assert not bad.ok and "lease_heads" in bad.differing_fields


def test_status_describes_why_the_seal_is_or_is_not_in_force(tmp_path: Path) -> None:
    store = _store(tmp_path)
    absent = seal.status(store)
    assert absent["exists"] is False and absent["reason"] == "seal_absent"
    assert absent["in_force_when_enabled"] is False
    _build(store)
    append_chain(store, "lease-new")
    present = seal.status(store)
    assert present["in_force_when_enabled"] is True and present["sealed_chains"] == 8
    assert present["would_take_from_seal"]["full_reasons"] == {"lease-new": "unsealed"}
    assert present["current_kernel_fingerprint"] == present["recorded_kernel_fingerprint"]
    assert "would_take_from_seal" not in seal.status(store, replay=False)


def test_the_seal_lives_in_the_store_root_next_to_events_and_blobs(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _build(store)
    assert sorted(path.name for path in store.root.iterdir() if path.is_file()) == [
        seal.SEAL_FILE_NAME
    ]
    assert (store.root / "events").is_dir()


def test_a_copied_store_with_its_seal_replays_identically(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _build(store)
    copy = tmp_path / "copy"
    shutil.copytree(store.root, copy)
    other = kernel.FileExecutionLeaseStore(copy, policy=store.policy)
    sealed, stats = seal.sealed_replay(
        events_root=other.events_root, blobs=other.blobs, seal=_loaded(other)
    )
    assert sealed == other._replay_serial() == store._replay_serial() and stats.full_chains == 0


def test_the_digest_of_a_chain_is_independent_of_the_machine(tmp_path: Path) -> None:
    store = _store(tmp_path, with_blobs=False)
    document = _build(store)
    entry = document["chains"]["lease-p001"]
    recomputed = hashlib.sha256(
        _canonical(
            {
                "head_event_id": entry["head_event_id"],
                "head_state": entry["head_state"],
                "files": entry["files"],
                "blobs": entry["blobs"],
            }
        ).encode()
    ).hexdigest()
    assert recomputed == entry["chain_sha256"]


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
