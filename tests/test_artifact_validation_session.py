from __future__ import annotations

import hashlib
import json
import os
import stat
import sys
import threading
import time
from collections import OrderedDict
from contextvars import copy_context
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.platform.artifacts import validation_session as validation_session_module
from ai_trading_system.platform.artifacts.validation_session import (
    ArtifactFingerprintInventory,
    ArtifactFingerprintScope,
    artifact_content_identity,
    artifact_validation_session,
    cached_artifact_validation,
)


def test_validation_session_reuses_only_unchanged_pass_artifact(tmp_path: Path) -> None:
    artifact_id = "artifact-1"
    artifact_root = tmp_path / artifact_id
    artifact_root.mkdir()
    payload_path = artifact_root / "payload.txt"
    payload_path.write_text("first", encoding="utf-8")
    calls: list[str] = []

    def validator(*, item_id: str, output_dir: Path) -> dict[str, Any]:
        content = (output_dir / item_id / "payload.txt").read_text(encoding="utf-8")
        calls.append(content)
        return {"status": "PASS", "details": {"content": content}}

    with artifact_validation_session():
        first = cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )
        first["details"]["content"] = "caller-mutation"
        cached = cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )
        payload_path.write_text("second", encoding="utf-8")
        changed = cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )

    assert calls == ["first", "second"]
    assert cached["details"]["content"] == "first"
    assert changed["details"]["content"] == "second"


def test_validation_session_never_reuses_failure(tmp_path: Path) -> None:
    artifact_id = "artifact-1"
    artifact_root = tmp_path / artifact_id
    artifact_root.mkdir()
    (artifact_root / "payload.txt").write_text("invalid", encoding="utf-8")
    calls = 0

    def validator(*, item_id: str, output_dir: Path) -> dict[str, Any]:
        nonlocal calls
        assert item_id == artifact_id
        assert output_dir == tmp_path
        calls += 1
        return {"status": "FAIL"}

    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_key="item_id",
                artifact_id=artifact_id,
                root=tmp_path,
            )

    assert calls == 2


def test_validation_session_invalidates_when_transitive_bound_source_changes(
    tmp_path: Path,
) -> None:
    artifact_id = "artifact-1"
    artifact_root = tmp_path / artifact_id
    artifact_root.mkdir()
    source_path = tmp_path / "live-source.csv"
    source_path.write_text("first", encoding="utf-8")
    source_bytes = source_path.read_bytes()
    snapshot = {
        "schema_version": "test.snapshot.v1",
        "source": {
            "path": str(source_path.resolve()),
            "sha256": hashlib.sha256(source_bytes).hexdigest(),
            "size_bytes": len(source_bytes),
        },
    }
    (artifact_root / "input_snapshot.json").write_text(
        json.dumps(snapshot, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    calls: list[str] = []

    def validator(*, item_id: str, output_dir: Path) -> dict[str, Any]:
        content = source_path.read_text(encoding="utf-8")
        calls.append(content)
        return {"status": "PASS", "item_id": item_id, "source": content}

    with artifact_validation_session():
        first = cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )
        source_path.write_text("second", encoding="utf-8")
        changed = cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )
        source_path.write_bytes(source_bytes)
        restored = cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )

    assert calls == ["first", "second"]
    assert first["source"] == "first"
    assert changed["source"] == "second"
    assert restored["source"] == "first"


def test_validation_session_legacy_without_scope_routes_only_to_compatibility_fingerprint(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    artifact_id = "artifact-1"
    artifact_root = tmp_path / artifact_id
    artifact_root.mkdir()
    validator_calls = 0
    compatibility_roots: list[Path] = []

    def compatibility_fingerprint(root: Path) -> str:
        compatibility_roots.append(root)
        return "compatibility-fingerprint"

    def hardened_fingerprint(
        root: Path,
        *,
        scope: ArtifactFingerprintScope | None = None,
    ) -> str:
        del root, scope
        raise AssertionError("legacy compatibility lane must not use artifact_fingerprint")

    monkeypatch.setattr(
        validation_session_module,
        "_compatibility_artifact_fingerprint",
        compatibility_fingerprint,
    )
    monkeypatch.setattr(
        validation_session_module,
        "artifact_fingerprint",
        hardened_fingerprint,
    )

    def validator(*, item_id: str, output_dir: Path) -> dict[str, Any]:
        nonlocal validator_calls
        validator_calls += 1
        return {"status": "PASS", "item_id": item_id, "root": str(output_dir)}

    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_key="item_id",
                artifact_id=artifact_id,
                root=tmp_path,
            )

    assert validator_calls == 1
    assert compatibility_roots
    assert all(root == artifact_root for root in compatibility_roots)


def test_validation_session_legacy_with_explicit_scope_routes_to_hardened_fingerprint(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    artifact_id = "artifact-1"
    artifact_root = tmp_path / artifact_id
    artifact_root.mkdir()
    dependency = tmp_path / "dependency.txt"
    dependency.write_text("stable", encoding="utf-8")
    scope = ArtifactFingerprintScope(paths=(dependency,))
    validator_calls = 0
    hardened_scopes: list[ArtifactFingerprintScope | None] = []

    def compatibility_fingerprint(root: Path) -> str:
        del root
        raise AssertionError("an explicit scope must not use the compatibility lane")

    def hardened_fingerprint(
        root: Path,
        *,
        scope: ArtifactFingerprintScope | None = None,
    ) -> str:
        assert root == artifact_root
        hardened_scopes.append(scope)
        return "hardened-fingerprint"

    monkeypatch.setattr(
        validation_session_module,
        "_compatibility_artifact_fingerprint",
        compatibility_fingerprint,
    )
    monkeypatch.setattr(
        validation_session_module,
        "artifact_fingerprint",
        hardened_fingerprint,
    )

    def validator(*, item_id: str, output_dir: Path) -> dict[str, Any]:
        nonlocal validator_calls
        validator_calls += 1
        return {"status": "PASS", "item_id": item_id, "root": str(output_dir)}

    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_key="item_id",
                artifact_id=artifact_id,
                root=tmp_path,
                fingerprint_scope=scope,
            )

    assert validator_calls == 1
    assert hardened_scopes
    assert all(observed is scope for observed in hardened_scopes)


def test_validation_session_fingerprint_lanes_do_not_share_identical_text_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_id = "artifact-1"
    artifact_root = tmp_path / artifact_id
    artifact_root.mkdir()
    forced_fingerprint = "forced-identical-fingerprint"
    validator_calls = 0

    monkeypatch.setattr(
        validation_session_module,
        "_compatibility_artifact_fingerprint",
        lambda root: forced_fingerprint,
    )
    monkeypatch.setattr(
        validation_session_module,
        "artifact_fingerprint",
        lambda root, *, scope=None: forced_fingerprint,
    )

    def validator(*, item_id: str, output_dir: Path) -> dict[str, Any]:
        nonlocal validator_calls
        validator_calls += 1
        return {
            "status": "PASS",
            "call": validator_calls,
            "item_id": item_id,
            "root": str(output_dir),
        }

    def validate(*, hardened: bool) -> dict[str, Any]:
        return cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
            fingerprint_scope=ArtifactFingerprintScope() if hardened else None,
        )

    with artifact_validation_session():
        compatibility_first = validate(hardened=False)
        hardened_first = validate(hardened=True)
        compatibility_cached = validate(hardened=False)
        hardened_cached = validate(hardened=True)

    assert validator_calls == 2
    assert compatibility_first == compatibility_cached
    assert hardened_first == hardened_cached
    assert compatibility_first["call"] == 1
    assert hardened_first["call"] == 2


def test_validation_session_generic_without_scope_routes_to_hardened_fingerprint(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    validator_calls = 0
    hardened_scopes: list[ArtifactFingerprintScope | None] = []

    def compatibility_fingerprint(root: Path) -> str:
        del root
        raise AssertionError("generic validator kwargs must not use the compatibility lane")

    def hardened_fingerprint(
        root: Path,
        *,
        scope: ArtifactFingerprintScope | None = None,
    ) -> str:
        assert root == artifact_root
        hardened_scopes.append(scope)
        return "hardened-fingerprint"

    monkeypatch.setattr(
        validation_session_module,
        "_compatibility_artifact_fingerprint",
        compatibility_fingerprint,
    )
    monkeypatch.setattr(
        validation_session_module,
        "artifact_fingerprint",
        hardened_fingerprint,
    )

    def validator(*, item_id: str) -> dict[str, Any]:
        nonlocal validator_calls
        validator_calls += 1
        return {"status": "PASS", "item_id": item_id}

    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_kwargs={"item_id": "artifact-1"},
                artifact_root=artifact_root,
            )

    assert validator_calls == 1
    assert hardened_scopes
    assert all(
        isinstance(observed, ArtifactFingerprintScope)
        for observed in hardened_scopes
    )


def test_validation_session_legacy_compatibility_boundary_does_not_scan_unbound_nested_file(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    artifact_id = "artifact-1"
    artifact_root = tmp_path / artifact_id
    artifact_root.mkdir()
    (artifact_root / "summary.json").write_text(
        '{"status":"stable"}\n',
        encoding="utf-8",
    )
    nested_path = artifact_root / "unbound" / "deep" / "large.bin"
    nested_path.parent.mkdir(parents=True)
    nested_path.write_bytes(b"a" * (2 * 1024 * 1024))
    nested_resolved = nested_path.resolve()
    artifact_root_resolved = artifact_root.resolve()
    validator_calls = 0
    real_path_open = Path.open
    real_path_rglob = Path.rglob

    def guarded_open(path: Path, *args: Any, **kwargs: Any) -> Any:
        if path.resolve() == nested_resolved:
            raise AssertionError("unbound nested file must not be fingerprint-read")
        return real_path_open(path, *args, **kwargs)

    def guarded_rglob(path: Path, *args: Any, **kwargs: Any) -> Any:
        if path.resolve() == artifact_root_resolved:
            raise AssertionError("compatibility boundary must not recursively scan the root")
        return real_path_rglob(path, *args, **kwargs)

    def hardened_fingerprint(
        root: Path,
        *,
        scope: ArtifactFingerprintScope | None = None,
    ) -> str:
        del root, scope
        raise AssertionError("legacy compatibility lane must not use artifact_fingerprint")

    monkeypatch.setattr(Path, "open", guarded_open)
    monkeypatch.setattr(Path, "rglob", guarded_rglob)
    monkeypatch.setattr(
        validation_session_module,
        "artifact_fingerprint",
        hardened_fingerprint,
    )

    def validator(*, item_id: str, output_dir: Path) -> dict[str, Any]:
        nonlocal validator_calls
        validator_calls += 1
        return {"status": "PASS", "item_id": item_id, "root": str(output_dir)}

    # This preserves only the historical call-shape boundary. Callers that depend on
    # nested files must bind them explicitly with ``fingerprint_scope``.
    with artifact_validation_session():
        first = cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )
        with real_path_open(nested_path, "wb") as handle:
            handle.write(b"b" * (2 * 1024 * 1024))
        second = cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )

    assert validator_calls == 1
    assert second == first


def test_legacy_compat_fingerprint_invalidates_absolute_sha256_binding_drift(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    artifact_id = "artifact-1"
    artifact_root = tmp_path / artifact_id
    artifact_root.mkdir()
    source_path = tmp_path / "live-source.csv"
    source_path.write_text("first", encoding="utf-8")
    source_bytes = source_path.read_bytes()
    (artifact_root / "input_snapshot.json").write_text(
        json.dumps(
            {
                "source": {
                    "path": str(source_path.resolve()),
                    "sha256": hashlib.sha256(source_bytes).hexdigest(),
                }
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    validator_calls: list[str] = []
    compatibility_roots: list[Path] = []
    real_compatibility_fingerprint = (
        validation_session_module._compatibility_artifact_fingerprint
    )

    def compatibility_fingerprint(root: Path) -> str:
        compatibility_roots.append(root)
        return real_compatibility_fingerprint(root)

    def hardened_fingerprint(
        root: Path,
        *,
        scope: ArtifactFingerprintScope | None = None,
    ) -> str:
        del root, scope
        raise AssertionError("legacy compatibility lane must not use artifact_fingerprint")

    monkeypatch.setattr(
        validation_session_module,
        "_compatibility_artifact_fingerprint",
        compatibility_fingerprint,
    )
    monkeypatch.setattr(
        validation_session_module,
        "artifact_fingerprint",
        hardened_fingerprint,
    )

    def validator(*, item_id: str, output_dir: Path) -> dict[str, Any]:
        content = source_path.read_text(encoding="utf-8")
        validator_calls.append(content)
        return {"status": "PASS", "item_id": item_id, "source": content}

    with artifact_validation_session():
        first = cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )
        source_path.write_text("second", encoding="utf-8")
        changed = cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )

    assert compatibility_roots
    assert validator_calls == ["first", "second"]
    assert first["source"] == "first"
    assert changed["source"] == "second"


def test_process_digest_lru_reuses_bytes_across_sessions_and_invalidates_signature(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_id = "artifact-1"
    artifact_root = tmp_path / artifact_id
    artifact_root.mkdir()
    payload_path = artifact_root / "payload.txt"
    payload_path.write_text("first", encoding="utf-8")
    resolved_payload = payload_path.resolve()
    path_type = type(payload_path)
    real_path_open = path_type.open
    fingerprint_binary_reads = 0

    def tracking_open(
        self: Path,
        mode: str = "r",
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        nonlocal fingerprint_binary_reads
        if self == resolved_payload and mode == "rb":
            fingerprint_binary_reads += 1
        return real_path_open(self, mode, *args, **kwargs)

    monkeypatch.setattr(path_type, "open", tracking_open)
    validator_calls = 0

    def validator(*, item_id: str, output_dir: Path) -> dict[str, Any]:
        nonlocal validator_calls
        validator_calls += 1
        return {
            "status": "PASS",
            "item_id": item_id,
            "content": (output_dir / item_id / "payload.txt").read_text(
                encoding="utf-8"
            ),
        }

    def validate_once() -> dict[str, Any]:
        with artifact_validation_session():
            return cached_artifact_validation(
                validator=validator,
                validator_key="item_id",
                artifact_id=artifact_id,
                root=tmp_path,
            )

    assert validate_once()["content"] == "first"
    assert validate_once()["content"] == "first"
    payload_path.write_text("second", encoding="utf-8")
    assert validate_once()["content"] == "second"

    assert validator_calls == 3
    assert fingerprint_binary_reads == 2
    process_keys = list(validation_session_module._PROCESS_FILE_DIGESTS)
    assert any(key[0] == str(resolved_payload) for key in process_keys)
    assert all(type(key[0]) is str for key in process_keys)
    assert not any(
        isinstance(component, Path)
        for key in process_keys
        for component in key
    )


def test_process_digest_lru_weights_retained_bytes_and_evicts_exactly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        validation_session_module,
        "_PROCESS_FILE_DIGESTS",
        OrderedDict(),
    )
    counter_name = (
        "_PROCESS_FILE_DIGEST_BYTES"
        if hasattr(validation_session_module, "_PROCESS_FILE_DIGEST_BYTES")
        else "_PROCESS_FILE_DIGEST_WEIGHT"
    )
    monkeypatch.setattr(validation_session_module, counter_name, 0)
    monkeypatch.setattr(
        validation_session_module,
        "_MAX_PROCESS_FILE_DIGEST_BYTES",
        1024 * 1024,
    )

    entries: list[tuple[Any, Any]] = []
    for index in range(3):
        signature = (17, 23, 29, 31, stat.S_IFREG, 37, 41)
        key = (
            str((tmp_path / f"payload-{index}.json").resolve()),
            "hardened",
            signature,
        )
        memo = validation_session_module._FileDigestMemo(
            signature=signature,
            size_bytes=17,
            digest=bytes([index]) * 32,
            content_bound_paths=(f"/content/{index}/" + "c" * 2048,),
            metadata_bound_paths=(f"/metadata/{index}/" + "m" * 2048,),
        )
        entries.append((key, memo))

    assert all(type(key[0]) is str for key, _ in entries)
    assert not any(
        isinstance(component, Path)
        for key, _ in entries
        for component in key
    )
    expected_weights = [
        validation_session_module._process_file_digest_retained_bytes(key, memo)
        for key, memo in entries
    ]
    assert expected_weights[0] == expected_weights[1] == expected_weights[2]
    sample_key, sample_memo = entries[0]
    independent_entry_object_floor = (
        sys.getsizeof(sample_key)
        + sys.getsizeof(sample_key[2])
        + sys.getsizeof(sample_memo)
        + sys.getsizeof(sample_memo.__dict__)
        + sys.getsizeof(sample_memo.digest)
        + sys.getsizeof(sample_memo.content_bound_paths)
        + sys.getsizeof(sample_memo.metadata_bound_paths)
        + sum(
            len(value.encode("utf-8"))
            for value in (
                sample_key[0],
                sample_key[1],
                *sample_memo.content_bound_paths,
                *sample_memo.metadata_bound_paths,
            )
        )
    )
    sample_value = (sample_memo, expected_weights[0])
    empty_lru: OrderedDict[Any, Any] = OrderedDict()
    one_entry_lru: OrderedDict[Any, Any] = OrderedDict(
        ((sample_key, sample_value),)
    )
    ordered_dict_entry_bytes = sys.getsizeof(one_entry_lru) - sys.getsizeof(empty_lru)
    independent_value_overhead = sys.getsizeof(sample_value) + sys.getsizeof(
        expected_weights[0]
    )
    table_budget_per_entry = (
        validation_session_module._PROCESS_FILE_DIGEST_ENTRY_OVERHEAD_BYTES
        - validation_session_module._PROCESS_FILE_DIGEST_VALUE_OVERHEAD_BYTES
    )
    assert ordered_dict_entry_bytes > 0
    assert (
        validation_session_module._PROCESS_FILE_DIGEST_VALUE_OVERHEAD_BYTES
        >= independent_value_overhead
    )
    assert table_budget_per_entry >= ordered_dict_entry_bytes
    independent_lru_container_bytes = (
        independent_value_overhead + ordered_dict_entry_bytes
    )
    assert (
        independent_lru_container_bytes
        <= validation_session_module._PROCESS_FILE_DIGEST_ENTRY_OVERHEAD_BYTES
    )
    independent_retained_floor = (
        independent_entry_object_floor + independent_lru_container_bytes
    )
    assert expected_weights[0] >= independent_retained_floor

    exact_two_entry_limit = expected_weights[0] + expected_weights[1]
    monkeypatch.setattr(
        validation_session_module,
        "_MAX_PROCESS_FILE_DIGEST_BYTES",
        exact_two_entry_limit,
    )
    first_key, first_memo = entries[0]
    second_key, second_memo = entries[1]
    third_key, third_memo = entries[2]
    validation_session_module._process_file_digest_put(first_key, first_memo)
    validation_session_module._process_file_digest_put(second_key, second_memo)

    assert list(validation_session_module._PROCESS_FILE_DIGESTS) == [
        first_key,
        second_key,
    ]
    assert [
        retained_bytes
        for _, retained_bytes in validation_session_module._PROCESS_FILE_DIGESTS.values()
    ] == expected_weights[:2]
    assert getattr(validation_session_module, counter_name) == exact_two_entry_limit

    steady_state_cache_identity = id(
        validation_session_module._PROCESS_FILE_DIGESTS
    )
    table_budget_per_entry = (
        validation_session_module._PROCESS_FILE_DIGEST_ENTRY_OVERHEAD_BYTES
        - validation_session_module._PROCESS_FILE_DIGEST_VALUE_OVERHEAD_BYTES
    )
    assert table_budget_per_entry >= 0
    steady_state_compact_limit = (
        sys.getsizeof(OrderedDict())
        + table_budget_per_entry
        * max(len(validation_session_module._PROCESS_FILE_DIGESTS), 1)
    )
    assert (
        sys.getsizeof(validation_session_module._PROCESS_FILE_DIGESTS)
        <= steady_state_compact_limit
    )
    validation_session_module._process_file_digest_put(first_key, first_memo)
    assert id(validation_session_module._PROCESS_FILE_DIGESTS) == (
        steady_state_cache_identity
    )
    assert list(validation_session_module._PROCESS_FILE_DIGESTS) == [
        second_key,
        first_key,
    ]
    assert validation_session_module._process_file_digest_get(first_key) == first_memo
    validation_session_module._process_file_digest_put(third_key, third_memo)

    assert id(validation_session_module._PROCESS_FILE_DIGESTS) == (
        steady_state_cache_identity
    )
    assert list(validation_session_module._PROCESS_FILE_DIGESTS) == [
        first_key,
        third_key,
    ]
    assert getattr(validation_session_module, counter_name) == exact_two_entry_limit

    validation_session_module._PROCESS_FILE_DIGESTS.clear()
    monkeypatch.setattr(validation_session_module, counter_name, 0)
    monkeypatch.setattr(
        validation_session_module,
        "_MAX_PROCESS_FILE_DIGEST_BYTES",
        expected_weights[0] - 1,
    )
    validation_session_module._process_file_digest_put(first_key, first_memo)

    assert not validation_session_module._PROCESS_FILE_DIGESTS
    assert getattr(validation_session_module, counter_name) == 0


def test_process_digest_lru_compacts_table_after_large_byte_cap_eviction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        validation_session_module,
        "_PROCESS_FILE_DIGESTS",
        OrderedDict(),
    )
    counter_name = (
        "_PROCESS_FILE_DIGEST_BYTES"
        if hasattr(validation_session_module, "_PROCESS_FILE_DIGEST_BYTES")
        else "_PROCESS_FILE_DIGEST_WEIGHT"
    )
    monkeypatch.setattr(validation_session_module, counter_name, 0)
    filled_entry_count = 128
    entries: list[tuple[Any, Any]] = []
    for index in range(filled_entry_count + 1):
        fixed_index = f"{index:04d}"
        signature = (17, 23, 29, 31, stat.S_IFREG, 37, 41)
        key = (
            str((tmp_path / f"payload-{fixed_index}.json").resolve()),
            "hardened",
            signature,
        )
        memo = validation_session_module._FileDigestMemo(
            signature=signature,
            size_bytes=17,
            digest=bytes([index % 256]) * 32,
            content_bound_paths=(f"/content/{fixed_index}/" + "c" * 256,),
            metadata_bound_paths=(f"/metadata/{fixed_index}/" + "m" * 256,),
        )
        entries.append((key, memo))

    retained_bytes = [
        validation_session_module._process_file_digest_retained_bytes(key, memo)
        for key, memo in entries
    ]
    assert len(set(retained_bytes)) == 1
    entry_bytes = retained_bytes[0]
    monkeypatch.setattr(
        validation_session_module,
        "_MAX_PROCESS_FILE_DIGEST_BYTES",
        entry_bytes * filled_entry_count,
    )
    for key, memo in entries[:filled_entry_count]:
        validation_session_module._process_file_digest_put(key, memo)

    assert len(validation_session_module._PROCESS_FILE_DIGESTS) == filled_entry_count
    empty_table_bytes = sys.getsizeof(OrderedDict())
    peak_table_bytes = sys.getsizeof(
        validation_session_module._PROCESS_FILE_DIGESTS
    )
    peak_cache_identity = id(validation_session_module._PROCESS_FILE_DIGESTS)
    expected_survivors = entries[-2:]
    compacted_probe = OrderedDict(
        (
            key,
            (memo, validation_session_module._process_file_digest_retained_bytes(key, memo)),
        )
        for key, memo in expected_survivors
    )
    compacted_table_overhead = sys.getsizeof(compacted_probe) - empty_table_bytes
    reduced_cap = 2 * entry_bytes + compacted_table_overhead
    assert 3 * entry_bytes > reduced_cap
    monkeypatch.setattr(
        validation_session_module,
        "_MAX_PROCESS_FILE_DIGEST_BYTES",
        reduced_cap,
    )

    validation_session_module._process_file_digest_put(*entries[-1])

    assert list(validation_session_module._PROCESS_FILE_DIGESTS) == [
        key for key, _ in expected_survivors
    ]
    assert id(validation_session_module._PROCESS_FILE_DIGESTS) != peak_cache_identity
    compacted_table_bytes = sys.getsizeof(
        validation_session_module._PROCESS_FILE_DIGESTS
    )
    assert compacted_table_bytes * 4 < peak_table_bytes
    actual_table_overhead = compacted_table_bytes - empty_table_bytes
    table_budget_per_entry = (
        validation_session_module._PROCESS_FILE_DIGEST_ENTRY_OVERHEAD_BYTES
        - validation_session_module._PROCESS_FILE_DIGEST_VALUE_OVERHEAD_BYTES
    )
    compacted_table_limit = empty_table_bytes + (
        table_budget_per_entry
        * max(len(validation_session_module._PROCESS_FILE_DIGESTS), 1)
    )
    assert compacted_table_bytes <= compacted_table_limit
    assert (
        validation_session_module._PROCESS_FILE_DIGEST_VALUE_OVERHEAD_BYTES
        * len(validation_session_module._PROCESS_FILE_DIGESTS)
        + actual_table_overhead
        <= validation_session_module._PROCESS_FILE_DIGEST_ENTRY_OVERHEAD_BYTES
        * len(validation_session_module._PROCESS_FILE_DIGESTS)
    )
    accounted_bytes = getattr(validation_session_module, counter_name)
    assert accounted_bytes == 2 * entry_bytes
    assert accounted_bytes + actual_table_overhead <= reduced_cap


def test_validation_session_does_not_mirror_process_digest_lru(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        validation_session_module,
        "_PROCESS_FILE_DIGESTS",
        OrderedDict(),
    )
    counter_name = (
        "_PROCESS_FILE_DIGEST_BYTES"
        if hasattr(validation_session_module, "_PROCESS_FILE_DIGEST_BYTES")
        else "_PROCESS_FILE_DIGEST_WEIGHT"
    )
    monkeypatch.setattr(validation_session_module, counter_name, 0)
    monkeypatch.setattr(
        validation_session_module,
        "_WINDOWS_CHANGE_TOKEN_REQUIRED",
        False,
    )
    process_cap_bytes = 8 * 1024
    monkeypatch.setattr(
        validation_session_module,
        "_MAX_PROCESS_FILE_DIGEST_BYTES",
        process_cap_bytes,
    )
    source_count = 64
    source_paths: list[Path] = []
    for index in range(source_count):
        source_path = tmp_path / f"source-{index:04d}.txt"
        source_path.write_text(f"payload-{index:04d}", encoding="utf-8")
        source_paths.append(source_path)

    with artifact_validation_session():
        state = validation_session_module._VALIDATION_SESSION.get()
        assert state is not None and state.active
        assert not hasattr(state, "file_digests")
        for source_path in source_paths:
            assert artifact_content_identity(source_path) is not None

        assert validation_session_module._VALIDATION_SESSION.get() is state
        assert not hasattr(state, "file_digests")
        process_cache = validation_session_module._PROCESS_FILE_DIGESTS
        assert 0 < len(process_cache) < source_count
        accounted_bytes = getattr(validation_session_module, counter_name)
        assert accounted_bytes == sum(
            retained_bytes for _, retained_bytes in process_cache.values()
        )
        assert accounted_bytes <= process_cap_bytes


@pytest.mark.parametrize("lane", ("compatibility", "hardened"))
@pytest.mark.parametrize(
    "invalid_binding",
    ("oversized", "cumulative_oversized", "embedded_nul"),
)
def test_process_digest_lru_rejects_commitment_before_invalid_topology_is_cached(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    lane: str,
    invalid_binding: str,
) -> None:
    monkeypatch.setattr(
        validation_session_module,
        "_PROCESS_FILE_DIGESTS",
        OrderedDict(),
    )
    counter_name = (
        "_PROCESS_FILE_DIGEST_BYTES"
        if hasattr(validation_session_module, "_PROCESS_FILE_DIGEST_BYTES")
        else "_PROCESS_FILE_DIGEST_WEIGHT"
    )
    monkeypatch.setattr(validation_session_module, counter_name, 0)
    monkeypatch.setattr(
        validation_session_module,
        "_WINDOWS_CHANGE_TOKEN_REQUIRED",
        False,
    )
    artifact_id = "artifact-1"
    artifact_root = tmp_path / artifact_id
    artifact_root.mkdir()
    absolute_prefix = str(tmp_path.resolve()) + os.sep
    if invalid_binding == "oversized":
        max_path_bytes = getattr(
            validation_session_module,
            "_MAX_BOUND_PATH_UTF8_BYTES",
            32 * 1024,
        )
        binding_path = absolute_prefix + "x" * (max_path_bytes + 1)
        snapshot_document = {
            "source": {
                "path": binding_path,
                "sha256": "declared-checksum",
            }
        }
    elif invalid_binding == "cumulative_oversized":
        binding_paths = (
            absolute_prefix + "first-source.csv",
            absolute_prefix + "second-source.csv",
        )
        max_path_bytes = validation_session_module._MAX_BOUND_PATH_UTF8_BYTES
        assert all(
            len(binding_path.encode("utf-8")) <= max_path_bytes
            for binding_path in binding_paths
        )
        monkeypatch.setattr(
            validation_session_module,
            "_MAX_BOUND_PATH_TOTAL_UTF8_BYTES",
            sum(len(binding_path.encode("utf-8")) for binding_path in binding_paths)
            - 1,
        )
        snapshot_document = {
            "sources": [
                {
                    "path": binding_path,
                    "sha256": f"declared-checksum-{index}",
                }
                for index, binding_path in enumerate(binding_paths)
            ]
        }
    else:
        binding_path = absolute_prefix + "invalid\x00path"
        snapshot_document = {
            "source": {
                "path": binding_path,
                "sha256": "declared-checksum",
            }
        }
    snapshot_path = artifact_root / "input_snapshot.json"
    snapshot_path.write_text(
        json.dumps(snapshot_document) + "\n",
        encoding="utf-8",
    )
    validator_calls = 0

    def validator(*, item_id: str, output_dir: Path) -> dict[str, Any]:
        nonlocal validator_calls
        validator_calls += 1
        return {"status": "PASS", "item_id": item_id, "root": str(output_dir)}

    with artifact_validation_session():
        for _ in range(2):
            kwargs: dict[str, Any] = {
                "validator": validator,
                "validator_key": "item_id",
                "artifact_id": artifact_id,
                "root": tmp_path,
            }
            if lane == "hardened":
                kwargs["fingerprint_scope"] = ArtifactFingerprintScope()
            cached_artifact_validation(**kwargs)

    assert validator_calls == 2
    retained_paths = {
        key[0] for key in validation_session_module._PROCESS_FILE_DIGESTS
    }
    assert all(type(key[0]) is str for key in validation_session_module._PROCESS_FILE_DIGESTS)
    assert not any(
        isinstance(component, Path)
        for key in validation_session_module._PROCESS_FILE_DIGESTS
        for component in key
    )
    assert str(snapshot_path.resolve()) not in retained_paths


@pytest.mark.parametrize("lane", ("compatibility", "hardened"))
@pytest.mark.parametrize("component_budget", ("single_path", "cumulative"))
def test_process_digest_lru_rejects_component_budget_before_topology_expansion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    lane: str,
    component_budget: str,
) -> None:
    monkeypatch.setattr(
        validation_session_module,
        "_PROCESS_FILE_DIGESTS",
        OrderedDict(),
    )
    counter_name = (
        "_PROCESS_FILE_DIGEST_BYTES"
        if hasattr(validation_session_module, "_PROCESS_FILE_DIGEST_BYTES")
        else "_PROCESS_FILE_DIGEST_WEIGHT"
    )
    monkeypatch.setattr(validation_session_module, counter_name, 0)
    monkeypatch.setattr(
        validation_session_module,
        "_WINDOWS_CHANGE_TOKEN_REQUIRED",
        False,
    )
    artifact_id = "artifact-1"
    artifact_root = tmp_path / artifact_id
    artifact_root.mkdir()
    resolved_tmp = tmp_path.resolve()
    base_component_count = len(resolved_tmp.parts)
    if component_budget == "single_path":
        per_path_limit = base_component_count + 3
        binding_paths = (
            str(resolved_tmp.joinpath("deep-1", "deep-2", "deep-3", "deep-4")),
        )
        assert len(Path(binding_paths[0]).parts) == per_path_limit + 1
        monkeypatch.setattr(
            validation_session_module,
            "_MAX_BOUND_PATH_COMPONENTS",
            per_path_limit,
        )
    else:
        binding_paths = (
            str(resolved_tmp / "first-source.csv"),
            str(resolved_tmp / "second-source.csv"),
        )
        component_counts = [len(Path(value).parts) for value in binding_paths]
        per_path_limit = base_component_count + 3
        assert all(count <= per_path_limit for count in component_counts)
        monkeypatch.setattr(
            validation_session_module,
            "_MAX_BOUND_PATH_COMPONENTS",
            per_path_limit,
        )
        monkeypatch.setattr(
            validation_session_module,
            "_MAX_BOUND_PATH_TOTAL_COMPONENTS",
            sum(component_counts) - 1,
        )

    topology_calls = 0

    def reject_topology_expansion(**kwargs: Any) -> None:
        nonlocal topology_calls
        topology_calls += 1
        raise AssertionError(
            f"component budgets must reject before topology expansion: {kwargs}"
        )

    monkeypatch.setattr(
        validation_session_module,
        "_validate_discovered_bound_path_topology",
        reject_topology_expansion,
    )
    snapshot_path = artifact_root / "input_snapshot.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "sources": [
                    {
                        "path": binding_path,
                        "sha256": f"declared-checksum-{index}",
                    }
                    for index, binding_path in enumerate(binding_paths)
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    validator_calls = 0

    def validator(*, item_id: str, output_dir: Path) -> dict[str, Any]:
        nonlocal validator_calls
        validator_calls += 1
        return {"status": "PASS", "item_id": item_id, "root": str(output_dir)}

    with artifact_validation_session():
        for _ in range(2):
            kwargs: dict[str, Any] = {
                "validator": validator,
                "validator_key": "item_id",
                "artifact_id": artifact_id,
                "root": tmp_path,
            }
            if lane == "hardened":
                kwargs["fingerprint_scope"] = ArtifactFingerprintScope()
            cached_artifact_validation(**kwargs)

    assert topology_calls == 0
    assert validator_calls == 2
    retained_paths = {
        key[0] for key in validation_session_module._PROCESS_FILE_DIGESTS
    }
    assert str(snapshot_path.resolve()) not in retained_paths
    assert getattr(validation_session_module, counter_name) == 0


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX os.fork")
def test_validation_session_active_report_and_digest_lock_do_not_cross_fork(
    tmp_path: Path,
) -> None:
    artifact_id = "artifact-1"
    artifact_root = tmp_path / artifact_id
    artifact_root.mkdir()
    (artifact_root / "payload.txt").write_text("stable", encoding="utf-8")
    child_probe = tmp_path / "child-probe.txt"
    child_probe.write_text("probe", encoding="utf-8")

    def validator(*, item_id: str, output_dir: Path) -> dict[str, Any]:
        return {
            "status": "PASS",
            "pid": os.getpid(),
            "item_id": item_id,
            "root": str(output_dir),
        }

    read_fd, write_fd = os.pipe()
    lock_acquired = threading.Event()
    release_lock = threading.Event()

    def hold_process_digest_lock() -> None:
        with validation_session_module._PROCESS_FILE_DIGEST_LOCK:
            lock_acquired.set()
            release_lock.wait(timeout=10)

    child_pid: int | None = None
    lock_holder: threading.Thread | None = None
    child_status: int | None = None
    try:
        with artifact_validation_session():
            parent_report = cached_artifact_validation(
                validator=validator,
                validator_key="item_id",
                artifact_id=artifact_id,
                root=tmp_path,
            )
            assert parent_report["pid"] == os.getpid()
            lock_holder = threading.Thread(target=hold_process_digest_lock)
            lock_holder.start()
            assert lock_acquired.wait(timeout=5)
            child_pid = os.fork()
            if child_pid == 0:
                os.close(read_fd)
                try:
                    child_report = cached_artifact_validation(
                        validator=validator,
                        validator_key="item_id",
                        artifact_id=artifact_id,
                        root=tmp_path,
                    )
                    child_identity = artifact_content_identity(child_probe)
                    payload = json.dumps(
                        {
                            "pid": child_report["pid"],
                            "identity": child_identity,
                        }
                    ).encode("utf-8")
                    os.write(write_fd, payload)
                    os._exit(0)
                except BaseException as exc:  # noqa: BLE001 - child must report then exit.
                    os.write(
                        write_fd,
                        json.dumps(
                            {"error": f"{type(exc).__name__}: {exc}"}
                        ).encode("utf-8"),
                    )
                    os._exit(1)
            os.close(write_fd)
            write_fd = -1
            release_lock.set()
            lock_holder.join(timeout=5)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                waited_pid, child_status = os.waitpid(child_pid, os.WNOHANG)
                if waited_pid == child_pid:
                    break
                time.sleep(0.01)
            else:
                os.kill(child_pid, 9)
                os.waitpid(child_pid, 0)
                pytest.fail("forked validation child deadlocked on inherited cache state")
            child_payload = json.loads(os.read(read_fd, 4096).decode("utf-8"))
    finally:
        release_lock.set()
        if lock_holder is not None:
            lock_holder.join(timeout=5)
        if child_pid not in {None, 0} and child_status is None:
            try:
                os.kill(child_pid, 9)
            except ProcessLookupError:
                pass
            else:
                os.waitpid(child_pid, 0)
        os.close(read_fd)
        if write_fd >= 0:
            os.close(write_fd)

    assert child_status is not None and os.waitstatus_to_exitcode(child_status) == 0
    assert "error" not in child_payload
    assert child_payload["pid"] != parent_report["pid"]
    assert child_payload["pid"] == child_pid
    assert child_payload["identity"] == hashlib.sha256(b"probe").hexdigest()


def test_validation_session_nested_contexts_share_but_outer_sessions_do_not(
    tmp_path: Path,
) -> None:
    artifact_id = "artifact-1"
    artifact_root = tmp_path / artifact_id
    artifact_root.mkdir()
    (artifact_root / "payload.txt").write_text("stable", encoding="utf-8")
    calls = 0

    def validator(*, item_id: str, output_dir: Path) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS", "item_id": item_id, "root": str(output_dir)}

    def validate() -> dict[str, Any]:
        return cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )

    with artifact_validation_session():
        validate()
        with artifact_validation_session():
            validate()
        validate()
    assert calls == 1

    with artifact_validation_session():
        validate()
    assert calls == 2


def test_validation_session_keys_generic_kwargs_semantics_and_version(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    (artifact_root / "payload.txt").write_text("stable", encoding="utf-8")
    calls: list[tuple[str, str]] = []

    def validator(*, item_id: str, mode: str) -> dict[str, Any]:
        calls.append((item_id, mode))
        return {"status": "PASS", "item_id": item_id, "mode": mode}

    def validate(*, mode: str, cutoff: str, version: str) -> dict[str, Any]:
        return cached_artifact_validation(
            validator=validator,
            validator_kwargs={"mode": mode, "item_id": "artifact-1"},
            artifact_root=artifact_root,
            semantic_key={"cutoff": cutoff, "options": {"b": 2, "a": 1}},
            validator_version=version,
        )

    with artifact_validation_session():
        validate(mode="strict", cutoff="2026-07-16", version="v1")
        validate(mode="strict", cutoff="2026-07-16", version="v1")
        validate(mode="relaxed", cutoff="2026-07-16", version="v1")
        validate(mode="strict", cutoff="2026-07-17", version="v1")
        validate(mode="strict", cutoff="2026-07-16", version="v2")

    assert calls == [
        ("artifact-1", "strict"),
        ("artifact-1", "relaxed"),
        ("artifact-1", "strict"),
        ("artifact-1", "strict"),
    ]


def test_validation_session_explicit_missing_and_directory_paths_invalidate(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    dependency = tmp_path / "external"
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS"}

    scope = ArtifactFingerprintScope(paths=(dependency,))
    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_kwargs={},
                artifact_root=artifact_root,
                fingerprint_scope=scope,
            )
        dependency.mkdir()
        nested = dependency / "nested" / "source.txt"
        nested.parent.mkdir()
        nested.write_text("first", encoding="utf-8")
        cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
            fingerprint_scope=scope,
        )
        nested.write_text("second", encoding="utf-8")
        cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
            fingerprint_scope=scope,
        )
        nested.unlink()
        cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
            fingerprint_scope=scope,
        )

    assert calls == 4


def test_validation_session_bounded_inventory_tracks_add_delete_and_bytes(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    inventory_root = tmp_path / "candidates"
    first = inventory_root / "first" / "manifest.json"
    first.parent.mkdir(parents=True)
    first.write_text('{"id":"first"}\n', encoding="utf-8")
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS"}

    scope = ArtifactFingerprintScope(
        inventories=(
            ArtifactFingerprintInventory(
                root=inventory_root,
                patterns=("*/manifest.json",),
            ),
        )
    )
    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_kwargs={},
                artifact_root=artifact_root,
                fingerprint_scope=scope,
            )
        second = inventory_root / "second" / "manifest.json"
        second.parent.mkdir()
        second.write_text('{"id":"second"}\n', encoding="utf-8")
        cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
            fingerprint_scope=scope,
        )
        first.unlink()
        cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
            fingerprint_scope=scope,
        )
        second.write_text('{"id":"changed"}\n', encoding="utf-8")
        cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
            fingerprint_scope=scope,
        )

    assert calls == 4


def test_validation_session_does_not_cache_when_validator_mutates_scope(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    payload_path = artifact_root / "payload.txt"
    payload_path.write_text("before", encoding="utf-8")
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        if calls == 1:
            payload_path.write_text("after", encoding="utf-8")
        return {"status": "PASS", "calls": calls}

    with artifact_validation_session():
        first = cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
        )
        second = cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
        )
        third = cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
        )

    assert calls == 2
    assert first["calls"] == 1
    assert second == third == {"status": "PASS", "calls": 2}


def test_validation_session_never_caches_warning_or_exception(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    warning_calls = 0

    def warning_validator() -> dict[str, Any]:
        nonlocal warning_calls
        warning_calls += 1
        return {"status": "PASS_WITH_WARNINGS"}

    exception_calls = 0

    def exception_validator() -> dict[str, Any]:
        nonlocal exception_calls
        exception_calls += 1
        if exception_calls == 1:
            raise RuntimeError("transient validation error")
        return {"status": "PASS"}

    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=warning_validator,
                validator_kwargs={},
                artifact_root=artifact_root,
            )
        with pytest.raises(RuntimeError, match="transient validation error"):
            cached_artifact_validation(
                validator=exception_validator,
                validator_kwargs={},
                artifact_root=artifact_root,
            )
        for _ in range(2):
            cached_artifact_validation(
                validator=exception_validator,
                validator_kwargs={},
                artifact_root=artifact_root,
            )

    assert warning_calls == 2
    assert exception_calls == 2


def test_validation_session_rehashes_same_size_content_with_restored_mtime(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    source_path = tmp_path / "source.txt"
    source_path.write_text("aaaa", encoding="utf-8")
    original_stat = source_path.stat()
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS"}

    scope = ArtifactFingerprintScope(paths=(source_path,))
    with artifact_validation_session():
        cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
            fingerprint_scope=scope,
        )
        source_path.write_text("bbbb", encoding="utf-8")
        os.utime(
            source_path,
            ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
        )
        cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
            fingerprint_scope=scope,
        )

    assert calls == 2


def test_validation_session_zero_windows_change_token_disables_digest_memo(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    source_path = tmp_path / "source.txt"
    source_path.write_text("aaaa", encoding="utf-8")
    original_stat = source_path.stat()
    calls: list[str] = []

    def fixed_signature(path: Path, path_stat: Any) -> tuple[int, int, int, int, int, int, int]:
        del path
        return (
            int(path_stat.st_size),
            int(path_stat.st_mtime_ns),
            1,
            0,
            int(path_stat.st_mode),
            int(getattr(path_stat, "st_ino", 0)),
            int(getattr(path_stat, "st_dev", 0)),
        )

    monkeypatch.setattr(validation_session_module, "_WINDOWS_CHANGE_TOKEN_REQUIRED", True)
    monkeypatch.setattr(validation_session_module, "_file_signature", fixed_signature)

    def validator() -> dict[str, Any]:
        content = source_path.read_text(encoding="utf-8")
        calls.append(content)
        return {"status": "PASS", "content": content}

    scope = ArtifactFingerprintScope(paths=(source_path,))
    with artifact_validation_session():
        cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
            fingerprint_scope=scope,
        )
        source_path.write_text("bbbb", encoding="utf-8")
        os.utime(
            source_path,
            ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
        )
        changed = cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
            fingerprint_scope=scope,
        )

    assert calls == ["aaaa", "bbbb"]
    assert changed["content"] == "bbbb"


def test_validation_session_metadata_path_invalidates_on_touch(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    source_path = tmp_path / "prices.csv"
    source_path.write_text("date,close\n2026-07-16,100\n", encoding="utf-8")
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS"}

    scope = ArtifactFingerprintScope(metadata_paths=(source_path,))
    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_kwargs={},
                artifact_root=artifact_root,
                fingerprint_scope=scope,
            )
        source_stat = source_path.stat()
        os.utime(
            source_path,
            ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns + 2_000_000_000),
        )
        cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
            fingerprint_scope=scope,
        )

    assert calls == 2


@pytest.mark.parametrize(
    "binding_kind",
    (
        "path_checksum",
        "path_checksum_sha256",
        "path_file_contents",
        "prefixed_path_checksum",
        "prefixed_path_generic_checksum_sha256",
        "plural_path_singular_checksum",
    ),
)
def test_validation_session_recognizes_explicit_commitment_formats(
    tmp_path: Path,
    binding_kind: str,
) -> None:
    artifact_root = tmp_path / binding_kind
    artifact_root.mkdir()
    source_path = tmp_path / f"{binding_kind}.txt"
    source_path.write_text("first", encoding="utf-8")
    if binding_kind == "path_checksum":
        binding = {"path": str(source_path), "checksum": "declared"}
    elif binding_kind == "path_checksum_sha256":
        binding = {"path": str(source_path), "checksum_sha256": "declared"}
    elif binding_kind == "path_file_contents":
        binding = {"path": str(source_path), "file_contents": "first"}
    elif binding_kind == "prefixed_path_checksum":
        binding = {
            "policy_path": str(source_path),
            "policy_checksum": "declared",
        }
    elif binding_kind == "prefixed_path_generic_checksum_sha256":
        binding = {
            "source_path": str(source_path),
            "checksum_sha256": "declared",
        }
    else:
        binding = {
            "prices_path": str(source_path),
            "price_checksum": "declared",
        }
    (artifact_root / "snapshot.json").write_text(
        json.dumps({"binding": binding}, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS"}

    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_kwargs={},
                artifact_root=artifact_root,
            )
        source_path.write_text("second", encoding="utf-8")
        cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
        )

    assert calls == 2


def test_validation_session_discovers_download_timestamp_metadata_binding(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    source_path = tmp_path / "rates.csv"
    source_path.write_text("date,rate\n2026-07-16,0.05\n", encoding="utf-8")
    (artifact_root / "snapshot.json").write_text(
        json.dumps(
            {
                "source": {
                    "path": str(source_path),
                    "download_timestamp": "2026-07-16T00:00:00+00:00",
                }
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS"}

    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_kwargs={},
                artifact_root=artifact_root,
            )
        source_stat = source_path.stat()
        os.utime(
            source_path,
            ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns + 2_000_000_000),
        )
        cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
        )

    assert calls == 2


def test_validation_session_invalidates_nested_artifact_bytes(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifact-1"
    nested_path = artifact_root / "nested" / "payload.txt"
    nested_path.parent.mkdir(parents=True)
    nested_path.write_text("first", encoding="utf-8")
    calls: list[str] = []

    def validator() -> dict[str, Any]:
        content = nested_path.read_text(encoding="utf-8")
        calls.append(content)
        return {"status": "PASS", "content": content}

    with artifact_validation_session():
        cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
        )
        nested_path.write_text("second", encoding="utf-8")
        changed = cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
        )

    assert calls == ["first", "second"]
    assert changed["content"] == "second"


def test_validation_session_separates_same_qualname_validator_instances(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    calls: list[str] = []

    def make_validator(label: str) -> Any:
        def validator() -> dict[str, Any]:
            calls.append(label)
            return {"status": "PASS", "label": label}

        return validator

    first_validator = make_validator("first")
    second_validator = make_validator("second")
    assert first_validator.__qualname__ == second_validator.__qualname__

    with artifact_validation_session():
        first = cached_artifact_validation(
            validator=first_validator,
            validator_kwargs={},
            artifact_root=artifact_root,
        )
        second = cached_artifact_validation(
            validator=second_validator,
            validator_kwargs={},
            artifact_root=artifact_root,
        )

    assert calls == ["first", "second"]
    assert first["label"] == "first"
    assert second["label"] == "second"


def test_validation_session_rejects_conflicting_legacy_artifact_root(
    tmp_path: Path,
) -> None:
    real_root = tmp_path / "real" / "artifact-1"
    real_root.mkdir(parents=True)
    unrelated_root = tmp_path / "unrelated"
    unrelated_root.mkdir()

    def validator(*, item_id: str, output_dir: Path) -> dict[str, Any]:
        return {"status": "PASS", "item_id": item_id, "output_dir": str(output_dir)}

    with pytest.raises(ValueError, match="artifact_root conflicts"):
        cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id="artifact-1",
            root=tmp_path / "real",
            artifact_root=unrelated_root,
        )


@pytest.mark.parametrize("artifact_id", ("..", "../escape", "nested/artifact", "C:\\escape"))
def test_validation_session_rejects_legacy_artifact_id_path_escape(
    tmp_path: Path,
    artifact_id: str,
) -> None:
    def validator(*, item_id: str, output_dir: Path) -> dict[str, Any]:
        return {"status": "PASS", "item_id": item_id, "output_dir": str(output_dir)}

    with pytest.raises(ValueError, match="artifact_id"):
        cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )


def test_validation_session_ignores_empty_bound_path(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    (artifact_root / "snapshot.json").write_text(
        json.dumps({"optional": {"path": "", "exists": False}}) + "\n",
        encoding="utf-8",
    )
    unrelated_path = tmp_path / "unrelated.txt"
    unrelated_path.write_text("first", encoding="utf-8")
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS"}

    with artifact_validation_session():
        cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
        )
        unrelated_path.write_text("second", encoding="utf-8")
        cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
        )

    assert calls == 1


def test_validation_session_semantic_path_cannot_collide_with_user_mapping(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    semantic_path = tmp_path / "semantic"
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS", "calls": calls}

    with artifact_validation_session():
        path_result = cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
            semantic_key=semantic_path,
        )
        mapping_result = cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
            semantic_key={"$path": str(semantic_path.resolve())},
        )

    assert calls == 2
    assert path_result["calls"] == 1
    assert mapping_result["calls"] == 2


def test_validation_session_distinguishes_lexical_path_semantics(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    semantic_root = tmp_path / "semantic"
    lexical_alias = semantic_root / ".." / semantic_root.name
    assert semantic_root.resolve() == lexical_alias.resolve()
    calls = 0

    def validator(*, output_dir: Path) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS", "output_dir": str(output_dir)}

    with artifact_validation_session():
        first = cached_artifact_validation(
            validator=validator,
            validator_kwargs={"output_dir": semantic_root},
            artifact_root=artifact_root,
        )
        second = cached_artifact_validation(
            validator=validator,
            validator_kwargs={"output_dir": lexical_alias},
            artifact_root=artifact_root,
        )

    assert calls == 2
    assert first["output_dir"] != second["output_dir"]


def test_validation_session_semantic_set_and_frozenset_do_not_collide(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS", "calls": calls}

    with artifact_validation_session():
        set_result = cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
            semantic_key={"value"},
        )
        frozenset_result = cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
            semantic_key=frozenset({"value"}),
        )

    assert calls == 2
    assert set_result["calls"] == 1
    assert frozenset_result["calls"] == 2


def test_validation_session_preserves_mapping_iteration_semantics(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    calls: list[str] = []

    def validator(*, policy: dict[str, int]) -> dict[str, Any]:
        first = next(iter(policy))
        calls.append(first)
        return {"status": "PASS", "first": first}

    with artifact_validation_session():
        first = cached_artifact_validation(
            validator=validator,
            validator_kwargs={"policy": {"allow": 1, "deny": 2}},
            artifact_root=artifact_root,
        )
        second = cached_artifact_validation(
            validator=validator,
            validator_kwargs={"policy": {"deny": 2, "allow": 1}},
            artifact_root=artifact_root,
        )

    assert calls == ["allow", "deny"]
    assert first["first"] == "allow"
    assert second["first"] == "deny"


def test_validation_session_distinguishes_datetime_fold_semantics(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    calls: list[int] = []

    def validator(*, cutoff: datetime) -> dict[str, Any]:
        calls.append(cutoff.fold)
        return {"status": "PASS", "fold": cutoff.fold}

    with artifact_validation_session():
        first = cached_artifact_validation(
            validator=validator,
            validator_kwargs={"cutoff": datetime(2026, 11, 1, 1, 30, fold=0)},
            artifact_root=artifact_root,
        )
        second = cached_artifact_validation(
            validator=validator,
            validator_kwargs={"cutoff": datetime(2026, 11, 1, 1, 30, fold=1)},
            artifact_root=artifact_root,
        )

    assert calls == [0, 1]
    assert first["fold"] == 0
    assert second["fold"] == 1


@pytest.mark.parametrize("location", ["root", "explicit", "recursive", "inventory"])
def test_validation_session_bypasses_cache_for_unsupported_content_topology(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    location: str,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    scope = ArtifactFingerprintScope()
    special_path = artifact_root
    if location == "explicit":
        special_path = tmp_path / "external.bin"
        special_path.write_bytes(b"external")
        scope = ArtifactFingerprintScope(paths=(special_path,))
    elif location == "recursive":
        dependency_root = tmp_path / "external"
        dependency_root.mkdir()
        special_path = dependency_root / "nested.bin"
        special_path.write_bytes(b"nested")
        scope = ArtifactFingerprintScope(paths=(dependency_root,))
    elif location == "inventory":
        inventory_root = tmp_path / "inventory"
        inventory_root.mkdir()
        special_path = inventory_root / "source.json"
        special_path.write_text("{}\n", encoding="utf-8")
        scope = ArtifactFingerprintScope(
            inventories=(
                ArtifactFingerprintInventory(
                    root=inventory_root,
                    patterns=("*.json",),
                ),
            )
        )

    special_path = special_path.resolve()
    path_type = type(special_path)
    original_stat = path_type.stat

    def special_stat(
        self: Path,
        *,
        follow_symlinks: bool = True,
    ) -> os.stat_result:
        if self == special_path:
            return os.stat_result((stat.S_IFIFO, 0, 0, 1, 0, 0, 0, 0, 0, 0))
        return original_stat(self, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(path_type, "stat", special_stat)
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS", "calls": calls}

    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_kwargs={},
                artifact_root=artifact_root,
                fingerprint_scope=scope,
            )

    assert calls == 2


def test_validation_session_bypasses_cache_for_linked_artifact_entry(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    target_root = tmp_path / "target"
    target_root.mkdir()
    payload_path = target_root / "payload.txt"
    payload_path.write_text("first", encoding="utf-8")
    linked_root = artifact_root / "linked"
    try:
        linked_root.symlink_to(target_root, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlink unavailable: {exc}")
    calls: list[str] = []

    def validator() -> dict[str, Any]:
        content = (linked_root / "payload.txt").read_text(encoding="utf-8")
        calls.append(content)
        return {"status": "PASS", "content": content}

    with artifact_validation_session():
        cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
        )
        payload_path.write_text("second", encoding="utf-8")
        changed = cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
        )

    assert calls == ["first", "second"]
    assert changed["content"] == "second"


def test_validation_session_bypasses_cache_for_automatic_directory_binding(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    bound_directory = tmp_path / "bound-directory"
    bound_directory.mkdir()
    (bound_directory / "payload.txt").write_text("stable", encoding="utf-8")
    (artifact_root / "snapshot.json").write_text(
        json.dumps({"source": {"path": str(bound_directory), "exists": True}}) + "\n",
        encoding="utf-8",
    )
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS"}

    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_kwargs={},
                artifact_root=artifact_root,
            )

    assert calls == 2


def test_validation_session_copy_cannot_reuse_after_outer_context_exits(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS", "calls": calls}

    def validate() -> dict[str, Any]:
        return cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
        )

    with artifact_validation_session():
        validate()
        copied = copy_context()

    copied.run(validate)
    copied.run(validate)
    assert calls == 3


def test_validation_session_unsupported_semantic_value_bypasses_cache(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    calls = 0

    def validator(*, threshold: Decimal) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS", "threshold": str(threshold)}

    with artifact_validation_session():
        for _ in range(2):
            result = cached_artifact_validation(
                validator=validator,
                validator_kwargs={"threshold": Decimal("0.1")},
                artifact_root=artifact_root,
            )

    assert calls == 2
    assert result["threshold"] == "0.1"


def test_validation_session_excess_automatic_bindings_bypass_cache(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    (artifact_root / "snapshot.json").write_text(
        json.dumps(
            {
                "sources": [
                    {"path": str(tmp_path / "one"), "exists": False},
                    {"path": str(tmp_path / "two"), "exists": False},
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(validation_session_module, "_MAX_AUTOMATIC_BOUND_PATHS", 1)
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS"}

    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_kwargs={},
                artifact_root=artifact_root,
            )

    assert calls == 2


def test_validation_session_malformed_automatic_path_bypasses_cache(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    (artifact_root / "snapshot.json").write_text(
        json.dumps({"source": {"path": "bad\u0000name", "exists": False}}) + "\n",
        encoding="utf-8",
    )
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "FAIL"}

    with artifact_validation_session():
        for _ in range(2):
            result = cached_artifact_validation(
                validator=validator,
                validator_kwargs={},
                artifact_root=artifact_root,
            )

    assert calls == 2
    assert result["status"] == "FAIL"


def test_validation_session_uncopyable_pass_report_bypasses_cache(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    calls = 0

    class NoCopy:
        def __deepcopy__(self, memo: dict[int, Any]) -> Any:
            del memo
            raise TypeError("not copyable")

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS", "value": NoCopy()}

    with artifact_validation_session():
        for _ in range(2):
            result = cached_artifact_validation(
                validator=validator,
                validator_kwargs={},
                artifact_root=artifact_root,
            )

    assert calls == 2
    assert result["status"] == "PASS"


def test_validation_session_bypasses_when_file_changes_during_hash(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    (artifact_root / "payload.txt").write_text("stable", encoding="utf-8")
    signature_calls = 0
    validator_calls = 0

    def changing_signature(
        path: Path,
        path_stat: Any,
    ) -> tuple[int, int, int, int, int, int, int]:
        nonlocal signature_calls
        del path
        signature_calls += 1
        return (
            int(path_stat.st_size),
            int(path_stat.st_mtime_ns),
            signature_calls,
            1,
            int(path_stat.st_mode),
            int(getattr(path_stat, "st_ino", 0)),
            int(getattr(path_stat, "st_dev", 0)),
        )

    monkeypatch.setattr(validation_session_module, "_file_signature", changing_signature)

    def validator() -> dict[str, Any]:
        nonlocal validator_calls
        validator_calls += 1
        return {"status": "PASS"}

    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_kwargs={},
                artifact_root=artifact_root,
            )

    assert signature_calls == 4
    assert validator_calls == 2


def test_raw_fingerprint_mode_handles_large_json_without_cross_mode_memo_reuse(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    snapshot_path = artifact_root / "snapshot.json"
    snapshot_path.write_text(json.dumps({"payload": "x" * 128}), encoding="utf-8")
    monkeypatch.setattr(validation_session_module, "_MAX_COMMITMENT_JSON_SIZE_BYTES", 32)
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS", "calls": calls}

    raw_scope = ArtifactFingerprintScope(discover_bound_paths=False)
    with artifact_validation_session():
        assert artifact_content_identity(snapshot_path) is not None
        for _ in range(2):
            raw_result = cached_artifact_validation(
                validator=validator,
                validator_kwargs={},
                artifact_root=artifact_root,
                fingerprint_scope=raw_scope,
            )
        for _ in range(2):
            discovered_result = cached_artifact_validation(
                validator=validator,
                validator_kwargs={},
                artifact_root=artifact_root,
            )

    assert raw_result["calls"] == 1
    assert discovered_result["calls"] == 3
    assert calls == 3


def test_compatibility_fingerprint_caches_bounded_high_node_snapshot(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "artifacts"
    artifact_root = output_dir / "artifact-1"
    artifact_root.mkdir(parents=True)
    (artifact_root / "snapshot.json").write_text(
        json.dumps({"payload": list(range(100_001))}),
        encoding="utf-8",
    )
    calls = 0

    def validator(*, artifact_id: str, output_dir: Path) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {
            "status": "PASS",
            "artifact_id": artifact_id,
            "output_dir": str(output_dir),
            "calls": calls,
        }

    with artifact_validation_session():
        for _ in range(2):
            result = cached_artifact_validation(
                validator=validator,
                validator_key="artifact_id",
                artifact_id="artifact-1",
                root=output_dir,
            )

    assert calls == 1
    assert result["calls"] == 1


def test_compatibility_fingerprint_bypasses_cache_above_node_limit(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    output_dir = tmp_path / "artifacts"
    artifact_root = output_dir / "artifact-1"
    artifact_root.mkdir(parents=True)
    (artifact_root / "snapshot.json").write_text(
        json.dumps({"payload": list(range(16))}),
        encoding="utf-8",
    )
    monkeypatch.setattr(validation_session_module, "_MAX_COMMITMENT_JSON_NODES", 8)
    calls = 0

    def validator(*, artifact_id: str, output_dir: Path) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {
            "status": "PASS",
            "artifact_id": artifact_id,
            "output_dir": str(output_dir),
            "calls": calls,
        }

    with artifact_validation_session():
        for _ in range(2):
            result = cached_artifact_validation(
                validator=validator,
                validator_key="artifact_id",
                artifact_id="artifact-1",
                root=output_dir,
            )

    assert calls == 2
    assert result["calls"] == 2


def test_validation_session_path_subclass_semantics_bypass_cache(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    derived_path_type = type("DerivedPath", (type(Path()),), {})
    derived_path = derived_path_type(str(tmp_path / "semantic"))
    calls = 0

    def validator(*, output_path: Path) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS", "path": str(output_path)}

    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_kwargs={"output_path": derived_path},
                artifact_root=artifact_root,
            )

    assert calls == 2


def test_validation_session_shared_mutable_alias_semantics_bypass_cache(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    shared: list[str] = []
    calls: list[bool] = []

    def validator(*, policy: dict[str, list[str]]) -> dict[str, Any]:
        aliased = policy["left"] is policy["right"]
        calls.append(aliased)
        return {"status": "PASS", "aliased": aliased}

    with artifact_validation_session():
        aliased = cached_artifact_validation(
            validator=validator,
            validator_kwargs={"policy": {"left": shared, "right": shared}},
            artifact_root=artifact_root,
        )
        independent = cached_artifact_validation(
            validator=validator,
            validator_kwargs={"policy": {"left": [], "right": []}},
            artifact_root=artifact_root,
        )

    assert calls == [True, False]
    assert aliased["aliased"] is True
    assert independent["aliased"] is False


def test_validation_session_repeated_set_semantics_bypass_cache(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS"}

    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_kwargs={},
                artifact_root=artifact_root,
                semantic_key={"value"},
            )

    assert calls == 2


@pytest.mark.parametrize("pattern", ("**/*", "../*"))
def test_validation_session_unsafe_inventory_pattern_bypasses_cache(
    tmp_path: Path,
    pattern: str,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    inventory_root = tmp_path / "inventory"
    artifact_root.mkdir()
    inventory_root.mkdir()
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS"}

    scope = ArtifactFingerprintScope(
        inventories=(ArtifactFingerprintInventory(inventory_root, (pattern,)),)
    )
    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_kwargs={},
                artifact_root=artifact_root,
                fingerprint_scope=scope,
            )

    assert calls == 2


def test_validation_session_excess_inventory_count_bypasses_cache(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    monkeypatch.setattr(validation_session_module, "_MAX_FINGERPRINT_INVENTORIES", 1)
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS"}

    scope = ArtifactFingerprintScope(
        inventories=(
            ArtifactFingerprintInventory(tmp_path / "one", ("*",)),
            ArtifactFingerprintInventory(tmp_path / "two", ("*",)),
        )
    )
    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_kwargs={},
                artifact_root=artifact_root,
                fingerprint_scope=scope,
            )

    assert calls == 2


def test_validation_session_bypasses_dependency_below_linked_parent(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    target_root = tmp_path / "target"
    target_root.mkdir()
    (target_root / "source.txt").write_text("stable", encoding="utf-8")
    linked_parent = tmp_path / "linked-parent"
    try:
        linked_parent.symlink_to(target_root, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlink unavailable: {exc}")
    source_path = linked_parent / "source.txt"
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS", "content": source_path.read_text(encoding="utf-8")}

    scope = ArtifactFingerprintScope(paths=(source_path,))
    with artifact_validation_session():
        for _ in range(2):
            result = cached_artifact_validation(
                validator=validator,
                validator_kwargs={},
                artifact_root=artifact_root,
                fingerprint_scope=scope,
            )

    assert calls == 2
    assert result["content"] == "stable"


def test_validation_session_relative_automatic_binding_bypasses_cache(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    (artifact_root / "snapshot.json").write_text(
        json.dumps({"source": {"path": "relative/source.csv", "checksum": "declared"}}),
        encoding="utf-8",
    )
    calls = 0

    def validator() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "PASS"}

    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_kwargs={},
                artifact_root=artifact_root,
            )

    assert calls == 2


def test_download_timestamp_binding_hashes_content_when_mtime_is_restored(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    source_path = tmp_path / "rates.csv"
    source_path.write_text("rate=1", encoding="utf-8")
    source_stat = source_path.stat()
    (artifact_root / "snapshot.json").write_text(
        json.dumps(
            {
                "source": {
                    "path": str(source_path),
                    "download_timestamp": "2026-07-16T00:00:00+00:00",
                }
            }
        ),
        encoding="utf-8",
    )
    calls: list[str] = []

    def validator() -> dict[str, Any]:
        content = source_path.read_text(encoding="utf-8")
        calls.append(content)
        return {"status": "PASS", "content": content}

    with artifact_validation_session():
        cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
        )
        source_path.write_text("rate=2", encoding="utf-8")
        os.utime(
            source_path,
            ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns),
        )
        changed = cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
        )

    assert calls == ["rate=1", "rate=2"]
    assert changed["content"] == "rate=2"


def test_validation_session_string_subclass_mapping_key_bypasses_cache(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    calls: list[str] = []

    class DerivedString(str):
        pass

    def validator(*, policy: dict[str, int]) -> dict[str, Any]:
        key = next(iter(policy))
        calls.append(type(key).__name__)
        return {"status": "PASS", "key_type": type(key).__name__}

    with artifact_validation_session():
        derived = cached_artifact_validation(
            validator=validator,
            validator_kwargs={"policy": {DerivedString("limit"): 1}},
            artifact_root=artifact_root,
        )
        plain = cached_artifact_validation(
            validator=validator,
            validator_kwargs={"policy": {"limit": 1}},
            artifact_root=artifact_root,
        )

    assert calls == ["DerivedString", "str"]
    assert derived["key_type"] == "DerivedString"
    assert plain["key_type"] == "str"


def test_validation_session_rechecks_fingerprint_before_cache_hit(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    artifact_root = tmp_path / "artifact-1"
    artifact_root.mkdir()
    payload_path = artifact_root / "payload.txt"
    payload_path.write_text("first", encoding="utf-8")
    calls: list[str] = []

    def validator() -> dict[str, Any]:
        content = payload_path.read_text(encoding="utf-8")
        calls.append(content)
        return {"status": "PASS", "content": content}

    real_fingerprint = validation_session_module.artifact_fingerprint
    with artifact_validation_session():
        old_fingerprint = real_fingerprint(artifact_root)
        cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
        )
        payload_path.write_text("second", encoding="utf-8")
        current_fingerprint = real_fingerprint(artifact_root)
        fingerprint_calls = 0

        def delayed_fingerprint(root: Path, *, scope: Any = None) -> str:
            nonlocal fingerprint_calls
            del root, scope
            fingerprint_calls += 1
            return old_fingerprint if fingerprint_calls == 1 else current_fingerprint

        monkeypatch.setattr(
            validation_session_module,
            "artifact_fingerprint",
            delayed_fingerprint,
        )
        changed = cached_artifact_validation(
            validator=validator,
            validator_kwargs={},
            artifact_root=artifact_root,
        )

    assert fingerprint_calls == 3
    assert calls == ["first", "second"]
    assert changed["content"] == "second"
