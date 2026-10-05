"""DEVX-022 S7b: the canonical-YAML byte check is memoized by content digest only.

The check "these exact bytes equal the canonical serialization of the mapping they parse to" is a
pure function of the bytes. The memo must never cache filesystem state: the file is read and parsed
on every call, only passing digests are stored, and any changed byte is checked in full.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.platform.architecture import task_registry_canonical as canonical
from ai_trading_system.yaml_loader import safe_load_yaml_path


def _reference_load_generated_mapping(path: Path) -> dict[str, Any]:
    """Pre-S7b behaviour: parse the file, then compare a second read with the re-serialization."""
    value = canonical._load_mapping(path)
    if path.read_bytes() != canonical._yaml_bytes(value):
        canonical._fail("NON_CANONICAL_GENERATED_YAML", str(path))
    return value


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return canonical._yaml_bytes(payload)


PAYLOAD: dict[str, Any] = {
    "schema_version": "example.v1",
    "task_id": "EXAMPLE-1",
    "projection": {"status": "OPEN", "owner": "someone", "notes": ["a", "b: c", "ünï"]},
}


@pytest.fixture(autouse=True)
def _fresh_memo() -> None:
    canonical._CANONICAL_YAML_VERIFIED.clear()


def _outcome(function: Any, path: Path) -> tuple[str, Any]:
    try:
        return "ok", function(path)
    except canonical.CanonicalTaskRegistryError as exc:
        return "error", exc.code


@pytest.mark.parametrize(
    "name,raw",
    [
        ("canonical", _canonical_bytes(PAYLOAD)),
        ("trailing_comment", _canonical_bytes(PAYLOAD) + b"# not canonical\n"),
        ("reordered_keys", b"task_id: EXAMPLE-1\nschema_version: example.v1\n"),
        ("crlf", _canonical_bytes(PAYLOAD).replace(b"\n", b"\r\n")),
        ("lone_cr", _canonical_bytes(PAYLOAD).replace(b"\n", b"\r")),
        ("list_not_mapping", b"- a\n- b\n"),
    ],
)
def test_memoized_loader_agrees_with_the_reference_for_every_input_shape(
    tmp_path: Path, name: str, raw: bytes
) -> None:
    path = tmp_path / f"{name}.yaml"
    path.write_bytes(raw)
    expected = _outcome(_reference_load_generated_mapping, path)
    # first call (populates the memo for canonical content) and second call (memo hit)
    assert _outcome(canonical._load_generated_mapping, path) == expected
    assert _outcome(canonical._load_generated_mapping, path) == expected


def test_repeated_load_of_identical_bytes_reads_and_parses_every_time_but_serializes_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "fragment.yaml"
    path.write_bytes(_canonical_bytes(PAYLOAD))
    serializations = 0
    reads = 0
    real_yaml_bytes = canonical._yaml_bytes
    real_read_bytes = Path.read_bytes

    def counting_yaml_bytes(payload: dict[str, Any]) -> bytes:
        nonlocal serializations
        serializations += 1
        return real_yaml_bytes(payload)

    def counting_read_bytes(self: Path) -> bytes:
        nonlocal reads
        reads += 1
        return real_read_bytes(self)

    monkeypatch.setattr(canonical, "_yaml_bytes", counting_yaml_bytes)
    monkeypatch.setattr(Path, "read_bytes", counting_read_bytes)
    for _ in range(3):
        assert canonical._load_generated_mapping(path) == PAYLOAD
    assert serializations == 1  # the repeated byte comparison is skipped
    assert reads == 3  # the filesystem is still read on every call


def test_a_changed_byte_is_checked_in_full_even_after_the_canonical_bytes_were_memoized(
    tmp_path: Path,
) -> None:
    path = tmp_path / "fragment.yaml"
    path.write_bytes(_canonical_bytes(PAYLOAD))
    assert canonical._load_generated_mapping(path) == PAYLOAD
    for tampered in (
        _canonical_bytes(PAYLOAD) + b"\n",
        _canonical_bytes(PAYLOAD).replace(b"OPEN", b"DONE"),
        b"task_id: EXAMPLE-1\nschema_version: example.v1\n",
    ):
        path.write_bytes(tampered)
        expected = _outcome(_reference_load_generated_mapping, path)
        assert _outcome(canonical._load_generated_mapping, path) == expected
    # a parse-equal but byte-different file is rejected, not served from the memo
    path.write_bytes(_canonical_bytes(PAYLOAD) + b"# comment\n")
    assert _outcome(canonical._load_generated_mapping, path) == (
        "error",
        "NON_CANONICAL_GENERATED_YAML",
    )


def test_non_canonical_content_is_never_cached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "fragment.yaml"
    path.write_bytes(_canonical_bytes(PAYLOAD) + b"# comment\n")
    serializations = 0
    real_yaml_bytes = canonical._yaml_bytes

    def counting_yaml_bytes(payload: dict[str, Any]) -> bytes:
        nonlocal serializations
        serializations += 1
        return real_yaml_bytes(payload)

    monkeypatch.setattr(canonical, "_yaml_bytes", counting_yaml_bytes)
    for _ in range(2):
        assert _outcome(canonical._load_generated_mapping, path) == (
            "error",
            "NON_CANONICAL_GENERATED_YAML",
        )
    assert serializations == 2
    assert not canonical._CANONICAL_YAML_VERIFIED


def test_the_memo_stores_only_digests_and_each_call_returns_a_fresh_mapping(
    tmp_path: Path,
) -> None:
    path = tmp_path / "fragment.yaml"
    path.write_bytes(_canonical_bytes(PAYLOAD))
    first = canonical._load_generated_mapping(path)
    first["injected"] = True
    first["projection"]["status"] = "MUTATED"
    second = canonical._load_generated_mapping(path)
    assert second == PAYLOAD and second is not first
    memo = canonical._CANONICAL_YAML_VERIFIED
    assert all(type(item) is bytes and len(item) == 32 for item in memo)
    assert safe_load_yaml_path(path) == PAYLOAD
