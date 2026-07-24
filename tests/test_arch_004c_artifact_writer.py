from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from ai_trading_system.platform.artifacts import (
    ArtifactWriteError,
    canonical_json_bytes,
    capture_runtime_metadata,
    write_bytes_atomic,
    write_json_atomic,
    write_markdown_atomic,
    write_yaml_atomic,
)


def test_canonical_json_writer_returns_checksum_pointer_and_deterministic_bytes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "nested" / "artifact.json"

    result = write_json_atomic(path, {"z": "最后", "a": 1})
    expected = b'{\n  "a": 1,\n  "z": "\xe6\x9c\x80\xe5\x90\x8e"\n}\n'

    assert path.read_bytes() == expected
    assert result.sha256 == hashlib.sha256(expected).hexdigest()
    assert result.size_bytes == len(expected)
    assert result.atomic is True
    assert result.to_pointer(schema_version="example.v1").to_dict() == {
        "path": str(path),
        "artifact_type": "json",
        "sha256": result.sha256,
        "size_bytes": len(expected),
        "schema_version": "example.v1",
    }


def test_canonical_json_bytes_supports_legacy_no_newline_and_unsorted_parity() -> None:
    payload = {"z": 1, "a": 2}

    content = canonical_json_bytes(
        payload,
        sort_keys=False,
        trailing_newline=False,
    )

    assert content.decode("utf-8") == json.dumps(payload, ensure_ascii=False, indent=2)


def test_markdown_and_yaml_use_same_atomic_writer(tmp_path: Path) -> None:
    markdown = tmp_path / "artifact.md"
    yaml_path = tmp_path / "artifact.yaml"

    markdown_result = write_markdown_atomic(markdown, "# 标题\n")
    yaml_result = write_yaml_atomic(yaml_path, {"z": "最后", "a": 1})

    assert markdown.read_text(encoding="utf-8") == "# 标题\n"
    assert markdown_result.sha256 == hashlib.sha256("# 标题\n".encode()).hexdigest()
    assert yaml.safe_load(yaml_path.read_text(encoding="utf-8")) == {"a": 1, "z": "最后"}
    assert yaml_result.artifact_type == "yaml"


def test_atomic_writer_preserves_existing_target_and_cleans_temp_on_replace_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "artifact.json"
    path.write_bytes(b"old-content")
    replace_calls = 0

    def fail_replace(source: Path, destination: Path) -> None:
        nonlocal replace_calls
        replace_calls += 1
        raise PermissionError(f"cannot replace {source} -> {destination}")

    monkeypatch.setattr("ai_trading_system.platform.artifacts.writer.os.replace", fail_replace)

    with pytest.raises(ArtifactWriteError, match="ATOMIC_ARTIFACT_WRITE_FAILED"):
        write_bytes_atomic(path, b"new-content")

    assert path.read_bytes() == b"old-content"
    assert replace_calls == 1
    assert list(tmp_path.glob(".artifact.json.*.tmp")) == []


def test_atomic_writer_retries_bounded_windows_replace_contention(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "artifact.json"
    path.write_bytes(b"old-content")
    real_replace = __import__("os").replace
    replace_calls = 0
    delays: list[float] = []

    def transient_then_replace(source: Path, destination: Path) -> None:
        nonlocal replace_calls
        replace_calls += 1
        if replace_calls <= 2:
            error = PermissionError("destination is temporarily open")
            error.winerror = 5
            raise error
        real_replace(source, destination)

    monkeypatch.setattr(
        "ai_trading_system.platform.artifacts.writer.os.replace",
        transient_then_replace,
    )
    monkeypatch.setattr(
        "ai_trading_system.platform.artifacts.writer.time.sleep",
        delays.append,
    )

    result = write_bytes_atomic(path, b"new-content")

    assert path.read_bytes() == b"new-content"
    assert result.sha256 == hashlib.sha256(b"new-content").hexdigest()
    assert replace_calls == 3
    assert delays == [0.005, 0.01]
    assert list(tmp_path.glob(".artifact.json.*.tmp")) == []


def test_atomic_writer_fails_closed_after_windows_contention_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "artifact.json"
    path.write_bytes(b"old-content")
    replace_calls = 0
    delays: list[float] = []

    def persistent_contention(_source: Path, _destination: Path) -> None:
        nonlocal replace_calls
        replace_calls += 1
        error = PermissionError("destination remains open")
        error.winerror = 32
        raise error

    monkeypatch.setattr(
        "ai_trading_system.platform.artifacts.writer.os.replace",
        persistent_contention,
    )
    monkeypatch.setattr(
        "ai_trading_system.platform.artifacts.writer.time.sleep",
        delays.append,
    )

    with pytest.raises(ArtifactWriteError, match="ATOMIC_ARTIFACT_WRITE_FAILED"):
        write_bytes_atomic(path, b"new-content")

    assert path.read_bytes() == b"old-content"
    assert replace_calls == 8
    assert delays == [0.005, 0.01, 0.02, 0.04, 0.08, 0.16, 0.32]
    assert list(tmp_path.glob(".artifact.json.*.tmp")) == []


def test_runtime_metadata_requires_timezone_and_is_serializable(tmp_path: Path) -> None:
    generated_at = datetime(2026, 7, 10, 22, 0, tzinfo=UTC)

    metadata = capture_runtime_metadata(generated_at=generated_at)

    assert metadata.generated_at == generated_at
    assert metadata.python_version
    assert metadata.platform
    assert metadata.process_id > 0
    assert metadata.to_dict()["generated_at"] == generated_at.isoformat()
    with pytest.raises(ValueError, match="timezone-aware"):
        capture_runtime_metadata(generated_at=datetime(2026, 7, 10, 22, 0))
