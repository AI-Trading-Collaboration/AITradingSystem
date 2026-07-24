from __future__ import annotations

import hashlib
import json
import os
import platform
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.contracts.artifact_envelope import ArtifactPointer

DEFAULT_FILE_HASH_CHUNK_SIZE_BYTES = 1024 * 1024
# Windows may briefly deny an atomic replace while a concurrent reader still
# holds the destination. Keep the established external-cache budget here at
# the canonical replace boundary so callers do not stack independent retries.
_WINDOWS_ATOMIC_REPLACE_MAX_ATTEMPTS = 8
_WINDOWS_ATOMIC_REPLACE_BASE_DELAY_SECONDS = 0.005
_WINDOWS_TRANSIENT_REPLACE_WINERRORS = frozenset({5, 32, 33})


class ArtifactWriteError(OSError):
    def __init__(self, code: str, path: Path, message: str) -> None:
        self.code = code
        self.path = path
        self.message = message
        super().__init__(f"{code}: {path}: {message}")


@dataclass(frozen=True)
class ArtifactWriteResult:
    path: Path
    artifact_type: str
    sha256: str
    size_bytes: int
    atomic: bool = True

    def to_pointer(self, *, schema_version: str) -> ArtifactPointer:
        return ArtifactPointer(
            path=str(self.path),
            artifact_type=self.artifact_type,
            sha256=self.sha256,
            size_bytes=self.size_bytes,
            schema_version=schema_version,
        )


@dataclass(frozen=True)
class RuntimeMetadata:
    generated_at: datetime
    python_version: str
    platform: str
    working_directory: str
    process_id: int

    def to_dict(self) -> dict[str, object]:
        return {
            "generated_at": self.generated_at.isoformat(),
            "python_version": self.python_version,
            "platform": self.platform,
            "working_directory": self.working_directory,
            "process_id": self.process_id,
        }


def capture_runtime_metadata(*, generated_at: datetime | None = None) -> RuntimeMetadata:
    timestamp = generated_at or datetime.now(tz=UTC)
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("generated_at must be timezone-aware")
    return RuntimeMetadata(
        generated_at=timestamp,
        python_version=platform.python_version(),
        platform=platform.platform(),
        working_directory=str(Path.cwd()),
        process_id=os.getpid(),
    )


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def sha256_path(
    path: Path,
    *,
    chunk_size: int = DEFAULT_FILE_HASH_CHUNK_SIZE_BYTES,
) -> str:
    """Return a streaming SHA-256 hex digest for a file path."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_json_bytes(
    payload: Any,
    *,
    sort_keys: bool = True,
    indent: int | None = 2,
    ensure_ascii: bool = False,
    trailing_newline: bool = True,
    default: Callable[[Any], Any] | None = None,
) -> bytes:
    text = json.dumps(
        payload,
        ensure_ascii=ensure_ascii,
        indent=indent,
        sort_keys=sort_keys,
        default=default,
    )
    if trailing_newline:
        text += "\n"
    return text.encode("utf-8")


def write_bytes_atomic(path: Path, content: bytes) -> ArtifactWriteResult:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(file_descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        _replace_with_bounded_windows_contention_retry(temporary_path, path)
    except Exception as exc:
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise ArtifactWriteError("ATOMIC_ARTIFACT_WRITE_FAILED", path, str(exc)) from exc
    return ArtifactWriteResult(
        path=path,
        artifact_type=path.suffix.lower().lstrip(".") or "file",
        sha256=sha256_bytes(content),
        size_bytes=len(content),
    )


def _replace_with_bounded_windows_contention_retry(
    source: Path,
    destination: Path,
) -> None:
    for attempt in range(_WINDOWS_ATOMIC_REPLACE_MAX_ATTEMPTS):
        try:
            os.replace(source, destination)
        except OSError as exc:
            if (
                getattr(exc, "winerror", None) not in _WINDOWS_TRANSIENT_REPLACE_WINERRORS
                or attempt + 1 >= _WINDOWS_ATOMIC_REPLACE_MAX_ATTEMPTS
            ):
                raise
            time.sleep(_WINDOWS_ATOMIC_REPLACE_BASE_DELAY_SECONDS * (2**attempt))
        else:
            return
    raise AssertionError("atomic replace retry loop exhausted without returning or raising")


def write_text_atomic(path: Path, content: str) -> ArtifactWriteResult:
    return write_bytes_atomic(path, content.encode("utf-8"))


def write_markdown_atomic(path: Path, content: str) -> ArtifactWriteResult:
    return write_text_atomic(path, content)


def write_json_atomic(
    path: Path,
    payload: Any,
    *,
    sort_keys: bool = True,
    indent: int | None = 2,
    ensure_ascii: bool = False,
    trailing_newline: bool = True,
    default: Callable[[Any], Any] | None = None,
) -> ArtifactWriteResult:
    return write_bytes_atomic(
        path,
        canonical_json_bytes(
            payload,
            sort_keys=sort_keys,
            indent=indent,
            ensure_ascii=ensure_ascii,
            trailing_newline=trailing_newline,
            default=default,
        ),
    )


def write_json_atomic_without_trailing_newline(
    path: Path,
    payload: Any,
    *,
    sort_keys: bool = True,
    indent: int | None = 2,
    ensure_ascii: bool = False,
    default: Callable[[Any], Any] | None = None,
) -> ArtifactWriteResult:
    """Write canonical JSON atomically while preserving no-newline byte contracts."""
    return write_json_atomic(
        path,
        payload,
        sort_keys=sort_keys,
        indent=indent,
        ensure_ascii=ensure_ascii,
        trailing_newline=False,
        default=default,
    )


def write_yaml_atomic(
    path: Path,
    payload: Any,
    *,
    sort_keys: bool = True,
    allow_unicode: bool = True,
) -> ArtifactWriteResult:
    text = yaml.safe_dump(payload, sort_keys=sort_keys, allow_unicode=allow_unicode)
    return write_text_atomic(path, text)
