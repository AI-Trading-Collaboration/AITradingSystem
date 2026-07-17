from __future__ import annotations

import asyncio
import inspect
import json
import os
import stat
import sys
import threading
from collections import OrderedDict
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from functools import wraps
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, ParamSpec, TypeVar
from zoneinfo import ZoneInfo

_P = ParamSpec("_P")
_R = TypeVar("_R")
_ValidationKey = tuple[int, str, str]
_BoundPathDiscoveryMode = Literal["none", "compatibility", "hardened"]
_FileSignature = tuple[int, int, int, int | None, int, int, int]
_ProcessFileDigestKey = tuple[str, _BoundPathDiscoveryMode, _FileSignature]
_MAX_RECURSIVE_DIRECTORY_ENTRIES = 4_096
_MAX_INVENTORY_ENTRIES = 4_096
_MAX_FINGERPRINT_INVENTORIES = 64
_MAX_INVENTORY_PATTERNS = 32
_MAX_FINGERPRINT_FILE_SIZE_BYTES = 256 * 1024 * 1024
_MAX_RAW_FINGERPRINT_FILE_SIZE_BYTES = 1024 * 1024 * 1024
_MAX_FINGERPRINT_TOTAL_BYTES = 1024 * 1024 * 1024
_MAX_COMMITMENT_JSON_SIZE_BYTES = 64 * 1024 * 1024
_FINGERPRINT_READ_CHUNK_BYTES = 1024 * 1024
_WINDOWS_CHANGE_TOKEN_REQUIRED = os.name == "nt"
_MAX_AUTOMATIC_BOUND_PATHS = 4_096
# Large bounded research snapshots currently contain about 226k JSON nodes while
# binding only tens of external paths.  Keep them cacheable without relaxing the
# independent 64 MiB document, 4,096 bound-path, and aggregate path-shape caps.
_MAX_COMMITMENT_JSON_NODES = 500_000
_MAX_FINGERPRINT_OBSERVED_PATHS = 16_384
_MAX_BOUND_PATH_UTF8_BYTES = 32 * 1024
_MAX_BOUND_PATH_TOTAL_UTF8_BYTES = 8 * 1024 * 1024
_MAX_BOUND_PATH_COMPONENTS = 256
_MAX_BOUND_PATH_TOTAL_COMPONENTS = 16_384
_MAX_PROCESS_FILE_DIGEST_BYTES = 64 * 1024 * 1024
# Rounded well above the measured OrderedDict node/table delta plus value tuple and weight int.
_PROCESS_FILE_DIGEST_ENTRY_OVERHEAD_BYTES = 1024
_PROCESS_FILE_DIGEST_VALUE_OVERHEAD_BYTES = (
    sys.getsizeof((None, 0)) + sys.getsizeof(0)
)
_CONCRETE_PATH_TYPE = type(Path())


@dataclass(frozen=True)
class ArtifactFingerprintInventory:
    """One bounded glob inventory whose entry paths, types, and bytes affect a key."""

    root: Path
    patterns: tuple[str, ...]


@dataclass(frozen=True)
class ArtifactFingerprintScope:
    """Additional explicit files, directories, and bounded inventories to fingerprint."""

    paths: tuple[Path, ...] = ()
    metadata_paths: tuple[Path, ...] = ()
    inventories: tuple[ArtifactFingerprintInventory, ...] = ()
    discover_bound_paths: bool = True


@dataclass(frozen=True)
class _FileDigestMemo:
    signature: _FileSignature
    size_bytes: int
    digest: bytes
    content_bound_paths: tuple[str, ...]
    metadata_bound_paths: tuple[str, ...]


@dataclass
class _ValidationSessionState:
    owner: tuple[int, int, int | None]
    active: bool = True
    validations: dict[_ValidationKey, dict[str, Any]] = field(default_factory=dict)
    validator_refs: dict[int, Callable[..., dict[str, Any]]] = field(default_factory=dict)


@dataclass(frozen=True)
class _ObservedPath:
    kind: str
    size_bytes: int = 0
    digest: bytes = b""


class _UncacheableFingerprintScope(ValueError):
    """Raised when path topology cannot be fingerprinted without alias ambiguity."""


_VALIDATION_SESSION: ContextVar[_ValidationSessionState | None] = ContextVar(
    "artifact_validation_session",
    default=None,
)
_PROCESS_FILE_DIGESTS: OrderedDict[
    _ProcessFileDigestKey,
    tuple[_FileDigestMemo, int],
] = OrderedDict()
_PROCESS_FILE_DIGEST_BYTES = 0
_PROCESS_FILE_DIGEST_PID = os.getpid()
_PROCESS_FILE_DIGEST_LOCK = threading.RLock()


def _resolved(path: Path) -> Path:
    return path.expanduser().resolve(strict=False)


def _execution_owner() -> tuple[int, int, int | None]:
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    return os.getpid(), threading.get_ident(), None if task is None else id(task)


def _is_link_or_junction(path: Path) -> bool:
    try:
        expanded = path.expanduser()
        expanded_parts = expanded.parts
        if (
            len(str(expanded).encode("utf-8")) > _MAX_BOUND_PATH_UTF8_BYTES
            or len(expanded_parts) > _MAX_BOUND_PATH_COMPONENTS
            or ".." in expanded_parts
        ):
            return True
        lexical = expanded if expanded.is_absolute() else Path.cwd() / expanded
        if len(lexical.parts) > _MAX_BOUND_PATH_COMPONENTS:
            return True
        for component in reversed(lexical.parents):
            if component.is_symlink():
                return True
            is_junction = getattr(component, "is_junction", None)
            if is_junction is not None and is_junction():
                return True
        if lexical.is_symlink():
            return True
        is_junction = getattr(lexical, "is_junction", None)
        if is_junction is not None and is_junction():
            return True
        return False
    except (OSError, RuntimeError, ValueError):
        return True


def _platform_change_token(path: Path) -> int | None:
    if os.name != "nt":
        return 0
    import ctypes
    from ctypes import wintypes

    class FileBasicInfo(ctypes.Structure):
        _fields_ = (
            ("creation_time", ctypes.c_longlong),
            ("last_access_time", ctypes.c_longlong),
            ("last_write_time", ctypes.c_longlong),
            ("change_time", ctypes.c_longlong),
            ("file_attributes", wintypes.DWORD),
        )

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    create_file.restype = wintypes.HANDLE
    get_information = kernel32.GetFileInformationByHandleEx
    get_information.argtypes = (
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    )
    get_information.restype = wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL

    handle = create_file(
        str(path),
        0x0080,  # FILE_READ_ATTRIBUTES
        0x0001 | 0x0002 | 0x0004,  # FILE_SHARE_READ | WRITE | DELETE
        None,
        3,  # OPEN_EXISTING
        0x02000000,  # FILE_FLAG_BACKUP_SEMANTICS
        None,
    )
    if handle == wintypes.HANDLE(-1).value:
        return None
    try:
        info = FileBasicInfo()
        if not get_information(handle, 0, ctypes.byref(info), ctypes.sizeof(info)):
            return None
        return int(info.change_time)
    finally:
        close_handle(handle)


def _file_signature(path: Path, path_stat: Any) -> _FileSignature:
    return (
        int(path_stat.st_size),
        int(path_stat.st_mtime_ns),
        int(path_stat.st_ctime_ns),
        _platform_change_token(path),
        int(path_stat.st_mode),
        int(getattr(path_stat, "st_ino", 0)),
        int(getattr(path_stat, "st_dev", 0)),
    )


def _signature_reusable(signature: _FileSignature) -> bool:
    change_token = signature[3]
    return not _WINDOWS_CHANGE_TOKEN_REQUIRED or (
        isinstance(change_token, int) and change_token > 0
    )


def _reset_process_file_digest_state_after_fork() -> None:
    """Discard inherited cache state and locks in a fork child."""
    global _PROCESS_FILE_DIGESTS
    global _PROCESS_FILE_DIGEST_BYTES
    global _PROCESS_FILE_DIGEST_LOCK
    global _PROCESS_FILE_DIGEST_PID

    _PROCESS_FILE_DIGESTS = OrderedDict()
    _PROCESS_FILE_DIGEST_BYTES = 0
    _PROCESS_FILE_DIGEST_LOCK = threading.RLock()
    _PROCESS_FILE_DIGEST_PID = os.getpid()
    _VALIDATION_SESSION.set(None)


def _ensure_process_file_digest_owner() -> None:
    # The PID guard also covers runtimes where ``register_at_fork`` is unavailable
    # or a test invokes the child boundary directly.
    if _PROCESS_FILE_DIGEST_PID != os.getpid():
        _reset_process_file_digest_state_after_fork()


def _retained_object_bytes(value: Any, *, seen: set[int]) -> int:
    object_id = id(value)
    if object_id in seen:
        return 0
    seen.add(object_id)
    retained = sys.getsizeof(value)
    if type(value) is tuple:
        return retained + sum(
            _retained_object_bytes(item, seen=seen) for item in value
        )
    if type(value) is _FileDigestMemo:
        return (
            retained
            + sys.getsizeof(value.__dict__)
            + _retained_object_bytes(value.signature, seen=seen)
            + _retained_object_bytes(value.size_bytes, seen=seen)
            + _retained_object_bytes(value.digest, seen=seen)
            + _retained_object_bytes(value.content_bound_paths, seen=seen)
            + _retained_object_bytes(value.metadata_bound_paths, seen=seen)
        )
    return retained


def _process_file_digest_retained_bytes(
    key: _ProcessFileDigestKey,
    memo: _FileDigestMemo,
) -> int:
    """Conservatively account for every key/memo object retained by one LRU entry."""
    seen: set[int] = set()
    return (
        _PROCESS_FILE_DIGEST_ENTRY_OVERHEAD_BYTES
        + _retained_object_bytes(key, seen=seen)
        + _retained_object_bytes(memo, seen=seen)
    )


def _process_file_digest_get(
    key: _ProcessFileDigestKey,
) -> _FileDigestMemo | None:
    _ensure_process_file_digest_owner()
    with _PROCESS_FILE_DIGEST_LOCK:
        cached = _PROCESS_FILE_DIGESTS.get(key)
        if cached is None:
            return None
        _PROCESS_FILE_DIGESTS.move_to_end(key)
        return cached[0]


def _process_file_digest_put(
    key: _ProcessFileDigestKey,
    memo: _FileDigestMemo,
) -> None:
    global _PROCESS_FILE_DIGESTS
    global _PROCESS_FILE_DIGEST_BYTES

    retained_bytes = _process_file_digest_retained_bytes(key, memo)
    if retained_bytes > _MAX_PROCESS_FILE_DIGEST_BYTES:
        return
    _ensure_process_file_digest_owner()
    with _PROCESS_FILE_DIGEST_LOCK:
        previous = _PROCESS_FILE_DIGESTS.pop(key, None)
        if previous is not None:
            _PROCESS_FILE_DIGEST_BYTES -= previous[1]
        _PROCESS_FILE_DIGESTS[key] = (memo, retained_bytes)
        _PROCESS_FILE_DIGEST_BYTES += retained_bytes
        evicted = False
        while (
            _PROCESS_FILE_DIGESTS
            and _PROCESS_FILE_DIGEST_BYTES > _MAX_PROCESS_FILE_DIGEST_BYTES
        ):
            _, (_, evicted_bytes) = _PROCESS_FILE_DIGESTS.popitem(last=False)
            _PROCESS_FILE_DIGEST_BYTES -= evicted_bytes
            evicted = True
        table_budget_per_entry = max(
            _PROCESS_FILE_DIGEST_ENTRY_OVERHEAD_BYTES
            - _PROCESS_FILE_DIGEST_VALUE_OVERHEAD_BYTES,
            0,
        )
        compact_table_limit = sys.getsizeof(OrderedDict()) + (
            table_budget_per_entry * max(len(_PROCESS_FILE_DIGESTS), 1)
        )
        if evicted and sys.getsizeof(_PROCESS_FILE_DIGESTS) > compact_table_limit:
            # Compact only stale tables after material shrink. Rebuilding on every
            # steady-state eviction would turn the LRU into an O(n²) hot path.
            _PROCESS_FILE_DIGESTS = OrderedDict(_PROCESS_FILE_DIGESTS)


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_process_file_digest_state_after_fork)


def _bounded_path_totals(
    path_text: str,
    *,
    seen: set[str],
    total_bytes: int,
    total_components: int,
    source: Path,
) -> tuple[int, int]:
    if path_text in seen:
        return total_bytes, total_components
    try:
        path_bytes = path_text.encode("utf-8")
        component_count = len(Path(path_text).parts)
    except (OSError, RuntimeError, UnicodeEncodeError, ValueError) as exc:
        raise _UncacheableFingerprintScope(
            f"commitment path shape is invalid: {source}"
        ) from exc
    if len(path_bytes) > _MAX_BOUND_PATH_UTF8_BYTES:
        raise _UncacheableFingerprintScope(
            f"commitment path exceeds byte limit: {source}"
        )
    if component_count > _MAX_BOUND_PATH_COMPONENTS:
        raise _UncacheableFingerprintScope(
            f"commitment path exceeds component limit: {source}"
        )
    next_total = total_bytes + len(path_bytes)
    next_components = total_components + component_count
    if (
        next_total > _MAX_BOUND_PATH_TOTAL_UTF8_BYTES
        or next_components > _MAX_BOUND_PATH_TOTAL_COMPONENTS
    ):
        raise _UncacheableFingerprintScope(
            f"commitment paths exceed aggregate shape limit: {source}"
        )
    seen.add(path_text)
    return next_total, next_components


def _validate_discovered_bound_path_topology(
    *,
    source: Path,
    content_paths: tuple[str, ...],
    metadata_paths: tuple[str, ...],
) -> None:
    """Validate live topology before bound-path strings can enter the process LRU."""
    content_path_set = set(content_paths)
    for path_text in sorted(content_path_set | set(metadata_paths)):
        path = Path(path_text)
        if _is_link_or_junction(path):
            raise _UncacheableFingerprintScope(
                f"commitment path has linked or invalid topology: {source}"
            )
        resolved = _resolved(path)
        try:
            path_stat = resolved.stat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise _UncacheableFingerprintScope(
                f"commitment path cannot be inspected: {source}"
            ) from exc
        if path_text in content_path_set:
            if not stat.S_ISREG(path_stat.st_mode):
                raise _UncacheableFingerprintScope(
                    f"content commitment must bind a regular file: {source}"
                )
        elif not (stat.S_ISREG(path_stat.st_mode) or stat.S_ISDIR(path_stat.st_mode)):
            raise _UncacheableFingerprintScope(
                f"metadata commitment has unsupported topology: {source}"
            )


def _json_bound_paths(
    path: Path,
    payload: bytes | bytearray,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if path.suffix.lower() != ".json":
        return (), ()
    try:
        document = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return (), ()
    except RecursionError as exc:
        raise _UncacheableFingerprintScope(
            f"commitment JSON nesting is too deep: {path}"
        ) from exc
    content_paths: set[str] = set()
    metadata_paths: set[str] = set()
    bounded_path_values: set[str] = set()
    bounded_path_total_bytes = 0
    bounded_path_total_components = 0
    stack = [document]
    visited_nodes = 0
    while stack:
        visited_nodes += 1
        if visited_nodes > _MAX_COMMITMENT_JSON_NODES:
            raise _UncacheableFingerprintScope(
                f"commitment JSON exceeds node limit: {path}"
            )
        value = stack.pop()
        if isinstance(value, Mapping):
            field_names = {
                field.casefold()
                for field in value
                if isinstance(field, str)
            }
            has_content_commitment = any(
                field in {"sha256", "checksum", "checksum_sha256", "file_contents", "exists"}
                or field.endswith(("_sha256", "_checksum", "_file_contents", "_exists"))
                or field.startswith("checksum_")
                for field in field_names
            )
            has_metadata_commitment = any(
                field == "download_timestamp" or field.endswith("_download_timestamp")
                for field in field_names
            )
            if has_content_commitment or has_metadata_commitment:
                for field, field_value in value.items():
                    if not isinstance(field, str) or (
                        field.casefold() != "path"
                        and not field.casefold().endswith("_path")
                    ):
                        continue
                    if not isinstance(field_value, str) or not field_value:
                        continue
                    try:
                        bound_path = Path(field_value)
                    except (OSError, RuntimeError, ValueError) as exc:
                        raise _UncacheableFingerprintScope(
                            f"commitment path is invalid: {path}"
                        ) from exc
                    if not bound_path.is_absolute():
                        raise _UncacheableFingerprintScope(
                            f"commitment path must be absolute: {path}"
                        )
                    (
                        bounded_path_total_bytes,
                        bounded_path_total_components,
                    ) = _bounded_path_totals(
                        field_value,
                        seen=bounded_path_values,
                        total_bytes=bounded_path_total_bytes,
                        total_components=bounded_path_total_components,
                        source=path,
                    )
                    if has_content_commitment:
                        content_paths.add(field_value)
                    if has_metadata_commitment:
                        content_paths.add(field_value)
                        metadata_paths.add(field_value)
                    if len(content_paths) + len(metadata_paths) > _MAX_AUTOMATIC_BOUND_PATHS:
                        raise _UncacheableFingerprintScope(
                            f"commitment JSON exceeds bound-path limit: {path}"
                        )
            if len(stack) + len(value) > _MAX_COMMITMENT_JSON_NODES:
                raise _UncacheableFingerprintScope(
                    f"commitment JSON exceeds node limit: {path}"
                )
            stack.extend(value.values())
        elif isinstance(value, list):
            if len(stack) + len(value) > _MAX_COMMITMENT_JSON_NODES:
                raise _UncacheableFingerprintScope(
                    f"commitment JSON exceeds node limit: {path}"
                )
            stack.extend(value)
    return tuple(sorted(content_paths)), tuple(sorted(metadata_paths))


def _compatibility_json_bound_paths(
    path: Path,
    payload: bytes | bytearray,
) -> tuple[str, ...]:
    """Preserve the legacy exact ``path`` + ``sha256`` binding contract."""
    if path.suffix.lower() != ".json":
        return ()
    try:
        document = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return ()
    except RecursionError as exc:
        raise _UncacheableFingerprintScope(
            f"compatibility JSON nesting is too deep: {path}"
        ) from exc
    content_paths: set[str] = set()
    bounded_path_values: set[str] = set()
    bounded_path_total_bytes = 0
    bounded_path_total_components = 0
    stack = [document]
    visited_nodes = 0
    while stack:
        visited_nodes += 1
        if visited_nodes > _MAX_COMMITMENT_JSON_NODES:
            raise _UncacheableFingerprintScope(
                f"compatibility JSON exceeds node limit: {path}"
            )
        value = stack.pop()
        if isinstance(value, Mapping):
            bound_path_value = value.get("path")
            if "sha256" in value and isinstance(bound_path_value, str) and bound_path_value:
                try:
                    bound_path = Path(bound_path_value)
                except (OSError, RuntimeError, ValueError) as exc:
                    raise _UncacheableFingerprintScope(
                        f"compatibility binding path is invalid: {path}"
                    ) from exc
                if not bound_path.is_absolute():
                    raise _UncacheableFingerprintScope(
                        f"compatibility binding path must be absolute: {path}"
                    )
                (
                    bounded_path_total_bytes,
                    bounded_path_total_components,
                ) = _bounded_path_totals(
                    bound_path_value,
                    seen=bounded_path_values,
                    total_bytes=bounded_path_total_bytes,
                    total_components=bounded_path_total_components,
                    source=path,
                )
                content_paths.add(bound_path_value)
                if len(content_paths) > _MAX_AUTOMATIC_BOUND_PATHS:
                    raise _UncacheableFingerprintScope(
                        f"compatibility JSON exceeds bound-path limit: {path}"
                    )
            if len(stack) + len(value) > _MAX_COMMITMENT_JSON_NODES:
                raise _UncacheableFingerprintScope(
                    f"compatibility JSON exceeds node limit: {path}"
                )
            stack.extend(value.values())
        elif isinstance(value, list):
            if len(stack) + len(value) > _MAX_COMMITMENT_JSON_NODES:
                raise _UncacheableFingerprintScope(
                    f"compatibility JSON exceeds node limit: {path}"
                )
            stack.extend(value)
    return tuple(sorted(content_paths))


def _file_digest(
    path: Path,
    *,
    discovery_mode: _BoundPathDiscoveryMode,
) -> _FileDigestMemo:
    resolved = _resolved(path)
    before = resolved.stat()
    file_size_limit = (
        _MAX_RAW_FINGERPRINT_FILE_SIZE_BYTES
        if discovery_mode == "none"
        else _MAX_FINGERPRINT_FILE_SIZE_BYTES
    )
    if before.st_size > file_size_limit:
        raise _UncacheableFingerprintScope(f"fingerprint file is too large: {resolved}")
    signature = _file_signature(resolved, before)
    process_key = (str(resolved), discovery_mode, signature)
    if _signature_reusable(signature):
        cached = _process_file_digest_get(process_key)
        if cached is not None:
            return cached

    digest = sha256()
    payload = (
        bytearray()
        if discovery_mode != "none" and resolved.suffix.lower() == ".json"
        else None
    )
    size_bytes = 0
    with resolved.open("rb") as handle:
        while chunk := handle.read(_FINGERPRINT_READ_CHUNK_BYTES):
            size_bytes += len(chunk)
            if size_bytes > file_size_limit:
                raise _UncacheableFingerprintScope(
                    f"fingerprint file is too large: {resolved}"
                )
            digest.update(chunk)
            if payload is not None:
                if size_bytes > _MAX_COMMITMENT_JSON_SIZE_BYTES:
                    raise _UncacheableFingerprintScope(
                        f"commitment JSON is too large: {resolved}"
                    )
                payload.extend(chunk)
    after = resolved.stat()
    final_signature = _file_signature(resolved, after)
    if signature != final_signature:
        raise _UncacheableFingerprintScope(
            f"fingerprint file changed while it was being read: {resolved}"
        )
    if payload is None or discovery_mode == "none":
        content_bound_paths, metadata_bound_paths = (), ()
    elif discovery_mode == "compatibility":
        content_bound_paths = _compatibility_json_bound_paths(resolved, payload)
        metadata_bound_paths = ()
    else:
        content_bound_paths, metadata_bound_paths = _json_bound_paths(
            resolved,
            payload,
        )
    if discovery_mode != "none":
        _validate_discovered_bound_path_topology(
            source=resolved,
            content_paths=content_bound_paths,
            metadata_paths=metadata_bound_paths,
        )
    memo = _FileDigestMemo(
        signature=signature,
        size_bytes=size_bytes,
        digest=digest.digest(),
        content_bound_paths=content_bound_paths,
        metadata_bound_paths=metadata_bound_paths,
    )
    if _signature_reusable(signature) and _signature_reusable(final_signature):
        _process_file_digest_put(process_key, memo)
    return memo


def artifact_content_identity(path: Path) -> str | None:
    """Return a stable streaming content hash, or ``None`` when hashing is unsafe."""
    if _is_link_or_junction(path):
        return None
    try:
        return _file_digest(path, discovery_mode="none").digest.hex()
    except (
        _UncacheableFingerprintScope,
        FileNotFoundError,
        OSError,
        ValueError,
        RuntimeError,
    ):
        return None


def _observe_explicit_directory(
    root: Path,
    *,
    observed: dict[Path, _ObservedPath],
    pending_files: set[Path],
) -> None:
    if _is_link_or_junction(root):
        raise _UncacheableFingerprintScope(f"linked directory is not cacheable: {root}")
    observed[root] = _ObservedPath("DIRECTORY")
    try:
        entries = []
        for index, entry in enumerate(root.rglob("*")):
            if index >= _MAX_RECURSIVE_DIRECTORY_ENTRIES:
                raise _UncacheableFingerprintScope(
                    f"recursive artifact directory exceeds entry limit: {root}"
                )
            entries.append(entry)
        entries.sort(key=lambda path: str(path))
    except OSError as exc:
        raise _UncacheableFingerprintScope(
            f"artifact directory cannot be enumerated: {root}"
        ) from exc
    for entry in entries:
        if _is_link_or_junction(entry):
            raise _UncacheableFingerprintScope(f"linked artifact entry is not cacheable: {entry}")
        resolved = _resolved(entry)
        try:
            entry_stat = resolved.stat()
        except FileNotFoundError:
            observed[resolved] = _ObservedPath("MISSING")
            continue
        except OSError as exc:
            raise _UncacheableFingerprintScope(
                f"artifact entry cannot be inspected: {resolved}"
            ) from exc
        if stat.S_ISDIR(entry_stat.st_mode):
            observed[resolved] = _ObservedPath("DIRECTORY")
        elif stat.S_ISREG(entry_stat.st_mode):
            pending_files.add(resolved)
        else:
            raise _UncacheableFingerprintScope(
                f"unsupported artifact entry type is not cacheable: {resolved}"
            )


def _observe_path(
    path: Path,
    *,
    recursive_directory: bool,
    observed: dict[Path, _ObservedPath],
    pending_files: set[Path],
) -> None:
    if _is_link_or_junction(path):
        raise _UncacheableFingerprintScope(f"linked dependency is not cacheable: {path}")
    resolved = _resolved(path)
    try:
        path_stat = resolved.stat()
    except FileNotFoundError:
        observed[resolved] = _ObservedPath("MISSING")
        return
    except OSError as exc:
        raise _UncacheableFingerprintScope(
            f"dependency cannot be inspected: {resolved}"
        ) from exc
    if stat.S_ISREG(path_stat.st_mode):
        pending_files.add(resolved)
    elif stat.S_ISDIR(path_stat.st_mode):
        if recursive_directory:
            _observe_explicit_directory(
                resolved,
                observed=observed,
                pending_files=pending_files,
            )
        else:
            observed[resolved] = _ObservedPath("DIRECTORY")
    else:
        raise _UncacheableFingerprintScope(
            f"unsupported dependency type is not cacheable: {resolved}"
        )


def _inventory_entries(inventory: ArtifactFingerprintInventory) -> list[Path]:
    if not inventory.patterns or any(not pattern for pattern in inventory.patterns):
        raise ValueError("artifact fingerprint inventory patterns must be non-empty")
    if len(inventory.patterns) > _MAX_INVENTORY_PATTERNS:
        raise ValueError("artifact fingerprint inventory exceeds pattern-count limit")
    for pattern in inventory.patterns:
        pattern_path = Path(pattern)
        if (
            "**" in pattern
            or pattern_path.is_absolute()
            or bool(pattern_path.drive)
            or ".." in pattern_path.parts
        ):
            raise ValueError(f"unsafe artifact fingerprint inventory pattern: {pattern}")
    if _is_link_or_junction(inventory.root):
        raise _UncacheableFingerprintScope(
            f"linked inventory root is not cacheable: {inventory.root}"
        )
    root = _resolved(inventory.root)
    try:
        root_stat = root.stat()
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise _UncacheableFingerprintScope(
            f"artifact inventory root cannot be inspected: {root}"
        ) from exc
    if not stat.S_ISDIR(root_stat.st_mode):
        return []
    entries: set[Path] = set()
    try:
        for pattern in sorted(set(inventory.patterns)):
            for path in root.glob(pattern):
                if _is_link_or_junction(path):
                    raise _UncacheableFingerprintScope(
                        f"linked inventory entry is not cacheable: {path}"
                    )
                entries.add(_resolved(path))
                if len(entries) > _MAX_INVENTORY_ENTRIES:
                    raise _UncacheableFingerprintScope(
                        f"artifact inventory exceeds entry limit: {root}"
                    )
    except OSError as exc:
        raise _UncacheableFingerprintScope(
            f"artifact inventory cannot be enumerated: {root}"
        ) from exc
    return sorted(entries, key=str)


def _normalized_inventories(
    inventories: tuple[ArtifactFingerprintInventory, ...],
) -> list[ArtifactFingerprintInventory]:
    if len(inventories) > _MAX_FINGERPRINT_INVENTORIES:
        raise ValueError("artifact fingerprint scope exceeds inventory-count limit")
    normalized: dict[tuple[str, str, tuple[str, ...]], ArtifactFingerprintInventory] = {}
    for inventory in inventories:
        patterns = tuple(sorted(set(inventory.patterns)))
        if not patterns or len(patterns) > _MAX_INVENTORY_PATTERNS:
            raise ValueError("artifact fingerprint inventory has an invalid pattern count")
        for pattern in patterns:
            pattern_path = Path(pattern)
            if (
                not pattern
                or "**" in pattern
                or pattern_path.is_absolute()
                or bool(pattern_path.drive)
                or ".." in pattern_path.parts
            ):
                raise ValueError(f"unsafe artifact fingerprint inventory pattern: {pattern}")
        key = (
            str(inventory.root),
            str(_resolved(inventory.root)),
            patterns,
        )
        normalized.setdefault(
            key,
            ArtifactFingerprintInventory(root=inventory.root, patterns=patterns),
        )
    return [normalized[key] for key in sorted(normalized)]


def _scope_descriptor(scope: ArtifactFingerprintScope) -> dict[str, Any]:
    if type(scope.discover_bound_paths) is not bool:
        raise TypeError("artifact fingerprint bound-path discovery flag must be boolean")
    return {
        "paths": sorted(
            {
                (str(path), str(_resolved(path)))
                for path in scope.paths
            }
        ),
        "metadata_paths": sorted(
            {
                (str(path), str(_resolved(path)))
                for path in scope.metadata_paths
            }
        ),
        "inventories": [
            {
                "lexical_root": str(inventory.root),
                "root": str(_resolved(inventory.root)),
                "patterns": list(inventory.patterns),
            }
            for inventory in _normalized_inventories(scope.inventories)
        ],
        "discover_bound_paths": scope.discover_bound_paths,
    }


def _metadata_path_marker(path: Path) -> bytes:
    if _is_link_or_junction(path):
        raise _UncacheableFingerprintScope(
            f"linked metadata dependency is not cacheable: {path}"
        )
    resolved = _resolved(path)
    try:
        path_stat = resolved.stat()
    except FileNotFoundError:
        payload: dict[str, Any] = {
            "metadata_path": str(resolved),
            "kind": "MISSING",
        }
    except OSError as exc:
        raise _UncacheableFingerprintScope(
            f"metadata dependency cannot be inspected: {resolved}"
        ) from exc
    else:
        if stat.S_ISREG(path_stat.st_mode):
            kind = "FILE"
        elif stat.S_ISDIR(path_stat.st_mode):
            kind = "DIRECTORY"
        else:
            raise _UncacheableFingerprintScope(
                f"metadata dependency has unsupported topology: {resolved}"
            )
        payload = {
            "metadata_path": str(resolved),
            "kind": kind,
            "size_bytes": int(path_stat.st_size),
            "mtime_ns": int(path_stat.st_mtime_ns),
        }
    return _canonical_json_bytes(payload)


def _enforce_fingerprint_path_budget(
    *,
    observed: Mapping[Path, _ObservedPath],
    pending_files: set[Path],
    pending_metadata_paths: set[Path],
) -> None:
    if (
        len(observed) + len(pending_files) + len(pending_metadata_paths)
        > _MAX_FINGERPRINT_OBSERVED_PATHS
    ):
        raise _UncacheableFingerprintScope("fingerprint scope exceeds path-count limit")


def _compatibility_artifact_fingerprint(root: Path) -> str:
    """Fingerprint the pre-migration direct-file and exact-checksum dependency surface.

    This lane exists only for legacy callers that have not declared an explicit scope.
    It deliberately does not recurse into unbound artifact subdirectories; doing so caused
    severe read amplification in existing validation DAGs.  New and migrated callers use
    :func:`artifact_fingerprint` instead.
    """
    if _is_link_or_junction(root):
        raise _UncacheableFingerprintScope(
            f"linked compatibility artifact root is not cacheable: {root}"
        )
    resolved_root = _resolved(root)
    observed: dict[Path, _ObservedPath] = {}
    pending_files: set[Path] = set()
    fingerprinted_bytes = 0

    try:
        root_stat = resolved_root.stat()
    except FileNotFoundError:
        observed[resolved_root] = _ObservedPath("MISSING")
    except OSError as exc:
        raise _UncacheableFingerprintScope(
            f"compatibility artifact root cannot be inspected: {resolved_root}"
        ) from exc
    else:
        if stat.S_ISDIR(root_stat.st_mode):
            observed[resolved_root] = _ObservedPath("DIRECTORY")
            try:
                entries = []
                for index, entry in enumerate(resolved_root.iterdir()):
                    if index >= _MAX_RECURSIVE_DIRECTORY_ENTRIES:
                        raise _UncacheableFingerprintScope(
                            "compatibility artifact root exceeds direct-entry limit: "
                            f"{resolved_root}"
                        )
                    entries.append(entry)
                entries.sort(key=str)
            except OSError as exc:
                raise _UncacheableFingerprintScope(
                    f"compatibility artifact root cannot be enumerated: {resolved_root}"
                ) from exc
            for entry in entries:
                if _is_link_or_junction(entry):
                    raise _UncacheableFingerprintScope(
                        f"linked compatibility artifact entry is not cacheable: {entry}"
                    )
                resolved_entry = _resolved(entry)
                try:
                    entry_stat = resolved_entry.stat()
                except FileNotFoundError:
                    observed[resolved_entry] = _ObservedPath("MISSING")
                    continue
                except OSError as exc:
                    raise _UncacheableFingerprintScope(
                        f"compatibility artifact entry cannot be inspected: {resolved_entry}"
                    ) from exc
                if stat.S_ISREG(entry_stat.st_mode):
                    pending_files.add(resolved_entry)
                elif stat.S_ISDIR(entry_stat.st_mode):
                    observed[resolved_entry] = _ObservedPath("DIRECTORY")
                else:
                    raise _UncacheableFingerprintScope(
                        "unsupported compatibility artifact entry type is not cacheable: "
                        f"{resolved_entry}"
                    )
        elif stat.S_ISREG(root_stat.st_mode):
            pending_files.add(resolved_root)
        else:
            raise _UncacheableFingerprintScope(
                f"unsupported compatibility artifact root type is not cacheable: {resolved_root}"
            )

    _enforce_fingerprint_path_budget(
        observed=observed,
        pending_files=pending_files,
        pending_metadata_paths=set(),
    )
    while pending_files:
        path = pending_files.pop()
        if path in observed and observed[path].kind == "FILE":
            continue
        try:
            memo = _file_digest(path, discovery_mode="compatibility")
        except FileNotFoundError:
            observed[path] = _ObservedPath("MISSING")
            continue
        except OSError as exc:
            raise _UncacheableFingerprintScope(
                f"compatibility fingerprint file cannot be read: {path}"
            ) from exc
        fingerprinted_bytes += memo.size_bytes
        if fingerprinted_bytes > _MAX_FINGERPRINT_TOTAL_BYTES:
            raise _UncacheableFingerprintScope(
                "compatibility fingerprint exceeds total byte limit"
            )
        observed[path] = _ObservedPath(
            "FILE",
            size_bytes=memo.size_bytes,
            digest=memo.digest,
        )
        for bound_path in memo.content_bound_paths:
            bound = Path(bound_path)
            _observe_path(
                bound,
                recursive_directory=False,
                observed=observed,
                pending_files=pending_files,
            )
            if observed.get(_resolved(bound)) == _ObservedPath("DIRECTORY"):
                raise _UncacheableFingerprintScope(
                    f"compatibility binding points to a directory: {bound}"
                )
        _enforce_fingerprint_path_budget(
            observed=observed,
            pending_files=pending_files,
            pending_metadata_paths=set(),
        )

    digest = sha256()
    digest.update(b"artifact-fingerprint.compatibility.v1\0")
    for path in sorted(observed, key=str):
        observation = observed[path]
        path_bytes = str(path).encode("utf-8")
        digest.update(len(path_bytes).to_bytes(8, "big"))
        digest.update(path_bytes)
        digest.update(observation.kind.encode("ascii"))
        if observation.kind == "FILE":
            digest.update(observation.size_bytes.to_bytes(8, "big"))
            digest.update(observation.digest)
    return digest.hexdigest()


def artifact_fingerprint(
    root: Path,
    *,
    scope: ArtifactFingerprintScope | None = None,
) -> str:
    """Hash an artifact, explicit dependencies, and transitive checksum bindings."""
    if _is_link_or_junction(root):
        raise _UncacheableFingerprintScope(f"linked artifact root is not cacheable: {root}")
    resolved_root = _resolved(root)
    fingerprint_scope = scope or ArtifactFingerprintScope()
    observed: dict[Path, _ObservedPath] = {}
    pending_files: set[Path] = set()
    pending_metadata_paths: set[Path] = set()
    for path in fingerprint_scope.metadata_paths:
        if _is_link_or_junction(path):
            raise _UncacheableFingerprintScope(
                f"linked metadata dependency is not cacheable: {path}"
            )
        pending_metadata_paths.add(_resolved(path))
    markers: list[bytes] = []
    fingerprinted_bytes = 0

    try:
        root_stat = resolved_root.stat()
    except FileNotFoundError:
        observed[resolved_root] = _ObservedPath("MISSING")
    except OSError as exc:
        raise _UncacheableFingerprintScope(
            f"artifact root cannot be inspected: {resolved_root}"
        ) from exc
    else:
        if stat.S_ISDIR(root_stat.st_mode):
            _observe_explicit_directory(
                resolved_root,
                observed=observed,
                pending_files=pending_files,
            )
        elif stat.S_ISREG(root_stat.st_mode):
            pending_files.add(resolved_root)
        else:
            raise _UncacheableFingerprintScope(
                f"unsupported artifact root type is not cacheable: {resolved_root}"
            )
    _enforce_fingerprint_path_budget(
        observed=observed,
        pending_files=pending_files,
        pending_metadata_paths=pending_metadata_paths,
    )

    explicit_paths: dict[tuple[str, str], Path] = {}
    for path in fingerprint_scope.paths:
        explicit_paths.setdefault((str(path), str(_resolved(path))), path)
    for path_key in sorted(explicit_paths):
        _observe_path(
            explicit_paths[path_key],
            recursive_directory=True,
            observed=observed,
            pending_files=pending_files,
        )
        _enforce_fingerprint_path_budget(
            observed=observed,
            pending_files=pending_files,
            pending_metadata_paths=pending_metadata_paths,
        )

    for inventory in _normalized_inventories(fingerprint_scope.inventories):
        inventory_root = _resolved(inventory.root)
        markers.append(
            _canonical_json_bytes(
                {
                    "inventory_root": str(inventory_root),
                    "patterns": list(inventory.patterns),
                }
            )
        )
        _observe_path(
            inventory_root,
            recursive_directory=False,
            observed=observed,
            pending_files=pending_files,
        )
        for entry in _inventory_entries(inventory):
            try:
                relative = entry.relative_to(inventory_root).as_posix()
            except ValueError:
                relative = str(entry)
            try:
                entry_stat = entry.stat()
            except FileNotFoundError:
                kind = "MISSING"
            except OSError as exc:
                raise _UncacheableFingerprintScope(
                    f"inventory entry cannot be inspected: {entry}"
                ) from exc
            else:
                if stat.S_ISREG(entry_stat.st_mode):
                    kind = "FILE"
                elif stat.S_ISDIR(entry_stat.st_mode):
                    kind = "DIRECTORY"
                else:
                    raise _UncacheableFingerprintScope(
                        f"unsupported inventory entry type is not cacheable: {entry}"
                    )
            markers.append(
                _canonical_json_bytes(
                    {
                        "inventory_root": str(inventory_root),
                        "relative_path": relative,
                        "kind": kind,
                    }
                )
            )
            _observe_path(
                entry,
                recursive_directory=False,
                observed=observed,
                pending_files=pending_files,
            )
            _enforce_fingerprint_path_budget(
                observed=observed,
                pending_files=pending_files,
                pending_metadata_paths=pending_metadata_paths,
            )

    while pending_files:
        path = pending_files.pop()
        if path in observed and observed[path].kind == "FILE":
            continue
        try:
            memo = _file_digest(
                path,
                discovery_mode=(
                    "hardened" if fingerprint_scope.discover_bound_paths else "none"
                ),
            )
        except FileNotFoundError:
            observed[path] = _ObservedPath("MISSING")
            continue
        except OSError as exc:
            raise _UncacheableFingerprintScope(
                f"fingerprint file cannot be read: {path}"
            ) from exc
        fingerprinted_bytes += memo.size_bytes
        if fingerprinted_bytes > _MAX_FINGERPRINT_TOTAL_BYTES:
            raise _UncacheableFingerprintScope("fingerprint scope exceeds total byte limit")
        observed[path] = _ObservedPath(
            "FILE",
            size_bytes=memo.size_bytes,
            digest=memo.digest,
        )
        for bound_path in memo.content_bound_paths:
            bound = Path(bound_path)
            _observe_path(
                bound,
                recursive_directory=False,
                observed=observed,
                pending_files=pending_files,
            )
            if observed.get(_resolved(bound)) == _ObservedPath("DIRECTORY"):
                raise _UncacheableFingerprintScope(
                    f"automatic content binding points to a directory: {bound}"
                )
        pending_metadata_paths.update(Path(path) for path in memo.metadata_bound_paths)
        _enforce_fingerprint_path_budget(
            observed=observed,
            pending_files=pending_files,
            pending_metadata_paths=pending_metadata_paths,
        )

    markers.extend(_metadata_path_marker(path) for path in pending_metadata_paths)

    digest = sha256()
    digest.update(b"artifact-fingerprint.v2\0")
    for marker in sorted(markers):
        digest.update(b"SCOPE\0")
        digest.update(len(marker).to_bytes(8, "big"))
        digest.update(marker)
    for path in sorted(observed, key=str):
        observation = observed[path]
        path_bytes = str(path).encode("utf-8")
        digest.update(len(path_bytes).to_bytes(8, "big"))
        digest.update(path_bytes)
        digest.update(observation.kind.encode("ascii"))
        if observation.kind == "FILE":
            digest.update(observation.size_bytes.to_bytes(8, "big"))
            digest.update(observation.digest)
    return digest.hexdigest()


@contextmanager
def artifact_validation_session() -> Iterator[None]:
    """Reuse stable PASS reports in one active synchronous execution context."""
    current = _VALIDATION_SESSION.get()
    owner = _execution_owner()
    if current is not None and current.active and current.owner == owner:
        yield
        return
    state = _ValidationSessionState(owner=owner)
    token = _VALIDATION_SESSION.set(state)
    try:
        yield
    finally:
        state.active = False
        _VALIDATION_SESSION.reset(token)


def with_artifact_validation_session(function: Callable[_P, _R]) -> Callable[_P, _R]:
    if (
        inspect.iscoroutinefunction(function)
        or inspect.isgeneratorfunction(function)
        or inspect.isasyncgenfunction(function)
    ):
        raise TypeError("validation-session decorator supports synchronous functions only")

    @wraps(function)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        with artifact_validation_session():
            return function(*args, **kwargs)

    return wrapped


def _canonical_value(
    value: Any,
    *,
    mutable_ids: set[int],
) -> Any:
    if isinstance(value, Enum):
        return {
            "$enum": f"{value.__class__.__module__}.{value.__class__.__qualname__}",
            "value": _canonical_value(value.value, mutable_ids=mutable_ids),
        }
    if value is None or type(value) in {str, bool, int}:
        return value
    if type(value) is float:
        if value != value or value in {float("inf"), float("-inf")}:
            raise ValueError("validation cache keys require finite floats")
        return value
    if type(value) is _CONCRETE_PATH_TYPE:
        return {
            "$path": {
                "lexical": str(value),
                "resolved": str(_resolved(value)),
            }
        }
    if isinstance(value, Path):
        raise TypeError("validation cache Path subclasses are unsupported")
    if type(value) is datetime:
        if value.tzinfo is None:
            timezone_key: dict[str, Any] = {"kind": "naive"}
        elif type(value.tzinfo) is timezone:
            offset = value.utcoffset()
            timezone_key = {
                "kind": "fixed",
                "offset_seconds": None if offset is None else offset.total_seconds(),
                "name": value.tzname(),
            }
        elif type(value.tzinfo) is ZoneInfo:
            timezone_key = {"kind": "zoneinfo", "key": value.tzinfo.key}
        else:
            raise TypeError("validation cache datetime tzinfo is unsupported")
        return {
            "$datetime": value.isoformat(),
            "fold": value.fold,
            "timezone": timezone_key,
        }
    if isinstance(value, datetime):
        raise TypeError("validation cache datetime subclasses are unsupported")
    if type(value) is date:
        return {"$date": value.isoformat()}
    if isinstance(value, date):
        raise TypeError("validation cache date subclasses are unsupported")
    if type(value) is bytes:
        return {"$bytes_sha256": sha256(value).hexdigest(), "$size_bytes": len(value)}
    if type(value) is dict:
        if id(value) in mutable_ids:
            raise TypeError("validation cache shared mutable mappings are unsupported")
        mutable_ids.add(id(value))
        if any(type(key) is not str for key in value):
            raise TypeError("validation cache mapping keys must be strings")
        return {
            "$mapping": [
                [key, _canonical_value(value[key], mutable_ids=mutable_ids)]
                for key in value
            ]
        }
    if isinstance(value, Mapping):
        raise TypeError("validation cache mappings must be plain dictionaries")
    if type(value) is tuple:
        return {
            "$tuple": [
                _canonical_value(item, mutable_ids=mutable_ids)
                for item in value
            ]
        }
    if type(value) is list:
        if id(value) in mutable_ids:
            raise TypeError("validation cache shared mutable lists are unsupported")
        mutable_ids.add(id(value))
        return [
            _canonical_value(item, mutable_ids=mutable_ids)
            for item in value
        ]
    if type(value) in {set, frozenset}:
        raise TypeError("validation cache set iteration semantics are unsupported")
    raise TypeError(f"unsupported validation cache key value: {type(value).__name__}")


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        _canonical_value(value, mutable_ids=set()),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _validation_kwargs(
    *,
    validator_key: str | None,
    artifact_id: str | None,
    root: Path | None,
    validator_kwargs: Mapping[str, Any] | None,
) -> dict[str, Any]:
    call_kwargs = dict(validator_kwargs or {})
    legacy_values = (validator_key, artifact_id, root)
    if any(value is not None for value in legacy_values):
        if not all(value is not None for value in legacy_values):
            raise ValueError("validator_key, artifact_id, and root must be provided together")
        assert validator_key is not None
        assert artifact_id is not None
        assert root is not None
        artifact_id_path = Path(artifact_id)
        if (
            not artifact_id
            or "/" in artifact_id
            or "\\" in artifact_id
            or artifact_id_path.is_absolute()
            or bool(artifact_id_path.drive)
            or len(artifact_id_path.parts) != 1
            or artifact_id_path.parts[0] in {".", ".."}
        ):
            raise ValueError("legacy artifact_id must be one non-empty path segment")
        if _resolved(root / artifact_id).parent != _resolved(root):
            raise ValueError("legacy artifact_id must remain directly under root")
        if validator_key in call_kwargs and call_kwargs[validator_key] != artifact_id:
            raise ValueError(f"validator kwargs conflict for {validator_key}")
        if "output_dir" in call_kwargs and _resolved(Path(call_kwargs["output_dir"])) != _resolved(
            root
        ):
            raise ValueError("validator kwargs conflict for output_dir")
        call_kwargs[validator_key] = artifact_id
        call_kwargs["output_dir"] = root
    return call_kwargs


def cached_artifact_validation(
    *,
    validator: Callable[..., dict[str, Any]],
    validator_key: str | None = None,
    artifact_id: str | None = None,
    root: Path | None = None,
    validator_kwargs: Mapping[str, Any] | None = None,
    artifact_root: Path | None = None,
    semantic_key: Any = None,
    validator_version: str = "1",
    fingerprint_scope: ArtifactFingerprintScope | None = None,
) -> dict[str, Any]:
    """Validate once per stable validator, semantics, path, and content tuple.

    The legacy ``validator_key``/``artifact_id``/``root`` call shape remains supported.
    Arbitrary validator signatures can instead use ``validator_kwargs`` plus an explicit
    ``artifact_root``. Extra live files, directories, or bounded glob inventories belong
    in ``fingerprint_scope``.
    """
    uses_legacy_call_shape = all(
        value is not None for value in (validator_key, artifact_id, root)
    )
    call_kwargs = _validation_kwargs(
        validator_key=validator_key,
        artifact_id=artifact_id,
        root=root,
        validator_kwargs=validator_kwargs,
    )
    resolved_artifact_root = artifact_root
    if resolved_artifact_root is None and root is not None and artifact_id is not None:
        resolved_artifact_root = root / artifact_id
    elif resolved_artifact_root is not None and root is not None and artifact_id is not None:
        if _resolved(resolved_artifact_root) != _resolved(root / artifact_id):
            raise ValueError("artifact_root conflicts with legacy root/artifact_id")
    if resolved_artifact_root is None:
        raise ValueError("artifact_root is required for generic validator kwargs")
    if not isinstance(validator_version, str) or not validator_version:
        raise ValueError("validator_version must be a non-empty string")

    session = _VALIDATION_SESSION.get()
    if session is None or not session.active or session.owner != _execution_owner():
        return validator(**call_kwargs)

    compatibility_mode = uses_legacy_call_shape and fingerprint_scope is None
    fingerprint_mode = (
        "legacy-direct-and-exact-checksum.v1"
        if compatibility_mode
        else "explicit-hardened-dag.v2"
    )
    scope = fingerprint_scope or ArtifactFingerprintScope()
    fingerprint_root = Path(resolved_artifact_root)
    resolved_root = _resolved(resolved_artifact_root)
    validator_module = getattr(validator, "__module__", validator.__class__.__module__)
    validator_qualname = getattr(
        validator,
        "__qualname__",
        validator.__class__.__qualname__,
    )
    validator_name = f"{validator_module}.{validator_qualname}"
    validator_token = id(validator)
    session.validator_refs.setdefault(validator_token, validator)
    try:
        semantic_payload = {
            "schema_version": "artifact-validation-cache-key.v2",
            "validator": validator_name,
            "validator_version": validator_version,
            "validator_kwargs": call_kwargs,
            "semantic_key": semantic_key,
            "artifact_root": str(resolved_root),
            "fingerprint_mode": fingerprint_mode,
            "fingerprint_scope": _scope_descriptor(scope),
        }
        semantic_digest = sha256(_canonical_json_bytes(semantic_payload)).hexdigest()
    except (TypeError, ValueError, OverflowError, OSError, RecursionError, RuntimeError):
        return validator(**call_kwargs)
    def current_fingerprint() -> str:
        if compatibility_mode:
            return _compatibility_artifact_fingerprint(fingerprint_root)
        return artifact_fingerprint(fingerprint_root, scope=scope)

    try:
        before_fingerprint = current_fingerprint()
    except (_UncacheableFingerprintScope, OSError, ValueError, RuntimeError):
        return validator(**call_kwargs)
    key = (validator_token, semantic_digest, before_fingerprint)
    cached = session.validations.get(key)
    if cached is not None:
        try:
            cached_result = deepcopy(cached)
        except Exception:  # noqa: BLE001 - cache copy failure must degrade to revalidation.
            session.validations.pop(key, None)
        else:
            try:
                confirmed_fingerprint = current_fingerprint()
            except (_UncacheableFingerprintScope, OSError, ValueError, RuntimeError):
                return validator(**call_kwargs)
            if confirmed_fingerprint == before_fingerprint:
                return cached_result
            before_fingerprint = confirmed_fingerprint
            key = (validator_token, semantic_digest, before_fingerprint)
    validation = validator(**call_kwargs)
    try:
        after_fingerprint = current_fingerprint()
    except (_UncacheableFingerprintScope, OSError, ValueError, RuntimeError):
        return validation
    if validation.get("status") == "PASS" and after_fingerprint == before_fingerprint:
        try:
            cached_validation = deepcopy(validation)
        except Exception:  # noqa: BLE001 - unsupported report values simply disable caching.
            return validation
        session.validations[key] = cached_validation
    return validation
