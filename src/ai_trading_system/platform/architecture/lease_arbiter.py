"""DEVX-014/S1a: OS ownership for the existing, sole execution-lease arbiter.

The stable arbiter.lock file is never replaced or removed by normal execution.
Local Windows byte-range locks and POSIX flock are supported; no distributed
filesystem or mixed-version writers are claimed. Callers must not fork while
holding the short critical section. Metadata and wall-clock expiry are diagnostic,
never permission to take a live OS lock. Migration requires independently proved
coordinator quiescence; a JSON assertion alone is not authorization.
"""

from __future__ import annotations

import errno
import hashlib
import importlib
import json
import os
import re
import stat
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, NoReturn
from uuid import uuid4

from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text
from ai_trading_system.platform.artifacts.writer import write_json_atomic

ARBITER_PROTOCOL = "execution_lease_os_arbiter.v2"
_ANCHOR_BYTES = (ARBITER_PROTOCOL + "\n").encode("ascii")
_OWNER_SCHEMA = "execution_lease_os_arbiter_owner.v2"
_QUIESCENCE_ASSERTION = "ALL_LEGACY_ARBITER_USERS_STOPPED_AND_NO_IN_FLIGHT_READERS"
_MIGRATION_SCHEMA = "lease_arbiter_migration_receipt.v1"
# Engineering resource limit for small protocol JSON, never an investment rule.
_MAX_EVIDENCE_BYTES = 64 * 1024
_OWNER_KEYS = {
    "schema_version",
    "acquisition_id",
    "actor",
    "pid",
    "thread_id",
    "acquired_at",
    "diagnostic_expires_at",
    "state",
    "production_effect",
}
_QUIESCENCE_KEYS = {
    "schema_version",
    "store_root",
    "actor",
    "owner_instruction_ref",
    "publication_transaction_id",
    "publication_transaction_sha256",
    "observed_owner_sha256",
    "checked_at",
    "assertion",
    "production_effect",
    "broker_action",
}
_SAFETY = {
    "production_effect": "none",
    "broker_action": "none",
    "business_lease_events_mutated": False,
}


def _fail(code: str, message: str) -> NoReturn:
    raise ParallelControlError("LEASE_ARBITER_" + code, message)


def _canonical(value: object) -> bytes:
    return (
        json.dumps(
            value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        )
        + "\n"
    ).encode("utf-8")


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _aware(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        _fail("STATE_INVALID", "timezone-aware timestamp required")
    return value.astimezone(UTC)


def _time(value: object) -> datetime:
    try:
        if not isinstance(value, str):
            raise ValueError("timestamp type")
        return _aware(datetime.fromisoformat(value))
    except (TypeError, ValueError):
        _fail("STATE_INVALID", "invalid timestamp")


def _text(value: object) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or "\x00" in value:
        _fail("STATE_INVALID", "nonempty canonical text required")
    return value


def _digest(value: object) -> str:
    text = _text(value)
    if re.fullmatch(r"[0-9a-f]{64}", text) is None:
        _fail("MIGRATION_BINDING_INVALID", "exact SHA-256 required")
    return text


def _safe(path: Path) -> None:
    for item in (path, *path.parents):
        try:
            info = item.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            _fail("PATH_UNSAFE", "symlink/reparse component is unsupported")


def _root(value: Path) -> Path:
    if not value.is_absolute():
        _fail("PATH_UNSAFE", "absolute store root required")
    _safe(value)
    return value.resolve()


def _read_regular(path: Path) -> bytes:
    _safe(path)
    before = path.stat()
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        _fail("PATH_UNSAFE", "single-link regular evidence file required")
    if before.st_size > _MAX_EVIDENCE_BYTES:
        _fail("STATE_INVALID", "protocol evidence exceeds engineering byte cap")
    with path.open("rb") as stream:
        opened = os.fstat(stream.fileno())
        content = stream.read(_MAX_EVIDENCE_BYTES + 1)
        after = os.fstat(stream.fileno())
    _safe(path)
    final = path.stat()
    identities = {
        (
            item.st_dev,
            item.st_ino,
            item.st_mode,
            item.st_nlink,
            item.st_size,
            item.st_mtime_ns,
            item.st_ctime_ns,
        )
        for item in (before, opened, after, final)
    }
    if len(identities) != 1 or len(content) != before.st_size:
        _fail("STATE_INVALID", "protocol evidence changed during bounded capture")
    return content


def _read_mapping(path: Path) -> dict[str, Any]:
    try:
        return _parse_mapping(_read_regular(path))
    except (OSError, UnicodeError, ValueError) as exc:
        if isinstance(exc, ParallelControlError):
            raise
        _fail("STATE_INVALID", f"invalid owner/evidence: {type(exc).__name__}")


def _parse_mapping(content: bytes) -> dict[str, Any]:
    try:
        value = load_strict_json_text(content.decode("utf-8"))
        if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
            _fail("STATE_INVALID", "JSON object required")
        return dict(value)
    except (UnicodeError, ValueError) as exc:
        if isinstance(exc, ParallelControlError):
            raise
        _fail("STATE_INVALID", f"invalid strict JSON: {type(exc).__name__}")


def _read_owner(path: Path) -> dict[str, Any]:
    owner = _read_mapping(path)
    if (
        set(owner) != _OWNER_KEYS
        or owner["schema_version"] != _OWNER_SCHEMA
        or owner["state"] not in {"ACTIVE", "RELEASED"}
        or owner["production_effect"] != "none"
        or not isinstance(owner["acquisition_id"], str)
        or re.fullmatch(r"[0-9a-f]{32}", owner["acquisition_id"]) is None
    ):
        _fail("STATE_INVALID", "invalid exact owner schema")
    _text(owner["actor"])
    for name in ("pid", "thread_id"):
        if type(owner[name]) is not int or owner[name] <= 0:
            _fail("STATE_INVALID", "invalid owner process/thread identity")
    if _time(owner["diagnostic_expires_at"]) < _time(owner["acquired_at"]):
        _fail("STATE_INVALID", "owner chronology mismatch")
    return owner


def _try_lock(fd: int) -> None:
    os.lseek(fd, 0, os.SEEK_SET)
    if os.name == "nt":
        import msvcrt

        # Fixed byte zero, length one is a protocol invariant, not a timeout.
        # Windows permits locking past EOF, so initialization is always after lock.
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
    elif os.name == "posix":
        fcntl: Any = importlib.import_module("fcntl")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    else:
        _fail("PLATFORM_UNSUPPORTED", "no reviewed local OS lock backend")


def _unlock(fd: int) -> None:
    os.lseek(fd, 0, os.SEEK_SET)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    elif os.name == "posix":
        fcntl: Any = importlib.import_module("fcntl")
        fcntl.flock(fd, fcntl.LOCK_UN)
    else:
        _fail("PLATFORM_UNSUPPORTED", "no reviewed local OS lock backend")


@dataclass
class _HeldLock:
    """Private process/thread-bound handle, never reconstructed from a receipt."""

    path: Path
    fd: int
    pid: int
    thread_id: int
    token: str
    file_identity: tuple[int, int]
    released: bool = False

    def assert_owner(self, *, owner_token: str) -> None:
        if (
            self.released
            or owner_token != self.token
            or os.getpid() != self.pid
            or threading.get_ident() != self.thread_id
        ):
            _fail("OWNER_MISMATCH", "caller does not own this OS handle")

    def assert_anchor(self) -> None:
        _safe(self.path)
        info = self.path.stat()
        opened = os.fstat(self.fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or info.st_nlink != 1
            or opened.st_nlink != 1
            or (info.st_dev, info.st_ino) != self.file_identity
            or (opened.st_dev, opened.st_ino) != self.file_identity
        ):
            _fail("OWNER_MISMATCH", "stable lock-file identity changed")

    def release(self, *, owner_token: str) -> None:
        # A wrong owner must not close/unlock the legitimate owner's handle.
        self.assert_owner(owner_token=owner_token)
        try:
            try:
                _unlock(self.fd)
            finally:
                os.close(self.fd)
        except OSError as exc:
            _fail("RELEASE_FAILED", f"OS release failed: {type(exc).__name__}")
        finally:
            self.released = True


def _acquire_lock(path: Path, *, create_only: bool = False) -> _HeldLock:
    _safe(path)
    if path.is_dir():
        _fail("MIGRATION_REQUIRED", "legacy directory requires explicit quiescent migration")
    if path.exists():
        info = path.stat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            _fail("PATH_UNSAFE", "stable arbiter anchor must be single-link regular file")
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOINHERIT", 0)
    if create_only:
        flags |= os.O_EXCL
    fd: int | None = None
    locked = False
    transferred = False
    try:
        try:
            fd = os.open(path, flags, 0o600)
        except FileExistsError:
            if create_only:
                _fail("MIGRATION_PARTIAL", "create-only migration anchor already exists")
            raise
        os.set_inheritable(fd, False)
        try:
            _try_lock(fd)
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EAGAIN} or getattr(exc, "winerror", None) == 33:
                _fail("BUSY", "OS arbiter is held by another handle")
            raise
        except (ImportError, AttributeError):
            _fail("PLATFORM_UNSUPPORTED", "required OS file-lock backend is unavailable")
        locked = True
        info = os.fstat(fd)
        held = _HeldLock(
            path, fd, os.getpid(), threading.get_ident(), uuid4().hex, (info.st_dev, info.st_ino)
        )
        held.assert_anchor()
        transferred = True
        return held
    except OSError as exc:
        _fail("LOCK_IO_ERROR", f"unable to acquire stable anchor: {type(exc).__name__}")
    finally:
        # From successful open until the capability is returned, this scope is
        # the sole FD owner, including failures constructing the capability.
        if fd is not None and not transferred:
            try:
                if locked:
                    _unlock(fd)
            finally:
                os.close(fd)


def _initialize_anchor(held: _HeldLock, owner_path: Path) -> bool:
    size = os.fstat(held.fd).st_size
    if size == 0:
        if owner_path.exists():
            _fail("STATE_INVALID", "empty anchor contradicts retained owner metadata")
        os.lseek(held.fd, 0, os.SEEK_SET)
        if os.write(held.fd, _ANCHOR_BYTES) != len(_ANCHOR_BYTES):
            _fail("LOCK_IO_ERROR", "short anchor initialization write")
        os.fsync(held.fd)
        return True
    os.lseek(held.fd, 0, os.SEEK_SET)
    if size != len(_ANCHOR_BYTES) or os.read(held.fd, len(_ANCHOR_BYTES)) != _ANCHOR_BYTES:
        _fail("STATE_INVALID", "unknown stable arbiter protocol")
    return False


def _owner(held: _HeldLock, actor: str, now: datetime, ttl: int, state: str) -> dict[str, Any]:
    return {
        "schema_version": _OWNER_SCHEMA,
        "acquisition_id": held.token,
        "actor": actor,
        "pid": held.pid,
        "thread_id": held.thread_id,
        "acquired_at": now.isoformat(),
        "diagnostic_expires_at": (now + timedelta(seconds=ttl)).isoformat(),
        "state": state,
        "production_effect": "none",
    }


def _check_quiescence(
    value: Mapping[str, Any],
    *,
    root: Path,
    actor: str,
    owner_instruction_ref: str,
    owner_sha256: str,
    instant: datetime,
) -> None:
    expected = {
        "schema_version": "lease_arbiter_quiescence.v1",
        "store_root": root.as_posix(),
        "actor": actor,
        "owner_instruction_ref": owner_instruction_ref,
        "observed_owner_sha256": owner_sha256,
        "assertion": _QUIESCENCE_ASSERTION,
        "production_effect": "none",
        "broker_action": "none",
    }
    if (
        set(value) != _QUIESCENCE_KEYS
        or any(value.get(key) != item for key, item in expected.items())
        or _time(value["checked_at"]) > instant
    ):
        _fail("MIGRATION_BINDING_INVALID", "quiescence is not bound to this exact migration")
    _text(value["publication_transaction_id"])
    _digest(value["publication_transaction_sha256"])


def _check_legacy_owner(content: bytes) -> None:
    old = _parse_mapping(content)
    keys = {"schema_version", "state", "actor", "acquired_at", "expires_at", "production_effect"}
    if (
        set(old) not in (keys, keys - {"state"})
        or old.get("schema_version") != "execution_lease_arbiter.v1"
        or old.get("state") not in {None, "ACTIVE", "RELEASED"}
        or old.get("production_effect") != "none"
    ):
        _fail("MIGRATION_BINDING_INVALID", "unknown legacy owner protocol")
    _text(old["actor"])
    if _time(old["expires_at"]) < _time(old["acquired_at"]):
        _fail("MIGRATION_BINDING_INVALID", "legacy chronology mismatch")


def _check_completed_migration(root: Path, run: Path) -> None:
    _safe(run)
    if (
        not run.is_dir()
        or re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,95}", run.name) is None
        or {path.name for path in run.iterdir()}
        != {"request.json", "quiescence.json", "legacy", "receipt.json"}
    ):
        _fail("MIGRATION_PARTIAL", "incomplete migration requires governed review")
    archive = run / "legacy"
    _safe(archive)
    if not archive.is_dir() or {path.name for path in archive.iterdir()} != {"owner.json"}:
        _fail("MIGRATION_PARTIAL", "invalid legacy archive members")
    archived_bytes = _read_regular(archive / "owner.json")
    _check_legacy_owner(archived_bytes)
    old_sha = _sha(archived_bytes)
    receipt = _read_mapping(run / "receipt.json")
    body = dict(receipt)
    digest = body.pop("receipt_sha256", None)
    if (
        set(receipt)
        != {
            "schema_version",
            "status",
            "migration_id",
            "store_root",
            "actor",
            "owner_instruction_ref",
            "quiescence_receipt",
            "legacy_owner",
            "anchor",
            "migrated_at",
            "safety",
            "receipt_sha256",
        }
        or receipt["schema_version"] != _MIGRATION_SCHEMA
        or receipt["status"] != "PASS"
        or receipt["store_root"] != root.as_posix()
        or receipt["migration_id"] != run.name
        or digest != _sha(_canonical(body))
        or receipt["safety"] != _SAFETY
        or receipt["anchor"]
        != {"path": (root / "arbiter.lock").as_posix(), "protocol": ARBITER_PROTOCOL}
        or receipt["legacy_owner"]
        != {"archive_path": (archive / "owner.json").as_posix(), "sha256": old_sha}
    ):
        _fail("MIGRATION_PARTIAL", "migration completion binding is invalid")
    actor = _text(receipt["actor"])
    owner_ref = _text(receipt["owner_instruction_ref"])
    instant = _time(receipt["migrated_at"])
    q_ref = receipt["quiescence_receipt"]
    if not isinstance(q_ref, dict) or set(q_ref) != {"path", "sha256"}:
        _fail("MIGRATION_PARTIAL", "invalid quiescence correlation binding")
    q_path = Path(_text(q_ref["path"]))
    if not q_path.is_absolute() or q_path.suffix != ".json" or q_path.as_posix() != q_ref["path"]:
        _fail("MIGRATION_PARTIAL", "invalid original quiescence locator")
    q_sha = _digest(q_ref["sha256"])
    # The original locator is correlation only. Replay reads exclusively the
    # frozen local capture, never an arbitrary path selected by receipt JSON.
    q_bytes = _read_regular(run / "quiescence.json")
    if _sha(q_bytes) != q_sha:
        _fail("MIGRATION_PARTIAL", "frozen quiescence digest mismatch")
    _check_quiescence(
        _parse_mapping(q_bytes),
        root=root,
        actor=actor,
        owner_instruction_ref=owner_ref,
        owner_sha256=old_sha,
        instant=instant,
    )
    if _read_mapping(run / "request.json") != {
        "schema_version": "lease_arbiter_migration_request.v1",
        "migration_id": run.name,
        "store_root": root.as_posix(),
        "actor": actor,
        "owner_instruction_ref": owner_ref,
        "expected_owner_sha256": old_sha,
        "quiescence_receipt": q_ref,
    }:
        _fail("MIGRATION_PARTIAL", "request does not bind completed migration evidence")


def _completed_migrations(root: Path) -> None:
    folder = root / "arbiter-migrations"
    _safe(folder)
    if not folder.exists():
        return
    try:
        for run in folder.iterdir():
            _check_completed_migration(root, run)
    except (OSError, ValueError, TypeError, KeyError) as exc:
        _fail("MIGRATION_PARTIAL", f"migration bundle cannot replay: {type(exc).__name__}")


@contextmanager
def hold_lease_arbiter(
    store_root: Path, *, actor: str, now: datetime, arbiter_ttl_seconds: int
) -> Iterator[None]:
    root = _root(store_root)
    instant = _aware(now)
    _text(actor)
    if type(arbiter_ttl_seconds) is not int or arbiter_ttl_seconds <= 0:
        _fail("STATE_INVALID", "positive diagnostic TTL required")
    root.mkdir(parents=True, exist_ok=True)
    _completed_migrations(root)
    held = _acquire_lock(root / "arbiter.lock")
    owner_path = root / "arbiter.owner.json"
    owner_written = False
    try:
        _safe(owner_path)
        initialized = _initialize_anchor(held, owner_path)
        if not initialized:
            # A valid ACTIVE sidecar with a free OS lock is diagnostic crash
            # residue. It never expires or releases a business execution lease.
            _read_owner(owner_path)
        active = _owner(held, actor, instant, arbiter_ttl_seconds, "ACTIVE")
        write_json_atomic(owner_path, active)
        owner_written = True
        yield
    finally:
        try:
            if owner_written:
                held.assert_owner(owner_token=held.token)
                held.assert_anchor()
                current = _read_owner(owner_path)
                if current != active:
                    _fail("OWNER_MISMATCH", "owner metadata changed while OS lock remained held")
                write_json_atomic(owner_path, {**active, "state": "RELEASED"})
        finally:
            held.release(owner_token=held.token)


def _exclusive_json(path: Path, value: object) -> None:
    _exclusive_bytes(path, _canonical(value))


def _exclusive_bytes(path: Path, content: bytes) -> None:
    _safe(path)
    if len(content) > _MAX_EVIDENCE_BYTES:
        _fail("STATE_INVALID", "protocol evidence exceeds engineering byte cap")
    with path.open("xb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


def migrate_legacy_arbiter(
    store_root: Path,
    *,
    migration_id: str,
    actor: str,
    now: datetime,
    expected_owner_sha256: str,
    owner_instruction_ref: str,
    quiescence_receipt_path: Path,
    quiescence_receipt_sha256: str,
) -> dict[str, object]:
    """Explicit coordinator-only migration; no legacy arbiter proves quiescence.

    The CLI/coordinator must first validate its publication scope and stop ALL
    legacy in-flight readers. Supplied evidence is correlation, not a signature.
    This helper deliberately does not claim a committed implementation identity.
    """
    root = _root(store_root)
    instant = _aware(now)
    if re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,95}", migration_id) is None:
        _fail("MIGRATION_BINDING_INVALID", "invalid migration id")
    _text(actor)
    _text(owner_instruction_ref)
    _digest(expected_owner_sha256)
    _digest(quiescence_receipt_sha256)
    if not quiescence_receipt_path.is_absolute() or quiescence_receipt_path.suffix != ".json":
        _fail("MIGRATION_BINDING_INVALID", "explicit JSON quiescence locator required")
    quiescence_bytes = _read_regular(quiescence_receipt_path)
    if _sha(quiescence_bytes) != quiescence_receipt_sha256:
        _fail("MIGRATION_BINDING_INVALID", "quiescence digest mismatch")
    quiescence = _parse_mapping(quiescence_bytes)
    _check_quiescence(
        quiescence,
        root=root,
        actor=actor,
        owner_instruction_ref=owner_instruction_ref,
        owner_sha256=expected_owner_sha256,
        instant=instant,
    )
    run = root / "arbiter-migrations" / migration_id
    legacy = root / "arbiter.lock"
    request = {
        "schema_version": "lease_arbiter_migration_request.v1",
        "migration_id": migration_id,
        "store_root": root.as_posix(),
        "actor": actor,
        "owner_instruction_ref": owner_instruction_ref,
        "expected_owner_sha256": expected_owner_sha256,
        "quiescence_receipt": {
            "path": quiescence_receipt_path.as_posix(),
            "sha256": quiescence_receipt_sha256,
        },
    }
    if run.exists():
        _completed_migrations(root)
        if _read_mapping(run / "request.json") != request or not legacy.is_file():
            _fail("MIGRATION_BINDING_INVALID", "same migration id cannot bind different evidence")
        existing_handle = _acquire_lock(legacy)
        try:
            if _initialize_anchor(existing_handle, root / "arbiter.owner.json"):
                _fail("MIGRATION_PARTIAL", "retained migration anchor was lost")
            _read_owner(root / "arbiter.owner.json")
        finally:
            existing_handle.release(owner_token=existing_handle.token)
        return _read_mapping(run / "receipt.json")
    _completed_migrations(root)
    _safe(legacy)
    if not legacy.is_dir():
        _fail("MIGRATION_REQUIRED", "exact legacy directory required for migration")
    if sorted(path.name for path in legacy.iterdir()) != ["owner.json"]:
        _fail("MIGRATION_BINDING_INVALID", "legacy directory contains undeclared members")
    old_bytes = _read_regular(legacy / "owner.json")
    if _sha(old_bytes) != expected_owner_sha256:
        _fail("MIGRATION_BINDING_INVALID", "legacy owner raw bytes mismatch")
    _check_legacy_owner(old_bytes)
    _safe(root / "arbiter.owner.json")
    if (root / "arbiter.owner.json").exists():
        _fail("MIGRATION_BINDING_INVALID", "legacy store already has v2 owner metadata")
    created = False
    held: _HeldLock | None = None
    try:
        _safe(run)
        run.mkdir(parents=True, exist_ok=False)
        created = True
        _exclusive_json(run / "request.json", request)
        _exclusive_bytes(run / "quiescence.json", quiescence_bytes)
        if _read_regular(legacy / "owner.json") != old_bytes:
            _fail("MIGRATION_BINDING_INVALID", "legacy owner changed after quiescence capture")
        archive = run / "legacy"
        # Explicitly validate both absolute move targets inside this store. This
        # administrative move is safe only under the externally proved quiescence.
        if not legacy.resolve().is_relative_to(root) or not archive.resolve().is_relative_to(root):
            _fail("PATH_UNSAFE", "migration move escaped exact store root")
        legacy.rename(archive)
        held = _acquire_lock(legacy, create_only=True)
        if not _initialize_anchor(held, root / "arbiter.owner.json"):
            _fail("MIGRATION_PARTIAL", "migration target was not exclusively initialized")
        # Migration has no running arbiter lifetime: diagnostic end equals start.
        write_json_atomic(root / "arbiter.owner.json", _owner(held, actor, instant, 0, "RELEASED"))
        if _read_regular(archive / "owner.json") != old_bytes:
            _fail("MIGRATION_BINDING_INVALID", "archived legacy owner differs")
        body = {
            "schema_version": _MIGRATION_SCHEMA,
            "status": "PASS",
            "migration_id": migration_id,
            "store_root": root.as_posix(),
            "actor": actor,
            "owner_instruction_ref": owner_instruction_ref,
            "quiescence_receipt": request["quiescence_receipt"],
            "legacy_owner": {
                "archive_path": (archive / "owner.json").as_posix(),
                "sha256": expected_owner_sha256,
            },
            "anchor": {"path": legacy.as_posix(), "protocol": ARBITER_PROTOCOL},
            "migrated_at": instant.isoformat(),
            "safety": dict(_SAFETY),
        }
        receipt: dict[str, object] = {**body, "receipt_sha256": _sha(_canonical(body))}
        _exclusive_json(run / "receipt.json", receipt)
        held.release(owner_token=held.token)
        held = None
        _completed_migrations(root)
        return receipt
    except BaseException as exc:
        try:
            if created and not (run / "failure.json").exists():
                try:
                    _exclusive_json(
                        run / "failure.json",
                        {
                            "schema_version": "lease_arbiter_migration_failure.v1",
                            "status": "FAIL",
                            "migration_id": migration_id,
                            "error_code": getattr(exc, "code", type(exc).__name__),
                            "safety": dict(_SAFETY),
                        },
                    )
                except OSError:
                    pass
        finally:
            if held is not None and not held.released:
                held.release(owner_token=held.token)
        if isinstance(exc, ParallelControlError):
            raise
        if isinstance(exc, Exception):
            _fail("MIGRATION_PARTIAL", f"migration stopped: {type(exc).__name__}")
        raise
