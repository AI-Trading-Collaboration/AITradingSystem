"""Capture hold: single-flight ownership of one prospective-capture run (GOV-008 L3, protocol v3).

A hold replaces the S4D lease in the capture protocol. It binds one run to three facts that the
capture evidence needs and nothing else:

1. the exact candidate commit, with result-affecting code unmodified (``core.provenance``);
2. exclusive write ownership of the declared evidence paths, so two captures cannot write the same
   store at once;
3. a bounded validity window that the capture must stay inside.

A hold is a local self-attestation by the parent process, not an independent authority: there is no
second store that could contradict it, and retained proofs carry the record bytes they were checked
against, not a signature. Evidence must say so (see the protocol-v3 notes in
``docs/requirements/GOV-008_Research_First_Governance_Refactor.md`` section 14.1).

Layout under ``<execution_root>/outputs/runtime/capture_holds`` (git-ignored runtime state):
``records/<hold_id>.json`` is written once; ``released/<hold_id>.json`` marks a hold released. A
hold is live when its record exists, no released marker exists and it has not expired. Acquisition
and release serialize on the store's OS file lock, so a crashed holder never leaves the lock held.
"""

from __future__ import annotations

import hashlib
import os
import re
import secrets
import socket
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn

from ai_trading_system.contracts.prospective_event_time_evidence import (
    EventBinding,
    TemporalEvidenceError,
    canonical_json_bytes,
    parse_utc_datetime,
    strict_json_loads,
)
from ai_trading_system.core.provenance import UNAVAILABLE, git_provenance
from ai_trading_system.data.immutable_publish import (
    DataPublicationIntegrityError,
    exclusive_store_maintenance,
    read_contained_artifact_bytes,
    write_contained_artifact_bytes,
)

HOLD_ROOT_RELATIVE = "outputs/runtime/capture_holds"
HOLD_SCHEMA = "capture_hold.v1"
RELEASE_SCHEMA = "capture_hold_release.v1"
CHECK_SCHEMA = "capture_hold_check.v1"
# Proof schemas that wrap a hold check (the dispatch and capture parents add their own fields).
ACCEPTED_PROOF_SCHEMAS = frozenset(
    {CHECK_SCHEMA, "named_dq_existing_parent_proof.v2", "prospective_capture_parent_proof.v2"}
)
# Invariant, not a tunable: a crashed holder blocks overlapping captures for at most one day, and
# every capture manifest already expires by the next XNYS decision close.
MAX_HOLD_SECONDS = 24 * 60 * 60

_HOLD_ID = re.compile(r"hold-[0-9a-f]{20}")
_COMMIT = re.compile(r"[0-9a-f]{40}")
_RECORD_FIELDS = {
    "schema_version",
    "hold_id",
    "actor",
    "candidate_commit",
    "execution_root",
    "required_paths",
    "holder_pid",
    "holder_host",
    "acquired_at",
    "expires_at",
}


class CaptureHoldError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def _fail(code: str, message: str) -> NoReturn:
    raise CaptureHoldError(code, message)


def _now() -> datetime:
    return datetime.now(UTC)


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _object(value: object) -> dict[str, Any]:
    if type(value) is not dict or any(type(key) is not str for key in value):
        _fail("CAPTURE_HOLD_OBJECT_REQUIRED", "strict JSON object required")
    return value


def _text(value: object) -> str:
    if type(value) is not str or not value or value != value.strip():
        _fail("CAPTURE_HOLD_TEXT_INVALID", "nonempty exact string required")
    return value


def _paths(value: object) -> tuple[str, ...]:
    if type(value) not in {tuple, list} or not value:
        _fail("CAPTURE_HOLD_PATHS_INVALID", "nonempty path sequence required")
    result = []
    for item in value:  # type: ignore[attr-defined]
        EventBinding(_text(item), "0" * 64, 0)  # portable relative path syntax
        result.append(item)
    if len({item.casefold() for item in result}) != len(result):
        _fail("CAPTURE_HOLD_PATHS_INVALID", "duplicate path")
    return tuple(sorted(result))


def covered(path: str, scope: str) -> bool:
    """Case-insensitive, component-wise containment (Windows path claims)."""
    target, declaration = PurePosixPath(path.casefold()), PurePosixPath(scope.casefold())
    return target == declaration or declaration in target.parents


def _overlap(left: tuple[str, ...], right: tuple[str, ...]) -> bool:
    return any(covered(a, b) or covered(b, a) for a in left for b in right)


def _root(execution_root: Path) -> Path:
    if not execution_root.is_absolute() or execution_root.resolve(strict=True) != execution_root:
        _fail("CAPTURE_HOLD_ROOT_INVALID", str(execution_root))
    return execution_root


def _store(root: Path, *, create: bool) -> Path:
    store = root / HOLD_ROOT_RELATIVE
    if create:
        (store / "records").mkdir(parents=True, exist_ok=True)
        (store / "released").mkdir(parents=True, exist_ok=True)
    return store


def _read(store: Path, relative: str) -> bytes | None:
    try:
        return read_contained_artifact_bytes(root=store, relative_path=relative)
    except DataPublicationIntegrityError as exc:
        if exc.code == "CONTAINED_ARTIFACT_MISSING" or (
            exc.code == "ARTIFACT_BOUND_DIRECTORY_FAILED"
            and isinstance(exc.__cause__, FileNotFoundError)
        ):
            return None
        raise


def _parse_record(content: bytes) -> dict[str, Any]:
    try:
        record = _object(strict_json_loads(content))
    except TemporalEvidenceError as exc:
        raise CaptureHoldError("CAPTURE_HOLD_RECORD_INVALID", str(exc)) from exc
    if canonical_json_bytes(record) != content or set(record) != _RECORD_FIELDS:
        _fail("CAPTURE_HOLD_RECORD_INVALID", "exact canonical record schema required")
    if (
        record["schema_version"] != HOLD_SCHEMA
        or _HOLD_ID.fullmatch(_text(record["hold_id"])) is None
        or _COMMIT.fullmatch(_text(record["candidate_commit"])) is None
        or type(record["holder_pid"]) is not int
        or record["holder_pid"] <= 0
    ):
        _fail("CAPTURE_HOLD_RECORD_INVALID", "field values")
    _text(record["actor"])
    _text(record["holder_host"])
    _text(record["execution_root"])
    _paths(record["required_paths"])
    acquired, expires = (
        parse_utc_datetime(record["acquired_at"]),
        parse_utc_datetime(record["expires_at"]),
    )
    if not acquired < expires <= acquired + timedelta(seconds=MAX_HOLD_SECONDS):
        _fail("CAPTURE_HOLD_RECORD_INVALID", "validity window")
    return record


def _released(store: Path, hold_id: str) -> bool:
    marker = _read(store, f"released/{hold_id}.json")
    if marker is None:
        return False
    row = _object(strict_json_loads(marker))
    if set(row) != {"schema_version", "hold_id", "released_at"} or (
        row["schema_version"] != RELEASE_SCHEMA or row["hold_id"] != hold_id
    ):
        _fail("CAPTURE_HOLD_RELEASE_INVALID", hold_id)
    parse_utc_datetime(row["released_at"])
    return True


def _live_records(store: Path, now: datetime) -> list[dict[str, Any]]:
    live = []
    records = store / "records"
    for entry in sorted(records.iterdir()) if records.is_dir() else ():
        content = _read(store, f"records/{entry.name}")
        assert content is not None
        record = _parse_record(content)
        if entry.name != record["hold_id"] + ".json":
            _fail("CAPTURE_HOLD_RECORD_INVALID", entry.name)
        if parse_utc_datetime(record["expires_at"]) > now and not _released(
            store, record["hold_id"]
        ):
            live.append(record)
    return live


@dataclass
class CaptureHold:
    """Typed handle for one live hold. Possession grants nothing: every use re-reads the record."""

    root: Path
    record: dict[str, Any]
    record_bytes: bytes
    released: bool = field(default=False)

    @property
    def hold_id(self) -> str:
        return str(self.record["hold_id"])

    @property
    def actor(self) -> str:
        return str(self.record["actor"])

    @property
    def expires_at(self) -> datetime:
        return parse_utc_datetime(self.record["expires_at"])


def _identity(root: Path, candidate_commit: str) -> dict[str, Any]:
    """Commit and code state read from git now; anything unreadable is a failure, not a pass."""
    if _COMMIT.fullmatch(_text(candidate_commit)) is None:
        _fail("CAPTURE_HOLD_CANDIDATE_INVALID", candidate_commit)
    observed = git_provenance(root)
    if observed["commit"] == UNAVAILABLE or observed["code_modified"] == UNAVAILABLE:
        _fail("CAPTURE_HOLD_PROVENANCE_UNAVAILABLE", "git state unreadable")
    if observed["commit"] != candidate_commit:
        _fail("CAPTURE_HOLD_COMMIT_DRIFT", f"HEAD {observed['commit']} != {candidate_commit}")
    if observed["code_modified"] is not False:
        _fail("CAPTURE_HOLD_CODE_MODIFIED", "result-affecting code differs from the candidate")
    return observed


def acquire_capture_hold(
    *,
    execution_root: Path,
    candidate_commit: str,
    required_paths: tuple[str, ...],
    actor: str,
    ttl_seconds: int,
) -> CaptureHold:
    root = _root(execution_root)
    required = _paths(required_paths)
    _text(actor)
    if type(ttl_seconds) is not int or not 0 < ttl_seconds <= MAX_HOLD_SECONDS:
        _fail("CAPTURE_HOLD_TTL_INVALID", f"1..{MAX_HOLD_SECONDS} seconds required")
    _identity(root, candidate_commit)
    store = _store(root, create=True)
    with exclusive_store_maintenance(store_root=store):
        now = _now()
        for other in _live_records(store, now):
            if _overlap(required, tuple(other["required_paths"])):
                _fail("CAPTURE_HOLD_PATH_CONFLICT", f"overlaps live hold {other['hold_id']}")
        record = {
            "schema_version": HOLD_SCHEMA,
            "hold_id": "hold-" + secrets.token_hex(10),
            "actor": actor,
            "candidate_commit": candidate_commit,
            "execution_root": root.as_posix(),
            "required_paths": list(required),
            "holder_pid": os.getpid(),
            "holder_host": socket.gethostname(),
            "acquired_at": now.isoformat(),
            "expires_at": (now + timedelta(seconds=ttl_seconds)).isoformat(),
        }
        content = canonical_json_bytes(record)
        write_contained_artifact_bytes(
            root=store,
            relative_path=f"records/{record['hold_id']}.json",
            content=content,
            immutable=True,
        )
    return CaptureHold(root, record, content)


def _live_record(root: Path, hold_id: str) -> tuple[dict[str, Any], bytes]:
    if _HOLD_ID.fullmatch(_text(hold_id)) is None:
        _fail("CAPTURE_HOLD_ID_INVALID", hold_id)
    store = _store(root, create=False)
    content = _read(store, f"records/{hold_id}.json")
    if content is None:
        _fail("CAPTURE_HOLD_INACTIVE", hold_id)
    record = _parse_record(content)
    if (
        record["hold_id"] != hold_id
        or record["execution_root"] != root.as_posix()
        or _released(store, hold_id)
        or parse_utc_datetime(record["expires_at"]) <= _now()
    ):
        _fail("CAPTURE_HOLD_INACTIVE", hold_id)
    return record, content


def restore_capture_hold(
    *,
    execution_root: Path,
    hold_id: str,
    candidate_commit: str,
    required_paths: tuple[str, ...],
) -> CaptureHold:
    """Rebuild a handle in a child process; the id is only an address into the record."""
    root = _root(execution_root)
    record, content = _live_record(root, hold_id)
    hold = CaptureHold(root, record, content)
    recheck_capture_hold(hold, candidate_commit=candidate_commit, required_paths=required_paths)
    return hold


def recheck_capture_hold(
    hold: CaptureHold, *, candidate_commit: str, required_paths: tuple[str, ...]
) -> dict[str, Any]:
    """Re-read the record and git state; a caller-built handle cannot grant scope."""
    if type(hold) is not CaptureHold or hold.released:
        _fail("CAPTURE_HOLD_REQUIRED", "live typed capture hold required")
    required = _paths(required_paths)
    record, content = _live_record(hold.root, hold.hold_id)
    if content != hold.record_bytes or record["candidate_commit"] != candidate_commit:
        _fail("CAPTURE_HOLD_RECORD_DRIFT", hold.hold_id)
    checked = _now()
    identity = _identity(hold.root, candidate_commit)
    if (
        not parse_utc_datetime(record["acquired_at"])
        <= checked
        < parse_utc_datetime(record["expires_at"])
    ):
        _fail("CAPTURE_HOLD_INACTIVE", hold.hold_id)
    for path in required:
        if not any(covered(path, scope) for scope in record["required_paths"]):
            _fail("CAPTURE_HOLD_SCOPE_INVALID", path)
    return {
        "schema_version": CHECK_SCHEMA,
        "status": "PASS",
        "checked_at": checked.isoformat(),
        "candidate_commit": candidate_commit,
        "execution_root": hold.root.as_posix(),
        "active_hold": record,
        "hold_record_sha256": _sha(content),
        "hold_record_bytes_hex": content.hex(),
        "required_paths": list(required),
        "provenance": {
            "commit": identity["commit"],
            "branch": identity["branch"],
            "code_modified": identity["code_modified"],
        },
        "hold_acquired_or_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def release_capture_hold(hold: CaptureHold) -> None:
    if type(hold) is not CaptureHold or hold.released:
        _fail("CAPTURE_HOLD_REQUIRED", "live typed capture hold required")
    store = _store(hold.root, create=False)
    with exclusive_store_maintenance(store_root=store):
        if not _released(store, hold.hold_id):
            write_contained_artifact_bytes(
                root=store,
                relative_path=f"released/{hold.hold_id}.json",
                content=canonical_json_bytes(
                    {
                        "schema_version": RELEASE_SCHEMA,
                        "hold_id": hold.hold_id,
                        "released_at": _now().isoformat(),
                    }
                ),
                immutable=True,
            )
    hold.released = True


def release_capture_hold_by_id(*, execution_root: Path, hold_id: str) -> None:
    """Operator path for a crashed holder: release without a live handle."""
    root = _root(execution_root)
    record, content = _live_record(root, hold_id)
    release_capture_hold(CaptureHold(root, record, content))


def verify_retained_capture_hold_proof(
    proof: object,
    *,
    execution_root: Path,
    candidate_commit: str,
    required_paths: tuple[str, ...],
    source_hold_id: str,
    checked_at: datetime,
) -> None:
    """Validate a retained check against the record bytes it carries, at its recorded time.

    This is retained local evidence, not a new action permission and not an independent
    authority: a released or expired hold does not erase a valid original check, and the record
    cannot be cross-checked against any second store.
    """
    root = _root(execution_root)
    if _COMMIT.fullmatch(_text(candidate_commit)) is None:
        _fail("CAPTURE_HOLD_CANDIDATE_INVALID", candidate_commit)
    if _HOLD_ID.fullmatch(_text(source_hold_id)) is None:
        _fail("CAPTURE_HOLD_ID_INVALID", source_hold_id)
    required = _paths(required_paths)
    if type(checked_at) is not datetime or checked_at.tzinfo is None:
        _fail("CAPTURE_HOLD_RETAINED_TIME_INVALID", "aware original check time required")
    checked = parse_utc_datetime(checked_at.isoformat())
    raw = _object(proof)
    if (
        raw.get("schema_version") not in ACCEPTED_PROOF_SCHEMAS
        or raw.get("status") != "PASS"
        or raw.get("candidate_commit") != candidate_commit
        or raw.get("execution_root") != root.as_posix()
        or parse_utc_datetime(raw.get("checked_at")) != checked
        or raw.get("hold_acquired_or_mutated") is not False
        or raw.get("production_effect") != "none"
        or raw.get("broker_action") != "none"
    ):
        _fail("CAPTURE_HOLD_RETAINED_PROOF_INVALID", source_hold_id)
    hex_value = _text(raw.get("hold_record_bytes_hex"))
    if re.fullmatch(r"(?:[0-9a-f]{2})+", hex_value) is None:
        _fail("CAPTURE_HOLD_RETAINED_BYTES_INVALID", "hold_record")
    content = bytes.fromhex(hex_value)
    record = _parse_record(content)
    if _sha(content) != raw.get("hold_record_sha256") or canonical_json_bytes(
        raw.get("active_hold")
    ) != canonical_json_bytes(record):
        _fail("CAPTURE_HOLD_RETAINED_BYTES_INVALID", "hold_record")
    provenance = _object(raw.get("provenance"))
    if (
        record["hold_id"] != source_hold_id
        or record["candidate_commit"] != candidate_commit
        or record["execution_root"] != root.as_posix()
        or provenance.get("commit") != candidate_commit
        or provenance.get("code_modified") is not False
        or not parse_utc_datetime(record["acquired_at"])
        <= checked
        < parse_utc_datetime(record["expires_at"])
    ):
        _fail("CAPTURE_HOLD_RETAINED_HOLD_INVALID", source_hold_id)
    for path in (*required, *_paths(raw.get("required_paths"))):
        if not any(covered(path, scope) for scope in record["required_paths"]):
            _fail("CAPTURE_HOLD_SCOPE_INVALID", path)
