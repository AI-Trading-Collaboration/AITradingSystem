"""DEVX-023 S: opt-in sealed replay of terminal lease chains.

Why: a replay parses and validates every event of every lease chain, and about 875 of the 893 chains
of the real store are terminal (their content never changes), so almost all of the replay's time
re-proves the same bytes. A seal records, per terminal chain whose full validation had no issue, the
exact files (name and SHA-256), the row-table blobs they name, and the head event. A later replay
re-hashes EVERY event file and every named blob; a chain whose bytes are identical is not validated
again event by event (only its head event is parsed in full), every other chain is validated exactly
as the serial replay does.

Trust model (owner-approved 2026-10-09, DEVX-023 section 10.3 and 10.9):
- the seal replaces no integrity check: any added, removed, changed or reordered byte of a chain
  makes that chain fall back to the full validation, whose conclusion (issues included) is the
  serial one;
- what is trusted is one conclusion: "these bytes passed the full validation under this code and
  policy". The seal is bound to a fingerprint of that code and policy; if any of it changes the
  whole seal is ignored and the replay is the full one;
- the seal is local state of the same trust level as the lease store, not a signature;
- every problem with the seal itself (absent, unreadable, wrong schema, wrong digest, wrong
  fingerprint, malformed) means "no seal": the caller runs its normal replay (fail safe, never
  open);
- default off: with AITS_LEASE_SEAL unset nothing here is imported and nothing is read.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from ai_trading_system.platform.architecture import parallel_control_kernel as kernel
from ai_trading_system.platform.architecture.parallel_control import (
    ControlIssue,
    ParallelControlError,
)
from ai_trading_system.platform.artifacts.writer import canonical_json_bytes, write_bytes_atomic

SEAL_ENV = "AITS_LEASE_SEAL"
SEAL_FILE_NAME = "replay_seal.v1.json"
SEAL_SCHEMA_VERSION = "lease_replay_seal.v1"
# Bump by hand when the meaning of a seal changes in a way the fingerprint cannot see (the
# fingerprint already covers this module's own source, so this is a belt-and-braces switch for a
# deliberate reset).
SEAL_CODE_VERSION = 1

_OFF_VALUES = frozenset({"", "0", "off", "false", "no"})
_ON_VALUES = frozenset({"1", "on", "true", "yes", "auto"})

# The states without an outgoing transition, taken from the kernel's own table (BLOCKED can still
# become RELEASED, so a BLOCKED chain is not final and is never sealed).
TERMINAL_STATES = frozenset(
    state
    for state, targets in kernel._LEASE_TRANSITIONS.items()
    if state is not None and not targets
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
# The fingerprint is a deliberate SUPERSET of the code that decides a validation conclusion (a
# replay executes workflow_coordination, the kernel, workflow_contract, workflow_integration and
# the artifact writer, but their static import closure is 42 modules): every Python source of the
# architecture platform and of the artifact helpers, the YAML loader, the two policies the lease
# store is built from, this module (it lives in the first directory), the interpreter's minor
# version and SEAL_CODE_VERSION. A superset can only invalidate a seal too often (it is rebuilt at
# the start of every publication chain), never too rarely.
FINGERPRINT_DIRECTORIES = (
    "src/ai_trading_system/platform/architecture",
    "src/ai_trading_system/platform/artifacts",
)
FINGERPRINT_FILES = (
    "src/ai_trading_system/yaml_loader.py",
    "config/architecture/arch_005_parallel_control_policy.yaml",
    "config/architecture/arch_005_s4d_checkout_guard.yaml",
)

# Diagnostics only (tests and measurements): why the last call did not return a sealed replay, and
# how the last sealed replay was composed.
LAST_SEAL_REASON: str | None = None
LAST_SEAL_STATS: SealedReplayStats | None = None


def seal_enabled(environ: Mapping[str, str] | None = None) -> bool:
    """False unless the switch is explicitly on; anything unparsable is off (fail safe)."""
    source = os.environ if environ is None else environ
    return source.get(SEAL_ENV, "").strip().lower() in _ON_VALUES


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def kernel_fingerprint(repository_root: Path | None = None) -> str:
    """Hash of the code and policy that decide a validation conclusion (see FINGERPRINT_*)."""
    root = REPOSITORY_ROOT if repository_root is None else repository_root
    entries: list[tuple[str, str]] = []
    for directory in FINGERPRINT_DIRECTORIES:
        for path in sorted((root / directory).rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            entries.append((path.relative_to(root).as_posix(), _sha256(path.read_bytes())))
    for relative in FINGERPRINT_FILES:
        target = root / relative
        entries.append((relative, _sha256(target.read_bytes()) if target.is_file() else "ABSENT"))
    header = (
        f"seal_code_version={SEAL_CODE_VERSION}\n"
        f"python={sys.version_info.major}.{sys.version_info.minor}\n"
    )
    return _sha256((header + "".join(f"{name}\0{digest}\n" for name, digest in entries)).encode())


def _canonical_digest(body: Mapping[str, Any]) -> str:
    text = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return _sha256(text.encode("utf-8"))


def _is_hex64(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and set(value) <= set("0123456789abcdef")


@dataclass(frozen=True)
class SealedChain:
    head_event_id: str
    head_state: str
    event_count: int
    files: tuple[tuple[str, str], ...]  # (file name, content sha256), sorted by name
    blobs: tuple[tuple[str, int], ...]  # (blob sha256, row count) named by the events, sorted
    chain_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "head_event_id": self.head_event_id,
            "head_state": self.head_state,
            "event_count": self.event_count,
            "files": [list(item) for item in self.files],
            "blobs": [list(item) for item in self.blobs],
            "chain_sha256": self.chain_sha256,
        }


def _chain_digest(
    head_event_id: str,
    head_state: str,
    files: tuple[tuple[str, str], ...],
    blobs: tuple[tuple[str, int], ...],
) -> str:
    return _canonical_digest(
        {
            "head_event_id": head_event_id,
            "head_state": head_state,
            "files": [list(item) for item in files],
            "blobs": [list(item) for item in blobs],
        }
    )


@dataclass(frozen=True)
class LoadedSeal:
    path: Path
    seal_sha256: str
    kernel_fingerprint: str
    created_at: str
    created_by: str
    source: Mapping[str, Any]
    chains: Mapping[str, SealedChain]


def _parse_chain(name: str, raw: object) -> SealedChain | None:
    if not isinstance(raw, Mapping) or set(raw) != {
        "head_event_id",
        "head_state",
        "event_count",
        "files",
        "blobs",
        "chain_sha256",
    }:
        return None
    head_event_id, head_state, count = raw["head_event_id"], raw["head_state"], raw["event_count"]
    if (
        not isinstance(head_event_id, str)
        or not head_event_id
        or head_state not in TERMINAL_STATES
        or type(count) is not int
        or count < 1
        or not isinstance(raw["files"], list)
        or not isinstance(raw["blobs"], list)
        or not _is_hex64(raw["chain_sha256"])
    ):
        return None
    files: list[tuple[str, str]] = []
    for item in raw["files"]:
        if (
            not isinstance(item, list)
            or len(item) != 2
            or not isinstance(item[0], str)
            or not item[0].endswith(".json")
            or not _is_hex64(item[1])
        ):
            return None
        files.append((item[0], item[1]))
    blobs: list[tuple[str, int]] = []
    for item in raw["blobs"]:
        if (
            not isinstance(item, list)
            or len(item) != 2
            or not _is_hex64(item[0])
            or type(item[1]) is not int
        ):
            return None
        blobs.append((item[0], item[1]))
    names = [item[0] for item in files]
    if (
        len(files) != count
        or names != sorted(names)
        or len(set(names)) != len(names)
        or f"{head_event_id}.json" not in names
        or blobs != sorted(blobs)
        or len(set(blobs)) != len(blobs)
    ):
        return None
    chain = SealedChain(
        head_event_id=head_event_id,
        head_state=head_state,
        event_count=count,
        files=tuple(files),
        blobs=tuple(blobs),
        chain_sha256=str(raw["chain_sha256"]),
    )
    if chain.chain_sha256 != _chain_digest(
        chain.head_event_id, chain.head_state, chain.files, chain.blobs
    ):
        return None
    return chain if name else None


def load_seal(path: Path, *, fingerprint: str) -> tuple[LoadedSeal | None, str | None]:
    """(seal, None) when the file is a valid seal for this code, else (None, reason)."""
    try:
        raw = path.read_bytes()
    except OSError as error:
        return None, "seal_absent" if isinstance(error, FileNotFoundError) else "seal_unreadable"
    try:
        document = json.loads(raw.decode("utf-8"))
    except ValueError:
        return None, "seal_not_json"
    if not isinstance(document, dict) or document.get("schema_version") != SEAL_SCHEMA_VERSION:
        return None, "seal_schema"
    body = {key: value for key, value in document.items() if key != "seal_sha256"}
    if not _is_hex64(document.get("seal_sha256")) or document["seal_sha256"] != _canonical_digest(
        body
    ):
        return None, "seal_digest"
    if body.get("kernel_fingerprint") != fingerprint:
        return None, "seal_fingerprint"
    chains_raw = body.get("chains")
    source = body.get("source")
    if (
        not isinstance(chains_raw, dict)
        or not isinstance(source, dict)
        or not isinstance(body.get("created_at"), str)
        or not isinstance(body.get("created_by"), str)
    ):
        return None, "seal_malformed"
    chains: dict[str, SealedChain] = {}
    for name, entry in chains_raw.items():
        parsed = _parse_chain(name, entry)
        if parsed is None:
            return None, "seal_malformed"
        chains[name] = parsed
    return (
        LoadedSeal(
            path=path,
            seal_sha256=str(document["seal_sha256"]),
            kernel_fingerprint=fingerprint,
            created_at=str(body["created_at"]),
            created_by=str(body["created_by"]),
            source=source,
            chains=chains,
        ),
        None,
    )


# --------------------------------------------------------------------------- shared small helpers


def _chain_directories(events_root: Path) -> list[Path]:
    """The chain directories as the serial replay's `*/*.json` sees them (built on events_root)."""
    if not events_root.exists():
        return []
    with os.scandir(events_root) as entries:
        names = sorted(entry.name for entry in entries if entry.is_dir())
    return [events_root / name for name in names]


def _event_files(directory: Path) -> list[Path]:
    return sorted(directory.glob("*.json"), key=lambda item: item.name)


def _parse_files(
    files: list[Path],
    blobs: kernel.ExternalizedRowsReader,
    events: list[kernel.LeaseEvent],
    issues: set[ControlIssue],
) -> None:
    """Exactly what the serial replay does for each event file."""
    for path in files:
        try:
            events.append(
                kernel.parse_lease_event(json.loads(path.read_text(encoding="utf-8")), blobs=blobs)
            )
        except (OSError, json.JSONDecodeError, ParallelControlError) as exc:
            issues.add(kernel._issue("LEASE_EVENT_INVALID", (), path.as_posix(), str(exc)))


class _RecordingReader(kernel.ExternalizedRowsReader):
    """Remembers which row-table blobs the events of a chain name (used only when building)."""

    seen: set[tuple[str, int]]

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        object.__setattr__(self, "seen", set())

    def read(self, sha256: str, row_count: int) -> kernel.ExternalizedRows:
        self.seen.add((sha256, row_count))
        return super().read(sha256, row_count)


def _blob_intact(
    blobs: kernel.ExternalizedRowsReader, digest: str, checked: dict[str, bool]
) -> bool:
    """The blob is a regular file of bounded size whose bytes hash to its own name."""
    known = checked.get(digest)
    if known is not None:
        return known
    ok = False
    try:
        path = blobs.path_for(digest)
        info = path.lstat()
        if (
            path.is_file()
            and not path.is_symlink()
            and info.st_size <= kernel.EXTERNALIZED_ROWS_MAX_BYTES
        ):
            ok = _sha256(path.read_bytes()) == digest
    except (OSError, ParallelControlError):
        ok = False
    checked[digest] = ok
    return ok


# ------------------------------------------------------------------------------- sealed replay


class SealBailout(Exception):  # noqa: N818 - a control-flow signal, not an error condition
    """The sealed route cannot give the serial replay's answer here: use the normal replay.

    The serial replay groups events by the lease id INSIDE them, not by their directory. If a
    directory that is validated in full holds an event of another lease id, that event belongs to
    another chain (a sealed one included) in the serial replay, which this directory-wise route
    cannot reproduce. It is not guessed at: the whole sealed route is abandoned.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass
class SealedReplayStats:
    sealed_chains: int = 0
    sealed_events: int = 0
    full_chains: int = 0
    full_events: int = 0
    # chain name -> why it was validated in full (only chains that were not taken from the seal)
    full_reasons: dict[str, str] = field(default_factory=dict)
    seconds: float = 0.0


def _sealed_head(
    directory: Path,
    files: list[Path],
    entry: SealedChain | None,
    blobs: kernel.ExternalizedRowsReader,
    checked_blobs: dict[str, bool],
) -> tuple[kernel.LeaseEvent | None, str]:
    """(head event, "") when the seal still describes this chain exactly, else (None, reason)."""
    if entry is None:
        return None, "unsealed"
    if len(files) != len(entry.files) or any(
        path.name != name for path, (name, _digest) in zip(files, entry.files, strict=True)
    ):
        return None, "files_changed"
    for path, (_name, digest) in zip(files, entry.files, strict=True):
        try:
            if _sha256(path.read_bytes()) != digest:
                return None, "files_changed"
        except OSError:
            return None, "files_changed"
    if any(not _blob_intact(blobs, digest, checked_blobs) for digest, _rows in entry.blobs):
        return None, "blob_changed"
    try:
        # Every byte of the chain, this head event included, is what passed the full validation
        # under this code and policy: the head is rebuilt (structure, schema, event id, blobs) but
        # its execution payload is not validated semantically a second time.
        head = kernel.parse_lease_event(
            json.loads((directory / f"{entry.head_event_id}.json").read_text(encoding="utf-8")),
            blobs=blobs,
            validate_execution_payload=False,
        )
    except (OSError, json.JSONDecodeError, ParallelControlError):
        return None, "head_invalid"
    if (
        head.event_id != entry.head_event_id
        or head.lease.lease_id != directory.name
        or head.to_state != entry.head_state
        or head.lease.state != entry.head_state
    ):
        return None, "head_mismatch"
    return head, ""


def sealed_replay(
    *, events_root: Path, blobs: kernel.ExternalizedRowsReader, seal: LoadedSeal
) -> tuple[kernel.LeaseReplay, SealedReplayStats]:
    """The store's replay with the sealed chains taken from the seal (module docstring)."""
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _validate_checked_execution_transition,
        replay_validation_scope,
    )

    started = time.perf_counter()
    stats = SealedReplayStats()
    sealed_heads: list[kernel.ExecutionLease] = []
    sealed_ids: list[tuple[str, str]] = []
    pending: list[kernel.LeaseEvent] = []
    issues: set[ControlIssue] = set()
    checked_blobs: dict[str, bool] = {}
    with replay_validation_scope(), kernel._rows_sharing_scope():
        for directory in _chain_directories(events_root):
            files = _event_files(directory)
            head, reason = _sealed_head(
                directory, files, seal.chains.get(directory.name), blobs, checked_blobs
            )
            if head is not None:
                sealed_heads.append(head.lease)
                sealed_ids.append((directory.name, head.event_id))
                stats.sealed_chains += 1
                stats.sealed_events += len(files)
                continue
            stats.full_chains += 1
            stats.full_events += len(files)
            stats.full_reasons[directory.name] = reason
            parsed: list[kernel.LeaseEvent] = []
            _parse_files(files, blobs, parsed, issues)
            if any(event.lease.lease_id != directory.name for event in parsed):
                raise SealBailout(f"lease_id_differs_from_directory:{directory.name}")
            pending.extend(parsed)
    part = kernel._replay_lease_events(
        pending,
        initial_issues=tuple(issues),
        execution_transition=_validate_checked_execution_transition,
    )
    replay = kernel._assemble_lease_replay(
        [*sealed_heads, *part.lease_heads],
        [*sealed_ids, *part.head_event_ids],
        set(part.issues),
        event_count=stats.sealed_events + part.event_count,
    )
    stats.seconds = time.perf_counter() - started
    return replay, stats


def replay_if_enabled(
    *, events_root: Path, blobs: kernel.ExternalizedRowsReader, seal_path: Path
) -> kernel.LeaseReplay | None:
    """The sealed replay, or None when the caller must run its normal replay."""
    global LAST_SEAL_REASON, LAST_SEAL_STATS
    LAST_SEAL_REASON = None
    LAST_SEAL_STATS = None
    if not seal_enabled():
        LAST_SEAL_REASON = "disabled"
        return None
    seal, reason = load_seal(seal_path, fingerprint=kernel_fingerprint())
    if seal is None:
        LAST_SEAL_REASON = reason
        return None
    try:
        replay, stats = sealed_replay(events_root=events_root, blobs=blobs, seal=seal)
    except SealBailout as bailout:
        LAST_SEAL_REASON = f"bailout:{bailout.reason}"
        return None
    except Exception as exc:  # noqa: BLE001 - any failure of the sealed route means "full replay"
        LAST_SEAL_REASON = f"sealed_failed:{type(exc).__name__}"
        return None
    LAST_SEAL_STATS = stats
    return replay


# ----------------------------------------------------------------------------------- building


@dataclass
class BuildReport:
    chains: int = 0
    events: int = 0
    sealed_chains: int = 0
    sealed_events: int = 0
    skipped_non_terminal: int = 0
    skipped_with_issues: int = 0
    skipped_other: int = 0
    # directories holding an event of another lease id: the sealed route will decline such a store
    foreign_event_directories: int = 0
    seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "chains": self.chains,
            "events": self.events,
            "sealed_chains": self.sealed_chains,
            "sealed_events": self.sealed_events,
            "skipped_non_terminal": self.skipped_non_terminal,
            "skipped_with_issues": self.skipped_with_issues,
            "skipped_other": self.skipped_other,
            "foreign_event_directories": self.foreign_event_directories,
            "seconds": round(self.seconds, 2),
        }


def build_seal_document(
    store: kernel.FileExecutionLeaseStore, *, created_at: datetime, created_by: str
) -> tuple[dict[str, Any], BuildReport]:
    """Validate every chain in full (no seal is consulted) and describe the sealable ones."""
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _validate_checked_execution_transition,
        replay_validation_scope,
    )

    started = time.perf_counter()
    report = BuildReport()
    recorder = _RecordingReader(store.blobs.root)
    chains: dict[str, dict[str, Any]] = {}
    with replay_validation_scope(), kernel._rows_sharing_scope():
        for directory in _chain_directories(store.events_root):
            files = _event_files(directory)
            report.chains += 1
            report.events += len(files)
            digests: list[tuple[str, str]] = []
            unreadable = False
            for path in files:
                try:
                    digests.append((path.name, _sha256(path.read_bytes())))
                except OSError:
                    unreadable = True
            recorder.seen.clear()
            events: list[kernel.LeaseEvent] = []
            issues: set[ControlIssue] = set()
            _parse_files(files, recorder, events, issues)
            part = kernel._replay_lease_events(
                events,
                initial_issues=tuple(issues),
                execution_transition=_validate_checked_execution_transition,
            )
            if any(event.lease.lease_id != directory.name for event in events):
                report.foreign_event_directories += 1
            if unreadable or not files:
                report.skipped_other += 1
                continue
            if part.issues or issues or len(events) != len(files):
                report.skipped_with_issues += 1
                continue
            heads, head_ids = part.lease_heads, part.head_event_ids
            if (
                len(heads) != 1
                or len(head_ids) != 1
                or heads[0].lease_id != directory.name
                or head_ids[0][0] != directory.name
                or any(event.lease.lease_id != directory.name for event in events)
                or part.event_count != len(files)
            ):
                report.skipped_other += 1
                continue
            if heads[0].state not in TERMINAL_STATES:
                report.skipped_non_terminal += 1
                continue
            file_tuple = tuple(digests)
            blob_tuple = tuple(sorted(recorder.seen))
            head_event_id = head_ids[0][1]
            entry = SealedChain(
                head_event_id=head_event_id,
                head_state=heads[0].state,
                event_count=len(files),
                files=file_tuple,
                blobs=blob_tuple,
                chain_sha256=_chain_digest(head_event_id, heads[0].state, file_tuple, blob_tuple),
            )
            chains[directory.name] = entry.to_dict()
            report.sealed_chains += 1
            report.sealed_events += len(files)
    report.seconds = time.perf_counter() - started
    body: dict[str, Any] = {
        "schema_version": SEAL_SCHEMA_VERSION,
        "kernel_fingerprint": kernel_fingerprint(),
        "created_at": created_at.isoformat(),
        "created_by": created_by,
        "source": report.to_dict() | {"seconds": None},
        "chains": chains,
    }
    return {**body, "seal_sha256": _canonical_digest(body)}, report


def write_seal(path: Path, document: Mapping[str, Any]) -> None:
    """One atomic replace: a reader sees the old seal or the new one, never a partial file."""
    write_bytes_atomic(path, canonical_json_bytes(dict(document), indent=None))


# ------------------------------------------------------------------------------ verify/status


@dataclass
class VerifyReport:
    ok: bool
    reason: str | None
    differing_fields: list[str]
    sealed_seconds: float
    full_seconds: float
    stats: SealedReplayStats | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "reason": self.reason,
            "differing_fields": self.differing_fields,
            "sealed_seconds": round(self.sealed_seconds, 2),
            "full_seconds": round(self.full_seconds, 2),
            "sealed_chains": None if self.stats is None else self.stats.sealed_chains,
            "full_chains": None if self.stats is None else self.stats.full_chains,
            "full_reasons": {} if self.stats is None else dict(self.stats.full_reasons),
        }


def verify(store: kernel.FileExecutionLeaseStore) -> VerifyReport:
    """The sealed replay against the serial full replay, field by field (`to_dict()` equality)."""
    seal, reason = load_seal(store.root / SEAL_FILE_NAME, fingerprint=kernel_fingerprint())
    if seal is None:
        return VerifyReport(False, reason, [], 0.0, 0.0, None)
    started = time.perf_counter()
    try:
        sealed, stats = sealed_replay(events_root=store.events_root, blobs=store.blobs, seal=seal)
    except SealBailout as bailout:
        return VerifyReport(False, f"bailout:{bailout.reason}", [], 0.0, 0.0, None)
    sealed_seconds = time.perf_counter() - started
    started = time.perf_counter()
    full = store._replay_serial()
    full_seconds = time.perf_counter() - started
    sealed_dict, full_dict = sealed.to_dict(), full.to_dict()
    differing = sorted(
        key
        for key in set(sealed_dict) | set(full_dict)
        if sealed_dict.get(key) != full_dict.get(key)
    )
    return VerifyReport(not differing, None, differing, sealed_seconds, full_seconds, stats)


def status(store: kernel.FileExecutionLeaseStore, *, replay: bool = True) -> dict[str, Any]:
    """Read-only description of the seal: why it is (not) in force and, optionally, what a sealed
    replay would do with the store as it is now."""
    path = store.root / SEAL_FILE_NAME
    current = kernel_fingerprint()
    recorded: str | None = None
    try:
        header = json.loads(path.read_bytes().decode("utf-8"))
        if isinstance(header, dict) and isinstance(header.get("kernel_fingerprint"), str):
            recorded = header["kernel_fingerprint"]
    except (OSError, ValueError):
        recorded = None
    seal, reason = load_seal(path, fingerprint=current)
    payload: dict[str, Any] = {
        "seal_path": str(path),
        "exists": path.is_file(),
        "size_bytes": path.stat().st_size if path.is_file() else None,
        "in_force_when_enabled": seal is not None,
        "reason": reason,
        "enabled_in_this_process": seal_enabled(),
        "current_kernel_fingerprint": current,
        "recorded_kernel_fingerprint": recorded,
        "sealed_chains": None if seal is None else len(seal.chains),
        "created_at": None if seal is None else seal.created_at,
        "created_by": None if seal is None else seal.created_by,
        "seal_sha256": None if seal is None else seal.seal_sha256,
    }
    if replay and seal is not None:
        _replay, stats = sealed_replay(events_root=store.events_root, blobs=store.blobs, seal=seal)
        payload["would_take_from_seal"] = {
            "sealed_chains": stats.sealed_chains,
            "sealed_events": stats.sealed_events,
            "full_chains": stats.full_chains,
            "full_events": stats.full_events,
            "full_reasons": dict(sorted(stats.full_reasons.items())),
            "seconds": round(stats.seconds, 2),
        }
    return payload
