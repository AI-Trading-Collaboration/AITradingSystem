"""DEVX-016 S3: the hash-chained run journal of one publication run, and its single-writer lock.

Every step of a run records what it actually did (the exact argv, exit code, evidence files and
their hashes, the facts later steps depend on). The journal is append-only and each entry binds the
hash of the previous one, so `resume` can replay it read-only, refuse a tampered or truncated
history, and continue from the first step that is not DONE. The journal records OBSERVATIONS; the
fence transaction and the lease remain the authority for every external effect, and nothing here
grants a push.

Writes go through the canonical atomic writer (a run journal is small: one line per step event).
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.platform.artifacts.writer import canonical_json_bytes, write_bytes_atomic

ENTRY_SCHEMA_VERSION = "devx_016_publication_journal_entry.v1"
LOCK_SCHEMA_VERSION = "devx_016_publication_run_lock.v1"
STEP_STATUSES = frozenset({"STARTED", "DONE", "FAILED", "NEEDS_REVIEW", "AWAITING_AUTHORIZATION"})
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


class PublicationJournalError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class JournalEntry:
    sequence: int
    step_id: str
    status: str
    recorded_at: str
    detail: Mapping[str, Any]
    previous_sha256: str | None
    entry_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {**_body(self), "entry_sha256": self.entry_sha256}


@dataclass(frozen=True)
class JournalReplay:
    status: str
    entries: tuple[JournalEntry, ...]
    issues: tuple[str, ...]

    def last_status(self, step_id: str) -> str | None:
        for entry in reversed(self.entries):
            if entry.step_id == step_id:
                return entry.status
        return None

    def last_detail(self, step_id: str, *, status: str = "DONE") -> Mapping[str, Any] | None:
        for entry in reversed(self.entries):
            if entry.step_id == step_id and entry.status == status:
                return entry.detail
        return None

    @property
    def head_sha256(self) -> str | None:
        return self.entries[-1].entry_sha256 if self.entries else None


class PublicationJournal:
    def __init__(self, path: Path) -> None:
        self.path = path

    def replay(self) -> JournalReplay:
        if not self.path.exists():
            return JournalReplay("PASS", (), ())
        issues: list[str] = []
        entries: list[JournalEntry] = []
        previous: str | None = None
        text = self.path.read_text(encoding="utf-8")
        for number, line in enumerate(text.split("\n"), start=1):
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                issues.append(f"JOURNAL_LINE_UNREADABLE:{number}")
                continue
            entry = _entry_from_row(row)
            if entry is None:
                issues.append(f"JOURNAL_ENTRY_SHAPE:{number}")
                continue
            if entry.sequence != len(entries) + 1:
                issues.append(f"JOURNAL_SEQUENCE:{number}")
            if entry.previous_sha256 != previous:
                issues.append(f"JOURNAL_CHAIN:{number}")
            if _entry_hash(entry) != entry.entry_sha256:
                issues.append(f"JOURNAL_ENTRY_HASH:{number}")
            if entry.status not in STEP_STATUSES:
                issues.append(f"JOURNAL_STATUS:{number}")
            previous = entry.entry_sha256
            entries.append(entry)
        return JournalReplay("PASS" if not issues else "FAIL", tuple(entries), tuple(issues))

    def append(
        self,
        *,
        step_id: str,
        status: str,
        detail: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> JournalEntry:
        if not _IDENTIFIER.match(step_id):
            raise PublicationJournalError("PUBLICATION_RUN_STEP_ID_INVALID", step_id)
        if status not in STEP_STATUSES:
            raise PublicationJournalError("PUBLICATION_RUN_STATUS_INVALID", status)
        replay = self.replay()
        if replay.status != "PASS":
            raise PublicationJournalError(
                "PUBLICATION_RUN_JOURNAL_INVALID", ",".join(replay.issues)
            )
        instant = (now or datetime.now(tz=UTC)).astimezone(UTC)
        draft = JournalEntry(
            sequence=len(replay.entries) + 1,
            step_id=step_id,
            status=status,
            recorded_at=instant.isoformat(),
            detail=_plain(detail or {}),
            previous_sha256=replay.head_sha256,
            entry_sha256="",
        )
        entry = replace(draft, entry_sha256=_entry_hash(draft))
        existing = self.path.read_bytes() if self.path.exists() else b""
        write_bytes_atomic(self.path, existing + _compact(entry.to_dict()) + b"\n")
        return entry


class PublicationRunLock:
    """One orchestrator per run directory. A leftover lock is never taken silently."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def acquire(self, *, owner: str, takeover: bool = False, now: datetime | None = None) -> None:
        instant = (now or datetime.now(tz=UTC)).astimezone(UTC)
        body = canonical_json_bytes(
            {
                "schema_version": LOCK_SCHEMA_VERSION,
                "owner": owner,
                "acquired_at": instant.isoformat(),
                "production_effect": "none",
                "broker_action": "none",
            }
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            if not takeover:
                raise PublicationJournalError(
                    "PUBLICATION_RUN_LOCKED",
                    f"{self.path.name} 自 {self._held_since()} 起被占用；"
                    "只有确认前一个 orchestrator 进程已不存在后才可传 takeover",
                )
        write_bytes_atomic(self.path, body)

    def release(self) -> None:
        self.path.unlink(missing_ok=True)

    def _held_since(self) -> str:
        try:
            held = json.loads(self.path.read_text(encoding="utf-8"))
            return str(held.get("acquired_at"))
        except (OSError, ValueError):
            return "unknown time"


def _plain(value: Any) -> Any:
    """Exact JSON types only: tuples become lists, mappings dicts; anything else is refused."""
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise PublicationJournalError("PUBLICATION_RUN_DETAIL_INVALID", type(value).__name__)


def _compact(value: Any) -> bytes:
    """One-line canonical JSON: the journal is line-oriented, canonical_json_bytes is indented."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def _body(entry: JournalEntry) -> dict[str, Any]:
    return {
        "schema_version": ENTRY_SCHEMA_VERSION,
        "sequence": entry.sequence,
        "step_id": entry.step_id,
        "status": entry.status,
        "recorded_at": entry.recorded_at,
        "detail": dict(entry.detail),
        "previous_sha256": entry.previous_sha256,
    }


def _entry_hash(entry: JournalEntry) -> str:
    return hashlib.sha256(_compact(_body(entry))).hexdigest()


def _entry_from_row(row: object) -> JournalEntry | None:
    if not isinstance(row, dict) or row.get("schema_version") != ENTRY_SCHEMA_VERSION:
        return None
    sequence = row.get("sequence")
    step_id = row.get("step_id")
    status = row.get("status")
    recorded_at = row.get("recorded_at")
    detail = row.get("detail")
    previous = row.get("previous_sha256")
    digest = row.get("entry_sha256")
    if (
        not isinstance(sequence, int)
        or isinstance(sequence, bool)
        or not isinstance(step_id, str)
        or not isinstance(status, str)
        or not isinstance(recorded_at, str)
        or not isinstance(detail, dict)
        or not (previous is None or isinstance(previous, str))
        or not isinstance(digest, str)
    ):
        return None
    return JournalEntry(sequence, step_id, status, recorded_at, detail, previous, digest)
