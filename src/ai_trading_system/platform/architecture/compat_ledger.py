"""DEVX-016 P4-1: the active source ledger of the compatibility authority as one generic algebra.

The compatibility authority is an ordered list of sections. Most of them record, in `sources`, the
hash of every file the wave pinned. Until now the question "which hash is the CURRENT authority of
a path, and does the checkout still match it" was answered by ~100 hand-written per-wave tests plus
a nested resolver and several hand-maintained path sets in `tests/test_arch_004_refactor_policy.py`.

This module states the same thing once, as data:

- the ACTIVE LEDGER is replayed from the sections in order: `removed_live_source_paths` and
  `superseded_source_paths` pop a path, then the section's non-historical `frozen_sources` /
  `sources` records overwrite (last writer wins);
- a record MISMATCHES when the live file differs from it (after the record's own hash normalization
  and its declared normalization migrations) or is missing;
- a mismatch is allowed only when the reviewed policy names it: a path RETIRED by a listed section
  (that section supersedes the path in `superseded_live_source_paths` without re-listing it, as the
  canonical task-source cutover did for the task shadow registry), a VOLATILE generated path, or an
  ACKNOWLEDGED stale record (a pinned file that changed after its last record and is excused today
  only by retroactive rules in the legacy tests);
- policy entries must stay true: an entry that no longer refers to a ledger record, or an
  acknowledgement whose record matches again, is itself a violation (the policy can only shrink);
- every section must satisfy the universal safety checks; sections after the policy's
  `strict_structure_after_section` (the generated ones) must also satisfy the strict structure
  checks. Older sections are immutable history (their bytes are fixed by the index chain and the
  legacy prefix pin), so their historical irregularities are deliberately not re-litigated here.

Nothing in this module writes, hashes anything but files named by validated records, or decides an
investment interpretation; the checks are structural rules, and the only reviewed data is the policy
file (`config/architecture/devx_016_compat_ledger_policy.v1.yaml`).
"""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Protocol

from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_SCHEMA_VERSION = "devx_016_compat_ledger_policy.v1"
DEFAULT_POLICY_PATH = Path("config/architecture/devx_016_compat_ledger_policy.v1.yaml")
POLICY_STATUSES = frozenset({"PROPOSED_PENDING_OWNER_REVIEW", "OWNER_APPROVED_ENFORCED"})
NORMALIZATION_LF = "git_eol_lf"
SOURCE_KEYS = ("frozen_sources", "sources")
REMOVAL_KEYS = ("removed_live_source_paths", "superseded_source_paths")
HISTORICAL_FLAG_PREFIX = "historical_"
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
DECLARED_PATH_LISTS = (
    "source_delta_paths",
    "new_source_paths",
    "removed_live_source_paths",
    "superseded_live_source_paths",
)
_POLICY_FIELDS = {
    "schema_version",
    "policy_id",
    "version",
    "status",
    "owner",
    "approval_ref",
    "rationale",
    "intended_effect",
    "validation_evidence",
    "review_condition",
    "strict_structure_after_section",
    "retiring_sections",
    "volatile_generated_paths",
    "acknowledged_stale_records",
}


class CompatLedgerError(ValueError):
    """Typed fail-closed contract failure of the policy or of an input section."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


@dataclass(frozen=True)
class LedgerViolation:
    code: str
    subject: str
    detail: str = ""


@dataclass(frozen=True)
class LedgerRecord:
    path: str
    sha256: str
    normalization: str | None
    previous_worktree_sha256: str | None
    section_id: str
    order: int


@dataclass(frozen=True)
class ActiveLedger:
    records: Mapping[str, LedgerRecord]
    migrations: Mapping[str, tuple[LedgerRecord, ...]]
    # path -> (order, section id) of every section that superseded it without re-listing it
    retirements: Mapping[str, tuple[tuple[int, str], ...]]
    malformed: tuple[LedgerViolation, ...]
    section_ids: tuple[str, ...]


@dataclass(frozen=True)
class Mismatch:
    record: LedgerRecord
    live_sha256: str | None

    @property
    def kind(self) -> str:
        return "MISSING" if self.live_sha256 is None else "CHANGED"


class LiveHasher(Protocol):
    def sha256(self, path: str, normalization: str | None) -> str | None: ...


class FileSystemHasher:
    """Hashes the live checkout; a missing path is None, never an exception."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def sha256(self, path: str, normalization: str | None) -> str | None:
        target = self.root / path
        if not target.is_file():
            return None
        payload = target.read_bytes()
        if normalization == NORMALIZATION_LF:
            payload = payload.replace(b"\r\n", b"\n")
        return hashlib.sha256(payload).hexdigest()


# ----------------------------------------------------------------------------- policy


@dataclass(frozen=True)
class PolicyEntry:
    path: str
    reason: str
    authority: str = ""


@dataclass(frozen=True)
class CompatLedgerPolicy:
    policy_id: str
    version: str
    status: str
    owner: str
    strict_structure_after_section: str
    retiring_sections: tuple[PolicyEntry, ...]
    volatile_generated_paths: tuple[PolicyEntry, ...]
    acknowledged_stale_records: tuple[PolicyEntry, ...]
    sha256: str

    def to_report_dict(self) -> dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "version": self.version,
            "status": self.status,
            "sha256": self.sha256,
            "retiring_section_count": len(self.retiring_sections),
            "volatile_generated_path_count": len(self.volatile_generated_paths),
            "acknowledged_stale_record_count": len(self.acknowledged_stale_records),
        }


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CompatLedgerError("COMPAT_LEDGER_POLICY_FIELD", label)
    return value


def _portable(path: str, label: str, *, prefix: bool) -> str:
    candidate = PurePosixPath(path)
    bad = (
        "\\" in path
        or path.startswith("/")
        or (len(path) > 1 and path[1] == ":")
        or any(part in {"", ".", ".."} for part in path.rstrip("/").split("/"))
        or candidate.is_absolute()
    )
    if bad or path.endswith("/") != prefix:
        raise CompatLedgerError("COMPAT_LEDGER_POLICY_PATH", f"{label}: {path}")
    return path


def _section_entries(value: object, label: str) -> tuple[PolicyEntry, ...]:
    if not isinstance(value, list) or not value:
        raise CompatLedgerError("COMPAT_LEDGER_POLICY_FIELD", label)
    entries: list[PolicyEntry] = []
    for row in value:
        if not isinstance(row, dict) or set(row) != {"section_id", "reason"}:
            raise CompatLedgerError("COMPAT_LEDGER_POLICY_ENTRY", f"{label}: {row!r}")
        entries.append(
            PolicyEntry(
                path=_text(row["section_id"], f"{label}.section_id"),
                reason=_text(row["reason"], f"{label}.reason"),
            )
        )
    if len({entry.path for entry in entries}) != len(entries):
        raise CompatLedgerError("COMPAT_LEDGER_POLICY_DUPLICATE", label)
    return tuple(entries)


def _entries(
    value: object, label: str, *, prefix: bool, extra: str | None, allow_empty: bool = False
) -> tuple[PolicyEntry, ...]:
    if not isinstance(value, list) or not (value or allow_empty):
        raise CompatLedgerError("COMPAT_LEDGER_POLICY_FIELD", label)
    allowed = {"path", "reason"} | ({extra} if extra else set())
    entries: list[PolicyEntry] = []
    for row in value:
        if not isinstance(row, dict) or set(row) != allowed:
            raise CompatLedgerError("COMPAT_LEDGER_POLICY_ENTRY", f"{label}: {row!r}")
        path = _portable(_text(row["path"], f"{label}.path"), label, prefix=prefix)
        entries.append(
            PolicyEntry(
                path=path,
                reason=_text(row["reason"], f"{label}.reason"),
                authority=_text(row[extra], f"{label}.{extra}") if extra else "",
            )
        )
    keys = [entry.path.casefold() for entry in entries]
    if len(set(keys)) != len(keys):
        raise CompatLedgerError("COMPAT_LEDGER_POLICY_DUPLICATE", label)
    return tuple(entries)


def load_ledger_policy(path: Path) -> CompatLedgerPolicy:
    raw = path.read_bytes()
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict) or set(payload) != _POLICY_FIELDS:
        raise CompatLedgerError("COMPAT_LEDGER_POLICY_FIELDS", str(path))
    if payload["schema_version"] != POLICY_SCHEMA_VERSION:
        raise CompatLedgerError("COMPAT_LEDGER_POLICY_SCHEMA", str(payload["schema_version"]))
    status = _text(payload["status"], "status")
    if status not in POLICY_STATUSES:
        raise CompatLedgerError("COMPAT_LEDGER_POLICY_STATUS", status)
    approval = payload["approval_ref"]
    if status == "OWNER_APPROVED_ENFORCED" and not (isinstance(approval, str) and approval):
        raise CompatLedgerError(
            "COMPAT_LEDGER_POLICY_APPROVAL", "approved policy needs approval_ref"
        )
    for field in ("rationale", "intended_effect", "validation_evidence", "review_condition"):
        _text(payload[field], field)
    retiring = _section_entries(payload["retiring_sections"], "retiring_sections")
    volatile = _entries(
        payload["volatile_generated_paths"],
        "volatile_generated_paths",
        prefix=False,
        extra=None,
        allow_empty=True,
    )
    # The acknowledgements can only shrink: once every stale record is adopted the list is empty.
    acknowledged = _entries(
        payload["acknowledged_stale_records"],
        "acknowledged_stale_records",
        prefix=False,
        extra="recorded_by",
        allow_empty=True,
    )
    singles = [e.path for e in volatile] + [e.path for e in acknowledged]
    if len({p.casefold() for p in singles}) != len(singles):
        raise CompatLedgerError("COMPAT_LEDGER_POLICY_OVERLAP", "volatile/acknowledged")
    return CompatLedgerPolicy(
        policy_id=_text(payload["policy_id"], "policy_id"),
        version=_text(payload["version"], "version"),
        status=status,
        owner=_text(payload["owner"], "owner"),
        strict_structure_after_section=_text(
            payload["strict_structure_after_section"], "strict_structure_after_section"
        ),
        retiring_sections=retiring,
        volatile_generated_paths=volatile,
        acknowledged_stale_records=acknowledged,
        sha256=hashlib.sha256(raw).hexdigest(),
    )


# ----------------------------------------------------------------------------- ledger


def is_historical_record(row: Mapping[str, Any]) -> bool:
    return any(
        str(key).startswith(HISTORICAL_FLAG_PREFIX) and bool(value) for key, value in row.items()
    )


def _paths(value: object) -> list[str]:
    return [str(item) for item in value] if isinstance(value, list) else []


def _portable_record_path(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    bad = (
        not value
        or "\\" in value
        or value.startswith("/")
        or (len(value) > 1 and value[1] == ":")
        or ".." in value.split("/")
    )
    return None if bad else value


def _record(
    row: object, section_id: str, order: int, malformed: list[LedgerViolation]
) -> LedgerRecord | None:
    if not isinstance(row, dict) or not {"path", "sha256"} <= row.keys():
        return None  # not a hash record (some sections carry other row shapes in these keys)
    path = _portable_record_path(row["path"])
    sha = row["sha256"]
    normalization = row.get("hash_normalization")
    previous = row.get("previous_worktree_sha256")
    problems = []
    if path is None:
        problems.append("path")
    if not (isinstance(sha, str) and SHA256_PATTERN.match(sha)):
        problems.append("sha256")
    if normalization not in {None, NORMALIZATION_LF}:
        problems.append("hash_normalization")
    if previous is not None and not (isinstance(previous, str) and SHA256_PATTERN.match(previous)):
        problems.append("previous_worktree_sha256")
    if problems or path is None:
        malformed.append(
            LedgerViolation(
                "LEDGER_RECORD_MALFORMED", section_id, f"{','.join(problems)}: {row.get('path')!r}"
            )
        )
        return None
    return LedgerRecord(path, str(sha), normalization, previous, section_id, order)


def build_active_ledger(sections: Iterable[tuple[str, Mapping[str, Any]]]) -> ActiveLedger:
    """Replay the sections in order into the active ledger (last writer wins, removals pop)."""
    active: dict[str, LedgerRecord] = {}
    migrations: dict[str, list[LedgerRecord]] = defaultdict(list)
    retirements: dict[str, list[tuple[int, str]]] = defaultdict(list)
    malformed: list[LedgerViolation] = []
    ids: list[str] = []
    for order, (section_id, section) in enumerate(sections):
        ids.append(section_id)
        relisted = {
            str(row["path"])
            for row in (section.get("sources") or [])
            if isinstance(row, dict) and "path" in row
        }
        for path in _paths(section.get("superseded_live_source_paths")):
            if path not in relisted:
                retirements[path].append((order, section_id))
        for key in REMOVAL_KEYS:
            for path in _paths(section.get(key)):
                active.pop(path, None)
        for key in SOURCE_KEYS:
            rows = section.get(key)
            if not isinstance(rows, list):
                continue
            for row in rows:
                record = _record(row, section_id, order, malformed)
                if record is None:
                    continue
                if record.normalization == NORMALIZATION_LF and record.previous_worktree_sha256:
                    migrations[record.path].append(record)
                if not is_historical_record(row):
                    active[record.path] = record
    return ActiveLedger(
        records=active,
        migrations={path: tuple(rows) for path, rows in migrations.items()},
        retirements={path: tuple(rows) for path, rows in retirements.items()},
        malformed=tuple(malformed),
        section_ids=tuple(ids),
    )


def ledger_mismatches(ledger: ActiveLedger, hasher: LiveHasher) -> tuple[Mismatch, ...]:
    found: list[Mismatch] = []
    for path, record in ledger.records.items():
        live = hasher.sha256(path, record.normalization)
        if live == record.sha256:
            continue
        migrated = live is not None and any(
            record.sha256 == move.previous_worktree_sha256
            and hasher.sha256(path, move.normalization) == move.sha256
            for move in ledger.migrations.get(path, ())
        )
        if not migrated:
            found.append(Mismatch(record, live))
    return tuple(found)


# ----------------------------------------------------------------------------- evaluation


@dataclass(frozen=True)
class LedgerReport:
    policy: dict[str, Any]
    active_record_count: int
    mismatch_count: int
    retired: tuple[str, ...]
    volatile: tuple[str, ...]
    acknowledged: tuple[str, ...]
    violations: tuple[LedgerViolation, ...]

    @property
    def ok(self) -> bool:
        return not self.violations

    def codes(self) -> set[str]:
        return {violation.code for violation in self.violations}

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy": self.policy,
            "active_record_count": self.active_record_count,
            "mismatch_count": self.mismatch_count,
            "retired_count": len(self.retired),
            "volatile": list(self.volatile),
            "acknowledged": list(self.acknowledged),
            "violations": [
                {"code": v.code, "subject": v.subject, "detail": v.detail} for v in self.violations
            ],
        }


def evaluate_ledger(
    ledger: ActiveLedger, hasher: LiveHasher, policy: CompatLedgerPolicy
) -> LedgerReport:
    """Classify every live mismatch against the reviewed policy and check the policy stays true."""
    retiring = {entry.path for entry in policy.retiring_sections}
    volatile = {entry.path for entry in policy.volatile_generated_paths}
    acknowledged = {entry.path for entry in policy.acknowledged_stale_records}
    mismatches = ledger_mismatches(ledger, hasher)
    retired_hit: list[str] = []
    retiring_used: set[str] = set()
    volatile_hit: list[str] = []
    acknowledged_hit: list[str] = []
    violations: list[LedgerViolation] = list(ledger.malformed)
    mismatched_paths = {mismatch.record.path for mismatch in mismatches}
    for mismatch in mismatches:
        path = mismatch.record.path
        retired_by = [
            section_id
            for order, section_id in ledger.retirements.get(path, ())
            if section_id in retiring and order > mismatch.record.order
        ]
        if retired_by:
            retired_hit.append(path)
            retiring_used.update(retired_by)
        elif path in volatile:
            volatile_hit.append(path)
        elif path in acknowledged:
            acknowledged_hit.append(path)
        else:
            code = "LEDGER_LIVE_MISSING" if mismatch.kind == "MISSING" else "LEDGER_LIVE_DRIFT"
            violations.append(
                LedgerViolation(code, path, f"recorded by {mismatch.record.section_id}")
            )
    for entry in policy.retiring_sections:
        if entry.path not in ledger.section_ids:
            violations.append(LedgerViolation("LEDGER_POLICY_UNKNOWN_RETIRING_SECTION", entry.path))
        elif entry.path not in retiring_used:
            violations.append(LedgerViolation("LEDGER_POLICY_STALE_RETIRING_SECTION", entry.path))
    for entry in policy.volatile_generated_paths:
        if entry.path not in ledger.records:
            violations.append(LedgerViolation("LEDGER_POLICY_STALE_VOLATILE", entry.path))
    for entry in policy.acknowledged_stale_records:
        if entry.path not in ledger.records:
            violations.append(LedgerViolation("LEDGER_POLICY_STALE_ACKNOWLEDGEMENT", entry.path))
        elif entry.path not in mismatched_paths:
            violations.append(LedgerViolation("LEDGER_POLICY_ACKNOWLEDGEMENT_OBSOLETE", entry.path))
    return LedgerReport(
        policy=policy.to_report_dict(),
        active_record_count=len(ledger.records),
        mismatch_count=len(mismatches),
        retired=tuple(sorted(retired_hit)),
        volatile=tuple(sorted(volatile_hit)),
        acknowledged=tuple(sorted(acknowledged_hit)),
        violations=tuple(violations),
    )


# ----------------------------------------------------------------------------- structure


def _casefold_sorted(values: Sequence[str]) -> bool:
    return list(values) == sorted(values, key=str.casefold)


def validate_sections(
    sections: Sequence[tuple[str, Mapping[str, Any]]], *, strict_after_section: str
) -> tuple[LedgerViolation, ...]:
    """Universal safety checks for every section; strict structure for the generated tail."""
    ids = [section_id for section_id, _ in sections]
    if strict_after_section not in ids:
        raise CompatLedgerError("COMPAT_LEDGER_STRICT_SECTION_UNKNOWN", strict_after_section)
    strict_from = ids.index(strict_after_section) + 1
    violations: list[LedgerViolation] = []
    active: set[str] = set()
    for order, (section_id, section) in enumerate(sections):
        violations.extend(_universal_checks(section_id, section))
        if order >= strict_from:
            violations.extend(_strict_checks(section_id, section, active))
        for key in REMOVAL_KEYS:
            active.difference_update(_paths(section.get(key)))
        for key in SOURCE_KEYS:
            for row in section.get(key) or []:
                if isinstance(row, dict) and {"path", "sha256"} <= row.keys():
                    if not is_historical_record(row):
                        active.add(str(row["path"]))
    return tuple(violations)


def _universal_checks(section_id: str, section: Mapping[str, Any]) -> list[LedgerViolation]:
    found: list[LedgerViolation] = []
    for container, label in ((section, "section"), (section.get("safety"), "safety")):
        if not isinstance(container, dict):
            continue
        for key in ("production_effect", "broker_action"):
            if key in container and container[key] != "none":
                found.append(
                    LedgerViolation(
                        "LEDGER_SAFETY_FLAG", section_id, f"{label}.{key}={container[key]!r}"
                    )
                )
    supersession = section.get("supersession")
    if isinstance(supersession, dict) and supersession.get("historical_hashes_rewritten") is True:
        found.append(LedgerViolation("LEDGER_HISTORY_REWRITTEN", section_id))
    return found


def _strict_checks(
    section_id: str, section: Mapping[str, Any], active_before: set[str]
) -> list[LedgerViolation]:
    found: list[LedgerViolation] = []
    rows = section.get("sources")
    source_paths = (
        [str(row["path"]) for row in rows if isinstance(row, dict) and "path" in row]
        if isinstance(rows, list)
        else None
    )
    if source_paths is not None:
        if len(set(source_paths)) != len(source_paths):
            found.append(LedgerViolation("LEDGER_SECTION_SOURCES_DUPLICATE", section_id))
        if not _casefold_sorted(source_paths):
            found.append(LedgerViolation("LEDGER_SECTION_SOURCES_UNSORTED", section_id))
    declared = {key: section.get(key) for key in DECLARED_PATH_LISTS}
    for key, value in declared.items():
        if value is not None and not (
            isinstance(value, list) and _casefold_sorted([str(item) for item in value])
        ):
            found.append(LedgerViolation("LEDGER_SECTION_LIST_UNSORTED", section_id, key))
    delta = set(_paths(declared["source_delta_paths"]))
    new = set(_paths(declared["new_source_paths"]))
    removed = set(_paths(declared["removed_live_source_paths"]))
    superseded = set(_paths(declared["superseded_live_source_paths"]))
    if all(value is not None for value in declared.values()):
        if delta != (superseded | new) - removed:
            found.append(LedgerViolation("LEDGER_SECTION_DELTA_ALGEBRA", section_id))
        if source_paths is not None and set(source_paths) != delta:
            found.append(LedgerViolation("LEDGER_SECTION_SOURCES_NE_DELTA", section_id))
    if declared["superseded_live_source_paths"] is not None and source_paths is not None:
        if superseded - set(source_paths):
            found.append(LedgerViolation("LEDGER_SECTION_SUPERSEDED_NOT_RELISTED", section_id))
        if set(source_paths) - superseded - new:
            found.append(LedgerViolation("LEDGER_SECTION_SOURCES_BEYOND_DECLARED", section_id))
    if new & active_before:
        found.append(LedgerViolation("LEDGER_SECTION_NEW_PATH_ACTIVE", section_id))
    if removed - active_before:
        found.append(LedgerViolation("LEDGER_SECTION_REMOVED_NOT_ACTIVE", section_id))
    return found


# ----------------------------------------------------------------------------- repository


def merged_sections(merged: Mapping[str, Any]) -> list[tuple[str, Mapping[str, Any]]]:
    """The ordered sections of the merged authority (the head fields are not sections)."""
    return [(key, value) for key, value in merged.items() if isinstance(value, dict)]


def evaluate_repository(
    root: Path,
    *,
    merged: Mapping[str, Any],
    policy_path: Path = DEFAULT_POLICY_PATH,
) -> LedgerReport:
    """The whole check on a real checkout: policy, active ledger, structure."""
    policy = load_ledger_policy(root / policy_path)
    sections = merged_sections(merged)
    report = evaluate_ledger(build_active_ledger(sections), FileSystemHasher(root), policy)
    structure = validate_sections(
        sections, strict_after_section=policy.strict_structure_after_section
    )
    return LedgerReport(
        policy=report.policy,
        active_record_count=report.active_record_count,
        mismatch_count=report.mismatch_count,
        retired=report.retired,
        volatile=report.volatile,
        acknowledged=report.acknowledged,
        violations=report.violations + structure,
    )


__all__ = [
    "DEFAULT_POLICY_PATH",
    "ActiveLedger",
    "CompatLedgerError",
    "CompatLedgerPolicy",
    "FileSystemHasher",
    "LedgerRecord",
    "LedgerReport",
    "LedgerViolation",
    "LiveHasher",
    "Mismatch",
    "PolicyEntry",
    "build_active_ledger",
    "evaluate_ledger",
    "evaluate_repository",
    "ledger_mismatches",
    "load_ledger_policy",
    "merged_sections",
    "validate_sections",
]
