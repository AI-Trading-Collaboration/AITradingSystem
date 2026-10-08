"""Shared builders for the compat ledger tests: a synthetic authority on a temporary checkout."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ai_trading_system.platform.architecture.compat_ledger import (
    CompatLedgerPolicy,
    FileSystemHasher,
    LedgerReport,
    LedgerViolation,
    PolicyEntry,
    build_active_ledger,
    evaluate_ledger,
    validate_sections,
)

STRICT_AFTER = "s_cutoff"
Sections = list[tuple[str, Mapping[str, Any]]]


def sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


class Checkout:
    """A temporary checkout whose files the records of a synthetic authority pin."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def write(self, path: str, payload: bytes | str) -> None:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload.encode("utf-8") if isinstance(payload, str) else payload)

    def delete(self, path: str) -> None:
        (self.root / path).unlink()

    def record(self, path: str, *, lf: bool = False, **extra: Any) -> dict[str, Any]:
        """A source record that matches the file's CURRENT bytes."""
        payload = (self.root / path).read_bytes()
        if lf:
            payload = payload.replace(b"\r\n", b"\n")
        row: dict[str, Any] = {"path": path, "sha256": sha(payload)}
        if lf:
            row["hash_normalization"] = "git_eol_lf"
        row.update(extra)
        return row


def section(sources: Sequence[Mapping[str, Any]] | None = None, **fields: Any) -> dict[str, Any]:
    body: dict[str, Any] = dict(fields)
    if sources is not None:
        body["sources"] = [dict(row) for row in sources]
    return body


def policy(
    *,
    retiring: Sequence[str] = (),
    volatile: Sequence[str] = (),
    acknowledged: Sequence[str] = (),
) -> CompatLedgerPolicy:
    return CompatLedgerPolicy(
        policy_id="TEST",
        version="1.0.0",
        status="PROPOSED_PENDING_OWNER_REVIEW",
        owner="test",
        strict_structure_after_section=STRICT_AFTER,
        retiring_sections=tuple(PolicyEntry(s, "test") for s in retiring),
        volatile_generated_paths=tuple(PolicyEntry(p, "test") for p in volatile),
        acknowledged_stale_records=tuple(PolicyEntry(p, "test", "s") for p in acknowledged),
        sha256="0" * 64,
    )


def evaluate(
    checkout: Checkout, sections: Sections, rules: CompatLedgerPolicy | None = None
) -> LedgerReport:
    ledger = build_active_ledger(sections)
    return evaluate_ledger(ledger, FileSystemHasher(checkout.root), rules or policy())


def structure_codes(sections: Sections, *, strict_after: str = STRICT_AFTER) -> set[str]:
    return {v.code for v in validate_sections(sections, strict_after_section=strict_after)}


def violations(report: LedgerReport) -> list[LedgerViolation]:
    return list(report.violations)
