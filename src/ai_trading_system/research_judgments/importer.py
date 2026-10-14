"""Local-only, byte-preserving judgment imports with a chained audit ledger."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from ai_trading_system.research_judgments.schema import Issue, check_export, parse_timestamp


@dataclass
class ImportResult:
    status: Literal["imported", "unchanged", "rejected"]
    issues: list[Issue]
    seq: int | None
    counts: dict[str, int]
    export_sha256: str


# The initial predecessor is the protocol's empty-chain sentinel, not a policy threshold.
EMPTY_CHAIN_SHA256 = "0" * 64


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _reject_constant(_: str) -> Any:
    raise ValueError("invalid JSON constant")


def _json(raw: bytes) -> Any:
    return json.loads(raw.decode("utf-8"), parse_constant=_reject_constant)


def _read_chain(path: Path) -> tuple[list[dict[str, Any]], str]:
    if not path.exists():
        return [], EMPTY_CHAIN_SHA256
    raw = path.read_bytes()
    if raw and not raw.endswith(b"\n"):
        raise ValueError("invalid import chain")
    lines = raw.split(b"\n")[:-1]
    records: list[dict[str, Any]] = []
    previous = EMPTY_CHAIN_SHA256
    for seq, line in enumerate(lines, 1):
        record = _json(line)
        if (
            not isinstance(record, dict)
            or type(record.get("seq")) is not int
            or record["seq"] != seq
            or record.get("prev_line_sha256") != previous
        ):
            raise ValueError("invalid import chain")
        records.append(record)
        previous = _sha256(line)
    return records, previous


def _counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "rows": len(rows),
        "research": sum(row["purpose"] == "research" for row in rows),
        "learning": sum(row["purpose"] == "learning" for row in rows),
    }


def _changed(record: dict[str, Any], old: dict[str, Any]) -> bool:
    return any(
        record.get(field) != old.get(field)
        for field in ("status", "review", "errorType", "resolvedAt")
    ) or any(
        len(record.get(field, [])) > len(old.get(field, []))
        for field in ("addenda", "probRevisions")
    )


def import_export(
    path: Path,
    *,
    data_dir: Path,
    known_tickers: set[str],
    method_ids: set[str],
    now: datetime,
    dry_run: bool = False,
) -> ImportResult:
    """Validate before creating directories; results contain only metadata and issue codes."""
    sha = ""

    def rejected(issues: list[Issue]) -> ImportResult:
        return ImportResult("rejected", issues, None, {}, sha)

    try:
        raw = path.read_bytes()
        sha = _sha256(raw)
        doc = _json(raw)
    except (OSError, ValueError, RecursionError):
        return rejected([Issue("format")])
    log_path = data_dir / "imports.jsonl"
    exports_dir = data_dir / "exports"
    try:
        history, previous = _read_chain(log_path)
    except (OSError, ValueError, RecursionError):
        return rejected([Issue("chain_broken")])
    last = history[-1] if history else None
    if last is not None and last.get("export_sha256") == sha:
        return ImportResult(
            "unchanged",
            [],
            last["seq"],
            {
                "rows": last["rows"],
                "research": last["rows_research"],
                "learning": last["rows_learning"],
            },
            sha,
        )
    baseline: dict[str, dict[str, Any]] = {}
    last_exported: datetime | None = None
    if last is not None:
        name = last.get("file")
        if not isinstance(name, str):
            return rejected([Issue("baseline_tampered")])
        # Audit filenames must stay in exports, even if someone edits the local ledger.
        if Path(name).name != name or not name or name in (".", ".."):
            return rejected([Issue("baseline_tampered", detail=name)])
        try:
            baseline_raw = (exports_dir / name).read_bytes()
            if _sha256(baseline_raw) != last.get("export_sha256"):
                return rejected([Issue("baseline_tampered", detail=name)])
            baseline_doc = _json(baseline_raw)
            if check_export(baseline_doc, known_tickers=known_tickers, method_ids=method_ids):
                return rejected([Issue("baseline_tampered", detail=name)])
            baseline = {r["id"]: r for r in baseline_doc["judgments"]}
        except (OSError, ValueError, RecursionError):
            return rejected([Issue("baseline_tampered", detail=name)])
        last_exported = parse_timestamp(last.get("exported_at"))
        if last_exported is None:
            return rejected([Issue("chain_broken")])
    issues = check_export(
        doc, known_tickers=known_tickers, method_ids=method_ids, baseline=baseline
    )
    exported = parse_timestamp(doc.get("exported_at")) if isinstance(doc, dict) else None
    if last_exported is not None and exported is not None and exported <= last_exported:
        issues.append(Issue("stale_export", field="exported_at"))
    if issues:
        return rejected(issues)
    assert exported is not None
    rows = doc["judgments"]
    counts = _counts(rows)
    seq = len(history) + 1
    if dry_run:
        return ImportResult("imported", [], seq, counts, sha)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("import time must have a timezone")
    name = f"judgments_{exported.strftime('%Y%m%dT%H%M%SZ')}_{sha[:8]}.json"
    record = {
        "seq": seq,
        "imported_at": now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "file": name,
        "export_sha256": sha,
        "exported_at": doc["exported_at"],
        "rows": counts["rows"],
        "rows_research": counts["research"],
        "rows_learning": counts["learning"],
        "new_ids": sorted(r["id"] for r in rows if r["id"] not in baseline),
        "changed_ids": sorted(
            r["id"] for r in rows if r["id"] in baseline and _changed(r, baseline[r["id"]])
        ),
        "prev_line_sha256": previous,
    }
    line = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    exports_dir.mkdir(parents=True, exist_ok=True)
    (exports_dir / name).write_bytes(raw)
    with log_path.open("ab") as log:
        log.write(line.encode("utf-8") + b"\n")
    return ImportResult("imported", [], seq, counts, sha)
