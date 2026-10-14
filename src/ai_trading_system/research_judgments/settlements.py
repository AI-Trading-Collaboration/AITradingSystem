"""Private settlement snapshots and append-only, hash-chained audit records."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal

from ai_trading_system.research_judgments.importer import _json, _read_chain, _sha256
from ai_trading_system.research_judgments.prices import digest, read_closes
from ai_trading_system.research_judgments.schema import Issue
from ai_trading_system.research_judgments.settle import RETURN_DECIMALS, SettleParams, settle_rows

FORMAT = "judgment_settlements.v1"
LOG = "settlements.jsonl"
FILES = "settlements"
COUNT_STATES = ("settled", "pending", "void", "data_gap")
# Freeze tolerance follows the shared serialized return precision, not a readiness threshold.
RETURN_TOLERANCE = 10 ** -(RETURN_DECIMALS - 1)


@dataclass
class SettleResult:
    status: Literal["settled", "unchanged", "rejected"]
    issues: list[Issue]
    seq: int | None
    counts: dict[str, int]


def _verified_document(directory: Path, record: dict[str, Any], hash_key: str) -> dict[str, Any]:
    name = record.get("file")
    if not isinstance(name, str) or not name or name in (".", "..") or Path(name).name != name:
        raise ValueError("invalid audit filename")
    raw = (directory / name).read_bytes()
    if _sha256(raw) != record.get(hash_key):
        raise ValueError("audit checksum mismatch")
    doc = _json(raw)
    if not isinstance(doc, dict):
        raise ValueError("invalid audit document")
    return doc


def _price_changed(old: dict[str, Any], new: dict[str, Any]) -> bool:
    if any(old.get(key) != new.get(key) for key in ("state", "outcome", "anchor_date", "end_date")):
        return True
    old_rel, new_rel = old.get("rel_return"), new.get("rel_return")
    return old_rel is None or new_rel is None or abs(old_rel - new_rel) > RETURN_TOLERANCE


def settle(
    *,
    data_dir: Path,
    prices_path: Path,
    params: SettleParams,
    asof: date,
    now: datetime,
    dry_run: bool = False,
) -> SettleResult:
    """Validate and freeze before any write; rejected and dry-run calls create nothing."""

    def rejected(issues: list[Issue]) -> SettleResult:
        return SettleResult("rejected", issues, None, {})

    try:
        imports, _ = _read_chain(data_dir / "imports.jsonl")
    except (OSError, ValueError, RecursionError):
        return rejected([Issue("chain_broken")])
    if not imports:
        return rejected([Issue("no_import")])
    latest_import = imports[-1]
    exports_dir = data_dir / "exports"
    try:
        imported = _verified_document(exports_dir, latest_import, "export_sha256")
        rows = imported["judgments"]
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise ValueError("invalid imported rows")
    except (OSError, ValueError, KeyError, RecursionError):
        return rejected([Issue("baseline_tampered")])
    closes, issues = read_closes(prices_path)
    if closes is None:
        return rejected(issues)
    log_path = data_dir / LOG
    files_dir = data_dir / FILES
    old_doc: dict[str, Any] | None = None
    try:
        history, previous = _read_chain(log_path)
        if history:
            old_doc = _verified_document(files_dir, history[-1], "file_sha256")
            old_asof = date.fromisoformat(old_doc["asof"])
            old_rows = old_doc["outcomes"]
            if not isinstance(old_rows, list) or not all(isinstance(row, dict) for row in old_rows):
                raise ValueError("invalid settlement rows")
            if asof < old_asof:
                return rejected([Issue("asof_backwards")])
    except (OSError, ValueError, KeyError, TypeError, RecursionError):
        return rejected([Issue("chain_broken")])
    outcomes = settle_rows(rows, closes, params, asof)
    current = [outcome.as_dict() for outcome in outcomes]
    old_by_id = {row["id"]: row for row in old_doc["outcomes"]} if old_doc else {}
    new_by_id = {row["id"]: row for row in current}
    changed_ids: list[str] = []
    freeze_issues: list[Issue] = []
    for id_, old in sorted(old_by_id.items()):
        if old["state"] != "settled":
            continue
        new = new_by_id.get(id_, {})
        if old["source"] == "price":
            if _price_changed(old, new):
                freeze_issues.append(Issue("settlement_changed", id_, "outcome"))
        elif any(old.get(key) != new.get(key) for key in ("outcome", "state")):
            changed_ids.append(id_)
    if freeze_issues:
        return rejected(freeze_issues)
    counts = {state: sum(outcome.state == state for outcome in outcomes) for state in COUNT_STATES}
    counts["conflicts"] = sum(outcome.conflict for outcome in outcomes)
    if old_doc is not None and old_doc["outcomes"] == current:
        return SettleResult("unchanged", [], history[-1]["seq"], counts)
    used_tickers = {outcome.ticker for outcome in outcomes if outcome.source == "price"}
    used_tickers.update(
        row["market"]["benchmark"]
        for row in rows
        if row["purpose"] == "research" and row["kind"] == "价格"
    )
    prices_sha = digest(closes, used_tickers)
    doc = {
        "format": FORMAT,
        "asof": asof.isoformat(),
        "export_sha256": latest_import["export_sha256"],
        "prices_sha256": prices_sha,
        "outcomes": current,
    }
    raw = (json.dumps(doc, sort_keys=True, ensure_ascii=False, indent=1) + "\n").encode("utf-8")
    sha = _sha256(raw)
    seq = len(history) + 1
    if dry_run:
        return SettleResult("settled", [], seq, counts)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("settlement time must have a timezone")
    name = f"settlement_{asof.isoformat()}_{sha[:8]}.json"
    record = {
        "seq": seq,
        "settled_at": now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "asof": asof.isoformat(),
        "file": name,
        "file_sha256": sha,
        "export_sha256": latest_import["export_sha256"],
        "prices_sha256": prices_sha,
        "counts": counts,
        "new_settled_ids": sorted(
            outcome.id
            for outcome in outcomes
            if outcome.state == "settled"
            and old_by_id.get(outcome.id, {}).get("state") != "settled"
        ),
        "changed_ids": sorted(changed_ids),
        "prev_line_sha256": previous,
    }
    line = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    files_dir.mkdir(parents=True, exist_ok=True)
    (files_dir / name).write_bytes(raw)
    with log_path.open("ab") as log:
        log.write(line.encode("utf-8") + b"\n")
    return SettleResult("settled", [], seq, counts)
