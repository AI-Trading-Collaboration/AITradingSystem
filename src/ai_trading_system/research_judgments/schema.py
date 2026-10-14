"""Pure, aggregate validation for private judgment exports (no content in issues)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, TypeGuard

# Protocol invariants of judgments_export.v1: these fields describe the original judgment,
# not its subsequent resolution. Revisions/addenda have separate append-only contracts.
IMMUTABLE_FIELDS = (
    "ticker",
    "statement",
    "prob",
    "createdAt",
    "deadline",
    "kind",
    "market",
    "methods",
    "aiMode",
    "purpose",
    "aiProb",
    "sawSystem",
)


@dataclass(frozen=True)
class Issue:
    code: str
    id: str | None = None
    field: str | None = None
    detail: str = ""


def parse_timestamp(value: Any) -> datetime | None:
    """Return an aware ISO timestamp in UTC, or None without exposing the input."""
    if not isinstance(value, str) or "T" not in value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        return parsed.astimezone(UTC)
    except (ValueError, OverflowError):
        return None


def _deadline(value: Any) -> date | None:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _prob(value: Any) -> bool:
    return type(value) is float and 0 < value < 1


def _nonempty(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _objects(value: Any) -> TypeGuard[list[dict[str, Any]]]:
    return isinstance(value, list) and all(isinstance(item, dict) for item in value)


def _methods(value: Any) -> TypeGuard[list[str]]:
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


def _market_valid(market: Any, kind: Any) -> bool:
    if not isinstance(market, dict):
        return False
    if set(market) != {"benchmark", "horizon", "anchor", "direction"}:
        return False
    direction = market["direction"]
    return (
        _nonempty(market["benchmark"])
        and type(market["horizon"]) is int
        and market["horizon"] > 0
        and market["anchor"] in ("event_prev_close", "entry_close")
        and direction in ("outperform", "underperform", "above", "below")
        and (kind != "价格" or direction in ("outperform", "underperform"))
        and (kind != "波动" or direction in ("above", "below"))
    )


def _immutable_value(field: str, value: Any) -> Any:
    if field == "createdAt":
        parsed = parse_timestamp(value)
        return parsed if parsed is not None else value
    if field == "methods" and _methods(value):
        return set(value)
    return value


def _revision_value(revision: dict[str, Any]) -> tuple[Any, Any]:
    at = revision.get("at")
    parsed = parse_timestamp(at)
    return (parsed if parsed is not None else at, revision.get("prob"))


def _check_baseline(
    record: dict[str, Any], old: dict[str, Any], id_: str, issues: list[Issue]
) -> None:
    for field in IMMUTABLE_FIELDS:
        if _immutable_value(field, record.get(field)) != _immutable_value(field, old.get(field)):
            issues.append(Issue("immutable", id_, field))
    old_addenda = old.get("addenda", [])
    new_addenda = record.get("addenda", [])
    if not _objects(new_addenda) or new_addenda[: len(old_addenda)] != old_addenda:
        issues.append(Issue("addenda_rewritten", id_, "addenda"))
    old_revisions = old.get("probRevisions", [])
    new_revisions = record.get("probRevisions", [])
    if not _objects(new_revisions) or [
        _revision_value(r) for r in new_revisions[: len(old_revisions)]
    ] != [_revision_value(r) for r in old_revisions]:
        issues.append(Issue("revisions_rewritten", id_, "probRevisions"))


def _check_record(
    record: dict[str, Any],
    id_: str | None,
    known_tickers: set[str],
    method_ids: set[str],
    issues: list[Issue],
) -> None:
    bad_fields: set[str] = set()

    def schema(field: str) -> None:
        if field not in bad_fields:
            bad_fields.add(field)
            issues.append(Issue("schema", id_, field))

    for field in ("id", "ticker", "statement", "status"):
        if not _nonempty(record.get(field)):
            schema(field)
    if not _prob(record.get("prob")):
        schema("prob")
    deadline = _deadline(record.get("deadline"))
    if deadline is None:
        schema("deadline")
    created = parse_timestamp(record.get("createdAt"))
    if created is None:
        schema("createdAt")
    for field, choices in (
        ("kind", ("基本面", "价格", "波动", "其他")),
        ("purpose", ("research", "learning")),
        ("aiMode", ("human", "assisted", "ai")),
    ):
        if record.get(field) not in choices:
            schema(field)
    methods = record.get("methods")
    if not _methods(methods):
        schema("methods")
    else:
        if len(set(methods)) != len(methods) or (
            record.get("purpose") == "research" and not methods
        ):
            schema("methods")
        for method in methods:
            if method not in method_ids:
                issues.append(Issue("unknown_method", id_, "methods", method))
    ticker = record.get("ticker")
    if _nonempty(ticker) and ticker not in known_tickers:
        issues.append(Issue("unknown_ticker", id_, "ticker"))
    if not _objects(record.get("addenda", [])):
        schema("addenda")
    for field, valid in (
        ("aiProb", _prob),
        ("sawSystem", lambda value: type(value) is bool),
        ("eventId", lambda value: isinstance(value, str)),
        ("eventTiming", lambda value: value in ("bmo", "amc", "unknown")),
    ):
        value = record.get(field)
        if value is not None and not valid(value):
            schema(field)
    kind = record.get("kind")
    market = record.get("market")
    if market is None:
        if kind in ("价格", "波动"):
            issues.append(Issue("market_missing", id_, "market"))
    elif not _market_valid(market, kind):
        schema("market")
    if created is not None and deadline is not None and created.date() >= deadline:
        issues.append(Issue("time_order", id_, "createdAt"))
    revisions = record.get("probRevisions", [])
    if not _objects(revisions):
        schema("probRevisions")
        return
    previous: datetime | None = None
    time_error = False
    for revision in revisions:
        at = parse_timestamp(revision.get("at"))
        if (
            not {"at", "prob"} <= set(revision) <= {"at", "prob", "note"}
            or at is None
            or not _prob(revision.get("prob"))
            or ("note" in revision and not isinstance(revision["note"], str))
        ):
            schema("probRevisions")
        if at is not None:
            if previous is not None and at <= previous:
                schema("probRevisions")
            previous = at
            if (created is not None and at < created) or (
                deadline is not None and at.date() >= deadline
            ):
                time_error = True
    if time_error:
        issues.append(Issue("time_order", id_, "probRevisions"))


def check_export(
    doc: Any,
    *,
    known_tickers: set[str],
    method_ids: set[str],
    baseline: dict[str, dict[str, Any]] | None = None,
) -> list[Issue]:
    """Report all actionable problems; unknown record fields are retained by the importer."""
    if not isinstance(doc, dict) or doc.get("format") != "judgments_export.v1":
        return [Issue("format")]
    issues: list[Issue] = []
    if parse_timestamp(doc.get("exported_at")) is None:
        issues.append(Issue("format", field="exported_at"))
    rows = doc.get("judgments")
    if not _objects(rows):
        issues.append(Issue("format", field="judgments"))
        return issues
    seen: set[str] = set()
    for record in rows:
        id_ = record.get("id")
        id_ = id_ if isinstance(id_, str) else None
        if id_ is not None:
            if id_ in seen:
                issues.append(Issue("duplicate_id", id_))
                continue
            seen.add(id_)
        _check_record(record, id_, known_tickers, method_ids, issues)
        if baseline is not None and id_ in baseline:
            assert id_ is not None
            _check_baseline(record, baseline[id_], id_, issues)
    if baseline is not None:
        for id_ in sorted(baseline.keys() - seen):
            issues.append(Issue("disappeared", id_))
    return issues


def effective_prob(record: dict[str, Any]) -> float:
    """Scoring probability for an already checked record; never parse prose addenda."""
    revisions = record.get("probRevisions", [])
    return float(revisions[-1]["prob"] if revisions else record["prob"])
